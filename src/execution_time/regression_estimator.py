import logging
from collections import defaultdict
from pathlib import Path

import numpy as np
from joblib import dump, load
from qiskit import QuantumCircuit
from qiskit.circuit import Clbit
from qiskit.providers import Backend
from sklearn.base import RegressorMixin
from sklearn.ensemble import (
    GradientBoostingRegressor,
    ExtraTreesRegressor,
    RandomForestRegressor,
    AdaBoostRegressor,
    HistGradientBoostingRegressor,
)
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import (
    GridSearchCV,
    KFold,
    cross_val_score,
)
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import PolynomialFeatures

from src.execution_time.base_estimator import BaseEstimator
from src.utils.database import (
    extract_jobs_from_archive,
)

logger = logging.getLogger(__name__)


class RegressionEstimator(BaseEstimator):
    def __init__(
        self,
        model: RegressorMixin | None = None,
        model_file: str | None = None,
        dataset_file: str | None = None,
    ):
        self.model_directory = (
            Path(__file__).absolute().parents[2] / "data" / "regression_models"
        )
        self.model_directory.mkdir(parents=True, exist_ok=True)
        if dataset_file is not None:
            self.dataset_file = dataset_file
        else:
            self.dataset_file = (
                Path(__file__).absolute().parents[2]
                / "data/regression_dataset.npz"
            )
        if model is not None and model_file is not None:
            logger.warning("Both model and model file specified, using model")
            self.model = model
        elif model is not None:
            self.model = model
        elif model_file is not None:
            self.model_file = model_file
            try:
                self._load_model()
            except FileNotFoundError:
                logger.warning(
                    f"Model file {model_file} not found, training new model"
                )
                self._train_regression_model()
        else:
            self.model_file = (
                Path(__file__).absolute().parents[2]
                / "data/regression_model.joblib"
            )
            try:
                self._load_model()
            except FileNotFoundError:
                logger.info("Training new model")
                self._train_regression_model()

    def estimate_execution_time(
        self,
        circuits: list[QuantumCircuit],
        backend: Backend = None,
        **kwargs,
    ) -> float:
        """
        Estimate the execution time of a quantum job on a specified backend
        :param circuits: Circuits in the quantum job
        :param backend: Backend to be executed on
        :param kwargs: Additional arguments, like the run configuration
        :return: Estimated execution time
        """
        if backend is None:
            default_shots = 100000
        else:
            default_shots = backend.max_shots
        shots = min(kwargs.get("shots", 4000), default_shots)
        features = self._extract_features(circuits, shots)
        return self.model.predict(features)[0]

    @classmethod
    def _extract_features(
        cls, circuits: list[QuantumCircuit], shots: int
    ) -> np.ndarray:
        """
        Extract features from a list of circuits
        :param circuits: Circuits to extract features from
        :param shots: Number of shots
        :return: Features
        """
        features = defaultdict(list)
        for circuit in circuits:
            circuit_features = cls._extract_circuit_features(circuit)
            for feature_name, feature_value in circuit_features.items():
                features[feature_name].append(feature_value)

        aggregate_features = {}
        for feature_name, feature_values in features.items():
            aggregate_features[feature_name] = np.array(feature_values).sum()

        aggregate_features["shots"] = shots
        aggregate_features["circuit_count"] = len(circuits)

        return np.array(list(aggregate_features.values())).reshape(1, -1)

    @classmethod
    def _get_feature_names(cls):
        feature_names = [
            "depth",
            "num_qubits",
            "swap",
            "shots",
            "circuit_count",
        ]

        return feature_names

    @classmethod
    def _extract_circuit_features(
        cls, circuit: QuantumCircuit
    ) -> dict[str, float]:
        features = {"swap": 0}
        swap_gates = ["cx", "cz", "ecr"]
        qubits = set()
        depth = 0
        bit_indices = {
            bit: idx for idx, bit in enumerate(circuit.qubits + circuit.clbits)
        }
        operations_stack = [0] * len(bit_indices)

        for operation, qargs, cargs in circuit.data:
            if operation.name in swap_gates:
                features["swap"] += 1
            if getattr(operation, "_directive", False):
                continue

            num_qargs = len(qargs)

            current_bit_indices = []
            longest_path = 0
            for i, bit in enumerate(qargs):
                qubits.add(bit)
                current_bit_indices.append(bit_indices[bit])
                operations_stack[current_bit_indices[i]] += 1

                if operations_stack[current_bit_indices[i]] > longest_path:
                    longest_path = operations_stack[current_bit_indices[i]]

            for i, bit in enumerate(cargs, num_qargs):
                current_bit_indices.append(bit_indices[bit])
                operations_stack[current_bit_indices[i]] += 1
                if operations_stack[current_bit_indices[i]] > longest_path:
                    longest_path = operations_stack[current_bit_indices[i]]

            if getattr(operation, "condition", None):
                if isinstance(operation.condition[0], Clbit):
                    condition_bits = [operation.condition[0]]
                else:
                    condition_bits = operation.condition[0]
                for cbit in condition_bits:
                    idx = bit_indices[cbit]
                    if idx not in current_bit_indices:
                        current_bit_indices.append(idx)
                        operations_stack[idx] += 1
                        if operations_stack[idx] > longest_path:
                            longest_path = operations_stack[idx]

            for i in current_bit_indices:
                operations_stack[i] = longest_path
            if longest_path > depth:
                depth = longest_path

        features["depth"] = depth

        features["num_qubits"] = len(qubits)

        return features

    @staticmethod
    def _run_grid_search(
        model: RegressorMixin,
        parameter_grid: dict[str, list],
        x: np.ndarray,
        y: np.ndarray,
        scoring: str = "r2",
    ) -> tuple[GridSearchCV, float]:
        """
        Run grid search on a model with a parameter grid
        :param model: Model to be evaluated
        :param parameter_grid: Parameter grid for grid search
        :param x: Independent variables
        :param y: Dependent variables
        :param scoring: Scoring method
        :return: Grid search and score
        """
        # K-fold cross-validation
        evaluation_cv = KFold(n_splits=10, shuffle=True, random_state=0)
        search_cv = KFold(n_splits=5, shuffle=True, random_state=0)
        # Grid search
        grid_search = GridSearchCV(
            model,
            parameter_grid,
            scoring=scoring,
            n_jobs=-1,
            cv=search_cv,
            refit=True,
        )
        # Evaluate model
        score = cross_val_score(
            grid_search, X=x, y=y, scoring=scoring, cv=evaluation_cv, n_jobs=-1
        )

        return grid_search, np.mean(score)

    def _choose_best_model(
        self,
        models: dict[str, RegressorMixin],
        parameter_grids: dict[str, dict[str, list]],
        x: np.ndarray,
        y: np.ndarray,
    ):
        """
        Choose the best model based on the scoring method
        :param models: Models to be evaluated
        :param parameter_grids: Parameter grids for grid search
        :param x: Independent variables
        :param y: Dependent variables
        :return: Best model
        """
        best_grid_search = None
        best_score = 0
        best_model_name = None
        # Evaluate models
        for model_name, model in models.items():
            # Get score for the tuned model
            logger.info(f"Evaluating {model_name}")
            grid_search, score = self._run_grid_search(
                model, parameter_grids[model_name], x, y
            )
            logger.info(f"{model_name} score: {score}")

            if score > best_score:
                best_score = score
                best_grid_search = grid_search
                best_model_name = model_name

            grid_search.fit(x, y)
            self._save_model(
                grid_search.best_estimator_,
                model_name.lower().replace(" ", "_"),
            )

        logger.info(f"Best model: {best_model_name} with score {best_score}")

        best_grid_search.fit(x, y)

        self.model = best_grid_search.best_estimator_
        self._rename_model(
            best_model_name.lower().replace(" ", "_"),
        )

    def _save_model(self, model: RegressorMixin, model_name: str) -> None:
        """
        Save the model to a file

        :param model: Model to be saved
        :param model_name: Name of the model
        """
        model_file = self.model_directory / f"{model_name}.joblib"

        dump(model, model_file)
        logger.info(f"Model saved to {model_file}")

    def _rename_model(self, model_name: str) -> None:
        """
        Rename the model file
        :param model_name: Chosen model name
        """
        model_file = self.model_directory / f"{model_name}.joblib"
        model_file.rename(self.model_file)

    def _load_model(self) -> None:
        """
        Load the model from a file
        """
        self.model = load(self.model_file)

    @staticmethod
    def _get_available_models() -> dict[str, RegressorMixin]:
        """
        Get the models to be evaluated
        :return: Models to be evaluated
        """
        models = {
            "Extra Trees": ExtraTreesRegressor(n_jobs=1, random_state=0),
            "Random Forest": RandomForestRegressor(n_jobs=1, random_state=0),
            "Gradient Boosting": GradientBoostingRegressor(random_state=0),
            "AdaBoost": AdaBoostRegressor(random_state=0),
            "Histogram Gradient Boosting": HistGradientBoostingRegressor(
                random_state=0
            ),
            "Polynomial Regression": make_pipeline(
                PolynomialFeatures(degree=3, include_bias=False),
                LinearRegression(),
            ),
        }

        return models

    @staticmethod
    def _get_model_parameter_grids() -> dict[str, dict[str, list]]:
        """
        Get the parameter grids for grid search
        :return: Parameter grids for grid search
        """
        parameter_grids = {
            "Extra Trees": {
                "n_estimators": [100, 300, 500, 700],
                "max_depth": [None, 5, 10, 30, 50],
                "min_samples_split": [2, 5, 10],
                "min_samples_leaf": [1, 5, 10, 20, 30],
                "max_features": [None, "sqrt", "log2"],
                "max_leaf_nodes": [None, 10, 30, 50, 70],
                "bootstrap": [True, False],
            },
            "Random Forest": {
                "n_estimators": [100, 300, 500, 700],
                "max_depth": [None, 5, 10, 30, 50],
                "min_samples_split": [2, 5, 10],
                "min_samples_leaf": [1, 5, 10, 20, 30],
                "max_features": [None, "sqrt", "log2"],
                "max_leaf_nodes": [None, 10, 30, 50, 70],
                "bootstrap": [True, False],
            },
            "Gradient Boosting": {
                "learning_rate": [0.001, 0.01, 0.1, 1],
                "n_estimators": [100, 300, 500, 700],
                "subsample": [0.3, 0.5, 0.7, 1.0],
                "max_depth": [None, 5, 10, 30],
                "min_samples_split": [2, 5, 10],
                "min_samples_leaf": [1, 5, 10, 20, 30],
                "max_features": [None, "sqrt", "log2"],
                "max_leaf_nodes": [None, 10, 30, 50, 70],
            },
            "AdaBoost": {
                "n_estimators": [100, 300, 500, 700],
                "learning_rate": [0.001, 0.01, 0.1, 1],
                "loss": ["linear", "square", "exponential"],
            },
            "Histogram Gradient Boosting": {
                "learning_rate": [0.001, 0.01, 0.1, 1],
                "max_iter": [100, 300, 500, 700],
                "max_leaf_nodes": [10, 30, 50, 70],
                "max_depth": [None, 5, 10, 30, 50],
                "min_samples_leaf": [5, 10, 20, 30],
                "l2_regularization": [0, 0.01, 0.1, 1],
            },
            "Polynomial Regression": {
                "polynomialfeatures__degree": [2, 3, 4],
            },
        }

        return parameter_grids

    def _create_dataset(self) -> tuple[np.ndarray, np.ndarray]:
        """
        Create the dataset
        """
        x = []
        y = []

        for job in extract_jobs_from_archive():
            try:
                logger.info(
                    "Extracting features from job %s", job.ibm_quantum_id
                )
                x.append(
                    self._extract_features(
                        [circuit.quantum_circuit for circuit in job.circuits],
                        job.shots,
                    )
                )
                y.append(job.taken_time)
            except Exception as e:
                logger.warning(
                    "Failed to extract features from job %s: %s",
                    job.ibm_quantum_id,
                    e,
                )

        x = np.concatenate(x, axis=0)
        y = np.array(y)

        np.savez(self.dataset_file, x=x, y=y)

        return x, y

    def _load_dataset(self) -> tuple[np.ndarray, np.ndarray]:
        """
        Load the dataset
        :return: Dataset with independent and dependent variables
        """
        try:
            dataset = np.load(self.dataset_file)
            x = dataset["x"]
            y = dataset["y"]
        except FileNotFoundError:
            logger.warning("Dataset not found, creating new dataset")
            x, y = self._create_dataset()

        return x, y

    def _train_regression_model(self) -> None:
        """
        Train a regression model
        """
        x, y = self._load_dataset()
        models = self._get_available_models()
        parameter_grids = self._get_model_parameter_grids()
        self._choose_best_model(
            models,
            parameter_grids,
            x,
            y,
        )
