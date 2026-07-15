import json
from pathlib import Path

from qiskit import QuantumCircuit
from qiskit.providers import Backend

from src.execution_time.base_estimator import BaseEstimator


class PolynomialEstimator(BaseEstimator):
    def __init__(self, coefficients: dict[str, float] | None = None):
        """
        Initialize the estimator
        :param coefficients: Regression coefficients
        """
        if coefficients is None:
            self._load_coefficients()
        else:
            self.coefficients = coefficients

    def _load_coefficients(self):
        """
        Load the regression coefficients from a JSON file
        """
        regression_coefficients_path = (
            Path(__file__).absolute().parents[2]
            / "data/polynomial_coefficients.json"
        )

        # Read the regression coefficients from a JSON file
        with open(regression_coefficients_path, "r") as f:
            self.coefficients = json.load(f)

    def estimate_execution_time(
        self,
        circuits: list[QuantumCircuit],
        backend: Backend,
        **kwargs,
    ) -> float:
        """
        Estimate the execution time of a quantum job on a specified backend
        :param circuits: Circuits in the quantum job
        :param backend: Backend to be executed on
        :param kwargs: Additional arguments, like the run configuration
        :return: Estimated execution time
        """
        shots = min(kwargs.get("shots", 4000), backend.max_shots)

        variables = [
            len(circuits),
            shots,
            circuits[0].depth(),
            circuits[0].count_ops().get("measure", 0),
        ]
        execution_time = self._calculate_polynomial(variables)

        return execution_time

    def _calculate_polynomial(self, x):
        """
        Calculate the polynomial  value
        :param x: Independent variables
        :return: Dependent variable
        """
        return (
            self.coefficients["a"]
            + self.coefficients["b"] * x[0]
            + self.coefficients["c"] * x[1]
            + self.coefficients["d"] * x[2]
            + self.coefficients["e"] * x[3]
            + self.coefficients["f"] * x[0] * x[1]
            + self.coefficients["g"] * x[1] * x[2]
            + self.coefficients["h"] * x[2] * x[3]
            + self.coefficients["i"] * x[0] * x[3]
            + self.coefficients["j"] * x[0] * x[2]
            + self.coefficients["k"] * x[1] * x[3]
            + self.coefficients["l"] * x[0] * x[1] * x[2]
            + self.coefficients["m"] * x[1] * x[2] * x[3]
            + self.coefficients["n"] * x[0] * x[2] * x[3]
            + self.coefficients["o"] * x[0] * x[1] * x[3]
            + self.coefficients["p"] * x[0] * x[1] * x[2] * x[3]
        )
