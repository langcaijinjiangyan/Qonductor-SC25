import logging
import multiprocessing
import os
import sys
from collections import OrderedDict
from enum import Enum
import time
from multiprocessing.pool import ThreadPool
from timeit import default_timer as timer
from typing import Any

import pymoo.gradient.toolbox as anp
from mapomatic import deflate_circuit, matching_layouts
from numpy import argmin
from pymoo.algorithms.moo.nsga2 import NSGA2
from pymoo.core.algorithm import Algorithm
from pymoo.parallelization import StarmapParallelization
from pymoo.core.result import Result
from pymoo.mcdm.pseudo_weights import PseudoWeights
from pymoo.operators.crossover.pntx import TwoPointCrossover
from pymoo.operators.crossover.sbx import SBX
from pymoo.operators.mutation.bitflip import BitflipMutation
from pymoo.operators.mutation.pm import PM
from pymoo.operators.repair.rounding import RoundingRepair
from pymoo.operators.sampling.rnd import (
    BinaryRandomSampling,
    IntegerRandomSampling,
)
from pymoo.optimize import minimize
from qiskit import QuantumCircuit, qasm3, transpile
from qiskit.circuit import Measure, Reset, Gate
from qiskit.providers import Backend
from qiskit.providers.fake_provider.fake_backend import FakeBackendV2

from src.execution_time.base_estimator import BaseEstimator
from src.execution_time.regression_estimator import RegressionEstimator
from src.optimization.binary_problem import BinarySchedulingProblem
from src.optimization.discrete_problem import DiscreteSchedulingProblem
from src.scheduler.base_scheduler import (
    BaseScheduler,
    Assignment,
    SchedulingJob,
)
from src.utils.benchmark import load_pre_transpiled_circuit

logger = logging.getLogger(__name__)


def _safe_backend_name(backend: Backend) -> str:
    return getattr(backend, "name", "<unknown>")


def _circuit_debug_dump(circuit: QuantumCircuit) -> str:
    """Return a detailed circuit dump for scheduler diagnostics."""
    try:
        circuit_qasm = qasm3.dumps(circuit)
    except Exception as exc:
        try:
            circuit_qasm = circuit.qasm()
        except Exception as qasm_exc:
            circuit_qasm = (
                "<failed to serialize circuit: "
                f"qasm3={exc!r}; qasm2={qasm_exc!r}>"
            )

    try:
        ops = dict(circuit.count_ops())
    except Exception as exc:
        ops = {"<count_ops_error>": repr(exc)}

    try:
        parameters = sorted(str(param) for param in circuit.parameters)
    except Exception as exc:
        parameters = [f"<parameters_error: {exc!r}>"]

    try:
        depth = circuit.depth()
    except Exception as exc:
        depth = f"<depth_error: {exc!r}>"

    return (
        "Circuit debug dump:\n"
        f"  name={getattr(circuit, 'name', '<unknown>')!r}\n"
        f"  qubits={getattr(circuit, 'num_qubits', '<unknown>')}\n"
        f"  clbits={getattr(circuit, 'num_clbits', '<unknown>')}\n"
        f"  depth={depth}\n"
        f"  ops={ops}\n"
        f"  parameters={parameters}\n"
        "  qasm3:\n"
        f"{circuit_qasm}"
    )


def _backend_debug_dump(backend: Backend) -> str:
    """Return backend details that affect transpilation."""
    try:
        operation_names = sorted(getattr(backend, "operation_names", []) or [])
    except Exception as exc:
        operation_names = [f"<operation_names_error: {exc!r}>"]

    try:
        coupling_map = getattr(backend, "coupling_map", None)
        coupling_edges = coupling_map.get_edges() if coupling_map else []
    except Exception as exc:
        coupling_edges = [f"<coupling_map_error: {exc!r}>"]

    return (
        "Backend debug dump:\n"
        f"  name={_safe_backend_name(backend)!r}\n"
        f"  num_qubits={getattr(backend, 'num_qubits', '<unknown>')}\n"
        f"  processor_type={getattr(backend, 'processor_type', '<unknown>')!r}\n"
        f"  operation_names={operation_names}\n"
        f"  coupling_edges={coupling_edges}"
    )


class TranspilationLevel(Enum):
    """
    Enum for transpilation level
    """

    QPU = "QPU"  # Transpile for each QPU
    PROCESSOR_TYPE = "processor type"  # Transpile for each processor type
    PRE_TRANSPILED = "pre-transpiled"  # Use pre-transpiled circuits


class ProblemType(Enum):
    """
    Enum for problem type
    """

    BINARY = "binary"
    DISCRETE = "discrete"

def calculate_exeucution_time(job, backends, estimator):
        current_execution_times = []

        for i, backend in enumerate(backends):
            if job[i] is None:
                execution_time = sys.maxsize
            else:
                execution_time = estimator.estimate_execution_time(
                    job[i].circuits, backend, shots=job[i].shots
                )
            current_execution_times.append(execution_time)

        return current_execution_times


class MultiObjectiveScheduler(BaseScheduler):
    """
    Circuit scheduler that optimizes for fidelity
    """

    AVERAGE_JOB_TIME = 10

    def __init__(
        self,
        transpilation_count: int = 10,
        transpilation_level: TranspilationLevel = TranspilationLevel.QPU,
        algorithm: Algorithm | None = None,
        estimator: BaseEstimator | None = None,
        problem_type: ProblemType = ProblemType.BINARY,
        transpilation_cache_enabled: bool = True,
        transpilation_cache_size: int = 256,
    ):
        self.transpilation_count = transpilation_count
        self.transpilation_level = transpilation_level
        self.problem_type = problem_type
        self.transpilation_cache_enabled = transpilation_cache_enabled
        self.transpilation_cache_size = max(1, transpilation_cache_size)
        self._transpilation_cache: OrderedDict[
            tuple[Any, ...], tuple[SchedulingJob, float]
        ] = OrderedDict()
        self._last_transpilation_cache_stats = {
            "enabled": transpilation_cache_enabled,
            "hits": 0,
            "misses": 0,
            "deduplicated": 0,
            "evictions": 0,
            "size": 0,
            "capacity": self.transpilation_cache_size,
        }

        self.algorithm = (
            algorithm
            or NSGA2(
                pop_size=100,
                sampling=BinaryRandomSampling(),
                crossover=TwoPointCrossover(),
                mutation=BitflipMutation(),
                eliminate_duplicates=True,
            )
            if problem_type == ProblemType.BINARY
            else NSGA2(
                pop_size=100,
                sampling=IntegerRandomSampling(),
                crossover=SBX(
                    prob=0.5, eta=2.0, vtype=float, repair=RoundingRepair()
                ),
                mutation=PM(
                    prob=0.5, eta=2.0, vtype=float, repair=RoundingRepair()
                ),
                eliminate_duplicates=True,
            )
        )
        self.estimator = estimator or RegressionEstimator()
        self.pre_transpiled_circuits = {}

    def __getstate__(self):
        """Avoid serializing the parent-process LRU into pool workers."""
        state = self.__dict__.copy()
        state["_transpilation_cache"] = OrderedDict()
        return state

    def schedule(
        self,
        jobs: list[SchedulingJob],
        backends: list[Backend],
        **kwargs,
    ) -> tuple[list[Assignment], list[SchedulingJob], Any]:
        """
        Schedule a list of jobs to a list of backends
        :param jobs: Jobs to be scheduled
        :param backends: Backends to be scheduled to
        :param kwargs: Additional arguments
        :return: A list of assignments, a list of jobs
        that could not be scheduled and scheduling metadata
        """
        metadata = {}
        # Filter out jobs which include circuits
        # that are too large for the backends
        rejected_jobs = self._filter_jobs(jobs, backends)
        if rejected_jobs and len(rejected_jobs) < len(jobs):
            logger.warning(
                "%d jobs were rejected due to including "
                "circuits that are too large for the backends",
                len(rejected_jobs),
            )
            jobs = [job for job in jobs if job not in rejected_jobs]
        elif rejected_jobs:
            logger.warning(
                "All jobs were rejected due to including "
                "circuits that are too large for the backends"
            )
            return [], jobs, None

        start_transpilation_time = timer()
        start = time.perf_counter()
        transpiled_jobs, fidelities = self._transpile_jobs(jobs, backends)
        total = time.perf_counter() - start
        print(total)

        end_transpilation_time = timer()
        
        metadata["transpilation_time"] = (
            end_transpilation_time - start_transpilation_time
        )
        metadata["transpilation_cache"] = dict(
            self._last_transpilation_cache_stats
        )
        logger.info(
            "Transpilation took %f seconds", metadata["transpilation_time"]
        )

        # Calculate execution times for each job on each backend
        start = time.perf_counter()
        execution_times = self._calculate_execution_times(
            transpiled_jobs, backends
        )
        total = time.perf_counter() - start
        print(total)

        end_estimation_time = timer()
        
        metadata["estimation_time"] = (
            end_estimation_time - end_transpilation_time
        )
        logger.info(
            "Execution time estimation took %f seconds",
            metadata["estimation_time"],
        )

        backend_sizes = self._get_backend_sizes(backends)
        job_sizes = self._get_job_sizes(jobs)

        # Estimate waiting times for each backend job queue
        start = time.perf_counter()
        waiting_times = self._calculate_backend_queue_waiting_times(backends)
        total = time.perf_counter() - start
        print(total)

        # Define the optimization problem
        start_optimization_time = timer()

        num_threads = max(1, int(multiprocessing.cpu_count() / 2))
        start = time.perf_counter()
        with ThreadPool(num_threads) as pool:
            runner = StarmapParallelization(pool.starmap)
            if self.problem_type == ProblemType.BINARY:
                problem = BinarySchedulingProblem(
                    len(jobs),
                    len(backends),
                    execution_times,
                    fidelities,
                    waiting_times,
                    job_sizes,
                    backend_sizes,
                    elementwise_runner=runner,
                )
            else:
                problem = DiscreteSchedulingProblem(
                    len(jobs),
                    len(backends),
                    execution_times,
                    fidelities,
                    waiting_times,
                    job_sizes,
                    backend_sizes,
                    elementwise_runner=runner,
                )

            # Run the optimization while the elementwise runner is alive.
            result = minimize(problem, self.algorithm, verbose=False)
        total = time.perf_counter() - start
        print(total)
        end_optimization_time = timer()
        metadata["optimization_time"] = (
            end_optimization_time - start_optimization_time
        )
        logger.info(
            "Optimization took %f seconds",
            metadata["optimization_time"],
        )

        # Check if a solution was found
        if result.X is None:
            logger.warning("No solution found")
            return [], rejected_jobs + jobs, metadata
        # Get the weights for the objectives
        weights = anp.array(kwargs.get("weights", [0.5, 0.5]))
        # Find the best solution matching the weights
        start_mcdm_time = timer()
        solution_index = self._get_best_solution(result, weights)
        end_mcdm_time = timer()
        metadata["mcdm_time"] = end_mcdm_time - start_mcdm_time
        logger.info(
            "MCDM took %f seconds",
            metadata["mcdm_time"],
        )

        solution = result.X[solution_index]

        # Create the schedule
        start_schedule_generation_time = timer()
        schedule = self._transform_solution_into_schedule(
            solution, transpiled_jobs, backends
        )
        end_schedule_generation_time = timer()
        metadata["schedule_generation_time"] = (
            end_schedule_generation_time - start_schedule_generation_time
        )

        logger.info(
            "Schedule generation took %f seconds",
            metadata["schedule_generation_time"],
        )

        metadata["mean_error"] = result.F[:, 1].tolist()
        metadata["mean_waiting_time"] = result.F[:, 0].tolist()
        metadata["solution_index"] = solution_index
        metadata["solution"] = solution.tolist()

        if self.problem_type == ProblemType.BINARY:
            solution = solution.reshape(len(transpiled_jobs), len(backends))
            solution_execution_times = anp.sum(
                execution_times * solution, axis=1
            )
            all_solutions = result.X.reshape(
                result.X.shape[0], len(transpiled_jobs), len(backends)
            )
            backend_execution_times = anp.sum(
                execution_times * all_solutions, axis=1
            )
            backend_waiting_times = backend_execution_times + waiting_times
            all_solutions_waiting_times = anp.sum(
                backend_waiting_times[:, None, :] * all_solutions, axis=-1
            )
            all_solutions_fidelities = anp.sum(
                fidelities * all_solutions, axis=2
            )
        else:
            assignment_indices = anp.arange(len(jobs))
            solution_execution_times = execution_times[
                assignment_indices, solution
            ]
            backend_execution_times = anp.array(
                [
                    anp.bincount(
                        solution,
                        weights=execution_times[
                            assignment_indices, assignments
                        ],
                        minlength=len(backends),
                    )
                    for assignments in result.X
                ]
            )
            backend_waiting_times = backend_execution_times + waiting_times
            all_solutions_waiting_times = backend_waiting_times[
                anp.arange(result.X.shape[0])[:, None], result.X
            ]
            all_solutions_fidelities = fidelities[assignment_indices, result.X]
        metadata["waiting_time_90_percentile"] = anp.percentile(
            all_solutions_waiting_times, 90, axis=1
        ).tolist()
        metadata["waiting_time_95_percentile"] = anp.percentile(
            all_solutions_waiting_times, 95, axis=1
        ).tolist()
        metadata["fidelity_90_percentile"] = anp.percentile(
            all_solutions_fidelities, 90, axis=1
        ).tolist()
        metadata["fidelity_95_percentile"] = anp.percentile(
            all_solutions_fidelities, 95, axis=1
        ).tolist()

        metadata[
            "solution_execution_times"
        ] = solution_execution_times.tolist()

        # Per-job fidelities for the chosen solution
        if self.problem_type == ProblemType.BINARY:
            per_job_fidelities = anp.sum(fidelities * solution, axis=1)
        else:
            per_job_fidelities = fidelities[
                anp.arange(len(jobs)), solution
            ]
        metadata["solution_fidelities"] = per_job_fidelities.tolist()

        return schedule, rejected_jobs, metadata

    def _transform_solution_into_schedule(
        self,
        solution: anp.ndarray,
        transpiled_jobs: list[list[SchedulingJob]],
        backends: list[Backend],
    ) -> list[Assignment]:
        """
        Transform a solution into a schedule
        :param solution: Optimization problem solution
        :param transpiled_jobs: Transpiled jobs
        :param backends: Quantum backends
        :return: A schedule
        """
        schedule = []

        if self.problem_type == ProblemType.BINARY:
            # Reshape the solution to a 2D array of shape
            # (len(jobs), len(backends))
            solution = solution.reshape(len(transpiled_jobs), len(backends))

            for i in range(len(transpiled_jobs)):
                # Find the index of the chosen backend
                backend_index = anp.flatnonzero(solution[i]).item()
                schedule.append(
                    (
                        transpiled_jobs[i][backend_index],
                        backends[backend_index],
                    )
                )
        else:
            for i, backend_index in enumerate(solution):
                schedule.append(
                    (
                        transpiled_jobs[i][backend_index],
                        backends[backend_index],
                    )
                )
        return schedule

    @staticmethod
    def _filter_jobs(
        jobs: list[SchedulingJob], backends: list[Backend]
    ) -> list[SchedulingJob]:
        """
        Filter out jobs that include circuits that are too large for the
        backends
        :param jobs: The jobs to filter
        :param backends: The backends to filter against
        :return: A list of jobs that include circuits that are too large for
        the backends
        """
        max_backend_size = max([backend.num_qubits for backend in backends])
        return [
            job
            for job in jobs
            if any(
                circuit.num_qubits > max_backend_size
                for circuit in job.circuits
            )
        ]

    @staticmethod
    def _normalize_objectives(objectives: anp.ndarray) -> anp.ndarray:
        """
        Normalize the objectives to the range [0, 1]
        :param objectives: The objectives to normalize
        :return: The normalized objectives
        """
        min_objectives = objectives.min(axis=0)
        max_objectives = objectives.max(axis=0)
        return (objectives - min_objectives) / (
            max_objectives - min_objectives
        )

    @staticmethod
    def _get_best_solution(result: Result, weights: anp.ndarray) -> int:
        """
        Find the best solution in the result matching the given weights
        :param result: The result of the optimization
        :param weights: The weights to match
        :return: The index of the best solution
        """
        return int(PseudoWeights(weights).do(result.F))

    def _transpile_circuit(
        self, circuit: QuantumCircuit, backend: Backend
    ) -> tuple[QuantumCircuit | None, float]:
        """
        Transpile a circuit for a given backend
        :param circuit: The circuit to transpile
        :param backend: The backend to transpile for
        :return: The transpiled circuit and the fidelity
        """
        # Check if the circuit can be transpiled for the backend
        if circuit.num_qubits > backend.num_qubits:
            return None, 0.0

        try:
            # Transpile the circuit multiple times due to the stochastic nature
            # of the transpiler
            try:
                transpiled_circuits = transpile(
                    [circuit] * self.transpilation_count,
                    backend=backend,
                    optimization_level=3,
                )
            except Exception:
                logger.exception(
                    "Initial Qiskit transpile failed "
                    "(backend=%s, optimization_level=3, transpilation_count=%s)\n"
                    "%s\n%s",
                    _safe_backend_name(backend),
                    self.transpilation_count,
                    _backend_debug_dump(backend),
                    _circuit_debug_dump(circuit),
                )
                return None, 0.0

            # Choose the circuit with the lowest number of SWAP gates
            swap_gate = set(backend.operation_names).intersection(
                {"cx", "cz", "ecr"}
            )
            if not swap_gate:
                logger.error(
                    "Cannot find swap gate for backend %s\n%s\n%s",
                    _safe_backend_name(backend),
                    _backend_debug_dump(backend),
                    _circuit_debug_dump(circuit),
                )
                return None, 0.0
            swap_gate = swap_gate.pop()
            swap_gate_counts = [
                transpiled_circuit.count_ops().get(swap_gate, 0)
                for transpiled_circuit in transpiled_circuits
            ]
            best_transpiled_circuit = transpiled_circuits[
                argmin(swap_gate_counts)
            ]

            # Deflate the circuit to remove ancilla qubits
            deflated_circuit = deflate_circuit(best_transpiled_circuit)

            # Find the best layout for the deflated circuit
            layouts = matching_layouts(
                deflated_circuit,
                backend.coupling_map,
                strict_direction=False,
            )
            if layouts:
                best_layout, fidelity = self._get_best_layout(
                    deflated_circuit, backend, layouts
                )
                try:
                    best_circuit = transpile(
                        deflated_circuit,
                        backend=backend,
                        initial_layout=best_layout,
                        optimization_level=0,
                    )
                except Exception:
                    logger.exception(
                        "Final Qiskit transpile failed "
                        "(backend=%s, optimization_level=0, initial_layout=%s)\n"
                        "%s\nOriginal %s\nDeflated %s",
                        _safe_backend_name(backend),
                        best_layout,
                        _backend_debug_dump(backend),
                        _circuit_debug_dump(circuit),
                        _circuit_debug_dump(deflated_circuit),
                    )
                    return None, 0.0

                return best_circuit, fidelity
            logger.error(
                "No matching layouts found during transpilation "
                "(backend=%s)\n%s\n%s",
                _safe_backend_name(backend),
                _backend_debug_dump(backend),
                _circuit_debug_dump(deflated_circuit),
            )
            return None, 0.0
        except Exception:
            logger.exception(
                "Unexpected error in transpilation pipeline "
                "(backend=%s)\n%s\n%s",
                _safe_backend_name(backend),
                _backend_debug_dump(backend),
                _circuit_debug_dump(circuit),
            )
            return None, 0.0

        return None, 0.0

    def _get_best_layout(
        self,
        circuit: QuantumCircuit,
        backend: Backend,
        layouts: list[list[int]],
    ) -> tuple[list[int], float]:
        """
        Find the best layout for a circuit on a given backend
        :param circuit: Quantum circuit
        :param backend: Quantum backend
        :param layouts: Possible layouts
        :return: The best layout and its fidelity
        """
        best_layout = None
        best_fidelity = 0.0
        # Calculate the fidelity for each layout and choose the best one
        for layout in layouts:
            fidelity = self._calculate_fidelity(circuit, backend, layout)
            if fidelity > best_fidelity:
                best_layout = layout
                best_fidelity = fidelity

        return best_layout, best_fidelity

    @staticmethod
    def _calculate_fidelity(
        circuit: QuantumCircuit,
        backend: Backend,
        layout: list[int] | None = None,
    ) -> float:
        """
        Calculate the fidelity of a circuit for a given backend and layout
        :param circuit: Quantum circuit
        :param backend: Quantum backend
        :param layout: Layout
        :return: The fidelity
        """
        props = backend.properties()
        fidelity = 1.0
        # Calculate the fidelity for each instruction in the circuit
        for instruction, qargs, cargs in circuit.data:
            # Use the readout error for measurements and resets
            if isinstance(instruction, Measure) or isinstance(
                instruction, Reset
            ):
                qubit = circuit.find_bit(qargs[0]).index
                layout_qubit = layout[qubit] if layout is not None else qubit
                fidelity *= 1 - props.readout_error(layout_qubit)
            # Use the gate error for gates
            elif isinstance(instruction, Gate):
                qubits = [circuit.find_bit(qarg).index for qarg in qargs]
                layout_qubit = (
                    [layout[qubit] for qubit in qubits]
                    if layout is not None
                    else qubits
                )
                fidelity *= 1 - props.gate_error(
                    instruction.name, layout_qubit
                )

        return fidelity

    def _transpile_job(self, job, backends, processor_types):
        # Transpile the job for each backend
        current_transpiled_job = []
        current_fidelities = []
        processor_type_circuits = {}
        for backend in backends:
            transpiled_job = None
            fidelity = 0.0
            if (
                self.transpilation_level
                == TranspilationLevel.PROCESSOR_TYPE
            ):
                # Transpile the circuit for each processor type
                if (
                    processor_types[backend.name]
                    not in processor_type_circuits
                ):
                    transpiled_circuits = []
                    circuit_fidelities = []
                    for circuit in job.circuits:
                        (
                            transpiled_circuit,
                            circuit_fidelity,
                        ) = self._transpile_circuit(circuit, backend)
                        transpiled_circuits.append(transpiled_circuit)
                        circuit_fidelities.append(circuit_fidelity)
                    if all(
                        transpiled_circuit is not None
                        for transpiled_circuit in transpiled_circuits
                    ):
                        transpiled_job = SchedulingJob(
                            transpiled_circuits, job.shots
                        )
                        processor_type_circuits[
                            processor_types[backend.name]
                        ] = transpiled_job
                        fidelity = anp.exp(
                            anp.log(circuit_fidelities).mean()
                        )
                else:
                    transpiled_job = processor_type_circuits[
                        processor_types[backend.name]
                    ]
                    circuit_fidelities = []
                    for transpiled_circuit in transpiled_job.circuits:
                        circuit_fidelity = self._calculate_fidelity(
                            transpiled_circuit, backend
                        )
                        circuit_fidelities.append(circuit_fidelity)
                    fidelity = anp.exp(anp.log(circuit_fidelities).mean())
            elif self.transpilation_level == TranspilationLevel.QPU:
                transpiled_circuits = []
                circuit_fidelities = []
                for circuit in job.circuits:
                    (
                        transpiled_circuit,
                        circuit_fidelity,
                    ) = self._transpile_circuit(circuit, backend)
                    transpiled_circuits.append(transpiled_circuit)
                    circuit_fidelities.append(circuit_fidelity)
                if all(
                    transpiled_circuit is not None
                    for transpiled_circuit in transpiled_circuits
                ):
                    transpiled_job = SchedulingJob(
                        transpiled_circuits, job.shots
                    )
                    fidelity = anp.exp(anp.log(circuit_fidelities).mean())
            elif (
                self.transpilation_level
                == TranspilationLevel.PRE_TRANSPILED
            ):
                transpiled_circuits = [
                    self.pre_transpiled_circuits[
                        (backend.name, circuit.name, circuit.num_qubits)
                    ]
                    for circuit in job.circuits
                ]
                
                if all(
                    transpiled_circuit is not None
                    for transpiled_circuit in transpiled_circuits
                ):
                    transpiled_job = SchedulingJob(
                        transpiled_circuits, job.shots
                    )
                    circuit_fidelities = []
                    for transpiled_circuit in transpiled_job.circuits:
                        circuit_fidelity = self._calculate_fidelity(
                            transpiled_circuit, backend
                        )
                        circuit_fidelities.append(circuit_fidelity)
                    fidelity = anp.exp(anp.log(circuit_fidelities).mean())
            else:
                message = (
                    f"Transpilation level {self.transpilation_level} "
                    f"is not supported"
                )
                logger.error(message)
                raise ValueError(message)
            current_transpiled_job.append(transpiled_job)
            current_fidelities.append(fidelity)

        return (current_transpiled_job, current_fidelities)

    def _transpile_job_backend(
        self,
        job: SchedulingJob,
        backend: Backend,
    ) -> tuple[SchedulingJob | None, float]:
        """Transpile one job for one backend for QPU-level parallelism."""
        transpiled_circuits = []
        circuit_fidelities = []
        for circuit in job.circuits:
            transpiled_circuit, circuit_fidelity = self._transpile_circuit(
                circuit, backend,
            )
            transpiled_circuits.append(transpiled_circuit)
            circuit_fidelities.append(circuit_fidelity)

        if not all(circuit is not None for circuit in transpiled_circuits):
            return None, 0.0

        transpiled_job = SchedulingJob(transpiled_circuits, job.shots)
        fidelity = anp.exp(anp.log(circuit_fidelities).mean())
        return transpiled_job, float(fidelity)

    @staticmethod
    def _transpilation_worker_count(task_count: int) -> int:
        configured = int(os.environ.get(
            "QONDUCTOR_TRANSPILATION_WORKERS",
            str(multiprocessing.cpu_count()),
        ))
        return min(task_count, max(1, configured))

    def _cache_key(
        self, job: SchedulingJob, backend: Backend,
    ) -> tuple[Any, ...]:
        return (
            job.transpilation_cache_key,
            id(backend),
            self.transpilation_level.value,
            self.transpilation_count,
        )

    def _cache_get(
        self, key: tuple[Any, ...],
    ) -> tuple[SchedulingJob, float] | None:
        value = self._transpilation_cache.get(key)
        if value is not None:
            self._transpilation_cache.move_to_end(key)
        return value

    def _cache_put(
        self,
        key: tuple[Any, ...],
        value: tuple[SchedulingJob, float],
    ) -> int:
        self._transpilation_cache[key] = value
        self._transpilation_cache.move_to_end(key)
        evictions = 0
        while len(self._transpilation_cache) > self.transpilation_cache_size:
            self._transpilation_cache.popitem(last=False)
            evictions += 1
        return evictions

    @staticmethod
    def _bind_cached_job(
        template: SchedulingJob,
        source: SchedulingJob,
    ) -> SchedulingJob:
        bindings = source.parameter_bindings
        circuits = []
        for circuit in template.circuits:
            if not bindings:
                circuits.append(circuit.copy())
                continue

            parameters = {
                parameter.name: parameter for parameter in circuit.parameters
            }
            parameters.update({
                str(parameter): parameter for parameter in circuit.parameters
            })
            assignments = {
                parameter: float(value)
                for name, value in bindings.items()
                if (parameter := parameters.get(name)) is not None
            }
            if assignments:
                circuits.append(
                    circuit.assign_parameters(assignments, inplace=False)
                )
            else:
                circuits.append(circuit.copy())
        return SchedulingJob(circuits=circuits, shots=source.shots)

    def _transpile_jobs_with_cache(
        self,
        jobs: list[SchedulingJob],
        backends: list[Backend],
    ) -> tuple[list[list[SchedulingJob | None]], anp.ndarray]:
        requests = [
            (job, backend, self._cache_key(job, backend))
            for job in jobs
            for backend in backends
        ]
        resolved: dict[
            tuple[Any, ...], tuple[SchedulingJob, float] | None
        ] = {}
        misses: dict[tuple[Any, ...], tuple[SchedulingJob, Backend]] = {}
        hits = 0
        deduplicated = 0
        evictions = 0

        for job, backend, key in requests:
            cached = self._cache_get(key)
            if cached is not None:
                resolved[key] = cached
                hits += 1
            elif key in misses:
                deduplicated += 1
            else:
                misses[key] = (job, backend)

        miss_items = list(misses.items())
        tasks = [task for _, task in miss_items]
        if tasks:
            worker_count = self._transpilation_worker_count(len(tasks))
            if worker_count == 1:
                values = [
                    self._transpile_job_backend(*task) for task in tasks
                ]
            else:
                with multiprocessing.Pool(processes=worker_count) as pool:
                    values = pool.starmap(
                        self._transpile_job_backend,
                        tasks,
                    )

            for (key, _), value in zip(miss_items, values):
                transpiled_job, fidelity = value
                if transpiled_job is None:
                    resolved[key] = None
                    continue
                cached_value = (transpiled_job, fidelity)
                evictions += self._cache_put(key, cached_value)
                resolved[key] = cached_value

        transpiled_jobs: list[list[SchedulingJob | None]] = []
        fidelities = []
        backend_count = len(backends)
        for offset in range(0, len(requests), backend_count):
            row_jobs: list[SchedulingJob | None] = []
            row_fidelities = []
            for source, _, key in requests[offset:offset + backend_count]:
                cached_value = resolved.get(key)
                if cached_value is None:
                    row_jobs.append(None)
                    row_fidelities.append(0.0)
                    continue
                template, fidelity = cached_value
                row_jobs.append(self._bind_cached_job(template, source))
                row_fidelities.append(fidelity)
            transpiled_jobs.append(row_jobs)
            fidelities.append(row_fidelities)

        self._last_transpilation_cache_stats = {
            "enabled": True,
            "hits": hits,
            "misses": len(misses),
            "deduplicated": deduplicated,
            "evictions": evictions,
            "size": len(self._transpilation_cache),
            "capacity": self.transpilation_cache_size,
        }
        logger.info(
            "Transpilation cache: hits=%d misses=%d deduplicated=%d "
            "evictions=%d size=%d/%d",
            hits, len(misses), deduplicated, evictions,
            len(self._transpilation_cache), self.transpilation_cache_size,
        )
        return transpiled_jobs, anp.array(fidelities)


    def _transpile_jobs(
        self,
        jobs: list[SchedulingJob],
        backends: list[Backend],
    ) -> tuple[list[list[SchedulingJob]], anp.ndarray]:
        """
        Transpile a list of jobs for a list of backends
        :param jobs: Jobs to be transpiled
        :param backends: Quantum backends
        :return: The transpiled circuits and the fidelities
        """
        if not jobs or not backends:
            return [], anp.array([])

        # Dynamic circuits use QPU-level transpilation. Flatten the matrix so
        # a single-job batch can still transpile for several QPUs in parallel.
        if self.transpilation_level == TranspilationLevel.QPU:
            if (
                self.transpilation_cache_enabled
                and all(job.transpilation_cache_key for job in jobs)
            ):
                return self._transpile_jobs_with_cache(jobs, backends)

            self._last_transpilation_cache_stats = {
                "enabled": False,
                "hits": 0,
                "misses": 0,
                "deduplicated": 0,
                "evictions": 0,
                "size": len(self._transpilation_cache),
                "capacity": self.transpilation_cache_size,
            }
            tasks = [
                (job, backend)
                for job in jobs
                for backend in backends
            ]
            worker_count = self._transpilation_worker_count(len(tasks))
            if worker_count == 1:
                values = [
                    self._transpile_job_backend(*task) for task in tasks
                ]
            else:
                with multiprocessing.Pool(processes=worker_count) as pool:
                    values = pool.starmap(
                        self._transpile_job_backend,
                        tasks,
                    )

            backend_count = len(backends)
            transpiled_jobs = []
            fidelities = []
            for offset in range(0, len(values), backend_count):
                job_values = values[offset:offset + backend_count]
                transpiled_jobs.append([value[0] for value in job_values])
                fidelities.append([value[1] for value in job_values])
            return transpiled_jobs, anp.array(fidelities)

        transpiled_jobs = []
        fidelities = []
        values = []
        processor_types = {
            backend.name: backend.processor_type["family"]
            + str(backend.processor_type["revision"])
            + str(backend.processor_type.get("segment", ""))
            for backend in backends
        }

        worker_count = self._transpilation_worker_count(len(jobs))
        if worker_count == 1:
            values = [
                self._transpile_job(job, backends, processor_types)
                for job in jobs
            ]
        else:
            with multiprocessing.Pool(processes=worker_count) as pool:
                values = pool.starmap(
                    self._transpile_job,
                    [(job, backends, processor_types) for job in jobs],
                )
     
        for v in values:
            transpiled_jobs.append(v[0])
            fidelities.append(v[1])
        """
        for job in jobs:
            # Transpile the job for each backend
            current_transpiled_job = []
            current_fidelities = []
            processor_type_circuits = {}
            for backend in backends:
                transpiled_job = None
                fidelity = 0.0
                if (
                    self.transpilation_level
                    == TranspilationLevel.PROCESSOR_TYPE
                ):
                    # Transpile the circuit for each processor type
                    if (
                        processor_types[backend.name]
                        not in processor_type_circuits
                    ):
                        transpiled_circuits = []
                        circuit_fidelities = []
                        for circuit in job.circuits:
                            (
                                transpiled_circuit,
                                circuit_fidelity,
                            ) = self._transpile_circuit(circuit, backend)
                            transpiled_circuits.append(transpiled_circuit)
                            circuit_fidelities.append(circuit_fidelity)
                        if all(
                            transpiled_circuit is not None
                            for transpiled_circuit in transpiled_circuits
                        ):
                            transpiled_job = SchedulingJob(
                                transpiled_circuits, job.shots
                            )
                            processor_type_circuits[
                                processor_types[backend.name]
                            ] = transpiled_job
                            fidelity = anp.exp(
                                anp.log(circuit_fidelities).mean()
                            )
                    else:
                        transpiled_job = processor_type_circuits[
                            processor_types[backend.name]
                        ]
                        circuit_fidelities = []
                        for transpiled_circuit in transpiled_job.circuits:
                            circuit_fidelity = self._calculate_fidelity(
                                transpiled_circuit, backend
                            )
                            circuit_fidelities.append(circuit_fidelity)
                        fidelity = anp.exp(anp.log(circuit_fidelities).mean())
                elif self.transpilation_level == TranspilationLevel.QPU:
                    transpiled_circuits = []
                    circuit_fidelities = []
                    for circuit in job.circuits:
                        (
                            transpiled_circuit,
                            circuit_fidelity,
                        ) = self._transpile_circuit(circuit, backend)
                        transpiled_circuits.append(transpiled_circuit)
                        circuit_fidelities.append(circuit_fidelity)
                    if all(
                        transpiled_circuit is not None
                        for transpiled_circuit in transpiled_circuits
                    ):
                        transpiled_job = SchedulingJob(
                            transpiled_circuits, job.shots
                        )
                        fidelity = anp.exp(anp.log(circuit_fidelities).mean())
                elif (
                    self.transpilation_level
                    == TranspilationLevel.PRE_TRANSPILED
                ):
                    transpiled_circuits = [
                        self.pre_transpiled_circuits[
                            (backend.name, circuit.name, circuit.num_qubits)
                        ]
                        for circuit in job.circuits
                    ]
                    if all(
                        transpiled_circuit is not None
                        for transpiled_circuit in transpiled_circuits
                    ):
                        transpiled_job = SchedulingJob(
                            transpiled_circuits, job.shots
                        )
                        circuit_fidelities = []
                        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as pool:
                            circuit_fidelities = pool.starmap(self._calculate_fidelity, [(transpiled_circuit, backend) for transpiled_circuit in transpiled_job.circuits])
                        #for transpiled_circuit in transpiled_job.circuits:
                         #   circuit_fidelity = self._calculate_fidelity(
                          #      transpiled_circuit, backend
                           # )
                            #circuit_fidelities.append(circuit_fidelity)
                        fidelity = anp.exp(anp.log(circuit_fidelities).mean())
                else:
                    message = (
                        f"Transpilation level {self.transpilation_level} "
                        f"is not supported"
                    )
                    logger.error(message)
                    raise ValueError(message)
                current_transpiled_job.append(transpiled_job)
                current_fidelities.append(fidelity)

            transpiled_jobs.append(current_transpiled_job)
            fidelities.append(current_fidelities)

        """


        return transpiled_jobs, anp.array(fidelities)



    def _calculate_execution_times(
        self,
        jobs: list[list[SchedulingJob]],
        backends: [Backend],
    ) -> anp.ndarray:
        """
        Calculate the execution times for a list of jobs on a list of
        backends
        :param jobs: Transpiled jobs
        :param backends: Quantum backends
        :return: The execution times
        """
        tasks = [
            (job, backends, self.estimator)
            for job in jobs
        ]
        worker_count = self._transpilation_worker_count(len(tasks))
        if worker_count == 1:
            execution_times = [
                calculate_exeucution_time(*task) for task in tasks
            ]
        else:
            # The parent already holds the full transpiled job/backend matrix.
            # Threads keep that data shared instead of forking and serializing
            # it into a second process pool at peak memory usage.
            with ThreadPool(worker_count) as pool:
                execution_times = pool.starmap(
                    calculate_exeucution_time,
                    tasks,
                )

        return anp.array(execution_times)

    def _calculate_backend_queue_waiting_times(
        self, backends: [Backend]
    ) -> anp.ndarray:
        """
        Estimate the current job queue waiting times for a list of backends
        :param backends: Quantum backends
        :return: The waiting times
        """
        backend_queue_waiting_times = []

        for backend in backends:
            # If the backend is fake, use the patched method
            if isinstance(backend, FakeBackendV2):
                backend_queue_waiting_times.append(backend.get_waiting_time())
            # Otherwise, use the average job time for estimation
            else:
                backend_queue_waiting_times.append(backend.status().pending_jobs * self.AVERAGE_JOB_TIME)
     
        return anp.array(backend_queue_waiting_times)

    @staticmethod
    def _get_backend_sizes(backends: [Backend]) -> anp.ndarray:
        """
        Get the backend sizes for a list of backends
        :param backends: Quantum backends
        :return: The backend sizes
        """
        return anp.array([backend.num_qubits for backend in backends])

    @staticmethod
    def _get_job_sizes(jobs: list[SchedulingJob]) -> anp.ndarray:
        """
        Get the max job qubit requirements for a list of jobs
        :param jobs: Jobs
        :return: The job sizes
        """
        return anp.array(
            [
                max(circuit.num_qubits for circuit in job.circuits)
                for job in jobs
            ]
        )

    def load_pre_transpiled_circuits(
        self, backends: [Backend], benchmarks: list[str], sizes: list[int]
    ) -> None:
        """
        Load pre-transpiled circuits for a list of backends
        :param backends: Backends to load pre-transpiled circuits for
        :param benchmarks: Benchmarks to load pre-transpiled circuits for
        :param sizes: Sizes to load pre-transpiled circuits for
        """
        self.pre_transpiled_circuits = {}
        self.pre_calculated_fidelities = {}
        for backend in backends:
            for benchmark in benchmarks:
                for size in sizes:
                    self.pre_transpiled_circuits[
                        (backend.name, benchmark, size)
                    ] = load_pre_transpiled_circuit(
                        backend, benchmark_name=benchmark, benchmark_size=size
                    )
