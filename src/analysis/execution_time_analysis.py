import cProfile
import logging
import os
import pstats
import random
import time
from collections import defaultdict
from pathlib import Path
from timeit import default_timer as timer

import numpy
import numpy as np
from mqt.bench import get_benchmark
from qiskit import transpile
from qiskit.providers import Backend
from qiskit.providers.jobstatus import JOB_FINAL_STATES, JobStatus
from supermarq import converters
from supermarq.benchmarks.vqe_proxy import VQEProxy

from src.execution_time.calibration_estimator import CalibrationEstimator
from src.execution_time.gate_length_estimator import GateLengthEstimator
from src.execution_time.pass_manager_estimator import PassManagerEstimator
from src.execution_time.regression_estimator import RegressionEstimator
from src.execution_time.schedule_estimator import ScheduleEstimator
from src.utils.benchmark import get_benchmark_names, generate_random_job
from src.utils.database import (
    Job,
    get_jobs_from_database,
    save_job_to_database,
)

logger = logging.getLogger(__name__)


def count_similar_jobs(
    jobs: [Job], backend: Backend, benchmarks: [str], shots: int
):
    """
    Count the number of jobs that have the same parameters
    :param jobs: Existing jobs
    :param backend: Quantum backend
    :param benchmarks: List of benchmarks
    :param shots: Number of shots
    :return: Number of similar jobs
    """
    count = 0
    for job in jobs:
        if (
            job.backend.name == backend.name
            and len(job.job_circuits) == len(benchmarks)
            and {
                job_circuit.circuit.benchmark_name
                for job_circuit in job.job_circuits
            }
            == set(benchmarks)
            and job.shots == shots
        ):
            count += 1
    return count


def run_comprehensive_analysis(backends: list[Backend]):
    """
    Run a comprehensive analysis of the execution time estimation
    :param backends: List of quantum backends
    """
    # Set up the analysis parameters
    benchmarks = get_benchmark_names()
    shots = [
        1000,
        4000,
        8000,
        12000,
        16000,
        20000,
    ]
    circuit_counts = [20, 40, 60, 80, 100]
    max_job_runs = 10
    # Get already executed jobs
    existing_jobs = get_jobs_from_database()

    backend_jobs = defaultdict(list)
    backend_queue = defaultdict(list)

    backends = {backend.name: backend for backend in backends}

    random.seed(15072023)

    for current_shots in shots:
        for benchmark in benchmarks:
            for backend_name, backend in backends.items():
                left_jobs = max_job_runs - count_similar_jobs(
                    existing_jobs, backend, [benchmark], current_shots
                )
                for _ in range(left_jobs):
                    backend_queue[backend_name].append(
                        ([benchmark], current_shots)
                    )

    for circuit_count in circuit_counts:
        random_benchmarks = random.choices(benchmarks, k=circuit_count)
        for current_shots in shots:
            for backend_name, backend in backends.items():
                left_jobs = max_job_runs - count_similar_jobs(
                    existing_jobs, backend, random_benchmarks, current_shots
                )
                for _ in range(left_jobs):
                    backend_queue[backend_name].append(
                        (random_benchmarks, current_shots)
                    )

    # Run the jobs until the queue is empty
    while len(backend_queue) > 0:
        # Check if the jobs are finished
        for i, job in enumerate(backend_jobs[backend_name]):
            job_status = None
            while job_status is None:
                try:
                    job_status = job.status()
                except Exception:
                    logger.warning(
                        'Failed to get job status for job "%s"', job
                    )
                    time.sleep(10)
            if job_status == JobStatus.DONE:
                logger.info(
                    "Job %s on %s backend is finished",
                    job.job_id(),
                    backend_name,
                )
                save_job_to_database(job)
                backend_jobs[backend_name].pop(i)
            elif job_status in JOB_FINAL_STATES:
                logger.info(
                    "Job %s on %s backend failed: %s",
                    job.job_id(),
                    backend_name,
                    job.error_message(),
                )
                backend_jobs[backend_name].pop(i)

        # Run the iteration for each backend
        for backend_name, job_parameters in list(backend_queue.items()):
            # Check if the backend is ready to run a new job
            while (
                len(backend_jobs[backend_name]) < max_job_runs
                and len(job_parameters) > 0
            ):
                benchmarks, current_shots = job_parameters.pop()
                logger.info(
                    "Running %s benchmark on %s backend with %d shots",
                    benchmarks,
                    backend_name,
                    current_shots,
                )

                backend = backends[backend_name]

                circuits = [
                    get_benchmark(
                        benchmark_name=benchmark,
                        level="indep",
                        circuit_size=backend.num_qubits,
                    )
                    for benchmark in benchmarks
                ]

                circuits = [
                    transpile(circuit, backend=backend, optimization_level=3)
                    for circuit in circuits
                ]

                job = backend.run(
                    circuits,
                    shots=current_shots,
                )
                backend_jobs[backend.name].append(job)

            # Remove the backends that have no jobs left
            if (
                len(backend_jobs[backend_name]) == 0
                and len(backend_queue[backend_name]) == 0
            ):
                backend_jobs.pop(backend_name)
                backend_queue.pop(backend_name)
        time.sleep(60)


def compare_time_estimations(
    backend: Backend, shots: int, depths: list[int]
) -> (list[float], list[float], list[float], list[float], list[int]):
    """
    Compare the time estimations of different methods
    :param backend: Quantum backend to be estimated on
    :param shots: Number of shots
    :param depths: List of circuit depths
    :return: Estimated execution times of different methods
    """
    gate_length_estimated_times = []
    calibration_estimated_times = []
    pass_manager_estimated_times = []
    schedule_estimated_times = []
    gate_length_estimator = GateLengthEstimator()
    calibration_estimator = CalibrationEstimator()
    pass_manager_estimator = PassManagerEstimator()
    schedule_estimator = ScheduleEstimator()

    real_depth = []
    for i in depths:
        vqe = VQEProxy(
            num_qubits=backend.configuration().num_qubits, num_layers=i
        )
        circuit = converters.cirq_to_qiskit(vqe.circuit()[0])
        circuit = transpile(circuit, backend=backend, optimization_level=3)
        real_depth.append(circuit.depth())
        start = timer()
        gate_length_estimated_time = (
            gate_length_estimator.estimate_circuit_execution_time(
                circuit, backend
            )
            * shots
        )
        gate_length_estimated_time_end = timer()
        calibration_estimated_time = (
            calibration_estimator.estimate_circuit_execution_time(
                circuit, backend
            )
            * shots
        )
        calibration_estimated_time_end = timer()
        pass_manager_estimated_time = (
            pass_manager_estimator.estimate_circuit_execution_time(
                circuit, backend
            )
            * shots
        )
        pass_manager_estimated_time_end = timer()
        schedule_estimated_time = (
            schedule_estimator.estimate_circuit_execution_time(
                circuit, backend
            )
            * shots
        )
        schedule_estimated_time_end = timer()
        logger.info(
            "Gate length estimation time: %f",
            gate_length_estimated_time_end - start,
        )
        logger.info(
            "Calibration estimation time: %f",
            calibration_estimated_time_end - gate_length_estimated_time_end,
        )
        logger.info(
            "Pass manager estimation time: %f",
            pass_manager_estimated_time_end - calibration_estimated_time_end,
        )
        logger.info(
            "Schedule estimation time: %f",
            schedule_estimated_time_end - pass_manager_estimated_time_end,
        )
        gate_length_estimated_times.append(gate_length_estimated_time)
        calibration_estimated_times.append(calibration_estimated_time)
        pass_manager_estimated_times.append(pass_manager_estimated_time)
        schedule_estimated_times.append(schedule_estimated_time)

    return (
        gate_length_estimated_times,
        calibration_estimated_times,
        pass_manager_estimated_times,
        schedule_estimated_times,
        real_depth,
    )


def run_benchmark_varying_circuit_depth(
    backend: Backend, shots: int, depths: list[int], run_counts: int
) -> ([float], [float], [int]):
    """
    Run a VQE benchmark on a backend with varying circuit depth
    :param backend: Quantum backend to run the benchmark on
    :param shots: Number of shots
    :param depths: List of circuit depths
    :param run_counts: Number of times to run the benchmark for each depth
    :return: Taken times, estimated times, real depths
    """
    taken_times = defaultdict(list)
    estimated_times = []
    estimator = GateLengthEstimator()
    real_depths = []
    for i in depths:
        vqe = VQEProxy(num_qubits=backend.num_qubits, num_layers=i)
        circuit = converters.cirq_to_qiskit(vqe.circuit()[0])
        circuit = transpile(circuit, backend=backend, optimization_level=3)
        estimated_time = estimator.estimate_execution_time(
            [circuit], backend, shots=shots
        )
        logger.info(
            "Executing VQE benchmark on %s backend with %d shots and %d depth %d times",
            backend.name,
            shots,
            i,
            run_counts,
        )
        real_depths.append(circuit.depth())
        estimated_times.append(estimated_time)
        logger.info("Estimated time: %f s", estimated_time)
        jobs = []
        for _ in range(run_counts):
            job = backend.run(circuit, shots=shots)
            jobs.append(job)
        for job in jobs:
            result = job.result()
            taken_times[i].append(result.time_taken)
            logger.info("Taken time: %f s", result.time_taken)
    return (
        [np.average(taken_times[i]) for i in depths],
        estimated_times,
        real_depths,
    )


def run_benchmark_varying_circuit_counts(
    backend: Backend,
    shots: int,
    circuit_counts: list[int],
) -> (list[float], list[float]):
    """
    Run a benchmark on a backend with varying circuit counts
    :param backend: Quantum backend to run the benchmark on
    :param shots: Number of shots
    :param circuit_counts: List of circuit counts in a job
    :return: Taken times and estimated times
    """
    estimator = RegressionEstimator()
    benchmark_names = get_benchmark_names()
    seed = int(os.environ.get("SEED", time.time()))
    random_generator = numpy.random.default_rng(seed)
    logger.info("Random seed: %d", seed)
    max_jobs = 3

    taken_times = defaultdict(list)
    estimated_times = defaultdict(list)
    for i in circuit_counts:
        logger.info(
            "Executing %d benchmarks on %s backend with %d shots",
            i,
            backend.name,
            shots,
        )
        for _ in range(5):
            benchmarks = random_generator.choice(benchmark_names, size=i)
            logger.info("Benchmarks: %s", benchmarks)
            job_circuits = [
                transpile(
                    get_benchmark(
                        benchmark_name=benchmark_name,
                        level="indep",
                        circuit_size=random_generator.integers(
                            low=3, high=backend.num_qubits, endpoint=True
                        ).item(),
                    ),
                    backend=backend,
                    optimization_level=3,
                )
                for benchmark_name in benchmarks
            ]
            job_circuits = job_circuits[: backend.max_circuits]

            estimated_time = estimator.estimate_execution_time(
                job_circuits, backend, shots=shots
            )
            estimated_times[i].append(estimated_time)
            logger.info("Estimated time: %f s", estimated_time)
            jobs = []
            for _ in range(max_jobs):
                job = backend.run(job_circuits, shots=shots)
                jobs.append(job)
            for job in jobs:
                result = job.result()
                taken_times[i].append(result.time_taken)
                logger.info("Taken time: %f s", result.time_taken)
    return [np.mean(taken_times[i]) for i in circuit_counts], [
        np.mean(estimated_times[i]) for i in circuit_counts
    ]


def run_all_benchmarks_varying_shots(
    backend: Backend,
    shots: [int],
) -> (list[float], list[float]):
    """
    Run all benchmarks on a backend with varying circuit counts
    :param backend: Quantum backend to run the benchmark on
    :param shots: Number of shots
    count
    :return: Taken times and estimated times
    """
    benchmarks = get_benchmark_names()
    circuits = [
        get_benchmark(
            benchmark_name=benchmark,
            level="indep",
            circuit_size=backend.num_qubits,
        )
        for benchmark in benchmarks
    ]
    circuits = [
        transpile(circuit, backend=backend, optimization_level=3)
        for circuit in circuits[:5]
    ]
    taken_times = defaultdict(list)
    estimated_times = []
    estimator = GateLengthEstimator()

    for i in shots:
        for j, circuit in enumerate(circuits):
            logger.info(
                "Executing %s benchmark on %s backend with %d shots",
                circuit.name,
                backend.name,
                i,
            )
            estimated_time = estimator.estimate_execution_time(
                [circuit], backend, shots=i
            )
            estimated_times.append(estimated_time)
            logger.info("Estimated time: %f s", estimated_time)
            job = backend.run(circuit, shots=i)
            result = job.result()
            taken_times[i].append(result.time_taken)
            logger.info("Taken time: %f s", result.time_taken)
    return [np.average(taken_times[i]) for i in shots], estimated_times


def analyze_regression_estimator_performance(
    backend: Backend, shots: int, job_counts: list[int], profiling_folder: Path
) -> list[float]:
    """
    Analyze the performance of the regression estimator
    :param backend: Backend to be estimated on
    :param shots: Number of shots
    :param job_counts: List of job counts
    :param profiling_folder: Folder to save the profiling results
    :return: List of estimation times
    """
    regression_estimator = RegressionEstimator()
    benchmark_names = get_benchmark_names()
    seed = int(os.environ.get("SEED", time.time()))
    random_generator = numpy.random.default_rng(seed)
    logger.info("Random seed: %d", seed)
    profiling_files = []
    estimation_times = defaultdict(list)

    for i in job_counts:
        logger.info(
            "Estimating %d jobs",
            i,
        )
        for _ in range(10):
            circuit_count = int(random_generator.normal(50, 20))
            circuit_count = max(1, min(circuit_count, 100))
            jobs = [
                generate_random_job(
                    backend.num_qubits,
                    benchmark_names,
                    random_generator,
                    circuit_count=circuit_count,
                    shots=4000,
                )
                for _ in range(i)
            ]
            for job in jobs:
                job.circuits = transpile(
                    job.circuits, backend=backend, optimization_level=3
                )
            profile = cProfile.Profile()
            start = timer()
            profile.enable()
            for job in jobs:
                regression_estimator.estimate_execution_time(
                    job.circuits, backend, shots=shots
                )
            profile.disable()
            end = timer()
            profiling_file = (
                profiling_folder
                / f"regression_estimator_{int(time.time())}.prof"
            )

            profile.dump_stats(str(profiling_file))
            profiling_files.append(profiling_file)
            estimation_times[i].append(end - start)

    combined_stats = pstats.Stats(str(profiling_files[0]))
    for profiling_file in profiling_files[1:]:
        combined_stats.add(str(profiling_file))

    combined_stats.dump_stats(
        str(
            profiling_folder
            / f"regression_estimator_combined_{int(time.time())}.prof"
        ),
    )
    for profiling_file in profiling_files:
        profiling_file.unlink()

    return [np.mean(estimation_times[i]) for i in job_counts]
