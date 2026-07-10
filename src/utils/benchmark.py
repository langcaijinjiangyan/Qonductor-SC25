import datetime as dt
import logging
import pathlib
from typing import Any

import numpy
from mqt.bench import get_benchmark
from mqt.bench.utils import get_supported_benchmarks
from numpy import argmin
from qiskit import transpile, QuantumCircuit
from qiskit.providers import Backend
from qiskit.providers.fake_provider import FakeProviderForBackendV2
from qiskit.providers.fake_provider.fake_backend import FakeBackendV2
from qiskit.providers.models import BackendStatus, BackendProperties
from qiskit.transpiler.target import target_to_backend_properties
from qiskit_ibm_provider import IBMProvider
from retry import retry

from src.scheduler.base_scheduler import SchedulingJob
from src.utils.circuit_archive import (
    save_circuit_to_archive,
    load_circuit_from_archive,
)

DATA_PATH = (
    pathlib.Path(__file__).absolute().parents[2] / "data" / "benchmarks"
)
ARCHIVE_NAME = "benchmarks.zip"

logger = logging.getLogger(__name__)


def get_benchmark_names() -> list[str]:
    """
    Get all benchmark names from MQT
    :return: List of benchmark names
    """
    excluded_benchmarks = [
        "shor",
        "pricingcall",
        "pricingput",
        "groundstate",
        "routing",
        "tsp",
        "qwalk-v-chain",
        "grover-v-chain",
        "grover-noancilla",
    ]
    benchmarks = get_supported_benchmarks()
    benchmarks = set(benchmarks).difference(excluded_benchmarks)
    return sorted(benchmarks)


def get_available_backends() -> list[Backend]:
    """
    Get all available backends from IBM Quantum
    :return: List of available backends
    """
    provider = IBMProvider()
    backends = provider.backends(simulator=False, operational=True)
    return sorted(backends, key=lambda backend: backend.name)


def patch_fake_backend() -> None:
    """
    Patch the fake backend to add missing properties and methods
    """

    def update_waiting_time(self, job_waiting_time: float) -> None:
        current_timestamp = dt.datetime.now(dt.timezone.utc)
        self._waiting_time = max(
            0,
            self._waiting_time
            - (
                current_timestamp - self._waiting_time_timestamp
            ).total_seconds(),
        )
        self._waiting_time += job_waiting_time
        self._waiting_time_timestamp = current_timestamp

    def get_waiting_time(self) -> float:
        current_timestamp = dt.datetime.now(dt.timezone.utc)
        self._waiting_time = max(
            0,
            self._waiting_time
            - (
                current_timestamp - self._waiting_time_timestamp
            ).total_seconds(),
        )
        self._waiting_time_timestamp = current_timestamp
        return self._waiting_time

    def properties(self) -> BackendProperties:
        return target_to_backend_properties(self.target)

    def status(self) -> BackendStatus:
        return BackendStatus(
            backend_name=self.name,
            backend_version=self.backend_version,
            operational=True,
            pending_jobs=self.pending_jobs,
            status_msg="",
        )

    def processor_type(self) -> dict[str, Any]:
        return self._conf_dict.get("processor_type")

    FakeBackendV2.properties = properties
    FakeBackendV2.status = status
    FakeBackendV2.update_waiting_time = update_waiting_time
    FakeBackendV2.get_waiting_time = get_waiting_time
    FakeBackendV2.pending_jobs = 0
    FakeBackendV2.max_shots = 4000
    FakeBackendV2.default_rep_delay = 0.0
    FakeBackendV2._waiting_time = 0
    FakeBackendV2._waiting_time_timestamp = dt.datetime.now(dt.timezone.utc)
    FakeBackendV2.processor_type = property(processor_type)


def get_fake_backends(remove_retired: bool = False) -> list[Backend]:
    """
    Get all fake backends from Qiskit
    :param remove_retired: Whether to remove retired backends
    :return: List of fake backends
    """
    if not hasattr(FakeBackendV2, "properties"):
        patch_fake_backend()

    fake_provider = FakeProviderForBackendV2()
    available_backends = [
        "sherbrooke",
        "kyiv",
        "brisbane",
        "nazca",
        "cusco",
        "algiers",
        "kolkata",
        "mumbai",
        "cairo",
        "hanoi",
        "peekskill",
        "cleveland",
        "kawasaki",
        "kyoto",
        "osaka",
        "quebec",
        "torino",
    ]
    backends = fake_provider.backends()
    backends = [
        backend
        for backend in backends
        if backend.num_qubits >= 3
        and (not remove_retired or backend.name[5:] in available_backends)
        and backend.processor_type is not None
    ]

    return sorted(backends, key=lambda backend: backend.name)


# Session-level cache for generated benchmark circuits.
# Keys are (benchmark_name, circuit_size); each unique combination is
# generated once via the expensive ``get_benchmark`` call and then
# shallow-copied on subsequent requests.  Because ``circuit_size`` is
# bounded (3 – min_backend_size) and the benchmark name list is small
# (~20 entries), the cache never exceeds ~60 entries.
_circuit_cache: dict[tuple[str, int], QuantumCircuit] = {}


@retry()
def generate_random_job(
    min_backend_size: int,
    benchmark_names: list[str],
    random_generator: numpy.random.Generator,
    circuit_count: int,
    shots: int,
) -> SchedulingJob:
    """
    Generate a random job
    :param min_backend_size: Minimum number of qubits in the backend
    :param benchmark_names: List of benchmark names
    :param random_generator: Random number generator
    :param circuit_count: Number of circuits in the job
    :param shots: Number of shots per circuit
    :return: A random job
    """
    benchmarks = random_generator.choice(benchmark_names, size=circuit_count)
    circuits: list[QuantumCircuit] = []
    for benchmark_name in benchmarks:
        size = random_generator.integers(
            low=3, high=min_backend_size, endpoint=True
        ).item()
        cache_key = (benchmark_name, size)
        if cache_key not in _circuit_cache:
            _circuit_cache[cache_key] = get_benchmark(
                benchmark_name=benchmark_name,
                level="indep",
                circuit_size=size,
            )
        circuits.append(_circuit_cache[cache_key].copy())
    return SchedulingJob(circuits, shots)


def prepare_benchmarks(backends: list[Backend]) -> None:
    """
    Prepare benchmarks for the scheduling experiments
    by transpiling them to the backends and saving
    them to the benchmark archive
    :param backends: List of backends
    """
    benchmark_sizes = range(3, 21)
    benchmark_names = get_benchmark_names()
    DATA_PATH.mkdir(parents=True, exist_ok=True)
    transpilation_count = 5
    generation_retry_count = 5
    failed_backend_count = 5
    for benchmark_name in benchmark_names:
        for benchmark_size in benchmark_sizes:
            reached_max_retry_count = 0

            logger.info(
                "Preparing %s benchmark of size %s",
                benchmark_name,
                benchmark_size,
            )
            for i in range(generation_retry_count):
                try:
                    circuit = get_benchmark(
                        benchmark_name=benchmark_name,
                        level="indep",
                        circuit_size=benchmark_size,
                    )
                    break
                except Exception as e:
                    logger.warning(
                        "Failed to generate %s benchmark of size %s: %s",
                        benchmark_name,
                        benchmark_size,
                        e,
                    )
            else:
                logger.error(
                    "Cannot generate %s benchmark of size %s after %d retries",
                    benchmark_name,
                    benchmark_size,
                    generation_retry_count,
                )
                break
            for backend in backends:
                if backend.num_qubits >= benchmark_size:
                    for i in range(generation_retry_count):
                        try:
                            transpiled_circuits = transpile(
                                [circuit] * transpilation_count,
                                backend=backend,
                                optimization_level=3,
                            )
                            break
                        except Exception as e:
                            logger.warning(
                                "Failed to transpile %s benchmark of size %s to backend %s: %s",
                                benchmark_name,
                                benchmark_size,
                                backend.name,
                                e,
                            )
                    else:
                        logger.error(
                            "Cannot transpile %s benchmark of size %s to backend %s after %d retries",
                            benchmark_name,
                            benchmark_size,
                            backend.name,
                            generation_retry_count,
                        )
                        reached_max_retry_count += 1
                        if reached_max_retry_count == failed_backend_count:
                            logger.error(
                                "Cannot transpile %s benchmark of size %s, skipping",
                                benchmark_name,
                                benchmark_size,
                            )
                            break
                        continue
                    swap_gate = set(backend.operation_names).intersection(
                        {"cx", "cz", "ecr"}
                    )
                    if not swap_gate:
                        logger.error(
                            "Cannot find swap gate for backend %s",
                            backend.name,
                        )
                        continue
                    swap_gate = swap_gate.pop()
                    swap_gate_counts = [
                        transpiled_circuit.count_ops()[swap_gate]
                        for transpiled_circuit in transpiled_circuits
                    ]
                    best_transpiled_circuit = transpiled_circuits[
                        argmin(swap_gate_counts)
                    ]
                    backend_name = backend.name
                    if backend_name.startswith("fake_"):
                        backend_name = backend_name[5:]
                    save_circuit_to_archive(
                        best_transpiled_circuit,
                        f"{benchmark_name}_{backend_name}_{benchmark_size}.qasm",
                        ARCHIVE_NAME,
                        DATA_PATH,
                    )

            if reached_max_retry_count == failed_backend_count:
                break


def load_pre_transpiled_circuit(
    backend: Backend,
    circuit: QuantumCircuit | None = None,
    benchmark_name: str | None = None,
    benchmark_size: int | None = None,
) -> QuantumCircuit | None:
    """
    Load a pre-transpiled circuit from the benchmark archive
    :param backend: Backend to load the circuit for
    :param circuit: Circuit to load
    :param benchmark_name: Benchmark name
    :param benchmark_size: Benchmark size
    :return: Loaded circuit
    """
    if circuit is None and (benchmark_name is None or benchmark_size is None):
        raise ValueError(
            "Either circuit or benchmark name and size must be provided"
        )
    elif circuit is not None and (
        benchmark_name is not None or benchmark_size is not None
    ):
        raise ValueError(
            "Either circuit or benchmark name and size must be provided"
        )
    elif circuit is not None:
        benchmark_name = circuit.name
        benchmark_size = circuit.num_qubits

    backend_name = backend.name

    if backend_name.startswith("fake_") or backend_name.startswith("ibmq_"):
        backend_name = backend_name[5:]
    elif backend_name.startswith("ibm_"):
        backend_name = backend_name[4:]

    filename = f"{benchmark_name}_{backend_name}_{benchmark_size}.qasm"

    circuit = load_circuit_from_archive(
        DATA_PATH / ARCHIVE_NAME, filename, unpack=True
    )

    return circuit
