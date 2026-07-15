import logging
import os
import time

import numpy
import pytest
from mqt.bench.utils import get_supported_benchmarks
from qiskit.providers.ibmq import least_busy
from qiskit_ibm_provider import IBMProvider

logging.getLogger("qiskit").setLevel(logging.WARNING)
logging.getLogger("qiskit_ibm_provider").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_all_benchmark_names():
    excluded_benchmarks = [
        "shor",
        "pricingcall",
        "pricingput",
        "groundstate",
        "routing",
        "tsp",
        "qwalk-v-chain",
        "grover-v-chain",
    ]
    benchmarks = get_supported_benchmarks()
    benchmarks = set(benchmarks).difference(excluded_benchmarks)
    return sorted(benchmarks)


@pytest.fixture(params=get_all_benchmark_names())
def benchmark_name(request):
    yield request.param


@pytest.fixture(scope="module")
def benchmark_names():
    yield get_all_benchmark_names()


def get_available_backends():
    provider = IBMProvider()
    backends = provider.backends(simulator=False, operational=True)
    return sorted(backends, key=lambda backend: backend.name)


@pytest.fixture(scope="module")
def available_backends():
    yield get_available_backends()


@pytest.fixture(scope="module")
def least_busy_backend(available_backends):
    yield least_busy(available_backends)


@pytest.fixture(
    params=set([backend.num_qubits for backend in get_available_backends()])
)
def backend_size(request):
    yield request.param


@pytest.fixture(
    params=[i for i in range(1, len(get_available_backends()) * 2 + 1)]
)
def circuit_count(request):
    yield request.param


@pytest.fixture(
    params=[i for i in range(1, len(get_available_backends()) + 1)]
)
def backend_count(request):
    yield request.param


@pytest.fixture(scope="module")
def random_generator():
    seed = int(os.environ.get("SEED", time.time()))
    generator = numpy.random.default_rng(seed)
    logging.getLogger().info("Random seed: %d", seed)
    yield generator
