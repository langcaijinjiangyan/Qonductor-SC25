import logging

import pytest
from mqt.bench import get_benchmark
from qiskit import QuantumCircuit
from qiskit.providers import Backend

from src.scheduler.multi_objective_scheduler import (
    MultiObjectiveScheduler,
    TranspilationLevel,
)


@pytest.fixture
def multi_objective_scheduler(caplog):
    caplog.set_level(logging.WARNING)
    caplog.set_level(logging.DEBUG, logger="src")
    yield MultiObjectiveScheduler(
        transpilation_level=TranspilationLevel.PROCESSOR_TYPE
    )


class TestMultiObjectiveScheduler:
    def test_schedule_single_circuit_single_backend(
        self, multi_objective_scheduler, least_busy_backend, benchmark_name
    ):
        # Arrange
        circuit = get_benchmark(
            benchmark_name=benchmark_name,
            level="indep",
            circuit_size=least_busy_backend.num_qubits,
        )
        # Act
        schedule, rejected_circuits = multi_objective_scheduler.schedule(
            [circuit], [least_busy_backend]
        )
        # Assert
        assert isinstance(schedule, list)
        assert isinstance(rejected_circuits, list)
        assert len(schedule) == 1
        assert len(rejected_circuits) == 0
        assert isinstance(schedule[0], tuple)
        assert isinstance(schedule[0][0], QuantumCircuit)
        assert isinstance(schedule[0][1], Backend)
        assert schedule[0][1] == least_busy_backend

    def test_schedule_single_circuit_multiple_backends(
        self,
        multi_objective_scheduler,
        available_backends,
        benchmark_name,
        backend_size,
    ):
        # Arrange
        circuit = get_benchmark(
            benchmark_name=benchmark_name,
            level="indep",
            circuit_size=backend_size,
        )
        # Act
        schedule, rejected_circuits = multi_objective_scheduler.schedule(
            [circuit], available_backends
        )
        # Assert
        assert isinstance(schedule, list)
        assert isinstance(rejected_circuits, list)
        assert len(schedule) == 1
        assert len(rejected_circuits) == 0
        assert isinstance(schedule[0], tuple)
        assert isinstance(schedule[0][0], QuantumCircuit)
        assert isinstance(schedule[0][1], Backend)
        assert schedule[0][1] in available_backends

    def test_schedule_multiple_circuits_multiple_backends(
        self,
        multi_objective_scheduler,
        random_generator,
        available_backends,
        benchmark_names,
        circuit_count,
        backend_count,
    ):
        # Arrange
        random_backends = random_generator.choice(
            available_backends, size=backend_count, replace=False
        )
        backend_sizes = [backend.num_qubits for backend in random_backends]
        max_backend_size = max(backend_sizes)
        random_benchmarks = random_generator.choice(
            benchmark_names, size=circuit_count
        )
        circuits = [
            get_benchmark(
                benchmark_name=benchmark_name,
                level="indep",
                circuit_size=random_generator.integers(
                    low=3, high=max_backend_size, endpoint=True
                ).item(),
            )
            for benchmark_name in random_benchmarks
        ]
        # Act
        schedule, rejected_circuits = multi_objective_scheduler.schedule(
            circuits,
            random_backends,
        )
        # Assert
        assert isinstance(schedule, list)
        assert len(schedule) == circuit_count
        assert isinstance(rejected_circuits, list)
        assert len(rejected_circuits) == 0
        for assignment in schedule:
            assert isinstance(assignment, tuple)
            assert isinstance(assignment[0], QuantumCircuit)
            assert isinstance(assignment[1], Backend)
            assert assignment[1] in random_backends

    def test_schedule_unfeasible_circuit_size(
        self,
        multi_objective_scheduler,
        available_backends,
        benchmark_name,
    ):
        # Arrange
        circuit = get_benchmark(
            benchmark_name=benchmark_name,
            level="indep",
            circuit_size=max(
                [backend.num_qubits for backend in available_backends]
            )
            + 1,
        )
        # Act
        schedule, rejected_circuits = multi_objective_scheduler.schedule(
            [circuit],
            available_backends,
        )
        # Assert
        assert isinstance(schedule, list)
        assert isinstance(rejected_circuits, list)
        assert len(schedule) == 0
        assert len(rejected_circuits) == 1
