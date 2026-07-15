from qiskit import QuantumCircuit, schedule as build_schedule
from qiskit.providers import Backend

from src.execution_time.circuit_estimator import CircuitEstimator


class ScheduleEstimator(CircuitEstimator):
    """
    Class for estimating job execution time using circuit schedules
    """

    def estimate_circuit_execution_time(
        self, circuit: QuantumCircuit, backend: Backend
    ) -> float:
        """
        Estimate the execution time of a single circuit on a backend using
        circuit schedule
        :param circuit: Circuit to be estimated
        :param backend: Backend to be estimated on
        :return: Estimated execution time
        """
        schedule = build_schedule(circuit, backend=backend)
        execution_time = schedule.duration * backend.dt
        return execution_time
