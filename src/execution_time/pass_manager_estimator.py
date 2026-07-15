from collections import defaultdict

from qiskit import QuantumCircuit
from qiskit.providers import Backend
from qiskit.transpiler import PassManager
from qiskit_ibm_provider.transpiler.passes.scheduling import (
    DynamicCircuitInstructionDurations,
    ALAPScheduleAnalysis,
)

from src.execution_time.circuit_estimator import CircuitEstimator


class PassManagerEstimator(CircuitEstimator):
    """
    Class for estimating job execution time using pass manager
    """

    def estimate_circuit_execution_time(
        self, circuit: QuantumCircuit, backend: Backend
    ) -> float:
        """
        Estimate the execution time of a single circuit on a backend using
        pass manager
        :param circuit: Circuit to be estimated
        :param backend: Backend to be estimated on
        :return: Estimated execution time
        """
        durations = DynamicCircuitInstructionDurations.from_backend(backend)
        pm = PassManager([ALAPScheduleAnalysis(durations)])
        pm.run(circuit)
        node_start_times = pm.property_set["node_start_time"]

        block_durations = defaultdict(int)
        for inst, (block, t0) in node_start_times.items():
            block_durations[block] = max(
                block_durations[block], t0 + inst.op.duration
            )

        execution_time = sum(block_durations.values()) * backend.dt

        return execution_time
