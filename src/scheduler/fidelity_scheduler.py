import logging
from typing import Any

import numpy as np
from mapomatic import deflate_circuit, matching_layouts, evaluate_layouts
from numpy import argmin
from qiskit import transpile
from qiskit.providers import Backend

from src.scheduler.base_scheduler import (
    BaseScheduler,
    Assignment,
    SchedulingJob,
)

logger = logging.getLogger(__name__)


class FidelityScheduler(BaseScheduler):
    """
    Circuit scheduler that optimizes for fidelity
    """

    def __init__(self, transpilation_count: int = 10):
        self.transpilation_count = transpilation_count

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
        schedule = []
        rejected_jobs = []
        for job in jobs:
            logger.info("Scheduling jbo %s", job.id)
            assignment = self._schedule_job(job, backends)
            if assignment is not None:
                schedule.append(assignment)
            else:
                rejected_jobs.append(job)
        return schedule, rejected_jobs, None

    def _schedule_job(
        self, job: SchedulingJob, backends: list[Backend]
    ) -> Assignment | None:
        """
        Schedule a single circuit to a backend
        :param job: Job to be scheduled
        :param backends: Backends to be scheduled to
        :return: A tuple of (circuit, backend) pair or None if no
        backend can schedule the circuit
        """
        possible_assignments = []
        for backend in backends:
            if all(
                circuit.num_qubits <= backend.num_qubits
                for circuit in job.circuits
            ):
                backend_layouts = []
                for circuit in job.circuits:
                    transpiled_circuits = transpile(
                        [circuit] * self.transpilation_count,
                        backend=backend,
                        optimization_level=3,
                    )
                    cx_gate_counts = [
                        transpiled_circuit.count_ops()["cx"]
                        for transpiled_circuit in transpiled_circuits
                    ]
                    best_transpiled_circuit = transpiled_circuits[
                        argmin(cx_gate_counts)
                    ]
                    deflated_circuit = deflate_circuit(best_transpiled_circuit)
                    layouts = matching_layouts(
                        deflated_circuit, backend.coupling_map
                    )
                    if layouts:
                        evaluated_layouts = evaluate_layouts(
                            deflated_circuit, layouts, backend
                        )

                        backend_layouts.append(
                            (
                                evaluated_layouts[0][0],
                                deflated_circuit,
                                evaluated_layouts[0][1],
                            )
                        )
                if len(backend_layouts) == len(job.circuits):
                    possible_assignments.append(
                        (
                            backend_layouts,
                            backend,
                        )
                    )

        if not possible_assignments:
            return None

        possible_assignments.sort(
            key=lambda assignment: np.median(
                [layout[2] for layout in assignment[0]]
            )
        )

        best_assignment = possible_assignments[0]
        best_backend = best_assignment[1]
        backend_layouts = best_assignment[0]

        for i in range(len(job.circuits)):
            job.circuits[i] = transpile(
                backend_layouts[i][1],
                backend=best_backend,
                initial_layout=backend_layouts[i][0],
            )

        return job, best_backend
