"""First-Come First-Served (FCFS) scheduler for baseline comparison.

Implements a greedy load-balancing scheduler that assigns each job to the
backend with the shortest current waiting time. No transpilation or
optimization is performed — this serves as the baseline against which the
MultiObjectiveScheduler (Qonductor) is compared in end-to-end experiments.
"""

import datetime as dt
import logging
from typing import Any

import numpy as np
from qiskit import QuantumCircuit, transpile
from qiskit.providers import Backend

from src.scheduler.base_scheduler import (
    Assignment,
    BaseScheduler,
    SchedulingJob,
)
from src.scheduler.multi_objective_scheduler import MultiObjectiveScheduler
from src.utils.benchmark import load_pre_transpiled_circuit

logger = logging.getLogger(__name__)


class FCFSScheduler(BaseScheduler):
    """First-Come First-Served scheduler.

    Assigns each job greedily to the backend with the shortest current
    (waiting_time + estimated_execution_time).  No transpilation or
    multi-objective optimization is performed — all timing metadata fields
    are set to zero to reflect the minimal scheduling overhead.
    """

    # Execution-time estimation constants
    _MIN_EXEC_TIME = 1.0   # seconds
    _MAX_EXEC_TIME = 300.0 # seconds

    def __init__(self) -> None:
        super().__init__()
        self.pre_transpiled_circuits: dict[
            tuple[str, str, int], QuantumCircuit | None
        ] = {}

    def load_pre_transpiled_circuits(
        self, backends: list[Backend], benchmarks: list[str], sizes: list[int]
    ) -> None:
        """Pre-load transpiled circuits from the benchmark archive.

        Populates ``self.pre_transpiled_circuits`` keyed by
        ``(backend.name, benchmark_name, circuit_size)``.
        """
        self.pre_transpiled_circuits = {}
        for backend in backends:
            for benchmark in benchmarks:
                for size in sizes:
                    self.pre_transpiled_circuits[
                        (backend.name, benchmark, size)
                    ] = load_pre_transpiled_circuit(
                        backend, benchmark_name=benchmark, benchmark_size=size
                    )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def schedule(
        self,
        jobs: list[SchedulingJob],
        backends: list[Backend],
        **kwargs: Any,
    ) -> tuple[list[Assignment], list[SchedulingJob], dict[str, Any]]:
        """Schedule *jobs* onto *backends* with a greedy FCFS policy.

        Returns
        -------
        assignments : list[Assignment]
            (job, backend) pairs for successfully scheduled jobs.
        rejected_jobs : list[SchedulingJob]
            Jobs that could not be placed on any backend.
        metadata : dict
            Scheduling metadata in the same schema as
            ``MultiObjectiveScheduler.schedule`` but with zero-valued
            timing fields and a single-Pareto-point representation.
        """
        # 1.  Filter out jobs whose circuits exceed every backend  ----
        rejected_jobs = MultiObjectiveScheduler._filter_jobs(jobs, backends)
        if rejected_jobs and len(rejected_jobs) == len(jobs):
            logger.warning("All %d jobs were rejected (circuits too large).",
                           len(jobs))
            return [], jobs, self._empty_metadata()
        if rejected_jobs:
            logger.warning("%d job(s) rejected (circuits too large).",
                           len(rejected_jobs))
        feasible_jobs = [j for j in jobs if j not in rejected_jobs]

        # 2.  Per-job execution-time & fidelity estimates  ------------
        exec_times, fidelities = self._estimate_jobs(feasible_jobs, backends)

        # 3.  Greedy fidelity-first assignment  -------------------------
        assignments: list[Assignment] = []
        solution: list[int] = []
        solution_exec_times: list[float] = []
        per_job_waiting: list[float] = []
        per_job_fidelity: list[float] = []

        # Track backend busy-time totals (for metadata)
        backend_busy: dict[str, float] = {
            b.name: 0.0 for b in backends
        }

        for job_idx, job in enumerate(feasible_jobs):
            best_backend_idx = 0
            best_fidelity = -1.0
            best_tiebreaker = float("inf")

            for b_idx, backend in enumerate(backends):
                fid = fidelities[job_idx][b_idx]
                waiting = backend.get_waiting_time()
                # Primary: highest fidelity.  Tie-break: shortest waiting time.
                if fid > best_fidelity or (
                    fid == best_fidelity and waiting < best_tiebreaker
                ):
                    best_fidelity = fid
                    best_tiebreaker = waiting
                    best_backend_idx = b_idx

            chosen = backends[best_backend_idx]
            est = exec_times[job_idx][best_backend_idx]
            fid = fidelities[job_idx][best_backend_idx]

            assignments.append((job, chosen))
            solution.append(best_backend_idx)
            solution_exec_times.append(est)
            per_job_waiting.append(chosen.get_waiting_time())
            per_job_fidelity.append(fid)

            chosen.update_waiting_time(est)
            backend_busy[chosen.name] += est

        # 4.  Build metadata (FCFS format — single Pareto point)  -----
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        mean_fidelity = float(np.mean(per_job_fidelity)) if per_job_fidelity else 0.0
        mean_wait = float(np.mean(per_job_waiting)) if per_job_waiting else 0.0

        metadata: dict[str, Any] = {
            "transpilation_time": 0.0,
            "estimation_time": 0.0,
            "optimization_time": 0.0,
            "mcdm_time": 0.0,
            "schedule_generation_time": 0.0,
            "mean_error": [1.0 - mean_fidelity],
            "mean_waiting_time": [mean_wait],
            "solution_index": 0,
            "solution": solution,
            "waiting_time_90_percentile": [
                float(np.percentile(per_job_waiting, 90))
                if per_job_waiting else 0.0
            ],
            "waiting_time_95_percentile": [
                float(np.percentile(per_job_waiting, 95))
                if per_job_waiting else 0.0
            ],
            "fidelity_90_percentile": [
                float(np.percentile(per_job_fidelity, 90))
                if per_job_fidelity else 0.0
            ],
            "fidelity_95_percentile": [
                float(np.percentile(per_job_fidelity, 95))
                if per_job_fidelity else 0.0
            ],
            "solution_execution_times": solution_exec_times,
            "time": now,
        }

        return assignments, rejected_jobs, metadata

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _estimate_jobs(
        self,
        jobs: list[SchedulingJob],
        backends: list[Backend],
    ) -> tuple[list[list[float]], list[list[float]]]:
        """Return per-job, per-backend execution-time and fidelity matrices.

        Execution time is estimated with the same ``RegressionEstimator``
        used by Qonductor when pre-transpiled circuits are available.
        Fidelity is computed from pre-transpiled circuits (or on-the-fly
        transpilation as a fallback).
        """
        import numpy as np

        from src.execution_time.regression_estimator import RegressionEstimator

        estimator = RegressionEstimator()
        exec_matrix: list[list[float]] = []
        fid_matrix: list[list[float]] = []

        for job in jobs:
            job_exec: list[float] = []
            job_fid: list[float] = []
            for backend in backends:
                # Collect pre-transpiled circuits for this (job, backend)
                transpiled: list[QuantumCircuit] = []
                for c in job.circuits:
                    key = (backend.name, c.name, c.num_qubits)
                    tc = self.pre_transpiled_circuits.get(key)
                    if tc is not None:
                        transpiled.append(tc)

                # -- Execution time  -----------------------------------
                if len(transpiled) == len(job.circuits):
                    et = estimator.estimate_execution_time(
                        transpiled, backend, shots=job.shots
                    )
                    job_exec.append(et)
                else:
                    # Fall back to backend-independent heuristic
                    job_exec.append(self._estimate_execution_time(job))

                # -- Fidelity  -----------------------------------------
                fids: list[float] = []
                for i, circuit in enumerate(job.circuits):
                    tc = transpiled[i] if i < len(transpiled) else None
                    if tc is None:
                        try:
                            tc = transpile(
                                circuit, backend=backend,
                                optimization_level=1,
                            )
                        except Exception:
                            fids.append(0.9)
                            continue
                    try:
                        fids.append(
                            MultiObjectiveScheduler._calculate_fidelity(
                                tc, backend, layout=None
                            )
                        )
                    except Exception:
                        fids.append(0.9)

                job_fid.append(
                    float(np.exp(np.log(fids).mean()))
                    if fids else 0.9
                )

            exec_matrix.append(job_exec)
            fid_matrix.append(job_fid)

        return exec_matrix, fid_matrix

    @staticmethod
    def _estimate_execution_time(job: SchedulingJob) -> float:
        """Fallback heuristic when pre-transpiled circuits are unavailable.

        Used only when ``self.pre_transpiled_circuits`` is empty.
        Formula:  Σ (num_qubits × depth × shots / 150_000) per circuit,
        clipped to [``_MIN_EXEC_TIME``, ``_MAX_EXEC_TIME``].
        """
        total = 0.0
        for circuit in job.circuits:
            est = (circuit.num_qubits * circuit.depth()
                   * job.shots / 150_000.0)
            total += max(0.5, min(est, 10.0))
        return max(FCFSScheduler._MIN_EXEC_TIME,
                   min(total, FCFSScheduler._MAX_EXEC_TIME))

    @staticmethod
    def _empty_metadata() -> dict[str, Any]:
        """Return minimal metadata for the all-jobs-rejected edge case."""
        return {
            "transpilation_time": 0.0,
            "estimation_time": 0.0,
            "optimization_time": 0.0,
            "mcdm_time": 0.0,
            "schedule_generation_time": 0.0,
            "mean_error": [0.0],
            "mean_waiting_time": [0.0],
            "solution_index": 0,
            "solution": [],
            "waiting_time_90_percentile": [0.0],
            "waiting_time_95_percentile": [0.0],
            "fidelity_90_percentile": [0.0],
            "fidelity_95_percentile": [0.0],
            "solution_execution_times": [],
            "time": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
