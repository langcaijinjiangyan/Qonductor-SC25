"""
QuantumSchedulerController — quantum job scheduling loop.

Watches QuantumJob CRs, accumulates pending quantum jobs, and invokes the
NSGA-II multi-objective scheduler (§7) when triggered.

Implements the paper's three-stage quantum scheduling:
    1. Job pre-processing   — filter, fetch fidelity/exec-time estimations
    2. Optimization          — NSGA-II Pareto front generation
    3. Selection             — MCDM with pseudo-weights

Triggers (from paper §7):
    - Queue size >= scheduling_threshold (default 10)
    - Time since last scheduling >= scheduling_interval (default 30 s)
"""

from __future__ import annotations

import json
import hashlib
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from src.operator.k8s_client import (
    QUANTUM_JOB_PLURAL,
    K8sClient,
    create_quantum_execution_job,
)
from src.scheduler.base_scheduler import SchedulingJob

# The multi_objective_scheduler and benchmark modules may not be importable
# if the environment lacks qiskit or mqt.bench.  Fall back gracefully.
try:
    from src.scheduler.multi_objective_scheduler import (
        MultiObjectiveScheduler,
        TranspilationLevel,
        ProblemType,
    )
    _HAS_MULTI_SCHEDULER = True
except ImportError:
    MultiObjectiveScheduler = None  # type: ignore
    TranspilationLevel = None  # type: ignore
    ProblemType = None  # type: ignore
    _HAS_MULTI_SCHEDULER = False

try:
    from src.utils.benchmark import get_fake_backends, get_benchmark_names
    _HAS_BENCHMARK = True
except ImportError:
    get_fake_backends = None  # type: ignore
    get_benchmark_names = None  # type: ignore
    _HAS_BENCHMARK = False

logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    """Convert scheduler metadata to CR-status-safe JSON values."""
    if hasattr(value, "tolist"):
        return value.tolist()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.generic):
        return value.item()
    return value


@dataclass
class _PendingQuantumJob:
    """Internal representation of a pending quantum job."""
    cr_name: str
    step_id: str
    workflow_ref: str
    qubits: int
    shots: int
    priority: str
    cr: dict
    logical_circuit_id: str = ""
    circuit_qasm: str = ""
    circuit_format: str = "qasm2"
    parameter_bindings: dict[str, float] = field(default_factory=dict)
    preferred_qpu: str = ""
    schedule_immediately: bool = False
    arrived_at: float = field(default_factory=time.monotonic)


class QuantumSchedulerController:
    """Watches QuantumJob CRs and runs NSGA-II scheduling.

    When deployed on K8s this runs as a separate controller pod or as a
    sidecar to the HybridWorkflowController.
    """

    def __init__(
        self,
        mode: str = "local",
        scheduling_interval: int = 30,
        scheduling_threshold: int = 10,
        scheduling_batch_size: int = 120,
        transpilation_cache_enabled: bool = True,
        transpilation_cache_size: int = 256,
    ) -> None:
        self.mode = mode
        self.k8s = K8sClient(mode=mode)
        self.scheduling_interval = scheduling_interval
        self.scheduling_threshold = scheduling_threshold
        self.scheduling_batch_size = max(1, scheduling_batch_size)
        self.transpilation_cache_enabled = transpilation_cache_enabled

        # The NSGA-II scheduler (reuses existing implementation).
        if _HAS_MULTI_SCHEDULER:
            self.scheduler = MultiObjectiveScheduler(
                transpilation_level=TranspilationLevel.PRE_TRANSPILED,
                problem_type=ProblemType.DISCRETE,
                transpilation_cache_enabled=transpilation_cache_enabled,
                transpilation_cache_size=transpilation_cache_size,
            )
        else:
            self.scheduler = None

        # Pending quantum jobs (paper's "job queue Q").
        self._pending: list[_PendingQuantumJob] = []
        self._known_quantum_jobs: set[str] = set()
        self._last_schedule_time = time.monotonic()
        self._stop_event = threading.Event()
        self._pending_condition = threading.Condition()
        self._schedule_lock = threading.Lock()

        # Backends — load from QPU profile ConfigMaps (published by the
        # device plugin on each quantum node).  Falls back to node-label
        # discovery or generic backends if ConfigMaps are not yet available.
        self._backends = self._load_backends_from_configmaps()
        if not self._backends:
            logger.warning(
                "No QPU profile ConfigMaps found — falling back to node "
                "label discovery (QPUBackend.generic stubs)."
            )
            self._backends = self._discover_backends_from_nodes()
        if not self._backends:
            # Minimal fallback for environments without QPU JSON profiles.
            from src.utils.qpu_backend import QPUBackend
            self._backends = []
            for i in [5, 7, 16, 27]:
                try:
                    be = QPUBackend.generic(num_qubits=i, name=f"fake_{i}q")
                    be.processor_type = {"family": "falcon", "revision": "r5.11"}
                    self._backends.append(be)
                except Exception:
                    pass

        # Pre-load transpiled circuits for speed (if scheduler available).
        if self.scheduler and _HAS_BENCHMARK:
            self.scheduler.load_pre_transpiled_circuits(
                self._backends, get_benchmark_names(), [3, 4, 5],
            )

        # Results cache for workflowResults().
        self._scheduling_results: dict[str, dict] = {}
        self._result_keys_by_cr: dict[str, str] = {}
        self._execution_jobs: dict[str, str] = {}
        self._execution_job_owners: dict[str, str] = {}

        logger.info(
            "QuantumSchedulerController ready: %d backends, "
            "interval=%ds, threshold=%d, batch_size=%d, transpilation_cache=%s, "
            "cache_size=%d",
            len(self._backends), scheduling_interval, scheduling_threshold,
            self.scheduling_batch_size,
            "enabled" if transpilation_cache_enabled else "disabled",
            transpilation_cache_size,
        )

    def _discover_backends_from_nodes(self) -> list[Any]:
        """Build schedulable fallback backends from K8s node labels."""
        try:
            from src.utils.qpu_backend import QPUBackend
        except Exception:
            logger.exception("Cannot import QPUBackend for node backends")
            return []

        backends = []
        seen: set[str] = set()
        try:
            nodes = self.k8s.get_nodes("qonductor.io/node-type=quantum")
        except Exception:
            logger.exception("Failed to discover quantum nodes from K8s")
            return []

        for node in nodes:
            node_name = node.get("metadata", {}).get("name", "")
            labels = node.get("metadata", {}).get("labels", {}) or {}
            for label_key, label_value in sorted(labels.items()):
                prefix = "qonductor.io/backend-"
                if not label_key.startswith(prefix) or label_value != "true":
                    continue
                backend_name = label_key[len(prefix):]
                if backend_name in seen:
                    continue
                try:
                    backend = QPUBackend.generic(
                        num_qubits=27, name=backend_name, node_name=node_name,
                    )
                    backend.processor_type = {
                        "family": "qonductor-node-label",
                        "revision": "fallback",
                    }
                    backends.append(backend)
                    seen.add(backend_name)
                    logger.info(
                        "Discovered QPU backend '%s' from node '%s'",
                        backend_name, node_name,
                    )
                except Exception:
                    logger.exception(
                        "Failed to create fallback backend '%s'",
                        backend_name,
                    )
        return sorted(backends, key=lambda b: b.name)

    def _load_backends_from_configmaps(self) -> list[Any]:
        """Load QPU backends from ``qpu-profile-*`` ConfigMaps.

        Each quantum node's device plugin publishes its local QPU JSON
        profiles as ConfigMaps labelled ``app=qonductor,component=qpu-profile``.
        This method discovers them cluster-wide and constructs full
        ``QPUBackend`` objects with real calibration data (coupling map,
        gate errors, T1/T2, readout errors) and the correct ``node_name``
        for K8s node affinity.

        Returns:
            A (possibly empty) list of ``QPUBackend`` instances sorted by name.
        """
        try:
            from src.utils.qpu_backend import QPUBackend
        except ImportError:
            logger.exception("Cannot import QPUBackend")
            return []

        try:
            configmaps = self.k8s.list_configmaps(
                label_selector="app=qonductor,component=qpu-profile",
            )
        except Exception:
            logger.exception("Failed to list QPU profile ConfigMaps")
            return []

        backends: list[Any] = []
        for cm in configmaps:
            try:
                cm_data = cm.get("data", {})
                profile_json = cm_data.get("profile", "")
                if not profile_json:
                    logger.warning(
                        "ConfigMap '%s' has no 'profile' key — skipping",
                        cm.get("metadata", {}).get("name", ""),
                    )
                    continue
                data = json.loads(profile_json)
                node_name = cm_data.get("node_name", "")
                backend = QPUBackend.from_dict(data, node_name=node_name)
                backends.append(backend)
                try:
                    num_edges = backend.coupling_map.size()
                except (TypeError, AttributeError):
                    num_edges = len(list(backend.coupling_map.get_edges()))
                logger.info(
                    "Loaded QPU backend '%s' from ConfigMap (node=%s, "
                    "%d qubits, %d edges)",
                    backend.name, node_name, backend.num_qubits,
                    num_edges,
                )
            except Exception:
                logger.exception(
                    "Failed to load QPU backend from ConfigMap '%s'",
                    cm.get("metadata", {}).get("name", ""),
                )

        return sorted(backends, key=lambda b: b.name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the controller loop. Blocks until stop()."""
        logger.info("QuantumSchedulerController starting (mode=%s)", self.mode)

        # Process existing pending QuantumJobs.
        for qj in self.k8s.list_cr(QUANTUM_JOB_PLURAL):
            if qj.get("status", {}).get("phase") in ("Pending", None):
                self._enqueue(qj)

        scheduler_thread = threading.Thread(
            target=self._scheduling_loop,
            name="quantum-scheduling-worker",
            daemon=True,
        )
        scheduler_thread.start()

        try:
            # Keep consuming events while scheduling runs independently.
            watcher = self.k8s.watch_cr(QUANTUM_JOB_PLURAL)
            for event in watcher:
                if self._stop_event.is_set():
                    break
                ev_type = event.get("type", "")
                obj = event.get("object", {})

                phase = obj.get("status", {}).get("phase", "")
                if ev_type in ("ADDED", "MODIFIED"):
                    if phase in ("", "Pending"):
                        self._enqueue(obj)
                    elif phase in ("Completed", "Failed", "Cancelled"):
                        self._forget_quantum_job(obj)

                elif ev_type == "DELETED":
                    self._forget_quantum_job(obj)

                elif ev_type == "HEARTBEAT":
                    self._check_trigger()
        finally:
            self.stop()
            scheduler_thread.join(timeout=1.0)

        logger.info("QuantumSchedulerController stopped")

    def stop(self) -> None:
        self._stop_event.set()
        with self._pending_condition:
            self._pending_condition.notify_all()

    def schedule_pending_now(self) -> dict | None:
        """Force an immediate scheduling cycle (useful for tests)."""
        return self._safe_run_scheduling_cycle()

    def get_result(self, step_id: str) -> dict | None:
        """Retrieve the scheduling result for a specific quantum step."""
        return self._scheduling_results.get(step_id)

    def get_execution_job(self, step_id: str) -> str | None:
        """Return the K8s Job name that executes *step_id*, or None."""
        ej = getattr(self, "_execution_jobs", {})
        return ej.get(step_id)

    def get_results_for_workflow(self, workflow_ref: str) -> list[dict]:
        """Retrieve all results for a given workflow."""
        return [
            r for r in self._scheduling_results.values()
            if r.get("workflow_ref") == workflow_ref
        ]

    def add_backend(self, backend) -> None:
        """Register an additional QPU backend."""
        self._backends.append(backend)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _enqueue(self, cr: dict) -> None:
        """Add a QuantumJob CR to the pending queue."""
        spec = cr.get("spec", {})
        cr_name = cr["metadata"]["name"]
        with self._pending_condition:
            if cr_name in self._known_quantum_jobs:
                logger.debug(
                    "QuantumJob %s already pending; skipping duplicate event",
                    cr_name,
                )
                return

            qj = _PendingQuantumJob(
                cr_name=cr_name,
                step_id=spec.get("stepId", ""),
                workflow_ref=spec.get("workflowRef", ""),
                qubits=spec.get("qubits", 10),
                shots=spec.get("shots", 4000),
                priority=spec.get("priority", "balanced"),
                cr=cr,
                logical_circuit_id=spec.get("logicalCircuitId", ""),
                circuit_qasm=spec.get("circuitQasm", ""),
                circuit_format=spec.get("circuitFormat", "qasm2"),
                parameter_bindings=spec.get("parameterBindings", {}) or {},
                preferred_qpu=spec.get("preferredQPU", ""),
                schedule_immediately=bool(
                    spec.get("scheduleImmediately", False)
                ),
            )
            self._pending.append(qj)
            self._known_quantum_jobs.add(cr_name)
            self._pending_condition.notify()
        logger.debug("Enqueued QuantumJob %s (%d qubits)", qj.step_id, qj.qubits)

    def _cache_result(self, qj: _PendingQuantumJob, result: dict) -> None:
        """Cache an in-flight result with ownership information for cleanup."""
        result["quantum_job"] = qj.cr_name
        self._scheduling_results[qj.step_id] = result
        self._result_keys_by_cr[qj.cr_name] = qj.step_id

    def _cache_execution_job(
        self, qj: _PendingQuantumJob, execution_job: str,
    ) -> None:
        self._execution_jobs[qj.step_id] = execution_job
        self._execution_job_owners[qj.step_id] = qj.cr_name

    def _forget_quantum_job(self, cr: dict) -> None:
        """Release queue and cache state for a terminal or deleted QuantumJob."""
        cr_name = cr.get("metadata", {}).get("name", "")
        if not cr_name:
            return

        with self._pending_condition:
            self._known_quantum_jobs.discard(cr_name)
            self._pending[:] = [
                qj for qj in self._pending if qj.cr_name != cr_name
            ]

        step_id = self._result_keys_by_cr.pop(cr_name, None)
        if not step_id:
            step_id = cr.get("spec", {}).get("stepId", "")
        if not step_id:
            return

        result = self._scheduling_results.get(step_id)
        if result and result.get("quantum_job") == cr_name:
            self._scheduling_results.pop(step_id, None)
        if self._execution_job_owners.get(step_id) == cr_name:
            self._execution_job_owners.pop(step_id, None)
            self._execution_jobs.pop(step_id, None)

    def _check_trigger(self) -> None:
        """Wake the scheduling worker to re-evaluate its trigger conditions."""
        with self._pending_condition:
            self._pending_condition.notify()

    def _scheduling_loop(self) -> None:
        """Wait for trigger conditions and run scheduling outside the watch."""
        while not self._stop_event.is_set():
            trigger_message = ""
            trigger_args: tuple[Any, ...] = ()

            with self._pending_condition:
                while not self._stop_event.is_set():
                    queue_size = len(self._pending)
                    if queue_size == 0:
                        self._pending_condition.wait()
                        continue

                    elapsed = time.monotonic() - self._last_schedule_time
                    if any(qj.schedule_immediately for qj in self._pending):
                        trigger_message = (
                            "Trigger: scheduleImmediately requested, "
                            "queue size=%d"
                        )
                        trigger_args = (queue_size,)
                        break
                    if queue_size >= self.scheduling_threshold:
                        trigger_message = "Trigger: queue size %d >= threshold %d"
                        trigger_args = (queue_size, self.scheduling_threshold)
                        break
                    if elapsed >= self.scheduling_interval:
                        trigger_message = (
                            "Trigger: interval %.0fs elapsed, queue size=%d"
                        )
                        trigger_args = (elapsed, queue_size)
                        break

                    self._pending_condition.wait(
                        timeout=max(0.0, self.scheduling_interval - elapsed),
                    )

            if self._stop_event.is_set():
                break

            logger.info(trigger_message, *trigger_args)
            self._safe_run_scheduling_cycle()

    def _safe_run_scheduling_cycle(self) -> dict | None:
        """Run one scheduling cycle without letting exceptions kill the thread."""
        with self._schedule_lock:
            try:
                return self._run_scheduling_cycle()
            except Exception:
                logger.exception(
                    "Quantum scheduling cycle failed; controller will keep running",
                )
                return None

    def _take_pending_batch(
        self,
    ) -> tuple[list[_PendingQuantumJob], int]:
        """Remove one bounded FIFO batch and return its remaining queue size."""
        with self._pending_condition:
            if not self._pending:
                return [], 0
            batch_size = min(len(self._pending), self.scheduling_batch_size)
            pending = self._pending[:batch_size]
            del self._pending[:batch_size]
            self._last_schedule_time = time.monotonic()
            return pending, len(self._pending)

    def _build_scheduling_job(self, qj: _PendingQuantumJob) -> SchedulingJob:
        """Convert a QuantumJob CR into the scheduler's native job object."""
        circuit = self._load_circuit_for_job(
            qj, bind_parameters=not self.transpilation_cache_enabled,
        )
        if not self.transpilation_cache_enabled:
            return SchedulingJob(circuits=[circuit], shots=qj.shots)

        if qj.circuit_qasm:
            cache_material = (
                f"{(qj.circuit_format or 'qasm2').lower()}\0"
                f"{qj.circuit_qasm}"
            ).encode("utf-8")
        else:
            cache_material = f"fallback\0{qj.qubits}".encode("ascii")
        cache_key = hashlib.sha256(cache_material).hexdigest()
        self._validate_parameter_bindings(circuit, qj.parameter_bindings)
        return SchedulingJob(
            circuits=[circuit],
            shots=qj.shots,
            transpilation_cache_key=cache_key,
            parameter_bindings=dict(qj.parameter_bindings),
        )

    def _load_circuit_for_job(
        self, qj: _PendingQuantumJob, *, bind_parameters: bool = True,
    ):
        """Load a real circuit from QuantumJob spec, or use a fallback."""
        if qj.circuit_qasm:
            circuit = self._load_circuit_from_qasm(
                qj.circuit_qasm, qj.circuit_format,
            )
            if bind_parameters:
                circuit = self._bind_parameters(
                    circuit, qj.parameter_bindings,
                )
            circuit.name = qj.logical_circuit_id or f"{qj.step_id}_circuit"
            return circuit

        # Backward-compatible fallback for older tests and CRs that only
        # specify a qubit count.
        from qiskit import QuantumCircuit
        circuit = QuantumCircuit(qj.qubits)
        circuit.h(range(qj.qubits))
        circuit.measure_all()
        circuit.name = f"{qj.step_id}_circuit"
        return circuit

    @staticmethod
    def _load_circuit_from_qasm(qasm_text: str, circuit_format: str):
        fmt = (circuit_format or "qasm2").lower()
        if fmt == "qasm3":
            from qiskit import qasm3
            return qasm3.loads(qasm_text)

        from qiskit import qasm2, qasm3
        try:
            return qasm2.loads(qasm_text)
        except Exception:
            logger.debug("Falling back to QASM3 parser for circuit payload")
            return qasm3.loads(qasm_text)

    @staticmethod
    def _bind_parameters(circuit, bindings: dict[str, float]):
        if not bindings:
            return circuit

        params_by_name = {p.name: p for p in circuit.parameters}
        params_by_string = {str(p): p for p in circuit.parameters}
        assignments = {}
        unknown: list[str] = []
        for name, value in bindings.items():
            param = params_by_name.get(name) or params_by_string.get(name)
            if param is None:
                unknown.append(name)
                continue
            assignments[param] = float(value)

        if unknown:
            logger.warning(
                "Ignoring parameter bindings not present in circuit: %s",
                ", ".join(sorted(unknown)),
            )
        if not assignments:
            return circuit

        if hasattr(circuit, "assign_parameters"):
            return circuit.assign_parameters(assignments, inplace=False)
        return circuit.bind_parameters(assignments)

    @staticmethod
    def _validate_parameter_bindings(
        circuit, bindings: dict[str, float],
    ) -> None:
        if not bindings:
            return
        parameters = {p.name: p for p in circuit.parameters}
        parameters.update({str(p): p for p in circuit.parameters})
        unknown = sorted(name for name in bindings if name not in parameters)
        if unknown:
            logger.warning(
                "Ignoring parameter bindings not present in circuit: %s",
                ", ".join(unknown),
            )
        assignments = {
            parameter: float(value)
            for name, value in bindings.items()
            if (parameter := parameters.get(name)) is not None
        }
        if assignments:
            circuit.assign_parameters(assignments, inplace=False)

    def _run_scheduling_cycle(self) -> dict | None:
        """Execute one quantum scheduling cycle.

        Implements the three-stage pipeline from paper §7:
        1. Pre-processing: filter jobs, fetch estimations
        2. Optimization: NSGA-II Pareto front
        3. Selection: MCDM pseudo-weights
        """
        # Take one bounded batch while the watch thread continues accepting jobs.
        pending, remaining_pending = self._take_pending_batch()
        if not pending:
            return None

        logger.info(
            "Scheduling cycle: %d quantum jobs, %d remaining pending",
            len(pending),
            remaining_pending,
        )

        # ---- Stage 1: Pre-processing ----
        # Convert QuantumJob CRs into SchedulingJob objects.  Newer CRs may
        # carry inline QASM/QASM3 and parameter bindings; older CRs keep the
        # placeholder-circuit fallback for compatibility.
        scheduling_jobs = []
        valid_pending: list[_PendingQuantumJob] = []
        cycle_results: dict[str, dict] = {}
        for qj in pending:
            try:
                scheduling_jobs.append(self._build_scheduling_job(qj))
                valid_pending.append(qj)
            except Exception as exc:
                logger.exception(
                    "Failed to materialize circuit for QuantumJob %s",
                    qj.cr_name,
                )
                result_entry = {
                    "step_id": qj.step_id,
                    "workflow_ref": qj.workflow_ref,
                    "assigned_qpu": "none",
                    "status": "Failed",
                    "error": str(exc),
                }
                self._cache_result(qj, result_entry)
                cycle_results[qj.step_id] = result_entry
                try:
                    self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj.cr_name, {
                        "phase": "Failed",
                        "conditions": [{
                            "type": "CircuitMaterializationFailed",
                            "status": "True",
                            "reason": str(exc),
                        }],
                    })
                except Exception:
                    logger.exception(
                        "Failed to mark QuantumJob %s as materialization failed",
                        qj.cr_name,
                    )

        pending = valid_pending
        if not pending:
            return cycle_results

        # ---- Stage 2: Optimization (NSGA-II) ----
        if self.scheduler is None or not self._backends:
            # No scheduler available → assign to first backend (degraded mode).
            logger.warning("No NSGA-II scheduler — using degraded greedy assignment")
            assignments = []
            for i, qj in enumerate(pending):
                if self._backends:
                    assignments.append((scheduling_jobs[i], self._backends[0]))
            rejected = pending[len(assignments):]
            metadata = {}
        else:
            if any(qj.circuit_qasm for qj in pending) and TranspilationLevel is not None:
                # Dynamic workflow circuits are not part of the pre-transpiled
                # benchmark cache, so transpile them against each QPU backend.
                self.scheduler.transpilation_level = TranspilationLevel.QPU

            # Priority → MCDM weights.
            priority_weights = {
                "fidelity": [1.0, 0.0],
                "jct": [0.0, 1.0],
                "balanced": [0.5, 0.5],
            }
            weights = priority_weights.get(
                pending[0].priority if pending else "balanced",
                [0.5, 0.5],
            )
            assignments, rejected, metadata = self.scheduler.schedule(
                scheduling_jobs, self._backends, weights=weights,
            )
        metadata = _json_safe(metadata or {})

        # ---- Stage 3: Selection (MCDM) ----
        # Assign QPU, update CR, and dispatch K8s Job for circuit execution.
        # Extract per-job estimates from scheduler metadata
        solution_fidelities = metadata.get("solution_fidelities", [])
        solution_exec_times = metadata.get("solution_execution_times", [])
        scheduler_metadata = {
            **metadata,
            "backend_names": [getattr(backend, "name", "") for backend in self._backends],
            "job_count": len(pending),
            "assigned_count": len(assignments),
            "rejected_count": len(pending) - len(assignments),
        }

        now_ts = _now_iso()
        for idx, ((job, backend), qj) in enumerate(
            zip(assignments, pending)
        ):
            if qj.preferred_qpu:
                preferred_backend = next(
                    (
                        candidate
                        for candidate in self._backends
                        if candidate.name == qj.preferred_qpu
                    ),
                    None,
                )
                if preferred_backend is None:
                    reason = (
                        f"preferred QPU {qj.preferred_qpu!r} is not available"
                    )
                    logger.error("QuantumJob %s: %s", qj.cr_name, reason)
                    self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj.cr_name, {
                        "phase": "Failed",
                        "conditions": [{
                            "type": "PreferredQPUUnavailable",
                            "status": "True",
                            "reason": reason,
                        }],
                    })
                    continue
                backend = preferred_backend

            est_fidelity = (
                solution_fidelities[idx]
                if idx < len(solution_fidelities) else 0.0
            )
            est_exec_time = (
                solution_exec_times[idx]
                if idx < len(solution_exec_times) else 0.0
            )

            result_entry = {
                "step_id": qj.step_id,
                "workflow_ref": qj.workflow_ref,
                "assigned_qpu": backend.name if backend else "none",
                "status": "Scheduled",
                "scheduled_at": now_ts,
                "estimated_fidelity": est_fidelity,
                "estimated_execution_time": est_exec_time,
                "arrived_at": qj.arrived_at,
                "scheduling_metadata": scheduler_metadata,
            }
            self._cache_result(qj, result_entry)
            cycle_results[qj.step_id] = result_entry

            # Update QuantumJob CR status to Scheduled with estimates
            try:
                self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj.cr_name, {
                    "phase": "Scheduled",
                    "assignedQPU": backend.name if backend else "none",
                    "scheduledAt": now_ts,
                    "estimatedFidelity": est_fidelity,
                    "estimatedExecutionTime": est_exec_time,
                    "schedulingMetadata": scheduler_metadata,
                })
            except Exception:
                logger.exception(
                    "Failed to update scheduled status for QuantumJob %s; "
                    "re-queueing for a later scheduling cycle",
                    qj.cr_name,
                )
                with self._pending_condition:
                    self._pending.append(qj)
                    self._pending_condition.notify()
                continue

            # Dispatch a K8s Job to execute the circuit on the assigned
            # QPU's quantum worker node (via nodeAffinity).
            try:
                exec_job_name = create_quantum_execution_job(
                    scheduling_job=job,
                    assigned_backend=backend,
                    quantum_job_cr=qj.cr,
                    mode=self.mode,
                    client_override=self.k8s,
                )
                self._cache_execution_job(qj, exec_job_name)
                try:
                    self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj.cr_name, {
                        "executionJob": exec_job_name,
                    })
                except Exception:
                    logger.exception(
                        "Failed to record execution Job '%s' on QuantumJob %s",
                        exec_job_name,
                        qj.cr_name,
                    )
                logger.info(
                    "Dispatched quantum execution Job '%s' for step '%s' "
                    "→ QPU '%s' (node=%s)",
                    exec_job_name, qj.step_id, backend.name,
                    getattr(backend, "node_name", "<none>"),
                )
            except Exception:
                logger.exception(
                    "Failed to create K8s Job for quantum step '%s'",
                    qj.step_id,
                )
                try:
                    self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj.cr_name, {
                        "phase": "Failed",
                        "conditions": [{
                            "type": "ExecutionJobCreationFailed",
                            "status": "True",
                            "reason": "Could not create quantum execution Job",
                        }],
                    })
                except Exception:
                    logger.exception(
                        "Failed to mark QuantumJob %s as failed",
                        qj.cr_name,
                    )

        for qj in pending[len(assignments):]:
            result_entry = {
                "step_id": qj.step_id,
                "workflow_ref": qj.workflow_ref,
                "assigned_qpu": "none",
                "status": "Rejected",
            }
            self._cache_result(qj, result_entry)
            cycle_results[qj.step_id] = result_entry
            try:
                self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj.cr_name, {
                    "phase": "Failed",
                    "conditions": [{"type": "Rejected",
                                    "status": "True",
                                    "reason": "No suitable QPU found"}],
                })
            except Exception:
                logger.exception(
                    "Failed to mark rejected QuantumJob %s as failed",
                    qj.cr_name,
                )

        logger.info(
            "Scheduling complete: %d assigned, %d rejected, "
            "mean_error=%.4f, mean_wait=%.1f",
            len(assignments),
            len(pending) - len(assignments),
            metadata.get("mean_error", [0])[0] if metadata else 0,
            metadata.get("mean_waiting_time", [0])[0] if metadata else 0,
        )

        return cycle_results

    # ------------------------------------------------------------------
    # Backend management
    # ------------------------------------------------------------------

    @property
    def backends(self):
        return self._backends

    @backends.setter
    def backends(self, value):
        self._backends = value


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
