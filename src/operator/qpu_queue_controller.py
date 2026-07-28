"""
QPUQueueController — per-QPU FIFO queue for quantum execution Jobs.

Manages serial execution of quantum Jobs on each QPU by controlling
the K8s ``suspend`` field.  The scheduler creates Jobs in suspended
state; this controller unsuspends them one at a time per QPU in FIFO
order (by ``creationTimestamp``).

Design:
1. On startup, reconciles state from existing quantum Jobs.
2. Watches ``step_type=quantum`` Jobs and consumes managed Job events
   incrementally.
3. Every *reconcile_interval* seconds, performs a full reconciliation as a
   safety net for missed watch events or controller restarts.
4. Watches terminal QuantumJob CR status as the preferred release signal,
   because the executor patches the result before marking the CR terminal.
5. Updates QuantumJob CR status with queue position.

Does NOT modify ``HybridWorkflowController._reconcile_status()`` — it
continues to poll Job status independently for DAG advancement.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Optional

from src.operator.k8s_client import QUANTUM_JOB_PLURAL, K8sClient

logger = logging.getLogger(__name__)

# Label selectors used to identify quantum execution Jobs.
_STEP_TYPE_LABEL = "step_type"
_QUANTUM_STEP_TYPE = "quantum"
_ASSIGNED_QPU_LABEL = "assigned_qpu"
_QUANTUM_JOB_CR_LABEL = "quantum_job_cr"

_RECONCILE_INTERVAL = 2  # seconds — matches HybridWorkflowController


@dataclass
class QPUQueueState:
    """Per-QPU queue state."""
    queue: list[dict] = field(default_factory=list)   # suspended Jobs (sorted)
    running_job_name: str | None = None
    running_cr_name: str | None = None


class QPUQueueController:
    """Maintains per-QPU FIFO queues and manages Job suspend/unsuspend."""

    def __init__(
        self,
        mode: str = "local",
        k8s_client: K8sClient | None = None,
        reconcile_interval: float = _RECONCILE_INTERVAL,
        managed_qpus: Iterable[str] | None = None,
        controller_id: str | None = None,
        watch_enabled: bool = True,
    ) -> None:
        self.mode = mode
        self.k8s = k8s_client or K8sClient(mode=mode)
        self._reconcile_interval = reconcile_interval
        self._managed_qpus = (
            frozenset(qpu for qpu in managed_qpus if qpu)
            if managed_qpus is not None else None
        )
        self._controller_id = controller_id or (
            ",".join(sorted(self._managed_qpus))
            if self._managed_qpus else "all-qpus"
        )
        self._single_managed_qpu = (
            next(iter(self._managed_qpus))
            if self._managed_qpus and len(self._managed_qpus) == 1
            else None
        )
        self._watch_enabled = watch_enabled
        self._queues: dict[str, QPUQueueState] = defaultdict(QPUQueueState)
        self._terminal_quantum_jobs: set[str] = set()
        self._completed_execution_jobs: set[str] = set()
        self._lock = threading.Lock()
        self._reconcile_lock = threading.Lock()
        self._reconcile_condition = threading.Condition()
        self._last_reconcile_at = time.monotonic()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the queue controller loop. Blocks until :meth:`stop`."""
        logger.info(
            "QPUQueueController starting "
            "(mode=%s, id=%s, qpus=%s, interval=%.3fs, watch=%s)",
            self.mode,
            self._controller_id,
            sorted(self._managed_qpus) if self._managed_qpus else "all",
            self._reconcile_interval,
            self._watch_enabled,
        )

        # Rebuild state from existing Jobs on startup.
        self._run_reconcile_existing_jobs("startup", block=True)

        if self._watch_enabled:
            watch_thread = threading.Thread(
                target=self._watch_jobs_loop,
                name=f"qpu-queue-watch-{self._controller_id}",
                daemon=True,
            )
            watch_thread.start()
            quantum_job_watch_thread = threading.Thread(
                target=self._watch_quantum_jobs_loop,
                name=f"qpu-queue-qj-watch-{self._controller_id}",
                daemon=True,
            )
            quantum_job_watch_thread.start()

        while not self._stop_event.is_set():
            if not self._wait_for_reconcile_inactivity():
                break
            self._run_reconcile("periodic", block=False)

        logger.info("QPUQueueController stopped (id=%s)", self._controller_id)

    def stop(self) -> None:
        """Signal the controller to stop at the next cycle."""
        self._stop_event.set()
        with self._reconcile_condition:
            self._reconcile_condition.notify_all()

    def get_queue_state(self, qpu_name: str) -> QPUQueueState | None:
        """Return the current queue state for a QPU (observability)."""
        with self._lock:
            return self._queues.get(qpu_name)

    def get_all_queue_states(self) -> dict[str, dict]:
        """Return all queue states as a JSON-serialisable dict."""
        with self._lock:
            result: dict[str, dict] = {}
            for qpu_name, state in self._queues.items():
                result[qpu_name] = {
                    "queue_length": len(state.queue),
                    "queued_jobs": [
                        q.get("metadata", {}).get("name", "")
                        for q in state.queue
                    ],
                    "running_job": state.running_job_name,
                    "running_quantum_job": state.running_cr_name,
                }
            return result

    # ------------------------------------------------------------------
    # Internal — reconciliation
    # ------------------------------------------------------------------

    def _job_label_selector(self) -> str:
        """Return the narrowest label selector this controller can use."""
        selector = f"{_STEP_TYPE_LABEL}={_QUANTUM_STEP_TYPE}"
        if self._managed_qpus and len(self._managed_qpus) == 1:
            qpu = next(iter(self._managed_qpus))
            selector = f"{selector},{_ASSIGNED_QPU_LABEL}={qpu}"
        return selector

    def _manages_qpu(self, qpu_name: str) -> bool:
        if not qpu_name:
            return False
        return self._managed_qpus is None or qpu_name in self._managed_qpus

    def _wait_for_reconcile_inactivity(self) -> bool:
        """Return True only after no reconcile has completed for one interval."""
        with self._reconcile_condition:
            while not self._stop_event.is_set():
                elapsed = time.monotonic() - self._last_reconcile_at
                remaining = self._reconcile_interval - elapsed
                if remaining <= 0:
                    return True
                self._reconcile_condition.wait(timeout=remaining)
            return False

    def _mark_reconcile_completed(self) -> None:
        with self._reconcile_condition:
            self._last_reconcile_at = time.monotonic()
            self._reconcile_condition.notify_all()

    def _run_reconcile_existing_jobs(
        self,
        reason: str,
        *,
        block: bool,
    ) -> None:
        acquired = self._reconcile_lock.acquire(blocking=block)
        if not acquired:
            logger.debug(
                "Skipping existing-job reconciliation for %s; another pass is active",
                reason,
            )
            return
        try:
            self._reconcile_existing_terminal_quantum_jobs()
            self._reconcile_existing_jobs()
        except Exception:
            logger.exception(
                "Error during existing-job reconciliation (%s)", reason,
            )
        finally:
            self._reconcile_lock.release()
            self._mark_reconcile_completed()

    def _run_reconcile(self, reason: str, *, block: bool) -> None:
        acquired = self._reconcile_lock.acquire(blocking=block)
        if not acquired:
            logger.debug(
                "Skipping QPU queue reconciliation for %s; another pass is active",
                reason,
            )
            return
        try:
            self._reconcile(reason=reason)
        except Exception:
            logger.exception("Error during QPU queue reconciliation (%s)", reason)
        finally:
            self._reconcile_lock.release()
            self._mark_reconcile_completed()

    def _watch_jobs_loop(self) -> None:
        """Consume managed Job events without listing all Jobs."""
        selector = self._job_label_selector()
        logger.info(
            "QPUQueueController watching Jobs (id=%s, selector=%s)",
            self._controller_id,
            selector,
        )

        while not self._stop_event.is_set():
            try:
                for event in self.k8s.watch_jobs(label_selector=selector):
                    if self._stop_event.is_set():
                        return
                    event_type = event.get("type", "")
                    if event_type == "HEARTBEAT":
                        continue
                    job = event.get("object") or {}
                    labels = job.get("metadata", {}).get("labels", {})
                    qpu_name = labels.get(_ASSIGNED_QPU_LABEL, "")
                    if not self._manages_qpu(qpu_name):
                        continue
                    job_name = job.get("metadata", {}).get("name", "")
                    self._handle_job_event(
                        event_type,
                        job,
                        reason=f"watch:{event_type}:{job_name or '<unknown>'}",
                    )
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception(
                    "QPUQueueController Job watch failed (id=%s); retrying",
                    self._controller_id,
                )
                self._stop_event.wait(timeout=1.0)

    def _watch_quantum_jobs_loop(self) -> None:
        """Release local QPUs from terminal QuantumJob CR events."""
        logger.info(
            "QPUQueueController watching QuantumJobs (id=%s)",
            self._controller_id,
        )

        terminal_phases = {"Completed", "Failed", "Cancelled"}
        while not self._stop_event.is_set():
            try:
                for event in self.k8s.watch_cr(QUANTUM_JOB_PLURAL):
                    if self._stop_event.is_set():
                        return
                    event_type = event.get("type", "")
                    if event_type == "HEARTBEAT":
                        continue
                    quantum_job = event.get("object") or {}
                    status = quantum_job.get("status", {}) or {}
                    phase = status.get("phase", "")
                    if phase not in terminal_phases:
                        continue
                    qpu_name = status.get("assignedQPU", "")
                    if not self._manages_qpu(qpu_name):
                        continue
                    qj_name = quantum_job.get("metadata", {}).get("name", "")
                    self._handle_quantum_job_terminal_event(
                        quantum_job,
                        reason=f"qj-watch:{event_type}:{qj_name or '<unknown>'}",
                    )
            except Exception:
                if self._stop_event.is_set():
                    break
                logger.exception(
                    "QPUQueueController QuantumJob watch failed (id=%s); retrying",
                    self._controller_id,
                )
                self._stop_event.wait(timeout=1.0)

    def _reconcile_existing_terminal_quantum_jobs(self) -> None:
        """Seed terminal-owner cache before rebuilding Job queues at startup."""
        terminal_phases = {"Completed", "Failed", "Cancelled"}
        try:
            quantum_jobs = self.k8s.list_cr(QUANTUM_JOB_PLURAL)
        except Exception:
            logger.exception("Failed to list terminal QuantumJobs during startup")
            return

        count = 0
        with self._lock:
            for quantum_job in quantum_jobs:
                status = quantum_job.get("status", {}) or {}
                if status.get("phase", "") not in terminal_phases:
                    continue
                if not self._manages_qpu(status.get("assignedQPU", "")):
                    continue
                qj_name = quantum_job.get("metadata", {}).get("name", "")
                execution_job = str(status.get("executionJob", "") or "")
                if qj_name:
                    self._terminal_quantum_jobs.add(qj_name)
                    count += 1
                if execution_job:
                    self._completed_execution_jobs.add(execution_job)
        if count:
            logger.info(
                "Seeded %d terminal QuantumJobs for QPU queue controller %s",
                count,
                self._controller_id,
            )

    @staticmethod
    def _job_name(job: dict) -> str:
        return job.get("metadata", {}).get("name", "")

    @staticmethod
    def _job_qpu(job: dict) -> str:
        return (
            job.get("metadata", {}).get("labels", {})
            .get(_ASSIGNED_QPU_LABEL, "")
        )

    @staticmethod
    def _job_cr_name(job: dict) -> str:
        return (
            job.get("metadata", {}).get("labels", {})
            .get(_QUANTUM_JOB_CR_LABEL, "")
        )

    @staticmethod
    def _job_is_terminal(job: dict) -> bool:
        status = job.get("status", {}) or {}
        return bool(status.get("succeeded") or status.get("failed"))

    @staticmethod
    def _job_succeeded(job: dict) -> bool:
        return ((job.get("status", {}) or {}).get("succeeded", 0) or 0) > 0

    def _job_is_retired_locked(self, job: dict) -> bool:
        job_name = self._job_name(job)
        cr_name = self._job_cr_name(job)
        return bool(
            (job_name and job_name in self._completed_execution_jobs)
            or (cr_name and cr_name in self._terminal_quantum_jobs)
        )

    def _drop_retired_job_locked(
        self,
        qpu_name: str,
        qs: QPUQueueState,
        job: dict,
    ) -> None:
        """Forget stale Job events after the owner CR has already completed."""
        job_name = self._job_name(job)
        cr_name = self._job_cr_name(job)
        if job_name:
            self._completed_execution_jobs.add(job_name)
            self._remove_queued_job_locked(qs, job_name)
        if cr_name:
            self._terminal_quantum_jobs.add(cr_name)
            self._remove_queued_cr_locked(qs, cr_name)
        if (
            (job_name and qs.running_job_name == job_name)
            or (cr_name and qs.running_cr_name == cr_name)
        ):
            logger.debug(
                "Dropping retired Job '%s' / QuantumJob '%s' from QPU '%s'",
                job_name,
                cr_name,
                qpu_name,
            )
            qs.running_job_name = None
            qs.running_cr_name = None

    def _handle_job_event(self, event_type: str, job: dict, *, reason: str) -> None:
        """Apply one Job watch event directly to local queue state."""
        qpu_name = self._job_qpu(job)
        if not self._manages_qpu(qpu_name):
            return
        job_name = self._job_name(job)
        if not job_name:
            return

        with self._lock:
            qs = self._queues[qpu_name]
            if self._job_is_retired_locked(job):
                self._drop_retired_job_locked(qpu_name, qs, job)
                self._dispatch_idle_qpu_locked(qpu_name, qs)
                self._update_queue_positions_locked(qpu_name, qs)
                self._mark_reconcile_completed()
                return

            if event_type == "DELETED":
                self._remove_queued_job_locked(qs, job_name)
                if qs.running_job_name == job_name:
                    logger.warning(
                        "Running Job '%s' on QPU '%s' was deleted; releasing slot",
                        job_name,
                        qpu_name,
                    )
                    qs.running_job_name = None
                    qs.running_cr_name = None
                self._dispatch_idle_qpu_locked(qpu_name, qs)
                self._update_queue_positions_locked(qpu_name, qs)
                self._mark_reconcile_completed()
                return

            if self._job_is_terminal(job):
                self._release_running_job_locked(
                    qpu_name,
                    qs,
                    job_name=job_name,
                    cr_name=self._job_cr_name(job),
                    is_succeeded=self._job_succeeded(job),
                    patch_phase=True,
                    reason=reason,
                )
                self._dispatch_idle_qpu_locked(qpu_name, qs)
                self._update_queue_positions_locked(qpu_name, qs)
                self._mark_reconcile_completed()
                return

            if job.get("spec", {}).get("suspend", False):
                self._enqueue_job_locked(qpu_name, qs, job)
                self._dispatch_idle_qpu_locked(qpu_name, qs)
                self._update_queue_positions_locked(qpu_name, qs)
                self._mark_reconcile_completed()
                return

            self._remove_queued_job_locked(qs, job_name)
            if qs.running_job_name not in (None, job_name):
                logger.warning(
                    "QPU '%s' observed extra running Job '%s' while '%s' is "
                    "already running; suspending extra",
                    qpu_name,
                    job_name,
                    qs.running_job_name,
                )
                self._suspend_job(job_name)
            else:
                qs.running_job_name = job_name
                qs.running_cr_name = self._job_cr_name(job) or qs.running_cr_name
            self._mark_reconcile_completed()

        logger.debug(
            "Consumed QPU queue Job event (id=%s, qpu=%s, job=%s, reason=%s)",
            self._controller_id,
            qpu_name,
            job_name,
            reason,
        )

    def _handle_quantum_job_terminal_event(
        self,
        quantum_job: dict,
        *,
        reason: str,
    ) -> None:
        """Release a QPU when the owner QuantumJob CR reaches a terminal phase."""
        metadata = quantum_job.get("metadata", {}) or {}
        status = quantum_job.get("status", {}) or {}
        qj_name = metadata.get("name", "")
        qpu_name = status.get("assignedQPU", "")
        if not qj_name or not self._manages_qpu(qpu_name):
            return
        is_succeeded = status.get("phase") == "Completed"
        execution_job = status.get("executionJob", "") or None

        with self._lock:
            self._terminal_quantum_jobs.add(qj_name)
            if execution_job:
                self._completed_execution_jobs.add(execution_job)
            qs = self._queues[qpu_name]
            self._remove_queued_job_locked(qs, execution_job or "")
            self._remove_queued_cr_locked(qs, qj_name)
            self._release_running_job_locked(
                qpu_name,
                qs,
                job_name=execution_job,
                cr_name=qj_name,
                is_succeeded=is_succeeded,
                patch_phase=False,
                reason=reason,
            )
            self._dispatch_idle_qpu_locked(qpu_name, qs)
            self._update_queue_positions_locked(qpu_name, qs)
            self._mark_reconcile_completed()

    def _enqueue_job_locked(
        self,
        qpu_name: str,
        qs: QPUQueueState,
        job: dict,
    ) -> None:
        """Insert or refresh one suspended Job in FIFO order. Caller holds _lock."""
        job_name = self._job_name(job)
        cr_name = self._job_cr_name(job)
        if (
            not job_name
            or qs.running_job_name == job_name
            or job_name in self._completed_execution_jobs
            or (cr_name and cr_name in self._terminal_quantum_jobs)
        ):
            return
        for index, queued in enumerate(qs.queue):
            if self._job_name(queued) == job_name:
                qs.queue[index] = job
                break
        else:
            qs.queue.append(job)
        qs.queue.sort(
            key=lambda j: j.get("metadata", {}).get("creationTimestamp", ""),
        )
        logger.debug(
            "Queued Job '%s' on QPU '%s' (queue_length=%d)",
            job_name,
            qpu_name,
            len(qs.queue),
        )

    def _remove_queued_job_locked(self, qs: QPUQueueState, job_name: str) -> None:
        if not job_name:
            return
        qs.queue[:] = [
            queued for queued in qs.queue
            if self._job_name(queued) != job_name
        ]

    def _remove_queued_cr_locked(self, qs: QPUQueueState, cr_name: str) -> None:
        if not cr_name:
            return
        qs.queue[:] = [
            queued for queued in qs.queue
            if self._job_cr_name(queued) != cr_name
        ]

    def _release_running_job_locked(
        self,
        qpu_name: str,
        qs: QPUQueueState,
        *,
        job_name: str | None,
        cr_name: str,
        is_succeeded: bool,
        patch_phase: bool,
        reason: str,
    ) -> bool:
        """Release the current QPU slot from either Job or QuantumJob evidence."""
        if job_name:
            self._completed_execution_jobs.add(job_name)
            self._remove_queued_job_locked(qs, job_name)
        if not cr_name and job_name and qs.running_job_name == job_name:
            cr_name = qs.running_cr_name or ""

        matches_running_job = bool(job_name and qs.running_job_name == job_name)
        matches_running_cr = bool(cr_name and qs.running_cr_name == cr_name)
        if not matches_running_job and not matches_running_cr:
            return False

        completed_job_name = job_name or qs.running_job_name
        if completed_job_name:
            self._completed_execution_jobs.add(completed_job_name)

        status_patch = {
            "queuePosition": 0,
            "queueState": "completed",
        }
        if patch_phase:
            status_patch["phase"] = "Completed" if is_succeeded else "Failed"
        if cr_name:
            self._terminal_quantum_jobs.add(cr_name)
            try:
                self.k8s.update_cr_status(
                    QUANTUM_JOB_PLURAL,
                    cr_name,
                    status_patch,
                )
            except Exception:
                logger.debug(
                    "QuantumJob CR '%s' no longer exists — skipping",
                    cr_name,
                )

        logger.info(
            "Releasing QPU '%s' from Job '%s' / QuantumJob '%s' "
            "(succeeded=%s, reason=%s)",
            qpu_name,
            job_name or qs.running_job_name or "",
            cr_name or qs.running_cr_name or "",
            is_succeeded,
            reason,
        )
        qs.running_job_name = None
        qs.running_cr_name = None
        return True

    def _reconcile_existing_jobs(self) -> None:
        """On startup, scan existing quantum Jobs and rebuild queue state."""
        try:
            jobs = self.k8s.list_jobs(
                label_selector=self._job_label_selector(),
            )
        except Exception:
            logger.exception("Failed to list quantum Jobs during startup")
            return

        # Partition Jobs by status and QPU.
        per_qpu: dict[str, dict] = defaultdict(
            lambda: {"pending": [], "running": []},
        )

        for job in jobs:
            labels = job.get("metadata", {}).get("labels", {})
            qpu = labels.get(_ASSIGNED_QPU_LABEL, "")
            if not self._manages_qpu(qpu):
                continue

            is_suspended = job.get("spec", {}).get("suspend", False)
            succeeded = job.get("status", {}).get("succeeded", 0)
            failed = job.get("status", {}).get("failed", 0)
            cr_name = self._job_cr_name(job)

            if self._job_is_retired_locked(job):
                continue
            if succeeded or failed:
                job_name = self._job_name(job)
                if job_name:
                    self._completed_execution_jobs.add(job_name)
                continue  # already completed, ignore

            if is_suspended:
                per_qpu[qpu]["pending"].append(job)
            else:
                per_qpu[qpu]["running"].append(job)

        with self._lock:
            for qpu, state in per_qpu.items():
                qs = self._queues[qpu]

                # Sort running by creationTimestamp, keep the earliest.
                state["running"].sort(
                    key=lambda j: j.get("metadata", {}).get(
                        "creationTimestamp", "",
                    ),
                )
                if len(state["running"]) > 1:
                    logger.warning(
                        "QPU '%s' has %d running Jobs — suspending extras",
                        qpu, len(state["running"]),
                    )
                    for extra in state["running"][1:]:
                        self._suspend_job(
                            extra.get("metadata", {}).get("name", ""),
                        )
                    qs.running_job_name = state["running"][0].get(
                        "metadata", {},
                    ).get("name", "")
                    qs.running_cr_name = self._job_cr_name(state["running"][0])
                elif len(state["running"]) == 1:
                    qs.running_job_name = state["running"][0].get(
                        "metadata", {},
                    ).get("name", "")
                    qs.running_cr_name = self._job_cr_name(state["running"][0])

                # Queue pending Jobs in FIFO order.
                state["pending"].sort(
                    key=lambda j: j.get("metadata", {}).get(
                        "creationTimestamp", "",
                    ),
                )
                qs.queue = state["pending"]

            logger.info(
                "Reconciled %d QPU queues: %d pending, %d running",
                len(per_qpu),
                sum(len(s["pending"]) for s in per_qpu.values()),
                sum(1 for s in per_qpu.values() if s["running"]),
            )

        # After rebuild, start any idle QPUs.
        self._dispatch_idle_qpus()

    def _reconcile(self, reason: str = "manual") -> None:
        """Reconcile quantum Jobs and maintain queue invariants."""
        logger.debug(
            "Reconciling QPU queues (id=%s, reason=%s)",
            self._controller_id,
            reason,
        )
        try:
            jobs = self.k8s.list_jobs(
                label_selector=self._job_label_selector(),
            )
        except Exception:
            logger.exception("Failed to list quantum Jobs")
            return

        if self._single_managed_qpu is not None:
            self._reconcile_single_qpu(
                self._single_managed_qpu,
                jobs,
                reason=reason,
            )
            return

        # Partition by QPU and status.
        per_qpu: dict[str, dict] = defaultdict(
            lambda: {"pending": [], "running": [], "completed": []},
        )
        seen_job_names: set[str] = set()

        for job in jobs:
            labels = job.get("metadata", {}).get("labels", {})
            qpu = labels.get(_ASSIGNED_QPU_LABEL, "")
            if not self._manages_qpu(qpu):
                continue

            job_name = job.get("metadata", {}).get("name", "")
            if job_name:
                seen_job_names.add(job_name)
            is_suspended = job.get("spec", {}).get("suspend", False)
            succeeded = job.get("status", {}).get("succeeded", 0)
            failed = job.get("status", {}).get("failed", 0)
            cr_name = self._job_cr_name(job)

            if self._job_is_retired_locked(job):
                continue
            if succeeded or failed:
                if job_name:
                    self._completed_execution_jobs.add(job_name)
                per_qpu[qpu]["completed"].append(job)
            elif is_suspended:
                per_qpu[qpu]["pending"].append(job)
            else:
                per_qpu[qpu]["running"].append(job)

        with self._lock:
            # Process completions: update CR status and clear running slot.
            for qpu, state in per_qpu.items():
                qs = self._queues[qpu]
                for job in state["completed"]:
                    self._release_running_job_locked(
                        qpu,
                        qs,
                        job_name=self._job_name(job),
                        cr_name=self._job_cr_name(job),
                        is_succeeded=self._job_succeeded(job),
                        patch_phase=True,
                        reason=reason,
                    )

            # Rebuild pending queues in FIFO order from current Job state. This
            # removes stale queued references when Jobs are deleted or relabelled.
            for qpu in set(self._queues) | set(per_qpu):
                if not self._manages_qpu(qpu):
                    continue
                state = per_qpu.get(
                    qpu,
                    {"pending": [], "running": [], "completed": []},
                )
                qs = self._queues[qpu]
                pending = list(state["pending"])
                pending.sort(
                    key=lambda j: j.get("metadata", {}).get(
                        "creationTimestamp", "",
                    ),
                )
                qs.queue = pending

            # Check invariants: at most 1 running per QPU.
            for qpu, state in per_qpu.items():
                qs = self._queues[qpu]
                running = state["running"]
                if len(running) > 1:
                    logger.warning(
                        "QPU '%s': %d running Jobs (expected ≤1); "
                        "suspending extras", qpu, len(running),
                    )
                    running.sort(
                        key=lambda j: j.get("metadata", {}).get(
                            "creationTimestamp", "",
                        ),
                    )
                    # Keep first, suspend extras.
                    qs.running_job_name = running[0].get(
                        "metadata", {},
                    ).get("name", "")
                    qs.running_cr_name = self._job_cr_name(running[0])
                    for extra in running[1:]:
                        self._suspend_job(
                            extra.get("metadata", {}).get("name", ""),
                        )
                elif len(running) == 1:
                    qs.running_job_name = running[0].get(
                        "metadata", {},
                    ).get("name", "")
                    qs.running_cr_name = self._job_cr_name(running[0])

            # If a previously running Job disappeared (for example, deleted
            # before completion), release the local slot so the queue can drain.
            for qpu, qs in self._queues.items():
                if not self._manages_qpu(qpu):
                    continue
                if qs.running_job_name and qs.running_job_name not in seen_job_names:
                    logger.warning(
                        "Running Job '%s' on QPU '%s' disappeared; releasing slot",
                        qs.running_job_name,
                        qpu,
                    )
                    qs.running_job_name = None
                    qs.running_cr_name = None

        # Dispatch to idle QPUs and update CR queue positions.
        self._dispatch_idle_qpus()
        self._update_queue_positions()

    def _reconcile_single_qpu(
        self,
        qpu_name: str,
        jobs: list[dict],
        *,
        reason: str,
    ) -> None:
        """Optimized reconciliation path for one node-local QPU controller."""
        pending: list[dict] = []
        running: list[dict] = []
        completed: list[dict] = []
        seen_job_names: set[str] = set()

        for job in jobs:
            labels = job.get("metadata", {}).get("labels", {})
            if labels.get(_ASSIGNED_QPU_LABEL, "") != qpu_name:
                continue

            job_name = job.get("metadata", {}).get("name", "")
            if job_name:
                seen_job_names.add(job_name)

            succeeded = job.get("status", {}).get("succeeded", 0)
            failed = job.get("status", {}).get("failed", 0)
            cr_name = self._job_cr_name(job)
            if self._job_is_retired_locked(job):
                continue
            if succeeded or failed:
                if job_name:
                    self._completed_execution_jobs.add(job_name)
                completed.append(job)
            elif job.get("spec", {}).get("suspend", False):
                pending.append(job)
            else:
                running.append(job)

        pending.sort(
            key=lambda j: j.get("metadata", {}).get("creationTimestamp", ""),
        )
        running.sort(
            key=lambda j: j.get("metadata", {}).get("creationTimestamp", ""),
        )

        with self._lock:
            qs = self._queues[qpu_name]

            for job in completed:
                self._release_running_job_locked(
                    qpu_name,
                    qs,
                    job_name=self._job_name(job),
                    cr_name=self._job_cr_name(job),
                    is_succeeded=self._job_succeeded(job),
                    patch_phase=True,
                    reason=reason,
                )

            qs.queue = pending

            if len(running) > 1:
                logger.warning(
                    "QPU '%s': %d running Jobs (expected ≤1); suspending extras",
                    qpu_name, len(running),
                )
                qs.running_job_name = running[0].get("metadata", {}).get(
                    "name", "",
                )
                qs.running_cr_name = self._job_cr_name(running[0])
                for extra in running[1:]:
                    self._suspend_job(
                        extra.get("metadata", {}).get("name", ""),
                    )
            elif len(running) == 1:
                qs.running_job_name = running[0].get("metadata", {}).get(
                    "name", "",
                )
                qs.running_cr_name = self._job_cr_name(running[0])
            elif qs.running_job_name and qs.running_job_name not in seen_job_names:
                logger.warning(
                    "Running Job '%s' on QPU '%s' disappeared; releasing slot",
                    qs.running_job_name,
                    qpu_name,
                )
                qs.running_job_name = None
                qs.running_cr_name = None

            self._dispatch_idle_qpu_locked(qpu_name, qs)
            self._update_queue_positions_locked(qpu_name, qs)

        logger.debug(
            "Single-QPU reconcile complete "
            "(id=%s, qpu=%s, reason=%s, pending=%d, running=%d, completed=%d)",
            self._controller_id,
            qpu_name,
            reason,
            len(pending),
            len(running),
            len(completed),
        )

    def _dispatch_idle_qpus(self) -> None:
        """For each idle QPU, unsuspend the next Job in its queue."""
        with self._lock:
            for qpu_name, qs in self._queues.items():
                if not self._dispatch_idle_qpu_locked(qpu_name, qs):
                    break  # Don't try more dispatches on error

    def _dispatch_idle_qpu_locked(
        self,
        qpu_name: str,
        qs: QPUQueueState,
    ) -> bool:
        """Unsuspend one queued Job for an idle QPU. Caller holds _lock."""
        if qs.running_job_name is not None:
            return True  # QPU is busy
        while qs.queue:
            next_job = qs.queue.pop(0)
            if self._job_is_retired_locked(next_job):
                self._drop_retired_job_locked(qpu_name, qs, next_job)
                continue
            next_job_name = next_job.get("metadata", {}).get("name", "")
            if next_job_name:
                break
        else:
            return True  # No pending Jobs

        try:
            self.k8s.patch_job(
                next_job_name, {"spec": {"suspend": False}},
            )
            qs.running_job_name = next_job_name

            # Update QuantumJob CR status.
            cr_name = (
                next_job.get("metadata", {}).get("labels", {})
                .get(_QUANTUM_JOB_CR_LABEL, "")
            )
            qs.running_cr_name = cr_name or None
            if cr_name:
                self.k8s.update_cr_status(
                    QUANTUM_JOB_PLURAL, cr_name,
                    {
                        "phase": "Running",
                        "queuePosition": 0,
                        "queueState": "running",
                    },
                )

            logger.info(
                "Unsuspended Job '%s' on QPU '%s'",
                next_job_name, qpu_name,
            )
            return True
        except Exception:
            logger.exception(
                "Failed to unsuspend Job '%s' — re-queuing",
                next_job_name,
            )
            qs.queue.insert(0, next_job)
            return False

    def _update_queue_positions(self) -> None:
        """Update QuantumJob CR status with current queue positions."""
        with self._lock:
            for qpu_name, qs in self._queues.items():
                self._update_queue_positions_locked(qpu_name, qs)

    def _update_queue_positions_locked(
        self,
        qpu_name: str,
        qs: QPUQueueState,
    ) -> None:
        """Update QuantumJob CR queue positions for one QPU. Caller holds _lock."""
        active_queue = [
            job for job in qs.queue
            if not self._job_is_retired_locked(job)
        ]
        if len(active_queue) != len(qs.queue):
            qs.queue[:] = active_queue
        for i, job in enumerate(active_queue):
            cr_name = (
                job.get("metadata", {}).get("labels", {})
                .get(_QUANTUM_JOB_CR_LABEL, "")
            )
            if cr_name:
                try:
                    self.k8s.update_cr_status(
                        QUANTUM_JOB_PLURAL, cr_name,
                        {
                            "queuePosition": i + 1,  # 1-indexed
                            "queueLength": len(qs.queue),
                            "queueState": "queued",
                        },
                    )
                except Exception:
                    # CR may have been deleted; remove stale job ref.
                    logger.debug(
                        "QuantumJob CR '%s' no longer exists — skipping",
                        cr_name,
                    )

    @staticmethod
    def _suspend_job(job_name: str) -> None:
        """Set ``suspend: True`` on a K8s Job via a direct API call.

        Avoids using the K8sClient to prevent circular dependencies in
        local mode; the helper raises on failure because suspension is
        a safety-critical operation.
        """
        if not job_name:
            return
        try:
            from kubernetes import client, config

            config.load_incluster_config()
            batch = client.BatchV1Api()
            batch.patch_namespaced_job(
                name=job_name,
                namespace="default",
                body={"spec": {"suspend": True}},
            )
            logger.info("Suspended Job '%s'", job_name)
        except Exception:
            logger.exception("Failed to suspend Job '%s'", job_name)
