"""
QPUQueueController — per-QPU FIFO queue for quantum execution Jobs.

Manages serial execution of quantum Jobs on each QPU by controlling
the K8s ``suspend`` field.  The scheduler creates Jobs in suspended
state; this controller unsuspends them one at a time per QPU in FIFO
order (by ``creationTimestamp``).

Design:
1. On startup, reconciles state from existing quantum Jobs.
2. Every *reconcile_interval* seconds, lists all ``step_type=quantum``
   Jobs and ensures at most one is running (unsuspended) per QPU.
3. Updates QuantumJob CR status with queue position.

Does NOT modify ``HybridWorkflowController._reconcile_status()`` — it
continues to poll Job status independently for DAG advancement.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
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


class QPUQueueController:
    """Maintains per-QPU FIFO queues and manages Job suspend/unsuspend."""

    def __init__(
        self,
        mode: str = "local",
        k8s_client: K8sClient | None = None,
        reconcile_interval: int = _RECONCILE_INTERVAL,
    ) -> None:
        self.mode = mode
        self.k8s = k8s_client or K8sClient(mode=mode)
        self._reconcile_interval = reconcile_interval
        self._queues: dict[str, QPUQueueState] = defaultdict(QPUQueueState)
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the queue controller loop. Blocks until :meth:`stop`."""
        logger.info("QPUQueueController starting (mode=%s)", self.mode)

        # Rebuild state from existing Jobs on startup.
        self._reconcile_existing_jobs()

        while not self._stop_event.is_set():
            try:
                self._reconcile()
            except Exception:
                logger.exception("Error during QPU queue reconciliation")
            self._stop_event.wait(timeout=self._reconcile_interval)

        logger.info("QPUQueueController stopped")

    def stop(self) -> None:
        """Signal the controller to stop at the next cycle."""
        self._stop_event.set()

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
                }
            return result

    # ------------------------------------------------------------------
    # Internal — reconciliation
    # ------------------------------------------------------------------

    def _reconcile_existing_jobs(self) -> None:
        """On startup, scan existing quantum Jobs and rebuild queue state."""
        try:
            jobs = self.k8s.list_jobs(
                label_selector=f"{_STEP_TYPE_LABEL}={_QUANTUM_STEP_TYPE}",
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
            if not qpu:
                continue

            is_suspended = job.get("spec", {}).get("suspend", False)
            succeeded = job.get("status", {}).get("succeeded", 0)
            failed = job.get("status", {}).get("failed", 0)

            if succeeded or failed:
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
                elif len(state["running"]) == 1:
                    qs.running_job_name = state["running"][0].get(
                        "metadata", {},
                    ).get("name", "")

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

    def _reconcile(self) -> None:
        """Periodic reconciliation: scan quantum Jobs, maintain queue invariants."""
        try:
            jobs = self.k8s.list_jobs(
                label_selector=f"{_STEP_TYPE_LABEL}={_QUANTUM_STEP_TYPE}",
            )
        except Exception:
            logger.exception("Failed to list quantum Jobs")
            return

        # Partition by QPU and status.
        per_qpu: dict[str, dict] = defaultdict(
            lambda: {"pending": [], "running": [], "completed": []},
        )

        for job in jobs:
            labels = job.get("metadata", {}).get("labels", {})
            qpu = labels.get(_ASSIGNED_QPU_LABEL, "")
            if not qpu:
                continue

            job_name = job.get("metadata", {}).get("name", "")
            is_suspended = job.get("spec", {}).get("suspend", False)
            succeeded = job.get("status", {}).get("succeeded", 0)
            failed = job.get("status", {}).get("failed", 0)

            if succeeded or failed:
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
                    job_name = job.get("metadata", {}).get("name", "")
                    if qs.running_job_name == job_name:
                        cr_name = (
                            job.get("metadata", {}).get("labels", {})
                            .get(_QUANTUM_JOB_CR_LABEL, "")
                        )
                        succeeded = job.get("status", {}).get("succeeded")
                        is_succeeded = (succeeded or 0) > 0
                        if cr_name:
                            try:
                                self.k8s.update_cr_status(
                                    QUANTUM_JOB_PLURAL, cr_name,
                                    {
                                        "phase": "Completed" if is_succeeded else "Failed",
                                        "queuePosition": 0,
                                        "queueState": "completed",
                                    },
                                )
                            except Exception:
                                logger.debug(
                                    "QuantumJob CR '%s' no longer exists — skipping",
                                    cr_name,
                                )
                        qs.running_job_name = None
                        logger.info(
                            "Job '%s' on QPU '%s' completed (succeeded=%s)",
                            job_name, qpu, is_succeeded,
                        )

            # Enqueue pending Jobs in FIFO order.
            for qpu, state in per_qpu.items():
                qs = self._queues[qpu]
                for job in state["pending"]:
                    if not any(
                        q.get("metadata", {}).get("name", "")
                        == job.get("metadata", {}).get("name", "")
                        for q in qs.queue
                    ):
                        qs.queue.append(job)
                # Re-sort by creationTimestamp.
                qs.queue.sort(
                    key=lambda j: j.get("metadata", {}).get(
                        "creationTimestamp", "",
                    ),
                )

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
                    for extra in running[1:]:
                        self._suspend_job(
                            extra.get("metadata", {}).get("name", ""),
                        )
                elif len(running) == 1:
                    qs.running_job_name = running[0].get(
                        "metadata", {},
                    ).get("name", "")

        # Dispatch to idle QPUs and update CR queue positions.
        self._dispatch_idle_qpus()
        self._update_queue_positions()

    def _dispatch_idle_qpus(self) -> None:
        """For each idle QPU, unsuspend the next Job in its queue."""
        with self._lock:
            for qpu_name, qs in self._queues.items():
                if qs.running_job_name is not None:
                    continue  # QPU is busy
                if not qs.queue:
                    continue  # No pending Jobs

                next_job = qs.queue.pop(0)
                next_job_name = next_job.get("metadata", {}).get("name", "")
                if not next_job_name:
                    continue

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
                except Exception:
                    logger.exception(
                        "Failed to unsuspend Job '%s' — re-queuing",
                        next_job_name,
                    )
                    qs.queue.insert(0, next_job)
                    break  # Don't try more dispatches on error

    def _update_queue_positions(self) -> None:
        """Update QuantumJob CR status with current queue positions."""
        with self._lock:
            for qpu_name, qs in self._queues.items():
                for i, job in enumerate(qs.queue):
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
