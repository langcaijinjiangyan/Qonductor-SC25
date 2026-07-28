"""
HybridWorkflowController — the Job Manager (paper §4.1).

Watches HybridWorkflow CRs, decomposes the DAG into individual steps,
dispatches CLASSICAL steps as K8s Jobs (→ kube-scheduler) and QUANTUM
steps as QuantumJob CRs (→ QuantumSchedulerController).

Also monitors child resource completion and updates the parent
HybridWorkflow.status accordingly.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import yaml

from src.operator.k8s_client import (
    HYBRID_WORKFLOW_PLURAL,
    QUANTUM_JOB_PLURAL,
    K8sClient,
    create_k8s_job_for_step,
    create_quantum_job_cr,
)

from src.workflow.dag_engine import StepType, DAGNode, HybridDAG
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry

logger = logging.getLogger(__name__)


class HybridWorkflowController:
    """K8s Operator controller for HybridWorkflow CRs.

    Responsibilities (aligned with the paper's Job Manager):
    1. Watch HybridWorkflow CRs for creation events
    2. Load the WorkflowImage from registry, parse its DAG
    3. For each node in topological order:
       a. Wait for dependencies (predecessors completed)
       b. CLASSICAL → create K8s Job → kube-scheduler allocates
       c. QUANTUM   → create QuantumJob CR → QuantumSchedulerController
    4. Monitor child resources and update parent CR status
    """

    QUANTUM_METRICS_TERMINAL_PHASES = frozenset({
        "Completed", "Failed", "Cancelled",
    })
    QUANTUM_METRICS_SETTLE_TIMEOUT_SECONDS = 5.0

    def __init__(
        self,
        mode: str = "local",
        registry_root: str = "data/workflow_registry",
    ) -> None:
        self.mode = mode
        self.k8s = K8sClient(mode=mode)
        self.registry = WorkflowRegistry(root_dir=registry_root)
        self._stop_event = threading.Event()
        # Track in-flight workflows: {workflow_cr_name: {run_id, dag, ...}}
        self._active: dict[str, dict[str, Any]] = {}
        # Track child job completions for status reconciliation.
        self._child_jobs: dict[str, dict[str, str]] = {}
        # Quantum state is scoped by workflow because DAG step IDs are reused.
        self._pending_quantum: dict[tuple[str, str], dict[str, Any]] = {}
        # Quantum steps whose K8s Job has finished.
        self._completed_quantum_steps: set[tuple[str, str]] = set()
        self._failed_quantum_steps: dict[tuple[str, str], str] = {}
        self._quantum_condition = threading.Condition()
        # Keep only aggregation fields, never full QuantumJobs with QASM.
        self._quantum_metrics_by_workflow: dict[
            str, dict[str, dict[str, Any]]
        ] = {}
        # Job state is populated by one cluster-wide watch stream.
        self._job_condition = threading.Condition()
        self._job_statuses: dict[str, dict[str, Any]] = {}
        # Callbacks for local-mode simulation.
        self._run_callbacks: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the controller loop. Blocks until Ctrl+C or stop()."""
        logger.info("HybridWorkflowController starting (mode=%s)", self.mode)
        watch_threads = [
            threading.Thread(
                target=self._watch_jobs,
                name="qonductor-job-watch",
                daemon=True,
            ),
            threading.Thread(
                target=self._watch_quantum_jobs,
                name="qonductor-quantumjob-watch",
                daemon=True,
            ),
        ]
        for thread in watch_threads:
            thread.start()

        try:
            watcher = self.k8s.watch_cr(HYBRID_WORKFLOW_PLURAL)
            for event in watcher:
                if self._stop_event.is_set():
                    break
                ev_type = event.get("type", "")
                obj = event.get("object", {})
                name = obj.get("metadata", {}).get("name", "")

                if ev_type == "ADDED":
                    phase = obj.get("status", {}).get("phase", "")
                    if phase in ("", "Pending"):
                        logger.info("New HybridWorkflow: %s", name)
                        self._handle_create(obj)
                elif ev_type == "DELETED":
                    logger.info("HybridWorkflow deleted: %s", name)
                    self._active.pop(name, None)
        finally:
            self.stop()
            for thread in watch_threads:
                thread.join(timeout=1.0)

        logger.info("HybridWorkflowController stopped")

    def stop(self) -> None:
        self._stop_event.set()
        with self._job_condition:
            self._job_condition.notify_all()
        with self._quantum_condition:
            self._quantum_condition.notify_all()

    def process_workflow(self, image_id: str,
                         inputs: dict | None = None) -> str:
        """Synchronous local-mode entry point (no K8s watch loop needed).

        Creates the HybridWorkflow CR, handles the full workflow execution
        in-process, and returns the run_id.
        """
        inputs = inputs or {}
        run_id = uuid.uuid4().hex[:16]

        # 1. Create CR.
        cr = self.k8s.create_cr(HYBRID_WORKFLOW_PLURAL, body={
            "apiVersion": "qonductor.io/v1",
            "kind": "HybridWorkflow",
            "metadata": {"name": f"qonductor-{image_id}-{uuid.uuid4().hex[:6]}"},
            "spec": {
                "workflowImageRef": image_id,
                "priority": "balanced",
                "maxRetries": 3,
                "containers": [],
                "scheduling": {},
                "errorMitigation": {"enabled": True},
                "workflowInputs": inputs,
            },
            "status": {"phase": "Pending", "stepsCompleted": 0, "totalSteps": 0},
        })

        cr_name = cr["metadata"]["name"]
        # 2. Execute.
        try:
            result = self._execute_workflow(image_id, inputs, cr, run_id)
        finally:
            self._release_workflow_quantum_state(cr_name)

        # 3. Update CR status.
        self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, cr_name, {
            "phase": result["phase"],
            "stepsCompleted": result["steps_completed"],
            "totalSteps": result["total_steps"],
            "results": result.get("results", {}),
        })

        return run_id

    # ------------------------------------------------------------------
    # Internal: CR handling
    # ------------------------------------------------------------------

    def _handle_create(self, cr: dict) -> str:
        """Handle a newly-created HybridWorkflow CR."""
        cr_name = cr["metadata"]["name"]
        if cr_name in self._active:
            logger.debug(
                "HybridWorkflow %s already active; skipping duplicate event",
                cr_name,
            )
            return self._active[cr_name]["run_id"]

        spec = cr.get("spec", {})
        image_id = spec.get("workflowImageRef", "")
        inputs = spec.get("workflowInputs", {})
        run_id = uuid.uuid4().hex[:16]

        self._active[cr_name] = {
            "run_id": run_id,
            "image_id": image_id,
            "cr": cr,
        }

        # Execute in a background thread so the watch loop continues.
        t = threading.Thread(
            target=self._run_workflow,
            args=(image_id, inputs, cr, run_id),
            name=f"workflow-{cr_name}",
            daemon=True,
        )
        t.start()
        return run_id

    def _run_workflow(
        self, image_id: str, inputs: dict, cr: dict, run_id: str,
    ) -> None:
        """Execute a watched workflow and always release its controller state."""
        cr_name = cr["metadata"]["name"]
        try:
            self._execute_workflow(image_id, inputs, cr, run_id)
        except Exception as exc:
            logger.exception("Workflow %s terminated unexpectedly", cr_name)
            try:
                self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, cr_name, {
                    "phase": "Failed",
                    "conditions": [{
                        "type": "ControllerError",
                        "status": "True",
                        "reason": str(exc),
                        "lastTransitionTime": _now_iso(),
                    }],
                })
            except Exception:
                logger.exception(
                    "Failed to record controller error for workflow %s",
                    cr_name,
                )
        finally:
            self._active.pop(cr_name, None)
            self._release_workflow_quantum_state(cr_name)

    def _release_workflow_quantum_state(self, workflow_name: str) -> None:
        """Release transient quantum state owned by one workflow."""
        with self._quantum_condition:
            pending_keys = {
                key for key in self._pending_quantum
                if key[0] == workflow_name
            }
            for key in pending_keys:
                self._pending_quantum.pop(key, None)
            self._completed_quantum_steps = {
                key for key in self._completed_quantum_steps
                if key[0] != workflow_name
            }
            self._failed_quantum_steps = {
                key: reason
                for key, reason in self._failed_quantum_steps.items()
                if key[0] != workflow_name
            }
            for child_name, info in list(self._child_jobs.items()):
                if (info.get("type") == "quantum" and
                        info.get("workflow_ref") == workflow_name):
                    self._child_jobs.pop(child_name, None)
            self._quantum_metrics_by_workflow.pop(workflow_name, None)
            self._quantum_condition.notify_all()

    def _execute_workflow(
        self, image_id: str, inputs: dict, cr: dict, run_id: str,
    ) -> dict:
        """Core workflow execution logic — shared by K8s and local modes."""
        cr_name = cr["metadata"]["name"]

        # 1. Load WorkflowImage from registry.
        image = self.registry.get(image_id)
        if image is None:
            logger.error("WorkflowImage '%s' not found in registry", image_id)
            self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, cr_name, {
                "phase": "Failed",
                "conditions": [{
                    "type": "ImageNotFound",
                    "status": "True",
                    "reason": f"image_id={image_id} not in registry",
                    "lastTransitionTime": _now_iso(),
                }],
            })
            return {"phase": "Failed", "steps_completed": 0, "total_steps": 0}

        dag = image.dag
        nodes = dag.topological_order()

        # Count step types for metrics
        classical_count = sum(
            1 for n in nodes if n.step_type == StepType.CLASSICAL
        )
        quantum_count = sum(
            1 for n in nodes if n.step_type == StepType.QUANTUM
        )

        self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, cr_name, {
            "phase": "Running",
            "totalSteps": len(nodes),
            "stepsCompleted": 0,
            "classicalStepCount": classical_count,
            "quantumStepCount": quantum_count,
        })

        # 2. Build execution order respecting DAG dependencies.
        #    Group nodes by "wave": nodes with all predecessors resolved.
        completed: set[str] = set()
        results: dict[str, Any] = {}
        step_index = 0
        first_error: Optional[str] = None

        while len(completed) < len(nodes):
            if self._stop_event.is_set():
                raise RuntimeError(
                    f"Stopped while executing workflow {cr_name!r}"
                )

            ready = []
            for node in nodes:
                if node.step_id in completed:
                    continue
                pred_ids = {e.from_step for e in dag.edges
                            if e.to_step == node.step_id}
                if pred_ids.issubset(completed):
                    ready.append(node)

            if not ready:
                # Should not happen for a valid DAG.
                break

            # Dispatch ready nodes to the appropriate scheduler.
            waiting_for_quantum = False
            for node in ready:
                quantum_key = (cr_name, node.step_id)
                if node.step_type == StepType.QUANTUM:
                    with self._quantum_condition:
                        if quantum_key in self._pending_quantum:
                            waiting_for_quantum = True
                            continue
                try:
                    if node.step_type == StepType.CLASSICAL:
                        step_result = self._dispatch_classical(node, cr)
                        completed.add(node.step_id)
                        step_index += 1
                    else:
                        step_result = self._dispatch_quantum(node, cr, inputs)
                        waiting_for_quantum = True
                        # Quantum steps: submitted to NSGA-II → K8s Job;
                        # completion is tracked via _completed_quantum_steps.
                except Exception as exc:
                    logger.exception("Step %s failed: %s", node.step_id, exc)
                    step_result = {"error": str(exc), "status": "failed"}
                    if first_error is None:
                        first_error = str(exc)
                    completed.add(node.step_id)
                    step_index += 1

                results[node.step_id] = step_result

                self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, cr_name, {
                    "phase": "Failed" if first_error else "Running",
                    "stepsCompleted": step_index,
                    "currentStep": node.label,
                    "results": results,
                })

                if first_error:
                    break  # Stop on first error (can be relaxed with maxRetries).

            # ---- Admit completed quantum steps into the DAG frontier -----
            with self._quantum_condition:
                newly_done = {
                    key for key in self._completed_quantum_steps
                    if key[0] == cr_name and key in self._pending_quantum
                }
                newly_failed = {
                    key: reason
                    for key, reason in self._failed_quantum_steps.items()
                    if key[0] == cr_name and key in self._pending_quantum
                }
                for key in newly_done:
                    step_id = key[1]
                    if step_id not in completed:
                        completed.add(step_id)
                        step_index += 1
                    self._pending_quantum.pop(key, None)
                    self._completed_quantum_steps.discard(key)
                for key, reason in newly_failed.items():
                    step_id = key[1]
                    completed.add(step_id)
                    step_index += 1
                    results[step_id] = {
                        "status": "failed",
                        "error": reason,
                    }
                    self._pending_quantum.pop(key, None)
                    self._failed_quantum_steps.pop(key, None)
                    if first_error is None:
                        first_error = reason

                # The terminal-state check and wait share the same lock as the
                # centralized QuantumJob watch, avoiding a lost notification.
                if (
                    waiting_for_quantum
                    and not newly_done
                    and not newly_failed
                    and not self._stop_event.is_set()
                ):
                    self._quantum_condition.wait()

            if first_error:
                break

        # ---- Aggregate quantum job metrics for the workflow ----------
        if first_error is None:
            try:
                self._aggregate_workflow_metrics(cr_name, nodes)
            except Exception:
                logger.exception(
                    "Failed to aggregate workflow metrics for '%s'",
                    cr_name,
                )

        final_result = {
            "phase": "Failed" if first_error else "Completed",
            "steps_completed": step_index,
            "total_steps": len(nodes),
            "results": results,
        }
        self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, cr_name, {
            "phase": final_result["phase"],
            "stepsCompleted": final_result["steps_completed"],
            "totalSteps": final_result["total_steps"],
            "results": final_result["results"],
        })
        return final_result

    def _aggregate_workflow_metrics(
        self, workflow_name: str, nodes: list,
    ) -> None:
        """Compute aggregate metrics for a completed workflow.

        Uses lightweight snapshots populated by the centralized QuantumJob
        watch.  This avoids listing every QuantumJob, including its QASM,
        whenever one workflow finishes.
        """
        deadline = (
            time.monotonic()
            + max(0.0, self.QUANTUM_METRICS_SETTLE_TIMEOUT_SECONDS)
        )
        unsettled_names: list[str] = []
        unsettled_phases: list[str] = []
        convergence_timed_out = False

        with self._quantum_condition:
            while not self._stop_event.is_set():
                workflow_metrics = self._quantum_metrics_by_workflow.get(
                    workflow_name, {}
                )
                unsettled_names = [
                    name
                    for name, metrics in workflow_metrics.items()
                    if metrics.get("phase")
                    not in self.QUANTUM_METRICS_TERMINAL_PHASES
                ]
                if not unsettled_names:
                    break

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    convergence_timed_out = True
                    unsettled_phases = sorted({
                        str(workflow_metrics[name].get("phase") or "Unknown")
                        for name in unsettled_names
                    })
                    break
                self._quantum_condition.wait(timeout=remaining)

            quantum_metrics = [
                dict(metrics)
                for metrics in self._quantum_metrics_by_workflow.get(
                    workflow_name, {}
                ).values()
            ]

        if convergence_timed_out:
            logger.warning(
                "Workflow '%s' metric convergence timed out after %.1fs; "
                "aggregating %d QuantumJobs with %d unsettled "
                "(phases=%s)",
                workflow_name,
                self.QUANTUM_METRICS_SETTLE_TIMEOUT_SECONDS,
                len(quantum_metrics),
                len(unsettled_names),
                ",".join(unsettled_phases),
            )

        fidelities = []
        total_waiting = 0.0
        total_execution = 0.0

        for metrics in quantum_metrics:
            fid = metrics.get("fidelity")
            if fid is not None and fid > 0:
                fidelities.append(fid)
            total_execution += metrics.get("execution_time", 0.0)
            total_waiting += metrics.get("waiting_time", 0.0)

        avg_fidelity = (
            sum(fidelities) / len(fidelities) if fidelities else 0.0
        )

        self.k8s.update_cr_status(HYBRID_WORKFLOW_PLURAL, workflow_name, {
            "averageFidelity": avg_fidelity,
            "totalWaitingTime": total_waiting,
            "totalExecutionTime": total_execution,
        })
        logger.info(
            "Workflow '%s' metrics: avg_fidelity=%.4f, "
            "total_waiting=%.1fs, total_execution=%.1fs, quantum_jobs=%d",
            workflow_name, avg_fidelity, total_waiting, total_execution,
            len(quantum_metrics),
        )

    # ------------------------------------------------------------------
    # Step dispatch
    # ------------------------------------------------------------------

    def _dispatch_classical(self, node: DAGNode, cr: dict) -> dict:
        """Dispatch a CLASSICAL DAG node.

        Creates a K8s Job → kube-scheduler automatically performs
        Filter-Scoring to assign it to a suitable worker node.

        In local mode this simulates the K8s scheduling cycle.
        """
        logger.info("Dispatching classical step %s", node.step_id)

        # Find the matching container spec from the CR.
        container_spec = None
        for c in cr.get("spec", {}).get("containers", []):
            reqs = node.resource_requirements
            c_limits = c.get("resources", {}).get("limits", {})
            if (reqs.get("gpu") == c_limits.get("nvidia.com/gpu") or
                    reqs.get("cpu") == c_limits.get("cpu")):
                container_spec = c
                break

        job_name = create_k8s_job_for_step(
            node, cr, container_spec=container_spec, mode=self.mode,
            client_override=self.k8s,
        )
        self._child_jobs[job_name] = {
            "step_id": node.step_id,
            "type": "classical",
        }
        try:
            if self.k8s.mode == "local":
                result = self._simulate_classical_execution(node)
                job = self.k8s._store.get("jobs", job_name)
                if job:
                    job["status"] = {"succeeded": 1, "active": 0, "failed": 0}
                    self.k8s._store.crs[f"jobs/{job_name}"] = job
                return result

            # Cover the narrow race where the Job completes before the watch
            # observes it after registration in _child_jobs.
            current_status = self.k8s.get_job_status(job_name) or {}
            if current_status.get("succeeded") or current_status.get("failed"):
                with self._job_condition:
                    self._job_statuses[job_name] = current_status
                    self._job_condition.notify_all()
            return self._wait_for_classical_job(job_name, node)
        finally:
            self._child_jobs.pop(job_name, None)
            with self._job_condition:
                self._job_statuses.pop(job_name, None)

    def _wait_for_classical_job(self, job_name: str, node: DAGNode) -> dict:
        """Block on the centralized Job watch until completion or failure."""
        with self._job_condition:
            while not self._stop_event.is_set():
                status = self._job_statuses.get(job_name, {})
                if status.get("succeeded"):
                    self._job_statuses.pop(job_name, None)
                    return {
                        "step_id": node.step_id,
                        "label": node.label,
                        "job_name": job_name,
                        "status": "completed",
                    }
                if status.get("failed"):
                    self._job_statuses.pop(job_name, None)
                    raise RuntimeError(
                        f"Classical Job {job_name!r} failed for step "
                        f"{node.step_id!r}"
                    )
                if status.get("deleted"):
                    self._job_statuses.pop(job_name, None)
                    raise RuntimeError(
                        f"Classical Job {job_name!r} was deleted before "
                        f"step {node.step_id!r} completed"
                    )
                self._job_condition.wait()
        raise RuntimeError(f"Stopped while waiting for classical Job {job_name!r}")

    def _consume_child_watch(
        self,
        watch_name: str,
        events: Iterable[dict],
        handler: Callable[[str, str, dict], None],
    ) -> None:
        """Consume one child-resource watch and dispatch normalized events."""
        logger.info("%s starting", watch_name)
        try:
            for event in events:
                if self._stop_event.is_set():
                    break
                event_type = event.get("type", "")
                if event_type == "HEARTBEAT":
                    continue

                obj = event.get("object", {})
                name = obj.get("metadata", {}).get("name", "")
                if name:
                    handler(event_type, name, obj)
        finally:
            logger.info("%s stopped", watch_name)

    def _watch_jobs(self) -> None:
        """Maintain classical Job status from one Kubernetes watch."""
        self._consume_child_watch(
            "Centralized Job watch",
            self.k8s.watch_jobs(
                label_selector="app=qonductor,step_type=classical",
            ),
            self._handle_classical_job_event,
        )

    def _handle_classical_job_event(
        self, event_type: str, job_name: str, job: dict,
    ) -> None:
        """Record one watched classical Job event for its waiting thread."""
        # Ignore terminal Jobs left from previous controller runs. Their
        # workflow threads are not waiting for these events.
        if job_name not in self._child_jobs:
            return

        status = dict(job.get("status", {}) or {})
        if event_type == "DELETED":
            status["deleted"] = True
        with self._job_condition:
            self._job_statuses[job_name] = status
            self._job_condition.notify_all()

    def _watch_quantum_jobs(self) -> None:
        """Maintain workflow-owned QuantumJob state from one CR watch."""
        self._consume_child_watch(
            "Centralized QuantumJob watch",
            self.k8s.watch_cr(QUANTUM_JOB_PLURAL),
            self._handle_quantum_job_event,
        )

    @staticmethod
    def _optional_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @classmethod
    def _extract_quantum_metric_snapshot(
        cls, quantum_job: dict,
    ) -> dict[str, Any]:
        """Extract only fields needed for workflow-level aggregation."""
        metadata = quantum_job.get("metadata", {}) or {}
        status = quantum_job.get("status", {}) or {}

        fidelity = (
            status.get("actualFidelity")
            or status.get("estimatedFidelity")
        )
        execution_time = (
            status.get("actualExecutionTime")
            or status.get("estimatedExecutionTime")
        )

        waiting_time = 0.0
        created = metadata.get("creationTimestamp", "")
        scheduled = status.get("scheduledAt", "")
        if created and scheduled:
            try:
                created_at = datetime.fromisoformat(
                    created.replace("Z", "+00:00"),
                )
                scheduled_at = datetime.fromisoformat(
                    scheduled.replace("Z", "+00:00"),
                )
                waiting_time = (scheduled_at - created_at).total_seconds()
            except (TypeError, ValueError):
                pass

        return {
            "phase": status.get("phase", ""),
            "resource_version": str(metadata.get("resourceVersion", "")),
            "fidelity": cls._optional_float(fidelity),
            "execution_time": cls._optional_float(execution_time) or 0.0,
            "waiting_time": waiting_time,
        }

    @classmethod
    def _is_stale_quantum_metric_snapshot(
        cls, current: dict[str, Any], incoming: dict[str, Any],
    ) -> bool:
        """Reject an older watch event after a newer synchronous read."""
        current_rv = current.get("resource_version", "")
        incoming_rv = incoming.get("resource_version", "")
        if current_rv and incoming_rv:
            try:
                if int(incoming_rv) < int(current_rv):
                    return True
            except ValueError:
                pass

        return (
            current.get("phase") in cls.QUANTUM_METRICS_TERMINAL_PHASES
            and incoming.get("phase")
            not in cls.QUANTUM_METRICS_TERMINAL_PHASES
        )

    def _handle_quantum_job_event(
        self, event_type: str, qj_name: str, quantum_job: dict,
    ) -> None:
        """Advance workflow quantum state from one watched QuantumJob event."""
        status = quantum_job.get("status", {}) or {}
        phase = status.get("phase", "")

        with self._quantum_condition:
            metrics_settled = False
            info = self._child_jobs.get(qj_name)
            owned_quantum_job = bool(
                info and info.get("type") == "quantum"
            )
            workflow_name = (
                quantum_job.get("spec", {}).get("workflowRef", "")
                or (info or {}).get("workflow_ref", "")
            )

            # Dynamic QAOA/VQE QuantumJobs are not child DAG steps, but their
            # metrics still belong to an active HybridWorkflow.
            should_cache = bool(
                workflow_name
                and (owned_quantum_job or workflow_name in self._active)
            )
            if event_type == "DELETED":
                workflow_metrics = self._quantum_metrics_by_workflow.get(
                    workflow_name
                )
                if workflow_metrics is not None:
                    removed = workflow_metrics.pop(qj_name, None)
                    metrics_settled = removed is not None
                    if not workflow_metrics:
                        self._quantum_metrics_by_workflow.pop(
                            workflow_name, None,
                        )
            elif should_cache:
                incoming = self._extract_quantum_metric_snapshot(quantum_job)
                workflow_metrics = self._quantum_metrics_by_workflow.setdefault(
                    workflow_name, {}
                )
                current = workflow_metrics.get(qj_name)
                if (
                    current is None
                    or not self._is_stale_quantum_metric_snapshot(
                        current, incoming,
                    )
                ):
                    if current != incoming:
                        workflow_metrics[qj_name] = incoming
                        metrics_settled = (
                            incoming.get("phase")
                            in self.QUANTUM_METRICS_TERMINAL_PHASES
                        )

            if metrics_settled:
                self._quantum_condition.notify_all()

            if not owned_quantum_job:
                return

            step_id = info.get("step_id", "")
            quantum_key = (workflow_name, step_id)

            if event_type == "DELETED":
                reason = (
                    f"QuantumJob {qj_name!r} was deleted before completion"
                )
                self._failed_quantum_steps[quantum_key] = reason
            elif phase == "Completed":
                self._completed_quantum_steps.add(quantum_key)
            elif phase in ("Failed", "Cancelled"):
                reason = ""
                for condition in reversed(status.get("conditions", []) or []):
                    reason = condition.get("reason", "")
                    if reason:
                        break
                failure = f"QuantumJob {qj_name!r} entered phase {phase}"
                if reason:
                    failure = f"{failure}: {reason}"
                self._failed_quantum_steps[quantum_key] = failure
            else:
                return

            self._child_jobs.pop(qj_name, None)
            self._quantum_condition.notify_all()

    def _sync_quantum_job_state(self, qj_name: str) -> None:
        """Read a newly registered QuantumJob once to close the watch race."""
        quantum_job = self.k8s.get_cr(QUANTUM_JOB_PLURAL, qj_name)
        if quantum_job is None:
            self._handle_quantum_job_event(
                "DELETED",
                qj_name,
                {"metadata": {"name": qj_name}},
            )
            return
        self._handle_quantum_job_event("SYNC", qj_name, quantum_job)

    @staticmethod
    def _extract_quantum_inputs(inputs: dict) -> dict[str, Any]:
        """Extract quantum circuit metadata from *workflowInputs*.

        Pure quantum workflows embed the QASM circuit, format, qubits, and
        shots under a per-algorithm key (e.g. ``quantum``).  This helper
        finds the first sub-dict that looks like a quantum payload and
        returns the fields needed to populate a QuantumJob CR.
        """
        # Find the first sub-key whose value is a dict containing circuitQasm.
        for _key, value in (inputs or {}).items():
            if not isinstance(value, dict):
                continue
            if "circuitQasm" in value:
                return value
        return {}

    def _dispatch_quantum(
        self, node: DAGNode, cr: dict, inputs: dict | None = None,
    ) -> dict:
        """Dispatch a QUANTUM DAG node.

        Creates a QuantumJob CR → QuantumSchedulerController watches it,
        collects jobs into the pending queue, runs NSGA-II scheduling,
        and assigns a QPU.

        Before creating the CR, the DAG node is enriched with circuit
        metadata extracted from the HybridWorkflow CR's *workflowInputs*
        so that pure-quantum jobs carry their QASM3 payload through to
        the scheduler.
        """
        # ---- Enrich DAGNode from workflowInputs (pure-quantum path) ----
        # Pure-quantum workflows carry the QASM3 payload in the HybridWorkflow
        # CR's workflowInputs rather than in the WorkflowImage DAG.  Extract
        # those fields so create_quantum_job_cr() produces a QuantumJob CR
        # that is consistent with what the hybrid runtime's submit_quantum_eval()
        # creates — i.e. spec.circuitQasm, spec.circuitFormat, spec.qubits, etc.
        q_inputs = self._extract_quantum_inputs(inputs or {})
        if q_inputs:
            # Circuit metadata → DAGNode.metadata
            # create_quantum_job_cr() maps these keys to QuantumJob spec fields:
            #   circuit_qasm → spec.circuitQasm
            #   circuit_format → spec.circuitFormat
            #   logical_circuit_id → spec.logicalCircuitId (+ spec.circuitNames)
            node.metadata.setdefault("circuit_qasm", q_inputs.get("circuitQasm", ""))
            node.metadata.setdefault("circuit_format", q_inputs.get("circuitFormat", "qasm3"))
            node.metadata.setdefault("logical_circuit_id", q_inputs.get("logicalCircuitId", ""))
            node.metadata.setdefault("qasm_path", q_inputs.get("qasmPath", ""))
            # Resource requirements — overwrite the hardcoded generator defaults
            # (qubits=1) with the actual values from workflowInputs.
            node.resource_requirements["qubits"] = q_inputs.get(
                "qubits", node.resource_requirements.get("qubits", 10),
            )
            node.resource_requirements["shots"] = q_inputs.get(
                "shots", node.resource_requirements.get("shots", 4000),
            )

        logger.info("Dispatching quantum step %s (qubits=%s)",
                    node.step_id, node.resource_requirements.get("qubits"))

        qj = create_quantum_job_cr(
            node, cr, mode=self.mode, client_override=self.k8s,
        )
        qj_name = qj["metadata"]["name"]
        workflow_name = cr["metadata"]["name"]
        quantum_key = (workflow_name, node.step_id)

        # Register before invoking callbacks so even immediate local-mode
        # completion cannot race ahead of the pending state.
        with self._quantum_condition:
            self._child_jobs[qj_name] = {
                "step_id": node.step_id,
                "type": "quantum",
                "cr_name": qj_name,
                "workflow_ref": workflow_name,
            }
            self._pending_quantum[quantum_key] = {
                "quantum_job": qj_name,
                "qubits": node.resource_requirements.get("qubits"),
            }

        if self.k8s.mode == "k8s":
            self._sync_quantum_job_state(qj_name)

        # If a quantum scheduler callback is registered, invoke it.
        cb = self._run_callbacks.get("quantum_scheduler")
        if cb:
            cb(qj)

        # Local mode does not run Pods, so the quantum execution path is
        # simulated as complete after optional scheduler callback processing.
        if self.k8s.mode == "local":
            completed_qj = self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj_name, {
                "phase": "Completed",
                "result": {"simulated": True},
            })
            self._handle_quantum_job_event(
                "MODIFIED", qj_name, completed_qj or qj,
            )

        return {
            "quantum_job": qj_name,
            "qubits": node.resource_requirements.get("qubits"),
            "status": "submitted",
        }

    # ------------------------------------------------------------------
    # Local simulation helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _simulate_classical_execution(node: DAGNode) -> dict:
        """Simulate classical job execution in local mode."""
        logger.info("Simulating classical execution: %s", node.label)
        time.sleep(0.01)  # minimal simulation delay
        return {
            "step_id": node.step_id,
            "label": node.label,
            "status": "completed",
            "outputs": {var: f"simulated_{var}" for var in node.outputs},
        }

    def register_quantum_scheduler_callback(self, cb) -> None:
        """Register a callback invoked for each dispatched QuantumJob."""
        self._run_callbacks["quantum_scheduler"] = cb


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_operator_config(config_path: str) -> dict[str, Any]:
    """Load and validate the operator's mounted YAML configuration."""
    if not config_path:
        return {}

    path = Path(config_path)
    if not path.is_file():
        raise RuntimeError(
            f"QONDUCTOR_CONFIG points to missing file: {config_path}"
        )
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise RuntimeError(
            f"Cannot load operator configuration {config_path}: {exc}"
        ) from exc
    if loaded is None:
        loaded = {}
    if not isinstance(loaded, dict):
        raise RuntimeError(
            f"Operator configuration {config_path} must contain a YAML mapping"
        )
    logger.info(
        "Loaded operator configuration from %s (%d settings)",
        config_path,
        len(loaded),
    )
    return loaded


def _positive_int_config(
    config: dict[str, Any], env_name: str, config_key: str, default: int,
) -> int:
    """Read a positive integer with environment-over-ConfigMap precedence."""
    import os

    raw = os.environ.get(env_name, config.get(config_key, default))
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"{env_name}/{config_key} must be an integer, got {raw!r}"
        ) from exc
    if value < 1:
        raise RuntimeError(
            f"{env_name}/{config_key} must be at least 1, got {value}"
        )
    return value


def _bool_config(
    config: dict[str, Any], env_name: str, config_key: str, default: bool,
) -> bool:
    """Read a boolean with environment-over-ConfigMap precedence."""
    import os

    raw = os.environ.get(env_name, config.get(config_key, default))
    if isinstance(raw, bool):
        return raw
    normalized = str(raw).strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    raise RuntimeError(
        f"{env_name}/{config_key} must be a boolean, got {raw!r}"
    )


# ===================================================================
# Module entry point used by docker/Dockerfile.operator
# ===================================================================

def main() -> None:
    """Run the HybridWorkflow and QuantumScheduler controllers together."""
    import os

    from src.utils.logging_config import configure_logging
    from src.operator.quantum_scheduler_controller import QuantumSchedulerController
    from src.operator.qpu_queue_controller import QPUQueueController
    from src.operator.metrics_server import start_metrics_server

    configure_logging()

    operator_config = _load_operator_config(
        os.environ.get("QONDUCTOR_CONFIG", ""),
    )
    mode = os.environ.get("QONDUCTOR_MODE", "local")
    registry_root = os.environ.get(
        "QONDUCTOR_REGISTRY_ROOT",
        str(operator_config.get("registry-root", "data/workflow_registry")),
    )
    scheduling_interval = _positive_int_config(
        operator_config,
        "QONDUCTOR_SCHEDULING_INTERVAL",
        "scheduling-interval",
        30,
    )
    scheduling_threshold = _positive_int_config(
        operator_config,
        "QONDUCTOR_SCHEDULING_THRESHOLD",
        "scheduling-threshold",
        10,
    )
    scheduling_batch_size = _positive_int_config(
        operator_config,
        "QONDUCTOR_SCHEDULING_BATCH_SIZE",
        "scheduling-batch-size",
        120,
    )
    transpilation_workers = _positive_int_config(
        operator_config,
        "QONDUCTOR_TRANSPILATION_WORKERS",
        "transpilation-workers",
        1,
    )
    os.environ["QONDUCTOR_TRANSPILATION_WORKERS"] = str(
        transpilation_workers
    )
    transpilation_cache_enabled = _bool_config(
        operator_config,
        "QONDUCTOR_TRANSPILATION_CACHE_ENABLED",
        "transpilation-cache-enabled",
        True,
    )
    transpilation_cache_size = _positive_int_config(
        operator_config,
        "QONDUCTOR_TRANSPILATION_CACHE_SIZE",
        "transpilation-cache-size",
        256,
    )
    transpilation_count = _positive_int_config(
        operator_config,
        "QONDUCTOR_TRANSPILATION_COUNT",
        "transpilation-count",
        10,
    )
    metrics_port = _positive_int_config(
        operator_config,
        "QONDUCTOR_METRICS_PORT",
        "metrics-port",
        9100,
    )
    enable_central_qpu_queue = _bool_config(
        operator_config,
        "QONDUCTOR_ENABLE_CENTRAL_QPU_QUEUE",
        "enable-central-qpu-queue",
        True,
    )

    quantum_controller = QuantumSchedulerController(
        mode=mode,
        scheduling_interval=scheduling_interval,
        scheduling_threshold=scheduling_threshold,
        scheduling_batch_size=scheduling_batch_size,
        transpilation_cache_enabled=transpilation_cache_enabled,
        transpilation_cache_size=transpilation_cache_size,
        transpilation_count=transpilation_count,
    )
    quantum_thread = threading.Thread(
        target=quantum_controller.run,
        daemon=True,
    )
    quantum_thread.start()

    # QPU queue controller — manages per-QPU FIFO serial execution.
    # Multi-host deployments can move this responsibility to the node-local
    # device-plugin queue agents by setting QONDUCTOR_ENABLE_CENTRAL_QPU_QUEUE=0.
    queue_controller = None
    if enable_central_qpu_queue:
        queue_controller = QPUQueueController(
            mode=mode,
            k8s_client=quantum_controller.k8s,
        )
        queue_thread = threading.Thread(
            target=queue_controller.run,
            daemon=True,
        )
        queue_thread.start()
    else:
        logger.info(
            "Central QPUQueueController disabled; expecting node-local queue agents",
        )

    workflow_controller = HybridWorkflowController(
        mode=mode,
        registry_root=registry_root,
    )

    # Start the embedded metrics HTTP server
    metrics_server, metrics_thread = start_metrics_server(
        workflow_controller.k8s,
        port=metrics_port,
    )

    try:
        workflow_controller.run()
    finally:
        workflow_controller.stop()
        quantum_controller.stop()
        if queue_controller is not None:
            queue_controller.stop()
        metrics_server.shutdown()
        metrics_server.server_close()
        quantum_thread.join(timeout=5)
        queue_thread.join(timeout=5)
        metrics_thread.join(timeout=5)


if __name__ == "__main__":
    main()
