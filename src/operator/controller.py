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
from pathlib import Path
from typing import Any, Optional

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

    RECONCILE_INTERVAL = 2  # seconds between status polls

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
        job_watch_thread = threading.Thread(
            target=self._watch_jobs,
            name="qonductor-job-watch",
            daemon=True,
        )
        job_watch_thread.start()

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
                elif ev_type == "MODIFIED":
                    self._reconcile_status(obj)
                elif ev_type == "DELETED":
                    logger.info("HybridWorkflow deleted: %s", name)
                    self._active.pop(name, None)
        finally:
            self.stop()
            job_watch_thread.join(timeout=1.0)

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
            self._quantum_condition.notify_all()

        for child_name, info in list(self._child_jobs.items()):
            if (info.get("type") == "quantum" and
                    info.get("workflow_ref") == workflow_name):
                self._child_jobs.pop(child_name, None)

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
                        step_result = self._dispatch_quantum(node, cr)
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

            # Poll execution Jobs while this workflow waits. Parent CR events
            # alone are not sufficient because QuantumJob updates do not
            # modify the HybridWorkflow resource.
            if waiting_for_quantum:
                self._reconcile_status(cr)

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

            if first_error:
                break
            if waiting_for_quantum and not newly_done:
                with self._quantum_condition:
                    self._quantum_condition.wait(
                        timeout=self.RECONCILE_INTERVAL,
                    )

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

        Queries all QuantumJob CRs belonging to *workflow_name* and
        computes average fidelity, total waiting time, and total
        execution time.  Results are written to the HybridWorkflow
        CR status.
        """
        from datetime import datetime, timezone

        quantum_jobs = self.k8s.list_cr(QUANTUM_JOB_PLURAL)
        wf_quantum_jobs = [
            qj for qj in quantum_jobs
            if qj.get("spec", {}).get("workflowRef") == workflow_name
        ]

        fidelities = []
        total_waiting = 0.0
        total_execution = 0.0

        for qj in wf_quantum_jobs:
            st = qj.get("status", {})
            # Fidelity: prefer actualFidelity, fall back to estimatedFidelity
            fid = (
                st.get("actualFidelity")
                or st.get("estimatedFidelity")
            )
            if fid is not None and fid > 0:
                fidelities.append(float(fid))

            # Execution time
            et = (
                st.get("actualExecutionTime")
                or st.get("estimatedExecutionTime")
                or 0.0
            )
            total_execution += float(et)

            # Waiting time = scheduledAt - creationTimestamp
            created = qj.get("metadata", {}).get("creationTimestamp", "")
            scheduled = st.get("scheduledAt", "")
            if created and scheduled:
                try:
                    c = datetime.fromisoformat(
                        created.replace("Z", "+00:00"),
                    )
                    s = datetime.fromisoformat(
                        scheduled.replace("Z", "+00:00"),
                    )
                    total_waiting += (s - c).total_seconds()
                except (ValueError, TypeError):
                    pass

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
            "total_waiting=%.1fs, total_execution=%.1fs",
            workflow_name, avg_fidelity, total_waiting, total_execution,
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

    def _watch_jobs(self) -> None:
        """Maintain Job status from one Kubernetes watch connection."""
        logger.info("Centralized Job watch starting")
        for event in self.k8s.watch_jobs(
            label_selector="app=qonductor,step_type=classical",
        ):
            if self._stop_event.is_set():
                break
            if event.get("type") == "HEARTBEAT":
                continue

            job = event.get("object", {})
            job_name = job.get("metadata", {}).get("name", "")
            if not job_name:
                continue

            # Ignore terminal Jobs left from previous controller runs. Their
            # workflow threads are not waiting for these events.
            if job_name not in self._child_jobs:
                continue

            status = dict(job.get("status", {}) or {})
            if event.get("type") == "DELETED":
                status["deleted"] = True
            with self._job_condition:
                self._job_statuses[job_name] = status
                self._job_condition.notify_all()
        logger.info("Centralized Job watch stopped")

    def _dispatch_quantum(self, node: DAGNode, cr: dict) -> dict:
        """Dispatch a QUANTUM DAG node.

        Creates a QuantumJob CR → QuantumSchedulerController watches it,
        collects jobs into the pending queue, runs NSGA-II scheduling,
        and assigns a QPU.
        """
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
        self._child_jobs[qj_name] = {
            "step_id": node.step_id,
            "type": "quantum",
            "cr_name": qj_name,
            "workflow_ref": workflow_name,
        }
        with self._quantum_condition:
            self._pending_quantum[quantum_key] = {
                "quantum_job": qj_name,
                "qubits": node.resource_requirements.get("qubits"),
            }

        # If a quantum scheduler callback is registered, invoke it.
        cb = self._run_callbacks.get("quantum_scheduler")
        if cb:
            cb(qj)

        # Local mode does not run Pods, so the quantum execution path is
        # simulated as complete after optional scheduler callback processing.
        if self.k8s.mode == "local":
            self.k8s.update_cr_status(QUANTUM_JOB_PLURAL, qj_name, {
                "phase": "Completed",
                "result": {"simulated": True},
            })
            with self._quantum_condition:
                self._completed_quantum_steps.add(quantum_key)
                self._quantum_condition.notify_all()

        return {
            "quantum_job": qj_name,
            "qubits": node.resource_requirements.get("qubits"),
            "status": "submitted",
        }

    # ------------------------------------------------------------------
    # Status reconciliation
    # ------------------------------------------------------------------

    def _reconcile_status(self, cr: dict) -> None:
        """Poll child K8s Job / QuantumJob statuses and advance the DAG.

        When a quantum execution Job completes (succeeded or failed), the
        corresponding quantum step is moved into
        ``_completed_quantum_steps`` so that ``_execute_workflow()`` can
        unblock dependent DAG nodes.
        """
        cr_name = cr["metadata"]["name"]

        for child_name, info in list(self._child_jobs.items()):
            if (info.get("type") != "quantum" or
                    info.get("workflow_ref") != cr_name):
                continue
            step_id = info["step_id"]
            quantum_key = (cr_name, step_id)

            # Try polling QuantumJob CR for execution status
            qj = self.k8s.get_cr(QUANTUM_JOB_PLURAL, child_name)
            if qj is None:
                continue

            qj_phase = qj.get("status", {}).get("phase", "")
            execution_job_name = qj.get("status", {}).get("executionJob", "")

            if qj_phase == "Scheduled" and execution_job_name:
                # QuantumSchedulerController dispatched a K8s Job — check it
                job_status = self.k8s.get_job_status(execution_job_name)
                if job_status:
                    if job_status.get("succeeded"):
                        self.k8s.update_cr_status(
                            QUANTUM_JOB_PLURAL, child_name,
                            {"phase": "Completed"},
                        )
                        self._child_jobs.pop(child_name, None)
                        with self._quantum_condition:
                            self._completed_quantum_steps.add(quantum_key)
                            self._quantum_condition.notify_all()
                        logger.info(
                            "Quantum step '%s' completed → "
                            "unblocking DAG for workflow '%s'",
                            step_id, cr_name,
                        )
                    elif job_status.get("failed"):
                        logger.error(
                            "Quantum execution Job '%s' failed for step '%s'",
                            execution_job_name, step_id,
                        )
                        try:
                            self.k8s.update_cr_status(
                                QUANTUM_JOB_PLURAL, child_name,
                                {"phase": "Failed"},
                            )
                        except Exception:
                            logger.exception(
                                "Failed to mark QuantumJob %s as failed",
                                child_name,
                            )
                        self._child_jobs.pop(child_name, None)
                        with self._quantum_condition:
                            self._failed_quantum_steps[quantum_key] = (
                                f"Quantum execution Job {execution_job_name!r} "
                                f"failed for step {step_id!r}"
                            )
                            self._quantum_condition.notify_all()

            elif qj_phase == "Completed":
                # Already completed in a previous reconciliation pass
                self._child_jobs.pop(child_name, None)
                with self._quantum_condition:
                    self._completed_quantum_steps.add(quantum_key)
                    self._quantum_condition.notify_all()
            elif qj_phase in ("Failed", "Cancelled"):
                self._child_jobs.pop(child_name, None)
                with self._quantum_condition:
                    self._failed_quantum_steps[quantum_key] = (
                        f"QuantumJob {child_name!r} entered phase {qj_phase}"
                    )
                    self._quantum_condition.notify_all()

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
    metrics_port = _positive_int_config(
        operator_config,
        "QONDUCTOR_METRICS_PORT",
        "metrics-port",
        9100,
    )

    quantum_controller = QuantumSchedulerController(
        mode=mode,
        scheduling_interval=scheduling_interval,
        scheduling_threshold=scheduling_threshold,
        transpilation_cache_enabled=transpilation_cache_enabled,
        transpilation_cache_size=transpilation_cache_size,
    )
    quantum_thread = threading.Thread(
        target=quantum_controller.run,
        daemon=True,
    )
    quantum_thread.start()

    # QPU queue controller — manages per-QPU FIFO serial execution.
    queue_controller = QPUQueueController(
        mode=mode,
        k8s_client=quantum_controller.k8s,
    )
    queue_thread = threading.Thread(
        target=queue_controller.run,
        daemon=True,
    )
    queue_thread.start()

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
        queue_controller.stop()
        metrics_server.shutdown()
        metrics_server.server_close()
        quantum_thread.join(timeout=5)
        queue_thread.join(timeout=5)
        metrics_thread.join(timeout=5)


if __name__ == "__main__":
    main()
