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
from typing import Any, Optional

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
        # Quantum steps pending execution completion (step_id → metadata).
        self._pending_quantum: dict[str, dict[str, Any]] = {}
        # Quantum steps whose K8s Job has finished.
        self._completed_quantum_steps: set[str] = set()
        # Callbacks for local-mode simulation.
        self._run_callbacks: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the controller loop. Blocks until Ctrl+C or stop()."""
        logger.info("HybridWorkflowController starting (mode=%s)", self.mode)
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

        logger.info("HybridWorkflowController stopped")

    def stop(self) -> None:
        self._stop_event.set()

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

        # 2. Execute.
        result = self._execute_workflow(image_id, inputs, cr, run_id)

        # 3. Update CR status.
        cr_name = cr["metadata"]["name"]
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
        spec = cr.get("spec", {})
        image_id = spec.get("workflowImageRef", "")
        inputs = spec.get("workflowInputs", {})
        run_id = uuid.uuid4().hex[:16]

        self._active[cr["metadata"]["name"]] = {
            "run_id": run_id,
            "image_id": image_id,
            "cr": cr,
        }

        # Execute in a background thread so the watch loop continues.
        t = threading.Thread(
            target=self._execute_workflow,
            args=(image_id, inputs, cr, run_id),
            daemon=True,
        )
        t.start()
        return run_id

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
            for node in ready:
                try:
                    if node.step_type == StepType.CLASSICAL:
                        step_result = self._dispatch_classical(node, cr)
                        completed.add(node.step_id)
                        step_index += 1
                    else:
                        step_result = self._dispatch_quantum(node, cr)
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
            newly_done = self._completed_quantum_steps & set(
                self._pending_quantum
            )
            for step_id in newly_done:
                if step_id not in completed:
                    completed.add(step_id)
                    step_index += 1
                self._pending_quantum.pop(step_id, None)
            self._completed_quantum_steps.difference_update(newly_done)

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

        return {
            "phase": "Failed" if first_error else "Completed",
            "steps_completed": step_index,
            "total_steps": len(nodes),
            "results": results,
        }

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

        # Simulate job completion for local mode, including k8s requests that
        # were downgraded by K8sClient because no cluster is reachable.
        if self.k8s.mode == "local":
            result = self._simulate_classical_execution(node)
            # Mark job as succeeded in the local store.
            job = self.k8s._store.get("jobs", job_name)
            if job:
                job["status"] = {"succeeded": 1, "active": 0, "failed": 0}
                self.k8s._store.crs[f"jobs/{job_name}"] = job
            return result

        return self._wait_for_classical_job(job_name, node)

    def _wait_for_classical_job(self, job_name: str, node: DAGNode) -> dict:
        """Block until a K8s classical Job succeeds or fails."""
        while not self._stop_event.is_set():
            status = self.k8s.get_job_status(job_name)
            if status:
                if status.get("succeeded"):
                    return {
                        "step_id": node.step_id,
                        "label": node.label,
                        "job_name": job_name,
                        "status": "completed",
                    }
                if status.get("failed"):
                    raise RuntimeError(
                        f"Classical Job {job_name!r} failed for step {node.step_id!r}"
                    )
            time.sleep(self.RECONCILE_INTERVAL)
        raise RuntimeError(f"Stopped while waiting for classical Job {job_name!r}")

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
            self._completed_quantum_steps.add(node.step_id)

        # Track for later status reconciliation (K8s Job polling).
        self._child_jobs[qj_name] = {
            "step_id": node.step_id,
            "type": "quantum",
            "cr_name": qj_name,
        }
        self._pending_quantum[node.step_id] = {
            "quantum_job": qj_name,
            "qubits": node.resource_requirements.get("qubits"),
        }

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
            if info.get("type") != "quantum":
                continue
            step_id = info["step_id"]

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
                        self._completed_quantum_steps.add(step_id)
                        self._child_jobs.pop(child_name, None)
                        self._pending_quantum.pop(step_id, None)
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
                        self._child_jobs.pop(child_name, None)

            elif qj_phase == "Completed":
                # Already completed in a previous reconciliation pass
                self._completed_quantum_steps.add(step_id)
                self._child_jobs.pop(child_name, None)
                self._pending_quantum.pop(step_id, None)

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


# ===================================================================
# Module entry point used by docker/Dockerfile.operator
# ===================================================================

def main() -> None:
    """Run the HybridWorkflow and QuantumScheduler controllers together."""
    import os

    from src.operator.quantum_scheduler_controller import QuantumSchedulerController
    from src.operator.qpu_queue_controller import QPUQueueController
    from src.operator.metrics_server import start_metrics_server

    mode = os.environ.get("QONDUCTOR_MODE", "local")
    registry_root = os.environ.get("QONDUCTOR_REGISTRY_ROOT", "data/workflow_registry")
    scheduling_interval = int(os.environ.get("QONDUCTOR_SCHEDULING_INTERVAL", "120"))
    scheduling_threshold = int(os.environ.get("QONDUCTOR_SCHEDULING_THRESHOLD", "100"))
    metrics_port = int(os.environ.get("QONDUCTOR_METRICS_PORT", "9100"))

    quantum_controller = QuantumSchedulerController(
        mode=mode,
        scheduling_interval=scheduling_interval,
        scheduling_threshold=scheduling_threshold,
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
    start_metrics_server(workflow_controller.k8s, port=metrics_port)

    try:
        workflow_controller.run()
    finally:
        workflow_controller.stop()
        quantum_controller.stop()
        queue_controller.stop()
        quantum_thread.join(timeout=5)
        queue_thread.join(timeout=5)


if __name__ == "__main__":
    main()
