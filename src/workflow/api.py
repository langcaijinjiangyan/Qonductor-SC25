"""
Qonductor API — hardware-agnostic user-facing API.

Implements the four core functions described in Qonductor paper
Table 2 and Listing 2:

    createWorkflow     — parse hybrid code, build DAG, create workflow image
    deploy             — deploy a workflow image (validate + store)
    invoke             — invoke a deployed workflow for execution
    workflowResults    — retrieve execution results
    workflowStatus     — query current workflow execution status

Based on Qonductor paper Section 5:
"In contrast to the current standard practice, the Qonductor APIs are
hardware-agnostic and delegate hybrid resource allocation to the
Qonductor leader node."
"""

from __future__ import annotations

import inspect
import logging
import textwrap
import uuid
from typing import Any, Callable, Optional

from src.workflow.dag_engine import CodeSplitter, HybridDAG
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton registry
# ---------------------------------------------------------------------------

_default_registry: Optional[WorkflowRegistry] = None


def get_registry(root_dir: str = "data/workflow_registry") -> WorkflowRegistry:
    """Get (or lazily create) the default workflow registry."""
    global _default_registry
    if _default_registry is None:
        _default_registry = WorkflowRegistry(root_dir=root_dir)
    return _default_registry


# ---------------------------------------------------------------------------
# Runtime state (in-memory execution tracking)
# ---------------------------------------------------------------------------

_runs: dict[str, dict[str, Any]] = {}
"""In-memory store of active and completed workflow runs.

In a production system this would be persisted to etcd / the system
monitor datastore.
"""


# ---------------------------------------------------------------------------
# Extended API: executor-backed invocation
# ---------------------------------------------------------------------------

def invoke_with_controller(
    image_id: str,
    inputs: dict[str, Any] | None = None,
    mode: str = "local",
    scheduling_interval: int = 30,
    scheduling_threshold: int = 10,
) -> str:
    """Like ``invoke()`` but dispatches through the HybridWorkflowController
    to actually execute the workflow (not just store the plan).

    In ``local`` mode the controller simulates K8s behaviour in-process:
    - Classical steps are executed as simulated K8s Jobs.
    - Quantum steps are submitted to the QuantumSchedulerController,
      which runs NSGA-II scheduling against FakeBackends.

    In ``k8s`` mode this creates a real HybridWorkflow CR in the cluster.

    Returns:
        A run_id for use with ``workflowResults()``.
    """
    from src.operator.controller import HybridWorkflowController
    from src.operator.quantum_scheduler_controller import QuantumSchedulerController

    run_id = uuid.uuid4().hex[:16]

    if mode == "k8s":
        run_id = invoke(image_id, inputs=inputs, mode="k8s",
                        scheduling_interval=scheduling_interval,
                        scheduling_threshold=scheduling_threshold)
        return run_id

    # Local mode: run the entire pipeline in-process.
    controller = HybridWorkflowController(mode="local")
    quantum_ctl = QuantumSchedulerController(
        mode="local",
        scheduling_interval=scheduling_interval,
        scheduling_threshold=scheduling_threshold,
    )

    # Wire the quantum scheduler to the controller.
    def _on_quantum_job(qj_cr):
        quantum_ctl._enqueue(qj_cr)
        quantum_ctl._check_trigger()

    controller.register_quantum_scheduler_callback(_on_quantum_job)

    # Run the workflow through the controller.
    controller.process_workflow(image_id, inputs=inputs)

    # Collect quantum scheduling results.
    results = {
        "workflow_status": "Completed",
        "quantum_results": quantum_ctl._scheduling_results,
    }
    _runs[run_id] = {
        "image_id": image_id,
        "status": WorkflowStatus.COMPLETED,
        "mode": "local",
        "plan": [],
        "inputs": inputs or {},
        "results": results,
        "current_step": 0,
    }
    return run_id


# ---------------------------------------------------------------------------
# Core API functions
# ---------------------------------------------------------------------------

def createWorkflow(
    code_or_functions: str | list[Callable],
    config: dict[str, Any] | None = None,
    name: str = "",
) -> WorkflowImage:
    """Create a hybrid workflow image from source code or functions.

    This is the primary entry point for users. It accepts either:

    * A string of hybrid Python source code, or
    * A list of Python callables (functions / callable objects like
      ``QAOA(...)``, ``ZNE.apply(...)``, etc.) that together form a
      hybrid workflow.

    The Workflow Manager parses the code, identifies quantum vs.
    classical sections, builds a DAG, and packages everything into
    a ``WorkflowImage``.

    Corresponds to Table 2: "Create a workflow with hybrid code."

    Args:
        code_or_functions: Hybrid Python source code as a string, or a
            list of callables representing workflow steps.
        config: Deployment configuration dict (YAML-parsed). See
            Listing 1 in the paper for schema.
        name: Optional human-readable name for the workflow.

    Returns:
        A ``WorkflowImage`` ready for ``deploy()``.

    Example (matching Listing 2 from paper)::

        qaoa = QAOA(qubits=10, optimizer='COBYLA')
        zne_circuit = ZNE.apply(qaoa, noise_factors=(1, 3, 5))
        pre_process = DD.apply(zne_circuit, sequence_type="XpXm")
        rem_corrected = partial(REM.post_select(counts))
        post_process = ZNE.inference(rem_corrected, "LinearFactory")

        with open('deployment.yaml', 'r') as f:
            config = yaml.safe_load(f)

        hwi = createWorkflow([pre_process, qaoa, post_process], config)
    """
    if config is None:
        config = {}

    splitter = CodeSplitter()

    if isinstance(code_or_functions, str):
        # String input — parse directly.
        source = code_or_functions
    elif isinstance(code_or_functions, list):
        # List of callables — extract source from each.
        source = _extract_source_from_callables(code_or_functions)
    else:
        raise TypeError(
            "code_or_functions must be a string or a list of callables, "
            f"got {type(code_or_functions)}"
        )

    # 1. Build DAG
    dag = splitter.parse(source, name=name or "hybrid_workflow")

    # 2. Split into quantum / classical code files
    classical_code, quantum_code = splitter.split_files(dag)

    # 3. Package into a workflow image
    image = WorkflowImage(
        name=name or "hybrid_workflow",
        dag=dag,
        quantum_code=quantum_code,
        classical_code=classical_code,
        config=config,
        metadata={
            "source_type": "string" if isinstance(code_or_functions, str) else "callables",
            "api_version": "1.0",
        },
        status=WorkflowStatus.CREATED,
    )

    # 4. Register in the workflow registry
    registry = get_registry()
    registry.register(image)

    logger.info("Created workflow image %s (%s)", image.image_id, image.name)
    return image


def deploy(workflow_image: WorkflowImage, register: bool = True) -> str:
    """Deploy a workflow image.

    Corresponds to Table 2: "Deploy a workflow."

    Validates the image, updates its status, and optionally stores it
    in the registry. Returns the ``workflowID`` (image_id).

    Args:
        workflow_image: The image to deploy.
        register: If True, register (or update) in the registry.

    Returns:
        The ``workflow_id`` string.
    """
    # Validation
    if not workflow_image.dag.nodes:
        raise ValueError("Cannot deploy empty workflow (no nodes in DAG).")
    if not workflow_image.dag.edges:
        logger.warning("Deploying workflow with no edges — standalone steps.")

    workflow_image.update_status(WorkflowStatus.DEPLOYED)

    if register:
        registry = get_registry()
        registry.register(workflow_image)

    logger.info("Deployed workflow %s", workflow_image.image_id)
    return workflow_image.image_id


def invoke(
    image_id: str,
    inputs: dict[str, Any] | None = None,
    mode: str = "local",
    scheduling_interval: int = 30,
    scheduling_threshold: int = 10,
) -> str:
    """Invoke (run) a deployed workflow.

    Corresponds to Table 2: "Invoke a workflow."

    Supports two modes:

    ``mode="local"`` (default):
        Produces a structured execution plan stored in the ``_runs`` dict.
        Optionally calls the ``JobManager`` (HybridWorkflowController) to
        execute the workflow in-process with simulated scheduling.

    ``mode="k8s"``:
        Creates a ``HybridWorkflow`` CR in the K8s API. The Qonductor
        Operator watches the CR and drives execution:
        - CLASSICAL steps → K8s Jobs → kube-scheduler (Filter-Scoring)
        - QUANTUM steps   → QuantumJob CRs → QuantumSchedulerController
          (NSGA-II multi-objective optimization)

    Args:
        image_id: The workflow image ID to invoke.
        inputs: Optional input data for the workflow (key-value pairs).
        mode: Execution mode — "local" or "k8s".
        scheduling_interval: K8s mode: scheduling interval in seconds.
        scheduling_threshold: K8s mode: queue depth trigger.

    Returns:
        A ``run_id`` string that can be used with ``workflowStatus()``
        and ``workflowResults()``. In K8s mode this corresponds to the
        HybridWorkflow CR name.
    """
    registry = get_registry()
    image = registry.get(image_id)
    if image is None:
        raise ValueError(f"Workflow image '{image_id}' not found in registry.")

    if image.status != WorkflowStatus.DEPLOYED:
        logger.warning(
            "Invoking workflow in state '%s' (expected 'deployed').",
            image.status.value,
        )

    image.update_status(WorkflowStatus.RUNNING)
    registry.update_status(image_id, WorkflowStatus.RUNNING)

    if mode == "k8s":
        # ---- K8s mode: create HybridWorkflow CR → Operator takes over ----
        from src.operator.k8s_client import create_hybrid_workflow_cr

        # Build container specs from the workflow image config.
        containers = image.config.get("spec", {}).get("containers", [])
        priority = image.config.get("spec", {}).get("priority", "balanced")

        cr = create_hybrid_workflow_cr(
            image_id=image_id,
            name=f"qonductor-{image.name}-{uuid.uuid4().hex[:6]}",
            priority=priority,
            containers=containers,
            workflow_inputs=inputs or {},
            mode="k8s",
        )
        run_id = cr["metadata"]["name"]

        _runs[run_id] = {
            "image_id": image_id,
            "status": WorkflowStatus.RUNNING,
            "mode": "k8s",
            "cr_name": cr["metadata"]["name"],
            "plan": [],
            "inputs": inputs or {},
            "results": {},
            "current_step": 0,
        }

        logger.info(
            "Invoked workflow '%s' → HybridWorkflow CR '%s' (%s mode)",
            image.name, run_id, mode,
        )
        return run_id

    # ---- Local mode: in-process execution ----
    run_id = uuid.uuid4().hex[:16]

    # Build execution plan from topologically sorted DAG.
    execution_order = image.dag.topological_order()
    plan = []
    for i, node in enumerate(execution_order):
        plan.append({
            "step_index": i,
            "step_id": node.step_id,
            "step_type": node.step_type.value,
            "label": node.label,
            "code": node.code,
            "inputs": node.inputs,
            "outputs": node.outputs,
            "resource_requirements": node.resource_requirements,
            "depends_on": [
                e.from_step
                for e in image.dag.edges
                if e.to_step == node.step_id
            ],
        })

    _runs[run_id] = {
        "image_id": image_id,
        "status": WorkflowStatus.RUNNING,
        "mode": "local",
        "plan": plan,
        "inputs": inputs or {},
        "results": {},
        "current_step": 0,
    }

    logger.info(
        "Invoked workflow '%s' → run %s (%d steps)",
        image.name, run_id, len(plan),
    )
    return run_id


def workflowResults(run_id: str) -> dict[str, Any]:
    """Retrieve the results of a completed workflow run.

    Corresponds to Table 2: "Get the workflow results."

    Args:
        run_id: The run ID returned by ``invoke()``.

    Returns:
        A dict with keys ``status``, ``results``, ``steps_completed``,
        ``image_id``, and optional ``errors``.

    Raises:
        KeyError: If *run_id* is unknown.
    """
    run = _runs[run_id]
    return {
        "status": run["status"].value,
        "results": run["results"],
        "steps_completed": run["current_step"],
        "total_steps": len(run["plan"]),
        "image_id": run["image_id"],
    }


def workflowStatus(run_id: str) -> str:
    """Query the current execution status of a workflow.

    Corresponds to the ``workflowStatus`` check in Listing 2.

    Args:
        run_id: The run ID returned by ``invoke()``.

    Returns:
        The status string: one of 'running', 'completed', 'failed'.

    Raises:
        KeyError: If *run_id* is unknown.
    """
    run = _runs[run_id]
    return run["status"].value


# ---------------------------------------------------------------------------
# Convenience: update run progress
# ---------------------------------------------------------------------------

def _update_run_step(run_id: str, step_index: int,
                     result: Any = None, error: str = "") -> None:
    """Internal: advance a run's progress after a step completes."""
    run = _runs.get(run_id)
    if run is None:
        return
    run["current_step"] = step_index + 1
    if result is not None:
        run["results"][f"step_{step_index}"] = result
    if error:
        run["results"][f"step_{step_index}_error"] = error
    if run["current_step"] >= len(run["plan"]):
        run["status"] = WorkflowStatus.COMPLETED
    if error:
        run["status"] = WorkflowStatus.FAILED


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_source_from_callables(callables: list[Callable]) -> str:
    """Extract source code from a list of callables.

    Uses ``inspect.getsource`` where available; falls back to repr.
    Stitches the fragments together into a single synthetic Python
    source string that the ``CodeSplitter`` can parse.
    """
    fragments: list[str] = []
    fragments.append(
        "# Auto-generated hybrid workflow from callables\n"
    )

    for i, fn in enumerate(callables):
        try:
            src = inspect.getsource(fn)
            # Dedent to avoid indentation issues.
            src = textwrap.dedent(src)
        except (TypeError, OSError):
            # Fallback for builtins / C extensions / lambdas.
            src = f"# [opaque callable: {getattr(fn, '__name__', repr(fn))}]\n"
            src += f"step_{i} = {fn!r}\n"

        # Annotate with resource requirements if the callable has them.
        comment = ""
        if hasattr(fn, 'qubits'):
            comment = f"  # @resource qubits: {fn.qubits}"
        elif hasattr(fn, 'num_qubits'):
            comment = f"  # @resource qubits: {fn.num_qubits}"

        fragments.append(f"\n# --- Step {i}: {getattr(fn, '__name__', repr(fn))} ---{comment}")
        fragments.append(src)

    return "\n".join(fragments)


# ---------------------------------------------------------------------------
# Listing 2 – style example runner
# ---------------------------------------------------------------------------

def run_example_listing2() -> None:
    """Run the example from Listing 2 of the Qonductor paper.

    Demonstrates the full user-facing API::

        createWorkflow → deploy → poll workflowStatus → workflowResults
    """
    import yaml

    # Simulated functions that would normally come from the library.
    def pre_process():
        pass  # DD, ZNE application (classical)

    def qaoa_circuit():
        pass  # Quantum circuit execution

    def post_process():
        pass  # ZNE inference / REM (classical)

    config = {
        "spec": {
            "containers": [
                {
                    "name": "qaoa-error-mitigated",
                    "image": "nvidia/cuda:11.0-base",
                    "resources": {
                        "limits": {
                            "nvidia.com/gpu": 1,
                        }
                    }
                },
                {
                    "name": "qaoa-algorithm",
                    "image": "qaoa:latest",
                    "resources": {
                        "limits": {
                            "quantum.ibm.com/qpu": 1,
                            "qubits": 20,
                        }
                    }
                }
            ]
        }
    }

    # 1. Create workflow image
    hwi = createWorkflow(
        [pre_process, qaoa_circuit, post_process],
        config=config,
        name="qaoa_error_mitigated",
    )
    print(f"[1] Created: {hwi}")

    # 2. Deploy
    workflow_id = deploy(hwi)
    print(f"[2] Deployed → workflowID: {workflow_id}")

    # 3. Invoke
    run_id = invoke(workflow_id)
    print(f"[3] Invoked → runID: {run_id}")

    # 4. Poll for completion (simulated)
    import time
    for _ in range(5):
        status = workflowStatus(run_id)
        print(f"    status: {status}")
        if status != "running":
            break
        time.sleep(0.5)

    # 5. Get results
    results = workflowResults(run_id)
    print(f"[4] Results: {results}")
