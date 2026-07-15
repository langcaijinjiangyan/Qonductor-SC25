#!/usr/bin/env python3
"""
Programmatic submission: QAOA MaxCut 20-iteration smoke test to Qonductor cluster.

Uses the Qonductor Python SDK to:
  1. Build a WorkflowImage with a dynamic SPSA driver DAG
  2. Register and deploy it
  3. Build workflow inputs (circuit, edges, parameters, 20 iter)
  4. Generate a HybridWorkflow manifest and apply via kubectl
  5. Poll for execution results
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# Ensure the project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry
from src.workflow.api import deploy, invoke
from src.workflow.qaoa_conversion import (
    build_hybrid_workflow_manifest,
    parse_qaoa_edges,
)

# ── Workload data ──────────────────────────────────────────────────────────

WORKLOAD_DIR = PROJECT_ROOT / "workload_transform_needed/quantum_approximation_optimization_algorithm_qaoa"
QASM_PATH = WORKLOAD_DIR / "qaoa_indep_qiskit_12.qasm3"

LOGICAL_CIRCUIT_ID = "qaoa_indep_qiskit_12"
PARAMETER_NAMES = ["_b_0_", "_b_1_", "_g_0_", "_g_1_"]
PARAMETER_COUNT = 4
BIT_COUNT = 12
SHOTS = 1024
MAX_ITERATIONS = 20
PRIORITY = "balanced"
SEED = 12345

# Driver code — runs inside the container image on the cluster
DRIVER_CODE = (
    "from src.workflow.qaoa_runtime import run_qaoa_spsa_driver\n"
    "run_qaoa_spsa_driver()\n"
)


def load_qasm() -> str:
    """Load the QAOA QASM3 circuit."""
    return QASM_PATH.read_text(encoding="utf-8")


def load_params() -> dict[str, Any]:
    """Load parameters_12.json."""
    with (WORKLOAD_DIR / "parameters_12.json").open(encoding="utf-8") as f:
        return json.load(f)


def build_workflow_image(name: str, driver_image: str) -> WorkflowImage:
    """Build a single-node dynamic SPSA driver WorkflowImage."""
    dag = HybridDAG(name=name)
    node = DAGNode(
        step_id="qaoa_spsa_driver",
        step_type=StepType.CLASSICAL,
        label="qaoa_spsa_driver",
        code=DRIVER_CODE,
        resource_requirements={"cpu": 1, "memory": "2Gi"},
        metadata={"dynamic_quantum_jobs": True},
    )
    dag.add_node(node)

    config = {
        "spec": {
            "priority": PRIORITY,
            "containers": [
                {
                    "name": "qaoa-spsa-driver",
                    "image": driver_image,
                    "resources": {"limits": {"cpu": "1", "memory": "2Gi"}},
                }
            ],
            "scheduling": {
                "classicalPolicy": "FilterScore",
                "quantumPolicy": "NSGA2",
            },
            "errorMitigation": {"enabled": False, "stackedTechniques": []},
        }
    }

    return WorkflowImage(
        name=name,
        dag=dag,
        quantum_code="",
        classical_code=DRIVER_CODE,
        config=config,
        metadata={
            "source_type": "legacy_slurm_workload",
            "workload_dir": str(WORKLOAD_DIR),
            "dynamic_quantum_jobs": True,
        },
        status=WorkflowStatus.CREATED,
    )


def build_workflow_inputs(qasm_text: str) -> dict[str, Any]:
    """Build workflowInputs dict matching the YAML declarative format."""
    edges = parse_qaoa_edges(qasm_text)
    params = load_params()

    return {
        "qaoa": {
            "logicalCircuitId": LOGICAL_CIRCUIT_ID,
            "qasmPath": str(QASM_PATH),
            "circuitQasm": qasm_text,
            "circuitFormat": "qasm3",
            "parameterNames": params.get("parameter_names", PARAMETER_NAMES),
            "parameterCount": params.get("parameter_count", PARAMETER_COUNT),
            "qubits": params.get("num_qubits", BIT_COUNT),
            "clbits": params.get("num_clbits", BIT_COUNT),
            "shots": SHOTS,
            "maxIterations": MAX_ITERATIONS,
            "priority": PRIORITY,
            "edges": edges,
            "seed": SEED,
            "source": {
                "workloadDir": str(WORKLOAD_DIR),
                "spec": str(WORKLOAD_DIR / "spec_12.json"),
                "parameters": str(WORKLOAD_DIR / "parameters_12.json"),
            },
        }
    }


def submit_to_cluster(
    *,
    image_name: str = "qaoa_12_dynamic_smoke20",
    workflow_name: str = "qaoa-12-dynamic-smoke20",
    driver_image: str = "qonductor-operator:latest",
    registry_root: str = "data/workflow_registry",
) -> dict[str, Any]:
    """
    Programmatic submission of QAOA workflow to the Qonductor cluster.

    Steps:
      1. Build WorkflowImage and register it
      2. Build workflow inputs
      3. Build HybridWorkflow manifest (Python dict)
      4. Apply to K8s cluster via kubectl
      5. Poll HybridWorkflow CR status until driver completes
    """
    qasm_text = load_qasm()

    # ── Step 1: Build and register the workflow image ──
    print("=" * 60)
    print("[1/5] Building WorkflowImage ...")
    image = build_workflow_image(image_name, driver_image)
    image.update_status(WorkflowStatus.DEPLOYED)

    registry = WorkflowRegistry(root_dir=str(PROJECT_ROOT / registry_root))
    image_id = registry.register(image)
    print(f"       image_id = {image_id}")

    # ── Step 2: Build workflow inputs ──
    print("[2/5] Building workflow inputs ...")
    inputs = build_workflow_inputs(qasm_text)
    print(f"       maxIterations = {MAX_ITERATIONS}")
    print(f"       shots = {SHOTS}")
    print(f"       edges = {len(inputs['qaoa']['edges'])} edges")

    # ── Step 3: Build HybridWorkflow manifest ──
    print("[3/5] Building HybridWorkflow manifest ...")
    manifest = build_hybrid_workflow_manifest(
        image, inputs, name=workflow_name, priority=PRIORITY,
    )
    manifest["metadata"]["labels"]["algorithm"] = "qaoa"

    # Write manifest to temp file for kubectl apply
    import yaml
    yaml_text = yaml.safe_dump(manifest, sort_keys=False)
    yaml_path = PROJECT_ROOT / f"workflow/{workflow_name}.yaml"
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(yaml_text, encoding="utf-8")
    print(f"       manifest written to {yaml_path}")

    # ── Step 4: Apply to cluster ──
    print("[4/5] Applying to cluster ...")
    kubectl = str(PROJECT_ROOT / "kubectl")
    result = subprocess.run(
        [kubectl, "apply", "-f", str(yaml_path)],
        capture_output=True, text=True, timeout=30,
    )
    print(f"       {result.stdout.strip()}")
    if result.returncode != 0:
        print(f"       ERROR: {result.stderr.strip()}")
        raise RuntimeError(f"kubectl apply failed: {result.stderr}")

    # ── Step 5: Poll for completion ──
    print("[5/5] Polling for completion ...")
    deadline = time.monotonic() + 900.0  # 15 min timeout
    while time.monotonic() < deadline:
        hw = subprocess.run(
            [kubectl, "get", "hybridworkflow", workflow_name,
             "-o", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if hw.returncode != 0:
            time.sleep(2)
            continue

        hw_obj = json.loads(hw.stdout)
        status = hw_obj.get("status", {})
        phase = status.get("phase", "")
        print(f"       phase={phase}  (elapsed={time.monotonic() - deadline + 900:.0f}s)")

        if phase == "Completed":
            print(f"\n✅ Workflow completed successfully!")
            results = status.get("results", {})
            print(f"   Results: {json.dumps(results, indent=2, default=str)[:2000]}")
            return {"image_id": image_id, "workflow_name": workflow_name, "status": phase, "results": results}
        elif phase == "Failed":
            print(f"\n❌ Workflow failed: {status}")
            return {"image_id": image_id, "workflow_name": workflow_name, "status": phase, "error": status}

        # Also show QuantumJob progress
        qj = subprocess.run(
            [kubectl, "get", "quantumjobs", "-l", f"workflow={workflow_name}",
             "--no-headers"],
            capture_output=True, text=True, timeout=10,
        )
        qj_lines = [l for l in qj.stdout.strip().split("\n") if l.strip()]
        completed = sum(1 for l in qj_lines if "Completed" in l)
        total = len(qj_lines)
        if total > 0:
            print(f"       QuantumJobs: {completed}/{total} completed")

        time.sleep(5)

    raise TimeoutError(f"Timed out waiting for workflow {workflow_name}")


# ── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print(f"QAOA Smoke-20: Programmatic Submission to Qonductor Cluster")
    print(f"  maxIterations = {MAX_ITERATIONS}")
    print(f"  shots         = {SHOTS}")
    print(f"  priority      = {PRIORITY}")
    print()

    result = submit_to_cluster()
    print(f"\nDone. workflowImageRef = {result['image_id']}")
