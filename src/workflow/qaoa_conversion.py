"""Convert legacy QAOA Slurm workloads into Qonductor workflows."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import yaml

from src.workflow.dag_engine import DAGNode, DAGEdge, HybridDAG, StepType
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry

DRIVER_CODE = """from src.workflow.qaoa_runtime import run_qaoa_spsa_driver\nrun_qaoa_spsa_driver()\n"""


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def parse_qaoa_edges(qasm_text: str) -> list[list[int]]:
    """Extract the MaxCut edge list from the first QAOA cost layer."""
    edges: list[list[int]] = []
    seen: set[tuple[int, int]] = set()
    pattern = re.compile(r"rzz\(2\*_g_0_\)\s+q\[(\d+)\],\s*q\[(\d+)\]")
    for match in pattern.finditer(qasm_text):
        edge = (int(match.group(1)), int(match.group(2)))
        if edge not in seen:
            seen.add(edge)
            edges.append([edge[0], edge[1]])
    return edges


def build_qaoa_workflow_inputs(
    workload_dir: str | Path,
    *,
    shots: int = 1024,
    max_iterations: int = 200,
    priority: str = "balanced",
    quantum_timeout_seconds: float = 21600.0,
    poll_seconds: float = 5.0,
) -> dict[str, Any]:
    workload_dir = Path(workload_dir)
    spec_path = workload_dir / "spec_12.json"
    params_path = workload_dir / "parameters_12.json"
    spec = load_json(spec_path)
    params = load_json(params_path)

    circuits = spec.get("circuits", {})
    if not circuits:
        raise ValueError(f"No circuits found in {spec_path}")
    logical_id, qasm_file = next(iter(circuits.items()))
    qasm_path = workload_dir / qasm_file
    qasm_text = qasm_path.read_text(encoding="utf-8")
    edges = parse_qaoa_edges(qasm_text)

    return {
        "qaoa": {
            "logicalCircuitId": logical_id,
            "qasmPath": str(qasm_path),
            "circuitQasm": qasm_text,
            "circuitFormat": "qasm3",
            "parameterNames": params.get("parameter_names", []),
            "parameterCount": params.get("parameter_count", 0),
            "qubits": params.get("num_qubits", int(spec.get("qnode", 12))),
            "clbits": params.get("num_clbits", params.get("num_qubits", 12)),
            "shots": shots,
            "maxIterations": max_iterations,
            "quantumTimeoutSeconds": quantum_timeout_seconds,
            "pollSeconds": poll_seconds,
            "priority": priority,
            "edges": edges,
            "source": {
                "workloadDir": str(workload_dir),
                "spec": str(spec_path),
                "parameters": str(params_path),
            },
        }
    }


def build_qaoa_workflow_image(
    workload_dir: str | Path,
    *,
    name: str = "qaoa_12_dynamic",
    driver_image: str = "qonductor-operator:latest",
) -> WorkflowImage:
    """Build a compact workflow image with one dynamic classical driver.

    The driver submits runtime QuantumJob children for plus/minus SPSA
    evaluations, so the DAG does not contain one node per iteration.
    """
    dag = HybridDAG(name=name)
    node = DAGNode(
        step_id="qaoa_spsa_driver",
        step_type=StepType.CLASSICAL,
        label="qaoa_spsa_driver",
        code=DRIVER_CODE,
        resource_requirements={"cpu": 1, "memory": "1Gi"},
        metadata={"dynamic_quantum_jobs": True},
    )
    dag.add_node(node)

    config = {
        "spec": {
            "priority": "balanced",
            "containers": [
                {
                    "name": "qaoa-spsa-driver",
                    "image": driver_image,
                    "resources": {
                        "limits": {"cpu": "1", "memory": "1Gi"},
                    },
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
            "source_type": "qaoa_slurm_workload",
            "workload_dir": str(Path(workload_dir)),
            "dynamic_quantum_jobs": True,
        },
        status=WorkflowStatus.CREATED,
    )


def register_qaoa_workflow(
    workload_dir: str | Path,
    *,
    registry_root: str | Path = "data/workflow_registry",
    name: str = "qaoa_12_dynamic",
    driver_image: str = "qonductor-operator:latest",
) -> WorkflowImage:
    image = build_qaoa_workflow_image(
        workload_dir, name=name, driver_image=driver_image,
    )
    image.update_status(WorkflowStatus.DEPLOYED)
    registry = WorkflowRegistry(root_dir=registry_root)
    registry.register(image)
    return image


def build_hybrid_workflow_manifest(
    image: WorkflowImage,
    workflow_inputs: dict[str, Any],
    *,
    name: str = "qaoa-12-dynamic",
    priority: str = "balanced",
    max_retries: int = 100,
) -> dict[str, Any]:
    containers = image.config.get("spec", {}).get("containers", [])
    return {
        "apiVersion": "qonductor.io/v1",
        "kind": "HybridWorkflow",
        "metadata": {
            "name": name,
            "namespace": "default",
            "labels": {"app": "qonductor", "algorithm": "qaoa"},
        },
        "spec": {
            "workflowImageRef": image.image_id,
            "priority": priority,
            "maxRetries": max_retries,
            "containers": containers,
            "scheduling": {
                "classicalPolicy": "FilterScore",
                "quantumPolicy": "NSGA2",
                "schedulingInterval": 120,
                "schedulingThreshold": 1,
            },
            "errorMitigation": {"enabled": False, "stackedTechniques": []},
            "workflowInputs": workflow_inputs,
        },
    }


def convert_qaoa_workload(
    workload_dir: str | Path,
    *,
    registry_root: str | Path = "data/workflow_registry",
    output_yaml: str | Path = "deploy/examples/qaoa-12-dynamic-workflow.yaml",
    name: str = "qaoa_12_dynamic",
    workflow_name: str = "qaoa-12-dynamic",
    driver_image: str = "qonductor-operator:latest",
    shots: int = 1024,
    max_iterations: int = 200,
    priority: str = "balanced",
) -> tuple[WorkflowImage, dict[str, Any]]:
    image = register_qaoa_workflow(
        workload_dir,
        registry_root=registry_root,
        name=name,
        driver_image=driver_image,
    )
    inputs = build_qaoa_workflow_inputs(
        workload_dir,
        shots=shots,
        max_iterations=max_iterations,
        priority=priority,
    )
    manifest = build_hybrid_workflow_manifest(
        image, inputs, name=workflow_name, priority=priority,
    )
    output_yaml = Path(output_yaml)
    output_yaml.parent.mkdir(parents=True, exist_ok=True)
    output_yaml.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    return image, manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "workload_dir",
        nargs="?",
        default="workload/quantum_approximation_optimization_algorithm_qaoa",
    )
    parser.add_argument("--registry-root", default="data/workflow_registry")
    parser.add_argument("--output-yaml", default="deploy/examples/qaoa-12-dynamic-workflow.yaml")
    parser.add_argument("--name", default="qaoa_12_dynamic")
    parser.add_argument("--workflow-name", default="qaoa-12-dynamic")
    parser.add_argument("--driver-image", default="qonductor-operator:latest")
    parser.add_argument("--shots", type=int, default=1024)
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument(
        "--priority", choices=("balanced", "fidelity", "jct"), default="balanced",
    )
    args = parser.parse_args(argv)

    image, _manifest = convert_qaoa_workload(
        args.workload_dir,
        registry_root=args.registry_root,
        output_yaml=args.output_yaml,
        name=args.name,
        workflow_name=args.workflow_name,
        driver_image=args.driver_image,
        shots=args.shots,
        max_iterations=args.max_iterations,
        priority=args.priority,
    )
    print(f"workflowImageRef: {image.image_id}")
    print(f"hybridWorkflowYaml: {args.output_yaml}")


if __name__ == "__main__":
    main()
