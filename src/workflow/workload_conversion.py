"""Convert legacy workload directories into Qonductor workflow manifests."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import yaml

from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
from src.workflow.qaoa_conversion import (
    build_qaoa_workflow_inputs,
    build_hybrid_workflow_manifest,
    register_qaoa_workflow,
)
from src.workflow.vqe_runtime import DEFAULT_H_COUPLING, DEFAULT_H_FIELD
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry

VQE_DRIVER_CODE = """from src.workflow.vqe_runtime import run_vqe_spsa_driver\nrun_vqe_spsa_driver()\n"""


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _k8s_name(value: str) -> str:
    name = re.sub(r"[^a-z0-9-]+", "-", value.lower().replace("_", "-"))
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:63].rstrip("-") or "workflow"


def _workflow_py_name(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_") or "workflow"


def _read_workload_payload(workload_dir: str | Path) -> tuple[dict[str, Any], dict[str, Any], str, str, str]:
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
    return spec, params, logical_id, str(qasm_path), qasm_text


def build_ansatz_workflow_inputs(
    workload_dir: str | Path,
    *,
    shots: int = 1024,
    max_iterations: int = 200,
    priority: str = "balanced",
    quantum_timeout_seconds: float = 21600.0,
    poll_seconds: float = 5.0,
) -> dict[str, Any]:
    workload_dir = Path(workload_dir)
    spec, params, logical_id, qasm_path, qasm_text = _read_workload_payload(workload_dir)
    key = params.get("benchmark", workload_dir.name)
    spec_path = workload_dir / "spec_12.json"
    params_path = workload_dir / "parameters_12.json"

    return {
        key: {
            "logicalCircuitId": logical_id,
            "qasmPath": qasm_path,
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
            "objective": "ising_energy",
            "hamiltonian": {
                "hField": list(DEFAULT_H_FIELD),
                "hCoupling": list(DEFAULT_H_COUPLING),
                "topology": "ring",
            },
            "source": {
                "workloadDir": str(workload_dir),
                "spec": str(spec_path),
                "parameters": str(params_path),
            },
        }
    }


def build_ansatz_workflow_image(
    workload_dir: str | Path,
    *,
    name: str,
    driver_image: str = "qonductor-operator:latest",
) -> WorkflowImage:
    dag = HybridDAG(name=name)
    node = DAGNode(
        step_id="vqe_spsa_driver",
        step_type=StepType.CLASSICAL,
        label="vqe_spsa_driver",
        code=VQE_DRIVER_CODE,
        resource_requirements={"cpu": 1, "memory": "1Gi"},
        metadata={"dynamic_quantum_jobs": True},
    )
    dag.add_node(node)

    config = {
        "spec": {
            "priority": "balanced",
            "containers": [
                {
                    "name": "vqe-spsa-driver",
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
        classical_code=VQE_DRIVER_CODE,
        config=config,
        metadata={
            "source_type": "legacy_slurm_workload",
            "workload_dir": str(Path(workload_dir)),
            "dynamic_quantum_jobs": True,
        },
        status=WorkflowStatus.CREATED,
    )


def register_ansatz_workflow(
    workload_dir: str | Path,
    *,
    registry_root: str | Path = "data/workflow_registry",
    name: str,
    driver_image: str = "qonductor-operator:latest",
) -> WorkflowImage:
    image = build_ansatz_workflow_image(
        workload_dir, name=name, driver_image=driver_image,
    )
    image.update_status(WorkflowStatus.DEPLOYED)
    registry = WorkflowRegistry(root_dir=registry_root)
    registry.register(image)
    return image


def convert_workload(
    workload_dir: str | Path,
    *,
    registry_root: str | Path = "data/workflow_registry",
    output_dir: str | Path = "workflow",
    driver_image: str = "qonductor-operator:latest",
    shots: int = 1024,
    max_iterations: int = 200,
    priority: str = "balanced",
) -> tuple[WorkflowImage, dict[str, Any], Path]:
    workload_dir = Path(workload_dir)
    params = load_json(workload_dir / "parameters_12.json")
    benchmark = params.get("benchmark", workload_dir.name)
    workflow_name = _k8s_name(f"{workload_dir.name}-12-dynamic")
    image_name = _workflow_py_name(f"{workload_dir.name}_12_dynamic")
    output_path = Path(output_dir) / f"{workflow_name}.yaml"

    if benchmark == "qaoa":
        image = register_qaoa_workflow(
            workload_dir,
            registry_root=registry_root,
            name=image_name,
            driver_image=driver_image,
        )
        inputs = build_qaoa_workflow_inputs(
            workload_dir,
            shots=shots,
            max_iterations=max_iterations,
            priority=priority,
        )
        manifest = build_hybrid_workflow_manifest(
            image,
            inputs,
            name=workflow_name,
            priority=priority,
        )
    else:
        image = register_ansatz_workflow(
            workload_dir,
            registry_root=registry_root,
            name=image_name,
            driver_image=driver_image,
        )
        inputs = build_ansatz_workflow_inputs(
            workload_dir,
            shots=shots,
            max_iterations=max_iterations,
            priority=priority,
        )
        manifest = build_hybrid_workflow_manifest(
            image,
            inputs,
            name=workflow_name,
            priority=priority,
        )
        manifest["metadata"]["labels"]["algorithm"] = benchmark

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    return image, manifest, output_path


def discover_workloads(root: str | Path) -> list[Path]:
    root = Path(root)
    return sorted(
        path for path in root.iterdir()
        if path.is_dir() and (path / "spec_12.json").exists() and (path / "parameters_12.json").exists()
    )


def convert_all_workloads(
    root: str | Path = "workload_transform_needed",
    *,
    registry_root: str | Path = "data/workflow_registry",
    output_dir: str | Path = "workflow",
    driver_image: str = "qonductor-operator:latest",
    shots: int = 1024,
    max_iterations: int = 200,
    priority: str = "balanced",
) -> list[tuple[WorkflowImage, dict[str, Any], Path]]:
    return [
        convert_workload(
            workload_dir,
            registry_root=registry_root,
            output_dir=output_dir,
            driver_image=driver_image,
            shots=shots,
            max_iterations=max_iterations,
            priority=priority,
        )
        for workload_dir in discover_workloads(root)
    ]


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="workload_transform_needed")
    parser.add_argument("--registry-root", default="data/workflow_registry")
    parser.add_argument("--output-dir", default="workflow")
    parser.add_argument("--driver-image", default="qonductor-operator:latest")
    parser.add_argument("--shots", type=int, default=1024)
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument(
        "--priority", choices=("balanced", "fidelity", "jct"), default="balanced",
    )
    args = parser.parse_args(argv)

    converted = convert_all_workloads(
        args.root,
        registry_root=args.registry_root,
        output_dir=args.output_dir,
        driver_image=args.driver_image,
        shots=args.shots,
        max_iterations=args.max_iterations,
        priority=args.priority,
    )
    for image, _manifest, output_path in converted:
        print(f"{output_path}: workflowImageRef={image.image_id}")


if __name__ == "__main__":
    main()
