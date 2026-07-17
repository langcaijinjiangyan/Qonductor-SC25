"""Shared helpers for qslurm-to-Qonductor Python workflow entries."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


def find_project_root(start: Path | None = None) -> Path:
    cursor = (start or Path(__file__)).resolve()
    for path in (cursor, *cursor.parents):
        if (path / "src" / "workflow").exists():
            return path
    raise RuntimeError("Could not locate project root containing src/workflow")


PROJECT_ROOT = find_project_root()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def k8s_name(value: str, max_length: int = 63) -> str:
    name = re.sub(r"[^a-z0-9-]+", "-", value.lower().replace("_", "-"))
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:max_length].rstrip("-") or "workflow"


def py_name(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_") or "workflow"


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def load_spec_and_qasm(workload_dir: Path, spec_file: str) -> tuple[dict[str, Any], str, Path, str]:
    spec_path = workload_dir / spec_file
    spec = load_json(spec_path)
    circuits = spec.get("circuits", {})
    if not circuits:
        raise ValueError(f"No circuits in {spec_path}")
    logical_id, qasm_file = next(iter(circuits.items()))
    qasm_path = workload_dir / qasm_file
    qasm_text = qasm_path.read_text(encoding="utf-8")
    return spec, logical_id, qasm_path, qasm_text


def qasm_format(qasm_path: Path, qasm_text: str) -> str:
    if qasm_path.suffix == ".qasm3" or qasm_text.lstrip().startswith("OPENQASM 3"):
        return "qasm3"
    return "qasm2"


def build_manifest(
    image: Any,
    workflow_inputs: dict[str, Any],
    *,
    workflow_name: str,
    priority: str,
    algorithm: str,
) -> dict[str, Any]:
    containers = image.config.get("spec", {}).get("containers", [])
    return {
        "apiVersion": "qonductor.io/v1",
        "kind": "HybridWorkflow",
        "metadata": {
            "name": workflow_name,
            "namespace": "default",
            "labels": {
                "app": "qonductor",
                "algorithm": k8s_name(algorithm),
                "converted_from": "qslurm_workloads",
            },
        },
        "spec": {
            "workflowImageRef": image.image_id,
            "priority": priority,
            "maxRetries": 3,
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


def register_image(image: Any, registry_root: str) -> str:
    from src.workflow.workflow_image import WorkflowStatus
    from src.workflow.workflow_registry import WorkflowRegistry

    image.update_status(WorkflowStatus.DEPLOYED)
    WorkflowRegistry(root_dir=PROJECT_ROOT / registry_root).register(image)
    return image.image_id


def build_pure_quantum_image(
    *,
    workflow_name: str,
    logical_id: str,
    qasm_text: str,
    circuit_format: str,
    qubits: int,
    shots: int,
    source: dict[str, Any],
) -> Any:
    from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
    from src.workflow.workflow_image import WorkflowImage, WorkflowStatus

    dag = HybridDAG(name=py_name(workflow_name))
    step_id = py_name(logical_id)
    quantum_code = (
        "# Quantum circuit payload is carried in the QuantumJob CR metadata.\n"
        "def execute_quantum_circuit():\n"
        "    pass\n"
    )
    node = DAGNode(
        step_id=step_id,
        step_type=StepType.QUANTUM,
        label=logical_id,
        code=quantum_code,
        resource_requirements={"qubits": qubits, "shots": shots},
        metadata={
            "logicalCircuitId": logical_id,
            "circuitNames": [logical_id],
            "circuitQasm": qasm_text,
            "circuitFormat": circuit_format,
        },
    )
    dag.add_node(node)
    return WorkflowImage(
        name=py_name(workflow_name),
        dag=dag,
        quantum_code=quantum_code,
        classical_code="",
        config={
            "spec": {
                "priority": "balanced",
                "scheduling": {
                    "classicalPolicy": "FilterScore",
                    "quantumPolicy": "NSGA2",
                },
                "errorMitigation": {"enabled": False, "stackedTechniques": []},
            }
        },
        metadata={
            "source_type": "qslurm_pure_quantum_workload",
            "source": source,
        },
        status=WorkflowStatus.CREATED,
    )


def pure_quantum_inputs(
    *,
    logical_id: str,
    qasm_path: Path,
    qasm_text: str,
    circuit_format: str,
    qubits: int,
    shots: int,
    priority: str,
    source: dict[str, Any],
) -> dict[str, Any]:
    return {
        "quantum": {
            "logicalCircuitId": logical_id,
            "qasmPath": str(qasm_path),
            "circuitQasm": qasm_text,
            "circuitFormat": circuit_format,
            "qubits": qubits,
            "shots": shots,
            "priority": priority,
            "source": source,
        }
    }


def build_hybrid_image(
    *,
    workflow_name: str,
    benchmark: str,
    driver_kind: str,
    source: dict[str, Any],
) -> Any:
    from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
    from src.workflow.workflow_image import WorkflowImage, WorkflowStatus

    if driver_kind == "qaoa":
        step_id = "qaoa_spsa_driver"
        code = "from src.workflow.qaoa_runtime import run_qaoa_spsa_driver\nrun_qaoa_spsa_driver()\n"
        container_name = "qaoa-spsa-driver"
    else:
        step_id = "vqe_spsa_driver"
        code = "from src.workflow.vqe_runtime import run_vqe_spsa_driver\nrun_vqe_spsa_driver()\n"
        container_name = "vqe-spsa-driver"

    dag = HybridDAG(name=py_name(workflow_name))
    node = DAGNode(
        step_id=step_id,
        step_type=StepType.CLASSICAL,
        label=step_id,
        code=code,
        resource_requirements={"cpu": 1, "memory": "1Gi"},
        metadata={"dynamic_quantum_jobs": True},
    )
    dag.add_node(node)
    return WorkflowImage(
        name=py_name(workflow_name),
        dag=dag,
        quantum_code="",
        classical_code=code,
        config={
            "spec": {
                "priority": "balanced",
                "containers": [
                    {
                        "name": container_name,
                        "image": "qonductor-operator:latest",
                        "resources": {"limits": {"cpu": "1", "memory": "1Gi"}},
                    }
                ],
                "scheduling": {
                    "classicalPolicy": "FilterScore",
                    "quantumPolicy": "NSGA2",
                },
                "errorMitigation": {"enabled": False, "stackedTechniques": []},
            }
        },
        metadata={
            "source_type": "qslurm_hybrid_workload",
            "benchmark": benchmark,
            "driver_kind": driver_kind,
            "dynamic_quantum_jobs": True,
            "source": source,
        },
        status=WorkflowStatus.CREATED,
    )


def parse_qaoa_edges(qasm_text: str) -> list[list[int]]:
    edges: list[list[int]] = []
    seen: set[tuple[int, int]] = set()
    pattern = re.compile(r"rzz\(2\*_g_0_\)\s+q\[(\d+)\],\s*q\[(\d+)\]")
    for match in pattern.finditer(qasm_text):
        edge = (int(match.group(1)), int(match.group(2)))
        if edge not in seen:
            seen.add(edge)
            edges.append([edge[0], edge[1]])
    return edges


def hybrid_inputs(
    *,
    workload_dir: Path,
    spec_file: str,
    parameters_file: str,
    shots: int,
    max_iterations: int,
    priority: str,
) -> tuple[dict[str, Any], str, str]:
    spec, logical_id, qasm_path, qasm_text = load_spec_and_qasm(workload_dir, spec_file)
    params = load_json(workload_dir / parameters_file)
    benchmark = params.get("benchmark", workload_dir.name)
    driver_kind = "qaoa" if benchmark == "qaoa" else "vqe"
    source = {
        "workloadDir": str(workload_dir),
        "spec": str(workload_dir / spec_file),
        "parameters": str(workload_dir / parameters_file),
    }
    key = "qaoa" if driver_kind == "qaoa" else benchmark
    cfg: dict[str, Any] = {
        "logicalCircuitId": logical_id,
        "qasmPath": str(qasm_path),
        "circuitQasm": qasm_text,
        "circuitFormat": qasm_format(qasm_path, qasm_text),
        "parameterNames": params.get("parameter_names", []),
        "parameterCount": params.get("parameter_count", 0),
        "qubits": params.get("num_qubits", int(spec.get("qnode", 12))),
        "clbits": params.get("num_clbits", params.get("num_qubits", 12)),
        "shots": shots,
        "maxIterations": max_iterations,
        "priority": priority,
        "seed": 12345,
        "source": source,
    }
    if driver_kind == "qaoa":
        cfg["edges"] = parse_qaoa_edges(qasm_text)
    else:
        cfg["objective"] = "ising_energy"
        cfg["hamiltonian"] = {
            "hField": [0.7, -0.45, 0.32, -0.58, 0.91, -0.27, 0.63, -0.74, 0.18, 0.52, -0.36, 0.81],
            "hCoupling": [-1.05, 0.86, -0.67, 0.49, -0.92, 0.73, -0.54, 1.11, -0.38, 0.64, -0.79, 0.57],
            "topology": "ring",
        }
    return {key: cfg}, benchmark, driver_kind


def run_pure_direct(workload_dir: Path, spec_file: str, shots: int) -> dict[str, Any]:
    spec, logical_id, qasm_path, qasm_text = load_spec_and_qasm(workload_dir, spec_file)
    fmt = qasm_format(qasm_path, qasm_text)
    if fmt == "qasm3":
        from qiskit import qasm3
        circuit = qasm3.loads(qasm_text)
    else:
        from qiskit import qasm2
        circuit = qasm2.loads(qasm_text)
    from qiskit_aer import AerSimulator

    backend = AerSimulator(method="automatic")
    result = backend.run(circuit, shots=shots).result()
    counts = result.get_counts()
    return {"logicalCircuitId": logical_id, "shots": shots, "counts": dict(counts)}


def pure_quantum_main(*, workload_dir: Path, spec_file: str, source_rel: str) -> None:
    parser = argparse.ArgumentParser(description="Qonductor pure quantum workflow entry")
    parser.add_argument("--mode", choices=("direct", "k8s"), default="direct")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--emit-manifest", action="store_true")
    parser.add_argument("--shots", type=int, default=1024)
    parser.add_argument("--priority", choices=("balanced", "fidelity", "jct"), default="balanced")
    parser.add_argument("--registry-root", default="data/workflow_registry")
    parser.add_argument("--workflow-name", default="")
    args = parser.parse_args()

    spec, logical_id, qasm_path, qasm_text = load_spec_and_qasm(workload_dir, spec_file)
    qubits = int(spec.get("qnode", 0) or re.search(r"_(\d+)$", logical_id).group(1))
    workflow_name = args.workflow_name or k8s_name(f"{workload_dir.name}-{logical_id}")
    source = {"qslurmSource": source_rel, "workloadDir": str(workload_dir), "spec": str(workload_dir / spec_file)}

    if not args.submit and not args.emit_manifest and args.mode == "direct":
        print(json.dumps(run_pure_direct(workload_dir, spec_file, args.shots), indent=2, sort_keys=True))
        return

    image = build_pure_quantum_image(
        workflow_name=workflow_name,
        logical_id=logical_id,
        qasm_text=qasm_text,
        circuit_format=qasm_format(qasm_path, qasm_text),
        qubits=qubits,
        shots=args.shots,
        source=source,
    )
    register_image(image, args.registry_root)
    inputs = pure_quantum_inputs(
        logical_id=logical_id,
        qasm_path=qasm_path,
        qasm_text=qasm_text,
        circuit_format=qasm_format(qasm_path, qasm_text),
        qubits=qubits,
        shots=args.shots,
        priority=args.priority,
        source=source,
    )
    manifest = build_manifest(
        image,
        inputs,
        workflow_name=workflow_name,
        priority=args.priority,
        algorithm=workload_dir.name,
    )
    manifest_path = workload_dir / f"{Path(spec_file).stem}-workflow.yaml"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    print(f"workflowImageRef={image.image_id}")
    print(f"manifest={manifest_path}")
    if args.submit:
        kubectl = PROJECT_ROOT / "kubectl"
        cmd = [str(kubectl if kubectl.exists() else "kubectl"), "apply", "-f", str(manifest_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        print(result.stdout.strip())
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())


def hybrid_main(
    *,
    workload_dir: Path,
    spec_file: str,
    parameters_file: str,
    source_rel: str,
) -> None:
    parser = argparse.ArgumentParser(description="Qonductor dynamic hybrid workflow entry")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--emit-manifest", action="store_true")
    parser.add_argument("--shots", type=int, default=1024)
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--priority", choices=("balanced", "fidelity", "jct"), default="balanced")
    parser.add_argument("--registry-root", default="data/workflow_registry")
    parser.add_argument("--workflow-name", default="")
    args = parser.parse_args()
    shots = 16 if args.smoke else args.shots
    max_iterations = 1 if args.smoke else args.max_iterations

    inputs, benchmark, driver_kind = hybrid_inputs(
        workload_dir=workload_dir,
        spec_file=spec_file,
        parameters_file=parameters_file,
        shots=shots,
        max_iterations=max_iterations,
        priority=args.priority,
    )
    workflow_name = args.workflow_name or k8s_name(f"{workload_dir.name}-{Path(spec_file).stem}-dynamic")
    image = build_hybrid_image(
        workflow_name=workflow_name,
        benchmark=benchmark,
        driver_kind=driver_kind,
        source={"qslurmSource": source_rel, "workloadDir": str(workload_dir)},
    )
    register_image(image, args.registry_root)
    manifest = build_manifest(
        image,
        inputs,
        workflow_name=workflow_name,
        priority=args.priority,
        algorithm=benchmark,
    )
    manifest_path = workload_dir / f"{Path(spec_file).stem}-workflow.yaml"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    print(f"workflowImageRef={image.image_id}")
    print(f"manifest={manifest_path}")
    print(f"benchmark={benchmark}, driver={driver_kind}, iterations={max_iterations}, shots={shots}")
    if args.submit:
        kubectl = PROJECT_ROOT / "kubectl"
        cmd = [str(kubectl if kubectl.exists() else "kubectl"), "apply", "-f", str(manifest_path)]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        print(result.stdout.strip())
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
