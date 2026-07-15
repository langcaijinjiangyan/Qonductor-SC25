#!/usr/bin/env python3
"""
VQE with Real Amplitudes Ansatz (48 params) — Qonductor Programmatic Submission.

Converted from the legacy SLURM + MPI C implementation:
    workload_transform_needed/real_amplitudes_ansatz_with_random_parameters/
    └── real_amplitudes_ansatz_with_random_parameters_12_hybrid.c

Usage:
    python vqe_hybrid_driver.py --mode direct --smoke
    python vqe_hybrid_driver.py --mode direct
    python vqe_hybrid_driver.py --submit --max-iterations 20
"""

from __future__ import annotations

import argparse
import json
import math
import random
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

BIT_COUNT: int = 12
SHOTS: int = 1024
MAX_ITERATIONS: int = 200
PI: float = 3.14159265358979323846

PARAMETER_NAMES: list[str] = [f"_θ_{i}_" for i in range(48)]
PARAMETER_COUNT: int = 48

LOGICAL_CIRCUIT_ID: str = "realamprandom_indep_qiskit_12"
QASM_FILENAME: str = "realamprandom_indep_qiskit_12.qasm3"
BENCHMARK: str = "vqe_real_amp"
ALGORITHM_TAG: str = "real_amplitudes"
CONTAINER_NAME: str = "vqe-spsa-driver"

H_FIELD: list[float] = [0.7, -0.45, 0.32, -0.58, 0.91, -0.27, 0.63, -0.74, 0.18, 0.52, -0.36, 0.81]
H_COUPLING: list[float] = [-1.05, 0.86, -0.67, 0.49, -0.92, 0.73, -0.54, 1.11, -0.38, 0.64, -0.79, 0.57]

SPSA_A_BASE: float = 0.18
SPSA_A_EXP: float = 0.602
SPSA_C_BASE: float = 0.12
SPSA_C_EXP: float = 0.101
RNG_SEED: int = 12345

_DRIVER_CODE = "from src.workflow.vqe_runtime import run_vqe_spsa_driver\nrun_vqe_spsa_driver()\n"


def _load_qasm() -> str:
    qasm_path = Path(__file__).resolve().parent / QASM_FILENAME
    if qasm_path.exists():
        return qasm_path.read_text(encoding="utf-8")
    raise FileNotFoundError(f"QASM file not found: {qasm_path}")


def initialize_parameters(count: int = PARAMETER_COUNT) -> list[float]:
    return [0.05 * float((i % 11) + 1) for i in range(count)]


def wrap_angles(theta: list[float]) -> list[float]:
    wrapped: list[float] = []
    for value in theta:
        while value > PI: value -= 2.0 * PI
        while value < -PI: value += 2.0 * PI
        wrapped.append(value)
    return wrapped


def bitstring_ising_energy(bitstring: str) -> float:
    bits = "".join(ch for ch in bitstring if ch in "01")
    if len(bits) != BIT_COUNT:
        raise ValueError(f"bitstring {bitstring!r} does not have {BIT_COUNT} bits")
    z_values = [-1 if bits[BIT_COUNT - 1 - i] == "1" else 1 for i in range(BIT_COUNT)]
    energy = sum(H_FIELD[i] * float(z_values[i]) for i in range(BIT_COUNT))
    energy += sum(H_COUPLING[i] * float(z_values[i] * z_values[(i + 1) % BIT_COUNT]) for i in range(BIT_COUNT))
    return energy


def extract_counts(result_payload: Any) -> dict[str, int]:
    if isinstance(result_payload, str):
        result_payload = json.loads(result_payload)
    if not isinstance(result_payload, dict):
        raise ValueError("quantum result payload must be a dict or JSON object")
    if "counts" in result_payload:
        return {str(k): int(v) for k, v in result_payload["counts"].items()}
    results = result_payload.get("results")
    if isinstance(results, list) and results:
        return {str(k): int(v) for k, v in results[0].get("counts", {}).items()}
    raise ValueError("quantum result payload does not contain counts")


def counts_energy_average(result_payload: Any) -> float:
    counts = extract_counts(result_payload)
    total = sum(counts.values())
    if total <= 0:
        return math.inf
    return sum(bitstring_ising_energy(bs) * cnt for bs, cnt in counts.items()) / float(total)


def _run_direct_simulation(circuit_qasm: str, theta: list[float], shots: int) -> dict[str, Any]:
    from qiskit.qasm3 import loads as qasm3_loads
    from qiskit_aer import AerSimulator

    param_dict = dict(zip(PARAMETER_NAMES, theta))
    qc = qasm3_loads(circuit_qasm)
    bound_qc = qc.assign_parameters(param_dict)
    backend = AerSimulator(method="automatic")
    job = backend.run(bound_qc, shots=shots)
    result = job.result()
    unified_counts: dict[str, int] = {}
    for bitstring, count in result.get_counts().items():
        clean = bitstring.replace(" ", "")
        padded = clean.zfill(BIT_COUNT)
        unified_counts[padded] = unified_counts.get(padded, 0) + int(count)
    return {"counts": unified_counts}


def run_vqe_spsa(
    *, mode: str = "direct", circuit_qasm: str | None = None,
    shots: int = SHOTS, max_iterations: int = MAX_ITERATIONS,
    priority: str = "balanced", seed: int = RNG_SEED, verbose: bool = True,
) -> dict[str, Any]:
    if circuit_qasm is None:
        circuit_qasm = _load_qasm()
    theta = initialize_parameters(PARAMETER_COUNT)
    best_theta = list(theta)
    best_objective = math.inf
    rng = random.Random(seed)
    history: list[dict[str, Any]] = []

    for iteration in range(max_iterations):
        a = SPSA_A_BASE / math.pow(float(iteration + 1), SPSA_A_EXP)
        c = SPSA_C_BASE / math.pow(float(iteration + 1), SPSA_C_EXP)
        delta = [-1 if rng.random() < 0.5 else 1 for _ in range(PARAMETER_COUNT)]
        theta_plus = [theta[i] + c * float(delta[i]) for i in range(PARAMETER_COUNT)]
        theta_minus = [theta[i] - c * float(delta[i]) for i in range(PARAMETER_COUNT)]

        plus_result = _run_direct_simulation(circuit_qasm, theta_plus, shots)
        minus_result = _run_direct_simulation(circuit_qasm, theta_minus, shots)

        objective_plus = counts_energy_average(plus_result)
        objective_minus = counts_energy_average(minus_result)
        if not math.isfinite(objective_plus) or not math.isfinite(objective_minus):
            print(f"[CLASSICAL] invalid objective at iteration {iteration}", file=sys.stderr)
            break

        center_objective = 0.5 * (objective_plus + objective_minus)
        if center_objective < best_objective:
            best_objective = center_objective
            best_theta = list(theta)
        for i in range(PARAMETER_COUNT):
            grad = (objective_plus - objective_minus) / (2.0 * c * float(delta[i]))
            theta[i] -= a * grad
        theta = wrap_angles(theta)
        entry = {"iteration": iteration, "objective": center_objective,
                 "objective_plus": objective_plus, "objective_minus": objective_minus, "theta": list(theta)}
        history.append(entry)
        if verbose:
            print(f"[Iter {iteration}] energy={center_objective:.8f} plus={objective_plus:.8f} minus={objective_minus:.8f}")
    if verbose:
        print(f"Best energy: {best_objective:.8f}")
    return {"best_objective": best_objective, "best_theta": best_theta, "iterations": max_iterations, "history": history}


def submit_to_qonductor(
    *, mode: str = "k8s", shots: int = SHOTS, max_iterations: int = MAX_ITERATIONS,
    priority: str = "balanced", name: str = "", workflow_name: str = "",
    registry_root: str = "data/workflow_registry",
) -> dict[str, Any]:
    import yaml
    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))
    from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
    from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
    from src.workflow.api import deploy
    from src.workflow.qaoa_conversion import build_hybrid_workflow_manifest

    circuit_qasm = _load_qasm()
    img_name = name or f"{BENCHMARK}_12_dynamic"
    print("=" * 60)
    print("[1/4] Building and registering WorkflowImage ...")
    dag = HybridDAG(name=img_name)
    node = DAGNode(step_id="vqe_spsa_driver", step_type=StepType.CLASSICAL,
                   label="vqe_spsa_driver", code=_DRIVER_CODE,
                   resource_requirements={"cpu": 1, "memory": "2Gi"},
                   metadata={"dynamic_quantum_jobs": True})
    dag.add_node(node)
    config: dict[str, Any] = {
        "spec": {"priority": priority, "containers": [
            {"name": CONTAINER_NAME, "image": "qonductor-operator:latest",
             "resources": {"limits": {"cpu": "1", "memory": "2Gi"}}}],
            "scheduling": {"classicalPolicy": "FilterScore", "quantumPolicy": "NSGA2"},
            "errorMitigation": {"enabled": False, "stackedTechniques": []}}}
    image = WorkflowImage(name=img_name, dag=dag, quantum_code="", classical_code=_DRIVER_CODE,
                          config=config, metadata={"source_type": "legacy_slurm_workload",
                          "workload_dir": str(Path(__file__).resolve().parent),
                          "dynamic_quantum_jobs": True}, status=WorkflowStatus.CREATED)
    workflow_id = deploy(image)
    print(f"       image_id = {workflow_id}")
    print("[2/4] Building workflow inputs ...")
    wl_dir = str(Path(__file__).resolve().parent)
    workflow_inputs: dict[str, Any] = {
        BENCHMARK: {"logicalCircuitId": LOGICAL_CIRCUIT_ID, "circuitQasm": circuit_qasm,
                    "circuitFormat": "qasm3", "parameterNames": PARAMETER_NAMES,
                    "parameterCount": PARAMETER_COUNT, "qubits": BIT_COUNT, "clbits": BIT_COUNT,
                    "shots": shots, "maxIterations": max_iterations, "priority": priority,
                    "objective": "ising_energy",
                    "hamiltonian": {"hField": H_FIELD, "hCoupling": H_COUPLING, "topology": "ring"},
                    "seed": RNG_SEED,
                    "source": {"workloadDir": wl_dir, "spec": f"{wl_dir}/spec_12.json",
                               "parameters": f"{wl_dir}/parameters_12.json"}}}
    print(f"       maxIterations={max_iterations}, shots={shots}, params={PARAMETER_COUNT}")
    wf_name = workflow_name or f"{BENCHMARK.replace('_', '-')}-iter{max_iterations}"
    print(f"[3/4] Building manifest and applying to cluster (name={wf_name}) ...")
    manifest = build_hybrid_workflow_manifest(image, workflow_inputs, name=wf_name, priority=priority)
    manifest["metadata"]["labels"]["algorithm"] = BENCHMARK
    yaml_path = project_root / f"workflow/{wf_name}.yaml"
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    kubectl = str(project_root / "kubectl")
    result = subprocess.run([kubectl, "apply", "-f", str(yaml_path)], capture_output=True, text=True, timeout=30)
    print(f"       kubectl: {result.stdout.strip()}")
    if result.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {result.stderr}")
    print("[4/4] Polling cluster for workflow completion ...")
    deadline = time.monotonic() + 900.0
    while time.monotonic() < deadline:
        elapsed = int(time.monotonic() - (deadline - 900.0))
        hw = subprocess.run([kubectl, "get", "hybridworkflow", wf_name, "-o", "json"],
                            capture_output=True, text=True, timeout=10)
        if hw.returncode != 0:
            time.sleep(2)
            continue
        phase = json.loads(hw.stdout).get("status", {}).get("phase", "")
        qj = subprocess.run([kubectl, "get", "quantumjobs", "-l", f"workflow={wf_name}",
                             "--no-headers"], capture_output=True, text=True, timeout=10)
        qj_lines = [l for l in qj.stdout.strip().split("\n") if l.strip()]
        qj_ok = sum(1 for l in qj_lines if "Completed" in l)
        print(f"       [{elapsed}s] phase={phase}, QuantumJobs: {qj_ok}/{len(qj_lines)} completed")
        if phase == "Completed":
            print(f"\n  ✅  Workflow completed!"); return {"image_id": workflow_id, "workflow_name": wf_name, "phase": phase}
        if phase == "Failed":
            print(f"\n  ❌  Workflow failed"); return {"image_id": workflow_id, "workflow_name": wf_name, "phase": phase}
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for workflow {wf_name}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=f"VQE {ALGORITHM_TAG} ({PARAMETER_COUNT} params) — Qonductor Submission")
    parser.add_argument("--mode", choices=("direct", "local", "k8s"), default="direct")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--shots", type=int, default=SHOTS)
    parser.add_argument("--max-iterations", type=int, default=MAX_ITERATIONS)
    parser.add_argument("--priority", choices=("balanced", "fidelity", "jct"), default="balanced")
    parser.add_argument("--seed", type=int, default=RNG_SEED)
    parser.add_argument("--registry-root", default="data/workflow_registry")
    parser.add_argument("--workflow-name", default="")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    shots = 16 if args.smoke else args.shots
    max_iterations = 1 if args.smoke else args.max_iterations
    if args.smoke:
        print(f"=== SMOKE TEST: {max_iterations} iter, {shots} shots ===")
    if args.submit:
        wf_name = args.workflow_name or f"{BENCHMARK.replace('_', '-')}-iter{max_iterations}"
        submit_to_qonductor(mode="k8s", shots=shots, max_iterations=max_iterations,
                            priority=args.priority, name=f"{BENCHMARK}_iter{max_iterations}",
                            workflow_name=wf_name, registry_root=args.registry_root)
    else:
        result = run_vqe_spsa(mode="direct", shots=shots, max_iterations=max_iterations,
                              priority=args.priority, seed=args.seed, verbose=not args.quiet)
        print(f"\nFinal: best_energy={result['best_objective']:.8f}")


if __name__ == "__main__":
    main()
