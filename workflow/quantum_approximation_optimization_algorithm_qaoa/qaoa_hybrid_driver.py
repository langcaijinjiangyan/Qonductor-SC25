#!/usr/bin/env python3
"""
QAOA (p=2) MaxCut 12-Qubit — Qonductor Programmatic Submission.

Converted from the legacy SLURM + MPI C implementation:
    workload_transform_needed/quantum_approximation_optimization_algorithm_qaoa/
    └── quantum_approximation_optimization_algorithm_qaoa_12_hybrid.c

This Python driver preserves the exact same SPSA optimization logic
(identical gain sequences, RNG seed, parameter initialization, and
MaxCut objective function) while replacing MPI-based quantum execution
with Qonductor's dynamic QuantumJob submission.

Usage:
    # Direct local simulation (uses Qiskit Aer, no Qonductor infrastructure)
    python qaoa_hybrid_driver.py --mode direct

    # Smoke test (1 iteration, 16 shots)
    python qaoa_hybrid_driver.py --mode direct --smoke

    # Qonductor K8s mode (requires running Qonductor operator)
    python qaoa_hybrid_driver.py --mode k8s

    # Qonductor local simulation mode
    python qaoa_hybrid_driver.py --mode local
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Constants — identical to the C code
# ---------------------------------------------------------------------------

BIT_COUNT: int = 12
SHOTS: int = 1024
MAX_ITERATIONS: int = 200
PI: float = 3.14159265358979323846

# Parameter names — identical to C code's PARAMETER_NAMES
PARAMETER_NAMES: list[str] = ["_b_0_", "_b_1_", "_g_0_", "_g_1_"]
PARAMETER_COUNT: int = len(PARAMETER_NAMES)

# Logical circuit ID — identical to C code's LOGICAL_ID
LOGICAL_CIRCUIT_ID: str = "qaoa_indep_qiskit_12"

# Graph edges for MaxCut — identical to C code's EDGES[][] array (36 edges)
EDGES: list[tuple[int, int]] = [
    (0, 1), (0, 4), (0, 5), (0, 6), (0, 8), (0, 9),
    (1, 3), (1, 6), (1, 7), (1, 9),
    (2, 3), (2, 4), (2, 5), (2, 7), (2, 8), (2, 10), (2, 11),
    (3, 4), (3, 7), (3, 8), (3, 11),
    (4, 6), (4, 7), (4, 10), (4, 11),
    (5, 7), (5, 10),
    (6, 7), (6, 8), (6, 9), (6, 11),
    (7, 9), (7, 10),
    (8, 9), (8, 10), (8, 11),
]

# SPSA gain sequences — identical to C code
SPSA_A_BASE: float = 0.18
SPSA_A_EXP: float = 0.602
SPSA_C_BASE: float = 0.12
SPSA_C_EXP: float = 0.101

# RNG seed — identical to C code's srand(12345)
RNG_SEED: int = 12345


def _load_qasm() -> str:
    """Load the QAOA OpenQASM 3 circuit from the workload directory."""
    qasm_path = Path(__file__).resolve().parent / "qaoa_indep_qiskit_12.qasm3"
    if qasm_path.exists():
        return qasm_path.read_text(encoding="utf-8")
    raise FileNotFoundError(f"QASM file not found: {qasm_path}")


# ---------------------------------------------------------------------------
# Utility functions — 1:1 ports of C code logic
# ---------------------------------------------------------------------------

def initialize_parameters(count: int = PARAMETER_COUNT) -> list[float]:
    """Initialize parameters identically to C code: theta[i] = 0.05 * ((i % 11) + 1)."""
    return [0.05 * float((i % 11) + 1) for i in range(count)]


def wrap_angles(theta: list[float]) -> list[float]:
    """Wrap angles to [-pi, pi] — identical to C code's wrap_angles()."""
    wrapped: list[float] = []
    for value in theta:
        while value > PI:
            value -= 2.0 * PI
        while value < -PI:
            value += 2.0 * PI
        wrapped.append(value)
    return wrapped


def bitstring_cut_value(bitstring: str) -> float:
    """
    Compute MaxCut value for a measured bitstring.

    Identical to C code's bitstring_cut_value():
      - Uses little-endian indexing: bits[BIT_COUNT - 1 - a]
      - Returns number of cut edges (edges where endpoint bits differ)
    """
    bits = "".join(ch for ch in bitstring if ch in "01")
    if len(bits) != BIT_COUNT:
        raise ValueError(f"bitstring {bitstring!r} does not have {BIT_COUNT} bits")

    cut: float = 0.0
    for a, b in EDGES:
        if bits[BIT_COUNT - 1 - a] != bits[BIT_COUNT - 1 - b]:
            cut += 1.0
    return cut


def extract_counts(result_payload: Any) -> dict[str, int]:
    """
    Extract measurement counts from a quantum execution result.

    Handles multiple result formats:
      - {"counts": {"000...": N, ...}}        (Qonductor QuantumJob)
      - {"results": [{"counts": {...}}]}       (Qiskit Sampler)
      - JSON string of the above
    """
    if isinstance(result_payload, str):
        result_payload = json.loads(result_payload)
    if not isinstance(result_payload, dict):
        raise ValueError("quantum result payload must be a dict or JSON object")

    if "counts" in result_payload:
        return {str(k): int(v) for k, v in result_payload["counts"].items()}

    results = result_payload.get("results")
    if isinstance(results, list) and results:
        counts = results[0].get("counts", {})
        return {str(k): int(v) for k, v in counts.items()}

    raise ValueError("quantum result payload does not contain counts")


def counts_average(result_payload: Any) -> float:
    """
    Compute the average MaxCut value from measurement counts.

    Identical to C code's counts_average():
      weighted_sum = sum(cut_value(bitstring) * count for each bitstring)
      return weighted_sum / total_counts
    Returns inf if no valid counts found.
    """
    counts = extract_counts(result_payload)
    total = sum(counts.values())
    if total <= 0:
        return math.inf
    weighted_sum = sum(
        bitstring_cut_value(bitstring) * count
        for bitstring, count in counts.items()
    )
    return weighted_sum / float(total)


def objective_from_average(average_cut: float) -> float:
    """
    Convert average cut to minimization objective.

    Identical to C code's objective_from_average():
      objective = -average_cut
    (SPSA minimizes, so minimizing -cut maximizes cut.)
    """
    return -average_cut


# ---------------------------------------------------------------------------
# Direct simulation mode — uses Qiskit Aer for local testing
# ---------------------------------------------------------------------------

def _run_direct_simulation(
    circuit_qasm: str,
    theta: list[float],
    shots: int,
) -> dict[str, Any]:
    """
    Execute the QAOA circuit directly using Qiskit AerSimulator.

    This bypasses Qonductor's K8s/CR infrastructure and directly simulates
    the quantum circuit.  Useful for local development and correctness testing.

    Handles OpenQASM 3 parameterized circuits via qiskit.qasm3.loads().
    """
    from qiskit.qasm3 import loads as qasm3_loads
    from qiskit_aer import AerSimulator

    # Bind parameters — qasm3_loads preserves input parameters as Qiskit Parameter objects
    param_dict = dict(zip(PARAMETER_NAMES, theta))
    qc = qasm3_loads(circuit_qasm)
    bound_qc = qc.assign_parameters(param_dict)

    # The QASM3 circuit already includes measurements; remove duplicates if any
    # by only measuring qubits that aren't already measured
    existing_cregs = {creg.name for creg in bound_qc.cregs}
    # Circuit already has 'meas' classical register and measure instructions from QASM3

    backend = AerSimulator(method="automatic")
    job = backend.run(bound_qc, shots=shots)
    result = job.result()

    # Convert to same format as Qonductor QuantumJob result
    counts_list = result.get_counts()
    # Aer returns counts keyed by measured clbits; convert to unified format
    unified_counts: dict[str, int] = {}
    for bitstring, count in counts_list.items():
        # Ensure consistent length (12 bits) — Aer may return little-endian bitstrings
        # Remove spaces that Aer sometimes inserts
        clean = bitstring.replace(" ", "")
        padded = clean.zfill(BIT_COUNT)
        # Reverse if needed: Qiskit uses little-endian by default (q[N-1]...q[0]),
        # which matches the C code's bit ordering convention
        unified_counts[padded] = unified_counts.get(padded, 0) + int(count)

    return {"counts": unified_counts}


# ---------------------------------------------------------------------------
# Qonductor K8s/local submission mode
# ---------------------------------------------------------------------------

def _submit_quantum_job_qonductor(
    client: Any,
    circuit_qasm: str,
    theta: list[float],
    shots: int,
    priority: str,
    iteration: int,
    eval_label: str,
    workflow_ref: str = "",
    step_id: str = "qaoa_spsa_driver",
    namespace: str = "default",
) -> dict[str, Any]:
    """
    Submit a single quantum evaluation via Qonductor's QuantumJob CR.

    Creates a QuantumJob custom resource and waits for completion.
    This replaces the C code's MPI rank 1 q_exec_named_params_with_shots() call.
    """
    # Import here to avoid circular imports and allow --mode direct without k8s deps
    from src.operator.k8s_client import K8sClient, QUANTUM_JOB_PLURAL

    param_bindings = dict(zip(PARAMETER_NAMES, theta))

    # Generate RFC 1123 compliant name
    raw = f"qj-{step_id}-{iteration}-{eval_label}-{uuid.uuid4().hex[:5]}"
    import re
    name = re.sub(r"[^a-z0-9.-]+", "-", raw.lower()).strip("-.")[:63]

    body: dict[str, Any] = {
        "apiVersion": "qonductor.io/v1",
        "kind": "QuantumJob",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app": "qonductor",
                "workflow": workflow_ref,
                "step_id": step_id,
                "iteration": str(iteration),
                "eval_label": eval_label,
            },
        },
        "spec": {
            "workflowRef": workflow_ref,
            "stepId": f"{step_id}-{iteration}-{eval_label}",
            "label": f"{step_id}_{iteration}_{eval_label}",
            "qubits": BIT_COUNT,
            "shots": shots,
            "priority": priority,
            "logicalCircuitId": LOGICAL_CIRCUIT_ID,
            "circuitNames": [LOGICAL_CIRCUIT_ID],
            "circuitQasm": circuit_qasm,
            "circuitFormat": "qasm3",
            "parameterBindings": param_bindings,
            "iteration": iteration,
            "evalLabel": eval_label,
        },
        "status": {"phase": "Pending"},
    }

    qj = client.create_cr(QUANTUM_JOB_PLURAL, namespace=namespace, body=body)
    qj_name = qj["metadata"]["name"]

    # Wait for completion (with timeout)
    deadline = time.monotonic() + 900.0  # 15 min timeout
    while time.monotonic() < deadline:
        qj = client.get_cr(QUANTUM_JOB_PLURAL, qj_name)
        if not qj:
            raise RuntimeError(f"QuantumJob {qj_name!r} disappeared")
        status = qj.get("status", {})
        phase = status.get("phase", "")
        if phase == "Completed":
            result = status.get("result")
            if result is None:
                raise RuntimeError(f"QuantumJob {qj_name!r} completed without result")
            return result
        if phase == "Failed":
            raise RuntimeError(f"QuantumJob {qj_name!r} failed: {status}")
        time.sleep(2.0)

    raise TimeoutError(f"Timed out waiting for QuantumJob {qj_name!r}")


# ---------------------------------------------------------------------------
# SPSA optimization driver — identical logic to C code's classical_driver()
# ---------------------------------------------------------------------------

def run_qaoa_spsa(
    *,
    mode: str = "direct",
    circuit_qasm: str | None = None,
    shots: int = SHOTS,
    max_iterations: int = MAX_ITERATIONS,
    priority: str = "balanced",
    seed: int = RNG_SEED,
    client: Any = None,
    workflow_ref: str = "",
    verbose: bool = True,
) -> dict[str, Any]:
    """
    Run SPSA optimization for QAOA MaxCut.

    This is a direct port of the C code's classical_driver() function.
    The only difference is how quantum evaluations are performed:
      C:      MPI send → rank 1 → q_exec_named_params_with_shots()
      Python: mode="direct" → Qiskit AerSimulator
              mode="k8s"    → Qonductor QuantumJob CR
              mode="local"  → Qonductor QuantumJob CR (local simulation store)

    Args:
        mode: "direct", "k8s", or "local"
        circuit_qasm: QAOA QASM3 circuit string (loaded from file if None)
        shots: Number of measurement shots per evaluation
        max_iterations: Maximum SPSA iterations
        priority: Scheduling priority ("balanced", "fidelity", "jct")
        seed: RNG seed (12345 matches C code)
        client: Pre-configured K8sClient (for k8s/local modes)
        workflow_ref: Workflow reference name (for k8s/local modes)
        verbose: Print iteration progress

    Returns:
        Dict with keys: best_objective, best_theta, iterations, history
    """
    if circuit_qasm is None:
        circuit_qasm = _load_qasm()

    # --- Parameter initialization (identical to C code) ---
    theta = initialize_parameters(PARAMETER_COUNT)
    best_theta = list(theta)
    best_objective = math.inf
    rng = random.Random(seed)
    history: list[dict[str, Any]] = []

    # --- SPSA main loop (identical to C code) ---
    for iteration in range(max_iterations):
        # SPSA gain sequences (identical to C code)
        a = SPSA_A_BASE / math.pow(float(iteration + 1), SPSA_A_EXP)
        c = SPSA_C_BASE / math.pow(float(iteration + 1), SPSA_C_EXP)

        # Random perturbation vector (identical to C code)
        delta = [-1 if rng.random() < 0.5 else 1 for _ in range(PARAMETER_COUNT)]

        # Perturbed parameter sets (identical to C code)
        theta_plus = [theta[i] + c * float(delta[i]) for i in range(PARAMETER_COUNT)]
        theta_minus = [theta[i] - c * float(delta[i]) for i in range(PARAMETER_COUNT)]

        # --- Quantum evaluations (replaces C code's evaluate_once / MPI calls) ---
        if mode == "direct":
            plus_result = _run_direct_simulation(circuit_qasm, theta_plus, shots)
            minus_result = _run_direct_simulation(circuit_qasm, theta_minus, shots)
        else:
            # k8s or local mode: submit QuantumJob CRs
            if client is None:
                raise ValueError("K8sClient required for k8s/local mode")
            plus_result = _submit_quantum_job_qonductor(
                client, circuit_qasm, theta_plus, shots, priority,
                iteration, "plus", workflow_ref=workflow_ref,
            )
            minus_result = _submit_quantum_job_qonductor(
                client, circuit_qasm, theta_minus, shots, priority,
                iteration, "minus", workflow_ref=workflow_ref,
            )

        # --- Objective computation (identical to C code) ---
        avg_cut_plus = counts_average(plus_result)
        avg_cut_minus = counts_average(minus_result)
        objective_plus = objective_from_average(avg_cut_plus)
        objective_minus = objective_from_average(avg_cut_minus)

        if not math.isfinite(objective_plus) or not math.isfinite(objective_minus):
            print(f"[CLASSICAL] invalid objective at iteration {iteration}", file=sys.stderr)
            break

        center_objective = 0.5 * (objective_plus + objective_minus)

        # Track best (identical to C code)
        if center_objective < best_objective:
            best_objective = center_objective
            best_theta = list(theta)

        # SPSA gradient update (identical to C code)
        for i in range(PARAMETER_COUNT):
            grad = (objective_plus - objective_minus) / (2.0 * c * float(delta[i]))
            theta[i] -= a * grad
        theta = wrap_angles(theta)

        # Log (matching C code's printf)
        entry = {
            "iteration": iteration,
            "objective": center_objective,
            "objective_plus": objective_plus,
            "objective_minus": objective_minus,
            "theta": list(theta),
        }
        history.append(entry)
        if verbose:
            print(f"[Iter {iteration}] objective={center_objective:.8f} "
                  f"plus={objective_plus:.8f} minus={objective_minus:.8f}")

    # --- Final results ---
    if verbose:
        print(f"Best objective: {best_objective:.8f}")

    return {
        "best_objective": best_objective,
        "best_theta": best_theta,
        "iterations": max_iterations,
        "history": history,
    }


# ---------------------------------------------------------------------------
# Qonductor programmatic submission entry point
# ---------------------------------------------------------------------------

def submit_to_qonductor(
    *,
    mode: str = "k8s",
    shots: int = SHOTS,
    max_iterations: int = MAX_ITERATIONS,
    priority: str = "balanced",
    name: str = "qaoa_12_dynamic",
    workflow_name: str = "",
    registry_root: str = "data/workflow_registry",
) -> dict[str, Any]:
    """
    Submit the QAOA workflow to Qonductor using the programmatic API.

    This demonstrates the complete programmatic submission flow:
      1. Build a WorkflowImage with a dynamic SPSA driver node
      2. Register and deploy it to the workflow registry
      3. Generate a HybridWorkflow manifest (Python dict → YAML)
      4. Apply to the K8s cluster via kubectl
      5. Poll for completion and results

    This is the equivalent of the YAML-based declarative submission
    (workflow/quantum-approximation-optimization-algorithm-qaoa-12-dynamic.yaml)
    but done entirely through Python SDK functions.
    """
    import subprocess

    project_root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(project_root))

    from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
    from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
    from src.workflow.workflow_registry import WorkflowRegistry
    from src.workflow.api import deploy
    from src.workflow.qaoa_conversion import build_hybrid_workflow_manifest

    circuit_qasm = _load_qasm()

    # ── Step 1: Build and register the workflow image ──
    print("=" * 60)
    print("[1/4] Building and registering WorkflowImage ...")

    driver_code = (
        "from src.workflow.qaoa_runtime import run_qaoa_spsa_driver\n"
        "run_qaoa_spsa_driver()\n"
    )

    dag = HybridDAG(name=name)
    node = DAGNode(
        step_id="qaoa_spsa_driver",
        step_type=StepType.CLASSICAL,
        label="qaoa_spsa_driver",
        code=driver_code,
        resource_requirements={"cpu": 1, "memory": "1Gi"},
        metadata={"dynamic_quantum_jobs": True},
    )
    dag.add_node(node)

    config: dict[str, Any] = {
        "spec": {
            "priority": priority,
            "containers": [
                {
                    "name": "qaoa-spsa-driver",
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
    }

    image = WorkflowImage(
        name=name,
        dag=dag,
        quantum_code="",
        classical_code=driver_code,
        config=config,
        metadata={
            "source_type": "legacy_slurm_workload",
            "workload_dir": str(Path(__file__).resolve().parent),
            "dynamic_quantum_jobs": True,
        },
        status=WorkflowStatus.CREATED,
    )

    workflow_id = deploy(image)
    print(f"       image_id = {workflow_id}")

    # ── Step 2: Build workflow inputs ──
    print("[2/4] Building workflow inputs ...")
    workload_dir_str = str(Path(__file__).resolve().parent)
    edges_list = [list(e) for e in EDGES]
    workflow_inputs: dict[str, Any] = {
        "qaoa": {
            "logicalCircuitId": LOGICAL_CIRCUIT_ID,
            "circuitQasm": circuit_qasm,
            "circuitFormat": "qasm3",
            "parameterNames": PARAMETER_NAMES,
            "parameterCount": PARAMETER_COUNT,
            "qubits": BIT_COUNT,
            "clbits": BIT_COUNT,
            "shots": shots,
            "maxIterations": max_iterations,
            "priority": priority,
            "edges": edges_list,
            "seed": RNG_SEED,
            "source": {
                "workloadDir": workload_dir_str,
                "spec": f"{workload_dir_str}/spec_12.json",
                "parameters": f"{workload_dir_str}/parameters_12.json",
            },
        }
    }
    print(f"       maxIterations={max_iterations}, shots={shots}, edges={len(edges_list)}")

    # ── Step 3: Build manifest and apply to cluster ──
    wf_name = workflow_name or f"{name}-smoke20"
    print(f"[3/4] Building manifest and applying to cluster (name={wf_name}) ...")

    manifest = build_hybrid_workflow_manifest(
        image, workflow_inputs, name=wf_name, priority=priority,
    )
    manifest["metadata"]["labels"]["algorithm"] = "qaoa"

    import yaml  # noqa: E402
    yaml_text = yaml.safe_dump(manifest, sort_keys=False)
    yaml_path = project_root / f"workflow/{wf_name}.yaml"
    yaml_path.parent.mkdir(parents=True, exist_ok=True)
    yaml_path.write_text(yaml_text, encoding="utf-8")

    kubectl = str(project_root / "kubectl")
    result = subprocess.run(
        [kubectl, "apply", "-f", str(yaml_path)],
        capture_output=True, text=True, timeout=30,
    )
    print(f"       kubectl: {result.stdout.strip()}")
    if result.returncode != 0:
        raise RuntimeError(f"kubectl apply failed: {result.stderr}")

    # ── Step 4: Poll for completion ──
    print("[4/4] Polling cluster for workflow completion ...")
    deadline = time.monotonic() + 900.0
    while time.monotonic() < deadline:
        elapsed = int(time.monotonic() - (deadline - 900.0))
        hw = subprocess.run(
            [kubectl, "get", "hybridworkflow", wf_name, "-o", "json"],
            capture_output=True, text=True, timeout=10,
        )
        if hw.returncode != 0:
            time.sleep(2)
            continue

        hw_obj = json.loads(hw.stdout)
        status = hw_obj.get("status", {})
        phase = status.get("phase", "")

        # Check QuantumJob progress
        qj = subprocess.run(
            [kubectl, "get", "quantumjobs", "-l", f"workflow={wf_name}",
             "--no-headers"],
            capture_output=True, text=True, timeout=10,
        )
        qj_lines = [l for l in qj.stdout.strip().split("\n") if l.strip()]
        qj_completed = sum(1 for l in qj_lines if "Completed" in l)

        print(f"       [{elapsed}s] phase={phase}, QuantumJobs: {qj_completed}/{len(qj_lines)} completed")

        if phase == "Completed":
            print(f"\n  ✅  Workflow completed successfully!")
            driver_pod = subprocess.run(
                [kubectl, "get", "pods", "-l", f"app=qonductor,workflow={wf_name}",
                 "--no-headers", "-o", "custom-columns=:metadata.name"],
                capture_output=True, text=True, timeout=10,
            )
            pod_name = driver_pod.stdout.strip().split("\n")[0] if driver_pod.stdout.strip() else ""
            if pod_name:
                logs = subprocess.run(
                    [kubectl, "logs", pod_name, "--tail=30"],
                    capture_output=True, text=True, timeout=10,
                )
                print(f"  Driver logs (last 30 lines):\n{logs.stdout[:3000]}")
            return {"image_id": workflow_id, "workflow_name": wf_name, "phase": phase}

        if phase == "Failed":
            print(f"\n  ❌  Workflow failed: {status}")
            return {"image_id": workflow_id, "workflow_name": wf_name, "phase": phase, "error": status}

        time.sleep(5)

    raise TimeoutError(f"Timed out waiting for workflow {wf_name}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="QAOA (p=2) MaxCut — Qonductor Programmatic Submission",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode direct --smoke     # Quick local test (1 iter, 16 shots)
  %(prog)s --mode direct             # Full local simulation (200 iter, 1024 shots)
  %(prog)s --mode direct --shots 4096 --max-iterations 100
  %(prog)s --submit                 # Programmatic submission to Qonductor
  %(prog)s --submit --mode k8s      # Submit to real K8s cluster
        """,
    )
    parser.add_argument(
        "--mode", choices=("direct", "local", "k8s"), default="direct",
        help="Execution mode: direct (Qiskit Aer), local (Qonductor sim), k8s (cluster)",
    )
    parser.add_argument(
        "--submit", action="store_true",
        help="Submit to Qonductor via programmatic API (deploy + invoke + results)",
    )
    parser.add_argument(
        "--smoke", action="store_true",
        help="Smoke test: reduce to 1 iteration and 16 shots",
    )
    parser.add_argument(
        "--shots", type=int, default=SHOTS,
        help=f"Measurement shots per evaluation (default: {SHOTS})",
    )
    parser.add_argument(
        "--max-iterations", type=int, default=MAX_ITERATIONS,
        help=f"Maximum SPSA iterations (default: {MAX_ITERATIONS})",
    )
    parser.add_argument(
        "--priority", choices=("balanced", "fidelity", "jct"), default="balanced",
        help="Scheduling priority (default: balanced)",
    )
    parser.add_argument(
        "--seed", type=int, default=RNG_SEED,
        help=f"RNG seed for reproducibility (default: {RNG_SEED})",
    )
    parser.add_argument(
        "--registry-root", default="data/workflow_registry",
        help="Workflow registry root directory",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress per-iteration output",
    )
    parser.add_argument(
        "--workflow-name", default="",
        help="HybridWorkflow CR name for cluster submission (default: auto-generated)",
    )

    args = parser.parse_args(argv)

    shots = 16 if args.smoke else args.shots
    max_iterations = 1 if args.smoke else args.max_iterations

    if args.smoke:
        print(f"=== SMOKE TEST: {max_iterations} iteration(s), {shots} shots ===")

    if args.submit:
        # Programmatic submission flow: build → register → manifest → kubectl apply → poll
        wf_name = args.workflow_name or f"qaoa-12-iter{max_iterations}"
        submit_to_qonductor(
            mode=args.mode if args.mode != "direct" else "k8s",
            shots=shots,
            max_iterations=max_iterations,
            priority=args.priority,
            name=f"qaoa_12_iter{max_iterations}",
            workflow_name=wf_name,
            registry_root=args.registry_root,
        )
    else:
        # Direct execution (Qiskit Aer simulation)
        if args.mode != "direct":
            print(
                "Warning: --mode k8s/local without --submit. "
                "Use --submit for full deploy→invoke flow, "
                "or --mode direct for local simulation.",
                file=sys.stderr,
            )

        result = run_qaoa_spsa(
            mode="direct",
            shots=shots,
            max_iterations=max_iterations,
            priority=args.priority,
            seed=args.seed,
            verbose=not args.quiet,
        )

        print(f"\nFinal: best_objective={result['best_objective']:.8f}")
        print(f"Best theta: {[f'{v:.6f}' for v in result['best_theta']]}")


if __name__ == "__main__":
    main()
