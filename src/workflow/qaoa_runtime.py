"""Runtime helpers for dynamically driven QAOA workflows.

The original Slurm workload uses two MPI ranks: a classical SPSA driver and
a quantum worker that calls ``q_exec``.  In Qonductor, the classical driver is
a normal K8s Job and each quantum evaluation becomes a runtime ``QuantumJob``
child resource.  The parent DAG therefore stays compact while the iterative
algorithm remains dynamic.
"""

from __future__ import annotations

import json
import math
import os
import random
import re
import time
import uuid
from typing import Any

from src.operator.k8s_client import K8sClient, QUANTUM_JOB_PLURAL
from src.utils.logging_config import configure_logging

DEFAULT_QAOA_EDGES: tuple[tuple[int, int], ...] = (
    (0, 1), (0, 4), (0, 5), (0, 6), (0, 8), (0, 9),
    (1, 3), (1, 6), (1, 7), (1, 9),
    (2, 3), (2, 4), (2, 5), (2, 7), (2, 8), (2, 10), (2, 11),
    (3, 4), (3, 7), (3, 8), (3, 11),
    (4, 6), (4, 7), (4, 10), (4, 11),
    (5, 7), (5, 10),
    (6, 7), (6, 8), (6, 9), (6, 11),
    (7, 9), (7, 10),
    (8, 9), (8, 10), (8, 11),
)


def _rfc1123_name(*parts: str, max_length: int = 63) -> str:
    raw = "-".join(str(part) for part in parts if part)
    name = re.sub(r"[^a-z0-9.-]+", "-", raw.lower())
    name = re.sub(r"-+", "-", name).strip("-.")
    if not name:
        name = "qonductor"
    return name[:max_length].rstrip("-.") or "qonductor"


def initialize_parameters(count: int) -> list[float]:
    return [0.05 * float((i % 11) + 1) for i in range(count)]


def wrap_angles(theta: list[float]) -> list[float]:
    wrapped = []
    for value in theta:
        while value > math.pi:
            value -= 2.0 * math.pi
        while value < -math.pi:
            value += 2.0 * math.pi
        wrapped.append(value)
    return wrapped


def bitstring_cut_value(
    bitstring: str,
    edges: list[list[int]] | list[tuple[int, int]] | tuple[tuple[int, int], ...],
    bit_count: int | None = None,
) -> float:
    bits = "".join(ch for ch in bitstring if ch in "01")
    bit_count = bit_count or len(bits)
    if len(bits) != bit_count:
        raise ValueError(f"bitstring {bitstring!r} does not have {bit_count} bits")

    cut = 0.0
    for a, b in edges:
        # Preserve the same little-endian interpretation used by the C driver.
        if bits[bit_count - 1 - int(a)] != bits[bit_count - 1 - int(b)]:
            cut += 1.0
    return cut


def extract_counts(result_payload: Any) -> dict[str, int]:
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


def counts_average(
    result_payload: Any,
    edges: list[list[int]] | list[tuple[int, int]] | tuple[tuple[int, int], ...],
    bit_count: int,
) -> float:
    counts = extract_counts(result_payload)
    total = sum(counts.values())
    if total <= 0:
        return math.inf
    weighted_sum = sum(
        bitstring_cut_value(bitstring, edges, bit_count) * count
        for bitstring, count in counts.items()
    )
    return weighted_sum / float(total)


def submit_quantum_eval(
    client: K8sClient,
    *,
    workflow_ref: str,
    step_id: str,
    logical_circuit_id: str,
    circuit_qasm: str,
    parameter_bindings: dict[str, float],
    qubits: int,
    shots: int,
    priority: str,
    iteration: int,
    eval_label: str,
    namespace: str = "default",
) -> dict:
    name = _rfc1123_name("qj", step_id, str(iteration), eval_label, uuid.uuid4().hex[:5])
    body = {
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
            "qubits": qubits,
            "shots": shots,
            "priority": priority,
            "logicalCircuitId": logical_circuit_id,
            "circuitNames": [logical_circuit_id],
            "circuitQasm": circuit_qasm,
            "circuitFormat": "qasm3",
            "parameterBindings": parameter_bindings,
            "iteration": iteration,
            "evalLabel": eval_label,
        },
        "status": {"phase": "Pending"},
    }
    return client.create_cr(QUANTUM_JOB_PLURAL, namespace=namespace, body=body)


def wait_for_quantum_result(
    client: K8sClient,
    name: str,
    *,
    timeout_s: float = 900.0,
    poll_s: float = 2.0,
) -> dict:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        qj = client.get_cr(QUANTUM_JOB_PLURAL, name)
        if not qj:
            raise RuntimeError(f"QuantumJob {name!r} disappeared")
        status = qj.get("status", {})
        phase = status.get("phase", "")
        if phase == "Completed":
            result = status.get("result")
            if result is None:
                raise RuntimeError(f"QuantumJob {name!r} completed without result")
            return result
        if phase == "Failed":
            raise RuntimeError(f"QuantumJob {name!r} failed: {status}")
        time.sleep(poll_s)
    raise TimeoutError(f"Timed out waiting for QuantumJob {name!r}")


def _load_runtime_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    if config is not None:
        return config.get("qaoa", config)
    raw = os.environ.get("QONDUCTOR_WORKFLOW_INPUTS", "{}")
    loaded = json.loads(raw or "{}")
    return loaded.get("qaoa", loaded)


def run_qaoa_spsa_driver(
    config: dict[str, Any] | None = None,
    client: K8sClient | None = None,
) -> dict[str, Any]:
    configure_logging()

    cfg = _load_runtime_config(config)
    mode = os.environ.get("QONDUCTOR_MODE", "k8s")
    namespace = os.environ.get("QONDUCTOR_NAMESPACE", "default")
    workflow_ref = os.environ.get("QONDUCTOR_WORKFLOW_NAME", cfg.get("workflowRef", ""))
    step_id = os.environ.get("QONDUCTOR_STEP_ID", "qaoa_spsa_driver")
    client = client or K8sClient(mode=mode)

    logical_id = cfg.get("logicalCircuitId") or cfg.get("circuit_id")
    circuit_qasm = cfg.get("circuitQasm") or cfg.get("circuit_qasm", "")
    if not circuit_qasm and cfg.get("qasmPath"):
        with open(cfg["qasmPath"], encoding="utf-8") as fh:
            circuit_qasm = fh.read()
    if not logical_id or not circuit_qasm:
        raise ValueError("QAOA config requires logicalCircuitId and circuitQasm")

    parameter_names = cfg.get("parameterNames") or cfg.get("parameter_names") or []
    qubits = int(cfg.get("qubits") or cfg.get("num_qubits") or 12)
    shots = int(cfg.get("shots", 1024))
    max_iterations = int(os.environ.get("R1_ITERATIONS", cfg.get("maxIterations", 200)))
    priority = cfg.get("priority", "balanced")
    edges = cfg.get("edges") or [list(edge) for edge in DEFAULT_QAOA_EDGES]
    timeout_s = float(cfg.get("quantumTimeoutSeconds", 900.0))
    poll_s = float(cfg.get("pollSeconds", 2.0))

    if not parameter_names:
        parameter_names = [f"theta_{i}" for i in range(4)]

    theta = initialize_parameters(len(parameter_names))
    best_theta = list(theta)
    best_objective = math.inf
    rng = random.Random(int(cfg.get("seed", 12345)))
    history = []

    for iteration in range(max_iterations):
        a = 0.18 / math.pow(float(iteration + 1), 0.602)
        c = 0.12 / math.pow(float(iteration + 1), 0.101)
        delta = [-1 if rng.random() < 0.5 else 1 for _ in parameter_names]
        theta_plus = [theta[i] + c * delta[i] for i in range(len(theta))]
        theta_minus = [theta[i] - c * delta[i] for i in range(len(theta))]

        plus_bindings = dict(zip(parameter_names, theta_plus))
        minus_bindings = dict(zip(parameter_names, theta_minus))
        plus_qj = submit_quantum_eval(
            client,
            workflow_ref=workflow_ref,
            step_id=step_id,
            logical_circuit_id=logical_id,
            circuit_qasm=circuit_qasm,
            parameter_bindings=plus_bindings,
            qubits=qubits,
            shots=shots,
            priority=priority,
            iteration=iteration,
            eval_label="plus",
            namespace=namespace,
        )
        minus_qj = submit_quantum_eval(
            client,
            workflow_ref=workflow_ref,
            step_id=step_id,
            logical_circuit_id=logical_id,
            circuit_qasm=circuit_qasm,
            parameter_bindings=minus_bindings,
            qubits=qubits,
            shots=shots,
            priority=priority,
            iteration=iteration,
            eval_label="minus",
            namespace=namespace,
        )

        plus_result = wait_for_quantum_result(
            client, plus_qj["metadata"]["name"],
            timeout_s=timeout_s, poll_s=poll_s,
        )
        minus_result = wait_for_quantum_result(
            client, minus_qj["metadata"]["name"],
            timeout_s=timeout_s, poll_s=poll_s,
        )

        avg_plus = counts_average(plus_result, edges, qubits)
        avg_minus = counts_average(minus_result, edges, qubits)
        objective_plus = -avg_plus
        objective_minus = -avg_minus
        if not math.isfinite(objective_plus) or not math.isfinite(objective_minus):
            raise RuntimeError(f"invalid objective at iteration {iteration}")

        center_objective = 0.5 * (objective_plus + objective_minus)
        if center_objective < best_objective:
            best_objective = center_objective
            best_theta = list(theta)

        for i in range(len(theta)):
            grad = (objective_plus - objective_minus) / (2.0 * c * delta[i])
            theta[i] -= a * grad
        theta = wrap_angles(theta)

        entry = {
            "iteration": iteration,
            "objective": center_objective,
            "objective_plus": objective_plus,
            "objective_minus": objective_minus,
            "theta": list(theta),
        }
        history.append(entry)
        print(json.dumps({"event": "qaoa_iteration", **entry}))

    summary = {
        "best_objective": best_objective,
        "best_theta": best_theta,
        "iterations": max_iterations,
        "history": history,
    }
    print(json.dumps({"event": "qaoa_complete", **summary}))
    return summary
