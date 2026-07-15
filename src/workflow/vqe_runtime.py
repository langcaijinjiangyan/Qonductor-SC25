"""Runtime helpers for VQE-style ansatz workflows.

The legacy ansatz workloads use two MPI ranks: rank 0 runs an SPSA driver
and rank 1 calls ``q_exec`` for each plus/minus parameter evaluation.  This
module keeps the same optimization loop without MPI by submitting each
quantum evaluation as a Qonductor ``QuantumJob`` child resource.
"""

from __future__ import annotations

import json
import math
import os
import random
from typing import Any

from src.operator.k8s_client import K8sClient
from src.workflow.qaoa_runtime import (
    initialize_parameters,
    submit_quantum_eval,
    wait_for_quantum_result,
    wrap_angles,
    extract_counts,
)

DEFAULT_H_FIELD: tuple[float, ...] = (
    0.7, -0.45, 0.32, -0.58, 0.91, -0.27,
    0.63, -0.74, 0.18, 0.52, -0.36, 0.81,
)

DEFAULT_H_COUPLING: tuple[float, ...] = (
    -1.05, 0.86, -0.67, 0.49, -0.92, 0.73,
    -0.54, 1.11, -0.38, 0.64, -0.79, 0.57,
)


def _load_runtime_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    if config is not None:
        return config.get("ansatz", config)

    raw = os.environ.get("QONDUCTOR_WORKFLOW_INPUTS", "{}")
    loaded = json.loads(raw or "{}")
    if "ansatz" in loaded:
        return loaded["ansatz"]
    if "vqe" in loaded:
        return loaded["vqe"]

    for value in loaded.values():
        if isinstance(value, dict) and value.get("logicalCircuitId"):
            return value
    return loaded


def bitstring_ising_energy(
    bitstring: str,
    h_field: list[float] | tuple[float, ...],
    h_coupling: list[float] | tuple[float, ...],
    bit_count: int,
) -> float:
    bits = "".join(ch for ch in bitstring if ch in "01")
    if len(bits) != bit_count:
        raise ValueError(f"bitstring {bitstring!r} does not have {bit_count} bits")

    z_values = []
    for i in range(bit_count):
        bit = bits[bit_count - 1 - i]
        z_values.append(-1 if bit == "1" else 1)

    energy = 0.0
    for i in range(bit_count):
        energy += float(h_field[i]) * float(z_values[i])
    for i in range(bit_count):
        energy += float(h_coupling[i]) * float(z_values[i] * z_values[(i + 1) % bit_count])
    return energy


def counts_energy_average(
    result_payload: Any,
    h_field: list[float] | tuple[float, ...],
    h_coupling: list[float] | tuple[float, ...],
    bit_count: int,
) -> float:
    counts = extract_counts(result_payload)
    total = sum(counts.values())
    if total <= 0:
        return math.inf
    weighted_sum = sum(
        bitstring_ising_energy(bitstring, h_field, h_coupling, bit_count) * count
        for bitstring, count in counts.items()
    )
    return weighted_sum / float(total)


def run_vqe_spsa_driver(
    config: dict[str, Any] | None = None,
    client: K8sClient | None = None,
) -> dict[str, Any]:
    cfg = _load_runtime_config(config)
    mode = os.environ.get("QONDUCTOR_MODE", "k8s")
    namespace = os.environ.get("QONDUCTOR_NAMESPACE", "default")
    workflow_ref = os.environ.get("QONDUCTOR_WORKFLOW_NAME", cfg.get("workflowRef", ""))
    step_id = os.environ.get("QONDUCTOR_STEP_ID", "vqe_spsa_driver")
    client = client or K8sClient(mode=mode)

    logical_id = cfg.get("logicalCircuitId") or cfg.get("circuit_id")
    circuit_qasm = cfg.get("circuitQasm") or cfg.get("circuit_qasm", "")
    if not circuit_qasm and cfg.get("qasmPath"):
        with open(cfg["qasmPath"], encoding="utf-8") as fh:
            circuit_qasm = fh.read()
    if not logical_id or not circuit_qasm:
        raise ValueError("VQE config requires logicalCircuitId and circuitQasm")

    parameter_names = cfg.get("parameterNames") or cfg.get("parameter_names") or []
    if not parameter_names:
        parameter_names = [f"theta_{i}" for i in range(int(cfg.get("parameterCount", 0)))]
    if not parameter_names:
        raise ValueError("VQE config requires parameterNames or parameterCount")

    qubits = int(cfg.get("qubits") or cfg.get("num_qubits") or 12)
    shots = int(cfg.get("shots", 1024))
    max_iterations = int(os.environ.get("R1_ITERATIONS", cfg.get("maxIterations", 200)))
    priority = cfg.get("priority", "balanced")
    timeout_s = float(cfg.get("quantumTimeoutSeconds", 900.0))
    poll_s = float(cfg.get("pollSeconds", 2.0))
    hamiltonian = cfg.get("hamiltonian", {})
    h_field = hamiltonian.get("hField", list(DEFAULT_H_FIELD))
    h_coupling = hamiltonian.get("hCoupling", list(DEFAULT_H_COUPLING))

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

        plus_qj = submit_quantum_eval(
            client,
            workflow_ref=workflow_ref,
            step_id=step_id,
            logical_circuit_id=logical_id,
            circuit_qasm=circuit_qasm,
            parameter_bindings=dict(zip(parameter_names, theta_plus)),
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
            parameter_bindings=dict(zip(parameter_names, theta_minus)),
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

        objective_plus = counts_energy_average(plus_result, h_field, h_coupling, qubits)
        objective_minus = counts_energy_average(minus_result, h_field, h_coupling, qubits)
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
        print(json.dumps({"event": "vqe_iteration", **entry}))

    summary = {
        "best_objective": best_objective,
        "best_theta": best_theta,
        "iterations": max_iterations,
        "history": history,
    }
    print(json.dumps({"event": "vqe_complete", **summary}))
    return summary
