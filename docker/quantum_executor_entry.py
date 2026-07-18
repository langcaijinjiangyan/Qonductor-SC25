#!/usr/bin/env python3
"""
Quantum circuit executor — container entry point for K8s quantum Jobs.

Reads the following environment variables (set by
``create_quantum_execution_job()`` in ``k8s_client.py``):

- ``QPU_NAME``              — name of the QPU backend (e.g. "qpu0_27q")
- ``QPU_JSON_PATH``         — path to the QPU calibration JSON file
- ``CIRCUIT_PAYLOAD_JSON``  — JSON list of {format, qasm} circuit payloads
- ``CIRCUIT_QASM``          — legacy single-circuit QASM fallback
- ``SHOTS``                 — number of measurement shots
- ``QUANTUM_JOB_NAME``      — optional QuantumJob CR to patch with results

Loads the QPU profile, deserialises QASM2/QASM3 circuits, runs them using
Qiskit Aer (``AerSimulator``) with the QPU's noise model from JSON, prints
measurement counts as JSON, and patches QuantumJob.status.result when it is
running inside Kubernetes with API access.
"""

from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

from src.utils.logging_config import configure_logging
from qiskit import QuantumCircuit, qasm3, transpile
from qiskit_aer import AerSimulator
from qiskit_aer.noise import NoiseModel, ReadoutError
from qiskit_aer.noise.errors import depolarizing_error, thermal_relaxation_error


def load_qpu_json(path: str) -> dict:
    """Load a QPU calibration JSON profile."""
    with open(path) as fh:
        return json.load(fh)


def build_noise_model(qpu_data: dict) -> NoiseModel:
    """Build an Aer ``NoiseModel`` from QPU JSON calibration data.

    Uses T1/T2 thermal relaxation for single-qubit errors and depolarising
    error for two-qubit gates, parameterised from the JSON edge/readout data.
    """
    hw = qpu_data["hardware"]
    noise = NoiseModel()

    for i, q in enumerate(hw["qubits"]):
        t1 = q.get("t1_ns", 120_000) * 1e-9  # ns -> s
        t2 = q.get("t2_ns", 90_000) * 1e-9
        noise.add_quantum_error(
            thermal_relaxation_error(t1, t2, 35.5e-9),
            "sx", [i],
        )
        noise.add_quantum_error(
            thermal_relaxation_error(t1, t2, 35.7e-9),
            "x", [i],
        )

    for edge in hw["edges"]:
        err = edge.get("error", 0.01)
        noise.add_quantum_error(
            depolarizing_error(err, 2),
            edge.get("gate", "cx"),
            [edge["source"], edge["target"]],
        )

    for i, q in enumerate(hw["qubits"]):
        ro_err = q.get("readout_error", 0.02)
        noise.add_readout_error(
            ReadoutError([[1 - ro_err, ro_err], [ro_err, 1 - ro_err]]),
            [i],
        )

    return noise


def _load_payloads() -> list[dict[str, str]]:
    payload_json = os.environ.get("CIRCUIT_PAYLOAD_JSON", "")
    if payload_json:
        payload = json.loads(payload_json)
        if not isinstance(payload, list):
            raise ValueError("CIRCUIT_PAYLOAD_JSON must be a list")
        return payload

    circuit_qasm = os.environ.get("CIRCUIT_QASM", "")
    if not circuit_qasm:
        return []
    return [{
        "format": os.environ.get("CIRCUIT_FORMAT", "qasm2"),
        "qasm": circuit_qasm,
    }]


def _load_circuit(item: dict[str, str]) -> QuantumCircuit:
    qasm_text = item.get("qasm", "")
    circuit_format = (item.get("format") or "qasm2").lower()
    if not qasm_text:
        raise ValueError("empty QASM payload")
    if circuit_format == "qasm3":
        qc = qasm3.loads(qasm_text)
    else:
        from qiskit import qasm2
        qc = qasm2.loads(qasm_text)

    # Bind parameters if provided in the payload item.
    binds = item.get("parameter_binds")
    if binds:
        qc = qc.assign_parameters(binds)
    return qc


def _patch_quantum_job_status(status: dict[str, Any]) -> None:
    qj_name = os.environ.get("QUANTUM_JOB_NAME", "")
    if not qj_name or os.environ.get("QONDUCTOR_MODE") != "k8s":
        return

    try:
        from kubernetes import client, config

        namespace = os.environ.get("QONDUCTOR_NAMESPACE", "default")
        config.load_incluster_config()
        api = client.CustomObjectsApi()
        api.patch_namespaced_custom_object_status(
            group="qonductor.io",
            version="v1",
            namespace=namespace,
            plural="quantumjobs",
            name=qj_name,
            body={"status": status},
        )
    except Exception as exc:
        print(json.dumps({
            "warning": "failed to patch QuantumJob status",
            "error": str(exc),
        }), file=sys.stderr)


def main() -> None:
    configure_logging()

    qpu_name = os.environ.get("QPU_NAME", "")
    qpu_json_path = os.environ.get("QPU_JSON_PATH", "")
    shots = int(os.environ.get("SHOTS", "4000"))

    started = time.monotonic()
    try:
        payloads = _load_payloads()
        if not qpu_json_path or not payloads:
            raise ValueError("Missing QPU_JSON_PATH or circuit payload env var")

        qpu_data = load_qpu_json(qpu_json_path)
        noise = build_noise_model(qpu_data)
        backend = AerSimulator(
            noise_model=noise,
            coupling_map=qpu_data["coupling_map"],
            basis_gates=(
                qpu_data["hardware"]["native_single_qubit_gates"]
                + [qpu_data["hardware"]["native_two_qubit_gate"]]
            ),
        )
        circuits = [
            transpile(
                _load_circuit(item).decompose(reps=10),
                backend=backend,
                optimization_level=0,
            )
            for item in payloads
        ]

        job = backend.run(circuits, shots=shots)
        result = job.result()
        elapsed = time.monotonic() - started

        # --- Optional: compute actual Hellinger fidelity ----------
        actual_fidelity = None
        if os.environ.get("COMPUTE_ACTUAL_FIDELITY", "").lower() in (
            "1", "true", "yes",
        ):
            import numpy as np

            ideal_backend = AerSimulator()
            ideal_result = ideal_backend.run(circuits, shots=shots).result()

            fidelities = []
            for i in range(len(circuits)):
                noisy_counts = result.get_counts(i)
                ideal_counts = ideal_result.get_counts(i)
                all_keys = (
                    set(noisy_counts.keys()) | set(ideal_counts.keys())
                )
                noisy_probs = np.array(
                    [noisy_counts.get(k, 0) / shots for k in all_keys]
                )
                ideal_probs = np.array(
                    [ideal_counts.get(k, 0) / shots for k in all_keys]
                )
                hd = (
                    np.sqrt(
                        np.sum(
                            (np.sqrt(noisy_probs) - np.sqrt(ideal_probs))
                            ** 2,
                        )
                    )
                    / np.sqrt(2)
                )
                fidelities.append(1.0 - hd**2)
            actual_fidelity = float(np.mean(fidelities))

        output = {
            "qpu": qpu_name,
            "shots": shots,
            "circuit_count": len(circuits),
            "execution_time_seconds": elapsed,
            "results": [
                {
                    "counts": dict(result.get_counts(i)),
                    "success": result.success,
                }
                for i in range(len(circuits))
            ],
        }
        print(json.dumps(output))
        status_patch: dict[str, Any] = {
            "phase": "Completed",
            "actualExecutionTime": elapsed,
            "result": output,
        }
        if actual_fidelity is not None:
            status_patch["actualFidelity"] = actual_fidelity
        _patch_quantum_job_status(status_patch)
    except Exception as exc:
        error = {"error": str(exc)}
        print(json.dumps(error))
        _patch_quantum_job_status({
            "phase": "Failed",
            "conditions": [{
                "type": "ExecutionFailed",
                "status": "True",
                "reason": str(exc),
            }],
            "result": error,
        })
        sys.exit(1)


if __name__ == "__main__":
    main()
