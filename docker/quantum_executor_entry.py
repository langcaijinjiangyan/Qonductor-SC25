#!/usr/bin/env python3
"""
Quantum circuit executor — container entry point for K8s quantum Jobs.

Reads the following environment variables (set by
``create_quantum_execution_job()`` in ``k8s_client.py``):

- ``QPU_NAME``              — name of the QPU backend (e.g. "qpu0_27q")
- ``QPU_JSON_PATH``         — path to the QPU calibration JSON file
- ``CIRCUIT_PAYLOAD_PATH``  — JSON file with {format, qasm} circuit payloads
- ``CIRCUIT_PAYLOAD_JSON``  — legacy JSON list payload fallback
- ``CIRCUIT_QASM``          — legacy single-circuit QASM fallback
- ``SHOTS``                 — number of measurement shots
- ``QUANTUM_JOB_NAME``      — optional QuantumJob CR to patch with results
- ``QONDUCTOR_EXECUTION_BACKEND`` — ``aer`` or ``offline-replay``
- ``QONDUCTOR_OFFLINE_RESULTS_PATH`` — read-only SQLite database path

Loads the QPU profile and either runs Qiskit Aer with its noise model or
replays an exact result from the offline SQLite database. Prints measurement
counts as JSON and patches QuantumJob.status.result when running in Kubernetes.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from typing import Any

from src.utils.logging_config import configure_logging
from qiskit import QuantumCircuit, qasm3, transpile
from qiskit_aer import AerSimulator
from qiskit_aer.noise import NoiseModel, ReadoutError
from qiskit_aer.noise.errors import depolarizing_error, thermal_relaxation_error


OFFLINE_REPLAY_BACKEND = "offline-replay"
DEFAULT_NOISE_MODEL_VERSION = "qpu-json-depolarizing-readout-v1"


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


def _decode_payload_json(payload_json: str, source: str) -> list[dict[str, Any]]:
    payload = json.loads(payload_json)
    if not isinstance(payload, list):
        raise ValueError(f"{source} must be a list")
    return payload


def _load_payloads() -> list[dict[str, Any]]:
    payload_path = os.environ.get("CIRCUIT_PAYLOAD_PATH", "")
    if payload_path:
        with open(payload_path, encoding="utf-8") as fh:
            return _decode_payload_json(fh.read(), "CIRCUIT_PAYLOAD_PATH")

    payload_json = os.environ.get("CIRCUIT_PAYLOAD_JSON", "")
    if payload_json:
        return _decode_payload_json(payload_json, "CIRCUIT_PAYLOAD_JSON")

    circuit_qasm = os.environ.get("CIRCUIT_QASM", "")
    if not circuit_qasm:
        return []
    return [{
        "format": os.environ.get("CIRCUIT_FORMAT", "qasm2"),
        "qasm": circuit_qasm,
    }]


def _load_circuit(item: dict[str, Any]) -> QuantumCircuit:
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


def _canonical_parameter_bindings(bindings: dict[str, Any]) -> str:
    if not isinstance(bindings, dict):
        raise ValueError("offline parameter bindings must be a JSON object")
    return json.dumps(
        bindings,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _replay_offline_results(
    payloads: list[dict[str, Any]],
    *,
    db_path: str,
    qpu_name: str,
    qpu_data: dict[str, Any],
    shots: int,
    noise_model_version: str,
) -> tuple[list[dict[str, Any]], float, list[dict[str, str]]]:
    if not db_path:
        raise ValueError("QONDUCTOR_OFFLINE_RESULTS_PATH is required")
    if not Path(db_path).is_file():
        raise FileNotFoundError(f"offline results database not found: {db_path}")

    profile_version = str(qpu_data.get("profile_version", ""))
    if not profile_version:
        raise ValueError("QPU profile is missing profile_version")
    lookup_qpu_name = str(
        qpu_data.get("offline_results_qpu_name")
        or qpu_data.get("qonductor_base_name")
        or qpu_name
    )

    db_uri = f"{Path(db_path).resolve().as_uri()}?mode=ro"
    connection = sqlite3.connect(db_uri, uri=True)
    connection.row_factory = sqlite3.Row
    replayed_results: list[dict[str, Any]] = []
    lookup_keys: list[dict[str, str]] = []
    total_execution_time_ns = 0.0
    try:
        for item in payloads:
            lookup = item.get("offline_lookup")
            if not isinstance(lookup, dict):
                raise ValueError(
                    "offline-replay payload is missing offline_lookup metadata"
                )

            qasm_sha256 = str(lookup.get("qasm_sha256", ""))
            if len(qasm_sha256) != 64:
                raise ValueError("offline_lookup.qasm_sha256 is invalid")
            logical_id = str(lookup.get("logical_id", ""))
            bindings_json = _canonical_parameter_bindings(
                lookup.get("parameter_bindings", {})
            )
            bindings_sha256 = hashlib.sha256(
                bindings_json.encode("utf-8")
            ).hexdigest()
            key = {
                "qasm_sha256": qasm_sha256,
                "parameter_bindings_sha256": bindings_sha256,
                "qpu_name": lookup_qpu_name,
                "profile_version": profile_version,
                "shots": str(shots),
                "noise_model_version": noise_model_version,
            }
            rows = connection.execute(
                """
                SELECT qasm_sha256, counts_json, simulated_execution_time_ns
                FROM hardware_results
                WHERE qasm_sha256 = ?
                  AND parameter_bindings_sha256 = ?
                  AND qpu_name = ?
                  AND profile_version = ?
                  AND shots = ?
                  AND noise_model_version = ?
                """,
                (
                    qasm_sha256,
                    bindings_sha256,
                    lookup_qpu_name,
                    profile_version,
                    shots,
                    noise_model_version,
                ),
            ).fetchall()
            if not rows and logical_id:
                rows = connection.execute(
                    """
                    SELECT qasm_sha256, counts_json,
                           simulated_execution_time_ns
                    FROM hardware_results
                    WHERE logical_id = ?
                      AND parameter_bindings_sha256 = ?
                      AND qpu_name = ?
                      AND profile_version = ?
                      AND shots = ?
                      AND noise_model_version = ?
                    LIMIT 2
                    """,
                    (
                        logical_id,
                        bindings_sha256,
                        lookup_qpu_name,
                        profile_version,
                        shots,
                        noise_model_version,
                    ),
                ).fetchall()
                if len(rows) == 1:
                    key["submitted_qasm_sha256"] = qasm_sha256
                    key["qasm_sha256"] = str(rows[0]["qasm_sha256"])
                    key["resolved_by"] = "logical_id"
                    key["logical_id"] = logical_id
            if not rows:
                raise LookupError(
                    "offline result not found for key "
                    + json.dumps(key, sort_keys=True)
                )
            if len(rows) != 1:
                raise LookupError(
                    "offline result is ambiguous for logical_id fallback "
                    + json.dumps(key, sort_keys=True)
                )
            row = rows[0]

            counts = json.loads(row["counts_json"])
            if not isinstance(counts, dict):
                raise ValueError("offline counts_json must contain an object")
            counts = {
                str(bitstring): int(count)
                for bitstring, count in counts.items()
            }
            if any(count < 0 for count in counts.values()):
                raise ValueError("offline counts_json contains a negative count")
            if sum(counts.values()) != shots:
                raise ValueError(
                    "offline counts do not sum to requested shots: "
                    f"{sum(counts.values())} != {shots}"
                )

            execution_time_ns = float(row["simulated_execution_time_ns"])
            if execution_time_ns < 0:
                raise ValueError("offline execution time must be non-negative")
            total_execution_time_ns += execution_time_ns
            replayed_results.append({"counts": counts, "success": True})
            lookup_keys.append(key)
    finally:
        connection.close()

    replay_seconds = total_execution_time_ns / 1e9
    time.sleep(replay_seconds)
    return replayed_results, replay_seconds, lookup_keys


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
    execution_backend = os.environ.get(
        "QONDUCTOR_EXECUTION_BACKEND", "aer"
    ).strip().lower()

    started = time.monotonic()
    try:
        payloads = _load_payloads()
        if not qpu_json_path or not payloads:
            raise ValueError("Missing QPU_JSON_PATH or circuit payload")

        qpu_data = load_qpu_json(qpu_json_path)
        actual_fidelity = None
        replay_seconds = None
        lookup_keys = None
        if execution_backend == OFFLINE_REPLAY_BACKEND:
            result_items, replay_seconds, lookup_keys = _replay_offline_results(
                payloads,
                db_path=os.environ.get("QONDUCTOR_OFFLINE_RESULTS_PATH", ""),
                qpu_name=qpu_name,
                qpu_data=qpu_data,
                shots=shots,
                noise_model_version=os.environ.get(
                    "QONDUCTOR_OFFLINE_NOISE_MODEL_VERSION",
                    DEFAULT_NOISE_MODEL_VERSION,
                ),
            )
        elif execution_backend == "aer":
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
            result_items = [
                {
                    "counts": dict(result.get_counts(i)),
                    "success": result.success,
                }
                for i in range(len(circuits))
            ]

            # --- Optional: compute actual Hellinger fidelity ----------
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
                    all_keys = set(noisy_counts.keys()) | set(ideal_counts.keys())
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
        else:
            raise ValueError(
                "QONDUCTOR_EXECUTION_BACKEND must be 'aer' or "
                f"'{OFFLINE_REPLAY_BACKEND}', got {execution_backend!r}"
            )

        elapsed = time.monotonic() - started

        output = {
            "qpu": qpu_name,
            "shots": shots,
            "circuit_count": len(payloads),
            "execution_backend": execution_backend,
            "execution_time_seconds": elapsed,
            "results": result_items,
        }
        if replay_seconds is not None:
            output["replayed_execution_time_seconds"] = replay_seconds
            output["offline_lookup_keys"] = lookup_keys
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
