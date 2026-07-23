from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3


ENTRYPOINT = Path(__file__).parents[2] / "docker" / "quantum_executor_entry.py"


def _load_executor_module():
    spec = importlib.util.spec_from_file_location("quantum_executor_entry", ENTRYPOINT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_offline_replay_uses_exact_key_and_database_time(tmp_path, monkeypatch):
    executor = _load_executor_module()
    db_path = tmp_path / "offline.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE hardware_results (
            qasm_sha256 TEXT NOT NULL,
            parameter_bindings_sha256 TEXT NOT NULL,
            qpu_name TEXT NOT NULL,
            profile_version TEXT NOT NULL,
            shots INTEGER NOT NULL,
            noise_model_version TEXT NOT NULL,
            counts_json TEXT NOT NULL,
            simulated_execution_time_ns REAL NOT NULL,
            PRIMARY KEY (
                qasm_sha256,
                parameter_bindings_sha256,
                qpu_name,
                profile_version,
                shots,
                noise_model_version
            )
        )
        """
    )
    bindings = {"_θ_0_": 0.25}
    canonical = executor._canonical_parameter_bindings(bindings)
    bindings_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    qasm_hash = "a" * 64
    connection.execute(
        """
        INSERT INTO hardware_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            qasm_hash,
            bindings_hash,
            "qpu0_27q",
            "v1",
            4,
            executor.DEFAULT_NOISE_MODEL_VERSION,
            json.dumps({"0": 3, "1": 1}),
            25_000_000.0,
        ),
    )
    connection.commit()
    connection.close()

    sleeps = []
    monkeypatch.setattr(executor.time, "sleep", sleeps.append)
    results, replay_seconds, keys = executor._replay_offline_results(
        [{
            "offline_lookup": {
                "qasm_sha256": qasm_hash,
                "parameter_bindings": bindings,
            },
        }],
        db_path=str(db_path),
        qpu_name="qpu0-instance",
        qpu_data={
            "profile_version": "v1",
            "qonductor_base_name": "qpu0_27q",
        },
        shots=4,
        noise_model_version=executor.DEFAULT_NOISE_MODEL_VERSION,
    )

    assert results == [{"counts": {"0": 3, "1": 1}, "success": True}]
    assert replay_seconds == 0.025
    assert sleeps == [0.025]
    assert keys[0]["parameter_bindings_sha256"] == bindings_hash
