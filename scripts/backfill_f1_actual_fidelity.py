#!/usr/bin/env python3
"""Backfill offline ideal-Aer actual fidelity into exported F1 run data.

The script deliberately treats ``quantum_job_crs_raw.json`` as the source of
truth: it reads the submitted QASM and parameter bindings from ``spec`` and
the observed counts from ``status.result.results[0]``.  Large JSON exports are
parsed and rewritten one object at a time.

Two modes are available:

* ``--analyze-only`` validates and inventories the inputs without Aer work or
  data-file changes.
* ``--in-place`` computes ideal counts, stages and validates every output,
  creates ``.pre_actual_fidelity`` backups, and atomically replaces the
  exported metric files.

The on-disk SQLite checkpoint is task-local working state.  It is not the
Qonductor offline-results database and this script never modifies that
database.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import multiprocessing
import os
import shutil
import sqlite3
import statistics
import sys
import tempfile
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_RUNS = (
    PROJECT_ROOT / "data" / "f1_qonductor_runs" / "f1-mixed1500-75",
    PROJECT_ROOT / "data" / "f1_qonductor_runs" / "f1-mixed1500-3600",
)
DEFAULT_SEED = 20260730
BACKUP_SUFFIX = ".pre_actual_fidelity"
TEMP_SUFFIX = ".actual_fidelity.tmp"
SUMMARY_NAME = "actual_fidelity_backfill_summary.json"
CHECKPOINT_NAME = ".actual_fidelity_backfill.sqlite"
FORMULA_NAME = "1 - hellinger_distance**2"
METHOD_NAME = "ideal_aer_sampled_counts"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "run_dirs",
        nargs="*",
        type=Path,
        help="F1 run directories (default: f1-mixed1500-75 and f1-mixed1500-3600)",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--analyze-only",
        action="store_true",
        help="validate and inventory inputs without simulation or data changes",
    )
    mode.add_argument(
        "--in-place",
        action="store_true",
        help="compute and transactionally backfill the exported data",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="reuse completed ideal simulations from the checkpoint",
    )
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--checkpoint",
        type=Path,
        default=None,
        help="checkpoint path (default: common run parent/.actual_fidelity_backfill.sqlite)",
    )
    return parser.parse_args()


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sim_key_for(
    circuit_format: str,
    qasm_text: str,
    bindings: dict[str, Any],
    shots: int,
) -> tuple[str, str, str]:
    qasm_sha256 = sha256_text(qasm_text)
    bindings_json = canonical_json(bindings)
    bindings_sha256 = sha256_text(bindings_json)
    material = canonical_json(
        [circuit_format, qasm_sha256, bindings_sha256, shots],
    )
    return sha256_text(material), qasm_sha256, bindings_json


def project_fidelity(
    observed_counts: dict[str, int],
    ideal_counts: dict[str, int],
) -> float:
    observed_total = sum(observed_counts.values())
    ideal_total = sum(ideal_counts.values())
    if observed_total <= 0 or ideal_total <= 0:
        raise ValueError("counts distributions must have a positive total")
    keys = set(observed_counts) | set(ideal_counts)
    squared_delta_sum = sum(
        (
            math.sqrt(observed_counts.get(key, 0) / observed_total)
            - math.sqrt(ideal_counts.get(key, 0) / ideal_total)
        )
        ** 2
        for key in keys
    )
    fidelity = 1.0 - squared_delta_sum / 2.0
    return min(1.0, max(0.0, float(fidelity)))


def formula_self_check() -> None:
    identical = project_fidelity({"0": 3, "1": 1}, {"0": 6, "1": 2})
    disjoint = project_fidelity({"0": 4}, {"1": 4})
    if not math.isclose(identical, 1.0, abs_tol=1e-12):
        raise AssertionError(f"identical-distribution fidelity is {identical}")
    if not math.isclose(disjoint, 0.0, abs_tol=1e-12):
        raise AssertionError(f"disjoint-distribution fidelity is {disjoint}")


def iter_object_items(path: Path) -> Iterator[dict[str, Any]]:
    """Yield objects from the top-level ``items`` array in a JSON object."""
    saw_items = False
    item_indent: int | None = None
    buffer: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as fp:
        for line in fp:
            if not saw_items:
                saw_items = '"items"' in line and "[" in line
                continue
            if item_indent is None:
                if line.lstrip().startswith("{"):
                    item_indent = len(line) - len(line.lstrip())
                    buffer = [line]
                continue
            buffer.append(line)
            indent = len(line) - len(line.lstrip())
            if indent == item_indent and line.strip() in ("},", "}"):
                yield json.loads("".join(buffer).rstrip().rstrip(","))
                item_indent = None
                buffer = []
    if not saw_items:
        raise ValueError(f"{path}: top-level items array not found")
    if item_indent is not None:
        raise ValueError(f"{path}: unterminated item")


def iter_array_objects(path: Path) -> Iterator[dict[str, Any]]:
    """Yield objects from a pretty-printed top-level JSON array."""
    saw_array = False
    item_indent: int | None = None
    buffer: list[str] = []
    with path.open("r", encoding="utf-8", newline="") as fp:
        for line in fp:
            stripped = line.strip()
            if not saw_array:
                if stripped == "[":
                    saw_array = True
                continue
            if item_indent is None:
                if line.lstrip().startswith("{"):
                    item_indent = len(line) - len(line.lstrip())
                    buffer = [line]
                continue
            buffer.append(line)
            indent = len(line) - len(line.lstrip())
            if indent == item_indent and stripped in ("},", "}"):
                yield json.loads("".join(buffer).rstrip().rstrip(","))
                item_indent = None
                buffer = []
    if not saw_array:
        raise ValueError(f"{path}: top-level array not found")
    if item_indent is not None:
        raise ValueError(f"{path}: unterminated array item")


def validate_counts(value: Any, shots: int, source: str) -> dict[str, int]:
    if not isinstance(value, dict) or not value:
        raise ValueError(f"{source}: counts must be a non-empty object")
    counts: dict[str, int] = {}
    for key, count in value.items():
        if not isinstance(key, str):
            raise ValueError(f"{source}: counts key is not a string")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise ValueError(f"{source}: invalid count for {key!r}: {count!r}")
        counts[key] = count
    if sum(counts.values()) != shots:
        raise ValueError(
            f"{source}: counts total {sum(counts.values())} != shots {shots}",
        )
    return counts


def open_checkpoint(path: Path, *, resume: bool, seed: int) -> sqlite3.Connection:
    if path.exists() and not resume:
        path.unlink()
        path.with_name(path.name + "-wal").unlink(missing_ok=True)
        path.with_name(path.name + "-shm").unlink(missing_ok=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs (
            run_name TEXT PRIMARY KEY,
            run_path TEXT NOT NULL,
            item_count INTEGER NOT NULL,
            workflow_count INTEGER NOT NULL,
            actual_present_before INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS circuits (
            sim_key TEXT PRIMARY KEY,
            circuit_format TEXT NOT NULL,
            qasm_sha256 TEXT NOT NULL,
            qasm_text TEXT NOT NULL,
            bindings_json TEXT NOT NULL,
            shots INTEGER NOT NULL,
            ideal_counts_json TEXT
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_name TEXT PRIMARY KEY,
            workflow_name TEXT NOT NULL,
            sim_key TEXT NOT NULL,
            counts_json TEXT NOT NULL,
            counts_sha256 TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS job_sources (
            run_name TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            job_name TEXT NOT NULL,
            PRIMARY KEY (run_name, ordinal),
            UNIQUE (run_name, job_name)
        );
        CREATE TABLE IF NOT EXISTS fidelities (
            job_name TEXT PRIMARY KEY,
            sim_key TEXT NOT NULL,
            counts_sha256 TEXT NOT NULL,
            value REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_jobs_sim_key ON jobs(sim_key);
        CREATE INDEX IF NOT EXISTS idx_sources_job ON job_sources(job_name);
        """,
    )
    existing_seed = connection.execute(
        "SELECT value FROM metadata WHERE key = 'seed'",
    ).fetchone()
    if existing_seed is not None and int(existing_seed["value"]) != seed:
        raise ValueError(
            f"checkpoint seed is {existing_seed['value']}, requested {seed}",
        )
    connection.execute(
        "INSERT OR REPLACE INTO metadata(key, value) VALUES('seed', ?)",
        (str(seed),),
    )
    connection.execute("DELETE FROM runs")
    connection.execute("DELETE FROM job_sources")
    connection.execute("DELETE FROM jobs")
    connection.commit()
    return connection


def insert_or_validate_circuit(
    connection: sqlite3.Connection,
    *,
    sim_key: str,
    circuit_format: str,
    qasm_sha256: str,
    qasm_text: str,
    bindings_json: str,
    shots: int,
) -> None:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO circuits(
            sim_key, circuit_format, qasm_sha256, qasm_text,
            bindings_json, shots, ideal_counts_json
        ) VALUES(?, ?, ?, ?, ?, ?, NULL)
        """,
        (
            sim_key,
            circuit_format,
            qasm_sha256,
            qasm_text,
            bindings_json,
            shots,
        ),
    )
    if cursor.rowcount:
        return
    row = connection.execute(
        """
        SELECT circuit_format, qasm_sha256, qasm_text, bindings_json, shots
        FROM circuits WHERE sim_key = ?
        """,
        (sim_key,),
    ).fetchone()
    expected = (
        circuit_format,
        qasm_sha256,
        qasm_text,
        bindings_json,
        shots,
    )
    actual = tuple(row) if row is not None else None
    if actual != expected:
        raise ValueError(f"simulation-key collision for {sim_key}")


def insert_or_validate_job(
    connection: sqlite3.Connection,
    *,
    job_name: str,
    workflow_name: str,
    sim_key: str,
    counts_json: str,
    counts_sha256: str,
) -> None:
    cursor = connection.execute(
        """
        INSERT OR IGNORE INTO jobs(
            job_name, workflow_name, sim_key, counts_json, counts_sha256
        ) VALUES(?, ?, ?, ?, ?)
        """,
        (
            job_name,
            workflow_name,
            sim_key,
            counts_json,
            counts_sha256,
        ),
    )
    if cursor.rowcount:
        return
    row = connection.execute(
        """
        SELECT workflow_name, sim_key, counts_json, counts_sha256
        FROM jobs WHERE job_name = ?
        """,
        (job_name,),
    ).fetchone()
    expected = (workflow_name, sim_key, counts_json, counts_sha256)
    actual = tuple(row) if row is not None else None
    if actual != expected:
        raise ValueError(
            f"overlapping job {job_name!r} differs between run packages",
        )


def scan_run(
    connection: sqlite3.Connection,
    run_dir: Path,
) -> dict[str, Any]:
    raw_path = run_dir / "metrics" / "quantum_job_crs_raw.json"
    if not raw_path.is_file():
        raise FileNotFoundError(raw_path)
    run_name = run_dir.name
    workflows: set[str] = set()
    item_count = 0
    actual_present = 0
    bound_count = 0
    for ordinal, item in enumerate(iter_object_items(raw_path)):
        metadata = item.get("metadata") or {}
        spec = item.get("spec") or {}
        status = item.get("status") or {}
        job_name = str(metadata.get("name") or "")
        workflow_name = str(spec.get("workflowRef") or "")
        source = f"{run_name}:{job_name or ordinal}"
        if not job_name or not workflow_name:
            raise ValueError(f"{source}: missing job name or workflowRef")
        if status.get("phase") != "Completed":
            raise ValueError(f"{source}: phase is {status.get('phase')!r}")
        circuit_format = str(spec.get("circuitFormat") or "qasm2").lower()
        qasm_text = spec.get("circuitQasm")
        if not isinstance(qasm_text, str) or not qasm_text:
            raise ValueError(f"{source}: missing circuitQasm")
        bindings = spec.get("parameterBindings") or {}
        if not isinstance(bindings, dict):
            raise ValueError(f"{source}: parameterBindings is not an object")
        if bindings:
            bound_count += 1
        shots = spec.get("shots")
        if isinstance(shots, bool) or not isinstance(shots, int) or shots <= 0:
            raise ValueError(f"{source}: invalid shots {shots!r}")
        result = status.get("result") or {}
        results = result.get("results") if isinstance(result, dict) else None
        if not isinstance(results, list) or len(results) != 1:
            raise ValueError(f"{source}: expected exactly one result item")
        counts = validate_counts(results[0].get("counts"), shots, source)
        counts_json = canonical_json(counts)
        counts_sha256 = sha256_text(counts_json)
        sim_key, qasm_sha256, bindings_json = sim_key_for(
            circuit_format,
            qasm_text,
            bindings,
            shots,
        )
        insert_or_validate_circuit(
            connection,
            sim_key=sim_key,
            circuit_format=circuit_format,
            qasm_sha256=qasm_sha256,
            qasm_text=qasm_text,
            bindings_json=bindings_json,
            shots=shots,
        )
        insert_or_validate_job(
            connection,
            job_name=job_name,
            workflow_name=workflow_name,
            sim_key=sim_key,
            counts_json=counts_json,
            counts_sha256=counts_sha256,
        )
        connection.execute(
            """
            INSERT INTO job_sources(run_name, ordinal, job_name)
            VALUES(?, ?, ?)
            """,
            (run_name, ordinal, job_name),
        )
        item_count += 1
        workflows.add(workflow_name)
        actual_present += status.get("actualFidelity") is not None
        if item_count % 250 == 0:
            connection.commit()
    connection.execute(
        """
        INSERT INTO runs(
            run_name, run_path, item_count, workflow_count,
            actual_present_before
        ) VALUES(?, ?, ?, ?, ?)
        """,
        (
            run_name,
            str(run_dir.resolve()),
            item_count,
            len(workflows),
            actual_present,
        ),
    )
    connection.commit()
    return {
        "run_name": run_name,
        "items": item_count,
        "workflows": len(workflows),
        "bound_jobs": bound_count,
        "actual_present_before": actual_present,
    }


_WORKER_CIRCUIT_CACHE: dict[tuple[str, str], Any] = {}
_WORKER_BACKEND: Any = None


def _load_worker_circuit(
    circuit_format: str,
    qasm_sha256: str,
    qasm_text: str,
) -> Any:
    cache_key = (circuit_format, qasm_sha256)
    cached = _WORKER_CIRCUIT_CACHE.get(cache_key)
    if cached is not None:
        return cached
    if circuit_format == "qasm3":
        from qiskit import qasm3

        circuit = qasm3.loads(qasm_text)
    else:
        from qiskit import qasm2, qasm3

        try:
            circuit = qasm2.loads(qasm_text)
        except Exception:
            circuit = qasm3.loads(qasm_text)
    _WORKER_CIRCUIT_CACHE[cache_key] = circuit
    return circuit


def _bind_worker_circuit(circuit: Any, bindings: dict[str, Any]) -> Any:
    parameters = {parameter.name: parameter for parameter in circuit.parameters}
    parameters.update({str(parameter): parameter for parameter in circuit.parameters})
    unknown = sorted(name for name in bindings if name not in parameters)
    if unknown:
        raise ValueError(
            "parameter bindings not present in circuit: " + ", ".join(unknown),
        )
    assignments = {
        parameters[name]: float(value)
        for name, value in bindings.items()
    }
    bound = (
        circuit.assign_parameters(assignments, inplace=False)
        if assignments
        else circuit.copy()
    )
    if bound.parameters:
        names = ", ".join(sorted(str(parameter) for parameter in bound.parameters))
        raise ValueError(f"unbound circuit parameters remain: {names}")
    return bound


def _simulate_one(task: tuple[Any, ...]) -> tuple[str, str, float]:
    (
        sim_key,
        circuit_format,
        qasm_sha256,
        qasm_text,
        bindings_json,
        shots,
        seed,
    ) = task
    started = time.monotonic()
    from qiskit import transpile
    from qiskit_aer import AerSimulator

    global _WORKER_BACKEND
    if _WORKER_BACKEND is None:
        _WORKER_BACKEND = AerSimulator(
            max_parallel_threads=1,
            max_parallel_experiments=1,
        )
    base = _load_worker_circuit(circuit_format, qasm_sha256, qasm_text)
    bound = _bind_worker_circuit(base, json.loads(bindings_json))
    transpiled = transpile(
        bound.decompose(reps=10),
        backend=_WORKER_BACKEND,
        optimization_level=0,
        seed_transpiler=seed,
    )
    result = _WORKER_BACKEND.run(
        transpiled,
        shots=shots,
        seed_simulator=seed,
    ).result()
    counts = {str(key): int(value) for key, value in result.get_counts().items()}
    validate_counts(counts, shots, f"ideal simulation {sim_key}")
    return sim_key, canonical_json(counts), time.monotonic() - started


def pending_simulation_tasks(
    connection: sqlite3.Connection,
    seed: int,
) -> Iterator[tuple[Any, ...]]:
    rows = connection.execute(
        """
        SELECT DISTINCT
            c.sim_key, c.circuit_format, c.qasm_sha256, c.qasm_text,
            c.bindings_json, c.shots
        FROM circuits AS c
        JOIN jobs AS j ON j.sim_key = c.sim_key
        WHERE c.ideal_counts_json IS NULL
        ORDER BY c.sim_key
        """,
    )
    for row in rows:
        yield (
            row["sim_key"],
            row["circuit_format"],
            row["qasm_sha256"],
            row["qasm_text"],
            row["bindings_json"],
            row["shots"],
            seed,
        )


def simulate_pending(
    connection: sqlite3.Connection,
    *,
    workers: int,
    seed: int,
) -> dict[str, Any]:
    pending = connection.execute(
        """
        SELECT COUNT(DISTINCT c.sim_key)
        FROM circuits AS c
        JOIN jobs AS j ON j.sim_key = c.sim_key
        WHERE c.ideal_counts_json IS NULL
        """,
    ).fetchone()[0]
    total = connection.execute(
        "SELECT COUNT(DISTINCT sim_key) FROM jobs",
    ).fetchone()[0]
    if pending == 0:
        print(f"Ideal simulations already complete: {total}/{total}", flush=True)
        return {"total": total, "computed": 0, "elapsed_seconds": 0.0}
    print(
        f"Running {pending} pending ideal simulations "
        f"({total - pending} cached), workers={workers}, seed={seed}",
        flush=True,
    )
    started = time.monotonic()
    completed = 0
    simulated_seconds = 0.0
    tasks = list(pending_simulation_tasks(connection, seed))
    if len(tasks) != pending:
        raise ValueError(f"pending task count {len(tasks)} != expected {pending}")
    context = multiprocessing.get_context("spawn")
    with context.Pool(processes=workers) as pool:
        results = pool.imap_unordered(
            _simulate_one,
            tasks,
            chunksize=1,
        )
        for sim_key, counts_json, duration in results:
            connection.execute(
                """
                UPDATE circuits SET ideal_counts_json = ?
                WHERE sim_key = ?
                """,
                (counts_json, sim_key),
            )
            completed += 1
            simulated_seconds += duration
            if completed % 25 == 0:
                connection.commit()
            if completed % 50 == 0 or completed == pending:
                elapsed = time.monotonic() - started
                rate = completed / elapsed if elapsed else 0.0
                print(
                    f"Ideal simulations: {completed}/{pending} "
                    f"({rate:.2f}/s, elapsed={elapsed:.1f}s)",
                    flush=True,
                )
    connection.commit()
    return {
        "total": total,
        "computed": completed,
        "elapsed_seconds": time.monotonic() - started,
        "worker_simulation_seconds": simulated_seconds,
    }


def compute_job_fidelities(connection: sqlite3.Connection) -> dict[str, Any]:
    started = time.monotonic()
    computed = 0
    reused = 0
    sim_rows = connection.execute(
        """
        SELECT DISTINCT c.sim_key, c.ideal_counts_json
        FROM circuits AS c
        JOIN jobs AS j ON j.sim_key = c.sim_key
        ORDER BY c.sim_key
        """,
    )
    for sim_row in sim_rows:
        ideal_json = sim_row["ideal_counts_json"]
        if not ideal_json:
            raise ValueError(f"missing ideal counts for {sim_row['sim_key']}")
        ideal_counts = json.loads(ideal_json)
        job_rows = connection.execute(
            """
            SELECT job_name, counts_json, counts_sha256
            FROM jobs WHERE sim_key = ?
            """,
            (sim_row["sim_key"],),
        ).fetchall()
        for job in job_rows:
            existing = connection.execute(
                """
                SELECT sim_key, counts_sha256 FROM fidelities
                WHERE job_name = ?
                """,
                (job["job_name"],),
            ).fetchone()
            if (
                existing is not None
                and existing["sim_key"] == sim_row["sim_key"]
                and existing["counts_sha256"] == job["counts_sha256"]
            ):
                reused += 1
                continue
            value = project_fidelity(
                json.loads(job["counts_json"]),
                ideal_counts,
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO fidelities(
                    job_name, sim_key, counts_sha256, value
                ) VALUES(?, ?, ?, ?)
                """,
                (
                    job["job_name"],
                    sim_row["sim_key"],
                    job["counts_sha256"],
                    value,
                ),
            )
            computed += 1
            if computed % 250 == 0:
                connection.commit()
    connection.commit()
    expected = connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    actual = connection.execute(
        """
        SELECT COUNT(*)
        FROM fidelities AS f
        JOIN jobs AS j
          ON j.job_name = f.job_name
         AND j.sim_key = f.sim_key
         AND j.counts_sha256 = f.counts_sha256
        """,
    ).fetchone()[0]
    if actual != expected:
        raise ValueError(f"valid fidelities {actual} != jobs {expected}")
    return {
        "jobs": expected,
        "computed": computed,
        "reused": reused,
        "elapsed_seconds": time.monotonic() - started,
    }


def load_fidelity_map(connection: sqlite3.Connection) -> dict[str, float]:
    return {
        row["job_name"]: float(row["value"])
        for row in connection.execute(
            """
            SELECT j.job_name, f.value
            FROM jobs AS j
            JOIN fidelities AS f
              ON f.job_name = j.job_name
             AND f.sim_key = j.sim_key
             AND f.counts_sha256 = j.counts_sha256
            """,
        )
    }


def load_workflow_averages(
    connection: sqlite3.Connection,
    run_name: str,
) -> dict[str, float]:
    return {
        row["workflow_name"]: float(row["average"])
        for row in connection.execute(
            """
            SELECT j.workflow_name, AVG(f.value) AS average
            FROM job_sources AS s
            JOIN jobs AS j ON j.job_name = s.job_name
            JOIN fidelities AS f
              ON f.job_name = j.job_name
             AND f.sim_key = j.sim_key
             AND f.counts_sha256 = j.counts_sha256
            WHERE s.run_name = ?
            GROUP BY j.workflow_name
            """,
            (run_name,),
        )
    }


def render_indented(item: dict[str, Any], item_indent: int, step: int) -> str:
    rendered = json.dumps(item, indent=step, ensure_ascii=False)
    prefix = " " * item_indent
    return "\n".join(prefix + line for line in rendered.splitlines())


def transform_object_items(
    source: Path,
    destination: Path,
    transform: Callable[[dict[str, Any]], dict[str, Any]],
) -> int:
    saw_items = False
    items_key_indent = 0
    item_indent: int | None = None
    buffer: list[str] = []
    count = 0
    with source.open("r", encoding="utf-8", newline="") as src, destination.open(
        "x",
        encoding="utf-8",
        newline="\n",
    ) as dst:
        for line in src:
            if not saw_items:
                dst.write(line)
                if '"items"' in line and "[" in line:
                    saw_items = True
                    items_key_indent = len(line) - len(line.lstrip())
                continue
            if item_indent is None:
                if line.lstrip().startswith("{"):
                    item_indent = len(line) - len(line.lstrip())
                    buffer = [line]
                else:
                    dst.write(line)
                continue
            buffer.append(line)
            indent = len(line) - len(line.lstrip())
            if indent == item_indent and line.strip() in ("},", "}"):
                had_comma = line.strip() == "},"
                item = json.loads("".join(buffer).rstrip().rstrip(","))
                item = transform(item)
                step = item_indent - items_key_indent
                dst.write(render_indented(item, item_indent, step))
                if had_comma:
                    dst.write(",")
                dst.write("\n")
                count += 1
                item_indent = None
                buffer = []
        dst.flush()
        os.fsync(dst.fileno())
    if not saw_items or item_indent is not None:
        raise ValueError(f"{source}: invalid top-level items structure")
    return count


def transform_array_objects(
    source: Path,
    destination: Path,
    transform: Callable[[dict[str, Any]], dict[str, Any]],
) -> int:
    saw_array = False
    item_indent: int | None = None
    buffer: list[str] = []
    count = 0
    with source.open("r", encoding="utf-8", newline="") as src, destination.open(
        "x",
        encoding="utf-8",
        newline="\n",
    ) as dst:
        for line in src:
            stripped = line.strip()
            if not saw_array:
                dst.write(line)
                if stripped == "[":
                    saw_array = True
                continue
            if item_indent is None:
                if line.lstrip().startswith("{"):
                    item_indent = len(line) - len(line.lstrip())
                    buffer = [line]
                else:
                    dst.write(line)
                continue
            buffer.append(line)
            indent = len(line) - len(line.lstrip())
            if indent == item_indent and stripped in ("},", "}"):
                had_comma = stripped == "},"
                item = json.loads("".join(buffer).rstrip().rstrip(","))
                item = transform(item)
                dst.write(render_indented(item, item_indent, item_indent))
                if had_comma:
                    dst.write(",")
                dst.write("\n")
                count += 1
                item_indent = None
                buffer = []
        dst.flush()
        os.fsync(dst.fileno())
    if not saw_array or item_indent is not None:
        raise ValueError(f"{source}: invalid top-level array structure")
    return count


def stage_csv_files(
    metrics_csv: Path,
    metrics_destination: Path,
    jct_csv: Path,
    jct_destination: Path,
    fidelities: dict[str, float],
) -> tuple[int, list[str]]:
    order: list[str] = []
    timestamps: list[str] = []
    with metrics_csv.open("r", encoding="utf-8", newline="") as src, (
        metrics_destination.open("x", encoding="utf-8", newline="")
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames or "actual_fidelity" not in reader.fieldnames:
            raise ValueError(f"{metrics_csv}: actual_fidelity column missing")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            job_name = row.get("job_name", "")
            if job_name not in fidelities:
                raise ValueError(f"{metrics_csv}: unknown job {job_name!r}")
            row["actual_fidelity"] = repr(fidelities[job_name])
            writer.writerow(row)
            order.append(job_name)
            timestamps.append(row.get("arrived_at", ""))
        dst.flush()
        os.fsync(dst.fileno())
    with jct_csv.open("r", encoding="utf-8", newline="") as src, (
        jct_destination.open("x", encoding="utf-8", newline="")
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames or "fidelity" not in reader.fieldnames:
            raise ValueError(f"{jct_csv}: fidelity column missing")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames)
        writer.writeheader()
        row_count = 0
        for row_count, row in enumerate(reader, start=1):
            index = row_count - 1
            if index >= len(order):
                raise ValueError(f"{jct_csv}: more rows than metrics CSV")
            if row.get("timestamp", "") != timestamps[index]:
                raise ValueError(
                    f"{jct_csv}: timestamp differs at row {row_count}",
                )
            row["fidelity"] = f"{fidelities[order[index]]:.6f}"
            writer.writerow(row)
        if row_count != len(order):
            raise ValueError(
                f"{jct_csv}: rows {row_count} != metrics rows {len(order)}",
            )
        dst.flush()
        os.fsync(dst.fileno())
    return len(order), order


def stage_run(
    run_dir: Path,
    fidelities: dict[str, float],
    workflow_averages: dict[str, float],
) -> dict[str, Any]:
    paths = metric_paths(run_dir)
    staged = {
        key: path.with_name(path.name + TEMP_SUFFIX)
        for key, path in paths.items()
    }
    for path in staged.values():
        if path.exists():
            path.unlink()

    def raw_transform(item: dict[str, Any]) -> dict[str, Any]:
        job_name = str((item.get("metadata") or {}).get("name") or "")
        if job_name not in fidelities:
            raise ValueError(f"raw CR contains unknown job {job_name!r}")
        status = item.setdefault("status", {})
        status["actualFidelity"] = fidelities[job_name]
        return item

    def metrics_transform(item: dict[str, Any]) -> dict[str, Any]:
        job_name = str(item.get("job_name") or "")
        if job_name not in fidelities:
            raise ValueError(f"metrics JSON contains unknown job {job_name!r}")
        item["actual_fidelity"] = fidelities[job_name]
        return item

    def workflow_transform(item: dict[str, Any]) -> dict[str, Any]:
        workflow_name = str(item.get("workflow_name") or "")
        if workflow_name not in workflow_averages:
            raise ValueError(
                f"workflow metrics contains unknown workflow {workflow_name!r}",
            )
        item["actual_average_fidelity"] = workflow_averages[workflow_name]
        return item

    raw_count = transform_object_items(paths["raw"], staged["raw"], raw_transform)
    metrics_json_count = transform_array_objects(
        paths["metrics_json"],
        staged["metrics_json"],
        metrics_transform,
    )
    csv_count, order = stage_csv_files(
        paths["metrics_csv"],
        staged["metrics_csv"],
        paths["jct_csv"],
        staged["jct_csv"],
        fidelities,
    )
    workflow_count = transform_array_objects(
        paths["workflow_json"],
        staged["workflow_json"],
        workflow_transform,
    )
    if not (raw_count == metrics_json_count == csv_count):
        raise ValueError(
            f"{run_dir.name}: staged job counts differ: "
            f"raw={raw_count}, json={metrics_json_count}, csv={csv_count}",
        )
    return {
        "run_dir": run_dir,
        "paths": paths,
        "staged": staged,
        "job_count": raw_count,
        "workflow_count": workflow_count,
        "csv_order": order,
    }


def metric_paths(run_dir: Path) -> dict[str, Path]:
    metrics = run_dir / "metrics"
    return {
        "raw": metrics / "quantum_job_crs_raw.json",
        "metrics_json": metrics / "quantum_job_metrics.json",
        "metrics_csv": metrics / "quantum_job_metrics.csv",
        "jct_csv": metrics / "quantum_job_jct_fidelity.csv",
        "workflow_json": metrics / "workflow_metrics.json",
    }


def expected_run_rows(
    connection: sqlite3.Connection,
    run_name: str,
) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            s.ordinal, j.job_name, j.workflow_name, j.sim_key,
            j.counts_sha256, f.value
        FROM job_sources AS s
        JOIN jobs AS j ON j.job_name = s.job_name
        JOIN fidelities AS f
          ON f.job_name = j.job_name
         AND f.sim_key = j.sim_key
         AND f.counts_sha256 = j.counts_sha256
        WHERE s.run_name = ?
        ORDER BY s.ordinal
        """,
        (run_name,),
    ).fetchall()


def validate_raw_output(
    path: Path,
    expected: list[sqlite3.Row],
) -> None:
    count = 0
    for ordinal, item in enumerate(iter_object_items(path)):
        if ordinal >= len(expected):
            raise ValueError(f"{path}: unexpected extra raw CR")
        row = expected[ordinal]
        metadata = item.get("metadata") or {}
        spec = item.get("spec") or {}
        status = item.get("status") or {}
        job_name = str(metadata.get("name") or "")
        if job_name != row["job_name"]:
            raise ValueError(
                f"{path}: job order differs at {ordinal}: "
                f"{job_name!r} != {row['job_name']!r}",
            )
        fidelity = status.get("actualFidelity")
        if fidelity is None or not math.isclose(
            float(fidelity),
            float(row["value"]),
            abs_tol=1e-15,
        ):
            raise ValueError(f"{path}: actualFidelity mismatch for {job_name}")
        bindings = spec.get("parameterBindings") or {}
        sim_key, _, _ = sim_key_for(
            str(spec.get("circuitFormat") or "qasm2").lower(),
            str(spec.get("circuitQasm") or ""),
            bindings,
            int(spec.get("shots")),
        )
        if sim_key != row["sim_key"]:
            raise ValueError(f"{path}: circuit changed for {job_name}")
        result = status.get("result") or {}
        results = result.get("results") or []
        if len(results) != 1:
            raise ValueError(f"{path}: result changed for {job_name}")
        counts_json = canonical_json(results[0].get("counts"))
        if sha256_text(counts_json) != row["counts_sha256"]:
            raise ValueError(f"{path}: counts changed for {job_name}")
        count += 1
    if count != len(expected):
        raise ValueError(f"{path}: raw CR count mismatch")


def validate_metrics_json_output(
    path: Path,
    fidelities: dict[str, float],
    expected_count: int,
) -> None:
    seen: set[str] = set()
    for item in iter_array_objects(path):
        job_name = str(item.get("job_name") or "")
        if job_name in seen or job_name not in fidelities:
            raise ValueError(f"{path}: invalid job {job_name!r}")
        seen.add(job_name)
        if not math.isclose(
            float(item.get("actual_fidelity")),
            fidelities[job_name],
            abs_tol=1e-15,
        ):
            raise ValueError(f"{path}: fidelity mismatch for {job_name}")
    if len(seen) != expected_count:
        raise ValueError(f"{path}: rows {len(seen)} != {expected_count}")


def validate_csv_outputs(
    metrics_path: Path,
    jct_path: Path,
    expected_order: list[str],
    fidelities: dict[str, float],
) -> None:
    timestamps: list[str] = []
    with metrics_path.open("r", encoding="utf-8", newline="") as fp:
        rows = list(csv.DictReader(fp))
    if [row.get("job_name", "") for row in rows] != expected_order:
        raise ValueError(f"{metrics_path}: job order changed")
    for row in rows:
        job_name = row["job_name"]
        if not math.isclose(
            float(row["actual_fidelity"]),
            fidelities[job_name],
            abs_tol=1e-15,
        ):
            raise ValueError(f"{metrics_path}: fidelity mismatch for {job_name}")
        timestamps.append(row.get("arrived_at", ""))
    with jct_path.open("r", encoding="utf-8", newline="") as fp:
        jct_rows = list(csv.DictReader(fp))
    if len(jct_rows) != len(expected_order):
        raise ValueError(f"{jct_path}: row count changed")
    for index, row in enumerate(jct_rows):
        if row.get("timestamp", "") != timestamps[index]:
            raise ValueError(f"{jct_path}: timestamp differs at {index}")
        expected_value = f"{fidelities[expected_order[index]]:.6f}"
        if row.get("fidelity") != expected_value:
            raise ValueError(f"{jct_path}: fidelity differs at {index}")


def validate_workflow_output(
    path: Path,
    averages: dict[str, float],
) -> None:
    seen: set[str] = set()
    for item in iter_array_objects(path):
        workflow_name = str(item.get("workflow_name") or "")
        if workflow_name in seen or workflow_name not in averages:
            raise ValueError(f"{path}: invalid workflow {workflow_name!r}")
        seen.add(workflow_name)
        if not math.isclose(
            float(item.get("actual_average_fidelity")),
            averages[workflow_name],
            abs_tol=1e-15,
        ):
            raise ValueError(
                f"{path}: actual average mismatch for {workflow_name}",
            )
    if seen != set(averages):
        raise ValueError(f"{path}: workflow set differs")


def validate_run_outputs(
    connection: sqlite3.Connection,
    run_info: dict[str, Any],
    *,
    staged: bool,
    fidelities: dict[str, float],
    workflow_averages: dict[str, float],
) -> None:
    expected = expected_run_rows(connection, run_info["run_dir"].name)
    selected = run_info["staged"] if staged else run_info["paths"]
    validate_raw_output(selected["raw"], expected)
    validate_metrics_json_output(
        selected["metrics_json"],
        fidelities,
        len(expected),
    )
    validate_csv_outputs(
        selected["metrics_csv"],
        selected["jct_csv"],
        run_info["csv_order"],
        fidelities,
    )
    validate_workflow_output(selected["workflow_json"], workflow_averages)


def check_disk_space(run_infos: list[dict[str, Any]], checkpoint: Path) -> None:
    source_bytes = sum(
        path.stat().st_size
        for info in run_infos
        for path in info["paths"].values()
    )
    checkpoint_bytes = checkpoint.stat().st_size if checkpoint.exists() else 0
    required = int(source_bytes * 1.25 + max(checkpoint_bytes, 512 * 1024**2))
    available = shutil.disk_usage(run_infos[0]["run_dir"]).free
    if available < required:
        raise OSError(
            f"insufficient disk space: available={available}, required={required}",
        )
    print(
        f"Disk preflight: available={available / 1024**3:.2f} GiB, "
        f"required>={required / 1024**3:.2f} GiB",
        flush=True,
    )


def source_and_output_hashes(
    run_info: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for key, source in run_info["paths"].items():
        staged = run_info["staged"][key]
        result[key] = {
            "path": str(source),
            "input_bytes": source.stat().st_size,
            "input_sha256": sha256_file(source),
            "output_bytes": staged.stat().st_size,
            "output_sha256": sha256_file(staged),
            "backup": str(source.with_name(source.name + BACKUP_SUFFIX)),
        }
    return result


def run_statistics(
    connection: sqlite3.Connection,
    run_name: str,
) -> dict[str, Any]:
    values = [
        float(row["value"])
        for row in connection.execute(
            """
            SELECT f.value
            FROM job_sources AS s
            JOIN jobs AS j ON j.job_name = s.job_name
            JOIN fidelities AS f
              ON f.job_name = j.job_name
             AND f.sim_key = j.sim_key
             AND f.counts_sha256 = j.counts_sha256
            WHERE s.run_name = ?
            """,
            (run_name,),
        )
    ]
    unique_simulations = connection.execute(
        """
        SELECT COUNT(DISTINCT j.sim_key)
        FROM job_sources AS s
        JOIN jobs AS j ON j.job_name = s.job_name
        WHERE s.run_name = ?
        """,
        (run_name,),
    ).fetchone()[0]
    return {
        "job_count": len(values),
        "unique_simulation_keys": unique_simulations,
        "minimum": min(values),
        "maximum": max(values),
        "mean": statistics.fmean(values),
    }


def write_summary_stage(
    run_info: dict[str, Any],
    *,
    connection: sqlite3.Connection,
    seed: int,
    workers: int,
    hashes: dict[str, dict[str, Any]],
    global_unique_jobs: int,
    global_unique_simulations: int,
    overlap_jobs: int,
    simulation_report: dict[str, Any],
    fidelity_report: dict[str, Any],
) -> Path:
    import qiskit
    import qiskit_aer

    summary_path = run_info["run_dir"] / "metrics" / SUMMARY_NAME
    if summary_path.exists():
        raise FileExistsError(summary_path)
    stage_path = summary_path.with_name(summary_path.name + TEMP_SUFFIX)
    if stage_path.exists():
        stage_path.unlink()
    summary = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "method": METHOD_NAME,
        "formula": FORMULA_NAME,
        "formula_detail": (
            "H=sqrt(sum((sqrt(p_k)-sqrt(q_k))^2)/2); "
            "actual_fidelity=1-H^2"
        ),
        "ideal_backend": "qiskit_aer.AerSimulator(no noise model)",
        "seed_simulator": seed,
        "seed_transpiler": seed,
        "workers": workers,
        "qiskit_version": qiskit.__version__,
        "qiskit_aer_version": qiskit_aer.__version__,
        "run": run_info["run_dir"].name,
        "statistics": run_statistics(connection, run_info["run_dir"].name),
        "global": {
            "unique_jobs": global_unique_jobs,
            "unique_simulation_keys": global_unique_simulations,
            "overlap_jobs": overlap_jobs,
        },
        "simulation": simulation_report,
        "fidelity_calculation": fidelity_report,
        "files": hashes,
        "validation": {
            "staged_outputs": True,
            "post_commit_outputs": False,
            "qasm_parameter_counts_unchanged": True,
            "job_order_unchanged": True,
            "workflow_average_recomputed": True,
            "failures": [],
        },
    }
    with stage_path.open("x", encoding="utf-8") as fp:
        json.dump(summary, fp, indent=2, sort_keys=True)
        fp.write("\n")
        fp.flush()
        os.fsync(fp.fileno())
    run_info["summary_path"] = summary_path
    run_info["summary_stage"] = stage_path
    return stage_path


def ensure_backup_targets_absent(run_infos: list[dict[str, Any]]) -> None:
    conflicts = []
    for info in run_infos:
        for path in info["paths"].values():
            backup = path.with_name(path.name + BACKUP_SUFFIX)
            if backup.exists():
                conflicts.append(str(backup))
        summary = info["run_dir"] / "metrics" / SUMMARY_NAME
        if summary.exists():
            conflicts.append(str(summary))
    if conflicts:
        raise FileExistsError(
            "refusing to overwrite existing backups/summary:\n"
            + "\n".join(conflicts),
        )


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_backups(run_infos: list[dict[str, Any]]) -> dict[Path, Path]:
    backups: dict[Path, Path] = {}
    try:
        for info in run_infos:
            for source in info["paths"].values():
                backup = source.with_name(source.name + BACKUP_SUFFIX)
                os.link(source, backup)
                backups[source] = backup
            fsync_directory(info["run_dir"] / "metrics")
    except Exception:
        for backup in backups.values():
            backup.unlink(missing_ok=True)
        raise
    return backups


def restore_from_backups(
    backups: dict[Path, Path],
    summaries: list[Path],
) -> None:
    for source, backup in backups.items():
        restore = source.with_name(source.name + ".actual_fidelity.rollback")
        restore.unlink(missing_ok=True)
        os.link(backup, restore)
        os.replace(restore, source)
    for summary in summaries:
        summary.unlink(missing_ok=True)
    for directory in {path.parent for path in backups}:
        fsync_directory(directory)


def commit_staged_outputs(
    run_infos: list[dict[str, Any]],
) -> dict[Path, Path]:
    backups = create_backups(run_infos)
    summaries: list[Path] = []
    try:
        for info in run_infos:
            for key, destination in info["paths"].items():
                os.replace(info["staged"][key], destination)
            os.replace(info["summary_stage"], info["summary_path"])
            summaries.append(info["summary_path"])
            fsync_directory(info["run_dir"] / "metrics")
    except Exception:
        restore_from_backups(backups, summaries)
        raise
    return backups


def mark_post_commit_valid(run_infos: list[dict[str, Any]]) -> None:
    for info in run_infos:
        summary_path = info["summary_path"]
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        summary["validation"]["post_commit_outputs"] = True
        temporary = summary_path.with_name(summary_path.name + TEMP_SUFFIX)
        temporary.unlink(missing_ok=True)
        with temporary.open("x", encoding="utf-8") as fp:
            json.dump(summary, fp, indent=2, sort_keys=True)
            fp.write("\n")
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(temporary, summary_path)
        fsync_directory(summary_path.parent)


def inventory_report(
    connection: sqlite3.Connection,
    scan_reports: list[dict[str, Any]],
) -> dict[str, Any]:
    unique_jobs = connection.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
    source_jobs = connection.execute(
        "SELECT COUNT(*) FROM job_sources",
    ).fetchone()[0]
    unique_simulations = connection.execute(
        "SELECT COUNT(DISTINCT sim_key) FROM jobs",
    ).fetchone()[0]
    unique_qasm = connection.execute(
        """
        SELECT COUNT(DISTINCT c.qasm_sha256)
        FROM circuits AS c JOIN jobs AS j ON j.sim_key = c.sim_key
        """,
    ).fetchone()[0]
    return {
        "runs": scan_reports,
        "source_jobs": source_jobs,
        "unique_jobs": unique_jobs,
        "overlap_jobs": source_jobs - unique_jobs,
        "unique_qasm": unique_qasm,
        "unique_simulation_keys": unique_simulations,
    }


def resolve_run_dirs(values: list[Path]) -> list[Path]:
    paths = [path.resolve() for path in (values or list(DEFAULT_RUNS))]
    if len(paths) < 1:
        raise ValueError("at least one run directory is required")
    names = [path.name for path in paths]
    if len(set(names)) != len(names):
        raise ValueError("run directory names must be unique")
    for path in paths:
        if not path.is_dir():
            raise FileNotFoundError(path)
    return paths


def main() -> None:
    args = parse_args()
    if args.workers < 1:
        raise ValueError("--workers must be positive")
    formula_self_check()
    run_dirs = resolve_run_dirs(args.run_dirs)
    if args.checkpoint is not None:
        checkpoint = args.checkpoint.resolve()
    else:
        common_parent = Path(os.path.commonpath([str(path.parent) for path in run_dirs]))
        checkpoint = common_parent / CHECKPOINT_NAME

    temporary_directory: tempfile.TemporaryDirectory[str] | None = None
    if args.analyze_only:
        temporary_directory = tempfile.TemporaryDirectory(
            prefix="qonductor-actual-fidelity-analysis-",
        )
        checkpoint = Path(temporary_directory.name) / CHECKPOINT_NAME

    connection = open_checkpoint(
        checkpoint,
        resume=args.resume and not args.analyze_only,
        seed=args.seed,
    )
    try:
        scan_reports = []
        for run_dir in run_dirs:
            print(f"Scanning {run_dir}", flush=True)
            report = scan_run(connection, run_dir)
            scan_reports.append(report)
            print(json.dumps(report, sort_keys=True), flush=True)
        inventory = inventory_report(connection, scan_reports)
        print(json.dumps(inventory, indent=2, sort_keys=True), flush=True)
        if args.analyze_only:
            return

        preflight_infos = [
            {"run_dir": run_dir, "paths": metric_paths(run_dir)}
            for run_dir in run_dirs
        ]
        ensure_backup_targets_absent(preflight_infos)
        check_disk_space(preflight_infos, checkpoint)
        simulation_report = simulate_pending(
            connection,
            workers=args.workers,
            seed=args.seed,
        )
        fidelity_report = compute_job_fidelities(connection)
        fidelities = load_fidelity_map(connection)
        if len(fidelities) != inventory["unique_jobs"]:
            raise ValueError("fidelity map does not cover all unique jobs")

        print("Staging enriched exports", flush=True)
        run_infos = []
        workflow_averages_by_run: dict[str, dict[str, float]] = {}
        for run_dir in run_dirs:
            averages = load_workflow_averages(connection, run_dir.name)
            workflow_averages_by_run[run_dir.name] = averages
            run_info = stage_run(run_dir, fidelities, averages)
            run_infos.append(run_info)
            print(
                f"Staged {run_dir.name}: jobs={run_info['job_count']}, "
                f"workflows={run_info['workflow_count']}",
                flush=True,
            )

        print("Validating staged exports", flush=True)
        for info in run_infos:
            validate_run_outputs(
                connection,
                info,
                staged=True,
                fidelities=fidelities,
                workflow_averages=workflow_averages_by_run[
                    info["run_dir"].name
                ],
            )
            print(f"Validated staged {info['run_dir'].name}", flush=True)

        print("Hashing original and staged files", flush=True)
        for info in run_infos:
            hashes = source_and_output_hashes(info)
            info["hashes"] = hashes
            write_summary_stage(
                info,
                connection=connection,
                seed=args.seed,
                workers=args.workers,
                hashes=hashes,
                global_unique_jobs=inventory["unique_jobs"],
                global_unique_simulations=inventory["unique_simulation_keys"],
                overlap_jobs=inventory["overlap_jobs"],
                simulation_report=simulation_report,
                fidelity_report=fidelity_report,
            )
            print(f"Hashed and summarized {info['run_dir'].name}", flush=True)

        print("Creating backups and atomically replacing exports", flush=True)
        backups = commit_staged_outputs(run_infos)
        try:
            print("Running post-commit validation", flush=True)
            for info in run_infos:
                validate_run_outputs(
                    connection,
                    info,
                    staged=False,
                    fidelities=fidelities,
                    workflow_averages=workflow_averages_by_run[
                        info["run_dir"].name
                    ],
                )
                for key, path in info["paths"].items():
                    actual_hash = sha256_file(path)
                    expected_hash = info["hashes"][key]["output_sha256"]
                    if actual_hash != expected_hash:
                        raise ValueError(f"post-commit hash mismatch: {path}")
                print(f"Validated committed {info['run_dir'].name}", flush=True)
        except Exception:
            restore_from_backups(
                backups,
                [info["summary_path"] for info in run_infos],
            )
            raise
        mark_post_commit_valid(run_infos)
        print(
            json.dumps(
                {
                    "status": "completed",
                    "checkpoint": str(checkpoint),
                    "runs": [
                        {
                            "run": info["run_dir"].name,
                            "jobs": info["job_count"],
                            "workflows": info["workflow_count"],
                            "summary": str(info["summary_path"]),
                            "backups": [
                                str(path.with_name(path.name + BACKUP_SUFFIX))
                                for path in info["paths"].values()
                            ],
                        }
                        for info in run_infos
                    ],
                },
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )
    finally:
        connection.close()
        if temporary_directory is not None:
            temporary_directory.cleanup()


if __name__ == "__main__":
    main()
