#!/usr/bin/env python3
"""Convert and optionally submit an F1 qslurm manifest to Qonductor.

The input manifest keeps qslurm fields such as qworkdir/qspec/binary.  This
script rewrites those fields to the local workflow library and emits
Qonductor-native HybridWorkflow YAMLs in the original submit order.
Successful submissions automatically export batch-scoped workflow and
QuantumJob data under the converted batch directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_LIB = PROJECT_ROOT / "workflow" / "_lib"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(WORKFLOW_LIB) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_LIB))

from qslurm_workflow import (  # noqa: E402
    build_hybrid_image,
    build_manifest,
    build_pure_quantum_image,
    hybrid_inputs,
    k8s_name,
    load_spec_and_qasm,
    pure_quantum_inputs,
    qasm_format,
    register_image,
)


CORPUS_MARKER = "mqtbench2_20"
TERMINAL_PHASES = {"Completed", "Failed", "Succeeded"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-manifest", default="")
    parser.add_argument(
        "--manifest-root",
        default=str(PROJECT_ROOT / "data" / "F1_manifest"),
        help="root containing F1 scenario manifests (default: data/F1_manifest)",
    )
    parser.add_argument("--workflow-root", default=str(PROJECT_ROOT / "workflow"))
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "data" / "f1_qonductor_runs"))
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--registry-root", default="data/workflow_registry")
    parser.add_argument("--scenario", default="")
    parser.add_argument("--priority", choices=("balanced", "fidelity", "jct"), default="balanced")
    parser.add_argument("--shots", type=int, default=1024)
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument("--quantum-timeout-seconds", type=float, default=21600.0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--on-missing", choices=("fail", "skip"), default="fail")
    parser.add_argument(
        "--no-generate",
        action="store_true",
        help="reuse an existing converted batch; requires --batch-id",
    )
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--no-arrival-sleep", action="store_true")
    parser.add_argument("--zero-base-arrivals", action="store_true")
    parser.add_argument(
        "--wait",
        action="store_true",
        help="wait for all workflows to reach a terminal phase before exporting data",
    )
    parser.add_argument("--poll-seconds", type=float, default=10.0)
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="disable the automatic batch data export after a successful submission",
    )
    parser.add_argument(
        "--kubectl",
        default="",
        help='kubectl command or prefix, for example "docker exec k3s-server kubectl"',
    )
    return parser.parse_args()


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def resolve_input_manifest(args: argparse.Namespace) -> Path:
    if args.input_manifest:
        return Path(args.input_manifest).resolve()

    manifest_root = Path(args.manifest_root).resolve()
    if args.scenario:
        candidate = manifest_root / args.scenario / "workload_manifest.jsonl"
        if candidate.is_file():
            return candidate
        raise SystemExit(f"manifest not found for scenario {args.scenario!r}: {candidate}")

    candidates = sorted(manifest_root.glob("*/workload_manifest.jsonl"))
    if len(candidates) == 1:
        return candidates[0]
    if candidates:
        scenarios = ", ".join(path.parent.name for path in candidates)
        raise SystemExit(
            "--input-manifest or --scenario is required when multiple default F1 manifests exist "
            f"under {manifest_root}: {scenarios}"
        )
    raise SystemExit(f"--input-manifest is required; no default manifests found under {manifest_root}")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> str:
    text = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
    path.write_text(text, encoding="utf-8")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def rel_after_corpus_marker(raw_path: str) -> str:
    parts = Path(raw_path).parts
    if CORPUS_MARKER in parts:
        index = parts.index(CORPUS_MARKER)
        return Path(*parts[index + 1 :]).as_posix()
    if "workflow" in parts:
        index = parts.index("workflow")
        return Path(*parts[index + 1 :]).as_posix()
    raise ValueError(f"cannot map path without {CORPUS_MARKER!r} or 'workflow': {raw_path}")


def workflow_driver(workload_type: str, category: str) -> str:
    if workload_type == "pure":
        return "quantum"
    return "qaoa" if "qaoa" in category else "vqe"


def workflow_name_for(row: dict[str, Any], batch_id: str, scenario: str) -> str:
    digest = hashlib.sha1(
        f"{batch_id}:{row['logical_job_id']}:{row['submit_order']}:{row['qspec']}".encode("utf-8")
    ).hexdigest()[:8]
    prefix_kind = "hybrid" if row["workload_type"] == "hybrid" else "pq"
    base = k8s_name(
        f"f1-{scenario}-{prefix_kind}-{row['category']}-{row['requested_qubits']}",
        max_length=63,
    )
    suffix = f"{row['submit_order']:06d}-{digest}"
    prefix = base[: 63 - len(suffix) - 1].rstrip("-") or "f1"
    return f"{prefix}-{suffix}"


def resolve_kubectl(raw: str) -> list[str]:
    if raw:
        command = shlex.split(raw)
        if not command:
            raise ValueError("--kubectl must not be empty")
        return command
    bundled = PROJECT_ROOT / "kubectl"
    if bundled.exists() and bundled.is_file():
        return [str(bundled)]
    found = shutil.which("kubectl")
    if found:
        return [found]
    raise RuntimeError("kubectl not found; pass --kubectl or add it to PATH")


def hybrid_driver_path(workload_dir: Path) -> Path:
    qaoa = workload_dir / "qaoa_hybrid_driver.py"
    if qaoa.exists():
        return qaoa
    return workload_dir / "vqe_hybrid_driver.py"


def build_rows(
    input_rows: list[dict[str, Any]],
    workflow_root: Path,
    batch_id: str,
    scenario: str,
    out_dir: Path,
    on_missing: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    rows = sorted(input_rows, key=lambda row: (int(row.get("submit_order", 0)), str(row.get("logical_job_id", ""))))
    manifest_dir = out_dir / "workflow_manifests"
    mapped: list[dict[str, Any]] = []
    missing: list[str] = []

    for row in rows:
        qspec_rel = rel_after_corpus_marker(str(row["qspec"]))
        qworkdir_rel = str(Path(qspec_rel).parent)
        qworkdir = (workflow_root / qworkdir_rel).resolve()
        qspec = (workflow_root / qspec_rel).resolve()
        qubits = int(row["requested_qubits"])
        submit_script = qworkdir / f"submit_{qubits}.py"
        if row.get("workload_type") == "hybrid" and not submit_script.exists():
            submit_script = hybrid_driver_path(qworkdir)
        template = qworkdir / f"{qspec.stem}-workflow.yaml"
        parameters = qworkdir / f"parameters_{qubits}.json"
        row_errors = []
        if not qworkdir.is_dir():
            row_errors.append(f"mapped qworkdir not found: {qworkdir}")
        if not qspec.is_file():
            row_errors.append(f"mapped qspec not found: {qspec}")
        if not submit_script.is_file():
            row_errors.append(f"mapped binary/driver not found: {submit_script}")
        if row.get("workload_type") == "hybrid" and not parameters.is_file():
            row_errors.append(f"mapped hybrid parameters not found: {parameters}")
        if row_errors:
            message = f"{row.get('logical_job_id')} ({row.get('benchmark_name')}): " + "; ".join(row_errors)
            if on_missing == "fail":
                raise FileNotFoundError(message)
            missing.append(message)
            continue

        item = dict(row)
        original = {
            "qworkdir": row.get("qworkdir", ""),
            "qspec": row.get("qspec", ""),
            "binary": row.get("binary", ""),
        }
        item["qworkdir"] = str(qworkdir)
        item["qspec"] = str(qspec)
        item["binary"] = str(submit_script)
        item["family"] = "Qonductor-workflow"
        item["submission"] = "Qonductor HybridWorkflow"
        item["submit_mode"] = "hybrid_workflow"
        item["workflow_type"] = "hybrid" if item["workload_type"] == "hybrid" else "pure_quantum"
        item["workflow_dir"] = str(qworkdir)
        item["workflow_spec"] = str(qspec)
        item["workflow_parameters"] = str(parameters) if parameters.exists() else ""
        item["workflow_manifest_template"] = str(template) if template.exists() else ""
        item["driver"] = workflow_driver(str(item["workload_type"]), str(item["category"]))
        item["workflow_name"] = workflow_name_for(item, batch_id, scenario)
        item["workflow_manifest"] = str((manifest_dir / f"{item['workflow_name']}.yaml").resolve())
        metadata = dict(item.get("metadata", {}) or {})
        metadata["source"] = "workflow"
        metadata["original_qslurm_paths"] = original
        metadata["mapped_qspec_rel"] = qspec_rel
        metadata["workflow_template_exists"] = template.exists()
        item["metadata"] = metadata
        mapped.append(item)

    return mapped, missing


def register_images(rows: list[dict[str, Any]], registry_root: str, shots: int, max_iterations: int, priority: str, quantum_timeout_seconds: float) -> dict[str, Any]:
    images: dict[str, Any] = {}
    for row in rows:
        key = str(row["qspec"])
        if key in images:
            continue
        workload_dir = Path(row["qworkdir"])
        spec_file = Path(row["qspec"]).name
        image_name = f"f1_{row['workload_type']}_{row['category']}_{row['requested_qubits']}_{row['driver']}"
        if row["workload_type"] == "pure":
            spec, logical_id, qasm_path, qasm_text = load_spec_and_qasm(workload_dir, spec_file)
            image = build_pure_quantum_image(
                workflow_name=image_name,
                logical_id=logical_id,
                qasm_text=qasm_text,
                circuit_format=qasm_format(qasm_path, qasm_text),
                qubits=int(spec.get("qnode", row["requested_qubits"])),
                shots=shots,
                source={"workloadDir": str(workload_dir), "spec": str(workload_dir / spec_file)},
            )
        else:
            parameters_file = Path(row["workflow_parameters"]).name
            _, benchmark, driver_kind = hybrid_inputs(
                workload_dir=workload_dir,
                spec_file=spec_file,
                parameters_file=parameters_file,
                shots=shots,
                max_iterations=max_iterations,
                priority=priority,
                quantum_timeout_seconds=quantum_timeout_seconds,
            )
            image = build_hybrid_image(
                workflow_name=image_name,
                benchmark=benchmark,
                driver_kind=driver_kind,
                source={"workloadDir": str(workload_dir), "spec": str(workload_dir / spec_file)},
            )
        register_image(image, registry_root)
        images[key] = image
    return images


def build_workflow_manifest(row: dict[str, Any], image: Any, args: argparse.Namespace, batch_id: str, scenario: str) -> dict[str, Any]:
    workload_dir = Path(row["qworkdir"])
    spec_file = Path(row["qspec"]).name
    if row["workload_type"] == "pure":
        spec, logical_id, qasm_path, qasm_text = load_spec_and_qasm(workload_dir, spec_file)
        inputs = pure_quantum_inputs(
            logical_id=logical_id,
            qasm_path=qasm_path,
            qasm_text=qasm_text,
            circuit_format=qasm_format(qasm_path, qasm_text),
            qubits=int(spec.get("qnode", row["requested_qubits"])),
            shots=args.shots,
            priority=args.priority,
            source={"workloadDir": str(workload_dir), "spec": str(workload_dir / spec_file)},
        )
        algorithm = row["category"]
    else:
        inputs, algorithm, _ = hybrid_inputs(
            workload_dir=workload_dir,
            spec_file=spec_file,
            parameters_file=Path(row["workflow_parameters"]).name,
            shots=args.shots,
            max_iterations=args.max_iterations,
            priority=args.priority,
            quantum_timeout_seconds=args.quantum_timeout_seconds,
        )

    manifest = build_manifest(
        image,
        inputs,
        workflow_name=row["workflow_name"],
        priority=args.priority,
        algorithm=str(algorithm),
    )
    labels = manifest.setdefault("metadata", {}).setdefault("labels", {})
    labels.update(
        {
            "batch_id": k8s_name(batch_id),
            "generated_by": "submit-f1-manifest",
            "f1_scenario": k8s_name(scenario),
            "logical_job_id": k8s_name(str(row["logical_job_id"])),
            "workload_type": str(row["workload_type"]),
        }
    )
    annotations = manifest["metadata"].setdefault("annotations", {})
    original = row.get("metadata", {}).get("original_qslurm_paths", {})
    annotations.update(
        {
            "qonductor.io/arrival-time-ms": str(row["arrival_time_ms"]),
            "qonductor.io/submit-order": str(row["submit_order"]),
            "qonductor.io/f1-logical-job-id": str(row["logical_job_id"]),
            "qonductor.io/workflow-spec": str(row["qspec"]),
            "qonductor.io/qslurm-qspec": str(original.get("qspec", "")),
            "qonductor.io/qslurm-qworkdir": str(original.get("qworkdir", "")),
            "qonductor.io/qslurm-binary": str(original.get("binary", "")),
        }
    )
    return manifest


def emit_outputs(
    rows: list[dict[str, Any]],
    images: dict[str, Any],
    args: argparse.Namespace,
    out_dir: Path,
    batch_id: str,
    scenario: str,
    missing_rows: list[str],
) -> dict[str, Any]:
    manifest_dir = out_dir / "workflow_manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    for row in rows:
        image = images[str(row["qspec"])]
        row["workflow_image_ref"] = str(image.image_id)
        manifest = build_workflow_manifest(row, image, args, batch_id, scenario)
        Path(row["workflow_manifest"]).write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    trace_rows = [
        {
            "logical_job_id": row["logical_job_id"],
            "arrival_time_ms": int(row["arrival_time_ms"]),
            "submit_order": int(row["submit_order"]),
            "workflow_name": row["workflow_name"],
            "workflow_manifest": row["workflow_manifest"],
        }
        for row in rows
    ]
    manifest_sha = write_jsonl(out_dir / "workload_manifest.jsonl", rows)
    trace_sha = write_jsonl(out_dir / "workload_trace.jsonl", trace_rows)
    summary = {
        "schema_version": 1,
        "batch_id": batch_id,
        "scenario": scenario,
        "total_jobs": len(rows),
        "skipped_jobs": len(missing_rows),
        "missing_rows": missing_rows,
        "pure_job_count": sum(1 for row in rows if row["workload_type"] == "pure"),
        "hybrid_job_count": sum(1 for row in rows if row["workload_type"] == "hybrid"),
        "workflow_image_count": len(images),
        "benchmark_counts": dict(sorted(Counter(row["benchmark_name"] for row in rows).items())),
        "qubit_counts": dict(sorted(Counter(str(row["requested_qubits"]) for row in rows).items())),
        "arrival_min_ms": min((int(row["arrival_time_ms"]) for row in rows), default=0),
        "arrival_max_ms": max((int(row["arrival_time_ms"]) for row in rows), default=0),
        "priority": args.priority,
        "shots": args.shots,
        "max_iterations": args.max_iterations,
        "quantum_timeout_seconds": args.quantum_timeout_seconds,
        "manifest_sha256": manifest_sha,
        "trace_sha256": trace_sha,
        "workflow_manifest_dir": str(manifest_dir.resolve()),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def submit_rows(rows: list[dict[str, Any]], args: argparse.Namespace, out_dir: Path, batch_id: str) -> None:
    kubectl = resolve_kubectl(args.kubectl)
    log_path = out_dir / "submission_log.csv"
    start = time.time()
    base_arrival = int(rows[0]["arrival_time_ms"]) if args.zero_base_arrivals and rows else 0
    submitted = 0
    failed = 0
    with log_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["actual_submit_time", "arrival_time_ms", "submit_order", "logical_job_id", "workflow_name", "workflow_manifest", "status", "kubectl_output"])
        for index, row in enumerate(rows, start=1):
            arrival_ms = max(0, int(row["arrival_time_ms"]) - base_arrival)
            if not args.no_arrival_sleep:
                target = start + arrival_ms / 1000.0
                delay = target - time.time()
                if delay > 0.001:
                    time.sleep(delay)
            manifest_text = Path(row["workflow_manifest"]).read_text(encoding="utf-8")
            result = subprocess.run(
                [*kubectl, "apply", "-f", "-"],
                cwd=PROJECT_ROOT,
                input=manifest_text,
                capture_output=True,
                text=True,
                timeout=60,
            )
            now = (
                datetime.now(UTC)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
            output = (result.stdout or result.stderr or "").strip()
            if result.returncode == 0:
                submitted += 1
                status = "SUCCESS"
                print(f"[submit {index}/{len(rows)}] {row['workflow_name']} arrival={arrival_ms}ms", flush=True)
            else:
                failed += 1
                status = "FAILED_KUBECTL"
                print(f"[error {index}/{len(rows)}] {row['workflow_name']}: {output}", file=sys.stderr, flush=True)
            writer.writerow([now, arrival_ms, row["submit_order"], row["logical_job_id"], row["workflow_name"], row["workflow_manifest"], status, output])
    print(f"submitted={submitted} failed={failed} submission_log={log_path}")
    if failed:
        raise SystemExit(1)
    if args.wait:
        wait_for_completion(kubectl, batch_id, submitted, args.poll_seconds)
    if not args.no_export:
        export_batch_data(kubectl, batch_id, rows, out_dir)


def wait_for_completion(kubectl: list[str], batch_id: str, expected: int, poll_seconds: float) -> None:
    selector = f"batch_id={k8s_name(batch_id)}"
    while True:
        result = subprocess.run(
            [*kubectl, "get", "hybridworkflows", "-l", selector, "-o", "json"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            print(f"[wait] kubectl get failed: {result.stderr.strip()}", file=sys.stderr)
            time.sleep(poll_seconds)
            continue
        data = json.loads(result.stdout or '{"items":[]}')
        items = data.get("items", [])
        phases = Counter(item.get("status", {}).get("phase", "Pending") for item in items)
        running = sum(count for phase, count in phases.items() if phase not in TERMINAL_PHASES)
        print(f"[wait] visible={len(items)}/{expected} phases={dict(phases)}", flush=True)
        if len(items) >= expected and running == 0:
            return
        time.sleep(poll_seconds)


def kubectl_json(kubectl: list[str], resource: str, selector: str = "") -> dict[str, Any]:
    command = [*kubectl, "get", resource]
    if selector:
        command.extend(["-l", selector])
    command.extend(["-o", "json"])
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "unknown kubectl error").strip()
        raise RuntimeError(f"kubectl get {resource} failed: {detail}")
    return json.loads(result.stdout or '{"items": []}')


def parse_iso_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def quantum_job_metrics(item: dict[str, Any]) -> dict[str, Any]:
    metadata = item.get("metadata", {})
    spec = item.get("spec", {})
    status = item.get("status", {})
    arrived_at = str(metadata.get("creationTimestamp", ""))
    scheduled_at = str(status.get("scheduledAt", ""))
    arrived = parse_iso_timestamp(arrived_at)
    scheduled = parse_iso_timestamp(scheduled_at)
    waiting_time = (scheduled - arrived).total_seconds() if arrived and scheduled else None
    actual_execution = status.get("actualExecutionTime")
    estimated_execution = status.get("estimatedExecutionTime")
    execution_time = actual_execution if actual_execution is not None else estimated_execution
    actual_fidelity = status.get("actualFidelity")
    estimated_fidelity = status.get("estimatedFidelity")
    return {
        "job_name": metadata.get("name", ""),
        "workflow": spec.get("workflowRef", ""),
        "step_id": spec.get("stepId", ""),
        "qubits": spec.get("qubits", 0),
        "shots": spec.get("shots", 0),
        "priority": spec.get("priority", "balanced"),
        "phase": status.get("phase", ""),
        "assigned_qpu": status.get("assignedQPU", ""),
        "arrived_at": arrived_at,
        "scheduled_at": scheduled_at,
        "completed_at": "",
        "waiting_time_s": waiting_time,
        "execution_time_s": float(execution_time) if execution_time is not None else None,
        "estimated_fidelity": float(estimated_fidelity) if estimated_fidelity is not None else None,
        "actual_fidelity": float(actual_fidelity) if actual_fidelity is not None else None,
        "scheduling_metadata": status.get("schedulingMetadata"),
    }


def workflow_metrics(item: dict[str, Any]) -> dict[str, Any]:
    metadata = item.get("metadata", {})
    status = item.get("status", {})
    return {
        "workflow_name": metadata.get("name", ""),
        "phase": status.get("phase", ""),
        "total_steps": status.get("totalSteps", 0),
        "steps_completed": status.get("stepsCompleted", 0),
        "classical_step_count": status.get("classicalStepCount", 0),
        "quantum_step_count": status.get("quantumStepCount", 0),
        "average_fidelity": status.get("averageFidelity"),
        "total_waiting_time_s": status.get("totalWaitingTime"),
        "total_execution_time_s": status.get("totalExecutionTime"),
        "created_at": metadata.get("creationTimestamp", ""),
    }


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_quantum_job_csv(path: Path, metrics: list[dict[str, Any]]) -> None:
    columns = [
        "timestamp", "workflow", "step_id", "job_name", "qubits", "shots",
        "priority", "phase", "assigned_qpu", "arrived_at", "scheduled_at",
        "waiting_time_s", "execution_time_s", "estimated_fidelity", "actual_fidelity",
    ]
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(columns)
        for item in metrics:
            writer.writerow(
                [
                    item["arrived_at"], item["workflow"], item["step_id"], item["job_name"],
                    item["qubits"], item["shots"], item["priority"], item["phase"],
                    item["assigned_qpu"], item["arrived_at"], item["scheduled_at"],
                    item["waiting_time_s"], item["execution_time_s"],
                    item["estimated_fidelity"], item["actual_fidelity"],
                ]
            )


def write_e2e_csv(path: Path, metrics: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        writer.writerow(["timestamp", "fidelity", "JCT"])
        for item in metrics:
            fidelity = item["actual_fidelity"]
            if fidelity is None:
                fidelity = item["estimated_fidelity"]
            writer.writerow(
                [
                    item["arrived_at"],
                    round(float(fidelity or 0.0), 6),
                    round(float(item["execution_time_s"] or 0.0), 3),
                ]
            )


def export_batch_data(
    kubectl: list[str],
    batch_id: str,
    rows: list[dict[str, Any]],
    out_dir: Path,
) -> None:
    metrics_dir = out_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    selector = f"batch_id={k8s_name(batch_id)}"
    workflow_crs = kubectl_json(kubectl, "hybridworkflows", selector)
    all_quantum_job_crs = kubectl_json(kubectl, "quantumjobs")

    workflow_names = {str(row["workflow_name"]) for row in rows}
    workflow_items = sorted(
        (
            item
            for item in workflow_crs.get("items", [])
            if item.get("metadata", {}).get("name") in workflow_names
        ),
        key=lambda item: item.get("metadata", {}).get("name", ""),
    )
    workflow_crs["items"] = workflow_items
    quantum_job_items = sorted(
        (
            item
            for item in all_quantum_job_crs.get("items", [])
            if item.get("spec", {}).get("workflowRef") in workflow_names
        ),
        key=lambda item: item.get("metadata", {}).get("name", ""),
    )
    quantum_job_crs = {
        "apiVersion": all_quantum_job_crs.get("apiVersion", "v1"),
        "items": quantum_job_items,
        "kind": all_quantum_job_crs.get("kind", "List"),
        "metadata": all_quantum_job_crs.get("metadata", {}),
    }

    workflow_rows = [workflow_metrics(item) for item in workflow_items]
    quantum_job_rows = [quantum_job_metrics(item) for item in quantum_job_items]
    write_json(metrics_dir / "workflow_metrics.json", workflow_rows)
    write_json(metrics_dir / "quantum_job_metrics.json", quantum_job_rows)
    write_json(metrics_dir / "workflow_crs_raw.json", workflow_crs)
    write_json(metrics_dir / "quantum_job_crs_raw.json", quantum_job_crs)
    write_quantum_job_csv(metrics_dir / "quantum_job_metrics.csv", quantum_job_rows)
    write_e2e_csv(metrics_dir / "quantum_job_jct_fidelity.csv", quantum_job_rows)

    workflow_phases = Counter(item.get("phase") or "Pending" for item in workflow_rows)
    quantum_job_phases = Counter(item.get("phase") or "Pending" for item in quantum_job_rows)
    terminal_workflows = sum(
        count for phase, count in workflow_phases.items() if phase in TERMINAL_PHASES
    )
    export_summary = {
        "batch_id": batch_id,
        "exported_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "expected_workflows": len(rows),
        "exported_workflows": len(workflow_rows),
        "exported_quantum_jobs": len(quantum_job_rows),
        "workflow_phases": dict(sorted(workflow_phases.items())),
        "quantum_job_phases": dict(sorted(quantum_job_phases.items())),
        "all_workflows_terminal": len(workflow_rows) >= len(rows) and terminal_workflows == len(workflow_rows),
        "metrics_dir": str(metrics_dir.resolve()),
    }
    write_json(metrics_dir / "export_summary.json", export_summary)
    print(
        f"exported_workflows={len(workflow_rows)} "
        f"exported_quantum_jobs={len(quantum_job_rows)} metrics_dir={metrics_dir}",
        flush=True,
    )


def main() -> None:
    args = parse_args()
    output_root = Path(args.output_dir).resolve()

    if args.no_generate:
        if not args.batch_id:
            raise SystemExit("--no-generate requires --batch-id")
        batch_id = args.batch_id
        out_dir = output_root / batch_id
        output_manifest = out_dir / "workload_manifest.jsonl"
        summary_path = out_dir / "summary.json"
        if not output_manifest.is_file() or not summary_path.is_file():
            raise SystemExit(f"converted batch not found: {out_dir}")
        rows = load_jsonl(output_manifest)
        missing_manifests = [row["workflow_manifest"] for row in rows if not Path(row["workflow_manifest"]).is_file()]
        if missing_manifests:
            raise SystemExit(f"converted batch has {len(missing_manifests)} missing workflow manifest(s)")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        print(f"BATCH_ID={batch_id}")
        print(f"OUTPUT_DIR={out_dir}")
        print(f"OUTPUT_MANIFEST={output_manifest}")
        print(f"WORKFLOW_MANIFEST_DIR={summary['workflow_manifest_dir']}")
        print(f"TOTAL_JOBS={len(rows)}")
        if args.submit:
            submit_rows(rows, args, out_dir, batch_id)
        else:
            print("Reused existing batch; pass --submit to apply the workflows to the cluster.")
        return

    input_manifest = resolve_input_manifest(args)
    workflow_root = Path(args.workflow_root).resolve()
    scenario = args.scenario or input_manifest.parent.name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    batch_id = args.batch_id or k8s_name(f"f1-{scenario}-{timestamp}")
    out_dir = output_root / batch_id
    out_dir.mkdir(parents=True, exist_ok=True)

    input_rows = load_jsonl(input_manifest)
    if args.limit:
        input_rows = input_rows[: args.limit]
    rows, missing_rows = build_rows(input_rows, workflow_root, batch_id, scenario, out_dir, args.on_missing)
    if not rows:
        raise SystemExit("No rows were mapped to local workflow paths.")
    for message in missing_rows[:10]:
        print(f"[skip] {message}", file=sys.stderr)
    if len(missing_rows) > 10:
        print(f"[skip] ... {len(missing_rows) - 10} more skipped row(s)", file=sys.stderr)
    images = register_images(rows, args.registry_root, args.shots, args.max_iterations, args.priority, args.quantum_timeout_seconds)
    summary = emit_outputs(rows, images, args, out_dir, batch_id, scenario, missing_rows)
    print(f"BATCH_ID={batch_id}")
    print(f"OUTPUT_DIR={out_dir}")
    print(f"OUTPUT_MANIFEST={out_dir / 'workload_manifest.jsonl'}")
    print(f"OUTPUT_TRACE={out_dir / 'workload_trace.jsonl'}")
    print(f"WORKFLOW_MANIFEST_DIR={summary['workflow_manifest_dir']}")
    print(f"TOTAL_JOBS={summary['total_jobs']}")
    print(f"WORKFLOW_IMAGE_COUNT={summary['workflow_image_count']}")
    if args.submit:
        submit_rows(rows, args, out_dir, batch_id)
    else:
        print("Generated only; pass --submit to apply the workflows to the cluster.")


if __name__ == "__main__":
    main()
