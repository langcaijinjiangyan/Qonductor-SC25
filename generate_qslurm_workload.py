#!/usr/bin/env python3
"""Generate reusable qslurm workload manifests from the copied corpus."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
from collections import Counter, defaultdict
from pathlib import Path


GENERATOR_VERSION = "1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus-root", required=True)
    parser.add_argument("--corpus-index", default="")
    parser.add_argument("--output-manifest", required=True)
    parser.add_argument("--output-trace", required=True)
    parser.add_argument("--output-summary", required=True)
    parser.add_argument("--total-jobs", type=int, default=200)
    parser.add_argument("--submit-window-sec", type=float, default=300.0)
    parser.add_argument("--hybrid-job-ratio", type=float, default=0.2)
    parser.add_argument(
        "--hybrid-subdir",
        default="hybrid",
        help="Hybrid corpus subdirectory to sample from, e.g. hybrid or hybrid_6q.",
    )
    parser.add_argument("--pure-size-ratio", default="4,3,2")
    parser.add_argument("--small-qubits", default="2,4")
    parser.add_argument("--medium-qubits", default="8,12")
    parser.add_argument("--large-qubits", default="16")
    parser.add_argument("--arrival-mode", choices=["deterministic", "poisson"], default="poisson")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--allow-hybrid-legacy", default="true", choices=["true", "false"])
    parser.add_argument("--runtime-model", default="qasm_gate_proxy_v1")
    parser.add_argument("--base-ms", type=float, default=1.0)
    parser.add_argument("--gate-weight", type=float, default=0.01)
    parser.add_argument("--two-qubit-weight", type=float, default=0.05)
    parser.add_argument("--qubit-weight", type=float, default=0.1)
    parser.add_argument("--hybrid-iteration-factor", type=float, default=5.0)
    parser.add_argument("--parameter-weight", type=float, default=0.02)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_csv_ints(raw: str) -> set[int]:
    return {int(item.strip()) for item in raw.split(",") if item.strip()}


def parse_ratio(raw: str) -> tuple[int, int, int]:
    values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    if len(values) != 3:
        raise SystemExit("pure-size-ratio must have exactly three integers")
    return values[0], values[1], values[2]


def load_index(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def stable_entry_key(item: dict) -> tuple:
    return (
        item.get("workload_type", ""),
        item.get("category", ""),
        item.get("benchmark_name", ""),
        int(item.get("requested_qubits", 0)),
        item.get("qspec_rel", item.get("qspec", "")),
        item.get("binary_rel", item.get("binary", "")),
    )


def assign_counts(total: int, weights: list[int]) -> list[int]:
    if total <= 0:
        return [0 for _ in weights]
    total_weight = sum(weights)
    raw = [total * weight / total_weight for weight in weights]
    base = [math.floor(value) for value in raw]
    remainder = total - sum(base)
    fractions = sorted(
        ((raw[index] - base[index], index) for index in range(len(weights))),
        reverse=True,
    )
    for _, index in fractions[:remainder]:
        base[index] += 1
    return base


def count_qasm_metrics(path: Path) -> dict:
    metrics = {
        "gate_count": 0,
        "two_qubit_gate_count": 0,
        "qasm_line_count": 0,
        "measurement_count": 0,
        "qasm_format": "qasm3" if path.suffix == ".qasm3" else "qasm",
    }
    two_qubit_gate_names = {
        "cx", "cz", "swap", "cp", "crx", "cry", "crz", "rxx", "ryy", "rzz",
        "cu", "cu1", "cu3", "ecr", "iswap", "ccx",
    }
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return metrics
    metrics["qasm_line_count"] = len(lines)
    gate_line = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)")
    qubit_token = re.compile(r"(q|qubit)[^;\[]*\[[^\]]+\]")
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("//") or line.startswith("#"):
            continue
        lowered = line.lower()
        if lowered.startswith(("openqasm", "include", "gate ", "def ", "pragma", "creg ", "qreg ", "qubit ", "bit ")):
            continue
        if "measure" in lowered:
            metrics["measurement_count"] += 1
        match = gate_line.match(line)
        if not match:
            continue
        opname = match.group(1).lower()
        if opname in {"measure", "barrier", "reset"}:
            continue
        metrics["gate_count"] += 1
        qargs = qubit_token.findall(line)
        if opname in two_qubit_gate_names or len(qargs) >= 2:
            metrics["two_qubit_gate_count"] += 1
    return metrics


def estimate_runtime(entry: dict, corpus_root: Path, args: argparse.Namespace) -> tuple[float, str, dict]:
    circuit_files = entry.get("circuit_files", [])
    qworkdir = corpus_root / entry["qworkdir_rel"]
    qasm_metrics = {
        "gate_count": 0,
        "two_qubit_gate_count": 0,
        "qasm_line_count": 0,
        "measurement_count": 0,
        "qasm_format": entry.get("qasm_format", "qasm"),
        "parameter_count": 0,
    }
    method = args.runtime_model
    parse_success = False

    if circuit_files:
        qasm_path = qworkdir / circuit_files[0]
        if qasm_path.exists():
            qasm_metrics.update(count_qasm_metrics(qasm_path))
            parse_success = True

    parameter_count = 0
    parameter_file = entry.get("parameter_file")
    if parameter_file:
        parameter_path = qworkdir / parameter_file
        if parameter_path.exists():
            try:
                parameter_data = json.loads(parameter_path.read_text(encoding="utf-8"))
                parameter_count = int(parameter_data.get("parameter_count", 0))
            except (OSError, ValueError, json.JSONDecodeError):
                parameter_count = 0
    qasm_metrics["parameter_count"] = parameter_count

    if not parse_success:
        method = "qubit_proxy_v1"
        predicted = args.base_ms + args.qubit_weight * int(entry["requested_qubits"])
        if entry["workload_type"] == "hybrid":
            predicted = predicted * args.hybrid_iteration_factor + args.parameter_weight * parameter_count
        return predicted, method, qasm_metrics

    pure_proxy = (
        args.base_ms
        + args.gate_weight * qasm_metrics["gate_count"]
        + args.two_qubit_weight * qasm_metrics["two_qubit_gate_count"]
        + args.qubit_weight * int(entry["requested_qubits"])
    )
    if entry["workload_type"] == "hybrid":
        predicted = pure_proxy * args.hybrid_iteration_factor + args.parameter_weight * parameter_count
    else:
        predicted = pure_proxy
    return predicted, method, qasm_metrics


def build_round_robin_selector(entries: list[dict], rng: random.Random):
    by_category: dict[str, list[dict]] = defaultdict(list)
    for entry in entries:
        by_category[entry["category"]].append(entry)
    categories = sorted(by_category)
    if not categories:
        return None, []
    for values in by_category.values():
        values.sort(key=stable_entry_key)
        rng.shuffle(values)
    positions = {category: 0 for category in categories}
    category_index = 0

    def choose() -> dict:
        nonlocal category_index
        category = categories[category_index % len(categories)]
        category_index += 1
        values = by_category[category]
        position = positions[category] % len(values)
        positions[category] += 1
        return values[position]

    return choose, categories


def generate_arrivals(total_jobs: int, submit_window_sec: float, arrival_mode: str, rng: random.Random) -> tuple[list[int], str]:
    window_ms = max(int(round(submit_window_sec * 1000)), 1)
    if total_jobs <= 0:
        return [], ""
    if arrival_mode == "deterministic":
        if total_jobs == 1:
            return [0], ""
        step = window_ms / max(total_jobs - 1, 1)
        arrivals = [int(round(index * step)) for index in range(total_jobs)]
        return arrivals, ""

    rate = total_jobs / max(submit_window_sec, 1e-9)
    raw_times = []
    current = 0.0
    for _ in range(total_jobs):
        current += rng.expovariate(rate)
        raw_times.append(current)
    max_time = raw_times[-1] if raw_times else 0.0
    if max_time <= 0:
        arrivals = [0 for _ in range(total_jobs)]
    else:
        arrivals = [int(round((value / max_time) * window_ms)) for value in raw_times]
    return arrivals, "poisson arrivals normalized to fit submit_window_sec"


def jsonl_text(rows: list[dict]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)


def json_text(payload: dict) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> None:
    args = parse_args()
    corpus_root = Path(args.corpus_root).resolve()
    corpus_index = Path(args.corpus_index).resolve() if args.corpus_index else corpus_root / "corpus_index.json"
    index = load_index(corpus_index)
    all_entries = sorted(index["entries"], key=stable_entry_key)
    rng = random.Random(args.seed)

    small_qubits = parse_csv_ints(args.small_qubits)
    medium_qubits = parse_csv_ints(args.medium_qubits)
    large_qubits = parse_csv_ints(args.large_qubits)
    pure_weights = list(parse_ratio(args.pure_size_ratio))
    pure_entries = [entry for entry in all_entries if entry["workload_type"] == "pure"]
    hybrid_subdir = args.hybrid_subdir.strip().strip("/")
    hybrid_prefix = f"{hybrid_subdir}/" if hybrid_subdir else ""
    hybrid_entries = [
        entry
        for entry in all_entries
        if entry["workload_type"] == "hybrid"
        and (not hybrid_prefix or str(entry.get("qworkdir_rel", "")).startswith(hybrid_prefix))
    ]

    hybrid_jobs = round(args.total_jobs * args.hybrid_job_ratio)
    pure_jobs = args.total_jobs - hybrid_jobs

    bucket_candidates = {
        "small": [entry for entry in pure_entries if int(entry["requested_qubits"]) in small_qubits],
        "medium": [entry for entry in pure_entries if int(entry["requested_qubits"]) in medium_qubits],
        "large": [entry for entry in pure_entries if int(entry["requested_qubits"]) in large_qubits],
    }
    warnings: list[str] = []

    target_counts = dict(zip(["small", "medium", "large"], assign_counts(pure_jobs, pure_weights)))
    if pure_jobs > 0:
        available_buckets = [name for name, values in bucket_candidates.items() if values]
        missing_buckets = [name for name, values in bucket_candidates.items() if not values and target_counts[name] > 0]
        if missing_buckets:
            warnings.append(f"missing pure bucket candidates: {','.join(missing_buckets)}")
            redistribute = sum(target_counts[name] for name in missing_buckets)
            for name in missing_buckets:
                target_counts[name] = 0
            if available_buckets:
                extra = assign_counts(redistribute, [1 for _ in available_buckets])
                for name, count in zip(available_buckets, extra):
                    target_counts[name] += count

    pure_selectors = {}
    pure_category_lists = {}
    for name, entries in bucket_candidates.items():
        selector, categories = build_round_robin_selector(entries, rng)
        pure_selectors[name] = selector
        pure_category_lists[name] = categories

    hybrid_categories = sorted({entry["category"] for entry in hybrid_entries})
    hybrid_category_entries = {
        category: sorted(
            [entry for entry in hybrid_entries if entry["category"] == category],
            key=stable_entry_key,
        )
        for category in hybrid_categories
    }
    for values in hybrid_category_entries.values():
        rng.shuffle(values)

    hybrid_target_counts = dict(zip(hybrid_categories, assign_counts(hybrid_jobs, [1 for _ in hybrid_categories]))) if hybrid_categories else {}
    if hybrid_jobs > 0 and not hybrid_categories:
        warnings.append("no hybrid candidates available")

    manifest_entries = []
    runtime_method_counts = Counter()
    pure_category_counts = Counter()
    hybrid_category_counts = Counter()
    pure_size_bucket_counts = Counter()
    def materialize(entry: dict, submit_order: int) -> dict:
        predicted, runtime_method, metrics = estimate_runtime(entry, corpus_root, args)
        runtime_method_counts[runtime_method] += 1
        if entry["workload_type"] == "pure":
            pure_size_bucket_counts[entry["size_bucket"]] += 1
            pure_category_counts[entry["category"]] += 1
        else:
            hybrid_category_counts[entry["category"]] += 1

        qworkdir = str((corpus_root / entry["qworkdir_rel"]).resolve())
        qspec = str((corpus_root / entry["qspec_rel"]).resolve())
        binary = str((corpus_root / entry["binary_rel"]).resolve()) if entry.get("binary_rel") else ""

        return {
            "schema_version": 1,
            "logical_job_id": f"job_{submit_order:06d}",
            "submit_order": submit_order,
            "workload_type": entry["workload_type"],
            "benchmark_name": entry["benchmark_name"],
            "category": entry["category"],
            "family": "MQTBench-2-20-copied",
            "qworkdir": qworkdir,
            "qspec": qspec,
            "binary": binary,
            "requested_qubits": int(entry["requested_qubits"]),
            "hybrid_job": bool(entry["hybrid_job"]),
            "predicted_qpu_work_ms": round(predicted, 6),
            "runtime_estimation_method": runtime_method,
            "arrival_time_ms": 0,
            "submit_mode": "sbatch_quantum_job",
            "slurm_partition": "hybrid" if entry["workload_type"] == "hybrid" else "q_compute",
            "nodes": "2-2" if entry["workload_type"] == "hybrid" else "1-1",
            "ntasks": 2 if entry["workload_type"] == "hybrid" else 1,
            "ntasks_per_node": 1,
            # Keep H2 hybrid launch aligned with the already-validated R1 path.
            "mpi_mode": "pmi2" if entry["workload_type"] == "hybrid" else "none",
            "size_bucket": entry["size_bucket"],
            "metadata": {
                "source": "copied_mqtbench2_20",
                "spec_version": int(entry.get("spec_version", 0)),
                "circuit_files": list(entry.get("circuit_files", [])),
                "gate_count": int(metrics["gate_count"]),
                "two_qubit_gate_count": int(metrics["two_qubit_gate_count"]),
                "qasm_line_count": int(metrics["qasm_line_count"]),
                "measurement_count": int(metrics["measurement_count"]),
                "parameter_count": int(metrics["parameter_count"]),
                "binary_exists": bool(entry.get("binary_exists", False)),
            },
        }

    submit_order = 1
    for bucket_name in ["small", "medium", "large"]:
        selector = pure_selectors[bucket_name]
        count = target_counts[bucket_name]
        if count > 0 and selector is None:
            warnings.append(f"no candidates for pure bucket {bucket_name}")
            continue
        for _ in range(count):
            manifest_entries.append(materialize(selector(), submit_order))
            submit_order += 1

    hybrid_positions = {category: 0 for category in hybrid_categories}
    for category in hybrid_categories:
        values = hybrid_category_entries[category]
        if not values and hybrid_target_counts.get(category, 0) > 0:
            warnings.append(f"no candidates for hybrid category {category}")
            continue
        for _ in range(hybrid_target_counts.get(category, 0)):
            if not values:
                continue
            position = hybrid_positions[category] % len(values)
            hybrid_positions[category] += 1
            manifest_entries.append(materialize(values[position], submit_order))
            submit_order += 1

    manifest_entries.sort(key=lambda item: item["submit_order"])
    arrivals, arrival_note = generate_arrivals(len(manifest_entries), args.submit_window_sec, args.arrival_mode, rng)
    for item, arrival in zip(manifest_entries, arrivals):
        item["arrival_time_ms"] = arrival
    manifest_entries.sort(key=lambda item: (item["arrival_time_ms"], item["submit_order"], item["logical_job_id"]))
    for index, item in enumerate(manifest_entries, start=1):
        item["submit_order"] = index
        item["logical_job_id"] = f"job_{index:06d}"

    trace_entries = [
        {
            "logical_job_id": item["logical_job_id"],
            "arrival_time_ms": item["arrival_time_ms"],
            "submit_order": item["submit_order"],
        }
        for item in manifest_entries
    ]
    trace_entries.sort(key=lambda item: (item["arrival_time_ms"], item["submit_order"], item["logical_job_id"]))

    total_predicted_qpu_work_ms = sum(item["predicted_qpu_work_ms"] for item in manifest_entries)
    pure_count_actual = sum(1 for item in manifest_entries if item["workload_type"] == "pure")
    hybrid_count_actual = sum(1 for item in manifest_entries if item["workload_type"] == "hybrid")
    summary = {
        "schema_version": 1,
        "total_jobs": len(manifest_entries),
        "submit_window_sec": args.submit_window_sec,
        "submission_rate_jobs_per_sec": (len(manifest_entries) / args.submit_window_sec) if args.submit_window_sec else 0.0,
        "arrival_mode": args.arrival_mode,
        "seed": args.seed,
        "pure_job_count": pure_count_actual,
        "hybrid_job_count": hybrid_count_actual,
        "pure_job_ratio": (pure_count_actual / len(manifest_entries)) if manifest_entries else 0.0,
        "hybrid_job_ratio": (hybrid_count_actual / len(manifest_entries)) if manifest_entries else 0.0,
        "size_bucket_counts": {name: int(pure_size_bucket_counts.get(name, 0)) for name in ["small", "medium", "large"]},
        "size_bucket_ratios": {
            name: ((pure_size_bucket_counts.get(name, 0) / pure_count_actual) if pure_count_actual else 0.0)
            for name in ["small", "medium", "large"]
        },
        "pure_category_counts": dict(sorted(pure_category_counts.items())),
        "hybrid_category_counts": dict(sorted(hybrid_category_counts.items())),
        "hybrid_subdir": hybrid_subdir,
        "excluded_hybrid_categories": [],
        "missing_specs": [],
        "runtime_estimation_method_counts": dict(sorted(runtime_method_counts.items())),
        "estimated_total_qpu_work_ms": round(total_predicted_qpu_work_ms, 6),
        "warnings": warnings + ([arrival_note] if arrival_note else []),
        "generator_version": GENERATOR_VERSION,
        "corpus_index_path": str(corpus_index),
        "manifest_sha256": "",
        "trace_sha256": "",
    }

    manifest_text = jsonl_text(manifest_entries)
    trace_text = jsonl_text(trace_entries)
    summary["manifest_sha256"] = sha256_text(manifest_text)
    summary["trace_sha256"] = sha256_text(trace_text)
    summary_text = json_text(summary)

    print("Generated qslurm workload plan:")
    print(f"  corpus_root={corpus_root}")
    print(f"  corpus_index={corpus_index}")
    print(f"  total_jobs={len(manifest_entries)}")
    print(f"  submit_window_sec={args.submit_window_sec}")
    print(f"  submission_rate_jobs_per_sec={summary['submission_rate_jobs_per_sec']:.6f}")
    print(f"  pure_job_count={pure_count_actual}")
    print(f"  hybrid_job_count={hybrid_count_actual}")
    print(f"  estimated_total_qpu_work_ms={summary['estimated_total_qpu_work_ms']}")
    print(f"  manifest_sha256={summary['manifest_sha256']}")
    print(f"  trace_sha256={summary['trace_sha256']}")
    if summary["warnings"]:
        print("  warnings=" + " | ".join(summary["warnings"]))

    if args.dry_run:
        print(summary_text, end="")
        return

    for path in [Path(args.output_manifest), Path(args.output_trace), Path(args.output_summary)]:
        path.parent.mkdir(parents=True, exist_ok=True)

    Path(args.output_manifest).write_text(manifest_text, encoding="utf-8")
    Path(args.output_trace).write_text(trace_text, encoding="utf-8")
    Path(args.output_summary).write_text(summary_text, encoding="utf-8")


if __name__ == "__main__":
    main()
