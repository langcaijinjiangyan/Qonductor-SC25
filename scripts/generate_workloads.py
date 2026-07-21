#!/usr/bin/env python3
"""Generate reusable Qonductor HybridWorkflow workload plans from workflow/."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import yaml

# Ensure project root is on sys.path so src.* imports resolve regardless
# of how the script is invoked (e.g. python3 scripts/generate_workloads.py).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.workflow.dag_engine import DAGNode, HybridDAG, StepType
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry

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
    parser.add_argument("--registry-root", default="data/workflow_registry",
                        help="Filesystem path for WorkflowRegistry storage")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_csv_ints(raw: str) -> set[int]:
    return {int(item.strip()) for item in raw.split(",") if item.strip()}


def parse_ratio(raw: str) -> tuple[int, int, int]:
    values = [int(item.strip()) for item in raw.split(",") if item.strip()]
    if len(values) != 3:
        raise SystemExit("pure-size-ratio must have exactly three integers")
    return values[0], values[1], values[2]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_index(path: Path) -> dict[str, Any]:
    return load_json(path)


def stable_entry_key(item: dict) -> tuple:
    return (
        item.get("workload_type", ""),
        item.get("category", ""),
        item.get("benchmark_name", ""),
        int(item.get("requested_qubits", 0)),
        item.get("qspec_rel", item.get("qspec", "")),
        item.get("workflow_manifest_rel", ""),
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


def k8s_name(value: str, max_length: int = 63) -> str:
    name = re.sub(r"[^a-z0-9-]+", "-", value.lower().replace("_", "-"))
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:max_length].rstrip("-") or "workflow"


def size_bucket_for_qubits(qubits: int) -> str:
    if qubits <= 4:
        return "small"
    if qubits <= 12:
        return "medium"
    return "large"


def workflow_driver(workload_type: str, benchmark: str) -> str:
    if workload_type == "pure":
        return "quantum"
    return "qaoa" if benchmark == "qaoa" else "vqe"


def _read_template_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "workflow_kind": "HybridWorkflow",
            "workflow_api_version": "qonductor.io/v1",
            "workflow_image_ref": "",
            "priority": "balanced",
            "scheduling": {},
        }
    manifest = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    spec = manifest.get("spec", {}) if isinstance(manifest, dict) else {}
    return {
        "workflow_kind": manifest.get("kind", "HybridWorkflow"),
        "workflow_api_version": manifest.get("apiVersion", "qonductor.io/v1"),
        "workflow_image_ref": str(spec.get("workflowImageRef", "")),
        "priority": spec.get("priority", "balanced"),
        "scheduling": dict(spec.get("scheduling", {}) or {}),
    }


def _spec_qubits(spec: dict[str, Any], spec_path: Path) -> int:
    value = spec.get("qnode")
    if value not in (None, ""):
        return int(value)
    match = re.search(r"spec_(\d+)\.json$", spec_path.name)
    if match:
        return int(match.group(1))
    return 0


def _entry_from_spec(corpus_root: Path, spec_path: Path) -> dict[str, Any] | None:
    if spec_path.name.endswith("-workflow.yaml"):
        return None
    workload_dir = spec_path.parent
    spec = load_json(spec_path)
    circuits = spec.get("circuits", {})
    if not circuits:
        return None

    qubits = _spec_qubits(spec, spec_path)
    workload_rel = workload_dir.relative_to(corpus_root).as_posix()
    spec_rel = spec_path.relative_to(corpus_root).as_posix()
    workflow_manifest_path = workload_dir / f"{spec_path.stem}-workflow.yaml"
    if not workflow_manifest_path.exists():
        return None

    parameters_path = workload_dir / f"parameters_{qubits}.json"
    params = load_json(parameters_path) if parameters_path.exists() else {}
    hybrid_job = bool(spec.get("hybrid_job", parameters_path.exists()))
    workload_type = "hybrid" if hybrid_job else "pure"
    benchmark = params.get("benchmark") or workload_dir.name
    corpus_meta_path = workload_dir / "corpus_meta.json"
    if corpus_meta_path.exists() and not params:
        try:
            corpus_meta = load_json(corpus_meta_path)
            benchmark = corpus_meta.get("benchmark_name", benchmark)
        except (OSError, ValueError, json.JSONDecodeError):
            pass

    circuit_files = list(circuits.values())
    qasm_format = "qasm"
    if circuit_files and Path(circuit_files[0]).suffix == ".qasm3":
        qasm_format = "qasm3"

    return {
        "workload_type": workload_type,
        "benchmark_name": benchmark,
        "category": workload_dir.name,
        "qworkdir_rel": workload_rel,
        "qspec_rel": spec_rel,
        "binary_rel": "",
        "binary_exists": False,
        "requested_qubits": qubits,
        "hybrid_job": hybrid_job,
        "size_bucket": size_bucket_for_qubits(qubits),
        "spec_version": int(spec.get("version", 0) or 0),
        "circuit_files": circuit_files,
        "parameter_file": parameters_path.name if parameters_path.exists() else None,
        "qasm_format": qasm_format,
        "workflow_manifest_rel": workflow_manifest_path.relative_to(corpus_root).as_posix(),
        "submit_script_rel": (workload_dir / f"submit_{qubits}.py").relative_to(corpus_root).as_posix(),
        "driver": workflow_driver(workload_type, str(benchmark)),
    }


def discover_workflow_index(corpus_root: Path) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for spec_path in sorted(corpus_root.rglob("spec_*.json")):
        if "_lib" in spec_path.parts:
            continue
        entry = _entry_from_spec(corpus_root, spec_path)
        if entry is not None:
            entries.append(entry)
    return {"schema_version": 1, "source": "workflow", "entries": entries}


def normalize_workflow_entries(index: dict[str, Any], corpus_root: Path) -> dict[str, Any]:
    entries = []
    for entry in index.get("entries", []):
        normalized = dict(entry)
        qworkdir = corpus_root / normalized["qworkdir_rel"]
        qspec = corpus_root / normalized["qspec_rel"]
        if "workflow_manifest_rel" not in normalized:
            normalized["workflow_manifest_rel"] = (
                qworkdir / f"{Path(normalized['qspec_rel']).stem}-workflow.yaml"
            ).relative_to(corpus_root).as_posix()
        if "parameter_file" not in normalized:
            qubits = int(normalized.get("requested_qubits", 0))
            parameter_path = qworkdir / f"parameters_{qubits}.json"
            normalized["parameter_file"] = parameter_path.name if parameter_path.exists() else None
        if "driver" not in normalized:
            benchmark = normalized.get("benchmark_name", "")
            normalized["driver"] = workflow_driver(normalized["workload_type"], benchmark)
        if "size_bucket" not in normalized:
            normalized["size_bucket"] = size_bucket_for_qubits(int(normalized["requested_qubits"]))
        if (corpus_root / normalized["workflow_manifest_rel"]).exists() and qspec.exists():
            entries.append(normalized)
    return {**index, "entries": entries}


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


def workflow_name_for(item: dict, args: argparse.Namespace) -> str:
    digest = hashlib.sha1(
        f"{args.seed}:{item['submit_order']}:{item['arrival_time_ms']}:{item['workflow_manifest_template']}".encode("utf-8")
    ).hexdigest()[:8]
    workflow_prefix = "hybrid" if item["workload_type"] == "hybrid" else "pq"
    base = k8s_name(f"{workflow_prefix}-{item['category']}-{item['requested_qubits']}")
    suffix = f"j{item['submit_order']:06d}-{digest}"
    prefix_len = 63 - len(suffix) - 1
    prefix = base[:prefix_len].rstrip("-") or "workflow"
    return f"{prefix}-{suffix}"


def workflow_manifest_dir(output_manifest: Path) -> Path:
    return output_manifest.parent / f"{output_manifest.stem}_workflow_manifests"


def assign_workflow_identity(items: list[dict], args: argparse.Namespace) -> None:
    manifest_dir = workflow_manifest_dir(Path(args.output_manifest))
    for item in items:
        workflow_name = workflow_name_for(item, args)
        item["workflow_name"] = workflow_name
        item["workflow_manifest"] = str((manifest_dir / f"{workflow_name}.yaml").resolve())


def _register_workflow_images(
    entries: list[dict],
    registry_root: str,
) -> dict[tuple[str, str, str], str]:
    """Build and register one WorkflowImage per unique (workload_type, category, driver).

    Returns a mapping ``(workload_type, category, driver) -> image_id`` so that
    every output manifest gets a valid, freshly-registered ``workflowImageRef``.
    """
    registry = WorkflowRegistry(root_dir=registry_root)
    image_map: dict[tuple[str, str, str], str] = {}

    for entry in entries:
        wl_type = entry["workload_type"]
        category = entry["category"]
        driver = entry.get("driver", "")

        key = (wl_type, category, driver)
        if key in image_map:
            continue

        image_name = re.sub(r"[^a-zA-Z0-9_]", "_", f"{wl_type}_{category}_{driver}")

        if wl_type == "pure":
            dag = HybridDAG(name=image_name)
            dag.add_node(DAGNode(
                step_id="quantum_exec",
                step_type=StepType.QUANTUM,
                label="quantum_exec",
                code="# circuit payload from QuantumJob CR metadata\n",
                resource_requirements={"qubits": 1},
            ))
            image = WorkflowImage(
                name=image_name,
                dag=dag,
                quantum_code="",
                classical_code="",
                config={
                    "spec": {
                        "priority": "balanced",
                        "scheduling": {"classicalPolicy": "FilterScore", "quantumPolicy": "NSGA2"},
                        "errorMitigation": {"enabled": False, "stackedTechniques": []},
                    }
                },
                status=WorkflowStatus.DEPLOYED,
            )
        else:
            if driver == "qaoa":
                step_id = "qaoa_spsa_driver"
                code = "from src.workflow.qaoa_runtime import run_qaoa_spsa_driver\nrun_qaoa_spsa_driver()\n"
                container_name = "qaoa-spsa-driver"
            else:
                step_id = "vqe_spsa_driver"
                code = "from src.workflow.vqe_runtime import run_vqe_spsa_driver\nrun_vqe_spsa_driver()\n"
                container_name = "vqe-spsa-driver"
            dag = HybridDAG(name=image_name)
            dag.add_node(DAGNode(
                step_id=step_id,
                step_type=StepType.CLASSICAL,
                label=step_id,
                code=code,
                resource_requirements={"cpu": 1, "memory": "1Gi"},
                metadata={"dynamic_quantum_jobs": True},
            ))
            image = WorkflowImage(
                name=image_name,
                dag=dag,
                quantum_code="",
                classical_code=code,
                config={
                    "spec": {
                        "priority": "balanced",
                        "containers": [{
                            "name": container_name,
                            "image": "qonductor-operator:latest",
                            "resources": {"limits": {"cpu": "1", "memory": "1Gi"}},
                        }],
                        "scheduling": {"classicalPolicy": "FilterScore", "quantumPolicy": "NSGA2"},
                        "errorMitigation": {"enabled": False, "stackedTechniques": []},
                    }
                },
                status=WorkflowStatus.DEPLOYED,
            )

        registry.register(image)
        image_map[key] = image.image_id

    return image_map


def write_workflow_manifests(items: list[dict], corpus_root: Path, image_id_map: dict[tuple[str, str, str], str] | None = None) -> None:
    for item in items:
        template_path = Path(item["workflow_manifest_template"])
        manifest = yaml.safe_load(template_path.read_text(encoding="utf-8")) or {}
        manifest.setdefault("metadata", {})
        manifest["metadata"]["name"] = item["workflow_name"]
        manifest["metadata"].setdefault("namespace", "default")
        labels = manifest["metadata"].setdefault("labels", {})
        labels.update(
            {
                "app": "qonductor",
                "converted_from": "workflow",
                "generated_by": "generate_workloads",
                "logical_job_id": item["logical_job_id"],
                "workload_type": item["workload_type"],
            }
        )
        annotations = manifest["metadata"].setdefault("annotations", {})
        annotations.update(
            {
                "qonductor.io/source-template": str(template_path),
                "qonductor.io/arrival-time-ms": str(item["arrival_time_ms"]),
                "qonductor.io/submit-order": str(item["submit_order"]),
                "qonductor.io/workflow-spec": item["workflow_spec"],
            }
        )
        if item.get("workflow_parameters"):
            annotations["qonductor.io/workflow-parameters"] = item["workflow_parameters"]

        # Overwrite workflowImageRef with freshly registered image id
        if image_id_map:
            driver = item.get("driver", "")
            key = (item["workload_type"], item["category"], driver)
            registered_id = image_id_map.get(key)
            if registered_id:
                manifest.setdefault("spec", {})["workflowImageRef"] = registered_id

        output_path = Path(item["workflow_manifest"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


def main() -> None:
    args = parse_args()
    corpus_root = Path(args.corpus_root).resolve()
    corpus_index = Path(args.corpus_index).resolve() if args.corpus_index else corpus_root / "corpus_index.json"
    if corpus_index.exists():
        index = normalize_workflow_entries(load_index(corpus_index), corpus_root)
        index_source = str(corpus_index)
    else:
        index = discover_workflow_index(corpus_root)
        index_source = "discovered_from_workflow_tree"
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

    # ------------------------------------------------------------------
    # Register workflow images before generating manifests so the
    # controller can resolve spec.workflowImageRef at runtime.
    # ------------------------------------------------------------------
    # Resolve registry_root relative to project root (scripts/..)
    _project_root = Path(__file__).resolve().parent.parent
    _reg_root = Path(args.registry_root)
    if not _reg_root.is_absolute():
        _reg_root = (_project_root / _reg_root).resolve()
    registry_root = str(_reg_root)

    # Build a flat list of unique entries for registration
    _register_entries = []
    _seen_keys = set()
    for entry in pure_entries:
        driver = entry.get("driver", workflow_driver(entry["workload_type"], entry["benchmark_name"]))
        key = (entry["workload_type"], entry["category"], driver)
        if key not in _seen_keys:
            _seen_keys.add(key)
            _register_entries.append(entry)
    for entry in hybrid_entries:
        driver = entry.get("driver", workflow_driver(entry["workload_type"], entry["benchmark_name"]))
        key = (entry["workload_type"], entry["category"], driver)
        if key not in _seen_keys:
            _seen_keys.add(key)
            _register_entries.append(entry)

    image_id_map = _register_workflow_images(_register_entries, registry_root=registry_root)
    print(f"Registered {len(image_id_map)} workflow image(s) in {registry_root}")
    for key, img_id in sorted(image_id_map.items()):
        print(f"  {key} -> {img_id}")

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
        workflow_template = str((corpus_root / entry["workflow_manifest_rel"]).resolve())
        parameters = (
            str((corpus_root / entry["qworkdir_rel"] / entry["parameter_file"]).resolve())
            if entry.get("parameter_file")
            else None
        )
        template_meta = _read_template_metadata(Path(workflow_template))

        # Use freshly registered image id, falling back to template's value
        driver = entry.get("driver", workflow_driver(entry["workload_type"], entry["benchmark_name"]))
        img_key = (entry["workload_type"], entry["category"], driver)
        workflow_image_ref = image_id_map.get(img_key, template_meta["workflow_image_ref"])

        return {
            "schema_version": 1,
            "logical_job_id": f"job_{submit_order:06d}",
            "submit_order": submit_order,
            "workload_type": entry["workload_type"],
            "workflow_type": "hybrid" if entry["workload_type"] == "hybrid" else "pure_quantum",
            "benchmark_name": entry["benchmark_name"],
            "category": entry["category"],
            "family": "Qonductor-workflow",
            "submission": "Qonductor HybridWorkflow",
            "submit_mode": "hybrid_workflow",
            "workflow_kind": template_meta["workflow_kind"],
            "workflow_api_version": template_meta["workflow_api_version"],
            "workflow_name": "",
            "workflow_manifest": "",
            "workflow_manifest_template": workflow_template,
            "workflow_image_ref": workflow_image_ref,
            "workflow_dir": qworkdir,
            "workflow_spec": qspec,
            "workflow_parameters": parameters,
            "requested_qubits": int(entry["requested_qubits"]),
            "hybrid_job": bool(entry["hybrid_job"]),
            "driver": entry.get("driver", workflow_driver(entry["workload_type"], entry["benchmark_name"])),
            "priority": template_meta["priority"],
            "scheduling": template_meta["scheduling"],
            "predicted_qpu_work_ms": round(predicted, 6),
            "runtime_estimation_method": runtime_method,
            "arrival_time_ms": 0,
            "size_bucket": entry["size_bucket"],
            "metadata": {
                "source": "workflow",
                "spec_version": int(entry.get("spec_version", 0)),
                "circuit_files": list(entry.get("circuit_files", [])),
                "gate_count": int(metrics["gate_count"]),
                "two_qubit_gate_count": int(metrics["two_qubit_gate_count"]),
                "qasm_line_count": int(metrics["qasm_line_count"]),
                "measurement_count": int(metrics["measurement_count"]),
                "parameter_count": int(metrics["parameter_count"]),
                "workflow_template_exists": Path(workflow_template).exists(),
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
    assign_workflow_identity(manifest_entries, args)

    trace_entries = [
        {
            "logical_job_id": item["logical_job_id"],
            "arrival_time_ms": item["arrival_time_ms"],
            "submit_order": item["submit_order"],
            "workflow_name": item["workflow_name"],
            "workflow_manifest": item["workflow_manifest"],
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
        "corpus_index_path": index_source,
        "manifest_dir": str(workflow_manifest_dir(Path(args.output_manifest)).resolve()),
        "manifest_sha256": "",
        "trace_sha256": "",
    }

    manifest_text = jsonl_text(manifest_entries)
    trace_text = jsonl_text(trace_entries)
    summary["manifest_sha256"] = sha256_text(manifest_text)
    summary["trace_sha256"] = sha256_text(trace_text)
    summary_text = json_text(summary)

    print("Generated Qonductor workflow plan:")
    print(f"  corpus_root={corpus_root}")
    print(f"  corpus_index={index_source}")
    print(f"  total_jobs={len(manifest_entries)}")
    print(f"  submit_window_sec={args.submit_window_sec}")
    print(f"  submission_rate_jobs_per_sec={summary['submission_rate_jobs_per_sec']:.6f}")
    print(f"  pure_job_count={pure_count_actual}")
    print(f"  hybrid_job_count={hybrid_count_actual}")
    print(f"  estimated_total_qpu_work_ms={summary['estimated_total_qpu_work_ms']}")
    print(f"  manifest_dir={summary['manifest_dir']}")
    print(f"  manifest_sha256={summary['manifest_sha256']}")
    print(f"  trace_sha256={summary['trace_sha256']}")
    if summary["warnings"]:
        print("  warnings=" + " | ".join(summary["warnings"]))

    if args.dry_run:
        print(summary_text, end="")
        return

    for path in [Path(args.output_manifest), Path(args.output_trace), Path(args.output_summary)]:
        path.parent.mkdir(parents=True, exist_ok=True)

    write_workflow_manifests(manifest_entries, corpus_root, image_id_map)
    Path(args.output_manifest).write_text(manifest_text, encoding="utf-8")
    Path(args.output_trace).write_text(trace_text, encoding="utf-8")
    Path(args.output_summary).write_text(summary_text, encoding="utf-8")


if __name__ == "__main__":
    main()
