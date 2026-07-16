#!/usr/bin/env python3
"""
Generate batch workload YAML manifests for timed batch submission.

For each task in the batch:
  - Randomly selects a workload directory from the workload library
  - Assigns varied iteration / shot counts for realistic diversity
  - Generates a unique kubectl-ready HybridWorkflow YAML manifest

Workflow images are pre-registered once per workload type and reused
across all manifests of that type — avoiding redundant registry entries.

Outputs:
  - One YAML manifest per task under <output-dir>/<batch_id>/
  - A schedule CSV enumerating every task with its metadata
  - BATCH_ID and SCHEDULE_PATH printed to stdout for the Bash wrapper

Usage:
    python scripts/generate_batch_manifests.py \\
        --workload-dir workload_python \\
        --count 30 \\
        --batch-id batch_20260714_120000 \\
        --shots 1024 \\
        --max-iterations 20 \\
        --seed 42
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
from pathlib import Path
from typing import Any

# Ensure the project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import yaml

from src.workflow.workflow_registry import WorkflowRegistry
from src.workflow.qaoa_conversion import (
    build_qaoa_workflow_inputs,
    build_hybrid_workflow_manifest,
    register_qaoa_workflow,
)
from src.workflow.workload_conversion import (
    build_ansatz_workflow_inputs,
    register_ansatz_workflow,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _k8s_name(value: str) -> str:
    """Convert an arbitrary string to a valid Kubernetes resource name."""
    name = re.sub(r"[^a-z0-9-]+", "-", value.lower().replace("_", "-"))
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:63].rstrip("-") or "workflow"


def _batch_workflow_name(workload_name: str, batch_id: str, index: int) -> str:
    """Build a unique, valid Kubernetes name for a batch workflow."""
    base = _k8s_name(f"{workload_name}-{batch_id}")
    digest = hashlib.sha1(
        f"{workload_name}-{batch_id}-{index:04d}".encode("utf-8")
    ).hexdigest()[:8]
    suffix = f"{digest}-{index:04d}"
    prefix_len = 63 - len(suffix) - 1
    prefix = base[:prefix_len].rstrip("-") or "workflow"
    return f"{prefix}-{suffix}"


def _py_name(value: str) -> str:
    """Convert to a Python-safe identifier fragment."""
    return re.sub(r"[^a-z0-9_]+", "_", value.lower()).strip("_") or "workflow"


def discover_workloads(root: Path) -> list[Path]:
    """Return sorted list of valid workload directories under *root*."""
    if not root.exists():
        raise FileNotFoundError(f"Workload directory not found: {root}")
    workloads = sorted(
        p
        for p in root.iterdir()
        if p.is_dir()
        and (p / "spec_12.json").exists()
        and (p / "parameters_12.json").exists()
    )
    if not workloads:
        raise RuntimeError(
            f"No valid workload directories found in {root}. "
            f"Each must contain spec_12.json and parameters_12.json."
        )
    return workloads


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate batch workload YAML manifests for Qonductor",
    )
    parser.add_argument(
        "--workload-dir",
        default="workload_python",
        help="Directory containing workload subdirectories (default: workload_python)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/batch_submission",
        help="Parent output directory (default: data/batch_submission)",
    )
    parser.add_argument(
        "--count",
        type=int,
        required=True,
        help="Total number of workflow manifests to generate",
    )
    parser.add_argument(
        "--batch-id",
        required=True,
        help="Unique batch identifier (e.g. batch_20260714_120000)",
    )
    parser.add_argument(
        "--registry-root",
        default="data/workflow_registry",
        help="Workflow registry root (default: data/workflow_registry)",
    )
    parser.add_argument(
        "--shots",
        type=int,
        default=1024,
        help="Base shot count; randomised ±50%% per task (default: 1024)",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=20,
        help="Upper bound for randomised iterations per task (default: 20)",
    )
    parser.add_argument(
        "--priority",
        choices=("balanced", "fidelity", "jct"),
        default="balanced",
        help="Scheduling priority (default: balanced)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducible task selection",
    )
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    # ------------------------------------------------------------------
    # 1. Discover workloads
    # ------------------------------------------------------------------
    workload_root = Path(args.workload_dir)
    workloads = discover_workloads(workload_root)

    print(f"Discovered {len(workloads)} workload type(s):")
    for wl in workloads:
        params = json.loads((wl / "parameters_12.json").read_text())
        print(f"  - {wl.name}  (benchmark={params.get('benchmark', '?')}, "
              f"qubits={params.get('num_qubits', '?')})")

    # ------------------------------------------------------------------
    # 2. Pre-register one WorkflowImage per workload type
    # ------------------------------------------------------------------
    registry_root = str(PROJECT_ROOT / args.registry_root)
    registry = WorkflowRegistry(root_dir=registry_root)
    workload_meta: dict[str, dict[str, Any]] = {}  # dir_name → {image_id, benchmark, ...}

    print(f"\nRegistering workflow images (registry: {args.registry_root}) ...")
    for wl_dir in workloads:
        params = json.loads((wl_dir / "parameters_12.json").read_text())
        benchmark = params.get("benchmark", wl_dir.name)
        image_name = _py_name(f"{wl_dir.name}_12_dynamic")

        try:
            if benchmark == "qaoa":
                image = register_qaoa_workflow(
                    wl_dir,
                    registry_root=registry_root,
                    name=image_name,
                )
            else:
                image = register_ansatz_workflow(
                    wl_dir,
                    registry_root=registry_root,
                    name=image_name,
                )
            workload_meta[wl_dir.name] = {
                "image_id": image.image_id,
                "benchmark": benchmark,
                "wl_dir": wl_dir,
            }
            print(f"  ✓ {wl_dir.name:60s} → image_ref={image.image_id}")
        except Exception as exc:
            # If image already exists we can still proceed — try to look it up
            print(f"  ⚠ {wl_dir.name}: register raised {exc} — "
                  f"attempting registry lookup ...")
            # Scan registry index for an existing image with matching name
            found = None
            for entry in registry.list_images():
                if entry.get("name") == image_name:
                    found = entry["image_id"]
                    break
            if found:
                workload_meta[wl_dir.name] = {
                    "image_id": found,
                    "benchmark": benchmark,
                    "wl_dir": wl_dir,
                }
                print(f"  ✓ {wl_dir.name:60s} → (existing) image_ref={found}")
            else:
                print(f"  ✗ {wl_dir.name}: could not register or find image — skipping this type",
                      file=sys.stderr)

    if not workload_meta:
        print("ERROR: No workload images could be registered.", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------
    # 3. Create output directory
    # ------------------------------------------------------------------
    out_dir = Path(args.output_dir) / args.batch_id
    out_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir = out_dir / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # 4. Generate manifests
    # ------------------------------------------------------------------
    schedule: list[dict[str, Any]] = []
    # Build a weighted list: each workload type appears once; selection is uniform
    wl_names = list(workload_meta.keys())

    print(f"\nGenerating {args.count} manifests ...")

    for i in range(args.count):
        # Randomly select a workload type
        wl_name = random.choice(wl_names)
        meta = workload_meta[wl_name]
        wl_dir = meta["wl_dir"]

        # Randomise iterations for workload diversity
        min_iter = max(1, args.max_iterations // 5)
        iterations = random.randint(min_iter, args.max_iterations)

        # Randomise shots ±50% around base
        shot_variation = random.randint(-args.shots // 2, args.shots // 2)
        # Round to a multiple of 128 (common shot quantisation)
        task_shots = max(128, ((args.shots + shot_variation) // 128) * 128)

        workflow_name = _batch_workflow_name(wl_name, args.batch_id, i)
        yaml_filename = f"{workflow_name}.yaml"
        yaml_path = manifests_dir / yaml_filename

        # Build inputs
        if meta["benchmark"] == "qaoa":
            inputs = build_qaoa_workflow_inputs(
                wl_dir,
                shots=task_shots,
                max_iterations=iterations,
                priority=args.priority,
            )
        else:
            inputs = build_ansatz_workflow_inputs(
                wl_dir,
                shots=task_shots,
                max_iterations=iterations,
                priority=args.priority,
            )

        # Retrieve the pre-registered image and build the manifest
        image = registry.get(meta["image_id"])
        if image is None:
            print(f"  ✗ Task {i:04d}: image {meta['image_id']} not found in registry — skipping",
                  file=sys.stderr)
            continue

        manifest = build_hybrid_workflow_manifest(
            image,
            inputs,
            name=workflow_name,
            priority=args.priority,
        )
        # Attach batch metadata labels
        manifest.setdefault("metadata", {}).setdefault("labels", {})
        manifest["metadata"]["labels"]["algorithm"] = meta["benchmark"]
        manifest["metadata"]["labels"]["batch_id"] = args.batch_id

        yaml_path.write_text(
            yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8"
        )

        schedule.append({
            "order": i,
            "workflow_name": workflow_name,
            "workload_type": wl_name,
            "benchmark": meta["benchmark"],
            "yaml_path": str(yaml_path),
            "iterations": iterations,
            "shots": task_shots,
            "image_ref": meta["image_id"],
        })

        if (i + 1) % max(1, args.count // 10) == 0:
            print(f"  ... {i + 1}/{args.count} manifests generated")

    # ------------------------------------------------------------------
    # 5. Write schedule CSV
    # ------------------------------------------------------------------
    schedule_path = out_dir / "schedule.csv"
    with open(schedule_path, "w", newline="") as fh:
        fh.write(
            "order,workflow_name,workload_type,benchmark,yaml_path,"
            "iterations,shots,image_ref\n"
        )
        for row in schedule:
            fh.write(
                f"{row['order']},{row['workflow_name']},{row['workload_type']},"
                f"{row['benchmark']},{row['yaml_path']},"
                f"{row['iterations']},{row['shots']},{row['image_ref']}\n"
            )

    # ------------------------------------------------------------------
    # 6. Report
    # ------------------------------------------------------------------
    print(f"\n{'='*60}")
    print(f"Batch generation complete")
    print(f"  Batch ID:    {args.batch_id}")
    print(f"  Manifests:   {len(schedule)}  → {manifests_dir}")
    print(f"  Schedule:    {schedule_path}")
    print(f"  Output dir:  {out_dir}")
    print(f"  Seed:        {args.seed}")
    print(f"{'='*60}")

    # Machine-parseable output for the Bash wrapper
    print(f"\nBATCH_ID={args.batch_id}")
    print(f"SCHEDULE_PATH={schedule_path}")
    print(f"MANIFEST_COUNT={len(schedule)}")


if __name__ == "__main__":
    main()
