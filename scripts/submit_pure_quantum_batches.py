#!/usr/bin/env python3
"""Submit workflow/pure_quantum tasks in fixed-size batches."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Sequence

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PURE_ROOT = PROJECT_ROOT / "workflow" / "pure_quantum"
TERMINAL_PHASES = {"Completed", "Failed", "Succeeded"}
DEFAULT_KUBECTL = ("docker", "exec", "k3s-server", "kubectl")
DEFAULT_KUBECTL_STDIN = ("docker", "exec", "-i", "k3s-server", "kubectl")


@dataclass
class RunRecord:
    batch: int
    index: int
    script: str
    manifest: str = ""
    name: str = ""
    phase: str = "Pending"
    submitted: bool = False
    error: str = ""
    quantum_jobs: dict[str, int] | None = None


class KubectlApi:
    def __init__(
        self,
        command: Sequence[str] = DEFAULT_KUBECTL,
        stdin_command: Sequence[str] = DEFAULT_KUBECTL_STDIN,
        namespace: str = "default",
    ) -> None:
        self.command = tuple(command)
        self.stdin_command = tuple(stdin_command)
        self.namespace = namespace

    def _run(
        self,
        args: Sequence[str],
        *,
        input_text: str | None = None,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        base = self.stdin_command if input_text is not None else self.command
        result = subprocess.run(
            [*base, *args],
            cwd=PROJECT_ROOT,
            input=input_text,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if check and result.returncode != 0:
            detail = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(" ".join([*base, *args]) + f": {detail}")
        return result

    def apply_manifest(self, manifest: dict[str, Any]) -> None:
        content = yaml.safe_dump(manifest, sort_keys=False)
        self._run(["apply", "-n", self.namespace, "-f", "-"], input_text=content)

    def get_hybridworkflow(self, name: str) -> dict[str, Any] | None:
        result = self._run(
            ["get", "hybridworkflow", name, "-n", self.namespace, "-o", "json"],
            check=False,
        )
        if result.returncode != 0:
            if "NotFound" in result.stderr or "not found" in result.stderr.lower():
                return None
            detail = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(detail)
        return json.loads(result.stdout)

    def list_quantumjobs(self) -> list[dict[str, Any]]:
        result = self._run(
            ["get", "quantumjobs", "-n", self.namespace, "-o", "json"],
            check=False,
        )
        if result.returncode != 0:
            if "No resources found" in result.stderr or "No resources found" in result.stdout:
                return []
            detail = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(detail)
        payload = json.loads(result.stdout)
        return payload.get("items", [])


def k8s_name(value: str, max_length: int = 63) -> str:
    name = re.sub(r"[^a-z0-9-]+", "-", value.lower().replace("_", "-"))
    name = re.sub(r"-+", "-", name).strip("-")
    return name[:max_length].rstrip("-") or "workflow"


def workflow_name(script: Path, index: int, run_id: str) -> str:
    rel = script.relative_to(PROJECT_ROOT)
    digest = hashlib.sha1(str(rel).encode("utf-8")).hexdigest()[:7]
    size = script.stem.removeprefix("submit-").removeprefix("submit_")
    base = k8s_name(f"pq-{index:03d}-{script.parent.name}-{size}")
    suffix = f"{run_id}-{digest}"
    return k8s_name(f"{base}-{suffix}", max_length=63)


def discover(limit: int = 0, offset: int = 0) -> list[Path]:
    scripts = sorted(PURE_ROOT.glob("**/submit_*.py"))
    if offset:
        scripts = scripts[offset:]
    return scripts[:limit] if limit else scripts


def run_generator(script: Path, name: str, shots: int) -> Path:
    args = [
        sys.executable,
        str(script),
        "--emit-manifest",
        "--shots",
        str(shots),
        "--workflow-name",
        name,
    ]
    result = subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())

    manifest_line = next(
        (line for line in result.stdout.splitlines()
         if line.startswith("manifest=")),
        "",
    )
    if not manifest_line:
        raise RuntimeError(f"manifest path not found in output: {result.stdout}")
    return Path(manifest_line.split("=", 1)[1].strip())


def manifest_name(path: Path) -> str:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    return manifest["metadata"]["name"]


def submit_manifest(api: KubectlApi, path: Path) -> None:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    api.apply_manifest(manifest)


def phase_of(api: KubectlApi, name: str) -> str:
    obj = api.get_hybridworkflow(name)
    if obj is None:
        return "Missing"
    return obj.get("status", {}).get("phase") or "Pending"


def qj_summary(api: KubectlApi, workflow: str) -> dict[str, int]:
    summary: dict[str, int] = {}
    for qj in api.list_quantumjobs():
        labels = qj.get("metadata", {}).get("labels", {}) or {}
        spec = qj.get("spec", {}) or {}
        if labels.get("workflow") != workflow and spec.get("workflowRef") != workflow:
            continue
        phase = qj.get("status", {}).get("phase") or "Pending"
        summary[phase] = summary.get(phase, 0) + 1
    return summary


def wait_batch(
    api: KubectlApi,
    records: list[RunRecord],
    *,
    poll: float,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        active = []
        for record in records:
            if not record.submitted or record.error:
                continue
            record.phase = phase_of(api, record.name)
            record.quantum_jobs = qj_summary(api, record.name)
            if record.phase not in TERMINAL_PHASES:
                active.append(record)

        completed = sum(1 for r in records if r.phase in {"Completed", "Succeeded"})
        failed = sum(1 for r in records if r.phase == "Failed" or r.error)
        print(
            f"[wait batch {records[0].batch}] "
            f"completed={completed} failed={failed} active={len(active)}",
            flush=True,
        )
        if not active:
            return
        time.sleep(poll)

    for record in records:
        if record.submitted and record.phase not in TERMINAL_PHASES:
            record.error = "timeout"


def write_summary(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--shots", type=int, default=128)
    parser.add_argument("--poll", type=float, default=15.0)
    parser.add_argument("--batch-timeout", type=float, default=1800.0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--continue-on-failure", action="store_true")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "pure_quantum_batch_results",
    )
    args = parser.parse_args()

    run_id = time.strftime("%Y%m%d%H%M%S")
    api = KubectlApi(namespace=args.namespace)
    scripts = discover(limit=args.limit, offset=args.offset)
    print(
        f"[plan] run_id={run_id} scripts={len(scripts)} "
        f"batch_size={args.batch_size} shots={args.shots}",
        flush=True,
    )

    all_records: list[RunRecord] = []
    stopped = False
    for batch_start in range(0, len(scripts), args.batch_size):
        batch_no = batch_start // args.batch_size + 1
        batch_scripts = scripts[batch_start:batch_start + args.batch_size]
        records: list[RunRecord] = []
        print(f"[batch {batch_no}] submitting {len(batch_scripts)} workflows",
              flush=True)

        for offset, script in enumerate(batch_scripts, start=1):
            index = batch_start + offset
            rel_script = str(script.relative_to(PROJECT_ROOT))
            record = RunRecord(batch=batch_no, index=index, script=rel_script)
            try:
                name = workflow_name(script, index, run_id)
                manifest = run_generator(script, name, args.shots)
                record.manifest = str(manifest.relative_to(PROJECT_ROOT))
                record.name = manifest_name(manifest)
                submit_manifest(api, manifest)
                record.submitted = True
                record.phase = phase_of(api, record.name)
                print(
                    f"[submit {index}/{len(scripts)}] {record.name} "
                    f"phase={record.phase} script={rel_script}",
                    flush=True,
                )
            except Exception as exc:
                record.error = str(exc)
                print(f"[error {index}/{len(scripts)}] {rel_script}: {exc}",
                      flush=True)
            records.append(record)
            all_records.append(record)

        wait_batch(api, records, poll=args.poll, timeout=args.batch_timeout)
        batch_failed = [r for r in records if r.phase == "Failed" or r.error]
        if batch_failed and not args.continue_on_failure:
            stopped = True
            print(
                f"[stop] batch {batch_no} has {len(batch_failed)} failure(s); "
                "use --continue-on-failure to submit remaining batches",
                flush=True,
            )
            break

    completed = sum(1 for r in all_records if r.phase in {"Completed", "Succeeded"})
    failed = sum(1 for r in all_records if r.phase == "Failed" or r.error)
    active = sum(
        1 for r in all_records
        if r.submitted and r.phase not in TERMINAL_PHASES and not r.error
    )
    summary = {
        "run_id": run_id,
        "shots": args.shots,
        "batch_size": args.batch_size,
        "submitted": sum(1 for r in all_records if r.submitted),
        "completed": completed,
        "failed_or_errors": failed,
        "still_active": active,
        "stopped": stopped,
        "records": [asdict(r) for r in all_records],
    }
    result_path = args.results_dir / f"pure_quantum_batches_{run_id}.json"
    write_summary(result_path, summary)
    print("[summary]", flush=True)
    print(json.dumps({k: v for k, v in summary.items() if k != "records"},
                     sort_keys=True), flush=True)
    print(f"[results] {result_path}", flush=True)

    if failed or active or stopped:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
