#!/usr/bin/env python3
"""Submit converted workflow entries to Kubernetes with bounded concurrency."""

from __future__ import annotations

import argparse
import base64
import json
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONVERTED_ROOTS = (
    PROJECT_ROOT / "workflow" / "pure_quantum",
    PROJECT_ROOT / "workflow" / "hybrid",
    PROJECT_ROOT / "workflow" / "hybrid_6q",
)
TERMINAL_PHASES = {"Completed", "Failed", "Succeeded"}


@dataclass
class WorkflowRun:
    script: Path
    manifest: Path | None = None
    name: str = ""
    category: str = ""
    submitted: bool = False
    phase: str = "Pending"
    started_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None
    error: str = ""


class KubeApi:
    def __init__(self, kubeconfig: Path):
        cfg = yaml.safe_load(kubeconfig.read_text(encoding="utf-8"))
        current = cfg.get("current-context")
        context = next(c for c in cfg["contexts"] if c["name"] == current)["context"]
        cluster = next(c for c in cfg["clusters"] if c["name"] == context["cluster"])["cluster"]
        user = next(u for u in cfg["users"] if u["name"] == context["user"])["user"]

        self.server = cluster["server"].rstrip("/")
        self.namespace = context.get("namespace", "default")
        self._tmpdir = tempfile.TemporaryDirectory(prefix="qonductor-kube-")
        tmp = Path(self._tmpdir.name)
        ca = tmp / "ca.crt"
        cert = tmp / "client.crt"
        key = tmp / "client.key"
        ca.write_bytes(base64.b64decode(cluster["certificate-authority-data"]))
        cert.write_bytes(base64.b64decode(user["client-certificate-data"]))
        key.write_bytes(base64.b64decode(user["client-key-data"]))
        self.context = ssl.create_default_context(cafile=str(ca))
        self.context.load_cert_chain(certfile=str(cert), keyfile=str(key))

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            f"{self.server}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(req, context=self.context, timeout=20) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"{method} {path} failed: HTTP {exc.code}: {raw}") from exc

    def get_nodes(self) -> dict[str, Any]:
        return self.request("GET", "/api/v1/nodes")

    def get_pods(self) -> dict[str, Any]:
        return self.request("GET", "/api/v1/pods")

    def create_hybridworkflow(self, manifest: dict[str, Any]) -> dict[str, Any]:
        ns = manifest.get("metadata", {}).get("namespace") or self.namespace
        return self.request(
            "POST",
            f"/apis/qonductor.io/v1/namespaces/{ns}/hybridworkflows",
            manifest,
        )

    def get_hybridworkflow(self, name: str, namespace: str = "default") -> dict[str, Any] | None:
        try:
            return self.request(
                "GET",
                f"/apis/qonductor.io/v1/namespaces/{namespace}/hybridworkflows/{name}",
            )
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return None
            raise

    def list_hybridworkflows(self, namespace: str = "default") -> list[dict[str, Any]]:
        data = self.request(
            "GET",
            f"/apis/qonductor.io/v1/namespaces/{namespace}/hybridworkflows",
        )
        return data.get("items", [])

    def list_quantumjobs(self, namespace: str = "default") -> list[dict[str, Any]]:
        data = self.request(
            "GET",
            f"/apis/qonductor.io/v1/namespaces/{namespace}/quantumjobs",
        )
        return data.get("items", [])


def discover_submit_scripts(limit: int | None = None) -> list[Path]:
    scripts: list[Path] = []
    for root in CONVERTED_ROOTS:
        if root.exists():
            scripts.extend(sorted(root.glob("**/submit_*.py")))
    scripts = sorted(scripts)
    return scripts[:limit] if limit else scripts


def run_generator(script: Path, pure_shots: int, hybrid_smoke: bool) -> Path:
    args = [sys.executable, str(script), "--emit-manifest"]
    if "/pure_quantum/" in str(script):
        args.extend(["--shots", str(pure_shots)])
    elif hybrid_smoke:
        args.append("--smoke")
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
        (line for line in result.stdout.splitlines() if line.startswith("manifest=")),
        "",
    )
    if not manifest_line:
        raise RuntimeError(f"manifest path not found in output: {result.stdout}")
    return Path(manifest_line.split("=", 1)[1].strip())


def manifest_name(path: Path) -> str:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    return manifest["metadata"]["name"]


def submit_manifest(api: KubeApi, path: Path) -> str:
    manifest = yaml.safe_load(path.read_text(encoding="utf-8"))
    name = manifest["metadata"]["name"]
    try:
        api.create_hybridworkflow(manifest)
        return name
    except RuntimeError as exc:
        if "HTTP 409" in str(exc):
            return name
        raise


def phase_of(api: KubeApi, run: WorkflowRun) -> str:
    obj = api.get_hybridworkflow(run.name)
    if not obj:
        return "Missing"
    return obj.get("status", {}).get("phase") or "Pending"


def active_runs(api: KubeApi, runs: list[WorkflowRun]) -> list[WorkflowRun]:
    active: list[WorkflowRun] = []
    for run in runs:
        if not run.submitted or run.phase in TERMINAL_PHASES or run.error:
            continue
        run.phase = phase_of(api, run)
        if run.phase in TERMINAL_PHASES:
            run.finished_at = time.monotonic()
        else:
            active.append(run)
    return active


def wait_for_slot(api: KubeApi, runs: list[WorkflowRun], max_active: int, poll: float) -> None:
    while len(active_runs(api, runs)) >= max_active:
        summary = ", ".join(f"{r.name}:{r.phase}" for r in active_runs(api, runs))
        print(f"[wait] active={len(active_runs(api, runs))}/{max_active} {summary}", flush=True)
        time.sleep(poll)


def print_cluster_snapshot(api: KubeApi) -> None:
    nodes = api.get_nodes().get("items", [])
    pods = api.get_pods().get("items", [])
    hws = api.list_hybridworkflows()
    qjs = api.list_quantumjobs()
    pod_phases: dict[str, int] = {}
    for pod in pods:
        phase = pod.get("status", {}).get("phase", "Unknown")
        pod_phases[phase] = pod_phases.get(phase, 0) + 1
    print(f"[cluster] nodes={len(nodes)} pods={len(pods)} pod_phases={pod_phases}")
    print(f"[cluster] existing hybridworkflows={len(hws)} quantumjobs={len(qjs)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", default=str(Path.home() / ".kube" / "config"))
    parser.add_argument("--max-active", type=int, default=2)
    parser.add_argument("--poll", type=float, default=15.0)
    parser.add_argument("--pure-shots", type=int, default=128)
    parser.add_argument("--hybrid-smoke", action="store_true", default=True)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--final-timeout", type=float, default=1800.0)
    args = parser.parse_args()

    api = KubeApi(Path(args.kubeconfig))
    print_cluster_snapshot(api)

    scripts = discover_submit_scripts(args.limit or None)
    print(f"[plan] submitting {len(scripts)} workflows with max_active={args.max_active}")

    runs: list[WorkflowRun] = []
    for index, script in enumerate(scripts, start=1):
        wait_for_slot(api, runs, args.max_active, args.poll)
        run = WorkflowRun(script=script, category=script.relative_to(PROJECT_ROOT).parts[1])
        try:
            run.manifest = run_generator(script, args.pure_shots, args.hybrid_smoke)
            run.name = manifest_name(run.manifest)
            submit_manifest(api, run.manifest)
            run.submitted = True
            run.phase = phase_of(api, run)
            print(f"[submit {index}/{len(scripts)}] {run.name} phase={run.phase} script={script.relative_to(PROJECT_ROOT)}", flush=True)
        except Exception as exc:
            run.error = str(exc)
            run.finished_at = time.monotonic()
            print(f"[error {index}/{len(scripts)}] {script.relative_to(PROJECT_ROOT)}: {run.error}", flush=True)
        runs.append(run)

    deadline = time.monotonic() + args.final_timeout
    while active_runs(api, runs) and time.monotonic() < deadline:
        active = active_runs(api, runs)
        summary = ", ".join(f"{r.name}:{r.phase}" for r in active)
        print(f"[final-wait] active={len(active)} {summary}", flush=True)
        time.sleep(args.poll)

    active_runs(api, runs)
    completed = sum(1 for r in runs if r.phase in {"Completed", "Succeeded"})
    failed = sum(1 for r in runs if r.phase == "Failed" or r.error)
    still_active = [r for r in runs if r.submitted and r.phase not in TERMINAL_PHASES]
    print("[summary]")
    print(f"submitted={sum(1 for r in runs if r.submitted)} completed={completed} failed_or_errors={failed} still_active={len(still_active)}")
    for run in runs:
        status = run.error or run.phase
        print(f"{run.name or run.script.name},{status},{run.script.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
