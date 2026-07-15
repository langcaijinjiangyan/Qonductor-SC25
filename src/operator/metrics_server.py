"""
Lightweight metrics export server for the Qonductor operator.

Exposes QuantumJob and HybridWorkflow metrics as JSON and CSV for
collection by external tools or the ``export_metrics.py`` CLI script.

Runs as a daemon thread inside the operator pod — zero additional
dependencies beyond the Python 3.11 standard library.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any
from urllib.parse import parse_qs, urlparse

from src.operator.k8s_client import (
    HYBRID_WORKFLOW_PLURAL,
    QUANTUM_JOB_PLURAL,
    K8sClient,
)

logger = logging.getLogger(__name__)

METRICS_PORT = 9100


def _parse_iso(s: str) -> datetime | None:
    """Parse an ISO-8601 timestamp string, tolerating trailing Z."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _extract_quantum_job_metrics(qj: dict) -> dict[str, Any]:
    """Extract standardised metrics fields from a QuantumJob CR."""
    meta = qj.get("metadata", {})
    spec = qj.get("spec", {})
    status = qj.get("status", {})

    arrived_at = meta.get("creationTimestamp", "")
    scheduled_at = status.get("scheduledAt", "")
    completed_at = ""  # executor / controller may set this in future

    # Compute waiting time from timestamps
    waiting_time_s: float | None = None
    arrived_dt = _parse_iso(arrived_at)
    scheduled_dt = _parse_iso(scheduled_at)
    if arrived_dt and scheduled_dt:
        waiting_time_s = (scheduled_dt - arrived_dt).total_seconds()

    actual_exec = status.get("actualExecutionTime")
    estimated_exec = status.get("estimatedExecutionTime")
    execution_time_s = (
        float(actual_exec) if actual_exec is not None
        else float(estimated_exec) if estimated_exec is not None
        else None
    )

    actual_fid = status.get("actualFidelity")
    estimated_fid = status.get("estimatedFidelity")

    return {
        "job_name": meta.get("name", ""),
        "workflow": spec.get("workflowRef", ""),
        "step_id": spec.get("stepId", ""),
        "qubits": spec.get("qubits", 0),
        "shots": spec.get("shots", 0),
        "priority": spec.get("priority", "balanced"),
        "phase": status.get("phase", ""),
        "assigned_qpu": status.get("assignedQPU", ""),
        "arrived_at": arrived_at,
        "scheduled_at": scheduled_at,
        "completed_at": completed_at,
        "waiting_time_s": waiting_time_s,
        "execution_time_s": execution_time_s,
        "estimated_fidelity": (
            float(estimated_fid) if estimated_fid is not None else None
        ),
        "actual_fidelity": (
            float(actual_fid) if actual_fid is not None else None
        ),
    }


def _extract_workflow_metrics(wf: dict) -> dict[str, Any]:
    """Extract aggregate metrics from a HybridWorkflow CR."""
    meta = wf.get("metadata", {})
    status = wf.get("status", {})

    return {
        "workflow_name": meta.get("name", ""),
        "phase": status.get("phase", ""),
        "total_steps": status.get("totalSteps", 0),
        "steps_completed": status.get("stepsCompleted", 0),
        "classical_step_count": status.get("classicalStepCount", 0),
        "quantum_step_count": status.get("quantumStepCount", 0),
        "average_fidelity": status.get("averageFidelity"),
        "total_waiting_time_s": status.get("totalWaitingTime"),
        "total_execution_time_s": status.get("totalExecutionTime"),
        "created_at": meta.get("creationTimestamp", ""),
    }


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP request handler for metrics endpoints.

    The ``k8s`` class attribute must be set to a ``K8sClient`` instance
    before the server starts accepting requests.
    """

    k8s: K8sClient = None  # type: ignore[assignment]

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)

        if path == "/healthz":
            self._send_json(200, {"status": "ok"})
        elif path == "/metrics/quantum-jobs":
            self._handle_quantum_jobs(params)
        elif path == "/metrics/workflows":
            self._handle_workflows()
        elif path == "/metrics/csv":
            self._handle_csv(params)
        else:
            self._send_json(404, {"error": "not found"})

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    def _handle_quantum_jobs(self, params: dict) -> None:
        """Return all QuantumJob CRs with per-job metrics as JSON."""
        workflow_filter = params.get("workflow", [None])[0]

        try:
            jobs = self.k8s.list_cr(QUANTUM_JOB_PLURAL)
        except Exception:
            logger.exception("Failed to list QuantumJob CRs")
            self._send_json(500, {"error": "failed to list quantum jobs"})
            return

        metrics = []
        for qj in jobs:
            m = _extract_quantum_job_metrics(qj)
            if workflow_filter and m["workflow"] != workflow_filter:
                continue
            metrics.append(m)

        self._send_json(200, metrics)

    def _handle_workflows(self) -> None:
        """Return all HybridWorkflow CRs with aggregate metrics as JSON."""
        try:
            workflows = self.k8s.list_cr(HYBRID_WORKFLOW_PLURAL)
        except Exception:
            logger.exception("Failed to list HybridWorkflow CRs")
            self._send_json(500, {"error": "failed to list workflows"})
            return

        metrics = [_extract_workflow_metrics(w) for w in workflows]
        self._send_json(200, metrics)

    def _handle_csv(self, params: dict) -> None:
        """Return per-job metrics as CSV.

        Query parameters:
        - ``workflow`` — filter by workflow name
        - ``format``   — ``"e2e"`` produces ``timestamp,fidelity,JCT``
          (compatible with the offline experiment CSVs); otherwise a
          wide row with all fields is returned.
        """
        fmt = params.get("format", ["full"])[0]
        workflow_filter = params.get("workflow", [None])[0]

        try:
            jobs = self.k8s.list_cr(QUANTUM_JOB_PLURAL)
        except Exception:
            logger.exception("Failed to list QuantumJob CRs for CSV")
            self._send_json(500, {"error": "failed to list quantum jobs"})
            return

        output = io.StringIO()
        writer = csv.writer(output)

        if fmt == "e2e":
            # Match the offline experiment format:
            #   timestamp, fidelity, JCT
            writer.writerow(["timestamp", "fidelity", "JCT"])
            for qj in jobs:
                m = _extract_quantum_job_metrics(qj)
                if workflow_filter and m["workflow"] != workflow_filter:
                    continue
                fid = (
                    m["actual_fidelity"]
                    or m["estimated_fidelity"]
                    or 0.0
                )
                jct = (
                    m["execution_time_s"]
                    if m["execution_time_s"] is not None
                    else 0.0
                )
                writer.writerow([
                    m["arrived_at"],
                    round(float(fid), 6),
                    round(float(jct), 3),
                ])
        else:
            # Full-width CSV
            writer.writerow([
                "timestamp", "workflow", "step_id", "job_name",
                "qubits", "shots", "priority", "phase", "assigned_qpu",
                "arrived_at", "scheduled_at",
                "waiting_time_s", "execution_time_s",
                "estimated_fidelity", "actual_fidelity",
            ])
            for qj in jobs:
                m = _extract_quantum_job_metrics(qj)
                if workflow_filter and m["workflow"] != workflow_filter:
                    continue
                writer.writerow([
                    m["arrived_at"],
                    m["workflow"],
                    m["step_id"],
                    m["job_name"],
                    m["qubits"],
                    m["shots"],
                    m["priority"],
                    m["phase"],
                    m["assigned_qpu"],
                    m["arrived_at"],
                    m["scheduled_at"],
                    (
                        round(m["waiting_time_s"], 3)
                        if m["waiting_time_s"] is not None else ""
                    ),
                    (
                        round(m["execution_time_s"], 3)
                        if m["execution_time_s"] is not None else ""
                    ),
                    (
                        round(m["estimated_fidelity"], 6)
                        if m["estimated_fidelity"] is not None else ""
                    ),
                    (
                        round(m["actual_fidelity"], 6)
                        if m["actual_fidelity"] is not None else ""
                    ),
                ])

        self._send_csv(200, output.getvalue())

    # ------------------------------------------------------------------
    # Response helpers
    # ------------------------------------------------------------------

    def _send_json(self, code: int, data: Any) -> None:
        body = json.dumps(data, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_csv(self, code: int, csv_text: str) -> None:
        body = csv_text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/csv")
        self.send_header(
            "Content-Disposition",
            "attachment; filename=qonductor_metrics.csv",
        )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug("Metrics HTTP: %s", fmt % args)


# ------------------------------------------------------------------
# Server factory
# ------------------------------------------------------------------


def _make_handler(k8s_client: K8sClient) -> type[MetricsHandler]:
    """Create a MetricsHandler subclass bound to *k8s_client*."""

    class _BoundHandler(MetricsHandler):
        k8s = k8s_client

    return _BoundHandler


def create_metrics_server(
    k8s_client: K8sClient,
    port: int = METRICS_PORT,
) -> HTTPServer:
    """Create (but do not start) the metrics HTTP server."""
    handler_class = _make_handler(k8s_client)
    return HTTPServer(("0.0.0.0", port), handler_class)


def start_metrics_server(
    k8s_client: K8sClient,
    port: int = METRICS_PORT,
) -> threading.Thread:
    """Start the metrics HTTP server in a daemon thread.

    Returns the thread so callers can ``join()`` if needed.
    """
    server = create_metrics_server(k8s_client, port)

    def _serve() -> None:
        logger.info("Metrics server listening on 0.0.0.0:%d", port)
        try:
            server.serve_forever()
        except Exception:
            logger.exception("Metrics server crashed")

    thread = threading.Thread(target=_serve, daemon=True, name="metrics")
    thread.start()
    return thread
