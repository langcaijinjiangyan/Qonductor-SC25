#!/usr/bin/env python3
"""Export Qonductor scheduling metrics from a k3s cluster.

Connects to the metrics HTTP endpoint exposed by the operator pod and
saves data locally as JSON or CSV.

Usage::

    # Port-forward the operator pod first (if running outside the cluster)
    kubectl port-forward -n default deploy/qonductor-operator 9100:9100 &

    # Export all quantum job metrics as CSV (full-width)
    python scripts/export_metrics.py -o data/k3s_metrics.csv

    # Export in e2e-compatible format (timestamp, fidelity, JCT)
    python scripts/export_metrics.py --format e2e -o data/jct_fidelity_k3s.csv

    # Filter by workflow name
    python scripts/export_metrics.py --workflow my-qaoa -o data/qaoa.csv

    # Export as JSON (quantum jobs)
    python scripts/export_metrics.py --json -o data/jobs.json

    # Export workflow-level aggregate metrics as JSON
    python scripts/export_metrics.py --type workflows --json -o data/wf.json

    # Direct cluster-internal access (no port-forward needed)
    python scripts/export_metrics.py \\
        --url http://qonductor-metrics.default.svc.cluster.local:9100 \\
        -o data/k3s_metrics.csv
"""

from __future__ import annotations

import argparse
import json
import sys
from urllib.error import URLError
from urllib.request import urlopen


def fetch_json(url: str) -> list[dict] | dict:
    """GET *url* and return the parsed JSON body."""
    with urlopen(url) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_csv(url: str) -> str:
    """GET *url* and return the raw CSV text."""
    with urlopen(url) as resp:
        return resp.read().decode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Qonductor scheduling metrics from a k3s cluster",
    )
    parser.add_argument(
        "--url",
        default="http://localhost:9100",
        help="Metrics server base URL (default: http://localhost:9100)",
    )
    parser.add_argument(
        "-o", "--output",
        default="qonductor_metrics.csv",
        help="Output file path (default: qonductor_metrics.csv)",
    )
    parser.add_argument(
        "--workflow",
        default=None,
        help="Filter by HybridWorkflow name",
    )
    parser.add_argument(
        "--format",
        choices=["full", "e2e"],
        default="full",
        help="CSV format: 'full' (all fields) or 'e2e' "
             "(timestamp,fidelity,JCT — matches offline experiments)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Export as JSON instead of CSV",
    )
    parser.add_argument(
        "--type",
        choices=["quantum-jobs", "workflows"],
        default="quantum-jobs",
        dest="data_type",
        help="Which resource type to export "
             "(default: quantum-jobs)",
    )

    args = parser.parse_args()
    base = args.url.rstrip("/")

    try:
        if args.json:
            # JSON export
            if args.data_type == "workflows":
                url = f"{base}/metrics/workflows"
            else:
                url = f"{base}/metrics/quantum-jobs"
                if args.workflow:
                    url += f"?workflow={args.workflow}"

            data = fetch_json(url)
            out = args.output
            if not out.endswith(".json"):
                out = out.replace(".csv", ".json")
            with open(out, "w") as fh:
                json.dump(data, fh, indent=2)
            count = len(data) if isinstance(data, list) else 1
            print(f"Exported {count} record(s) → {out}")
        else:
            # CSV export
            if args.data_type == "workflows":
                print(
                    "CSV export for workflows is not supported. "
                    "Use --json --type workflows instead.",
                    file=sys.stderr,
                )
                sys.exit(1)

            url = f"{base}/metrics/csv?format={args.format}"
            if args.workflow:
                url += f"&workflow={args.workflow}"

            csv_text = fetch_csv(url)
            with open(args.output, "w", newline="") as fh:
                fh.write(csv_text)
            lines = csv_text.strip().split("\n")
            count = len(lines) - 1  # subtract header
            print(f"Exported {count} job record(s) → {args.output}")

    except URLError as exc:
        print(
            f"ERROR: Could not reach metrics server at {base}\n"
            f"  {exc}\n"
            f"  Hint: run 'kubectl port-forward -n default "
            f"deploy/qonductor-operator 9100:9100 &' first.",
            file=sys.stderr,
        )
        sys.exit(1)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
