#!/usr/bin/env python3
"""Plot Qonductor job scheduling overhead from the latest 1500-job run.

Loads the most recent metadata file from data/end_to_end/qonductor_1500jobs/
and produces a line chart showing the per-event scheduling overhead breakdown.
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Project root (script lives in scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "end_to_end" / "qonductor_1500jobs"
PLOTS_DIR = PROJECT_ROOT / "plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

# --- Style -------------------------------------------------------------------
sns.set_theme(style="whitegrid", context="talk")
plt.rcParams.update({"font.size": 18, "axes.titlesize": 20, "axes.labelsize": 18})

COLORS = {
    "transpilation": "#4c72b0",
    "estimation": "#dd8452",
    "optimization": "#55a868",
    "total": "#c44e52",
}
LINEWIDTH = 2.0
MARKER_SIZE = 6


def find_latest_metadata(data_dir: Path) -> Path:
    """Return the path to the most recent metadata JSON in *data_dir*."""
    files = sorted(data_dir.glob("metadata_*.json"))
    if not files:
        sys.exit(f"No metadata_*.json files found in {data_dir}")
    return files[-1]


def load_overhead_data(metadata_path: Path) -> dict:
    """Parse the metadata file and return a dict of arrays keyed by component."""
    with open(metadata_path) as f:
        events = json.load(f)

    n = len(events)
    data = {
        "transpilation": np.zeros(n),
        "estimation": np.zeros(n),
        "optimization": np.zeros(n),
        "mcdm": np.zeros(n),
        "schedule_generation": np.zeros(n),
        "total": np.zeros(n),
        "timestamps": [],
    }

    for i, ev in enumerate(events):
        data["transpilation"][i] = ev["transpilation_time"]
        data["estimation"][i] = ev["estimation_time"]
        data["optimization"][i] = ev["optimization_time"]
        data["mcdm"][i] = ev["mcdm_time"]
        data["schedule_generation"][i] = ev["schedule_generation_time"]
        data["total"][i] = (
            ev["transpilation_time"]
            + ev["estimation_time"]
            + ev["optimization_time"]
            + ev["mcdm_time"]
            + ev["schedule_generation_time"]
        )
        data["timestamps"].append(datetime.fromisoformat(ev["time"]))

    return data


def plot_overhead(data: dict, metadata_path: Path, output_path: Path) -> None:
    """Draw a line chart of scheduling overhead per event."""
    x = np.arange(1, len(data["total"]) + 1)
    timestamp_strs = [ts.strftime("%H:%M:%S") for ts in data["timestamps"]]

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(18, 10), sharex=True, gridspec_kw={"height_ratios": [3, 1]}
    )

    # --- Top panel: stacked components + total line --------------------------
    components = [
        ("transpilation", "Transpilation", COLORS["transpilation"], "-"),
        ("estimation", "Estimation", COLORS["estimation"], "-"),
        ("optimization", "Optimization", COLORS["optimization"], "-"),
    ]
    for key, label, color, ls in components:
        ax1.plot(
            x, data[key],
            marker="o", markersize=MARKER_SIZE, linewidth=LINEWIDTH,
            linestyle=ls, color=color, label=label, alpha=0.85,
        )

    ax1.plot(
        x, data["total"],
        marker="D", markersize=MARKER_SIZE + 1, linewidth=LINEWIDTH + 0.5,
        linestyle="--", color=COLORS["total"], label="Total Overhead",
    )

    ax1.set_ylabel("Time (seconds)")
    ax1.set_title(
        f"Qonductor Scheduling Overhead — {len(x)} Events\n"
        f"({metadata_path.name})"
    )
    ax1.legend(loc="upper left", frameon=True, fontsize=15)
    ax1.grid(True, alpha=0.3)

    # Annotate averages on the right side
    ylim = ax1.get_ylim()
    for i, (key, label, color, _) in enumerate(components + [("total", "Total", COLORS["total"], None)]):
        avg = np.mean(data[key])
        ax1.axhline(y=avg, color=color, linestyle=":", alpha=0.4, linewidth=1)
        ax1.text(
            len(x) + 0.3, avg, f"avg {avg:.1f}s",
            fontsize=13, color=color, va="center", alpha=0.8,
        )

    # --- Bottom panel: mcdm + schedule_generation (sub-ms times) -------------
    # Convert to milliseconds
    mcdm_ms = data["mcdm"] * 1000
    sched_ms = data["schedule_generation"] * 1000

    ax2.plot(
        x, mcdm_ms,
        marker="s", markersize=MARKER_SIZE, linewidth=LINEWIDTH,
        color="#8e7cc3", label="MCDM", alpha=0.85,
    )
    ax2.plot(
        x, sched_ms,
        marker="^", markersize=MARKER_SIZE, linewidth=LINEWIDTH,
        color="#e7ba52", label="Schedule Generation", alpha=0.85,
    )
    ax2.set_xlabel("Scheduling Event #")
    ax2.set_ylabel("Time (milliseconds)")
    ax2.set_title("Fine-grained Overhead Components")
    ax2.legend(loc="upper right", frameon=True, fontsize=15)
    ax2.grid(True, alpha=0.3)

    # --- X-axis ticks --------------------------------------------------------
    # Show a subset of tick labels so they don't overlap
    tick_step = max(1, len(x) // 15)
    tick_positions = x[::tick_step] - 1
    tick_labels = [timestamp_strs[i] for i in range(0, len(x), tick_step)]
    plt.xticks(tick_positions + 1, tick_labels, rotation=45, ha="right", fontsize=13)
    ax1.set_xlim(0.5, len(x) + 3)

    # --- Summary text box ----------------------------------------------------
    summary_lines = [
        f"Total scheduling overhead: {np.sum(data['total']):.1f} s",
        f"Mean per event:            {np.mean(data['total']):.1f} s",
        f"Median per event:          {np.median(data['total']):.1f} s",
        f"Transpilation share:       {np.sum(data['transpilation'])/np.sum(data['total'])*100:.0f}%",
        f"Estimation share:          {np.sum(data['estimation'])/np.sum(data['total'])*100:.0f}%",
        f"Optimization share:        {np.sum(data['optimization'])/np.sum(data['total'])*100:.0f}%",
    ]
    ax1.text(
        0.98, 0.97, "\n".join(summary_lines),
        transform=ax1.transAxes, fontsize=13, va="top", ha="right",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="wheat", alpha=0.7),
    )

    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved → {output_path}")


def main():
    metadata_path = find_latest_metadata(DATA_DIR)
    print(f"Loading: {metadata_path.name}")

    data = load_overhead_data(metadata_path)
    print(f"Events: {len(data['total'])}")
    print(f"Total overhead: {np.sum(data['total']):.2f} s  "
          f"(avg {np.mean(data['total']):.2f} s/event)")

    output_path = PLOTS_DIR / "scheduling_overhead.pdf"
    plot_overhead(data, metadata_path, output_path)


if __name__ == "__main__":
    main()
