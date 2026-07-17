#!/usr/bin/env python3
"""Plot submission-time vs fidelity and submission-time vs JCT from E2E CSV.

Produces a single figure with two vertically-stacked line charts, styled per
the Qonductor data-viz palette (blue sequential for fidelity, green sequential
for JCT).
"""


import argparse
import csv
from datetime import datetime, timezone

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from typing import List, Tuple

matplotlib.rcParams.update(
    {
        "font.family": "sans-serif",
        "font.sans-serif": ["system-ui", "DejaVu Sans", "sans-serif"],
        "axes.edgecolor": "#c3c2b7",
        "axes.grid": True,
        "axes.grid.axis": "y",
        "grid.color": "#e1e0d9",
        "grid.linewidth": 1.0,
        "xtick.color": "#898781",
        "ytick.color": "#898781",
        "axes.labelcolor": "#52514e",
        "axes.titlecolor": "#0b0b0b",
        "figure.facecolor": "#f9f9f7",
        "axes.facecolor": "#fcfcfb",
    }
)

# — palette slots from reference —
BLUE_450 = "#2a78d6"   # fidelity line colour
GREEN_400 = "#008300"  # JCT line colour
BLUE_300 = "#6da7ec"   # marker edge (lighter for contrast)
GREEN_200 = "#4cb04c"  # marker edge (lighter for contrast)


def load_data(path: str) -> Tuple[List[datetime], List[float], List[float]]:
    """Read CSV and return sorted (timestamps, fidelities, jcts)."""
    rows: List[Tuple[datetime, float, float]] = []
    with open(path, newline="") as fh:
        for rec in csv.DictReader(fh):
            ts = datetime.fromisoformat(rec["timestamp"].replace("Z", "+00:00"))
            fid = float(rec["fidelity"])
            jct = float(rec["JCT"])
            rows.append((ts, fid, jct))
    rows.sort(key=lambda r: r[0])
    return (
        [r[0] for r in rows],
        [r[1] for r in rows],
        [r[2] for r in rows],
    )


def build_base_ax(ax: plt.Axes, x: List[datetime], y: List[float],
                  *, color: str, marker_color: str, y_label: str,
                  title: str) -> None:
    """Draw one line chart on *ax* with mark specs from the method."""
    # — line: 2px, round join/cap (the "line" spec) —
    ax.plot(
        x, y,
        color=color,
        linewidth=2,
        solid_capstyle="round",
        solid_joinstyle="round",
        zorder=3,
    )
    # — markers: ≥ 8px, filled, with 2px surface ring —
    ax.scatter(
        x, y,
        s=52,                    # area — radius ≈ 4 px → diam ≈ 8 px
        facecolor=color,
        edgecolor="#fcfcfb",     # surface ring
        linewidths=2,
        zorder=4,
    )
    # — axis formatting —
    ax.set_title(title, fontsize=11, fontweight="bold", pad=10, loc="left")
    ax.set_ylabel(y_label, fontsize=9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    for label in ax.get_xticklabels():
        label.set_rotation(30)
        label.set_ha("right")
    # — recessive y-axis —
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(
        lambda v, _: f"{v:.4f}" if y_label == "Fidelity" else f"{v:.1f}s"
    ))
    # — remove top/right spines —
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#c3c2b7")
    ax.spines["bottom"].set_color("#c3c2b7")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plot Qonductor E2E metrics",
    )
    parser.add_argument(
        "input",
        nargs="?",
        default="results/batch_20260715_143255/metrics/quantum_job_jct_fidelity.csv",
        help="Path to E2E CSV (default: latest batch)",
    )
    parser.add_argument(
        "-o", "--output",
        default=None,
        help="Output PNG path (default: derived from input path)",
    )
    args = parser.parse_args()

    ts, fids, jcts = load_data(args.input)

    fig, (ax1, ax2) = plt.subplots(
        nrows=2, ncols=1,
        figsize=(10, 8),
        sharex=True,
        facecolor="#f9f9f7",
    )
    fig.subplots_adjust(hspace=0.28)

    build_base_ax(
        ax1, ts, fids,
        color=BLUE_450, marker_color=BLUE_300,
        y_label="Fidelity", title="Quantum Job Submission Time vs Fidelity",
    )
    build_base_ax(
        ax2, ts, jcts,
        color=GREEN_400, marker_color=GREEN_200,
        y_label="JCT", title="Quantum Job Submission Time vs JCT",
    )

    ax2.set_xlabel("Submission time (UTC)", fontsize=9, color="#52514e")

    # — tight layout and save —
    fig.tight_layout(pad=2.0)

    out = args.output
    if out is None:
        out = args.input.replace(".csv", ".png")
    fig.savefig(out, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor())
    print(f"Saved → {out}")
    plt.close(fig)


if __name__ == "__main__":
    main()
