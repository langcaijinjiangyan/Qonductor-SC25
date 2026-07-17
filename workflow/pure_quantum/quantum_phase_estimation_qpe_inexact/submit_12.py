#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

WORKLOAD_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = WORKLOAD_DIR
while PROJECT_ROOT != PROJECT_ROOT.parent and not (PROJECT_ROOT / "src" / "workflow").exists():
    PROJECT_ROOT = PROJECT_ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT / "workflow" / "_lib"))

from qslurm_workflow import pure_quantum_main


if __name__ == "__main__":
    pure_quantum_main(
        workload_dir=WORKLOAD_DIR,
        spec_file="spec_12.json",
        source_rel="qslurm_workloads/pure_quantum/quantum_phase_estimation_qpe_inexact/spec_12.json",
    )
