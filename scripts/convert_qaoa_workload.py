#!/usr/bin/env python3
"""CLI wrapper for converting the QAOA Slurm workload to Qonductor."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.workflow.qaoa_conversion import main


if __name__ == "__main__":
    main()
