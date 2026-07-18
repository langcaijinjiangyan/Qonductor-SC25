"""Shared logging setup for Qonductor runtime entry points."""

from __future__ import annotations

import logging
import os


def configure_logging(
    env_var: str = "QONDUCTOR_LOG_LEVEL",
    default: str = "INFO",
) -> int:
    """Configure root logging from an environment variable.

    Returns the numeric level that was applied.
    """
    raw_level = os.environ.get(env_var, default).strip().upper()
    level = getattr(logging, raw_level, None)
    if not isinstance(level, int):
        level = getattr(logging, default.upper(), logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        force=True,
    )
    logging.getLogger("qiskit").setLevel(logging.WARNING)
    logging.getLogger("qiskit_ibm_provider").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return level
