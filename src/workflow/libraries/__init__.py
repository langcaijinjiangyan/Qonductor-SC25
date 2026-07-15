"""
Qonductor algorithm libraries.

Provides reusable quantum algorithms and classical error-mitigation
routines that users compose into hybrid workflows via the Qonductor API.

Based on Qonductor paper Section 5:
"The extensible libraries of commonly used quantum and classical
functions aid programmability."
"""

from src.workflow.libraries.quantum import QAOA, VQE, QFT, GroverSearch
from src.workflow.libraries.classical import ZNE, PEC, DD, REM, PauliTwirling

__all__ = [
    # Quantum
    "QAOA",
    "VQE",
    "QFT",
    "GroverSearch",
    # Classical
    "ZNE",
    "PEC",
    "DD",
    "REM",
    "PauliTwirling",
]
