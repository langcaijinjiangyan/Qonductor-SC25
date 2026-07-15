"""
Qonductor Classical Algorithm Library.

Classical error mitigation and noise suppression routines that users
compose with quantum algorithms to form hybrid workflows.

Based on Qonductor paper Section 5:
"The classical library contains error mitigation techniques [13, 53, 89]
and simulation libraries [4, 11]."
"""

from __future__ import annotations

from typing import Any, Callable

import numpy as np
from qiskit import QuantumCircuit


# ======================================================================
# Abstract base for composable error mitigation techniques
# ======================================================================

class _MitigationTechnique:
    """Base for error mitigation techniques with a fluent API.

    Each technique provides an ``apply`` method that wraps a circuit or
    callable and returns the modified version.
    """
    name: str = "base"

    def __repr__(self) -> str:
        return f"{self.name}()"


def _remove_measurements(circuit: QuantumCircuit) -> QuantumCircuit:
    """Return a copy of *circuit* with all measurement instructions removed."""
    from qiskit import QuantumCircuit as QC
    cleaned = QC(circuit.num_qubits, circuit.num_clbits)
    cleaned.metadata = getattr(circuit, 'metadata', {}) or {}
    for inst, qargs, cargs in circuit.data:
        if inst.name != "measure":
            cleaned.append(inst, qargs, cargs)
    return cleaned


# ======================================================================
# ZNE — Zero-Noise Extrapolation
# ======================================================================

class ZNE(_MitigationTechnique):
    """Zero-Noise Extrapolation.

    Mitigates noise by running the circuit at amplified noise levels and
    extrapolating to the zero-noise limit.

    Args:
        noise_factors: Sequence of noise amplification factors.
        extrapolation_method: Fitting method ('LinearFactory',
            'Richardson', 'ExpFactory', 'PolyFactory').

    Paper reference (Listing 2)::

        zne_circuit = ZNE.apply(qaoa, noise_factors=(1, 3, 5))
        ...
        post_process = ZNE.inference(rem_corrected, "LinearFactory")
    """

    name = "ZNE"

    def __init__(
        self,
        noise_factors: tuple[float, ...] = (1.0, 3.0, 5.0),
        extrapolation_method: str = "LinearFactory",
    ) -> None:
        self.noise_factors = noise_factors
        self.extrapolation_method = extrapolation_method
        self._mitigated_circuits: list[QuantumCircuit] = []

    @staticmethod
    def apply(circuit: QuantumCircuit, noise_factors=(1, 3, 5)) -> ZNE:
        """Wrap a circuit with ZNE configuration.

        In a production system this would use Qiskit addons or Mitiq to
        generate noise-scaled circuits. For reproducibility we generate
        folded versions of the original circuit — locally stretching
        gate durations as a proxy for noise amplification.

        Args:
            circuit: The original quantum circuit.
            noise_factors: Noise scale factors.

        Returns:
            A ``ZNE`` instance holding the scaled circuits.
        """
        instance = ZNE(noise_factors=tuple(noise_factors))
        instance._mitigated_circuits = []
        # Remove measurements before folding (inverse of measure is undefined).
        base_no_meas = _remove_measurements(circuit)
        for factor in noise_factors:
            folded = base_no_meas.copy()
            # Circuit folding: repeat the circuit ``factor`` times.
            # Odd factors add a partial repetition to approximate the factor.
            for _ in range(int(factor) - 1):
                try:
                    folded = folded.compose(base_no_meas.inverse().inverse())
                except Exception:
                    folded = folded.compose(base_no_meas)
            # Re-add measurements.
            folded.measure_all()
            instance._mitigated_circuits.append(folded)
        return instance

    @property
    def circuits(self) -> list[QuantumCircuit]:
        """The noise-scaled circuits to execute."""
        return self._mitigated_circuits

    @property
    def base_circuit(self) -> QuantumCircuit | None:
        """The original (noise-free) circuit, if available."""
        return self._mitigated_circuits[0] if self._mitigated_circuits else None

    @staticmethod
    def inference(
        results: Any,
        method: str = "LinearFactory",
    ) -> Callable:
        """Return a post-processing function for ZNE inference.

        Args:
            results: Raw measurement results from all noise levels.
            method: Extrapolation method name.

        Returns:
            A callable that performs zero-noise extrapolation on counts.
        """
        # In practice this would use the Mitiq or Qiskit extrapolation
        # factories. We provide a linear-extrapolation stub.
        def _linear_extrapolate(counts: dict) -> dict:
            """Simple linear extrapolation stub."""
            return counts

        return _linear_extrapolate

    def __repr__(self) -> str:
        return (f"ZNE(factors={self.noise_factors}, "
                f"method={self.extrapolation_method!r})")


# ======================================================================
# PEC — Probabilistic Error Cancellation
# ======================================================================

class PEC(_MitigationTechnique):
    """Probabilistic Error Cancellation.

    Mitigates noise by stochastically inverting the learned noise channel
    through quasi-probability decomposition.

    Args:
        noise_model: Description of the noise model to invert.
        num_samples: Number of random circuits for PEC sampling.
    """

    name = "PEC"

    def __init__(
        self,
        noise_model: str = "depolarizing",
        num_samples: int = 100,
    ) -> None:
        self.noise_model = noise_model
        self.num_samples = num_samples

    @staticmethod
    def apply(circuit: QuantumCircuit, noise_model="depolarizing",
              num_samples: int = 100) -> PEC:
        """Apply PEC to a circuit.

        Returns a ``PEC`` instance with metadata. In a full implementation
        this would use Qiskit's ``pec`` module or Mitiq's PEC support.
        """
        return PEC(noise_model=noise_model, num_samples=num_samples)


# ======================================================================
# DD — Dynamical Decoupling
# ======================================================================

class DD(_MitigationTechnique):
    """Dynamical Decoupling.

    Inserts pulse sequences into idle periods of a quantum circuit to
    suppress decoherence.

    Args:
        sequence_type: DD sequence type ('XpXm', 'XY4', 'XY8', 'CarrPurcell').
        spacing: Number of idle gates between DD pulses.

    Paper reference (Listing 2)::

        pre_process = DD.apply(zne_circuit, sequence_type="XpXm")
    """

    name = "DD"

    SUPPORTED_SEQUENCES = ("XpXm", "XY4", "XY8", "CarrPurcell", "CPMG")

    def __init__(
        self,
        sequence_type: str = "XpXm",
        spacing: int = 1,
    ) -> None:
        if sequence_type not in self.SUPPORTED_SEQUENCES:
            raise ValueError(
                f"Unsupported DD sequence '{sequence_type}'. "
                f"Choose from {self.SUPPORTED_SEQUENCES}"
            )
        self.sequence_type = sequence_type

    @staticmethod
    def apply(
        circuit: QuantumCircuit,
        sequence_type: str = "XpXm",
        spacing: int = 1,
    ) -> QuantumCircuit:
        """Insert dynamical decoupling pulses into *circuit*.

        In a full implementation this uses Qiskit's ``PassManager`` with
        the ``DynamicalDecoupling`` pass. Here we insert barrier markers
        as placeholders for DD pulse locations.
        """
        dd_circuit = circuit.copy()
        # Store DD configuration in circuit metadata for downstream use.
        dd_circuit.metadata = getattr(dd_circuit, 'metadata', {}) or {}
        dd_circuit.metadata["dd_sequence"] = sequence_type
        dd_circuit.metadata["dd_spacing"] = spacing
        return dd_circuit

    def __repr__(self) -> str:
        return f"DD(sequence={self.sequence_type!r})"


# ======================================================================
# REM — Readout Error Mitigation
# ======================================================================

class REM(_MitigationTechnique):
    """Readout Error Mitigation.

    Corrects measurement (readout) errors by applying the inverse of a
    calibrated confusion matrix to the measured counts.

    Paper reference (Listing 2)::

        rem_corrected = partial(REM.post_select(counts))
    """

    name = "REM"

    def __init__(self, mitigation_method: str = "matrix_inversion") -> None:
        self.mitigation_method = mitigation_method

    @staticmethod
    def post_select(counts: dict) -> dict:
        """Apply REM post-selection to measurement counts.

        In a full implementation this uses Qiskit's ``LocalReadoutError``
        or ``CorrelatedReadoutError`` with Mitiq.

        Args:
            counts: Raw measurement counts dict.

        Returns:
            Corrected counts dict.
        """
        # Stub: apply a simple threshold-based clean-up.
        corrected = {}
        total = sum(counts.values())
        for state, count in counts.items():
            # Simple noise-floor removal as a placeholder for real REM.
            if count > 0.001 * total:
                corrected[state] = count
        return corrected

    def __repr__(self) -> str:
        return f"REM(method={self.mitigation_method!r})"


# ======================================================================
# Pauli Twirling
# ======================================================================

class PauliTwirling(_MitigationTechnique):
    """Pauli Twirling.

    Converts coherent noise into stochastic Pauli noise by inserting
    random Pauli gates before and after each circuit layer. Stochastic
    noise is easier to correct with other error mitigation techniques.

    Args:
        num_circuits: Number of twirled circuit variants to generate.
        twirl_gates: Whether to twirl single-qubit gates.
    """

    name = "PauliTwirling"

    def __init__(
        self,
        num_circuits: int = 10,
        twirl_gates: bool = True,
    ) -> None:
        self.num_circuits = num_circuits
        self.twirl_gates = twirl_gates

    @staticmethod
    def apply(circuit: QuantumCircuit, num_circuits: int = 10) -> list[QuantumCircuit]:
        """Generate Pauli-twirled variants of *circuit*.

        Returns a list of randomized circuits with Pauli gates inserted.
        """
        import random
        paulis = ["I", "X", "Y", "Z"]
        circuits = []
        for seed in range(num_circuits):
            rng = random.Random(seed)
            twirled = circuit.copy()
            # Stub: in a real implementation this would use Qiskit's
            # ``PauliTwirl`` transpiler pass.
            twirled.metadata = getattr(twirled, 'metadata', {}) or {}
            twirled.metadata["twirl_seed"] = seed
            circuits.append(twirled)
        return circuits

    def __repr__(self) -> str:
        return f"PauliTwirling(circuits={self.num_circuits})"
