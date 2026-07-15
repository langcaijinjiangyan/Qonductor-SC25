"""
Qonductor Quantum Algorithm Library.

Pre-built quantum algorithms that users can import and compose into
hybrid workflows. Each algorithm returns a Qiskit QuantumCircuit or
a callable that generates one.

Based on Qonductor paper Section 5:
"The quantum library includes state-of-the-art quantum algorithms such
as the Variational Quantum Eigensolver (VQE), the Quantum Approximate
Optimization Algorithm (QAOA), and the Quantum Fourier Transform (QFT),
among others."
"""

from __future__ import annotations

import numpy as np
from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister
from qiskit.circuit import Parameter


# ---------------------------------------------------------------------------
# QAOA — Quantum Approximate Optimization Algorithm
# ---------------------------------------------------------------------------

class QAOA:
    """Quantum Approximate Optimization Algorithm.

    Constructs a parameterized QAOA circuit for a given cost Hamiltonian
    encoded as a list of Pauli-Z interaction terms.

    Example usage (matching Listing 2 in the paper)::

        qaoa = QAOA(qubits=10, optimizer='COBYLA')
        circuit = qaoa.build_circuit()

    Args:
        qubits: Number of qubits for the algorithm.
        depth: Number of QAOA layers (p).
        optimizer: Classical optimizer name (informational, for metadata).
    """

    def __init__(
        self,
        qubits: int = 4,
        depth: int = 1,
        optimizer: str = "COBYLA",
    ) -> None:
        self.qubits = qubits
        self.depth = depth
        self.optimizer = optimizer
        self._gammas = [Parameter(f"γ_{i}") for i in range(depth)]
        self._betas = [Parameter(f"β_{i}") for i in range(depth)]

    def build_circuit(self) -> QuantumCircuit:
        """Build the QAOA circuit.

        Returns:
            A parameterized ``QuantumCircuit`` with gamma and beta
            parameters for each layer.
        """
        qr = QuantumRegister(self.qubits, "q")
        cr = ClassicalRegister(self.qubits, "c")
        circuit = QuantumCircuit(qr, cr)

        # Initial superposition.
        circuit.h(qr)

        # QAOA layers.
        for layer in range(self.depth):
            # Cost unitary: ZZ interactions on a ring.
            for i in range(self.qubits):
                j = (i + 1) % self.qubits
                circuit.cx(qr[i], qr[j])
                circuit.rz(2 * self._gammas[layer], qr[j])
                circuit.cx(qr[i], qr[j])
            # Mixer unitary.
            for i in range(self.qubits):
                circuit.rx(2 * self._betas[layer], qr[i])

        circuit.measure(qr, cr)
        circuit.name = f"qaoa_{self.qubits}q_p{self.depth}"
        return circuit

    def __repr__(self) -> str:
        return (f"QAOA(qubits={self.qubits}, depth={self.depth}, "
                f"optimizer={self.optimizer!r})")


# ---------------------------------------------------------------------------
# VQE — Variational Quantum Eigensolver
# ---------------------------------------------------------------------------

class VQE:
    """Variational Quantum Eigensolver.

    Constructs a parameterized ansatz circuit suitable for VQE.

    Args:
        num_qubits: Number of qubits.
        depth: Number of ansatz repetitions.
        ansatz_type: Type of ansatz circuit ('efficient_su2', 'two_local', 'real_amplitudes').
    """

    def __init__(
        self,
        num_qubits: int = 4,
        depth: int = 2,
        ansatz_type: str = "efficient_su2",
    ) -> None:
        self.num_qubits = num_qubits
        self.depth = depth
        self.ansatz_type = ansatz_type

    def build_circuit(self) -> QuantumCircuit:
        """Build the VQE ansatz circuit."""
        from qiskit.circuit.library import EfficientSU2, RealAmplitudes, TwoLocal

        ansatz_map = {
            "efficient_su2": EfficientSU2,
            "real_amplitudes": RealAmplitudes,
            "two_local": TwoLocal,
        }
        ansatz_cls = ansatz_map.get(self.ansatz_type, EfficientSU2)

        if self.ansatz_type == "two_local":
            ansatz = ansatz_cls(
                self.num_qubits, "ry", "cx", reps=self.depth,
                entanglement="linear",
            )
        else:
            ansatz = ansatz_cls(
                self.num_qubits, reps=self.depth, entanglement="linear",
            )

        ansatz.measure_all()
        ansatz.name = f"vqe_{self.ansatz_type}_{self.num_qubits}q"
        return ansatz

    def __repr__(self) -> str:
        return (f"VQE(qubits={self.num_qubits}, depth={self.depth}, "
                f"ansatz={self.ansatz_type!r})")


# ---------------------------------------------------------------------------
# QFT — Quantum Fourier Transform
# ---------------------------------------------------------------------------

class QFT:
    """Quantum Fourier Transform.

    Args:
        num_qubits: Number of qubits.
        inverse: If True, build the inverse QFT.
    """

    def __init__(self, num_qubits: int = 4, inverse: bool = False) -> None:
        self.num_qubits = num_qubits
        self.inverse = inverse

    def build_circuit(self) -> QuantumCircuit:
        """Build the QFT circuit."""
        qr = QuantumRegister(self.num_qubits, "q")
        cr = ClassicalRegister(self.num_qubits, "c")
        circuit = QuantumCircuit(qr, cr)

        for i in range(self.num_qubits):
            circuit.h(qr[i])
            for j in range(i + 1, self.num_qubits):
                angle = np.pi / (2 ** (j - i))
                if self.inverse:
                    angle = -angle
                circuit.cp(angle, qr[j], qr[i])

        # Swap qubits to get correct bit ordering.
        for i in range(self.num_qubits // 2):
            circuit.swap(qr[i], qr[self.num_qubits - i - 1])

        circuit.measure(qr, cr)
        direction = "iqft" if self.inverse else "qft"
        circuit.name = f"{direction}_{self.num_qubits}q"
        return circuit

    def __repr__(self) -> str:
        direction = "inverse" if self.inverse else "forward"
        return f"QFT(qubits={self.num_qubits}, direction={direction})"


# ---------------------------------------------------------------------------
# Grover's Search
# ---------------------------------------------------------------------------

class GroverSearch:
    """Grover's Search Algorithm.

    Constructs a Grover's search circuit for a given number of qubits
    and marked element(s).

    Args:
        num_qubits: Number of qubits for the search space.
        marked_elements: Integer indices of the marked (target) elements.
        iterations: Number of Grover iterations. If None, uses
            optimal count floor(pi/4 * sqrt(N/M)).
    """

    def __init__(
        self,
        num_qubits: int = 4,
        marked_elements: list[int] | None = None,
        iterations: int | None = None,
    ) -> None:
        self.num_qubits = num_qubits
        self.marked_elements = marked_elements or [0]
        self.iterations = iterations

    def build_circuit(self) -> QuantumCircuit:
        """Build the Grover's search circuit."""
        from qiskit.circuit.library import GroverOperator, MCMT
        from qiskit.circuit.library import ZGate

        N = 2 ** self.num_qubits
        n_iters = self.iterations or max(1, int(np.floor(np.pi / 4 * np.sqrt(N))))

        qr = QuantumRegister(self.num_qubits, "q")
        cr = ClassicalRegister(self.num_qubits, "c")
        circuit = QuantumCircuit(qr, cr)

        # Initialize superposition.
        circuit.h(qr)

        # Oracle and diffusion iterations.
        for _ in range(n_iters):
            # Oracle: mark the target element(s).
            self._apply_oracle(circuit, qr)
            # Diffusion operator.
            circuit.h(qr)
            circuit.x(qr)
            circuit.h(qr[-1])
            circuit.mcx(list(qr[:-1]), qr[-1])
            circuit.h(qr[-1])
            circuit.x(qr)
            circuit.h(qr)

        circuit.measure(qr, cr)
        circuit.name = f"grover_{self.num_qubits}q"
        return circuit

    def _apply_oracle(
        self, circuit: QuantumCircuit, qr: QuantumRegister,
    ) -> None:
        """Flip the phase of marked elements."""
        for marked in self.marked_elements:
            binary = format(marked, f"0{self.num_qubits}b")
            # Flip 0-bits to 1-bits for multi-controlled-Z.
            for i, bit in enumerate(binary):
                if bit == "0":
                    circuit.x(qr[i])
            circuit.h(qr[-1])
            if self.num_qubits > 1:
                circuit.mcx(list(qr[:-1]), qr[-1])
            else:
                circuit.z(qr[0])
            circuit.h(qr[-1])
            # Undo bit flips.
            for i, bit in enumerate(binary):
                if bit == "0":
                    circuit.x(qr[i])

    def __repr__(self) -> str:
        return (f"GroverSearch(qubits={self.num_qubits}, "
                f"marked={self.marked_elements})")
