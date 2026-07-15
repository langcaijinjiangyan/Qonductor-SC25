import math

from qiskit import QuantumCircuit
from qiskit.circuit.library import PhaseGate, UGate


def replace_deprecated_gates(circuit: QuantumCircuit) -> QuantumCircuit:
    """
    Replace deprecated gates in a circuit
    :param circuit: Circuit to be modified
    :return: Modified circuit
    """
    instructions = []

    for instruction, qargs, cargs in circuit.data:
        if instruction.name == "u1":
            instructions.append(
                (PhaseGate(instruction.params[0]), qargs, cargs)
            )
        elif instruction.name == "u2":
            instructions.append(
                (UGate(math.pi / 2, *instruction.params), qargs, cargs)
            )
        elif instruction.name == "u3":
            instructions.append((UGate(*instruction.params), qargs, cargs))
        else:
            instructions.append((instruction, qargs, cargs))

    circuit.data = instructions

    return circuit
