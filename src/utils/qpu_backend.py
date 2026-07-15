"""
QPUBackend — Qiskit Backend constructed from QPU JSON calibration profiles.

Each QPU JSON file (in ``qpus/``) describes a 27-qubit superconducting
QPU with its coupling map, native gates, per-qubit gate errors, per-edge
CNOT errors, readout/reset errors, and T1/T2 times.

``QPUBackend`` reads such a file and exposes the full Qiskit ``BackendV2``
interface so that the existing ``MultiObjectiveScheduler`` can transpile
circuits, calculate fidelities, and run NSGA-II scheduling against it
without any code changes in the scheduler itself.

It also carries a ``node_name`` attribute so that after NSGA-II assigns a
job, a K8s Job with ``nodeAffinity`` can be created to dispatch actual
circuit execution to the specific quantum worker node that hosts this QPU.

Compatible with Qiskit ≥ 1.0 (tested with 2.3.1).

Based on Qonductor paper §6 (Resource Estimator) and §4.1 (QPU Device
Plugin extended resources).
"""

from __future__ import annotations

import json
import logging
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qiskit.circuit import Measure, Parameter, Reset
from qiskit.circuit.library import (
    CXGate,
    CZGate,
    ECRGate,
    IGate,
    RZGate,
    SXGate,
    XGate,
)
from qiskit.providers.backend import BackendV2
from qiskit.providers.options import Options
from qiskit.transpiler import CouplingMap, Target, InstructionProperties

logger = logging.getLogger(__name__)

# Qiskit instruction names for fidelity lookup classification
_SINGLE_QUBIT_INSTRS: set[str] = {"rz", "sx", "x", "id"}

# Qiskit instruction names that map to two-qubit edge error lookups
_TWO_QUBIT_INSTRS: set[str] = {"cx", "cz", "ecr"}

# Mapping from gate name strings (used in QPU JSON and fidelity lookups) to
# Gate *instances* required by ``Target.add_instruction()`` in qiskit ≥ 0.45.
# Parameterised gates receive a ``Parameter`` placeholder so the Target can
# recognise them without binding to a specific numeric angle.
_GATE_BY_NAME: dict[str, object] = {
    "rz": lambda: RZGate(Parameter("θ")),
    "sx": SXGate,
    "x": XGate,
    "id": IGate,
    "cx": CXGate,
    "cz": CZGate,
    "ecr": ECRGate,
}


# ---------------------------------------------------------------------------
# Compatibility wrappers for the scheduler's BackendProperties / BackendStatus
# expectations  (API-compatible with Qiskit 0.44 BackendProperties)
# ---------------------------------------------------------------------------

@dataclass
class _FidelityData:
    """Drop-in replacement for ``BackendProperties`` that answers
    ``gate_error()`` and ``readout_error()`` from pre-computed QPU JSON
    lookup tables."""

    _single_errors: list[dict[str, float]]          # [qubit_idx][gate_name] → error
    _edge_errors:  dict[tuple[int, int], dict[str, float]]  # (src,tgt)[gate_name]
    _readout_errs: list[float]                      # [qubit_idx] → error

    def gate_error(self, gate_name: str, qubits: list[int] | tuple[int, ...]) -> float:
        """Return the gate error for *gate_name* on *qubits*.

        Matches the Qiskit 0.44 ``BackendProperties.gate_error()`` signature.
        """
        if gate_name in _SINGLE_QUBIT_INSTRS:
            q = qubits[0] if isinstance(qubits, (list, tuple)) else qubits
            return self._single_errors[q].get(gate_name, 0.0)
        if gate_name in _TWO_QUBIT_INSTRS:
            key = (qubits[0], qubits[1]) if len(qubits) >= 2 else (0, 0)
            # Try both directions — coupling may be undirected in lookup
            edge_map = self._edge_errors.get(key)
            if edge_map is None:
                edge_map = self._edge_errors.get((key[1], key[0]), {})
            return edge_map.get(gate_name, 0.01)
        return 0.0

    def readout_error(self, qubit: int) -> float:
        """Return the readout error for *qubit*."""
        if 0 <= qubit < len(self._readout_errs):
            return self._readout_errs[qubit]
        return 0.02


@dataclass
class _QPUBackendStatus:
    """Simple object with ``pending_jobs`` — emulates Qiskit 0.44
    ``BackendStatus``."""
    backend_name: str
    backend_version: str
    operational: bool = True
    pending_jobs: int = 0
    status_msg: str = ""


# ===================================================================
# QPUBackend
# ===================================================================

class QPUBackend(BackendV2):
    """A Qiskit ``BackendV2`` built from a QPU JSON calibration profile.

    Implements the full BackendV2 interface required by the
    ``MultiObjectiveScheduler`` (transpile, fidelity calculation, etc.).

    Additionally provides the ``node_name`` attribute consumed by
    ``create_quantum_execution_job()`` for K8s node affinity.

    Args:
        qpu_json_path: Path to a QPU JSON profile file.
        node_name: K8s worker node that hosts this QPU.
    """

    def __init__(
        self,
        qpu_json_path: str | Path,
        node_name: str = "",
    ) -> None:
        qpu_json_path = Path(qpu_json_path)
        with open(qpu_json_path) as fh:
            data: dict[str, Any] = json.load(fh)
        self._init_from_data(data, qpu_json_path, node_name)

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        node_name: str = "",
    ) -> "QPUBackend":
        """Construct a QPUBackend from a parsed QPU JSON dictionary.

        Args:
            data: The full QPU JSON profile (same schema as the .json files).
            node_name: K8s worker node that hosts this QPU.

        Returns:
            A fully initialised ``QPUBackend`` instance.
        """
        backend = cls.__new__(cls)
        backend._init_from_data(data, None, node_name)
        return backend

    def _init_from_data(
        self,
        data: dict[str, Any],
        qpu_json_path: Path | None,
        node_name: str,
    ) -> None:
        """Shared initialisation from a parsed QPU JSON dict."""
        self._qpu_json_path = qpu_json_path
        self._qpu_data: dict[str, Any] = data

        hw: dict[str, Any] = self._qpu_data["hardware"]
        num_qubits: int = self._qpu_data["max_qubits"]
        name: str = self._qpu_data["name"]

        # ---- coupling map --------------------------------------------------
        raw_coupling: list[list[int]] = self._qpu_data["coupling_map"]
        self._coupling_map_obj = CouplingMap(raw_coupling)

        # ---- build Target from calibration data ----------------------------
        self._target = self._build_target(num_qubits, hw)

        # ---- initialise BackendV2 ------------------------------------------
        desc = (
            f"QPU backend from {self._qpu_json_path.name}"
            if self._qpu_json_path is not None
            else f"QPU backend '{name}' (from ConfigMap)"
        )
        super().__init__(
            name=name,
            backend_version=hw.get("calibration_version", "v1"),
            description=desc,
        )

        # ---- pre-compute fidelity lookup tables ----------------------------
        qubits_data: list[dict] = hw["qubits"]
        edges_data: list[dict] = hw["edges"]

        self._single_errors: list[dict[str, float]] = []
        for q in qubits_data:
            self._single_errors.append(dict(q.get("single_gate_error", {})))

        self._edge_errors: dict[tuple[int, int], dict[str, float]] = {}
        for edge in edges_data:
            key = (edge["source"], edge["target"])
            self._edge_errors[key] = {edge["gate"]: edge["error"]}

        self._readout_errs: list[float] = [
            q.get("readout_error", 0.02) for q in qubits_data
        ]

        # ---- K8s affinity --------------------------------------------------
        self.node_name: str = node_name

        # ---- patched attributes (mirrors benchmark.patch_fake_backend) ------
        self.pending_jobs: int = 0
        self.max_shots: int = 4000
        self.default_rep_delay: float = 0.0
        self._waiting_time: float = 0.0
        self._waiting_time_timestamp: dt.datetime = dt.datetime.now(dt.timezone.utc)
        self._processor_type: dict[str, Any] = {
            "family": hw.get("family", "superconducting"),
            "revision": hw.get("calibration_version", "v1"),
        }

        logger.debug("QPUBackend '%s' initialised: %d qubits, %d edges",
                     name, num_qubits, len(raw_coupling))

    # ------------------------------------------------------------------
    # Core BackendV2 properties
    # ------------------------------------------------------------------

    @property
    def target(self) -> Target:
        return self._target

    @property
    def coupling_map(self) -> CouplingMap:
        return self._coupling_map_obj

    @property
    def num_qubits(self) -> int:
        return self._target.num_qubits

    @property
    def max_circuits(self) -> int:
        return 1

    @classmethod
    def _default_options(cls) -> Options:
        return Options(shots=4000)

    @property
    def operation_names(self) -> set[str]:
        return set(self._target.operation_names)

    # ------------------------------------------------------------------
    # Build Target from JSON calibration
    # ------------------------------------------------------------------

    def _build_target(
        self,
        num_qubits: int,
        hw: dict[str, Any],
    ) -> Target:
        """Construct a Qiskit ``Target`` from the QPU JSON hardware section.

        Populates single-qubit gates, the native two-qubit gate, measurement,
        and reset with per-qubit / per-edge error rates and durations.
        """
        target = Target(
            num_qubits=num_qubits,
            description=f"QPU target for {self._qpu_data['name']}",
        )

        qubits_data: list[dict] = hw["qubits"]
        edges_data: list[dict] = hw["edges"]

        # -- single-qubit gates ------------------------------------------
        for gate_name in hw["native_single_qubit_gates"]:
            gate_factory = _GATE_BY_NAME.get(gate_name)
            if gate_factory is None:
                logger.warning("Unknown single-qubit gate %r — skipping", gate_name)
                continue
            props: dict[tuple[int, ...], InstructionProperties] = {}
            for i, q in enumerate(qubits_data):
                err = q.get("single_gate_error", {}).get(gate_name, 0.0)
                dur = q.get("single_gate_duration_ns", {}).get(gate_name, 35.0)
                props[(i,)] = InstructionProperties(
                    duration=dur * 1e-9,   # ns → seconds
                    error=err,
                )
            target.add_instruction(
                instruction=gate_factory() if callable(gate_factory) else gate_factory,
                properties=props,
                name=gate_name,
            )

        # -- two-qubit gate ----------------------------------------------
        two_gate_name: str = hw["native_two_qubit_gate"]
        two_gate_factory = _GATE_BY_NAME.get(two_gate_name)
        if two_gate_factory is None:
            logger.warning("Unknown two-qubit gate %r — skipping", two_gate_name)
        else:
            props: dict[tuple[int, ...], InstructionProperties] = {}
            for edge in edges_data:
                key = (edge["source"], edge["target"])
                props[key] = InstructionProperties(
                    duration=edge.get("duration_ns", 300.0) * 1e-9,
                    error=edge.get("error", 0.01),
                )
                # Also register the reverse direction (undirected coupling)
                rev_key = (edge["target"], edge["source"])
                if rev_key not in props:
                    props[rev_key] = InstructionProperties(
                        duration=edge.get("duration_ns", 300.0) * 1e-9,
                        error=edge.get("error", 0.01),
                    )
            target.add_instruction(
                instruction=two_gate_factory() if callable(two_gate_factory) else two_gate_factory,
                properties=props,
                name=two_gate_name,
            )

        # -- measurement -------------------------------------------------
        meas_props: dict[tuple[int, ...], InstructionProperties] = {}
        for i, q in enumerate(qubits_data):
            meas_props[(i,)] = InstructionProperties(
                duration=q.get("readout_duration_ns", 5000.0) * 1e-9,
                error=q.get("readout_error", 0.02),
            )
        target.add_instruction(
            instruction=Measure(),
            properties=meas_props,
            name="measure",
        )

        # -- reset -------------------------------------------------------
        reset_props: dict[tuple[int, ...], InstructionProperties] = {}
        for i, q in enumerate(qubits_data):
            reset_props[(i,)] = InstructionProperties(
                duration=q.get("reset_duration_ns", 7000.0) * 1e-9,
                error=q.get("reset_error", 0.01),
            )
        target.add_instruction(
            instruction=Reset(),
            properties=reset_props,
            name="reset",
        )

        return target

    # ------------------------------------------------------------------
    # Methods required by MultiObjectiveScheduler
    # ------------------------------------------------------------------

    def properties(self) -> _FidelityData:
        """Return fidelity data compatible with the scheduler's
        ``_calculate_fidelity()`` method.

        The returned object provides ``gate_error(gate_name, qubits)``
        and ``readout_error(qubit)``, matching the Qiskit 0.44
        ``BackendProperties`` API used by the scheduler.
        """
        return _FidelityData(
            _single_errors=self._single_errors,
            _edge_errors=self._edge_errors,
            _readout_errs=self._readout_errs,
        )

    def status(self) -> _QPUBackendStatus:
        return _QPUBackendStatus(
            backend_name=self.name,
            backend_version=self.backend_version,
            operational=True,
            pending_jobs=self.pending_jobs,
            status_msg="",
        )

    def update_waiting_time(self, job_waiting_time: float) -> None:
        """Accumulate simulated queue waiting time (used by SchedulingManager)."""
        now = dt.datetime.now(dt.timezone.utc)
        elapsed = (now - self._waiting_time_timestamp).total_seconds()
        self._waiting_time = max(0.0, self._waiting_time - elapsed)
        self._waiting_time += job_waiting_time
        self._waiting_time_timestamp = now

    def get_waiting_time(self) -> float:
        """Return the current simulated queue waiting time in seconds."""
        now = dt.datetime.now(dt.timezone.utc)
        elapsed = (now - self._waiting_time_timestamp).total_seconds()
        self._waiting_time = max(0.0, self._waiting_time - elapsed)
        self._waiting_time_timestamp = now
        return self._waiting_time

    @property
    def processor_type(self) -> dict[str, Any]:
        return self._processor_type

    @processor_type.setter
    def processor_type(self, value: dict[str, Any]) -> None:
        self._processor_type = value

    # ------------------------------------------------------------------
    # run — required by BackendV2 ABC
    # ------------------------------------------------------------------

    def run(self, run_input, **options):
        """Stub: QPU execution is dispatched via K8s Job, not run locally."""
        raise NotImplementedError(
            "QPUBackend.run() is a stub — use K8s Job dispatch for execution"
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (f"QPUBackend(name={self.name!r}, qubits={self.num_qubits}, "
                f"node={self.node_name!r})")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_GENERIC_DEFAULT_QUBIT = {
    "single_gate_duration_ns": {"rz": 0.0, "sx": 35.5, "x": 35.7, "id": 35.4},
    "single_gate_error": {"rz": 0.0, "sx": 0.00022, "x": 0.0003, "id": 0.00024},
    "readout_duration_ns": 5350.0,
    "readout_error": 0.02,
    "reset_duration_ns": 7340.0,
    "reset_error": 0.01,
    "t1_ns": 120000.0,
    "t2_ns": 90000.0,
}
_GENERIC_DEFAULT_EDGE = {
    "gate": "cx",
    "error": 0.01,
    "duration_ns": 300.0,
}


def _make_generic_qpu_data(name: str, num_qubits: int) -> dict:
    """Build a minimal QPU JSON dict suitable for ``QPUBackend.from_dict()``.

    The resulting backend uses a linear coupling map and default noise
    parameters — useful as a fallback when no real QPU profile is available.
    """
    coupling_map = [[i, i + 1] for i in range(num_qubits - 1)]
    return {
        "name": name,
        "max_qubits": num_qubits,
        "coupling_map": coupling_map,
        "hardware": {
            "calibration_version": "generic-fallback-v1",
            "family": "generic",
            "native_single_qubit_gates": ["rz", "sx", "x", "id"],
            "native_two_qubit_gate": "cx",
            "qubits": [dict(_GENERIC_DEFAULT_QUBIT) for _ in range(num_qubits)],
            "edges": [
                {"source": s, "target": t, **_GENERIC_DEFAULT_EDGE}
                for s, t in coupling_map
            ],
        },
    }


# Patch the class method onto QPUBackend so callers can use
# ``QPUBackend.generic(num_qubits=5)`` as a drop-in replacement for the
# removed ``GenericBackendV2(num_qubits=5)``.
def _generic(cls, num_qubits: int, name: str = "", node_name: str = ""):
    """Create a generic *num_qubits*-qubit ``QPUBackend``.

    Args:
        num_qubits: Number of qubits.
        name: Backend name (auto-generated if empty).
        node_name: Optional K8s node name for affinity.

    Returns:
        A fully initialised ``QPUBackend`` with a linear coupling map
        and default noise parameters.
    """
    backend_name = name or f"generic_{num_qubits}q"
    data = _make_generic_qpu_data(backend_name, num_qubits)
    return QPUBackend.from_dict(data, node_name=node_name)


QPUBackend.generic = classmethod(_generic)


def load_qpu_backends(
    qpu_dir: str | Path = "qpus",
    node_mapping: dict[str, str] | None = None,
) -> list[QPUBackend]:
    """Load all QPU JSON profiles from *qpu_dir* as ``QPUBackend`` objects.

    Args:
        qpu_dir: Directory containing ``*.json`` QPU profile files.
        node_mapping: Optional ``{qpu_name: k8s_node_name}`` dict.  When
            provided, each backend's ``node_name`` is set accordingly.

    Returns:
        A list of ``QPUBackend`` instances, sorted by name.
    """
    qpu_dir = Path(qpu_dir)
    backends: list[QPUBackend] = []

    for json_file in sorted(qpu_dir.glob("*.json")):
        try:
            with open(json_file) as fh:
                data = json.load(fh)
            qpu_name = data["name"]
            node_name = ""
            if node_mapping:
                node_name = node_mapping.get(qpu_name, "")
            backend = QPUBackend(str(json_file), node_name=node_name)
            backends.append(backend)
            logger.info("Loaded QPU backend '%s' from %s (node=%s)",
                        qpu_name, json_file.name, node_name or "<none>")
        except Exception:
            logger.exception("Failed to load QPU profile %s", json_file)

    return sorted(backends, key=lambda b: b.name)
