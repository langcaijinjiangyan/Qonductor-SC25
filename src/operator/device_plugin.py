"""
QPU Device Plugin — registers quantum.ibm.com/qpu as a K8s extended resource.

Based on Qonductor paper §4.1:
"The device manager periodically queries the classical nodes and QPUs to
get static (e.g., number of cores/qubits) and the current dynamic
information (e.g., queue sizes, utilization, calibration data), and
updates the system monitor accordingly."

In the K8s device plugin framework, this plugin would:
1. Register with kubelet via the Device Plugin API (Unix socket)
2. Report available QPU devices under quantum.ibm.com/qpu
3. Periodically refresh calibration data from the quantum cloud

Our implementation:
- In K8s mode: patches Node status.capacity to declare QPU resources
- In local mode: maintains a registry of simulated QPU backends
- Provides calibration data via ConfigMap writes
"""

from __future__ import annotations

import json
import os
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from src.operator.k8s_client import K8sClient

logger = logging.getLogger(__name__)


@dataclass
class QPUInfo:
    """Static and dynamic info for a single QPU."""
    name: str
    num_qubits: int
    technology: str = "superconducting"
    model: str = ""
    # Dynamic info (periodically refreshed).
    pending_jobs: int = 0
    t1_us: float = 100.0          # T1 relaxation time (μs)
    t2_us: float = 80.0           # T2 dephasing time (μs)
    gate_error_cx: float = 0.01   # CNOT gate error rate
    gate_error_sx: float = 0.001  # SX (√X) gate error rate
    readout_error: float = 0.02   # Measurement readout error
    last_calibration: str = ""


class QPUDevicePlugin:
    """Simulates a K8s Device Plugin for quantum.ibm.com/qpu resources.

    On startup:
    1. Discovers available QPUs (simulated backends or real IBM cloud).
    2. Registers them as extended resources on worker nodes.
    3. Starts a background thread that periodically refreshes calibration
       data and updates the System Monitor (K8s ConfigMaps / CR status).

    Paper alignment:
        - quantum.ibm.com/qpu → K8s extended resource
        - Calibration data → ConfigMap qpu-calibration-<name>
        - Node health → K8s Node conditions
    """

    RESOURCE_NAME = "quantum.ibm.com/qpu"
    CALIBRATION_REFRESH_INTERVAL = 300  # seconds (~5 min)

    def __init__(self, mode: str = "local") -> None:
        self.mode = mode
        self.k8s = K8sClient(mode=mode)
        self._qpus: dict[str, QPUInfo] = {}
        self._node_assignments: dict[str, str] = {}  # qpu_name → node_name
        self._stop_event = threading.Event()
        self._refresh_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_qpu(self, qpu: QPUInfo, node_name: str = "worker") -> None:
        """Register a QPU and associate it with a worker node."""
        self._qpus[qpu.name] = qpu
        self._node_assignments[qpu.name] = node_name
        logger.info("Registered QPU '%s' (%d qubits, %s) on node '%s'",
                    qpu.name, qpu.num_qubits, qpu.technology, node_name)

    def register_fake_backends_as_qpus(self, backends=None,
                                       node_name: str = "worker") -> int:
        """Register Qiskit FakeBackends as QPU resources.

        Returns the number of QPUs registered.
        """
        if backends is None:
            try:
                from src.utils.benchmark import get_fake_backends
                backends = get_fake_backends()[:8]
            except ImportError:
                # Fallback when mqt.bench is not installed.
                from src.utils.qpu_backend import QPUBackend
                backends = []
                for i, nq in enumerate([5, 7, 16, 27]):
                    be = QPUBackend.generic(num_qubits=nq, name=f"fake_backend_{i}")
                    be.name = f"fake_backend_{i}"
                    be.processor_type = {"family": "falcon", "revision": "r5.11"}
                    # num_qubits is a read-only property; set via constructor.
                    backends.append(be)

        count = 0
        for backend in backends:
            pt = getattr(backend, 'processor_type', {}) or {}
            qpu = QPUInfo(
                name=backend.name,
                num_qubits=backend.num_qubits,
                technology=pt.get("family", "superconducting"),
                model=pt.get("family", "") + str(pt.get("revision", "")),
                last_calibration=_now_iso(),
            )
            self.register_qpu(qpu, node_name=node_name)
            count += 1

        return count

    def register_qpus_from_json_dir(
        self, qpu_dir: str = "qpus", node_name: str = "worker",
    ) -> int:
        """Register QPUs from JSON calibration profile files.

        Reads every ``*.json`` file in *qpu_dir*, extracts the QPU name
        and metadata, and registers each QPU with this device plugin.  Profiles
        generated by ``docker/multi-host/deploy-cluster.sh`` may include
        ``qonductor_node_name``; when present, this plugin only registers
        profiles targeted at its own K8s node.  This lets several nodes share
        the same hostPath while still using unique QPU instance names.
        Also publishes the full QPU JSON as a ConfigMap so the operator
        can construct accurate ``QPUBackend`` objects for NSGA-II.

        K8s node labels (``qonductor.io/backend-<name>=true``) are emitted
        by :meth:`advertise_resources` and during cluster setup
        (``docker/setup-cluster.sh``).

        Returns the number of QPUs registered.
        """
        qpu_dir = Path(qpu_dir)
        count = 0
        for json_file in sorted(qpu_dir.glob("*.json")):
            try:
                with open(json_file) as fh:
                    data = json.load(fh)
                target_node = data.get("qonductor_node_name")
                if target_node and target_node != node_name:
                    logger.debug(
                        "Skipping QPU profile %s for node '%s' on node '%s'",
                        json_file.name, target_node, node_name,
                    )
                    continue
                qpu = QPUInfo(
                    name=data["name"],
                    num_qubits=data["max_qubits"],
                    technology="superconducting",
                    last_calibration=_now_iso(),
                )
                self.register_qpu(qpu, node_name=node_name)
                # Publish full QPU JSON as ConfigMap for the operator.
                self._publish_qpu_profile_configmap(data, node_name)
                count += 1
                logger.info(
                    "Registered QPU '%s' on node '%s'",
                    data["name"], node_name,
                )
            except Exception:
                logger.exception(
                    "Failed to register QPU from %s", json_file,
                )
        return count

    def _publish_qpu_profile_configmap(
        self, qpu_data: dict, node_name: str,
    ) -> None:
        """Create or update a ConfigMap containing the full QPU JSON profile.

        The ConfigMap is named ``qpu-profile-<name>`` and labelled so the
        operator can discover it cluster-wide via
        ``list_configmaps("app=qonductor,component=qpu-profile")``.
        """
        qpu_name = qpu_data["name"]
        # K8s ConfigMap names must be valid RFC 1123 subdomains (no underscores).
        cm_name = f"qpu-profile-{qpu_name.replace('_', '-')}"
        cm_data = {
            "profile": json.dumps(qpu_data),
            "node_name": node_name,
            "qpu_name": qpu_name,
            "num_qubits": str(qpu_data.get("max_qubits", "")),
        }
        cm_labels = {"app": "qonductor", "component": "qpu-profile"}
        try:
            # Try update first (ConfigMap already exists from a previous run).
            self.k8s.update_configmap(cm_name, cm_data, labels=cm_labels)
            logger.debug("Updated ConfigMap '%s'", cm_name)
        except Exception:
            # ConfigMap doesn't exist yet — create it.
            try:
                self.k8s.create_configmap(cm_name, cm_data, labels=cm_labels)
                logger.info("Created ConfigMap '%s' for QPU '%s'", cm_name, qpu_name)
            except Exception:
                logger.exception(
                    "Failed to create ConfigMap '%s'", cm_name,
                )

    # ------------------------------------------------------------------
    # K8s integration
    # ------------------------------------------------------------------

    def advertise_resources(self) -> None:
        """Announce QPU capacity on each worker node.

        In a real K8s cluster this is done via the Device Plugin API
        (Unix socket with kubelet). Here we patch the Node status directly
        via the K8s API.
        """
        # Aggregate QPU counts per node.
        node_counts: dict[str, int] = {}
        for qpu_name, node_name in self._node_assignments.items():
            node_counts[node_name] = node_counts.get(node_name, 0) + 1

        for node_name, count in node_counts.items():
            resources = {self.RESOURCE_NAME: str(count)}
            try:
                self.k8s.patch_node_extended_resources(node_name, resources)
                logger.info("Advertised %s=%d on node '%s'",
                            self.RESOURCE_NAME, count, node_name)
            except Exception as exc:
                logger.warning("Failed to advertise QPU on '%s': %s",
                              node_name, exc)

        # Also register nodes in local store (with backend labels for
        # nodeAffinity used by create_quantum_execution_job).
        for node_name in node_counts:
            # Build node labels: generic + per-backend
            node_labels: dict[str, str] = {
                "qonductor.io/qpu": "true",
                "qonductor.io/node-type": "quantum",
            }
            for qpu_name, qnode_name in self._node_assignments.items():
                if qnode_name == node_name:
                    node_labels[f"qonductor.io/backend-{qpu_name}"] = "true"

            self.k8s.register_node(node_name, {
                "metadata": {
                    "name": node_name,
                    "labels": node_labels,
                },
                "status": {
                    "capacity": {self.RESOURCE_NAME: str(node_counts[node_name])},
                    "allocatable": {self.RESOURCE_NAME: str(node_counts[node_name])},
                    "conditions": [
                        {"type": "Ready", "status": "True"},
                    ],
                },
            })

    # ------------------------------------------------------------------
    # Calibration refresh
    # ------------------------------------------------------------------

    def start_calibration_refresh(self) -> None:
        """Start a background thread to periodically refresh calibration data."""
        self._refresh_thread = threading.Thread(
            target=self._calibration_loop, daemon=True,
        )
        self._refresh_thread.start()
        logger.info("Calibration refresh thread started (interval=%ds)",
                    self.CALIBRATION_REFRESH_INTERVAL)

    def stop(self) -> None:
        self._stop_event.set()
        if self._refresh_thread:
            self._refresh_thread.join(timeout=5)

    def _calibration_loop(self) -> None:
        """Background loop: fetch calibration data, update System Monitor."""
        while not self._stop_event.is_set():
            self._refresh_calibration_data()
            self._stop_event.wait(timeout=self.CALIBRATION_REFRESH_INTERVAL)

    def _refresh_calibration_data(self) -> None:
        """Fetch the latest calibration data for all managed QPUs.

        In production this queries the IBM Quantum cloud API;
        for simulation we use FakeBackend properties.
        """
        for name, qpu in self._qpus.items():
            # Simulate calibration drift.
            qpu.t1_us *= np.random.uniform(0.95, 1.05)
            qpu.t2_us *= np.random.uniform(0.95, 1.05)
            qpu.gate_error_cx *= np.random.uniform(0.90, 1.10)
            qpu.readout_error *= np.random.uniform(0.90, 1.10)
            qpu.last_calibration = _now_iso()

            calib_data = {
                "name": qpu.name,
                "num_qubits": str(qpu.num_qubits),
                "technology": qpu.technology,
                "model": qpu.model,
                "pending_jobs": str(qpu.pending_jobs),
                "t1_us": f"{qpu.t1_us:.2f}",
                "t2_us": f"{qpu.t2_us:.2f}",
                "gate_error_cx": f"{qpu.gate_error_cx:.6f}",
                "gate_error_sx": f"{qpu.gate_error_sx:.6f}",
                "readout_error": f"{qpu.readout_error:.6f}",
                "last_calibration": qpu.last_calibration,
            }

            # Store as K8s ConfigMap (mirrors the System Monitor datastore).
            cm_name = f"qpu-calibration-{name}"
            try:
                self.k8s.update_configmap(cm_name, calib_data)
            except Exception:
                try:
                    self.k8s.create_configmap(cm_name, calib_data)
                except Exception:
                    logger.debug("Failed to store calibration ConfigMap '%s'", cm_name)

            logger.debug("Refreshed calibration for QPU '%s'", name)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_qpu_info(self, qpu_name: str) -> QPUInfo | None:
        return self._qpus.get(qpu_name)

    def list_qpus(self) -> list[QPUInfo]:
        return list(self._qpus.values())

    def get_calibration(self, qpu_name: str) -> dict | None:
        cm = self.k8s.get_configmap(f"qpu-calibration-{qpu_name}")
        return cm.get("data") if cm else None

    @property
    def resource_name(self) -> str:
        return self.RESOURCE_NAME


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ===================================================================
# Standalone convenience
# ===================================================================

def create_default_device_plugin(mode: str = "local") -> QPUDevicePlugin:
    """Factory: create a device plugin populated with fake backends."""
    plugin = QPUDevicePlugin(mode=mode)
    plugin.register_fake_backends_as_qpus()
    plugin.advertise_resources()
    return plugin


# ===================================================================
# Module entry point used by docker/Dockerfile.device-plugin
# ===================================================================

def main() -> None:
    mode = os.environ.get("QONDUCTOR_MODE", "local")
    node_name = os.environ.get("NODE_NAME", "worker")
    qpu_dir = os.environ.get("QPU_PROFILES_DIR", "/etc/qonductor/qpus")

    plugin = QPUDevicePlugin(mode=mode)
    has_profiles = any(Path(qpu_dir).glob("*.json"))
    count = plugin.register_qpus_from_json_dir(qpu_dir, node_name=node_name)
    if count == 0 and not has_profiles:
        plugin.register_fake_backends_as_qpus(node_name=node_name)
    plugin.advertise_resources()
    plugin.start_calibration_refresh()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        plugin.stop()


if __name__ == "__main__":
    main()
