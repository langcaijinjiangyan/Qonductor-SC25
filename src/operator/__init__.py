"""
Qonductor Operator — Kubernetes-native controllers.

Runs the control plane of Qonductor as a set of K8s controllers that watch
Custom Resources (HybridWorkflow, QuantumJob) and drive workflow execution.

Components:
    HybridWorkflowController  — Job Manager: watches HybridWorkflow CRs,
                                splits into K8s Jobs (classical) and
                                QuantumJob CRs (quantum).
    QuantumSchedulerController — Watches QuantumJob CRs, collects pending
                                jobs, runs NSGA-II multi-objective
                                scheduling, assigns QPUs.
    QPUDevicePlugin           — Registers quantum.ibm.com/qpu resources on
                                worker nodes with calibration data.

Based on Qonductor paper (SC '25) Sections 4, 5, 7.
"""

from src.operator.controller import HybridWorkflowController
from src.operator.k8s_client import (
    K8sClient,
    create_hybrid_workflow_cr,
    update_workflow_status,
    create_k8s_job_for_step,
    create_quantum_job_cr,
)

# QuantumSchedulerController may not be importable if qiskit/mqt are missing.
try:
    from src.operator.quantum_scheduler_controller import QuantumSchedulerController
except ImportError:
    QuantumSchedulerController = None  # type: ignore

try:
    from src.operator.device_plugin import QPUDevicePlugin
except ImportError:
    QPUDevicePlugin = None  # type: ignore

__all__ = [
    "HybridWorkflowController",
    "QuantumSchedulerController",
    "QPUDevicePlugin",
    "K8sClient",
    "create_hybrid_workflow_cr",
    "update_workflow_status",
    "create_k8s_job_for_step",
    "create_quantum_job_cr",
]
