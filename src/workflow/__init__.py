"""
Qonductor Workflow Manager — Data Plane component.

The Workflow Manager provides:
- Hybrid code parsing and DAG construction
- Workflow image packaging
- Workflow registry for storing and retrieving images
- Hardware-agnostic Qonductor API for users
- Libraries of quantum algorithms and classical error mitigation

Based on the Qonductor paper (SC '25): "Qonductor: A Cloud Orchestrator
for Quantum Computing" by Giortamis et al.
"""

from src.workflow.dag_engine import HybridDAG, DAGNode, DAGEdge, StepType, CodeSplitter
from src.workflow.workflow_image import WorkflowImage, WorkflowStatus
from src.workflow.workflow_registry import WorkflowRegistry
from src.workflow.api import (
    createWorkflow,
    deploy,
    invoke,
    workflowResults,
    workflowStatus,
)
from src.workflow.qaoa_conversion import (
    build_qaoa_workflow_image,
    build_qaoa_workflow_inputs,
    convert_qaoa_workload,
)
from src.workflow.workload_conversion import (
    build_ansatz_workflow_image,
    build_ansatz_workflow_inputs,
    convert_all_workloads,
    convert_workload,
)

__all__ = [
    # DAG Engine
    "HybridDAG",
    "DAGNode",
    "DAGEdge",
    "StepType",
    "CodeSplitter",
    # Workflow Image
    "WorkflowImage",
    "WorkflowStatus",
    # Registry
    "WorkflowRegistry",
    # API
    "createWorkflow",
    "deploy",
    "invoke",
    "workflowResults",
    "workflowStatus",
    # QAOA workload conversion
    "build_qaoa_workflow_image",
    "build_qaoa_workflow_inputs",
    "convert_qaoa_workload",
    "build_ansatz_workflow_image",
    "build_ansatz_workflow_inputs",
    "convert_all_workloads",
    "convert_workload",
]
