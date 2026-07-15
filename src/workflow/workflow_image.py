"""
Workflow Image — the packaged hybrid workflow unit.

Defines the ``WorkflowImage`` dataclass that bundles the DAG model, split
code files, and execution configuration together, as described in
Qonductor paper Section 5.

"The workflow graph model, the hybrid code files, and the execution
configuration files are packed into a hybrid workflow image and stored
in the workflow registry."
"""

from __future__ import annotations

import base64
import enum
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.workflow.dag_engine import HybridDAG


class WorkflowStatus(enum.Enum):
    """Lifecycle status of a workflow image / execution."""
    CREATED = "created"
    DEPLOYED = "deployed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class WorkflowImage:
    """A packaged hybrid quantum-classical workflow image.

    This is the central unit that flows through the Qonductor pipeline:
    it is created by the Workflow Manager, stored in the Registry, and
    consumed by the Job Manager for scheduling.

    Attributes:
        image_id: Unique identifier (UUID hex string).
        name: Human-readable workflow name.
        dag: The DAG representation of the workflow.
        quantum_code: Separated quantum Python source code.
        classical_code: Separated classical Python source code.
        config: Execution / deployment configuration dict (from YAML).
        metadata: Arbitrary metadata (dependencies, versions, etc.).
        status: Current lifecycle status.
        created_at: UTC timestamp of creation.
        updated_at: UTC timestamp of last status change.
        results: Execution results placeholder (populated after completion).
    """
    image_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    name: str = ""
    dag: HybridDAG = field(default_factory=HybridDAG)
    quantum_code: str = ""
    classical_code: str = ""
    config: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    status: WorkflowStatus = WorkflowStatus.CREATED
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    updated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    results: Any = None

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return {
            "image_id": self.image_id,
            "name": self.name,
            "dag": self.dag.to_dict(),
            "quantum_code": self.quantum_code,
            "classical_code": self.classical_code,
            "config": self.config,
            "metadata": self.metadata,
            "status": self.status.value,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "results": self.results,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "WorkflowImage":
        """Deserialize from a dictionary."""
        return cls(
            image_id=data.get("image_id", ""),
            name=data.get("name", ""),
            dag=HybridDAG.from_dict(data.get("dag", {})),
            quantum_code=data.get("quantum_code", ""),
            classical_code=data.get("classical_code", ""),
            config=data.get("config", {}),
            metadata=data.get("metadata", {}),
            status=WorkflowStatus(data.get("status", "created")),
            created_at=data.get("created_at", ""),
            updated_at=data.get("updated_at", ""),
            results=data.get("results"),
        )

    def to_json(self, indent: int = 2) -> str:
        """Serialize to a JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    @classmethod
    def from_json(cls, json_str: str) -> "WorkflowImage":
        """Deserialize from a JSON string."""
        return cls.from_dict(json.loads(json_str))

    # ------------------------------------------------------------------
    # Pack / unpack as portable bytes
    # ------------------------------------------------------------------

    def pack(self) -> bytes:
        """Pack the entire image into a portable bytes representation.

        Uses JSON with base64-encoding for the code files (which may
        contain arbitrary characters).
        """
        payload = self.to_dict()
        # The code fields are already strings in the dict; JSON handles that.
        return json.dumps(payload, ensure_ascii=False).encode("utf-8")

    @classmethod
    def unpack(cls, data: bytes) -> "WorkflowImage":
        """Unpack a ``WorkflowImage`` from portable bytes."""
        return cls.from_dict(json.loads(data.decode("utf-8")))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def update_status(self, status: WorkflowStatus) -> None:
        self.status = status
        self.updated_at = datetime.now(timezone.utc).isoformat()

    @property
    def quantum_steps(self) -> list[dict]:
        return [
            {
                "step_id": n.step_id,
                "label": n.label,
                "resource_requirements": n.resource_requirements,
            }
            for n in self.dag.quantum_nodes
        ]

    @property
    def classical_steps(self) -> list[dict]:
        return [
            {
                "step_id": n.step_id,
                "label": n.label,
                "resource_requirements": n.resource_requirements,
            }
            for n in self.dag.classical_nodes
        ]

    def __repr__(self) -> str:
        return (f"WorkflowImage(id={self.image_id!r}, name={self.name!r}, "
                f"status={self.status.value}, "
                f"nodes={len(self.dag.nodes)}, edges={len(self.dag.edges)})")
