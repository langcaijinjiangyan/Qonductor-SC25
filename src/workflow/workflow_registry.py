"""
Workflow Registry — persistent storage for hybrid workflow images.

A lightweight file-system-based registry with a JSON index file.
Each workflow image is stored in its own subdirectory containing:
- ``image.json``   — serialized WorkflowImage
- ``quantum.py``   — extracted quantum code
- ``classical.py`` — extracted classical code
- ``config.yaml``  — deployment configuration

Based on Qonductor paper Section 5:
"To streamline the deployment of such applications, the workflow registry
is a repository for ready-to-execute workflow images."
"""

from __future__ import annotations

import json
import os
import shutil
import threading
from pathlib import Path
from typing import Optional

import yaml

from src.workflow.workflow_image import WorkflowImage, WorkflowStatus


class WorkflowRegistry:
    """Registry for persistent storage and retrieval of workflow images.

    Storage layout (under *root_dir*)::

        root_dir/
        ├── index.json                    # image_id -> summary mapping
        └── {image_id}/
            ├── image.json                # full WorkflowImage serialization
            ├── quantum.py                # quantum code file
            ├── classical.py              # classical code file
            └── config.yaml               # deployment configuration
    """

    INDEX_FILENAME = "index.json"

    def __init__(self, root_dir: str | Path = "data/workflow_registry") -> None:
        self._root = Path(root_dir)
        self._root.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def register(self, image: WorkflowImage) -> str:
        """Register a workflow image and return its *image_id*."""
        with self._lock:
            image_dir = self._root / image.image_id
            image_dir.mkdir(parents=True, exist_ok=True)

            # Write the full image JSON.
            (image_dir / "image.json").write_text(
                image.to_json(), encoding="utf-8"
            )
            # Write the split code files.
            (image_dir / "quantum.py").write_text(
                image.quantum_code, encoding="utf-8"
            )
            (image_dir / "classical.py").write_text(
                image.classical_code, encoding="utf-8"
            )
            # Write config as YAML.
            (image_dir / "config.yaml").write_text(
                yaml.dump(image.config, default_flow_style=False),
                encoding="utf-8",
            )

            # Update index.
            self._update_index_entry(image)

            return image.image_id

    def get(self, image_id: str) -> Optional[WorkflowImage]:
        """Retrieve a workflow image by ID, or None if not found."""
        image_dir = self._root / image_id
        json_path = image_dir / "image.json"
        if not json_path.exists():
            return None
        data = json.loads(json_path.read_text(encoding="utf-8"))
        return WorkflowImage.from_dict(data)

    def list_images(self) -> list[dict]:
        """List all registered images (summary only, no code content)."""
        index_path = self._root / self.INDEX_FILENAME
        if not index_path.exists():
            return []
        index = json.loads(index_path.read_text(encoding="utf-8"))
        return list(index.values())

    def delete(self, image_id: str) -> bool:
        """Delete a workflow image and its directory. Returns success."""
        with self._lock:
            image_dir = self._root / image_id
            if not image_dir.exists():
                return False
            shutil.rmtree(image_dir)
            self._remove_index_entry(image_id)
            return True

    def update_status(self, image_id: str, status: WorkflowStatus) -> bool:
        """Update the status of a registered image."""
        image = self.get(image_id)
        if image is None:
            return False
        image.update_status(status)
        self.register(image)
        return True

    def exists(self, image_id: str) -> bool:
        return (self._root / image_id / "image.json").exists()

    # ------------------------------------------------------------------
    # Index helpers
    # ------------------------------------------------------------------

    def _update_index_entry(self, image: WorkflowImage) -> None:
        index = self._load_index()
        index[image.image_id] = {
            "image_id": image.image_id,
            "name": image.name,
            "status": image.status.value,
            "node_count": len(image.dag.nodes),
            "quantum_nodes": len(image.dag.quantum_nodes),
            "classical_nodes": len(image.dag.classical_nodes),
            "created_at": image.created_at,
            "updated_at": image.updated_at,
        }
        self._save_index(index)

    def _remove_index_entry(self, image_id: str) -> None:
        index = self._load_index()
        index.pop(image_id, None)
        self._save_index(index)

    def _load_index(self) -> dict:
        index_path = self._root / self.INDEX_FILENAME
        if not index_path.exists():
            return {}
        return json.loads(index_path.read_text(encoding="utf-8"))

    def _save_index(self, index: dict) -> None:
        index_path = self._root / self.INDEX_FILENAME
        index_path.write_text(
            json.dumps(index, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    @property
    def root_dir(self) -> Path:
        return self._root
