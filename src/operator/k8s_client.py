"""
K8s API client wrapper for the Qonductor Operator.

Provides a thin abstraction over the Kubernetes Python client for:
- Watching custom resources (HybridWorkflow, QuantumJob)
- Creating K8s Jobs (classical steps → kube-scheduler)
- Updating CR status subresources (System Monitor role)
- Managing Node extended resources (QPU device plugin)
- Reading/writing ConfigMaps (calibration data)

In K8s mode the underlying etcd store is used; for local simulation a
dict-based store mirrors the API semantics.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Optional

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

HYBRID_WORKFLOW_GROUP = "qonductor.io"
HYBRID_WORKFLOW_VERSION = "v1"
HYBRID_WORKFLOW_PLURAL = "hybridworkflows"
QUANTUM_JOB_PLURAL = "quantumjobs"
QONDUCTOR_NODE_TYPE_LABEL = "qonductor.io/node-type"
QONDUCTOR_MEMORY_LIMIT_LABEL = "qonductor.io/memory-limit"

_MEMORY_UNITS = {
    "Ki": 1024,
    "Mi": 1024 ** 2,
    "Gi": 1024 ** 3,
    "Ti": 1024 ** 4,
    "K": 1000,
    "M": 1000 ** 2,
    "G": 1000 ** 3,
    "T": 1000 ** 4,
}

# Try to import the real K8s client; fall back to local simulation.
try:
    from kubernetes import client, config, watch
    from kubernetes.client.rest import ApiException
    _HAS_K8S = True
except ImportError:
    ApiException = None  # type: ignore
    _HAS_K8S = False
    logger.warning("kubernetes client not available — using local simulation mode")


# ---------------------------------------------------------------------------
# Local (non-K8s) simulation store
# ---------------------------------------------------------------------------

@dataclass
class _LocalStore:
    """In-memory store that mirrors K8s CR semantics for local testing."""
    crs: dict[str, dict] = field(default_factory=dict)
    jobs: dict[str, dict] = field(default_factory=dict)
    nodes: dict[str, dict] = field(default_factory=dict)
    configmaps: dict[str, dict] = field(default_factory=dict)
    watch_handlers: dict[str, list[Callable]] = field(default_factory=dict)
    _index: dict[str, list[str]] = field(default_factory=lambda: {
        HYBRID_WORKFLOW_PLURAL: [],
        QUANTUM_JOB_PLURAL: [],
    })

    def create(self, kind: str, body: dict) -> dict:
        name = body.get("metadata", {}).get("name", str(uuid.uuid4())[:8])
        key = f"{kind}/{name}"
        body["metadata"]["name"] = name
        body["metadata"]["uid"] = str(uuid.uuid4())[:16]
        body["metadata"]["creationTimestamp"] = _now_iso()
        self.crs[key] = body
        self._index.setdefault(kind, []).append(name)
        self._notify(kind, "ADDED", body)
        return body

    def get(self, kind: str, name: str) -> Optional[dict]:
        return self.crs.get(f"{kind}/{name}")

    def list(self, kind: str) -> list[dict]:
        return [self.crs[f"{kind}/{n}"]
                for n in self._index.get(kind, [])
                if f"{kind}/{n}" in self.crs]

    def patch_status(self, kind: str, name: str, status: dict) -> Optional[dict]:
        obj = self.get(kind, name)
        if obj is None:
            return None
        obj.setdefault("status", {}).update(status)
        self.crs[f"{kind}/{name}"] = obj
        self._notify(kind, "MODIFIED", obj)
        return obj

    def delete(self, kind: str, name: str) -> bool:
        key = f"{kind}/{name}"
        if key not in self.crs:
            return False
        obj = self.crs.pop(key)
        self._index.get(kind, []).remove(name)
        self._notify(kind, "DELETED", obj)
        return True

    def register_node(self, name: str, node: dict) -> None:
        self.nodes[name] = node

    def get_node(self, name: str) -> Optional[dict]:
        return self.nodes.get(name)

    def list_nodes(self) -> list[dict]:
        return list(self.nodes.values())

    def create_configmap(
        self, name: str, data: dict, labels: dict | None = None,
    ) -> dict:
        cm = {"metadata": {"name": name, "labels": labels or {}}, "data": data}
        self.configmaps[name] = cm
        return cm

    def get_configmap(self, name: str) -> Optional[dict]:
        return self.configmaps.get(name)

    def list_configmaps(self, label_selector: str = "") -> list[dict]:
        """List ConfigMaps, optionally filtered by label selector."""
        result = []
        for cm in self.configmaps.values():
            if label_selector:
                labels = cm.get("metadata", {}).get("labels", {})
                for kv in label_selector.split(","):
                    kv = kv.strip()
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        if labels.get(k) != v:
                            break
                else:
                    result.append(cm)
            else:
                result.append(cm)
        return result

    def update_configmap(
        self, name: str, data: dict, labels: dict | None = None,
    ) -> dict:
        """Update an existing ConfigMap."""
        cm = self.configmaps.get(name, {"metadata": {"name": name}})
        if labels is not None:
            cm.setdefault("metadata", {})["labels"] = labels
        cm["data"] = data
        self.configmaps[name] = cm
        return cm

    def list_jobs(self, label_selector: str = "") -> list[dict]:
        """List Jobs, optionally filtered by label selector."""
        result = []
        for name in self._index.get("jobs", []):
            job = self.crs.get(f"jobs/{name}")
            if job is None:
                continue
            if label_selector:
                labels = job.get("metadata", {}).get("labels", {})
                matched = True
                for kv in label_selector.split(","):
                    kv = kv.strip()
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        if labels.get(k) != v:
                            matched = False
                            break
                if not matched:
                    continue
            result.append(job)
        return result

    def patch_job(self, name: str, body: dict) -> Optional[dict]:
        """Patch a Job in the local store."""
        key = f"jobs/{name}"
        job = self.crs.get(key)
        if job is None:
            return None
        _deep_merge(job, body)
        self.crs[key] = job
        self._notify("jobs", "MODIFIED", job)
        return job

    def watch(self, kind: str) -> Iterator[dict]:
        """Generator that yields CR events (simulated)."""
        import threading
        import queue as qmod
        q: qmod.Queue = qmod.Queue()
        self.watch_handlers.setdefault(kind, []).append(
            lambda event: q.put(event)
        )
        # Yield existing resources first.
        for name in self._index.get(kind, []):
            obj = self.get(kind, name)
            if obj:
                q.put({"type": "ADDED", "object": obj})
        # Then yield new events.
        while True:
            try:
                event = q.get(timeout=30)
                yield event
            except qmod.Empty:
                yield {"type": "HEARTBEAT", "object": {}}

    def _notify(self, kind: str, event_type: str, obj: dict) -> None:
        for handler in self.watch_handlers.get(kind, []):
            try:
                handler({"type": event_type, "object": obj})
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Singleton store
# ---------------------------------------------------------------------------

_local_store: Optional[_LocalStore] = None
_k8s_api: Optional[Any] = None
_k8s_custom_api: Optional[Any] = None


def _get_store() -> _LocalStore:
    global _local_store
    if _local_store is None:
        _local_store = _LocalStore()
    return _local_store


def reset_local_store() -> None:
    """Reset the local simulation store (useful for test isolation)."""
    global _local_store, _CLIENT
    _local_store = _LocalStore()
    _CLIENT = None


def _get_k8s_custom_api():
    """Return a configured CustomObjectsApi, or None in local mode."""
    global _k8s_custom_api
    if _k8s_custom_api is None and _HAS_K8S:
        try:
            config.load_incluster_config()
        except Exception:
            try:
                config.load_kube_config()
            except Exception:
                return None
        _k8s_custom_api = client.CustomObjectsApi()
    return _k8s_custom_api


def _get_k8s_api():
    """Return CoreV1Api + BatchV1Api, or None."""
    global _k8s_api
    if _k8s_api is None and _HAS_K8S:
        try:
            config.load_incluster_config()
        except Exception:
            try:
                config.load_kube_config()
            except Exception:
                return None, None
        _k8s_api = (client.CoreV1Api(), client.BatchV1Api())
    return _k8s_api


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _deep_merge(base: dict, patch: dict) -> None:
    """Recursively merge *patch* into *base* in-place."""
    for key, value in patch.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def _parse_memory_quantity(value: Any) -> Optional[int]:
    """Parse Kubernetes/Docker-style memory quantities into bytes."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]+)?", text)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2) or ""
    return int(amount * _MEMORY_UNITS.get(unit, 1))


def _classical_node_affinity(client_obj: "K8sClient", resources: dict) -> dict | None:
    """Build nodeAffinity from Qonductor node labels for classical steps."""
    try:
        nodes = client_obj.get_nodes(f"{QONDUCTOR_NODE_TYPE_LABEL}=classical")
    except Exception as exc:
        logger.warning("Could not inspect nodes for classical scheduling: %s", exc)
        return None

    if not nodes:
        return None

    node_names = [
        node.get("metadata", {}).get("name", "")
        for node in nodes
        if node.get("metadata", {}).get("name")
    ]
    requested_memory = _parse_memory_quantity(resources.get("memory"))
    if requested_memory is not None:
        labelled_nodes: list[str] = []
        eligible_nodes: list[str] = []
        for node in nodes:
            name = node.get("metadata", {}).get("name", "")
            labels = node.get("metadata", {}).get("labels", {}) or {}
            limit = _parse_memory_quantity(labels.get(QONDUCTOR_MEMORY_LIMIT_LABEL))
            if limit is None:
                continue
            labelled_nodes.append(name)
            if limit >= requested_memory:
                eligible_nodes.append(name)

        if labelled_nodes:
            if not eligible_nodes:
                raise RuntimeError(
                    "No classical node has enough qonductor.io/memory-limit "
                    f"for requested memory {resources.get('memory')!r}"
                )
            node_names = eligible_nodes

    if not node_names:
        return None

    return {
        "nodeAffinity": {
            "requiredDuringSchedulingIgnoredDuringExecution": {
                "nodeSelectorTerms": [
                    {
                        "matchExpressions": [{
                            "key": QONDUCTOR_NODE_TYPE_LABEL,
                            "operator": "In",
                            "values": ["classical"],
                        }],
                        "matchFields": [{
                            "key": "metadata.name",
                            "operator": "In",
                            "values": [name],
                        }],
                    }
                    for name in node_names
                ],
            },
        },
    }


# ===================================================================
# Public API
# ===================================================================

class K8sClient:
    """Unified K8s API client — real cluster or local simulation.

    Usage::

        client = K8sClient(mode="local")   # or "k8s"
        client.create_cr("hybridworkflows", {...})
        client.watch_cr("quantumjobs")
    """

    def __init__(self, mode: str = "local"):
        # Auto-downgrade K8s mode to local if the cluster is unreachable.
        effective_mode = mode
        if mode == "k8s":
            customs = _get_k8s_custom_api()
            core, batch = _get_k8s_api()
            if customs is None or core is None:
                logger.warning(
                    "K8s cluster unreachable — falling back to local mode."
                )
                effective_mode = "local"
                self._custom_api = None
                self._core_api = None
                self._batch_api = None
            else:
                self._custom_api = customs
                self._core_api = core
                self._batch_api = batch
        else:
            self._custom_api = None
            self._core_api = None
            self._batch_api = None
        self.mode = effective_mode
        self._store = _get_store()

    # -- CR operations ----------------------------------------------------

    def create_cr(self, kind: str, namespace: str = "default",
                  body: dict | None = None) -> dict:
        """Create a CR and return it (with metadata populated)."""
        if body is None:
            body = {}
        if self.mode == "k8s" and self._custom_api:
            group = HYBRID_WORKFLOW_GROUP
            version = HYBRID_WORKFLOW_VERSION
            plural = (HYBRID_WORKFLOW_PLURAL if "workflow" in kind.lower()
                      else QUANTUM_JOB_PLURAL)
            try:
                result = self._custom_api.create_namespaced_custom_object(
                    group=group, version=version, namespace=namespace,
                    plural=plural, body=body,
                )
                return result
            except Exception as exc:
                logger.warning("K8s API call failed: %s — falling back to local", exc)
                self.mode = "local"  # permanent fallback for this client
        return self._store.create(kind, body)

    def get_cr(self, kind: str, name: str) -> Optional[dict]:
        if self.mode == "k8s" and self._custom_api:
            plural = (HYBRID_WORKFLOW_PLURAL if "workflow" in kind.lower()
                      else QUANTUM_JOB_PLURAL)
            return self._custom_api.get_namespaced_custom_object(
                group=HYBRID_WORKFLOW_GROUP, version=HYBRID_WORKFLOW_VERSION,
                namespace="default", plural=plural, name=name,
            )
        return self._store.get(kind, name)

    def list_cr(self, kind: str) -> list[dict]:
        if self.mode == "k8s" and self._custom_api:
            plural = (HYBRID_WORKFLOW_PLURAL if "workflow" in kind.lower()
                      else QUANTUM_JOB_PLURAL)
            result = self._custom_api.list_namespaced_custom_object(
                group=HYBRID_WORKFLOW_GROUP, version=HYBRID_WORKFLOW_VERSION,
                namespace="default", plural=plural,
            )
            return result.get("items", [])
        return self._store.list(kind)

    def update_cr_status(self, kind: str, name: str,
                         status: dict) -> Optional[dict]:
        if self.mode == "k8s" and self._custom_api:
            plural = (HYBRID_WORKFLOW_PLURAL if "workflow" in kind.lower()
                      else QUANTUM_JOB_PLURAL)
            return self._custom_api.patch_namespaced_custom_object_status(
                group=HYBRID_WORKFLOW_GROUP, version=HYBRID_WORKFLOW_VERSION,
                namespace="default", plural=plural, name=name,
                body={"status": status},
            )
        return self._store.patch_status(kind, name, status)

    def delete_cr(self, kind: str, name: str) -> bool:
        if self.mode == "k8s" and self._custom_api:
            plural = (HYBRID_WORKFLOW_PLURAL if "workflow" in kind.lower()
                      else QUANTUM_JOB_PLURAL)
            self._custom_api.delete_namespaced_custom_object(
                group=HYBRID_WORKFLOW_GROUP, version=HYBRID_WORKFLOW_VERSION,
                namespace="default", plural=plural, name=name,
            )
            return True
        return self._store.delete(kind, name)

    def watch_cr(self, kind: str) -> Iterator[dict]:
        """Generator yielding {"type": "ADDED"/"MODIFIED"/"DELETED", "object": ...}."""
        if self.mode == "k8s" and _HAS_K8S and self._custom_api:
            plural = (HYBRID_WORKFLOW_PLURAL if "workflow" in kind.lower()
                      else QUANTUM_JOB_PLURAL)
            resource_version = ""
            backoff_seconds = 1.0

            while True:
                try:
                    listed = self._custom_api.list_namespaced_custom_object(
                        group=HYBRID_WORKFLOW_GROUP,
                        version=HYBRID_WORKFLOW_VERSION,
                        namespace="default",
                        plural=plural,
                        resource_version=resource_version or None,
                    )
                    resource_version = (
                        listed.get("metadata", {}).get("resourceVersion", "")
                    )
                    for obj in listed.get("items", []):
                        yield {"type": "ADDED", "object": obj}

                    w = watch.Watch()
                    for event in w.stream(
                        self._custom_api.list_namespaced_custom_object,
                        group=HYBRID_WORKFLOW_GROUP,
                        version=HYBRID_WORKFLOW_VERSION,
                        namespace="default",
                        plural=plural,
                        resource_version=resource_version or None,
                        timeout_seconds=300,
                    ):
                        obj = event.get("object", {})
                        rv = obj.get("metadata", {}).get("resourceVersion")
                        if rv:
                            resource_version = rv
                        backoff_seconds = 1.0
                        yield event
                except Exception as exc:
                    status = getattr(exc, "status", None)
                    if ApiException is not None and isinstance(exc, ApiException) and status == 410:
                        logger.warning(
                            "Watch for %s expired at resourceVersion=%s; "
                            "relisting and reconnecting",
                            plural,
                            resource_version or "<initial>",
                        )
                        resource_version = ""
                        backoff_seconds = 1.0
                        continue

                    logger.warning(
                        "Watch for %s failed: %s; retrying in %.1fs",
                        plural,
                        exc,
                        backoff_seconds,
                    )
                    time.sleep(backoff_seconds)
                    backoff_seconds = min(backoff_seconds * 2, 30.0)
        yield from self._store.watch(kind)

    # -- Node operations --------------------------------------------------

    def get_nodes(self, label_selector: str = "") -> list[dict]:
        """Return nodes that optionally match *label_selector*."""
        if self.mode == "k8s" and self._core_api:
            nodes = self._core_api.list_node(
                label_selector=label_selector,
            ).items
            return [self._node_to_dict(n) for n in nodes]
        nodes = self._store.list_nodes()
        if label_selector:
            # Simple label selector parsing.
            key, val = label_selector.split("=")
            nodes = [n for n in nodes
                     if n.get("metadata", {}).get("labels", {}).get(key) == val]
        return nodes

    def patch_node_extended_resources(self, node_name: str,
                                      resources: dict) -> None:
        """Add or update extended resources on a node (e.g. quantum.ibm.com/qpu)."""
        if self.mode == "k8s" and self._core_api:
            self._core_api.patch_node(
                node_name,
                {"status": {"capacity": resources, "allocatable": resources}},
            )
        else:
            node = self._store.get_node(node_name)
            if node:
                node.setdefault("status", {}).setdefault("capacity", {}).update(resources)
                node["status"].setdefault("allocatable", {}).update(resources)

    def register_node(self, name: str, node: dict) -> None:
        self._store.register_node(name, node)

    @staticmethod
    def _node_to_dict(node) -> dict:
        return {
            "metadata": {
                "name": node.metadata.name,
                "labels": node.metadata.labels or {},
            },
            "status": {
                "capacity": node.status.capacity or {},
                "allocatable": node.status.allocatable or {},
                "conditions": [
                    {"type": c.type, "status": c.status}
                    for c in (node.status.conditions or [])
                ],
            },
        }

    # -- Job operations (classical steps → K8s Job) -----------------------

    def create_job(self, job_manifest: dict,
                   namespace: str = "default") -> str:
        """Create a K8s Job for a classical workflow step.
        Returns the job name.
        """
        if self.mode == "k8s" and self._batch_api:
            result = self._batch_api.create_namespaced_job(
                namespace=namespace, body=job_manifest,
            )
            return result.metadata.name
        return self._store.create("jobs", job_manifest)["metadata"]["name"]

    def get_job_status(self, name: str,
                       namespace: str = "default") -> Optional[dict]:
        if self.mode == "k8s" and self._batch_api:
            j = self._batch_api.read_namespaced_job_status(
                name=name, namespace=namespace,
            )
            return {"active": j.status.active, "succeeded": j.status.succeeded,
                    "failed": j.status.failed}
        job = self._store.get("jobs", name)
        if job is None:
            return None
        s = job.get("status", {})
        return {"active": s.get("active", 0),
                "succeeded": s.get("succeeded", 0),
                "failed": s.get("failed", 0)}

    # -- ConfigMap operations (System Monitor calibration data) -----------

    def create_configmap(self, name: str, data: dict,
                         labels: dict | None = None,
                         namespace: str = "default") -> dict:
        if self.mode == "k8s" and self._core_api:
            return self._core_api.create_namespaced_config_map(
                namespace=namespace,
                body={
                    "metadata": {"name": name, "labels": labels or {}},
                    "data": data,
                },
            )
        return self._store.create_configmap(name, data, labels=labels)

    def get_configmap(self, name: str) -> Optional[dict]:
        if self.mode == "k8s" and self._core_api:
            cm = self._core_api.read_namespaced_config_map(name, "default")
            return {"data": cm.data}
        return self._store.get_configmap(name)

    def list_configmaps(self, label_selector: str = "",
                        namespace: str = "default") -> list[dict]:
        """List ConfigMaps, optionally filtered by label selector."""
        if self.mode == "k8s" and self._core_api:
            result = self._core_api.list_namespaced_config_map(
                namespace=namespace,
                label_selector=label_selector or None,
            )
            return [
                {
                    "metadata": {
                        "name": cm.metadata.name,
                        "labels": cm.metadata.labels or {},
                    },
                    "data": cm.data or {},
                }
                for cm in result.items
            ]
        return self._store.list_configmaps(label_selector)

    def update_configmap(self, name: str, data: dict,
                         labels: dict | None = None,
                         namespace: str = "default") -> dict:
        """Update an existing ConfigMap (replace data)."""
        if self.mode == "k8s" and self._core_api:
            body = {"data": data}
            if labels is not None:
                body["metadata"] = {"labels": labels}
            return self._core_api.patch_namespaced_config_map(
                name=name, namespace=namespace,
                body=body,
            )
        return self._store.update_configmap(name, data, labels=labels)

    def list_jobs(self, label_selector: str = "") -> list[dict]:
        """List K8s Jobs, optionally filtered by label selector."""
        if self.mode == "k8s" and self._batch_api:
            result = self._batch_api.list_namespaced_job(
                namespace="default",
                label_selector=label_selector or None,
            )
            return [
                {
                    "metadata": {
                        "name": j.metadata.name,
                        "labels": j.metadata.labels or {},
                        "creationTimestamp": str(j.metadata.creation_timestamp),
                    },
                    "spec": {
                        "suspend": getattr(j.spec, "suspend", False),
                    },
                    "status": {
                        "active": getattr(j.status, "active", None),
                        "succeeded": getattr(j.status, "succeeded", None),
                        "failed": getattr(j.status, "failed", None),
                    },
                }
                for j in result.items
            ]
        return self._store.list_jobs(label_selector)

    def patch_job(self, name: str, body: dict,
                  namespace: str = "default") -> dict:
        """Patch a K8s Job (used for suspend/unsuspend)."""
        if self.mode == "k8s" and self._batch_api:
            return self._batch_api.patch_namespaced_job(
                name=name, namespace=namespace, body=body,
            )
        return self._store.patch_job(name, body) or {}


# ===================================================================
# Convenience functions (no K8sClient object needed for simple ops)
# ===================================================================

_CLIENT: Optional[K8sClient] = None


def _get_client(mode: str = "local") -> K8sClient:
    global _CLIENT
    if _CLIENT is None or _CLIENT.mode != mode:
        _CLIENT = K8sClient(mode=mode)
    return _CLIENT


def _merge_env(*env_lists: list[dict] | None) -> list[dict]:
    """Merge K8s env lists by name while preserving the last value."""
    merged: dict[str, dict] = {}
    for env_list in env_lists:
        for item in env_list or []:
            name = item.get("name")
            if name:
                merged[name] = item
    return list(merged.values())


def _rfc1123_name(*parts: str, max_length: int = 63) -> str:
    """Build a Kubernetes-safe resource name from arbitrary identifiers."""
    raw = "-".join(str(part) for part in parts if part)
    name = re.sub(r"[^a-z0-9.-]+", "-", raw.lower())
    name = re.sub(r"-+", "-", name).strip("-.")
    if not name:
        name = "qonductor"
    return name[:max_length].rstrip("-.") or "qonductor"


def _serialize_circuit_for_executor(circuit) -> dict[str, str]:
    """Serialize a Qiskit circuit for the quantum executor container."""
    from qiskit import qasm3
    return {"format": "qasm3", "qasm": qasm3.dumps(circuit)}


def create_hybrid_workflow_cr(
    image_id: str,
    name: str = "",
    priority: str = "balanced",
    containers: list | None = None,
    workflow_inputs: dict | None = None,
    mode: str = "local",
) -> dict:
    """Build and create a HybridWorkflow CR.

    Args:
        image_id: WorkflowImage.image_id from the registry.
        name: CR name (auto-generated if empty).
        priority: 'fidelity', 'jct', or 'balanced'.
        containers: Container specs from Listing 1.
        workflow_inputs: Key-value inputs for the workflow.
        mode: 'local' or 'k8s'.
    """
    client = _get_client(mode)
    cr = {
        "apiVersion": f"{HYBRID_WORKFLOW_GROUP}/{HYBRID_WORKFLOW_VERSION}",
        "kind": "HybridWorkflow",
        "metadata": {
            "name": name or f"qonductor-{image_id}-{uuid.uuid4().hex[:6]}",
            "labels": {"app": "qonductor", "image_id": image_id},
        },
        "spec": {
            "workflowImageRef": image_id,
            "priority": priority,
            "maxRetries": 3,
            "containers": containers or [],
            "scheduling": {
                "classicalPolicy": "FilterScore",
                "quantumPolicy": "NSGA2",
                "schedulingInterval": 120,
                "schedulingThreshold": 100,
            },
            "errorMitigation": {"enabled": True, "stackedTechniques": []},
            "workflowInputs": workflow_inputs or {},
        },
        "status": {
            "phase": "Pending",
            "stepsCompleted": 0,
            "totalSteps": 0,
        },
    }
    return client.create_cr(HYBRID_WORKFLOW_PLURAL, body=cr)


def update_workflow_status(image_id: str, status: dict,
                           mode: str = "local") -> Optional[dict]:
    """Update the status of a HybridWorkflow CR."""
    client = _get_client(mode)
    crs = client.list_cr(HYBRID_WORKFLOW_PLURAL)
    for cr in crs:
        if cr.get("spec", {}).get("workflowImageRef") == image_id:
            name = cr["metadata"]["name"]
            return client.update_cr_status(HYBRID_WORKFLOW_PLURAL, name, status)
    return None


def create_k8s_job_for_step(step_node, cr: dict, container_spec: dict | None = None,
                            mode: str = "local",
                            client_override: K8sClient | None = None) -> str:
    """Create a K8s Job for a CLASSICAL DAG step.

    The job is submitted to the K8s API; kube-scheduler handles Filter-Scoring.

    Args:
        step_node: DAGNode with step_type == CLASSICAL.
        cr: The parent HybridWorkflow CR.
        container_spec: Optional container overrides (image, resources).
        mode: 'local' or 'k8s'.

    Returns:
        The K8s Job name.
    """
    client = client_override or _get_client(mode)
    job_name = _rfc1123_name("qonductor", step_node.step_id, uuid.uuid4().hex[:6])
    workflow_name = cr.get("metadata", {}).get("name", "")
    workflow_inputs = cr.get("spec", {}).get("workflowInputs", {})

    resources = {}
    for key, val in step_node.resource_requirements.items():
        if key in ("gpu", "nvidia.com/gpu"):
            resources["nvidia.com/gpu"] = str(val)
        elif key in ("cpu",):
            resources["cpu"] = str(val)
        elif key in ("memory",):
            resources["memory"] = str(val)

    container_image = "python:3.11-slim"
    image_pull_policy = "IfNotPresent"
    command = ["python", "-c", step_node.code]
    container_env: list[dict] = []
    if container_spec:
        container_image = container_spec.get("image", container_image)
        image_pull_policy = container_spec.get(
            "imagePullPolicy", image_pull_policy,
        )
        if container_spec.get("command"):
            command = container_spec["command"]
        container_env = container_spec.get("env", [])
    elif cr.get("spec", {}).get("containers"):
        container_image = cr["spec"]["containers"][0].get(
            "image", container_image,
        )

    runtime_env = [
        {"name": "QONDUCTOR_MODE", "value": mode},
        {"name": "QONDUCTOR_NAMESPACE", "value": cr.get("metadata", {}).get("namespace", "default")},
        {"name": "QONDUCTOR_WORKFLOW_NAME", "value": workflow_name},
        {"name": "QONDUCTOR_STEP_ID", "value": step_node.step_id},
        {"name": "QONDUCTOR_WORKFLOW_INPUTS", "value": json.dumps(workflow_inputs)},
    ]
    pod_spec = {
        "serviceAccountName": "qonductor-operator",
        "containers": [{
            "name": step_node.label.replace("_", "-"),
            "image": container_image,
            "imagePullPolicy": image_pull_policy,
            "command": command,
            "env": _merge_env(container_env, runtime_env),
            "resources": {
                "limits": resources,
                "requests": {k: v for k, v in resources.items()},
            },
        }],
        "restartPolicy": "Never",
    }
    affinity = _classical_node_affinity(client, resources)
    if affinity:
        pod_spec["affinity"] = affinity

    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": {
                "app": "qonductor",
                "workflow": workflow_name,
                "step_id": step_node.step_id,
                "step_type": "classical",
            },
        },
        "spec": {
            "backoffLimit": int(cr.get("spec", {}).get("maxRetries", 100)),
            "template": {
                "metadata": {"labels": {"app": "qonductor-classical"}},
                "spec": pod_spec,
            },
        },
    }
    return client.create_job(job_manifest)

def create_quantum_execution_job(
    scheduling_job,       # SchedulingJob (with transpiled circuits)
    assigned_backend,     # QPUBackend (with node_name for affinity)
    quantum_job_cr: dict,
    mode: str = "local",
    client_override: K8sClient | None = None,
) -> str:
    """Create a K8s Job that executes a quantum circuit on the assigned QPU.

    Dispatches the **transpiled** circuit from *scheduling_job* as a K8s
    ``batch/v1`` Job with:

    1. ``nodeAffinity`` targeting the quantum worker node that hosts the
       assigned QPU backend (via label ``qonductor.io/backend-<name>=true``).
    2. ``quantum.ibm.com/qpu=1`` resource request so kube-scheduler can
       also filter on the extended resource.

    The container receives a structured QASM payload and the QPU JSON
    profile is available via a hostPath volume mount.

    Args:
        scheduling_job: ``SchedulingJob`` containing the transpiled circuits.
        assigned_backend: ``QPUBackend`` assigned by NSGA-II scheduling.
        quantum_job_cr: The ``QuantumJob`` CR dict.
        mode: ``"local"`` or ``"k8s"``.

    Returns:
        The K8s Job name.
    """
    client = client_override or _get_client(mode)
    qj_spec = quantum_job_cr.get("spec", {})
    qj_name = quantum_job_cr.get("metadata", {}).get("name", "")
    workflow_ref = qj_spec.get("workflowRef", "")
    step_id = qj_spec.get("stepId", "")
    job_name = _rfc1123_name("qonductor-q", step_id, uuid.uuid4().hex[:6])

    if scheduling_job and scheduling_job.circuits:
        circuit_payload = [
            _serialize_circuit_for_executor(circuit)
            for circuit in scheduling_job.circuits
        ]
    else:
        # Fallback for dynamic workflows: use circuit_qasm from CR spec
        raw_qasm = qj_spec.get("circuitQasm", "")
        raw_fmt = qj_spec.get("circuitFormat", "qasm3")
        # Include parameter bindings so the executor can bind symbolic params.
        param_binds = qj_spec.get("parameterBindings") or None
        payload = {"format": raw_fmt, "qasm": raw_qasm}
        if param_binds:
            payload["parameter_binds"] = param_binds
        circuit_payload = [payload]
    legacy_qasm = "\n---QONDUCTOR-CIRCUIT---\n".join(
        item["qasm"] for item in circuit_payload
    )
    circuit_format = circuit_payload[0]["format"] if circuit_payload else "qasm2"

    qpu_name = assigned_backend.name

    job_manifest = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "labels": {
                "app": "qonductor",
                "workflow": workflow_ref,
                "step_id": step_id,
                "step_type": "quantum",
                "assigned_qpu": qpu_name,
                "quantum_job_cr": qj_name,
            },
        },
        "spec": {
            "suspend": True,
            "template": {
                "metadata": {"labels": {"app": "qonductor-quantum"}},
                "spec": {
                    "serviceAccountName": "qonductor-operator",
                    # Pin the Pod to the quantum worker node hosting this QPU
                    "affinity": {
                        "nodeAffinity": {
                            "requiredDuringSchedulingIgnoredDuringExecution": {
                                "nodeSelectorTerms": [{
                                    "matchExpressions": [
                                        {
                                            "key": f"qonductor.io/backend-{qpu_name}",
                                            "operator": "In",
                                            "values": ["true"],
                                        },
                                        {
                                            "key": "qonductor.io/node-type",
                                            "operator": "In",
                                            "values": ["quantum"],
                                        },
                                    ],
                                }],
                            },
                        },
                    },
                    "containers": [{
                        "name": "quantum-circuit-executor",
                        "image": "qonductor-quantum-executor:latest",
                        "imagePullPolicy": "IfNotPresent",
                        "env": [
                            {"name": "QONDUCTOR_MODE", "value": mode},
                            {"name": "QONDUCTOR_NAMESPACE", "value": quantum_job_cr.get("metadata", {}).get("namespace", "default")},
                            {"name": "QUANTUM_JOB_NAME", "value": qj_name},
                            {"name": "QONDUCTOR_WORKFLOW_NAME", "value": workflow_ref},
                            {"name": "QPU_NAME", "value": qpu_name},
                            {"name": "QPU_JSON_PATH",
                             "value": f"/etc/qonductor/qpus/{qpu_name}.json"},
                            {"name": "CIRCUIT_PAYLOAD_JSON",
                             "value": json.dumps(circuit_payload)},
                            {"name": "CIRCUIT_FORMAT", "value": circuit_format},
                            {"name": "CIRCUIT_QASM", "value": legacy_qasm},
                            {"name": "SHOTS", "value": str(scheduling_job.shots)},
                        ],
                        "resources": {
                            "requests": {
                                "cpu": "100m",
                                "memory": "256Mi",
                            },
                            "limits": {
                                "cpu": "1",
                                "memory": "1Gi",
                            },
                        },
                        "volumeMounts": [{
                            "name": "qpu-profiles",
                            "mountPath": "/etc/qonductor/qpus",
                            "readOnly": True,
                        }],
                    }],
                    "volumes": [{
                        "name": "qpu-profiles",
                        "hostPath": {"path": "/etc/qonductor/qpus"},
                    }],
                    "restartPolicy": "Never",
                },
            },
        },
    }
    return client.create_job(job_manifest)

def create_quantum_job_cr(step_node, workflow_cr: dict,
                          mode: str = "local",
                          client_override: K8sClient | None = None) -> dict:
    """Create a QuantumJob CR for a QUANTUM DAG step.

    Args:
        step_node: DAGNode with step_type == QUANTUM.
        workflow_cr: The parent HybridWorkflow CR.
        mode: 'local' or 'k8s'.

    Returns:
        The created QuantumJob CR.
    """
    client = client_override or _get_client(mode)
    metadata = getattr(step_node, "metadata", {}) or {}
    resource_reqs = getattr(step_node, "resource_requirements", {}) or {}
    spec = {
        "workflowRef": workflow_cr.get("metadata", {}).get("name", ""),
        "stepId": step_node.step_id,
        "label": step_node.label,
        "qubits": resource_reqs.get("qubits", 10),
        "shots": resource_reqs.get("shots", 4000),
        "priority": workflow_cr.get("spec", {}).get("priority", "balanced"),
    }

    optional_fields = {
        "logical_circuit_id": "logicalCircuitId",
        "logicalCircuitId": "logicalCircuitId",
        "circuit_names": "circuitNames",
        "circuitNames": "circuitNames",
        "circuit_qasm": "circuitQasm",
        "circuitQasm": "circuitQasm",
        "circuit_format": "circuitFormat",
        "circuitFormat": "circuitFormat",
        "parameter_bindings": "parameterBindings",
        "parameterBindings": "parameterBindings",
        "qasm_path": "qasmPath",
        "qasmPath": "qasmPath",
        "iteration": "iteration",
        "eval_label": "evalLabel",
        "evalLabel": "evalLabel",
        "schedule_immediately": "scheduleImmediately",
        "scheduleImmediately": "scheduleImmediately",
    }
    for source_key, spec_key in optional_fields.items():
        if source_key in metadata and metadata[source_key] is not None:
            spec[spec_key] = metadata[source_key]

    if "logicalCircuitId" in spec and "circuitNames" not in spec:
        spec["circuitNames"] = [spec["logicalCircuitId"]]

    qj = {
        "apiVersion": f"{HYBRID_WORKFLOW_GROUP}/{HYBRID_WORKFLOW_VERSION}",
        "kind": "QuantumJob",
        "metadata": {
            "name": _rfc1123_name("qj", step_node.step_id, uuid.uuid4().hex[:4]),
            "labels": {
                "app": "qonductor",
                "workflow": workflow_cr.get("metadata", {}).get("name", ""),
                "step_id": step_node.step_id,
            },
        },
        "spec": spec,
        "status": {"phase": "Pending"},
    }
    return client.create_cr(QUANTUM_JOB_PLURAL, body=qj)
