from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.operator import k8s_client


class _K8sError(Exception):
    def __init__(self, status: int | None = None):
        super().__init__(f"status={status}")
        self.status = status


class _CustomApi:
    def __init__(self, failures: int = 0, status: int | None = 500):
        self.failures = failures
        self.status = status
        self.calls = 0

    def create_namespaced_custom_object(self, **kwargs):
        self.calls += 1
        if self.calls <= self.failures:
            raise _K8sError(self.status)
        return {"metadata": {"name": kwargs["body"]["metadata"]["name"]}}


def _install_fake_k8s(monkeypatch, custom_api):
    monkeypatch.setattr(k8s_client, "_HAS_K8S", True)
    monkeypatch.setattr(k8s_client, "_get_k8s_custom_api", lambda: custom_api)
    monkeypatch.setattr(k8s_client, "_get_k8s_api", lambda: (object(), object()))


def _quantum_job_cr(qasm: str = "OPENQASM 3;") -> dict:
    return {
        "metadata": {"name": "qj-test", "namespace": "default"},
        "spec": {
            "workflowRef": "wf-test",
            "stepId": "quantum-step",
            "circuitQasm": qasm,
            "circuitFormat": "qasm3",
        },
    }


def test_k8s_mode_refuses_to_fall_back_when_api_unavailable(monkeypatch):
    monkeypatch.setattr(k8s_client, "_HAS_K8S", True)
    monkeypatch.setattr(k8s_client, "_get_k8s_custom_api", lambda: None)
    monkeypatch.setattr(k8s_client, "_get_k8s_api", lambda: (None, None))

    with pytest.raises(RuntimeError, match="refusing to fall back to local mode"):
        k8s_client.K8sClient(mode="k8s")


def test_k8s_api_call_retries_and_keeps_k8s_mode(monkeypatch):
    custom_api = _CustomApi(failures=2, status=500)
    _install_fake_k8s(monkeypatch, custom_api)
    monkeypatch.setenv("QONDUCTOR_K8S_API_RETRIES", "3")
    monkeypatch.setenv("QONDUCTOR_K8S_API_BACKOFF_SECONDS", "0")
    monkeypatch.setattr(k8s_client.time, "sleep", lambda _seconds: None)

    client = k8s_client.K8sClient(mode="k8s")
    created = client.create_cr(
        k8s_client.HYBRID_WORKFLOW_PLURAL,
        body={"metadata": {"name": "wf-retry"}},
    )

    assert created["metadata"]["name"] == "wf-retry"
    assert custom_api.calls == 3
    assert client.mode == "k8s"


def test_k8s_api_call_does_not_retry_non_retryable_error(monkeypatch):
    custom_api = _CustomApi(failures=3, status=404)
    _install_fake_k8s(monkeypatch, custom_api)
    monkeypatch.setenv("QONDUCTOR_K8S_API_RETRIES", "3")

    client = k8s_client.K8sClient(mode="k8s")

    with pytest.raises(_K8sError):
        client.create_cr(
            k8s_client.HYBRID_WORKFLOW_PLURAL,
            body={"metadata": {"name": "wf-missing"}},
        )

    assert custom_api.calls == 1
    assert client.mode == "k8s"


def test_quantum_execution_job_mounts_payload_configmap():
    client = k8s_client.K8sClient(mode="local")
    scheduling_job = SimpleNamespace(circuits=[], shots=128)
    backend = SimpleNamespace(name="qpu0_27q")

    job_name = k8s_client.create_quantum_execution_job(
        scheduling_job,
        backend,
        _quantum_job_cr(),
        mode="local",
        client_override=client,
    )

    job = client._store.get("jobs", job_name)
    container = job["spec"]["template"]["spec"]["containers"][0]
    env_names = {env["name"] for env in container["env"]}
    assert "CIRCUIT_PAYLOAD_PATH" in env_names
    assert "CIRCUIT_PAYLOAD_JSON" not in env_names
    assert "CIRCUIT_QASM" not in env_names

    configmaps = client._store.list_configmaps("component=quantum-payload")
    assert len(configmaps) == 1
    assert k8s_client.QUANTUM_PAYLOAD_FILE_NAME in configmaps[0]["data"]

    volumes = job["spec"]["template"]["spec"]["volumes"]
    payload_volume = next(v for v in volumes if v["name"] == "circuit-payload")
    assert payload_volume["configMap"]["name"] == configmaps[0]["metadata"]["name"]


def test_quantum_execution_job_rejects_payload_over_configmap_limit():
    client = k8s_client.K8sClient(mode="local")
    scheduling_job = SimpleNamespace(circuits=[], shots=128)
    backend = SimpleNamespace(name="qpu0_27q")
    huge_qasm = "x" * (k8s_client.QUANTUM_PAYLOAD_CONFIGMAP_MAX_BYTES + 1)

    with pytest.raises(ValueError, match="exceeds ConfigMap limit"):
        k8s_client.create_quantum_execution_job(
            scheduling_job,
            backend,
            _quantum_job_cr(huge_qasm),
            mode="local",
            client_override=client,
        )
