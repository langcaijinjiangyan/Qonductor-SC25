from __future__ import annotations

import threading
import time

from src.operator import k8s_client
from src.operator.controller import HybridWorkflowController
from src.operator.quantum_scheduler_controller import QuantumSchedulerController
from src.scheduler.multi_objective_scheduler import (
    MultiObjectiveScheduler,
    TranspilationLevel,
)
from src.workflow.dag_engine import DAGNode


def _quantum_cr(name: str) -> dict:
    return {
        "metadata": {"name": name},
        "spec": {"stepId": name, "qubits": 4},
    }


def test_quantum_watch_can_enqueue_while_scheduling_runs():
    controller = object.__new__(QuantumSchedulerController)
    controller.scheduling_interval = 600
    controller.scheduling_threshold = 1
    controller._pending = []
    controller._known_quantum_jobs = set()
    controller._last_schedule_time = time.monotonic()
    controller._stop_event = threading.Event()
    controller._pending_condition = threading.Condition()

    first_cycle_started = threading.Event()
    release_first_cycle = threading.Event()
    batches = []

    def run_cycle():
        with controller._pending_condition:
            batch = [job.cr_name for job in controller._pending]
            controller._pending.clear()
            controller._last_schedule_time = time.monotonic()
        batches.append(batch)
        if len(batches) == 1:
            first_cycle_started.set()
            release_first_cycle.wait(timeout=2)
        else:
            controller.stop()

    controller._safe_run_scheduling_cycle = run_cycle
    controller._enqueue(_quantum_cr("job-1"))

    worker = threading.Thread(target=controller._scheduling_loop)
    worker.start()
    assert first_cycle_started.wait(timeout=1)

    controller._enqueue(_quantum_cr("job-2"))
    controller._enqueue(_quantum_cr("job-3"))
    with controller._pending_condition:
        assert [job.cr_name for job in controller._pending] == [
            "job-2",
            "job-3",
        ]

    release_first_cycle.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert batches == [["job-1"], ["job-2", "job-3"]]


def test_qpu_transpilation_parallelizes_job_backend_matrix(monkeypatch):
    scheduler = object.__new__(MultiObjectiveScheduler)
    scheduler.transpilation_level = TranspilationLevel.QPU
    scheduler._transpilation_worker_count = lambda task_count: task_count
    scheduler._transpile_job_backend = (
        lambda job, backend: (f"{job}:{backend}", 0.5)
    )

    pool_calls = []

    class FakePool:
        def __init__(self, processes):
            pool_calls.append(processes)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def starmap(func, tasks):
            return [func(*task) for task in tasks]

    monkeypatch.setattr(
        "src.scheduler.multi_objective_scheduler.multiprocessing.Pool",
        FakePool,
    )

    transpiled, fidelities = scheduler._transpile_jobs(
        ["job-a", "job-b"],
        ["qpu-1", "qpu-2"],
    )

    assert pool_calls == [4]
    assert transpiled == [
        ["job-a:qpu-1", "job-a:qpu-2"],
        ["job-b:qpu-1", "job-b:qpu-2"],
    ]
    assert fidelities.tolist() == [[0.5, 0.5], [0.5, 0.5]]


def test_classical_wait_uses_centralized_job_state(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    controller.k8s.get_job_status = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("per-job polling must not be used")
    )
    node = DAGNode(step_id="step-1", label="classical-step")
    result = {}

    def wait_for_job():
        result.update(controller._wait_for_classical_job("job-1", node))

    waiter = threading.Thread(target=wait_for_job)
    waiter.start()
    with controller._job_condition:
        controller._job_statuses["job-1"] = {"succeeded": 1}
        controller._job_condition.notify_all()
    waiter.join(timeout=1)

    assert not waiter.is_alive()
    assert result["status"] == "completed"


def test_local_job_watch_filters_and_yields_existing_jobs():
    k8s_client.reset_local_store()
    client = k8s_client.K8sClient(mode="local")
    client.create_job({
        "metadata": {
            "name": "qonductor-job",
            "labels": {"app": "qonductor", "step_type": "classical"},
        },
        "spec": {},
    })
    client.create_job({
        "metadata": {
            "name": "other-job",
            "labels": {"app": "other"},
        },
        "spec": {},
    })

    watcher = client.watch_jobs(
        label_selector="app=qonductor,step_type=classical",
    )
    event = next(watcher)
    watcher.close()

    assert event["type"] == "ADDED"
    assert event["object"]["metadata"]["name"] == "qonductor-job"
    assert "jobs" not in client._store.watch_handlers


def test_workflow_state_is_released_after_unexpected_failure(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    cr = {"metadata": {"name": "workflow-1"}}
    controller._active["workflow-1"] = {"run_id": "run-1", "cr": cr}
    controller._execute_workflow = lambda *_args: (_ for _ in ()).throw(
        RuntimeError("failed")
    )
    controller.k8s.update_cr_status = lambda *_args, **_kwargs: None

    controller._run_workflow("image-1", {}, cr, "run-1")

    assert "workflow-1" not in controller._active


def test_terminal_quantum_job_releases_all_controller_caches():
    controller = object.__new__(QuantumSchedulerController)
    controller._pending_condition = threading.Condition()
    controller._pending = []
    controller._known_quantum_jobs = {"quantum-1"}
    controller._scheduling_results = {
        "step-1": {"quantum_job": "quantum-1"},
    }
    controller._result_keys_by_cr = {"quantum-1": "step-1"}
    controller._execution_jobs = {"step-1": "execution-1"}
    controller._execution_job_owners = {"step-1": "quantum-1"}

    controller._forget_quantum_job({
        "metadata": {"name": "quantum-1"},
        "spec": {"stepId": "step-1"},
    })

    assert not controller._known_quantum_jobs
    assert not controller._scheduling_results
    assert not controller._result_keys_by_cr
    assert not controller._execution_jobs
    assert not controller._execution_job_owners
