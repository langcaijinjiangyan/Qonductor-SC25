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
    scheduler.transpilation_cache_enabled = False
    scheduler._transpilation_cache = {}
    scheduler.transpilation_cache_size = 256
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
        "src.scheduler.multi_objective_scheduler.ThreadPool",
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


def test_quantum_scheduling_cycle_takes_bounded_fifo_batch():
    controller = object.__new__(QuantumSchedulerController)
    controller.scheduling_batch_size = 120
    controller._pending_condition = threading.Condition()
    controller._pending = list(range(125))
    controller._last_schedule_time = 0.0

    batch, remaining = controller._take_pending_batch()

    assert batch == list(range(120))
    assert controller._pending == list(range(120, 125))
    assert remaining == 5
    assert controller._last_schedule_time > 0.0


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


def _register_quantum_child(
    controller: HybridWorkflowController,
    qj_name: str,
    step_id: str = "quantum-step",
    workflow_name: str = "workflow-1",
) -> tuple[str, str]:
    quantum_key = (workflow_name, step_id)
    with controller._quantum_condition:
        controller._child_jobs[qj_name] = {
            "step_id": step_id,
            "type": "quantum",
            "cr_name": qj_name,
            "workflow_ref": workflow_name,
        }
        controller._pending_quantum[quantum_key] = {
            "quantum_job": qj_name,
            "qubits": 4,
        }
    return quantum_key


def test_quantum_watch_completed_event_advances_registered_step(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    quantum_key = _register_quantum_child(controller, "quantum-1")
    waiter_ready = threading.Event()
    waiter_woke = threading.Event()

    def wait_for_quantum_event():
        with controller._quantum_condition:
            waiter_ready.set()
            controller._quantum_condition.wait(timeout=1)
            waiter_woke.set()

    waiter = threading.Thread(target=wait_for_quantum_event)
    waiter.start()
    assert waiter_ready.wait(timeout=1)

    controller._handle_quantum_job_event(
        "MODIFIED",
        "quantum-1",
        {
            "metadata": {"name": "quantum-1"},
            "status": {"phase": "Completed"},
        },
    )
    waiter.join(timeout=1)

    assert not waiter.is_alive()
    assert waiter_woke.is_set()
    assert quantum_key in controller._completed_quantum_steps
    assert quantum_key not in controller._failed_quantum_steps
    assert "quantum-1" not in controller._child_jobs


def test_quantum_watch_terminal_failures_preserve_reason(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )

    failed_key = _register_quantum_child(
        controller, "quantum-failed", step_id="failed-step",
    )
    controller._handle_quantum_job_event(
        "MODIFIED",
        "quantum-failed",
        {
            "metadata": {"name": "quantum-failed"},
            "status": {
                "phase": "Failed",
                "conditions": [{"reason": "execution failed"}],
            },
        },
    )

    cancelled_key = _register_quantum_child(
        controller, "quantum-cancelled", step_id="cancelled-step",
    )
    controller._handle_quantum_job_event(
        "MODIFIED",
        "quantum-cancelled",
        {
            "metadata": {"name": "quantum-cancelled"},
            "status": {"phase": "Cancelled"},
        },
    )

    deleted_key = _register_quantum_child(
        controller, "quantum-deleted", step_id="deleted-step",
    )
    controller._handle_quantum_job_event(
        "DELETED",
        "quantum-deleted",
        {"metadata": {"name": "quantum-deleted"}},
    )

    assert "execution failed" in controller._failed_quantum_steps[failed_key]
    assert "Cancelled" in controller._failed_quantum_steps[cancelled_key]
    assert (
        "deleted before completion"
        in controller._failed_quantum_steps[deleted_key]
    )


def test_quantum_watch_ignores_unowned_and_duplicate_events(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    completed = {
        "metadata": {"name": "dynamic-quantum"},
        "status": {"phase": "Completed"},
    }

    controller._handle_quantum_job_event(
        "MODIFIED", "dynamic-quantum", completed,
    )
    assert not controller._completed_quantum_steps

    quantum_key = _register_quantum_child(controller, "quantum-owned")
    completed["metadata"]["name"] = "quantum-owned"
    controller._handle_quantum_job_event(
        "MODIFIED", "quantum-owned", completed,
    )
    controller._handle_quantum_job_event(
        "MODIFIED", "quantum-owned", completed,
    )

    assert controller._completed_quantum_steps == {quantum_key}


def test_quantum_registration_sync_reads_once_without_job_polling(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    quantum_key = _register_quantum_child(controller, "quantum-sync")
    get_calls = []

    def get_cr(kind, name):
        get_calls.append((kind, name))
        return {
            "metadata": {"name": name},
            "status": {"phase": "Completed"},
        }

    controller.k8s.get_cr = get_cr
    controller.k8s.get_job_status = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("quantum execution Job polling must not be used")
    )

    controller._sync_quantum_job_state("quantum-sync")

    assert get_calls == [(k8s_client.QUANTUM_JOB_PLURAL, "quantum-sync")]
    assert quantum_key in controller._completed_quantum_steps


def _quantum_metrics_cr(
    name: str,
    *,
    resource_version: str,
    phase: str = "Completed",
    scheduled_at: str = "2026-07-23T00:00:02Z",
    **status_fields,
) -> dict:
    return {
        "metadata": {
            "name": name,
            "resourceVersion": resource_version,
            "creationTimestamp": "2026-07-23T00:00:00Z",
        },
        "spec": {
            "workflowRef": "workflow-1",
            "circuitQasm": "OPENQASM 3; " + "x" * 1000,
        },
        "status": {
            "phase": phase,
            "scheduledAt": scheduled_at,
            **status_fields,
        },
    }


def test_quantum_watch_caches_lightweight_dynamic_job_metrics(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    controller._active["workflow-1"] = {"run_id": "run-1"}

    controller._handle_quantum_job_event(
        "MODIFIED",
        "dynamic-quantum",
        _quantum_metrics_cr(
            "dynamic-quantum",
            resource_version="10",
            actualFidelity=0.8,
            actualExecutionTime=3.0,
        ),
    )

    cached = controller._quantum_metrics_by_workflow["workflow-1"][
        "dynamic-quantum"
    ]
    assert set(cached) == {
        "phase",
        "resource_version",
        "fidelity",
        "execution_time",
        "waiting_time",
    }
    assert cached["waiting_time"] == 2.0
    assert not controller._completed_quantum_steps


def test_workflow_metric_aggregation_uses_watch_cache_without_listing(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    controller._active["workflow-1"] = {"run_id": "run-1"}

    controller._handle_quantum_job_event(
        "MODIFIED",
        "quantum-1",
        _quantum_metrics_cr(
            "quantum-1",
            resource_version="11",
            actualFidelity=0.8,
            actualExecutionTime=3.0,
        ),
    )
    controller._handle_quantum_job_event(
        "MODIFIED",
        "quantum-2",
        _quantum_metrics_cr(
            "quantum-2",
            resource_version="12",
            scheduled_at="2026-07-23T00:00:05Z",
            estimatedFidelity=0.6,
            estimatedExecutionTime=4.0,
        ),
    )

    # A delayed event must not regress a terminal snapshot populated by sync.
    controller._handle_quantum_job_event(
        "MODIFIED",
        "quantum-1",
        _quantum_metrics_cr(
            "quantum-1",
            resource_version="10",
            phase="Pending",
            scheduled_at="",
        ),
    )

    controller.k8s.list_cr = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("workflow metric aggregation must not list QuantumJobs")
    )
    status_updates = []
    controller.k8s.update_cr_status = (
        lambda kind, name, status: status_updates.append((kind, name, status))
    )

    controller._aggregate_workflow_metrics("workflow-1", [])

    assert status_updates == [(
        k8s_client.HYBRID_WORKFLOW_PLURAL,
        "workflow-1",
        {
            "averageFidelity": 0.7,
            "totalWaitingTime": 7.0,
            "totalExecutionTime": 7.0,
        },
    )]


def test_workflow_metric_aggregation_waits_for_dynamic_terminal_event(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    controller.QUANTUM_METRICS_SETTLE_TIMEOUT_SECONDS = 1.0
    controller._active["workflow-1"] = {"run_id": "run-1"}
    controller._handle_quantum_job_event(
        "MODIFIED",
        "dynamic-quantum",
        _quantum_metrics_cr(
            "dynamic-quantum",
            resource_version="20",
            phase="Running",
            estimatedFidelity=0.5,
            estimatedExecutionTime=2.0,
        ),
    )

    status_updates = []
    controller.k8s.list_cr = lambda *_args, **_kwargs: (_ for _ in ()).throw(
        AssertionError("metric convergence must not list QuantumJobs")
    )
    controller.k8s.update_cr_status = (
        lambda kind, name, status: status_updates.append((kind, name, status))
    )
    aggregation_started = threading.Event()

    def aggregate():
        aggregation_started.set()
        controller._aggregate_workflow_metrics("workflow-1", [])

    worker = threading.Thread(target=aggregate)
    worker.start()
    assert aggregation_started.wait(timeout=1)
    time.sleep(0.05)
    assert worker.is_alive()
    assert not status_updates

    controller._handle_quantum_job_event(
        "MODIFIED",
        "dynamic-quantum",
        _quantum_metrics_cr(
            "dynamic-quantum",
            resource_version="21",
            actualFidelity=0.9,
            actualExecutionTime=3.0,
        ),
    )
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert status_updates[0][2] == {
        "averageFidelity": 0.9,
        "totalWaitingTime": 2.0,
        "totalExecutionTime": 3.0,
    }


def test_workflow_metric_convergence_timeout_uses_latest_snapshot(tmp_path):
    controller = HybridWorkflowController(
        mode="local",
        registry_root=str(tmp_path / "registry"),
    )
    controller.QUANTUM_METRICS_SETTLE_TIMEOUT_SECONDS = 0.0
    controller._active["workflow-1"] = {"run_id": "run-1"}
    controller._handle_quantum_job_event(
        "MODIFIED",
        "dynamic-quantum",
        _quantum_metrics_cr(
            "dynamic-quantum",
            resource_version="30",
            phase="Running",
            estimatedFidelity=0.5,
            estimatedExecutionTime=2.0,
        ),
    )

    status_updates = []
    controller.k8s.update_cr_status = (
        lambda _kind, _name, status: status_updates.append(status)
    )
    controller._aggregate_workflow_metrics("workflow-1", [])

    assert status_updates == [{
        "averageFidelity": 0.5,
        "totalWaitingTime": 2.0,
        "totalExecutionTime": 2.0,
    }]


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
    controller._quantum_metrics_by_workflow["workflow-1"] = {
        "quantum-1": {"fidelity": 0.9},
    }
    controller._execute_workflow = lambda *_args: (_ for _ in ()).throw(
        RuntimeError("failed")
    )
    controller.k8s.update_cr_status = lambda *_args, **_kwargs: None

    controller._run_workflow("image-1", {}, cr, "run-1")

    assert "workflow-1" not in controller._active
    assert "workflow-1" not in controller._quantum_metrics_by_workflow


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
