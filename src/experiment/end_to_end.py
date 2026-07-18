#!/usr/bin/env python3
"""End-to-end experiment runner comparing Qonductor vs FCFS scheduling.

Usage
-----
    # Run both schedulers with 1500 jobs (full experiment):
    python -m src.experiment.end_to_end --job-count 1500

    # Run only FCFS with 20 jobs (quick test):
    python -m src.experiment.end_to_end --job-count 20 --scheduler fcfs

    # Run only Qonductor with 10 jobs:
    python -m src.experiment.end_to_end --job-count 10 --scheduler qonductor

Outputs
-------
Per-run (saved in ``data/end_to_end/<scheduler>_<N>jobs/``):
    - metadata_<timestamp>.json       — list of per-round metadata dicts
    - backend_times_<timestamp>.json  — per-backend total busy time
    - job_waiting_times_<timestamp>.json — per-job queue waiting times
    - queue_size_<timestamp>.json     — queue depth over time

Aggregated (saved in ``data/end_to_end/``):
    - jct_fidelity_<N>.csv            — fidelity & JCT over 3600 s (Qonductor)
    - jct_fidelity_fcfs.csv           — fidelity & JCT over 3600 s (FCFS)
    - utilizations.csv                — QPU utilisation for both schedulers
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import pathlib
import queue
import sys
import threading
import time
from collections import defaultdict
from timeit import default_timer as timer
from typing import Any

from src.scheduler.base_scheduler import BaseScheduler, SchedulingJob
from src.scheduler.fcfs_scheduler import FCFSScheduler
from src.scheduler.multi_objective_scheduler import (
    MultiObjectiveScheduler,
    ProblemType,
    TranspilationLevel,
)
from src.scheduling_manager.load_generator import LoadGenerator
from src.utils.benchmark import (
    get_benchmark_names,
    get_fake_backends,
    patch_fake_backend,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NUM_POINTS = 100
RUN_DURATION = 3600.0       # seconds covered by the output CSVs
MEASUREMENT_INTERVAL = RUN_DURATION / NUM_POINTS  # 36.0 s


def _auto_frequency(job_count: int) -> float:
    """Inter-arrival time (seconds) that spreads *job_count* jobs over RUN_DURATION.

    Per the paper: 1500 jobs over one hour → one job every 2.4 s.
    For small job counts (< 500) the paper-accurate interval would be
    impractically long (e.g. 360 s for 10 jobs), so we cap at 10 s
    for quick-test mode.  Use ``--frequency`` to override.
    """
    exact = RUN_DURATION / job_count
    if job_count < 500:
        return min(exact, 10.0)
    return exact  # 1500 → 2.4 s


def _auto_interval(job_count: int) -> int:
    """Auto-scale scheduling interval to the job count.

    Small experiments shouldn't wait 30 s for the first round.
    """
    if job_count <= 20:
        return 5
    elif job_count <= 500:
        return 20
    else:
        return 30


def _auto_threshold(job_count: int) -> int:
    """Auto-scale scheduling threshold to the job count."""
    if job_count <= 20:
        return 5
    elif job_count <= 500:
        return 20
    else:
        return 10


def _auto_drain_wait(job_count: int) -> int:
    """Seconds to wait for the queue to drain after load generation ends."""
    if job_count <= 20:
        return 10
    elif job_count <= 500:
        return 40
    else:
        return 90  # scheduling_interval (30 s) + 60 s buffer


def _decay_backends(backends: list, elapsed: float) -> float:
    """Simulate backends processing work for *elapsed* virtual seconds.

    Reduces each backend's ``_waiting_time`` by *elapsed* (capped at 0)
    and resets the timestamp so subsequent wall-clock decay is negligible.

    Returns the total amount of work actually completed (sum of reductions,
    capped by ``elapsed`` per backend), which can be used to compute
    utilisation as ``work_done / (elapsed * N)``.
    """
    total_work_done = 0.0
    for backend in backends:
        before = backend._waiting_time
        backend._waiting_time = max(0.0, before - elapsed)
        backend._waiting_time_timestamp = dt.datetime.now(dt.timezone.utc)
        total_work_done += before - backend._waiting_time
    return total_work_done


# ---------------------------------------------------------------------------
# Helper: drain up to *limit* items from a queue.Queue
# ---------------------------------------------------------------------------

def _drain_queue(
    q: queue.Queue, limit: int
) -> tuple[list[SchedulingJob], list[float]]:
    """Drain up to *limit* ``(submit_time, job)`` tuples from *q*."""
    jobs: list[SchedulingJob] = []
    timestamps: list[float] = []
    while len(jobs) < limit:
        try:
            ts, job = q.get_nowait()
            timestamps.append(ts)
            jobs.append(job)
        except queue.Empty:
            break
    return jobs, timestamps


# ===================================================================
# EndToEndExperiment
# ===================================================================

class EndToEndExperiment:
    """Orchestrate a single end-to-end scheduling experiment.

    Parameters
    ----------
    data_dir : pathlib.Path
        Directory where per-run JSON and aggregated CSV files are saved.
    job_count : int
        Total number of jobs to submit (10 | 20 | 500 | 1500).
    scheduler_type : str
        ``"qonductor"`` or ``"fcfs"``.
    scheduling_interval : int
        Seconds between forced scheduling rounds (default 30).
    scheduling_threshold : int
        Queue depth that triggers an immediate scheduling round (default 10).
    """

    def __init__(
        self,
        data_dir: pathlib.Path,
        job_count: int = 1500,
        scheduler_type: str = "qonductor",
        scheduling_interval: int | None = None,
        scheduling_threshold: int | None = None,
        frequency: float | None = None,
        simulate: bool = False,
    ) -> None:
        self.data_dir = data_dir
        self.job_count = job_count
        self.scheduler_type = scheduler_type.lower()
        self.simulate = simulate
        self._frequency = (
            frequency
            if frequency is not None
            else _auto_frequency(job_count)
        )
        # Auto-scale when not explicitly provided
        self.scheduling_interval = (
            scheduling_interval
            if scheduling_interval is not None
            else _auto_interval(job_count)
        )
        self.scheduling_threshold = (
            scheduling_threshold
            if scheduling_threshold is not None
            else _auto_threshold(job_count)
        )

        # Ensure fake backends are patched before creation
        patch_fake_backend()
        self.backends = get_fake_backends()[:8]
        logger.info("Using %d backends: %s",
                      len(self.backends),
                      [b.name for b in self.backends])

        # Scheduler
        self.scheduler: BaseScheduler
        if self.scheduler_type == "fcfs":
            self.scheduler = FCFSScheduler()
        else:
            self.scheduler = MultiObjectiveScheduler(
                transpilation_level=TranspilationLevel.PRE_TRANSPILED,
                problem_type=ProblemType.DISCRETE,
            )

        # Shared queue (thread-safe queue.Queue since we use threads)
        self.job_queue: queue.Queue = queue.Queue()

        # Load generator — scale pool to job count (capped at 100)
        pool_size = min(100, max(10, self.job_count))
        self.load_generator = LoadGenerator(self.job_queue, self.data_dir,
                                            pool_size=pool_size)

        # Stop signal for the scheduling thread
        self._stop_event = threading.Event()

        # Results accumulation (populated by scheduling thread)
        self.all_metadata: list[dict[str, Any]] = []
        self.backend_exec_times: dict[str, float] = defaultdict(float)
        self.job_waiting_times: list[float] = []
        self.utilization_log: list[dict[str, Any]] = []

        # Timestamps
        self.experiment_start: float | None = None

        # Thread handles
        self._scheduling_thread: threading.Thread | None = None
        self._util_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run the full experiment: submission → scheduling → CSV export."""
        logger.info("=== Starting %s experiment with %d jobs ===",
                      self.scheduler_type, self.job_count)

        # Pre-load transpiled circuits for both schedulers
        if self.scheduler_type in ("qonductor", "fcfs"):
            logger.info("Loading pre-transpiled circuits …")
            self.scheduler.load_pre_transpiled_circuits(
                self.backends,
                get_benchmark_names(),
                [3, 4, 5],
            )
            logger.info("Pre-transpiled circuits loaded.")

        if self.simulate:
            self._run_simulate()
        else:
            self._run_realtime()

        # Save per-run JSON artefacts
        self._save_results()

        # Generate aggregated CSVs
        self._generate_aggregated_csvs()

        logger.info("=== %s experiment complete ===", self.scheduler_type)

    def _run_realtime(self) -> None:
        """Original real-time path: LoadGenerator + threaded scheduling loop."""
        self.experiment_start = timer()
        self.experiment_start_iso = dt.datetime.now(
            dt.timezone.utc
        ).isoformat()

        # 1. Launch scheduling thread
        self._scheduling_thread = threading.Thread(
            target=self._scheduling_loop,
            name="scheduling-loop",
            daemon=True,
        )
        self._scheduling_thread.start()

        # 2. Launch utilisation tracker
        self._util_thread = threading.Thread(
            target=self._utilization_tracker,
            name="util-tracker",
            daemon=True,
        )
        self._util_thread.start()

        # 3. Run load generator (blocking)
        logger.info("Starting load generator (%d jobs, %.1f s/job) …",
                      self.job_count, self._frequency)
        self.load_generator.run(job_count=self.job_count,
                                frequency=self._frequency)
        logger.info("Load generator finished. Waiting for queue to drain …")

        # 4. Wait for the queue to drain
        drain_wait = _auto_drain_wait(self.job_count)
        time.sleep(drain_wait)

        # 5. Signal shutdown
        self._stop_event.set()
        logger.info("Stop signal sent. Waiting for scheduling thread …")
        self._scheduling_thread.join(timeout=120)
        self._util_thread.join(timeout=30)

    # ------------------------------------------------------------------
    # Scheduling loop (runs in background thread)
    # ------------------------------------------------------------------

    def _scheduling_loop(self) -> None:
        """Background loop that mirrors ``SchedulingManager.run()``.

        Drains the queue when *scheduling_threshold* jobs have accumulated
        or *scheduling_interval* seconds have elapsed since the last round.
        Exits when ``_stop_event`` is set **and** the queue is empty.
        """
        next_schedule = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
            seconds=self.scheduling_interval
        )

        while not self._stop_event.is_set() or not self.job_queue.empty():
            now = dt.datetime.now(dt.timezone.utc)

            should_schedule = (
                self.job_queue.qsize() >= self.scheduling_threshold
                or (now >= next_schedule and not self.job_queue.empty())
            )

            if should_schedule:
                jobs, timestamps = _drain_queue(
                    self.job_queue, self.scheduling_threshold
                )
                if not jobs:
                    # Queue was drained by another check — skip
                    next_schedule = now + dt.timedelta(
                        seconds=self.scheduling_interval
                    )
                    time.sleep(1)
                    continue

                logger.info("Scheduling %d job(s) on %d backend(s) …",
                              len(jobs), len(self.backends))

                try:
                    assignments, rejected, metadata = self.scheduler.schedule(
                        jobs, self.backends
                    )
                except Exception:
                    logger.exception("Scheduler raised an exception — "
                                     "aborting scheduling loop")
                    break

                # Record per-job waiting times
                now_ts = timer()
                for ts in timestamps:
                    self.job_waiting_times.append(now_ts - ts)

                # Stamp metadata
                metadata["time"] = dt.datetime.now(
                    dt.timezone.utc
                ).isoformat()
                self.all_metadata.append(metadata)

                # Update backend state
                if assignments and metadata.get("solution_execution_times"):
                    for idx, (_, backend) in enumerate(assignments):
                        exec_time = metadata["solution_execution_times"][idx]
                        backend.update_waiting_time(exec_time)
                        self.backend_exec_times[backend.name] += exec_time

                logger.info("Round complete — %d assigned, %d rejected.",
                              len(assignments), len(rejected))

                next_schedule = dt.datetime.now(dt.timezone.utc) + dt.timedelta(
                    seconds=self.scheduling_interval
                )
            else:
                time.sleep(1)

        logger.info("Scheduling loop exited.")

    # ------------------------------------------------------------------
    # Utilisation tracker (runs in background thread)
    # ------------------------------------------------------------------

    def _utilization_tracker(self) -> None:
        """Track backend utilisation as *work completed / capacity*.

        Samples once per second.  Between consecutive samples each backend
        can process at most 1 s of work (the wall-clock elapsed time).
        The metric captures how much of that capacity was actually used.
        """
        prev_waiting = {b.name: b.get_waiting_time() for b in self.backends}
        while not self._stop_event.is_set():
            time.sleep(1)
            total_completed = 0.0
            for b in self.backends:
                curr = b.get_waiting_time()
                # Work completed since last sample (capped at 1 s / backend)
                completed = prev_waiting[b.name] - curr
                total_completed += max(0.0, min(completed, 1.0))
                prev_waiting[b.name] = curr
            util = total_completed / len(self.backends) * 100.0
            self.utilization_log.append({
                "time": dt.datetime.now(dt.timezone.utc).isoformat(),
                "busy_backends": int(
                    sum(1 for b in self.backends
                        if b.get_waiting_time() > 0.0)
                ),
                "total_backends": len(self.backends),
                "utilization": util,
            })

    # ------------------------------------------------------------------
    # Simulated-time path (no wall-clock waits)
    # ------------------------------------------------------------------

    def _run_simulate(self) -> None:
        """Virtual-time experiment: no ``time.sleep()``, instant results.

        All jobs are pre-generated and assigned virtual arrival timestamps
        spread evenly over ``RUN_DURATION``.  Scheduling rounds are
        triggered immediately as soon as the virtual clock reaches the
        next interval / threshold point.
        """
        logger.info("Simulate mode — using virtual time (no wall-clock waits).")

        self.experiment_start = timer()
        self.experiment_start_iso = dt.datetime.now(
            dt.timezone.utc
        ).isoformat()

        # Record virtual start for CSV alignment
        _virt_start_dt = dt.datetime.fromisoformat(self.experiment_start_iso)

        # 1. Pre-generate all jobs  ------------------------------------
        logger.info("Generating %d jobs …", self.job_count)
        job_pool = self._generate_job_pool(self.job_count)
        logger.info("Job pool ready (%d jobs).", len(job_pool))

        # 2. Virtual arrival timestamps  -------------------------------
        virtual_submit = [
            i * self._frequency for i in range(self.job_count)
        ]

        # 3. Virtual-time scheduling loop  -----------------------------
        job_idx = 0
        virtual_time = 0.0
        total_assigned = 0

        while job_idx < self.job_count or total_assigned < self.job_count:
            # Collect jobs that have "arrived" by now
            batch: list[SchedulingJob] = []
            batch_ts: list[float] = []
            while job_idx < self.job_count and len(batch) < self.scheduling_threshold:
                if virtual_submit[job_idx] <= virtual_time:
                    batch.append(job_pool[job_idx])
                    batch_ts.append(virtual_submit[job_idx])
                    job_idx += 1
                else:
                    break

            if batch:
                logger.info("[t=%.0fs] Scheduling %d job(s) …",
                              virtual_time, len(batch))
                try:
                    assignments, rejected, metadata = self.scheduler.schedule(
                        batch, self.backends
                    )
                except Exception:
                    logger.exception("Scheduler failed at virtual t=%.0f", virtual_time)
                    break

                # Waiting time = virtual_time - submission time
                for ts in batch_ts:
                    self.job_waiting_times.append(virtual_time - ts)

                # Stamp metadata with virtual time
                metadata["time"] = (
                    _virt_start_dt + dt.timedelta(seconds=virtual_time)
                ).isoformat()
                self.all_metadata.append(metadata)

                # Update backend state
                if assignments and metadata.get("solution_execution_times"):
                    for idx, (_, backend) in enumerate(assignments):
                        exec_time = metadata["solution_execution_times"][idx]
                        backend.update_waiting_time(exec_time)
                        self.backend_exec_times[backend.name] += exec_time

                total_assigned += len(assignments)
                logger.info("[t=%.0fs] Round complete — %d assigned, %d rejected "
                              "(total: %d/%d).",
                              virtual_time, len(assignments), len(rejected),
                              total_assigned, self.job_count)
            else:
                # No jobs available yet — jump to next arrival
                if job_idx < self.job_count:
                    # Decay backends for the idle period
                    _decay_backends(
                        self.backends, virtual_submit[job_idx] - virtual_time
                    )
                    virtual_time = virtual_submit[job_idx]
                    continue

            # Advance virtual clock to next scheduling point
            virtual_time += self.scheduling_interval

            # Simulate backends processing work during the elapsed interval
            _decay_backends(self.backends, self.scheduling_interval)

        # 4. Utilisation is now computed post-hoc from the per-job
        #    timeline in _build_utilization (called by _generate_aggregated_csvs).

    def _generate_job_pool(self, n: int) -> list[SchedulingJob]:
        """Generate *n* ``SchedulingJob`` instances for simulated mode.

        Only a small pool of *unique* jobs is created (capped at 200);
        the final list is then sampled from that pool with replacement.
        This mirrors ``LoadGenerator._generate_jobs`` behaviour and
        keeps generation cost independent of *n*.
        """
        import numpy as np

        from src.utils.benchmark import generate_random_job, get_benchmark_names

        seed = int(os.environ.get("SEED", int(time.time())))
        rng = np.random.default_rng(seed)
        benchmarks = get_benchmark_names()

        pool_size = min(n, 200)
        unique_jobs: list[SchedulingJob] = []
        for _ in range(pool_size):
            circuit_count = int(rng.normal(50, 20))
            circuit_count = max(1, min(circuit_count, 100))
            job = generate_random_job(
                min_backend_size=5,
                benchmark_names=benchmarks,
                random_generator=rng,
                circuit_count=circuit_count,
                shots=4000,
            )
            unique_jobs.append(job)

        indices = rng.integers(0, pool_size, size=n)
        return [unique_jobs[i] for i in indices]

    # ------------------------------------------------------------------
    # Result persistence
    # ------------------------------------------------------------------

    def _save_results(self) -> None:
        """Write the four JSON output files to *data_dir*."""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        ts = dt.datetime.now(dt.timezone.utc).isoformat(timespec="minutes")

        files: list[tuple[str, Any]] = [
            (f"metadata_{ts}.json", self.all_metadata),
            (f"backend_times_{ts}.json", dict(self.backend_exec_times)),
            (f"job_waiting_times_{ts}.json", self.job_waiting_times),
            (f"utilization_log_{ts}.json", self.utilization_log),
        ]

        for filename, data in files:
            filepath = self.data_dir / filename
            logger.info("Saving %s", filepath)
            with open(filepath, "w") as fh:
                json.dump(data, fh, indent=4)

    # ------------------------------------------------------------------
    # CSV aggregation
    # ------------------------------------------------------------------

    def _generate_aggregated_csvs(self) -> None:
        """Produce ``jct_fidelity`` and ``utilizations`` CSV files."""
        jct_fid = self._build_jct_fidelity()
        util = self._build_utilization()

        # Determine output filenames
        if self.scheduler_type == "fcfs":
            jct_name = "jct_fidelity_fcfs.csv"
            util_name = "utilizations_fcfs.csv"
        else:
            jct_name = f"jct_fidelity_{self.job_count}.csv"
            util_name = f"utilizations_{self.job_count}.csv"

        _write_csv(self.data_dir / jct_name, jct_fid,
                    ["timestamp", "fidelity", "JCT"])
        _write_csv(self.data_dir / util_name, util,
                    ["timestamp", "utilization"])

    # ------------------------------------------------------------------
    # Per-job timeline (shared by JCT / fidelity / utilisation)
    # ------------------------------------------------------------------

    def _compute_per_job_timeline(
        self, start_dt: dt.datetime
    ) -> tuple[list[tuple[float, float, float, float]], list[tuple[str, float, float]]]:
        """Compute per-job virtual completion times from round metadata.

        Simulates 8 backends processing jobs in parallel: each job is
        placed on its assigned backend and completes when that backend
        becomes free (tracked via ``backend_busy_until``, which persists
        across scheduling rounds).

        Returns
        -------
        all_jobs : list of (completion_time, jct_contrib, fidelity, submit_time)
            Each job's virtual completion time, its JCT contribution
            (wt + et), the round fidelity, and the virtual submission
            timestamp.  The caller decides which key to sort/filter on.

        backend_segments : list of (backend_name, start_time, end_time)
            Time intervals during which each backend was busy.
        """
        backend_busy_until: dict[str, float] = {}
        all_jobs: list[tuple[float, float, float, float]] = []
        backend_segments: list[tuple[str, float, float]] = []

        waiting_idx = 0
        for meta in self.all_metadata:
            try:
                round_dt = dt.datetime.fromisoformat(meta["time"])
            except (KeyError, ValueError):
                continue
            round_start = (round_dt - start_dt).total_seconds()

            exec_times = meta.get("solution_execution_times", [])
            solution = meta.get("solution", [])
            sol_idx = meta.get("solution_index", 0)
            mean_errors = meta.get("mean_error", [])
            round_fidelity = (
                1.0 - mean_errors[sol_idx]
                if mean_errors and sol_idx < len(mean_errors)
                else 0.0
            )

            for j in range(len(exec_times)):
                wt = (
                    self.job_waiting_times[waiting_idx]
                    if waiting_idx < len(self.job_waiting_times)
                    else 0.0
                )
                et = exec_times[j]

                # Assigned backend
                if solution and j < len(solution):
                    backend_name = self.backends[solution[j]].name
                else:
                    backend_name = f"_unknown_{j}"

                # Virtual submission time: round_start minus the
                # queue waiting time that elapsed before scheduling.
                submit_time = max(0.0, round_start - wt)

                # When does this backend become free?
                bf = backend_busy_until.get(backend_name, round_start)
                start = max(round_start, bf)
                completion = start + et
                backend_busy_until[backend_name] = completion

                jct_contrib = wt + (start - round_start) + et
                all_jobs.append(
                    (completion, jct_contrib, round_fidelity, submit_time)
                )
                backend_segments.append((backend_name, start, completion))
                waiting_idx += 1

        return all_jobs, backend_segments

    # ------------------------------------------------------------------
    # JCT & fidelity CSV builder
    # ------------------------------------------------------------------

    def _build_jct_fidelity(self) -> list[dict[str, Any]]:
        """Aggregate per-job metrics into 100 evenly-spaced data points.

        Each job contributes to the cumulative JCT / fidelity at its
        *virtual submission time* so that the metric reflects the
        experience of jobs that have already entered the system,
        regardless of whether they have completed yet.
        """
        if not self.all_metadata:
            logger.warning("No metadata — returning empty CSV rows.")
            return []

        try:
            start_dt = dt.datetime.fromisoformat(self.experiment_start_iso)
        except (KeyError, ValueError):
            logger.warning("Cannot parse experiment start timestamp.")
            return []

        # -- Pre-compute per-job timeline  --------------------------------
        all_jobs, _ = self._compute_per_job_timeline(start_dt)

        # Sort by virtual submission time (index 3) instead of completion
        all_jobs.sort(key=lambda x: x[3])

        # -- Walk measurement points, adding jobs as they are submitted --
        rows: list[dict[str, Any]] = []
        cum_jct = 0.0
        cum_fidelity_weighted = 0.0
        cum_job_count = 0
        job_ptr = 0

        for i in range(NUM_POINTS):
            point_elapsed = i * MEASUREMENT_INTERVAL
            cut_dt = start_dt + dt.timedelta(seconds=point_elapsed)

            while job_ptr < len(all_jobs) and all_jobs[job_ptr][3] <= point_elapsed:
                _, jct_contrib, fid, _ = all_jobs[job_ptr]
                cum_jct += jct_contrib
                cum_fidelity_weighted += fid
                cum_job_count += 1
                job_ptr += 1

            avg_fidelity = (
                cum_fidelity_weighted / cum_job_count
                if cum_job_count > 0 else 0.0
            )

            rows.append({
                "timestamp": cut_dt.isoformat(),
                "fidelity": avg_fidelity,
                "JCT": cum_jct,
            })

        return rows

    # ------------------------------------------------------------------
    # Utilisation CSV builder
    # ------------------------------------------------------------------

    def _build_utilization(self) -> list[dict[str, Any]]:
        """Compute utilisation from per-job backend busy intervals.

        For each 36-second measurement interval we sum the overlap of
        every job's ``[start_time, completion_time)`` segment on its
        assigned backend with the interval, then divide by the total
        available capacity (``MEASUREMENT_INTERVAL × N_backends``).
        This produces a continuous utilisation metric that varies at
        every measurement point.
        """
        if not self.all_metadata:
            logger.warning("No metadata — returning empty CSV rows.")
            return []

        try:
            start_dt = dt.datetime.fromisoformat(self.experiment_start_iso)
        except (KeyError, ValueError):
            logger.warning("Cannot parse experiment start timestamp.")
            return []

        _, backend_segments = self._compute_per_job_timeline(start_dt)
        n_backends = len(self.backends)

        rows: list[dict[str, Any]] = []
        for i in range(NUM_POINTS):
            t_start = max(0.0, (i - 1) * MEASUREMENT_INTERVAL)
            t_end = i * MEASUREMENT_INTERVAL

            total_busy = 0.0
            for _bname, seg_start, seg_end in backend_segments:
                overlap = max(0.0, min(seg_end, t_end) - max(seg_start, t_start))
                total_busy += overlap

            capacity = MEASUREMENT_INTERVAL * n_backends
            util = total_busy / capacity * 100.0 if capacity > 0 else 0.0

            rows.append({
                "timestamp": (start_dt + dt.timedelta(seconds=t_end)).isoformat(),
                "utilization": util,
            })

        return rows


# ===================================================================
# Standalone helpers
# ===================================================================

def _write_csv(
    filepath: pathlib.Path,
    rows: list[dict[str, Any]],
    fieldnames: list[str],
) -> None:
    """Write *rows* as CSV to *filepath*."""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %s (%d rows)", filepath, len(rows))


def _merge_utilization_csvs(
    q_rows: list[dict[str, Any]],
    f_rows: list[dict[str, Any]],
    output_path: pathlib.Path,
) -> None:
    """Merge two single-scheduler utilisation CSVs into one combined file."""
    merged: list[dict[str, Any]] = []
    for q, f in zip(q_rows, f_rows):
        merged.append({
            "timestamp": q["timestamp"],
            "Qonductor": q["utilization"],
            "FCFS": f["utilization"],
        })
    _write_csv(output_path, merged, ["timestamp", "Qonductor", "FCFS"])


# ===================================================================
# CLI entry point
# ===================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="End-to-end scheduling experiment (Qonductor vs FCFS)."
    )
    parser.add_argument(
        "--job-count", type=int, default=1500,
        choices=[10, 20, 500, 1500],
        help="Number of jobs to submit (default: 1500)."
    )
    parser.add_argument(
        "--scheduler", type=str, default="both",
        choices=["qonductor", "fcfs", "both"],
        help="Which scheduler(s) to run (default: both)."
    )
    parser.add_argument(
        "--data-dir", type=str, default="data/end_to_end",
        help="Base output directory (default: data/end_to_end)."
    )
    parser.add_argument(
        "--scheduling-interval", type=int, default=None,
        help="Scheduling interval in seconds (auto-scaled by job count if omitted)."
    )
    parser.add_argument(
        "--scheduling-threshold", type=int, default=None,
        help="Queue size that triggers immediate scheduling (auto-scaled if omitted)."
    )
    parser.add_argument(
        "--frequency", type=float, default=None,
        help="Inter-arrival time in seconds (auto-scaled from job count if omitted)."
    )
    parser.add_argument(
        "--simulate", action="store_true", default=False,
        help="Use virtual time instead of wall-clock waits (fast mode)."
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for job generation (ensures identical job pools "
             "when running both schedulers).  Falls back to the SEED env "
             "variable, then to the current Unix timestamp."
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logging.getLogger("qiskit").setLevel(logging.WARNING)
    logging.getLogger("qiskit_ibm_provider").setLevel(logging.WARNING)

    # Pin the random seed so that when running both schedulers they
    # operate on identical job pools.  The seed is read by
    # _generate_job_pool and LoadGenerator via os.environ["SEED"].
    seed = args.seed
    if seed is None:
        seed = int(os.environ.get("SEED", int(time.time())))
    os.environ["SEED"] = str(seed)
    logger.info("Random seed: %d", seed)

    base_dir = pathlib.Path(args.data_dir)

    schedulers_to_run: list[str] = (
        ["qonductor", "fcfs"] if args.scheduler == "both" else [args.scheduler]
    )

    jct_results: dict[str, list[dict[str, Any]]] = {}
    util_results: dict[str, list[dict[str, Any]]] = {}

    for sched in schedulers_to_run:
        run_dir = base_dir / f"{sched}_{args.job_count}jobs"
        logger.info("Output directory: %s", run_dir)

        experiment = EndToEndExperiment(
            data_dir=run_dir,
            job_count=args.job_count,
            scheduler_type=sched,
            scheduling_interval=args.scheduling_interval,
            scheduling_threshold=args.scheduling_threshold,
            frequency=args.frequency,
            simulate=args.simulate,
        )
        experiment.run()

        # Collect CSVs for potential merging
        jct_rows = experiment._build_jct_fidelity()
        util_rows = experiment._build_utilization()
        jct_results[sched] = jct_rows
        util_results[sched] = util_rows

    # If both schedulers ran, produce the legacy-named CSVs that
    # ``src/analysis/e2e_performance.py`` expects.
    if len(schedulers_to_run) == 2:
        logger.info("Merging utilisation CSVs …")
        _merge_utilization_csvs(
            util_results["qonductor"],
            util_results["fcfs"],
            base_dir / "utilizations.csv",
        )

        # Also copy / rename Qonductor JCT+fidelity to the legacy name
        q_jct_path = base_dir / f"jct_fidelity_{args.job_count}.csv"
        f_jct_path = base_dir / "jct_fidelity_fcfs.csv"

        _write_csv(q_jct_path, jct_results["qonductor"],
                    ["timestamp", "fidelity", "JCT"])
        _write_csv(f_jct_path, jct_results["fcfs"],
                    ["timestamp", "fidelity", "JCT"])
        logger.info("Legacy CSVs written to %s", base_dir)


if __name__ == "__main__":
    main()
