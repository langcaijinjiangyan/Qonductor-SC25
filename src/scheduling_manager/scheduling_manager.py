import datetime as dt
import json
import logging
import pathlib
import queue
import time
from collections import defaultdict
from multiprocessing import Queue
from timeit import default_timer as timer
from typing import Any

from qiskit.providers import Backend

from src.scheduler.base_scheduler import BaseScheduler
from src.scheduler.multi_objective_scheduler import (
    MultiObjectiveScheduler,
    TranspilationLevel,
    ProblemType,
)
from src.utils.benchmark import get_fake_backends, get_benchmark_names

logger = logging.getLogger(__name__)

TIMEOUT = 10


class SchedulingManager:
    """
    Class for accepting scheduling jobs and scheduling them
    """

    def __init__(
        self,
        queue: Queue,
        data_folder: pathlib.Path,
        backends: list[Backend] = None,
        scheduler: BaseScheduler = None,
        scheduling_interval: int = 120,
        scheduling_threshold: int = 100,
    ):
        """
        Initialize the scheduling manager
        :param queue: Queue to accept jobs from
        :param data_folder: Folder to save scheduling statistics to
        :param backends: List of backends to schedule on
        :param scheduler: Scheduler to use
        :param scheduling_interval: Interval between scheduling jobs in seconds
        :param scheduling_threshold: Number of jobs to schedule at once
        """
        self.queue = queue
        self.data_folder = data_folder
        self.data_folder.mkdir(parents=True, exist_ok=True)
        self.backends = backends or get_fake_backends()[:8]
        self.scheduler = scheduler or MultiObjectiveScheduler(
            transpilation_level=TranspilationLevel.PRE_TRANSPILED,
            problem_type=ProblemType.DISCRETE,
        )
        self.scheduling_interval = scheduling_interval
        self.scheduling_threshold = scheduling_threshold
        self.next_scheduling_time = dt.datetime.now(
            dt.timezone.utc
        ) + dt.timedelta(seconds=self.scheduling_interval)

    def _save_statistics(self, statistics: Any, filename: str) -> None:
        """
        Save scheduling statistics to a file
        :param statistics: Statistics to save
        :param filename: Filename to save to
        """
        file_path = self.data_folder / filename
        logger.info("Saving statistics to %s", file_path)
        with open(file_path, "w+") as file:
            json.dump(statistics, file, indent=4)

    def run(self) -> None:
        """
        Monitor the queue and schedule jobs when the scheduling queue size
        threshold is reached or the scheduling interval has passed
        """
        self.scheduler.load_pre_transpiled_circuits(
            self.backends, get_benchmark_names(), [3, 4, 5]
        )
        all_metadata = []
        backend_execution_times = defaultdict(lambda: 0)
        job_waiting_times = []
        while True:
            try:
                if self.queue.qsize() >= self.scheduling_threshold or (
                    dt.datetime.now(dt.timezone.utc)
                    >= self.next_scheduling_time
                    and not self.queue.empty()
                ):
                    jobs = []
                    job_timestamps = []
                    while len(jobs) < self.scheduling_threshold:
                        try:
                            timestamp, job = self.queue.get_nowait()
                            job_timestamps.append(timestamp)
                            jobs.append(job)
                        except queue.Empty:
                            break
                    logger.info(
                        "Scheduling %d jobs on %d backends",
                        len(jobs),
                        len(self.backends),
                    )
                    assignments, _, metadata = self.scheduler.schedule(
                        jobs, self.backends
                    )
                    for timestamp in job_timestamps:
                        job_waiting_times.append(timer() - timestamp)
                    metadata["time"] = dt.datetime.now(
                        dt.timezone.utc
                    ).isoformat()
                    all_metadata.append(metadata)
                    for i, (job, backend) in enumerate(assignments):
                        backend.update_waiting_time(
                            metadata["solution_execution_times"][i]
                        )
                        backend_execution_times[backend.name] += metadata[
                            "solution_execution_times"
                        ][i]
                    logger.info("Jobs were scheduled")

                    self.next_scheduling_time = dt.datetime.now(
                        dt.timezone.utc
                    ) + dt.timedelta(seconds=self.scheduling_interval)
                elif (
                    dt.datetime.now(dt.timezone.utc)
                    >= self.next_scheduling_time
                    or self.queue.empty()
                ):
                    self.next_scheduling_time = dt.datetime.now(
                        dt.timezone.utc
                    ) + dt.timedelta(seconds=self.scheduling_interval)
                    time.sleep(1)
                else:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat(
            timespec="minutes"
        )
        self._save_statistics(all_metadata, f"metadata_{timestamp}.json")
        self._save_statistics(
            backend_execution_times, f"backend_times_{timestamp}.json"
        )
        self._save_statistics(
            job_waiting_times, f"job_waiting_times_{timestamp}.json"
        )
