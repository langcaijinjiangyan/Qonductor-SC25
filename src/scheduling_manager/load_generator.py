import datetime as dt
import json
import logging
import os
import pathlib
import time
from queue import Queue
from timeit import default_timer as timer
from typing import Any

import numpy

from src.utils.benchmark import generate_random_job, get_benchmark_names

logger = logging.getLogger(__name__)

TIMEOUT = 10
POOL_SIZE = 100


class LoadGenerator:
    """
    Class for generating jobs and submitting them to the scheduling manager
    """

    def __init__(
        self,
        queue: Queue,
        data_folder: pathlib.Path,
        pool_size: int = POOL_SIZE,
    ):
        """
        Initialize the load generator
        :param queue: Queue to submit jobs to
        :param data_folder: Folder to save scheduling statistics to
        :param pool_size: Number of jobs in the pre-generated pool
        """
        self.scheduler_queue = queue
        self.data_folder = data_folder
        self.pool_size = pool_size
        self.job_pool = []
        seed = int(os.environ.get("SEED", time.time()))
        self.random_generator = numpy.random.default_rng(seed)
        logger.info("Random seed: %d", seed)

    def run(self, job_count: int = 1000, frequency: float = 1.0) -> None:
        """
        Run the load generator
        :param job_count: Number of jobs to generate
        :param frequency: Frequency of job generation in seconds
        """
        self._generate_jobs()
        queue_size = []
        logger.info("Running job submission")
        for _ in range(job_count):
            queue_size.append(
                {
                    "time": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "size": self.scheduler_queue.qsize(),
                }
            )
            try:
                job = self.random_generator.choice(self.job_pool)
                self.scheduler_queue.put((timer(), job))
                logger.info(
                    "Submitted job %s, queue size %d",
                    job.id,
                    self.scheduler_queue.qsize(),
                )

                time.sleep(frequency)
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt")
                break
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat(
            timespec="minutes"
        )
        self._save_statistics(queue_size, f"queue_size_{timestamp}.json")

    def _generate_jobs(self) -> None:
        """
        Generate jobs and add them to the job pool
        """
        for _ in range(self.pool_size):
            circuit_count = int(self.random_generator.normal(50, 20))
            circuit_count = max(1, min(circuit_count, 100))
            job = generate_random_job(
                min_backend_size=5,
                benchmark_names=get_benchmark_names(),
                random_generator=self.random_generator,
                circuit_count=circuit_count,
                shots=4000,
            )
            self.job_pool.append(job)

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
