import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TypeAlias, Any

from qiskit import QuantumCircuit
from qiskit.providers import Backend


@dataclass
class SchedulingJob:
    """
    Dataclass for a scheduling job
    """

    circuits: list[QuantumCircuit]
    shots: int = 4000
    id: uuid.UUID = field(default_factory=uuid.uuid4, init=False)

    def __json__(self):
        return {
            'circuits': self.circuits,
            'shots': self.shots,
            'id': self.id
        }
    
    @classmethod
    def __json_decode__(cls, json_data):
        return cls(json_data['circuits'], json_data['shots'], json_data['id'])


Assignment: TypeAlias = tuple[SchedulingJob, Backend]


class BaseScheduler(ABC):
    """
    Base class for scheduling circuits
    """

    @abstractmethod
    def schedule(
        self,
        jobs: list[SchedulingJob],
        backends: list[Backend],
        **kwargs,
    ) -> tuple[list[Assignment], list[SchedulingJob], Any]:
        """
        Schedule a list of jobs to a list of backends
        :param jobs: Jobs to be scheduled
        :param backends: Backends to be scheduled to
        :param kwargs: Additional arguments
        :return: A list of assignments, a list of jobs
        that could not be scheduled and scheduling metadata
        """
        ...
