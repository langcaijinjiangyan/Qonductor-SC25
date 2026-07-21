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
    transpilation_cache_key: str = ""
    parameter_bindings: dict[str, float] = field(default_factory=dict)

    def __json__(self):
        return {
            'circuits': self.circuits,
            'shots': self.shots,
            'id': self.id,
            'transpilation_cache_key': self.transpilation_cache_key,
            'parameter_bindings': self.parameter_bindings,
        }
    
    @classmethod
    def __json_decode__(cls, json_data):
        return cls(
            circuits=json_data['circuits'],
            shots=json_data['shots'],
            transpilation_cache_key=json_data.get(
                'transpilation_cache_key', ''
            ),
            parameter_bindings=json_data.get('parameter_bindings', {}),
        )


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
