import datetime
import logging
from pathlib import Path
from typing import List, Generator, Sequence

from qiskit import QuantumCircuit
from qiskit_ibm_provider import IBMProvider, IBMJob
from sqlalchemy import create_engine, ForeignKey, select
from sqlalchemy.orm import DeclarativeBase, Mapped, relationship, Session
from sqlalchemy.orm import mapped_column

from src.utils.circuit_archive import (
    load_circuit_from_archive,
    save_circuit_to_archive,
)

DATA_PATH = Path(__file__).absolute().parents[2] / "data" / "database"

DATABASE_PATH = DATA_PATH / "quantum_scheduler.db"
ARCHIVE_NAME = "circuits.zip"
DATABASE_URI = f"sqlite:///{DATABASE_PATH}"

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "job"
    backend_name: Mapped[str]
    backend_version: Mapped[str]
    id: Mapped[int] = mapped_column(primary_key=True)
    ibm_quantum_id: Mapped[str]
    date: Mapped[str]
    taken_time: Mapped[float]
    shots: Mapped[int]
    circuits: Mapped[List["Circuit"]] = relationship(lazy="selectin")


class Circuit(Base):
    __tablename__ = "circuit"
    __allow_unmapped__ = True

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]
    filename: Mapped[str]
    job_id: Mapped[int] = mapped_column(ForeignKey("job.id"))
    quantum_circuit: QuantumCircuit = None

    def load_circuit(self) -> None:
        """
        Load the circuit from the archive
        """
        self.quantum_circuit = load_circuit_from_archive(
            DATA_PATH / ARCHIVE_NAME, self.filename, unpack=True
        )


engine = create_engine(DATABASE_URI, echo=False)

# Ensure the database directory exists before creating tables.
DATA_PATH.mkdir(parents=True, exist_ok=True)
Base.metadata.create_all(engine)


def save_job_to_database(qiskit_job: IBMJob):
    """
    Save Qiskit job to the database
    :param qiskit_job: Job to be saved
    """
    # Open database session
    with Session(engine) as session:
        # Check if circuits exist
        circuits = []
        for index, qiskit_circuit in enumerate(qiskit_job.circuits()):
            circuit = Circuit(
                name=qiskit_circuit.name,
                filename=f"{qiskit_job.job_id()}-{index}.qasm",
            )
            save_circuit_to_archive(
                qiskit_circuit, circuit.filename, ARCHIVE_NAME, DATA_PATH
            )
            session.add(circuit)
            circuits.append(circuit)

        job_result = qiskit_job.result()

        # Save job
        job = Job(
            backend_name=job_result.backend_name,
            backend_version=job_result.backend_version,
            shots=qiskit_job.backend_options()["shots"],
            ibm_quantum_id=qiskit_job.job_id(),
            date=qiskit_job.creation_date()
            .astimezone(datetime.timezone.utc)
            .isoformat(),
            taken_time=job_result.time_taken,
            circuits=circuits,
        )
        session.add(job)

        # Commit changes
        session.commit()


def get_jobs_from_database() -> Sequence[Job]:
    """
    Retrieve all jobs from the database
    :return: List of jobs
    """
    with Session(engine) as session:
        return session.scalars(select(Job)).all()


def check_job_exists(qiskit_job: IBMJob) -> bool:
    """
    Check if job is in database
    :param qiskit_job: Job to be checked
    :return: True if job is in database, False otherwise
    """
    with Session(engine) as session:
        return (
            session.scalars(
                select(Job).where(Job.ibm_quantum_id == qiskit_job.job_id())
            ).first()
            is not None
        )


def extract_jobs_from_ibm_quantum(
    batch_size: int = 10,
    account_name: str | None = None,
) -> Generator[IBMJob, None, None]:
    """
    Iteratively extract all jobs from IBM Quantum
    :param batch_size: Batch size
    :param account_name: Name of the account
    :return: List of jobs
    """
    provider = IBMProvider(name=account_name)
    skip = 0
    while True:
        jobs = provider.jobs(limit=batch_size, skip=skip, status="completed")
        if len(jobs) == 0:
            break
        yield from [job for job in jobs if job.done()]
        skip += batch_size


def extract_jobs_from_archive() -> Generator[Job, None, None]:
    """
    Iteratively extract all jobs from the archive
    :return: List of jobs
    """
    with Session(engine) as session:
        stmt = select(Job).execution_options(yield_per=101)
        for job in session.scalars(stmt):
            for circuit in job.circuits:
                circuit.load_circuit()
            yield job


def save_missing_jobs_to_database(account_name: str | None = None) -> None:
    """
    Saves all jobs that are not yet in the database to the database.
    :param account_name: Name of the account
    """
    for job in extract_jobs_from_ibm_quantum(account_name=account_name):
        if not check_job_exists(job):
            logger.info("Saving job %s to database", job.job_id())
            save_job_to_database(job)
