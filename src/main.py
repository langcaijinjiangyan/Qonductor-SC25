import logging
from pathlib import Path

import numpy as np
from matplotlib import pyplot as plt
from qiskit_ibm_provider import IBMProvider

from src.analysis.execution_time_analysis import (
    run_benchmark_varying_circuit_counts,
)

np.seterr(all="ignore")
logging.basicConfig(level=logging.INFO)
logging.getLogger("qiskit").setLevel(logging.WARNING)
logging.getLogger("qiskit_ibm_provider").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
if __name__ == "__main__":
    provider = IBMProvider()
    backends = provider.backends(simulator=False, operational=True)
    backends = [
        backend
        for backend in backends
        if backend.status().status_msg == "active"
    ]
    backends = sorted(
        backends, key=lambda backend: backend.status().pending_jobs
    )
    shots = 4000
    circuit_counts = [1, 20, 40, 60, 80, 100]
    for backend in backends:
        logger.info("Using %s backend", backend.name)
        taken_times, estimated_times = run_benchmark_varying_circuit_counts(
            backend, shots, circuit_counts
        )
        plt.plot(circuit_counts, taken_times, label="Taken time")
        plt.plot(circuit_counts, estimated_times, label="Estimated time")
        plt.xlabel("Circuit count")
        plt.ylabel("Time (s)")
        plt.title(f"{backend.name} ({shots} shots)")
        plt.legend()
        plot_path = (
            Path(__file__).absolute().parents[2]
            / "plots"
            / "execution_time"
            / f"plots/{backend.name}.png"
        )
