import os
import shutil
import numpy as np
import sys

from supermarq.benchmarks.ghz import GHZ
from qiskit.providers.fake_provider import *
from qiskit import QuantumCircuit, transpile

def move_and_rename_file(src_path, dst_dir, new_name):
    dst_path = os.path.join(dst_dir, new_name)
    shutil.copy2(src_path, dst_path)


def iterate_files_in_directory(venv_path, store_folder):
    bechmark = GHZ(6)
    circuit = bechmark.qiskit_circuit()
    fidelities = []

    calibration_data_folder = os.path.join("data/ibm_status/temporal_variance/calibration_data")
    if os.path.exists(calibration_data_folder):
        for file_name in os.listdir(calibration_data_folder):
            filename = os.path.join(calibration_data_folder, file_name)
            if os.path.isfile(filename):
                move_and_rename_file(filename, os.path.join(venv_path, "lib/python3.11/site-packages/qiskit/providers/fake_provider/backends/perth"), "props_perth.json")
    
                backend = FakePerth()

                transpiled_circuit = transpile(circuit, backend)

                tmp_fidelites = []
                for i in range(5):
                    result = backend.run(transpiled_circuit, shots=8192).result().get_counts()
                    tmp_fidelites.append(bechmark.score(result))

                fidelities.append(np.mean(tmp_fidelites))


    with open(store_folder + "/perth_fidelities_6_qubits.csv", 'w') as file:
        file.write("fidelity\n")
        for item in fidelities:
            file.write(str(item) + '\n')


iterate_files_in_directory(sys.argv[1], "data/ibm_status/temporal_variance/")