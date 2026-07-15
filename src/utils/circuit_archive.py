import logging
import pathlib
import zipfile

from qiskit import QuantumCircuit, qasm3

logger = logging.getLogger(__name__)


def unpack_archive(
    archive_path: pathlib.Path, saving_folder: pathlib.Path
) -> None:
    """
    Unpack an archive
    :param archive_path: Path to the archive
    :param saving_folder: Folder to save the archive to
    """
    saving_folder.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        archive.extractall(saving_folder)


def save_circuit_to_archive(
    circuit: QuantumCircuit,
    filename: str,
    archive_name: str,
    saving_folder: pathlib.Path,
) -> None:
    """
    Save a circuit to an archive
    :param circuit: Circuit to be saved
    :param filename: Circuit filename
    :param archive_name: Archive filename
    :param saving_folder: Folder to save the circuit to
    """
    circuit_path = saving_folder / filename
    archive_path = saving_folder / archive_name

    try:
        circuit.qasm(formatted=False, filename=str(circuit_path))
    except Exception as ex:
        logger.warning("Failed to save circuit in QASM2 format: %s", ex)
        with open(circuit_path, "w+") as circuit_file:
            qasm3.dump(circuit, circuit_file)

    # Create archive if it does not exist
    if not archive_path.exists():
        with zipfile.ZipFile(
            archive_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9
        ) as archive:
            archive.write(circuit_path, arcname=filename)
    else:
        with zipfile.ZipFile(
            archive_path, "a", zipfile.ZIP_DEFLATED, compresslevel=9
        ) as archive:
            archive.write(circuit_path, arcname=filename)

    # Remove circuit file
    circuit_path.unlink()


def load_circuit_from_archive(
    archive_path: pathlib.Path, filename: str, unpack: bool = False
) -> QuantumCircuit | None:
    """
    Load a circuit from an archive
    :param archive_path: Path to the archive
    :param filename: Circuit filename
    :param unpack: Unpack the archive or load directly from it
    :return: Loaded circuit or None if it does not exist
    """
    if unpack:
        unpacked_folder = archive_path.parent / f"{archive_path.stem}_unpacked"
        if not unpacked_folder.exists():
            logger.info("Unpacking archive %s", archive_path.name)
            unpack_archive(archive_path, unpacked_folder)
        if not (unpacked_folder / filename).exists():
            logger.warning("Circuit %s not found", unpacked_folder / filename)
            return None
        circuit_path = unpacked_folder / filename

        try:
            circuit = QuantumCircuit.from_qasm_file(str(circuit_path))
        except Exception as ex:
            logger.warning("Failed to load circuit in QASM2 format: %s", ex)
            circuit = qasm3.load(str(circuit_path))
    else:
        with zipfile.ZipFile(archive_path) as archive:
            if filename not in archive.namelist():
                return None
            with archive.open(filename) as circuit_file:
                circuit_string = circuit_file.read().decode("utf-8")
        try:
            circuit = QuantumCircuit.from_qasm_str(circuit_string)
        except Exception as ex:
            logger.warning("Failed to load circuit in QASM2 format: %s", ex)
            circuit = qasm3.loads(circuit_string)

    return circuit
