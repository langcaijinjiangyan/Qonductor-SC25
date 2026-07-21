#!/usr/bin/env python3
"""Convert workflow/pure_quantum OpenQASM 2 inputs to OpenQASM 3."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOT = PROJECT_ROOT / "workflow" / "pure_quantum"

QREG_RE = re.compile(r"^qreg\s+([A-Za-z_][A-Za-z0-9_]*)\[(\d+)\];$")
CREG_RE = re.compile(r"^creg\s+([A-Za-z_][A-Za-z0-9_]*)\[(\d+)\];$")
MEASURE_RE = re.compile(
    r"^measure\s+([A-Za-z_][A-Za-z0-9_]*\[\d+\])\s*->\s*"
    r"([A-Za-z_][A-Za-z0-9_]*\[\d+\]);$"
)


def convert_qasm2_text(text: str) -> str:
    """Translate the simple MQTBench QASM2 dialect used here to QASM3."""
    out: list[str] = []
    saw_version = False
    saw_include = False

    for raw_line in text.splitlines():
        stripped = raw_line.strip()

        if stripped == "OPENQASM 2.0;":
            out.append("OPENQASM 3.0;")
            saw_version = True
            continue

        if stripped == 'include "qelib1.inc";':
            out.append('include "stdgates.inc";')
            saw_include = True
            continue

        qreg_match = QREG_RE.match(stripped)
        if qreg_match:
            name, size = qreg_match.groups()
            out.append(f"qubit[{size}] {name};")
            continue

        creg_match = CREG_RE.match(stripped)
        if creg_match:
            name, size = creg_match.groups()
            out.append(f"bit[{size}] {name};")
            continue

        measure_match = MEASURE_RE.match(stripped)
        if measure_match:
            qubit, bit = measure_match.groups()
            out.append(f"{bit} = measure {qubit};")
            continue

        out.append(raw_line)

    if not saw_version:
        out.insert(0, "OPENQASM 3.0;")
    if not saw_include:
        insert_at = 1 if out and out[0] == "OPENQASM 3.0;" else 0
        out.insert(insert_at, 'include "stdgates.inc";')

    return "\n".join(out).rstrip() + "\n"


def convert_file(path: Path) -> Path:
    qasm3_path = path.with_suffix(".qasm3")
    qasm3_path.write_text(convert_qasm2_text(path.read_text(encoding="utf-8")),
                          encoding="utf-8")
    return qasm3_path


def update_specs(root: Path, renamed: dict[str, str]) -> int:
    updated = 0
    for spec_path in sorted(root.rglob("spec_*.json")):
        data = json.loads(spec_path.read_text(encoding="utf-8"))
        circuits = data.get("circuits", {})
        changed = False
        for logical_id, qasm_name in list(circuits.items()):
            if qasm_name in renamed:
                circuits[logical_id] = renamed[qasm_name]
                changed = True
        if changed:
            spec_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            updated += 1
    return updated


def update_corpus_meta(root: Path, renamed: dict[str, str]) -> int:
    updated = 0
    for meta_path in sorted(root.rglob("corpus_meta.json")):
        data = json.loads(meta_path.read_text(encoding="utf-8"))
        changed = False
        for entry in data.get("entries", []):
            files = entry.get("circuit_files")
            if not isinstance(files, list):
                continue
            new_files = [renamed.get(name, name) for name in files]
            if new_files != files:
                entry["circuit_files"] = new_files
                changed = True
        if changed:
            meta_path.write_text(
                json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            updated += 1
    return updated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()

    root = args.root.resolve()
    converted = []
    renamed_by_dir: dict[Path, dict[str, str]] = {}

    for qasm_path in sorted(root.rglob("*.qasm")):
        if qasm_path.read_text(encoding="utf-8").lstrip().startswith("OPENQASM 3"):
            continue
        qasm3_path = convert_file(qasm_path)
        converted.append(qasm3_path)
        renamed_by_dir.setdefault(qasm_path.parent, {})[
            qasm_path.name
        ] = qasm3_path.name

    updated_specs = 0
    updated_meta = 0
    for directory, renamed in renamed_by_dir.items():
        updated_specs += update_specs(directory, renamed)
        updated_meta += update_corpus_meta(directory, renamed)

    print(f"converted_qasm={len(converted)}")
    print(f"updated_specs={updated_specs}")
    print(f"updated_corpus_meta={updated_meta}")


if __name__ == "__main__":
    main()
