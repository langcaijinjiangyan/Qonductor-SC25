# qslurm Workload Python Workflow Conversion

Source: `qslurm_workloads`
Output: `workflow`

## Structure Summary

- `qslurm_workloads/pure_quantum/<benchmark>/` stores one or more fixed-size pure quantum tasks as `spec_N.json`, QASM2 files, and legacy `submit_N.sh`/C launchers.
- `qslurm_workloads/hybrid/<benchmark>/` stores 12-qubit parameterized QASM3 tasks plus `parameters_12.json` and legacy MPI/C hybrid drivers.
- `qslurm_workloads/hybrid_6q/<benchmark>/` has the same hybrid shape, but with `spec_6.json` and `parameters_6.json`.
- Converted workflow folders mirror those source folders under `workflow/` and replace Slurm submission with Python entrypoints.

## Python Workflow Characteristics

- Submission is workflow-native: drivers build/register a `WorkflowImage` and emit a `HybridWorkflow` manifest.
- Classical work remains a normal Kubernetes/classical step; dynamic quantum evaluations are represented as `QuantumJob` children.
- Pure quantum workloads are represented as a single QUANTUM DAG node carrying `logicalCircuitId`, QASM text, qubit count, and shots.
- Hybrid QAOA/VQE workloads keep the Python SPSA driver compact and pass QASM/parameter metadata through `workflowInputs`.
- Original `spec`, `parameters`, and QASM filenames are preserved in each converted folder for traceability.

## Conversion Counts

- Converted folders: 21
- Converted spec entries: 73
- Pure quantum workflow entries: 65
- Hybrid workflow entries: 8
