# quantum_phase_estimation_qpe_exact

Converted from `qslurm_workloads/pure_quantum/quantum_phase_estimation_qpe_exact`.

This folder mirrors the qslurm workload input files, but the executable entry
points are Qonductor workflow-native Python drivers:

- `submit_12.py` builds/registers a `WorkflowImage` and can emit or submit a
  `HybridWorkflow` manifest.
- Pure quantum workloads become a one-step QUANTUM workflow.
- Hybrid workloads become a compact CLASSICAL driver workflow that creates
  dynamic `QuantumJob` children at runtime.

Examples:

```bash
python submit_12.py --emit-manifest
python submit_12.py --submit --shots 1024
```
