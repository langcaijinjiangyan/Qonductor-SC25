# efficient_su2_ansatz_with_random_parameters

Converted from `qslurm_workloads/hybrid_6q/efficient_su2_ansatz_with_random_parameters`.

This folder mirrors the qslurm workload input files, but the executable entry
points are Qonductor workflow-native Python drivers:

- `submit_6.py` builds/registers a `WorkflowImage` and can emit or submit a
  `HybridWorkflow` manifest.
- Pure quantum workloads become a one-step QUANTUM workflow.
- Hybrid workloads become a compact CLASSICAL driver workflow that creates
  dynamic `QuantumJob` children at runtime.

Examples:

```bash
python submit_6.py --emit-manifest
python submit_6.py --submit --shots 1024
```
