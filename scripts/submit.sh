#!/usr/bin/env bash
# =============================================================================
# submit.sh
#
# End-to-end workload submission script for the Qonductor cluster.
#
# Generates a workload plan via generate_workloads.py, submits tasks according
# to the arrival-time trace, waits for all workflows to complete (no timeout),
# and exports scheduling metrics.
#
# Usage:
#   ./scripts/submit.sh \
#       --workload-dir workflow \
#       --total-jobs 200 \
#       --submit-window-sec 300 \
#       [--hybrid-job-ratio 0.2] \
#       [--arrival-mode poisson] \
#       [--seed 1] \
#       [--output-dir data/batch_results] \
#       [--dry-run]
#
# Requirements:
#   - kubectl with access to the Qonductor cluster
#   - Python 3.11+ with project dependencies installed
#   - scripts/generate_workloads.py (workload plan generator)
#   - scripts/export_metrics.py (metrics collector)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step()  { echo -e "${CYAN}[STEP]${NC}  $*"; }

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# ---- Script-level parameters ----
WORKLOAD_DIR="${PROJECT_ROOT}/workflow"          # maps to --corpus-root
OUTPUT_DIR="${PROJECT_ROOT}/data/batch_results"   # parent directory for batch outputs
METRICS_PORT=9100
POLL_SECONDS=10                                   # wait-poll interval (seconds)

# ---- Generator parameters (mirror generate_workloads.py defaults) ----
CORPUS_INDEX=""                                   # pre-built corpus index (empty = auto-discover)
TOTAL_JOBS=200
SUBMIT_WINDOW_SEC=300.0
HYBRID_JOB_RATIO=0.2
HYBRID_SUBDIR="hybrid"
PURE_SIZE_RATIO="4,3,2"
SMALL_QUBITS="2,4"
MEDIUM_QUBITS="8,12"
LARGE_QUBITS="16"
ARRIVAL_MODE="poisson"
SEED=1
ALLOW_HYBRID_LEGACY="true"
RUNTIME_MODEL="qasm_gate_proxy_v1"
BASE_MS=1.0
GATE_WEIGHT=0.01
TWO_QUBIT_WEIGHT=0.05
QUBIT_WEIGHT=0.1
HYBRID_ITERATION_FACTOR=5.0
PARAMETER_WEIGHT=0.02
DRY_RUN=""                                        # empty = false; set to "1" via --dry-run

# ---- kubectl detection ----
KUBECTL="${PROJECT_ROOT}/kubectl"
if [[ ! -x "${KUBECTL}" ]]; then
    if command -v kubectl &>/dev/null; then
        KUBECTL="$(command -v kubectl)"
    else
        error "kubectl not found at ${KUBECTL} or in PATH"
        exit 1
    fi
fi
info "Using kubectl: ${KUBECTL}"

# ---------------------------------------------------------------------------
# Usage
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Generate a workload plan via generate_workloads.py, submit tasks on schedule,
wait for all workflows to complete (no timeout), and export metrics.

Required:
  --workload-dir DIR       Workload corpus root directory (maps to --corpus-root)

Generator options:
  --total-jobs N            Total number of jobs to generate (default: ${TOTAL_JOBS})
  --submit-window-sec SEC   Submission time window in seconds (default: ${SUBMIT_WINDOW_SEC})
  --hybrid-job-ratio RATIO  Fraction of jobs that are hybrid VQE/QAOA (default: ${HYBRID_JOB_RATIO})
  --hybrid-subdir NAME      Subdirectory for hybrid entries (default: ${HYBRID_SUBDIR})
  --pure-size-ratio RATIO   small:medium:large ratio for pure quantum (default: ${PURE_SIZE_RATIO})
  --small-qubits LIST       Comma-separated small qubit counts (default: ${SMALL_QUBITS})
  --medium-qubits LIST      Comma-separated medium qubit counts (default: ${MEDIUM_QUBITS})
  --large-qubits LIST       Comma-separated large qubit counts (default: ${LARGE_QUBITS})
  --arrival-mode MODE       Arrival distribution: deterministic|poisson (default: ${ARRIVAL_MODE})
  --seed N                  Random seed (default: ${SEED})
  --allow-hybrid-legacy BOOL Include legacy hybrid entries: true|false (default: ${ALLOW_HYBRID_LEGACY})
  --corpus-index PATH       Pre-built corpus index JSON (default: auto-discover)

Runtime estimation options:
  --runtime-model MODEL     Model name for provenance (default: ${RUNTIME_MODEL})
  --base-ms MS              Base runtime in ms (default: ${BASE_MS})
  --gate-weight W           Per-gate weight (default: ${GATE_WEIGHT})
  --two-qubit-weight W      Per-2q-gate weight (default: ${TWO_QUBIT_WEIGHT})
  --qubit-weight W          Per-qubit weight (default: ${QUBIT_WEIGHT})
  --hybrid-iteration-factor F  Hybrid iteration multiplier (default: ${HYBRID_ITERATION_FACTOR})
  --parameter-weight W      Per-parameter weight (default: ${PARAMETER_WEIGHT})

Script options:
  --output-dir DIR          Parent directory for batch results (default: data/batch_results)
  --poll-seconds SEC        Poll interval for workflow status checks (default: ${POLL_SECONDS})
  --dry-run                 Generate workload plans only; do not submit
  -h, --help                Show this help message
EOF
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload-dir)
            WORKLOAD_DIR="$2"; shift 2 ;;
        --output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        --corpus-index)
            CORPUS_INDEX="$2"; shift 2 ;;
        --total-jobs)
            TOTAL_JOBS="$2"; shift 2 ;;
        --submit-window-sec)
            SUBMIT_WINDOW_SEC="$2"; shift 2 ;;
        --hybrid-job-ratio)
            HYBRID_JOB_RATIO="$2"; shift 2 ;;
        --hybrid-subdir)
            HYBRID_SUBDIR="$2"; shift 2 ;;
        --pure-size-ratio)
            PURE_SIZE_RATIO="$2"; shift 2 ;;
        --small-qubits)
            SMALL_QUBITS="$2"; shift 2 ;;
        --medium-qubits)
            MEDIUM_QUBITS="$2"; shift 2 ;;
        --large-qubits)
            LARGE_QUBITS="$2"; shift 2 ;;
        --arrival-mode)
            ARRIVAL_MODE="$2"; shift 2 ;;
        --seed)
            SEED="$2"; shift 2 ;;
        --allow-hybrid-legacy)
            ALLOW_HYBRID_LEGACY="$2"; shift 2 ;;
        --runtime-model)
            RUNTIME_MODEL="$2"; shift 2 ;;
        --base-ms)
            BASE_MS="$2"; shift 2 ;;
        --gate-weight)
            GATE_WEIGHT="$2"; shift 2 ;;
        --two-qubit-weight)
            TWO_QUBIT_WEIGHT="$2"; shift 2 ;;
        --qubit-weight)
            QUBIT_WEIGHT="$2"; shift 2 ;;
        --hybrid-iteration-factor)
            HYBRID_ITERATION_FACTOR="$2"; shift 2 ;;
        --parameter-weight)
            PARAMETER_WEIGHT="$2"; shift 2 ;;
        --poll-seconds)
            POLL_SECONDS="$2"; shift 2 ;;
        --dry-run)
            DRY_RUN="1"; shift ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            error "Unknown option: $1"
            usage; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

# Resolve workload dir relative to project root
if [[ ! "${WORKLOAD_DIR}" = /* ]]; then
    WORKLOAD_DIR="${PROJECT_ROOT}/${WORKLOAD_DIR}"
fi

if [[ ! -d "${WORKLOAD_DIR}" ]]; then
    error "Workload directory not found: ${WORKLOAD_DIR}"
    exit 1
fi

if [[ "${TOTAL_JOBS}" -lt 1 ]]; then
    error "--total-jobs must be >= 1"
    exit 1
fi

# Validate arrival-mode choice
if [[ "${ARRIVAL_MODE}" != "deterministic" && "${ARRIVAL_MODE}" != "poisson" ]]; then
    error "--arrival-mode must be 'deterministic' or 'poisson', got: ${ARRIVAL_MODE}"
    exit 1
fi

# Validate allow-hybrid-legacy choice
if [[ "${ALLOW_HYBRID_LEGACY}" != "true" && "${ALLOW_HYBRID_LEGACY}" != "false" ]]; then
    error "--allow-hybrid-legacy must be 'true' or 'false', got: ${ALLOW_HYBRID_LEGACY}"
    exit 1
fi

# Resolve output dir relative to project root
if [[ ! "${OUTPUT_DIR}" = /* ]]; then
    OUTPUT_DIR="${PROJECT_ROOT}/${OUTPUT_DIR}"
fi

# ---------------------------------------------------------------------------
# Derived constants
# ---------------------------------------------------------------------------
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BATCH_ID="batch_${TIMESTAMP}"
BATCH_OUT_DIR="${OUTPUT_DIR}/${BATCH_ID}"
SUBMISSION_LOG="${BATCH_OUT_DIR}/submission_log.csv"
METRICS_DIR="${BATCH_OUT_DIR}/metrics"

PYTHON_CMD="python3"
GENERATOR="${PROJECT_ROOT}/scripts/generate_workloads.py"
EXPORT_SCRIPT="${PROJECT_ROOT}/scripts/export_metrics.py"

# Output file paths (generated by generate_workloads.py)
OUTPUT_MANIFEST="${BATCH_OUT_DIR}/manifest.jsonl"
OUTPUT_TRACE="${BATCH_OUT_DIR}/trace.jsonl"
OUTPUT_SUMMARY="${BATCH_OUT_DIR}/generation_summary.json"

# ---------------------------------------------------------------------------
# Print configuration
# ---------------------------------------------------------------------------
echo ""
echo "=============================================================================="
echo "  Qonductor Workload Submission"
echo "=============================================================================="
echo "  Batch ID:       ${BATCH_ID}"
echo "  Workload dir:   ${WORKLOAD_DIR}"
echo "  Total jobs:     ${TOTAL_JOBS}"
echo "  Submit window:  ${SUBMIT_WINDOW_SEC}s"
echo "  Arrival mode:   ${ARRIVAL_MODE}"
echo "  Hybrid ratio:   ${HYBRID_JOB_RATIO}"
echo "  Hybrid subdir:  ${HYBRID_SUBDIR}"
echo "  Pure size ratio: ${PURE_SIZE_RATIO} (small:medium:large)"
echo "  Seed:           ${SEED}"
echo "  Dry run:        $([[ -n "${DRY_RUN}" ]] && echo "yes" || echo "no")"
echo "  Output dir:     ${BATCH_OUT_DIR}"
echo "=============================================================================="
echo ""

# ---------------------------------------------------------------------------
# Step 1: Generate workload plans via generate_workloads.py
# ---------------------------------------------------------------------------
step "Step 1/5: Generating workload plans ..."

mkdir -p "${BATCH_OUT_DIR}"

# Build corpus-index argument (only if explicitly provided)
CORPUS_INDEX_ARG=()
if [[ -n "${CORPUS_INDEX}" ]]; then
    CORPUS_INDEX_ARG=(--corpus-index "${CORPUS_INDEX}")
fi

# Build dry-run argument
DRY_RUN_ARG=()
if [[ -n "${DRY_RUN}" ]]; then
    DRY_RUN_ARG=(--dry-run)
fi

if ! "${PYTHON_CMD}" "${GENERATOR}" \
    --corpus-root "${WORKLOAD_DIR}" \
    "${CORPUS_INDEX_ARG[@]}" \
    --output-manifest "${OUTPUT_MANIFEST}" \
    --output-trace "${OUTPUT_TRACE}" \
    --output-summary "${OUTPUT_SUMMARY}" \
    --total-jobs "${TOTAL_JOBS}" \
    --submit-window-sec "${SUBMIT_WINDOW_SEC}" \
    --hybrid-job-ratio "${HYBRID_JOB_RATIO}" \
    --hybrid-subdir "${HYBRID_SUBDIR}" \
    --pure-size-ratio "${PURE_SIZE_RATIO}" \
    --small-qubits "${SMALL_QUBITS}" \
    --medium-qubits "${MEDIUM_QUBITS}" \
    --large-qubits "${LARGE_QUBITS}" \
    --arrival-mode "${ARRIVAL_MODE}" \
    --seed "${SEED}" \
    --allow-hybrid-legacy "${ALLOW_HYBRID_LEGACY}" \
    --runtime-model "${RUNTIME_MODEL}" \
    --base-ms "${BASE_MS}" \
    --gate-weight "${GATE_WEIGHT}" \
    --two-qubit-weight "${TWO_QUBIT_WEIGHT}" \
    --qubit-weight "${QUBIT_WEIGHT}" \
    --hybrid-iteration-factor "${HYBRID_ITERATION_FACTOR}" \
    --parameter-weight "${PARAMETER_WEIGHT}" \
    "${DRY_RUN_ARG[@]}" 2>&1 | tee "${BATCH_OUT_DIR}/generation.log"; then
    error "Workload generation failed — see ${BATCH_OUT_DIR}/generation.log"
    exit 1
fi

# If dry-run, we're done after generation
if [[ -n "${DRY_RUN}" ]]; then
    echo ""
    info "Dry-run complete. No workflows were submitted."
    info "Generation log: ${BATCH_OUT_DIR}/generation.log"
    info "Manifest:       ${OUTPUT_MANIFEST}"
    info "Trace:          ${OUTPUT_TRACE}"
    info "Summary:        ${OUTPUT_SUMMARY}"
    exit 0
fi

# Extract manifest directory from generator stdout
MANIFEST_DIR=$(awk -F= '/manifest_dir=/{print $2}' "${BATCH_OUT_DIR}/generation.log" | head -1)
if [[ -z "${MANIFEST_DIR}" ]]; then
    # Fallback: compute from output-manifest path convention
    # workflow_manifest_dir() = output_manifest.parent / f"{output_manifest.stem}_workflow_manifests"
    MANIFEST_DIR="${BATCH_OUT_DIR}/manifest_workflow_manifests"
fi

# Extract actual job count from generator stdout
ACTUAL_JOBS=$(awk -F= '/total_jobs=/{print $2}' "${BATCH_OUT_DIR}/generation.log" | tail -1)
if [[ -z "${ACTUAL_JOBS}" ]]; then
    ACTUAL_JOBS="${TOTAL_JOBS}"
fi

info "Generated ${ACTUAL_JOBS} workload(s)"
info "Manifest dir: ${MANIFEST_DIR}"

# ---------------------------------------------------------------------------
# Step 2: Inject batch_id label into YAML manifests
# ---------------------------------------------------------------------------
step "Step 2/5: Adding batch_id labels to workflow manifests ..."

if ! "${PYTHON_CMD}" -c "
import sys, yaml, json, os

manifest_path = '${OUTPUT_MANIFEST}'
batch_id = '${BATCH_ID}'

if not os.path.exists(manifest_path):
    print(f'ERROR: manifest not found: {manifest_path}', file=sys.stderr)
    sys.exit(1)

count = 0
with open(manifest_path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        entry = json.loads(line)
        yaml_path = entry.get('workflow_manifest', '')
        if not yaml_path or not os.path.exists(yaml_path):
            print(f'  WARNING: YAML not found, skipping: {yaml_path}', file=sys.stderr)
            continue
        # Read, inject batch_id label, write back
        with open(yaml_path) as yf:
            doc = yaml.safe_load(yf) or {}
        doc.setdefault('metadata', {})
        doc['metadata'].setdefault('labels', {})
        doc['metadata']['labels']['batch_id'] = batch_id
        with open(yaml_path, 'w') as yf:
            yaml.safe_dump(doc, yf, sort_keys=False)
        count += 1

print(f'Injected batch_id={batch_id} into {count} workflow manifest(s)')
" 2>&1 | tee -a "${BATCH_OUT_DIR}/generation.log"; then
    error "Failed to inject batch_id labels"
    exit 1
fi

info "batch_id labels injected successfully"

# ---------------------------------------------------------------------------
# Step 3: Read trace queue and submit on schedule
# ---------------------------------------------------------------------------
step "Step 3/5: Submitting workflows on schedule ..."

# Initialise submission log
echo "actual_submit_time,arrival_time_ms,submit_order,logical_job_id,workflow_name,workflow_manifest,status" > "${SUBMISSION_LOG}"

# Convert JSONL trace to TSV for efficient bash parsing
TRACE_TSV="${BATCH_OUT_DIR}/trace.tsv"
"${PYTHON_CMD}" -c "
import json, sys
with open('${OUTPUT_TRACE}') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        e = json.loads(line)
        print('\t'.join(str(v) for v in [
            e['arrival_time_ms'],
            e['submit_order'],
            e['logical_job_id'],
            e['workflow_name'],
            e['workflow_manifest'],
        ]))
" > "${TRACE_TSV}"

# Read trace into bash arrays
TRACE_COUNT=0
declare -a TRACE_ARRIVAL_MS TRACE_ORDER TRACE_JOB_ID TRACE_WF_NAME TRACE_MANIFEST

while IFS=$'\t' read -r arrival order job_id wf_name manifest; do
    [[ -z "${arrival}" ]] && continue
    TRACE_ARRIVAL_MS+=("${arrival}")
    TRACE_ORDER+=("${order}")
    TRACE_JOB_ID+=("${job_id}")
    TRACE_WF_NAME+=("${wf_name}")
    TRACE_MANIFEST+=("${manifest}")
    TRACE_COUNT=$((TRACE_COUNT + 1))
done < "${TRACE_TSV}"

if [[ "${TRACE_COUNT}" -eq 0 ]]; then
    error "Trace is empty — no tasks to submit"
    exit 1
fi

info "Loaded ${TRACE_COUNT} task(s) from trace"

# Submission timing
START_TIME=$(date +%s.%N)

info "Submission window starts now"
info "Press Ctrl+C to abort submission (already-submitted workflows will keep running)"

# Trap Ctrl+C: stop submitting but don't kill already-submitted workflows
ABORT_SUBMISSION=0
trap 'warn "Aborting submission — already-submitted workflows will keep running"; ABORT_SUBMISSION=1' INT

SUBMITTED=0
FAILED_SUBMIT=0

for ((i = 0; i < TRACE_COUNT; i++)); do
    if [[ "${ABORT_SUBMISSION}" -eq 1 ]]; then
        break
    fi

    ARRIVAL_MS="${TRACE_ARRIVAL_MS[$i]}"

    # Calculate target wall-clock time: START_TIME + (arrival_ms / 1000)
    TARGET_TIME=$(awk "BEGIN {printf \"%.3f\", ${START_TIME} + ${ARRIVAL_MS} / 1000.0}")

    # Sleep until target time (if in the future)
    NOW=$(date +%s.%N)
    SLEEP_DURATION=$(awk "BEGIN {d = ${TARGET_TIME} - ${NOW}; if (d > 0) printf \"%.3f\", d; else print 0}")

    if [[ "$(echo "${SLEEP_DURATION} > 0.001" | bc -l 2>/dev/null || echo 0)" == "1" ]]; then
        sleep "${SLEEP_DURATION}"
    fi

    ACTUAL_SUBMIT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
    YAML_PATH="${TRACE_MANIFEST[$i]}"

    if [[ ! -f "${YAML_PATH}" ]]; then
        error "  [${i}/$((TRACE_COUNT - 1))] MISSING YAML: ${YAML_PATH}"
        FAILED_SUBMIT=$((FAILED_SUBMIT + 1))
        echo "${ACTUAL_SUBMIT_TIME},${ARRIVAL_MS},${TRACE_ORDER[$i]},${TRACE_JOB_ID[$i]},${TRACE_WF_NAME[$i]},${YAML_PATH},FAILED_YAML_MISSING" >> "${SUBMISSION_LOG}"
        continue
    fi

    # Submit via kubectl
    if KUBECTL_OUT=$("${KUBECTL}" apply -f "${YAML_PATH}" 2>&1); then
        info "  [${i}/$((TRACE_COUNT - 1))] ✓ ${TRACE_WF_NAME[$i]}  (arrival=${ARRIVAL_MS}ms, order=${TRACE_ORDER[$i]})"
        echo "${ACTUAL_SUBMIT_TIME},${ARRIVAL_MS},${TRACE_ORDER[$i]},${TRACE_JOB_ID[$i]},${TRACE_WF_NAME[$i]},${YAML_PATH},SUCCESS" >> "${SUBMISSION_LOG}"
        SUBMITTED=$((SUBMITTED + 1))
    else
        error "  [${i}/$((TRACE_COUNT - 1))] ✗ ${TRACE_WF_NAME[$i]}  — kubectl apply failed"
        FAILED_SUBMIT=$((FAILED_SUBMIT + 1))
        echo "${ACTUAL_SUBMIT_TIME},${ARRIVAL_MS},${TRACE_ORDER[$i]},${TRACE_JOB_ID[$i]},${TRACE_WF_NAME[$i]},${YAML_PATH},FAILED_KUBECTL" >> "${SUBMISSION_LOG}"
    fi
done

END_SUBMIT_TIME=$(date +%s.%N)
SUBMIT_DURATION=$(awk "BEGIN {printf \"%.1f\", ${END_SUBMIT_TIME} - ${START_TIME}}")

# Remove the trap
trap - INT

echo ""
info "Submission phase complete: ${SUBMITTED} submitted, ${FAILED_SUBMIT} failed (${SUBMIT_DURATION}s elapsed)"
info "Submission log: ${SUBMISSION_LOG}"

if [[ "${SUBMITTED}" -eq 0 ]]; then
    error "No workflows were submitted successfully — aborting"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 4: Wait for all workflows to complete (NO timeout)
# ---------------------------------------------------------------------------
step "Step 4/5: Waiting for all workflows to complete (no timeout) ..."

POLL_INTERVAL="${POLL_SECONDS}"

info "Polling every ${POLL_INTERVAL}s for batch_id=${BATCH_ID}"
info "No timeout — waiting indefinitely until all ${SUBMITTED} submitted workflows reach terminal state."

while true; do
    WF_STATUS=$("${KUBECTL}" get hybridworkflows \
        -l "batch_id=${BATCH_ID}" \
        -o json 2>/dev/null || echo '{"items":[]}')

    TOTAL=$(echo "${WF_STATUS}" | "${PYTHON_CMD}" -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('items',[])))" 2>/dev/null || echo 0)
    COMPLETED=$(echo "${WF_STATUS}" | "${PYTHON_CMD}" -c "import json,sys; d=json.load(sys.stdin); items=d.get('items',[]); print(sum(1 for i in items if i.get('status',{}).get('phase','')=='Completed'))" 2>/dev/null || echo 0)
    FAILED=$(echo "${WF_STATUS}" | "${PYTHON_CMD}" -c "import json,sys; d=json.load(sys.stdin); items=d.get('items',[]); print(sum(1 for i in items if i.get('status',{}).get('phase','')=='Failed'))" 2>/dev/null || echo 0)
    RUNNING=$((TOTAL - COMPLETED - FAILED))

    ELAPSED=$(awk "BEGIN {printf \"%.0f\", $(date +%s) - ${START_TIME%.*}}")

    # Format elapsed time as hh:mm:ss for readability
    ELAPSED_H=$((ELAPSED / 3600))
    ELAPSED_M=$(((ELAPSED % 3600) / 60))
    ELAPSED_S=$((ELAPSED % 60))
    ELAPSED_FMT=$(printf "%dh%02dm%02ds" "${ELAPSED_H}" "${ELAPSED_M}" "${ELAPSED_S}")

    info "  [${ELAPSED_FMT}] total=${TOTAL}  completed=${COMPLETED}  failed=${FAILED}  running/pending=${RUNNING}"

    # Terminal condition: all submitted workflows visible AND all in terminal state
    if [[ "${TOTAL}" -ge "${SUBMITTED}" && "${RUNNING}" -eq 0 ]]; then
        echo ""
        info "All ${TOTAL} workflows have reached terminal state (${COMPLETED} completed, ${FAILED} failed)"
        break
    fi

    # Informational: some workflows not yet visible in cluster
    if [[ "${TOTAL}" -lt "${SUBMITTED}" ]]; then
        MISSING=$((SUBMITTED - TOTAL))
        if [[ "${MISSING}" -gt 0 ]]; then
            info "  ${MISSING} workflow(s) not yet visible in cluster (may still be provisioning)"
        fi
    fi

    sleep "${POLL_INTERVAL}"
done

# ---------------------------------------------------------------------------
# Step 5: Export metrics
# ---------------------------------------------------------------------------
step "Step 5/5: Exporting scheduling metrics ..."

mkdir -p "${METRICS_DIR}"

# --- 5a. Port-forward to operator metrics server ---
info "Setting up metrics server port-forward ..."

# Kill any existing port-forward on our port
pkill -f "port-forward.*${METRICS_PORT}" 2>/dev/null || true
sleep 1

# Start port-forward in background
"${KUBECTL}" port-forward -n default deploy/qonductor-operator ${METRICS_PORT}:${METRICS_PORT} &>/dev/null &
PF_PID=$!
sleep 3

# Verify port-forward is alive
METRICS_URL="http://localhost:${METRICS_PORT}"
if ! kill -0 "${PF_PID}" 2>/dev/null; then
    warn "Port-forward failed to start — trying once more ..."
    sleep 2
    "${KUBECTL}" port-forward -n default deploy/qonductor-operator ${METRICS_PORT}:${METRICS_PORT} &>/dev/null &
    PF_PID=$!
    sleep 3
fi

if ! kill -0 "${PF_PID}" 2>/dev/null; then
    error "Cannot establish port-forward to operator — metrics export will use kubectl fallback"
    METRICS_AVAILABLE=0
else
    # Quick health check
    if curl -sf "${METRICS_URL}/healthz" &>/dev/null; then
        info "Metrics server reachable at ${METRICS_URL}"
        METRICS_AVAILABLE=1
    else
        warn "Metrics server not responding on ${METRICS_URL} — will retry individual calls"
        METRICS_AVAILABLE=1
    fi
fi

# --- 5b. Export via metrics server ---
if [[ "${METRICS_AVAILABLE}" -eq 1 ]]; then
    # Workflow aggregate metrics (JSON)
    info "Exporting workflow-level metrics ..."
    if "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --type workflows \
        --json \
        -o "${METRICS_DIR}/workflow_metrics.json" 2>&1; then
        info "  Workflow metrics → ${METRICS_DIR}/workflow_metrics.json"
    else
        warn "  Workflow metrics export failed — falling back to kubectl"
        "${KUBECTL}" get hybridworkflows \
            -l "batch_id=${BATCH_ID}" \
            -o json > "${METRICS_DIR}/workflow_metrics_raw.json" 2>/dev/null || true
        info "  Raw workflow CRs → ${METRICS_DIR}/workflow_metrics_raw.json"
    fi

    # Quantum job metrics (JSON)
    info "Exporting quantum-job metrics ..."
    if "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --type quantum-jobs \
        --json \
        -o "${METRICS_DIR}/quantum_job_metrics.json" 2>&1; then
        info "  Quantum-job metrics → ${METRICS_DIR}/quantum_job_metrics.json"
    else
        warn "  Quantum-job metrics export failed — falling back to kubectl"
        "${KUBECTL}" get quantumjobs \
            -o json > "${METRICS_DIR}/quantum_job_metrics_raw.json" 2>/dev/null || true
        info "  Raw QuantumJob CRs → ${METRICS_DIR}/quantum_job_metrics_raw.json"
    fi

    # Full CSV for easy analysis
    info "Exporting quantum-job CSV (full) ..."
    "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --format full \
        -o "${METRICS_DIR}/quantum_job_metrics.csv" 2>/dev/null || true

    # E2E format CSV (timestamp, fidelity, JCT)
    info "Exporting quantum-job CSV (e2e) ..."
    "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --format e2e \
        -o "${METRICS_DIR}/quantum_job_jct_fidelity.csv" 2>/dev/null || true
else
    # Fallback: dump raw CRs via kubectl
    warn "Metrics server unavailable — dumping raw CRs via kubectl"
    "${KUBECTL}" get hybridworkflows \
        -l "batch_id=${BATCH_ID}" \
        -o json > "${METRICS_DIR}/workflow_metrics_raw.json" 2>/dev/null || true
    "${KUBECTL}" get quantumjobs \
        -o json > "${METRICS_DIR}/quantum_job_metrics_raw.json" 2>/dev/null || true
fi

# --- 5c. Raw CR snapshots (always taken) ---
info "Exporting raw CR snapshots ..."
"${KUBECTL}" get hybridworkflows \
    -l "batch_id=${BATCH_ID}" \
    -o json > "${METRICS_DIR}/workflow_crs_raw.json" 2>/dev/null || true
"${KUBECTL}" get quantumjobs \
    -o json > "${METRICS_DIR}/quantum_job_crs_raw.json" 2>/dev/null || true
info "  Raw HybridWorkflow CRs → ${METRICS_DIR}/workflow_crs_raw.json"
info "  Raw QuantumJob CRs → ${METRICS_DIR}/quantum_job_crs_raw.json"

# --- 5d. Clean up port-forward ---
if [[ -n "${PF_PID:-}" ]]; then
    if kill -0 "${PF_PID}" 2>/dev/null; then
        kill "${PF_PID}" 2>/dev/null || true
    fi
    wait "${PF_PID}" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
SUMMARY_FILE="${BATCH_OUT_DIR}/summary.txt"
{
    echo "=============================================================================="
    echo "  Qonductor Batch Submission Summary"
    echo "=============================================================================="
    echo "  Batch ID:       ${BATCH_ID}"
    echo "  Started:        $(date -d "@${START_TIME%.*}" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date '+%Y-%m-%d %H:%M:%S')"
    echo "  Submit window:  ${SUBMIT_WINDOW_SEC}s"
    echo "  Arrival mode:   ${ARRIVAL_MODE}"
    echo "  Total planned:  ${ACTUAL_JOBS}"
    echo "  Submitted:      ${SUBMITTED}"
    echo "  Failed submit:  ${FAILED_SUBMIT}"
    echo "  Hybrid ratio:   ${HYBRID_JOB_RATIO}"
    echo "  Seed:           ${SEED}"
    echo ""
    echo "  Output directory: ${BATCH_OUT_DIR}"
    echo "  Submission log:   ${SUBMISSION_LOG}"
    echo "  Metrics:          ${METRICS_DIR}/"
    echo ""
    echo "  Files:"
    for f in "${BATCH_OUT_DIR}"/* "${METRICS_DIR}"/*; do
        [[ -f "$f" ]] && echo "    $(basename "$f")"
    done
    echo ""
    echo "  To reproduce this run:"
    echo "    $(basename "$0") \\"
    echo "        --workload-dir ${WORKLOAD_DIR} \\"
    echo "        --total-jobs ${ACTUAL_JOBS} \\"
    echo "        --submit-window-sec ${SUBMIT_WINDOW_SEC} \\"
    echo "        --arrival-mode ${ARRIVAL_MODE} \\"
    echo "        --seed ${SEED} \\"
    echo "        --output-dir ${OUTPUT_DIR}"
    echo "=============================================================================="
} > "${SUMMARY_FILE}"

cat "${SUMMARY_FILE}"

# Clean up completed workflows (optional — commented out by default)
# "${KUBECTL}" delete hybridworkflows -l "batch_id=${BATCH_ID}" 2>/dev/null || true

echo ""
info "Batch submission complete."
info "Results: ${BATCH_OUT_DIR}"
info "Summary: ${SUMMARY_FILE}"
