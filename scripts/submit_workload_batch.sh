#!/usr/bin/env bash
# =============================================================================
# submit_workload_batch.sh
#
# Batch workload submission script for the Qonductor cluster.
#
# Submits a configurable number of hybrid quantum-classical workflows at
# uniform intervals over a specified time window.  Workloads are randomly
# sampled from a workload library directory.  After all workflows complete,
# scheduling metrics are automatically exported.
#
# Usage:
#   ./scripts/submit_workload_batch.sh \
#       --duration 600 \
#       --count 30 \
#       --workload-dir workload_python \
#       [--max-iterations 20] \
#       [--shots 1024] \
#       [--priority balanced] \
#       [--output-dir data/batch_results] \
#       [--seed 42] \
#       [--timeout 3600]
#
# Requirements:
#   - kubectl with access to the Qonductor cluster
#   - Python 3.11+ with project dependencies installed
#   - scripts/generate_batch_manifests.py (Python manifest generator)
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
WORKLOAD_DIR="${PROJECT_ROOT}/workload_python"
OUTPUT_DIR="${PROJECT_ROOT}/data/batch_results"
SHOTS=1024
MAX_ITERATIONS=20
PRIORITY="balanced"
SEED=""
DURATION=""
TASK_COUNT=""
TIMEOUT=3600               # max wait time for all workflows (seconds)
METRICS_PORT=9100
KUBECTL="${PROJECT_ROOT}/kubectl"

# Use system kubectl if the bundled one isn't available
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
# Argument parsing
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Required:
  --duration SECONDS     Time window over which tasks are submitted
  --count N              Total number of workflow tasks to submit

Optional:
  --workload-dir DIR     Workload library directory (default: workload_python)
  --output-dir DIR       Output directory for results (default: data/batch_results)
  --shots N              Base shot count per task, randomised ±50% (default: 1024)
  --max-iterations N     Upper bound for randomised SPSA iterations (default: 20)
  --priority PRIORITY    Scheduling priority: balanced|fidelity|jct (default: balanced)
  --seed N               Random seed for reproducible task selection
  --timeout SECONDS      Max wait time for all workflows to finish (default: 3600)
  -h, --help             Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration)
            DURATION="$2"; shift 2 ;;
        --count)
            TASK_COUNT="$2"; shift 2 ;;
        --workload-dir)
            WORKLOAD_DIR="$2"; shift 2 ;;
        --output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        --shots)
            SHOTS="$2"; shift 2 ;;
        --max-iterations)
            MAX_ITERATIONS="$2"; shift 2 ;;
        --priority)
            PRIORITY="$2"; shift 2 ;;
        --seed)
            SEED="$2"; shift 2 ;;
        --timeout)
            TIMEOUT="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            error "Unknown option: $1"
            usage; exit 1 ;;
    esac
done

# Validate required arguments
if [[ -z "${DURATION}" || -z "${TASK_COUNT}" ]]; then
    error "--duration and --count are required"
    echo ""
    usage
    exit 1
fi

if [[ "${TASK_COUNT}" -lt 1 ]]; then
    error "--count must be >= 1"
    exit 1
fi
if [[ "${DURATION}" -lt 1 ]]; then
    error "--duration must be >= 1"
    exit 1
fi

# Resolve workload dir relative to project root
if [[ ! "${WORKLOAD_DIR}" = /* ]]; then
    WORKLOAD_DIR="${PROJECT_ROOT}/${WORKLOAD_DIR}"
fi

# ---------------------------------------------------------------------------
# Derived constants
# ---------------------------------------------------------------------------
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
BATCH_ID="batch_${TIMESTAMP}"
BATCH_OUT_DIR="${OUTPUT_DIR}/${BATCH_ID}"
SUBMISSION_LOG="${BATCH_OUT_DIR}/submission_log.csv"
METRICS_DIR="${BATCH_OUT_DIR}/metrics"

# Calculate submission interval (floating-point arithmetic via bc or awk)
if [[ "${TASK_COUNT}" -eq 1 ]]; then
    INTERVAL=0
else
    INTERVAL=$(awk "BEGIN {printf \"%.3f\", ${DURATION} / (${TASK_COUNT} - 1)}")
fi

# ---------------------------------------------------------------------------
# Print configuration
# ---------------------------------------------------------------------------
echo ""
echo "=============================================================================="
echo "  Qonductor Batch Workload Submission"
echo "=============================================================================="
echo "  Batch ID:       ${BATCH_ID}"
echo "  Duration:       ${DURATION}s"
echo "  Task count:     ${TASK_COUNT}"
echo "  Interval:       ${INTERVAL}s"
echo "  Workload dir:   ${WORKLOAD_DIR}"
echo "  Max iterations: ${MAX_ITERATIONS}"
echo "  Base shots:     ${SHOTS}"
echo "  Priority:       ${PRIORITY}"
echo "  Timeout:        ${TIMEOUT}s"
echo "  Output dir:     ${BATCH_OUT_DIR}"
[[ -n "${SEED}" ]] && echo "  Seed:           ${SEED}"
echo "=============================================================================="
echo ""

# ---------------------------------------------------------------------------
# Step 1: Generate manifests via Python
# ---------------------------------------------------------------------------
step "Step 1/5: Generating workflow YAML manifests ..."

mkdir -p "${BATCH_OUT_DIR}"

PYTHON_CMD="python3"
GENERATOR="${PROJECT_ROOT}/scripts/generate_batch_manifests.py"

SEED_ARG=()
[[ -n "${SEED}" ]] && SEED_ARG=(--seed "${SEED}")

if ! "${PYTHON_CMD}" "${GENERATOR}" \
    --workload-dir "${WORKLOAD_DIR}" \
    --output-dir "${OUTPUT_DIR}" \
    --count "${TASK_COUNT}" \
    --batch-id "${BATCH_ID}" \
    --shots "${SHOTS}" \
    --max-iterations "${MAX_ITERATIONS}" \
    --priority "${PRIORITY}" \
    "${SEED_ARG[@]}" 2>&1 | tee "${BATCH_OUT_DIR}/generation.log"; then
    error "Manifest generation failed — see ${BATCH_OUT_DIR}/generation.log"
    exit 1
fi

# Extract the schedule path from the Python output
SCHEDULE_PATH=$(awk -F= '/^SCHEDULE_PATH=/{print $2}' "${BATCH_OUT_DIR}/generation.log")
MANIFEST_COUNT=$(awk -F= '/^MANIFEST_COUNT=/{print $2}' "${BATCH_OUT_DIR}/generation.log")

if [[ -z "${SCHEDULE_PATH}" || ! -f "${SCHEDULE_PATH}" ]]; then
    error "Schedule file not found after generation"
    exit 1
fi

info "Generated ${MANIFEST_COUNT:-?} manifests"
info "Schedule: ${SCHEDULE_PATH}"

# ---------------------------------------------------------------------------
# Step 2: Read schedule into memory
# ---------------------------------------------------------------------------
step "Step 2/5: Reading schedule and preparing submission queue ..."

# Parse CSV (skip header) into arrays
declare -a WF_ORDER WF_NAME WF_TYPE WF_BENCHMARK WF_YAML WF_ITERS WF_SHOTS WF_IMAGEREF

while IFS=, read -r order name type benchmark yaml iters shots imgref; do
    # Skip header
    [[ "${order}" == "order" ]] && continue
    WF_ORDER+=("${order}")
    WF_NAME+=("${name}")
    WF_TYPE+=("${type}")
    WF_BENCHMARK+=("${benchmark}")
    WF_YAML+=("${yaml}")
    WF_ITERS+=("${iters}")
    WF_SHOTS+=("${shots}")
    WF_IMAGEREF+=("${imgref}")
done < "${SCHEDULE_PATH}"

ACTUAL_COUNT=${#WF_NAME[@]}
if [[ "${ACTUAL_COUNT}" -eq 0 ]]; then
    error "Schedule is empty"
    exit 1
fi
info "Loaded ${ACTUAL_COUNT} tasks from schedule"

# ---------------------------------------------------------------------------
# Step 3: Timed submission
# ---------------------------------------------------------------------------
step "Step 3/5: Submitting workflows at uniform intervals ..."

# Initialise submission log
echo "actual_submit_time,scheduled_time,order,workflow_name,workload_type,benchmark,yaml_path,iterations,shots,image_ref" > "${SUBMISSION_LOG}"

# Calculate scheduled times (evenly spaced across the duration)
# Each task i gets scheduled at time: i * interval (relative to start)
# We use the start time + offset for logging purposes
START_TIME=$(date +%s.%N)

info "Submission window starts now (${START_TIME})"
info "Press Ctrl+C to abort (already-submitted workflows will keep running)"

# Trap Ctrl+C: stop submitting but don't kill already-submitted workflows
ABORT_SUBMISSION=0
trap 'warn "Aborting submission — already-submitted workflows will keep running"; ABORT_SUBMISSION=1' INT

SUBMITTED=0
FAILED_SUBMIT=0

for ((i = 0; i < ACTUAL_COUNT; i++)); do
    [[ "${ABORT_SUBMISSION}" -eq 1 ]] && break

    # Calculate target time for this task
    TARGET_TIME=$(awk "BEGIN {printf \"%.3f\", ${START_TIME} + ${i} * ${INTERVAL}}")
    NOW=$(date +%s.%N)

    # Sleep until target time (if it's in the future)
    SLEEP_DURATION=$(awk "BEGIN {d = ${TARGET_TIME} - ${NOW}; if (d > 0) printf \"%.3f\", d; else print 0}")
    if [[ "$(echo "${SLEEP_DURATION} > 0" | bc -l 2>/dev/null || echo 0)" == "1" ]]; then
        sleep "${SLEEP_DURATION}"
    fi

    ACTUAL_SUBMIT_TIME=$(date -u +"%Y-%m-%dT%H:%M:%S.%3NZ")
    YAML_PATH="${WF_YAML[$i]}"

    if [[ ! -f "${YAML_PATH}" ]]; then
        error "  [${i}/$((${ACTUAL_COUNT} - 1))] MISSING YAML: ${YAML_PATH}"
        FAILED_SUBMIT=$((FAILED_SUBMIT + 1))
        echo "${ACTUAL_SUBMIT_TIME},${TARGET_TIME},${WF_ORDER[$i]},${WF_NAME[$i]},${WF_TYPE[$i]},${WF_BENCHMARK[$i]},${YAML_PATH},${WF_ITERS[$i]},${WF_SHOTS[$i]},${WF_IMAGEREF[$i]},FAILED_YAML_MISSING" >> "${SUBMISSION_LOG}"
        continue
    fi

    # Submit via kubectl
    if KUBECTL_OUT=$("${KUBECTL}" apply -f "${YAML_PATH}" 2>&1); then
        echo "${ACTUAL_SUBMIT_TIME},${TARGET_TIME},${WF_ORDER[$i]},${WF_NAME[$i]},${WF_TYPE[$i]},${WF_BENCHMARK[$i]},${YAML_PATH},${WF_ITERS[$i]},${WF_SHOTS[$i]},${WF_IMAGEREF[$i]},SUCCESS" >> "${SUBMISSION_LOG}"
        info "  [${i}/$((${ACTUAL_COUNT} - 1))] ✓ ${WF_NAME[$i]}  (${WF_BENCHMARK[$i]}, iter=${WF_ITERS[$i]}, shots=${WF_SHOTS[$i]})"
        SUBMITTED=$((SUBMITTED + 1))
    else
        error "  [${i}/$((${ACTUAL_COUNT} - 1))] ✗ ${WF_NAME[$i]}  — kubectl apply failed"
        FAILED_SUBMIT=$((FAILED_SUBMIT + 1))
        echo "${ACTUAL_SUBMIT_TIME},${TARGET_TIME},${WF_ORDER[$i]},${WF_NAME[$i]},${WF_TYPE[$i]},${WF_BENCHMARK[$i]},${YAML_PATH},${WF_ITERS[$i]},${WF_SHOTS[$i]},${WF_IMAGEREF[$i]},FAILED_KUBECTL" >> "${SUBMISSION_LOG}"
    fi
done

END_SUBMIT_TIME=$(date +%s.%N)
SUBMIT_DURATION=$(awk "BEGIN {printf \"%.1f\", ${END_SUBMIT_TIME} - ${START_TIME}}")

echo ""
info "Submission phase complete: ${SUBMITTED} submitted, ${FAILED_SUBMIT} failed (${SUBMIT_DURATION}s elapsed)"
info "Submission log: ${SUBMISSION_LOG}"

# Remove the trap
trap - INT

if [[ "${SUBMITTED}" -eq 0 ]]; then
    error "No workflows were submitted successfully — aborting"
    exit 1
fi

# ---------------------------------------------------------------------------
# Step 4: Wait for all workflows to complete
# ---------------------------------------------------------------------------
step "Step 4/5: Waiting for all workflows to complete ..."

# Build comma-separated list of workflow names for label-based queries
# We use the batch_id label for efficient querying
POLL_DEADLINE=$(awk "BEGIN {printf \"%.0f\", $(date +%s) + ${TIMEOUT}}")
POLL_INTERVAL=10  # seconds between polls

info "Polling every ${POLL_INTERVAL}s, timeout at $(date -d "@${POLL_DEADLINE}" '+%H:%M:%S' 2>/dev/null || echo "${POLL_DEADLINE}")"

while true; do
    NOW_EPOCH=$(date +%s)
    if [[ "${NOW_EPOCH}" -ge "${POLL_DEADLINE}" ]]; then
        warn "Timeout reached (${TIMEOUT}s) — proceeding to metrics export with available data"
        break
    fi

    # Query all workflows in this batch
    WF_STATUS=$("${KUBECTL}" get hybridworkflows \
        -l "batch_id=${BATCH_ID}" \
        -o json 2>/dev/null || echo '{"items":[]}')

    TOTAL=$(echo "${WF_STATUS}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d.get('items',[])))" 2>/dev/null || echo 0)
    COMPLETED=$(echo "${WF_STATUS}" | python3 -c "import json,sys; d=json.load(sys.stdin); items=d.get('items',[]); print(sum(1 for i in items if i.get('status',{}).get('phase','')=='Completed'))" 2>/dev/null || echo 0)
    FAILED=$(echo "${WF_STATUS}" | python3 -c "import json,sys; d=json.load(sys.stdin); items=d.get('items',[]); print(sum(1 for i in items if i.get('status',{}).get('phase','')=='Failed'))" 2>/dev/null || echo 0)
    RUNNING=$((TOTAL - COMPLETED - FAILED))

    ELAPSED=$(awk "BEGIN {printf \"%.0f\", $(date +%s) - ${START_TIME}}")

    info "  [${ELAPSED}s] total=${TOTAL}  completed=${COMPLETED}  failed=${FAILED}  running/pending=${RUNNING}"

    if [[ "${TOTAL}" -gt 0 && "${RUNNING}" -eq 0 ]]; then
        echo ""
        info "All ${TOTAL} workflows have reached terminal state (${COMPLETED} completed, ${FAILED} failed)"
        break
    fi

    sleep "${POLL_INTERVAL}"
done

# ---------------------------------------------------------------------------
# Step 5: Export metrics
# ---------------------------------------------------------------------------
step "Step 5/5: Exporting scheduling metrics ..."

mkdir -p "${METRICS_DIR}"

# --- 5a. Ensure port-forward to the operator pod is active ---
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
    error "Cannot establish port-forward to operator pod — metrics export will be skipped"
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

# --- 5b. Export workflow-level metrics ---
EXPORT_SCRIPT="${PROJECT_ROOT}/scripts/export_metrics.py"

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

    # Also export full CSV for easy analysis
    info "Exporting quantum-job CSV ..."
    "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --format full \
        -o "${METRICS_DIR}/quantum_job_metrics.csv" 2>/dev/null || true

    # E2E format CSV (timestamp, fidelity, JCT)
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

# --- 5c. Clean up port-forward ---
if [[ -n "${PF_PID:-}" ]]; then
    kill "${PF_PID}" 2>/dev/null || true
fi

# --- 5d. Generate a human-readable summary ---
SUMMARY_FILE="${BATCH_OUT_DIR}/summary.txt"
{
    echo "=============================================================================="
    echo "  Qonductor Batch Submission Summary"
    echo "=============================================================================="
    echo "  Batch ID:       ${BATCH_ID}"
    echo "  Started:        $(date -d "@${START_TIME%.*}" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date '+%Y-%m-%d %H:%M:%S')"
    echo "  Duration:       ${DURATION}s (planned), ${SUBMIT_DURATION}s (actual submission)"
    echo "  Tasks:          ${SUBMITTED} submitted / ${FAILED_SUBMIT} failed / ${ACTUAL_COUNT} planned"
    echo "  Interval:       ${INTERVAL}s"
    echo "  Max iterations: ${MAX_ITERATIONS}"
    echo "  Base shots:     ${SHOTS}"
    echo "  Priority:       ${PRIORITY}"
    echo "  Seed:           ${SEED:-<none>}"
    echo ""
    echo "  Output directory: ${BATCH_OUT_DIR}"
    echo "  Submission log:   ${SUBMISSION_LOG}"
    echo "  Metrics:          ${METRICS_DIR}/"
    echo ""
    echo "  Files:"
    echo "    $(ls "${BATCH_OUT_DIR}/" 2>/dev/null | tr '\n' ' ')"
    echo ""
    echo "  To reproduce this run:"
    echo "    $(basename "$0") \\"
    echo "        --duration ${DURATION} \\"
    echo "        --count ${TASK_COUNT} \\"
    echo "        --workload-dir ${WORKLOAD_DIR} \\"
    echo "        --max-iterations ${MAX_ITERATIONS} \\"
    echo "        --shots ${SHOTS} \\"
    echo "        --priority ${PRIORITY} \\"
    [[ -n "${SEED}" ]] && echo "        --seed ${SEED} \\"
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
