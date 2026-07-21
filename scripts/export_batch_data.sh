#!/usr/bin/env bash
# =============================================================================
# export_batch_data.sh
#
# 从 submit_workload_batch.sh 的 Step 5 提取出的独立数据导出脚本。
# 用于在 submit_workload_batch.sh 超时中断后，单独导出已完成作业的指标数据。
#
# 用法:
#   ./scripts/export_batch_data.sh <BATCH_ID>
#
#   ./scripts/export_batch_data.sh batch_20260719_200828
#   ./scripts/export_batch_data.sh batch_20260719_200828 --output-dir /custom/output
#
# 导出文件:
#   <output-dir>/<BATCH_ID>/metrics/
#     workflow_metrics.json         # 工作流级别聚合指标 (JSON)
#     quantum_job_metrics.json      # QuantumJob 指标 (JSON)
#     quantum_job_metrics.csv       # QuantumJob 全量字段 (CSV)
#     quantum_job_jct_fidelity.csv  # E2E 格式: timestamp, fidelity, JCT
#     workflow_crs_raw.json         # 原始 HybridWorkflow CR (kubectl 兜底)
#     quantum_job_crs_raw.json      # 原始 QuantumJob CR (kubectl 兜底)
#   <output-dir>/<BATCH_ID>/summary.txt  # 可读摘要
#
# 依赖:
#   - kubectl (集群访问)
#   - python3 + scripts/export_metrics.py
#   - curl (健康检查)
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# 颜色
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }
step()  { echo -e "${CYAN}[STEP]${NC}  $*"; }

# ---------------------------------------------------------------------------
# 默认值
# ---------------------------------------------------------------------------
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METRICS_PORT=9100
PYTHON_CMD="python3"
EXPORT_SCRIPT="${PROJECT_ROOT}/scripts/export_metrics.py"

# kubectl 路径
KUBECTL="${PROJECT_ROOT}/kubectl"
if [[ ! -x "${KUBECTL}" ]]; then
    if command -v kubectl &>/dev/null; then
        KUBECTL="$(command -v kubectl)"
    else
        error "kubectl not found at ${KUBECTL} or in PATH"
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: $(basename "$0") <BATCH_ID> [--output-dir DIR]

从已有批次导出调度指标数据。

Arguments:
  BATCH_ID          批次 ID (如 batch_20260719_200828)，对应 data/batch_results/ 下的目录名

Options:
  --output-dir DIR  批次结果的父目录 (默认: data/batch_results)
  -h, --help        显示帮助
EOF
}

if [[ $# -lt 1 ]]; then
    error "缺少 BATCH_ID 参数"
    echo ""
    usage
    exit 1
fi

BATCH_ID="$1"
shift

OUTPUT_DIR="${PROJECT_ROOT}/data/batch_results"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --output-dir)
            OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            error "Unknown option: $1"
            usage; exit 1 ;;
    esac
done

# 解析为绝对路径
if [[ ! "${OUTPUT_DIR}" = /* ]]; then
    OUTPUT_DIR="${PROJECT_ROOT}/${OUTPUT_DIR}"
fi

# ---------------------------------------------------------------------------
# 派生路径
# ---------------------------------------------------------------------------
BATCH_OUT_DIR="${OUTPUT_DIR}/${BATCH_ID}"
METRICS_DIR="${BATCH_OUT_DIR}/metrics"

if [[ ! -d "${BATCH_OUT_DIR}" ]]; then
    error "Batch output directory not found: ${BATCH_OUT_DIR}"
    error "Check that BATCH_ID is correct and the batch was submitted from this machine"
    exit 1
fi

echo ""
echo "=============================================================================="
echo "  Qonductor Batch Data Export"
echo "=============================================================================="
echo "  Batch ID:    ${BATCH_ID}"
echo "  Output dir:  ${BATCH_OUT_DIR}"
echo "  Metrics dir: ${METRICS_DIR}"
echo "  kubectl:     ${KUBECTL}"
echo "=============================================================================="
echo ""

mkdir -p "${METRICS_DIR}"

# ---------------------------------------------------------------------------
# Step 1: 建立端口转发
# ---------------------------------------------------------------------------
step "Step 1/4: Setting up metrics server port-forward ..."

pkill -f "port-forward.*${METRICS_PORT}" 2>/dev/null || true
sleep 1

"${KUBECTL}" port-forward -n default deploy/qonductor-operator ${METRICS_PORT}:${METRICS_PORT} &>/dev/null &
PF_PID=$!
sleep 3

METRICS_URL="http://localhost:${METRICS_PORT}"

if ! kill -0 "${PF_PID}" 2>/dev/null; then
    warn "Port-forward failed to start — retrying ..."
    sleep 2
    "${KUBECTL}" port-forward -n default deploy/qonductor-operator ${METRICS_PORT}:${METRICS_PORT} &>/dev/null &
    PF_PID=$!
    sleep 3
fi

if ! kill -0 "${PF_PID}" 2>/dev/null; then
    error "Cannot establish port-forward to operator pod"
    METRICS_AVAILABLE=0
else
    if curl -sf "${METRICS_URL}/healthz" &>/dev/null; then
        info "Metrics server reachable at ${METRICS_URL}"
        METRICS_AVAILABLE=1
    else
        warn "Metrics server not responding — will retry individual calls"
        METRICS_AVAILABLE=1
    fi
fi

# ---------------------------------------------------------------------------
# Step 2: 通过 metrics server 导出
# ---------------------------------------------------------------------------
step "Step 2/4: Exporting via metrics server ..."

if [[ "${METRICS_AVAILABLE}" -eq 1 ]]; then

    # 2a. 工作流聚合指标
    info "Exporting workflow-level metrics (JSON) ..."
    if "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --type workflows \
        --json \
        -o "${METRICS_DIR}/workflow_metrics.json" 2>&1; then
        info "  → ${METRICS_DIR}/workflow_metrics.json"
    else
        warn "  Failed — falling back to kubectl"
        "${KUBECTL}" get hybridworkflows \
            -l "batch_id=${BATCH_ID}" \
            -o json > "${METRICS_DIR}/workflow_metrics_raw.json" 2>/dev/null || true
        info "  → ${METRICS_DIR}/workflow_metrics_raw.json (raw kubectl fallback)"
    fi

    # 2b. QuantumJob 指标 (JSON)
    info "Exporting quantum-job metrics (JSON) ..."
    if "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --type quantum-jobs \
        --json \
        -o "${METRICS_DIR}/quantum_job_metrics.json" 2>&1; then
        info "  → ${METRICS_DIR}/quantum_job_metrics.json"
    else
        warn "  Failed — falling back to kubectl"
        "${KUBECTL}" get quantumjobs \
            -o json > "${METRICS_DIR}/quantum_job_metrics_raw.json" 2>/dev/null || true
        info "  → ${METRICS_DIR}/quantum_job_metrics_raw.json (raw kubectl fallback)"
    fi

    # 2c. QuantumJob 全量 CSV
    info "Exporting quantum-job metrics (full CSV) ..."
    "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --format full \
        -o "${METRICS_DIR}/quantum_job_metrics.csv" 2>/dev/null || \
        warn "  Full CSV export failed (non-fatal)"

    # 2d. E2E 格式 CSV (timestamp, fidelity, JCT)
    info "Exporting quantum-job metrics (e2e CSV) ..."
    "${PYTHON_CMD}" "${EXPORT_SCRIPT}" \
        --url "${METRICS_URL}" \
        --format e2e \
        -o "${METRICS_DIR}/quantum_job_jct_fidelity.csv" 2>/dev/null || \
        warn "  E2E CSV export failed (non-fatal)"

else
    # metrics server 完全不可用时的兜底
    warn "Metrics server unavailable — dumping raw CRs via kubectl only"
    "${KUBECTL}" get hybridworkflows \
        -l "batch_id=${BATCH_ID}" \
        -o json > "${METRICS_DIR}/workflow_metrics_raw.json" 2>/dev/null || true
    "${KUBECTL}" get quantumjobs \
        -o json > "${METRICS_DIR}/quantum_job_metrics_raw.json" 2>/dev/null || true
fi

# ---------------------------------------------------------------------------
# Step 3: 原始 CR 快照 (始终执行，保证至少有数据)
# ---------------------------------------------------------------------------
step "Step 3/4: Exporting raw CR snapshots (always-on fallback) ..."

"${KUBECTL}" get hybridworkflows \
    -l "batch_id=${BATCH_ID}" \
    -o json > "${METRICS_DIR}/workflow_crs_raw.json" 2>/dev/null || true
info "  → ${METRICS_DIR}/workflow_crs_raw.json"

"${KUBECTL}" get quantumjobs \
    -o json > "${METRICS_DIR}/quantum_job_crs_raw.json" 2>/dev/null || true
info "  → ${METRICS_DIR}/quantum_job_crs_raw.json"

# ---------------------------------------------------------------------------
# Step 4: 清理
# ---------------------------------------------------------------------------
step "Step 4/4: Cleaning up port-forward ..."

if [[ -n "${PF_PID:-}" ]]; then
    if kill -0 "${PF_PID}" 2>/dev/null; then
        kill "${PF_PID}" 2>/dev/null || true
    fi
    wait "${PF_PID}" 2>/dev/null || true
fi
info "Port-forward cleaned up"

# ---------------------------------------------------------------------------
# 生成摘要
# ---------------------------------------------------------------------------
SUMMARY_FILE="${BATCH_OUT_DIR}/summary.txt"
{
    echo "=============================================================================="
    echo "  Qonductor Batch Data Export Summary"
    echo "=============================================================================="
    echo "  Batch ID:     ${BATCH_ID}"
    echo "  Exported at:  $(date '+%Y-%m-%d %H:%M:%S')"
    echo "  Metrics dir:  ${METRICS_DIR}"
    echo ""
    echo "  Files:"
    for f in "${METRICS_DIR}"/*; do
        if [[ -f "${f}" ]]; then
            size=$(du -h "${f}" 2>/dev/null | cut -f1)
            echo "    $(basename "${f}")  (${size})"
        fi
    done
    echo ""
    echo "  Submission log (if available):"
    if [[ -f "${BATCH_OUT_DIR}/submission_log.csv" ]]; then
        submitted=$(tail -n +2 "${BATCH_OUT_DIR}/submission_log.csv" 2>/dev/null | wc -l)
        failed=$(grep -c 'FAILED' "${BATCH_OUT_DIR}/submission_log.csv" 2>/dev/null || echo 0)
        echo "    ${submitted} submitted, ${failed} failed"
    fi
    echo "=============================================================================="
} > "${SUMMARY_FILE}"

cat "${SUMMARY_FILE}"

echo ""
info "Export complete."
info "Metrics: ${METRICS_DIR}"
info "Summary: ${SUMMARY_FILE}"
