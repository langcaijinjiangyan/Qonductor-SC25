#!/usr/bin/env bash
# ============================================================================
# Qonductor Multi-Host Image Sync
# ============================================================================
# Builds Qonductor Docker images and imports them into the k3s embedded
# containerd on each node.  k3s does not read from the Docker image store,
# so we must use `ctr images import` inside each k3s container.
#
# This script also sets up a local Docker registry (if not already running)
# on the server host for cross-host image distribution.
#
# Usage:
#   bash docker/multi-host/sync-images.sh
#
# Environment variables:
#   REGISTRY_PORT   — local registry port (default: 5000)
#   BUILD_ONLY      — set to 1 to skip registry push and containerd import
#   SKIP_BUILD      — set to 1 to skip Docker image builds
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLUSTER_CONFIG="${SCRIPT_DIR}/cluster-config.yaml"
REGISTRY_PORT="${REGISTRY_PORT:-5000}"
BUILD_ONLY="${BUILD_ONLY:-0}"
SKIP_BUILD="${SKIP_BUILD:-0}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[sync]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
die()  { echo -e "${RED}[fatal]${NC} $*" >&2; exit 1; }

# Image names (must match the DaemonSet/Deployment specs).
OPERATOR_IMAGE="${OPERATOR_IMAGE:-qonductor-operator:latest}"
DEVICE_PLUGIN_IMAGE="${DEVICE_PLUGIN_IMAGE:-qonductor-device-plugin:latest}"
QUANTUM_EXECUTOR_IMAGE="${QUANTUM_EXECUTOR_IMAGE:-qonductor-quantum-executor:latest}"

# Qonductor images (built from source).
QONDUCTOR_IMAGES=("$OPERATOR_IMAGE" "$DEVICE_PLUGIN_IMAGE" "$QUANTUM_EXECUTOR_IMAGE")

# k3s infrastructure images required for pod sandbox, DNS, ingress, etc.
# These must be present on every node's containerd for pods to start.
K3S_INFRA_IMAGES=(
    "rancher/mirrored-pause:3.6"
    "rancher/mirrored-coredns-coredns:1.12.0"
    "rancher/mirrored-library-traefik:2.11.10"
    "rancher/local-path-provisioner:v0.0.30"
    "rancher/mirrored-metrics-server:v0.7.2"
    "rancher/klipper-helm:v0.9.3-build20241008"
    "rancher/klipper-lb:v0.4.9"
)

# K3S_IMAGE and ALL_IMAGES are set in main() after cluster config is loaded.
ALL_IMAGES=("${QONDUCTOR_IMAGES[@]}")

# ---------------------------------------------------------------------------
# YAML config loader (stdlib-only Python fallback, no PyYAML needed)
# ---------------------------------------------------------------------------
source "${SCRIPT_DIR}/lib/config-loader.sh"

# ---------------------------------------------------------------------------
# Config access helpers (require load_cluster_config to have been called)
# ---------------------------------------------------------------------------

get_server_host() { echo "${SERVER_HOST}"; }
get_agent_hosts() {
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        echo "${!host_var}"
    done
}

get_all_hosts() {
    get_server_host
    get_agent_hosts
}

get_all_containers() {
    echo "${SERVER_CONTAINERNAME}"
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local name_var="AGENTS_${i}_CONTAINERNAME"
        echo "${!name_var}"
    done
}

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build_images() {
    if [[ "$SKIP_BUILD" == "1" ]]; then
        warn "SKIP_BUILD=1 — skipping Docker image builds."
        return 0
    fi

    log "Building Qonductor Docker images …"
    mkdir -p "${PROJECT_ROOT}/data/workflow_registry"

    log "  Building ${OPERATOR_IMAGE}"
    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.operator" \
        -t "${OPERATOR_IMAGE}" "${PROJECT_ROOT}"

    log "  Building ${QUANTUM_EXECUTOR_IMAGE}"
    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.quantum-executor" \
        -t "${QUANTUM_EXECUTOR_IMAGE}" "${PROJECT_ROOT}"

    log "  Building ${DEVICE_PLUGIN_IMAGE}"
    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.device-plugin" \
        -t "${DEVICE_PLUGIN_IMAGE}" "${PROJECT_ROOT}"

    log "  ✓ All Qonductor images built locally"

    # Ensure k3s base image is available locally for distribution.
    if docker image inspect "$K3S_IMAGE" >/dev/null 2>&1; then
        log "  k3s image ${K3S_IMAGE} already present locally"
    else
        log "  Pulling k3s image ${K3S_IMAGE} …"
        docker pull "$K3S_IMAGE" || warn "Failed to pull ${K3S_IMAGE} from registry — will try to sync from server"
    fi

    # Check k3s infrastructure images.
    local missing_infra=()
    for img in "${K3S_INFRA_IMAGES[@]}"; do
        if ! docker image inspect "$img" >/dev/null 2>&1; then
            missing_infra+=("$img")
        fi
    done
    if [[ ${#missing_infra[@]} -gt 0 ]]; then
        warn "${#missing_infra[@]} k3s infrastructure image(s) missing locally."
        warn "  Run 'docker pull' for each or load from images.tar."
        warn "  Missing: ${missing_infra[*]}"
    else
        log "  ✓ All k3s infrastructure images present locally"
    fi
}

# ---------------------------------------------------------------------------
# Distribute images to remote hosts via docker save/load over SSH.
# Avoids the need for a Docker registry with TLS configuration.
# ---------------------------------------------------------------------------

distribute_to_host() {
    local host="$1"

    if _is_localhost "$host"; then
        log "  Skipping distribution to localhost ${host} (images already present)"
        return 0
    fi

    log "Distributing images to ${host} via docker save/load …"

    # Build a tar archive of all images and pipe it through SSH.
    # This is more reliable than a local HTTP registry (no TLS config needed).
    for img in "${ALL_IMAGES[@]}"; do
        if docker image inspect "$img" >/dev/null 2>&1; then
            log "  Sending ${img} → ${host}"
            docker save "$img" 2>/dev/null | ssh "$host" "docker load 2>/dev/null" && \
                log "    ✓ loaded" || \
                warn "    ✗ Failed to transfer ${img} to ${host}"
        else
            warn "  Image ${img} not available locally — skipping"
        fi
    done
    log "  ✓ Distribution to ${host} complete"
}

_is_localhost() {
    local host="$1"
    local my_ip
    my_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
    [[ "$host" == "127.0.0.1" || "$host" == "localhost" || "$host" == "$my_ip" ]]
}

# ---------------------------------------------------------------------------
# Import images into k3s embedded containerd
# ---------------------------------------------------------------------------

import_into_containerd() {
    local host="$1"
    local container_name="$2"

    log "Importing images into k3s containerd (${container_name} on ${host}) …"

    for img in "${ALL_IMAGES[@]}"; do
        # Export from Docker, pipe into k3s containerd.
        ssh "$host" "docker save ${img} 2>/dev/null | docker exec -i ${container_name} ctr images import - 2>/dev/null" && \
            log "  ✓ ${img} → ${container_name}" || \
            warn "  ✗ Failed to import ${img} into ${container_name}"
    done
}

# ---------------------------------------------------------------------------
# SSH wrapper that handles "localhost" (current machine) without SSH
# ---------------------------------------------------------------------------

_remote() {
    local host="$1"; shift
    local current_ip
    current_ip="$(hostname -I 2>/dev/null | awk '{print $1}')"

    if [[ "$host" == "127.0.0.1" || "$host" == "localhost" || "$host" == "$current_ip" ]]; then
        # Run locally.
        eval "$@"
    else
        ssh "$host" "$@"
    fi
}

_remote_container_exists() {
    local host="$1" container="$2"
    _remote "$host" "docker ps -a --format '{{.Names}}' | grep -qx '${container}'"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Qonductor Multi-Host Image Sync${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo ""

    [[ -f "$CLUSTER_CONFIG" ]] || die "cluster-config.yaml not found at $CLUSTER_CONFIG"
    load_cluster_config "$CLUSTER_CONFIG"

    # k3s base image (must be available on every node for k3s containers).
    K3S_IMAGE="${K3S_IMAGE:-rancher/k3s:${CLUSTER_KUBERNETESVERSION}}"
    ALL_IMAGES=("${QONDUCTOR_IMAGES[@]}" "$K3S_IMAGE" "${K3S_INFRA_IMAGES[@]}")

    # ---- 1. Build Qonductor images, pull k3s image locally
    build_images

    if [[ "$BUILD_ONLY" == "1" ]]; then
        log "BUILD_ONLY=1 — stopping after build."
        return 0
    fi

    # ---- 2. Distribute images to all remote hosts via SSH pipe
    local server_host="${SERVER_HOST}"
    for host in $(get_all_hosts | sort -u); do
        distribute_to_host "$host"
    done

    # ---- 5. Import into k3s containerd
    local server_container="${SERVER_CONTAINERNAME}"

    if _remote_container_exists "$server_host" "$server_container"; then
        import_into_containerd "$server_host" "$server_container"
    else
        warn "Server container '${server_container}' not running on ${server_host} — skip containerd import."
    fi

    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        local agent_host="${!host_var}"
        local name_var="AGENTS_${i}_CONTAINERNAME"
        local agent_container="${!name_var}"
        if _remote_container_exists "$agent_host" "$agent_container"; then
            import_into_containerd "$agent_host" "$agent_container"
        else
            warn "Agent container '${agent_container}' not running on ${agent_host} — skip containerd import."
        fi
    done

    echo ""
    log "Image sync complete."
    echo ""
}

main "$@"
