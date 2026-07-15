#!/usr/bin/env bash
# ============================================================================
# Qonductor Multi-Host k3s-in-Docker Cluster Teardown
# ============================================================================
# Reads docker/multi-host/cluster-config.yaml and tears down the k3s cluster
# by stopping and removing all k3s Docker containers on all hosts.
#
# Usage:
#   bash docker/multi-host/teardown-cluster.sh
#
# Environment variables:
#   CLEANUP_VOLUMES=0    skip removing Docker data volumes (default: 1)
#   CLEANUP_IMAGES=0     skip removing Qonductor & k3s Docker images (default: 1)
#   CLEANUP_REGISTRY=0   skip removing the local Docker registry container (default: 1)
#   CLEANUP_RANCHER=0    skip removing /etc/rancher (default: 1, cleans stale passwords)
#   CLEANUP_QONDUCTOR=0  skip removing /etc/qonductor (default: 1)
#   DRY_RUN=1            print commands without executing
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLUSTER_CONFIG="${SCRIPT_DIR}/cluster-config.yaml"

CLEANUP_VOLUMES="${CLEANUP_VOLUMES:-1}"
CLEANUP_REGISTRY="${CLEANUP_REGISTRY:-1}"
CLEANUP_IMAGES="${CLEANUP_IMAGES:-1}"
CLEANUP_RANCHER="${CLEANUP_RANCHER:-1}"
CLEANUP_QONDUCTOR="${CLEANUP_QONDUCTOR:-1}"
DRY_RUN="${DRY_RUN:-0}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[teardown]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}     $*"; }
die()  { echo -e "${RED}[fatal]${NC} $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# YAML config loader (stdlib-only Python fallback, no PyYAML needed)
# ---------------------------------------------------------------------------
source "${SCRIPT_DIR}/lib/config-loader.sh"

# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------

_current_ip() {
    hostname -I 2>/dev/null | awk '{print $1}'
}

_is_local() {
    local host="$1"
    local my_ip
    my_ip="$(_current_ip)"
    [[ "$host" == "127.0.0.1" || "$host" == "localhost" || "$host" == "$my_ip" ]]
}

_remote() {
    local host="$1"; shift
    if _is_local "$host"; then
        eval "$@"
    else
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new "$host" "$@"
    fi
}

# ---------------------------------------------------------------------------
# Stop and remove containers
# ---------------------------------------------------------------------------

stop_container() {
    local host="$1" container_name="$2"

    if ! _remote "$host" "docker ps -a --format '{{.Names}}' | grep -qx '${container_name}'" 2>/dev/null; then
        warn "Container '${container_name}' not found on ${host} — skipping."
        return 0
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] Stop + remove ${container_name} on ${host}"
        return 0
    fi

    log "Stopping ${container_name} on ${host} …"
    _remote "$host" "docker stop ${container_name} 2>/dev/null || true"
    _remote "$host" "docker rm -f ${container_name} 2>/dev/null || true"
    log "  ✓ ${container_name} removed"
}

remove_volume() {
    local host="$1" volume_name="$2"

    if ! _remote "$host" "docker volume ls --format '{{.Name}}' | grep -qx '${volume_name}'" 2>/dev/null; then
        return 0
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] Remove volume ${volume_name} on ${host}"
        return 0
    fi

    warn "Removing Docker volume '${volume_name}' on ${host} …"
    _remote "$host" "docker volume rm ${volume_name}" || warn "  Could not remove volume ${volume_name}"
}

# ---------------------------------------------------------------------------
# Clean up k3s host-level state (/etc/rancher)
# ---------------------------------------------------------------------------
# k3s containers bind-mount /etc/rancher from the host filesystem.  The node
# password stored there survives container removal and causes "Node password
# rejected" errors on the next deploy if the server's node-passwd database
# was rebuilt.  Delete this directory on every host to guarantee a clean slate.
# ---------------------------------------------------------------------------

_rancher_dir_exists() {
    local host="$1"
    _remote "$host" "test -d /etc/rancher" 2>/dev/null
}

remove_rancher_state() {
    local host="$1"

    if ! _rancher_dir_exists "$host"; then
        log "  /etc/rancher on ${host}: not present"
        return 0
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] Remove /etc/rancher on ${host}"
        return 0
    fi

    warn "Removing /etc/rancher on ${host} (node passwords, k3s config) …"
    _remote "$host" "sudo rm -rf /etc/rancher" || warn "  Could not remove /etc/rancher on ${host}"
    log "  ✓ /etc/rancher removed on ${host}"
}

remove_qonductor_state() {
    local host="$1"

    if ! _remote "$host" "test -d /etc/qonductor" 2>/dev/null; then
        log "  /etc/qonductor on ${host}: not present"
        return 0
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "  [dry-run] Remove /etc/qonductor on ${host}"
        return 0
    fi

    warn "Removing /etc/qonductor on ${host} …"
    _remote "$host" "sudo rm -rf /etc/qonductor" || warn "  Could not remove /etc/qonductor on ${host}"
    log "  ✓ /etc/qonductor removed on ${host}"
}

# ---------------------------------------------------------------------------
# Clean up Docker images
# ---------------------------------------------------------------------------

QONDUCTOR_IMAGE_NAMES=(
    "qonductor-operator:latest"
    "qonductor-device-plugin:latest"
    "qonductor-quantum-executor:latest"
)

# k3s infrastructure images (mirrors deploy-cluster.sh _K3S_INFRA_IMAGES).
# These are pulled and distributed to every host during deployment.
K3S_INFRA_IMAGE_NAMES=(
    "rancher/mirrored-pause:3.6"
    "rancher/mirrored-coredns-coredns:1.12.0"
    "rancher/mirrored-library-traefik:2.11.10"
    "rancher/local-path-provisioner:v0.0.30"
    "rancher/mirrored-metrics-server:v0.7.2"
    "rancher/klipper-helm:v0.9.3-build20241008"
    "rancher/klipper-lb:v0.4.9"
)

# Images that should NEVER be removed during teardown (k3s core components).
# These are the essential k3s infrastructure images that must remain on each host
# to allow the cluster to restart without re-pulling.
K3S_PRESERVE_IMAGE_NAMES=(
    "rancher/k3s:v1.32.0-k3s1"
    "rancher/klipper-helm:v0.9.3-build20241008"
    "rancher/klipper-lb:v0.4.9"
    "rancher/local-path-provisioner:v0.0.30"
    "rancher/mirrored-coredns-coredns:1.12.0"
    "rancher/mirrored-library-traefik:2.11.10"
    "rancher/mirrored-metrics-server:v0.7.2"
    "rancher/mirrored-pause:3.6"
)

# Check if an image name is in the preserve list.
_is_preserved_image() {
    local img="$1"
    for preserved in "${K3S_PRESERVE_IMAGE_NAMES[@]}"; do
        [[ "$img" == "$preserved" ]] && return 0
    done
    return 1
}

remove_images_on_host() {
    local host="$1"
    local k3s_image="rancher/k3s:${CLUSTER_KUBERNETESVERSION}"

    log "Removing Qonductor images on ${host} …"

    for img in "${QONDUCTOR_IMAGE_NAMES[@]}"; do
        if _remote "$host" "docker image inspect '${img}' >/dev/null 2>&1"; then
            if [[ "$DRY_RUN" == "1" ]]; then
                echo "  [dry-run] Remove image ${img} on ${host}"
            else
                _remote "$host" "docker rmi '${img}' 2>/dev/null || docker rmi -f '${img}' 2>/dev/null || true"
                log "  ✓ ${img} removed"
            fi
        else
            log "  ${img} not found — skipping"
        fi
    done

    # Also remove the k3s base image if present (unless preserved).
    if _is_preserved_image "$k3s_image"; then
        log "  ${k3s_image} is preserved — skipping"
    elif _remote "$host" "docker image inspect '${k3s_image}' >/dev/null 2>&1"; then
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "  [dry-run] Remove image ${k3s_image} on ${host}"
        else
            _remote "$host" "docker rmi '${k3s_image}' 2>/dev/null || docker rmi -f '${k3s_image}' 2>/dev/null || true"
            log "  ✓ ${k3s_image} removed"
        fi
    fi

    # Remove k3s infrastructure images (pause, coredns, traefik, etc.).
    log "Removing k3s infrastructure images on ${host} …"
    for img in "${K3S_INFRA_IMAGE_NAMES[@]}"; do
        if _is_preserved_image "$img"; then
            log "  ${img} is preserved — skipping"
            continue
        fi
        if _remote "$host" "docker image inspect '${img}' >/dev/null 2>&1"; then
            if [[ "$DRY_RUN" == "1" ]]; then
                echo "  [dry-run] Remove image ${img} on ${host}"
            else
                _remote "$host" "docker rmi '${img}' 2>/dev/null || docker rmi -f '${img}' 2>/dev/null || true"
                log "  ✓ ${img} removed"
            fi
        else
            log "  ${img} not found — skipping"
        fi
    done
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

main() {
    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Qonductor Multi-Host k3s Cluster Teardown${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo ""

    [[ -f "$CLUSTER_CONFIG" ]] || die "cluster-config.yaml not found at $CLUSTER_CONFIG"
    load_cluster_config "$CLUSTER_CONFIG"

    local server_host="${SERVER_HOST}"
    local server_container="${SERVER_CONTAINERNAME}"
    local cluster_name="${CLUSTER_NAME}"

    # ---- Agents first (they depend on the server) ---------------------------
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        local agent_host="${!host_var}"
        local name_var="AGENTS_${i}_CONTAINERNAME"
        local agent_container="${!name_var}"
        stop_container "$agent_host" "$agent_container"

        if [[ "$CLEANUP_VOLUMES" == "1" ]]; then
            remove_volume "$agent_host" "${agent_container}-data"
        fi
    done

    # ---- Server last --------------------------------------------------------
    stop_container "$server_host" "$server_container"

    if [[ "$CLEANUP_VOLUMES" == "1" ]]; then
        remove_volume "$server_host" "${server_container}-data"
    fi

    # ---- Clean up k3s host-level state (/etc/rancher, /etc/qonductor) ------
    # These directories are bind-mounted from the host filesystem, so Docker
    # volumes alone do not cover them.  Stale node passwords cause "Node
    # password rejected" errors on the next deploy.
    if [[ "$CLEANUP_RANCHER" == "1" ]]; then
        log "Cleaning up k3s host state (/etc/rancher) on all nodes …"
        local -A _seen_rancher
        _seen_rancher["$server_host"]=1
        for ((i = 0; i < AGENTS_COUNT; i++)); do
            local host_var="AGENTS_${i}_HOST"
            _seen_rancher["${!host_var}"]=1
        done
        for host in "${!_seen_rancher[@]}"; do
            remove_rancher_state "$host"
        done
    fi

    if [[ "$CLEANUP_QONDUCTOR" == "1" ]]; then
        log "Cleaning up Qonductor host state (/etc/qonductor) on all nodes …"
        local -A _seen_qonc
        _seen_qonc["$server_host"]=1
        for ((i = 0; i < AGENTS_COUNT; i++)); do
            local host_var="AGENTS_${i}_HOST"
            _seen_qonc["${!host_var}"]=1
        done
        for host in "${!_seen_qonc[@]}"; do
            remove_qonductor_state "$host"
        done
    fi

    # ---- Optional: clean up Docker images -----------------------------------
    if [[ "$CLEANUP_IMAGES" == "1" ]]; then
        # Collect unique hosts (server + agents).
        local -A seen_hosts
        seen_hosts["$server_host"]=1
        for ((i = 0; i < AGENTS_COUNT; i++)); do
            local host_var="AGENTS_${i}_HOST"
            seen_hosts["${!host_var}"]=1
        done
        for host in "${!seen_hosts[@]}"; do
            remove_images_on_host "$host"
        done
        # Also clean up local images.
        log "Removing Qonductor images locally …"
        for img in "${QONDUCTOR_IMAGE_NAMES[@]}"; do
            if docker image inspect "$img" >/dev/null 2>&1; then
                if [[ "$DRY_RUN" == "1" ]]; then
                    echo "  [dry-run] Remove local image ${img}"
                else
                    docker rmi "$img" 2>/dev/null || docker rmi -f "$img" 2>/dev/null || true
                    log "  ✓ ${img} removed locally"
                fi
            fi
        done
    fi

    # ---- Optional: stop local registry --------------------------------------
    if [[ "$CLEANUP_REGISTRY" == "1" ]]; then
        log "Stopping local Docker registry …"
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "  [dry-run] Stop + remove registry on ${server_host}"
        else
            _remote "$server_host" "docker stop registry 2>/dev/null || true"
            _remote "$server_host" "docker rm -f registry 2>/dev/null || true"
            remove_volume "$server_host" "registry-data"
        fi
    fi

    # ---- Clean kubeconfig ---------------------------------------------------
    # Remove the cluster entry from local kubeconfig if present.
    local kubeconfig="${KUBECONFIG:-$HOME/.kube/config}"
    local context_name="default"  # k3s uses 'default' as context name

    if [[ -f "$kubeconfig" ]]; then
        if kubectl config get-contexts -o name 2>/dev/null | grep -qx "$context_name"; then
            if [[ "$DRY_RUN" == "1" ]]; then
                echo "  [dry-run] Remove kubeconfig context '${context_name}'"
                echo "  [dry-run]   kubectl config delete-context ${context_name}"
                echo "  [dry-run]   kubectl config delete-cluster ${context_name}"
                echo "  [dry-run]   kubectl config unset users.${context_name}"
            else
                log "Removing '${context_name}' context from local kubeconfig …"
                kubectl config delete-context "$context_name" 2>/dev/null || true
                kubectl config delete-cluster "$context_name" 2>/dev/null || true
                kubectl config unset "users.${context_name}" 2>/dev/null || true
                log "  ✓ Kubeconfig cleaned"
            fi
        fi
    fi

    # ---- Clean generated local files ---------------------------------------
    local images_tar="${SCRIPT_DIR}/images.tar"
    local image_list="${PROJECT_ROOT:-${SCRIPT_DIR}/../..}/image-list.txt"
    local workflow_registry="${PROJECT_ROOT:-${SCRIPT_DIR}/../..}/data/workflow_registry"

    if [[ -f "$images_tar" ]]; then
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "  [dry-run] Remove ${images_tar}"
        else
            rm -f "$images_tar"
            log "  ✓ ${images_tar} removed"
        fi
    fi

    if [[ -f "$image_list" ]]; then
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "  [dry-run] Remove ${image_list}"
        else
            rm -f "$image_list"
            log "  ✓ ${image_list} removed"
        fi
    fi

    if [[ -d "$workflow_registry" ]]; then
        if [[ "$DRY_RUN" == "1" ]]; then
            echo "  [dry-run] Remove ${workflow_registry}"
        else
            rm -rf "$workflow_registry"
            log "  ✓ ${workflow_registry} removed"
        fi
    fi

    echo ""
    log "Cluster '${cluster_name}' torn down."
    if [[ "$CLEANUP_VOLUMES" == "0" ]]; then
        warn "Docker volumes were preserved. This may cause 'Node password rejected' on redeploy."
        warn "  Remove manually or set CLEANUP_VOLUMES=1 (default)."
    fi
    if [[ "$CLEANUP_IMAGES" != "1" ]]; then
        warn "Docker images were preserved. Set CLEANUP_IMAGES=1 to remove them."
    fi
    if [[ "$CLEANUP_RANCHER" != "1" ]]; then
        warn "/etc/rancher was preserved. Set CLEANUP_RANCHER=1 to remove stale node passwords."
    fi
    if [[ "$CLEANUP_QONDUCTOR" != "1" ]]; then
        warn "/etc/qonductor was preserved. Set CLEANUP_QONDUCTOR=1 to remove stale QPU state."
    fi
    echo ""
}

main "$@"
