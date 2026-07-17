#!/usr/bin/env bash
# ============================================================================
# Qonductor Multi-Host k3s-in-Docker Cluster Deployment (Refactored)
# ============================================================================
# Reads docker/multi-host/cluster-config.yaml, deploys a k3s Kubernetes cluster
# across multiple physical hosts using Docker containers.
#
# Architecture:
#   - One k3s server container on the designated server host.
#   - One k3s agent container per agent entry (max 1 per physical host).
#   - All K8s components run inside Docker — the hosts only need Docker.
#
# Flow:
#   Phase 0: Pre-flight   — deps, config, firewall hints
#   Phase 1: Teardown      — destroy existing containers/volumes/images
#   Phase 2: Image Prepare — load from images.tar or build from Dockerfiles
#   Phase 3: Distribute    — push images to Docker on every host
#   Phase 4: Deploy k3s    — server → agents → wait for nodes
#   Phase 5: Containerd    — import images into k3s embedded containerd
#   Phase 6: Configure     — QPU profiles, labels, CRDs, controllers
#   Phase 7: Summary       — print cluster info
#
# Usage:
#   bash docker/multi-host/deploy-cluster.sh              # full deploy
#   SKIP_TEARDOWN=1  bash docker/multi-host/deploy-cluster.sh   # keep existing
#   # preserve volumes (skip volume cleanup, may cause node password issues):
#   CLEANUP_VOLUMES=0 bash docker/multi-host/deploy-cluster.sh
#   CLEANUP_RANCHER=0 bash docker/multi-host/deploy-cluster.sh   # keep /etc/rancher
#   DRY_RUN=1        bash docker/multi-host/deploy-cluster.sh   # print only
#
# Image sources (checked in order):
#   1. ${IMAGES_TAR} file exists → load from tar (no build needed)
#   2. Otherwise → docker build + docker pull, optional SAVE_IMAGES_TAR=1
#
# Environment variables:
#   IMAGES_TAR              path to images.tar (default: ${PROJECT_ROOT}/images.tar)
#   SKIP_TEARDOWN           skip teardown phase
#   CLEANUP_VOLUMES         remove Docker volumes during teardown (default: 1)
#   SKIP_IMAGE_BUILD        skip Docker image builds / tar load (images pre-loaded)
#   SKIP_IMAGE_DISTRIBUTE   skip image distribution to remote hosts
#   SKIP_CONTAINERD_IMPORT  skip importing into k3s embedded containerd
#   SKIP_CONTROLLERS        skip operator/device-plugin deployment
#   SAVE_IMAGES_TAR         save all images to images.tar after building
#   SKIP_FIREWALL           skip firewall port hints
#   DRY_RUN                 print commands without executing
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CLUSTER_CONFIG="${SCRIPT_DIR}/cluster-config.yaml"
QPUS_DIR="${PROJECT_ROOT}/qpus"
DEPLOY_DIR="${PROJECT_ROOT}/deploy"

# ---- Flags -------------------------------------------------------------------
SKIP_FIREWALL="${SKIP_FIREWALL:-1}"
SKIP_TEARDOWN="${SKIP_TEARDOWN:-0}"
CLEANUP_VOLUMES="${CLEANUP_VOLUMES:-1}"
CLEANUP_RANCHER="${CLEANUP_RANCHER:-0}"
SKIP_IMAGE_BUILD="${SKIP_IMAGE_BUILD:-0}"
SKIP_IMAGE_DISTRIBUTE="${SKIP_IMAGE_DISTRIBUTE:-0}"
SKIP_CONTAINERD_IMPORT="${SKIP_CONTAINERD_IMPORT:-0}"
SKIP_CONTROLLERS="${SKIP_CONTROLLERS:-0}"
SAVE_IMAGES_TAR="${SAVE_IMAGES_TAR:-0}"
DRY_RUN="${DRY_RUN:-0}"
IMAGES_TAR="${IMAGES_TAR:-${SCRIPT_DIR}/images.tar}"
IMAGE_LIST_FILE="${PROJECT_ROOT}/image-list.txt"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[deploy]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC}  $*"; }
die()  { echo -e "${RED}[fatal]${NC} $*" >&2; exit 1; }

# ---- Banner ------------------------------------------------------------------

_banner() {
    local phase="$1"
    echo ""
    echo -e "${GREEN}── ${phase} ──────────────────────────────────────────────${NC}"
    echo ""
}

# ==============================================================================
# YAML config loader (stdlib-only Python parser, no PyYAML needed)
# ==============================================================================
source "${SCRIPT_DIR}/lib/config-loader.sh"

# ==============================================================================
# SSH helpers — run locally when host is localhost / same IP
# ==============================================================================

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

_remote_dry() {
    local host="$1"; shift
    if _is_local "$host"; then
        echo "  [dry-run] $*"
    else
        echo "  [dry-run] ssh $host $*"
    fi
}

_run() {
    local host="$1"; shift
    if [[ "$DRY_RUN" == "1" ]]; then
        _remote_dry "$host" "$@"
        return 0
    fi
    _remote "$host" "$@"
}

_remote_user_id() {
    local host="$1"
    _remote "$host" "id -u"
}

_remote_group_id() {
    local host="$1"
    _remote "$host" "id -g"
}

_qpu_instance_name() {
    local node_name="$1"
    local qpu_index="$2"
    local qpu_file="$3"
    python3 - "$node_name" "$qpu_index" "${QPUS_DIR}/${qpu_file}" <<'PY'
import hashlib
import json
import re
import sys

node_name, qpu_index, qpu_path = sys.argv[1:4]
with open(qpu_path, encoding="utf-8") as fh:
    base_name = str(json.load(fh)["name"])

raw = f"{base_name}-{node_name}-{qpu_index}"
safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("._-").lower()
if not safe:
    safe = "qpu"

# Kubernetes label names are limited to 63 characters.  The deploy script uses
# this value in label key names like qonductor.io/backend-<qpu_name>, so keep
# the generated instance ID short enough for that suffix too.
if len(safe) > 55:
    digest = hashlib.sha1(safe.encode("utf-8")).hexdigest()[:8]
    safe = f"{safe[:46].rstrip('._-')}-{digest}"

print(safe)
PY
}

_render_qpu_instance_profile() {
    local node_name="$1"
    local qpu_index="$2"
    local qpu_file="$3"
    local output_path="$4"
    python3 - "$node_name" "$qpu_index" "${QPUS_DIR}/${qpu_file}" "$output_path" <<'PY'
import hashlib
import json
import re
import sys
from pathlib import Path

node_name, qpu_index, qpu_path, output_path = sys.argv[1:5]
with open(qpu_path, encoding="utf-8") as fh:
    data = json.load(fh)

base_name = str(data["name"])
raw = f"{base_name}-{node_name}-{qpu_index}"
instance_name = re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("._-").lower()
if not instance_name:
    instance_name = "qpu"
if len(instance_name) > 55:
    digest = hashlib.sha1(instance_name.encode("utf-8")).hexdigest()[:8]
    instance_name = f"{instance_name[:46].rstrip('._-')}-{digest}"

data["name"] = instance_name
data["qonductor_base_name"] = base_name
data["qonductor_profile_source"] = Path(qpu_path).name
data["qonductor_node_name"] = node_name
data["qonductor_instance_index"] = int(qpu_index)

with open(output_path, "w", encoding="utf-8") as fh:
    json.dump(data, fh, indent=2, sort_keys=True)
    fh.write("\n")
PY
}

_clear_qpu_node_labels() {
    local node_name="$1"
    local labels_to_remove
    labels_to_remove="$(
        kubectl get node "$node_name" -o json 2>/dev/null | python3 -c '
import json
import sys

try:
    node = json.load(sys.stdin)
except json.JSONDecodeError:
    raise SystemExit(0)

labels = (node.get("metadata", {}) or {}).get("labels", {}) or {}
for key in sorted(labels):
    if key.startswith("qonductor.io/backend-") or key.startswith("qonductor.io/qpu-"):
        print(f"{key}-")
'
    )"
    [[ -n "$labels_to_remove" ]] || return 0
    # shellcheck disable=SC2086
    kubectl label node "$node_name" $labels_to_remove --overwrite 2>/dev/null || true
}

# ==============================================================================
# Helpers: collect hosts / containers from parsed config
# ==============================================================================

_all_hosts() {
    echo "${SERVER_HOST}"
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        echo "${!host_var}"
    done
}

_all_containers() {
    echo "${SERVER_CONTAINERNAME}"
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local name_var="AGENTS_${i}_CONTAINERNAME"
        echo "${!name_var}"
    done
}

_container_for_host() {
    local host="$1"
    if [[ "$host" == "${SERVER_HOST}" ]]; then
        echo "${SERVER_CONTAINERNAME}"
        return 0
    fi
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        if [[ "${!host_var}" == "$host" ]]; then
            local name_var="AGENTS_${i}_CONTAINERNAME"
            echo "${!name_var}"
            return 0
        fi
    done
    return 1
}

# ==============================================================================
# Image list
# ==============================================================================

_QONDUCTOR_IMAGES=(
    "qonductor-operator:latest"
    "qonductor-device-plugin:latest"
    "qonductor-quantum-executor:latest"
)

# k3s infrastructure images (must match the k3s version).
# Verified against k3s v1.32.0+k3s1 (July 2026).
_K3S_INFRA_IMAGES=(
    "rancher/mirrored-pause:3.6"
    "rancher/mirrored-coredns-coredns:1.12.0"
    "rancher/mirrored-library-traefik:2.11.10"
    "rancher/local-path-provisioner:v0.0.30"
    "rancher/mirrored-metrics-server:v0.7.2"
    "rancher/klipper-helm:v0.9.3-build20241008"
    "rancher/klipper-lb:v0.4.9"
)

_get_k3s_image()    { echo "rancher/k3s:${CLUSTER_KUBERNETESVERSION}"; }
_get_all_images()   { _get_k3s_image; printf '%s\n' "${_K3S_INFRA_IMAGES[@]}" "${_QONDUCTOR_IMAGES[@]}"; }

_read_image_list() {
    # Read image names from image-list.txt if available, otherwise use built-in.
    if [[ -f "$IMAGE_LIST_FILE" ]]; then
        grep -v '^[[:space:]]*$' "$IMAGE_LIST_FILE" | grep -v '^#'
    else
        _get_all_images
    fi
}

_load_image_list() {
    local -n _out="$1"
    mapfile -t _out < <(_read_image_list)
    [[ "${#_out[@]}" -gt 0 ]] || die "No images resolved from ${IMAGE_LIST_FILE} or built-in image list."
}

_require_docker_images() {
    local host="$1"; shift
    local missing=()

    for img in "$@"; do
        if ! _remote "$host" "docker image inspect '${img}' >/dev/null 2>&1"; then
            missing+=("$img")
        fi
    done

    if [[ "${#missing[@]}" -gt 0 ]]; then
        warn "Missing Docker image(s) on ${host}:"
        printf '  - %s\n' "${missing[@]}"
        return 1
    fi
    return 0
}

# ==============================================================================
# Phase 0: Pre-flight checks
# ==============================================================================

check_deps() {
    log "Checking local dependencies …"
    command -v docker   >/dev/null 2>&1 || die "docker is required but not found"
    command -v kubectl  >/dev/null 2>&1 || die "kubectl is required but not found"
    command -v ssh      >/dev/null 2>&1 || die "ssh is required but not found"
    command -v python3  >/dev/null 2>&1 || die "python3 is required but not found"
    log "  ✓ Dependencies OK"
}

validate_config() {
    log "Validating cluster configuration …"
    [[ -f "$CLUSTER_CONFIG" ]] || die "cluster-config.yaml not found at $CLUSTER_CONFIG"
    [[ -n "${SERVER_HOST:-}" && "${SERVER_HOST:-}" != "null" ]] || die "server.host is required"

    local qpu_count=0
    for ((i = 0; i < ${AGENTS_COUNT:-0}; i++)); do
        local host_var="AGENTS_${i}_HOST"
        [[ -n "${!host_var:-}" && "${!host_var:-}" != "null" ]] || die "agents[$i].host is required"

        local qpu_count_var="AGENTS_${i}_QPUS_COUNT"
        local qpus=()
        # bash 4.4: indirect expansion (${!ref}) of an empty array triggers
        # nounset; guard with the scalar _COUNT variable (unaffected).
        if [[ "${!qpu_count_var:-0}" -gt 0 ]]; then
            local qpus_ref="AGENTS_${i}_QPUS[@]"
            qpus=("${!qpus_ref}")
        fi
        for qpu_file in "${qpus[@]}"; do
            local qpu_path="${QPUS_DIR}/${qpu_file}"
            [[ -f "$qpu_path" ]] || die "QPU profile not found: $qpu_path"
            python3 -c "
import json
data = json.load(open('$qpu_path'))
assert 'name' in data, 'Missing name'
assert 'max_qubits' in data, 'Missing max_qubits'
assert 'coupling_map' in data, 'Missing coupling_map'
assert 'hardware' in data, 'Missing hardware'
" || die "Invalid QPU profile: $qpu_file"
            qpu_count=$((qpu_count + 1))
        done
    done
    log "  ✓ ${AGENTS_COUNT:-0} agent(s) with ${qpu_count} QPU profile(s)"
}

show_firewall_hints() {
    [[ "$SKIP_FIREWALL" == "1" ]] && return 0

    echo ""
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${YELLOW}  Firewall Requirements${NC}"
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo "  TCP 6443   — k3s API Server"
    echo "  UDP 8472   — Flannel VXLAN overlay"
    echo "  TCP 10250  — Kubelet API"
    echo ""
    echo "  Quick fix:"
    echo "    sudo firewall-cmd --add-port=6443/tcp --add-port=8472/udp --add-port=10250/tcp --permanent && sudo firewall-cmd --reload"
    echo ""
    echo -e "${YELLOW}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    sleep 3
}

check_remote_docker() {
    local host="$1"
    local version
    version="$(_remote "$host" "docker --version 2>/dev/null" || echo "")"
    [[ -n "$version" ]] || die "Docker not found on ${host}. Install Docker >= 20.10 first."
    log "  ${host}: ${version}"
}

detect_iface() {
    local host="$1"

    if [[ "$host" == "${SERVER_HOST}" ]]; then
        local first_agent="${AGENTS_0_HOST:-}"
        if [[ -n "$first_agent" ]]; then
            _remote "$host" "ip -4 route get '${first_agent}' 2>/dev/null | awk '{for(i=1;i<=NF;i++) if(\$i==\"dev\") print \$(i+1)}' | head -1"
        else
            _remote "$host" "ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if(\$i==\"dev\") print \$(i+1)}' | head -1"
        fi
    else
        _remote "$host" "ip -4 route get '${SERVER_HOST}' 2>/dev/null | awk '{for(i=1;i<=NF;i++) if(\$i==\"dev\") print \$(i+1)}' | head -1"
    fi
}

# ==============================================================================
# Phase 1: Teardown existing cluster
# ==============================================================================

teardown_existing() {
    if [[ "$SKIP_TEARDOWN" == "1" ]]; then
        warn "SKIP_TEARDOWN=1 — skipping teardown."
        return 0
    fi

    _banner "Phase 1: Teardown existing cluster"

    local -A seen_hosts
    seen_hosts["${SERVER_HOST}"]="${SERVER_CONTAINERNAME}"
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        local name_var="AGENTS_${i}_CONTAINERNAME"
        seen_hosts["${!host_var}"]="${!name_var}"
    done

    for host in "${!seen_hosts[@]}"; do
        local container="${seen_hosts[$host]}"

        # Stop and remove k3s container.
        if _remote "$host" "docker ps -a --format '{{.Names}}' | grep -qx '${container}'" 2>/dev/null; then
            log "Stopping ${container} on ${host} …"
            _run "$host" "docker stop '${container}' 2>/dev/null || true"
            _run "$host" "docker rm -f '${container}' 2>/dev/null || true"
            log "  ✓ ${container} removed"
        else
            log "  ${container} on ${host}: not present"
        fi

        # Optionally remove data volume.
        if [[ "$CLEANUP_VOLUMES" == "1" ]]; then
            if _remote "$host" "docker volume ls --format '{{.Name}}' | grep -qx '${container}-data'" 2>/dev/null; then
                warn "Removing volume ${container}-data on ${host} …"
                _run "$host" "docker volume rm '${container}-data'" || warn "  Could not remove volume"
            fi
        fi

        # Remove stale k3s host-level state.
        # k3s bind-mounts /etc/rancher from the host filesystem — the node
        # password stored there survives container removal and causes
        # "Node password rejected" errors on redeploy.
        if [[ "${CLEANUP_RANCHER:-1}" == "1" ]] && _remote "$host" "test -d /etc/rancher" 2>/dev/null; then
            warn "Removing /etc/rancher on ${host} (stale node passwords) …"
            _run "$host" "sudo rm -rf /etc/rancher" || warn "  Could not remove /etc/rancher"
        fi

        # Remove old Qonductor images (so we start clean).
        for img in "${_QONDUCTOR_IMAGES[@]}"; do
            if _remote "$host" "docker image inspect '${img}' >/dev/null 2>&1"; then
                _run "$host" "docker rmi '${img}' 2>/dev/null || true"
            fi
        done
    done

    log "  ✓ Teardown complete"
}

# ==============================================================================
# Phase 2: Prepare images (from tar or build)
# ==============================================================================

_build_qonductor_images() {
    log "Building Qonductor Docker images …"
    mkdir -p "${PROJECT_ROOT}/data/workflow_registry"

    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.operator" \
        -t qonductor-operator:latest "${PROJECT_ROOT}"

    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.device-plugin" \
        -t qonductor-device-plugin:latest "${PROJECT_ROOT}"

    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.quantum-executor" \
        -t qonductor-quantum-executor:latest "${PROJECT_ROOT}"

    log "  ✓ Qonductor images built"
}

_pull_k3s_images() {
    local k3s_image="$(_get_k3s_image)"

    if docker image inspect "$k3s_image" >/dev/null 2>&1; then
        log "  k3s image ${k3s_image} already present"
    else
        log "  Pulling ${k3s_image} …"
        docker pull "$k3s_image" || warn "Failed to pull ${k3s_image} from registry"
    fi

    for img in "${_K3S_INFRA_IMAGES[@]}"; do
        if docker image inspect "$img" >/dev/null 2>&1; then
            log "  [skip] $img (already present)"
        else
            log "  Pulling $img …"
            docker pull "$img" || warn "Failed to pull ${img}"
        fi
    done
    log "  ✓ k3s images ready"
}

_save_all_to_tar() {
    local tar_path="$1"
    local images=()
    mapfile -t images < <(_get_all_images)

    log "Saving ${#images[@]} images → ${tar_path} …"
    docker save "${images[@]}" -o "$tar_path"
    local size; size=$(du -h "$tar_path" | cut -f1)
    log "  ✓ ${tar_path} (${size})"

    # Also write image-list.txt for future reference.
    _get_all_images > "$IMAGE_LIST_FILE"
    log "  ✓ ${IMAGE_LIST_FILE} updated"
}

prepare_images() {
    _banner "Phase 2: Prepare images"

    if [[ "$SKIP_IMAGE_BUILD" == "1" ]]; then
        local -a images
        _load_image_list images
        warn "SKIP_IMAGE_BUILD=1 — skipping local build/load."
        if [[ "$SKIP_IMAGE_DISTRIBUTE" != "1" ]]; then
            log "Verifying local Docker has images needed for distribution …"
            _require_docker_images "localhost" "${images[@]}" || \
                die "SKIP_IMAGE_BUILD=1 requires all images in local Docker unless SKIP_IMAGE_DISTRIBUTE=1."
        fi
        return 0
    fi

    # ---- Path A: images.tar exists, load from it -------------------------------
    if [[ -f "$IMAGES_TAR" ]]; then
        local tar_size; tar_size=$(du -h "$IMAGES_TAR" | cut -f1)
        log "Found ${IMAGES_TAR} (${tar_size}) — loading into local Docker …"
        docker load -i "$IMAGES_TAR"
        log "  ✓ Images loaded from ${IMAGES_TAR}"
        return 0
    fi

    # ---- Path B: Build + pull from source -------------------------------------
    log "No ${IMAGES_TAR} found — building from source …"
    echo ""

    _build_qonductor_images
    _pull_k3s_images

    if [[ "$SAVE_IMAGES_TAR" == "1" ]]; then
        _save_all_to_tar "$IMAGES_TAR"
    else
        # Always keep image-list.txt up to date.
        _get_all_images > "$IMAGE_LIST_FILE"
        log "  ✓ ${IMAGE_LIST_FILE} updated"
    fi
}

# ==============================================================================
# Phase 3: Distribute images to every host's Docker
# ==============================================================================

distribute_images() {
    _banner "Phase 3: Distribute images to all hosts"

    local -a hosts
    mapfile -t hosts < <(_all_hosts | sort -u)

    local -a images
    _load_image_list images

    if [[ "$SKIP_IMAGE_DISTRIBUTE" == "1" ]]; then
        warn "SKIP_IMAGE_DISTRIBUTE=1 — verifying images are already present on every host."
        for host in "${hosts[@]}"; do
            _require_docker_images "$host" "${images[@]}" || \
                die "SKIP_IMAGE_DISTRIBUTE=1 requires all images to be present on ${host}."
        done
        return 0
    fi

    for host in "${hosts[@]}"; do
        if _is_local "$host"; then
            log "Verifying localhost ${host} already has required images …"
            _require_docker_images "$host" "${images[@]}" || \
                die "Local host ${host} is part of the cluster but is missing required images."
            continue
        fi

        log "Distributing ${#images[@]} images to ${host} …"

        # Stream images tar over SSH to avoid temp files on remote.
        log "  Sending images via ssh pipe …"
        if docker save "${images[@]}" 2>/dev/null | \
           ssh -o ConnectTimeout=10 "$host" "docker load 2>/dev/null"; then
            log "  ✓ ${host} done"
        else
            die "Failed to distribute images to ${host}"
        fi
    done

    log "  ✓ All hosts have images in Docker"
}

# ==============================================================================
# Phase 4: Deploy k3s server + agents
# ==============================================================================

deploy_server() {
    local host="${SERVER_HOST}"
    local container_name="${SERVER_CONTAINERNAME}"
    local node_name="${SERVER_NODENAME}"
    local node_type="${SERVER_NODETYPE}"
    local flannel_iface="${CLUSTER_FLANNELINTERFACE}"
    local k3s_version="${CLUSTER_KUBERNETESVERSION}"
    local k3s_token="${CLUSTER_K3STOKEN}"
    local cpus="${SERVER_CPUS:-}"
    local memory="${SERVER_MEMORY:-}"

    # Build optional Docker resource flags.
    local cpu_flag="" mem_flag=""
    [[ -n "$cpus" ]]   && cpu_flag="--cpus ${cpus}"
    [[ -n "$memory" ]] && mem_flag="--memory ${memory}"

    # Auto-detect interface if set to "auto".
    if [[ "$flannel_iface" == "auto" ]]; then
        flannel_iface="$(detect_iface "$host")"
        log "Auto-detected Flannel interface on ${host}: ${flannel_iface}"
    fi

    # Safety: container should have been removed in teardown.  If it somehow
    # still exists, check if running — reuse if so, otherwise remove.
    if _remote "$host" "docker ps -a --format '{{.Names}}' | grep -qx '${container_name}'" 2>/dev/null; then
        if _remote "$host" "docker ps --format '{{.Names}}' | grep -qx '${container_name}'" 2>/dev/null; then
            warn "Container '${container_name}' is already running — reusing."
            return 0
        else
            warn "Container '${container_name}' exists but stopped — removing."
            _run "$host" "docker rm -f ${container_name}"
        fi
    fi

    log "Deploying k3s server on ${host} …"
    log "  Container: ${container_name}  |  Node: ${node_name}  |  Type: ${node_type}"
    log "  Flannel iface: ${flannel_iface}"
    [[ -n "$cpus" ]] && log "  CPUs: ${cpus}" ; [[ -n "$memory" ]] && log "  Memory: ${memory}"

    # Create data volume.
    _run "$host" "docker volume create ${container_name}-data 2>/dev/null || true"
    _run "$host" "sudo mkdir -p /etc/qonductor/qpus"

    _run "$host" "docker run -d \
        --name '${container_name}' \
        --network host \
        --privileged \
        --restart unless-stopped \
        ${cpu_flag} \
        ${mem_flag} \
        -v '${container_name}-data:/var/lib/rancher/k3s' \
        -v '/etc/rancher:/etc/rancher' \
        -v '/etc/qonductor/qpus:/etc/qonductor/qpus:ro' \
        -v '${PROJECT_ROOT}/data:${PROJECT_ROOT}/data' \
        -e K3S_KUBECONFIG_MODE=644 \
        -e K3S_TOKEN='${k3s_token}' \
        'rancher/k3s:${k3s_version}' \
        server \
            --node-name='${node_name}' \
            --bind-address='${host}' \
            --advertise-address='${host}' \
            --flannel-iface='${flannel_iface}' \
            --kubelet-arg='node-ip=${host}' \
            --node-label='qonductor.io/node-type=${node_type}' \
    "

    log "  ✓ Server container started"
}

wait_for_server() {
    local host="${SERVER_HOST}"
    local max_attempts=30 delay=3

    log "Waiting for k3s API server on ${host}:6443 …"

    for ((i = 1; i <= max_attempts; i++)); do
        if _remote "$host" "curl -sk -o /dev/null -w '%{http_code}' https://127.0.0.1:6443/healthz" 2>/dev/null | grep -q "200\|401\|403"; then
            log "  ✓ API server reachable (attempt ${i})"
            return 0
        fi
        echo -n "."
        sleep "$delay"
    done

    die "API server did not become ready within $((max_attempts * delay)) seconds."
}

fetch_kubeconfig() {
    local host="${SERVER_HOST}"
    local container_name="${SERVER_CONTAINERNAME}"

    log "Fetching kubeconfig from ${container_name} on ${host} …"

    local tmp_kubeconfig="/tmp/k3s-multi-host-config.yaml"

    if _is_local "$host"; then
        # docker cp to LOCAL filesystem writes the actual file content (not tar).
        docker cp "${container_name}:/etc/rancher/k3s/k3s.yaml" "$tmp_kubeconfig"
    else
        # Two-step copy for remote hosts:
        #   1. docker cp to a temp file on the remote HOST filesystem.
        #      docker cp from a remote container to a LOCAL path (where "local"
        #      is the remote host) writes the raw file — not a tar archive.
        #   2. scp the raw file back to the deploy machine.
        #      scp is purpose-built for binary-clean file transfer over SSH
        #      and is immune to MOTD/banner injection.
        #
        # The original approach ("docker cp … -" piped over ssh stdout) wrote a
        # TAR ARCHIVE (not file content) to the local file — docker cp uses tar
        # format when stdout is the destination.  This produced a file full of
        # null bytes, which kubectl rejected as "control characters not allowed".
        local remote_tmp="/tmp/k3s-multi-host-config-remote.yaml"
        _run "$host" "docker cp '${container_name}:/etc/rancher/k3s/k3s.yaml' '${remote_tmp}'"
        scp -o ConnectTimeout=5 -o StrictHostKeyChecking=accept-new \
            "${host}:${remote_tmp}" "$tmp_kubeconfig"
        _run "$host" "rm -f '${remote_tmp}'"
    fi

    # Verify the fetched file is a valid kubeconfig.
    local first_line
    first_line="$(head -1 "$tmp_kubeconfig")"
    if [[ "$first_line" != "apiVersion:"* ]]; then
        warn "Kubeconfig does not start with 'apiVersion:' — file may be corrupted:"
        echo ""
        head -20 "$tmp_kubeconfig"
        echo ""
        die "Kubeconfig is invalid (first line: '${first_line}')."
    fi

    # Replace 127.0.0.1 with the server's actual IP.
    sed -i "s/127\.0\.0\.1/${host}/g" "$tmp_kubeconfig"

    mkdir -p ~/.kube
    cp "$tmp_kubeconfig" ~/.kube/config
    chmod 600 ~/.kube/config

    export KUBECONFIG=~/.kube/config
    log "  ✓ Kubeconfig written to ~/.kube/config"
}

deploy_agents() {
    local k3s_version="${CLUSTER_KUBERNETESVERSION}"
    local k3s_token="${CLUSTER_K3STOKEN}"
    local server_host="${SERVER_HOST}"

    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        local host="${!host_var}"
        local name_var="AGENTS_${i}_CONTAINERNAME"
        local container_name="${!name_var}"
        local node_var="AGENTS_${i}_NODENAME"
        local node_name="${!node_var}"
        local type_var="AGENTS_${i}_NODETYPE"
        local node_type="${!type_var}"
        local flannel_iface="${CLUSTER_FLANNELINTERFACE}"
        local cpus_var="AGENTS_${i}_CPUS"
        local cpus="${!cpus_var:-}"
        local mem_var="AGENTS_${i}_MEMORY"
        local memory="${!mem_var:-}"

        local cpu_flag="" mem_flag=""
        [[ -n "$cpus" ]]   && cpu_flag="--cpus ${cpus}"
        [[ -n "$memory" ]] && mem_flag="--memory ${memory}"

        if [[ "$flannel_iface" == "auto" ]]; then
            flannel_iface="$(detect_iface "$host")"
            log "Auto-detected Flannel interface on ${host}: ${flannel_iface}"
        fi

        # Safety check — container should have been removed in teardown.
        if _remote "$host" "docker ps -a --format '{{.Names}}' | grep -qx '${container_name}'" 2>/dev/null; then
            if _remote "$host" "docker ps --format '{{.Names}}' | grep -qx '${container_name}'" 2>/dev/null; then
                warn "Container '${container_name}' is already running — reusing."
                continue
            else
                warn "Container '${container_name}' exists but stopped — removing."
                _run "$host" "docker rm -f ${container_name}"
            fi
        fi

        log "Deploying k3s agent on ${host} …"
        log "  Container: ${container_name}  |  Node: ${node_name}  |  Type: ${node_type}"
        log "  Flannel iface: ${flannel_iface}"
        [[ -n "$cpus" ]] && log "  CPUs: ${cpus}" ; [[ -n "$memory" ]] && log "  Memory: ${memory}"

        _run "$host" "docker volume create ${container_name}-data 2>/dev/null || true"
        _run "$host" "sudo mkdir -p /etc/qonductor/qpus"

        _run "$host" "docker run -d \
            --name '${container_name}' \
            --network host \
            --privileged \
            --restart unless-stopped \
            ${cpu_flag} \
            ${mem_flag} \
            -v '${container_name}-data:/var/lib/rancher/k3s' \
            -v '/etc/qonductor/qpus:/etc/qonductor/qpus:ro' \
            -e K3S_TOKEN='${k3s_token}' \
            -e K3S_URL='https://${server_host}:6443' \
            'rancher/k3s:${k3s_version}' \
            agent \
                --node-name='${node_name}' \
                --flannel-iface='${flannel_iface}' \
                --kubelet-arg='node-ip=${host}' \
                --node-label='qonductor.io/node-type=${node_type}' \
        "

        log "  ✓ Agent container started (${container_name})"
    done
}

wait_for_nodes() {
    log "Waiting for all nodes to become Ready …"

    local -a expected_nodes=("${SERVER_NODENAME}")
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local node_var="AGENTS_${i}_NODENAME"
        expected_nodes+=("${!node_var}")
    done

    for node_name in "${expected_nodes[@]}"; do
        local waited=0
        local delay=3

        while ! kubectl get node "$node_name" >/dev/null 2>&1; do
            if [[ "$waited" -ge 300 ]]; then
                warn "Node ${node_name} did not join the cluster — showing current status:"
                kubectl get nodes -o wide
                die "All configured nodes must join before proceeding. Check node status above."
            fi
            sleep "$delay"
            waited=$((waited + delay))
        done

        kubectl wait --for=condition=Ready "node/${node_name}" --timeout=300s 2>/dev/null || {
            warn "Node ${node_name} did not become Ready — showing current status:"
            kubectl get nodes -o wide
            die "All configured nodes must join and become Ready before proceeding. Check node status above."
        }
    done

    log "  ✓ Nodes:"
    kubectl get nodes -o wide
}

# ==============================================================================
# Phase 5: Import images into k3s embedded containerd
# ==============================================================================

import_containerd() {
    _banner "Phase 5: Import images into k3s containerd"

    if [[ "$SKIP_CONTAINERD_IMPORT" == "1" ]]; then
        warn "SKIP_CONTAINERD_IMPORT=1 — skipping containerd import."
        return 0
    fi

    local -a images
    _load_image_list images

    local -a hosts
    mapfile -t hosts < <(_all_hosts | sort -u)

    # Per-host log directory for containerd import errors.
    local import_log_dir="${PROJECT_ROOT}/data/deploy_logs/containerd_import"
    mkdir -p "$import_log_dir"

    local -a import_pids=()
    local -a import_labels=()
    local failures=0

    for host in "${hosts[@]}"; do
        local container; container="$(_container_for_host "$host")" || {
            warn "No container found for host ${host} — skipping"
            continue
        }

        # Verify container is running.
        if ! _remote "$host" "docker ps --format '{{.Names}}' | grep -qx '${container}'" 2>/dev/null; then
            warn "Container ${container} not running on ${host} — skipping containerd import"
            continue
        fi

        log "Queueing import of ${#images[@]} images into ${container} on ${host} …"
        _require_docker_images "$host" "${images[@]}" || \
            die "Cannot import into ${container}: required image(s) missing from Docker on ${host}."

        local host_log="${import_log_dir}/${host}_${container}.log"
        # Truncate log for a fresh run.
        if _is_local "$host"; then
            : >"$host_log" 2>/dev/null || true
        else
            _remote "$host" "mkdir -p '$(dirname "$host_log")'" 2>/dev/null || true
            : >"$host_log" 2>/dev/null || true
        fi

        if [[ "$DRY_RUN" == "1" ]]; then
            echo "  [dry-run] ${host}: import ${#images[@]} images one-by-one into ${container}"
        else
            (
                local import_ok=0
                local import_fail=0
                for img in "${images[@]}"; do
                    # Import each image individually so a failure on one does not
                    # break the rest.  Capture stderr into the per-host log.
                    if _remote "$host" \
                        "docker save '${img}' 2>&1 | docker exec -i '${container}' ctr images import - 2>&1" \
                        >>"$host_log" 2>&1; then
                        import_ok=$((import_ok + 1))
                    else
                        import_fail=$((import_fail + 1))
                        echo "[FAIL] $(date -Iseconds) ${host}:${container} — ${img}" >>"$host_log"
                    fi
                done

                # Post-import verification: check each image exists in containerd.
                local verify_missing=0
                local ctr_img_list
                ctr_img_list="$(_remote "$host" "docker exec '${container}' ctr images ls -q 2>&1" 2>>"$host_log" || true)"
                for img in "${images[@]}"; do
                    # Match without the tag for flexible comparison (ctr list outputs
                    # full references like docker.io/library/<name>:<tag>).
                    if ! echo "$ctr_img_list" | grep -qF "${img}"; then
                        verify_missing=$((verify_missing + 1))
                        echo "[MISSING] $(date -Iseconds) ${host}:${container} — ${img} not found after import" >>"$host_log"
                    fi
                done

                if [[ "$import_fail" -gt 0 || "$verify_missing" -gt 0 ]]; then
                    echo "[SUMMARY] $(date -Iseconds) ${host}:${container} — ok=${import_ok} fail=${import_fail} missing=${verify_missing}" >>"$host_log"
                    exit 1
                fi
                echo "[SUMMARY] $(date -Iseconds) ${host}:${container} — all ${import_ok} images imported and verified" >>"$host_log"
                exit 0
            ) &
            import_pids+=("$!")
            import_labels+=("${host}:${container}")
        fi
    done

    for i in "${!import_pids[@]}"; do
        local pid="${import_pids[$i]}"
        local label="${import_labels[$i]}"
        if wait "$pid"; then
            log "  ✓ ${label} done"
        else
            warn "  ${label} import failed — see ${import_log_dir}/${label}.log"
            failures=$((failures + 1))
        fi
    done

    [[ "$failures" -eq 0 ]] || die "${failures} containerd import task(s) failed"
    log "  ✓ Containerd import complete (logs: ${import_log_dir})"
}

# ==============================================================================
# Phase 6: Configure cluster
# ==============================================================================

sync_qpu_profiles() {
    log "Syncing QPU profiles to remote hosts …"

    local -a hosts
    mapfile -t hosts < <(_all_hosts | sort -u)
    for host in "${hosts[@]}"; do
        _run "$host" "sudo mkdir -p /etc/qonductor/qpus && sudo rm -f /etc/qonductor/qpus/*.json"
    done

    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        local host="${!host_var}"
        local node_var="AGENTS_${i}_NODENAME"
        local node_name="${!node_var}"
        local qpu_count_var="AGENTS_${i}_QPUS_COUNT"
        local qpus=()
        # bash 4.4: indirect expansion (${!ref}) of an empty array triggers
        # nounset; guard with the scalar _COUNT variable (unaffected).
        if [[ "${!qpu_count_var:-0}" -gt 0 ]]; then
            local qpus_ref="AGENTS_${i}_QPUS[@]"
            qpus=("${!qpus_ref}")
        fi
        local qpu_count=${#qpus[@]}

        if [[ "$qpu_count" -gt 0 ]]; then
            for ((j = 0; j < qpu_count; j++)); do
                local qpu_file="${qpus[$j]}"
                local qpu_name
                qpu_name="$(_qpu_instance_name "$node_name" "$j" "$qpu_file")"
                local rendered_profile
                rendered_profile="$(mktemp "/tmp/qonductor-qpu-${qpu_name}.XXXXXX.json")"
                _render_qpu_instance_profile "$node_name" "$j" "$qpu_file" "$rendered_profile"

                if _is_local "$host"; then
                    sudo cp "$rendered_profile" "/etc/qonductor/qpus/${qpu_name}.json"
                else
                    scp -q "$rendered_profile" "${host}:/tmp/${qpu_name}.json"
                    _remote "$host" "sudo mv /tmp/${qpu_name}.json /etc/qonductor/qpus/${qpu_name}.json"
                fi
                rm -f "$rendered_profile"
                log "  ✓ ${qpu_file} as ${qpu_name}.json → ${host}:/etc/qonductor/qpus/"
            done
        fi
    done
}

label_nodes() {
    log "Setting Qonductor node labels and QPU resources …"

    local server_node="${SERVER_NODENAME}"
    local server_type="${SERVER_NODETYPE}"

    kubectl label node "$server_node" "qonductor.io/node-type=${server_type}" --overwrite 2>/dev/null || true
    log "  ✓ ${server_node}: node-type=${server_type}"

    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local node_var="AGENTS_${i}_NODENAME"
        local node_name="${!node_var}"
        local type_var="AGENTS_${i}_NODETYPE"
        local node_type="${!type_var}"

        kubectl label node "$node_name" "qonductor.io/node-type=${node_type}" --overwrite 2>/dev/null || true
        _clear_qpu_node_labels "$node_name"

        local qpu_count_var="AGENTS_${i}_QPUS_COUNT"
        local qpus=()
        # bash 4.4: indirect expansion (${!ref}) of an empty array triggers
        # nounset; guard with the scalar _COUNT variable (unaffected).
        if [[ "${!qpu_count_var:-0}" -gt 0 ]]; then
            local qpus_ref="AGENTS_${i}_QPUS[@]"
            qpus=("${!qpus_ref}")
        fi
        local qpu_count_for_agent=${#qpus[@]}

        for ((j = 0; j < qpu_count_for_agent; j++)); do
            local qpu_file="${qpus[$j]}"
            local qpu_name
            qpu_name="$(_qpu_instance_name "$node_name" "$j" "$qpu_file")"

            kubectl label node "$node_name" "qonductor.io/backend-${qpu_name}=true" --overwrite 2>/dev/null || true
            kubectl label node "$node_name" "qonductor.io/qpu-${j}=${qpu_name}" --overwrite 2>/dev/null || true
        done

        kubectl patch node "$node_name" --type=json \
            -p="[{\"op\":\"add\",\"path\":\"/status/capacity/quantum.ibm.com~1qpu\",\"value\":\"${qpu_count_for_agent}\"}]" \
            2>/dev/null || warn "Could not patch QPU capacity on ${node_name} (will be handled by device plugin)"

        log "  ✓ ${node_name}: node-type=${node_type}, ${qpu_count_for_agent} QPU(s)"
    done
}

deploy_qonductor_crds() {
    log "Applying Qonductor CRDs …"
    kubectl apply -f "${DEPLOY_DIR}/crds/" 2>/dev/null || \
        warn "CRD apply had warnings (expected if CRDs already exist)"
    log "  ✓ CRDs applied"
}

prepare_workflow_registry() {
    log "Preparing workflow registry directory …"

    local host="${SERVER_HOST}"
    local uid gid
    uid="$(_remote_user_id "$host")"
    gid="$(_remote_group_id "$host")"

    # The operator sees this path through a k3s-in-Docker hostPath mount. If
    # the directory is first created from inside that container stack, ownership
    # can appear as nobody:nogroup on the deploy host. Keep it host-created and
    # writable by the user who generates workflow registry entries locally.
    _run "$host" "mkdir -p '${PROJECT_ROOT}/data/workflow_registry'"
    _run "$host" "sudo chown -R '${uid}:${gid}' '${PROJECT_ROOT}/data/workflow_registry'"
    _run "$host" "chmod u+rwx '${PROJECT_ROOT}/data' '${PROJECT_ROOT}/data/workflow_registry'"

    log "  ✓ ${PROJECT_ROOT}/data/workflow_registry owner=${uid}:${gid}"
}

deploy_qonductor_controllers() {
    if [[ "$SKIP_CONTROLLERS" == "1" ]]; then
        warn "SKIP_CONTROLLERS=1 — skipping operator/device-plugin deployment."
        return 0
    fi

    log "Deploying Qonductor operator and device plugin …"
    kubectl delete configmap -n default \
        -l app=qonductor,component=qpu-profile \
        --ignore-not-found=true 2>/dev/null || true

    # Compute operator resources from the k3s server container config:
    # requests = half of server resources; limits = all server resources.
    local server_cpus="${SERVER_CPUS:-2}"
    local server_memory="${SERVER_MEMORY:-4g}"
    local resource_assignments
    resource_assignments="$(
        python3 - "$server_cpus" "$server_memory" <<'PY'
from decimal import Decimal, InvalidOperation
import re
import sys


def parse_cpu(value: str) -> Decimal:
    raw = str(value).strip()
    if raw.endswith("m"):
        return Decimal(raw[:-1]) / Decimal(1000)
    return Decimal(raw)


def render_cpu(value: Decimal) -> str:
    if value == value.to_integral_value():
        return str(int(value))
    milli = (value * Decimal(1000)).to_integral_value()
    return f"{int(milli)}m"


def split_memory(value: str) -> tuple[Decimal, str]:
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)\s*", str(value))
    if not match:
        raise ValueError(f"unsupported memory quantity: {value!r}")
    number = Decimal(match.group(1))
    unit = match.group(2) or "g"
    return number, unit


def k8s_memory_unit(unit: str) -> str:
    normalized = unit.strip()
    lower = normalized.lower()
    if lower in ("g", "gb", "gi", "gib"):
        return "Gi"
    if lower in ("m", "mb", "mi", "mib"):
        return "Mi"
    if lower in ("k", "kb", "ki", "kib"):
        return "Ki"
    return normalized or "Gi"


def render_memory(number: Decimal, unit: str) -> str:
    unit = k8s_memory_unit(unit)
    if number == number.to_integral_value():
        return f"{int(number)}{unit}"

    # Kubernetes accepts fixed-point quantities, but emitting Mi for fractional
    # Gi keeps the generated manifest easy to read and avoids parser surprises.
    if unit == "Gi":
        mib = (number * Decimal(1024)).to_integral_value()
        return f"{int(mib)}Mi"
    if unit == "Mi":
        kib = (number * Decimal(1024)).to_integral_value()
        return f"{int(kib)}Ki"
    return f"{number.normalize()}{unit}"


try:
    cpu = parse_cpu(sys.argv[1])
    mem_num, mem_unit = split_memory(sys.argv[2])
except (InvalidOperation, ValueError) as exc:
    raise SystemExit(str(exc))

print(f"op_cpu_req={render_cpu(cpu / Decimal(2))}")
print(f"op_mem_req={render_memory(mem_num / Decimal(2), mem_unit)}")
print(f"op_cpu_limit={render_cpu(cpu)}")
print(f"op_mem_limit={render_memory(mem_num, mem_unit)}")
PY
    )" || die "Failed to compute operator resources from server.cpus=${server_cpus}, server.memory=${server_memory}"
    eval "$resource_assignments"
    log "  Operator resources: requests=${op_cpu_req} CPU/${op_mem_req}, limits=${op_cpu_limit} CPU/${op_mem_limit}"

    local rendered_operator
    rendered_operator="$(mktemp /tmp/qonductor-operator-deployment.XXXXXX.yaml)"
    sed -e "s|__QONDUCTOR_DATA_PATH__|${PROJECT_ROOT}/data|g" \
        -e "s|__OPERATOR_CPU_REQUEST__|${op_cpu_req}|g" \
        -e "s|__OPERATOR_MEMORY_REQUEST__|${op_mem_req}|g" \
        -e "s|__OPERATOR_CPU_LIMIT__|${op_cpu_limit}|g" \
        -e "s|__OPERATOR_MEMORY_LIMIT__|${op_mem_limit}|g" \
        "${DEPLOY_DIR}/operator/deployment.yaml" > "$rendered_operator"

    kubectl apply -f "${DEPLOY_DIR}/operator/rbac.yaml"
    kubectl apply -f "${DEPLOY_DIR}/operator/configmap.yaml"
    kubectl apply -f "$rendered_operator"
    rm -f "$rendered_operator"
    kubectl apply -f "${DEPLOY_DIR}/device-plugin/daemonset.yaml"

    log "Waiting for Qonductor operator rollout …"
    kubectl rollout status deployment/qonductor-operator \
        -n default --timeout=180s || die "Operator rollout failed — check 'kubectl describe deployment qonductor-operator'"

    log "Waiting for QPU device-plugin rollout …"
    kubectl rollout status daemonset/qonductor-qpu-device-plugin \
        -n default --timeout=300s || die "Device-plugin rollout failed — check 'kubectl describe daemonset qonductor-qpu-device-plugin'"

    log "  ✓ Qonductor controllers deployed"
}

# ==============================================================================
# Phase 7: Summary
# ==============================================================================

print_summary() {
    local cluster_name="${CLUSTER_NAME}"
    local server_host="${SERVER_HOST}"

    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Qonductor multi-host cluster '${cluster_name}' is ready!${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  API Server:  https://${server_host}:6443"
    echo "  Kubeconfig:  ~/.kube/config"
    echo ""
    echo "  Nodes:"
    kubectl get nodes -o custom-columns=\
NAME:.metadata.name,\
STATUS:.status.conditions[?\(@.type==\"Ready\"\)].status,\
TYPE:metadata.labels.qonductor\\.io/node-type 2>/dev/null || kubectl get nodes
    echo ""
    echo "  Quick start:"
    echo "    kubectl get pods -n default"
    echo "    kubectl apply -f deploy/examples/qaoa-12-dynamic-workflow.yaml"
    echo "    kubectl get hybridworkflows -n default -w"
    echo ""
    echo "  Rebuild & redeploy:"
    echo "    bash docker/multi-host/deploy-cluster.sh"
    echo ""
    echo "  Teardown:"
    echo "    bash docker/multi-host/teardown-cluster.sh"
    echo ""

    # Show container status on each host.
    echo "  Container status:"
    for host in $(_all_hosts | sort -u); do
        local container; container="$(_container_for_host "$host")"
        echo "    ${host}:"
        _remote "$host" "docker ps --format '      {{.Names}}  {{.Status}}' --filter name=${container}" 2>/dev/null || true
    done
    echo ""
}

# ==============================================================================
# Main
# ==============================================================================

main() {
    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Qonductor Multi-Host k3s Cluster Deployment${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"

    # ---- Phase 0: Pre-flight ------------------------------------------------
    _banner "Phase 0: Pre-flight"
    check_deps
    load_cluster_config "$CLUSTER_CONFIG"
    validate_config
    show_firewall_hints

    log "Checking Docker on remote hosts …"
    check_remote_docker "${SERVER_HOST}"
    for ((i = 0; i < AGENTS_COUNT; i++)); do
        local host_var="AGENTS_${i}_HOST"
        check_remote_docker "${!host_var}"
    done

    # ---- Phase 1: Teardown --------------------------------------------------
    teardown_existing

    # ---- Phase 2: Prepare images --------------------------------------------
    prepare_images

    # ---- Phase 3: Distribute images -----------------------------------------
    distribute_images

    # ---- Phase 4: Deploy k3s ------------------------------------------------
    _banner "Phase 4: Deploy k3s cluster"
    deploy_server
    wait_for_server
    fetch_kubeconfig
    deploy_agents
    wait_for_nodes

    # ---- Phase 5: Containerd import -----------------------------------------
    import_containerd

    # ---- Phase 6: Configure -------------------------------------------------
    _banner "Phase 6: Configure cluster"
    sync_qpu_profiles
    label_nodes
    deploy_qonductor_crds
    prepare_workflow_registry
    deploy_qonductor_controllers

    # ---- Phase 7: Summary --------------------------------------------------
    print_summary
}

main "$@"
