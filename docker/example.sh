#!/bin/bash
# =============================================================================
# qslurm Multi-Host Cluster Deployment Script
#
# Deploys a qslurm cluster across multiple hosts using:
#   - Weave Net for overlay networking (cross-host container communication)
#   - Docker bridge network via weave plugin + independent containers (docker run)
#   - SSH for remote host orchestration
#
# Usage:
#   bash deploy.sh                 # Deploy cluster
#   bash deploy.sh --destroy       # Tear down cluster
#   bash deploy.sh --skip-weave    # Skip Weave setup
#   bash deploy.sh --skip-image    # Skip image distribution
#   bash deploy.sh --force-image   # Reload image tarball even if tag exists
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/cluster.yml"

# ---- defaults (override in cluster.yml) ----
IMAGE_NAME="${IMAGE_NAME:-qslurm:latest}"
SLURM_CONF_HOST_DIR="${SLURM_CONF_HOST_DIR:-/etc/qslurm}"
QSLURM_SHARE_HOST_DIR="${QSLURM_SHARE_HOST_DIR:-}"

# Weave Net defaults (ports default to 6783/6784 — change if occupied)
WEAVE_VERSION="${WEAVE_VERSION:-2.8.1}"
WEAVE_IP_RANGE="${WEAVE_IP_RANGE:-10.32.0.0/12}"
WEAVE_PORT="${WEAVE_PORT:-6783}"
WEAVE_DATAPATH_PORT="${WEAVE_DATAPATH_PORT:-6784}"
WEAVE_HTTP_PORT="${WEAVE_HTTP_PORT:-6784}"

# Docker Hub user/organisation for Weave images
DOCKERHUB_USER="${DOCKERHUB_USER:-weaveworks}"

# Weave image names.
# Only three images are needed for the direct-docker launch path:
#   weave      — the router (creates the overlay network, DNS, IPAM)
#   weaveexec  — utility image (attach, expose, etc. invoked via docker exec)
#   weavedb    — data-only container backing the weave database
# The Docker network plugin is registered by the router itself (it listens
# on /run/docker/plugins/weavemesh.sock); no separate plugin image is needed.
WEAVE_IMAGE="${DOCKERHUB_USER}/weave:${WEAVE_VERSION}"
WEAVEEXEC_IMAGE="${DOCKERHUB_USER}/weaveexec:${WEAVE_VERSION}"
WEAVEDB_IMAGE="${DOCKERHUB_USER}/weavedb:latest"
# Space-separated list used for tarball instructions
WEAVE_IMAGE_LIST="${WEAVE_IMAGE} ${WEAVEEXEC_IMAGE} ${WEAVEDB_IMAGE}"

SKIP_WEAVE="${SKIP_WEAVE:-false}"
SKIP_IMAGE="${SKIP_IMAGE:-false}"
SKIP_WEAVE_IMAGE="${SKIP_WEAVE_IMAGE:-false}"
FORCE_IMAGE="${FORCE_IMAGE:-false}"
DESTROY_MODE=false
REMOTE_SUDO="${REMOTE_SUDO:-false}"

# ---- python3 YAML parser (embedded, no pyyaml needed) ----
PY_YAML_PARSER=$(cat <<'PY_YAML'
import sys, re

def die(msg):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(1)

def strip_inline_comment(line):
    """Remove inline comment respecting quoted strings. Returns cleaned line."""
    in_single = False
    in_double = False
    for i, ch in enumerate(line):
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == '#' and not in_single and not in_double:
            return line[:i].rstrip()
    return line

def parse_yaml(path):
    with open(path) as f:
        lines = f.readlines()

    raw = []
    for line in lines:
        stripped = line.rstrip()
        if not stripped:
            continue
        # Strip inline comments (respects quoted strings)
        cleaned = strip_inline_comment(stripped)
        if not cleaned or cleaned.lstrip().startswith("#"):
            continue
        raw.append(cleaned)

    result = {}
    hosts = []
    section = "root"

    for line in raw:
        content = line.lstrip()
        indent = len(line) - len(content)

        if not content:
            continue

        # List item: "- key: value"
        if content.startswith("- "):
            content = content[2:]
            indent += 2
            current_host = {}
            hosts.append(current_host)
            section = "host_item"

        m = re.match(r"^(\w[\w_]*)\s*:\s*(.*?)\s*$", content)
        if not m:
            continue

        key = m.group(1)
        val = m.group(2)

        # Strip optional surrounding quotes
        if val and len(val) >= 2:
            if (val[0] == '"' and val[-1] == '"') or (val[0] == "'" and val[-1] == "'"):
                val = val[1:-1]

        if indent == 0:
            if val:
                result[key] = val
            else:
                section = key
        elif section == "host_item":
            current_host[key] = val
        else:
            result[f"{section}_{key}"] = val

    result["_hosts"] = hosts
    return result

def emit_bash(cfg):
    out = []
    def emit(k, v):
        if isinstance(v, bool):
            v = str(v).lower()
        out.append(f"{k}={v!r}")

    emit("SSH_USER",          cfg.get("ssh_user", "root"))
    emit("REMOTE_SUDO",       cfg.get("ssh_remote_sudo", "false"))
    emit("CONTROLLER_HOST",   cfg.get("controller_host", ""))
    emit("CONTROLLER_CPU",    cfg.get("controller_cpus", ""))
    emit("QOSD_QPU_COUNT",    cfg.get("quantum_qpu_count", "3"))
    emit("QOSD_QPU_CONFIG_DIR", cfg.get("quantum_qpu_config_dir", "/mnt/qslurm_share/qasm_tools/configs/qpus"))
    emit("QOSD_QPU_CONFIG_SEQUENCE", cfg.get("quantum_qpu_config_sequence", ""))
    emit("QOSD_TRANSPORT_MS", cfg.get("quantum_transport_ms", "20"))
    emit("QOS_SIMULATOR",     cfg.get("quantum_simulator", "hardware"))
    emit("QOS_PACKING_ENABLED", cfg.get("qos_packing_enabled", "1"))
    emit("QOS_SIM_PRELOAD_ENABLED", cfg.get("qos_preload_enabled", "1"))
    emit("QOS_CONTEXT_REUSE_ENABLED", cfg.get("qos_context_reuse_enabled", "1"))
    emit("QOSD_ENTRYPOINT", cfg.get("qos_entrypoint", "qosd.py"))
    emit("QOS_RUNTIME_MODE", cfg.get("qos_runtime_mode", "fde"))
    emit("QOS_HARDWARE_RESULT_FALLBACK", cfg.get("qos_hardware_result_fallback", "error"))
    emit("QUANTUM_MAX_LOAD", cfg.get("slurm_quantum_max_load", "1.5"))
    emit("QUANTUM_OVERLOAD_MAX_CIRCUIT_DURATION", cfg.get("slurm_quantum_overload_max_circuit_duration", "0.00001"))
    emit("IMAGE_NAME",        cfg.get("image", "qslurm:latest"))
    emit("QSLURM_SHARE_HOST_DIR", cfg.get("share_host_dir", ""))

    # Weave configuration (weave: section)
    emit("WEAVE_IP_RANGE",        cfg.get("weave_ip_range", "10.32.0.0/12"))
    emit("WEAVE_PORT",            cfg.get("weave_port", "6783"))
    emit("WEAVE_DATAPATH_PORT",   cfg.get("weave_datapath_port", "6784"))
    emit("WEAVE_VERSION",         cfg.get("weave_version", "2.8.1"))

    # Build HOSTS array
    host_entries = []
    for h in cfg.get("_hosts", []):
        ip     = h.get("ip", "")
        cn     = h.get("classic_nodes", "0")
        ccpu   = h.get("classic_cpus", "0")
        qn     = h.get("quantum_nodes", "0")
        qcpu   = h.get("quantum_cpus", "0")
        qq     = h.get("quantum_qubits", "0")
        host_entries.append(f"{ip}:{cn}:{ccpu}:{qn}:{qcpu}:{qq}")

    hosts_str = " ".join(repr(e) for e in host_entries)
    out.append(f"HOSTS=({hosts_str})")

    print("\n".join(out))

if __name__ == "__main__":
    cfg = parse_yaml(sys.argv[1])
    emit_bash(cfg)
PY_YAML
)

# ---- colors ----
C_GREEN='\033[0;32m'
C_YELLOW='\033[1;33m'
C_RED='\033[0;31m'
C_NC='\033[0m'

info()  { echo -e "${C_GREEN}[INFO]${C_NC}  $*"; }
warn()  { echo -e "${C_YELLOW}[WARN]${C_NC}  $*"; }
err()   { echo -e "${C_RED}[ERROR]${C_NC} $*"; }

usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Options:
  -c, --config FILE       Path to cluster config (default: cluster.yml)
  --skip-weave            Skip Weave Net initialization
  --skip-weave-image      Skip Weave Docker image distribution to hosts
  --skip-image            Skip Docker image distribution to hosts
  --force-image           Force qslurm image reload on all hosts
  --destroy               Tear down the entire cluster (stop containers, leave Weave)
  -h, --help              Show this help
EOF
    exit 0
}

# ==================== config parsing ====================

load_config() {
    if [ ! -f "$CONFIG_FILE" ]; then
        err "Config file not found: $CONFIG_FILE"
        exit 1
    fi

    # Parse YAML via embedded python3 parser (no pyyaml needed)
    if ! command -v python3 >/dev/null 2>&1; then
        err "python3 is required but not installed"
        exit 1
    fi

    local parsed
    parsed=$(python3 -c "$PY_YAML_PARSER" "$CONFIG_FILE") || {
        err "Failed to parse ${CONFIG_FILE}"
        exit 1
    }

    eval "$parsed"

    case "${QOS_PACKING_ENABLED:-1}" in
        true|True|TRUE|yes|Yes|YES|on|On|ON) QOS_PACKING_ENABLED=1 ;;
        false|False|FALSE|no|No|NO|off|Off|OFF) QOS_PACKING_ENABLED=0 ;;
    esac
    case "${QOS_SIM_PRELOAD_ENABLED:-1}" in
        true|True|TRUE|yes|Yes|YES|on|On|ON) QOS_SIM_PRELOAD_ENABLED=1 ;;
        false|False|FALSE|no|No|NO|off|Off|OFF) QOS_SIM_PRELOAD_ENABLED=0 ;;
    esac
    case "${QOS_CONTEXT_REUSE_ENABLED:-1}" in
        true|True|TRUE|yes|Yes|YES|on|On|ON) QOS_CONTEXT_REUSE_ENABLED=1 ;;
        false|False|FALSE|no|No|NO|off|Off|OFF) QOS_CONTEXT_REUSE_ENABLED=0 ;;
    esac

    # Apply defaults and validate
    SSH_USER="${SSH_USER:-root}"
    REMOTE_SUDO="${REMOTE_SUDO:-false}"
    local missing=""
    [ -z "${CONTROLLER_HOST:-}" ]    && missing="$missing CONTROLLER_HOST"
    [ -z "${CONTROLLER_CPU:-}" ]     && missing="$missing CONTROLLER_CPU"
    if ! declare -p HOSTS >/dev/null 2>&1 || [ "${#HOSTS[@]}" -eq 0 ]; then
        missing="$missing HOSTS"
    fi

    if [ -n "$missing" ]; then
        err "Missing required config variable(s):$missing"
        exit 1
    fi

    info "Configuration loaded"
    info "  Controller: ${CONTROLLER_HOST} (${CONTROLLER_CPU} CPUs)"
    info "  SSH user: ${SSH_USER}"
    info "  Remote sudo: ${REMOTE_SUDO}"
    info "  Image: ${IMAGE_NAME}"
    if [ -n "${QSLURM_SHARE_HOST_DIR:-}" ]; then
        info "  Share host dir: ${QSLURM_SHARE_HOST_DIR}"
    fi
    info "  Weave IP range: ${WEAVE_IP_RANGE}"
    info "  Weave port: ${WEAVE_PORT} (data: ${WEAVE_DATAPATH_PORT})"
    info "  Hosts:"
    for entry in "${HOSTS[@]}"; do
        local ip cn ccpu qn qcpu qq
        IFS=':' read -r ip cn ccpu qn qcpu qq <<< "$entry"
        local parts=()
        [ "${cn:-0}" -gt 0 ] && parts+=("${cn} classic (${ccpu:-?} CPU)")
        [ "${qn:-0}" -gt 0 ] && parts+=("${qn} quantum (${qcpu:-?} CPU, ${qq:-?} qubits)")
        info "    ${ip} -> ${parts[*]:-(none)}"
    done
}

# ==================== SSH helpers ====================

ssh_cmd() {
    local host="$1"; shift
    local cmd="$*"
    if [ "${REMOTE_SUDO}" = "true" ]; then
        local quoted_cmd
        printf -v quoted_cmd '%q' "$cmd"
        ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 -o BatchMode=yes \
            "${SSH_USER}@${host}" "sudo bash -lc ${quoted_cmd}"
    else
        ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 -o BatchMode=yes \
            "${SSH_USER}@${host}" "$cmd"
    fi
}

ssh_check() {
    local host="$1"
    if ssh_cmd "$host" "echo ok" >/dev/null 2>&1; then
        return 0
    else
        err "SSH check failed for ${host} (user: ${SSH_USER})"
        err "Make sure SSH key is configured: ssh-copy-id ${SSH_USER}@${host}"
        return 1
    fi
}

docker_check() {
    local host="$1"
    if ssh_cmd "$host" "docker info --format '{{.ServerVersion}}'" >/dev/null 2>&1; then
        return 0
    else
        err "Docker not accessible on ${host} (user: ${SSH_USER})"
        if [ "${REMOTE_SUDO}" = "true" ]; then
            err "Ensure Docker is installed and ${SSH_USER} can run sudo without interaction"
        else
            err "Ensure Docker is installed and ${SSH_USER} is in the docker group"
        fi
        return 1
    fi
}

# Build deduplicated list of all hosts into global HOST_LIST array.
# Must be called once before each usage — callers iterate with:
#   get_hosts; for host in "${HOST_LIST[@]}"; do ...; done
get_hosts() {
    HOST_LIST=()
    local -A seen
    for entry in "${HOSTS[@]}"; do
        local ip
        IFS=':' read -r ip _ <<< "$entry"
        if [ -z "${seen[$ip]:-}" ]; then
            seen[$ip]=1
            HOST_LIST+=("$ip")
        fi
    done
}

# ==================== prerequisite checks ====================

check_prereqs() {
    info "=== Checking prerequisites ==="

    # Controller
    ssh_check "$CONTROLLER_HOST" || exit 1
    docker_check "$CONTROLLER_HOST" || exit 1
    info "  Controller ${CONTROLLER_HOST}: OK"

    # Image on controller — auto-load from tarball if missing or forced
    if [ "$FORCE_IMAGE" = "true" ] || ! ssh_cmd "$CONTROLLER_HOST" "docker image inspect ${IMAGE_NAME} >/dev/null 2>&1"; then
        if [ "$FORCE_IMAGE" = "true" ]; then
            warn "Force reloading image '${IMAGE_NAME}' on controller ${CONTROLLER_HOST}"
        else
            warn "Image '${IMAGE_NAME}' not found on controller ${CONTROLLER_HOST}"
        fi
        local local_tarball="${SCRIPT_DIR}/qslurm_latest.tar.gz"
        if [ ! -f "${local_tarball}" ]; then
            err "  Local tarball not found: ${local_tarball}"
            err "  Build it first:  cd docker_build && bash build_image.sh"
            err "  Or copy a pre-built tarball to: ${local_tarball}"
            exit 1
        fi
        # Transfer and load the tarball onto the controller
        info "  Transferring ${local_tarball} to controller... ($(du -h "${local_tarball}" | awk '{print $1}'))"
        scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
            "${local_tarball}" "${SSH_USER}@${CONTROLLER_HOST}:/tmp/qslurm_latest.tar.gz" || {
            err "  Failed to transfer image tarball to ${CONTROLLER_HOST}"
            exit 1
        }
        info "  Loading image on controller..."
        ssh_cmd "$CONTROLLER_HOST" "bash -c 'set -o pipefail; gunzip -c /tmp/qslurm_latest.tar.gz | docker load'" || {
            err "  Failed to load image on controller"
            ssh_cmd "$CONTROLLER_HOST" "rm -f /tmp/qslurm_latest.tar.gz" || true
            exit 1
        }
        ssh_cmd "$CONTROLLER_HOST" "rm -f /tmp/qslurm_latest.tar.gz" || true
        # Verify the image is now present
        if ssh_cmd "$CONTROLLER_HOST" "docker image inspect ${IMAGE_NAME} >/dev/null 2>&1"; then
            info "  Image loaded on controller successfully"
        else
            err "  Image still not found after loading — check disk space on ${CONTROLLER_HOST}"
            exit 1
        fi
    else
        info "  Image ${IMAGE_NAME} on controller: OK"
    fi

    # All other hosts
    get_hosts
    for host in "${HOST_LIST[@]}"; do
        ssh_check "$host" || exit 1
        docker_check "$host" || exit 1
        info "  Host ${host}: OK"
    done
}

# ==================== Weave Net image distribution ====================

# Acquire weave images on ONE host with internet access, save them to a
# tarball, then distribute that tarball to all other hosts.
# Precedence:
#   1. Existing tarball in SCRIPT_DIR → distribute directly
#   2. Local docker can pull → pull locally, docker save, then distribute
#   3. Neither works → error with instructions
distribute_weave_images() {
    if [ "$SKIP_WEAVE_IMAGE" = "true" ]; then
        info "=== Skipping Weave image distribution (--skip-weave-image) ==="
        return 0
    fi

    info "=== Distributing Weave Net Docker images ==="

    local weave_tarball="${SCRIPT_DIR}/weave_images_${WEAVE_VERSION}.tar.gz"

    # ---- Stage 1: obtain the tarball ----
    if [ -f "${weave_tarball}" ]; then
        info "  Using existing tarball: ${weave_tarball} ($(du -h "${weave_tarball}" | awk '{print $1}'))"
    else
        # No tarball yet — try to create one by pulling on a single host
        # that might have internet access.
        info "  No weave image tarball found — trying to acquire images once on a single host..."

        local pull_host=""
        local pull_via_ssh=false

        # Option A: local machine has docker and can pull
        if command -v docker >/dev/null 2>&1 && docker info --format '{{.ServerVersion}}' >/dev/null 2>&1; then
            info "  Trying to pull weave images locally..."
            local all_ok=true
            for img in ${WEAVE_IMAGE_LIST}; do
                if docker image inspect "$img" >/dev/null 2>&1; then
                    continue  # already present
                fi
                if ! docker pull "$img" 2>&1; then
                    all_ok=false
                    break
                fi
            done
            if $all_ok; then
                pull_host="localhost"
                info "  Local pull succeeded"
            else
                info "  Local pull failed (no internet?)"
            fi
        fi

        # Option B: try the controller
        if [ -z "$pull_host" ]; then
            info "  Trying to pull on controller ${CONTROLLER_HOST}..."
            if weave_ensure_images "$CONTROLLER_HOST"; then
                pull_host="$CONTROLLER_HOST"
                pull_via_ssh=true
                info "  Controller pull succeeded"
            else
                info "  Controller pull failed (no internet?)"
            fi
        fi

        # Neither worked
        if [ -z "$pull_host" ]; then
            err "  Could not pull weave images on any host (local or controller)."
            err ""
            err "  Prepare the tarball on an internet-connected machine (one-time step):"
            err ""
            err "    docker pull ${WEAVE_IMAGE}"
            err "    docker pull ${WEAVEEXEC_IMAGE}"
            err "    docker pull ${WEAVEDB_IMAGE}"
            err "    docker save ${WEAVE_IMAGE_LIST} | gzip > weave_images_${WEAVE_VERSION}.tar.gz"
            err ""
            err "  Then copy weave_images_${WEAVE_VERSION}.tar.gz to:"
            err "    ${SCRIPT_DIR}/"
            err "  and re-run:  bash deploy.sh"
            exit 1
        fi

        # Save the pulled images into a tarball
        info "  Images pulled on ${pull_host} — saving to tarball..."
        if [ "$pull_host" = "localhost" ]; then
            docker save ${WEAVE_IMAGE_LIST} | gzip > "${weave_tarball}" || {
                err "  Failed to save weave images to tarball locally"
                exit 1
            }
        else
            ssh_cmd "$pull_host" "docker save ${WEAVE_IMAGE_LIST} | gzip > /tmp/weave_images.tar.gz" || {
                err "  Failed to save weave images on ${pull_host}"
                exit 1
            }
            scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
                "${SSH_USER}@${pull_host}:/tmp/weave_images.tar.gz" "${weave_tarball}" || {
                err "  Failed to download tarball from ${pull_host}"
                exit 1
            }
            ssh_cmd "$pull_host" "rm -f /tmp/weave_images.tar.gz" || true
        fi
        info "  Tarball saved: ${weave_tarball} ($(du -h "${weave_tarball}" | awk '{print $1}'))"
    fi

    # ---- Stage 2: distribute tarball to all hosts ----
    local all_hosts=()
    all_hosts+=("$CONTROLLER_HOST")
    get_hosts
    for host in "${HOST_LIST[@]}"; do
        all_hosts+=("$host")
    done

    local weave_images_list=(${WEAVE_IMAGE_LIST})
    local -A sent

    for host in "${all_hosts[@]}"; do
        [ -n "${sent[$host]:-}" ] && continue
        sent[$host]=1

        # Check if all required images are already present
        if weave_verify_images "$host" 2>/dev/null; then
            info "  ${host}: weave images already present"
            continue
        fi

        # The pull host already has them — skip
        if [ "${pull_host:-}" = "$host" ]; then
            info "  ${host}: images pulled above — skip transfer"
            continue
        fi

        info "  ${host}: transferring weave images..."
        scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
            "${weave_tarball}" "${SSH_USER}@${host}:/tmp/weave_images.tar.gz" || {
            err "  ${host}: scp failed — check network and SSH key"
            exit 1
        }

        ssh_cmd "$host" "bash -c 'set -o pipefail; gunzip -c /tmp/weave_images.tar.gz | docker load'" || {
            err "  ${host}: docker load (weave images) failed"
            ssh_cmd "$host" "rm -f /tmp/weave_images.tar.gz" || true
            exit 1
        }

        ssh_cmd "$host" "rm -f /tmp/weave_images.tar.gz" || true
        info "  ${host}: weave images loaded"
    done
}

# ==================== Weave Net setup ====================

# ==================== direct-docker weave helpers ====================
#
# The official "weave" script bundles an old Docker client inside its weaveexec
# image (API v1.18) that is incompatible with modern Docker daemons requiring
# API v1.40+.  Instead of using "weave launch", we manage the weave containers
# directly with the host's docker binary.

# Pull weave images on a host (idempotent — skips images already present).
# Returns 0 if all required images are present after the call, 1 otherwise.
weave_ensure_images() {
    local host="$1"
    local images=("${WEAVE_IMAGE}" "${WEAVEEXEC_IMAGE}" "${WEAVEDB_IMAGE}")
    local missing=()
    local pull_failed=false

    # First pass: check which images are missing
    for img in "${images[@]}"; do
        if ! ssh_cmd "$host" "docker image inspect ${img} >/dev/null 2>&1"; then
            missing+=("$img")
        fi
    done

    if [ ${#missing[@]} -eq 0 ]; then
        return 0
    fi

    # Try to pull missing images from Docker Hub
    info "  ${host}: pulling ${#missing[@]} missing weave image(s)..."
    for img in "${missing[@]}"; do
        info "  ${host}: pulling ${img}..."
        if ssh_cmd "$host" "docker pull ${img} 2>&1"; then
            continue
        fi
        pull_failed=true
        break
    done

    if $pull_failed; then
        return 1
    fi
    return 0
}

# Verify weave images exist on a host (no pulling — check only).
# Returns 0 if all images are present, 1 otherwise with the missing list.
weave_verify_images() {
    local host="$1"
    local images=("${WEAVE_IMAGE}" "${WEAVEEXEC_IMAGE}" "${WEAVEDB_IMAGE}")
    local missing=()

    for img in "${images[@]}"; do
        if ! ssh_cmd "$host" "docker image inspect ${img} >/dev/null 2>&1"; then
            missing+=("$img")
        fi
    done

    if [ ${#missing[@]} -eq 0 ]; then
        return 0
    fi

    err "  ${host}: missing weave images: ${missing[*]}"
    return 1
}

# Get the docker bridge gateway IP (used for weave DNS listener).
weave_get_bridge_ip() {
    local host="$1"
    ssh_cmd "$host" \
        "docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null" \
        | tr -d '\r\n' || echo "172.17.0.1"
}

# Launch the weave router on a single host via direct docker commands.
# Args: <host> [peer_ip]
weave_launch_on_host() {
    local host="$1"
    local peer="${2:-}"  # optional peer to connect to after start

    # Check if weave router is already running AND healthy
    if ssh_cmd "$host" "docker ps --filter name=^weave\$ --format '{{.Names}}' 2>/dev/null" | grep -q 'weave'; then
        # Verify it's actually working (not crash-looping)
        if ssh_cmd "$host" "curl -sf -m 3 http://127.0.0.1:6784/status 2>/dev/null" | grep -q '.'; then
            info "  ${host}: weave router already running and healthy"
            return 0
        fi
        warn "  ${host}: weave container exists but is unhealthy — recreating..."
    fi

    # Clean up any stale weave state from a previous run. Removing only the
    # containers is not enough; leftover host bridges/datapaths can make weave
    # fail with "Existing bridge type ... different than requested ...".
    weave_stop_on_host "$host"

    # Resolve resolv.conf path (handle symlinked /etc/resolv.conf)
    local resolv_dir
    resolv_dir=$(ssh_cmd "$host" \
        "if [ -L /etc/resolv.conf ]; then dirname \$(readlink -f /etc/resolv.conf); else echo /etc; fi" \
        2>/dev/null | tr -d '\r\n')
    resolv_dir="${resolv_dir:-/etc}"

    local bridge_ip
    bridge_ip=$(weave_get_bridge_ip "$host")
    bridge_ip="${bridge_ip:-172.17.0.1}"

    info "  ${host}: creating weavedb data container..."
    ssh_cmd "$host" "docker create --name weavedb \
        -v /weavedb \
        --label io.weave.volumes=weavevolumes \
        ${WEAVEDB_IMAGE} true" || {
        err "  ${host}: failed to create weavedb container"
        return 1
    }

    info "  ${host}: launching weave router..."
    local launch_cmd="docker run -d \
        --name weave \
        --restart always \
        --network host \
        --pid host \
        --privileged \
        --volumes-from weavedb \
        -v /var/run/weave:/var/run/weave \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -v /run/docker/plugins:/run/docker/plugins \
        -v /etc:/host/etc \
        -v /var/lib/dbus:/host/var/lib/dbus \
        -v ${resolv_dir}:/var/run/weave/etc \
        -v /proc:/host/proc \
        -e DOCKER_BRIDGE=docker0 \
        -e DOCKER_HOST=unix:///var/run/docker.sock \
        -e EXEC_IMAGE=${WEAVEEXEC_IMAGE} \
        -e WEAVE_DATAPATH_PORT=${WEAVE_DATAPATH_PORT} \
        ${WEAVE_IMAGE} \
        --port ${WEAVE_PORT} \
        --host-root=/host \
        --nickname \$(hostname) \
        --ipalloc-range ${WEAVE_IP_RANGE} \
        --dns-listen-address ${bridge_ip}:53 \
        --http-addr 127.0.0.1:6784 \
        --status-addr 127.0.0.1:6782 \
        --resolv-conf '/var/run/weave/etc/resolv.conf' \
        --no-default-ipalloc \
        --datapath datapath \
        --plugin \
        --plugin"

    ssh_cmd "$host" "${launch_cmd}" || {
        err "  ${host}: failed to launch weave router"
        ssh_cmd "$host" "docker logs weave 2>&1 | tail -20" || true
        return 1
    }

    # Wait for weave to become ready.
    # Since weave runs in --network host, we can curl the HTTP endpoint
    # directly from the host instead of relying on docker exec.
    info "  ${host}: waiting for weave router to become ready..."
    local ready=false
    for i in $(seq 1 30); do
        if ssh_cmd "$host" "curl -sf -m 3 http://127.0.0.1:6784/status 2>/dev/null" | grep -q '.'; then
            ready=true
            break
        fi
        # Also check if the container has exited (crashed)
        if ! ssh_cmd "$host" "docker ps --filter name=^weave\$ --format '{{.Names}}' 2>/dev/null" | grep -q 'weave'; then
            # Container stopped entirely — don't wait, it's dead
            break
        fi
        sleep 2
    done

    if [ "$ready" != "true" ]; then
        err "  ${host}: weave router failed to become ready within 60s"
        ssh_cmd "$host" "docker logs weave 2>&1 | tail -30" || true
        return 1
    fi

    info "  ${host}: weave router is ready"

    # Connect to peer if specified.
    # The peer address must include the weave port so the router knows
    # which port to contact on the remote host.
    if [ -n "$peer" ]; then
        local peer_addr="${peer}:${WEAVE_PORT}"
        info "  ${host}: connecting to peer ${peer_addr}..."
        local connect_result
        connect_result=$(ssh_cmd "$host" "curl -sf -X POST 'http://127.0.0.1:6784/connect' -d 'peer=${peer_addr}' 2>&1" 2>&1) || {
            warn "  ${host}: connect to ${peer_addr} failed (curl exit $?): ${connect_result}"
        }
    fi
}

# Stop and remove the weave router on a single host.
weave_stop_on_host() {
    local host="$1"
    # Force-remove weave containers
    ssh_cmd "$host" "docker rm -fv weave weavedb 2>/dev/null" || true
    ssh_cmd "$host" "docker rm -f \$(docker ps -a --filter name=^weaveplugin\$ --format '{{.Names}}' 2>/dev/null) 2>/dev/null" || true
    # Remove weave Docker network (releases plugin socket)
    ssh_cmd "$host" "docker network rm weave 2>/dev/null" || true
    # Delete plugin sockets
    ssh_cmd "$host" "rm -f /run/docker/plugins/weave.sock /run/docker/plugins/weavemesh.sock 2>/dev/null" || true
    # Use weaveutil to delete the ODP datapath (ip link del doesn't work on it)
    ssh_cmd "$host" "
        docker run --rm --privileged --pid host --net host \
            -v /var/run/docker.sock:/var/run/docker.sock \
            --entrypoint=/usr/bin/weaveutil \
            ${WEAVEEXEC_IMAGE} delete-datapath datapath 2>/dev/null
    " || true
    # Remove remaining weave/datapath interfaces (veth pairs, regular bridge)
    ssh_cmd "$host" "
        ip link del vxlan-16784 2>/dev/null
        ip link del vethwe-bridge 2>/dev/null
        ip link del vethwe-datapath 2>/dev/null
        ip link del weave 2>/dev/null
        ip link del datapath 2>/dev/null
    " || true
}

setup_weave() {
    if [ "$SKIP_WEAVE" = "true" ]; then
        info "=== Skipping Weave Net setup (--skip-weave) ==="
        return 0
    fi

    info "=== Weave Net setup (direct Docker, no weave script) ==="

    # ---- Acquire & distribute weave Docker images ----
    # This handles everything: tarball → distribute, or pull-once → dist.
    distribute_weave_images

    # Quick sanity check: every host must have the images now.
    info "Verifying weave images on all hosts..."
    local all_hosts=()
    all_hosts+=("$CONTROLLER_HOST")
    get_hosts
    for host in "${HOST_LIST[@]}"; do
        all_hosts+=("$host")
    done

    local -A seen_host
    local verify_failed=false
    for host in "${all_hosts[@]}"; do
        [ -n "${seen_host[$host]:-}" ] && continue
        seen_host[$host]=1
        if ! weave_verify_images "$host" 2>/dev/null; then
            err "  ${host}: missing required weave images — distribution failed"
            verify_failed=true
        fi
    done
    if $verify_failed; then
        err "Cannot proceed — one or more hosts lack the weave images"
        exit 1
    fi
    info "  All hosts have weave images"

    # ---- Launch weave on controller first ----
    info "Launching Weave Net on controller (${CONTROLLER_HOST})..."
    weave_launch_on_host "$CONTROLLER_HOST" "" || {
        err "Failed to launch Weave Net on controller"
        exit 1
    }

    # ---- Launch weave on worker hosts (peer with controller) ----
    get_hosts
    for host in "${HOST_LIST[@]}"; do
        if [ "$host" = "$CONTROLLER_HOST" ]; then
            continue
        fi

        info "Launching Weave Net on ${host} (peer: ${CONTROLLER_HOST})..."
        weave_launch_on_host "$host" "${CONTROLLER_HOST}" || {
            err "Failed to launch Weave Net on ${host}"
            exit 1
        }
    done

    # ---- Wait for full mesh to establish ----
    info "Waiting for Weave Net mesh to converge..."

    local total_hosts
    total_hosts=$( { echo "$CONTROLLER_HOST"; get_hosts; for h in "${HOST_LIST[@]}"; do echo "$h"; done; } | sort -u | wc -l)

    # Retry loop: wait for all peers to show up on the controller
    local mesh_ready=false
    for i in $(seq 1 15); do
        local peer_count
        peer_count=$(ssh_cmd "$CONTROLLER_HOST" \
            "curl -sf http://127.0.0.1:6784/status/peers 2>/dev/null | grep -cE '^[0-9a-f]{2}:' 2>/dev/null" \
            | tr -d '\r\n' || echo "0")
        peer_count=${peer_count:-0}
        info "  Peers visible: ${peer_count}/${total_hosts} (attempt ${i}/15)"
        if [ "${peer_count}" -ge "$((total_hosts - 1))" ]; then
            mesh_ready=true
            break
        fi
        # Retry connect on workers that haven't joined
        get_hosts
        for host in "${HOST_LIST[@]}"; do
            [ "$host" = "$CONTROLLER_HOST" ] && continue
            ssh_cmd "$host" "curl -sf -X POST 'http://127.0.0.1:6784/connect' -d 'peer=${CONTROLLER_HOST}:${WEAVE_PORT}'" 2>/dev/null || true
        done
        sleep 2
    done

    if [ "$mesh_ready" = "true" ]; then
        info "Weave Net mesh fully converged (${total_hosts} hosts, $((total_hosts - 1)) peers)"
    else
        warn "Weave mesh may not be fully converged — continuing anyway"
    fi

    # ---- iptables: allow weave bridge/datapath forwarding ----
    # Docker's default FORWARD policy is often DROP, which blocks all
    # inter-container traffic across bridges (including weave).  Add
    # ACCEPT rules for the weave and datapath bridges so the overlay
    # can forward traffic.
    info "Adding iptables rules for weave forwarding..."
    local -A fwd_done
    for host in "${all_hosts[@]}"; do
        [ -n "${fwd_done[$host]:-}" ] && continue
        fwd_done[$host]=1
        ssh_cmd "$host" "
            iptables -C FORWARD -i weave -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -i weave -j ACCEPT
            iptables -C FORWARD -o weave -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -o weave -j ACCEPT
            iptables -C FORWARD -i datapath -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -i datapath -j ACCEPT
            iptables -C FORWARD -o datapath -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -o datapath -j ACCEPT
        " 2>/dev/null || warn "  ${host}: failed to add some weave iptables rules"
    done
    info "  Weave iptables rules added on all hosts"

    info "Weave Net status:"
    ssh_cmd "$CONTROLLER_HOST" "curl -sf http://127.0.0.1:6784/status 2>/dev/null | head -5" || true
    ssh_cmd "$CONTROLLER_HOST" "curl -sf http://127.0.0.1:6784/status/peers 2>/dev/null" || true

    # Verify weave plugin socket exists
    info "Checking weave Docker plugin socket..."
    ssh_cmd "$CONTROLLER_HOST" "ls -la /run/docker/plugins/ 2>&1" || true

    # Verify weave network is visible to Docker
    info "Checking weave Docker plugin registration..."
    for i in $(seq 1 10); do
        if ssh_cmd "$CONTROLLER_HOST" "docker network ls --filter name=^weave$ --format '{{.Name}}'" 2>/dev/null | grep -q 'weave'; then
            info "Weave network 'weave' is available for Docker containers"
            break
        fi
        info "  Waiting for weave network plugin (attempt ${i}/10)..."
        sleep 3
    done

    # Final check
    if ! ssh_cmd "$CONTROLLER_HOST" "docker network ls --filter name=^weave$ --format '{{.Name}}'" 2>/dev/null | grep -q 'weave'; then
        warn "Weave network not yet visible in 'docker network ls' — will retry at container deployment"
        warn "Trying to manually register weave plugin with Docker..."
        ssh_cmd "$CONTROLLER_HOST" "docker plugin install --grant-all-permissions --alias weave weaveworks/plugin:${WEAVE_VERSION} 2>&1 || docker plugin enable weave 2>&1" || true
    fi

    info "Weave Net setup complete."
}

# ==================== container cleanup ====================

cleanup_qslurm_containers() {
    # Remove old containers before network changes so stale endpoints do not block `weave reset`.
    ssh_cmd "$CONTROLLER_HOST" \
        "docker ps -a --filter name=qslurm- --format '{{.Names}}' | xargs -r docker rm -f" || true

    get_hosts
    for host in "${HOST_LIST[@]}"; do
        ssh_cmd "$host" \
            "docker ps -a --filter name=qslurm- --format '{{.Names}}' | xargs -r docker rm -f" || true
    done
}

# ==================== image distribution ====================

distribute_image() {
    if [ "$SKIP_IMAGE" = "true" ]; then
        info "=== Skipping image distribution (--skip-image) ==="
        return 0
    fi

    info "=== Distributing Docker image ==="

    # Locate the pre-built image tarball
    local local_tarball="${SCRIPT_DIR}/qslurm_latest.tar.gz"
    if [ ! -f "${local_tarball}" ]; then
        err "Local image tarball not found: ${local_tarball}"
        err "Build it first:  cd docker_build && bash build_image.sh"
        exit 1
    fi
    info "  Using local tarball: ${local_tarball} ($(du -h "${local_tarball}" | awk '{print $1}'))"

    get_hosts
    for host in "${HOST_LIST[@]}"; do
        if [ "$FORCE_IMAGE" != "true" ] && ssh_cmd "$host" "docker image inspect ${IMAGE_NAME} >/dev/null 2>&1"; then
            info "  ${host}: image already present"
            continue
        fi

        if [ "$FORCE_IMAGE" = "true" ]; then
            info "  ${host}: force transferring... (this may take a while)"
        else
            info "  ${host}: transferring... (this may take a while)"
        fi

        # scp the pre-built tarball directly from deploy host to worker,
        # then load it.  This avoids the fragile controller→local→worker
        # pipe and does not require controller→worker SSH keys.
        scp -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
            "${local_tarball}" "${SSH_USER}@${host}:/tmp/qslurm_latest.tar.gz" || {
            err "  ${host}: scp failed — check network and SSH key"
            exit 1
        }

        # Use bash -c with pipefail so gunzip failure is not masked by
        # docker load exiting 0 on empty input.
        ssh_cmd "$host" "bash -c 'set -o pipefail; gunzip -c /tmp/qslurm_latest.tar.gz | docker load'" || {
            err "  ${host}: docker load failed"
            ssh_cmd "$host" "rm -f /tmp/qslurm_latest.tar.gz" || true
            exit 1
        }

        ssh_cmd "$host" "rm -f /tmp/qslurm_latest.tar.gz" || true

        # Final verification — make sure the image actually landed
        if ssh_cmd "$host" "docker image inspect ${IMAGE_NAME} >/dev/null 2>&1"; then
            info "  ${host}: done"
        else
            err "  ${host}: image still not found after transfer — check disk space"
            exit 1
        fi
    done
}

# ==================== slurm.conf generation ====================

generate_slurm_conf() {
    info "=== Generating slurm.conf ==="

    # Build global node/partition lists
    local nodes_lines=""
    local cn_list="" qn_list=""
    local cn_idx=0 qn_idx=0
    local first_cn=true first_qn=true

    for entry in "${HOSTS[@]}"; do
        local ip cn_num ccpu qn_num qcpu qq
        IFS=':' read -r ip cn_num ccpu qn_num qcpu qq <<< "$entry"

        for ((i=1; i<=${cn_num:-0}; i++)); do
            cn_idx=$((cn_idx + 1))
            [ "$first_cn" = true ] && first_cn=false || cn_list="${cn_list},"
            cn_list="${cn_list}qslurm-cn${cn_idx}"
            nodes_lines="${nodes_lines}NodeName=qslurm-cn${cn_idx} CPUs=${ccpu} RealMemory=3867 State=UNKNOWN"$'\n'
        done

        for ((i=1; i<=${qn_num:-0}; i++)); do
            qn_idx=$((qn_idx + 1))
            [ "$first_qn" = true ] && first_qn=false || qn_list="${qn_list},"
            qn_list="${qn_list}qslurm-qn${qn_idx}"
            nodes_lines="${nodes_lines}NodeName=qslurm-qn${qn_idx} CPUs=${qcpu} RealMemory=3867 Qubits=${qq} State=UNKNOWN"$'\n'
        done
    done

    # Build partition lines
    local partitions_lines=""
    if [ "$cn_idx" -gt 0 ]; then
        partitions_lines="${partitions_lines}PartitionName=compute Nodes=${cn_list} Default=YES MaxTime=INFINITE State=UP"$'\n'
    fi
    if [ "$qn_idx" -gt 0 ]; then
        partitions_lines="${partitions_lines}PartitionName=q_compute Nodes=${qn_list} MaxTime=INFINITE State=UP"$'\n'
    fi

    local all_list="${cn_list}"
    if [ -n "$cn_list" ] && [ -n "$qn_list" ]; then
        all_list="${cn_list},${qn_list}"
    elif [ -n "$qn_list" ]; then
        all_list="${qn_list}"
    fi
    if [ -n "$all_list" ]; then
        partitions_lines="${partitions_lines}PartitionName=hybrid Nodes=${all_list} OverSubscribe=FORCE:2 MaxTime=INFINITE State=UP"$'\n'
    fi

    info "  Classic nodes: ${cn_idx}"
    info "  Quantum nodes: ${qn_idx}"

    # Write slurm.conf locally first, then distribute
    local local_conf="${SCRIPT_DIR}/conf/slurm-multi.conf"
    local template="${SCRIPT_DIR}/conf/slurm-multi.conf.template"

    if [ -f "$template" ]; then
        # Use template, replacing sentinels
        # ControlAddr uses the container hostname; Weave DNS resolves it cross-host
        awk -v nodes="${nodes_lines}" -v parts="${partitions_lines}" \
            -v qmax="${QUANTUM_MAX_LOAD:-1.5}" \
            -v qover="${QUANTUM_OVERLOAD_MAX_CIRCUIT_DURATION:-0.00001}" \
            -v ctrl_addr="qslurm-ctl" '
            /^ControlAddr=/ { print "ControlAddr=" ctrl_addr; next }
            /^QuantumMaxLoad=/ { print "QuantumMaxLoad=" qmax; next }
            /^QuantumOverloadMaxCircuitDuration=/ { print "QuantumOverloadMaxCircuitDuration=" qover; next }
            /^__NODES__$/   { printf "%s", nodes; next }
            /^__PARTITIONS__$/ { printf "%s", parts; next }
            { print }
        ' "$template" > "$local_conf"
    else
        # Generate entirely from scratch
        cat > "$local_conf" << SLURM_EOF
ClusterName=qslurm-cluster
ControlMachine=qslurm-ctl
ControlAddr=qslurm-ctl
SlurmUser=slurm
SlurmdUser=root
StateSaveLocation=/var/spool/slurm/state
SlurmdSpoolDir=/var/spool/slurmd
SlurmctldPort=6817
SlurmdPort=6818
AuthType=auth/munge
ProctrackType=proctrack/linuxproc
TaskPlugin=task/none
SelectType=select/quantum_hybrid
MailProg=/bin/true
AccountingStorageType=accounting_storage/none
JobCompType=jobcomp/none
SlurmctldDebug=info
SlurmdDebug=info
QuantumPort=6820
Epilog=/mnt/qslurm_share/qasm_tools/qosd_job_epilog.sh
QuantumMaxLoad=${QUANTUM_MAX_LOAD:-1.5}
QuantumOverloadMaxCircuitDuration=${QUANTUM_OVERLOAD_MAX_CIRCUIT_DURATION:-0.00001}
QuantumScoreWeightCap=0.4
QuantumScoreWeightFid=0.4
QuantumScoreWeightQfc=0.2
MinJobAge=3600
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log
${nodes_lines}
${partitions_lines}
SLURM_EOF
    fi

    info "  -> ${local_conf}"
}

# ==================== munge key ====================

setup_munge_key() {
    info "=== Setting up Munge key ==="

    local local_key="${SCRIPT_DIR}/munge-key/munge.key"
    local key_size=0
    if [ -f "$local_key" ]; then
        key_size=$(wc -c < "$local_key" 2>/dev/null || echo 0)
    fi

    if [ ! -f "$local_key" ] || [ "${key_size:-0}" -lt 32 ]; then
        if [ -f "$local_key" ]; then
            warn "Existing munge key is too small (${key_size} bytes); regenerating..."
            rm -f "$local_key"
        else
            info "Generating munge key..."
        fi
        mkdir -p "$(dirname "$local_key")"
        docker run --rm \
            --entrypoint bash "${IMAGE_NAME}" \
            -c 'mungekey -c -f >/dev/null 2>&1 && base64 -w0 /etc/munge/munge.key' \
            | base64 -d > "$local_key"
        chmod 400 "$local_key"
        key_size=$(wc -c < "$local_key" 2>/dev/null || echo 0)
        if [ "${key_size:-0}" -lt 32 ]; then
            err "Generated munge key is invalid (${key_size} bytes)"
            exit 1
        fi
        info "  -> ${local_key}"
    else
        info "Using existing munge key: ${local_key} (${key_size} bytes)"
    fi
}

# ==================== ssh key ====================

setup_ssh_key() {
    info "=== Setting up cluster SSH key ==="

    local local_ssh_dir="${SCRIPT_DIR}/ssh"
    local local_key="${local_ssh_dir}/id_rsa"
    local local_pub="${local_ssh_dir}/id_rsa.pub"

    if [ -f "$local_key" ] && [ -f "$local_pub" ]; then
        info "Using existing cluster SSH key: ${local_key}"
    else
        info "Generating new cluster SSH key pair..."
        mkdir -p "$local_ssh_dir"
        ssh-keygen -t rsa -b 4096 -f "$local_key" -N "" -C "qslurm-cluster" || {
            err "Failed to generate SSH key"
            exit 1
        }
        chmod 600 "$local_key"
        chmod 644 "$local_pub"
        info "  -> ${local_key}"
    fi
}

# ==================== config distribution ====================

distribute_configs() {
    info "=== Distributing configuration files ==="

    local local_conf="${SCRIPT_DIR}/conf/slurm-multi.conf"
    local local_cgroup="${SCRIPT_DIR}/conf/cgroup.conf"
    local local_key="${SCRIPT_DIR}/munge-key/munge.key"
    local local_ssh_dir="${SCRIPT_DIR}/ssh"

    # Auto-generate cgroup.conf if missing (only needs CgroupPlugin=disabled)
    if [ ! -f "$local_cgroup" ]; then
        info "  cgroup.conf not found, generating default (CgroupPlugin=disabled)..."
        mkdir -p "$(dirname "$local_cgroup")"
        echo "CgroupPlugin=disabled" > "$local_cgroup"
    fi

    # Ensure controller has configs
    ssh_cmd "$CONTROLLER_HOST" "mkdir -p ${SLURM_CONF_HOST_DIR}"
    cat "$local_conf"   | ssh_cmd "$CONTROLLER_HOST" "cat > ${SLURM_CONF_HOST_DIR}/slurm.conf"
    cat "$local_cgroup" | ssh_cmd "$CONTROLLER_HOST" "cat > ${SLURM_CONF_HOST_DIR}/cgroup.conf"

    if [ -f "$local_key" ]; then
        cat "$local_key" | ssh_cmd "$CONTROLLER_HOST" "cat > ${SLURM_CONF_HOST_DIR}/munge.key"
        ssh_cmd "$CONTROLLER_HOST" "chown 101:101 ${SLURM_CONF_HOST_DIR}/munge.key && chmod 400 ${SLURM_CONF_HOST_DIR}/munge.key"
    fi

    # Distribute cluster SSH key for passwordless inter-container access
    if [ -f "${local_ssh_dir}/id_rsa" ] && [ -f "${local_ssh_dir}/id_rsa.pub" ]; then
        ssh_cmd "$CONTROLLER_HOST" "mkdir -p ${SLURM_CONF_HOST_DIR}/ssh"
        cat "${local_ssh_dir}/id_rsa"     | ssh_cmd "$CONTROLLER_HOST" "cat > ${SLURM_CONF_HOST_DIR}/ssh/id_rsa"
        cat "${local_ssh_dir}/id_rsa.pub" | ssh_cmd "$CONTROLLER_HOST" "cat > ${SLURM_CONF_HOST_DIR}/ssh/id_rsa.pub"
        ssh_cmd "$CONTROLLER_HOST" "chmod 600 ${SLURM_CONF_HOST_DIR}/ssh/id_rsa ${SLURM_CONF_HOST_DIR}/ssh/id_rsa.pub"
    fi

    info "  Controller: done"

    # Distribute to all hosts
    get_hosts
    for host in "${HOST_LIST[@]}"; do
        ssh_cmd "$host" "mkdir -p ${SLURM_CONF_HOST_DIR}"
        cat "$local_conf"   | ssh_cmd "$host" "cat > ${SLURM_CONF_HOST_DIR}/slurm.conf"
        cat "$local_cgroup" | ssh_cmd "$host" "cat > ${SLURM_CONF_HOST_DIR}/cgroup.conf"
        if [ -f "$local_key" ]; then
            cat "$local_key" | ssh_cmd "$host" "cat > ${SLURM_CONF_HOST_DIR}/munge.key"
            ssh_cmd "$host" "chown 101:101 ${SLURM_CONF_HOST_DIR}/munge.key && chmod 400 ${SLURM_CONF_HOST_DIR}/munge.key"
        fi
        # Distribute cluster SSH key
        if [ -f "${local_ssh_dir}/id_rsa" ] && [ -f "${local_ssh_dir}/id_rsa.pub" ]; then
            ssh_cmd "$host" "mkdir -p ${SLURM_CONF_HOST_DIR}/ssh"
            cat "${local_ssh_dir}/id_rsa"     | ssh_cmd "$host" "cat > ${SLURM_CONF_HOST_DIR}/ssh/id_rsa"
            cat "${local_ssh_dir}/id_rsa.pub" | ssh_cmd "$host" "cat > ${SLURM_CONF_HOST_DIR}/ssh/id_rsa.pub"
            ssh_cmd "$host" "chmod 600 ${SLURM_CONF_HOST_DIR}/ssh/id_rsa ${SLURM_CONF_HOST_DIR}/ssh/id_rsa.pub"
        fi
        info "  ${host}: done"
    done
}

# ==================== container deployment ====================


deploy_containers() {
    info "=== Deploying containers ==="

    # Stop any existing qslurm containers first
    info "Cleaning up existing qslurm containers..."
    cleanup_qslurm_containers

    sleep 2

    # Verify weave network is available before starting containers
    if [ "$SKIP_WEAVE" != "true" ]; then
        info "Verifying weave network..."
        for i in $(seq 1 10); do
            if ssh_cmd "$CONTROLLER_HOST" "docker network ls --filter name=^weave$ --format '{{.Name}}'" 2>/dev/null | grep -q 'weave'; then
                info "  Weave network 'weave' is ready"
                break
            fi
            info "  Waiting for weave network plugin (attempt ${i}/10)..."
            sleep 3
        done
    fi

    # Container network: use 'weave' for cross-host communication.
    # Weave Net's libnetwork plugin registers a Docker network called 'weave'
    # with driver 'weavemesh'.  Containers on this network resolve each
    # other's hostnames via WeaveDNS (e.g. qslurm-ctl → weave IP).
    local NETWORK="weave"
    if [ "$SKIP_WEAVE" = "true" ]; then
        # If weave is skipped, fall back to host networking
        warn "Weave skipped — containers will use host networking"
        NETWORK="host"
    fi

    # ---- Start controller ----
    info "Starting controller container..."
    if [ -z "${QSLURM_SHARE_HOST_DIR:-}" ]; then
        QSLURM_SHARE_HOST_DIR="~/qslurm_share"
    fi
    if [ "$NETWORK" = "host" ]; then
        ssh_cmd "$CONTROLLER_HOST" "mkdir -p ${QSLURM_SHARE_HOST_DIR}/qasm_tools/ && chmod 755 ${QSLURM_SHARE_HOST_DIR}/"
        info "  Controller host share: ${QSLURM_SHARE_HOST_DIR} -> /mnt/qslurm_share"
        ssh_cmd "$CONTROLLER_HOST" "docker run -d \
            --name qslurm-ctl \
            --hostname qslurm-ctl \
            --network host \
            --cpus ${CONTROLLER_CPU} \
            -e NODE_TYPE=controller \
            -e ENABLE_QUANTUM=no \
            -v ${SLURM_CONF_HOST_DIR}/slurm.conf:/etc/slurm/slurm.conf:ro \
            -v ${SLURM_CONF_HOST_DIR}/cgroup.conf:/etc/slurm/cgroup.conf:ro \
            -v ${SLURM_CONF_HOST_DIR}/munge.key:/etc/munge/munge.key:ro \
            -v ${SLURM_CONF_HOST_DIR}/ssh:/etc/qslurm/ssh:ro \
            -v ${QSLURM_SHARE_HOST_DIR}:/mnt/qslurm_share \
            --cap-add SYS_NICE \
            --privileged \
            --restart unless-stopped \
            ${IMAGE_NAME}"
    else
        ssh_cmd "$CONTROLLER_HOST" "mkdir -p ${QSLURM_SHARE_HOST_DIR}/ && chmod 755 ${QSLURM_SHARE_HOST_DIR}/"
        info "  Controller host share: ${QSLURM_SHARE_HOST_DIR} -> /mnt/qslurm_share"
        ssh_cmd "$CONTROLLER_HOST" "docker run -d \
            --name qslurm-ctl \
            --hostname qslurm-ctl \
            --network ${NETWORK} \
            --cpus ${CONTROLLER_CPU} \
            -p 6817:6817 \
            -e NODE_TYPE=controller \
            -e ENABLE_QUANTUM=no \
            -v ${SLURM_CONF_HOST_DIR}/slurm.conf:/etc/slurm/slurm.conf:ro \
            -v ${SLURM_CONF_HOST_DIR}/cgroup.conf:/etc/slurm/cgroup.conf:ro \
            -v ${SLURM_CONF_HOST_DIR}/munge.key:/etc/munge/munge.key:ro \
            -v ${SLURM_CONF_HOST_DIR}/ssh:/etc/qslurm/ssh:ro \
            -v ${QSLURM_SHARE_HOST_DIR}:/mnt/qslurm_share \
            --cap-add SYS_NICE \
            --privileged \
            --restart unless-stopped \
            ${IMAGE_NAME}"
    fi

    # Wait for controller
    info "Waiting for controller to be ready..."
    local ready=false
    for i in $(seq 1 60); do
        if ssh_cmd "$CONTROLLER_HOST" "docker exec qslurm-ctl /opt/slurm/bin/scontrol ping" >/dev/null 2>&1; then
            info "Controller ready!"
            ready=true
            break
        fi
        sleep 2
    done

    if [ "$ready" != "true" ]; then
        err "Controller failed to start. Check logs:"
        ssh_cmd "$CONTROLLER_HOST" "docker logs qslurm-ctl" || true
        exit 1
    fi

    # Get controller's weave IP for DNS workaround.
    # Weave DNS can be unreliable across hosts, so we pin the controller's
    # weave IP into every compute container via --add-host.
    local ctrl_weave_ip=""
    local CTRL_HOSTS_ARG=""
    if [ "$NETWORK" = "weave" ]; then
        ctrl_weave_ip=$(ssh_cmd "$CONTROLLER_HOST" \
            "docker inspect qslurm-ctl --format '{{.NetworkSettings.Networks.weave.IPAddress}}' 2>/dev/null" \
            | tr -d '\r\n')
        if [ -n "$ctrl_weave_ip" ]; then
            info "Controller weave IP: ${ctrl_weave_ip}"
            CTRL_HOSTS_ARG="--add-host qslurm-ctl:${ctrl_weave_ip}"
        else
            warn "Could not determine controller weave IP — DNS may be unreliable"
        fi
    fi

    # ---- Start compute containers on each host ----
    local cn_idx=0 qn_idx=0

    for entry in "${HOSTS[@]}"; do
        local ip cn_num ccpu qn_num qcpu qq
        IFS=':' read -r ip cn_num ccpu qn_num qcpu qq <<< "$entry"

        # Classic nodes
        for ((i=1; i<=${cn_num:-0}; i++)); do
            cn_idx=$((cn_idx + 1))
            local name="qslurm-cn${cn_idx}"
            info "Starting ${name} on ${ip}..."

            if [ "$NETWORK" = "host" ]; then
                ssh_cmd "$ip" "docker run -d \
                    --name ${name} \
                    --hostname ${name} \
                    --network host \
                    ${CTRL_HOSTS_ARG} \
                    --cpus ${ccpu} \
                    -e NODE_TYPE=compute \
                    -e ENABLE_QUANTUM=no \
                    -v ${SLURM_CONF_HOST_DIR}/slurm.conf:/etc/slurm/slurm.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/cgroup.conf:/etc/slurm/cgroup.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/munge.key:/etc/munge/munge.key:ro \
                    -v ${SLURM_CONF_HOST_DIR}/ssh:/etc/qslurm/ssh:ro \
                    --cap-add SYS_NICE \
                    --cap-add SYS_ADMIN \
                    --security-opt apparmor=unconfined \
                    --restart unless-stopped \
                    ${IMAGE_NAME}"
            else
                ssh_cmd "$ip" "docker run -d \
                    --name ${name} \
                    --hostname ${name} \
                    --network ${NETWORK} \
                    ${CTRL_HOSTS_ARG} \
                    --cpus ${ccpu} \
                    -e NODE_TYPE=compute \
                    -e ENABLE_QUANTUM=no \
                    -v ${SLURM_CONF_HOST_DIR}/slurm.conf:/etc/slurm/slurm.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/cgroup.conf:/etc/slurm/cgroup.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/munge.key:/etc/munge/munge.key:ro \
                    -v ${SLURM_CONF_HOST_DIR}/ssh:/etc/qslurm/ssh:ro \
                    --cap-add SYS_NICE \
                    --cap-add SYS_ADMIN \
                    --security-opt apparmor=unconfined \
                    --restart unless-stopped \
                    ${IMAGE_NAME}"
            fi
        done

        # Quantum nodes
        for ((i=1; i<=${qn_num:-0}; i++)); do
            qn_idx=$((qn_idx + 1))
            local name="qslurm-qn${qn_idx}"
            info "Starting ${name} on ${ip}..."

            if [ "$NETWORK" = "host" ]; then
                ssh_cmd "$ip" "docker run -d \
                    --name ${name} \
                    --hostname ${name} \
                    --network host \
                    ${CTRL_HOSTS_ARG} \
                    --cpus ${qcpu} \
                    -e NODE_TYPE=quantum-compute \
                    -e ENABLE_QUANTUM=yes \
                    -e QOSD_QPU_COUNT=${QOSD_QPU_COUNT:-3} \
                    -e QOSD_QPU_CONFIG_DIR=${QOSD_QPU_CONFIG_DIR:-/mnt/qslurm_share/qasm_tools/configs/qpus} \
                    -e QOSD_QPU_CONFIG_SEQUENCE=${QOSD_QPU_CONFIG_SEQUENCE:-} \
                    -e QOSD_TRANSPORT_MS=${QOSD_TRANSPORT_MS:-20} \
                    -e QOS_SIMULATOR=${QOS_SIMULATOR:-hardware} \
                    -e QOS_PACKING_ENABLED=${QOS_PACKING_ENABLED:-1} \
                    -e QOS_SIM_PRELOAD_ENABLED=${QOS_SIM_PRELOAD_ENABLED:-1} \
                    -e QOS_CONTEXT_REUSE_ENABLED=${QOS_CONTEXT_REUSE_ENABLED:-1} \
                    -e QOSD_ENTRYPOINT=${QOSD_ENTRYPOINT:-qosd.py} \
                    -e QOS_RUNTIME_MODE=${QOS_RUNTIME_MODE:-fde} \
                    -e QOS_HARDWARE_RESULT_FALLBACK=${QOS_HARDWARE_RESULT_FALLBACK:-error} \
                    -v ${SLURM_CONF_HOST_DIR}/slurm.conf:/etc/slurm/slurm.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/cgroup.conf:/etc/slurm/cgroup.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/munge.key:/etc/munge/munge.key:ro \
                    -v ${SLURM_CONF_HOST_DIR}/ssh:/etc/qslurm/ssh:ro \
                    --cap-add SYS_NICE \
                    --cap-add SYS_ADMIN \
                    --security-opt apparmor=unconfined \
                    --restart unless-stopped \
                    ${IMAGE_NAME}"
            else
                ssh_cmd "$ip" "docker run -d \
                    --name ${name} \
                    --hostname ${name} \
                    --network ${NETWORK} \
                    ${CTRL_HOSTS_ARG} \
                    --cpus ${qcpu} \
                    -e NODE_TYPE=quantum-compute \
                    -e ENABLE_QUANTUM=yes \
                    -e QOSD_QPU_COUNT=${QOSD_QPU_COUNT:-3} \
                    -e QOSD_QPU_CONFIG_DIR=${QOSD_QPU_CONFIG_DIR:-/mnt/qslurm_share/qasm_tools/configs/qpus} \
                    -e QOSD_QPU_CONFIG_SEQUENCE=${QOSD_QPU_CONFIG_SEQUENCE:-} \
                    -e QOSD_TRANSPORT_MS=${QOSD_TRANSPORT_MS:-20} \
                    -e QOS_SIMULATOR=${QOS_SIMULATOR:-hardware} \
                    -e QOS_PACKING_ENABLED=${QOS_PACKING_ENABLED:-1} \
                    -e QOS_SIM_PRELOAD_ENABLED=${QOS_SIM_PRELOAD_ENABLED:-1} \
                    -e QOS_CONTEXT_REUSE_ENABLED=${QOS_CONTEXT_REUSE_ENABLED:-1} \
                    -e QOSD_ENTRYPOINT=${QOSD_ENTRYPOINT:-qosd.py} \
                    -e QOS_RUNTIME_MODE=${QOS_RUNTIME_MODE:-fde} \
                    -e QOS_HARDWARE_RESULT_FALLBACK=${QOS_HARDWARE_RESULT_FALLBACK:-error} \
                    -v ${SLURM_CONF_HOST_DIR}/slurm.conf:/etc/slurm/slurm.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/cgroup.conf:/etc/slurm/cgroup.conf:ro \
                    -v ${SLURM_CONF_HOST_DIR}/munge.key:/etc/munge/munge.key:ro \
                    -v ${SLURM_CONF_HOST_DIR}/ssh:/etc/qslurm/ssh:ro \
                    --cap-add SYS_NICE \
                    --cap-add SYS_ADMIN \
                    --security-opt apparmor=unconfined \
                    --restart unless-stopped \
                    ${IMAGE_NAME}"
            fi
        done
    done

    info "All containers deployed."
}

# ==================== weave /etc/hosts fixup ====================

# Docker's embedded DNS (127.0.0.11) ONLY resolves container names on the
# SAME Docker network.  Containers on the Weave overlay network get their
# DNS from Docker, which doesn't know about Weave container names.  WeaveDNS
# runs on the host at 172.17.0.1:53 but the Weave Docker plugin does not
# configure containers to use it (compatibility gap between Weave 2.8.1 and
# modern Docker's DNS model).
#
# Workaround: after all containers are deployed, collect every container's
# Weave IP and append static /etc/hosts entries to every container so
# hostname resolution works cross-host.
fixup_weave_hosts() {
    if [ "${SKIP_WEAVE:-false}" = "true" ]; then
        return 0
    fi

    info "=== Fixing /etc/hosts for cross-container name resolution ==="
    info "  (Docker embedded DNS cannot resolve Weave container names)"

    # Collect: container_name -> weave_ip, container_name -> host_ip
    local -A ctn_ips
    local -A ctn_hosts
    local ctn_names=()

    # Controller
    local ctl_ip
    ctl_ip=$(ssh_cmd "$CONTROLLER_HOST" \
        "docker inspect qslurm-ctl --format '{{.NetworkSettings.Networks.weave.IPAddress}}' 2>/dev/null" \
        | tr -d '\r\n')
    if [ -n "$ctl_ip" ] && [ "$ctl_ip" != "<no value>" ]; then
        ctn_ips["qslurm-ctl"]="$ctl_ip"
        ctn_hosts["qslurm-ctl"]="$CONTROLLER_HOST"
        ctn_names+=("qslurm-ctl")
        info "  qslurm-ctl : ${ctl_ip}"
    else
        warn "  qslurm-ctl: no weave IP found — DNS resolution will fail"
    fi

    # Compute nodes (classic + quantum)
    local cn_idx=0 qn_idx=0
    for entry in "${HOSTS[@]}"; do
        local host_ip cn_num ccpu qn_num qcpu qq
        IFS=':' read -r host_ip cn_num ccpu qn_num qcpu qq <<< "$entry"

        for ((i=1; i<=${cn_num:-0}; i++)); do
            cn_idx=$((cn_idx + 1))
            local name="qslurm-cn${cn_idx}"
            local weave_ip
            weave_ip=$(ssh_cmd "$host_ip" \
                "docker inspect ${name} --format '{{.NetworkSettings.Networks.weave.IPAddress}}' 2>/dev/null" \
                | tr -d '\r\n')
            if [ -n "$weave_ip" ] && [ "$weave_ip" != "<no value>" ]; then
                ctn_ips["$name"]="$weave_ip"
                ctn_hosts["$name"]="$host_ip"
                ctn_names+=("$name")
                info "  ${name} : ${weave_ip}"
            else
                warn "  ${name}: no weave IP — skipping"
            fi
        done

        for ((i=1; i<=${qn_num:-0}; i++)); do
            qn_idx=$((qn_idx + 1))
            local name="qslurm-qn${qn_idx}"
            local weave_ip
            weave_ip=$(ssh_cmd "$host_ip" \
                "docker inspect ${name} --format '{{.NetworkSettings.Networks.weave.IPAddress}}' 2>/dev/null" \
                | tr -d '\r\n')
            if [ -n "$weave_ip" ] && [ "$weave_ip" != "<no value>" ]; then
                ctn_ips["$name"]="$weave_ip"
                ctn_hosts["$name"]="$host_ip"
                ctn_names+=("$name")
                info "  ${name} : ${weave_ip}"
            else
                warn "  ${name}: no weave IP — skipping"
            fi
        done
    done

    if [ ${#ctn_names[@]} -le 1 ]; then
        warn "  Only ${#ctn_names[@]} container(s) found — skipping hosts fixup"
        return 0
    fi

    # Build a clean /etc/hosts snippet: every container name → its weave IP
    local hosts_snippet=""
    for name in "${ctn_names[@]}"; do
        hosts_snippet="${hosts_snippet}${ctn_ips[$name]} ${name}\\n"
    done

    # Append to every container (deduplicate by host)
    local -A hosts_done
    for name in "${ctn_names[@]}"; do
        local h="${ctn_hosts[$name]}"
        if [ -n "${hosts_done[$h]:-}" ]; then
            continue  # same host already processed (multiple containers per host)
        fi
        hosts_done[$h]=1

        # Apply to ALL containers on this host
        for ctn in "${ctn_names[@]}"; do
            if [ "${ctn_hosts[$ctn]}" != "$h" ]; then
                continue  # container is on a different host
            fi
            ssh_cmd "$h" "docker exec ${ctn} bash -c 'echo -e \"${hosts_snippet}\" >> /etc/hosts'" 2>/dev/null || {
                warn "  Failed to add hosts to ${ctn} on ${h}"
            }
        done
    done

    # Verify: check that the controller can now resolve a compute node
    if [ ${#ctn_names[@]} -ge 2 ]; then
        local test_name="${ctn_names[1]}"  # first non-controller container
        if ssh_cmd "$CONTROLLER_HOST" "docker exec qslurm-ctl getent hosts ${test_name}" >/dev/null 2>&1; then
            info "  Verified: qslurm-ctl can resolve ${test_name}"
        else
            warn "  Verification failed: qslurm-ctl cannot resolve ${test_name}"
        fi
    fi

    info "Hosts fixup complete."
}

# ==================== cluster activation ====================

activate_cluster() {
    info "=== Activating cluster ==="

    # ---- Fix cross-container DNS before Slurm daemons communicate ----
    # Docker embedded DNS (127.0.0.11) cannot resolve Weave container
    # names.  Without this fixup, slurmctld cannot reach slurmd on the
    # compute nodes (hostname lookup fails → sinfo timeout).
    fixup_weave_hosts

    # Slurm timing issue: slurmd may start before slurmctld is fully
    # initialised (especially with quantum collector).  When slurmd's
    # first registration attempt fails it backs off exponentially,
    # leaving nodes UNKNOWN for minutes.  Sending SIGHUP to every slurmd
    # forces an immediate re-registration.
    # Wait for slurmd processes to start inside containers
    sleep 5

    info "Forcing slurmd re-registration on all compute nodes..."
    local cn_idx=0 qn_idx=0
    for entry in "${HOSTS[@]}"; do
        local ip cn_num ccpu qn_num qcpu qq
        IFS=':' read -r ip cn_num ccpu qn_num qcpu qq <<< "$entry"

        for ((i=1; i<=${cn_num:-0}; i++)); do
            cn_idx=$((cn_idx + 1))
            # Retry: slurmd may not be running yet
            for attempt in $(seq 1 5); do
                ssh_cmd "$ip" "docker exec qslurm-cn${cn_idx} pkill -HUP slurmd" 2>/dev/null && break
                sleep 2
            done
        done

        for ((i=1; i<=${qn_num:-0}; i++)); do
            qn_idx=$((qn_idx + 1))
            for attempt in $(seq 1 5); do
                ssh_cmd "$ip" "docker exec qslurm-qn${qn_idx} pkill -HUP slurmd" 2>/dev/null && break
                sleep 2
            done
        done
    done

    # Wait for registrations to complete.
    # slurmd re-registers directly as IDLE after SIGHUP — no RESUME needed.
    info "Waiting for nodes to register..."
    sleep 10

    # Final reconfigure to pick up any state changes
    ssh_cmd "$CONTROLLER_HOST" \
        "docker exec qslurm-ctl /opt/slurm/bin/scontrol reconfigure" 2>/dev/null || true
    sleep 3

    # ---- Verify passwordless SSH between containers ----
    info "Verifying passwordless SSH between containers..."
    local ssh_ok=true
    local cn_idx=0 qn_idx=0

    for entry in "${HOSTS[@]}"; do
        local ip cn_num ccpu qn_num qcpu qq
        IFS=':' read -r ip cn_num ccpu qn_num qcpu qq <<< "$entry"

        for ((i=1; i<=${cn_num:-0}; i++)); do
            cn_idx=$((cn_idx + 1))
            local name="qslurm-cn${cn_idx}"
            if ssh_cmd "$CONTROLLER_HOST" \
                "docker exec qslurm-ctl ssh -o ConnectTimeout=5 -o BatchMode=yes ${name} 'echo ok'" \
                >/dev/null 2>&1; then
                info "  SSH qslurm-ctl -> ${name}: OK"
            else
                warn "  SSH qslurm-ctl -> ${name}: FAILED"
                ssh_ok=false
            fi
        done

        for ((i=1; i<=${qn_num:-0}; i++)); do
            qn_idx=$((qn_idx + 1))
            local name="qslurm-qn${qn_idx}"
            if ssh_cmd "$CONTROLLER_HOST" \
                "docker exec qslurm-ctl ssh -o ConnectTimeout=5 -o BatchMode=yes ${name} 'echo ok'" \
                >/dev/null 2>&1; then
                info "  SSH qslurm-ctl -> ${name}: OK"
            else
                warn "  SSH qslurm-ctl -> ${name}: FAILED"
                ssh_ok=false
            fi
        done
    done

    if [ "$ssh_ok" = "true" ]; then
        info "Passwordless SSH: all connections verified"
    else
        warn "Some SSH connections failed — check container logs: docker exec <name> cat /var/log/slurm/sshd.log"
    fi
}

# ==================== status display ====================

show_status() {
    info "=== Cluster Status ==="
    echo ""
    ssh_cmd "$CONTROLLER_HOST" "docker exec qslurm-ctl /opt/slurm/bin/sinfo" || true
    echo ""
    info "Node details:"
    ssh_cmd "$CONTROLLER_HOST" \
        "docker exec qslurm-ctl /opt/slurm/bin/scontrol show nodes" 2>/dev/null \
        | grep -E "NodeName|CPUTot|RealMemory|Qubits|State" || true
    echo ""
    info "Cluster is ready."
    echo ""
    info "Useful commands:"
    echo "  docker exec qslurm-ctl sinfo"
    echo "  docker exec qslurm-ctl srun -p compute -N1 hostname"
    echo "  docker exec qslurm-ctl srun -p q_compute -N1 hostname"
    echo ""
    echo "  bash deploy.sh --destroy    # tear down cluster"
}

# ==================== teardown ====================

destroy_cluster() {
    warn "=== Tearing down qslurm cluster ==="

    # Step 1: remove qslurm containers FIRST (they hold references to weave network)
    info "Removing qslurm containers on all hosts..."
    for host in $( { echo "$CONTROLLER_HOST"; get_hosts; for h in "${HOST_LIST[@]}"; do echo "$h"; done; } | sort -u); do
        ssh_cmd "$host" \
            "docker rm -f \$(docker ps -a --filter name=qslurm- --format '{{.Names}}' 2>/dev/null) 2>/dev/null" || true
    done

    # Step 2: stop weave (removes containers, bridge, plugin sockets)
    info "Stopping Weave Net on all hosts..."
    for host in $( { echo "$CONTROLLER_HOST"; get_hosts; for h in "${HOST_LIST[@]}"; do echo "$h"; done; } | sort -u); do
        weave_stop_on_host "$host"
    done

    # Clean up weave iptables rules
    info "Cleaning up weave iptables rules..."
    for host in $( { echo "$CONTROLLER_HOST"; get_hosts; for h in "${HOST_LIST[@]}"; do echo "$h"; done; } | sort -u); do
        ssh_cmd "$host" "
            iptables -D FORWARD -i weave -j ACCEPT 2>/dev/null
            iptables -D FORWARD -o weave -j ACCEPT 2>/dev/null
            iptables -D FORWARD -i datapath -j ACCEPT 2>/dev/null
            iptables -D FORWARD -o datapath -j ACCEPT 2>/dev/null
        " 2>/dev/null || true
    done

    # Step 3: remove qslurm image from all hosts so the next deploy
    #         re-imports from the local tarball (ensures a fresh build).
    info "Removing qslurm image from all hosts..."
    for host in $( { echo "$CONTROLLER_HOST"; get_hosts; for h in "${HOST_LIST[@]}"; do echo "$h"; done; } | sort -u); do
        if ssh_cmd "$host" "docker image inspect ${IMAGE_NAME} >/dev/null 2>&1"; then
            ssh_cmd "$host" "docker rmi ${IMAGE_NAME}" 2>/dev/null || {
                warn "  ${host}: force-removing image ${IMAGE_NAME}"
                ssh_cmd "$host" "docker rmi -f ${IMAGE_NAME}" 2>/dev/null || true
            }
            info "  ${host}: image removed"
        else
            info "  ${host}: image not present — skip"
        fi
    done

    info "Cluster destroyed."
}

# ==================== main ====================

main() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -c|--config)          CONFIG_FILE="$2"; shift 2 ;;
            --skip-weave)         SKIP_WEAVE=true; shift ;;
            --skip-weave-image)   SKIP_WEAVE_IMAGE=true; shift ;;
            --skip-image)         SKIP_IMAGE=true; shift ;;
            --force-image)        FORCE_IMAGE=true; shift ;;
            --destroy)            DESTROY_MODE=true; shift ;;
            -h|--help)            usage ;;
            *)                    err "Unknown option: $1"; usage ;;
        esac
    done

    load_config

    if [ "$DESTROY_MODE" = "true" ]; then
        destroy_cluster
        exit 0
    fi

    check_prereqs
    setup_weave
    distribute_image
    generate_slurm_conf
    setup_munge_key
    setup_ssh_key
    distribute_configs
    deploy_containers
    activate_cluster
    show_status
}

main "$@"
