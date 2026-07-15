#!/usr/bin/env bash
# ============================================================================
# Qonductor + k3s Offline Image Loader
# ============================================================================
# Loads images from images.tar into both Docker and the k3s embedded
# containerd on each target node.
#
# Usage (on each REDHAT node):
#   sudo bash load-images.sh
#
# Environment variables:
#   K3S_CONTAINER_NAME   k3s container name (auto-detected if empty)
#   SKIP_DOCKER_LOAD     skip loading into Docker (only k3s containerd)
#   SKIP_K3S_LOAD        skip loading into k3s containerd (only Docker)
#   IMAGE_TAR            path to images.tar (default: ./images.tar)
# ============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[load]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
die()  { echo -e "${RED}[fatal]${NC} $*" >&2; exit 1; }

IMAGE_TAR="${IMAGE_TAR:-./images.tar}"
K3S_CONTAINER_NAME="${K3S_CONTAINER_NAME:-}"
SKIP_DOCKER_LOAD="${SKIP_DOCKER_LOAD:-0}"
SKIP_K3S_LOAD="${SKIP_K3S_LOAD:-0}"

# Auto-detect k3s container if not specified.
if [[ -z "$K3S_CONTAINER_NAME" ]]; then
    K3S_CONTAINER_NAME=$(docker ps --format '{{.Names}}' | grep -E 'k3s-server|k3s-agent' | head -1 || true)
    if [[ -z "$K3S_CONTAINER_NAME" ]]; then
        warn "No running k3s container detected. Set K3S_CONTAINER_NAME env var."
        warn "Skipping k3s containerd import."
    else
        log "Auto-detected k3s container: ${K3S_CONTAINER_NAME}"
    fi
fi

[[ -f "$IMAGE_TAR" ]] || die "images.tar not found at ${IMAGE_TAR}"

# ---- Load into Docker -----------------------------------------------------
if [[ "$SKIP_DOCKER_LOAD" != "1" ]]; then
    log "Loading images into Docker …"
    docker load -i "$IMAGE_TAR"
    log "  ✓ Docker images loaded"
else
    warn "SKIP_DOCKER_LOAD=1 — skipping Docker load."
fi

# ---- Load into k3s embedded containerd ------------------------------------
if [[ "$SKIP_K3S_LOAD" != "1" && -n "$K3S_CONTAINER_NAME" ]]; then
    log "Importing images into k3s containerd (${K3S_CONTAINER_NAME}) …"

    # Extract image names from the tar.
    IMAGE_NAMES=$(docker load -i "$IMAGE_TAR" 2>/dev/null | grep 'Loaded image' | sed 's/Loaded image: //' || true)

    # If docker load already ran above, get names from tar.
    if [[ -z "$IMAGE_NAMES" ]]; then
        # Get image list from the tar metadata.
        IMAGE_NAMES=$(tar -xf "$IMAGE_TAR" -O manifest.json 2>/dev/null | \
            python3 -c "import json,sys; [print(t.replace('docker.io/','')) for r in json.load(sys.stdin) for t in r.get('RepoTags',[])]" 2>/dev/null || true)
    fi

    for img in $IMAGE_NAMES; do
        # Normalize image name (strip docker.io/ prefix).
        img="${img#docker.io/}"
        echo -n "  [import] $img … "
        if docker save "$img" 2>/dev/null | docker exec -i "$K3S_CONTAINER_NAME" ctr images import - >/dev/null 2>&1; then
            echo "✓"
        else
            warn "✗ Failed (may already exist or be a non-image layer)"
        fi
    done
    log "  ✓ k3s containerd import done"
else
    warn "Skipping k3s containerd import."
fi

echo ""
log "Image loading complete."
echo ""
echo "  Verify: docker exec ${K3S_CONTAINER_NAME} ctr images list"
