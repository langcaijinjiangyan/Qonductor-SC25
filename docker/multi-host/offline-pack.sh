#!/usr/bin/env bash
# ============================================================================
# Qonductor + k3s Offline Image Packager
# ============================================================================
# Pulls ALL required container images and packages them into a single
# images.tar for offline deployment on REDHAT nodes.
#
# Output: images.tar (in current working directory)
#
# Usage:
#   bash docker/multi-host/offline-pack.sh              # full pack
#   SKIP_QONDUCTOR=1 bash docker/multi-host/offline-pack.sh  # k3s infra only
#   SKIP_K3S_IMAGES=1 bash docker/multi-host/offline-pack.sh  # Qonductor only
#
# Environment variables:
#   OUTPUT_DIR         output directory (default: .)
#   OUTPUT_FILE        output tar filename (default: images.tar)
#   SKIP_QONDUCTOR     skip Qonductor image build (use existing images)
#   SKIP_K3S_IMAGES    skip k3s infrastructure images
#   DRY_RUN            only list images, don't pull or save
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUTPUT_DIR="${OUTPUT_DIR:-.}"
OUTPUT_FILE="${OUTPUT_FILE:-images.tar}"
SKIP_QONDUCTOR="${SKIP_QONDUCTOR:-0}"
SKIP_K3S_IMAGES="${SKIP_K3S_IMAGES:-0}"
DRY_RUN="${DRY_RUN:-0}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[pack]${NC} $*"; }
warn() { echo -e "${YELLOW}[warn]${NC} $*"; }
die()  { echo -e "${RED}[fatal]${NC} $*" >&2; exit 1; }

# ===========================================================================
# Image definitions
# ===========================================================================

# -- k3s core ---------------------------------------------------------------
K3S_IMAGE="rancher/k3s:v1.32.0-k3s1"

# -- k3s infrastructure (must match the running k3s version) -----------------
# Checked from a live k3s v1.32.0+k3s1 cluster (July 2026).
# These are pulled automatically by k3s. For offline, we preload them.
K3S_INFRA_IMAGES=(
    # Sandbox / pause container
    "rancher/mirrored-pause:3.6"

    # CoreDNS
    "rancher/mirrored-coredns-coredns:1.12.0"

    # Traefik ingress controller
    "rancher/mirrored-library-traefik:2.11.10"

    # Local path provisioner (default StorageClass)
    "rancher/local-path-provisioner:v0.0.30"

    # Metrics server
    "rancher/mirrored-metrics-server:v0.7.2"

    # Klipper (Helm controller + ServiceLB)
    "rancher/klipper-helm:v0.9.3-build20241008"
    "rancher/klipper-lb:v0.4.9"
)

# -- Qonductor images -------------------------------------------------------
QONDUCTOR_IMAGES=(
    "qonductor-operator:latest"
    "qonductor-device-plugin:latest"
    "qonductor-quantum-executor:latest"
)

# -- User images (optional, uncomment or extend) ----------------------------
# Add any extra images your workflows need here.
EXTRA_IMAGES=(
    # "python:3.11-slim"     # default classical step runner
    # "busybox:latest"       # debugging
)

# ===========================================================================
# Collect all images
# ===========================================================================

collect_images() {
    local images=()

    if [[ "$SKIP_K3S_IMAGES" != "1" ]]; then
        images+=("$K3S_IMAGE")
        images+=("${K3S_INFRA_IMAGES[@]}")
    fi

    if [[ "$SKIP_QONDUCTOR" != "1" ]]; then
        images+=("${QONDUCTOR_IMAGES[@]}")
    fi

    images+=("${EXTRA_IMAGES[@]}")

    # Deduplicate and print.
    printf '%s\n' "${images[@]}" | sort -u
}

# ===========================================================================
# Pull images
# ===========================================================================

pull_images() {
    local images=("$@")
    local failed=()

    log "Pulling ${#images[@]} images …"

    for img in "${images[@]}"; do
        if docker image inspect "$img" >/dev/null 2>&1; then
            log "  [skip] $img (already present)"
            continue
        fi
        echo -n "  [pull] $img … "
        if docker pull "$img" >/dev/null 2>&1; then
            echo "✓"
        else
            echo "✗ FAILED"
            failed+=("$img")
        fi
    done

    if [[ ${#failed[@]} -gt 0 ]]; then
        warn "${#failed[@]} image(s) failed to pull:"
        for f in "${failed[@]}"; do
            warn "  - $f"
        done
    fi
}

# ===========================================================================
# Build Qonductor images
# ===========================================================================

build_qonductor() {
    if [[ "$SKIP_QONDUCTOR" == "1" ]]; then
        warn "SKIP_QONDUCTOR=1 — using existing Qonductor images."
        return 0
    fi

    log "Building Qonductor images …"
    mkdir -p "${PROJECT_ROOT}/data/workflow_registry"

    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.operator" \
        -t qonductor-operator:latest "${PROJECT_ROOT}"

    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.device-plugin" \
        -t qonductor-device-plugin:latest "${PROJECT_ROOT}"

    docker build -f "${PROJECT_ROOT}/docker/Dockerfile.quantum-executor" \
        -t qonductor-quantum-executor:latest "${PROJECT_ROOT}"

    log "  ✓ Qonductor images built"
}

# ===========================================================================
# Save to tar
# ===========================================================================

save_images() {
    local images=("$@")
    local output="${OUTPUT_DIR}/${OUTPUT_FILE}"

    if [[ "$DRY_RUN" == "1" ]]; then
        log "[dry-run] Would save ${#images[@]} images to ${output}:"
        for img in "${images[@]}"; do
            echo "  $img"
        done
        return 0
    fi

    log "Saving ${#images[@]} images → ${output} …"
    docker save "${images[@]}" -o "$output"

    local size
    size=$(du -h "$output" | cut -f1)
    log "  ✓ ${output} (${size})"
}

# ===========================================================================
# Generate load script
# ===========================================================================

generate_load_script() {
    local load_script="${OUTPUT_DIR}/load-images.sh"
    local image_list_file="${OUTPUT_DIR}/image-list.txt"

    cat > "$load_script" <<'LOAD_HEADER'
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
LOAD_HEADER

    chmod +x "$load_script"

    # ---- Generate image list reference file --------------------------------
    collect_images > "$image_list_file"

    log "  ✓ load-images.sh generated"
    log "  ✓ image-list.txt generated"
}

# ===========================================================================
# Main
# ===========================================================================

main() {
    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Qonductor + k3s Offline Image Packager${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo ""

    # ---- 1. Build Qonductor images -----------------------------------------
    build_qonductor

    # ---- 2. Collect image list ---------------------------------------------
    local all_images
    mapfile -t all_images < <(collect_images)

    echo ""
    log "Images to package (${#all_images[@]} total):"
    for img in "${all_images[@]}"; do
        echo "  - $img"
    done
    echo ""

    # ---- 3. Pull missing images --------------------------------------------
    pull_images "${all_images[@]}"

    # ---- 4. Save everything to images.tar ----------------------------------
    save_images "${all_images[@]}"

    # ---- 5. Generate companion load script ---------------------------------
    generate_load_script

    # ---- 6. Summary --------------------------------------------------------
    echo ""
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}  Offline package ready${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════${NC}"
    echo ""
    echo "  Deliver to target nodes:"
    echo "    ${OUTPUT_DIR}/${OUTPUT_FILE} ($(du -h "${OUTPUT_DIR}/${OUTPUT_FILE}" 2>/dev/null | cut -f1 || echo 'N/A'))"
    echo "    ${OUTPUT_DIR}/load-images.sh"
    echo "    ${OUTPUT_DIR}/image-list.txt"
    echo ""
    echo "  On each REDHAT node:"
    echo "    sudo bash load-images.sh"
    echo ""
}

main "$@"
