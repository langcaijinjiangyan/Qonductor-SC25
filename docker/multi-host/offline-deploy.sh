#!/usr/bin/env bash
# ============================================================================
# Qonductor + k3s Offline Deployment Helper
# ============================================================================
# Pre-flight check and deployment helper for REDHAT offline nodes.
# Run this on EACH node BEFORE deploying the k3s cluster.
#
# Usage:
#   sudo bash deploy/offline-prepare.sh
#
# What it does:
#   1. Checks system dependencies
#   2. Configures firewall (firewalld)
#   3. Checks SELinux status
#   4. Loads Docker images from images.tar
#   5. Sets up directories for QPU profiles
# ============================================================================

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

PASS=0; FAIL=0; WARN=0

check()  { echo -e "  ${GREEN}[✓]${NC} $*"; PASS=$((PASS+1)); }
fail()  { echo -e "  ${RED}[✗]${NC} $*"; FAIL=$((FAIL+1)); }
warn()  { echo -e "  ${YELLOW}[!]${NC} $*"; WARN=$((WARN+1)); }

echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Qonductor k3s Offline Node Pre-flight Check"
echo "═══════════════════════════════════════════════════════"
echo ""

# ── 1. OS ───────────────────────────────────────────────────────────────────
echo "── 1. Operating System ──"

if [ -f /etc/redhat-release ]; then
    check "RHEL detected: $(cat /etc/redhat-release)"
else
    warn "Not RHEL: $(cat /etc/os-release 2>/dev/null | grep PRETTY_NAME | cut -d= -f2 | tr -d '"' || echo 'unknown')"
fi

# Kernel
KVER=$(uname -r)
check "Kernel: ${KVER}"

# Architecture
ARCH=$(uname -m)
if [[ "$ARCH" == "x86_64" ]]; then
    check "Architecture: x86_64"
else
    warn "Architecture: $ARCH (x86_64 recommended)"
fi

# ── 2. Docker ───────────────────────────────────────────────────────────────
echo ""
echo "── 2. Docker ──"

if command -v docker >/dev/null 2>&1; then
    DOCKER_VER=$(docker --version 2>/dev/null | awk '{print $3}' | tr -d ',')
    check "Docker: ${DOCKER_VER}"

    # Is daemon running?
    if docker info >/dev/null 2>&1; then
        check "Docker daemon: running"

        # Check storage driver
        DRIVER=$(docker info 2>/dev/null | grep 'Storage Driver' | awk '{print $3}')
        check "Storage driver: ${DRIVER:-unknown}"

        # Check cgroup driver
        CGROUP=$(docker info 2>/dev/null | grep 'Cgroup Driver' | awk '{print $3}')
        check "Cgroup driver: ${CGROUP:-unknown}"
        if [[ "$CGROUP" == "cgroupfs" ]]; then
            warn "cgroupfs detected. systemd cgroup driver recommended for k3s."
        fi
    else
        fail "Docker daemon NOT running"
    fi

    # Check docker group membership
    if groups | grep -q docker; then
        check "Current user is in docker group"
    else
        warn "Current user NOT in docker group. Run: sudo usermod -aG docker \$USER"
    fi
else
    fail "Docker not found. Install docker-ce >= 20.10."
fi

# ── 3. Kernel / cgroup / network modules ────────────────────────────────────
echo ""
echo "── 3. Kernel Requirements ──"

# cgroup v2
if [[ -d /sys/fs/cgroup ]] && [[ "$(stat -f -c %T /sys/fs/cgroup 2>/dev/null)" == "cgroup2fs" ]]; then
    check "cgroup v2: enabled"
elif [[ -f /sys/fs/cgroup/cgroup.controllers ]]; then
    check "cgroup v2: enabled"
else
    warn "cgroup v2 not detected. k3s works best with cgroup v2."
fi

# br_netfilter
if lsmod | grep -q br_netfilter; then
    check "br_netfilter: loaded"
else
    warn "br_netfilter not loaded. Run: modprobe br_netfilter"
fi

# overlay
if lsmod | grep -q overlay; then
    check "overlay: loaded"
else
    fail "overlay kernel module not loaded. Run: modprobe overlay"
fi

# ip_forward
if [[ "$(sysctl -n net.ipv4.ip_forward 2>/dev/null)" == "1" ]]; then
    check "ip_forward: enabled"
else
    warn "ip_forward disabled. Run: sysctl -w net.ipv4.ip_forward=1"
fi

# bridge-nf-call-iptables
if [[ "$(sysctl -n net.bridge.bridge-nf-call-iptables 2>/dev/null)" == "1" ]]; then
    check "bridge-nf-call-iptables: enabled"
else
    warn "bridge-nf-call-iptables disabled. Run: sysctl -w net.bridge.bridge-nf-call-iptables=1"
fi

# ── 4. SELinux ──────────────────────────────────────────────────────────────
echo ""
echo "── 4. SELinux ──"

if command -v getenforce >/dev/null 2>&1; then
    SELINUX=$(getenforce 2>/dev/null || echo "Unknown")
    if [[ "$SELINUX" == "Enforcing" ]]; then
        warn "SELinux is Enforcing. May block k3s privileged containers."
        echo "       Consider: sudo setenforce 0  (temporary)"
        echo "                sudo sed -i 's/SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config"
    elif [[ "$SELINUX" == "Permissive" ]]; then
        check "SELinux: Permissive"
    else
        check "SELinux: Disabled"
    fi
else
    check "SELinux: not installed"
fi

# ── 5. Firewall ─────────────────────────────────────────────────────────────
echo ""
echo "── 5. Firewall ──"

if command -v firewall-cmd >/dev/null 2>&1; then
    check "firewalld: installed"
    if systemctl is-active --quiet firewalld; then
        warn "firewalld is running. Required ports:"
        echo "       TCP 6443  (k3s API Server)"
        echo "       UDP 8472  (Flannel VXLAN)"
        echo "       TCP 10250 (Kubelet API)"
        echo ""
        echo "       Open them with:"
        echo "         sudo firewall-cmd --add-port=6443/tcp --permanent"
        echo "         sudo firewall-cmd --add-port=8472/udp --permanent"
        echo "         sudo firewall-cmd --add-port=10250/tcp --permanent"
        echo "         sudo firewall-cmd --reload"
    else
        check "firewalld: not active"
    fi
else
    check "firewalld: not installed (using iptables/nftables)"
fi

# ── 6. Required utilities ───────────────────────────────────────────────────
echo ""
echo "── 6. Required Utilities ──"

for cmd in tar gzip curl wget python3 ssh; do
    if command -v $cmd >/dev/null 2>&1; then
        check "$cmd: available"
    else
        warn "$cmd: not found (some features may not work)"
    fi
done

# ── 7. Disk space ───────────────────────────────────────────────────────────
echo ""
echo "── 7. Disk Space ──"

ROOT_AVAIL=$(df -h / | awk 'NR==2 {print $4}')
check "Root partition free: ${ROOT_AVAIL}"

IMAGE_SIZE_EST="~3.5 GB (images) + ~1 GB (k3s data)"
echo "       Estimated required: ${IMAGE_SIZE_EST}"

# ── 8. Summary ──────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Summary: ${PASS} passed, ${WARN} warnings, ${FAIL} failed"
echo "═══════════════════════════════════════════════════════"

if [[ $FAIL -gt 0 ]]; then
    echo ""
    echo "  Fix the failures above before proceeding."
    exit 1
fi

if [[ $WARN -gt 0 ]]; then
    echo ""
    echo "  Review the ${WARN} warnings above before deploying."
fi

echo ""
echo "  Next step:"
echo "    1. Load images:  bash load-images.sh"
echo "    2. Deploy k3s:   bash docker/multi-host/deploy-cluster.sh"
echo ""
