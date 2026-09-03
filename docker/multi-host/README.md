# Qonductor Multi-Host k3s-in-Docker Cluster

This directory contains scripts to deploy a **multi-host Kubernetes cluster**
where **all K8s components run inside Docker containers** — nothing is
installed on the hosts except Docker itself.

## Architecture

```
192.168.36.128 (Server)              192.168.36.129 (Agent)
┌────────────────────────┐           ┌────────────────────────┐
│ Docker                 │           │ Docker                 │
│  ┌──────────────────┐  │  TCP 6443 │  ┌──────────────────┐  │
│  │ k3s-server       │◄─┼───────────┤  │ k3s-agent-1      │  │
│  │ (control-plane +  │  │ UDP 8472 │  │ (worker)         │  │
│  │  embedded etcd)   │◄─┼──────────►│  │                  │  │
│  │ --network host    │  │ Flannel  │  │ --network host   │  │
│  └──────────────────┘  │   VXLAN   │  └──────────────────┘  │
└────────────────────────┘           └────────────────────────┘
```

- **k3s Server** — API server, controller manager, scheduler, embedded
  etcd/SQLite (via kine shim), kubelet, Flannel CNI.
- **k3s Agent** — kubelet, kube-proxy, Flannel CNI, containerd.
- **Host networking** — containers use `--network host` so K8s ports are
  exposed directly on the physical host IPs.  No nested CNI overhead.
- **Privileged containers** — required for k3s to create Pod network
  namespaces and manage cgroups.  Suitable for dev/test; for production
  consider additional hardening.

## Quick Start

### Prerequisites

- Docker >= 20.10 on all hosts
- SSH key-based access between hosts
- `kubectl` and `python3` on the machine running the script
- Firewall: TCP 6443, UDP 8472, TCP 10250 open between hosts

### 1. Edit the configuration

Edit `docker/multi-host/cluster-config.yaml` to match your topology:

```yaml
cluster:
  name: qonductor
  kubernetesVersion: "v1.32.0-k3s1"
  k3sToken: "your-secure-token-here"
  flannelInterface: "eth0"  # or "auto" to auto-detect

server:
  host: 192.168.36.128
  nodeName: control-plane-1
  nodeType: classical

agents:
  - host: 192.168.36.129
    nodeName: quantum-worker-1
    nodeType: quantum
    qpus:
      - qpu0_27q.json
      - qpu1_27q.json
```

> **IMPORTANT:** Only ONE agent per physical host when using host
> networking. Two agents on the same host would conflict on kubelet port
> 10250 and NodePort range 30000-32767.

### 2. Deploy

```bash
bash docker/multi-host/deploy-cluster.sh
```

This will:
1. Validate the configuration and QPU profiles
2. Start the k3s server container on the server host
3. Start k3s agent containers on each agent host
4. Wait for all nodes to join and become Ready
5. Load `images.tar`, copy it to remote hosts, and import images
6. Deploy CRDs, operator, and device plugin
7. Label nodes and configure QPU extended resources

### 3. Verify

```bash
kubectl get nodes -o wide
kubectl get pods -n default

# Submit a test workflow
kubectl apply -f deploy/examples/qaoa-12-dynamic-workflow.yaml
kubectl get hybridworkflows -w
```

### 4. Tear down

```bash
# Tear down the cluster; preserves host state, Docker images, and images.tar by default
bash docker/multi-host/teardown-cluster.sh

# Stop containers but preserve Docker data volumes
CLEANUP_VOLUMES=0 bash docker/multi-host/teardown-cluster.sh
```

## Scripts

| Script | Purpose |
|--------|---------|
| `deploy-cluster.sh` | Full deployment: load/distribute `images.tar` → server → agents → labels → controllers |
| `teardown-cluster.sh` | Stop/remove all k3s containers and optional cleanup |
| `offline-pack.sh` | Build/package required images into `images.tar` |
| `load-images.sh` | Load `images.tar` into Docker and an existing k3s containerd |

## Environment Variables

### deploy-cluster.sh

| Variable | Default | Description |
|----------|---------|-------------|
| `SKIP_FIREWALL` | `1` | Skip firewall port hints |
| `IMAGES_TAR` | `docker/multi-host/images.tar` | Image bundle used by deployment |
| `REMOTE_IMAGES_TAR` | `/tmp/qonductor-images.tar` | Remote path used when copying the image bundle |
| `SKIP_IMAGE_DISTRIBUTE` | `1` | Skip copying/loading `images.tar` on remote hosts |
| `SKIP_CONTAINERD_IMPORT` | `0` | Skip importing loaded Docker images into k3s containerd |
| `SKIP_CONTROLLERS` | `0` | Skip operator/device-plugin deployment |
| `DRY_RUN` | `0` | Print commands without executing |

### teardown-cluster.sh

| Variable | Default | Description |
|----------|---------|-------------|
| `CLEANUP_VOLUMES` | `1` | Remove k3s Docker data volumes |
| `CLEANUP_IMAGES` | `0` | Remove Qonductor Docker images |
| `CLEANUP_RANCHER` | `0` | Remove host `/etc/rancher` k3s state |
| `CLEANUP_QONDUCTOR` | `0` | Remove host `/etc/qonductor` QPU/offline state |
| `CLEANUP_QONDUCTOR_K8S` | `1` | Delete Qonductor K8s resources before teardown |
| `CLEANUP_REMOTE_IMAGES_TAR` | `0` | Remove distributed `/tmp/qonductor-images.tar` copies |
| `CLEANUP_DEPLOY_LOGS` | `1` | Remove local `data/deploy_logs` |
| `CLEANUP_KUBECONFIG_TMP` | `1` | Remove local `/tmp/k3s-multi-host-config.yaml` |
| `CLEANUP_IMAGES_TAR` | `0` | Remove local `docker/multi-host/images.tar` |
| `REMOTE_IMAGES_TAR` | `/tmp/qonductor-images.tar` | Distributed bundle path to remove when cleanup is enabled |
| `DRY_RUN` | `0` | Print commands without executing |

### offline-pack.sh

| Variable | Default | Description |
|----------|---------|-------------|
| `OUTPUT_FILE` | `images.tar` | Output tar filename |
| `SKIP_QONDUCTOR` | `0` | Skip rebuilding Qonductor images |

## Networking

All k3s containers use `--network host`, so the K8s control plane and data
plane are exposed on the host's network interfaces directly:

| Port | Protocol | Purpose |
|------|----------|---------|
| 6443 | TCP | k3s API Server |
| 8472 | UDP | Flannel VXLAN overlay |
| 10250 | TCP | Kubelet API |

These ports **must** be reachable between all cluster hosts.

## Image Distribution

Unlike Kind (`kind load docker-image`), k3s uses its own embedded
containerd. Images must be imported into the `k8s.io` namespace:

`deploy-cluster.sh` expects `images.tar` to exist.  It loads the tarball into
local Docker, copies the same tarball to remote hosts, runs `docker load -i`,
then imports the loaded Docker images into each k3s container's embedded
containerd.

```bash
cd docker/multi-host
OUTPUT_FILE=images.tar bash offline-pack.sh
cd ../..
bash docker/multi-host/deploy-cluster.sh
```

## Limitations

- **One agent per host** with host networking (port conflicts). Add more
  physical hosts to scale out worker nodes.
- **Privileged containers** — acceptable for dev/test but not recommended
  for production without additional hardening.
- **No HA control plane** — single k3s server. For HA, configure 3+ servers
  with embedded etcd.
- **Kubeconfig** is written to `~/.kube/config` on the execution host,
  overwriting any existing config. Back up your kubeconfig first if needed.

## Troubleshooting

```bash
# Server logs
ssh 192.168.36.128 "docker logs k3s-server --tail 50"

# Agent logs
ssh 192.168.36.129 "docker logs k3s-agent-1 --tail 50"

# Check Flannel
kubectl logs -n kube-system -l app=flannel --tail 20

# Check k3s node status
kubectl get nodes -o wide

# Verify cross-node pod communication
kubectl run test --image=busybox --restart=Never --rm -it -- \
  wget -qO- http://<pod-ip-on-other-node>
```
