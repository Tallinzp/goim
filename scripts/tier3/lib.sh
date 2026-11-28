# shellcheck shell=bash
# lib.sh: what Tier 3 TARGET=local scenarios share (Plan 6d1, #149, #150):
# gateway and Logic in a kind cluster on the Tier 2 host, the Redis Cluster
# and loadgen on the host. Source it after scripts/tier2/lib.sh, stack.sh
# and loadgen.sh, then t3_init SCENARIO, t3_build, t3_cluster_up,
# t3_redis_up, t3_deploy, t3_probes, t3_sources N (each below).
# Settings: TARGET (local; cloud is refused until a cloud cluster exists),
# TIER3_SCALE (default 0.15: the per-gateway cap is 250k x scale).

T3_CLUSTER=goim-t3
T3_NODE_IMAGE=kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5
# The probe image is busybox's file system imported as a new local image.
# kind load cannot import the pulled busybox from Docker's containerd store
# (a blob is missing), and a "FROM busybox" build is the same image
# (6d1 host runs 2 and 3). An imported image holds only local content.
T3_PROBE_BASE=busybox:1.37.0
T3_PROBE_IMAGE=goim-probe:tier3
T3_NODEPORT=30800
T3_SOURCE_CIDR=198.18.0.0/24
# Gateway node k on one Zen 5c core, both SMT siblings (#150.1); the
# control plane and the Logic node share T2_CPUS_LOGIC with Redis.
T3_GW_CPUS=(4,16 5,17 6,18 7,19 8,20 9,21)
T3_NOT_COVERED=("the real NLB's RST/FIN pass-through, flow-hash spreading and deregistration delay"
  "cloud network latency" "the 1M absolute scale")

# t3_cap SCALE: the per-gateway cap 250,000 x SCALE, rounded; fails unless
# 0 < SCALE <= 1.
t3_cap() {
  awk -v s="$1" 'BEGIN { if (s !~ /^(0|1)?(\.[0-9]+)?$/ || s + 0 <= 0 || s + 0 > 1) exit 1; printf "%d\n", 250000 * s + 0.5 }'
}

t3_init() {
  T2_TIER=3
  t2_init "$1"
  case ${TARGET:-} in
    local) ;;
    cloud) t2_refuse "refused: TARGET=cloud needs a cloud cluster, which does not exist yet (spec: Tier 3 target)" ;;
    *) t2_refuse "refused: set TARGET=local or TARGET=cloud, e.g. GOIM_TEST_TIER=3 make test-tier3 SCENARIO=$1 TARGET=local" ;;
  esac
  T3_SCALE=${TIER3_SCALE:-0.15}
  T3_CAP=$(t3_cap "$T3_SCALE") || t2_refuse "refused: TIER3_SCALE=$T3_SCALE, want 0 < scale <= 1"
  t2_field target '"local"'
  t2_field scale "$T3_SCALE"
  t2_not_covered "${T3_NOT_COVERED[@]}"
  t2_need docker curl python3 sudo
  sudo -n -l /usr/local/sbin/goim-srcaddr >/dev/null 2>&1 || t2_refuse "refused: sudo -n /usr/local/sbin/goim-srcaddr is not allowed (run once: sudo scripts/tier2/host-setup.sh)"
  docker info >/dev/null 2>&1 || t2_refuse "refused: docker info fails (is the daemon up and usable without sudo?)"
  K8S_NODE_IMAGE=$T3_NODE_IMAGE scripts/k8s-tools.sh .cache/k8s >"$T2_OUT/k8s-tools.log" 2>&1 || t2_refuse "refused: scripts/k8s-tools.sh failed, see $T2_OUT/k8s-tools.log"
  PATH=$PWD/.cache/k8s/bin:$PATH
  # kind writes the context to the run's kubeconfig, not ~/.kube/config.
  export KUBECONFIG=$T2_OUT/kubeconfig STACK_DIR=$T2_OUT/stack STACK_PIN=1
}

# t3_build: host binaries; the image goim:tier3 (static, #150.5).
t3_build() {
  stack_build || return 1
  mkdir -p "$T2_OUT/image"
  CGO_ENABLED=0 go build -trimpath -o "$T2_OUT/image/" ./cmd/gateway ./cmd/logic || return 1
  docker build -q -t goim:tier3 -f deploy/k8s/Dockerfile "$T2_OUT/image" || return 1
  docker image inspect "$T3_PROBE_BASE" >/dev/null 2>&1 || docker pull -q --platform linux/amd64 "$T3_PROBE_BASE" || return 1
  local cid
  cid=$(docker create --platform linux/amd64 "$T3_PROBE_BASE") || return 1
  docker export "$cid" | docker import -c 'ENV PATH=/bin:/usr/bin:/sbin:/usr/sbin' - "$T3_PROBE_IMAGE" >/dev/null
  local rc=$?
  docker rm "$cid" >/dev/null
  return "$rc"
}

# t3_stop WHAT LOG: stops the run (exit 1) after a setup step failed; the
# report and result.json name the step, and the report shows LOG's tail.
t3_stop() {
  T3_STOP_WHAT=$1 T3_STOP_LOG=$2
  t2_field stopped_at "$(t2_json_str "$1")"
  echo "$1 failed, see $2" >&2
  exit 1
}

# t3_cluster_up: a fresh cluster, nodes pinned before our pods start.
# Sets T3_BRIDGE (the kind bridge) and T3_HOST (its address).
t3_cluster_up() {
  t2_on_exit 't3_cluster_down'
  kind delete cluster --name "$T3_CLUSTER" >/dev/null 2>&1
  kind create cluster --name "$T3_CLUSTER" --image "$T3_NODE_IMAGE" --config scripts/tier3/kind.yaml --wait 0s || return 1
  local i=0 n
  for n in $(kubectl get nodes -l goim.io/role=gateway -o name | sort -V); do
    docker update --cpuset-cpus "${T3_GW_CPUS[i]}" "${n#node/}" >/dev/null || return 1
    i=$((i + 1))
  done
  [ "$i" -eq 6 ] || { echo "tier3: $i gateway nodes, want 6" >&2; return 1; }
  for n in $(kubectl get nodes -l 'goim.io/role!=gateway' -o name); do
    docker update --cpuset-cpus "$T2_CPUS_LOGIC" "${n#node/}" >/dev/null || return 1
  done
  kind load docker-image --name "$T3_CLUSTER" goim:tier3 "$T3_PROBE_IMAGE" || return 1
  kubectl wait --for=condition=Ready nodes --all --timeout=300s >/dev/null || return 1
  local id
  id=$(docker network inspect kind -f '{{.Id}}') || return 1
  T3_BRIDGE=br-${id:0:12}
  T3_HOST=$(ip -4 -o addr show dev "$T3_BRIDGE" | awk '{ split($4, a, "/"); if (a[1] !~ /^198\.18\.0\./) { print a[1]; exit } }')
  [ -n "$T3_HOST" ] || { echo "tier3: no IPv4 address on $T3_BRIDGE" >&2; return 1; }
}

# t3_cluster_down: logs and events, then the cluster, aliases and Redis.
t3_cluster_down() {
  local p
  mkdir -p "$STACK_DIR/logs/k8s"
  kubectl --request-timeout=20s get events -A --sort-by=.lastTimestamp >"$STACK_DIR/logs/k8s/events.txt" 2>&1
  for p in $(kubectl -n im get pods -o name 2>/dev/null); do
    kubectl --request-timeout=20s -n im logs "$p" >"$STACK_DIR/logs/k8s/${p#pod/}.log" 2>&1
  done
  kind delete cluster --name "$T3_CLUSTER"
  [ -n "${T3_BRIDGE:-}" ] && sudo -n /usr/local/sbin/goim-srcaddr del "$T3_BRIDGE"
  stack_down
}

# t3_redis_up: the Cluster, also on T3_HOST; sets T3_REDIS_ADDRS.
t3_redis_up() {
  local base=${STACK_PORT_BASE:-17000} j
  mkdir -p "$STACK_DIR/logs" "$STACK_DIR/redis"
  # Exported for the whole run: a node restarted later keeps this address.
  export REDIS_CLUSTER_HOST=$T3_HOST
  stack_redis_cluster_up "$STACK_REDIS_SERVER" "$T2_CPUS_LOGIC" || return 1
  T3_REDIS_ADDRS=""
  for j in 0 1 2 3 4 5; do T3_REDIS_ADDRS+="${T3_REDIS_ADDRS:+,}$T3_HOST:$((base + 20 + j))"; done
}

# t3_overlay DIR REDIS_ADDRS MAX_CONNS: the run's kustomization: the local
# overlay (by a relative path; kustomize refuses an absolute one), the
# goim-config values, DIR/keys/jwks.json as jwks-fallback (no JWKS URL:
# Logic runs on the fallback set). Not the Secret noise-keys: kustomize
# drops a data key named 1, the key_id file name (t3_deploy makes it).
t3_overlay() {
  cat >"$1/kustomization.yaml" <<YAML
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: im
resources:
  - $(realpath --relative-to="$1" deploy/k8s/overlays/local)
configMapGenerator:
  - name: goim-config
    literals:
      - REDIS_ADDRS=$2
      - JWKS_URL=
      - MAX_CONNS=$3
  - name: jwks-fallback
    files:
      - jwks.json=keys/jwks.json
YAML
}

# t3_deploy: new keys, the run overlay, apply, rollout; sets T3_NOISE_PUBLIC.
t3_deploy() {
  local k=$T2_OUT/k8s
  mkdir -p "$k/keys"
  T3_NOISE_PUBLIC=$("$STACK_DIR/bin/gateway" noise-keygen -key-id 1 -out "$k/keys/noise-1") || return 1
  T3_NOISE_PUBLIC=${T3_NOISE_PUBLIC##* public }
  "$STACK_DIR/bin/devtoken" keygen -kid k1 -out "$k/keys/jwt.jwk" || return 1
  "$STACK_DIR/bin/devtoken" jwks -out "$k/keys/jwks.json" "$k/keys/jwt.jwk" || return 1
  t3_overlay "$k" "$T3_REDIS_ADDRS" "$T3_CAP"
  kubectl kustomize "$k" >"$k/rendered.yaml" || return 1
  kubectl -n im create secret generic noise-keys --from-file=1="$k/keys/noise-1" --dry-run=client -o yaml >"$k/noise-keys.yaml" || return 1
  # In order: the Namespace (kustomize puts it first), then the Secret.
  kubectl apply -f "$k/rendered.yaml" -f "$k/noise-keys.yaml" >"$k/apply.log" || return 1
  kubectl -n im rollout status statefulset/gw --timeout=300s && kubectl -n im rollout status deployment/logic --timeout=300s
}

# t3_probes: a busybox pod in monitoring (admitted to 9090, #85) and in
# goim-other (not), on the Logic node (the others are tainted).
t3_probes() {
  local ns
  for ns in monitoring goim-other; do
    kubectl create namespace "$ns" >/dev/null &&
      kubectl -n "$ns" run probe --image="$T3_PROBE_IMAGE" --image-pull-policy=Never --restart=Never --command -- sleep 86400 >/dev/null || return 1
  done
  for ns in monitoring goim-other; do
    kubectl -n "$ns" wait --for=condition=Ready pod/probe --timeout=120s >/dev/null || return 1
  done
}

t3_exec() { local ns=$1; shift; timeout 20 kubectl -n "$ns" exec probe -- "$@"; }
# T3_TCP: run in a probe as sh -c "$T3_TCP" IP PORT; exit 0 when the TCP
# connect succeeds. busybox nc may be the small one (no -z, waits for the
# server to close), so: stdin stays open, and nc alive after its 3s
# connect timeout means connected.
T3_TCP='sleep 6 | nc -w 3 "$0" "$1" >/dev/null 2>&1 & p=$!; sleep 4; kill $p 2>/dev/null && exit 0; wait $p'
t3_metrics() { t3_exec monitoring wget -q -T 5 -O - "http://$1:9090/metrics"; }

# t3_sources N: N aliases (goim-srcaddr); stack.env names one target, the
# Logic node's NodePort, where IPVS picks the gateway (#150.2). Sets T3_LB.
t3_sources() {
  sudo -n /usr/local/sbin/goim-srcaddr add "$T3_BRIDGE" "$1" || return 1
  T3_LB=$(kubectl get nodes -l goim.io/role=logic -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}'):$T3_NODEPORT
  cat >"$STACK_DIR/stack.env" <<ENV
GATEWAY_ADDRS=$T3_LB
GATEWAY_IDS=lb
NOISE_KEY_ID=1
NOISE_PUBLIC=$T3_NOISE_PUBLIC
JWT_KEY=$T2_OUT/k8s/keys/jwt.jwk
ENV
}

# t3_ipvs PROC_NET_IP_VS ADDR: the IPVS service for ADDR (IP:PORT) in a
# /proc/net/ip_vs listing: {"scheduler", "servers", "active": [ActiveConn
# per real server]}, or null.
t3_ipvs() {
  python3 - "$1" "$2" <<'PY'
import json, socket, struct, sys
ip, port = sys.argv[2].rsplit(":", 1)
want = "%08X:%04X" % (struct.unpack("!I", socket.inet_aton(ip))[0], int(port))
svc = cur = None
for line in open(sys.argv[1]):
    f = line.split()
    if len(f) >= 3 and f[0] in ("TCP", "UDP"):
        cur = {"scheduler": f[2], "servers": 0, "active": []} if f[:2] == ["TCP", want] else None
        svc = svc or cur
    elif cur is not None and len(f) >= 6 and f[0] == "->":
        cur["servers"] += 1
        cur["active"].append(int(f[4]))
print(json.dumps(svc))
PY
}

# t3_balance COUNTS: from "NAME=N" words, {"total", "mean", "max_dev_pct",
# "ok"}: ok when every count is within 10% of the mean (#150.3; R5S-O4's
# 10% on a least-connections LB); null without counts or with a non-number.
t3_balance() {
  python3 - "$@" <<'PY'
import json, sys
try:
    c = {k: int(v) for k, v in (a.split("=", 1) for a in sys.argv[1:])}
except ValueError:
    c = {}
if not c:
    print("null"); sys.exit()
mean = sum(c.values()) / len(c)
dev = max(abs(v - mean) for v in c.values()) / mean * 100 if mean else 100.0
print(json.dumps({"counts": c, "total": sum(c.values()), "mean": round(mean, 1),
                  "max_dev_pct": round(dev, 2), "ok": mean > 0 and dev <= 10}))
PY
}
