#!/usr/bin/env bash
# Tests for scripts/tier3/lib.sh that run on any machine (make
# test-scripts, Plan 6d1): the guard's Tier 3 refusals, the scale, the run
# overlay rendered by the pinned kubectl, kind.yaml decoded by the pinned
# kind, and the readers. Needs python3 and make k8s-tools.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
K8S=$PWD/.cache/k8s/bin
[ -x "$K8S/kubectl" ] && [ -x "$K8S/kind" ] || { echo "tier3 lib_test: run make k8s-tools first" >&2; exit 2; }
. scripts/tier2/lib.sh
. scripts/stack.sh
. scripts/loadgen.sh
. scripts/tier3/lib.sh

# The guard: GOIM_TEST_TIER=3 and TARGET, before anything else runs.
run_init() { (cd "$tmp/repo" && env "$@" bash -c ". '$OLDPWD/scripts/tier2/lib.sh'; . '$OLDPWD/scripts/tier3/lib.sh'; T2_MIN_CORES=1 T2_MIN_MEM_GB=1; t3_init x; echo reached" 2>&1); }
mkdir -p "$tmp/repo" && git -C "$tmp/repo" init -q
out=$(run_init GOIM_TEST_TIER=2 TARGET=local); expect "init: GOIM_TEST_TIER=2 is refused" "$?" 2
contains "init: names Tier 3" "$out" "GOIM_TEST_TIER=3"
out=$(run_init GOIM_TEST_TIER=3 TARGET=); expect "init: no TARGET is refused" "$?" 2
contains "init: names TARGET" "$out" "TARGET=local"
out=$(run_init GOIM_TEST_TIER=3 TARGET=cloud); expect "init: TARGET=cloud is refused" "$?" 2
contains "init: cloud does not exist yet" "$out" "cloud cluster"
out=$(run_init GOIM_TEST_TIER=3 TARGET=local TIER3_SCALE=2); expect "init: scale 2 is refused" "$?" 2
r=$(ls "$tmp/repo/tier3-results"/x-*/result.json 2>/dev/null | head -n 1)
expect "init: a refused run's result.json says tier 3" "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["tier"])' "$r" 2>&1)" 3

# The scale (spec: TIER3_SCALE, the per-gateway cap 250k x scale).
expect "cap: the default scale" "$(t3_cap 0.15)" 37500
expect "cap: scale 1" "$(t3_cap 1)" 250000
for s in 0 1.5 -0.1 abc ""; do t3_cap "$s" >/dev/null; expect "cap: '$s' is refused" "$?" 1; done

# The run overlay, rendered with the pinned kubectl: the local overlay with
# the run's values, every name reference rewritten to the hashed names.
mkdir -p "$tmp/k8s/keys" && echo '{"keys": []}' >"$tmp/k8s/keys/jwks.json"
t3_overlay "$tmp/k8s" 172.18.0.1:17020,172.18.0.1:17021 37500
"$K8S/kubectl" kustomize "$tmp/k8s" >"$tmp/rendered.yaml" 2>"$tmp/render.err"; expect "overlay: renders" "$?" 0
r=$(python3 - "$tmp/rendered.yaml" <<'PY'
import json, re, sys
docs = open(sys.argv[1]).read().split("\n---\n")
def has(kind, name, *subs):
    return any(f"kind: {kind}\n" in d and re.search(rf"^  name: {name}$", d, re.M) and all(s in d for s in subs) for d in docs)
cm = re.search(r"name: (goim-config-\w+)", open(sys.argv[1]).read()).group(1)
print(json.dumps({
  "config": has("ConfigMap", cm, "MAX_CONNS: \"37500\"", "REDIS_ADDRS: 172.18.0.1:17020,172.18.0.1:17021", "JWKS_URL: \"\""),
  "gw": has("StatefulSet", "gw", f"name: {cm}", "image: goim:tier3", "imagePullPolicy: Never", "goim.io/role: gateway", "secretName: noise-keys"),
  "logic": has("Deployment", "logic", f"name: {cm}", "name: jwks-fallback-", "goim.io/role: logic"),
  "client": has("Service", "gateway-client", "type: NodePort", "nodePort: 30800"),
  "ns": all("namespace: im" in d for d in docs if "kind: Namespace" not in d)}))
PY
)
expect "overlay: config, gateways, Logic, client NodePort, namespace" "$r" \
  '{"config": true, "gw": true, "logic": true, "client": true, "ns": true}'

# kind.yaml decodes strictly (kind refuses an unknown field before it
# needs docker): with a docker that fails, the error is docker's.
mkdir -p "$tmp/bin" && printf '#!/bin/sh\nexit 1\n' >"$tmp/bin/docker" && chmod +x "$tmp/bin/docker"
out=$(PATH=$tmp/bin:$PATH "$K8S/kind" create cluster --name t3-test --config scripts/tier3/kind.yaml 2>&1)
contains "kind.yaml: decodes, kind goes on to docker" "$out" "docker info"
sed 's/    labels:/    labelz:/' scripts/tier3/kind.yaml >"$tmp/bad.yaml"
out=$(PATH=$tmp/bin:$PATH "$K8S/kind" create cluster --name t3-test --config "$tmp/bad.yaml" 2>&1)
contains "kind.yaml: a typo is refused" "$out" "field labelz not found"

# t3_balance: within 10% of the mean (#150.3).
b=$(t3_balance gw-0=1000 gw-1=1050 gw-2=950 gw-3=1000 gw-4=1000 gw-5=1000)
expect "balance: 5% passes" "$(get "$b" '(d["total"], d["max_dev_pct"], d["ok"])')" "(6000, 5.0, True)"
b=$(t3_balance gw-0=1200 gw-1=960 gw-2=960 gw-3=960 gw-4=960 gw-5=960)
expect "balance: 20% fails" "$(get "$b" '(d["max_dev_pct"], d["ok"])')" "(20.0, False)"
expect "balance: zero connections fail" "$(get "$(t3_balance gw-0=0 gw-1=0)" 'd["ok"]')" False
expect "balance: no counts" "$(t3_balance)" null
expect "balance: an unreadable count" "$(t3_balance gw-0=1000 gw-1=null)" null

# t3_ipvs: the NodePort's virtual service on the node's own address.
cat >"$tmp/ip_vs" <<'IPVS'
IP Virtual Server version 1.2.1 (size=4096)
Prot LocalAddress:Port Scheduler Flags
  -> RemoteAddress:Port Forward Weight ActiveConn InActConn
TCP  0A600001:01BB lc
  -> AC120005:192B                Masq    1      0          0
TCP  AC120009:7850 lc
  -> 0AF40101:1F40                Masq    1      1001       0
  -> 0AF40201:1F40                Masq    1      999        2
TCP  0AF40001:7850 rr
  -> 0AF40101:1F40                Masq    1      0          0
IPVS
expect "ipvs: the service on the node's address" "$(t3_ipvs "$tmp/ip_vs" 172.18.0.9:30800)" \
  '{"scheduler": "lc", "servers": 2, "active": [1001, 999]}'
expect "ipvs: no such service" "$(t3_ipvs "$tmp/ip_vs" 172.18.0.8:30800)" null

echo "$fails failure(s)"
[ "$fails" -eq 0 ]
