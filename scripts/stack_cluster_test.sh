#!/usr/bin/env bash
# Tests for scripts/stack.sh's Redis Cluster (Plan 6c1, STACK_REDIS=cluster)
# on any machine (make test-scripts): 3 masters and 3 replicas in the zone
# layout come up pinned with the stack, Logic runs as a cluster client,
# logins land on every master, the per-node helpers read each node, the
# session check reads every client's key per slot, and stack_down stops
# every node. Needs redis-server (make redis-server) and curl.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/stack.sh
. scripts/loadgen.sh
eval "$(sed -n '/^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_sessions() {/,/^}/p' scripts/tier2/reconnect.sh)"
STACK_DIR=$(mktemp -d)
trap 'lg_stop; stack_down; rm -rf "$STACK_DIR"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }

STACK_GATEWAYS=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17600}
STACK_PIN=1
ncpu=$(getconf _NPROCESSORS_ONLN)
T2_CPUS_GATEWAY=0-$((ncpu - 1))
T2_CPUS_LOGIC=0
STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000)
b=$STACK_PORT_BASE

expect "ports: single has no Cluster ports" "$(STACK_REDIS=single stack_ports | grep -cx $((b + 20)))" 0
STACK_REDIS=cluster
expect "ports: the nodes and their buses" "$(stack_ports | grep -cxE "$((b + 20))|$((b + 25))|$((b + 30))|$((b + 35))")" 4
expect "names: redis-0..5" "$(stack_redis_names)" "redis-0 redis-1 redis-2 redis-3 redis-4 redis-5"
out=$(STACK_REDIS=bogus; stack_up 2>&1) && bad "up: STACK_REDIS=bogus must fail" ||
  case $out in *"want single or cluster"*) ok "up: an unknown STACK_REDIS is refused" ;; *) bad "up: STACK_REDIS=bogus: [$out]" ;; esac

stack_build || { echo "build failed"; exit 1; }
if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi
. "$STACK_DIR/stack.env"
expect "env: REDIS_MODE" "$REDIS_MODE" cluster
expect "env: the 6 nodes" "$REDIS_ADDR" "$(for j in 0 1 2 3 4 5; do printf '127.0.0.1:%d\n' $((b + 20 + j)); done | paste -sd, -)"
grep -q -- '-redis-cluster=true' "/proc/${STACK_PIDS[logic]}/cmdline" 2>/dev/null ||
  tr '\0' ' ' <"/proc/${STACK_PIDS[logic]}/cmdline" | grep -q -- '-redis-cluster=true' && ok "logic: a cluster client" || bad "logic: not -redis-cluster=true"
wrong=""
for n in $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
expect "pin: every node on the Logic set" "${wrong# }" ""
expect "layout: masters and replicas in zones" "$(awk '{ print $1, $3, $4, $5 }' "$STACK_DIR/redis/layout" | paste -sd, -)" \
  "redis-0 master z0 -,redis-1 master z1 -,redis-2 master z2 -,redis-3 replica z1 redis-0,redis-4 replica z2 redis-1,redis-5 replica z0 redis-2"
expect "layout: replicas follow their masters" "$(for j in 3 4 5; do stack_redis_node_cli redis-$j info replication | tr -d '\r' | sed -n 's/^master_port://p'; done | paste -sd, -)" \
  "$((b + 20)),$((b + 21)),$((b + 22))"
expect "slots: 3 ranges, 0-16383, one per master" "$(stack_redis_slots | paste -sd, -)" "0 5460 redis-0,5461 10922 redis-1,10923 16383 redis-2"

stack_redis_resetstat && ok "redis: resetstat on every node" || bad "redis: resetstat"
lg_start c -mode light -clients 30 -rate 100 -uid-prefix sc && lg_wait_online 30 30 && ok "loadgen: 30 online" || bad "loadgen: not online"
# 30 uids over 3 masters: each gets some (all on two: p ~ 1.5e-5).
expect "logins: sess keys on every master" "$(for j in 0 1 2; do [ "$(stack_redis_node_cli redis-$j dbsize)" -gt 0 ] && echo y; done | wc -l)" 3
lat=$(stack_redis_latency)
expect "latency: every node, its role and zone" "$(get "$lat" '[(n, d[n]["role"], d[n]["zone"]) for n in sorted(d)]')" \
  "[('redis-0', 'master', 'z0'), ('redis-1', 'master', 'z1'), ('redis-2', 'master', 'z2'), ('redis-3', 'replica', 'z1'), ('redis-4', 'replica', 'z2'), ('redis-5', 'replica', 'z0')]"
expect "latency: logins' scripts on every master" "$(get "$lat" 'all(d[n]["cmds"].get("evalsha", {}).get("calls", 0) > 0 for n in ("redis-0", "redis-1", "redis-2"))')" True
expect "latency: no reader's own commands" "$(get "$lat" 'any(k == "info" or k.startswith(("config", "cluster")) for n in d for k in d[n]["cmds"])')" False
mkdir -p "$STACK_DIR/artifacts"
s=$(rc_sessions 0)
expect "sessions: every client found per slot" "$(get "$s" '(d["clients"], d["found"])')" "(30, 30)"
expect "sessions: no CROSSSLOT" "$(cat "$STACK_DIR/sessions"/*.out | grep -c CROSSSLOT)" 0
stack_snapshot snap
for f in redis-0.info redis-5.info cluster-nodes.txt; do
  [ -s "$STACK_DIR/artifacts/snap/$f" ] && ok "snapshot: $f" || bad "snapshot: $f missing"
done
grep -q '^redis_version:' "$STACK_DIR/artifacts/snap/redis.info" && ok "snapshot: redis.info (foundation)" || bad "snapshot: redis.info"
lg_stop
stack_wait_idle 30 && ok "idle: the logins' Logouts settle" || bad "idle: after loadgen"

pids=("${STACK_PIDS[@]}")
stack_down
expect "down: every node's exit status" "$(grep -c '^redis-[0-5] 0$' "$STACK_DIR/exit-status")" 6
expect "health: a clean stop has no problems" "$(stack_health 2>&1)" ""
left=0
for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
expect "down: no process left" "$left" 0
expect "down: no port left" "$(stack_busy_ports)" ""

echo "$fails failure(s)"
[ "$fails" -eq 0 ]
