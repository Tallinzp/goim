#!/usr/bin/env bash
# Tests for scripts/stack.sh's Cluster failover helpers (Plan 6c2a) on any
# machine (make test-scripts): a SIGKILLed master fails over, the slot map
# and the session check come from a live node (review P6c1I-1), the node
# restarts with its nodes.conf and rejoins as a replica (P6c1I-2), the
# start layout comes back, a stopped zone (one master, one other master's
# replica) fails over and returns, and nothing hangs on a stopped node.
# The gateway's lease and new logins live through all of it. cluster-node-
# timeout is 2s here (a failover in ~3s). Needs redis-server (make
# redis-server) and curl.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/stack.sh
. scripts/loadgen.sh
eval "$(sed -n '/^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_sessions() {/,/^}/p' scripts/tier2/reconnect.sh)"
STACK_DIR=$(mktemp -d)
trap 'lg_stop_all; stack_down; rm -rf "$STACK_DIR"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
missing() { stack_metric gw-0 goim_gateway_lease_calls_total 'result="missing"'; }

STACK_GATEWAYS=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17700}
STACK_PIN=1
ncpu=$(getconf _NPROCESSORS_ONLN)
T2_CPUS_GATEWAY=0-$((ncpu - 1))
T2_CPUS_LOGIC=0
STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000)
STACK_REDIS=cluster
export REDIS_CLUSTER_NODE_TIMEOUT_MS=2000
start_roles='{"redis-0": "master", "redis-1": "master", "redis-2": "master", "redis-3": "replica", "redis-4": "replica", "redis-5": "replica"}'

expect "default node timeout: 10s (#139)" "$(unset REDIS_CLUSTER_NODE_TIMEOUT_MS; . scripts/redis-cluster.sh; redis_cluster_args 1 2 d; echo "${REDIS_CLUSTER_ARGS[@]}" | grep -o 'cluster-node-timeout [0-9]*')" "cluster-node-timeout 10000"

stack_build || { echo "build failed"; exit 1; }
if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi
expect "roles: the start layout" "$(stack_redis_roles)" "$start_roles"
lg_in a lg_start a -mode light -clients 30 -rate 100 -uid-prefix fa && lg_in a lg_wait_online 30 30 && ok "loadgen: 30 online" || bad "loadgen: not online"
key=$(stack_lease_key gw-0)
case $key in gwlease:gw-0:\{[0-9]*\}) ok "lease key: $key" ;; *) bad "lease key: [$key]" ;; esac
expect "lease key: owned by a master" "$(stack_redis_owner "$key" | grep -cE '^redis-[012]$')" 1
expect "owner: a fixed key's slot (gwepochs, 5482) on redis-1" "$(stack_redis_owner gwepochs)" redis-1
m0=$(missing)

# A SIGKILLed master: the slot map now comes from redis-1 (P6c1I-1), its
# replica takes over, the lease and new logins live on.
stack_redis_kill redis-0 && ok "kill: redis-0 reaped" || bad "kill: redis-0"
expect "kill: recorded as expected" "$(grep -c '^redis-0 expected:137$' "$STACK_DIR/exit-status")" 1
expect "live: the first node that answers" "$(stack_redis_live)" redis-1
expect "slots: 3 ranges from a live node" "$(stack_redis_slots | wc -l)" 3
expect "role: a killed node is down" "$(stack_redis_role redis-0)" down
t=$SECONDS
stack_redis_wait_role redis-3 master 15 && ok "failover: redis-3 is master after $((SECONDS - t))s" || bad "failover: redis-3 not master"
until stack_redis_slots | grep -q '^0 5460 redis-3$' || [ $((SECONDS - t)) -gt 15 ]; do sleep 0.2; done
expect "slots: redis-0's range on redis-3" "$(stack_redis_slots | paste -sd, -)" "0 5460 redis-3,5461 10922 redis-1,10923 16383 redis-2"
lg_in b lg_start b -mode light -clients 30 -rate 100 -uid-prefix fb && lg_in b lg_wait_online 30 30 && ok "logins: 30 more online after the failover" || bad "logins: after the failover"
out=$(stack_redis_wait_whole 2 2>&1) && bad "whole: a dead node must fail the wait" || ok "whole: not whole with a node dead"

# The restart rejoins redis-0 as redis-3's replica (its nodes.conf kept);
# then the start layout comes back.
stack_redis_restart redis-0 && ok "restart: redis-0 answers" || bad "restart: redis-0"
stack_redis_wait_role redis-0 replica 10 && ok "restart: redis-0 rejoins as a replica" || bad "restart: redis-0 is $(stack_redis_role redis-0)"
expect "restart: it follows redis-3" "$(stack_redis_node_cli redis-0 info replication | tr -d '\r' | sed -n 's/^master_port://p')" $((STACK_PORT_BASE + 23))
[ -s "$STACK_DIR/logs/redis-0.log.1" ] && ok "restart: the first log kept" || bad "restart: logs/redis-0.log.1 missing"
stack_redis_wait_whole 30 && ok "whole: after the restart" || bad "whole: after the restart"
stack_redis_restore 30 && ok "restore: done" || bad "restore"
expect "restore: the start layout" "$(stack_redis_roles)" "$start_roles"
expect "restore: redis-0's range back" "$(stack_redis_slots | head -n 1)" "0 5460 redis-0"
expect "sessions: every client of a and b found after the failover" "$(get "$(lg_in a rc_sessions 0)" 'd["found"]'),$(get "$(lg_in b rc_sessions 0)" 'd["found"]')" "30,30"

# Zone z0 stopped (redis-0, a master, and redis-5, redis-2's replica): no
# reader hangs on it, redis-3 takes over, and on SIGCONT the zone returns.
stack_redis_stop redis-0 && stack_redis_stop redis-5 && ok "zone: z0 stopped"
t=$SECONDS
expect "role: a stopped node is down" "$(stack_redis_role redis-0)" down
lat=$(stack_redis_latency)
expect "latency: the stopped nodes are null, the others read" "$(get "$lat" '[n for n in sorted(d) if d[n] is None]')" "['redis-0', 'redis-5']"
[ $((SECONDS - t)) -le 6 ] && ok "zone: readers bounded ($((SECONDS - t))s)" || bad "zone: readers took $((SECONDS - t))s"
out=$(stack_redis_wait_whole 2 2>&1) && bad "whole: a stopped zone must fail the wait" || ok "whole: not whole with a zone stopped"
stack_redis_wait_role redis-3 master 15 && ok "zone: redis-3 is master" || bad "zone: redis-3 not master"
expect "zone: a key of redis-2's range (slot 16287) still on redis-2" "$(stack_redis_owner '{x}')" redis-2
stack_redis_cont redis-0 && stack_redis_cont redis-5 && ok "zone: z0 back"
stack_redis_wait_role redis-0 replica 15 && ok "zone: the old master returns as a replica" || bad "zone: redis-0 is $(stack_redis_role redis-0)"
stack_redis_restore 60 && ok "restore: after the zone" || bad "restore: after the zone"
expect "restore: the start layout again" "$(stack_redis_roles)" "$start_roles"
expect "lease: no missing reply through both failovers" "$(missing)" "$m0"
lg_in c lg_start c -mode light -clients 30 -rate 100 -uid-prefix fc && lg_in c lg_wait_online 30 30 && ok "logins: 30 more online after the zone" || bad "logins: after the zone"

# A stopped node at stack_down is resumed and stops cleanly.
stack_redis_stop redis-4
lg_stop_all
stack_down
expect "down: every node stops with 0" "$(grep -cE '^redis-[0-5] 0$' "$STACK_DIR/exit-status")" 6
expect "health: the kill was expected, nothing else" "$(stack_health 2>&1)" ""

echo "$fails failure(s)"
[ "$fails" -eq 0 ]
