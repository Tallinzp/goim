# shellcheck shell=bash
# redis-cluster.sh: a Redis Cluster of local redis-server processes (Plan 6c1,
# choice #66): 3 masters and 3 replicas, no Docker. Zones are labels only
# (spec: Redis placement, R5S-O9): master i is in zone zi, its replica in
# zone z((i+1)%3), so one zone holds one master and one other master's
# replica. Source it; the caller starts the processes (stack.sh in its own
# process groups, redis-cluster-local.sh in the background):
#
#   redis_cluster_layout          # "NODE ROLE ZONE MASTER" per node: redis-0..2 masters, redis-3..5 replicas
#   redis_cluster_args PORT BUS DIR   # sets REDIS_CLUSTER_ARGS: one node's redis-server flags
#   redis_cluster_join CLI PORT... # the 6 ports in layout order: create, attach replicas, wait until
#                                 #   every node reports cluster_state:ok and every replica is linked
#
# Settings: REDIS_CLUSTER_NODE_TIMEOUT_MS (cluster-node-timeout, default
# 15000, Redis's default; Plan 6c2 measures the failover it gives).

redis_cluster_layout() {
  printf '%s\n' "redis-0 master z0 -" "redis-1 master z1 -" "redis-2 master z2 -" \
    "redis-3 replica z1 redis-0" "redis-4 replica z2 redis-1" "redis-5 replica z0 redis-2"
}

# redis_cluster_args PORT BUS DIR: nothing persisted, noeviction (as the
# single Redis), the cluster bus on BUS (not PORT + 10000, which lands in
# the race suite's 20000-32000), nodes.conf in DIR.
redis_cluster_args() {
  REDIS_CLUSTER_ARGS=(--port "$1" --cluster-port "$2" --bind 127.0.0.1 --dir "$3"
    --save "" --appendonly no --maxmemory-policy noeviction
    --cluster-enabled yes --cluster-config-file nodes.conf
    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-15000}"
    --repl-diskless-sync yes --repl-diskless-sync-delay 0)
}

# redis_cluster_join CLI P0 P1 P2 P3 P4 P5: P0-P2 become the masters (slots
# split evenly by redis-cli), P3-P5 the replicas of P0-P2 in that order.
# Fails, naming what did not happen, after 30s.
redis_cluster_join() {
  local cli=$1 i p id ok
  shift
  local ports=("$@")
  [ ${#ports[@]} -eq 6 ] || { echo "redis-cluster: want 6 ports, got ${#ports[@]}" >&2; return 1; }
  for p in "${ports[@]}"; do
    ok=""
    for i in $(seq 100); do
      "$cli" -p "$p" ping >/dev/null 2>&1 && { ok=1; break; }
      sleep 0.1
    done
    [ -n "$ok" ] || { echo "redis-cluster: no answer on port $p within 10s" >&2; return 1; }
  done
  "$cli" --cluster create "127.0.0.1:${ports[0]}" "127.0.0.1:${ports[1]}" "127.0.0.1:${ports[2]}" \
    --cluster-yes >/dev/null || { echo "redis-cluster: --cluster create failed" >&2; return 1; }
  for i in 0 1 2; do
    id=$("$cli" -p "${ports[i]}" cluster myid)
    "$cli" --cluster add-node "127.0.0.1:${ports[i + 3]}" "127.0.0.1:${ports[0]}" \
      --cluster-slave --cluster-master-id "$id" >/dev/null ||
      { echo "redis-cluster: replica on port ${ports[i + 3]} not added" >&2; return 1; }
  done
  for i in $(seq 300); do
    ok=1
    # Every node must also see the 3 replicas: gossip spreads the new roles
    # after the replicas link, and a client that loads the slot map from a
    # node that has not heard yet sees 6 masters.
    for p in "${ports[@]}"; do
      "$cli" -p "$p" cluster info 2>/dev/null | grep -q '^cluster_state:ok' || ok=""
      [ "$("$cli" -p "$p" cluster nodes 2>/dev/null | grep -cE '[ ,]slave ')" = 3 ] || ok=""
    done
    for p in "${ports[@]:3}"; do
      "$cli" -p "$p" info replication 2>/dev/null | grep -q '^master_link_status:up' || ok=""
    done
    [ -n "$ok" ] && return 0
    sleep 0.1
  done
  echo "redis-cluster: not every node reports cluster_state:ok with its replicas linked within 30s" >&2
  "$cli" -p "${ports[0]}" cluster nodes >&2
  return 1
}
