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
#   redis_cluster_join CLI PORT... # the 6 ports in layout order: create, attach replicas, then
#                                 #   redis_cluster_wait
#   redis_cluster_wait CLI SECONDS PORT...  # until every node reports cluster_state:ok, sees 3
#                                 #   replicas and no failed node, and every replica is linked
#
# Settings: REDIS_CLUSTER_NODE_TIMEOUT_MS (cluster-node-timeout, default
# 10000: a SIGKILLed master fails over in ~16.5s, inside the lease rule's
# TTL 30s - interval 10s; Redis's 15000 took 20.6-22.7s, #139);
# REDIS_CLUSTER_JOIN_TIMEOUT_S (how long the join waits, default 30).

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
    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-10000}"
    --repl-diskless-sync yes --repl-diskless-sync-delay 0)
}

# redis_cluster_join CLI P0 P1 P2 P3 P4 P5: P0-P2 become the masters (slots
# split evenly by redis-cli), P3-P5 the replicas of P0-P2 in that order.
# Fails, naming what did not happen, after REDIS_CLUSTER_JOIN_TIMEOUT_S.
redis_cluster_join() {
  local cli=$1 i p id ok deadline
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
  redis_cluster_wait "$cli" "${REDIS_CLUSTER_JOIN_TIMEOUT_S:-30}" "${ports[@]}"
}

# redis_cluster_wait CLI SECONDS PORT...: waits until every node answers,
# reports cluster_state:ok, sees 3 replicas and no node flagged fail or
# fail?, and every node that is a replica now has its master link up: the
# join's end, and a Cluster whole again after a failover (Plan 6c2a). Each
# call is bounded by 1s, so a stopped node fails the wait, not hangs it.
# Fails, naming what did not happen, after SECONDS.
redis_cluster_wait() {
  local cli=$1 secs=$2 p ok deadline
  shift 2
  deadline=$((SECONDS + secs))
  while :; do
    ok=1
    # Every node must also see the 3 replicas: gossip spreads the new roles
    # after the replicas link, and a client that loads the slot map from a
    # node that has not heard yet sees 6 masters.
    for p in "$@"; do
      timeout 1 "$cli" -p "$p" cluster info 2>/dev/null | grep -q '^cluster_state:ok' || { ok=""; break; }
      timeout 1 "$cli" -p "$p" cluster nodes 2>/dev/null | awk '$3 ~ /(^|,)slave(,|$)/ { s++ } $3 ~ /(^|,)fail\??(,|$)/ { f++ }
        END { exit !(s == 3 && f == 0) }' || { ok=""; break; }
      if timeout 1 "$cli" -p "$p" info replication 2>/dev/null | grep -q '^role:slave'; then
        timeout 1 "$cli" -p "$p" info replication 2>/dev/null | grep -q '^master_link_status:up' || { ok=""; break; }
      fi
    done
    [ -n "$ok" ] && return 0
    [ "$SECONDS" -ge "$deadline" ] && break
    sleep 0.1
  done
  echo "redis-cluster: not every node reports cluster_state:ok with its replicas linked within ${secs}s" >&2
  timeout 1 "$cli" -p "$1" cluster nodes >&2
  return 1
}
