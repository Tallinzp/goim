#!/usr/bin/env bash
# Runs a command against a local Redis Cluster (Plan 6c1, P5bI-4): 3 masters
# and 3 replicas from scripts/redis-cluster.sh, no Docker, on ports
# GOIM_REDIS_CLUSTER_PORT (16400) + 0..5, cluster bus + 10..15. Every node
# accepts DEBUG (Revoke's WAIT test puts a replica to sleep). The command
# runs with GOIM_TEST_REDIS_CLUSTER set to the 6 addresses; the processes are
# stopped when it ends, and its exit code is the script's.
#
# Usage: scripts/redis-cluster-local.sh REDIS_SERVER COMMAND...
#   make test-redis-cluster-local (dev machine), make test-redis-cluster (Tier 2 host)
set -uo pipefail
. "$(dirname "$0")/redis-cluster.sh"
server=$1
shift
base=${GOIM_REDIS_CLUSTER_PORT:-16400}
cli="$(dirname "$server")/redis-cli"
for f in "$server" "$cli"; do
  [ -x "$f" ] || { echo "redis-cluster-local: $f not found (make redis-server)" >&2; exit 2; }
done
for ((i = 0; i < 6; i++)); do
  for p in $((base + i)) $((base + 10 + i)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
      echo "redis-cluster-local: port $p is in use; pick another base with GOIM_REDIS_CLUSTER_PORT" >&2
      exit 2
    fi
  done
done

dir=$(mktemp -d)
pids=()
stop() {
  kill "${pids[@]}" 2>/dev/null
  wait "${pids[@]}" 2>/dev/null
  rm -rf "$dir"
}
trap stop EXIT
ports=() addrs=""
for ((i = 0; i < 6; i++)); do
  mkdir "$dir/$i"
  redis_cluster_args $((base + i)) $((base + 10 + i)) "$dir/$i"
  "$server" "${REDIS_CLUSTER_ARGS[@]}" --enable-debug-command yes >"$dir/$i.log" 2>&1 &
  pids+=($!)
  ports+=($((base + i)))
  addrs+="${addrs:+,}127.0.0.1:$((base + i))"
done
if ! redis_cluster_join "$cli" "${ports[@]}"; then
  tail -n 5 "$dir"/*.log >&2
  exit 2
fi
echo "redis-cluster-local: $("$server" --version | cut -d' ' -f3) cluster $addrs"

GOIM_TEST_REDIS_CLUSTER=$addrs "$@"
