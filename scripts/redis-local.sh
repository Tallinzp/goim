#!/usr/bin/env bash
# Runs a command against a local Redis primary and replica: two redis-server
# processes, no Docker (Plan 5b). They take the Tier 2 compose Redis's place
# on the dev machine: the same ports (GOIM_REDIS_PORT, GOIM_REDIS_REPLICA_PORT;
# 16379 and 16380), nothing persisted, noeviction, and a replica that accepts
# DEBUG (DEBUG SLEEP is a replica that stops acknowledging). The command runs
# with GOIM_TEST_REDIS and GOIM_TEST_REDIS_REPLICA set; both processes are
# stopped when it ends, and its exit code is the script's.
#
# Usage: scripts/redis-local.sh REDIS_SERVER COMMAND...   (make test-redis-local)
set -uo pipefail
server=$1
shift
port=${GOIM_REDIS_PORT:-16379}
replica_port=${GOIM_REDIS_REPLICA_PORT:-16380}
cli="$(dirname "$server")/redis-cli"
for f in "$server" "$cli"; do
  [ -x "$f" ] || { echo "redis-local: $f not found (make redis-server)" >&2; exit 2; }
done
for p in "$port" "$replica_port"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
    echo "redis-local: port $p is in use; pick others with GOIM_REDIS_PORT / GOIM_REDIS_REPLICA_PORT" >&2
    exit 2
  fi
done

dir=$(mktemp -d)
pids=()
stop() {
  kill "${pids[@]}" 2>/dev/null
  wait "${pids[@]}" 2>/dev/null
  rm -rf "$dir"
}
trap stop EXIT
mkdir "$dir/primary" "$dir/replica"
common=(--bind 127.0.0.1 --save "" --appendonly no --maxmemory-policy noeviction
  --repl-diskless-sync yes --repl-diskless-sync-delay 0)
"$server" --port "$port" --dir "$dir/primary" "${common[@]}" >"$dir/primary.log" 2>&1 &
pids+=($!)
"$server" --port "$replica_port" --dir "$dir/replica" "${common[@]}" \
  --replicaof 127.0.0.1 "$port" --enable-debug-command yes >"$dir/replica.log" 2>&1 &
pids+=($!)

for _ in $(seq 100); do
  if "$cli" -p "$replica_port" info replication 2>/dev/null | grep -q master_link_status:up; then
    linked=1
    break
  fi
  sleep 0.1
done
if [ -z "${linked:-}" ]; then
  echo "redis-local: the replica did not link within 10s" >&2
  tail -n 20 "$dir/primary.log" "$dir/replica.log" >&2
  exit 2
fi
echo "redis-local: $("$server" --version | cut -d' ' -f3) primary 127.0.0.1:$port, replica 127.0.0.1:$replica_port"

GOIM_TEST_REDIS=127.0.0.1:$port GOIM_TEST_REDIS_REPLICA=127.0.0.1:$replica_port "$@"
