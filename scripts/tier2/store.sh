#!/usr/bin/env bash
# Tier 2: run Plan 1's store tests against a real Redis primary + replica.
#
# Usage (on the Tier 2 host, from the repo root): scripts/tier2/store.sh
#   GOIM_TEST_TIER=2 is set by this script; the host guard refuses smaller hosts.
#   Results: tier2-results/store-<timestamp>/ (result.json and the raw logs), plus
#   ONE file to send back if anything fails: tier2-results/store-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. tests:    `make test-redis` passes (every test on real Redis 7.4, race detector on)
#   2. no_skips: no test was skipped as "TODO: Tier 2 integration"
#   3. mutation: with Revoke's WAIT moved to a pooled connection,
#                TestRevokeFailsWhenReplicaDoesNotAck fails (the test catches the bug)
#   4. eviction: the test Redis reports maxmemory-policy noeviction
set -uo pipefail

cd "$(git rev-parse --show-toplevel)"
export GOIM_TEST_TIER=2 CGO_ENABLED=1
export GOIM_REDIS_PORT="${GOIM_REDIS_PORT:-16379}" GOIM_REDIS_REPLICA_PORT="${GOIM_REDIS_REPLICA_PORT:-16380}"
export GOIM_TEST_REDIS="127.0.0.1:$GOIM_REDIS_PORT" GOIM_TEST_REDIS_REPLICA="127.0.0.1:$GOIM_REDIS_REPLICA_PORT"

# Host guard (spec: Tier 2 host is 12 cores / 26GB; never the dev machine).
# Works on Linux and macOS; a host it cannot measure is refused, never let through.
cores=$(getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null)
if [ -r /proc/meminfo ]; then
  mem_gb=$(awk '/MemTotal/ {printf "%d", $2 / 1024 / 1024}' /proc/meminfo)
else
  mem_bytes=$(sysctl -n hw.memsize 2>/dev/null)
  mem_gb=$(( ${mem_bytes:-0} / 1024 / 1024 / 1024 ))
fi
for v in "$cores" "$mem_gb"; do
  case "$v" in
    ''|*[!0-9]*|0) echo "refused: cannot determine cores/RAM (cores='$cores', mem_gb='$mem_gb')" >&2; exit 2 ;;
  esac
done
if [ "$cores" -lt 12 ] || [ "$mem_gb" -lt 24 ]; then
  echo "refused: Tier 2 needs >= 12 cores and >= 24GB RAM (have $cores cores, ${mem_gb}GB)" >&2
  exit 2
fi
for tool in docker go gcc make git; do
  command -v "$tool" >/dev/null || { echo "refused: $tool not installed" >&2; exit 2; }
done

out="tier2-results/store-$(date +%Y%m%d-%H%M%S)"
report="$out.report.txt"
mkdir -p "$out"
mutdir=""

section() { printf '\n===== %s =====\n' "$1"; }
# write_report gathers everything needed to diagnose a failure into one file:
# environment, result.json, and the log lines that matter (passing-test noise removed).
write_report() {
  {
    echo "goim Tier 2 report: scenario store"
    section "environment"
    echo "date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit:  $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD))"
    echo "dirty:   $(git status --porcelain | wc -l) changed files"
    echo "host:    $cores cores, ${mem_gb}GB RAM, $(uname -sr)"
    echo "go:      $(go version)"
    echo "gcc:     $(gcc --version | head -1)"
    echo "docker:  $(docker --version); $(docker compose version 2>&1 | head -1)"
    echo "ports:   primary $GOIM_REDIS_PORT, replica $GOIM_REDIS_REPLICA_PORT"
    section "result.json"
    cat "$out/result.json" 2>/dev/null || echo "(not written: the run stopped early)"
    section "redis-up.log"
    cat "$out/redis-up.log" 2>/dev/null
    section "tests.log (RUN/PASS lines removed)"
    if [ -f "$out/tests.log" ]; then
      grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 3000
    fi
    section "mutation.log"
    tail -n 200 "$out/mutation.log" 2>/dev/null
    section "redis logs (last 100 lines each)"
    docker compose -f deploy/compose/redis-test.yml logs --no-color --tail 100 2>&1
  } >"$report" 2>&1
}
teardown() {
  write_report
  [ -n "$mutdir" ] && git worktree remove --force "$mutdir" >/dev/null 2>&1
  make redis-down >/dev/null 2>&1
  echo "report: $report"
}
trap teardown EXIT

make redis-down >/dev/null 2>&1   # idempotent: start from a clean Redis

# The host ports must be free; name whatever holds them.
for port in "$GOIM_REDIS_PORT" "$GOIM_REDIS_REPLICA_PORT"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    {
      echo "port $port is already in use. Pick free ports, e.g.:"
      echo "  GOIM_REDIS_PORT=26379 GOIM_REDIS_REPLICA_PORT=26380 make test-tier2 SCENARIO=store"
      echo "holder:"
      command -v lsof >/dev/null && lsof -nP -iTCP:"$port" -sTCP:LISTEN
      docker ps --filter "publish=$port" --format '  container {{.Names}} ({{.Image}}) {{.Ports}}'
    } | tee "$out/redis-up.log" >&2
    exit 1
  fi
done
if ! make redis-up >"$out/redis-up.log" 2>&1; then
  echo "redis-up failed, see $out/redis-up.log" >&2
  exit 1
fi

# 1 + 2: full suite on real Redis.
go test -race -count=1 -v ./... >"$out/tests.log" 2>&1
tests_rc=$?
skips=$(grep -c 'TODO: Tier 2 integration' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)

# 3: mutation check in a throwaway worktree, so the checkout is never modified.
mutdir=$(mktemp -d)
git worktree add --detach "$mutdir" HEAD >/dev/null 2>&1
sed -i.orig 's/conn\.Wait(ctx, 1, s\.cfg\.WaitTimeout)/s.rdb.Wait(ctx, 1, s.cfg.WaitTimeout)/' "$mutdir/internal/store/revoke.go"
if ! grep -q 's.rdb.Wait(ctx, 1' "$mutdir/internal/store/revoke.go"; then
  mutation="error: could not apply the mutation"
elif (cd "$mutdir" && go test -race -count=1 -run '^TestRevokeFailsWhenReplicaDoesNotAck$' ./internal/store/) >"$out/mutation.log" 2>&1; then
  mutation="fail: test still passes with WAIT on a pooled connection"
else
  mutation="pass"
fi

# 4: eviction policy (Execution Notes risk 12).
policy=$(docker compose -f deploy/compose/redis-test.yml exec -T redis-primary \
  redis-cli -p 6379 config get maxmemory-policy | tail -1 | tr -d '\r')

ok=true
[ "$tests_rc" -eq 0 ] || ok=false
[ "$skips" -eq 0 ] || ok=false
[ "$mutation" = "pass" ] || ok=false
[ "$policy" = "noeviction" ] || ok=false

cat >"$out/result.json" <<JSON
{
  "scenario": "store",
  "tier": 2,
  "commit": "$(git rev-parse HEAD)",
  "host": {"cores": $cores, "mem_gb": $mem_gb},
  "redis": "$(docker compose -f deploy/compose/redis-test.yml exec -T redis-primary redis-server --version | tr -d '\r')",
  "pass": $ok,
  "checks": {
    "tests": {"pass": $([ "$tests_rc" -eq 0 ] && echo true || echo false), "failed": "$failed"},
    "no_skips": {"pass": $([ "$skips" -eq 0 ] && echo true || echo false), "tier2_skips": $skips},
    "mutation": {"pass": $([ "$mutation" = "pass" ] && echo true || echo false), "detail": "$mutation"},
    "eviction": {"pass": $([ "$policy" = "noeviction" ] && echo true || echo false), "maxmemory_policy": "$policy"}
  },
  "not_covered": ["Redis Cluster: MasterForKey, MOVED after failover, multi-node NOSCRIPT, CRC16 key slots",
                  "lost replies after a write (needs a fault-injecting proxy)"]
}
JSON

cat "$out/result.json"
$ok && exit 0 || exit 1
