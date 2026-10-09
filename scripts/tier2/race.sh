#!/usr/bin/env bash
# Tier 2: run the race integration suite (Plan 5b) on the Tier 2 host. The
# binaries run as local processes (choice #47), with a redis-server primary
# and replica per scenario, the pinned 7.4.11 that make redis-server builds
# (choice #62): the same suite as the dev machine, on a host with CPU to spare.
#
# Usage (on the Tier 2 host, from the repo root): make test-tier2 SCENARIO=race
#   The host guard refuses smaller hosts.
#   Results: tier2-results/race-<timestamp>/ (result.json, tests.log, the
#   process logs of every scenario), plus ONE file to send back if anything
#   fails: tier2-results/race-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. tests:    every scenario passes (GOIM_RACE_SUITE=1, binaries built with -race)
#   2. all_ran:  every scenario ran (passed, not skipped): `go test -list` count = PASS count
#   3. mutation: with fence (a) removed (a closed connection's Logout is sent
#                before its Login reply), TestRace5CloseWhileLoginInFlight fails
#   4. redis:    the suite ran on the pinned redis-server 7.4.11
set -uo pipefail

cd "$(git rev-parse --show-toplevel)"
export CGO_ENABLED=1 GOIM_RACE_SUITE=1
export GOIM_REDIS_SERVER="$PWD/.cache/redis/bin/redis-server"
redis_version=7.4.11

# Host guard (spec: Tier 2 host is 12 cores / 26GB; never the dev machine).
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
for tool in go gcc make git curl; do
  command -v "$tool" >/dev/null || { echo "refused: $tool not installed" >&2; exit 2; }
done

out="$PWD/tier2-results/race-$(date +%Y%m%d-%H%M%S)"
report="$out.report.txt"
mkdir -p "$out/logs"
export GOIM_RACE_LOGS="$out/logs"
mutdir=""

section() { printf '\n===== %s =====\n' "$1"; }
write_report() {
  {
    echo "goim Tier 2 report: scenario race"
    section "environment"
    echo "date:    $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "commit:  $(git rev-parse HEAD) ($(git rev-parse --abbrev-ref HEAD))"
    echo "dirty:   $(git status --porcelain | wc -l) changed files"
    echo "host:    $cores cores, ${mem_gb}GB RAM, $(uname -sr)"
    echo "go:      $(go version)"
    echo "gcc:     $(gcc --version | head -1)"
    echo "redis:   $("$GOIM_REDIS_SERVER" --version 2>&1)"
    echo "ports:   suite 20000-32000; ephemeral $(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null)"
    section "result.json"
    cat "$out/result.json" 2>/dev/null || echo "(not written: the run stopped early)"
    section "redis-build.log"
    tail -n 50 "$out/redis-build.log" 2>/dev/null
    section "tests.log (RUN/PASS lines removed; failing scenarios include their process logs)"
    if [ -f "$out/tests.log" ]; then
      grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 4000
    fi
    section "mutation.log"
    tail -n 200 "$out/mutation.log" 2>/dev/null
  } >"$report" 2>&1
}
teardown() {
  write_report
  [ -n "$mutdir" ] && git worktree remove --force "$mutdir" >/dev/null 2>&1
  echo "report: $report"
}
trap teardown EXIT

if ! make redis-server >"$out/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $out/redis-build.log" >&2
  exit 1
fi

# 1 + 2: the suite. Each scenario starts its own Redis primary and replica.
listed=$(GOIM_RACE_SUITE= go test -list '.*' ./internal/racetest/ | grep -c '^Test') # no suite: lists without building the binaries
go test -count=1 -v -timeout 45m ./internal/racetest/ >"$out/tests.log" 2>&1
tests_rc=$?
passed=$(grep -cE '^--- PASS' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)

# 3: mutation check in a throwaway worktree. The mutant sends a closed
# connection's Logout as soon as it closes while its Login is in flight
# (fence (a) removed): the Logout lands before the write and a ghost stays.
mutdir=$(mktemp -d)
if ! git worktree add --detach "$mutdir" HEAD >"$out/mutation.log" 2>&1; then
  rmdir "$mutdir" 2>/dev/null
  mutdir=""
  mutation="error: git worktree add failed, see mutation.log"
elif ! sed -i.orig 's/if c.outcome == loginOK || c.fenceB { \/\/ fence (a) or (b)/if c.outcome == loginOK || c.outcome == loginPending || c.fenceB { \/\/ MUTANT: no fence (a)/' "$mutdir/internal/gateway/gateway.go" ||
  ! grep -q 'MUTANT: no fence (a)' "$mutdir/internal/gateway/gateway.go"; then
  mutation="error: could not apply the mutation"
elif ! (cd "$mutdir" && go vet ./internal/gateway/) >>"$out/mutation.log" 2>&1; then
  mutation="error: the mutant does not compile"
elif (cd "$mutdir" && GOIM_RACE_LOGS= go test -count=1 -v -run '^TestRace5CloseWhileLoginInFlight$' ./internal/racetest/) >>"$out/mutation.log" 2>&1; then
  mutation="fail: race 5 still passes without fence (a)"
elif grep -q -- '--- FAIL: TestRace5CloseWhileLoginInFlight' "$out/mutation.log" && grep -q 'no ghost\|fenced Logout' "$out/mutation.log"; then
  mutation="pass"
else
  mutation="error: the test failed for another reason, see mutation.log"
fi

# 4: the Redis the suite reported it ran (TestMain prints it).
ran_on=$(grep -m1 '^racetest: .*: Redis ' "$out/tests.log" | sed 's/.*: Redis //')

ok=true
[ "$tests_rc" -eq 0 ] || ok=false
[ "$passed" -eq "$listed" ] || ok=false
[ "$mutation" = "pass" ] || ok=false
[ "$ran_on" = "$redis_version" ] || ok=false

cat >"$out/result.json" <<JSON
{
  "scenario": "race",
  "tier": 2,
  "commit": "$(git rev-parse HEAD)",
  "host": {"cores": $cores, "mem_gb": $mem_gb},
  "pass": $ok,
  "checks": {
    "tests": {"pass": $([ "$tests_rc" -eq 0 ] && echo true || echo false), "failed": "$failed"},
    "all_ran": {"pass": $([ "$passed" -eq "$listed" ] && echo true || echo false), "listed": $listed, "passed": $passed},
    "mutation": {"pass": $([ "$mutation" = "pass" ] && echo true || echo false), "detail": "$mutation"},
    "redis": {"pass": $([ "$ran_on" = "$redis_version" ] && echo true || echo false), "want": "$redis_version", "ran_on": "$ran_on"}
  },
  "not_covered": ["Redis Cluster (one primary + one replica here)", "a real network between the processes (all on 127.0.0.1)"]
}
JSON

cat "$out/result.json"
$ok && exit 0 || exit 1
