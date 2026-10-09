#!/usr/bin/env bash
# Tier 2: run the race integration suite (Plan 5b) on the Tier 2 host. The
# binaries run as local processes (choice #47), with a redis-server primary
# and replica per scenario, the pinned 7.4.11 that make redis-server builds
# (choice #62): the same suite as the dev machine, on a host with CPU to spare.
#
# Usage (on the Tier 2 host, from the repo root):
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race
#   The host guard (scripts/tier2/lib.sh) refuses smaller hosts.
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

. "$(dirname "$0")/lib.sh"
t2_init race
t2_need curl
export GOIM_RACE_SUITE=1
export GOIM_REDIS_SERVER="$PWD/.cache/redis/bin/redis-server"
redis_version=7.4.11
out=$T2_OUT
mkdir -p "$out/logs"
export GOIM_RACE_LOGS="$out/logs"
mutdir=""
t2_on_exit '[ -n "$mutdir" ] && git worktree remove --force "$mutdir"'

t2_report_extra() {
  t2_section "tools"
  echo "gcc:     $(gcc --version | head -1)"
  echo "redis:   $("$GOIM_REDIS_SERVER" --version 2>&1)"
  echo "ports:   suite 20000-32000; ephemeral $(cat /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null)"
  t2_section "redis-build.log"
  tail -n 50 "$out/redis-build.log" 2>/dev/null
  t2_section "tests.log (RUN/PASS lines removed; failing scenarios include their process logs)"
  if [ -f "$out/tests.log" ]; then
    grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 4000
  fi
  t2_section "mutation.log"
  tail -n 200 "$out/mutation.log" 2>/dev/null
}

if ! make redis-server >"$out/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $out/redis-build.log" >&2
  exit 1
fi

t2_duration setup $((SECONDS - T2_T0))

# 1 + 2: the suite. Each scenario starts its own Redis primary and replica.
t=$SECONDS
listed=$(GOIM_RACE_SUITE= go test -list '.*' ./internal/racetest/ | grep -c '^Test') # no suite: lists without building the binaries
go test -count=1 -v -timeout 45m ./internal/racetest/ >"$out/tests.log" 2>&1
tests_rc=$?
passed=$(grep -cE '^--- PASS' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)
tests_s=$((SECONDS - t))

# 3: mutation check in a throwaway worktree. The mutant sends a closed
# connection's Logout as soon as it closes while its Login is in flight
# (fence (a) removed): the Logout lands before the write and a ghost stays.
t=$SECONDS
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
mutation_s=$((SECONDS - t))

# 4: the Redis the suite reported it ran (TestMain prints it).
ran_on=$(grep -m1 '^racetest: .*: Redis ' "$out/tests.log" | sed 's/.*: Redis //')

t2_check tests "$(t2_bool [ "$tests_rc" -eq 0 ])" "\"failed\": $(t2_json_str "$failed"), \"duration_s\": $tests_s"
t2_check all_ran "$(t2_bool [ "$passed" -eq "$listed" ])" "\"listed\": $listed, \"passed\": $passed"
t2_check mutation "$(t2_bool [ "$mutation" = pass ])" "\"detail\": $(t2_json_str "$mutation"), \"duration_s\": $mutation_s"
t2_check redis "$(t2_bool [ "$ran_on" = "$redis_version" ])" "\"want\": \"$redis_version\", \"ran_on\": $(t2_json_str "$ran_on")"
t2_not_covered "Redis Cluster (one primary + one replica here)" "a real network between the processes (all on 127.0.0.1)"
t2_finish
