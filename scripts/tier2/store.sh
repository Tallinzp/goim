#!/usr/bin/env bash
# Tier 2: run Plan 1's store tests against a real Redis primary + replica.
#
# Usage (on the Tier 2 host, from the repo root):
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=store
#   The host guard (scripts/tier2/lib.sh) refuses smaller hosts.
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

. "$(dirname "$0")/lib.sh"
t2_init store
t2_need docker
export GOIM_REDIS_PORT="${GOIM_REDIS_PORT:-16379}" GOIM_REDIS_REPLICA_PORT="${GOIM_REDIS_REPLICA_PORT:-16380}"
export GOIM_TEST_REDIS="127.0.0.1:$GOIM_REDIS_PORT" GOIM_TEST_REDIS_REPLICA="127.0.0.1:$GOIM_REDIS_REPLICA_PORT"
out=$T2_OUT
mutdir=""
t2_on_exit 'make redis-down'
# Exit hooks run before the report, newest first: save the Redis logs before redis-down.
t2_on_exit 'docker compose -f deploy/compose/redis-test.yml logs --no-color --tail 100 >"$out/redis.log" 2>&1'
t2_on_exit '[ -n "$mutdir" ] && git worktree remove --force "$mutdir"'

# The report: the log lines that matter (passing-test noise removed).
t2_report_extra() {
  t2_section "tools"
  echo "gcc:     $(gcc --version | head -1)"
  echo "docker:  $(docker --version); $(docker compose version 2>&1 | head -1)"
  echo "ports:   primary $GOIM_REDIS_PORT, replica $GOIM_REDIS_REPLICA_PORT"
  t2_section "redis-up.log"
  cat "$out/redis-up.log" 2>/dev/null
  t2_section "tests.log (RUN/PASS lines removed)"
  if [ -f "$out/tests.log" ]; then
    grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests.log" | head -n 3000
  fi
  t2_section "mutation.log"
  tail -n 200 "$out/mutation.log" 2>/dev/null
  t2_section "redis logs (last 100 lines each)"
  cat "$out/redis.log" 2>/dev/null
}

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

t2_duration setup $((SECONDS - T2_T0))

# 1 + 2: full suite on real Redis.
t=$SECONDS
go test -race -count=1 -v ./... >"$out/tests.log" 2>&1
tests_rc=$?
skips=$(grep -c 'TODO: Tier 2 integration' "$out/tests.log")
failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)
tests_s=$((SECONDS - t))

# 3: mutation check in a throwaway worktree, so the checkout is never modified.
# The mutant sends WAIT through node (the pooled *redis.Client) instead of the
# Conn that wrote the marker. It counts as caught only if it compiles and the
# test fails its own assertion; a build error or any other failure is an error.
t=$SECONDS
mutdir=$(mktemp -d)
git worktree add --detach "$mutdir" HEAD >/dev/null 2>&1
sed -i.orig 's/conn\.Wait(ctx, 1, s\.cfg\.WaitTimeout)/node.Wait(ctx, 1, s.cfg.WaitTimeout)/' "$mutdir/internal/store/revoke.go"
if ! grep -q 'node.Wait(ctx, 1' "$mutdir/internal/store/revoke.go"; then
  mutation="error: could not apply the mutation"
elif ! (cd "$mutdir" && go vet ./internal/store/) >"$out/mutation.log" 2>&1; then
  mutation="error: the mutant does not compile"
elif (cd "$mutdir" && go test -race -count=1 -v -run '^TestRevokeFailsWhenReplicaDoesNotAck$' ./internal/store/) >>"$out/mutation.log" 2>&1; then
  mutation="fail: test still passes with WAIT on a pooled connection"
elif grep -q -- '--- FAIL: TestRevokeFailsWhenReplicaDoesNotAck' "$out/mutation.log" &&
     grep -q 'want ErrNotReplicated' "$out/mutation.log"; then
  mutation="pass"
else
  mutation="error: the test failed for another reason, see mutation.log"
fi
mutation_s=$((SECONDS - t))

# 4: eviction policy (Execution Notes risk 12).
policy=$(docker compose -f deploy/compose/redis-test.yml exec -T redis-primary \
  redis-cli -p 6379 config get maxmemory-policy | tail -1 | tr -d '\r')

t2_field redis "$(t2_json_str "$(docker compose -f deploy/compose/redis-test.yml exec -T redis-primary redis-server --version | tr -d '\r')")"
t2_check tests "$(t2_bool [ "$tests_rc" -eq 0 ])" "\"failed\": $(t2_json_str "$failed"), \"duration_s\": $tests_s"
t2_check no_skips "$(t2_bool [ "$skips" -eq 0 ])" "\"tier2_skips\": $skips"
t2_check mutation "$(t2_bool [ "$mutation" = pass ])" "\"detail\": $(t2_json_str "$mutation"), \"duration_s\": $mutation_s"
t2_check eviction "$(t2_bool [ "$policy" = noeviction ])" "\"maxmemory_policy\": $(t2_json_str "$policy")"
t2_not_covered "Redis Cluster: MasterForKey, MOVED after failover, multi-node NOSCRIPT, CRC16 key slots" \
  "lost replies after a write (needs a fault-injecting proxy)"
t2_finish
