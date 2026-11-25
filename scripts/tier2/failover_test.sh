#!/usr/bin/env bash
# Tests for scripts/tier2/failover.sh's readers (make test-scripts): the
# zone and the replica from redis/layout, the login log per shard
# (fo_shards: the slot owner by CRC16, a window, nearest-rank p99), the
# other shards' rule (fo_other_ok, #141.2), the affected share and the wait
# for the worst time after a renewal (fo_wait_at, #139.3). The scenario
# itself runs on the Tier 2 host.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/loadgen.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
eval "$(sed -n '/^fo_zone() {/,/^}/p; /^fo_replica() {/,/^}/p; /^fo_shards() {/,/^}/p; /^fo_other_ok() {/,/^}/p; /^fo_affected() {/,/^}/p; /^fo_wait_at() {/,/^}/p' scripts/tier2/failover.sh)"

# The start layout (scripts/redis-cluster.sh): each master's replica in
# the next zone.
printf '%s\n' "redis-0 17020 master z0 -" "redis-1 17021 master z1 -" "redis-2 17022 master z2 -" \
  "redis-3 17023 replica z1 redis-0" "redis-4 17024 replica z2 redis-1" "redis-5 17025 replica z0 redis-2" >"$tmp/layout"
expect "zone: redis-0's zone, the master first" "$(fo_zone "$tmp/layout" redis-0)" "redis-0 redis-5"
expect "zone: redis-1's zone" "$(fo_zone "$tmp/layout" redis-1)" "redis-1 redis-3"
expect "replica: redis-0's" "$(fo_replica "$tmp/layout" redis-0)" redis-3
expect "replica: redis-2's" "$(fo_replica "$tmp/layout" redis-2)" redis-5

# 8 clients p-0..p-7; their sess slots (CRC16 of the uid): 7905, 3776,
# 16035, 11906, 7781, 3652, 15911, 11782, so on redis-1, 0, 2, 2, 1, 0, 2, 2.
# p-5 gave up, p-6 has no sequence yet; p-1 and p-7 retried.
printf '%s\n' "0 5460 redis-0" "5461 10922 redis-1" "10923 16383 redis-2" >"$tmp/slots"
echo '{"uid_prefix": "p", "first_ms": [1000, 1000, 2000, 2000, 5000, 2500, 0, 2900], "end_ms": [1100, 3000, 2050, 2300, 5040, 0, 0, 2960], "attempts": [1, 3, 1, 1, 1, 10, 0, 2]}' >"$tmp/logins.json"
s=$(fo_shards "$tmp/logins.json" "$tmp/slots" redis-0)
expect "shards: the target" "$(get "$s" 'd["target"], d["window"]')" "('redis-0', None)"
expect "shards: redis-0" "$(get "$s" 'd["shards"]["redis-0"]')" "{'clients': 2, 'logged_in': 1, 'gave_up': 1, 'retried': 1, 'pending': 0, 'p50_ms': 2000, 'p99_ms': 2000, 'max_ms': 2000}"
expect "shards: redis-1, nearest rank" "$(get "$s" '[d["shards"]["redis-1"][k] for k in ("clients", "retried", "p50_ms", "p99_ms")]')" "[2, 0, 40, 100]"
expect "shards: redis-2, one pending" "$(get "$s" '[d["shards"]["redis-2"][k] for k in ("clients", "logged_in", "pending", "p99_ms")]')" "[4, 3, 1, 300]"
expect "shards: others, every shard but the target" "$(get "$s" 'd["others"]')" "{'clients': 6, 'logged_in': 5, 'gave_up': 0, 'retried': 1, 'pending': 1, 'p50_ms': 60, 'p99_ms': 300, 'max_ms': 300}"
expect "shards: others counts the pending one" "$(get "$s" 'd["others"]["clients"] - d["others"]["logged_in"]')" 1
w=$(fo_shards "$tmp/logins.json" "$tmp/slots" redis-0 1000 3000)
expect "window: only the sequences started in it" "$(get "$w" '[d["others"][k] for k in ("clients", "pending", "p50_ms", "p99_ms")], d["window"]')" "([4, 0, 60, 300], [1000, 3000])"
expect "window: the target's affected share" "$(fo_affected "$w")" 1.0
expect "affected: none in the window" "$(fo_affected "$(fo_shards "$tmp/logins.json" "$tmp/slots" redis-0 9000 9999)")" null
expect "shards: an unreadable log" "$(fo_shards "$tmp/none.json" "$tmp/slots" redis-0)" null
printf '%s\n' "0 5460 redis-0" "5461 10922 redis-1" >"$tmp/short"
expect "shards: a slot with no owner" "$(fo_shards "$tmp/logins.json" "$tmp/short" redis-0)" null

# fo_other_ok: at most max(2 x before, before + floor).
o() { echo "{\"others\": {\"clients\": $1, \"gave_up\": $2, \"pending\": 0, \"p99_ms\": $3}}"; }
expect "other: 200 within 2 x 100" "$(fo_other_ok "$(o 5 0 100)" "$(o 5 0 200)" 50)" true
expect "other: 201 over 2 x 100" "$(fo_other_ok "$(o 5 0 100)" "$(o 5 0 201)" 50)" false
expect "other: 60 within 10 + 50" "$(fo_other_ok "$(o 5 0 10)" "$(o 5 0 60)" 50)" true
expect "other: 61 over 10 + 50" "$(fo_other_ok "$(o 5 0 10)" "$(o 5 0 61)" 50)" false
expect "other: a gave-up fails" "$(fo_other_ok "$(o 5 0 100)" "$(o 5 1 100)" 50)" false
expect "other: no logins fails" "$(fo_other_ok "$(o 5 0 100)" "$(o 0 0 None)" 50)" false
expect "other: no before p99 fails" "$(fo_other_ok "$(o 0 0 None)" "$(o 5 0 100)" 50)" false
expect "other: a null reading fails" "$(fo_other_ok null "$(o 5 0 100)" 50)" false

# fo_wait_at: PTTLs 15000 (too late in the interval: waits for the next
# renewal), 30000, 25000, 21200, then 20900: 9.1s after the renewal.
printf '%s\n' 15000 30000 25000 21200 20900 20800 >"$tmp/pttl"
stack_redis_node_cli() { local p; p=$(head -n 1 "$tmp/pttl"); sed -i 1d "$tmp/pttl"; echo "$p"; }
expect "wait_at: the first PTTL in (20500, 21000]" "$(fo_wait_at k redis-0 9)" 9100
expect "wait_at: polled until then" "$(cat "$tmp/pttl")" 20800

if [ "$fails" -eq 0 ]; then echo "failover_test: all passed"; else echo "failover_test: $fails failure(s)"; exit 1; fi
