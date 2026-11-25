#!/usr/bin/env bash
# Tests for scripts/tier2/gwconns.sh's readers (make test-scripts): the
# shard rule (gc_shards: the set's master's p99 at most max(2x, + floor)
# each other master's, #147.2, #148.3, #148.6), the end check (gc_end: one ReconcileEnd at the
# first attempt, every uid scanned, the rate at least 90% of the delete
# rate, #147.3) and the counter wait (gc_wait_inc). The scenario itself
# runs on the Tier 2 host.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/loadgen.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
T2_TMP_READS=$tmp/reads
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
eval "$(sed -n '/^gc_shards() {/,/^}/p; /^gc_end() {/,/^}/p; /^gc_wait_inc() {/,/^}/p' scripts/tier2/gwconns.sh)"

# stack_redis_latency on a Cluster: masters redis-0..2 with evalsha p99
# A, B, C (µs), replicas redis-3..5; redis-1 holds the set.
lat() {
  python3 -c 'import json, sys
a, b, c = (float(x) if "." in x else int(x) for x in sys.argv[1:4])
calls, down = int(sys.argv[4]), sys.argv[5] == "down"
m = lambda z, p, n=9: {"role": "master", "zone": z, "cmds": {"evalsha": {"calls": n, "p50_us": 10, "p99_us": p, "p999_us": 900}}}
d = {"redis-0": m("z0", a), "redis-1": m("z1", b, calls), "redis-2": None if down else m("z2", c),
     "redis-3": {"role": "replica", "zone": "z1", "cmds": {"evalsha": {"calls": 9, "p50_us": 1, "p99_us": 9999, "p999_us": 9999}}},
     "redis-4": {"role": "replica", "zone": "z2", "cmds": {}}, "redis-5": None}
d["redis-1"]["cmds"]["sadd"] = {"calls": 9, "p50_us": 1, "p99_us": 400, "p999_us": 900}
print(json.dumps(d))' "$1" "$2" "$3" "${4:-9}" "${5:-up}"
}
s=$(gc_shards "$(lat 120 240 150)" redis-1 evalsha 50)
expect "shards: 240 within 2 x the lower other (120)" "$(get "$s" 'd["ok"], d["bound_us"]')" "(True, 240)"
expect "shards: masters only, per master" "$(get "$s" 'd["p99_us"], d["calls"]')" "({'redis-0': 120, 'redis-1': 240, 'redis-2': 150}, {'redis-0': 9, 'redis-1': 9, 'redis-2': 9})"
expect "shards: 241 over 2 x 120" "$(get "$(gc_shards "$(lat 120 241 150)" redis-1 evalsha 50)" 'd["ok"]')" False
expect "shards: against each other master, not the higher" "$(get "$(gc_shards "$(lat 120 290 150)" redis-1 evalsha 50)" 'd["ok"]')" False
expect "shards: the owner lower than the others" "$(get "$(gc_shards "$(lat 120 50 150)" redis-1 evalsha 50)" 'd["ok"]')" True
expect "shards: the owner ran no evalsha" "$(get "$(gc_shards "$(lat 120 100 150 0)" redis-1 evalsha 50)" 'd["ok"]')" False
expect "shards: a master without the command" "$(get "$(gc_shards "$(lat 120 100 150)" redis-1 sadd 50)" 'd["ok"], d["p99_us"]')" "(False, {'redis-0': None, 'redis-1': 400, 'redis-2': None})"
expect "shards: a master that did not answer" "$(get "$(gc_shards "$(lat 120 100 150 9 down)" redis-1 evalsha 50)" 'd["ok"], sorted(d["p99_us"])')" "(False, ['redis-0', 'redis-1'])"
expect "shards: the owner not a master" "$(get "$(gc_shards "$(lat 120 100 150)" redis-3 evalsha 50)" 'd["ok"]')" False
expect "shards: one bucket over 2x, within the floor" "$(get "$(gc_shards "$(lat 7.007 14.015 7.007)" redis-1 evalsha 50)" 'd["ok"], d["bound_us"]')" "(True, 57.007)"
expect "shards: over the floor" "$(get "$(gc_shards "$(lat 7.007 57.008 9.0)" redis-1 evalsha 50)" 'd["ok"]')" False
expect "shards: no floor, one bucket over 2x" "$(get "$(gc_shards "$(lat 7.007 14.015 7.007)" redis-1 evalsha 0)" 'd["ok"]')" False
expect "shards: an unreadable reading" "$(gc_shards null redis-1 evalsha 50)" null

# gc_end over gw-0's and Logic's deltas: 250k at 10k/s, a 26s phase.
gw() { echo "{\"ends_ok\": ${1:-1}, \"ends_retried\": ${2:-0}, \"ends_dropped\": ${3:-null}}"; }
lg() { echo "{\"end_s\": ${1:-26.0}, \"ends\": ${2:-1}, \"scanned\": ${3:-250000}, \"deleted\": 0}"; }
e=$(gc_end "$(gw)" "$(lg)" 250000 10000 30)
expect "end: 250k in 26s" "$(get "$e" '[d[k] for k in ("ok", "rate", "want_rate", "margin_s", "deadline_s", "deleted")]')" "[True, 9615.4, 9000.0, 4.0, 60.0, 0]"
expect "end: 9,000 uids/s is 90%" "$(get "$(gc_end "$(gw)" "$(lg 27.7778)" 250000 10000 30)" 'd["ok"]')" True
expect "end: below 90%" "$(get "$(gc_end "$(gw)" "$(lg 27.8)" 250000 10000 30)" 'd["ok"], d["rate"]')" "(False, 8992.8)"
expect "end: a retry fails" "$(get "$(gc_end "$(gw 1 1)" "$(lg)" 250000 10000 30)" 'd["ok"], d["retried"]')" "(False, 1)"
expect "end: a drop fails" "$(get "$(gc_end "$(gw 0 0 1)" "$(lg)" 250000 10000 30)" 'd["ok"]')" False
expect "end: no reply" "$(get "$(gc_end "$(gw null)" "$(lg null 0 0)" 250000 10000 30)" 'd["ok"], d["rate"], d["margin_s"]')" "(False, None, None)"
expect "end: two phases" "$(get "$(gc_end "$(gw 2)" "$(lg 26.0 2 500000)" 250000 10000 30)" 'd["ok"]')" False
expect "end: an earlier phase in Logic's window" "$(get "$(gc_end "$(gw)" "$(lg 52.0 2 500000)" 250000 10000 30)" 'd["ok"]')" False
expect "end: not every uid scanned" "$(get "$(gc_end "$(gw)" "$(lg 20.0 1 249999)" 250000 10000 30)" 'd["ok"]')" False
expect "end: over the grace is recorded only" "$(get "$(gc_end "$(gw)" "$(lg)" 250000 10000 25)" 'd["ok"], d["margin_s"]')" "(True, -1.0)"
expect "end: an unreadable delta" "$(gc_end null "$(lg)" 250000 10000 30)" null

# gc_wait_inc: readings null, 3, 3, then 4 over a start of 3.
printf '%s\n' null 3 3 4 >"$T2_TMP_READS"
stack_metric() { local v; v=$(head -n 1 "$T2_TMP_READS"); sed -i 1d "$T2_TMP_READS"; echo "${v:-null}"; }
sleep() { :; }
expect "wait: until the sum exceeds the start" "$(gc_wait_inc gw-0 m x 3 5 && echo done; cat "$T2_TMP_READS")" done
printf '%s\n' null 1 >"$T2_TMP_READS"
expect "wait: a null start counts as 0" "$(gc_wait_inc gw-0 m x null 5 && echo done)" done
SECONDS=0
expect "wait: fails after SECONDS" "$(gc_wait_inc gw-0 m x 3 0 || echo timeout)" timeout

if [ "$fails" -eq 0 ]; then echo "gwconns_test: all passed"; else echo "gwconns_test: $fails failure(s)"; exit 1; fi
