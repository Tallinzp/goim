#!/usr/bin/env bash
# Tests for scripts/tier2/gwconns.sh's readers (make test-scripts): the
# shard rule (gc_shards: the set's master's p99 at most max(2x, + floor)
# each other master's, #147.2, #148.3, #148.6), the end check (gc_end: one ReconcileEnd at the
# first attempt, every uid scanned, the rate at least 90% of the delete
# rate, #147.3), the counter wait (gc_wait_inc) and the probe reader
# (gc_probe_latency: the probe's round-trip p99 per master in a window). The scenario itself
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
eval "$(sed -n '/^gc_shards() {/,/^}/p; /^gc_end() {/,/^}/p; /^gc_wait_inc() {/,/^}/p; /^gc_probe_latency() {/,/^}/p; /^gc_probe_errors() {/,/^}/p; /^gc_probe() {/,/^}/p' scripts/tier2/gwconns.sh)"

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

# gc_probe_latency over a probe file ("MS NAME US"): redis-0..2 at MS
# 1000..1009 (US base+1..base+10), a sample at 999 and one at 1010 that
# the window [1000, 1010) must ignore.
probe="$tmp/probe.tsv"
for n in 0 1 2; do
  for i in $(seq 0 9); do echo "$((1000 + i)) redis-$n $((100 * (n + 1) + i + 1))"; done
  echo "999 redis-$n 9999"; echo "1010 redis-$n 9999"
done >"$probe"
pl=$(gc_probe_latency "$probe" 1000 1010)
expect "probe: per master in the window" "$(get "$pl" '[(n, v["role"], v["cmds"]["ping"]["calls"], v["cmds"]["ping"]["p50_us"], v["cmds"]["ping"]["p99_us"], v["cmds"]["ping"]["p999_us"], v["cmds"]["ping"]["errors"]) for n, v in sorted(d.items())]')" "[('redis-0', 'master', 10, 105, 110, 110, 0), ('redis-1', 'master', 10, 205, 210, 210, 0), ('redis-2', 'master', 10, 305, 310, 310, 0)]"
printf '%s\n' "1000 redis-0 100" "1001 redis-0 -1" "1002 redis-0 -1" "1003 redis-0 300" >"$tmp/err.tsv"
expect "probe: errors counted, not timed" "$(get "$(gc_probe_latency "$tmp/err.tsv" 1000 1010)" 'd["redis-0"]["cmds"]["ping"]')" "{'calls': 2, 'p50_us': 100, 'p99_us': 300, 'p999_us': 300, 'errors': 2}"
grep -v ' redis-2 ' "$probe" >"$tmp/two.tsv"
pl=$(gc_probe_latency "$tmp/two.tsv" 1000 1010)
expect "probe: a master with no sample" "$(get "$pl" 'd.get("redis-2")')" None
printf '%s\n' "1000 redis-2 -1" >>"$tmp/two.tsv"
pl=$(gc_probe_latency "$tmp/two.tsv" 1000 1010)
expect "probe: a master with only errors" "$(get "$pl" 'd["redis-2"]["cmds"]["ping"]')" "{'calls': 0, 'p50_us': None, 'p99_us': None, 'p999_us': None, 'errors': 1}"
expect "probe: no good sample fails the rule" "$(get "$(gc_shards "$pl" redis-1 ping 50)" 'd["ok"], d["calls"]["redis-2"]')" "(False, 0)"
pp() { printf '%s\n' "1000 redis-0 120" "1000 redis-1 $1" "1000 redis-2 150" >"$tmp/p.tsv"; gc_shards "$(gc_probe_latency "$tmp/p.tsv" 1000 1001)" redis-1 ping 50; }
expect "probe: through gc_shards, 240 within 2 x 120" "$(get "$(pp 240)" 'd["ok"], d["bound_us"]')" "(True, 240)"
expect "probe: through gc_shards, 241 over" "$(get "$(pp 241)" 'd["ok"]')" False
expect "probe: errors per master" "$(gc_probe_errors "$(gc_probe_latency "$tmp/err.tsv" 1000 1010)")" "{\"redis-0\": 2}"
expect "probe: unreadable file" "$(gc_probe_latency "$tmp/none.tsv" 1000 1010)" null

# gc_wait_inc: readings null, 3, 3, then 4 over a start of 3.
printf '%s\n' null 3 3 4 >"$T2_TMP_READS"
stack_metric() { local v; v=$(head -n 1 "$T2_TMP_READS"); sed -i 1d "$T2_TMP_READS"; echo "${v:-null}"; }
sleep() { :; }
expect "wait: until the sum exceeds the start" "$(gc_wait_inc gw-0 m x 3 5 && echo done; cat "$T2_TMP_READS")" done
printf '%s\n' null 1 >"$T2_TMP_READS"
expect "wait: a null start counts as 0" "$(gc_wait_inc gw-0 m x null 5 && echo done)" done
SECONDS=0
expect "wait: fails after SECONDS" "$(gc_wait_inc gw-0 m x 3 0 || echo timeout)" timeout

# gc_probe started as the scenario starts it (a bash -c running the
# function): killing the PID it leaves stops the probe itself, not only
# its bash (re-review of fix wave 1).
bash -c "$(declare -f gc_probe); gc_probe \"\$@\"" _ "$tmp/probe-kill.tsv" redis-0=127.0.0.1:1 2>/dev/null &
pid=$!
command sleep 1
kill "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
command sleep 0.5
expect "probe: killing its PID stops it" "$(pgrep -f "$tmp/probe-kill.tsv" | wc -l)" 0
pkill -f "$tmp/probe-kill.tsv" 2>/dev/null
expect "probe: it wrote failed round trips" "$(awk '$3 == -1' "$tmp/probe-kill.tsv" | head -n 1 | wc -l)" 1

if [ "$fails" -eq 0 ]; then echo "gwconns_test: all passed"; else echo "gwconns_test: $fails failure(s)"; exit 1; fi
