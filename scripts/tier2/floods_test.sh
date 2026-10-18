#!/usr/bin/env bash
# Tests for scripts/tier2/floods.sh's pure parts on any machine (make
# test-scripts): in_set, heartbeats' verdict gate, slow_verdict and
# slow_series over synthetic slow_sampler lines (6b3b3 final review I1,
# M1-M6). The scenario itself runs on the Tier 2 host. t2_kill_tree, now
# lib.sh's, is tested in lib_test.sh.
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
for f in in_set heartbeats slow_series; do
  eval "$(sed -n "/^$f() {/,/^}/p" scripts/tier2/floods.sh)"
done
eval "$(grep '^slow_verdict=' scripts/tier2/floods.sh)"

# in_set: a list that expands to no CPU (a reversed range) is not inside a
# set (M3).
in_set 0-1 0-3 && ok "in_set: inside" || bad "in_set: inside"
in_set 5 0-3 && bad "in_set: outside must fail" || ok "in_set: outside fails"
in_set 5-3 0-7 && bad "in_set: a reversed range must fail" || ok "in_set: a reversed range fails"

# heartbeats: with a verdict argument the check needs exactly "valid"; an
# empty or null verdict fails (M4). Without one (garbage) there is no gate
# and no verdict member.
t2_check() { echo "$1 $2 {$3}" >"$tmp/check"; }
STACK_DIR=$tmp LG_NAME=hb HB=30
mkdir -p "$tmp/artifacts"
echo '{"pongs": 5, "pong_missing": 0, "pong_rtt": {"max_ms": 1}, "lost": {}, "ping_gap_max_ms": 30000, "ping_tick_lag_max_ms": 1, "ping_send_max_ms": 1, "ping_send_timeouts": 0}' >"$tmp/artifacts/loadgen-hb.json"
g='{"gw-0": {"closes_heartbeat": 0}}'
hb() { heartbeats slow 5 "$g" "$@"; read -r _ pass rest <"$tmp/check"; echo "$pass $(python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("verdict", "absent"))' "$rest")"; }
expect "heartbeats: no verdict argument" "$(hb)" "true absent"
expect "heartbeats: valid" "$(hb '"valid"')" "true valid"
expect "heartbeats: saturated" "$(hb '"loadgen-saturated"')" "false loadgen-saturated"
expect "heartbeats: an empty verdict fails" "$(hb '')" "false None"
expect "heartbeats: a null verdict fails" "$(hb null)" "false None"

# slow_verdict: the order and the threshold.
w() { echo "{\"loadgen\": {\"idle_pct\": $1}, \"gateway\": {\"idle_pct\": $2}, \"logic\": {\"idle_pct\": $3}}"; }
chk() { echo "{\"ramp\": $1, \"hold\": $2}" | lg_py - "$slow_verdict"; }
expect "verdict: valid" "$(chk "$(w 50 50 50)" "$(w 50 50 50)")" '"valid"'
expect "verdict: loadgen first" "$(chk "$(w 10 10 10)" "$(w 50 50 50)")" '"loadgen-saturated"'
expect "verdict: gateways before Logic" "$(chk "$(w 50 10 10)" "$(w 50 50 50)")" '"gateway-set-saturated"'
expect "verdict: 85.0% busy is in" "$(chk "$(w 15.1 15.1 15.1)" "$(w 15 50 50)")" '"loadgen-saturated"'
expect "verdict: unread" "$(chk null "$(w 50 50 50)")" '"unread"'

# slow_series over synthetic lines. gwj NO_SLOT [CLOSES] is one gateway's
# reading; line MS SKEW_MS GW0 GW1 [LG [RQ0 RQ1]] one sample.
gwj() { echo "{\"online\": 100, \"handshake\": 1, \"auth_pending\": 1, \"pending_mem\": 0, \"no_slot\": $1, \"logins_ok\": 50, \"closes_heartbeat\": ${2:-0}}"; }
line() { echo "{\"ms\": $1, \"ticks\": [0, 0, 0], \"sets\": {}, \"lg\": ${5:-null}, \"rq\": {\"gw-0\": ${6:-0}, \"gw-1\": ${7:-0}}, \"gw\": {\"gw-0\": $3, \"gw-1\": $4}, \"ms_end\": $(($1 + $2))}"; }
col() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); c=d["columns"].index(sys.argv[2]); print([r[c] for r in d["rows"]])' "$1" "$2"; }
first() { python3 -c 'import json,sys; print(json.loads(sys.argv[1])["first_s"][sys.argv[2]])' "$1" "$2"; }

# A gateway's failed scrape: its row has no gateway readings, and the next
# row's rates are taken from the last whole sample, not one gateway's
# total against two (I1).
{ line 0 10 "$(gwj 5000)" "$(gwj 5000)"; line 1000 10 "$(gwj 5000)" null; line 2000 10 "$(gwj 5000)" "$(gwj 5000)"; } >"$tmp/i1.jsonl"
s=$(slow_series "$tmp/i1.jsonl")
expect "series: a missing gateway nulls its row" "$(col "$s" no_slot_per_s)" "[None, 0.0]"
expect "series: a missing gateway's online" "$(col "$s" online_conns)" "[None, 200]"
expect "series: a missing gateway is no refusal" "$(first "$s" no_slot)" None

# A late scrape (counters read 2.5s after the clock): its row has no
# gateway readings, and the refusals it saw are dated by the next on-time
# sample, against the last on-time one (M1).
{ line 0 10 "$(gwj 0)" "$(gwj 0)"; line 1000 2500 "$(gwj 1500)" "$(gwj 1500)"; line 4000 10 "$(gwj 1500)" "$(gwj 1500)"; line 5000 10 "$(gwj 1500)" "$(gwj 1500)"; } >"$tmp/m1.jsonl"
s=$(slow_series "$tmp/m1.jsonl")
expect "series: scrape_ms" "$(col "$s" scrape_ms)" "[2500, 10, 10]"
expect "series: a late scrape nulls its row" "$(col "$s" no_slot_per_s)" "[None, 750.0, 0.0]"
expect "series: a late scrape is dated" "$(first "$s" scrape_late)" 1.0
expect "series: refusals dated by the next on-time sample" "$(first "$s" no_slot)" 4.0

# A counter with no sample yet counts 0 before its first rise (M2).
nc() { gwj 0 | sed 's/"closes_heartbeat": 0/"closes_heartbeat": null/'; }
{ line 0 10 "$(nc)" "$(nc)"; line 1000 10 "$(nc)" "$(nc)"; line 2000 10 "$(gwj 0 4)" "$(nc)"; } >"$tmp/m2.jsonl"
s=$(slow_series "$tmp/m2.jsonl")
expect "series: a first rise from no sample" "$(col "$s" closes_heartbeat_per_s)" "[None, None]"
{ line 0 10 "$(nc)" "$(nc)"; line 1000 10 "$(nc)" "$(nc)"; line 2000 10 "$(gwj 0 4)" "$(gwj 0 0)"; } >"$tmp/m2.jsonl"
s=$(slow_series "$tmp/m2.jsonl")
expect "series: a first rise from no sample counts" "$(col "$s" closes_heartbeat_per_s)" "[None, 4.0]"
expect "series: a first rise is dated" "$(first "$s" closes_heartbeat)" 2.0

# The light clients' own refusals (pre-handshake), apart from the floods'
# no_slot (M5).
lg() { echo "{\"online\": 10, \"gave_up\": 0, \"failed\": $1, \"refused\": $2}"; }
{ line 0 10 "$(gwj 0)" "$(gwj 0)" "$(lg 0 0)"; line 1000 10 "$(gwj 900)" "$(gwj 900)" "$(lg 0 0)"; line 2000 10 "$(gwj 1800)" "$(gwj 1800)" "$(lg 6 6)"; } >"$tmp/m5.jsonl"
s=$(slow_series "$tmp/m5.jsonl")
expect "series: lg_refused_per_s" "$(col "$s" lg_refused_per_s)" "[0.0, 6.0]"
expect "series: the light clients' first refusal" "$(first "$s" light_refused)" 2.0
expect "series: the gateways' first no_slot" "$(first "$s" no_slot)" 1.0

# A run-queue total that went down (a thread gone, a restarted gateway) is
# no reading, never a negative wait (M6).
{ line 0 10 "$(gwj 0)" "$(gwj 0)" null 5000000000 5000000000; line 1000 10 "$(gwj 0)" "$(gwj 0)" null 1000000000 1000000000; line 2000 10 "$(gwj 0)" "$(gwj 0)" null 1100000000 1000000000; } >"$tmp/m6.jsonl"
s=$(slow_series "$tmp/m6.jsonl")
expect "series: no negative run-queue wait" "$(col "$s" gw_runq_ms_per_s)" "[None, 100.0]"

# A gateway's failed scrape keeps its run-queue wait, which is read beside
# it, not inside it (P6b3b3I-2): gw-1 waits 1s/s and its middle scrape fails.
{ line 0 10 "$(gwj 0)" "$(gwj 0)" null 0 0; line 1000 10 "$(gwj 0)" null null 100000000 1000000000; line 2000 10 "$(gwj 0)" "$(gwj 0)" null 200000000 2000000000; } >"$tmp/i2.jsonl"
s=$(slow_series "$tmp/i2.jsonl")
expect "series: run-queue wait through a failed scrape" "$(col "$s" gw_runq_ms_per_s)" "[1000.0, 1000.0]"
expect "series: columns" "$(python3 -c 'import json,sys; print(len(json.loads(sys.argv[1])["columns"]))' "$s")" 19

# The exit trap stops the profile subshell with its curls (M4).
grep -q "^t2_on_exit '\[ -n \"\$prof\" \] && t2_kill_tree \"\$prof\"'$" scripts/tier2/floods.sh && ok "kill_tree: the exit trap uses it" || bad "kill_tree: the exit trap doesn't use it"

[ "$fails" -eq 0 ] && echo "floods_test: all passed" || { echo "floods_test: $fails failed"; exit 1; }
