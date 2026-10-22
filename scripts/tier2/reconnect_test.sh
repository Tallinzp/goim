#!/usr/bin/env bash
# Tests for scripts/tier2/reconnect.sh's rc_series over synthetic
# rc_sampler lines (make test-scripts): the per-gateway rates, a late
# scrape, loadgen's run-queue wait, and the funnel reading and its bar
# (#133, review O3, O6); rc_sessions_cmp (review O4); and rc_heartbeats_ok
# (the final review's I-1: the ghost allowance). The scenario
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
eval "$(sed -n '/^rc_series() {/,/^}/p; /^rc_sessions_cmp() {/,/^}/p; /^rc_heartbeats_ok() {/,/^}/p' scripts/tier2/reconnect.sh)"

# gw HANDSHAKE ONLINE PENDING_MEM: one gateway's reading (its Kick closes
# = ONLINE); line MS SKEW GW0 GW1 [RQ] one sample (loadgen 10 online, the
# balancer's 5 and 6 open, 3 ghosts, run-queue wait RQ ns, default MS ms).
gw() { echo "{\"handshake\": $1, \"auth_pending\": 0, \"online\": $2, \"pending_mem\": $3, \"at_capacity\": 0, \"no_slot\": 0, \"logins_ok\": $2, \"busy\": 0, \"login_queue\": 0, \"awaiting_logout\": 0, \"logouts_done\": 0, \"logouts_dropped\": 0, \"closes_kick\": $2, \"closes_heartbeat\": 0}"; }
line() { echo "{\"ms\": $1, \"sets\": {}, \"rq\": ${5-$1000000}, \"lg\": {\"online\": 10, \"open\": {\"gw-0\": 5, \"gw-1\": 6}, \"ghosts\": 3}, \"gw\": {\"gw-0\": $3, \"gw-1\": $4}, \"ms_end\": $(($1 + $2))}"; }
get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }

# gw-0 refuses at pending_mem from 2s while gw-1, under the cap (100), has
# no handshake in progress: the funnel, for 2s in a row; at 4s gw-1 starts
# a handshake and it ends. A late scrape (2.5s) at 5s has no gateway
# readings, and the next sample's rates are taken from 5s's; then gw-1
# is at its cap: full, not idle.
{ line 1000 10 "$(gw 5 10 0)" "$(gw 1 10 0)"; line 2000 10 "$(gw 5 10 0)" "$(gw 1 10 0)"
  line 3000 10 "$(gw 5 10 100)" "$(gw 0 10 0)"; line 4000 10 "$(gw 5 10 200)" "$(gw 0 10 0)"
  line 5000 10 "$(gw 5 10 300)" "$(gw 1 10 0)"; line 6000 2500 "$(gw 5 10 900)" "$(gw 0 10 0)"
  line 9000 10 "$(gw 5 10 900)" "$(gw 0 100 0)"; } >"$tmp/f.jsonl"
s=$(rc_series "$tmp/f.jsonl" 1000 100 5)
expect "series: rows from the mark" "$(get "$s" '[r[0] for r in d["rows"]]')" "[1.0, 2.0, 3.0, 4.0, 5.0, 8.0]"
expect "series: gw-0 pending_mem per s" "$(get "$s" '[r[-1] and r[-1]["gw-0"][d["gw_columns"].index("pending_mem_per_s")] for r in d["rows"]]')" "[0.0, 100.0, 100.0, 100.0, None, 150.0]"
expect "series: open and lb_open" "$(get "$s" '[d["rows"][0][-1]["gw-1"][d["gw_columns"].index(k)] for k in ("pending", "open", "lb_open")]')" "[1, 11, 6]"
expect "series: closes by Kick per s" "$(get "$s" '[r[-1] and r[-1]["gw-1"][d["gw_columns"].index("closes_kick_per_s")] for r in d["rows"]]')" "[0.0, 0.0, 0.0, 0.0, None, 22.5]"
expect "series: ghosts and run-queue wait (1ms per ms)" "$(get "$s" '[[r[d["columns"].index(k)] for k in ("lg_ghosts", "lg_runq_ms_per_s")] for r in d["rows"][:2]]')" "[[3, 1000.0], [3, 1000.0]]"
expect "series: the funnel under the bar" "$(get "$s" 'd["funnel"]')" "{'seconds': 2.0, 'first_s': 2.0, 'longest_s': 2.0, 'bar_s': 5.0, 'over_bar': False, 'rows': [[2.0, 'gw-0', 'gw-1'], [3.0, 'gw-0', 'gw-1']]}"
expect "series: the funnel at the bar" "$(get "$(rc_series "$tmp/f.jsonl" 1000 100 2)" 'd["funnel"]["over_bar"]')" True
# At its cap a gateway with no handshake in progress is full, not idle.
expect "series: a full gateway is no funnel" "$(get "$(rc_series "$tmp/f.jsonl" 1000 10 5)" 'd["funnel"]["seconds"]')" 0.0
expect "series: unreadable" "$(rc_series "$tmp/none.jsonl" 0 100 5)" null
# A failed run-queue reading, and a total that went down, give null.
{ line 1000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 5000000; line 2000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" null
  line 3000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 9000000; line 4000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 1000; } >"$tmp/q.jsonl"
expect "series: run-queue gaps are null" "$(get "$(rc_series "$tmp/q.jsonl" 1000 100 5)" '[r[d["columns"].index("lg_runq_ms_per_s")] for r in d["rows"]]')" "[None, None, None]"

# rc_sessions_cmp: 7 clients on gw-0/gw-1, the mark at 1000: one good on
# each gateway, one missing, one with two sessions, one on the wrong
# gateway, one logged in before the mark, one offline at loadgen.
echo '{"uid_prefix": "u", "gateways": ["gw-0", "gw-1"], "gw": [0, 1, 0, 1, 0, 1, -1]}' >"$tmp/clients.json"
printf '%s\n' "1|gw-0|7|1|dev|1|1500|0" "1|gw-1|7|2|dev|1|1200|0" "0|" "2|gw-0|7|4|dev|1|1300|0" "1|gw-1|7|5|dev|1|1300|0" \
  "1|gw-1|7|6|dev|1|900|0" "1|gw-0|7|7|dev|1|1300|0" >"$tmp/sessions.txt"
c=$(rc_sessions_cmp "$tmp/clients.json" "$tmp/sessions.txt" 1000)
expect "sessions: counts" "$(get "$c" '[d[k] for k in ("clients", "found", "missing", "extra", "wrong_gateway", "stale", "offline")]')" "[7, 2, 1, 1, 1, 1, 1]"
expect "sessions: examples" "$(get "$c" 'd["examples"]')" "[['u-2', 'missing'], ['u-3', 'extra'], ['u-4', 'wrong_gateway'], ['u-5', 'stale'], ['u-6', 'offline']]"
head -n 3 "$tmp/sessions.txt" >"$tmp/short.txt"
expect "sessions: too few lines" "$(rc_sessions_cmp "$tmp/clients.json" "$tmp/short.txt" 1000)" null
{ cat "$tmp/sessions.txt"; echo "ERR something"; } >"$tmp/long.txt"
expect "sessions: too many lines" "$(rc_sessions_cmp "$tmp/clients.json" "$tmp/long.txt" 1000)" null

# rc_heartbeats_ok: gw-0 kept 5 ghosts and Kicked 3 of them, so 2
# heartbeat closes are its ghosts. A Pong left unread at the ghost instant
# made loadgen count one swept ghost as a kick (4 kick, 1 silent): the
# allowance comes from the gateway's Kicks, not loadgen's labels.
g='{"gw-0": {"closes_kick": 3, "closes_heartbeat": 2}}'
gh='{"open": 0, "closed": {"gw-0": {"kick": 4, "silent": 1}}}'
expect "heartbeats: ghosts the Kicks missed" "$(rc_heartbeats_ok '{"lost": 5}' "$g" 5 "$gh" ghost)" true
expect "heartbeats: one more is a timeout" "$(rc_heartbeats_ok '{"lost": 5}' '{"gw-0": {"closes_kick": 3, "closes_heartbeat": 3}}' 5 "$gh" ghost)" false
expect "heartbeats: none allowed after a reset" "$(rc_heartbeats_ok '{"lost": 5}' "$g" 5 null reset)" false
expect "heartbeats: a Pong timeout" "$(rc_heartbeats_ok '{"lost": 5, "pong-timeout": 1}' "$g" 5 "$gh" ghost)" false
# Run 31: gw-0's Reconcile closed 4,119 ghosts as not_owner (with a Kick
# frame). Those are not heartbeat closes: 5 ghosts, 3 Kicked, 1 not_owner,
# so only 1 heartbeat close is allowed.
expect "heartbeats: not_owner closes are no heartbeat allowance" "$(rc_heartbeats_ok '{"lost": 5}' '{"gw-0": {"closes_kick": 3, "closes_not_owner": 1, "closes_heartbeat": 2}}' 5 "$gh" ghost)" false
expect "heartbeats: kick, not_owner and heartbeat closes" "$(rc_heartbeats_ok '{"lost": 5}' '{"gw-0": {"closes_kick": 3, "closes_not_owner": 1, "closes_heartbeat": 1}}' 5 "$gh" ghost)" true
expect "heartbeats: a failed reading" "$(rc_heartbeats_ok null "$g" 5 "$gh" ghost)" false

[ "$fails" -eq 0 ] && echo "reconnect_test: all passed" || { echo "reconnect_test: $fails failed"; exit 1; }
