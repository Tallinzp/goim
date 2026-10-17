#!/usr/bin/env bash
# Tier 1a load probe (spec: Test execution model, "small load probe (≤ 5k
# connections)"; Plan 6a2): a real stack of 2 gateways (scripts/stack.sh,
# unpinned, production timing) and cmd/loadgen, on the dev machine.
#
#   1. light: 3,000 connections log in, Ping every 5s for 15s: every Ping
#      answered, no connection lost
#   2. floods (Plan 6b3a): beside 2,000 light clients, loadgen instance
#      flood sends garbage handshakes at 2,000/s for 10s, then runs again
#      holding 500 slow-handshake connections beside flood2's 500 slow-auth
#      ones: the gateways fail and close every garbage handshake, close
#      every slow connection by its 5s timeout (held p99 ≤ 6.5s), and the
#      light clients keep every Ping answered
#   3. sdk:   2,000 SDK clients log in; loadgen SIGKILLs gw-1 (POST /kill):
#      99% of its clients log in again within 30s of T_death, and the
#      killed gateway is the only process that did not stop cleanly
#
# Usage: make test-load-probe (needs redis-server from make redis-server,
# curl and python3). Exit 0 if every check passes, else 1. The run's
# directory (logs, loadgen results, snapshots) is kept on failure, or with
# PROBE_KEEP=1.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/stack.sh
. scripts/loadgen.sh
STACK_DIR=$(mktemp -d -t goim-probe.XXXXXX)
STACK_GATEWAYS=2
STACK_PORT_BASE=${STACK_PORT_BASE:-17200}
STACK_GATEWAY_ARGS=(-max-conns 6000 -pending-cap 6000 -logout-queue 6000)
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
check() { if [ "$2" = true ]; then ok "$1"; else bad "$1: $3"; fi; }
cleanup() {
  lg_stop_all
  stack_down
  if [ "$fails" -eq 0 ] && [ "${PROBE_KEEP:-0}" != 1 ]; then rm -rf "$STACK_DIR"; else echo "probe: results in $STACK_DIR"; fi
}
trap cleanup EXIT

stack_build >"$STACK_DIR/build.log" 2>&1 || { bad "build: see $STACK_DIR/build.log"; exit 1; }
mkdir -p "$STACK_DIR/logs"
stack_up 2>"$STACK_DIR/stack-up.err" || { bad "stack: $(cat "$STACK_DIR/stack-up.err")"; exit 1; }
ok "stack: 2 gateways Ready"

# 1: light.
lg_start light -mode light -clients 3000 -rate 1000 -ping-every 5s -uid-prefix light || { bad "light: loadgen did not start"; exit 1; }
if lg_wait_online 3000 60; then ok "light: 3000 online"; else bad "light: not all online"; fi
check "light: spread over both gateways" "$(lg_get /stats 'min(d["online_by_gateway"].values()) >= 1400')" "$(lg_get /stats 'd["online_by_gateway"]')"
sleep 15
stack_snapshot light
lg_stop
check "light: loadgen exited 0" "$([ "$LG_STATUS" = 0 ] && echo true)" "status $LG_STATUS"
check "light: every Ping answered" "$(lg_json 'd["pong_missing"] == 0 and d["pongs"] >= 3000 * 2')" \
  "pongs $(lg_json 'd["pongs"]'), missing $(lg_json 'd["pong_missing"]')"
check "light: none lost while held" "$(lg_json 'd["lost"] == {}')" "$(lg_json 'd["lost"]')"
echo "     light: login p99 $(lg_json 'd["login_latency"]["p99_ms"]')ms, Pong p99 $(lg_json 'd["pong_rtt"]["p99_ms"]')ms, failed $(lg_json 'd["failed"]')"

# 2: floods beside light clients, each flood on its own loadgen instance
# from its own source addresses; instance flood runs garbage, then
# slow-handshake (instances are named by role and reused, #120).
# gw_sum METRIC MATCH: over both gateways; a labelled counter has no sample
# before its first increment, which counts as 0. A gateway whose metrics
# cannot be read prints 0 and fails, so the caller counts a FAIL.
gw_sum() {
  local gw body vals=()
  for gw in gw-0 gw-1; do
    body=$(stack_metrics "$gw") || { echo "gw_sum: $gw's metrics unreadable" >&2; echo 0; return 1; }
    vals+=("$(stack_metric_in "$body" "$1" "$2")")
  done
  python3 -c 'import sys; print(sum(0 if x == "null" else int(float(x)) for x in sys.argv[1:]))' "${vals[@]}" ||
    { echo 0; return 1; }
}
lg_start floods-light -mode light -clients 2000 -rate 1000 -ping-every 5s -uid-prefix floods || { bad "floods: light loadgen did not start"; exit 1; }
if lg_wait_online 2000 60; then ok "floods: 2000 light clients online"; else bad "floods: light clients not all online"; fi
failed0=$(gw_sum goim_gateway_handshakes_total 'result="failed"') || bad "garbage: gateway metrics unreadable"
limited0=$(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_rate"') || bad "garbage: gateway metrics unreadable"
lg_in flood lg_start garbage -mode garbage -rate 2000 -clients 500 -source-net 1 || bad "garbage: loadgen did not start"
sleep 10
lg_in flood lg_stop
check "garbage: loadgen exited 0" "$([ "${LG_STATUSES[flood]}" = 0 ] && echo true)" "status ${LG_STATUSES[flood]}"
outcomes=$(lg_in flood lg_json 'd["flood"]["outcomes"]')
closed=$(lg_in flood lg_json 'd["flood"]["outcomes"].get("closed", 0)')
check "garbage: ≥ 15000 attempts, every one counted closed by a gateway" "$(lg_in flood lg_true "$STACK_DIR/artifacts/loadgen-garbage.json" 'set(d["flood"]["outcomes"]) == {"closed"} and d["flood"]["outcomes"]["closed"] >= 15000')" "$outcomes"
failed=$(gw_sum goim_gateway_handshakes_total 'result="failed"') || bad "garbage: gateway metrics unreadable"
limited=$(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_rate"') || bad "garbage: gateway metrics unreadable"
failed=$((failed - failed0)) limited=$((limited - limited0))
check "garbage: the gateways failed (or limited) as many handshakes" "$([ $((failed + limited)) -ge "$closed" ] && [ "$failed" -gt 0 ] && echo true)" "failed $failed + handshake_rate $limited, closed $closed"
echo "     garbage: $(lg_in flood lg_json 'd["flood"]["attempts"]') attempts, $(lg_in flood lg_json 'd["flood"]["skipped"]') skipped, held p50 $(lg_in flood lg_json 'd["flood"]["held"]["p50_ms"]')ms p99 $(lg_in flood lg_json 'd["flood"]["held"]["p99_ms"]')ms; gateways: failed $failed, handshake_rate $limited"
hst0=$(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_timeout"') || bad "slow: gateway metrics unreadable"
aut0=$(gw_sum goim_gateway_admission_rejects_total 'reason="auth_timeout"') || bad "slow: gateway metrics unreadable"
lg_in flood lg_start slowhs -mode slow-handshake -clients 500 -rate 500 -source-net 2 || bad "slow-handshake: loadgen did not start"
lg_in flood2 lg_start slowauth -mode slow-auth -clients 500 -rate 500 -source-net 3 || bad "slow-auth: loadgen did not start"
for inst in flood flood2; do
  lg_in "$inst" lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= 500 and d["flood"]["open"] >= 450' 30 ||
    bad "$inst: not 500 closed with 450 open again: $(lg_in "$inst" lg_get /stats 'd["flood"]')"
done
lg_in flood lg_stop
lg_in flood2 lg_stop
for inst in slowhs slowauth; do
  check "$inst: held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway" "$(lg_true "$STACK_DIR/artifacts/loadgen-$inst.json" 'set(d["flood"]["outcomes"]) == {"closed"} and d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500')" \
    "$(lg_py "$STACK_DIR/artifacts/loadgen-$inst.json" 'd["flood"]')"
done
echo "     slow: held p50/p99/max: slow-handshake $(lg_in flood lg_json '[d["flood"]["held"][k] for k in ("p50_ms", "p99_ms", "max_ms")]')ms, slow-auth $(lg_in flood2 lg_json '[d["flood"]["held"][k] for k in ("p50_ms", "p99_ms", "max_ms")]')ms"
hst=$(gw_sum goim_gateway_admission_rejects_total 'reason="handshake_timeout"') || bad "slow: gateway metrics unreadable"
aut=$(gw_sum goim_gateway_admission_rejects_total 'reason="auth_timeout"') || bad "slow: gateway metrics unreadable"
hst=$((hst - hst0)) aut=$((aut - aut0))
check "slow: the gateways closed them by their timeouts" "$([ "$hst" -ge 500 ] && [ "$aut" -ge 500 ] && echo true)" "handshake_timeout $hst, auth_timeout $aut"
lg_stop
check "floods: light loadgen exited 0" "$([ "$LG_STATUS" = 0 ] && echo true)" "status $LG_STATUS"
check "floods: every light Ping answered through the floods" "$(lg_json 'd["pong_missing"] == 0 and d["pongs"] >= 2000 * 3 and d["lost"] == {}')" \
  "pongs $(lg_json 'd["pongs"]'), missing $(lg_json 'd["pong_missing"]'), lost $(lg_json 'd["lost"]')"
echo "     floods: light Pong p99 $(lg_json 'd["pong_rtt"]["p99_ms"]')ms, max $(lg_json 'd["pong_rtt"]["max_ms"]')ms"

# 3: sdk, and a gateway killed by loadgen.
lg_start sdk -mode sdk -clients 2000 -rate 500 -uid-prefix sdk || { bad "sdk: loadgen did not start"; exit 1; }
if lg_wait_online 2000 60; then ok "sdk: 2000 online"; else bad "sdk: not all online"; fi
reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - '"population %d" % d["population"]')
# Not in $(...): stack_expect_dead reaps gw-1 and forgets it in this shell.
if stack_expect_dead gw-1; then ok "sdk: loadgen killed gw-1 ($reply)"; else bad "sdk: gw-1 not killed: $reply"; fi
lg_wait_back death 45 || true
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
check "sdk: 99% of gw-1's clients back within 30s of T_death" "$(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["pass_from_mark"] and d["population"] >= 900')" \
  "$(cat "$STACK_DIR/artifacts/slo-death.json")"
echo "     sdk: population $(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["population"]'), from T_death p50 $(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["from_mark"]["p50_ms"]')ms p99 $(lg_py "$STACK_DIR/artifacts/slo-death.json" 'd["from_mark"]["p99_ms"]')ms"
check "sdk: all 2000 online on gw-0 again" "$(lg_get /stats 'd["online_by_gateway"]["gw-0"] == 2000')" "$(lg_get /stats 'd["online_by_gateway"]')"
lg_stop
check "sdk: loadgen exited 0" "$([ "$LG_STATUS" = 0 ] && echo true)" "status $LG_STATUS"
check "sdk: the result holds the death SLO" "$(lg_json '"death" in d["slos"]')" "no slos.death"

stack_down
problems=$(stack_health)
check "processes: only the killed gateway stopped uncleanly, and it is expected" "$([ -z "$problems" ] && echo true)" "$problems"

[ "$fails" -eq 0 ] && echo "load-probe: all passed" || { echo "load-probe: $fails failed"; exit 1; }
