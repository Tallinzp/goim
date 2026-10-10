#!/usr/bin/env bash
# Tier 2: a smoke load on the Tier 2 host (Plan 6a2), so the load pipeline is
# proven end to end before Plan 6b's measurements: cmd/loadgen pinned to the
# loadgen cores (#71) from 127.0.0.2… (#69), 4 pinned gateways, a recorded
# window with CPU profiles, and the storm SLO computed by loadgen after it
# SIGKILLs a gateway. The scale is a fraction of 6b's; no capacity number is
# judged here.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=smoke
#   Results: tier2-results/smoke-<timestamp>/ (result.json, stack/ with logs,
#   artifacts and loadgen results), plus ONE file to send back:
#   tier2-results/smoke-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. stack:            redis, devtoken, Logic and gw-0..gw-3 become Ready
#   2. pinning:          gateways, Logic + Redis and loadgen on their CPU sets
#   3. light_online:     100,000 light connections log in (25,000 per gateway)
#   4. light_pongs:      a Ping every 10s for 60s: none unanswered
#   5. light_held:       no connection lost during the window
#   6. no_swap_in_light: no page swapped in during the window
#   7. sdk_online:       20,000 SDK clients log in
#   8. slo_b1:           loadgen SIGKILLs gw-3: 99% of its clients log in
#                        again within 30s of T_death
#   9. artifacts:        metrics, heap, goroutine and CPU profiles of every
#                        process, and both loadgen results
#  10. processes:        nothing but the killed gateway died, panicked or
#                        stopped uncleanly
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init smoke
t2_need curl taskset sudo python3
t2_require_host_setup
t2_mem_budget 8
t2_nofile 1048576

export STACK_DIR=$T2_OUT/stack STACK_GATEWAYS=4 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LIGHT=${SMOKE_LIGHT:-100000} SDK=${SMOKE_SDK:-20000}
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

# rss_json: resident memory in MB of loadgen and each running gateway, so
# Plan 6b sizes 600k light and 200k SDK clients per process from measured
# bytes per connection (P5I-2), not estimates.
rss_mb() {
  local v
  v=$(awk '/^VmRSS:/ { printf "%d", $2 / 1024 }' "/proc/$1/status" 2>/dev/null)
  echo "${v:-null}"
}
rss_json() {
  local out="\"loadgen\": $(rss_mb "$LG_PID")" name
  for name in "${STACK_ORDER[@]}"; do
    case $name in gw-*) out+=", \"$name\": $(rss_mb "${STACK_PIDS[$name]}")" ;; esac
  done
  echo "{$out}"
}

t2_report_extra() {
  local f
  for f in "$STACK_DIR"/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "$(basename "$f") (last 30 lines)"
    tail -n 30 "$f"
  done
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

# 1: the stack.
busy=$(stack_busy_ports)
[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
stack_up 2>"$T2_OUT/stack-up.err"
up=$?
t2_check stack "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$T2_OUT/stack-up.err")")"
[ $up -eq 0 ] || t2_finish

# 3-6: light connections, held through a recorded window.
t=$SECONDS
lg_start light -mode light -clients "$LIGHT" -rate 5000 -ping-every 10s -uid-prefix light
online=$(t2_bool lg_wait_online "$LIGHT" 120)
t2_duration light_ramp $((SECONDS - t))
t2_check light_online "$online" "\"want\": $LIGHT, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"failed\": $(lg_get /stats 'd["failed"]')"

# 2: pinning, loadgen included, while it runs.
wrong=""
for name in "${STACK_ORDER[@]}"; do
  case $name in gw-*) want=$T2_CPUS_GATEWAY ;; *) want=$T2_CPUS_LOGIC ;; esac
  stack_pinned "$name" "$want" || wrong+="${wrong:+ }$name"
done
have=$(taskset -pc "$LG_PID" 2>/dev/null)
[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+="${wrong:+ }loadgen"
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"wrong\": $(t2_json_str "$wrong")"

t2_record_start light
stack_profile light 30 & wait $! # CPU profiles over the window's first 30s
sleep 30 & wait $!
t2_record_end light
stack_snapshot light
rss=$(rss_json)
lg_stop
t2_field light "{\"exit\": $LG_STATUS, \"rss_mb\": $rss, \"login_latency\": $(lg_json 'd["login_latency"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"failed\": $(lg_json 'd["failed"]'), \"sources\": $(lg_json 'd["sources"]')}"
res=$STACK_DIR/artifacts/loadgen-light.json
t2_check light_pongs "$(lg_true "$res" 'd["pong_missing"] == 0 and d["pongs"] >= '"$LIGHT"' * 5')" "\"pongs\": $(lg_json 'd["pongs"]'), \"missing\": $(lg_json 'd["pong_missing"]')"
# The result is taken before any client stops (#84): even "stopped" is a loss.
t2_check light_held "$(lg_true "$res" 'd["lost"] == {}')" "\"lost\": $(lg_json 'd["lost"]')"

# 7-8: SDK clients, and a gateway killed by loadgen (b1 in small).
t=$SECONDS
lg_start sdk -mode sdk -clients "$SDK" -rate 2000 -uid-prefix sdk
online=$(t2_bool lg_wait_online "$SDK" 60)
t2_duration sdk_ramp $((SECONDS - t))
t2_check sdk_online "$online" "\"want\": $SDK, \"online\": $(lg_get /stats 'd["online"]'), \"failed\": $(lg_get /stats 'd["failed"]')"
rss=$(rss_json)
reply=$(lg_post "/kill?gateway=gw-3&pid=${STACK_PIDS[gw-3]}&mark=death" | lg_py - d)
stack_expect_dead gw-3 # in this shell: it reaps and forgets gw-3
dead=$?
lg_wait_back death 45
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
slo=$(cat "$STACK_DIR/artifacts/slo-death.json")
pass=false
[ $dead -eq 0 ] && pass=$(lg_true "$STACK_DIR/artifacts/slo-death.json" 'd["pass_from_mark"] and d["population"] >= '"$SDK"' // 5')
t2_check slo_b1 "$pass" "\"kill\": ${reply:-null}, \"slo\": ${slo:-null}"
lg_stop
t2_field sdk "{\"exit\": $LG_STATUS, \"rss_mb\": $rss, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]')}"

# 9: artifacts.
missing=""
for f in logic gw-0 gw-1 gw-2 gw-3; do
  a=$STACK_DIR/artifacts/light
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
  gzip -t "$a/$f.heap.pb.gz" 2>/dev/null || missing+=" $f.heap"
  grep -q 'goroutine profile:' "$a/$f.goroutine.txt" 2>/dev/null || missing+=" $f.goroutine"
  gzip -t "$a/$f.cpu.pb.gz" 2>/dev/null || missing+=" $f.cpu"
done
for f in light sdk; do
  [ "$(lg_true "$STACK_DIR/artifacts/loadgen-$f.json" '"mode" in d')" = true ] || missing+=" loadgen-$f.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

# 10: process health, after a clean stop.
stack_down
problems=$(stack_health)
t2_check processes "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

t2_not_covered "scale: $LIGHT light and $SDK SDK clients, not Plan 6b's 600k and 200k" \
  "b2, storms, floods and slowed Logic (Plan 6b)" "Redis Cluster (Plan 6c)"
t2_finish
