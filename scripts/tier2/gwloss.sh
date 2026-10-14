#!/usr/bin/env bash
# Tier 2: gwloss (Plan 6b2a-b1), the spec's load test (b1) with SDK clients:
#   (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of T_death
#        (loadgen records the SIGKILL time; R4M-17); Kick attempts to it are
#        about one per relogin, with no retries (R2-O6);
#   storm: the other gateway never misses a lease renewal.
# Two gateways share the gateway CPU set (GOMAXPROCS 6 each, #79), with Go's
# default GC (6b1), and 2 x 200k SDK clients up from the start (#93): the
# survivor carries its own 200k through the storm and ends with 400k. Logic
# is one process throughout (minReplicas, no HPA), so the storm runs at
# Logic's minimum. Plan 6b2a-stress adds the b_stress phase (a 600k victim).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss
#   Results: tier2-results/gwloss-<timestamp>/ (result.json, b1/ with logs,
#   artifacts and loadgen results), plus ONE file to send back:
#   tier2-results/gwloss-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 8 minutes.
#
# Settings:
#   GWLOSS_CLIENTS     SDK clients over both gateways (default 400000)
#   GWLOSS_RATE        clients started per second (default 5000)
#   GWLOSS_CAP         each gateway's -max-conns (default 450000: the
#                      survivor's 400k plus room for clients in flight)
#   GWLOSS_SWEEP_WAIT  seconds after the kill to wait for the dead epoch's
#                      sweep (default 180: stale 60s after its last renewal,
#                      then 200k uids at the sweeper's 10k/s)
#   GWLOSS_SOURCES     loadgen's source addresses (default: loadgen's, one per
#                      5,000 connections per gateway, sized for the survivor)
#
# Checks:
#   b1_stack           redis, devtoken, Logic, gw-0 and gw-1 become Ready
#   pinning            gateways, Logic + Redis and loadgen on their CPU sets
#   b1_online          every client logs in, each gateway holds 45-55% of
#                      them
#   b1_slo             loadgen SIGKILLs gw-1: 99% of its clients log in again
#                      within 30s of T_death (R4M-17), and the survivor
#                      rejected none at capacity (SDK clients retry a reject
#                      without counting it, P6b2a-b1I-2)
#   b1_kicks           Kick first attempts in the storm, sent or dropped by a
#                      full queue (R5S-O2), are 0.9-1.1 per relogin, none is
#                      retried and none gives up as live: a refused Kick is
#                      not retried (R2-O6, #97)
#   b1_lease           the survivor gw-0 renews its lease every interval from
#                      the kill to the sweep: no call failed or found the
#                      lease missing, and Logic saw no renewal error
#   b1_swept           the sweeper finishes the dead epoch within GWLOSS_SWEEP_WAIT
#   no_swap_in_b1      no page swapped in from the kill to the sweep (#70)
#   no_lo_drops_in_b1  loopback dropped no packet from the ramp to the sweep
#   b1_loadgen_headroom loadgen used under 3/4 of its CPU set in the ramp and
#                      the storm (P6a2aI-4)
#   b1_idle            after loadgen stops, gw-0 reaches 0 connections and 0
#                      awaiting Logout within 300s
#   b1_processes       nothing but gw-1 died, panicked or stopped uncleanly
#   artifacts          storm CPU profiles, metrics before and after, loadgen's result
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init gwloss
t2_need curl taskset sudo python3
t2_require_host_setup
t2_mem_budget 14
t2_nofile 1048576

CLIENTS=${GWLOSS_CLIENTS:-400000} RATE=${GWLOSS_RATE:-5000} CAP=${GWLOSS_CAP:-450000}
SWEEP_WAIT=${GWLOSS_SWEEP_WAIT:-180} SOURCES=${GWLOSS_SOURCES:-}
export STACK_GATEWAYS=2 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

# The storm's counters (Plan 6b2a): Logic's Kicks, lease renewals, sweeps
# and relogins onto a dead epoch; the survivor's own lease calls.
LOGIC_COUNTERS=(
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  kick_first='goim_logic_kick_attempts_total:attempt="first"'
  kick_retry='goim_logic_kick_attempts_total:attempt="retry"'
  kick_giveups_dead='goim_logic_kick_giveups_total:target_lease="dead"'
  kick_giveups_live='goim_logic_kick_giveups_total:target_lease="live"'
  kick_dropped=goim_logic_kick_dropped_total
  dead_epoch_relogins=goim_logic_dead_epoch_relogin_seconds_count
  lease_live='goim_logic_lease_renewals_total:result="live"'
  lease_missing='goim_logic_lease_renewals_total:result="missing"'
  lease_error='goim_logic_lease_renewals_total:result="error"'
  sweeps_done='goim_logic_sweeps_total:result="done"'
  sweeps=goim_logic_sweeps_total
  sweep_uids=goim_logic_sweep_uids_total
  sweep_deleted=goim_logic_sweep_deleted_total
  guard_holds=goim_logic_sweep_guard_holds_total
)
GW_COUNTERS=(
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
)

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# lo_dropped: packets loopback dropped on input since boot (host-wide); as capacity.sh.
lo_dropped() { cat /sys/class/net/lo/statistics/rx_dropped 2>/dev/null || echo null; }
# usage_ok JSON: loadgen used under 3/4 of its CPU set.
usage_ok() { [ "$(echo "$1" | lg_true - 'd["loadgen"]["cores"] < '"$LG_CPUS"' * 0.75')" = true ]; }

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

# ---- b1: SIGKILL a 200k gateway ----
name=b1
export STACK_DIR=$T2_OUT/$name
mkdir -p "$STACK_DIR"
ln -sfn ../bin "$STACK_DIR/bin"
STACK_GATEWAY_ARGS=(-max-conns "$CAP")
t0=$SECONDS

busy=$(stack_busy_ports)
[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
stack_up 2>"$STACK_DIR/stack-up.err"
up=$?
t2_check "${name}_stack" "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
[ $up -eq 0 ] || t2_finish

# The ramp: every client up from the start, split by the balancer.
lo0=$(lo_dropped)
t2_cpustat_snap "$STACK_DIR/cpustat-0"
t2_tcpstat_snap "$STACK_DIR/tcpstat-0"
lg_start "$name" -mode sdk -clients "$CLIENTS" -rate "$RATE" -uid-prefix "$name" ${SOURCES:+-sources "$SOURCES"}
stack_usage_start loadgen="$LG_PID"
t=$SECONDS
online=false
lg_wait_online "$CLIENTS" $((CLIENTS / RATE * 3 + 60)) &&
  online=$(lg_get /stats 'd["online"] == '"$CLIENTS"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$CLIENTS"' * 0.45')
[ "$online" = true ] || online=false # null: /stats unreadable, a failed run, not a stopped one
ramp_s=$((SECONDS - t))
ramp=$(stack_usage_json)
t2_cpustat_snap "$STACK_DIR/cpustat-1"
t2_tcpstat_snap "$STACK_DIR/tcpstat-1"
by_gateway=$(lg_get /stats 'd["online_by_gateway"]')
t2_check "${name}_online" "$online" "\"want\": $CLIENTS, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $by_gateway, \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"

wrong=""
for n in gw-0 gw-1; do stack_pinned "$n" "$T2_CPUS_GATEWAY" || wrong+=" $n"; done
for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
have=$(taskset -pc "$LG_PID" 2>/dev/null)
[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
[ "$online" = true ] || t2_finish

# The storm: counters before, SIGKILL, CPU profiles of its first 30s.
stack_snapshot before
logic0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
gw0=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_record_start "$name"
t2_cpustat_snap "$STACK_DIR/cpustat-2"
t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
stack_usage_start loadgen="$LG_PID"
reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - d)
kill_t=$SECONDS
stack_expect_dead gw-1 # in this shell: it reaps and forgets gw-1
dead=$?
(stack_profile storm 30 & lg_profile storm 30; wait) >/dev/null 2>&1 &
prof=$!
back=$(t2_bool lg_wait_back death 60)
back_s=$((SECONDS - kill_t))
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
# The dead epoch's sweep (2 x TTL after its last renewal, then its uids at
# the sweeper's rate): the storm's window ends when it is done.
# A sweep done since the kill, not before it (P6b2a-b1I-3).
swept=false
done0=$(echo "$logic0" | lg_py - 'd["sweeps_done"] or 0')
while [ $((SECONDS - kill_t)) -lt "$SWEEP_WAIT" ]; do
  d=$(stack_counters logic sweeps_done='goim_logic_sweeps_total:result="done"')
  [ "$(echo "$d" | lg_true - 'd["sweeps_done"] > '"$done0")" = true ] && { swept=true; break; }
  sleep 1
done
sweep_s=$((SECONDS - kill_t))
wait "$prof"
storm=$(stack_usage_json)
logic1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
gw1=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_cpustat_snap "$STACK_DIR/cpustat-3"
t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
t2_record_end "$name"
lo1=$(lo_dropped)
stack_snapshot after
logic_d=$(stack_counters_delta "$logic0" "$logic1")
gw_d=$(stack_counters_delta "$gw0" "$gw1")
window_s=$(echo "$storm" | lg_py - 'd["window_s"]')

slo=$STACK_DIR/artifacts/slo-death.json
pop=$(lg_py "$slo" 'd["population"]')
pass=false
rejects=$(echo "$gw_d" | lg_py - 'd["rejects_at_capacity"]')
[ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_mark"] and d["population"] >= '"$CLIENTS"' * 0.45')
t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - 'd["d"]["kick_retry"] == 0 and d["d"]["kick_giveups_live"] == 0 and 0.9 * d["pop"] <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]')" \
  "\"relogins\": $pop, \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
# One renewal per 10s interval (the gateway's -lease-interval default): the
# window holds at least window_s / 10 - 1 whole intervals.
t2_check "${name}_lease" "$(echo "{\"g\": $gw_d, \"l\": $logic_d, \"w\": $window_s}" | lg_true - 'd["g"]["lease_error"] == 0 and d["g"]["lease_missing"] == 0 and d["g"]["lease_live"] >= d["w"] // 10 - 1 and d["l"]["lease_error"] == 0')" \
  "\"window_s\": $window_s, \"survivor\": $gw_d, \"logic_lease_error\": $(echo "$logic_d" | lg_py - 'd["lease_error"]')"
t2_check "${name}_swept" "$swept" "\"sweep_s\": $sweep_s, \"wait_s\": $SWEEP_WAIT, \"sweeps\": $(echo "$logic_d" | lg_py - 'd["sweeps"]'), \"uids\": $(echo "$logic_d" | lg_py - 'd["sweep_uids"]'), \"deleted\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]')"
lo=null
[ "$lo0" = null ] || [ "$lo1" = null ] || lo=$((lo1 - lo0))
t2_check "no_lo_drops_in_$name" "$(t2_bool [ "$lo" = 0 ])" "\"dropped\": $lo, \"netdev_max_backlog\": $(cat /proc/sys/net/core/netdev_max_backlog 2>/dev/null || echo null)"
headroom=false
usage_ok "$ramp" && usage_ok "$storm" && headroom=true
t2_check "${name}_loadgen_headroom" "$headroom" \
  "\"loadgen_cpus\": $LG_CPUS, \"ramp_cores\": $(echo "$ramp" | lg_py - 'd["loadgen"]["cores"]'), \"storm_cores\": $(echo "$storm" | lg_py - 'd["loadgen"]["cores"]')"

# The end: the result is written before any client stops (#84), then gw-0
# drains to idle before the stack stops.
lg_stop
res=$STACK_DIR/artifacts/loadgen-$name.json
t=$SECONDS
idle=$(t2_bool stack_wait_idle 300 2>"$STACK_DIR/idle.err")
stop_s=$((SECONDS - t))
body=$(stack_metrics gw-0) || body=""
t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"done_logouts\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"'), \"dropped_logouts\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"'), \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
stack_down
problems=$(stack_health)
t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
tcp="{\"ramp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-0" "$STACK_DIR/tcpstat-1"), \"storm\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3")}"
t2_field "$name" "{\"clients\": $CLIENTS, \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"storm\": $storm, \"back_s\": $back_s, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivor\": $gw_d, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
t2_duration "$name" $((SECONDS - t0))

# Artifacts: metrics before and after the storm, its CPU profiles, the SLO
# and loadgen's result.
missing=""
a=$STACK_DIR/artifacts
for f in before/logic before/gw-0 before/gw-1 after/logic after/gw-0; do
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
done
for f in logic gw-0 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" storm/$f.cpu"; done
[ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" slo-death.json"
[ "$(lg_true "$res" '"mode" in d')" = true ] || missing+=" loadgen-$name.json"
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "per-shard Redis CPU and p99 with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)" \
  "b_stress, the 600k victim (Plan 6b2a-stress)" "b2 and slowed Logic (Plan 6b2b)"
t2_finish
