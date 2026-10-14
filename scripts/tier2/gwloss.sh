#!/usr/bin/env bash
# Tier 2: gwloss (Plans 6b2a-b1 and 6b2a-stress), the spec's load tests (b1)
# and (b-stress) with SDK clients, each phase on a fresh stack:
#   (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of T_death
#        (loadgen records the SIGKILL time; R4M-17); Kick attempts to it are
#        about one per relogin, with no retries (R2-O6);
#   (b-stress) kill a 600k gateway: measured, no SLO; includes a 600k-member
#        gwconns SSCAN + UNLINK (the dead epoch's sweep; R4M-9);
#   storm: the other gateways never miss a lease renewal.
# The gateways share the gateway CPU set (GOMAXPROCS = its CPUs / gateways,
# #79), with Go's default GC (6b1). Logic is one process throughout
# (minReplicas, no HPA), so each storm runs at Logic's minimum.
#   b1: 2 gateways, 2 x 200k SDK clients up from the start (#93): the
#       survivor gw-0 carries its own 200k through the storm and ends with
#       400k.
#   b_stress: 3 gateways. Loadgen starts with gw-1 and gw-2 out of its
#       balancer, ramps 600k SDK clients onto gw-0 alone, puts the others
#       back (POST /up), then SIGKILLs gw-0: the survivors start empty and
#       take the 600k relogins (#93). Loadgen runs with GOMEMLIMIT.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss
#   Results: tier2-results/gwloss-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/gwloss-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 25 minutes (b1 ~8, b_stress ~17; up to ~40 if b_stress's relogins
#   use their whole wait).
#
# Settings:
#   GWLOSS_PHASES      phases to run, in order (default "b1 b_stress")
#   GWLOSS_CLIENTS     b1: SDK clients over both gateways (default 400000)
#   GWLOSS_RATE        b1: clients started per second (default 5000)
#   GWLOSS_CAP         b1: each gateway's -max-conns (default 450000: the
#                      survivor's 400k plus room for clients in flight)
#   GWLOSS_SWEEP_WAIT  b1: seconds after the kill to wait for the dead epoch's
#                      sweep (default 180: stale 60s after its last renewal,
#                      then 200k uids at the sweeper's 10k/s)
#   GWLOSS_SOURCES     b1: loadgen's source addresses (default: loadgen's, one
#                      per 5,000 connections per gateway, sized for the survivor)
#   GWLOSS_STRESS_CLIENTS  b_stress: SDK clients, all on the victim (default 600000)
#   GWLOSS_STRESS_RATE     b_stress: clients started per second (default 5000)
#   GWLOSS_STRESS_CAP      b_stress: each gateway's -max-conns (default 650000:
#                          the victim's 600k plus room for clients in flight)
#   GWLOSS_STRESS_SWEEP_WAIT  b_stress: as GWLOSS_SWEEP_WAIT (default 300: 60s
#                          stale, then 600k uids at 10k/s, and slower relogins)
#   GWLOSS_STRESS_BACK_WAIT   b_stress: seconds after the kill to wait for every
#                          victim to log in again (default 900: the SDK's backoff
#                          doubles to 5 min, and in run 22 a loadgen-bound storm
#                          took 739s; #102)
#   GWLOSS_STRESS_SOURCES  b_stress: loadgen's source addresses (default:
#                          loadgen's, sized for the victim alone: 120 for 600k)
#   GWLOSS_STRESS_LG_MEMLIMIT b_stress: loadgen's GOMEMLIMIT (default 16GiB, #93)
#
# Checks (b1; b_stress has the same with its own prefix, unless noted):
#   b1_stack           redis, devtoken, Logic and the gateways become Ready
#   pinning            (first phase) gateways, Logic + Redis and loadgen on their CPU sets
#   b1_online          every client logs in, each gateway holds 45-55% of
#                      them; b_stress: every client on gw-0, then gw-1 and
#                      gw-2 are back in loadgen's balancer
#   b1_slo             (b1 only) loadgen SIGKILLs gw-1: 99% of its clients log
#                      in again within 30s of T_death (R4M-17), and the
#                      survivor rejected none at capacity (SDK clients retry a
#                      reject without counting it, P6b2a-b1I-2)
#   b_stress_back      (b_stress only) loadgen SIGKILLs gw-0: every one of its
#                      clients logs in again within GWLOSS_STRESS_BACK_WAIT, and
#                      the survivors rejected none at capacity; no SLO. Its
#                      relogin times are loadgen-bound when loadgen_bound is
#                      true (#102): one host cannot do the client side of a
#                      600k storm's Noise handshakes in the SDK's 0-20s jitter
#   b1_kicks           Kick first attempts in the storm, sent or dropped by a
#                      full queue (R5S-O2), are 0.9-1.1 per relogin (at least 0.9
#                      per relogin whose entry the sweep had not deleted: such a
#                      relogin replaces nothing, so it Kicks nothing), and none
#                      gives up as live. A refused Kick is not retried (#97):
#                      only first attempts that timed out while the victim's
#                      sockets were torn down are, once each, and refused
#                      (R2-O6; #97 amended, run 20)
#   b1_cross_check     Logic's dead-epoch cross-check (R1-O7) has a sample for
#                      at least 0.9 of the relogins whose first Kick was sent
#                      and did not time out (by Login once the lease is gone,
#                      or by the refused Kick while it is live, P6b2a-b1I-7),
#                      and at most 1.1 per relogin
#   b1_lease           every survivor renews its lease every interval from
#                      the kill to the sweep: no call failed or found the
#                      lease missing, and Logic saw no renewal error
#   b1_swept           the sweeper finishes the dead epoch within the sweep wait
#   no_swap_in_b1      no page swapped in from the kill to the sweep (#70)
#   no_lo_drops_in_b1  loopback dropped no packet from the ramp to the sweep
#   b1_loadgen_headroom loadgen used under 3/4 of its CPU set in the ramp, the
#                      storm, and the storm's first 30s, where its relogins
#                      peak (P6a2aI-4, #102); b_stress's checks the ramp and
#                      the storm, and records the first 30s as loadgen_bound
#   b1_idle            after loadgen stops, every survivor reaches 0
#                      connections, 0 awaiting Logout and one Logout done per
#                      login, none dropped (stack_wait_idle, P6b1I-4)
#   b1_processes       nothing but the victim died, panicked or stopped uncleanly
#   artifacts          (once) every phase's storm CPU profiles, metrics before
#                      and after, loadgen's heap at the ramp's end, the SLO and
#                      loadgen's result
# The phase field's redis_latency.ramp and .storm are Redis's per-command
# latency since a CONFIG RESETSTAT at each window's start; .reset says
# whether that reset succeeded (false: the window also holds the commands
# before it). The reset clears commandstats too, so after/'s redis INFO
# covers the storm only.
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init gwloss
t2_need curl taskset sudo python3
t2_require_host_setup

PHASES=${GWLOSS_PHASES:-b1 b_stress}
for p in $PHASES; do
  case $p in b1 | b_stress) ;; *) echo "gwloss: unknown phase '$p' (b1, b_stress)" >&2; exit 1 ;; esac
done
# The memory each phase needs available (#93): b1's loadgen peaked at
# 10.6-11.5GB with 400k SDK clients (runs 20, 21). b_stress's loadgen peaked
# at 16.1GB with 600k (run 22): its live heap was 3.25GB, the rest mostly
# goroutine stacks, which GOMEMLIMIT does not bound. Run 22 started with
# 22GiB available (t2_mem_budget counts GiB) and swapped ~1GiB out, so
# b_stress needs about 23GiB with the victim, the survivors, Redis and the
# kernel's socket memory (#105). That is the host's ceiling with a desktop
# session, so there is no margin here: the swap check is the verdict.
mem=14
case " $PHASES " in *" b_stress "*) mem=23 ;; esac
t2_mem_budget $mem
t2_nofile 1048576

export STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

# The storm's counters (Plan 6b2a): Logic's Kicks, lease renewals, sweeps
# and relogins onto a dead epoch; each survivor's own lease calls and rejects.
LOGIC_COUNTERS=(
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  kick_first='goim_logic_kick_attempts_total:attempt="first"'
  kick_retry='goim_logic_kick_attempts_total:attempt="retry"'
  kick_first_timeout='goim_logic_kick_attempts_total:attempt="first",result="timeout"'
  kick_retry_refused='goim_logic_kick_attempts_total:attempt="retry",result="refused"'
  kick_giveups_dead='goim_logic_kick_giveups_total:target_lease="dead"'
  kick_giveups_live='goim_logic_kick_giveups_total:target_lease="live"'
  kick_dropped=goim_logic_kick_dropped_total
  dead_epoch_relogins=goim_logic_dead_epoch_relogin_seconds_count
  dead_epoch_relogin_s=goim_logic_dead_epoch_relogin_seconds_sum
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
  rejects_handshake_rate='goim_gateway_admission_rejects_total:reason="handshake_rate"'
  # Every other reject and every handshake result, so a storm's closes are
  # explained from the result (#104: run 22's survivors timed out 6.4M
  # handshakes that the two readings above did not show).
  rejects_pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
  rejects_handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"'
  rejects_auth_timeout='goim_gateway_admission_rejects_total:reason="auth_timeout"'
  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
  rejects_draining='goim_gateway_admission_rejects_total:reason="draining"'
  rejects_bad_version='goim_gateway_admission_rejects_total:reason="bad_version"'
  rejects_noise_unknown_key='goim_gateway_admission_rejects_total:reason="noise_unknown_key"'
  handshakes_ok='goim_gateway_handshakes_total:result="ok"'
  handshakes_failed='goim_gateway_handshakes_total:result="failed"'
  handshakes_no_slot='goim_gateway_handshakes_total:result="no_slot"'
  handshakes_no_token='goim_gateway_handshakes_total:result="no_token"'
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

first=1
missing=""
for name in $PHASES; do
  # Each phase's stack, victim and waits.
  case $name in
    b1)
      export STACK_GATEWAYS=2
      clients=${GWLOSS_CLIENTS:-400000} rate=${GWLOSS_RATE:-5000} cap=${GWLOSS_CAP:-450000}
      sweep_wait=${GWLOSS_SWEEP_WAIT:-180} back_wait=60 idle_wait=300 sources=${GWLOSS_SOURCES:-}
      victim=gw-1 survivors=(gw-0) down="" LG_ENV=()
      # Every client up from the start, split by the balancer.
      online_rule='d["online"] == '"$clients"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$clients"' * 0.45'
      ;;
    b_stress)
      export STACK_GATEWAYS=3
      clients=${GWLOSS_STRESS_CLIENTS:-600000} rate=${GWLOSS_STRESS_RATE:-5000} cap=${GWLOSS_STRESS_CAP:-650000}
      sweep_wait=${GWLOSS_STRESS_SWEEP_WAIT:-300} back_wait=${GWLOSS_STRESS_BACK_WAIT:-900} idle_wait=600
      sources=${GWLOSS_STRESS_SOURCES:-}
      victim=gw-0 survivors=(gw-1 gw-2) down=gw-1,gw-2 LG_ENV=(GOMEMLIMIT="${GWLOSS_STRESS_LG_MEMLIMIT:-16GiB}")
      # The victim ramped alone (#93).
      online_rule='d["online"] == '"$clients"' and d["online_by_gateway"].get("gw-0") == '"$clients"
      ;;
  esac
  export STACK_DIR=$T2_OUT/$name
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  STACK_GATEWAY_ARGS=(-max-conns "$cap")
  t0=$SECONDS

  busy=$(stack_busy_ports)
  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
  stack_up 2>"$STACK_DIR/stack-up.err"
  up=$?
  t2_check "${name}_stack" "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
  [ $up -eq 0 ] || t2_finish

  # The ramp.
  lo0=$(lo_dropped)
  t2_cpustat_snap "$STACK_DIR/cpustat-0"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-0"
  reset_ramp=true; stack_redis_resetstat || reset_ramp=false
  lg_start "$name" -mode sdk -clients "$clients" -rate "$rate" -uid-prefix "$name" ${sources:+-sources "$sources"} ${down:+-down "$down"}
  stack_usage_start loadgen="$LG_PID"
  t=$SECONDS
  online=false
  lg_wait_online "$clients" $((clients / rate * 3 + 60)) && online=$(lg_get /stats "$online_rule")
  [ "$online" = true ] || online=false # null: /stats unreadable, a failed run, not a stopped one
  ramp_s=$((SECONDS - t))
  ramp=$(stack_usage_json)
  redis_ramp=$(stack_redis_latency)
  t2_cpustat_snap "$STACK_DIR/cpustat-1"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-1"
  by_gateway=$(lg_get /stats 'd["online_by_gateway"]')
  # Loadgen's live heap per client at the ramp's end, after a forced GC (#93's
  # memory budget estimates it from RSS, which includes GC headroom).
  mkdir -p "$STACK_DIR/artifacts/ramp"
  curl -sf -m 60 "http://$(lg_control)/debug/pprof/heap?gc=1&debug=1" >"$STACK_DIR/artifacts/ramp/loadgen.heap.txt"
  lg_heap=$(awk -v n="$clients" '/^# HeapAlloc = / { a = $4 } /^# Sys = / { s = $4 }
    END { if (a == "") print "null"; else printf "{\"live_mb\": %d, \"sys_mb\": %d, \"live_bytes_per_client\": %d}\n", a / 1048576, s / 1048576, a / n }' \
    "$STACK_DIR/artifacts/ramp/loadgen.heap.txt")
  # b_stress: the survivors back in the balancer before the kill.
  ups=""
  for g in ${down//,/ }; do
    lg_post "/up?gateway=$g" >/dev/null || ups+=" $g"
  done
  [ -z "$ups" ] || online=false
  t2_check "${name}_online" "$online" "\"want\": $clients, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $by_gateway, \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]'), \"up_failed\": $(t2_json_str "${ups# }")"

  if [ -n "$first" ]; then
    first=""
    wrong=""
    for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
    for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
    have=$(taskset -pc "$LG_PID" 2>/dev/null)
    [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
    t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
  fi
  if [ "$online" != true ]; then
    lg_stop
    stack_down
    t2_duration "$name" $((SECONDS - t0))
    continue
  fi

  # The storm: counters before, SIGKILL, CPU profiles of its first 30s.
  stack_snapshot before
  # The ramp's backoff (no mark yet: since loadgen started), apart from the
  # storm's, which counts since the death mark (P6b2a-stressI-1).
  backoff_ramp=$(lg_get /stats 'd["backoff_since_mark"]')
  logic0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  declare -A sv0=() sv1=()
  for g in "${survivors[@]}"; do sv0[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
  reset_storm=true; stack_redis_resetstat || reset_storm=false
  t2_record_start "$name"
  t2_cpustat_snap "$STACK_DIR/cpustat-2"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
  stack_usage_start loadgen="$LG_PID"
  reply=$(lg_post "/kill?gateway=$victim&pid=${STACK_PIDS[$victim]}&mark=death" | lg_py - d)
  kill_t=$SECONDS
  stack_expect_dead "$victim" # in this shell: it reaps and forgets the victim
  dead=$?
  # The storm's first 30s, where its relogins peak: loadgen's cores there
  # say whether the storm was loadgen-bound (#102); the whole storm's average
  # hid it in run 22 (3.2 of 8 cores, 7.9 in the first 30s).
  (stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
  prof=$!
  # The dead epoch's sweep (2 x TTL after its last renewal, then its uids at
  # the sweeper's rate), watched from the kill alongside the relogins: a
  # storm slower than the sweep must not hide it or delay its time. The
  # storm's window ends when both are done. A sweep done since the kill, not
  # before it (P6b2a-b1I-3).
  done0=$(echo "$logic0" | lg_py - 'd["sweeps_done"] or 0')
  (
    while [ $((SECONDS - kill_t)) -lt "$sweep_wait" ]; do
      d=$(stack_counters logic sweeps_done='goim_logic_sweeps_total:result="done"')
      [ "$(echo "$d" | lg_true - 'd["sweeps_done"] > '"$done0")" = true ] && { echo "true $((SECONDS - kill_t))"; exit; }
      sleep 1
    done
    echo "false $((SECONDS - kill_t))"
  ) >"$STACK_DIR/sweep.out" 2>/dev/null &
  sweeping=$!
  back=$(t2_bool lg_wait_back death "$back_wait")
  back_s=$((SECONDS - kill_t))
  lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
  wait "$sweeping"
  read -r swept sweep_s <"$STACK_DIR/sweep.out" || { swept=false sweep_s=null; }
  wait "$prof"
  storm30=$(cat "$STACK_DIR/storm30.json" 2>/dev/null) || storm30=null
  loadgen_bound=true
  usage_ok "$storm30" && loadgen_bound=false
  storm=$(stack_usage_json)
  redis_storm=$(stack_redis_latency)
  logic1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  for g in "${survivors[@]}"; do sv1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
  t2_cpustat_snap "$STACK_DIR/cpustat-3"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
  t2_record_end "$name"
  lo1=$(lo_dropped)
  stack_snapshot after
  logic_d=$(stack_counters_delta "$logic0" "$logic1")
  sv_d=""
  for g in "${survivors[@]}"; do sv_d+="${sv_d:+, }\"$g\": $(stack_counters_delta "${sv0[$g]}" "${sv1[$g]}")"; done
  sv_d="{$sv_d}"
  window_s=$(echo "$storm" | lg_py - 'd["window_s"]')
  backoff="{\"ramp\": $backoff_ramp, \"storm\": $(lg_get /stats 'd["backoff_since_mark"]'), \"run\": $(lg_get /stats '{"clients": d["backoff_clients"], "waits": d["backoff_waits"]}')}"

  slo=$STACK_DIR/artifacts/slo-death.json
  pop=$(lg_py "$slo" 'd["population"]')
  # Rejects at capacity over every survivor; null if any reading failed.
  rejects=$(echo "$sv_d" | lg_py - 'sum(v["rejects_at_capacity"] for v in d.values())')
  pass=false
  if [ "$name" = b1 ]; then
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_mark"] and d["population"] >= '"$clients"' * 0.45')
    t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
  else
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && [ "$back" = true ] && pass=$(lg_true "$slo" 'd["population"] == '"$clients")
    t2_check "${name}_back" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"loadgen_bound\": $loadgen_bound, \"rejects_at_capacity\": ${rejects:-null}, \"backoff\": $backoff, \"slo\": $(cat "$slo")"
  fi
  t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - 'd["d"]["kick_retry"] <= d["d"]["kick_first_timeout"] and d["d"]["kick_retry_refused"] == d["d"]["kick_retry"] and d["d"]["kick_giveups_live"] == 0 and 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0)) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]')" \
    "\"relogins\": $pop, \"swept_first\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]'), \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"first_timeouts\": $(echo "$logic_d" | lg_py - 'd["kick_first_timeout"]'), \"retries_refused\": $(echo "$logic_d" | lg_py - 'd["kick_retry_refused"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
  # R1-O7: one sample per relogin whose first Kick was sent and did not time
  # out, by Login (lease gone) or by the refused Kick (lease live).
  t2_check "${name}_cross_check" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - '0.9 * (d["d"]["kick_first"] - d["d"]["kick_first_timeout"]) <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]')" \
    "\"relogins\": $pop, \"samples\": $(echo "$logic_d" | lg_py - 'd["dead_epoch_relogins"]'), \"mean_s\": $(echo "$logic_d" | lg_py - 'round(d["dead_epoch_relogin_s"] / d["dead_epoch_relogins"], 1)')"
  # One renewal per 10s interval (the gateway's -lease-interval default): the
  # window holds at least window_s / 10 - 1 whole intervals.
  t2_check "${name}_lease" "$(echo "{\"g\": $sv_d, \"l\": $logic_d, \"w\": $window_s}" | lg_true - 'all(v["lease_error"] == 0 and v["lease_missing"] == 0 and v["lease_live"] >= d["w"] // 10 - 1 for v in d["g"].values()) and d["l"]["lease_error"] == 0')" \
    "\"window_s\": $window_s, \"survivors\": $sv_d, \"logic_lease_error\": $(echo "$logic_d" | lg_py - 'd["lease_error"]')"
  t2_check "${name}_swept" "$swept" "\"sweep_s\": $sweep_s, \"wait_s\": $sweep_wait, \"sweeps\": $(echo "$logic_d" | lg_py - 'd["sweeps"]'), \"uids\": $(echo "$logic_d" | lg_py - 'd["sweep_uids"]'), \"deleted\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]')"
  lo=null
  [ "$lo0" = null ] || [ "$lo1" = null ] || lo=$((lo1 - lo0))
  t2_check "no_lo_drops_in_$name" "$(t2_bool [ "$lo" = 0 ])" "\"dropped\": $lo, \"netdev_max_backlog\": $(cat /proc/sys/net/core/netdev_max_backlog 2>/dev/null || echo null)"
  headroom=false
  # b1's SLO is only valid if loadgen kept up with its storm; b_stress's
  # relogin times are recorded as loadgen-bound instead (#102).
  usage_ok "$ramp" && usage_ok "$storm" && { [ "$name" = b_stress ] || [ $loadgen_bound = false ]; } && headroom=true
  t2_check "${name}_loadgen_headroom" "$headroom" \
    "\"loadgen_cpus\": $LG_CPUS, \"ramp_cores\": $(echo "$ramp" | lg_py - 'd["loadgen"]["cores"]'), \"storm_cores\": $(echo "$storm" | lg_py - 'd["loadgen"]["cores"]'), \"storm_first30_cores\": $(echo "$storm30" | lg_py - 'd["loadgen"]["cores"]'), \"loadgen_bound\": $loadgen_bound"

  # The end: the result is written before any client stops (#84), then the
  # survivors drain to idle before the stack stops.
  lg_stop
  res=$STACK_DIR/artifacts/loadgen-$name.json
  t=$SECONDS
  idle=$(t2_bool stack_wait_idle "$idle_wait" 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  logouts=""
  for g in "${survivors[@]}"; do
    body=$(stack_metrics "$g") || body=""
    logouts+="${logouts:+, }\"$g\": {\"done\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"'), \"logins_ok\": $(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"'), \"dropped\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"')}"
  done
  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"wait_s\": $idle_wait, \"logouts\": {$logouts}, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

  cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
  tcp="{\"ramp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-0" "$STACK_DIR/tcpstat-1"), \"storm\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3")}"
  t2_field "$name" "{\"clients\": $clients, \"victim\": \"$victim\", \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"loadgen_heap\": $lg_heap, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"back_s\": $back_s, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivors\": $sv_d, \"redis_latency\": {\"ramp\": $redis_ramp, \"storm\": $redis_storm, \"reset\": {\"ramp\": $reset_ramp, \"storm\": $reset_storm}}, \"backoff\": $backoff, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
  t2_duration "$name" $((SECONDS - t0))

  # Artifacts: metrics before and after the storm, its CPU profiles, the SLO
  # and loadgen's result.
  a=$STACK_DIR/artifacts
  for f in before/logic before/"$victim" after/logic; do
    grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
  done
  for g in "${survivors[@]}"; do
    for f in before/"$g" after/"$g"; do
      grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
    done
  done
  for f in logic "${survivors[@]}" loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/storm/$f.cpu"; done
  grep -q '^# HeapAlloc = ' "$a/ramp/loadgen.heap.txt" 2>/dev/null || missing+=" $name/ramp/loadgen.heap.txt"
  [ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" $name/slo-death.json"
  [ "$(lg_true "$res" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "per-shard Redis CPU and p99 with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)" \
  "b_stress's survivors start empty: no survivor carries its own load through the 600k storm (#93)" \
  "b2 and slowed Logic (Plan 6b2b)"
t2_finish
