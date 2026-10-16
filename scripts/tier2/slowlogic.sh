#!/usr/bin/env bash
# Tier 2: slowlogic (Plan 6b2b-slow), the spec's load test "Logic slowed
# down" (P5bI-2) with SDK clients: Logic runs under a CPU quota (cgroup
# cpu.max through a systemd user scope, as a Kubernetes CPU limit; roadmap
# #94, amended), on two gateways, each phase on a fresh stack:
#   calibrate: SDK clients offered well above Logic's capacity, behind a
#       small Login-queue cap, so no queued Login expires; Logic's saturated
#       login throughput T is the rate of its ok logins while its CPU sits
#       at the quota.
#   storm: each gateway's -login-queue-cap set by the spec's rule for the
#       gateway that takes the storm, T per gateway x (login_deadline - 3s
#       margin) = T x 2s, since gw-0 is then alone and Logic serves it at T
#       (#115: at T x 4s, the 1s margin, four of five runs expired
#       2.7-19.6%: Logic's storm pace ran at 0.69-1.04 T, with dips over a
#       few seconds to 0.72 T in run 29h; 2T and 2.69T passed);
#       2 x 30T clients ramped at T/2 per second (Logic at half load), then
#       gw-1 SIGKILLed: its 30T clients come back over the SDK's 0-20s storm
#       wait, at 1.5T per second, onto gw-0 (#113). Logins beyond Logic's
#       pace queue up to the cap (a queued Login waits up to 2s at T, the
#       rule's bound), and the rest are answered Busy: rejected as Busy, not
#       EXPIRED (R4H-O2). At 30T, Logic needs >= 30s to serve them, so the
#       30s SLO is recorded, not gated (#113). Busy needs >= ~27T victims
#       to reach its floor (0.1 x victims), so a population capped below
#       that is refused, not run.
# Logic is one process (minReplicas, no HPA).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=slowlogic
#   Results: tier2-results/slowlogic-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/slowlogic-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 15 minutes (at T ~3,000/s).
#
# Settings:
#   SLOWLOGIC_QUOTA        Logic's CPUQuota (default 25%: ~3,000 logins/s from run
#                          18's 0.41 cores at 5,000/s; roadmap #94, amended)
#   SLOWLOGIC_CAL_CLIENTS  calibrate: SDK clients (default 120000)
#   SLOWLOGIC_CAL_RATE     calibrate: clients started per second (default 8000,
#                          above Logic's ~3,000/s at 25%)
#   SLOWLOGIC_CAL_QUEUE_CAP calibrate: each gateway's -login-queue-cap (default
#                          1000: under 1s of Logic's pace per gateway at ~1,500/s,
#                          so no Login waits out its deadline and Logic spends no
#                          CPU on logins that come back EXPIRED; up to 5 Login RPCs
#                          of 200 in flight per gateway, enough to keep 0.25 cores
#                          busy, which calibrate_saturated checks). The default
#                          8,192 made T ~30% low in the prototype, and the storm's
#                          Busy marginal
#   SLOWLOGIC_CAL_SKIP     calibrate: seconds from loadgen's start to the window
#                          (default 5: the Login queues fill first)
#   SLOWLOGIC_CAL_WINDOW   calibrate: the measured window, seconds (default 10)
#   SLOWLOGIC_VICTIMS      storm: clients on each gateway (default 30 x T, at most
#                          SLOWLOGIC_MAX_VICTIMS)
#   SLOWLOGIC_MAX_VICTIMS  storm: the most victims the memory budget covers (default
#                          120000: 240k SDK clients, ~5GB in loadgen); below 27 x T
#                          the storm is refused (storm_size): lower SLOWLOGIC_QUOTA
#   SLOWLOGIC_RATE         storm: clients started per second (default T/2)
#   SLOWLOGIC_QUEUE_CAP    storm: each gateway's -login-queue-cap (default
#                          T x 2, the spec's rule for the lone survivor, #115)
#   SLOWLOGIC_LOGIN_RPCS   storm: each gateway's -login-rpcs (default: the gateway's,
#                          16); with SLOWLOGIC_LOGIN_BATCH (-login-batch, 200) the
#                          Logins inside Logic at once, ~1s of work at 25%
#                          (#115's diagnostic, run 29g)
#   SLOWLOGIC_BUSY_WINDOW  storm: seconds from the kill over which Logic's own
#                          pace T' (ok + replaced logins per second) and cores
#                          are recorded (default 20, the SDK's storm wait)
#   SLOWLOGIC_BACK_WAIT    storm: seconds after the kill to wait for every victim
#                          (default 600: Busy 5 times in a row hands a client to
#                          the SDK's exponential backoff, capped at 5 min)
#   Every number setting is whole (SLOWLOGIC_CAL_SKIP may be 0, the rest
#   positive), refused otherwise before anything starts (P6b2b-slowI-2).
#
# Checks:
#   calibrate_stack     redis, devtoken, Logic and both gateways become Ready
#   slow_scope          Logic runs under the quota (its cgroup's cpu.max) and
#                       on its CPU set (once, in calibrate); Logic's GOMAXPROCS
#                       is recorded (Go derives it from the quota)
#   calibrate_saturated Logic's CPU in the window was at least 0.9 x the
#                       quota, so T is Logic's pace, not the offered load's,
#                       and wasted logins (Logic's expired, the gateways'
#                       expired_queued and error) were at most 1% of its ok;
#                       Logic's cgroup cpu.stat over the window (throttling)
#                       and its other login results are recorded; fails if
#                       loadgen did not start
#   calibrate_lease     both gateways renewed their leases every interval
#                       while Logic was saturated (no missing or error; the
#                       5s call timeout is the bound, latency not measured),
#                       and Logic saw no renewal error; ~16-19s, so one
#                       renewal each (storm_lease covers gw-0 longer)
#   calibrate_idle      after loadgen stops, every gateway reaches 0
#                       connections and settles its Logouts; dropped Logouts
#                       count as settled and are recorded (roadmap #94)
#   calibrate_processes nothing died, panicked or stopped uncleanly
#   storm_size          30 x T victims (or the capped number) is at least
#                       27 x T, so Busy can reach its floor; recorded with
#                       whether the cap clamped it
#   storm_stack         as calibrate_stack, with the queue cap set
#   storm_online        every client logs in, each gateway holds 45-55%
#   storm_busy          gw-0 answered Busy at its Login-queue cap in the storm,
#                       at least 0.1 x victims times
#   storm_expired       gw-0's EXPIRED logins in the storm (Logic's and the
#                       queue's, expired_queued) are at most 0.1% of its ok
#                       logins: Busy, not EXPIRED (R4H-O2); gw-0's Login
#                       timeouts (error: answered Busy, so not EXPIRED, but a
#                       wait past the deadline the rule bounds) are recorded
#   storm_back          every victim logs in again within SLOWLOGIC_BACK_WAIT,
#                       and gw-0 rejected none at capacity; the 30s SLO from
#                       the kill is recorded (#113)
#   storm_lease         gw-0 renews its lease every interval through the
#                       storm (own connection and worker pool, same quota,
#                       R1-O2; the 5s call timeout is the bound; the same
#                       floor as calibrate_lease), and Logic saw no renewal
#                       error
#   no_swap_in_storm    no page swapped in from the kill to the last victim (#70)
#   storm_idle          as calibrate_idle
#   storm_processes     as calibrate_processes
#   artifacts           metrics before and after each window, the storm's CPU
#                       profiles, the SLO, loadgen's results and gw-0's Login
#                       series (logins-gw-0.tsv)
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init slowlogic
t2_need curl taskset sudo python3 systemd-run
t2_require_host_setup

QUOTA=${SLOWLOGIC_QUOTA:-25%}
case $QUOTA in
  [1-9]% | [1-9][0-9]% | 100%) ;;
  *) echo "slowlogic: SLOWLOGIC_QUOTA must be 1%-100%, got '$QUOTA'" >&2; exit 1 ;;
esac
# The quota needs the user manager to delegate the cpu controller (the Tier 2
# host's does: user@UID.service/cgroup.controllers lists cpu).
systemd-run --user --scope --quiet -p "CPUQuota=$QUOTA" true 2>/dev/null ||
  t2_refuse "refused: systemd-run --user --scope cannot set CPUQuota=$QUOTA (does /sys/fs/cgroup/user.slice/user-$(id -u).slice/user@$(id -u).service/cgroup.controllers list cpu?)"
# Every number setting lands in arithmetic and in result.json unquoted
# (P6b2b-slowI-2, as gwloss's P6b2b-b2I-2).
for v in SLOWLOGIC_CAL_CLIENTS SLOWLOGIC_CAL_RATE SLOWLOGIC_CAL_QUEUE_CAP SLOWLOGIC_CAL_WINDOW \
  SLOWLOGIC_VICTIMS SLOWLOGIC_MAX_VICTIMS SLOWLOGIC_RATE SLOWLOGIC_QUEUE_CAP \
  SLOWLOGIC_BUSY_WINDOW SLOWLOGIC_BACK_WAIT SLOWLOGIC_LOGIN_RPCS SLOWLOGIC_LOGIN_BATCH; do
  case ${!v-1} in ''|*[!0-9]*|0) echo "slowlogic: $v must be a positive whole number" >&2; exit 1 ;; esac
done
case ${SLOWLOGIC_CAL_SKIP-0} in ''|*[!0-9]*) echo "slowlogic: SLOWLOGIC_CAL_SKIP must be whole seconds" >&2; exit 1 ;; esac
t2_mem_budget 10
t2_nofile 1048576

export STACK_PIN=1 STACK_GATEWAYS=2 STACK_LOGIC_CPU_QUOTA=$QUOTA
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

GW_COUNTERS=(
  logins_ok='goim_gateway_logins_total:result="ok"'
  logins_busy='goim_gateway_logins_total:result="busy"'
  logins_expired='goim_gateway_logins_total:result="expired"'
  logins_expired_queued='goim_gateway_logins_total:result="expired_queued"'
  logins_error='goim_gateway_logins_total:result="error"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
)
LOGIC_COUNTERS=(
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  logins_expired='goim_logic_logins_total:result="expired"'
  logins_all='goim_logic_logins_total' # every result: logins_other (P6b2b-slowI-4)
  prepare_s='goim_logic_login_prepare_seconds_sum' # a batch's token checks (#115 diagnostic)
  prepare_n='goim_logic_login_prepare_seconds_count'
  lease_error='goim_logic_lease_renewals_total:result="error"'
  kick_attempts='goim_logic_kick_attempts_total'
  kick_dropped='goim_logic_kick_dropped_total'
)

# lease_floor SECONDS: the fewest renewals a window of SECONDS (whole) must
# hold. A renewal counts when its call ends, at most 5s (the call timeout)
# after its 10s tick, so (SECONDS - 6) // 10 of them (1s for SECONDS'
# resolution), and never fewer than SECONDS // 10 - 1 (P6b2b-slowI-5).
lease_floor() {
  local s=$1
  echo $((s / 10 - 1 > (s - 6) / 10 ? s / 10 - 1 : (s - 6) / 10))
}

# The other Logic login results (superseded, stale, kicked, error...) in a
# stack_counters_delta of LOGIC_COUNTERS: Logic work that is neither T's nor
# T''s (P6b2b-slowI-4).
LOGINS_OTHER='None if d["logins_all"] is None else d["logins_all"] - sum(d[k] or 0 for k in ("logins_ok", "logins_replaced", "logins_expired"))'

# hist_ms A B: a histogram between two stack_buckets readings as {"p50_ms",
# "p90_ms", "p99_ms", "max_ms"}, bucket bounds. Read for Logic's
# login_age_seconds (each entry's age from recv_ms as its batch goes to the
# store) and gw-0's login_queue_wait_seconds / login_logic_wait_seconds (an
# entry's wait before and after its Login RPC's send); with the counters'
# delta D, the batches' mean token-check time: prepare_ms D (#115 diagnostic).
hist_ms() {
  echo "{\"p50_ms\": $(stack_hist_quantile "$1" "$2" 0.5), \"p90_ms\": $(stack_hist_quantile "$1" "$2" 0.9), \"p99_ms\": $(stack_hist_quantile "$1" "$2" 0.99), \"max_ms\": $(stack_hist_quantile "$1" "$2" 1)}"
}
GW_WAITS=(goim_gateway_login_queue_wait_seconds goim_gateway_login_logic_wait_seconds)
# gw_waits NAME: NAME's two wait histograms as "QUEUE LOGIC", two
# stack_buckets readings with their spaces removed, for gw_wait_json.
gw_waits() { echo "$(stack_buckets "$1" "${GW_WAITS[0]}" | tr -d ' ')" "$(stack_buckets "$1" "${GW_WAITS[1]}" | tr -d ' ')"; }
# gw_wait_json "A" "B": {"queue": hist_ms, "logic": hist_ms} between two gw_waits.
gw_wait_json() {
  local a=($1) b=($2)
  echo "{\"queue\": $(hist_ms "${a[0]}" "${b[0]}"), \"logic\": $(hist_ms "${a[1]}" "${b[1]}")}"
}

# login_sampler NAME FILE: appends "unix_ms done queue" every second, from
# NAME's metrics: its Logins with a reply (every logins_total result but
# busy) and its login_queue gauge (#115 tail diagnostic: Logic's pace second
# by second, and the queue it drains).
login_sampler() {
  local body
  while :; do
    body=$(stack_metrics "$1") && awk -v t="$(date +%s%3N)" '
      index($1, "goim_gateway_logins_total{") == 1 && $1 !~ /result="busy"/ { d += $NF }
      $1 == "goim_gateway_login_queue" { q = $NF }
      END { printf "%s %.0f %.0f\n", t, d, q }' <<<"$body" >>"$2"
    sleep 1
  done
}
# login_series FILE: the replies per second between samples, as {"n",
# "rate_min", "rate_p10", "rate_p50", "rate_max", "queue_max", "series":
# [[s since the first sample, replies/s, queue], ...]} (first 180 samples).
login_series() {
  python3 -c 'import json, sys
rows = [tuple(int(x) for x in l.split()) for l in open(sys.argv[1]) if len(l.split()) == 3]
pts = [((t1 - rows[0][0]) / 1000, (d1 - d0) * 1000 / (t1 - t0), q1)
       for (t0, d0, _), (t1, d1, q1) in zip(rows, rows[1:]) if t1 > t0]
if not pts:
    print("null"); sys.exit()
r = sorted(p[1] for p in pts)
q = lambda f: round(r[min(len(r) - 1, int(f * len(r)))])
print(json.dumps({"n": len(pts), "rate_min": round(r[0]), "rate_p10": q(0.1), "rate_p50": q(0.5), "rate_max": round(r[-1]),
       "queue_max": max(p[2] for p in pts),
       "series": [[round(t, 1), round(v), qq] for t, v, qq in pts[:180]]}))' "$1" 2>/dev/null || echo null
}
prepare_ms() { echo "$1" | lg_py - 'round(1000 * d["prepare_s"] / d["prepare_n"], 1) if d["prepare_n"] else None'; }

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# up NAME: a fresh stack in $T2_OUT/NAME; checks NAME_stack.
up() {
  export STACK_DIR=$T2_OUT/$1
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  local busy
  busy=$(stack_busy_ports)
  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
  stack_up 2>"$STACK_DIR/stack-up.err"
  local rc=$?
  t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
  return $rc
}

# down NAME: loadgen stopped, the gateways idle (dropped Logouts settled and
# recorded), the stack stopped; checks NAME_idle and NAME_processes.
down() {
  lg_stop
  local t=$SECONDS idle stop_s logouts="" g body problems
  idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
    body=$(stack_metrics "$g") || body=""
    logouts+="${logouts:+, }\"$g\": {\"done\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"'), \"dropped\": $(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"'), \"logins_ok\": $(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"')}"
  done
  t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"logouts\": {$logouts}, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${1}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))
quota_cores=$(python3 -c 'import sys; print(int(sys.argv[1].rstrip("%")) / 100)' "$QUOTA")
missing=""

# Calibrate: Logic's saturated login throughput at the quota. A small queue
# cap keeps every queued Login inside its deadline: the excess is answered
# Busy at the gateway, and Logic's CPU goes only to logins that count.
t0=$SECONDS
cal_clients=${SLOWLOGIC_CAL_CLIENTS:-120000} cal_rate=${SLOWLOGIC_CAL_RATE:-8000}
cal_cap=${SLOWLOGIC_CAL_QUEUE_CAP:-1000}
skip=${SLOWLOGIC_CAL_SKIP:-5} win=${SLOWLOGIC_CAL_WINDOW:-10}
STACK_GATEWAY_ARGS=(-max-conns $((cal_clients + 10000)) -login-queue-cap "$cal_cap")
up calibrate || t2_finish
cpu_max=$(stack_cpu_max logic)
want_max="$(python3 -c 'import sys; print(round(float(sys.argv[1]) * 100000))' "$quota_cores") 100000"
gomaxprocs=$(stack_metric logic go_sched_gomaxprocs_threads)
wrong=""
for g in gw-0 gw-1; do stack_pinned "$g" "$T2_CPUS_GATEWAY" || wrong+=" $g"; done
for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
scope=false
[ "$cpu_max" = "$want_max" ] && [ -z "$wrong" ] && scope=true
t2_check slow_scope "$scope" \
  "\"quota\": $(t2_json_str "$QUOTA"), \"cpu_max\": $(t2_json_str "$cpu_max"), \"want\": $(t2_json_str "$want_max"), \"logic_gomaxprocs\": $gomaxprocs, \"wrong_pinning\": $(t2_json_str "${wrong# }")"
# calibrate_lease's counters cover the whole saturated stretch, from
# loadgen's start to its stop.
lease0=$(stack_counters logic "${LOGIC_COUNTERS[@]}") lease_t=$SECONDS
declare -A lg0=() lg1=() g0=() g1=()
for g in gw-0 gw-1; do lg0[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
if ! lg_start calibrate -mode sdk -clients "$cal_clients" -rate "$cal_rate" -uid-prefix cal; then
  t2_check calibrate_saturated false "\"error\": \"loadgen did not start, see calibrate/logs/loadgen-calibrate.log\""
  down calibrate
  t2_finish
fi
sleep "$skip"
stack_snapshot cal_before
# Logic's counters and cpu.stat right around the usage window, so T's count
# and its window_s cover the same span; the gateways first (P6b2b-slowI-3).
for g in gw-0 gw-1; do g0[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
cs0=$(stack_cpu_stat logic)
age0=$(stack_buckets logic goim_logic_login_age_seconds)
w0=$(gw_waits gw-0)
l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
stack_usage_start loadgen="$LG_PID"
# The window's CPU clocks: T depends on them (run 29d; #115 diagnostic).
t2_clock_sampler "$STACK_DIR/clocks-calibrate.tsv" &
clk=$!
t2_on_exit '[ -n "${clk:-}" ] && kill "$clk" 2>/dev/null'
sleep "$win"
usage=$(stack_usage_json)
l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
kill "$clk" 2>/dev/null; wait "$clk" 2>/dev/null; clk=""
age1=$(stack_buckets logic goim_logic_login_age_seconds)
w1=$(gw_waits gw-0)
cs1=$(stack_cpu_stat logic)
for g in gw-0 gw-1; do g1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
stack_snapshot cal_after
logic_d=$(stack_counters_delta "$l0" "$l1")
gws_d="{\"gw-0\": $(stack_counters_delta "${g0[gw-0]}" "${g1[gw-0]}"), \"gw-1\": $(stack_counters_delta "${g0[gw-1]}" "${g1[gw-1]}")}"
# T: Logic's ok logins per second over the window, whole.
T=$(echo "{\"l\": $logic_d, \"u\": $usage}" | lg_py - 'int(d["l"]["logins_ok"] / d["u"]["window_s"])' 2>/dev/null)
logic_cores=$(echo "$usage" | lg_py - 'd["logic"]["cores"]' 2>/dev/null)
# Wasted logins burn the quota too, so the CPU alone does not prove T:
# Logic's expired and the gateways' expired_queued and error are at most 1%
# of its ok logins (O4 of the plan's review).
waste=$(echo "{\"l\": $logic_d, \"g\": $gws_d}" | lg_py - '{"logic_expired": d["l"]["logins_expired"] or 0, "expired_queued": sum(d["g"][g]["logins_expired_queued"] or 0 for g in d["g"]), "error": sum(d["g"][g]["logins_error"] or 0 for g in d["g"])}' 2>/dev/null)
saturated=$(echo "{\"c\": ${logic_cores:-null}, \"w\": ${waste:-null}, \"l\": $logic_d}" | lg_true - 'd["c"] is not None and d["c"] >= 0.9 * '"$quota_cores"' and d["w"] is not None and sum(d["w"].values()) <= 0.01 * (d["l"]["logins_ok"] or 0)')
[ -n "$T" ] && [ "$T" -gt 0 ] 2>/dev/null || { T=null saturated=false; }
t2_check calibrate_saturated "$saturated" "\"throughput_per_s\": $T, \"logic_cores\": ${logic_cores:-null}, \"quota_cores\": $quota_cores, \"waste\": ${waste:-null}, \"waste_max\": $(echo "$logic_d" | lg_py - '0.01 * (d["logins_ok"] or 0)'), \"window_s\": $(echo "$usage" | lg_py - 'd["window_s"]'), \"logins_other\": $(echo "$logic_d" | lg_py - "$LOGINS_OTHER"), \"cpu_stat\": $(stack_counters_delta "$cs0" "$cs1"), \"cpu_clocks\": $(t2_clock_summary "$STACK_DIR/clocks-calibrate.tsv"), \"login_age\": $(hist_ms "$age0" "$age1"), \"gw0_wait\": $(gw_wait_json "$w0" "$w1"), \"prepare_ms\": $(prepare_ms "$logic_d")"
t2_field calibrate "{\"clients\": $cal_clients, \"rate\": $cal_rate, \"queue_cap\": $cal_cap, \"skip_s\": $skip, \"throughput_per_s\": $T, \"logic_gomaxprocs\": $gomaxprocs, \"usage\": $usage, \"logic\": $logic_d, \"cpu_stat\": $(stack_counters_delta "$cs0" "$cs1"), \"gateways\": $gws_d, \"loadgen_online\": $(lg_get /stats 'd["online"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
lease1=$(stack_counters logic "${LOGIC_COUNTERS[@]}") lease_s=$((SECONDS - lease_t))
for g in gw-0 gw-1; do lg1[$g]=$(stack_counters "$g" "${GW_COUNTERS[@]}"); done
# 1 over the ~16s calibration, where lease_s // 10 - 1 alone would be 0.
lease_floor=$(lease_floor "$lease_s")
lease_d="{\"gw-0\": $(stack_counters_delta "${lg0[gw-0]}" "${lg1[gw-0]}"), \"gw-1\": $(stack_counters_delta "${lg0[gw-1]}" "${lg1[gw-1]}"), \"logic\": $(stack_counters_delta "$lease0" "$lease1")}"
t2_check calibrate_lease "$(echo "$lease_d" | lg_true - 'all(d[g]["lease_error"] == 0 and d[g]["lease_missing"] == 0 and d[g]["lease_live"] >= '"$lease_floor"' for g in ("gw-0", "gw-1")) and d["logic"]["lease_error"] == 0')" \
  "\"elapsed_s\": $lease_s, \"live_floor\": $lease_floor, \"gateways\": $(echo "$lease_d" | lg_py - '{g: {"live": d[g]["lease_live"], "missing": d[g]["lease_missing"], "error": d[g]["lease_error"]} for g in ("gw-0", "gw-1")}'), \"logic_lease_error\": $(echo "$lease_d" | lg_py - 'd["logic"]["lease_error"]')"
down calibrate
t2_duration calibrate $((SECONDS - t0))
for f in cal_before/logic cal_after/logic; do
  grep -q '^# TYPE' "$T2_OUT/calibrate/artifacts/$f.metrics" 2>/dev/null || missing+=" calibrate/$f.metrics"
done
[ "$saturated" = true ] || { t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"; t2_finish; }

# The storm, sized from T (#113).
t0=$SECONDS
# The population, capped by the memory budget (SLOWLOGIC_MAX_VICTIMS). Busy
# reaches its floor only from ~27T victims: over the 0-20s storm wait they
# arrive at V/20 per second against a drain of T, a 4T queue fills after
# 4T / (V/20 - T) seconds, and first attempts turned away after that come
# to 20T(V/20T - 1) - 4T >= 0.1V for V >= ~26.7T (a smaller cap needs fewer:
# 2T ~24.4T).
max_victims=${SLOWLOGIC_MAX_VICTIMS:-120000} clamped=false
victims=${SLOWLOGIC_VICTIMS:-$((30 * T))}
[ -z "${SLOWLOGIC_VICTIMS:-}" ] && [ "$victims" -gt "$max_victims" ] && victims=$max_victims clamped=true
need=$((27 * T))
t2_check storm_size "$(t2_bool [ "$victims" -ge "$need" ])" "\"throughput_per_s\": $T, \"victims\": $victims, \"need\": $need, \"max_victims\": $max_victims, \"clamped\": $clamped, \"victims_per_t\": $(python3 -c 'import sys; print(round(int(sys.argv[1]) / int(sys.argv[2]), 1))' "$victims" "$T")"
[ "$victims" -ge "$need" ] || { t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"; t2_finish; }
clients=$((2 * victims))
rate=${SLOWLOGIC_RATE:-$((T / 2))}
qcap=${SLOWLOGIC_QUEUE_CAP:-$((T * 2))}
back_wait=${SLOWLOGIC_BACK_WAIT:-600}
busy_win=${SLOWLOGIC_BUSY_WINDOW:-20}
STACK_GATEWAY_ARGS=(-max-conns $((clients + clients / 10)) -login-queue-cap "$qcap"
  ${SLOWLOGIC_LOGIN_RPCS:+-login-rpcs "$SLOWLOGIC_LOGIN_RPCS"} ${SLOWLOGIC_LOGIN_BATCH:+-login-batch "$SLOWLOGIC_LOGIN_BATCH"})
up storm || t2_finish
lg_start storm -mode sdk -clients "$clients" -rate "$rate" -uid-prefix slow
online=false
lg_wait_online "$clients" $((clients / rate * 3 + 60)) &&
  online=$(lg_get /stats 'd["online"] == '"$clients"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$clients"' * 0.45')
[ "$online" = true ] || online=false
by_gateway=$(lg_get /stats 'd["online_by_gateway"]')
ramp_gw=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_check storm_online "$online" "\"want\": $clients, \"by_gateway\": $by_gateway, \"rate\": $rate, \"failed\": $(lg_get /stats 'd["failed"]'), \"gw0_ramp\": $ramp_gw"
if [ "$online" != true ]; then
  down storm
  t2_duration storm $((SECONDS - t0))
  t2_finish
fi

stack_snapshot before
s0=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
storm_cpu_max=$(stack_cpu_max logic)
cs0=$(stack_cpu_stat logic)
age0=$(stack_buckets logic goim_logic_login_age_seconds)
w0=$(gw_waits gw-0)
l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
t2_record_start storm
stack_usage_start loadgen="$LG_PID"
reply=$(lg_post "/kill?gateway=gw-1&pid=${STACK_PIDS[gw-1]}&mark=death" | lg_py - d)
kill_t=$SECONDS
login_sampler gw-0 "$STACK_DIR/artifacts/logins-gw-0.tsv" &
smp=$!
t2_on_exit '[ -n "${smp:-}" ] && kill "$smp" 2>/dev/null'
# T': Logic's own pace while the storm keeps it saturated (relogins that
# replace gw-1's entries, each with a Kick to the dead address), against the
# calibration's fresh logins. Its own usage window, in a subshell, leaves
# the storm's whole-window reading alone.
(c0=$(stack_cpu_stat logic); b0=$(stack_counters logic "${LOGIC_COUNTERS[@]}"); stack_usage_start; sleep "$busy_win"
  u=$(stack_usage_json); b1=$(stack_counters logic "${LOGIC_COUNTERS[@]}"); c1=$(stack_cpu_stat logic)
  echo "{\"u\": $u, \"d\": $(stack_counters_delta "$b0" "$b1"), \"c\": $(stack_counters_delta "$c0" "$c1")}" >"$STACK_DIR/busy.json") >/dev/null 2>&1 &
busy_pid=$!
stack_expect_dead gw-1 60
dead=$?
(stack_profile storm 30 & lg_profile storm 30 & wait) >/dev/null 2>&1 &
prof=$!
back=$(t2_bool lg_wait_back death "$back_wait")
back_s=$((SECONDS - kill_t))
kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
lg_get "/slo?mark=death&within=30s" d >"$STACK_DIR/artifacts/slo-death.json"
wait "$prof" "$busy_pid"
usage=$(stack_usage_json)
busy_window=$(lg_py "$STACK_DIR/busy.json" '{"window_s": d["u"]["window_s"], "throughput_per_s": int(((d["d"]["logins_ok"] or 0) + (d["d"]["logins_replaced"] or 0)) / d["u"]["window_s"]), "logic_cores": d["u"]["logic"]["cores"], "logins_other": (lambda d: '"$LOGINS_OTHER"')(d["d"]), "prepare_ms": round(1000 * d["d"]["prepare_s"] / d["d"]["prepare_n"], 1) if d["d"]["prepare_n"] else None, "cpu_stat": d["c"], "logic": d["d"]}' 2>/dev/null) || busy_window=null
[ -n "$busy_window" ] || busy_window=null
l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
age1=$(stack_buckets logic goim_logic_login_age_seconds)
w1=$(gw_waits gw-0)
cs1=$(stack_cpu_stat logic)
s1=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
t2_record_end storm
stack_snapshot after
logic_d=$(stack_counters_delta "$l0" "$l1")
gw_d=$(stack_counters_delta "$s0" "$s1")
window_s=$(echo "$usage" | lg_py - 'd["window_s"]')
slo=$STACK_DIR/artifacts/slo-death.json
storm_floor=null # an unreadable window fails storm_lease
[[ $window_s =~ ^[0-9]+(\.[0-9]+)?$ ]] && storm_floor=$(lease_floor "${window_s%.*}")

t2_check storm_busy "$(echo "$gw_d" | lg_true - '(d["logins_busy"] or 0) >= 0.1 * '"$victims")" \
  "\"busy\": $(echo "$gw_d" | lg_py - 'd["logins_busy"]'), \"floor\": $((victims / 10)), \"rejects_login_queue\": $(echo "$gw_d" | lg_py - 'd["rejects_login_queue"]'), \"queue_cap\": $qcap"
t2_check storm_expired "$(echo "$gw_d" | lg_true - '(d["logins_expired"] or 0) + (d["logins_expired_queued"] or 0) <= 0.001 * d["logins_ok"]')" \
  "\"expired\": $(echo "$gw_d" | lg_py - 'd["logins_expired"]'), \"expired_queued\": $(echo "$gw_d" | lg_py - 'd["logins_expired_queued"]'), \"ok\": $(echo "$gw_d" | lg_py - 'd["logins_ok"]'), \"error\": $(echo "$gw_d" | lg_py - 'd["logins_error"]'), \"logic_expired\": $(echo "$logic_d" | lg_py - 'd["logins_expired"]')"
pass=false
[ $dead -eq 0 ] && [ "$back" = true ] && [ "$(echo "$gw_d" | lg_py - 'd["rejects_at_capacity"]')" = 0 ] &&
  pass=$(lg_true "$slo" 'd["population"] == '"$victims"' and d["back"] == d["population"]')
t2_check storm_back "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"backoff\": $(lg_get /stats 'd["backoff_since_mark"]'), \"slo\": $(cat "$slo")"
t2_check storm_lease "$(echo "{\"g\": $gw_d, \"l\": $logic_d, \"w\": $window_s}" | lg_true - 'd["g"]["lease_error"] == 0 and d["g"]["lease_missing"] == 0 and d["g"]["lease_live"] >= '"$storm_floor"' and d["l"]["lease_error"] == 0')" \
  "\"window_s\": $window_s, \"live_floor\": $storm_floor, \"gw-0\": {\"live\": $(echo "$gw_d" | lg_py - 'd["lease_live"]'), \"missing\": $(echo "$gw_d" | lg_py - 'd["lease_missing"]'), \"error\": $(echo "$gw_d" | lg_py - 'd["lease_error"]')}, \"logic_lease_error\": $(echo "$logic_d" | lg_py - 'd["lease_error"]')"
t2_field storm "{\"throughput_per_s\": $T, \"busy_window\": $busy_window, \"victims\": $victims, \"clamped\": $clamped, \"clients\": $clients, \"rate\": $rate, \"queue_cap\": $qcap, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"by_gateway_at_kill\": $by_gateway, \"back_s\": $back_s, \"usage\": $usage, \"cpu_max\": $(t2_json_str "$storm_cpu_max"), \"cpu_stat\": $(stack_counters_delta "$cs0" "$cs1"), \"logins_other\": $(echo "$logic_d" | lg_py - "$LOGINS_OTHER"), \"login_age\": $(hist_ms "$age0" "$age1"), \"gw0_wait\": $(gw_wait_json "$w0" "$w1"), \"logins_per_s\": $(login_series "$STACK_DIR/artifacts/logins-gw-0.tsv"), \"prepare_ms\": $(prepare_ms "$logic_d"), \"logic\": $logic_d, \"gw-0\": $gw_d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]'), \"backoff\": $(lg_get /stats '{"clients": d["backoff_clients"], "waits": d["backoff_waits"]}')}"
down storm
t2_duration storm $((SECONDS - t0))

a=$T2_OUT/storm/artifacts
for f in before/logic before/gw-0 after/logic after/gw-0; do
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" storm/$f.metrics"
done
for f in logic gw-0 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" storm/$f.cpu"; done
[ "$(lg_true "$a/slo-death.json" '"population" in d')" = true ] || missing+=" storm/slo-death.json"
[ "$(lg_true "$a/loadgen-storm.json" '"mode" in d')" = true ] || missing+=" storm/loadgen-storm.json"
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "Logic scaled out under load (HPA from minReplicas): one Logic process here (Tier 3, Plan 6d)" \
  "a slowed Redis, or Logic slowed by something other than CPU (Redis Cluster failover in Plan 6c)" \
  "the 30s SLO under a slowed Logic: recorded, not gated (#113)"
t2_finish
