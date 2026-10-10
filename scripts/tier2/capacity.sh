#!/usr/bin/env bash
# Tier 2: capacity (Plan 6b1), the spec's load tests (a) and (c) with light
# clients on ONE gateway that has the whole gateway CPU set (6 Zen 5c cores,
# 12 CPUs, GOMAXPROCS 12; #71, #79):
#   (a) 600k connections on one gateway (the ceiling), with GOMEMLIMIT/GOGC
#       tuning: once with Go's defaults and once tuned (#88);
#   (c) heartbeat CPU cost at 200k and at 600k at 150s, plus 60s at 200k.
#       600k at 150s is (a)'s tuned phase.
# Each phase runs on a fresh stack (the gateway's -heartbeat and GC settings
# are fixed at start) and ends at the phase boundary (P6a2bI-3): loadgen
# stopped, every connection closed and every Logout done, before the stack
# stops.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=capacity
#   Results: tier2-results/capacity-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/capacity-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 20 minutes.
#
# Settings:
#   CAPACITY_PHASES    "NAME:CLIENTS:HEARTBEAT_S:GC ..." (GC: default = Go's
#                      defaults, tuned = CAPACITY_GOGC + CAPACITY_GOMEMLIMIT);
#                      default "a_default:600000:150:default a_tuned:600000:150:tuned
#                      c200_150:200000:150:tuned c200_60:200000:60:tuned"
#   CAPACITY_GOGC      tuned GOGC (default off)
#   CAPACITY_GOMEMLIMIT tuned GOMEMLIMIT (default 4GiB)
#   CAPACITY_CAP       the gateway's -max-conns (default 650000)
#   CAPACITY_RATE      clients started per second (default 5000)
#   CAPACITY_WINDOW    measured window in seconds (default: whole heartbeats,
#                      at least 120s: 150s at 150s, 120s at 60s)
#   CAPACITY_RECONCILE_CYCLE the gateway's -reconcile-cycle (default 24h: the
#                      production 10m cycle starts its first pass at a random
#                      point, which would land in some windows and not others;
#                      reconcile_entries records any that did)
#
# Checks, for each phase P:
#   P_stack            redis, devtoken, Logic and gw-0 become Ready
#   P_online           every client logs in on gw-0; none gave up
#   P_pongs            a Pong for every Ping in the window, one per client per heartbeat
#   P_held             no connection lost
#   no_swap_in_P       no page swapped in during the window (#70)
#   P_loadgen_headroom loadgen used under 3/4 of its CPU set in the ramp and the
#                      window, so the gateway, not loadgen, set the pace (P6a2aI-4)
#   P_idle             after loadgen stops, gw-0 reaches 0 connections and 0
#                      awaiting Logout within 300s, with one Logout done per
#                      client (counted, so a missing metric cannot pass)
#   P_processes        nothing died, panicked or stopped uncleanly
# and once: pinning (in the first phase), artifacts.
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init capacity
t2_need curl taskset sudo python3 go
t2_require_host_setup
t2_mem_budget 16
t2_nofile 1048576

PHASES=${CAPACITY_PHASES:-a_default:600000:150:default a_tuned:600000:150:tuned c200_150:200000:150:tuned c200_60:200000:60:tuned}
TUNED=(GOGC=${CAPACITY_GOGC:-off} GOMEMLIMIT=${CAPACITY_GOMEMLIMIT:-4GiB})
CAP=${CAPACITY_CAP:-650000} RATE=${CAPACITY_RATE:-5000}
RECONCILE=${CAPACITY_RECONCILE_CYCLE:-24h}
export STACK_GATEWAYS=1 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# gc_mark_cpu_pct PROFILE: the share of the profile's samples in the GC's
# mark workers and mark assists (sweeping not included), in percent; null if
# go tool pprof cannot read it.
gc_mark_cpu_pct() {
  go tool pprof -top "$1" 2>/dev/null | awk '
    $NF == "runtime.gcBgMarkWorker" || $NF == "runtime.gcAssistAlloc" { s += $5; n++ }
    /^Showing nodes/ { ok = 1 }
    END { if (ok) printf "%.1f\n", s; else print "null" }'
}

# usage_ok JSON: loadgen used under 3/4 of its CPU set.
usage_ok() { [ "$(echo "$1" | lg_true - 'd["loadgen"]["cores"] < '"$LG_CPUS"' * 0.75')" = true ]; }

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

summary=()
first=1
for phase in $PHASES; do
  IFS=: read -r name clients hb gc <<<"$phase"
  window=${CAPACITY_WINDOW:-$(( (120 + hb - 1) / hb * hb ))}
  export STACK_DIR=$T2_OUT/$name
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  STACK_GATEWAY_ARGS=(-max-conns "$CAP" -heartbeat "${hb}s" -reconcile-cycle "$RECONCILE" -reconcile-jitter 0)
  STACK_GATEWAY_ENV=()
  [ "$gc" = tuned ] && STACK_GATEWAY_ENV=("${TUNED[@]}")
  t0=$SECONDS

  # The stack, fresh for each phase.
  busy=$(stack_busy_ports)
  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
  stack_up 2>"$STACK_DIR/stack-up.err"
  up=$?
  t2_check "${name}_stack" "$(t2_bool [ $up -eq 0 ])" "\"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
  [ $up -eq 0 ] || t2_finish

  # The ramp.
  lg_start "$name" -mode light -clients "$clients" -rate "$RATE" -ping-every "${hb}s" -uid-prefix "$name"
  stack_usage_start loadgen="$LG_PID"
  t=$SECONDS
  online=false
  lg_wait_online "$clients" $((clients / RATE * 3 + 60)) && online=$(lg_get /stats 'd["online_by_gateway"].get("gw-0") == '"$clients"' and "gave-up" not in d["failed"]')
  ramp_s=$((SECONDS - t))
  ramp=$(stack_usage_json)
  t2_check "${name}_online" "$online" "\"want\": $clients, \"online\": $(lg_get /stats 'd["online"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"

  if [ -n "$first" ]; then
    first=""
    wrong=""
    stack_pinned gw-0 "$T2_CPUS_GATEWAY" || wrong+=" gw-0"
    for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
    have=$(taskset -pc "$LG_PID" 2>/dev/null)
    [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
    t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
  fi

  # The window: whole heartbeats after a 10s settle, with CPU profiles of
  # its first 30s and the gateway's GC counters at both ends.
  sleep 10 & wait $!
  pongs0=$(lg_get /stats 'd["pongs"]')
  gc0=$(stack_metric gw-0 go_gc_duration_seconds_count)
  pause0=$(stack_metric gw-0 go_gc_duration_seconds_sum)
  rec0=$(stack_metric gw-0 goim_gateway_reconcile_entries_total)
  t2_record_start "$name"
  stack_usage_start loadgen="$LG_PID"
  stack_profile window 30 & p1=$!
  lg_profile window 30 & p2=$!
  sleep "$window" & wait $!
  wait $p1; wait $p2
  win=$(stack_usage_json)
  t2_record_end "$name"
  # Reconcile entries in the window (no sample until the first entry, so
  # null at both ends is 0; null at one end only is unknown).
  rec1=$(stack_metric gw-0 goim_gateway_reconcile_entries_total)
  reconcile=$(python3 -c 'import sys; a, b = sys.argv[1:3]
print(0 if a == b == "null" else "null" if "null" in (a, b) else int(float(b) - float(a)))' "$rec0" "$rec1")
  pongs1=$(lg_get /stats 'd["pongs"]')
  gw=$(python3 -c 'import json, sys
def n(s):
    if s == "null": return None
    v = float(s)
    return int(v) if v.is_integer() else v
gc0, gc1, p0, p1 = map(n, sys.argv[1:5])
print(json.dumps({"gc_cycles_before_window": gc0,
  "gc_cycles": None if None in (gc0, gc1) else gc1 - gc0,
  "gc_pause_s": None if None in (p0, p1) else round(p1 - p0, 4),
  "gc_pause_max_s": n(sys.argv[5]), "gc_mark_cpu_pct": n(sys.argv[6]),
  "heap_inuse_mb": None if sys.argv[7] == "null" else int(n(sys.argv[7]) / 2**20),
  "next_gc_mb": None if sys.argv[8] == "null" else int(n(sys.argv[8]) / 2**20),
  "goroutines": n(sys.argv[9])}))' \
    "$gc0" "$(stack_metric gw-0 go_gc_duration_seconds_count)" "$pause0" "$(stack_metric gw-0 go_gc_duration_seconds_sum)" \
    "$(stack_metric gw-0 go_gc_duration_seconds 'quantile="1"')" "$(gc_mark_cpu_pct "$STACK_DIR/artifacts/window/gw-0.cpu.pb.gz")" \
    "$(stack_metric gw-0 go_memstats_heap_inuse_bytes)" "$(stack_metric gw-0 go_memstats_next_gc_bytes)" "$(stack_metric gw-0 go_goroutines)")
  stack_snapshot window
  # The live heap: after the window, a forced GC (with GOGC=off the heap may
  # not have been collected since the ramp), then heap in use and a heap
  # profile of what is live, for bytes per connection and pooling.
  curl -sf -m 120 "http://$(stack_admins | awk '$1 == "gw-0" { print $2 }')/debug/pprof/heap?gc=1" >"$STACK_DIR/artifacts/window/gw-0.heap-live.pb.gz"
  live=$(stack_metric gw-0 go_memstats_heap_inuse_bytes)
  live_mb=$([ "$live" = null ] && echo null || echo $((live / 1048576)))
  per_conn=$([ "$live" = null ] && echo null || echo $((live / clients)))
  pings=$(python3 -c 'import sys; print(int(sys.argv[2]) - int(sys.argv[1]))' "$pongs0" "$pongs1" 2>/dev/null || echo null)
  per=$(python3 -c 'import json, sys
w, p = json.loads(sys.argv[1]), int(sys.argv[2])
print(round(w["gw-0"]["cpu_s"] / p * 1e6, 1) if p > 0 and w.get("gw-0") else "null")' "$win" "$pings" 2>/dev/null || echo null)

  # The phase boundary (P6a2bI-3): the result is written before any client
  # stops (#84), then the stop is timed until gw-0 is idle.
  lg_stop
  res=$STACK_DIR/artifacts/loadgen-$name.json
  t2_check "${name}_pongs" "$(lg_true "$res" 'd["pong_missing"] == 0 and '"$pings"' >= '"$clients"' * '"$window"' // '"$hb"' * 0.99')" \
    "\"pings_in_window\": $pings, \"want\": $((clients * window / hb)), \"missing\": $(lg_json 'd["pong_missing"]')"
  t2_check "${name}_held" "$(lg_true "$res" 'd["lost"] == {}')" "\"lost\": $(lg_json 'd["lost"]')"
  headroom=false
  usage_ok "$ramp" && usage_ok "$win" && headroom=true
  t2_check "${name}_loadgen_headroom" "$headroom" \
    "\"loadgen_cpus\": $LG_CPUS, \"ramp_cores\": $(echo "$ramp" | lg_py - 'd["loadgen"]["cores"]'), \"window_cores\": $(echo "$win" | lg_py - 'd["loadgen"]["cores"]')"
  t=$SECONDS
  idle=$(t2_bool stack_wait_idle 300 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  done_logouts=$(stack_metric gw-0 goim_gateway_logouts_total 'result="done"')
  dropped=$(stack_metric gw-0 goim_gateway_logouts_total 'result="dropped"')
  [ "$done_logouts" = "$clients" ] || idle=false
  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"done_logouts\": $done_logouts, \"want\": $clients, \"dropped_logouts\": $dropped, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

  t2_field "$name" "{\"clients\": $clients, \"heartbeat_s\": $hb, \"window_s\": $window, \"gc\": $(t2_json_str "$gc"), \"gateway_env\": $(t2_json_str "${STACK_GATEWAY_ENV[*]}"), \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"window\": $win, \"gateway\": $gw, \"live_heap_mb\": $live_mb, \"live_bytes_per_conn\": $per_conn, \"pings_in_window\": $pings, \"gateway_us_per_pong\": $per, \"reconcile_entries\": $reconcile, \"stop_s\": $stop_s, \"login_latency\": $(lg_json 'd["login_latency"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"failed\": $(lg_json 'd["failed"]'), \"sources\": $(lg_json 'd["sources"]')}"
  summary+=("$(t2_json_str "$name"): {\"clients\": $clients, \"heartbeat_s\": $hb, \"gc\": $(t2_json_str "$gc"), \"gateway_cores\": $(echo "$win" | lg_py - 'd["gw-0"]["cores"]'), \"gateway_rss_mb\": $(echo "$win" | lg_py - 'd["gw-0"]["rss_mb"]'), \"live_heap_mb\": $live_mb, \"live_bytes_per_conn\": $per_conn, \"gateway_us_per_pong\": $per, \"gc_mark_cpu_pct\": $(echo "$gw" | lg_py - 'd["gc_mark_cpu_pct"]'), \"ramp_gateway_cores\": $(echo "$ramp" | lg_py - 'd["gw-0"]["cores"]'), \"loadgen_rss_mb\": $(echo "$win" | lg_py - 'd["loadgen"]["rss_mb"]')}")
  t2_duration "$name" $((SECONDS - t0))
done
t2_field summary "{$(t2_join "${summary[@]}")}"

# Artifacts of every phase: the gateway's and Logic's metrics, heap and CPU
# profiles, loadgen's CPU profile and result.
missing=""
for phase in $PHASES; do
  name=${phase%%:*} a=$T2_OUT/${phase%%:*}/artifacts
  for f in logic gw-0; do
    grep -q '^# TYPE' "$a/window/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
    gzip -t "$a/window/$f.heap.pb.gz" 2>/dev/null || missing+=" $name/$f.heap"
    gzip -t "$a/window/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/$f.cpu"
  done
  gzip -t "$a/window/gw-0.heap-live.pb.gz" 2>/dev/null || missing+=" $name/gw-0.heap-live"
  gzip -t "$a/window/loadgen.cpu.pb.gz" 2>/dev/null || missing+=" $name/loadgen.cpu"
  [ "$(lg_true "$a/loadgen-$name.json" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "(a) and (c) with SDK clients (light clients only; SDK sizing in Plan 6b2)" \
  "a gateway CPU set other than 6 Zen 5c cores (12 CPUs)"
t2_finish
