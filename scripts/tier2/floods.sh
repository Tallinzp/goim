#!/usr/bin/env bash
# Tier 2: floods (Plan 6b3b), the spec's load tests "handshake flood" (two
# tests, #121) and "slow-client flood", each phase on a fresh stack:
#   storm: a legitimate 20k/s storm on one gateway, no flood: 200k light
#       clients ramped at 20,000/s against the gateway's default handshake
#       limit (25,000/s bucket, burst 2,500, half the loops computing): the
#       bucket shapes, it does not reject (#121 (2)). One gateway, since the
#       bucket is per gateway and the spec's 20k/s is a storm's peak.
#   garbage: 2 x 200k light clients (the operating point per gateway) Ping
#       every 30s, then loadgen instance flood sends garbage handshakes (a
#       valid header and key_id, a random Noise message: each costs the
#       gateway a DH) at 60,000/s for 90s from 253 sources, above both
#       gateways' buckets (2 x 25,000/s), so the limiter refuses the excess:
#       every Ping is still answered on time and the gateways renew their
#       leases (#121 (1)).
#   slow: instances flood and flood2 hold 80k slow-handshake and 40k
#       slow-auth connections (sends nothing; handshakes, then sends
#       nothing), freed by the gateways' 5s timeouts and redialed, while
#       400k light clients log in beside them and Ping: no Busy, no reject,
#       no limiter refusal (slow-auth's ~7k handshakes/s fit the buckets,
#       6b3a Review Focus 5).
# Logic is one process (minReplicas, no HPA), unthrottled.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods
#   Results: tier2-results/floods-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/floods-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 12 minutes.
#
# Settings (every number whole and positive, refused otherwise before
# anything starts):
#   FLOODS_PHASES          phases to run, in order (default "storm garbage slow")
#   FLOODS_STORM_CLIENTS   storm: light clients (default 200000: 10s at the rate)
#   FLOODS_STORM_RATE      storm: clients started per second (default 20000)
#   FLOODS_CLIENTS         garbage, slow: light clients over both gateways (default 400000)
#   FLOODS_RATE            garbage, slow: their clients started per second (default 10000)
#   FLOODS_HEARTBEAT       garbage, slow: the gateways' -heartbeat and the light
#                          clients' Ping interval, seconds (default 30, the
#                          lowest the gateway takes: most Pings per flood)
#   FLOODS_GARBAGE_RATE    garbage attempts per second (default 60000)
#   FLOODS_GARBAGE_INFLIGHT  garbage attempts in flight at most (default 20000)
#   FLOODS_GARBAGE_SECONDS the garbage flood's length (default 90: 3 Pings each;
#                          at least 45, so the profile from 10s to 40s ends inside it)
#   FLOODS_GARBAGE_BUCKET  garbage: each gateway's -handshake-rate (default: the
#                          gateway's 25,000/s; a dry run lowers it so its small
#                          flood reaches the limiter)
#   FLOODS_SLOW_HANDSHAKE  slow: slow-handshake connections (default 80000)
#   FLOODS_SLOW_AUTH       slow: slow-auth connections (default 40000)
#   FLOODS_SLOW_RATE       slow: each flood's connections started per second (default 10000)
#   FLOODS_SLOW_HOLD       slow: seconds held once the light clients are online (default 60)
#
# Checks (per phase run):
#   storm_stack           redis, devtoken, Logic and the gateway become Ready
#   storm_online          every client logs in, none gives up
#   storm_rate            the gateway completed handshakes at >= 0.9 x the
#                         rate, between 5% and 95% of the clients (so the
#                         bucket saw the storm, not a loadgen-bound trickle);
#                         its per-second series is recorded
#   storm_bucket          the bucket refused nothing (no_token 0, #121);
#                         the concurrency limit's refusals (no_slot, each a
#                         pre-handshake close the client retries after
#                         0.5-1.5s), their share of the handshakes and
#                         Busy are recorded
#   no_swap_in_storm      no page swapped in from the ramp to the last login
#   garbage_stack, slow_stack   as storm_stack, two gateways
#   garbage_online        every light client logs in, each gateway holds 45-55%
#   garbage_delivered     loadgen kept the schedule (attempts >= 0.95 x rate x
#                         seconds) and the gateways saw it: failed + no_slot +
#                         no_token >= 0.95 x rate x seconds (6b3a Review
#                         Focus 2); outcomes, TIME_WAIT and kernel TCP
#                         counters are recorded (Review Focus 3)
#   garbage_limited       each gateway's limiter refused part of the flood
#                         (no_slot + no_token > 0): it ran past the bucket
#   garbage_heartbeats    every Ping answered (no Pong missing, every client
#                         Pinged at least once), the slowest Pong under 10s
#                         (the SDK's Pong timeout), no connection lost, and no
#                         gateway closed one for its heartbeat
#   garbage_lease         each gateway renewed its lease every interval
#                         through the flood, and Logic saw no renewal error
#   no_swap_in_garbage    no page swapped in over the flood
#   slow_flood            both floods held >= 85% of their connections, each
#                         slot freed at least once, before the light clients start
#   slow_held             every slow connection was closed by a gateway, held
#                         p50 >= 5s, p99 <= 6.5s, max <= 7.5s (5s + one tick of
#                         the wheel, plus scheduling slack, 6b3a O7)
#   slow_freed            the gateways counted at least as many
#                         handshake_timeout and auth_timeout closes
#   slow_legit            every light client logs in beside the floods: no
#                         Busy, no reject (login_queue, pending_mem,
#                         at_capacity) and no limiter refusal
#   slow_heartbeats, slow_lease   as garbage's, over the floods
#   no_swap_in_slow       no page swapped in from the floods' start to their stop
#   <phase>_idle          after loadgen stops, every gateway reaches 0
#                         connections and settles its Logouts
#   <phase>_processes     nothing died, panicked or stopped uncleanly
#   artifacts             metrics before and after each phase's window, the
#                         garbage flood's CPU profiles, loadgen's results
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init floods
t2_need curl taskset sudo python3 ss
t2_require_host_setup

PHASES=${FLOODS_PHASES:-storm garbage slow}
for p in $PHASES; do
  case $p in storm | garbage | slow) ;; *) echo "floods: unknown phase '$p' (storm, garbage, slow)" >&2; exit 1 ;; esac
done
for v in FLOODS_STORM_CLIENTS FLOODS_STORM_RATE FLOODS_CLIENTS FLOODS_RATE FLOODS_HEARTBEAT \
  FLOODS_GARBAGE_RATE FLOODS_GARBAGE_INFLIGHT FLOODS_GARBAGE_SECONDS FLOODS_GARBAGE_BUCKET \
  FLOODS_SLOW_HANDSHAKE FLOODS_SLOW_AUTH FLOODS_SLOW_RATE FLOODS_SLOW_HOLD; do
  case ${!v-1} in ''|*[!0-9]*|0) echo "floods: $v must be a positive whole number" >&2; exit 1 ;; esac
done
[ "${FLOODS_GARBAGE_SECONDS:-90}" -ge 45 ] || { echo "floods: FLOODS_GARBAGE_SECONDS must be at least 45 (the flood's profile)" >&2; exit 1; }
# Light clients cost loadgen ~10KB each and the gateway less; the garbage
# phase's 400k are the most at once (capacity's 600k on one gateway ran
# within 16).
t2_mem_budget 12
t2_nofile 1048576

export STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop_all' # newest first: every loadgen instance stops before the stack
smp=""
t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'

GW_COUNTERS=(
  hs_ok='goim_gateway_handshakes_total:result="ok"'
  hs_failed='goim_gateway_handshakes_total:result="failed"'
  hs_no_slot='goim_gateway_handshakes_total:result="no_slot"'
  hs_no_token='goim_gateway_handshakes_total:result="no_token"'
  rejects_handshake_rate='goim_gateway_admission_rejects_total:reason="handshake_rate"'
  rejects_handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"'
  rejects_auth_timeout='goim_gateway_admission_rejects_total:reason="auth_timeout"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  rejects_pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  logins_busy='goim_gateway_logins_total:result="busy"'
  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
)
LOGIC_LEASE_ERROR='goim_logic_lease_renewals_total:result="error"'
HB=${FLOODS_HEARTBEAT:-30}

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

# down NAME: every loadgen stopped, the gateways idle, the stack stopped;
# checks NAME_idle and NAME_processes.
down() {
  lg_stop_all
  local t=$SECONDS idle stop_s problems
  idle=$(t2_bool stack_wait_idle 900 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${1}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
}

# gws_counters: {"gw-0": stack_counters, ...} of every gateway; gws_delta A B
# the same of two readings.
gws_counters() {
  local g out=""
  for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
    out+="${out:+, }\"$g\": $(stack_counters "$g" "${GW_COUNTERS[@]}")"
  done
  echo "{$out}"
}
# (stack_counters_delta per gateway: a key null in A counts as 0).
gws_delta() {
  python3 -c 'import json, sys
a, b = (json.loads(x) for x in sys.argv[1:3])
def delta(x, y):
    if x is None or y is None:
        return None
    return {k: None if v is None else round(v - (x.get(k) or 0), 6) for k, v in y.items()}
print(json.dumps({g: delta(a.get(g), y) for g, y in b.items()}))' "$1" "$2" 2>/dev/null || echo null
}

# hs_sampler NAME FILE: appends "unix_ms ok" every second, NAME's completed
# handshakes, until killed.
hs_sampler() {
  local n
  while :; do
    n=$(stack_metric "$1" goim_gateway_handshakes_total 'result="ok"') &&
      echo "$(date +%s%3N) $n" >>"$2"
    sleep 1
  done
}
# hs_rate FILE N: {"rate_per_s" (handshakes per second between the samples
# where 5% and 95% of N had completed: the ramp's body, without the retries'
# tail), "ramp_s", "peak_per_s", "series": [[s since the first sample,
# handshakes/s], ...] (first 120)}; null when the samples never reach 95% of N.
hs_rate() {
  python3 -c 'import json, sys
rows = [tuple(int(x) for x in l.split()) for l in open(sys.argv[1]) if len(l.split()) == 2 and l.split()[1].isdigit()]
n = int(sys.argv[2])
lo = next((r for r in rows if r[1] >= 0.05 * n), None)
hi = next((r for r in rows if r[1] >= 0.95 * n), None)
if not lo or not hi or hi[0] <= lo[0]:
    print("null"); sys.exit()
pts = [((t1 - rows[0][0]) / 1000, (c1 - c0) * 1000 / (t1 - t0)) for (t0, c0), (t1, c1) in zip(rows, rows[1:]) if t1 > t0]
print(json.dumps({"rate_per_s": round((hi[1] - lo[1]) * 1000 / (hi[0] - lo[0])), "ramp_s": round((hi[0] - lo[0]) / 1000, 1),
       "peak_per_s": round(max(p[1] for p in pts)), "series": [[round(t, 1), round(v)] for t, v in pts[:120]]}))' "$1" "$2" 2>/dev/null || echo null
}

# timewait: the host's TIME_WAIT sockets now (ss -s).
timewait() { ss -s 2>/dev/null | sed -n 's/^TCP:.*timewait \([0-9]*\).*/\1/p' | head -n 1 | grep . || echo null; }

# lease PHASE DELTA SECONDS LOGIC_DELTA: checks PHASE_lease over the gateways'
# counter delta of a window of SECONDS.
lease() {
  local floor
  floor=$(t2_lease_floor "$3")
  t2_check "${1}_lease" "$(echo "{\"g\": $2, \"l\": $4}" | lg_true - 'all(v["lease_error"] == 0 and v["lease_missing"] == 0 and v["lease_live"] >= '"$floor"' for v in d["g"].values()) and d["l"]["lease_error"] == 0')" \
    "\"window_s\": $3, \"live_floor\": $floor, \"gateways\": $(echo "$2" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic_lease_error\": $(echo "$4" | lg_py - 'd["lease_error"]')"
}

# heartbeats PHASE N DELTA: checks PHASE_heartbeats over the light result of
# N clients (instance main, stopped) and the gateways' counter delta.
heartbeats() {
  local r=$STACK_DIR/artifacts/loadgen-$LG_NAME.json
  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3}" | lg_true - 'd["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values())')" \
    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}')"
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))
missing=""

# storm: #121 (2), a legitimate 20k/s storm on one gateway's default limiter.
phase_storm() {
  local n=${FLOODS_STORM_CLIENTS:-200000} rate=${FLOODS_STORM_RATE:-20000} s0 s1 d usage online hs
  STACK_GATEWAYS=1 STACK_GATEWAY_ARGS=(-max-conns $((n + n / 10)))
  up storm || { stack_down; return; }
  stack_snapshot before
  s0=$(gws_counters)
  t2_record_start storm
  lg_start storm -mode light -clients "$n" -rate "$rate" -uid-prefix storm
  stack_usage_start loadgen="$LG_PID"
  hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" &
  smp=$!
  online=$(t2_bool lg_wait_online "$n" $((n / rate * 3 + 60)))
  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
  usage=$(stack_usage_json)
  s1=$(gws_counters)
  t2_record_end storm
  stack_snapshot after
  d=$(gws_delta "$s0" "$s1")
  hs=$(hs_rate "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$n")
  [ "$online" = true ] && online=$(lg_get /stats 'd["online"] == '"$n"' and "gave-up" not in d["failed"]')
  t2_check storm_online "${online:-false}" "\"want\": $n, \"online\": $(lg_get /stats 'd["online"]'), \"failed\": $(lg_get /stats 'd["failed"]'), \"login_latency\": $(lg_get /stats 'd["login_latency"]')"
  t2_check storm_rate "$(echo "$hs" | lg_true - 'd["rate_per_s"] >= 0.9 * '"$rate")" \
    "\"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
  t2_check storm_bucket "$(echo "$d" | lg_true - 'd["gw-0"]["hs_no_token"] == 0')" \
    "\"gw-0\": $(echo "$d" | lg_py - '{k: d["gw-0"][k] for k in ("hs_ok", "hs_no_token", "hs_no_slot", "rejects_handshake_rate", "logins_busy", "rejects_pending_mem", "rejects_at_capacity")}'), \"no_slot_share\": $(echo "$d" | lg_py - 'round(d["gw-0"]["hs_no_slot"] / (d["gw-0"]["hs_ok"] + d["gw-0"]["hs_no_slot"]), 4)'), \"failed\": $(lg_get /stats 'd["failed"]')"
  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
  down storm
}

# garbage: #121 (1), garbage handshakes beside a Pinging population.
phase_garbage() {
  local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} grate=${FLOODS_GARBAGE_RATE:-60000}
  local inflight=${FLOODS_GARBAGE_INFLIGHT:-20000} secs=${FLOODS_GARBAGE_SECONDS:-90}
  local online s0 s1 d l0 l1 tw0 tw1 usage want flood prof t0 lease_s f
  STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $((n / 2 + n / 10 + inflight)) -heartbeat "${HB}s"
    ${FLOODS_GARBAGE_BUCKET:+-handshake-rate "$FLOODS_GARBAGE_BUCKET"})
  up garbage || { stack_down; return; }
  lg_start garbage-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix garbage
  online=false
  lg_wait_online "$n" $((n / rate * 3 + 60)) &&
    online=$(lg_get /stats 'd["online"] == '"$n"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$n"' * 0.45')
  t2_check garbage_online "${online:-false}" "\"want\": $n, \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"failed\": $(lg_get /stats 'd["failed"]')"
  [ "$online" = true ] || { down garbage; return; }
  stack_snapshot before
  s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  t2_tcpstat_snap "$STACK_DIR/tcp0"
  tw0=$(timewait)
  t2_record_start garbage
  t0=$SECONDS
  lg_in flood lg_start garbage -mode garbage -rate "$grate" -clients "$inflight" -source-net 1
  stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}"
  # The gateways' and the flood's CPU over 30s of the flood, from 10s in.
  (sleep 10; stack_profile garbage 30 & lg_in flood lg_profile garbage 30 & wait) >/dev/null 2>&1 &
  prof=$!
  sleep "$secs"
  usage=$(stack_usage_json)
  s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  lease_s=$((SECONDS - t0))
  t2_tcpstat_snap "$STACK_DIR/tcp1"
  tw1=$(timewait)
  lg_in flood lg_stop
  wait "$prof"
  t2_record_end garbage
  stack_snapshot after
  lg_stop
  d=$(gws_delta "$s0" "$s1")
  flood=$(lg_py "$STACK_DIR/artifacts/loadgen-garbage.json" 'd["flood"]')
  want=$(python3 -c 'import sys; print(int(0.95 * int(sys.argv[1]) * int(sys.argv[2])))' "$grate" "$secs")
  t2_check garbage_delivered "$(echo "{\"g\": $d, \"f\": $flood}" | lg_true - 'd["f"]["attempts"] >= '"$want"' and sum(v["hs_failed"] + v["hs_no_slot"] + v["hs_no_token"] for v in d["g"].values()) >= '"$want")" \
    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1")"
  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_slot"] + v["hs_no_token"] > 0 for v in d.values())')" \
    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"]} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
  heartbeats garbage "$n" "$d"
  lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d, \"flood\": $flood}"
  down garbage
  for f in before/gw-0 after/gw-0 garbage/gw-0.cpu.pb.gz garbage/gw-1.cpu.pb.gz garbage/loadgen-flood.cpu.pb.gz; do
    case $f in
      *.gz) gzip -t "$STACK_DIR/artifacts/$f" 2>/dev/null || missing+=" garbage:$f" ;;
      *) grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" garbage:$f.metrics" ;;
    esac
  done
}

# slow: slow-handshake and slow-auth floods beside light clients logging in.
phase_slow() {
  local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} hs=${FLOODS_SLOW_HANDSHAKE:-80000}
  local au=${FLOODS_SLOW_AUTH:-40000} srate=${FLOODS_SLOW_RATE:-10000} hold=${FLOODS_SLOW_HOLD:-60}
  local s0 s1 d l0 l1 t0 lease_s flooding=true online usage inst held f
  STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
  up slow || { stack_down; return; }
  stack_snapshot before
  s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  t2_record_start slow
  t0=$SECONDS
  lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
  lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
  lg_in flood lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$hs"' and d["flood"]["open"] >= 0.85 * '"$hs" $((hs / srate + 60)) || flooding=false
  lg_in flood2 lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$au"' and d["flood"]["open"] >= 0.85 * '"$au" $((au / srate + 60)) || flooding=false
  t2_check slow_flood "$flooding" "\"slowhs\": $(lg_in flood lg_get /stats 'd["flood"]'), \"slowauth\": $(lg_in flood2 lg_get /stats 'd["flood"]')"
  [ "$flooding" = true ] || { t2_record_end slow; down slow; return; }
  lg_start slow-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix slow
  stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}" flood2="${LG_PIDS[flood2]}"
  online=false
  lg_wait_online "$n" $((n / rate * 3 + 60)) && online=$(lg_get /stats 'd["online"] == '"$n")
  sleep "$hold"
  usage=$(stack_usage_json)
  lg_in flood lg_stop
  lg_in flood2 lg_stop
  s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  lease_s=$((SECONDS - t0))
  t2_record_end slow
  stack_snapshot after
  lg_stop
  d=$(gws_delta "$s0" "$s1")
  held=true
  for inst in slowhs slowauth; do
    [ "$(lg_true "$STACK_DIR/artifacts/loadgen-$inst.json" 'set(d["flood"]["outcomes"]) == {"closed"} and d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500')" = true ] || held=false
  done
  t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}')"
  t2_check slow_freed "$(echo "{\"g\": $d, \"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')}" | lg_true - 'sum(v["rejects_handshake_timeout"] for v in d["g"].values()) >= d["h"] and sum(v["rejects_auth_timeout"] for v in d["g"].values()) >= d["a"]')" \
    "\"handshake_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_handshake_timeout"] for v in d.values())'), \"slowhs_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"auth_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_auth_timeout"] for v in d.values())'), \"slowauth_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')"
  t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}}" | lg_true - 'd["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token"))')" \
    "\"want\": $n, \"online\": ${online:-false}, \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
  heartbeats slow "$n" "$d"
  lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d}"
  down slow
  for f in before/gw-0 after/gw-0; do
    grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
  done
}

for p in $PHASES; do
  t0=$SECONDS
  "phase_$p"
  t2_duration "$p" $((SECONDS - t0))
  for f in "$T2_OUT/$p"/artifacts/loadgen-*.json; do
    [ -e "$f" ] || continue
    [ "$(lg_true "$f" '"mode" in d')" = true ] || missing+=" ${f#"$T2_OUT"/}"
  done
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "garbage from distinct hosts: 253 loopback sources of one loadgen (the limiter is global, never per IP)" \
  "the handshake flood across several gateways behind an LB (Tier 3, Plan 6d)" \
  "a storm's lease renewals on the other gateways: storm runs one gateway (gwloss b1 covers it)"
t2_finish
