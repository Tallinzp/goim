#!/usr/bin/env bash
# Tier 2: failover (Plan 6c2b), the spec's Redis failover test (R2-O5,
# R5S-O1, R5S-O9): fail over the shard that holds a lease key and measure
# the time; the lease must live through it at the configured TTL; logins
# for uids on the other shards keep their normal p99; then a whole zone at
# once (with 3 masters: one master and the replica in its zone), with the
# failover time and the share of logins affected recorded. On a Redis
# Cluster (3 masters, 3 replicas, zones z0-z2 as labels, stack.sh
# STACK_REDIS=cluster, cluster-node-timeout 5s: #140). One stack for every
# phase (#141.4): one start, one ramp of 600k light clients (run 33's
# scale, #141.1), online through every phase; each phase starts from the
# start layout (stack_redis_restore, measured). The phases:
#   manual: CLUSTER FAILOVER on the replica of gw-0's lease key's master
#       (no write is lost; the replica waits for its master's offset).
#   kill: SIGKILL of that master (#139.3: ~9s after gw-0's last renewal,
#       the worst time: the key then has the least TTL left), then the
#       node starts again with its nodes.conf and rejoins as a replica.
#   zone: SIGSTOP of that master's zone, the master and the other master's
#       replica in it (#139.2: a zone cut off by the network, at the same
#       time), then SIGCONT: the old master rejoins as a replica by itself.
# Each phase has three login windows (#141.1), each a new light loadgen
# instance (P6c2aI-5) of FAILOVER_FLOW_RATE logins/s: before (from the
# start layout), through (the failover lands in it) and after (the Cluster
# whole again). GET /logins gives each client's sequence (first attempt's
# start, its login, its attempts); each uid's shard is its sess key's slot
# owner before the failover (CRC16 of the uid, the hash tag).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=failover
#   Results: tier2-results/failover-<timestamp>/ (result.json, the stack's
#   logs and artifacts, one directory of login logs per phase), plus ONE
#   file to send back: tier2-results/failover-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 20 minutes (the ramp 120s; each phase ~4 min: the restore, three
#   windows, the rejoin with a full resync of the returning node).
#
# Settings (every number whole and positive, refused otherwise before
# anything starts):
#   FAILOVER_PHASES       phases to run, in order (default "manual kill zone")
#   FAILOVER_CLIENTS      light clients online over the 3 gateways (default 600000)
#   FAILOVER_RATE         their ramp, clients per second (default 5000)
#   FAILOVER_FLOW_RATE    each window's logins per second (default 200)
#   FAILOVER_FLOW_ATTEMPTS each flow client's login attempts before it gives
#                         up (loadgen -light-attempts; default 30: a killed
#                         master refuses each attempt at once, and 10 attempts
#                         0.5-1.5s apart span ~9s against a ~8.2s failover,
#                         run 34's slowest target-shard login 9.8s)
#   FAILOVER_WINDOW_S     the before and after windows' length (default 30)
#   FAILOVER_THROUGH_S    the through window's length (default 40: the
#                         wait for the worst time, up to 10s, the failover
#                         and the logins after it)
#   FAILOVER_AT_S         seconds after gw-0's last renewal for kill and zone
#                         (default 9, #139.3; manual too, harmless)
#   FAILOVER_MAX_S        the failover's bound (default 20: lease TTL 30s minus
#                         the 10s renewal interval, the spec's lease rule)
#   FAILOVER_WHOLE_WAIT   seconds for the restore and the rejoin (default 600:
#                         a returning node's full resync of ~200k sessions)
#   FAILOVER_P99_FLOOR_MS the other shards' bound's floor over the before
#                         p99 (default 50, #141.2)
#   FAILOVER_LG_MEMLIMIT  loadgen's GOMEMLIMIT (default 8GiB)
#   FAILOVER_CPUS_LOADGEN, FAILOVER_CPUS_GATEWAY, FAILOVER_CPUS_LOGIC
#                         the CPU sets (default reconnect's, run 33's:
#                         0-5,10,12-17,22; 6-9,18-21; 11,23: Logic and the
#                         6 Redis nodes share 11,23, #138)
#   FAILOVER_GATEWAY_PROCS  each gateway's GOMAXPROCS (default 4)
#
# Checks (once):
#   stack          redis (6 nodes), devtoken, Logic and the 3 gateways Ready
#   online         every main client logs in, each gateway holds >= 30% of them
#   pinning        the gateways, Logic + Redis and loadgen on their CPU sets;
#                  a failure ends the run
# Checks (per phase P):
#   P_restore      the start layout back within FAILOVER_WHOLE_WAIT (the
#                  time recorded; the first phase's is a no-op)
#   P_failover     the target master's replica is master within
#                  FAILOVER_MAX_S of the trigger, and a live node then says
#                  cluster_state:ok; both times, the trigger's time since
#                  gw-0's last renewal, the zone's nodes, gwepochs' master
#                  (P6c2aI-2) and each gateway's lease key's master recorded
#   P_other_shards the through window's logins on the other shards, those
#                  started from the trigger to the replica's master + 1s: none
#                  gave up, and their p99 is at most max(2 x, + FLOOR ms)
#                  the before window's other-shard p99 (#141.2); the
#                  target shard's affected share (retried or gave up) and
#                  every window's per-shard numbers recorded
#   P_logins       every flow client of the three windows logged in in
#                  the end (light: up to 10 attempts; the failing shard's
#                  retries are recorded in P_other_shards)
#   P_whole        the Cluster whole again within FAILOVER_WHOLE_WAIT after
#                  the node returns (manual: at once); the time recorded
#   P_lease        from the through window's start to the end of the
#                  rejoin, no gateway got a missing lease reply and Logic
#                  deleted nothing in a sweep (the lease lived, R2-O5);
#                  each gateway renewed at least every interval (live
#                  floor); the error replies (a retry every 1s while Redis
#                  refuses, P6c2aI-3) and Logic's renewal errors recorded,
#                  not gated
#   P_sessions     after the rejoin, every main client's session is in
#                  the store, on its gateway (written minutes earlier: no
#                  failover may lose it); the through window's lost writes
#                  (its sessions missing then: ~1s of writes may be lost
#                  in kill and zone, R2-O4, repaired by Reconcile in a
#                  10-minute cycle) recorded
#   no_swap_in_P   no page swapped in from the through window's start to
#                  the end of P_sessions
#                  (P_* per phase: 8 checks each)
# Checks (once, at the end):
#   idle           after loadgen stops, every gateway reaches 0 connections
#   processes      nothing died, panicked or stopped uncleanly (the killed
#                  Redis node is expected)
#   artifacts      the metrics before and after, each window's login log
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init failover
t2_need curl taskset sudo python3
t2_require_host_setup

PHASES=${FAILOVER_PHASES:-manual kill zone}
named=" "
for p in $PHASES; do
  case $p in manual | kill | zone) ;; *) echo "failover: unknown phase '$p' (manual, kill, zone)" >&2; exit 1 ;; esac
  case $named in *" $p "*) echo "failover: phase '$p' named twice in FAILOVER_PHASES" >&2; exit 1 ;; esac
  named+="$p "
done
for v in FAILOVER_CLIENTS FAILOVER_RATE FAILOVER_FLOW_RATE FAILOVER_FLOW_ATTEMPTS FAILOVER_WINDOW_S FAILOVER_THROUGH_S FAILOVER_AT_S \
  FAILOVER_MAX_S FAILOVER_WHOLE_WAIT FAILOVER_P99_FLOOR_MS FAILOVER_GATEWAY_PROCS; do
  case ${!v-1} in ''|*[!0-9]*|0*) echo "failover: $v must be a positive whole number" >&2; exit 1 ;; esac
done
N=${FAILOVER_CLIENTS:-600000} RATE=${FAILOVER_RATE:-5000} FLOW=${FAILOVER_FLOW_RATE:-200} FLOW_ATTEMPTS=${FAILOVER_FLOW_ATTEMPTS:-30}
WIN=${FAILOVER_WINDOW_S:-30} THROUGH=${FAILOVER_THROUGH_S:-40} AT=${FAILOVER_AT_S:-9} MAX=${FAILOVER_MAX_S:-20}
WHOLE=${FAILOVER_WHOLE_WAIT:-600} FLOOR=${FAILOVER_P99_FLOOR_MS:-50}
[ "$AT" -lt 10 ] || { echo "failover: FAILOVER_AT_S must be below the 10s renewal interval" >&2; exit 1; }
# 600k light clients as capacity's (~16GB with the gateways and Redis).
t2_mem_budget 16
t2_nofile 1048576

export STACK_PIN=1 STACK_GATEWAYS=3 STACK_REDIS=cluster
export T2_CPUS_GATEWAY=${FAILOVER_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${FAILOVER_CPUS_LOADGEN:-0-5,10,12-17,22}
export T2_CPUS_LOGIC=${FAILOVER_CPUS_LOGIC:-11,23}
export STACK_GATEWAY_PROCS=${FAILOVER_GATEWAY_PROCS:-4}
LG_ENV=(GOMEMLIMIT="${FAILOVER_LG_MEMLIMIT:-8GiB}")
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop_all' # newest first: loadgen stops before the stack

GW_COUNTERS=(
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  logins_busy='goim_gateway_logins_total:result="busy"'
  logins_expired='goim_gateway_logins_total:result="expired"'
)
LOGIC_COUNTERS=(
  lease_error='goim_logic_lease_renewals_total:result="error"'
  lease_missing='goim_logic_lease_renewals_total:result="missing"'
  lease_index_failures=goim_logic_lease_index_failures_total
  sweep_deleted=goim_logic_sweep_deleted_total
  logins_ok='goim_logic_logins_total:result="ok"'
)

t2_report_extra() {
  local f
  for f in "$T2_OUT"/stack/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# fo_zone LAYOUT MASTER: the nodes in MASTER's zone (redis/layout's "NAME
# PORT ROLE ZONE MASTER" lines), MASTER first, space-separated.
fo_zone() {
  awk -v m="$2" '$1 == m { z = $4 } { n[NR] = $1; zz[NR] = $4 } END { printf "%s", m; for (i = 1; i <= NR; i++) if (zz[i] == z && n[i] != m) printf " %s", n[i]; print "" }' "$1"
}

# fo_replica LAYOUT MASTER: MASTER's replica in the start layout.
fo_replica() {
  awk -v m="$2" '$3 == "replica" && $5 == m { print $1 }' "$1"
}

# fo_shards LOGINS SLOTS TARGET [T0_MS T1_MS]: a window's login log (GET
# /logins) per shard, each uid's shard its sess key's slot owner in SLOTS
# (stack_redis_slots's "START END NAME" lines): {"shards": {NAME: STATS},
# "others": STATS over every shard but TARGET, "target": TARGET, "window"}.
# With T0_MS and T1_MS only the sequences whose first attempt started in
# [T0_MS, T1_MS] count. STATS: {"clients", "logged_in", "gave_up",
# "retried" (logged in after more than one attempt), "pending" (no
# sequence ended; never in a window), "p50_ms", "p99_ms", "max_ms" (the
# logged-in sequences' first start to login, nearest rank; null with
# none)}. null if a file is unreadable or a slot has no owner.
fo_shards() {
  python3 -c 'import json, math, sys
d = json.load(open(sys.argv[1]))
ranges = [(int(a), int(b), n) for a, b, n in (l.split() for l in open(sys.argv[2]) if l.strip())]
target = sys.argv[3]
win = (int(sys.argv[4]), int(sys.argv[5])) if len(sys.argv) > 5 else None
def crc16(b):  # CRC16-XMODEM, Redis Cluster key slots
    r = 0
    for x in b:
        r ^= x << 8
        for _ in range(8):
            r = ((r << 1) ^ 0x1021 if r & 0x8000 else r << 1) & 0xFFFF
    return r
def owner(slot):
    n = next((n for a, b, n in ranges if a <= slot <= b), None)
    if n is None:
        raise SystemExit(1)
    return n
def new():
    return {"clients": 0, "logged_in": 0, "gave_up": 0, "retried": 0, "pending": 0, "ms": []}
groups = {}
for i, (f, e, a) in enumerate(zip(d["first_ms"], d["end_ms"], d["attempts"])):
    if win and not (a > 0 and win[0] <= f <= win[1]):
        continue
    name = owner(crc16(("%s-%d" % (d["uid_prefix"], i)).encode()) % 16384)
    for g in (groups.setdefault(name, new()), groups.setdefault("\0others", new()) if name != target else None):
        if g is None:
            continue
        g["clients"] += 1
        if a == 0:
            g["pending"] += 1
        elif e == 0:
            g["gave_up"] += 1
        else:
            g["logged_in"] += 1
            g["retried"] += a > 1
            g["ms"].append(e - f)
def stats(g):
    ms = sorted(g.pop("ms"))
    q = lambda p: ms[max(0, math.ceil(p * len(ms)) - 1)] if ms else None
    g.update(p50_ms=q(0.5), p99_ms=q(0.99), max_ms=ms[-1] if ms else None)
    return g
others = stats(groups.pop("\0others", new()))
print(json.dumps({"shards": {n: stats(g) for n, g in sorted(groups.items())}, "others": others, "target": target,
                  "window": list(win) if win else None}))' "$@" 2>/dev/null || echo null
}

# fo_other_ok BEFORE THROUGH FLOOR_MS: P_other_shards' rule over two
# fo_shards readings: the through window's other shards have logins, none
# gave up or is pending, and their p99 is at most max(2 x BEFORE's, BEFORE's
# + FLOOR_MS) (#141.2). Prints true or false.
fo_other_ok() {
  echo "{\"b\": ${1:-null}, \"t\": ${2:-null}}" | lg_true - 'd["b"]["others"]["p99_ms"] is not None and d["t"]["others"]["clients"] > 0 and d["t"]["others"]["gave_up"] == d["t"]["others"]["pending"] == 0 and d["t"]["others"]["p99_ms"] <= max(2 * d["b"]["others"]["p99_ms"], d["b"]["others"]["p99_ms"] + '"$3"')'
}

# fo_affected SHARDS: the target shard's share of a window's sequences
# that retried or gave up (null with none).
fo_affected() {
  echo "${1:-null}" | lg_py - '(lambda g: round((g["retried"] + g["gave_up"]) / g["clients"], 4) if g and g["clients"] else None)(d["shards"].get(d["target"]))'
}

# fo_flow NAME CLIENTS: a window's light instance (flow, its own uid
# prefix and source net) started; FLOW logins per second.
fo_flow() {
  lg_in flow lg_start "$1" -mode light -clients "$2" -rate "$FLOW" -light-attempts "$FLOW_ATTEMPTS" -uid-prefix "$1" -source-net 1
}

# fo_flow_done SECONDS FILE: waits until every flow client's sequence
# ended (logged in or gave up), then writes GET /logins to FILE. Fails
# after SECONDS (FILE then has the log as it is).
fo_flow_done() {
  local end=$((SECONDS + $1)) rc=1
  while [ $SECONDS -lt "$end" ]; do
    [ "$(lg_in flow lg_get /logins 'all(a > 0 for a in d["attempts"])')" = true ] && { rc=0; break; }
    sleep 1
  done
  curl -sf -m 30 "http://$(lg_in flow lg_control)/logins" >"$2"
  return $rc
}

# fo_wait_at KEY OWNER AT_S: waits (up to 25s) until KEY's PTTL on OWNER
# says AT_S seconds passed since its last renewal (PTTL in (TTL - AT_S -
# 0.5s, TTL - AT_S], TTL 30s), polling every 20ms; prints the ms since the
# renewal at the return (null after the 25s).
fo_wait_at() {
  local hi=$((30000 - $3 * 1000)) end=$((SECONDS + 25)) p
  while [ $SECONDS -lt $end ]; do
    p=$(STACK_REDIS_TIMEOUT=1 stack_redis_node_cli "$2" pttl "$1" 2>/dev/null)
    case $p in '' | *[!0-9]*) ;; *) [ "$p" -le "$hi" ] && [ "$p" -gt $((hi - 500)) ] && { echo $((30000 - p)); return 0; } ;; esac
    sleep 0.02
  done
  echo null
}

# fo_wait_ok SECONDS: until a live node says cluster_state:ok; fails after SECONDS.
fo_wait_ok() {
  local end=$((SECONDS + $1))
  until STACK_REDIS_TIMEOUT=1 stack_redis_cli cluster info 2>/dev/null | grep -q '^cluster_state:ok'; do
    [ $SECONDS -lt "$end" ] || return 1
    sleep 0.05
  done
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))
missing=""

# One stack (#141.4): its checks carry no phase name.
t0=$SECONDS
export STACK_DIR=$T2_OUT/stack
mkdir -p "$STACK_DIR"
ln -sfn ../bin "$STACK_DIR/bin"
busy=$(stack_busy_ports)
[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
stack_up 2>"$STACK_DIR/stack-up.err"
rc=$?
t2_check stack "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"redis\": \"cluster\", \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
[ $rc -eq 0 ] || { stack_down; t2_duration stack $((SECONDS - t0)); t2_finish; }
lg_start main -mode light -clients "$N" -rate "$RATE" -uid-prefix main
t=$SECONDS
online=false
lg_wait_online "$N" $((N / RATE * 3 + 60)) &&
  online=$(lg_get /stats 'd["online"] == '"$N"' and min(d["online_by_gateway"].values()) >= '"$N"' * 0.3')
[ "$online" = true ] || online=false
ramp_s=$((SECONDS - t))
t2_check online "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
wrong=""
for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
have=$(taskset -pc "$LG_PID" 2>/dev/null)
[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
[ -z "$wrong" ] && [ "$online" = true ] || { lg_stop_all; stack_down; t2_duration stack $((SECONDS - t0)); t2_finish; }
stack_snapshot before
t2_duration ramp $((SECONDS - t0))
layout=$STACK_DIR/redis/layout

for name in $PHASES; do
  t0=$SECONDS
  d=$STACK_DIR/$name
  mkdir -p "$d"
  # The start layout back (the first phase: already in it).
  t=$SECONDS
  restore=$(t2_bool stack_redis_restore "$WHOLE" 2>"$d/restore.err")
  restore_s=$((SECONDS - t))
  t2_check "${name}_restore" "$restore" "\"restore_s\": $restore_s, \"roles\": $(stack_redis_roles), \"error\": $(t2_json_str "$(cat "$d/restore.err")")"
  key=$(stack_lease_key gw-0) target=$(stack_redis_owner "$key")
  replica=$(fo_replica "$layout" "$target") zone=$(fo_zone "$layout" "$target") epochs=$(stack_redis_owner gwepochs)
  leases=""
  for g in gw-0 gw-1 gw-2; do leases+="${leases:+, }\"$g\": \"$(stack_redis_owner "$(stack_lease_key "$g")")\""; done
  stack_redis_slots >"$d/slots.txt"

  # Before: the normal p99.
  fo_flow "$name-before" $((FLOW * WIN))
  before_done=$(t2_bool fo_flow_done $((WIN + 120)) "$d/before.json")
  lg_in flow lg_stop
  before=$(fo_shards "$d/before.json" "$d/slots.txt" "$target")

  # Through: the failover lands in it, at the worst time after gw-0's
  # last renewal (#139.3).
  t2_record_start "$name"
  g0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  redis_reset=true; stack_redis_resetstat || redis_reset=false
  lease_t=$SECONDS
  fo_flow "$name-through" $((FLOW * THROUGH))
  at_ms=$(fo_wait_at "$key" "$target" "$AT")
  trig_ms=$(date +%s%3N)
  case $name in
    manual) stack_redis_failover "$replica" ;;
    kill) stack_redis_kill "$target" ;;
    zone) for n in $zone; do stack_redis_stop "$n"; done ;;
  esac
  master=false master_ms=null ok=false ok_ms=null
  stack_redis_wait_role "$replica" master "$MAX" && { master=true; master_ms=$(($(date +%s%3N) - trig_ms)); }
  fo_wait_ok 60 && { ok=true; ok_ms=$(($(date +%s%3N) - trig_ms)); }
  t2_check "${name}_failover" "$([ "$master" = true ] && [ "$ok" = true ] && echo true || echo false)" \
    "\"target\": \"$target\", \"replica\": \"$replica\", \"lease_key\": \"$key\", \"at_ms_since_renewal\": $at_ms, \"master_ms\": $master_ms, \"cluster_ok_ms\": $ok_ms, \"max_s\": $MAX, \"stopped\": $(t2_json_str "$([ "$name" = zone ] && echo "$zone")"), \"gwepochs_on\": \"$epochs\", \"lease_keys\": {$leases}, \"roles\": $(stack_redis_roles)"
  through_done=$(t2_bool fo_flow_done $((THROUGH + 120)) "$d/through.json")
  t1=$((trig_ms + ${master_ms/null/$((MAX * 1000))} + 1000))
  through=$(fo_shards "$d/through.json" "$d/slots.txt" "$target")
  failwin=$(fo_shards "$d/through.json" "$d/slots.txt" "$target" "$trig_ms" "$t1")

  # The node back; the Cluster whole again (a full resync).
  case $name in
    kill) stack_redis_restart "$target" ;;
    zone) for n in $zone; do stack_redis_cont "$n"; done ;;
  esac
  t=$SECONDS
  whole=$(t2_bool stack_redis_wait_whole "$WHOLE" 2>"$d/whole.err")
  whole_s=$((SECONDS - t))
  t2_check "${name}_whole" "$whole" "\"whole_s\": $whole_s, \"roles\": $(stack_redis_roles), \"error\": $(t2_json_str "$(cat "$d/whole.err")")"
  redis=$(stack_redis_latency)
  g1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  gd=$(t2_gws_delta "$g0" "$g1") ld=$(stack_counters_delta "$l0" "$l1")
  window_s=$((SECONDS - lease_t))
  floor=$(t2_lease_floor "$window_s")
  t2_check "${name}_lease" "$(echo "{\"g\": $gd, \"l\": $ld}" | lg_true - 'all(v["lease_missing"] == 0 and v["lease_live"] >= '"$floor"' for v in d["g"].values()) and d["l"]["lease_missing"] == 0 and d["l"]["sweep_deleted"] == 0')" \
    "\"window_s\": $window_s, \"live_floor\": $floor, \"gateways\": $(echo "$gd" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("lease_error", "lease_missing", "lease_index_failures", "sweep_deleted")}')"
  sessions=$(t2_sessions 0)
  through_sessions=$(lg_in flow t2_sessions 0)
  t2_check "${name}_sessions" "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
    "\"main\": ${sessions:-null}, \"through_lost\": $(echo "${through_sessions:-null}" | lg_py - '{k: d[k] for k in ("clients", "offline", "found", "missing", "wrong_gateway", "examples")}')"
  lg_in flow lg_stop
  t2_record_end "$name"

  # After: the Cluster whole again.
  fo_flow "$name-after" $((FLOW * WIN))
  after_done=$(t2_bool fo_flow_done $((WIN + 120)) "$d/after.json")
  lg_in flow lg_stop
  after=$(fo_shards "$d/after.json" "$d/slots.txt" "$target")

  t2_check "${name}_other_shards" "$(fo_other_ok "$before" "$failwin" "$FLOOR")" \
    "\"before\": $(echo "${before:-null}" | lg_py - 'd["others"]'), \"failover_window\": ${failwin:-null}, \"floor_ms\": $FLOOR, \"target_affected_share\": $(fo_affected "$failwin")"
  t2_check "${name}_logins" "$(echo "{\"b\": ${before:-null}, \"t\": ${through:-null}, \"a\": ${after:-null}}" | lg_true - 'all(sum(g["logged_in"] for g in d[k]["shards"].values()) == '"$FLOW"' * n for k, n in (("b", '"$WIN"'), ("t", '"$THROUGH"'), ("a", '"$WIN"')))')" \
    "\"ended\": {\"before\": $before_done, \"through\": $through_done, \"after\": $after_done}, \"gave_up\": $(echo "{\"b\": ${before:-null}, \"t\": ${through:-null}, \"a\": ${after:-null}}" | lg_py - '{k: {n: g["gave_up"] for n, g in v["shards"].items()} for k, v in d.items()}')"
  t2_field "$name" "{\"target\": \"$target\", \"replica\": \"$replica\", \"zone\": $(t2_json_str "$zone"), \"gwepochs_on\": \"$epochs\", \"restore_s\": $restore_s, \"at_ms_since_renewal\": $at_ms, \"master_ms\": $master_ms, \"cluster_ok_ms\": $ok_ms, \"whole_s\": $whole_s, \"before\": ${before:-null}, \"through\": ${through:-null}, \"failover_window\": ${failwin:-null}, \"after\": ${after:-null}, \"gateways\": $gd, \"logic\": $ld, \"redis_latency\": ${redis:-null}, \"redis_reset\": $redis_reset, \"sessions\": ${sessions:-null}, \"through_sessions\": ${through_sessions:-null}}"
  t2_duration "$name" $((SECONDS - t0))
  for f in before through after; do [ "$(lg_true "$d/$f.json" '"attempts" in d')" = true ] || missing+=" $name/$f.json"; done
done

stack_snapshot after
lg_stop_all
t=$SECONDS
idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
t2_check idle "$idle" "\"stop_s\": $((SECONDS - t)), \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
stack_down
problems=$(stack_health)
t2_check processes "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
a=$STACK_DIR/artifacts
for f in before/logic after/logic before/gw-0 after/gw-0; do
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
t2_field run "{\"clients\": $N, \"rate\": $RATE, \"flow_rate\": $FLOW, \"flow_attempts\": $FLOW_ATTEMPTS, \"window_s\": $WIN, \"through_s\": $THROUGH, \"at_s\": $AT, \"ramp_s\": $ramp_s, \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"node_timeout_ms\": ${REDIS_CLUSTER_NODE_TIMEOUT_MS:-5000}}"

t2_not_covered "Revoke at zone loss (#141.3): no Tier 2 tool calls Revoke; the store tests cover a WAIT the replica does not acknowledge (TestRevokeFailsWhenReplicaDoesNotAck)" \
  "the repair of lost writes: Reconcile's 10-minute cycle is not waited for; P_sessions records the through window's lost sessions, and the race suite and store tests cover rules 3 and 4" \
  "a zone with two masters (6 masters): Tier 2 runs 3 masters (#137); Tier 3 (Plan 6d)" \
  "a failover under a login storm: the flow is FAILOVER_FLOW_RATE logins/s beside 600k idle clients (#141.1)"
t2_finish
