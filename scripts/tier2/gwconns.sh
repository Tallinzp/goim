#!/usr/bin/env bash
# Tier 2: gwconns (Plan 6c3), the spec's gwconns shard load test (R4M-9)
# and the ReconcileEnd deadline (R5S-O8, Plan 2's deferred minor): a
# triggered Reconcile pass at the cap (250k connections on one gateway),
# started through the test fault API (gateway -faults, POST
# /fault/action/reconcile, #147.1), then its ReconcileEnd's delete phase.
# On a Redis Cluster (3 masters, 3 replicas, stack.sh STACK_REDIS=cluster);
# Logic and the 6 nodes share Logic's CPU set, as runs 33-35 (#148.4).
# One gateway (#148.1): its gwconns:gw-0:{EPOCH} set is on one master (the
# set and the lease key share the epoch's hash tag), the other two hold
# none. Two windows (#148.2), each from a CONFIG RESETSTAT on every node:
#   pass:   from the trigger to the pass's end (every entry resolved):
#           per entry an HGET, HMGET and PEXPIRE on its uid's master (an
#           unchanged session runs no compare-and-set script) and a SADD
#           per chunk on the set's master;
#   delete: from the pass's end to ReconcileEnd's reply: the delete phase,
#           an SSCAN page of the set and an HGETALL per uid on its master.
# The rule (#147.2 amended, #148.3, #148.5-7): in each window, the set's
# master's p99 of a client-side probe's round trip is at most max(2x, +
# GWCONNS_FLOOR_US) each other master's. The probe (gc_probe, on loadgen's
# CPUs) sends PING to each master every 10ms, so a per-uid command queued
# behind a slow SSCAN page or SADD on the set's master shows as a long
# round trip (R4M-9). Redis's own latency stats time a command's run only
# and cannot see that wait: the service-time p99s of the window's per-uid
# command (pass: pexpire; delete: hgetall) are recorded, not gated, as are
# the set's own commands (sadd, sscan, srem). Reading the pass window's
# numbers takes ~1s of the delete phase into the pass window.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwconns
#   Results: tier2-results/gwconns-<timestamp>/ (result.json, the stack's
#   logs and artifacts, probe.tsv: "MS NAME US" per probe round trip), plus ONE file to send back:
#   tier2-results/gwconns-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 5 minutes (the ramp 50s, the pass 50s, the delete phase 25s).
#
# Settings (every number whole and positive, refused otherwise before
# anything starts):
#   GWCONNS_CLIENTS        light clients online on gw-0 (default 250000, the cap)
#   GWCONNS_CAP            gw-0's -max-conns (default 250000, the spec's cap)
#   GWCONNS_RATE           the ramp, clients per second (default 5000)
#   GWCONNS_TRIGGERED_RATE gw-0's -triggered-rate, entries/s (default 5000)
#   GWCONNS_DELETE_RATE    the delete phase's rate, uids/s: Logic's and
#                          gw-0's -delete-rate (default 10000)
#   GWCONNS_FLOOR_US       the rule's floor over the other masters' p99, µs
#                          (default 200, #148.6-7: the probe's round trip)
#   GWCONNS_GRACE_S        MaxConnectionAgeGrace and ShutdownGrace, for the
#                          recorded margin (default 30, Logic's defaults)
#   GWCONNS_LG_MEMLIMIT    loadgen's GOMEMLIMIT (default 4GiB)
#   GWCONNS_CPUS_LOADGEN, GWCONNS_CPUS_GATEWAY, GWCONNS_CPUS_LOGIC
#                          the CPU sets (default failover's: 0-5,10,12-17,22;
#                          6-9,18-21; 11,23)
#   GWCONNS_GATEWAY_PROCS  gw-0's GOMAXPROCS (default 8, its CPU set)
#
# Checks:
#   stack          redis (6 nodes), devtoken, Logic and gw-0 Ready
#   online         every client logs in on gw-0
#   pinning        gw-0, Logic + Redis and loadgen on their CPU sets; a
#                  failure ends the run
#   gwconns_set    gw-0's gwconns set has every client (SCARD); its master
#                  and the lease key's master recorded (P6c1I-6)
#   pass           the trigger is accepted and the triggered pass ends
#                  (result done) within 2 x clients / rate + 60s; its
#                  time, the entries by result, the re-queues and the
#                  not_owner entries by reason (Kicks: a reading, run 31
#                  had 4,119) recorded
#   pass_entries   every client's entry got a definitive result once:
#                  written + unchanged + not_owner = clients, none dropped
#                  at the re-queue limit
#   pass_shards    the pass window's probe round-trip p99 (ping) on the
#                  set's master is at most max(2x, + FLOOR_US) each other
#                  master's; every master answered the probe; its failed
#                  round trips and the service-time pexpire p99s recorded;
#                  every node's commands and CPU recorded
#   end            ReconcileEnd succeeds at its first attempt (none
#                  retried or dropped), the phase scanned every client,
#                  and its rate (uids scanned / the phase's time, from
#                  Logic's histograms) is at least 90% of the delete rate
#                  (#147.3); the time, its margin to GRACE_S and gw-0's
#                  deadline (2 x tracked / delete rate + 10s) recorded only
#   end_shards     the delete window's probe round-trip p99 (ping) on the
#                  set's master is at most max(2x, + FLOOR_US) each other
#                  master's; every master answered the probe; its failed
#                  round trips and the service-time hgetall p99s recorded
#   sessions       after the phase, every client's session is in the store,
#                  on gw-0 (the phase deletes no live session); the fields
#                  it deleted recorded (0 expected: no ghost)
#   no_swap_in     no page swapped in from the trigger to the sessions check
#   idle           after loadgen stops, gw-0 reaches 0 connections
#   processes      nothing died, panicked or stopped uncleanly
#   artifacts      the metrics before and after
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init gwconns
t2_need curl taskset sudo python3
t2_require_host_setup

for v in GWCONNS_CLIENTS GWCONNS_CAP GWCONNS_RATE GWCONNS_TRIGGERED_RATE GWCONNS_DELETE_RATE GWCONNS_FLOOR_US GWCONNS_GRACE_S GWCONNS_GATEWAY_PROCS; do
  case ${!v-1} in ''|*[!0-9]*|0*) echo "gwconns: $v must be a positive whole number" >&2; exit 1 ;; esac
done
N=${GWCONNS_CLIENTS:-250000} CAP=${GWCONNS_CAP:-250000} RATE=${GWCONNS_RATE:-5000}
TR=${GWCONNS_TRIGGERED_RATE:-5000} DR=${GWCONNS_DELETE_RATE:-10000} GRACE=${GWCONNS_GRACE_S:-30} FLOOR=${GWCONNS_FLOOR_US:-200}
[ "$N" -le "$CAP" ] || { echo "gwconns: GWCONNS_CLIENTS must not exceed GWCONNS_CAP" >&2; exit 1; }
# 250k light clients: under capacity's 600k budget.
t2_mem_budget 12
t2_nofile 1048576

export STACK_PIN=1 STACK_GATEWAYS=1 STACK_REDIS=cluster
export T2_CPUS_GATEWAY=${GWCONNS_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${GWCONNS_CPUS_LOADGEN:-0-5,10,12-17,22}
export T2_CPUS_LOGIC=${GWCONNS_CPUS_LOGIC:-11,23}
export STACK_GATEWAY_PROCS=${GWCONNS_GATEWAY_PROCS:-8}
# The production cycle (10m) would start a periodic pass at a random time;
# 24h keeps it out of the run (capacity's choice), its count recorded.
STACK_GATEWAY_ARGS=(-faults -max-conns "$CAP" -reconcile-cycle 24h -reconcile-jitter 0 -triggered-rate "$TR" -delete-rate "$DR")
STACK_LOGIC_ARGS=(-delete-rate "$DR")
LG_ENV=(GOMEMLIMIT="${GWCONNS_LG_MEMLIMIT:-4GiB}")
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop_all' # newest first: loadgen stops before the stack

TRIG='pace="PACE_TRIGGERED"'
GW_COUNTERS=(
  passes_done="goim_gateway_reconcile_passes_total:$TRIG,result=\"done\""
  passes_incomplete="goim_gateway_reconcile_passes_total:$TRIG,result=\"incomplete\""
  passes_periodic='goim_gateway_reconcile_passes_total:pace="PACE_PERIODIC"'
  pass_s="goim_gateway_reconcile_pass_seconds_sum:$TRIG"
  written='goim_gateway_reconcile_entries_total:result="written"'
  unchanged='goim_gateway_reconcile_entries_total:result="unchanged"'
  refused='goim_gateway_reconcile_entries_total:result="refused"'
  not_owner='goim_gateway_reconcile_entries_total:result="not_owner"'
  no_reply='goim_gateway_reconcile_entries_total:result="no_reply"'
  requeued=goim_gateway_reconcile_requeued_total
  requeue_limit=goim_gateway_reconcile_requeue_limit_total
  ends_ok='goim_gateway_reconcile_end_total:result="ok"'
  ends_retried='goim_gateway_reconcile_end_total:result="retried"'
  ends_dropped='goim_gateway_reconcile_end_total:result="dropped"'
  end_deleted=goim_gateway_reconcile_end_deleted_total
)
LOGIC_COUNTERS=(
  end_s='goim_logic_reconcile_end_seconds_sum:pace="triggered"'
  ends='goim_logic_reconcile_end_seconds_count:pace="triggered"'
  scanned='goim_logic_reconcile_end_scanned_uids_sum:pace="triggered"'
  deleted=goim_logic_reconcile_end_deleted_total
)

t2_report_extra() {
  local f
  for f in "$T2_OUT"/stack/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# gc_shards LATENCY OWNER CMD FLOOR_US: the rule over one window's
# stack_redis_latency (#147.2, #148.3, #148.6): {"cmd", "owner", "p99_us"
# and "calls" per master, "bound_us": max(2 x, + FLOOR_US) the lowest other
# master's p99, "ok"}. ok: 3 masters
# answered, OWNER is one, each ran CMD, and OWNER's p99 <= bound_us. null if
# LATENCY does not parse.
gc_shards() {
  python3 -c 'import json, sys
lat, owner, cmd, floor = json.loads(sys.argv[1]), sys.argv[2], sys.argv[3], int(sys.argv[4])
m = {n: v["cmds"].get(cmd) or {} for n, v in lat.items() if v and v["role"] == "master"}
p99 = {n: c.get("p99_us") for n, c in m.items()}
calls = {n: c.get("calls", 0) for n, c in m.items()}
others = [p99[n] for n in m if n != owner]
bound = max(2 * min(others), min(others) + floor) if others and None not in others else None
ok = len(m) == 3 and owner in m and all(calls.values()) and bound is not None and p99[owner] is not None and p99[owner] <= bound
print(json.dumps({"cmd": cmd, "owner": owner, "p99_us": p99, "calls": calls, "bound_us": bound, "ok": ok}))' "$@" 2>/dev/null || echo null
}

# gc_probe OUT NAME=HOST:PORT...: the client-side probe (runs until killed;
# start it in the background). One TCP connection per master; every 10ms it
# sends PING to each in turn and waits for the reply, and appends one line
# per round trip to OUT: "MS NAME US" (MS: wall clock ms at the send, US:
# the round trip in µs, -1 if the send or read failed, then it reconnects).
# It sees what Redis's own latency stats cannot: the wait behind a slow
# command on the same master (head-of-line blocking, R4M-9). It execs
# python, so a bash -c running it leaves python's PID to kill.
gc_probe() {
  exec python3 -c 'import socket, sys, time
out = open(sys.argv[1], "a", buffering=1)
tg = [(a.split("=")[0], a.split("=")[1].rsplit(":", 1)) for a in sys.argv[2:]]
tg = [(n, (h, int(p))) for n, (h, p) in tg]
cs = {}
def conn(n, a):
    s = socket.create_connection(a, 2); s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1); s.settimeout(2)
    cs[n] = s.makefile("rb", 0), s
while True:
    t = time.perf_counter()
    for n, a in tg:
        ms, t0, us = time.time_ns() // 10**6, time.perf_counter_ns(), -1
        try:
            if n not in cs: conn(n, a)
            r, s = cs[n]
            s.sendall(b"PING\r\n")
            if r.readline() == b"+PONG\r\n": us = (time.perf_counter_ns() - t0) // 1000
        except OSError:
            pass
        if us < 0: cs.pop(n, None)
        out.write("%d %s %d\n" % (ms, n, us))
    time.sleep(max(0, 0.01 - (time.perf_counter() - t)))' "$@"
}

# gc_probe_latency FILE T0_MS T1_MS: the probe's samples of FILE with
# T0_MS <= MS < T1_MS, in stack_redis_latency's shape so gc_shards reads
# them with CMD ping: {NAME: {"role": "master", "zone": "", "cmds": {"ping":
# {"calls", "p50_us", "p99_us", "p999_us" (nearest rank over the good
# samples; null if none), "errors" (the -1 lines)}}}}. null if FILE is
# unreadable.
gc_probe_latency() {
  python3 -c 'import json, sys
t0, t1 = int(sys.argv[2]), int(sys.argv[3])
good, err = {}, {}
for line in open(sys.argv[1]):
    f = line.split()
    if len(f) != 3 or not t0 <= int(f[0]) < t1: continue
    good.setdefault(f[1], []); err.setdefault(f[1], 0)
    if int(f[2]) >= 0: good[f[1]].append(int(f[2]))
    else: err[f[1]] += 1
def pct(v, num, den): return v[max(0, -(-num * len(v) // den) - 1)] if v else None
out = {}
for n, v in good.items():
    v.sort()
    out[n] = {"role": "master", "zone": "", "cmds": {"ping": {"calls": len(v), "p50_us": pct(v, 50, 100), "p99_us": pct(v, 99, 100),
              "p999_us": pct(v, 999, 1000), "errors": err[n]}}}
print(json.dumps(out))' "$@" 2>/dev/null || echo null
}

# gc_probe_errors LATENCY: {NAME: the probe's failed round trips} from
# gc_probe_latency's output; null if it does not parse.
gc_probe_errors() {
  python3 -c 'import json, sys
print(json.dumps({n: v["cmds"]["ping"]["errors"] for n, v in json.loads(sys.argv[1]).items()}))' "$@" 2>/dev/null || echo null
}

# gc_end GW LOGIC CLIENTS RATE GRACE_S: the end check over gw-0's and
# Logic's counter deltas (#147.3): {"ends_ok", "retried", "dropped",
# "scanned", "duration_s", "rate", "want_rate" (90% of RATE), "margin_s"
# (GRACE_S - duration_s), "deadline_s" (gw-0's: 2 x CLIENTS / RATE + 10),
# "deleted", "ok"}. ok: one ReconcileEnd ok, none retried or dropped, at
# least CLIENTS scanned, and rate >= want_rate. null if a delta is unreadable.
gc_end() {
  python3 -c 'import json, sys
g, l = json.loads(sys.argv[1]), json.loads(sys.argv[2])
n, rate, grace = int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
z = lambda d, k: d.get(k) or 0
dur, scanned = l.get("end_s"), z(l, "scanned")
r = round(scanned / dur, 1) if dur else None
ok = z(g, "ends_ok") == 1 and z(g, "ends_retried") == 0 and z(g, "ends_dropped") == 0 and z(l, "ends") == 1 and scanned >= n and r is not None and r >= 0.9 * rate
print(json.dumps({"ends_ok": z(g, "ends_ok"), "retried": z(g, "ends_retried"), "dropped": z(g, "ends_dropped"),
                  "scanned": scanned, "duration_s": None if dur is None else round(dur, 3), "rate": r, "want_rate": 0.9 * rate,
                  "margin_s": None if dur is None else round(grace - dur, 3), "deadline_s": 2 * n / rate + 10,
                  "deleted": l.get("deleted"), "ok": ok}))' "$@" 2>/dev/null || echo null
}

# gc_wait_inc NAME METRIC MATCH FROM SECONDS: polls (every 0.2s) NAME's
# METRIC samples with MATCH (stack_metric) until their sum exceeds FROM
# (null counts as 0); fails after SECONDS.
gc_wait_inc() {
  local end=$((SECONDS + $5)) from=${4/null/0} v
  while :; do
    v=$(stack_metric "$1" "$2" "$3")
    [ "$v" != null ] && [ "$(awk -v a="$v" -v b="$from" 'BEGIN { print (a > b) }')" = 1 ] && return 0
    [ $SECONDS -lt "$end" ] || return 1
    sleep 0.2
  done
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

t0=$SECONDS
export STACK_DIR=$T2_OUT/stack
mkdir -p "$STACK_DIR"
ln -sfn ../bin "$STACK_DIR/bin"
busy=$(stack_busy_ports)
[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
stack_up 2>"$STACK_DIR/stack-up.err"
rc=$?
t2_check stack "$(t2_bool [ $rc -eq 0 ])" "\"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"logic_args\": $(t2_json_str "${STACK_LOGIC_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
[ $rc -eq 0 ] || { stack_down; t2_finish; }
lg_start main -mode light -clients "$N" -rate "$RATE" -uid-prefix main
t=$SECONDS
online=false
lg_wait_online "$N" $((N / RATE * 3 + 60)) && online=$(lg_get /stats 'd["online_by_gateway"].get("gw-0") == '"$N")
[ "$online" = true ] || online=false
ramp_s=$((SECONDS - t))
t2_check online "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
wrong=""
stack_pinned gw-0 "$T2_CPUS_GATEWAY" || wrong+=" gw-0"
for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
have=$(taskset -pc "$LG_PID" 2>/dev/null)
[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
[ -z "$wrong" ] && [ "$online" = true ] || { lg_stop_all; stack_down; t2_finish; }
t2_duration ramp $((SECONDS - t0))

# The set: its master (the lease key's: one hash tag, the epoch) and size.
lease=$(stack_lease_key gw-0) set=${lease/#gwlease:/gwconns:}
owner="" lease_owner=""
if [ -n "$lease" ]; then # an empty key would hash to slot 0's master
  owner=$(stack_redis_owner "$set") lease_owner=$(stack_redis_owner "$lease")
fi
scard=$(STACK_REDIS_TIMEOUT=5 stack_redis_node_cli "${owner:-redis-0}" scard "$set" 2>/dev/null)
case $scard in '' | *[!0-9]*) scard=null ;; esac
t2_check gwconns_set "$([ -n "$owner" ] && [ "$scard" = "$N" ] && echo true || echo false)" \
  "\"key\": \"$set\", \"owner\": \"$owner\", \"lease_owner\": \"$lease_owner\", \"members\": $scard, \"want\": $N, \"roles\": $(stack_redis_roles)"
stack_snapshot before
admin=$(stack_admins | awk '$1 == "gw-0" { print $2 }')

# The probe: a PING to each master every 10ms from loadgen's CPUs, from
# before the trigger to after the delete window (the round trip a client
# waits, #148.7).
probe_args=()
for n in $(stack_redis_slots | awk '{ print $3 }' | sort -u); do
  probe_args+=("$n=127.0.0.1:$(awk -v n="$n" '$1 == n { print $2 }' "$STACK_DIR/redis/layout")")
done
rm -f "$STACK_DIR/probe.tsv"
taskset -c "$T2_CPUS_LOADGEN" bash -c "$(declare -f gc_probe); gc_probe \"\$@\"" _ "$STACK_DIR/probe.tsv" "${probe_args[@]}" 2>"$STACK_DIR/probe.err" &
PROBE_PID=$!
t2_on_exit 'kill "$PROBE_PID" 2>/dev/null'
sleep 2

# The pass window, from the trigger (#147.1).
t2_record_start
g0=$(stack_counters gw-0 "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
redis_reset=true; stack_redis_resetstat || redis_reset=false
stack_usage_start loadgen="$LG_PID"
trig_ms=$(date +%s%3N)
trigger=$(t2_bool curl -sf -m 5 -X POST "http://$admin/fault/action/reconcile" -o /dev/null)
pass_wait=$((2 * N / TR + 60))
done=false pass_ms=null
[ "$trigger" = true ] && gc_wait_inc gw-0 goim_gateway_reconcile_passes_total "$TRIG,result=\"done\"" \
  "$(echo "$g0" | lg_py - 'd["passes_done"]')" "$pass_wait" && { done=true; pass_ms=$(($(date +%s%3N) - trig_ms)); }
pass_end_ms=$(date +%s%3N)
pass_lat=$(stack_redis_latency)
pass_use=$(stack_usage_json)
# The delete window, from the pass's end.
redis_reset2=true; stack_redis_resetstat || redis_reset2=false
stack_usage_start loadgen="$LG_PID"
end_t0=$(date +%s%3N)
g1=$(stack_counters gw-0 "${GW_COUNTERS[@]}")
end_wait=$((2 * N / DR + 10 + 90)) # gw-0's deadline, then its retries (60s) and a margin
replied=false end_ms=null
[ "$done" = true ] && gc_wait_inc gw-0 goim_gateway_reconcile_end_total 'result="ok"' \
  "$(echo "$g1" | lg_py - 'd["ends_ok"]')" "$end_wait" && { replied=true; end_ms=$(($(date +%s%3N) - end_t0)); }
end_reply_ms=$(date +%s%3N)
end_lat=$(stack_redis_latency)
end_use=$(stack_usage_json)
kill "$PROBE_PID" 2>/dev/null; wait "$PROBE_PID" 2>/dev/null
g2=$(stack_counters gw-0 "${GW_COUNTERS[@]}") l2=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
gp=$(stack_counters_delta "$g0" "$g1") gd=$(stack_counters_delta "$g1" "$g2") ld=$(stack_counters_delta "$l0" "$l2")
not_owner=$(stack_metrics gw-0 | awk '$1 ~ /^goim_gateway_reconcile_not_owner_total\{/ { r = $1; sub(/.*reason="/, "", r); sub(/".*/, "", r); out = out (out == "" ? "" : ", ") "\"" r "\": " $NF } END { print "{" out "}" }')

t2_check pass "$(t2_bool [ "$done" = true ])" \
  "\"trigger\": $trigger, \"pass_ms\": $pass_ms, \"wait_s\": $pass_wait, \"triggered_rate\": $TR, \"counters\": $gp, \"not_owner_by_reason\": ${not_owner:-null}"
t2_check pass_entries "$(echo "${gp:-null}" | lg_true - '(d["written"] or 0) + (d["unchanged"] or 0) + (d["not_owner"] or 0) == '"$N"' and (d["requeue_limit"] or 0) == 0')" \
  "\"want\": $N, \"entries\": $(echo "${gp:-null}" | lg_py - '{k: d[k] for k in ("written", "unchanged", "refused", "not_owner", "no_reply", "requeued", "requeue_limit")}')"
probe=$STACK_DIR/probe.tsv
pp=$(gc_probe_latency "$probe" "$trig_ms" "$pass_end_ms")
ps=$(gc_shards "$pp" "$owner" ping "$FLOOR")
pst=$(gc_shards "${pass_lat:-null}" "$owner" pexpire "$FLOOR")
t2_check pass_shards "$(echo "${ps:-null}" | lg_true - 'd["ok"]')" "\"rule\": ${ps:-null}, \"probe_errors\": $(gc_probe_errors "$pp"), \"service_time\": ${pst:-null}, \"redis_reset\": $redis_reset"
end=$(gc_end "${gd:-null}" "${ld:-null}" "$N" "$DR" "$GRACE")
t2_check end "$(echo "${end:-null}" | lg_true - 'd["ok"]')" "\"replied\": $replied, \"reply_ms\": $end_ms, \"wait_s\": $end_wait, \"delete_rate\": $DR, \"grace_s\": $GRACE, \"reading\": ${end:-null}"
ep=$(gc_probe_latency "$probe" "$end_t0" "$end_reply_ms")
es=$(gc_shards "$ep" "$owner" ping "$FLOOR")
est=$(gc_shards "${end_lat:-null}" "$owner" hgetall "$FLOOR")
t2_check end_shards "$(echo "${es:-null}" | lg_true - 'd["ok"]')" "\"rule\": ${es:-null}, \"probe_errors\": $(gc_probe_errors "$ep"), \"service_time\": ${est:-null}, \"redis_reset\": $redis_reset2"
sessions=$(t2_sessions 0)
t2_check sessions "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
  "\"sessions\": ${sessions:-null}, \"deleted\": $(echo "${ld:-null}" | lg_py - 'd["deleted"]')"
t2_record_end
t2_field pass_window "{\"owner\": \"$owner\", \"pass_ms\": $pass_ms, \"counters\": ${gp:-null}, \"redis_latency\": ${pass_lat:-null}, \"usage\": ${pass_use:-null}}"
t2_field delete_window "{\"owner\": \"$owner\", \"reply_ms\": $end_ms, \"gateway\": ${gd:-null}, \"logic\": ${ld:-null}, \"redis_latency\": ${end_lat:-null}, \"usage\": ${end_use:-null}}"
t2_duration reconcile $((SECONDS - t0))

stack_snapshot after
lg_stop_all
t=$SECONDS
idle=$(t2_bool stack_wait_idle 600 2>"$STACK_DIR/idle.err")
t2_check idle "$idle" "\"stop_s\": $((SECONDS - t)), \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
stack_down
problems=$(stack_health)
t2_check processes "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
missing=""
a=$STACK_DIR/artifacts
for f in before/logic after/logic before/gw-0 after/gw-0; do
  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
t2_field run "{\"clients\": $N, \"cap\": $CAP, \"rate\": $RATE, \"triggered_rate\": $TR, \"delete_rate\": $DR, \"grace_s\": $GRACE, \"floor_us\": $FLOOR, \"ramp_s\": $ramp_s, \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}")}"

t2_not_covered "a delete phase cut at a grace (MaxConnectionAgeGrace, ShutdownGrace): no Logic SIGTERM phase (#147.4); the race suite pins a cut ReconcileEnd's retry and the idempotent deletes" \
  "ghosts: every client is live, so the delete phase deletes nothing and runs no compare-and-delete script" \
  "the 600k benchmark's ReconcileEnd (4.8MB): capacity's P_reconcile_end (CAPACITY_RECONCILE_CYCLE set)" \
  "several gateways' passes at once: one gateway (#148.1)"
t2_finish
