#!/usr/bin/env bash
# Tier 2: floods (Plan 6b3b), the spec's load tests "handshake flood" (two
# tests, #121) and "slow-client flood", each phase on a fresh stack:
#   storm: a legitimate 20k/s storm on one gateway, no flood: 200k light
#       clients ramped at 20,000/s against the gateway's default handshake
#       limit (25,000/s bucket, burst 2,500, half the loops computing): the
#       bucket shapes, it does not reject (#121 (2)). One gateway, since the
#       bucket is per gateway and the spec's 20k/s is a storm's peak. The
#       concurrency limit refuses (no_slot) without a queue, so its loss is
#       recorded, as parked #111's input, not failed (O2).
#   garbage: 2 x 200k light clients (the operating point per gateway) Ping
#       every 30s, then loadgen instance flood sends garbage handshakes (a
#       valid header and key_id, a random Noise message: each costs the
#       gateway a DH) at 80,000/s for 90s from 253 sources, above both
#       gateways' buckets (2 x 25,000/s) even when least-connections splits
#       it 65/35 (the dry run's split: a refused attempt closes sooner, so
#       its gateway gets more), so each bucket refuses part of what the
#       concurrency limit lets through (the slot is taken first):
#       every Ping is still answered on time and the gateways renew their
#       leases (#121 (1)).
#   slow: instances flood and flood2 hold 80k slow-handshake and 40k
#       slow-auth connections (sends nothing; handshakes, then sends
#       nothing), freed by the gateways' 5s timeouts and redialed, while
#       400k light clients log in beside them and Ping: no Busy, no reject,
#       no bucket refusal (slow-auth's ~7k handshakes/s fit the buckets,
#       6b3a Review Focus 5); the concurrency limit's refusals are recorded.
#       The floods run off loadgen's CPU set: in run 30b the three loadgens
#       saturated loadgen's 8 CPUs while the gateway set sat 69.5% idle and
#       Logic's 94.6% (#126). Slow-handshake (no DH) runs on Logic's set,
#       slow-auth on the gateways', each with a small GOMAXPROCS so its
#       threads don't crowd the gateways' (#127, 6b3b3 review O1, O2). A
#       per-second series from the light clients' start orders cause and
#       effect (the CPU sets, the gateways' run-queue delay, pending
#       sockets and refusals, logins), and a verdict on the three CPU sets
#       tells a saturated run from a gateway failure.
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
#   FLOODS_GARBAGE_RATE    garbage attempts per second (default 80000)
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
#   FLOODS_SLOW_HS_CPUS    slow: the CPU list the slow-handshake flood is pinned
#                          to (default T2_CPUS_LOGIC, shared with Logic and
#                          Redis, #127); inside one of the three CPU sets
#   FLOODS_SLOW_AUTH_CPUS  slow: the slow-auth flood's (default
#                          T2_CPUS_GATEWAY, shared with the gateways)
#   FLOODS_SLOW_HS_PROCS   slow: the slow-handshake flood's GOMAXPROCS (default 2)
#   FLOODS_SLOW_AUTH_PROCS slow: the slow-auth flood's GOMAXPROCS (default 4)
#
# Checks (per phase run):
#   storm_stack           redis, devtoken, Logic and the gateway become Ready
#   storm_online          every client logs in, or each one that did not gave
#                         up after pre-handshake closes only (the concurrency
#                         limit's loss, #111's input, O2); gave_up is recorded
#   storm_rate            new clients reached the gateway (handshakes it saw,
#                         ok + no_slot + no_token, less loadgen's
#                         pre-handshake closes, which each come back as a
#                         retry) at >= 0.9 x the rate, between 5% and 95% of
#                         them (so the limiter saw the storm, not a
#                         loadgen-bound trickle that retries inflate: final
#                         review I-1); the seen and completed (ok) rates and
#                         the per-second series are recorded. A loadgen at
#                         >= 85% of its CPUs over the same window (its
#                         ramp, not the settling after it: 6b3b2 review O4)
#                         is the bound, not the gateway (run 30: 6.88 of 8
#                         over the whole phase): the check passes with
#                         verdict "loadgen-bound" and the rate goes to
#                         not_covered
#   storm_bucket          the bucket refused nothing (no_token 0, #121);
#                         the concurrency limit's refusals (no_slot, each a
#                         pre-handshake close the client retries after
#                         0.5-1.5s), their share of the handshakes and
#                         Busy are recorded
#   no_swap_in_storm      no page swapped in from the ramp to the last login
#   garbage_stack, slow_stack   as storm_stack, two gateways
#   garbage_online        every light client logs in (or gave up as in
#                         storm_online), each gateway holds 45-55%
#   garbage_delivered     loadgen kept the schedule (attempts >= 0.95 x rate x
#                         seconds) and the gateways saw it: failed + no_slot +
#                         no_token >= 0.95 x rate x seconds (6b3a Review
#                         Focus 2); outcomes, TIME_WAIT and kernel TCP
#                         counters are recorded (Review Focus 3)
#   garbage_limited       each gateway's limiter refused part of the flood
#                         (no_slot + no_token > 0): it ran past the limiter.
#                         The slot is taken first, so the bucket sees only
#                         what the concurrency limit lets through; on the
#                         Tier 2 host the slots bind first (~16k DH/s on 3,
#                         run 30), so which part bound and the DHs per second
#                         are recorded (#111's input). Either part passes,
#                         which 6b3b's review O3 had ruled out: when only the
#                         slots bound, the bucket goes to not_covered
#                         (6b3b2 review O6)
#   garbage_heartbeats    every Ping answered (no Pong missing, at least as
#                         many Pongs over the light run as clients online: a
#                         floor, not per client), the slowest Pong under 10s
#                         (the SDK's Pong timeout), no connection lost, no
#                         gateway closed one for its heartbeat, and no client
#                         waited more than the interval + 5s for its next
#                         Ping (ping_gap_max_ms: run 30's pinger fell 126s
#                         behind); the pinger's own lag, slowest Send and
#                         Sends past their 1s deadline (each closes its
#                         client, so lost is not empty) are recorded
#   garbage_lease         each gateway renewed its lease every interval
#                         through the flood, and Logic saw no renewal error
#   no_swap_in_garbage    no page swapped in over the flood
#   slow_flood            both floods held >= 85% of their connections, each
#                         slot freed at least once, before the light clients start
#   slow_held             every slow connection was closed by a gateway, held
#                         p50 >= 5s, p99 <= 6.5s, max <= 7.5s (5s + one tick of
#                         the wheel, plus scheduling slack, 6b3a O7); a
#                         slow-auth handshake the limiter refused ends
#                         pre-handshake, with no hold, and is counted (O1);
#                         a connection the gateway closed before 2.5s (at
#                         capacity, a limit, pending_mem) is refused, with
#                         no hold, and is counted (#135); the two floods'
#                         refused are at most the gateways' refusals over
#                         the phase (pending_mem + at_capacity + no_slot +
#                         no_token, the light clients' included: a loose
#                         bound, but a close no counter explains fails it,
#                         review O7)
#   slow_freed            the gateways counted at least as many
#                         handshake_timeout and auth_timeout closes as the
#                         floods' closed outcomes, the closes held >= 2.5s
#                         (refused ones are not timeouts, #135, run 30n)
#   slow_legit            every light client logs in beside the floods (or
#                         gave up as in storm_online): no
#                         Busy, no reject (login_queue, pending_mem: the
#                         slots, at_capacity) and no bucket refusal
#                         (no_token); the concurrency limit's (no_slot,
#                         retried as in storm) and loadgen's failed are
#                         recorded. Only a valid run passes: verdict
#                         "valid" when none of loadgen's, the gateways' and
#                         Logic's CPU sets was >= 85% busy over the light
#                         clients' ramp or over the hold, and none was >= 95%
#                         busy for 5s in a row in slow.series before every
#                         light client was online (the ramp's average hid
#                         run 30m's 16s at 98-100%, Plan 6b3c), else
#                         "loadgen-saturated", "gateway-set-saturated" or
#                         "logic-set-saturated" (the floods share the last
#                         two), which fails with a not_covered line (#126:
#                         run 30b's pending_mem rejects came with a starved
#                         loadgen, and which came first is unknown). The
#                         verdict names the first saturated set, not the
#                         cause: retries after gateway refusals saturate
#                         loadgen too, so the order in slow.series.first_s
#                         rules (6b3b3 review O3)
#   slow_heartbeats       as garbage's, over the floods, and only with
#                         verdict "valid", as slow_legit
#   slow_lease            as garbage's, over the floods
#   no_swap_in_slow       no page swapped in from the floods' start to their stop
#   <phase>_conntrack     the host's nf_conntrack_count, sampled every second
#                         from the stack up to its stop, peaked under 80% of
#                         nf_conntrack_max (the spec's alert) and under 10% of
#                         the phase's connections, so the NOTRACK rules work,
#                         not only exist (#123, 6b3b2 review O3); run 30's
#                         slow phase ran on a full table unseen; passes with
#                         no conntrack loaded
#   <phase>_idle          after loadgen stops, every gateway reaches 0
#                         connections and settles its Logouts
#   <phase>_processes     nothing died, panicked or stopped uncleanly
#   artifacts             metrics before and after each phase's window, the
#                         slow phase's series (slow-series.jsonl), the
#                         storm's and the garbage flood's CPU profiles,
#                         loadgen's results
# Each phase also records its CPU sets' shares and softnet drops over its
# window (cpu_sets: run 30's garbage SYN loss had none snapshotted), and
# conntrack's own drops (conntrack_stat: insert_failed, drop, early_drop,
# invalid, summed over CPUs; null without conntrack), which a table that is
# not full still makes (6b3b2 review O7).
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init floods
t2_need curl taskset sudo python3 ss
t2_require_host_setup

PHASES=${FLOODS_PHASES:-storm garbage slow}
named=" "
for p in $PHASES; do
  case $p in storm | garbage | slow) ;; *) echo "floods: unknown phase '$p' (storm, garbage, slow)" >&2; exit 1 ;; esac
  case $named in *" $p "*) echo "floods: phase '$p' named twice in FLOODS_PHASES" >&2; exit 1 ;; esac
  named+="$p "
done
for v in FLOODS_STORM_CLIENTS FLOODS_STORM_RATE FLOODS_CLIENTS FLOODS_RATE FLOODS_HEARTBEAT \
  FLOODS_GARBAGE_RATE FLOODS_GARBAGE_INFLIGHT FLOODS_GARBAGE_SECONDS FLOODS_GARBAGE_BUCKET \
  FLOODS_SLOW_HANDSHAKE FLOODS_SLOW_AUTH FLOODS_SLOW_RATE FLOODS_SLOW_HOLD \
  FLOODS_SLOW_HS_PROCS FLOODS_SLOW_AUTH_PROCS; do
  case ${!v-1} in ''|*[!0-9]*|0*) echo "floods: $v must be a positive whole number" >&2; exit 1 ;; esac
done
# in_set LIST SET: LIST names a CPU, and every CPU of LIST is in SET (a
# reversed range such as 5-3 names none).
in_set() {
  local c set=" $(t2_cpulist_expand "$2") " l
  l=$(t2_cpulist_expand "$1")
  [ -n "$l" ] || return 1
  for c in $l; do [[ $set == *" $c "* ]] || return 1; done
}
# The slow floods' CPU lists, each inside one of the three sets, so the
# verdict reads every CPU a flood runs on (6b3b3 review O5).
HS_CPUS=${FLOODS_SLOW_HS_CPUS:-$T2_CPUS_LOGIC} AUTH_CPUS=${FLOODS_SLOW_AUTH_CPUS:-$T2_CPUS_GATEWAY}
for l in "$HS_CPUS" "$AUTH_CPUS"; do
  [[ $l =~ ^[0-9]+(-[0-9]+)?(,[0-9]+(-[0-9]+)?)*$ ]] &&
    { in_set "$l" "$T2_CPUS_GATEWAY" || in_set "$l" "$T2_CPUS_LOGIC" || in_set "$l" "$T2_CPUS_LOADGEN"; } ||
    { echo "floods: FLOODS_SLOW_HS_CPUS and FLOODS_SLOW_AUTH_CPUS must each be a CPU list inside T2_CPUS_GATEWAY, T2_CPUS_LOGIC or T2_CPUS_LOADGEN" >&2; exit 1; }
done
[ "${FLOODS_GARBAGE_SECONDS:-90}" -ge 45 ] || { echo "floods: FLOODS_GARBAGE_SECONDS must be at least 45 (the flood's profile)" >&2; exit 1; }
[ "${FLOODS_HEARTBEAT:-30}" -ge 30 ] && [ "${FLOODS_HEARTBEAT:-30}" -le 300 ] ||
  { echo "floods: FLOODS_HEARTBEAT must be 30 to 300 (the gateway's -heartbeat)" >&2; exit 1; }
# Light clients cost loadgen ~10KB each and the gateway less; the slow
# phase's 400k light and 120k slow connections are the most at once, near
# capacity's 600k on one gateway, which ran within 16.
t2_mem_budget 16
t2_nofile 1048576

export STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop_all' # newest first: every loadgen instance stops before the stack
smp="" prof=""
t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
# The profile subshell with its curls (6b3b4 final review M4).
t2_on_exit '[ -n "$prof" ] && t2_kill_tree "$prof"'
cts=""
t2_on_exit '[ -n "$cts" ] && kill "$cts" 2>/dev/null'

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

# up NAME: t2_up, then the conntrack sampler.
up() {
  t2_up "$1" || return
  ct_start
}

# down NAME CONNS: every loadgen stopped, the gateways idle, the stack
# stopped; checks NAME_conntrack (CONNS: the phase's connections at their
# most), NAME_idle and NAME_processes.
down() {
  lg_stop_all
  local t=$SECONDS idle stop_s problems
  idle=$(t2_bool stack_wait_idle 900 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  ct_check "$1" "$2"
  problems=$(stack_health)
  t2_check "${1}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
}

# ct_start: samples the host's nf_conntrack_count ("unix_s count") every
# second into $STACK_DIR/conntrack.tsv until ct_check.
CT_PROC=/proc/sys/net/netfilter
ct_start() {
  while :; do
    echo "$(date +%s) $(cat "$CT_PROC/nf_conntrack_count" 2>/dev/null)"
    sleep 1
  done >"$STACK_DIR/conntrack.tsv" &
  cts=$!
}
# ct_check PHASE CONNS: stops the sampler; checks PHASE_conntrack. With the
# stack's ports untracked the peak is the host's own traffic, far below the
# phase's CONNS connections; tracked, it is at least them.
ct_check() {
  kill "$cts" 2>/dev/null; wait "$cts" 2>/dev/null; cts=""
  local max peak
  max=$(cat "$CT_PROC/nf_conntrack_max" 2>/dev/null || echo null)
  peak=$(awk 'NF == 2 { if ($2 + 0 > p) p = $2 + 0; n++ } END { print n ? p : "null" }' "$STACK_DIR/conntrack.tsv" 2>/dev/null)
  t2_check "${1}_conntrack" "$(echo "{\"peak\": ${peak:-null}, \"max\": $max}" | lg_true - 'd["max"] is None or (d["peak"] is not None and d["peak"] <= 0.8 * d["max"] and d["peak"] < 0.1 * '"$2"')')" \
    "\"peak\": ${peak:-null}, \"max\": $max, \"conns\": $2, \"peak_per_conn\": $(echo "{\"peak\": ${peak:-null}}" | lg_py - 'None if d["peak"] is None else round(d["peak"] / '"$2"', 4)'), \"samples\": $(wc -l <"$STACK_DIR/conntrack.tsv" 2>/dev/null || echo 0)"
}
# ct_stat_snap FILE: conntrack's per-CPU statistics (absent: no conntrack);
# ct_stat_json A B: the columns that count drops, summed over the CPUs, B
# less A, or null.
CT_STAT=/proc/net/stat/nf_conntrack
ct_stat_snap() { cat "$CT_STAT" >"$1" 2>/dev/null || rm -f "$1"; }
ct_stat_json() {
  python3 -c 'import json, sys
def read(p):
    rows = [l.split() for l in open(p)]
    sums = {}
    for r in rows[1:]:
        for k, v in zip(rows[0], r):
            sums[k] = sums.get(k, 0) + int(v, 16)
    return sums
a, b = read(sys.argv[1]), read(sys.argv[2])
print(json.dumps({k: b[k] - a[k] for k in ("insert_failed", "drop", "early_drop", "invalid") if k in a and k in b}))' "$1" "$2" 2>/dev/null || echo null
}

# hs_sampler NAME FILE PID: appends "unix_ms ok refused pre cpu" every
# second, NAME's completed handshakes, the limiter's refusals (no_slot +
# no_token, a label with no sample yet read as 0), the current loadgen's
# pre-handshake closes (each a retry to come, final review I-1) and PID's
# CPU time in clock ticks (loadgen's, so its share over the ramp alone can
# be read: 6b3b2 review O4), until killed. A second with an unreadable
# loadgen is skipped.
hs_sampler() {
  local c pre cpu re='"ok": ([0-9]+), "slot": ([0-9]+|null), "token": ([0-9]+|null)'
  while :; do
    pre=$(lg_get /stats 'd["failed"].get("pre-handshake", 0)')
    c=$(stack_counters "$1" ok='goim_gateway_handshakes_total:result="ok"' \
      slot='goim_gateway_handshakes_total:result="no_slot"' token='goim_gateway_handshakes_total:result="no_token"')
    cpu=$(awk '{ print $14 + $15 }' "/proc/$3/stat" 2>/dev/null) # read beside the timestamp: /stats is slow on a busy loadgen
    [[ $pre =~ ^[0-9]+$ ]] && [[ $cpu =~ ^[0-9]+$ ]] && [[ $c =~ $re ]] && # c last: BASH_REMATCH is its
      echo "$(date +%s%3N) ${BASH_REMATCH[1]} $((${BASH_REMATCH[2]/null/0} + ${BASH_REMATCH[3]/null/0})) $pre $cpu" >>"$2"
    sleep 1
  done
}
# storm_verdict RATE: the lg_py expression for storm_rate's verdict, on
# {"h": hs_rate's reading, "cpus": loadgen's CPUs}: met, loadgen-bound (a
# ramp reading at >= 85% of a non-empty CPU set) or short.
storm_verdict() {
  echo '"met" if ((d["h"] or {}).get("new_per_s") or 0) >= 0.9 * '"$1"' else "loadgen-bound" if d["cpus"] > 0 and (d["h"] or {}).get("loadgen_cores_ramp") is not None and d["h"]["loadgen_cores_ramp"] >= 0.85 * d["cpus"] else "short"'
}
# slots_only: the gateways whose bucket refused nothing while their slots did.
slots_only='[g for g, v in d.items() if v["hs_no_token"] == 0 and v["hs_no_slot"] > 0]'

# hs_rate FILE N: {"seen_per_s" (handshakes the gateway saw, ok or refused,
# per second between the first sample at 5% of N seen and the last below 95%:
# the ramp's body, a refused client's retry included), "new_per_s" (the same
# less the pre-handshake closes 1s earlier, the retries made by then: new
# clients only, final review I-1), "samples" (the samples new_per_s rests
# on and their median gap in ms: a saturated loadgen answers /stats late,
# P6b3bI-1), "rate_per_s" (completed, between 5% and 95% of N completed,
# which the retries stretch), "ramp_s" (of the completed, 5% to 95%
# interpolated), "peak_per_s" (completed), "loadgen_cores_ramp" (loadgen's
# CPU over new_per_s's samples, null with fewer than 2), "series": [[s since the first
# sample, completed/s, seen/s, new/s], ...] (first 120)}; null when the
# samples never reach 95% of N completed.
hs_rate() {
  python3 -c 'import json, os, sys
rows = [tuple(int(x) for x in l.split()) for l in open(sys.argv[1]) if len(l.split()) == 5 and all(x.isdigit() for x in l.split())]
n = int(sys.argv[2])
def cross(f, x):
    # When f reached x, interpolated between the samples around it: a sample
    # well past the end of a short ramp would read the rate low.
    for i, r in enumerate(rows):
        if f(r) >= x:
            if i == 0 or f(r) == f(rows[i - 1]):
                return r[0]
            p = rows[i - 1]
            return p[0] + (x - f(p)) * (r[0] - p[0]) / (f(r) - f(p))
    return None
def per_s(f):
    # The rate between the first sample at 5% of N and the last below 95%,
    # both inside the ramp: an interval holding its start or end reads low.
    body = [r for r in rows if 0.05 * n <= f(r) < 0.95 * n]
    lo, hi = cross(f, 0.05 * n), cross(f, 0.95 * n)
    if len(body) < 2 or body[-1][0] <= body[0][0] or lo is None or hi is None:
        return None, None
    return round((f(body[-1]) - f(body[0])) * 1000 / (body[-1][0] - body[0][0])), round((hi - lo) / 1000, 1)
rate, ramp = per_s(lambda r: r[1])
seen, _ = per_s(lambda r: r[1] + r[2])
def pre_at(t):
    # Pre-handshake closes by time t, interpolated (0 before the first sample).
    if not rows or t < rows[0][0]:
        return 0
    for p, r in zip(rows, rows[1:]):
        if t < r[0]:
            return p[3] + (r[3] - p[3]) * (t - p[0]) / (r[0] - p[0])
    return rows[-1][3]
# A close is retried 0.5-1.5s later, so the retries made by a sample are
# about the closes 1s before it; a close still waiting is no retry yet.
fnew = lambda r: r[1] + r[2] - pre_at(r[0] - 1000)
new, _ = per_s(fnew)
if rate is None:
    print("null"); sys.exit()
nb = [r[0] for r in rows if 0.05 * n <= fnew(r) < 0.95 * n]
gaps = sorted(b - a for a, b in zip(nb, nb[1:]))
rb = [r for r in rows if 0.05 * n <= fnew(r) < 0.95 * n]
cores = round((rb[-1][4] - rb[0][4]) / os.sysconf("SC_CLK_TCK") * 1000 / (rb[-1][0] - rb[0][0]), 3) if len(rb) >= 2 and rb[-1][0] > rb[0][0] else None
pts = [((r1[0] - rows[0][0]) / 1000, (r1[1] - r0[1]) * 1000 / (r1[0] - r0[0]), (r1[1] + r1[2] - r0[1] - r0[2]) * 1000 / (r1[0] - r0[0]),
        (fnew(r1) - fnew(r0)) * 1000 / (r1[0] - r0[0])) for r0, r1 in zip(rows, rows[1:]) if r1[0] > r0[0]]
print(json.dumps({"seen_per_s": seen, "new_per_s": new, "samples": {"n": len(nb), "gap_ms": gaps[len(gaps) // 2] if gaps else None},
       "rate_per_s": rate, "ramp_s": ramp, "peak_per_s": round(max(p[1] for p in pts)), "loadgen_cores_ramp": cores,
       "series": [[round(t, 1), round(v), round(w), round(x)] for t, v, w, x in pts[:120]]}))' "$1" "$2" 2>/dev/null || echo null
}

# settled N and logged_in N: /stats conditions for N light clients. Settled:
# the ramp is done and each client is online or gave up. Logged in: all N
# online, or each one that is not gave up after pre-handshake closes only,
# the concurrency limit's loss that parked #111 is about (O2); any other
# failure (Busy, a reject, a timeout) still fails. The failures are read
# over all N, since loadgen keeps no reasons per client: with every client
# online a failure some client recovered from passes, while with one client
# given up the same failure anywhere fails (always on the failing side,
# P6b3bI-4).
settled() { echo 'd["ramp_done"] and d["online"] + d["failed"].get("gave-up", 0) >= '"$1"; }
logged_in() {
  echo 'd["online"] == '"$1"' or (d["online"] + d["failed"].get("gave-up", 0) == '"$1"' and set(d["failed"]) <= {"pre-handshake", "gave-up"})'
}

# timewait: the host's TIME_WAIT sockets now (ss -s).
timewait() { ss -s 2>/dev/null | sed -n 's/^TCP:.*timewait \([0-9]*\).*/\1/p' | head -n 1 | grep . || echo null; }

# heartbeats PHASE N DELTA [VERDICT]: checks PHASE_heartbeats over the light
# result of N clients online (instance main, stopped) and the gateways'
# counter delta; with VERDICT (slow_verdict's, a JSON string) only "valid"
# passes.
# The gateway closes a client 2 x interval + 10s after its last frame, so a
# Ping more than the interval + 5s late is a pinger that fell behind.
# Pongs >= N is a floor over the whole light run, not per client: pong_missing
# and lost catch a Ping left unanswered in the flood (O5).
heartbeats() {
  local r=$STACK_DIR/artifacts/loadgen-$LG_NAME.json v=null gate=
  # A verdict argument, even an empty one, gates: only "valid" passes.
  [ $# -ge 4 ] && v=${4:-null} gate='d["v"] == "valid" and '
  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3, \"v\": $v}" | lg_true - "$gate"'d["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values()) and d["r"]["ping_gap_max_ms"] <= '"$(((HB + 5) * 1000))")" \
    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ping_gap_max_ms\": $(lg_json 'd["ping_gap_max_ms"]'), \"ping_tick_lag_max_ms\": $(lg_json 'd["ping_tick_lag_max_ms"]'), \"ping_send_max_ms\": $(lg_json 'd["ping_send_max_ms"]'), \"ping_send_timeouts\": $(lg_json 'd["ping_send_timeouts"]')$([ $# -ge 4 ] && echo ", \"verdict\": $v")"
}

# slow_sampler FILE LIGHT FLOOD FLOOD2: appends one JSON line a second to
# FILE until killed (#126): {"ms", "ticks": [the three loadgens' CPU
# ticks], "sets": {"loadgen", "gateway", "logic": [busy, total]}, "lg":
# the light instance's {"online", "gave_up", "failed", "refused": its own
# pre-handshake failures, a gateway's refusals} (null when /stats does not
# answer within 1s: a saturated loadgen answers late), "rq": {gateway: its
# threads' run-queue wait in ns}, apart from its scrape so a failed one
# keeps it (P6b3b3I-2), "gw": {gateway: SERIES_COUNTERS, null if its
# /metrics does not answer within 1s}, "ms_end": the clock after the
# gateways' scrapes}. The clock,
# CPU and run-queue readings come first, side by side; the gateways'
# scrapes trail them by ms_end - ms (up to ~2s), and /stats comes last.
SERIES_COUNTERS=(
  handshake='goim_gateway_connections:state="handshake"'
  auth_pending='goim_gateway_connections:state="auth_pending"'
  online='goim_gateway_connections:state="online"'
  pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
  no_slot='goim_gateway_handshakes_total:result="no_slot"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
)
slow_sampler() {
  local ms ms_end ticks sets lg gw rqj g p t
  local -A rq
  while :; do
    ms=$(date +%s%3N) ticks=""
    for p in "$2" "$3" "$4"; do t=$(stack_ticks "$p"); ticks+="${ticks:+, }${t:-null}"; done
    sets="\"loadgen\": [$(t2_set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(t2_set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(t2_set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
    rq=()
    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do rq[$g]=$(t2_runq_ns "${STACK_PIDS[$g]}"); done
    gw="" rqj=""
    for g in "${!rq[@]}"; do
      t=$(STACK_CURL_TIMEOUT=1 stack_counters "$g" "${SERIES_COUNTERS[@]}")
      gw+="${gw:+, }\"$g\": $t" rqj+="${rqj:+, }\"$g\": ${rq[$g]:-null}"
    done
    ms_end=$(date +%s%3N)
    lg=$(LG_CURL_TIMEOUT=1 lg_get /stats '{"online": d["online"], "gave_up": d["failed"].get("gave-up", 0), "failed": sum(d["failed"].values()), "refused": d["failed"].get("pre-handshake", 0)}')
    echo "{\"ms\": $ms, \"ticks\": [$ticks], \"sets\": {$sets}, \"lg\": ${lg:-null}, \"rq\": {$rqj}, \"gw\": {$gw}, \"ms_end\": $ms_end}" >>"$1"
    sleep 1
  done
}

# slow_series FILE N: slow_sampler's lines as {"columns", "rows" (one per
# interval between samples, t_s since the first; a column null where a
# reading is missing), "first_s" (when each sign of trouble first showed,
# null if never: a CPU set at >= 85% busy, /stats unanswered, a gateway
# scrape late, a pending_mem reject, a no_slot refusal, a light client
# refused, a client given up, a heartbeat close), "samples"}; null if
# unreadable. A sample whose gateway scrapes trailed its clock by over
# 1.5s, or missed a gateway, gives no gateway readings: the next on-time
# sample's rates are taken from the last on-time one, so its refusals are
# dated late rather than early (6b3b3 final review I1, M1). no_slot counts
# slow-auth's own refusals too; lg_refused_per_s is the light clients'
# alone (M5). gw_runq_ms_per_s is the larger gateway's run-queue wait per
# second, summed over its threads, null for a gateway whose total went
# down (a thread gone, M6), kept through a failed scrape (P6b3b3I-2):
# recorded, not gated (6b3b3 review O2). "sat_run_s": per CPU set, the
# longest run of rows at >= 95% busy while fewer than N light clients were
# online or gave up, as in settled (or /stats went unanswered), in seconds: what slow_verdict reads
# beside the windows' averages (Plan 6b3c, run 30m).
slow_series() {
  python3 -c 'import json, os, sys
rows = []
for l in open(sys.argv[1]):
    try:
        rows.append(json.loads(l))
    except ValueError:
        pass
hz = os.sysconf("SC_CLK_TCK")
cols = ["t_s", "scrape_ms", "lg_online", "lg_gave_up", "lg_failed_per_s", "lg_refused_per_s", "light_cores", "flood_cores", "flood2_cores",
        "loadgen_set_busy_pct", "gateway_set_busy_pct", "logic_set_busy_pct", "gw_runq_ms_per_s", "pending_max",
        "online_conns", "pending_mem_per_s", "no_slot_per_s", "logins_ok_per_s", "closes_heartbeat_per_s"]
def busy(a, b, k):
    x, y = a["sets"].get(k, []), b["sets"].get(k, [])
    return round(100 * (y[0] - x[0]) / (y[1] - x[1]), 1) if len(x) == 2 and len(y) == 2 and y[1] > x[1] else None
def skew(b):
    return b["ms_end"] - b["ms"] if b.get("ms_end") is not None else None
def on_time(b):
    return skew(b) is not None and skew(b) <= 1500 and len(b["gw"]) > 0 and all(b["gw"].values())
def gsum(b, k):
    v = [g.get(k) for g in b["gw"].values()]
    return None if None in v else sum(v)
def per_s(p, b, k):
    # A counter with no sample yet in p counts 0 there (M2).
    if p is None or set(p["gw"]) != set(b["gw"]) or None in [g.get(k) for g in b["gw"].values()]:
        return None
    return round(sum(b["gw"][g][k] - (p["gw"][g].get(k) or 0) for g in b["gw"]) / ((b["ms"] - p["ms"]) / 1000), 1)
def runq(a, b, dt):
    x, y = a.get("rq") or {}, b.get("rq") or {}
    v = [(y[g] - x[g]) / 1e6 / dt for g in y if y[g] is not None and x.get(g) is not None]
    v = [x for x in v if x >= 0]
    return round(max(v), 1) if v else None
def lrate(a, b, k, dt):
    return round((b["lg"][k] - a["lg"][k]) / dt, 1) if b["lg"] and a["lg"] and b["lg"].get(k) is not None and a["lg"].get(k) is not None else None
out = []
last = rows[0] if rows and on_time(rows[0]) else None
for a, b in zip(rows, rows[1:]):
    dt = (b["ms"] - a["ms"]) / 1000
    if dt <= 0:
        continue
    cores = [None if x is None or y is None else round((y - x) / hz / dt, 2) for x, y in zip(a["ticks"], b["ticks"])]
    lg = b["lg"]
    gwr = [None] * 6
    if on_time(b):
        pend = [g["handshake"] + g["auth_pending"] for g in b["gw"].values() if g.get("handshake") is not None and g.get("auth_pending") is not None]
        gwr = [max(pend) if pend else None, gsum(b, "online")] + [per_s(last, b, k) for k in ("pending_mem", "no_slot", "logins_ok", "closes_heartbeat")]
        last = b
    out.append([round((b["ms"] - rows[0]["ms"]) / 1000, 1), skew(b), lg and lg["online"], lg and lg["gave_up"],
                lrate(a, b, "failed", dt), lrate(a, b, "refused", dt)] + cores +
               [busy(a, b, "loadgen"), busy(a, b, "gateway"), busy(a, b, "logic"), runq(a, b, dt)] + gwr)
c = {k: i for i, k in enumerate(cols)}
def first(f):
    return next((r[0] for r in out if f(r)), None)
pos = lambda k: lambda r: (r[c[k]] or 0) > 0
sat = lambda k: lambda r: (r[c[k]] or 0) >= 85
def sat_run(k):
    best = run = 0.0
    prev = 0.0
    for r in out:
        dt, prev = r[0] - prev, r[0]
        if (r[c["lg_online"]] is None or r[c["lg_online"]] + (r[c["lg_gave_up"]] or 0) < int(sys.argv[2])) and (r[c[k + "_set_busy_pct"]] or 0) >= 95:
            run += dt
            best = max(best, run)
        else:
            run = 0.0
    return round(best, 1)
print(json.dumps({"columns": cols, "rows": out, "samples": len(rows),
    "sat_run_s": {k: sat_run(k) for k in ("loadgen", "gateway", "logic")}, "first_s": {
    "loadgen_saturated": first(sat("loadgen_set_busy_pct")), "gateway_set_saturated": first(sat("gateway_set_busy_pct")),
    "logic_set_saturated": first(sat("logic_set_busy_pct")),
    "stats_unanswered": first(lambda r: r[c["lg_online"]] is None),
    "scrape_late": first(lambda r: r[c["scrape_ms"]] is None or r[c["scrape_ms"]] > 1500),
    "pending_mem": first(pos("pending_mem_per_s")), "no_slot": first(pos("no_slot_per_s")),
    "light_refused": first(pos("lg_refused_per_s")),
    "gave_up": first(pos("lg_gave_up")), "closes_heartbeat": first(pos("closes_heartbeat_per_s"))}}))' "$1" "$2" 2>/dev/null || echo null
}

# slow_verdict: the lg_py expression for the slow phase's verdict, on
# {"ramp", "hold": t2_cpusets_json of the light clients' ramp and of the
# hold, "sat": slow_series' sat_run_s}: the first of loadgen's, the
# gateways' (with slow-auth) and Logic's (with slow-handshake) sets >= 85%
# busy in either window, or >= 95% for 5s in a row before the light
# clients were online, as loadgen-saturated, gateway-set-saturated or
# logic-set-saturated; else valid; unread if a snapshot or the series is
# missing. Only valid lets slow_legit and
# slow_heartbeats pass (#126). The order names a set, not a cause: which
# came first is read in slow.series.first_s (6b3b3 review O3).
slow_verdict='"unread" if not (d["ramp"] and d["hold"] and d["sat"]) else next((k + "-saturated" if k == "loadgen" else k + "-set-saturated" for k in ("loadgen", "gateway", "logic") if any(100 - w[k]["idle_pct"] >= 85 for w in (d["ramp"], d["hold"])) or d["sat"][k] >= 5), "valid")'

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
  local n=${FLOODS_STORM_CLIENTS:-200000} rate=${FLOODS_STORM_RATE:-20000} s0 s1 d usage online hs rated
  STACK_GATEWAYS=1 STACK_GATEWAY_ARGS=(-max-conns $((n + n / 10)))
  up storm || { stack_down; return; }
  stack_snapshot before
  s0=$(t2_gws_counters "${GW_COUNTERS[@]}")
  t2_record_start storm
  t2_cpustat_snap "$STACK_DIR/cpustat-0"
  ct_stat_snap "$STACK_DIR/ctstat-0"
  lg_start storm -mode light -clients "$n" -rate "$rate" -uid-prefix storm
  stack_usage_start loadgen="$LG_PID"
  hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$LG_PID" &
  smp=$!
  # The gateway's and loadgen's CPU over 8s of the ramp, from 1s in: X25519's
  # share on both sides of the handshake (#130). pprof's few percent fall
  # inside the window storm_rate measures, as the garbage phase's do in its
  # window: runs before 6b3b4 had no storm profile (final review M3).
  (sleep 1; stack_profile storm 8 & lg_profile storm 8 & wait) >/dev/null 2>&1 &
  prof=$!
  online=$(t2_bool lg_wait "$(settled "$n")" $((n / rate * 3 + 60)))
  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
  usage=$(stack_usage_json)
  t2_cpustat_snap "$STACK_DIR/cpustat-1"
  ct_stat_snap "$STACK_DIR/ctstat-1"
  s1=$(t2_gws_counters "${GW_COUNTERS[@]}")
  wait "$prof"
  prof=""
  t2_record_end storm
  stack_snapshot after
  d=$(t2_gws_delta "$s0" "$s1")
  hs=$(hs_rate "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$n")
  [ "$online" = true ] && online=$(lg_get /stats "$(logged_in "$n")")
  [ "$online" = true ] || online=false # a null /stats reading fails the check, not the run
  t2_check storm_online "$online" "\"want\": $n, \"online\": $(lg_get /stats 'd["online"]'), \"gave_up\": $(lg_get /stats 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_get /stats 'd["failed"]'), \"login_latency\": $(lg_get /stats 'd["login_latency"]')"
  # The verdict: met, loadgen-bound (loadgen at >= 85% of its CPUs over the
  # ramp new_per_s reads) or short (a reading missing included). The CPUs
  # are the set lg_start pinned loadgen to.
  lgcpus=$(t2_cpulist_expand "${LG_CPUS:-$T2_CPUS_LOADGEN}" | wc -w)
  rated=$(echo "{\"h\": $hs, \"cpus\": $lgcpus}" | lg_py - "$(storm_verdict "$rate")")
  [ "$rated" = '"loadgen-bound"' ] && t2_not_covered "storm: $rate new clients/s at the gateway; loadgen was the bound (storm_rate.loadgen_cores_ramp)"
  t2_check storm_rate "$(echo "$rated" | lg_true - 'd in ("met", "loadgen-bound")')" \
    "\"verdict\": $rated, \"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("new_per_s", "samples", "seen_per_s", "rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores_ramp\": $(echo "$hs" | lg_py - 'd["loadgen_cores_ramp"]'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"loadgen_cpus\": $lgcpus, \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
  t2_check storm_bucket "$(echo "$d" | lg_true - 'd["gw-0"]["hs_no_token"] == 0')" \
    "\"gw-0\": $(echo "$d" | lg_py - '{k: d["gw-0"][k] for k in ("hs_ok", "hs_no_token", "hs_no_slot", "rejects_handshake_rate", "logins_busy", "rejects_pending_mem", "rejects_at_capacity")}'), \"no_slot_share\": $(echo "$d" | lg_py - 'round(d["gw-0"]["hs_no_slot"] / (d["gw-0"]["hs_ok"] + d["gw-0"]["hs_no_slot"]), 4)'), \"failed\": $(lg_get /stats 'd["failed"]')"
  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
  down storm "$n"
  for f in gw-0 loadgen; do
    gzip -t "$STACK_DIR/artifacts/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" storm:storm/$f.cpu.pb.gz"
  done
}

# garbage: #121 (1), garbage handshakes beside a Pinging population.
phase_garbage() {
  local n=${FLOODS_CLIENTS:-400000} rate=${FLOODS_RATE:-10000} grate=${FLOODS_GARBAGE_RATE:-80000}
  local inflight=${FLOODS_GARBAGE_INFLIGHT:-20000} secs=${FLOODS_GARBAGE_SECONDS:-90}
  local online on s0 s1 d l0 l1 tw0 tw1 usage want flood t0 lease_s f
  STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $((n / 2 + n / 10 + inflight)) -heartbeat "${HB}s"
    ${FLOODS_GARBAGE_BUCKET:+-handshake-rate "$FLOODS_GARBAGE_BUCKET"})
  up garbage || { stack_down; return; }
  lg_start garbage-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix garbage
  online=false
  lg_wait "$(settled "$n")" $((n / rate * 3 + 60)) &&
    online=$(lg_get /stats '('"$(logged_in "$n")"') and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$n"' * 0.45')
  on=$(lg_get /stats 'd["online"]')
  [ "$online" = true ] || online=false
  t2_check garbage_online "$online" "\"want\": $n, \"online\": $on, \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"gave_up\": $(lg_get /stats 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_get /stats 'd["failed"]')"
  [ "$online" = true ] || { down garbage $((n + inflight)); return; }
  stack_snapshot before
  s0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  t2_tcpstat_snap "$STACK_DIR/tcp0"
  t2_cpustat_snap "$STACK_DIR/cpustat-0"
  ct_stat_snap "$STACK_DIR/ctstat-0"
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
  s1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  lease_s=$((SECONDS - t0))
  t2_tcpstat_snap "$STACK_DIR/tcp1"
  t2_cpustat_snap "$STACK_DIR/cpustat-1"
  ct_stat_snap "$STACK_DIR/ctstat-1"
  tw1=$(timewait)
  lg_in flood lg_stop
  wait "$prof"
  prof=""
  t2_record_end garbage
  stack_snapshot after
  lg_stop
  d=$(t2_gws_delta "$s0" "$s1")
  flood=$(lg_py "$STACK_DIR/artifacts/loadgen-garbage.json" 'd["flood"]')
  want=$(python3 -c 'import sys; print(int(0.95 * int(sys.argv[1]) * int(sys.argv[2])))' "$grate" "$secs")
  t2_check garbage_delivered "$(echo "{\"g\": $d, \"f\": $flood}" | lg_true - 'd["f"]["attempts"] >= '"$want"' and sum(v["hs_failed"] + v["hs_no_slot"] + v["hs_no_token"] for v in d["g"].values()) >= '"$want")" \
    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1"), \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1")"
  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_slot"] + v["hs_no_token"] > 0 for v in d.values())')" \
    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"], "bound_by": "bucket" if v["hs_no_token"] > 0 else "slots" if v["hs_no_slot"] > 0 else None} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
  for g in $(echo "$d" | lg_py - "$slots_only" | tr -d '[]",'); do
    t2_not_covered "garbage: the bucket (${FLOODS_GARBAGE_BUCKET:-25000}/s) refused nothing on $g; the concurrency limit bound first (garbage_limited.dh_per_s)"
  done
  heartbeats garbage "$on" "$d"
  t2_lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d, \"flood\": $flood}"
  down garbage $((n + inflight))
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
  local s0 s1 d l0 l1 t0 lease_s flooding=true online on usage inst held f win series verdict refused refusals
  STACK_GATEWAYS=2 STACK_GATEWAY_ARGS=(-max-conns $(((n + hs + au) / 2 + (n + hs + au) / 10)) -heartbeat "${HB}s")
  up slow || { stack_down; return; }
  stack_snapshot before
  s0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  t2_record_start slow
  t2_cpustat_snap "$STACK_DIR/cpustat-0"
  ct_stat_snap "$STACK_DIR/ctstat-0"
  t0=$SECONDS
  # The floods off loadgen's CPU set, left to the light clients (#126):
  # slow-handshake on Logic's, slow-auth on the gateways', each with a
  # GOMAXPROCS of its own, not one per CPU of the set (#127, review O1, O2).
  LG_ENV+=(GOMAXPROCS=${FLOODS_SLOW_HS_PROCS:-2})
  LG_CPUS=$HS_CPUS lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
  LG_ENV[-1]=GOMAXPROCS=${FLOODS_SLOW_AUTH_PROCS:-4}
  LG_CPUS=$AUTH_CPUS lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
  unset 'LG_ENV[-1]'
  lg_in flood lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$hs"' and d["flood"]["open"] >= 0.85 * '"$hs" $((hs / srate + 60)) || flooding=false
  lg_in flood2 lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$au"' and d["flood"]["open"] >= 0.85 * '"$au" $((au / srate + 60)) || flooding=false
  t2_check slow_flood "$flooding" "\"slowhs\": $(lg_in flood lg_get /stats 'd["flood"]'), \"slowauth\": $(lg_in flood2 lg_get /stats 'd["flood"]')"
  [ "$flooding" = true ] || { t2_record_end slow; down slow $((n + hs + au)); return; }
  lg_start slow-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix slow
  stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}" flood2="${LG_PIDS[flood2]}"
  t2_cpustat_snap "$STACK_DIR/cpustat-light0"
  slow_sampler "$STACK_DIR/artifacts/slow-series.jsonl" "$LG_PID" "${LG_PIDS[flood]}" "${LG_PIDS[flood2]}" &
  smp=$!
  # On Logic's set (94.6% idle in run 30b, slow-handshake's ~1.3 CPUs
  # beside it now), off the gateways' and loadgen's.
  [ "${STACK_PIN:-0}" = 1 ] && taskset -apc "$T2_CPUS_LOGIC" "$smp" >/dev/null
  online=false
  lg_wait "$(settled "$n")" $((n / rate * 3 + 60)) && online=$(lg_get /stats "$(logged_in "$n")")
  on=$(lg_get /stats 'd["online"]')
  t2_cpustat_snap "$STACK_DIR/cpustat-light1"
  sleep "$hold"
  t2_cpustat_snap "$STACK_DIR/cpustat-light2"
  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
  usage=$(stack_usage_json)
  lg_in flood lg_stop
  lg_in flood2 lg_stop
  s1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
  lease_s=$((SECONDS - t0))
  t2_cpustat_snap "$STACK_DIR/cpustat-1"
  ct_stat_snap "$STACK_DIR/ctstat-1"
  t2_record_end slow
  stack_snapshot after
  lg_stop
  d=$(t2_gws_delta "$s0" "$s1")
  win="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-light0" "$STACK_DIR/cpustat-light1"), \"hold\": $(t2_cpusets_json "$STACK_DIR/cpustat-light1" "$STACK_DIR/cpustat-light2")}"
  series=$(slow_series "$STACK_DIR/artifacts/slow-series.jsonl" "$n")
  verdict=$(echo "{\"w\": $win, \"s\": $series}" | lg_py - '{"ramp": d["w"]["ramp"], "hold": d["w"]["hold"], "sat": (d["s"] or {}).get("sat_run_s")}' | lg_py - "$slow_verdict")
  [ "$verdict" = '"valid"' ] ||
    t2_not_covered "slow: legitimate logins and heartbeats beside the floods; verdict $verdict, ruled by the order in slow.series.first_s (slow.window, slow.series)"
  # Slow-auth's handshakes meet the limiter: a refused one ends pre-handshake,
  # with no hold, and the bounds are on the holds the timeouts ended (O1). A
  # close before 2.5s (slowHeld) is refused, with no hold, in both floods (#135).
  held=true
  for inst in slowhs:'{"closed", "refused"}' slowauth:'{"closed", "pre-handshake", "refused"}'; do
    [ "$(lg_true "$STACK_DIR/artifacts/loadgen-${inst%%:*}.json" 'set(d["flood"]["outcomes"]) <= '"${inst#*:}"' and "closed" in d["flood"]["outcomes"] and d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500')" = true ] || held=false
  done
  # Each refused close needs a gateway refusal to explain it (review O7).
  refused=$(echo "{\"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("refused", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("refused", 0)')}" | lg_py - 'd["h"] + d["a"]')
  refusals=$(echo "$d" | lg_py - 'sum(v[k] for v in d.values() for k in ("rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token"))')
  [ "$(echo "{\"r\": ${refused:-null}, \"g\": ${refusals:-null}}" | lg_true - 'd["r"] <= d["g"]')" = true ] || held=false
  t2_check slow_held "$held" "\"slowhs\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"slowauth\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" '{k: d["flood"][k] for k in ("attempts", "open", "outcomes", "held")}'), \"refused\": ${refused:-null}, \"gateway_refusals\": ${refusals:-null}"
  t2_check slow_freed "$(echo "{\"g\": $d, \"h\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"a\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')}" | lg_true - 'sum(v["rejects_handshake_timeout"] for v in d["g"].values()) >= d["h"] and sum(v["rejects_auth_timeout"] for v in d["g"].values()) >= d["a"]')" \
    "\"handshake_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_handshake_timeout"] for v in d.values())'), \"slowhs_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowhs.json" 'd["flood"]["outcomes"].get("closed", 0)'), \"auth_timeout\": $(echo "$d" | lg_py - 'sum(v["rejects_auth_timeout"] for v in d.values())'), \"slowauth_closed\": $(lg_py "$STACK_DIR/artifacts/loadgen-slowauth.json" 'd["flood"]["outcomes"].get("closed", 0)')"
  t2_check slow_legit "$(echo "{\"g\": $d, \"o\": ${online:-false}, \"v\": $verdict}" | lg_true - 'd["v"] == "valid" and d["o"] is True and all(v[k] == 0 for v in d["g"].values() for k in ("logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_token"))')" \
    "\"verdict\": $verdict, \"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
  heartbeats slow "$on" "$d" "$verdict"
  t2_lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"flood_cpus\": {\"slowhs\": $(t2_json_str "$HS_CPUS"), \"slowauth\": $(t2_json_str "$AUTH_CPUS")}, \"flood_procs\": {\"slowhs\": ${FLOODS_SLOW_HS_PROCS:-2}, \"slowauth\": ${FLOODS_SLOW_AUTH_PROCS:-4}}, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"verdict\": $verdict, \"window\": $win, \"series\": $series, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
  down slow $((n + hs + au))
  for f in before/gw-0 after/gw-0; do
    grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
  done
  [ -s "$STACK_DIR/artifacts/slow-series.jsonl" ] || missing+=" slow:slow-series.jsonl"
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
