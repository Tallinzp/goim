#!/usr/bin/env bash
# Tier 2: reconnect (Plan 6b3c), the spec's load test "whole-population
# reconnect" (R5S-O6): drop every connection on all gateways at once and
# measure, with no SLO, the time until 99% are logged in again and the
# share of clients that reach backoff; the heartbeats of clients already
# back are answered on time and no component restarts. The spec names
# three causes: an LB reset or failover, and a carrier's or a region's
# network that recovers. Each phase on a fresh stack:
#   reset: 600k SDK clients over 3 gateways (as gwloss b_stress runs them:
#       -mode sdk -precompute, #134), then POST /reset: loadgen marks every
#       gateway and closes every connection with an RST in one instant
#       (P6a2bI-2). The clients log in again after the SDK's 0-20s storm
#       wait, its pre-handshake retries and backoff.
#   netchange: the same, then POST /network-change once every client has
#       lost its connection: each cuts its storm wait to the local
#       trigger's 0-1s (spec: "the same with every client raising a
#       network-change event at once").
#   ghost: the network recovery (review O1, #136): POST /ghost, so every
#       client loses its connection while loadgen keeps each socket open
#       and unread, and the gateways see nothing: each still holds its
#       ~200k old connections ONLINE until DeadAfter (2 x heartbeat + 10s).
#       Then POST /network-change, as in netchange. The relogins meet the
#       cap (the old connections plus the new against 250k), each one
#       admitted replaces a live session (Logic's Kick closes the old
#       connection), and the sweep closes the old connections that no Kick
#       reached. The hold lasts until DeadAfter after the mark plus a wheel
#       tick, so no old connection is left at the stop. Loadgen holds up to
#       1.2M sockets (the old and the new): nofile 2,097,152 (#136.5).
# The gateways run the spec's cap (250k, #136) and a 30s heartbeat (#136),
# so the clients back send Pings during the storm. A per-second series
# per gateway records pending sockets, pending_mem refusals, open
# connections, handshakes in progress, Busy, the login queue and the
# Logouts awaiting, the closes by Kick and by heartbeat, beside loadgen's
# balancer counts, its kept sockets (ghost) and its run-queue wait, and a
# funnel reading (one gateway refusing at pending_mem while another, under
# its cap, has no handshake in progress) is recorded, not gated (#133). The
# funnel is evidence for parked 6b3b5 and #111 only at or over its bar,
# RECONNECT_FUNNEL_BAR_S in a row (review O6). Redis's per-command latency
# from the mark to the hold's end is recorded per phase (review O5), the
# single-instance baseline for Plan 6c. CPU sets as
# b_stress's (#108), 4 GOMAXPROCS per gateway: 2 handshake slots each, as
# #133 sized it. Logic is one process (minReplicas, no HPA).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=reconnect
#   Results: tier2-results/reconnect-<timestamp>/ (result.json, one
#   directory per phase with logs, artifacts and loadgen results), plus ONE
#   file to send back: tier2-results/reconnect-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 30 minutes (each phase ~10: the ramp 120s, the storm up to
#   RECONNECT_BACK_WAIT, the hold, the stop).
#
# Settings (every number whole and positive, refused otherwise before
# anything starts):
#   RECONNECT_PHASES      phases to run, in order (default "reset netchange ghost")
#   RECONNECT_CLIENTS     SDK clients over the 3 gateways (default 600000)
#   RECONNECT_RATE        clients started per second (default 5000)
#   RECONNECT_HEARTBEAT   the gateways' -heartbeat, seconds (default 30, the
#                         SDK's lowest: most Pings in the storm; 30-300)
#   RECONNECT_CAP         each gateway's -max-conns (default: the gateway's
#                         250,000, the spec's cap)
#   RECONNECT_DROP_MAX_S  every client must notice the drop within this
#                         many seconds of the mark (default 10)
#   RECONNECT_BACK_WAIT   seconds after the mark to wait for every client to
#                         log in again (default 900: as b_stress, the
#                         SDK's backoff doubles to 5 min)
#   RECONNECT_HOLD        seconds held once every client is back (default
#                         the heartbeat + 15: each sends a Ping and gets its
#                         Pong, or its 10s Pong timeout); ghost holds at
#                         least until DeadAfter + 5s after the mark
#   RECONNECT_FUNNEL_BAR_S  the funnel's bar, seconds in a row (default 5)
#   RECONNECT_REDIS       single (default, run 31's) or cluster: 3 masters and
#                         3 replicas on the Logic set (stack.sh STACK_REDIS,
#                         Plan 6c1); P.redis_latency is then per node
#   RECONNECT_LG_MEMLIMIT loadgen's GOMEMLIMIT (default 16GiB, as b_stress)
#   RECONNECT_CPUS_LOADGEN, RECONNECT_CPUS_GATEWAY, RECONNECT_CPUS_LOGIC
#                         the CPU sets (default b_stress's: 0-5,10,12-17,22;
#                         6-9,18-21; 11,23)
#   RECONNECT_GATEWAY_PROCS  each gateway's GOMAXPROCS (default 4)
#
# Checks (per phase P):
#   P_stack        redis, devtoken, Logic and the 3 gateways become Ready
#   P_online       every client logs in, each gateway holds >= 30% of them
#   P_pinning      the gateways, Logic + Redis and loadgen on their CPU sets;
#                  a failure ends the run
#   P_drop         POST /reset (ghost: /ghost) marked every client
#                  (population = clients), every one noticed the loss
#                  within RECONNECT_DROP_MAX_S, and the gateways counted a
#                  client close for each; for netchange and ghost also POST
#                  /network-change reached every client. ghost instead of
#                  the client closes: by the hold's end the gateways closed
#                  every old connection (loadgen saw each close: by Kick or
#                  silent, none left open; the gateways' kick + heartbeat
#                  closes >= population). The reset's close time and the
#                  mark-to-network-change time are recorded
#   P_back         every client logged in again within RECONNECT_BACK_WAIT;
#                  no SLO: the time until 99% are back (slo.from_mark p99),
#                  the share that reached backoff, the waits since the mark
#                  by kind, the rejects by reason (ghost: at_capacity, the
#                  cap shapes it), Logic's Kicks, and whether loadgen was
#                  the bound (>= 3/4 of its CPUs over the storm's first
#                  30s, #102) are recorded
#   P_heartbeats   from the mark to the hold's end every connection that
#                  ended was the reset's (lost "lost" = population, no
#                  Pong timeout, no other reason) and no gateway closed one
#                  for its heartbeat: the clients back were answered on
#                  time while the others logged in (R5S-O6). ghost: a
#                  gateway's heartbeat closes are allowed up to the old
#                  connections loadgen saw closed with no Kick on it. A
#                  failure while loadgen was the bound is labelled (a
#                  not_covered line): its reads may have been late
#                  (lg_runq_ms_per_s in the series, review O3)
#   P_sessions     after the hold, Logic's store holds one session per
#                  client, logged in after the mark, on the gateway that
#                  holds the client's connection (loadgen's GET /clients
#                  against every sess key; review O4). Logic's replaced
#                  logins and deleted or not_owner Logouts beside it
#   P_lease        every gateway renewed its lease every interval from the
#                  mark to the hold's end, and Logic saw no renewal error
#   no_swap_in_P   no page swapped in from the mark to the hold's end
#   P_idle         after loadgen stops, every gateway reaches 0 connections
#                  and settles its Logouts (dropped ones allowed, counted:
#                  the logout queue is the cap's size, and every gateway
#                  queues ~200k at once twice)
#                  (P_* per phase: 11 checks each)
#   P_processes    nothing died, panicked or stopped uncleanly ("no
#                  component restarts")
#   artifacts      (once) every phase's metrics before and after, the
#                  storm's CPU profiles (gateways, Logic, loadgen), the
#                  series, the SLO and loadgen's result
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init reconnect
t2_need curl taskset sudo python3
t2_require_host_setup

PHASES=${RECONNECT_PHASES:-reset netchange ghost}
named=" "
for p in $PHASES; do
  case $p in reset | netchange | ghost) ;; *) echo "reconnect: unknown phase '$p' (reset, netchange, ghost)" >&2; exit 1 ;; esac
  case $named in *" $p "*) echo "reconnect: phase '$p' named twice in RECONNECT_PHASES" >&2; exit 1 ;; esac
  named+="$p "
done
for v in RECONNECT_CLIENTS RECONNECT_RATE RECONNECT_HEARTBEAT RECONNECT_CAP RECONNECT_DROP_MAX_S \
  RECONNECT_BACK_WAIT RECONNECT_HOLD RECONNECT_GATEWAY_PROCS RECONNECT_FUNNEL_BAR_S; do
  case ${!v-1} in ''|*[!0-9]*|0*) echo "reconnect: $v must be a positive whole number" >&2; exit 1 ;; esac
done
case ${RECONNECT_REDIS:-single} in single | cluster) ;; *) echo "reconnect: RECONNECT_REDIS must be single or cluster" >&2; exit 1 ;; esac
HB=${RECONNECT_HEARTBEAT:-30}
[ "$HB" -ge 30 ] && [ "$HB" -le 300 ] || { echo "reconnect: RECONNECT_HEARTBEAT must be 30 to 300 (the gateway's -heartbeat)" >&2; exit 1; }
N=${RECONNECT_CLIENTS:-600000} RATE=${RECONNECT_RATE:-5000}
DROP_MAX=${RECONNECT_DROP_MAX_S:-10} BACK_WAIT=${RECONNECT_BACK_WAIT:-900} HOLD=${RECONNECT_HOLD:-$((HB + 15))}
BAR=${RECONNECT_FUNNEL_BAR_S:-5} DEAD=$((2 * HB + 10))
# As gwloss b_stress (#93, #105): 600k SDK clients' loadgen peaks at ~16GB
# under its 16GiB GOMEMLIMIT, beside 3 gateways, Redis and the kernel's
# socket memory. ghost adds up to ~150k socket pairs (the cap holds the old
# connections plus the new at 750k on the gateways) and the dials in flight,
# ~8.5KB of kernel memory per loopback pair (measured on dev): ~2-4GB more,
# which 23 cannot hold on the 26GB host, so the swap check is its verdict.
t2_mem_budget 23
# ghost: each client holds its old socket and a new one: up to 1.2M of
# loadgen's fds, over the host's former 1,048,576 (#136.5, host-setup.sh).
case " $PHASES " in *" ghost "*) t2_nofile 2097152 ;; *) t2_nofile 1048576 ;; esac

export STACK_PIN=1 STACK_GATEWAYS=3 STACK_REDIS=${RECONNECT_REDIS:-single}
export T2_CPUS_GATEWAY=${RECONNECT_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${RECONNECT_CPUS_LOADGEN:-0-5,10,12-17,22}
export T2_CPUS_LOGIC=${RECONNECT_CPUS_LOGIC:-11,23}
export STACK_GATEWAY_PROCS=${RECONNECT_GATEWAY_PROCS:-4}
LG_NCPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
LG_ENV=(GOMEMLIMIT="${RECONNECT_LG_MEMLIMIT:-16GiB}")
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
STACK_GATEWAY_ARGS=(-heartbeat "${HB}s" ${RECONNECT_CAP:+-max-conns "$RECONNECT_CAP"})
CAP=${RECONNECT_CAP:-250000}
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack
smp="" prof=""
t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
t2_on_exit '[ -n "$prof" ] && t2_kill_tree "$prof"'

GW_COUNTERS=(
  closes_client='goim_gateway_closes_total:reason="client"'
  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
  closes_kick='goim_gateway_closes_total:reason="kick"'
  closes_not_owner='goim_gateway_closes_total:reason="not_owner"'
  hs_ok='goim_gateway_handshakes_total:result="ok"'
  hs_no_slot='goim_gateway_handshakes_total:result="no_slot"'
  hs_no_token='goim_gateway_handshakes_total:result="no_token"'
  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  rejects_pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
  rejects_handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"'
  rejects_auth_timeout='goim_gateway_admission_rejects_total:reason="auth_timeout"'
  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  logins_busy='goim_gateway_logins_total:result="busy"'
  logouts_done='goim_gateway_logouts_total:result="done"'
  logouts_dropped='goim_gateway_logouts_total:result="dropped"'
  logouts_retried='goim_gateway_logouts_total:result="retried"'
  lease_live='goim_gateway_lease_calls_total:result="live"'
  lease_missing='goim_gateway_lease_calls_total:result="missing"'
  lease_error='goim_gateway_lease_calls_total:result="error"'
)
LOGIC_COUNTERS=(
  lease_error='goim_logic_lease_renewals_total:result="error"'
  logins_ok='goim_logic_logins_total:result="ok"'
  logins_replaced='goim_logic_logins_total:result="replaced"'
  logouts_deleted='goim_logic_logouts_total:result="deleted"'
  logouts_unknown='goim_logic_logouts_total:result="unknown"'
  logouts_not_owner='goim_logic_logouts_total:result="not_owner"'
  kick_attempts=goim_logic_kick_attempts_total
  kicks_ok='goim_logic_kick_attempts_total:result="ok"'
  kick_giveups=goim_logic_kick_giveups_total
  kick_dropped=goim_logic_kick_dropped_total
)

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# rc_sampler FILE: appends one JSON line a second to FILE until killed
# (#133): {"ms", "sets": {"loadgen", "gateway", "logic": [busy, total]},
# "rq": loadgen's run-queue wait, ns (null if gone; review O3), "lg":
# {"online", "open": the balancer's per gateway, "ghosts": the sockets
# /ghost keeps} (null when /stats does not answer within 1s), "gw":
# {gateway: SERIES_COUNTERS, null if its /metrics does not answer within
# 1s}, "ms_end": the clock after the gateways' scrapes}.
SERIES_COUNTERS=(
  handshake='goim_gateway_connections:state="handshake"'
  auth_pending='goim_gateway_connections:state="auth_pending"'
  online='goim_gateway_connections:state="online"'
  pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
  at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
  no_slot='goim_gateway_handshakes_total:result="no_slot"'
  logins_ok='goim_gateway_logins_total:result="ok"'
  busy='goim_gateway_logins_total:result="busy"'
  login_queue=goim_gateway_login_queue
  awaiting_logout=goim_gateway_awaiting_logout
  logouts_done='goim_gateway_logouts_total:result="done"'
  logouts_dropped='goim_gateway_logouts_total:result="dropped"'
  closes_kick='goim_gateway_closes_total:reason="kick"'
  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
)
rc_sampler() {
  local ms ms_end sets lg gw g t rq
  while :; do
    ms=$(date +%s%3N)
    rq=$(t2_runq_ns "$LG_PID")
    sets="\"loadgen\": [$(t2_set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(t2_set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(t2_set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
    gw=""
    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
      t=$(STACK_CURL_TIMEOUT=1 stack_counters "$g" "${SERIES_COUNTERS[@]}")
      gw+="${gw:+, }\"$g\": ${t:-null}"
    done
    ms_end=$(date +%s%3N)
    lg=$(LG_CURL_TIMEOUT=1 lg_get /stats '{"online": d["online"], "open": d["balancer_open"], "ghosts": d["ghosts_open"]}')
    echo "{\"ms\": $ms, \"sets\": {$sets}, \"rq\": ${rq:-null}, \"lg\": ${lg:-null}, \"gw\": {$gw}, \"ms_end\": $ms_end}" >>"$1"
    sleep 1
  done
}

# rc_series FILE MARK_MS CAP BAR: rc_sampler's lines as {"columns",
# "gw_columns", "rows": [[t_s since the mark, scrape_ms, lg_online,
# lg_ghosts, lg_runq_ms_per_s, the three sets' busy %, {gateway:
# [gw_columns]}], ...], "funnel", "samples"}; null if unreadable. A sample
# whose gateway scrapes trailed its clock by over 1.5s, or missed a
# gateway, has no gateway readings, and the next on-time sample's rates are
# taken from the last on-time one (as floods' slow_series).
# lg_runq_ms_per_s is null for a failed reading or a total that went down
# (6b3b3 M6). The funnel (#131, #133): the rows where a gateway refused at
# pending_mem while another, under CAP open connections, had no socket in
# HANDSHAKE; "seconds" (their intervals summed), "first_s", "longest_s" (in
# a row), "over_bar" (longest_s >= BAR: only then is it the evidence for
# 6b3b5 and #111, review O6) and the first 20 as [t_s, refusing, idle].
# Recorded, not gated.
rc_series() {
  python3 -c 'import json, sys
rows = []
for l in open(sys.argv[1]):
    try:
        rows.append(json.loads(l))
    except ValueError:
        pass
mark, cap, bar = int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
cols = ["t_s", "scrape_ms", "lg_online", "lg_ghosts", "lg_runq_ms_per_s", "loadgen_set_busy_pct", "gateway_set_busy_pct",
        "logic_set_busy_pct", "gw"]
gcols = ["pending", "handshake", "open", "lb_open", "pending_mem_per_s", "at_capacity_per_s", "no_slot_per_s",
         "logins_ok_per_s", "busy_per_s", "login_queue", "awaiting_logout", "logouts_done_per_s", "logouts_dropped_per_s",
         "closes_kick_per_s", "closes_heartbeat_per_s"]
rates = ("pending_mem", "at_capacity", "no_slot", "logins_ok", "busy", "logouts_done", "logouts_dropped", "closes_kick",
         "closes_heartbeat")
def busy(a, b, k):
    x, y = a["sets"].get(k, []), b["sets"].get(k, [])
    return round(100 * (y[0] - x[0]) / (y[1] - x[1]), 1) if len(x) == 2 and len(y) == 2 and y[1] > x[1] else None
def on_time(b):
    return b.get("ms_end") is not None and b["ms_end"] - b["ms"] <= 1500 and len(b["gw"]) > 0 and all(b["gw"].values())
def rate(p, b, g, k):
    if p is None or g not in p["gw"] or b["gw"][g].get(k) is None:
        return None
    return round((b["gw"][g][k] - (p["gw"][g].get(k) or 0)) / ((b["ms"] - p["ms"]) / 1000), 1)
def gauge(v, k):
    return v.get(k)
def runq(a, b):
    x, y = a.get("rq"), b.get("rq")
    return round((y - x) / 1e6 / ((b["ms"] - a["ms"]) / 1000), 1) if x is not None and y is not None and y >= x else None
out, ids = [], sorted({g for r in rows for g in r["gw"]})
last = rows[0] if rows and on_time(rows[0]) else None
for a, b in zip(rows, rows[1:]):
    if b["ms"] <= a["ms"]:
        continue
    lg = b["lg"]
    gw = None
    if on_time(b):
        gw = {}
        for g, v in sorted(b["gw"].items()):
            h, ap, on = gauge(v, "handshake"), gauge(v, "auth_pending"), gauge(v, "online")
            pend = None if h is None or ap is None else h + ap
            gw[g] = [pend, h, None if pend is None or on is None else pend + on, (lg or {}).get("open", {}).get(g) if lg else None] + \
                [rate(last, b, g, k) for k in rates[:5]] + [gauge(v, "login_queue"), gauge(v, "awaiting_logout")] + [rate(last, b, g, k) for k in rates[5:]]
        last = b
    out.append([round((b["ms"] - mark) / 1000, 1), b["ms_end"] - b["ms"] if b.get("ms_end") is not None else None,
                lg and lg["online"], lg and lg.get("ghosts"), runq(a, b), busy(a, b, "loadgen"), busy(a, b, "gateway"),
                busy(a, b, "logic"), gw])
gi = {k: i for i, k in enumerate(gcols)}
def funnel(r):
    gw = r[-1]
    if not gw:
        return None
    refusing = [g for g, v in gw.items() if (v[gi["pending_mem_per_s"]] or 0) > 0]
    idle = [g for g, v in gw.items() if v[gi["handshake"]] == 0 and v[gi["open"]] is not None and v[gi["open"]] < cap]
    pair = next(([a, b] for a in refusing for b in idle if a != b), None)
    return pair
hits, longest, run, prev = [], 0.0, 0.0, None
total = 0.0
for r in out:
    dt = r[0] - prev if prev is not None else 0.0
    prev = r[0]
    p = funnel(r)
    if p:
        hits.append([r[0]] + p)
        total += dt
        run += dt
        longest = max(longest, run)
    else:
        run = 0.0
print(json.dumps({"columns": cols, "gw_columns": gcols, "gateways": ids, "rows": out, "samples": len(rows),
    "funnel": {"seconds": round(total, 1), "first_s": hits[0][0] if hits else None, "longest_s": round(longest, 1),
               "bar_s": bar, "over_bar": round(longest, 1) >= bar, "rows": hits[:20]}}))' "$1" "$2" "$3" "$4" 2>/dev/null || echo null
}

# rc_heartbeats_ok LOST G POP GH PHASE: true if every connection that ended
# after the mark was the drop's (LOST = {"lost": POP}) and no gateway
# closed more by heartbeat than it may. In ghost a gateway's heartbeat
# closes may be its old connections that no Kick reached: its ghosts (GH's
# kick + silent) minus its own Kick and not_owner closes (Reconcile Kicks
# an old connection whose session moved, run 31). Loadgen's kick/silent
# label is not used: a Pong unread at the ghost instant labels a swept
# ghost kick.
rc_heartbeats_ok() {
  local allowed='0'
  [ "$5" = ghost ] && allowed='sum(((d["gh"] or {}).get("closed") or {}).get(g, {}).values()) - v["closes_kick"] - (v.get("closes_not_owner") or 0)'
  echo "{\"l\": ${1:-null}, \"g\": ${2:-null}, \"p\": ${3:-null}, \"gh\": ${4:-null}}" |
    lg_true - 'd["l"] == {"lost": d["p"]} and all(v["closes_heartbeat"] <= '"$allowed"' for g, v in d["g"].items())'
}

# rc_sessions_cmp CLIENTS SESSIONS MARK_MS: P_sessions' reading (review
# O4). CLIENTS is loadgen's GET /clients; SESSIONS one line per client, in
# order: "N|VALUE", N the sess hash's fields, VALUE the first ("gw|epoch|
# conn_id|device|conn_seq|login_ms|tok_iat"). {"clients", "offline": not
# logged in at loadgen, "found": one session on the client's gateway with
# login_ms >= MARK_MS, "missing", "extra": more than one, "wrong_gateway",
# "stale": logged in before the mark, "examples": up to 5 [uid, why]}; null
# if a file is unreadable or SESSIONS has not exactly one line per client.
rc_sessions_cmp() {
  python3 -c 'import json, sys
c = json.load(open(sys.argv[1]))
lines = open(sys.argv[2]).read().splitlines()
mark = int(sys.argv[3])
gw = c["gw"]
if len(lines) != len(gw):
    raise SystemExit(1)
r = {"clients": len(gw), "offline": 0, "found": 0, "missing": 0, "extra": 0, "wrong_gateway": 0, "stale": 0, "examples": []}
for i, (g, l) in enumerate(zip(gw, lines)):
    n, _, v = l.partition("|")
    why = None
    if g < 0:
        why = "offline"
    elif int(n) == 0:
        why = "missing"
    elif int(n) > 1:
        why = "extra"
    else:
        f = v.split("|")
        why = "wrong_gateway" if f[0] != c["gateways"][g] else "stale" if int(f[5]) < mark else None
    if why:
        r[why] += 1
        if len(r["examples"]) < 5:
            r["examples"].append(["%s-%d" % (c["uid_prefix"], i), why])
    else:
        r["found"] += 1
if len(sys.argv) > 4:
    r["ms"] = int(sys.argv[4])
print(json.dumps(r))' "$@" 2>/dev/null || echo null
}

# rc_sessions_cmds CLIENTS SLOTS DIR: the session check's commands. Each
# client's sess key goes to the master that owns its slot (SLOTS:
# stack_redis_slots's "START END NAME" lines), in one EVAL per slot (HVALS
# each; ~37 keys at 600k): a Cluster refuses a script whose keys span slots
# (CROSSSLOT, review P6b3c2I-3). Writes DIR/NAME.cmd and DIR/NAME.idx (the
# client index of each output line, in order) per master; fails if a slot
# has no owner.
rc_sessions_cmds() {
  python3 -c 'import json, sys
c = json.load(open(sys.argv[1]))
ranges = [(int(a), int(b), n) for a, b, n in (l.split() for l in open(sys.argv[2]) if l.strip())]
out = sys.argv[3]
def crc16(b):  # CRC16-XMODEM, Redis Cluster key slots
    r = 0
    for x in b:
        r ^= x << 8
        for _ in range(8):
            r = ((r << 1) ^ 0x1021 if r & 0x8000 else r << 1) & 0xFFFF
    return r
lua = "local r = {} for i, k in ipairs(KEYS) do local v = redis.call(\"HVALS\", k) r[i] = #v .. \"|\" .. (v[1] or \"\") end return r"
slots = {}
for i in range(len(c["gw"])):
    uid = "%s-%d" % (c["uid_prefix"], i)
    slots.setdefault(crc16(uid.encode()) % 16384, []).append(i)
files = {}
for slot in sorted(slots):
    name = next((n for a, b, n in ranges if a <= slot <= b), None)
    if name is None:
        raise SystemExit("slot %d has no owner" % slot)
    if name not in files:
        files[name] = (open("%s/%s.cmd" % (out, name), "w"), open("%s/%s.idx" % (out, name), "w"))
    cmd, idx = files[name]
    b = slots[slot]
    cmd.write("EVAL %s %d %s\n" % (json.dumps(lua), len(b), " ".join("sess:{%s-%d}" % (c["uid_prefix"], i) for i in b)))
    idx.write("".join("%d\n" % i for i in b))' "$@"
}

# rc_sessions_join CLIENTS DIR: the replies (DIR/NAME.out, one line per key)
# put back in client order, the SESSIONS lines rc_sessions_cmp reads. Fails
# if a reply's line count is not its NAME.idx's or a client has no line.
rc_sessions_join() {
  python3 -c 'import glob, json, sys
n = len(json.load(open(sys.argv[1]))["gw"])
lines = [None] * n
for f in glob.glob(sys.argv[2] + "/*.idx"):
    idx = [int(x) for x in open(f).read().split()]
    out = open(f[:-4] + ".out").read().splitlines()
    if len(out) != len(idx):
        raise SystemExit(1)
    for i, l in zip(idx, out):
        lines[i] = l
if None in lines:
    raise SystemExit(1)
print("\n".join(lines))' "$@"
}

# rc_sessions MARK_MS: rc_sessions_cmp over loadgen's /clients and every
# client's sess key, read per slot on each key's master (rc_sessions_cmds):
# at 600k ~16k short scripts after the hold, on one Redis or a Cluster alike.
rc_sessions() {
  local a=$STACK_DIR/artifacts d=$STACK_DIR/sessions f name t0
  t0=$(date +%s%3N)
  curl -sf -m 60 "http://$(lg_control)/clients" >"$a/clients.json" || { echo null; return; }
  rm -rf "$d" && mkdir -p "$d"
  stack_redis_slots >"$d/slots.txt"
  rc_sessions_cmds "$a/clients.json" "$d/slots.txt" "$d" 2>"$a/sessions.err" || { echo null; return; }
  for f in "$d"/*.cmd; do
    name=$(basename "$f" .cmd)
    stack_redis_node_cli "$name" --raw <"$f" >"$d/$name.out" 2>&1
  done
  rc_sessions_join "$a/clients.json" "$d" >"$a/sessions.txt" 2>>"$a/sessions.err" || { echo null; return; }
  rc_sessions_cmp "$a/clients.json" "$a/sessions.txt" "$1" $(($(date +%s%3N) - t0))
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))
missing=""

for name in $PHASES; do
  t0=$SECONDS
  t2_up "$name" || { stack_down; t2_duration "$name" $((SECONDS - t0)); continue; }

  # The ramp: as b_stress's, over every gateway.
  t2_cpustat_snap "$STACK_DIR/cpustat-0"
  lg_start "$name" -mode sdk -precompute -clients "$N" -rate "$RATE" -uid-prefix "$name"
  stack_usage_start loadgen="$LG_PID"
  t=$SECONDS
  online=false
  lg_wait_online "$N" $((N / RATE * 3 + 60)) &&
    online=$(lg_get /stats 'd["online"] == '"$N"' and min(d["online_by_gateway"].values()) >= '"$N"' * 0.3')
  [ "$online" = true ] || online=false # null: /stats unreadable
  ramp_s=$((SECONDS - t))
  ramp=$(stack_usage_json)
  t2_cpustat_snap "$STACK_DIR/cpustat-1"
  t2_check "${name}_online" "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
  wrong=""
  for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
  for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
  have=$(taskset -pc "$LG_PID" 2>/dev/null)
  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
  t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
  if [ -n "$wrong" ]; then
    lg_stop
    stack_down
    t2_duration "$name" $((SECONDS - t0))
    t2_finish
  fi
  if [ "$online" != true ]; then
    lg_stop
    stack_down
    t2_duration "$name" $((SECONDS - t0))
    continue
  fi
  # #106: every client holds its next handshake frame at the mark, as in
  # b_stress, so the first attempts are the spec's, not loadgen's X25519.
  t=$SECONDS
  until [ "$(lg_get /stats '(d.get("precompute") or {}).get("pending") == 0 and (d.get("precompute") or {}).get("ready") == d["online"]')" = true ]; do
    [ $((SECONDS - t)) -lt 120 ] || break
    sleep 1
  done
  precompute=$(lg_get /stats '{"pending": (d.get("precompute") or {}).get("pending"), "ready": (d.get("precompute") or {}).get("ready"), "online": d["online"], "wait_s": '"$((SECONDS - t))"'}')

  # The storm: counters before, the reset (ghost: /ghost), then the series
  # and profiles.
  mark=reset
  [ "$name" = ghost ] && mark=ghost
  stack_snapshot before
  g0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  lost0=$(lg_get /stats 'd["lost"]')
  t2_record_start "$name"
  t2_cpustat_snap "$STACK_DIR/cpustat-2"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
  stack_usage_start loadgen="$LG_PID"
  redis_reset=true; stack_redis_resetstat || redis_reset=false
  mark_ms=$(date +%s%3N)
  rc_sampler "$STACK_DIR/artifacts/series.jsonl" &
  smp=$!
  # On loadgen's 14 CPUs, not Logic's 2: Logic and Redis share those, the
  # set most likely to bound a login storm, and Review Focus 4 reads its
  # busy % (review O8).
  taskset -apc "$T2_CPUS_LOADGEN" "$smp" >/dev/null
  (stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
  prof=$!
  reply=$(lg_post "/$mark?mark=$mark" | lg_py - d)
  mark_t=$SECONDS
  # Every client notices the RST (ghost: its failed read) at once; until
  # then a network change would find it online and do nothing. Each /slo
  # walks every member under the recorder's lock: polled every 0.5s.
  dropped=false
  until [ "$(lg_get "/slo?mark=$mark&within=30s" 'd["lost"] == d["population"]')" = true ]; do
    [ $((SECONDS - mark_t)) -lt "$DROP_MAX" ] || break
    sleep 0.5
  done
  [ "$(lg_get "/slo?mark=$mark&within=30s" 'd["lost"] == d["population"]')" = true ] && dropped=true
  nc=null nc_after_ms=null
  if [ "$name" != reset ]; then
    nc_after_ms=$(($(date +%s%3N) - mark_ms))
    nc=$(lg_post /network-change | lg_py - d)
  fi
  back=$(t2_bool lg_wait_back "$mark" "$BACK_WAIT")
  back_s=$((SECONDS - mark_t))
  sleep "$HOLD"
  # ghost: every old connection is gone by DeadAfter after the mark (its
  # last Ping came before it), one wheel tick, and loadgen's closes.
  left=$((mark_t + DEAD + 5 - SECONDS))
  [ "$name" = ghost ] && [ "$left" -gt 0 ] && sleep "$left"
  redis=$(stack_redis_latency)
  gh=$(lg_get /stats '{"open": d["ghosts_open"], "closed": d["ghosts_closed"], "unkept": d["ghosts_unkept"]}')
  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
  wait "$prof"; prof=""
  storm=$(stack_usage_json)
  storm30=$(cat "$STACK_DIR/storm30.json" 2>/dev/null) || storm30=null
  g1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
  lost1=$(lg_get /stats 'd["lost"]')
  window_s=$((SECONDS - mark_t))
  t2_cpustat_snap "$STACK_DIR/cpustat-3"
  t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
  t2_record_end "$name"
  stack_snapshot after
  lg_get "/slo?mark=$mark&within=30s" d >"$STACK_DIR/artifacts/slo-$mark.json"
  slo=$STACK_DIR/artifacts/slo-$mark.json
  d=$(t2_gws_delta "$g0" "$g1")
  ld=$(stack_counters_delta "$l0" "$l1")
  st=$(lg_get /stats '{"backoff": d["backoff_since_mark"], "storm": d["storm_since_mark"], "login_latency": d["login_latency"], "failed": d["failed"]}')
  series=$(rc_series "$STACK_DIR/artifacts/series.jsonl" "$mark_ms" "$CAP" "$BAR")
  loadgen_bound=$(echo "$storm30" | lg_true - 'd["loadgen"]["cores"] >= '"$LG_NCPUS"' * 0.75')

  pop=$(lg_py "$slo" 'd["population"]')
  rule='d["r"]["population"] == '"$N"' and d["s"]["lost"] == d["s"]["population"] and 0 <= d["s"]["detection"]["max_ms"] <= '"$DROP_MAX"' * 1000'
  if [ "$name" = ghost ]; then
    # The gateways closed every old connection: loadgen saw each close,
    # none is left open, and the gateways counted them (Kick, Reconcile's
  # not_owner Kick, or heartbeat).
    rule+=' and d["gh"]["open"] == 0 and sum(v["kick"] + v["silent"] for v in d["gh"]["closed"].values()) == d["s"]["population"] and sum(v["closes_kick"] + (v.get("closes_not_owner") or 0) + v["closes_heartbeat"] for v in d["g"].values()) >= d["s"]["population"]'
  else
    rule+=' and sum(v["closes_client"] for v in d["g"].values()) >= d["s"]["population"]'
  fi
  [ "$name" != reset ] && rule+=' and d["nc"] is not None and d["nc"]["clients"] == '"$N"
  t2_check "${name}_drop" "$([ "$dropped" = true ] && echo "{\"r\": ${reply:-null}, \"s\": $(cat "$slo"), \"g\": $d, \"nc\": $nc, \"gh\": ${gh:-null}}" | lg_true - "$rule" || echo false)" \
    "\"$mark\": ${reply:-null}, \"lost\": $(lg_py "$slo" 'd["lost"]'), \"detection\": $(lg_py "$slo" 'd["detection"]'), \"max_s\": $DROP_MAX, \"closes\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("closes_client", "closes_kick", "closes_not_owner", "closes_heartbeat")} for g, v in d.items()}'), \"ghosts\": ${gh:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms"
  t2_check "${name}_back" "$([ "$back" = true ] && lg_true "$slo" 'd["back"] == d["population"] == '"$N" || echo false)" \
    "\"back_s\": $back_s, \"wait_s\": $BACK_WAIT, \"from_mark\": $(lg_py "$slo" 'd["from_mark"]'), \"backoff\": $(echo "$st" | lg_py - 'd["backoff"]'), \"backoff_share\": $(echo "$st" | lg_py - 'round(d["backoff"]["clients"] / '"${pop:-0}"', 4) if '"${pop:-0}"' else None'), \"waits\": $(echo "$st" | lg_py - 'd["storm"]["waits"]'), \"loadgen_bound\": $loadgen_bound, \"rejects\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_no_slot", "hs_no_token", "rejects_at_capacity", "rejects_pending_mem", "rejects_handshake_timeout", "rejects_auth_timeout", "rejects_login_queue", "logins_busy")} for g, v in d.items()}'), \"kicks\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("kick_attempts", "kicks_ok", "kick_giveups", "kick_dropped")}'), \"funnel\": $(echo "$series" | lg_py - 'd["funnel"]')"
  lost=$(echo "{\"a\": ${lost0:-null}, \"b\": ${lost1:-null}}" | lg_py - '{k: v - d["a"].get(k, 0) for k, v in d["b"].items() if v - d["a"].get(k, 0)}')
  hb_ok=$(rc_heartbeats_ok "${lost:-null}" "$d" "${pop:-null}" "${gh:-null}" "$name")
  runq_max=$(echo "$series" | lg_py - 'max([r[4] for r in d["rows"] if r[4] is not None], default=None)')
  t2_check "${name}_heartbeats" "$hb_ok" \
    "\"lost_since_mark\": ${lost:-null}, \"population\": ${pop:-null}, \"closes_heartbeat\": $(echo "$d" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ghosts_silent\": $(echo "${gh:-null}" | lg_py - '{g: v["silent"] for g, v in d["closed"].items()}'), \"heartbeat_s\": $HB, \"hold_s\": $HOLD, \"loadgen_bound\": $loadgen_bound, \"lg_runq_ms_per_s_max\": $runq_max"
  # As floods' verdict (#126, #127): the check still fails, labelled, when
  # loadgen was the bound: its reads may have been late (review O3).
  [ "$hb_ok" != true ] && [ "$loadgen_bound" = true ] &&
    t2_not_covered "${name}_heartbeats failed while loadgen was the bound: a late read on loadgen's side can also time a Pong out; read ${name}.series lg_runq_ms_per_s and loadgen_set_busy_pct beside the Pong timeouts (${name}.loadgen.storm)"
  t2_lease "$name" "$d" "$window_s" "$ld"
  sessions=$(rc_sessions "$mark_ms")
  t2_check "${name}_sessions" "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
    "\"sessions\": ${sessions:-null}, \"logic\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("logins_replaced", "logouts_deleted", "logouts_not_owner", "logouts_unknown")}')"

  # The end: the result is written before any client stops (#84), then the
  # gateways drain to idle (dropped Logouts allowed and counted).
  lg_stop
  t=$SECONDS
  idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  logouts=$(t2_gws_counters done='goim_gateway_logouts_total:result="done"' dropped='goim_gateway_logouts_total:result="dropped"' logins_ok='goim_gateway_logins_total:result="ok"')
  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"logouts\": $logouts, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

  cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
  # "reset" is the drop's reply: /reset's {"mark", "population", "close_ms"},
  # in ghost /ghost's {"mark", "population", "ms"} (the ghosting time, not
  # a close time). The name stays as run 31 recorded it (P6b3c2I-2).
  t2_field "$name" "{\"clients\": $N, \"rate\": $RATE, \"redis\": \"$STACK_REDIS\", \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"precompute_at_mark\": $precompute, \"reset\": ${reply:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms, \"back_s\": $back_s, \"hold_s\": $HOLD, \"window_s\": $window_s, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"slo\": $(cat "$slo"), \"loadgen\": $st, \"gateways\": $d, \"logic\": $ld, \"cpu_sets\": $cpu_sets, \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3"), \"series\": $series, \"redis_latency\": ${redis:-null}, \"redis_reset\": $redis_reset, \"ghosts\": ${gh:-null}, \"sessions\": ${sessions:-null}, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS}"
  t2_duration "$name" $((SECONDS - t0))

  a=$STACK_DIR/artifacts
  for f in before/logic after/logic before/gw-0 after/gw-0 before/gw-1 after/gw-1 before/gw-2 after/gw-2; do
    grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
  done
  for f in logic gw-0 gw-1 gw-2 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/storm/$f.cpu"; done
  [ -s "$a/series.jsonl" ] || missing+=" $name/series.jsonl"
  [ "$(lg_true "$slo" '"population" in d')" = true ] || missing+=" $name/slo-$mark.json"
  [ "$(lg_true "$a/loadgen-$name.json" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "the drop as an LB restart or failover: Tier 2's LB is loadgen's in-process balancer (#134); Tier 3 (Plan 6d)" \
  "a carrier or regional network recovering for part of the population: ghost drops every client (the spec's worst case)" \
  "heartbeat Pong times: SDK clients record a Pong only by its absence (the 10s Pong timeout), not its delay" \
  "$([ "$STACK_REDIS" = cluster ] && echo "the gwconns shards marked in P.redis_latency (Plan 6c3)" || echo "per-shard Redis load (one redis-server, its per-command latency in P.redis_latency; RECONNECT_REDIS=cluster for a Cluster)")" \
  "the network change at once: /network-change calls each client in turn (64 workers, each waits for the client's run loop); at 600k it took 27-36s in run 31, after most storm waits ended; several loadgens at Tier 3 (Plan 6d)"
t2_finish
