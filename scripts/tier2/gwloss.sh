#!/usr/bin/env bash
# Tier 2: gwloss (Plans 6b2a-b1, 6b2a-stress and 6b2b-b2), the spec's load
# tests (b1), (b2) and (b-stress) with SDK clients, each phase on a fresh stack:
#   (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of T_death
#        (loadgen records the SIGKILL time; R4M-17); Kick attempts to it are
#        about one per relogin, with no retries (R2-O6);
#   (b2) silent node loss of a 200k gateway: iptables DROP on its client and
#        Kick ports (goim-netdrop, #68), then SIGKILL behind the DROP, so no
#        FIN or RST reaches a client and its lease ends as in a crash (#112):
#        99% logged in again within 30s of the client detecting the loss
#        (Pong timeout); detection within one heartbeat plus the Pong
#        timeout; the survivor's Login p99 unaffected by the Kicks that time
#        out; Logic's goroutines bounded (R5S-O2). Its gateways send a 30s
#        heartbeat: the SDK's Pings keep their login's phase, so at 150s the
#        relogins would come in one band set by the script's timing, after
#        the lease and often after the sweep (6b2b-b2 review O1);
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
#   About 32 minutes (b1 ~8, b2 ~7, b_stress ~17; up to ~50 if b_stress's
#   relogins use their whole wait).
#
# Settings:
#   GWLOSS_PHASES      phases to run, in order (default "b1 b2 b_stress")
#   GWLOSS_CLIENTS     b1, b2: SDK clients over both gateways (default 400000)
#   GWLOSS_RATE        b1, b2: clients started per second (default 5000)
#   GWLOSS_CAP         b1, b2: each gateway's -max-conns (default 450000: the
#                      survivor's 400k plus room for clients in flight)
#   GWLOSS_SWEEP_WAIT  b1, b2: seconds after the kill to wait for the dead
#                      epoch's sweep (default 180: stale 60s after its last
#                      renewal, then 200k uids at the sweeper's 10k/s)
#   GWLOSS_SOURCES     b1, b2: loadgen's source addresses (default: loadgen's,
#                      one per 5,000 connections per gateway, sized for the
#                      survivor)
#   GWLOSS_B2_HEARTBEAT  b2: the gateways' -heartbeat (default 30s, the
#                      SDK's lowest): Ping phases spread evenly over the 80s
#                      ramp, so some relogins meet a live lease (review O1)
#   GWLOSS_B2_BACK_WAIT  b2: seconds after the DROP to wait for every victim
#                      to log in again (default 120: detection within 45s,
#                      then the SDK's 0-20s storm wait and the login)
#   GWLOSS_B2_DETECT_MAX_S  b2: the bound on every victim's detection, from
#                      the DROP to its Pong timeout (default 45: the 30s
#                      heartbeat plus the 10s Pong timeout, plus 5s for the
#                      timers' and loadgen's slack)
#   GWLOSS_B2_PROFILE_AT  b2: seconds after the DROP that the storm's 30s CPU
#                      profiles and loadgen-bound reading start (default 15:
#                      its relogins run from ~10s to ~60s; review O4)
#   GWLOSS_B2_GOROUTINE_GROWTH  b2: the most Logic's goroutines may grow over
#                      their count at the DROP while the victims log in again
#                      (default 500; a goroutine per Kick to the silent
#                      gateway would be thousands, R5S-O2)
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
#   GWLOSS_STRESS_HANDSHAKE_CONCURRENCY  b_stress: each gateway's
#                      -handshake-concurrency (default unset: the gateway's
#                      own, half its loops, 2 at 4). An experiment (run at
#                      271c022: 3.9M no_slot rejects for 600k logins, 201k
#                      clients in backoff, all back in 188s); the victim's
#                      ramp gets it too. The phase field's gateway_args
#                      records it.
#   GWLOSS_LAG_P99_MS  the storm's p99 bound on loadgen's lags, attempt_lag,
#                      connect_lag, send_lag and reply_lag, ms (default 100,
#                      #107)
#   GWLOSS_STRESS_CPUS_LOADGEN  b_stress: loadgen's CPU set (default
#                      0-5,10,12-17,22, #108)
#   GWLOSS_STRESS_CPUS_GATEWAY  b_stress: the gateways' CPU set (default 6-9,18-21,
#                      4 GOMAXPROCS each, #108)
#   GWLOSS_STRESS_CPUS_LOGIC  b_stress: Logic + Redis's CPU set (default
#                      11,23, #108)
#   GWLOSS_SS_EVERY    seconds between socket samples from the kill until every
#                      victim is back (default 0: off). A diagnostic (run 26:
#                      the survivors timed out 3.9M handshakes while idle):
#                      per survivor, its established sockets and those with
#                      unread bytes (Recv-Q), loadgen's sockets to it and
#                      those with unacked bytes (Send-Q), its accept queue,
#                      and its handshake gauge and counters; on the Logic CPU
#                      set. The result gets ss_<phase>, the samples in order.
#                      Each sample also has loadgen's GET /runtime: GC CPU
#                      seconds, GC cycles, goroutines, stack and heap bytes,
#                      mapped bytes against GOMEMLIMIT, and its scheduling
#                      waits (run 27: GC at the memory limit?). Each phase
#                      records one such reading once its victims are back,
#                      sampler or not: loadgen_at_back (P6b2a-pingerI-1).
#   GWLOSS_TAIL_PROFILE_AT  seconds after the kill to take a second 30s
#                      loadgen CPU profile, artifacts/storm_tail/ (default 0:
#                      off; at least 30, after the storm's own profile:
#                      loadgen serves one CPU profile at a time). A
#                      diagnostic (run 27: loadgen held ~13 cores while few
#                      clients were left to log in); taken only if the
#                      victims are not back by then.
#
# Checks (b1; b2 and b_stress have the same with their own prefix, unless
# noted):
#   b1_stack           redis, devtoken, Logic and the gateways become Ready
#   b1_pinning         gateways, Logic + Redis and loadgen on their phase's CPU
#                      sets (b_stress's differ, #108)
#   b1_online          every client logs in, each gateway holds 45-55% of
#                      them; b_stress: every client on gw-0, then gw-1 and
#                      gw-2 are back in loadgen's balancer
#   b1_slo             (b1 only) loadgen SIGKILLs gw-1: 99% of its clients log
#                      in again within 30s of T_death (R4M-17), and the
#                      survivor rejected none at capacity (SDK clients retry a
#                      reject without counting it, P6b2a-b1I-2)
#   b2_netdrop         (b2 only) both of gw-1's DROP rules were in before the
#                      SIGKILL, and none is left after the storm; without
#                      them there is no kill and no storm (review O7)
#   b2_slo             (b2 only) every victim detected the loss, and 99% of them
#                      logged in again within 30s of their detection (the
#                      spec's b2 SLO); the survivor rejected none at capacity
#   b2_detection       (b2 only) every victim's detection, from the DROP mark,
#                      within GWLOSS_B2_DETECT_MAX_S, and each by Pong timeout
#                      (loadgen's lost["pong-timeout"]), not by a FIN or RST
#   b2_login_p99       (b2 only) the survivor's Login p99 in the storm is at
#                      most one histogram bucket (2x) above its p99 in the
#                      ramp: Kicks to the silent gateway, each up to Logic's
#                      1s Kick deadline, never delay a Login (R5S-O2); the
#                      storm's Redis latency beside it (the sweep may
#                      overlap the storm's tail, review O2)
#   b2_goroutines      (b2 only) Logic's goroutines, sampled every second from
#                      the DROP until the victims are back, never grow more
#                      than GWLOSS_B2_GOROUTINE_GROWTH over the DROP's count
#   b_stress_back      (b_stress only) loadgen SIGKILLs gw-0: every one of its
#                      clients logs in again within GWLOSS_STRESS_BACK_WAIT, and
#                      the survivors rejected none at capacity; no SLO.
#                      Its relogin times are the gateways' measurement when
#                      b_stress_storm_real's lags_within is true (#107, #110)
#   b1_kicks           Kick first attempts in the storm, sent or dropped by a
#                      full queue (R5S-O2), are 0.9-1.1 per relogin (at least 0.9
#                      per relogin whose entry the sweep had not deleted: such a
#                      relogin replaces nothing, so it Kicks nothing), and none
#                      gives up as live. A refused Kick is not retried (#97):
#                      only first attempts that timed out while the victim's
#                      sockets were torn down are, once each, and refused
#                      (R2-O6; #97 amended, run 20). b2: the same bounds (first
#                      attempts at most 1.1 per relogin, sent or dropped at
#                      least 0.9), no Kick refused (the DROP answers nothing:
#                      they time out or fail fast, and keep their retries while
#                      the lease is live), at most 3 retries per first attempt,
#                      none gives up as live; and Kicks were exercised: some
#                      retried, sent or dropped first attempts for at least
#                      half the relogins (review O1)
#   b1_cross_check     Logic's dead-epoch cross-check (R1-O7) has a sample for
#                      at least 0.9 of the relogins whose first Kick was sent
#                      and did not time out (by Login once the lease is gone,
#                      or by the refused Kick while it is live, P6b2a-b1I-7),
#                      and at most 1.1 per relogin. b2: no Kick is refused, so
#                      only Login records it, for the relogins between the
#                      lease's end and the sweep: at least 0.1 per relogin
#                      (a cautious floor, review O6), at most 1.1
#   b1_storm_real      the storm was the spec's, not loadgen's (#106, #107):
#                      every logged-in client held its next handshake frame
#                      at the kill (loadgen -precompute); after the kill,
#                      loadgen started each attempt within GWLOSS_LAG_P99_MS
#                      of its wait's end, wrote each handshake frame within
#                      GWLOSS_LAG_P99_MS of its dial and each Login frame
#                      within GWLOSS_LAG_P99_MS of the handshake reply (all
#                      p99, at least one per relogin). b_stress_storm_real
#                      bounds the lags only when its storm was not
#                      loadgen_bound (#110: one host's loadgen is the bound in
#                      the first 30s); lags_within records whether they held
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
#                      (data; b_stress_storm_real decides, #107)
#   b1_idle            after loadgen stops, every survivor reaches 0
#                      connections, 0 awaiting Logout and one Logout done per
#                      login, none dropped (stack_wait_idle, P6b1I-4)
#   b1_processes       nothing but the victim died, panicked or stopped uncleanly
#   artifacts          (once) every phase's storm CPU profiles, metrics before
#                      and after, loadgen's heap at the ramp's end and at the
#                      kill, the SLO and loadgen's result
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

PHASES=${GWLOSS_PHASES:-b1 b2 b_stress}
for p in $PHASES; do
  case $p in b1 | b2 | b_stress) ;; *) echo "gwloss: unknown phase '$p' (b1, b2, b_stress)" >&2; exit 1 ;; esac
done
# The tail profile starts after the storm's own 30s profile: loadgen serves
# one CPU profile at a time.
case ${GWLOSS_TAIL_PROFILE_AT:-0} in
  0) ;;
  *[!0-9]*) echo "gwloss: GWLOSS_TAIL_PROFILE_AT must be 0 or seconds >= 30" >&2; exit 1 ;;
  *) [ "$GWLOSS_TAIL_PROFILE_AT" -ge 30 ] || { echo "gwloss: GWLOSS_TAIL_PROFILE_AT must be 0 or seconds >= 30" >&2; exit 1; } ;;
esac
case ${GWLOSS_STRESS_HANDSHAKE_CONCURRENCY:-1} in
  ''|*[!0-9]*|0) echo "gwloss: GWLOSS_STRESS_HANDSHAKE_CONCURRENCY must be a positive integer" >&2; exit 1 ;;
esac
# The memory each phase needs available (#93): b1's loadgen peaked at
# 10.6-11.5GB with 400k SDK clients (runs 20, 21). b_stress's loadgen peaked
# at 16.1GB with 600k (run 22): its live heap was 3.25GB, the rest mostly
# goroutine stacks, which GOMEMLIMIT does not bound. Run 22 started with
# 22GiB available (t2_mem_budget counts GiB) and swapped ~1GiB out, so
# b_stress was given about 23GiB with the victim, the survivors, Redis and
# the kernel's socket memory (#105). Since the SDK's pinger runs on a timer
# (plan 6b2a-pinger, 8381a9e), b_stress's loadgen peaks at 15.5GB RSS
# (7.38GB of stacks, mapped 16.51GB under its 16GiB limit) and the run
# swapped nothing at 23. The budgets stay until the host's UMA change
# (P6b2a-pingerI-3, I-4); the swap check is the verdict.
mem=14
case " $PHASES " in *" b_stress "*) mem=23 ;; esac
t2_mem_budget $mem
t2_nofile 1048576

export STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
NETDROP=/usr/local/sbin/goim-netdrop
t2_on_exit "sudo -n $NETDROP flush" # b2's DROP rules, whatever stopped the run
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
  # b2: a silent target refuses nothing; its attempts time out or fail fast
  # (gRPC's fail-fast RPCs on a connection in TRANSIENT_FAILURE: error).
  kick_first_refused='goim_logic_kick_attempts_total:attempt="first",result="refused"'
  kick_first_error='goim_logic_kick_attempts_total:attempt="first",result="error"'
  kick_retry_timeout='goim_logic_kick_attempts_total:attempt="retry",result="timeout"'
  kick_retry_error='goim_logic_kick_attempts_total:attempt="retry",result="error"'
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
# lg_heap WHEN: saves loadgen's heap after a forced GC to artifacts/WHEN/ and
# prints {live_mb, sys_mb, live_bytes_per_client} from it (null: unreadable).
lg_heap() {
  mkdir -p "$STACK_DIR/artifacts/$1"
  curl -sf -m 60 "http://$(lg_control)/debug/pprof/heap?gc=1&debug=1" >"$STACK_DIR/artifacts/$1/loadgen.heap.txt"
  awk -v n="$clients" '/^# HeapAlloc = / { a = $4 } /^# Sys = / { s = $4 }
    END { if (a == "") print "null"; else printf "{\"live_mb\": %d, \"sys_mb\": %d, \"live_bytes_per_client\": %d}\n", a / 1048576, s / 1048576, a / n }' \
    "$STACK_DIR/artifacts/$1/loadgen.heap.txt"
}
# ss_sample T: one GWLOSS_SS_EVERY sample, T seconds after the kill, as a JSON
# line. Per survivor (its listener 127.0.0.1:PORT): srv_est and srv_recvq_*
# are its established sockets and those holding bytes it has not read;
# cli_est and cli_sendq_* are loadgen's sockets to it and those holding bytes
# its kernel has not acked; accept_q is its listener's queue (backlog: the
# limit); then its handshake gauge and counters. ss_ms: what ss took.
# loadgen: its GET /runtime (run 27).
ss_sample() {
  local t=$1 g ports="" t0 ms est lst gw=""
  for g in "${survivors[@]}"; do ports+=" 127.0.0.1:$((STACK_PORT_BASE + 100 + 10 * ${g#gw-}))"; done
  t0=$(date +%s%N)
  est=$(ss -tnH state established 2>/dev/null | awk -v ports="$ports" '
    BEGIN { n = split(ports, p, " "); for (i = 1; i <= n; i++) want[p[i]] = 1 }
    $3 in want { se[$3]++; if ($1 > 0) { rn[$3]++; rb[$3] += $1 } ; next }
    $4 in want { ce[$4]++; if ($2 > 0) { sn[$4]++; sb[$4] += $2 } }
    END { for (i = 1; i <= n; i++) { q = p[i]
            printf "%s\"%s\": {\"srv_est\": %d, \"srv_recvq_conns\": %d, \"srv_recvq_bytes\": %d, \"cli_est\": %d, \"cli_sendq_conns\": %d, \"cli_sendq_bytes\": %d}", (i > 1 ? ", " : ""), q, se[q], rn[q], rb[q], ce[q], sn[q], sb[q] } }')
  lst=$(ss -tnH state listening 2>/dev/null | awk -v ports="$ports" '
    BEGIN { n = split(ports, p, " "); for (i = 1; i <= n; i++) want[p[i]] = 1 }
    $3 in want { printf "%s\"%s\": {\"accept_q\": %d, \"backlog\": %d}", (k++ ? ", " : ""), $3, $1, $2 }')
  ms=$((($(date +%s%N) - t0) / 1000000))
  for g in "${survivors[@]}"; do
    gw+="${gw:+, }\"$g\": $(stack_counters "$g" handshake_conns='goim_gateway_connections:state="handshake"' \
      handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"' \
      pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"' \
      handshakes_ok='goim_gateway_handshakes_total:result="ok"' handshakes_no_slot='goim_gateway_handshakes_total:result="no_slot"')"
  done
  echo "{\"t\": $t, \"ss_ms\": $ms, \"sockets\": {$est}, \"listeners\": {$lst}, \"gateways\": {$gw}, \"loadgen\": $(lg_get /runtime d)}"
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

missing=""
# b1 keeps #71's CPU sets; b_stress gives loadgen the CPUs its storm left
# idle (#108).
cpus_gateway=$T2_CPUS_GATEWAY cpus_loadgen=$T2_CPUS_LOADGEN cpus_logic=$T2_CPUS_LOGIC
lag_max=${GWLOSS_LAG_P99_MS:-100}
for name in $PHASES; do
  # Each phase's stack, victim and waits.
  case $name in
    b1 | b2)
      export STACK_GATEWAYS=2
      export T2_CPUS_GATEWAY=$cpus_gateway T2_CPUS_LOADGEN=$cpus_loadgen T2_CPUS_LOGIC=$cpus_logic
      unset STACK_GATEWAY_PROCS
      clients=${GWLOSS_CLIENTS:-400000} rate=${GWLOSS_RATE:-5000} cap=${GWLOSS_CAP:-450000}
      sweep_wait=${GWLOSS_SWEEP_WAIT:-180} back_wait=60 idle_wait=300 sources=${GWLOSS_SOURCES:-}
      victim=gw-1 survivors=(gw-0) down="" LG_ENV=() mark=death
      # b2: the loss is the DROP; its clients detect it by Pong timeout,
      # within one heartbeat (30s, GWLOSS_B2_HEARTBEAT) plus 10s.
      [ "$name" = b2 ] && back_wait=${GWLOSS_B2_BACK_WAIT:-120} mark=drop
      # Every client up from the start, split by the balancer.
      online_rule='d["online"] == '"$clients"' and min(d["online_by_gateway"].get(g, 0) for g in ("gw-0", "gw-1")) >= '"$clients"' * 0.45'
      ;;
    b_stress)
      export STACK_GATEWAYS=3
      # #108: loadgen the 4 fast cores and 3 compact (14 CPUs); the gateways
      # 4 compact cores at 4 GOMAXPROCS each as before (the victim alone in
      # the ramp, the two survivors after the kill); Logic + Redis one core.
      export T2_CPUS_GATEWAY=${GWLOSS_STRESS_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${GWLOSS_STRESS_CPUS_LOADGEN:-0-5,10,12-17,22}
      export T2_CPUS_LOGIC=${GWLOSS_STRESS_CPUS_LOGIC:-11,23}
      export STACK_GATEWAY_PROCS=4
      clients=${GWLOSS_STRESS_CLIENTS:-600000} rate=${GWLOSS_STRESS_RATE:-5000} cap=${GWLOSS_STRESS_CAP:-650000}
      sweep_wait=${GWLOSS_STRESS_SWEEP_WAIT:-300} back_wait=${GWLOSS_STRESS_BACK_WAIT:-900} idle_wait=600
      sources=${GWLOSS_STRESS_SOURCES:-}
      victim=gw-0 survivors=(gw-1 gw-2) down=gw-1,gw-2 LG_ENV=(GOMEMLIMIT="${GWLOSS_STRESS_LG_MEMLIMIT:-16GiB}") mark=death
      # The victim ramped alone (#93).
      online_rule='d["online"] == '"$clients"' and d["online_by_gateway"].get("gw-0") == '"$clients"
      ;;
  esac
  LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
  export STACK_DIR=$T2_OUT/$name
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  STACK_GATEWAY_ARGS=(-max-conns "$cap")
  [ "$name" = b_stress ] && [ -n "${GWLOSS_STRESS_HANDSHAKE_CONCURRENCY:-}" ] &&
    STACK_GATEWAY_ARGS+=(-handshake-concurrency "$GWLOSS_STRESS_HANDSHAKE_CONCURRENCY")
  [ "$name" = b2 ] && STACK_GATEWAY_ARGS+=(-heartbeat "${GWLOSS_B2_HEARTBEAT:-30s}")
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
  lg_start "$name" -mode sdk -precompute -clients "$clients" -rate "$rate" -uid-prefix "$name" ${sources:+-sources "$sources"} ${down:+-down "$down"} ${GWLOSS_DRY_NO_PRECOMPUTE:+-precompute=false}
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
  heap_ramp=$(lg_heap ramp)
  # b_stress: the survivors back in the balancer before the kill.
  ups=""
  for g in ${down//,/ }; do
    lg_post "/up?gateway=$g" >/dev/null || ups+=" $g"
  done
  [ -z "$ups" ] || online=false
  t2_check "${name}_online" "$online" "\"want\": $clients, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $by_gateway, \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]'), \"up_failed\": $(t2_json_str "${ups# }")"

  wrong=""
  for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
  for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
  have=$(taskset -pc "$LG_PID" 2>/dev/null)
  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
  t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
  if [ "$online" != true ]; then
    lg_stop
    stack_down
    t2_duration "$name" $((SECONDS - t0))
    continue
  fi

  # #107: every logged-in client holds its next handshake frame at
  # the kill, so the storm's first attempts are the spec's, not loadgen's.
  # Wall clock (#103).
  t=$SECONDS
  until [ "$(lg_get /stats '(d.get("precompute") or {}).get("pending") == 0 and (d.get("precompute") or {}).get("ready") == d["online"]')" = true ]; do
    [ $((SECONDS - t)) -lt 120 ] || break
    sleep 1
  done
  precompute=$(lg_get /stats '{"pending": (d.get("precompute") or {}).get("pending"), "ready": (d.get("precompute") or {}).get("ready"), "online": d["online"], "wait_s": '"$((SECONDS - t))"'}')
  # Again at the kill: the prepared handshakes (#106) are live through the
  # storm's first 20s, its memory peak (P6b2a-stormI-1).
  heap_kill=$(lg_heap at_kill)

  # The storm: counters before, SIGKILL, CPU profiles of its first 30s.
  stack_snapshot before
  # b2: the survivor's Login latency so far (the ramp's; a fresh stack) and
  # Logic's goroutines, both at the DROP.
  login_h0=null goroutines0=null
  if [ "$name" = b2 ]; then
    login_h0=$(stack_buckets gw-0 goim_gateway_login_seconds)
    goroutines0=$(stack_metric logic go_goroutines)
  fi
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
  # b2 (#112): the DROP goes in first, on the victim's client and Kick ports,
  # so the SIGKILL's FINs and RSTs never reach a client and a Kick's connect
  # is never refused: the node is silent, then gone, as in a node loss. The
  # mark is taken at the SIGKILL, a few ms after the DROP.
  drop_rules=null
  if [ "$name" = b2 ]; then
    vport=$((STACK_PORT_BASE + 100 + 10 * ${victim#gw-}))
    sudo -n "$NETDROP" add "$vport" && sudo -n "$NETDROP" add $((vport + 1))
    drop_rules=$(sudo -n "$NETDROP" list | wc -l)
    # Without both rules the storm would be b1's (RSTs reach the clients):
    # no kill, no storm (review O7).
    if [ "$drop_rules" != 4 ]; then
      t2_check "${name}_netdrop" false "\"ports\": [$vport, $((vport + 1))], \"rules_at_kill\": $drop_rules, \"rules_left\": null"
      sudo -n "$NETDROP" flush
      lg_stop
      stack_down
      t2_duration "$name" $((SECONDS - t0))
      continue
    fi
  fi
  reply=$(lg_post "/kill?gateway=$victim&pid=${STACK_PIDS[$victim]}&mark=$mark" | lg_py - d)
  kill_t=$SECONDS
  # In this shell: it reaps and forgets the victim. 60s: a 600k gateway
  # exits only once the kernel has closed its sockets (~5.3s in run 25).
  stack_expect_dead "$victim" 60
  dead=$?
  # The storm's first 30s, where its relogins peak: loadgen's cores there
  # say whether the storm was loadgen-bound (#102); the whole storm's average
  # hid it in run 22 (3.2 of 8 cores, 7.9 in the first 30s).
  # b2: from GWLOSS_B2_PROFILE_AT after the mark, its relogins starting at
  # the first Pong timeouts (review O4).
  prof_at=0
  [ "$name" = b2 ] && prof_at=${GWLOSS_B2_PROFILE_AT:-15}
  (sleep "$prof_at"; [ "$prof_at" = 0 ] || stack_usage_start loadgen="$LG_PID"
    stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
  prof=$!
  # GWLOSS_TAIL_PROFILE_AT: loadgen's CPU in the storm's tail (run 27), if
  # the victims are not back by then; stopped with them.
  tail_at=${GWLOSS_TAIL_PROFILE_AT:-0} tailprof=""
  if [ "$tail_at" -gt 0 ]; then
    (sleep "$tail_at"; lg_profile storm_tail 30) >/dev/null 2>&1 &
    tailprof=$!
  fi
  # GWLOSS_SS_EVERY: socket samples from the kill until the victims are back,
  # on the Logic CPU set (a diagnostic, off by default; run 26).
  ss_every=${GWLOSS_SS_EVERY:-0} sampler=""
  if [ "$ss_every" -gt 0 ]; then
    (while :; do ss_sample $((SECONDS - kill_t)); sleep "$ss_every"; done) >"$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null &
    sampler=$!
    taskset -a -p -c "$T2_CPUS_LOGIC" "$sampler" >/dev/null 2>&1
  fi
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
  # b2: Logic's goroutines every second until the victims are back (R5S-O2),
  # on the Logic CPU set.
  counting=""
  if [ "$name" = b2 ]; then
    (while :; do stack_metric logic go_goroutines; sleep 1; done) >"$STACK_DIR/goroutines.txt" 2>/dev/null &
    counting=$!
    taskset -a -p -c "$T2_CPUS_LOGIC" "$counting" >/dev/null 2>&1
  fi
  back=$(t2_bool lg_wait_back "$mark" "$back_wait")
  back_s=$((SECONDS - kill_t))
  goroutines=null
  if [ -n "$counting" ]; then
    kill "$counting" 2>/dev/null
    wait "$counting" 2>/dev/null
    goroutines=$(python3 -c 'import json, sys
n = [int(l) for l in open(sys.argv[1]) if l.strip().isdigit()]
print(json.dumps({"at_drop": int(sys.argv[2]) if sys.argv[2].isdigit() else None, "max": max(n) if n else None, "samples": len(n)}))' \
      "$STACK_DIR/goroutines.txt" "$goroutines0" 2>/dev/null || echo null)
  fi
  # Loadgen's runtime once the victims are back (or the wait ran out), even
  # with GWLOSS_SS_EVERY off: its memory against GOMEMLIMIT and the GC
  # limiter at the storm's end (P6b2a-pingerI-1). Recorded, not a check.
  rt_back=$(lg_get /runtime d 2>/dev/null)
  [ -n "$rt_back" ] || rt_back=null
  if [ -n "$sampler" ]; then
    kill "$sampler" 2>/dev/null
    wait "$sampler" 2>/dev/null
    # A sample the kill cut short is left out.
    t2_field "ss_$name" "$(python3 -c 'import json, sys
out = []
for l in open(sys.argv[1]):
    try:
        out.append(json.loads(l))
    except ValueError:
        pass
print(json.dumps(out))' "$STACK_DIR/artifacts/ss-storm.jsonl" 2>/dev/null || echo null)"
  fi
  # A tail profile still running finishes: the storm's last clients are its
  # tail too; one not started yet is not taken.
  if [ -n "$tailprof" ]; then
    [ $((SECONDS - kill_t)) -lt "$tail_at" ] && { pkill -P "$tailprof" 2>/dev/null; kill "$tailprof" 2>/dev/null; }
    wait "$tailprof" 2>/dev/null
  fi
  lg_get "/slo?mark=$mark&within=30s" d >"$STACK_DIR/artifacts/slo-$mark.json"
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
  # b2: the survivor's Login latency in the storm; then the DROP rules come
  # out, so the dead port's leftover sockets on both sides end by RST.
  login_h1=null drop_left=null
  if [ "$name" = b2 ]; then
    login_h1=$(stack_buckets gw-0 goim_gateway_login_seconds)
    sudo -n "$NETDROP" del "$vport"
    sudo -n "$NETDROP" del $((vport + 1))
    drop_left=$(sudo -n "$NETDROP" list | wc -l)
  fi
  stack_snapshot after
  logic_d=$(stack_counters_delta "$logic0" "$logic1")
  sv_d=""
  for g in "${survivors[@]}"; do sv_d+="${sv_d:+, }\"$g\": $(stack_counters_delta "${sv0[$g]}" "${sv1[$g]}")"; done
  sv_d="{$sv_d}"
  window_s=$(echo "$storm" | lg_py - 'd["window_s"]')
  backoff="{\"ramp\": $backoff_ramp, \"storm\": $(lg_get /stats 'd["backoff_since_mark"]'), \"run\": $(lg_get /stats '{"clients": d["backoff_clients"], "waits": d["backoff_waits"]}')}"

  slo=$STACK_DIR/artifacts/slo-$mark.json
  pop=$(lg_py "$slo" 'd["population"]')
  # Rejects at capacity over every survivor; null if any reading failed.
  rejects=$(echo "$sv_d" | lg_py - 'sum(v["rejects_at_capacity"] for v in d.values())')
  pass=false
  if [ "$name" = b1 ]; then
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_mark"] and d["population"] >= '"$clients"' * 0.45')
    t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
  elif [ "$name" = b2 ]; then
    t2_check "${name}_netdrop" "$(t2_bool [ "$drop_rules:$drop_left" = 4:0 ])" "\"ports\": [$vport, $((vport + 1))], \"rules_at_kill\": $drop_rules, \"rules_left\": $drop_left"
    # The spec's b2 SLO: from each client's detection; every victim detected.
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && pass=$(lg_true "$slo" 'd["pass_from_detection"] and d["lost"] == d["population"] and d["population"] >= '"$clients"' * 0.45')
    t2_check "${name}_slo" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"rejects_at_capacity\": ${rejects:-null}, \"slo\": $(cat "$slo")"
    lost=$(lg_get /stats 'd["lost"]')
    detect_max=${GWLOSS_B2_DETECT_MAX_S:-45}
    t2_check "${name}_detection" "$(echo "{\"s\": $(cat "$slo"), \"lost\": $lost}" | lg_true - 'd["s"]["detection"]["n"] == d["s"]["population"] and 0 <= d["s"]["detection"]["max_ms"] <= '"$detect_max"' * 1000 and d["lost"].get("pong-timeout", 0) >= d["s"]["population"]')" \
      "\"max_s\": $detect_max, \"detection\": $(lg_py "$slo" 'd["detection"]'), \"lost\": $lost"
    p99_ramp=$(stack_hist_quantile '{}' "$login_h0" 0.99) p99_storm=$(stack_hist_quantile "$login_h0" "$login_h1" 0.99)
    t2_check "${name}_login_p99" "$(echo "{\"r\": $p99_ramp, \"s\": $p99_storm}" | lg_true - 'd["r"] is not None and d["s"] is not None and d["s"] <= 2 * d["r"]')" \
      "\"ramp_p99_ms\": $p99_ramp, \"storm_p99_ms\": $p99_storm, \"bucket_bounds\": true, \"redis_latency_storm\": $redis_storm"
    growth=${GWLOSS_B2_GOROUTINE_GROWTH:-500}
    t2_check "${name}_goroutines" "$(echo "$goroutines" | lg_true - 'd is not None and d["at_drop"] is not None and d["max"] is not None and d["max"] - d["at_drop"] <= '"$growth")" \
      "\"max_growth\": $growth, \"logic\": $goroutines"
  else
    [ $dead -eq 0 ] && [ "$rejects" = 0 ] && [ "$back" = true ] && pass=$(lg_true "$slo" 'd["population"] == '"$clients")
    t2_check "${name}_back" "$pass" "\"kill\": ${reply:-null}, \"back_all\": $back, \"back_s\": $back_s, \"wait_s\": $back_wait, \"loadgen_bound\": $loadgen_bound, \"rejects_at_capacity\": ${rejects:-null}, \"backoff\": $backoff, \"slo\": $(cat "$slo")"
  fi
  if [ "$name" = b2 ]; then
    # A silent target (#112): no Kick refused; retries kept while the lease
    # is live, at most 3 per first attempt (R2-O6); the sent-or-dropped
    # bounds as b1's (dropped also counts retries a full queue dropped, so
    # only the first attempts have the upper bound). Kicks were exercised
    # (review O1): some retried while the lease was live, and first attempts
    # sent or dropped for at least half the relogins (at a 30s heartbeat
    # nearly all relogins come before the sweep).
    kicks_rule='d["d"]["kick_first_refused"] == 0 and d["d"]["kick_retry_refused"] == 0 and d["d"]["kick_giveups_live"] == 0 and 0 < d["d"]["kick_retry"] <= 3 * d["d"]["kick_first"] and d["d"]["kick_first"] <= 1.1 * d["pop"] and max(0.5 * d["pop"], 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0))) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0)'
    # R1-O7: only Login records the cross-check here (no refused Kick), for
    # relogins between the lease's end and the sweep: most of them at a 30s
    # heartbeat; 0.1 per relogin is a cautious floor (review O6).
    cross_rule='0.1 * d["pop"] <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]'
  else
    kicks_rule='d["d"]["kick_retry"] <= d["d"]["kick_first_timeout"] and d["d"]["kick_retry_refused"] == d["d"]["kick_retry"] and d["d"]["kick_giveups_live"] == 0 and 0.9 * (d["pop"] - (d["d"]["sweep_deleted"] or 0)) <= d["d"]["kick_first"] + (d["d"]["kick_dropped"] or 0) <= 1.1 * d["pop"]'
    # R1-O7: one sample per relogin whose first Kick was sent and did not time
    # out, by Login (lease gone) or by the refused Kick (lease live).
    cross_rule='0.9 * (d["d"]["kick_first"] - d["d"]["kick_first_timeout"]) <= d["d"]["dead_epoch_relogins"] <= 1.1 * d["pop"]'
  fi
  t2_check "${name}_kicks" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - "$kicks_rule")" \
    "\"relogins\": $pop, \"swept_first\": $(echo "$logic_d" | lg_py - 'd["sweep_deleted"]'), \"first\": $(echo "$logic_d" | lg_py - 'd["kick_first"]'), \"retries\": $(echo "$logic_d" | lg_py - 'd["kick_retry"]'), \"first_timeouts\": $(echo "$logic_d" | lg_py - 'd["kick_first_timeout"]'), \"first_errors\": $(echo "$logic_d" | lg_py - 'd["kick_first_error"]'), \"first_refused\": $(echo "$logic_d" | lg_py - 'd["kick_first_refused"]'), \"retries_refused\": $(echo "$logic_d" | lg_py - 'd["kick_retry_refused"]'), \"retries_timeout\": $(echo "$logic_d" | lg_py - 'd["kick_retry_timeout"]'), \"retries_error\": $(echo "$logic_d" | lg_py - 'd["kick_retry_error"]'), \"giveups_dead\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_dead"]'), \"giveups_live\": $(echo "$logic_d" | lg_py - 'd["kick_giveups_live"]'), \"dropped\": $(echo "$logic_d" | lg_py - 'd["kick_dropped"]')"
  t2_check "${name}_cross_check" "$(echo "{\"d\": $logic_d, \"pop\": $pop}" | lg_true - "$cross_rule")" \
    "\"relogins\": $pop, \"samples\": $(echo "$logic_d" | lg_py - 'd["dead_epoch_relogins"]'), \"mean_s\": $(echo "$logic_d" | lg_py - 'round(d["dead_epoch_relogin_s"] / d["dead_epoch_relogins"], 1)')"
  # #107: the storm was the spec's: every logged-in client was prepared at
  # the kill, and after it loadgen started each attempt, got each dial back
  # after connect (connect_lag: the survivor's handshake timer starts at the
  # accept, inside it; the b_stress diagnostic run), sent each handshake
  # frame after its dial (send_lag, run 25: a late frame is a survivor's
  # handshake_timeout) and answered each handshake reply within
  # GWLOSS_LAG_P99_MS (p99; loadgen's own lags only, review I1/I2). Its
  # detail has the storm's handshake counts: precomputed, inline and reused
  # (#109; run 25: 5.2M inline, before #109). b-stress has no SLO; its
  # relogin distribution is the measurement only when this passes. Detection (mark → loss noticed on the
  # client's run loop) is recorded beside the lags, not gated: it includes
  # the dying gateway's socket teardown, which production has too.
  # b_stress's lags are bounded only when its storm was not loadgen-bound
  # (#110): on one host, 600k SDK clients' first 30s take ~12 of loadgen's 14
  # cores (the 6b2a-pinger run), so its lags measure loadgen, not the
  # survivors. Preparation and one sample per relogin are always required;
  # lags_within says whether the relogin times are the gateways'.
  storm_d=$(lg_get /stats 'd["storm_since_mark"]')
  lags_within=$(echo "{\"s\": $storm_d, \"pop\": $pop}" | lg_true - 'all(d["s"][k]["p99_ms"] <= '"$lag_max"' for k in ("attempt_lag", "connect_lag", "send_lag", "reply_lag"))')
  real=$(echo "{\"s\": $storm_d, \"p\": $precompute, \"pop\": $pop}" | lg_true - 'd["p"]["ready"] is not None and d["p"]["ready"] == d["p"]["online"] and all(d["s"][k]["n"] >= d["pop"] for k in ("attempt_lag", "connect_lag", "send_lag", "reply_lag"))')
  { [ "$lags_within" = true ] || { [ "$name" = b_stress ] && [ $loadgen_bound = true ]; }; } || real=false
  t2_check "${name}_storm_real" "$real" \
    "\"lag_p99_max_ms\": $lag_max, \"lags_within\": $lags_within, \"loadgen_bound\": $loadgen_bound, \"precompute_at_kill\": $precompute, \"storm\": $storm_d, \"detection\": $(lg_py "$slo" 'd["detection"]')"
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
  t2_field "$name" "{\"clients\": $clients, \"victim\": \"$victim\", \"by_gateway_at_kill\": $by_gateway, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"loadgen_heap\": {\"ramp\": $heap_ramp, \"at_kill\": $heap_kill}, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"precompute_at_kill\": $precompute, \"storm_since_mark\": $storm_d, \"back_s\": $back_s, \"loadgen_at_back\": $rt_back, \"sweep_s\": $sweep_s, \"logic\": $logic_d, \"survivors\": $sv_d, \"redis_latency\": {\"ramp\": $redis_ramp, \"storm\": $redis_storm, \"reset\": {\"ramp\": $reset_ramp, \"storm\": $reset_storm}}, \"backoff\": $backoff, \"cpu_sets\": $cpu_sets, \"tcp\": $tcp, \"profile_in_storm_s\": 30, \"profile_at_s\": $prof_at, \"stop_s\": $stop_s, \"goroutines\": $goroutines, \"loadgen_exit\": $LG_STATUS, \"login_latency\": $(lg_json 'd["login_latency"]'), \"failed\": $(lg_json 'd["failed"]'), \"lost\": $(lg_json 'd["lost"]'), \"sources\": $(lg_json 'd["sources"]')}"
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
  for w in ramp at_kill; do
    grep -q '^# HeapAlloc = ' "$a/$w/loadgen.heap.txt" 2>/dev/null || missing+=" $name/$w/loadgen.heap.txt"
  done
  [ "$(lg_true "$a/slo-$mark.json" '"population" in d')" = true ] || missing+=" $name/slo-$mark.json"
  [ "$(lg_true "$res" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "per-shard Redis CPU and p99 with the gwconns shards marked (one redis-server; Redis Cluster in Plan 6c)" \
  "b_stress's survivors start empty: no survivor carries its own load through the 600k storm (#93)" \
  "b2: the lost node's recovery (out-of-service taint, gw-k recreated elsewhere, R5S-O3) is Tier 3 (Plan 6d)" \
  "slowed Logic (scenario slowlogic, Plan 6b2b-slow)"
t2_finish
