#!/usr/bin/env bash
# Tests for scripts/stack.sh on any machine (make test-scripts): a real stack
# of two gateways comes up Ready, pinned when asked, writes stack.env and a
# metrics snapshot, and stack_down leaves no process and no port behind.
# Needs redis-server (make redis-server) and curl.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/stack.sh
. scripts/loadgen.sh
STACK_DIR=$(mktemp -d)
trap 'stack_down; rm -rf "$STACK_DIR"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }

STACK_GATEWAYS=2
STACK_PORT_BASE=${STACK_PORT_BASE:-17500}
STACK_PIN=1
ncpu=$(getconf _NPROCESSORS_ONLN)
# Gateways get every CPU, so their CPU share (#79: CPUs / gateways) differs
# from the GOMAXPROCS Go would take from the affinity mask.
T2_CPUS_GATEWAY=0-$((ncpu - 1))
T2_CPUS_LOGIC=0
STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000)
STACK_GATEWAY_ENV=(GOGC=50 GOMEMLIMIT=1GiB)
procs=$((ncpu / 2 > 1 ? ncpu / 2 : 1))

# The ports checked before stack_up include loadgen's control port.
grep -qx $((STACK_PORT_BASE + 5)) <<<"$(stack_ports)" && ok "ports: loadgen's control port" || bad "ports: loadgen's control port missing"

# stack_metric prints byte-sized sums whole, as Prometheus writes them
# (5.36870912e+09), also under an awk whose %d stops at 2^31 (busybox; 6b1
# minor M1). A fake gw-0 /metrics, before the real stack.
if command -v busybox >/dev/null; then
  (
    mkdir -p "$STACK_DIR/fake"
    printf '%s\n' 'go_memstats_heap_inuse_bytes 5.36870912e+09' 'goim_x{a="1"} 3e+09' 'goim_x{a="2"} 3e+09' >"$STACK_DIR/fake/metrics"
    python3 -m http.server --bind 127.0.0.1 -d "$STACK_DIR/fake" $((STACK_PORT_BASE + 102)) >/dev/null 2>&1 &
    srv=$!
    trap 'kill $srv' EXIT
    declare -A STACK_PIDS=([gw-0]=$srv)
    awk() { busybox awk "$@"; }
    for i in $(seq 50); do curl -sf -o /dev/null "http://127.0.0.1:$((STACK_PORT_BASE + 102))/metrics" && break; sleep 0.1; done
    expect "metric: bytes past 2^31" "$(stack_metric gw-0 go_memstats_heap_inuse_bytes)" 5368709120
    expect "metric: a sum past 2^32" "$(stack_metric gw-0 goim_x)" 6000000000
    exit $fails
  ) || fails=$((fails + $?))
else
  echo "skip metric: bytes past 2^31 (no busybox)"
fi

# stack_wait_idle also waits for each running gateway's Logouts to settle
# (P6b1I-4): one done per login, none dropped, read from one scrape. A fake
# gw-0 /metrics, before the real stack.
(
  mkdir -p "$STACK_DIR/fake-idle"
  fake() { printf '%s\n' goim_gateway_connections\ 0 goim_gateway_awaiting_logout\ 0 "$@" >"$STACK_DIR/fake-idle/metrics"; }
  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 4'
  python3 -m http.server --bind 127.0.0.1 -d "$STACK_DIR/fake-idle" $((STACK_PORT_BASE + 102)) >/dev/null 2>&1 &
  srv=$!
  trap 'kill $srv' EXIT
  STACK_GATEWAYS=1
  declare -A STACK_PIDS=([gw-0]=$srv)
  for i in $(seq 50); do curl -sf -o /dev/null "http://127.0.0.1:$((STACK_PORT_BASE + 102))/metrics" && break; sleep 0.1; done
  out=$(stack_wait_idle 1 2>&1) && bad "idle: a Logout short must keep it busy" || ok "idle: a Logout short keeps it busy"
  case $out in *"gw-0: connections 0, awaiting_logout 0, logouts done 4 of 5 logins"*) ok "idle: names the Logouts" ;; *) bad "idle: Logouts not named: [$out]" ;; esac
  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 6'
  stack_wait_idle 2 && ok "idle: more Logouts than logins settle (retried logins)" || bad "idle: done 6 of 5 logins"
  fake
  stack_wait_idle 2 && ok "idle: no login and no Logout yet settle" || bad "idle: a fresh gateway"
  fake 'goim_gateway_logins_total{result="ok"} 5' 'goim_gateway_logouts_total{result="done"} 4' 'goim_gateway_logouts_total{result="dropped"} 1'
  start=$SECONDS
  out=$(stack_wait_idle 10 2>&1) && bad "idle: a dropped Logout must fail" || ok "idle: a dropped Logout fails"
  [ $((SECONDS - start)) -lt 3 ] && ok "idle: a dropped Logout fails at once" || bad "idle: a dropped Logout waited $((SECONDS - start))s"
  case $out in *"dropped 1"*) ok "idle: names the dropped Logout" ;; *) bad "idle: dropped not named: [$out]" ;; esac
  # A gateway that does not answer is busy, never idle.
  kill $srv; wait $srv 2>/dev/null
  out=$(stack_wait_idle 1 2>&1) && bad "idle: a silent gateway must keep it busy" || ok "idle: a silent gateway keeps it busy"
  case $out in *"gw-0: no answer"*) ok "idle: names the silent gateway" ;; *) bad "idle: silent gateway not named: [$out]" ;; esac
  exit $fails
) || fails=$((fails + $?))

# stack_term records what wait reports, "killed" only for a process our
# SIGKILL ended (M1): a kill -KILL that lands on an exited, unreaped process
# (stubbed here) must not turn a clean exit 0 into "killed".
term_status() {
  (
    STACK_DIR=$(mktemp -d)
    declare -A STACK_PIDS=()
    trap '' TERM # inherited at fork: no window where SIGTERM still works
    sleep "$2" >/dev/null 2>&1 & STACK_PIDS[p]=$!
    trap - TERM
    [ "$1" = stub ] && kill() { if [ "$1" = -KILL ]; then return 0; fi; command kill "$@"; }
    stack_term 1 p
    cat "$STACK_DIR/exit-status"
    rm -rf "$STACK_DIR"
  )
}
expect "term: a process that exits 0 at the deadline is not killed" "$(term_status stub 1.3)" "p 0"
expect "term: a process that ignores SIGTERM is killed" "$(term_status real 30)" "p killed"

stack_build || { echo "build failed"; exit 1; }
if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi

. "$STACK_DIR/stack.env"
expect "env: two gateway addresses" "$GATEWAY_ADDRS" "127.0.0.1:$((STACK_PORT_BASE + 100)),127.0.0.1:$((STACK_PORT_BASE + 110))"
expect "env: Noise public key is 32 bytes" "$(printf '%s' "$NOISE_PUBLIC" | base64 -d 2>/dev/null | wc -c)" 32
[ -s "$JWT_KEY" ] && ok "env: token key" || bad "env: token key missing"
b=$STACK_PORT_BASE
expect "env: gateway ids, in address order" "${GATEWAY_IDS:-}" "gw-0,gw-1"
expect "env: gateway admin addresses" "${GATEWAY_ADMINS:-}" "127.0.0.1:$((b + 102)),127.0.0.1:$((b + 112))"
expect "env: gateway Kick addresses" "${GATEWAY_KICK_ADDRS:-}" "127.0.0.1:$((b + 101)),127.0.0.1:$((b + 111))"
expect "env: Logic admin" "$LOGIC_ADMIN" "127.0.0.1:$((b + 4))"
expect "env: Redis" "$REDIS_ADDR" "127.0.0.1:$b"
expect "env: no mixed ADMIN_ADDRS" "${ADMIN_ADDRS-unset}" unset
expect "env: gateway procs is the CPU share" "${GATEWAY_PROCS:-}" "$procs"
for name in gw-0 gw-1; do
  grep -q " loops=$procs " "$STACK_DIR/logs/$name.log" && ok "procs: $name runs $procs loops" || bad "procs: $name: $(grep -o ' loops=[0-9]* ' "$STACK_DIR/logs/$name.log")"
done
# Stack processes have process groups of their own, so a Ctrl-C to the
# run's group does not reach them all at once (M3), and lib.sh counts them as ours.
mypgid=$(ps -o pgid= -p $$ | tr -d ' ')
apart=0
for name in "${STACK_ORDER[@]}"; do
  pg=$(ps -o pgid= -p "${STACK_PIDS[$name]}" | tr -d ' ')
  [ "$pg" != "$mypgid" ] && [[ " ${T2_OURS_PGIDS[*]:-} " == *" $pg "* ]] && apart=$((apart + 1))
done
expect "pgid: every process in its own group, registered as ours" "$apart" 5
for name in gw-0 gw-1; do
  stack_pinned "$name" "$T2_CPUS_GATEWAY" && ok "pin: $name on CPU $T2_CPUS_GATEWAY" || bad "pin: $name"
done
for name in redis devtoken logic; do
  stack_pinned "$name" "$T2_CPUS_LOGIC" && ok "pin: $name on CPU 0" || bad "pin: $name"
done
stack_pinned gw-0 "$T2_CPUS_LOGIC" && bad "pin: a wrong set must not match" || ok "pin: a wrong set does not match"

stack_snapshot idle
for f in logic gw-0 gw-1; do
  grep -q '^# TYPE' "$STACK_DIR/artifacts/idle/$f.metrics" && ok "snapshot: $f metrics" || bad "snapshot: $f metrics"
done
grep -q '^redis_version:' "$STACK_DIR/artifacts/idle/redis.info" && ok "snapshot: redis INFO" || bad "snapshot: redis INFO"
# Profiles from pprof on the admin ports (choice #85).
for f in logic gw-0 gw-1; do
  grep -q 'goroutine profile:' "$STACK_DIR/artifacts/idle/$f.goroutine.txt" 2>/dev/null && ok "snapshot: $f goroutines" || bad "snapshot: $f goroutines"
  gzip -t "$STACK_DIR/artifacts/idle/$f.heap.pb.gz" 2>/dev/null && ok "snapshot: $f heap profile" || bad "snapshot: $f heap profile"
done
# The gateways run with STACK_GATEWAY_ENV (#88), Logic without it.
for name in gw-0 gw-1; do
  expect "env: $name has GOGC and GOMEMLIMIT" "$(tr '\0' '\n' <"/proc/${STACK_PIDS[$name]}/environ" | grep -E '^GO(GC|MEMLIMIT)=' | sort | tr '\n' ' ')" "GOGC=50 GOMEMLIMIT=1GiB "
done
expect "env: Logic has no GOGC" "$(tr '\0' '\n' <"/proc/${STACK_PIDS[logic]}/environ" | grep -c '^GOGC=')" 0

# stack_metric sums a metric's samples, filtered by a label; null when absent.
expect "metric: a gauge" "$(stack_metric gw-0 goim_gateway_tracked)" 0
expect "metric: summed over labels" "$(stack_metric gw-0 goim_gateway_connections)" 0
expect "metric: filtered by a label" "$(stack_metric gw-0 goim_gateway_limit 'limit="nofile"')" "$(ulimit -n)"
expect "metric: a name is not a prefix" "$(stack_metric gw-0 go_gc_duration_seconds_coun)" null
expect "metric: no such metric" "$(stack_metric gw-0 goim_no_such_metric)" null
expect "metric: no such process" "$(stack_metric gw-9 goim_gateway_ready)" null
case $(stack_metric gw-0 go_gc_duration_seconds 'quantile="1"') in
  null | "") bad "metric: a summary quantile" ;; *) ok "metric: a summary quantile" ;;
esac

# stack_metrics + stack_metric_in: several readings from ONE scrape, so an
# absent sample means no sample, not a failed scrape (run 14's P_idle).
body=$(stack_metrics gw-0) && ok "metrics: one scrape" || bad "metrics: one scrape"
expect "metrics: a reading from the scrape" "$(stack_metric_in "$body" goim_gateway_connections)" 0
expect "metrics: an absent sample" "$(stack_metric_in "$body" goim_no_such_metric)" null
expect "metrics: a label filter" "$(stack_metric_in 'goim_x{r="done"} 5
goim_x{r="dropped"} 2' goim_x 'r="done"')" 5
stack_metrics gw-9 >/dev/null && bad "metrics: no such process must fail" || ok "metrics: no such process fails"

# stack_counters: several counters from one scrape, as JSON (Plan 6b2a). A
# metric with no sample is null (a typo must not read 0); a label value
# with no sample of a metric that has others is 0 (a retry counter before
# the first retry); a process that does not answer is null.
c=$(stack_counters gw-0 conns=goim_gateway_connections first='goim_gateway_lease_calls_total:result="first"' \
  never='goim_gateway_lease_calls_total:result="never"' typo=goim_gateway_lease_calls_totl)
expect "counters: one scrape as JSON" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(sorted(d), d["conns"], d["first"], d["never"], d["typo"])' "$c")" \
  "['conns', 'first', 'never', 'typo'] 0 1 0 None"
expect "counters: no such process" "$(stack_counters gw-9 conns=goim_gateway_connections)" null
expect "counters: delta, an absent sample before is 0" \
  "$(stack_counters_delta '{"a": 1, "b": null, "c": 2, "d": 0.5}' '{"a": 4, "b": 3, "c": null, "d": 0.75}')" '{"a": 3, "b": 3, "c": null, "d": 0.25}'
expect "counters: delta of a failed scrape" "$(stack_counters_delta null '{"a": 1}')" null

# A histogram's quantile is the bound of the bucket that holds it, over the
# samples between two readings (Plan 6b2b: b2's Login p99).
h0='{"0.0005": 50, "0.001": 50, "0.002": 50, "+Inf": 50}'
h1='{"0.0005": 90, "0.001": 99, "0.002": 100, "+Inf": 100}'
expect "hist: p99's bucket bound in ms" "$(stack_hist_quantile '{}' "$h1" 0.99)" 1
expect "hist: a sub-ms bound" "$(stack_hist_quantile '{}' "$h1" 0.5)" 0.5
expect "hist: only the samples between the readings" "$(stack_hist_quantile "$h0" "$h1" 0.99)" 2
expect "hist: no sample between the readings" "$(stack_hist_quantile "$h1" "$h1" 0.99)" null
expect "hist: a quantile in +Inf" "$(stack_hist_quantile '{}' '{"0.001": 1, "+Inf": 3}' 0.9)" null
expect "hist: a failed reading" "$(stack_hist_quantile null "$h1" 0.99)" null
expect "hist: a quantile above 1" "$(stack_hist_quantile '{}' "$h1" 1.5)" null
# 0.28 x 25 is 7.000000000000001 in floating point: the rank is still 7.
expect "hist: an exact rank" "$(stack_hist_quantile '{}' '{"0.001": 7, "0.002": 25, "+Inf": 25}' 0.28)" 1

# The phase boundary (P6a2bI-3): idle at once on an idle stack; a socket open
# on gw-0 keeps it busy, named, until the socket closes.
stack_wait_idle 5 && ok "idle: an idle stack" || bad "idle: an idle stack"
python3 -c 'import socket,sys,time; s=socket.create_connection(("127.0.0.1", int(sys.argv[1]))); time.sleep(3)' $((STACK_PORT_BASE + 100)) &
holder=$!
sleep 0.5
out=$(stack_wait_idle 1 2>&1) && bad "idle: an open socket must keep it busy" || ok "idle: an open socket keeps it busy"
contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
contains "idle: names the gateway" "$out" "gw-0: connections 1"
wait $holder
stack_wait_idle 10 && ok "idle: after the socket closed" || bad "idle: after the socket closed"

# Loadgen runs with LG_ENV (GOMEMLIMIT, #93); its logins settle on the
# gateways after it stops (P6b1I-4); Redis's per-command latency since a
# reset covers the run's commands and none of the readers' own.
stack_redis_resetstat && ok "redis: resetstat" || bad "redis: resetstat"
LG_ENV=(GOMEMLIMIT=1GiB)
STACK_PIN=0 # loadgen's CPU set is the Tier 2 host's
lg_start t -mode light -clients 4 -rate 100 -uid-prefix st && lg_wait_online 4 20 && ok "loadgen: 4 online" || bad "loadgen: not online"
STACK_PIN=1
expect "loadgen: runs with LG_ENV" "$(tr '\0' '\n' <"/proc/$LG_PID/environ" | grep '^GOMEMLIMIT=')" GOMEMLIMIT=1GiB
# Each login is one sample of a gateway's login_seconds histogram, its
# buckets in Prometheus's order.
b0=$(stack_buckets gw-0 goim_gateway_login_seconds) b1=$(stack_buckets gw-1 goim_gateway_login_seconds)
expect "buckets: one sample per login, in order" "$(python3 -c 'import json,sys; a, b = (json.loads(x) for x in sys.argv[1:3]); print(a["+Inf"] + b["+Inf"], list(a)[0], list(a)[-1])' "$b0" "$b1")" "4 0.0005 +Inf"
expect "buckets: no such histogram" "$(stack_buckets gw-0 goim_gateway_no_such_seconds)" null
expect "buckets: no such process" "$(stack_buckets gw-9 goim_gateway_login_seconds)" null
lat=$(stack_redis_latency)
expect "redis: latency of the logins' commands" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(any(v["calls"] > 0 and v["p99_us"] > 0 for v in d.values()), "info" in d, any(k.startswith("config") for k in d))' "$lat")" "True False False"
expect "redis: no answer is null" "$(STACK_PORT_BASE=1; stack_redis_latency)" null
lg_stop
stack_wait_idle 30 && ok "idle: the logins' Logouts settle" || bad "idle: after loadgen"
expect "idle: one Logout done per login" "$(stack_metric gw-0 goim_gateway_logouts_total 'result="done"')" "$(stack_metric gw-0 goim_gateway_logins_total 'result="ok"')"
LG_ENV=()

# stack_usage: CPU and RSS per process over a window, extras included; a
# process gone since is null.
sleep 30 & extra=$!
stack_usage_start extra=$extra gone=999999999
sleep 1
kill $extra; wait $extra 2>/dev/null
usage=$(stack_usage_json)
expect "usage: every process, the extras, and the window" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(sorted(d), 0.9 < d["window_s"] < 3, d["gw-0"]["rss_mb"] > 0, d["gw-0"]["cpu_s"] >= 0, d["gone"], d["extra"])' "$usage")" \
  "['devtoken', 'extra', 'gone', 'gw-0', 'gw-1', 'logic', 'redis', 'window_s'] True True True None None"
# The peak (VmHWM, P6b1I-3) is at least the current RSS.
expect "usage: peak RSS" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(all(d[n]["rss_peak_mb"] >= d[n]["rss_mb"] > 0 for n in ("gw-0", "logic", "redis")))' "$usage")" True

stack_profile cpu 1
# A failed profile fails stack_profile, even when it is not the last one
# (Logic's comes first; 6a2a final review).
(curl() { case "$*" in *":$((STACK_PORT_BASE + 4))/"*) return 22 ;; esac; command curl "$@"; }
 stack_profile failed 1) && bad "profile: a failed profile is not reported" || ok "profile: a failed profile fails"
for f in logic gw-0 gw-1; do
  gzip -t "$STACK_DIR/artifacts/cpu/$f.cpu.pb.gz" 2>/dev/null && ok "profile: $f CPU profile" || bad "profile: $f CPU profile"
done

# A second stack on the same ports is refused before it starts anything.
(STACK_DIR=$(mktemp -d); STACK_ORDER=(); stack_up 2>"$STACK_DIR/err"; echo $? >"$STACK_DIR/rc"; cat "$STACK_DIR/err" "$STACK_DIR/rc") >"$STACK_DIR/second" 2>&1
grep -q "is in use" "$STACK_DIR/second" && ok "up: busy ports are refused" || bad "up: busy ports: $(cat "$STACK_DIR/second")"
grep -q "port $STACK_PORT_BASE is in use by .*redis-server" "$STACK_DIR/second" && ok "up: names the holder" || bad "up: holder not named: $(cat "$STACK_DIR/second")"
grep -q "stop" "$STACK_DIR/second" && ok "up: says to stop it" || bad "up: no advice: $(cat "$STACK_DIR/second")"
grep -q "STACK_PORT_BASE" "$STACK_DIR/second" && bad "up: still advises another STACK_PORT_BASE" || ok "up: no port-base advice"
busy=$(stack_busy_ports)
expect "busy: every port of the stack" "$(echo "$busy" | wc -l)" 11

declare -F stack_health >/dev/null && ok "health: stack_health defined" || bad "health: stack_health not defined"
expect "health: a running healthy stack has no problems" "$(stack_health 2>&1)" ""

pids=("${STACK_PIDS[@]}")
stack_down
expect "health: every process's exit status recorded" "$(wc -l <"$STACK_DIR/exit-status" 2>/dev/null)" 5
expect "health: a clean stop has no problems" "$(stack_health 2>&1)" ""
left=0
for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
expect "down: no process left" "$left" 0
stack_port_free "$STACK_PORT_BASE" && stack_port_free $((STACK_PORT_BASE + 100)) && ok "down: ports free" || bad "down: ports still in use"
# Gateways stop before Logic (#78), so a drain keeps its lease renewals and
# ends its epoch on Logic.
for name in gw-0 gw-1; do
  lost=$(grep -m 1 -E "lease renewal failed|EpochEnded failed" "$STACK_DIR/logs/$name.log")
  expect "down: $name drained with Logic up" "$lost" ""
done

# STACK_GATEWAY_PROCS overrides the share (one gateway alone would get every CPU).
STACK_GATEWAYS=1 STACK_GATEWAY_PROCS=1
if stack_up; then
  grep -q " loops=1 " "$STACK_DIR/logs/gw-0.log" && ok "procs: STACK_GATEWAY_PROCS=1 runs 1 loop" || bad "procs: override: $(grep -o ' loops=[0-9]* ' "$STACK_DIR/logs/gw-0.log")"
else
  bad "up: second stack"
fi
# A gateway that dies during the run is reported, and stays reported after the stop.
kill -KILL "${STACK_PIDS[gw-0]}"
sleep 0.2
# A stack_down cut short (a signal) is finished by the exit hook's: each
# process is recorded once (M2).
stack_term 5 devtoken
grep -q "^gw-0: not running" <<<"$(stack_health)" && ok "health: a dead gateway is reported" || bad "health: dead gateway: [$(stack_health 2>&1)]"
stack_down
grep -q "^gw-0: .*SIGKILL" <<<"$(stack_health)" && ok "health: its stop is reported unclean" || bad "health: killed gateway after stop: [$(stack_health 2>&1)]"
expect "health: only the dead gateway" "$(stack_health | grep -v '^gw-0:')" ""
expect "health: each process recorded once" "$(cut -d' ' -f1 "$STACK_DIR/exit-status" | sort | uniq -d)" ""
echo "panic: runtime error: test" >>"$STACK_DIR/logs/logic.log"
grep -q "^logic: panic: runtime error: test" <<<"$(stack_health)" && ok "health: a panic in a log is reported" || bad "health: panic: [$(stack_health 2>&1)]"

# A refused stack_up leaves no earlier stack's statuses or logs to report (M5).
python3 -c 'import socket,sys,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(); time.sleep(30)' "$STACK_PORT_BASE" &
holder=$!
sleep 0.5
STACK_GATEWAYS=1
stack_up 2>/dev/null && bad "up: should refuse the held port" || ok "up: refused while the port is held"
expect "health: a refused stack_up reports no earlier stack" "$(stack_health 2>&1)" ""
kill $holder; wait $holder 2>/dev/null

# A gateway killed on purpose (loadgen's /kill, #84) is reaped and is not a
# problem; a running one is refused.
STACK_GATEWAYS=2
if stack_up; then
  t=$SECONDS
  stack_expect_dead gw-1 1 2>/dev/null && bad "dead: a running gateway is refused" || ok "dead: a running gateway is refused"
  [ $((SECONDS - t)) -le 3 ] && ok "dead: SECONDS bounds the wait" || bad "dead: waited $((SECONDS - t))s for 1"
  kill -KILL "${STACK_PIDS[gw-1]}"
  stack_expect_dead gw-1 && ok "dead: a killed gateway is reaped" || bad "dead: reap"
  expect "dead: not a problem while the stack runs" "$(stack_health 2>&1)" ""
  stack_snapshot after-kill
  [ -e "$STACK_DIR/artifacts/after-kill/gw-1.metrics" ] && bad "dead: snapshot skips it" || ok "dead: snapshot skips it"
  stack_down
  expect "dead: recorded as expected" "$(grep '^gw-1 ' "$STACK_DIR/exit-status")" "gw-1 expected:137"
  expect "dead: not a problem after the stop" "$(stack_health 2>&1)" ""
  echo "panic: runtime error: before the kill" >>"$STACK_DIR/logs/gw-1.log"
  grep -q "^gw-1: panic: runtime error: before the kill" <<<"$(stack_health)" && ok "dead: a panic in its log is still reported" || bad "dead: panic not reported: [$(stack_health 2>&1)]"
  # Only the SIGKILL is expected: a gateway that exited by itself just
  # before the kill is still reported (6a2a final review).
  echo "gw-0 expected:1" >>"$STACK_DIR/exit-status"
  grep -q "^gw-0: expected a kill, exited with status 1$" <<<"$(stack_health)" && ok "dead: another status is reported" || bad "dead: other status: [$(stack_health 2>&1)]"
  sed -i '$d' "$STACK_DIR/exit-status"
  # stack_profile with nothing up returns at once (a bare wait would wait for
  # every child), and a profile that fails fails it.
  start=$SECONDS
  (sleep 5 & stack_profile none 1) && ok "profile: nothing up returns" || bad "profile: nothing up failed"
  [ $((SECONDS - start)) -lt 3 ] && ok "profile: nothing up does not wait" || bad "profile: nothing up waited $((SECONDS - start))s"
else
  bad "up: expect-dead stack"
fi

# Pinning without lib.sh is refused, not run with GOMAXPROCS 1 (M6).
out=$(unset -f t2_cpulist_expand; STACK_PIN=1; stack_up 2>&1; echo "rc=$?"; stack_down)
contains "pin: without lib.sh is refused" "$out" "rc=1"
contains "pin: says lib.sh is needed" "$out" "lib.sh"
expect "pin: nothing started" "$(grep -c 'Ready\|serving' <<<"$out")" 0

[ "$fails" -eq 0 ] && echo "stack_test: all passed" || { echo "stack_test: $fails failed"; exit 1; }
