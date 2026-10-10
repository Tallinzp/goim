#!/usr/bin/env bash
# Tests for scripts/stack.sh on any machine (make test-scripts): a real stack
# of two gateways comes up Ready, pinned when asked, writes stack.env and a
# metrics snapshot, and stack_down leaves no process and no port behind.
# Needs redis-server (make redis-server) and curl.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/tier2/lib.sh
. scripts/stack.sh
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
procs=$((ncpu / 2 > 1 ? ncpu / 2 : 1))

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
grep -q "^gw-0: not running" <<<"$(stack_health)" && ok "health: a dead gateway is reported" || bad "health: dead gateway: [$(stack_health 2>&1)]"
stack_down
grep -q "^gw-0: .*SIGKILL" <<<"$(stack_health)" && ok "health: its stop is reported unclean" || bad "health: killed gateway after stop: [$(stack_health 2>&1)]"
expect "health: only the dead gateway" "$(stack_health | grep -v '^gw-0:')" ""
echo "panic: runtime error: test" >>"$STACK_DIR/logs/logic.log"
grep -q "^logic: panic: runtime error: test" <<<"$(stack_health)" && ok "health: a panic in a log is reported" || bad "health: panic: [$(stack_health 2>&1)]"

[ "$fails" -eq 0 ] && echo "stack_test: all passed" || { echo "stack_test: $fails failed"; exit 1; }
