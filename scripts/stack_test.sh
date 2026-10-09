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
T2_CPUS_GATEWAY=$(($(getconf _NPROCESSORS_ONLN) - 1)) # the last CPU
T2_CPUS_LOGIC=0
STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000 -loops 1)

stack_build || { echo "build failed"; exit 1; }
if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi

. "$STACK_DIR/stack.env"
expect "env: two gateway addresses" "$GATEWAY_ADDRS" "127.0.0.1:$((STACK_PORT_BASE + 100)),127.0.0.1:$((STACK_PORT_BASE + 110))"
expect "env: Noise public key is 32 bytes" "$(printf '%s' "$NOISE_PUBLIC" | base64 -d 2>/dev/null | wc -c)" 32
[ -s "$JWT_KEY" ] && ok "env: token key" || bad "env: token key missing"
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

pids=("${STACK_PIDS[@]}")
stack_down
left=0
for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
expect "down: no process left" "$left" 0
stack_port_free "$STACK_PORT_BASE" && stack_port_free $((STACK_PORT_BASE + 100)) && ok "down: ports free" || bad "down: ports still in use"

[ "$fails" -eq 0 ] && echo "stack_test: all passed" || { echo "stack_test: $fails failed"; exit 1; }
