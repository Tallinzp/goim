#!/usr/bin/env bash
# Tier 2: the load-test foundation on the Tier 2 host (Plan 6a1), before any
# load: the host setup is in place, a stack of 4 gateways comes up pinned by
# core type (#71), the run is recorded (#70, #71), artifacts are written, and
# goim-netdrop really silences a gateway port and restores it (#68, for b2).
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=foundation
#   Results: tier2-results/foundation-<timestamp>/ (result.json, stack/ with
#   logs and artifacts), plus ONE file to send back:
#   tier2-results/foundation-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#
# Checks:
#   1. stack:      redis, devtoken, Logic and gw-0..gw-3 become Ready
#   2. pinning:    gateways on T2_CPUS_GATEWAY, the rest on T2_CPUS_LOGIC
#   3. artifacts:  a metrics snapshot of every process and Redis INFO
#   4. netdrop:    with goim-netdrop add, a connect to gw-0 times out; after
#                  del it connects again, and no goim-netdrop rule is left
#   5. no_swap_in: no page swapped in during the 30s idle window
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
t2_init foundation
t2_need curl taskset sudo
t2_require_host_setup
t2_mem_budget 4
t2_nofile 1048576

export STACK_DIR=$T2_OUT/stack STACK_GATEWAYS=4 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
t2_on_exit 'stack_down'
t2_on_exit 'sudo -n /usr/local/sbin/goim-netdrop flush'

t2_report_extra() {
  t2_section "host setup"
  for k in net.netfilter.nf_conntrack_max net.core.somaxconn net.ipv4.tcp_mem; do
    echo "$k = $(cat "/proc/sys/${k//.//}" 2>/dev/null)"
  done
  echo "nofile: $(ulimit -Sn) / $(ulimit -Hn)"
  t2_section "background-before.txt"
  cat "$T2_OUT/background-before.txt" 2>/dev/null
  local f
  for f in "$STACK_DIR"/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "$(basename "$f") (last 30 lines)"
    tail -n 30 "$f"
  done
}

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

# 1: the stack.
stack_up 2>"$T2_OUT/stack-up.err"
up=$?
t2_check stack "$(t2_bool [ $up -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"error\": $(t2_json_str "$(cat "$T2_OUT/stack-up.err")")"
[ $up -eq 0 ] || t2_finish

# 2: pinning.
wrong=""
for name in "${STACK_ORDER[@]}"; do
  case $name in gw-*) want=$T2_CPUS_GATEWAY ;; *) want=$T2_CPUS_LOGIC ;; esac
  stack_pinned "$name" "$want" || wrong+="${wrong:+ }$name"
done
t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": \"$T2_CPUS_GATEWAY\", \"logic_cpus\": \"$T2_CPUS_LOGIC\", \"wrong\": $(t2_json_str "$wrong")"

# 5 (window): 30s idle, recorded.
t2_record_start
sleep 30 & wait $! # a SIGTERM runs the trap now, not after the sleep
t2_record_end

# 3: artifacts.
stack_snapshot idle
missing=""
for f in logic gw-0 gw-1 gw-2 gw-3; do
  grep -q '^# TYPE' "$STACK_DIR/artifacts/idle/$f.metrics" 2>/dev/null || missing+="${missing:+ }$f"
done
grep -q '^redis_version:' "$STACK_DIR/artifacts/idle/redis.info" 2>/dev/null || missing+="${missing:+ }redis"
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "$missing")"

# 4: netdrop on gw-0's client port.
port=$((STACK_PORT_BASE + 100))
connects() { timeout 3 bash -c "exec 3<>/dev/tcp/127.0.0.1/$port" 2>/dev/null; }
before=$(t2_bool connects)
sudo -n /usr/local/sbin/goim-netdrop add "$port"
dropped=$(t2_bool connects)
sudo -n /usr/local/sbin/goim-netdrop del "$port"
after=$(t2_bool connects)
left=$(sudo -n /usr/local/sbin/goim-netdrop list | wc -l)
t2_check netdrop "$(t2_bool [ "$before$dropped$after$left" = truefalsetrue0 ])" \
  "\"connects_before\": $before, \"connects_while_dropped\": $dropped, \"connects_after\": $after, \"rules_left\": $left"

t2_not_covered "load: loadgen is Plan 6a2" "Redis Cluster (Plan 6c)"
t2_finish
