# shellcheck shell=bash
# loadgen.sh: run cmd/loadgen against a scripts/stack.sh stack (Plan 6a2).
# Source it after stack.sh; stack_build has built bin/loadgen. Then:
#
#   lg_start NAME ARGS...       # loadgen in the background, reading $STACK_DIR/stack.env
#   lg_wait_online N SECONDS    # until the ramp is done and N clients are logged in
#   lg_get PATH EXPR            # EXPR (python, over the JSON as d) of GET PATH on the control API
#   lg_post PATH                # POST PATH; prints the reply, fails on an HTTP error
#   lg_wait_back MARK SECONDS   # until every client of MARK logged in again
#   lg_profile NAME SECONDS     # loadgen's CPU profile over SECONDS -> $STACK_DIR/artifacts/NAME/loadgen.cpu.pb.gz
#   lg_stop                     # SIGTERM, then wait (60s) for it to write its result; LG_STATUS = exit status
#   lg_json EXPR                # EXPR over the result file of the last loadgen
#   lg_true SOURCE EXPR         # "true" if EXPR is true over SOURCE (a file, or - for stdin), else "false"
#
# Values print as JSON (null when the JSON cannot be read, e.g. loadgen died
# before writing it), so they can go straight into t2_check members.
#
# Logs go to $STACK_DIR/logs/loadgen-NAME.log, the result (final stats and
# every mark's SLO) to $STACK_DIR/artifacts/loadgen-NAME.json. The control
# API listens on STACK_PORT_BASE + 5, one of stack_ports, so a leftover
# loadgen there is refused before the stack starts. With STACK_PIN=1
# loadgen is pinned to T2_CPUS_LOADGEN (choice #71). Like stack.sh's processes, loadgen runs in
# its own process group, added to T2_OURS_PGIDS.

LG_PID=""
LG_NAME=""
LG_STATUS=""

lg_control() { echo "127.0.0.1:$((${STACK_PORT_BASE:-17000} + 5))"; }

lg_start() {
  LG_NAME=$1
  shift
  local cmd=("$STACK_DIR/bin/loadgen" -env "$STACK_DIR/stack.env" -control "$(lg_control)"
    -out "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$@")
  if [ "${STACK_PIN:-0}" = 1 ]; then cmd=(taskset -c "$T2_CPUS_LOADGEN" "${cmd[@]}"); fi
  mkdir -p "$STACK_DIR/artifacts" "$STACK_DIR/logs"
  rm -f "$STACK_DIR/artifacts/loadgen-$LG_NAME.json"
  setsid "${cmd[@]}" >"$STACK_DIR/logs/loadgen-$LG_NAME.log" 2>&1 &
  LG_PID=$!
  T2_OURS_PGIDS+=("$LG_PID")
  local i
  for i in $(seq 100); do
    curl -sf -o /dev/null "http://$(lg_control)/stats" && return 0
    kill -0 "$LG_PID" 2>/dev/null || break
    sleep 0.1
  done
  echo "loadgen: control API not up; last log lines:" >&2
  tail -n 20 "$STACK_DIR/logs/loadgen-$LG_NAME.log" >&2
  return 1
}

# lg_py FILE EXPR: EXPR evaluated with d = the JSON in FILE (- for stdin),
# printed as JSON; null if the file or EXPR fails.
lg_py() {
  python3 -c 'import json, sys
try:
    d = json.load(sys.stdin if sys.argv[1] == "-" else open(sys.argv[1]))
    print(json.dumps(eval(sys.argv[2], {"d": d})))
except Exception:
    print("null")' "$1" "$2"
}

lg_true() { [ "$(lg_py "$1" "bool($2)")" = true ] && echo true || echo false; }

lg_get() { curl -sf "http://$(lg_control)$1" | lg_py - "$2"; }

lg_post() { curl -sf -X POST "http://$(lg_control)$1"; }

lg_wait_online() {
  local i
  for i in $(seq $(($2 * 2))); do
    [ "$(lg_get /stats 'd["ramp_done"] and d["online"] >= '"$1")" = true ] && return 0
    kill -0 "$LG_PID" 2>/dev/null || { echo "loadgen exited" >&2; return 1; }
    sleep 0.5
  done
  echo "loadgen: $(lg_get /stats 'd["online"]') of $1 online after $2s; failed: $(lg_get /stats 'd["failed"]')" >&2
  return 1
}

lg_wait_back() {
  local i
  for i in $(seq $(($2 * 2))); do
    [ "$(lg_get "/slo?mark=$1&within=30s" 'd["back"] == d["population"]')" = true ] && return 0
    sleep 0.5
  done
  return 1
}

# lg_profile NAME SECONDS: loadgen's own CPU profile (its control API serves
# pprof), beside stack_profile's, so a run shows whether loadgen was the
# bottleneck (P6a2aI-4). Blocks for SECONDS.
lg_profile() {
  mkdir -p "$STACK_DIR/artifacts/$1"
  curl -sf -m $(($2 + 30)) "http://$(lg_control)/debug/pprof/profile?seconds=$2" >"$STACK_DIR/artifacts/$1/loadgen.cpu.pb.gz"
}

# lg_stop sets LG_STATUS rather than printing it: in $(...) it would run in a
# subshell, which cannot wait for the loadgen process.
lg_stop() {
  [ -n "$LG_PID" ] || return 0
  kill -TERM "$LG_PID" 2>/dev/null
  local i
  for i in $(seq 600); do
    kill -0 "$LG_PID" 2>/dev/null || break
    sleep 0.1
  done
  kill -KILL "$LG_PID" 2>/dev/null
  wait "$LG_PID" 2>/dev/null
  LG_STATUS=$?
  LG_PID=""
}

lg_json() { lg_py "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$1"; }
