# shellcheck shell=bash
# loadgen.sh: run cmd/loadgen against a scripts/stack.sh stack (Plan 6a2).
# Source it after stack.sh; stack_build has built bin/loadgen. Then:
#
#   lg_start NAME ARGS...       # loadgen in the background, reading $STACK_DIR/stack.env,
#                               #   with LG_ENV (VAR=VALUE..., e.g. GOMEMLIMIT=16GiB, #93) in its environment
#   lg_wait_online N SECONDS    # until the ramp is done and N clients are logged in
#   lg_wait EXPR SECONDS        # until EXPR over GET /stats is true
#   lg_get PATH EXPR            # EXPR (python, over the JSON as d) of GET PATH on the control API
#   lg_post PATH                # POST PATH; prints the reply, fails on an HTTP error
#   lg_wait_back MARK SECONDS   # until every client of MARK logged in again
#   lg_profile NAME SECONDS     # loadgen's CPU profile over SECONDS -> $STACK_DIR/artifacts/NAME/loadgen.cpu.pb.gz
#                               #   (another instance's: loadgen-INST.cpu.pb.gz)
#   lg_stop                     # SIGTERM, then wait (60s) for it to write its result; LG_STATUS = exit status
#   lg_json EXPR                # EXPR over the result file of the last loadgen
#   lg_true SOURCE EXPR         # "true" if EXPR is true over SOURCE (a file, or - for stdin), else "false"
#   lg_in INST CMD...           # CMD (one of the above) on loadgen instance INST
#   lg_stop_all                 # lg_stop on every instance; LG_STATUSES[INST] = each exit status
#
# Values print as JSON (null when the JSON cannot be read, e.g. loadgen died
# before writing it), so they can go straight into t2_check members.
#
# Logs go to $STACK_DIR/logs/loadgen-NAME.log, the result (final stats and
# every mark's SLO) to $STACK_DIR/artifacts/loadgen-NAME.json. With
# STACK_PIN=1 loadgen is pinned to LG_CPUS, default T2_CPUS_LOADGEN (choice
# #71). Like stack.sh's processes, loadgen runs in its own process group,
# added to T2_OURS_PGIDS.
#
# Instances (P6a2bI-1): several loadgens can run on one stack, one mode
# each, e.g. a flood beside a legitimate population, or a light instance
# answering heartbeats beside an SDK storm. Every function above acts on the
# current instance, "main" unless inside lg_in; LG_PID, LG_NAME and
# LG_STATUS are the current instance's. Instance k (main is 0, the others
# numbered in the order lg_in first names them) has its control API on
# STACK_PORT_BASE + 5 + k, among stack_ports, so a leftover loadgen there is
# refused before the stack starts. A script names at most LG_MAX_INSTANCES
# instances in all, and a name is never freed: name instances by role
# (flood, flood2), not by phase, and start a stopped one again for the next
# phase. Each lg_start takes a NAME no lg_start took before (its log and
# result file; a used one is refused), and each instance its own
# -uid-prefix and, for a flood, -source-net (loadgen refuses a flood
# without one). LG_ENV and LG_CPUS are read by lg_start, so set them
# before each. A scenario with several instances registers
# t2_on_exit 'lg_stop_all', not 'lg_stop':
#
#   lg_start legit -mode light -clients 20000 -uid-prefix legit
#   LG_CPUS=0-1 lg_in flood lg_start garbage -mode garbage -rate 20000 -clients 5000 -source-net 1
#   lg_in flood lg_get /stats 'd["flood"]["outcomes"]'
#   lg_in flood lg_stop
#   lg_in flood lg_start slowhs -mode slow-handshake -clients 50000 -source-net 1
#   lg_stop_all

LG_PID=""
LG_NAME=""
LG_STATUS=""
LG_ENV=()
LG_INST=main
LG_INSTANCES=(main)
LG_MAX_INSTANCES=4 # control ports STACK_PORT_BASE + 5 … + 8 (stack_ports)
declare -A LG_PIDS=() LG_NAMES=() LG_STATUSES=()
declare -A LG_USED=() # NAME -> the instance whose lg_start took it (P6b3aI-2)

# lg_k INST: the instance's index; fails for an instance never named.
lg_k() {
  local i
  for i in "${!LG_INSTANCES[@]}"; do
    [ "${LG_INSTANCES[$i]}" = "$1" ] && { echo "$i"; return 0; }
  done
  return 1
}

lg_control() { echo "127.0.0.1:$((${STACK_PORT_BASE:-17000} + 5 + $(lg_k "$LG_INST")))"; }

# lg_in INST CMD...: CMD with INST as the current instance, then the
# previous one again. A name first seen here takes the next index, for
# good (#120, O4). Not in $(...) for lg_start or lg_stop: a subshell cannot
# keep the pid or wait for it.
lg_in() {
  local inst=$1 prev=$LG_INST rc
  shift
  if ! lg_k "$inst" >/dev/null; then
    [ ${#LG_INSTANCES[@]} -lt "$LG_MAX_INSTANCES" ] || { echo "loadgen: instance $inst: already $LG_MAX_INSTANCES instances" >&2; return 1; }
    LG_INSTANCES+=("$inst")
  fi
  LG_PIDS[$prev]=$LG_PID LG_NAMES[$prev]=$LG_NAME LG_STATUSES[$prev]=$LG_STATUS
  LG_INST=$inst LG_PID=${LG_PIDS[$inst]:-} LG_NAME=${LG_NAMES[$inst]:-} LG_STATUS=${LG_STATUSES[$inst]:-}
  "$@"
  rc=$?
  LG_PIDS[$inst]=$LG_PID LG_NAMES[$inst]=$LG_NAME LG_STATUSES[$inst]=$LG_STATUS
  LG_INST=$prev LG_PID=${LG_PIDS[$prev]:-} LG_NAME=${LG_NAMES[$prev]:-} LG_STATUS=${LG_STATUSES[$prev]:-}
  return $rc
}

lg_start() {
  local inst
  if [ -n "$LG_PID" ] && kill -0 "$LG_PID" 2>/dev/null; then
    echo "loadgen: instance $LG_INST already runs ($LG_NAME)" >&2
    return 1
  fi
  # A NAME is taken once per script, running or not: lg_start removes
  # loadgen-NAME.json, which would lose an earlier run's result.
  if [ -n "${LG_USED[$1]:-}" ]; then
    echo "loadgen: name $1 is instance ${LG_USED[$1]}'s" >&2
    return 1
  fi
  LG_USED[$1]=$LG_INST
  LG_NAME=$1
  shift
  local cmd=(env "${LG_ENV[@]}" "$STACK_DIR/bin/loadgen" -env "$STACK_DIR/stack.env" -control "$(lg_control)"
    -out "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$@")
  if [ "${STACK_PIN:-0}" = 1 ]; then cmd=(taskset -c "${LG_CPUS:-$T2_CPUS_LOADGEN}" "${cmd[@]}"); fi
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

# lg_get gives up after LG_CURL_TIMEOUT seconds (default 10), so a wedged
# loadgen reads as null rather than hanging the run.
lg_get() { curl -sf -m "${LG_CURL_TIMEOUT:-10}" "http://$(lg_control)$1" | lg_py - "$2"; }

lg_post() { curl -sf -X POST "http://$(lg_control)$1"; }

# The waits poll every 0.5s until a wall-clock deadline, not for a count of
# polls: a poll over 600k clients takes its own time (#103, run 22).
lg_wait_online() {
  local end=$((SECONDS + $2))
  while [ $SECONDS -lt $end ]; do
    [ "$(lg_get /stats 'd["ramp_done"] and d["online"] >= '"$1")" = true ] && return 0
    kill -0 "$LG_PID" 2>/dev/null || { echo "loadgen exited" >&2; return 1; }
    sleep 0.5
  done
  echo "loadgen: online and failed after $2s (want $1 online): $(lg_get /stats '[d["online"], d["failed"]]')" >&2
  return 1
}

lg_wait() {
  local end=$((SECONDS + $2))
  while [ $SECONDS -lt $end ]; do
    [ "$(lg_get /stats "bool($1)")" = true ] && return 0
    kill -0 "$LG_PID" 2>/dev/null || { echo "loadgen exited" >&2; return 1; }
    sleep 0.5
  done
  return 1
}

lg_wait_back() {
  local end=$((SECONDS + $2))
  while [ $SECONDS -lt $end ]; do
    [ "$(lg_get "/slo?mark=$1&within=30s" 'd["back"] == d["population"]')" = true ] && return 0
    sleep 0.5
  done
  return 1
}

# lg_profile NAME SECONDS: loadgen's own CPU profile (its control API serves
# pprof), beside stack_profile's, so a run shows whether loadgen was the
# bottleneck (P6a2aI-4). Blocks for SECONDS. Main's is loadgen.cpu.pb.gz,
# another instance's loadgen-INST.cpu.pb.gz (P6b3aI-1).
lg_profile() {
  local f=loadgen.cpu.pb.gz
  [ "$LG_INST" = main ] || f=loadgen-$LG_INST.cpu.pb.gz
  mkdir -p "$STACK_DIR/artifacts/$1"
  curl -sf -m $(($2 + 30)) "http://$(lg_control)/debug/pprof/profile?seconds=$2" >"$STACK_DIR/artifacts/$1/$f"
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
  LG_STATUSES[$LG_INST]=$LG_STATUS
  LG_PID=""
}

lg_stop_all() {
  local inst
  for inst in "${LG_INSTANCES[@]}"; do lg_in "$inst" lg_stop; done
}

lg_json() { lg_py "$STACK_DIR/artifacts/loadgen-$LG_NAME.json" "$1"; }
