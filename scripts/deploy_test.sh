#!/usr/bin/env bash
# Tests for scripts/deploy.sh on any machine (make test-scripts): the process
# helpers with stand-in services, then a real single-node stack (Plan: single-node
# deploy). Needs curl and python3; the stack part needs redis-server
# (make redis-server, or GOIM_REDIS_SERVER).
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
. scripts/deploy.sh
TMP=$(mktemp -d)
export GOIM_HOME="$TMP/goim home"
trap 'deploy_cleanup_all; rm -rf "$TMP"' EXIT
fails=0
ok() { echo "ok   $1"; }
bad() { echo "FAIL $1"; fails=$((fails + 1)); }
expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }

# Whatever a failed test left running.
deploy_cleanup_all() {
  local f pid
  for f in "$GOIM_HOME"/run/*.pid; do
    [ -f "$f" ] || continue
    read -r pid _ <"$f"
    kill -KILL "$pid" 2>/dev/null
  done
  [ -n "${HOLD_PID:-}" ] && kill "$HOLD_PID" 2>/dev/null
}

# Stand-in services: one that traps TERM and exits 0, one that ignores TERM.
mkdir -p "$TMP/bin"
printf '%s\n' '#!/bin/bash' "trap 'exit 0' TERM" 'while :; do sleep 0.2; done' >"$TMP/bin/trapper"
printf '%s\n' '#!/bin/bash' "trap '' TERM" 'while :; do sleep 0.2; done' >"$TMP/bin/stubborn"
chmod +x "$TMP/bin/trapper" "$TMP/bin/stubborn"

# --- Task 1: helpers ---------------------------------------------------------
(
  unset GOIM_HOME GOIM_PORT_BASE GOIM_LISTEN_HOST GOIM_ADVERTISE_HOST GOIM_DOWN_WAIT GOIM_TOKEN_TTL GOIM_APPID GOIM_DRAIN_WINDOW
  HOME=$TMP deploy_config 2>/dev/null
  echo "$D_BASE $D_HOST $D_WAIT $D_TTL $D_APPID $D_DRAIN $D_HOME"
) >"$TMP/cfg"
expect "config: defaults" "$(cat "$TMP/cfg")" "18000 0.0.0.0 60 24h im 5s $TMP/.goim"

deploy_config 2>/dev/null
SECONDS=0
out=$(deploy_spawn sleeper sleep 300)
expect "spawn: returns at once, no inherited pipe" "$([ "$SECONDS" -lt 3 ] && echo fast)" fast
deploy_alive sleeper && ok "spawn: alive right after" || bad "spawn: alive right after"
expect "spawn: pid file has two fields" "$(wc -w <"$D_HOME/run/sleeper.pid" | tr -d ' ')" 2
deploy_spawn talker sh -c 'echo hello-from-talker; sleep 300'
sleep 0.3
grep -q hello-from-talker "$D_HOME/logs/talker.log" && ok "spawn: output in the log" || bad "spawn: output in the log"
deploy_stop talker 5 >/dev/null 2>&1

msg=$(deploy_stop sleeper 5); rc=$?
expect "stop: sleep dies from SIGTERM, not clean" "$rc" 1
case $msg in *"exited with status 143"*) ok "stop: reports 143" ;; *) bad "stop: reports 143: [$msg]" ;; esac
[ ! -f "$D_HOME/run/sleeper.pid" ] && ok "stop: pid file gone" || bad "stop: pid file gone"

deploy_spawn trapper "$TMP/bin/trapper"
msg=$(deploy_stop trapper 5); rc=$?
expect "stop: a TERM-trapping service stops clean" "$rc" 0

msg=$(deploy_stop nothing 5); rc=$?
expect "stop: not running is a no-op" "$rc:$msg" "0:"

deploy_spawn stubborn "$TMP/bin/stubborn"
msg=$(deploy_stop stubborn 1); rc=$?
case $msg in *"still running after 1s"*) ok "stop: timeout is reported" ;; *) bad "stop: timeout is reported: [$msg]" ;; esac
expect "stop: timeout returns 1" "$rc" 1
deploy_alive stubborn && ok "stop: timeout leaves the process" || bad "stop: timeout leaves the process"
FORCE=1 deploy_stop stubborn 1 >/dev/null 2>&1
deploy_alive stubborn && bad "stop: FORCE=1 kills" || ok "stop: FORCE=1 kills"

mkdir -p "$D_HOME/run"
echo "$$ sleep" >"$D_HOME/run/ghost.pid"
msg=$(deploy_alive ghost 2>&1); rc=$?
expect "alive: pid of an unrelated process is stale" "$rc" 1
[ ! -f "$D_HOME/run/ghost.pid" ] && ok "alive: stale pid file removed" || bad "alive: stale pid file removed"
kill -0 $$ && ok "alive: the unrelated process is untouched" || bad "alive: the unrelated process is untouched"

PORT=$((18900 + RANDOM % 50))
deploy_port_free "$PORT" && ok "port: free" || bad "port: free"
python3 -c 'import socket,sys,time
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(); time.sleep(60)' "$PORT" &
HOLD_PID=$!
sleep 0.5
deploy_port_free "$PORT" && bad "port: busy" || ok "port: busy"
kill "$HOLD_PID"; HOLD_PID=

empty=$(mktemp -d)
msg=$(PATH=$empty deploy_lan_ip 2>"$TMP/lan.err")
expect "lan ip: fallback" "$msg" 127.0.0.1
grep -q 'no LAN address' "$TMP/lan.err" && ok "lan ip: warns" || bad "lan ip: warns"

pat='declare -A|mapfile|\$\{[A-Za-z_]+(,,|\^\^)|\|&|&>>|setsid|taskset|timeout |readlink -f|sha256sum|(^|[^a-z])ss -'
if grep -nE "$pat" scripts/deploy.sh; then bad "lint: portability"; else ok "lint: portability"; fi
bash -n scripts/deploy.sh && ok "lint: bash -n" || bad "lint: bash -n"
if command -v shellcheck >/dev/null; then
  shellcheck -s bash scripts/deploy.sh scripts/deploy_test.sh && ok "lint: shellcheck" || bad "lint: shellcheck"
else
  echo "skip lint: shellcheck (not installed)"
fi

echo
if [ "$fails" -eq 0 ]; then echo "deploy_test: all passed"; else echo "deploy_test: $fails failure(s)"; exit 1; fi
