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

# --- Task 2: a real node -------------------------------------------------------
export GOIM_PORT_BASE=18700 GOIM_LISTEN_HOST=127.0.0.1 GOIM_DOWN_WAIT=40
export GOIM_HOME="$TMP/node home"
deploy_config 2>/dev/null
B=$GOIM_PORT_BASE
no_pid_files() { ! ls "$GOIM_HOME"/run/*.pid >/dev/null 2>&1; }
all_ports_free() {
  local p
  for p in $B $((B + 1)) $((B + 2)) $((B + 3)) $((B + 4)) $((B + 100)) $((B + 101)) $((B + 102)); do
    deploy_port_free "$p" || { echo "port $p busy"; return 1; }
  done
}
pubkey() { sed -n 's/^ *noise public  *//p' "$1"; }

if [ ! -x "$D_REDIS" ]; then
  echo "skip node tests: no redis-server (make redis-server, or set GOIM_REDIS_SERVER)"
else
  python3 -c 'import socket,sys,time
s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("127.0.0.1", int(sys.argv[1]))); s.listen(); time.sleep(60)' $((B + 100)) &
  HOLD_PID=$!
  sleep 0.5
  deploy_up >"$TMP/up.out" 2>&1; rc=$?
  expect "up: busy port refuses" "$rc" 1
  grep -q "$((B + 100))" "$TMP/up.out" && ok "up: names the busy port" || bad "up: names the busy port"
  no_pid_files && deploy_port_free "$B" && ok "up: busy port starts nothing" || bad "up: busy port starts nothing"
  kill "$HOLD_PID"; HOLD_PID=; sleep 0.3

  ( ulimit -S -n 256; deploy_up ) >"$TMP/up.out" 2>&1; rc=$?
  expect "up: low file limit" "$rc" 0
  curl -sf -o /dev/null "http://127.0.0.1:$((B + 102))/readyz" && ok "up: low file limit, gateway ready" || { bad "up: low file limit, gateway ready"; tail -n 5 "$TMP/up.out"; }
  first_pub=$(pubkey "$TMP/up.out")
  deploy_down >/dev/null 2>&1

  deploy_up >"$TMP/up.out" 2>&1; rc=$?
  expect "up: node comes up" "$rc" 0
  deploy_status >"$TMP/st.out" 2>&1; rc=$?
  expect "status: all running and ready" "$rc" 0
  grep -Eq 'noise public  +[A-Za-z0-9+/]{43}=$' "$TMP/up.out" && ok "up: prints the noise public key" || bad "up: prints the noise public key"
  expect "up: same noise key as before the stop" "$(pubkey "$TMP/up.out")" "$first_pub"
  (exec 3<>"/dev/tcp/127.0.0.1/$((B + 100))") 2>/dev/null && ok "up: gateway accepts TCP" || bad "up: gateway accepts TCP"
  curl -sf -o /dev/null "http://127.0.0.1:$((B + 4))/readyz" && ok "up: logic ready" || bad "up: logic ready"

  before=$(cat "$GOIM_HOME"/run/*.pid | cksum; cat "$GOIM_HOME"/keys/noise/1 "$GOIM_HOME"/keys/jwt.jwk | cksum)
  deploy_up >"$TMP/up2.out" 2>&1; rc=$?
  after=$(cat "$GOIM_HOME"/run/*.pid | cksum; cat "$GOIM_HOME"/keys/noise/1 "$GOIM_HOME"/keys/jwt.jwk | cksum)
  expect "up: second run is a no-op" "$rc:$before" "0:$after"

  tok=$(deploy_token alice)
  claims=$(python3 -c 'import sys,json,base64
p=sys.argv[1].split(".")[1]; p+="="*(-len(p)%4)
c=json.loads(base64.urlsafe_b64decode(p)); print(c["sub"], c["appid"], int(c["exp"]-c["iat"]))' "$tok" 2>&1)
  expect "token: claims" "$claims" "alice im 86400"
  msg=$(deploy_token 2>&1); rc=$?
  expect "token: no uid" "$rc" 2
  msg=$(GOIM_HOME="$TMP/empty home" deploy_token alice 2>&1); rc=$?
  expect "token: before deploy" "$rc" 2
  case $msg in *"run make deploy first"*) ok "token: says what to do" ;; *) bad "token: says what to do: [$msg]" ;; esac

  deploy_down >"$TMP/down.out" 2>&1; rc=$?
  expect "down: graceful" "$rc" 0
  expect "down: gateway status" "$(cat "$GOIM_HOME/run/gateway.status")" 0
  expect "down: logic status" "$(cat "$GOIM_HOME/run/logic.status")" 0
  expect "down: redis status" "$(cat "$GOIM_HOME/run/redis.status")" 0
  case $(cat "$GOIM_HOME/run/devtoken.status") in 0 | 143) ok "down: devtoken status" ;; *) bad "down: devtoken status" ;; esac
  no_pid_files && ok "down: no pid file" || bad "down: no pid file"
  all_ports_free >/dev/null && ok "down: no listener" || bad "down: no listener: $(all_ports_free)"
  msg=$(deploy_down 2>&1); rc=$?
  expect "down: nothing running" "$rc" 0
  case $msg in *"no node is running"*) ok "down: says so" ;; *) bad "down: says so: [$msg]" ;; esac

  printf '%s\n' '#!/bin/bash' 'exit 1' >"$TMP/bin/badredis"
  chmod +x "$TMP/bin/badredis"
  (GOIM_REDIS_SERVER="$TMP/bin/badredis"; deploy_up) >"$TMP/up.out" 2>&1; rc=$?
  expect "up: failure rolls back" "$rc" 1
  no_pid_files && all_ports_free >/dev/null && ok "up: failure leaves nothing" || bad "up: failure leaves nothing"

  for sig in INT TERM; do
    rm -f "$GOIM_HOME"/run/*.status
    set -m
    scripts/deploy.sh up >"$TMP/sig.out" 2>&1 &
    UP=$!
    set +m
    for _ in $(seq 100); do [ -f "$GOIM_HOME/run/redis.pid" ] && break; sleep 0.1; done
    kill -$sig "$UP"
    wait "$UP" 2>/dev/null
    sleep 1
    no_pid_files && all_ports_free >/dev/null && ok "up: SIG$sig rolls back" || bad "up: SIG$sig rolls back: $(ls "$GOIM_HOME"/run 2>&1 | tr '\n' ' ')"
  done
fi

echo
if [ "$fails" -eq 0 ]; then echo "deploy_test: all passed"; else echo "deploy_test: $fails failure(s)"; exit 1; fi
