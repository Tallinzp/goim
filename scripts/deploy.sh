#!/usr/bin/env bash
# deploy.sh: a single-node goim stack for a real client app (Ubuntu and macOS):
# Redis, devtoken, Logic and one gateway as detached processes, with keys and
# data kept in $GOIM_HOME. Run it as `scripts/deploy.sh up|down|status|logs|token`
# (make deploy, deploy-down, ...) or source it to call the functions.
#
# It runs under the bash 3.2 of macOS and uses only commands both systems have
# (scripts/deploy_test.sh lints for the ones that differ). stack.sh is for
# load runs and is not reused: it makes new keys at every start, listens on
# 127.0.0.1 only and keeps its processes as children of one shell.
#
# Settings (environment):
#   GOIM_HOME            data directory (default ~/.goim): bin/ keys/ logs/ run/ redis/
#   GOIM_PORT_BASE       Redis +0, devtoken +1, Logic gRPC +2, lease +3, Logic admin +4;
#                        gateway client +100, Kick +101, admin +102 (default 18000)
#   GOIM_LISTEN_HOST     gateway client listen address (default 0.0.0.0); all else is 127.0.0.1
#   GOIM_ADVERTISE_HOST  address printed for the app (default: the LAN address)
#   GOIM_REDIS_SERVER    redis-server binary (default: on PATH, else .cache/redis/bin)
#   GOIM_DOWN_WAIT       seconds the gateway may drain at stop (default 60)
#   GOIM_DRAIN_WINDOW    gateway -drain-window (default 5s: one node needs no long spread)
#   GOIM_MAX_CONNS       gateway connection cap (default 5000)
#   GOIM_TOKEN_TTL, GOIM_APPID, GOIM_PLATFORM   token lifetime, appid, platform (24h, im, mobile)

D_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

deploy_config() {
  D_HOME=${GOIM_HOME:-$HOME/.goim}
  D_BASE=${GOIM_PORT_BASE:-18000}
  D_HOST=${GOIM_LISTEN_HOST:-0.0.0.0}
  if [ -n "${GOIM_ADVERTISE_HOST:-}" ]; then
    D_ADV=$GOIM_ADVERTISE_HOST
  elif [ "$D_HOST" != 0.0.0.0 ]; then
    D_ADV=$D_HOST
  else
    D_ADV=$(deploy_lan_ip)
  fi
  D_REDIS=${GOIM_REDIS_SERVER:-$(command -v redis-server || echo "$D_ROOT/.cache/redis/bin/redis-server")}
  D_WAIT=${GOIM_DOWN_WAIT:-60}
  D_TTL=${GOIM_TOKEN_TTL:-24h}
  D_APPID=${GOIM_APPID:-im}
  D_DRAIN=${GOIM_DRAIN_WINDOW:-5s}
  D_MAXCONNS=${GOIM_MAX_CONNS:-5000}
}

# deploy_lan_ip: this machine's LAN IPv4, else 127.0.0.1 with a warning.
deploy_lan_ip() {
  local ip="" ifc
  if command -v ip >/dev/null 2>&1; then
    ip=$(ip route get 1.1.1.1 2>/dev/null | sed -n 's/.* src \([0-9.]*\).*/\1/p' | head -n 1)
  fi
  if [ -z "$ip" ] && command -v route >/dev/null 2>&1 && command -v ipconfig >/dev/null 2>&1; then
    ifc=$(route -n get default 2>/dev/null | sed -n 's/^ *interface: *//p' | head -n 1)
    [ -n "$ifc" ] && ip=$(ipconfig getifaddr "$ifc" 2>/dev/null)
  fi
  if [ -z "$ip" ]; then
    echo "goim: no LAN address found, using 127.0.0.1" >&2
    ip=127.0.0.1
  fi
  echo "$ip"
}

# deploy_preflight: one install hint per missing tool; 1 if any is missing.
deploy_preflight() {
  local bad=0
  command -v go >/dev/null 2>&1 || { echo "goim: go is missing (install Go 1.27: go.dev/dl)" >&2; bad=1; }
  command -v cc >/dev/null 2>&1 || { echo "goim: no C compiler (Ubuntu: apt install build-essential; macOS: xcode-select --install)" >&2; bad=1; }
  command -v curl >/dev/null 2>&1 || { echo "goim: curl is missing" >&2; bad=1; }
  [ -x "$D_REDIS" ] || { echo "goim: redis-server not found (Ubuntu: apt install redis-server; macOS: brew install redis; or make redis-server)" >&2; bad=1; }
  return $bad
}

# deploy_port_free PORT: true when a connect to 127.0.0.1:PORT is refused.
deploy_port_free() { ! (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# deploy_spawn NAME CMD...: starts CMD detached, output to logs/NAME.log.
# run/NAME.pid holds "PID COMM"; a supervising subshell writes run/NAME.status
# (CMD's exit status) and removes the pid file when CMD ends. Returns once
# the pid file exists; 1 if CMD ended before that.
deploy_spawn() {
  local name=$1 log pidf stf i
  shift
  mkdir -p "$D_HOME/logs" "$D_HOME/run"
  log=$D_HOME/logs/$name.log pidf=$D_HOME/run/$name.pid stf=$D_HOME/run/$name.status
  rm -f "$pidf" "$stf"
  (
    trap '' HUP # a closed terminal must not stop the node
    "$@" </dev/null >>"$log" 2>&1 &
    child=$!
    echo "$child $(basename "$1")" >"$pidf.tmp" && mv "$pidf.tmp" "$pidf"
    wait "$child"
    echo $? >"$stf"
    rm -f "$pidf"
  ) </dev/null >>"$log" 2>&1 &
  for i in $(seq 50); do
    [ -f "$pidf" ] && return 0
    [ -f "$stf" ] && return 1
    sleep 0.1
  done
  return 1
}

# deploy_alive NAME: true if NAME's pid is alive and still has its command
# name. A pid file that fails the check is stale: removed, and nothing
# is signalled.
deploy_alive() {
  local f=$D_HOME/run/$1.pid pid comm cur
  [ -f "$f" ] || return 1
  read -r pid comm <"$f"
  if kill -0 "$pid" 2>/dev/null; then
    cur=$(ps -p "$pid" -o comm= 2>/dev/null)
    cur=${cur##*/}
    [ "${cur:0:15}" = "${comm:0:15}" ] && return 0
  fi
  rm -f "$f"
  echo "$1: stale pid file removed" >&2
  return 1
}

# deploy_stop NAME WAIT: SIGTERM, then up to WAIT seconds for the exit status.
# 0: not running, or a clean exit (0; devtoken has no SIGTERM handler, so 143
# too). 1: anything else; a process that outlives WAIT is left running unless
# FORCE=1.
deploy_stop() {
  local name=$1 wait=$2 pid st i
  deploy_alive "$name" || return 0
  read -r pid _ <"$D_HOME/run/$name.pid"
  rm -f "$D_HOME/run/$name.status"
  kill -TERM "$pid" 2>/dev/null
  for ((i = 0; i < wait * 10; i++)); do
    [ -f "$D_HOME/run/$name.status" ] && break
    sleep 0.1
  done
  if [ ! -f "$D_HOME/run/$name.status" ]; then
    echo "$name: still running after ${wait}s"
    if [ "${FORCE:-}" = 1 ]; then
      kill -KILL "$pid" 2>/dev/null
      for ((i = 0; i < 50; i++)); do
        [ -f "$D_HOME/run/$name.status" ] && break
        sleep 0.1
      done
    fi
    return 1
  fi
  st=$(cat "$D_HOME/run/$name.status")
  if [ "$st" = 0 ] || { [ "$name" = devtoken ] && [ "$st" = 143 ]; }; then
    echo "$name: stopped"
    return 0
  fi
  echo "$name: exited with status $st"
  return 1
}

# deploy_ready NAME URL SECONDS: waits for URL to answer; 1 (with the last
# log lines) if NAME dies or the time ends.
deploy_ready() {
  local name=$1 url=$2 secs=$3 i
  for ((i = 0; i < secs * 10; i++)); do
    if ! deploy_alive "$name"; then
      echo "goim: $name exited; last log lines:" >&2
      tail -n 20 "$D_HOME/logs/$name.log" >&2
      return 1
    fi
    curl -sf -o /dev/null "$url" && return 0
    sleep 0.1
  done
  echo "goim: $name not ready at $url within ${secs}s; last log lines:" >&2
  tail -n 20 "$D_HOME/logs/$name.log" >&2
  return 1
}
