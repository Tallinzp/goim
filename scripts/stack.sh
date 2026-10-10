# shellcheck shell=bash
# stack.sh: a goim stack as local processes for load runs (Plan 6a1, P5bI-1):
# one redis-server, devtoken serving the JWKS, Logic and N gateways, built
# without -race and run at production timing. Tier 2 scenarios pin it by
# core type (choice #71); the Tier 1a load probe (Plan 6a2) runs it unpinned.
# Source it, set STACK_DIR, then:
#
#   stack_build                 # binaries into $STACK_DIR/bin
#   stack_up                    # keys, processes, waits until all are Ready
#   stack_snapshot NAME         # /metrics of each process + redis INFO -> $STACK_DIR/artifacts/NAME/
#   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
#   stack_health                # one line per problem: dead process, unclean stop, panic in a log
#
# Settings (environment, read by stack_up):
#   STACK_DIR            work dir (required): bin/, keys/, logs/, artifacts/, stack.env
#   STACK_GATEWAYS       gateways, ids gw-0.. (default 1)
#   STACK_PORT_BASE      redis +0, devtoken +1, Logic gRPC +2, lease +3, admin +4;
#                        gateway i: listen +100+10i, Kick +101+10i, admin +102+10i (default 17000:
#                        below the race suite's 20000-32000 and the ephemeral range)
#   STACK_PIN            1: taskset gateways to T2_CPUS_GATEWAY, Logic and Redis
#                        to T2_CPUS_LOGIC (lib.sh sets both); 0: no pinning (default)
#   STACK_REDIS_SERVER   default: redis-server on PATH, else .cache/redis/bin/redis-server
#   STACK_GATEWAY_PROCS  each gateway's GOMAXPROCS (default: max(1, CPUs / STACK_GATEWAYS),
#                        CPUs = T2_CPUS_GATEWAY when pinned, else all online; #79)
#   STACK_GATEWAY_ARGS, STACK_LOGIC_ARGS   arrays of extra flags
#   STACK_DRAIN_WINDOW   gateway -drain-window (default 1s, so stack_down's drain ends and
#                        exits 0 well inside STACK_DOWN_WAIT; a measured drain sets 60s)
#   STACK_DOWN_WAIT      seconds stack_down waits for the gateways to drain (default 10)
#
# stack.env (for loadgen, Plan 6a2) holds GATEWAY_ADDRS (comma-separated),
# NOISE_KEY_ID, NOISE_PUBLIC, JWT_KEY (the devtoken private key file), and
# the admin addresses. Every process is started from this shell, so it
# stays in the caller's process group (lib.sh counts that group as ours).

declare -A STACK_PIDS=()
STACK_ORDER=()

stack_build() {
  mkdir -p "$STACK_DIR/bin"
  go build -o "$STACK_DIR/bin/" ./cmd/gateway ./cmd/logic ./cmd/devtoken
}

# stack_start NAME CPUS CMD...: starts CMD in the background, logging to
# logs/NAME.log, pinned to CPUS when STACK_PIN=1.
stack_start() {
  local name=$1 cpus=$2
  shift 2
  if [ "${STACK_PIN:-0}" = 1 ]; then set -- taskset -c "$cpus" "$@"; fi
  "$@" >"$STACK_DIR/logs/$name.log" 2>&1 &
  STACK_PIDS[$name]=$!
  STACK_ORDER+=("$name")
}

# stack_wait NAME URL: waits up to 30s for URL to answer 200.
stack_wait() {
  local i
  for i in $(seq 300); do
    if ! kill -0 "${STACK_PIDS[$1]}" 2>/dev/null; then
      echo "stack: $1 exited; last log lines:" >&2
      tail -n 20 "$STACK_DIR/logs/$1.log" >&2
      return 1
    fi
    curl -sf -o /dev/null "$2" && return 0
    sleep 0.1
  done
  echo "stack: $1 not ready at $2 within 30s; last log lines:" >&2
  tail -n 20 "$STACK_DIR/logs/$1.log" >&2
  return 1
}

# stack_port_free PORT: true only if a connect is refused at once; a connect
# that hangs (a DROP rule) counts as busy instead of blocking for minutes.
stack_port_free() { timeout 2 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$0"' "$1" 2>/dev/null; [ $? -eq 1 ]; }

# stack_ports: every port stack_up listens on.
stack_ports() {
  local base=${STACK_PORT_BASE:-17000} i
  seq "$base" $((base + 4))
  for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
    echo $((base + 100 + 10 * i)) $((base + 101 + 10 * i)) $((base + 102 + 10 * i))
  done
}

# stack_busy_ports: one line per port of the stack in use, naming its holder
# when ss can see it (our own processes); nothing when all are free.
stack_busy_ports() {
  local p holder
  for p in $(stack_ports); do
    stack_port_free "$p" && continue
    holder=$(ss -Hltnp "sport = :$p" 2>/dev/null | grep -o 'users:(([^)]*))' | head -n 1)
    echo "port $p is in use${holder:+ by $holder}"
  done
}

stack_up() {
  : "${STACK_DIR:?set STACK_DIR}"
  local n=${STACK_GATEWAYS:-1} base=${STACK_PORT_BASE:-17000} bin=$STACK_DIR/bin i
  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
  local gw_cpus=${T2_CPUS_GATEWAY:-} logic_cpus=${T2_CPUS_LOGIC:-}
  if [ "${STACK_PIN:-0}" = 1 ] && { [ -z "$gw_cpus" ] || [ -z "$logic_cpus" ]; }; then
    echo "stack: STACK_PIN=1 needs T2_CPUS_GATEWAY and T2_CPUS_LOGIC" >&2
    return 1
  fi
  [ -x "$redis" ] || { echo "stack: $redis not found (make redis-server)" >&2; return 1; }
  local busy
  busy=$(stack_busy_ports)
  if [ -n "$busy" ]; then
    printf 'stack: %s\n' "$busy" >&2
    echo "stack: stop whatever holds them first (a leftover stack?); a second stack beside it distorts the numbers" >&2
    return 1
  fi
  mkdir -p "$STACK_DIR/keys/noise" "$STACK_DIR/logs" "$STACK_DIR/artifacts" "$STACK_DIR/redis"
  rm -f "$STACK_DIR/exit-status"

  # Each gateway's GOMAXPROCS is its CPU share (#79); -loops and the
  # handshake concurrency default from it, as in a pod with a CPU limit.
  local cpus procs=${STACK_GATEWAY_PROCS:-}
  if [ -z "$procs" ]; then
    if [ "${STACK_PIN:-0}" = 1 ]; then
      cpus=$(t2_cpulist_expand "$gw_cpus" | wc -w)
    else
      cpus=$(getconf _NPROCESSORS_ONLN)
    fi
    procs=$((cpus / n > 1 ? cpus / n : 1))
  fi

  # Keys: the Noise static key (key_id 1) and the token signing key.
  local pub
  rm -f "$STACK_DIR/keys/noise/1" "$STACK_DIR/keys/jwt.jwk"
  pub=$("$bin/gateway" noise-keygen -key-id 1 -out "$STACK_DIR/keys/noise/1") || return 1
  pub=${pub##* public }
  "$bin/devtoken" keygen -kid k1 -out "$STACK_DIR/keys/jwt.jwk" || return 1
  "$bin/devtoken" jwks -out "$STACK_DIR/keys/jwks.json" "$STACK_DIR/keys/jwt.jwk" || return 1

  local redis_addr=127.0.0.1:$base devtoken=127.0.0.1:$((base + 1))
  local grpc=127.0.0.1:$((base + 2)) lease=127.0.0.1:$((base + 3)) admin=127.0.0.1:$((base + 4))
  local kick="" addrs="" admins="$admin"
  for ((i = 0; i < n; i++)); do
    kick+="${kick:+,}\"gw-$i\": \"127.0.0.1:$((base + 101 + 10 * i))\""
  done
  echo "{$kick}" >"$STACK_DIR/kick.json"

  stack_start redis "$logic_cpus" "$redis" --port "$base" --bind 127.0.0.1 --dir "$STACK_DIR/redis" \
    --save "" --appendonly no --maxmemory-policy noeviction
  local pong=""
  for i in $(seq 100); do
    "$(dirname "$redis")/redis-cli" -p "$base" ping >/dev/null 2>&1 && { pong=1; break; }
    sleep 0.1
  done
  if [ -z "$pong" ]; then
    echo "stack: redis not answering on port $base within 10s; last log lines:" >&2
    tail -n 20 "$STACK_DIR/logs/redis.log" >&2
    return 1
  fi
  stack_start devtoken "$logic_cpus" "$bin/devtoken" serve -addr "$devtoken" -jwks "$STACK_DIR/keys/jwks.json"
  stack_wait devtoken "http://$devtoken/" || return 1
  stack_start logic "$logic_cpus" "$bin/logic" -redis "$redis_addr" -redis-cluster=false \
    -grpc-addr "$grpc" -lease-addr "$lease" -admin-addr "$admin" \
    -jwks-url "http://$devtoken/" -jwks-fallback "$STACK_DIR/keys/jwks.json" \
    -kick-addr-file "$STACK_DIR/kick.json" "${STACK_LOGIC_ARGS[@]}"
  stack_wait logic "http://$admin/readyz" || return 1
  for ((i = 0; i < n; i++)); do
    local listen=127.0.0.1:$((base + 100 + 10 * i)) gadmin=127.0.0.1:$((base + 102 + 10 * i))
    stack_start "gw-$i" "$gw_cpus" env GOMAXPROCS="$procs" "$bin/gateway" -id "gw-$i" -listen "tcp://$listen" \
      -noise-keys "$STACK_DIR/keys/noise" -logic "$grpc" -lease "$lease" \
      -kick-addr "127.0.0.1:$((base + 101 + 10 * i))" -admin-addr "$gadmin" \
      -drain-window "${STACK_DRAIN_WINDOW:-1s}" "${STACK_GATEWAY_ARGS[@]}"
    addrs+="${addrs:+,}$listen"
    admins+=",$gadmin"
  done
  for ((i = 0; i < n; i++)); do
    stack_wait "gw-$i" "http://127.0.0.1:$((base + 102 + 10 * i))/readyz" || return 1
  done
  cat >"$STACK_DIR/stack.env" <<ENV
GATEWAY_ADDRS=$addrs
NOISE_KEY_ID=1
NOISE_PUBLIC=$pub
JWT_KEY=$STACK_DIR/keys/jwt.jwk
REDIS_ADDR=$redis_addr
LOGIC_ADMIN=$admin
ADMIN_ADDRS=$admins
GATEWAY_PROCS=$procs
ENV
}

# stack_snapshot NAME: every process's /metrics and Redis INFO, for result
# artifacts (spec: Automation contract, a metrics snapshot).
stack_snapshot() {
  local dir=$STACK_DIR/artifacts/$1 base=${STACK_PORT_BASE:-17000} i
  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
  mkdir -p "$dir"
  curl -sf "http://127.0.0.1:$((base + 4))/metrics" >"$dir/logic.metrics"
  for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
    curl -sf "http://127.0.0.1:$((base + 102 + 10 * i))/metrics" >"$dir/gw-$i.metrics"
  done
  "$(dirname "$redis")/redis-cli" -p "$base" info all >"$dir/redis.info" 2>&1
}

# stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
stack_pinned() {
  local have
  have=$(taskset -pc "${STACK_PIDS[$1]}" 2>/dev/null) || return 1
  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$2")" ]
}

# stack_term WAIT NAME...: SIGTERM to NAMEs at once, wait up to WAIT seconds
# for them to exit, then SIGKILL whatever is left. Appends "NAME STATUS" to
# $STACK_DIR/exit-status, STATUS "killed" when it needed the SIGKILL.
stack_term() {
  local wait=$1 name alive i
  shift
  for name in "$@"; do kill -TERM "${STACK_PIDS[$name]}" 2>/dev/null; done
  for i in $(seq $((wait * 10))); do
    alive=""
    for name in "$@"; do kill -0 "${STACK_PIDS[$name]}" 2>/dev/null && alive=1; done
    [ -z "$alive" ] && break
    sleep 0.1
  done
  for name in "$@"; do
    if kill -KILL "${STACK_PIDS[$name]}" 2>/dev/null; then
      wait "${STACK_PIDS[$name]}" 2>/dev/null
      echo "$name killed" >>"$STACK_DIR/exit-status"
    else
      wait "${STACK_PIDS[$name]}" 2>/dev/null
      echo "$name $?" >>"$STACK_DIR/exit-status"
    fi
  done
}

# stack_down: stops in dependency order (#78): the gateways first, together,
# up to STACK_DOWN_WAIT (10s) for their drain, so Logic still answers their
# lease renewals; then Logic, devtoken and Redis, each up to 5s. Whatever
# outlives its wait is SIGKILLed, so teardown stays bounded (25s).
stack_down() {
  local name gws=()
  for name in "${STACK_ORDER[@]}"; do
    [[ $name == gw-* ]] && gws+=("$name")
  done
  [ ${#gws[@]} -gt 0 ] && stack_term "${STACK_DOWN_WAIT:-10}" "${gws[@]}"
  for name in logic devtoken redis; do
    [ -n "${STACK_PIDS[$name]:-}" ] && stack_term 5 "$name"
  done
  STACK_ORDER=()
  STACK_PIDS=()
}

# stack_health: one line per problem, nothing when healthy (P6a1I-2): a
# process not running while the stack is up; once stack_down has run, a stop
# that was not clean (status 0; devtoken has no SIGTERM handler, so 143 too)
# or needed SIGKILL; and any log line starting with "panic:" or "fatal error:".
stack_health() {
  local name status f line
  if [ -f "$STACK_DIR/exit-status" ]; then
    while read -r name status; do
      case "$name:$status" in
        *:0 | devtoken:143) ;;
        *:killed) echo "$name: did not stop after SIGTERM, killed" ;;
        *) if [ "$status" -gt 128 ] 2>/dev/null; then
             echo "$name: stopped by SIG$(kill -l "$((status - 128))" 2>/dev/null || echo "$((status - 128))") (status $status)"
           else
             echo "$name: exited with status $status"
           fi ;;
      esac
    done <"$STACK_DIR/exit-status"
  else
    for name in "${STACK_ORDER[@]}"; do
      kill -0 "${STACK_PIDS[$name]}" 2>/dev/null || echo "$name: not running"
    done
  fi
  for f in "$STACK_DIR"/logs/*.log; do
    [ -f "$f" ] || continue
    line=$(grep -m 1 -E '^(panic:|fatal error:)' "$f") && echo "$(basename "$f" .log): $line"
  done
}
