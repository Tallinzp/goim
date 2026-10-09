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
#   stack_down                  # SIGTERM, then SIGKILL after 10s; call it from the exit trap
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
#   STACK_GATEWAY_ARGS, STACK_LOGIC_ARGS   arrays of extra flags
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

stack_up() {
  : "${STACK_DIR:?set STACK_DIR}"
  local n=${STACK_GATEWAYS:-1} base=${STACK_PORT_BASE:-17000} bin=$STACK_DIR/bin i p
  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
  local gw_cpus=${T2_CPUS_GATEWAY:-} logic_cpus=${T2_CPUS_LOGIC:-}
  if [ "${STACK_PIN:-0}" = 1 ] && { [ -z "$gw_cpus" ] || [ -z "$logic_cpus" ]; }; then
    echo "stack: STACK_PIN=1 needs T2_CPUS_GATEWAY and T2_CPUS_LOGIC" >&2
    return 1
  fi
  [ -x "$redis" ] || { echo "stack: $redis not found (make redis-server)" >&2; return 1; }
  for p in $(seq "$base" $((base + 4))) $(for ((i = 0; i < n; i++)); do echo $((base + 100 + 10 * i)) $((base + 101 + 10 * i)) $((base + 102 + 10 * i)); done); do
    stack_port_free "$p" || { echo "stack: port $p is in use; pick another STACK_PORT_BASE" >&2; return 1; }
  done
  mkdir -p "$STACK_DIR/keys/noise" "$STACK_DIR/logs" "$STACK_DIR/artifacts" "$STACK_DIR/redis"

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
    stack_start "gw-$i" "$gw_cpus" "$bin/gateway" -id "gw-$i" -listen "tcp://$listen" \
      -noise-keys "$STACK_DIR/keys/noise" -logic "$grpc" -lease "$lease" \
      -kick-addr "127.0.0.1:$((base + 101 + 10 * i))" -admin-addr "$gadmin" "${STACK_GATEWAY_ARGS[@]}"
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

# stack_down: SIGTERM to all (gateways drain), SIGKILL whatever is left after 10s.
stack_down() {
  local name pid i
  for ((i = ${#STACK_ORDER[@]} - 1; i >= 0; i--)); do
    kill -TERM "${STACK_PIDS[${STACK_ORDER[i]}]}" 2>/dev/null
  done
  for i in $(seq 100); do
    pid=""
    for name in "${STACK_ORDER[@]}"; do
      kill -0 "${STACK_PIDS[$name]}" 2>/dev/null && pid=1
    done
    [ -z "$pid" ] && break
    sleep 0.1
  done
  for name in "${STACK_ORDER[@]}"; do
    kill -KILL "${STACK_PIDS[$name]}" 2>/dev/null
    wait "${STACK_PIDS[$name]}" 2>/dev/null
  done
  STACK_ORDER=()
  STACK_PIDS=()
}
