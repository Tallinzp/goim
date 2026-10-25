# shellcheck shell=bash
# stack.sh: a goim stack as local processes for load runs (Plan 6a1, P5bI-1):
# one redis-server (or a Redis Cluster, STACK_REDIS=cluster: Plan 6c1),
# devtoken serving the JWKS, Logic and N gateways, built
# without -race and run at production timing. Tier 2 scenarios pin it by
# core type (choice #71); the Tier 1a load probe (Plan 6a2) runs it unpinned.
# Source it, set STACK_DIR, then:
#
#   stack_build                 # binaries into $STACK_DIR/bin (loadgen too, for scripts/loadgen.sh)
#   stack_up                    # keys, processes, waits until all are Ready
#   stack_snapshot NAME         # /metrics, heap and goroutine profiles of each process + redis INFO
#                               #   -> $STACK_DIR/artifacts/NAME/
#   stack_profile NAME SECONDS  # CPU profiles of Logic and every gateway at once -> artifacts/NAME/
#   stack_expect_dead NAME [SECONDS]  # NAME was killed on purpose (loadgen /kill): reap and record it
#   stack_metric NAME METRIC [MATCH]  # sum of METRIC's samples (with MATCH in the line) on NAME's /metrics
#   stack_metrics NAME          # NAME's /metrics once; stack_metric_in BODY METRIC [MATCH] reads it
#   stack_wait_idle SECONDS [dropped-ok]  # phase boundary: every gateway at 0 connections, 0 awaiting
#                               #   Logout, its Logouts settled (one done per login, none dropped;
#                               #   dropped-ok: a dropped Logout counts as settled)
#   stack_cpu_max NAME          # NAME's cgroup cpu.max, e.g. "25000 100000" (null: unreadable)
#   stack_cpu_stat NAME         # NAME's cgroup cpu.stat {"usage_usec", "nr_periods", "nr_throttled",
#                               #   "throttled_usec"} (null: unreadable), for stack_counters_delta
#   stack_usage_start [N=PID..] # start a CPU window over the stack's processes (and N=PID extras)
#   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb", "rss_peak_mb"}...} since the start
#   stack_counters NAME KEY=METRIC[:MATCH]...  # {"KEY": sum, ...} from one scrape of NAME (null: no answer)
#   stack_counters_delta A B    # {"KEY": B - A, ...} of two stack_counters readings
#   stack_buckets NAME METRIC   # {"LE": cumulative count, ...} of histogram METRIC on NAME (null: no answer)
#   stack_hist_quantile A B Q   # Q's bucket bound in ms over two stack_buckets readings (null: no sample)
#   stack_redis_names           # the Redis processes: "redis", or redis-0..5 on a Cluster
#   stack_redis_node_cli NAME ARGS...  # redis-cli against one Redis process
#   stack_redis_slots           # "START END NAME" per master's slot range ("0 16383 redis" on one Redis)
#   stack_redis_resetstat       # Redis CONFIG RESETSTAT on every node: a latency window starts
#   stack_redis_latency         # {"CMD": {"calls", "p50_us", "p99_us", "p999_us"}...} since the reset (null: no answer);
#                               #   on a Cluster {NAME: {"role", "zone", "cmds": {"CMD": ...}}...}
#   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
#   stack_health                # one line per problem: dead process, unclean stop, panic in a log
#
# Settings (environment, read by stack_up):
#   STACK_DIR            work dir (required): bin/, keys/, logs/, artifacts/, stack.env
#   STACK_GATEWAYS       gateways, ids gw-0.. (default 1)
#   STACK_PORT_BASE      redis +0, devtoken +1, Logic gRPC +2, lease +3, admin +4;
#                        Cluster node j: +20+j, its bus +30+j;
#                        gateway i: listen +100+10i, Kick +101+10i, admin +102+10i (default 17000:
#                        below the race suite's 20000-32000 and the ephemeral range)
#   STACK_REDIS          single (default): one redis-server; cluster: 3 masters and 3
#                        replicas (scripts/redis-cluster.sh: zones z0-z2 as labels,
#                        layout in redis/layout), Logic with -redis-cluster
#   STACK_PIN            1: taskset gateways to T2_CPUS_GATEWAY, Logic and Redis
#                        to T2_CPUS_LOGIC (lib.sh sets both); 0: no pinning (default)
#   STACK_REDIS_SERVER   default: redis-server on PATH, else .cache/redis/bin/redis-server
#   STACK_GATEWAY_PROCS  each gateway's GOMAXPROCS (default: max(1, CPUs / STACK_GATEWAYS),
#                        CPUs = T2_CPUS_GATEWAY when pinned, else all online; #79)
#   STACK_GATEWAY_ARGS, STACK_LOGIC_ARGS   arrays of extra flags
#   STACK_LOGIC_CPU_QUOTA  e.g. 25%: Logic runs in a transient systemd user scope with
#                        that CPUQuota (cgroup cpu.max, as a Kubernetes CPU limit; Plan
#                        6b2b-slow); needs the user manager to delegate the cpu controller
#   STACK_GATEWAY_ENV    array of VAR=VALUE for every gateway, e.g. (GOGC=off GOMEMLIMIT=4GiB) (#88)
#   STACK_DRAIN_WINDOW   gateway -drain-window (default 1s, so stack_down's drain ends and
#                        exits 0 well inside STACK_DOWN_WAIT; a measured drain sets 60s)
#   STACK_DOWN_WAIT      seconds stack_down waits for the gateways to drain (default 10)
#
# stack.env (for loadgen, Plan 6a2; P6a1I-4) holds, comma-separated and in
# the same gateway order: GATEWAY_ADDRS (client listeners), GATEWAY_IDS,
# GATEWAY_ADMINS, GATEWAY_KICK_ADDRS; then GATEWAY_PROCS (each gateway's
# GOMAXPROCS), NOISE_KEY_ID, NOISE_PUBLIC, JWT_KEY (the devtoken private key
# file), REDIS_ADDR (on a Cluster the 6 nodes, comma-separated), REDIS_MODE
# (single or cluster) and LOGIC_ADMIN. Every process is started from this shell
# in a process group of its own, added to T2_OURS_PGIDS (lib.sh counts it as
# ours), so a Ctrl-C reaches the run's script, whose teardown stops the stack
# in order.

declare -A STACK_PIDS=()
STACK_ORDER=()

stack_build() {
  mkdir -p "$STACK_DIR/bin"
  go build -o "$STACK_DIR/bin/" ./cmd/gateway ./cmd/logic ./cmd/devtoken ./cmd/loadgen
}

# stack_start NAME CPUS CMD...: starts CMD in the background in its own
# process group, logging to logs/NAME.log, pinned to CPUS when STACK_PIN=1.
stack_start() {
  local name=$1 cpus=$2
  shift 2
  if [ "${STACK_PIN:-0}" = 1 ]; then set -- taskset -c "$cpus" "$@"; fi
  # A process group of its own (setsid execs in place: a background job is
  # never a group leader), so a Ctrl-C to the run's group does not stop the
  # whole stack at once, out of #78's order; stack_down stops it. lib.sh
  # counts the group as ours.
  setsid "$@" >"$STACK_DIR/logs/$name.log" 2>&1 &
  STACK_PIDS[$name]=$!
  STACK_ORDER+=("$name")
  T2_OURS_PGIDS+=("$!")
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

# stack_ports: every port stack_up listens on, and loadgen's control ports
# (base + 5 … + 8, one per instance, scripts/loadgen.sh): a leftover loadgen
# there would answer lg_start's /stats in place of the new one.
stack_ports() {
  local base=${STACK_PORT_BASE:-17000} i
  seq "$base" $((base + 8))
  if [ "${STACK_REDIS:-single}" = cluster ]; then
    seq $((base + 20)) $((base + 25))
    seq $((base + 30)) $((base + 35))
  fi
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
  rm -f "$STACK_DIR/exit-status" # an earlier stack's, even if this one is refused
  if [ "${STACK_PIN:-0}" = 1 ] && { [ -z "$gw_cpus" ] || [ -z "$logic_cpus" ]; }; then
    echo "stack: STACK_PIN=1 needs T2_CPUS_GATEWAY and T2_CPUS_LOGIC" >&2
    return 1
  fi
  if [ "${STACK_PIN:-0}" = 1 ] && ! declare -F t2_cpulist_expand >/dev/null; then
    echo "stack: STACK_PIN=1 needs scripts/tier2/lib.sh sourced (t2_cpulist_expand)" >&2
    return 1
  fi
  [ -x "$redis" ] || { echo "stack: $redis not found (make redis-server)" >&2; return 1; }
  case ${STACK_REDIS:-single} in
    single | cluster) ;;
    *) echo "stack: STACK_REDIS=$STACK_REDIS: want single or cluster" >&2; return 1 ;;
  esac
  local busy
  busy=$(stack_busy_ports)
  if [ -n "$busy" ]; then
    printf 'stack: %s\n' "$busy" >&2
    echo "stack: stop whatever holds them first (a leftover stack?); a second stack beside it distorts the numbers" >&2
    return 1
  fi
  mkdir -p "$STACK_DIR/keys/noise" "$STACK_DIR/logs" "$STACK_DIR/artifacts" "$STACK_DIR/redis"

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
  local kick="" addrs="" ids="" admins="" kicks=""
  for ((i = 0; i < n; i++)); do
    kick+="${kick:+,}\"gw-$i\": \"127.0.0.1:$((base + 101 + 10 * i))\""
  done
  echo "{$kick}" >"$STACK_DIR/kick.json"

  if [ "${STACK_REDIS:-single}" = cluster ]; then
    stack_redis_cluster_up "$redis" "$logic_cpus" || return 1
    local j
    redis_addr=""
    for j in 0 1 2 3 4 5; do redis_addr+="${redis_addr:+,}127.0.0.1:$((base + 20 + j))"; done
  else
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
  fi
  stack_start devtoken "$logic_cpus" "$bin/devtoken" serve -addr "$devtoken" -jwks "$STACK_DIR/keys/jwks.json"
  stack_wait devtoken "http://$devtoken/" || return 1
  # A CPU quota (Plan 6b2b-slow): systemd-run --scope moves itself into a
  # new scope and execs Logic, so the PID, the process group and the
  # affinity stay as stack_start set them.
  local logic=("$bin/logic")
  [ -n "${STACK_LOGIC_CPU_QUOTA:-}" ] &&
    logic=(systemd-run --user --scope --quiet -p "CPUQuota=$STACK_LOGIC_CPU_QUOTA" -- "$bin/logic")
  stack_start logic "$logic_cpus" "${logic[@]}" -redis "$redis_addr" -redis-cluster="$([ "${STACK_REDIS:-single}" = cluster ] && echo true || echo false)" \
    -grpc-addr "$grpc" -lease-addr "$lease" -admin-addr "$admin" \
    -jwks-url "http://$devtoken/" -jwks-fallback "$STACK_DIR/keys/jwks.json" \
    -kick-addr-file "$STACK_DIR/kick.json" "${STACK_LOGIC_ARGS[@]}"
  stack_wait logic "http://$admin/readyz" || return 1
  for ((i = 0; i < n; i++)); do
    local listen=127.0.0.1:$((base + 100 + 10 * i)) gadmin=127.0.0.1:$((base + 102 + 10 * i))
    stack_start "gw-$i" "$gw_cpus" env GOMAXPROCS="$procs" "${STACK_GATEWAY_ENV[@]}" "$bin/gateway" -id "gw-$i" -listen "tcp://$listen" \
      -noise-keys "$STACK_DIR/keys/noise" -logic "$grpc" -lease "$lease" \
      -kick-addr "127.0.0.1:$((base + 101 + 10 * i))" -admin-addr "$gadmin" \
      -drain-window "${STACK_DRAIN_WINDOW:-1s}" "${STACK_GATEWAY_ARGS[@]}"
    addrs+="${addrs:+,}$listen"
    ids+="${ids:+,}gw-$i"
    admins+="${admins:+,}$gadmin"
    kicks+="${kicks:+,}127.0.0.1:$((base + 101 + 10 * i))"
  done
  for ((i = 0; i < n; i++)); do
    stack_wait "gw-$i" "http://127.0.0.1:$((base + 102 + 10 * i))/readyz" || return 1
  done
  cat >"$STACK_DIR/stack.env" <<ENV
GATEWAY_ADDRS=$addrs
GATEWAY_IDS=$ids
GATEWAY_ADMINS=$admins
GATEWAY_KICK_ADDRS=$kicks
GATEWAY_PROCS=$procs
NOISE_KEY_ID=1
NOISE_PUBLIC=$pub
JWT_KEY=$STACK_DIR/keys/jwt.jwk
REDIS_ADDR=$redis_addr
REDIS_MODE=${STACK_REDIS:-single}
LOGIC_ADMIN=$admin
ENV
}

# stack_redis_cluster_up SERVER CPUS: the Cluster's 6 nodes (redis-0..5,
# each a process of its own pinned to CPUS like the single Redis), joined
# in scripts/redis-cluster.sh's layout; redis/layout gets "NAME PORT ROLE
# ZONE MASTER" per node, the layout at the start (roles change in a failover).
stack_redis_cluster_up() {
  local server=$1 cpus=$2 base=${STACK_PORT_BASE:-17000} j name role zone master ports=()
  . "$(dirname "${BASH_SOURCE[0]}")/redis-cluster.sh"
  : >"$STACK_DIR/redis/layout"
  j=0
  while read -r name role zone master; do
    mkdir -p "$STACK_DIR/redis/$name"
    redis_cluster_args $((base + 20 + j)) $((base + 30 + j)) "$STACK_DIR/redis/$name"
    stack_start "$name" "$cpus" "$server" "${REDIS_CLUSTER_ARGS[@]}"
    echo "$name $((base + 20 + j)) $role $zone $master" >>"$STACK_DIR/redis/layout"
    ports+=($((base + 20 + j)))
    j=$((j + 1))
  done < <(redis_cluster_layout)
  redis_cluster_join "$(dirname "$server")/redis-cli" "${ports[@]}" >"$STACK_DIR/logs/redis-join.log" 2>&1 || {
    echo "stack: the Redis Cluster did not form; last log lines:" >&2
    tail -n 20 "$STACK_DIR/logs/redis-join.log" "$STACK_DIR/logs/redis-0.log" >&2
    return 1
  }
}

# stack_admins: "NAME ADMIN" for Logic and each running gateway.
stack_admins() {
  local base=${STACK_PORT_BASE:-17000} i
  [ -n "${STACK_PIDS[logic]:-}" ] && echo "logic 127.0.0.1:$((base + 4))"
  for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
    [ -n "${STACK_PIDS[gw-$i]:-}" ] && echo "gw-$i 127.0.0.1:$((base + 102 + 10 * i))"
  done
}

# stack_snapshot NAME: every process's /metrics, heap profile and goroutine
# dump (pprof on the admin port, choice #85), and Redis INFO, for result
# artifacts (spec: Automation contract: pprof, a metrics snapshot). A
# gateway killed on purpose (stack_expect_dead) is skipped.
stack_snapshot() {
  local dir=$STACK_DIR/artifacts/$1 base=${STACK_PORT_BASE:-17000} name admin
  mkdir -p "$dir"
  while read -r name admin; do
    curl -sf "http://$admin/metrics" >"$dir/$name.metrics"
    curl -sf "http://$admin/debug/pprof/heap" >"$dir/$name.heap.pb.gz"
    curl -sf "http://$admin/debug/pprof/goroutine?debug=1" >"$dir/$name.goroutine.txt"
  done < <(stack_admins)
  stack_redis_cli info all >"$dir/redis.info" 2>&1
  if [ "${STACK_REDIS:-single}" = cluster ]; then
    for name in $(stack_redis_names); do
      stack_redis_node_cli "$name" info all >"$dir/$name.info" 2>&1
    done
    stack_redis_cli cluster nodes >"$dir/cluster-nodes.txt" 2>&1
  fi
}

# stack_redis_names: the Redis processes, "redis" or redis-0..5.
stack_redis_names() {
  if [ "${STACK_REDIS:-single}" = cluster ]; then echo redis-0 redis-1 redis-2 redis-3 redis-4 redis-5; else echo redis; fi
}

# stack_redis_node_cli NAME ARGS...: redis-cli against one Redis process
# (redis-j on port base + 20 + j), no redirects followed.
stack_redis_node_cli() {
  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
  local base=${STACK_PORT_BASE:-17000} port name=$1
  shift
  case $name in
    redis) port=$base ;;
    redis-[0-5]) port=$((base + 20 + ${name#redis-})) ;;
    *) echo "stack: no Redis process $name" >&2; return 1 ;;
  esac
  "$(dirname "$redis")/redis-cli" -p "$port" "$@"
}

# stack_redis_cli ARGS...: redis-cli against the stack's Redis: the one
# redis-server, or on a Cluster redis-0 (a key it does not own answers
# MOVED: per-key commands go through stack_redis_slots, review P6b3c2I-3).
stack_redis_cli() {
  if [ "${STACK_REDIS:-single}" = cluster ]; then stack_redis_node_cli redis-0 "$@"; else stack_redis_node_cli redis "$@"; fi
}

# stack_redis_slots: "START END NAME" for each master's slot range, from
# CLUSTER NODES as redis-0 sees it now (after a failover too); "0 16383
# redis" on one Redis. Prints nothing if Redis does not answer.
stack_redis_slots() {
  if [ "${STACK_REDIS:-single}" != cluster ]; then
    stack_redis_cli ping >/dev/null 2>&1 && echo "0 16383 redis"
    return
  fi
  stack_redis_cli cluster nodes 2>/dev/null | awk -v base="${STACK_PORT_BASE:-17000}" '
    $3 ~ /master/ && $3 !~ /fail/ {
      split($2, a, "[:@]"); name = "redis-" (a[2] - base - 20)
      for (i = 9; i <= NF; i++) { if ($i ~ /^\[/) continue; n = split($i, r, "-"); print r[1], r[n], name } }' | sort -n
}

# stack_redis_resetstat: CONFIG RESETSTAT on every Redis process, so their
# per-command latency percentiles and call counts cover a window from now
# (Plan 6b2a-stress). Fails if any node did not answer OK.
stack_redis_resetstat() {
  local name
  for name in $(stack_redis_names); do
    [ "$(stack_redis_node_cli "$name" config resetstat 2>/dev/null)" = OK ] || return 1
  done
}

# stack_redis_node_latency NAME: one node's per-command latency since the
# last resetstat, from INFO latencystats (p50, p99 and p99.9 in µs, Redis 7's
# defaults) and the calls from INFO commandstats, as {"CMD": {"calls",
# "p50_us", "p99_us", "p999_us"}, ...}. The readers' own INFO and CONFIG
# are left out. Prints null if the node does not answer.
stack_redis_node_latency() {
  local info
  info=$(stack_redis_node_cli "$1" info latencystats 2>/dev/null && stack_redis_node_cli "$1" info commandstats 2>/dev/null) &&
    grep -q '^# Latencystats' <<<"$info" || { echo null; return; }
  awk -F'[:,=]' '{ sub(/\r$/, "") }
    /^latency_percentiles_usec_/ { c = substr($1, 26); p50[c] = $3; p99[c] = $5; p999[c] = $7; cmd[++n] = c }
    /^cmdstat_/ { calls[substr($1, 9)] = $3 }
    END { out = ""
          for (i = 1; i <= n; i++) {
            c = cmd[i]
            if (c == "info" || index(c, "config") == 1 || index(c, "cluster") == 1) continue
            out = out (out == "" ? "" : ", ") sprintf("\"%s\": {\"calls\": %d, \"p50_us\": %s, \"p99_us\": %s, \"p999_us\": %s}", c, calls[c], p50[c], p99[c], p999[c]) }
          print "{" out "}" }' <<<"$info"
}

# stack_redis_latency: stack_redis_node_latency of the one Redis; on a
# Cluster, per node with its role now (master or replica) and its zone from
# redis/layout: {NAME: {"role", "zone", "cmds": {...}}, ...}, the per-shard
# numbers the spec asks for (R4M-9). Prints null if no node answers; a node
# that does not answer is null.
stack_redis_latency() {
  [ "${STACK_REDIS:-single}" = cluster ] || { stack_redis_node_latency redis; return; }
  local name role zone cmds out="" any=""
  for name in $(stack_redis_names); do
    cmds=$(stack_redis_node_latency "$name")
    role=$(stack_redis_node_cli "$name" info replication 2>/dev/null | tr -d '\r' | sed -n 's/^role://p')
    zone=$(awk -v n="$name" '$1 == n { print $4 }' "$STACK_DIR/redis/layout" 2>/dev/null)
    if [ "$cmds" = null ]; then
      out+="${out:+, }\"$name\": null"
    else
      any=1
      out+="${out:+, }\"$name\": {\"role\": \"$([ "$role" = slave ] && echo replica || echo "${role:-unknown}")\", \"zone\": \"$zone\", \"cmds\": $cmds}"
    fi
  done
  if [ -n "$any" ]; then echo "{$out}"; else echo null; fi
}

# stack_profile NAME SECONDS: a CPU profile of Logic and every running
# gateway over the same SECONDS, into artifacts/NAME/<process>.cpu.pb.gz.
# Blocks for SECONDS; a scenario takes it inside its measured window. Fails
# if any profile failed.
stack_profile() {
  local dir=$STACK_DIR/artifacts/$1 name admin pid rc=0 pids=()
  mkdir -p "$dir"
  while read -r name admin; do
    curl -sf -m $(($2 + 30)) "http://$admin/debug/pprof/profile?seconds=$2" >"$dir/$name.cpu.pb.gz" &
    pids+=($!)
  done < <(stack_admins)
  # One wait per pid: a bare wait (no process up) would wait for every
  # child, Redis included, and one wait for several returns the last status.
  for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
  return $rc
}

# stack_metric NAME METRIC [MATCH]: the sum of METRIC's samples on NAME's
# /metrics (lines "METRIC{...} V" or "METRIC V", and containing MATCH if
# given, e.g. 'state="online"'), as an integer when whole (%.0f, not %d,
# which stops at 2^31 in some awks); "null" if NAME does not answer or has
# no such sample, so it can go straight into JSON.
stack_metric() {
  local body
  body=$(stack_metrics "$1") || { echo null; return; }
  stack_metric_in "$body" "$2" "${3:-}"
}

# stack_metrics NAME: NAME's /metrics, once; fails if NAME does not answer
# within STACK_CURL_TIMEOUT seconds (default 10). With stack_metric_in,
# several readings come from the same scrape.
stack_metrics() {
  local admin
  admin=$(stack_admins | awk -v n="$1" '$1 == n { print $2 }')
  [ -n "$admin" ] && curl -sf -m "${STACK_CURL_TIMEOUT:-10}" "http://$admin/metrics"
}

# stack_metric_in BODY METRIC [MATCH]: stack_metric over a scrape already taken.
stack_metric_in() {
  local body=$1
  awk -v m="$2" -v want="${3:-}" '
    ($1 == m || index($1, m "{") == 1) && (want == "" || index($0, want)) { s += $NF; n++ }
    END { if (!n) print "null"; else if (s == int(s)) printf "%.0f\n", s; else printf "%.6g\n", s }' <<<"$body"
}

# stack_wait_idle SECONDS: the phase boundary on a shared stack (P6a2bI-3):
# waits until every running gateway has no open connection, no closed one
# still awaiting its fenced Logout, and its Logouts settled: at least one
# done per login, none dropped (P6b1I-4). A client that retried after a
# timeout has two sessions, and a Login with an unknown outcome gets one
# too, so done can exceed the logins. So a stopped loadgen's closes and
# Logouts do not spill into the next phase. Each gateway is read from one
# scrape, where a counter with no sample yet is 0; a gateway that does not
# answer counts as busy. Only running gateways count: a killed one's
# sessions end by EpochEnded, not Logouts, and it is gone from stack_admins
# (stack_expect_dead). Fails, naming what is left, after SECONDS, or at once
# on a dropped Logout, which never settles. With dropped-ok, a dropped
# Logout counts as settled instead (done + dropped >= logins): a slowed
# Logic may fill a gateway's Logout queue, which the spec allows, Reconcile
# repairing the session (R1-O3, R3-O1; roadmap #94, P6b2a-stressI-2).
stack_wait_idle() {
  local end=$((SECONDS + $1)) ok=${2:-} name admin busy body c a ldone logins dropped
  while :; do
    busy=""
    while read -r name admin; do
      [[ $name == gw-* ]] || continue
      body=$(stack_metrics "$name") || { busy+=" $name: no answer;"; continue; }
      c=$(stack_metric_in "$body" goim_gateway_connections)
      a=$(stack_metric_in "$body" goim_gateway_awaiting_logout)
      ldone=$(stack_metric_in "$body" goim_gateway_logouts_total 'result="done"')
      logins=$(stack_metric_in "$body" goim_gateway_logins_total 'result="ok"')
      dropped=$(stack_metric_in "$body" goim_gateway_logouts_total 'result="dropped"')
      [ "$ldone" = null ] && ldone=0
      [ "$logins" = null ] && logins=0
      [ "$dropped" = null ] && dropped=0
      if [ "$dropped" != 0 ] && [ "$ok" != dropped-ok ]; then
        echo "stack: $name dropped $dropped Logouts: they never settle (logouts done $ldone of $logins logins, dropped $dropped)" >&2
        return 1
      fi
      [ "$ok" = dropped-ok ] || dropped=0
      [ "$c" = 0 ] && [ "$a" = 0 ] && [ $((ldone + dropped)) -ge "$logins" ] ||
        busy+=" $name: connections $c, awaiting_logout $a, logouts done $ldone$([ "$dropped" = 0 ] || echo " + dropped $dropped") of $logins logins;"
    done < <(stack_admins)
    [ -z "$busy" ] && return 0
    if [ $SECONDS -ge $end ]; then
      echo "stack: not idle after $1s:${busy%;}" >&2
      return 1
    fi
    sleep 0.5
  done
}

# stack_ticks PID: utime + stime of PID in clock ticks (every thread), read
# after the last ')' so a comm with spaces cannot shift the fields.
stack_ticks() {
  awk '{ n = length($0); while (n > 0 && substr($0, n, 1) != ")") n--
         split(substr($0, n + 2), f, " "); print f[12] + f[13] }' "/proc/$1/stat" 2>/dev/null
}

# stack_usage_start [NAME=PID...]: starts a CPU window over every running
# stack process and the NAME=PID extras (loadgen), for per-process sizing:
# CPU-seconds and cores in the window, resident memory at its end.
declare -A STACK_USAGE0=()
STACK_USAGE_PIDS=()
STACK_USAGE_T0=0
stack_usage_start() {
  local name e
  STACK_USAGE0=() STACK_USAGE_PIDS=()
  for name in "${STACK_ORDER[@]}"; do STACK_USAGE_PIDS+=("$name=${STACK_PIDS[$name]}"); done
  STACK_USAGE_PIDS+=("$@")
  for e in "${STACK_USAGE_PIDS[@]}"; do STACK_USAGE0[${e%%=*}]=$(stack_ticks "${e#*=}"); done
  STACK_USAGE_T0=$(date +%s%N)
}

# stack_usage_json: {"window_s": W, NAME: {"cpu_s", "cores", "rss_mb",
# "rss_peak_mb"}, ...} since stack_usage_start (cores = cpu_s / W); a process
# gone since is null. rss_peak_mb is the process's peak resident memory since
# it started (VmHWM, P6b1I-3), not the window's: the memory budget of a run
# (#93) is its peak, which a reading at the window's end can miss.
stack_usage_json() {
  local hz e name pid t1 out
  hz=$(getconf CLK_TCK)
  t1=$(date +%s%N)
  out="\"window_s\": $(awk -v d=$((t1 - STACK_USAGE_T0)) 'BEGIN { printf "%.1f", d / 1e9 }')"
  for e in "${STACK_USAGE_PIDS[@]}"; do
    name=${e%%=*} pid=${e#*=}
    out+=", \"$name\": $(awk -v t0="${STACK_USAGE0[$name]:-}" -v t1="$(stack_ticks "$pid")" -v hz="$hz" \
      -v w=$((t1 - STACK_USAGE_T0)) -v mem="$(awk '/^Vm(RSS|HWM):/ { printf "%s ", $2 }' "/proc/$pid/status" 2>/dev/null)" '
      BEGIN { if (t0 == "" || t1 == "" || split(mem, m, " ") != 2) { print "null"; exit }
              c = (t1 - t0) / hz
              printf "{\"cpu_s\": %.2f, \"cores\": %.3f, \"rss_mb\": %d, \"rss_peak_mb\": %d}", c, c / (w / 1e9), m[2] / 1024, m[1] / 1024 }')"
  done
  echo "{$out}"
}

# stack_counters NAME KEY=METRIC[:MATCH]...: one scrape of NAME's /metrics
# as {"KEY": the sum of METRIC's samples containing MATCH, ...}, for counters
# read before and after an event (Plan 6b2a: the storm's Kicks, lease
# renewals and sweeps). Prints null if NAME does not answer. A KEY is null
# when the scrape has no sample of METRIC at all (a mistyped name must not
# read as 0), and 0 when METRIC has samples but none contains MATCH: a
# labelled counter has no sample for a label value until its first
# increment, e.g. kick_attempts_total{attempt="retry"} while no Kick was
# retried.
stack_counters() {
  local name=$1 body
  shift
  body=$(stack_metrics "$name") || { echo null; return; }
  awk -v specs="$*" '
    BEGIN { n = split(specs, sp, " ")
            for (i = 1; i <= n; i++) {
              e = index(sp[i], "="); key[i] = substr(sp[i], 1, e - 1); rest = substr(sp[i], e + 1)
              c = index(rest, ":"); met[i] = c ? substr(rest, 1, c - 1) : rest; want[i] = c ? substr(rest, c + 1) : "" } }
    { for (i = 1; i <= n; i++)
        if ($1 == met[i] || index($1, met[i] "{") == 1) { seen[i] = 1; if (want[i] == "" || index($0, want[i])) s[i] += $NF } }
    END { out = ""
          for (i = 1; i <= n; i++)
            out = out (i > 1 ? ", " : "") "\"" key[i] "\": " (!seen[i] ? "null" : (s[i] == int(s[i]) ? sprintf("%.0f", s[i]) : sprintf("%.6g", s[i])))
          print "{" out "}" }' <<<"$body"
}

# stack_counters_delta A B: {"KEY": B - A, ...} over two stack_counters
# readings of the same process. A KEY null in A counts as 0 there (the
# counter had no sample yet; a failed scrape makes all of A null instead),
# and is null in the delta when B lacks it or either reading is null.
stack_counters_delta() {
  python3 -c 'import json, sys
a, b = (json.loads(x) for x in sys.argv[1:3])
if a is None or b is None:
    print("null")
else:
    print(json.dumps({k: None if v is None else round(v - (a.get(k) or 0), 6) for k, v in b.items()}))' "$1" "$2" 2>/dev/null || echo null
}

# stack_buckets NAME METRIC: histogram METRIC's cumulative bucket counts on
# NAME from one scrape, as {"LE": count, ...} with LE as Prometheus writes it
# ("0.0005", ..., "+Inf"), summed over the other labels (Plan 6b2b: the
# survivor's Login latency before and during b2's storm). Prints null if
# NAME does not answer or has no METRIC_bucket sample.
stack_buckets() {
  local body
  body=$(stack_metrics "$1") || { echo null; return; }
  awk -v m="$2_bucket" '
    index($1, m "{") == 1 && match($1, /le="[^"]*"/) {
      le = substr($1, RSTART + 4, RLENGTH - 5)
      if (!(le in s)) order[++n] = le
      s[le] += $NF }
    END { if (!n) { print "null"; exit }
          out = ""
          for (i = 1; i <= n; i++) out = out (i > 1 ? ", " : "") "\"" order[i] "\": " sprintf("%.0f", s[order[i]])
          print "{" out "}" }' <<<"$body"
}

# stack_hist_quantile A B Q: the upper bound, in ms, of the bucket that
# holds quantile Q (0-1) of the samples between two stack_buckets readings
# of the same process (B - A); a bucket's bound, not an interpolation, so a
# p99 reads as one of the histogram's bounds. Prints null when either
# reading is null, there is no sample between them, Q falls in +Inf, or Q
# is above 1.
# The rank is exact (Q as a fraction), so it never moves up a sample.
stack_hist_quantile() {
  python3 -c 'import json, math, sys
from fractions import Fraction
a, b = (json.loads(x) for x in sys.argv[1:3])
q = Fraction(sys.argv[3]) # exact: 0.28 x 25 is 7, not 7.000000000000001
if a is None or b is None:
    print("null"); sys.exit()
les = sorted(b, key=lambda le: math.inf if le == "+Inf" else float(le))
d = {le: b[le] - a.get(le, 0) for le in les}
total = d.get("+Inf", 0)
if total <= 0:
    print("null"); sys.exit()
rank = math.ceil(q * total)
for le in les:
    if d[le] >= rank:
        ms = None if le == "+Inf" else round(float(le) * 1000, 3)
        print(json.dumps(int(ms) if ms is not None and ms == int(ms) else ms))
        break
else:
    print("null") # Q above 1 (P6b2b-b2I-5)' "$1" "$2" "$3" 2>/dev/null || echo null
}

# stack_cpu_max NAME: the cpu.max of NAME's cgroup (cgroup v2), e.g.
# "25000 100000" for a 25% quota, "max 100000" for none; null if NAME is
# not running or its cgroup has no cpu.max (the cpu controller is not
# enabled there).
stack_cpu_max() {
  local pid=${STACK_PIDS[$1]:-} path
  path=$(awk -F: '$1 == "0" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null)
  [ -n "$pid" ] && [ -n "$path" ] && cat "/sys/fs/cgroup$path/cpu.max" 2>/dev/null || echo null
}

# stack_cpu_stat NAME: the cpu.stat of NAME's cgroup as {"usage_usec",
# "nr_periods", "nr_throttled", "throttled_usec"}, for stack_counters_delta
# around a window: how often a CPU quota throttled NAME (P6b2b-slowI-1).
# null as for stack_cpu_max; the nr_* keys are null without a quota's
# accounting (the cpu controller is not enabled there).
stack_cpu_stat() {
  local pid=${STACK_PIDS[$1]:-} path
  path=$(awk -F: '$1 == "0" { print $3 }' "/proc/$pid/cgroup" 2>/dev/null)
  { [ -n "$pid" ] && [ -n "$path" ] && [ -r "/sys/fs/cgroup$path/cpu.stat" ]; } || { echo null; return; }
  awk 'BEGIN { split("usage_usec nr_periods nr_throttled throttled_usec", k, " ") }
    { v[$1] = $2 }
    END { out = ""
          for (i = 1; i <= 4; i++) out = out (i > 1 ? ", " : "") "\"" k[i] "\": " (k[i] in v ? v[k[i]] : "null")
          print "{" out "}" }' "/sys/fs/cgroup$path/cpu.stat"
}

# stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
stack_pinned() {
  local have
  have=$(taskset -pc "${STACK_PIDS[$1]}" 2>/dev/null) || return 1
  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$2")" ]
}

# stack_forget NAME: drops NAME from STACK_PIDS and STACK_ORDER.
stack_forget() {
  local n order=()
  unset "STACK_PIDS[$1]"
  for n in "${STACK_ORDER[@]}"; do [ "$n" = "$1" ] || order+=("$n"); done
  STACK_ORDER=("${order[@]}")
}

# stack_expect_dead NAME [SECONDS]: NAME was killed on purpose (a load
# scenario's SIGKILL through loadgen's /kill, choice #84). Waits up to
# SECONDS (default 5) for it to end, records "NAME expected:STATUS" in
# exit-status and forgets it, so stack_health does not report it and
# stack_down does not stop it again. Fails, keeping NAME, if it is still
# running. A process exits only once the kernel has closed its sockets: a
# gateway with 600k took ~5.3s (Tier 2 run 25).
stack_expect_dead() {
  local pid=${STACK_PIDS[$1]:-} i
  [ -n "$pid" ] || { echo "stack: no process $1" >&2; return 1; }
  for i in $(seq $((${2:-5} * 10))); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.1
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "stack: $1 is still running" >&2
    return 1
  fi
  wait "$pid" 2>/dev/null
  echo "$1 expected:$?" >>"$STACK_DIR/exit-status"
  stack_forget "$1"
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
  local pid status sent
  for name in "$@"; do
    pid=${STACK_PIDS[$name]} sent=""
    kill -KILL "$pid" 2>/dev/null && sent=1
    wait "$pid" 2>/dev/null
    status=$?
    # "killed" only if our SIGKILL ended it: one that lands on a process
    # that had just exited (not yet reaped) leaves its own status.
    [ -n "$sent" ] && [ "$status" -eq 137 ] && status=killed
    echo "$name $status" >>"$STACK_DIR/exit-status"
    # Forgotten once recorded, so a stack_down cut short by a signal and
    # finished by the exit hook records each process once.
    stack_forget "$name"
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
  for name in logic devtoken; do
    [ -n "${STACK_PIDS[$name]:-}" ] && stack_term 5 "$name"
  done
  # Redis last; a Cluster's nodes together, so no replica is left to
  # fail over a stopped master.
  local rs=()
  for name in "${STACK_ORDER[@]}"; do
    [[ $name == redis || $name == redis-* ]] && rs+=("$name")
  done
  [ ${#rs[@]} -gt 0 ] && stack_term 5 "${rs[@]}"
  STACK_ORDER=()
  STACK_PIDS=()
}

# stack_health: one line per problem, nothing when healthy (P6a1I-2): a
# process not running while the stack is up; once stack_down has run, a stop
# that was not clean (status 0; devtoken has no SIGTERM handler, so 143 too)
# or needed SIGKILL; and any log line starting with "panic:" or "fatal error:".
# A process killed on purpose (stack_expect_dead, status 137) is not a
# problem, but its log is still scanned; one that ended otherwise before
# the kill is.
stack_health() {
  local name status line names=()
  # Stopped processes by their recorded status, running ones by kill -0.
  if [ -f "$STACK_DIR/exit-status" ]; then
    while read -r name status; do
      case "$name:$status" in
        *:0 | devtoken:143 | *:expected:137) ;;
        *:expected:*) echo "$name: expected a kill, exited with status ${status#expected:}" ;;
        *:killed) echo "$name: did not stop after SIGTERM, killed" ;;
        *) if [ "$status" -gt 128 ] 2>/dev/null; then
             echo "$name: stopped by SIG$(kill -l "$((status - 128))" 2>/dev/null || echo "$((status - 128))") (status $status)"
           else
             echo "$name: exited with status $status"
           fi ;;
      esac
      names+=("$name")
    done <"$STACK_DIR/exit-status"
  fi
  for name in "${STACK_ORDER[@]}"; do
    kill -0 "${STACK_PIDS[$name]}" 2>/dev/null || echo "$name: not running"
    names+=("$name")
  done
  # Only this stack's processes: a log left by an earlier, larger stack is not.
  for name in "${names[@]}"; do
    line=$(grep -m 1 -E '^(panic:|fatal error:)' "$STACK_DIR/logs/$name.log" 2>/dev/null) && echo "$name: $line"
  done
}
