# Plan 6b1: Capacity — 600k on One Gateway and Heartbeat Cost (Tier 2 (a) and (c)) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure the spec's capacity ceiling and heartbeat cost on the Tier 2 host: (a) 600k light connections held on one gateway, with Go's default GC and with GOMEMLIMIT/GOGC tuned, and (c) the gateway's heartbeat CPU cost at 200k and 600k at 150s, plus 60s at 200k, with per-process CPU and memory recorded for Plan 6b2's sizing.

**Architecture:** One new Tier 2 scenario, `scripts/tier2/capacity.sh`, runs four phases, each on a fresh one-gateway stack pinned to the whole gateway CPU set: ramp light clients through loadgen, hold them through a measured window of whole heartbeats, then stop loadgen and wait for the phase boundary (every connection closed and every Logout done) before the stack stops. `scripts/stack.sh` gains what the phases need: a per-gateway environment (`GOGC`, `GOMEMLIMIT`), a metric reader, the phase boundary `stack_wait_idle`, and per-process CPU/RSS windows; `scripts/loadgen.sh` gains loadgen's own CPU profile. The gateway's Logout queue now follows `-max-conns`, so 600k connections closing at once do not drop Logouts.

**Tech Stack:** Go 1.27.1, bash 5.2, the pinned `redis-server` 7.4.11 (`make redis-server`), curl, gzip, python3 (reads JSON only), `go tool pprof` (on the Tier 2 host).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Success criteria → Capacity ceiling* ("one gateway holds 600k connections (measure CPU / memory / GC)"), *Verification → Test execution model* (*CPU isolation*: capacity reported per gateway core count; *Automation contract*), *Verification → Load tests* (a) and (c) (R4M-6), *Gateway → Admission control* (connection cap; pooling left to profiling in (a)). Roadmap choices #65, #69, #71, #79–#87 and Plan 6a2a's *Execution Notes* (run 13 sizing, P6a2aI-4, P6a2bI-3).

**Branch:** `feat/plan6b1-capacity`, from `main` (`373fc73`, which holds every earlier plan and the README).

**Split (choice #87):** Plan 6b is three plans, each written and executed in its own session with its own Tier 2 run: **6b1** (this plan) capacity with light clients, **6b2** gateway loss and storms with SDK clients, **6b3** population events. This plan is three tasks (~55KB), inside the ~80KB per-session limit, so it is not split again.

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module.
- `make test` (`go test -race -count=1 ./...`) and `make test-scripts` stay green on the dev machine (2 cores / 2GB), loopback only.
- Scripts: bash with `set -uo pipefail`; `scripts/tier2/lib.sh` writes JSON without jq; python3 only reads JSON (and does arithmetic on it).
- Ports (choice #76): `STACK_PORT_BASE` 17000 on Tier 2, 17500 in `stack_test.sh`; loadgen's control API is `STACK_PORT_BASE + 5`.
- CPU sets (#71): gateway `4-9,16-21` (6 Zen 5c cores, 12 CPUs), Logic + Redis `10,11,22,23`, loadgen `0-3,12-15` (8 CPUs). Capacity is reported per gateway core count (spec: *CPU isolation*).
- Loadgen sources 127.0.0.2… (#69); one gateway gets ⌈600,000 ÷ 20,000⌉ = 30 sources by default (`New` sizes for `max(1, N−1)` gateways, 6a2b minor 1).
- Spec numbers: capacity ceiling 600k per gateway (a benchmark, not the operating point); operating point ≤ 200k; heartbeat 150s, and 60s at 200k for (c) (R4M-6); gateway heartbeat range 30s–300s; dead after 2 × heartbeat + 10s.
- Swap during a measured window fails the window (#70); the user closes the desktop apps before the run (the host is also a desktop).
- Comments and names match the surrounding code; new behaviour cites its choice number (#88–#92).

## Review Focus

1. **600k closes at once must not drop Logouts.** At a phase's end loadgen closes every connection in seconds; each queues one fenced Logout, and the queue was 250k whatever `-max-conns` said, so ~350k would be dropped and left to the sweeper (`logouts_total{result="dropped"}`). The queue now follows the cap unless set (#89): `TestLogoutQueueFollowsTheCap` (Task 1); on the host, `P_idle` fails unless one Logout per client is done (Task 3).
2. **A metric that cannot be read is never 0.** A gateway holding 600k connections may answer `/metrics` slowly; a reader that turned a failed scrape into 0 would declare the phase boundary reached while 600k Logouts are still pending. `stack_metric` prints `null` for a process that does not answer or a metric it lacks, and `stack_wait_idle` counts `null` as busy (`metric: no such process`, `metric: a name is not a prefix`, Task 2).
3. **The phase boundary is open connections *and* pending Logouts** (#90): a gateway at 0 connections can still hold hundreds of thousands of closed connections waiting for their fenced Logout; the next phase would then share Logic and Redis with them. `idle: an open socket keeps it busy` (Task 2) pins the connections half; the Logout half is checked on the host by `P_idle`, which also counts one Logout done per client (the dev machine cannot hold Logouts back without the fault hooks).
4. **Loadgen must not be the bottleneck** (P6a2aI-4): a ceiling measured while loadgen's 8 CPUs are saturated is loadgen's number, not the gateway's. `P_loadgen_headroom` fails a phase whose loadgen used ¾ or more of its CPU set in the ramp or the window, and loadgen's CPU profile is kept beside the gateway's (Task 3).
5. **A GOMEMLIMIT below the live heap makes the GC run continuously** rather than failing: with `GOGC=off` the collector runs only at the limit, so a limit below the live heap means back-to-back cycles, Pongs late or missing, and a gateway still "up". `P_pongs` fails on any missing Pong and the phase records `gc_cycles` and `gc_mark_cpu_pct` from the window (Task 3); the tuned limit (4GiB) is about twice run 13's extrapolated 600k RSS (~2GB).

## Implementation choices this plan makes

Proposed 2026-10-10 with this plan; **decided 2026-10-10 by Claude at the user's request** (recorded in the roadmap as #88–#92): #88, #89, #92 as proposed; #90 and #91 amended (the amendments are in the text below and in *Execution Notes → Plan review*). Execution: inline (superpowers:executing-plans), in a fresh session.

88. **One `capacity` scenario for (a) and (c), four phases, each on a fresh one-gateway stack.** (c) at 600k and 150s is the same population as (a), so one scenario runs `a_default` (600k, 150s, Go's GC defaults), `a_tuned` (600k, 150s, `GOGC=off GOMEMLIMIT=4GiB`), `c200_150` and `c200_60` (200k at 150s and 60s, tuned), in that order; `CAPACITY_PHASES` picks a subset for a rerun. The gateway has the whole gateway CPU set (12 CPUs, `GOMAXPROCS` 12 by #79's rule with one gateway), so the ceiling is reported for 6 Zen 5c cores. A fresh stack per phase, because `-heartbeat` and the GC environment are fixed at start. Cap `-max-conns 650000` (600k plus ~8% for retries in flight; nofile needs 715k of 1,048,576, conntrack 1.3M of 4.2M); ramp 5,000/s (run 13: ~2% pre-handshake retries, no holes); window = whole heartbeats, at least 120s (150s at 150s, 120s at 60s), after a 10s settle, with 30s CPU profiles of the gateway, Logic and loadgen at its start; uid prefix = the phase name. **Tuned values:** `GOGC=off` with `GOMEMLIMIT=4GiB`: the GC runs only when the heap nears the limit, trading memory for CPU, which is the pod setting a fixed-memory gateway uses (a pod memory limit of ~4.5GiB with the limit at ~90%); 4GiB is about twice run 13's extrapolated 600k RSS. `a_default` is the baseline the tuning is judged against; `CAPACITY_GOGC`/`CAPACITY_GOMEMLIMIT` rerun with other values. `STACK_GATEWAY_ENV` in `stack.sh` carries the environment.
89. **The gateway's Logout queue defaults to `-max-conns`** (`cmd/gateway`; `Config` already documents `LogoutQueue` as "(MaxConns)", but the flag kept the 250k default whatever the cap). An explicit `-logout-queue` still wins. Without it, the 600k phases drop ~350k Logouts at their end, and production with a raised cap would too in a whole-population reconnect.
90. **The phase boundary** (P6a2bI-3) is every running gateway at 0 open connections **and** 0 closed connections awaiting their fenced Logout (`goim_gateway_connections` summed over states, `goim_gateway_awaiting_logout`), within 300s, with one Logout done per client (`logouts_total{result="done"}` = clients; amended in review: `result="dropped"` has no sample until the first drop, so its absence could not tell 0 from an unread metric); dropped Logouts are recorded; `stop_s` records how long loadgen's stop took to reach it. A gateway that does not answer `/metrics` counts as busy. `stack_wait_idle SECONDS` in `stack.sh`; 6b2 and 6b3 use it between phases on a shared stack.
91. **What a phase records:** per process (gateway, Logic, Redis, devtoken, loadgen) CPU-seconds, cores and RSS over the ramp and over the window (`stack_usage_start`/`stack_usage_json`); the gateway's GC from its Go collector (`go_gc_duration_seconds` count, sum and max, `go_memstats_heap_inuse_bytes`, `go_memstats_next_gc_bytes`, `go_goroutines`) and the GC mark share of its CPU profile (`runtime.gcBgMarkWorker` + `runtime.gcAssistAlloc`, cum%, sweeping not included); Pongs in the window and **µs of gateway CPU per Pong** (all gateway CPU in the window ÷ Pongs: the heartbeat cost per client per interval, including the idle work around it); login latency, Pong RTT, `failed` and sources; and (amended in review) the **live heap**: after the window, a heap profile with `?gc=1` (`gw-0.heap-live.pb.gz`), then `live_heap_mb` (heap in use after that GC, an upper bound of live bytes; the profile has the exact ones) and `live_bytes_per_conn`, because with `GOGC=off` the heap may not be collected after the ramp, so RSS ≈ the limit and the window's heap profile is stale. A `summary` field lists per phase the gateway's cores, RSS, live heap and bytes per connection, µs per Pong, GC mark share, ramp cores and loadgen's RSS. The numbers are fields, not checks: the spec gives no threshold for (a) or (c).
92. **Loadgen headroom is a check** (P6a2aI-4): a phase fails when loadgen used ¾ or more of its CPU set (6 of 8 CPUs) in the ramp or the window, since the gateway's numbers would then be loadgen's. Loadgen's CPU profile (`lg_profile`) is saved beside the gateway's.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `cmd/gateway/main.go` | modify | `-logout-queue` defaults to `-max-conns` (#89) |
| `cmd/gateway/main_test.go` | modify | `TestLogoutQueueFollowsTheCap` |
| `scripts/stack.sh` | modify | `STACK_GATEWAY_ENV` (#88), `stack_metric`, `stack_wait_idle` (#90), `stack_ticks`, `stack_usage_start`, `stack_usage_json` (#91) |
| `scripts/stack_test.sh` | modify | tests for the above; the `pipefail` flake fix |
| `scripts/loadgen.sh` | modify | `lg_profile` (#92) |
| `scripts/tier2/capacity.sh` | create | the Tier 2 `capacity` scenario (#88–#92) |
| `Makefile` | modify | `capacity` in the `test-tier2` scenario list |

---

### Task 1: the Logout queue follows the connection cap (#89)

**Files:**
- Modify: `cmd/gateway/main.go` (`parseFlags`)
- Test: `cmd/gateway/main_test.go`

**Interfaces:**
- Consumes: `parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config, options, error)`; `gateway.Config.MaxConns`, `.LogoutQueue`.
- Produces: `cfg.LogoutQueue == cfg.MaxConns` unless `-logout-queue` is given. Task 3's gateway runs with `-max-conns 650000` and no `-logout-queue`.

- [ ] **Step 1: Write the failing test**

Add to `cmd/gateway/main_test.go`, before `// The race suite's flags (Plan 5b).`:

```go
// The Logout queue follows the connection cap unless set (#89): a gateway
// whose connections all close at once (a loadgen stop, a whole-population
// reconnect) queues one Logout per connection, and a queue below the cap
// drops the excess, leaving sessions for the sweeper.
func TestLogoutQueueFollowsTheCap(t *testing.T) {
	for _, c := range []struct {
		args []string
		want int
	}{
		{nil, 250_000},
		{[]string{"-max-conns", "650000"}, 650_000},
		{[]string{"-max-conns", "650000", "-logout-queue", "1000"}, 1000},
		{[]string{"-logout-queue", "1000", "-max-conns", "650000"}, 1000},
	} {
		cfg, _, _, _, err := parseFlags(append([]string{"-id", "gw-0"}, c.args...))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.LogoutQueue != c.want {
			t.Errorf("%v: logout queue %d, want %d", c.args, cfg.LogoutQueue, c.want)
		}
	}
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `go test -count=1 -run TestLogoutQueueFollowsTheCap ./cmd/gateway/`
Expected: FAIL: `[-max-conns 650000]: logout queue 250000, want 650000`.

- [ ] **Step 3: Implement**

In `cmd/gateway/main.go`:

```diff
diff --git a/cmd/gateway/main.go b/cmd/gateway/main.go
index 466e232..0625b0d 100644
--- a/cmd/gateway/main.go
+++ b/cmd/gateway/main.go
@@ -143,7 +143,7 @@ func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config
 	fs.IntVar(&cfg.MaxConns, "max-conns", cfg.MaxConns, "connection cap: open sockets in any state")
 	fs.IntVar(&cfg.PendingCap, "pending-cap", cfg.PendingCap, "HANDSHAKE + AUTH_PENDING sockets")
 	fs.IntVar(&cfg.LoginQueueCap, "login-queue-cap", cfg.LoginQueueCap, "Logins waiting for or inside a Login RPC")
-	fs.IntVar(&cfg.LogoutQueue, "logout-queue", cfg.LogoutQueue, "Logouts queued or waiting for a retry")
+	fs.IntVar(&cfg.LogoutQueue, "logout-queue", cfg.LogoutQueue, "Logouts queued or waiting for a retry (default: -max-conns)")
 	fs.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", cfg.HandshakeTimeout, "accept → handshake done (3s–30s)")
 	fs.DurationVar(&cfg.AuthTimeout, "auth-timeout", cfg.AuthTimeout, "handshake done → LoginReq (3s–30s)")
 	fs.DurationVar(&cfg.Heartbeat, "heartbeat", cfg.Heartbeat, "heartbeat interval sent in LoginResp")
@@ -179,6 +179,13 @@ func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config
 	if err := fs.Parse(args); err != nil {
 		return cfg, sc, tc, o, err
 	}
+	// The Logout queue defaults to the cap (#89): when every connection
+	// closes at once, each queues one Logout, and a full queue drops it.
+	logoutQueueSet := false
+	fs.Visit(func(f *flag.Flag) { logoutQueueSet = logoutQueueSet || f.Name == "logout-queue" })
+	if !logoutQueueSet {
+		cfg.LogoutQueue = cfg.MaxConns
+	}
 	tc.HandshakeConcurrency = tcp.DefaultConfig(cfg.NumLoops).HandshakeConcurrency
 	if *concurrency != 0 {
 		tc.HandshakeConcurrency = *concurrency
```

- [ ] **Step 4: Run the package's tests**

Run: `gofmt -l cmd/gateway; go test -race -count=1 ./cmd/gateway/`
Expected: no file listed; `ok`. (`TestDefaultFlagsAreValid` still passes: with the default cap the queue stays 250k.)

- [ ] **Step 5: Commit**

```bash
git add cmd/gateway/main.go cmd/gateway/main_test.go
git commit -m "fix(gateway): -logout-queue defaults to -max-conns (#89)"
```

---

### Task 2: stack and loadgen helpers for capacity phases (#88, #90–#92)

**Files:**
- Modify: `scripts/stack.sh`, `scripts/loadgen.sh`
- Test: `scripts/stack_test.sh` (run by `make test-scripts`)

**Interfaces:**
- Consumes: `stack_admins` ("NAME ADMIN" per running process), `STACK_ORDER`, `STACK_PIDS`, `lg_control` (all existing).
- Produces, for Task 3 and Plans 6b2/6b3:
  - `STACK_GATEWAY_ENV` — bash array of `VAR=VALUE`, put in every gateway's environment by `stack_up` (after `GOMAXPROCS`).
  - `stack_metric NAME METRIC [MATCH]` — prints the sum of METRIC's samples on NAME's `/metrics` (exact name; lines containing MATCH if given), an integer when whole, else `null`.
  - `stack_wait_idle SECONDS` — returns 0 once every running gateway has `goim_gateway_connections` 0 and `goim_gateway_awaiting_logout` 0; else 1 after SECONDS with `stack: not idle after Ns: gw-0: connections C, awaiting_logout A` on stderr.
  - `stack_usage_start [NAME=PID...]`, then `stack_usage_json` — prints `{"window_s": W, NAME: {"cpu_s", "cores", "rss_mb"} | null, ...}` for every stack process and extra.
  - `lg_profile NAME SECONDS` — loadgen's CPU profile into `$STACK_DIR/artifacts/NAME/loadgen.cpu.pb.gz`.

- [ ] **Step 1: Write the failing tests**

Apply to `scripts/stack_test.sh` (also the flake fix: `stack_ports | grep -qx` under `pipefail` fails when `grep -q` exits on its first match and `stack_ports` dies of SIGPIPE writing the rest; seen once in the prototype, and `contains` moves up to its first use):

```diff
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index afa1822..df9c318 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -23,10 +23,11 @@ ncpu=$(getconf _NPROCESSORS_ONLN)
 T2_CPUS_GATEWAY=0-$((ncpu - 1))
 T2_CPUS_LOGIC=0
 STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000)
+STACK_GATEWAY_ENV=(GOGC=50 GOMEMLIMIT=1GiB)
 procs=$((ncpu / 2 > 1 ? ncpu / 2 : 1))
 
 # The ports checked before stack_up include loadgen's control port.
-stack_ports | grep -qx $((STACK_PORT_BASE + 5)) && ok "ports: loadgen's control port" || bad "ports: loadgen's control port missing"
+grep -qx $((STACK_PORT_BASE + 5)) <<<"$(stack_ports)" && ok "ports: loadgen's control port" || bad "ports: loadgen's control port missing"
 
 # stack_term records what wait reports, "killed" only for a process our
 # SIGKILL ended (M1): a kill -KILL that lands on an exited, unreaped process
@@ -92,6 +93,45 @@ for f in logic gw-0 gw-1; do
   grep -q 'goroutine profile:' "$STACK_DIR/artifacts/idle/$f.goroutine.txt" 2>/dev/null && ok "snapshot: $f goroutines" || bad "snapshot: $f goroutines"
   gzip -t "$STACK_DIR/artifacts/idle/$f.heap.pb.gz" 2>/dev/null && ok "snapshot: $f heap profile" || bad "snapshot: $f heap profile"
 done
+# The gateways run with STACK_GATEWAY_ENV (#88), Logic without it.
+for name in gw-0 gw-1; do
+  expect "env: $name has GOGC and GOMEMLIMIT" "$(tr '\0' '\n' <"/proc/${STACK_PIDS[$name]}/environ" | grep -E '^GO(GC|MEMLIMIT)=' | sort | tr '\n' ' ')" "GOGC=50 GOMEMLIMIT=1GiB "
+done
+expect "env: Logic has no GOGC" "$(tr '\0' '\n' <"/proc/${STACK_PIDS[logic]}/environ" | grep -c '^GOGC=')" 0
+
+# stack_metric sums a metric's samples, filtered by a label; null when absent.
+expect "metric: a gauge" "$(stack_metric gw-0 goim_gateway_tracked)" 0
+expect "metric: summed over labels" "$(stack_metric gw-0 goim_gateway_connections)" 0
+expect "metric: filtered by a label" "$(stack_metric gw-0 goim_gateway_limit 'limit="nofile"')" "$(ulimit -n)"
+expect "metric: a name is not a prefix" "$(stack_metric gw-0 go_gc_duration_seconds_coun)" null
+expect "metric: no such metric" "$(stack_metric gw-0 goim_no_such_metric)" null
+expect "metric: no such process" "$(stack_metric gw-9 goim_gateway_ready)" null
+case $(stack_metric gw-0 go_gc_duration_seconds 'quantile="1"') in
+  null | "") bad "metric: a summary quantile" ;; *) ok "metric: a summary quantile" ;;
+esac
+
+# The phase boundary (P6a2bI-3): idle at once on an idle stack; a socket open
+# on gw-0 keeps it busy, named, until the socket closes.
+stack_wait_idle 5 && ok "idle: an idle stack" || bad "idle: an idle stack"
+python3 -c 'import socket,sys,time; s=socket.create_connection(("127.0.0.1", int(sys.argv[1]))); time.sleep(3)' $((STACK_PORT_BASE + 100)) &
+holder=$!
+sleep 0.5
+out=$(stack_wait_idle 1 2>&1) && bad "idle: an open socket must keep it busy" || ok "idle: an open socket keeps it busy"
+contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
+contains "idle: names the gateway" "$out" "gw-0: connections 1"
+wait $holder
+stack_wait_idle 10 && ok "idle: after the socket closed" || bad "idle: after the socket closed"
+
+# stack_usage: CPU and RSS per process over a window, extras included; a
+# process gone since is null.
+sleep 30 & extra=$!
+stack_usage_start extra=$extra gone=999999999
+sleep 1
+kill $extra; wait $extra 2>/dev/null
+usage=$(stack_usage_json)
+expect "usage: every process, the extras, and the window" "$(python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(sorted(d), 0.9 < d["window_s"] < 3, d["gw-0"]["rss_mb"] > 0, d["gw-0"]["cpu_s"] >= 0, d["gone"], d["extra"])' "$usage")" \
+  "['devtoken', 'extra', 'gone', 'gw-0', 'gw-1', 'logic', 'redis', 'window_s'] True True True None None"
+
 stack_profile cpu 1
 # A failed profile fails stack_profile, even when it is not the last one
 # (Logic's comes first; 6a2a final review).
@@ -189,7 +229,6 @@ fi
 
 # Pinning without lib.sh is refused, not run with GOMAXPROCS 1 (M6).
 out=$(unset -f t2_cpulist_expand; STACK_PIN=1; stack_up 2>&1; echo "rc=$?"; stack_down)
-contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
 contains "pin: without lib.sh is refused" "$out" "rc=1"
 contains "pin: says lib.sh is needed" "$out" "lib.sh"
 expect "pin: nothing started" "$(grep -c 'Ready\|serving' <<<"$out")" 0
```

- [ ] **Step 2: Run them to see them fail**

Run: `STACK_REDIS_SERVER=$(command -v redis-server || echo .cache/redis/bin/redis-server) scripts/stack_test.sh 2>&1 | grep -E 'FAIL|stack_test:'`
Expected: FAIL lines for `env: gw-0 has GOGC and GOMEMLIMIT`, `env: gw-1 …`, every `metric: …` line, `idle: …` and `usage: …` (`stack_metric: command not found` and the like), and `stack_test: N failed`.

- [ ] **Step 3: Implement**

Apply to `scripts/stack.sh`:

```diff
diff --git a/scripts/stack.sh b/scripts/stack.sh
index b77c2e1..1ae5196 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -11,6 +11,10 @@
 #                               #   -> $STACK_DIR/artifacts/NAME/
 #   stack_profile NAME SECONDS  # CPU profiles of Logic and every gateway at once -> artifacts/NAME/
 #   stack_expect_dead NAME      # NAME was killed on purpose (loadgen /kill): reap and record it
+#   stack_metric NAME METRIC [MATCH]  # sum of METRIC's samples (with MATCH in the line) on NAME's /metrics
+#   stack_wait_idle SECONDS     # phase boundary: every gateway at 0 connections and 0 awaiting Logout
+#   stack_usage_start [N=PID..] # start a CPU window over the stack's processes (and N=PID extras)
+#   stack_usage_json            # {"window_s", NAME: {"cpu_s", "cores", "rss_mb"}...} since the start
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
 #   stack_health                # one line per problem: dead process, unclean stop, panic in a log
 #
@@ -26,6 +30,7 @@
 #   STACK_GATEWAY_PROCS  each gateway's GOMAXPROCS (default: max(1, CPUs / STACK_GATEWAYS),
 #                        CPUs = T2_CPUS_GATEWAY when pinned, else all online; #79)
 #   STACK_GATEWAY_ARGS, STACK_LOGIC_ARGS   arrays of extra flags
+#   STACK_GATEWAY_ENV    array of VAR=VALUE for every gateway, e.g. (GOGC=off GOMEMLIMIT=4GiB) (#88)
 #   STACK_DRAIN_WINDOW   gateway -drain-window (default 1s, so stack_down's drain ends and
 #                        exits 0 well inside STACK_DOWN_WAIT; a measured drain sets 60s)
 #   STACK_DOWN_WAIT      seconds stack_down waits for the gateways to drain (default 10)
@@ -179,7 +184,7 @@ stack_up() {
   stack_wait logic "http://$admin/readyz" || return 1
   for ((i = 0; i < n; i++)); do
     local listen=127.0.0.1:$((base + 100 + 10 * i)) gadmin=127.0.0.1:$((base + 102 + 10 * i))
-    stack_start "gw-$i" "$gw_cpus" env GOMAXPROCS="$procs" "$bin/gateway" -id "gw-$i" -listen "tcp://$listen" \
+    stack_start "gw-$i" "$gw_cpus" env GOMAXPROCS="$procs" "${STACK_GATEWAY_ENV[@]}" "$bin/gateway" -id "gw-$i" -listen "tcp://$listen" \
       -noise-keys "$STACK_DIR/keys/noise" -logic "$grpc" -lease "$lease" \
       -kick-addr "127.0.0.1:$((base + 101 + 10 * i))" -admin-addr "$gadmin" \
       -drain-window "${STACK_DRAIN_WINDOW:-1s}" "${STACK_GATEWAY_ARGS[@]}"
@@ -247,6 +252,83 @@ stack_profile() {
   return $rc
 }
 
+# stack_metric NAME METRIC [MATCH]: the sum of METRIC's samples on NAME's
+# /metrics (lines "METRIC{...} V" or "METRIC V", and containing MATCH if
+# given, e.g. 'state="online"'), as an integer when whole; "null" if NAME
+# does not answer or has no such sample, so it can go straight into JSON.
+stack_metric() {
+  local admin body
+  admin=$(stack_admins | awk -v n="$1" '$1 == n { print $2 }')
+  [ -n "$admin" ] && body=$(curl -sf -m 10 "http://$admin/metrics") || { echo null; return; }
+  awk -v m="$2" -v want="${3:-}" '
+    ($1 == m || index($1, m "{") == 1) && index($0, want) { s += $NF; n++ }
+    END { if (!n) print "null"; else if (s == int(s)) printf "%d\n", s; else printf "%.6g\n", s }' <<<"$body"
+}
+
+# stack_wait_idle SECONDS: the phase boundary on a shared stack (P6a2bI-3):
+# waits until every running gateway has no open connection and no closed
+# one still awaiting its fenced Logout, so a stopped loadgen's closes and
+# Logouts do not spill into the next phase. Fails, naming what is left, after
+# SECONDS. A gateway that does not answer counts as busy.
+stack_wait_idle() {
+  local end=$((SECONDS + $1)) name admin busy c a
+  while :; do
+    busy=""
+    while read -r name admin; do
+      [[ $name == gw-* ]] || continue
+      c=$(stack_metric "$name" goim_gateway_connections)
+      a=$(stack_metric "$name" goim_gateway_awaiting_logout)
+      [ "$c" = 0 ] && [ "$a" = 0 ] || busy+=" $name: connections $c, awaiting_logout $a;"
+    done < <(stack_admins)
+    [ -z "$busy" ] && return 0
+    if [ $SECONDS -ge $end ]; then
+      echo "stack: not idle after $1s:${busy%;}" >&2
+      return 1
+    fi
+    sleep 0.5
+  done
+}
+
+# stack_ticks PID: utime + stime of PID in clock ticks (every thread), read
+# after the last ')' so a comm with spaces cannot shift the fields.
+stack_ticks() {
+  awk '{ n = length($0); while (n > 0 && substr($0, n, 1) != ")") n--
+         split(substr($0, n + 2), f, " "); print f[12] + f[13] }' "/proc/$1/stat" 2>/dev/null
+}
+
+# stack_usage_start [NAME=PID...]: starts a CPU window over every running
+# stack process and the NAME=PID extras (loadgen), for per-process sizing:
+# CPU-seconds and cores in the window, resident memory at its end.
+declare -A STACK_USAGE0=()
+STACK_USAGE_PIDS=()
+STACK_USAGE_T0=0
+stack_usage_start() {
+  local name e
+  STACK_USAGE0=() STACK_USAGE_PIDS=()
+  for name in "${STACK_ORDER[@]}"; do STACK_USAGE_PIDS+=("$name=${STACK_PIDS[$name]}"); done
+  STACK_USAGE_PIDS+=("$@")
+  for e in "${STACK_USAGE_PIDS[@]}"; do STACK_USAGE0[${e%%=*}]=$(stack_ticks "${e#*=}"); done
+  STACK_USAGE_T0=$(date +%s%N)
+}
+
+# stack_usage_json: {"window_s": W, NAME: {"cpu_s", "cores", "rss_mb"}, ...}
+# since stack_usage_start (cores = cpu_s / W); a process gone since is null.
+stack_usage_json() {
+  local hz e name pid t1 out
+  hz=$(getconf CLK_TCK)
+  t1=$(date +%s%N)
+  out="\"window_s\": $(awk -v d=$((t1 - STACK_USAGE_T0)) 'BEGIN { printf "%.1f", d / 1e9 }')"
+  for e in "${STACK_USAGE_PIDS[@]}"; do
+    name=${e%%=*} pid=${e#*=}
+    out+=", \"$name\": $(awk -v t0="${STACK_USAGE0[$name]:-}" -v t1="$(stack_ticks "$pid")" -v hz="$hz" \
+      -v w=$((t1 - STACK_USAGE_T0)) -v rss="$(awk '/^VmRSS:/ { print $2 }' "/proc/$pid/status" 2>/dev/null)" '
+      BEGIN { if (t0 == "" || t1 == "" || rss == "") { print "null"; exit }
+              c = (t1 - t0) / hz
+              printf "{\"cpu_s\": %.2f, \"cores\": %.3f, \"rss_mb\": %d}", c, c / (w / 1e9), rss / 1024 }')"
+  done
+  echo "{$out}"
+}
+
 # stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
 stack_pinned() {
   local have
```

Apply to `scripts/loadgen.sh`:

```diff
diff --git a/scripts/loadgen.sh b/scripts/loadgen.sh
index c7418f1..80553fc 100644
--- a/scripts/loadgen.sh
+++ b/scripts/loadgen.sh
@@ -7,6 +7,7 @@
 #   lg_get PATH EXPR            # EXPR (python, over the JSON as d) of GET PATH on the control API
 #   lg_post PATH                # POST PATH; prints the reply, fails on an HTTP error
 #   lg_wait_back MARK SECONDS   # until every client of MARK logged in again
+#   lg_profile NAME SECONDS     # loadgen's CPU profile over SECONDS -> $STACK_DIR/artifacts/NAME/loadgen.cpu.pb.gz
 #   lg_stop                     # SIGTERM, then wait (60s) for it to write its result; LG_STATUS = exit status
 #   lg_json EXPR                # EXPR over the result file of the last loadgen
 #   lg_true SOURCE EXPR         # "true" if EXPR is true over SOURCE (a file, or - for stdin), else "false"
@@ -86,6 +87,14 @@ lg_wait_back() {
   return 1
 }
 
+# lg_profile NAME SECONDS: loadgen's own CPU profile (its control API serves
+# pprof), beside stack_profile's, so a run shows whether loadgen was the
+# bottleneck (P6a2aI-4). Blocks for SECONDS.
+lg_profile() {
+  mkdir -p "$STACK_DIR/artifacts/$1"
+  curl -sf -m $(($2 + 30)) "http://$(lg_control)/debug/pprof/profile?seconds=$2" >"$STACK_DIR/artifacts/$1/loadgen.cpu.pb.gz"
+}
+
 # lg_stop sets LG_STATUS rather than printing it: in $(...) it would run in a
 # subshell, which cannot wait for the loadgen process.
 lg_stop() {
```

- [ ] **Step 4: Run the script tests**

Run: `bash -n scripts/stack.sh scripts/loadgen.sh && make test-scripts 2>&1 | grep -E 'FAIL|all passed|failed'`
Expected: `lib_test: all passed`, `loadgen_test: all passed`, `stack_test: all passed`; no FAIL.

- [ ] **Step 5: Check the boundary test catches a boundary that ignores connections**

Mutant: in `stack_wait_idle`, replace `[ "$c" = 0 ] && [ "$a" = 0 ] || busy+=` with `[ "$a" = 0 ] || busy+=`, rerun `scripts/stack_test.sh`.
Expected: `FAIL idle: an open socket must keep it busy`, `FAIL idle: names the gateway: [] lacks [gw-0: connections 1]`. Restore the line (`git diff scripts/stack.sh` shows only Step 3's change).

- [ ] **Step 6: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh scripts/loadgen.sh
git commit -m "feat(stack): gateway env, metric reader, phase boundary, CPU/RSS windows (#88, #90-#92)"
```

---

### Task 3: the Tier 2 `capacity` scenario, and Tier 2 run 14

**Files:**
- Create: `scripts/tier2/capacity.sh`
- Modify: `Makefile`

**Interfaces:**
- Consumes: Task 1's Logout queue default; Task 2's `STACK_GATEWAY_ENV`, `stack_metric`, `stack_wait_idle`, `stack_usage_start`, `stack_usage_json`, `lg_profile`; `lib.sh` (`t2_init`, `t2_check`, `t2_field`, `t2_record_start/end NAME`, `t2_mem_budget`, `t2_nofile`, `t2_require_host_setup`, `t2_cpulist_expand`, `t2_finish`), `stack.sh` (`stack_build`, `stack_up`, `stack_profile`, `stack_snapshot`, `stack_pinned`, `stack_down`, `stack_health`, `stack_busy_ports`), `loadgen.sh` (`lg_start`, `lg_wait_online`, `lg_get`, `lg_stop`, `lg_json`, `lg_true`, `lg_py`).
- Produces: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=capacity`; `result.json` with, per phase P in `a_default a_tuned c200_150 c200_60`, checks `P_stack`, `P_online`, `P_pongs`, `P_held`, `no_swap_in_P`, `P_loadgen_headroom`, `P_idle`, `P_processes`, plus `pinning` and `artifacts` (34 checks); fields P (#91), `background_P`, `summary`, `mem_budget`; durations `setup` and P. Settings `CAPACITY_PHASES`, `CAPACITY_GOGC`, `CAPACITY_GOMEMLIMIT`, `CAPACITY_CAP`, `CAPACITY_RATE`, `CAPACITY_WINDOW`.

- [ ] **Step 1: Write the scenario**

Create `scripts/tier2/capacity.sh` (mode 755):

```bash
#!/usr/bin/env bash
# Tier 2: capacity (Plan 6b1), the spec's load tests (a) and (c) with light
# clients on ONE gateway that has the whole gateway CPU set (6 Zen 5c cores,
# 12 CPUs, GOMAXPROCS 12; #71, #79):
#   (a) 600k connections on one gateway (the ceiling), with GOMEMLIMIT/GOGC
#       tuning: once with Go's defaults and once tuned (#88);
#   (c) heartbeat CPU cost at 200k and at 600k at 150s, plus 60s at 200k.
#       600k at 150s is (a)'s tuned phase.
# Each phase runs on a fresh stack (the gateway's -heartbeat and GC settings
# are fixed at start) and ends at the phase boundary (P6a2bI-3): loadgen
# stopped, every connection closed and every Logout done, before the stack
# stops.
#
# Usage (on the Tier 2 host, from the repo root), after the one-time
#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=capacity
#   Results: tier2-results/capacity-<timestamp>/ (result.json, one directory
#   per phase with logs, artifacts and loadgen results), plus ONE file to
#   send back: tier2-results/capacity-<timestamp>.report.txt
#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
#   About 20 minutes.
#
# Settings:
#   CAPACITY_PHASES    "NAME:CLIENTS:HEARTBEAT_S:GC ..." (GC: default = Go's
#                      defaults, tuned = CAPACITY_GOGC + CAPACITY_GOMEMLIMIT);
#                      default "a_default:600000:150:default a_tuned:600000:150:tuned
#                      c200_150:200000:150:tuned c200_60:200000:60:tuned"
#   CAPACITY_GOGC      tuned GOGC (default off)
#   CAPACITY_GOMEMLIMIT tuned GOMEMLIMIT (default 4GiB)
#   CAPACITY_CAP       the gateway's -max-conns (default 650000)
#   CAPACITY_RATE      clients started per second (default 5000)
#   CAPACITY_WINDOW    measured window in seconds (default: whole heartbeats,
#                      at least 120s: 150s at 150s, 120s at 60s)
#
# Checks, for each phase P:
#   P_stack            redis, devtoken, Logic and gw-0 become Ready
#   P_online           every client logs in on gw-0; none gave up
#   P_pongs            a Pong for every Ping in the window, one per client per heartbeat
#   P_held             no connection lost
#   no_swap_in_P       no page swapped in during the window (#70)
#   P_loadgen_headroom loadgen used under 3/4 of its CPU set in the ramp and the
#                      window, so the gateway, not loadgen, set the pace (P6a2aI-4)
#   P_idle             after loadgen stops, gw-0 reaches 0 connections and 0
#                      awaiting Logout within 300s, with one Logout done per
#                      client (counted, so a missing metric cannot pass)
#   P_processes        nothing died, panicked or stopped uncleanly
# and once: pinning (in the first phase), artifacts.
set -uo pipefail

. "$(dirname "$0")/lib.sh"
. "$(dirname "$0")/../stack.sh"
. "$(dirname "$0")/../loadgen.sh"
t2_init capacity
t2_need curl taskset sudo python3 go
t2_require_host_setup
t2_mem_budget 16
t2_nofile 1048576

PHASES=${CAPACITY_PHASES:-a_default:600000:150:default a_tuned:600000:150:tuned c200_150:200000:150:tuned c200_60:200000:60:tuned}
TUNED=(GOGC=${CAPACITY_GOGC:-off} GOMEMLIMIT=${CAPACITY_GOMEMLIMIT:-4GiB})
CAP=${CAPACITY_CAP:-650000} RATE=${CAPACITY_RATE:-5000}
export STACK_GATEWAYS=1 STACK_PIN=1
STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
t2_on_exit 'stack_down'
t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack

t2_report_extra() {
  local f
  for f in "$T2_OUT"/*/logs/*.log; do
    [ -f "$f" ] || continue
    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
    tail -n 20 "$f"
  done
}

# gc_mark_cpu_pct PROFILE: the share of the profile's samples in the GC's
# mark workers and mark assists (sweeping not included), in percent; null if
# go tool pprof cannot read it.
gc_mark_cpu_pct() {
  go tool pprof -top "$1" 2>/dev/null | awk '
    $NF == "runtime.gcBgMarkWorker" || $NF == "runtime.gcAssistAlloc" { s += $5; n++ }
    /^Showing nodes/ { ok = 1 }
    END { if (ok) printf "%.1f\n", s; else print "null" }'
}

# usage_ok JSON: loadgen used under 3/4 of its CPU set.
usage_ok() { [ "$(echo "$1" | lg_true - 'd["loadgen"]["cores"] < '"$LG_CPUS"' * 0.75')" = true ]; }

t=$SECONDS
if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
  exit 1
fi
STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
t2_duration setup $((SECONDS - t))

summary=()
first=1
for phase in $PHASES; do
  IFS=: read -r name clients hb gc <<<"$phase"
  window=${CAPACITY_WINDOW:-$(( (120 + hb - 1) / hb * hb ))}
  export STACK_DIR=$T2_OUT/$name
  mkdir -p "$STACK_DIR"
  ln -sfn ../bin "$STACK_DIR/bin"
  STACK_GATEWAY_ARGS=(-max-conns "$CAP" -heartbeat "${hb}s")
  STACK_GATEWAY_ENV=()
  [ "$gc" = tuned ] && STACK_GATEWAY_ENV=("${TUNED[@]}")
  t0=$SECONDS

  # The stack, fresh for each phase.
  busy=$(stack_busy_ports)
  [ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
  stack_up 2>"$STACK_DIR/stack-up.err"
  up=$?
  t2_check "${name}_stack" "$(t2_bool [ $up -eq 0 ])" "\"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
  [ $up -eq 0 ] || t2_finish

  # The ramp.
  lg_start "$name" -mode light -clients "$clients" -rate "$RATE" -ping-every "${hb}s" -uid-prefix "$name"
  stack_usage_start loadgen="$LG_PID"
  t=$SECONDS
  online=false
  lg_wait_online "$clients" $((clients / RATE * 3 + 60)) && online=$(lg_get /stats 'd["online_by_gateway"].get("gw-0") == '"$clients"' and "gave-up" not in d["failed"]')
  ramp_s=$((SECONDS - t))
  ramp=$(stack_usage_json)
  t2_check "${name}_online" "$online" "\"want\": $clients, \"online\": $(lg_get /stats 'd["online"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"

  if [ -n "$first" ]; then
    first=""
    wrong=""
    stack_pinned gw-0 "$T2_CPUS_GATEWAY" || wrong+=" gw-0"
    for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
    have=$(taskset -pc "$LG_PID" 2>/dev/null)
    [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
    t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
  fi

  # The window: whole heartbeats after a 10s settle, with CPU profiles of
  # its first 30s and the gateway's GC counters at both ends.
  sleep 10 & wait $!
  pongs0=$(lg_get /stats 'd["pongs"]')
  gc0=$(stack_metric gw-0 go_gc_duration_seconds_count)
  pause0=$(stack_metric gw-0 go_gc_duration_seconds_sum)
  t2_record_start "$name"
  stack_usage_start loadgen="$LG_PID"
  stack_profile window 30 & p1=$!
  lg_profile window 30 & p2=$!
  sleep "$window" & wait $!
  wait $p1; wait $p2
  win=$(stack_usage_json)
  t2_record_end "$name"
  pongs1=$(lg_get /stats 'd["pongs"]')
  gw=$(python3 -c 'import json, sys
def n(s):
    if s == "null": return None
    v = float(s)
    return int(v) if v.is_integer() else v
gc0, gc1, p0, p1 = map(n, sys.argv[1:5])
print(json.dumps({"gc_cycles_before_window": gc0,
  "gc_cycles": None if None in (gc0, gc1) else gc1 - gc0,
  "gc_pause_s": None if None in (p0, p1) else round(p1 - p0, 4),
  "gc_pause_max_s": n(sys.argv[5]), "gc_mark_cpu_pct": n(sys.argv[6]),
  "heap_inuse_mb": None if sys.argv[7] == "null" else int(n(sys.argv[7]) / 2**20),
  "next_gc_mb": None if sys.argv[8] == "null" else int(n(sys.argv[8]) / 2**20),
  "goroutines": n(sys.argv[9])}))' \
    "$gc0" "$(stack_metric gw-0 go_gc_duration_seconds_count)" "$pause0" "$(stack_metric gw-0 go_gc_duration_seconds_sum)" \
    "$(stack_metric gw-0 go_gc_duration_seconds 'quantile="1"')" "$(gc_mark_cpu_pct "$STACK_DIR/artifacts/window/gw-0.cpu.pb.gz")" \
    "$(stack_metric gw-0 go_memstats_heap_inuse_bytes)" "$(stack_metric gw-0 go_memstats_next_gc_bytes)" "$(stack_metric gw-0 go_goroutines)")
  stack_snapshot window
  # The live heap: after the window, a forced GC (with GOGC=off the heap may
  # not have been collected since the ramp), then heap in use and a heap
  # profile of what is live, for bytes per connection and pooling.
  curl -sf -m 120 "http://$(stack_admins | awk '$1 == "gw-0" { print $2 }')/debug/pprof/heap?gc=1" >"$STACK_DIR/artifacts/window/gw-0.heap-live.pb.gz"
  live=$(stack_metric gw-0 go_memstats_heap_inuse_bytes)
  live_mb=$([ "$live" = null ] && echo null || echo $((live / 1048576)))
  per_conn=$([ "$live" = null ] && echo null || echo $((live / clients)))
  pings=$(python3 -c 'import sys; print(int(sys.argv[2]) - int(sys.argv[1]))' "$pongs0" "$pongs1" 2>/dev/null || echo null)
  per=$(python3 -c 'import json, sys
w, p = json.loads(sys.argv[1]), int(sys.argv[2])
print(round(w["gw-0"]["cpu_s"] / p * 1e6, 1) if p > 0 and w.get("gw-0") else "null")' "$win" "$pings" 2>/dev/null || echo null)

  # The phase boundary (P6a2bI-3): the result is written before any client
  # stops (#84), then the stop is timed until gw-0 is idle.
  lg_stop
  res=$STACK_DIR/artifacts/loadgen-$name.json
  t2_check "${name}_pongs" "$(lg_true "$res" 'd["pong_missing"] == 0 and '"$pings"' >= '"$clients"' * '"$window"' // '"$hb"' * 0.99')" \
    "\"pings_in_window\": $pings, \"want\": $((clients * window / hb)), \"missing\": $(lg_json 'd["pong_missing"]')"
  t2_check "${name}_held" "$(lg_true "$res" 'd["lost"] == {}')" "\"lost\": $(lg_json 'd["lost"]')"
  headroom=false
  usage_ok "$ramp" && usage_ok "$win" && headroom=true
  t2_check "${name}_loadgen_headroom" "$headroom" \
    "\"loadgen_cpus\": $LG_CPUS, \"ramp_cores\": $(echo "$ramp" | lg_py - 'd["loadgen"]["cores"]'), \"window_cores\": $(echo "$win" | lg_py - 'd["loadgen"]["cores"]')"
  t=$SECONDS
  idle=$(t2_bool stack_wait_idle 300 2>"$STACK_DIR/idle.err")
  stop_s=$((SECONDS - t))
  done_logouts=$(stack_metric gw-0 goim_gateway_logouts_total 'result="done"')
  dropped=$(stack_metric gw-0 goim_gateway_logouts_total 'result="dropped"')
  [ "$done_logouts" = "$clients" ] || idle=false
  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"done_logouts\": $done_logouts, \"want\": $clients, \"dropped_logouts\": $dropped, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
  stack_down
  problems=$(stack_health)
  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"

  t2_field "$name" "{\"clients\": $clients, \"heartbeat_s\": $hb, \"window_s\": $window, \"gc\": $(t2_json_str "$gc"), \"gateway_env\": $(t2_json_str "${STACK_GATEWAY_ENV[*]}"), \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"window\": $win, \"gateway\": $gw, \"live_heap_mb\": $live_mb, \"live_bytes_per_conn\": $per_conn, \"pings_in_window\": $pings, \"gateway_us_per_pong\": $per, \"stop_s\": $stop_s, \"login_latency\": $(lg_json 'd["login_latency"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"failed\": $(lg_json 'd["failed"]'), \"sources\": $(lg_json 'd["sources"]')}"
  summary+=("$(t2_json_str "$name"): {\"clients\": $clients, \"heartbeat_s\": $hb, \"gc\": $(t2_json_str "$gc"), \"gateway_cores\": $(echo "$win" | lg_py - 'd["gw-0"]["cores"]'), \"gateway_rss_mb\": $(echo "$win" | lg_py - 'd["gw-0"]["rss_mb"]'), \"live_heap_mb\": $live_mb, \"live_bytes_per_conn\": $per_conn, \"gateway_us_per_pong\": $per, \"gc_mark_cpu_pct\": $(echo "$gw" | lg_py - 'd["gc_mark_cpu_pct"]'), \"ramp_gateway_cores\": $(echo "$ramp" | lg_py - 'd["gw-0"]["cores"]'), \"loadgen_rss_mb\": $(echo "$win" | lg_py - 'd["loadgen"]["rss_mb"]')}")
  t2_duration "$name" $((SECONDS - t0))
done
t2_field summary "{$(t2_join "${summary[@]}")}"

# Artifacts of every phase: the gateway's and Logic's metrics, heap and CPU
# profiles, loadgen's CPU profile and result.
missing=""
for phase in $PHASES; do
  name=${phase%%:*} a=$T2_OUT/${phase%%:*}/artifacts
  for f in logic gw-0; do
    grep -q '^# TYPE' "$a/window/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
    gzip -t "$a/window/$f.heap.pb.gz" 2>/dev/null || missing+=" $name/$f.heap"
    gzip -t "$a/window/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/$f.cpu"
  done
  gzip -t "$a/window/gw-0.heap-live.pb.gz" 2>/dev/null || missing+=" $name/gw-0.heap-live"
  gzip -t "$a/window/loadgen.cpu.pb.gz" 2>/dev/null || missing+=" $name/loadgen.cpu"
  [ "$(lg_true "$a/loadgen-$name.json" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
done
t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"

t2_not_covered "(a) and (c) with SDK clients (light clients only; SDK sizing in Plan 6b2)" \
  "a gateway CPU set other than 6 Zen 5c cores (12 CPUs)"
t2_finish
```

And in `Makefile`:

```diff
diff --git a/Makefile b/Makefile
index 5966022..344c33a 100644
--- a/Makefile
+++ b/Makefile
@@ -71,7 +71,7 @@ test-redis:
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, smoke, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, capacity, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
```

Run: `chmod +x scripts/tier2/capacity.sh && bash -n scripts/tier2/capacity.sh && echo ok`
Expected: `ok`.

- [ ] **Step 2: Dry run on the dev machine**

The guard refuses the dev machine, so check the script in a throwaway copy with the guard lowered, the root parts removed and the scale cut (as 6a2b dry-ran `smoke.sh`): four phases of 3,000 and 1,000 clients at a 30s heartbeat (the gateway's lowest) with 30s windows.

```bash
D=$(mktemp -d); cp scripts/tier2/capacity.sh "$D/capacity.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget 16$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 go$|t2_need curl taskset python3 go|' "$D/capacity.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/capacity.sh"
R=$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/capacity.sh"
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 T2_CPUS_LOADGEN=0-1 \
  CAPACITY_PHASES="a_default:3000:30:default a_tuned:3000:30:tuned c200_150:1000:30:tuned c200_60:1000:30:tuned" \
  CAPACITY_WINDOW=30 CAPACITY_RATE=500 CAPACITY_CAP=5000 CAPACITY_GOMEMLIMIT=256MiB bash "$D/capacity.sh"; echo "exit $?"
```

Check the `sed -i` lines changed what they should (`grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/capacity.sh"`: three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, `t2_need` without sudo). Expected (prototype, ~3.5 min): `result.json` parses (`python3 -m json.tool tier2-results/capacity-*/result.json >/dev/null`); 34 checks, all passing except possibly `no_swap_in_*` from the dev machine's own swapping (the prototype saw 1–13 pages in with 1GB available); exit 1 if one fails, else 0. In `summary`: `a_default` has `"gc": "default"`, the others `"tuned"`; `a_tuned.gateway.next_gc_mb` ≈ 240 (the 256MiB limit) against `a_default`'s ≈ 9; every phase `pings_in_window` ≈ clients (one Ping per client in a 30s window, e.g. 3030); `stop_s` 0–1; every `P_idle` has `done_logouts` = clients. Every phase directory holds `artifacts/window/{gw-0,logic}.{metrics,heap.pb.gz,cpu.pb.gz}`, `gw-0.heap-live.pb.gz` and `loadgen.cpu.pb.gz`. No process is left (`pgrep -f "$PWD/tier2-results/capacity" || echo none`). Remove the dry run's `tier2-results/capacity-*` and `$D`.

- [ ] **Step 3: Commit**

```bash
git add scripts/tier2/capacity.sh Makefile
git commit -m "feat(tier2): capacity scenario, Tier 2 (a) and (c) (#88-#92)"
```

- [ ] **Step 4: Verify the whole branch**

Run: `make test && make test-scripts && make test-load-probe && bash -n scripts/tier2/capacity.sh`
Expected: all pass. Record the commits and the dry run in this plan's *Execution Notes*, the choices #88–#92 as decided in the roadmap (with the 6b1 status), and push the branch.

- [ ] **Step 5: Tier 2 run 14 (on the host)**

The user runs, on the Tier 2 host after `git fetch && git switch feat/plan6b1-capacity`, with the desktop apps closed: `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=capacity` (~20 min), then `SCENARIO=foundation` (`stack.sh` changed), and sends back the two `.report.txt` files. Expected: `capacity` 34/34; `foundation` 6/6. What to read from it, for the *Execution Notes*, the roadmap and the README's *Measured results* (both languages):
- **(a):** 600,000 online on gw-0 in each 600k phase, ramp ~120–140s at 5,000/s with `failed` only `pre-handshake` (retried); the gateway's RSS, `live_heap_mb` and `live_bytes_per_conn` (memory per connection: in the tuned phase RSS is close to the limit by design), heap in use, `next_gc_mb`, GC cycles, pause total and max, GC mark share, and cores in the window, `a_default` against `a_tuned`; the ramp's gateway cores (handshakes + logins at 5,000/s); loadgen's RSS at 600k (run 13 predicts ~5.3GB) and its cores (P6a2aI-4: read its CPU profile before trusting the sizing).
- **(c):** gateway cores and µs per Pong at 200k/150s, 200k/60s and 600k/150s (`a_tuned`): the cost of lowering the heartbeat to 60s at 200k (R4M-6).
- **For 6b2:** Logic and Redis cores in the ramps (5,000 logins/s), loadgen's bytes per light connection at 600k, `stop_s` at 600k (P6a2bI-3), and whether `a_tuned`'s tuning should be the gateway default in 6b2's stacks.
- If a check fails: `P_loadgen_headroom` → lower `CAPACITY_RATE` and rerun that phase (`CAPACITY_PHASES=...`); `P_online` with `gave-up` → handshake limit or ports (look at `failed` and the gateway's `admission_rejects_total` in `artifacts/window/gw-0.metrics`); `P_idle` → read `idle.err`, `done_logouts` and `dropped_logouts`.

---

## After this plan

- **Plan 6b2** (its own session): first decision the SDK memory budget (#87: ~24KB per SDK client; b1's 2 × 200k ≈ 9.6GB; b-stress's 600k victim). It uses `stack_wait_idle` between phases, `stack_usage_*` for sizing, `STACK_GATEWAY_ENV` with the GC setting run 14 picks, and run 14's Logic/Redis cores per 1,000 logins/s for the storm's headroom.
- **Plan 6b3:** multi-instance `loadgen.sh` (P6a2bI-1); `lg_profile` already takes the instance's control port from `lg_control`, which 6b3 makes per instance.
- **Pooling** (spec: *Admission control*, "left to profiling in load test (a)"): run 14's live heap profiles at 600k (`artifacts/window/gw-0.heap-live.pb.gz` per phase) say whether conn structs, Noise state or write buffers are worth pooling; a pooling change is its own plan, measured by rerunning `capacity`.

## Execution Notes

(Filled in during execution.)

- **Prototype (2026-10-10, while writing the plan):** every block was written and run in a scratch worktree of `main` `373fc73` (branch `proto-6b1`, commit `cb5e6e6`), then copied into this plan by a render script (new files whole, modified files as `diff -u` against `373fc73`). Results on the dev machine: `go test -race -count=1 ./cmd/gateway/` green (`TestLogoutQueueFollowsTheCap` failed first with `logout queue 250000, want 650000`); `make test-scripts` green; Task 2 Step 5's mutant caught (two FAILs). Dry run as in Task 3 Step 2: 34 checks, all passing except `no_swap_in_a_tuned`, `no_swap_in_c200_150`, `no_swap_in_c200_60` (1, 1 and 13 pages swapped in: the dev machine's 1GB available), 204s; `a_tuned.next_gc_mb` 240 vs `a_default` 9; `pings_in_window` 3,030 and 1,009; `stop_s` 0; login p99 20–63ms; a two-phase rerun after the last edits passed 18/18 with exit 0. `gc_mark_cpu_pct`'s parsing of `go tool pprof -top` was checked on an allocation benchmark's profile (13.75% `gcBgMarkWorker` + 1.43% `gcAssistAlloc` → 15.2).
- **Found while prototyping:** (1) `-logout-queue` kept 250k with a raised cap (#89). (2) `stack_test.sh`'s `stack_ports | grep -qx` failed once under `pipefail` (SIGPIPE when `grep -q` exits early); a here-string fixes it (Task 2). (3) `goim_gateway_ready` is refreshed once a second, so it reads 0 just after `/readyz` answers; `stack_test` reads `goim_gateway_tracked` instead. (4) The gateway does not log its heartbeat, so the phase field records `gateway_args` for audit.
- **Plan review (2026-10-10, Claude at the user's request; choices #88–#92 decided):** #88, #89, #92 as proposed; two amendments, made in the prototype (`proto-6b1` commit `1aa39c7`), dry-run, and copied back into Task 3 byte for byte. (1) **#90:** `P_idle` passed when `logouts_total{result="dropped"}` read `null`, which is both "no drop yet" (the counter has no sample until its first increment) and "the scrape failed", against Review Focus 2; it now needs `logouts_total{result="done"}` = clients (positive evidence: every logged-in client's Logout finished), and records the dropped count. (2) **#91:** with `GOGC=off` and a 4GiB limit above the ~2GB live heap, the tuned gateway may not collect after the ramp, so its RSS is the limit and the window's heap profile (no `gc=1`) is stale; that was the plan's only memory-per-connection number and the input for pooling. After the window (outside the measurement), a `?gc=1` heap profile is saved as `gw-0.heap-live.pb.gz` and `live_heap_mb` / `live_bytes_per_conn` are recorded and summarised. Re-verified with Task 3 Step 2's dry run: 34 checks, all passing except `no_swap_in_a_default` and `no_swap_in_a_tuned` (the dev machine's own swap, as before); `done_logouts` 3,000/3,000 and 1,000/1,000; `live_bytes_per_conn` 2.7–7.9KB at 1,000–3,000 connections (fixed overhead dominates at that scale); no process left. Considered and not changed: the 650k Logout channel costs ~5MB of pointers; the production default (250k cap, 250k queue) is unchanged; `GOGC=off` for the 200k phases is the fixed-memory pod policy, and 6b2's GC default is picked from run 14 (`CAPACITY_GOGC` can rerun with e.g. `GOGC=200`); `gc_mark_cpu_pct` reads 0 when the GC's share is under pprof's 0.5% node cut, which is fine for a field.
- **Execution (2026-10-10, inline, superpowers:executing-plans):** plan code applied verbatim, no rulings. Task 1 `7beebbf` (`TestLogoutQueueFollowsTheCap` failed first with `logout queue 250000, want 650000`, then green); Task 2 `55bd654` (`stack_test` 13 FAILs first, then `make test-scripts` all passed; Step 5's mutant caught: two FAILs); Task 3 `06fb8fa` (dry run as in Step 2: 34 checks, exit 0; `a_tuned.next_gc_mb` 240 vs `a_default` 10; `pings_in_window` 3,030/3,040/1,009/1,012; `stop_s` 0; every artifact present; no process left). Step 4: `make test`, `make test-scripts`, `make test-load-probe`, `bash -n` all green.
- **Final review (fresh Opus):** no Critical. **I1 fixed** in `e446e6a`: the gateway's first periodic Reconcile pass starts at a random point of its 10m cycle, so it overlapped some ~7-minute phases and not others (at 600k ~1,100 entries/s of Reconcile RPCs over 90% of the cycle), and `gateway_us_per_pong`, cores and GC numbers, and so `a_default` vs `a_tuned` and `c200_150` vs `c200_60`, depended on that coin flip. Phases now run with `-reconcile-cycle 24h -reconcile-jitter 0` (`CAPACITY_RECONCILE_CYCLE`), and each phase records `reconcile_entries` in its window (0 when the counter has no sample at either end). Verified RED→GREEN on one-phase dry runs (before: no field; after: 0 entries at 24h, 1,397 at a 20s cycle), then the 4-phase dry run (34 checks, only `no_swap_in_a_tuned` failing from the dev machine's swap; `reconcile_entries` 0 in every phase) and `make test-scripts`. **Minors deferred:** M1 `stack_metric` uses `printf "%d"`, which caps at 2^31 under an old mawk 1.3.3 (gawk and mawk 1.3.4 are fine); M2 `online` = `null` (a failed `lg_get`) stops the whole run in `t2_check` instead of failing the phase; M3 `live_heap_mb` is heap in use after the GC (an upper bound, as #91 says), not `heap_alloc`; M4 `gc_pause_max_s` is the maximum of the recent pauses since start, not the window's; M5 `gc_mark_cpu_pct` reads 0 below pprof's 0.5% node cut (`-nodefraction=0` would fix it); M6 loadgen's exit status (`LG_STATUS`, e.g. SIGKILL after 60s) is not recorded; M7 `lg_get` has no curl timeout (pre-existing); M8 the window's gateway CPU includes 30s of pprof SIGPROF overhead (~1–3%). **Read run 14 with these in mind:** `gc_mark_cpu_pct` includes the GC's idle-mode mark workers, which use otherwise idle CPU, so on a mostly idle gateway it overstates what the GC takes from the work. **For 6b2/6c:** at 600k a finished Reconcile pass would send a ReconcileEnd of ~4.8MB, over gRPC's 4MB default message size, and no `MaxCallSendMsgSize`/`MaxRecvMsgSize` is set (spec: "the list is split or the limit raised"); the kill-600k and long-running tests must split the list or raise the limit.
- **Minors M1–M8 fixed (2026-10-10, at the user's request)**, each RED→GREEN. `bab301d`: M1 `stack_metric` prints whole sums with `%.0f`, and an absent MATCH no longer rejects every line under busybox awk (`index($0, "")` is 0 there; found by the new test). New `stack_test` checks run under `busybox awk` against a fake `/metrics` (`metric: bytes past 2^31`, `metric: a sum past 2^32`; both read `null` before the fix). M7 `lg_get` gives up after `LG_CURL_TIMEOUT` (10s) (`loadgen_test`: `get: a silent control API is null`). The next commit has the `capacity.sh` minors. M2: an unreadable `online` is `false`, so the phase fails and the run goes on (fault-injected dry run: the run stopped at `t2_check`, now `c200_60_online` fails and the phase continues). M3: `live_heap_mb` and `live_bytes_per_conn` come from `go_memstats_heap_alloc_bytes`, with `live_heap_inuse_mb` beside them (dry run: ~1.9KB per connection in both `a_default` and `a_tuned`, where heap in use gave 2.7 vs 5.2KB). M4: `gc_pause_max_recent_s`. M5: `gc_mark_cpu_pct` passes `-nodefraction=0 -edgefraction=0` (on a test profile: 4.4 → 4.9, the 0.47% `gcAssistAlloc` had been cut). M6: `loadgen_exit`. M8: the 30s CPU profiles now run as the settle, replacing the 10s sleep (+20s per phase), before the window, and `profile_in_window_s` records any overlap (was 30, now 0). So `gc_mark_cpu_pct` and the CPU profiles describe the 30s just before the window. Suite: `make test-scripts` all passed; 4-phase dry run 34 checks, only `no_swap_*` failing (the dev machine's swap).
- **Tier 2 run 14 (2026-10-11, `cbf7bc9`, 1,902s): `capacity` failed, `foundation` passed 6/6.**
  - **600k phases: ~505k online** (504,690 default GC, 505,131 tuned), after 420s. Failures: `dial` 1.2M, `gave-up` ~88k, `handshake` 92–120k; 6.8–7.2k connections lost in the window; Pong RTT p99 ~720ms, max 29s. Gateway 0.46 cores of 12 in the ramp, Logic 0.09, Redis 0.02, loadgen 3.3 of 8: nothing CPU-bound. The gateway counted 126,893 `handshake_timeout` rejects (accepted, no handshake within 5s) and only 6,389 `no_slot`.
  - **The cause was the host, not the gateway:** loopback dropped 48,738,157 of 138,501,333 received packets since boot (35%; `ip -s link show lo`), with `net.core.netdev_max_backlog` at the default 1000. Every loopback packet is queued on the sending CPU's input backlog. A dropped SYN is resent after 1s, 3s, 7s (`TCPSynRetrans` 87,761): dials time out at loadgen's 10s, and login latency is p90 5–6s, p99 ~9.8s even at 200k. A dropped handshake segment gives the 5s handshake timeouts; dropped data gives `TCPAbortOnTimeout` 42,349 and the lost connections. Ruled out by counters: accept/SYN queue (`ListenOverflows`, `ListenDrops`, `ReqQFullDrop`, syncookies all 0), TCP memory (`AbortOnMemory`, `MemoryPressures` 0). `TCPTimeWaitOverflow` 1.7M (`tcp_max_tw_buckets` 131,072) is harmless: the kernel skips TIME_WAIT instead.
  - **`P_idle` failed in every phase: a check bug (#90 as amended).** A client that retried after a timeout has two sessions, so done Logouts exceed clients (201,183 and 200,001 for 200,000). They equal successful Logins exactly (512,753 = 512,753; 201,183 = 201,183).
  - **Valid numbers (light clients; (a) at ~505k, not 600k):** gateway cores in the window 0.154 (default GC) / 0.146 (tuned) at 505k and 150s, 0.080 at 200k/150s, 0.139 at 200k/60s. The 60s heartbeat is +0.059 cores, +74%, for 2.5x the Pongs (3,342/s vs 1,336/s). µs of gateway CPU per Pong: 45.6 / 43.3 / 60.2 / 41.6 (a fixed idle cost is spread over more Pongs). Live heap after a forced GC: 1,015–1,040 B per connection at 505k, ~1,390 B at 200k. RSS: 854MB default vs 3,908MB tuned (≈ the 4GiB limit, by design). Default GC: 1 cycle in the 150s window, 0.3ms max pause; tuning bought nothing measurable, so 6b2's stacks keep Go's default GC. Loadgen RSS ~3.7GB at 505k. Logic and Redis in a 5,000/s ramp: 0.25–0.28 and ~0.04 cores at 200k.
  - **Fixes (2026-10-11):** `e927e66` loadgen counts failed dials as `dial-timeout`, `dial-refused` or `dial-no-port` (`TestDialFailuresAreCountedByReason`). `deb5db7`: `sysctl.conf` sets `net.core.netdev_max_backlog = 65536`, so `t2_require_host_setup` refuses until `sudo scripts/tier2/host-setup.sh` is rerun. Each phase records loopback `rx_dropped` over the ramp and over settle + window, and fails `no_lo_drops_in_P` on any drop (38 checks now). `P_idle` reads one scrape (`stack_metrics` + `stack_metric_in`, tested in `stack_test`) and wants done Logouts ≥ successful Logins with none dropped (`logouts_settled`, tested with run 14's values). Dry run: 38 checks, only `no_swap_*` failing (dev machine); `make test`, `make test-scripts`, `make test-load-probe` green. The host's `awk` is mawk (no `strtonum`), so M1's portability fix mattered.
  - **Not yet in the README's measured results:** wait for run 15.
- **Next:** on the host, `sudo scripts/tier2/host-setup.sh` once (new sysctl), then Tier 2 run 15: `capacity` (expected 38/38) and `foundation` (6/6). Run 13's smoke (100k at 5,000/s, ~2% pre-handshake retries) probably also ran with loopback drops.
