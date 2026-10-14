# Plan 6a2a: Loadgen Core (pprof, stack helpers, balancer, recorder) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give load runs their profiles and the parts of `internal/loadgen` that need no running client: pprof on the gateway and Logic admin ports, stack helpers to save profiles and to account for a gateway killed on purpose, loadgen's `stack.env` reader, its least-connections balancer over source addresses 127.0.0.2…, and the per-client recorder that computes the storm SLO distribution.

**Architecture:** A tiny `internal/pprofhttp` registers `net/http/pprof` on both admin muxes; `scripts/stack.sh` saves heap and goroutine profiles in every snapshot, CPU profiles on demand, and records a deliberate SIGKILL as `expected:STATUS`. `internal/loadgen` starts here with three pure units: `Env` (reads `stack.env`), `Balancer` (the NLB stand-in: least connections, round-robin sources, `IP_BIND_ADDRESS_NO_PORT`), and `Recorder` (per-client login/loss times under one mutex, fixed-memory histograms, SLO from a mark and from detection). Plan 6a2b adds the clients, the control API, `cmd/loadgen`, the Tier 1a probe and the Tier 2 smoke scenario on top.

**Tech Stack:** Go 1.27.1 (`net/http/pprof`, `golang.org/x/sys/unix` promoted from indirect), bash 5.2, the pinned `redis-server` 7.4.11 (`make redis-server`), curl, gzip, python3 (tests only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Success criteria → Storm SLO*, *Verification → Test execution model* (Tier 1a probe, *CPU isolation*, *Automation contract*: pprof, metrics snapshot, loadgen latency distributions), *Verification → Load tests* ("loadgen records detection time and login-success time per client and computes the SLO distribution directly"). Roadmap choices #65, #69, #71, #72 and Plan 6a1 (`docs/superpowers/plans/2026-10-09-connection-session-core-06a1-tier2-foundation.md`, *After this plan*).

**Branch:** `feat/plan6a2-loadgen`, a new branch from `feat/plan6a-foundation` (head `80bbffa` or later); the branches keep stacking.

**Split (proposed with this plan, choice #86):** #72's Plan 6a2 is written as two plans, each executed in its own session, in order, because together they are ~120KB (the limit is ~80KB per session). **6a2a** (this plan): Tasks 1–3. **6a2b** (`2026-10-09-connection-session-core-06a2b-loadgen-runs.md`): Tasks 4–6, the light and SDK clients, the control API, `cmd/loadgen`, `scripts/loadgen.sh`, the Tier 1a probe and the Tier 2 `smoke` scenario. The choices and the *Execution Notes* of both live in this file.

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module. `golang.org/x/sys v0.48.0` moves from indirect to direct (`go mod tidy`).
- `make test` (`go test -race -count=1 ./...`) stays green on the dev machine (2 cores / 2GB) and uses loopback only.
- Ports (choice #76): `STACK_PORT_BASE` 17000 on Tier 2, 17500 in `stack_test.sh`; Plan 6a2b's probe uses 17200; loadgen's control API is `STACK_PORT_BASE + 5`.
- Loadgen source addresses are 127.0.0.2, 127.0.0.3, … (#69), at most 253; the ephemeral range 32768–60999 (28,232 ports per source and destination pair) is kept.
- CPU sets (#71): gateway `4-9,16-21`, Logic + Redis `10,11,22,23`, loadgen `0-3,12-15`.
- Storm SLO (spec): 99% logged in again within 30s of `T_death` (b1) or of the client detecting the loss (b2); the default storm window is 0–20s.
- Login slot of every loadgen client: appid `im`, platform mobile; uid `<prefix>-<i>` (`^[A-Za-z0-9_-]{1,64}$`).
- Scripts: bash with `set -uo pipefail`; `scripts/tier2/lib.sh` keeps writing JSON without jq; python3 only reads JSON.
- Comments and names match the surrounding code; new behaviour cites its choice number (#80–#86).

## Review Focus

1. **Source ports per destination, not per source.** `net.Dialer{LocalAddr}` binds before connecting, and Linux then gives each source address one 28,232-port range shared by every destination: 600k on one gateway would need 22 sources but 4 gateways × 200k would run out at the same count. `IP_BIND_ADDRESS_NO_PORT` defers the port to `connect`, per (source, destination) pair (#69, #81). Pinned by `TestBalancerBindsWithoutPort` (Task 2); 6b's (a) is the first run at the real scale.
2. **A client that never logs in again must fail the SLO**, however fast the others were. Percentiles are taken over the whole population, missing members count as infinitely slow, and a percentile that falls on one is -1 (`TestHistMissingMembersFailThePercentile`, `TestRecorderSLOMissingMemberFails`, Task 3).
3. **A gateway killed on purpose is not a process problem, but a panic before the kill still is.** `stack_expect_dead` reaps it and records `expected:STATUS`; `stack_health` still scans its log (stack_test `dead: …`, Task 1).
4. **The population of a mark is the clients logged in on that gateway at that instant**, not every client and not those that log in later; a client counts as back only after a loss (`TestRecorderSLOFromMarkAndDetection`, `TestRecorderBackNeedsALoss`, Task 3).
5. **A failed dial must give its balancer slot back**, or least-connections drifts away from a gateway that refused a few dials (e.g. while it restarts) and never sends it new clients (`TestBalancerFailedDialFreesTheSlot`, Task 2).

## Implementation choices this plan makes

Proposed 2026-10-09 with this plan; **decided 2026-10-09 by Claude at the user's request** (recorded in the roadmap as #80–#86): #80, #81, #82, #85 as proposed; #83, #84, #86 amended (the amendments are in the text below and in *Execution Notes → Plan review*). The ones implemented in 6a2b are listed here too, so they are reviewed together.

80. **Two loadgen modes, per P5I-2.** `light`: `internal/clientconn`, one reader goroutine per connection, Pings from one shared pinger that spreads clients over `PingEvery / 100ms` slots and counts a Ping due while the previous one is unanswered as missing; before it logs in a light client retries any failure after 0.5–1.5s, at most 10 times (a gateway's handshake limit closes the excess of a fast ramp: seen under `-race` in the prototype), and once logged in it never reconnects (a loss is counted). `sdk`: one `internal/sdk` client per connection with a `MemStore`, so its reconnect rules are what is measured. (Plan 6a2b, Task 4.)
81. **The balancer stands in for the NLB.** Every dial goes to the live gateway with the fewest open connections (the least-connections algorithm the spec asks of the cloud LB, R5S-O4), from the next source address in turn, with `IP_BIND_ADDRESS_NO_PORT`. A gateway loadgen killed (`/kill`) or was told is down (`/mark … down=1`) gets no new dials, as an LB removes a failed target; without that, an SDK client that redials the dead address takes a failure backoff (2.5–5s, then 5–10s) that no production client behind an NLB would see, and b1's p99 would measure the test, not the system. The SDK gets the single address `balancer`, as an app gets the NLB's. Sources default to ⌈clients per gateway ÷ 20,000⌉.
82. **Loadgen signs its own tokens** with `stack.env`'s `JWT_KEY` (devtoken's private key), as the account service would: `sub` `<prefix>-<i>`, appid `im`, mobile, `iat` now − 1s, 24h lifetime, signed on every attempt (Ed25519, ~20µs). Each run uses its own uid prefix, so two runs in one scenario never replace each other's sessions.
83. **The recorder** keeps, per client, the gateway it is logged in on and its place in its latest mark, all under one mutex (events arrive at most at the login rate). Distributions use a fixed histogram (1ms buckets below 60s, 100ms up to 1h, 763KB each), so memory does not grow with clients or run length; *amended at review:* the exact range covers every storm bound (30s from the mark, and b2's detection plus 30s), where a 100ms bucket's lower bound could pass a p99 up to 99ms over the bound. An SLO is taken over a **mark**: a named instant whose population is the clients logged in on one gateway (or all) at that instant; per member it keeps the first loss after the mark and the first login after that loss. It reports the distributions from the mark (b1: from `T_death`), from detection (b2) and of detection itself; members never back count as infinitely slow; pass = p99 ≤ `within`. A client belongs to its latest mark only.
84. **Control API** on `STACK_PORT_BASE + 5` (6a2b, Task 4): `GET /stats`, `POST /kill?gateway&pid&mark` (mark taken, then the gateway out of the balancer, then `kill(pid, SIGKILL)`, so `T_death` is when loadgen issues the signal, R4M-17; *amended at review:* a mark name already taken returns 409 with no gateway removed and no signal sent), `POST /mark?gateway|all&mark[&down=1]`, `GET /slo?mark&within`, and pprof. At the end `cmd/loadgen` writes the result (stats and every mark's SLO) **before** stopping the clients, so a teardown that outlasts the caller's wait cannot lose it; *amended at review:* the runner has its own context, cancelled only after the result is written, so the result is the run as it stood and no client is already being stopped while it is taken. In `stack.sh`, `stack_expect_dead NAME` reaps a gateway loadgen killed and records `NAME expected:STATUS`, which `stack_health` accepts (Task 1).
85. **pprof is always on the gateway and Logic admin ports** (`/debug/pprof/`), with no flag: the admin port is pod-internal and never behind the NLB, and production profiling needs it as much as the load runs do. Plan 6d's manifests must keep the admin port off any Service the NLB or the internet reaches; *amended at the interface review (P6a2aI-3):* and add a NetworkPolicy that admits it only from the monitoring namespace, since any pod reaches a pod IP and `/debug/pprof/profile` and `trace` cost CPU. `stack_snapshot` adds each process's heap profile and goroutine dump; `stack_profile NAME SECONDS` takes CPU profiles of Logic and every gateway over the same window.
86. **Plan 6a2 split** into 6a2a (Tasks 1–3) and 6a2b (Tasks 4–6, see *Split*). In 6a2b: the Tier 1a probe is `make test-load-probe` (not in `make test`; ~1 min on the dev machine): 2 unpinned gateways, 3,000 light connections pinged every 5s, then 2,000 SDK clients with gw-1 killed through `/kill` and the b1 SLO checked. The Tier 2 `smoke` scenario: 4 pinned gateways, 100,000 light connections pinged every 10s through a 60s recorded window with 30s CPU profiles, then 20,000 SDK clients with gw-3 killed, 10 checks; `make test-tier2 SCENARIO=smoke`. *Amended at review:* smoke also records the resident memory of loadgen and each gateway in both phases (fields `light.rss_mb`, `sdk.rss_mb`; not checks), so 6b sizes 600k light and 200k SDK clients per loadgen process from measured bytes per connection (P5I-2).

## File Structure

| File | Task | Responsibility |
|---|---|---|
| `internal/pprofhttp/pprofhttp.go` (+ test) | 1 | `Register(mux)`: `net/http/pprof` on an admin mux |
| `internal/gateway/server.go`, `internal/logic/server.go` (+ tests) | 1 | admin mux registers pprof |
| `scripts/stack.sh`, `scripts/stack_test.sh` | 1 | profiles in `stack_snapshot`, `stack_profile`, `stack_forget`, `stack_expect_dead`, `stack_health` accepts `expected:` |
| `internal/loadgen/env.go` (+ test) | 2 | package doc; `Env`, `LoadEnv`, `Env.Gateway` |
| `internal/loadgen/balancer.go` (+ test) | 2 | `Balancer` (least connections, sources, down gateways), `Sources`, `ErrNoGateway` |
| `go.mod` | 2 | `golang.org/x/sys` direct |
| `internal/loadgen/hist.go` (+ test) | 3 | `Dist`, fixed-memory histogram |
| `internal/loadgen/recorder.go` (+ test) | 3 | `Recorder`, marks, `SLO`, `Counts` |

---

### Task 1: pprof on the admin ports, stack profiles, a gateway killed on purpose

**Files:**
- Create: `internal/pprofhttp/pprofhttp.go`, `internal/pprofhttp/pprofhttp_test.go`
- Modify: `internal/gateway/server.go`, `internal/logic/server.go`, `internal/gateway/server_test.go`, `internal/logic/server_test.go`, `scripts/stack.sh`, `scripts/stack_test.sh`

**Interfaces:**
- Consumes: the admin `http.ServeMux` in `gateway.(*Gateway).Start` and `logic.NewServer`; `stack.sh`'s `STACK_PIDS`, `STACK_ORDER`, `exit-status` (Plan 6a1).
- Produces: `pprofhttp.Register(mux *http.ServeMux)` (6a2b's control API uses it too); `/debug/pprof/` on every admin port; shell functions `stack_admins`, `stack_profile NAME SECONDS` (→ `artifacts/NAME/<process>.cpu.pb.gz`), `stack_forget NAME`, `stack_expect_dead NAME` (→ `NAME expected:STATUS` in `exit-status`); `stack_snapshot NAME` now also writes `<process>.heap.pb.gz` and `<process>.goroutine.txt`, and skips a gateway that `stack_expect_dead` forgot.

- [ ] **Step 1: Write the failing tests**

`internal/pprofhttp/pprofhttp_test.go`:

```go
package pprofhttp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRegisterServesProfiles(t *testing.T) {
	mux := http.NewServeMux()
	Register(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if code, body := get(t, srv.URL+"/debug/pprof/goroutine?debug=1"); code != http.StatusOK || !strings.Contains(body, "goroutine profile:") {
		t.Fatalf("goroutine: %d %.80q", code, body)
	}
	if code, body := get(t, srv.URL+"/debug/pprof/heap"); code != http.StatusOK || len(body) == 0 {
		t.Fatalf("heap: %d, %d bytes", code, len(body))
	}
	if code, body := get(t, srv.URL+"/debug/pprof/profile?seconds=1"); code != http.StatusOK || len(body) == 0 {
		t.Fatalf("cpu profile: %d, %d bytes", code, len(body))
	}
}
```

The gateway and Logic admin ports serve pprof (add `"io"` to the gateway test's imports):

```diff
diff --git a/internal/gateway/server_test.go b/internal/gateway/server_test.go
index 583801f..e598429 100644
--- a/internal/gateway/server_test.go
+++ b/internal/gateway/server_test.go
@@ -2,6 +2,7 @@ package gateway
 
 import (
 	"errors"
+	"io"
 	"io/fs"
 	"net/http"
 	"strings"
@@ -96,3 +97,18 @@ func TestFailedLimitsKeepTheGatewayNotReady(t *testing.T) {
 		t.Fatal("/readyz is OK with a failed limits check")
 	}
 }
+
+// The admin port serves pprof, so load runs can save profiles (choice #85).
+func TestAdminServesPprof(t *testing.T) {
+	l := startLive(t, nil, nil)
+	_, admin := l.g.Addrs()
+	resp, err := http.Get("http://" + admin + "/debug/pprof/goroutine?debug=1")
+	if err != nil {
+		t.Fatal(err)
+	}
+	body, _ := io.ReadAll(resp.Body)
+	resp.Body.Close()
+	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "goroutine profile:") {
+		t.Fatalf("pprof: %d %.80q", resp.StatusCode, body)
+	}
+}
diff --git a/internal/logic/server_test.go b/internal/logic/server_test.go
index 8b94194..8018efb 100644
--- a/internal/logic/server_test.go
+++ b/internal/logic/server_test.go
@@ -76,6 +76,9 @@ func TestServerEndToEnd(t *testing.T) {
 	if c := httpStatus(t, "http://"+adminAddr+"/healthz"); c != http.StatusOK {
 		t.Fatalf("healthz: %d", c)
 	}
+	if c := httpStatus(t, "http://"+adminAddr+"/debug/pprof/goroutine?debug=1"); c != http.StatusOK {
+		t.Fatalf("pprof: %d", c) // choice #85
+	}
 	if err := keys.Load(ctx); err != nil {
 		t.Fatal(err)
 	}
```

`scripts/stack_test.sh`: profiles in the snapshot and from `stack_profile`, and a gateway killed on purpose:

```diff
diff --git a/scripts/stack_test.sh b/scripts/stack_test.sh
index de63769..2cb6dc3 100755
--- a/scripts/stack_test.sh
+++ b/scripts/stack_test.sh
@@ -84,6 +84,15 @@ for f in logic gw-0 gw-1; do
   grep -q '^# TYPE' "$STACK_DIR/artifacts/idle/$f.metrics" && ok "snapshot: $f metrics" || bad "snapshot: $f metrics"
 done
 grep -q '^redis_version:' "$STACK_DIR/artifacts/idle/redis.info" && ok "snapshot: redis INFO" || bad "snapshot: redis INFO"
+# Profiles from pprof on the admin ports (choice #85).
+for f in logic gw-0 gw-1; do
+  grep -q 'goroutine profile:' "$STACK_DIR/artifacts/idle/$f.goroutine.txt" 2>/dev/null && ok "snapshot: $f goroutines" || bad "snapshot: $f goroutines"
+  gzip -t "$STACK_DIR/artifacts/idle/$f.heap.pb.gz" 2>/dev/null && ok "snapshot: $f heap profile" || bad "snapshot: $f heap profile"
+done
+stack_profile cpu 1
+for f in logic gw-0 gw-1; do
+  gzip -t "$STACK_DIR/artifacts/cpu/$f.cpu.pb.gz" 2>/dev/null && ok "profile: $f CPU profile" || bad "profile: $f CPU profile"
+done
 
 # A second stack on the same ports is refused before it starts anything.
 (STACK_DIR=$(mktemp -d); STACK_ORDER=(); stack_up 2>"$STACK_DIR/err"; echo $? >"$STACK_DIR/rc"; cat "$STACK_DIR/err" "$STACK_DIR/rc") >"$STACK_DIR/second" 2>&1
@@ -142,6 +151,25 @@ stack_up 2>/dev/null && bad "up: should refuse the held port" || ok "up: refused
 expect "health: a refused stack_up reports no earlier stack" "$(stack_health 2>&1)" ""
 kill $holder; wait $holder 2>/dev/null
 
+# A gateway killed on purpose (loadgen's /kill, #84) is reaped and is not a
+# problem; a running one is refused.
+STACK_GATEWAYS=2
+if stack_up; then
+  stack_expect_dead gw-1 2>/dev/null && bad "dead: a running gateway is refused" || ok "dead: a running gateway is refused"
+  kill -KILL "${STACK_PIDS[gw-1]}"
+  stack_expect_dead gw-1 && ok "dead: a killed gateway is reaped" || bad "dead: reap"
+  expect "dead: not a problem while the stack runs" "$(stack_health 2>&1)" ""
+  stack_snapshot after-kill
+  [ -e "$STACK_DIR/artifacts/after-kill/gw-1.metrics" ] && bad "dead: snapshot skips it" || ok "dead: snapshot skips it"
+  stack_down
+  expect "dead: recorded as expected" "$(grep '^gw-1 ' "$STACK_DIR/exit-status")" "gw-1 expected:137"
+  expect "dead: not a problem after the stop" "$(stack_health 2>&1)" ""
+  echo "panic: runtime error: before the kill" >>"$STACK_DIR/logs/gw-1.log"
+  grep -q "^gw-1: panic: runtime error: before the kill" <<<"$(stack_health)" && ok "dead: a panic in its log is still reported" || bad "dead: panic not reported: [$(stack_health 2>&1)]"
+else
+  bad "up: expect-dead stack"
+fi
+
 # Pinning without lib.sh is refused, not run with GOMAXPROCS 1 (M6).
 out=$(unset -f t2_cpulist_expand; STACK_PIN=1; stack_up 2>&1; echo "rc=$?"; stack_down)
 contains() { case "$2" in *"$3"*) ok "$1" ;; *) bad "$1: [$2] lacks [$3]" ;; esac; }
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -race -count=1 ./internal/pprofhttp/ ./internal/gateway/ -run 'Register|Pprof' && go test -race -count=1 ./internal/logic/ -run TestServerEndToEnd`
Expected: FAIL: `undefined: Register` in pprofhttp; once that exists, the gateway test fails with `pprof: 404` and Logic with `pprof: 404`.

Run: `make test-scripts`
Expected: FAIL lines `snapshot: <process> goroutines` and `… heap profile`, `profile: <process> CPU profile` (`stack_profile: command not found`), and `dead: a killed gateway is reaped` (`stack_expect_dead: command not found`).

- [ ] **Step 3: Implement**

`internal/pprofhttp/pprofhttp.go`:

```go
// Package pprofhttp serves net/http/pprof on an admin mux (Plan 6a2, choice
// #85), so load runs can save CPU, heap and goroutine profiles as artifacts
// (spec: Automation contract). The admin port is pod-internal: never behind
// the NLB.
package pprofhttp

import (
	"net/http"
	"net/http/pprof"
)

// Register adds /debug/pprof/ and its handlers to mux.
func Register(mux *http.ServeMux) {
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}
```

Register it on both admin muxes:

```diff
diff --git a/internal/gateway/server.go b/internal/gateway/server.go
index c26dfa7..a95b257 100644
--- a/internal/gateway/server.go
+++ b/internal/gateway/server.go
@@ -13,6 +13,7 @@ import (
 
 	pb "github.com/Tallinzp/goim/api/internalpb"
 	"github.com/Tallinzp/goim/internal/fault"
+	"github.com/Tallinzp/goim/internal/pprofhttp"
 	"github.com/prometheus/client_golang/prometheus"
 	"github.com/prometheus/client_golang/prometheus/promhttp"
 	"google.golang.org/grpc"
@@ -104,6 +105,7 @@ func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
 	mux := http.NewServeMux()
 	mux.Handle("/metrics", promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{}))
 	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
+	pprofhttp.Register(mux)
 	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
 		if err := g.Ready(); err != nil {
 			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
diff --git a/internal/logic/server.go b/internal/logic/server.go
index f207574..e6cb3e4 100644
--- a/internal/logic/server.go
+++ b/internal/logic/server.go
@@ -15,6 +15,7 @@ import (
 
 	pb "github.com/Tallinzp/goim/api/internalpb"
 	"github.com/Tallinzp/goim/internal/fault"
+	"github.com/Tallinzp/goim/internal/pprofhttp"
 	"github.com/prometheus/client_golang/prometheus"
 	"github.com/prometheus/client_golang/prometheus/promhttp"
 	"google.golang.org/grpc"
@@ -134,6 +135,7 @@ func NewServer(cfg Config, sc ServerConfig, d Deps) (*Server, error) {
 	mux := http.NewServeMux()
 	mux.Handle("/metrics", promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}))
 	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
+	pprofhttp.Register(mux)
 	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
 		if !s.Ready() {
 			http.Error(w, "not ready: no key set or not serving", http.StatusServiceUnavailable)
```

`scripts/stack.sh`:

```diff
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -7,7 +7,10 @@
 #
 #   stack_build                 # binaries into $STACK_DIR/bin
 #   stack_up                    # keys, processes, waits until all are Ready
-#   stack_snapshot NAME         # /metrics of each process + redis INFO -> $STACK_DIR/artifacts/NAME/
+#   stack_snapshot NAME         # /metrics, heap and goroutine profiles of each process + redis INFO
+#                               #   -> $STACK_DIR/artifacts/NAME/
+#   stack_profile NAME SECONDS  # CPU profiles of Logic and every gateway at once -> artifacts/NAME/
+#   stack_expect_dead NAME      # NAME was killed on purpose (loadgen /kill): reap and record it
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
 #   stack_health                # one line per problem: dead process, unclean stop, panic in a log
 #
@@ -200,19 +203,44 @@
 ENV
 }
 
-# stack_snapshot NAME: every process's /metrics and Redis INFO, for result
-# artifacts (spec: Automation contract, a metrics snapshot).
+# stack_admins: "NAME ADMIN" for Logic and each running gateway.
+stack_admins() {
+  local base=${STACK_PORT_BASE:-17000} i
+  [ -n "${STACK_PIDS[logic]:-}" ] && echo "logic 127.0.0.1:$((base + 4))"
+  for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
+    [ -n "${STACK_PIDS[gw-$i]:-}" ] && echo "gw-$i 127.0.0.1:$((base + 102 + 10 * i))"
+  done
+}
+
+# stack_snapshot NAME: every process's /metrics, heap profile and goroutine
+# dump (pprof on the admin port, choice #85), and Redis INFO, for result
+# artifacts (spec: Automation contract: pprof, a metrics snapshot). A
+# gateway killed on purpose (stack_expect_dead) is skipped.
 stack_snapshot() {
-  local dir=$STACK_DIR/artifacts/$1 base=${STACK_PORT_BASE:-17000} i
+  local dir=$STACK_DIR/artifacts/$1 base=${STACK_PORT_BASE:-17000} name admin
   local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
   mkdir -p "$dir"
-  curl -sf "http://127.0.0.1:$((base + 4))/metrics" >"$dir/logic.metrics"
-  for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
-    curl -sf "http://127.0.0.1:$((base + 102 + 10 * i))/metrics" >"$dir/gw-$i.metrics"
-  done
+  while read -r name admin; do
+    curl -sf "http://$admin/metrics" >"$dir/$name.metrics"
+    curl -sf "http://$admin/debug/pprof/heap" >"$dir/$name.heap.pb.gz"
+    curl -sf "http://$admin/debug/pprof/goroutine?debug=1" >"$dir/$name.goroutine.txt"
+  done < <(stack_admins)
   "$(dirname "$redis")/redis-cli" -p "$base" info all >"$dir/redis.info" 2>&1
 }
 
+# stack_profile NAME SECONDS: a CPU profile of Logic and every running
+# gateway over the same SECONDS, into artifacts/NAME/<process>.cpu.pb.gz.
+# Blocks for SECONDS; a scenario takes it inside its measured window.
+stack_profile() {
+  local dir=$STACK_DIR/artifacts/$1 name admin pids=()
+  mkdir -p "$dir"
+  while read -r name admin; do
+    curl -sf -m $(($2 + 30)) "http://$admin/debug/pprof/profile?seconds=$2" >"$dir/$name.cpu.pb.gz" &
+    pids+=($!)
+  done < <(stack_admins)
+  wait "${pids[@]}"
+}
+
 # stack_pinned NAME CPUS: the process's affinity is exactly CPUS.
 stack_pinned() {
   local have
@@ -220,6 +248,35 @@
   [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$2")" ]
 }
 
+# stack_forget NAME: drops NAME from STACK_PIDS and STACK_ORDER.
+stack_forget() {
+  local n order=()
+  unset "STACK_PIDS[$1]"
+  for n in "${STACK_ORDER[@]}"; do [ "$n" = "$1" ] || order+=("$n"); done
+  STACK_ORDER=("${order[@]}")
+}
+
+# stack_expect_dead NAME: NAME was killed on purpose (a load scenario's
+# SIGKILL through loadgen's /kill, choice #84). Waits up to 5s for it to
+# end, records "NAME expected:STATUS" in exit-status and forgets it, so
+# stack_health does not report it and stack_down does not stop it again.
+# Fails, keeping NAME, if it is still running.
+stack_expect_dead() {
+  local pid=${STACK_PIDS[$1]:-} i
+  [ -n "$pid" ] || { echo "stack: no process $1" >&2; return 1; }
+  for i in $(seq 50); do
+    kill -0 "$pid" 2>/dev/null || break
+    sleep 0.1
+  done
+  if kill -0 "$pid" 2>/dev/null; then
+    echo "stack: $1 is still running" >&2
+    return 1
+  fi
+  wait "$pid" 2>/dev/null
+  echo "$1 expected:$?" >>"$STACK_DIR/exit-status"
+  stack_forget "$1"
+}
+
 # stack_term WAIT NAME...: SIGTERM to NAMEs at once, wait up to WAIT seconds
 # for them to exit, then SIGKILL whatever is left. Appends "NAME STATUS" to
 # $STACK_DIR/exit-status, STATUS "killed" when it needed the SIGKILL.
@@ -233,7 +290,7 @@
     [ -z "$alive" ] && break
     sleep 0.1
   done
-  local pid status sent n order
+  local pid status sent
   for name in "$@"; do
     pid=${STACK_PIDS[$name]} sent=""
     kill -KILL "$pid" 2>/dev/null && sent=1
@@ -245,10 +302,7 @@
     echo "$name $status" >>"$STACK_DIR/exit-status"
     # Forgotten once recorded, so a stack_down cut short by a signal and
     # finished by the exit hook records each process once.
-    unset "STACK_PIDS[$name]"
-    order=()
-    for n in "${STACK_ORDER[@]}"; do [ "$n" = "$name" ] || order+=("$n"); done
-    STACK_ORDER=("${order[@]}")
+    stack_forget "$name"
   done
 }
 
@@ -273,13 +327,15 @@
 # process not running while the stack is up; once stack_down has run, a stop
 # that was not clean (status 0; devtoken has no SIGTERM handler, so 143 too)
 # or needed SIGKILL; and any log line starting with "panic:" or "fatal error:".
+# A process killed on purpose (stack_expect_dead) is not a problem, but its
+# log is still scanned.
 stack_health() {
   local name status line names=()
   # Stopped processes by their recorded status, running ones by kill -0.
   if [ -f "$STACK_DIR/exit-status" ]; then
     while read -r name status; do
       case "$name:$status" in
-        *:0 | devtoken:143) ;;
+        *:0 | devtoken:143 | *:expected:*) ;;
         *:killed) echo "$name: did not stop after SIGTERM, killed" ;;
         *) if [ "$status" -gt 128 ] 2>/dev/null; then
              echo "$name: stopped by SIG$(kill -l "$((status - 128))" 2>/dev/null || echo "$((status - 128))") (status $status)"
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `go test -race -count=1 ./internal/pprofhttp/ ./internal/gateway/ ./internal/logic/`
Expected: PASS.

Run: `make test-scripts`
Expected: `stack_test: all passed` (lib_test too). The stack test prints a bash `Killed` line for gw-1: that is the deliberate SIGKILL.

- [ ] **Step 5: Mutation check**

In a scratch copy, remove `*:expected:*` from `stack_health`'s accepted statuses: `stack_test.sh` must fail `dead: not a problem while the stack runs` and `dead: not a problem after the stop`. Remove `pprofhttp.Register(mux)` from `internal/gateway/server.go`: `TestAdminServesPprof` must fail. Restore both.

- [ ] **Step 6: Commit**

```bash
git add internal/pprofhttp internal/gateway/server.go internal/gateway/server_test.go internal/logic/server.go internal/logic/server_test.go scripts/stack.sh scripts/stack_test.sh
git commit -m "feat(stack): pprof on admin ports, stack profiles, expected kills (#85, #84)"
```

---

### Task 2: loadgen `Env` and the balancer

**Files:**
- Create: `internal/loadgen/env.go`, `internal/loadgen/env_test.go`, `internal/loadgen/balancer.go`, `internal/loadgen/balancer_test.go`
- Modify: `go.mod` (by `go mod tidy`)

**Interfaces:**
- Consumes: `scripts/stack.sh`'s `stack.env` keys `GATEWAY_ADDRS`, `GATEWAY_IDS`, `NOISE_KEY_ID`, `NOISE_PUBLIC` (base64, 32 bytes), `JWT_KEY` (path of devtoken's private JWK); `auth.ParsePrivateJWK`.
- Produces:
  - `type Env struct { GatewayAddrs, GatewayIDs []string; NoiseKeyID uint16; NoisePublic []byte; JWTKey auth.PrivateKey }`, `func LoadEnv(path string) (Env, error)`, `func (e Env) Gateway(id string) int` (-1 if unknown).
  - `var ErrNoGateway error`; `func Sources(n int) ([]net.IP, error)`; `func NewBalancer(addrs []string, sources int) (*Balancer, error)`; `func (b *Balancer) Dial(ctx context.Context) (net.Conn, int, error)` (the int is the gateway index; the connection counts as open until its first `Close`); `func (b *Balancer) SetDown(gw int)`; `func (b *Balancer) Open() []int`. 6a2b's tests read the unexported `b.down` and the `*conn` wrapper.

- [ ] **Step 1: Write the failing tests**

`internal/loadgen/env_test.go`:

```go
package loadgen

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tallinzp/goim/internal/auth"
)

// writeEnv writes a stack.env with a real token key; mod edits the lines.
func writeEnv(t *testing.T, mod func(map[string]string)) string {
	t.Helper()
	dir := t.TempDir()
	k, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := auth.MarshalPrivateJWK(k)
	key := filepath.Join(dir, "jwt.jwk")
	if err := os.WriteFile(key, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	kv := map[string]string{"GATEWAY_ADDRS": "127.0.0.1:1,127.0.0.1:2", "GATEWAY_IDS": "gw-0,gw-1",
		"NOISE_KEY_ID": "1", "NOISE_PUBLIC": base64.StdEncoding.EncodeToString(make([]byte, 32)),
		"JWT_KEY": key, "REDIS_ADDR": "127.0.0.1:3"}
	if mod != nil {
		mod(kv)
	}
	var b strings.Builder
	b.WriteString("# stack.env\n\n")
	for k, v := range kv {
		b.WriteString(k + "=" + v + "\n")
	}
	path := filepath.Join(dir, "stack.env")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadEnv(t *testing.T) {
	e, err := LoadEnv(writeEnv(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	if len(e.GatewayAddrs) != 2 || e.GatewayAddrs[1] != "127.0.0.1:2" || e.GatewayIDs[1] != "gw-1" ||
		e.NoiseKeyID != 1 || len(e.NoisePublic) != 32 || e.JWTKey.Kid != "k1" {
		t.Fatalf("env: %+v", e)
	}
	if e.Gateway("gw-1") != 1 || e.Gateway("gw-9") != -1 {
		t.Fatal("Gateway lookup")
	}
}

func TestLoadEnvRefusesBrokenFiles(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		mod        func(map[string]string)
	}{
		{"missing key", "NOISE_PUBLIC is missing", func(kv map[string]string) { delete(kv, "NOISE_PUBLIC") }},
		{"ids and addrs differ", "2 GATEWAY_ADDRS but 1 GATEWAY_IDS", func(kv map[string]string) { kv["GATEWAY_IDS"] = "gw-0" }},
		{"short noise key", "32-byte", func(kv map[string]string) { kv["NOISE_PUBLIC"] = "AAAA" }},
		{"bad key id", "NOISE_KEY_ID", func(kv map[string]string) { kv["NOISE_KEY_ID"] = "70000" }},
		{"no token key file", "JWT_KEY", func(kv map[string]string) { kv["JWT_KEY"] = "/nonexistent" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadEnv(writeEnv(t, tc.mod))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want %q", err, tc.want)
			}
		})
	}
}
```

`internal/loadgen/balancer_test.go`:

```go
package loadgen

import (
	"context"
	"errors"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// listeners starts n TCP listeners that accept and hold connections.
func listeners(t *testing.T, n int) []string {
	t.Helper()
	var addrs []string
	for range n {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		go func() {
			for {
				c, err := l.Accept()
				if err != nil {
					return
				}
				t.Cleanup(func() { c.Close() })
			}
		}()
		addrs = append(addrs, l.Addr().String())
	}
	return addrs
}

func TestBalancerLeastConnectionsAndSources(t *testing.T) {
	b, err := NewBalancer(listeners(t, 3), 2)
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	srcs := map[string]int{}
	for range 6 {
		c, _, err := b.Dial(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		srcs[c.LocalAddr().(*net.TCPAddr).IP.String()]++
	}
	if got := b.Open(); got[0] != 2 || got[1] != 2 || got[2] != 2 {
		t.Fatalf("open per gateway %v, want 2 each", got)
	}
	if srcs["127.0.0.2"] != 3 || srcs["127.0.0.3"] != 3 {
		t.Fatalf("sources %v, want 127.0.0.2 and .3 three times each", srcs)
	}
	// Closing frees the slot once: a second Close does not count again.
	conns[0].Close()
	conns[0].Close()
	if got := b.Open(); got[0]+got[1]+got[2] != 5 {
		t.Fatalf("open after one close %v", got)
	}
	// The next dial goes to the gateway that lost a connection.
	_, gw, err := b.Dial(context.Background())
	if err != nil || b.Open()[gw] != 2 {
		t.Fatalf("dial after close: gw %d %v %v", gw, b.Open(), err)
	}
}

func TestBalancerSkipsDownGateways(t *testing.T) {
	b, _ := NewBalancer(listeners(t, 2), 1)
	b.SetDown(0)
	for range 3 {
		if _, gw, err := b.Dial(context.Background()); err != nil || gw != 1 {
			t.Fatalf("dial: gw %d %v, want gw 1", gw, err)
		}
	}
	b.SetDown(1)
	if _, _, err := b.Dial(context.Background()); !errors.Is(err, ErrNoGateway) {
		t.Fatalf("all down: %v", err)
	}
}

func TestBalancerFailedDialFreesTheSlot(t *testing.T) {
	addrs := listeners(t, 1)
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := l.Addr().String()
	l.Close()
	b, _ := NewBalancer([]string{dead, addrs[0]}, 1)
	if _, gw, err := b.Dial(context.Background()); err == nil || gw != 0 {
		t.Fatalf("dial to a closed port: gw %d %v", gw, err)
	}
	if got := b.Open(); got[0] != 0 {
		t.Fatalf("a failed dial still counts: %v", got)
	}
}

// Each socket has IP_BIND_ADDRESS_NO_PORT, so ports are per (source,
// destination) pair (#69).
func TestBalancerBindsWithoutPort(t *testing.T) {
	b, _ := NewBalancer(listeners(t, 1), 1)
	c, _, err := b.Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := c.(*conn).Conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var v int
	var gerr error
	raw.Control(func(fd uintptr) { v, gerr = unix.GetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT) })
	if gerr != nil || v != 1 {
		t.Fatalf("IP_BIND_ADDRESS_NO_PORT = %d %v, want 1", v, gerr)
	}
}

func TestSourcesRange(t *testing.T) {
	if _, err := Sources(0); err == nil {
		t.Fatal("0 sources accepted")
	}
	if _, err := Sources(254); err == nil {
		t.Fatal("254 sources accepted (127.0.0.255 is past the range)")
	}
	ips, _ := Sources(253)
	if ips[252].String() != "127.0.0.254" {
		t.Fatalf("last source %v", ips[252])
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -count=1 ./internal/loadgen/`
Expected: FAIL to build: `undefined: LoadEnv`, `undefined: NewBalancer`, `undefined: Sources`, `undefined: ErrNoGateway`, `undefined: conn`.

- [ ] **Step 3: Implement**

`internal/loadgen/env.go`:

```go
// Package loadgen is the load generator behind cmd/loadgen (Plan 6a2): many
// client connections from one process, spread over the stack's gateways by
// a least-connections balancer, with per-client times that give the storm
// SLO distribution directly (spec: Verification → Load tests). Two modes
// (choice #80): light holds connections with clientconn and a shared
// pinger; sdk runs one internal/sdk client per connection, with its
// reconnect rules.
package loadgen

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Tallinzp/goim/internal/auth"
)

// Env is what loadgen reads from scripts/stack.sh's stack.env (P6a1I-4).
type Env struct {
	GatewayAddrs []string // client listeners, in gateway order
	GatewayIDs   []string // gw-0, gw-1, … in the same order
	NoiseKeyID   uint16
	NoisePublic  []byte // the pinned Noise static public key
	JWTKey       auth.PrivateKey
}

// LoadEnv reads a stack.env file: KEY=VALUE lines, blank lines and #
// comments skipped. GATEWAY_ADDRS, GATEWAY_IDS, NOISE_KEY_ID, NOISE_PUBLIC
// and JWT_KEY (the path of devtoken's private key file) are required.
func LoadEnv(path string) (Env, error) {
	f, err := os.Open(path)
	if err != nil {
		return Env{}, err
	}
	defer f.Close()
	kv := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			return Env{}, fmt.Errorf("loadgen: %s: not KEY=VALUE: %q", path, line)
		}
		kv[k] = v
	}
	if err := sc.Err(); err != nil {
		return Env{}, err
	}
	for _, k := range []string{"GATEWAY_ADDRS", "GATEWAY_IDS", "NOISE_KEY_ID", "NOISE_PUBLIC", "JWT_KEY"} {
		if kv[k] == "" {
			return Env{}, fmt.Errorf("loadgen: %s: %s is missing", path, k)
		}
	}
	e := Env{GatewayAddrs: strings.Split(kv["GATEWAY_ADDRS"], ","), GatewayIDs: strings.Split(kv["GATEWAY_IDS"], ",")}
	if len(e.GatewayAddrs) != len(e.GatewayIDs) {
		return Env{}, fmt.Errorf("loadgen: %s: %d GATEWAY_ADDRS but %d GATEWAY_IDS", path, len(e.GatewayAddrs), len(e.GatewayIDs))
	}
	id, err := strconv.ParseUint(kv["NOISE_KEY_ID"], 10, 16)
	if err != nil {
		return Env{}, fmt.Errorf("loadgen: %s: NOISE_KEY_ID: %w", path, err)
	}
	e.NoiseKeyID = uint16(id)
	if e.NoisePublic, err = base64.StdEncoding.DecodeString(kv["NOISE_PUBLIC"]); err != nil || len(e.NoisePublic) != 32 {
		return Env{}, fmt.Errorf("loadgen: %s: NOISE_PUBLIC is not a base64 32-byte key", path)
	}
	doc, err := os.ReadFile(kv["JWT_KEY"])
	if err != nil {
		return Env{}, fmt.Errorf("loadgen: %s: JWT_KEY: %w", path, err)
	}
	if e.JWTKey, err = auth.ParsePrivateJWK(doc); err != nil {
		return Env{}, fmt.Errorf("loadgen: %s: JWT_KEY: %w", path, err)
	}
	return e, nil
}

// Gateway is the index of gateway id, or -1.
func (e Env) Gateway(id string) int {
	for i, g := range e.GatewayIDs {
		if g == id {
			return i
		}
	}
	return -1
}
```

`internal/loadgen/balancer.go`:

```go
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

// ErrNoGateway is a dial while every gateway is marked down.
var ErrNoGateway = errors.New("loadgen: every gateway is down")

// Balancer stands in for the NLB (choice #81): each dial goes to the live
// gateway with the fewest open connections (the least-connections algorithm
// the spec asks of the cloud LB, R5S-O4), from the next source address in
// turn. A gateway marked down (loadgen killed or silenced it) gets no new
// dials, as an LB removes a failed target.
type Balancer struct {
	addrs []string
	srcs  []net.IP
	next  atomic.Uint64
	mu    sync.Mutex
	open  []int
	down  []bool
}

// Sources are loadgen's source addresses 127.0.0.2, 127.0.0.3, … (choice
// #69): Linux answers on all of 127/8 with no setup.
func Sources(n int) ([]net.IP, error) {
	if n < 1 || n > 253 {
		return nil, fmt.Errorf("loadgen: %d sources, want 1–253", n)
	}
	ips := make([]net.IP, n)
	for i := range ips {
		ips[i] = net.IPv4(127, 0, 0, byte(2+i))
	}
	return ips, nil
}

// NewBalancer balances over addrs from n source addresses.
func NewBalancer(addrs []string, sources int) (*Balancer, error) {
	srcs, err := Sources(sources)
	if err != nil {
		return nil, err
	}
	return &Balancer{addrs: addrs, srcs: srcs, open: make([]int, len(addrs)), down: make([]bool, len(addrs))}, nil
}

// SetDown takes gateway gw out of rotation for new dials.
func (b *Balancer) SetDown(gw int) {
	b.mu.Lock()
	b.down[gw] = true
	b.mu.Unlock()
}

// Open is the number of open connections per gateway.
func (b *Balancer) Open() []int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int(nil), b.open...)
}

func (b *Balancer) pick() (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	best := -1
	for i := range b.addrs {
		if !b.down[i] && (best < 0 || b.open[i] < b.open[best]) {
			best = i
		}
	}
	if best < 0 {
		return -1, ErrNoGateway
	}
	b.open[best]++
	return best, nil
}

func (b *Balancer) release(gw int) {
	b.mu.Lock()
	b.open[gw]--
	b.mu.Unlock()
}

// Dial connects to the chosen gateway and returns its index. The
// connection counts as open on that gateway until it is closed.
func (b *Balancer) Dial(ctx context.Context) (net.Conn, int, error) {
	gw, err := b.pick()
	if err != nil {
		return nil, -1, err
	}
	src := b.srcs[b.next.Add(1)%uint64(len(b.srcs))]
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: src}, Control: bindNoPort}
	nc, err := d.DialContext(ctx, "tcp", b.addrs[gw])
	if err != nil {
		b.release(gw)
		return nil, gw, err
	}
	return &conn{Conn: nc, release: func() { b.release(gw) }}, gw, nil
}

// bindNoPort sets IP_BIND_ADDRESS_NO_PORT: the kernel picks the source port
// at connect, per (source, destination) pair. Without it, bind() picks a
// port unique to the source address across every destination, and each
// source gives 28,232 connections in total instead of per gateway (#69).
func bindNoPort(_, _ string, rc syscall.RawConn) error {
	var serr error
	if err := rc.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BIND_ADDRESS_NO_PORT, 1)
	}); err != nil {
		return err
	}
	return serr
}

// conn releases its balancer slot once, on the first Close.
type conn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *conn) Close() error {
	c.once.Do(c.release)
	return c.Conn.Close()
}
```

Then `go mod tidy`, which moves `golang.org/x/sys` to the direct requirements:

```diff
diff --git a/go.mod b/go.mod
index 052501f..04ade22 100644
--- a/go.mod
+++ b/go.mod
@@ -9,6 +9,7 @@ require (
 	github.com/prometheus/client_golang v1.24.1
 	github.com/prometheus/client_model v0.6.2
 	github.com/redis/go-redis/v9 v9.23.0
+	golang.org/x/sys v0.48.0
 	golang.org/x/time v0.16.0
 	google.golang.org/grpc v1.84.0
 	google.golang.org/protobuf v1.36.12
@@ -30,7 +31,6 @@ require (
 	golang.org/x/crypto v0.54.0 // indirect
 	golang.org/x/net v0.57.0 // indirect
 	golang.org/x/sync v0.22.0 // indirect
-	golang.org/x/sys v0.48.0 // indirect
 	golang.org/x/text v0.40.0 // indirect
 	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
 	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `go vet ./internal/loadgen/ && go test -race -count=1 ./internal/loadgen/`
Expected: PASS.

- [ ] **Step 5: Mutation check**

Each of these must fail a test; restore after each: `IP_BIND_ADDRESS_NO_PORT, 1` → `0` (`TestBalancerBindsWithoutPort`); `b.open[i] < b.open[best]` → `>` (`TestBalancerLeastConnectionsAndSources`); delete `b.release(gw)` in `Dial`'s error path (`TestBalancerFailedDialFreesTheSlot`).

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen go.mod go.sum
git commit -m "feat(loadgen): stack.env reader and least-connections balancer (#81)"
```

---

### Task 3: distributions and the recorder

**Files:**
- Create: `internal/loadgen/hist.go`, `internal/loadgen/hist_test.go`, `internal/loadgen/recorder.go`, `internal/loadgen/recorder_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces (6a2b's runner, control API and `cmd/loadgen` use all of it):
  - `type Dist struct { N, Missing, P50, P90, P99, Max int64 }` (JSON `n`, `missing`, `p50_ms`, `p90_ms`, `p99_ms`, `max_ms`; -1 = not reached) and `func (d Dist) Within(limitMs int64) bool`.
  - `func NewRecorder(n int, ids []string) *Recorder`; `Online(i, gw int, start, now time.Time)`; `Lost(i int, why string, now time.Time)`; `Failed(why string)`; `Pong(rtt time.Duration)`; `PongMissing()`; `Mark(name string, gw int, now time.Time) (int, error)` (gw -1: every gateway; returns the population); `SLO(name string, within time.Duration) (SLO, error)`; `Marks() []string` (sorted); `Counts() Counts`.
  - `type SLO struct` (JSON `mark`, `gateway`, `at_ms`, `within_ms`, `population`, `lost`, `back`, `from_mark`, `detection`, `from_detection`, `pass_from_mark`, `pass_from_detection`) and `type Counts struct` (JSON `online`, `online_by_gateway`, `logins`, `lost`, `failed`, `login_latency`, `pongs`, `pong_missing`, `pong_rtt`).

- [ ] **Step 1: Write the failing tests**

`internal/loadgen/hist_test.go`:

```go
package loadgen

import "testing"

func TestHistPercentiles(t *testing.T) {
	h := newHist()
	for ms := int64(1); ms <= 100; ms++ {
		h.add(ms)
	}
	d := h.dist(100)
	if d.P50 != 50 || d.P90 != 90 || d.P99 != 99 || d.Max != 100 || d.Missing != 0 {
		t.Fatalf("dist %+v", d)
	}
	if !d.Within(99) || d.Within(98) {
		t.Fatal("Within")
	}
}

// Members with no sample are infinitely slow: with 2 of 100 missing, p99
// is not reached.
func TestHistMissingMembersFailThePercentile(t *testing.T) {
	h := newHist()
	for range 98 {
		h.add(10)
	}
	d := h.dist(100)
	if d.P50 != 10 || d.P90 != 10 || d.P99 != -1 || d.Missing != 2 || d.Within(1_000_000) {
		t.Fatalf("dist %+v", d)
	}
	h.add(10)
	if d := h.dist(100); d.P99 != 10 {
		t.Fatalf("99 of 100: p99 %d, want 10", d.P99)
	}
}

// Exact to the millisecond up to 60s (every SLO bound), 100ms above.
func TestHistCoarseAboveAMinute(t *testing.T) {
	h := newHist()
	h.add(30_001)
	h.add(61_234)
	if d := h.dist(2); d.P50 != 30_001 || d.P99 != 61_200 || d.Max != 61_234 {
		t.Fatalf("dist %+v", d)
	}
	h.add(7_200_000) // 2h: the last bucket
	if d := h.dist(3); d.P99 != histValue(histBucket-1) || d.Max != 7_200_000 {
		t.Fatalf("dist %+v", d)
	}
	if empty := newHist().dist(0); empty.P99 != -1 || empty.Max != -1 {
		t.Fatalf("empty %+v", empty)
	}
}
```

`internal/loadgen/recorder_test.go`:

```go
package loadgen

import (
	"testing"
	"time"
)

var t0 = time.UnixMilli(1_000_000)

func at(ms int64) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func TestRecorderCountsPerGateway(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0", "gw-1"})
	r.Online(0, 0, at(0), at(5))
	r.Online(1, 1, at(0), at(7))
	r.Online(2, 1, at(0), at(9))
	r.Lost(2, "lost", at(10))
	r.Lost(2, "lost", at(11)) // not logged in: ignored
	r.Failed("dial")
	c := r.Counts()
	if c.Online != 2 || c.OnlineByGateway["gw-0"] != 1 || c.OnlineByGateway["gw-1"] != 1 || c.Logins != 3 ||
		c.Lost["lost"] != 1 || c.Failed["dial"] != 1 || c.LoginLatency.Max != 9 || c.LoginLatency.N != 3 {
		t.Fatalf("counts %+v", c)
	}
}

// The population is the clients logged in on the marked gateway at the
// mark; times run from the mark (b1) and from each client's loss (b2).
func TestRecorderSLOFromMarkAndDetection(t *testing.T) {
	r := NewRecorder(4, []string{"gw-0", "gw-1"})
	for i := range 3 {
		r.Online(i, 1, at(0), at(1))
	}
	r.Online(3, 0, at(0), at(1)) // on another gateway: not in the population
	if n, err := r.Mark("death", 1, at(1000)); err != nil || n != 3 {
		t.Fatalf("mark: %d %v", n, err)
	}
	if _, err := r.Mark("death", 1, at(1000)); err == nil {
		t.Fatal("duplicate mark accepted")
	}
	r.Lost(0, "lost", at(1010))
	r.Lost(1, "lost", at(1020))
	r.Lost(2, "pong-timeout", at(13_000))
	r.Online(0, 0, at(5000), at(6000))
	r.Online(1, 0, at(20000), at(21_020))
	r.Online(2, 0, at(30000), at(33_000))
	r.Lost(3, "lost", at(40_000)) // not a member: no effect
	s, err := r.SLO("death", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if s.Gateway != "gw-1" || s.Population != 3 || s.Lost != 3 || s.Back != 3 {
		t.Fatalf("slo %+v", s)
	}
	// From the mark: 5000, 20020, 32000 → p99 32000 > 30000.
	if s.FromMark.P50 != 20_020 || s.FromMark.P99 != 32_000 || s.PassFromMark {
		t.Fatalf("from mark %+v pass %v", s.FromMark, s.PassFromMark)
	}
	// From detection: 4990, 20000, 20000 → p99 20000.
	if s.FromDetection.P99 != 20_000 || !s.PassFromDetection {
		t.Fatalf("from detection %+v pass %v", s.FromDetection, s.PassFromDetection)
	}
	if s.Detection.P99 != 12_000 {
		t.Fatalf("detection %+v", s.Detection)
	}
}

// A member that never logs in again fails the SLO, however fast the rest.
func TestRecorderSLOMissingMemberFails(t *testing.T) {
	r := NewRecorder(2, []string{"gw-0", "gw-1"})
	r.Online(0, 0, at(0), at(1))
	r.Online(1, 0, at(0), at(1))
	r.Mark("death", 0, at(100))
	r.Lost(0, "lost", at(101))
	r.Lost(1, "lost", at(101))
	r.Online(0, 0, at(200), at(300))
	s, _ := r.SLO("death", 30*time.Second)
	if s.Back != 1 || s.FromMark.Missing != 1 || s.PassFromMark || s.PassFromDetection {
		t.Fatalf("slo %+v", s)
	}
	if _, err := r.SLO("nope", time.Second); err == nil {
		t.Fatal("unknown mark accepted")
	}
	// A mark with nobody in it does not pass either.
	r.Mark("empty", 1, at(400))
	if s, _ := r.SLO("empty", time.Second); s.Population != 0 || s.PassFromMark {
		t.Fatalf("empty %+v", s)
	}
}

// A mark on every gateway (-1) takes everyone logged in.
func TestRecorderMarkAll(t *testing.T) {
	r := NewRecorder(3, []string{"gw-0", "gw-1"})
	r.Online(0, 0, at(0), at(1))
	r.Online(1, 1, at(0), at(1))
	if n, _ := r.Mark("all", -1, at(5)); n != 2 {
		t.Fatalf("population %d, want 2", n)
	}
	if s, _ := r.SLO("all", time.Second); s.Gateway != "all" {
		t.Fatalf("gateway %q", s.Gateway)
	}
	if got := r.Marks(); len(got) != 1 || got[0] != "all" {
		t.Fatalf("marks %v", got)
	}
}

// A member counts as back only after it lost its connection: a second
// Online with no loss in between (not a relogin) is not.
func TestRecorderBackNeedsALoss(t *testing.T) {
	r := NewRecorder(1, []string{"gw-0"})
	r.Online(0, 0, at(0), at(1))
	r.Mark("m", 0, at(10))
	r.Online(0, 0, at(20), at(30))
	if s, _ := r.SLO("m", time.Second); s.Back != 0 {
		t.Fatalf("back %d without a loss", s.Back)
	}
	r.Lost(0, "lost", at(40))
	r.Online(0, 0, at(50), at(60))
	if s, _ := r.SLO("m", time.Second); s.Back != 1 || s.FromMark.P50 != 50 {
		t.Fatalf("slo %+v", s)
	}
}
```

- [ ] **Step 2: Run them to see them fail**

Run: `go test -count=1 ./internal/loadgen/ -run 'Hist|Recorder'`
Expected: FAIL to build: `undefined: newHist`, `undefined: NewRecorder`.

- [ ] **Step 3: Implement**

`internal/loadgen/hist.go`:

```go
package loadgen

import "math"

// Dist is a latency distribution in milliseconds over a population of N.
// Members with no sample (a client that never logged in again) count as
// infinitely slow: a percentile they reach is -1, so it fails any bound.
type Dist struct {
	N       int64 `json:"n"`
	Missing int64 `json:"missing"`
	P50     int64 `json:"p50_ms"`
	P90     int64 `json:"p90_ms"`
	P99     int64 `json:"p99_ms"`
	Max     int64 `json:"max_ms"` // of the samples; -1 with none
}

// Within reports whether p99 is reached and at most limitMs.
func (d Dist) Within(limitMs int64) bool { return d.P99 >= 0 && d.P99 <= limitMs }

// hist counts samples at 1ms resolution below 60s and 100ms above, up to
// 1h (the last bucket holds everything longer): 95,400 buckets, 763KB, so
// memory stays fixed at 600k clients. Every storm bound (30s, and b2's
// detection plus 30s) falls in the exact range: a bucket's lower bound
// above 60s could pass a p99 up to 99ms over the bound.
type hist struct {
	n, max int64
	counts []int64
}

const (
	histFine   = 60_000 // ms at 1ms resolution
	histStep   = 100    // ms per bucket above
	histBucket = histFine + (3_600_000-histFine)/histStep
)

func newHist() *hist { return &hist{max: -1, counts: make([]int64, histBucket)} }

func histIndex(ms int64) int {
	switch {
	case ms < 0:
		return 0
	case ms < histFine:
		return int(ms)
	}
	return min(histFine+int((ms-histFine)/histStep), histBucket-1)
}

// histValue is bucket i's lower bound in ms.
func histValue(i int) int64 {
	if i < histFine {
		return int64(i)
	}
	return histFine + int64(i-histFine)*histStep
}

func (h *hist) add(ms int64) {
	h.counts[histIndex(ms)]++
	h.n++
	h.max = max(h.max, ms)
}

// dist is the distribution over a population of n ≥ h.n members.
func (h *hist) dist(n int64) Dist {
	n = max(n, h.n)
	d := Dist{N: n, Missing: n - h.n, Max: h.max}
	d.P50, d.P90, d.P99 = h.rank(n, 0.50), h.rank(n, 0.90), h.rank(n, 0.99)
	return d
}

// rank is the value at percentile p of n, -1 if it falls on a missing member.
func (h *hist) rank(n int64, p float64) int64 {
	if n == 0 {
		return -1
	}
	want := int64(math.Ceil(p * float64(n)))
	if want > h.n {
		return -1
	}
	var seen int64
	for i, c := range h.counts {
		if seen += c; seen >= want {
			return histValue(i)
		}
	}
	return -1
}
```

`internal/loadgen/recorder.go`:

```go
package loadgen

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Recorder keeps per-client times and the run's counters (choice #83). Every
// method takes one mutex: events arrive at most at the login rate (20k/s),
// far below what a mutex serves.
type Recorder struct {
	mu      sync.Mutex
	ids     []string
	clients []clientState
	online  []int
	logins  int64
	login   *hist
	lost    map[string]int64
	failed  map[string]int64
	pongs   int64
	missing int64
	pongRTT *hist
	marks   []*mark // by index
	byName  map[string]*mark
}

// clientState: gw is the gateway it is logged in on (-1: none); mark and
// pos place it in its latest mark (-1: none).
type clientState struct{ gw, mark, pos int32 }

// mark is a named instant and the clients logged in on its gateway then
// (the population of an SLO), with when each lost its connection and
// logged in again after it (0: not yet).
type mark struct {
	gw         int
	at         int64
	members    []int32
	lost, back []int64
}

// NewRecorder records clients 0..n-1 over gateways ids.
func NewRecorder(n int, ids []string) *Recorder {
	r := &Recorder{ids: ids, clients: make([]clientState, n), online: make([]int, len(ids)),
		login: newHist(), pongRTT: newHist(), lost: map[string]int64{}, failed: map[string]int64{}, byName: map[string]*mark{}}
	for i := range r.clients {
		r.clients[i] = clientState{gw: -1, mark: -1}
	}
	return r
}

func (r *Recorder) markOf(c clientState) *mark {
	if c.mark < 0 {
		return nil
	}
	return r.marks[c.mark]
}

// Online: client i logged in on gateway gw; its attempt started at start.
func (r *Recorder) Online(i, gw int, start, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &r.clients[i]
	if c.gw >= 0 {
		r.online[c.gw]--
	}
	c.gw = int32(gw)
	r.online[gw]++
	r.logins++
	r.login.add(now.Sub(start).Milliseconds())
	if m := r.markOf(*c); m != nil && m.back[c.pos] == 0 && m.lost[c.pos] != 0 {
		m.back[c.pos] = now.UnixMilli()
	}
}

// Lost: client i's logged-in connection ended, for why (an sdk.EndKind name,
// or what the light client saw).
func (r *Recorder) Lost(i int, why string, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := &r.clients[i]
	if c.gw < 0 {
		return
	}
	r.online[c.gw]--
	c.gw = -1
	r.lost[why]++
	if m := r.markOf(*c); m != nil && m.lost[c.pos] == 0 {
		m.lost[c.pos] = now.UnixMilli()
	}
}

// Failed: an attempt ended before logging in, for why.
func (r *Recorder) Failed(why string) {
	r.mu.Lock()
	r.failed[why]++
	r.mu.Unlock()
}

// Pong: a Ping was answered after rtt.
func (r *Recorder) Pong(rtt time.Duration) {
	r.mu.Lock()
	r.pongs++
	r.pongRTT.add(rtt.Milliseconds())
	r.mu.Unlock()
}

// PongMissing: a Ping was due while the previous one had no Pong yet.
func (r *Recorder) PongMissing() {
	r.mu.Lock()
	r.missing++
	r.mu.Unlock()
}

// Mark records instant now as name, with the clients logged in on gateway
// gw (-1: on any gateway) as its population, and returns its size. A client
// belongs to its latest mark only. Names are unique.
func (r *Recorder) Mark(name string, gw int, now time.Time) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byName[name]; dup {
		return 0, fmt.Errorf("loadgen: mark %q exists", name)
	}
	idx, m := int32(len(r.marks)), &mark{gw: gw, at: now.UnixMilli()}
	for i := range r.clients {
		c := &r.clients[i]
		if c.gw >= 0 && (gw < 0 || int(c.gw) == gw) {
			c.mark, c.pos = idx, int32(len(m.members))
			m.members = append(m.members, int32(i))
		}
	}
	m.lost, m.back = make([]int64, len(m.members)), make([]int64, len(m.members))
	r.marks = append(r.marks, m)
	r.byName[name] = m
	return len(m.members), nil
}

// SLO is the storm distribution after a mark (spec: Load tests; loadgen
// computes it directly). From the mark: b1's "logged in again within 30s of
// T_death". From detection: b2's "within 30s of the client detecting the
// loss". A member that never logged in again fails both.
type SLO struct {
	Mark              string `json:"mark"`
	Gateway           string `json:"gateway"`
	AtMs              int64  `json:"at_ms"`
	WithinMs          int64  `json:"within_ms"`
	Population        int    `json:"population"`
	Lost              int    `json:"lost"`
	Back              int    `json:"back"`
	FromMark          Dist   `json:"from_mark"`
	Detection         Dist   `json:"detection"`
	FromDetection     Dist   `json:"from_detection"`
	PassFromMark      bool   `json:"pass_from_mark"`
	PassFromDetection bool   `json:"pass_from_detection"`
}

// SLO computes mark name's distributions; pass means p99 ≤ within.
func (r *Recorder) SLO(name string, within time.Duration) (SLO, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, ok := r.byName[name]
	if !ok {
		return SLO{}, fmt.Errorf("loadgen: no mark %q", name)
	}
	s := SLO{Mark: name, Gateway: "all", AtMs: m.at, WithinMs: within.Milliseconds(), Population: len(m.members)}
	if m.gw >= 0 {
		s.Gateway = r.ids[m.gw]
	}
	fromMark, detect, fromDetect := newHist(), newHist(), newHist()
	for k := range m.members {
		if m.lost[k] != 0 {
			s.Lost++
			detect.add(m.lost[k] - m.at)
		}
		if m.back[k] != 0 {
			s.Back++
			fromMark.add(m.back[k] - m.at)
			fromDetect.add(m.back[k] - m.lost[k])
		}
	}
	n := int64(len(m.members))
	s.FromMark, s.Detection, s.FromDetection = fromMark.dist(n), detect.dist(n), fromDetect.dist(n)
	s.PassFromMark = n > 0 && s.FromMark.Within(s.WithinMs)
	s.PassFromDetection = n > 0 && s.FromDetection.Within(s.WithinMs)
	return s, nil
}

// Marks are the names of every mark.
func (r *Recorder) Marks() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.byName))
	for n := range r.byName {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Counts are the run's counters now.
type Counts struct {
	Online          int              `json:"online"`
	OnlineByGateway map[string]int   `json:"online_by_gateway"`
	Logins          int64            `json:"logins"`
	Lost            map[string]int64 `json:"lost"`
	Failed          map[string]int64 `json:"failed"`
	LoginLatency    Dist             `json:"login_latency"`
	Pongs           int64            `json:"pongs"`
	PongMissing     int64            `json:"pong_missing"`
	PongRTT         Dist             `json:"pong_rtt"`
}

// Counts returns a snapshot of the counters.
func (r *Recorder) Counts() Counts {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := Counts{OnlineByGateway: map[string]int{}, Logins: r.logins, Lost: map[string]int64{}, Failed: map[string]int64{},
		LoginLatency: r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PongRTT: r.pongRTT.dist(r.pongRTT.n)}
	for i, n := range r.online {
		c.Online += n
		c.OnlineByGateway[r.ids[i]] = n
	}
	for k, v := range r.lost {
		c.Lost[k] = v
	}
	for k, v := range r.failed {
		c.Failed[k] = v
	}
	return c
}
```

- [ ] **Step 4: Run the tests to see them pass**

Run: `go vet ./internal/loadgen/ && go test -race -count=1 ./internal/loadgen/`
Expected: PASS.

- [ ] **Step 5: Mutation check**

Each must fail a test; restore after each: in `Mark`, `(gw < 0 || int(c.gw) == gw)` → `true` (`TestRecorderSLOFromMarkAndDetection`); in `Online`, drop `&& m.lost[c.pos] != 0` (`TestRecorderBackNeedsALoss`); `s.PassFromMark = n > 0 && …` → `true` (`TestRecorderSLOFromMarkAndDetection`); in `hist.go`, `histFine` 60_000 → 10_000 (`TestHistCoarseAboveAMinute`). Not a mutant worth running: deleting `rank`'s early `want > h.n` return is equivalent (the loop then returns -1 itself).

- [ ] **Step 6: Commit, then the whole suite**

```bash
git add internal/loadgen
git commit -m "feat(loadgen): fixed-memory distributions and the SLO recorder (#83)"
make test
```
Expected: `make test` passes. Record the commits in *Execution Notes*; Plan 6a2b runs in a fresh session.

---

## After this plan

- **Plan 6a2b** (its own session): Task 4 `internal/loadgen` runner (light clients and pinger, SDK clients, `Kill`/`Mark`, control API, in-process end-to-end tests), Task 5 `cmd/loadgen`, `scripts/loadgen.sh`, `make test-load-probe`, Task 6 the Tier 2 `smoke` scenario and Tier 2 run 13.
- **Plan 6b:** the balancer is the NLB stand-in for every Tier 2 scenario. b2 calls `POST /mark?gateway=gw-k&mark=drop` when it adds the DROP (no `down=1` until the scenario models the LB's health check removing the target), and judges `pass_from_detection`; b1 and the storm use `/kill` and `pass_from_mark`. A measured drain uses `stack_stop` (6a1 note) and, if loadgen must stop dialing the draining pod, `/mark … down=1` at NotReady. Whole-population reconnect is `/mark?gateway=all`.
- **Plan 6d:** keep the admin port (pprof, metrics) off any Service reachable from outside the cluster (#85).

## Execution Notes

(Filled in during execution.)

- **Executed 2026-10-09 inline (superpowers:executing-plans), on `feat/plan6a2-loadgen`:** Task 1 `ec22ab5`, Task 2 `6588e2c`, Task 3 `bf3186c`, each block applied verbatim from the plan with no rulings; every Step 2 failed as expected, every mutant named in Step 5 was caught. `make test` and `make test-scripts` green. **Final review** (fresh Opus reviewer): 0 Critical, 1 Important, 5 Minor. Fixed `7f3c15b`: the balancer's one source turn shared by every gateway fell into step with least connections (which visits the gateways in order), so with gcd(gateways, sources) = g > 1 each gateway used 1/g of the sources: 4 gateways × 200k with 10 sources would put ~40k connections on a (source, destination) pair against 28,232 ports. Sources now turn per gateway; `TestBalancerEveryGatewayUsesEverySource` (2 gateways × 2 sources) failed first. **Deferred minors, for 6a2b/6b:** `stack_expect_dead` accepts any exit status (`stack_health` could accept only `expected:137`, and 6a2b's `/kill` check it); `stack_profile` with no process up runs a bare `wait` (hangs) and hides a failed curl; a later mark silently takes members from an earlier one (false fail only; could report a reassigned count); the recorder stores wall-clock `UnixMilli`, not monotonic offsets; a negative sample can make a histogram's `Max` < 0 (cosmetic). **Minors fixed at the user's request ("if necessary"):** `5da8c42`: `stack_health` accepts only `expected:137` and reports any other status as `NAME: expected a kill, exited with status N`; `stack_profile` waits on each curl and fails if any profile failed (a bare `wait` with nothing up waited for Redis; one `wait` for several pids returned the last status only). `60fc079`: `SLO.Reassigned` (JSON `reassigned`) counts members a later mark took over before they were back. Each test failed first (`dead: another status is reported`, `profile: nothing up does not wait`, `profile: a failed profile fails`, `TestRecorderSLOReportsReassignedMembers`). **Left as is:** wall-clock `UnixMilli` in the recorder (a clock step inside a ~30s storm window on a slewing chrony host is unlikely, and offsets from a monotonic start would change the 0 sentinel's meaning; revisit if a Tier 3 run shows a step); a negative sample (cannot happen with ordered timestamps).

- **Plan 6a2b executed 2026-10-09 inline (superpowers:executing-plans), on `feat/plan6a2-loadgen`:** Task 4 `b6e5609`, Task 5 `f2f1527`, Task 6 `2e28c67`, plan code verbatim; every Step 2 failed as expected and every named mutant was caught (`Mark` without `SetDown`, gateway removed before the mark, no login retry, missing Pongs not counted, `stack_expect_dead` in `$(...)` → `processes: gw-1: expected a kill, exited with status -1`); `run`'s write-then-stop order checked by reading. `make test`, `make test-scripts` green. **Probe:** light 3,000 online, login p99 174–310ms, Pong p99 11–82ms, `failed {}`; SDK population 1,000, from `T_death` p50 ~10.3s, p99 19.8–19.9s. **Smoke dry run** (4,000 light, 2,000 SDK): 9/10, `no_swap_in_light` failing on the dev machine's own swap (19 pages); `slo_b1` population 500, p99 19.9s; `rss_mb` light: loadgen 65MB, gateways 23MB. **Found and fixed outside the plan (`60561a2`, a ruling):** a probe started within ~60s of the last failed `stack_up` with gw-1 `tcp: bind: address already in use`: the gnet listener had no SO_REUSEADDR (gnet's default is false), and a gateway's closed connections, by drain or by the kernel closing a SIGKILLed process's sockets with a FIN, leave ~1,000 TIME_WAIT sockets on its listen port. That would also have failed run 13's `foundation` right after `smoke`, any 6b scenario that restarts a gateway, and a gateway container restarted in the same pod. `gnet.WithReuseAddr(true)` (as `net.Listen` does; `WithReusePort` stays false); `TestListenBesideTimeWait` failed first; the probe then passed twice back to back. **Final review** (fresh Opus reviewer): 0 Critical, 0 Important, agrees with `60561a2`; **deferred minors:** (1) default `-sources` is sized for clients ÷ N gateways, so after `/kill` on a 2-gateway 200k run a (source, gateway) pair holds ~40k connections > 28,232 ports: **6b sizes sources against N−1 gateways or passes `-sources`**; (2) no test catches `writeResult` moving after the runner stop (the held checks accept `lost {"stopped"}`; tightening them to `lost == {}` would); (3) the light `gave-up` path is untested; (4) `/kill` with a stale pid takes the mark and the gateway out of the balancer before `syscall.Kill` fails (409); (5) the control port `STACK_PORT_BASE + 5` is not in `stack_busy_ports`, so a leftover loadgen could answer `lg_start`'s `/stats`. **Next:** Tier 2 run 13 (6a2b Task 6 Step 5) by the user on the host.
- **Tier 2 run 13 (2026-10-10, on `7f4ca54`, dirty 0, 24 cores, kernel 7.0): passed.** `smoke` 10/10 in 119s, `foundation` 6/6 in 37s. **Light:** 100,000 online, 25,000 per gateway, ramp 21s at 5,000/s; `failed {"pre-handshake": 2258}` (2.3% of dials closed by the gateways' handshake limit, all retried, none `gave-up`); login p99 24ms (max 52ms); 718,334 Pongs, none missing, Pong p99 11ms; none lost while held; no swap-in. **SDK:** 20,000 online, ramp 12s at 2,000/s, `failed {"pre-handshake": 410}` (1.6%); `/kill` of gw-3: population 5,000, all back, none reassigned; from the mark p50 10.1s, p99 19.87s, max 20.03s (bound 30s; the expected 0–20s storm window); detection p99 47ms. **Sizing for 6b (`rss_mb`):** light loadgen 888MB at 100k = ~8.9KB per connection (600k ≈ 5.3GB); gateway 95–97MB at 25k = ~3–3.8KB per connection (600k on one gateway ≈ 2GB); SDK loadgen 491MB at 20k = ~24KB per client (200k ≈ 4.9GB in one process, at the low end of P5I-2's 25–35KB, so one loadgen process fits on this host's 23GB available, alongside the gateways); gateways 64–66MB in the SDK phase. **CPU:** gateway set mean 1,964MHz under load (min 1,394MHz), Logic 1,656MHz, loadgen 2,762MHz; host busy 32.5 CPU-s in the 60s light window, 1.8s outside ours (top: mongod 0.5s). **Ramp for 6b:** 5,000/s against 4 gateways with handshake concurrency 2 each costs ~2% retries and no holes, so 6b keeps 5,000/s and watches `failed` for `gave-up`. **SO_REUSEADDR confirmed:** `foundation` bound 17100–17130 at 22:21:17, 4s after `smoke` began draining and well within 60s of gw-3's kill; before `60561a2` it would have failed to bind. Redis logs the `vm.overcommit_memory` warning (6c, as noted). Plan 6a2 complete.
- **6a2b deferred minors decided (2026-10-10, Claude at the user's request), fixed in `dc940ba`:** (1) **fixed:** `New`'s default `Sources` is sized for one gateway less than the stack has (`max(1, N−1)`), so after `/kill` the survivors' (source, gateway) pairs stay ≤ 20,000 connections: 2 gateways × 200k now get 10 sources, not 5; `TestNewSizesSourcesForOneGatewayLess` failed first. Source addresses cost nothing on 127/8, so the default carries the margin rather than every 6b scenario remembering `-sources`. (2) **fixed:** `smoke.sh`'s `light_held` and the probe's "none lost while held" now require `lost == {}`: the result is written before any client stops (#84), so even `stopped` is a loss; moving `writeResult` after the stop now fails the probe (`FAIL light: none lost while held: {"stopped": 3000}`, mutant run). (3) **left:** the light `gave-up` path (10 attempts × 0.5–1.5s) is four lines whose outcome is visible in `failed`; a test needs ~10s or a test-only knob. **Fixed 2026-10-14:** `TestLightGivesUpAfterItsAttempts` runs a light client against a closed port under `testing/synctest` (fake clock for the waits, real refused dials, no knob): 10 `dial-refused`, one `gave-up`, never online, 4.5–13.5s of waits; three mutants caught (gives up one attempt early, `gave-up` not counted, no wait). Tier 2 runs 14–16 had already exercised it (e.g. 503,815 online + ~96k `gave-up` ≈ 600k). Also fixed `Config.Sources`'s comment (one per 5,000, since `214cdfc`). (4) **fixed:** `Kill` checks the pid with signal 0 before taking the mark, so a stale pid changes nothing (409, no mark, the gateway stays in the balancer); a SIGKILL that still fails after the mark (the process ended in between) is `ErrKillAfterMark`, answered 500, and the mark stands. `TestControlAPI` checks the stale pid; with the check removed it fails. (5) **fixed:** `stack_ports` includes `STACK_PORT_BASE + 5`, so `stack_up` (and the scenarios' upfront `stack_busy_ports`) refuses a leftover loadgen that would answer `lg_start`'s `/stats`; `stack_test` checks it. Verified: `go test -race ./internal/loadgen/ ./cmd/loadgen/`, `make test-scripts`, `make test-load-probe` (light login p99 96ms, Pong p99 15ms; SDK population 1,000, p99 19.9s).
- **6a2b interface review (2026-10-10, Claude at the user's request):** P6a2bI-1..5, all notes for later plans; no code change beyond the minors. **P6a2bI-1 (6b):** `loadgen.sh` drives one loadgen at a time (`LG_PID`, `LG_NAME`, one control port). The floods need a flood generator beside a legitimate population, and whole-population reconnect checks heartbeats of reconnected clients beside the storm, so 6b makes `loadgen.sh` instance-named (per-name pid, result and control port `STACK_PORT_BASE + 5 + k`, `stack_ports` extended to the ports it uses) and keeps one mode per process: profiles and `rss_mb` stay per role, and `Runner` stays single-mode. Each instance needs its own `-uid-prefix`. **P6a2bI-2 (6b):** what the spec's 6b checks need that loadgen lacks: a count of SDK clients that reach exponential backoff (whole-population reconnect, and Tier 3's < 1%), from `EventWaiting` with `Wait.Kind == WaitBackoff`/`WaitFailure`, in `Counts`; `POST /reset` (close every SDK client's connection with `SO_LINGER 0` in one instant, through the `Dial` wrapper, with a `gw=all` mark) and `POST /network-change` (`sdk.Client.NetworkChanged` on every client); `POST /up?gateway=` with `Balancer.SetUp` for a restarted gateway (P6a2aI-1). SDK mode records no Pong RTT; heartbeats "answered on time" come from a light instance beside it (P6a2bI-1). **P6a2bI-3 (6b):** a phase boundary on a shared stack: `lg_stop` SIGKILLs loadgen after 60s (the result is already written), and the kernel then closes its sockets, so the gateways' logouts would spill into the next phase. 6b starts a phase only when every gateway's `connections` gauge is back to 0 (or restarts the stack per phase), and measures how long stopping 200k SDK clients takes in run (a). **P6a2bI-4 (6d):** `/kill` signals a local pid, so it is Tier 2 only. Tier 3 takes `POST /mark?gateway=gw-k&mark=death` and then deletes the pod (`--grace-period=0 --force`) or kills the process in the container; T_death is then the mark, slightly before the kill, so the SLO is judged conservatively. Population attribution behind a real LB stays as P6a2aI-2. **P6a2bI-5:** `cmd/loadgen`'s `-within` judges every mark in the result against one bound; a scenario with marks under different bounds (b1 from the mark, b2 from detection) reads `/slo?mark=…&within=…` per mark before `lg_stop`, as `smoke.sh` does, and the result's `slos` are a record, not the verdict.
- **Interface review (2026-10-09, decided by Claude as recommended, at the user's request):** P6a2aI-1..4. **P6a2aI-1:** `(*Balancer).Down(gw) bool` under the lock, so 6a2b's `TestControlAPI` no longer reads the unexported `b.down` unlocked (a race with a concurrent dial); `TestBalancerSkipsDownGateways` checks it (a `Down` that always answers false fails it). `SetDown` stays permanent; the first scenario that restarts a gateway (6b) adds `SetUp`. **P6a2aI-2:** a mark's population relies on loadgen's balancer knowing each client's gateway; `LoginResp` names none, so behind a real LB (Tier 3) it cannot. `Mark`'s doc now says gw -1 is only for an event that ends every client's connection (a member never lost is never back and fails). 6d decides Tier 3 attribution; the lean is an SLO population of the members that lost their connection within the window, not a gateway id in the protocol for tests only. **P6a2aI-3:** the admin address defaults to `:9090` (every interface) and now serves pprof, and pod IPs are reachable cluster-wide, so #85's 6d note is amended: a NetworkPolicy admits the admin port only from the monitoring namespace (roadmap #85 amended). `-admin-addr` help text in `cmd/gateway` and `cmd/logic` now lists `/debug/pprof/`. **P6a2aI-4:** the `Recorder` comment said events arrive at the login rate; Pongs arrive at clients ÷ `PingEvery` (60k/s at 600k every 10s). One mutex still serves that, and callers pass the times in so a lock wait never shows in a sample; comment fixed, and 6b checks loadgen's CPU profile in run (a) before trusting the sizing.
- **Plan review (2026-10-09, Claude at the user's request; choices #80–#86 decided):** #80, #81, #82, #85 as proposed; three amendments and one fix, each made in the prototype (`proto-6a2` commit `2a09af8`), tested, and copied back into both plans byte for byte. (1) **#83:** the histogram is exact to 1ms up to 60s (was 10s): above 10s the bucket's lower bound could pass a p99 up to 99ms over the 30s bound; 763KB per histogram; `TestHistCoarseAboveAMinute` replaces `…AboveTenSeconds` and catches the old 10s resolution. (2) **#84:** `Kill` and `Mark` take the mark first and only then remove the gateway from the balancer and send SIGKILL, so a taken mark name (409) changes nothing (before, the gateway left the balancer and was never killed); `TestControlAPI` checks it, and the old order fails it. (3) **#84:** `cmd/loadgen` runs the runner on its own context, cancelled after the result is written: the SIGTERM no longer starts stopping clients while the result is being taken. (4) **#86:** `smoke.sh` records `rss_mb` of loadgen and each gateway in both phases, so 6b sizes loadgen from measured bytes per connection. Also a gofmt fix in `light_test.go`. Re-verified: `go test -race ./internal/loadgen/ ./cmd/loadgen/`, `make test`, `make test-load-probe` (light login p99 128ms, Pong p99 19ms; SDK population 1,000, from `T_death` p99 19.8s) and the smoke dry run (9/10, `no_swap_in_light` failing on the dev machine's own swap as before; `rss_mb` light: loadgen 76MB at 4,000 connections, gateways 22–24MB). Not changed, noted for 6b: `Balancer.SetDown` is permanent (a restarted gateway needs a `SetUp` for rolling-restart scenarios); the pinger writes to every connection from one goroutine with no write deadline, so a gateway that stops reading (SIGSTOP) could stall Pings to the others once its socket buffer fills, which at one Ping per 10–150s takes far longer than any run; roadmap #79's "handshake concurrency is 1 per gateway" is corrected to 2 (`(loops+1)/2` with 3 loops), as Task 6 Step 5 already says.

- **Prototype (2026-10-09, while writing the plans):** every block of 6a2a and 6a2b was written and run in a scratch worktree of `80bbffa` (branch `proto-6a2`), then copied into the plans by a render script (new files whole, modified files as `diff -u` against `80bbffa`; `stack.sh`'s `stack_build` line and the `Makefile` split between Tasks 1, 5 and 6 by an intermediate copy). Results on the dev machine: `go test -race -count=1 ./...` green; `make test-scripts` green (`lib_test`, `loadgen_test`, `stack_test`); `make test-load-probe` passed twice (~50s): light 3,000 online, every Ping answered, login p99 ~205ms, Pong p99 ~45ms; SDK 2,000 online, gw-1 killed through `/kill`, population 1,000, from `T_death` p50 ~10s, p99 19.8s, all back on gw-0, only the expected kill in `stack_health`. `smoke.sh` dry run as in 6a2b Task 6 Step 2 (4,000 light, 2,000 SDK): 9 of 10 checks passed, `no_swap_in_light` failed on the dev machine's own swapping (5 pages in, 1GB available); `result.json` valid; SLO population 500, p99 19.7s. Mutants caught: gateway and Logic pprof unregistered; `IP_BIND_ADDRESS_NO_PORT` 0; most-connections; failed dial keeping its slot; mark ignoring the gateway; back without a loss (after adding `TestRecorderBackNeedsALoss`); `PassFromMark` always true; `/kill` keeping the gateway in the balancer; no login retry; missing Pongs not counted; `stack_health` without `expected:`. Equivalent, not run: `rank`'s early -1 return.
- **Found while prototyping:** (1) under `-race` the in-process gateways (2 loops, handshake concurrency 1) closed ~1 in 3 light handshakes before the reply at 400/s, so light clients now retry before login (#80); the same limit will shape 6b's ramp rate (6a2b Task 6 Step 5). (2) A test "gateway" that closes before reading makes the client's handshake **write** fail (`handshake`), not the reply read (`pre-handshake`); the fakes read one byte first, as the gateway's limiter does. A zero Noise public key fails the handshake before any byte is sent. (3) `stack_expect_dead` and `lg_stop` must not run in `$(...)` (a subshell cannot reap the parent's child); `lg_stop` sets `LG_STATUS` instead of printing it.
