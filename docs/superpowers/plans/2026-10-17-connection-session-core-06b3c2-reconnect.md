# Plan 6b3c2: Whole-Population Reconnect Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the spec's last Tier 2 population test, the whole-population reconnect (R5S-O6), as scenario `reconnect` and Tier 2 run 31, for all three causes the spec names: an LB reset (`reset`), every client raising a network change (`netchange`), and a carrier's or region's network that drops and recovers while the gateways still hold the old connections (`ghost`, review O1).

**Architecture:** Two parts, one task each, then the run:
- **Loadgen `POST /reset`, `POST /network-change`, `POST /ghost` and `GET /clients`** (Task 1, P6a2bI-2, #134, review O1, O4). `/reset` marks every gateway (`gw=all`) and then closes every SDK client's newest connection with `SO_LINGER 0`: each gateway gets an RST, each client loses its connection and waits its 0–20s storm window. `/network-change` calls `sdk.Client.NetworkChanged` on every client. `/ghost` marks every gateway and then makes every client's connection fail to the SDK (`EndLost`) while loadgen keeps the socket open and unread, so each gateway still holds it ONLINE; a keeper waits in its own epoll set for each gateway's close (a Kick or the heartbeat sweep), then drains, closes and counts it (`ghosts_open`, `ghosts_closed` per gateway, `kick`/`silent`). `/clients` gives each client's gateway, for the session check. All but `/clients` are sdk mode only. Loadgen also counts every SDK wait by kind since the latest mark (`storm_since_mark.waits`) and reports its balancer's open connections per gateway (`balancer_open`).
- **Scenario `reconnect`** (Task 2). Phases `reset`, `netchange` and `ghost`, each on a fresh stack: 600,000 SDK clients over 3 gateways, as gwloss b_stress runs them (`-mode sdk -precompute`, b_stress's CPU sets, 4 GOMAXPROCS per gateway), the spec's 250k cap and a 30s heartbeat (#136.3, #136.4). Then `/reset` (`netchange`: and `/network-change` once every client has lost its connection; `ghost`: `/ghost`, then `/network-change`, and a hold until `DeadAfter` + 5s after the mark). It measures the time until 99% are back and the share that reached backoff, with no SLO, and checks that every client comes back, that the old connections all closed (`ghost`), that the clients already back lose no connection to a Pong timeout (a labelled fail when loadgen was the bound, O3), that Logic's store holds one session per client on the right gateway (O4), and that no component fails. A per-second series per gateway records #133's readings, the closes by Kick and by heartbeat, loadgen's kept sockets and run-queue wait, and a funnel reading with a bar set before the run (O6), not gated. Redis's per-command latency is recorded per phase (O5). `host-setup.sh` raises the nofile hard limit to 2,097,152 for `ghost` (#136.5).
- **Run 31** (Task 3): the user runs `reconnect` on the Tier 2 host, after `host-setup.sh` and a new login.

**Tech Stack:** Go 1.27.1 (`internal/loadgen`, `cmd/loadgen`, `golang.org/x/sys/unix` epoll), bash 5.2, python3 (JSON and arithmetic only), `taskset`, `/proc/stat`, `redis-cli`.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Success criteria* (~l.28): whole-population reconnect (LB failover or reset, a regional or carrier network recovering) is best effort, no SLO; recovery ≈ users ÷ admitted login rate plus the backoff tail; admission control and the connection cap shape it; no component may fail or fall over.
- *Load tests* (~l.548): drop every connection on all gateways at once (RST from loadgen in one instant); measured: the time until 99% are logged in again and the share of clients reaching backoff; heartbeats of already-reconnected clients answered on time; no component restarts. The same with every client raising a network-change event at once.
- *Client SDK rules*, *Local trigger* and *Precedence* (~l.128–129): a network change cuts only the 0–20s storm wait, to 0–1s.
- *Storms, drain, failures*, row *Whole-population reconnect* (~l.428). *Dead-client detection*: an ONLINE connection with no frame is dead after `DeadAfter` = 2 × heartbeat + 10s.

Inputs:
- Plan 6b3c1 (executed first): Task 1's helpers in `lib.sh`.
- The roadmap's #133 (series, funnel, not gated), #134 (600k SDK clients, `/reset`), and #136 (the rulings, below).
- The review: `docs/superpowers/objections/connection-session-core-06b3c-reconnect.md` and `-dispositions.md` (O1–O6 and O8 apply here; O7 to 6b3c1).
- P6a2bI-2 and P6a2bI-3 (the 6a2a plan's *Execution Notes*, ~l.1539): `/reset`, `/network-change`, the backoff count (already in loadgen: `backoff_since_mark`), a phase boundary on a fresh stack.
- gwloss b_stress's 30j rerun (6b3b4 *Execution Notes*, ~l.919): 600,000 back in 127s from one gateway's SIGKILL, loadgen-bound, `pending_mem` 0, `login_queue` 17,713 + 16,898. Run 31 is the first with every gateway's clients reconnecting at once.

**Decisions:**
- #133, #134 (the user, 2026-10-17, before writing).
- **#136 (the user, 2026-10-17, amended by the review 2026-10-17 and 2026-10-18):** (1) one plan, amended: split into 6b3c1 and this plan for size; (2) is 6b3c1's; (3) `reconnect`'s gateways send a 30s heartbeat (not the 150s default), so the clients back send Pings during the storm; (4) the spec's 250k cap (not b_stress's 650k): in the RST phases it does not bind (200k per gateway, every slot freed at once; `at_capacity` expected 0); in `ghost` it binds (the old connections plus the relogins), and `at_capacity` is recorded, not gated (O2); (5) a third phase, `ghost` (O1): loadgen's silent drop, then a network change; loadgen holds up to 1.2M sockets, so the host's nofile hard limit goes to 2,097,152 (`fs.nr_open`, `limits.d`, `user@.service`; the user chose this over a smaller `ghost` population, 2026-10-18).
- Review O3–O6, O8 (accepted): a labelled `P_heartbeats` fail and `lg_runq_ms_per_s`; `P_sessions`; Redis latency per phase; the funnel's bar; the sampler on loadgen's set.

**Branch:** `feat/plan6b3`. Execute from its head, after Plan 6b3c1 is executed.

**Size:** 3 tasks. Task 1 adds ~450 lines to `internal/loadgen` (a new `ghost.go`, 236 lines) and two test files (`reset_test.go` 139 lines, `ghost_test.go` 159). Task 2 adds `reconnect.sh` (~590 lines) and its 63-line test, and changes `host-setup.sh`, `sysctl.conf`, `lib.sh`'s `t2_nofile` and their test. Task 3 is the user's run and the record. The prototype `proto-6b3c` (Plan 6b3c1's tasks `f2546aa`, `4658e13`, then this plan's `d1d4c06`, `fd1dc04`) ran every step. Its diff is replayed here byte for byte: the changed files as `git apply` patches, one per task, the new files inside them. On the dev machine (AMD EPYC 7742, 2 cores):
- `make test` (race), `go vet ./...` and `make test-scripts` green.
- Mutations: 11 caught in Task 1; the series' and the session check's cases by the script tests.
- `reconnect` dry run (3,000 clients, ~4 min): 34 checks, only the three dev swap checks failing. `reset`: all back in 20s (p99 19.9s, the 0–20s storm wait). `netchange`: the network change sent 338ms after the mark, 2,957 of 3,000 waits cut to local, p99 1.6s. `ghost`: the network change at 228ms, p99 1.7s; every old connection held by its gateway until a relogin's Kick closed it (3,000 `kick`, 0 `silent`, `closes_client` 0); `P_sessions` 3,000 of 3,000 found. The sampler's rows came every ~1.1–1.4s.

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- No change to the gateway, Logic, the SDK or the wire. Go changes only in `internal/loadgen` and `cmd/loadgen`'s doc comment.
- Loadgen's existing endpoints, flags and result fields keep their meaning. New: `POST /reset`, `POST /network-change`, `POST /ghost`, `GET /clients`, `Stats.balancer_open`, `ghosts_open`, `ghosts_closed`, `storm_since_mark.waits`.
- No check of `floods`, `gwloss` or `slowlogic` changes. `t2_nofile`'s refusal names the new remedy; `host-setup.sh` installs two more files and `fs.nr_open`, so `t2_require_host_setup` refuses until the user runs it again.
- `reconnect` gates no SLO (the spec has none): it gates only that every client comes back within `RECONNECT_BACK_WAIT`, the drop reached everyone (`ghost`: and every old connection closed), no Pong timeout (`ghost`: gateway heartbeat closes up to the silent ghosts), one session per client on its gateway, lease renewals, idle, processes, swap and artifacts. `pending_mem`, the funnel, `at_capacity`, Kicks, Redis latency, backoff and recovery times are recorded (#133).

## Review Focus

1. **"One instant" is 600k closes.** `/reset` takes the mark, then 64 workers close every connection (`eachSDK`). In the dry run 3,000 took ~130ms; on the host 600k may take seconds, each close also running the gateway's side of the RST on loopback (the 30j profile: closes were 15% of loadgen's storm). A client closed late starts its storm wait late, so its relogin time from the mark includes it. `P_drop` bounds every client's detection by `RECONNECT_DROP_MAX_S` (10s) and records `close_ms` and the detection distribution beside the recovery times. `/ghost` is the same with a deadline instead of a close.
2. **A network change to an online client does nothing.** `sdk.Client.local` acts only in a wait or a failing attempt. So `netchange` and `ghost` post `/network-change` only once every client has lost its connection (`slo.lost == population`), and a client whose 0–20s wait ended before that keeps its attempt: `waits.local` counts the waits cut, `network_change_after_ms` the delay.
3. **The logout queue is the cap's size.** At the reset each gateway queues ~200k Logouts at once into a 250k queue (`-logout-queue` follows `-max-conns`), while the relogins replace those sessions, and again at the stop. A full queue drops Logouts (reconciled later by design), so `P_idle` allows dropped ones (`stack_wait_idle … dropped-ok`) and records them per gateway, and the series has `logouts_dropped_per_s`. `P_sessions` (O4) reads the end state that a Logout racing a relogin could break; in `ghost` the race is the other way round (the relogin first, then the Kick's Logout: `logouts_not_owner`).
4. **12 gateway threads on 8 CPUs.** As #133 sized it (2 handshake slots each), every gateway has 4 GOMAXPROCS on b_stress's 8-CPU gateway set, and all three handshake at once (in b_stress only two did, in the storm). The gateway set may saturate before Logic does; the series' `gateway_set_busy_pct` beside `pending_mem_per_s` and `busy_per_s` says which came first. `RECONNECT_GATEWAY_PROCS` changes it for a rerun. The sampler runs on loadgen's 14 CPUs, so `logic_set_busy_pct` holds only Logic and Redis (O8).
5. **The heartbeat check is by absence.** SDK clients record no Pong RTT, only a Pong timeout (10s) as `lost["pong-timeout"]`. `P_heartbeats` passes only if every connection that ended after the mark was the drop's. With a 30s heartbeat and a hold of the heartbeat + 15s, every client back sends at least one Ping after its relogin. In a loadgen-bound storm a late read on loadgen's side could also cause a Pong timeout: the check still fails, but labelled (a `not_covered` line, as floods' verdict, #126, #127), with `loadgen_bound` and `lg_runq_ms_per_s_max` in its detail and loadgen's run-queue wait per second in the series (O3).
6. **`ghost`'s peak and its hold.** Each gateway holds ~200k old connections ONLINE until `DeadAfter` (70s at the 30s heartbeat): with the relogins that is ~400k against the 250k cap, so `at_capacity` refuses until Kicks free slots, and every admitted relogin Kicks its old connection (600k Kicks, the cost behind parked #117). Loadgen holds each client's old socket and its new one: up to 1.2M fds (the bound is 2 × clients), over the host's former 1,048,576, so `ghost` needs `t2_nofile 2097152` and the new hard limit. Kernel memory: ~8.5KB per loopback socket pair with unread data (measured on dev); the cap keeps old plus new at 750k pairs on the gateways, ~150k more than the RST phases, plus the dials in flight: ~2–4GB over the 23GiB budget on the 26GB host. The budget stays 23 and `no_swap_in_ghost` is the verdict. The hold lasts until `DeadAfter` + 5s after the mark (one wheel tick and loadgen's closes), so every old connection is gone before `P_drop` reads `ghosts_open`.
7. **The session check after the hold.** `P_sessions` reads every client's `sess:{uid}` hash in EVALs of 500 keys (~1,200 short scripts on the one Redis) after the hold, after Redis's latency is read and before loadgen stops, so it does not load the storm or the latency reading. A session counts when it is the hash's only field, on the gateway loadgen says holds the client, with `login_ms` at or after the mark.

## Implementation choices this plan makes

1. **The reset closes from loadgen's side.** Each SDK client keeps its newest connection (`sdkClient.conn`, set in its `Dial` wrapper), and `replyConn.abort` sets `SO_LINGER 0` on the TCP socket and closes it. The client's read fails, so the SDK ends the connection as `EndLost` and waits its storm window, as after an RST from the network. The mark comes first, so the population is every client online at the mark.
2. **The ghost drop is a state on `replyConn`** (`connOpen`, `connGhost`, `connClosed`): `ghost` sets it and a past read deadline (a Read in progress returns), Read and Write fail with `errGhost` at once, and the SDK's later `Close` hands the socket to the keeper instead of closing it. The keeper registers it in its own epoll set with `EPOLLRDHUP` only (no goroutine per socket, no read while the gateway is silent), and on the gateway's close 4 workers drain it (data first = `kick`), close it (a FIN: nothing left unread) and free its balancer slot: a real LB also keeps the flow until then. `goim-netdrop` is not used: it drops by port, so it would also block the relogins.
3. **`eachSDK`** runs a function on every started SDK client with 64 workers, as `Run`'s stop does: `NetworkChanged` waits for the client's run loop.
4. **Waits since the mark by kind** (`StormCount.Waits`), from every `EventWaiting`, so `netchange` and `ghost` show how many storm waits they cut (`local`). The existing backoff counts stay as they are.
5. **`reconnect` has its own sampler and series** (`rc_sampler`, `rc_series`), per gateway. `floods`' `slow_sampler`/`slow_series` sum the gateways, and the funnel needs each gateway apart (6b3b3's *After this plan*: generalize only if needed; the two shapes differ). Both use the moved `t2_set_ticks`.
6. **The funnel reading** (#133): a row where one gateway refused at `pending_mem` while another, under the cap, had no socket in HANDSHAKE. `rc_series` sums those rows' seconds and gives the first, the longest run, `over_bar` and the first 20. **The bar is 5s in a row** (`RECONNECT_FUNNEL_BAR_S`, O6): the sampler's rows come every ~1.1–1.4s (dry run), so 5s is about 4 rows in a row, not one sampled second. Only a funnel at or over the bar is the evidence for unparking 6b3b5 and #111; one under it is recorded as "seen, under the bar". The row pairs a gauge at one instant (`handshake == 0`) with a rate over the interval (`pending_mem_per_s`); that caveat is recorded, not changed (O6's second half).
7. **The session check reads all clients**, not a sample (O4): one `GET /clients` (each client's gateway index) and one redis-cli fed every EVAL, compared in python (`rc_sessions_cmp`, tested on synthetic files).
8. **A fresh stack per phase** (P6a2bI-3): `netchange` does not start on `reset`'s Logouts, nor `ghost` on either's.

## File Structure

- `internal/loadgen/runner.go`, `sdkclient.go`, `recorder.go`, `control.go`, new `ghost.go`, new `reset_test.go`, new `ghost_test.go`, `flood_test.go` (`metric` reads gauges too); `cmd/loadgen/main.go` (doc) (Task 1).
- `scripts/tier2/reconnect.sh` (new), `scripts/tier2/reconnect_test.sh` (new), `Makefile`; `scripts/tier2/host-setup.sh`, `scripts/tier2/sysctl.conf`, `scripts/tier2/lib.sh` (`t2_nofile`), `scripts/tier2/lib_test.sh` (Task 2).
- This plan's *Execution Notes*, the roadmap, the README (only if the user asks) (Task 3).

---

### Task 1: Loadgen `POST /reset`, `POST /network-change`, `POST /ghost` and `GET /clients`

**Files:**
- Modify: `internal/loadgen/runner.go`, `internal/loadgen/sdkclient.go`, `internal/loadgen/recorder.go`, `internal/loadgen/control.go`, `internal/loadgen/flood_test.go`, `cmd/loadgen/main.go`
- Create: `internal/loadgen/ghost.go`, `internal/loadgen/reset_test.go`, `internal/loadgen/ghost_test.go`

**Interfaces:**
- Produces: `(*Runner).Reset(name string) (n int, took time.Duration, err error)`; `(*Runner).NetworkChange() (n int, took time.Duration, err error)`; `(*Runner).Ghost(name string) (n int, took time.Duration, err error)`; `ErrNotSDK`; `POST /reset?mark=NAME` → `{"mark", "population", "close_ms"}` and `POST /ghost?mark=NAME` → `{"mark", "population", "ms"}` (400 without a mark, 409 outside sdk mode or with a taken mark, nothing changed); `POST /network-change` → `{"clients", "ms"}` (409 outside sdk mode); `GET /clients` → `{"uid_prefix", "gateways", "gw": [index, -1 offline]}`; `Stats.BalancerOpen` (`balancer_open`: `{"gw-k": open}`); `Stats.GhostsOpen` (`ghosts_open`), `Stats.GhostsClosed` (`ghosts_closed`: `{"gw-k": {"kick", "silent"}}`); `StormCount.Waits` (`storm_since_mark.waits`: `{"storm": n, "local": n, ...}`, every SDK wait by kind since the latest mark); `(*Recorder).Wait(kind string)`, `(*Recorder).Gateways() []int32`.
- Consumes: `(*Runner).Mark`, `Balancer.Open`, `sdk.Client.NetworkChanged`, `sdk.WaitKind.String`, `unix.EpollCreate1`/`EpollCtl`/`EpollWait`.

- [ ] **Step 1: The patch**

The tests are `reset_test.go` (new, in the patch): `TestResetDropsEveryConnection`, `TestAbortSendsAnRST`, `TestNetworkChangeCutsTheStormWait`, `TestResetAndNetworkChangeNeedSDKMode` (also `/ghost`); and `ghost_test.go` (new): `TestGhostKeepsTheGatewaysConnections`, `TestGhostKeeperWaitsForThePeer`, `TestClientsGivesEachGateway`. `flood_test.go`'s `metric` now reads a gauge too (`goim_gateway_connections`).

```diff
diff --git a/cmd/loadgen/main.go b/cmd/loadgen/main.go
index 6b6f8f5..bdcab51 100644
--- a/cmd/loadgen/main.go
+++ b/cmd/loadgen/main.go
@@ -2,7 +2,9 @@
 // scripts/stack.sh's stack.env, starts -clients at -rate through a
 // least-connections balancer from 127.0.0.2… (choices #69, #81), holds them,
 // and serves the control API on -control (GET /stats, POST /kill, POST /mark,
-// POST /up, GET /slo, GET /runtime; choice #84) until SIGTERM, Ctrl-C or -duration.
+// POST /up, GET /slo, GET /runtime, GET /clients; choice #84; sdk mode also
+// POST /reset, POST /network-change and POST /ghost, Plan 6b3c) until
+// SIGTERM, Ctrl-C or -duration.
 // -down keeps gateways out of the balancer until POST /up (#93). The flood
 // modes (garbage, slow-handshake, slow-auth; Plan 6b3a) log nothing in: they
 // put a handshake flood or a slow-client flood beside another instance's
diff --git a/internal/loadgen/control.go b/internal/loadgen/control.go
index a126b04..587be17 100644
--- a/internal/loadgen/control.go
+++ b/internal/loadgen/control.go
@@ -34,6 +34,13 @@ func (r *Runner) Result(within time.Duration) Result {
 //	POST /mark?gateway=gw-1|all&mark=drop     a mark; &down=1 also takes the gateway out of the balancer
 //	POST /up?gateway=gw-1                     the gateway is back in the balancer  {"gateway"}
 //	                                          also after /kill: send it once the gateway listens again
+//	POST /reset?mark=reset                    sdk: mark every gateway, then RST every connection  {"mark", "population", "close_ms"}
+//	                                          409: not sdk mode or the mark taken, nothing changed
+//	POST /network-change                      sdk: NetworkChanged on every client  {"clients", "ms"}; 409: not sdk mode
+//	POST /ghost?mark=ghost                    sdk: mark every gateway, then every connection lost to its client
+//	                                          and kept open, unread, until its gateway closes it  {"mark", "population", "ms"}
+//	                                          409: not sdk mode or the mark taken, nothing changed
+//	GET  /clients                             {"uid_prefix", "gateways", "gw": [client i's gateway index, -1: offline]}
 //	GET  /slo?mark=death&within=30s           SLO
 //	GET  /runtime                             Runtime: loadgen's GC, memory and scheduling counters
 //	/debug/pprof/                             loadgen's own profiles
@@ -83,6 +90,41 @@ func (r *Runner) Handler() http.Handler {
 		r.Up(gw)
 		writeJSON(w, map[string]any{"gateway": q.FormValue("gateway")})
 	})
+	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, q *http.Request) {
+		if q.FormValue("mark") == "" {
+			http.Error(w, "want mark=<name>", http.StatusBadRequest)
+			return
+		}
+		n, took, err := r.Reset(q.FormValue("mark"))
+		if err != nil {
+			http.Error(w, err.Error(), http.StatusConflict)
+			return
+		}
+		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n, "close_ms": took.Milliseconds()})
+	})
+	mux.HandleFunc("POST /ghost", func(w http.ResponseWriter, q *http.Request) {
+		if q.FormValue("mark") == "" {
+			http.Error(w, "want mark=<name>", http.StatusBadRequest)
+			return
+		}
+		n, took, err := r.Ghost(q.FormValue("mark"))
+		if err != nil {
+			http.Error(w, err.Error(), http.StatusConflict)
+			return
+		}
+		writeJSON(w, map[string]any{"mark": q.FormValue("mark"), "population": n, "ms": took.Milliseconds()})
+	})
+	mux.HandleFunc("GET /clients", func(w http.ResponseWriter, _ *http.Request) {
+		writeJSON(w, map[string]any{"uid_prefix": r.cfg.UIDPrefix, "gateways": r.env.GatewayIDs, "gw": r.rec.Gateways()})
+	})
+	mux.HandleFunc("POST /network-change", func(w http.ResponseWriter, _ *http.Request) {
+		n, took, err := r.NetworkChange()
+		if err != nil {
+			http.Error(w, err.Error(), http.StatusConflict)
+			return
+		}
+		writeJSON(w, map[string]any{"clients": n, "ms": took.Milliseconds()})
+	})
 	mux.HandleFunc("GET /slo", func(w http.ResponseWriter, q *http.Request) {
 		within, err := time.ParseDuration(q.FormValue("within"))
 		if err != nil || within <= 0 {
diff --git a/internal/loadgen/flood_test.go b/internal/loadgen/flood_test.go
index 126b04a..39a058d 100644
--- a/internal/loadgen/flood_test.go
+++ b/internal/loadgen/flood_test.go
@@ -76,7 +76,8 @@ func TestNewFloodModes(t *testing.T) {
 	}
 }
 
-// metric is the value of name{label=value} in gateway id's registry.
+// metric is the value of name{label=value} in gateway id's registry, a
+// counter or a gauge.
 func metric(t *testing.T, id, name, label, value string) float64 {
 	t.Helper()
 	reg, _ := gatewayRegs.Load(id)
@@ -91,7 +92,7 @@ func metric(t *testing.T, id, name, label, value string) float64 {
 		for _, m := range mf.GetMetric() {
 			for _, l := range m.GetLabel() {
 				if l.GetName() == label && l.GetValue() == value {
-					return m.GetCounter().GetValue()
+					return m.GetCounter().GetValue() + m.GetGauge().GetValue()
 				}
 			}
 		}
diff --git a/internal/loadgen/ghost.go b/internal/loadgen/ghost.go
new file mode 100644
index 0000000..d6e284c
--- /dev/null
+++ b/internal/loadgen/ghost.go
@@ -0,0 +1,236 @@
+package loadgen
+
+import (
+	"errors"
+	"net"
+	"sync"
+	"sync/atomic"
+	"time"
+
+	"golang.org/x/sys/unix"
+)
+
+// errGhost fails a ghosted connection's reads and writes: the SDK sees a
+// lost connection, and nothing it writes reaches the gateway (Ghost).
+var errGhost = errors.New("loadgen: ghost: the network dropped the connection")
+
+// ghosts keeps the sockets of POST /ghost open and unread, so their
+// gateways see only silence, as when a carrier's network drops every flow
+// (Plan 6b3c, review O1). A gateway closes each one when a relogin's Kick
+// reaches it or its heartbeat sweep finds it dead (DeadAfter). The keeper
+// waits for that close in its own epoll set (EPOLLRDHUP, no goroutine per
+// socket), then drains and closes the socket, so none stays in CLOSE_WAIT,
+// and counts the close per gateway: "kick" when a frame came first (the
+// Kick), "silent" when none did (the sweep). A ghost keeps its balancer slot
+// until then: the LB still holds the flow.
+type ghosts struct {
+	once   sync.Once
+	ep     int
+	err    error
+	mu     sync.Mutex
+	byFD   map[int32]*replyConn
+	work   chan *replyConn
+	done   chan struct{}
+	open   atomic.Int64
+	kick   []atomic.Int64 // per gateway
+	silent []atomic.Int64 // per gateway
+}
+
+// GhostCloses is one gateway's ghosts closed since the run started.
+type GhostCloses struct {
+	Kick   int64 `json:"kick"`
+	Silent int64 `json:"silent"`
+}
+
+func newGhosts(gateways int) *ghosts {
+	return &ghosts{byFD: map[int32]*replyConn{}, done: make(chan struct{}),
+		kick: make([]atomic.Int64, gateways), silent: make([]atomic.Int64, gateways)}
+}
+
+// start opens the epoll set and its loop on first use.
+func (g *ghosts) start() error {
+	g.once.Do(func() {
+		if g.ep, g.err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); g.err != nil {
+			return
+		}
+		g.work = make(chan *replyConn, 4096)
+		for range 4 {
+			go func() {
+				for c := range g.work {
+					g.close(c)
+				}
+			}()
+		}
+		go g.loop()
+	})
+	return g.err
+}
+
+// keep takes c's socket: it stays open, unread, until its gateway closes it.
+func (g *ghosts) keep(c *replyConn) {
+	tc := c.tcp()
+	if tc == nil || g.start() != nil {
+		c.Conn.Close()
+		return
+	}
+	rc, err := tc.SyscallConn()
+	if err != nil {
+		c.Conn.Close()
+		return
+	}
+	g.open.Add(1)
+	var added bool
+	rc.Control(func(fd uintptr) {
+		g.mu.Lock()
+		defer g.mu.Unlock()
+		g.byFD[int32(fd)] = c
+		ev := unix.EpollEvent{Events: unix.EPOLLRDHUP, Fd: int32(fd)}
+		if unix.EpollCtl(g.ep, unix.EPOLL_CTL_ADD, int(fd), &ev) == nil {
+			added = true
+		} else {
+			delete(g.byFD, int32(fd))
+		}
+	})
+	if !added {
+		g.close(c)
+	}
+}
+
+// loop hands every socket its gateway closed (or reset) to a closer.
+func (g *ghosts) loop() {
+	evs := make([]unix.EpollEvent, 256)
+	for {
+		n, err := unix.EpollWait(g.ep, evs, 1000)
+		select {
+		case <-g.done:
+			return
+		default:
+		}
+		if err != nil {
+			if errors.Is(err, unix.EINTR) {
+				continue
+			}
+			return
+		}
+		for _, ev := range evs[:n] {
+			g.mu.Lock()
+			c := g.byFD[ev.Fd]
+			delete(g.byFD, ev.Fd)
+			if c != nil {
+				unix.EpollCtl(g.ep, unix.EPOLL_CTL_DEL, int(ev.Fd), nil)
+			}
+			g.mu.Unlock()
+			if c != nil {
+				g.work <- c
+			}
+		}
+	}
+}
+
+// close drains what the gateway sent, then closes c's socket (a FIN, as
+// nothing is left unread) and frees its balancer slot.
+func (g *ghosts) close(c *replyConn) {
+	kicked := false
+	if tc := c.tcp(); tc != nil {
+		tc.SetReadDeadline(time.Now().Add(time.Second))
+		var buf [512]byte
+		for {
+			n, err := tc.Read(buf[:])
+			kicked = kicked || n > 0
+			if err != nil {
+				break
+			}
+		}
+	}
+	c.Conn.Close()
+	g.open.Add(-1)
+	if c.gw >= 0 && c.gw < len(g.kick) {
+		if kicked {
+			g.kick[c.gw].Add(1)
+		} else {
+			g.silent[c.gw].Add(1)
+		}
+	}
+}
+
+// stop closes every ghost still kept, at the run's end.
+func (g *ghosts) stop() {
+	g.mu.Lock()
+	left := g.byFD
+	g.byFD = map[int32]*replyConn{}
+	g.mu.Unlock()
+	for _, c := range left {
+		c.Conn.Close()
+		g.open.Add(-1)
+	}
+	close(g.done)
+}
+
+// tcp is c's TCP socket under the balancer's wrapper.
+func (c *replyConn) tcp() *net.TCPConn {
+	if lc, ok := c.Conn.(*conn); ok {
+		if tc, ok := lc.Conn.(*net.TCPConn); ok {
+			return tc
+		}
+	}
+	return nil
+}
+
+// Connection states for Ghost: a connection is ghosted only while open, and
+// a ghosted one's Close hands the socket to the keeper.
+const (
+	connOpen int32 = iota
+	connGhost
+	connClosed
+)
+
+// ghost makes c's reads and writes fail at once while its socket stays
+// open: Read and Write check the state, and the past deadline wakes a Read
+// in progress. False if c was already closed.
+func (c *replyConn) ghost() bool {
+	if !c.state.CompareAndSwap(connOpen, connGhost) {
+		return false
+	}
+	c.Conn.SetReadDeadline(aLongTimeAgo) // a blocked Read returns now
+	return true
+}
+
+var aLongTimeAgo = time.Unix(1, 0)
+
+func (c *replyConn) Close() error {
+	if c.state.CompareAndSwap(connGhost, connClosed) {
+		c.sc.r.ghosts.keep(c)
+		return nil
+	}
+	if c.state.Swap(connClosed) == connClosed {
+		return nil
+	}
+	return c.Conn.Close()
+}
+
+// Ghost is POST /ghost (Plan 6b3c, review O1): mark name on every gateway,
+// its population every client logged in now, then every SDK client's
+// newest connection is lost to the client (its read fails: EndLost, and its
+// storm wait) while its socket stays open and unread, so each gateway still
+// holds it ONLINE: a carrier's or a region's network that drops and then
+// recovers. The relogins then meet the old connections on the gateways, the
+// cap and Logic's Kicks. took is how long that took after the mark. A taken
+// mark name changes nothing.
+func (r *Runner) Ghost(name string) (n int, took time.Duration, err error) {
+	if r.cfg.Mode != "sdk" {
+		return 0, 0, ErrNotSDK
+	}
+	if err := r.ghosts.start(); err != nil {
+		return 0, 0, err
+	}
+	if n, err = r.Mark(-1, name, false); err != nil {
+		return 0, 0, err
+	}
+	begin := time.Now()
+	r.eachSDK(func(sc *sdkClient) {
+		if c := sc.conn.Load(); c != nil {
+			c.ghost()
+		}
+	})
+	return n, time.Since(begin), nil
+}
diff --git a/internal/loadgen/ghost_test.go b/internal/loadgen/ghost_test.go
new file mode 100644
index 0000000..37251b4
--- /dev/null
+++ b/internal/loadgen/ghost_test.go
@@ -0,0 +1,159 @@
+package loadgen
+
+import (
+	"encoding/json"
+	"errors"
+	"net"
+	"net/http"
+	"os"
+	"testing"
+	"time"
+
+	"github.com/Tallinzp/goim/internal/sdk"
+)
+
+// POST /ghost loses every connection to its client while the gateways
+// still hold it ONLINE: no gateway sees a client close. After a network
+// change every client is back, and each relogin's Kick closes its ghost,
+// which loadgen then closes and counts (review O1).
+func TestGhostKeepsTheGatewaysConnections(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 30 * time.Second, Hi: 60 * time.Second})
+	code, m := post(t, srv.URL+"/ghost?mark=ghost")
+	if code != http.StatusOK || m["population"] != 20.0 || m["ms"] == nil {
+		t.Fatalf("/ghost: %d %v", code, m)
+	}
+	waitFor(t, "all lost", 5*time.Second, func() bool { s, _ := r.Recorder().SLO("ghost", time.Second); return s.Lost == 20 })
+	gwSum := func(name, label, value string) float64 {
+		return metric(t, env.GatewayIDs[0], name, label, value) + metric(t, env.GatewayIDs[1], name, label, value)
+	}
+	time.Sleep(200 * time.Millisecond) // a close would reach the gateways by now
+	if st := r.Stats(); st.GhostsOpen != 20 || st.Lost["lost"] != 20 {
+		t.Fatalf("ghosts open %d, lost %v: want 20 kept, 20 lost", st.GhostsOpen, st.Lost)
+	}
+	if c, on := gwSum("goim_gateway_closes_total", "reason", "client"), gwSum("goim_gateway_connections", "state", "online"); c != 0 || on != 20 {
+		t.Fatalf("the gateways saw %v client closes and hold %v online, want 0 and the 20 ghosts", c, on)
+	}
+	if code, _ := post(t, srv.URL+"/network-change"); code != http.StatusOK {
+		t.Fatalf("/network-change: %d", code)
+	}
+	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("ghost", time.Second); return s.Back == 20 })
+	waitFor(t, "every ghost kicked and closed", 10*time.Second, func() bool { return r.Stats().GhostsOpen == 0 })
+	st := r.Stats()
+	var kick, silent int64
+	for _, g := range st.GhostsClosed {
+		kick, silent = kick+g.Kick, silent+g.Silent
+	}
+	if kick != 20 || silent != 0 || gwSum("goim_gateway_closes_total", "reason", "kick") != 20 {
+		t.Fatalf("ghosts closed %v, gateway kicks %v: want 20 by Kick", st.GhostsClosed, gwSum("goim_gateway_closes_total", "reason", "kick"))
+	}
+	if open := st.BalancerOpen[env.GatewayIDs[0]] + st.BalancerOpen[env.GatewayIDs[1]]; open != 20 || st.Lost["lost"] != 20 {
+		t.Fatalf("balancer open %v, lost %v: want the 20 back and no other loss", st.BalancerOpen, st.Lost)
+	}
+	if code, _ := post(t, srv.URL+"/ghost?mark=ghost"); code != http.StatusConflict {
+		t.Fatalf("/ghost with a taken mark: %d", code)
+	}
+}
+
+// A ghosted connection fails its reads and writes at once, nothing it
+// writes reaches the peer, and its socket stays open until the peer closes
+// it; then the keeper closes it, frees its balancer slot and counts the
+// close: "silent" with no data first, "kick" after a frame.
+func TestGhostKeeperWaitsForThePeer(t *testing.T) {
+	ln, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer ln.Close()
+	b, err := NewBalancer([]string{ln.Addr().String()}, 1)
+	if err != nil {
+		t.Fatal(err)
+	}
+	r := &Runner{ghosts: newGhosts(1), rec: NewRecorder(1, []string{"gw-0"})}
+	defer r.ghosts.stop()
+	sc := &sdkClient{r: r}
+	ghost := func() (*replyConn, net.Conn) {
+		nc, gw, err := b.Dial(t.Context())
+		if err != nil {
+			t.Fatal(err)
+		}
+		peer, err := ln.Accept()
+		if err != nil {
+			t.Fatal(err)
+		}
+		rc := sc.track(nc, sc.dials.Add(1))
+		rc.gw = gw
+		if !rc.ghost() {
+			t.Fatal("ghost refused an open connection")
+		}
+		return rc, peer
+	}
+	rc, peer := ghost()
+	var buf [8]byte
+	if _, err := rc.Read(buf[:]); !errors.Is(err, errGhost) {
+		t.Fatalf("ghost read: %v", err)
+	}
+	if _, err := rc.Write([]byte("ping")); !errors.Is(err, errGhost) {
+		t.Fatalf("ghost write: %v", err)
+	}
+	rc.Close() // the SDK ends the connection: the keeper takes the socket
+	peer.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
+	if n, err := peer.Read(buf[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
+		t.Fatalf("peer read %d, %v: want silence (no byte, no close)", n, err)
+	}
+	if r.ghosts.open.Load() != 1 || b.Open()[0] != 1 {
+		t.Fatalf("ghosts open %d, balancer %v: want the socket kept", r.ghosts.open.Load(), b.Open())
+	}
+	peer.Close()
+	waitFor(t, "the silent ghost closed", 5*time.Second, func() bool { return r.ghosts.open.Load() == 0 })
+	if r.ghosts.silent[0].Load() != 1 || b.Open()[0] != 0 {
+		t.Fatalf("silent %d, balancer %v: want 1 silent close, the slot freed", r.ghosts.silent[0].Load(), b.Open())
+	}
+
+	rc, peer = ghost()
+	rc.Close()
+	peer.Write([]byte("kick"))
+	peer.Close()
+	waitFor(t, "the kicked ghost closed", 5*time.Second, func() bool { return r.ghosts.open.Load() == 0 })
+	if r.ghosts.kick[0].Load() != 1 || r.ghosts.silent[0].Load() != 1 {
+		t.Fatalf("kick %d, silent %d: want one each", r.ghosts.kick[0].Load(), r.ghosts.silent[0].Load())
+	}
+
+	rc, _ = ghost() // closed before the ghost: not ghosted
+	rc.state.Store(connClosed)
+	if rc.ghost() {
+		t.Fatal("ghost took a closed connection")
+	}
+}
+
+// GET /clients gives each client's gateway, -1 when offline (review O4).
+func TestClientsGivesEachGateway(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, srv := sdkRun(t, env, 6, sdk.DefaultWindows().Storm)
+	resp, err := http.Get(srv.URL + "/clients")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer resp.Body.Close()
+	var c struct {
+		UIDPrefix string   `json:"uid_prefix"`
+		Gateways  []string `json:"gateways"`
+		GW        []int    `json:"gw"`
+	}
+	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
+		t.Fatal(err)
+	}
+	if c.UIDPrefix != "rst" || len(c.Gateways) != 2 || len(c.GW) != 6 {
+		t.Fatalf("/clients: %+v", c)
+	}
+	n := map[string]int{}
+	for i, g := range c.GW {
+		if g < 0 || g > 1 {
+			t.Fatalf("client %d on gateway %d, want 0 or 1", i, g)
+		}
+		n[c.Gateways[g]]++
+	}
+	if by := r.Stats().OnlineByGateway; n[env.GatewayIDs[0]] != by[env.GatewayIDs[0]] || n[env.GatewayIDs[1]] != by[env.GatewayIDs[1]] {
+		t.Fatalf("/clients per gateway %v, online by gateway %v", n, by)
+	}
+}
diff --git a/internal/loadgen/recorder.go b/internal/loadgen/recorder.go
index eca1422..884730f 100644
--- a/internal/loadgen/recorder.go
+++ b/internal/loadgen/recorder.go
@@ -114,6 +114,18 @@ func (r *Recorder) Lost(i int, why string, now time.Time) {
 	}
 }
 
+// Gateways is the gateway index each client is logged in on now, -1 for
+// none (GET /clients, Plan 6b3c review O4).
+func (r *Recorder) Gateways() []int32 {
+	r.mu.Lock()
+	defer r.mu.Unlock()
+	gw := make([]int32, len(r.clients))
+	for i, c := range r.clients {
+		gw[i] = c.gw
+	}
+	return gw
+}
+
 // Failed: an attempt ended before logging in, for why.
 func (r *Recorder) Failed(why string) {
 	r.mu.Lock()
@@ -195,6 +207,18 @@ func (r *Recorder) Handshake(how string) {
 	r.mu.Unlock()
 }
 
+// Wait: an SDK client's next attempt waits, of kind (an sdk.WaitKind
+// name), counted since the latest mark: after /network-change the storm
+// waits it cut show as local ones (Plan 6b3c).
+func (r *Recorder) Wait(kind string) {
+	r.mu.Lock()
+	if r.storm.Waits == nil {
+		r.storm.Waits = map[string]int64{}
+	}
+	r.storm.Waits[kind]++
+	r.mu.Unlock()
+}
+
 // Pong: a Ping was answered after rtt.
 func (r *Recorder) Pong(rtt time.Duration) {
 	r.mu.Lock()
@@ -355,16 +379,17 @@ type BackoffCount struct {
 // (SendLag, ms), how long each handshake took from the
 // server's reply to the Login frame (ReplyLag, ms), and where its first
 // message came from: made ahead of time, when it dialed, or the last attempt's
-// again (#106, #107, #109).
+// again (#106, #107, #109); and every SDK wait by kind (Plan 6b3c).
 type StormCount struct {
-	Mark                  string `json:"mark"`
-	AttemptLag            Dist   `json:"attempt_lag"`
-	ConnectLag            Dist   `json:"connect_lag"`
-	SendLag               Dist   `json:"send_lag"`
-	ReplyLag              Dist   `json:"reply_lag"`
-	HandshakesPrecomputed int64  `json:"handshakes_precomputed"`
-	HandshakesInline      int64  `json:"handshakes_inline"`
-	HandshakesReused      int64  `json:"handshakes_reused"`
+	Mark                  string           `json:"mark"`
+	AttemptLag            Dist             `json:"attempt_lag"`
+	ConnectLag            Dist             `json:"connect_lag"`
+	SendLag               Dist             `json:"send_lag"`
+	ReplyLag              Dist             `json:"reply_lag"`
+	HandshakesPrecomputed int64            `json:"handshakes_precomputed"`
+	HandshakesInline      int64            `json:"handshakes_inline"`
+	HandshakesReused      int64            `json:"handshakes_reused"`
+	Waits                 map[string]int64 `json:"waits"`
 }
 
 // Counts returns a snapshot of the counters.
@@ -398,5 +423,9 @@ func (r *Recorder) Counts() Counts {
 	c.StormSinceMark.ReplyLag = r.reply.dist(r.reply.n)
 	c.StormSinceMark.ConnectLag = r.connect.dist(r.connect.n)
 	c.StormSinceMark.SendLag = r.send.dist(r.send.n)
+	c.StormSinceMark.Waits = map[string]int64{}
+	for k, v := range r.storm.Waits {
+		c.StormSinceMark.Waits[k] = v
+	}
 	return c
 }
diff --git a/internal/loadgen/reset_test.go b/internal/loadgen/reset_test.go
new file mode 100644
index 0000000..fe175e5
--- /dev/null
+++ b/internal/loadgen/reset_test.go
@@ -0,0 +1,139 @@
+package loadgen
+
+import (
+	"errors"
+	"net"
+	"net/http"
+	"net/http/httptest"
+	"syscall"
+	"testing"
+	"time"
+
+	"github.com/Tallinzp/goim/internal/sdk"
+)
+
+// sdkRun starts n SDK clients on env with the storm window storm, serves
+// their control API, and waits until all are online.
+func sdkRun(t *testing.T, env Env, n int, storm sdk.Window) (*Runner, *httptest.Server) {
+	t.Helper()
+	r, err := New(Config{Mode: "sdk", Clients: n, Rate: 300, UIDPrefix: "rst"}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	r.cfg.windows = sdk.DefaultWindows()
+	r.cfg.windows.Storm = storm
+	t.Cleanup(run(r))
+	srv := httptest.NewServer(r.Handler())
+	t.Cleanup(srv.Close)
+	waitFor(t, "all online", 10*time.Second, func() bool { s := r.Stats(); return s.RampDone && s.Online == n })
+	return r, srv
+}
+
+// POST /reset marks every gateway and closes every SDK connection at once:
+// every client loses it and logs in again after its storm wait, and each
+// gateway sees its clients close (#134). A taken mark closes nothing.
+func TestResetDropsEveryConnection(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 0, Hi: time.Second})
+	code, m := post(t, srv.URL+"/reset?mark=reset")
+	if code != http.StatusOK || m["population"] != 20.0 || m["close_ms"] == nil {
+		t.Fatalf("/reset: %d %v", code, m)
+	}
+	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Back == 20 })
+	s, _ := r.Recorder().SLO("reset", 5*time.Second)
+	st := r.Stats()
+	if s.Gateway != "all" || s.Population != 20 || s.Lost != 20 || st.Lost["lost"] != 20 || st.StormSinceMark.Waits["storm"] != 20 {
+		t.Fatalf("slo %+v, lost %v, waits %v: want all 20 lost, each after one storm wait", s, st.Lost, st.StormSinceMark.Waits)
+	}
+	if open := st.BalancerOpen[env.GatewayIDs[0]] + st.BalancerOpen[env.GatewayIDs[1]]; open != 20 {
+		t.Fatalf("balancer open %v, want the 20 back", st.BalancerOpen)
+	}
+	waitFor(t, "the gateways count 20 client closes", 5*time.Second, func() bool {
+		return metric(t, env.GatewayIDs[0], "goim_gateway_closes_total", "reason", "client")+
+			metric(t, env.GatewayIDs[1], "goim_gateway_closes_total", "reason", "client") >= 20
+	})
+	if code, _ := post(t, srv.URL+"/reset?mark=reset"); code != http.StatusConflict {
+		t.Fatalf("/reset with a taken mark: %d", code)
+	}
+	if code, _ := post(t, srv.URL+"/reset"); code != http.StatusBadRequest {
+		t.Fatalf("/reset without a mark: %d", code)
+	}
+	if st := r.Stats(); st.Online != 20 || st.Lost["lost"] != 20 {
+		t.Fatalf("a refused /reset closed connections: %+v", st.Counts)
+	}
+}
+
+// abort closes with an RST: the peer's read fails with ECONNRESET, not
+// EOF (SO_LINGER 0, #134).
+func TestAbortSendsAnRST(t *testing.T) {
+	ln, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer ln.Close()
+	b, err := NewBalancer([]string{ln.Addr().String()}, 1)
+	if err != nil {
+		t.Fatal(err)
+	}
+	nc, _, err := b.Dial(t.Context())
+	if err != nil {
+		t.Fatal(err)
+	}
+	peer, err := ln.Accept()
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer peer.Close()
+	(&replyConn{Conn: nc}).abort()
+	peer.SetReadDeadline(time.Now().Add(5 * time.Second))
+	var buf [1]byte
+	if _, err := peer.Read(buf[:]); !errors.Is(err, syscall.ECONNRESET) {
+		t.Fatalf("peer read: %v, want ECONNRESET", err)
+	}
+	if b.Open()[0] != 0 {
+		t.Fatalf("balancer open %v, want the slot freed", b.Open())
+	}
+}
+
+// POST /network-change after a reset cuts every client's storm wait to the
+// local trigger's 0–1s (spec: Local trigger): with a 30–60s storm window
+// all are back within seconds, each after one storm and one local wait.
+func TestNetworkChangeCutsTheStormWait(t *testing.T) {
+	env, _ := startStack(t, 2)
+	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 30 * time.Second, Hi: 60 * time.Second})
+	if code, _ := post(t, srv.URL+"/reset?mark=reset"); code != http.StatusOK {
+		t.Fatalf("/reset: %d", code)
+	}
+	waitFor(t, "all lost", 5*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Lost == 20 })
+	waitFor(t, "all in their storm wait", 5*time.Second, func() bool { return r.Stats().StormSinceMark.Waits["storm"] == 20 })
+	code, m := post(t, srv.URL+"/network-change")
+	if code != http.StatusOK || m["clients"] != 20.0 {
+		t.Fatalf("/network-change: %d %v", code, m)
+	}
+	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Back == 20 })
+	if w := r.Stats().StormSinceMark.Waits; w["local"] != 20 {
+		t.Fatalf("waits since the mark %v, want 20 local", w)
+	}
+}
+
+// Outside sdk mode /reset, /network-change and /ghost are refused, and no
+// mark is taken: light clients don't follow the SDK's reconnect rules (#134).
+func TestResetAndNetworkChangeNeedSDKMode(t *testing.T) {
+	env, _ := startStack(t, 1)
+	r, err := New(Config{Mode: "light", Clients: 2, Rate: 100, UIDPrefix: "nr", PingEvery: time.Second, Timeout: 5 * time.Second}, env)
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer run(r)()
+	srv := httptest.NewServer(r.Handler())
+	defer srv.Close()
+	waitFor(t, "2 online", 10*time.Second, func() bool { return r.Stats().Online == 2 })
+	for _, p := range []string{"/reset?mark=reset", "/network-change", "/ghost?mark=reset"} {
+		if code, _ := post(t, srv.URL+p); code != http.StatusConflict {
+			t.Fatalf("%s in light mode: %d", p, code)
+		}
+	}
+	if _, err := r.Recorder().SLO("reset", time.Second); err == nil || r.Stats().Online != 2 {
+		t.Fatal("a refused /reset or /ghost took a mark or closed connections")
+	}
+}
diff --git a/internal/loadgen/runner.go b/internal/loadgen/runner.go
index 122ba8b..fdffc6d 100644
--- a/internal/loadgen/runner.go
+++ b/internal/loadgen/runner.go
@@ -52,6 +52,14 @@ type Stats struct {
 	ElapsedS   float64          `json:"elapsed_s"`
 	Precompute *PrecomputeStats `json:"precompute,omitempty"`
 	Flood      *FloodStats      `json:"flood,omitempty"` // flood modes only
+	// BalancerOpen is the balancer's open connections per gateway, the
+	// count least connections picks by (#133: the funnel).
+	BalancerOpen map[string]int `json:"balancer_open"`
+	// GhostsOpen is the sockets POST /ghost keeps that no gateway closed
+	// yet; GhostsClosed the ones closed, per gateway, by a Kick or silently
+	// (the heartbeat sweep).
+	GhostsOpen   int64                  `json:"ghosts_open"`
+	GhostsClosed map[string]GhostCloses `json:"ghosts_closed"`
 	Counts
 }
 
@@ -70,7 +78,10 @@ type Runner struct {
 	ramped   chan struct{} // closed when every client has started
 
 	mu    sync.Mutex
-	stops []func() // one per started client
+	stops []func()     // one per started client
+	sdks  []*sdkClient // sdk mode: every started client (Reset, NetworkChange, Ghost)
+
+	ghosts *ghosts // the sockets Ghost keeps
 }
 
 // New checks cfg and prepares a run against env's gateways.
@@ -121,7 +132,8 @@ func New(cfg Config, env Env) (*Runner, error) {
 	for _, gw := range down {
 		bal.SetDown(gw)
 	}
-	r := &Runner{cfg: cfg, env: env, rec: NewRecorder(cfg.Clients, env.GatewayIDs), bal: bal, ramped: make(chan struct{})}
+	r := &Runner{cfg: cfg, env: env, rec: NewRecorder(cfg.Clients, env.GatewayIDs), bal: bal, ramped: make(chan struct{}),
+		ghosts: newGhosts(len(env.GatewayIDs))}
 	if cfg.Mode == "light" {
 		r.pinger = newPingers(runtime.GOMAXPROCS(0), cfg.PingEvery, r.rec)
 	}
@@ -204,6 +216,7 @@ func (r *Runner) Run(ctx context.Context) {
 	close(work)
 	sw.Wait()
 	wg.Wait()
+	r.ghosts.stop()
 }
 
 func (r *Runner) start(ctx context.Context, wg *sync.WaitGroup, i int) {
@@ -217,13 +230,16 @@ func (r *Runner) start(ctx context.Context, wg *sync.WaitGroup, i int) {
 		stop = lc.stop
 		wg.Go(func() { r.light(ctx, lc) })
 	default:
-		c, err := r.newSDK(i)
+		sc, err := r.newSDK(i)
 		if err != nil {
 			r.rec.Failed("sdk")
 			return
 		}
-		stop = c.Close
-		c.Connect()
+		stop = sc.c.Close
+		sc.c.Connect()
+		r.mu.Lock()
+		r.sdks = append(r.sdks, sc)
+		r.mu.Unlock()
 	}
 	r.mu.Lock()
 	r.stops = append(r.stops, stop)
@@ -283,6 +299,73 @@ func (r *Runner) Mark(gw int, name string, down bool) (int, error) {
 	return n, err
 }
 
+// ErrNotSDK refuses /reset and /network-change outside sdk mode: only SDK
+// clients follow the spec's reconnect rules (#134). Nothing changes.
+var ErrNotSDK = errors.New("loadgen: /reset and /network-change need sdk mode")
+
+// Reset is POST /reset (P6a2bI-2, #134): mark name on every gateway, its
+// population every client logged in now, then close every SDK client's
+// newest connection with SO_LINGER 0, so each gateway gets an RST and each
+// client loses its connection at once, as when an LB resets every flow
+// (the spec's whole-population reconnect, "RST from loadgen in one
+// instant"). took is how long the closes took after the mark. A taken mark
+// name closes nothing.
+func (r *Runner) Reset(name string) (n int, took time.Duration, err error) {
+	if r.cfg.Mode != "sdk" {
+		return 0, 0, ErrNotSDK
+	}
+	if n, err = r.Mark(-1, name, false); err != nil {
+		return 0, 0, err
+	}
+	begin := time.Now()
+	r.eachSDK(func(sc *sdkClient) {
+		if c := sc.conn.Load(); c != nil {
+			c.abort()
+		}
+	})
+	return n, time.Since(begin), nil
+}
+
+// NetworkChange is POST /network-change (P6a2bI-2): every SDK client gets
+// sdk.Client.NetworkChanged, as when a city's network recovers. A client
+// in its 0–20s storm wait then attempts within the 0–1s local window; one
+// online, or in a wait the server set, keeps it (spec: Precedence). n is
+// the clients told, took how long that took.
+func (r *Runner) NetworkChange() (n int, took time.Duration, err error) {
+	if r.cfg.Mode != "sdk" {
+		return 0, 0, ErrNotSDK
+	}
+	begin := time.Now()
+	var told atomic.Int64
+	r.eachSDK(func(sc *sdkClient) {
+		sc.c.NetworkChanged()
+		told.Add(1)
+	})
+	return int(told.Load()), time.Since(begin), nil
+}
+
+// eachSDK runs f on every started SDK client, 64 at a time: a call into a
+// client waits for its run loop.
+func (r *Runner) eachSDK(f func(*sdkClient)) {
+	r.mu.Lock()
+	scs := r.sdks
+	r.mu.Unlock()
+	work := make(chan *sdkClient)
+	var wg sync.WaitGroup
+	for range 64 {
+		wg.Go(func() {
+			for sc := range work {
+				f(sc)
+			}
+		})
+	}
+	for _, sc := range scs {
+		work <- sc
+	}
+	close(work)
+	wg.Wait()
+}
+
 // Up is POST /up: gateway gw is back in the balancer. It is not refused for
 // a gateway that /kill or /mark&down=1 took out (#99): a scenario that
 // restarts one needs it. The caller sends it only once gw listens again:
@@ -292,7 +375,12 @@ func (r *Runner) Up(gw int) { r.bal.SetUp(gw) }
 // Stats is the run's state now.
 func (r *Runner) Stats() Stats {
 	s := Stats{Mode: r.cfg.Mode, Clients: r.cfg.Clients, Sources: r.cfg.Sources, Started: r.started.Load(),
-		RampDone: r.rampDone.Load(), Counts: r.rec.Counts()}
+		RampDone: r.rampDone.Load(), BalancerOpen: map[string]int{}, GhostsOpen: r.ghosts.open.Load(),
+		GhostsClosed: map[string]GhostCloses{}, Counts: r.rec.Counts()}
+	for i, n := range r.bal.Open() {
+		s.BalancerOpen[r.env.GatewayIDs[i]] = n
+		s.GhostsClosed[r.env.GatewayIDs[i]] = GhostCloses{Kick: r.ghosts.kick[i].Load(), Silent: r.ghosts.silent[i].Load()}
+	}
 	if b := r.begin.Load(); b != 0 {
 		s.ElapsedS = time.Since(time.Unix(0, b)).Seconds()
 	}
diff --git a/internal/loadgen/sdkclient.go b/internal/loadgen/sdkclient.go
index 267e2be..23f7144 100644
--- a/internal/loadgen/sdkclient.go
+++ b/internal/loadgen/sdkclient.go
@@ -18,6 +18,8 @@ import (
 type sdkClient struct {
 	r      *Runner
 	i      int
+	c      *sdk.Client
+	conn   atomic.Pointer[replyConn] // the newest connection dialed, for Reset
 	gw     atomic.Int32              // gateway of the last dial
 	tok    atomic.Pointer[string]    // the last token, reused until a refresh (spec: Client SDK rules)
 	pre    atomic.Pointer[prepared]  // the next attempt's first message (#106)
@@ -29,9 +31,9 @@ type sdkClient struct {
 	online bool                      // run loop only
 }
 
-func (r *Runner) newSDK(i int) (*sdk.Client, error) {
+func (r *Runner) newSDK(i int) (*sdkClient, error) {
 	sc := &sdkClient{r: r, i: i}
-	return sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
+	c, err := sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
 		AppID: AppID, Platform: clientpb.Platform_PLATFORM_MOBILE,
 		Token: func(_ context.Context, refresh bool) (string, error) {
 			if t := sc.tok.Load(); t != nil && !refresh {
@@ -57,16 +59,31 @@ func (r *Runner) newSDK(i int) (*sdk.Client, error) {
 				sc.gw.Store(int32(gw))
 			}
 			rc := sc.track(nc, seq)
+			rc.gw = gw
+			if seq == sc.dials.Load() {
+				sc.conn.Store(rc)
+			}
 			r.rec.ConnectLag(rc.dialed.Sub(nc.(*conn).connecting))
 			return rc, nil
 		},
 		Initiate: sc.initiate})
+	sc.c = c
+	return sc, err
+}
+
+// abort closes c with an RST, not a FIN (SO_LINGER 0): an LB resetting
+// the flow (Reset, #134).
+func (c *replyConn) abort() {
+	if tc := c.tcp(); tc != nil {
+		tc.SetLinger(0)
+	}
+	c.Conn.Close()
 }
 
 // track wraps a connection the client just dialed; seq is its dial number,
 // taken when its dial started (#107, #109).
 func (sc *sdkClient) track(nc net.Conn, seq uint64) *replyConn {
-	return &replyConn{Conn: nc, sc: sc, seq: seq, dialed: time.Now()}
+	return &replyConn{Conn: nc, sc: sc, gw: -1, seq: seq, dialed: time.Now()}
 }
 
 // initiate is the client's Config.Initiate. It sends the last attempt's
@@ -106,8 +123,11 @@ func (sc *sdkClient) initiate(keyID uint16, pub []byte) (*wire.Initiator, []byte
 // event runs on the client's run loop: it must not block.
 func (sc *sdkClient) event(e sdk.Event) {
 	now := time.Now()
-	if e.Kind == sdk.EventWaiting && (e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure) {
-		sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
+	if e.Kind == sdk.EventWaiting {
+		sc.r.rec.Wait(e.Wait.Kind.String())
+		if e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure {
+			sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
+		}
 	}
 	switch e.Kind {
 	case sdk.EventConnecting:
@@ -195,7 +215,9 @@ var errHandshakeTaken = errors.New("loadgen: handshake used by a later attempt")
 type replyConn struct {
 	net.Conn
 	sc     *sdkClient
-	seq    uint64 // its dial number (#109)
+	gw     int          // the gateway dialed
+	seq    uint64       // its dial number (#109)
+	state  atomic.Int32 // connOpen, connGhost, connClosed (Ghost)
 	dialed time.Time
 	read   atomic.Int64 // unix ns the latest Read returned
 	writes atomic.Int32
@@ -203,7 +225,13 @@ type replyConn struct {
 }
 
 func (c *replyConn) Read(b []byte) (int, error) {
+	if c.state.Load() == connGhost {
+		return 0, errGhost
+	}
 	n, err := c.Conn.Read(b)
+	if c.state.Load() == connGhost {
+		return 0, errGhost
+	}
 	c.read.Store(time.Now().UnixNano())
 	if h := c.hs.Load(); h != nil && n > 0 {
 		if !h.reply(c) {
@@ -221,6 +249,9 @@ func (c *replyConn) Read(b []byte) (int, error) {
 // is a cancelled attempt's: its first message, held or one answered since,
 // is not sent (review M1).
 func (c *replyConn) Write(b []byte) (int, error) {
+	if c.state.Load() == connGhost {
+		return 0, errGhost
+	}
 	switch c.writes.Add(1) {
 	case 1:
 		c.sc.r.rec.SendLag(time.Since(c.dialed))
```

- [ ] **Step 2: Run the tests**

Run: `go test -race -count=1 -run 'TestReset|TestAbort|TestNetworkChange|TestGhost|TestClients' ./internal/loadgen/`
Expected: `ok`.

- [ ] **Step 3: Mutations**

Each change below, alone, must fail the named test (then undo it):

| change | test that fails |
|---|---|
| `tc.SetLinger(0)` → `_ = tc` (`sdkclient.go`) | `TestAbortSendsAnRST` (EOF, not ECONNRESET) |
| `c.abort()` → `_ = c` (`runner.go`, `Reset`) | `TestResetDropsEveryConnection` |
| `sc.c.NetworkChanged()` → `_ = sc` (`runner.go`) | `TestNetworkChangeCutsTheStormWait` |
| delete `sc.r.rec.Wait(e.Wait.Kind.String())` (`sdkclient.go`) | `TestResetDropsEveryConnection`, `TestNetworkChangeCutsTheStormWait` |
| delete `Reset`'s `if r.cfg.Mode != "sdk" { … }` | `TestResetAndNetworkChangeNeedSDKMode` |
| `c.sc.r.ghosts.keep(c)` and `return nil` → `return c.Conn.Close()` (`ghost.go`, `Close`) | `TestGhostKeepsTheGatewaysConnections` (ghosts open 0) |
| delete `c.Conn.SetReadDeadline(aLongTimeAgo)` (`ghost.go`, `ghost`) | `TestGhostKeepsTheGatewaysConnections` (timed out waiting for all lost) |
| delete Write's `if c.state.Load() == connGhost { … }` (`sdkclient.go`) | `TestGhostKeeperWaitsForThePeer` (ghost write: `<nil>`) |
| `kicked || n > 0` → `kicked || n < 0` (`ghost.go`) | `TestGhostKeepsTheGatewaysConnections` (closed as silent) |
| `gw[i] = c.gw` → `gw[i] = c.gw * 0` (`recorder.go`) | `TestClientsGivesEachGateway` |
| delete `Ghost`'s `if r.cfg.Mode != "sdk" { … }` | `TestResetAndNetworkChangeNeedSDKMode` (`/ghost` in light mode: 200) |

- [ ] **Step 4: The whole suite**

Run: `gofmt -l . ; go vet ./... && make test`
Expected: no file listed, `ok` everywhere.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/runner.go internal/loadgen/sdkclient.go internal/loadgen/recorder.go internal/loadgen/control.go internal/loadgen/flood_test.go internal/loadgen/ghost.go internal/loadgen/reset_test.go internal/loadgen/ghost_test.go cmd/loadgen/main.go
git commit -m "feat: add loadgen reset, network change and ghost drops" -m "/reset marks every gateway, then closes every SDK connection with
SO_LINGER 0. Each gateway gets an RST, and each client waits its
storm window. /network-change calls NetworkChanged on every client.
/ghost marks every gateway, then fails every connection to its client
but keeps the socket open and unread. The gateways see only silence.
Loadgen closes each kept socket when its gateway closes it, and counts
the close as kick or silent. All three work in sdk mode only.
GET /clients gives each client's gateway. Loadgen also counts every
SDK wait by kind since the latest mark, and reports its balancer's
open connections per gateway." -m "Refs: P6a2bI-2, #133, #134, #136, review O1 and O4 (Plan 6b3c2 Task 1)."
```

---

### Task 2: Scenario `reconnect`

**Files:**
- Create: `scripts/tier2/reconnect.sh`, `scripts/tier2/reconnect_test.sh`
- Modify: `Makefile` (`test-scripts`, the scenario list), `scripts/tier2/host-setup.sh`, `scripts/tier2/sysctl.conf`, `scripts/tier2/lib.sh` (`t2_nofile`), `scripts/tier2/lib_test.sh`

**Interfaces:**
- Consumes: Plan 6b3c1's `t2_up`, `t2_gws_counters`, `t2_gws_delta`, `t2_lease`, `t2_set_ticks`, `t2_runq_ns`, `t2_kill_tree`; Task 1's `POST /reset?mark=reset`, `POST /ghost?mark=ghost`, `POST /network-change`, `GET /clients`, `balancer_open`, `ghosts_open`, `ghosts_closed`, `storm_since_mark.waits`; loadgen's `/slo?mark=M` (`population`, `lost`, `back`, `detection`, `from_mark`), `backoff_since_mark`; `stack_redis_resetstat`, `stack_redis_latency`, `stack_redis_cli`.
- Produces: `result.json` checks `P_stack`, `P_online`, `P_pinning`, `no_swap_in_P`, `P_drop`, `P_back`, `P_heartbeats`, `P_lease`, `P_sessions`, `P_idle`, `P_processes` for `P` in `reset`, `netchange`, `ghost`, and `artifacts` (34 checks with the three phases); field `P` with `slo`, `loadgen` (backoff, storm lags, waits), `gateways` (counter deltas), `logic` (with Kicks), `series` (`rc_series`: rows from the mark with `lg_ghosts` and `lg_runq_ms_per_s`, per gateway `gw_columns` with `closes_kick_per_s` and `closes_heartbeat_per_s`, `funnel` with `bar_s`, `over_bar`), `redis_latency`, `ghosts`, `sessions`, `storm_first30`, `loadgen_bound`, `reset`, `network_change`, `network_change_after_ms`. `host-setup.sh`: `fs.nr_open = 2097152`, `/etc/security/limits.d/90-goim-tier2.conf`, `/etc/systemd/system/user@.service.d/90-goim-tier2.conf`.

- [ ] **Step 1: The patch**

```diff
diff --git a/Makefile b/Makefile
index e6124d3..9afe218 100644
--- a/Makefile
+++ b/Makefile
@@ -27,6 +27,7 @@ test-race-suite:
 test-scripts:
 	scripts/tier2/lib_test.sh
 	scripts/tier2/floods_test.sh
+	scripts/tier2/reconnect_test.sh
 	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
 
@@ -72,7 +73,7 @@ test-redis:
 
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
-# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, probe (read-only, no guard).
+# scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, reconnect, probe (read-only, no guard).
 # The script's exit code is 0 (pass), 1 (a check failed or the run stopped
 # early) or 2 (host refused); make exits 2 on any failure, so the recipe
 # prints the script's code. Automation that needs it runs the script directly.
diff --git a/scripts/tier2/host-setup.sh b/scripts/tier2/host-setup.sh
index 6b9737d..ce0aa59 100755
--- a/scripts/tier2/host-setup.sh
+++ b/scripts/tier2/host-setup.sh
@@ -11,6 +11,11 @@
 #   /usr/local/sbin/goim-netdrop           root-owned, 0755 (the b2 DROP rule only)
 #   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run goim-netdrop, and only
 #                                          it, without a password (checked with visudo)
+#   /etc/security/limits.d/90-goim-tier2.conf  and
+#   /etc/systemd/system/user@.service.d/90-goim-tier2.conf
+#                                          $SUDO_USER's nofile hard limit 2,097,152
+#                                          (reconnect's ghost phase, #136.5): new
+#                                          sessions only, so log in again
 #   NOTRACK rules for ports 17000-17199 on lo (goim-netdrop notrack, #123): not
 #                                          kept across a reboot, so run it again
 #                                          after one (the Tier 2 guard says so)
@@ -62,6 +67,10 @@ if [ -z "$root" ]; then
   visudo -cqf "$tmp/sudoers" || { echo "host-setup: visudo rejected the sudoers line" >&2; exit 1; }
 fi
 put "$tmp/sudoers" /etc/sudoers.d/goim-netdrop 0440
+echo "$user hard nofile 2097152" >"$tmp/limits"
+put "$tmp/limits" /etc/security/limits.d/90-goim-tier2.conf 0644
+printf '[Service]\nLimitNOFILE=1048576:2097152\n' >"$tmp/user-service"
+put "$tmp/user-service" /etc/systemd/system/user@.service.d/90-goim-tier2.conf 0644
 
 if [ -n "$root" ]; then
   echo "test mode: nothing applied"
@@ -69,6 +78,7 @@ if [ -n "$root" ]; then
 fi
 grep -vE '^[[:space:]]*(#|$)' "$here/modules.conf" | while read -r m; do modprobe "$m"; done
 sysctl -q -p /etc/sysctl.d/90-goim-tier2.conf
+systemctl daemon-reload
 if [ -x /usr/sbin/iptables ]; then
   /usr/local/sbin/goim-netdrop notrack
   echo "applied: NOTRACK for ports 17000-17199 on lo"
@@ -77,4 +87,5 @@ else
   exit 1
 fi
 echo "host-setup: done. Check: sudo -n /usr/local/sbin/goim-netdrop list; sudo -n /usr/local/sbin/goim-netdrop notrack-check"
+echo "host-setup: the nofile hard limit 2,097,152 applies to new sessions: log out and in again (ulimit -Hn shows it), or raise one shell's: sudo prlimit --pid \$\$ --nofile=1048576:2097152"
 echo "host-setup: NOTRACKed packets are UNTRACKED, not ESTABLISHED: check that iptables -S INPUT, iptables -S OUTPUT and nft list ruleset accept lo before any conntrack-state rule"
diff --git a/scripts/tier2/lib.sh b/scripts/tier2/lib.sh
index 8badaad..83b7bd2 100644
--- a/scripts/tier2/lib.sh
+++ b/scripts/tier2/lib.sh
@@ -289,12 +289,13 @@ t2_mem_budget() {
 }
 
 # t2_nofile N: raises the soft nofile limit to N for this run and its
-# children (the hard limit is 1,048,576 on the host, #68), or refuses.
+# children (the hard limit is 2,097,152 on the host after host-setup.sh and
+# a new login, #68, #136.5), or refuses.
 t2_nofile() {
   local hard
   hard=$(ulimit -Hn)
   if [ "$hard" != unlimited ] && [ "$hard" -lt "$1" ]; then
-    t2_refuse "refused: nofile hard limit $hard < $1"
+    t2_refuse "refused: nofile hard limit $hard < $1 (run sudo scripts/tier2/host-setup.sh, then log in again, or: sudo prlimit --pid $$ --nofile=1048576:$1)"
   fi
   ulimit -Sn "$1"
 }
diff --git a/scripts/tier2/lib_test.sh b/scripts/tier2/lib_test.sh
index 9e3df28..a917b95 100755
--- a/scripts/tier2/lib_test.sh
+++ b/scripts/tier2/lib_test.sh
@@ -212,8 +212,10 @@ expect "setup: test mode exits 0" "$?" 0
 expect "setup: sudoers line" "$(cat "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" "alice ALL=(root) NOPASSWD: /usr/local/sbin/goim-netdrop"
 expect "setup: sudoers mode" "$(stat -c %a "$tmp/hs/root/etc/sudoers.d/goim-netdrop")" 440
 expect "setup: helper mode" "$(stat -c %a "$tmp/hs/root/usr/local/sbin/goim-netdrop")" 755
+expect "setup: nofile hard limit" "$(cat "$tmp/hs/root/etc/security/limits.d/90-goim-tier2.conf")" "alice hard nofile 2097152"
+expect "setup: user manager's nofile" "$(grep LimitNOFILE "$tmp/hs/root/etc/systemd/system/user@.service.d/90-goim-tier2.conf")" "LimitNOFILE=1048576:2097152"
 again=$(scripts/tier2/host-setup.sh --root "$tmp/hs/root" --user alice | grep -c '^unchanged')
-expect "setup: idempotent" "$again" 4
+expect "setup: idempotent" "$again" 6
 scripts/tier2/host-setup.sh --root "$tmp/hs/root" --user 'a b' >/dev/null 2>&1; expect "setup: odd user refused" "$?" 2
 scripts/tier2/host-setup.sh --root "$tmp/hs/root" >/dev/null 2>&1 </dev/null; rc=$?
 [ -n "${SUDO_USER:-}" ] || expect "setup: no user refused" "$rc" 2
diff --git a/scripts/tier2/reconnect.sh b/scripts/tier2/reconnect.sh
new file mode 100755
index 0000000..15b3bd6
--- /dev/null
+++ b/scripts/tier2/reconnect.sh
@@ -0,0 +1,590 @@
+#!/usr/bin/env bash
+# Tier 2: reconnect (Plan 6b3c), the spec's load test "whole-population
+# reconnect" (R5S-O6): drop every connection on all gateways at once and
+# measure, with no SLO, the time until 99% are logged in again and the
+# share of clients that reach backoff; the heartbeats of clients already
+# back are answered on time and no component restarts. The spec names
+# three causes: an LB reset or failover, and a carrier's or a region's
+# network that recovers. Each phase on a fresh stack:
+#   reset: 600k SDK clients over 3 gateways (as gwloss b_stress runs them:
+#       -mode sdk -precompute, #134), then POST /reset: loadgen marks every
+#       gateway and closes every connection with an RST in one instant
+#       (P6a2bI-2). The clients log in again after the SDK's 0-20s storm
+#       wait, its pre-handshake retries and backoff.
+#   netchange: the same, then POST /network-change once every client has
+#       lost its connection: each cuts its storm wait to the local
+#       trigger's 0-1s (spec: "the same with every client raising a
+#       network-change event at once").
+#   ghost: the network recovery (review O1, #136): POST /ghost, so every
+#       client loses its connection while loadgen keeps each socket open
+#       and unread, and the gateways see nothing: each still holds its
+#       ~200k old connections ONLINE until DeadAfter (2 x heartbeat + 10s).
+#       Then POST /network-change, as in netchange. The relogins meet the
+#       cap (the old connections plus the new against 250k), each one
+#       admitted replaces a live session (Logic's Kick closes the old
+#       connection), and the sweep closes the old connections that no Kick
+#       reached. The hold lasts until DeadAfter after the mark plus a wheel
+#       tick, so no old connection is left at the stop. Loadgen holds up to
+#       1.2M sockets (the old and the new): nofile 2,097,152 (#136.5).
+# The gateways run the spec's cap (250k, #136) and a 30s heartbeat (#136),
+# so the clients back send Pings during the storm. A per-second series
+# per gateway records pending sockets, pending_mem refusals, open
+# connections, handshakes in progress, Busy, the login queue and the
+# Logouts awaiting, the closes by Kick and by heartbeat, beside loadgen's
+# balancer counts, its kept sockets (ghost) and its run-queue wait, and a
+# funnel reading (one gateway refusing at pending_mem while another, under
+# its cap, has no handshake in progress) is recorded, not gated (#133). The
+# funnel is evidence for parked 6b3b5 and #111 only at or over its bar,
+# RECONNECT_FUNNEL_BAR_S in a row (review O6). Redis's per-command latency
+# from the mark to the hold's end is recorded per phase (review O5), the
+# single-instance baseline for Plan 6c. CPU sets as
+# b_stress's (#108), 4 GOMAXPROCS per gateway: 2 handshake slots each, as
+# #133 sized it. Logic is one process (minReplicas, no HPA).
+#
+# Usage (on the Tier 2 host, from the repo root), after the one-time
+#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
+#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=reconnect
+#   Results: tier2-results/reconnect-<timestamp>/ (result.json, one
+#   directory per phase with logs, artifacts and loadgen results), plus ONE
+#   file to send back: tier2-results/reconnect-<timestamp>.report.txt
+#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
+#   About 30 minutes (each phase ~10: the ramp 120s, the storm up to
+#   RECONNECT_BACK_WAIT, the hold, the stop).
+#
+# Settings (every number whole and positive, refused otherwise before
+# anything starts):
+#   RECONNECT_PHASES      phases to run, in order (default "reset netchange ghost")
+#   RECONNECT_CLIENTS     SDK clients over the 3 gateways (default 600000)
+#   RECONNECT_RATE        clients started per second (default 5000)
+#   RECONNECT_HEARTBEAT   the gateways' -heartbeat, seconds (default 30, the
+#                         SDK's lowest: most Pings in the storm; 30-300)
+#   RECONNECT_CAP         each gateway's -max-conns (default: the gateway's
+#                         250,000, the spec's cap)
+#   RECONNECT_DROP_MAX_S  every client must notice the drop within this
+#                         many seconds of the mark (default 10)
+#   RECONNECT_BACK_WAIT   seconds after the mark to wait for every client to
+#                         log in again (default 900: as b_stress, the
+#                         SDK's backoff doubles to 5 min)
+#   RECONNECT_HOLD        seconds held once every client is back (default
+#                         the heartbeat + 15: each sends a Ping and gets its
+#                         Pong, or its 10s Pong timeout); ghost holds at
+#                         least until DeadAfter + 5s after the mark
+#   RECONNECT_FUNNEL_BAR_S  the funnel's bar, seconds in a row (default 5)
+#   RECONNECT_LG_MEMLIMIT loadgen's GOMEMLIMIT (default 16GiB, as b_stress)
+#   RECONNECT_CPUS_LOADGEN, RECONNECT_CPUS_GATEWAY, RECONNECT_CPUS_LOGIC
+#                         the CPU sets (default b_stress's: 0-5,10,12-17,22;
+#                         6-9,18-21; 11,23)
+#   RECONNECT_GATEWAY_PROCS  each gateway's GOMAXPROCS (default 4)
+#
+# Checks (per phase P):
+#   P_stack        redis, devtoken, Logic and the 3 gateways become Ready
+#   P_online       every client logs in, each gateway holds >= 30% of them
+#   P_pinning      the gateways, Logic + Redis and loadgen on their CPU sets;
+#                  a failure ends the run
+#   P_drop         POST /reset (ghost: /ghost) marked every client
+#                  (population = clients), every one noticed the loss
+#                  within RECONNECT_DROP_MAX_S, and the gateways counted a
+#                  client close for each; for netchange and ghost also POST
+#                  /network-change reached every client. ghost instead of
+#                  the client closes: by the hold's end the gateways closed
+#                  every old connection (loadgen saw each close: by Kick or
+#                  silent, none left open; the gateways' kick + heartbeat
+#                  closes >= population). The reset's close time and the
+#                  mark-to-network-change time are recorded
+#   P_back         every client logged in again within RECONNECT_BACK_WAIT;
+#                  no SLO: the time until 99% are back (slo.from_mark p99),
+#                  the share that reached backoff, the waits since the mark
+#                  by kind, the rejects by reason (ghost: at_capacity, the
+#                  cap shapes it), Logic's Kicks, and whether loadgen was
+#                  the bound (>= 3/4 of its CPUs over the storm's first
+#                  30s, #102) are recorded
+#   P_heartbeats   from the mark to the hold's end every connection that
+#                  ended was the reset's (lost "lost" = population, no
+#                  Pong timeout, no other reason) and no gateway closed one
+#                  for its heartbeat: the clients back were answered on
+#                  time while the others logged in (R5S-O6). ghost: a
+#                  gateway's heartbeat closes are allowed up to the old
+#                  connections loadgen saw closed with no Kick on it. A
+#                  failure while loadgen was the bound is labelled (a
+#                  not_covered line): its reads may have been late
+#                  (lg_runq_ms_per_s in the series, review O3)
+#   P_sessions     after the hold, Logic's store holds one session per
+#                  client, logged in after the mark, on the gateway that
+#                  holds the client's connection (loadgen's GET /clients
+#                  against every sess key; review O4). Logic's replaced
+#                  logins and deleted or not_owner Logouts beside it
+#   P_lease        every gateway renewed its lease every interval from the
+#                  mark to the hold's end, and Logic saw no renewal error
+#   no_swap_in_P   no page swapped in from the mark to the hold's end
+#   P_idle         after loadgen stops, every gateway reaches 0 connections
+#                  and settles its Logouts (dropped ones allowed, counted:
+#                  the logout queue is the cap's size, and every gateway
+#                  queues ~200k at once twice)
+#                  (P_* per phase: 11 checks each)
+#   P_processes    nothing died, panicked or stopped uncleanly ("no
+#                  component restarts")
+#   artifacts      (once) every phase's metrics before and after, the
+#                  storm's CPU profiles (gateways, Logic, loadgen), the
+#                  series, the SLO and loadgen's result
+set -uo pipefail
+
+. "$(dirname "$0")/lib.sh"
+. "$(dirname "$0")/../stack.sh"
+. "$(dirname "$0")/../loadgen.sh"
+t2_init reconnect
+t2_need curl taskset sudo python3
+t2_require_host_setup
+
+PHASES=${RECONNECT_PHASES:-reset netchange ghost}
+named=" "
+for p in $PHASES; do
+  case $p in reset | netchange | ghost) ;; *) echo "reconnect: unknown phase '$p' (reset, netchange, ghost)" >&2; exit 1 ;; esac
+  case $named in *" $p "*) echo "reconnect: phase '$p' named twice in RECONNECT_PHASES" >&2; exit 1 ;; esac
+  named+="$p "
+done
+for v in RECONNECT_CLIENTS RECONNECT_RATE RECONNECT_HEARTBEAT RECONNECT_CAP RECONNECT_DROP_MAX_S \
+  RECONNECT_BACK_WAIT RECONNECT_HOLD RECONNECT_GATEWAY_PROCS RECONNECT_FUNNEL_BAR_S; do
+  case ${!v-1} in ''|*[!0-9]*|0*) echo "reconnect: $v must be a positive whole number" >&2; exit 1 ;; esac
+done
+HB=${RECONNECT_HEARTBEAT:-30}
+[ "$HB" -ge 30 ] && [ "$HB" -le 300 ] || { echo "reconnect: RECONNECT_HEARTBEAT must be 30 to 300 (the gateway's -heartbeat)" >&2; exit 1; }
+N=${RECONNECT_CLIENTS:-600000} RATE=${RECONNECT_RATE:-5000}
+DROP_MAX=${RECONNECT_DROP_MAX_S:-10} BACK_WAIT=${RECONNECT_BACK_WAIT:-900} HOLD=${RECONNECT_HOLD:-$((HB + 15))}
+BAR=${RECONNECT_FUNNEL_BAR_S:-5} DEAD=$((2 * HB + 10))
+# As gwloss b_stress (#93, #105): 600k SDK clients' loadgen peaks at ~16GB
+# under its 16GiB GOMEMLIMIT, beside 3 gateways, Redis and the kernel's
+# socket memory. ghost adds up to ~150k socket pairs (the cap holds the old
+# connections plus the new at 750k on the gateways) and the dials in flight,
+# ~8.5KB of kernel memory per loopback pair (measured on dev): ~2-4GB more,
+# which 23 cannot hold on the 26GB host, so the swap check is its verdict.
+t2_mem_budget 23
+# ghost: each client holds its old socket and a new one: up to 1.2M of
+# loadgen's fds, over the host's former 1,048,576 (#136.5, host-setup.sh).
+case " $PHASES " in *" ghost "*) t2_nofile 2097152 ;; *) t2_nofile 1048576 ;; esac
+
+export STACK_PIN=1 STACK_GATEWAYS=3
+export T2_CPUS_GATEWAY=${RECONNECT_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${RECONNECT_CPUS_LOADGEN:-0-5,10,12-17,22}
+export T2_CPUS_LOGIC=${RECONNECT_CPUS_LOGIC:-11,23}
+export STACK_GATEWAY_PROCS=${RECONNECT_GATEWAY_PROCS:-4}
+LG_NCPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
+LG_ENV=(GOMEMLIMIT="${RECONNECT_LG_MEMLIMIT:-16GiB}")
+STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
+STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
+STACK_GATEWAY_ARGS=(-heartbeat "${HB}s" ${RECONNECT_CAP:+-max-conns "$RECONNECT_CAP"})
+CAP=${RECONNECT_CAP:-250000}
+t2_on_exit 'stack_down'
+t2_on_exit 'lg_stop' # newest first: loadgen stops before the stack
+smp="" prof=""
+t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
+t2_on_exit '[ -n "$prof" ] && t2_kill_tree "$prof"'
+
+GW_COUNTERS=(
+  closes_client='goim_gateway_closes_total:reason="client"'
+  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
+  closes_kick='goim_gateway_closes_total:reason="kick"'
+  hs_ok='goim_gateway_handshakes_total:result="ok"'
+  hs_no_slot='goim_gateway_handshakes_total:result="no_slot"'
+  hs_no_token='goim_gateway_handshakes_total:result="no_token"'
+  rejects_at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
+  rejects_pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
+  rejects_handshake_timeout='goim_gateway_admission_rejects_total:reason="handshake_timeout"'
+  rejects_auth_timeout='goim_gateway_admission_rejects_total:reason="auth_timeout"'
+  rejects_login_queue='goim_gateway_admission_rejects_total:reason="login_queue"'
+  logins_ok='goim_gateway_logins_total:result="ok"'
+  logins_busy='goim_gateway_logins_total:result="busy"'
+  logouts_done='goim_gateway_logouts_total:result="done"'
+  logouts_dropped='goim_gateway_logouts_total:result="dropped"'
+  logouts_retried='goim_gateway_logouts_total:result="retried"'
+  lease_live='goim_gateway_lease_calls_total:result="live"'
+  lease_missing='goim_gateway_lease_calls_total:result="missing"'
+  lease_error='goim_gateway_lease_calls_total:result="error"'
+)
+LOGIC_COUNTERS=(
+  lease_error='goim_logic_lease_renewals_total:result="error"'
+  logins_ok='goim_logic_logins_total:result="ok"'
+  logins_replaced='goim_logic_logins_total:result="replaced"'
+  logouts_deleted='goim_logic_logouts_total:result="deleted"'
+  logouts_unknown='goim_logic_logouts_total:result="unknown"'
+  logouts_not_owner='goim_logic_logouts_total:result="not_owner"'
+  kick_attempts=goim_logic_kick_attempts_total
+  kicks_ok='goim_logic_kick_attempts_total:result="ok"'
+  kick_giveups=goim_logic_kick_giveups_total
+  kick_dropped=goim_logic_kick_dropped_total
+)
+
+t2_report_extra() {
+  local f
+  for f in "$T2_OUT"/*/logs/*.log; do
+    [ -f "$f" ] || continue
+    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
+    tail -n 20 "$f"
+  done
+}
+
+# rc_sampler FILE: appends one JSON line a second to FILE until killed
+# (#133): {"ms", "sets": {"loadgen", "gateway", "logic": [busy, total]},
+# "rq": loadgen's run-queue wait, ns (null if gone; review O3), "lg":
+# {"online", "open": the balancer's per gateway, "ghosts": the sockets
+# /ghost keeps} (null when /stats does not answer within 1s), "gw":
+# {gateway: SERIES_COUNTERS, null if its /metrics does not answer within
+# 1s}, "ms_end": the clock after the gateways' scrapes}.
+SERIES_COUNTERS=(
+  handshake='goim_gateway_connections:state="handshake"'
+  auth_pending='goim_gateway_connections:state="auth_pending"'
+  online='goim_gateway_connections:state="online"'
+  pending_mem='goim_gateway_admission_rejects_total:reason="pending_mem"'
+  at_capacity='goim_gateway_admission_rejects_total:reason="at_capacity"'
+  no_slot='goim_gateway_handshakes_total:result="no_slot"'
+  logins_ok='goim_gateway_logins_total:result="ok"'
+  busy='goim_gateway_logins_total:result="busy"'
+  login_queue=goim_gateway_login_queue
+  awaiting_logout=goim_gateway_awaiting_logout
+  logouts_done='goim_gateway_logouts_total:result="done"'
+  logouts_dropped='goim_gateway_logouts_total:result="dropped"'
+  closes_kick='goim_gateway_closes_total:reason="kick"'
+  closes_heartbeat='goim_gateway_closes_total:reason="heartbeat"'
+)
+rc_sampler() {
+  local ms ms_end sets lg gw g t rq
+  while :; do
+    ms=$(date +%s%3N)
+    rq=$(t2_runq_ns "$LG_PID")
+    sets="\"loadgen\": [$(t2_set_ticks "$T2_CPUS_LOADGEN" | tr ' ' ,)], \"gateway\": [$(t2_set_ticks "$T2_CPUS_GATEWAY" | tr ' ' ,)], \"logic\": [$(t2_set_ticks "$T2_CPUS_LOGIC" | tr ' ' ,)]"
+    gw=""
+    for g in $(stack_admins | awk '$1 ~ /^gw-/ { print $1 }'); do
+      t=$(STACK_CURL_TIMEOUT=1 stack_counters "$g" "${SERIES_COUNTERS[@]}")
+      gw+="${gw:+, }\"$g\": ${t:-null}"
+    done
+    ms_end=$(date +%s%3N)
+    lg=$(LG_CURL_TIMEOUT=1 lg_get /stats '{"online": d["online"], "open": d["balancer_open"], "ghosts": d["ghosts_open"]}')
+    echo "{\"ms\": $ms, \"sets\": {$sets}, \"rq\": ${rq:-null}, \"lg\": ${lg:-null}, \"gw\": {$gw}, \"ms_end\": $ms_end}" >>"$1"
+    sleep 1
+  done
+}
+
+# rc_series FILE MARK_MS CAP BAR: rc_sampler's lines as {"columns",
+# "gw_columns", "rows": [[t_s since the mark, scrape_ms, lg_online,
+# lg_ghosts, lg_runq_ms_per_s, the three sets' busy %, {gateway:
+# [gw_columns]}], ...], "funnel", "samples"}; null if unreadable. A sample
+# whose gateway scrapes trailed its clock by over 1.5s, or missed a
+# gateway, has no gateway readings, and the next on-time sample's rates are
+# taken from the last on-time one (as floods' slow_series).
+# lg_runq_ms_per_s is null for a failed reading or a total that went down
+# (6b3b3 M6). The funnel (#131, #133): the rows where a gateway refused at
+# pending_mem while another, under CAP open connections, had no socket in
+# HANDSHAKE; "seconds" (their intervals summed), "first_s", "longest_s" (in
+# a row), "over_bar" (longest_s >= BAR: only then is it the evidence for
+# 6b3b5 and #111, review O6) and the first 20 as [t_s, refusing, idle].
+# Recorded, not gated.
+rc_series() {
+  python3 -c 'import json, sys
+rows = []
+for l in open(sys.argv[1]):
+    try:
+        rows.append(json.loads(l))
+    except ValueError:
+        pass
+mark, cap, bar = int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
+cols = ["t_s", "scrape_ms", "lg_online", "lg_ghosts", "lg_runq_ms_per_s", "loadgen_set_busy_pct", "gateway_set_busy_pct",
+        "logic_set_busy_pct", "gw"]
+gcols = ["pending", "handshake", "open", "lb_open", "pending_mem_per_s", "at_capacity_per_s", "no_slot_per_s",
+         "logins_ok_per_s", "busy_per_s", "login_queue", "awaiting_logout", "logouts_done_per_s", "logouts_dropped_per_s",
+         "closes_kick_per_s", "closes_heartbeat_per_s"]
+rates = ("pending_mem", "at_capacity", "no_slot", "logins_ok", "busy", "logouts_done", "logouts_dropped", "closes_kick",
+         "closes_heartbeat")
+def busy(a, b, k):
+    x, y = a["sets"].get(k, []), b["sets"].get(k, [])
+    return round(100 * (y[0] - x[0]) / (y[1] - x[1]), 1) if len(x) == 2 and len(y) == 2 and y[1] > x[1] else None
+def on_time(b):
+    return b.get("ms_end") is not None and b["ms_end"] - b["ms"] <= 1500 and len(b["gw"]) > 0 and all(b["gw"].values())
+def rate(p, b, g, k):
+    if p is None or g not in p["gw"] or b["gw"][g].get(k) is None:
+        return None
+    return round((b["gw"][g][k] - (p["gw"][g].get(k) or 0)) / ((b["ms"] - p["ms"]) / 1000), 1)
+def gauge(v, k):
+    return v.get(k)
+def runq(a, b):
+    x, y = a.get("rq"), b.get("rq")
+    return round((y - x) / 1e6 / ((b["ms"] - a["ms"]) / 1000), 1) if x is not None and y is not None and y >= x else None
+out, ids = [], sorted({g for r in rows for g in r["gw"]})
+last = rows[0] if rows and on_time(rows[0]) else None
+for a, b in zip(rows, rows[1:]):
+    if b["ms"] <= a["ms"]:
+        continue
+    lg = b["lg"]
+    gw = None
+    if on_time(b):
+        gw = {}
+        for g, v in sorted(b["gw"].items()):
+            h, ap, on = gauge(v, "handshake"), gauge(v, "auth_pending"), gauge(v, "online")
+            pend = None if h is None or ap is None else h + ap
+            gw[g] = [pend, h, None if pend is None or on is None else pend + on, (lg or {}).get("open", {}).get(g) if lg else None] + \
+                [rate(last, b, g, k) for k in rates[:5]] + [gauge(v, "login_queue"), gauge(v, "awaiting_logout")] + [rate(last, b, g, k) for k in rates[5:]]
+        last = b
+    out.append([round((b["ms"] - mark) / 1000, 1), b["ms_end"] - b["ms"] if b.get("ms_end") is not None else None,
+                lg and lg["online"], lg and lg.get("ghosts"), runq(a, b), busy(a, b, "loadgen"), busy(a, b, "gateway"),
+                busy(a, b, "logic"), gw])
+gi = {k: i for i, k in enumerate(gcols)}
+def funnel(r):
+    gw = r[-1]
+    if not gw:
+        return None
+    refusing = [g for g, v in gw.items() if (v[gi["pending_mem_per_s"]] or 0) > 0]
+    idle = [g for g, v in gw.items() if v[gi["handshake"]] == 0 and v[gi["open"]] is not None and v[gi["open"]] < cap]
+    pair = next(([a, b] for a in refusing for b in idle if a != b), None)
+    return pair
+hits, longest, run, prev = [], 0.0, 0.0, None
+total = 0.0
+for r in out:
+    dt = r[0] - prev if prev is not None else 0.0
+    prev = r[0]
+    p = funnel(r)
+    if p:
+        hits.append([r[0]] + p)
+        total += dt
+        run += dt
+        longest = max(longest, run)
+    else:
+        run = 0.0
+print(json.dumps({"columns": cols, "gw_columns": gcols, "gateways": ids, "rows": out, "samples": len(rows),
+    "funnel": {"seconds": round(total, 1), "first_s": hits[0][0] if hits else None, "longest_s": round(longest, 1),
+               "bar_s": bar, "over_bar": round(longest, 1) >= bar, "rows": hits[:20]}}))' "$1" "$2" "$3" "$4" 2>/dev/null || echo null
+}
+
+# rc_sessions_cmp CLIENTS SESSIONS MARK_MS: P_sessions' reading (review
+# O4). CLIENTS is loadgen's GET /clients; SESSIONS one line per client, in
+# order: "N|VALUE", N the sess hash's fields, VALUE the first ("gw|epoch|
+# conn_id|device|conn_seq|login_ms|tok_iat"). {"clients", "offline": not
+# logged in at loadgen, "found": one session on the client's gateway with
+# login_ms >= MARK_MS, "missing", "extra": more than one, "wrong_gateway",
+# "stale": logged in before the mark, "examples": up to 5 [uid, why]}; null
+# if a file is unreadable or SESSIONS has too few lines.
+rc_sessions_cmp() {
+  python3 -c 'import json, sys
+c = json.load(open(sys.argv[1]))
+lines = open(sys.argv[2]).read().split("\n")
+mark = int(sys.argv[3])
+gw = c["gw"]
+if len(lines) < len(gw):
+    raise SystemExit(1)
+r = {"clients": len(gw), "offline": 0, "found": 0, "missing": 0, "extra": 0, "wrong_gateway": 0, "stale": 0, "examples": []}
+for i, (g, l) in enumerate(zip(gw, lines)):
+    n, _, v = l.partition("|")
+    why = None
+    if g < 0:
+        why = "offline"
+    elif int(n) == 0:
+        why = "missing"
+    elif int(n) > 1:
+        why = "extra"
+    else:
+        f = v.split("|")
+        why = "wrong_gateway" if f[0] != c["gateways"][g] else "stale" if int(f[5]) < mark else None
+    if why:
+        r[why] += 1
+        if len(r["examples"]) < 5:
+            r["examples"].append(["%s-%d" % (c["uid_prefix"], i), why])
+    else:
+        r["found"] += 1
+print(json.dumps(r))' "$1" "$2" "$3" 2>/dev/null || echo null
+}
+
+# rc_sessions MARK_MS: rc_sessions_cmp over loadgen's /clients and every
+# client's sess key, read in EVALs of 500 keys (HVALS each) through one
+# redis-cli: ~1,200 short scripts on the one Redis after the hold.
+rc_sessions() {
+  local a=$STACK_DIR/artifacts
+  curl -sf -m 60 "http://$(lg_control)/clients" >"$a/clients.json" || { echo null; return; }
+  python3 -c 'import json, sys
+c = json.load(open(sys.argv[1]))
+lua = "local r = {} for i, k in ipairs(KEYS) do local v = redis.call(\"HVALS\", k) r[i] = #v .. \"|\" .. (v[1] or \"\") end return r"
+keys = ["sess:{%s-%d}" % (c["uid_prefix"], i) for i in range(len(c["gw"]))]
+for j in range(0, len(keys), 500):
+    b = keys[j:j + 500]
+    print("EVAL %s %d %s" % (json.dumps(lua), len(b), " ".join(b)))' "$a/clients.json" >"$STACK_DIR/sessions.cmd" || { echo null; return; }
+  stack_redis_cli --raw <"$STACK_DIR/sessions.cmd" >"$a/sessions.txt" 2>&1
+  rc_sessions_cmp "$a/clients.json" "$a/sessions.txt" "$1"
+}
+
+t=$SECONDS
+if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
+  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
+  exit 1
+fi
+STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
+t2_duration setup $((SECONDS - t))
+missing=""
+
+for name in $PHASES; do
+  t0=$SECONDS
+  t2_up "$name" || { stack_down; t2_duration "$name" $((SECONDS - t0)); continue; }
+
+  # The ramp: as b_stress's, over every gateway.
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
+  lg_start "$name" -mode sdk -precompute -clients "$N" -rate "$RATE" -uid-prefix "$name"
+  stack_usage_start loadgen="$LG_PID"
+  t=$SECONDS
+  online=false
+  lg_wait_online "$N" $((N / RATE * 3 + 60)) &&
+    online=$(lg_get /stats 'd["online"] == '"$N"' and min(d["online_by_gateway"].values()) >= '"$N"' * 0.3')
+  [ "$online" = true ] || online=false # null: /stats unreadable
+  ramp_s=$((SECONDS - t))
+  ramp=$(stack_usage_json)
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
+  t2_check "${name}_online" "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
+  wrong=""
+  for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
+  for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+  have=$(taskset -pc "$LG_PID" 2>/dev/null)
+  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
+  t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
+  if [ -n "$wrong" ]; then
+    lg_stop
+    stack_down
+    t2_duration "$name" $((SECONDS - t0))
+    t2_finish
+  fi
+  if [ "$online" != true ]; then
+    lg_stop
+    stack_down
+    t2_duration "$name" $((SECONDS - t0))
+    continue
+  fi
+  # #106: every client holds its next handshake frame at the mark, as in
+  # b_stress, so the first attempts are the spec's, not loadgen's X25519.
+  t=$SECONDS
+  until [ "$(lg_get /stats '(d.get("precompute") or {}).get("pending") == 0 and (d.get("precompute") or {}).get("ready") == d["online"]')" = true ]; do
+    [ $((SECONDS - t)) -lt 120 ] || break
+    sleep 1
+  done
+  precompute=$(lg_get /stats '{"pending": (d.get("precompute") or {}).get("pending"), "ready": (d.get("precompute") or {}).get("ready"), "online": d["online"], "wait_s": '"$((SECONDS - t))"'}')
+
+  # The storm: counters before, the reset (ghost: /ghost), then the series
+  # and profiles.
+  mark=reset
+  [ "$name" = ghost ] && mark=ghost
+  stack_snapshot before
+  g0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+  lost0=$(lg_get /stats 'd["lost"]')
+  t2_record_start "$name"
+  t2_cpustat_snap "$STACK_DIR/cpustat-2"
+  t2_tcpstat_snap "$STACK_DIR/tcpstat-2"
+  stack_usage_start loadgen="$LG_PID"
+  redis_reset=true; stack_redis_resetstat || redis_reset=false
+  mark_ms=$(date +%s%3N)
+  rc_sampler "$STACK_DIR/artifacts/series.jsonl" &
+  smp=$!
+  # On loadgen's 14 CPUs, not Logic's 2: Logic and Redis share those, the
+  # set most likely to bound a login storm, and Review Focus 4 reads its
+  # busy % (review O8).
+  taskset -apc "$T2_CPUS_LOADGEN" "$smp" >/dev/null
+  (stack_profile storm 30 & lg_profile storm 30 & { sleep 30; stack_usage_json >"$STACK_DIR/storm30.json"; }; wait) >/dev/null 2>&1 &
+  prof=$!
+  reply=$(lg_post "/$mark?mark=$mark" | lg_py - d)
+  mark_t=$SECONDS
+  # Every client notices the RST (ghost: its failed read) at once; until
+  # then a network change would find it online and do nothing.
+  dropped=false
+  until [ "$(lg_get "/slo?mark=$mark&within=30s" 'd["lost"] == d["population"]')" = true ]; do
+    [ $((SECONDS - mark_t)) -lt "$DROP_MAX" ] || break
+    sleep 0.2
+  done
+  [ "$(lg_get "/slo?mark=$mark&within=30s" 'd["lost"] == d["population"]')" = true ] && dropped=true
+  nc=null nc_after_ms=null
+  if [ "$name" != reset ]; then
+    nc_after_ms=$(($(date +%s%3N) - mark_ms))
+    nc=$(lg_post /network-change | lg_py - d)
+  fi
+  back=$(t2_bool lg_wait_back "$mark" "$BACK_WAIT")
+  back_s=$((SECONDS - mark_t))
+  sleep "$HOLD"
+  # ghost: every old connection is gone by DeadAfter after the mark (its
+  # last Ping came before it), one wheel tick, and loadgen's closes.
+  left=$((mark_t + DEAD + 5 - SECONDS))
+  [ "$name" = ghost ] && [ "$left" -gt 0 ] && sleep "$left"
+  redis=$(stack_redis_latency)
+  gh=$(lg_get /stats '{"open": d["ghosts_open"], "closed": d["ghosts_closed"]}')
+  kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
+  wait "$prof"; prof=""
+  storm=$(stack_usage_json)
+  storm30=$(cat "$STACK_DIR/storm30.json" 2>/dev/null) || storm30=null
+  g1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+  lost1=$(lg_get /stats 'd["lost"]')
+  window_s=$((SECONDS - mark_t))
+  t2_cpustat_snap "$STACK_DIR/cpustat-3"
+  t2_tcpstat_snap "$STACK_DIR/tcpstat-3"
+  t2_record_end "$name"
+  stack_snapshot after
+  lg_get "/slo?mark=$mark&within=30s" d >"$STACK_DIR/artifacts/slo-$mark.json"
+  slo=$STACK_DIR/artifacts/slo-$mark.json
+  d=$(t2_gws_delta "$g0" "$g1")
+  ld=$(stack_counters_delta "$l0" "$l1")
+  st=$(lg_get /stats '{"backoff": d["backoff_since_mark"], "storm": d["storm_since_mark"], "login_latency": d["login_latency"], "failed": d["failed"]}')
+  series=$(rc_series "$STACK_DIR/artifacts/series.jsonl" "$mark_ms" "$CAP" "$BAR")
+  loadgen_bound=$(echo "$storm30" | lg_true - 'd["loadgen"]["cores"] >= '"$LG_NCPUS"' * 0.75')
+
+  pop=$(lg_py "$slo" 'd["population"]')
+  rule='d["r"]["population"] == '"$N"' and d["s"]["lost"] == d["s"]["population"] and 0 <= d["s"]["detection"]["max_ms"] <= '"$DROP_MAX"' * 1000'
+  if [ "$name" = ghost ]; then
+    # The gateways closed every old connection: loadgen saw each close,
+    # none is left open, and the gateways counted them (Kick or heartbeat).
+    rule+=' and d["gh"]["open"] == 0 and sum(v["kick"] + v["silent"] for v in d["gh"]["closed"].values()) == d["s"]["population"] and sum(v["closes_kick"] + v["closes_heartbeat"] for v in d["g"].values()) >= d["s"]["population"]'
+  else
+    rule+=' and sum(v["closes_client"] for v in d["g"].values()) >= d["s"]["population"]'
+  fi
+  [ "$name" != reset ] && rule+=' and d["nc"] is not None and d["nc"]["clients"] == '"$N"
+  t2_check "${name}_drop" "$([ "$dropped" = true ] && echo "{\"r\": ${reply:-null}, \"s\": $(cat "$slo"), \"g\": $d, \"nc\": $nc, \"gh\": ${gh:-null}}" | lg_true - "$rule" || echo false)" \
+    "\"$mark\": ${reply:-null}, \"lost\": $(lg_py "$slo" 'd["lost"]'), \"detection\": $(lg_py "$slo" 'd["detection"]'), \"max_s\": $DROP_MAX, \"closes\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("closes_client", "closes_kick", "closes_heartbeat")} for g, v in d.items()}'), \"ghosts\": ${gh:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms"
+  t2_check "${name}_back" "$([ "$back" = true ] && lg_true "$slo" 'd["back"] == d["population"] == '"$N" || echo false)" \
+    "\"back_s\": $back_s, \"wait_s\": $BACK_WAIT, \"from_mark\": $(lg_py "$slo" 'd["from_mark"]'), \"backoff\": $(echo "$st" | lg_py - 'd["backoff"]'), \"backoff_share\": $(echo "$st" | lg_py - 'round(d["backoff"]["clients"] / '"${pop:-0}"', 4) if '"${pop:-0}"' else None'), \"waits\": $(echo "$st" | lg_py - 'd["storm"]["waits"]'), \"loadgen_bound\": $loadgen_bound, \"rejects\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_no_slot", "hs_no_token", "rejects_at_capacity", "rejects_pending_mem", "rejects_handshake_timeout", "rejects_auth_timeout", "rejects_login_queue", "logins_busy")} for g, v in d.items()}'), \"kicks\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("kick_attempts", "kicks_ok", "kick_giveups", "kick_dropped")}'), \"funnel\": $(echo "$series" | lg_py - 'd["funnel"]')"
+  lost=$(echo "{\"a\": ${lost0:-null}, \"b\": ${lost1:-null}}" | lg_py - '{k: v - d["a"].get(k, 0) for k, v in d["b"].items() if v - d["a"].get(k, 0)}')
+  # ghost: a gateway's heartbeat closes may be its old connections that no
+  # Kick reached (loadgen saw them close silently), and no more.
+  allowed='0'
+  [ "$name" = ghost ] && allowed='((d["gh"] or {}).get("closed") or {}).get(g, {}).get("silent", 0)'
+  hb_ok=$(echo "{\"l\": ${lost:-null}, \"g\": $d, \"p\": ${pop:-null}, \"gh\": ${gh:-null}}" | lg_true - 'd["l"] == {"lost": d["p"]} and all(v["closes_heartbeat"] <= '"$allowed"' for g, v in d["g"].items())')
+  runq_max=$(echo "$series" | lg_py - 'max([r[4] for r in d["rows"] if r[4] is not None], default=None)')
+  t2_check "${name}_heartbeats" "$hb_ok" \
+    "\"lost_since_mark\": ${lost:-null}, \"population\": ${pop:-null}, \"closes_heartbeat\": $(echo "$d" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ghosts_silent\": $(echo "${gh:-null}" | lg_py - '{g: v["silent"] for g, v in d["closed"].items()}'), \"heartbeat_s\": $HB, \"hold_s\": $HOLD, \"loadgen_bound\": $loadgen_bound, \"lg_runq_ms_per_s_max\": $runq_max"
+  # As floods' verdict (#126, #127): the check still fails, labelled, when
+  # loadgen was the bound: its reads may have been late (review O3).
+  [ "$hb_ok" != true ] && [ "$loadgen_bound" = true ] &&
+    t2_not_covered "${name}_heartbeats failed while loadgen was the bound: a late read on loadgen's side can also time a Pong out; read ${name}.series lg_runq_ms_per_s and loadgen_set_busy_pct beside the Pong timeouts (${name}.loadgen.storm)"
+  t2_lease "$name" "$d" "$window_s" "$ld"
+  sessions=$(rc_sessions "$mark_ms")
+  t2_check "${name}_sessions" "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
+    "\"sessions\": ${sessions:-null}, \"logic\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("logins_replaced", "logouts_deleted", "logouts_not_owner", "logouts_unknown")}')"
+
+  # The end: the result is written before any client stops (#84), then the
+  # gateways drain to idle (dropped Logouts allowed and counted).
+  lg_stop
+  t=$SECONDS
+  idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
+  stop_s=$((SECONDS - t))
+  logouts=$(t2_gws_counters done='goim_gateway_logouts_total:result="done"' dropped='goim_gateway_logouts_total:result="dropped"' logins_ok='goim_gateway_logins_total:result="ok"')
+  t2_check "${name}_idle" "$idle" "\"stop_s\": $stop_s, \"logouts\": $logouts, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
+  stack_down
+  problems=$(stack_health)
+  t2_check "${name}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
+
+  cpu_sets="{\"ramp\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"storm\": $(t2_cpusets_json "$STACK_DIR/cpustat-2" "$STACK_DIR/cpustat-3")}"
+  t2_field "$name" "{\"clients\": $N, \"rate\": $RATE, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"precompute_at_mark\": $precompute, \"reset\": ${reply:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms, \"back_s\": $back_s, \"hold_s\": $HOLD, \"window_s\": $window_s, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"slo\": $(cat "$slo"), \"loadgen\": $st, \"gateways\": $d, \"logic\": $ld, \"cpu_sets\": $cpu_sets, \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3"), \"series\": $series, \"redis_latency\": ${redis:-null}, \"redis_reset\": $redis_reset, \"ghosts\": ${gh:-null}, \"sessions\": ${sessions:-null}, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS}"
+  t2_duration "$name" $((SECONDS - t0))
+
+  a=$STACK_DIR/artifacts
+  for f in before/logic after/logic before/gw-0 after/gw-0 before/gw-1 after/gw-1 before/gw-2 after/gw-2; do
+    grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $name/$f.metrics"
+  done
+  for f in logic gw-0 gw-1 gw-2 loadgen; do gzip -t "$a/storm/$f.cpu.pb.gz" 2>/dev/null || missing+=" $name/storm/$f.cpu"; done
+  [ -s "$a/series.jsonl" ] || missing+=" $name/series.jsonl"
+  [ "$(lg_true "$slo" '"population" in d')" = true ] || missing+=" $name/slo-$mark.json"
+  [ "$(lg_true "$a/loadgen-$name.json" '"mode" in d')" = true ] || missing+=" $name/loadgen-$name.json"
+done
+t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
+
+t2_not_covered "the drop as an LB restart or failover: Tier 2's LB is loadgen's in-process balancer (#134); Tier 3 (Plan 6d)" \
+  "a carrier or regional network recovering for part of the population: ghost drops every client (the spec's worst case)" \
+  "heartbeat Pong times: SDK clients record a Pong only by its absence (the 10s Pong timeout), not its delay" \
+  "per-shard Redis load with the gwconns shards marked (one redis-server, its per-command latency in P.redis_latency; Redis Cluster in Plan 6c)" \
+  "the session check in Redis Cluster: its EVALs read 500 keys of many slots (one redis-server here)"
+t2_finish
diff --git a/scripts/tier2/reconnect_test.sh b/scripts/tier2/reconnect_test.sh
new file mode 100755
index 0000000..6a4fd36
--- /dev/null
+++ b/scripts/tier2/reconnect_test.sh
@@ -0,0 +1,63 @@
+#!/usr/bin/env bash
+# Tests for scripts/tier2/reconnect.sh's rc_series over synthetic
+# rc_sampler lines (make test-scripts): the per-gateway rates, a late
+# scrape, loadgen's run-queue wait, and the funnel reading and its bar
+# (#133, review O3, O6); and rc_sessions_cmp (review O4). The scenario
+# itself runs on the Tier 2 host.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+. scripts/tier2/lib.sh
+. scripts/loadgen.sh
+tmp=$(mktemp -d)
+trap 'rm -rf "$tmp"' EXIT
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+eval "$(sed -n '/^rc_series() {/,/^}/p; /^rc_sessions_cmp() {/,/^}/p' scripts/tier2/reconnect.sh)"
+
+# gw HANDSHAKE ONLINE PENDING_MEM: one gateway's reading (its Kick closes
+# = ONLINE); line MS SKEW GW0 GW1 [RQ] one sample (loadgen 10 online, the
+# balancer's 5 and 6 open, 3 ghosts, run-queue wait RQ ns, default MS ms).
+gw() { echo "{\"handshake\": $1, \"auth_pending\": 0, \"online\": $2, \"pending_mem\": $3, \"at_capacity\": 0, \"no_slot\": 0, \"logins_ok\": $2, \"busy\": 0, \"login_queue\": 0, \"awaiting_logout\": 0, \"logouts_done\": 0, \"logouts_dropped\": 0, \"closes_kick\": $2, \"closes_heartbeat\": 0}"; }
+line() { echo "{\"ms\": $1, \"sets\": {}, \"rq\": ${5-$1000000}, \"lg\": {\"online\": 10, \"open\": {\"gw-0\": 5, \"gw-1\": 6}, \"ghosts\": 3}, \"gw\": {\"gw-0\": $3, \"gw-1\": $4}, \"ms_end\": $(($1 + $2))}"; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+
+# gw-0 refuses at pending_mem from 2s while gw-1, under the cap (100), has
+# no handshake in progress: the funnel, for 2s in a row; at 4s gw-1 starts
+# a handshake and it ends. A late scrape (2.5s) at 5s has no gateway
+# readings, and the next sample's rates are taken from 5s's; then gw-1
+# is at its cap: full, not idle.
+{ line 1000 10 "$(gw 5 10 0)" "$(gw 1 10 0)"; line 2000 10 "$(gw 5 10 0)" "$(gw 1 10 0)"
+  line 3000 10 "$(gw 5 10 100)" "$(gw 0 10 0)"; line 4000 10 "$(gw 5 10 200)" "$(gw 0 10 0)"
+  line 5000 10 "$(gw 5 10 300)" "$(gw 1 10 0)"; line 6000 2500 "$(gw 5 10 900)" "$(gw 0 10 0)"
+  line 9000 10 "$(gw 5 10 900)" "$(gw 0 100 0)"; } >"$tmp/f.jsonl"
+s=$(rc_series "$tmp/f.jsonl" 1000 100 5)
+expect "series: rows from the mark" "$(get "$s" '[r[0] for r in d["rows"]]')" "[1.0, 2.0, 3.0, 4.0, 5.0, 8.0]"
+expect "series: gw-0 pending_mem per s" "$(get "$s" '[r[-1] and r[-1]["gw-0"][d["gw_columns"].index("pending_mem_per_s")] for r in d["rows"]]')" "[0.0, 100.0, 100.0, 100.0, None, 150.0]"
+expect "series: open and lb_open" "$(get "$s" '[d["rows"][0][-1]["gw-1"][d["gw_columns"].index(k)] for k in ("pending", "open", "lb_open")]')" "[1, 11, 6]"
+expect "series: closes by Kick per s" "$(get "$s" '[r[-1] and r[-1]["gw-1"][d["gw_columns"].index("closes_kick_per_s")] for r in d["rows"]]')" "[0.0, 0.0, 0.0, 0.0, None, 22.5]"
+expect "series: ghosts and run-queue wait (1ms per ms)" "$(get "$s" '[[r[d["columns"].index(k)] for k in ("lg_ghosts", "lg_runq_ms_per_s")] for r in d["rows"][:2]]')" "[[3, 1000.0], [3, 1000.0]]"
+expect "series: the funnel under the bar" "$(get "$s" 'd["funnel"]')" "{'seconds': 2.0, 'first_s': 2.0, 'longest_s': 2.0, 'bar_s': 5.0, 'over_bar': False, 'rows': [[2.0, 'gw-0', 'gw-1'], [3.0, 'gw-0', 'gw-1']]}"
+expect "series: the funnel at the bar" "$(get "$(rc_series "$tmp/f.jsonl" 1000 100 2)" 'd["funnel"]["over_bar"]')" True
+# At its cap a gateway with no handshake in progress is full, not idle.
+expect "series: a full gateway is no funnel" "$(get "$(rc_series "$tmp/f.jsonl" 1000 10 5)" 'd["funnel"]["seconds"]')" 0.0
+expect "series: unreadable" "$(rc_series "$tmp/none.jsonl" 0 100 5)" null
+# A failed run-queue reading, and a total that went down, give null.
+{ line 1000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 5000000; line 2000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" null
+  line 3000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 9000000; line 4000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 1000; } >"$tmp/q.jsonl"
+expect "series: run-queue gaps are null" "$(get "$(rc_series "$tmp/q.jsonl" 1000 100 5)" '[r[d["columns"].index("lg_runq_ms_per_s")] for r in d["rows"]]')" "[None, None, None]"
+
+# rc_sessions_cmp: 7 clients on gw-0/gw-1, the mark at 1000: one good on
+# each gateway, one missing, one with two sessions, one on the wrong
+# gateway, one logged in before the mark, one offline at loadgen.
+echo '{"uid_prefix": "u", "gateways": ["gw-0", "gw-1"], "gw": [0, 1, 0, 1, 0, 1, -1]}' >"$tmp/clients.json"
+printf '%s\n' "1|gw-0|7|1|dev|1|1500|0" "1|gw-1|7|2|dev|1|1200|0" "0|" "2|gw-0|7|4|dev|1|1300|0" "1|gw-1|7|5|dev|1|1300|0" \
+  "1|gw-1|7|6|dev|1|900|0" "1|gw-0|7|7|dev|1|1300|0" >"$tmp/sessions.txt"
+c=$(rc_sessions_cmp "$tmp/clients.json" "$tmp/sessions.txt" 1000)
+expect "sessions: counts" "$(get "$c" '[d[k] for k in ("clients", "found", "missing", "extra", "wrong_gateway", "stale", "offline")]')" "[7, 2, 1, 1, 1, 1, 1]"
+expect "sessions: examples" "$(get "$c" 'd["examples"]')" "[['u-2', 'missing'], ['u-3', 'extra'], ['u-4', 'wrong_gateway'], ['u-5', 'stale'], ['u-6', 'offline']]"
+head -n 3 "$tmp/sessions.txt" >"$tmp/short.txt"
+expect "sessions: too few lines" "$(rc_sessions_cmp "$tmp/clients.json" "$tmp/short.txt" 1000)" null
+
+[ "$fails" -eq 0 ] && echo "reconnect_test: all passed" || { echo "reconnect_test: $fails failed"; exit 1; }
diff --git a/scripts/tier2/sysctl.conf b/scripts/tier2/sysctl.conf
index d42eb19..0946e75 100644
--- a/scripts/tier2/sysctl.conf
+++ b/scripts/tier2/sysctl.conf
@@ -16,6 +16,10 @@ net.ipv4.tcp_max_syn_backlog = 65535
 net.core.netdev_max_backlog = 65536
 # TCP buffer memory in pages (4, 6, 8 GB): both ends of 600k sockets live on this host.
 net.ipv4.tcp_mem = 1048576 1572864 2097152
+# File handles per process: reconnect's ghost phase keeps every client's old
+# socket beside its new one on loadgen, up to 1.2M fds (#136.5). nr_open caps
+# the nofile hard limit, which host-setup.sh raises to 2,097,152.
+fs.nr_open = 2097152
 # kind (Tier 3 TARGET=local, choice #67).
 fs.inotify.max_user_instances = 8192
 fs.inotify.max_user_watches = 1048576
```

- [ ] **Step 2: Script tests**

Run: `bash -n scripts/tier2/reconnect.sh && make test-scripts`
Expected: `reconnect_test: all passed` (rates from the mark, a late scrape, closes by Kick per s, ghosts and run-queue wait, the funnel under and at the bar, a full gateway is no funnel, run-queue gaps are null, the session counts and examples, too few lines), `lib_test` with `setup: nofile hard limit`, `setup: user manager's nofile` and `setup: idempotent` (6), and the other three `all passed`.

- [ ] **Step 3: Dry run on the dev machine**

A throwaway copy as in Plan 6b3c1's Task 2 Step 3, with the CPU sets on the dev machine's two CPUs, the scale cut, and `ghost`'s nofile at the dev machine's hard limit (~4 min; `ghost` holds 75s after its mark):
```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/reconnect.sh "$D/reconnect.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|; s|t2_nofile 2097152|t2_nofile 1048576|' "$D/reconnect.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/reconnect.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/reconnect.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|t2_nofile\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/reconnect.sh"
rm -rf tier2-results/reconnect-*
GOIM_TEST_TIER=2 RECONNECT_CPUS_GATEWAY=0-1 RECONNECT_CPUS_LOGIC=0-1 RECONNECT_CPUS_LOADGEN=0-1 RECONNECT_GATEWAY_PROCS=1 \
  RECONNECT_CLIENTS=3000 RECONNECT_RATE=500 RECONNECT_BACK_WAIT=120 RECONNECT_LG_MEMLIMIT=512MiB \
  bash "$D/reconnect.sh" >"$D/out.txt" 2>&1; echo "exit $?"
python3 -c 'import json,glob; d=json.load(open(glob.glob("tier2-results/reconnect-*/result.json")[0])); c=d["checks"]; print(len(c), [k for k, v in c.items() if not v["pass"]]); [print(p, c[p + "_back"]["from_mark"]["p99_ms"], c[p + "_back"]["waits"], c[p + "_drop"]["network_change_after_ms"], c[p + "_drop"]["ghosts"], c[p + "_sessions"]["sessions"]["found"], d[p]["series"]["funnel"]["over_bar"]) for p in ("reset", "netchange", "ghost")]'
```
The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, a `t2_nofile` line with `1048576` twice, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype): `exit 1`; **34** checks; failing only `no_swap_in_reset`, `no_swap_in_netchange` and `no_swap_in_ghost` (the dev machine's swap; none to all three). `reset`: p99 under 20,500ms (the 0–20s storm wait; prototype 19,889), `waits` `{"storm": 3000}`, `network_change_after_ms` `None`, ghosts all 0. `netchange`: p99 under 3,000ms (prototype 1,627), `waits` with `local` near 3,000 (prototype 2,957) beside `storm` 3,000, `network_change_after_ms` under 1,000 (prototype 338). `ghost`: p99 under 3,000ms (prototype 1,742), `waits` as `netchange`'s (prototype `local` 2,961), ghosts `open` 0 and `kick` 1,000 per gateway, `silent` 0 (every relogin's Kick reached its old connection before `DeadAfter`). Each phase: sessions found 3,000, funnel `over_bar` `False`. Then `rm -rf tier2-results/reconnect-*`.

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/reconnect.sh scripts/tier2/reconnect_test.sh Makefile scripts/tier2/host-setup.sh scripts/tier2/sysctl.conf scripts/tier2/lib.sh scripts/tier2/lib_test.sh
git commit -m "test: add Tier 2 scenario reconnect" -m "Phases reset, netchange and ghost drop 600k SDK clients on 3 gateways
at once, with no SLO. In ghost the gateways keep the old connections
until a Kick or the heartbeat sweep closes them.
The scenario records the time until 99% are back, the backoff share,
Redis latency, and a per-second series per gateway with a funnel
reading and its bar. It checks that every client comes back, no Pong
times out, each client has one session on its gateway, and no
component fails.
host-setup.sh raises the nofile hard limit to 2,097,152 for ghost." -m "Refs: R5S-O6, #133, #134, #136, review O1-O6 and O8 (Plan 6b3c2 Task 2)."
```

Push the branch.

---

### Task 3: Tier 2 run 31 and the record

- [ ] **Step 1: The host's new limit (the user, once)**

`ghost` needs a nofile hard limit of 2,097,152 (Review Focus 6). Run `sudo scripts/tier2/host-setup.sh`, then log out and in again, and check `ulimit -Hn` (2097152). Without a new login, raise one shell's limit instead: `sudo prlimit --pid $$ --nofile=1048576:2097152`. Until then `t2_require_host_setup` refuses (`fs.nr_open`) and `t2_nofile` refuses with the same remedy.

- [ ] **Step 2: Run 31, `reconnect` (the user, on the host, ~30 min)**

```bash
git pull && GOIM_TEST_TIER=2 make test-tier2 SCENARIO=reconnect
```
Send back `tier2-results/reconnect-<timestamp>.report.txt`, and `dmesg | tail -20` if a check fails.

What to read, per phase, against gwloss b_stress's 30j rerun (127s from one gateway's kill, loadgen-bound, `pending_mem` 0):
- `P_back.from_mark` p99 (the spec's "time until 99% are back") and `back_s`; the spec's estimate is users ÷ admitted login rate plus the backoff tail. `P_back.backoff_share` (Tier 3's < 1% is a later bound, not gated here) and `waits`.
- `P_back.loadgen_bound`: if true, the times are an upper bound, as b_stress's (#110). A failed `P_heartbeats` with `loadgen_bound` true is labelled (a `not_covered` line): read `lg_runq_ms_per_s` and `loadgen_set_busy_pct` in the series beside the Pong timeouts before ruling it a gateway fault (O3).
- #133: `P_back.rejects` (`rejects_pending_mem`, `logins_busy`, `rejects_login_queue`, `rejects_at_capacity` per gateway) and `P_back.funnel`. A funnel with `over_bar` true (`longest_s` ≥ 5s in a row) is the evidence #133 names for unparking 6b3b5 and #111: the user's call. One under the bar is recorded as "seen, under the bar" (O6).
- `at_capacity`: expected 0 in `reset` and `netchange` (the RST frees every slot; 200k per gateway against 250k); in `ghost` read it per gateway and per second beside Logic's Kicks (`P_back.kicks`), `closes_kick_per_s`, `closes_heartbeat_per_s` and `lg_ghosts` (O2).
- `P.series` per second: which came first of `gateway_set_busy_pct` ≥ 85, `logic_set_busy_pct` ≥ 85, `pending_mem_per_s` > 0 and `busy_per_s` > 0 (Review Focus 4); `logouts_dropped_per_s` (Review Focus 3).
- `P.redis_latency`: Redis's p99 per command beside `logic_set_busy_pct` (Redis shares Logic's set): the single-instance baseline for Plan 6c (O5).
- `P_sessions`: `missing`, `extra`, `wrong_gateway`, `stale` beside Logic's `logins_replaced`, `logouts_deleted`, `logouts_not_owner` (O4).
- `netchange` against `reset`: the 0–20s storm wait cut to 0–1s puts the arrivals into ~1s, so the admission control, not the jitter, shapes it. `ghost` against `netchange`: the cap and the Kicks, not the jitter, shape it; `ghosts.closed` says how many old connections a Kick closed and how many waited for the sweep (`silent`), and `no_swap_in_ghost` whether the peak fit (Review Focus 6).

- [ ] **Step 3: Record**

In this plan's *Execution Notes*: run 31's commit, duration, checks, and the readings of Step 2. In the roadmap: the plan 6 row (6b3c2 executed, run 31), and #133's outcome (funnel over the bar, under it, or not seen). The README's rows (English and 中文) only if the user asks: a new row for the whole-population reconnect.

```bash
git add docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-17-connection-session-core-06b3c2-reconnect.md
git commit -m "docs: record Tier 2 run 31 (reconnect)" -m "Refs: #133, #136 (Plan 6b3c2 Task 3)."
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6c** (Redis Cluster, `gwconns` shard load, ReconcileEnd/drain deadline sizing; run 31's `P.redis_latency` is its single-instance baseline; the session check's multi-key EVALs need a per-slot form there), then **6d** (Tier 3, k8s; #132's fresh-pod funnel and the LB-side remedies; the drop as a real LB restart).
- **#111 / 6b3b5 (parked):** run 31's `pending_mem` and funnel readings, against the bar, are their input (#133).
- **#117 (parked, cheaper Kicks):** `ghost`'s Kicks and their cost in run 31's profiles are an input.
- **The spec:** nothing here changes its text. The measured recovery and backoff share go with #97, #101, #125 to the next revision if they differ from the spec's estimate.

## Execution Notes

**Tasks 1–2 executed 2026-10-18** (inline, superpowers:executing-plans) on `feat/plan6b3` from `c221ab4`: `c32f14c` (Task 1), `e2e4102` (Task 2). Both patches applied cleanly on top of 6b3c1 (including its `e5f401d` fix).
- Task 1: the 7 new tests pass; `gofmt`, `go vet ./...` and `make test` (race) green; mutations 11/11 caught (each a test failure, not a build error).
- Task 2: `make test-scripts` all five passed (`lib_test` with the three `setup:` cases). Dry run (3,000 clients): exit 1, 34 checks, failing only `no_swap_in_reset` and `no_swap_in_ghost` (the dev swap). `reset` p99 19,894ms, `waits` `{"storm": 3000}`; `netchange` p99 1,452ms, `local` 2,961, network change at 277ms; `ghost` p99 1,761ms, `local` 2,965, network change at 198ms, ghosts `open` 0, `kick` 1,000 per gateway, `silent` 0. Sessions found 3,000 in each phase; funnel `over_bar` false.

**Final review (fresh Opus reviewer, 2026-10-18):** 0 Critical, 1 Important, 9 Minor.
- **I-1, fixed `e5c070d`:** the gateway answers each Ping with a Pong. A Pong that reaches a socket after `/ghost` stays unread, so the keeper reads `n > 0` and labels a ghost that the heartbeat sweep closed as `kick`. At 600k clients and a 30s heartbeat (~20k Pings/s), tens to hundreds of ghosts can hold such a Pong, and `ghost_heartbeats` then fails falsely (`closes_heartbeat > silent`). Now each gateway may close by heartbeat its ghosts (`kick` + `silent`) minus its own `closes_kick`, in the new `rc_heartbeats_ok` (5 cases in `reconnect_test.sh`; the first failed before the fix). Loadgen's `kick`/`silent` stays a reading only. This amends Global Constraints' "gateway heartbeat closes up to the silent ghosts". A `ghost`-only dry run after the fix: 12 checks, only `no_swap_in_ghost` failing.
- Minors, not fixed (the user's call):
  - M1: when `keep()`'s `EPOLL_CTL_ADD` fails (e.g. `fs.epoll.max_user_watches`), its fallback blocks the SDK's `Close` for 1s, closes the socket from loadgen's side and counts it `silent`.
  - M2: `abort()` closes the socket outside the `replyConn` state; a `/reset` after a `/ghost` on one runner would close a kept socket (the scenario never does this).
  - M3: a narrow race between `Close` and `ghost()` can close a socket instead of keeping it (then `P_drop` fails).
  - M4: the drop wait polls `/slo` every 0.2s, and each poll walks 600k members under `Recorder.mu` early in the storm (0.5s suggested).
  - M5: the `user@.service` drop-in applies to every user and raises the soft limit to 1,048,576 (`LimitNOFILE=1024:2097152` suggested).
  - M6: `fs.nr_open = 2097152` can lower a host value that systemd raised at boot (1073741816); check `sysctl fs.nr_open` on the host first.
  - M7: `daemon-reload` does not restart a running user manager, so a new login may not apply the drop-in; the `prlimit` fallback covers it.
  - M8: `rc_sessions_cmp` checks only that there are enough lines, not exactly one per client (a fail, not a false pass).
  - M9: `ErrNotSDK`'s text omits `/ghost`; an epoll failure in `Ghost` also gives 409.
- Not judged by the reviewer (host data, read in run 31): the 23GiB budget with ~1.2M loadgen sockets, the host's `epoll max_user_watches` and `file-max`, and whether a Kick frame always reaches the socket before the close under load.

**Run 31 (2026-10-18, `32e1dad`, 878s, host 30GB after the UMA change; Docker containers idle on the host, outside CPU 0.4–0.8 cpu-s per phase): 33/34.** Only `ghost_drop` failed, on a check bug: gw-0's periodic Reconcile asked Logic about 4,119 old connections still ONLINE; Logic answered `not_owner` (the relogins owned the sessions), and the gateway Kicked them with close reason `not_owner` (`reconcile.go:364`, by design). The check counted only `kick` + `heartbeat` (595,881 < 600,000; gw-0 122,403 + 4,119 + 73,478 = 200,000). Fixed `69557e7`: `closes_not_owner` joins `GW_COUNTERS`, `ghost_drop`'s sum and the heartbeat allowance (2 cases in `reconnect_test.sh`, the first failed before the fix). The user chose a `ghost`-only rerun.

**Run 31b (2026-10-18, `69557e7`, `RECONNECT_PHASES=ghost`, 348s): 12/12.** Closes: kick 123,826 / 123,524 / 125,831, `not_owner` 4,006 / 0 / 2,861, heartbeat 72,168 / 76,476 / 71,308 (gw-0/1/2); loadgen's ghosts `kick` 385,508, `silent` 214,492, `open` 0. The I-1 fix was needed: loadgen labelled ~8–10k more `kick` than the gateways counted (unread Pongs).

Readings (31 for `reset`/`netchange`, 31b for `ghost`; 31's `ghost` in brackets):

| | `reset` | `netchange` | `ghost` |
|---|---|---|---|
| drop: close / ghost ms; detection p99 | 2,441; 2.4s | 1,648; 1.6s | 603; 3.2s [610; 1.9s] |
| network change after / took | — | 1,701ms / 27.1s | 3,516ms / 35.7s [2,014 / 36.3s] |
| waits cut to `local` | — | 140,550 | 126,309 [160,238] |
| from mark p50 / p90 / p99 / max | 22.0 / 38.0 / 47.0 / 87.9s | 22.9 / 38.1 / 48.3 / 93.2s | 28.6 / 51.4 / 75.3 / 161.0s [27.4 / 50.6 / 69.0 / 140.8s] |
| all back (`back_s`) | 86s | 92s | 161s [140s] |
| backoff share | 10.2% | 11.9% | 29.5% [28.7%] |
| `no_slot` per gateway | 0.73–1.0M | 0.88–0.98M | 0.25–0.27M |
| `rejects_login_queue` (gw-0/1/2) | 31,111 / 5,378 / 4,770 | 1,468 / 1,501 / 23,329 | 0 |
| `at_capacity` per gateway | 0 | 0 | 0.92–0.93M [0.86–0.87M] |
| Logic Kicks ok | 142,485 | 139,101 | 373,918 [373,900] |
| `pending_mem`, funnel, Logouts dropped | 0 | 0 | 0 |
| loadgen peak RSS, swap-in | 15.3GB, 0 | 15.5GB, 0 | 16.2GB, 0 [15.8GB] |
| sessions found | 600,000 | 600,000 | 600,000 |

- Every phase was loadgen-bound (`storm_first30` loadgen 11.7–13.4 cores of 14; `lg_runq_ms_per_s` up to 1.5–1.7s/s): the times are upper bounds, as b_stress's (#110). `*_heartbeats` passed in all phases, so no labelled `not_covered` line.
- Which bound came first (Review Focus 4): the gateway set reached 98–99% busy from ~14s (`reset`), ~4s (`netchange`) and ~6s (`ghost`), with Logic's set at 99–100% at the same time; `pending_mem` never fired. In `ghost` the cap bound from ~6–9s (`open` 250,000 on each gateway) until the sweep at `DeadAfter` (~41–73s) freed it.
- `netchange` ≈ `reset`: `/network-change` reached all 600k clients only 27–36s after the mark (`eachSDK` over 600k `NetworkChanged` on a saturated loadgen), after most 0–20s storm waits had ended; it cut 126–160k waits. The spec's "0–1s instead of 0–20s" shape is not what Tier 2 measured; the bound is loadgen's own fan-out.
- `ghost`: ~62% of old connections were Kicked by a relogin (incl. `not_owner`), ~38% waited for the heartbeat sweep; `at_capacity` (~2.8M refusals in all) and backoff (29.5%) shaped the recovery, which took ~1.9× `reset`'s p99.
- Redis (single instance, Plan 6c's baseline): `evalsha` p99 75–94µs, p99.9 2.6–3.1ms; `sadd` p99 150–309µs; `hget`/`hset`/`hmget` p99 2–5µs.
- Deferred minors M1–M9 unchanged (the user's call).
- **M1–M9 fixed 2026-10-21** (`d99db0c`, branch `fix/deferred-minors`), before Plan 6c:
  - M1: a socket the keeper cannot take is closed at once and counted in the new `/stats` `ghosts_unkept`, not `silent`. `reconnect.sh` adds `unkept` to the ghost reading; the `kick + silent == population` rule then fails such a run. Test `TestGhostUnkeptClosesAtOnce` (net.Pipe path; the `EPOLL_CTL_ADD` failure path is not reached on dev).
  - M2: `abort()` acts only on `connOpen`. A test case in `TestGhostKeeperWaitsForThePeer` failed with the old `abort` (peer saw an RST).
  - M3: `Close` swaps the state in one CAS loop. `TestGhostCloseRace` (200k rounds, two goroutines at one barrier) failed on the old `Close` in each of 3 runs.
  - M4: the drop wait polls every 0.5s.
  - M5: the drop-in is now `user@UID.service.d`, `LimitNOFILE=1024:2097152`; host-setup removes the old every-user file. `--uid` added for test mode.
  - M6: host-setup leaves out any sysctl the host already has higher in every field (fs.nr_open: 1073741816 after systemd's boot). The guard still checks `>=` sysctl.conf.
  - M7: after `daemon-reload` host-setup reads the running user manager's `/proc/PID/limits` and says when it keeps the old limit.
  - M8: `rc_sessions_cmp` needs exactly one line per client (`splitlines`); new case "too many lines".
  - M9: `ErrNotSDK` names `/ghost`; a keeper failure is `ErrGhostKeeper`, 500. Test `TestGhostKeeperFailureIs500`.
  - Verified: `go vet`, `make test` (race, 20 packages), `make test-scripts` all passed. Run on the host before run 32: `sudo scripts/tier2/host-setup.sh` again (new drop-in path).
