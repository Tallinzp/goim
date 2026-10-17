# Plan 6b3b2: Run 30's Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `floods` a valid test on the Tier 2 host, then rerun it as run 30b. This plan carries #123's follow-ups to run 30 (failed 21/27): NOTRACK for the stack's ports on loopback, a loadgen pinger that cannot fall minutes behind, and the gates and records run 30 lacked.

**Architecture:** Four small changes, one task each.
- **The pinger.** Loadgen's light pinger picks slots by the clock instead of counting ticks. A late tick pings every slot due since the last one, at most one cycle's worth. The pinger records its own lag and its slowest `Send`.
- **The gateway.** It exports the node's `nf_conntrack_count`, as the spec's *limits* metrics name it.
- **NOTRACK.** `goim-netdrop` gains `notrack` and `notrack-check` (raw table, `CT --notrack`, ports 17000–17199 on `lo`). `host-setup.sh` applies the rules. The Tier 2 guard refuses a run while they are missing, as after a reboot, or while `STACK_PORT_BASE` puts the stack outside them.
- **`floods.sh`.** It gates `ping_gap_max_ms` and each phase's conntrack peak. It gives `storm_rate` a verdict (`met`, `loadgen-bound` or `short`), lets `garbage_limited` pass on either part of the limiter, and snapshots CPU sets and softnet drops per phase.

**Tech Stack:** Go 1.27.1 (`testing/synctest`, `prometheus/client_golang`), bash 5.2, python3 (JSON and arithmetic only), iptables (raw table, `xt_CT`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Deployment requirements → Production node and LB limits* (~l.76): "per gateway node, `nf_conntrack_max` ≥ 2 × cap, **or** conntrack disabled for the gateway port (`NOTRACK` / CNI equivalent), declared in the gateway config as `conntrack: notrack`".
- *Observability → limits* (~l.457): "measured `RLIMIT_NOFILE` and `nf_conntrack_max` as gauges, the node's `nf_conntrack_count` where readable (alert at 80% of max)".
- *Gateway → Readiness → Limits self-check* (~l.233): "`nf_conntrack_max` is node-wide; an absent file means conntrack isn't loaded and passes".
- *Client SDK rules*: the Pong timeout is one heartbeat interval + 10s. The gateway closes a connection silent for 2 × heartbeat + 10s.
- *Verification → Load tests*: the handshake-flood and slow-client-flood bullets that `floods` (#121, #122) runs.

Inputs: Plan 6b3b (`2026-10-16-connection-session-core-06b3b-floods.md`): its *Execution Notes*, run 30's record. The roadmap's #111 (parked), #122, #123.

**Decisions:** #123 (the user, 2026-10-16) made this plan: NOTRACK in `host-setup.sh`, the pinger's catch-up and a `ping_gap_max_ms` gate, softnet snapshots, a ruling on `storm`'s loadgen bound, and maybe a `nf_conntrack_count` gauge. Before writing, the user ruled on four open points (2026-10-16, with AskUserQuestion). They are proposed together as **#124** under *Implementation choices* and go to review as usual:
1. **`storm_rate`:** the check gates `new_per_s ≥ 0.9 × rate` as before. When loadgen used ≥ 85% of its CPUs, the check passes with verdict `loadgen-bound` and the rate goes to `not_covered`. It is not a gateway failure.
2. **`garbage_limited`:** gates the limiter as a whole, `no_slot + no_token > 0` per gateway. It records which part bound (`bound_by`), as #111's input.
3. **The pinger:** catch-up plus diagnostics (`ping_tick_lag_max_ms`, `ping_send_max_ms`). No write deadline.
4. **The gauge:** included (`goim_gateway_nf_conntrack_count`).

**Branch:** `feat/plan6b3`. Execute from its head, `4528a22` or later.

**Size:** 5 tasks, about 330 lines changed in 11 files. The prototype `proto-6b3b2` (from `4528a22`: `e9d387c` pinger, `3cd0a25` gauge, `1104a5f` NOTRACK, `c9211dc` floods) ran every block. Its code is replayed here byte for byte, as `git apply` patches applied in task order. On the dev machine it passed `make test-scripts`, `make test` and `go test -race` on the changed packages. The `floods` dry run passed all 30 checks except the dev machine's `no_swap_in_slow`. Forced failures were run for the new gates, and both storm verdicts were reached. The NOTRACK rules themselves can't run on the dev machine (no root, no user namespaces). Their command text is tested, and the host runs them in Task 5.

## Global Constraints

- `make test`, `make test-scripts` and `go vet ./...` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- Root actions stay in `goim-netdrop` (choice #68): fixed `PATH`, fixed iptables path, fixed rule shapes, no environment variable read. Its existing `add`/`del`/`flush`/`list` behaviour is unchanged, and `flush` never removes a NOTRACK rule.
- The NOTRACK range is 17000–17199 in both `goim-netdrop` (`NOTRACK_PORTS`) and `lib.sh` (`T2_NOTRACK_LO`/`T2_NOTRACK_HI`). A test checks they agree. The stack's highest port is `STACK_PORT_BASE + 132` (the 4th gateway's last, `stack_ports`), so base 17000–17067 is covered.
- Scripts: bash with `set -uo pipefail`, JSON without jq, python3 only for reading JSON and arithmetic. Remember that `t2_bool A || B` runs `||` outside `t2_bool`, so compound conditions go through `lg_true`.
- No change to the gateway's limits self-check or its flags. The stack's gateways are not passed `-conntrack-notrack`: `nf_conntrack_max` (4,194,304) passes the check anyway, and the flag would only skip a check that holds.
- `floods`' phases, sizes and defaults are unchanged (#122). Only gates and records change.

## Review Focus

1. **NOTRACK on loopback.** A locally sent packet meets raw `OUTPUT`, then raw `PREROUTING` when `lo` delivers it. The rules mark both, in both directions, so neither hook creates an entry. If the shape is wrong, the table fills again as in run 30. `<phase>_conntrack` (peak under 80% of max) catches that. Run 30b also reads its `peak` against the population: with NOTRACK working, the peak should stay far below the phase's connections (the host's own traffic only).
2. **A host firewall that matches on conntrack state.** NOTRACKed packets have state `UNTRACKED`, not `ESTABLISHED`. A host `INPUT` chain that accepts only `ESTABLISHED,RELATED` before accepting `lo` would drop the stack's traffic. Before run 30b, `sudo iptables -S INPUT` must show `lo` accepted before any `--ctstate` rule (ufw's `-i lo -j ACCEPT` comes first), or a policy of `ACCEPT`. `garbage_online`'s first dial would show such drops at once as dial timeouts.
3. **The catch-up burst.** After a long stall a pinger shard pings a whole cycle's slots at once. With 400k clients on loadgen's 8 CPUs (8 shards, one per GOMAXPROCS) that is up to 50k Pings in one burst per shard. That is what the clients are owed, and the gateway sees the same number of Pings per cycle. `ping_tick_lag_max_ms` says whether it happened. If it is large while `ping_send_max_ms` is small, the goroutine was starved (CPU, GC). If `ping_send_max_ms` is large, a `Send` blocked (a full socket buffer).
4. **The `loadgen-bound` threshold.** 85% of loadgen's CPUs was run 30's 6.88 of 8 (86%), so storm sits on the edge. A gateway-bound storm with loadgen also at ≥ 85% would pass as `loadgen-bound`. `storm_bucket` (no_token 0) and `storm_online` still gate. `storm_rate` records `gateway_cores` and the CPU sets' shares (`storm.cpu_sets`), and the record must read them before calling a pass `loadgen-bound`.
5. **`garbage_delivered`'s SYN loss.** Run 30 lost SYNs (`TCPSynRetrans` 437k, 29k dial timeouts) with no listen-queue drops and before the table filled. Its cause is open. Run 30b records `garbage_delivered.cpu_sets` (softnet drops and squeezes per CPU set) and `garbage_conntrack`. If it fails again with softnet drops on the gateway's CPUs, `netdev_max_backlog` is the next suspect. With neither, it is loadgen's in-flight cap (`skipped`). The gate is not changed here.

## Implementation choices this plan makes

1. **Slots follow the clock** (#124.3). Visit k (slot `k % len`) is due at `start + (k+1) × tick`, as before when no tick is lost. At each tick the pinger makes every visit due by now, starting from the oldest, but at most one cycle's, so each slot is pinged once however long the stall. `PingLag` records how late the oldest due visit was. `PingSend` times each `Send`. The `send` hook exists only so the test can block a `Send` on synctest's clock. *Alternative:* a write deadline on the pinger's `Send` (needs a `clientconn` API), not chosen. The diagnostics come first.
2. **`<phase>_heartbeats` gates `ping_gap_max_ms ≤ (HB + 5) × 1000`.** The gateway closes a client 2 × HB + 10s after its last frame. A Ping more than 5s late is a pinger that fell behind. Run 30's garbage phase read 30,125ms, its slow phase 126,280ms.
3. **`<phase>_conntrack`** samples `nf_conntrack_count` every second from the stack's start to its stop (`ct_start` in `up`, `ct_check` in `down`). The check passes if the peak is ≤ 80% of `nf_conntrack_max`, the spec's alert, or if conntrack isn't loaded (as the self-check does). `CT_PROC` names the proc directory, so the dry run can point it at fake files. That makes three new checks: 30 in all.
4. **The gauge** is a `GaugeFunc` registered in `Start` when the count can be read and parsed at startup. It reads the count at each scrape and reports NaN on a later failure. Where the file is unreadable, nothing is registered, which makes "where readable" literal. The name is `goim_gateway_nf_conntrack_count`. It is a separate gauge, not a `limit` label: it changes at runtime, while `limit` holds what the self-check read.
5. **The guard checks NOTRACK through `goim-netdrop notrack-check`**, which runs as root with `iptables -C`. The Tier 2 user can't read the raw table. The guard already uses `goim-netdrop list` the same way. `host-setup.sh` now fails, instead of warning, when iptables is missing, since NOTRACK needs it.

## File Structure

- `internal/loadgen/light.go`: `pinger.run` follows the clock, plus the `send` hook (Task 1).
- `internal/loadgen/recorder.go`: `PingLag`, `PingSend`, and `Counts.PingTickLagMaxMs`/`PingSendMaxMs` (Task 1).
- `internal/gateway/limits.go`, `server.go`: `conntrackCount`, registered in `Start` (Task 2).
- `scripts/tier2/goim-netdrop`, `host-setup.sh`, `lib.sh`: NOTRACK and the guard (Task 3).
- `scripts/tier2/floods.sh`: the gates and records (Task 4).
- Tests: `light_test.go`, `server_test.go`, `lib_test.sh`. `floods.sh` is checked by the dry run.

---

### Task 1: The light pinger follows the clock

**Files:**
- Modify: `internal/loadgen/light.go:169-257` (the pinger)
- Modify: `internal/loadgen/recorder.go` (two fields, two methods, two `Counts` fields)
- Test: `internal/loadgen/light_test.go`

**Interfaces:**
- Produces: `(*Recorder).PingLag(time.Duration)` and `(*Recorder).PingSend(time.Duration)`, each keeping the max. `Counts.PingTickLagMaxMs` (`json:"ping_tick_lag_max_ms"`) and `Counts.PingSendMaxMs` (`json:"ping_send_max_ms"`), in loadgen's `/stats` and result file. Task 4 reads them from the result file. `pinger.send func(*lightConn)` is for tests only.

- [ ] **Step 1: Write the failing test**

```bash
git apply <<'PATCH'
diff --git a/internal/loadgen/light_test.go b/internal/loadgen/light_test.go
index 20af0e7..61f2296 100644
--- a/internal/loadgen/light_test.go
+++ b/internal/loadgen/light_test.go
@@ -37,6 +37,47 @@ func TestPingerCountsMissingPongs(t *testing.T) {
 	}
 }
 
+// A pinger that falls behind pings, at its next tick, every slot whose time
+// has passed, each once however long the stall: slots follow the clock, not
+// the ticks it got (run 30: dropped ticks stretched a 30s cycle to 126s, and
+// the gateways closed 177,743 clients for their heartbeat). How late it was
+// and its slowest Send are recorded. The Send stub sleeps on synctest's clock.
+func TestPingerCatchesUpOnMissedSlots(t *testing.T) {
+	synctest.Test(t, func(t *testing.T) {
+		rec := NewRecorder(5, []string{"gw-0"})
+		p := newPinger(5*pingTick, rec) // five slots, one client each
+		var pings [5]int
+		p.send = func(lc *lightConn) {
+			pings[lc.i]++
+			if lc.i == 0 && pings[0] == 1 {
+				time.Sleep(10 * pingTick) // the first Send blocks for 1s
+			}
+		}
+		for i := range 5 {
+			p.add(&lightConn{i: i})
+		}
+		ctx, cancel := context.WithCancel(context.Background())
+		done := make(chan struct{})
+		go func() { p.run(ctx); close(done) }()
+		time.Sleep(12*pingTick + pingTick/2) // the stall ends at 1.1s
+		synctest.Wait()
+		for i, n := range pings {
+			if n < 1 || n > 2 {
+				t.Errorf("client %d pinged %d times by 1.25s, want 1 or 2", i, n)
+			}
+		}
+		c := rec.Counts()
+		if c.PingTickLagMaxMs < 900 {
+			t.Errorf("ping_tick_lag_max_ms %d, want >= 900", c.PingTickLagMaxMs)
+		}
+		if c.PingSendMaxMs < 1000 {
+			t.Errorf("ping_send_max_ms %d, want >= 1000", c.PingSendMaxMs)
+		}
+		cancel()
+		<-done
+	})
+}
+
 // A failed dial is counted by why it failed, so a run's report tells lost
 // SYNs (timeout) from refused connections and exhausted source ports (run 14).
 func TestDialFailuresAreCountedByReason(t *testing.T) {
PATCH
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/loadgen -run TestPingerCatchesUpOnMissedSlots -count=1`
Expected: build failure, `c.PingTickLagMaxMs undefined` and `c.PingSendMaxMs undefined`. With only the recorder's fields and the `send` hook added, and the old tick-counting loop kept, the prototype failed on behaviour: `client 3 pinged 0 times by 1.25s`, `client 4 pinged 0 times`, `ping_tick_lag_max_ms 0, want >= 900`.

- [ ] **Step 3: Implement the clock-driven pinger and the recorder's maxima**

```bash
git apply <<'PATCH'
diff --git a/internal/loadgen/light.go b/internal/loadgen/light.go
index 36822f7..7b6981f 100644
--- a/internal/loadgen/light.go
+++ b/internal/loadgen/light.go
@@ -167,21 +167,23 @@ func (lc *lightConn) read(rec *Recorder) string {
 }
 
 // pinger sends every light client a Ping each PingEvery, spread evenly:
-// clients sit in PingEvery/100ms slots by index, and one slot is pinged per
-// 100ms tick. A Ping due while the last one is unanswered is counted
-// missing.
+// clients sit in PingEvery/100ms slots by index, and one slot is due every
+// 100ms by the clock; a late tick pings every slot due since. A Ping due
+// while the last one is unanswered is counted missing.
 type pinger struct {
 	tick   time.Duration
 	rec    *Recorder
 	shards int // of the pingers it belongs to (1 alone)
 	mu     sync.Mutex
 	slots  []map[*lightConn]struct{}
+	send   func(*lightConn) // sends one Ping; tests replace it
 }
 
 const pingTick = 100 * time.Millisecond
 
 func newPinger(every time.Duration, rec *Recorder) *pinger {
-	p := &pinger{tick: pingTick, rec: rec, shards: 1, slots: make([]map[*lightConn]struct{}, max(1, int(every/pingTick)))}
+	p := &pinger{tick: pingTick, rec: rec, shards: 1, slots: make([]map[*lightConn]struct{}, max(1, int(every/pingTick))),
+		send: func(lc *lightConn) { lc.c.Send(clientpb.Cmd_CMD_PING, lc.reqID.Add(1), nil) }} // a failure ends read
 	for i := range p.slots {
 		p.slots[i] = map[*lightConn]struct{}{}
 	}
@@ -232,17 +234,29 @@ func (ps pingers) run(ctx context.Context) {
 func (p *pinger) run(ctx context.Context) {
 	t := time.NewTicker(p.tick)
 	defer t.Stop()
+	start, next := time.Now(), 0 // visit k (slot k % len) is due at start + (k+1) tick
 	var due []*lightConn
-	for n := 0; ; n++ {
+	for {
 		select {
 		case <-ctx.Done():
 			return
 		case <-t.C:
 		}
+		// Slots follow the clock: a Ticker drops the ticks a busy loop
+		// misses, so every visit due by now is made, at most one cycle's.
+		since := time.Since(start)
+		upto := int(since / p.tick)
+		if upto <= next {
+			continue
+		}
+		p.rec.PingLag(since - time.Duration(next+1)*p.tick)
+		next = max(next, upto-len(p.slots))
 		p.mu.Lock()
 		due = due[:0]
-		for lc := range p.slots[n%len(p.slots)] {
-			due = append(due, lc)
+		for ; next < upto; next++ {
+			for lc := range p.slots[next%len(p.slots)] {
+				due = append(due, lc)
+			}
 		}
 		p.mu.Unlock()
 		now := time.Now().UnixNano()
@@ -251,7 +265,9 @@ func (p *pinger) run(ctx context.Context) {
 				p.rec.PongMissing()
 			}
 			p.rec.PingGap(time.Duration(now - lc.lastPing.Swap(now)))
-			lc.c.Send(clientpb.Cmd_CMD_PING, lc.reqID.Add(1), nil) // a failure ends read
+			s := time.Now()
+			p.send(lc)
+			p.rec.PingSend(time.Since(s))
 		}
 	}
 }
diff --git a/internal/loadgen/recorder.go b/internal/loadgen/recorder.go
index c46b42c..25b39d1 100644
--- a/internal/loadgen/recorder.go
+++ b/internal/loadgen/recorder.go
@@ -32,6 +32,8 @@ type Recorder struct {
 	pongs          int64
 	missing        int64
 	gapMax         time.Duration
+	lagMax         time.Duration
+	sendMax        time.Duration
 	pongRTT        *hist
 	marks          []*mark // by index
 	byName         map[string]*mark
@@ -215,6 +217,22 @@ func (r *Recorder) PingGap(gap time.Duration) {
 	r.mu.Unlock()
 }
 
+// PingLag: a pinger reached a slot lag after its time (a dropped tick, a
+// starved goroutine); the longest is kept.
+func (r *Recorder) PingLag(lag time.Duration) {
+	r.mu.Lock()
+	r.lagMax = max(r.lagMax, lag)
+	r.mu.Unlock()
+}
+
+// PingSend: one Ping's Send took d (a full socket buffer blocks it); the
+// longest is kept.
+func (r *Recorder) PingSend(d time.Duration) {
+	r.mu.Lock()
+	r.sendMax = max(r.sendMax, d)
+	r.mu.Unlock()
+}
+
 // Mark records instant now as name, with the clients logged in on gateway
 // gw (-1: on any gateway) as its population, and returns its size. A client
 // belongs to its latest mark only. Names are unique. gw -1 is only for an
@@ -325,6 +343,8 @@ type Counts struct {
 	Pongs            int64            `json:"pongs"`
 	PongMissing      int64            `json:"pong_missing"`
 	PingGapMaxMs     int64            `json:"ping_gap_max_ms"`
+	PingTickLagMaxMs int64            `json:"ping_tick_lag_max_ms"` // a slot pinged late by the pinger itself
+	PingSendMaxMs    int64            `json:"ping_send_max_ms"`     // the slowest Ping Send
 	PongRTT          Dist             `json:"pong_rtt"`
 }
 
@@ -364,7 +384,8 @@ func (r *Recorder) Counts() Counts {
 		BackoffClients: r.backoffClients, BackoffWaits: map[string]int64{},
 		BackoffSinceMark: BackoffCount{Mark: r.since.Mark, Clients: r.since.Clients, Waits: map[string]int64{}},
 		StormSinceMark:   r.storm,
-		LoginLatency:     r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PingGapMaxMs: r.gapMax.Milliseconds(), PongRTT: r.pongRTT.dist(r.pongRTT.n)}
+		LoginLatency:     r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PingGapMaxMs: r.gapMax.Milliseconds(),
+		PingTickLagMaxMs: r.lagMax.Milliseconds(), PingSendMaxMs: r.sendMax.Milliseconds(), PongRTT: r.pongRTT.dist(r.pongRTT.n)}
 	for i, n := range r.online {
 		c.Online += n
 		c.OnlineByGateway[r.ids[i]] = n
PATCH
```

- [ ] **Step 4: Run the tests to make sure they pass**

Run: `gofmt -l internal/loadgen; go vet ./internal/loadgen && go test -race -count=1 ./internal/loadgen ./cmd/loadgen`
Expected: no `gofmt` output, both `ok`. `TestPingerRecordsTheLongestPingGap` (a 1s real-time stall) still passes, now through the catch-up.

Mutation (prototype): deleting the line `next = max(next, upto-len(p.slots))` fails the new test with `client 0 pinged 3 times by 1.25s, want 1 or 2`. Restore it.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/light.go internal/loadgen/recorder.go internal/loadgen/light_test.go
git commit -m "fix(loadgen): the light pinger's slots follow the clock; ping_tick_lag_max_ms, ping_send_max_ms (#123, run 30)"
```

### Task 2: The gateway exports `nf_conntrack_count`

**Files:**
- Modify: `internal/gateway/limits.go` (new `conntrackCount`)
- Modify: `internal/gateway/server.go:75-78` (`Start` registers it)
- Test: `internal/gateway/server_test.go`

**Interfaces:**
- Consumes: `limitSource.readFile`, `Config.Limits.CountPath` (already `/proc/sys/net/netfilter/nf_conntrack_count`, never read until now).
- Produces: `conntrackCount(path string, src limitSource) prometheus.Collector` (nil when unreadable or unparsable at startup). The metric is `goim_gateway_nf_conntrack_count`. Nothing in this plan's scripts reads it: `floods` reads `/proc` directly, since one host-wide sample per second serves every gateway.

- [ ] **Step 1: Write the failing tests**

```bash
git apply <<'PATCH'
diff --git a/internal/gateway/server_test.go b/internal/gateway/server_test.go
index e598429..a3125af 100644
--- a/internal/gateway/server_test.go
+++ b/internal/gateway/server_test.go
@@ -11,6 +11,7 @@ import (
 
 	pb "github.com/Tallinzp/goim/api/internalpb"
 	"github.com/prometheus/client_golang/prometheus"
+	"github.com/prometheus/client_golang/prometheus/testutil"
 )
 
 func TestLimitsSelfCheck(t *testing.T) {
@@ -85,6 +86,47 @@ func TestReadyOnlyAfterLogicAndFirstLease(t *testing.T) {
 	}
 }
 
+// The node's nf_conntrack_count is a gauge read at each scrape, where the
+// file is readable at startup, and absent where it is not (spec:
+// Observability, limits; run 30: the table filled unseen).
+func TestConntrackCountGauge(t *testing.T) {
+	n := "120\n"
+	c := conntrackCount("/count", limitSource{readFile: func(string) ([]byte, error) { return []byte(n), nil }})
+	if c == nil {
+		t.Fatal("a readable count has no gauge")
+	}
+	if v := testutil.ToFloat64(c); v != 120 {
+		t.Fatalf("gauge %v, want 120", v)
+	}
+	n = "121\n"
+	if v := testutil.ToFloat64(c); v != 121 {
+		t.Fatalf("gauge %v after the count changed, want 121", v)
+	}
+	for _, err := range []error{fs.ErrNotExist, errors.New("permission denied")} {
+		if conntrackCount("/count", limitSource{readFile: func(string) ([]byte, error) { return nil, err }}) != nil {
+			t.Fatalf("%v: a gauge, want none", err)
+		}
+	}
+	if conntrackCount("/count", limitSource{readFile: func(string) ([]byte, error) { return []byte("x\n"), nil }}) != nil {
+		t.Fatal("an unparsable count: a gauge, want none")
+	}
+}
+
+// Start registers the gauge, so /metrics serves it.
+func TestMetricsServeConntrackCount(t *testing.T) {
+	l := startLive(t, nil, nil) // okLimits: every limit file reads 4000000
+	_, admin := l.g.Addrs()
+	resp, err := http.Get("http://" + admin + "/metrics")
+	if err != nil {
+		t.Fatal(err)
+	}
+	body, _ := io.ReadAll(resp.Body)
+	resp.Body.Close()
+	if !strings.Contains(string(body), "goim_gateway_nf_conntrack_count 4e+06") {
+		t.Fatal("/metrics has no goim_gateway_nf_conntrack_count 4e+06")
+	}
+}
+
 func TestFailedLimitsKeepTheGatewayNotReady(t *testing.T) {
 	l := startLive(t, nil, func(g *Gateway, _ *fakeLogic) {
 		g.limits = limitSource{nofile: func() (uint64, error) { return 10, nil }, readFile: okLimits.readFile}
PATCH
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/gateway -run Conntrack -count=1`
Expected: build failure, `undefined: conntrackCount`.

- [ ] **Step 3: Implement `conntrackCount` and register it in `Start`**

```bash
git apply <<'PATCH'
diff --git a/internal/gateway/limits.go b/internal/gateway/limits.go
index c5a6045..05c6b29 100644
--- a/internal/gateway/limits.go
+++ b/internal/gateway/limits.go
@@ -9,6 +9,8 @@ import (
 	"strconv"
 	"strings"
 	"syscall"
+
+	"github.com/prometheus/client_golang/prometheus"
 )
 
 // limitSource reads the two limits the gateway can see (spec: Limits
@@ -64,3 +66,29 @@ func checkLimits(cfg Config, src limitSource, m *Metrics) error {
 	}
 	return errors.Join(errs...)
 }
+
+// conntrackCount is the node's nf_conntrack_count read from path at each
+// scrape (spec: Observability, limits), or nil when it can't be read at
+// startup: conntrack not loaded, or no permission. A later failed read
+// reports NaN.
+func conntrackCount(path string, src limitSource) prometheus.Collector {
+	read := func() (float64, error) {
+		b, err := src.readFile(path)
+		if err != nil {
+			return 0, err
+		}
+		n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
+		return float64(n), err
+	}
+	if _, err := read(); err != nil {
+		return nil
+	}
+	return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "goim_gateway", Name: "nf_conntrack_count",
+		Help: "The node's nf_conntrack_count, read at each scrape."}, func() float64 {
+		n, err := read()
+		if err != nil {
+			return math.NaN()
+		}
+		return n
+	})
+}
diff --git a/internal/gateway/server.go b/internal/gateway/server.go
index a95b257..c65bde3 100644
--- a/internal/gateway/server.go
+++ b/internal/gateway/server.go
@@ -76,6 +76,11 @@ func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
 	if g.limitsErr != nil {
 		g.log.Error("limits self-check failed: staying NotReady", "err", g.limitsErr)
 	}
+	if c := conntrackCount(g.cfg.Limits.CountPath, g.limits); c != nil {
+		if err := g.registry.Register(c); err != nil {
+			g.log.Warn("nf_conntrack_count not exported", "err", err)
+		}
+	}
 	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
 		cc, err := g.dial(target)
 		if err != nil {
PATCH
```

- [ ] **Step 4: Run the tests to make sure they pass**

Run: `gofmt -l internal/gateway; go vet ./internal/gateway && go test -race -count=1 ./internal/gateway/... ./cmd/gateway`
Expected: no `gofmt` output, three `ok`. On the dev machine `/proc/sys/net/netfilter/nf_conntrack_count` doesn't exist, so `cmd/gateway`'s real gateway registers nothing, and that is the "where readable" path.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/limits.go internal/gateway/server.go internal/gateway/server_test.go
git commit -m "feat(gateway): export the node's nf_conntrack_count where readable (spec: limits; run 30)"
```

### Task 3: NOTRACK for the stack's ports on loopback (#123)

**Files:**
- Modify: `scripts/tier2/goim-netdrop` (`notrack`, `notrack-check`, `--print notrack`)
- Modify: `scripts/tier2/host-setup.sh` (applies `notrack`, header, iptables now required)
- Modify: `scripts/tier2/lib.sh` (`T2_NOTRACK_LO`/`HI`, `t2_notrack_covers`, `t2_require_host_setup`)
- Test: `scripts/tier2/lib_test.sh`

**Interfaces:**
- Produces: `goim-netdrop notrack` adds the four rules if absent, idempotently. `goim-netdrop notrack-check` exits 0 only if all four are in place, and prints `missing: …` per absent rule. `goim-netdrop --print notrack` prints the commands. `t2_notrack_covers BASE` prints a refusal and returns 1 when `BASE` < 17000 or `BASE + 132` > 17199. `t2_require_host_setup` refuses (exit 2) when NOTRACK is missing or the base is outside the range. Every load scenario already calls it.

- [ ] **Step 1: Write the failing tests**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/lib_test.sh b/scripts/tier2/lib_test.sh
index d46ef2b..b8a00c2 100755
--- a/scripts/tier2/lib_test.sh
+++ b/scripts/tier2/lib_test.sh
@@ -241,13 +241,26 @@ expect "netdrop: add" "$(scripts/tier2/goim-netdrop --print add 21100)" \
 "iptables -w -I INPUT -i lo -p tcp --dport 21100 -m comment --comment goim-netdrop -j DROP
 iptables -w -I INPUT -i lo -p tcp --sport 21100 -m comment --comment goim-netdrop -j DROP"
 contains "netdrop: del" "$(scripts/tier2/goim-netdrop --print del 21100)" "iptables -w -D INPUT -i lo -p tcp --dport 21100"
-for bad_args in "add 80" "add 01167" "add 70000" "add 21100x" "add" "add 1 2" "drop 21100" "--print flush"; do
+# NOTRACK for the stack's ports on lo (#123): both directions, both chains a
+# loopback packet meets, the range lib.sh's guard checks the ports against.
+expect "netdrop: notrack" "$(scripts/tier2/goim-netdrop --print notrack)" \
+"iptables -w -t raw -I PREROUTING -i lo -p tcp --dport 17000:17199 -m comment --comment goim-notrack -j CT --notrack
+iptables -w -t raw -I PREROUTING -i lo -p tcp --sport 17000:17199 -m comment --comment goim-notrack -j CT --notrack
+iptables -w -t raw -I OUTPUT -o lo -p tcp --dport 17000:17199 -m comment --comment goim-notrack -j CT --notrack
+iptables -w -t raw -I OUTPUT -o lo -p tcp --sport 17000:17199 -m comment --comment goim-notrack -j CT --notrack"
+contains "netdrop: notrack covers lib.sh's range" "$(scripts/tier2/goim-netdrop --print notrack)" "--dport $T2_NOTRACK_LO:$T2_NOTRACK_HI "
+t2_notrack_covers 17000 >/dev/null; expect "notrack range: the default base" "$?" 0
+t2_notrack_covers 17067 >/dev/null; expect "notrack range: 4 gateways' last port is 17199" "$?" 0
+why=$(t2_notrack_covers 17068); expect "notrack range: past the end refused" "$?" 1
+contains "notrack range: says the range" "$why" "17000-17199"
+t2_notrack_covers 16999 >/dev/null; expect "notrack range: below refused" "$?" 1
+for bad_args in "add 80" "add 01167" "add 70000" "add 21100x" "add" "add 1 2" "drop 21100" "--print flush" "notrack 17000" "notrack-check x" "--print notrack-check"; do
   # shellcheck disable=SC2086
   scripts/tier2/goim-netdrop $bad_args >/dev/null 2>&1; expect "netdrop: refuses '$bad_args'" "$?" 2
 done
 # Without root, iptables fails: list and flush must say so, not exit 0.
 if [ "$(id -u)" != 0 ]; then
-  for c in list flush; do
+  for c in list flush notrack notrack-check; do
     scripts/tier2/goim-netdrop $c >/dev/null 2>&1 && bad "netdrop: $c exits 0 when iptables fails" || ok "netdrop: $c fails when iptables fails"
   done
 fi
PATCH
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `scripts/tier2/lib_test.sh 2>&1 | grep -E 'FAIL|unbound|lib_test:'`
Expected: `FAIL netdrop: notrack: got [], want [iptables -w -t raw -I PREROUTING …]`, then `T2_NOTRACK_LO: unbound variable` (the test stops there).

- [ ] **Step 3: Implement `notrack`/`notrack-check`, the setup step and the guard**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/goim-netdrop b/scripts/tier2/goim-netdrop
index 3093324..7fdcaf6 100755
--- a/scripts/tier2/goim-netdrop
+++ b/scripts/tier2/goim-netdrop
@@ -8,18 +8,30 @@
 #   sudo -n /usr/local/sbin/goim-netdrop del PORT   remove that port's rules
 #   sudo -n /usr/local/sbin/goim-netdrop flush      remove every goim-netdrop rule
 #   sudo -n /usr/local/sbin/goim-netdrop list       show them
-#   goim-netdrop --print add|del PORT               print the iptables commands, run nothing
+#   sudo -n /usr/local/sbin/goim-netdrop notrack        untrack the stack's ports on lo (#123)
+#   sudo -n /usr/local/sbin/goim-netdrop notrack-check  exit 0 only if every NOTRACK rule is in place
+#   goim-netdrop --print add|del PORT | notrack     print the iptables commands, run nothing
 #
 # It runs only iptables, from a fixed path, with a fixed rule shape: DROP in
 # INPUT on lo, by destination port (client to gateway) and by source port
 # (gateway to client), tagged with the comment goim-netdrop. PORT must be
 # 1024-65535. It reads no environment variable.
+#
+# NOTRACK (#123): flood churn filled the host's conntrack table with
+# TIME_WAIT entries in run 30 and SYNs were dropped, so the stack's ports,
+# 17000-17199 (STACK_PORT_BASE's default, up to 4 gateways), skip conntrack
+# on lo, as the spec allows a gateway (conntrack: notrack). The raw table's
+# CT --notrack in PREROUTING and OUTPUT, both directions, tagged goim-notrack;
+# flush leaves them. iptables rules do not survive a reboot: host-setup.sh
+# adds them, and lib.sh's guard refuses a run until they are back.
 set -eu
 PATH=/usr/sbin:/usr/bin:/sbin:/bin
 IPT=/usr/sbin/iptables
 TAG=goim-netdrop
+NOTRACK_TAG=goim-notrack
+NOTRACK_PORTS=17000:17199 # lib.sh's T2_NOTRACK_LO:T2_NOTRACK_HI
 
-usage() { echo "usage: goim-netdrop [--print] add|del PORT | flush | list" >&2; exit 2; }
+usage() { echo "usage: goim-netdrop [--print] add|del PORT | notrack | flush | list | notrack-check" >&2; exit 2; }
 
 print=0
 if [ "${1:-}" = --print ]; then print=1; shift; fi
@@ -33,6 +45,15 @@ fi
 
 # rule DIR: the match for one direction.
 rule() { echo "-i lo -p tcp --$1 $port -m comment --comment $TAG -j DROP"; }
+# notrack_rules: "CHAIN MATCH" per NOTRACK rule, one per line.
+notrack_rules() {
+  local chain dir
+  for chain in "PREROUTING -i lo" "OUTPUT -o lo"; do
+    for dir in dport sport; do
+      echo "$chain -p tcp --$dir $NOTRACK_PORTS -m comment --comment $NOTRACK_TAG -j CT --notrack"
+    done
+  done
+}
 
 case $cmd in
   add|del)
@@ -62,6 +83,28 @@ case $cmd in
     printf '%s\n' "$rules" | grep -- "--comment $TAG -j DROP\$" |
       while read -r -a r; do r[0]=-D; $IPT -w "${r[@]}"; done
     ;;
+  notrack)
+    [ $# -eq 1 ] || usage
+    notrack_rules | while read -r chain spec; do
+      # shellcheck disable=SC2086
+      if [ $print = 1 ]; then
+        echo "iptables -w -t raw -I $chain $spec"
+      else
+        $IPT -w -t raw -C $chain $spec 2>/dev/null || $IPT -w -t raw -I $chain $spec
+      fi
+    done
+    ;;
+  notrack-check)
+    [ $# -eq 1 ] && [ $print = 0 ] || usage
+    notrack_rules | {
+      ok=0
+      while read -r chain spec; do
+        # shellcheck disable=SC2086
+        $IPT -w -t raw -C $chain $spec 2>/dev/null || { echo "missing: -t raw $chain $spec"; ok=1; }
+      done
+      exit $ok
+    }
+    ;;
   list)
     [ $# -eq 1 ] && [ $print = 0 ] || usage
     rules=$($IPT -w -S INPUT)
diff --git a/scripts/tier2/host-setup.sh b/scripts/tier2/host-setup.sh
index f7169e7..52a1bfc 100755
--- a/scripts/tier2/host-setup.sh
+++ b/scripts/tier2/host-setup.sh
@@ -11,6 +11,9 @@
 #   /usr/local/sbin/goim-netdrop           root-owned, 0755 (the b2 DROP rule only)
 #   /etc/sudoers.d/goim-netdrop            $SUDO_USER may run goim-netdrop, and only
 #                                          it, without a password (checked with visudo)
+#   NOTRACK rules for ports 17000-17199 on lo (goim-netdrop notrack, #123): not
+#                                          kept across a reboot, so run it again
+#                                          after one (the Tier 2 guard says so)
 # Everything else the runs need (nofile up to the hard limit, loopback
 # source addresses, CPU pinning) needs no root.
 #
@@ -66,5 +69,11 @@ if [ -n "$root" ]; then
 fi
 grep -vE '^[[:space:]]*(#|$)' "$here/modules.conf" | while read -r m; do modprobe "$m"; done
 sysctl -q -p /etc/sysctl.d/90-goim-tier2.conf
-command -v iptables >/dev/null || echo "host-setup: warning: iptables is not installed; goim-netdrop needs it (apt install iptables)" >&2
-echo "host-setup: done. Check: sudo -n /usr/local/sbin/goim-netdrop list"
+if [ -x /usr/sbin/iptables ]; then
+  /usr/local/sbin/goim-netdrop notrack
+  echo "applied: NOTRACK for ports 17000-17199 on lo"
+else
+  echo "host-setup: iptables is not installed; goim-netdrop needs it (apt install iptables), then run this again" >&2
+  exit 1
+fi
+echo "host-setup: done. Check: sudo -n /usr/local/sbin/goim-netdrop list; sudo -n /usr/local/sbin/goim-netdrop notrack-check"
diff --git a/scripts/tier2/lib.sh b/scripts/tier2/lib.sh
index f5b961d..85f57af 100644
--- a/scripts/tier2/lib.sh
+++ b/scripts/tier2/lib.sh
@@ -544,17 +544,36 @@ t2_netdrop_leftover() {
   fi
 }
 
+# The ports goim-netdrop's NOTRACK rules cover on lo (#123), as its
+# NOTRACK_PORTS: STACK_PORT_BASE's default and up to 4 gateways.
+T2_NOTRACK_LO=17000 T2_NOTRACK_HI=17199
+
+# t2_notrack_covers BASE: refuses (returns 1) a STACK_PORT_BASE whose ports
+# (base … base + 132, the 4th gateway's last, stack.sh's stack_ports) leave
+# the NOTRACK range: they would be tracked again, unseen.
+t2_notrack_covers() {
+  if [ "$1" -lt $T2_NOTRACK_LO ] || [ $(($1 + 132)) -gt $T2_NOTRACK_HI ]; then
+    echo "refused: STACK_PORT_BASE $1 puts the stack's ports outside $T2_NOTRACK_LO-$T2_NOTRACK_HI, the ports host-setup.sh untracks (use $T2_NOTRACK_LO-$((T2_NOTRACK_HI - 132)))"
+    return 1
+  fi
+}
+
 # t2_require_host_setup: load scenarios call it after t2_init; refuses
-# (exit 2) until host-setup.sh has run with this checkout's files, and while
-# an earlier run's DROP rules are left.
+# (exit 2) until host-setup.sh has run with this checkout's files and its
+# NOTRACK rules are in place (they do not survive a reboot), while an
+# earlier run's DROP rules are left, and for a STACK_PORT_BASE outside them.
 t2_require_host_setup() {
   local missing rules why
   missing=$(t2_host_setup_missing scripts/tier2 /proc/sys /sys/module /usr/local/sbin/goim-netdrop)
   if [ -z "$missing" ] && ! rules=$(sudo -n /usr/local/sbin/goim-netdrop list 2>/dev/null); then
     missing="sudo -n /usr/local/sbin/goim-netdrop fails (no sudoers line?)"
   fi
+  if [ -z "$missing" ] && ! sudo -n /usr/local/sbin/goim-netdrop notrack-check >/dev/null 2>&1; then
+    missing="the NOTRACK rules for ports $T2_NOTRACK_LO-$T2_NOTRACK_HI on lo are missing (not kept across a reboot)"
+  fi
   if [ -n "$missing" ]; then
     t2_refuse "$(printf 'refused: the host is not set up for load tests:\n%s\nrun once: sudo scripts/tier2/host-setup.sh' "$missing")"
   fi
   why=$(t2_netdrop_leftover "$rules") || t2_refuse "$why"
+  why=$(t2_notrack_covers "${STACK_PORT_BASE:-17000}") || t2_refuse "$why"
 }
PATCH
```

- [ ] **Step 4: Run the tests to make sure they pass**

Run: `scripts/tier2/lib_test.sh 2>&1 | grep -E 'FAIL|notrack|lib_test:'`
Expected: no `FAIL`. Twelve `ok` lines naming `notrack` (the four refusals among them, and `netdrop: notrack fails when iptables fails` and `netdrop: notrack-check fails when iptables fails` without root), then `lib_test: all passed`. Also `bash -n scripts/tier2/goim-netdrop scripts/tier2/host-setup.sh`.

- [ ] **Step 5: Commit**

```bash
git add scripts/tier2/goim-netdrop scripts/tier2/host-setup.sh scripts/tier2/lib.sh scripts/tier2/lib_test.sh
git commit -m "feat(tier2): NOTRACK for the stack's ports on lo, applied by host-setup.sh and required by the guard (#123)"
```

### Task 4: `floods`: run 30's gates and records

**Files:**
- Modify: `scripts/tier2/floods.sh` (header, `up`/`down`, `ct_start`/`ct_check`, `heartbeats`, `phase_storm`, `phase_garbage`, `phase_slow`)

**Interfaces:**
- Consumes: Task 1's `ping_gap_max_ms`, `ping_tick_lag_max_ms`, `ping_send_max_ms` in the light instance's result file. `t2_cpustat_snap`/`t2_cpusets_json`, `t2_cpulist_expand`, `T2_CPUS_LOADGEN` (`lib.sh`), `lg_true`/`lg_py` (`loadgen.sh`).
- Produces: checks `storm_conntrack`, `garbage_conntrack`, `slow_conntrack` (30 checks in all). `storm_rate.verdict`, `storm_rate.loadgen_cpus`, `garbage_limited.refused.<gw>.bound_by`, the `<phase>_heartbeats` pinger fields, `cpu_sets` in `storm`, `garbage`, `slow` and in `garbage_delivered`.

- [ ] **Step 1: Apply the changes**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/floods.sh b/scripts/tier2/floods.sh
index 0cbc605..4dd6029 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -69,7 +69,10 @@
 #                         them (so the limiter saw the storm, not a
 #                         loadgen-bound trickle that retries inflate: final
 #                         review I-1); the seen and completed (ok) rates and
-#                         the per-second series are recorded
+#                         the per-second series are recorded. A loadgen at
+#                         >= 85% of its CPUs is the bound, not the gateway
+#                         (run 30: 6.88 of 8): the check passes with verdict
+#                         "loadgen-bound" and the rate goes to not_covered
 #   storm_bucket          the bucket refused nothing (no_token 0, #121);
 #                         the concurrency limit's refusals (no_slot, each a
 #                         pre-handshake close the client retries after
@@ -84,16 +87,22 @@
 #                         no_token >= 0.95 x rate x seconds (6b3a Review
 #                         Focus 2); outcomes, TIME_WAIT and kernel TCP
 #                         counters are recorded (Review Focus 3)
-#   garbage_limited       each gateway's bucket refused part of the flood
-#                         (no_token > 0): it ran past the bucket. The slot is
-#                         taken first, so the bucket sees only what the
-#                         concurrency limit lets through; its no_slot and
-#                         the DHs per second are recorded (O3)
+#   garbage_limited       each gateway's limiter refused part of the flood
+#                         (no_slot + no_token > 0): it ran past the limiter.
+#                         The slot is taken first, so the bucket sees only
+#                         what the concurrency limit lets through; on the
+#                         Tier 2 host the slots bind first (~16k DH/s on 3,
+#                         run 30), so which part bound and the DHs per second
+#                         are recorded (#111's input)
 #   garbage_heartbeats    every Ping answered (no Pong missing, at least as
 #                         many Pongs over the light run as clients online: a
 #                         floor, not per client), the slowest Pong under 10s
-#                         (the SDK's Pong timeout), no connection lost, and no
-#                         gateway closed one for its heartbeat
+#                         (the SDK's Pong timeout), no connection lost, no
+#                         gateway closed one for its heartbeat, and no client
+#                         waited more than the interval + 5s for its next
+#                         Ping (ping_gap_max_ms: run 30's pinger fell 126s
+#                         behind); the pinger's own lag and slowest Send are
+#                         recorded
 #   garbage_lease         each gateway renewed its lease every interval
 #                         through the flood, and Logic saw no renewal error
 #   no_swap_in_garbage    no page swapped in over the flood
@@ -115,11 +124,19 @@
 #                         recorded
 #   slow_heartbeats, slow_lease   as garbage's, over the floods
 #   no_swap_in_slow       no page swapped in from the floods' start to their stop
+#   <phase>_conntrack     the host's nf_conntrack_count, sampled every second
+#                         from the stack up to its stop, peaked under 80% of
+#                         nf_conntrack_max (the spec's alert): the stack's
+#                         ports are untracked (#123), and run 30's slow phase
+#                         ran on a full table unseen; passes with no conntrack
+#                         loaded
 #   <phase>_idle          after loadgen stops, every gateway reaches 0
 #                         connections and settles its Logouts
 #   <phase>_processes     nothing died, panicked or stopped uncleanly
 #   artifacts             metrics before and after each phase's window, the
 #                         garbage flood's CPU profiles, loadgen's results
+# Each phase also records its CPU sets' shares and softnet drops over its
+# window (cpu_sets: run 30's garbage SYN loss had none snapshotted).
 set -uo pipefail
 
 . "$(dirname "$0")/lib.sh"
@@ -158,6 +175,8 @@ t2_on_exit 'lg_stop_all' # newest first: every loadgen instance stops before the
 smp="" prof=""
 t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
 t2_on_exit '[ -n "$prof" ] && kill "$prof" 2>/dev/null'
+cts=""
+t2_on_exit '[ -n "$cts" ] && kill "$cts" 2>/dev/null'
 
 GW_COUNTERS=(
   hs_ok='goim_gateway_handshakes_total:result="ok"'
@@ -200,11 +219,12 @@ up() {
   stack_up 2>"$STACK_DIR/stack-up.err"
   local rc=$?
   t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
+  [ $rc -eq 0 ] && ct_start
   return $rc
 }
 
 # down NAME: every loadgen stopped, the gateways idle, the stack stopped;
-# checks NAME_idle and NAME_processes.
+# checks NAME_conntrack, NAME_idle and NAME_processes.
 down() {
   lg_stop_all
   local t=$SECONDS idle stop_s problems
@@ -212,10 +232,31 @@ down() {
   stop_s=$((SECONDS - t))
   t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
   stack_down
+  ct_check "$1"
   problems=$(stack_health)
   t2_check "${1}_processes" "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
 }
 
+# ct_start: samples the host's nf_conntrack_count ("unix_s count") every
+# second into $STACK_DIR/conntrack.tsv until ct_check.
+CT_PROC=/proc/sys/net/netfilter
+ct_start() {
+  while :; do
+    echo "$(date +%s) $(cat "$CT_PROC/nf_conntrack_count" 2>/dev/null)"
+    sleep 1
+  done >"$STACK_DIR/conntrack.tsv" &
+  cts=$!
+}
+# ct_check PHASE: stops the sampler; checks PHASE_conntrack.
+ct_check() {
+  kill "$cts" 2>/dev/null; wait "$cts" 2>/dev/null; cts=""
+  local max peak
+  max=$(cat "$CT_PROC/nf_conntrack_max" 2>/dev/null || echo null)
+  peak=$(awk 'NF == 2 { if ($2 + 0 > p) p = $2 + 0; n++ } END { print n ? p : "null" }' "$STACK_DIR/conntrack.tsv" 2>/dev/null)
+  t2_check "${1}_conntrack" "$(echo "{\"peak\": ${peak:-null}, \"max\": $max}" | lg_true - 'd["max"] is None or (d["peak"] is not None and d["peak"] <= 0.8 * d["max"])')" \
+    "\"peak\": ${peak:-null}, \"max\": $max, \"samples\": $(wc -l <"$STACK_DIR/conntrack.tsv" 2>/dev/null || echo 0)"
+}
+
 # gws_counters: {"gw-0": stack_counters, ...} of every gateway; gws_delta A B
 # the same of two readings.
 gws_counters() {
@@ -338,12 +379,14 @@ lease() {
 
 # heartbeats PHASE N DELTA: checks PHASE_heartbeats over the light result of
 # N clients online (instance main, stopped) and the gateways' counter delta.
+# The gateway closes a client 2 x interval + 10s after its last frame, so a
+# Ping more than the interval + 5s late is a pinger that fell behind.
 # Pongs >= N is a floor over the whole light run, not per client: pong_missing
 # and lost catch a Ping left unanswered in the flood (O5).
 heartbeats() {
   local r=$STACK_DIR/artifacts/loadgen-$LG_NAME.json
-  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3}" | lg_true - 'd["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values())')" \
-    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}')"
+  t2_check "${1}_heartbeats" "$(echo "{\"r\": $(lg_py "$r" d), \"g\": $3}" | lg_true - 'd["r"]["pong_missing"] == 0 and d["r"]["pongs"] >= '"$2"' and d["r"]["pong_rtt"]["max_ms"] < 10000 and d["r"]["lost"] == {} and all(v["closes_heartbeat"] == 0 for v in d["g"].values()) and d["r"]["ping_gap_max_ms"] <= '"$(((HB + 5) * 1000))")" \
+    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ping_gap_max_ms\": $(lg_json 'd["ping_gap_max_ms"]'), \"ping_tick_lag_max_ms\": $(lg_json 'd["ping_tick_lag_max_ms"]'), \"ping_send_max_ms\": $(lg_json 'd["ping_send_max_ms"]')"
 }
 
 t=$SECONDS
@@ -357,12 +400,13 @@ missing=""
 
 # storm: #121 (2), a legitimate 20k/s storm on one gateway's default limiter.
 phase_storm() {
-  local n=${FLOODS_STORM_CLIENTS:-200000} rate=${FLOODS_STORM_RATE:-20000} s0 s1 d usage online hs
+  local n=${FLOODS_STORM_CLIENTS:-200000} rate=${FLOODS_STORM_RATE:-20000} s0 s1 d usage online hs rated
   STACK_GATEWAYS=1 STACK_GATEWAY_ARGS=(-max-conns $((n + n / 10)))
   up storm || { stack_down; return; }
   stack_snapshot before
   s0=$(gws_counters)
   t2_record_start storm
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
   lg_start storm -mode light -clients "$n" -rate "$rate" -uid-prefix storm
   stack_usage_start loadgen="$LG_PID"
   hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" &
@@ -370,6 +414,7 @@ phase_storm() {
   online=$(t2_bool lg_wait "$(settled "$n")" $((n / rate * 3 + 60)))
   kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
   usage=$(stack_usage_json)
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
   s1=$(gws_counters)
   t2_record_end storm
   stack_snapshot after
@@ -378,11 +423,15 @@ phase_storm() {
   [ "$online" = true ] && online=$(lg_get /stats "$(logged_in "$n")")
   [ "$online" = true ] || online=false # a null /stats reading fails the check, not the run
   t2_check storm_online "$online" "\"want\": $n, \"online\": $(lg_get /stats 'd["online"]'), \"gave_up\": $(lg_get /stats 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_get /stats 'd["failed"]'), \"login_latency\": $(lg_get /stats 'd["login_latency"]')"
-  t2_check storm_rate "$(echo "$hs" | lg_true - 'd["new_per_s"] >= 0.9 * '"$rate")" \
-    "\"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("new_per_s", "samples", "seen_per_s", "rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
+  # The verdict: met, loadgen-bound (loadgen at >= 85% of its CPUs) or short.
+  rated=$(echo "{\"h\": $hs, \"u\": $usage, \"cpus\": $(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)}" | lg_py - \
+    '"met" if d["h"]["new_per_s"] >= 0.9 * '"$rate"' else "loadgen-bound" if d["u"]["loadgen"]["cores"] >= 0.85 * d["cpus"] else "short"')
+  [ "$rated" = '"loadgen-bound"' ] && t2_not_covered "storm: $rate new clients/s at the gateway; loadgen was the bound (storm_rate.loadgen_cores)"
+  t2_check storm_rate "$(echo "$rated" | lg_true - 'd in ("met", "loadgen-bound")')" \
+    "\"verdict\": $rated, \"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("new_per_s", "samples", "seen_per_s", "rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"loadgen_cpus\": $(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w), \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
   t2_check storm_bucket "$(echo "$d" | lg_true - 'd["gw-0"]["hs_no_token"] == 0')" \
     "\"gw-0\": $(echo "$d" | lg_py - '{k: d["gw-0"][k] for k in ("hs_ok", "hs_no_token", "hs_no_slot", "rejects_handshake_rate", "logins_busy", "rejects_pending_mem", "rejects_at_capacity")}'), \"no_slot_share\": $(echo "$d" | lg_py - 'round(d["gw-0"]["hs_no_slot"] / (d["gw-0"]["hs_ok"] + d["gw-0"]["hs_no_slot"]), 4)'), \"failed\": $(lg_get /stats 'd["failed"]')"
-  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
+  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
   down storm
 }
 
@@ -405,6 +454,7 @@ phase_garbage() {
   stack_snapshot before
   s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   t2_tcpstat_snap "$STACK_DIR/tcp0"
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
   tw0=$(timewait)
   t2_record_start garbage
   t0=$SECONDS
@@ -418,6 +468,7 @@ phase_garbage() {
   s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   lease_s=$((SECONDS - t0))
   t2_tcpstat_snap "$STACK_DIR/tcp1"
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
   tw1=$(timewait)
   lg_in flood lg_stop
   wait "$prof"
@@ -429,12 +480,12 @@ phase_garbage() {
   flood=$(lg_py "$STACK_DIR/artifacts/loadgen-garbage.json" 'd["flood"]')
   want=$(python3 -c 'import sys; print(int(0.95 * int(sys.argv[1]) * int(sys.argv[2])))' "$grate" "$secs")
   t2_check garbage_delivered "$(echo "{\"g\": $d, \"f\": $flood}" | lg_true - 'd["f"]["attempts"] >= '"$want"' and sum(v["hs_failed"] + v["hs_no_slot"] + v["hs_no_token"] for v in d["g"].values()) >= '"$want")" \
-    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1")"
-  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_token"] > 0 for v in d.values())')" \
-    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"]} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
+    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1"), \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1")"
+  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_slot"] + v["hs_no_token"] > 0 for v in d.values())')" \
+    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"], "bound_by": "bucket" if v["hs_no_token"] > 0 else "slots" if v["hs_no_slot"] > 0 else None} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
   heartbeats garbage "$on" "$d"
   lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
-  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d, \"flood\": $flood}"
+  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"gateways\": $d, \"flood\": $flood}"
   down garbage
   for f in before/gw-0 after/gw-0 garbage/gw-0.cpu.pb.gz garbage/gw-1.cpu.pb.gz garbage/loadgen-flood.cpu.pb.gz; do
     case $f in
@@ -454,6 +505,7 @@ phase_slow() {
   stack_snapshot before
   s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   t2_record_start slow
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
   t0=$SECONDS
   lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
   lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
@@ -472,6 +524,7 @@ phase_slow() {
   lg_in flood2 lg_stop
   s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   lease_s=$((SECONDS - t0))
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
   t2_record_end slow
   stack_snapshot after
   lg_stop
@@ -489,7 +542,7 @@ phase_slow() {
     "\"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
   heartbeats slow "$on" "$d"
   lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
-  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d}"
+  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"gateways\": $d}"
   down slow
   for f in before/gw-0 after/gw-0; do
     grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
PATCH
```

- [ ] **Step 2: Check the syntax**

Run: `bash -n scripts/tier2/floods.sh && grep -c 't2_bool .* || ' scripts/tier2/floods.sh`
Expected: no syntax error and `0` (no `t2_bool A || B`).

- [ ] **Step 3: Dry run on the dev machine**

The same throwaway copy as Plan 6b3b's Task 2 Step 3: the guard lowered, the root parts removed, the scale cut. The copy lives in the session scratchpad (`$S`), not `/tmp`.

```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/floods.sh "$D/floods.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3 ss$|t2_need curl taskset python3 ss|' "$D/floods.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/floods.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/floods.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/floods.sh"
rm -rf tier2-results/floods-*
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=0-1 T2_CPUS_LOGIC=0-1 T2_CPUS_LOADGEN=0-1 \
  FLOODS_STORM_CLIENTS=2000 FLOODS_STORM_RATE=1000 FLOODS_CLIENTS=2000 FLOODS_RATE=1000 \
  FLOODS_GARBAGE_RATE=2000 FLOODS_GARBAGE_INFLIGHT=500 FLOODS_GARBAGE_SECONDS=45 FLOODS_GARBAGE_BUCKET=500 \
  FLOODS_SLOW_HANDSHAKE=500 FLOODS_SLOW_AUTH=500 FLOODS_SLOW_RATE=500 FLOODS_SLOW_HOLD=35 \
  bash "$D/floods.sh" >"$D/out.txt" 2>&1; echo "exit $?"
```

The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype `c9211dc`, ~2 min): `result.json` parses and has **30** checks. All pass except `no_swap_in_*` checks failing from the dev machine's own swapping (allowed, as in 6b3b; the prototype failed `no_swap_in_slow`, 8 pages). Values from the prototype:
- `storm_rate`: `verdict` `"met"`, `new_per_s` 993 of 1,000, `loadgen_cores` 0.394, `loadgen_cpus` 2.
- `garbage_limited`: gw-0 `no_token` 34,017, gw-1 6,092, `no_slot` 0, both `bound_by` `"bucket"`.
- `garbage_heartbeats`: `ping_gap_max_ms` 30,001, `ping_tick_lag_max_ms` 12, `ping_send_max_ms` 0. `slow_heartbeats`: 30,000, 5, 1.
- `storm_conntrack`, `garbage_conntrack`, `slow_conntrack`: pass with `peak` and `max` null (no conntrack on the dev machine), `samples` 8, 51, 49.
- `storm.cpu_sets` has `gateway`, `logic` and `loadgen` (one 2-CPU set on the dev machine, so the three are equal), each with `softnet_dropped` and `softnet_squeezed`.

Then the forced failures, each in its own run (`rm -rf tier2-results/floods-*` first):
- **The heartbeat gap and the conntrack peak.** In a second copy `D2=$S/dry2`, made by the same `cp` and `sed` lines, point `CT_PROC` at fake files and lower the gap bound below the interval: `mkdir -p "$D2/ct"; echo 801 >"$D2/ct/nf_conntrack_count"; echo 1000 >"$D2/ct/nf_conntrack_max"; sed -i "s|^CT_PROC=.*|CT_PROC=$D2/ct|; s|(HB + 5) \* 1000|(HB - 5) * 1000|" "$D2/floods.sh"`. Then run `FLOODS_PHASES=garbage FLOODS_CLIENTS=2000 FLOODS_RATE=1000 FLOODS_GARBAGE_RATE=2000 FLOODS_GARBAGE_INFLIGHT=500 FLOODS_GARBAGE_SECONDS=45 FLOODS_GARBAGE_BUCKET=500` (with the same `GOIM_TEST_TIER` and CPU sets). Expected (prototype): `garbage_heartbeats` fails with `ping_gap_max_ms` 30,003 and every other field as on the pass path. `garbage_conntrack` fails with `peak` 801, `max` 1000, `samples` 51.
- **A loadgen-bound storm passes.** `T2_CPUS_LOADGEN=0 FLOODS_PHASES=storm FLOODS_STORM_CLIENTS=20000 FLOODS_STORM_RATE=20000` (gateway and Logic sets as above). Expected: `storm_rate` passes with `verdict` `"loadgen-bound"` (prototype: `new_per_s` 25, `loadgen_cores` 0.924 of 1 CPU). `not_covered` starts with `storm: 20000 new clients/s at the gateway; loadgen was the bound (storm_rate.loadgen_cores)`. `storm_online` fails, as in 6b3b's O2 check.
- **A short storm fails.** The same with `T2_CPUS_LOADGEN=0-1`. Expected: `storm_rate` fails with `verdict` `"short"` (prototype: `new_per_s` 1,048, `loadgen_cores` 0.977 of 2 CPUs), with no `storm:` line in `not_covered`.

Afterwards, remove `tier2-results/floods-*`, `$D` and `$D2`, and check that no process is left (`pgrep -af "scratchpad/dry" | grep -v pgrep || echo none`).

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/floods.sh
git commit -m "feat(tier2): floods: ping gap and conntrack gates, storm_rate's loadgen-bound verdict, garbage_limited on the whole limiter, CPU sets per phase (#123, #124)"
```

- [ ] **Step 5: Verify the whole branch**

Run: `make test-scripts && make test && go vet ./... && bash -n scripts/tier2/*.sh scripts/tier2/goim-netdrop`
Expected: `lib_test`, `loadgen_test` and `stack_test` print `all passed`, and `make test` is ok. Record the commits and the dry run in this plan's *Execution Notes* and push the branch.

### Task 5: Host setup, Tier 2 run 30b and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (plan 6 row, #123, #124), Plan 6b3b's *Execution Notes* (one line pointing here), `README.md` (both languages) only if run 30b passes.

- [ ] **Step 1: Host setup (the user, on the Tier 2 host, once)**

After `git fetch && git switch feat/plan6b3 && git pull`:
1. `sudo iptables -S INPUT`: `lo` is accepted before any `--ctstate` rule, or the policy is `ACCEPT` (Review Focus 2).
2. `sudo scripts/tier2/host-setup.sh`: expect `installed: /usr/local/sbin/goim-netdrop` (it changed), `applied: NOTRACK for ports 17000-17199 on lo`, `host-setup: done`.
3. `sudo -n /usr/local/sbin/goim-netdrop notrack-check; echo $?` prints `0` only. `sudo iptables -t raw -S` shows the four `goim-notrack` rules.

After any reboot, step 2 is needed again, and the guard refuses a run until then with `the NOTRACK rules for ports 17000-17199 on lo are missing (not kept across a reboot)`.

- [ ] **Step 2: Tier 2 run 30b (on the host, ~12 min)**

With the desktop apps closed, the user runs `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods` and sends back the `.report.txt`. Expected: 30/30, run 30's 27 plus `storm_conntrack`, `garbage_conntrack`, `slow_conntrack`.

What to read, for the *Execution Notes* and the roadmap:
- **NOTRACK:** each `<phase>_conntrack.peak` against `max` (4,194,304) and the phase's connections: far below them (Review Focus 1). `dmesg | grep conntrack` shows no `table full` during the run.
- **storm:** `storm_rate.verdict`, `new_per_s`, `loadgen_cores` of `loadgen_cpus`, `gateway_cores`, and `storm.cpu_sets` (softirq and idle of each set). For a `loadgen-bound` verdict, gw-0's set must have idle to spare (Review Focus 4). `storm_bucket.no_slot_share` against run 30's 0.267.
- **garbage:** `garbage_delivered` (`attempts`, `skipped`, `outcomes`, `tcp.TCPSynRetrans`, `cpu_sets`' `softnet_dropped`, Review Focus 5), `garbage_limited.refused` (`bound_by` per gateway, `dh_per_s`: #111's input), the heartbeats' pinger fields.
- **slow:** valid this time? (no `table full`, few `dial-timeout`.) `slow_held` max against 7.5s, `slow_legit`, `slow_heartbeats` (`ping_gap_max_ms` under 35,000, no heartbeat closes), `ping_tick_lag_max_ms` and `ping_send_max_ms` (Review Focus 3).
- If a check fails: a `_conntrack` failure means Review Focus 1 (rule shape). Dial timeouts from the first dial mean Review Focus 2. A `_heartbeats` failure on the gap: read `ping_tick_lag_max_ms` against `ping_send_max_ms` (Review Focus 3). `storm_rate` `short` means loadgen fell short below its CPU bound, so read `storm.cpu_sets.loadgen` (softirq, softnet). `garbage_delivered` means Review Focus 5.

- [ ] **Step 3: Record**

- This plan's *Execution Notes*: run 30b's numbers above, with the commit hash and `dirty` from `result.json`.
- Roadmap: #123 and #124 each get one line with run 30b's result. The plan 6 row: "6b3b2 executed, run 30b …; next 6b3c (fresh session)", or the follow-up the run calls for.
- Plan 6b3b's *Execution Notes*: one line, "Run 30b: see Plan 6b3b2".
- If run 30b passes, add the README *Measured results* row in both languages, as Plan 6b3b's Task 3 Step 2 describes (**Floods**: the storm's verdict and `no_slot_share`, garbage at 80k/s beside 400k Pinging clients with Pong p99 and none missing, slow clients freed in ~5.5s with 400k logins and no Busy). Move "floods" from *Next* to *Done* in the status paragraph.

```bash
git add README.md docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-16-connection-session-core-06b3b-floods.md docs/superpowers/plans/2026-10-16-connection-session-core-06b3b2-run30-followups.md
git commit -m "docs: Tier 2 run 30b, floods recorded"
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6b3c** (`POST /reset`, `POST /network-change`, scenario `reconnect`, run 31) gets `floods`' gates as they stand after this plan: `<phase>_heartbeats` with the gap bound, `<phase>_conntrack` (6b3b's I-3 note: `floods`' helpers move to `lib.sh` in 6b3c Task 1, and `ct_start`/`ct_check` move with them).
- **#111 (parked):** run 30b's `no_slot_share` and `garbage_limited.bound_by` are its input. Unparking it is the user's call.
- **The spec:** nothing here changes it. The gauge implements a line it already has.

## Execution Notes
