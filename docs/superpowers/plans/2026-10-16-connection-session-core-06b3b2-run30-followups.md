# Plan 6b3b2: Run 30's Follow-ups Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `floods` a valid test on the Tier 2 host, then rerun it as run 30b. This plan carries #123's follow-ups to run 30 (failed 21/27): NOTRACK for the stack's ports on loopback, a loadgen pinger that cannot fall minutes behind, and the gates and records run 30 lacked.

**Architecture:** Four small changes, one task each.
- **The pinger.** Loadgen's light pinger picks slots by the clock instead of counting ticks. A late tick pings every slot due since the last one, at most one cycle's worth. Each Ping `Send` has a 1s write deadline: a client past it is closed and counted, so one blocked socket can't hold up its shard. Each Ping is stamped at its own `Send`. The pinger records its own lag, its slowest `Send` and the timeouts.
- **The gateway.** It exports the node's `nf_conntrack_count`, as the spec's *limits* metrics name it.
- **NOTRACK.** `goim-netdrop` gains `notrack` and `notrack-check` (raw table, `CT --notrack`, ports 17000–17199 on `lo`). `host-setup.sh` applies the rules. The Tier 2 guard refuses a run while they are missing, as after a reboot, or while `STACK_PORT_BASE` puts the stack outside them.
- **`floods.sh`.** It gates `ping_gap_max_ms` and each phase's conntrack peak, against `nf_conntrack_max` and against the phase's connections. It gives `storm_rate` a verdict (`met`, `loadgen-bound` or `short`) from loadgen's CPU over the ramp. It lets `garbage_limited` pass on either part of the limiter, and sends the bucket to `not_covered` when only the slots bound. It snapshots CPU sets, softnet drops and conntrack's drop counters per phase.

**Tech Stack:** Go 1.27.1 (`testing/synctest`, `prometheus/client_golang`), bash 5.2, python3 (JSON and arithmetic only), iptables (raw table, `xt_CT`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`. Used here:
- *Deployment requirements → Production node and LB limits* (~l.76): "per gateway node, `nf_conntrack_max` ≥ 2 × cap, **or** conntrack disabled for the gateway port (`NOTRACK` / CNI equivalent), declared in the gateway config as `conntrack: notrack`".
- *Observability → limits* (~l.457): "measured `RLIMIT_NOFILE` and `nf_conntrack_max` as gauges, the node's `nf_conntrack_count` where readable (alert at 80% of max)".
- *Gateway → Readiness → Limits self-check* (~l.233): "`nf_conntrack_max` is node-wide; an absent file means conntrack isn't loaded and passes".
- *Client SDK rules*: the Pong timeout is one heartbeat interval + 10s. The gateway closes a connection silent for 2 × heartbeat + 10s.
- *Verification → Load tests*: the handshake-flood and slow-client-flood bullets that `floods` (#121, #122) runs.

Inputs: Plan 6b3b (`2026-10-16-connection-session-core-06b3b-floods.md`): its *Execution Notes*, run 30's record. The roadmap's #111 (parked), #122, #123.

**Decisions:** #123 (the user, 2026-10-16) made this plan: NOTRACK in `host-setup.sh`, the pinger's catch-up and a `ping_gap_max_ms` gate, softnet snapshots, a ruling on `storm`'s loadgen bound, and maybe a `nf_conntrack_count` gauge. Before writing, the user ruled on four open points (2026-10-16, with AskUserQuestion). They are proposed together as **#124** under *Implementation choices* and go to review as usual:
1. **`storm_rate`:** the check gates `new_per_s ≥ 0.9 × rate` as before. When loadgen used ≥ 85% of its CPUs over the ramp, the check passes with verdict `loadgen-bound` and the rate goes to `not_covered`. It is not a gateway failure.
2. **`garbage_limited`:** gates the limiter as a whole, `no_slot + no_token > 0` per gateway. It records which part bound (`bound_by`), as #111's input.
3. **The pinger:** catch-up plus diagnostics (`ping_tick_lag_max_ms`, `ping_send_max_ms`).
4. **The gauge:** included (`goim_gateway_nf_conntrack_count`).

**Reviewed 2026-10-17:** O1–O10 all accepted (`objections/connection-session-core-06b3b2-run30-followups.md` and `-dispositions.md`). #124 is decided as amended, and #125 (a spec follow-up) is added. The amendments are already in this plan:
- **O1:** a 1s write deadline on the Ping `Send`, with `ping_send_timeouts`.
- **O9:** each Ping stamped at its own `Send`.
- **O3:** `<phase>_conntrack` also gates the peak under 10% of the phase's connections.
- **O4:** the `loadgen-bound` verdict reads loadgen's CPU over the ramp (`loadgen_cores_ramp`).
- **O6:** `not_covered` for the bucket when only the slots bound. This reverses 6b3b's O3.
- **O7:** per-check expectations for run 30b, plus `conntrack_stat`.
- **O8:** the firewall check includes nftables.
- **O5:** the wider loadgen CPU set is named as rejected.
- **O10:** a comparability note.
- **O2:** #125.

**Branch:** `feat/plan6b3`. Execute from its head, `4528a22` or later.

**Size:** 5 tasks, about 540 lines changed in 12 files. The prototype `proto-6b3b2` (from `4528a22`: `e9d387c` pinger, `3cd0a25` gauge, `1104a5f` NOTRACK, `c9211dc` floods, then `3393ffb` with the review's amendments to Tasks 1 and 4) ran every block. Its code is replayed here byte for byte, as `git apply` patches applied in task order. On the dev machine it passed `make test-scripts`, `make test`, `go vet ./...` and `go test -race` on the changed packages. Task 1's three mutations were caught. The `floods` dry run passed all 30 checks except the dev machine's `no_swap_in_*` (its own swapping). Forced failures were run for the new gates, and the `met`, `loadgen-bound` and `short` storm verdicts were each reached. The NOTRACK rules themselves can't run on the dev machine (no root, no user namespaces). Their command text is tested, and the host runs them in Task 5.

## Global Constraints

- `make test`, `make test-scripts` and `go vet ./...` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- Root actions stay in `goim-netdrop` (choice #68): fixed `PATH`, fixed iptables path, fixed rule shapes, no environment variable read. Its existing `add`/`del`/`flush`/`list` behaviour is unchanged, and `flush` never removes a NOTRACK rule.
- The NOTRACK range is 17000–17199 in both `goim-netdrop` (`NOTRACK_PORTS`) and `lib.sh` (`T2_NOTRACK_LO`/`T2_NOTRACK_HI`). A test checks they agree. The stack's highest port is `STACK_PORT_BASE + 132` (the 4th gateway's last, `stack_ports`), so base 17000–17067 is covered.
- Scripts: bash with `set -uo pipefail`, JSON without jq, python3 only for reading JSON and arithmetic. Remember that `t2_bool A || B` runs `||` outside `t2_bool`, so compound conditions go through `lg_true`.
- No change to the gateway's limits self-check or its flags. The stack's gateways are not passed `-conntrack-notrack`: `nf_conntrack_max` (4,194,304) passes the check anyway, and the flag would only skip a check that holds.
- `floods`' phases, sizes and defaults are unchanged (#122). Only gates and records change.

## Review Focus

1. **NOTRACK on loopback.** A locally sent packet meets raw `OUTPUT`, then raw `PREROUTING` when `lo` delivers it. The rules mark both, in both directions, so neither hook creates an entry. If the shape is wrong, the stack's connections are tracked again. `<phase>_conntrack` catches that: it gates the peak under 10% of the phase's connections, the host's own traffic only (review O3), as well as under 80% of max, the overflow run 30 hit. `notrack-check` proves only that the rules exist. This check proves they match.
2. **A host firewall that matches on conntrack state.** NOTRACKed packets have state `UNTRACKED`, not `ESTABLISHED`. A host `INPUT` chain that accepts only `ESTABLISHED,RELATED` before accepting `lo` would drop the stack's traffic. Before run 30b, `sudo iptables -S INPUT` must show `lo` accepted before any `--ctstate` rule (ufw's `-i lo -j ACCEPT` comes first), or a policy of `ACCEPT`. `sudo nft list ruleset` must show the same for native nftables tables, which `iptables -S` doesn't list (a firewalld zone, say): no input hook chain with a `ct state` accept-only policy and no prior `iifname "lo" accept` (review O8). `garbage_online`'s first dial would show such drops at once as dial timeouts.
3. **The catch-up burst and the write deadline.** After a long stall a pinger shard pings a whole cycle's slots at once. With 400k clients on loadgen's 8 CPUs (8 shards, one per GOMAXPROCS) that is up to 50k Pings in one burst per shard. That is what the clients are owed, and the gateway sees the same number of Pings per cycle. Each Ping is stamped at its own `Send`, so a burst's tail doesn't count loadgen's own queue in its gap or its Pong RTT (review O9). Run 30's stall cause is open (review O1): dropped ticks, or a `Send` blocked on a full socket buffer, which held up the whole shard. The catch-up covers the first. A 1s write deadline covers the second: a client past it is closed (its read ends `lost`, which `<phase>_heartbeats` gates) and counted in `ping_send_timeouts`. `ping_tick_lag_max_ms` large with `ping_send_max_ms` small means the goroutine was starved (CPU, GC). `ping_send_timeouts > 0` means sockets blocked: read it with `lost` and the gateways' `closes_heartbeat`.
4. **The `loadgen-bound` threshold.** 85% of loadgen's CPUs was run 30's 6.88 of 8 (86%) over the whole phase, settling included, so the old reading sat on the edge. The verdict now reads loadgen's CPU over the ramp `new_per_s` reads (`loadgen_cores_ramp`, from `/proc/PID/stat` beside each handshake sample, review O4). A loadgen that was the bound should sit clearly above 85% there. `loadgen_cores` over the whole phase is still recorded. A missing reading gives `short`, a failure. A gateway-bound storm with loadgen also at ≥ 85% would pass as `loadgen-bound`. `storm_bucket` (no_token 0) and `storm_online` still gate. `storm_rate` records `gateway_cores` and the CPU sets' shares (`storm.cpu_sets`), and the record must read them before calling a pass `loadgen-bound`. *Alternative* (review O5), rejected: in `storm` only gw-0 runs (5.42 of the gateway set's 12 CPUs in run 30), so loadgen could take half the gateway set for that phase. That cuts gw-0's GOMAXPROCS from 12 to 6, so the gateway under test would differ from the other phases and scenarios.
5. **`garbage_delivered`'s SYN loss.** Run 30 lost SYNs (`TCPSynRetrans` 437k, 29k dial timeouts) with no listen-queue drops and before the table filled. Its cause is open. A table that is not full still drops: insert races and early drops at 61k new flows/s show in `/proc/net/stat/nf_conntrack`, which run 30 didn't read. So NOTRACK itself may be the cure (review O7). Run 30b records `garbage_delivered.cpu_sets` (softnet drops and squeezes per CPU set), `garbage_delivered.conntrack_stat` (`insert_failed`, `drop`, `early_drop`, `invalid`, summed over CPUs) and `garbage_conntrack`. If it passes, NOTRACK is the likely cure, and `conntrack_stat` should read zeros. If it fails with softnet drops on the gateway's CPUs, `netdev_max_backlog` is the next suspect. With neither, it is loadgen's in-flight cap (`skipped`). The gate is not changed here.

## Implementation choices this plan makes

1. **Slots follow the clock, and a Ping `Send` has a deadline** (#124.3, review O1, O9). Visit k (slot `k % len`) is due at `start + (k+1) × tick`, as before when no tick is lost. At each tick the pinger makes every visit due by now, starting from the oldest, but at most one cycle's, so each slot is pinged once however long the stall. `PingLag` records how late the oldest due visit was. Each Ping is stamped (`pingAt`, `lastPing`) just before its own `Send`, which gets a deadline `pingSendTimeout` (1s) later through the new `clientconn.(*Conn).SetWriteDeadline`. A `Send` past it (`os.ErrDeadlineExceeded`) closes its client and counts in `PingSendTimeout`. 1s is ten ticks, far under the Pong timeout, and a loopback socket that can't take a few bytes for 1s is stuck. `PingSend` times each `Send`. The `send` hook (client and deadline) exists only so the tests can block a `Send` on synctest's clock. *Alternative:* catch-up only (the plan before review), rejected: a blocked `Send` keeps the shard's goroutine inside it, where catching up can't help.
2. **`<phase>_heartbeats` gates `ping_gap_max_ms ≤ (HB + 5) × 1000`.** The gateway closes a client 2 × HB + 10s after its last frame. A Ping more than 5s late is a pinger that fell behind. Run 30's garbage phase read 30,125ms, its slow phase 126,280ms.
3. **`<phase>_conntrack`** samples `nf_conntrack_count` every second from the stack's start to its stop (`ct_start` in `up`, `ct_check` in `down`). The check passes if the peak is ≤ 80% of `nf_conntrack_max` (the spec's alert) **and** < 10% of the phase's connections at their most (`down NAME CONNS`: `storm` N, `garbage` N + the flood's in-flight cap, `slow` N + both floods' clients, review O3). It also passes if conntrack isn't loaded, as the self-check does. With NOTRACK working, the peak is the host's own traffic. Tracked, it is at least the connections. `CT_PROC` names the proc directory, so the dry run can point it at fake files. That makes three new checks: 30 in all. Each phase also snapshots `/proc/net/stat/nf_conntrack` at its window's edges (`ct_stat_snap`/`ct_stat_json`, `CT_STAT`) and records `conntrack_stat`, the drop columns summed over CPUs, or null (review O7).
4. **The gauge** is a `GaugeFunc` registered in `Start` when the count can be read and parsed at startup. It reads the count at each scrape and reports NaN on a later failure. Where the file is unreadable, nothing is registered, which makes "where readable" literal. The name is `goim_gateway_nf_conntrack_count`. It is a separate gauge, not a `limit` label: it changes at runtime, while `limit` holds what the self-check read.
5. **The guard checks NOTRACK through `goim-netdrop notrack-check`**, which runs as root with `iptables -C`. The Tier 2 user can't read the raw table. The guard already uses `goim-netdrop list` the same way. `host-setup.sh` now fails, instead of warning, when iptables is missing, since NOTRACK needs it.

## File Structure

- `internal/clientconn/conn.go`: `SetWriteDeadline` (Task 1).
- `internal/loadgen/light.go`: `pinger.run` follows the clock, stamps each Ping at its `Send` and gives it a deadline, plus the `send` hook (Task 1).
- `internal/loadgen/recorder.go`: `PingLag`, `PingSend`, `PingSendTimeout`, and `Counts.PingTickLagMaxMs`/`PingSendMaxMs`/`PingSendTimeouts` (Task 1).
- `internal/gateway/limits.go`, `server.go`: `conntrackCount`, registered in `Start` (Task 2).
- `scripts/tier2/goim-netdrop`, `host-setup.sh`, `lib.sh`: NOTRACK and the guard (Task 3).
- `scripts/tier2/floods.sh`: the gates and records (Task 4).
- Tests: `light_test.go`, `server_test.go`, `lib_test.sh`. `floods.sh` is checked by the dry run.

---

### Task 1: The light pinger follows the clock, and its Sends have a deadline

**Files:**
- Modify: `internal/clientconn/conn.go` (`SetWriteDeadline`)
- Modify: `internal/loadgen/light.go:169-257` (the pinger)
- Modify: `internal/loadgen/recorder.go` (three fields, three methods, three `Counts` fields)
- Test: `internal/loadgen/light_test.go`

**Interfaces:**
- Produces: `(*clientconn.Conn).SetWriteDeadline(time.Time) error`, mirroring `SetReadDeadline`. `(*Recorder).PingLag(time.Duration)` and `(*Recorder).PingSend(time.Duration)`, each keeping the max. `(*Recorder).PingSendTimeout()`, a count. `Counts.PingTickLagMaxMs` (`json:"ping_tick_lag_max_ms"`), `Counts.PingSendMaxMs` (`json:"ping_send_max_ms"`) and `Counts.PingSendTimeouts` (`json:"ping_send_timeouts"`), in loadgen's `/stats` and result file. Task 4 reads them from the result file. `pingSendTimeout` = 1s. `pinger.send func(lc *lightConn, deadline time.Time) error` is replaced only by tests.

- [ ] **Step 1: Write the failing test**

```bash
git apply <<'PATCH'
diff --git a/internal/loadgen/light_test.go b/internal/loadgen/light_test.go
index 20af0e7..4356e59 100644
--- a/internal/loadgen/light_test.go
+++ b/internal/loadgen/light_test.go
@@ -3,6 +3,7 @@ package loadgen
 import (
 	"context"
 	"errors"
+	"io"
 	"net"
 	"os"
 	"syscall"
@@ -37,6 +38,101 @@ func TestPingerCountsMissingPongs(t *testing.T) {
 	}
 }
 
+// A pinger that falls behind pings, at its next tick, every slot whose time
+// has passed, each once however long the stall: slots follow the clock, not
+// the ticks it got. Run 30's pinger left clients unpinged for 126s and the
+// gateways closed 177,743 for their heartbeat; whether ticks were dropped or
+// a Send blocked is open, so this covers the first and the Send deadline the
+// second. How late it was and its slowest Send are recorded. The Send stub
+// sleeps on synctest's clock.
+func TestPingerCatchesUpOnMissedSlots(t *testing.T) {
+	synctest.Test(t, func(t *testing.T) {
+		rec := NewRecorder(5, []string{"gw-0"})
+		p := newPinger(5*pingTick, rec) // five slots, one client each
+		var pings [5]int
+		p.send = func(lc *lightConn, _ time.Time) error {
+			pings[lc.i]++
+			if lc.i == 0 && pings[0] == 1 {
+				time.Sleep(10 * pingTick) // the first Send blocks for 1s
+			}
+			return nil
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
+// A Ping Send that misses its deadline closes its client, so its read ends
+// lost, and is counted; the clients after it in the same visit are pinged,
+// each stamped at its own Send, not at the blocked one's (6b3b2 review O1,
+// O9).
+func TestPingerTimesOutABlockedSend(t *testing.T) {
+	synctest.Test(t, func(t *testing.T) {
+		rec := NewRecorder(2, []string{"gw-0"})
+		p := newPinger(pingTick, rec) // one slot: every client every tick
+		var calls int
+		var blocked *lightConn
+		var stamped int64 // the second client's pingAt at its Send
+		p.send = func(lc *lightConn, deadline time.Time) error {
+			calls++
+			switch calls {
+			case 1:
+				blocked = lc
+				time.Sleep(time.Until(deadline))
+				return os.ErrDeadlineExceeded
+			case 2:
+				stamped = lc.pingAt.Load()
+			}
+			return nil
+		}
+		var peers [2]net.Conn
+		for i := range 2 {
+			a, b := net.Pipe()
+			defer b.Close()
+			peers[i] = b
+			p.add(&lightConn{i: i, c: clientconn.New(a)})
+		}
+		t0 := time.Now()
+		ctx, cancel := context.WithCancel(context.Background())
+		done := make(chan struct{})
+		go func() { p.run(ctx); close(done) }()
+		time.Sleep(pingTick + pingSendTimeout + pingTick/2)
+		synctest.Wait()
+		if c := rec.Counts(); c.PingSendTimeouts != 1 {
+			t.Errorf("ping_send_timeouts %d, want 1", c.PingSendTimeouts)
+		}
+		if want := t0.Add(pingTick + pingSendTimeout).UnixNano(); stamped < want {
+			t.Errorf("the second client stamped %v before the blocked Send ended", time.Duration(want-stamped))
+		}
+		peers[blocked.i].SetReadDeadline(time.Now().Add(pingTick))
+		if _, err := peers[blocked.i].Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
+			t.Errorf("the blocked client's connection: read %v, want EOF (closed)", err)
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

Run: `go test ./internal/loadgen -run 'TestPingerCatchesUpOnMissedSlots|TestPingerTimesOutABlockedSend' -count=1`
Expected: build failure, starting `p.send undefined (type *pinger has no field or method send)`, then `c.PingTickLagMaxMs undefined`, `c.PingSendMaxMs undefined`, `undefined: pingSendTimeout`, `c.PingSendTimeouts undefined`. Before the review, with only the recorder's fields and the `send` hook added and the old tick-counting loop kept, the prototype failed on behaviour: `client 3 pinged 0 times by 1.25s`, `client 4 pinged 0 times`, `ping_tick_lag_max_ms 0, want >= 900`.

- [ ] **Step 3: Implement the clock-driven pinger, the Send deadline and the recorder's fields**

```bash
git apply <<'PATCH'
diff --git a/internal/clientconn/conn.go b/internal/clientconn/conn.go
index 3898503..476f3e2 100644
--- a/internal/clientconn/conn.go
+++ b/internal/clientconn/conn.go
@@ -132,6 +132,10 @@ func (c *Conn) Recv(deadline time.Time) (Frame, error) {
 // the Pong timeout while another goroutine waits in Recv.
 func (c *Conn) SetReadDeadline(t time.Time) error { return c.nc.SetReadDeadline(t) }
 
+// SetWriteDeadline bounds the Sends that follow, e.g. a Ping a full socket
+// buffer would block.
+func (c *Conn) SetWriteDeadline(t time.Time) error { return c.nc.SetWriteDeadline(t) }
+
 // Close closes the socket with a FIN.
 func (c *Conn) Close() error { return c.nc.Close() }
 
diff --git a/internal/loadgen/light.go b/internal/loadgen/light.go
index 36822f7..00d7cd6 100644
--- a/internal/loadgen/light.go
+++ b/internal/loadgen/light.go
@@ -5,6 +5,7 @@ import (
 	"errors"
 	"math/rand/v2"
 	"net"
+	"os"
 	"sync"
 	"sync/atomic"
 	"syscall"
@@ -167,21 +168,31 @@ func (lc *lightConn) read(rec *Recorder) string {
 }
 
 // pinger sends every light client a Ping each PingEvery, spread evenly:
-// clients sit in PingEvery/100ms slots by index, and one slot is pinged per
-// 100ms tick. A Ping due while the last one is unanswered is counted
-// missing.
+// clients sit in PingEvery/100ms slots by index, and one slot is due every
+// 100ms by the clock; a late tick pings every slot due since. A Ping due
+// while the last one is unanswered is counted missing. A Send not done in
+// pingSendTimeout (a full socket buffer) closes its client, so one blocked
+// socket can't hold back its shard: the client's read ends lost.
 type pinger struct {
 	tick   time.Duration
 	rec    *Recorder
 	shards int // of the pingers it belongs to (1 alone)
 	mu     sync.Mutex
 	slots  []map[*lightConn]struct{}
+	send   func(lc *lightConn, deadline time.Time) error // sends one Ping; tests replace it
 }
 
-const pingTick = 100 * time.Millisecond
+const (
+	pingTick        = 100 * time.Millisecond
+	pingSendTimeout = time.Second
+)
 
 func newPinger(every time.Duration, rec *Recorder) *pinger {
-	p := &pinger{tick: pingTick, rec: rec, shards: 1, slots: make([]map[*lightConn]struct{}, max(1, int(every/pingTick)))}
+	p := &pinger{tick: pingTick, rec: rec, shards: 1, slots: make([]map[*lightConn]struct{}, max(1, int(every/pingTick))),
+		send: func(lc *lightConn, deadline time.Time) error {
+			lc.c.SetWriteDeadline(deadline)
+			return lc.c.Send(clientpb.Cmd_CMD_PING, lc.reqID.Add(1), nil) // a failure ends read
+		}}
 	for i := range p.slots {
 		p.slots[i] = map[*lightConn]struct{}{}
 	}
@@ -232,26 +243,45 @@ func (ps pingers) run(ctx context.Context) {
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
-		now := time.Now().UnixNano()
 		for _, lc := range due {
+			// Each client's own time, so a Send blocked before it is not
+			// counted in its gap or its Pong's RTT.
+			s := time.Now()
+			now := s.UnixNano()
 			if lc.pingAt.Swap(now) != 0 {
 				p.rec.PongMissing()
 			}
 			p.rec.PingGap(time.Duration(now - lc.lastPing.Swap(now)))
-			lc.c.Send(clientpb.Cmd_CMD_PING, lc.reqID.Add(1), nil) // a failure ends read
+			if err := p.send(lc, s.Add(pingSendTimeout)); errors.Is(err, os.ErrDeadlineExceeded) {
+				p.rec.PingSendTimeout()
+				lc.c.Close()
+			}
+			p.rec.PingSend(time.Since(s))
 		}
 	}
 }
diff --git a/internal/loadgen/recorder.go b/internal/loadgen/recorder.go
index c46b42c..9bbba60 100644
--- a/internal/loadgen/recorder.go
+++ b/internal/loadgen/recorder.go
@@ -32,6 +32,9 @@ type Recorder struct {
 	pongs          int64
 	missing        int64
 	gapMax         time.Duration
+	lagMax         time.Duration
+	sendMax        time.Duration
+	sendTimeouts   int64
 	pongRTT        *hist
 	marks          []*mark // by index
 	byName         map[string]*mark
@@ -215,6 +218,30 @@ func (r *Recorder) PingGap(gap time.Duration) {
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
+// PingSendTimeout: a Ping's Send missed its deadline and its client was
+// closed.
+func (r *Recorder) PingSendTimeout() {
+	r.mu.Lock()
+	r.sendTimeouts++
+	r.mu.Unlock()
+}
+
 // Mark records instant now as name, with the clients logged in on gateway
 // gw (-1: on any gateway) as its population, and returns its size. A client
 // belongs to its latest mark only. Names are unique. gw -1 is only for an
@@ -325,6 +352,9 @@ type Counts struct {
 	Pongs            int64            `json:"pongs"`
 	PongMissing      int64            `json:"pong_missing"`
 	PingGapMaxMs     int64            `json:"ping_gap_max_ms"`
+	PingTickLagMaxMs int64            `json:"ping_tick_lag_max_ms"` // a slot pinged late by the pinger itself
+	PingSendMaxMs    int64            `json:"ping_send_max_ms"`     // the slowest Ping Send
+	PingSendTimeouts int64            `json:"ping_send_timeouts"`   // Sends past the deadline, their clients closed
 	PongRTT          Dist             `json:"pong_rtt"`
 }
 
@@ -364,7 +394,9 @@ func (r *Recorder) Counts() Counts {
 		BackoffClients: r.backoffClients, BackoffWaits: map[string]int64{},
 		BackoffSinceMark: BackoffCount{Mark: r.since.Mark, Clients: r.since.Clients, Waits: map[string]int64{}},
 		StormSinceMark:   r.storm,
-		LoginLatency:     r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PingGapMaxMs: r.gapMax.Milliseconds(), PongRTT: r.pongRTT.dist(r.pongRTT.n)}
+		LoginLatency:     r.login.dist(r.login.n), Pongs: r.pongs, PongMissing: r.missing, PingGapMaxMs: r.gapMax.Milliseconds(),
+		PingTickLagMaxMs: r.lagMax.Milliseconds(), PingSendMaxMs: r.sendMax.Milliseconds(),
+		PingSendTimeouts: r.sendTimeouts, PongRTT: r.pongRTT.dist(r.pongRTT.n)}
 	for i, n := range r.online {
 		c.Online += n
 		c.OnlineByGateway[r.ids[i]] = n
PATCH
```

- [ ] **Step 4: Run the tests to make sure they pass**

Run: `gofmt -l internal/loadgen; go vet ./internal/loadgen && go test -race -count=1 ./internal/loadgen ./cmd/loadgen`
Expected: no `gofmt` output, both `ok`. `TestPingerRecordsTheLongestPingGap` (a 1s real-time stall) still passes, now through the catch-up.

Mutations (prototype), each restored after, with `timeout 90` on the test run:
- Delete the line `next = max(next, upto-len(p.slots))`. `TestPingerCatchesUpOnMissedSlots` fails with `client 0 pinged 3 times by 1.25s, want 1 or 2`.
- Make the timeout branch `false && errors.Is(…)`. `TestPingerTimesOutABlockedSend` fails with `ping_send_timeouts 0, want 1` and `read pipe: i/o timeout, want EOF (closed)`.
- Delete `lc.c.Close()` alone. The same test fails with `want EOF (closed)`.
- Stamp once per visit (`burst := time.Now()` before the loop, `s := burst` in it). The same test fails with `the second client stamped 1s before the blocked Send ended`.

- [ ] **Step 5: Commit**

```bash
git add internal/clientconn/conn.go internal/loadgen/light.go internal/loadgen/recorder.go internal/loadgen/light_test.go
git commit -m "fix(loadgen): the light pinger's slots follow the clock, its Sends have a 1s deadline, each Ping stamped at its Send; ping_tick_lag_max_ms, ping_send_max_ms, ping_send_timeouts (#123, #124, run 30)"
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
- Consumes: Task 1's `ping_gap_max_ms`, `ping_tick_lag_max_ms`, `ping_send_max_ms`, `ping_send_timeouts` in the light instance's result file. `/proc/PID/stat` (loadgen's CPU time) and `/proc/net/stat/nf_conntrack` (`CT_STAT`). `t2_cpustat_snap`/`t2_cpusets_json`, `t2_cpulist_expand`, `T2_CPUS_LOADGEN` (`lib.sh`), `lg_true`/`lg_py` (`loadgen.sh`).
- Produces: checks `storm_conntrack`, `garbage_conntrack`, `slow_conntrack` (30 checks in all), with `conns` and `peak_per_conn`. `storm_rate.verdict`, `storm_rate.loadgen_cores_ramp` (also `storm.handshakes.loadgen_cores_ramp`), `storm_rate.loadgen_cpus`, `garbage_limited.refused.<gw>.bound_by`, a `not_covered` line `garbage: the bucket …` when only the slots bound, the `<phase>_heartbeats` pinger fields, `cpu_sets` and `conntrack_stat` in `storm`, `garbage`, `slow` and in `garbage_delivered`. `hs_sampler NAME FILE PID` writes 5 columns (`… cpu`). `down NAME CONNS`.

- [ ] **Step 1: Apply the changes**

```bash
git apply <<'PATCH'
diff --git a/scripts/tier2/floods.sh b/scripts/tier2/floods.sh
index 0cbc605..30ca07d 100755
--- a/scripts/tier2/floods.sh
+++ b/scripts/tier2/floods.sh
@@ -69,7 +69,13 @@
 #                         them (so the limiter saw the storm, not a
 #                         loadgen-bound trickle that retries inflate: final
 #                         review I-1); the seen and completed (ok) rates and
-#                         the per-second series are recorded
+#                         the per-second series are recorded. A loadgen at
+#                         >= 85% of its CPUs over the same window (its
+#                         ramp, not the settling after it: 6b3b2 review O4)
+#                         is the bound, not the gateway (run 30: 6.88 of 8
+#                         over the whole phase): the check passes with
+#                         verdict "loadgen-bound" and the rate goes to
+#                         not_covered
 #   storm_bucket          the bucket refused nothing (no_token 0, #121);
 #                         the concurrency limit's refusals (no_slot, each a
 #                         pre-handshake close the client retries after
@@ -84,16 +90,26 @@
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
+#                         are recorded (#111's input). Either part passes,
+#                         which 6b3b's review O3 had ruled out: when only the
+#                         slots bound, the bucket goes to not_covered
+#                         (6b3b2 review O6)
 #   garbage_heartbeats    every Ping answered (no Pong missing, at least as
 #                         many Pongs over the light run as clients online: a
 #                         floor, not per client), the slowest Pong under 10s
-#                         (the SDK's Pong timeout), no connection lost, and no
-#                         gateway closed one for its heartbeat
+#                         (the SDK's Pong timeout), no connection lost, no
+#                         gateway closed one for its heartbeat, and no client
+#                         waited more than the interval + 5s for its next
+#                         Ping (ping_gap_max_ms: run 30's pinger fell 126s
+#                         behind); the pinger's own lag, slowest Send and
+#                         Sends past their 1s deadline (each closes its
+#                         client, so lost is not empty) are recorded
 #   garbage_lease         each gateway renewed its lease every interval
 #                         through the flood, and Logic saw no renewal error
 #   no_swap_in_garbage    no page swapped in over the flood
@@ -115,11 +131,23 @@
 #                         recorded
 #   slow_heartbeats, slow_lease   as garbage's, over the floods
 #   no_swap_in_slow       no page swapped in from the floods' start to their stop
+#   <phase>_conntrack     the host's nf_conntrack_count, sampled every second
+#                         from the stack up to its stop, peaked under 80% of
+#                         nf_conntrack_max (the spec's alert) and under 10% of
+#                         the phase's connections, so the NOTRACK rules work,
+#                         not only exist (#123, 6b3b2 review O3); run 30's
+#                         slow phase ran on a full table unseen; passes with
+#                         no conntrack loaded
 #   <phase>_idle          after loadgen stops, every gateway reaches 0
 #                         connections and settles its Logouts
 #   <phase>_processes     nothing died, panicked or stopped uncleanly
 #   artifacts             metrics before and after each phase's window, the
 #                         garbage flood's CPU profiles, loadgen's results
+# Each phase also records its CPU sets' shares and softnet drops over its
+# window (cpu_sets: run 30's garbage SYN loss had none snapshotted), and
+# conntrack's own drops (conntrack_stat: insert_failed, drop, early_drop,
+# invalid, summed over CPUs; null without conntrack), which a table that is
+# not full still makes (6b3b2 review O7).
 set -uo pipefail
 
 . "$(dirname "$0")/lib.sh"
@@ -158,6 +186,8 @@ t2_on_exit 'lg_stop_all' # newest first: every loadgen instance stops before the
 smp="" prof=""
 t2_on_exit '[ -n "$smp" ] && kill "$smp" 2>/dev/null'
 t2_on_exit '[ -n "$prof" ] && kill "$prof" 2>/dev/null'
+cts=""
+t2_on_exit '[ -n "$cts" ] && kill "$cts" 2>/dev/null'
 
 GW_COUNTERS=(
   hs_ok='goim_gateway_handshakes_total:result="ok"'
@@ -200,11 +230,13 @@ up() {
   stack_up 2>"$STACK_DIR/stack-up.err"
   local rc=$?
   t2_check "${1}_stack" "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
+  [ $rc -eq 0 ] && ct_start
   return $rc
 }
 
-# down NAME: every loadgen stopped, the gateways idle, the stack stopped;
-# checks NAME_idle and NAME_processes.
+# down NAME CONNS: every loadgen stopped, the gateways idle, the stack
+# stopped; checks NAME_conntrack (CONNS: the phase's connections at their
+# most), NAME_idle and NAME_processes.
 down() {
   lg_stop_all
   local t=$SECONDS idle stop_s problems
@@ -212,10 +244,50 @@ down() {
   stop_s=$((SECONDS - t))
   t2_check "${1}_idle" "$idle" "\"stop_s\": $stop_s, \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
   stack_down
+  ct_check "$1" "$2"
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
+# ct_check PHASE CONNS: stops the sampler; checks PHASE_conntrack. With the
+# stack's ports untracked the peak is the host's own traffic, far below the
+# phase's CONNS connections; tracked, it is at least them.
+ct_check() {
+  kill "$cts" 2>/dev/null; wait "$cts" 2>/dev/null; cts=""
+  local max peak
+  max=$(cat "$CT_PROC/nf_conntrack_max" 2>/dev/null || echo null)
+  peak=$(awk 'NF == 2 { if ($2 + 0 > p) p = $2 + 0; n++ } END { print n ? p : "null" }' "$STACK_DIR/conntrack.tsv" 2>/dev/null)
+  t2_check "${1}_conntrack" "$(echo "{\"peak\": ${peak:-null}, \"max\": $max}" | lg_true - 'd["max"] is None or (d["peak"] is not None and d["peak"] <= 0.8 * d["max"] and d["peak"] < 0.1 * '"$2"')')" \
+    "\"peak\": ${peak:-null}, \"max\": $max, \"conns\": $2, \"peak_per_conn\": $(echo "{\"peak\": ${peak:-null}}" | lg_py - 'None if d["peak"] is None else round(d["peak"] / '"$2"', 4)'), \"samples\": $(wc -l <"$STACK_DIR/conntrack.tsv" 2>/dev/null || echo 0)"
+}
+# ct_stat_snap FILE: conntrack's per-CPU statistics (absent: no conntrack);
+# ct_stat_json A B: the columns that count drops, summed over the CPUs, B
+# less A, or null.
+CT_STAT=/proc/net/stat/nf_conntrack
+ct_stat_snap() { cat "$CT_STAT" >"$1" 2>/dev/null || rm -f "$1"; }
+ct_stat_json() {
+  python3 -c 'import json, sys
+def read(p):
+    rows = [l.split() for l in open(p)]
+    sums = {}
+    for r in rows[1:]:
+        for k, v in zip(rows[0], r):
+            sums[k] = sums.get(k, 0) + int(v, 16)
+    return sums
+a, b = read(sys.argv[1]), read(sys.argv[2])
+print(json.dumps({k: b[k] - a[k] for k in ("insert_failed", "drop", "early_drop", "invalid") if k in a and k in b}))' "$1" "$2" 2>/dev/null || echo null
+}
+
 # gws_counters: {"gw-0": stack_counters, ...} of every gateway; gws_delta A B
 # the same of two readings.
 gws_counters() {
@@ -236,19 +308,22 @@ def delta(x, y):
 print(json.dumps({g: delta(a.get(g), y) for g, y in b.items()}))' "$1" "$2" 2>/dev/null || echo null
 }
 
-# hs_sampler NAME FILE: appends "unix_ms ok refused pre" every second, NAME's
-# completed handshakes, the limiter's refusals (no_slot + no_token, a label
-# with no sample yet read as 0) and the current loadgen's pre-handshake
-# closes (each a retry to come, final review I-1), until killed. A second
-# with an unreadable loadgen is skipped.
+# hs_sampler NAME FILE PID: appends "unix_ms ok refused pre cpu" every
+# second, NAME's completed handshakes, the limiter's refusals (no_slot +
+# no_token, a label with no sample yet read as 0), the current loadgen's
+# pre-handshake closes (each a retry to come, final review I-1) and PID's
+# CPU time in clock ticks (loadgen's, so its share over the ramp alone can
+# be read: 6b3b2 review O4), until killed. A second with an unreadable
+# loadgen is skipped.
 hs_sampler() {
-  local c pre re='"ok": ([0-9]+), "slot": ([0-9]+|null), "token": ([0-9]+|null)'
+  local c pre cpu re='"ok": ([0-9]+), "slot": ([0-9]+|null), "token": ([0-9]+|null)'
   while :; do
     pre=$(lg_get /stats 'd["failed"].get("pre-handshake", 0)')
     c=$(stack_counters "$1" ok='goim_gateway_handshakes_total:result="ok"' \
       slot='goim_gateway_handshakes_total:result="no_slot"' token='goim_gateway_handshakes_total:result="no_token"')
-    [[ $pre =~ ^[0-9]+$ ]] && [[ $c =~ $re ]] && # c last: BASH_REMATCH is its
-      echo "$(date +%s%3N) ${BASH_REMATCH[1]} $((${BASH_REMATCH[2]/null/0} + ${BASH_REMATCH[3]/null/0})) $pre" >>"$2"
+    cpu=$(awk '{ print $14 + $15 }' "/proc/$3/stat" 2>/dev/null) # read beside the timestamp: /stats is slow on a busy loadgen
+    [[ $pre =~ ^[0-9]+$ ]] && [[ $cpu =~ ^[0-9]+$ ]] && [[ $c =~ $re ]] && # c last: BASH_REMATCH is its
+      echo "$(date +%s%3N) ${BASH_REMATCH[1]} $((${BASH_REMATCH[2]/null/0} + ${BASH_REMATCH[3]/null/0})) $pre $cpu" >>"$2"
     sleep 1
   done
 }
@@ -260,12 +335,13 @@ hs_sampler() {
 # on and their median gap in ms: a saturated loadgen answers /stats late,
 # P6b3bI-1), "rate_per_s" (completed, between 5% and 95% of N completed,
 # which the retries stretch), "ramp_s" (of the completed, 5% to 95%
-# interpolated), "peak_per_s" (completed), "series": [[s since the first
+# interpolated), "peak_per_s" (completed), "loadgen_cores_ramp" (loadgen's
+# CPU over new_per_s's samples, null with fewer than 2), "series": [[s since the first
 # sample, completed/s, seen/s, new/s], ...] (first 120)}; null when the
 # samples never reach 95% of N completed.
 hs_rate() {
-  python3 -c 'import json, sys
-rows = [tuple(int(x) for x in l.split()) for l in open(sys.argv[1]) if len(l.split()) == 4 and all(x.isdigit() for x in l.split())]
+  python3 -c 'import json, os, sys
+rows = [tuple(int(x) for x in l.split()) for l in open(sys.argv[1]) if len(l.split()) == 5 and all(x.isdigit() for x in l.split())]
 n = int(sys.argv[2])
 def cross(f, x):
     # When f reached x, interpolated between the samples around it: a sample
@@ -303,10 +379,12 @@ if rate is None:
     print("null"); sys.exit()
 nb = [r[0] for r in rows if 0.05 * n <= fnew(r) < 0.95 * n]
 gaps = sorted(b - a for a, b in zip(nb, nb[1:]))
+rb = [r for r in rows if 0.05 * n <= fnew(r) < 0.95 * n]
+cores = round((rb[-1][4] - rb[0][4]) / os.sysconf("SC_CLK_TCK") * 1000 / (rb[-1][0] - rb[0][0]), 3) if len(rb) >= 2 and rb[-1][0] > rb[0][0] else None
 pts = [((r1[0] - rows[0][0]) / 1000, (r1[1] - r0[1]) * 1000 / (r1[0] - r0[0]), (r1[1] + r1[2] - r0[1] - r0[2]) * 1000 / (r1[0] - r0[0]),
         (fnew(r1) - fnew(r0)) * 1000 / (r1[0] - r0[0])) for r0, r1 in zip(rows, rows[1:]) if r1[0] > r0[0]]
 print(json.dumps({"seen_per_s": seen, "new_per_s": new, "samples": {"n": len(nb), "gap_ms": gaps[len(gaps) // 2] if gaps else None},
-       "rate_per_s": rate, "ramp_s": ramp, "peak_per_s": round(max(p[1] for p in pts)),
+       "rate_per_s": rate, "ramp_s": ramp, "peak_per_s": round(max(p[1] for p in pts)), "loadgen_cores_ramp": cores,
        "series": [[round(t, 1), round(v), round(w), round(x)] for t, v, w, x in pts[:120]]}))' "$1" "$2" 2>/dev/null || echo null
 }
 
@@ -338,12 +416,14 @@ lease() {
 
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
+    "\"pongs\": $(lg_json 'd["pongs"]'), \"pong_missing\": $(lg_json 'd["pong_missing"]'), \"pong_rtt\": $(lg_json 'd["pong_rtt"]'), \"lost\": $(lg_json 'd["lost"]'), \"closes_heartbeat\": $(echo "$3" | lg_py - '{g: v["closes_heartbeat"] for g, v in d.items()}'), \"ping_gap_max_ms\": $(lg_json 'd["ping_gap_max_ms"]'), \"ping_tick_lag_max_ms\": $(lg_json 'd["ping_tick_lag_max_ms"]'), \"ping_send_max_ms\": $(lg_json 'd["ping_send_max_ms"]'), \"ping_send_timeouts\": $(lg_json 'd["ping_send_timeouts"]')"
 }
 
 t=$SECONDS
@@ -357,19 +437,23 @@ missing=""
 
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
+  ct_stat_snap "$STACK_DIR/ctstat-0"
   lg_start storm -mode light -clients "$n" -rate "$rate" -uid-prefix storm
   stack_usage_start loadgen="$LG_PID"
-  hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" &
+  hs_sampler gw-0 "$STACK_DIR/artifacts/handshakes-gw-0.tsv" "$LG_PID" &
   smp=$!
   online=$(t2_bool lg_wait "$(settled "$n")" $((n / rate * 3 + 60)))
   kill "$smp" 2>/dev/null; wait "$smp" 2>/dev/null; smp=""
   usage=$(stack_usage_json)
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
+  ct_stat_snap "$STACK_DIR/ctstat-1"
   s1=$(gws_counters)
   t2_record_end storm
   stack_snapshot after
@@ -378,12 +462,17 @@ phase_storm() {
   [ "$online" = true ] && online=$(lg_get /stats "$(logged_in "$n")")
   [ "$online" = true ] || online=false # a null /stats reading fails the check, not the run
   t2_check storm_online "$online" "\"want\": $n, \"online\": $(lg_get /stats 'd["online"]'), \"gave_up\": $(lg_get /stats 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_get /stats 'd["failed"]'), \"login_latency\": $(lg_get /stats 'd["login_latency"]')"
-  t2_check storm_rate "$(echo "$hs" | lg_true - 'd["new_per_s"] >= 0.9 * '"$rate")" \
-    "\"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("new_per_s", "samples", "seen_per_s", "rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
+  # The verdict: met, loadgen-bound (loadgen at >= 85% of its CPUs over the
+  # ramp new_per_s reads) or short (a reading missing included).
+  rated=$(echo "{\"h\": $hs, \"cpus\": $(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)}" | lg_py - \
+    '"met" if ((d["h"] or {}).get("new_per_s") or 0) >= 0.9 * '"$rate"' else "loadgen-bound" if ((d["h"] or {}).get("loadgen_cores_ramp") or 0) >= 0.85 * d["cpus"] else "short"')
+  [ "$rated" = '"loadgen-bound"' ] && t2_not_covered "storm: $rate new clients/s at the gateway; loadgen was the bound (storm_rate.loadgen_cores_ramp)"
+  t2_check storm_rate "$(echo "$rated" | lg_true - 'd in ("met", "loadgen-bound")')" \
+    "\"verdict\": $rated, \"rate\": $rate, \"handshakes\": $(echo "$hs" | lg_py - '{k: d[k] for k in ("new_per_s", "samples", "seen_per_s", "rate_per_s", "ramp_s", "peak_per_s")}'), \"loadgen_cores_ramp\": $(echo "$hs" | lg_py - 'd["loadgen_cores_ramp"]'), \"loadgen_cores\": $(echo "$usage" | lg_py - 'd["loadgen"]["cores"]'), \"loadgen_cpus\": $(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w), \"gateway_cores\": $(echo "$usage" | lg_py - 'd["gw-0"]["cores"]')"
   t2_check storm_bucket "$(echo "$d" | lg_true - 'd["gw-0"]["hs_no_token"] == 0')" \
     "\"gw-0\": $(echo "$d" | lg_py - '{k: d["gw-0"][k] for k in ("hs_ok", "hs_no_token", "hs_no_slot", "rejects_handshake_rate", "logins_busy", "rejects_pending_mem", "rejects_at_capacity")}'), \"no_slot_share\": $(echo "$d" | lg_py - 'round(d["gw-0"]["hs_no_slot"] / (d["gw-0"]["hs_ok"] + d["gw-0"]["hs_no_slot"]), 4)'), \"failed\": $(lg_get /stats 'd["failed"]')"
-  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
-  down storm
+  t2_field storm "{\"clients\": $n, \"rate\": $rate, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"handshakes\": $hs, \"gateways\": $d, \"login_latency\": $(lg_get /stats 'd["login_latency"]'), \"failed\": $(lg_get /stats 'd["failed"]')}"
+  down storm "$n"
 }
 
 # garbage: #121 (1), garbage handshakes beside a Pinging population.
@@ -401,10 +490,12 @@ phase_garbage() {
   on=$(lg_get /stats 'd["online"]')
   [ "$online" = true ] || online=false
   t2_check garbage_online "$online" "\"want\": $n, \"online\": $on, \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"gave_up\": $(lg_get /stats 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_get /stats 'd["failed"]')"
-  [ "$online" = true ] || { down garbage; return; }
+  [ "$online" = true ] || { down garbage $((n + inflight)); return; }
   stack_snapshot before
   s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   t2_tcpstat_snap "$STACK_DIR/tcp0"
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
+  ct_stat_snap "$STACK_DIR/ctstat-0"
   tw0=$(timewait)
   t2_record_start garbage
   t0=$SECONDS
@@ -418,6 +509,8 @@ phase_garbage() {
   s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   lease_s=$((SECONDS - t0))
   t2_tcpstat_snap "$STACK_DIR/tcp1"
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
+  ct_stat_snap "$STACK_DIR/ctstat-1"
   tw1=$(timewait)
   lg_in flood lg_stop
   wait "$prof"
@@ -429,13 +522,15 @@ phase_garbage() {
   flood=$(lg_py "$STACK_DIR/artifacts/loadgen-garbage.json" 'd["flood"]')
   want=$(python3 -c 'import sys; print(int(0.95 * int(sys.argv[1]) * int(sys.argv[2])))' "$grate" "$secs")
   t2_check garbage_delivered "$(echo "{\"g\": $d, \"f\": $flood}" | lg_true - 'd["f"]["attempts"] >= '"$want"' and sum(v["hs_failed"] + v["hs_no_slot"] + v["hs_no_token"] for v in d["g"].values()) >= '"$want")" \
-    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1")"
-  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_token"] > 0 for v in d.values())')" \
-    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"]} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
+    "\"rate\": $grate, \"seconds\": $secs, \"want\": $want, \"attempts\": $(echo "$flood" | lg_py - 'd["attempts"]'), \"skipped\": $(echo "$flood" | lg_py - 'd["skipped"]'), \"outcomes\": $(echo "$flood" | lg_py - 'd["outcomes"]'), \"held\": $(echo "$flood" | lg_py - 'd["held"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("hs_ok", "hs_failed", "hs_no_slot", "hs_no_token", "rejects_handshake_rate")} for g, v in d.items()}'), \"timewait\": [$tw0, $tw1], \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcp0" "$STACK_DIR/tcp1"), \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1")"
+  t2_check garbage_limited "$(echo "$d" | lg_true - 'all(v["hs_no_slot"] + v["hs_no_token"] > 0 for v in d.values())')" \
+    "\"refused\": $(echo "$d" | lg_py - '{g: {"no_slot": v["hs_no_slot"], "no_token": v["hs_no_token"], "bound_by": "bucket" if v["hs_no_token"] > 0 else "slots" if v["hs_no_slot"] > 0 else None} for g, v in d.items()}'), \"dh_per_s\": $(echo "$d" | lg_py - '{g: round(v["hs_failed"] / '"$secs"') for g, v in d.items()}'), \"bucket\": ${FLOODS_GARBAGE_BUCKET:-25000}"
+  [ "$(echo "$d" | lg_true - 'all(v["hs_no_token"] == 0 and v["hs_no_slot"] > 0 for v in d.values())')" = true ] &&
+    t2_not_covered "garbage: the bucket (${FLOODS_GARBAGE_BUCKET:-25000}/s) refused nothing; the concurrency limit bound first (garbage_limited.dh_per_s)"
   heartbeats garbage "$on" "$d"
   lease garbage "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
-  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d, \"flood\": $flood}"
-  down garbage
+  t2_field garbage "{\"clients\": $n, \"rate\": $rate, \"garbage_rate\": $grate, \"inflight\": $inflight, \"seconds\": $secs, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d, \"flood\": $flood}"
+  down garbage $((n + inflight))
   for f in before/gw-0 after/gw-0 garbage/gw-0.cpu.pb.gz garbage/gw-1.cpu.pb.gz garbage/loadgen-flood.cpu.pb.gz; do
     case $f in
       *.gz) gzip -t "$STACK_DIR/artifacts/$f" 2>/dev/null || missing+=" garbage:$f" ;;
@@ -454,13 +549,15 @@ phase_slow() {
   stack_snapshot before
   s0=$(gws_counters) l0=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   t2_record_start slow
+  t2_cpustat_snap "$STACK_DIR/cpustat-0"
+  ct_stat_snap "$STACK_DIR/ctstat-0"
   t0=$SECONDS
   lg_in flood lg_start slowhs -mode slow-handshake -clients "$hs" -rate "$srate" -source-net 1
   lg_in flood2 lg_start slowauth -mode slow-auth -clients "$au" -rate "$srate" -source-net 2
   lg_in flood lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$hs"' and d["flood"]["open"] >= 0.85 * '"$hs" $((hs / srate + 60)) || flooding=false
   lg_in flood2 lg_wait 'd["flood"]["outcomes"].get("closed", 0) >= '"$au"' and d["flood"]["open"] >= 0.85 * '"$au" $((au / srate + 60)) || flooding=false
   t2_check slow_flood "$flooding" "\"slowhs\": $(lg_in flood lg_get /stats 'd["flood"]'), \"slowauth\": $(lg_in flood2 lg_get /stats 'd["flood"]')"
-  [ "$flooding" = true ] || { t2_record_end slow; down slow; return; }
+  [ "$flooding" = true ] || { t2_record_end slow; down slow $((n + hs + au)); return; }
   lg_start slow-light -mode light -clients "$n" -rate "$rate" -ping-every "${HB}s" -uid-prefix slow
   stack_usage_start loadgen="$LG_PID" flood="${LG_PIDS[flood]}" flood2="${LG_PIDS[flood2]}"
   online=false
@@ -472,6 +569,8 @@ phase_slow() {
   lg_in flood2 lg_stop
   s1=$(gws_counters) l1=$(stack_counters logic lease_error="$LOGIC_LEASE_ERROR")
   lease_s=$((SECONDS - t0))
+  t2_cpustat_snap "$STACK_DIR/cpustat-1"
+  ct_stat_snap "$STACK_DIR/ctstat-1"
   t2_record_end slow
   stack_snapshot after
   lg_stop
@@ -489,8 +588,8 @@ phase_slow() {
     "\"want\": $n, \"online\": $on, \"gave_up\": $(lg_json 'd["failed"].get("gave-up", 0)'), \"failed\": $(lg_json 'd["failed"]'), \"login_latency\": $(lg_json 'd["login_latency"]'), \"gateways\": $(echo "$d" | lg_py - '{g: {k: v[k] for k in ("logins_ok", "logins_busy", "rejects_login_queue", "rejects_pending_mem", "rejects_at_capacity", "hs_no_slot", "hs_no_token")} for g, v in d.items()}')"
   heartbeats slow "$on" "$d"
   lease slow "$d" "$lease_s" "$(stack_counters_delta "$l0" "$l1")"
-  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"gateways\": $d}"
-  down slow
+  t2_field slow "{\"clients\": $n, \"rate\": $rate, \"slow_handshake\": $hs, \"slow_auth\": $au, \"slow_rate\": $srate, \"hold_s\": $hold, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"usage\": $usage, \"cpu_sets\": $(t2_cpusets_json "$STACK_DIR/cpustat-0" "$STACK_DIR/cpustat-1"), \"conntrack_stat\": $(ct_stat_json "$STACK_DIR/ctstat-0" "$STACK_DIR/ctstat-1"), \"gateways\": $d}"
+  down slow $((n + hs + au))
   for f in before/gw-0 after/gw-0; do
     grep -q '^# TYPE' "$STACK_DIR/artifacts/$f.metrics" 2>/dev/null || missing+=" slow:$f.metrics"
   done
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

Expected (prototype `3393ffb`, ~3 min): `result.json` parses and has **30** checks. All pass except `no_swap_in_*` checks failing from the dev machine's own swapping (allowed, as in 6b3b; the prototype's last run failed all three, 10/32/864 pages, an earlier one only `no_swap_in_garbage`). Values from the prototype:
- `storm_rate`: `verdict` `"met"`, `new_per_s` 999 of 1,000, `loadgen_cores_ramp` 0.783, `loadgen_cores` 0.404, `loadgen_cpus` 2.
- `garbage_limited`: gw-0 `no_token` 33,360, gw-1 6,913, `no_slot` 0, both `bound_by` `"bucket"`, and no `garbage:` line in `not_covered`.
- `garbage_heartbeats`: `ping_gap_max_ms` 30,002, `ping_tick_lag_max_ms` 3, `ping_send_max_ms` 4, `ping_send_timeouts` 0. `slow_heartbeats`: 30,001, 12, 0, 0.
- `storm_conntrack`, `garbage_conntrack`, `slow_conntrack`: pass with `peak`, `max` and `peak_per_conn` null (no conntrack on the dev machine), `conns` 2,000, 2,500, 3,000, `samples` 9, 51, 49.
- `storm.cpu_sets` has `gateway`, `logic` and `loadgen` (one 2-CPU set on the dev machine, so the three are equal), each with `softnet_dropped` and `softnet_squeezed`. `conntrack_stat` is null in every phase and in `garbage_delivered` (no `/proc/net/stat/nf_conntrack`).

Then the forced failures, each in its own run (`rm -rf tier2-results/floods-*` first):
- **The heartbeat gap and the conntrack peak.** In a second copy `D2=$S/dry2`, made by the same `cp` and `sed` lines, point `CT_PROC` at fake files and lower the gap bound below the interval: `mkdir -p "$D2/ct"; echo 801 >"$D2/ct/nf_conntrack_count"; echo 1000 >"$D2/ct/nf_conntrack_max"; sed -i "s|^CT_PROC=.*|CT_PROC=$D2/ct|; s|(HB + 5) \* 1000|(HB - 5) * 1000|" "$D2/floods.sh"`. Then run `FLOODS_PHASES=garbage FLOODS_CLIENTS=2000 FLOODS_RATE=1000 FLOODS_GARBAGE_RATE=2000 FLOODS_GARBAGE_INFLIGHT=500 FLOODS_GARBAGE_SECONDS=45 FLOODS_GARBAGE_BUCKET=500` (with the same `GOIM_TEST_TIER` and CPU sets). Expected (prototype): `garbage_heartbeats` fails with `ping_gap_max_ms` 30,002 and every other field as on the pass path (`ping_send_timeouts` 0). `garbage_conntrack` fails with `peak` 801, `max` 1000, `conns` 2,500, `peak_per_conn` 0.3204, `samples` 52.
- **NOTRACK rules that don't match** (review O3). In `D2`, restore the gap bound (`sed -i "s|(HB - 5) \* 1000|(HB + 5) * 1000|" "$D2/floods.sh"`), set `echo 300 >"$D2/ct/nf_conntrack_count"; echo 1000000 >"$D2/ct/nf_conntrack_max"` and run `FLOODS_PHASES=storm FLOODS_STORM_CLIENTS=2000 FLOODS_STORM_RATE=1000`. Expected: `storm_conntrack` fails with `peak` 300, `max` 1,000,000 (0.03% of it), `conns` 2,000, `peak_per_conn` 0.15. Then with `echo 100` it passes, `peak_per_conn` 0.05.
- **A loadgen-bound storm passes.** `T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=1 T2_CPUS_LOADGEN=0 FLOODS_PHASES=storm FLOODS_STORM_CLIENTS=20000 FLOODS_STORM_RATE=20000`. Loadgen needs its CPU to itself here: with the gateway also on CPU 0 (sets `0-1`), the prototype read `loadgen_cores_ramp` 0.847 and got `short`. Expected: `storm_rate` passes with `verdict` `"loadgen-bound"` (prototype: `new_per_s` 1,539, `loadgen_cores_ramp` 0.948, `loadgen_cores` 0.966 of 1 CPU). `not_covered` starts with `storm: 20000 new clients/s at the gateway; loadgen was the bound (storm_rate.loadgen_cores_ramp)`. `storm_online` fails, as in 6b3b's O2 check.
- **A short storm fails.** The same with all three sets `0-1`. Expected: `storm_rate` fails with `verdict` `"short"` (prototype: `new_per_s` 951, `loadgen_cores_ramp` 1.076 of 2 CPUs), with no `storm:` line in `not_covered`. A run whose samples are too sparse for `new_per_s` (null) also gives `"short"`.
- **The bucket's `not_covered` line** (review O6) can't be reached on the dev machine: with `FLOODS_GARBAGE_BUCKET=100000` at 8,000/s, the dev gateways refused nothing (`no_slot` and `no_token` 0, `dh_per_s` ~3,300 each), so `garbage_limited` fails and no line is written, as it should. The condition is checked on JSON instead: `echo '{"gw-0": {"hs_no_token": 0, "hs_no_slot": 5}, "gw-1": {"hs_no_token": 0, "hs_no_slot": 3}}' | lg_true - 'all(v["hs_no_token"] == 0 and v["hs_no_slot"] > 0 for v in d.values())'` (after sourcing `lib.sh` and `loadgen.sh`) prints `true`. With `hs_no_token` 2 on gw-0 it prints `false`, and with gw-0's `hs_no_slot` 0, `false`.

Afterwards, remove `tier2-results/floods-*`, `$D` and `$D2`, and check that no process is left (`pgrep -af "scratchpad/dry" | grep -v pgrep || echo none`).

- [ ] **Step 4: Commit**

```bash
git add scripts/tier2/floods.sh
git commit -m "feat(tier2): floods: ping gap and conntrack gates (vs max and the phase's connections), storm_rate's loadgen-bound verdict over the ramp, garbage_limited on the whole limiter with the bucket in not_covered, CPU sets and conntrack stats per phase (#123, #124)"
```

- [ ] **Step 5: Verify the whole branch**

Run: `make test-scripts && make test && go vet ./... && bash -n scripts/tier2/*.sh scripts/tier2/goim-netdrop`
Expected: `lib_test`, `loadgen_test` and `stack_test` print `all passed`, and `make test` is ok. Record the commits and the dry run in this plan's *Execution Notes* and push the branch.

### Task 5: Host setup, Tier 2 run 30b and the record

**Files:**
- Modify: this plan's *Execution Notes*, the roadmap (plan 6 row, #123, #124), Plan 6b3b's *Execution Notes* (one line pointing here), `README.md` (both languages) only if run 30b passes.

- [ ] **Step 1: Host setup (the user, on the Tier 2 host, once)**

After `git fetch && git switch feat/plan6b3 && git pull`:
1. `sudo iptables -S INPUT` and `sudo iptables -S OUTPUT`: in each, `lo` (`-i lo` in INPUT, `-o lo` in OUTPUT) is accepted before any `--ctstate` rule, or the policy is `ACCEPT` (final review M5: an OUTPUT chain that accepts only `ESTABLISHED` drops UNTRACKED packets too). Then `sudo nft list ruleset`: no input or output hook chain accepts only by `ct state` without an earlier `iifname "lo" accept` / `oifname "lo" accept` (Review Focus 2). `host-setup.sh` prints these three commands at its end.
2. `sudo scripts/tier2/host-setup.sh`: expect `installed: /usr/local/sbin/goim-netdrop` (it changed), `applied: NOTRACK for ports 17000-17199 on lo`, `host-setup: done`.
3. `sudo -n /usr/local/sbin/goim-netdrop notrack-check; echo $?` prints `0` only. `sudo iptables -t raw -S` shows the four `goim-notrack` rules.

After any reboot, step 2 is needed again, and the guard refuses a run until then with `the NOTRACK rules for ports 17000-17199 on lo are missing (not kept across a reboot)`.

- [ ] **Step 2: Tier 2 run 30b (on the host, ~12 min)**

With the desktop apps closed, the user runs `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=floods` and sends back the `.report.txt`. There are 30 checks: run 30's 27 plus `storm_conntrack`, `garbage_conntrack`, `slow_conntrack`. Expected for run 30's six failures (review O7):
- `storm_rate`: passes, `met` or `loadgen-bound` (Review Focus 4). `short` is a real failure.
- `garbage_limited`: passes. Run 30 had `no_slot` > 0 on both gateways. Expect a `not_covered` line for the bucket.
- `slow_heartbeats`, `slow_legit`: should pass, now that the table can't fill (NOTRACK) and the pinger can't fall behind or block (catch-up, deadline).
- `garbage_delivered`: **may still fail.** Nothing here targets its SYN loss, though NOTRACK may cure it (Review Focus 5).
- `slow_held`: **may still fail.** Run 30's slowhs `max_ms` was 7,782 against 7.5s, inside a phase made invalid by the full table. Nothing here targets it. If it fails again with a valid phase, its 7.5s bound or loadgen's timing of the hold is the next question.

A run that fails only `garbage_delivered` and/or `slow_held` is still a valid record. The README row waits until they pass or are ruled on.

What to read, for the *Execution Notes* and the roadmap:
- **NOTRACK:** each `<phase>_conntrack` (`peak`, `max` 4,194,304, `conns`, `peak_per_conn`, gated under 0.1, Review Focus 1) and each phase's `conntrack_stat` (zeros with NOTRACK working). `dmesg | grep conntrack` shows no `table full` during the run.
- **storm:** `storm_rate.verdict`, `new_per_s`, `loadgen_cores` of `loadgen_cpus`, `gateway_cores`, and `storm.cpu_sets` (softirq and idle of each set). For a `loadgen-bound` verdict, gw-0's set must have idle to spare (Review Focus 4). `storm_bucket.no_slot_share` against run 30's 0.267.
- **garbage:** `garbage_delivered` (`attempts`, `skipped`, `outcomes`, `tcp.TCPSynRetrans`, `cpu_sets`' `softnet_dropped`, `conntrack_stat`, Review Focus 5), `garbage_limited.refused` (`bound_by` per gateway, `dh_per_s`: #111's input), the heartbeats' pinger fields (`ping_send_timeouts` among them).
- **slow:** valid this time? (no `table full`, few `dial-timeout`.) `slow_held` max against 7.5s, `slow_legit`, `slow_heartbeats` (`ping_gap_max_ms` under 35,000, no heartbeat closes), `ping_tick_lag_max_ms`, `ping_send_max_ms` and `ping_send_timeouts` (Review Focus 3).
- If a check fails: a `_conntrack` failure on `peak_per_conn` means Review Focus 1 (rule shape), and on `max` means a full table. Dial timeouts from the first dial mean Review Focus 2. A `_heartbeats` failure on the gap: read `ping_tick_lag_max_ms` against `ping_send_max_ms`. On `lost`, read `ping_send_timeouts` (Review Focus 3). `storm_rate` `short` means loadgen fell short below its CPU bound over the ramp, so read `loadgen_cores_ramp` and `storm.cpu_sets.loadgen` (softirq, softnet). `garbage_delivered` means Review Focus 5.

- [ ] **Step 3: Record**

- This plan's *Execution Notes*: run 30b's numbers above, with the commit hash and `dirty` from `result.json`.
- Roadmap: #123 and #124 each get one line with run 30b's result. The plan 6 row: "6b3b2 executed, run 30b …; next 6b3c (fresh session)", or the follow-up the run calls for. Under *Dev machine and test tiers*, one line (review O10): Tier 2 runs up to and including run 30 tracked the stack's flows in conntrack, and runs from 30b on untrack ports 17000–17199 on `lo` (#123). Results from before and after aren't directly comparable where per-flow kernel cost matters (capacity, gwloss, slowlogic).
- Plan 6b3b's *Execution Notes*: one line, "Run 30b: see Plan 6b3b2".
- If run 30b passes in full, add the README *Measured results* row in both languages, as Plan 6b3b's Task 3 Step 2 describes (**Floods**: the storm's verdict and `no_slot_share`, garbage at 80k/s beside 400k Pinging clients with Pong p99 and none missing, slow clients freed in ~5.5s with 400k logins and no Busy). Move "floods" from *Next* to *Done* in the status paragraph.

```bash
git add README.md docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-16-connection-session-core-06b3b-floods.md docs/superpowers/plans/2026-10-16-connection-session-core-06b3b2-run30-followups.md
git commit -m "docs: Tier 2 run 30b, floods recorded"
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **6b3c** (`POST /reset`, `POST /network-change`, scenario `reconnect`, run 31) gets `floods`' gates as they stand after this plan: `<phase>_heartbeats` with the gap bound, `<phase>_conntrack` (6b3b's I-3 note: `floods`' helpers move to `lib.sh` in 6b3c Task 1, and `ct_start`/`ct_check` move with them).
- **#111 (parked):** run 30b's `no_slot_share` and `garbage_limited.bound_by` are its input. Unparking it is the user's call.
- **The spec:** nothing here changes its text. The gauge implements a line it already has. #125 (review O2) queues a follow-up for the next revision, with #97 and #101: run 30 showed that `nf_conntrack_max ≥ 2 × cap` doesn't survive a handshake flood.

## Execution Notes

**Executed 2026-10-17** (inline, superpowers:executing-plans), on `feat/plan6b3` from `512c5cb`:
- `1793f91` Task 1, the pinger. The four mutations were caught as the plan says.
- `8ca6336` Task 2, the gauge.
- `d960a9a` Task 3, NOTRACK and the guard. 12 `notrack` oks.
- `f0145d0` Task 4, `floods`. The checks below all passed.

`make test`, `make test-scripts`, `go vet ./...` and `go test -race` on the changed packages are green.

**Dry run** (dev machine):
- 30 checks. Only `no_swap_in_*` failed (1, 29 and 558 pages of the dev machine's own swapping).
- `storm_rate`: `met`, `new_per_s` 957 of 1,000, `loadgen_cores_ramp` 0.745 of 2 CPUs.
- `garbage_limited`: both gateways `bound_by` `bucket` (gw-0 `no_token` 33,831, gw-1 6,386).
- `ping_gap_max_ms`: 30,005 in garbage and 30,001 in slow. Tick lag ≤ 12ms, no Send timeouts.
- `_conntrack`: null peaks (no conntrack on the dev machine), `conns` 2,000, 2,500 and 3,000.

**Forced cases** (each as the plan expects, except two differences noted):
- **Gap and conntrack max:** `garbage_heartbeats` and `garbage_conntrack` failed (801 of 1000, `peak_per_conn` 0.3204).
- **Connections share:** with 300 of 1M, `storm_conntrack` failed at `peak_per_conn` 0.15. With 100 it passed at 0.05.
- **Loadgen-bound:** verdict `loadgen-bound`, `loadgen_cores_ramp` 0.919 of 1 CPU, with the `storm:` `not_covered` line. Different from the prototype:
  - `new_per_s` was null, because the starved sampler's gaps reached 2.7s. The verdict comes from the CPU reading, so it still holds.
  - `storm_online` passed, because the dev machine got all 20k clients online this time.
- **Short:** verdict `short`, `new_per_s` 772, 1.073 of 2 CPUs.
- **Bucket `not_covered` condition** (checked on JSON): true, false, false.

**Final review** (Opus, fresh context) found 0 Critical and 1 Important.
- **I-1, fixed `f82adb2`:** no test ran the pinger's real `send`, so nothing would catch a missing `SetWriteDeadline`. The new test is `TestPingerTimesOutABlockedSend/the_real_Send`: a handshaken `net.Pipe` whose peer never reads. It fails with the deadline line removed and passes with it.
- **Minors, all fixed on the user's request** (see the fix line below):
  1. The storm verdict counts `T2_CPUS_LOADGEN`, not `lg_start`'s `${LG_CPUS:-…}`. With an empty set and a null reading it would give `loadgen-bound`.
  2. The bucket's `not_covered` line is written only when every gateway is bound by the slots alone, so a mixed case writes none.
  3. The write deadline stays set after a Ping, so a write added later in `read()` would inherit it.
  4. `start` is taken after `NewTicker`, so ticks come a few ns early. This was harmless in the reviewer's probe.
  5. Task 5's firewall check should cover `OUTPUT` too.
  6. A non-numeric `STACK_PORT_BASE` is refused with an empty message.
  7. Each Ping takes the Recorder lock 2–3 times. Watch `ping_send_max_ms` in run 30b.

Next: Task 5 on the host (the user): `sudo scripts/tier2/host-setup.sh` after the firewall checks, then run 30b.

**Minors fixed 2026-10-17** (the user: "fix all the minors"):
1. `storm_verdict` counts `${LG_CPUS:-$T2_CPUS_LOADGEN}` and needs a non-empty set and a non-null reading. Checked on JSON: the old expression gave `loadgen-bound` with 0 CPUs and no reading, the new one `short`.
2. The bucket's `not_covered` line is written per gateway bound by the slots alone (`slots_only`). Checked on JSON: mixed → `gw-1`, both → `gw-0 gw-1`, none → empty.
3. `lightConn.c`'s comment says only the pinger writes after login and its deadline stays set.
4. `start` is taken before `NewTicker`.
5. Task 5's check covers `OUTPUT` too, and `host-setup.sh` names the three commands.
6. `t2_notrack_covers` refuses a non-numeric base with a message. New `lib_test` checks went RED, then GREEN.
7. The pinger records once per visit (`Recorder.PingVisit`) instead of 2–3 times per Ping, covered by the existing pinger tests.

Verified: `make test`, `make test-scripts`, `go vet ./...`, `go test -race` on loadgen, and a full `floods` dry run (30 checks, only dev swap failing).

**Run 30b** (2026-10-17, the user, Tier 2 host; commit `85e79af`, `dirty` 0, 427s; `floods-20261017-015447`). **Failed 28/30**: `slow_legit` and `slow_heartbeats`. Host check before it: INPUT and OUTPUT policy `ACCEPT` (INPUT jumps only to Tailscale's `ts-input`, whose one drop is for 100.64.0.0/10 sources), no native nft table; `notrack-check` 0, the four `goim-notrack` rules in raw.

What run 30's six failures did (Task 5 Step 2's expectations):
- **NOTRACK:** `storm_conntrack` peak 18, `garbage_conntrack` 19, `slow_conntrack` 22 of 4,194,304 (`conns` 200,000, 420,000 and 520,000; `peak_per_conn` ≤ 0.0001). `conntrack_stat` null in every phase (the host has no `/proc/net/stat/nf_conntrack`). Review Focus 1 holds.
- **`storm_rate`:** passes, `loadgen-bound`. `loadgen_cores_ramp` 7.929 of 8, `loadgen_cores` 6.782, `new_per_s` 15,945 (of 20,000), `seen_per_s` 20,780. gw-0 used 5.31 cores, and its 12-CPU set was 55.5% idle, so the gateway had room (Review Focus 4). `storm_bucket.no_slot_share` 0.2664 (run 30: 0.267).
- **`garbage_delivered`:** passes. 7,206,236 attempts (want 6,840,000), `skipped` 0, `TCPSynRetrans` 8 (run 30: 437k), no softnet drops on any set. NOTRACK was the cure (Review Focus 5).
- **`garbage_limited`:** passes. `bound_by` `slots` on both gateways (`no_slot` 1,747,883 and 1,603,317, `no_token` 0), `dh_per_s` 21,634 and 21,179, and a `not_covered` line per gateway for the bucket (25,000/s). These are #111's inputs.
- **`garbage_heartbeats`:** passes. 1,497,990 Pongs, none missing, p99 4ms. `ping_gap_max_ms` 30,063, `ping_tick_lag_max_ms` 61, `ping_send_max_ms` 14, `ping_send_timeouts` 0.
- **`slow_held`:** passes. slowhs max 6,542ms and slowauth 6,496ms (bound 7.5s; run 30: 7,782 in an invalid phase).
- **`slow_legit`: fails.** 90,334 of 400,000 online, 19,097 gave up. Failures: `dial-timeout` 425,619, `handshake` 612,147, `pre-handshake` 284,561, `login` 159,389. The gateways refused legitimate logins too: `rejects_pending_mem` 63,006 and 66,026, `hs_no_slot` 157,018 and 154,271, `logins_busy` 0.
- **`slow_heartbeats`: fails.** `lost` 142,250, `closes_heartbeat` 71,247 and 70,981, `pong_missing` 1,616, Pong p99 9,231ms. `ping_gap_max_ms` 155,087, `ping_tick_lag_max_ms` 123,302, `ping_send_max_ms` 121,559, `ping_send_timeouts` 37.

Reading of the slow phase:
- **Loadgen's CPU set was saturated:** 84% user, 3.6% idle over 244s. Its three processes used 7.94 of its 8 CPUs: the light instance 4.42 cores and 11.6GB RSS (3.5GB in garbage), slow-auth 2.68, slow-handshake 0.83. The gateways' set was 69.5% idle and logic's 94.6%.
- **The pinger was starved, not blocked:** a 121s `Send` with a 1s write deadline, and only 37 timeouts, means the goroutine wasn't running, not that it waited on a socket. Hypothesis, not yet checked: the 8 pinger goroutines waited behind a large run queue of retrying light clients (Noise handshakes are CPU-heavy). This is the case Review Focus 3 names: large tick lag means a starved goroutine.
- **Open questions:** how much of `slow_legit` is the gateways' own `pending_mem` and slot refusals (the spec's slow-client row expects no such refusals of legitimate clients), and how much is loadgen's saturation feeding retries. The run has no time series to order the two.
- **Not changed:** `dmesg` wasn't sent. The README row waits.

- **Decided (the user, 2026-10-17), #126:** Plan 6b3b3, with own CPUs for the slow floods, a per-second slow-phase series and a loadgen-saturation verdict, then run 30c.
