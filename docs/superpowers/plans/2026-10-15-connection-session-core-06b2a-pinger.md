# Plan 6b2a-pinger: the SDK's Pinger as a Timer (b-stress) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An online SDK client holds one goroutine for its connection, not two, so b-stress's loadgen (600k SDK clients) fits under its 16GiB `GOMEMLIMIT`.

**Architecture:** `attempt.serve` now starts `attempt.ping` as a goroutine of its own, which sleeps on a `time.Timer` between Pings. This plan moves that loop's body into a `pinger` driven by `time.AfterFunc`. Between firings no goroutine exists; each firing runs briefly on a goroutine the runtime starts for the callback. Behaviour (when Pings go out, when a missing Pong closes the connection) stays as it is, and the existing synctest tests pin it. Then the smoke run measures memory per SDK client again, and b_stress runs at 16GiB.

**Tech Stack:** Go 1.27.1 (`time.AfterFunc`, `testing/synctest`, `runtime.Stack`), bash Tier 2 scripts.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (*Dead-server detection*: one Ping per heartbeat; close and reconnect when no frame arrives within 10s of a Ping). Inputs: plan 6b2a-reuse's *Execution Notes* (`docs/superpowers/plans/2026-10-14-connection-session-core-06b2a-reuse.md`). The 16GiB diagnostic run (`517cfba`) had 1.80M goroutines at the end (three per online client: `Client.run`, `attempt.run`, `attempt.ping`) and 9.83GB of stacks (~5.5KB each). The 19GiB run (`96b5733`) got all clients back in 208s with GC calm, but the host swapped.

**Review (2026-10-15):** objections O1–O5 (`docs/superpowers/objections/connection-session-core-06b2a-pinger.md`) decided and applied (`…-06b2a-pinger-dispositions.md`): `stop` does not wait for a firing (O1, O2), the dry run measures the saving before and after (O3), a numeric gate (O4).

## Global Constraints

- The SDK's observable behaviour does not change: first Ping at one heartbeat after the Login reply, then one per heartbeat; the connection is closed (`EndPongTimeout`) when no frame arrives within `PongTimeout` (10s) of the oldest Ping sent since the last frame.
- No new SDK `Config` fields and no API changes; loadgen (`internal/loadgen/sdkclient.go`) is unchanged.
- b_stress keeps loadgen's `GOMEMLIMIT` at 16GiB (`GWLOSS_STRESS_LG_MEMLIMIT` default). Results must hold without the host's UMA change (memory `tier2-host-uma`).
- `go test -race ./internal/sdk/... ./internal/loadgen/... ./cmd/loadgen/...`, `make test` and `make test-scripts` stay green.

## Review Focus

1. **A firing that races `stop`.** When the connection ends (Kick, Reconnect, read error), `serve` stops the pinger while a firing may be running or due. Expected: `stop` returns at once; after it, no firing re-arms the timer, so nothing more is sent or reported. A firing already past its `stopped` check may still finish its one `Send` or `timeout`, harmlessly: the connection is closed first, and `serve` has stopped reading the timeout flag (review O2). Pinned by `TestPingerStopsForGood` (Task 1).
2. **The Pong timeout on the timer's goroutine.** The close now runs on the callback's goroutine, not on a goroutine `serve` started. Expected: `serve`'s `Recv` fails and the end is still `EndPongTimeout`, not `EndLost`. Pinned by the existing `TestPongTimeoutClosesAndReconnects` and by `TestPingerTimesOutOncePongTimeoutAfterAPing` (Task 1).
3. **A `Send` that blocks** (a full socket send buffer). Expected: the firing waits in `Send` as the goroutine used to, without `mu`; `stop` does not wait for it, so a blocked `Send` cannot hold up `serve` (review O2). `serve`'s deferred `Close` still runs before `stop`, so the `Send` fails at once, but that order is no longer load-bearing.
4. **Goroutines per online client.** Expected: one goroutine holds an `attempt` frame per online client, not two. Pinned by `TestAnOnlineClientHoldsOneAttemptGoroutine` (Task 1).
5. **Heartbeat Pings answered over a long time.** Expected: no spurious timeout over many heartbeats once fires come from a timer. Pinned by `TestPingerKeepsPingingWhileAnswered` (Task 1) and the existing 10-minute tail of `TestPongTimeoutClosesAndReconnects`.

## Implementation choices this plan makes

- **`pinger` in its own file** (`internal/sdk/pinger.go`) with `send` and `timeout` as functions, so it is tested without a connection. `attempt.serve` wires it to `a.cc`.
- **A mutex around the state, not around `send`/`timeout`** (review O2). Firings never overlap (one timer, re-armed at the end of each firing), but the race detector sees no happens-before edge from `Reset` to the next callback; the pinger's state is touched only under `mu`, which gives it one. `fire` releases `mu` while it calls `send` or `timeout`, and `stop` (under `mu`: `stopped`, `t.Stop()`) never waits for a firing: holding `mu` through a `Send` would let a blocked `Send` hold up `serve`, a hang the old goroutine (cancelled, never waited for) could not have. Nothing needs `stop` to be exact: after `serve`'s `Close` a late `Send` fails, and a late `timeout` sets a flag `serve` no longer reads and closes a closed socket. What must hold is that no firing re-arms the timer after `stop`: `fire` checks `stopped` on entry and again before `Reset`.
- **No context.** The old goroutine also watched the attempt's context. A cancel closes the socket (`context.AfterFunc` in `login`), `serve`'s `Recv` fails and `serve` stops the pinger, so the context adds nothing.

## File Structure

- Create `internal/sdk/pinger.go`: `pinger`, `startPinger`, `(*pinger).fire`, `(*pinger).stop`.
- Create `internal/sdk/pinger_test.go`: the pinger's unit tests and the goroutine count.
- Modify `internal/sdk/attempt.go`: `serve` starts a `pinger`; `attempt.ping` is removed.
- Modify `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`, this plan's *Execution Notes*, and the reuse plan's status line (Task 2).

---

### Task 1: the pinger runs on a timer

**Files:**
- Create: `internal/sdk/pinger.go`
- Create: `internal/sdk/pinger_test.go`
- Modify: `internal/sdk/attempt.go` (`serve`, and `ping` removed)

**Interfaces:**
- Consumes: `clientconn.Conn.Send(cmd, reqID, msg) error`, `clientconn.Conn.Close() error`; test helpers `newFake`, `testClient`, `expect`, `(*fakeConn).online`, `(*fakeConn).pong` (`internal/sdk/fake_test.go`).
- Produces: `startPinger(hb, pongTimeout time.Duration, start time.Time, lastRecv *atomic.Int64, send func(reqID uint32) error, timeout func()) *pinger`; `(*pinger).stop()`.

- [ ] **Step 1: Write the failing tests**

`internal/sdk/pinger_test.go`:

```go
package sdk

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// pingLog records a pinger's Pings and timeouts, as offsets from start.
type pingLog struct {
	mu       sync.Mutex
	start    time.Time
	pings    []time.Duration
	timeouts []time.Duration
	failSend bool
}

func (l *pingLog) send(uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failSend {
		return errors.New("broken")
	}
	l.pings = append(l.pings, time.Since(l.start))
	return nil
}

func (l *pingLog) timeout() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.timeouts = append(l.timeouts, time.Since(l.start))
}

func (l *pingLog) got() (pings, timeouts []time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Duration(nil), l.pings...), append([]time.Duration(nil), l.timeouts...)
}

// Review Focus 2: with no frame after the first Ping, the pinger reports a
// timeout PongTimeout after it, once, and sends nothing more.
func TestPingerTimesOutOncePongTimeoutAfterAPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &pingLog{start: time.Now()}
		var lastRecv atomic.Int64
		p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, l.send, l.timeout)
		defer p.stop()
		time.Sleep(10 * time.Minute)
		pings, timeouts := l.got()
		if len(pings) != 1 || pings[0] != 30*time.Second {
			t.Fatalf("pings %v, want [30s]", pings)
		}
		if len(timeouts) != 1 || timeouts[0] != 40*time.Second {
			t.Fatalf("timeouts %v, want [40s]", timeouts)
		}
	})
}

// Review Focus 5: a frame after each Ping keeps the pinger going, one Ping
// per heartbeat, with no timeout.
func TestPingerKeepsPingingWhileAnswered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := &pingLog{start: time.Now()}
		var lastRecv atomic.Int64
		p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, func(id uint32) error {
			err := l.send(id)
			go func() { // the Pong, 1s later
				time.Sleep(time.Second)
				lastRecv.Store(int64(time.Since(l.start)))
			}()
			return err
		}, l.timeout)
		defer p.stop()
		time.Sleep(10*time.Minute + time.Second)
		pings, timeouts := l.got()
		if len(pings) != 20 || pings[19] != 600*time.Second {
			t.Fatalf("pings %v, want 20, the last at 600s", pings)
		}
		if len(timeouts) != 0 {
			t.Fatalf("timeouts %v, want none", timeouts)
		}
	})
}

// Review Focus 1: after stop, no firing re-arms, whether stop comes between
// firings or during one (stop does not wait for it), and stop twice is
// harmless.
func TestPingerStopsForGood(t *testing.T) {
	t.Run("between firings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &pingLog{start: time.Now()}
			var lastRecv atomic.Int64
			p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, l.send, l.timeout)
			time.Sleep(35 * time.Second) // one Ping out, its timeout due at 40s
			p.stop()
			p.stop()
			time.Sleep(10 * time.Minute)
			if pings, timeouts := l.got(); len(pings) != 1 || len(timeouts) != 0 {
				t.Fatalf("pings %v timeouts %v, want one Ping and no timeout", pings, timeouts)
			}
		})
	})
	t.Run("during a firing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &pingLog{start: time.Now()}
			var lastRecv atomic.Int64
			inSend, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, func(id uint32) error {
				once.Do(func() {
					close(inSend)
					<-release // a Send blocked on a full socket buffer
				})
				return l.send(id)
			}, l.timeout)
			<-inSend
			p.stop() // returns at once: it does not wait for the firing (review O2)
			close(release)
			time.Sleep(10 * time.Minute)
			// The released firing finishes its Ping but does not re-arm: no
			// timeout at 40s, no second Ping.
			if pings, timeouts := l.got(); len(pings) != 1 || len(timeouts) != 0 {
				t.Fatalf("pings %v timeouts %v, want the one Ping and no timeout", pings, timeouts)
			}
		})
	})
	t.Run("a failed Send stops the Pings", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			l := &pingLog{start: time.Now(), failSend: true}
			var lastRecv atomic.Int64
			p := startPinger(30*time.Second, 10*time.Second, l.start, &lastRecv, l.send, l.timeout)
			defer p.stop()
			time.Sleep(10 * time.Minute)
			if pings, timeouts := l.got(); len(pings) != 0 || len(timeouts) != 0 {
				t.Fatalf("pings %v timeouts %v, want none: the reader sees the broken connection", pings, timeouts)
			}
		})
	})
}

// Review Focus 4 (plan 6b2a-pinger): an online client holds one goroutine
// in its attempt (serve), not a second one for the pinger. b-stress's
// loadgen runs 600k SDK clients; each goroutine costs ~5.5KB of stack.
func TestAnOnlineClientHoldsOneAttemptGoroutine(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(&clientpb.LoginResp{HeartbeatS: 30})
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(time.Second)
		expect(t, r.take(), EventConnecting, EventOnline)
		synctest.Wait()
		buf := make([]byte, 1<<20)
		buf = buf[:runtime.Stack(buf, true)]
		n := 0
		for _, g := range strings.Split(string(buf), "\n\n") {
			if strings.Contains(g, "internal/sdk.(*attempt).") {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%d goroutines in an attempt, want 1 (serve)\n%s", n, buf)
		}
	})
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/sdk/ -run 'TestPinger|TestAnOnlineClientHoldsOneAttemptGoroutine' -v`
Expected: build failure, `undefined: startPinger` and `undefined: pinger`. To see `TestAnOnlineClientHoldsOneAttemptGoroutine` fail for the right reason first, comment out the three `TestPinger*` functions and `pingLog` and run it alone. Expected: `2 goroutines in an attempt, want 1 (serve)`, with `(*attempt).ping` in the dump. Then restore them.

- [ ] **Step 3: Implement**

`internal/sdk/pinger.go`:

```go
package sdk

import (
	"sync"
	"sync/atomic"
	"time"
)

// pinger sends a Ping every hb and reports a timeout when no frame has
// arrived within pongTimeout of the oldest Ping sent since the last frame
// (spec: Dead-server detection). It runs on a timer, not on a goroutine of
// its own: an online client holds one goroutine for its connection, not
// two (plan 6b2a-pinger: b-stress's loadgen runs 600k SDK clients).
type pinger struct {
	hb, pongTimeout time.Duration
	start           time.Time
	lastRecv        *atomic.Int64 // ns since start of the last frame received
	send            func(reqID uint32) error
	timeout         func()

	// mu guards the state below, not send or timeout: stop never waits for
	// a firing (a blocked Send must not hold up serve), and a firing checks
	// stopped before it re-arms, so none runs after the one in progress.
	mu         sync.Mutex
	t          *time.Timer
	stopped    bool
	reqID      uint32
	next       time.Duration // next Ping, since start
	unanswered time.Duration // send time of the oldest Ping no frame has followed; -1: none
}

// startPinger arms the first Ping one heartbeat after start.
func startPinger(hb, pongTimeout time.Duration, start time.Time, lastRecv *atomic.Int64, send func(reqID uint32) error, timeout func()) *pinger {
	p := &pinger{hb: hb, pongTimeout: pongTimeout, start: start, lastRecv: lastRecv,
		send: send, timeout: timeout, reqID: 1, next: hb, unanswered: -1}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.t = time.AfterFunc(hb-time.Since(start), p.fire)
	return p
}

// fire sends a Ping when one is due, reports a timeout when the oldest
// unanswered Ping is pongTimeout old, and otherwise arms the next firing.
func (p *pinger) fire() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	now := time.Since(p.start)
	if p.unanswered >= 0 && p.lastRecv.Load() >= int64(p.unanswered) {
		p.unanswered = -1
	}
	if p.unanswered >= 0 && now >= p.unanswered+p.pongTimeout {
		p.mu.Unlock()
		p.timeout()
		return
	}
	ping := now >= p.next
	if ping {
		p.reqID++
	}
	reqID := p.reqID
	p.mu.Unlock()

	if ping && p.send(reqID) != nil {
		return // the reader sees the broken connection
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return // stopped during the Send: do not re-arm
	}
	if ping {
		if p.unanswered < 0 {
			p.unanswered = now
		}
		p.next = now + p.hb
	}
	at := p.next
	if p.unanswered >= 0 {
		at = min(at, p.unanswered+p.pongTimeout)
	}
	p.t.Reset(at - time.Since(p.start))
}

// stop ends the pinger without waiting for a firing in progress: that
// firing may still finish its Send or timeout, harmlessly (serve closes the
// connection first), but it does not re-arm, and none runs after it.
func (p *pinger) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.t.Stop()
}
```

In `internal/sdk/attempt.go`, replace `serve`'s pinger setup and delete `attempt.ping` (the whole function and its comment):

```go
func (a *attempt) serve(ctx context.Context, cfg *Config, resp *clientpb.LoginResp, report func(attemptMsg) bool) end {
	hb, _ := cfg.Heartbeat.Of(resp)
	start := time.Now()
	var lastRecv atomic.Int64 // ns since start of the last frame received
	var pongTimedOut atomic.Bool
	p := startPinger(hb, cfg.PongTimeout, start, &lastRecv,
		func(reqID uint32) error { return a.cc.Send(clientpb.Cmd_CMD_PING, reqID, nil) },
		func() { pongTimedOut.Store(true); a.cc.Close() })
	defer p.stop()
	defer a.cc.Close() // before stop: a firing still in Send fails at once
	for {
```

The rest of `serve` is unchanged. `ctx` stays a parameter (the signature is used by `run`). If `go vet` reports it unused, keep it and name it `_`. Update `serve`'s doc comment from "one Ping per heartbeat interval" to "one Ping per heartbeat interval from a pinger". Remove the `serve` comment's mention of a ping goroutine if there is one.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race -count=3 ./internal/sdk/`
Expected: PASS, including `TestPongTimeoutClosesAndReconnects`, `TestHeartbeatIsClampedToItsRange`, `TestMissingHeartbeatUsesTheDefault`, `TestEventsSayWhatEndedTheConnection` (its Pong-timeout case).

Then `gofmt -l internal/sdk` (empty), `go vet ./internal/sdk/`, and `go test -race ./internal/loadgen/... ./cmd/loadgen/... ./internal/racetest/...`.
Expected: all pass.

- [ ] **Step 5: Mutation checks**

Make each change, run the named test, confirm it fails, then revert:
1. In `fire`, drop both `stopped` checks (on entry and before `Reset`). Expected: `TestPingerStopsForGood/during_a_firing` fails: the released firing re-arms at 40s and reports a timeout ("timeouts [40s]"). Dropping only one does not fail it, and that is expected: the entry check stops the re-armed 40s firing, and the check before `Reset` stops the re-arm. The entry check alone also covers a callback the timer had already started when `stop` ran; no deterministic test reaches that order, so it is reviewed by reading.
2. In `fire`, drop `p.unanswered = now`. Expected: `TestPingerTimesOutOncePongTimeoutAfterAPing` fails (no timeout).

- [ ] **Step 6: Commit**

```bash
git add internal/sdk/pinger.go internal/sdk/pinger_test.go internal/sdk/attempt.go
git commit -m "feat(sdk): the pinger runs on a timer, not a goroutine: one goroutine per online client's connection (plan 6b2a-pinger)"
```

---

### Task 2: measure, record, run b_stress at 16GiB

**Files:**
- Modify: `docs/superpowers/plans/2026-10-15-connection-session-core-06b2a-pinger.md` (*Execution Notes*)
- Modify: `docs/superpowers/plans/2026-10-14-connection-session-core-06b2a-reuse.md` (status line)
- Modify: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md` (a note under #93's SDK sizing)
- Modify, only if the user confirms the UMA change is made: the roadmap's host line (~line 148, "26GB RAM / UMA 4096MB") and `scripts/tier2/gwloss.sh`'s `mem=23` and its comment (lines ~163–172)

**Interfaces:**
- Consumes: Task 1's SDK; `scripts/tier2/smoke.sh` (`SMOKE_SDK`), `scripts/tier2/gwloss.sh` (`GWLOSS_PHASES`, `GWLOSS_SS_EVERY`, loadgen's `GET /runtime` samples).
- Produces: per-SDK-client RSS after the change, and b_stress's result at 16GiB.

- [ ] **Step 1: Dry run on the dev machine**

Run: `GWLOSS_PHASES=b_stress GWLOSS_STRESS_CLIENTS=1500 GWLOSS_SS_EVERY=2 make test-tier2 SCENARIO=gwloss`. Use the same small-scale settings as the reuse plan's dry runs; check `gwloss.sh`'s header (lines 30–80) for the client, rate and cap variables. Run it twice (review O3): first on the commit before Task 1 (a temporary `git worktree` at it), then on Task 1's commit.
Expected: 15 checks each, only `no_swap_in_b_stress` may fail (the dev machine). From loadgen's last `/runtime` sample once all victims are back, record in each run the goroutines and stack bytes: goroutines about 3 per SDK client before (≈4,500 plus a constant) and 2 after (≈3,000). In the *Execution Notes*: stack bytes per online SDK client before and after, the difference, and the difference scaled to 600,000, against this plan's ~3.3GB. The 1,500-client figure is a guide (stack sizes may differ at scale), not the Tier 2 result.

- [ ] **Step 2: Smoke at 100k SDK clients (Tier 2 host)**

Run: `GOIM_TEST_TIER=2 SMOKE_SDK=100000 make test-tier2 SCENARIO=smoke`
Expected: 10/10. The SDK loadgen's RSS after the kill divided by 100,000 should fall below `ca7c6e9`'s 23.8KB per client, by about one goroutine stack (~5.5KB per online client, if that is what the stack costs at 100k). Record the measured figure, not this estimate.

- [ ] **Step 3: b_stress at 16GiB (Tier 2 host)**

Host prep as for run 25. Then run: `GOIM_TEST_TIER=2 GWLOSS_PHASES=b_stress GWLOSS_SS_EVERY=10 make test-tier2 SCENARIO=gwloss` (15 checks; `GWLOSS_STRESS_LG_MEMLIMIT` unset, so 16GiB).
Read it against the 16GiB diagnostic run (`517cfba`) and the 19GiB run (`96b5733`):
- loadgen goroutines at the end (1.80M before; expect ~1.2M) and stacks (9.83GB; expect ~6.5GB);
- mapped memory against the 16GiB limit (it reached the limit by t≈29 and stayed over it from t≈108);
- GC cycles over the storm and whether the CPU limiter engaged (in every cycle at 16GiB, never at 19GiB);
- time until all 600,000 are back (650s at 16GiB, 208s at 19GiB), and `connect_lag` p50/p99;
- `no_swap_in_b_stress` (762k pages in at 19GiB);
- `b_stress_loadgen_headroom` and `b_stress_storm_real`.
`b_stress_storm_real`'s 100ms p99 bound may still fail in the storm's first 30s, where loadgen used 12.2 of 14 cores without GC pressure (19GiB run). That is a separate question, not this plan's gate. This plan's gate (review O4):
1. loadgen's mapped memory ≤ `GOMEMLIMIT` (16GiB, 17.18GB) in every `/runtime` sample after all victims are back;
2. the GC CPU limiter engaged in no cycle after t≈60s;
3. `no_swap_in_b_stress` may fail with ≤ 1,000 pages swapped in: recorded, not gating (every 16GiB b_stress run so far had 1–272).
Run this step before the host's UMA change. If the UMA change is already made, say so in the *Execution Notes*; that run does not show the result holds without it (Global Constraints).

- [ ] **Step 4: Record**

Add to this plan's *Execution Notes*: the dry run, the smoke run (per-client RSS, commit hash), and the b_stress run, each with the numbers listed in Step 3. Update the reuse plan's status line ("**Next: option 2 …**") to name this plan and its outcome. Under #93 in the roadmap, add one line: the SDK client's goroutines per online connection (now 2: `Client.run`, the attempt's `serve`) and the new bytes per SDK client.
If the user has confirmed the UMA change: update the roadmap's host line from the run's `host.mem_gb`, and raise `mem=23` in `gwloss.sh` only if the run shows the room. Rewrite its comment with this run's loadgen peak.

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/plans scripts/tier2/gwloss.sh
git commit -m "docs: b_stress with the SDK pinger as a timer recorded (plan 6b2a-pinger)"
```

## After this plan

If gate 1 or 2 (Task 2 Step 3) fails, loadgen is still over its limit: stop and decide with the user between `Client.run`'s goroutine (the one left per online client besides `serve`), a smaller b_stress population, or recording b_stress as loadgen-bound (review O3).
If b_stress passes apart from `b_stress_storm_real`'s first-30s bound, decide that bound with the user: a second machine for loadgen, or record b_stress's storm as loadgen-bound (#102's `loadgen_bound`). If it passes 15/15, run the full `gwloss` (both phases, 29 checks). Then 6b2a-stress is complete: the README's *Measured results* (both languages) get b-stress's numbers, and `main` is fast-forwarded.

## Execution Notes

- **Task 1 executed inline (2026-10-15, `ec03f7e`).** As written. Red first: the build failed on `undefined: startPinger`; with the `TestPinger*` tests set aside, `TestAnOnlineClientHoldsOneAttemptGoroutine` failed with `2 goroutines in an attempt, want 1 (serve)` and `(*attempt).ping` in the dump. Then `go test -race -count=3 ./internal/sdk/`, `gofmt`, `go vet ./internal/sdk/`, `go test -race` for `loadgen`, `cmd/loadgen` and `racetest`, `make test` and `make test-scripts` green. `serve` keeps its `ctx` parameter (vet does not report it). Both mutations fail as stated: without the two `stopped` checks, `during a firing` reports `timeouts [40s]`; without `p.unanswered = now`, `TestPingerTimesOutOncePongTimeoutAfterAPing` fails (no timeout; a Ping every 30s).
- **Dry run (Task 2 Step 1, 2026-10-15; dev machine, 2 cores):** b_stress alone, 1,500 SDK clients at 500/s, cap 5,000, `GWLOSS_SS_EVERY=2`, as the stress plan's dry-run copy plus the b_stress CPU sets folded onto the dev machine (`GWLOSS_STRESS_CPUS_LOADGEN=0-1 GWLOSS_STRESS_CPUS_GATEWAY=1 GWLOSS_STRESS_CPUS_LOGIC=0`). Before (`fe146e1`, a temporary worktree) and after (`ec03f7e`): 15 checks each, only `no_swap_in_b_stress` failed (the dev machine; 4 pages in after); all back in 20s / 21s.
  - Goroutines, loadgen's last `/runtime` sample: before 4,384 (t=19; ~2.9 per client), after 2,970 (t=20; ~2.0 per client). As expected.
  - Stack bytes with all 1,500 online (`at_kill/loadgen.heap.txt`, `Stack`): before 22,118,400 (14.7KB per client), after 18,907,136 (12.6KB per client): **2,141 bytes per online client saved**, scaled to 600,000 **~1.28GB, not this plan's ~3.3GB**. The last `/runtime` samples agree (21.50MB → 18.55MB, ~1.97KB per client). The ping goroutine's own stack was near Go's 2KB minimum; the ~5.5KB average per goroutine at 600k comes from `Client.run` and `attempt.run`/`serve`, which remain. A guide, not the Tier 2 result: at 600k the 16GiB diagnostic run's 9.83GB of stacks would fall to ~8.5GB, not ~6.5GB, so gate 1 (mapped ≤ 16GiB after all are back) is in doubt; Steps 2–3 on the Tier 2 host say.
- **Final review (fresh reviewer, Task 1):** no Critical or Important; all five Review Focus items checked. One minor, fixed at the user's request: `serve`'s `ctx` is now unused, so it is named `_`, and the doc comment says a cancel reaches `serve` through `login`'s `context.AfterFunc` close. Ruling: a Ping `Send` blocked on a full socket buffer (no write deadline) holds back the Pong-timeout check, as in the old loop; not a regression, left as is.
- **Smoke at 100k SDK clients (Task 2 Step 2, Tier 2 host, `5366fc0`, dirty 0, 2026-10-15): 9/10.** Failed only `no_swap_in_light` (7 pages in; host noise, not the SDK phase). SDK loadgen RSS **2,065MB at 100k = ~20.65KB per SDK client** (read after the kill, with all 100k back; same reading as `ca7c6e9`'s 2,379MB = 23.8KB): **~3.1KB less per client**, above the dry run's 2.1KB of stack alone (RSS also drops the goroutine's `g` and its timer/channel; GC timing makes RSS the noisier of the two). Scaled to 600k: ~1.9GB, still well short of the ~3.3GB this plan estimated. Ramp 52s at 2,000/s (`failed` 2,019 `pre-handshake`, retried). Kill of gw-3 (25,000 victims): all back, none reassigned; from the mark p50 10.2s, p99 19.96s, max 21.6s (SLO 30s); detection p99 230ms. Login p99 26ms (125,000 logins). Gateways 96–99MB RSS. Light phase as before (100k, login p99 24ms, Pong RTT p99 4ms, none lost). The `Killed` line from `stack.sh` is gw-3's expected SIGKILL (`processes` passed). **Next:** Step 3, b_stress at 16GiB, before the UMA change.
- **b_stress at 16GiB (Task 2 Step 3, Tier 2 host, `8381a9e`, dirty 0, `GWLOSS_PHASES=b_stress GWLOSS_SS_EVERY=10`, 2026-10-15): 14/15, this plan's gate passed.** Run before the UMA change (`host.mem_gb` 26). Failed only `b_stress_storm_real` (`attempt_lag` p99 1,435ms, `connect_lag` p50 344ms / p99 2,112ms, `send_lag` p99 2,661ms / max 9,571ms, `reply_lag` p99 0; bound 100ms). `no_swap_in_b_stress` and `b_stress_loadgen_headroom` passed. Against the 16GiB diagnostic run (`517cfba`) and the 19GiB run (`96b5733`):
  - **Goroutines and stacks at the end:** 1,200,035 goroutines (1.80M; expected ~1.2M), stacks 7.38GB (9.83GB): **2.45GB saved, ~4.1KB per client**, twice the dry run's 2.1KB; the plan's ~6.5GB was not reached (6.1KB per remaining goroutine). The dry-run figure understated the saving at scale; the plan's 5.5KB-per-goroutine figure overstated it.
  - **Mapped memory against 16GiB (17.18GB):** under the limit in every sample; 13.55GB at t=5, 15.85GB at t=28, 16.51GB at t=178 (the last sample; all back at t=188), rising ~5MB/s in the tail as clients come back: **0.67GB of headroom at the end.** RSS peak 15.5GB (17.6GB at 16GiB).
  - **GC:** 21 → 27 cycles, none after t=67; `gc_limiter_last_cycle` 0 in every sample (the limiter engaged in every cycle at 16GiB, never at 19GiB); the heap goal (5.4GB) stayed above the live heap (3.1GB). GC 74.5 CPU-s over the storm against 612.8 user.
  - **Back:** all 600,000 in **188s** (650s at 16GiB, 208s at 19GiB); from the mark p50 39.0s, p90 66.2s, p99 97.4s, max 187.7s; detection p99 5.06s. Scheduling waits over 100ms 50,943, all by t=54. 45 inline handshakes, 4,009,278 reused. Survivors' `handshake_timeout` 572 and 603 (~1.9M in run 27); `no_slot` ~1.96M and ~1.98M each (the survivors' handshake limiter is the bound, as at 19GiB); 3.1 cores each in the first 30s.
  - **Loadgen CPU:** 12.0 of 14 cores in the storm's first 30s, 3.7 over the 188s storm (`loadgen_bound` true).
  - **Swap:** 0 pages in, 0 out.
  - **Gate (review O4):** 1. mapped ≤ 17.18GB in every sample: **pass** (max 16.51GB; the samples end at t=178, before the last victims were back at t=188). 2. the GC limiter engaged in no cycle after t≈60: **pass** (never). 3. swap: **pass** (0).
  - Kicks: 539,540 first, 256 retries (all first timeouts then refused), 38,856 dropped, 21,604 swept first; sweep 121s. Logins p99 6.4s (1.2M logins).
  **Outcome:** loadgen fits under 16GiB without the UMA change, with 0.67GB of headroom at the end. Left: `b_stress_storm_real`'s 100ms p99 bound fails in the storm's first ~54s, where loadgen runs 12 of 14 cores with GC calm (as at 19GiB). That is the *After this plan* decision for the user: a second machine for loadgen, or recording b_stress's storm as loadgen-bound (#102's `loadgen_bound`).
- **After this plan: #110 (the user, 2026-10-15, option (a)).** `b_stress_storm_real` bounds the lags only when the storm was not `loadgen_bound`; its detail records `lags_within` and `loadgen_bound`; b1 unchanged. Checked by a throwaway harness over the check's three lines with the real `lg_true` (6 cases: b1 over the bound fails with or without `loadgen_bound`; b_stress over the bound passes only when `loadgen_bound`, and never with fewer samples than relogins; within the bound passes), which fails when the `b_stress` condition is dropped; `make test-scripts` green. Dry run (2 × 1,000 / 1,500, both phases): 27/29, only the two swap checks (the dev machine); both `storm_real` details show `lags_within` true, `loadgen_bound` false. **Next:** the full `gwloss` on the Tier 2 host (29 checks), then the README's *Measured results* and `main` fast-forwarded.
- **Interface review (2026-10-15, Claude; decided by the user, "apply all", I-4 option (a)):** P6b2a-pingerI-1..4. **Checked, unchanged:** `startPinger` holds `mu` across `AfterFunc`, so an early firing waits for `p.t`; `stop` sets `stopped` under `mu` and `fire` checks it under `mu` before `Reset`, so nothing re-arms after `stop`; the timeout and failed-`Send` paths return without re-arming; reqIDs start at 2, one per heartbeat, as in the old loop; `HeartbeatRange.Of` clamps the heartbeat to 30–300s, so no zero-interval spin; the timeout callback sets `pongTimedOut` before `Close`, so `serve` still ends `EndPongTimeout`. Loadgen uses no pinger internals; light mode's shared pinger is separate. Guides and README never named the ping goroutine; `05a-sdk.md` and the b1 plan's 23.8KB stay as history.
  - **P6b2a-pingerI-1 (fixed):** Task 2 Step 3's gate 1 asked for `/runtime` samples after all victims were back, but the `GWLOSS_SS_EVERY` sampler stops when `lg_wait_back` returns (last sample t=178, all back t=188). `gwloss.sh` now takes one `GET /runtime` once the victims are back (or the wait ran out), with or without the sampler, and each phase's field records it as `loadgen_at_back` (recorded, not a check). `make test-scripts` green. Dry run (2 × 1,000 / 1,500, both phases): 27/29, only the two swap checks (the dev machine); b_stress's `loadgen_at_back`: 3,021 goroutines, mapped 62.7MB against a 16GiB `mem_limit_bytes`, `gc_limiter_last_cycle` 0.
  - **P6b2a-pingerI-2 (decided, no code):** the 16GiB fit stays recorded, not gated (#93: memory is recorded, the swap check is the verdict). The full `gwloss` run on the Tier 2 host runs with `GWLOSS_SS_EVERY=10`, and its record reads gates 1–2 again (mapped ≤ 17.18GB in every sample and in `loadgen_at_back`; the GC limiter engaged in no cycle after t≈60).
  - **P6b2a-pingerI-3 (fixed):** `gwloss.sh`'s memory-budget comment now gives this plan's b_stress figures (loadgen 15.5GB RSS peak, 7.38GB of stacks, mapped 16.51GB, no swap at 23GiB); `mem=14` and `mem=23` unchanged until the UMA change. Roadmap #93's note says it supersedes the older loadgen and host-peak estimates.
  - **P6b2a-pingerI-4 (decided, option (a)):** b_stress fits without the host's UMA change, so it is not needed for 6b2a. It is made after the full `gwloss` run (so that run compares with `8381a9e`); the next run after it updates the roadmap's host line and the memory budgets from its `host.mem_gb`.
  - **For 6b2b (not a fix):** b2's DROP rules are the first scale use of the pinger's timeout path (~200k `EndPongTimeout` ends on timer goroutines, each calling `Close`); every run so far ended clients by RST/EOF (`EndLost`). 6b2b's plan names it in *Review Focus*, and b2's check expects the victims' ends to be `pong-timeout` (loadgen's `lost` counts ends by reason).
  **Next:** the full `gwloss` on the Tier 2 host with `GWLOSS_SS_EVERY=10` (29 checks), then the README's *Measured results* and `main` fast-forwarded; then the UMA change.
