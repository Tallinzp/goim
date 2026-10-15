# Plan 6b2a-reuse: a Refused Handshake Sent Again (b-stress) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make b_stress's storm valid on one host: stop loadgen paying two X25519 for every attempt the survivors refuse, measure the one loadgen lag #107 left out (dial to handshake frame), and stop the harness from failing a 600k gateway's SIGKILL that takes over 5s to finish. Tier 2 run 25 (`1f08da3`, 2026-10-14) passed 25/29: b1 passed in full (`b1_storm_real` 400,000 prepared, `attempt_lag` p99 3ms), memory held (no swap), but `b_stress_storm_real` failed (`attempt_lag` p99 1,185ms; loadgen 12.8 of 14 CPUs in the storm's first 30s) and three b_stress checks failed on the victim's slow exit.

**Architecture:** Three parts.
1. **Send an unanswered handshake again (#109).** Run 25's storm made 6.02M attempts for 600k victims; 5.2M of them made their first message inline. The survivors refuse a handshake over their limit when its frame arrives, before any X25519 (`internal/gateway/tcp/conn.go:125`); `pending_mem` refuses at accept, and `handshake_timeout` closes without a reply. A refused attempt costs the server nothing and loadgen two X25519 (the profile: X25519 47% of loadgen's 13.7 cores; syscalls 19%, GC ~15%). A phone pays that on its own CPU. Loadgen now keeps a client's last first message while no reply byte has come back for it and sends it again on the next attempt. An Initiator still serves one handshake: it is sent again only once the connection that sent it last is closed, so no two attempts can finish it. The server sees the same bytes at the same moments; nothing in the gateway or `wire` tracks first messages.
2. **`send_lag` (#109).** #107's lags skip the time from the dial returning to the handshake frame's write (review I1 took the TCP connect out of `attempt_lag`, and the write after it went too). It holds the inline X25519 and the scheduling wait of a saturated loadgen, and a late frame is a survivor's `handshake_timeout` (1.8M per survivor in run 25). `<phase>_storm_real` now bounds it too.
3. **`stack_expect_dead NAME [SECONDS]`.** A SIGKILLed process exits only once the kernel has closed its sockets; with 600k the victim took ~5.3s (detection max 5,257ms), past the 5s wait, so the victim was not reaped and `b_stress_back`, `b_stress_idle` and `b_stress_processes` failed. `gwloss` waits up to 60s; other callers keep 5s.

**Tech Stack:** Go 1.27.1, `github.com/flynn/noise` v1.1.0, bash 5.2, python3 (reads JSON only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Client SDK rules* (line 127: a new handshake per attempt is the SDK's; loadgen's hook only spares its CPU), *Admission control* (the handshake limit refuses before computing), *Verification → Load tests* (line 540: "(b-stress) kill a 600k gateway: measured, no SLO"). Roadmap choices #102, #106–#108, and the user's decision of 2026-10-14 (a real storm, never a throttle; a second machine if one host cannot make it).

**Branch:** `feat/plan6b2a-stress` (head after this plan's commit; run 25 recorded in plan 6b2a-storm's Execution Notes).

**Size:** 3 tasks, ~30KB. Tasks 1–2 were prototyped in a throwaway worktree from `1f08da3` by applying this plan's code: `go vet` clean; `go test -race` passes for `loadgen`, `cmd/loadgen` and `sdk` (the new tests 3 times); `make test-scripts` passes (all three suites). Both guards in `handshake` were checked by mutation: dropping the closed-sender check in `reply` fails `its closed sender reads no reply`, and dropping the closed-owner check in `reusable` fails `not while its sender is open`. The prototype found the design's first version wrong (it cleared the held handshake on a reply, after which a second connection could no longer recognize the frame; the test hung): the rule "sent again only once its sender is closed" replaced it. Task 3's dry run is part of execution.

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module.
- `make test` (`go test -race -count=1 ./...`), `make test-race-suite`, `make test-scripts` and `make test-load-probe` stay green on the dev machine (2 cores / 2GB), loopback only.
- Production does not change: the SDK, gateway, Logic and Lua are untouched. #109 lives in `internal/loadgen` (sdk mode) behind the SDK's existing `Config.Initiate` hook.
- Scripts: bash with `set -uo pipefail`; JSON without jq; python3 only reads JSON. The host's awk is mawk.
- SDK timing is the spec's and is not changed (storm wait 0–20s, pre-handshake retry 0.5–1.5s, other retryable rejects 1–3s, backoff 2.5–5s doubling to 5 min). #109 changes no attempt's time, only which bytes it sends.
- CPU sets and memory as plan 6b2a-storm (#108; I3: no host change, swap stays fatal).
- New behaviour cites its choice number (#109).

## Review Focus

1. **An Initiator is never finished twice.** The SDK cancels an attempt by closing its connection from a `context.AfterFunc`, so the cancelled attempt can still be running when the next one calls `Initiate`. A held handshake is sent again only once its last sender's connection is closed (`replyConn.Close` marks it), and a closed connection's reply byte is refused (`errHandshakeTaken`), both under the handshake's lock. Test: `TestAHeldHandshakeIsFinishedOnce` (three cases).
2. **An answered handshake is never sent again.** The first reply byte on the sender's connection marks it answered; the client then drops it (`held` → nil), so it keeps no Initiator while online. Test: `TestUnansweredHandshakeIsSentAgain` (attempt 3) and `answered before the resend`.
3. **A refused attempt is any end without a reply byte.** The gateway's `handshake_rate` (frame read, then closed), `pending_mem` (closed at accept), `handshake_timeout` (closed after 5s) and a client-side connect timeout all leave the handshake unanswered; the SDK's `login` closes the connection on every such end (`nc.Close()`, `internal/sdk/attempt.go`), which makes it reusable. Test: `TestUnansweredHandshakeIsSentAgain` attempt 1 → 2 (a gateway that reads the frame and closes, then one that answers the same bytes with `wire.Respond`).
4. **`send_lag` is loadgen's only.** It runs from `Dial` returning (the TCP connect done) to the handshake frame's write: the SDK's `clientconn.New`, `Initiate` (inline X25519 or a take) and scheduling. No server time is in it. Test: `TestSDKStormUsesPrecomputedHandshakes` (`send_lag.n` ≥ 15 after the mark) and `TestRecorderCountsTheStormSinceMark`.
5. **Other `stack_expect_dead` callers keep 5s.** `load-probe.sh` and `stack_test.sh` kill small gateways; only `gwloss` passes 60. Test: `stack_test.sh` `dead: SECONDS bounds the wait` (1s on a running gateway returns within 3s).

## Implementation choices this plan makes

Proposed 2026-10-14 with this plan, for the user's review. The user asked for #109 as a small plan after run 25's profiles.

109. **Loadgen sends an unanswered handshake again** (sdk mode, always on; no flag). An SDK client keeps the `prepared` first message its last attempt sent (`sdkClient.held`). The next `Initiate` returns it again if no reply byte came back for it and the connection that sent it last is closed; otherwise it takes the precomputed one (#106) or makes one inline, and holds that. `replyConn` tells the handshake which connection sent it (its first write, matched by the frame's backing array) and which read a reply byte; a reply byte marks it answered and the client drops it. The recorder counts `handshakes_reused` beside `handshakes_precomputed` and `handshakes_inline` (`Recorder.Handshake(how string)` with `HandshakePrecomputed`, `HandshakeInline`, `HandshakeReused`), and a third lag, `send_lag` (dial returned → handshake frame written), since the latest mark. `<phase>_storm_real` requires `send_lag.n` ≥ the population and its p99 ≤ `GWLOSS_LAG_P99_MS`, like the other two. Expected in b_stress: inline handshakes fall from 5.2M to about one per answered handshake (~655k in run 25), and `handshakes_reused` takes the rest. Not a throttle: no attempt is delayed or dropped; the survivors receive the same number of frames at the same times and do the same work. What differs on the wire: a client's refused connections carry identical first-message bytes, which a phone's would not; the gateway refuses or answers each frame without looking at earlier ones. Rejected: a cap on attempts (a throttle, the user's decision); a pool of handshakes refilled during the storm (spends the storm's CPU); reusing an answered handshake (an Initiator serves one handshake: `Finish` would fail, and the session keys would repeat). **Also:** `stack_expect_dead NAME [SECONDS]` (default 5; `gwloss` 60), since a 600k SIGKILL takes ~5.3s (run 25).

If `b_stress_storm_real` still fails after #109, what remains in loadgen is syscalls and GC, real per-connection work: the next step is a second machine for loadgen (the user's decision, 2026-10-14).

## File Structure

| File | Change |
|---|---|
| `scripts/stack.sh` | `stack_expect_dead NAME [SECONDS]` |
| `scripts/stack_test.sh` | `dead: SECONDS bounds the wait` |
| `internal/loadgen/recorder.go` | `SendLag`, `Handshake(how string)` and its constants, `StormCount.SendLag`, `StormCount.HandshakesReused` |
| `internal/loadgen/sdkclient.go` | `held`, `track`, `initiate`, `handshake`, `replyConn` (`send_lag`, `Close`, the reply guard) |
| `internal/loadgen/recorder_test.go`, `sdkclient_test.go`, `e2e_test.go` | tests |
| `scripts/tier2/gwloss.sh` | `stack_expect_dead "$victim" 60`; `send_lag` in `<phase>_storm_real`; header |
| `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md` | #109 |

---

### Task 1: `stack_expect_dead NAME [SECONDS]`

**Files:**
- Modify: `scripts/stack.sh` (header list ~line 13; `stack_expect_dead`, ~line 458)
- Modify: `scripts/tier2/gwloss.sh` (~line 349)
- Test: `scripts/stack_test.sh` (~line 299)

**Interfaces:**
- Produces: `stack_expect_dead NAME [SECONDS]` (default 5): waits up to SECONDS for NAME to exit, then as before.

- [ ] **Step 1: Write the failing test**

In `scripts/stack_test.sh`, replace

```bash
  stack_expect_dead gw-1 2>/dev/null && bad "dead: a running gateway is refused" || ok "dead: a running gateway is refused"
```

with

```bash
  t=$SECONDS
  stack_expect_dead gw-1 1 2>/dev/null && bad "dead: a running gateway is refused" || ok "dead: a running gateway is refused"
  [ $((SECONDS - t)) -le 3 ] && ok "dead: SECONDS bounds the wait" || bad "dead: waited $((SECONDS - t))s for 1"
```

- [ ] **Step 2: Run it to verify it fails**

Run: `make test-scripts`
Expected: `stack_test` reports `bad dead: waited 5s for 1` (the 1 is ignored; it waits 5s).

- [ ] **Step 3: Implement**

In `scripts/stack.sh`, the header line becomes

```bash
#   stack_expect_dead NAME [SECONDS]  # NAME was killed on purpose (loadgen /kill): reap and record it
```

and the function's comment and loop head become

```bash
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
```

(the rest of the function is unchanged). In `scripts/tier2/gwloss.sh`, replace

```bash
  stack_expect_dead "$victim" # in this shell: it reaps and forgets the victim
```

with

```bash
  # In this shell: it reaps and forgets the victim. 60s: a 600k gateway
  # exits only once the kernel has closed its sockets (~5.3s in run 25).
  stack_expect_dead "$victim" 60
```

- [ ] **Step 4: Run the tests**

Run: `make test-scripts && bash -n scripts/tier2/gwloss.sh`
Expected: all three suites `all passed`, including `ok   dead: SECONDS bounds the wait`.

- [ ] **Step 5: Commit**

```bash
git add scripts/stack.sh scripts/stack_test.sh scripts/tier2/gwloss.sh
git commit -m "fix(scripts): stack_expect_dead takes a wait; gwloss gives a 600k victim 60s to exit (run 25)"
```

---

### Task 2: loadgen sends an unanswered handshake again, and `send_lag` (#109)

**Files:**
- Modify: `internal/loadgen/recorder.go`
- Modify: `internal/loadgen/sdkclient.go` (whole file below)
- Test: `internal/loadgen/recorder_test.go`, `internal/loadgen/sdkclient_test.go`, `internal/loadgen/e2e_test.go`

**Interfaces:**
- Consumes: `sdk.Config.Initiate` (plan 6b2a-storm), `precomputer.take`, `prepared{in, frame}`, `wire.Initiate`, `wire.ClientHandshakeWith`, `wire.Respond`.
- Produces: `(*Recorder).SendLag(time.Duration)`; `(*Recorder).Handshake(how string)` with `HandshakePrecomputed = "precomputed"`, `HandshakeInline = "inline"`, `HandshakeReused = "reused"` (replaces `Handshake(bool)`); `StormCount.SendLag Dist` (`send_lag`) and `StormCount.HandshakesReused int64` (`handshakes_reused`) in `/stats`' `storm_since_mark`; unexported `sdkClient.track`, `sdkClient.initiate`, `handshake`, `errHandshakeTaken`.

- [ ] **Step 1: Write the failing tests**

Replace `TestRecorderCountsTheStormSinceMark` at the end of `internal/loadgen/recorder_test.go` with:

```go
// #107, #109: the storm's lags and handshake counts restart at each mark; a
// negative lag (a wall-clock step) counts as 0.
func TestRecorderCountsTheStormSinceMark(t *testing.T) {
	r := NewRecorder(2, []string{"gw-0"})
	r.AttemptLag(5 * time.Second) // the ramp's: before any mark
	r.SendLag(5 * time.Second)
	r.ReplyLag(5 * time.Second)
	r.Handshake(HandshakeInline)
	if _, err := r.Mark("death", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.AttemptLag(120 * time.Millisecond)
	r.AttemptLag(-time.Millisecond)
	r.SendLag(7 * time.Millisecond)
	r.ReplyLag(3 * time.Millisecond)
	r.Handshake(HandshakePrecomputed)
	r.Handshake(HandshakeInline)
	r.Handshake(HandshakeInline)
	r.Handshake(HandshakeReused)
	s := r.Counts().StormSinceMark
	if s.Mark != "death" || s.AttemptLag.N != 2 || s.AttemptLag.Max != 120 || s.SendLag.N != 1 || s.SendLag.Max != 7 ||
		s.ReplyLag.N != 1 || s.ReplyLag.Max != 3 || s.HandshakesPrecomputed != 1 || s.HandshakesInline != 2 || s.HandshakesReused != 1 {
		t.Fatalf("storm since mark %+v", s)
	}
}
```

In `internal/loadgen/sdkclient_test.go`, the imports become

```go
import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
	"github.com/Tallinzp/goim/internal/wire"
)
```

and append:

```go
// handshakeClient is an sdkClient with a server key, for #109's tests.
func handshakeClient(t *testing.T) (*sdkClient, wire.Key) {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{env: Env{NoiseKeyID: 1, NoisePublic: key.Public}, rec: NewRecorder(1, []string{"gw-0"})}
	return &sdkClient{r: r}, key
}

// readFrame reads a client's handshake frame on the server's side of a pipe.
func readFrame(s net.Conn) <-chan []byte {
	got := make(chan []byte, 1)
	go func() {
		f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
		if _, err := io.ReadFull(s, f); err != nil {
			f = nil
		}
		got <- f
	}()
	return got
}

// #109: a handshake no reply answered is sent again by the next attempt,
// and completes there; once answered, the next attempt makes a new one.
func TestUnansweredHandshakeIsSentAgain(t *testing.T) {
	sc, key := handshakeClient(t)
	cmd := uint16(clientpb.Cmd_CMD_HANDSHAKE)

	// Attempt 1: the gateway reads the frame and closes (its handshake limit).
	c1, s1 := net.Pipe()
	in1, f1, err := sc.initiate(1, key.Public)
	if err != nil {
		t.Fatal(err)
	}
	got := readFrame(s1)
	go func() { <-got; s1.Close() }()
	rc1 := sc.track(c1)
	if _, err := wire.ClientHandshakeWith(rc1, in1, f1); err == nil {
		t.Fatal("attempt 1: a closed gateway completed the handshake")
	}
	rc1.Close() // as the SDK's login does when a handshake fails

	// Attempt 2: the same frame, answered this time.
	c2, s2 := net.Pipe()
	defer c2.Close()
	defer s2.Close()
	in2, f2, err := sc.initiate(1, key.Public)
	if err != nil {
		t.Fatal(err)
	}
	if in2 != in1 || !bytes.Equal(f2, f1) {
		t.Fatal("attempt 2: a new handshake, want attempt 1's")
	}
	srv := make(chan *wire.Session, 1)
	go func() {
		f := <-readFrame(s2)
		sess, reply, err := wire.Respond(key, f, cmd)
		if err != nil || !bytes.Equal(f, f1) {
			srv <- nil
			return
		}
		s2.Write(reply)
		srv <- sess
	}()
	cs, err := wire.ClientHandshakeWith(sc.track(c2), in2, f2)
	if err != nil {
		t.Fatalf("attempt 2: %v", err)
	}
	ss := <-srv
	if ss == nil {
		t.Fatal("attempt 2: the gateway did not get attempt 1's frame")
	}
	sealed, err := cs.Seal(nil, 9, 1, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, body, _, err := ss.Open(nil, sealed); err != nil || string(body) != "hi" {
		t.Fatalf("open: %q %v", body, err)
	}

	// Attempt 3: answered, so a new one; the client holds no Initiator.
	if sc.held.Load() != nil {
		t.Fatal("an answered handshake is still held")
	}
	_, f3, err := sc.initiate(1, key.Public)
	if err != nil || bytes.Equal(f3, f1) {
		t.Fatalf("attempt 3: %v, same frame %v", err, bytes.Equal(f3, f1))
	}
	s := sc.r.rec.Counts().StormSinceMark
	if s.HandshakesInline != 2 || s.HandshakesReused != 1 || s.SendLag.N != 2 {
		t.Fatalf("storm %+v: want 2 inline, 1 reused, 2 send lags", s)
	}
}

// byteConn is a connection whose every Read returns one byte: a reply that
// is already there.
type byteConn struct{ net.Conn }

func (byteConn) Read(b []byte) (int, error)  { b[0] = 1; return 1, nil }
func (byteConn) Write(b []byte) (int, error) { return len(b), nil }
func (byteConn) Close() error                { return nil }

// #109: an attempt the SDK cancels can still be running when the next one
// starts. A handshake is sent again only once its sender's connection is
// closed, which then reads no reply; one answered is never sent again.
func TestAHeldHandshakeIsFinishedOnce(t *testing.T) {
	t.Run("not while its sender is open", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		if _, err := sc.track(byteConn{}).Write(f); err != nil {
			t.Fatal(err)
		}
		if _, f2, _ := sc.initiate(1, key.Public); bytes.Equal(f2, f) {
			t.Fatal("sent again while its sender may still read the reply")
		}
	})
	t.Run("its closed sender reads no reply", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		rc1 := sc.track(byteConn{})
		if _, err := rc1.Write(f); err != nil {
			t.Fatal(err)
		}
		rc1.Close()
		_, f2, _ := sc.initiate(1, key.Public)
		if !bytes.Equal(f2, f) {
			t.Fatal("not sent again after its sender closed")
		}
		if _, err := rc1.Read(make([]byte, 1)); !errors.Is(err, errHandshakeTaken) {
			t.Fatalf("the closed sender read a reply: %v", err)
		}
		rc2 := sc.track(byteConn{})
		if _, err := rc2.Write(f2); err != nil {
			t.Fatal(err)
		}
		if n, err := rc2.Read(make([]byte, 1)); n != 1 || err != nil {
			t.Fatalf("the new sender: %d %v", n, err)
		}
	})
	t.Run("answered before the resend", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		rc1 := sc.track(byteConn{})
		if _, err := rc1.Write(f); err != nil {
			t.Fatal(err)
		}
		if n, err := rc1.Read(make([]byte, 1)); n != 1 || err != nil {
			t.Fatalf("the sender: %d %v", n, err)
		}
		rc1.Close()
		if _, f2, _ := sc.initiate(1, key.Public); bytes.Equal(f2, f) {
			t.Fatal("an answered handshake was sent again")
		}
	})
}
```

In `internal/loadgen/e2e_test.go` (`TestSDKStormUsesPrecomputedHandshakes`), the storm check becomes

```go
	if s.Mark != "death" || s.HandshakesPrecomputed != 15 || s.AttemptLag.N < 15 || s.AttemptLag.Max > 1000 ||
		s.SendLag.N < 15 || s.SendLag.Max > 1000 || s.ReplyLag.N < 15 || s.ReplyLag.Max > 1000 {
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go vet ./internal/loadgen`
Expected: FAIL to compile: `r.SendLag undefined`, `undefined: HandshakeInline`, `sc.initiate undefined`, `sc.track undefined`.

- [ ] **Step 3: The recorder**

In `internal/loadgen/recorder.go`:

1. Add the field after `reply`:

```go
	send           *hist        // send lag since the latest mark, ms (#109)
```

2. In `NewRecorder`, `lag: newHist(), reply: newHist(), byName` becomes `lag: newHist(), reply: newHist(), send: newHist(), byName`.

3. Replace `Handshake(precomputed bool)` and its comment with:

```go
// SendLag: a client wrote its handshake frame lag after its dial returned:
// making or taking the frame, on loadgen's CPU (#109).
func (r *Recorder) SendLag(lag time.Duration) {
	r.mu.Lock()
	r.send.add(max(lag, 0).Milliseconds())
	r.mu.Unlock()
}

// Where an attempt's first handshake message came from (#106, #109).
const (
	HandshakePrecomputed = "precomputed" // made ahead of time, before the first mark
	HandshakeInline      = "inline"      // made when it dialed
	HandshakeReused      = "reused"      // the last attempt's, which no reply answered
)

// Handshake counts an attempt's first handshake message by where it came
// from: HandshakePrecomputed, HandshakeInline or HandshakeReused.
func (r *Recorder) Handshake(how string) {
	r.mu.Lock()
	switch how {
	case HandshakePrecomputed:
		r.storm.HandshakesPrecomputed++
	case HandshakeReused:
		r.storm.HandshakesReused++
	default:
		r.storm.HandshakesInline++
	}
	r.mu.Unlock()
}
```

4. In `Mark`, `r.lag, r.reply, r.storm = newHist(), newHist(), StormCount{Mark: name}` becomes `r.lag, r.reply, r.send, r.storm = newHist(), newHist(), newHist(), StormCount{Mark: name}`.

5. `StormCount` and its comment become:

```go
// StormCount is the attempts since mark Mark ("": the run's start): how late
// each started after its wait's end (AttemptLag, ms; an attempt with no wait,
// such as the first, is not in it), how long each took from its dial to its
// handshake frame (SendLag, ms), how long each handshake took from the
// server's reply to the Login frame (ReplyLag, ms), and where its first
// message came from (#106, #107, #109).
type StormCount struct {
	Mark                  string `json:"mark"`
	AttemptLag            Dist   `json:"attempt_lag"`
	SendLag               Dist   `json:"send_lag"`
	ReplyLag              Dist   `json:"reply_lag"`
	HandshakesPrecomputed int64  `json:"handshakes_precomputed"`
	HandshakesInline      int64  `json:"handshakes_inline"`
	HandshakesReused      int64  `json:"handshakes_reused"`
}
```

6. In `Counts`, after `c.StormSinceMark.ReplyLag = r.reply.dist(r.reply.n)` add `c.StormSinceMark.SendLag = r.send.dist(r.send.n)`.

- [ ] **Step 4: The SDK client**

Replace `internal/loadgen/sdkclient.go` with:

```go
package loadgen

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/sdk"
	"github.com/Tallinzp/goim/internal/wire"
)

// sdkClient is one sdk-mode client (choice #80): the real SDK with its
// reconnect rules, dialing through the balancer.
type sdkClient struct {
	r      *Runner
	i      int
	gw     atomic.Int32              // gateway of the last dial
	tok    atomic.Pointer[string]    // the last token, reused until a refresh (spec: Client SDK rules)
	pre    atomic.Pointer[prepared]  // the next attempt's first message (#106)
	queued atomic.Bool               // waiting for a precompute worker
	held   atomic.Pointer[handshake] // the last attempt's first message while no reply answered it (#109)
	due    time.Time                 // the next attempt's due time (its wait's end), zero: no wait (#107); run loop only
	start  time.Time                 // the current attempt's start; run loop only
	online bool                      // run loop only
}

func (r *Runner) newSDK(i int) (*sdk.Client, error) {
	sc := &sdkClient{r: r, i: i}
	return sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
		AppID: AppID, Platform: clientpb.Platform_PLATFORM_MOBILE,
		Token: func(_ context.Context, refresh bool) (string, error) {
			if t := sc.tok.Load(); t != nil && !refresh {
				return *t, nil // a phone's cached token
			}
			t, err := r.token(i)
			if err == nil {
				sc.tok.Store(&t)
			}
			return t, err
		},
		Store: &sdk.MemStore{}, OnEvent: sc.event, Windows: r.cfg.windows,
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			nc, gw, err := r.bal.Dial(ctx)
			if err != nil {
				return nil, err
			}
			sc.gw.Store(int32(gw))
			return sc.track(nc), nil
		},
		Initiate: sc.initiate})
}

// track wraps a connection the client just dialed (#107, #109).
func (sc *sdkClient) track(nc net.Conn) *replyConn {
	return &replyConn{Conn: nc, sc: sc, dialed: time.Now()}
}

// initiate is the client's Config.Initiate. It sends the last attempt's
// first message again while no reply byte answered it and its connection is
// closed (#109): the gateway refuses a handshake over its limit before any
// X25519, so it costs the server the same, and loadgen is spared the two
// X25519 a phone makes on its own CPU. Otherwise it takes the precomputed
// one (#106) or makes one. A prepared or held frame is for the env's key,
// which is this Config's KeyID and ServerKey: the SDK pins one key
// (P6b2a-stormI-3).
func (sc *sdkClient) initiate(keyID uint16, pub []byte) (*wire.Initiator, []byte, error) {
	r := sc.r
	if h := sc.held.Load(); h != nil && h.reusable() {
		r.rec.Handshake(HandshakeReused)
		return h.pr.in, h.pr.frame, nil
	}
	var pr *prepared
	how := HandshakePrecomputed
	if r.pre != nil {
		pr = r.pre.take(sc)
	}
	if pr == nil {
		in, frame, err := wire.Initiate(keyID, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		if err != nil {
			return nil, nil, err
		}
		pr, how = &prepared{in: in, frame: frame}, HandshakeInline
	}
	r.rec.Handshake(how)
	sc.held.Store(&handshake{pr: pr})
	return pr.in, pr.frame, nil
}

// event runs on the client's run loop: it must not block.
func (sc *sdkClient) event(e sdk.Event) {
	now := time.Now()
	if e.Kind == sdk.EventWaiting && (e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure) {
		sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
	}
	switch e.Kind {
	case sdk.EventConnecting:
		sc.start = now
		if !sc.due.IsZero() { // how late loadgen starts the attempt (#107)
			sc.r.rec.AttemptLag(now.Sub(sc.due))
			sc.due = time.Time{}
		}
	case sdk.EventOnline:
		sc.online = true
		sc.r.rec.Online(sc.i, int(sc.gw.Load()), sc.start, now)
		if sc.r.pre != nil {
			sc.r.pre.enqueue(sc)
		}
	case sdk.EventWaiting, sdk.EventIdle, sdk.EventLoginRequired:
		if e.Kind == sdk.EventWaiting {
			sc.due = now.Add(e.Wait.D)
		}
		switch {
		case sc.online:
			sc.online = false
			sc.r.rec.Lost(sc.i, e.End.String(), now)
		case e.End != 0:
			sc.r.rec.Failed(e.End.String())
		}
	}
}

// handshake is a client's last first message (#109). An Initiator serves
// one handshake, so it is sent again only while no reply byte has come back
// for it, and only once the connection that sent it last is closed: a
// closed connection reads no reply, so no two attempts can finish it. (An
// attempt the SDK cancels can still be running when the next one starts;
// the cancel closes its connection.)
type handshake struct {
	pr       *prepared
	mu       sync.Mutex
	owner    *replyConn // the connection that sent it last
	answered bool       // a reply byte came back on owner: never sent again
}

func (h *handshake) reusable() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.answered && h.owner != nil && h.owner.closed.Load()
}

// send: c sends h's frame. False if a reply answered h meanwhile: c must not
// use it.
func (h *handshake) send(c *replyConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.answered {
		return false
	}
	h.owner = c
	return true
}

// reply: a reply byte for h came back on c. False if c is closed or another
// connection sent h since: c must not finish it.
func (h *handshake) reply(c *replyConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.owner != c || c.closed.Load() {
		return false
	}
	h.answered = true
	return true
}

// errHandshakeTaken fails a connection whose handshake was answered on
// another connection, or a reply on a connection already closed (#109).
var errHandshakeTaken = errors.New("loadgen: handshake used by a later attempt")

// replyConn measures loadgen's own lags on one connection: from the dial to
// the handshake frame, its first write (SendLag: making or taking the frame,
// #109), and from the read that completed the server's handshake reply to
// the Login frame, its second write (ReplyLag: Finish and the Login's seal,
// #107). It also tells the client's held handshake which connection may
// finish it (#109).
type replyConn struct {
	net.Conn
	sc     *sdkClient
	dialed time.Time
	read   atomic.Int64 // unix ns the latest Read returned
	writes atomic.Int32
	hs     atomic.Pointer[handshake] // the held handshake this connection sent
	closed atomic.Bool
}

func (c *replyConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func (c *replyConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	c.read.Store(time.Now().UnixNano())
	if h := c.hs.Load(); h != nil && n > 0 {
		if !h.reply(c) {
			return 0, errHandshakeTaken
		}
		c.hs.Store(nil)
		c.sc.held.CompareAndSwap(h, nil) // answered: the client keeps no Initiator
	}
	return n, err
}

func (c *replyConn) Write(b []byte) (int, error) {
	switch c.writes.Add(1) {
	case 1:
		c.sc.r.rec.SendLag(time.Since(c.dialed))
		if h := c.sc.held.Load(); h != nil && len(b) > 0 && len(h.pr.frame) > 0 && &b[0] == &h.pr.frame[0] {
			if !h.send(c) {
				return 0, errHandshakeTaken
			}
			c.hs.Store(h)
		}
	case 2:
		if at := c.read.Load(); at != 0 {
			c.sc.r.rec.ReplyLag(time.Duration(time.Now().UnixNano() - at))
		}
	}
	return c.Conn.Write(b)
}
```

- [ ] **Step 5: Run the tests**

Run: `gofmt -l internal/loadgen; go vet ./internal/loadgen && go test -race -count=3 -run 'Unanswered|HeldHandshake|StormSinceMark|AttemptLag|BackoffWaits' ./internal/loadgen && go test -race -count=1 ./internal/loadgen ./cmd/loadgen ./internal/sdk`
Expected: no gofmt output; all PASS (`TestSDKStormUsesPrecomputedHandshakes` ~3s).

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen/recorder.go internal/loadgen/recorder_test.go internal/loadgen/sdkclient.go internal/loadgen/sdkclient_test.go internal/loadgen/e2e_test.go
git commit -m "feat(loadgen): an unanswered handshake is sent again; send_lag (#109)"
```

---

### Task 3: `gwloss` bounds `send_lag`; roadmap; Tier 2 run 26 (#109)

**Files:**
- Modify: `scripts/tier2/gwloss.sh` (header; `<phase>_storm_real`, ~line 412)
- Modify: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`

**Interfaces:**
- Consumes: `/stats`' `storm_since_mark.send_lag` and `.handshakes_reused` (Task 2).
- Produces: `<phase>_storm_real` requires `send_lag` like `attempt_lag` and `reply_lag`; its detail carries `handshakes_reused` (the `storm` object is recorded whole).

- [ ] **Step 1: The check**

In `scripts/tier2/gwloss.sh`, the header's `GWLOSS_LAG_P99_MS` entry becomes

```bash
#   GWLOSS_LAG_P99_MS  the storm's p99 bound on loadgen's lags, attempt_lag,
#                      send_lag and reply_lag, ms (default 100, #107, #109)
```

and the `b1_storm_real` entry's last lines become

```bash
#                      at the kill (loadgen -precompute); after the kill,
#                      loadgen started each attempt within GWLOSS_LAG_P99_MS
#                      of its wait's end, wrote each handshake frame within
#                      GWLOSS_LAG_P99_MS of its dial (#109) and each Login
#                      frame within GWLOSS_LAG_P99_MS of the handshake reply
#                      (all p99, at least one per relogin)
```

Above the `<phase>_storm_real` check, the comment's first sentence becomes

```bash
  # #107: the storm was the spec's: every logged-in client was prepared at
  # the kill, and after it loadgen started each attempt, sent each handshake
  # frame (#109) and answered each handshake reply within GWLOSS_LAG_P99_MS
  # (p99; loadgen's own lags only, review I1/I2). Run 25's attempt lag p99
  # was 1,185ms with 5.2M inline handshakes; the storm's handshake counts
  # (precomputed, inline, reused) are in its detail.
```

(the rest of that comment is unchanged), and in the check's python, `for k in ("attempt_lag", "reply_lag")` becomes `for k in ("attempt_lag", "send_lag", "reply_lag")`.

- [ ] **Step 2: Script tests and dry run**

Run: `make test-scripts`
Expected: PASS.

Dry run as in plan 6b2a-storm Task 3 Step 6 (the `D` copy with the guard lowered; the commands are in plan 6b2a-stress Task 4 Step 2), with b_stress's sets folded onto the dev machine:

```bash
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 T2_CPUS_LOADGEN=0-1 \
  GWLOSS_STRESS_CPUS_GATEWAY=1 GWLOSS_STRESS_CPUS_LOADGEN=0-1 GWLOSS_STRESS_CPUS_LOGIC=0 \
  GWLOSS_CLIENTS=2000 GWLOSS_RATE=500 GWLOSS_CAP=5000 \
  GWLOSS_STRESS_CLIENTS=1500 GWLOSS_STRESS_RATE=500 GWLOSS_STRESS_CAP=5000 bash "$D/gwloss.sh"; echo "exit $?"
```

Expected: 29 checks, all pass except possibly the two swap checks (the dev machine). Both `<phase>_storm_real` details show `send_lag` with `n` ≥ the population and p99 well under 100ms, and `handshakes_reused` (likely 0 at this scale: nothing is refused). Remove the dry run's `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 3: Roadmap and commit**

Add #109 to the roadmap's choices (as in this plan's *Implementation choices*), and set the plan 6 row's *next* to "execute 6b2a-reuse, then Tier 2 run 26".

```bash
git add scripts/tier2/gwloss.sh docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-14-connection-session-core-06b2a-reuse.md
git commit -m "feat(tier2): <phase>_storm_real bounds send_lag; #109 in the roadmap"
git push
```

- [ ] **Step 4: Tier 2 run 26 (the user, on the host)**

Host prep as for run 25, then `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss`. Expected: 29/29. What to read: `b_stress_storm_real` (`attempt_lag`, `send_lag`, `reply_lag` p99; `handshakes_inline` against `handshakes_reused`: inline should be about one per answered handshake), loadgen's `storm_first30` cores against 14, `b_stress_back`'s relogin distribution (b-stress's measurement once `storm_real` passes), the survivors' `handshake_rate`, `pending_mem`, `handshake_timeout` and `auth_timeout` rejects (`handshake_timeout` should fall if run 25's came from late frames), and `b_stress_processes` (the victim reaped). If `b_stress_storm_real` still fails, take the storm's CPU profile (`go tool pprof -top b_stress/artifacts/storm/loadgen.cpu.pb.gz`): what remains is per-connection work, and the next step is a second machine for loadgen.

## After this plan

6b2a-stress is complete when run 26 passes 29/29; then the README's *Measured results* (both languages) get b-stress's numbers, and `main` is fast-forwarded. 6b2b (b2 and slowed Logic) follows in a fresh session.

## Execution Notes
- **User decision (2026-10-14):** execute Task 1 (`stack_expect_dead NAME [SECONDS]`) and `send_lag` now; #109's reuse waits for run 26's `send_lag`, which says whether the survivors' `handshake_timeout` rejects come from loadgen. Done inline as one commit, without the reuse: `Recorder.SendLag`, `StormCount.SendLag` (`send_lag`), `replyConn.dialed` and its first write recording the lag; `Handshake(bool)` stays (no `HandshakesReused`, no `HandshakeInline`/… constants, no `held`/`track`/`initiate`); `<phase>_storm_real` bounds `send_lag` like the other two. `send_lag` is cited as #107 (one of the storm's loadgen lags, run 25), not #109. `TestRecorderCountsTheStormSinceMark` and `TestSDKStormUsesPrecomputedHandshakes` check `send_lag`; `stack_test.sh` has `dead: SECONDS bounds the wait`. `go test -race` for `loadgen` and `cmd/loadgen`, and `make test-scripts` green. Dry run (2 × 1,000 / 1,500): 27/29, only the two swap checks (the dev machine); `send_lag` n 1000 / 1500, p99 0ms in both phases; `b_stress_processes` passes (the victim reaped). Task 2's reuse and Task 3's remaining text (`handshakes_reused`) apply once #109 is decided.
- **`send_lag` interface review (2026-10-14, Claude; decided as proposed by the user, "apply all"):** P-sendlagI-1..4. **Checked, unchanged:** the first write on an SDK connection is the handshake frame (`clientconn.HandshakeWith` only sets a deadline before `wire.ClientHandshakeWith` writes it), the Login frame the second, as `reply_lag` assumes; `dialed` and the first `Write` run on the attempt's own goroutine, on the monotonic clock; the histogram holds seconds (run 25's `attempt_lag` max 2,112ms).
  - **P-sendlagI-1 (fixed):** the roadmap's #107 now records `send_lag` as an amendment, and why it does not bring back what I1 removed: it starts after the TCP connect and ends at the `Write` call, so a full accept queue is not in it.
  - **P-sendlagI-2 (decided): run 26 (without #109) and what decides #109.** Expected: `b_stress_storm_real` still fails on `attempt_lag` (loadgen's CPU is unchanged); `b_stress_back`, `b_stress_idle` and `b_stress_processes` pass (the victim reaped within 60s). Then: if b_stress's `send_lag` p99 is ≥ 1s, the survivors' `handshake_timeout` rejects are late frames from a saturated loadgen, and #109 goes ahead (Task 2's reuse, Task 3's `handshakes_reused`), then run 27. If `send_lag` p99 is small, frames left loadgen on time and were not handled within 5s: look at the survivors' side first (their storm CPU profiles, `handshakes_ok` against `handshake_timeout` over the storm), then decide #109.
  - **P-sendlagI-3 (noted):** `send_lag` includes an inline `Initiate` (two X25519, ~0.1–0.3ms), far below the 100ms bound.
  - **P-sendlagI-4 (noted):** an attempt that never writes (cancelled before it, or `Initiate` failed) has no sample; rare, and every relogin writes at least once, so `n ≥ population` holds.
