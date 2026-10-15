# Plan 6b2a-reuse: a Refused Handshake Sent Again (b-stress) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Status (2026-10-15):** Task 1 and `send_lag` are done (`8e85cfd`, see *Execution Notes*). Tier 2 run 26 (`d4c5b3c`) gave `send_lag` p99 2,538ms, but its survivors timed out 3.9M handshakes while idle, which late frames do not explain (dispositions, *Follow-up*). The b_stress diagnostic run with `GWLOSS_SS_EVERY` (`8ea8930`, 2026-10-15) put the timeouts on loadgen's side, in a part no lag measured: from connect to the dial's return. The b_stress run with `connect_lag` (`0fe01ae`) measured that gap: p50 8.9s against the survivors' 5s, from loadgen's CPU saturated in the storm (13.6 of 14 cores, X25519 43%, GC ~20–25%). The user decided #109 (2026-10-15): Tasks 2–3 executed inline (`22bea9e` and the docs commit after it; see *Execution Notes*). **Next: Tier 2 run 27** (b_stress, `GWLOSS_SS_EVERY=10`). Tasks 2–3 below are the delta after `8e85cfd`, with review O2–O5 applied (`docs/superpowers/objections/connection-session-core-06b2a-reuse-dispositions.md`).

**Goal:** Make b_stress's storm valid on one host: stop loadgen paying two X25519 for every attempt the survivors refuse, measure the one loadgen lag #107 left out (dial to handshake frame), and stop the harness from failing a 600k gateway's SIGKILL that takes over 5s to finish. Tier 2 run 25 (`1f08da3`, 2026-10-14) passed 25/29: b1 passed in full (`b1_storm_real` 400,000 prepared, `attempt_lag` p99 3ms), memory held (no swap), but `b_stress_storm_real` failed (`attempt_lag` p99 1,185ms; loadgen 12.8 of 14 CPUs in the storm's first 30s) and three b_stress checks failed on the victim's slow exit.

**Architecture:** Three parts.
1. **Send an unanswered handshake again (#109; Tasks 2–3, waiting for run 26).** Run 25's storm made 6.02M attempts for 600k victims; 5.2M of them made their first message inline. The survivors refuse a handshake over their limit when its frame arrives, before any X25519 (`internal/gateway/tcp/conn.go:125`); `pending_mem` refuses at accept, and `handshake_timeout` closes without a reply. A refused attempt costs the server nothing and loadgen two X25519 (NK's first message is `e, es`; the profile: X25519 47% of loadgen's 13.7 cores; syscalls 19%, GC ~15%). A phone pays that on its own CPU. Loadgen now keeps a client's last first message while no reply byte has come back for it and sends it again on the next attempt. An Initiator still serves one handshake: it is sent again only once the connection that sent it last is closed, and a closed connection can neither take it nor finish it, so no two attempts can finish it. The server sees the same bytes at the same moments; nothing in the gateway or `wire` tracks first messages.
2. **`send_lag` (done, `8e85cfd`, cited as #107 per P-sendlagI-1).** #107's lags skipped the time from the dial returning to the handshake frame's write. It holds the inline X25519 and the scheduling wait of a saturated loadgen, and a late frame is a survivor's `handshake_timeout` (1.8M per survivor in run 25). `<phase>_storm_real` bounds it.
3. **`stack_expect_dead NAME [SECONDS]` (done, `8e85cfd`).** A SIGKILLed process exits only once the kernel has closed its sockets; with 600k the victim took ~5.3s, past the 5s wait. `gwloss` waits up to 60s; other callers keep 5s.

**Tech Stack:** Go 1.27.1, `github.com/flynn/noise` v1.1.0, bash 5.2, python3 (reads JSON only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Client SDK rules* (line 127: a new handshake per attempt is the SDK's; loadgen's hook only spares its CPU), *Admission control* (the handshake limit refuses before computing), *Verification → Load tests* (line 540: "(b-stress) kill a 600k gateway: measured, no SLO"). Roadmap choices #102, #106–#108, and the user's decision of 2026-10-14 (a real storm, never a throttle; a second machine if one host cannot make it).

**Branch:** `feat/plan6b2a-stress`.

**Size:** Tasks 2–3 remain, ~25KB. Task 2's first version was prototyped in a throwaway worktree from `1f08da3`: `go vet` clean; `go test -race` passes for `loadgen`, `cmd/loadgen` and `sdk`; both guards in `handshake` were checked by mutation (dropping the closed-sender check in `reply` fails `its closed sender reads no reply`; dropping the closed-owner check in `reusable` fails `not while its sender is open`). The prototype found the design's first version wrong (it cleared the held handshake on a reply, after which a second connection could no longer recognize the frame; the test hung). After the review (2026-10-14), Task 2 as written here was replayed in a throwaway worktree from `d4c5b3c`: the red step fails to compile as stated; then `go vet` clean, the new tests pass 3 times under `-race`, `loadgen`, `cmd/loadgen` and `sdk` pass; both Step 5 mutations fail as stated (`a closed sender's late write`: the late write returns nil; `TestSDKResendsARefusedHandshake`: 6 inline, 0 reused).

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module.
- `make test` (`go test -race -count=1 ./...`), `make test-race-suite`, `make test-scripts` and `make test-load-probe` stay green on the dev machine (2 cores / 2GB), loopback only.
- Production does not change: the SDK, gateway, Logic and Lua are untouched. #109 lives in `internal/loadgen` (sdk mode) behind the SDK's existing `Config.Initiate` hook.
- Scripts: bash with `set -uo pipefail`; JSON without jq; python3 only reads JSON. The host's awk is mawk.
- SDK timing is the spec's and is not changed (storm wait 0–20s, pre-handshake retry 0.5–1.5s, other retryable rejects 1–3s, backoff 2.5–5s doubling to 5 min). #109 changes no attempt's time, only which bytes it sends.
- CPU sets and memory as plan 6b2a-storm (#108; I3: no host change, swap stays fatal).
- New behaviour cites its choice number (#109); `send_lag` stays #107 (P-sendlagI-1).

## Review Focus

1. **An Initiator is never finished twice.** The SDK cancels an attempt by closing its connection from a `context.AfterFunc`, so the cancelled attempt can still be running when the next one calls `Initiate`, and can even write after the next one wrote. Only the client's newest connection (dial number) may send the held handshake or finish it, under the handshake's lock; the SDK dials before it initiates, so an older connection is always a cancelled attempt's (amended after the final review, M1–M3; it replaced the closed-connection rule of review O3). Test: `TestAHeldHandshakeIsFinishedOnce` (five cases).
2. **An answered handshake is never sent again.** The first reply byte on the sender's connection marks it answered; the client then drops it (`held` → nil), so it keeps no Initiator while online. Test: `TestUnansweredHandshakeIsSentAgain` (attempt 3) and `answered before the resend`.
3. **A refused attempt is any end without a reply byte.** The gateway's `handshake_rate` (frame read, then closed), `pending_mem` (closed at accept), `handshake_timeout` (closed after 5s) and a client-side connect timeout all leave the handshake unanswered; the SDK's `login` closes the connection on every such end (`nc.Close()`, `internal/sdk/attempt.go`), which makes it reusable. Test: `TestUnansweredHandshakeIsSentAgain` attempt 1 → 2.
4. **The reuse works through the real SDK** (review O4). `replyConn` recognizes the handshake frame by its backing array, which holds because `clientconn.HandshakeWith` hands the `replyConn` to `wire.ClientHandshakeWith`, which writes `frame` unchanged in one `Write`. Test: `TestSDKResendsARefusedHandshake` (sdk mode, a forwarder that refuses each client's first frame: every client online, 3 inline and 3 reused, every frame that logged in is one the forwarder refused).

## Implementation choices this plan makes

Proposed 2026-10-14 with this plan; reviewed 2026-10-14 (O1–O5 accepted). **Executed only after Tier 2 run 26** (P-sendlagI-2, O1).

109. **Loadgen sends an unanswered handshake again** (sdk mode, always on; no flag). An SDK client keeps the `prepared` first message its last attempt sent (`sdkClient.held`). The next `Initiate` returns it again if no reply byte came back for it; otherwise it takes the precomputed one (#106) or makes one inline, and holds that. `replyConn` tells the handshake which connection sent it (its first write, matched by the frame's backing array) and which read a reply byte; a reply byte marks it answered and the client drops it. **Amended 2026-10-15 (final review M1–M3, the user):** only the client's newest connection (its dial number, `track`) may send or finish the held handshake. The SDK runs one attempt at a time and dials before it initiates, so an older connection is a cancelled attempt's, whose close has not landed yet; its first write is refused (held or answered frame alike) and so is its reply. The handshake keeps its sender's dial number, not the connection, and is sent again while unanswered even if its sender is still open, so no other unanswered first message exists. This replaces the closed-connection rule (review O3), which let a cancelled attempt's late write take the handshake from the live one (M2), let a third overlapping attempt replace an unanswered handshake whose stale frame then passed unguarded (M1), and kept every waiting client's closed connection alive (M3). **Assumption** (review O4, enforced by `TestSDKResendsARefusedHandshake`): the SDK path writes `frame` unchanged in one `Write` (`clientconn.HandshakeWith` → `wire.ClientHandshakeWith`); if that changes, the match misses and reuse silently stops. The recorder counts `handshakes_reused` beside `handshakes_precomputed` and `handshakes_inline` (`Recorder.Handshake(how string)` with `HandshakePrecomputed`, `HandshakeInline`, `HandshakeReused`). Expected in b_stress: inline handshakes fall from 5.2M to about one per answered handshake (~655k in run 25), and `handshakes_reused` takes the rest. Not a throttle: no attempt is delayed or dropped; the survivors receive the same number of frames at the same times and do the same work. What differs on the wire: a client's refused connections carry identical first-message bytes, which a phone's would not; the gateway refuses or answers each frame without looking at earlier ones. Rejected: a cap on attempts (a throttle, the user's decision); a pool of handshakes refilled during the storm (spends the storm's CPU); reusing an answered handshake (an Initiator serves one handshake: `Finish` uses its handshake state up, review O5). **Also (done, `8e85cfd`):** `stack_expect_dead NAME [SECONDS]` (default 5; `gwloss` 60), since a 600k SIGKILL takes ~5.3s (run 25).

If `b_stress_storm_real` still fails after #109, what remains in loadgen is syscalls and GC, real per-connection work: the next step is a second machine for loadgen (the user's decision, 2026-10-14).

## File Structure

| File | Change |
|---|---|
| `scripts/stack.sh`, `scripts/stack_test.sh` | done (`8e85cfd`): `stack_expect_dead NAME [SECONDS]` |
| `internal/loadgen/recorder.go` | `Handshake(how string)` and its constants, `StormCount.HandshakesReused` (`SendLag` done) |
| `internal/loadgen/sdkclient.go` | `held`, `track`, `initiate`, `handshake`, `errHandshakeTaken`; `replyConn` gets `sc`, `hs`, `closed`, `Close` and the guards |
| `internal/loadgen/recorder_test.go`, `sdkclient_test.go`, `e2e_test.go` | tests |
| `scripts/tier2/gwloss.sh` | done: `stack_expect_dead "$victim" 60`, `send_lag` in `<phase>_storm_real`; Task 3: the comment names the handshake counts |
| `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md` | #109 |

---

### Task 1: `stack_expect_dead NAME [SECONDS]` — done (`8e85cfd`)

`stack_expect_dead NAME [SECONDS]` (default 5) in `scripts/stack.sh`; `gwloss.sh` passes 60 for the victim; `stack_test.sh` has `dead: SECONDS bounds the wait`. Nothing left to do.

---

### Task 2: loadgen sends an unanswered handshake again (#109)

> **Superseded in part:** the closed-connection rule in this task's code (`replyConn.closed`, `owner *replyConn`) was replaced by the newest-dial rule (*Execution Notes*: final review M1–M3), and connections are numbered when their dial starts (interface review P6b2a-reuseI-1). The code as built is `internal/loadgen/sdkclient.go`.

**Precondition:** Tier 2 run 26 is recorded and its b_stress `send_lag` p99 is ≥ 1s, or the user decided #109 after looking at the survivors' side (P-sendlagI-2).

**Files:**
- Modify: `internal/loadgen/recorder.go`, `internal/loadgen/sdkclient.go`
- Test: `internal/loadgen/recorder_test.go`, `internal/loadgen/sdkclient_test.go`, `internal/loadgen/e2e_test.go`

**Interfaces:**
- Consumes: `sdk.Config.Initiate` (plan 6b2a-storm), `precomputer.take`, `prepared{in, frame}`, `wire.Initiate`, `wire.ClientHandshakeWith`, `wire.Respond`; `Recorder.SendLag` and `replyConn.dialed` (`8e85cfd`).
- Produces: `(*Recorder).Handshake(how string)` with `HandshakePrecomputed = "precomputed"`, `HandshakeInline = "inline"`, `HandshakeReused = "reused"` (replaces `Handshake(bool)`); `StormCount.HandshakesReused int64` (`handshakes_reused`) in `/stats`' `storm_since_mark`; unexported `sdkClient.held`, `sdkClient.track`, `sdkClient.initiate`, `handshake`, `errHandshakeTaken`.

- [ ] **Step 1: Write the failing tests**

In `internal/loadgen/recorder_test.go`, `TestRecorderCountsTheStormSinceMark`: its comment's `// #107:` becomes `// #107, #109:`; `r.Handshake(false)` before the mark becomes `r.Handshake(HandshakeInline)`; after the mark, the three calls become

```go
	r.Handshake(HandshakePrecomputed)
	r.Handshake(HandshakeInline)
	r.Handshake(HandshakeInline)
	r.Handshake(HandshakeReused)
```

and the check's last line `s.HandshakesPrecomputed != 1 || s.HandshakesInline != 2 {` becomes `s.HandshakesPrecomputed != 1 || s.HandshakesInline != 2 || s.HandshakesReused != 1 {`.

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
// closed, which then can neither take it nor read a reply for it; one
// answered is never sent again.
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
	// Review O3: A took the held handshake, was cancelled before writing
	// (its connection closed), and B took it too; A's late write must not
	// take it back from B, whose reply would then be refused.
	t.Run("a closed sender's late write", func(t *testing.T) {
		sc, key := handshakeClient(t)
		_, f, _ := sc.initiate(1, key.Public)
		rc0 := sc.track(byteConn{})
		if _, err := rc0.Write(f); err != nil {
			t.Fatal(err)
		}
		rc0.Close() // refused: the handshake is reusable
		_, fa, _ := sc.initiate(1, key.Public)
		rcA := sc.track(byteConn{})
		rcA.Close() // cancelled before its write
		_, fb, _ := sc.initiate(1, key.Public)
		if !bytes.Equal(fa, f) || !bytes.Equal(fb, f) {
			t.Fatal("not sent again")
		}
		rcB := sc.track(byteConn{})
		if _, err := rcB.Write(fb); err != nil {
			t.Fatal(err)
		}
		if _, err := rcA.Write(fa); !errors.Is(err, errHandshakeTaken) {
			t.Fatalf("the cancelled attempt's late write: %v", err)
		}
		if n, err := rcB.Read(make([]byte, 1)); n != 1 || err != nil {
			t.Fatalf("the live sender: %d %v", n, err)
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

In `internal/loadgen/e2e_test.go`, add `"bytes"` and `"slices"` to the imports, and after `TestLightRetriesAPreHandshakeClose` add (review O4):

```go
// refuseFirst forwards to addr like closeFirst, but reads each connection's
// whole handshake frame first: it closes the first n after their frame, as
// a gateway's handshake limit does, and sends the rest on with their frame.
// frames returns the refused and the forwarded frames (#109).
func refuseFirst(t *testing.T, addr string, n int) (string, func() (refused, passed [][]byte)) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	var mu sync.Mutex
	var refused, passed [][]byte
	go func() {
		for k := 0; ; k++ {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func(k int) {
				f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
				if _, err := io.ReadFull(c, f); err != nil {
					c.Close()
					return
				}
				mu.Lock()
				if k < n {
					refused = append(refused, f)
				} else {
					passed = append(passed, f)
				}
				mu.Unlock()
				if k < n {
					c.Close()
					return
				}
				up, err := net.Dial("tcp", addr)
				if err != nil {
					c.Close()
					return
				}
				if _, err := up.Write(f); err != nil {
					up.Close()
					c.Close()
					return
				}
				go func() { io.Copy(up, c); up.Close() }()
				io.Copy(c, up)
				c.Close()
			}(k)
		}
	}()
	return l.Addr().String(), func() ([][]byte, [][]byte) {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(refused), slices.Clone(passed)
	}
}

// #109, review O4: through the real SDK and clientconn, a client whose
// first handshake the gateway refused sends the same frame again and logs
// in with it. The clients start 33ms apart and retry after 0.5–1.5s, so the
// first three connections are the three clients' first attempts.
func TestSDKResendsARefusedHandshake(t *testing.T) {
	env, _ := startStack(t, 1)
	addr, frames := refuseFirst(t, env.GatewayAddrs[0], 3)
	env.GatewayAddrs[0] = addr
	r, err := New(Config{Mode: "sdk", Clients: 3, Rate: 30, UIDPrefix: "reuse"}, env)
	if err != nil {
		t.Fatal(err)
	}
	stop := run(r)
	defer stop()
	waitFor(t, "3 online", 10*time.Second, func() bool { return r.Stats().Online == 3 })
	if s := r.Stats().StormSinceMark; s.HandshakesInline != 3 || s.HandshakesReused != 3 || s.HandshakesPrecomputed != 0 {
		t.Fatalf("storm %+v: want 3 inline, 3 reused", s)
	}
	refused, passed := frames()
	if len(refused) != 3 || len(passed) != 3 {
		t.Fatalf("%d refused, %d passed: want 3 and 3", len(refused), len(passed))
	}
	for _, p := range passed {
		if !slices.ContainsFunc(refused, func(f []byte) bool { return bytes.Equal(f, p) }) {
			t.Fatal("a client logged in with a frame the gateway had not refused")
		}
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go vet ./internal/loadgen`
Expected: FAIL to compile: `undefined: HandshakeInline` (and the other constants), `sc.initiate undefined`, `sc.track undefined`, `sc.held undefined`, `s.HandshakesReused undefined`.

- [ ] **Step 3: The recorder**

In `internal/loadgen/recorder.go`:

1. Replace `Handshake(precomputed bool)` and its comment with:

```go
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

2. In `StormCount`, add after `HandshakesInline`:

```go
	HandshakesReused      int64  `json:"handshakes_reused"`
```

and its comment's end `and whether its first
// message was made ahead of time or when it dialed (#106, #107).` becomes `and where its first
// message came from: made ahead of time, when it dialed, or the last attempt's
// again (#106, #107, #109).`

- [ ] **Step 4: The SDK client**

In `internal/loadgen/sdkclient.go`:

1. Imports: add `"errors"` and `"sync"`.

2. In `sdkClient`, after `queued`, add (gofmt aligns the block):

```go
	held   atomic.Pointer[handshake] // the last attempt's first message while no reply answered it (#109)
```

3. In `newSDK`, the `Dial` hook's `return &replyConn{Conn: nc, rec: r.rec, dialed: time.Now()}, nil` becomes `return sc.track(nc), nil`, and the `Initiate` hook with its comment becomes `Initiate: sc.initiate})`.

4. After `newSDK`, add:

```go
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
```

5. After `event`, add:

```go
// handshake is a client's last first message (#109). An Initiator serves
// one handshake, so it is sent again only while no reply byte has come back
// for it, and only once the connection that sent it last is closed. A
// closed connection can neither take it (a cancelled attempt writing late,
// review O3) nor finish it, so no two attempts can finish it. (An attempt
// the SDK cancels can still be running when the next one starts; the
// cancel closes its connection.)
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

// send: c sends h's frame. False if a reply answered h meanwhile, or c is
// closed (review O3): c must not use it.
func (h *handshake) send(c *replyConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.answered || c.closed.Load() {
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

// errHandshakeTaken fails a connection that may not use the client's held
// handshake: closed, or another connection sent it since (#109).
var errHandshakeTaken = errors.New("loadgen: handshake used by a later attempt")
```

6. `replyConn`: its comment gains a last sentence, `It also tells the client's held handshake which connection may finish it (#109).`; the struct, `Read` and `Write` become (the `rec` field goes: the recorder is `c.sc.r.rec`):

```go
type replyConn struct {
	net.Conn
	sc     *sdkClient
	dialed time.Time
	read   atomic.Int64 // unix ns the latest Read returned
	writes atomic.Int32
	hs     atomic.Pointer[handshake] // the held handshake this connection sent (#109)
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

// Write's first call is the handshake frame, recognized as the held one by
// its backing array: the SDK writes it unchanged in one Write (#109, review
// O4; TestSDKResendsARefusedHandshake).
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

Run: `gofmt -l internal/loadgen; go vet ./internal/loadgen && go test -race -count=3 -run 'Unanswered|HeldHandshake|ResendsARefused|StormSinceMark|AttemptLag|BackoffWaits' ./internal/loadgen && go test -race -count=1 ./internal/loadgen ./cmd/loadgen ./internal/sdk`
Expected: no gofmt output; all PASS (`TestSDKStormUsesPrecomputedHandshakes` ~3s, `TestSDKResendsARefusedHandshake` ~2s).

Mutation checks (revert each after): drop `|| c.closed.Load()` from `send` → `a closed sender's late write` fails; make `Write` never match the frame (`&b[0] == &h.pr.frame[0]` → `false`) → `TestSDKResendsARefusedHandshake` fails (`0 reused`) — the first through the unit test, the second through the real SDK path.

- [ ] **Step 6: Commit**

```bash
git add internal/loadgen/recorder.go internal/loadgen/recorder_test.go internal/loadgen/sdkclient.go internal/loadgen/sdkclient_test.go internal/loadgen/e2e_test.go
git commit -m "feat(loadgen): an unanswered handshake is sent again (#109)"
```

---

### Task 3: `gwloss` names the handshake counts; roadmap; Tier 2 run 27 (#109)

`send_lag` in `<phase>_storm_real` and its header text are done (`8e85cfd`). The check's detail records the `storm` object whole, so `handshakes_reused` appears in it without a script change.

**Files:**
- Modify: `scripts/tier2/gwloss.sh` (the `<phase>_storm_real` comment)
- Modify: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`

- [ ] **Step 1: The comment**

In `scripts/tier2/gwloss.sh`, in the comment above `<phase>_storm_real`, after `(p99; loadgen's own lags only, review I1/I2).` add `Its detail has the storm's handshake counts: precomputed, inline and reused (#109; run 25: 5.2M inline, before #109).`

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

Expected: 29 checks, all pass except possibly the two swap checks (the dev machine). Both `<phase>_storm_real` details show `handshakes_reused` (likely 0 at this scale: nothing is refused; the reuse itself is covered by `TestSDKResendsARefusedHandshake`). Remove the dry run's `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 3: Roadmap and commit**

Add #109 to the roadmap's choices (as in this plan's *Implementation choices*), and set the plan 6 row's *next* to "Tier 2 run 27 (#109)".

```bash
git add scripts/tier2/gwloss.sh docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-14-connection-session-core-06b2a-reuse.md
git commit -m "docs(tier2): storm_real's handshake counts; #109 in the roadmap"
git push
```

- [ ] **Step 4: Tier 2 run 27 (the user, on the host)**

Host prep as for run 25, then `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss`. Expected: 29/29. What to read, against run 26: `b_stress_storm_real` (`attempt_lag`, `send_lag`, `reply_lag` p99; `handshakes_inline` against `handshakes_reused`: inline should be about one per answered handshake), loadgen's `storm_first30` cores against 14, `b_stress_back`'s relogin distribution (b-stress's measurement once `storm_real` passes), the survivors' `handshake_rate`, `pending_mem`, `handshake_timeout` and `auth_timeout` rejects (`handshake_timeout` should fall if run 26 showed late frames), and the attempt count against run 26's (it falls too if late frames fed retries). If `b_stress_storm_real` still fails, take the storm's CPU profile (`go tool pprof -top b_stress/artifacts/storm/loadgen.cpu.pb.gz`): what remains is per-connection work, and the next step is a second machine for loadgen.

## After this plan

6b2a-stress is complete when a run passes 29/29 (run 26 if it already does without #109; otherwise run 27); then the README's *Measured results* (both languages) get b-stress's numbers, and `main` is fast-forwarded. 6b2b (b2 and slowed Logic) follows in a fresh session.

## Execution Notes
- **User decision (2026-10-14):** execute Task 1 (`stack_expect_dead NAME [SECONDS]`) and `send_lag` now; #109's reuse waits for run 26's `send_lag`, which says whether the survivors' `handshake_timeout` rejects come from loadgen. Done inline as one commit, without the reuse: `Recorder.SendLag`, `StormCount.SendLag` (`send_lag`), `replyConn.dialed` and its first write recording the lag; `Handshake(bool)` stays (no `HandshakesReused`, no `HandshakeInline`/… constants, no `held`/`track`/`initiate`); `<phase>_storm_real` bounds `send_lag` like the other two. `send_lag` is cited as #107 (one of the storm's loadgen lags, run 25), not #109. `TestRecorderCountsTheStormSinceMark` and `TestSDKStormUsesPrecomputedHandshakes` check `send_lag`; `stack_test.sh` has `dead: SECONDS bounds the wait`. `go test -race` for `loadgen` and `cmd/loadgen`, and `make test-scripts` green. Dry run (2 × 1,000 / 1,500): 27/29, only the two swap checks (the dev machine); `send_lag` n 1000 / 1500, p99 0ms in both phases; `b_stress_processes` passes (the victim reaped). Task 2's reuse and Task 3's remaining text (`handshakes_reused`) apply once #109 is decided.
- **`send_lag` interface review (2026-10-14, Claude; decided as proposed by the user, "apply all"):** P-sendlagI-1..4. **Checked, unchanged:** the first write on an SDK connection is the handshake frame (`clientconn.HandshakeWith` only sets a deadline before `wire.ClientHandshakeWith` writes it), the Login frame the second, as `reply_lag` assumes; `dialed` and the first `Write` run on the attempt's own goroutine, on the monotonic clock; the histogram holds seconds (run 25's `attempt_lag` max 2,112ms).
  - **P-sendlagI-1 (fixed):** the roadmap's #107 now records `send_lag` as an amendment, and why it does not bring back what I1 removed: it starts after the TCP connect and ends at the `Write` call, so a full accept queue is not in it.
  - **P-sendlagI-2 (decided): run 26 (without #109) and what decides #109.** Expected: `b_stress_storm_real` still fails on `attempt_lag` (loadgen's CPU is unchanged); `b_stress_back`, `b_stress_idle` and `b_stress_processes` pass (the victim reaped within 60s). Then: if b_stress's `send_lag` p99 is ≥ 1s, the survivors' `handshake_timeout` rejects are late frames from a saturated loadgen, and #109 goes ahead (Task 2's reuse, Task 3's `handshakes_reused`), then run 27. If `send_lag` p99 is small, frames left loadgen on time and were not handled within 5s: look at the survivors' side first (their storm CPU profiles, `handshakes_ok` against `handshake_timeout` over the storm), then decide #109.
  - **P-sendlagI-3 (noted):** `send_lag` includes an inline `Initiate` (two X25519, ~0.1–0.3ms), far below the 100ms bound.
  - **P-sendlagI-4 (noted):** an attempt that never writes (cancelled before it, or `Initiate` failed) has no sample; rare, and every relogin writes at least once, so `n ≥ population` holds.
- **Plan review (2026-10-14, advocatus diaboli; O1–O5 accepted by the user):** record `docs/superpowers/objections/connection-session-core-06b2a-reuse.md`, decisions in `…-reuse-dispositions.md`. O1: #109 waits for run 26 (P-sendlagI-2). O2: Tasks 2–3 rewritten as the delta after `8e85cfd`; the first run with #109 is run 27. O3: `handshake.send` refuses a closed connection (a cancelled attempt's late write), test case `a closed sender's late write`. O4: `TestSDKResendsARefusedHandshake` (sdk mode through `refuseFirst`) and the frame-match assumption stated in #109. O5: the reason for never reusing an answered handshake is the Initiator's used-up state (NK's reply brings a fresh responder ephemeral, so keys would not repeat). Task 2 replayed from `d4c5b3c` with O3–O4 in it: all green, both mutations caught (see *Size*).
- **Tier 2 run 26 (`d4c5b3c`, 2026-10-14; recorded 2026-10-15): 27/29.** Failed: `no_swap_in_b_stress` (1 page in) and `b_stress_storm_real` (`attempt_lag` p99 1,382ms, `send_lag` p99 2,538ms / p90 57ms, `reply_lag` p99 0; 600,000 precomputed, 5,360,005 inline). The 60s `stack_expect_dead` worked: `b_stress_back` (all back in 546s, `loadgen_bound`), `b_stress_idle` and `b_stress_processes` pass. b1 passed in full (p99 22.6s; `send_lag` p99 0). Survivors (each): `handshakes_ok` ~318k, `handshake_rate` ~604k, `pending_mem` ~209k, `handshake_timeout` ~1.94M, `auth_timeout` ~10k; gateway CPU set 88% idle over the storm, 2.0 cores each in its first 30s. Storm profiles: gw-1 X25519 57%, syscalls 25%; loadgen 13.7 cores, X25519 46%, syscalls 19%, GC ~15%. Late frames cannot explain 3.88M timeouts (dispositions, *Follow-up*), so the next run is a b_stress diagnostic: `gwloss` gained `GWLOSS_SS_EVERY` (socket samples from the kill until the victims are back; `ss_<phase>` in the result). Dry run (b_stress alone, 1,500 clients, every 2s): 15 checks, only the swap check failed (the dev machine), 10 samples, `ss` ~20ms each.
- **b_stress diagnostic run (`8ea8930`, `GWLOSS_PHASES=b_stress GWLOSS_SS_EVERY=10`, 2026-10-15): 13/15.** Failed: `no_swap_in_b_stress` (272 pages in) and `b_stress_storm_real` (`attempt_lag` p99 1,438ms, `send_lag` p99 3,540ms / p90 9ms / max 13,581ms; 5,316,831 inline). All 600,000 back in 428s (`loadgen_bound`). Survivors (each): `handshake_timeout` ~1.87M, `handshakes_ok` ~334k, `no_slot` ~633k, `pending_mem` ~221k. 37 socket samples, `ss_ms` 0.4–2.6s. **The handshake timeouts are frames loadgen had not written yet (b), not frames unread on the survivors (a) or the accept queue (c):**
  - **Not the accept queue:** `accept_q` 0–1 of 65,535 in every sample; `ListenOverflows`, `ListenDrops`, `TCPReqQFullDrop` 0.
  - **Not the survivors' read path:** the gateway runs the handshake inline on the loop (`tcp/conn.go` `read` → `handshake`); every whole frame read becomes `ok`, `no_slot`, `failed` or `noise_unknown_key`. From t=39s to t=141s after the kill, gw-1's `handshakes_ok` stays at 128,519 and `no_slot` at 456,072 (gw-2 likewise), `failed` and `noise_unknown_key` 0, while `handshake_timeout` grows by ~3.23M on both (~33k new connections/s), Recv-Q 0 on every survivor socket and the gateway CPU set 85% idle.
  - **Loadgen had not written:** in the same window loadgen's sockets to the survivors have Send-Q 0: nothing written and unacked. Send-Q and Recv-Q are non-zero only in samples where frames flow (t=5, 15, 152, 165). Both survivors stall and resume at the same samples: the common cause is loadgen. `EstabResets` 3.97M fits loadgen writing onto sockets the survivor had already closed.
  - **Why `send_lag` missed it:** it starts at `dialed`, after `DialContext` returns, when the dialing goroutine runs again; the survivor's 5s starts at the accept, inside the connect. The gap between them (TCP connect plus loadgen's wait to run the goroutine) was measured by nothing. Inferred, not measured: a goroutine wait above 5s held up by retries (each timed-out attempt dials again), ending at ~t=145s once clients are in longer backoffs (857k backoff, 479k failure waits). A suspect to check: loadgen's storm RSS 16,573MB against `GOMEMLIMIT=16GiB` (GC ~15% in run 26's profile).
- **`connect_lag` (the user, 2026-10-15; #107 amended):** a fourth loadgen lag, from `Balancer.Dial` calling connect (`conn.connecting`, stamped just before `DialContext`) to the dial's return in the SDK client's `Dial` (`Recorder.ConnectLag`, `StormCount.ConnectLag`, `/stats`' `storm_since_mark.connect_lag`), reset at each mark; `<phase>_storm_real` bounds it like the other three (`n ≥ population`, p99 ≤ `GWLOSS_LAG_P99_MS`). SDK clients only, as `send_lag`. `TestRecorderCountsTheStormSinceMark` (a negative lag counts as 0) and `TestSDKStormUsesPrecomputedHandshakes` check it; both failed first (`s.ConnectLag undefined`); with the SDK's `ConnectLag` call removed the e2e test fails. `go test -race` for `loadgen` and `cmd/loadgen`, `make test-scripts` green. Dry run (b_stress alone, 1,500 clients, `GWLOSS_SS_EVERY=2`): 15 checks, only the swap check failed (the dev machine); `connect_lag` n 1500, p99 1ms. **Next:** a b_stress run with `connect_lag` (`GWLOSS_PHASES=b_stress GWLOSS_SS_EVERY=10`): a `connect_lag` p99 near or above 5s confirms the gap; then decide #109.
- **b_stress run with `connect_lag` (`0fe01ae`, `GWLOSS_PHASES=b_stress GWLOSS_SS_EVERY=10`, 2026-10-15): 12/15.** Failed: `no_swap_in_b_stress` (12 pages in), `b_stress_storm_real` and `b_stress_loadgen_headroom` (loadgen 11.7 of 14 cores over the 622s storm, 12.8 in its first 30s). All 600,000 back in 622s. **`connect_lag` p50 8,922ms, p90 9,604, p99 9,987, max 11,554 (n 5.84M)** against the survivors' 5s handshake timer: most dials returned after the survivor had already timed the connection out. Once a dial returned, the frame went out at once (`send_lag` p90 0ms, p99 1,345ms). The samples bunch under 10s because the SDK's `ConnectTimeout` (dial and handshake together, 10s) cuts the slower dials off unsampled: 6.12M attempts, 5.84M samples, so the real wait is longer still. Socket samples as in the diagnostic run: no handshake completed on either survivor from t≈28s to t≈142s (`handshakes_ok` 114,471 / 114,368), Recv-Q and Send-Q 0, `accept_q` 0–8 (one sample of 129); `handshake_timeout` ~1.9M per survivor. A loopback connect takes well under 1ms and the survivors accepted at once, so the ~9s is loadgen's wait to run the dialing goroutine again.
- **Loadgen's storm CPU profile (first 30s, `artifacts/storm/loadgen.cpu.pb.gz`):** 424.77s of samples in 31.23s, 13.6 of 14 cores: saturated, so a goroutine woken by its finished connect queues behind that work. `crypto/ecdh.x25519ScalarMult` 43% cum, nearly all the inline first message (5.24M inline × 2 X25519 ≈ 10.5M; the 608k replies' `Finish` ≈ 0.6M, ~6%). GC an estimated 20–25% (`markroot` 9.4% cum, `scanSpan` 6.6%, `tryDeferToSpanScan` 7.4%, `scanblock` 3.9%, stack walking in `unwinder`/`pcvalue`/`step`: hundreds of thousands of goroutine stacks); `-top` omits `gcBgMarkWorker`/`gcAssistAlloc`, so not exact, and whether `GOMEMLIMIT` drives it is not known. Syscalls 19% (`Syscall6`).
- **Recommendation (Claude, 2026-10-15; not yet decided by the user):** go ahead with #109 (Tasks 2–3 as reviewed, O2–O5 applied), then run 27. ~5.37M attempts ended before the handshake, every one unanswered, which is what #109 sends again; removing most inline X25519 should take loadgen from ~13.6 to ~8 cores in the storm's first 30s, so woken goroutines run within milliseconds and fewer attempts time out and retry (inferred; run 27 tests it). #109 leaves the GC cost; `b_stress_storm_real`'s 100ms p99 bound on the four lags may still fail; the survivors' ~1.2M `no_slot` rejects are the gateway's limiter and stay. If loadgen is still CPU-short after #109: a run with `GODEBUG=gctrace=1` for loadgen.
- **#109 executed (the user's decision, 2026-10-15; inline):** Task 2 as written (`22bea9e`), one ruling: the brief predates `connect_lag` (`0fe01ae`), so the `Dial` hook keeps `ConnectLag`, now taken against `track(nc).dialed`. The red step failed to compile as stated (`undefined: HandshakeInline`, `sc.initiate`, `sc.track`, `s.HandshakesReused`); then `gofmt` and `go vet` clean, the new tests pass 3 times under `-race`, `loadgen`, `cmd/loadgen` and `sdk` pass. Both mutations fail as stated (`a closed sender's late write`: the late write returns nil; `TestSDKResendsARefusedHandshake`: 6 inline, 0 reused). Task 3: the `<phase>_storm_real` comment names the handshake counts; #109 in the roadmap; `make test-scripts` green. Dry run (2 × 1,000 / 1,500, both phases): 29 checks, 28 pass, only `no_swap_in_b_stress` (323 pages in, the dev machine); `handshakes_reused` 0 in both phases (nothing refused at this scale), every lag p99 ≤ 1ms. **Run 27 (b_stress only, as the user asked):** host prep as for run 25, then `GOIM_TEST_TIER=2 GWLOSS_PHASES=b_stress GWLOSS_SS_EVERY=10 make test-tier2 SCENARIO=gwloss` (15 checks). Read against the `connect_lag` run (`0fe01ae`): `handshakes_inline` against `handshakes_reused` (inline ≈ one per answered handshake, ~0.6M, against 5.24M), loadgen's `storm_first30` cores against 13.6 of 14, `connect_lag` p50/p99 against 8.9s/10.0s, the survivors' `handshake_timeout` (~1.9M each), the attempt count (6.12M), and the socket samples' stall (no handshake from t≈28s to t≈142s). If `b_stress_storm_real` still fails: the storm's CPU profile, then a run with `GODEBUG=gctrace=1` for loadgen, or a second machine.
- **Final review minors fixed (the user, 2026-10-15; #109 amended):** M1 (two finishes with three overlapping cancelled attempts), M2 (a cancelled attempt's late write failed the live one) and M3 (a waiting client kept its closed connection) share one cause, the closed-connection rule. Replaced by the newest-dial rule: `track` numbers each connection (`sdkClient.dials`); `send`, `reply` and the first `Write` refuse a connection older than the newest; the held handshake keeps the sender's number; an unanswered handshake is always sent again; `replyConn.closed` and its `Close` are gone. `TestAHeldHandshakeIsFinishedOnce` now has five cases (`a newer attempt takes it from an open sender`, `an older attempt's late write` (M2), `two cancelled attempts` (M1), `answered before the resend`, `keeps no closed sender` (M3, a `weak` pointer after `runtime.GC`)); `TestUnansweredHandshakeIsSentAgain` dials before it initiates, as the SDK does. Red first: four cases failed for the reasons named; green 3× under `-race`. Mutations: dropping the first `Write`'s dial check fails `two cancelled attempts` (a late write of the answered handshake); dropping `reply`'s dial check fails `a newer attempt takes it from an open sender`. `make test` and `make test-load-probe` green. Run 27 as above.
- **Interface review of the M1–M3 fix (2026-10-15, Claude; decided by the user, "apply all"):** P6b2a-reuseI-1..3. **I-1:** `track` numbered a connection when its dial returned, so a cancelled attempt whose dial returned after the live attempt's took the newest number; its late `initiate` and first `Write` took the held handshake, and the live attempt's first `Write` failed with `errHandshakeTaken` (M2 again, by another order). Fixed: the `Dial` closure takes the number when the dial starts (`seq := sc.dials.Add(1)`), `track(nc, seq)`; `sc.gw` is stored only when `seq` is still the newest (a late dial no longer counts the client on the wrong gateway, older than #109). New subtest `a cancelled attempt's dial returns late`: red on the old numbering (scratch test before the fix), and the mutation numbering at `track` fails it. Not reachable in loadgen today (the SDK cancels only on `Close`, `Logout`, `SwitchAccount`; loadgen calls only `Connect` and `Close`), so run 27 was unaffected; 6b2b's Logout → Connect could reach it. **I-2:** Task 2 got a note that its code's closed-connection rule is superseded. **I-3:** #109's two gateway assumptions are written in `initiate`'s comment and roadmap #109: a refusal closes with no byte sent, and there is no replay check on a first message (Noise NK message 1 is replayable). No gate: `handshakes_reused` is already in `<phase>_storm_real`'s detail. **Checked, unchanged:** the SDK dials before `Initiate` and runs one attempt at a time; a refused handshake stays reusable (`Reject` → `closeConn`, `ErrClosedBeforeReply`); the first reply byte clears the held handshake, so no `Initiator` is used after `Finish`.
