# Plan 6b2a-storm: a Real Storm in Tier 2 (b-stress) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `gwloss`'s storms the spec's storms, not loadgen's. After the kill, every victim's first attempt must reach a survivor when the SDK's timing says it should. The run proves it with a measured check, and fails when the check does not hold. Today b_stress is recorded as `loadgen_bound` (#102): in Tier 2 runs 22–24 loadgen's 8 CPUs spent the storm on X25519 for handshakes that timed out (~3.2M `handshake_timeout` per survivor against ~342k handshakes ok), so the relogin times (p99 ~441s) measured loadgen. The user wants a real storm, and rejected throttling loadgen's attempts: a throttle makes the storm gentler than the real one (2026-10-14).

**Architecture:** Three parts.
1. **Precompute what a phone already holds (#106).** A phone whose connection drops already has a cached token, and its own CPU makes its handshake message. Loadgen pays for both on 8 shared CPUs at the worst moment. With `-precompute`, loadgen prepares each logged-in SDK client's next token and Noise first message (the ephemeral key and `es`, the two X25519 operations of the client's three) in background workers during the ramp, and stops at the first mark. The survivors receive the same bytes at the same moments. The SDK gets `Config.Initiate`, the hook that hands an attempt its first message; production leaves it nil. `wire` and `clientconn` get `ClientHandshakeWith` and `HandshakeWith`, which take a first message made earlier.
2. **Measure how late each attempt is (#107).** Loadgen records, for every attempt that follows a wait, the time from the wait's end to the attempt's first write (its handshake frame): `attempt_lag`, since the latest mark. A new check `<phase>_storm_real` passes only if every logged-in client was prepared at the kill and the storm's attempt lag p99 is at most `GWLOSS_LAG_P99_MS` (100ms). An invalid storm fails the run; it is not recorded as a pass.
3. **Give loadgen the idle CPUs in b_stress (#108).** In runs 22–24 the gateway set was 92–93% idle in the storm and the Logic set 96%. b_stress moves loadgen to `0-5,12-17` (12 CPUs) and the gateways to `6-9,18-21` (8 CPUs, `GOMAXPROCS` 4 each as before: the victim alone in the ramp, the two survivors after the kill). b1 keeps #71's sets.

**Tech Stack:** Go 1.27.1, `github.com/flynn/noise` v1.1.0, bash 5.2, python3 (reads JSON only).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`, *Client SDK rules* (line 127: after losing an established connection, wait a uniform random 0–20s before the first attempt; `Token` before every attempt, a new one only on `refresh`), *Verification → Load tests* (line 540: "(b-stress) kill a 600k gateway: measured, no SLO"), line 24 ("200k users spread over the 0–20s reconnect jitter is ~10k logins/s"), *Transport* (Noise NK). Roadmap choices #71, #79, #93, #102, and the user's decision of 2026-10-14 (a real storm; no throttle; fall back to a second machine, not a throttle, if one host cannot make it).

**Branch:** `feat/plan6b2a-stress` (head `ef3ace4`, runs 23 and 24 recorded).

**Size:** 3 tasks, ~44KB. Tasks 1–2 were prototyped in a throwaway worktree from `ef3ace4` by applying this plan's code blocks: `go vet` clean, and `go test -race` passes for `wire`, `clientconn`, `sdk`, `loadgen` and `cmd/loadgen`, with the new tests stable over 3 repetitions (`TestSDKStormUsesPrecomputedHandshakes` ~3s). The fixes the prototype needed are in the steps (an import, gofmt alignment, the `Initiate` field's place). Task 3 (shell) is not prototyped: its dry run is part of execution. The per-handshake numbers were measured on the dev machine.

## Global Constraints

- Go 1.27.1, module `github.com/Tallinzp/goim`; no new module.
- `make test` (`go test -race -count=1 ./...`), `make test-race-suite`, `make test-scripts` and `make test-load-probe` stay green on the dev machine (2 cores / 2GB), loopback only.
- Production behaviour does not change: `sdk.Config.Initiate` nil means `wire.Initiate` with `CMD_HANDSHAKE`, computed after the dial, exactly as today. The gateway, Logic and Lua are untouched.
- Scripts: bash with `set -uo pipefail`; JSON without jq; python3 only reads JSON. The host's awk is mawk.
- CPU sets: b1 keeps #71 (gateways `4-9,16-21`, Logic + Redis `10,11,22,23`, loadgen `0-3,12-15`). b_stress per #108. The host's SMT siblings are CPU n and n+12 (the sets above already assume it).
- SDK timing is the spec's and is not changed: storm wait 0–20s uniform, pre-handshake retry 0.5–1.5s, other retryable rejects 1–3s, backoff 2.5–5s doubling to 5 min.
- Memory: a prepared client costs ~679 bytes of live heap for the handshake state and frame (measured: 100,000 `wire.Initiate` results) plus its token string (~300 bytes), ~0.6GB at 600k. `t2_mem_budget` stays 23GiB (#105 amended). Swap during a measured window fails it (#70).
- New behaviour cites its choice number (#106–#108).

## Review Focus

1. **A prepared handshake must be used once.** A Noise `HandshakeState` used twice would send the same ephemeral key twice; the server would answer, and the client's `Finish` would fail on the second. `sdkClient.pre` is taken with `Swap(nil)`, so an attempt either takes it or makes its own. Test: `TestSDKStormUsesPrecomputedHandshakes` (Task 2: `handshakes_precomputed` is exactly the victims' 15, and the survivor's 15 still hold theirs).
2. **No preparing after the kill.** Preparing during the storm would spend loadgen's CPU exactly where the plan frees it. `Runner.Mark` stops the precomputer before the recorder takes the mark, and clients that log in again after it are not queued. Test: the same e2e test (`ready` is 15 after the storm, not 30).
3. **An attempt without a wait has no lag.** The first `Connect` and a local trigger start an attempt with no wait; counting them would put the ramp's attempts (lag from no due time) into the storm's numbers. `sdkClient.due` is set only on `EventWaiting` and taken with `Swap(0)` at the first write. Test: `TestRecorderCountsTheStormSinceMark` (Task 2) and the e2e test (`attempt_lag.n` ≥ 15, all after the mark).
4. **The lag clock is wall time.** `due` and the first write both use `time.Now().UnixNano()`; a clock step backwards would give a negative lag. `Recorder.AttemptLag` counts a negative lag as 0. Test: `TestRecorderCountsTheStormSinceMark`.
5. **`<phase>_storm_real` fails on a partial precompute.** If the workers have not prepared every logged-in client by the kill (the wait gives up after 120s), the storm's first attempts partly compute inline and its lag is not the spec's storm. The check requires `ready == online` at the kill, as well as the lag. Test: Task 3's dry run with `GWLOSS_DRY_NO_PRECOMPUTE=1` fails both phases' `storm_real` checks.

## Implementation choices this plan makes

Proposed 2026-10-14 with this plan, for the user's review. #106 was decided in principle by the user on 2026-10-14 (a real storm, never a throttle).

106. **Loadgen prepares what a phone already holds** (`-precompute`, sdk mode only). When an SDK client logs in, it is queued (a channel sized to `Clients`, so `OnEvent` never blocks) for one of `max(1, GOMAXPROCS/2)` workers, which signs its next token (`Runner.token`, as today) and runs `wire.Initiate` for the pinned key. The client takes both at its next attempt: `Config.Token` returns the prepared token unless `refresh` is set, and `Config.Initiate` swaps the prepared first message out, or computes one inline when there is none. Preparing stops at the first mark (`Runner.Mark`, which `/kill` also calls). Retries after the storm's first attempt compute inline (counted). Per attempt this moves the ephemeral key, `es` and the token's Ed25519 signature off the storm: on the dev machine `wire.Initiate` costs 281µs and `Finish` 153µs, so the storm's client-side handshake cost falls to about a third. The server sees the same bytes. **SDK:** `Config.Initiate func(keyID uint16, serverPub []byte) (*wire.Initiator, []byte, error)`, nil by default; `wire.ClientHandshakeWith` and `clientconn.Conn.HandshakeWith` run a handshake from a first message made earlier. Rejected: an in-flight handshake cap (a throttle: the user's decision); precomputing only the ephemeral key (`noise.Config.EphemeralKeypair`), which saves one X25519 of three; a shared pool refilled during the storm (spends the storm's CPU).
107. **Attempt lag and `<phase>_storm_real`.** The SDK client stores `due = now + Wait.D` (unix ns) on every `EventWaiting`. Loadgen's `Dial` wraps the connection; its first `Write` (the handshake frame) records `now − due` in the recorder's `attempt_lag` and clears `due`. The recorder keeps `attempt_lag` and the handshake counts (`handshakes_precomputed`, `handshakes_inline`) since the latest mark, like `backoff_since_mark`: `/stats`' `storm_since_mark` `{mark, attempt_lag, handshakes_precomputed, handshakes_inline}`. `gwloss.sh` waits up to 120s (wall clock, #103) before the kill until `precompute.pending` is 0 and `precompute.ready` equals `online`, and records `precompute_at_kill`. **New check in both phases:** `<phase>_storm_real`: `ready == online` at the kill, `attempt_lag.n` ≥ the population, `attempt_lag.p99_ms` ≤ `GWLOSS_LAG_P99_MS` (default 100). `b_stress_back` keeps its rule (every victim back within the wait, no rejects at capacity); its relogin distribution is the measurement (b-stress has no SLO) once `b_stress_storm_real` passes. `loadgen_bound` stays recorded (loadgen's CPU in the storm's first 30s) as data, no longer the run's verdict. Detection (mark → loss noticed) is recorded as now and not gated: it includes the dying gateway's own socket teardown, which production has too.
108. **b_stress's CPU sets.** `GWLOSS_STRESS_CPUS_LOADGEN` (default `0-5,12-17`, 12 CPUs) and `GWLOSS_STRESS_CPUS_GATEWAY` (default `6-9,18-21`, 8 CPUs), with `STACK_GATEWAY_PROCS=4` so each gateway keeps 4 `GOMAXPROCS` (without it #79 would give 8 / 3 = 2). Logic and Redis keep `10,11,22,23`. b1 keeps #71's sets. The `pinning` check runs in every phase, renamed `<phase>_pinning`, because b_stress's sets differ. 4 + 4 survivor procs fit the 8 gateway CPUs exactly; the victim runs alone in the ramp (1.7 cores in run 24). If the user's `lscpu -e` shows the fast cores elsewhere, the defaults change before the run, not the mechanism.

If `<phase>_storm_real` fails on the host after this plan, the next step is a second machine for loadgen, or 6d's cloud target, not a throttle (the user, 2026-10-14).

## File Structure

| File | Change |
|---|---|
| `internal/wire/noise.go` | `ClientHandshakeWith`; `ClientHandshake` calls it |
| `internal/wire/noise_test.go` | `TestClientHandshakeWithAPrecomputedInitiator` |
| `internal/clientconn/conn.go` | `HandshakeWith`; `Handshake` calls it |
| `internal/sdk/client.go` | `Config.Initiate`, defaulted in `New` |
| `internal/sdk/attempt.go` | `login` calls `cfg.Initiate`, then `HandshakeWith` |
| `internal/sdk/client_test.go` | `TestInitiateMakesTheAttemptsHandshakeFrame` |
| `internal/loadgen/precompute.go` (new) | `precomputer`, `prepared`, `Runner.prepare` |
| `internal/loadgen/sdkclient.go` | prepared token and frame, `due`, `lagConn` |
| `internal/loadgen/recorder.go` | `AttemptLag`, `Handshake`, `StormCount`, `Counts.StormSinceMark` |
| `internal/loadgen/runner.go` | `Config.Precompute`, `Stats.Precompute`, workers in `Run`, stop in `Mark` |
| `internal/loadgen/recorder_test.go`, `runner_test.go`, `e2e_test.go` | tests |
| `cmd/loadgen/main.go`, `main_test.go` | `-precompute` |
| `scripts/tier2/gwloss.sh` | per-phase CPU sets, `-precompute`, the wait before the kill, `<phase>_storm_real`, `<phase>_pinning`, header |
| `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md` | #106–#108 |

---

### Task 1: a handshake from a first message made earlier (#106, SDK side)

**Files:**
- Modify: `internal/wire/noise.go` (`ClientHandshake`, ~line 238)
- Modify: `internal/clientconn/conn.go` (`Handshake`, ~line 65)
- Modify: `internal/sdk/client.go` (`Config`, `New`)
- Modify: `internal/sdk/attempt.go` (`login`, ~line 79)
- Test: `internal/wire/noise_test.go`, `internal/sdk/client_test.go`

**Interfaces:**
- Produces: `wire.ClientHandshakeWith(rw io.ReadWriter, in *wire.Initiator, frame []byte) (*wire.Session, error)`; `(*clientconn.Conn).HandshakeWith(in *wire.Initiator, frame []byte, deadline time.Time) error`; `sdk.Config.Initiate func(keyID uint16, serverPub []byte) (*wire.Initiator, []byte, error)` (nil: `wire.Initiate(keyID, serverPub, uint16(clientpb.Cmd_CMD_HANDSHAKE))`).

- [ ] **Step 1: Write the failing wire test**

Append to `internal/wire/noise_test.go` (it already imports `bytes`, `io` and `net`):

```go
// #106: a first message made ahead of time completes a handshake later, and
// the server receives exactly its bytes.
func TestClientHandshakeWithAPrecomputedInitiator(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	in, frame, err := Initiate(7, key.Public, 1)
	if err != nil {
		t.Fatal(err)
	}
	c, s := net.Pipe()
	defer c.Close()
	defer s.Close()
	srv := make(chan *Session, 1)
	go func() {
		got := make([]byte, HeaderLen+HandshakeLen)
		if _, err := io.ReadFull(s, got); err != nil || !bytes.Equal(got, frame) {
			srv <- nil
			return
		}
		sess, reply, err := Respond(key, got, 1)
		if err != nil {
			srv <- nil
			return
		}
		if _, err := s.Write(reply); err != nil {
			srv <- nil
			return
		}
		srv <- sess
	}()
	cs, err := ClientHandshakeWith(c, in, frame)
	if err != nil {
		t.Fatal(err)
	}
	ss := <-srv
	if ss == nil {
		t.Fatal("the server did not receive the precomputed frame, or could not answer it")
	}
	sealed, err := cs.Seal(nil, 9, 1, []byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, body, _, err := ss.Open(nil, sealed); err != nil || string(body) != "hi" {
		t.Fatalf("open: %q %v", body, err)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/wire -run TestClientHandshakeWithAPrecomputedInitiator`
Expected: FAIL to compile: `undefined: ClientHandshakeWith`.

- [ ] **Step 3: Split `ClientHandshake`**

In `internal/wire/noise.go`, replace `ClientHandshake` with:

```go
// ClientHandshake runs the client side over rw.
func ClientHandshake(rw io.ReadWriter, keyID uint16, serverPub []byte, cmd uint16) (*Session, error) {
	in, f, err := Initiate(keyID, serverPub, cmd)
	if err != nil {
		return nil, err
	}
	return ClientHandshakeWith(rw, in, f)
}

// ClientHandshakeWith runs the client side over rw from Initiate's result,
// which may have been made ahead of time (loadgen's precomputed first
// message, #106). An Initiator serves one handshake.
func ClientHandshakeWith(rw io.ReadWriter, in *Initiator, frame []byte) (*Session, error) {
	if _, err := rw.Write(frame); err != nil {
		return nil, err
	}
	reply := make([]byte, HeaderLen+HandshakeMsgLen)
	if n, err := io.ReadFull(rw, reply); err != nil {
		if n == 0 && (errors.Is(err, io.EOF) || errors.Is(err, syscall.ECONNRESET)) {
			return nil, fmt.Errorf("%w: %w", ErrClosedBeforeReply, err)
		}
		return nil, err
	}
	return in.Finish(reply)
}
```

- [ ] **Step 4: Run the wire tests**

Run: `go test -race ./internal/wire`
Expected: PASS.

- [ ] **Step 5: `clientconn.HandshakeWith`**

In `internal/clientconn/conn.go`, replace `Handshake` with:

```go
// Handshake runs Noise NK against the server key pinned under keyID. It
// must finish by deadline (zero: none). A close or reset before any reply
// byte is reported as errors.Is(err, ErrClosedBeforeReply).
func (c *Conn) Handshake(keyID uint16, serverPub []byte, deadline time.Time) error {
	in, f, err := wire.Initiate(keyID, serverPub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return err
	}
	return c.HandshakeWith(in, f, deadline)
}

// HandshakeWith is Handshake from wire.Initiate's result for CMD_HANDSHAKE,
// made ahead of time (loadgen's precomputed first message, #106).
func (c *Conn) HandshakeWith(in *wire.Initiator, frame []byte, deadline time.Time) error {
	if err := c.nc.SetDeadline(deadline); err != nil {
		return err
	}
	sess, err := wire.ClientHandshakeWith(c.nc, in, frame)
	if err != nil {
		return err
	}
	c.sess = sess
	return c.nc.SetDeadline(time.Time{})
}
```

- [ ] **Step 6: Write the failing SDK test**

Append to `internal/sdk/client_test.go` (add `bytes`, `net` and `github.com/Tallinzp/goim/internal/wire` to its imports; it already has `sync`):

```go
// #106: Config.Initiate makes the attempt's first handshake frame, once per
// attempt, and the gateway receives exactly its bytes.
func TestInitiateMakesTheAttemptsHandshakeFrame(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			c.pong()
		})
		var mu sync.Mutex
		var made [][]byte
		written := make(chan []byte, 1)
		c, r := testClient(t, f, &MemStore{}, func(cfg *Config) {
			cfg.Initiate = func(keyID uint16, pub []byte) (*wire.Initiator, []byte, error) {
				in, fr, err := wire.Initiate(keyID, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
				mu.Lock()
				made = append(made, fr)
				mu.Unlock()
				return in, fr, err
			}
			cfg.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
				nc, err := f.dial(ctx, addr)
				return &firstWrite{Conn: nc, out: written}, err
			}
		})
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline)
		mu.Lock()
		defer mu.Unlock()
		if len(made) != 1 {
			t.Fatalf("Initiate called %d times, want 1", len(made))
		}
		if w := <-written; !bytes.Equal(w, made[0]) {
			t.Fatalf("first write %x, want Initiate's frame %x", w, made[0])
		}
	})
}

// firstWrite sends a copy of its first Write to out.
type firstWrite struct {
	net.Conn
	out  chan<- []byte
	once sync.Once
}

func (w *firstWrite) Write(b []byte) (int, error) {
	w.once.Do(func() { w.out <- bytes.Clone(b) })
	return w.Conn.Write(b)
}
```

- [ ] **Step 7: Run it to verify it fails**

Run: `go test ./internal/sdk -run TestInitiateMakesTheAttemptsHandshakeFrame`
Expected: FAIL to compile: `cfg.Initiate undefined (type *Config has no field or method Initiate)`.

- [ ] **Step 8: `Config.Initiate`**

In `internal/sdk/client.go`, add to `Config` after `Rand`, the last field, behind a blank line so gofmt keeps `Dial` and `Rand` aligned (and import `github.com/Tallinzp/goim/internal/wire`):

```go

	// Initiate makes an attempt's first handshake frame for the pinned key
	// (nil: wire.Initiate for CMD_HANDSHAKE, after the dial). Loadgen hands
	// over one made ahead of time (#106): the server sees the same bytes,
	// and a phone's own CPU, not loadgen's, would have made them.
	Initiate func(keyID uint16, serverPub []byte) (*wire.Initiator, []byte, error)
```

In `New`, next to the `cfg.Dial == nil` default:

```go
	if cfg.Initiate == nil {
		cfg.Initiate = func(keyID uint16, serverPub []byte) (*wire.Initiator, []byte, error) {
			return wire.Initiate(keyID, serverPub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		}
	}
```

In `internal/sdk/attempt.go`, `login`, replace the line `switch err := a.cc.Handshake(cfg.KeyID, cfg.ServerKey, connectBy); {` with:

```go
	in, frame, err := cfg.Initiate(cfg.KeyID, cfg.ServerKey)
	if err != nil {
		nc.Close()
		return nil, end{kind: EndFailed, err: err}
	}
	switch err := a.cc.HandshakeWith(in, frame, connectBy); {
```

- [ ] **Step 9: Run the tests**

Run: `gofmt -l internal/` (expect no output), then `go test -race -count=1 ./internal/wire ./internal/clientconn ./internal/sdk ./internal/loadgen`
Expected: PASS (`loadgen` uses the SDK unchanged).

- [ ] **Step 10: Commit**

```bash
git add internal/wire/noise.go internal/wire/noise_test.go internal/clientconn/conn.go internal/sdk/client.go internal/sdk/attempt.go internal/sdk/client_test.go
git commit -m "feat(sdk): Config.Initiate hands an attempt a first handshake message made earlier (#106)"
```

---

### Task 2: loadgen `-precompute` and the attempt lag (#106, #107)

**Files:**
- Create: `internal/loadgen/precompute.go`
- Modify: `internal/loadgen/sdkclient.go`, `internal/loadgen/recorder.go`, `internal/loadgen/runner.go`, `cmd/loadgen/main.go`
- Test: `internal/loadgen/recorder_test.go`, `internal/loadgen/runner_test.go`, `internal/loadgen/e2e_test.go`, `cmd/loadgen/main_test.go`

**Interfaces:**
- Consumes: Task 1's `sdk.Config.Initiate`, `wire.Initiate`.
- Produces: `Config.Precompute bool`; `Stats.Precompute *PrecomputeStats` (`/stats`' `precompute` `{pending, ready, stopped}`, absent without `-precompute`); `Counts.StormSinceMark StormCount` (`/stats`' `storm_since_mark` `{mark, attempt_lag, handshakes_precomputed, handshakes_inline}`, `attempt_lag` a `Dist` in ms); `-precompute` flag.

- [ ] **Step 1: Write the failing recorder test**

Append to `internal/loadgen/recorder_test.go`:

```go
// #107: the storm's attempt lag and handshake counts restart at each mark;
// a negative lag (a wall-clock step) counts as 0.
func TestRecorderCountsTheStormSinceMark(t *testing.T) {
	r := NewRecorder(2, []string{"gw-0"})
	r.AttemptLag(5 * time.Second) // the ramp's: before any mark
	r.Handshake(false)
	if _, err := r.Mark("death", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	r.AttemptLag(120 * time.Millisecond)
	r.AttemptLag(-time.Millisecond)
	r.Handshake(true)
	r.Handshake(false)
	r.Handshake(false)
	s := r.Counts().StormSinceMark
	if s.Mark != "death" || s.AttemptLag.N != 2 || s.AttemptLag.Max != 120 ||
		s.HandshakesPrecomputed != 1 || s.HandshakesInline != 2 {
		t.Fatalf("storm since mark %+v", s)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./internal/loadgen -run TestRecorderCountsTheStormSinceMark`
Expected: FAIL to compile: `r.AttemptLag undefined`.

- [ ] **Step 3: The recorder's storm count**

In `internal/loadgen/recorder.go`:

Add to `Recorder`, after `since`:

```go
	lag            *hist      // attempt lag since the latest mark, ms (#107)
	storm          StormCount // since the latest mark; AttemptLag filled by Counts
```

In `NewRecorder`, add `lag: newHist(),` to the literal.

In `Mark`, after `r.since = BackoffCount{Mark: name, Waits: map[string]int64{}}`:

```go
	r.lag, r.storm = newHist(), StormCount{Mark: name}
```

After `Backoff`:

```go
// AttemptLag: an attempt wrote its handshake frame lag after its wait's end
// (#107). A negative lag, from a wall-clock step, counts as 0.
func (r *Recorder) AttemptLag(lag time.Duration) {
	r.mu.Lock()
	r.lag.add(max(lag, 0).Milliseconds())
	r.mu.Unlock()
}

// Handshake: an attempt's first handshake message was made ahead of time
// (precomputed) or when it dialed (#106).
func (r *Recorder) Handshake(precomputed bool) {
	r.mu.Lock()
	if precomputed {
		r.storm.HandshakesPrecomputed++
	} else {
		r.storm.HandshakesInline++
	}
	r.mu.Unlock()
}
```

Add to `Counts`, after `BackoffSinceMark`:

```go
	StormSinceMark   StormCount       `json:"storm_since_mark"`
```

After `BackoffCount`:

```go
// StormCount is the attempts since mark Mark ("": the run's start): how late
// each wrote its handshake frame after its wait's end (AttemptLag, ms; an
// attempt with no wait, such as the first, is not in it), and whether that
// frame was made ahead of time or when it dialed (#106, #107).
type StormCount struct {
	Mark                  string `json:"mark"`
	AttemptLag            Dist   `json:"attempt_lag"`
	HandshakesPrecomputed int64  `json:"handshakes_precomputed"`
	HandshakesInline      int64  `json:"handshakes_inline"`
}
```

In `Counts()`, add `StormSinceMark: r.storm,` to the literal, and before `return c`:

```go
	c.StormSinceMark.AttemptLag = r.lag.dist(r.lag.n)
```

- [ ] **Step 4: Run the recorder tests**

Run: `go test -race ./internal/loadgen -run 'TestRecorder'`
Expected: PASS.

- [ ] **Step 5: Write the failing runner and e2e tests**

Append to `internal/loadgen/runner_test.go`:

```go
// #106: preparing tokens and handshakes is for SDK clients.
func TestNewRefusesPrecomputeInLightMode(t *testing.T) {
	env := Env{GatewayIDs: []string{"gw-0"}, GatewayAddrs: []string{"127.0.0.1:1"}}
	_, err := New(Config{Mode: "light", Clients: 1, Rate: 1, UIDPrefix: "x", PingEvery: time.Second,
		Timeout: time.Second, Precompute: true}, env)
	if err == nil || !strings.Contains(err.Error(), "precompute") {
		t.Fatalf("err %v, want a precompute error", err)
	}
}
```

Add `time` to `runner_test.go`'s imports (it imports only `strings` and `testing`).

Append to `internal/loadgen/e2e_test.go`:

```go
// #106, #107: with Precompute every logged-in client holds its next token
// and handshake frame before the mark; each victim's first attempt after it
// uses its own once, nobody prepares after the mark, and every attempt after
// a wait records its lag.
func TestSDKStormUsesPrecomputedHandshakes(t *testing.T) {
	env, stopGW := startStack(t, 2)
	r, err := New(Config{Mode: "sdk", Clients: 30, Rate: 300, UIDPrefix: "pre", Precompute: true}, env)
	if err != nil {
		t.Fatal(err)
	}
	r.cfg.windows = sdk.DefaultWindows()
	r.cfg.windows.Storm = sdk.Window{Lo: 0, Hi: time.Second}
	stop := run(r)
	defer stop()
	waitFor(t, "30 online and prepared", 10*time.Second, func() bool {
		s := r.Stats()
		return s.RampDone && s.Online == 30 && s.Precompute != nil && s.Precompute.Ready == 30 && s.Precompute.Pending == 0
	})
	n, err := r.Mark(1, "death", true)
	if err != nil || n != 15 {
		t.Fatalf("mark: %d %v", n, err)
	}
	stopGW[1]()
	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("death", time.Second); return s.Back == 15 })
	st := r.Stats()
	s := st.StormSinceMark
	if s.Mark != "death" || s.HandshakesPrecomputed != 15 || s.AttemptLag.N < 15 || s.AttemptLag.Max > 1000 {
		t.Fatalf("storm since mark %+v", s)
	}
	if !st.Precompute.Stopped || st.Precompute.Ready != 15 {
		t.Fatalf("precompute %+v: want stopped, the survivor's 15 still prepared", st.Precompute)
	}
}
```

Add to `TestRunRefusesBadFlags`' table in `cmd/loadgen/main_test.go`:

```go
		{[]string{"-env", "ENV", "-precompute"}, "precompute"},
```

(The default `-mode` is `light`, so `-precompute` alone is refused.)

- [ ] **Step 6: Run them to verify they fail**

Run: `go test ./internal/loadgen ./cmd/loadgen -run 'TestNewRefusesPrecomputeInLightMode|TestSDKStormUsesPrecomputedHandshakes|TestRunRefusesBadFlags'`
Expected: FAIL to compile: `unknown field Precompute in struct literal`.

- [ ] **Step 7: The precomputer**

Create `internal/loadgen/precompute.go`:

```go
package loadgen

import (
	"context"
	"sync/atomic"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
)

// prepared is what a phone already holds when its connection drops: a token
// from its cache and, made by its own CPU, the first handshake message
// (#106). One serves one attempt.
type prepared struct {
	tok   string
	in    *wire.Initiator
	frame []byte
}

// precomputer prepares each logged-in SDK client's next attempt in the
// background until the first mark (#106), so a storm's first attempts do not
// spend loadgen's CPU on what a phone has ready.
type precomputer struct {
	r       *Runner
	q       chan *sdkClient // sized to Clients: each client is queued at most once
	workers int
	stopped atomic.Bool
	pending atomic.Int64 // queued or being prepared
	ready   atomic.Int64 // clients holding a prepared attempt
}

// PrecomputeStats is /stats' precompute (#106).
type PrecomputeStats struct {
	Pending int64 `json:"pending"` // logged-in clients queued or being prepared
	Ready   int64 `json:"ready"`   // clients holding a prepared token and handshake
	Stopped bool  `json:"stopped"` // a mark was taken: no more are prepared
}

func newPrecomputer(r *Runner, clients, workers int) *precomputer {
	return &precomputer{r: r, q: make(chan *sdkClient, clients), workers: max(1, workers)}
}

// enqueue queues a client that just logged in; it runs on the client's run
// loop and never blocks.
func (p *precomputer) enqueue(sc *sdkClient) {
	if p.stopped.Load() || sc.pre.Load() != nil || !sc.queued.CompareAndSwap(false, true) {
		return
	}
	p.pending.Add(1)
	p.q <- sc
}

// run prepares queued clients until ctx ends.
func (p *precomputer) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case sc := <-p.q:
			sc.queued.Store(false)
			if !p.stopped.Load() && sc.pre.Load() == nil {
				if pr, err := p.r.prepare(sc.i); err == nil && sc.pre.CompareAndSwap(nil, pr) {
					p.ready.Add(1)
				}
			}
			p.pending.Add(-1)
		}
	}
}

// take hands client sc's prepared attempt to its Initiate, or nil.
func (p *precomputer) take(sc *sdkClient) *prepared {
	pr := sc.pre.Swap(nil)
	if pr != nil {
		p.ready.Add(-1)
	}
	return pr
}

func (p *precomputer) stats() *PrecomputeStats {
	return &PrecomputeStats{Pending: p.pending.Load(), Ready: p.ready.Load(), Stopped: p.stopped.Load()}
}

// prepare signs client i's next token and makes its first handshake message.
func (r *Runner) prepare(i int) (*prepared, error) {
	tok, err := r.token(i)
	if err != nil {
		return nil, err
	}
	in, frame, err := wire.Initiate(r.env.NoiseKeyID, r.env.NoisePublic, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return nil, err
	}
	return &prepared{tok: tok, in: in, frame: frame}, nil
}
```

- [ ] **Step 8: The SDK client: prepared attempts, `due`, `lagConn`**

Replace `internal/loadgen/sdkclient.go` with:

```go
package loadgen

import (
	"context"
	"net"
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
	gw     atomic.Int32               // gateway of the last dial
	due    atomic.Int64               // unix ns the next attempt is due (its wait's end); 0: no wait (#107)
	pre    atomic.Pointer[prepared]   // the next attempt's token and first message (#106)
	queued atomic.Bool                // waiting for a precompute worker
	start  time.Time                  // the current attempt's start; run loop only
	online bool                       // run loop only
}

func (r *Runner) newSDK(i int) (*sdk.Client, error) {
	sc := &sdkClient{r: r, i: i}
	return sdk.New(sdk.Config{Addrs: []string{"balancer"}, KeyID: r.env.NoiseKeyID, ServerKey: r.env.NoisePublic,
		AppID: AppID, Platform: clientpb.Platform_PLATFORM_MOBILE,
		Token: func(_ context.Context, refresh bool) (string, error) {
			if pr := sc.pre.Load(); pr != nil && !refresh {
				return pr.tok, nil // a phone's cached token (#106)
			}
			return r.token(i)
		},
		Store: &sdk.MemStore{}, OnEvent: sc.event, Windows: r.cfg.windows,
		Dial: func(ctx context.Context, _ string) (net.Conn, error) {
			nc, gw, err := r.bal.Dial(ctx)
			if err != nil {
				return nil, err
			}
			sc.gw.Store(int32(gw))
			return &lagConn{Conn: nc, sc: sc}, nil
		},
		Initiate: func(keyID uint16, pub []byte) (*wire.Initiator, []byte, error) {
			if r.pre != nil {
				if pr := r.pre.take(sc); pr != nil {
					r.rec.Handshake(true)
					return pr.in, pr.frame, nil
				}
			}
			r.rec.Handshake(false)
			return wire.Initiate(keyID, pub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
		}})
}

// event runs on the client's run loop: it must not block.
func (sc *sdkClient) event(e sdk.Event) {
	now := time.Now()
	if e.Kind == sdk.EventWaiting && (e.Wait.Kind == sdk.WaitBackoff || e.Wait.Kind == sdk.WaitFailure) {
		sc.r.rec.Backoff(sc.i, e.Wait.Kind.String())
	}
	if e.Kind == sdk.EventWaiting {
		sc.due.Store(now.Add(e.Wait.D).UnixNano())
	}
	switch e.Kind {
	case sdk.EventConnecting:
		sc.start = now
	case sdk.EventOnline:
		sc.online = true
		sc.r.rec.Online(sc.i, int(sc.gw.Load()), sc.start, now)
		if sc.r.pre != nil {
			sc.r.pre.enqueue(sc)
		}
	case sdk.EventWaiting, sdk.EventIdle, sdk.EventLoginRequired:
		switch {
		case sc.online:
			sc.online = false
			sc.r.rec.Lost(sc.i, e.End.String(), now)
		case e.End != 0:
			sc.r.rec.Failed(e.End.String())
		}
	}
}

// lagConn records, at an attempt's first write (its handshake frame), how
// late the attempt is against its wait's end (#107).
type lagConn struct {
	net.Conn
	sc    *sdkClient
	wrote atomic.Bool
}

func (c *lagConn) Write(b []byte) (int, error) {
	if c.wrote.CompareAndSwap(false, true) {
		if due := c.sc.due.Swap(0); due != 0 {
			c.sc.r.rec.AttemptLag(time.Duration(time.Now().UnixNano() - due))
		}
	}
	return c.Conn.Write(b)
}
```

- [ ] **Step 9: The runner**

In `internal/loadgen/runner.go`:

Add to `Config` after `Down`:

```go
	Precompute bool         // sdk: prepare each logged-in client's next token and handshake until the first mark (#106)
```

Add to `Stats` after `ElapsedS`:

```go
	Precompute *PrecomputeStats `json:"precompute,omitempty"`
```

Add to `Runner` after `pinger`:

```go
	pre      *precomputer // nil without Precompute
```

In `New`, add a case to the first `switch`:

```go
	case cfg.Precompute && cfg.Mode != "sdk":
		return nil, errors.New("loadgen: precompute needs sdk mode")
```

and after `r := &Runner{...}`:

```go
	if cfg.Precompute {
		r.pre = newPrecomputer(r, cfg.Clients, runtime.GOMAXPROCS(0)/2)
	}
```

In `Run`, after the pinger's `wg.Go`:

```go
	if r.pre != nil {
		for range r.pre.workers {
			wg.Go(func() { r.pre.run(ctx) })
		}
	}
```

In `Mark`, as its first statement:

```go
	if r.pre != nil {
		r.pre.stopped.Store(true) // the storm's CPU is the storm's (#106)
	}
```

In `Stats`, before `return s`:

```go
	if r.pre != nil {
		s.Precompute = r.pre.stats()
	}
```

In `cmd/loadgen/main.go`, after the `-down` flag:

```go
	fs.BoolVar(&cfg.Precompute, "precompute", false, "sdk: prepare each logged-in client's next token and handshake until the first mark (#106)")
```

- [ ] **Step 10: Run the tests**

Run: `gofmt -w internal/loadgen cmd/loadgen && go vet ./internal/loadgen ./cmd/loadgen && go test -race -count=1 ./internal/loadgen ./cmd/loadgen` (gofmt realigns the struct fields this task adds)
Expected: PASS, including `TestSDKStormAfterAGatewayStops` unchanged (no `-precompute`: `precompute` absent, every handshake inline).

- [ ] **Step 11: Full suite and the load probe**

Run: `make test && make test-load-probe`
Expected: PASS.

- [ ] **Step 12: Commit**

```bash
git add internal/loadgen/precompute.go internal/loadgen/sdkclient.go internal/loadgen/recorder.go internal/loadgen/runner.go \
  internal/loadgen/recorder_test.go internal/loadgen/runner_test.go internal/loadgen/e2e_test.go cmd/loadgen/main.go cmd/loadgen/main_test.go
git commit -m "feat(loadgen): -precompute prepares what a phone holds before the storm; the storm's attempt lag (#106, #107)"
```

---

### Task 3: `gwloss`: b_stress's CPU sets, the wait before the kill, `<phase>_storm_real` (#107, #108)

**Files:**
- Modify: `scripts/tier2/gwloss.sh`
- Modify: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`

**Interfaces:**
- Consumes: Task 2's `-precompute`, `/stats`' `precompute` and `storm_since_mark`.
- Produces: checks `<phase>_storm_real` and `<phase>_pinning` (replaces `pinning`); the phase field's `precompute_at_kill` and `storm`; knobs `GWLOSS_LAG_P99_MS`, `GWLOSS_STRESS_CPUS_LOADGEN`, `GWLOSS_STRESS_CPUS_GATEWAY`; 28 checks in all.

- [ ] **Step 1: Per-phase CPU sets**

Before the phase loop (`for name in $PHASES; do`), add:

```bash
# b1 keeps #71's CPU sets; b_stress gives loadgen the CPUs its storm left
# idle (#108).
cpus_gateway=$T2_CPUS_GATEWAY cpus_loadgen=$T2_CPUS_LOADGEN
lag_max=${GWLOSS_LAG_P99_MS:-100}
```

Delete the global `LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)` line (~136).

In the `b1)` case, add after `export STACK_GATEWAYS=2`:

```bash
      export T2_CPUS_GATEWAY=$cpus_gateway T2_CPUS_LOADGEN=$cpus_loadgen
      unset STACK_GATEWAY_PROCS
```

In the `b_stress)` case, add after `export STACK_GATEWAYS=3`:

```bash
      # #108: loadgen 12 CPUs; the gateways 8, at 4 GOMAXPROCS each as before
      # (the victim alone in the ramp, the two survivors after the kill).
      export T2_CPUS_GATEWAY=${GWLOSS_STRESS_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${GWLOSS_STRESS_CPUS_LOADGEN:-0-5,12-17}
      export STACK_GATEWAY_PROCS=4
```

After the `esac` that closes the phase `case`, add:

```bash
  LG_CPUS=$(t2_cpulist_expand "$T2_CPUS_LOADGEN" | wc -w)
```

- [ ] **Step 2: `-precompute`, and pinning in every phase**

Change the `lg_start` line to pass `-precompute`. `GWLOSS_DRY_NO_PRECOMPUTE=1` turns it off again for the dry run's negative case only; it goes last because Go's `flag` package keeps the last value:

```bash
  lg_start "$name" -mode sdk -precompute -clients "$clients" -rate "$rate" -uid-prefix "$name" ${sources:+-sources "$sources"} ${down:+-down "$down"} ${GWLOSS_DRY_NO_PRECOMPUTE:+-precompute=false}
```

Replace the pinning block (`if [ -n "$first" ]; then` … `fi`, ~lines 276–284) with the following, and delete `first=1` (~line 208), which nothing else reads:

```bash
  wrong=""
  for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
  for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
  have=$(taskset -pc "$LG_PID" 2>/dev/null)
  [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
  t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
```

`usage_ok` (~line 198) reads `LG_CPUS` when called, so the per-phase value from Step 1 reaches `<phase>_loadgen_headroom` and `loadgen_bound`.

- [ ] **Step 3: Wait for the precompute before the kill**

Just before `stack_snapshot before` (the storm's start), add the wait below. Without `-precompute` (the dry run's negative case) `/stats` has no `precompute`; the reads use `.get` so they print `null` instead of failing, the wait gives up after 120s, and Step 4's check fails on `ready` null.

```bash
  # #107: every logged-in client holds its next token and handshake frame at
  # the kill, so the storm's first attempts are the spec's, not loadgen's.
  # Wall clock (#103).
  t=$SECONDS
  until [ "$(lg_get /stats '(d.get("precompute") or {}).get("pending") == 0 and (d.get("precompute") or {}).get("ready") == d["online"]')" = true ]; do
    [ $((SECONDS - t)) -lt 120 ] || break
    sleep 1
  done
  precompute=$(lg_get /stats '{"pending": (d.get("precompute") or {}).get("pending"), "ready": (d.get("precompute") or {}).get("ready"), "online": d["online"], "wait_s": '"$((SECONDS - t))"'}')
```

- [ ] **Step 4: `<phase>_storm_real`**

After the `<phase>_cross_check` check, add:

```bash
  # #107: the storm was the spec's: every logged-in client was prepared at
  # the kill, and the attempts after it wrote their handshake frames within
  # GWLOSS_LAG_P99_MS of their waits' ends. b-stress has no SLO; its relogin
  # distribution is the measurement only when this passes.
  storm_d=$(lg_get /stats 'd["storm_since_mark"]')
  t2_check "${name}_storm_real" "$(echo "{\"s\": $storm_d, \"p\": $precompute, \"pop\": $pop}" | lg_true - 'd["p"]["ready"] is not None and d["p"]["ready"] == d["p"]["online"] and d["s"]["attempt_lag"]["n"] >= d["pop"] and d["s"]["attempt_lag"]["p99_ms"] <= '"$lag_max")" \
    "\"lag_p99_max_ms\": $lag_max, \"precompute_at_kill\": $precompute, \"storm\": $storm_d"
```

In the phase field (`t2_field "$name" "{…}"`), add after `\"loadgen_bound\": $loadgen_bound,`:

```bash
\"precompute_at_kill\": $precompute, \"storm_since_mark\": $storm_d,
```

- [ ] **Step 5: The header**

In the header's knobs, add:

```bash
#   GWLOSS_LAG_P99_MS  the storm's attempt lag p99 bound, ms (default 100, #107)
#   GWLOSS_STRESS_CPUS_LOADGEN  b_stress: loadgen's CPU set (default 0-5,12-17, #108)
#   GWLOSS_STRESS_CPUS_GATEWAY  b_stress: the gateways' CPU set (default 6-9,18-21,
#                      4 GOMAXPROCS each, #108)
```

Replace the `pinning` line of the checks with:

```bash
#   b1_pinning         gateways, Logic + Redis and loadgen on their phase's CPU
#                      sets (b_stress's differ, #108)
```

In `b_stress_back`'s description, replace the sentence from "Its relogin times are loadgen-bound" to the end of the entry with:

```bash
#                      Its relogin times are the measurement when
#                      b_stress_storm_real passes (#107)
```

Add after `b1_cross_check`'s entry:

```bash
#   b1_storm_real      the storm was the spec's, not loadgen's (#106, #107):
#                      every logged-in client held its next token and
#                      handshake frame at the kill (loadgen -precompute), and
#                      the attempts after the kill wrote their handshake
#                      frames within GWLOSS_LAG_P99_MS of their waits' ends
#                      (p99), at least one attempt per relogin
```

In `b1_loadgen_headroom`'s entry, replace "and records the first 30s as loadgen_bound" with "and records the first 30s as loadgen_bound (data; b_stress_storm_real decides, #107)".

- [ ] **Step 6: Script tests and dry run**

Run: `make test-scripts`
Expected: PASS (`bash -n` and shellcheck, if the target runs it, accept the script).

Dry run on the dev machine as in 6b2a-stress Task 4 Step 2 (the `D=$(mktemp -d)` copy with the guard lowered), with b_stress's CPU sets folded onto the dev machine's two CPUs:

```bash
GOIM_TEST_TIER=2 T2_CPUS_GATEWAY=1 T2_CPUS_LOGIC=0 T2_CPUS_LOADGEN=0-1 \
  GWLOSS_STRESS_CPUS_GATEWAY=1 GWLOSS_STRESS_CPUS_LOADGEN=0-1 \
  GWLOSS_CLIENTS=2000 GWLOSS_RATE=500 GWLOSS_CAP=5000 \
  GWLOSS_STRESS_CLIENTS=1500 GWLOSS_STRESS_RATE=500 GWLOSS_STRESS_CAP=5000 bash "$D/gwloss.sh"; echo "exit $?"
```

Expected: 28 checks; all pass except possibly the two swap checks (the dev machine). `b1_storm_real` and `b_stress_storm_real` show `precompute_at_kill.ready` = `online` (2000, 1500), `storm.handshakes_precomputed` = the population (1000, 1500), `attempt_lag.n` ≥ the population, p99 well under 100ms. `b_stress_pinning` shows `gateway_procs` 4.

Then the negative case, b1 only: `GWLOSS_PHASES=b1 GWLOSS_DRY_NO_PRECOMPUTE=1 … bash "$D/gwloss.sh"`. Expected: `b1_storm_real` fails with `precompute_at_kill.ready` null after `wait_s` ≈ 120. Remove the dry runs' `tier2-results/gwloss-*` and `$D`.

- [ ] **Step 7: Roadmap and commit**

Add #106–#108 to the roadmap's choices (as written in this plan's *Implementation choices*, marked "proposed, for the user's review" until decided), and set the plan 6 row's *next* to "execute 6b2a-storm, then Tier 2 run 25".

```bash
git add scripts/tier2/gwloss.sh docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-14-connection-session-core-06b2a-storm-real.md
git commit -m "feat(tier2): gwloss storms are the spec's: -precompute, b_stress CPU sets, <phase>_storm_real (#106-#108)"
git push
```

- [ ] **Step 8: Tier 2 run 25 (the user, on the host)**

Host prep as for run 24 (services stopped, `vm.swappiness=1`, swap emptied, desktop off), then `GOIM_TEST_TIER=2 make test-tier2 SCENARIO=gwloss`. Before it, the user runs `lscpu -e=CPU,CORE,MAXMHZ` once, so the plan's CPU sets can be checked against the host's fast and compact cores (#108). Expected: 28/28. What to read: `b_stress_storm_real` (`attempt_lag` p99, `handshakes_precomputed` vs `handshakes_inline`), `b_stress_back`'s `back_s` and relogin distribution, the survivors' `handshake_rate`, `handshake_timeout` and `pending_mem` rejects (the herd the survivors' guards now meet), loadgen's `storm_first30` cores on 12 CPUs, `no_swap_in_b_stress`. If `b_stress_storm_real` fails, the host cannot make the storm: the next step is a second machine for loadgen (the user's decision, not a throttle).

## After this plan

6b2a-stress is complete when run 25 passes 28/28; then the README's *Measured results* (both languages) get b-stress's real numbers, and `main` is fast-forwarded. 6b2b (b2 and slowed Logic) follows in a fresh session.

## Execution Notes
