---
spec: docs/superpowers/plans/2026-10-15-connection-session-core-06b2a-pinger.md
date: 2026-10-15
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: specification quality
    severity: high
    claim: "TestPingerStopsForGood/during_a_firing hangs instead of passing or failing: synctest.Wait never returns while a goroutine is blocked on a sync.Mutex."
    evidence: "go func() { p.stop(); close(stopped) }()\nsynctest.Wait()  (Task 1 Step 1), with stop taking p.mu while the firing holds it (Step 3)"
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: implementation
    severity: high
    claim: "Making stop wait for a firing adds a hang the old code could not have (serve blocks until a Send returns), it rests on a defer order no test pins, and the exactness it buys is not needed."
    evidence: "A firing blocked in Send holds mu, so stop waits for it. serve closes the connection before it stops the pinger, so that Send fails at once. / Mutation 2: Expected: no unit test fails; review by reading only."
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: premise
    severity: medium
    claim: "The plan predicts the saving from an average stack size, does not measure it before the Tier 2 run, and has no branch for the case where loadgen is still over its limit."
    evidence: "loadgen goroutines at the end (1.80M before; expect ~1.2M) and stacks (9.83GB; expect ~6.5GB) / After this plan: If b_stress passes apart from b_stress_storm_real's first-30s bound … If it passes 15/15 …"
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: specification quality
    severity: medium
    claim: "The gate ('under its limit with GC calm and no swap') has no numbers, and its swap part has failed in every 16GiB b_stress run so far, for reasons this plan does not touch."
    evidence: "This plan's gate is loadgen staying under its limit with GC calm and no swap. (Task 2 Step 3)"
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: specification quality
    severity: low
    claim: "Mutation 1's stated consequence is wrong: with stop unlocked, the re-armed firing reports a timeout at 40s, not a second Ping at 60s."
    evidence: "the firing then re-arms the timer, so a second Ping at 60s follows. (Task 1 Step 5, mutation 1)"
    disposition: pending
    disposition_rationale: null
---

## O1 — specification quality — high

### Claim

The `during a firing` subtest cannot run as written. The firing holds `p.mu` while it waits on `<-release`. The test then starts a goroutine that calls `p.stop()`, which blocks on `p.mu`, and calls `synctest.Wait()`. In a synctest bubble, a goroutine blocked on `sync.Mutex` is not *durably blocked*, so `Wait` never returns. The bubble's fake clock cannot advance either. The test hangs until `go test`'s timeout. It does not fail with a message.

I checked this on the dev machine's Go (go1.27.1) with a minimal bubble: goroutine A holds a mutex and waits on a channel, goroutine B blocks in `Lock`, and the test calls `synctest.Wait()`. It hung and was killed by `-timeout 20s`, with B's stack showing `[sync.Mutex.Lock, synctest bubble 1]`.

### Evidence

> ```go
> <-inSend
> stopped := make(chan struct{})
> go func() { p.stop(); close(stopped) }()
> synctest.Wait()
> ```

and

> **One mutex held for the whole firing.** … A firing blocked in `Send` holds `mu`, so `stop` waits for it.

### Why this matters

Review Focus 1 says this subtest pins the race between a firing and `stop`, and mutation 1 depends on it. Run as written, Step 4 (`go test -race -count=3 ./internal/sdk/`) hangs, and the executor has to redesign the test or the lock on the spot. The same property affects production code under synctest: any SDK test where `serve`'s `stop` waits on a firing that is blocked in a fake `Send` would freeze the bubble's clock. Possible fixes: a 1-buffered channel as the lock (channel operations are durably blocking), a white-box `p.mu.TryLock()` check instead of `Wait` plus `select`, or dropping the wait altogether (O2).

## O2 — implementation — high

### Claim

The old `stopPing()` cancelled a context and returned at once. It never waited for the ping goroutine. The new `stop` waits for any firing in progress, so `serve` cannot return until that firing's `Send` returns. That hang is new. The plan's guard is the order of two `defer` lines, and its own mutation 2 says no test notices if they are swapped. The guarantee that costs this, "once `stop` returns, no firing is running or will run", is not something anything depends on:
- After `serve`'s `Close`, a late `Send` fails harmlessly.
- A late `timeout()` sets `serve`'s local `pongTimedOut` after `serve` has returned and closes a socket that is already closed.
- The only real requirement is that the timer is not re-armed after `stop`. A `stopped` check before `Reset` gives that without `stop` waiting.

### Evidence

> A firing blocked in `Send` holds `mu`, so `stop` waits for it. `serve` closes the connection before it stops the pinger, so that `Send` fails at once.

> 2. In `serve`, swap the two defers … Expected: no unit test fails; review by reading only. Record in the *Execution Notes* that Review Focus 3 rests on the defer order and its comment.

The current code (`internal/sdk/attempt.go:171–174`) cancels and does not wait: `pctx, stopPing := context.WithCancel(ctx); defer stopPing(); go a.ping(...)`.

### Why this matters

The failure is a client stuck in `serve` for good: it never reconnects and never reports an end. It shows up only when a `Send` blocks, which is the full-send-buffer case b_stress produces, and nothing would catch it if a later edit reorders the defers. Either remove the reason to wait (hold `mu` only around state; release it around `send`/`timeout`; `stop` sets `stopped` and calls `t.Stop()`; `fire` checks `stopped` under `mu` before `Reset`), or pin Review Focus 3 with a test whose fake `Send` blocks until `Close`. The first option also fixes O1.

## O3 — premise — medium

### Claim

The expected saving (~600k goroutines, ~3.3GB of stacks) assumes the ping goroutine's stack is the average size, 5.5KB (9.83GB / 1.80M). Stacks come in power-of-two sizes. The reader (`serve` → `Recv` → decrypt) and the pinger (`Send` → seal) may not be the same size. If the pinger's stack is the smaller one, less than a third of the stack memory goes away. The 19GiB run's mapped peak was 20.2GB. Even with GC slack in that figure, the margin under 16GiB (17.18GB) is a few hundred MB to a few GB, not a comfortable gap. The plan checks this only on the Tier 2 host, and *After this plan* only covers the pass cases.

### Evidence

> loadgen goroutines at the end (1.80M before; expect ~1.2M) and stacks (9.83GB; expect ~6.5GB)

> If b_stress passes apart from `b_stress_storm_real`'s first-30s bound, decide that bound with the user … If it passes 15/15, run the full `gwloss`

Reuse plan, 19GiB run: "mapped peaked at 20.2GB (stacks 9.6GB, live heap 3.8GB)".

### Why this matters

A Tier 2 b_stress run takes host prep plus about 10–15 minutes. Finding a shortfall there, with no decision ready, means another stop to diagnose and decide. A cheap prediction is available: in the Step 1 dry run, record loadgen's `/runtime` stack bytes and goroutines per online SDK client before and after Task 1. That gives the measured saving per client, which can be scaled to 600k before the Tier 2 run. The plan should also name the next lever if 16GiB is still exceeded. The remaining per-client goroutine besides `serve` is `Client.run`'s select loop (`internal/sdk/client.go:283`). Other options are a smaller population, or recording b_stress as loadgen-bound.

## O4 — specification quality — medium

### Claim

The gate has three parts, and none of them has a number. For "Under its limit": every sample, or the end only? Mapped or RSS? For "GC calm": the limiter never engaged? A cycle count? For "No swap": `no_swap_in_b_stress` fails on a single page swapped in (#70). It has failed in every 16GiB b_stress run recorded in the reuse plan: 1, 272, 12, 53 and 4 pages. Those were mostly too small to matter, and their cause is not loadgen's stacks.

### Evidence

> This plan's gate is loadgen staying under its limit with GC calm and no swap.

Reuse plan *Execution Notes*: run 26 "`no_swap_in_b_stress` (1 page in)", diagnostic "272 pages in", `connect_lag` run "12 pages in", run 27 "53 pages in", `517cfba` "4 pages in".

### Why this matters

If the run has, say, 30 pages swapped in and the limiter engages in 3 cycles, two readers could reach opposite verdicts on whether the plan succeeded. That decides whether *After this plan* runs the full `gwloss` or goes back to the user. Possible fixes: a numeric gate (mapped ≤ `GOMEMLIMIT` in every `/runtime` sample after the victims are back; the limiter engaged in no cycle after t≈60s; pages swapped in below a stated bound). There is also a related problem with the Global Constraint "Results must hold without the host's UMA change". If the UMA change is made before Step 3, the swap result cannot show that.

## O5 — specification quality — low

### Claim

In `during a firing`, `lastRecv` is never stored. With `stop`'s lock removed, the released firing records `unanswered = 30s` and re-arms at `min(60s, 40s) = 40s`, and that firing reports a timeout. The test still fails first on "stop returned while a firing was in Send". But the explanation is wrong, and an executor checking the full output against it will be confused. The race detector will most likely also report the unlocked write to `stopped`.

### Evidence

> the firing then re-arms the timer, so a second Ping at 60s follows.

### Why this matters

This is minor. Mutation steps are recorded in the *Execution Notes* as checked "as stated", so the wrong statement would be copied there.

## Explicitly not objecting to

- **The premise of fewer goroutines rather than more memory**: the 19GiB run showed both that memory is the cause and that this host cannot give more (swap), and the user decided on option 2.
- **`fire`'s scheduling logic**: I traced it against `attempt.ping` (lines 205–241). Same state, same order of checks, same `min(next, unanswered+PongTimeout)` re-arm. Both firing tests' expected values (Ping at 30s, timeout at 40s; 20 Pings, the last at 600s) follow from it.
- **`TestAnOnlineClientHoldsOneAttemptGoroutine` matching `(*attempt).`**: the only goroutines an attempt starts are the ping goroutine, `token`'s helper (a plain function, gone once online) and `login`'s `context.AfterFunc` (only on cancel). So the count is 2 before and 1 after, as Step 2 says.
- **`Close` inside `timeout()` under `mu`**: `clientconn.Conn.Close` is `nc.Close()` and takes no lock (`conn.go:136`). Loadgen's `replyConn` embeds `net.Conn` and does not override `Close`, so it cannot deadlock against `Send`'s `wmu`.
- **Dropping the context watch**: `login`'s `context.AfterFunc(ctx, nc.Close)` (`attempt.go:77`) stays registered through `serve`, so a cancel still ends `serve` through `Recv`.
- **Transient callback goroutines**: about 40k firings/s at 600k clients (Ping and the Pong check) is the same wake-up rate as the old loop. They live microseconds, so their stacks do not add up.
- **The `b_stress_storm_real` first-30s bound left out of scope**: the plan says this outright and defers it to the user, which is correct for a memory plan.
