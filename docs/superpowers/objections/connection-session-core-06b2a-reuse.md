---
spec: docs/superpowers/plans/2026-10-14-connection-session-core-06b2a-reuse.md
date: 2026-10-14
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: premise
    severity: high
    claim: "#109 is gated on run 26's send_lag (P-sendlagI-2), and run 26 has not run; the reuse's premise (refused attempts are what saturates loadgen) is still the open question."
    evidence: "Execution Notes, P-sendlagI-2: 'if b_stress's send_lag p99 is ≥ 1s ... #109 goes ahead ... If send_lag p99 is small ... look at the survivors' side first ... then decide #109.' Repo head d4c5b3c records no run 26."
    disposition: pending
    disposition_rationale: null
  - id: O2
    category: specification quality
    severity: high
    claim: "The plan's Tasks 1–3 predate 8e85cfd, which already applied Task 1, send_lag and most of Task 3; executed as written, Task 2 contradicts the decided P-sendlagI-1 and its expected failures will not happen."
    evidence: "Task 2 Step 2: 'Expected: FAIL to compile: r.SendLag undefined'; Step 3 adds `send *hist // ... (#109)`; Step 4 'Replace internal/loadgen/sdkclient.go with' a file citing send_lag as #109; Task 3 Step 4 'Tier 2 run 26 ... Expected: 29/29'. Execution Notes: 'send_lag is cited as #107 ..., not #109'; P-sendlagI-2 makes run 26 the run without #109."
    disposition: pending
    disposition_rationale: null
  - id: O3
    category: implementation
    severity: medium
    claim: "A cancelled attempt's late first Write can take the held handshake back from the next attempt, whose real reply is then refused with errHandshakeTaken and classified as EndFailed (backoff), not as success."
    evidence: "`func (h *handshake) send(c *replyConn) bool { ... if h.answered { return false }; h.owner = c ...}` (no check that c is closed); `reply`: 'if h.owner != c || c.closed.Load() { return false }'; Review Focus 1: 'the cancelled attempt can still be running when the next one calls Initiate'."
    disposition: pending
    disposition_rationale: null
  - id: O4
    category: risk
    severity: high
    claim: "No test runs the reuse through the real SDK and clientconn, and the only link from a write to the held handshake is a backing-array pointer match; if it misses, reuse silently never happens and the first evidence is a failed host run."
    evidence: "`&b[0] == &h.pr.frame[0]` in replyConn.Write; the #109 tests call `sc.track` and `wire.ClientHandshakeWith` directly; Task 3 Step 2: 'handshakes_reused (likely 0 at this scale: nothing is refused)'."
    disposition: pending
    disposition_rationale: null
  - id: O5
    category: specification quality
    severity: low
    claim: "The reason given for never reusing an answered handshake ('the session keys would repeat') is wrong for Noise NK; the real reason is that the Initiator's HandshakeState is used up by Finish."
    evidence: "#109: 'reusing an answered handshake (an Initiator serves one handshake: Finish would fail, and the session keys would repeat)'; internal/wire/noise.go:166 uses `noise.HandshakeNK`, whose reply carries the responder's new ephemeral key (e, ee)."
    disposition: pending
    disposition_rationale: null
---

# Objections: Plan 6b2a-reuse (#109)

Scope of this review: only the parts of the plan not yet executed. That means #109's reuse (Task 2, minus `send_lag`) and Task 3's `handshakes_reused`. Task 1 and `send_lag` shipped in 8e85cfd and were covered by the P-sendlagI review. I checked the code at d4c5b3c: `internal/sdk/attempt.go`, `internal/clientconn/conn.go`, `internal/wire/noise.go`, `internal/loadgen/sdkclient.go` and `recorder.go`.

## O1 — premise — high

### Claim

#109 rests on one premise: the refused attempts' inline X25519 is what saturates loadgen, so the survivors' refusals cost loadgen and not the server. The user decided that run 26's `send_lag` will test this premise before #109 goes ahead. Run 26 has not run yet.

### Evidence

> P-sendlagI-2 (decided): … if b_stress's `send_lag` p99 is ≥ 1s, the survivors' `handshake_timeout` rejects are late frames from a saturated loadgen, and #109 goes ahead … If `send_lag` p99 is small, frames left loadgen on time and were not handled within 5s: look at the survivors' side first … then decide #109.

The plan's own figures show why the order matters. The survivors closed 1.8M `handshake_timeout` connections each (Architecture §2). Run 25 made 6.02M attempts for 600k clients. If late frames caused those timeouts, then loadgen's lateness creates retries, the retries make more inline handshakes, and the loop feeds itself. #109 breaks that loop, and the attempt count should fall as well. If the survivors caused them, #109 removes CPU but leaves the attempt count unchanged. Then "inline falls to ~655k" still holds, but `attempt_lag` may not recover.

### Why this matters

If #109 is approved or executed now, the user's gate is skipped. Run 26's result would then arrive with #109 already in, and it could no longer tell the two causes apart. My recommendation is to finish this review, decide the objections, and keep execution parked until run 26.

## O2 — specification quality — high

### Claim

The plan text was written before 8e85cfd and before P-sendlagI-1..4 were decided. An executor following Tasks 2–3 literally would:
- redo changes that already exist: the `send` hist, `SendLag`, `StormCount.SendLag`, the `Mark` reset, the storm-check loop and the test edits;
- get no compile failure in Task 2 Step 2, because `r.SendLag` already exists, so TDD's red step is wrong;
- replace `sdkclient.go` wholesale with a version that cites `send_lag` as #109, which contradicts the decided P-sendlagI-1 (send_lag is an amendment of #107);
- expect "run 26: 29/29" in Task 3 Step 4. Under P-sendlagI-2, run 26 is the run *without* #109, so the first run with #109 is run 27.

### Evidence

Task 2 Step 2: "FAIL to compile: `r.SendLag undefined`". Step 3 item 1: `send *hist // send lag since the latest mark, ms (#109)` (current `recorder.go:29` already has it as `(#107, run 25)`). Task 3 Step 1's `for k in ("attempt_lag", "send_lag", "reply_lag")` is already at `gwloss.sh:426`. The Execution Notes themselves say that `Handshake(bool)` stayed and that send_lag is #107.

### Why this matters

Two executors could reasonably produce different results from this text. One applies the whole-file replacement and silently re-cites send_lag. The other merges by hand. The fix is to rewrite Tasks 2–3 as the remaining delta against 8e85cfd: `Handshake(how)` and its constants, `HandshakesReused`, `held`/`track`/`initiate`/`handshake`, and the guard in `replyConn`. That delta needs a correct red step and the run numbers from P-sendlagI-2.

## O3 — implementation — medium

### Claim

`handshake.send` lets any connection become the owner as long as the handshake is unanswered, even a connection that is already closed. Here is the interleaving that breaks:

1. Attempt A's `initiate` reuses h, whose owner X is closed.
2. The SDK cancels A. `AfterFunc` closes A's connection before A writes.
3. Attempt B's `initiate` also finds h reusable, since its owner X is closed. B writes, and `send(B)` sets owner = B.
4. A's goroutine, still running (Review Focus 1), reaches its first `Write`. `send(A)` sets owner = A, then `A.Conn.Write` fails on the closed connection.
5. The gateway answers B. B's `Read` calls `reply(B)`, which sees owner ≠ B, so B gets `errHandshakeTaken`.

In step 5, `ClientHandshakeWith` returns that error with n = 0. It is neither EOF nor ECONNRESET, so `attempt.login` classifies it as `EndFailed` (`internal/sdk/attempt.go:88`). The client then takes the 2.5–5s doubling backoff instead of going online. Meanwhile the survivor has already spent its DH on B.

### Evidence

> `func (h *handshake) send(c *replyConn) bool { … if h.answered { return false }; h.owner = c; return true }`

> Review Focus 1: "the cancelled attempt can still be running when the next one calls `Initiate`."

None of the three `TestAHeldHandshakeIsFinishedOnce` cases writes on a closed sender after a newer sender has written.

### Why this matters

The window is narrow, but a saturated loadgen running 600k goroutines is exactly where narrow windows open. When it hits, b-stress's relogin distribution gains spurious failures and backoffs, which is the measurement this plan exists to make valid. The guard is one condition: `send` refuses a connection that is already closed (`c.closed.Load()`). With it, A's late write fails as A's own failure and ownership stays with B. A fourth test case would pin this.

## O4 — risk — high

### Claim

Whether reuse ever happens depends on `replyConn.Write` recognizing the frame by identity, `&b[0] == &h.pr.frame[0]`. That holds today, because `clientconn.HandshakeWith` passes `c.nc`, the `replyConn` itself, to `wire.ClientHandshakeWith`, which writes `frame` in one call. Nothing tests that path, though:
- The #109 unit tests bypass the SDK and `clientconn` and call `sc.track` and `ClientHandshakeWith` directly.
- The e2e storm test has no refusals.
- The dry run expects `handshakes_reused` to be 0.

If anything in the chain copies or splits the frame, owner stays nil, `reusable()` is always false, and loadgen quietly goes back to inline X25519. Examples are a buffered writer, header and body written separately, or a future clientconn change. Nothing would fail until a host run came back loadgen-bound again.

### Evidence

Task 2 Step 4's `replyConn.Write`; the `TestUnansweredHandshakeIsSentAgain` setup (`sc.track(c1)` → `wire.ClientHandshakeWith`); Task 3 Step 2: "`handshakes_reused` (likely 0 at this scale: nothing is refused)".

### Why this matters

A host run costs hours and host prep. One loadgen e2e test would turn a silent miss into a red test before that run. The test needs a gateway or listener that refuses the first frames (reads, then closes) and must assert, through the real SDK, that `handshakes_reused > 0`, `handshakes_inline` equals the answered count, and every client ends online. The alternative is to state the coupling explicitly as a carried assumption in #109.

## O5 — specification quality — low

### Claim

The rejected-alternatives text in #109 gives a wrong reason for never reusing an answered handshake: "the session keys would repeat". The wire uses Noise NK (`internal/wire/noise.go:166`). In NK the server's reply brings a new responder ephemeral key (`e, ee`), so a second answer to the same first message would produce different keys. The real reason, also stated in that text, is that `Finish` consumes the Initiator's `HandshakeState`.

### Evidence

> "reusing an answered handshake (an Initiator serves one handshake: `Finish` would fail, and the session keys would repeat)"

### Why this matters

The conclusion is right, but the recorded reason is wrong. A later reader might rely on that reason, for example when deciding whether resending a first message is safe if the server *did* answer but the reply was lost. That case is safe for the keys and unsafe only because of the Initiator. The fix is to drop the clause.

## Explicitly not objecting to

- **Fidelity: identical first-message bytes on a client's refused connections.** The plan names this difference. The gateway keeps no replay state (`WriteMessage(frame, nil)`, no timestamp in the payload, the prologue is only header and key id), and a refused frame costs the server the same, so the storm the survivors see is unchanged.
- **The X25519 cost model ("two X25519" per refused attempt).** NK's first message is `e, es`: one key generation and one DH, which matches the profile's 47%.
- **Memory of held Initiators across 600k offline clients.** Each one replaces a `prepared` of the same size that #106 already holds per client, so the 24GB budget from #105 covers it.
- **Partial replies count as answered.** Treating a short reply as answered (a new handshake next time) costs one inline X25519 in a rare case and can never finish an Initiator twice.
- **The other locking in `handshake`.** `closed` only ever goes from false to true, and both `reusable` and `reply` read it under `h.mu`, so a reply and a reuse cannot both succeed. I checked both orders.
- **Rejecting a throttle or a refilling pool.** Both follow from the user's 2026-10-14 decision (a real storm, a second machine as the fallback), and nothing in the code argues against them.
- **The fallback to a second loadgen machine.** That is the user's decision, and it is out of this plan's scope.
