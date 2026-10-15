# Dispositions: Plan 6b2a-reuse (#109) objections

Objection record: `connection-session-core-06b2a-reuse.md`. Decided by the user one at a time.

## O1 — premise — high: #109 waits for run 26

**Decision (2026-10-14): accepted.** #109's reuse is not executed before Tier 2 run 26. Run 26's `send_lag` p99 decides it, as P-sendlagI-2 says: if it is ≥ 1s, #109 goes ahead (then run 27); if it is small, look at the survivors' side first.

**Plan changes to apply:** a note at the top of *Implementation choices* (#109) that execution is gated on run 26 (P-sendlagI-2, O1); the Review Focus and *After this plan* sections must not suggest otherwise.

## O2 — specification quality — high: the plan predates 8e85cfd

**Decision (2026-10-14): accepted.** Tasks 2–3 are rewritten as the delta remaining after 8e85cfd, once all objections are decided (so O3–O5's changes land in the same edit).

**Plan changes to apply:**
- Task 2 keeps only #109's reuse: `Handshake(how string)` and `HandshakePrecomputed`/`HandshakeInline`/`HandshakeReused` (replacing `Handshake(bool)`), `StormCount.HandshakesReused`, `sdkClient.held`, `track`, `initiate`, `handshake`, `errHandshakeTaken`, and `replyConn`'s `sc`/`hs`/`closed`/`Close` and reply guard. It edits the current `sdkclient.go` rather than replacing the file; `send_lag` stays cited as #107 (P-sendlagI-1).
- Task 2's red step names failures that happen now (`undefined: HandshakeInline`, `sc.initiate undefined`, `sc.track undefined`); test edits already in 8e85cfd (`send_lag` in `TestRecorderCountsTheStormSinceMark` and `TestSDKStormUsesPrecomputedHandshakes`) are not repeated.
- Task 1 and Task 3 Step 1's `send_lag` changes are marked done (8e85cfd). Task 3 keeps the header/comment text for `handshakes_reused` and the roadmap's #109.
- Run numbering follows P-sendlagI-2: run 26 is without #109; the first run with #109 is run 27 (expected 29/29), executed only after run 26 (O1).

## O3 — implementation — medium: a cancelled attempt's late write takes the handshake back

**Decision (2026-10-14): accepted.** `handshake.send` refuses a connection already closed, so a cancelled attempt's late first write fails as its own error and the live sender keeps the handshake.

**Plan changes to apply:**
- `send`: `if h.answered || c.closed.Load() { return false }`; its comment says a closed connection (a cancelled attempt writing late) must not take the handshake from a later sender.
- `TestAHeldHandshakeIsFinishedOnce` gains a fourth case, "a closed sender's late write": h held with a closed owner, B writes it, the closed A writes it (`errHandshakeTaken`), B reads its reply byte (n 1, no error).
- Review Focus 1 names the late write as covered.

## O4 — risk — high: no test runs the reuse through the real SDK

**Decision (2026-10-14): accepted.** A loadgen e2e test drives refusals through the real SDK and `clientconn`, so a missed frame match fails a test instead of a host run.

**Plan changes to apply:**
- Task 2 adds `TestSDKResendsARefusedHandshake` (`internal/loadgen/e2e_test.go`): sdk-mode clients whose first connection is refused after its frame is read (a stub listener behind the balancer, or the real gateway with a handshake limit that refuses first, whichever the existing e2e fixtures make cheaper; chosen when the plan is rewritten). Asserts: every client online; `handshakes_reused` ≥ 1 per client; `handshakes_inline + handshakes_precomputed` = answered handshakes; where a stub sees the bytes, a refused frame and its resend are identical.
- #109's text states the coupling as an assumption the test enforces: `replyConn` recognizes the handshake frame by its backing array, so the SDK path must write `frame` unchanged in one `Write` (`clientconn.HandshakeWith` → `wire.ClientHandshakeWith`).

## O5 — specification quality — low: the wrong reason for never reusing an answered handshake

**Decision (2026-10-14): accepted.** Noise NK's reply carries the responder's fresh ephemeral, so the keys would not repeat; the reason is the Initiator's state.

**Plan changes to apply:** #109's rejected alternative reads "reusing an answered handshake (an Initiator serves one handshake: `Finish` uses its handshake state up)".
