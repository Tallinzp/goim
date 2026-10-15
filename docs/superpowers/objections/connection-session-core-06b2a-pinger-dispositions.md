# Dispositions: Plan 6b2a-pinger objections

Objection record: `connection-session-core-06b2a-pinger.md`. Decided by the user one at a time.

## O1 — specification quality — high: the "during a firing" subtest hangs

**Decision (2026-10-15): accepted, folded into O2.** `synctest.Wait` does not return while `stop` is blocked on `p.mu` (a mutex wait is not durably blocking; reproduced on go1.27.1). The fix is O2's: `stop` no longer waits for a firing, so nothing blocks on `p.mu` beyond a state update. The lock's exact shape is decided with O2.

**Plan changes to apply (with O2's):**
- `TestPingerStopsForGood/during a firing` is rewritten: with the firing blocked in `Send`, `stop` returns at once; after `release`, the firing does not re-arm: no timeout at 40s, no second Ping, over 10 minutes.
- Review Focus 1's expectation and mutation 1 are restated for the non-waiting `stop` (see O2, O5).

## O2 — implementation — high: stop waits for a firing

**Decision (2026-10-15): accepted, non-waiting `stop`.** Nothing depends on `stop` waiting: after `serve`'s `Close` a late `Send` fails, and a late `timeout` sets `serve`'s local flag and closes a closed socket. What must hold is only that the timer is not re-armed after `stop`.

**Plan changes to apply:**
- `fire`: lock `mu`; return if `stopped`; update `unanswered`, decide between timeout, Ping and nothing; unlock. Call `timeout()` or `send(reqID)` without `mu`. For a Ping, lock again: return if `stopped` or the send failed; set `unanswered`/`next`; `Reset`; unlock. The pinger's state is touched only under `mu` (the race detector's edge from one firing to the next).
- `stop`: under `mu`, set `stopped` and call `t.Stop()`; it does not wait for a firing. Its comment: a firing in progress may still finish its `Send` or `timeout`, harmlessly (the connection is closed first), but none re-arms the timer.
- *Implementation choices*: "One mutex held for the whole firing" is replaced by the above, with the reason (no new hang: `serve` never waits on a `Send`).
- Review Focus 1: after `stop` returns no firing re-arms; a firing already past its check may finish its one `Send`/`timeout`. Review Focus 3: `stop` does not wait, so a blocked `Send` cannot hold up `serve`; the defer order (Close then stop) is kept but is no longer load-bearing; mutation 2 and its *Execution Notes* line are dropped.
- `TestPingerStopsForGood/during a firing` as in O1. Mutation 1 becomes: in `fire`, drop both `stopped` checks — the subtest fails (the released firing re-arms, and a timeout follows at 40s after `stop`). Dropping only one does not fail it: the entry check stops the re-armed 40s firing, and the post-`Send` check stops the re-arm; the plan says so, and that the entry check alone covers a callback already started when `stop` ran (reviewed by reading).

## O3 — premise — medium: the saving is estimated, not measured

**Decision (2026-10-15): accepted, measure and name the fallback.**

**Plan changes to apply:**
- Task 2 Step 1: the dry run is done twice, on the commit before Task 1 and after it, each reading loadgen's last `/runtime` sample once all victims are back (stack bytes, goroutines). The *Execution Notes* record stack bytes per online SDK client before and after, the difference, and that difference scaled to 600k, against the plan's ~3.3GB. A per-client figure at 1,500 clients is a guide (stack sizes may differ at scale), not the Tier 2 result.
- *After this plan* gains the branch for loadgen still over 16GiB (mapped above the limit after the victims are back, or the limiter engaged as before): stop and decide with the user between `Client.run`'s goroutine (the remaining one per client besides `serve`), a smaller b_stress population, or recording b_stress as loadgen-bound.

## O4 — specification quality — medium: the gate has no numbers

**Decision (2026-10-15): accepted, numeric gate.**

**Plan changes to apply:**
- Task 2 Step 3's gate: (1) loadgen's mapped memory ≤ `GOMEMLIMIT` (16GiB, 17.18GB) in every `/runtime` sample after all victims are back; (2) the GC CPU limiter engaged in no cycle after t≈60s; (3) `no_swap_in_b_stress` may fail with ≤ 1,000 pages swapped in: recorded, not gating (every 16GiB b_stress run so far had 1–272).
- Step 3 runs before the host's UMA change. If the UMA change is already made, the *Execution Notes* say so, and that run does not claim the result holds without it (Global Constraints).
- *After this plan*'s over-limit branch (O3) uses the same (1)/(2).

## O5 — specification quality — low: mutation 1's consequence is wrong

**Decision (2026-10-15): accepted, covered by O2.** O2's rewritten mutation 1 replaces the sentence ("a second Ping at 60s follows"); nothing more to apply.
