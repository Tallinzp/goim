# Dispositions: Plan 6b3b review (2026-10-16)

Objections: `connection-session-core-06b3b-floods.md`. Decided by the user, one at a time.

## O1 — accepted: slow_held accepts slow-auth's `pre-handshake` outcome

**Decision:** `slow_held` allows `closed` and `pre-handshake` in slow-auth's outcomes. The hold bounds (p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s) apply to `closed` holds only, as `held` already does: a pre-handshake close records no hold. `answered`, `timeout`, `handshake` and `error` stay failures. Slow-handshake still allows `closed` only, because it never asks for a handshake.

**Rationale:** a slow-auth handshake that the concurrency limit refuses (`no_slot`) is closed before its reply, and loadgen records that as `pre-handshake` (`flood.go:241`). The plan expects such refusals (Review Focus 1: ~4.5%, more after O2). With `{"closed"}` only, `slow_held` would fail on the host for a reason the plan records without gating everywhere else. The dev dry run passed only because the slot was free at 500/s.

**Plan changes to apply:**
- `slow_held`: per instance, `set(outcomes) <= {"closed", "pre-handshake"}` for `slowauth`, `== {"closed"}` for `slowhs`, then the hold bounds. Its detail records `outcomes` already. Name `pre-handshake` in the header line.
- Header `slow_held` comment, Choice 4 and *What to read*: slow-auth's refusals are `pre-handshake`, recorded. They are the same limit as `slow_legit`'s `no_slot`.
- Re-verify in `proto-6b3b`: dry run plus a forced refusal (e.g. `FLOODS_GARBAGE_BUCKET`-style low `-handshake-rate` for `slow` in a throwaway copy) so that `pre-handshake > 0` and `slow_held` still passes. Replay byte for byte.

## O2 — accepted: run 30 records the semaphore's effects instead of failing on them

**Decision:** keep 20k/s in `storm` and the `slow` sizes. Gates that depend on the semaphore's loss indirectly are changed so that the loss is recorded, not failed:
- `storm_rate` gates the rate the gateway **saw**: `ok + no_slot + no_token` per second ≥ 0.9 × rate over the ramp's body (the window is still placed by `ok` crossing 5% and 95% of N). The completed (`ok`) rate, its peak and the series are recorded beside it. So the check still fails on a loadgen-bound trickle, but not on the semaphore.
- `storm_online` and `slow_legit`: every client logs in, **or** each client that did not had only pre-handshake failures. That means `online + gave-up == N` and `set(failed) <= {"pre-handshake", "gave-up"}`. `gave-up` is recorded with its count. Any other failure (Busy, a reject, a timeout) still fails.
- Every `no_slot` effect (share, `pre-handshake`, `gave-up`) is an input to parked #111 (see O4).

**Rationale:** the plan's 1.2% / 4.5% estimates assume a ~100µs slot hold. The project's own data put it at ~300–500µs. This plan's dry run gives ~500µs at 1,000/s and ~320µs at 200/s by Erlang-B on one slot. #111's b_stress gives ~2.2k/s per slot, ~460µs. At that hold, `storm` (6 slots, 20k/s) and `slow` (3 per gateway) refuse 20–45%. `storm_rate`'s completed 18k/s would then be out of reach, and light clients' 10-attempt limit would turn the loss into `gave-up`. Run 30 would fail 2–4 checks while measuring #111 again. The spec's clause is about the bucket. The semaphore question is already parked as #111 and needs exactly these numbers.

**Plan changes to apply:**
- `hs_sampler`/`hs_rate`: sample `ok` and the refusals (`no_slot + no_token`) each second. `hs_rate` returns `seen_per_s` (all results) and `rate_per_s` (`ok`), with the window placed by `ok`. `storm_rate` gates `seen_per_s >= 0.9 × rate` and records both.
- `storm_online`, `slow_legit`: the condition above. The detail records `gave_up`. `slow_legit` keeps its zero gates on Busy, `login_queue`, `pending_mem`, `at_capacity` and `no_token`.
- Review Focus 1: replace the ~100µs estimate with the measured range (dry run ~320–500µs, #111 ~460µs) and the resulting 20–45% expectation. State which checks record the loss and which still gate. Header comments and Choice 2/4 accordingly.
- *What to read*: `storm_rate.handshakes.rate_per_s` against `seen_per_s`, and `gave_up`, as #111's input.
- Re-verify in `proto-6b3b`: the dev dry run's `storm_rate` (33% `no_slot` at 1,000/s) should now **pass** on `seen_per_s`. A loadgen-bound case must still fail (e.g. a rate the dev loadgen cannot reach). Update the expected dry-run results and the check count if it changes.

## O3 — accepted: garbage_limited gates the bucket per gateway

**Decision:** `garbage_limited` requires `hs_no_token > 0` on each gateway, so the bucket fired, as the text claims. `no_slot` is recorded beside it. If the semaphore binds first on the host and the bucket never fires on a gateway, run 30 shows that as this check failing, with `dh_per_s` and `no_slot` to read.

**Rationale:** `no_slot + no_token > 0` cannot fail. The semaphore refuses some handshakes even at 200/s without a flood (the dry run's 6%). And because the slot is taken before the token (`limiter.go`), the bucket sees only what the semaphore lets through. Whether the bucket ever fires depends on the slot hold time (O2). The spec's flood row names both limits, and the README row should not claim the bucket was tested unless it fired.

**Plan changes to apply:**
- `garbage_limited`: `all(v["hs_no_token"] > 0 for v in d.values())`. The detail keeps `no_slot`, `no_token`, `dh_per_s` and `bucket`.
- Review Focus 3: restate the sizing in slot-first order. A gateway's `dh_per_s` is capped by its 3 slots (≈ 3 / hold, minus the Erlang loss) before the bucket sees anything, so 80,000/s reaches the bucket only if the slots carry more than 25,000/s. If `garbage_limited` fails with `dh_per_s` under 25,000 and `no_slot` large, the semaphore bound first. That is a finding, recorded as #111's input, and not fixed by raising `FLOODS_GARBAGE_RATE`.
- Header, Choice 3 and *What to read*: the bucket check is per gateway. Which limit fired is recorded.
- Re-verify in `proto-6b3b`: the dry run (`FLOODS_GARBAGE_BUCKET=500`) already has `no_token` > 0 on both gateways (34,737 / 5,389), so it still passes. Add a mutation (bucket raised far above the flood) that makes it fail.

## O4 — accepted: cite parked #111

**Decision:** text only. Review Focus 1, the Decisions line and *After this plan* cite #111 (parked 2026-10-15) and its concurrency-4 collapse (594s). Run 30's `no_slot_share`, slow-auth's `pre-handshake` and the `gave-up` counts are the "later" input #111 waits for. Revisiting it stays the user's call.

**Rationale:** without the link, run 30's numbers could open a new choice that repeats #111, or someone could "size it differently" (raise the concurrency) against #111's recorded collapse.

**Plan changes to apply:**
- Review Focus 1's last sentences: "… is #111 (parked by the user 2026-10-15); run 30 is its input. Raising the concurrency collapsed b_stress (594s), so sizing up is not the open option."
- Decisions line: "#111 (parked) is the open question run 30 feeds."
- *After this plan*, the Spec bullet: the same reference.

## O5 — accepted: the heartbeat check's text says what it counts

**Decision:** text only. The header's `<phase>_heartbeats` comment and Review Focus 5 say "at least N Pongs over the light run". `pong_missing == 0` and `lost == {}` stay the gates that catch an unanswered Ping during the flood.

**Rationale:** `pongs >= N` counts Pongs from the ramp and the wait before the flood. It is not per-client coverage of the flood, and the text should not read that way.

**Plan changes to apply:**
- Header (`garbage_heartbeats`) and Review Focus 5: replace "every client Pinged at least once" with "at least N Pongs over the light run (a floor, not per client)".
- No code change.

## Roadmap

- #122 (proposed) gets amended with O1–O3 when the plan is updated: `storm_rate` on the seen rate, `storm_online`/`slow_legit` allow pre-handshake-only gave-ups, `garbage_limited` on `no_token` per gateway, `slow_held` allows slow-auth `pre-handshake`.
