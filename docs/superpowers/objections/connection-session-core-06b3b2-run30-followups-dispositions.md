# Dispositions: Plan 6b3b2 review (2026-10-17)

Objections: `connection-session-core-06b3b2-run30-followups.md`. The user decided them with AskUserQuestion: O1–O4 first, then O5–O10. All ten are accepted. O5 is accepted as "name the alternative as rejected", not "do it". The changes below go into the plan and are re-verified in `proto-6b3b2`, then replayed into the plan byte for byte, before execution.

## O1 — accepted: the pinger's Send gets a write deadline, besides the catch-up

**Decision:** keep the clock-driven catch-up and the two diagnostics, and add a write deadline to the pinger's Ping `Send`. A client that misses the deadline fails its `Send` and is closed. Its `read` then ends as `lost`, which `<phase>_heartbeats` already gates (`lost == {}`). So a blocked socket shows up as a counted client loss and can't hold up its shard (1/8 of the population). `ping_send_max_ms` stays as the diagnostic. A count of timed-out Sends is recorded beside it (`ping_send_timeouts`).

**Rationale:** run 30's record leaves the stall cause open: ticks dropped by the `Ticker`, or a `Send` with no deadline. The light instance used 3.02 cores, which argues against plain starvation. A blocked `Send` keeps the shard's goroutine inside `p.send`, where catching up can't help.

**Plan changes to apply:**
- `clientconn`: a way to bound a write. Either `SetWriteDeadline`, mirroring `SetReadDeadline`, or a `Send` variant that takes a deadline. Pick the smaller API in the prototype. The pinger's default `send` sets the deadline. Proposed value: 1s (10 ticks), well under the Pong timeout. Settle it in the prototype and state it in Implementation choice 1.
- `Recorder`: `PingSendTimeout()` and `Counts.PingSendTimeouts` (`ping_send_timeouts`). The heartbeats detail records it.
- Test: a blocked `Send` with the deadline (synctest) times out, the client is closed, the rest of the shard is pinged on time, and the count increments. One mutation: remove the deadline and the test fails.
- Task 1's test comment: the stall cause is open (ticks or a blocked Send). The catch-up covers the first case and the deadline the second. Implementation choice 1: the write deadline is no longer "not chosen".
- Review Focus 3 and Task 5's reading: `ping_send_timeouts > 0` means sockets blocked. Read it with `lost` and the gateways' `closes_heartbeat`.

## O2 — accepted: a spec follow-up on the conntrack sizing option

**Decision:** queue a spec follow-up beside #97/#101 for the next spec revision, as a new roadmap choice (**#125**, proposed by this review, decided by the user 2026-10-17). Run 30 shows that `nf_conntrack_max ≥ 2 × cap` doesn't survive a handshake flood. Each flood connection holds an entry for 120s (TIME_WAIT) before the limiter can refuse it, so a 2 × cap table fills at about cap/60 connections per second. The revision decides between sizing the table from a flood rate (rate × `nf_conntrack_tcp_timeout_time_wait` + 2 × cap), making NOTRACK the only option for gateway ports, or saying that the sizing option assumes no flood.

**Plan changes to apply:**
- Roadmap: add #125 (spec follow-up, with run 30's figures: 61k/s, 4,194,304 entries full).
- *After this plan* → **The spec:** replace "nothing here changes it" with a pointer to #125. This plan itself changes no spec text.

## O3 — accepted: `<phase>_conntrack` also checks that NOTRACK works

**Decision:** besides `peak ≤ 0.8 × max`, `<phase>_conntrack` gates `peak` against the phase's connections, so the check fails when the rules exist but don't match. Proposed bound: `peak < 0.1 × the phase's peak connections`, using the phase's population (`storm` N, `garbage` and `slow` the light clients plus the flood's in-flight). Settle the population and the factor in the prototype. With conntrack not loaded (`max` null), the check still passes. The detail records `peak`, `max`, the population, and the ratio.

**Plan changes to apply:**
- `ct_check PHASE POP`: callers pass the population. The condition and the detail as above.
- Header, Implementation choice 3, Review Focus 1: the check now proves NOTRACK works. Task 5's hand reading becomes a check.
- Dry run: the forced conntrack failure stays (801 of 1000). Add a forced "NOTRACK not working" case: a fake count above 10% of the population but under 80% of max, which fails.

## O4 — accepted: loadgen's CPU over the ramp only

**Decision:** the `loadgen-bound` verdict reads loadgen's cores over the storm's ramp window, the one `hs_rate` places by `ok` crossing 5% and 95% of N. It no longer uses the whole `stack_usage` window, which includes the settle time. The 85% threshold stays. Both readings are recorded (`loadgen_cores_ramp` gated, `loadgen_cores` as before).

**Plan changes to apply:**
- `phase_storm`: loadgen's CPU sampled so that its ramp-window share can be computed, for example by sampling its `/proc/PID/stat` CPU time each second beside `hs_sampler` and cutting the series to the ramp. Choose the smaller mechanism in the prototype.
- Review Focus 4: the threshold reads the ramp, and run 30's 86% was a whole-window figure, so 30b's ramp reading should sit clearly above it if loadgen was the bound.
- Dry run: the forced loadgen-bound and short cases still reach their verdicts.

## O5 — accepted as "name it as rejected": loadgen keeps its CPU set in `storm`

**Decision:** the CPU sets stay as they are. The plan names the alternative and says why it is rejected. Giving loadgen half of the gateway set in the storm phase would cut gw-0 to 6 CPUs (GOMAXPROCS 12 → 6), so the gateway under test would change between phases and from the other scenarios.

**Plan changes to apply:** one *Alternative:* sentence under #124.1 or Review Focus 4.

## O6 — accepted: `not_covered` when the bucket never bound, and cite 6b3b O3

**Decision:** keep #124.2's gate (`no_slot + no_token > 0`). When every gateway's `bound_by` is `slots`, `phase_garbage` adds a `not_covered` line saying the bucket was not exercised, since the concurrency limit bound first, with the bucket and `dh_per_s`. The plan cites 6b3b's O3 (accepted) and says why the gate goes back: on this host the slots always bind first (run 30), and the gate now covers the limiter as a whole, with the bucket's coverage recorded as `not_covered`.

**Plan changes to apply:** `phase_garbage` (the `t2_not_covered` call), the header comment, #124.2 in Decisions. Dry run: the default case (bucket 500, `bound_by` bucket) has no such line. A forced case with the bucket above the flood has the line and passes on `no_slot`.

## O7 — accepted: per-check expectations, a NOTRACK branch, and conntrack stats

**Decision:** Task 5 drops the bare "Expected: 30/30" and states an expectation for each of run 30's failures. `storm_rate`, `garbage_limited` and `slow_heartbeats` change in this plan. `slow_legit` should follow from NOTRACK. `garbage_delivered` and `slow_held` have no change aimed at them and may still fail: say so, and say what to read for each. Review Focus 5 gains a branch: if `garbage_delivered` passes, NOTRACK is the likely cure. Conntrack drops SYNs without a full table. Each phase snapshots `/proc/net/stat/nf_conntrack` (summed per column across CPUs: `insert_failed`, `drop`, `early_drop`, `invalid`) beside its CPU sets, so a later run can tell.

**Plan changes to apply:**
- `floods.sh`: a conntrack-stat snapshot at each phase's window edges, its deltas in `garbage_delivered`'s detail and each phase's field. Where the file is absent (dev machine), it reads null.
- Review Focus 5 and Task 5 *What to read*: the branch above, and the per-check expectations.

## O8 — accepted: the firewall check covers nftables

**Plan changes to apply:** Review Focus 2 and Task 5 Step 1.1 also run `sudo nft list ruleset`. No `ct state` accept-only policy may apply to `lo` without a prior `lo` accept (`iifname "lo" accept`) in any input hook chain.

## O9 — accepted: each client is stamped just before its own Send

**Plan changes to apply:** in `pinger.run`, take `now` per client immediately before `p.send(lc)`, and use the same value for `pingAt`, `lastPing`/`PingGap` and the `PingSend` start. Test: in a burst behind a blocked Send, the later clients' gap and RTT don't include the block. Can join O1's test.

## O10 — accepted: a comparability note

**Plan changes to apply:** Task 5 Step 3 adds one line to the roadmap's Tier 2 notes: runs up to and including run 30 tracked the stack's flows in conntrack, and runs from 30b on are NOTRACK on 17000–17199 (#123). Results from before and after are not directly comparable where per-flow kernel cost matters (capacity, gwloss, slowlogic).
