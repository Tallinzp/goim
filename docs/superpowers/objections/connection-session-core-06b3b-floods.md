---
spec: docs/superpowers/plans/2026-10-16-connection-session-core-06b3b-floods.md
date: 2026-10-16
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: implementation
    severity: critical
    claim: "slow_held requires slow-auth's outcomes to be exactly {\"closed\"}, but a slow-auth attempt the concurrency limit refuses is recorded as \"pre-handshake\", and the plan itself expects ~4.5% of slow's handshakes to be refused as no_slot, so slow_held fails on the host by the plan's own estimate."
    evidence: "slow_held: `set(d[\"flood\"][\"outcomes\"]) == {\"closed\"}` for both slowhs and slowauth; internal/loadgen/flood.go:241-243 `case errors.Is(err, clientconn.ErrClosedBeforeReply): r.flood.outcome(FloodPreHandshake, -1)`; Review Focus 1: \"In `slow`, each gateway has 3 slots at ~8.6k handshakes/s (slow-auth's redials plus the light ramp): ~0.9 Erlang, ~4.5%\"."
    disposition: accepted
    disposition_rationale: "slow_held accepts slow-auth pre-handshake; hold bounds on closed only (see dispositions file)"
  - id: O2
    category: risk
    severity: high
    claim: "The Erlang estimates assume a slot is held ~100µs, but the project's own measurements put the hold at ~300-500µs; at that hold time storm (6 slots, 20k/s) and slow (3 slots per gateway) refuse 20-45% as no_slot, storm_rate's 0.9 x 20k/s is out of reach, and light clients' 10-attempt limit turns the loss into gave-up failures of storm_online and slow_legit."
    evidence: "Review Focus 1: \"at 20,000/s × ~100µs that is 2 Erlangs, so ~1.2% are refused\"; the plan's own dry run: \"with 1 slot, refused 33–38% at 1,000/s and 6% at 200/s\" (B(1,a)=a/(1+a) gives a≈0.5 and 0.064, i.e. ~500µs and ~320µs held); roadmap #111: \"the two 4-core survivors admit ~8.7k handshakes/s together … each survivor runs 2 handshakes at once … closes every other handshake frame at once as no_slot (~1.9M each, ~6.5 per login)\" (≈2.2k/s per slot, ~460µs); internal/loadgen/light.go:43 `const lightAttempts = 10` then `Failed(\"gave-up\")`; storm_online: `\"gave-up\" not in d[\"failed\"]`."
    disposition: accepted
    disposition_rationale: "run 30 records the semaphore's effects: storm_rate on the seen rate, pre-handshake-only gave-ups allowed; input to #111 (see dispositions file)"
  - id: O3
    category: specification quality
    severity: high
    claim: "garbage_limited passes on no_slot alone, which the semaphore's Erlang loss produces at almost any flood rate, so it cannot fail and does not show what the plan says it shows (\"it ran past the bucket\"); Review Focus 3's 80,000/s sizing is argued against the bucket, but whether the bucket or the semaphore binds depends on the unmeasured hold time (O2)."
    evidence: "garbage_limited: `all(v[\"hs_no_slot\"] + v[\"hs_no_token\"] > 0 for v in d.values())`, header: \"(no_slot + no_token > 0): it ran past the bucket\"; limiter.go: \"The slot is taken first, so a full semaphore never spends a token\"; dry run storm at 200/s: `hs_no_slot` 67 (6%) with no flood at all; Review Focus 3: \"At 60,000/s that split would leave one gateway at ~21,000/s, under its 25,000/s bucket … the default of 80,000/s keeps the smaller share at ~28,000/s, so `garbage_limited` … passes\"."
    disposition: accepted
    disposition_rationale: "garbage_limited gates no_token > 0 per gateway; Review Focus 3 restated slot-first (see dispositions file)"
  - id: O4
    category: specification quality
    severity: low
    claim: "Review Focus 1 and After this plan present \"let a handshake wait briefly for a slot\" as a fresh decision for after run 30 without citing parked choice #111, which is that decision and already holds the b_stress evidence."
    evidence: "Review Focus 1: \"Whether to size it differently or let a handshake wait briefly for a slot is the user's decision after the run, not this plan's\"; roadmap #111: \"A handshake that finds no free slot waits briefly instead of being rejected (… parked 2026-10-15 by the user …)\"; the plan's Decisions line cites #118, #121, #122 only."
    disposition: accepted
    disposition_rationale: "text only: cite parked #111 (see dispositions file)"
  - id: O5
    category: specification quality
    severity: low
    claim: "The heartbeat check's \"every client Pinged at least once\" is gated as an aggregate over the light instance's whole run (pongs >= N), including Pongs from before the flood, so it does not say what the header says."
    evidence: "heartbeats(): `d[\"r\"][\"pongs\"] >= '\"$2\"'` over `loadgen-$LG_NAME.json` (the whole run); header: \"every Ping answered (no Pong missing, every client Pinged at least once)\"; Review Focus 5: \"The light client's Pong RTT is one distribution over its whole run. It covers its ramp as well as the flood\"."
    disposition: accepted
    disposition_rationale: "text only: 'at least N Pongs over the light run' (see dispositions file)"
---

## O1 — implementation — critical

### Claim

`slow_held` passes only if every outcome of both slow instances is `closed`. Loadgen records a slow-auth attempt that the gateway closes before the handshake reply as `pre-handshake`. That is exactly what a `no_slot` (or `no_token`) refusal looks like to the client. The plan expects such refusals in `slow`: Review Focus 1 estimates ~4.5% at 3 slots per gateway. Slow-auth makes ~7,300 handshakes/s for 60+ seconds, so several thousand `pre-handshake` outcomes are expected, and `slow_held` fails for a reason the plan calls acceptable everywhere else. `slow_legit` records `no_slot` without gating it, so `slow_held` contradicts it.

### Evidence

> `[ "$(lg_true "$STACK_DIR/artifacts/loadgen-$inst.json" 'set(d["flood"]["outcomes"]) == {"closed"} and …')" = true ] || held=false` (slow_held, for `slowhs` and `slowauth`)

> `internal/loadgen/flood.go:241` — `case errors.Is(err, clientconn.ErrClosedBeforeReply): r.flood.outcome(FloodPreHandshake, -1)`; `:49` `FloodPreHandshake = "pre-handshake" // slow-auth: closed before the handshake reply (the limit)`

> Review Focus 1: "In `slow`, each gateway has 3 slots at ~8.6k handshakes/s … ~0.9 Erlang, ~4.5%."

The dev dry run passed because 500 slow-auth connections at 500/s found the slot free (3,500 closed, all `closed`). It never exercised this path.

### Why this matters

On the host, run 30 would very likely report `slow_held` failed. Its *What to read* line ("`slow_held` past 6.5s → the gateways' wheel was late") would send the reader to the wrong cause. The fix is small: accept `pre-handshake` in slow-auth's outcomes and record its count. Only `closed` gets the hold bounds, and `answered`/`timeout`/`handshake`/`error` stay failures. It needs re-verifying in the prototype, though, so it should be decided before execution.

## O2 — risk — high

### Claim

Every refusal estimate in the plan (1.2% in storm, 4.5% in slow) assumes the semaphore slot is held ~100µs per handshake. The plan gives no source for that figure, and the project's own data put it several times higher:

- **This plan's dry run:** 1 slot refused 33–38% at 1,000/s and 6% at 200/s. With one server, B = a/(1+a), so a ≈ 0.5 and 0.064 Erlang, which means a hold of ~500µs and ~320µs.
- **#111 (b_stress):** two survivors with 2 slots each admitted ~8.7k handshakes/s together while closing ~6.5 handshakes per login as `no_slot`, so the slots were saturated. That is ~2.2k/s per slot, a hold of ~460µs.

The host is faster than the dev box, but b_stress *was* the host. At a hold of 300–460µs:

- **storm:** offers 6–9 Erlangs to 6 slots. The Erlang-B loss is ~25–40% before retries, and each retry adds load. Completed handshakes cap at ~13–20k/s, so `storm_rate` (≥ 18,000/s) likely fails. Light clients give up after 10 attempts, and a share of retries that heavy makes `"gave-up"` likely, which fails `storm_online`.
- **slow:** 3 slots per gateway at ~8.6k/s is 2.6–4 Erlangs, ~25–45% refused. At 0.45, 0.45¹⁰ × 400,000 ≈ 130 clients give up, which fails `slow_legit`'s `online == n`. It also feeds O1.

### Evidence

> Review Focus 1: "at 20,000/s × ~100µs that is 2 Erlangs, so ~1.2% are refused … The dev dry run, with 1 slot, refused 33–38% at 1,000/s and 6% at 200/s."

> Roadmap #111: "In b_stress the two 4-core survivors admit ~8.7k handshakes/s together … each survivor runs 2 handshakes at once (`HandshakeConcurrency`, half the loops) and closes every other handshake frame at once as `no_slot` (~1.9M each, ~6.5 per login); after 10 such closes in a row the SDK backs off … 201,564 clients (34%) did".

> `internal/loadgen/light.go:43` `const lightAttempts = 10` … `r.rec.Failed("gave-up")`; storm_online gates `"gave-up" not in d["failed"]`.

### Why this matters

Review Focus 1 calls `no_slot` "recorded, not gated". But two gates (`storm_rate`, and `storm_online`/`slow_legit` through gave-up) depend on it indirectly. If the hold time is what the project has measured, run 30 fails 2–4 checks and mostly re-measures #111. The plan should decide in advance what run 30 is for. One option: storm at 20k/s stays as is, and failures are read as #111's finding. Another is to gate the semaphore's indirect effects differently: `storm_rate` against the completed rate the slots allow, and gave-up recorded rather than gated. A third is a short calibration (one storm at a lower rate) that measures the hold first, or taking #111 out of parking before run 30. If none of these is decided, the run's failure will look like a test bug.

## O3 — specification quality — high

### Claim

`garbage_limited` is meant to show that "the flood ran past the limit" and, in the header, "it ran past the bucket". It passes when `no_slot + no_token > 0` on each gateway. The semaphore refuses some handshakes at any non-trivial rate: the dry run's storm at 200/s, with no flood at all, had 6% `no_slot`. So the check cannot fail at any flood rate the scenario would use, and it does not tell the bucket apart from the semaphore. Review Focus 3 sizes the flood at 80,000/s so that the smaller gateway's share (~28,000/s) exceeds the 25,000/s **bucket**. The limiter takes the slot first, though, so the bucket sees only what the semaphore lets through: a hold of h µs on 3 slots carries at most ~3/h, minus the Erlang loss. At ~100µs, 28k/s offered carries ~19–24k/s and the bucket never fires on that gateway. At ~50µs it does. The plan's sizing argument depends on a number it has not measured (O2).

### Evidence

> Header: "`garbage_limited` each gateway's limiter refused part of the flood (no_slot + no_token > 0): it ran past the bucket"; Choice 3: "each gateway's limiter refused part of it (`garbage_limited`, so the flood ran past the limit)".

> `limiter.go`: "The slot is taken first, so a full semaphore never spends a token."

> Review Focus 3: "At 60,000/s that split would leave one gateway at ~21,000/s, under its 25,000/s bucket. Expected: the default of 80,000/s keeps the smaller share at ~28,000/s, so `garbage_limited`, which is gated per gateway, passes."

> Dry run storm pass path: "`storm_bucket` `hs_no_slot` 67 (`no_slot_share` 0.06), `hs_no_token` 0" at 200/s.

### Why this matters

The spec's handshake-flood row names both limits ("The global handshake concurrency limit and token bucket protect established connections"). As written, run 30 can pass `garbage_limited` with the bucket never reached, and the README row would still read as though the bucket was tested. There are two fixes. One is to gate what the text claims, `no_token > 0` per gateway, and size the rate for it. The other is to say what the check shows (the limiter refused a share), gate a meaningful refused share per gateway, and record which limit fired. Review Focus 3 then needs rewording to match either way.

## O4 — specification quality — low

### Claim

Review Focus 1 and *After this plan* frame "size it differently or let a handshake wait briefly for a slot" as a new decision for after run 30. That decision is #111, which the user parked on 2026-10-15 with the b_stress evidence and the concurrency-4 collapse (594s). The plan's Decisions line does not cite it.

### Evidence

> Review Focus 1: "Whether to size it differently or let a handshake wait briefly for a slot is the user's decision after the run, not this plan's."

> Roadmap #111: "A handshake that finds no free slot waits briefly instead of being rejected (proposed 2026-10-15 … parked 2026-10-15 by the user: … revisit with Tier 3 capacity numbers or a later admission-control pass)". Also: "Raising the concurrency to 4 instead collapsed (594s …), so the half-the-loops split must stay."

### Why this matters

Without the link, run 30's `no_slot_share` could go into a fresh choice number and repeat #111's analysis, or someone could "size it differently" (raise the concurrency) against #111's recorded result that doing so collapsed. Citing #111 makes run 30 the "later" input #111 is waiting for.

## O5 — specification quality — low

### Claim

The heartbeat check's header promises "every client Pinged at least once". The gate is `pongs >= N` over the light instance's whole run. In `garbage`, the clients have already Pinged during the ~40s ramp and the wait before the flood, so the count can reach N with no client Pinged during the flood. `pong_missing == 0` and `lost == {}` still catch an unanswered Ping, so this is about the description, not a hole in the protection gates.

### Evidence

> `heartbeats()`: `d["r"]["pongs"] >= '"$2"'` read from `loadgen-$LG_NAME.json` after the instance stops.

> Header: "every Ping answered (no Pong missing, every client Pinged at least once)". Review Focus 5: "It covers its ramp as well as the flood".

### Why this matters

A reader of run 30 would take `pongs >= N` as per-client coverage of the flood. Either the text should say "at least N Pongs over the run", or the gate should count Pongs since the flood started (for example, as the delta of a `/stats` read at `t0`).

## Explicitly not objecting to

- **`storm_bucket` gating only `no_token`**: the spec's clause is about the bucket, and recording `no_slot` keeps the semaphore's question visible; O2 is about the gates that depend on it indirectly, not this one.
- **Pong RTT under 10s as "on time"**: tying "on time" to the SDK's timeout above the heartbeat is a defensible reading of the spec, and the plan records the quantiles and names the baseline alternative (which needs loadgen to report `pong_rtt` since a mark, which it does not).
- **The 6b2a-pinger ruling**: the light clients are read by the gateways throughout, so a Ping write with no deadline cannot hold back the Pong check here, as the plan says.
- **TIME_WAIT overflow on the gateway's side**: it is recorded with the kernel counters and the outcomes, and loadgen closes no garbage attempt first, so its own ports are not tied up.
- **`-max-conns` above the spec's 250,000 in `garbage`/`slow`**: it is a test sizing to fit floods plus clients, not a claim about production capacity.
- **Logic's load in `storm`**: ~0.08 cores per 1,000 logins/s (b1) puts 20k/s at ~1.6 of Logic's 4 CPUs, and Busy is recorded.
- **Three phases on fresh stacks and `FLOODS_PHASES`**: this follows `gwloss` and keeps one phase's TIME_WAIT and half-open state out of the next.
- **`t2_lease_floor` moving to `lib.sh`**: a pure move with a test; no behaviour changes in `slowlogic`.
- **The loadgen bound (Review Focus 2)**: `storm_rate` and `garbage_delivered` already fail rather than pass on a trickle, which is the right shape.
