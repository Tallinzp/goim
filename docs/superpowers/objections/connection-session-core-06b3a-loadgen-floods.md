---
spec: docs/superpowers/plans/2026-10-16-connection-session-core-06b3a-loadgen-floods.md
date: 2026-10-16
mode: spec
diaboli_model: claude-opus-5-5
objections:
  - id: O1
    category: risk
    severity: high
    claim: "Slow clients that redial \"at once\" stay phase-locked to the gateway's 1s wheel tick, so each tick's whole cohort reconnects within milliseconds; at Tier 2 scale slow-auth arrives as per-tick handshake bursts far above the bucket's 2,500 burst, not as the ~18k/s average Review Focus 5 sizes it by."
    evidence: "Choice 1: \"a slow client closed after ≥ 3s … dials again at once, so the flood keeps its slots\"; Global Constraints: \"The gateway's timer wheel has a 1s tick and rounds deadlines up\", \"Handshake bucket defaults are 25,000/s with a burst of 2,500, and concurrency is half the loops\"; Review Focus 5: \"100k slow-auth connections ≈ 18k handshakes/s\"."
    disposition: accepted
    disposition_rationale: "redial after a uniform 0–1s (one wheel tick) (see dispositions file)"
  - id: O2
    category: scope
    severity: high
    claim: "Nothing in the plan makes the handshake limiter fire, so the spec's \"excess handshakes are closed and counted\" path, slow-auth's pre-handshake outcome and the 0.5–1.5s backoff branch are first exercised on the Tier 2 host; and startStack's new opts cannot reach the limiter, which lives in tcp.Config."
    evidence: "Probe output: \"gateways: failed 20012, handshake_rate 0\"; e2e patch: opts are applied to `cfg` (gateway.Config), then `tcfg := tcp.DefaultConfig(cfg.NumLoops)` (HandshakeConcurrency/Rate/Burst live in tcp.Config, internal/gateway/tcp/transport.go:41–52); the mutation list has no case for the `held < slowHeld` backoff or FloodPreHandshake."
    disposition: accepted
    disposition_rationale: "startStack reaches tcp.Config; in-process test with a tiny bucket: pre-handshake counted, backoff, no spin (see dispositions file)"
  - id: O3
    category: specification quality
    severity: medium
    claim: "Loadgen's garbage counts cannot say how much expensive garbage reached a DH: `attempts` counts dial starts (failed dials and attempts in flight included), and `closed` is the same for a failed DH, handshake_rate, noise_unknown_key or at_capacity; yet Review Focus 2 proposes that 6b3b gate the achieved rate on attempts, and After this plan does not carry the probe's gateway-side check to 6b3b."
    evidence: "Review Focus 2: \"the achieved rate is `flood.attempts / elapsed_s`, and 6b3b gates on it (e.g. attempts ≥ 0.95 × rate × duration)\"; `floodDial` calls `r.flood.attempt()` before dialing; FloodClosed: \"the gateway closed (FIN or RST)\"; the probe's own guard is gateway-side: \"failed + limited ≥ closed and failed > 0\"; After this plan lists the rate gate, slow-auth sizing and TIME_WAIT, not the failed-counter check."
    disposition: accepted
    disposition_rationale: "text only: 6b3b gates the delivered rate on the gateways' handshakes_total deltas (see dispositions file)"
  - id: O4
    category: implementation
    severity: medium
    claim: "The 4-instance cap counts every instance name a script ever uses, not instances running at once, so the probe already spends all four and a phased 6b3b scenario runs out even though the justification counts concurrent instances."
    evidence: "`lg_in` appends to LG_INSTANCES for any unknown name and nothing removes one; \"Why at most 4: main plus three, enough for 6b3b (legitimate + garbage, or + slow-handshake + slow-auth)\"; the probe names main, garbage, slowhs, slowauth although garbage is stopped before the other two start; test `inst: a 5th instance is refused`."
    disposition: accepted
    disposition_rationale: "names are slots for the whole script; phases reuse them; probe reuses one (see dispositions file)"
  - id: O5
    category: specification quality
    severity: low
    claim: "LG_STATUS is not per instance although the interface says it keeps its meaning for the current instance: after `lg_in X lg_stop` or `lg_stop_all` it holds the last stopped instance's status while main is current."
    evidence: "Task 2 Interfaces: \"`LG_PID`, `LG_NAME` and `LG_STATUS` keep their meaning, for the current instance\"; `lg_in` saves and restores only LG_PID and LG_NAME; existing scenarios write `\"loadgen_exit\": $LG_STATUS` (capacity.sh:302, gwloss.sh:833, smoke.sh:114/137)."
    disposition: accepted
    disposition_rationale: "lg_in swaps LG_STATUS too (see dispositions file)"
  - id: O6
    category: specification quality
    severity: low
    claim: "The plan decides, in passing and without a choice number, that the spec's \"a legitimate 20k/s storm is not rejected by the bucket\" is a flood-free test, which shapes 6b3b's scope."
    evidence: "Spec (quoted in the plan header): \"handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket\"; After this plan: \"The spec's 'a legitimate 20k/s storm is not rejected by the bucket' needs no flood\"."
    disposition: accepted
    disposition_rationale: "reading recorded as choice #121 (proposed) (see dispositions file)"
  - id: O7
    category: risk
    severity: low
    claim: "The probe's slow-client upper bound (max held ≤ 6.5s) has about 0.4s of headroom over the prototype's 6.12s on a 2-core machine running two gateways, Logic, Redis and four loadgens, and it gates every `make test-load-probe`."
    evidence: "Probe check: `d[\"flood\"][\"held\"][\"max_ms\"] <= 6500`; prototype: \"slow-auth [5530, 6120]ms\"; light Pong max under the floods 107ms; slow-handshake's held starts at loadgen's dial, before the gateway's OnOpen starts its timer."
    disposition: accepted
    disposition_rationale: "p99 <= 6.5s, max <= 7.5s (see dispositions file)"
---

## O1 — risk — high

### Claim

A slow client that the gateway closes by its timeout redials immediately. Its new connection is accepted within milliseconds, so it lands in the same 1s wheel interval as every other client of its cohort, and the wheel's round-up gives all of them the same closing tick again. The cohorts never spread out: whatever the ramp put into one second is closed in one tick and redials together, every ~5–6s, for the whole flood. For slow-auth, each tick's cohort is a burst of real Noise handshakes arriving within milliseconds. Review Focus 5 sizes slow-auth by its average (100k ≈ 18k/s against 25k/s). The bucket's burst is 2,500, and concurrency is half the loops, so it is the instantaneous burst that sets what happens.

### Evidence

- Choice 1, *Redialing*: "a slow client closed after ≥ 3s … dials again at once, so the flood keeps its slots."
- Global Constraints: "The gateway's timer wheel has a 1s tick and rounds deadlines up, so a slow connection closes 5–6s after its state began. Handshake bucket defaults are 25,000/s with a burst of 2,500, and concurrency is half the loops."
- Review Focus 5: "At Tier 2 scale (100k slow-auth connections ≈ 18k handshakes/s) it would by itself near the 25k/s bucket."
- `internal/gateway/tcp/limiter.go`: a handshake that finds the semaphore full or the bucket empty "is not started: the connection is closed as handshake_rate".

### Why this matters

Take a 100k slow-auth flood ramped at 20k/s. That gives five cohorts of ~20k, and each redials as a block once per round. Each block empties the bucket (2,500) and overflows the semaphore. Roughly 17k of each block are refused, back off 0.5–1.5s, and then use up the refill (25/ms) for most of the next second. A legitimate client that arrives in that second is refused at the limit. 6b3b would then see "legitimate clients get no `Busy`" fail, or see refusals it cannot assign. The cause would be how loadgen schedules its redials, which neither a timeout nor a real slow client population produces. Slow-handshake has the same lock-step at accept, without the crypto. In the probe (500 per mode, one loop per gateway, so concurrency 1) the bursts stay under 2,500 and the issue cannot show. That is why the dry run passed. The fix is cheap and belongs in 6b3a, because the redial policy is 6b3a's: for example, jitter the immediate redial over one wheel tick. Or 6b3b states and records the burst shape. Either way, Review Focus 5's average needs a per-tick figure next to it.

## O2 — scope — high

### Claim

The plan's tests and its probe phase never make the gateway refuse a handshake at the limit. These paths therefore have no test anywhere: the gateway closing at the limit while loadgen is flooding (the spec's "excess handshakes are closed and counted"), slow-auth's `pre-handshake` outcome, and the `held < slowHeld` branch that waits 0.5–1.5s so a refused client "never spins". The in-process harness cannot reach these paths either. The new `startStack(t, n, opts…)` passes options to `gateway.Config`, but the limiter's settings are in `tcp.Config`, and `startStack` builds that afterwards from `tcp.DefaultConfig`.

### Evidence

- The probe's prototype output: "gateways: failed 20012, handshake_rate 0".
- The e2e patch: `for _, o := range opts { o(&cfg) }` then `tcfg := tcp.DefaultConfig(cfg.NumLoops)`. `HandshakeConcurrency`, `HandshakeRate` and `HandshakeBurst` are fields of `tcp.Config` (`internal/gateway/tcp/transport.go:41–52`).
- `TestGarbageFloodIsClosedByTheGateway` reads `handshake_rate` but accepts 0. `TestSlowFloodHeldUntilTheGatewaysTimeout` asserts no outcome other than closed/timeout/answered for the `pre-handshake` case.
- Step 5's mutations cover the key_id, the garbage branch, the sources default and redialing. None removes the backoff, and none miscounts `pre-handshake`.

### Why this matters

The limiter is what the spec's handshake-flood row exists to show. If the backoff branch were wrong (say the comparison were inverted, so a client refused at the limit redialed at once), slow-auth would spin against the limiter at the host's full dial rate. Nothing on the dev machine would notice. The first evidence would be a Tier 2 run whose limiter numbers measure loadgen's spin, not the flood. Two remedies are cheap: give the probe's gateways a low `-handshake-rate`/`-handshake-burst` (the flags exist, `cmd/gateway/main.go:178–180`), or let `startStack` take `tcp.Config` options and add one in-process test where the bucket is exhausted. Both run in seconds.

## O3 — specification quality — medium

### Claim

From loadgen's counts alone, nobody can tell how much *expensive* garbage reached a gateway's DH. `attempts` is incremented before the dial, so it includes failed dials and attempts cut off at the stop. `closed` covers every way a gateway closes: a failed DH, `handshake_rate`, `noise_unknown_key`, `at_capacity`, `pending_mem`. Review Focus 2 still proposes `attempts` as 6b3b's achieved-rate gate. The probe guards this properly, gateway-side (`failed + limited ≥ closed`, `failed > 0`), but *After this plan* passes 6b3b only the attempts gate.

### Evidence

- Review Focus 2: "the achieved rate is `flood.attempts / elapsed_s`, and 6b3b gates on it (e.g. attempts ≥ 0.95 × rate × duration) before reading any gateway number as the effect of that rate."
- `floodDial`: `r.flood.attempt()` comes first, then `r.bal.Dial`.
- `FloodClosed = "closed" // the gateway closed (FIN or RST)`.
- *After this plan*: "It gates the achieved garbage rate (Review Focus 2), sizes slow-auth against the bucket (Review Focus 5), and records the gateway's TIME_WAIT (Review Focus 3)."

### Why this matters

Suppose a Tier 2 run had a stale `NOISE_KEY_ID`, or a gateway at capacity. Every garbage attempt would be closed before any crypto. That is the cheap flood the plan itself says "test[s] nothing". Yet loadgen would report 100% `closed` and pass an attempts-based rate gate, and the run would wrongly credit the gateway with surviving a 20k/s expensive flood. The measure that means something is the delta of the gateways' `handshakes_total{result="failed"}` plus `{no_slot, no_token}` over the flood window. That measure should be what 6b3b inherits as its rate gate, the way the probe already does it.

## O4 — implementation — medium

### Claim

The cap is justified by instances running at once ("main plus three"), but it is enforced on names. `lg_in` registers every unknown name for good, and stopping an instance does not free its index. A script with phases runs out of names even when no more than two instances ever run together. The probe already uses all four: `main`, `garbage`, `slowhs`, `slowauth`, with `garbage` stopped before the other two start.

### Evidence

- `lg_in`: `if ! lg_k "$inst" … LG_INSTANCES+=("$inst")`. Nothing removes an entry.
- Choice 3: "Why at most 4: main plus three, enough for 6b3b (legitimate + garbage, or + slow-handshake + slow-auth)".
- The probe's floods phase: `lg_in garbage …`, then `lg_in slowhs …`, `lg_in slowauth …`.
- Test: `inst: a 5th instance is refused`.

### Why this matters

6b3b's `floods` scenario is phased in the same way: a garbage phase, then a slow phase. If it also wants anything else under its own name, it is refused partway through a Tier 2 run. That could be a separate instance for the 20k/s legitimate storm, a heartbeat probe, or a second garbage rate. The workaround is to reuse names across phases, but the scenario writer has to discover that from a refusal. The plan should either say that names are slots for the whole script and should be reused (in the header and the example), or free a stopped instance's index. Note that freeing an index weakens Review Focus 4's argument, which depends on indexes never changing. This is a real trade-off for the user to decide.

## O5 — specification quality — low

### Claim

The interface promises a per-instance `LG_STATUS` that the code does not provide. `lg_in` swaps `LG_PID` and `LG_NAME` but not `LG_STATUS`. After `lg_in garbage lg_stop`, `$LG_STATUS` is garbage's exit status while `main` is current. After `lg_stop_all`, it is the last instance's.

### Evidence

- Task 2 Interfaces: "`LG_PID`, `LG_NAME` and `LG_STATUS` keep their meaning, for the current instance."
- `lg_in`: `LG_PIDS[$prev]=$LG_PID LG_NAMES[$prev]=$LG_NAME …`, with no LG_STATUS.
- Existing consumers write `"loadgen_exit": $LG_STATUS` (`capacity.sh:302`, `gwloss.sh:833`, `smoke.sh:114/137`).

### Why this matters

None of today's scenarios is affected, because each reads `LG_STATUS` right after its own `lg_stop`. But 6b3b will copy these `t2_field` lines. If it stops its floods (or calls `lg_stop_all`) between main's `lg_stop` and the field, its report puts a flood's exit status under the legitimate instance's name. The fix is either one line in `lg_in`, or to correct the interface text to name `LG_STATUSES` as the per-instance value.

## O6 — specification quality — low

### Claim

The plan settles what the spec's handshake-flood test means, and it does so in its closing prose rather than as a numbered choice. It splits the spec's single bullet into a flood test (heartbeats) and a flood-free test (a 20k/s storm against the bucket).

### Evidence

- The spec, as quoted in the plan's header: "handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket".
- *After this plan*: "The spec's 'a legitimate 20k/s storm is not rejected by the bucket' needs no flood: a light instance ramps at 20,000/s …".

### Why this matters

The reading is probably right: 20k/s of garbage plus a 20k/s storm cannot fit under a 25k/s bucket. But it decides 6b3b's scope and the scope of the spec's verdict. If the spec meant "a legitimate storm *during* a moderate flood", 6b3b would quietly leave out that test. Recorded as a choice (or as a spec follow-up in the rev5 dispositions file, like #97 and #101), the reading is a decision. Left in the prose, it is a default that 6b3b inherits without anyone having decided it.

## O7 — risk — low

### Claim

The probe gates every slow connection's hold at ≤ 6.5s. The prototype's worst case was 6.12s. The bound is theory (5s + one 1s tick) plus 0.5s of slack, and that slack is spent on a 2-core machine that runs two gateways, Logic, Redis and up to four loadgens. For slow-handshake, the hold starts at loadgen's dial, before the gateway's `OnOpen` starts its timer, so accept latency counts too.

### Evidence

- The probe check: `d["flood"]["held"]["max_ms"] <= 6500`.
- The prototype: "slow-handshake [5518, 6051]ms, slow-auth [5530, 6120]ms".
- `slowOnce` measures `awaitClose(ctx, nc, time.Now())` from the dial, or from after the handshake.

### Why this matters

`make test-load-probe` is part of the "stays green" constraint. A max-based gate with 0.4s of headroom on a contended 2-core box can fail now and then without any regression, and a flaky gate teaches people to ignore it. Options: gate the p99 instead of the max, or widen the bound to 7s (it still catches a gateway that closes a tick late).

## Explicitly not objecting to

- **Flood modes in the existing `Runner` rather than a separate binary**: the plan names the alternative, and sharing `Env`, the balancer and the control API is clearly cheaper. "One mode per process" is preserved.
- **Expensive garbage only (a valid key_id, random Noise payload)**: I checked `internal/gateway/tcp/conn.go:119–135`. The key_id check runs before `lim.acquire()`, so cheap garbage really does cost the gateway no token and no DH, and the plan's argument holds.
- **TIME_WAIT on the gateway at 20k/s (Review Focus 3)**: the gateway closes with an ordinary FIN (`gc.Close()`). Kernel overflow of `tcp_max_tw_buckets` only drops TIME_WAIT entries, and the balancer's `IP_BIND_ADDRESS_NO_PORT` keeps loadgen's ports per (source, gateway). The plan already hands 6b3b the `ss -s` record.
- **`-source-net` per instance**: Linux answers on 127/8, so it needs no setup. It separates the port budgets the way #69 requires, and it keeps the populations apart in `ss`.
- **Open-loop garbage with catch-up after a stall**: back-to-back catch-up avoids coordinated omission. The bursts it produces are still a flood, which the gateway's limiter is meant to absorb. Review Focus 2 already discloses it.
- **Flood instances defaulting to the legitimate instance's CPUs (`LG_CPUS` → `T2_CPUS_LOADGEN`)**: on Tier 2 this could inflate the legitimate instance's Pong RTT. But `LG_CPUS` is exposed, and choosing the split belongs to 6b3b. In this plan the Pong RTT under floods is recorded, not gated.
- **The probe running at one loop per gateway (concurrency 1)**: this explains why `no_slot` never fired and why O1 cannot show on the dev machine. It is the probe's existing setup, not a 6b3a choice. O1 and O2 cover the consequences.
- **The `lg_start` running-instance guard breaking existing scenarios (Review Focus 1)**: the plan grepped every `lg_start`/`lg_stop` pair and pins the guard with a test. I found no counter-example.
- **`$(lg_in x …)` registering only in a subshell (Review Focus 4)**: a name registered only in the subshell gets the index right after the parent's last instance. That index is never a running parent instance, so a typo reads an unused port (null) and never another instance's stats.
