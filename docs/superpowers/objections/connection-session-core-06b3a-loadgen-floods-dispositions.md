# Dispositions: Plan 6b3a review (2026-10-16)

Objections: `connection-session-core-06b3a-loadgen-floods.md`. Decided by the user, one at a time.

## O1 — accepted: a timeout-closed slow client redials after a uniform 0–1s

**Decision:** a slow client that the gateway closed after ≥ `slowHeld` (3s) waits a uniform 0–1s (one wheel tick) and then redials. It no longer redials at once. A client closed sooner keeps the SDK's 0.5–1.5s wait.

**Rationale:** with an immediate redial, every connection the wheel closes in one tick is accepted again within milliseconds. That puts it back in the same tick interval, and the wheel's round-up gives it the same closing tick next round. So the cohorts stay phase-locked for the whole flood. At Tier 2 scale, slow-auth would then arrive as per-tick blocks of handshakes (≈ the ramp rate, e.g. 20k, within milliseconds), far above the bucket's 2,500 burst and the semaphore. Legitimate clients would be refused because of loadgen's scheduling, not because of the slow-client flood the spec describes. A 0–1s jitter spreads each cohort over a tick. A slot is empty for 0.5s on average out of a ~5.5s hold, so the flood still holds ~90% of `-clients`.

**Plan changes to apply:**
- `flood.go`: in `slow`, a close after ≥ `slowHeld` waits `mrand.N(time.Second)` (ctx-aware) instead of `continue`. Update the `slowHeld` and `slow` comments: the redial waits up to one wheel tick so cohorts do not stay phase-locked to the tick (Plan 6b3a, O1).
- Choice 1, *Redialing*: "dials again after a uniform 0–1s (one wheel tick), so cohorts closed in the same tick do not redial together, and the flood keeps ~90% of its slots", plus the reason.
- Review Focus 5: next to the average (~18k/s for 100k), state that an immediate redial would have made per-tick bursts. With the jitter, the arrival rate is the average spread over each tick, and 6b3b still records `handshakes_total{result=no_slot|no_token}`.
- `TestSlowFloodHeldUntilTheGatewaysTimeout`: the "5 open again" wait accounts for the ≤ 1s jitter (its 5s bound already does; re-check). Add a mutation to Step 5: the jitter removed (back to `continue`). If no test catches that, add one: the redial times of a cohort closed in one tick spread over ≥ 200ms, or the equivalent.
- The probe's slow check (`open ≥ 450` after 500 closed) still holds at ~90%. Re-run it and record the numbers.
- Roadmap #119's text: the redial rule.

## O2 — accepted: an in-process test where the handshake limiter fires

**Decision:** `startStack` can also change each gateway's `tcp.Config`. A new in-process test runs slow-auth against a gateway with a tiny handshake bucket, so the limiter refuses most handshakes. The probe is unchanged.

**Rationale:** the plan's tests and the probe never make the limiter fire (the dry run had `handshake_rate 0`). Three things were therefore first exercised on the Tier 2 host: the gateway closing at the limit under a flood (the spec's "excess handshakes are closed and counted"), slow-auth's `pre-handshake` outcome, and the 0.5–1.5s wait that keeps a refused client from spinning. The current `opts` reach only `gateway.Config`, and the limiter lives in `tcp.Config`. An in-process test costs seconds. A probe step would need its own stack.

**Plan changes to apply:**
- `e2e_test.go`: `startStack`'s options can reach `tcp.Config` too. Either a second option type (e.g. `func(*tcp.Config)`, applied after `tcp.DefaultConfig`), or one option over a small struct holding both configs. Pick whichever reads better in the existing helper, and keep current callers unchanged.
- `flood_test.go`: a new test (e.g. `TestSlowAuthRefusedAtTheLimitBacksOff`). It uses one gateway with `HandshakeRate` ≈ 5/s and `HandshakeBurst` 1, and slow-auth with `Clients` ≈ 20, run for a few seconds. Expected: `outcomes["pre-handshake"] > 0`, and the gateway's `admission_rejects_total{reason="handshake_rate"}` (and `handshakes_total{result="no_token"}`) > 0. No spin: attempts stay bounded by about Clients × (elapsed / 0.5s) plus the handshakes admitted. No `answered`, no `error`.
- Step 5: add a mutation in which the backoff is skipped after a short hold (e.g. `held >= slowHeld` → `true`, or the wait removed). The new test must fail on the attempts bound. Also a mutation that miscounts `ErrClosedBeforeReply` as `handshake`, caught by the `pre-handshake > 0` check.
- Review Focus or *After this plan*: the limiter path now has an in-process test. 6b3b's Tier 2 run is its first test at scale.

## O3 — accepted: 6b3b gates the delivered garbage rate on the gateways' counters

**Decision:** a text change only. 6b3b's gate on the delivered garbage rate is the gateways' `handshakes_total{result="failed"}` delta over the flood window, plus `{no_slot, no_token}`. That is the probe's own check, carried to Tier 2. Loadgen's `attempts` / `elapsed_s` and its outcomes stay a check on loadgen's own schedule (did loadgen keep up). They do not show what reached a DH.

**Rationale:** `attempts` is counted before the dial, so it includes failed dials and attempts cut off at the stop. `closed` is the same whatever the gateway did: a failed DH, `handshake_rate`, `noise_unknown_key`, `at_capacity` or `pending_mem`. A stale `NOISE_KEY_ID` or a full gateway would turn the run into the cheap flood the plan says "test[s] nothing", and still pass an attempts-based gate.

**Plan changes to apply:**
- Review Focus 2: the achieved *schedule* rate is `flood.attempts / elapsed_s` (loadgen kept up). The *delivered* expensive rate is the gateways' `handshakes_total{failed}` (+ `no_slot`, `no_token`) delta over the window. 6b3b gates on the latter (e.g. ≥ 0.95 × rate × duration) before it reads any gateway number as the effect of that rate. `closed` also covers closes made before any crypto.
- *After this plan*, the 6b3b bullet: "gates the delivered garbage rate on the gateways' handshake counters (Review Focus 2), as the probe's `failed + limited ≥ closed` check does".
- No code change.

## O4 — accepted: instance names are slots for the whole script; phases reuse them

**Decision:** the cap stays at 4 *names*, and indexes never change. That keeps Review Focus 4's argument intact: a name registered only in a subshell takes an index no parent instance holds. The plan says plainly that instance names are slots for the whole script, and that a phased scenario reuses a stopped instance's name for its next loadgen. The probe shows this by reusing one.

**Rationale:** the cap was justified by instances running at once ("main plus three"), but it is enforced on names, and the probe already uses all four. A phased 6b3b scenario that names instances per phase would be refused partway through a Tier 2 run. Freeing indexes would reopen the subshell problem. Documenting reuse costs nothing.

**Plan changes to apply:**
- Choice 3 (*Why at most 4*), the `loadgen.sh` header and `lg_in`'s comment: at most 4 instance names per script, never freed (ports follow the order they are first named). Name instances by role (e.g. `flood`, `flood2`), not by phase, and reuse a stopped one. Each `lg_start` still takes its own NAME, so result files stay apart.
- The probe's floods phase: the garbage loadgen runs on instance `flood` (`lg_in flood lg_start garbage …`), and slow-handshake reuses `flood` once garbage is stopped (`lg_in flood lg_start slowhs …`), next to `flood2` for slow-auth. Three instance names in all, so one slot stays free for later phases. Its checks read through the instance in use at that point (`lg_in flood lg_json …`).
- A `loadgen_test.sh` check: a stopped instance can be started again under a new NAME (the guard allows it), and `lg_json` then reads the new NAME's result.
- *After this plan*, the 6b3b bullet: name instances by role and reuse them across phases.

## O5 — accepted: lg_in swaps LG_STATUS too

**Decision:** `lg_in` saves and restores `LG_STATUS` through `LG_STATUSES`, the way it already does `LG_PID` and `LG_NAME`. After `lg_in X lg_stop` or `lg_stop_all`, `$LG_STATUS` is the current instance's status again (main's, at the top level).

**Rationale:** the interface says `LG_STATUS` keeps its meaning for the current instance, but `lg_in` did not swap it. 6b3b will copy `"loadgen_exit": $LG_STATUS`, and stopping a flood after main's `lg_stop` would have put the flood's exit status there.

**Plan changes to apply:**
- `lg_in`: on entry `LG_STATUSES[$prev]=$LG_STATUS` and `LG_STATUS=${LG_STATUSES[$inst]:-}`. On exit, the same the other way round. `lg_stop` keeps writing `LG_STATUSES[$LG_INST]`.
- `loadgen_test.sh`: after main's `lg_stop` and then `lg_in flood lg_stop`, `$LG_STATUS` is main's. Make the two statuses differ if the fake loadgen can (e.g. an exit-code argument), so the check can fail.

## O6 — accepted: the reading of the spec's 20k/s storm clause becomes choice #121

**Decision:** a new choice, #121 (proposed with this plan). The spec's handshake-flood bullet holds two tests. (1) Garbage handshakes from many sources leave heartbeats answered on time. (2) A legitimate 20k/s storm is not rejected by the bucket. The second is the bucket's sizing check and runs without garbage: 20k/s of garbage plus a 20k/s storm cannot fit under a 25k/s bucket. 6b3b runs (2) as a light instance ramping at 20,000/s, and judges it by `failed["pre-handshake"]` and the gateways' `handshake_rate` / `handshakes_total{no_slot, no_token}`.

**Rationale:** the reading decides 6b3b's scope and how the spec's verdict is interpreted, and the plan made it only in its closing prose. As a numbered choice, it is a decision 6b3b can cite.

**Plan changes to apply:**
- *Implementation choices*: a fifth item, **#121 (proposed)**, with the reading above and the arithmetic behind it.
- *After this plan*: the 6b3b bullet cites #121 instead of stating the reading.
- Roadmap: #121 is listed with #119 and #120 under the plan 6 entry, and gets its status after this review.

## O7 — accepted: the probe gates the slow holds' p99 at 6.5s and their max at 7.5s

**Decision:** the probe's slow check becomes `held.p99_ms ≤ 6500` (theory: 5s + one 1s tick + 0.5s) and `held.max_ms ≤ 7500`. The p50 floor stays at ≥ 5000.

**Rationale:** the prototype's max was 6.12s against a 6.5s max gate, on a 2-core machine running two gateways, Logic, Redis and up to four loadgens. Slow-handshake's hold also includes accept latency. A max gate with 0.4s of headroom can flake on `make test-load-probe` with no regression behind it. The p99 still catches a gateway whose timeouts drift. A 7.5s max still catches one that closes a whole tick late.

**Plan changes to apply:**
- The probe's `$inst: every connection held 5–6.5s` check: the expression is `… d["flood"]["held"]["p50_ms"] >= 5000 and d["flood"]["held"]["p99_ms"] <= 6500 and d["flood"]["held"]["max_ms"] <= 7500`. The label becomes "held p50 ≥ 5s, p99 ≤ 6.5s, max ≤ 7.5s, then closed by a gateway". The header comment and the expected output change to match.
- The `slow:` echo line prints p50/p99/max.
- O1's jitter does not change `held`, which runs from the dial or the handshake to the gateway's close.
