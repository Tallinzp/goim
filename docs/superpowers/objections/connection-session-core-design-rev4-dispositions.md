---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design-rev4.md
objections:
  - docs/superpowers/objections/not-opus/connection-session-core-design-rev4-gpt.md
  - docs/superpowers/objections/not-opus/connection-session-core-design-rev4-review-minimax.md
date: 2026-10-04
decision_maker: user
status: decided (2026-10-07) — every entry is decided by the user; accepted changes are applied to the rev5 spec (2026-10-03-connection-session-core-design-rev5.md)
guiding_principle: new system; take the smallest fix that closes the failure, and avoid overdesign
---

# Disposition record — Connection & Session Core, revision 4

Two outside reviews of rev4 came in together. This file triages them: each objection was checked against the rev4 spec text, and gets a **proposed** disposition with a rationale and the spec changes it would imply. The user decides each one. Nothing has been applied to the spec.

**Numbering.** The two reviews number their points independently, so references use a reviewer prefix:

- *R4G-O<n>*: `rev4-gpt.md`, objections O1–O12.
- *R4M-<n>*: `rev4-review-minimax.md`, sections 1–17.

Where both reviews raise the same point, it is triaged once, under the more precise version of the claim.

## Triage summary

| Ref | Topic | Reviewer severity | Verified? | Proposed | Change size |
|---|---|---|---|---|---|
| R4M-1 | `rev:{uid}` TTL shorter than a connection's life → revoked session restored | critical | **Yes**, with a narrower trigger | **DECIDED: ACCEPT** (rev5) | one TTL value |
| R4M-3 | `gwepochs` score vs lease key not atomic → live gateway swept, no `missing` | high | **Yes**, narrow | **DECIDED: ACCEPT** (rev5) | one `EXISTS` in the sweeper; `EpochEnded` deletes the lease |
| R4M-2 | Reconcile vs lease check not atomic | high | Yes (already open point 1) | **DECIDED: ACCEPT**, wording (rev5) | closes open point 1 |
| R4M-5 + R4G-O9 + R4M-4 (skew part) | Clock-skew assumption implicit and unmonitored | high | Yes | **DECIDED: ACCEPT**, contract + metric (rev5) | one field on `GatewayLease` |
| R4G-O11 | `Reconnect{jitter}` payload undefined | medium | Yes | SUPERSEDED by R4H-O3 (rev5) | — |
| R4G-O12 | 0–20s jitter distribution unstated | high | Yes | **DECIDED: ACCEPT** (rev5) | one word |
| R4G-O5 | "Transport interface" claimed but not defined | medium | Yes | **DECIDED: ACCEPT** (rev5) | one paragraph |
| R4G-O6 | Noise key rotation has no runbook | high | Yes | **DECIDED: ACCEPT** (rev5) | one paragraph |
| R4G-O7 | gnet loop claim checked against `dev`, not a tag | medium | Yes | **DECIDED: ACCEPT** (rev5) | wording |
| R4M-11 | "Source of truth" used for both gateway and Redis | low | Yes | **DECIDED: ACCEPT** | wording |
| R4M-17 | SLO "gateway death" timestamp undefined | low | Yes | **DECIDED: ACCEPT** | wording |
| R4M-7 | Graceful drain not tested against the real NLB | medium | Partly | **DECIDED: ACCEPT**, test only | assertions on an existing test |
| R4M-9 | `gwconns` single-shard load not measured | low | Yes | **DECIDED: ACCEPT**, test only | measurements on existing tests |
| R4M-6 | 150s heartbeat cost at other values unknown | medium | n/a (trade-off) | **DECIDED: ACCEPT**, test only | one more load-test point |
| R4M-8 + R4G-O2 | 250k cap asserted, not measured | high | Partly (load test (a) covers it) | **DECIDED: ACCEPT**, wording | one sentence |
| R4M-4 (ordering part) | Order logins by Redis arrival instead of `recv_ms` | medium | — | **DECIDED: REJECT** | — |
| R4G-O1 | device_id uniqueness trusted to the SDK | high | No | **DECIDED: REJECT** | optional sentence |
| R4G-O3 | Replace Noise with TLS on mobile | medium | — | **DECIDED: REJECT**, record rationale | one sentence |
| R4G-O4 | Reconcile as TTL-refresh only, no compare-and-set | high | No | **DECIDED: REJECT** | — |
| R4G-O8 | Refused re-queues amplify overload | high | No | **DECIDED: REJECT**, clarify | one sentence |
| R4G-O10 | Stalled lease pool → healthy gateway swept, circular | high | No | **DECIDED: REJECT** | — |
| R4M-10 | Formal invariant list mapped to tests | medium | — | **DECIDED: DEFER** to the plan | — |
| R4M-12 … R4M-16 | Endorsements (no session version, no lock, no reverse mapping, Logout fence, epochs) | — | — | **DECIDED: NO ACTION** | — |

Net effect if accepted as proposed: two real defects fixed (R4M-1, R4M-3), one skew metric added, several under-specified items (`Reconnect{jitter}`, jitter distribution, transport seam, Noise key runbook) pinned down, and open point 1 closed. No new key, RPC, lock or state machine.

---

## R4M-1 — revocation marker can expire before the revoked connection is closed — DECIDED: ACCEPT

> **Decision (2026-10-07):** accepted as proposed (30d TTL, Option B rejected) and applied to the rev5 spec. The user asked that the tests fit the rev5 test execution model (R4H-O11): the `rev:` TTL equals the `sess:` TTL and is not separately configurable; a Tier 1a Lua test checks the TTL on every marker write; a Tier 1b integration test uses 2s devtoken tokens and the profile's 9s gateway cut-off, so the old TTL would have expired during the outage, and asserts the connection is closed with `REVOKED`, not restored.

**Claim.** `rev:{uid}` has TTL = max token lifetime + margin, but an authenticated connection outlives its token. If the marker expires before Reconcile closes the revoked connection, rule 1 no longer fires and rule 4 restores it.

**Verification.** Confirmed, but the trigger is narrower than the review's scenario. In the normal case the first Reconcile pass after the Revoke returns `not_owner` (`REVOKED`) by rule 1, and the gateway closes the connection locally, which can't be lost. Resurrection needs a live connection whose Reconcile entry is **not processed by Logic for longer than the marker TTL** (≥ 1h). Concrete path:

1. A gateway is cut off from Logic (or Logic is down) for more than 1h + margin; its connections stay up, because heartbeats are gateway-local.
2. `Revoke` writes the marker and deletes the field. The Kick gets one attempt only, because the target's lease is dead (R2-O6), and fails.
3. The sweeper deletes the gateway's other entries.
4. The marker expires. Connectivity returns, the renewal reports `missing`, and the triggered pass hits rule 4 for the revoked connection: **restored**.

A sustained overload in which every pass refuses the entry for over an hour has the same effect. The root cause is that the R1-O10 TTL rationale ("any token old enough to be revoked has expired anyway") covers logins only. Reconcile rule 1 checks `tok_iat` of connections that can be arbitrarily older than any token's lifetime.

**Proposed disposition.** Set the `rev:{uid}` key TTL to the `sess:{uid}` safety-net TTL (30d), refreshed on every marker write. Reject the review's Option B (a bounded authenticated-connection lifetime with forced reauth).

**Rationale.** The marker only needs to outlive the gap between the Revoke and the first Reconcile pass that processes the connection. The 30d `sess:` TTL is already the horizon past which the contract makes no promises. The cost is memory for revoked users only: one small hash each, refreshed only when a Revoke happens. Option B adds a new lifetime, a forced reconnect of every connection every Y hours, and a new client behaviour, all to fix a TTL value.

**Spec changes to apply:**
- *Redis schema, `rev:{uid}`:* TTL becomes "30d (the `sess:{uid}` safety net), refreshed on every marker write". Add the reason: Reconcile rule 1 checks connections whose token may be far older than any token's lifetime, so the marker must outlive the longest time a revoked connection can go without a Reconcile pass, not just the token.
- *Consistency contract, revoked-owner row:* the bound holds while some pass processes the connection within 30d.
- *Tests:* a gateway cut off from Logic receives a Revoke for one of its connections (Kick dropped). Expire the marker at the old TTL in a fast-clock run, or simply assert the new TTL. Restore connectivity: the triggered pass closes the connection with `REVOKED` and does not restore it.

---

## R4M-3 — `gwepochs` score and lease key are written separately — DECIDED: ACCEPT

> **Decision (2026-10-07):** accepted as proposed and applied to the rev5 spec. Tests follow the rev5 tiers: the stale-score skip is a Tier 1a Lua test; the `ZADD`-failure, mid-sweep key re-add and `EpochEnded` cases are Tier 1b integration tests under the timing profile (9s for the 90s failure).

**Claim.** `GatewayLease` does `SET gwlease… GET` and a `ZADD gwepochs` as two commands on different slots. If the `SET` succeeds and the `ZADD` fails, the sweeper sees a stale score for a live gateway.

**Verification.** Confirmed, and the consequence is worse than the review says. The sweeper picks epochs by `gwepochs` score alone; it never checks the lease key. If a live gateway's `ZADD`s fail for 2 × TTL while its `SET`s succeed, the sweeper sweeps it. Because the lease key was **present** all along, the renewal reports `live`, not `missing`, so no triggered pass runs. The sessions stay missing for up to one periodic cycle (~10 min), not ~1 min. The R3-O3 abort check, which re-reads the score, doesn't catch it either. The trigger is narrow: the mass-stale guard holds if `gwepochs` is unreachable for everyone, so it needs one renewal path whose `ZADD` fails while its `SET` succeeds. But the fix is cheap and turns an implicit assumption into a guarantee.

**Proposed disposition.** The lease key is the authoritative liveness predicate (it already is, in the validity rule), and `gwepochs` is only an index of candidates. The sweeper sweeps an epoch only if `gwlease:gw:{epoch}` is absent.

**Rationale.** One `EXISTS` per candidate epoch, and there are only a handful of epochs. It also makes R3-O3 airtight: every sweep now implies an absent lease key, so the renewal that ends a wrongful sweep always reports `missing` and triggers the restore.

**Spec changes to apply:**
- *Redis schema, `gwepochs`:* purpose becomes "index of candidate epochs for the sweeper; advisory: a stale score never proves an epoch dead".
- *Sweeper:* after claiming, check `EXISTS gwlease:gw:{epoch}`. If the key is present, release the claim (`DEL sweep:`) and skip. **Abort on renewal:** between batches, abort if the score is newer than the claimed score **or the lease key exists**.
- *Logic → `EpochEnded`:* `DEL` the epoch's lease key, so an ended epoch passes the new check at once. Its entries become invalid immediately, which is correct: the gateway has drained.
- *Races handled, #12:* add a renewal whose `SET` succeeds and `ZADD` fails: the sweeper skips the epoch because the lease key exists.
- *Tests:* fail the `ZADD` for 90s while the `SET` succeeds: nothing is swept. Remove the lease key mid-sweep without changing the score, then re-add it: the sweeper aborts. An epoch that sent `EpochEnded` is swept without waiting for its TTL.

---

## R4M-2 — Reconcile's lease check is not atomic with its write — DECIDED: ACCEPT (wording; closes open point 1)

> **Decision (2026-10-07):** accepted as proposed and applied to the rev5 spec; open point 1 removed. The integration test is Tier 1b under the timing profile.

**Claim.** Logic checks lease liveness outside Lua, so the lease can change between the check and the compare-and-set.

**Verification.** Confirmed. This is rev4's open point 1. The worst case: rule 4 sees the stored entry's lease as absent (its gateway G42 is only starved), and overwrites a **newer** live session S2 with an older one S1 whose Kick was lost. It self-heals. Rule 4 fired because G42's lease key was absent, so G42's next renewal reports `missing`. Its triggered pass then applies rule 3 to S2: the stored S1's gateway is live, S2 is newer, so S2 is written back and S1 is kicked, all within ~1 min. In the other direction (lease observed live, expires right after), rule 3 decides by ordering, which doesn't depend on liveness.

**Proposed disposition.** State in the body that the lease check is an observation, and how a write made on a stale observation is corrected. Remove open point 1.

**Spec changes to apply:**
- *Logic → Reconcile handling, after the compare-and-set paragraph:* "The lease check is an observation and can be stale when the compare-and-set runs. A rule-4 restore over an entry whose gateway was only starved is corrected by that gateway's own triggered pass (its lease key was absent, so its renewal reports `missing`), which applies rule 3. The lease key stays the authoritative liveness predicate."
- *Consistency contract, "Live connection missing from Redis" row:* add the cause "overwritten on a stale lease observation", with bound ≤ ~1 min after the owner's `missing` reply.
- *Open points:* remove item 1.
- *Tests:* starve G42's lease (key removed) while its newer session is stored; deliver G17's older entry with a lost Kick. Rule 4 overwrites, then G42's triggered pass restores S2 and kicks S1.

---

## R4M-5 + R4G-O9 (+ skew part of R4M-4) — clock-skew assumption is implicit — DECIDED: ACCEPT (contract + metric)

> **Decision (2026-10-07):** accepted as proposed and applied to the rev5 spec. Tests follow the rev5 tiers: the skew computation is Tier 1a; the +2s (ghost repaired) and −5s (logins `EXPIRED`, clients log in elsewhere — milder since R4H-O2 made `EXPIRED` a retryable reject) cases are Tier 1b with a fake gateway clock.

**Claim.** Fences (b) and (c) compare gateway wall-clock stamps with Redis `TIME`, budgeting 1s of skew. An NTP incident or step could exceed it, and nothing would notice.

**Verification.** Confirmed: the budget is stated but not monitored. I traced what a violation actually breaks:

| Skew | Effect | Severity |
|---|---|---|
| Gateway **ahead** of Redis by > 1s | Fence (b)/(c) releases a Logout before a late Login or Reconcile write can still land → a **ghost** of a closed connection | Bounded: the conn_id is no longer tracked, so the delete phase removes it at the end of the next pass (≤ 1 cycle). It can't override a newer session: Logout is exact-match and the login table still orders writes. |
| Gateway **behind** Redis by ≳ `login_deadline` minus RPC latency | Every Login via that gateway is `EXPIRED`; its clients can't log in | **Availability loss on one gateway.** The review missed this one. |
| Two gateways skewed by more than the gap between two device logins | The wrong device wins a same-slot race (race 3, rule 3) | User sees a spurious `KICKED_BY_OTHER_DEVICE`; no state corruption |

So skew degrades into bounded ghosts or a visible login outage, never into silent corruption. It still has to be observable.

**Proposed disposition.** Make the assumption an explicit contract (`|gateway clock − Redis TIME| ≤ 1s`), list the consequences above, and measure skew in-band on the lease path. Keep the 1s budget. Raising it would delay every fenced Logout and wouldn't close the gap for larger steps.

**Rationale.** Node-level NTP metrics don't cover a managed Redis's clock. The lease RPC already runs every 10s per gateway on a dedicated, low-latency path, so it can carry the measurement for one field and one `TIME` call.

**Spec changes to apply:**
- *Gateway → Lease:* each `GatewayLease` call carries the gateway's wall clock `gw_now_ms`.
- *Logic → GatewayLease:* read Redis `TIME` on the lease key's shard. Export `clock_skew_ms{gw} = redis_time − gw_now_ms` (RPC latency puts a few ms of noise on it). Alert at |skew| > 500ms, half the budget.
- *Login Lua, Assumption paragraph:* replace with the explicit contract and the consequence table above.
- *Observability:* add `clock_skew_ms` per gateway and its alert.
- *Tests:* a unit test of the skew computation. Integration: offset one gateway's clock by +2s (a fake clock in the gateway). The alert fires, and a ghost created by a late Login write is removed by the next pass's delete phase.

---

## R4G-O11 — `Reconnect{jitter}` payload undefined — SUPERSEDED by R4H-O3

> **Superseded (2026-10-07):** decided by the user as R4H-O3 and applied in the rev5 spec: `Reconnect{jitter_ms}`, uniform `[0, jitter_ms]`, default 5s, reason `Reconnect from drain`.

**Claim.** `Kick` has a reason enum and `Busy` has a duration, but `Reconnect{jitter}` has no defined meaning.

**Verification.** Confirmed. The spec names the command and its use in drain, nothing else.

**Proposed disposition.** `Reconnect{jitter_ms}`: the client closes the connection, waits a uniform random time in `[0, jitter_ms]`, then connects with `reconnect_reason = Reconnect from drain`. This is not an unexpected loss: the 0–20s wait does not apply. Failures after that follow the normal rules. The gateway sets `jitter_ms` (configurable, default 5s). The drain already spreads load by sending batches over ~60s, so the jitter only smooths each batch.

**Spec changes to apply:** *Client protocol, Commands:* define `Reconnect{jitter_ms}` as above. *Client SDK rules → Reconnect:* add the rule. *Gateway → Graceful drain:* name the default. *Tests:* SDK unit test: after `Reconnect`, the first attempt falls within `[0, jitter_ms]` and carries the drain reason.

---

## R4G-O12 — jitter distribution unstated — DECIDED: ACCEPT

> **Decision (2026-10-07):** accepted and applied to the rev5 spec. R4H-O2/O3 had already made the 1–3s, 0–1s and `[0, jitter_ms]` waits uniform; this adds "uniformly distributed" to the 0–20s storm wait and the `Busy` 1–3s value, and a Tier 1a SDK bucket test over all four waits.

**Claim.** "A random 0–20s" doesn't say uniform, but the 10k/s arithmetic assumes it.

**Verification.** Confirmed. R1-O7's analysis assumed uniform; the spec text doesn't say so.

**Spec changes to apply:** *Client SDK rules → Reconnect:* "wait a **uniformly distributed** random 0–20s". Same wording for the 1–3s pre-handshake retry and `Busy` jitter. *SDK unit test:* the distribution is uniform (bucket check).

---

## R4G-O5 — "designed behind a transport interface", but no interface is defined — DECIDED: ACCEPT

> **Decision (2026-10-07):** Accepted and applied to rev5, adjusted for R4H-O1: both transports carry the same 6-byte-header frames, so the seam is at decoded frames `(cmd, req_id, body)`; wss carries one frame per binary message and a `Length` mismatch is a bad frame. Test: Tier 1a state-machine tests against a fake transport.

**Claim.** The spec says the web transport is "designed now behind a transport interface", but no interface appears. The outer framing is TCP-shaped while wss is message-shaped.

**Verification.** Confirmed: the only web material is constraints (wss, TLS at the LB, one connection per browser profile, no Noise).

**Proposed disposition.** Define the seam in one paragraph, without designing the web slice.

**Spec changes to apply:** *Gateway*, new item **Transport seam**: "The gateway core (state machine, timeouts, heartbeat, admission except the Noise limit, Login/Logout/Reconcile, Kick) works on whole plaintext `Envelope`s. A transport turns a connection into: an open event, a stream of received Envelopes, `Send(Envelope)` and `Close()`. **TCP transport:** outer `[u32 length]` framing plus Noise NK; its `HANDSHAKE` state is the Noise handshake. **wss transport (later slice):** one binary WebSocket message per Envelope, TLS from the LB, no outer length and no Noise; its `HANDSHAKE` state is the HTTP upgrade. Whether wss runs on gnet or a separate listener is decided in the web slice." *Tests:* none new; the state-machine unit tests run against a fake transport.

---

## R4G-O6 — Noise static-key rotation has a name, not a runbook — DECIDED: ACCEPT

> **Decision (2026-10-07):** Accepted and applied to rev5, with one addition found while checking against R4H-O1: the handshake payload is `key_id(2) || noise_msg`, because the gateway needs the key ID before processing the NK message; the prologue is header + `key_id`. `noise_unknown_key` is a pre-handshake close (retryable reject). Tests are Tier 1a.

**Claim.** JWKS has a rotation runbook. The pinned Noise key has only "a key ID for rotation", with no distribution, overlap or compromise procedure.

**Verification.** Confirmed. Because the key is pinned in app builds, rotation is bounded by app release cycles, and that is worth stating.

**Proposed disposition.** A runbook paragraph parallel to the JWKS one:
- **Server key set:** gateways load `{key_id → private key}` from a Kubernetes Secret and select the key by the `key_id` in handshake frame 1. An unknown or retired `key_id` closes the connection before the handshake and is counted as `noise_unknown_key`.
- **Rotation:** (1) add the new key to the Secret and roll the gateways; (2) ship app builds that pin both the current and the new key; (3) remove the old key only when every app version that pins only the old key is below the supported-version floor.
- **Compromise:** NK gives forward secrecy for recorded traffic. Session keys mix `ee`, so a stolen static key doesn't decrypt past sessions, provided no application data rides in the first handshake message. But a stolen key lets an attacker impersonate the server to apps that pin it. Response: emergency app release with a new key, then remove the compromised key from the Secret. App versions that pin only that key then fail the handshake (5 pre-handshake retries, then backoff) and must upgrade through the app's update path, which is outside this protocol.

**Spec changes to apply:** *Decisions → Client encryption:* reference the runbook. *Gateway:* add the runbook above. *Client protocol:* the first handshake message carries no application payload. *Observability:* `noise_unknown_key` rejects. *Tests:* a handshake with either of two configured key IDs succeeds; an unknown key ID is closed before the handshake and counted.

---

## R4G-O7 — gnet loop-ownership claim checked against `dev` — DECIDED: ACCEPT

> **Decision (2026-10-07):** Accepted and applied to rev5: pinned `github.com/panjf2000/gnet/v2 v2.10.0` (latest release on 2026-10-07). Verified against that tag: `Conn.EventLoop()` and `EventLoop.Execute` exist; `OnOpen` runs on the owning loop on both accept paths (`accept0` → `Trigger(el.register)` on the target loop; reuseport `accept` → `register0`); `Engine` cannot list loops. Any upgrade is gated by the Tier 1a `-race` loop-ownership and loopState tests.

**Claim.** "Checked against gnet's `dev` source" isn't a release; the claim may not hold at the version actually used.

**Verification.** Confirmed: the spec requires "≥ v2.8.0" but cites `dev`.

**Proposed disposition.** Pin an exact gnet tag in `go.mod`, verify the `OnOpen`-on-owning-loop claim and `Conn.EventLoop()` against that tag's source, and cite the tag. The existing `-race` loop-ownership test is the guard for upgrades.

**Spec changes to apply:** *Gateway:* "gnet **vX.Y.Z (exact tag, pinned)**; checked against that tag's source: …". *Verification:* the loop-ownership test must pass before any gnet upgrade.

---

## R4M-11 — "source of truth" means two things — DECIDED: ACCEPT (wording)

> **Decision (2026-10-07):** Accepted and applied to rev5 (*Decisions → Source of truth*; `sess:{uid}` purpose is "slot ownership record"); the epoch-liveness owner notes R4M-3's advisory `gwepochs`.

**Claim.** Decisions says "the gateway is the source of truth for which connections exist". The schema calls `sess:{uid}` "online mark (source of truth in Redis)".

**Proposed disposition.** Name the three facts and their owners once, in *Decisions → Source of truth*:
- **Does this connection exist?** The gateway.
- **Which connection owns this slot?** `sess:{uid}`, reconciled to the gateway.
- **Is this gateway epoch alive?** `gwlease:gw:{epoch}`.

In the schema, `sess:{uid}`'s purpose becomes "slot ownership record".

---

## R4M-17 — "gateway death" timestamp undefined for the SLO — DECIDED: ACCEPT (wording)

> **Decision (2026-10-07):** Accepted and applied to rev5 (crash row of the SLO table; load test (b1) measures from `T_death`).

**Claim.** "Within 30s of gateway death" needs a reproducible start time.

**Verification.** Partly addressed already: production uses `offline_ms` plus the dead-epoch cross-check. The load test (b1) has no stated start point.

**Spec changes to apply:** *Success criteria, crash row:* "gateway death = the moment loadgen issues SIGKILL (in tests); in production, approximated by the dead-epoch cross-check". *Load test (b1):* loadgen records the SIGKILL time as `T_death`.

---

## R4M-7 — graceful drain needs a test against the real NLB — DECIDED: ACCEPT (test only)

> **Decision (2026-10-07):** Accepted and applied to rev5 as assertions on the Tier 3 rolling-restart test. With `TARGET=local` the LB is endpoint-following (kube-proxy IPVS); the real NLB deregistration delay is listed as `not_covered`.

**Claim.** Drain depends on NLB deregistration and on internal DNS still resolving the pod. The compose rig can't show either.

**Verification.** Partly covered: "a draining gateway still receives Kicks" is tested, and the 1M rolling-restart load test exercises drain. Neither asserts the NLB side.

**Proposed disposition.** Add assertions to the existing rolling-restart load test, run in the cloud cluster:
- after NotReady plus the NLB's deregistration delay, the draining pod accepts no new connections;
- every client on the draining pod receives `Reconnect` before its connection closes;
- Kicks sent to the draining pod during drain succeed.

---

## R4M-9 — `gwconns` single-shard load — DECIDED: ACCEPT (test only)

> **Decision (2026-10-07):** Accepted and applied to rev5 as Tier 2 measurements in (b1), (b-stress) and a triggered pass at the cap. On the single host the pass criterion is relative: the `gwconns` shard's p99 within 2× the other shards'.

**Claim.** One `gwconns` key per epoch concentrates a gateway's `SADD`/`SSCAN` traffic on one shard. It needs measuring at 200k / 500k / 1M members.

**Verification.** R2-O9 judged ~10k SADD/s acceptable, and that judgement still stands. One epoch can't exceed the 250k cap (600k in the benchmark), so 1M members per key can't occur. Six epochs' keys can, however, hash to the same shard.

**Proposed disposition.** No design change. In load tests (b1) and a triggered pass at the cap, record CPU and p99 latency per Redis shard, and identify the shards holding `gwconns` keys. Also run a 600k-member `SSCAN` + `UNLINK` during (b-stress).

---

## R4M-6 — 150s heartbeat cost at other intervals — DECIDED: ACCEPT (test only)

> **Decision (2026-10-07):** Accepted and applied to rev5: load test (c), Tier 2, adds 60s at 200k.

**Claim.** 150s is a trade-off. Benchmark 30/60/90/150s.

**Verification.** Not a defect. The interval is already server-set in `LoginResp`, and `reconnect-after-idle` already detects NAT timeouts shorter than 150s.

**Proposed disposition.** Extend load test (c) with one more point, **60s at 200k**, so the cost of lowering the interval is known if `reconnect-after-idle` forces it. Battery and NAT measurements are client-side and out of scope.

---

## R4M-8 + R4G-O2 — 250k cap asserted, not measured — DECIDED: ACCEPT (wording only)

> **Decision (2026-10-07):** Accepted and applied to rev5 (*Density*), adjusted for R4H-O11: the ceiling is measured per gateway core on the Tier 2 host and scaled to the production node size; re-derivation keeps operating point ≈ ceiling / 3 and the cap between operating point and ceiling.

**Claim.** The N−1 arithmetic depends on every gateway holding 250k, which is never measured.

**Verification.** Mostly covered. Load test (a) measures 600k on one gateway, a superset of 250k, and the rolling-restart test drives gateways to the cap. What's missing is what happens if (a) fails. R4M-8's `min(memory, CPU, GC, …)` formula is overdesign: the inputs are what (a) and (b) measure.

**Spec changes to apply:** *Density:* "250k is a configured value. It holds only if load test (a) reaches 600k. If the measured ceiling is lower, the operating point, the cap and the gateway count are re-derived from it before rollout."

---

## R4M-4 — order logins by Redis arrival, not gateway `recv_ms` — DECIDED: REJECT

> **Decision (2026-10-07):** Rejected. The skew concern is handled by R4M-5 (clock contract and `clock_skew_ms`). No spec change.

**Claim.** Gateway timestamps are subject to skew, so define login order by arrival at Redis.

**Rationale.** That reverts R1-O4. Ordering by Redis arrival is exactly what lets a delayed login from an abandoned device, received earlier but arriving later, kick the device the user is now holding (race 3, `SUPERSEDED`). The skew concern is real and is handled by R4M-5: an explicit contract and a metric.

---

## R4G-O1 — device_id uniqueness is trusted to the SDK — DECIDED: REJECT

> **Decision (2026-10-07):** Rejected; the clarifying sentence is added to *Client protocol → device_id* in rev5.

**Claim.** If the SDK's one-connection-per-device_id guard fails (cloned apps, rooted devices), the server's behaviour is unspecified, and a kicked instance may already have pushed frames downstream.

**Verification.** Not confirmed. The server doesn't depend on the guard for correctness. The login Lua serializes two simultaneous logins on one slot into one winner (`STALE` or `REPLACED`). Race 10 covers the steady state: they replace each other once, and `REPLACED` doesn't auto-reconnect. A connection in `AUTH_PENDING` processes only `LoginReq`. After losing the slot, the *Ownership rule* (interface constraints) stops it acting as the user, and sub-project 2 enforces that. A misbehaving client can only contend for its own account's slot.

**Optional clarification (one sentence, in *Client protocol → device_id*):** "The server doesn't rely on this for correctness. A violation only makes the instances replace each other on the user's own slot (race 10)."

---

## R4G-O3 — replace Noise with TLS on mobile — DECIDED: REJECT (record the rationale)

> **Decision (2026-10-07):** Rejected; the rationale is recorded in rev5 *Decisions → Client encryption*. Verified: gnet v2.10.0 has no TLS (README lists it as an unchecked roadmap item; no source imports `crypto/tls`).

**Claim.** Mobile and web use different security stacks for no documented reason. TLS would remove the whole Noise subsystem.

**Rationale.** It is an agreed decision; the reviewer just couldn't find the reason in the spec. The reasons:
- **TLS inside the gateway:** Go's `crypto/tls` runs on a blocking `net.Conn`, so it needs a goroutine per connection. That conflicts with the gnet model ("no goroutine per connection"), or else needs a separate non-blocking TLS implementation. *To verify before recording:* whether the pinned gnet tag ships its own TLS layer.
- **TLS at the NLB:** this is possible, but leaves plaintext between the NLB and the pod and moves 1M TLS sessions into the LB's limits and pricing.
- **Noise NK:** one round trip, ~100µs, no certificate chain, and the codec is owned by the gateway.

**Spec changes to apply:** *Decisions → Client encryption:* add one sentence with the reasons above. Note TLS at the NLB as the fallback if Noise proves costly in load test (a).

---

## R4G-O4 — Reconcile as TTL refresh only — DECIDED: REJECT

> **Decision (2026-10-07):** Rejected. No spec change.

**Claim.** Reconcile could refresh TTLs and write only empty fields, never overwriting, which would drop the ordering tree and the tie-break.

**Verification.** Not confirmed; the alternative breaks two contract rows.
- **Kick lost after a replacement:** the replaced connection's Reconcile entry finds a different session in the field. To return `not_owner` it must decide which session is newer, which is the ordering anyway. With "never overwrite, never judge", the replaced device stays connected indefinitely, which breaks "Old owner still connected after being replaced ≤ 1 cycle".
- **Failover rollback (R2-O4) while both connections are live:** the older session stays the owner, so the newer device can't act as the user (ownership rule) until the older one disconnects.

"Stale entries get cleaned by TTL" doesn't hold either: the `sess:` TTL is 30d. Rule 3 exists for R2-O4 and R3-O4, and those reasons still stand.

---

## R4G-O8 — refused re-queues amplify overload — DECIDED: REJECT (one-line clarification)

> **Decision (2026-10-07):** Rejected; the clarifying sentence is added to rev5 *Reconcile → Pass mechanics* (re-queued entries go back into their loop's partition, R4H-O9).

**Claim.** Re-queuing refused entries up to 3 times per pass feeds more load into an already slow path.

**Verification.** Not confirmed. Re-queued entries go back on the same pass queue and are sent in chunks under the pass's rate limit (330/s periodic, ~5k/s triggered). Re-queuing makes a pass up to 4× longer; it doesn't raise the Lua call rate. Reconcile is also a small share of Logic load next to the 20k/s login peak.

**Spec changes to apply:** *Gateway → Reconcile, per-entry results:* add "Re-queued entries count against the pass's rate limit; re-queuing lengthens the pass but never raises its rate."

---

## R4G-O10 — a stalled lease pool gets a healthy gateway swept, circularly — DECIDED: REJECT

> **Decision (2026-10-07):** Rejected. Strengthened by R4M-3: every sweep now implies an absent lease key, so the renewal ending the stall always reports `missing`. No spec change.

**Claim.** If a gateway's dedicated lease pool stalls, it looks dead and is swept. Recovery depends on the same pool recovering.

**Verification.** Not confirmed as a defect; this is race 12, third bullet, by design. A gateway that can't renew within the TTL has invalid sessions by the validity rule whether or not they are swept, so the sweep only cleans up. When renewals resume, the reply is `missing` (and with R4M-3, a sweep now requires the lease key to be absent, so this is guaranteed), and the triggered pass restores the sessions within ~1 min. If the pool never recovers, the gateway is effectively dead and nothing better is possible. One gateway's stall doesn't affect the guard for the others. The existing "lease renewal failures" metric plus the gRPC latency metrics cover detection.

---

## R4M-10 — formal invariant list — DECIDED: DEFER to the implementation plan

> **Decision (2026-10-07):** deferred. Commitment: the implementation plan (writing-plans) indexes every test task by race number (1–18), consistency-contract row and test tier (R4H-O11).

**Claim.** The state machine has grown. Write invariants I1–I10 and map every Lua script and test to them.

**Rationale.** It's useful, but it belongs to the plan rather than the spec. *Races handled*, *Consistency contract* and *Verification* already act as the invariant → test map in the spec. The plan (writing-plans) should index each test task by race number and contract row. Several of the review's invariants are restatements of those rows; I9/I10 are covered by R4M-11.

---

## R4M-12 … R4M-16 — endorsements — DECIDED: NO ACTION

No session version (12), no distributed login lock (13), no reverse mapping (14), the Logout fence (15), epoch-based identity (16): the review agrees with the spec. Nothing to change.

---

## Status after this round (decided 2026-10-07)

- All entries are decided and applied to the **rev5** spec, alongside the user's own review (R4H-O1…O11, applied directly to rev5). The rev4 spec is left unchanged.
- Real defects fixed: **R4M-1** (`rev:` TTL = `sess:` TTL, 30d) and **R4M-3** (lease key authoritative for the sweeper; `EpochEnded` deletes it).
- Accepted: R4M-2 (closes rev4 open point 1), R4M-5 (clock contract + `clock_skew_ms`), R4G-O12, R4G-O5, R4G-O6 (with `key_id` before the Noise message), R4G-O7 (gnet v2.10.0 pinned), R4M-11, R4M-17, R4M-8 + R4G-O2, and the test-only items R4M-7, R4M-9, R4M-6, each placed in the R4H-O11 test tiers.
- R4G-O11 superseded by R4H-O3. Rejected: R4M-4, R4G-O1, R4G-O3, R4G-O4, R4G-O8, R4G-O10 (three with a clarifying sentence in rev5). Deferred to the plan: R4M-10. No action: R4M-12…16.
- Rev4's other carried open point (order of `REVOKED` vs `EXPIRED` in the login Lua), raised by neither review, decided by the user as *R4H-OP1* (follow-up, 2026-10-07): revocation first. Both write nothing, so correctness is unaffected; `REVOKED` first gives a revoked-and-late login its final answer at once instead of a wasted `EXPIRED` retry. Applied to rev5 with a Tier 1a Lua test. **rev5 has no open points.**
