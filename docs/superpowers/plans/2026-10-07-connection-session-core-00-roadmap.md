# Connection & Session Core — Plan Roadmap

> Index of the implementation plans for sub-project 1. Each plan is a separate document and produces working, tested software on its own. Plans are written and reviewed one at a time: a later plan is written against the reviewed interfaces of the plans before it.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative; the zh-CN translation is not). Plan 1 Tasks 1–9 were written against rev5; rev6 (the rev5 system-design review, *R5S-O1*…*O9*) changes only Plan 1's Login/Logout batching (Task 10) and is otherwise additive for Plans 2–6.

## The plans

| # | Plan | Builds | Depends on | Spec sections | Status |
|---|---|---|---|---|---|
| 1 | `2026-10-07-connection-session-core-01-session-store.md` | Repo skeleton, Tier 1a test Redis, `internal/session` (entry format, ordering rule), `internal/store` (every Redis key and Lua script: login, logout, revoke, Reconcile compare-and-set, ReconcileEnd delete, lease, sweeper ops) | — | *Redis schema*, *Login Lua*, *Logout Lua*, *Sweeper* (store side), *Revoke* (store side), *Reconcile handling* (store side) | Tasks 1–10 implemented (Task 10: per-node Login/Logout results, R5S-O1, commit 9c8519e) |
| 2 | `2026-10-08-connection-session-core-02-logic.md` | `api/internal.proto` + generated code; JWT/JWKS verification with unknown-`kid` handling; `cmd/devtoken`; Logic RPCs (`Login`, `Logout`, `GatewayLease` on its own pool, `EpochEnded`, `Reconcile` rules 1–4, `ReconcileEnd`, `Revoke`); hardened Kick client; sweeper loop and mass-stale guard; readiness; Logic metrics; `cmd/logic` | 1 | *Logic*, *Sweeper*, *Auth*, *Observability* (Logic side) | implemented 2026-10-08 on `feat/plan2-logic` (13 tasks, Native); final review: 1 Important + 1 re-graded Minor fixed, 7 minors deferred (Plan 2 Execution Notes) |
| 3 | `2026-10-08-connection-session-core-03-gateway-core.md` | `api/client.proto` (moved from Plan 4, choice #24); transport-independent gateway: loopState and `conn_id`, timing wheel, connection state machine on a fake transport, admission (connection cap, pending cap, Login-queue cap), Login batching, Logout fence (a)(b)(c) and retry queue, lease loop, Reconcile pass controller, Kick server, readiness, drain; gateway metrics; layer-rule import test (no gateway package imports `internal/store`) | 2 (proto) | *Gateway* (all but the TCP transport), *Timers*, *Client protocol* (bodies) | written 2026-10-08 (12 tasks, prototyped and replayed); choices #24–#37 decided 2026-10-08, all as proposed; implemented 2026-10-08 on `feat/plan3-gateway` (12 tasks, Native); final review: 1 Important + 1 re-graded Minor fixed, 4 minors deferred (Plan 3 Execution Notes) |
| 4 | TCP transport | Frame codec (6-byte header; message bodies are Plan 3's `api/client.proto`), Noise NK with `key_id` prologue, handshake limiter (semaphore + token bucket), gnet v2.10.0 integration, gnet loop-ownership `-race` test, `cmd/gateway` | 3 | *Client protocol*, *Transport seam*, *Noise key runbook* | not written |
| 5 | Client SDK and Tier 1b race suite | Go client SDK (reconnect rules, retryable-reject counter, jitter distributions), scripted test client, fault-injection hooks (Logic delay/drop, Kick drop, fake gateway clock), `deploy/compose` with 4 gateways, test timing profile and ratio checks, every Tier 1b race integration test | 2, 3, 4 | *Client SDK rules*, *Verification → Race integration tests*, *Test execution model* | not written |
| 6 | Load and release tests | `cmd/loadgen`, Tier 2 scenarios (a, b1, b2, b-stress, c, storm, floods, slowed Logic, Redis failover, `gwconns` shard load), Tier 3 rolling restart with `TARGET=local|cloud`, host guard, JSON results, `deploy/k8s` manifests | 5 | *Verification → Load tests*, *Tier 3 target*, *Deployment* | not written |

The web transport (wss) is designed in the spec and built in a late slice; it is not in these six plans.

## Implementation choices the spec leaves open

Plan 1 makes these choices. They are listed here so they can be reviewed in one place; each is restated in the plan task that implements it. **Approved by the user (2026-10-07): #2, #3, #4, #5.** The others stand as written unless the review of Plan 1 changes them.

1. **Key format for per-epoch keys.** `gwlease:<gw>:{<epoch>}`, `gwconns:<gw>:{<epoch>}`, `sweep:<gw>:{<epoch>}`: the hash tag is the epoch, so one epoch's lease and `gwconns` set share a shard. `gwepochs` members are `<gw>:<epoch>`.
2. **How the sweeper learns about `EpochEnded`.** `EpochEnded` deletes the lease key and sets the epoch's `gwepochs` score to 0. A score of 0 means "ended": the sweeper treats it as stale at once and exempts it from the mass-stale guard. No new key.
3. **`tok_iat` is stored in milliseconds**, the unit of `revoked_before_ms`. Plan 2 must read the JWT `iat` with millisecond precision (devtoken issues it that way); a whole-second `iat` is truncated, so a token issued in the same second after a revocation would read as revoked.
4. **The Reconcile compare-and-set re-checks rule 1 inside the Lua.** Without it, a `Revoke` that lands between Logic's read and the compare-and-set (marker confirmed, field deleted) is undone by a rule-4 restore until the next pass. The check costs one `HMGET` on the same slot and returns `REVOKED`, which Logic reports as `not_owner(REVOKED)`.
5. **Revoke step 2 deletes only fields whose `tok_iat` is below their effective marker.** A login with a fresh token that lands between the marker write and the delete keeps its session.
6. **Revoke's `WAIT` runs on the connection that wrote the marker** (`MasterForKey` + `Conn()`): `WAIT` counts only that connection's writes.
7. **Compare-and-set result precedence:** `REVOKED`, then `REFUSED` (deadline), then `UNCHANGED`, then `WRITTEN`. A late entry is refused (and re-queued with a fresh read) even if the field also changed.
8. **Login result `REGRESSED`** is reported separately from `REPLACED` so Logic can count `conn_seq_regressed`; both kick the old connection with `REPLACED`.
9. **Input limits** that the `|`-separated value and Lua doubles need: uid `[A-Za-z0-9_-]{1,64}`; appid `[A-Za-z0-9_.-]{1,64}`; gateway id a DNS-1123 label; device_id 1–128 bytes without `|`; `conn_seq` ≤ 2^53 − 1. `epoch` and `conn_id` are full uint64s and are only ever compared as strings in Lua.
10. **Revoke `WAIT` timeout** defaults to 1s (`store.Config.WaitTimeout`).

Choices left to the plans by rev6, **decided by the user (2026-10-07), all as recommended:**

11. **Per-entry Login results come from per-node grouping in the store** (R5S-O1). `store.LoginEach` / `LogoutEach` group a batch by owning master (`MasterForKey`), pipeline each group concurrently through the cluster client, and report each group as it returns (Plan 1 Task 10). Rejected: one `EVALSHA` per entry (20k round trips/s in a storm), and grouping by slot in Logic (Redis topology in Logic).
12. **Kick queue per target** (R5S-O2): per Logic pod and target `gw|epoch`, **64 workers and a queue of 1024**, both configurable. A dead target drains 64 attempts/s at the ~1s deadline and drops the rest; a live target (~1ms per Kick) handles ~64k/s. Plan 2.
13. **SDK clamp ranges** (R5S-O6), each window `[lo, hi]` clamped at both ends and `lo < hi` enforced. Plan 5.

    | Window | Default | Allowed `hi` | Allowed `lo` |
    |---|---|---|---|
    | Storm wait | 0–20s | 10–120s | 0 |
    | Local trigger | 0–1s | 1–10s | 0 |
    | Pre-handshake retry | 0.5–1.5s | 1–5s | ≥ 0.25s |
    | Other retryable | 1–3s | 2–10s | ≥ 0.5s |

    The storm `hi` floor is 10s because 200k reconnects over 10s is 20k/s, exactly the headroom; rev6's example "storm 5–120s" was corrected to match.
14. **Reconcile chunks stay unary** (R5S-O1 follow-up): a chunk is resolved as a whole (fence (c) per chunk, one `Execute` per chunk); a slow shard only delays that chunk and its deadline re-queues the entries. `CompareAndSet` and `ReadSlots` keep one pipeline per chunk. Plans 2 and 3.

Choices Plan 2 makes, **decided 2026-10-08, all as proposed, #22 amended at review** (details in Plan 2, *Implementation choices this plan makes*):

15. Proto tooling: protoc 33.1 release binary (no sudo), `protoc-gen-go v1.36.12`, `protoc-gen-go-grpc v1.6.2`; generated code committed; `make proto`.
16. JWT verified on the standard library (EdDSA JWS compact only, `kid`, strict base64url, ≤ 4096 bytes); claims `sub`, `appid`, `platform`, `iat`, `exp`; NumericDates read exactly to the ms; `exp` required with 30s leeway.
17. Login codes beyond the spec's table: `TOKEN_INVALID` (client gets a new token; Plan 5 confirms), `BAD_REQUEST`, `OUTCOME_UNKNOWN` (gateway-internal: fence (b)).
18. Ports: `Logic` + `Revocation` on :9000 (MaxConnectionAge 5m, grace 30s); `Lease` on its own server on :9001 (own pool, no MaxConnectionAge); admin :9090. Pools: 64 main, 16 lease; `ReconcileEnd` and `Revoke` unpooled.
19. Reply shapes: `Logout` unary with per-entry `done` (false = retry); `Reconcile` fails the whole RPC on any unknown outcome; an invalid chunk entry fails the chunk with `INVALID_ARGUMENT`.
20. Login follow-ups (`gwconns` SADD once per batch, dead-epoch cross-check) run after the results, on a context detached from the stream.
21. Sweeper: 5s cycle, pages of 500, abort check per batch; guard durations on the local clock, score ages on Redis `TIME`; one limiter per pod; failover time defaults to 15s for the ratio check.
22. Kick: workers on demand up to 64 per target, retire after 1m idle; one connection per gateway pod name; retries 10s apart on `UNAVAILABLE`/`DEADLINE_EXCEEDED` only; `target_lease=unknown` when the lease can't be read. A drop is labelled from the lease cache only (`unknown` if nothing is cached), so `Kick` never reads Redis on its caller's goroutine.
23. One ~1s lease cache serves Kick retries, Reconcile rules 3/4 and the cross-check; Logic's own renewals and `EpochEnded` update it.

Choices Plan 3 makes, **decided 2026-10-08, all as proposed** (#34 kept latched: a single-pod Logic partition answers `Busy` and clients retry elsewhere) (details in Plan 3, *Implementation choices this plan makes*):

24. `api/client.proto` is built in Plan 3 (the core decodes `LoginReq` and encodes the replies); `Platform` and `KickReason` duplicated with a parity test; `ReconnectReason` adds `LOCAL_TRIGGER`.
25. Transport seam: `Loop.Execute(func()) error`, `Link{Send, Close}`, core calls `Open`/`HandshakeDone`/`Frame`/`Closed` on the loop; no code relies on `Execute` order (gnet v2.10.0 can reorder); reject reason `draining`.
26. Rejected, cancelled-before-send and dropped-at-deadline Logins need no Logout; a dropped one gets `Error{EXPIRED}` (label `expired_queued`).
27. No definitive reply: `Busy` (1–3s) and close; the fenced Logout names the token's unverified `sub`.
28. Lease trigger: `missing`, or a success ending ≥ one lease TTL after the previous success started (gateway `LeaseTTL` = Logic's); errors alone don't trigger; first success ignored.
29. A trigger during a triggered pass runs one more triggered pass after it.
30. Chunks of 200, 4 in flight, triggered 5k/s, periodic = ONLINE ÷ (0.9 × cycle) in [10/s, 5k/s]; `sent_ms` stamped on the loop; chunk deadline on the wheel; RPC error waits for the deadline; late reply only Kicks; re-queue after the loop reports resolution.
31. `ReconcileEnd` deadline 2 × tracked ÷ 10k/s + 10s; retries 1s doubling ≤ 10s for 60s; no `ReconcileEnd` if any loop's fragment is missing.
32. Logout: batches 200/5ms, 4 RPCs, 5s; per-batch retries 1s doubling ≤ 10s for 60s; queue = cap; full → drop and untrack.
33. 16 Login RPCs; `LoginQueueCap` 8192 until Plan 6 measures it; `PendingCap` 100k.
34. Readiness latches once Ready; Kick :9100, admin :9090; Logic channels plaintext, `round_robin`, keepalive 20s/5s.
35. Drain: Reconnect in 1s steps over 60s (also to connections that come ONLINE meanwhile), Reconcile stopped, close the rest, wait ≤ login_deadline + 1s + 10s for nothing tracked, `EpochEnded` ×3; grace period ≥ 90s.
36. Metrics `goim_gateway_*`, with `closes_total{reason}` and Login results `busy`, `expired_queued`, `error`.
37. Request IDs `<epoch hex>-<seq>` (≤ 37 bytes).

## Invariant index (R4M-10)

Every race, consistency-contract row and spec test maps to the plan and test that pin it. Plan 1's test names are final; names for plans 2–6 are planned and are fixed when each plan is written.

### Races (spec *Races handled*)

| Race | Tier 1a (unit / Lua) | Tier 1b (integration) |
|---|---|---|
| 1 Delayed disconnect | P1 `TestLogoutCompareAndDelete` (race 1 subtest) | P5 race 1 |
| 2 Delayed login, same device | P1 `TestLoginTable` (STALE rows), `TestLoginExpired` | P5 race 2 |
| 3 Delayed login, other device | P1 `TestLoginTable` (SUPERSEDED row) | P5 race 3 |
| 4 Zombie gateway, same pod name | P1 `TestSweepDeletesOnlyDeadEpochFields`; P3 `TestKickForAnotherEpochOrUnknownConnIsIgnored` | P5 race 4 |
| 5 Close while Login in flight | P3 `TestFenceAHoldsLogoutUntilTheLoginReply`, `TestCloseWhileQueuedNeedsNoLogout` | P5 race 5 |
| 6 No definitive reply, Redis written | P3 `TestFenceBReleasesLogoutAfterLoginDeadlinePlusOneSecond`, `TestLoginEntryWithoutResultByItsTimeoutGetsBusyThenFencedLogout`, `TestLoginStreamEndingEarlyIsNoDefinitiveReply` | P5 race 6, both orders, Logic crash |
| 7 Logout lost, gateway alive | P1 `TestDeleteUntracked`; P2 `TestReconcileEndDeletesOnlyUntrackedGhosts`; P3 `TestLogoutIsDroppedWhenItsRetryBudgetRunsOut`, `TestE2EReconcileRestoresAndDeletesGhosts` | P5 dropped-Logout repair |
| 8 Kick lost or early | P2 `TestKickRetriesOnlyWhileLeaseLive`, `TestKickDropDoesNotReadRedis`, `TestKickNewEpochOfSamePodGetsItsOwnConnection`, `TestReconcileKicksWrittenOverrideWhenChunkFails`; P3 `TestKickDuringAuthPendingIsAppliedWhenTheLoginReplyArrives`, `TestKickPendingWinsOverNoDefinitiveReply`, `TestNotOwnerIsKickedWithItsReason` | P5 Kick tests |
| 9 Two devices kick each other | P5 SDK: no reconnect after `KICKED_BY_OTHER_DEVICE` | — |
| 10 Shared device_id | P1 `TestLoginTable` (REPLACED row); P5 SDK: no reconnect after `REPLACED` | P5 race 10 |
| 11 Counter regression | P1 `TestLoginTable` (regression and STALE rows), `TestChunkWins`; P2 `TestReconcileRule3RegressedCounter` (miniredis at Tier 1a, real Redis at Tier 2) | — |
| 12 Swept while healthy | P1 `TestRenewLease` (missing after DEL); P2 `TestGuardSurvivesTenSecondRhythmWithJitter`, `TestGuardResetsWhenRenewalsStop`, `TestSweeperAbortsWhenEpochRenews`, `TestSweeperHoldsWhenNobodyRenews`, `TestSweeperSkipsEpochWhoseLeaseExists` | P5 sweep-abort, 90s outages, Logic restart, forced sweep |
| 13 Two loops generating conn_ids | P3 `TestConnIDsAreUniqueAndCarryTheWorkerID`; P4 gnet loop-ownership `-race` test | — |
| 14 Close during a Reconcile pass | P3 `TestFenceCHoldsLogoutUntilTheChunkIsResolved`, `TestFailedChunkIsResolvedAtSentMsPlusDeadlinePlusOneSecond` | P5 race 14 |
| 15 Write lost in failover | P1 `TestCompareAndSet`, `TestChunkWins`; P2 `TestReconcileRule3AfterLostWrite`, `TestReconcileRule3TieKeepsStored` | P6 Redis failover (Tier 2) |
| 16 Login in flight / Logout dropped around a pass | P1 `TestDeleteUntracked` (`login_ms ≥ as_of`, tracked); P3 `TestReconcileEndCarriesTheTrackedSetCollectedAfterAsOf`, `TestIncompleteTrackedSetSendsNoReconcileEnd` | P5 race 16, ReconcileEnd tests |
| 17 Slot logout, Kick lost | P1 `TestRevokeDeletesOnlyAffectedFields`, `TestLoginRevoked`, `TestCompareAndSet` (slot-scope subtests); P2 `TestReconcileRule1Revoked`, `TestReconcileConcurrentChange` | P5 slot-scoped Revoke |
| 18 Client Logout during a chunk | P3 `TestClientLogoutDuringAChunkGetsLogoutRespAtOnce` | P5 race 18 |

### Consistency contract rows

| Row | Pinned by |
|---|---|
| Ghost after a dropped Logout, ≤ 1 cycle | P1 `TestDeleteUntracked`; P5 dropped-Logout repair |
| Old owner still connected after replace/revoke, ≤ 1 cycle (revocation within the 30d marker TTL) | P1 `TestRevokeMarkerScriptIsMaxIdempotentAndRefreshesTTL`; P2 `TestReconcileRule1Revoked`, `TestReconcileRule3StoredNewerIsNotOwner`; P5 Kick-dropped and revocation-outlives-token tests |
| Dead gateway's entries: invalid after lease TTL, deleted after 2 × TTL + guard + sweep | P1 `TestSweepDeletesOnlyDeadEpochFields`, `TestEndEpochAndLeasesExist`; P2 `TestSweeperSweepsDeadEpochAfterGuard`, `TestSweeperEndedEpochIsExempt`; P3 `TestNoLeaseRenewalAfterEpochEnded`; P5 gateway crash → sweep |
| Live connection missing from Redis, ≤ 1 cycle / ~1 min after `missing` | P1 `TestCompareAndSet`, `TestRenewLease`; P3 `TestFirstLeaseMissingIsIgnoredAndALaterOneTriggersAPass`, `TestRenewalAfterFailuresForALeaseTTLTriggersAPass`, `TestE2EReconcileRestoresAndDeletesGhosts`; P5 forced sweep, stale lease observation |
| Healthy session reads invalid during an outage | P5 Redis paused 90s, gateway→Logic blocked 90s |

### Tiers

| Tier | Plans that add tests to it |
|---|---|
| 1a (every change, dev machine) | 1, 2, 3, 4, 5 (SDK unit tests) |
| 1b (before every commit, dev machine, test timing profile) | 5 |
| 2 (dedicated host) | 6 |
| 3 (`TARGET=local` or `cloud`) | 6 |

## Dev machine and test tiers

The dev machine (Tier 1a/1b, 2 cores / 2GB) has Go 1.27.1. Plan 1, Task 1 installs gcc and make for `-race`.

**User decision (2026-10-07): Docker is not installed on the dev machine.** This overrides the spec's "Lua tests against a real Redis in Docker (Tier 1a)". Tier 1a Lua tests run on in-process miniredis. Tests that need real Redis skip with `TODO: Tier 2 integration` and run on the Tier 2 host (`GOIM_TEST_TIER=2 make redis-up test-redis`). Each plan's *Execution Notes* lists the risks this leaves open.

This also affects Plan 5: the spec puts the race integration suite (`deploy/compose`, 4 gateways, Tier 1b) on the dev machine, which needs Docker. Where that suite runs is decided when Plan 5 is written.
