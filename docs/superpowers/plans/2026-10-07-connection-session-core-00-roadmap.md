# Connection & Session Core — Plan Roadmap

> Index of the implementation plans for sub-project 1. Each plan is a separate document and produces working, tested software on its own. Plans are written and reviewed one at a time: a later plan is written against the reviewed interfaces of the plans before it.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative; the zh-CN translation is not). Plan 1 Tasks 1–9 were written against rev5; rev6 (the rev5 system-design review, *R5S-O1*…*O9*) changes only Plan 1's Login/Logout batching (Task 10) and is otherwise additive for Plans 2–6.

## The plans

| # | Plan | Builds | Depends on | Spec sections | Status |
|---|---|---|---|---|---|
| 1 | `2026-10-07-connection-session-core-01-session-store.md` | Repo skeleton, Tier 1a test Redis, `internal/session` (entry format, ordering rule), `internal/store` (every Redis key and Lua script: login, logout, revoke, Reconcile compare-and-set, ReconcileEnd delete, lease, sweeper ops) | — | *Redis schema*, *Login Lua*, *Logout Lua*, *Sweeper* (store side), *Revoke* (store side), *Reconcile handling* (store side) | Tasks 1–10 implemented (Task 10: per-node Login/Logout results, R5S-O1, commit 9c8519e) |
| 2 | `2026-10-08-connection-session-core-02-logic.md` | `api/internal.proto` + generated code; JWT/JWKS verification with unknown-`kid` handling; `cmd/devtoken`; Logic RPCs (`Login`, `Logout`, `GatewayLease` on its own pool, `EpochEnded`, `Reconcile` rules 1–4, `ReconcileEnd`, `Revoke`); hardened Kick client; sweeper loop and mass-stale guard; readiness; Logic metrics; `cmd/logic` | 1 | *Logic*, *Sweeper*, *Auth*, *Observability* (Logic side) | implemented 2026-10-08 on `feat/plan2-logic` (13 tasks, Native); final review: 1 Important + 1 re-graded Minor fixed, 7 minors deferred (Plan 2 Execution Notes) |
| 3 | `2026-10-08-connection-session-core-03-gateway-core.md` | `api/client.proto` (moved from Plan 4, choice #24); transport-independent gateway: loopState and `conn_id`, timing wheel, connection state machine on a fake transport, admission (connection cap, pending cap, Login-queue cap), Login batching, Logout fence (a)(b)(c) and retry queue, lease loop, Reconcile pass controller, Kick server, readiness, drain; gateway metrics; layer-rule import test (no gateway package imports `internal/store`) | 2 (proto) | *Gateway* (all but the TCP transport), *Timers*, *Client protocol* (bodies) | written 2026-10-08 (12 tasks, prototyped and replayed); choices #24–#37 decided 2026-10-08, all as proposed; implemented 2026-10-08 on `feat/plan3-gateway` (12 tasks, Native); final review: 2 Important fixed (one re-graded from Minor), 4 minors deferred, one open risk (Plan 3 Execution Notes) |
| 4 | `2026-10-08-connection-session-core-04-tcp-transport.md` | `internal/wire` (frame codec with the 6-byte header, `Cut`, Noise NK on both sides with the `key_id` prologue, per-frame AEAD, key set); `internal/gateway/tcp` (gnet v2.10.0 transport, handshake limiter with a semaphore and a token bucket, re-entry guard); gnet loop-ownership `-race` test and the gnet pin; core seam `Reject`/`Close`/`Config`; `cmd/gateway` with `noise-keygen` and the SIGTERM drain; end-to-end test over TCP against Logic | 3 | *Client protocol*, *Transport seam*, *Noise key runbook*, *Admission control* (handshake limit) | written 2026-10-08 (8 tasks, prototyped and replayed); reviewed 2026-10-08: #38–#45 decided (#42/#44 amended), execution Native on `feat/plan4-tcp`; implemented 2026-10-08 (8 tasks, Native); final review: 2 Important fixed (outbound cap `slow_consumer`, `noise-keygen` O_EXCL; one re-graded from Minor), 6 minors deferred (Plan 4 Execution Notes); choice #46 (outbound cap) decided; Tier 2 run 5 passed on a34f4a1; interface review P4I-1..P4I-6 decided, P4I-1..4 implemented as Task 9 |
| 5a | `2026-10-08-connection-session-core-05a-sdk.md` | `internal/clientconn` (the scripted test client: one blocking connection, any frame, FIN or reset); `internal/sdk` (the Go client SDK: persistence of device_id/conn_seq/windows, storm wait, local trigger, `Reconnect`, the two retry counters, backoff, clamped server-set windows, Ping and Pong timeout, stop conditions); SDK unit tests on `synctest` fake time; end-to-end over TCP against real gateways and Logic (races 9, 10) | 2, 3, 4 | *Client SDK rules*, *Client protocol* (client side), *Verification → Unit tests → SDK* | written 2026-10-08 (7 tasks, prototyped and replayed); split from Plan 5 (choice #47); choices #48–#57 decided 2026-10-08, all as proposed; plan review 2026-10-08: 9 items decided (#49, #52, #53, #56, #57 amended; Task 7 adds the gateway heartbeat range) |
| 5b | Tier 1b race suite | fault-injection hooks (Logic delay/drop/crash, Kick drop, `ZADD gwepochs` failure), test timing profile and ratio checks, the process harness of choice #47 (real binaries; miniredis server process at Tier 1b, compose Redis primary + replica at Tier 2 as `make test-tier2 SCENARIO=race`), `cmd/gateway` fake-clock flag (P4I-5), every Tier 1b race integration test | 5a | *Verification → Race integration tests*, *Test execution model* | not written; written after Plan 5a's interfaces are reviewed |
| 6 | Load and release tests | `cmd/loadgen`, Tier 2 scenarios (a, b1, b2, b-stress, c, storm, floods, slowed Logic, Redis failover, `gwconns` shard load), Tier 3 rolling restart with `TARGET=local|cloud`, host guard, JSON results, `deploy/k8s` manifests | 5b | *Verification → Load tests*, *Tier 3 target*, *Deployment* | not written |

The web transport (wss) is designed in the spec and built in a late slice; it is not in these six plans. Plan 5 is written as two plans, 5a and 5b (choice #47).

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

Choices Plan 4 makes, **decided 2026-10-08 at the plan review, all accepted; #42/#44 amended (the limiter reports which limit refused), #39's rationale corrected** (details in Plan 4, *Implementation choices this plan makes*):

38. Packages: `internal/wire` (byte format and Noise, both sides; reused by Plan 5's SDK and Plan 6's loadgen) and `internal/gateway/tcp` (gnet); the layer test covers `internal/gateway/...`.
39. Noise suite `Noise_NK_25519_ChaChaPoly_SHA256` on `flynn/noise v1.1.0` (no mobile platform library ships BLAKE2s; SHA-256 is everywhere; Android's JCA has X25519 only from API 33, so its SDK carries a crypto library below that either way).
40. Handshake framing: prologue = frame 1's header + `key_id`; the server replies with one `HANDSHAKE` frame carrying the raw 48-byte Noise message, header checked exactly by the client; frame 1's `Length` is exactly 50.
41. Client frames after the handshake: `Length` in [20, `MaxClientFrame` = 8192], checked from the header before buffering.
42. Handshake limiter inline on the loop: semaphore = half the loops, bucket 25k/s with burst 2.5k, slot before token; a refusal is a `handshake_rate` reject and also `handshakes_total{result=no_slot|no_token}`, so Plan 6 can re-derive each limit.
43. Noise key Secret: one file per decimal `key_id` with the base64 private key; a bad file or an empty set stops the gateway; `gateway noise-keygen` prints the public key.
44. Transport mechanics: one acceptor + `RoundRobin`, `TCP_NODELAY`, default gnet buffers, listener open during drain, re-entry guard for gnet's close-inside-write; core seam `Reject`, `Close`, `Config`; `closes_total` reasons `rejected`, `bad_frame`, `decrypt`, `handshake`, and `slow_consumer` past `MaxOutbound` (added at the final review: gnet's outbound buffer is unbounded; see #46); `handshakes_total{result=ok|failed|no_slot|no_token}`.
45. `cmd/gateway`: flags mirror the configs, `-id` from `$POD_NAME`, port `:8000`, loops = `GOMAXPROCS`; the core runs on its own context, SIGTERM drains on a fresh context bounded by `-drain-timeout` 85s.
46. **Outbound cap** (decided 2026-10-08, after the Plan 4 final review): `tcp.Config.MaxOutbound` defaults to one max frame, `HeaderLen + MaxLength` = 65,541 bytes, which is also its minimum; flag `-max-outbound`. A frame that would pass it is dropped and the connection closed as `closes_total{reason="slow_consumer"}`. Why: sub-project 1 sends only control frames, and the only volume is Pongs, one per client Ping. A client with 64KB queued in gnet, behind a full kernel send buffer (16KB, autotuned up to `tcp_wmem` max), has ignored more than 2,500 Pongs, so a larger default protects nothing. The worst case at the 250k cap is 16GB, against 64GB at 256KB, and each such connection needs a valid login. **Sub-project 2 must revisit it:** offline sync after a reconnect can queue more than 64KB legitimately; the fix there is pull-based flow control (the client acks or pulls pages), not a larger buffer. Plan 6 records `slow_consumer` closes in the load tests, and any nonzero count there is a bug in the load generator or in this limit.

Choices Plan 5a makes (details in Plan 5a, *Implementation choices this plan makes*). **#47 decided by the user (2026-10-08); #48–#57 decided at the Plan 5a review (2026-10-08), all as proposed; amended by the second Plan 5a review the same day (Plan 5a *Execution Notes → Plan review*, items 1–9):**

47. **Plan 5 split; the race harness runs processes** (decided 2026-10-08): Plan 5a is the SDK and the scripted client; Plan 5b is the fault hooks, the test timing profile, the harness and every race scenario, written after 5a's interfaces are reviewed. The harness runs the real binaries (Logic, 4 gateways, devtoken) as local processes on both tiers: Tier 1b on the dev machine with a miniredis server process, Tier 2 (`make test-tier2 SCENARIO=race`) on the compose Redis primary + replica; kills and pauses are signals, faults go through a test-only endpoint; scenarios that need real Redis skip at Tier 1b. No 4-gateway compose file (Plan 6 adds Kubernetes manifests). Amends P4I-6.
48. Packages `internal/clientconn` (blocking connection, no policy: scripted client, SDK transport, candidate loadgen transport) and `internal/sdk` (client side only, pinned by a dependency test).
49. Classification: `STALE`, `SUPERSEDED`, `TOKEN_INVALID` share the second counter with `Busy`/`EXPIRED`/`KEY_UNKNOWN`/close-before-reply; `BAD_REQUEST` and unknown codes (an undecodable `Error` is one) back off with an anomaly, and the policy alone decides which endings are anomalies; dial, token, `Save` and handshake failures (and no handshake reply within `ConnectTimeout`) are a failure backoff that leaves both counters; `REVOKED` (`Error` or `Kick`) asks for a new login; after `TOKEN_INVALID` the next `Token(ctx, refresh)` call gets `refresh = true` (items 2, 4).
50. Backoff step n uniform in [d/2, d], d = min(5m, 5s × 2^(n−1)); reset at login only; never below `retry_after`. Closes 1–10 and rejects 1–5 retry fast.
51. A local trigger cuts the storm wait and a failure backoff, nothing else; `Foreground` also ends the idle state after `REPLACED`; `KICKED_BY_OTHER_DEVICE` and `REVOKED` need `Connect`.
52. Clamps: storm and local `lo` pinned at 0; `lo ≥ hi` after clamping becomes the floor; only server-sent windows are persisted and clamped; `heartbeat_s` 0 means 150s, any other value is clamped to 30s–300s (`sdk.Config.Heartbeat`), and the gateway refuses a heartbeat outside the same range (`HeartbeatMin`/`HeartbeatMax`, `-heartbeat-min`/`-heartbeat-max`; the test profile lowers both ends); `retry_after` capped at 5m and `jitter_ms` at 120s; every clamp or cap of a server value is an anomaly (items 5, 6).
53. Client timeouts: token 10s (a timeout is a failure backoff), connect (dial + handshake) 10s, `LoginReq` → reply 10s, Pong 10s, Logout → `LogoutResp` 2s (item 3).
54. `reconnect_reason` = the reason of the wait before the attempt (a failure backoff keeps the previous one); `offline_ms` from the loss of the last established connection, across retries, on the monotonic clock.
55. One Ping per heartbeat at a fixed cadence; the Pong timeout runs from the oldest Ping no frame has followed.
56. One persisted record (device_id, conn_seq, server windows); `conn_seq` saved before every dial (a failed `Save` skips the attempt); `STALE` saved at once; windows saved when a `LoginResp` changes them; a corrupt record (`ErrCorrupt`, kept as `.corrupt`) starts fresh with a new device_id, while a record that can't be read yet is kept and read again before each attempt; `FileStore` is locked (`flock`) so a second client on it fails with `ErrStoreInUse` (items 1, 8).
57. API: `New` (idle), `Connect`, `NetworkChanged`, `Foreground`, `Logout`, `SwitchAccount`, `Close`; one run-loop goroutine; ordered events through `OnEvent`, all on that goroutine, `New`'s store events included; a `LogoutResp` ends the connection only after the client's `Logout`; `Addrs` picked at random per attempt (items 7, 9).

## Invariant index (R4M-10)

Every race, consistency-contract row and spec test maps to the plan and test that pin it. Plan 1's test names are final; names for plans 2–6 are planned and are fixed when each plan is written.

### Races (spec *Races handled*)

| Race | Tier 1a (unit / Lua) | Tier 1b (integration) |
|---|---|---|
| 1 Delayed disconnect | P1 `TestLogoutCompareAndDelete` (race 1 subtest) | P5b race 1 |
| 2 Delayed login, same device | P1 `TestLoginTable` (STALE rows), `TestLoginExpired` | P5b race 2 |
| 3 Delayed login, other device | P1 `TestLoginTable` (SUPERSEDED row) | P5b race 3 |
| 4 Zombie gateway, same pod name | P1 `TestSweepDeletesOnlyDeadEpochFields`; P3 `TestKickForAnotherEpochOrUnknownConnIsIgnored` | P5b race 4 |
| 5 Close while Login in flight | P3 `TestFenceAHoldsLogoutUntilTheLoginReply`, `TestCloseWhileQueuedNeedsNoLogout` | P5b race 5 |
| 6 No definitive reply, Redis written | P3 `TestFenceBReleasesLogoutAfterLoginDeadlinePlusOneSecond`, `TestLoginEntryWithoutResultByItsTimeoutGetsBusyThenFencedLogout`, `TestLoginStreamEndingEarlyIsNoDefinitiveReply` | P5b race 6, both orders, Logic crash |
| 7 Logout lost, gateway alive | P1 `TestDeleteUntracked`; P2 `TestReconcileEndDeletesOnlyUntrackedGhosts`; P3 `TestLogoutIsDroppedWhenItsRetryBudgetRunsOut`, `TestE2EReconcileRestoresAndDeletesGhosts` | P5b dropped-Logout repair |
| 8 Kick lost or early | P2 `TestKickRetriesOnlyWhileLeaseLive`, `TestKickDropDoesNotReadRedis`, `TestKickNewEpochOfSamePodGetsItsOwnConnection`, `TestReconcileKicksWrittenOverrideWhenChunkFails`; P3 `TestKickDuringAuthPendingIsAppliedWhenTheLoginReplyArrives`, `TestKickPendingWinsOverNoDefinitiveReply`, `TestNotOwnerIsKickedWithItsReason` | P5b Kick tests |
| 9 Two devices kick each other | P5a `TestKicksDoNotReconnect` (SDK: no reconnect after `KICKED_BY_OTHER_DEVICE`) | P5a `TestE2EKickedByOtherDeviceDoesNotReconnect` (real gateway and Logic, Tier 1a) |
| 10 Shared device_id | P1 `TestLoginTable` (REPLACED row); P5a `TestKicksDoNotReconnect`, `TestE2ESharedDeviceIDReplacesOnce` | P5b race 10 |
| 11 Counter regression | P1 `TestLoginTable` (regression and STALE rows), `TestChunkWins`; P2 `TestReconcileRule3RegressedCounter` (miniredis at Tier 1a, real Redis at Tier 2) | — |
| 12 Swept while healthy | P1 `TestRenewLease` (missing after DEL); P2 `TestGuardSurvivesTenSecondRhythmWithJitter`, `TestGuardResetsWhenRenewalsStop`, `TestSweeperAbortsWhenEpochRenews`, `TestSweeperHoldsWhenNobodyRenews`, `TestSweeperSkipsEpochWhoseLeaseExists` | P5b sweep-abort, 90s outages, Logic restart, forced sweep |
| 13 Two loops generating conn_ids | P3 `TestConnIDsAreUniqueAndCarryTheWorkerID`; P4 `TestLoopOwnershipUnderRace` | — |
| 14 Close during a Reconcile pass | P3 `TestFenceCHoldsLogoutUntilTheChunkIsResolved`, `TestFailedChunkIsResolvedAtSentMsPlusDeadlinePlusOneSecond` | P5b race 14 |
| 15 Write lost in failover | P1 `TestCompareAndSet`, `TestChunkWins`; P2 `TestReconcileRule3AfterLostWrite`, `TestReconcileRule3TieKeepsStored` | P6 Redis failover (Tier 2) |
| 16 Login in flight / Logout dropped around a pass | P1 `TestDeleteUntracked` (`login_ms ≥ as_of`, tracked); P3 `TestReconcileEndCarriesTheTrackedSetCollectedAfterAsOf`, `TestIncompleteTrackedSetSendsNoReconcileEnd` | P5b race 16, ReconcileEnd tests |
| 17 Slot logout, Kick lost | P1 `TestRevokeDeletesOnlyAffectedFields`, `TestLoginRevoked`, `TestCompareAndSet` (slot-scope subtests); P2 `TestReconcileRule1Revoked`, `TestReconcileConcurrentChange` | P5b slot-scoped Revoke |
| 18 Client Logout during a chunk | P3 `TestClientLogoutDuringAChunkGetsLogoutRespAtOnce` | P5b race 18 |

### Consistency contract rows

| Row | Pinned by |
|---|---|
| Ghost after a dropped Logout, ≤ 1 cycle | P1 `TestDeleteUntracked`; P5b dropped-Logout repair |
| Old owner still connected after replace/revoke, ≤ 1 cycle (revocation within the 30d marker TTL) | P1 `TestRevokeMarkerScriptIsMaxIdempotentAndRefreshesTTL`; P2 `TestReconcileRule1Revoked`, `TestReconcileRule3StoredNewerIsNotOwner`; P5b Kick-dropped and revocation-outlives-token tests |
| Dead gateway's entries: invalid after lease TTL, deleted after 2 × TTL + guard + sweep | P1 `TestSweepDeletesOnlyDeadEpochFields`, `TestEndEpochAndLeasesExist`; P2 `TestSweeperSweepsDeadEpochAfterGuard`, `TestSweeperEndedEpochIsExempt`; P3 `TestNoLeaseRenewalAfterEpochEnded`; P5b gateway crash → sweep |
| Live connection missing from Redis, ≤ 1 cycle / ~1 min after `missing` | P1 `TestCompareAndSet`, `TestRenewLease`; P3 `TestFirstLeaseMissingIsIgnoredAndALaterOneTriggersAPass`, `TestRenewalAfterFailuresForALeaseTTLTriggersAPass`, `TestE2EReconcileRestoresAndDeletesGhosts`; P5b forced sweep, stale lease observation |
| Healthy session reads invalid during an outage | P5b Redis paused 90s, gateway→Logic blocked 90s |

### Tiers

| Tier | Plans that add tests to it |
|---|---|
| 1a (every change, dev machine) | 1, 2, 3, 4, 5a (SDK and scripted-client tests) |
| 1b (before every commit, dev machine, test timing profile) | 5b (on the dev machine with miniredis, and on the Tier 2 host with real Redis, choice #47) |
| 2 (dedicated host) | 6 |
| 3 (`TARGET=local` or `cloud`) | 6 |

## Dev machine and test tiers

The dev machine (Tier 1a/1b, 2 cores / 2GB) has Go 1.27.1. Plan 1, Task 1 installs gcc and make for `-race`.

**User decision (2026-10-07): Docker is not installed on the dev machine.** This overrides the spec's "Lua tests against a real Redis in Docker (Tier 1a)". Tier 1a Lua tests run on in-process miniredis. Tests that need real Redis skip with `TODO: Tier 2 integration` and run on the Tier 2 host (`GOIM_TEST_TIER=2 make redis-up test-redis`). Each plan's *Execution Notes* lists the risks this leaves open.

This also affects Plan 5: the spec puts the race integration suite (`deploy/compose`, 4 gateways, Tier 1b) on the dev machine, which needs Docker. **Decided 2026-10-08 (choice #47, amending P4I-6):** the suite runs the real binaries as local processes, with a miniredis server process on the dev machine and the compose Redis primary + replica on the Tier 2 host (`make test-tier2 SCENARIO=race`); scenarios that need real Redis skip on the dev machine.
