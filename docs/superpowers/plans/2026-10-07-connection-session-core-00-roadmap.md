# Connection & Session Core — Plan Roadmap

> Index of the implementation plans for sub-project 1. Each plan is a separate document and produces working, tested software on its own. Plans are written and reviewed one at a time: a later plan is written against the reviewed interfaces of the plans before it.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev5.md` (authoritative; the zh-CN translation is not).

## The plans

| # | Plan | Builds | Depends on | Spec sections | Status |
|---|---|---|---|---|---|
| 1 | `2026-10-07-connection-session-core-01-session-store.md` | Repo skeleton, Tier 1a test Redis, `internal/session` (entry format, ordering rule), `internal/store` (every Redis key and Lua script: login, logout, revoke, Reconcile compare-and-set, ReconcileEnd delete, lease, sweeper ops) | — | *Redis schema*, *Login Lua*, *Logout Lua*, *Sweeper* (store side), *Revoke* (store side), *Reconcile handling* (store side) | written; Native execution approved 2026-10-07 |
| 2 | Logic service | `api/internal.proto` + generated code; JWT/JWKS verification with unknown-`kid` handling; `cmd/devtoken`; Logic RPCs (`Login`, `Logout`, `GatewayLease` on its own pool, `EpochEnded`, `Reconcile` rules 1–4, `ReconcileEnd`, `Revoke`); hardened Kick client; sweeper loop and mass-stale guard; readiness; Logic metrics; `cmd/logic` | 1 | *Logic*, *Sweeper*, *Auth*, *Observability* (Logic side) | not written |
| 3 | Gateway core | Transport-independent gateway: loopState and `conn_id`, timing wheel, connection state machine on a fake transport, admission (connection cap, pending cap, Login-queue cap), Login batching, Logout fence (a)(b)(c) and retry queue, lease loop, Reconcile pass controller, Kick server, readiness, drain; gateway metrics; layer-rule import test (no gateway package imports `internal/store`) | 2 (proto) | *Gateway* (all but the TCP transport), *Timers* | not written |
| 4 | TCP transport | Frame codec (6-byte header), Noise NK with `key_id` prologue, handshake limiter (semaphore + token bucket), gnet v2.10.0 integration, gnet loop-ownership `-race` test, `cmd/gateway` | 3 | *Client protocol*, *Transport seam*, *Noise key runbook* | not written |
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

## Invariant index (R4M-10)

Every race, consistency-contract row and spec test maps to the plan and test that pin it. Plan 1's test names are final; names for plans 2–6 are planned and are fixed when each plan is written.

### Races (spec *Races handled*)

| Race | Tier 1a (unit / Lua) | Tier 1b (integration) |
|---|---|---|
| 1 Delayed disconnect | P1 `TestLogoutCompareAndDelete` (race 1 subtest) | P5 race 1 |
| 2 Delayed login, same device | P1 `TestLoginTable` (STALE rows), `TestLoginExpired` | P5 race 2 |
| 3 Delayed login, other device | P1 `TestLoginTable` (SUPERSEDED row) | P5 race 3 |
| 4 Zombie gateway, same pod name | P1 `TestSweepDeletesOnlyDeadEpochFields`; P3 Kick epoch-mismatch test | P5 race 4 |
| 5 Close while Login in flight | P3 Logout fence (a) test | P5 race 5 |
| 6 No definitive reply, Redis written | P3 Logout fence (b) test | P5 race 6, both orders, Logic crash |
| 7 Logout lost, gateway alive | P1 `TestDeleteUntracked`; P2 ReconcileEnd handler | P5 dropped-Logout repair |
| 8 Kick lost or early | P3 kick-pending test | P5 Kick tests |
| 9 Two devices kick each other | P5 SDK: no reconnect after `KICKED_BY_OTHER_DEVICE` | — |
| 10 Shared device_id | P1 `TestLoginTable` (REPLACED row); P5 SDK: no reconnect after `REPLACED` | P5 race 10 |
| 11 Counter regression | P1 `TestLoginTable` (regression and STALE rows), `TestChunkWins` | P2 rule 3 with regressed counter (Tier 1a, real Redis) |
| 12 Swept while healthy | P1 `TestRenewLease` (missing after DEL); P2 guard and abort tests | P5 sweep-abort, 90s outages, Logic restart, forced sweep |
| 13 Two loops generating conn_ids | P3 conn_id test; P4 gnet loop-ownership `-race` test | — |
| 14 Close during a Reconcile pass | P3 Logout fence (c) test | P5 race 14 |
| 15 Write lost in failover | P1 `TestCompareAndSet`, `TestChunkWins`; P2 rule 3 after a lost write | P6 Redis failover (Tier 2) |
| 16 Login in flight / Logout dropped around a pass | P1 `TestDeleteUntracked` (`login_ms ≥ as_of`, tracked); P3 tracked-set collection after `as_of_ms` | P5 race 16, ReconcileEnd tests |
| 17 Slot logout, Kick lost | P1 `TestRevokeDeletesOnlyAffectedFields`, `TestLoginRevoked`, `TestCompareAndSet` (slot-scope subtests) | P5 slot-scoped Revoke |
| 18 Client Logout during a chunk | P3 client Logout through fence (c) | P5 race 18 |

### Consistency contract rows

| Row | Pinned by |
|---|---|
| Ghost after a dropped Logout, ≤ 1 cycle | P1 `TestDeleteUntracked`; P5 dropped-Logout repair |
| Old owner still connected after replace/revoke, ≤ 1 cycle (revocation within the 30d marker TTL) | P1 `TestRevokeMarkerScriptIsMaxIdempotentAndRefreshesTTL`; P2 rule 1; P5 Kick-dropped and revocation-outlives-token tests |
| Dead gateway's entries: invalid after lease TTL, deleted after 2 × TTL + guard + sweep | P1 `TestSweepDeletesOnlyDeadEpochFields`, `TestEndEpochAndLeasesExist`; P2 guard; P5 gateway crash → sweep |
| Live connection missing from Redis, ≤ 1 cycle / ~1 min after `missing` | P1 `TestCompareAndSet`, `TestRenewLease`; P5 forced sweep, stale lease observation |
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
