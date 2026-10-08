# Logic Service Implementation Plan — Plan 2 of 6

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Logic service on top of Plan 1's `internal/store`: the internal gRPC API, access-token verification with JWKS refresh, `cmd/devtoken`, every Logic RPC (`Login`, `Logout`, `Reconcile`, `ReconcileEnd`, `GatewayLease`, `EpochEnded`, `Revoke`), the hardened Kick client, the sweeper with its mass-stale guard, readiness, Logic's metrics, and `cmd/logic`.

**Architecture:** `api/internal.proto` defines four services: `Logic` (gateway → Logic), `Lease` (gateway → Logic, on its own port), `Revocation` (account service → Logic) and `Gateway` (Logic → gateway, Kick; implemented in Plan 3). `internal/auth` verifies Ed25519 JWS tokens itself (no JWT library) and keeps the key set fresh. `internal/logic` holds the handlers, the 1s lease cache, the Kick client (per-target bounded queue and workers), the sweeper, and the server that wires them. Handlers depend on a `Store` interface that `*store.Store` satisfies, so tests can wrap the real store to delay or fail one call.

**Tech Stack:** Go 1.27, `google.golang.org/grpc v1.84.0`, `google.golang.org/protobuf v1.36.12`, `protoc` 33.1 with `protoc-gen-go v1.36.12` and `protoc-gen-go-grpc v1.6.2`, `github.com/prometheus/client_golang v1.24.1`, `golang.org/x/time v0.16.0` (rate limiters), Plan 1's `internal/store` on go-redis v9.23.0, miniredis v2.39.0 at Tier 1a.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative), sections *Logic*, *Sweeper*, *Auth* (Decisions table and *JWKS*), *Revoke*, *Reconcile handling*, *Observability* (Logic side). Roadmap, implementation choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. Store interfaces: `docs/superpowers/plans/2026-10-07-connection-session-core-01-session-store.md` (Execution Notes: *Final review fixes* and *Interface review decisions* PI-1..PI-6 supersede the task code blocks there).

## Global Constraints

- Module path `github.com/Tallinzp/goim`. Layer rule: "The gateway never touches Redis. Only Logic touches Redis." Only `internal/logic` and `cmd/logic` import `internal/store`.
- Internal RPC (spec *Decisions*): "`Login` streams a result back per entry as soon as that entry finishes, so a slow Redis shard holds back only its own entries *(R5S-O1)*; Logic server sets `MaxConnectionAge` ≈5m **and `MaxConnectionAgeGrace` = 30s** (above the 6s Login timeout) so in-flight batches finish after GOAWAY *(R1-O12)*. `GatewayLease` uses its own gRPC connection *(R1-O2)*."
- Logic RPCs (spec *Logic*): "`GatewayLease` (own channel and **separate worker pool**, so Login load can't starve it)"; "`EpochEnded` (`DEL`s the epoch's lease key, so the epoch is invalid at once …)".
- "**Kick after the result** *(R5S-O2)*: the Kick follow-up is queued **after** the entry's result is returned; a Login result never waits for a Kick."
- Hardened Kick: the **first attempt is always sent**; "**Retries** (up to 3 over ~30s, on transport errors) happen only while the target `gw|epoch` lease is live. Logic caches lease state for ~1s"; "Kick RPCs are **fail-fast** (no `WaitForReady`)"; "every attempt, first and retries, has its own deadline (default ~1s, configurable) covering connect and RPC"; bounded queue per target `gw|epoch`, **64 workers and a queue of 1024** (roadmap choice #12); a full queue drops and counts `kick_dropped{target_lease=live|dead}`.
- JWKS: "Refresh every 5 min"; "Logic reports **Ready only once it holds a key set** from either source"; unknown `kid` "is **rejected at once** as `KEY_UNKNOWN`; Login never waits on a fetch"; "at most one unknown-`kid` refresh per **30s per Logic pod**, global across kids"; single-flight; negative cache.
- Revoke (PI-4): "Logic rejects `before_ms ≤ 0` or more than 5 minutes ahead of Redis `TIME` with `INVALID_ARGUMENT` (not retryable)." The marker must be confirmed by `WAIT` before any field is deleted and before `Revoke` returns success.
- Reconcile handling, in this order: rule 1 (`tok_iat <` effective marker → `not_owner(REVOKED)`, write nothing); rule 2 (field holds this `gw|epoch|conn_id` → refresh TTL, ensure `gwconns`, `written`); rule 3 (different session with a **live** lease → login-table ordering with its regression window, ties to the stored session; chunk newer → compare-and-set, and kick the stored session **only if the compare-and-set wrote**; stored newer → `not_owner(REPLACED | KICKED_BY_OTHER_DEVICE)`); rule 4 (empty, or no live lease → compare-and-set restore).
- Delete phase (R5S-O8): "runs under a configurable rate limit (default ~10k uids/s, the sweeper's rate)"; "`ReconcileEnd` replies when the phase completes"; a retry with the same payload is safe (idempotent).
- Sweeper: stale after **2 × lease TTL** (60s); guard: "at least one **other** epoch renewed within **1.5 × the renewal interval (15s)**, **and this has held continuously for one lease TTL (30s)**" and "the sweeper's own Redis access has been healthy for one full **lease TTL**"; `EpochEnded` epochs exempt; abort on renewal between batches "without `UNLINK`ing `gwconns` and without removing the ZSET member, `DEL` the `sweep:gw:{epoch}` claim"; "Rate-limited to about 10k users/s".
- Clock skew (R4M-5, PI-5): "`clock_skew_ms{gw} = redis_time − gw_now_ms`"; "alert at |skew| > 500ms".
- Logs: "`slog` lines with `uid` and `conn_id`; a request ID passed in gRPC metadata."
- Test timing profile: "Gateway and Logic refuse to start if a profile breaks a required ratio (e.g. lease TTL ≤ failover time + one renewal interval)." `login_deadline` is not scaled.
- Tiers: Tier 1a runs on the dev machine (2 cores / 2GB) with `go test -race`, Redis on in-process miniredis. **No Docker on the dev machine** (user decision 2026-10-07). Tests that need real Redis call `testredis.RequireReal` and skip with `TODO: Tier 2 integration`.
- Roadmap implementation choices #1–#14 apply. In particular #3: `tok_iat` is in milliseconds, so the token's `iat` must be read with millisecond precision. Choices #15–#23 below are this plan's.

## Review Focus

1. **A token issued in the same millisecond as a revocation** (`iat == before_ms`). Expected: it logs in, because only tokens issued *before* the marker are revoked. Reading `iat` as a float64 can round `1759900000.123` down by a millisecond and reject it. Pinned in Task 2 (`TestVerifyRoundTripKeepsMilliseconds`) and Task 8 (`TestLoginRejections/token issued in the marker's millisecond logs in`).
2. **A gateway that cancels the Login stream as soon as it has every result.** Expected: the follow-ups (the `gwconns` SADD) still run. Otherwise the uid is missing from `gwconns` and the sweeper never cleans its field if that gateway dies. Pinned in Task 8 (`TestLoginFollowUpsOutliveTheStream`).
3. **Two logins for the same slot in one batch** (a reconnect within the gateway's 5ms batching window). Expected: they resolve in batch order; the later one wins and the earlier connection gets exactly one `REPLACED` Kick. Pinned in Task 8 (`TestLoginSameSlotTwiceInOneBatch`).
4. **An empty Login batch or Reconcile chunk.** Expected: an empty stream or reply, no error. Pinned in Task 8 (`TestLoginEmptyBatch`) and Task 9 (`TestReconcileEmptyChunk`).
5. **`ReconcileEnd` for an epoch that has no `gwconns` key yet** (the first pass of a gateway with no logins). Expected: `deleted = 0`, no error. Pinned in Task 10 (`TestReconcileEndWithoutConnsKey`).

## Implementation choices this plan makes (decided 2026-10-08)

The spec leaves these open. Each is restated in the task that implements it. They are recorded in the roadmap as #15–#23. All were accepted as proposed at review (2026-10-08); #22 was amended so a dropped Kick is labelled from the cache only (see *Review fixes* in Execution Notes).

15. **Proto tooling.** `protoc` 33.1 from the GitHub release zip (installed under `~/.local`, no sudo), plus `protoc-gen-go v1.36.12` and `protoc-gen-go-grpc v1.6.2` via `go install`. The generated code is committed, so building and testing need no protoc; `make proto` regenerates it byte for byte. One package `api/internalpb`, proto package `goim.internal.v1`.
16. **JWT verified by hand on the standard library**, not a JWT library. Logic accepts exactly one form: JWS compact, `alg: EdDSA`, a `kid` header, strict base64url, at most 4096 bytes. Claims are `sub` (uid), `appid`, `platform`, `iat` and `exp`. `iat` and `exp` are NumericDates with up to 9 fraction digits, read exactly to the millisecond (choice #3). `exp` is required and gets a 30s leeway for issuer↔Logic clock skew. Why: libraries parse `iat` as a float64 or round it to a global precision. This verifier is ~100 lines.
17. **Three Login result codes the spec's table lacks.**
    - `TOKEN_INVALID`: bad signature, malformed token, expired, or a token for another appid/platform. Client action (Plan 5 to confirm): get a new token from the account service, then log in. It is not a quick retryable reject.
    - `BAD_REQUEST`: the gateway sent a value the store can't hold (`device_id` with `|`, `conn_seq` > 2^53 − 1, unknown platform). It means a client or gateway bug.
    - `OUTCOME_UNKNOWN` (gateway-internal): no definitive reply; the gateway applies fence (b).

    Metrics get the labels `token_invalid` and `bad_request`. `ok` covers `WRITTEN`, `ALREADY` and `REPAIRED`; `replaced` covers `REPLACED` and `REGRESSED`.
18. **Ports.** `Logic` + `Revocation` on `:9000`, with `MaxConnectionAge` 5m and grace 30s. `Lease` (`GatewayLease`, `EpochEnded`) on a separate gRPC server on `:9001`, with its own worker pool and no `MaxConnectionAge`, so the renewal path never gets a GOAWAY. Admin HTTP (`/metrics`, `/healthz`, `/readyz`) on `:9090`. Worker pools: 64 concurrent Login/Logout/Reconcile calls, 16 lease calls. `ReconcileEnd` (rate-limited, long-running) and `Revoke` (rare) are not pooled.
19. **Reply shapes.**
    - `Logout` is unary with a per-entry `done` list; `false` means the outcome is unknown, so the gateway retries that entry.
    - `Reconcile` fails the whole RPC (`UNAVAILABLE`) if any entry's outcome is unknown. The gateway then resolves the chunk at its deadline, and by then the compare-and-set can no longer write.
    - An invalid Reconcile entry fails the chunk with `INVALID_ARGUMENT`. It can only come from a gateway bug, since every entry came from a Login result.
    - `Revoke` takes `all` plus `appid`/`platform` instead of a scope enum.
20. **Login follow-ups after the results.** One `gwconns` SADD per batch, after the stream's results, on a context detached from the stream (1s timeout). Not before each result: one slow `gwconns` shard would then hold back every result in the batch, which R5S-O1 forbids. A lost SADD is repaired by rule 2. The dead-epoch cross-check runs there too, reading lease state and the `gwepochs` score through the 1s cache, so a relogin storm toward one dead epoch reads `gwepochs` about once per second.
21. **Sweeper mechanics.**
    - The cycle runs every 5s, with SSCAN pages of 500 uids and an abort check after every batch.
    - `renewing_since` and Redis health are measured on the local clock; score ages on Redis `TIME`, the clock the scores use.
    - One rate limiter per Logic pod, shared by every epoch that pod sweeps.
    - A failed cycle resets both guard clocks.
    - The default measured failover time is 15s, used for the `lease TTL > failover + renewal` check until Tier 2 measures it.
22. **Kick mechanics.**
    - Workers per target are started on demand, up to 64, and retire after 1 minute idle; the last one out drops the target.
    - One gRPC connection per gateway pod name, shared by that pod's epochs, dialled as `dns:///<pod>.gateway.im.svc.cluster.local:9100`.
    - Retries are spaced 10s apart and happen only on `UNAVAILABLE` or `DEADLINE_EXCEEDED`.
    - The `target_lease` label is `unknown` when the lease can't be read. On the enqueue path (a drop, a failed dial) it comes from the lease cache only, and is `unknown` when nothing is cached, so `Kick` never reads Redis on the caller's goroutine.
23. **Lease cache.** One cache (~1s) serves Kick retries, Reconcile rules 3/4 and the cross-check. A renewal Logic just made marks the epoch live; an `EpochEnded` marks it dead.

## Execution Notes

- **Branch:** Plan 1 (`feat/plan1-session-store`) is not merged yet, so Plan 2 branches from it: `git checkout -b feat/plan2-logic feat/plan1-session-store`. Commit this plan before Task 1, so the branch diff holds only Plan 2's code.
- **Prototype:** every code block in this plan was compiled and run before the plan was written. Each task was applied in order to a copy of `4955b4f`, with `go vet ./...` and `go test ./...` passing after each one. The whole suite passed under `-race` several times in a row on the dev machine. Two flaky tests were found and fixed that way: a signature tamper that only changed base64 padding bits, and the miniredis SSCAN issue below.
- **Execution mode:** Native (decided 2026-10-08), as for Plan 1: the tasks are implemented in one session with superpowers:executing-plans. The code is complete and verified, the tasks share test helpers closely (`env`, `kickRecorder`, `stableScan`), and per-task subagents would rebuild that context 13 times on a 2-core machine. One final whole-branch review runs on the most capable model and lists every risk not yet verified on real Redis.
- **Review fixes (2026-10-08).** Applied to the prototype, then copied into the tasks; `go vet ./...` and `go test -race ./...` passed, and each new test was checked to fail without its fix.
  1. **Kick could block its caller (Task 7).** A full queue or a failed dial labelled the drop with `leaseLabel`, which can read Redis for up to the 1s Kick deadline. That ran on the caller's goroutine. `LoginEach` calls `done` one entry after another per node group, so one blocked Kick held back the later entries' results (R5S-O2, R5S-O1). With a slow lease read, 10 drops took 5s. Fix: `LeaseCache.Peek` and `cachedLeaseLabel` (cache only, `unknown` when nothing is cached). Pinned by `TestKickDropDoesNotReadRedis`. `TestKickQueueFullDrops` now expects `unknown`, because no attempt has failed yet.
  2. **A failed Reconcile chunk dropped kicks for writes that happened (Task 9).** If one entry's compare-and-set outcome was unknown, the handler returned before kicking the sessions that other entries' writes had already replaced. They stayed connected until the next pass. Fix: record the first unknown outcome, run the kicks for `WRITTEN` overrides, then fail the RPC. Pinned by `TestReconcileKicksWrittenOverrideWhenChunkFails`.
  3. **Request IDs are capped at 64 bytes (Task 13)**, so a caller can't put an unbounded string into every log line. Pinned in `TestRequestIDFromMetadata`.
- **Not verified at Tier 1a, and where each is closed:**
  1. **Revoke's success path.** miniredis answers `WAIT` with 0 replicas, so locally the real store always returns `ErrNotReplicated`. `TestRevokeUnconfirmedMarkerIsRetryable` covers that branch; the success branch runs on a fake store. `TestRevokeEndToEndOnRealRedis` runs the full path on Tier 2.
  2. **SSCAN while the delete phase removes members.** A miniredis SSCAN cursor is an index into the sorted members, so the SREMs that `DeleteUntracked` makes mid-scan skip members (found while prototyping). Real Redis guarantees a full scan returns every member present throughout. The ReconcileEnd tests wrap `ScanConns` in `stableScan` (a snapshot), so the handler's paging is tested, but real SSCAN under SREM is not. A real `gwconns` of up to 128 members is listpack-encoded and comes back in one page anyway. Open until Plan 6's `gwconns` shard load test (200k members).
  3. **Sweeper cycles on real Redis.** A cycle reads and sweeps the global `gwepochs` key. The shared Tier 2 Redis holds other tests' epochs, and other packages run in parallel, so the sweeper-loop tests skip on real Redis with a message that is not `TODO: Tier 2 integration`. Their store primitives are verified there by Plan 1. Open until Plan 5's compose suite: gateway crash → sweep, kill 2 and 3 gateways, Redis paused, forced sweep.
  4. **Kicks to real pod DNS.** A dead pod is simulated by a local listener that accepts and never answers. NXDOMAIN after a pod is deleted, a stale DNS cache, and `publishNotReadyAddresses` are untested until Plan 5 (compose) and Plan 6 (k8s).
  5. **`MaxConnectionAge` GOAWAY with a Login stream in flight.** Only configured; exercised by Plan 5's race 6 ("`MaxConnectionAge` reset").
  6. **Pool separation under CPU starvation.** `TestLeasePoolBoundsConcurrency` shows the lease pool is separate. Whether renewals keep their 10s rhythm while Login saturates the CPU is Tier 2's "storm: the other gateways never miss a lease renewal" (Plan 6).
  7. **Redis Cluster.** `cmd/logic` builds a `ClusterClient` by default; no test runs one (Plan 1 risks 7, 9, 10b, 14 and 16 carry over).
  8. **A real account service's JWKS.** The format is the standard OKP/Ed25519 JWK (RFC 8037). `devtoken serve` stands in for the account service.
- **Implemented 2026-10-08** (Native) as 13 task commits `00ce33e..b06cc93`, then a final whole-branch review on Opus: 0 Critical, 1 Important, 8 Minor. The task code is the code blocks above; the two final fixes below supersede them.
- **Final review fixes:**
  1. **Kick connections are per gateway epoch, not per pod name** (`internal/logic/kick.go`). With one connection per pod name, a node loss left it stuck on the dead IP (no RST, no keepalive), and Kicks to the pod's new epoch after a restart went down that transport and failed for minutes. Now each target `gw|epoch` dials its own connection and closes it when its last worker retires. Pinned by `TestKickNewEpochOfSamePodGetsItsOwnConnection`. Not done: client keepalive and a shorter reconnect backoff. A restarted pod already gets a fresh dial, and keepalive would add a ping-rate rule for Plan 3.
  2. **No unknown-`kid` refresh after the key manager's `Run` ends** (`internal/auth/manager.go`). During a drain, Logins are still verified after `Run`'s context is cancelled. A refresh started then raced `Run`'s final `bg.Wait` and failed on the cancelled context, counting a false `jwks_refresh_failures_total` on every rollout. Pinned by `TestNoUnknownKidRefreshAfterRun`.
- **Deferred minors (final review):**
  - The sweeper releases its claim on the cancelled context at shutdown, so the claim lingers for 300s after a rolling deploy (cleanup delay only).
  - A `ReconcileEnd` or drain that runs longer than `MaxConnectionAgeGrace` or `ShutdownGrace` (30s, about 280k `gwconns` members at 10k/s) is cut and retried. Plans 3 and 6 size the deadlines.
  - `gw_now_ms = 0` sets a false `clock_skew_alert`.
  - `LeaseCache.Live` can overwrite a newer `Set` observation for up to 1s.
  - `cmd/logic` lacks flags for `SweepBatch`, `DeleteBatch`, `Kick.Retries`, `RetrySpacing` and `IdleTimeout`. Plan 5's test profile needs them.
  - `OUTCOME_UNKNOWN` Login entries get no `gwconns` SADD. If the epoch then dies, their fields (already invalid) wait for the 30-day TTL.
  - `Server.Start` ignores `Serve` errors; retries queued after `Kicker.Close` are dropped without being counted; `Server.Stop` hangs if `Start`'s context is never cancelled.
- **Unverified risks the final review added** to the list above:
  9. R5S-O1 under a real shard failover: per-master grouping has only run with one group on miniredis.
  10. Review Focus 3 on a real cluster: batch order across MOVED/ASK or a NOSCRIPT re-run after failover.
  11. How often one slow shard fails a whole Reconcile chunk or delete-phase page during a failover.
  12. Guard clock source: `TIME` from one node against scores from each lease shard (ms-scale, low risk).
  13. go-redis with a context cancelled mid-pipeline: the `OUTCOME_UNKNOWN` paths are tested only on miniredis.
  14. `ReconcileEnd` wall time at 200k members with real Redis latency, against the 30s graces.
  15. TCP dead-peer detection on the Kick path (node loss with no RST).
- **Tier 2:** `make test-tier2 SCENARIO=store` runs `make test-redis`, which runs `./...`, so Plan 2's tests run on real Redis too, including `TestRevokeEndToEndOnRealRedis`. The scenario's no-skip check still applies: no test may skip with `TODO: Tier 2 integration`.
- **For Plan 3 (gateway), the Logic contract this plan fixes:**
  - Read the Login stream to EOF. An entry with no result at EOF, a stream error, or `OUTCOME_UNKNOWN` has no definitive reply: fence (b).
  - Retry Logout entries whose `done` is false.
  - A Reconcile RPC error means the chunk is resolved at `sent_ms + login_deadline + 1s`.
  - A `GatewayLease` error is treated as `missing` (Plan 1 note).
  - Dial the `Lease` service on its own port with its own connection.
  - Send `x-request-id` in gRPC metadata (at most 64 bytes; longer IDs are cut).
  - Client keepalive pings no more often than every 10s: both Logic servers enforce `MinTime` 10s and close connections that ping faster.
  - Implement `Gateway.Kick` and ignore epoch mismatches and unknown `conn_id`s.

---

## File Structure

| File | Responsibility |
|---|---|
| `api/internal.proto` | The four internal services and their messages |
| `api/internalpb/internal.pb.go`, `internal_grpc.pb.go` | Generated by `make proto`, committed |
| `api/internalpb/size_test.go` | `ReconcileEnd` at the 250k cap fits gRPC's 4MB default |
| `Makefile` | adds `proto` |
| `internal/auth/jwt.go` | `Verify`, `Sign`, `Claims`, `KeySet`, exact NumericDate parsing |
| `internal/auth/jwks.go` | JWKS parse/marshal, private JWK key files, `GenerateKey` |
| `internal/auth/manager.go` | `Manager`: startup load with ConfigMap fallback, refresh, unknown-`kid` gate, `Ready` |
| `cmd/devtoken/main.go` | `keygen`, `jwks`, `issue`, `serve` |
| `internal/logic/config.go` | `Config`, `KickConfig`, defaults, ratio checks |
| `internal/logic/store.go` | `Store` interface (`*store.Store` satisfies it) |
| `internal/logic/metrics.go` | every Logic metric |
| `internal/logic/leases.go` | `LeaseCache` (~1s lease and `gwepochs` score observations) |
| `internal/logic/log.go` | request-scoped `logger(ctx)` |
| `internal/logic/convert.go` | proto ↔ store conversions, gRPC status helpers, worker `pool` |
| `internal/logic/lease.go` | `LeaseService`: `GatewayLease` (+ clock skew), `EpochEnded` |
| `internal/logic/kick.go` | `Kicker`, `DialFunc`, `PodDNSDial` |
| `internal/logic/service.go` | `Service`, `Verifier`, `KickSender` |
| `internal/logic/login.go`, `logout.go` | `Login` (streamed), `Logout` |
| `internal/logic/reconcile.go` | `Reconcile` rules 1–4 |
| `internal/logic/reconcile_end.go` | `ReconcileEnd` delete phase |
| `internal/logic/revoke.go` | `Revoke` |
| `internal/logic/guard.go`, `sweeper.go` | mass-stale `Guard`, `Sweeper` |
| `internal/logic/server.go` | `Server`: gRPC servers, interceptors (request ID, latency), admin HTTP, readiness, shutdown |
| `cmd/logic/main.go` | flags → config, Redis client, signal handling |

Every `.go` file under `internal/logic` has its tests next to it. Shared test helpers are in `internal/logic/helpers_test.go` (Task 5), `service_test.go` (Task 8: the `env` harness), `kick_test.go` (Task 7: fake gateway, blackhole, `waitFor`) and `reconcile_test.go` (Task 9: `put`/`get`).

---

### Task 1: Internal proto, generated code and dependencies (choices #15, #18, #19)

**Files:**
- Create: `api/internal.proto`, `api/internalpb/internal.pb.go` and `api/internalpb/internal_grpc.pb.go` (generated), `api/internalpb/size_test.go`
- Modify: `Makefile`, `go.mod`, `go.sum`

**Interfaces:**
- Consumes: nothing.
- Produces (package `internalpb`, import as `pb`):
  - services: `LogicServer` / `LogicClient` (`Login(*LoginBatch, grpc.ServerStreamingServer[LoginResult]) error`, `Logout`, `Reconcile`, `ReconcileEnd`); `LeaseServer` (`GatewayLease`, `EpochEnded`); `RevocationServer` (`Revoke`); `GatewayServer` / `GatewayClient` (`Kick`); `Register*Server`, `New*Client`, `Unimplemented*Server`.
  - messages: `GatewayRef{Gateway, Epoch}`, `LoginEntry`, `LoginBatch`, `LoginResult{Index, Code, Uid, TokIatMs, StoredConnSeq}`, `LogoutEntry`, `LogoutBatch`, `LogoutReply{Done []bool}`, `ReconcileEntry`, `ReconcileChunk{Gw, SentMs, Entries}`, `ReconcileResult{Code, Reason}`, `ReconcileReply`, `ReconcileEndRequest{Gw, AsOfMs, TrackedConnIds, Pace}`, `ReconcileEndReply{Deleted}`, `LeaseRequest{Gw, GwNowMs}`, `LeaseReply{Missing}`, `EpochEndedRequest`, `RevokeRequest{Uid, All, Appid, Platform, BeforeMs, Reason}`, `RevokeReply{Kicked}`, `KickRequest{Gw, ConnId, Reason}`.
  - enums: `Platform_PLATFORM_{MOBILE,PAD,WEB,PC}`, `KickReason_KICK_REASON_{REPLACED,KICKED_BY_OTHER_DEVICE,REVOKED}`, `LoginCode_LOGIN_CODE_{OK,STALE,SUPERSEDED,EXPIRED,REVOKED,KEY_UNKNOWN,TOKEN_INVALID,BAD_REQUEST,OUTCOME_UNKNOWN}`, `ReconcileCode_RECONCILE_CODE_{WRITTEN,UNCHANGED,REFUSED,NOT_OWNER}`, `Pace_PACE_{PERIODIC,TRIGGERED}`.
  - `make proto`.

- [ ] **Step 1: Install protoc and the two plugins (no sudo)**

```bash
mkdir -p ~/.local/protoc
curl -sSL -o /tmp/protoc.zip https://github.com/protocolbuffers/protobuf/releases/download/v33.1/protoc-33.1-linux-x86_64.zip
python3 -m zipfile -e /tmp/protoc.zip ~/.local/protoc
chmod +x ~/.local/protoc/bin/protoc
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
~/.local/protoc/bin/protoc --version
```

Expected: `libprotoc 33.1`. Both plugins are now in `$(go env GOPATH)/bin`.

- [ ] **Step 2: Write the proto**

`api/internal.proto`:

```protobuf
// Internal RPCs between gateways, Logic and the account service
// (spec: Architecture, Logic). The client protocol is api/client.proto (Plan 4).
syntax = "proto3";

package goim.internal.v1;

option go_package = "github.com/Tallinzp/goim/api/internalpb";

// GatewayRef names one gateway instance: pod name plus its boot epoch.
message GatewayRef {
  string gateway = 1;
  fixed64 epoch = 2;
}

// Platform is the device class of a slot (userid + appid + platform).
enum Platform {
  PLATFORM_UNSPECIFIED = 0;
  PLATFORM_MOBILE = 1; // Android and iOS share one slot
  PLATFORM_PAD = 2;
  PLATFORM_WEB = 3;
  PLATFORM_PC = 4;
}

// KickReason is sent to the client in the Kick frame. None of them makes the
// client reconnect on its own.
enum KickReason {
  KICK_REASON_UNSPECIFIED = 0;
  KICK_REASON_REPLACED = 1;               // same device, newer connection
  KICK_REASON_KICKED_BY_OTHER_DEVICE = 2; // another device took the slot
  KICK_REASON_REVOKED = 3;                // token revoked: drop it, show login
}

// ---- Gateway -> Logic: Login, Logout, Reconcile, ReconcileEnd ----

service Logic {
  // Login streams one LoginResult per entry as soon as that entry finishes
  // (R5S-O1). An entry with no result when the stream ends has no definitive
  // reply: the gateway applies Logout fence (b) to it.
  rpc Login(LoginBatch) returns (stream LoginResult);
  rpc Logout(LogoutBatch) returns (LogoutReply);
  // Reconcile resolves a chunk as a whole (roadmap choice #14). A gRPC error
  // means no definitive reply: the chunk is resolved at sent_ms + login_deadline + 1s.
  rpc Reconcile(ReconcileChunk) returns (ReconcileReply);
  // ReconcileEnd returns after the rate-limited delete phase (R5S-O8).
  rpc ReconcileEnd(ReconcileEndRequest) returns (ReconcileEndReply);
}

message LoginEntry {
  fixed64 conn_id = 1;
  string token = 2;
  string appid = 3;
  Platform platform = 4;
  string device_id = 5;
  uint64 conn_seq = 6;
  int64 recv_ms = 7; // gateway wall clock when the Login frame arrived (R1-O4)
}

message LoginBatch {
  GatewayRef gw = 1;
  repeated LoginEntry entries = 2;
}

enum LoginCode {
  LOGIN_CODE_UNSPECIFIED = 0;
  LOGIN_CODE_OK = 1;          // the session is stored; uid and tok_iat_ms are set
  LOGIN_CODE_STALE = 2;       // stored_conn_seq is set
  LOGIN_CODE_SUPERSEDED = 3;
  LOGIN_CODE_EXPIRED = 4;
  LOGIN_CODE_REVOKED = 5;
  LOGIN_CODE_KEY_UNKNOWN = 6;
  LOGIN_CODE_TOKEN_INVALID = 7; // bad signature, malformed, expired, or appid/platform mismatch
  LOGIN_CODE_BAD_REQUEST = 8;   // the gateway sent a value the store can't hold
  // OUTCOME_UNKNOWN is not a definitive reply: the script may or may not have
  // run (Redis error, timeout). The gateway applies Logout fence (b).
  LOGIN_CODE_OUTCOME_UNKNOWN = 9;
}

message LoginResult {
  uint32 index = 1; // position of the entry in LoginBatch.entries
  LoginCode code = 2;
  string uid = 3;          // OK only: the token's subject
  int64 tok_iat_ms = 4;    // OK only: the token's iat in ms (roadmap choice #3)
  uint64 stored_conn_seq = 5; // STALE only
}

message LogoutEntry {
  fixed64 conn_id = 1;
  string uid = 2;
  string appid = 3;
  Platform platform = 4;
}

message LogoutBatch {
  GatewayRef gw = 1;
  repeated LogoutEntry entries = 2;
}

message LogoutReply {
  // done[i] is true when entry i is settled (deleted, owned by a newer
  // session, or invalid); false means the outcome is unknown: retry it.
  repeated bool done = 1;
}

message ReconcileEntry {
  fixed64 conn_id = 1;
  string uid = 2;
  string appid = 3;
  Platform platform = 4;
  string device_id = 5;
  uint64 conn_seq = 6;
  int64 login_ms = 7;
  int64 tok_iat_ms = 8;
}

message ReconcileChunk {
  GatewayRef gw = 1;
  int64 sent_ms = 2; // gateway wall clock when the chunk was sent (R2-O1)
  repeated ReconcileEntry entries = 3;
}

enum ReconcileCode {
  RECONCILE_CODE_UNSPECIFIED = 0;
  RECONCILE_CODE_WRITTEN = 1;   // refreshed (rule 2) or written (rules 3, 4)
  RECONCILE_CODE_UNCHANGED = 2; // compare-and-set lost to a concurrent change
  RECONCILE_CODE_REFUSED = 3;   // past sent_ms + login_deadline: re-queue
  RECONCILE_CODE_NOT_OWNER = 4; // Kick with reason and close
}

message ReconcileResult {
  ReconcileCode code = 1;
  KickReason reason = 2; // NOT_OWNER only
}

message ReconcileReply {
  repeated ReconcileResult results = 1; // same order as the chunk's entries
}

enum Pace {
  PACE_UNSPECIFIED = 0;
  PACE_PERIODIC = 1;
  PACE_TRIGGERED = 2;
}

message ReconcileEndRequest {
  GatewayRef gw = 1;
  int64 as_of_ms = 2;
  repeated fixed64 tracked_conn_ids = 3; // packed: 8 bytes each
  Pace pace = 4; // metric label only
}

message ReconcileEndReply {
  uint32 deleted = 1;
}

// ---- Gateway -> Logic, dedicated connection and port (R1-O2) ----

service Lease {
  rpc GatewayLease(LeaseRequest) returns (LeaseReply);
  rpc EpochEnded(EpochEndedRequest) returns (EpochEndedReply);
}

message LeaseRequest {
  GatewayRef gw = 1;
  int64 gw_now_ms = 2; // gateway wall clock, for clock_skew_ms (R4M-5)
}

message LeaseReply {
  bool missing = 1; // the lease key was absent before this renewal (R3-O3)
}

message EpochEndedRequest {
  GatewayRef gw = 1;
}

message EpochEndedReply {}

// ---- Account service -> Logic ----

service Revocation {
  // Revoke returns UNAVAILABLE when no replica confirmed the marker; the
  // account service retries with the same before_ms (PI-4).
  rpc Revoke(RevokeRequest) returns (RevokeReply);
}

message RevokeRequest {
  string uid = 1;
  bool all = 2;          // scope=all; otherwise scope=slot (appid, platform)
  string appid = 3;
  Platform platform = 4;
  int64 before_ms = 5;   // issuer clock, unit of tok_iat (PI-4)
  string reason = 6;     // logged only
}

message RevokeReply {
  uint32 kicked = 1; // connections queued for a REVOKED Kick
}

// ---- Logic -> Gateway (pod DNS) ----

service Gateway {
  // Kick is ignored by the gateway on an epoch mismatch or an unknown conn_id.
  rpc Kick(KickRequest) returns (KickReply);
}

message KickRequest {
  GatewayRef gw = 1;
  fixed64 conn_id = 2;
  KickReason reason = 3;
}

message KickReply {}
```


- [ ] **Step 3: Add the `proto` target to the Makefile**

Change the `.PHONY` line to `.PHONY: test proto redis-up redis-down test-redis test-tier2`, and add after the `test` target:

```make
# Regenerate api/internalpb from api/internal.proto. Needs protoc 33.1 and the
# two pinned plugins (Plan 2, Task 1); the generated code is committed.
PROTOC ?= protoc
GOBIN_DIR := $(shell go env GOPATH)/bin
proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) --go_out=. --go_opt=module=github.com/Tallinzp/goim \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Tallinzp/goim api/internal.proto
```

- [ ] **Step 4: Generate the code and add the dependencies**

```bash
make proto PROTOC=~/.local/protoc/bin/protoc
go get google.golang.org/grpc@v1.84.0 google.golang.org/protobuf@v1.36.12
go mod tidy
```

Expected: `api/internalpb/internal.pb.go` and `internal_grpc.pb.go` exist; their headers name `protoc-gen-go v1.36.12`, `protoc-gen-go-grpc v1.6.2` and `protoc v6.33.1`. Running `make proto` again leaves them byte-identical. Each later dependency is added, pinned, by the task that first imports it (Task 5: Prometheus; Task 10: `x/time`). Never let `go mod tidy` pick a version.

- [ ] **Step 5: Write the size test**

The spec puts `tracked_conn_ids` at the 250k cap at ≈2MB, under gRPC's 4MB default. This pins that the field is packed `fixed64` (a `uint64` varint would be up to 10 bytes per random id).

`api/internalpb/size_test.go`:

```go
package internalpb

import (
	"math/rand/v2"
	"testing"

	"google.golang.org/protobuf/proto"
)

// Spec (Reconcile, pass end): tracked_conn_ids at the 250k cap is 8 bytes
// each, ≈2MB, under gRPC's 4MB default message limit.
func TestReconcileEndFitsDefaultLimitAtCap(t *testing.T) {
	ids := make([]uint64, 250_000)
	for i := range ids {
		ids[i] = rand.Uint64()
	}
	b, err := proto.Marshal(&ReconcileEndRequest{Gw: &GatewayRef{Gateway: "gw-17", Epoch: rand.Uint64()},
		AsOfMs: 1759900000123, TrackedConnIds: ids, Pace: Pace_PACE_TRIGGERED})
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 8*len(ids) || len(b) >= 4<<20 {
		t.Fatalf("ReconcileEnd at the cap is %d bytes; want packed fixed64 (≥ %d) under 4MB", len(b), 8*len(ids))
	}
}
```


- [ ] **Step 6: Run it**

Run: `go vet ./... && go test -race -count=1 ./api/...`
Expected: `ok  github.com/Tallinzp/goim/api/internalpb`.

- [ ] **Step 7: Commit**

```bash
git add api Makefile go.mod go.sum
git commit -m "feat(api): internal gRPC API (Logic, Lease, Revocation, Gateway) and make proto"
```

---

### Task 2: Token verification (choice #16)

**Files:**
- Create: `internal/auth/jwt.go`, `internal/auth/jwks.go`
- Test: `internal/auth/jwt_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces (package `auth`):
  - `var ErrKeyUnknown, ErrInvalid error`; `const MaxTokenLen = 4096`
  - `type Claims struct { UID, AppID, Platform string; IatMs, ExpMs int64 }`
  - `type KeySet map[string]ed25519.PublicKey`
  - `func Verify(token string, keys KeySet, nowMs, leewayMs int64) (Claims, error)`: an unknown kid matches `ErrKeyUnknown`, every other failure `ErrInvalid`
  - `type PrivateKey struct { Kid string; Key ed25519.PrivateKey }`; `func Sign(k PrivateKey, c Claims) (string, error)`
  - `func ParseJWKS([]byte) (KeySet, error)`, `func MarshalJWKS(keys ...PrivateKey) ([]byte, error)`, `func GenerateKey(kid string) (PrivateKey, error)`, `func MarshalPrivateJWK(PrivateKey) ([]byte, error)`, `func ParsePrivateJWK([]byte) (PrivateKey, error)`

Why no JWT library (choice #16): Logic accepts exactly one form (EdDSA, `kid`, five claims), and `iat` must be read exactly to the millisecond (roadmap choice #3). A float64 parse can round `1759900000.123` down to `…122`; a token issued in the same millisecond as a revocation would then read as revoked. Strict base64 gives one string per token.

- [ ] **Step 1: Write the failing tests**

`internal/auth/jwt_test.go`:

```go
package auth

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func testKey(t *testing.T, kid string) PrivateKey {
	t.Helper()
	k, err := GenerateKey(kid)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func keySet(t *testing.T, keys ...PrivateKey) KeySet {
	t.Helper()
	doc, err := MarshalJWKS(keys...)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := ParseJWKS(doc)
	if err != nil {
		t.Fatal(err)
	}
	return ks
}

func claims(iatMs int64) Claims {
	return Claims{UID: "u1", AppID: "im", Platform: "mobile", IatMs: iatMs, ExpMs: iatMs + 3600_000}
}

func sign(t *testing.T, k PrivateKey, c Claims) string {
	t.Helper()
	tok, err := Sign(k, c)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestVerifyRoundTripKeepsMilliseconds(t *testing.T) {
	k := testKey(t, "k1")
	// 1759900000.123 is not exact as a float64; the ms must survive anyway.
	for _, iat := range []int64{1759900000123, 1759900000001, 1759900000999, 1000} {
		c := claims(iat)
		got, err := Verify(sign(t, k, c), keySet(t, k), iat, 0)
		if err != nil {
			t.Fatalf("iat %d: %v", iat, err)
		}
		if got != c {
			t.Fatalf("got %+v, want %+v", got, c)
		}
	}
}

func TestVerifyAcceptsEitherKeyDuringRotation(t *testing.T) {
	k1, k2 := testKey(t, "k1"), testKey(t, "k2")
	ks := keySet(t, k1, k2)
	for _, k := range []PrivateKey{k1, k2} {
		if _, err := Verify(sign(t, k, claims(5000)), ks, 5000, 0); err != nil {
			t.Fatalf("kid %s: %v", k.Kid, err)
		}
	}
}

func TestVerifyRejects(t *testing.T) {
	k, other := testKey(t, "k1"), testKey(t, "k1") // same kid, different key
	ks := keySet(t, k)
	good := sign(t, k, claims(5000))
	parts := strings.Split(good, ".")
	hdr := func(h string) string { return b64.EncodeToString([]byte(h)) }
	body := func(p string) string { return b64.EncodeToString([]byte(p)) }

	cases := []struct {
		name  string
		token string
		now   int64
		want  error
	}{
		{"unknown kid", sign(t, testKey(t, "k9"), claims(5000)), 5000, ErrKeyUnknown},
		{"wrong key for kid", sign(t, other, claims(5000)), 5000, ErrInvalid},
		{"tampered claims", parts[0] + "." + body(`{"sub":"u2","appid":"im","platform":"mobile","iat":5,"exp":9999}`) + "." + parts[2], 5000, ErrInvalid},
		{"alg none", hdr(`{"alg":"none","kid":"k1"}`) + "." + parts[1] + ".", 5000, ErrInvalid},
		{"alg HS256", hdr(`{"alg":"HS256","kid":"k1"}`) + "." + parts[1] + "." + parts[2], 5000, ErrInvalid},
		{"two segments", parts[0] + "." + parts[1], 5000, ErrInvalid},
		{"expired", good, 5000 + 3600_000, ErrInvalid},
		{"too long", strings.Repeat("a", MaxTokenLen+1), 5000, ErrInvalid},
	}
	for _, c := range cases {
		if _, err := Verify(c.token, ks, c.now, 0); !errors.Is(err, c.want) {
			t.Errorf("%s: err %v, want %v", c.name, err, c.want)
		}
	}
}

func TestVerifyRejectsNonCanonicalSignature(t *testing.T) {
	k := testKey(t, "k1")
	tok := sign(t, k, claims(5000))
	// A 64-byte signature is 86 base64 characters; the last one carries 2 data
	// bits and 4 padding bits. Setting a padding bit keeps the bytes the same.
	alpha := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	last := strings.IndexByte(alpha, tok[len(tok)-1])
	alt := tok[:len(tok)-1] + string(alpha[last|1])
	if alt == tok {
		alt = tok[:len(tok)-1] + string(alpha[last|2])
	}
	if _, err := Verify(alt, keySet(t, k), 5000, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-canonical signature: %v", err)
	}
}

func TestVerifyLeeway(t *testing.T) {
	k := testKey(t, "k1")
	tok := sign(t, k, claims(5000)) // exp 3605000
	if _, err := Verify(tok, keySet(t, k), 3605000+29_999, 30_000); err != nil {
		t.Fatalf("inside leeway: %v", err)
	}
	if _, err := Verify(tok, keySet(t, k), 3605000+30_000, 30_000); !errors.Is(err, ErrInvalid) {
		t.Fatalf("past leeway: %v", err)
	}
}

func TestVerifyRejectsBadClaims(t *testing.T) {
	k := testKey(t, "k1")
	ks := keySet(t, k)
	for _, p := range []string{
		`{"appid":"im","platform":"mobile","iat":5,"exp":99999999}`,               // no sub
		`{"sub":"u","appid":"im","platform":"mobile","iat":1.7e9,"exp":99999999}`, // exponent
		`{"sub":"u","appid":"im","platform":"mobile","iat":-5,"exp":99999999}`,    // negative
		`{"sub":"u","appid":"im","platform":"mobile","iat":5}`,                    // no exp
	} {
		tok := rawSign(t, k, p)
		if _, err := Verify(tok, ks, 1, 0); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err %v, want ErrInvalid", p, err)
		}
	}
}

// rawSign signs an arbitrary claims document.
func rawSign(t *testing.T, k PrivateKey, claimsJSON string) string {
	t.Helper()
	h, err := json.Marshal(header{Alg: "EdDSA", Kid: k.Kid})
	if err != nil {
		t.Fatal(err)
	}
	signing := b64.EncodeToString(h) + "." + b64.EncodeToString([]byte(claimsJSON))
	return signing + "." + b64.EncodeToString(ed25519.Sign(k.Key, []byte(signing)))
}

func TestParseMs(t *testing.T) {
	for in, want := range map[string]int64{
		"0": 0, "5": 5000, "5.1": 5100, "5.12": 5120, "5.123": 5123, "5.1239": 5123, "1759900000.123": 1759900000123,
	} {
		got, err := parseMs(in)
		if err != nil || got != want {
			t.Errorf("parseMs(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "-1", "1e3", "1.", ".5", "1.0000000001", "0x10"} {
		if _, err := parseMs(in); err == nil {
			t.Errorf("parseMs(%q) accepted", in)
		}
	}
}

func TestPrivateJWKRoundTrip(t *testing.T) {
	k := testKey(t, "k1")
	doc, err := MarshalPrivateJWK(k)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParsePrivateJWK(doc)
	if err != nil || got.Kid != "k1" || !got.Key.Equal(k.Key) {
		t.Fatalf("round trip: %+v, %v", got, err)
	}
	if _, err := ParseJWKS([]byte(`{"keys":[{"kty":"RSA","kid":"r"}]}`)); err == nil {
		t.Fatal("a JWKS with no Ed25519 key must be an error")
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/auth/`
Expected: FAIL to build: `undefined: GenerateKey`, `undefined: Verify`, ….

- [ ] **Step 3: Implement**

`internal/auth/jwt.go`:

```go
// Package auth verifies the account service's Ed25519 access tokens (spec:
// Auth, JWKS). It is a minimal JWS compact / EdDSA implementation: only what
// Logic accepts is parsed, and iat is read with millisecond precision.
package auth

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	// ErrKeyUnknown: the token's kid is not in the key set (R4H-O10).
	ErrKeyUnknown = errors.New("auth: unknown kid")
	// ErrInvalid: malformed, bad signature, wrong alg, missing claim or expired.
	ErrInvalid = errors.New("auth: invalid token")
)

// MaxTokenLen bounds the work spent on one token.
const MaxTokenLen = 4096

// Claims is what Logic reads from an access token.
type Claims struct {
	UID      string // sub
	AppID    string // appid
	Platform string // platform: mobile | pad | web | pc
	IatMs    int64  // iat, fractional seconds read exactly to ms (roadmap choice #3)
	ExpMs    int64  // exp
}

// KeySet maps kid to an Ed25519 public key.
type KeySet map[string]ed25519.PublicKey

type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ,omitempty"`
}

type payload struct {
	Sub      string      `json:"sub"`
	AppID    string      `json:"appid"`
	Platform string      `json:"platform"`
	Iat      json.Number `json:"iat"`
	Exp      json.Number `json:"exp"`
}

// b64 rejects non-canonical encodings (non-zero padding bits), so one token
// has exactly one string form.
var b64 = base64.RawURLEncoding.Strict()

// Verify checks token against keys at nowMs. A token is expired once
// nowMs ≥ exp + leewayMs. An unknown kid returns an error that matches
// ErrKeyUnknown; every other failure matches ErrInvalid.
func Verify(token string, keys KeySet, nowMs, leewayMs int64) (Claims, error) {
	if len(token) > MaxTokenLen {
		return Claims{}, fmt.Errorf("%w: %d bytes", ErrInvalid, len(token))
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("%w: not a JWS compact token", ErrInvalid)
	}
	var h header
	if err := decodeJSON(parts[0], &h); err != nil {
		return Claims{}, fmt.Errorf("%w: header: %v", ErrInvalid, err)
	}
	if h.Alg != "EdDSA" || h.Kid == "" {
		return Claims{}, fmt.Errorf("%w: alg %q kid %q", ErrInvalid, h.Alg, h.Kid)
	}
	pub, ok := keys[h.Kid]
	if !ok {
		return Claims{}, fmt.Errorf("%w: %q", ErrKeyUnknown, h.Kid)
	}
	sig, err := b64.DecodeString(parts[2])
	if err != nil || !ed25519.Verify(pub, []byte(parts[0]+"."+parts[1]), sig) {
		return Claims{}, fmt.Errorf("%w: bad signature", ErrInvalid)
	}
	var p payload
	if err := decodeJSON(parts[1], &p); err != nil {
		return Claims{}, fmt.Errorf("%w: claims: %v", ErrInvalid, err)
	}
	c := Claims{UID: p.Sub, AppID: p.AppID, Platform: p.Platform}
	if c.UID == "" || c.AppID == "" || c.Platform == "" {
		return Claims{}, fmt.Errorf("%w: sub, appid and platform are required", ErrInvalid)
	}
	if c.IatMs, err = parseMs(string(p.Iat)); err != nil {
		return Claims{}, fmt.Errorf("%w: iat: %v", ErrInvalid, err)
	}
	if c.ExpMs, err = parseMs(string(p.Exp)); err != nil {
		return Claims{}, fmt.Errorf("%w: exp: %v", ErrInvalid, err)
	}
	if nowMs >= c.ExpMs+leewayMs {
		return Claims{}, fmt.Errorf("%w: expired", ErrInvalid)
	}
	return c, nil
}

func decodeJSON(seg string, v any) error {
	raw, err := b64.DecodeString(seg)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	return d.Decode(v)
}

// numericDate is a JSON NumericDate as devtoken writes it: whole seconds with
// up to 9 fraction digits, no sign or exponent.
var numericDate = regexp.MustCompile(`^([0-9]{1,12})(?:\.([0-9]{1,9}))?$`)

// parseMs reads a NumericDate exactly to milliseconds (extra digits are
// truncated). Parsing it as a float64 could round 1759900000.123 down to
// …122, and a token issued in the same ms as a revocation would read as revoked.
func parseMs(s string) (int64, error) {
	m := numericDate.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("NumericDate %q", s)
	}
	sec, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, err
	}
	frac := (m[2] + "000")[:3]
	msPart, _ := strconv.ParseInt(frac, 10, 64)
	return sec*1000 + msPart, nil
}

// formatMs writes ms as a NumericDate with exactly three fraction digits.
func formatMs(ms int64) json.Number {
	return json.Number(fmt.Sprintf("%d.%03d", ms/1000, ms%1000))
}

// PrivateKey is a signing key with its kid (devtoken and tests only; the
// account service signs production tokens).
type PrivateKey struct {
	Kid string
	Key ed25519.PrivateKey
}

// Sign issues a token for c. IatMs and ExpMs must be non-negative.
func Sign(k PrivateKey, c Claims) (string, error) {
	if c.IatMs < 0 || c.ExpMs < 0 {
		return "", errors.New("auth: iat and exp must be non-negative")
	}
	h, err := json.Marshal(header{Alg: "EdDSA", Kid: k.Kid, Typ: "JWT"})
	if err != nil {
		return "", err
	}
	p, err := json.Marshal(payload{Sub: c.UID, AppID: c.AppID, Platform: c.Platform,
		Iat: formatMs(c.IatMs), Exp: formatMs(c.ExpMs)})
	if err != nil {
		return "", err
	}
	signing := b64.EncodeToString(h) + "." + b64.EncodeToString(p)
	return signing + "." + b64.EncodeToString(ed25519.Sign(k.Key, []byte(signing))), nil
}
```


`internal/auth/jwks.go`:

```go
package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
)

// jwk is one JSON Web Key; only OKP / Ed25519 keys are used.
type jwk struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	X   string `json:"x"`
	D   string `json:"d,omitempty"` // private part; devtoken key files only
}

type jwkSet struct {
	Keys []jwk `json:"keys"`
}

// ParseJWKS reads a JWKS document. Keys that are not OKP/Ed25519 are skipped
// (the issuer may publish other kinds); a document with no usable key is an
// error, so a broken fetch never replaces a good set.
func ParseJWKS(data []byte) (KeySet, error) {
	var s jwkSet
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("auth: jwks: %w", err)
	}
	ks := KeySet{}
	for _, k := range s.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" || k.Kid == "" {
			continue
		}
		x, err := b64.DecodeString(k.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("auth: jwks: key %q: bad x", k.Kid)
		}
		ks[k.Kid] = ed25519.PublicKey(x)
	}
	if len(ks) == 0 {
		return nil, errors.New("auth: jwks: no Ed25519 key")
	}
	return ks, nil
}

// MarshalJWKS writes the public JWKS for keys.
func MarshalJWKS(keys ...PrivateKey) ([]byte, error) {
	s := jwkSet{Keys: make([]jwk, 0, len(keys))}
	for _, k := range keys {
		pub := k.Key.Public().(ed25519.PublicKey)
		s.Keys = append(s.Keys, jwk{Kty: "OKP", Crv: "Ed25519", Kid: k.Kid, X: b64.EncodeToString(pub)})
	}
	return json.MarshalIndent(s, "", "  ")
}

// GenerateKey returns a new signing key.
func GenerateKey(kid string) (PrivateKey, error) {
	if kid == "" {
		return PrivateKey{}, errors.New("auth: empty kid")
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return PrivateKey{}, err
	}
	return PrivateKey{Kid: kid, Key: priv}, nil
}

// MarshalPrivateJWK writes k as a private OKP JWK (devtoken key file).
func MarshalPrivateJWK(k PrivateKey) ([]byte, error) {
	pub := k.Key.Public().(ed25519.PublicKey)
	return json.MarshalIndent(jwk{Kty: "OKP", Crv: "Ed25519", Kid: k.Kid,
		X: b64.EncodeToString(pub), D: b64.EncodeToString(k.Key.Seed())}, "", "  ")
}

// ParsePrivateJWK reads a key file written by MarshalPrivateJWK.
func ParsePrivateJWK(data []byte) (PrivateKey, error) {
	var k jwk
	if err := json.Unmarshal(data, &k); err != nil {
		return PrivateKey{}, fmt.Errorf("auth: private jwk: %w", err)
	}
	seed, err := b64.DecodeString(k.D)
	if k.Kty != "OKP" || k.Crv != "Ed25519" || k.Kid == "" || err != nil || len(seed) != ed25519.SeedSize {
		return PrivateKey{}, errors.New("auth: private jwk: not an Ed25519 key with kid and d")
	}
	return PrivateKey{Kid: k.Kid, Key: ed25519.NewKeyFromSeed(seed)}, nil
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/auth/ && go test -race -count=1 ./internal/auth/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/auth
git commit -m "feat(auth): Ed25519 JWS verification with exact millisecond iat"
```

---

### Task 3: Key set manager (JWKS refresh, fallback, unknown kid)

**Files:**
- Create: `internal/auth/manager.go`
- Test: `internal/auth/manager_test.go`

**Interfaces:**
- Consumes: `Verify`, `ParseJWKS`, `ErrKeyUnknown` (Task 2).
- Produces (package `auth`):
  - `type ManagerConfig struct { URL, FallbackPath string; Refresh, RetryEmpty, UnknownKid, FetchTimeout, Leeway time.Duration }`; `func DefaultManagerConfig(url, fallback string) ManagerConfig` (5m, 5s, 30s, 5s, 30s)
  - `type Observer struct { RefreshFailed func(error); UnknownKidRefresh func(); Source func(fallback bool) }`
  - `func NewManager(cfg ManagerConfig, obs Observer) *Manager`
  - `(*Manager).Load(ctx) error` (account service, then fallback file), `Run(ctx)` (refresh loop), `Ready() bool`, `OnFallback() bool`, `Verify(token string) (Claims, error)`

The spec's three unknown-`kid` rules come from one gate: a refresh may start only if none started in the last 30s. That gives the rate limit (one per 30s per pod, across kids) and single-flight (requests inside the window share the refresh already running). It also gives the negative cache: a kid still unknown after that refresh is rejected from memory until the window ends. Verification never waits on a fetch.

- [ ] **Step 1: Write the failing tests**

`internal/auth/manager_test.go`:

```go
package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// source is a JWKS endpoint whose document and availability tests change.
type source struct {
	mu    sync.Mutex
	doc   []byte
	down  bool
	calls atomic.Int32
}

func (s *source) fetch(context.Context) ([]byte, error) {
	s.calls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down {
		return nil, errors.New("account service down")
	}
	return s.doc, nil
}

func (s *source) publish(t *testing.T, keys ...PrivateKey) {
	t.Helper()
	doc, err := MarshalJWKS(keys...)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.doc, s.down = doc, false
	s.mu.Unlock()
}

func noFallback() ([]byte, error) { return nil, errors.New("no fallback") }

func testManager(src *source, fallback func() ([]byte, error), clk *fakeClock) *Manager {
	cfg := DefaultManagerConfig("unused", "unused")
	return newManager(cfg, Observer{}, src.fetch, fallback, clk.Now)
}

func TestManagerFallsBackToConfigMap(t *testing.T) {
	k := testKey(t, "k1")
	doc, err := MarshalJWKS(k)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "jwks.json")
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultManagerConfig("http://127.0.0.1:1/jwks", path) // nothing listens on port 1
	cfg.FetchTimeout = 500 * time.Millisecond
	var fallbackSeen bool
	m := NewManager(cfg, Observer{Source: func(fb bool) { fallbackSeen = fb }})
	if m.Ready() {
		t.Fatal("ready before Load")
	}
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !m.Ready() || !m.OnFallback() || !fallbackSeen {
		t.Fatalf("ready %v fallback %v seen %v", m.Ready(), m.OnFallback(), fallbackSeen)
	}
	if _, err := m.Verify(sign(t, k, claims(time.Now().UnixMilli()))); err != nil {
		t.Fatalf("token from the fallback key: %v", err)
	}
}

func TestManagerNotReadyWithoutAnySource(t *testing.T) {
	src := &source{down: true}
	m := testManager(src, noFallback, &fakeClock{t: time.Unix(5, 0)})
	if err := m.Load(context.Background()); err == nil {
		t.Fatal("Load must fail with no source")
	}
	if m.Ready() {
		t.Fatal("ready with no key set")
	}
	if _, err := m.Verify("a.b.c"); !errors.Is(err, ErrKeyUnknown) {
		t.Fatalf("verify with no key set: %v", err)
	}
}

func TestManagerFetchesOverHTTP(t *testing.T) {
	k := testKey(t, "k1")
	doc, _ := MarshalJWKS(k)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write(doc) }))
	defer srv.Close()
	m := NewManager(DefaultManagerConfig(srv.URL, ""), Observer{})
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.OnFallback() {
		t.Fatal("set came from the URL, not the fallback")
	}
}

func TestUnknownKidFloodCausesOneFetchPer30s(t *testing.T) {
	k1, k2 := testKey(t, "k1"), testKey(t, "k2")
	src := &source{}
	src.publish(t, k1)
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	m := testManager(src, noFallback, clk)
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.calls.Store(0)

	// k2 starts signing before Logic has it (a broken runbook): every request
	// is rejected at once, and the flood shares one background fetch.
	src.publish(t, k1, k2)
	tok := sign(t, k2, claims(clk.Now().UnixMilli()))
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := m.Verify(tok); !errors.Is(err, ErrKeyUnknown) && err != nil {
				t.Errorf("verify: %v", err)
			}
		}()
	}
	wg.Wait()
	m.bg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("fetches = %d, want 1", n)
	}
	// The refresh landed: the client's retry after 1–3s succeeds.
	if _, err := m.Verify(tok); err != nil {
		t.Fatalf("after refresh: %v", err)
	}

	// A kid still unknown is rejected from memory until the window ends.
	tok9 := sign(t, testKey(t, "k9"), claims(clk.Now().UnixMilli()))
	clk.Add(29 * time.Second)
	m.Verify(tok9)
	m.bg.Wait()
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("fetches inside the window = %d, want 1", n)
	}
	clk.Add(time.Second)
	m.Verify(tok9)
	m.bg.Wait()
	if n := src.calls.Load(); n != 2 {
		t.Fatalf("fetches after 30s = %d, want 2", n)
	}
}

func TestFailedRefreshKeepsLastGoodSet(t *testing.T) {
	k := testKey(t, "k1")
	src := &source{}
	src.publish(t, k)
	var failures atomic.Int32
	m := newManager(DefaultManagerConfig("u", ""), Observer{RefreshFailed: func(error) { failures.Add(1) }},
		src.fetch, noFallback, (&fakeClock{t: time.Unix(1_000_000, 0)}).Now)
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	src.mu.Lock()
	src.down = true
	src.mu.Unlock()
	if err := m.refreshURL(context.Background()); err == nil {
		t.Fatal("refresh against a down source succeeded")
	}
	if !m.Ready() || failures.Load() != 1 {
		t.Fatalf("ready %v failures %d", m.Ready(), failures.Load())
	}
	if _, err := m.Verify(sign(t, k, claims(1_000_000_000))); err != nil {
		t.Fatalf("last good set: %v", err)
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/auth/`
Expected: FAIL to build: `undefined: DefaultManagerConfig`, `undefined: newManager`, ….

- [ ] **Step 3: Implement**

`internal/auth/manager.go`:

```go
package auth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ManagerConfig configures the key set source (spec: JWKS, R2-O7, R4H-O10).
type ManagerConfig struct {
	URL          string        // account service JWKS endpoint
	FallbackPath string        // mounted jwks-fallback ConfigMap file; "" for none
	Refresh      time.Duration // periodic refresh (5m)
	RetryEmpty   time.Duration // retry interval while no key set is held (5s)
	UnknownKid   time.Duration // at most one unknown-kid refresh per this long (30s)
	FetchTimeout time.Duration // per fetch (5s)
	Leeway       time.Duration // allowed exp overrun for issuer↔Logic clock skew (30s)
}

// DefaultManagerConfig returns the spec defaults for url and fallback.
func DefaultManagerConfig(url, fallback string) ManagerConfig {
	return ManagerConfig{URL: url, FallbackPath: fallback, Refresh: 5 * time.Minute,
		RetryEmpty: 5 * time.Second, UnknownKid: 30 * time.Second,
		FetchTimeout: 5 * time.Second, Leeway: 30 * time.Second}
}

// Observer receives the Manager's events, for metrics and alerts. Nil fields are skipped.
type Observer struct {
	RefreshFailed     func(err error)     // a fetch from the account service failed
	UnknownKidRefresh func()              // an unknown kid started a refresh
	Source            func(fallback bool) // the key set now in use came from the fallback file (true) or the URL
}

// Manager holds the current key set and keeps it fresh.
type Manager struct {
	cfg      ManagerConfig
	fetch    func(ctx context.Context) ([]byte, error)
	fallback func() ([]byte, error)
	now      func() time.Time
	obs      Observer

	keys       atomic.Pointer[KeySet]
	onFallback atomic.Bool

	mu          sync.Mutex
	nextUnknown time.Time       // unknown-kid refreshes are allowed from this time on
	ctx         context.Context // Run's context, for background refreshes
	bg          sync.WaitGroup  // background refreshes (tests wait on it)
}

// NewManager builds a Manager that fetches cfg.URL over HTTP and reads
// cfg.FallbackPath from disk.
func NewManager(cfg ManagerConfig, obs Observer) *Manager {
	client := &http.Client{Timeout: cfg.FetchTimeout}
	fetch := func(ctx context.Context) ([]byte, error) {
		if cfg.URL == "" {
			return nil, errors.New("auth: no JWKS URL configured")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.URL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("auth: JWKS fetch: %s", resp.Status)
		}
		return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	}
	fallback := func() ([]byte, error) {
		if cfg.FallbackPath == "" {
			return nil, errors.New("auth: no fallback configured")
		}
		return os.ReadFile(cfg.FallbackPath)
	}
	return newManager(cfg, obs, fetch, fallback, time.Now)
}

func newManager(cfg ManagerConfig, obs Observer, fetch func(context.Context) ([]byte, error),
	fallback func() ([]byte, error), now func() time.Time) *Manager {
	return &Manager{cfg: cfg, fetch: fetch, fallback: fallback, now: now, obs: obs, ctx: context.Background()}
}

// Ready reports whether a key set is held from either source (spec: Logic is
// Ready only once it holds a key set).
func (m *Manager) Ready() bool { return m.keys.Load() != nil }

// OnFallback reports whether the key set in use came from the fallback file.
func (m *Manager) OnFallback() bool { return m.onFallback.Load() }

// Load does the startup load: the account service first, then the fallback
// file. It returns an error only if neither source gave a key set.
func (m *Manager) Load(ctx context.Context) error {
	if err := m.refreshURL(ctx); err == nil {
		return nil
	}
	data, err := m.fallback()
	if err != nil {
		return fmt.Errorf("auth: no key set: fallback: %w", err)
	}
	ks, err := ParseJWKS(data)
	if err != nil {
		return fmt.Errorf("auth: no key set: fallback: %w", err)
	}
	m.set(ks, true)
	return nil
}

// Run refreshes until ctx ends: every Refresh while a key set is held, every
// RetryEmpty (trying the fallback too) while none is. Call Load first.
func (m *Manager) Run(ctx context.Context) {
	m.mu.Lock()
	m.ctx = ctx
	m.mu.Unlock()
	for {
		wait := m.cfg.Refresh
		if !m.Ready() {
			wait = m.cfg.RetryEmpty
		}
		select {
		case <-ctx.Done():
			m.bg.Wait()
			return
		case <-time.After(wait):
		}
		if m.Ready() {
			_ = m.refreshURL(ctx) // failure: keep the last good set (counted by the observer)
		} else {
			_ = m.Load(ctx)
		}
	}
}

// refreshURL fetches the account service's set and installs it on success.
func (m *Manager) refreshURL(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.FetchTimeout)
	defer cancel()
	data, err := m.fetch(ctx)
	if err == nil {
		var ks KeySet
		if ks, err = ParseJWKS(data); err == nil {
			m.set(ks, false)
			return nil
		}
	}
	if m.obs.RefreshFailed != nil {
		m.obs.RefreshFailed(err)
	}
	return err
}

func (m *Manager) set(ks KeySet, fallback bool) {
	m.keys.Store(&ks)
	m.onFallback.Store(fallback)
	if m.obs.Source != nil {
		m.obs.Source(fallback)
	}
}

// Verify checks token against the current key set. An unknown kid is
// rejected at once with ErrKeyUnknown and may start a background refresh.
func (m *Manager) Verify(token string) (Claims, error) {
	ksp := m.keys.Load()
	if ksp == nil {
		return Claims{}, fmt.Errorf("%w: no key set loaded", ErrKeyUnknown)
	}
	c, err := Verify(token, *ksp, m.now().UnixMilli(), m.cfg.Leeway.Milliseconds())
	if errors.Is(err, ErrKeyUnknown) {
		m.unknownKid()
	}
	return c, err
}

// unknownKid starts a background refresh unless one started less than
// UnknownKid ago. This one gate gives the spec's three rules: the rate limit
// (one refresh per 30s per pod, across kids), single-flight (requests inside
// the window share the refresh already started) and the negative cache (a kid
// still unknown after that refresh is rejected from memory until the window
// ends, without a fetch).
func (m *Manager) unknownKid() {
	m.mu.Lock()
	now := m.now()
	if now.Before(m.nextUnknown) {
		m.mu.Unlock()
		return
	}
	m.nextUnknown = now.Add(m.cfg.UnknownKid)
	ctx := m.ctx
	m.bg.Add(1)
	m.mu.Unlock()
	if m.obs.UnknownKidRefresh != nil {
		m.obs.UnknownKidRefresh()
	}
	go func() {
		defer m.bg.Done()
		_ = m.refreshURL(ctx)
	}()
}
```


- [ ] **Step 4: Run the tests**

Run: `gofmt -l internal/auth; go vet ./internal/auth/ && go test -race -count=1 ./internal/auth/`
Expected: no gofmt output; `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/auth
git commit -m "feat(auth): JWKS manager with ConfigMap fallback and unknown-kid refresh gate"
```

---

### Task 4: `cmd/devtoken`

**Files:**
- Create: `cmd/devtoken/main.go`
- Test: `cmd/devtoken/main_test.go`

**Interfaces:**
- Consumes: `auth.GenerateKey`, `MarshalPrivateJWK`, `ParsePrivateJWK`, `MarshalJWKS`, `Sign`, `NewManager` (Tasks 2–3).
- Produces: the `devtoken` command. `keygen -kid K -out FILE`; `jwks -out FILE KEYFILE...` (atomic rename, so a running `serve` never reads half a file); `issue -key FILE -sub UID -appid A -platform P [-ttl D] [-iat-ms N]` prints one token with a millisecond `iat`; `serve -addr :8081 -jwks FILE` serves the file and re-reads it on every request (key-rotation tests swap it while Logic runs). Plan 5's compose uses `serve` as the account service.

- [ ] **Step 1: Write the failing test**

`cmd/devtoken/main_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
)

func TestKeygenJWKSIssueServe(t *testing.T) {
	dir := t.TempDir()
	k1, k2, set := filepath.Join(dir, "k1.json"), filepath.Join(dir, "k2.json"), filepath.Join(dir, "jwks.json")
	for _, args := range [][]string{
		{"keygen", "-kid", "k1", "-out", k1},
		{"keygen", "-kid", "k2", "-out", k2},
		{"jwks", "-out", set, k1, k2},
	} {
		if err := run(args, &bytes.Buffer{}); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}

	// iat is issued with ms precision (roadmap choice #3).
	var out bytes.Buffer
	fixed := func() time.Time { return time.UnixMilli(1759900000123) }
	if err := issue([]string{"-key", k2, "-sub", "u1", "-appid", "im", "-platform", "pad", "-ttl", "2s"}, &out, fixed); err != nil {
		t.Fatal(err)
	}
	tok := strings.TrimSpace(out.String())

	srv := httptest.NewServer(jwksHandler(set))
	defer srv.Close()
	m := auth.NewManager(auth.DefaultManagerConfig(srv.URL, ""), auth.Observer{})
	if err := m.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	ks, err := auth.ParseJWKS(mustRead(t, set))
	if err != nil {
		t.Fatal(err)
	}
	c, err := auth.Verify(tok, ks, 1759900000123, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := auth.Claims{UID: "u1", AppID: "im", Platform: "pad", IatMs: 1759900000123, ExpMs: 1759900002123}
	if c != want {
		t.Fatalf("claims %+v, want %+v", c, want)
	}
	if !m.Ready() {
		t.Fatal("manager did not load the served JWKS")
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
```


- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./cmd/devtoken/`
Expected: FAIL to build: `undefined: run`, `undefined: issue`, `undefined: jwksHandler`.

- [ ] **Step 3: Implement**

`cmd/devtoken/main.go`:

```go
// Command devtoken issues test access tokens and publishes a test JWKS. It
// stands in for the account service in tests and on the dev machine.
//
//	devtoken keygen -kid k1 -out k1.json
//	devtoken jwks -out jwks.json k1.json [k2.json ...]
//	devtoken issue -key k1.json -sub u1 -appid im -platform mobile [-ttl 1h] [-iat-ms N]
//	devtoken serve -addr :8081 -jwks jwks.json
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Tallinzp/goim/internal/auth"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "devtoken:", err)
		os.Exit(2)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: devtoken keygen|jwks|issue|serve [flags]")
	}
	switch args[0] {
	case "keygen":
		return keygen(args[1:])
	case "jwks":
		return jwks(args[1:])
	case "issue":
		return issue(args[1:], out, time.Now)
	case "serve":
		fs := flag.NewFlagSet("serve", flag.ContinueOnError)
		addr := fs.String("addr", ":8081", "listen address")
		path := fs.String("jwks", "jwks.json", "JWKS file, re-read on every request")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		log.Printf("devtoken: serving %s on %s", *path, *addr)
		return http.ListenAndServe(*addr, jwksHandler(*path))
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func keygen(args []string) error {
	fs := flag.NewFlagSet("keygen", flag.ContinueOnError)
	kid := fs.String("kid", "", "key id")
	out := fs.String("out", "", "private key file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return errors.New("keygen: -out is required")
	}
	k, err := auth.GenerateKey(*kid)
	if err != nil {
		return err
	}
	doc, err := auth.MarshalPrivateJWK(k)
	if err != nil {
		return err
	}
	return os.WriteFile(*out, doc, 0o600)
}

func jwks(args []string) error {
	fs := flag.NewFlagSet("jwks", flag.ContinueOnError)
	out := fs.String("out", "", "JWKS file to write")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" || fs.NArg() == 0 {
		return errors.New("jwks: -out and at least one key file are required")
	}
	var keys []auth.PrivateKey
	for _, p := range fs.Args() {
		k, err := readKey(p)
		if err != nil {
			return err
		}
		keys = append(keys, k)
	}
	doc, err := auth.MarshalJWKS(keys...)
	if err != nil {
		return err
	}
	// Write and rename, so a serve process re-reading the file never sees half of it.
	tmp := *out + ".tmp"
	if err := os.WriteFile(tmp, doc, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, *out)
}

func issue(args []string, out io.Writer, now func() time.Time) error {
	fs := flag.NewFlagSet("issue", flag.ContinueOnError)
	keyPath := fs.String("key", "", "private key file")
	sub := fs.String("sub", "", "uid")
	appid := fs.String("appid", "", "appid")
	platform := fs.String("platform", "mobile", "mobile | pad | web | pc")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	iatMs := fs.Int64("iat-ms", 0, "iat in ms (default: now)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	k, err := readKey(*keyPath)
	if err != nil {
		return err
	}
	iat := *iatMs
	if iat == 0 {
		iat = now().UnixMilli()
	}
	tok, err := auth.Sign(k, auth.Claims{UID: *sub, AppID: *appid, Platform: *platform,
		IatMs: iat, ExpMs: iat + ttl.Milliseconds()})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, tok)
	return err
}

func readKey(path string) (auth.PrivateKey, error) {
	doc, err := os.ReadFile(path)
	if err != nil {
		return auth.PrivateKey{}, err
	}
	return auth.ParsePrivateJWK(doc)
}

// jwksHandler serves the JWKS file, re-read on every request so a rotation
// test can publish a new key while Logic runs.
func jwksHandler(path string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		doc, err := os.ReadFile(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(doc)
	})
}
```


- [ ] **Step 4: Run the test**

Run: `go vet ./cmd/devtoken/ && go test -race -count=1 ./cmd/devtoken/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add cmd/devtoken
git commit -m "feat(devtoken): issue test tokens and publish a test JWKS"
```

---

### Task 5: Logic config, store interface, metrics and lease cache (choices #21–#23)

**Files:**
- Create: `internal/logic/config.go`, `internal/logic/store.go`, `internal/logic/metrics.go`, `internal/logic/leases.go`, `internal/logic/log.go`
- Test: `internal/logic/helpers_test.go`, `internal/logic/config_test.go`, `internal/logic/leases_test.go`

**Interfaces:**
- Consumes: `store.Config`, `store.DefaultConfig`, every `*store.Store` method Logic uses (Plan 1), `auth.Observer` (Task 3).
- Produces (package `logic`):
  - `type Config struct { Store store.Config; RenewInterval, RedisFailover, LeaseCache, SweepInterval time.Duration; SweepRate, SweepBatch, DeleteRate, DeleteBatch int; RevokeMaxAhead time.Duration; Kick KickConfig }`
  - `type KickConfig struct { Workers, Queue int; Deadline time.Duration; Retries int; RetrySpacing, IdleTimeout time.Duration; AddrFormat string }`
  - `func DefaultConfig() Config`, `(Config).StaleAfter()` (2 × lease TTL), `(Config).RecentRenewal()` (1.5 × renew interval), `(Config).Validate() error`
  - `type Store interface { … }`: the 20 store methods, with `var _ Store = (*store.Store)(nil)`
  - `type Metrics struct { … }`, `func NewMetrics(prometheus.Registerer) *Metrics`, `(*Metrics).AuthObserver() auth.Observer`, `const SkewAlertMs = 500`
  - `type LeaseCache`, `func NewLeaseCache(st Store, ttl time.Duration, now func() time.Time) *LeaseCache`, `(*LeaseCache).Live(ctx, []store.EpochRef) (map[store.EpochRef]bool, error)`, `LiveOne(ctx, ref) (bool, error)`, `Peek(ref) (live, ok bool)` (cache only), `Set(ref, live bool)`, `RenewedMs(ctx, ref) (int64, bool, error)`
  - unexported `logger(ctx) *slog.Logger`, `logKey`
  - test helpers (package `logic`, `_test.go`): `ctx`, `field`, `fakeClock`/`newClock`, `testConfig()`, `newStore(t, cfg)`, `newMetrics()`, `counter(c)`, `redisNow(t, st)`, `countingStore`, `histogram`/`sampleCount`/`sampleSum`

`Validate` is the "refuse to start" rule: lease TTL must exceed the measured failover time plus one renewal interval (R2-O5). `RedisFailover` defaults to 15s (choice #21) until Tier 2 measures it. The lease cache must be shorter than the renewal interval, and each limiter batch must fit its rate (a `rate.Limiter` with burst = batch refuses `WaitN` above the burst).

- [ ] **Step 1: Add the metrics dependency, write the test helpers and the failing tests**

```bash
go get github.com/prometheus/client_golang@v1.24.1 github.com/prometheus/client_model@v0.6.2
```

`internal/logic/helpers_test.go`:

```go
package logic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const field = "im:mobile"

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_000_000, 0)} }

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// testConfig is DefaultConfig with a Kick client that gives up quickly.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Kick.Deadline = 300 * time.Millisecond
	cfg.Kick.RetrySpacing = 50 * time.Millisecond
	cfg.Kick.IdleTimeout = 200 * time.Millisecond
	return cfg
}

// newStore returns a real store on a fresh miniredis (or the Tier 2 Redis).
func newStore(t *testing.T, cfg Config) (*store.Store, *redis.Client) {
	t.Helper()
	rdb := testredis.New(t)
	return store.New(rdb, cfg.Store), rdb
}

func newMetrics() *Metrics { return NewMetrics(prometheus.NewRegistry()) }

func counter(c prometheus.Collector) float64 { return testutil.ToFloat64(c) }

func redisNow(t *testing.T, st Store) int64 {
	t.Helper()
	n, err := st.RedisNowMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// countingStore counts LeasesExist calls.
type countingStore struct {
	*store.Store
	mu     sync.Mutex
	leases int
}

func (s *countingStore) LeasesExist(ctx context.Context, refs []store.EpochRef) ([]bool, error) {
	s.mu.Lock()
	s.leases++
	s.mu.Unlock()
	return s.Store.LeasesExist(ctx, refs)
}

func histogram(t *testing.T, h prometheus.Histogram) *dto.Histogram {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram()
}

func sampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	return histogram(t, h).GetSampleCount()
}
func sampleSum(t *testing.T, h prometheus.Histogram) float64 { return histogram(t, h).GetSampleSum() }
```


`internal/logic/config_test.go`:

```go
package logic

import (
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

func TestDefaultConfigIsValid(t *testing.T) {
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
	c := DefaultConfig()
	if c.StaleAfter() != 60*time.Second || c.RecentRenewal() != 15*time.Second {
		t.Fatalf("stale %v recent %v", c.StaleAfter(), c.RecentRenewal())
	}
}

// The test timing profile scales time ~10× but keeps login_deadline (spec).
func TestScaledProfileIsValid(t *testing.T) {
	c := DefaultConfig()
	c.Store.LeaseTTL = 3 * time.Second
	c.RenewInterval = time.Second
	c.RedisFailover = 1500 * time.Millisecond
	c.LeaseCache = 100 * time.Millisecond
	c.SweepInterval = 500 * time.Millisecond
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRefusesBrokenRatios(t *testing.T) {
	cases := map[string]func(*Config){
		"lease TTL":    func(c *Config) { c.Store.LeaseTTL = c.RedisFailover + c.RenewInterval },
		"lease cache":  func(c *Config) { c.LeaseCache = c.RenewInterval },
		"sweep batch":  func(c *Config) { c.SweepBatch = c.SweepRate + 1 },
		"delete rate":  func(c *Config) { c.DeleteRate = 0 },
		"kick workers": func(c *Config) { c.Kick.Workers = 0 },
		"kick addr":    func(c *Config) { c.Kick.AddrFormat = "" },
		"deadline": func(c *Config) {
			c.Store = store.Config{SessTTL: time.Hour, LeaseTTL: time.Hour, WaitTimeout: time.Second}
		},
		"kick deadline": func(c *Config) { c.Kick.Deadline = 0 },
	}
	for name, mutate := range cases {
		c := DefaultConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	c := DefaultConfig()
	c.Store.LeaseTTL = 20 * time.Second
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "R2-O5") {
		t.Fatalf("lease TTL 20s with failover 15s + renew 10s: %v", err)
	}
}
```


`internal/logic/leases_test.go`:

```go
package logic

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestLeaseCacheReusesObservationsForTTL(t *testing.T) {
	cfg := testConfig()
	real, _ := newStore(t, cfg)
	st := &countingStore{Store: real}
	clk := newClock()
	c := NewLeaseCache(st, time.Second, clk.Now)
	alive := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1 << 62}
	dead := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
	if _, err := real.RenewLease(ctx, alive); err != nil {
		t.Fatal(err)
	}

	got, err := c.Live(ctx, []store.EpochRef{alive, dead, alive})
	if err != nil {
		t.Fatal(err)
	}
	if !got[alive] || got[dead] || st.leases != 1 {
		t.Fatalf("got %v after %d calls", got, st.leases)
	}

	// Within the TTL the cache answers, even though the lease is gone.
	if err := real.EndEpoch(ctx, alive); err != nil {
		t.Fatal(err)
	}
	clk.Add(999 * time.Millisecond)
	if live, _ := c.LiveOne(ctx, alive); !live || st.leases != 1 {
		t.Fatalf("cached: live %v calls %d", live, st.leases)
	}
	clk.Add(time.Millisecond)
	if live, _ := c.LiveOne(ctx, alive); live || st.leases != 2 {
		t.Fatalf("expired: live %v calls %d", live, st.leases)
	}

	// Set overrides at once (a renewal Logic just made).
	c.Set(dead, true)
	if live, _ := c.LiveOne(ctx, dead); !live || st.leases != 2 {
		t.Fatalf("after Set: live %v calls %d", live, st.leases)
	}
}

func TestLeaseCacheRenewedMs(t *testing.T) {
	cfg := testConfig()
	st, _ := newStore(t, cfg)
	clk := newClock()
	c := NewLeaseCache(st, time.Second, clk.Now)
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 9}
	if _, ok, err := c.RenewedMs(ctx, ref); ok || err != nil {
		t.Fatalf("unknown epoch: ok %v err %v", ok, err)
	}
	res, err := st.RenewLease(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	clk.Add(time.Second)
	ms, ok, err := c.RenewedMs(ctx, ref)
	if err != nil || !ok || ms != res.RedisNowMs {
		t.Fatalf("score %d ok %v err %v, want %d", ms, ok, err, res.RedisNowMs)
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL to build: `undefined: Config`, `undefined: DefaultConfig`, `undefined: NewLeaseCache`, ….

- [ ] **Step 3: Implement**

`internal/logic/config.go`:

```go
// Package logic is the Logic service (spec: Logic): the RPC handlers the
// gateways and the account service call, the hardened Kick client, and the
// sweeper. It is the only code that uses internal/store (layer rule).
package logic

import (
	"errors"
	"fmt"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

// Config holds Logic's tunables. Every interval is configurable so the test
// timing profile can scale them (spec: Test execution model).
type Config struct {
	Store store.Config // LoginDeadline, SessTTL, LeaseTTL, WaitTimeout

	// RenewInterval is the gateways' lease renewal interval (10s). The
	// mass-stale guard's "recent renewal" window is 1.5 × this.
	RenewInterval time.Duration
	// RedisFailover is the measured shard failover time; LeaseTTL must exceed
	// it plus one RenewInterval (R2-O5). Default 15s until Tier 2 measures it.
	RedisFailover time.Duration
	LeaseCache    time.Duration // how long a lease observation is reused (~1s)

	SweepInterval time.Duration // sweeper cycle (5s)
	SweepRate     int           // sweeper uids/s (10k)
	SweepBatch    int           // uids per SSCAN page and abort check (500)
	DeleteRate    int           // ReconcileEnd delete phase uids/s (10k, R5S-O8)
	DeleteBatch   int           // uids per delete-phase page (500)

	RevokeMaxAhead time.Duration // before_ms may be at most this far past Redis TIME (5m, PI-4)

	Kick KickConfig
}

// KickConfig sizes the hardened Kick client (R2-O6, R5S-O2, roadmap choice #12).
type KickConfig struct {
	Workers      int           // per target gw|epoch (64)
	Queue        int           // per target (1024)
	Deadline     time.Duration // per attempt, connect included (1s)
	Retries      int           // after the first attempt, only while the lease is live (3)
	RetrySpacing time.Duration // between attempts (10s: 3 retries over ~30s)
	IdleTimeout  time.Duration // a target with no work for this long is dropped (1m)
	// AddrFormat turns a gateway id (pod name) into a dial target via the
	// StatefulSet's pod DNS, e.g. "%s.gateway.im.svc.cluster.local:9100".
	AddrFormat string
}

// DefaultConfig returns the spec defaults.
func DefaultConfig() Config {
	return Config{
		Store:          store.DefaultConfig(),
		RenewInterval:  10 * time.Second,
		RedisFailover:  15 * time.Second,
		LeaseCache:     time.Second,
		SweepInterval:  5 * time.Second,
		SweepRate:      10_000,
		SweepBatch:     500,
		DeleteRate:     10_000,
		DeleteBatch:    500,
		RevokeMaxAhead: 5 * time.Minute,
		Kick: KickConfig{
			Workers: 64, Queue: 1024, Deadline: time.Second, Retries: 3,
			RetrySpacing: 10 * time.Second, IdleTimeout: time.Minute,
			AddrFormat: "%s.gateway.im.svc.cluster.local:9100",
		},
	}
}

// StaleAfter is how old a gwepochs score must be before the sweeper considers
// the epoch (2 × lease TTL, R2-O5).
func (c Config) StaleAfter() time.Duration { return 2 * c.Store.LeaseTTL }

// RecentRenewal is the guard's window for "another epoch renewed" (1.5 × the
// renewal interval, R3-O2).
func (c Config) RecentRenewal() time.Duration { return c.RenewInterval * 3 / 2 }

// Validate refuses a configuration that breaks a ratio the design relies on
// (spec: the test timing profile must keep them; Logic refuses to start otherwise).
func (c Config) Validate() error {
	var errs []error
	pos := func(name string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %v", name, d))
		}
	}
	pos("login deadline", c.Store.LoginDeadline)
	pos("sess TTL", c.Store.SessTTL)
	pos("lease TTL", c.Store.LeaseTTL)
	pos("WAIT timeout", c.Store.WaitTimeout)
	pos("renew interval", c.RenewInterval)
	pos("lease cache", c.LeaseCache)
	pos("sweep interval", c.SweepInterval)
	pos("revoke max ahead", c.RevokeMaxAhead)
	pos("kick deadline", c.Kick.Deadline)
	pos("kick retry spacing", c.Kick.RetrySpacing)
	pos("kick idle timeout", c.Kick.IdleTimeout)
	if c.RedisFailover < 0 {
		errs = append(errs, errors.New("redis failover time must not be negative"))
	}
	if c.Store.LeaseTTL <= c.RedisFailover+c.RenewInterval {
		errs = append(errs, fmt.Errorf("lease TTL %v must exceed failover %v + renew interval %v (R2-O5)",
			c.Store.LeaseTTL, c.RedisFailover, c.RenewInterval))
	}
	if c.LeaseCache >= c.RenewInterval {
		errs = append(errs, fmt.Errorf("lease cache %v must be shorter than the renew interval %v", c.LeaseCache, c.RenewInterval))
	}
	for _, r := range []struct {
		name        string
		rate, batch int
	}{{"sweep", c.SweepRate, c.SweepBatch}, {"delete phase", c.DeleteRate, c.DeleteBatch}} {
		if r.rate <= 0 || r.batch <= 0 || r.batch > r.rate {
			errs = append(errs, fmt.Errorf("%s rate %d and batch %d: need 0 < batch ≤ rate", r.name, r.rate, r.batch))
		}
	}
	if c.Kick.Workers <= 0 || c.Kick.Queue <= 0 || c.Kick.Retries < 0 {
		errs = append(errs, fmt.Errorf("kick workers %d, queue %d, retries %d", c.Kick.Workers, c.Kick.Queue, c.Kick.Retries))
	}
	if c.Kick.AddrFormat == "" {
		errs = append(errs, errors.New("kick address format is empty"))
	}
	return errors.Join(errs...)
}
```


`internal/logic/store.go`:

```go
package logic

import (
	"context"

	"github.com/Tallinzp/goim/internal/store"
)

// Store is the part of *store.Store that Logic uses. Handlers take the
// interface so a test can wrap the real store and delay or fail one call.
type Store interface {
	LoginEach(ctx context.Context, reqs []store.LoginReq, done func(i int, r store.LoginResult))
	AddConns(ctx context.Context, ref store.EpochRef, uids []string) error
	LogoutEach(ctx context.Context, reqs []store.LogoutReq, done func(i int, r store.LogoutResult))

	ReadSlots(ctx context.Context, refs []store.SlotRef) []store.SlotState
	CompareAndSet(ctx context.Context, reqs []store.CASReq) []store.CASResult
	RefreshSlots(ctx context.Context, ref store.EpochRef, uids []string) error
	ScanConns(ctx context.Context, ref store.EpochRef, cursor uint64, count int64) ([]string, uint64, error)
	DeleteUntracked(ctx context.Context, ref store.EpochRef, asOfMs int64, uids []string, tracked map[uint64]struct{}) (int, error)

	Revoke(ctx context.Context, uid, scope string, beforeMs int64) ([]store.Revoked, error)

	RenewLease(ctx context.Context, ref store.EpochRef) (store.LeaseResult, error)
	EndEpoch(ctx context.Context, ref store.EpochRef) error
	LeasesExist(ctx context.Context, refs []store.EpochRef) ([]bool, error)
	ListEpochs(ctx context.Context) ([]store.EpochScore, error)
	EpochScoreOf(ctx context.Context, ref store.EpochRef) (int64, bool, error)

	ClaimSweep(ctx context.Context, ref store.EpochRef) (bool, error)
	ReleaseSweep(ctx context.Context, ref store.EpochRef) error
	SweepUsers(ctx context.Context, ref store.EpochRef, uids []string) (int, error)
	FinishSweep(ctx context.Context, ref store.EpochRef) error

	RedisNowMs(ctx context.Context) (int64, error)
}

var _ Store = (*store.Store)(nil)
```


`internal/logic/metrics.go`:

```go
package logic

import (
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/prometheus/client_golang/prometheus"
)

// SkewAlertMs is the clock_skew_ms alert threshold: half the 1s skew budget (R4M-5).
const SkewAlertMs = 500

// Metrics are Logic's Prometheus metrics (spec: Observability, Logic side).
type Metrics struct {
	Logins           *prometheus.CounterVec // result
	ConnSeqRegressed prometheus.Counter
	Undecodable      *prometheus.CounterVec // op: stored entries that did not decode (PI-3)
	DeadEpochRelogin prometheus.Histogram   // login_ms − dead epoch's last renewal (R1-O7)
	StoreSeconds     *prometheus.HistogramVec
	RPCSeconds       *prometheus.HistogramVec // method, code
	Logouts          *prometheus.CounterVec   // result
	ConnsAddFailures prometheus.Counter       // Login follow-up SADD gwconns failed

	LeaseRenewals      *prometheus.CounterVec // result: live | missing | error
	LeaseIndexFailures prometheus.Counter     // ZADD gwepochs failed after a renewal
	ClockSkewMs        *prometheus.GaugeVec   // gw
	ClockSkewAlert     *prometheus.GaugeVec   // gw: 1 while |skew| > SkewAlertMs

	KickAttempts       *prometheus.CounterVec // attempt: first | retry; result: ok | error
	KickGiveUps        *prometheus.CounterVec // target_lease
	KickDropped        *prometheus.CounterVec // target_lease (R5S-O2)
	KickQueueDepth     *prometheus.GaugeVec   // target gw:epoch
	KickAttemptSeconds prometheus.Histogram

	ReconcileEntries    *prometheus.CounterVec // result, reason
	ReconcileEndDeleted prometheus.Counter
	ReconcileEndTracked prometheus.Histogram
	ReconcileEndSeconds *prometheus.HistogramVec // pace (R5S-O8)
	ReconcileEndScanned *prometheus.HistogramVec // pace

	Revokes         *prometheus.CounterVec // scope, result
	RevocationKicks prometheus.Counter

	Sweeps               *prometheus.CounterVec // result
	SweepUIDs            prometheus.Counter
	SweepDeleted         prometheus.Counter
	GuardHolding         prometheus.Gauge // 1 while the mass-stale guard holds back a sweep (alert)
	GuardHolds           prometheus.Counter
	GuardRenewingSeconds prometheus.Gauge // time since renewing_since; 0 while not renewing

	JWKSRefreshFailures     prometheus.Counter
	JWKSUnknownKidRefreshes prometheus.Counter
	JWKSFallback            prometheus.Gauge // 1 while on jwks-fallback (alert)
}

// NewMetrics registers every metric on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "goim_logic"
	cv := func(name, help string, labels ...string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help}, labels)
		reg.MustRegister(v)
		return v
	}
	c := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{Namespace: ns, Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	gv := func(name, help string, labels ...string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help}, labels)
		reg.MustRegister(v)
		return v
	}
	g := func(name, help string) prometheus.Gauge {
		v := prometheus.NewGauge(prometheus.GaugeOpts{Namespace: ns, Name: name, Help: help})
		reg.MustRegister(v)
		return v
	}
	hv := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets}, labels)
		reg.MustRegister(v)
		return v
	}
	h := func(name, help string, buckets []float64) prometheus.Histogram {
		v := prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: ns, Name: name, Help: help, Buckets: buckets})
		reg.MustRegister(v)
		return v
	}
	lat := prometheus.ExponentialBuckets(0.0005, 2, 16) // 0.5ms … ~16s
	return &Metrics{
		Logins:           cv("logins_total", "Logins by result.", "result"),
		ConnSeqRegressed: c("conn_seq_regressed_total", "Logins accepted as a device counter regression."),
		Undecodable:      cv("undecodable_entries_total", "Stored sess values that did not decode, by operation.", "op"),
		DeadEpochRelogin: h("dead_epoch_relogin_seconds", "Relogin login_ms minus the dead epoch's last lease renewal.",
			prometheus.LinearBuckets(0, 5, 24)),
		StoreSeconds:     hv("store_seconds", "Session store call latency.", lat, "op"),
		RPCSeconds:       hv("rpc_seconds", "gRPC handler latency.", lat, "method", "code"),
		Logouts:          cv("logouts_total", "Logout entries by result.", "result"),
		ConnsAddFailures: c("gwconns_add_failures_total", "Login follow-up SADD to gwconns failed."),

		LeaseRenewals:      cv("lease_renewals_total", "GatewayLease calls by result.", "result"),
		LeaseIndexFailures: c("lease_index_failures_total", "ZADD gwepochs failed after a lease renewal."),
		ClockSkewMs:        gv("clock_skew_ms", "Redis TIME minus the gateway's clock at its last lease call.", "gw"),
		ClockSkewAlert:     gv("clock_skew_alert", "1 while |clock_skew_ms| exceeds 500.", "gw"),

		KickAttempts:       cv("kick_attempts_total", "Kick attempts.", "attempt", "result"),
		KickGiveUps:        cv("kick_giveups_total", "Kicks given up after their attempts.", "target_lease"),
		KickDropped:        cv("kick_dropped_total", "Kick attempts dropped by a full per-target queue.", "target_lease"),
		KickQueueDepth:     gv("kick_queue_depth", "Queued Kick attempts per target.", "target"),
		KickAttemptSeconds: h("kick_attempt_seconds", "Kick attempt latency, connect included.", lat),

		ReconcileEntries:    cv("reconcile_entries_total", "Reconcile entries by result.", "result", "reason"),
		ReconcileEndDeleted: c("reconcile_end_deleted_total", "Fields deleted by ReconcileEnd delete phases."),
		ReconcileEndTracked: h("reconcile_end_tracked", "tracked_conn_ids per ReconcileEnd.",
			prometheus.ExponentialBuckets(1000, 2, 10)),
		ReconcileEndSeconds: hv("reconcile_end_seconds", "ReconcileEnd delete phase duration.",
			prometheus.ExponentialBuckets(0.01, 2, 14), "pace"),
		ReconcileEndScanned: hv("reconcile_end_scanned_uids", "uids scanned per delete phase.",
			prometheus.ExponentialBuckets(1000, 2, 10), "pace"),

		Revokes:         cv("revokes_total", "Revoke calls.", "scope", "result"),
		RevocationKicks: c("revocation_kicks_total", "REVOKED Kicks queued by Revoke."),

		Sweeps:               cv("sweeps_total", "Sweep attempts by result.", "result"),
		SweepUIDs:            c("sweep_uids_total", "uids visited by the sweeper."),
		SweepDeleted:         c("sweep_deleted_total", "Fields deleted by the sweeper."),
		GuardHolding:         g("sweep_guard_holding", "1 while the mass-stale guard holds back a stale epoch."),
		GuardHolds:           c("sweep_guard_holds_total", "Sweeps held back by the mass-stale guard."),
		GuardRenewingSeconds: g("sweep_guard_renewing_seconds", "Seconds since renewing_since; 0 while not renewing."),

		JWKSRefreshFailures:     c("jwks_refresh_failures_total", "Failed JWKS fetches from the account service."),
		JWKSUnknownKidRefreshes: c("jwks_unknown_kid_refreshes_total", "JWKS refreshes started by an unknown kid."),
		JWKSFallback:            g("jwks_fallback", "1 while the key set in use came from jwks-fallback."),
	}
}

// AuthObserver wires the key manager's events to the metrics.
func (m *Metrics) AuthObserver() auth.Observer {
	return auth.Observer{
		RefreshFailed:     func(error) { m.JWKSRefreshFailures.Inc() },
		UnknownKidRefresh: func() { m.JWKSUnknownKidRefreshes.Inc() },
		Source: func(fallback bool) {
			if fallback {
				m.JWKSFallback.Set(1)
			} else {
				m.JWKSFallback.Set(0)
			}
		},
	}
}
```


`internal/logic/leases.go`:

```go
package logic

import (
	"context"
	"sync"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

// LeaseCache reuses lease observations for a short time (spec: Logic caches
// lease state for ~1s; there are only a handful of epochs). A cached answer is
// an observation, never a guarantee (R4M-2).
type LeaseCache struct {
	st  Store
	ttl time.Duration
	now func() time.Time

	mu     sync.Mutex
	live   map[store.EpochRef]obs[bool]
	scores map[store.EpochRef]obs[score]
}

type obs[T any] struct {
	v  T
	at time.Time
}

type score struct {
	ms int64
	ok bool
}

// pruneAt bounds the maps: epochs accumulate with every gateway restart.
const pruneAt = 1024

// NewLeaseCache returns a cache whose entries live for ttl.
func NewLeaseCache(st Store, ttl time.Duration, now func() time.Time) *LeaseCache {
	return &LeaseCache{st: st, ttl: ttl, now: now,
		live: map[store.EpochRef]obs[bool]{}, scores: map[store.EpochRef]obs[score]{}}
}

// Live reports whether each ref's lease key exists, from the cache where fresh
// and with one LeasesExist call for the rest.
func (c *LeaseCache) Live(ctx context.Context, refs []store.EpochRef) (map[store.EpochRef]bool, error) {
	out := make(map[store.EpochRef]bool, len(refs))
	var miss []store.EpochRef
	c.mu.Lock()
	now := c.now()
	for _, r := range refs {
		if o, ok := c.live[r]; ok && now.Sub(o.at) < c.ttl {
			out[r] = o.v
		} else if _, dup := out[r]; !dup {
			miss = append(miss, r)
			out[r] = false
		}
	}
	c.mu.Unlock()
	if len(miss) == 0 {
		return out, nil
	}
	exists, err := c.st.LeasesExist(ctx, miss)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, r := range miss {
		out[r] = exists[i]
		c.live[r] = obs[bool]{exists[i], now}
	}
	prune(c.live, now, c.ttl)
	return out, nil
}

// LiveOne is Live for one ref.
func (c *LeaseCache) LiveOne(ctx context.Context, ref store.EpochRef) (bool, error) {
	m, err := c.Live(ctx, []store.EpochRef{ref})
	if err != nil {
		return false, err
	}
	return m[ref], nil
}

// Peek returns ref's cached observation without reading Redis; ok is false
// if there is none or it is older than the cache TTL.
func (c *LeaseCache) Peek(ref store.EpochRef) (live, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, found := c.live[ref]
	if !found || c.now().Sub(o.at) >= c.ttl {
		return false, false
	}
	return o.v, true
}

// Set records a lease state Logic has just caused (a renewal or EpochEnded).
func (c *LeaseCache) Set(ref store.EpochRef, live bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.live[ref] = obs[bool]{live, c.now()}
}

// RenewedMs returns ref's gwepochs score (last renewal, Redis ms); ok is false
// if the member is gone. Cached like Live, so a relogin storm toward one dead
// epoch reads the gwepochs key about once per ttl, not once per login.
func (c *LeaseCache) RenewedMs(ctx context.Context, ref store.EpochRef) (int64, bool, error) {
	c.mu.Lock()
	now := c.now()
	if o, ok := c.scores[ref]; ok && now.Sub(o.at) < c.ttl {
		c.mu.Unlock()
		return o.v.ms, o.v.ok, nil
	}
	c.mu.Unlock()
	ms, ok, err := c.st.EpochScoreOf(ctx, ref)
	if err != nil {
		return 0, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scores[ref] = obs[score]{score{ms, ok}, now}
	prune(c.scores, now, c.ttl)
	return ms, ok, nil
}

func prune[T any](m map[store.EpochRef]obs[T], now time.Time, ttl time.Duration) {
	if len(m) < pruneAt {
		return
	}
	for k, o := range m {
		if now.Sub(o.at) >= ttl {
			delete(m, k)
		}
	}
}
```


`internal/logic/log.go`:

```go
package logic

import (
	"context"
	"log/slog"
)

type logKey struct{}

// logger returns the request-scoped logger (request_id and method attached by
// the server's interceptors, Task 13), or the default logger.
func logger(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(logKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
```


- [ ] **Step 4: Run the tests**

Run: `go mod tidy && go vet ./internal/logic/ && go test -race -count=1 ./internal/logic/`
Expected: `ok`; `go.mod` requires `client_golang v1.24.1` and `client_model v0.6.2` directly.

- [ ] **Step 5: Commit**

```bash
git add internal/logic go.mod go.sum
git commit -m "feat(logic): config with ratio checks, store interface, metrics, lease cache"
```

---

### Task 6: `GatewayLease` and `EpochEnded` (choice #18)

**Files:**
- Create: `internal/logic/convert.go`, `internal/logic/lease.go`
- Test: `internal/logic/lease_test.go`

**Interfaces:**
- Consumes: `Store`, `LeaseCache`, `Metrics`, `SkewAlertMs`, `logger` (Task 5); `pb` (Task 1); `store.EpochRef`, `session.ValidGateway`, `session.SlotField` (Plan 1).
- Produces:
  - `type LeaseService` (implements `pb.LeaseServer`), `func NewLeaseService(st Store, leases *LeaseCache, m *Metrics, workers int) *LeaseService`
  - unexported helpers used by later tasks: `epochRef(*pb.GatewayRef) (store.EpochRef, error)`, `pbRef(store.EpochRef) *pb.GatewayRef`, `platforms map[pb.Platform]session.Platform`, `slotField(appid string, p pb.Platform) (string, error)`, `kickReason(sameDevice bool) pb.KickReason`, `invalid(...)`/`unavailable(...)` (gRPC status errors), `type pool`, `newPool(n)`, `(pool).acquire(ctx) (release func(), err error)`

`GatewayLease` computes `clock_skew_ms{gw} = RedisNowMs − gw_now_ms` (PI-5) and sets `clock_skew_alert{gw}` to 1 while |skew| > 500ms. A renewal error returns `UNAVAILABLE`; the gateway treats it as `missing` (Plan 1 note), because the renewal may have recreated an absent key. The service gets its own `pool`: a full Login pool never delays a renewal, and a full lease pool delays only lease calls.

- [ ] **Step 1: Write the failing tests**

`internal/logic/lease_test.go`:

```go
package logic

import (
	"context"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newLeaseService(t *testing.T, workers int) (*LeaseService, *store.Store, *Metrics) {
	t.Helper()
	cfg := testConfig()
	st, _ := newStore(t, cfg)
	m := newMetrics()
	return NewLeaseService(st, NewLeaseCache(st, time.Second, time.Now), m, workers), st, m
}

func TestGatewayLeaseReportsMissingThenLive(t *testing.T) {
	s, _, m := newLeaseService(t, 4)
	gw := &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1 << 63}
	for i, want := range []bool{true, false, false} {
		rep, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: gw, GwNowMs: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Missing != want {
			t.Fatalf("call %d: missing %v, want %v", i, rep.Missing, want)
		}
	}
	if counter(m.LeaseRenewals.WithLabelValues("missing")) != 1 || counter(m.LeaseRenewals.WithLabelValues("live")) != 2 {
		t.Fatal("renewal counters")
	}
}

func TestGatewayLeaseClockSkew(t *testing.T) {
	s, st, m := newLeaseService(t, 4)
	gwName := testredis.Gateway()
	gw := &pb.GatewayRef{Gateway: gwName, Epoch: 1}
	for _, c := range []struct {
		offsetMs  int64 // gateway clock minus Redis clock
		wantAlert float64
	}{{-2000, 1}, {+2000, 1}, {-100, 0}} {
		now := redisNow(t, st)
		if _, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: gw, GwNowMs: now + c.offsetMs}); err != nil {
			t.Fatal(err)
		}
		skew := counter(m.ClockSkewMs.WithLabelValues(gwName))
		// skew = redis − gw = −offset, plus the few ms the call took.
		if d := skew + float64(c.offsetMs); d < 0 || d > 200 {
			t.Fatalf("offset %d: skew %v", c.offsetMs, skew)
		}
		if a := counter(m.ClockSkewAlert.WithLabelValues(gwName)); a != c.wantAlert {
			t.Fatalf("offset %d: alert %v, want %v", c.offsetMs, a, c.wantAlert)
		}
	}
}

func TestEpochEndedEndsTheLease(t *testing.T) {
	s, st, _ := newLeaseService(t, 4)
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 5}
	if _, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: pbRef(ref)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EpochEnded(ctx, &pb.EpochEndedRequest{Gw: pbRef(ref)}); err != nil {
		t.Fatal(err)
	}
	live, err := st.LeasesExist(ctx, []store.EpochRef{ref})
	if err != nil || live[0] {
		t.Fatalf("lease still exists: %v %v", live, err)
	}
	if score, ok, _ := st.EpochScoreOf(ctx, ref); !ok || score != 0 {
		t.Fatalf("gwepochs score %d ok %v, want 0 (ended)", score, ok)
	}
	if l, _ := s.leases.LiveOne(ctx, ref); l {
		t.Fatal("cache still says live")
	}
}

func TestLeaseRejectsBadGateway(t *testing.T) {
	s, _, _ := newLeaseService(t, 4)
	_, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: &pb.GatewayRef{Gateway: "GW|1"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err %v", err)
	}
}

// The lease pool is separate: a full pool makes lease calls wait, and only
// lease calls (the Login pool is another pool).
func TestLeasePoolBoundsConcurrency(t *testing.T) {
	s, _, _ := newLeaseService(t, 1)
	release, err := s.pool.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = s.GatewayLease(c, &pb.LeaseRequest{Gw: &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1}})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err %v, want DeadlineExceeded while the pool is full", err)
	}
	release()
	if _, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1}}); err != nil {
		t.Fatal(err)
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL to build: `undefined: LeaseService`, `undefined: NewLeaseService`, `undefined: pbRef`.

- [ ] **Step 3: Implement**

`internal/logic/convert.go`:

```go
package logic

import (
	"context"
	"fmt"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// epochRef converts and validates a GatewayRef.
func epochRef(g *pb.GatewayRef) (store.EpochRef, error) {
	if g == nil {
		return store.EpochRef{}, fmt.Errorf("missing gateway ref")
	}
	ref := store.EpochRef{Gateway: g.GetGateway(), Epoch: g.GetEpoch()}
	if err := session.ValidGateway(ref.Gateway); err != nil {
		return store.EpochRef{}, err
	}
	return ref, nil
}

func pbRef(ref store.EpochRef) *pb.GatewayRef {
	return &pb.GatewayRef{Gateway: ref.Gateway, Epoch: ref.Epoch}
}

var platforms = map[pb.Platform]session.Platform{
	pb.Platform_PLATFORM_MOBILE: session.Mobile,
	pb.Platform_PLATFORM_PAD:    session.Pad,
	pb.Platform_PLATFORM_WEB:    session.Web,
	pb.Platform_PLATFORM_PC:     session.PC,
}

// slotField returns the sess field "appid:platform".
func slotField(appid string, p pb.Platform) (string, error) {
	sp, ok := platforms[p]
	if !ok {
		return "", fmt.Errorf("invalid platform %v", p)
	}
	return session.SlotField(appid, sp)
}

// kickReason is the reason a replaced entry is kicked with: REPLACED for the
// same device, KICKED_BY_OTHER_DEVICE otherwise.
func kickReason(sameDevice bool) pb.KickReason {
	if sameDevice {
		return pb.KickReason_KICK_REASON_REPLACED
	}
	return pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE
}

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

func unavailable(format string, a ...any) error {
	return status.Errorf(codes.Unavailable, format, a...)
}

// pool bounds how many handlers of one kind run at once (spec: GatewayLease
// has its own worker pool, so Login load can't starve it, R1-O2).
type pool chan struct{}

func newPool(n int) pool { return make(pool, n) }

// acquire waits for a slot until ctx ends. A nil pool never blocks.
func (p pool) acquire(ctx context.Context) (release func(), err error) {
	if p == nil {
		return func() {}, nil
	}
	select {
	case p <- struct{}{}:
		return func() { <-p }, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
```


`internal/logic/lease.go`:

```go
package logic

import (
	"context"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// LeaseService serves GatewayLease and EpochEnded on Logic's dedicated lease
// port, with its own worker pool (R1-O2).
type LeaseService struct {
	pb.UnimplementedLeaseServer
	st     Store
	leases *LeaseCache
	m      *Metrics
	pool   pool
}

// NewLeaseService returns the lease handlers; workers bounds concurrent calls.
func NewLeaseService(st Store, leases *LeaseCache, m *Metrics, workers int) *LeaseService {
	return &LeaseService{st: st, leases: leases, m: m, pool: newPool(workers)}
}

// GatewayLease renews the caller's lease and reports whether the key was
// missing before (R3-O3). It also records clock_skew_ms = Redis TIME on the
// lease shard − gw_now_ms (R4M-5, PI-5).
func (s *LeaseService) GatewayLease(ctx context.Context, req *pb.LeaseRequest) (*pb.LeaseReply, error) {
	ref, err := epochRef(req.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	res, err := s.st.RenewLease(ctx, ref)
	if err != nil {
		s.m.LeaseRenewals.WithLabelValues("error").Inc()
		return nil, unavailable("lease renewal: %v", err)
	}
	s.leases.Set(ref, true)
	if res.IndexErr != nil {
		s.m.LeaseIndexFailures.Inc()
		logger(ctx).Warn("gwepochs update failed after a lease renewal", "gw", ref.Gateway, "epoch", ref.Epoch, "err", res.IndexErr)
	}
	skew := res.RedisNowMs - req.GetGwNowMs()
	s.m.ClockSkewMs.WithLabelValues(ref.Gateway).Set(float64(skew))
	alert := 0.0
	if skew > SkewAlertMs || skew < -SkewAlertMs {
		alert = 1
	}
	s.m.ClockSkewAlert.WithLabelValues(ref.Gateway).Set(alert)
	if res.Missing {
		s.m.LeaseRenewals.WithLabelValues("missing").Inc()
	} else {
		s.m.LeaseRenewals.WithLabelValues("live").Inc()
	}
	return &pb.LeaseReply{Missing: res.Missing}, nil
}

// EpochEnded deletes the drained epoch's lease key, so its entries are
// invalid at once, and marks it ended for the sweeper (roadmap choice #2).
func (s *LeaseService) EpochEnded(ctx context.Context, req *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	ref, err := epochRef(req.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.st.EndEpoch(ctx, ref); err != nil {
		return nil, unavailable("epoch ended: %v", err)
	}
	s.leases.Set(ref, false)
	return &pb.EpochEndedReply{}, nil
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/logic/ && go test -race -count=1 ./internal/logic/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/logic
git commit -m "feat(logic): GatewayLease with clock skew, EpochEnded, own worker pool"
```

---

### Task 7: Hardened Kick client (R1-O8, R2-O6, R5S-O2, choices #12, #22)

**Files:**
- Create: `internal/logic/kick.go`
- Test: `internal/logic/kick_test.go`

**Interfaces:**
- Consumes: `KickConfig`, `LeaseCache`, `Metrics` (Task 5); `pbRef` (Task 6); `pb.GatewayClient` (Task 1).
- Produces:
  - `type DialFunc func(gateway string) (*grpc.ClientConn, error)`; `func PodDNSDial(addrFormat string) DialFunc`
  - `type Kicker`, `func NewKicker(cfg KickConfig, leases *LeaseCache, m *Metrics, dial DialFunc) *Kicker`, `(*Kicker).Kick(ref store.EpochRef, connID uint64, reason pb.KickReason)` (never blocks), `(*Kicker).Close()`
  - test helpers: `fakeGateway`, `startGateway(t, g) string`, `blackhole(t) string`, `staticDial(map[string]string) DialFunc`, `waitFor(t, what, cond)`, `slowLeases`

Each target `gw|epoch` gets a queue of `Queue` jobs. Workers start on demand up to `Workers`, and each retires after `IdleTimeout` idle. The last worker out drops the target, and closes its gateway's connection if no other epoch of that pod uses it. Every attempt has its own `Deadline` covering connect and RPC; the call is fail-fast (the gRPC default, no `WaitForReady`). A failure retries only on `UNAVAILABLE` or `DEADLINE_EXCEEDED`, only while the target's lease reads live, at most `Retries` times, `RetrySpacing` apart, through the same queue. A full queue drops the attempt and counts `kick_dropped{target_lease}`, labelled from the lease cache only (`unknown` if nothing is cached), so `Kick` never waits on Redis. At the defaults, a dead target costs 64 attempts per second at the ~1s deadline; everything beyond that is dropped. A live target (~1ms per Kick) handles ~64k/s.

The spec's Kick test: "against a target that never answers (blackholed address), … each attempt ends within its deadline, and a full per-target queue drops and counts instead of growing." The blackhole is a local listener that accepts TCP and never speaks HTTP/2, like a dead pod's IP behind a stale DNS record. "Login results return without waiting for the Kick" is pinned in Task 8.

- [ ] **Step 1: Write the failing tests**

`internal/logic/kick_test.go`:

```go
package logic

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// fakeGateway records Kicks; fail makes every Kick return UNAVAILABLE.
type fakeGateway struct {
	pb.UnimplementedGatewayServer
	mu    sync.Mutex
	kicks []*pb.KickRequest
	fail  bool
	got   chan struct{}
}

func (g *fakeGateway) Kick(_ context.Context, r *pb.KickRequest) (*pb.KickReply, error) {
	g.mu.Lock()
	g.kicks = append(g.kicks, r)
	fail := g.fail
	g.mu.Unlock()
	select {
	case g.got <- struct{}{}:
	default:
	}
	if fail {
		return nil, status.Error(codes.Unavailable, "injected")
	}
	return &pb.KickReply{}, nil
}

func (g *fakeGateway) count() int { g.mu.Lock(); defer g.mu.Unlock(); return len(g.kicks) }

// startGateway serves g on a local port and returns its address.
func startGateway(t *testing.T, g *fakeGateway) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterGatewayServer(srv, g)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// blackhole accepts TCP connections and never answers: like a dead pod's IP
// behind a stale DNS record, a connect "succeeds" but no RPC ever completes.
func blackhole(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		lis.Close()
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	})
	return lis.Addr().String()
}

// staticDial maps gateway names to addresses.
func staticDial(addrs map[string]string) DialFunc {
	return func(gw string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///"+addrs[gw], grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}

type kickEnv struct {
	k   *Kicker
	st  *store.Store
	m   *Metrics
	cfg KickConfig
}

func newKickEnv(t *testing.T, cfg KickConfig, addrs map[string]string) kickEnv {
	t.Helper()
	full := testConfig()
	st, _ := newStore(t, full)
	m := newMetrics()
	k := NewKicker(cfg, NewLeaseCache(st, 20*time.Millisecond, time.Now), m, staticDial(addrs))
	t.Cleanup(k.Close)
	return kickEnv{k: k, st: st, m: m, cfg: cfg}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestKickReachesGateway(t *testing.T) {
	g := &fakeGateway{got: make(chan struct{}, 1)}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: startGateway(t, g)})
	ref := store.EpochRef{Gateway: gw, Epoch: 1 << 63}
	env.k.Kick(ref, 42, pb.KickReason_KICK_REASON_REPLACED)
	<-g.got
	g.mu.Lock()
	r := g.kicks[0]
	g.mu.Unlock()
	if r.Gw.Gateway != gw || r.Gw.Epoch != 1<<63 || r.ConnId != 42 || r.Reason != pb.KickReason_KICK_REASON_REPLACED {
		t.Fatalf("kick %+v", r)
	}
	waitFor(t, "ok attempt counted", func() bool { return counter(env.m.KickAttempts.WithLabelValues("first", "ok")) == 1 })
}

// R5S-O2: an attempt to a blackholed address ends within its deadline, Kick
// never blocks the caller, and with the lease dead there are no retries.
func TestKickToBlackholeEndsWithinDeadline(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	env := newKickEnv(t, cfg, map[string]string{gw: blackhole(t)})
	ref := store.EpochRef{Gateway: gw, Epoch: 3} // no lease: dead

	start := time.Now()
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REPLACED)
	if d := time.Since(start); d > 50*time.Millisecond {
		t.Fatalf("Kick blocked for %v", d)
	}
	waitFor(t, "give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	if d := time.Since(start); d > cfg.Deadline+500*time.Millisecond {
		t.Fatalf("attempt took %v, deadline %v", d, cfg.Deadline)
	}
	time.Sleep(3 * cfg.RetrySpacing)
	if n := counter(env.m.KickAttempts.WithLabelValues("retry", "error")); n != 0 {
		t.Fatalf("%v retries to a dead target", n)
	}
}

func TestKickRetriesOnlyWhileLeaseLive(t *testing.T) {
	g := &fakeGateway{fail: true, got: make(chan struct{}, 8)}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: startGateway(t, g)})
	ref := store.EpochRef{Gateway: gw, Epoch: 9}
	if _, err := env.st.RenewLease(ctx, ref); err != nil {
		t.Fatal(err)
	}
	env.k.Kick(ref, 1, pb.KickReason_KICK_REASON_REVOKED)
	waitFor(t, "live give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("live")) == 1 })
	if n := g.count(); n != 1+env.cfg.Retries {
		t.Fatalf("attempts %d, want %d", n, 1+env.cfg.Retries)
	}

	// Same failure with the lease gone: one attempt, no retry.
	if err := env.st.EndEpoch(ctx, ref); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond) // past the 20ms lease cache
	env.k.Kick(ref, 2, pb.KickReason_KICK_REASON_REVOKED)
	waitFor(t, "dead give-up", func() bool { return counter(env.m.KickGiveUps.WithLabelValues("dead")) == 1 })
	time.Sleep(3 * env.cfg.RetrySpacing)
	if n := g.count(); n != 1+env.cfg.Retries+1 {
		t.Fatalf("attempts %d, want %d", n, 1+env.cfg.Retries+1)
	}
}

// R5S-O2: a full per-target queue drops and counts instead of growing.
func TestKickQueueFullDrops(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	cfg.Workers, cfg.Queue = 1, 2
	env := newKickEnv(t, cfg, map[string]string{gw: blackhole(t)})
	ref := store.EpochRef{Gateway: gw, Epoch: 4}
	for i := range 10 {
		env.k.Kick(ref, uint64(i), pb.KickReason_KICK_REASON_REPLACED)
	}
	// One attempt in flight (or not yet taken) and two queued; the rest dropped.
	// No attempt has failed yet, so the lease was never read: "unknown".
	d := counter(env.m.KickDropped.WithLabelValues("unknown"))
	if d < 7 || d > 8 {
		t.Fatalf("dropped %v, want 7 or 8", d)
	}
	if q := counter(env.m.KickQueueDepth.WithLabelValues(gw + ":4")); q > 2 {
		t.Fatalf("queue depth %v above its size", q)
	}
}

// slowLeases makes every lease read take a second.
type slowLeases struct{ *store.Store }

func (s slowLeases) LeasesExist(ctx context.Context, refs []store.EpochRef) ([]bool, error) {
	time.Sleep(time.Second)
	return s.Store.LeasesExist(ctx, refs)
}

// R5S-O2: Kick never reads Redis on the caller's goroutine, not even to label
// a drop. LoginEach calls done one entry after another per node group, so a
// blocking Kick would hold back the next entries' results.
func TestKickDropDoesNotReadRedis(t *testing.T) {
	gw := testredis.Gateway()
	cfg := testConfig().Kick
	cfg.Workers, cfg.Queue = 1, 1
	st, _ := newStore(t, testConfig())
	k := NewKicker(cfg, NewLeaseCache(slowLeases{st}, time.Minute, time.Now), newMetrics(),
		staticDial(map[string]string{gw: blackhole(t)}))
	t.Cleanup(k.Close)
	start := time.Now()
	for i := range 10 {
		k.Kick(store.EpochRef{Gateway: gw, Epoch: 4}, uint64(i), pb.KickReason_KICK_REASON_REPLACED)
	}
	if d := time.Since(start); d > 100*time.Millisecond {
		t.Fatalf("10 Kicks to a full queue took %v", d)
	}
}

func TestIdleTargetIsRemoved(t *testing.T) {
	g := &fakeGateway{got: make(chan struct{}, 1)}
	gw := testredis.Gateway()
	env := newKickEnv(t, testConfig().Kick, map[string]string{gw: startGateway(t, g)})
	env.k.Kick(store.EpochRef{Gateway: gw, Epoch: 1}, 1, pb.KickReason_KICK_REASON_REPLACED)
	<-g.got
	if env.k.targetCount() != 1 {
		t.Fatal("no target")
	}
	waitFor(t, "idle target removed", func() bool { return env.k.targetCount() == 0 })
	// A later Kick to the same target works again.
	env.k.Kick(store.EpochRef{Gateway: gw, Epoch: 1}, 2, pb.KickReason_KICK_REASON_REPLACED)
	<-g.got
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL to build: `undefined: DialFunc`, `undefined: Kicker`, `undefined: NewKicker`.

- [ ] **Step 3: Implement**

`internal/logic/kick.go`:

```go
package logic

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// DialFunc opens a client connection to a gateway pod by name. It must not
// block (grpc.NewClient connects lazily).
type DialFunc func(gateway string) (*grpc.ClientConn, error)

// PodDNSDial dials gateways by StatefulSet pod DNS through addrFormat
// (spec: Logic→Gateway Kick, no registry).
func PodDNSDial(addrFormat string) DialFunc {
	return func(gw string) (*grpc.ClientConn, error) {
		return grpc.NewClient("dns:///"+fmt.Sprintf(addrFormat, gw),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}

// Kicker is the hardened Kick client (R1-O8, R2-O6, R5S-O2). Each target
// gw|epoch has a bounded queue drained by a bounded set of workers; every
// attempt has its own deadline; retries go through the same queue and happen
// only while the target's lease is live. A full queue drops the attempt:
// Reconcile is the backstop (race 8).
type Kicker struct {
	cfg    KickConfig
	leases *LeaseCache
	m      *Metrics
	dial   DialFunc

	mu      sync.Mutex
	targets map[store.EpochRef]*kickTarget
	conns   map[string]*gwConn // by gateway pod name, shared by its epochs
	closed  bool
	done    chan struct{}
	wg      sync.WaitGroup
}

type kickTarget struct {
	ref     store.EpochRef
	label   string
	q       chan kickJob
	client  pb.GatewayClient
	workers int // running workers; guarded by Kicker.mu
}

type gwConn struct {
	cc   *grpc.ClientConn
	refs int
}

type kickJob struct {
	connID  uint64
	reason  pb.KickReason
	attempt int // 0 = first
}

// NewKicker returns a Kicker; Close stops it.
func NewKicker(cfg KickConfig, leases *LeaseCache, m *Metrics, dial DialFunc) *Kicker {
	return &Kicker{cfg: cfg, leases: leases, m: m, dial: dial,
		targets: map[store.EpochRef]*kickTarget{}, conns: map[string]*gwConn{}, done: make(chan struct{})}
}

// Kick queues the first attempt and returns at once. The first attempt is
// always queued, whatever the target's lease looks like (R2-O6).
func (k *Kicker) Kick(ref store.EpochRef, connID uint64, reason pb.KickReason) {
	k.enqueue(ref, kickJob{connID: connID, reason: reason})
}

func (k *Kicker) enqueue(ref store.EpochRef, j kickJob) {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return
	}
	t, err := k.targetLocked(ref)
	if err != nil {
		k.mu.Unlock()
		slog.Warn("kick: dial failed", "gw", ref.Gateway, "epoch", ref.Epoch, "err", err)
		k.m.KickGiveUps.WithLabelValues(k.cachedLeaseLabel(ref)).Inc()
		return
	}
	select {
	case t.q <- j:
		k.m.KickQueueDepth.WithLabelValues(t.label).Set(float64(len(t.q)))
		// Workers follow the load up to the cap and retire when idle.
		if t.workers < k.cfg.Workers {
			t.workers++
			k.wg.Add(1)
			go k.work(t)
		}
		k.mu.Unlock()
	default:
		k.mu.Unlock()
		k.m.KickDropped.WithLabelValues(k.cachedLeaseLabel(ref)).Inc()
	}
}

// targetLocked returns ref's target, creating it if needed.
func (k *Kicker) targetLocked(ref store.EpochRef) (*kickTarget, error) {
	if t, ok := k.targets[ref]; ok {
		return t, nil
	}
	c, ok := k.conns[ref.Gateway]
	if !ok {
		cc, err := k.dial(ref.Gateway)
		if err != nil {
			return nil, err
		}
		c = &gwConn{cc: cc}
		k.conns[ref.Gateway] = c
	}
	c.refs++
	t := &kickTarget{ref: ref, label: ref.Gateway + ":" + strconv.FormatUint(ref.Epoch, 10),
		q: make(chan kickJob, k.cfg.Queue), client: pb.NewGatewayClient(c.cc)}
	k.targets[ref] = t
	return t, nil
}

// work drains t's queue. A worker idle for IdleTimeout exits; the last one
// out removes the target, so dead epochs don't keep goroutines or connections.
func (k *Kicker) work(t *kickTarget) {
	defer k.wg.Done()
	idle := time.NewTimer(k.cfg.IdleTimeout)
	defer idle.Stop()
	for {
		select {
		case j := <-t.q:
			k.m.KickQueueDepth.WithLabelValues(t.label).Set(float64(len(t.q)))
			k.attempt(t, j)
			idle.Reset(k.cfg.IdleTimeout)
		case <-idle.C:
			if k.retire(t) {
				return
			}
			idle.Reset(k.cfg.IdleTimeout)
		case <-k.done:
			return
		}
	}
}

// retire lets one idle worker exit unless work arrived meanwhile. The last
// worker out removes the target and, with it, the gateway's connection if no
// other epoch of that gateway uses it.
func (k *Kicker) retire(t *kickTarget) bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if len(t.q) > 0 {
		return false
	}
	t.workers--
	if t.workers > 0 {
		return true
	}
	delete(k.targets, t.ref)
	k.m.KickQueueDepth.DeleteLabelValues(t.label)
	if c := k.conns[t.ref.Gateway]; c != nil {
		if c.refs--; c.refs == 0 {
			delete(k.conns, t.ref.Gateway)
			c.cc.Close()
		}
	}
	return true
}

func (k *Kicker) attempt(t *kickTarget, j kickJob) {
	kind := "first"
	if j.attempt > 0 {
		kind = "retry"
	}
	ctx, cancel := context.WithTimeout(context.Background(), k.cfg.Deadline)
	start := time.Now()
	// Fail-fast (no WaitForReady): a dead pod's address fails or times out
	// within Deadline instead of blocking on connect (R5S-O2).
	_, err := t.client.Kick(ctx, &pb.KickRequest{Gw: pbRef(t.ref), ConnId: j.connID, Reason: j.reason})
	cancel()
	k.m.KickAttemptSeconds.Observe(time.Since(start).Seconds())
	if err == nil {
		k.m.KickAttempts.WithLabelValues(kind, "ok").Inc()
		return
	}
	k.m.KickAttempts.WithLabelValues(kind, "error").Inc()
	if c := status.Code(err); (c != codes.Unavailable && c != codes.DeadlineExceeded) || j.attempt >= k.cfg.Retries {
		k.m.KickGiveUps.WithLabelValues(k.leaseLabel(t.ref)).Inc()
		return
	}
	// Retry only while the target's lease is live (R2-O6).
	if k.leaseLabel(t.ref) != "live" {
		k.m.KickGiveUps.WithLabelValues("dead").Inc()
		return
	}
	j.attempt++
	time.AfterFunc(k.cfg.RetrySpacing, func() { k.enqueue(t.ref, j) })
}

// leaseLabel is the target_lease label: live, dead, or unknown if the lease
// could not be read. It may read Redis: call it from a worker only.
func (k *Kicker) leaseLabel(ref store.EpochRef) string {
	ctx, cancel := context.WithTimeout(context.Background(), k.cfg.Deadline)
	defer cancel()
	live, err := k.leases.LiveOne(ctx, ref)
	switch {
	case err != nil:
		return "unknown"
	case live:
		return "live"
	default:
		return "dead"
	}
}

// cachedLeaseLabel is leaseLabel from the cache only. enqueue runs on the
// caller's goroutine (a Login result callback, Reconcile, Revoke) and must not
// wait on Redis (R5S-O2); a target's workers keep its observation fresh.
func (k *Kicker) cachedLeaseLabel(ref store.EpochRef) string {
	live, ok := k.leases.Peek(ref)
	switch {
	case !ok:
		return "unknown"
	case live:
		return "live"
	default:
		return "dead"
	}
}

// Close stops every worker and closes every connection. Queued Kicks are
// dropped; Reconcile is the backstop.
func (k *Kicker) Close() {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return
	}
	k.closed = true
	close(k.done)
	k.mu.Unlock()
	k.wg.Wait()
	k.mu.Lock()
	defer k.mu.Unlock()
	for gw, c := range k.conns {
		c.cc.Close()
		delete(k.conns, gw)
	}
	k.targets = map[store.EpochRef]*kickTarget{}
}

// targetCount is for tests.
func (k *Kicker) targetCount() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.targets)
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/logic/ && go test -race -count=1 -run 'Kick|Idle' -v ./internal/logic/`
Expected: `TestKickReachesGateway`, `TestKickToBlackholeEndsWithinDeadline`, `TestKickRetriesOnlyWhileLeaseLive`, `TestKickQueueFullDrops`, `TestKickDropDoesNotReadRedis` and `TestIdleTargetIsRemoved` PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/logic
git commit -m "feat(logic): hardened Kick client with per-target bounded queue and deadlines"
```

---

### Task 8: `Login` (streamed per entry) and `Logout` (choices #17, #19, #20)

**Files:**
- Create: `internal/logic/service.go`, `internal/logic/login.go`, `internal/logic/logout.go`
- Test: `internal/logic/service_test.go` (the `env` harness), `internal/logic/login_test.go`

**Interfaces:**
- Consumes: `Store`, `LeaseCache`, `Metrics`, `logger`, `Config` (Task 5); `epochRef`, `pbRef`, `slotField`, `platforms`, `invalid`, `pool` (Task 6); `staticDial`, `blackhole`, `waitFor`, `NewKicker` (Task 7, in tests); `auth.Verify`, `auth.Sign`, `auth.ErrKeyUnknown` (Task 2); `store.LoginReq/LoginResult/LoginCode*`, `store.LogoutReq/LogoutResult`, `session.Entry`, `session.ValidUID` (Plan 1).
- Produces:
  - `type Verifier interface { Verify(token string) (auth.Claims, error) }` (`*auth.Manager` satisfies it)
  - `type KickSender interface { Kick(ref store.EpochRef, connID uint64, reason pb.KickReason) }` (`*Kicker` satisfies it)
  - `type Service` (implements `pb.LogicServer` and `pb.RevocationServer`), `func NewService(cfg Config, st Store, keys Verifier, leases *LeaseCache, kick KickSender, m *Metrics, workers int) *Service`
  - `(*Service).Login(*pb.LoginBatch, grpc.ServerStreamingServer[pb.LoginResult]) error`, `(*Service).Logout(ctx, *pb.LogoutBatch) (*pb.LogoutReply, error)`
  - test harness: `env`, `newEnv(t)`, `newEnvWith(t, cfg, wrap func(*store.Store) Store)`, `(*env).token/loginEntry/dial/login`, `kickRecorder`, `recordedKick`, `staticKeys`

How an entry is handled:
- **Checks before the store.** The gateway's own fields come first (slot, `device_id`, `conn_seq`, `recv_ms`) and fail as `BAD_REQUEST`. Then the token: an unknown kid is `KEY_UNKNOWN`; any other failure, or a token for another appid/platform or with an unusable uid, is `TOKEN_INVALID`. A rejected entry's result is sent at once.
- **Streaming.** The rest go to `LoginEach`. Its `done` callback sends each result on the stream as its node group returns (R5S-O1), serialized by a mutex because `done` runs concurrently and `Send` is not safe for that.
- **Kick.** Queued after the entry's result has been sent (R5S-O2).
- **Follow-ups.** After the last result: one `gwconns` SADD for the batch, then the dead-epoch cross-check. They run on a context detached from the stream (choice #20).
- **Logout.** Unary; `done[i]` is false only when entry `i`'s outcome is unknown (choice #19).

- [ ] **Step 1: Write the test harness and the failing tests**

`internal/logic/service_test.go`:

```go
package logic

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// staticKeys verifies tokens against a fixed key set at the real time.
type staticKeys struct{ ks auth.KeySet }

func (k staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, k.ks, time.Now().UnixMilli(), 0)
}

// kickRecorder records Kicks instead of sending them.
type kickRecorder struct {
	mu    sync.Mutex
	kicks []recordedKick
}

type recordedKick struct {
	ref    store.EpochRef
	connID uint64
	reason pb.KickReason
}

func (r *kickRecorder) Kick(ref store.EpochRef, connID uint64, reason pb.KickReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kicks = append(r.kicks, recordedKick{ref, connID, reason})
}

func (r *kickRecorder) all() []recordedKick {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedKick(nil), r.kicks...)
}

// env is one Logic service on a fresh store, with a signing key and a gateway.
type env struct {
	t     *testing.T
	cfg   Config
	svc   *Service
	real  *store.Store
	rdb   *redis.Client
	kicks *kickRecorder
	m     *Metrics
	key   auth.PrivateKey
	gw    store.EpochRef
}

func newEnv(t *testing.T) *env {
	return newEnvWith(t, testConfig(), func(s *store.Store) Store { return s })
}

// newEnvWith lets a test wrap the real store.
func newEnvWith(t *testing.T, cfg Config, wrap func(*store.Store) Store) *env {
	t.Helper()
	real, rdb := newStore(t, cfg)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := auth.MarshalJWKS(key)
	ks, err := auth.ParseJWKS(doc)
	if err != nil {
		t.Fatal(err)
	}
	st := wrap(real)
	m := newMetrics()
	kicks := &kickRecorder{}
	svc := NewService(cfg, st, staticKeys{ks}, NewLeaseCache(st, cfg.LeaseCache, time.Now), kicks, m, 8)
	return &env{t: t, cfg: cfg, svc: svc, real: real, rdb: rdb, kicks: kicks, m: m, key: key,
		gw: store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 5}}
}

func (e *env) token(uid string, iatMs int64) string {
	e.t.Helper()
	tok, err := auth.Sign(e.key, auth.Claims{UID: uid, AppID: "im", Platform: "mobile",
		IatMs: iatMs, ExpMs: time.Now().UnixMilli() + 3600_000})
	if err != nil {
		e.t.Fatal(err)
	}
	return tok
}

// loginEntry is a valid entry for uid on the mobile slot, received now.
func (e *env) loginEntry(uid string, connID uint64, dev string, seq uint64) *pb.LoginEntry {
	now := redisNow(e.t, e.real)
	return &pb.LoginEntry{ConnId: connID, Token: e.token(uid, now-1000), Appid: "im",
		Platform: pb.Platform_PLATFORM_MOBILE, DeviceId: dev, ConnSeq: seq, RecvMs: now}
}

// dial serves the service on an in-process listener and returns a client.
func (e *env) dial() pb.LogicClient {
	e.t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	pb.RegisterLogicServer(srv, e.svc)
	pb.RegisterRevocationServer(srv, e.svc)
	go srv.Serve(lis)
	e.t.Cleanup(srv.Stop)
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { cc.Close() })
	return pb.NewLogicClient(cc)
}

// login runs one batch through the gRPC stream and returns results by index.
func (e *env) login(c pb.LogicClient, entries ...*pb.LoginEntry) map[uint32]*pb.LoginResult {
	e.t.Helper()
	stream, err := c.Login(ctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: entries})
	if err != nil {
		e.t.Fatal(err)
	}
	out := map[uint32]*pb.LoginResult{}
	for {
		r, err := stream.Recv()
		if err != nil {
			break
		}
		if _, dup := out[r.Index]; dup {
			e.t.Fatalf("index %d reported twice", r.Index)
		}
		out[r.Index] = r
	}
	if len(out) != len(entries) {
		e.t.Fatalf("%d results for %d entries", len(out), len(entries))
	}
	return out
}
```


`internal/logic/login_test.go`:

```go
package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestLoginWritesAndReturnsUID(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	entry := e.loginEntry(uid, 7, "d1", 1)
	r := e.login(c, entry)[0]
	if r.Code != pb.LoginCode_LOGIN_CODE_OK || r.Uid != uid || r.TokIatMs != entry.RecvMs-1000 {
		t.Fatalf("result %+v", r)
	}
	uids, _, err := e.real.ScanConns(ctx, e.gw, 0, 100)
	if err != nil || len(uids) != 1 || uids[0] != uid {
		t.Fatalf("gwconns %v %v, want [%s]", uids, err, uid)
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("kick for an empty slot")
	}
}

func TestLoginKicksReplacedEntry(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	e.login(c, e.loginEntry(uid, 1, "d1", 1))
	// Same device, higher conn_seq: REPLACED.
	if r := e.login(c, e.loginEntry(uid, 2, "d1", 2))[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v", r)
	}
	// Another device: KICKED_BY_OTHER_DEVICE.
	if r := e.login(c, e.loginEntry(uid, 3, "d2", 1))[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v", r)
	}
	want := []recordedKick{
		{e.gw, 1, pb.KickReason_KICK_REASON_REPLACED},
		{e.gw, 2, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE},
	}
	got := e.kicks.all()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("kicks %+v, want %+v", got, want)
	}
	if counter(e.m.Logins.WithLabelValues("replaced")) != 1 || counter(e.m.Logins.WithLabelValues("kicked")) != 1 {
		t.Fatal("result counters")
	}
}

func TestLoginRejections(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	k9, err := auth.GenerateKey("k9")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stale returns the stored conn_seq", func(t *testing.T) {
		uid := testredis.UID()
		e.login(c, e.loginEntry(uid, 1, "d1", 5))
		r := e.login(c, e.loginEntry(uid, 2, "d1", 3))[0]
		if r.Code != pb.LoginCode_LOGIN_CODE_STALE || r.StoredConnSeq != 5 {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("superseded", func(t *testing.T) {
		uid := testredis.UID()
		e.login(c, e.loginEntry(uid, 1, "d1", 1))
		late := e.loginEntry(uid, 2, "d2", 1)
		late.RecvMs -= 100 // received before the stored login
		if r := e.login(c, late)[0]; r.Code != pb.LoginCode_LOGIN_CODE_SUPERSEDED {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("expired", func(t *testing.T) {
		old := e.loginEntry(testredis.UID(), 1, "d1", 1)
		old.RecvMs -= e.cfg.Store.LoginDeadline.Milliseconds() + 1000
		if r := e.login(c, old)[0]; r.Code != pb.LoginCode_LOGIN_CODE_EXPIRED {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("revoked", func(t *testing.T) {
		uid := testredis.UID()
		entry := e.loginEntry(uid, 1, "d1", 1) // token iat = recv − 1s
		if err := e.rdb.HSet(ctx, "rev:{"+uid+"}", "*", entry.RecvMs).Err(); err != nil {
			t.Fatal(err)
		}
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_REVOKED {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("token issued in the marker's millisecond logs in", func(t *testing.T) {
		uid := testredis.UID()
		entry := e.loginEntry(uid, 1, "d1", 1) // iat = recv − 1000
		if err := e.rdb.HSet(ctx, "rev:{"+uid+"}", "*", entry.RecvMs-1000).Err(); err != nil {
			t.Fatal(err)
		}
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
			t.Fatalf("iat == marker: %+v", r)
		}
	})
	t.Run("key unknown", func(t *testing.T) {
		entry := e.loginEntry(testredis.UID(), 1, "d1", 1)
		entry.Token, _ = auth.Sign(k9, auth.Claims{UID: "u", AppID: "im", Platform: "mobile",
			IatMs: 1, ExpMs: time.Now().UnixMilli() + 60_000})
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("token for another slot", func(t *testing.T) {
		entry := e.loginEntry(testredis.UID(), 1, "d1", 1) // token says mobile
		entry.Platform = pb.Platform_PLATFORM_PAD
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_TOKEN_INVALID {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("bad signature", func(t *testing.T) {
		entry := e.loginEntry(testredis.UID(), 1, "d1", 1)
		// Flip a character in the middle of the signature (the last one is
		// partly padding bits, which the decoder ignores).
		tok := []byte(entry.Token)
		i := len(tok) - 20
		if tok[i] == 'A' {
			tok[i] = 'B'
		} else {
			tok[i] = 'A'
		}
		entry.Token = string(tok)
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_TOKEN_INVALID {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("bad request", func(t *testing.T) {
		pipe := e.loginEntry(testredis.UID(), 1, "d|1", 1)
		noPlatform := e.loginEntry(testredis.UID(), 2, "d1", 1)
		noPlatform.Platform = pb.Platform_PLATFORM_UNSPECIFIED
		bigSeq := e.loginEntry(testredis.UID(), 3, "d1", 1<<53)
		good := e.loginEntry(testredis.UID(), 4, "d1", 1)
		rs := e.login(c, pipe, noPlatform, bigSeq, good)
		for i := range uint32(3) {
			if rs[i].Code != pb.LoginCode_LOGIN_CODE_BAD_REQUEST {
				t.Fatalf("entry %d: %+v", i, rs[i])
			}
		}
		if rs[3].Code != pb.LoginCode_LOGIN_CODE_OK {
			t.Fatalf("a bad entry failed its neighbour: %+v", rs[3])
		}
	})
}

// gatedLogin delays the entries of one uid until release is closed (or 2s
// pass, so a handler that collects results fails instead of hanging).
type gatedLogin struct {
	*store.Store
	slowUID string
	release chan struct{}
}

func (g *gatedLogin) LoginEach(ctx context.Context, reqs []store.LoginReq, done func(int, store.LoginResult)) {
	var fast, slow []int
	for i, r := range reqs {
		if r.UID == g.slowUID {
			slow = append(slow, i)
		} else {
			fast = append(fast, i)
		}
	}
	run := func(is []int) {
		sub := make([]store.LoginReq, len(is))
		for k, i := range is {
			sub[k] = reqs[i]
		}
		g.Store.LoginEach(ctx, sub, func(k int, r store.LoginResult) { done(is[k], r) })
	}
	run(fast)
	select {
	case <-g.release:
	case <-time.After(2 * time.Second):
	}
	run(slow)
}

// Spec (R5S-O1): with one entry's script delayed, the other entries' results
// arrive before it.
func TestLoginStreamsEachEntryAsItFinishes(t *testing.T) {
	slow := testredis.UID()
	gate := &gatedLogin{slowUID: slow, release: make(chan struct{})}
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { gate.Store = s; return gate })
	c := e.dial()
	entries := []*pb.LoginEntry{e.loginEntry(slow, 1, "d", 1)}
	for i := range 3 {
		entries = append(entries, e.loginEntry(testredis.UID(), uint64(i+2), "d", 1))
	}
	stream, err := c.Login(ctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for range 3 {
		r, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if r.Index == 0 {
			t.Fatal("the delayed entry arrived before the others")
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("fast entries took %v: held back by the slow one", d)
	}
	close(gate.release)
	r, err := stream.Recv()
	if err != nil || r.Index != 0 || r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("slow entry: %+v %v", r, err)
	}
}

// failingLogin reports every entry's outcome as unknown.
type failingLogin struct{ *store.Store }

func (f failingLogin) LoginEach(_ context.Context, reqs []store.LoginReq, done func(int, store.LoginResult)) {
	for i := range reqs {
		done(i, store.LoginResult{Err: errors.New("i/o timeout")})
	}
}

func TestLoginOutcomeUnknownIsNotDefinitive(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return failingLogin{s} })
	r := e.login(e.dial(), e.loginEntry(testredis.UID(), 1, "d", 1))[0]
	if r.Code != pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN {
		t.Fatalf("result %+v", r)
	}
}

// Spec (R5S-O2): Login results return without waiting for the Kick, even when
// the replaced connection's gateway never answers.
func TestLoginDoesNotWaitForKick(t *testing.T) {
	e := newEnv(t)
	deadGW := testredis.Gateway()
	kicker := NewKicker(e.cfg.Kick, e.svc.leases, e.m, staticDial(map[string]string{deadGW: blackhole(t)}))
	t.Cleanup(kicker.Close)
	e.svc.kick = kicker
	c := e.dial()
	uid := testredis.UID()

	// The stored session lives on a gateway that is now blackholed.
	other := *e
	other.gw = store.EpochRef{Gateway: deadGW, Epoch: 1}
	other.login(c, other.loginEntry(uid, 1, "d1", 1))

	start := time.Now()
	r := e.login(c, e.loginEntry(uid, 2, "d2", 1))[0]
	if d := time.Since(start); d >= e.cfg.Kick.Deadline/2 {
		t.Fatalf("login took %v with a %v kick deadline", d, e.cfg.Kick.Deadline)
	}
	if r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v", r)
	}
	waitFor(t, "the kick attempt", func() bool { return counter(e.m.KickAttempts.WithLabelValues("first", "error")) == 1 })
}

func TestLoginDeadEpochCrossCheck(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	dead := *e
	dead.gw = store.EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	res, err := e.real.RenewLease(ctx, dead.gw) // last renewal recorded in gwepochs
	if err != nil {
		t.Fatal(err)
	}
	dead.login(c, dead.loginEntry(uid, 1, "d1", 1))
	if _, err := e.rdb.Del(ctx, "gwlease:"+dead.gw.Gateway+":{2}").Result(); err != nil {
		t.Fatal(err)
	}
	entry := e.loginEntry(uid, 2, "d1", 2)
	e.login(c, entry)
	h := e.m.DeadEpochRelogin
	if n := sampleCount(t, h); n != 1 {
		t.Fatalf("cross-check samples %d, want 1", n)
	}
	if s := sampleSum(t, h); s < 0 || s > float64(entry.RecvMs-res.RedisNowMs)/1000+0.01 {
		t.Fatalf("cross-check %vs", s)
	}
}

func TestLogout(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	mine, theirs := testredis.UID(), testredis.UID()
	e.login(c, e.loginEntry(mine, 1, "d1", 1), e.loginEntry(theirs, 2, "d1", 1))
	e.login(c, e.loginEntry(theirs, 3, "d1", 2)) // conn 2 replaced by conn 3

	rep, err := c.Logout(ctx, &pb.LogoutBatch{Gw: pbRef(e.gw), Entries: []*pb.LogoutEntry{
		{ConnId: 1, Uid: mine, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE},
		{ConnId: 2, Uid: theirs, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE}, // not the owner any more
		{ConnId: 4, Uid: "bad uid", Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Done) != 3 || !rep.Done[0] || !rep.Done[1] || !rep.Done[2] {
		t.Fatalf("done %v", rep.Done)
	}
	if v, _ := e.rdb.HGet(ctx, "sess:{"+mine+"}", field).Result(); v != "" {
		t.Fatalf("own field not deleted: %q", v)
	}
	if v, _ := e.rdb.HGet(ctx, "sess:{"+theirs+"}", field).Result(); v == "" {
		t.Fatal("a newer session was deleted")
	}
	for label, want := range map[string]float64{"deleted": 1, "not_owner": 1, "invalid": 1} {
		if got := counter(e.m.Logouts.WithLabelValues(label)); got != want {
			t.Errorf("%s = %v, want %v", label, got, want)
		}
	}
}

// failingLogout reports every entry's outcome as unknown.
type failingLogout struct{ *store.Store }

func (f failingLogout) LogoutEach(_ context.Context, reqs []store.LogoutReq, done func(int, store.LogoutResult)) {
	for i := range reqs {
		done(i, store.LogoutResult{Err: errors.New("i/o timeout")})
	}
}

func TestLogoutUnknownOutcomeIsRetried(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return failingLogout{s} })
	rep, err := e.dial().Logout(ctx, &pb.LogoutBatch{Gw: pbRef(e.gw), Entries: []*pb.LogoutEntry{
		{ConnId: 1, Uid: testredis.UID(), Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE}}})
	if err != nil || len(rep.Done) != 1 || rep.Done[0] {
		t.Fatalf("reply %v err %v: want done=[false]", rep, err)
	}
}

// Review Focus 3: two logins for one slot in the same batch (a fast reconnect
// within one 5ms batch) resolve in order: the later one wins and kicks the first.
func TestLoginSameSlotTwiceInOneBatch(t *testing.T) {
	e := newEnv(t)
	uid := testredis.UID()
	rs := e.login(e.dial(), e.loginEntry(uid, 1, "d1", 1), e.loginEntry(uid, 2, "d1", 2))
	if rs[0].Code != pb.LoginCode_LOGIN_CODE_OK || rs[1].Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("results %+v %+v", rs[0], rs[1])
	}
	want := recordedKick{e.gw, 1, pb.KickReason_KICK_REASON_REPLACED}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

// Review Focus 4: an empty batch is answered with an empty stream.
func TestLoginEmptyBatch(t *testing.T) {
	e := newEnv(t)
	e.login(e.dial()) // login() fails the test on a result count other than 0
}

// heldAddConns holds the gwconns SADD until release, then records whether its
// context was still alive.
type heldAddConns struct {
	*store.Store
	release chan struct{}
	ctxErr  chan error
}

func (h *heldAddConns) AddConns(ctx context.Context, ref store.EpochRef, uids []string) error {
	<-h.release
	h.ctxErr <- ctx.Err()
	return h.Store.AddConns(ctx, ref, uids)
}

// Review Focus 2: a gateway that cancels the stream once it has every result
// does not cancel the follow-ups.
func TestLoginFollowUpsOutliveTheStream(t *testing.T) {
	h := &heldAddConns{release: make(chan struct{}), ctxErr: make(chan error, 1)}
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { h.Store = s; return h })
	uid := testredis.UID()
	sctx, cancel := context.WithCancel(ctx)
	stream, err := e.dial().Login(sctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: []*pb.LoginEntry{e.loginEntry(uid, 1, "d", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := stream.Recv(); err != nil || r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v %v", r, err)
	}
	cancel()
	time.Sleep(50 * time.Millisecond) // let the cancellation reach the server
	close(h.release)
	if err := <-h.ctxErr; err != nil {
		t.Fatalf("follow-up context cancelled with the stream: %v", err)
	}
	waitFor(t, "uid in gwconns", func() bool {
		uids, _, _ := e.real.ScanConns(ctx, e.gw, 0, 10)
		return len(uids) == 1
	})
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL to build: `undefined: Service`, `undefined: NewService`, ….

- [ ] **Step 3: Implement**

`internal/logic/service.go`:

```go
package logic

import (
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/store"
)

// Verifier checks an access token (an *auth.Manager in production).
type Verifier interface {
	Verify(token string) (auth.Claims, error)
}

// KickSender queues a Kick (a *Kicker in production). It must not block.
type KickSender interface {
	Kick(ref store.EpochRef, connID uint64, reason pb.KickReason)
}

// Service implements the Logic and Revocation gRPC services.
type Service struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedRevocationServer

	cfg    Config
	st     Store
	keys   Verifier
	leases *LeaseCache
	kick   KickSender
	m      *Metrics
	pool   pool // Login, Logout and Reconcile; the lease RPCs have their own (R1-O2)
	now    func() time.Time
}

// NewService wires the handlers; workers bounds concurrent Login, Logout and
// Reconcile calls.
func NewService(cfg Config, st Store, keys Verifier, leases *LeaseCache, kick KickSender, m *Metrics, workers int) *Service {
	return &Service{cfg: cfg, st: st, keys: keys, leases: leases, kick: kick, m: m,
		pool: newPool(workers), now: time.Now}
}
```


`internal/logic/login.go`:

```go
package logic

import (
	"context"
	"errors"
	"sync"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"google.golang.org/grpc"
)

// Login verifies each entry's token, runs the login Lua and streams each
// entry's result as soon as it is known (R5S-O1). Follow-ups come after the
// result: the Kick of a replaced connection (R5S-O2), then, once the batch is
// done, the gwconns SADD and the dead-epoch cross-check.
func (s *Service) Login(b *pb.LoginBatch, stream grpc.ServerStreamingServer[pb.LoginResult]) error {
	ctx := stream.Context()
	ref, err := epochRef(b.GetGw())
	if err != nil {
		return invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	var mu sync.Mutex // Send is not safe for concurrent use; LoginEach's done is concurrent
	var sendErr error
	send := func(r *pb.LoginResult) {
		if err := stream.Send(r); err != nil && sendErr == nil {
			sendErr = err
		}
	}

	entries := b.GetEntries()
	reqs := make([]store.LoginReq, 0, len(entries))
	idx := make([]int, 0, len(entries))
	for i, e := range entries {
		req, code := s.prepareLogin(ref, e)
		if code != pb.LoginCode_LOGIN_CODE_OK {
			s.m.Logins.WithLabelValues(loginLabel[code]).Inc()
			mu.Lock()
			send(&pb.LoginResult{Index: uint32(i), Code: code})
			mu.Unlock()
			continue
		}
		reqs = append(reqs, req)
		idx = append(idx, i)
	}

	var uids []string
	var prevs []replaced
	start := time.Now()
	s.st.LoginEach(ctx, reqs, func(j int, r store.LoginResult) {
		i := idx[j]
		res, kickReason := s.loginResult(ctx, r)
		res.Index = uint32(i)
		if res.Code == pb.LoginCode_LOGIN_CODE_OK {
			res.Uid, res.TokIatMs = reqs[j].UID, reqs[j].Entry.TokIatMs
		}
		mu.Lock()
		send(res)
		if res.Code == pb.LoginCode_LOGIN_CODE_OK {
			uids = append(uids, reqs[j].UID)
			if r.Prev != nil {
				prevs = append(prevs, replaced{prev: *r.Prev, loginMs: reqs[j].Entry.LoginMs})
			}
		}
		mu.Unlock()
		// The Kick is queued after the result is sent and never delays it (R5S-O2).
		if kickReason != pb.KickReason_KICK_REASON_UNSPECIFIED && r.Prev != nil {
			s.kick.Kick(store.EpochRef{Gateway: r.Prev.Gateway, Epoch: r.Prev.Epoch}, r.Prev.ConnID, kickReason)
		}
	})
	s.m.StoreSeconds.WithLabelValues("login").Observe(time.Since(start).Seconds())

	// Follow-ups that don't change any result. They outlive the stream: a
	// gateway that cancels once it has every result must not cancel them. A
	// lost SADD is repaired by Reconcile rule 2 (gwconns is only a cleanup hint).
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), followUpTimeout)
	defer cancel()
	if len(uids) > 0 {
		if err := s.st.AddConns(fctx, ref, uids); err != nil {
			s.m.ConnsAddFailures.Inc()
			logger(ctx).Warn("login: gwconns SADD failed", "gw", ref.Gateway, "epoch", ref.Epoch, "uids", len(uids), "err", err)
		}
	}
	s.crossCheck(fctx, ref, prevs)
	return sendErr
}

// followUpTimeout bounds Login's follow-ups after the results are sent.
const followUpTimeout = time.Second

// prepareLogin validates one entry and verifies its token. It returns the
// store request and LOGIN_CODE_OK, or the rejection code.
func (s *Service) prepareLogin(ref store.EpochRef, e *pb.LoginEntry) (store.LoginReq, pb.LoginCode) {
	field, err := slotField(e.GetAppid(), e.GetPlatform())
	if err != nil {
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_BAD_REQUEST
	}
	entry := session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: e.GetConnId(),
		DeviceID: e.GetDeviceId(), ConnSeq: e.GetConnSeq(), LoginMs: e.GetRecvMs()}
	if err := entry.Validate(); err != nil {
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_BAD_REQUEST
	}
	c, err := s.keys.Verify(e.GetToken())
	switch {
	case errors.Is(err, auth.ErrKeyUnknown):
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN
	case err != nil:
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_TOKEN_INVALID
	}
	// The slot must be the one the token was issued for (spec: Presence).
	if c.AppID != e.GetAppid() || c.Platform != string(platforms[e.GetPlatform()]) || session.ValidUID(c.UID) != nil {
		return store.LoginReq{}, pb.LoginCode_LOGIN_CODE_TOKEN_INVALID
	}
	entry.TokIatMs = c.IatMs
	return store.LoginReq{UID: c.UID, Field: field, Entry: entry}, pb.LoginCode_LOGIN_CODE_OK
}

var loginLabel = map[pb.LoginCode]string{
	pb.LoginCode_LOGIN_CODE_OK:              "ok",
	pb.LoginCode_LOGIN_CODE_STALE:           "stale",
	pb.LoginCode_LOGIN_CODE_SUPERSEDED:      "superseded",
	pb.LoginCode_LOGIN_CODE_EXPIRED:         "expired",
	pb.LoginCode_LOGIN_CODE_REVOKED:         "revoked",
	pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN:     "key_unknown",
	pb.LoginCode_LOGIN_CODE_TOKEN_INVALID:   "token_invalid",
	pb.LoginCode_LOGIN_CODE_BAD_REQUEST:     "bad_request",
	pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN: "error",
}

// loginResult maps the store's result to the RPC result and the reason to
// kick the replaced connection with (UNSPECIFIED: no kick).
func (s *Service) loginResult(ctx context.Context, r store.LoginResult) (*pb.LoginResult, pb.KickReason) {
	if r.Err != nil {
		s.m.Logins.WithLabelValues("error").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN}, pb.KickReason_KICK_REASON_UNSPECIFIED
	}
	if r.PrevErr != nil {
		// The stored entry did not decode: follow Code, skip the kick;
		// Reconcile closes the old connection (PI-3).
		s.m.Undecodable.WithLabelValues("login").Inc()
	}
	ok := &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_OK}
	none := pb.KickReason_KICK_REASON_UNSPECIFIED
	switch r.Code {
	case store.LoginWritten, store.LoginAlready, store.LoginRepaired:
		s.m.Logins.WithLabelValues("ok").Inc()
		return ok, none
	case store.LoginReplaced:
		s.m.Logins.WithLabelValues("replaced").Inc()
		return ok, pb.KickReason_KICK_REASON_REPLACED
	case store.LoginRegressed:
		s.m.Logins.WithLabelValues("replaced").Inc()
		s.m.ConnSeqRegressed.Inc()
		return ok, pb.KickReason_KICK_REASON_REPLACED
	case store.LoginKicked:
		s.m.Logins.WithLabelValues("kicked").Inc()
		return ok, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE
	case store.LoginStale:
		s.m.Logins.WithLabelValues("stale").Inc()
		res := &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_STALE}
		if r.Prev != nil {
			res.StoredConnSeq = r.Prev.ConnSeq
		}
		return res, none
	case store.LoginSuperseded:
		s.m.Logins.WithLabelValues("superseded").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_SUPERSEDED}, none
	case store.LoginExpired:
		s.m.Logins.WithLabelValues("expired").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_EXPIRED}, none
	case store.LoginRevoked:
		s.m.Logins.WithLabelValues("revoked").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_REVOKED}, none
	default:
		logger(ctx).Error("login: unknown script result", "code", r.Code)
		s.m.Logins.WithLabelValues("error").Inc()
		return &pb.LoginResult{Code: pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN}, none
	}
}

// replaced is a stored entry a login replaced, with the new login's login_ms.
type replaced struct {
	prev    session.Entry
	loginMs int64
}

// crossCheck records, for each relogin that replaced an entry of another
// epoch whose lease is gone, login_ms − that epoch's last renewal: time from
// gateway death to relogin, with no client data (R1-O7). Ended epochs
// (score 0) and swept ones (no score) are skipped.
func (s *Service) crossCheck(ctx context.Context, ref store.EpochRef, rs []replaced) {
	var refs []store.EpochRef
	for _, r := range rs {
		if pr := (store.EpochRef{Gateway: r.prev.Gateway, Epoch: r.prev.Epoch}); pr != ref {
			refs = append(refs, pr)
		}
	}
	if len(refs) == 0 {
		return
	}
	live, err := s.leases.Live(ctx, refs)
	if err != nil {
		return
	}
	for _, r := range rs {
		pr := store.EpochRef{Gateway: r.prev.Gateway, Epoch: r.prev.Epoch}
		if pr == ref || live[pr] {
			continue
		}
		renewed, ok, err := s.leases.RenewedMs(ctx, pr)
		if err != nil || !ok || renewed == 0 {
			continue
		}
		s.m.DeadEpochRelogin.Observe(float64(r.loginMs-renewed) / 1000)
	}
}
```


`internal/logic/logout.go`:

```go
package logic

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
)

// Logout compare-and-deletes each entry's field (spec: Logout Lua). done[i]
// is false only when entry i's outcome is unknown; the gateway retries those.
// The gateway's Logout fence, not Logic, orders a Logout after its Login.
func (s *Service) Logout(ctx context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	ref, err := epochRef(b.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	entries := b.GetEntries()
	done := make([]bool, len(entries))
	reqs := make([]store.LogoutReq, 0, len(entries))
	idx := make([]int, 0, len(entries))
	for i, e := range entries {
		field, err := slotField(e.GetAppid(), e.GetPlatform())
		if err == nil {
			err = session.ValidUID(e.GetUid())
		}
		if err != nil {
			// A retry can't fix it; the gateway drops it and Reconcile's
			// delete phase removes any field it left.
			done[i] = true
			s.m.Logouts.WithLabelValues("invalid").Inc()
			logger(ctx).Warn("logout: invalid entry", "uid", e.GetUid(), "conn_id", e.GetConnId(), "err", err)
			continue
		}
		reqs = append(reqs, store.LogoutReq{UID: e.GetUid(), Field: field,
			Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: e.GetConnId()})
		idx = append(idx, i)
	}
	start := time.Now()
	// done is called concurrently, but each call writes its own element.
	s.st.LogoutEach(ctx, reqs, func(j int, r store.LogoutResult) {
		switch {
		case r.Err != nil:
			s.m.Logouts.WithLabelValues("unknown").Inc()
		case r.Deleted:
			done[idx[j]] = true
			s.m.Logouts.WithLabelValues("deleted").Inc()
		default:
			done[idx[j]] = true
			s.m.Logouts.WithLabelValues("not_owner").Inc()
		}
	})
	s.m.StoreSeconds.WithLabelValues("logout").Observe(time.Since(start).Seconds())
	return &pb.LogoutReply{Done: done}, nil
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/logic/ && go test -race -count=1 -run 'Login|Logout' -v ./internal/logic/`
Expected: every `TestLogin*` and `TestLogout*` PASS, including `TestLoginStreamsEachEntryAsItFinishes` (the spec's per-entry results test), `TestLoginDoesNotWaitForKick` (R5S-O2) and the Review Focus tests 1–4.

- [ ] **Step 5: Commit**

```bash
git add internal/logic
git commit -m "feat(logic): Login streams per-entry results, kicks after the result; Logout"
```

---

### Task 9: `Reconcile` rules 1–4 (choice #19)

**Files:**
- Create: `internal/logic/reconcile.go`
- Test: `internal/logic/reconcile_test.go`

**Interfaces:**
- Consumes: `Service` and its fields (Task 8); `store.SlotRef`, `ReadSlots`, `CompareAndSet`, `CASCode*`, `RefreshSlots`, `session.ChunkWins`, `Entry.OwnerPrefix` (Plan 1).
- Produces: `(*Service).Reconcile(ctx, *pb.ReconcileChunk) (*pb.ReconcileReply, error)`; test helpers `(*env).rEntry/asEntry/put/get/reconcile/liveGateway`, `onGateway`.

One chunk costs one `ReadSlots` pipeline, one cached lease lookup for the stored entries' epochs, one `CompareAndSet` pipeline for the rule-3 and rule-4 writes, and one `RefreshSlots` pipeline (TTL and `gwconns` for every `written` entry). A stored value that doesn't decode (PI-3) is overwritten by rule 4, using its raw value as `ExpectRaw`, and counted. A compare-and-set result of `REVOKED` (a Revoke landed after the read, roadmap choice #4) becomes `not_owner(REVOKED)`. Kicks for rule-3 overrides are queued only for `WRITTEN` results, and they are sent even when another entry's unknown outcome fails the RPC.

The two spec tests this task owns: "Reconcile rule 3 after a simulated lost write" and "Reconcile rule 3 with a regressed counter … Equal `login_ms` on two devices: the stored session wins and no kick loop occurs". The roadmap lists them as Tier 1a on real Redis; here they run on miniredis at Tier 1a and on real Redis at Tier 2.

- [ ] **Step 1: Write the failing tests**

`internal/logic/reconcile_test.go`:

```go
package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rEntry is a chunk entry for uid on e.gw.
func (e *env) rEntry(uid string, connID uint64, dev string, seq uint64, loginMs int64) *pb.ReconcileEntry {
	return &pb.ReconcileEntry{ConnId: connID, Uid: uid, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE,
		DeviceId: dev, ConnSeq: seq, LoginMs: loginMs, TokIatMs: loginMs - 1000}
}

// asEntry is the session.Entry a chunk entry on e.gw stands for.
func (e *env) asEntry(r *pb.ReconcileEntry) session.Entry {
	return session.Entry{Gateway: e.gw.Gateway, Epoch: e.gw.Epoch, ConnID: r.ConnId, DeviceID: r.DeviceId,
		ConnSeq: r.ConnSeq, LoginMs: r.LoginMs, TokIatMs: r.TokIatMs}
}

// put writes a sess field by hand (a lost write, a rollback, or another gateway's login).
func (e *env) put(uid string, ent session.Entry) {
	e.t.Helper()
	if err := e.rdb.HSet(ctx, "sess:{"+uid+"}", field, ent.Encode()).Err(); err != nil {
		e.t.Fatal(err)
	}
}

func (e *env) get(uid string) string {
	v, _ := e.rdb.HGet(ctx, "sess:{"+uid+"}", field).Result()
	return v
}

func (e *env) reconcile(entries ...*pb.ReconcileEntry) []*pb.ReconcileResult {
	e.t.Helper()
	rep, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: redisNow(e.t, e.real), Entries: entries})
	if err != nil {
		e.t.Fatal(err)
	}
	return rep.Results
}

// liveGateway is another gateway epoch with a live lease.
func (e *env) liveGateway() store.EpochRef {
	e.t.Helper()
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 77}
	if _, err := e.real.RenewLease(ctx, ref); err != nil {
		e.t.Fatal(err)
	}
	return ref
}

func onGateway(ref store.EpochRef, conn uint64, dev string, seq uint64, loginMs int64) session.Entry {
	return session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: conn, DeviceID: dev,
		ConnSeq: seq, LoginMs: loginMs, TokIatMs: loginMs - 1000}
}

func code(r *pb.ReconcileResult) pb.ReconcileCode { return r.Code }

func TestReconcileRule1Revoked(t *testing.T) {
	e := newEnv(t)
	uid, now := testredis.UID(), redisNow(t, e.real)
	if err := e.rdb.HSet(ctx, "rev:{"+uid+"}", field, now).Err(); err != nil { // slot marker
		t.Fatal(err)
	}
	r := e.reconcile(e.rEntry(uid, 1, "d", 1, now-10))[0]
	if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != pb.KickReason_KICK_REASON_REVOKED {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != "" {
		t.Fatal("rule 1 wrote")
	}
}

func TestReconcileRule2RefreshesOwnEntry(t *testing.T) {
	e := newEnv(t)
	uid, now := testredis.UID(), redisNow(t, e.real)
	re := e.rEntry(uid, 1, "d", 1, now)
	e.put(uid, e.asEntry(re))
	if err := e.rdb.PExpire(ctx, "sess:{"+uid+"}", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if r := e.reconcile(re)[0]; code(r) != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
		t.Fatalf("result %+v", r)
	}
	if ttl := e.rdb.PTTL(ctx, "sess:{"+uid+"}").Val(); ttl < time.Hour {
		t.Fatalf("TTL not refreshed: %v", ttl)
	}
	if uids, _, _ := e.real.ScanConns(ctx, e.gw, 0, 10); len(uids) != 1 || uids[0] != uid {
		t.Fatalf("gwconns %v", uids)
	}
}

func TestReconcileRule3StoredNewerIsNotOwner(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	now := redisNow(t, e.real)
	for _, c := range []struct {
		dev    string
		reason pb.KickReason
	}{{"d1", pb.KickReason_KICK_REASON_REPLACED}, {"d2", pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}} {
		uid := testredis.UID()
		stored := onGateway(other, 9, c.dev, 5, now) // newer: same login_ms window, higher seq / later
		e.put(uid, stored)
		r := e.reconcile(e.rEntry(uid, 1, "d1", 1, now-100))[0]
		if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != c.reason {
			t.Fatalf("dev %s: result %+v", c.dev, r)
		}
		if e.get(uid) != stored.Encode() {
			t.Fatal("stored session overwritten")
		}
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("not_owner must not kick the stored session")
	}
}

// Spec: Reconcile rule 3 after a simulated lost write (R2-O4): restore an
// older field by hand while the newer connection is live; Reconcile keeps the
// newer connection and kicks the older one.
func TestReconcileRule3AfterLostWrite(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	older := onGateway(other, 9, "d2", 1, now-2000) // another device, earlier login
	e.put(uid, older)                               // the rollback
	newer := e.rEntry(uid, 1, "d1", 1, now)
	if r := e.reconcile(newer)[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != e.asEntry(newer).Encode() {
		t.Fatal("newer session not written back")
	}
	want := recordedKick{other, 9, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

// Spec: Reconcile rule 3 with a regressed counter (R3-O4): a session that won
// through a counter regression keeps winning after its write is rolled back.
func TestReconcileRule3RegressedCounter(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	deadline := e.cfg.Store.LoginDeadline.Milliseconds()
	highSeqOld := onGateway(other, 9, "d1", 9, now-deadline-1000) // before the reinstall
	e.put(uid, highSeqOld)
	regressed := e.rEntry(uid, 1, "d1", 1, now) // low conn_seq, outside the window
	if r := e.reconcile(regressed)[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != e.asEntry(regressed).Encode() {
		t.Fatal("regressed session not kept")
	}
	want := recordedKick{other, 9, pb.KickReason_KICK_REASON_REPLACED}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

// Spec: equal login_ms on two devices: the stored session wins and no kick
// loop occurs.
func TestReconcileRule3TieKeepsStored(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	stored := onGateway(other, 9, "d2", 1, now)
	e.put(uid, stored)
	r := e.reconcile(e.rEntry(uid, 1, "d1", 1, now))[0]
	if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("result %+v", r)
	}
	if e.get(uid) != stored.Encode() || len(e.kicks.all()) != 0 {
		t.Fatal("tie must keep the stored session and kick nobody")
	}
}

func TestReconcileRule4Restores(t *testing.T) {
	e := newEnv(t)
	now := redisNow(t, e.real)
	empty, deadOwner, unreadable := testredis.UID(), testredis.UID(), testredis.UID()
	dead := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 3} // never renewed
	e.put(deadOwner, onGateway(dead, 9, "d2", 1, now+1000))        // even a "newer" one: its lease is gone
	if err := e.rdb.HSet(ctx, "sess:{"+unreadable+"}", field, "garbage").Err(); err != nil {
		t.Fatal(err)
	}
	entries := []*pb.ReconcileEntry{e.rEntry(empty, 1, "d", 1, now), e.rEntry(deadOwner, 2, "d", 1, now), e.rEntry(unreadable, 3, "d", 1, now)}
	for i, r := range e.reconcile(entries...) {
		if r.Code != pb.ReconcileCode_RECONCILE_CODE_WRITTEN {
			t.Fatalf("entry %d: %+v", i, r)
		}
		if got := e.get(entries[i].Uid); got != e.asEntry(entries[i]).Encode() {
			t.Fatalf("entry %d not restored: %q", i, got)
		}
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("rule 4 kicks nobody")
	}
	if counter(e.m.Undecodable.WithLabelValues("reconcile")) != 1 {
		t.Fatal("unreadable entry not counted")
	}
}

func TestReconcileRefusedAfterDeadline(t *testing.T) {
	e := newEnv(t)
	uid, now := testredis.UID(), redisNow(t, e.real)
	rep, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw),
		SentMs:  now - e.cfg.Store.LoginDeadline.Milliseconds() - 1000,
		Entries: []*pb.ReconcileEntry{e.rEntry(uid, 1, "d", 1, now)}})
	if err != nil {
		t.Fatal(err)
	}
	if code(rep.Results[0]) != pb.ReconcileCode_RECONCILE_CODE_REFUSED || e.get(uid) != "" {
		t.Fatalf("result %+v, stored %q", rep.Results[0], e.get(uid))
	}
}

// racingRead changes the slot right after Logic reads it.
type racingRead struct {
	*store.Store
	after func()
}

func (r racingRead) ReadSlots(ctx context.Context, refs []store.SlotRef) []store.SlotState {
	out := r.Store.ReadSlots(ctx, refs)
	r.after()
	return out
}

func TestReconcileConcurrentChange(t *testing.T) {
	uid := testredis.UID()
	var e *env
	var mutate func()
	e = newEnvWith(t, testConfig(), func(s *store.Store) Store { return racingRead{s, func() { mutate() }} })
	now := redisNow(t, e.real)

	// A login lands between the read and the compare-and-set: UNCHANGED, no kick.
	mutate = func() { e.put(uid, onGateway(e.liveGateway(), 9, "d2", 1, now)) }
	if r := e.reconcile(e.rEntry(uid, 1, "d", 1, now))[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_UNCHANGED {
		t.Fatalf("result %+v", r)
	}

	// A Revoke lands between the read and the compare-and-set: NOT_OWNER(REVOKED).
	uid2 := testredis.UID()
	mutate = func() { e.rdb.HSet(ctx, "rev:{"+uid2+"}", "*", now) }
	r := e.reconcile(e.rEntry(uid2, 2, "d", 1, now))[0]
	if r.Code != pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER || r.Reason != pb.KickReason_KICK_REASON_REVOKED || e.get(uid2) != "" {
		t.Fatalf("result %+v stored %q", r, e.get(uid2))
	}

	// Rule 3 (chunk newer than a live stored session) whose compare-and-set
	// loses to a concurrent change: UNCHANGED, and the stored session is not kicked.
	uid3, other := testredis.UID(), e.liveGateway()
	e.put(uid3, onGateway(other, 9, "d2", 1, now-2000))
	mutate = func() { e.put(uid3, onGateway(other, 10, "d3", 1, now-1000)) }
	if r := e.reconcile(e.rEntry(uid3, 3, "d1", 1, now))[0]; r.Code != pb.ReconcileCode_RECONCILE_CODE_UNCHANGED {
		t.Fatalf("rule 3 race: result %+v", r)
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("no kick when the compare-and-set did not write")
	}
}

// Spec (R3-O6): a refused rule-3 write sends no kick.
func TestReconcileRefusedRule3SendsNoKick(t *testing.T) {
	e := newEnv(t)
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	e.put(uid, onGateway(other, 9, "d2", 1, now-2000))
	rep, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw),
		SentMs:  now - e.cfg.Store.LoginDeadline.Milliseconds() - 1000,
		Entries: []*pb.ReconcileEntry{e.rEntry(uid, 1, "d1", 1, now)}})
	if err != nil {
		t.Fatal(err)
	}
	if code(rep.Results[0]) != pb.ReconcileCode_RECONCILE_CODE_REFUSED || len(e.kicks.all()) != 0 {
		t.Fatalf("result %+v kicks %v", rep.Results[0], e.kicks.all())
	}
}

// failingCAS reports every compare-and-set's outcome as unknown.
type failingCAS struct{ *store.Store }

func (f failingCAS) CompareAndSet(_ context.Context, reqs []store.CASReq) []store.CASResult {
	out := make([]store.CASResult, len(reqs))
	for i := range out {
		out[i].Err = errors.New("i/o timeout")
	}
	return out
}

func TestReconcileUnknownOutcomeFailsTheChunk(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return failingCAS{s} })
	_, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: redisNow(t, e.real),
		Entries: []*pb.ReconcileEntry{e.rEntry(testredis.UID(), 1, "d", 1, redisNow(t, e.real))}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
}

// secondCASFails reports the second compare-and-set's outcome as unknown.
type secondCASFails struct{ *store.Store }

func (f secondCASFails) CompareAndSet(ctx context.Context, reqs []store.CASReq) []store.CASResult {
	out := f.Store.CompareAndSet(ctx, reqs[:1])
	for range reqs[1:] {
		out = append(out, store.CASResult{Err: errors.New("i/o timeout")})
	}
	return out
}

// A rule-3 override that wrote is kicked even when another entry of the chunk
// fails the RPC: the stored session it replaced must not wait a whole cycle.
func TestReconcileKicksWrittenOverrideWhenChunkFails(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return secondCASFails{s} })
	other := e.liveGateway()
	uid, now := testredis.UID(), redisNow(t, e.real)
	e.put(uid, onGateway(other, 9, "d2", 1, now-2000))
	_, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: now, Entries: []*pb.ReconcileEntry{
		e.rEntry(uid, 1, "d1", 1, now), e.rEntry(testredis.UID(), 2, "d", 1, now)}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
	want := recordedKick{other, 9, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

func TestReconcileRejectsInvalidChunk(t *testing.T) {
	e := newEnv(t)
	bad := e.rEntry("bad uid", 1, "d", 1, redisNow(t, e.real))
	_, err := e.svc.Reconcile(ctx, &pb.ReconcileChunk{Gw: pbRef(e.gw), SentMs: 1, Entries: []*pb.ReconcileEntry{bad}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err %v", err)
	}
}

// Review Focus 4: an empty chunk gets an empty reply.
func TestReconcileEmptyChunk(t *testing.T) {
	e := newEnv(t)
	if rs := e.reconcile(); len(rs) != 0 {
		t.Fatalf("results %v", rs)
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL. `Service` embeds `pb.UnimplementedLogicServer`, so the tests build, and each fails with `rpc error: code = Unimplemented desc = method Reconcile not implemented`.

- [ ] **Step 3: Implement**

`internal/logic/reconcile.go`:

```go
package logic

import (
	"context"
	"strings"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
)

// Reconcile applies rules 1–4 to each entry of one chunk (spec: Reconcile
// handling). The chunk is resolved as a whole (roadmap choice #14): if any
// entry's outcome is unknown, the RPC fails, and the gateway resolves the
// chunk at its deadline, after which the compare-and-set can no longer write.
func (s *Service) Reconcile(ctx context.Context, c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
	ref, err := epochRef(c.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	entries := c.GetEntries()
	slots := make([]store.SlotRef, len(entries))
	chunk := make([]session.Entry, len(entries))
	for i, e := range entries {
		field, err := slotField(e.GetAppid(), e.GetPlatform())
		if err == nil {
			err = session.ValidUID(e.GetUid())
		}
		ent := session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: e.GetConnId(),
			DeviceID: e.GetDeviceId(), ConnSeq: e.GetConnSeq(), LoginMs: e.GetLoginMs(), TokIatMs: e.GetTokIatMs()}
		if err == nil {
			err = ent.Validate()
		}
		if err != nil {
			// Entries come from Logic's own Login results, so this is a gateway bug.
			return nil, invalid("entry %d: %v", i, err)
		}
		slots[i], chunk[i] = store.SlotRef{UID: e.GetUid(), Field: field}, ent
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	start := time.Now()
	defer func() { s.m.StoreSeconds.WithLabelValues("reconcile").Observe(time.Since(start).Seconds()) }()
	states := s.st.ReadSlots(ctx, slots)
	var others []store.EpochRef
	for i, st := range states {
		if st.Err != nil {
			return nil, unavailable("read slot %d: %v", i, st.Err)
		}
		if st.StoredErr != nil {
			s.m.Undecodable.WithLabelValues("reconcile").Inc()
		}
		if st.Stored != nil {
			others = append(others, storedRef(st.Stored))
		}
	}
	live, err := s.leases.Live(ctx, others)
	if err != nil {
		return nil, unavailable("lease check: %v", err)
	}

	results := make([]*pb.ReconcileResult, len(entries))
	written := &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_WRITTEN}
	var refresh []string // uids whose TTL and gwconns membership to refresh
	var cas []store.CASReq
	var casIdx []int
	var casKick []pb.KickReason // UNSPECIFIED: rule 4, no kick
	deadlineMs := s.cfg.Store.LoginDeadline.Milliseconds()
	for i, e := range chunk {
		st := states[i]
		switch {
		case e.TokIatMs < st.MarkerMs: // rule 1: revoked, write nothing
			results[i] = s.notOwner(pb.KickReason_KICK_REASON_REVOKED)
		case st.Raw != "" && strings.HasPrefix(st.Raw, e.OwnerPrefix()): // rule 2: ours
			results[i] = written
			refresh = append(refresh, slots[i].UID)
			s.m.ReconcileEntries.WithLabelValues("refreshed", "").Inc()
		case st.Stored != nil && live[storedRef(st.Stored)]: // rule 3: another live session
			reason := kickReason(e.DeviceID == st.Stored.DeviceID)
			if !session.ChunkWins(e, *st.Stored, deadlineMs) {
				results[i] = s.notOwner(reason)
				continue
			}
			// Only after a lost write (e.g. a failover): write ours back.
			cas = append(cas, store.CASReq{UID: slots[i].UID, Field: slots[i].Field,
				ExpectRaw: st.Raw, Entry: e, SentMs: c.GetSentMs()})
			casIdx, casKick = append(casIdx, i), append(casKick, reason)
		default: // rule 4: empty, dead lease, or unreadable: restore
			cas = append(cas, store.CASReq{UID: slots[i].UID, Field: slots[i].Field,
				ExpectRaw: st.Raw, Entry: e, SentMs: c.GetSentMs()})
			casIdx, casKick = append(casIdx, i), append(casKick, pb.KickReason_KICK_REASON_UNSPECIFIED)
		}
	}

	var kicks []func()
	var unknown error // first entry whose outcome is unknown
	for j, r := range s.st.CompareAndSet(ctx, cas) {
		i := casIdx[j]
		if r.Err != nil {
			if unknown == nil {
				unknown = unavailable("compare-and-set %d: %v", i, r.Err)
			}
			continue
		}
		switch r.Code {
		case store.CASWritten:
			results[i] = written
			refresh = append(refresh, slots[i].UID)
			if reason := casKick[j]; reason != pb.KickReason_KICK_REASON_UNSPECIFIED {
				// Kick the replaced session only if the compare-and-set wrote (R3-O6).
				prev := *states[i].Stored
				kicks = append(kicks, func() { s.kick.Kick(storedRef(&prev), prev.ConnID, reason) })
				s.m.ReconcileEntries.WithLabelValues("override", "").Inc()
			} else {
				s.m.ReconcileEntries.WithLabelValues("restored", "").Inc()
			}
		case store.CASUnchanged:
			results[i] = &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_UNCHANGED}
			s.m.ReconcileEntries.WithLabelValues("unchanged", "").Inc()
		case store.CASRefused:
			results[i] = &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_REFUSED}
			s.m.ReconcileEntries.WithLabelValues("refused", "").Inc()
		case store.CASRevoked: // a Revoke landed after the read (roadmap choice #4)
			results[i] = s.notOwner(pb.KickReason_KICK_REASON_REVOKED)
		default:
			logger(ctx).Error("reconcile: unknown compare-and-set result", "code", r.Code)
			if unknown == nil {
				unknown = unavailable("compare-and-set %d: result %q", i, r.Code)
			}
		}
	}
	// A compare-and-set that wrote has replaced the stored session, whatever
	// happened to the rest of the chunk: kick it even if the RPC fails below.
	for _, k := range kicks {
		k()
	}
	if unknown != nil {
		return nil, unknown
	}
	// TTL refresh and gwconns SADD are never refused (R3-O6). If they fail,
	// the chunk has no definitive reply; nothing it wrote is wrong.
	if err := s.st.RefreshSlots(ctx, ref, refresh); err != nil {
		return nil, unavailable("refresh: %v", err)
	}
	return &pb.ReconcileReply{Results: results}, nil
}

var kickLabel = map[pb.KickReason]string{
	pb.KickReason_KICK_REASON_REPLACED:               "replaced",
	pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE: "kicked_by_other_device",
	pb.KickReason_KICK_REASON_REVOKED:                "revoked",
}

func (s *Service) notOwner(reason pb.KickReason) *pb.ReconcileResult {
	s.m.ReconcileEntries.WithLabelValues("not_owner", kickLabel[reason]).Inc()
	return &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER, Reason: reason}
}

func storedRef(e *session.Entry) store.EpochRef {
	return store.EpochRef{Gateway: e.Gateway, Epoch: e.Epoch}
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/logic/ && go test -race -count=1 -run Reconcile -v ./internal/logic/`
Expected: every `TestReconcile*` PASS.

Mutation check (expected to fail, then revert): in `reconcile.go`, add `s.kick.Kick(storedRef(st.Stored), st.Stored.ConnID, reason)` right after the rule-3 `casIdx, casKick = append(...)` line, so the stored session is kicked before the compare-and-set. `TestReconcileRule3AfterLostWrite`, `TestReconcileRule3RegressedCounter`, `TestReconcileConcurrentChange` and `TestReconcileRefusedRule3SendsNoKick` must fail. Revert.

- [ ] **Step 5: Commit**

```bash
git add internal/logic
git commit -m "feat(logic): Reconcile rules 1-4 with compare-and-set and kick-only-if-written"
```

---

### Task 10: `ReconcileEnd` delete phase (R3-O1, R5S-O8)

**Files:**
- Create: `internal/logic/reconcile_end.go`
- Test: `internal/logic/reconcile_end_test.go`

**Interfaces:**
- Consumes: `Service` (Task 8); `env.put/get` (Task 9); `store.ScanConns`, `DeleteUntracked` (Plan 1); `golang.org/x/time/rate`.
- Produces: `(*Service).ReconcileEnd(ctx, *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error)`; test helpers `phaseFixture`, `dump`, `uidList`, `stableScan`.

The phase pages through `gwconns:gw:{epoch}` with SSCAN (`COUNT` = `DeleteBatch`) and splits each page into parts of at most one batch (`COUNT` is only a hint). It waits on a limiter of `DeleteRate` uids/s with a burst of one batch, then runs `DeleteUntracked` per part. It replies when the scan ends, so the gateway's RPC deadline must cover the tracked uids ÷ rate plus a margin (Plan 3). The limiter starts with a full burst, so N uids take at least (N − batch) ÷ rate.

**miniredis SSCAN (Execution Notes, item 2):** a miniredis SSCAN cursor is an index into the sorted members, so the SREMs the phase makes mid-scan skip members. The multi-page tests therefore wrap `ScanConns` in `stableScan`, which pages through a snapshot, as real Redis's scan guarantee does.

- [ ] **Step 1: Write the failing tests**

`internal/logic/reconcile_end_test.go`:

```go
package logic

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
)

// phaseFixture stores, for n uids on e.gw: a ghost (untracked, before as_of)
// for even i, a tracked connection for odd i, and every 5th uid also holds a
// login received at as_of. It returns the uids and the request.
func phaseFixture(t *testing.T, e *env, uids []string) *pb.ReconcileEndRequest {
	t.Helper()
	asOf := redisNow(t, e.real)
	req := &pb.ReconcileEndRequest{Gw: pbRef(e.gw), AsOfMs: asOf, Pace: pb.Pace_PACE_PERIODIC}
	for i, uid := range uids {
		ent := session.Entry{Gateway: e.gw.Gateway, Epoch: e.gw.Epoch, ConnID: uint64(1000 + i),
			DeviceID: "d", ConnSeq: 1, LoginMs: asOf - 10, TokIatMs: 1}
		if i%5 == 0 {
			ent.LoginMs = asOf // in flight at as_of: kept
		}
		if i%2 == 1 {
			req.TrackedConnIds = append(req.TrackedConnIds, ent.ConnID)
		}
		e.put(uid, ent)
	}
	if err := e.real.AddConns(ctx, e.gw, uids); err != nil {
		t.Fatal(err)
	}
	return req
}

// dump returns, per uid, whether its field is still there and whether the
// uid is still in gwconns: the delete phase's outcome, comparable across envs.
func dump(t *testing.T, e *env, uids []string) []string {
	t.Helper()
	out := make([]string, len(uids))
	for i, uid := range uids {
		present := e.get(uid) != ""
		in, err := e.rdb.SIsMember(ctx, "gwconns:"+e.gw.Gateway+fmt.Sprintf(":{%d}", e.gw.Epoch), uid).Result()
		if err != nil {
			t.Fatal(err)
		}
		out[i] = fmt.Sprintf("present=%v in=%v", present, in)
	}
	return out
}

func uidList(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = testredis.UID()
	}
	return out
}

// stableScan pages through a snapshot of gwconns taken at cursor 0. On
// miniredis an SSCAN cursor is an index into the sorted members, so the SREMs
// the delete phase makes mid-scan shift it and skip members. Real Redis
// guarantees that a full scan returns every member present throughout, which
// is what the handler relies on; this wrapper restores that guarantee.
type stableScan struct {
	Store
	snap []string
}

func (s *stableScan) ScanConns(ctx context.Context, ref store.EpochRef, cursor uint64, count int64) ([]string, uint64, error) {
	if cursor == 0 {
		s.snap = nil
		for c := uint64(0); ; {
			page, next, err := s.Store.ScanConns(ctx, ref, c, 1000)
			if err != nil {
				return nil, 0, err
			}
			s.snap = append(s.snap, page...)
			if c = next; c == 0 {
				break
			}
		}
	}
	end := min(int(cursor)+int(count), len(s.snap))
	next := uint64(end)
	if end == len(s.snap) {
		next = 0
	}
	return s.snap[cursor:end], next, nil
}

func TestReconcileEndDeletesOnlyUntrackedGhosts(t *testing.T) {
	e := newEnv(t)
	uids := uidList(10)
	other := testredis.UID() // a field of another epoch on a uid in this set
	otherGw := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	req := phaseFixture(t, e, uids)
	if err := e.rdb.HSet(ctx, "sess:{"+other+"}", field,
		session.Entry{Gateway: otherGw.Gateway, Epoch: 2, ConnID: 5, DeviceID: "d", ConnSeq: 1, LoginMs: 1, TokIatMs: 1}.Encode()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := e.real.AddConns(ctx, e.gw, []string{other}); err != nil {
		t.Fatal(err)
	}
	rep, err := e.svc.ReconcileEnd(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	// Ghosts: even i not divisible by 5 → i = 2, 4, 6, 8.
	if rep.Deleted != 4 {
		t.Fatalf("deleted %d, want 4", rep.Deleted)
	}
	for i, uid := range uids {
		gone := e.get(uid) == ""
		if want := i%2 == 0 && i%5 != 0; gone != want {
			t.Errorf("uid %d: deleted %v, want %v", i, gone, want)
		}
	}
	if e.get(other) == "" {
		t.Fatal("another epoch's field was deleted")
	}
	// A uid with no field of this epoch left leaves gwconns.
	in, _ := e.rdb.SIsMember(ctx, fmt.Sprintf("gwconns:%s:{%d}", e.gw.Gateway, e.gw.Epoch), uids[2]).Result()
	if in {
		t.Fatal("emptied uid still in gwconns")
	}
}

// Spec (R5S-O8): a delete phase over N uids takes ≥ N ÷ rate (less the
// limiter's initial burst of one batch) and deletes the same fields as an
// unthrottled run.
func TestReconcileEndIsRateLimited(t *testing.T) {
	throttled := testConfig()
	throttled.DeleteRate, throttled.DeleteBatch = 1000, 100
	fast := testConfig()
	fast.DeleteRate, fast.DeleteBatch = 1_000_000, 100

	uids := uidList(600)
	run := func(cfg Config) ([]string, time.Duration) {
		e := newEnvWith(t, cfg, func(s *store.Store) Store { return &stableScan{Store: s} })
		req := phaseFixture(t, e, uids)
		start := time.Now()
		if _, err := e.svc.ReconcileEnd(ctx, req); err != nil {
			t.Fatal(err)
		}
		return dump(t, e, uids), time.Since(start)
	}
	slowState, slow := run(throttled)
	fastState, _ := run(fast)
	if min := time.Duration(600-100) * time.Second / 1000; slow < min {
		t.Fatalf("600 uids at 1000/s took %v, want ≥ %v", slow, min)
	}
	if !reflect.DeepEqual(slowState, fastState) {
		t.Fatal("throttled and unthrottled runs left different state")
	}
}

// interrupted fails DeleteUntracked after the first page, like a Logic node
// dying mid-phase.
type interrupted struct {
	stableScan
	calls int
}

func (s *interrupted) DeleteUntracked(ctx context.Context, ref store.EpochRef, asOf int64, uids []string, tracked map[uint64]struct{}) (int, error) {
	s.calls++
	if s.calls == 2 {
		return 0, errors.New("node died")
	}
	return s.stableScan.DeleteUntracked(ctx, ref, asOf, uids, tracked)
}

// Spec (R5S-O8): a ReconcileEnd interrupted mid-phase and retried leaves the
// same result as one uninterrupted run.
func TestReconcileEndRetryAfterInterruption(t *testing.T) {
	cfg := testConfig()
	cfg.DeleteBatch = 10
	uids := uidList(50)

	cut := &interrupted{}
	e := newEnvWith(t, cfg, func(s *store.Store) Store { cut.stableScan.Store = s; return cut })
	req := phaseFixture(t, e, uids)
	if _, err := e.svc.ReconcileEnd(ctx, req); err == nil {
		t.Fatal("interrupted phase reported success")
	}
	if _, err := e.svc.ReconcileEnd(ctx, req); err != nil { // the gateway's retry, same payload
		t.Fatal(err)
	}

	whole := newEnvWith(t, cfg, func(s *store.Store) Store { return &stableScan{Store: s} })
	req2 := phaseFixture(t, whole, uids)
	if _, err := whole.svc.ReconcileEnd(ctx, req2); err != nil {
		t.Fatal(err)
	}
	if a, b := dump(t, e, uids), dump(t, whole, uids); !reflect.DeepEqual(a, b) {
		t.Fatalf("retried run differs from an uninterrupted one:\n%v\n%v", a, b)
	}
}

func TestReconcileEndRejectsBadRequest(t *testing.T) {
	e := newEnv(t)
	if _, err := e.svc.ReconcileEnd(ctx, &pb.ReconcileEndRequest{Gw: pbRef(e.gw)}); err == nil {
		t.Fatal("as_of_ms 0 accepted")
	}
}

// Review Focus 5: the first pass of a fresh epoch has no gwconns key yet.
func TestReconcileEndWithoutConnsKey(t *testing.T) {
	e := newEnv(t)
	rep, err := e.svc.ReconcileEnd(ctx, &pb.ReconcileEndRequest{Gw: pbRef(e.gw), AsOfMs: redisNow(t, e.real)})
	if err != nil || rep.Deleted != 0 {
		t.Fatalf("reply %v err %v", rep, err)
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL with `rpc error: code = Unimplemented desc = method ReconcileEnd not implemented`.

- [ ] **Step 3: Add the rate limiter dependency and implement**

```bash
go get golang.org/x/time@v0.16.0
```

`internal/logic/reconcile_end.go`:

```go
package logic

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/status"
)

var paceLabel = map[pb.Pace]string{
	pb.Pace_PACE_UNSPECIFIED: "unspecified",
	pb.Pace_PACE_PERIODIC:    "periodic",
	pb.Pace_PACE_TRIGGERED:   "triggered",
}

// ReconcileEnd runs the delete phase for one finished pass (R3-O1, R5S-O8):
// SSCAN gwconns:gw:{epoch} and compare-and-delete every field of this epoch
// whose conn_id is not tracked and whose login_ms < as_of_ms, at most
// DeleteRate uids/s. It replies when the phase completes. A retry after a
// failure resends the same payload; the deletes are idempotent.
func (s *Service) ReconcileEnd(ctx context.Context, r *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
	ref, err := epochRef(r.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	if r.GetAsOfMs() <= 0 {
		return nil, invalid("as_of_ms %d", r.GetAsOfMs())
	}
	ids := r.GetTrackedConnIds()
	tracked := make(map[uint64]struct{}, len(ids))
	for _, id := range ids {
		tracked[id] = struct{}{}
	}
	pace := paceLabel[r.GetPace()]
	s.m.ReconcileEndTracked.Observe(float64(len(ids)))

	batch := s.cfg.DeleteBatch
	lim := rate.NewLimiter(rate.Limit(s.cfg.DeleteRate), batch)
	start := time.Now()
	deleted, scanned := 0, 0
	var cursor uint64
	for {
		uids, next, err := s.st.ScanConns(ctx, ref, cursor, int64(batch))
		if err != nil {
			return nil, unavailable("scan gwconns: %v", err)
		}
		// SSCAN's COUNT is a hint: a page can be larger than the batch.
		for off := 0; off < len(uids); off += batch {
			part := uids[off:min(off+batch, len(uids))]
			if err := lim.WaitN(ctx, len(part)); err != nil {
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			n, err := s.st.DeleteUntracked(ctx, ref, r.GetAsOfMs(), part, tracked)
			deleted += n
			s.m.ReconcileEndDeleted.Add(float64(n))
			if err != nil {
				return nil, unavailable("delete phase: %v", err)
			}
			scanned += len(part)
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	s.m.ReconcileEndSeconds.WithLabelValues(pace).Observe(time.Since(start).Seconds())
	s.m.ReconcileEndScanned.WithLabelValues(pace).Observe(float64(scanned))
	return &pb.ReconcileEndReply{Deleted: uint32(deleted)}, nil
}
```


- [ ] **Step 4: Run the tests**

Run: `go mod tidy && go vet ./internal/logic/ && go test -race -count=3 -run ReconcileEnd -v ./internal/logic/`
Expected: every `TestReconcileEnd*` PASS three times. `TestReconcileEndIsRateLimited` takes ≥ 0.5s.

Mutation check (expected to fail, then revert): change `lim.WaitN(ctx, len(part))` to `lim.WaitN(ctx, 0)`. `TestReconcileEndIsRateLimited` must fail with `600 uids at 1000/s took …ms, want ≥ 500ms`. Revert.

- [ ] **Step 5: Commit**

```bash
git add internal/logic go.mod go.sum
git commit -m "feat(logic): ReconcileEnd rate-limited delete phase"
```

---

### Task 11: `Revoke` (R2-O4, R3-OP2, PI-2, PI-4)

**Files:**
- Create: `internal/logic/revoke.go`
- Test: `internal/logic/revoke_test.go`

**Interfaces:**
- Consumes: `Service` (Task 8); `env.get` (Task 9); `store.Revoke`, `store.ScopeAll`, `store.ErrNotReplicated`, `store.Revoked` (Plan 1, with Task 11's `EntryErr`).
- Produces: `(*Service).Revoke(ctx, *pb.RevokeRequest) (*pb.RevokeReply, error)`.

Validation comes first and is not retryable (`INVALID_ARGUMENT`): uid, slot (when not `all`), and `before_ms` in `(0, Redis TIME + 5m]`. Then `store.Revoke` writes the marker, `WAIT`s on the marker's connection, and deletes. `ErrNotReplicated` and any other store error are `UNAVAILABLE`: the account service retries with the same `before_ms`, and a re-run returns the same deleted entries (PI-2), so their Kicks are resent. Each deleted entry is kicked with `REVOKED`. An entry that didn't decode is counted and left to Reconcile rule 1 (PI-3).

- [ ] **Step 1: Write the failing tests**

`internal/logic/revoke_test.go`:

```go
package logic

import (
	"context"
	"errors"
	"testing"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRevokeRejectsBadArguments(t *testing.T) {
	e := newEnv(t)
	now := redisNow(t, e.real)
	uid := testredis.UID()
	for name, req := range map[string]*pb.RevokeRequest{
		"before_ms zero":          {Uid: uid, All: true, BeforeMs: 0},
		"before_ms 10 min ahead":  {Uid: uid, All: true, BeforeMs: now + 10*60_000},
		"before_ms in seconds×1k": {Uid: uid, All: true, BeforeMs: now * 1000},
		"bad uid":                 {Uid: "a|b", All: true, BeforeMs: now},
		"slot without platform":   {Uid: uid, Appid: "im", BeforeMs: now},
	} {
		if _, err := e.svc.Revoke(ctx, req); status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: err %v, want InvalidArgument", name, err)
		}
	}
	if v, _ := e.rdb.Exists(ctx, "rev:{"+uid+"}").Result(); v != 0 {
		t.Fatal("a rejected Revoke wrote a marker")
	}
}

// miniredis answers WAIT with 0 replicas, so the real store reports
// ErrNotReplicated: the RPC is retryable and nothing is deleted or kicked.
func TestRevokeUnconfirmedMarkerIsRetryable(t *testing.T) {
	if testredis.Real() {
		t.Skip("needs a Redis whose WAIT reports no replica (miniredis)")
	}
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	e.login(c, e.loginEntry(uid, 1, "d", 1))
	_, err := e.svc.Revoke(ctx, &pb.RevokeRequest{Uid: uid, All: true, BeforeMs: redisNow(t, e.real)})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("err %v, want Unavailable", err)
	}
	if e.get(uid) == "" || len(e.kicks.all()) != 0 {
		t.Fatal("an unconfirmed Revoke deleted or kicked")
	}
	if counter(e.m.Revokes.WithLabelValues("all", "not_replicated")) != 1 {
		t.Fatal("not_replicated not counted")
	}
}

// confirmedRevoke stands in for a store whose replica confirms the marker.
type confirmedRevoke struct {
	*store.Store
	out []store.Revoked
	got []string
}

func (c *confirmedRevoke) Revoke(_ context.Context, uid, scope string, beforeMs int64) ([]store.Revoked, error) {
	c.got = append(c.got, scope)
	return c.out, nil
}

func TestRevokeKicksEveryDeletedConnection(t *testing.T) {
	gw := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1 << 62}
	ent := func(conn uint64) session.Entry {
		return session.Entry{Gateway: gw.Gateway, Epoch: gw.Epoch, ConnID: conn, DeviceID: "d", ConnSeq: 1, LoginMs: 1}
	}
	fake := &confirmedRevoke{out: []store.Revoked{
		{Field: "im:mobile", Entry: ent(1)},
		{Field: "im:pad", Entry: ent(2)},
		{Field: "im:web", EntryErr: errors.New("garbage")},
	}}
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { fake.Store = s; return fake })
	now := redisNow(t, e.real)

	rep, err := e.svc.Revoke(ctx, &pb.RevokeRequest{Uid: testredis.UID(), All: true, BeforeMs: now, Reason: "ban"})
	if err != nil || rep.Kicked != 2 {
		t.Fatalf("reply %v err %v", rep, err)
	}
	want := []recordedKick{{gw, 1, pb.KickReason_KICK_REASON_REVOKED}, {gw, 2, pb.KickReason_KICK_REASON_REVOKED}}
	if k := e.kicks.all(); len(k) != 2 || k[0] != want[0] || k[1] != want[1] {
		t.Fatalf("kicks %+v", k)
	}
	if counter(e.m.Undecodable.WithLabelValues("revoke")) != 1 {
		t.Fatal("undecodable deleted entry not counted")
	}

	// Slot scope passes the slot field as the marker scope.
	if _, err := e.svc.Revoke(ctx, &pb.RevokeRequest{Uid: testredis.UID(), Appid: "im",
		Platform: pb.Platform_PLATFORM_PAD, BeforeMs: now}); err != nil {
		t.Fatal(err)
	}
	if fake.got[1] != "im:pad" {
		t.Fatalf("scope %q, want im:pad", fake.got[1])
	}
}

// Tier 2: the full path on a real primary with a replica. Spec (PI-4): a retry
// with the same before_ms leaves the marker unchanged and returns the same
// kicks; a token whose iat ≥ before_ms logs in after the revoke.
func TestRevokeEndToEndOnRealRedis(t *testing.T) {
	testredis.RequireReal(t)
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	e.login(c, e.loginEntry(uid, 1, "d", 1))
	before := redisNow(t, e.real)
	req := &pb.RevokeRequest{Uid: uid, All: true, BeforeMs: before}
	for i := range 2 {
		rep, err := e.svc.Revoke(ctx, req)
		if err != nil || rep.Kicked != 1 {
			t.Fatalf("attempt %d: reply %v err %v", i, rep, err)
		}
	}
	if e.get(uid) != "" {
		t.Fatal("field not deleted")
	}
	if v, _ := e.rdb.HGet(ctx, "rev:{"+uid+"}", "*").Int64(); v != before {
		t.Fatalf("marker %d, want %d", v, before)
	}
	fresh := e.loginEntry(uid, 2, "d", 2)
	fresh.Token = e.token(uid, before) // iat == before_ms: not revoked
	if r := e.login(c, fresh)[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("fresh token after revoke: %+v", r)
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL. `Service` embeds `pb.UnimplementedRevocationServer`, so the tests build; `TestRevokeRejectsBadArguments` fails with `err rpc error: code = Unimplemented desc = method Revoke not implemented, want InvalidArgument`.

- [ ] **Step 3: Implement**

`internal/logic/revoke.go`:

```go
package logic

import (
	"context"
	"errors"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
)

// Revoke writes the revocation marker, confirmed by a replica, then deletes
// the affected fields and kicks their connections with REVOKED (spec: Revoke,
// R2-O4, R3-OP2, PI-4). UNAVAILABLE means the marker was not confirmed and
// nothing was deleted: the account service retries with the same before_ms.
func (s *Service) Revoke(ctx context.Context, r *pb.RevokeRequest) (*pb.RevokeReply, error) {
	scope, label := store.ScopeAll, "all"
	if !r.GetAll() {
		f, err := slotField(r.GetAppid(), r.GetPlatform())
		if err != nil {
			s.m.Revokes.WithLabelValues("slot", "invalid").Inc()
			return nil, invalid("%v", err)
		}
		scope, label = f, "slot"
	}
	if err := session.ValidUID(r.GetUid()); err != nil {
		s.m.Revokes.WithLabelValues(label, "invalid").Inc()
		return nil, invalid("%v", err)
	}
	now, err := s.st.RedisNowMs(ctx)
	if err != nil {
		s.m.Revokes.WithLabelValues(label, "error").Inc()
		return nil, unavailable("redis time: %v", err)
	}
	// Catches a seconds-vs-ms bug in the caller; not retryable (PI-4).
	if b := r.GetBeforeMs(); b <= 0 || b > now+s.cfg.RevokeMaxAhead.Milliseconds() {
		s.m.Revokes.WithLabelValues(label, "invalid").Inc()
		return nil, invalid("before_ms %d: must be positive and at most %v past Redis TIME %d", b, s.cfg.RevokeMaxAhead, now)
	}
	revoked, err := s.st.Revoke(ctx, r.GetUid(), scope, r.GetBeforeMs())
	switch {
	case errors.Is(err, store.ErrNotReplicated):
		s.m.Revokes.WithLabelValues(label, "not_replicated").Inc()
		return nil, unavailable("revocation marker not confirmed by a replica; retry")
	case err != nil:
		s.m.Revokes.WithLabelValues(label, "error").Inc()
		return nil, unavailable("revoke: %v", err)
	}
	kicked := 0
	for _, rv := range revoked {
		if rv.EntryErr != nil {
			// No connection to kick; Reconcile rule 1 closes it (PI-3).
			s.m.Undecodable.WithLabelValues("revoke").Inc()
			continue
		}
		s.kick.Kick(store.EpochRef{Gateway: rv.Entry.Gateway, Epoch: rv.Entry.Epoch}, rv.Entry.ConnID,
			pb.KickReason_KICK_REASON_REVOKED)
		kicked++
	}
	s.m.RevocationKicks.Add(float64(kicked))
	s.m.Revokes.WithLabelValues(label, "ok").Inc()
	logger(ctx).Info("revoked", "uid", r.GetUid(), "scope", scope, "before_ms", r.GetBeforeMs(),
		"reason", r.GetReason(), "kicked", kicked)
	return &pb.RevokeReply{Kicked: uint32(kicked)}, nil
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/logic/ && go test -race -count=1 -run Revoke -v ./internal/logic/`
Expected: `TestRevokeRejectsBadArguments`, `TestRevokeUnconfirmedMarkerIsRetryable` and `TestRevokeKicksEveryDeletedConnection` PASS; `TestRevokeEndToEndOnRealRedis` SKIPs with `TODO: Tier 2 integration` (it runs on the Tier 2 host).

- [ ] **Step 5: Commit**

```bash
git add internal/logic
git commit -m "feat(logic): Revoke with before_ms validation and REVOKED kicks"
```

---

### Task 12: Sweeper and mass-stale guard (R1-O2, R2-O3, R2-O5, R3-O2, R3-O3, R3-OP6, R4M-3, choice #21)

**Files:**
- Create: `internal/logic/guard.go`, `internal/logic/sweeper.go`
- Test: `internal/logic/guard_test.go`, `internal/logic/sweeper_test.go`

**Interfaces:**
- Consumes: `Config`, `Store`, `Metrics` (Task 5); `uidList` (Task 10, in tests); `store.ListEpochs`, `EpochScoreOf`, `ClaimSweep`, `ReleaseSweep`, `LeasesExist`, `ScanConns`, `SweepUsers`, `FinishSweep`, `EndEpoch`, `RedisNowMs` (Plan 1).
- Produces:
  - `type Guard struct { Recent, Hold time.Duration; … }`, `(*Guard).RedisFailed()`, `Observe(now time.Time, redisNowMs int64, epochs []store.EpochScore)`, `Allows(now time.Time) bool`, `Renewing(now time.Time) time.Duration`
  - `type Sweeper`, `func NewSweeper(cfg Config, st Store, m *Metrics, now func() time.Time) *Sweeper`, `(*Sweeper).Run(ctx)`, `(*Sweeper).Cycle(ctx)`

The guard is pure and clock-driven, so the 10s-rhythm rule is tested by simulation. A stale epoch's score is older than 2 × TTL, so any epoch that renewed within 15s is necessarily another one; that is how the spec's "other epoch" is met. Each cycle:
1. Read Redis `TIME` and `ListEpochs`. A failure resets both guard clocks.
2. Feed the guard.
3. For each epoch that ended (score 0, always swept) or is stale (and the guard allows): claim it, then record the score at claim time.
4. If its lease key exists, release the claim and skip it (R4M-3).
5. Otherwise SSCAN its `gwconns` and, per batch, wait on the pod's limiter, delete with `SweepUsers`, then re-check score and lease. If it renewed, release the claim and stop (no UNLINK, no ZREM).
6. On completion, `FinishSweep`.

**Real Redis (Execution Notes, item 3):** the sweeper-loop tests skip on the shared Tier 2 Redis, because a cycle would sweep other tests' epochs. The guard tests need no Redis and always run.

- [ ] **Step 1: Write the failing tests**

`internal/logic/guard_test.go`:

```go
package logic

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

func newGuard() *Guard {
	cfg := DefaultConfig()
	return &Guard{Recent: cfg.RecentRenewal(), Hold: cfg.Store.LeaseTTL}
}

// sim drives a guard with one surviving gateway that renews every 10s plus
// jitter, observed every 5s (the sweeper cycle). Redis and local time are the
// same simulated clock here.
type sim struct {
	g        *Guard
	t        time.Time
	renewed  int64 // last renewal, ms
	nextRenw time.Time
}

func (s *sim) epochs() []store.EpochScore {
	return []store.EpochScore{
		{EpochRef: store.EpochRef{Gateway: "gw-live", Epoch: 1}, ScoreMs: s.renewed},
		{EpochRef: store.EpochRef{Gateway: "gw-dead", Epoch: 2}, ScoreMs: 1}, // stale
	}
}

// run advances 1s at a time for d, renewing on the gateway's rhythm while
// renew is true, and observing every 5s.
func (s *sim) run(d time.Duration, renew bool, jitter func() time.Duration) {
	for end := s.t.Add(d); s.t.Before(end); {
		s.t = s.t.Add(time.Second)
		if renew && !s.t.Before(s.nextRenw) {
			s.renewed = s.t.UnixMilli()
			s.nextRenw = s.t.Add(10*time.Second + jitter())
		}
		if s.t.Unix()%5 == 0 {
			s.g.Observe(s.t, s.t.UnixMilli(), s.epochs())
		}
	}
}

// Spec (R3-O2): renewing_since is not reset by a lone gateway's 10s rhythm
// with jitter, and the guard releases one lease TTL after renewals resume.
func TestGuardSurvivesTenSecondRhythmWithJitter(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	jitter := func() time.Duration { return time.Duration(rng.IntN(3000)) * time.Millisecond } // RPC latency, 0–3s
	s := &sim{g: newGuard(), t: time.Unix(1000, 0)}
	s.run(time.Second, true, jitter)
	start := s.t
	s.run(29*time.Second, true, jitter)
	if s.g.Allows(s.t) {
		t.Fatal("released before one lease TTL of renewals")
	}
	s.run(10*time.Minute, true, jitter)
	if !s.g.Allows(s.t) || s.g.Renewing(s.t) < s.t.Sub(start)-5*time.Second {
		t.Fatalf("renewing_since was reset by the 10s rhythm: renewing %v", s.g.Renewing(s.t))
	}
}

// Spec (R3-O2): renewing_since resets when no other epoch renewed within 15s.
func TestGuardResetsWhenRenewalsStop(t *testing.T) {
	none := func() time.Duration { return 0 }
	s := &sim{g: newGuard(), t: time.Unix(1000, 0)}
	s.run(time.Minute, true, none)
	if !s.g.Allows(s.t) {
		t.Fatal("not released after a minute of renewals")
	}
	s.run(20*time.Second, false, none) // > 15s without a renewal
	if s.g.Allows(s.t) || s.g.Renewing(s.t) != 0 {
		t.Fatal("still released after renewals stopped")
	}
	s.run(25*time.Second, true, none)
	if s.g.Allows(s.t) {
		t.Fatal("released less than one lease TTL after renewals resumed")
	}
	s.run(10*time.Second, true, none)
	if !s.g.Allows(s.t) {
		t.Fatal("not released one lease TTL after renewals resumed")
	}
}

func TestGuardNeedsHealthyRedisForOneTTL(t *testing.T) {
	none := func() time.Duration { return 0 }
	s := &sim{g: newGuard(), t: time.Unix(1000, 0)}
	s.run(time.Minute, true, none)
	s.g.RedisFailed()
	s.run(25*time.Second, true, none)
	if s.g.Allows(s.t) {
		t.Fatal("released within one lease TTL of a Redis failure")
	}
	s.run(10*time.Second, true, none)
	if !s.g.Allows(s.t) {
		t.Fatal("not released after Redis was healthy for one lease TTL")
	}
}

// A systemic outage: no epoch renews at all, so nothing is ever released.
func TestGuardHoldsWhenNobodyRenews(t *testing.T) {
	s := &sim{g: newGuard(), t: time.Unix(1000, 0), renewed: 1}
	s.run(time.Hour, false, nil)
	if s.g.Allows(s.t) {
		t.Fatal("released with no renewals")
	}
}
```


`internal/logic/sweeper_test.go`:

```go
package logic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
)

type sweepEnv struct {
	t   *testing.T
	sw  *Sweeper
	st  Store
	rdb *redis.Client
	clk *fakeClock
	m   *Metrics
	cfg Config
}

func newSweepEnv(t *testing.T, wrap func(*store.Store) Store) *sweepEnv {
	t.Helper()
	// A cycle reads and sweeps the global gwepochs key. The shared Tier 2
	// Redis holds other tests' epochs (other packages run in parallel), so a
	// cycle there would see their renewals and sweep their data. The store
	// primitives a cycle calls are verified on real Redis by Plan 1's tests.
	if testredis.Real() {
		t.Skip("sweeper loop tests need a gwepochs of their own (miniredis only)")
	}
	cfg := testConfig()
	cfg.SweepBatch = 2
	real, rdb := newStore(t, cfg)
	st := wrap(real)
	clk := newClock()
	m := newMetrics()
	return &sweepEnv{t: t, sw: NewSweeper(cfg, st, m, clk.Now), st: st, rdb: rdb, clk: clk, m: m, cfg: cfg}
}

// deadEpoch registers an epoch whose last renewal is 2 × TTL + 1s ago, with no
// lease key, and n sessions; one uid also holds a field of another epoch.
func (e *sweepEnv) deadEpoch(n int) (store.EpochRef, []string) {
	e.t.Helper()
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 1}
	now := redisNow(e.t, e.st)
	member := fmt.Sprintf("%s:%d", ref.Gateway, ref.Epoch)
	if err := e.rdb.ZAdd(ctx, "gwepochs", redis.Z{Score: float64(now - e.cfg.StaleAfter().Milliseconds() - 1000), Member: member}).Err(); err != nil {
		e.t.Fatal(err)
	}
	uids := uidList(n)
	for i, uid := range uids {
		ent := session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: uint64(i + 1), DeviceID: "d", ConnSeq: 1, LoginMs: now, TokIatMs: 1}
		if err := e.rdb.HSet(ctx, "sess:{"+uid+"}", field, ent.Encode()).Err(); err != nil {
			e.t.Fatal(err)
		}
	}
	other := session.Entry{Gateway: "gw-other", Epoch: 5, ConnID: 1, DeviceID: "d", ConnSeq: 1, LoginMs: now, TokIatMs: 1}
	if err := e.rdb.HSet(ctx, "sess:{"+uids[0]+"}", "im:pad", other.Encode()).Err(); err != nil {
		e.t.Fatal(err)
	}
	if err := e.st.AddConns(ctx, ref, uids); err != nil {
		e.t.Fatal(err)
	}
	return ref, uids
}

// liveEpoch renews a lease now (another gateway that keeps renewing).
func (e *sweepEnv) liveEpoch() store.EpochRef {
	e.t.Helper()
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	if _, err := e.st.RenewLease(ctx, ref); err != nil {
		e.t.Fatal(err)
	}
	return ref
}

func (e *sweepEnv) fields(uids []string) (mobile, pad int) {
	for _, uid := range uids {
		if v, _ := e.rdb.HGet(ctx, "sess:{"+uid+"}", field).Result(); v != "" {
			mobile++
		}
		if v, _ := e.rdb.HGet(ctx, "sess:{"+uid+"}", "im:pad").Result(); v != "" {
			pad++
		}
	}
	return
}

func (e *sweepEnv) exists(key string) bool { return e.rdb.Exists(ctx, key).Val() == 1 }

func keysOf(ref store.EpochRef) (conns, claim, member string) {
	suffix := fmt.Sprintf("%s:{%d}", ref.Gateway, ref.Epoch)
	return "gwconns:" + suffix, "sweep:" + suffix, fmt.Sprintf("%s:%d", ref.Gateway, ref.Epoch)
}

func TestSweeperSweepsDeadEpochAfterGuard(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, uids := e.deadEpoch(5)
	live := e.liveEpoch()

	e.sw.Cycle(ctx) // renewing_since starts now: held
	if m, _ := e.fields(uids); m != 5 || counter(e.m.GuardHolds) != 1 || counter(e.m.GuardHolding) != 1 {
		t.Fatalf("swept before the guard released: %d fields left, holds %v", m, counter(e.m.GuardHolds))
	}
	e.clk.Add(e.cfg.Store.LeaseTTL)
	if _, err := e.st.RenewLease(ctx, live); err != nil { // the live gateway keeps renewing
		t.Fatal(err)
	}
	e.sw.Cycle(ctx)
	mobile, pad := e.fields(uids)
	if mobile != 0 || pad != 1 {
		t.Fatalf("after sweep: %d dead-epoch fields left, %d other-epoch fields (want 0, 1)", mobile, pad)
	}
	conns, _, member := keysOf(ref)
	if e.exists(conns) || e.rdb.ZScore(ctx, "gwepochs", member).Err() != redis.Nil {
		t.Fatal("gwconns not unlinked or gwepochs member not removed")
	}
	if counter(e.m.Sweeps.WithLabelValues("done")) != 1 || counter(e.m.GuardHolding) != 0 {
		t.Fatal("metrics")
	}
}

// Spec (R4M-3): the sweeper skips an epoch whose lease key exists although its
// score is stale, and releases its claim.
func TestSweeperSkipsEpochWhoseLeaseExists(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, uids := e.deadEpoch(3)
	e.liveEpoch()
	conns, claim, _ := keysOf(ref)
	if err := e.rdb.Set(ctx, "gwlease:"+conns[len("gwconns:"):], "x", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	e.sw.Cycle(ctx)
	e.clk.Add(e.cfg.Store.LeaseTTL)
	e.liveEpoch()
	e.sw.Cycle(ctx)
	if m, _ := e.fields(uids); m != 3 {
		t.Fatalf("%d fields left, want 3", m)
	}
	if e.exists(claim) || counter(e.m.Sweeps.WithLabelValues("lease_live")) != 1 {
		t.Fatal("claim not released or result not counted")
	}
}

// renewMidSweep renews the epoch after the first SweepUsers batch, like a
// starved gateway coming back during its sweep.
type renewMidSweep struct {
	*store.Store
	ref     store.EpochRef
	batches int
	missing bool
}

func (r *renewMidSweep) SweepUsers(ctx context.Context, ref store.EpochRef, uids []string) (int, error) {
	n, err := r.Store.SweepUsers(ctx, ref, uids)
	if r.batches++; r.batches == 1 {
		res, err := r.Store.RenewLease(ctx, r.ref)
		if err != nil {
			panic(err)
		}
		r.missing = res.Missing
	}
	return n, err
}

// Spec (R3-O3, R3-OP6): a renewal mid-sweep reports missing; the sweeper
// aborts without UNLINK or ZREM and deletes its claim key.
func TestSweeperAbortsWhenEpochRenews(t *testing.T) {
	hook := &renewMidSweep{}
	e := newSweepEnv(t, func(s *store.Store) Store { hook.Store = s; return hook })
	ref, uids := e.deadEpoch(6)
	hook.ref = ref
	e.liveEpoch()
	e.sw.Cycle(ctx)
	e.clk.Add(e.cfg.Store.LeaseTTL)
	e.liveEpoch()
	e.sw.Cycle(ctx)

	if !hook.missing {
		t.Fatal("the renewal during a sweep must report missing")
	}
	if m, _ := e.fields(uids); m != 4 { // one batch of 2 deleted before the abort
		t.Fatalf("%d fields left, want 4", m)
	}
	conns, claim, member := keysOf(ref)
	if !e.exists(conns) || e.rdb.ZScore(ctx, "gwepochs", member).Err() != nil {
		t.Fatal("an aborted sweep unlinked gwconns or removed the member")
	}
	if e.exists(claim) || counter(e.m.Sweeps.WithLabelValues("aborted")) != 1 {
		t.Fatal("claim not deleted or abort not counted")
	}
}

// Epochs that sent EpochEnded are exempt from the guard (roadmap choice #2).
func TestSweeperEndedEpochIsExempt(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, uids := e.deadEpoch(2)
	if err := e.st.EndEpoch(ctx, ref); err != nil { // score 0
		t.Fatal(err)
	}
	e.sw.Cycle(ctx) // fresh sweeper, no renewals at all: the guard would hold
	if m, _ := e.fields(uids); m != 0 {
		t.Fatalf("%d fields of an ended epoch left", m)
	}
}

// A systemic outage: nobody renews, so a stale epoch is never swept.
func TestSweeperHoldsWhenNobodyRenews(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	_, uids := e.deadEpoch(2)
	for range 20 {
		e.sw.Cycle(ctx)
		e.clk.Add(e.cfg.SweepInterval)
	}
	if m, _ := e.fields(uids); m != 2 || counter(e.m.GuardHolding) != 1 {
		t.Fatalf("%d fields left, holding %v", m, counter(e.m.GuardHolding))
	}
}

func TestTwoSweepersClaimOnce(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, _ := e.deadEpoch(2)
	if ok, err := e.st.ClaimSweep(ctx, ref); !ok || err != nil { // another node holds it
		t.Fatalf("claim %v %v", ok, err)
	}
	e.liveEpoch()
	e.sw.Cycle(ctx)
	e.clk.Add(e.cfg.Store.LeaseTTL)
	e.liveEpoch()
	e.sw.Cycle(ctx)
	if counter(e.m.Sweeps.WithLabelValues("claimed_elsewhere")) != 1 {
		t.Fatal("swept an epoch another node had claimed")
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/`
Expected: FAIL to build: `undefined: Guard`, `undefined: Sweeper`, `undefined: NewSweeper`.

- [ ] **Step 3: Implement**

`internal/logic/guard.go`:

```go
package logic

import (
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

// Guard is the sweeper's mass-stale guard (R1-O2, R2-O3, R3-O2). A stale
// epoch may be swept only if
//   - some epoch renewed within Recent (1.5 × the renewal interval), and that
//     has held continuously for Hold (one lease TTL): renewing_since; and
//   - the sweeper's own Redis access has been healthy for Hold.
//
// A stale epoch's score is older than 2 × lease TTL, so any epoch that renewed
// within Recent is another epoch, as the spec requires. Durations are measured
// on the local clock; score ages on Redis TIME, the clock the scores use. All
// state is in memory: a restart starts both clocks over, which is conservative.
type Guard struct {
	Recent time.Duration
	Hold   time.Duration

	renewingSince time.Time // zero while no epoch renews
	healthySince  time.Time // zero after a Redis failure
}

// RedisFailed records a failed cycle: Redis health restarts, and with no view
// of the scores, so does renewing_since.
func (g *Guard) RedisFailed() {
	g.healthySince = time.Time{}
	g.renewingSince = time.Time{}
}

// Observe records one successful cycle at local time now.
func (g *Guard) Observe(now time.Time, redisNowMs int64, epochs []store.EpochScore) {
	if g.healthySince.IsZero() {
		g.healthySince = now
	}
	renewing := false
	for _, e := range epochs {
		if e.ScoreMs > 0 && redisNowMs-e.ScoreMs <= g.Recent.Milliseconds() {
			renewing = true
			break
		}
	}
	switch {
	case !renewing:
		g.renewingSince = time.Time{}
	case g.renewingSince.IsZero():
		g.renewingSince = now
	}
}

// Allows reports whether a stale (not ended) epoch may be swept at now.
func (g *Guard) Allows(now time.Time) bool {
	return !g.renewingSince.IsZero() && now.Sub(g.renewingSince) >= g.Hold &&
		!g.healthySince.IsZero() && now.Sub(g.healthySince) >= g.Hold
}

// Renewing returns how long renewals have held continuously (0 if they don't).
func (g *Guard) Renewing(now time.Time) time.Duration {
	if g.renewingSince.IsZero() {
		return 0
	}
	return now.Sub(g.renewingSince)
}
```


`internal/logic/sweeper.go`:

```go
package logic

import (
	"context"
	"log/slog"
	"time"

	"github.com/Tallinzp/goim/internal/store"
	"golang.org/x/time/rate"
)

// Sweeper deletes the entries of dead gateway epochs (spec: Sweeper). Every
// Logic node runs one; the sweep: claim keeps two nodes off the same epoch.
// Sweeping only cleans up: a dead epoch's entries are already invalid by the
// validity rule, so a held-back sweep never affects correctness.
type Sweeper struct {
	cfg   Config
	st    Store
	m     *Metrics
	now   func() time.Time
	guard Guard
	lim   *rate.Limiter // ~10k uids/s across every epoch this node sweeps
}

// NewSweeper returns a sweeper; now is the local clock (time.Now in production).
func NewSweeper(cfg Config, st Store, m *Metrics, now func() time.Time) *Sweeper {
	return &Sweeper{cfg: cfg, st: st, m: m, now: now,
		guard: Guard{Recent: cfg.RecentRenewal(), Hold: cfg.Store.LeaseTTL},
		lim:   rate.NewLimiter(rate.Limit(cfg.SweepRate), cfg.SweepBatch)}
}

// Run calls Cycle every SweepInterval until ctx ends.
func (s *Sweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Cycle(ctx)
		}
	}
}

// Cycle runs one sweeper pass: observe renewals for the guard, then sweep
// every epoch that ended or whose score is older than 2 × lease TTL, if the
// guard allows (ended epochs are exempt).
func (s *Sweeper) Cycle(ctx context.Context) {
	local := s.now()
	redisNow, err := s.st.RedisNowMs(ctx)
	var epochs []store.EpochScore
	if err == nil {
		epochs, err = s.st.ListEpochs(ctx)
	}
	if err != nil {
		s.guard.RedisFailed()
		s.m.GuardRenewingSeconds.Set(0)
		slog.Warn("sweeper: redis", "err", err)
		return
	}
	s.guard.Observe(local, redisNow, epochs)
	s.m.GuardRenewingSeconds.Set(s.guard.Renewing(local).Seconds())

	holding := false
	for _, e := range epochs {
		ended := e.ScoreMs == 0
		if !ended && redisNow-e.ScoreMs <= s.cfg.StaleAfter().Milliseconds() {
			continue
		}
		if !ended && !s.guard.Allows(local) {
			holding = true
			s.m.GuardHolds.Inc()
			continue
		}
		s.sweep(ctx, e.EpochRef)
	}
	if holding {
		s.m.GuardHolding.Set(1)
		slog.Warn("sweeper: mass-stale guard holds back stale epochs", "renewing", s.guard.Renewing(local))
	} else {
		s.m.GuardHolding.Set(0)
	}
}

// sweep claims ref, checks its lease key (authoritative, R4M-3), deletes its
// fields page by page and aborts as soon as the epoch renews (R3-O3).
func (s *Sweeper) sweep(ctx context.Context, ref store.EpochRef) {
	result := s.sweepOnce(ctx, ref)
	s.m.Sweeps.WithLabelValues(result).Inc()
	if result != "claimed_elsewhere" {
		slog.Info("sweeper: epoch", "gw", ref.Gateway, "epoch", ref.Epoch, "result", result)
	}
}

func (s *Sweeper) sweepOnce(ctx context.Context, ref store.EpochRef) string {
	ok, err := s.st.ClaimSweep(ctx, ref)
	if err != nil {
		return "error"
	}
	if !ok {
		return "claimed_elsewhere"
	}
	release := func(result string) string {
		if err := s.st.ReleaseSweep(ctx, ref); err != nil {
			slog.Warn("sweeper: release claim", "gw", ref.Gateway, "epoch", ref.Epoch, "err", err)
		}
		return result
	}
	claimed, ok, err := s.st.EpochScoreOf(ctx, ref) // the score at claim time (R3-O3)
	if err != nil {
		return release("error")
	}
	if !ok {
		return release("gone") // another node finished it
	}
	if live, err := s.alive(ctx, ref, claimed); err != nil {
		return release("error")
	} else if live {
		return release("lease_live") // its ZADD failed while its SET succeeded (R4M-3)
	}

	var cursor uint64
	for {
		uids, next, err := s.st.ScanConns(ctx, ref, cursor, int64(s.cfg.SweepBatch))
		if err != nil {
			return release("error")
		}
		for off := 0; off < len(uids); off += s.cfg.SweepBatch {
			part := uids[off:min(off+s.cfg.SweepBatch, len(uids))]
			if err := s.lim.WaitN(ctx, len(part)); err != nil {
				return release("error")
			}
			n, err := s.st.SweepUsers(ctx, ref, part)
			s.m.SweepUIDs.Add(float64(len(part)))
			s.m.SweepDeleted.Add(float64(n))
			if err != nil {
				return release("error")
			}
			// Abort on renewal, between batches: no UNLINK, no ZREM, and the
			// claim is deleted so it can't block a later sweep (R3-OP6). The
			// renewal reported missing, so the gateway's triggered pass restores.
			if live, err := s.alive(ctx, ref, claimed); err != nil {
				return release("error")
			} else if live {
				return release("aborted")
			}
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if err := s.st.FinishSweep(ctx, ref); err != nil {
		return release("error")
	}
	return "done"
}

// alive reports whether ref renewed since the claim: its score is newer than
// claimed, or its lease key exists.
func (s *Sweeper) alive(ctx context.Context, ref store.EpochRef, claimed int64) (bool, error) {
	score, ok, err := s.st.EpochScoreOf(ctx, ref)
	if err != nil {
		return false, err
	}
	if ok && score > claimed {
		return true, nil
	}
	exists, err := s.st.LeasesExist(ctx, []store.EpochRef{ref})
	if err != nil {
		return false, err
	}
	return exists[0], nil
}
```


- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/logic/ && go test -race -count=1 -run 'Guard|Sweep' -v ./internal/logic/`
Expected: four `TestGuard*` and six sweeper tests PASS.

Mutation check (expected to fail, then revert): in `config.go`, change `RecentRenewal` to return `c.RenewInterval` (a 10s window instead of 15s). `TestGuardSurvivesTenSecondRhythmWithJitter` must fail with `renewing_since was reset by the 10s rhythm`. Revert.

- [ ] **Step 5: Commit**

```bash
git add internal/logic
git commit -m "feat(logic): sweeper with mass-stale guard, lease-key check and abort on renewal"
```

---

### Task 13: Server, readiness, request IDs and `cmd/logic` (choice #18)

**Files:**
- Create: `internal/logic/server.go`, `cmd/logic/main.go`
- Test: `internal/logic/server_test.go`, `cmd/logic/main_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `type ServerConfig struct { GRPCAddr, LeaseAddr, AdminAddr string; Workers, LeaseWorkers int; MaxConnectionAge, MaxConnectionAgeGrace, ShutdownGrace time.Duration }`, `func DefaultServerConfig() ServerConfig`, `(ServerConfig).Validate(Config) error` (grace must exceed `login_deadline + 1s`)
  - `type Deps struct { Store Store; Keys interface{ Verifier; Ready() bool; Run(ctx) }; Dial DialFunc; Registry *prometheus.Registry; Metrics *Metrics }`
  - `type Server`, `func NewServer(cfg Config, sc ServerConfig, d Deps) (*Server, error)`, `(*Server).Start(ctx) error`, `Addrs() (grpc, lease, admin string)`, `Ready() bool`, `Stop()`
  - unexported `withRequestID(ctx, method) context.Context` (fills `logger(ctx)`), unary and stream interceptors
  - the `logic` command: flags for every `Config`/`ServerConfig` field that the test timing profile scales, plus `-redis`, `-redis-cluster`, `-jwks-url`, `-jwks-fallback`

Readiness is `/readyz`: 200 only when the servers are up and the key manager holds a key set, from either source. `/healthz` is always 200 while the process runs. On SIGTERM the pod goes NotReady, `GracefulStop`s both gRPC servers for up to 30s, closes the Kick client (queued Kicks are dropped; Reconcile is the backstop) and exits. A failed startup key load is logged and leaves the pod NotReady while `Run` keeps retrying both sources every 5s.

- [ ] **Step 1: Write the failing tests**

`internal/logic/server_test.go`:

```go
package logic

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func grpcConn(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return cc
}

// TestServerEndToEnd starts a whole Logic pod on local ports: it is NotReady
// until it holds a key set, then serves Login on the main port and
// GatewayLease on the lease port, and exports metrics.
func TestServerEndToEnd(t *testing.T) {
	cfg := testConfig()
	cfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis's gwepochs
	st, _ := newStore(t, cfg)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := auth.MarshalJWKS(key)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write(doc) }))
	defer jwks.Close()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	keys := auth.NewManager(auth.DefaultManagerConfig(jwks.URL, ""), m.AuthObserver())

	sc := DefaultServerConfig()
	sc.GRPCAddr, sc.LeaseAddr, sc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	srv, err := NewServer(cfg, sc, Deps{Store: st, Keys: keys, Dial: staticDial(nil), Registry: reg, Metrics: m})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	if err := srv.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); srv.Stop() }()
	grpcAddr, leaseAddr, adminAddr := srv.Addrs()

	if c := httpStatus(t, "http://"+adminAddr+"/readyz"); c != http.StatusServiceUnavailable {
		t.Fatalf("readyz without a key set: %d", c)
	}
	if c := httpStatus(t, "http://"+adminAddr+"/healthz"); c != http.StatusOK {
		t.Fatalf("healthz: %d", c)
	}
	if err := keys.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if c := httpStatus(t, "http://"+adminAddr+"/readyz"); c != http.StatusOK {
		t.Fatalf("readyz with a key set: %d", c)
	}

	gw := &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1 << 63}
	lease, err := pb.NewLeaseClient(grpcConn(t, leaseAddr)).GatewayLease(ctx, &pb.LeaseRequest{Gw: gw, GwNowMs: time.Now().UnixMilli()})
	if err != nil || !lease.Missing {
		t.Fatalf("first lease: %v %v", lease, err)
	}

	now := redisNow(t, st)
	uid := testredis.UID()
	tok, _ := auth.Sign(key, auth.Claims{UID: uid, AppID: "im", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3600_000})
	mdCtx := metadata.AppendToOutgoingContext(ctx, "x-request-id", "req-1")
	stream, err := pb.NewLogicClient(grpcConn(t, grpcAddr)).Login(mdCtx, &pb.LoginBatch{Gw: gw, Entries: []*pb.LoginEntry{{
		ConnId: 1, Token: tok, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE, DeviceId: "d", ConnSeq: 1, RecvMs: now}}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := stream.Recv()
	if err != nil || r.Code != pb.LoginCode_LOGIN_CODE_OK || r.Uid != uid {
		t.Fatalf("login: %+v %v", r, err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("stream end: %v", err)
	}

	resp, err := http.Get("http://" + adminAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`goim_logic_logins_total{result="ok"} 1`,
		`goim_logic_lease_renewals_total{result="missing"} 1`,
		`goim_logic_rpc_seconds_count{code="OK",method="/goim.internal.v1.Logic/Login"} 1`,
		`goim_logic_jwks_fallback 0`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics lack %s", want)
		}
	}
}

func TestServerConfigRefusesShortGrace(t *testing.T) {
	sc := DefaultServerConfig()
	sc.MaxConnectionAgeGrace = 6 * time.Second // = login_deadline + 1s
	if err := sc.Validate(DefaultConfig()); err == nil {
		t.Fatal("grace not above the Login timeout accepted")
	}
}

// Spec (Observability): log lines carry the request ID passed in gRPC metadata.
func TestRequestIDFromMetadata(t *testing.T) {
	var buf strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(old)

	in := metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", "abc"))
	logger(withRequestID(in, "/m")).Info("hello", "uid", "u1", "conn_id", 7)
	logger(withRequestID(ctx, "/m")).Info("generated")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "request_id=abc") || !strings.Contains(lines[0], "uid=u1") {
		t.Fatalf("log lines %q", lines)
	}
	if !strings.Contains(lines[1], "request_id=") || strings.Contains(lines[1], "request_id=abc") {
		t.Fatalf("no generated request id: %q", lines[1])
	}
	buf.Reset()
	long := metadata.NewIncomingContext(ctx, metadata.Pairs("x-request-id", strings.Repeat("x", 1000)))
	logger(withRequestID(long, "/m")).Info("long")
	if out := buf.String(); !strings.Contains(out, "request_id="+strings.Repeat("x", 64)+" ") {
		t.Fatalf("request id not capped at 64 bytes: %q", out)
	}
}
```


`cmd/logic/main_test.go`:

```go
package main

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultFlagsAreValid(t *testing.T) {
	cfg, sc, o, err := parseFlags([]string{"-jwks-url", "http://account/jwks"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Store.LeaseTTL != 30*time.Second || sc.MaxConnectionAgeGrace != 30*time.Second || !o.redisCluster {
		t.Fatalf("defaults: %+v %+v %+v", cfg, sc, o)
	}
}

// The test timing profile (Plan 5) scales intervals ~10× and keeps the ratios.
func TestScaledProfileFlags(t *testing.T) {
	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s",
		"-redis-failover", "1500ms", "-lease-cache", "100ms", "-sweep-interval", "500ms"})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRefusesBrokenRatio(t *testing.T) {
	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s", "-redis-failover", "2s"})
	if err == nil || !strings.Contains(err.Error(), "R2-O5") {
		t.Fatalf("lease TTL 3s with failover 2s + renew 1s: %v", err)
	}
	if err := run([]string{"-jwks-url", "x", "-max-conn-age-grace", "5s"}); err == nil {
		t.Fatal("run started with a grace below the Login timeout")
	}
}
```


- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/logic/ ./cmd/logic/`
Expected: FAIL to build: `undefined: DefaultServerConfig`, `undefined: NewServer`, `undefined: parseFlags`, ….

- [ ] **Step 3: Implement**

`internal/logic/server.go`:

```go
package logic

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerConfig is the process-level configuration of a Logic pod.
type ServerConfig struct {
	GRPCAddr  string // Logic + Revocation services (":9000")
	LeaseAddr string // Lease service, its own listener and pool (":9001", R1-O2)
	AdminAddr string // /metrics, /healthz, /readyz (":9090")

	Workers      int // concurrent Login/Logout/Reconcile calls (64)
	LeaseWorkers int // concurrent lease calls (16)

	// MaxConnectionAge spreads gateways over new Logic pods (≈5m); the grace
	// lets in-flight batches finish after GOAWAY and must exceed the Login
	// timeout, login_deadline + 1s (R1-O12).
	MaxConnectionAge      time.Duration
	MaxConnectionAgeGrace time.Duration
	ShutdownGrace         time.Duration // GracefulStop limit on SIGTERM (30s)
}

// DefaultServerConfig returns the spec defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{GRPCAddr: ":9000", LeaseAddr: ":9001", AdminAddr: ":9090",
		Workers: 64, LeaseWorkers: 16, MaxConnectionAge: 5 * time.Minute,
		MaxConnectionAgeGrace: 30 * time.Second, ShutdownGrace: 30 * time.Second}
}

// Validate checks sc against the service config.
func (sc ServerConfig) Validate(cfg Config) error {
	if sc.Workers <= 0 || sc.LeaseWorkers <= 0 {
		return fmt.Errorf("workers %d and lease workers %d must be positive", sc.Workers, sc.LeaseWorkers)
	}
	if min := cfg.Store.LoginDeadline + time.Second; sc.MaxConnectionAgeGrace <= min {
		return fmt.Errorf("MaxConnectionAgeGrace %v must exceed the Login timeout %v (R1-O12)", sc.MaxConnectionAgeGrace, min)
	}
	return nil
}

// Deps are the parts a Server is built from; tests pass fakes or in-process ones.
type Deps struct {
	Store Store
	Keys  interface {
		Verifier
		Ready() bool
		Run(ctx context.Context)
	}
	Dial     DialFunc
	Registry *prometheus.Registry // served on /metrics
	Metrics  *Metrics             // registered on Registry (NewMetrics)
}

// Server is one Logic pod: two gRPC servers, the admin HTTP server, the
// sweeper, the key refresher and the Kick client.
type Server struct {
	cfg  Config
	sc   ServerConfig
	deps Deps
	m    *Metrics

	svc    *Service
	lease  *LeaseService
	kicker *Kicker
	sweep  *Sweeper

	main, leaseSrv *grpc.Server
	admin          *http.Server
	lis            [3]net.Listener
	serving        atomic.Bool
	wg             sync.WaitGroup
}

// NewServer wires a Logic pod. Call Start, then Stop.
func NewServer(cfg Config, sc ServerConfig, d Deps) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := sc.Validate(cfg); err != nil {
		return nil, err
	}
	m := d.Metrics
	leases := NewLeaseCache(d.Store, cfg.LeaseCache, time.Now)
	kicker := NewKicker(cfg.Kick, leases, m, d.Dial)
	s := &Server{cfg: cfg, sc: sc, deps: d, m: m, kicker: kicker,
		svc:   NewService(cfg, d.Store, d.Keys, leases, kicker, m, sc.Workers),
		lease: NewLeaseService(d.Store, leases, m, sc.LeaseWorkers),
		sweep: NewSweeper(cfg, d.Store, m, time.Now)}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(s.unaryObserve),
		grpc.ChainStreamInterceptor(s.streamObserve),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
	s.main = grpc.NewServer(append(opts, grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionAge: sc.MaxConnectionAge, MaxConnectionAgeGrace: sc.MaxConnectionAgeGrace}))...)
	pb.RegisterLogicServer(s.main, s.svc)
	pb.RegisterRevocationServer(s.main, s.svc)
	// The lease server keeps its connections: no MaxConnectionAge on the
	// dedicated renewal path.
	s.leaseSrv = grpc.NewServer(opts...)
	pb.RegisterLeaseServer(s.leaseSrv, s.lease)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(d.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.Ready() {
			http.Error(w, "not ready: no key set or not serving", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	s.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	return s, nil
}

// Ready reports whether the pod should receive traffic: it holds a key set
// (spec: JWKS) and its servers are up.
func (s *Server) Ready() bool { return s.serving.Load() && s.deps.Keys.Ready() }

// Start binds the three listeners and starts serving and the background loops.
func (s *Server) Start(ctx context.Context) error {
	for i, addr := range []string{s.sc.GRPCAddr, s.sc.LeaseAddr, s.sc.AdminAddr} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			for _, open := range s.lis[:i] {
				open.Close()
			}
			return err
		}
		s.lis[i] = l
	}
	s.wg.Add(5)
	go func() { defer s.wg.Done(); s.main.Serve(s.lis[0]) }()
	go func() { defer s.wg.Done(); s.leaseSrv.Serve(s.lis[1]) }()
	go func() {
		defer s.wg.Done()
		if err := s.admin.Serve(s.lis[2]); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("admin server", "err", err)
		}
	}()
	go func() { defer s.wg.Done(); s.sweep.Run(ctx) }()
	go func() { defer s.wg.Done(); s.deps.Keys.Run(ctx) }()
	s.serving.Store(true)
	return nil
}

// Addrs returns the bound gRPC, lease and admin addresses.
func (s *Server) Addrs() (grpcAddr, leaseAddr, adminAddr string) {
	return s.lis[0].Addr().String(), s.lis[1].Addr().String(), s.lis[2].Addr().String()
}

// Stop goes NotReady, lets in-flight RPCs finish for up to ShutdownGrace, and
// stops everything. ctx passed to Start must be cancelled too.
func (s *Server) Stop() {
	s.serving.Store(false)
	done := make(chan struct{})
	go func() {
		s.main.GracefulStop()
		s.leaseSrv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(s.sc.ShutdownGrace):
		s.main.Stop()
		s.leaseSrv.Stop()
	}
	shut, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.admin.Shutdown(shut)
	s.kicker.Close()
	s.wg.Wait()
}

// ---- request IDs, logs and latency ----

// maxRequestID bounds a caller-supplied request ID in every log line.
const maxRequestID = 64

// withRequestID takes x-request-id from the incoming metadata (at most
// maxRequestID bytes), or makes one.
func withRequestID(ctx context.Context, method string) context.Context {
	id := ""
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-request-id"); len(v) > 0 {
			id = v[0][:min(len(v[0]), maxRequestID)]
		}
	}
	if id == "" {
		var b [8]byte
		rand.Read(b[:])
		id = hex.EncodeToString(b[:])
	}
	return context.WithValue(ctx, logKey{}, slog.Default().With("request_id", id, "method", method))
}

func (s *Server) observe(ctx context.Context, method string, start time.Time, err error) {
	code := status.Code(err)
	s.m.RPCSeconds.WithLabelValues(method, code.String()).Observe(time.Since(start).Seconds())
	if err != nil && code != codes.InvalidArgument && code != codes.Canceled {
		logger(ctx).Warn("rpc failed", "code", code.String(), "err", err)
	}
}

func (s *Server) unaryObserve(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	ctx = withRequestID(ctx, info.FullMethod)
	start := time.Now()
	resp, err := h(ctx, req)
	s.observe(ctx, info.FullMethod, start, err)
	return resp, err
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (c ctxStream) Context() context.Context { return c.ctx }

func (s *Server) streamObserve(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
	ctx := withRequestID(ss.Context(), info.FullMethod)
	start := time.Now()
	err := h(srv, ctxStream{ss, ctx})
	s.observe(ctx, info.FullMethod, start, err)
	return err
}
```


`cmd/logic/main.go`:

```go
// Command logic runs one Logic pod (spec: Logic): the gateway-facing RPCs,
// Revoke for the account service, the hardened Kick client and the sweeper.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("logic: exit", "err", err)
		os.Exit(1)
	}
}

// options are the flags that are not part of logic.Config.
type options struct {
	redisAddrs   string
	redisCluster bool
	jwksURL      string
	jwksFallback string
}

// parseFlags returns the validated configuration. Logic refuses to start on
// a configuration that breaks a required ratio (spec: Test execution model).
func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error) {
	cfg, sc, o := logic.DefaultConfig(), logic.DefaultServerConfig(), options{}
	fs := flag.NewFlagSet("logic", flag.ContinueOnError)
	fs.StringVar(&o.redisAddrs, "redis", "127.0.0.1:6379", "Redis address(es), comma-separated")
	fs.BoolVar(&o.redisCluster, "redis-cluster", true, "Redis Cluster (false: one node, for tests)")
	fs.StringVar(&o.jwksURL, "jwks-url", "", "account service JWKS URL")
	fs.StringVar(&o.jwksFallback, "jwks-fallback", "/etc/goim/jwks-fallback/jwks.json", "jwks-fallback ConfigMap file")

	fs.StringVar(&sc.GRPCAddr, "grpc-addr", sc.GRPCAddr, "Logic and Revocation gRPC listen address")
	fs.StringVar(&sc.LeaseAddr, "lease-addr", sc.LeaseAddr, "Lease gRPC listen address (dedicated, R1-O2)")
	fs.StringVar(&sc.AdminAddr, "admin-addr", sc.AdminAddr, "/metrics, /healthz, /readyz listen address")
	fs.IntVar(&sc.Workers, "workers", sc.Workers, "concurrent Login/Logout/Reconcile calls")
	fs.IntVar(&sc.LeaseWorkers, "lease-workers", sc.LeaseWorkers, "concurrent lease calls")
	fs.DurationVar(&sc.MaxConnectionAge, "max-conn-age", sc.MaxConnectionAge, "gRPC MaxConnectionAge")
	fs.DurationVar(&sc.MaxConnectionAgeGrace, "max-conn-age-grace", sc.MaxConnectionAgeGrace, "gRPC MaxConnectionAgeGrace")

	fs.DurationVar(&cfg.Store.LoginDeadline, "login-deadline", cfg.Store.LoginDeadline, "login_deadline (not scaled by the test profile)")
	fs.DurationVar(&cfg.Store.SessTTL, "sess-ttl", cfg.Store.SessTTL, "sess:{uid} and rev:{uid} TTL")
	fs.DurationVar(&cfg.Store.LeaseTTL, "lease-ttl", cfg.Store.LeaseTTL, "gateway lease TTL")
	fs.DurationVar(&cfg.Store.WaitTimeout, "wait-timeout", cfg.Store.WaitTimeout, "Revoke WAIT timeout")
	fs.DurationVar(&cfg.RenewInterval, "renew-interval", cfg.RenewInterval, "gateways' lease renewal interval")
	fs.DurationVar(&cfg.RedisFailover, "redis-failover", cfg.RedisFailover, "measured Redis shard failover time")
	fs.DurationVar(&cfg.LeaseCache, "lease-cache", cfg.LeaseCache, "lease observation cache")
	fs.DurationVar(&cfg.SweepInterval, "sweep-interval", cfg.SweepInterval, "sweeper cycle")
	fs.IntVar(&cfg.SweepRate, "sweep-rate", cfg.SweepRate, "sweeper uids/s")
	fs.IntVar(&cfg.DeleteRate, "delete-rate", cfg.DeleteRate, "ReconcileEnd delete phase uids/s")
	fs.DurationVar(&cfg.RevokeMaxAhead, "revoke-max-ahead", cfg.RevokeMaxAhead, "largest before_ms lead over Redis TIME")
	fs.IntVar(&cfg.Kick.Workers, "kick-workers", cfg.Kick.Workers, "Kick workers per target")
	fs.IntVar(&cfg.Kick.Queue, "kick-queue", cfg.Kick.Queue, "Kick queue per target")
	fs.DurationVar(&cfg.Kick.Deadline, "kick-deadline", cfg.Kick.Deadline, "Kick attempt deadline")
	fs.StringVar(&cfg.Kick.AddrFormat, "kick-addr-format", cfg.Kick.AddrFormat, "gateway pod DNS, %s = pod name")
	if err := fs.Parse(args); err != nil {
		return cfg, sc, o, err
	}
	if err := cfg.Validate(); err != nil {
		return cfg, sc, o, fmt.Errorf("config: %w", err)
	}
	if err := sc.Validate(cfg); err != nil {
		return cfg, sc, o, fmt.Errorf("config: %w", err)
	}
	if o.jwksURL == "" && o.jwksFallback == "" {
		return cfg, sc, o, errors.New("config: -jwks-url or -jwks-fallback is required")
	}
	return cfg, sc, o, nil
}

func run(args []string) error {
	cfg, sc, o, err := parseFlags(args)
	if err != nil {
		return err
	}
	addrs := strings.Split(o.redisAddrs, ",")
	var rdb redis.UniversalClient
	if o.redisCluster {
		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: addrs[0]})
	}
	defer rdb.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	m := logic.NewMetrics(reg)
	keys := auth.NewManager(auth.DefaultManagerConfig(o.jwksURL, o.jwksFallback), m.AuthObserver())
	srv, err := logic.NewServer(cfg, sc, logic.Deps{Store: store.New(rdb, cfg.Store), Keys: keys,
		Dial: logic.PodDNSDial(cfg.Kick.AddrFormat), Registry: reg, Metrics: m})
	if err != nil {
		return err
	}
	// Not Ready until a key set is held; Run keeps retrying both sources.
	if err := keys.Load(ctx); err != nil {
		slog.Error("logic: no key set yet; staying NotReady", "err", err)
	}
	if err := srv.Start(ctx); err != nil {
		return err
	}
	g, l, a := srv.Addrs()
	slog.Info("logic: serving", "grpc", g, "lease", l, "admin", a, "fallback_keys", keys.OnFallback())
	<-ctx.Done()
	slog.Info("logic: shutting down")
	srv.Stop()
	return nil
}
```


- [ ] **Step 4: Run every test**

Run: `gofmt -l . && go vet ./... && make test`
Expected: no gofmt output; every package `ok`. The only skip is `TestRevokeEndToEndOnRealRedis`, plus Plan 1's Tier 2 tests. Run `make test` twice more: the suite must stay green (Execution Notes: two flaky tests were found this way while prototyping).

- [ ] **Step 5: Check that the binary refuses a broken ratio**

```bash
go build -o /tmp/logic ./cmd/logic
/tmp/logic -jwks-url x -lease-ttl 20s; echo "exit $?"
```

Expected: `ERROR logic: exit err="config: lease TTL 20s must exceed failover 15s + renew interval 10s (R2-O5)"`, then `exit 1`. It fails before dialling Redis, so this works on the dev machine.

- [ ] **Step 6: Commit**

```bash
git add internal/logic cmd/logic
git commit -m "feat(logic): server with lease port, readiness, request IDs; cmd/logic"
```

---

## After this plan

Logic is complete against the store, and runs alone with `cmd/logic` plus `cmd/devtoken serve`. It has no gateway yet: Plan 3 builds the gateway core against `api/internal.proto` and the Logic contract in *Execution Notes*.

The roadmap already lists this plan, its proposed choices #15–#23 and its test names in the invariant index. When the plan is approved, record the decisions on #15–#23 there and update Plan 2's status.

The open risks are listed in *Execution Notes*.
