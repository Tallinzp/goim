# Gateway Core Implementation Plan — Plan 3 of 6

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the transport-independent gateway core: the client protocol messages, per-loop connection tables with `conn_id` generation and timing wheels, the connection state machine on a fake transport, admission control (connection cap, pending cap, Login-queue cap), Login batching with streamed per-entry results, the Logout fence (a)(b)(c) with its retry queue, the Kick server, the lease loop, the Reconcile pass controller, readiness with the limits self-check, graceful drain, and the gateway's metrics.

**Architecture:** One package, `internal/gateway`. A transport (TCP + Noise on gnet in Plan 4, wss later) owns sockets and event loops and calls the core on the connection's loop goroutine: `Open`, `HandshakeDone`, `Frame(cmd, req_id, body)`, `Closed`. The core reaches a loop only through `Loop.Execute`, so every table, wheel and connection field is touched by its own loop and needs no lock. Background goroutines (Login and Logout batchers, the wheel ticker, the lease loop, the Reconcile pass controller, the Kick gRPC server) talk to loops through `Execute` and wait for replies on channels; they never assume `Execute` keeps order. The core talks to Logic over two gRPC connections (`Logic` and the dedicated `Lease`), using the contract Plan 2 fixed. It never imports `internal/store`.

**Tech Stack:** Go 1.27, `google.golang.org/grpc v1.84.0`, `google.golang.org/protobuf v1.36.12`, `protoc` 33.1 with `protoc-gen-go v1.36.12` and `protoc-gen-go-grpc v1.6.2` (Plan 2, choice #15), `github.com/prometheus/client_golang v1.24.1`. No new module dependencies. Tests use real gRPC on localhost against a fake Logic, and, in one end-to-end file, Plan 2's real Logic on miniredis.

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative), sections *Gateway* (all but the TCP transport), *Timers*, *Client protocol* (message bodies only), *Admission control*, *Logout fence*, *Lease*, *Reconcile* (gateway side), *Readiness*, *Graceful drain*, *Observability* (gateway side), *Verification → Unit tests* (gateway items). Roadmap, implementation choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. The Logic contract this plan builds against: `docs/superpowers/plans/2026-10-08-connection-session-core-02-logic.md`, *Execution Notes → For Plan 3*.

## Global Constraints

- Module path `github.com/Tallinzp/goim`. Layer rule: "The gateway never touches Redis. Only Logic touches Redis." `internal/gateway` must not depend on `internal/store` or `github.com/redis/...` (Task 5 pins it with `go list -deps`). Its test files may import Logic for the end-to-end test.
- gnet is not used in this plan; the core must work with gnet v2.10.0's `EventLoop.Execute` (`Execute(ctx, Runnable) error`, concurrency-safe, unbounded queue, fails only after engine shutdown). **`Execute` does not keep order**: in `pkg/netpoll/poller_epoll_default.go`, `Trigger` puts a low-priority task in `asyncTaskQueue` once `urgentAsyncTaskQueue` holds `MaxPollEventsCap` tasks, and the loop drains the urgent queue first, so a later task can run first. No code may rely on the order of two `Execute` calls.
- "`OnTraffic` never blocks; RPCs go through a bounded worker pool." Core methods called on a loop never block and never do I/O other than `Link.Send` / `Link.Close`.
- "Connection states: `HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`."
- Identity: "`gateway_id` = pod name; `epoch` = random uint64 chosen at boot; `conn_id = (worker_id << 48) | loopState.counter++`." "loopStates are built at boot, only the binding is lazy"; "A loop seen beyond `numLoops` is a startup bug: the gateway panics." The counter is unexported and only `Open` touches it.
- Pre-allocation: "`make(map[uint64]*conn, maxConnsPerGateway / numLoops × 1.5)`".
- Kick handling: "`ONLINE`: send the Kick frame and close. `AUTH_PENDING` (Login in flight): set a kick-pending mark … When the Login reply arrives, send the Kick frame and close instead of moving to `ONLINE`. `HANDSHAKE`, `CLOSED`, unknown conn_id, or epoch mismatch: ignore."
- Heartbeat: "**150s by default, set by the server in `LoginResp`**. Any frame resets the timer (lazily) … dead after 2 × interval + 10s (= 310s)". Timeouts: `HANDSHAKE` and `AUTH_PENDING` "default **5s**, allowed range **3–30s**. A gateway refuses to start if either is out of range."; "The `AUTH_PENDING` timer stops when LoginReq arrives."
- Timers: "**1s tick, single-level wheel** of 512 slots"; "**Never fire early**: deadlines round **up** to the next tick"; "One wheel per loop … A single ticker goroutine posts `loop.Execute(advance)` to every loop once per tick. gnet's `OnTick` is not used".
- Admission: connection cap "counted from accept to close, in every state (configurable, default **250k**) … Closed entries kept in the table only for a pending Logout do not count"; Login-queue cap: "At the cap, reply `Busy{retry_after}` and close"; "`Busy{retry_after}` uses a short value, **1–3s, uniformly distributed**"; "One login attempt per connection (a failure closes it)"; "Queued Logins older than `login_deadline` (measured from `recv_ms`) are dropped before being sent; they never reach Logic, so they need no Logout."
- Login batching: "up to 200 logins or 5ms per RPC"; "The Login timeout is at least `login_deadline + 1s` (= 6s by default), measured from `recv_ms`, and applies **per entry**. An entry with no result when its timeout passes, or when the stream ends or fails, has no definitive reply and falls under fence (b)."
- Logout fence: (a) definitive Login reply, or (b) `recv_ms + login_deadline + 1s` passed; and (c) "no Reconcile chunk containing the connection is unresolved". Client Logout: "the gateway replies `LogoutResp`, closes, and sends the Logout RPC through the fenced path." "Logouts go through a bounded retry queue (60s)".
- Lease: "every 10s on a **dedicated gRPC connection**"; "On `missing`, the gateway starts an immediate triggered Reconcile pass. The first lease call after boot always finds the key absent … the gateway ignores `missing` on that call, and neither counts nor acts on it"; each call carries `gw_now_ms`.
- Reconcile pass mechanics, verbatim rules: key snapshot of ONLINE conn_ids per loop; "the controller draws up to N IDs from **one** loop's partition, taking turns across loops round-robin"; "Re-queued entries count against the pass's rate limit"; "for each `refused` entry, if `requeues < 3` push `(conn_id, requeues + 1)` back"; "at pass end the controller first stamps `as_of_ms`, **then** posts one `Execute` per loop to collect that loop's tracked conn_ids"; "A cancelled pass builds and sends no further chunks and sends no `ReconcileEnd`. The triggered pass starts once the cancelled pass's in-flight chunks are resolved"; `ReconcileEnd` failures are retried with the same payload for 60s, then dropped; its deadline covers Logic's delete phase.
- Two paces: periodic "every ~10 min with random jitter per gateway, spread over the cycle (~330 entries/s at 200k)"; triggered "~5k entries/s per gateway".
- Readiness: Ready "only after the gRPC connections to Logic … are connected, the Noise static key set is loaded and the first `GatewayLease` call has succeeded"; limits self-check: "`RLIMIT_NOFILE` ≥ cap + headroom (default headroom 10%)", "`nf_conntrack_max` ≥ 2 × cap, **or** … `conntrack: notrack`", "an absent file means conntrack isn't loaded and passes".
- Drain: "on SIGTERM, go NotReady and stop accepting, send `Reconnect{jitter_ms}` (default `jitter_ms` = 5s, configurable) in batches over about 60s, close the rest, then call `EpochEnded`." "Kicks still reach a draining gateway".
- Logic contract (Plan 2 Execution Notes): read the Login stream to EOF; an entry with no result at EOF, a stream error, or `OUTCOME_UNKNOWN` has no definitive reply; retry Logout entries whose `done` is false; a Reconcile RPC error means the chunk is resolved at `sent_ms + login_deadline + 1s`; dial `Lease` on its own connection; send `x-request-id` (≤ 64 bytes); client keepalive no more often than every 10s; Kick server on port 9100, ignoring epoch mismatches and unknown `conn_id`s.
- Test timing profile: every interval is configurable; `login_deadline` and the 1s skew allowance are not scaled.
- Tiers: Tier 1a on the dev machine (2 cores / 2GB), `go test -race`, no Docker. Roadmap choices #1–#23 apply; this plan's are #24–#37 below.

## Review Focus

1. **A connection closes in the instant its queued Login is being sent.** Expected: exactly one side owns the outcome. If the loop cancels first, nothing reaches Logic and nothing is tracked; if the worker sends first, the Logout waits for the reply or fence (b). Pinned in Task 5 (`TestCloseWhileQueuedNeedsNoLogout`) and Task 6 (`TestQueuedLoginPastTheDeadlineIsDroppedBeforeSending`), on the `itemQueued` compare-and-swap both sides use.
2. **A Kick that reaches the gateway before the Login reply that makes the connection ONLINE** (Logic's Kick to a replaced connection racing its own Login result, race 8). Expected: the connection gets the Kick frame instead of `LoginResp`. Pinned in Task 8 (`TestKickDuringAuthPendingIsAppliedWhenTheLoginReplyArrives`).
3. **A Reconcile chunk whose RPC fails at once** (connection refused, Logic crash before reading it). Expected: its connections' Logouts still wait until `sent_ms + login_deadline + 1s`, not the moment of the error, because the chunk may be in Logic's pipeline. Pinned in Task 9 (`TestFailedChunkIsResolvedAtSentMsPlusDeadlinePlusOneSecond`).
4. **Lease replies lost while the lease key expired and was swept**, followed by a renewal that reports `live` (it re-created the key). Expected: a triggered pass anyway, since a lost reply may have hidden the `missing`. Pinned in Task 10 (`TestRenewalAfterFailuresForALeaseTTLTriggersAPass`).
5. **One loop can't be read at pass end** (shut down, or a future transport bug). Expected: no `ReconcileEnd`, because an incomplete tracked set would delete the sessions of that loop's live connections. Pinned in Task 9 (`TestIncompleteTrackedSetSendsNoReconcileEnd`).

## Implementation choices this plan makes (decided 2026-10-08)

The spec leaves these open. Each is restated in the task that implements it. They are recorded in the roadmap as #24–#37, **decided by the user 2026-10-08, all as proposed**. On #34 the user kept the latch knowing that a partition of one gateway from Logic leaves that pod Ready and answering `Busy`.

24. **`api/client.proto` moves from Plan 4 to Plan 3.** The core decodes `LoginReq` and encodes `LoginResp`, `Error`, `Busy`, `Kick`, `Reconnect`; only the 6-byte header codec and Noise stay in Plan 4. `Platform` and `KickReason` are duplicated in the client package (an app-facing proto should not import the internal one) with the same numbers and names, pinned by a parity test. `ReconnectReason` adds `LOCAL_TRIGGER` (network change, app foreground) to the spec's list. `Error` codes are the spec's five plus Plan 2's `TOKEN_INVALID` and `BAD_REQUEST`.
25. **Transport seam.** `Loop{Execute(func()) error}` (Plan 4 wraps gnet's `EventLoop.Execute` with a `RunnableFunc`), `Link{Send(cmd, reqID, body); Close()}`, and four core methods called on the loop: `Open(loop, link) (*Conn, reject)`, `HandshakeDone`, `Frame`, `Closed`. `Open` returns the reject reason (already counted) and the transport closes the socket; after `Close` the transport calls `Closed` exactly once. New reject reason `draining`. No code relies on the order of two `Execute` calls (see *Global Constraints*): every multi-step exchange waits for a reply on a channel.
26. **Which Logins need a Logout.** A Login rejected by Logic wrote nothing: no Logout, untracked at close. A Login cancelled before it was sent (the loop wins the `itemQueued` compare-and-swap) never reached Logic: no Logout. A Login dropped at send time for being older than `login_deadline` gets `Error{EXPIRED}` (metric label `expired_queued`, apart from Logic's `expired`) and needs no Logout.
27. **No definitive reply.** The client gets `Busy{retry_after}` (1–3s) and the connection closes; the Logout waits for fence (b). Its uid comes from the token's **unverified** `sub` claim, since only an OK reply carries Logic's verified uid. A forged `sub` only makes that compare-and-delete miss; a token whose `sub` can't be read was rejected by Logic, so it needs no Logout.
28. **Lease trigger rule.** A pass is triggered by `missing`, and also by any successful renewal that ends at least one lease TTL after the start of the previous successful one: during that gap the key may have expired and been swept, and a lost reply may have hidden the `missing` of the call that re-created it. Errors alone trigger nothing (no passes against a Logic that is down). The gateway gets `LeaseTTL` in its config; it must equal Logic's `-lease-ttl`. The first successful call after boot neither counts nor acts.
29. **Triggers during a pass.** During a periodic pass: cancel it, then run a triggered pass (spec). During a triggered pass: run one more triggered pass after it ends, because the sweep that caused the trigger may have deleted entries this pass had already processed. Triggers coalesce (a channel of one).
30. **Pass pacing and chunks.** Chunk 200 entries; at most 4 chunks in flight; triggered pace 5k entries/s; periodic pace = ONLINE count ÷ (0.9 × cycle), at least 10/s and at most the triggered pace (~370/s at 200k over 10 min). Pacing is a send schedule (`due += n / rate`), so re-queued entries count against it. `sent_ms` is stamped on the loop when the chunk is built, and the chunk's deadline (`sent_ms + login_deadline + 1s`) is a wheel timer on that loop. A Reconcile RPC error does nothing: the wheel resolves the chunk. A reply that arrives after the deadline still Kicks its `not_owner` connections and does nothing else. Refused entries are re-queued only after the loop reports the chunk resolved. The first periodic pass starts at a random point of the first cycle; later ones every cycle ± 1 min.
31. **`ReconcileEnd`.** Deadline = 2 × tracked ÷ `DeleteRate` + 10s, with `DeleteRate` (10k/s) mirroring Logic's `-delete-rate`; retries start at 1s, double, are capped at 10s and stop after 60s. If any loop's tracked fragment can't be collected, the pass sends no `ReconcileEnd`.
32. **Logout sender.** Batches of 200 or 5ms, 4 concurrent RPCs, 5s per RPC. Entries not done are retried per batch after 1s, doubling, capped at 10s, while their 60s budget lasts; then dropped (counted) and untracked. The queue holds `LogoutQueue` entries (default = the 250k cap); when it is full a Logout is dropped at once. A connection stays tracked until its Logout is acknowledged or dropped.
33. **Login path sizes.** 16 concurrent Login RPCs. `LoginQueueCap` default 8192, to be re-derived in Tier 2 (Plan 6) by the spec's rule `≤ measured Logic login throughput per gateway × (login_deadline − margin)`. `PendingCap` default 100k (a 20k/s storm with 5s timeouts peaks there).
34. **Readiness latches and process wiring.** Ready requires: not draining; limits pass; the Logic channel was `READY` once; the first `GatewayLease` succeeded; the transport's checks pass. Once Ready, a later Logic outage does not make the gateway NotReady (every gateway would see it alike, and clients could only churn). Kick gRPC server on `:9100`, admin HTTP (`/metrics`, `/healthz`, `/readyz`) on `:9090`. Both Logic channels: plaintext, `round_robin`, keepalive every 20s with a 5s timeout.
35. **Drain mechanics.** At SIGTERM: NotReady and `Open` rejects `draining`; the Reconcile controller stops; `Reconnect{jitter_ms}` goes to the ONLINE connections in 1s steps over `Drain.Window` (60s); a connection that comes ONLINE during the drain gets `Reconnect` right after `LoginResp`. At the end every remaining connection is closed, then the drain waits until nothing is tracked, at most `login_deadline + 1s + Drain.Flush` (10s), so no Login or Reconcile write lands after the sweeper deletes the epoch; then `EpochEnded`, up to 3 attempts. The whole drain takes about 77s, so `terminationGracePeriodSeconds` must be ≥ 90s (Plan 6 sets it).
36. **Metrics** are `goim_gateway_*` (Task 2). Labels beyond the spec's list: `closes_total{reason}` for closes the gateway makes (`client` counts closes the client made); `logins_total{result}` includes `busy` (Login-queue cap), `expired_queued` (choice #26) and `error` (no definitive reply); `admission_rejects_total` adds `draining`. Reconnect-after-idle is read from `relogin_seconds_count{reconnect_reason}`; handshakes/s and the Go runtime collectors come with Plan 4's `cmd/gateway`.
37. **Request IDs** are `<epoch in 16 hex digits>-<sequence>`, at most 37 bytes, in `x-request-id` on every call to Logic.

## Execution Notes

- **Branch:** Plan 2 (`feat/plan2-logic`) is not merged yet, so Plan 3 branches from it: `git checkout -b feat/plan3-gateway feat/plan2-logic`. Commit this plan before Task 1, so the branch diff holds only Plan 3's code.
- **Prototype:** every code block in this plan was compiled and run before the plan was written. The whole plan was built once in a scratch copy of `59f7a81`, then replayed task by task into a second copy, one commit per task, with `go vet ./...` and `go test -race -count=1 ./internal/gateway` passing after each task; the replay's final tree matched the first copy byte for byte. The full suite (`./...`) passed under `-race` three times, and `./internal/gateway` six more times in a row, on the dev machine. One flaky test was found that way: four chunks in flight reach the fake Logic in any order, so the round-robin test now runs with one chunk in flight.
- **Mutation check:** each of these breakages was made in the prototype and caught by the named test: fence (a)/(b) ignored (`TestFenceA…`, `TestFenceB…`); fence (c) ignored (`TestFenceC…`, `TestClientLogoutDuringAChunk…`); chunk deadline at `sent_ms` (`TestFailedChunk…`); first lease `missing` acted on (`TestFirstLease…`); the lease-TTL gap rule removed (`TestRenewalAfterFailures…`); Kick epoch check removed (`TestKickForAnotherEpoch…`); wheel without round-up (`TestWheelNeverFiresEarly`); no `login_deadline` drop at send (`TestQueuedLoginPast…`); no re-queue limit (`TestRefused…`); a trigger that doesn't cancel a periodic pass (`TestTriggerCancels…`); a cancelled queued Login treated as sent (`TestCloseWhileQueued…`).
- **Execution mode:** Native (decided 2026-10-08), as for Plans 1 and 2.
- **Tests and timing.** The core tests (`newCore`) drive the wheels by hand with a clock that is real time plus an offset. The started tests (`newLive`) run every goroutine with intervals of 20–300ms; the gateway package takes ~12s under `-race` on the dev machine. The wheel ticker runs every real second in started tests, so tests that wait for fence (b) or a chunk deadline take 1–2s.
- **Not verified at Tier 1a, and where each is closed:**
  1. **`Execute` reordering.** The fake loops keep order; gnet doesn't. The design never relies on order (*Global Constraints*), but no test runs the core on a reordering loop. Plan 4's gnet tests run the core on real loops under load.
  2. **The tracked-set "after `as_of_ms`" rule** holds by construction (stamp, then post the collection `Execute`s, then read the bound loops). `TestReconcileEndCarriesTheTrackedSetCollectedAfterAsOf` checks the set's contents, not that order. Race 16 is exercised end to end in Plan 5.
  3. **gnet integration:** `Conn.EventLoop()` identity as the `sync.Map` key, `OnClose` calling `Closed` exactly once, and closing a socket from `Execute`. Plan 4 (loop-ownership `-race` test).
  4. **Stop accepting during a drain.** Here `Open` rejects with `draining`; the NLB stops sending once NotReady. Whether Plan 4 also closes the listener is decided there.
  5. **Real Redis and Redis Cluster.** The end-to-end test uses miniredis at Tier 1a; at Tier 2 (`make test-redis`) it runs on the real Redis with a unique gateway id and uid. Cluster behaviour is Plan 1's open risk.
  6. **Scale.** Pre-allocation is checked at 64k/4 loops; memory per connection, `ReconcileEnd` at 250k IDs (~2MB, under gRPC's 4MB default) and pass timings at 200k are Plan 6's load tests (a) and the `gwconns` shard test.
  7. **`ReconcileEnd` longer than Logic's 30s grace** (Plan 2 deferred minor, ~280k members at 10k/s): the gateway's deadline is longer than that, so a cut call fails and is retried; sized in Plan 6.
- **Implemented 2026-10-08** on `feat/plan3-gateway` (Native, 12 task commits `d358d0a`…`5294a48`). One test deviation: `TestFirstOpenOnALoopAllocatesNoTable` flaked once (5,640 B against a 1 KiB bar, because `TotalAlloc` counts the whole process). The bar is now 64 KiB; a table built on first use is ~590 KB and is still caught (`d73d423`).
- **Final review** (a fresh Opus reviewer, verdict "with fixes"). Fixed, each with a test that failed first:
  - (Important) `Drain` left the lease loop running, so a renewal after `EpochEnded` re-created the key and reset the `gwepochs` score. `Drain` now stops the lease loop and waits for its last call before `EpochEnded`. Test: `TestNoLeaseRenewalAfterEpochEnded`, commit `a1597a5`. Open risk: a renewal that timed out at the gateway but still runs in Logic after `EpochEnded`.
  - (re-graded Minor → Important) A pending Kick was replaced by `Busy` when the Login had no definitive reply, so the client's retry evicted the device that had replaced it. The pending Kick now wins. Test: `TestKickPendingWinsOverNoDefinitiveReply`, commit `23443f8`.
- **Deferred minors:**
  - The comment in `loginFrame` wrongly calls the channel-full branch unreachable.
  - `Stop` doesn't wait for Reconcile `call` goroutines or the `retryLogouts` timers.
  - `Stop` before or after a failed `Start` panics, and so does `Drain` before `Start` (fix with Plan 4's `cmd/gateway`).
  - `TestRenewalAfterFailuresForALeaseTTLTriggersAPass` has a thin timing margin under `-race`.
  - Plan 2's *Execution Notes* still say a lease error counts as `missing`; choice #28 replaced that rule.
- **For Plan 4 (TCP transport), the seam this plan fixes:**
  - Build `NumLoops` from gnet's `NumEventLoop` and use `gnet.WithLoadBalancing(gnet.RoundRobin)`.
  - In `OnOpen`: `conn, reason := g.Open(loopOf(c.EventLoop()), link)`; on a reason, close before any crypto. The `Loop` passed must be the same comparable value every time for the same loop (e.g. a struct wrapping the `EventLoop`).
  - Call `HandshakeDone` after Noise completes, `Frame` for every decrypted frame, and `Closed` from `OnClose` (once). `Link.Send` and `Link.Close` are called only on the loop.
  - Pass the Noise key set check as a `Deps.ReadyChecks` entry.
  - `cmd/gateway`: flags into `Config` and `ServerConfig`, `GatewayID` from the pod name, `Epoch` from `NewEpoch()`, SIGTERM → `Drain` then `Stop`.

---

## File Structure

| File | Responsibility |
|---|---|
| `api/client.proto` | Client protocol bodies and the `Cmd` numbers (choice #24) |
| `api/clientpb/client.pb.go` | Generated by `make proto`, committed |
| `api/clientpb/parity_test.go` | `Platform`, `KickReason` and rejection codes match `internal.proto` |
| `internal/gateway/config.go` | `Config`, defaults, `NewEpoch`, `Validate` (3–30s timeouts, ratio checks) |
| `internal/gateway/metrics.go` | every gateway metric, reject reasons |
| `internal/gateway/wheel.go` | per-loop timing wheel, intrusive timers |
| `internal/gateway/loops.go` | `Loop`, `loopState`, lazy binding, `conn_id`, table pre-sizing |
| `internal/gateway/conn.go` | `Link`, `Conn`, states, Login outcomes, `loginItem` |
| `internal/gateway/gateway.go` | `Gateway`, `Deps`, `New`, `Open`/`HandshakeDone`/`Frame`/`Closed`, timeouts, heartbeat, Logout fence `settle` |
| `internal/gateway/token.go` | unverified `sub` claim for a Logout after no definitive reply |
| `internal/gateway/login.go` | LoginReq admission, replies to the client, batcher, Login RPC with per-entry results |
| `internal/gateway/logout.go` | Logout queue, batcher, RPC, retry queue, untracking |
| `internal/gateway/kick.go` | `Gateway.Kick` server |
| `internal/gateway/reconcile.go` | pass controller, chunk build/resolve on loops, `ReconcileEnd` |
| `internal/gateway/lease.go` | lease loop and trigger rule |
| `internal/gateway/limits.go` | `RLIMIT_NOFILE` and `nf_conntrack_max` self-check |
| `internal/gateway/server.go` | `ServerConfig`, `Start`, `Stop`, `Ready`, gRPC clients, admin HTTP, wheel ticker, request IDs |
| `internal/gateway/drain.go` | `Drain`, `EpochEnded` |

Shared test helpers: `config_test.go` (`testConfig`, Task 2), `wheel_test.go` (`t0`, Task 3), `loops_test.go` (`stubLoop`, Task 4), `harness_test.go` (fake loops and links, the `core` harness, `waitFor`, Task 5), `logic_test.go` (fake Logic, the `live` harness, Task 6), `login_test.go` (`shortDeadline`, Task 6), `kick_test.go` (`kick`, Task 8), `reconcile_test.go` (`passes`, `blockChunks`, Task 9).

---

### Task 1: Client protocol messages (choice #24)

**Files:**
- Create: `api/client.proto`, `api/clientpb/client.pb.go` (generated), `api/clientpb/parity_test.go`
- Modify: `Makefile` (the `proto` target), `api/internal.proto` (header comment), `api/internalpb/*.pb.go` (regenerated: the header comment only)

**Interfaces:**
- Consumes: `api/internalpb` enums `Platform`, `KickReason`, `LoginCode` (Plan 2).
- Produces: package `clientpb` with `Cmd` (`CMD_HANDSHAKE` … `CMD_BUSY`), `LoginReq`, `LoginResp`, `Window`, `Error`/`ErrorCode`, `Kick`/`KickReason`, `Reconnect`, `Busy`, `Platform`, `ReconnectReason`.

- [ ] **Step 1: Write the failing test**

`api/clientpb/parity_test.go`:

```go
package clientpb_test

import (
	"strings"
	"testing"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// The gateway converts Platform and KickReason between the two protocols by
// number, so both enums must keep the same numbers and names.
func TestEnumsMatchTheInternalProtocol(t *testing.T) {
	for n, name := range pb.Platform_name {
		if got := clientpb.Platform_name[n]; got != name {
			t.Errorf("Platform %d: client %q, internal %q", n, got, name)
		}
	}
	for n, name := range pb.KickReason_name {
		if got := clientpb.KickReason_name[n]; got != name {
			t.Errorf("KickReason %d: client %q, internal %q", n, got, name)
		}
	}
	if len(pb.Platform_name) != len(clientpb.Platform_name) || len(pb.KickReason_name) != len(clientpb.KickReason_name) {
		t.Error("the enums have different sizes")
	}
}

// Every rejection code Logic sends has a client ErrorCode of the same name.
func TestEveryLoginRejectionHasAClientErrorCode(t *testing.T) {
	for _, name := range pb.LoginCode_name {
		code := strings.TrimPrefix(name, "LOGIN_CODE_")
		switch code {
		case "UNSPECIFIED", "OK", "OUTCOME_UNKNOWN":
			continue
		}
		if _, ok := clientpb.ErrorCode_value["ERROR_CODE_"+code]; !ok {
			t.Errorf("no client ErrorCode for %s", name)
		}
	}
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./api/clientpb/`
Expected: FAIL, `no non-test Go files` / `cannot find package … clientpb` (the package does not exist yet).

- [ ] **Step 3: Write the proto and regenerate**

`api/client.proto`:

```protobuf
// Client protocol bodies (spec: Client protocol). Every frame is a 6-byte
// header (Version, Cmd, Flags, Length) and a payload; the frame codec and the
// Noise handshake are the TCP transport's (Plan 4). The gateway core sees
// only decoded frames (cmd, req_id, body), where body is one of the messages
// below, protobuf-encoded.
syntax = "proto3";

package goim.client.v1;

option go_package = "github.com/Tallinzp/goim/api/clientpb";

// Cmd is the header's 2-byte command. HANDSHAKE is the TCP transport's only.
enum Cmd {
  CMD_UNSPECIFIED = 0;
  CMD_HANDSHAKE = 1;   // key_id(2) || noise_msg, no req_id, no AEAD
  CMD_LOGIN = 2;       // client: LoginReq
  CMD_LOGIN_RESP = 3;  // server: LoginResp
  CMD_PING = 4;        // client: empty body
  CMD_PONG = 5;        // server: empty body, echoes the Ping's req_id
  CMD_LOGOUT = 6;      // client: empty body
  CMD_LOGOUT_RESP = 7; // server: the gateway accepted the logout (R4H-O8)
  CMD_KICK = 8;        // server: Kick
  CMD_RECONNECT = 9;   // server: Reconnect
  CMD_ERROR = 10;      // server: Error (a rejected Login)
  CMD_BUSY = 11;       // server: Busy
}

// Platform is the device class of a slot; numbered as in internal.proto.
enum Platform {
  PLATFORM_UNSPECIFIED = 0;
  PLATFORM_MOBILE = 1;
  PLATFORM_PAD = 2;
  PLATFORM_WEB = 3;
  PLATFORM_PC = 4;
}

// ReconnectReason says why the client is connecting (R1-O7).
enum ReconnectReason {
  RECONNECT_REASON_UNSPECIFIED = 0;
  RECONNECT_REASON_FIRST_CONNECT = 1;
  RECONNECT_REASON_RST_FIN = 2;
  RECONNECT_REASON_PONG_TIMEOUT = 3;
  // A retryable reject, including a close before the handshake completed (R3-O5).
  RECONNECT_REASON_BUSY_RETRY = 4;
  RECONNECT_REASON_DRAIN = 5;         // after Reconnect{jitter_ms} (R4H-O3)
  RECONNECT_REASON_LOCAL_TRIGGER = 6; // network change or app foreground
}

message LoginReq {
  string token = 1;
  string appid = 2;
  Platform platform = 3;
  string device_id = 4;
  uint64 conn_seq = 5;
  ReconnectReason reconnect_reason = 6;
  int64 offline_ms = 7; // ms since the client detected the loss (monotonic)
}

// Window is a uniform random wait [lo_ms, hi_ms] (R5S-O6).
message Window {
  uint32 lo_ms = 1;
  uint32 hi_ms = 2;
}

message LoginResp {
  uint32 heartbeat_s = 1;
  // Reconnect windows; an absent one keeps the SDK's current value (R5S-O6).
  Window storm_wait = 2;
  Window local_trigger = 3;
  Window pre_handshake_retry = 4;
  Window retryable = 5;
}

// ErrorCode is a rejected Login (spec: Login errors; roadmap choice #17).
enum ErrorCode {
  ERROR_CODE_UNSPECIFIED = 0;
  ERROR_CODE_STALE = 1;         // stored_conn_seq is set
  ERROR_CODE_SUPERSEDED = 2;
  ERROR_CODE_EXPIRED = 3;
  ERROR_CODE_REVOKED = 4;
  ERROR_CODE_KEY_UNKNOWN = 5;
  ERROR_CODE_TOKEN_INVALID = 6;
  ERROR_CODE_BAD_REQUEST = 7;
}

message Error {
  ErrorCode code = 1;
  uint64 stored_conn_seq = 2; // STALE only
}

// KickReason is numbered as in internal.proto. None of them makes the client
// reconnect on its own.
enum KickReason {
  KICK_REASON_UNSPECIFIED = 0;
  KICK_REASON_REPLACED = 1;
  KICK_REASON_KICKED_BY_OTHER_DEVICE = 2;
  KICK_REASON_REVOKED = 3;
}

message Kick {
  KickReason reason = 1;
}

message Reconnect {
  uint32 jitter_ms = 1; // wait uniform [0, jitter_ms], then reconnect (R4H-O3)
}

message Busy {
  uint32 retry_after_ms = 1;
}
```

In `Makefile`:

Replace:

```makefile
test:
	go test -race -count=1 ./...

# Regenerate api/internalpb from api/internal.proto. Needs protoc 33.1 and the
# two pinned plugins (Plan 2, Task 1); the generated code is committed.
PROTOC ?= protoc
GOBIN_DIR := $(shell go env GOPATH)/bin
proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) --go_out=. --go_opt=module=github.com/Tallinzp/goim \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Tallinzp/goim api/internal.proto

# Tier 2 host only: real Redis primary + replica for the tests that skip at Tier 1a.
redis-up:
```

with:

```makefile
test:
	go test -race -count=1 ./...

# Regenerate api/internalpb and api/clientpb from api/internal.proto and
# api/client.proto. Needs protoc 33.1 and the two pinned plugins (Plan 2,
# Task 1); the generated code is committed.
PROTOC ?= protoc
GOBIN_DIR := $(shell go env GOPATH)/bin
proto:
	PATH="$(GOBIN_DIR):$$PATH" $(PROTOC) --go_out=. --go_opt=module=github.com/Tallinzp/goim \
		--go-grpc_out=. --go-grpc_opt=module=github.com/Tallinzp/goim api/internal.proto api/client.proto

# Tier 2 host only: real Redis primary + replica for the tests that skip at Tier 1a.
redis-up:
```

In `api/internal.proto`:

Replace:

```protobuf
// Internal RPCs between gateways, Logic and the account service
// (spec: Architecture, Logic). The client protocol is api/client.proto (Plan 4).
syntax = "proto3";

package goim.internal.v1;
```

with:

```protobuf
// Internal RPCs between gateways, Logic and the account service
// (spec: Architecture, Logic). The client protocol is api/client.proto.
syntax = "proto3";

package goim.internal.v1;
```

Run: `make proto PROTOC=$HOME/.local/protoc/bin/protoc`
Expected: creates `api/clientpb/client.pb.go`; `git diff api/internalpb` shows only the changed header comment in both files.

- [ ] **Step 4: Run the tests**

Run: `go vet ./api/... && go test -count=1 ./api/...`
Expected: PASS (`api/clientpb`, `api/internalpb`).

- [ ] **Step 5: Commit**

```bash
git add api/client.proto api/clientpb api/internal.proto api/internalpb Makefile
git commit -m "feat(api): client protocol messages (api/client.proto)"
```

---

### Task 2: Gateway config and metrics (choices #33, #36)

**Files:**
- Create: `internal/gateway/config.go`, `internal/gateway/metrics.go`
- Test: `internal/gateway/config_test.go`

**Interfaces:**
- Consumes: `session.ValidGateway` (Plan 1).
- Produces: `Config` (fields listed in the code), `ReconcileConfig`, `DrainConfig`, `Window{Lo, Hi}`, `LoginWindows`, `LimitsConfig`; `DefaultConfig() Config`; `NewEpoch() uint64`; `(Config) DeadAfter()`, `(Config) FenceAfter()`, `(Config) Validate() error`; `const skewAllowance = time.Second`; `Metrics` and `NewMetrics(prometheus.Registerer) *Metrics`; reject reason constants `RejectAtCapacity`, `RejectPendingMem`, `RejectDraining`, `RejectLoginQueue`, `RejectHandshakeTimeout`, `RejectAuthTimeout`. Test helper `testConfig()`.

- [ ] **Step 1: Write the failing test**

`internal/gateway/config_test.go`:

```go
package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// testConfig is a small valid configuration: 4 loops, caps of 1000.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.GatewayID, cfg.Epoch = "gw-1", 0xE1
	cfg.NumLoops = 4
	cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 1000, 1000, 1000, 1000
	return cfg
}

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.GatewayID, cfg.Epoch = "gw-0", NewEpoch()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.DeadAfter() != 310*time.Second || cfg.FenceAfter() != 6*time.Second {
		t.Fatalf("dead after %v, fence after %v; want 310s and 6s", cfg.DeadAfter(), cfg.FenceAfter())
	}
	if err := testConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutRangeIsEnforced(t *testing.T) {
	for _, d := range []time.Duration{2 * time.Second, 31 * time.Second} {
		cfg := testConfig()
		cfg.HandshakeTimeout = d
		if cfg.Validate() == nil {
			t.Errorf("HandshakeTimeout %v accepted", d)
		}
		cfg = testConfig()
		cfg.AuthTimeout = d
		if cfg.Validate() == nil {
			t.Errorf("AuthTimeout %v accepted", d)
		}
	}
	for _, d := range []time.Duration{3 * time.Second, 30 * time.Second} {
		cfg := testConfig()
		cfg.HandshakeTimeout, cfg.AuthTimeout = d, d
		if err := cfg.Validate(); err != nil {
			t.Errorf("%v refused: %v", d, err)
		}
	}
}

func TestValidateRefuses(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*Config)
		want string
	}{
		{"bad gateway id", func(c *Config) { c.GatewayID = "GW_1" }, "gateway id"},
		{"zero epoch", func(c *Config) { c.Epoch = 0 }, "epoch"},
		{"no loops", func(c *Config) { c.NumLoops = 0 }, "NumLoops"},
		{"too many loops", func(c *Config) { c.NumLoops = 1<<16 + 1 }, "NumLoops"},
		{"zero cap", func(c *Config) { c.MaxConns = 0 }, "MaxConns"},
		{"Login timeout below the fence", func(c *Config) { c.LoginTimeout = 5900 * time.Millisecond }, "LoginTimeout"},
		{"fractional heartbeat", func(c *Config) { c.Heartbeat = 1500 * time.Millisecond }, "Heartbeat"},
		{"lease timeout above the interval", func(c *Config) { c.LeaseTimeout = 11 * time.Second }, "LeaseTimeout"},
		{"lease TTL not above the interval", func(c *Config) { c.LeaseTTL = 10 * time.Second }, "LeaseTTL"},
		{"jitter of half a cycle", func(c *Config) { c.Reconcile.Jitter = 5 * time.Minute }, "Jitter"},
		{"empty Busy window", func(c *Config) { c.BusyRetry = Window{3 * time.Second, time.Second} }, "BusyRetry"},
		{"inverted LoginResp window", func(c *Config) { c.Windows.Retryable = Window{2 * time.Second, time.Second} }, "Retryable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			tc.mod(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want an error about %s", err, tc.want)
			}
		})
	}
}

func TestNewEpochIsNonZeroAndRandom(t *testing.T) {
	a, b := NewEpoch(), NewEpoch()
	if a == 0 || b == 0 || a == b {
		t.Fatalf("epochs %x, %x", a, b)
	}
}

func TestNewMetricsRegistersEverything(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	m.Conns.WithLabelValues("online").Inc()
	m.Rejects.WithLabelValues(RejectAtCapacity).Inc()
	families, err := reg.Gather()
	if err != nil || len(families) < 2 {
		t.Fatalf("gathered %d families, err %v", len(families), err)
	}
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/`
Expected: FAIL, build errors such as `undefined: DefaultConfig`, `undefined: Config`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/config.go`:

```go
// Package gateway is the transport-independent gateway core (spec: Gateway):
// per-loop connection tables and timing wheels, the connection state
// machine, admission control, Login batching, the Logout fence, the lease
// loop, the Reconcile pass controller, the Kick server, readiness and drain.
// A transport (TCP + Noise in Plan 4, wss later) feeds it decoded frames.
// It never touches Redis: only Logic does (layer rule).
package gateway

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/Tallinzp/goim/internal/session"
)

// skewAllowance is the 1s fences add for gateway↔Redis clock skew (spec:
// Clock contract). It is not scaled by the test timing profile.
const skewAllowance = time.Second

// Config holds the gateway's tunables. Every interval is configurable so the
// test timing profile can scale them (spec: Test execution model).
type Config struct {
	GatewayID string // pod name, a DNS-1123 label
	Epoch     uint64 // random per boot (NewEpoch)
	NumLoops  int    // event loops (gnet NumEventLoop); worker_id = loop index

	MaxConns      int // connection cap: open sockets in any state (250k)
	PendingCap    int // HANDSHAKE + AUTH_PENDING sockets, memory only (100k)
	LoginQueueCap int // Logins waiting for or inside a Login RPC (8192)

	HandshakeTimeout time.Duration // accept → handshake done (5s, 3–30s)
	AuthTimeout      time.Duration // handshake done → LoginReq (5s, 3–30s)
	Heartbeat        time.Duration // sent in LoginResp; dead after 2× + 10s (150s)

	LoginDeadline time.Duration // login_deadline, the same value as Logic's (5s)
	LoginTimeout  time.Duration // per entry from recv_ms, ≥ LoginDeadline + 1s (6s)
	LoginBatch    int           // entries per Login RPC (200)
	LoginLinger   time.Duration // longest wait to fill a batch (5ms)
	LoginRPCs     int           // concurrent Login RPCs (16)
	BusyRetry     Window        // Busy{retry_after}, uniform (1–3s)

	LogoutBatch   int           // entries per Logout RPC (200)
	LogoutLinger  time.Duration // (5ms)
	LogoutRPCs    int           // concurrent Logout RPCs (4)
	LogoutQueue   int           // Logouts queued or waiting for a retry (MaxConns)
	RPCTimeout    time.Duration // Logout RPC deadline (5s)
	RetryFor      time.Duration // Logout and ReconcileEnd retry budget (60s)
	RetryBackoff  time.Duration // first retry delay, doubled per attempt (1s)
	RetryMaxDelay time.Duration // longest delay between retries (10s)

	LeaseInterval time.Duration // GatewayLease period (10s)
	LeaseTimeout  time.Duration // per call (5s)
	LeaseTTL      time.Duration // Logic's lease TTL (30s), for the trigger rule

	Reconcile ReconcileConfig
	Drain     DrainConfig
	Windows   LoginWindows // reconnect windows sent in LoginResp (R5S-O6)

	Limits LimitsConfig
}

// ReconcileConfig sizes the pass controller (spec: Reconcile).
type ReconcileConfig struct {
	Cycle         time.Duration // periodic pass every Cycle ± Jitter (10m)
	Jitter        time.Duration // (1m)
	Chunk         int           // entries per chunk (200)
	TriggeredRate int           // entries/s of a pass after a missing lease (5000)
	MinRate       int           // floor of the periodic pace, entries/s (10)
	MaxInFlight   int           // chunks sent and not yet resolved (4)
	MaxRequeues   int           // per entry and pass (3, R3-O6)
	DeleteRate    int           // Logic's delete-phase rate, sizes the ReconcileEnd deadline (10k)
}

// DrainConfig is the SIGTERM drain (spec: Graceful drain).
type DrainConfig struct {
	Window time.Duration // Reconnect frames are spread over it (60s)
	Jitter time.Duration // Reconnect{jitter_ms} (5s, R4H-O3)
	Flush  time.Duration // after the last close, wait for Logouts (10s)
}

// Window is a uniform random wait [Lo, Hi].
type Window struct{ Lo, Hi time.Duration }

// LoginWindows are the reconnect windows LoginResp carries. A zero Window is
// not sent, so the SDK keeps its current value (R5S-O6).
type LoginWindows struct {
	StormWait, LocalTrigger, PreHandshakeRetry, Retryable Window
}

// LimitsConfig is the startup self-check (spec: Readiness, R5S-O7).
type LimitsConfig struct {
	Notrack       bool    // conntrack disabled for the gateway port
	Headroom      float64 // nofile ≥ MaxConns × (1 + Headroom) (0.10)
	ConntrackPath string  // node-wide nf_conntrack_max
	CountPath     string  // nf_conntrack_count, exported where readable
}

// DefaultConfig returns the spec defaults. GatewayID and Epoch must be set.
func DefaultConfig() Config {
	return Config{
		NumLoops:         16,
		MaxConns:         250_000,
		PendingCap:       100_000,
		LoginQueueCap:    8192,
		HandshakeTimeout: 5 * time.Second,
		AuthTimeout:      5 * time.Second,
		Heartbeat:        150 * time.Second,
		LoginDeadline:    5 * time.Second,
		LoginTimeout:     6 * time.Second,
		LoginBatch:       200,
		LoginLinger:      5 * time.Millisecond,
		LoginRPCs:        16,
		BusyRetry:        Window{time.Second, 3 * time.Second},
		LogoutBatch:      200,
		LogoutLinger:     5 * time.Millisecond,
		LogoutRPCs:       4,
		LogoutQueue:      250_000,
		RPCTimeout:       5 * time.Second,
		RetryFor:         60 * time.Second,
		RetryBackoff:     time.Second,
		RetryMaxDelay:    10 * time.Second,
		LeaseInterval:    10 * time.Second,
		LeaseTimeout:     5 * time.Second,
		LeaseTTL:         30 * time.Second,
		Reconcile: ReconcileConfig{Cycle: 10 * time.Minute, Jitter: time.Minute, Chunk: 200,
			TriggeredRate: 5000, MinRate: 10, MaxInFlight: 4, MaxRequeues: 3, DeleteRate: 10_000},
		Drain: DrainConfig{Window: 60 * time.Second, Jitter: 5 * time.Second, Flush: 10 * time.Second},
		Limits: LimitsConfig{Headroom: 0.10,
			ConntrackPath: "/proc/sys/net/netfilter/nf_conntrack_max",
			CountPath:     "/proc/sys/net/netfilter/nf_conntrack_count"},
	}
}

// NewEpoch returns a random non-zero boot epoch.
func NewEpoch() uint64 {
	var b [8]byte
	for {
		rand.Read(b[:])
		if e := binary.BigEndian.Uint64(b[:]); e != 0 {
			return e
		}
	}
}

// DeadAfter is when a silent ONLINE connection is dead: 2 × heartbeat + 10s.
func (c Config) DeadAfter() time.Duration { return 2*c.Heartbeat + 10*time.Second }

// FenceAfter is how long after recv_ms (or sent_ms) no Login (or Reconcile)
// write can still land: login_deadline + the 1s skew allowance.
func (c Config) FenceAfter() time.Duration { return c.LoginDeadline + skewAllowance }

// Validate refuses a configuration the design can't run with. The gateway
// refuses to start on an error (spec: Timeouts, Test execution model).
func (c Config) Validate() error {
	var errs []error
	bad := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	if err := session.ValidGateway(c.GatewayID); err != nil {
		bad("gateway id: %v", err)
	}
	if c.Epoch == 0 {
		bad("epoch must be non-zero (NewEpoch)")
	}
	if c.NumLoops < 1 || c.NumLoops > 1<<16 {
		bad("NumLoops %d must be in [1, 65536]: worker_id is 16 bits", c.NumLoops)
	}
	for name, v := range map[string]int{"MaxConns": c.MaxConns, "PendingCap": c.PendingCap,
		"LoginQueueCap": c.LoginQueueCap, "LoginBatch": c.LoginBatch, "LoginRPCs": c.LoginRPCs,
		"LogoutBatch": c.LogoutBatch, "LogoutRPCs": c.LogoutRPCs, "LogoutQueue": c.LogoutQueue,
		"Reconcile.Chunk": c.Reconcile.Chunk, "Reconcile.TriggeredRate": c.Reconcile.TriggeredRate,
		"Reconcile.MinRate": c.Reconcile.MinRate, "Reconcile.MaxInFlight": c.Reconcile.MaxInFlight,
		"Reconcile.DeleteRate": c.Reconcile.DeleteRate} {
		if v <= 0 {
			bad("%s %d must be positive", name, v)
		}
	}
	if c.Reconcile.MaxRequeues < 0 {
		bad("Reconcile.MaxRequeues %d must not be negative", c.Reconcile.MaxRequeues)
	}
	for name, d := range map[string]time.Duration{"HandshakeTimeout": c.HandshakeTimeout, "AuthTimeout": c.AuthTimeout} {
		if d < 3*time.Second || d > 30*time.Second {
			bad("%s %v outside the allowed 3s–30s", name, d)
		}
	}
	if c.Heartbeat < time.Second || c.Heartbeat%time.Second != 0 {
		bad("Heartbeat %v must be a whole number of seconds (LoginResp carries seconds)", c.Heartbeat)
	}
	if c.LoginDeadline <= 0 {
		bad("LoginDeadline %v must be positive", c.LoginDeadline)
	}
	if c.LoginTimeout < c.FenceAfter() {
		bad("LoginTimeout %v must be at least login_deadline + 1s = %v (R1-O9)", c.LoginTimeout, c.FenceAfter())
	}
	for name, d := range map[string]time.Duration{"LoginLinger": c.LoginLinger, "LogoutLinger": c.LogoutLinger,
		"RPCTimeout": c.RPCTimeout, "RetryFor": c.RetryFor, "RetryBackoff": c.RetryBackoff,
		"RetryMaxDelay": c.RetryMaxDelay, "LeaseInterval": c.LeaseInterval, "LeaseTimeout": c.LeaseTimeout,
		"Reconcile.Cycle": c.Reconcile.Cycle, "Drain.Window": c.Drain.Window, "Drain.Jitter": c.Drain.Jitter} {
		if d <= 0 {
			bad("%s %v must be positive", name, d)
		}
	}
	if c.LeaseTimeout > c.LeaseInterval {
		bad("LeaseTimeout %v must not exceed LeaseInterval %v", c.LeaseTimeout, c.LeaseInterval)
	}
	if c.LeaseTTL <= c.LeaseInterval {
		bad("LeaseTTL %v must exceed LeaseInterval %v", c.LeaseTTL, c.LeaseInterval)
	}
	if c.Reconcile.Jitter < 0 || c.Reconcile.Jitter >= c.Reconcile.Cycle/2 {
		bad("Reconcile.Jitter %v must be in [0, Cycle/2)", c.Reconcile.Jitter)
	}
	if c.Drain.Flush < 0 {
		bad("Drain.Flush %v must not be negative", c.Drain.Flush)
	}
	if err := c.BusyRetry.check(false); err != nil {
		bad("BusyRetry: %v", err)
	}
	for name, w := range map[string]Window{"StormWait": c.Windows.StormWait, "LocalTrigger": c.Windows.LocalTrigger,
		"PreHandshakeRetry": c.Windows.PreHandshakeRetry, "Retryable": c.Windows.Retryable} {
		if err := w.check(true); err != nil {
			bad("Windows.%s: %v", name, err)
		}
	}
	if c.Limits.Headroom < 0 {
		bad("Limits.Headroom %v must not be negative", c.Limits.Headroom)
	}
	return errors.Join(errs...)
}

func (w Window) check(zeroOK bool) error {
	switch {
	case zeroOK && w == Window{}:
		return nil
	case w.Lo < 0 || w.Hi <= 0 || w.Lo >= w.Hi:
		return fmt.Errorf("[%v, %v] needs 0 ≤ lo < hi", w.Lo, w.Hi)
	case w.Hi > time.Duration(1<<32-1)*time.Millisecond:
		return fmt.Errorf("hi %v does not fit in uint32 ms", w.Hi)
	}
	return nil
}
```

`internal/gateway/metrics.go`:

```go
package gateway

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Admission reject reasons (spec: Observability). The TCP transport adds
// handshake_rate, bad_version and noise_unknown_key (Plan 4).
const (
	RejectAtCapacity       = "at_capacity"
	RejectPendingMem       = "pending_mem"
	RejectDraining         = "draining"
	RejectLoginQueue       = "login_queue"
	RejectHandshakeTimeout = "handshake_timeout"
	RejectAuthTimeout      = "auth_timeout"
)

// Metrics are the gateway core's Prometheus metrics (spec: Observability).
// Connection skew (max / mean ONLINE per gateway) is computed in Prometheus
// from Conns{state="online"}.
type Metrics struct {
	Conns          *prometheus.GaugeVec   // state: handshake | auth_pending | online
	AwaitingLogout prometheus.Gauge       // closed connections still tracked
	Tracked        prometheus.Gauge       // every table entry
	Rejects        *prometheus.CounterVec // reason
	Closes         *prometheus.CounterVec // reason: client | heartbeat | kick | not_owner | logout | protocol | drain

	Logins         *prometheus.CounterVec // result
	LoginQueue     prometheus.Gauge       // Logins waiting for or inside a Login RPC
	LoginSeconds   prometheus.Histogram   // LoginResp sent − recv_ms
	ReloginSeconds *prometheus.HistogramVec

	Logouts *prometheus.CounterVec // result: done | retried | dropped
	Kicks   *prometheus.CounterVec // result: closed | pending | ignored

	LeaseCalls *prometheus.CounterVec // result: live | missing | error

	Passes            *prometheus.CounterVec // pace, result: done | cancelled | incomplete
	PassSeconds       *prometheus.HistogramVec
	ReconcileEntries  *prometheus.CounterVec // result: written | unchanged | refused | not_owner | no_reply
	ReconcileNotOwner *prometheus.CounterVec // reason
	Requeued          prometheus.Counter
	RequeueLimit      prometheus.Counter
	ReconcileEnds     *prometheus.CounterVec // result: ok | retried | dropped
	EndDeleted        prometheus.Counter
	EndTracked        prometheus.Gauge // tracked_conn_ids of the last ReconcileEnd

	Limits  *prometheus.GaugeVec // limit: nofile | nf_conntrack_max
	Ready   prometheus.Gauge
	RPCSecs *prometheus.HistogramVec // method, code
}

// NewMetrics registers every metric on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	const ns = "goim_gateway"
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
		Conns:          gv("connections", "Open connections by state.", "state"),
		AwaitingLogout: g("awaiting_logout", "Closed connections still tracked: their fenced Logout is pending."),
		Tracked:        g("tracked", "Connection table entries (the tracked set)."),
		Rejects:        cv("admission_rejects_total", "Connections rejected or closed by admission control.", "reason"),
		Closes:         cv("closes_total", "Connections the gateway closed, by reason.", "reason"),

		Logins:       cv("logins_total", "Logins by result, as the client sees it.", "result"),
		LoginQueue:   g("login_queue", "Logins waiting for or inside a Login RPC."),
		LoginSeconds: h("login_seconds", "LoginResp sent minus recv_ms.", lat),
		ReloginSeconds: hv("relogin_seconds", "offline_ms + (login OK − recv_ms).",
			prometheus.LinearBuckets(0, 2.5, 25), "reconnect_reason"),

		Logouts: cv("logouts_total", "Fenced Logout entries by result.", "result"),
		Kicks:   cv("kicks_total", "Kick RPCs received, by what they did.", "result"),

		LeaseCalls: cv("lease_calls_total", "GatewayLease calls by result.", "result"),

		Passes: cv("reconcile_passes_total", "Reconcile passes by pace and result.", "pace", "result"),
		PassSeconds: hv("reconcile_pass_seconds", "Reconcile pass duration (cycle), by pace.",
			prometheus.ExponentialBuckets(1, 2, 12), "pace"),
		ReconcileEntries:  cv("reconcile_entries_total", "Reconcile entries by result.", "result"),
		ReconcileNotOwner: cv("reconcile_not_owner_total", "not_owner entries by reason.", "reason"),
		Requeued:          c("reconcile_requeued_total", "refused entries put back on the pass queue."),
		RequeueLimit:      c("reconcile_requeue_limit_total", "refused entries dropped at the re-queue limit."),
		ReconcileEnds:     cv("reconcile_end_total", "ReconcileEnd attempts by result.", "result"),
		EndDeleted:        c("reconcile_end_deleted_total", "Fields deleted by this gateway's ReconcileEnds."),
		EndTracked:        g("reconcile_end_tracked", "tracked_conn_ids in the last ReconcileEnd."),

		Limits:  gv("limit", "Limits read by the startup self-check.", "limit"),
		Ready:   g("ready", "1 while the gateway reports Ready."),
		RPCSecs: hv("rpc_seconds", "Gateway → Logic RPC latency.", lat, "method", "code"),
	}
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): config with timeout ranges and ratio checks, metrics"
```

---

### Task 3: Timing wheel (R4H-O6)

**Files:**
- Create: `internal/gateway/wheel.go`
- Test: `internal/gateway/wheel_test.go`

**Interfaces:**
- Produces: `const wheelTick = time.Second`, `wheelSlots = 512`; `timer` (embed it in its owner; the zero value is stopped); `newWheel(now time.Time) *wheel`; `(*wheel) schedule(t *timer, at time.Time, fn func())` (rounds up, moves an armed timer), `stop(t *timer)` (also cancels a timer already collected by the running `advance`), `advance(now time.Time)`. Test var `t0`.

The wheel stores each timer's absolute tick; that tick is the round counter. A callback that arms a timer lands on a later tick, never in the running `advance`. A clock jump past one turn visits every slot once.

- [ ] **Step 1: Write the failing test**

`internal/gateway/wheel_test.go`:

```go
package gateway

import (
	"testing"
	"time"
)

var t0 = time.UnixMilli(1_760_000_000_000) // on a tick boundary

func TestWheelNeverFiresEarly(t *testing.T) {
	for _, tc := range []struct {
		name     string
		start    time.Duration // wheel's now, after t0
		deadline time.Duration // after t0
	}{
		{"on a boundary", 0, 5 * time.Second},
		{"between ticks", 300 * time.Millisecond, 5*time.Second + 300*time.Millisecond},
		{"1ms past a boundary", 0, 5*time.Second + time.Millisecond},
		{"past one turn of the wheel", 0, 600 * time.Second},
		{"in the past", 2 * time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWheel(t0.Add(tc.start))
			var tm timer
			var firedAt time.Time
			now := t0.Add(tc.start)
			w.schedule(&tm, t0.Add(tc.deadline), func() { firedAt = now })
			for step := 0; firedAt.IsZero() && step < 10_000; step++ {
				now = now.Add(100 * time.Millisecond)
				w.advance(now)
			}
			if firedAt.IsZero() {
				t.Fatal("never fired")
			}
			if firedAt.Before(t0.Add(tc.deadline)) {
				t.Fatalf("fired at %v, before its deadline %v", firedAt.Sub(t0), tc.deadline)
			}
			if late := firedAt.Sub(t0.Add(tc.deadline)); tc.deadline > tc.start && late > wheelTick {
				t.Fatalf("fired %v late, more than one tick", late)
			}
		})
	}
}

func TestWheelLongDelayWaitsForItsRound(t *testing.T) {
	w := newWheel(t0)
	var tm timer
	fired := false
	w.schedule(&tm, t0.Add(600*time.Second), func() { fired = true }) // slot 600 % 512 = 88
	w.advance(t0.Add(88 * time.Second))
	w.advance(t0.Add(599 * time.Second))
	if fired {
		t.Fatal("fired on an earlier turn of the wheel")
	}
	w.advance(t0.Add(600 * time.Second))
	if !fired {
		t.Fatal("did not fire at 600s")
	}
}

func TestWheelStopAndRearm(t *testing.T) {
	w := newWheel(t0)
	var a, b timer
	var got []string
	w.schedule(&a, t0.Add(5*time.Second), func() { got = append(got, "a") })
	w.schedule(&b, t0.Add(5*time.Second), func() { got = append(got, "b") })
	w.stop(&a)
	w.stop(&a) // no-op
	w.schedule(&b, t0.Add(7*time.Second), func() { got = append(got, "b2") })
	w.advance(t0.Add(6 * time.Second))
	if len(got) != 0 {
		t.Fatalf("fired %v", got)
	}
	w.advance(t0.Add(7 * time.Second))
	if len(got) != 1 || got[0] != "b2" {
		t.Fatalf("got %v, want [b2]", got)
	}
	if w.n != 0 {
		t.Fatalf("%d timers still armed", w.n)
	}
}

func TestWheelCallbackStopsAnotherDueTimer(t *testing.T) {
	w := newWheel(t0)
	var a, b timer
	var got []string
	// Both are due in the same advance; whichever runs first stops the other.
	w.schedule(&a, t0.Add(time.Second), func() { got = append(got, "a"); w.stop(&b) })
	w.schedule(&b, t0.Add(time.Second), func() { got = append(got, "b"); w.stop(&a) })
	w.advance(t0.Add(time.Second))
	if len(got) != 1 {
		t.Fatalf("got %v, want exactly one", got)
	}
}

func TestWheelCallbackRearmsForALaterTick(t *testing.T) {
	w := newWheel(t0)
	var tm timer
	n := 0
	var fn func()
	fn = func() { n++; w.schedule(&tm, t0, fn) } // a deadline in the past
	w.schedule(&tm, t0.Add(time.Second), fn)
	w.advance(t0.Add(3 * time.Second))
	if n != 1 {
		t.Fatalf("ran %d times in one advance, want 1", n)
	}
	w.advance(t0.Add(4 * time.Second))
	if n != 2 {
		t.Fatalf("ran %d times after the next tick, want 2", n)
	}
}

func TestWheelClockJumpFiresEveryDueTimerOnce(t *testing.T) {
	w := newWheel(t0)
	timers := make([]timer, 1000)
	fired := make([]int, len(timers))
	for i := range timers {
		w.schedule(&timers[i], t0.Add(time.Duration(i+1)*time.Second), func() { fired[i]++ })
	}
	w.advance(t0.Add(2 * time.Hour))
	for i, n := range fired {
		if n != 1 {
			t.Fatalf("timer %d fired %d times", i, n)
		}
	}
}

func TestWheelScheduleDoesNotAllocate(t *testing.T) {
	w := newWheel(t0)
	var tm timer
	fn := func() {}
	allocs := testing.AllocsPerRun(1000, func() {
		w.schedule(&tm, t0.Add(310*time.Second), fn)
		w.stop(&tm)
	})
	if allocs != 0 {
		t.Fatalf("%v allocations per schedule+stop", allocs)
	}
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run Wheel`
Expected: FAIL, `undefined: newWheel`, `undefined: timer`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/wheel.go`:

```go
package gateway

import "time"

// Timing wheel (spec: Timers, R4H-O6): one per loop, touched only on the
// loop's goroutine, so it needs no lock and its callbacks may use the loop's
// connection table. A 1s tick and 512 slots; a timer stores its absolute
// tick, which plays the role of the round counter for delays past 512s.
const (
	wheelTick  = time.Second
	wheelSlots = 512
)

// timer is embedded in its owner (a connection or a chunk), so arming one
// allocates nothing. The zero value is a stopped timer.
type timer struct {
	at         int64 // tick index; fires when the wheel reaches it
	fn         func()
	prev, next *timer
	armed      bool // in a slot
	firing     bool // taken out by advance, not run yet
}

type wheel struct {
	tickMs int64
	cur    int64 // last tick processed
	heads  [wheelSlots]*timer
	due    []*timer // scratch for advance
	n      int      // armed timers
}

func newWheel(now time.Time) *wheel {
	w := &wheel{tickMs: wheelTick.Milliseconds()}
	w.cur = now.UnixMilli() / w.tickMs
	return w
}

// schedule arms t to run fn at the first tick at or after at: deadlines round
// up, so a timer never fires early (it may fire up to one tick late). An
// armed t is moved.
func (w *wheel) schedule(t *timer, at time.Time, fn func()) {
	w.stop(t)
	ms := at.UnixMilli()
	tick := ms / w.tickMs
	if ms%w.tickMs != 0 {
		tick++
	}
	if tick <= w.cur {
		tick = w.cur + 1
	}
	t.at, t.fn, t.armed = tick, fn, true
	h := &w.heads[tick%wheelSlots]
	t.prev, t.next = nil, *h
	if *h != nil {
		(*h).prev = t
	}
	*h = t
	w.n++
}

// stop disarms t, also when advance has taken it out but not yet run it
// (another callback of the same advance stops it); stopping a stopped timer
// is a no-op.
func (w *wheel) stop(t *timer) {
	if t.firing {
		t.firing, t.fn = false, nil
	}
	if !t.armed {
		return
	}
	if t.prev != nil {
		t.prev.next = t.next
	} else {
		w.heads[t.at%wheelSlots] = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	}
	t.prev, t.next, t.fn, t.armed = nil, nil, nil, false
	w.n--
}

// advance runs every timer whose tick is at or before now's tick. A callback
// that arms a timer lands on a later tick, never in this advance.
func (w *wheel) advance(now time.Time) {
	target := now.UnixMilli() / w.tickMs
	if target <= w.cur {
		return
	}
	steps := min(target-w.cur, wheelSlots) // a jump past one turn visits every slot once
	for k := target - steps + 1; k <= target; k++ {
		for t := w.heads[k%wheelSlots]; t != nil; {
			next := t.next
			if t.at <= target {
				fn := t.fn
				w.stop(t)
				t.fn, t.firing = fn, true
				w.due = append(w.due, t)
			}
			t = next
		}
	}
	w.cur = target
	due := w.due
	for i, t := range due {
		due[i] = nil
		if !t.firing {
			continue // stopped or re-armed by an earlier callback
		}
		fn := t.fn
		t.firing, t.fn = false, nil
		fn()
	}
	w.due = due[:0]
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/wheel.go internal/gateway/wheel_test.go
git commit -m "feat(gateway): per-loop timing wheel that never fires early"
```

---

### Task 4: Loop states, `conn_id` and connection records (R4H-O4, R4H-O5, race 13)

**Files:**
- Create: `internal/gateway/loops.go`, `internal/gateway/conn.go`
- Test: `internal/gateway/loops_test.go`

**Interfaces:**
- Consumes: `newWheel` (Task 3), `Config` (Task 2).
- Produces: `Loop` interface `{Execute(fn func()) error}`; `loopState` (`workerID`, `bound`, `counter`, `conns map[uint64]*Conn`, `wheel`), `(*loopState) loop() Loop`, `(*loopState) nextID() uint64`; `loops`, `newLoops(cfg, now)`, `(*loops) bind(Loop) *loopState`, `bound() []*loopState`, `byWorker(uint64) *loopState`; `tableHint(cfg) int`. `Link` interface `{Send(cmd clientpb.Cmd, reqID uint32, body []byte); Close()}`; `Conn` and its fields; `state` (`stateHandshake` … `stateClosed`), `stateLabel`; `outcome` (`loginNone`, `loginPending`, `loginOK`, `loginRejected`, `loginUnknown`); `loginItem{ls, connID, entry *pb.LoginEntry, state atomic.Int32}` with `itemQueued`, `itemSent`, `itemCancelled`, `itemDropped`. Test helpers `stubLoop`, `bytesAllocated`.

`loops_test.go` checks the table pre-sizing with `testing.AllocsPerRun` (zero allocations for a loop's share of the cap). It checks that binding a loop allocates no table by measuring **bytes** with `runtime.MemStats`, because `AllocsPerRun` counts allocations and a table is one allocation like any small struct.

- [ ] **Step 1: Write the failing test**

`internal/gateway/loops_test.go`:

```go
package gateway

import (
	"runtime"
	"sync"
	"testing"
)

// stubLoop is a Loop that is never run; bind only needs its identity. The
// field keeps each one a distinct pointer (zero-size values may share one).
type stubLoop struct{ _ int }

func (*stubLoop) Execute(func()) error { return nil }

func TestConnIDsAreUniqueAndCarryTheWorkerID(t *testing.T) {
	cfg := testConfig()
	ls := newLoops(cfg, t0)
	const perLoop = 1000
	ids := make([][]uint64, cfg.NumLoops)
	var wg sync.WaitGroup
	for i := range cfg.NumLoops {
		wg.Add(1)
		go func() { // each goroutine is one loop: binds, then opens connections
			defer wg.Done()
			s := ls.bind(&stubLoop{})
			for range perLoop {
				ids[i] = append(ids[i], s.nextID())
			}
		}()
	}
	wg.Wait()
	seen := make(map[uint64]bool)
	workers := make(map[uint64]bool)
	for i, list := range ids {
		worker := list[0] >> 48
		workers[worker] = true
		for _, id := range list {
			if seen[id] {
				t.Fatalf("conn_id %#x issued twice", id)
			}
			seen[id] = true
			if id>>48 != worker || id&(1<<48-1) == 0 {
				t.Fatalf("loop %d: conn_id %#x (worker %d, counter 0 not allowed)", i, id, worker)
			}
		}
	}
	if len(workers) != cfg.NumLoops || len(ls.bound()) != cfg.NumLoops {
		t.Fatalf("%d worker ids and %d bound loops, want %d", len(workers), len(ls.bound()), cfg.NumLoops)
	}
	for w := range uint64(cfg.NumLoops) {
		if ls.byWorker(w) == nil {
			t.Fatalf("byWorker(%d) = nil", w)
		}
	}
	if ls.byWorker(uint64(cfg.NumLoops)) != nil {
		t.Fatal("byWorker beyond NumLoops")
	}
}

func TestBindIsStable(t *testing.T) {
	ls := newLoops(testConfig(), t0)
	l := &stubLoop{}
	if a, b := ls.bind(l), ls.bind(l); a != b || ls.claimed.Load() != 1 {
		t.Fatal("binding the same loop twice claimed two loopStates")
	}
}

func TestLoopBeyondNumLoopsPanics(t *testing.T) {
	cfg := testConfig()
	cfg.NumLoops = 1
	ls := newLoops(cfg, t0)
	ls.bind(&stubLoop{})
	defer func() {
		if recover() == nil {
			t.Fatal("a second loop with NumLoops = 1 did not panic")
		}
	}()
	ls.bind(&stubLoop{})
}

// bytesAllocated measures what f allocates. AllocsPerRun counts allocations,
// not their size, and a table is one allocation like any small struct.
func bytesAllocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestFirstOpenOnALoopAllocatesNoTable(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns, cfg.NumLoops = 250_000, 16 // tables of 23k slots each
	ls := newLoops(cfg, t0)
	fakes := make([]*stubLoop, cfg.NumLoops)
	for i := range fakes {
		fakes[i] = &stubLoop{}
	}
	for i := range fakes {
		if n := bytesAllocated(func() { ls.bind(fakes[i]) }); n > 1024 {
			t.Fatalf("binding loop %d allocated %d bytes: the table must be built at boot", i, n)
		}
	}
}

func TestPreSizedTableDoesNotGrow(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns, cfg.NumLoops = 64_000, 4
	n := cfg.MaxConns / cfg.NumLoops // a loop's share of the cap
	conns := make([]*Conn, n)
	for i := range conns {
		conns[i] = &Conn{}
	}
	tables := []map[uint64]*Conn{
		make(map[uint64]*Conn, tableHint(cfg)), // warm-up run
		make(map[uint64]*Conn, tableHint(cfg)), // measured run
	}
	run := 0
	allocs := testing.AllocsPerRun(1, func() {
		m := tables[run]
		run++
		for i, c := range conns {
			m[uint64(i)+1] = c
		}
	})
	if allocs != 0 {
		t.Fatalf("inserting cap/loops entries into a pre-sized table allocated %v times (map growth)", allocs)
	}
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run 'ConnID|Bind|Loop|Table'`
Expected: FAIL, `undefined: newLoops`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/loops.go`:

```go
package gateway

import (
	"sync"
	"sync/atomic"
	"time"
)

// Loop is a transport's event loop: the goroutine that owns a set of
// connections. Execute queues fn to run on that goroutine; it is safe from any
// goroutine and never blocks. It does not keep order between calls: gnet
// v2.10.0 moves low-priority tasks to a second queue under load, so a later
// task can run first. It fails only once the loop has shut down.
type Loop interface {
	Execute(fn func()) error
}

// loopState is one loop's share of the gateway: its worker_id, conn_id
// counter, connection table and timing wheel. Every
// field but bound is touched only on the loop's goroutine, so none needs a
// lock (spec: Per-loop connection tables).
type loopState struct {
	workerID uint64
	bound    atomic.Pointer[boundLoop] // set once, by the first Open on the loop

	counter uint64 // conn_id low 48 bits; only Open touches it
	conns   map[uint64]*Conn
	wheel   *wheel
}

type boundLoop struct{ l Loop }

// loop returns the bound Loop; call only after the loopState has been bound.
func (ls *loopState) loop() Loop { return ls.bound.Load().l }

// nextID returns a new conn_id: worker_id << 48 | counter (spec: Identity).
// IDs start at 1 and are never reused within an epoch.
func (ls *loopState) nextID() uint64 {
	ls.counter++
	return ls.workerID<<48 | ls.counter
}

// loops holds every loopState, built at boot (R4H-O5). A transport can't list
// its loops at boot (gnet has no such call), so the first Open on a loop
// claims the next loopState and caches the binding.
type loops struct {
	states  []*loopState
	claimed atomic.Int64
	byLoop  sync.Map // Loop → *loopState
}

// tableHint pre-sizes each loop's table so it does not grow during a storm:
// cap / loops × 1.5 (R4H-O4). It is a hint, not a limit.
func tableHint(cfg Config) int { return cfg.MaxConns / cfg.NumLoops * 3 / 2 }

func newLoops(cfg Config, now time.Time) *loops {
	ls := &loops{states: make([]*loopState, cfg.NumLoops)}
	for i := range ls.states {
		ls.states[i] = &loopState{
			workerID: uint64(i),
			conns:    make(map[uint64]*Conn, tableHint(cfg)),
			wheel:    newWheel(now),
		}
	}
	return ls
}

// bind returns l's loopState, claiming the next free one the first time l is
// seen. It runs on l's goroutine. More loops than NumLoops is a startup bug.
func (ls *loops) bind(l Loop) *loopState {
	if s, ok := ls.byLoop.Load(l); ok {
		return s.(*loopState)
	}
	i := ls.claimed.Add(1) - 1
	if i >= int64(len(ls.states)) {
		panic("gateway: an event loop beyond NumLoops opened a connection")
	}
	s := ls.states[i]
	s.bound.Store(&boundLoop{l})
	ls.byLoop.Store(l, s)
	return s
}

// bound returns the loopStates bound so far.
func (ls *loops) bound() []*loopState {
	n := min(ls.claimed.Load(), int64(len(ls.states)))
	out := make([]*loopState, 0, n)
	for _, s := range ls.states[:n] {
		if s.bound.Load() != nil {
			out = append(out, s)
		}
	}
	return out
}

// byWorker returns the bound loopState for a worker_id, or nil.
func (ls *loops) byWorker(w uint64) *loopState {
	if w >= uint64(len(ls.states)) {
		return nil
	}
	if s := ls.states[w]; s.bound.Load() != nil {
		return s
	}
	return nil
}
```

`internal/gateway/conn.go`:

```go
package gateway

import (
	"sync/atomic"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// Link is the transport's side of one connection (spec: Transport seam). The
// core calls it only on the connection's loop goroutine.
type Link interface {
	// Send queues one frame; body is the encoded message for cmd.
	Send(cmd clientpb.Cmd, reqID uint32, body []byte)
	// Close closes the socket. The transport then calls Gateway.Closed once,
	// on the loop, as gnet does from OnClose.
	Close()
}

// state is the connection state machine (spec: Connection states).
type state uint8

const (
	stateHandshake state = iota
	stateAuthPending
	stateOnline
	stateClosed
)

var stateLabel = [...]string{"handshake", "auth_pending", "online", "closed"}

// outcome is what the gateway knows about a connection's Login.
type outcome uint8

const (
	loginNone     outcome = iota // no Login sent to Logic: nothing to log out
	loginPending                 // sent or queued, no definitive reply yet
	loginOK                      // definitive: the session was written
	loginRejected                // definitive: nothing was written
	loginUnknown                 // no definitive reply (fence b)
)

// Conn is one tracked connection. It lives in its loop's table from accept
// until it no longer needs a Logout (spec: Tracked connections), and is
// touched only on that loop's goroutine.
type Conn struct {
	id      uint64
	ls      *loopState
	link    Link // nil once closed
	state   state
	closing bool // Close requested; Closed not called yet

	life    timer // HANDSHAKE, then AUTH_PENDING, then heartbeat
	fence   timer // Logout fence (b): recv_ms + login_deadline + 1s
	onLife  func()
	onFence func()

	lastActive int64 // ms, last frame while ONLINE

	login       *loginItem // the Login while pending or unknown
	loginReqID  uint32
	outcome     outcome
	fenceB      bool
	kickPending clientpb.KickReason // a Kick that arrived during AUTH_PENDING

	// The session, for Reconcile and Logout. uid is the token's verified
	// subject from Logic's reply.
	uid       string
	appid     string
	platform  pb.Platform
	deviceID  string
	connSeq   uint64
	recvMs    int64 // login_ms
	tokIatMs  int64
	reason    clientpb.ReconnectReason
	offlineMs int64

	inChunk      bool // in a Reconcile chunk not yet resolved (fence c)
	logoutQueued bool
}

// ID returns the conn_id.
func (c *Conn) ID() uint64 { return c.id }

// A loginItem is one Login between the loop and Logic. Its state is the
// hand-off between the loop (which may cancel it on close) and the Login
// worker (which sends it or drops it at login_deadline): whoever moves it out
// of itemQueued owns the outcome.
type loginItem struct {
	ls     *loopState
	connID uint64
	entry  *pb.LoginEntry
	state  atomic.Int32
}

const (
	itemQueued int32 = iota
	itemSent
	itemCancelled // the connection closed first: never sent, no Logout
	itemDropped   // older than login_deadline at send time: never sent
)
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/loops.go internal/gateway/conn.go internal/gateway/loops_test.go
git commit -m "feat(gateway): loop states with lazy binding, conn_id, pre-sized tables"
```

---

### Task 5: Connection state machine, admission and Logout fence (a)(b) on a fake transport (choices #25–#27)

**Files:**
- Create: `internal/gateway/gateway.go`, `internal/gateway/login.go` (first part), `internal/gateway/logout.go` (first part), `internal/gateway/token.go`
- Test: `internal/gateway/harness_test.go`, `internal/gateway/conn_test.go`, `internal/gateway/layer_test.go`

**Interfaces:**
- Consumes: Tasks 2–4; `clientpb` (Task 1); `pb.LoginEntry`, `pb.LogoutEntry`, `pb.GatewayRef`, `pb.Platform` (Plan 2).
- Produces:
  - `Gateway`; `Deps{Metrics *Metrics; Registry *prometheus.Registry; Clock func() time.Time; ReadyChecks []func() error}`; `New(cfg Config, d Deps) (*Gateway, error)`.
  - Transport entry points, all on the connection's loop: `(*Gateway) Open(l Loop, link Link) (*Conn, string)`, `HandshakeDone(c *Conn)`, `Frame(c *Conn, cmd clientpb.Cmd, reqID uint32, body []byte)`, `Closed(c *Conn)`; `(*Conn) ID() uint64`.
  - Internal, used by later tasks: `g.loginCh chan *loginItem` (Task 6 drains it), `g.logoutCh chan *logoutItem` (Task 7 drains it), `g.closeConn(c, reason)`, `g.send(c, cmd, reqID, msg)`, `g.busy(c, reqID)`, `g.kickConn(c, reason, metric)`, `g.settle(c)`, `g.untrack(c)`, `g.applyLogin(it *loginItem, r loginReply)`, `g.loginFinished()`, `g.reconnect(c)`, `loginReply{outcome, code, label, uid, tokIatMs, storedConnSeq}`, `unknownReply`, `logoutItem{ls, connID, entry, first, attempts}`, `g.queueLogout(c, uid)`, `tokenSubject(token) string`, `g.draining atomic.Bool`, `g.tracked`, `g.loginQueued atomic.Int64`.
  - Test harness (`harness_test.go`): `fakeLoop` (`Execute`, `do`, `close`), `fakeLink` (`clientClose`, `isClosed`, `sent`, `last`), `fakeClock` (`now`, `jump`), `core` with `newCore(t, mod)`, `open`, `authed`, `frame`, `login`, `reply`, `online`, `advance`, `sync`, `tracked`, `takeLogout`; `onLoop`, `loginReq`, `fakeToken`, `waitFor`.

How the fence works (`settle`, run whenever a condition may have changed): a closed connection is untracked when it needs no Logout (no Login reached Logic, or Logic definitively wrote nothing, or no uid can be named); otherwise its Logout is queued once fence (a) (an OK reply) or fence (b) (the wheel's `recv_ms + login_deadline + 1s` timer) holds and it is in no unresolved Reconcile chunk (fence c, Task 9 sets `inChunk`). The Login hand-off: the loop queues a `loginItem` in `itemQueued`; whoever moves it out of that state with a compare-and-swap owns the outcome.

- [ ] **Step 1: Write the failing tests**

`internal/gateway/harness_test.go`:

```go
package gateway

import (
	"encoding/base64"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

// ---- fake transport ----

// fakeLoop is an event loop: one goroutine running queued funcs. Unlike gnet
// it keeps order, so sync can wait for everything queued before it.
type fakeLoop struct {
	mu     sync.Mutex
	cond   *sync.Cond
	q      []func()
	closed bool
	done   chan struct{}
}

func newFakeLoop() *fakeLoop {
	l := &fakeLoop{done: make(chan struct{})}
	l.cond = sync.NewCond(&l.mu)
	go l.run()
	return l
}

var errLoopClosed = errors.New("fake loop closed")

func (l *fakeLoop) Execute(fn func()) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errLoopClosed
	}
	l.q = append(l.q, fn)
	l.cond.Signal()
	return nil
}

func (l *fakeLoop) run() {
	defer close(l.done)
	for {
		l.mu.Lock()
		for len(l.q) == 0 && !l.closed {
			l.cond.Wait()
		}
		if len(l.q) == 0 {
			l.mu.Unlock()
			return
		}
		fn := l.q[0]
		l.q = l.q[1:]
		l.mu.Unlock()
		fn()
	}
}

// do runs fn on the loop and waits for it.
func (l *fakeLoop) do(fn func()) {
	done := make(chan struct{})
	if l.Execute(func() { fn(); close(done) }) != nil {
		return
	}
	<-done
}

func (l *fakeLoop) close() {
	l.mu.Lock()
	l.closed = true
	l.cond.Signal()
	l.mu.Unlock()
	<-l.done
}

type frame struct {
	cmd   clientpb.Cmd
	reqID uint32
	body  []byte
}

// fakeLink is one connection's transport side. Close calls Gateway.Closed
// later, on the loop, as gnet's OnClose does.
type fakeLink struct {
	g    *Gateway
	loop *fakeLoop
	c    *Conn // set on the loop by open

	mu     sync.Mutex
	frames []frame
	closed bool
}

func (k *fakeLink) Send(cmd clientpb.Cmd, reqID uint32, body []byte) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.closed {
		k.frames = append(k.frames, frame{cmd, reqID, append([]byte(nil), body...)})
	}
}

func (k *fakeLink) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed {
		return
	}
	k.closed = true
	k.loop.Execute(func() { k.g.Closed(k.c) })
}

// clientClose is the client hanging up (RST/FIN).
func (k *fakeLink) clientClose() { k.Close() }

func (k *fakeLink) isClosed() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.closed
}

func (k *fakeLink) sent() []frame {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]frame(nil), k.frames...)
}

// last returns the last frame with cmd, decoded into msg (if not nil).
func (k *fakeLink) last(t *testing.T, cmd clientpb.Cmd, msg proto.Message) bool {
	t.Helper()
	fs := k.sent()
	for i := len(fs) - 1; i >= 0; i-- {
		if fs[i].cmd == cmd {
			if msg != nil {
				if err := proto.Unmarshal(fs[i].body, msg); err != nil {
					t.Fatalf("decode %v: %v", cmd, err)
				}
			}
			return true
		}
	}
	return false
}

// ---- clock ----

// fakeClock is real time plus an offset tests can move forward.
type fakeClock struct{ off atomic.Int64 }

func (c *fakeClock) now() time.Time       { return time.Now().Add(time.Duration(c.off.Load())) }
func (c *fakeClock) jump(d time.Duration) { c.off.Add(int64(d)) }

// ---- core harness (no Logic) ----

// core is a Gateway on fake loops, not started: tests drive the state
// machine and feed Login replies by hand.
type core struct {
	t     *testing.T
	g     *Gateway
	loops []*fakeLoop
	clock *fakeClock
}

func newCore(t *testing.T, mod func(*Config)) *core {
	t.Helper()
	cfg := testConfig()
	if mod != nil {
		mod(&cfg)
	}
	clock := &fakeClock{}
	reg := prometheus.NewRegistry()
	g, err := New(cfg, Deps{Metrics: NewMetrics(reg), Registry: reg, Clock: clock.now})
	if err != nil {
		t.Fatal(err)
	}
	h := &core{t: t, g: g, clock: clock}
	for range cfg.NumLoops {
		h.loops = append(h.loops, newFakeLoop())
	}
	t.Cleanup(func() {
		for _, l := range h.loops {
			l.close()
		}
	})
	return h
}

// open accepts a connection on loop li; it returns nil link on a reject.
func (h *core) open(li int) (*fakeLink, string) {
	l := h.loops[li]
	k := &fakeLink{g: h.g, loop: l}
	var reason string
	l.do(func() {
		k.c, reason = h.g.Open(l, k)
	})
	if reason != "" {
		return nil, reason
	}
	return k, ""
}

// authed opens a connection and completes its handshake.
func (h *core) authed(li int) *fakeLink {
	h.t.Helper()
	k, reason := h.open(li)
	if reason != "" {
		h.t.Fatalf("open rejected: %s", reason)
	}
	k.loop.do(func() { h.g.HandshakeDone(k.c) })
	return k
}

func (h *core) frame(k *fakeLink, cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	var body []byte
	if msg != nil {
		var err error
		if body, err = proto.Marshal(msg); err != nil {
			h.t.Fatal(err)
		}
	}
	k.loop.do(func() { h.g.Frame(k.c, cmd, reqID, body) })
}

// login sends a LoginReq for uid and returns the queued item.
func (h *core) login(k *fakeLink, uid string) *loginItem {
	h.t.Helper()
	h.frame(k, clientpb.Cmd_CMD_LOGIN, 7, loginReq(uid, "dev-"+uid, 1))
	select {
	case it := <-h.g.loginCh:
		return it
	case <-time.After(time.Second):
		h.t.Fatal("LoginReq was not queued")
		return nil
	}
}

// reply hands a worker's reply to the loop, as post does.
func (h *core) reply(it *loginItem, r loginReply) {
	it.ls.loop().(*fakeLoop).do(func() { h.g.applyLogin(it, r) })
}

// online logs a connection in with an OK reply.
func (h *core) online(li int, uid string) *fakeLink {
	h.t.Helper()
	k := h.authed(li)
	it := h.login(k, uid)
	it.state.Store(itemSent)
	h.reply(it, loginReply{outcome: loginOK, label: "ok", uid: uid, tokIatMs: 1})
	if k.c.state != stateOnline {
		h.t.Fatalf("state %s, want online", stateLabel[k.c.state])
	}
	return k
}

// advance moves the clock and runs every loop's wheel up to it.
func (h *core) advance(d time.Duration) {
	h.clock.jump(d)
	for i, l := range h.loops {
		ls := h.g.loops.states[i]
		l.do(func() { ls.wheel.advance(h.g.now()) })
	}
	h.sync()
}

// sync waits until every loop has run what was queued before it.
func (h *core) sync() {
	for _, l := range h.loops {
		l.do(func() {})
	}
}

// onLoop reads loop state on its goroutine.
func onLoop[T any](k *fakeLink, fn func(c *Conn) T) T {
	var v T
	k.loop.do(func() { v = fn(k.c) })
	return v
}

func (h *core) tracked(k *fakeLink) bool {
	return onLoop(k, func(c *Conn) bool { _, ok := c.ls.conns[c.id]; return ok })
}

// takeLogout returns the next queued Logout, or nil.
func (h *core) takeLogout() *logoutItem {
	select {
	case it := <-h.g.logoutCh:
		return it
	default:
		return nil
	}
}

func loginReq(uid, device string, seq uint64) *clientpb.LoginReq {
	return &clientpb.LoginReq{Token: fakeToken(uid), Appid: "app", Platform: clientpb.Platform_PLATFORM_MOBILE,
		DeviceId: device, ConnSeq: seq, ReconnectReason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}
}

// fakeToken is a JWS-shaped token with the given sub; only Logic verifies.
func fakeToken(sub string) string {
	return "eyJhbGciOiJFZERTQSJ9." + base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"`+sub+`"}`)) + ".c2ln"
}

// waitFor polls cond for up to 5s.
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
```

`internal/gateway/conn_test.go`:

```go
package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func rejects(h *core, reason string) int {
	return int(testutil.ToFloat64(h.g.m.Rejects.WithLabelValues(reason)))
}

func TestConnectionCapCountsSocketsNotEntriesAwaitingLogout(t *testing.T) {
	h := newCore(t, func(c *Config) { c.MaxConns = 3 })
	a := h.online(0, "ua")
	h.online(1, "ub")
	h.authed(2)
	if _, reason := h.open(3); reason != RejectAtCapacity {
		t.Fatalf("4th socket: reject %q, want at_capacity", reason)
	}
	if n := rejects(h, RejectAtCapacity); n != 1 {
		t.Fatalf("at_capacity counted %d times", n)
	}
	a.clientClose()
	h.sync()
	if !h.tracked(a) {
		t.Fatal("a closed ONLINE connection must stay tracked until its Logout is settled")
	}
	if _, reason := h.open(3); reason != "" {
		t.Fatalf("after a close, open rejected with %q: an entry awaiting Logout must not count", reason)
	}
}

func TestPendingCapCountsHandshakeAndAuthPendingOnly(t *testing.T) {
	h := newCore(t, func(c *Config) { c.PendingCap = 2 })
	h.online(0, "ua") // ONLINE: not pending
	h.open(0)         // HANDSHAKE
	h.authed(1)       // AUTH_PENDING
	if _, reason := h.open(2); reason != RejectPendingMem {
		t.Fatalf("reject %q, want pending_mem", reason)
	}
	if got := h.g.open.Load(); got != 3 {
		t.Fatalf("open sockets %d after a pending_mem reject, want 3", got)
	}
}

func TestHandshakeTimeoutClosesSilently(t *testing.T) {
	h := newCore(t, nil)
	k, _ := h.open(0)
	h.advance(4 * time.Second)
	if k.isClosed() {
		t.Fatal("closed before the 5s handshake timeout")
	}
	h.advance(2 * time.Second)
	if !k.isClosed() || len(k.sent()) != 0 {
		t.Fatalf("closed=%v frames=%d, want a silent close", k.isClosed(), len(k.sent()))
	}
	h.sync()
	if rejects(h, RejectHandshakeTimeout) != 1 || h.tracked(k) {
		t.Fatal("want one handshake_timeout reject and the entry untracked")
	}
}

func TestAuthTimeoutClosesSilently(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	h.advance(6 * time.Second)
	if !k.isClosed() || len(k.sent()) != 0 || rejects(h, RejectAuthTimeout) != 1 {
		t.Fatal("want a silent close counted as auth_timeout")
	}
}

func TestQueuedLoginIsNotClosedByAuthTimeout(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	h.login(k, "u1") // queued, no reply
	h.advance(5500 * time.Millisecond)
	if k.isClosed() || rejects(h, RejectAuthTimeout) != 0 {
		t.Fatal("a Login waiting past the AUTH_PENDING timeout was closed by it (R4H-O7)")
	}
}

func TestHeartbeatReArmsOnActivityAndFires310sAfterTheLastFrame(t *testing.T) {
	h := newCore(t, nil)
	k := h.online(0, "u1")
	for range 3 { // 3 × 200s with a Ping each time: alive for 600s
		h.advance(200 * time.Second)
		h.frame(k, clientpb.Cmd_CMD_PING, 9, nil)
	}
	if k.isClosed() {
		t.Fatal("closed while frames kept arriving")
	}
	if !k.last(t, clientpb.Cmd_CMD_PONG, nil) {
		t.Fatal("Ping got no Pong")
	}
	h.advance(309 * time.Second)
	if k.isClosed() {
		t.Fatal("closed 309s after the last frame")
	}
	h.advance(2 * time.Second)
	if !k.isClosed() {
		t.Fatal("still open 311s after the last frame")
	}
}

func TestProtocolViolationsClose(t *testing.T) {
	h := newCore(t, nil)
	for name, run := range map[string]func() *fakeLink{
		"Ping before Login": func() *fakeLink {
			k := h.authed(0)
			h.frame(k, clientpb.Cmd_CMD_PING, 1, nil)
			return k
		},
		"undecodable LoginReq": func() *fakeLink {
			k := h.authed(0)
			k.loop.do(func() { h.g.Frame(k.c, clientpb.Cmd_CMD_LOGIN, 1, []byte{0xff, 0xff}) })
			return k
		},
		"second LoginReq": func() *fakeLink {
			k := h.authed(0)
			h.login(k, "u2")
			h.frame(k, clientpb.Cmd_CMD_LOGIN, 2, loginReq("u2", "d", 2))
			return k
		},
		"LoginReq while ONLINE": func() *fakeLink {
			k := h.online(0, "u3")
			h.frame(k, clientpb.Cmd_CMD_LOGIN, 2, loginReq("u3", "d", 2))
			return k
		},
		"frame before the handshake": func() *fakeLink {
			k, _ := h.open(0)
			h.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u4", "d", 1))
			return k
		},
	} {
		if k := run(); !k.isClosed() {
			t.Errorf("%s: not closed", name)
		}
	}
}

func TestLoginRepliesReachTheClient(t *testing.T) {
	h := newCore(t, func(c *Config) {
		c.Windows.StormWait = Window{0, 40 * time.Second}
	})
	t.Run("ok", func(t *testing.T) {
		k := h.online(0, "ok")
		var resp clientpb.LoginResp
		if !k.last(t, clientpb.Cmd_CMD_LOGIN_RESP, &resp) {
			t.Fatal("no LoginResp")
		}
		if resp.HeartbeatS != 150 || resp.StormWait.GetHiMs() != 40_000 || resp.LocalTrigger != nil {
			t.Fatalf("LoginResp %v: want heartbeat 150, storm hi 40s, local trigger absent", &resp)
		}
		if k.sent()[0].reqID != 7 {
			t.Fatal("LoginResp does not echo the LoginReq's req_id")
		}
	})
	t.Run("stale", func(t *testing.T) {
		k := h.authed(1)
		it := h.login(k, "st")
		it.state.Store(itemSent)
		h.reply(it, loginReply{outcome: loginRejected, code: clientpb.ErrorCode_ERROR_CODE_STALE, label: "stale", storedConnSeq: 42})
		var e clientpb.Error
		if !k.last(t, clientpb.Cmd_CMD_ERROR, &e) || e.Code != clientpb.ErrorCode_ERROR_CODE_STALE || e.StoredConnSeq != 42 {
			t.Fatalf("Error %v, want STALE with stored conn_seq 42", &e)
		}
		if !k.isClosed() {
			t.Fatal("a rejected Login must close the connection")
		}
	})
	t.Run("no definitive reply", func(t *testing.T) {
		k := h.authed(2)
		it := h.login(k, "un")
		it.state.Store(itemSent)
		h.reply(it, unknownReply)
		var b clientpb.Busy
		if !k.last(t, clientpb.Cmd_CMD_BUSY, &b) || b.RetryAfterMs < 1000 || b.RetryAfterMs > 3000 {
			t.Fatalf("Busy %v, want retry_after in [1s, 3s]", &b)
		}
		if !k.isClosed() {
			t.Fatal("not closed")
		}
	})
}

func TestLoginQueueCapRepliesBusy(t *testing.T) {
	h := newCore(t, func(c *Config) { c.LoginQueueCap = 2 })
	h.login(h.authed(0), "a")
	h.login(h.authed(1), "b")
	k := h.authed(2)
	h.frame(k, clientpb.Cmd_CMD_LOGIN, 3, loginReq("c", "dc", 1))
	if !k.last(t, clientpb.Cmd_CMD_BUSY, nil) || !k.isClosed() || rejects(h, RejectLoginQueue) != 1 {
		t.Fatal("the third Login must get Busy, close, and count login_queue")
	}
	h.sync()
	if h.tracked(k) {
		t.Fatal("a connection that never sent a Login needs no Logout")
	}
}

func TestClientLogoutRepliesAtOnceThenLogsOut(t *testing.T) {
	h := newCore(t, nil)
	k := h.online(0, "u1")
	h.frame(k, clientpb.Cmd_CMD_LOGOUT, 5, nil)
	if !k.last(t, clientpb.Cmd_CMD_LOGOUT_RESP, nil) || !k.isClosed() {
		t.Fatal("want LogoutResp and a close")
	}
	h.sync()
	it := h.takeLogout()
	if it == nil || it.entry.Uid != "u1" || it.entry.Appid != "app" || it.connID != k.c.id {
		t.Fatalf("Logout %+v, want u1/app for this conn_id", it)
	}
}

// ---- Logout fence (a) and (b) ----

func TestFenceAHoldsLogoutUntilTheLoginReply(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reply      loginReply
		wantLogout bool
	}{
		{"ok", loginReply{outcome: loginOK, label: "ok", uid: "u1"}, true},
		{"rejected", loginReply{outcome: loginRejected, code: clientpb.ErrorCode_ERROR_CODE_SUPERSEDED, label: "superseded"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCore(t, nil)
			k := h.authed(0)
			it := h.login(k, "u1")
			it.state.Store(itemSent) // the worker sent it
			k.clientClose()          // race 5: close while the Login is in flight
			h.sync()
			if h.takeLogout() != nil {
				t.Fatal("Logout sent before the definitive Login reply")
			}
			h.reply(it, tc.reply)
			got := h.takeLogout()
			if (got != nil) != tc.wantLogout {
				t.Fatalf("Logout queued = %v, want %v", got != nil, tc.wantLogout)
			}
			if !tc.wantLogout && h.tracked(k) {
				t.Fatal("a rejected Login wrote nothing: the entry must be untracked")
			}
		})
	}
}

func TestFenceBReleasesLogoutAfterLoginDeadlinePlusOneSecond(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	it := h.login(k, "u6")
	it.state.Store(itemSent)
	h.reply(it, unknownReply) // race 6: no definitive reply; the client gets Busy
	h.sync()
	h.advance(5900 * time.Millisecond)
	if h.takeLogout() != nil {
		t.Fatal("Logout before recv_ms + login_deadline + 1s")
	}
	h.advance(1100 * time.Millisecond)
	got := h.takeLogout()
	if got == nil {
		t.Fatal("no Logout after fence (b)")
	}
	if got.entry.Uid != "u6" {
		t.Fatalf("Logout uid %q, want the token's sub u6", got.entry.Uid)
	}
	if !h.tracked(k) {
		t.Fatal("untracked before its Logout settled")
	}
}

func TestCloseWhileQueuedNeedsNoLogout(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	it := h.login(k, "u1")
	k.clientClose()
	h.sync()
	if it.state.Load() != itemCancelled || h.tracked(k) || h.g.loginQueued.Load() != 0 {
		t.Fatal("a Login cancelled before it was sent must free its slot and leave nothing tracked")
	}
	h.advance(10 * time.Second)
	if h.takeLogout() != nil {
		t.Fatal("Logout for a Login that never reached Logic")
	}
}

func TestUnreadableTokenNeedsNoLogout(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	h.frame(k, clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: "garbage", Appid: "app", DeviceId: "d"})
	it := <-h.g.loginCh
	it.state.Store(itemSent)
	k.clientClose()
	h.advance(7 * time.Second)
	if h.takeLogout() != nil || h.tracked(k) {
		t.Fatal("a token without a readable sub can't have been written: no Logout, untracked")
	}
}

func TestTokenSubject(t *testing.T) {
	for tok, want := range map[string]string{
		fakeToken("u-1"):                        "u-1",
		"a.b":                                   "",
		"onlyone":                               "",
		"a.!!!.c":                               "",
		"a." + strings.Repeat("A", 5000) + ".c": "",
	} {
		if got := tokenSubject(tok); got != want {
			t.Errorf("tokenSubject(%.20q) = %q, want %q", tok, got, want)
		}
	}
}
```

`internal/gateway/layer_test.go`:

```go
package gateway

import (
	"os/exec"
	"strings"
	"testing"
)

// The layer rule: the gateway never touches Redis; only Logic does.
func TestGatewayDoesNotImportTheStore(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/Tallinzp/goim/internal/gateway").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	for _, dep := range strings.Fields(string(out)) {
		if dep == "github.com/Tallinzp/goim/internal/store" || strings.HasPrefix(dep, "github.com/redis/") {
			t.Fatalf("internal/gateway depends on %s", dep)
		}
	}
}
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/gateway/`
Expected: FAIL, `undefined: New`, `undefined: Deps`, `undefined: loginReply`, `undefined: logoutItem`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/gateway.go`:

```go
package gateway

import (
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
)

// Gateway is one gateway instance (pod name + boot epoch). A transport calls
// Open, HandshakeDone, Frame and Closed on the connection's loop goroutine.
type Gateway struct {
	cfg   Config
	m     *Metrics
	now   func() time.Time // wall clock: recv_ms, sent_ms, as_of_ms, fences
	ref   *pb.GatewayRef
	loops *loops
	log   *slog.Logger

	open        atomic.Int64 // sockets in any state (connection cap)
	pending     atomic.Int64 // HANDSHAKE + AUTH_PENDING sockets (pending cap)
	tracked     atomic.Int64 // table entries
	loginQueued atomic.Int64 // Logins waiting for or inside a Login RPC
	draining    atomic.Bool

	loginCh  chan *loginItem
	logoutCh chan *logoutItem
	gauges   [3]stateGauges
}

type stateGauges struct{ inc, dec func() }

// Deps are what a Gateway is built with; tests pass fakes.
type Deps struct {
	Metrics  *Metrics             // registered on Registry (NewMetrics)
	Registry *prometheus.Registry // served on /metrics
	// Clock is the wall clock (nil: time.Now). Plan 5 injects a skewed one.
	Clock func() time.Time
	// ReadyChecks are the transport's readiness conditions, e.g. the Noise
	// key set is loaded (Plan 4).
	ReadyChecks []func() error
}

// New builds the gateway core. Call Start to connect it to Logic.
func New(cfg Config, d Deps) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	now, m := d.Clock, d.Metrics
	if now == nil {
		now = time.Now
	}
	g := &Gateway{cfg: cfg, m: m, now: now,
		ref:      &pb.GatewayRef{Gateway: cfg.GatewayID, Epoch: cfg.Epoch},
		loops:    newLoops(cfg, now()),
		log:      slog.Default().With("gw", cfg.GatewayID, "epoch", cfg.Epoch),
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
	}
	return g, nil
}

// Open admits a new socket on loop l (spec: Admission control). It returns
// the reject reason, already counted, when the transport must close the
// socket before any crypto.
func (g *Gateway) Open(l Loop, link Link) (*Conn, string) {
	ls := g.loops.bind(l)
	switch {
	case g.draining.Load():
		return nil, g.reject(RejectDraining)
	case g.open.Add(1) > int64(g.cfg.MaxConns):
		g.open.Add(-1)
		return nil, g.reject(RejectAtCapacity)
	case g.pending.Add(1) > int64(g.cfg.PendingCap):
		g.pending.Add(-1)
		g.open.Add(-1)
		return nil, g.reject(RejectPendingMem)
	}
	c := &Conn{id: ls.nextID(), ls: ls, link: link, state: stateHandshake}
	c.onLife = func() { g.lifeExpired(c) }
	c.onFence = func() { g.fenceExpired(c) }
	ls.conns[c.id] = c
	g.tracked.Add(1)
	g.m.Tracked.Inc()
	g.gauges[stateHandshake].inc()
	ls.wheel.schedule(&c.life, g.now().Add(g.cfg.HandshakeTimeout), c.onLife)
	return c, ""
}

func (g *Gateway) reject(reason string) string {
	g.m.Rejects.WithLabelValues(reason).Inc()
	return reason
}

// HandshakeDone moves c from HANDSHAKE to AUTH_PENDING.
func (g *Gateway) HandshakeDone(c *Conn) {
	if c.state != stateHandshake || c.closing {
		return
	}
	g.setState(c, stateAuthPending)
	c.ls.wheel.schedule(&c.life, g.now().Add(g.cfg.AuthTimeout), c.onLife)
}

// Frame handles one decoded frame from the client.
func (g *Gateway) Frame(c *Conn, cmd clientpb.Cmd, reqID uint32, body []byte) {
	if c.closing || c.state == stateClosed {
		return
	}
	switch c.state {
	case stateAuthPending:
		// AUTH_PENDING accepts one LoginReq and nothing else.
		if cmd != clientpb.Cmd_CMD_LOGIN || c.outcome != loginNone || c.login != nil {
			g.closeConn(c, "protocol")
			return
		}
		g.loginFrame(c, reqID, body)
	case stateOnline:
		c.lastActive = g.now().UnixMilli() // any frame resets the heartbeat, lazily
		switch cmd {
		case clientpb.Cmd_CMD_PING:
			c.link.Send(clientpb.Cmd_CMD_PONG, reqID, nil)
		case clientpb.Cmd_CMD_LOGOUT:
			// Reply at once; the Logout RPC goes through the fence (R4H-O8).
			c.link.Send(clientpb.Cmd_CMD_LOGOUT_RESP, reqID, nil)
			g.closeConn(c, "logout")
		default:
			g.closeConn(c, "protocol")
		}
	default: // HANDSHAKE: the transport delivers no frames before the handshake
		g.closeConn(c, "protocol")
	}
}

// Closed is called once per accepted connection, after its socket closed.
func (g *Gateway) Closed(c *Conn) {
	if c.state == stateClosed {
		return
	}
	if !c.closing {
		g.m.Closes.WithLabelValues("client").Inc()
	}
	prev := c.state
	g.gauges[prev].dec()
	g.open.Add(-1)
	if prev == stateHandshake || prev == stateAuthPending {
		g.pending.Add(-1)
	}
	c.state, c.closing, c.link = stateClosed, false, nil
	c.ls.wheel.stop(&c.life)
	g.m.AwaitingLogout.Inc()
	if c.outcome == loginPending && c.login.state.CompareAndSwap(itemQueued, itemCancelled) {
		// Never sent: it can't reach Logic, so it needs no Logout.
		c.outcome, c.login = loginNone, nil
		c.ls.wheel.stop(&c.fence)
		g.loginFinished()
	}
	g.settle(c)
}

func (g *Gateway) setState(c *Conn, s state) {
	g.gauges[c.state].dec()
	g.gauges[s].inc()
	if c.state == stateAuthPending && s == stateOnline {
		g.pending.Add(-1)
	}
	c.state = s
}

// closeConn asks the transport to close c; Closed follows.
func (g *Gateway) closeConn(c *Conn, reason string) {
	if c.closing || c.state == stateClosed {
		return
	}
	c.closing = true
	g.m.Closes.WithLabelValues(reason).Inc()
	c.link.Close()
}

func (g *Gateway) send(c *Conn, cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	body, err := proto.Marshal(msg)
	if err != nil {
		g.log.Error("marshal client frame", "cmd", cmd, "err", err)
		return
	}
	c.link.Send(cmd, reqID, body)
}

// lifeExpired is c's HANDSHAKE, AUTH_PENDING or heartbeat timer (spec:
// Timeouts, Heartbeat). Timeouts close silently.
func (g *Gateway) lifeExpired(c *Conn) {
	if c.closing {
		return
	}
	switch c.state {
	case stateHandshake:
		g.reject(RejectHandshakeTimeout)
		g.closeConn(c, "timeout")
	case stateAuthPending:
		if c.outcome == loginNone { // the timer stops when LoginReq arrives
			g.reject(RejectAuthTimeout)
			g.closeConn(c, "timeout")
		}
	case stateOnline:
		dead := time.UnixMilli(c.lastActive).Add(g.cfg.DeadAfter())
		if g.now().Before(dead) {
			c.ls.wheel.schedule(&c.life, dead, c.onLife) // lazy re-arm
			return
		}
		g.closeConn(c, "heartbeat")
	}
}

// busy sends Busy{retry_after}, uniform in BusyRetry (R4G-O12), and closes.
func (g *Gateway) busy(c *Conn, reqID uint32) {
	w := g.cfg.BusyRetry
	after := w.Lo + rand.N(w.Hi-w.Lo+1)
	g.send(c, clientpb.Cmd_CMD_BUSY, reqID, &clientpb.Busy{RetryAfterMs: uint32(after.Milliseconds())})
	g.closeConn(c, "busy")
}

// kickConn sends the Kick frame and closes (spec: Kick handling).
func (g *Gateway) kickConn(c *Conn, reason clientpb.KickReason, metric string) {
	g.send(c, clientpb.Cmd_CMD_KICK, 0, &clientpb.Kick{Reason: reason})
	g.closeConn(c, metric)
}

// ---- Logout fence (spec: Logout fence) ----

// fenceExpired is fence (b): recv_ms + login_deadline + 1s has passed, so no
// write for this Login can still land.
func (g *Gateway) fenceExpired(c *Conn) {
	c.fenceB = true
	g.settle(c)
}

// settle decides what a closed connection still needs: nothing (untrack it),
// a fenced Logout, or more waiting. It runs whenever a fence condition may
// have changed.
func (g *Gateway) settle(c *Conn) {
	if c.state != stateClosed || c.logoutQueued {
		return
	}
	if c.inChunk { // fence (c): its Reconcile chunk is unresolved
		return
	}
	uid := g.logoutUID(c)
	if uid == "" || (c.outcome != loginPending && c.outcome != loginOK && c.outcome != loginUnknown) {
		g.untrack(c) // no Login reached Logic, or it definitively wrote nothing
		return
	}
	if c.outcome == loginOK || c.fenceB { // fence (a) or (b)
		c.logoutQueued = true
		g.queueLogout(c, uid)
	}
}

// logoutUID is the uid a Logout for c must name: Logic's verified uid, or,
// with no definitive reply, the token's unverified sub claim. A wrong sub only
// makes the compare-and-delete miss; an unreadable token can't have been
// accepted, so it needs no Logout.
func (g *Gateway) logoutUID(c *Conn) string {
	if c.uid != "" || c.login == nil {
		return c.uid
	}
	c.uid = tokenSubject(c.login.entry.GetToken())
	return c.uid
}

// untrack removes a closed connection from its loop's table: it has passed
// its Logout fence for good (spec: Tracked connections).
func (g *Gateway) untrack(c *Conn) {
	if _, ok := c.ls.conns[c.id]; !ok {
		return
	}
	delete(c.ls.conns, c.id)
	c.ls.wheel.stop(&c.fence)
	c.login = nil
	g.tracked.Add(-1)
	g.m.Tracked.Dec()
	g.m.AwaitingLogout.Dec()
}
```

`internal/gateway/login.go` (Task 6 appends the batcher and the RPC):

```go
package gateway

import (
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/protobuf/proto"
)

// loginReply is what a Login worker learned about one entry.
type loginReply struct {
	outcome       outcome // loginOK, loginRejected or loginUnknown
	code          clientpb.ErrorCode
	label         string // logins_total result
	uid           string
	tokIatMs      int64
	storedConnSeq uint64
}

// unknownReply is a Login with no definitive reply: fence (b) applies.
var unknownReply = loginReply{outcome: loginUnknown, label: "error"}

// loginFrame admits a LoginReq (spec: Login-queue cap, recv_ms) and queues it
// for the batcher.
func (g *Gateway) loginFrame(c *Conn, reqID uint32, body []byte) {
	var req clientpb.LoginReq
	if err := proto.Unmarshal(body, &req); err != nil {
		g.closeConn(c, "protocol")
		return
	}
	c.ls.wheel.stop(&c.life) // AUTH_PENDING ends at LoginReq; login_deadline governs from here (R4H-O7)
	c.loginReqID = reqID
	if g.loginQueued.Add(1) > int64(g.cfg.LoginQueueCap) {
		g.loginQueued.Add(-1)
		g.reject(RejectLoginQueue)
		g.m.Logins.WithLabelValues("busy").Inc()
		g.busy(c, reqID)
		return
	}
	g.m.LoginQueue.Inc()
	now := g.now()
	c.recvMs = now.UnixMilli()
	c.appid, c.platform = req.GetAppid(), pb.Platform(req.GetPlatform())
	c.deviceID, c.connSeq = req.GetDeviceId(), req.GetConnSeq()
	c.reason, c.offlineMs = req.GetReconnectReason(), req.GetOfflineMs()
	it := &loginItem{ls: c.ls, connID: c.id, entry: &pb.LoginEntry{
		ConnId: c.id, Token: req.GetToken(), Appid: c.appid, Platform: c.platform,
		DeviceId: c.deviceID, ConnSeq: c.connSeq, RecvMs: c.recvMs}}
	c.login, c.outcome = it, loginPending
	c.ls.wheel.schedule(&c.fence, now.Add(g.cfg.FenceAfter()), c.onFence)
	select {
	case g.loginCh <- it:
	default: // unreachable: the channel holds LoginQueueCap items
		it.state.Store(itemCancelled)
		c.login, c.outcome = nil, loginNone
		g.loginFinished()
		g.busy(c, reqID)
	}
}

// loginFinished releases one Login-queue slot.
func (g *Gateway) loginFinished() {
	g.loginQueued.Add(-1)
	g.m.LoginQueue.Dec()
}

// applyLogin runs on the loop with a worker's reply for it.
func (g *Gateway) applyLogin(it *loginItem, r loginReply) {
	c := it.ls.conns[it.connID]
	if c == nil || c.login != it || c.outcome != loginPending {
		return
	}
	g.m.Logins.WithLabelValues(r.label).Inc()
	open := c.state == stateAuthPending && !c.closing
	switch r.outcome {
	case loginOK: // fence (a)
		c.outcome, c.login, c.uid, c.tokIatMs = loginOK, nil, r.uid, r.tokIatMs
		c.ls.wheel.stop(&c.fence)
		if open {
			if c.kickPending != clientpb.KickReason_KICK_REASON_UNSPECIFIED {
				g.kickConn(c, c.kickPending, "kick")
			} else {
				g.online(c)
			}
		}
	case loginRejected: // fence (a); nothing was written
		c.outcome, c.login = loginRejected, nil
		c.ls.wheel.stop(&c.fence)
		if open {
			g.send(c, clientpb.Cmd_CMD_ERROR, c.loginReqID, &clientpb.Error{Code: r.code, StoredConnSeq: r.storedConnSeq})
			g.closeConn(c, "login_rejected")
		}
	default: // no definitive reply: the Logout waits for fence (b)
		c.outcome = loginUnknown
		if open {
			g.busy(c, c.loginReqID)
		}
	}
	g.settle(c)
}

// online completes a Login: LoginResp, then ONLINE with the heartbeat armed.
func (g *Gateway) online(c *Conn) {
	now := g.now()
	g.setState(c, stateOnline)
	c.lastActive = now.UnixMilli()
	g.send(c, clientpb.Cmd_CMD_LOGIN_RESP, c.loginReqID, g.loginResp())
	c.ls.wheel.schedule(&c.life, now.Add(g.cfg.DeadAfter()), c.onLife)
	took := float64(now.UnixMilli()-c.recvMs) / 1000
	g.m.LoginSeconds.Observe(took)
	g.m.ReloginSeconds.WithLabelValues(c.reason.String()).Observe(float64(c.offlineMs)/1000 + took)
	if g.draining.Load() {
		g.reconnect(c)
	}
}

func (g *Gateway) loginResp() *clientpb.LoginResp {
	w := g.cfg.Windows
	return &clientpb.LoginResp{HeartbeatS: uint32(g.cfg.Heartbeat / time.Second),
		StormWait: w.StormWait.pb(), LocalTrigger: w.LocalTrigger.pb(),
		PreHandshakeRetry: w.PreHandshakeRetry.pb(), Retryable: w.Retryable.pb()}
}

func (w Window) pb() *clientpb.Window {
	if w == (Window{}) {
		return nil // absent: the SDK keeps its current value
	}
	return &clientpb.Window{LoMs: uint32(w.Lo.Milliseconds()), HiMs: uint32(w.Hi.Milliseconds())}
}

// reconnect sends Reconnect{jitter_ms} (spec: Graceful drain); the client
// closes and reconnects elsewhere.
func (g *Gateway) reconnect(c *Conn) {
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}
```

`internal/gateway/logout.go` (Task 7 appends the sender):

```go
package gateway

import (
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// A logoutItem is one fenced Logout. The connection stays tracked until the
// Logout is acknowledged or dropped from the retry queue (spec: Tracked
// connections).
type logoutItem struct {
	ls       *loopState
	connID   uint64
	entry    *pb.LogoutEntry
	first    time.Time // first send, for the RetryFor budget
	attempts int
}

// queueLogout hands a closed connection's Logout to the sender. It runs on
// the loop once the fence holds. A full queue drops it: the next Reconcile
// pass repairs a dropped Logout (R1-O3, R3-O1).
func (g *Gateway) queueLogout(c *Conn, uid string) {
	it := &logoutItem{ls: c.ls, connID: c.id, entry: &pb.LogoutEntry{
		ConnId: c.id, Uid: uid, Appid: c.appid, Platform: c.platform}}
	select {
	case g.logoutCh <- it:
	default:
		g.m.Logouts.WithLabelValues("dropped").Inc()
		g.untrack(c)
	}
}
```

`internal/gateway/token.go`:

```go
package gateway

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

// maxToken matches Logic's limit on a JWS compact token (roadmap choice #16).
const maxToken = 4096

// tokenSubject returns the token's sub claim without verifying anything, or
// "" if it can't be read. Only a Logout after a Login with no definitive
// reply uses it, because only Logic's reply carries the verified uid: a
// forged sub makes that compare-and-delete miss, and a token that can't be
// read was rejected by Logic, so it wrote nothing.
func tokenSubject(tok string) string {
	if len(tok) > maxToken {
		return ""
	}
	_, rest, ok := strings.Cut(tok, ".")
	if !ok {
		return ""
	}
	payload, _, ok := strings.Cut(rest, ".")
	if !ok {
		return ""
	}
	b, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(b, &claims) != nil {
		return ""
	}
	return claims.Sub
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS, including `TestGatewayDoesNotImportTheStore`.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): connection state machine, admission caps, Logout fence (a)(b)"
```

---

### Task 6: Login batching and the streamed Login RPC (R5S-O1, choices #26, #27, #33, #37)

**Files:**
- Modify: `internal/gateway/login.go` (append), `internal/gateway/gateway.go` (embed `server`)
- Create: `internal/gateway/server.go`
- Test: `internal/gateway/logic_test.go`, `internal/gateway/login_test.go`

**Interfaces:**
- Consumes: Task 5's `loginCh`, `applyLogin`, `loginFinished`, `loginReply`, `unknownReply`; `pb.LogicClient` (Plan 2).
- Produces:
  - `batch[T](ctx, in <-chan T, out chan<- []T, size int, linger time.Duration)` (Task 7 reuses it); `g.startLogin(ctx, logic pb.LogicClient)`; `g.sendLogins`; `replyFromPB(*pb.LoginResult) loginReply`; `g.post(it, r)`.
  - `ServerConfig{LogicTarget, LeaseTarget, KickAddr, AdminAddr string}`, `DefaultServerConfig()`; `(*Gateway) Start(ctx, ServerConfig) error`, `Stop()`; `server` struct (embedded in `Gateway`), `g.goRun(f)`, `g.rpcCtx(ctx, timeout) (ctx, cancel)`, `g.dial(target)`, `g.runTicker(ctx)`, `g.advanceWheels()`, `g.closeConns()`.
  - Test harness (`logic_test.go`): `fakeLogic` with replaceable `login`, `logout`, `reconcile`, `reconcileEnd`, `lease` handlers and recorded calls, `startFakeLogic(t)`, `okResult`, `allDone`, `replyAll`, `get`; `live` with `newLive(t, mod)`, `startLive(t, mod, pre)`, `fastConfig`, `loginLive`. `login_test.go`: `shortDeadline`.

Per-entry timeout: the worker keeps one `time.Timer` set to the earliest unresolved entry's `recv_ms + LoginTimeout` and resolves overdue entries as having no definitive reply. Results come from a goroutine reading the stream, so a slow entry never holds back a fast one. Before sending, each entry is checked against `login_deadline` and moved out of `itemQueued` with a compare-and-swap (sent, or dropped with `EXPIRED`).

- [ ] **Step 1: Write the failing tests**

`internal/gateway/logic_test.go`:

```go
package gateway

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

// fakeLogic serves the Logic and Lease services on two localhost listeners.
// Each handler can be replaced; the defaults accept everything.
type fakeLogic struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedLeaseServer

	mu           sync.Mutex
	login        func(*pb.LoginBatch, grpc.ServerStreamingServer[pb.LoginResult]) error
	logout       func(*pb.LogoutBatch) (*pb.LogoutReply, error)
	reconcile    func(*pb.ReconcileChunk) (*pb.ReconcileReply, error)
	reconcileEnd func(*pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error)
	lease        func(*pb.LeaseRequest) (*pb.LeaseReply, error)

	logins     []*pb.LoginBatch
	logouts    []*pb.LogoutBatch
	chunks     []*pb.ReconcileChunk
	ends       []*pb.ReconcileEndRequest
	leaseCalls []*pb.LeaseRequest
	epochEnded int

	logicAddr, leaseAddr string
}

func startFakeLogic(t *testing.T) *fakeLogic {
	t.Helper()
	f := &fakeLogic{}
	for i, reg := range []func(*grpc.Server){
		func(s *grpc.Server) { pb.RegisterLogicServer(s, f) },
		func(s *grpc.Server) { pb.RegisterLeaseServer(s, f) },
	} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s := grpc.NewServer()
		reg(s)
		go s.Serve(l)
		t.Cleanup(s.Stop)
		if i == 0 {
			f.logicAddr = l.Addr().String()
		} else {
			f.leaseAddr = l.Addr().String()
		}
	}
	return f
}

func (f *fakeLogic) set(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn()
}

func (f *fakeLogic) Login(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
	f.mu.Lock()
	f.logins = append(f.logins, b)
	h := f.login
	f.mu.Unlock()
	if h != nil {
		return h(b, s)
	}
	for i, e := range b.GetEntries() {
		if err := s.Send(okResult(i, e)); err != nil {
			return err
		}
	}
	return nil
}

func okResult(i int, e *pb.LoginEntry) *pb.LoginResult {
	return &pb.LoginResult{Index: uint32(i), Code: pb.LoginCode_LOGIN_CODE_OK, Uid: tokenSubject(e.GetToken()), TokIatMs: 1}
}

func (f *fakeLogic) Logout(_ context.Context, b *pb.LogoutBatch) (*pb.LogoutReply, error) {
	f.mu.Lock()
	f.logouts = append(f.logouts, b)
	h := f.logout
	f.mu.Unlock()
	if h != nil {
		return h(b)
	}
	return allDone(b), nil
}

func allDone(b *pb.LogoutBatch) *pb.LogoutReply {
	r := &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}
	for i := range r.Done {
		r.Done[i] = true
	}
	return r
}

func (f *fakeLogic) Reconcile(_ context.Context, c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
	f.mu.Lock()
	f.chunks = append(f.chunks, c)
	h := f.reconcile
	f.mu.Unlock()
	if h != nil {
		return h(c)
	}
	return replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN), nil
}

func replyAll(c *pb.ReconcileChunk, code pb.ReconcileCode) *pb.ReconcileReply {
	r := &pb.ReconcileReply{}
	for range c.GetEntries() {
		r.Results = append(r.Results, &pb.ReconcileResult{Code: code})
	}
	return r
}

func (f *fakeLogic) ReconcileEnd(_ context.Context, r *pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
	f.mu.Lock()
	f.ends = append(f.ends, r)
	h := f.reconcileEnd
	f.mu.Unlock()
	if h != nil {
		return h(r)
	}
	return &pb.ReconcileEndReply{}, nil
}

func (f *fakeLogic) GatewayLease(_ context.Context, r *pb.LeaseRequest) (*pb.LeaseReply, error) {
	f.mu.Lock()
	f.leaseCalls = append(f.leaseCalls, r)
	h, first := f.lease, len(f.leaseCalls) == 1
	f.mu.Unlock()
	if h != nil {
		return h(r)
	}
	return &pb.LeaseReply{Missing: first}, nil
}

func (f *fakeLogic) EpochEnded(context.Context, *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	f.mu.Lock()
	f.epochEnded++
	f.mu.Unlock()
	return &pb.EpochEndedReply{}, nil
}

// get reads recorded calls under the lock.
func get[T any](f *fakeLogic, fn func() T) T {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fn()
}

// ---- started harness ----

// live is a started Gateway on fake loops, talking to a fakeLogic.
type live struct {
	*core
	logic *fakeLogic
}

// fastConfig shrinks the background intervals so tests run quickly.
func fastConfig(c *Config) {
	c.LeaseInterval, c.LeaseTimeout, c.LeaseTTL = 100*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond
	c.Reconcile.Cycle, c.Reconcile.Jitter = time.Hour, 0 // passes only when a test triggers one
	c.RetryBackoff, c.RetryMaxDelay = 20*time.Millisecond, 50*time.Millisecond
}

func newLive(t *testing.T, mod func(*Config)) *live { t.Helper(); return startLive(t, mod, nil) }

// startLive is newLive with pre run on the Gateway before Start.
func startLive(t *testing.T, mod func(*Config), pre func(*Gateway, *fakeLogic)) *live {
	t.Helper()
	f := startFakeLogic(t)
	h := newCore(t, func(c *Config) {
		fastConfig(c)
		if mod != nil {
			mod(c)
		}
	})
	if pre != nil {
		pre(h.g, f)
	}
	sc := ServerConfig{LogicTarget: f.logicAddr, LeaseTarget: f.leaseAddr}
	if err := h.g.Start(context.Background(), sc); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.g.Stop)
	return &live{core: h, logic: f}
}

// loginLive sends a LoginReq and waits for the connection to be ONLINE.
func (l *live) loginLive(t *testing.T, li int, uid string) *fakeLink {
	t.Helper()
	k := l.authed(li)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 7, loginReq(uid, "dev-"+uid, 1))
	waitFor(t, uid+" ONLINE", func() bool { return onLoop(k, func(c *Conn) bool { return c.state == stateOnline }) })
	return k
}
```

`internal/gateway/login_test.go`:

```go
package gateway

import (
	"slices"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

func TestLoginBatchCarriesTheEntryAndGateway(t *testing.T) {
	l := newLive(t, nil)
	before := l.clock.now().UnixMilli()
	k := l.loginLive(t, 0, "u1")
	b := get(l.logic, func() *pb.LoginBatch { return l.logic.logins[0] })
	e := b.GetEntries()[0]
	if b.GetGw().GetGateway() != "gw-1" || b.GetGw().GetEpoch() != 0xE1 {
		t.Fatalf("batch gw %v", b.GetGw())
	}
	if e.GetConnId() != k.c.id || e.GetDeviceId() != "dev-u1" || e.GetConnSeq() != 1 ||
		e.GetAppid() != "app" || e.GetPlatform() != pb.Platform_PLATFORM_MOBILE {
		t.Fatalf("entry %v", e)
	}
	if e.GetRecvMs() < before || e.GetRecvMs() > l.clock.now().UnixMilli() {
		t.Fatalf("recv_ms %d is not the gateway clock at receipt", e.GetRecvMs())
	}
}

func TestLoginBatchesFillUpToTheBatchSize(t *testing.T) {
	l := newLive(t, func(c *Config) { c.LoginBatch, c.LoginLinger = 3, 300*time.Millisecond })
	var links []*fakeLink
	for i := range 7 {
		k := l.authed(i % 4)
		l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq(string(rune('a'+i)), "d", 1))
		links = append(links, k)
	}
	waitFor(t, "all ONLINE", func() bool {
		for _, k := range links {
			if !onLoop(k, func(c *Conn) bool { return c.state == stateOnline }) {
				return false
			}
		}
		return true
	})
	sizes := get(l.logic, func() []int {
		var s []int
		for _, b := range l.logic.logins {
			s = append(s, len(b.GetEntries()))
		}
		return s
	})
	slices.Sort(sizes)
	if !slices.Equal(sizes, []int{1, 3, 3}) {
		t.Fatalf("batch sizes %v, want [1 3 3]", sizes)
	}
}

func TestLoginResultsArrivePerEntry(t *testing.T) {
	l := newLive(t, func(c *Config) { c.LoginBatch, c.LoginLinger = 2, time.Second })
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			s.Send(okResult(1, b.Entries[1])) // entry 1 is on a healthy shard
			<-release                         // entry 0's shard is slow
			return s.Send(okResult(0, b.Entries[0]))
		}
	})
	slow, fast := l.authed(0), l.authed(1)
	l.frame(slow, clientpb.Cmd_CMD_LOGIN, 1, loginReq("slow", "d", 1))
	l.frame(fast, clientpb.Cmd_CMD_LOGIN, 1, loginReq("fast", "d", 1))
	waitFor(t, "fast ONLINE", func() bool { return onLoop(fast, func(c *Conn) bool { return c.state == stateOnline }) })
	if onLoop(slow, func(c *Conn) bool { return c.state != stateAuthPending }) {
		t.Fatal("the slow entry finished before its result")
	}
	close(release)
	waitFor(t, "slow ONLINE", func() bool { return onLoop(slow, func(c *Conn) bool { return c.state == stateOnline }) })
}

// shortDeadline makes the per-entry Login timeout 1.1s.
func shortDeadline(c *Config) {
	c.LoginDeadline, c.LoginTimeout = 100*time.Millisecond, 1100*time.Millisecond
}

func TestLoginEntryWithoutResultByItsTimeoutGetsBusyThenFencedLogout(t *testing.T) {
	l := newLive(t, func(c *Config) { shortDeadline(c); c.LoginBatch, c.LoginLinger = 2, time.Second })
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			s.Send(okResult(0, b.Entries[0]))
			<-s.Context().Done() // entry 1 never gets a result
			return nil
		}
	})
	ok, lost := l.authed(0), l.authed(1)
	l.frame(ok, clientpb.Cmd_CMD_LOGIN, 1, loginReq("ok", "d", 1))
	l.frame(lost, clientpb.Cmd_CMD_LOGIN, 1, loginReq("lost", "d", 1))
	waitFor(t, "lost gets Busy", func() bool { return lost.last(t, clientpb.Cmd_CMD_BUSY, nil) })
	if !onLoop(ok, func(c *Conn) bool { return c.state == stateOnline }) {
		t.Fatal("the entry with a result is not ONLINE")
	}
	// Fence (b) on the 1s wheel queues the Logout, with the token's sub as uid.
	waitFor(t, "Logout queued for lost", func() bool {
		return onLoop(lost, func(c *Conn) bool { return c.logoutQueued && c.uid == "lost" })
	})
}

func TestLoginStreamEndingEarlyIsNoDefinitiveReply(t *testing.T) {
	l := newLive(t, nil)
	l.logic.set(func() {
		l.logic.login = func(*pb.LoginBatch, grpc.ServerStreamingServer[pb.LoginResult]) error { return nil }
	})
	k := l.authed(0)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u1", "d", 1))
	waitFor(t, "Busy", func() bool { return k.last(t, clientpb.Cmd_CMD_BUSY, nil) })
	if got := onLoop(k, func(c *Conn) outcome { return c.outcome }); got != loginUnknown {
		t.Fatalf("outcome %d, want unknown (fence b)", got)
	}
}

func TestLogicCodesReachTheClient(t *testing.T) {
	l := newLive(t, nil)
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			for i, e := range b.GetEntries() {
				code := pb.LoginCode_LOGIN_CODE_REVOKED
				if tokenSubject(e.GetToken()) == "unknown" {
					code = pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN
				}
				s.Send(&pb.LoginResult{Index: uint32(i), Code: code})
			}
			return nil
		}
	})
	rev, unk := l.authed(0), l.authed(1)
	l.frame(rev, clientpb.Cmd_CMD_LOGIN, 1, loginReq("rev", "d", 1))
	l.frame(unk, clientpb.Cmd_CMD_LOGIN, 1, loginReq("unknown", "d", 1))
	var e clientpb.Error
	waitFor(t, "Error", func() bool { return rev.last(t, clientpb.Cmd_CMD_ERROR, &e) })
	if e.Code != clientpb.ErrorCode_ERROR_CODE_REVOKED {
		t.Fatalf("code %v, want REVOKED", e.Code)
	}
	waitFor(t, "Busy for OUTCOME_UNKNOWN", func() bool { return unk.last(t, clientpb.Cmd_CMD_BUSY, nil) })
}

func TestQueuedLoginPastTheDeadlineIsDroppedBeforeSending(t *testing.T) {
	l := newLive(t, func(c *Config) { shortDeadline(c); c.LoginRPCs, c.LoginBatch = 1, 1 })
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			if tokenSubject(b.Entries[0].GetToken()) == "first" {
				<-release // the only worker is busy
			}
			return s.Send(okResult(0, b.Entries[0]))
		}
	})
	first, late := l.authed(0), l.authed(1)
	l.frame(first, clientpb.Cmd_CMD_LOGIN, 1, loginReq("first", "d", 1))
	waitFor(t, "first sent", func() bool { return get(l.logic, func() int { return len(l.logic.logins) }) == 1 })
	l.frame(late, clientpb.Cmd_CMD_LOGIN, 1, loginReq("late", "d", 1))
	time.Sleep(200 * time.Millisecond) // past login_deadline while queued
	close(release)
	var e clientpb.Error
	waitFor(t, "late gets EXPIRED", func() bool { return late.last(t, clientpb.Cmd_CMD_ERROR, &e) })
	if e.Code != clientpb.ErrorCode_ERROR_CODE_EXPIRED {
		t.Fatalf("code %v, want EXPIRED", e.Code)
	}
	time.Sleep(100 * time.Millisecond)
	if n := get(l.logic, func() int { return len(l.logic.logins) }); n != 1 {
		t.Fatalf("%d Login RPCs, want 1: the late entry must never reach Logic", n)
	}
	l.sync()
	if l.tracked(late) || l.g.loginQueued.Load() != 0 {
		t.Fatal("a dropped Login needs no Logout and frees its queue slot")
	}
}
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/gateway/`
Expected: FAIL, `undefined: ServerConfig`, `h.g.Start undefined`.

- [ ] **Step 3: Write the implementation**

In `internal/gateway/login.go`:

Replace:

```go
package gateway

import (
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
```

with:

```go
package gateway

import (
	"context"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
```

Replace:

```go
func (g *Gateway) reconnect(c *Conn) {
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}
```

with:

```go
func (g *Gateway) reconnect(c *Conn) {
	g.send(c, clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: uint32(g.cfg.Drain.Jitter.Milliseconds())})
}

// ---- batching and the Login RPC ----

// batch collects items from in into slices of up to size, sending each when
// it is full or linger after its first item (spec: Login batching).
func batch[T any](ctx context.Context, in <-chan T, out chan<- []T, size int, linger time.Duration) {
	var b []T
	t := time.NewTimer(linger)
	t.Stop()
	flush := func() bool {
		t.Stop()
		select {
		case out <- b:
			b = nil
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		case it := <-in:
			b = append(b, it)
			if len(b) == 1 {
				t.Reset(linger)
			}
			if len(b) >= size && !flush() {
				return
			}
		case <-t.C:
			if len(b) > 0 && !flush() {
				return
			}
		}
	}
}

// startLogin runs the Login batcher and LoginRPCs workers.
func (g *Gateway) startLogin(ctx context.Context, logic pb.LogicClient) {
	work := make(chan []*loginItem)
	g.goRun(func() { batch(ctx, g.loginCh, work, g.cfg.LoginBatch, g.cfg.LoginLinger) })
	for range g.cfg.LoginRPCs {
		g.goRun(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case b := <-work:
					g.sendLogins(ctx, logic, b)
				}
			}
		})
	}
}

// sendLogins sends one batch and hands each entry's reply to its loop as it
// arrives (R5S-O1). An entry with no result by recv_ms + LoginTimeout, or when
// the stream ends, has no definitive reply (spec: Login batching).
func (g *Gateway) sendLogins(ctx context.Context, logic pb.LogicClient, items []*loginItem) {
	nowMs := g.now().UnixMilli()
	live := make([]*loginItem, 0, len(items))
	entries := make([]*pb.LoginEntry, 0, len(items))
	var latest int64
	for _, it := range items {
		if nowMs-it.entry.RecvMs > g.cfg.LoginDeadline.Milliseconds() {
			// Dropped before it was sent: it never reaches Logic, so it needs no Logout.
			if it.state.CompareAndSwap(itemQueued, itemDropped) {
				g.loginFinished()
				g.post(it, loginReply{outcome: loginRejected, code: clientpb.ErrorCode_ERROR_CODE_EXPIRED, label: "expired_queued"})
			}
			continue
		}
		if !it.state.CompareAndSwap(itemQueued, itemSent) {
			continue // cancelled: the loop released the slot
		}
		live = append(live, it)
		entries = append(entries, it.entry)
		latest = max(latest, it.entry.RecvMs)
	}
	if len(live) == 0 {
		return
	}
	resolved := make([]bool, len(live))
	left := len(live)
	resolve := func(i int, r loginReply) {
		if !resolved[i] {
			resolved[i] = true
			left--
			g.loginFinished()
			g.post(live[i], r)
		}
	}
	defer func() {
		for i := range live {
			resolve(i, unknownReply)
		}
	}()

	until := func(recvMs int64) time.Duration {
		return time.Duration(recvMs+g.cfg.LoginTimeout.Milliseconds()-g.now().UnixMilli()) * time.Millisecond
	}
	cctx, cancel := g.rpcCtx(ctx, until(latest))
	defer cancel()
	stream, err := logic.Login(cctx, &pb.LoginBatch{Gw: g.ref, Entries: entries})
	if err != nil {
		return
	}
	results := make(chan *pb.LoginResult)
	go func() {
		defer close(results)
		for {
			r, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case results <- r:
			case <-cctx.Done():
				return
			}
		}
	}()
	t := time.NewTimer(0)
	defer t.Stop()
	for left > 0 {
		next := time.Duration(-1)
		for i, it := range live {
			if resolved[i] {
				continue
			}
			if d := until(it.entry.RecvMs); d <= 0 {
				resolve(i, unknownReply) // per-entry timeout (R5S-O1)
			} else if next < 0 || d < next {
				next = d
			}
		}
		if left == 0 {
			return
		}
		t.Reset(next)
		select {
		case r, ok := <-results:
			if !ok {
				return // the stream ended: the rest have no definitive reply
			}
			if i := int(r.GetIndex()); i < len(live) {
				resolve(i, replyFromPB(r))
			}
		case <-t.C:
		}
	}
}

func (g *Gateway) post(it *loginItem, r loginReply) {
	_ = it.ls.loop().Execute(func() { g.applyLogin(it, r) }) // fails only at shutdown
}

var rejectCodes = map[pb.LoginCode]struct {
	code  clientpb.ErrorCode
	label string
}{
	pb.LoginCode_LOGIN_CODE_STALE:         {clientpb.ErrorCode_ERROR_CODE_STALE, "stale"},
	pb.LoginCode_LOGIN_CODE_SUPERSEDED:    {clientpb.ErrorCode_ERROR_CODE_SUPERSEDED, "superseded"},
	pb.LoginCode_LOGIN_CODE_EXPIRED:       {clientpb.ErrorCode_ERROR_CODE_EXPIRED, "expired"},
	pb.LoginCode_LOGIN_CODE_REVOKED:       {clientpb.ErrorCode_ERROR_CODE_REVOKED, "revoked"},
	pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN:   {clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN, "key_unknown"},
	pb.LoginCode_LOGIN_CODE_TOKEN_INVALID: {clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID, "token_invalid"},
	pb.LoginCode_LOGIN_CODE_BAD_REQUEST:   {clientpb.ErrorCode_ERROR_CODE_BAD_REQUEST, "bad_request"},
}

// replyFromPB maps Logic's result. OUTCOME_UNKNOWN, and any code this gateway
// doesn't know, is not a definitive reply.
func replyFromPB(r *pb.LoginResult) loginReply {
	if r.GetCode() == pb.LoginCode_LOGIN_CODE_OK && r.GetUid() != "" {
		return loginReply{outcome: loginOK, label: "ok", uid: r.GetUid(), tokIatMs: r.GetTokIatMs()}
	}
	if rc, ok := rejectCodes[r.GetCode()]; ok {
		return loginReply{outcome: loginRejected, code: rc.code, label: rc.label, storedConnSeq: r.GetStoredConnSeq()}
	}
	return unknownReply
}
```

In `internal/gateway/gateway.go`:

Replace:

```go
	loginCh  chan *loginItem
	logoutCh chan *logoutItem
	gauges   [3]stateGauges
}

type stateGauges struct{ inc, dec func() }
```

with:

```go
	loginCh  chan *loginItem
	logoutCh chan *logoutItem
	gauges   [3]stateGauges

	server // Start, Stop, readiness (server.go)
}

type stateGauges struct{ inc, dec func() }
```

Replace:

```go
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
```

with:

```go
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	g.server = server{registry: d.Registry, readyChecks: d.ReadyChecks}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
```

`internal/gateway/server.go`:

```go
package gateway

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ServerConfig is where Logic is and what this pod listens on.
type ServerConfig struct {
	LogicTarget string // Login/Logout/Reconcile channel, round_robin over Logic pods
	LeaseTarget string // the dedicated lease channel (R1-O2)
	KickAddr    string // Gateway service; Logic dials <pod>.gateway…:9100
	AdminAddr   string // /metrics, /healthz, /readyz
}

// DefaultServerConfig returns the ports Logic and its Kick client expect
// (roadmap choices #18, #22).
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		LogicTarget: "dns:///logic.im.svc.cluster.local:9000",
		LeaseTarget: "dns:///logic.im.svc.cluster.local:9001",
		KickAddr:    ":9100",
		AdminAddr:   ":9090",
	}
}

// server is the Gateway's process side: gRPC clients and servers, the
// background goroutines, readiness.
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error
	reqSeq      atomic.Uint64

	clients []*grpc.ClientConn
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Start dials Logic on two connections and starts the background
// goroutines.
func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
		cc, err := g.dial(target)
		if err != nil {
			g.closeConns()
			return err
		}
		g.clients = append(g.clients, cc)
	}
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	return nil
}

// Stop ends everything. Call Drain first for a graceful exit.
func (g *Gateway) Stop() {
	g.cancel()
	g.wg.Wait()
	g.closeConns()
}

func (g *Gateway) closeConns() {
	for _, cc := range g.clients {
		cc.Close()
	}
}

func (g *Gateway) goRun(f func()) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		f()
	}()
}

// runTicker advances every loop's wheel once per tick, through Execute
// (spec: Timers; gnet's engine-wide OnTick is not used).
func (g *Gateway) runTicker(ctx context.Context) {
	t := time.NewTicker(wheelTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.advanceWheels()
		}
	}
}

func (g *Gateway) advanceWheels() {
	for _, ls := range g.loops.bound() {
		_ = ls.loop().Execute(func() { ls.wheel.advance(g.now()) })
	}
}

// dial opens a Logic channel: round_robin over the headless service's
// addresses, fail-fast RPCs, keepalive no faster than Logic's 10s MinTime.
func (g *Gateway) dial(target string) (*grpc.ClientConn, error) {
	return grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"round_robin":{}}]}`),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 20 * time.Second, Timeout: 5 * time.Second, PermitWithoutStream: true}),
		grpc.WithChainUnaryInterceptor(g.observeUnary),
		grpc.WithChainStreamInterceptor(g.observeStream))
}

// rpcCtx bounds a call to Logic and tags it with a request ID (at most 64
// bytes: epoch in hex, then a sequence number).
func (g *Gateway) rpcCtx(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	id := fmt.Sprintf("%016x-%d", g.cfg.Epoch, g.reqSeq.Add(1))
	return context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "x-request-id", id), timeout)
}

func (g *Gateway) observeUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	g.m.RPCSecs.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return err
}

// observeStream records the time to open a stream; per-entry Login latency
// is login_seconds.
func (g *Gateway) observeStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	start := time.Now()
	s, err := streamer(ctx, desc, cc, method, opts...)
	g.m.RPCSecs.WithLabelValues(method, status.Code(err).String()).Observe(time.Since(start).Seconds())
	return s, err
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): Login batching with per-entry results and timeouts"
```

---

### Task 7: Logout sender and retry queue (choice #32)

**Files:**
- Modify: `internal/gateway/logout.go` (append), `internal/gateway/server.go` (`Start`)
- Test: `internal/gateway/logout_test.go`

**Interfaces:**
- Consumes: `batch` (Task 6), `logoutCh`, `untrack` (Task 5).
- Produces: `g.startLogout(ctx, logic)`, `g.sendLogouts`, `g.retryLogouts`, `g.untrackAll(items []*logoutItem)` (one `Execute` per loop).

- [ ] **Step 1: Write the failing test**

`internal/gateway/logout_test.go`:

```go
package gateway

import (
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func logouts(l *live, result string) int {
	return int(testutil.ToFloat64(l.g.m.Logouts.WithLabelValues(result)))
}

func TestConnectionStaysTrackedUntilItsLogoutIsAcknowledged(t *testing.T) {
	l := newLive(t, nil)
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.logout = func(b *pb.LogoutBatch) (*pb.LogoutReply, error) {
			<-release
			return allDone(b), nil
		}
	})
	k := l.loginLive(t, 0, "u1")
	k.clientClose()
	waitFor(t, "Logout sent", func() bool { return get(l.logic, func() int { return len(l.logic.logouts) }) == 1 })
	if !l.tracked(k) {
		t.Fatal("untracked while its Logout is in flight")
	}
	close(release)
	waitFor(t, "untracked", func() bool { return !l.tracked(k) })
	e := get(l.logic, func() *pb.LogoutEntry { return l.logic.logouts[0].GetEntries()[0] })
	if e.GetUid() != "u1" || e.GetConnId() != k.c.id || e.GetAppid() != "app" {
		t.Fatalf("Logout entry %v", e)
	}
}

func TestLogoutRetriesEntriesNotDone(t *testing.T) {
	l := newLive(t, nil)
	calls := 0
	l.logic.set(func() {
		l.logic.logout = func(b *pb.LogoutBatch) (*pb.LogoutReply, error) {
			calls++
			if calls == 1 {
				return &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}, nil // outcome unknown
			}
			return allDone(b), nil
		}
	})
	k := l.loginLive(t, 0, "u1")
	k.clientClose()
	waitFor(t, "untracked after the retry", func() bool { return !l.tracked(k) })
	if n := get(l.logic, func() int { return len(l.logic.logouts) }); n != 2 || logouts(l, "retried") != 1 {
		t.Fatalf("%d Logout RPCs and %d retries, want 2 and 1", n, logouts(l, "retried"))
	}
}

func TestLogoutIsDroppedWhenItsRetryBudgetRunsOut(t *testing.T) {
	l := newLive(t, func(c *Config) { c.RetryFor = 150 * time.Millisecond })
	l.logic.set(func() {
		l.logic.logout = func(*pb.LogoutBatch) (*pb.LogoutReply, error) { return nil, errors.New("Logic down") }
	})
	k := l.loginLive(t, 0, "u1")
	k.clientClose()
	waitFor(t, "dropped and untracked", func() bool { return !l.tracked(k) })
	if logouts(l, "dropped") != 1 || logouts(l, "done") != 0 {
		t.Fatal("want one dropped Logout: the next Reconcile pass repairs it")
	}
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run Logout`
Expected: FAIL: `TestConnectionStaysTrackedUntilItsLogoutIsAcknowledged` times out waiting for "Logout sent" (nothing drains `logoutCh` yet).

- [ ] **Step 3: Write the implementation**

In `internal/gateway/logout.go`:

Replace:

```go
package gateway

import (
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
```

with:

```go
package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
```

Replace:

```go
		g.untrack(c)
	}
}
```

with:

```go
		g.untrack(c)
	}
}

// startLogout runs the Logout batcher and LogoutRPCs workers.
func (g *Gateway) startLogout(ctx context.Context, logic pb.LogicClient) {
	work := make(chan []*logoutItem)
	g.goRun(func() { batch(ctx, g.logoutCh, work, g.cfg.LogoutBatch, g.cfg.LogoutLinger) })
	for range g.cfg.LogoutRPCs {
		g.goRun(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case b := <-work:
					g.sendLogouts(ctx, logic, b)
				}
			}
		})
	}
}

// sendLogouts sends one batch. An entry whose done is false, or every entry
// when the RPC fails, goes to the retry queue.
func (g *Gateway) sendLogouts(ctx context.Context, logic pb.LogicClient, items []*logoutItem) {
	now := time.Now()
	entries := make([]*pb.LogoutEntry, len(items))
	for i, it := range items {
		if it.first.IsZero() {
			it.first = now
		}
		it.attempts++
		entries[i] = it.entry
	}
	cctx, cancel := g.rpcCtx(ctx, g.cfg.RPCTimeout)
	reply, err := logic.Logout(cctx, &pb.LogoutBatch{Gw: g.ref, Entries: entries})
	cancel()
	var done, retry []*logoutItem
	for i, it := range items {
		if err == nil && i < len(reply.GetDone()) && reply.GetDone()[i] {
			done = append(done, it)
		} else {
			retry = append(retry, it)
		}
	}
	g.m.Logouts.WithLabelValues("done").Add(float64(len(done)))
	g.untrackAll(done)
	if len(retry) > 0 {
		g.retryLogouts(retry)
	}
}

// retryLogouts re-queues items after a backoff (RetryBackoff, doubling, at
// most RetryMaxDelay) while their RetryFor budget lasts, then drops them.
func (g *Gateway) retryLogouts(items []*logoutItem) {
	var again, dropped []*logoutItem
	attempts := 0
	for _, it := range items {
		attempts = max(attempts, it.attempts)
	}
	delay := min(g.cfg.RetryBackoff<<min(attempts-1, 30), g.cfg.RetryMaxDelay)
	for _, it := range items {
		if time.Since(it.first)+delay > g.cfg.RetryFor {
			dropped = append(dropped, it)
		} else {
			again = append(again, it)
		}
	}
	g.m.Logouts.WithLabelValues("dropped").Add(float64(len(dropped)))
	g.untrackAll(dropped)
	if len(again) == 0 {
		return
	}
	g.m.Logouts.WithLabelValues("retried").Add(float64(len(again)))
	time.AfterFunc(delay, func() {
		var full []*logoutItem
		for _, it := range again {
			select {
			case g.logoutCh <- it:
			default:
				full = append(full, it)
			}
		}
		g.m.Logouts.WithLabelValues("dropped").Add(float64(len(full)))
		g.untrackAll(full)
	})
}

// untrackAll ends tracking for settled Logouts, one Execute per loop.
func (g *Gateway) untrackAll(items []*logoutItem) {
	byLoop := make(map[*loopState][]uint64)
	for _, it := range items {
		byLoop[it.ls] = append(byLoop[it.ls], it.connID)
	}
	for ls, ids := range byLoop {
		_ = ls.loop().Execute(func() {
			for _, id := range ids {
				if c := ls.conns[id]; c != nil && c.state == stateClosed && c.logoutQueued {
					g.untrack(c)
				}
			}
		})
	}
}
```

In `internal/gateway/server.go`:

Replace:

```go

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	return nil
}
```

with:

```go

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): fenced Logout sender with a bounded 60s retry queue"
```

---

### Task 8: Kick server (R1-O8, race 4, race 8)

**Files:**
- Create: `internal/gateway/kick.go`
- Modify: `internal/gateway/server.go` (listeners, Kick and admin servers, `Addrs`, `Stop`), `internal/gateway/logic_test.go` (listen addresses, `kickClient`)
- Test: `internal/gateway/kick_test.go`

**Interfaces:**
- Consumes: `kickConn` (Task 5), `loops.byWorker` (Task 4), `pb.GatewayServer` (Plan 2).
- Produces: `kickServer` (registered on the Kick gRPC server), `g.kickOnLoop(ls, id, reason)`; `(*Gateway) Addrs() (kick, admin string)`; admin HTTP with `/metrics` and `/healthz` (`/readyz` comes in Task 11). Test helpers `kick(t, client, gw, epoch, connID, reason)`, `(*live) kickClient(t)`.

The handler returns as soon as the Kick is queued on the loop; Logic retries only on transport errors, and a stray Kick is harmless.

- [ ] **Step 1: Write the failing test**

`internal/gateway/kick_test.go`:

```go
package gateway

import (
	"context"
	"testing"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

func kick(t *testing.T, kc pb.GatewayClient, gw string, epoch, connID uint64, reason pb.KickReason) {
	t.Helper()
	if _, err := kc.Kick(context.Background(), &pb.KickRequest{
		Gw: &pb.GatewayRef{Gateway: gw, Epoch: epoch}, ConnId: connID, Reason: reason}); err != nil {
		t.Fatal(err)
	}
}

func TestKickOnlineSendsKickAndCloses(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	k := l.loginLive(t, 1, "u1")
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE)
	waitFor(t, "closed", k.isClosed)
	var m clientpb.Kick
	if !k.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("Kick frame %v", &m)
	}
}

func TestKickDuringAuthPendingIsAppliedWhenTheLoginReplyArrives(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			<-release
			return s.Send(okResult(0, b.Entries[0]))
		}
	})
	k := l.authed(0)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u1", "d", 1))
	waitFor(t, "Login sent", func() bool { return get(l.logic, func() int { return len(l.logic.logins) }) == 1 })
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_REPLACED) // race 8: Kick before the reply
	waitFor(t, "kick-pending", func() bool {
		return onLoop(k, func(c *Conn) bool { return c.kickPending == clientpb.KickReason_KICK_REASON_REPLACED })
	})
	if k.isClosed() {
		t.Fatal("closed before the Login reply")
	}
	close(release)
	waitFor(t, "closed", k.isClosed)
	if k.last(t, clientpb.Cmd_CMD_LOGIN_RESP, nil) || !k.last(t, clientpb.Cmd_CMD_KICK, nil) {
		t.Fatal("want a Kick frame instead of LoginResp")
	}
}

func TestKickForAnotherEpochOrUnknownConnIsIgnored(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	k := l.loginLive(t, 0, "u1")
	kick(t, kc, "gw-1", 0xE2, k.c.id, pb.KickReason_KICK_REASON_REPLACED) // race 4: zombie's epoch
	kick(t, kc, "gw-2", 0xE1, k.c.id, pb.KickReason_KICK_REASON_REPLACED)
	kick(t, kc, "gw-1", 0xE1, k.c.id+1000, pb.KickReason_KICK_REASON_REPLACED) // unknown conn_id
	kick(t, kc, "gw-1", 0xE1, 9<<48|1, pb.KickReason_KICK_REASON_REPLACED)     // worker beyond NumLoops
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_UNSPECIFIED)
	l.sync()
	if k.isClosed() {
		t.Fatal("an ignored Kick closed the connection")
	}
}
```

In `internal/gateway/logic_test.go`:

Replace:

```go
	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

// fakeLogic serves the Logic and Lease services on two localhost listeners.
```

with:

```go
	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeLogic serves the Logic and Lease services on two localhost listeners.
```

Replace:

```go
	if pre != nil {
		pre(h.g, f)
	}
	sc := ServerConfig{LogicTarget: f.logicAddr, LeaseTarget: f.leaseAddr}
	if err := h.g.Start(context.Background(), sc); err != nil {
		t.Fatal(err)
	}
```

with:

```go
	if pre != nil {
		pre(h.g, f)
	}
	sc := ServerConfig{LogicTarget: f.logicAddr, LeaseTarget: f.leaseAddr, KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}
	if err := h.g.Start(context.Background(), sc); err != nil {
		t.Fatal(err)
	}
```

Replace:

```go
	return &live{core: h, logic: f}
}

// loginLive sends a LoginReq and waits for the connection to be ONLINE.
func (l *live) loginLive(t *testing.T, li int, uid string) *fakeLink {
	t.Helper()
```

with:

```go
	return &live{core: h, logic: f}
}

// kickClient dials the gateway's Kick server.
func (l *live) kickClient(t *testing.T) pb.GatewayClient {
	t.Helper()
	addr, _ := l.g.Addrs()
	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return pb.NewGatewayClient(cc)
}

// loginLive sends a LoginReq and waits for the connection to be ONLINE.
func (l *live) loginLive(t *testing.T, li int, uid string) *fakeLink {
	t.Helper()
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run Kick`
Expected: FAIL, `l.g.Addrs undefined`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/kick.go`:

```go
package gateway

import (
	"context"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// kickServer is the Gateway service Logic calls by pod DNS (spec: Kick
// handling). It never blocks on a loop: the Kick is applied by an Execute on
// the connection's loop, found from conn_id >> 48.
type kickServer struct {
	pb.UnimplementedGatewayServer
	g *Gateway
}

func (k kickServer) Kick(_ context.Context, req *pb.KickRequest) (*pb.KickReply, error) {
	g := k.g
	// A Kick for another epoch of this pod name is ignored (race 4).
	if req.GetGw().GetGateway() != g.cfg.GatewayID || req.GetGw().GetEpoch() != g.cfg.Epoch {
		g.m.Kicks.WithLabelValues("ignored").Inc()
		return &pb.KickReply{}, nil
	}
	ls := g.loops.byWorker(req.GetConnId() >> 48)
	if ls == nil {
		g.m.Kicks.WithLabelValues("ignored").Inc()
		return &pb.KickReply{}, nil
	}
	id, reason := req.GetConnId(), clientpb.KickReason(req.GetReason())
	if ls.loop().Execute(func() { g.kickOnLoop(ls, id, reason) }) != nil {
		g.m.Kicks.WithLabelValues("ignored").Inc()
	}
	return &pb.KickReply{}, nil
}

// kickOnLoop: ONLINE → Kick frame and close; AUTH_PENDING with a Login in
// flight → kick-pending, applied when the Login reply arrives; anything else
// (HANDSHAKE, CLOSED, unknown conn_id) is ignored.
func (g *Gateway) kickOnLoop(ls *loopState, id uint64, reason clientpb.KickReason) {
	c := ls.conns[id]
	switch {
	case reason == clientpb.KickReason_KICK_REASON_UNSPECIFIED || c == nil || c.closing:
		g.m.Kicks.WithLabelValues("ignored").Inc()
	case c.state == stateOnline:
		g.kickConn(c, reason, "kick")
		g.m.Kicks.WithLabelValues("closed").Inc()
	case c.state == stateAuthPending && c.outcome == loginPending:
		c.kickPending = reason
		g.m.Kicks.WithLabelValues("pending").Inc()
	default:
		g.m.Kicks.WithLabelValues("ignored").Inc()
	}
}
```

In `internal/gateway/server.go`:

Replace:

```go

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
```

with:

```go

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
```

Replace:

```go
	reqSeq      atomic.Uint64

	clients []*grpc.ClientConn
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Start dials Logic on two connections and starts the background
// goroutines.
func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
		cc, err := g.dial(target)
```

with:

```go
	reqSeq      atomic.Uint64

	clients []*grpc.ClientConn
	kick    *grpc.Server
	admin   *http.Server
	lis     [2]net.Listener
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Start dials Logic on two connections, opens the Kick and admin listeners
// and starts the background goroutines.
func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
		cc, err := g.dial(target)
```

Replace:

```go
		}
		g.clients = append(g.clients, cc)
	}
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	return nil
}

// Stop ends everything. Call Drain first for a graceful exit.
func (g *Gateway) Stop() {
	g.cancel()
	g.wg.Wait()
	g.closeConns()
}
```

with:

```go
		}
		g.clients = append(g.clients, cc)
	}
	for i, addr := range []string{sc.KickAddr, sc.AdminAddr} {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			for _, open := range g.lis[:i] {
				open.Close()
			}
			g.closeConns()
			return err
		}
		g.lis[i] = l
	}
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])

	g.kick = grpc.NewServer()
	pb.RegisterGatewayServer(g.kick, kickServer{g: g})
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	g.goRun(func() { g.kick.Serve(g.lis[0]) })
	g.goRun(func() {
		if err := g.admin.Serve(g.lis[1]); err != nil && !errors.Is(err, http.ErrServerClosed) {
			g.log.Error("admin server", "err", err)
		}
	})

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	return nil
}

// Addrs returns the bound Kick and admin addresses.
func (g *Gateway) Addrs() (kick, admin string) {
	return g.lis[0].Addr().String(), g.lis[1].Addr().String()
}

// Stop ends everything. Call Drain first for a graceful exit.
func (g *Gateway) Stop() {
	g.cancel()
	done := make(chan struct{})
	go func() { g.kick.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		g.kick.Stop()
	}
	shut, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g.admin.Shutdown(shut)
	g.wg.Wait()
	g.closeConns()
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): Kick server with kick-pending and epoch checks"
```

---

### Task 9: Reconcile pass controller and fence (c) (R2-O1, R3-O1, R3-O6, R4H-O9, choices #29–#31)

**Files:**
- Create: `internal/gateway/reconcile.go`
- Modify: `internal/gateway/loops.go` (`chunks` table), `internal/gateway/server.go` (start the controller, wait for `ReconcileEnd`s on `Stop`)
- Test: `internal/gateway/reconcile_test.go`

**Interfaces:**
- Consumes: `settle`, `kickConn` (Task 5); `rpcCtx`, `goRun` (Task 6); wheel (Task 3).
- Produces: `reconciler` with `newReconciler(g, logic)`, `Trigger()` (Task 10 calls it), `run(ctx)`, `pass`, `snapshot`, `send`, `call`, `end`, `sendEnd`, `ends sync.WaitGroup`; `gather[T](ctx, loops, fn) ([]T, bool)` (Task 12 reuses it); `g.buildChunk`, `g.resolveChunk`; `chunkRec`, `passItem`, `chunkDone`, `built`, `flight`; server fields `recon`, `stopRecon`. Test helpers `passes(l)`, `(*live) pass(t)`, `onlineMany`, `blockChunks`, `logoutCalls`, `count`.

A pass, step by step (all table work through `Execute`, each waited on):
1. Snapshot: one `Execute` per bound loop copies its ONLINE IDs into that loop's partition.
2. Chunks: when the schedule allows and fewer than 4 are in flight, take up to 200 IDs from the next non-empty partition (round-robin), build the chunk on its loop (`buildChunk` skips IDs no longer ONLINE, sets `inChunk`, stamps `sent_ms`, arms the chunk deadline) and send it from a goroutine.
3. Resolution: a reply is applied by `resolveChunk` on the loop (clear `inChunk`, Kick `not_owner`, `settle` closed connections, report `refused`); without a reply, the chunk's wheel timer does the same at `sent_ms + login_deadline + 1s`. The loop reports each resolution on a channel; only then does the controller re-queue refused IDs (`requeues < 3`).
4. End: stamp `as_of_ms`, read the bound loops, collect every tracked ID with one `Execute` per loop, send `ReconcileEnd` from a goroutine with retries.

- [ ] **Step 1: Write the failing test**

`internal/gateway/reconcile_test.go`:

```go
package gateway

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// onlineMany logs n connections in on each loop.
func (l *live) onlineMany(t *testing.T, n int) []*fakeLink {
	t.Helper()
	var ks []*fakeLink
	for li := range l.loops {
		for i := range n {
			ks = append(ks, l.loginLive(t, li, "u"+string(rune('a'+li))+string(rune('a'+i))))
		}
	}
	return ks
}

func chunkIDs(c *pb.ReconcileChunk) []uint64 {
	var ids []uint64
	for _, e := range c.GetEntries() {
		ids = append(ids, e.GetConnId())
	}
	return ids
}

// passes counts ReconcileEnd calls.
func passes(l *live) int { return get(l.logic, func() int { return len(l.logic.ends) }) }

// pass triggers a pass and waits for its ReconcileEnd.
func (l *live) pass(t *testing.T) *pb.ReconcileEndRequest {
	t.Helper()
	n := passes(l)
	l.g.recon.Trigger()
	waitFor(t, "ReconcileEnd", func() bool { return passes(l) > n })
	return get(l.logic, func() *pb.ReconcileEndRequest { return l.logic.ends[n] })
}

func TestChunksHoldOneLoopEachAndTakeLoopsInTurn(t *testing.T) {
	// One chunk in flight at a time, so Logic sees them in the order sent.
	l := newLive(t, func(c *Config) { c.Reconcile.Chunk, c.Reconcile.MaxInFlight = 3, 1 })
	ks := l.onlineMany(t, 5)
	l.pass(t)
	chunks := get(l.logic, func() []*pb.ReconcileChunk { return l.logic.chunks })
	var workers []uint64
	seen := map[uint64]int{}
	for _, c := range chunks {
		ids := chunkIDs(c)
		for _, id := range ids {
			if id>>48 != ids[0]>>48 {
				t.Fatalf("chunk mixes loops: %x", ids)
			}
			seen[id]++
		}
		workers = append(workers, ids[0]>>48)
	}
	if len(seen) != len(ks) {
		t.Fatalf("%d connections reconciled, want %d", len(seen), len(ks))
	}
	first := slices.Clone(workers[:4])
	slices.Sort(first)
	if !slices.Equal(first, []uint64{0, 1, 2, 3}) {
		t.Fatalf("first four chunks came from loops %v, want each loop once", workers[:4])
	}
	if e := chunks[0].GetEntries()[0]; e.GetUid() == "" || e.GetLoginMs() == 0 || e.GetTokIatMs() != 1 || e.GetDeviceId() == "" {
		t.Fatalf("entry %v lacks the session fields", e)
	}
}

func TestRefusedEntriesAreRequeuedAtMostThreeTimes(t *testing.T) {
	l := newLive(t, nil)
	ks := l.onlineMany(t, 1)
	refused := ks[0].c.id
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			r := replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN)
			for i, e := range c.GetEntries() {
				if e.GetConnId() == refused {
					r.Results[i].Code = pb.ReconcileCode_RECONCILE_CODE_REFUSED
				}
			}
			return r, nil
		}
	})
	l.pass(t)
	sent := get(l.logic, func() int {
		n := 0
		for _, c := range l.logic.chunks {
			n += count(chunkIDs(c), refused)
		}
		return n
	})
	if sent != 4 || testutil.ToFloat64(l.g.m.RequeueLimit) != 1 {
		t.Fatalf("refused entry sent %d times (want 1 + 3 re-queues), limit hits %v", sent, testutil.ToFloat64(l.g.m.RequeueLimit))
	}
}

func TestRequeuedEntryNoLongerOnlineIsSkipped(t *testing.T) {
	l := newLive(t, nil)
	ks := l.onlineMany(t, 1)
	victim := ks[0]
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			r := replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN)
			for i, e := range c.GetEntries() {
				if e.GetConnId() == victim.c.id {
					r.Results[i].Code = pb.ReconcileCode_RECONCILE_CODE_REFUSED
					victim.clientClose() // gone before its re-queued chunk is built
				}
			}
			return r, nil
		}
	})
	l.pass(t)
	sent := get(l.logic, func() int {
		n := 0
		for _, c := range l.logic.chunks {
			n += count(chunkIDs(c), victim.c.id)
		}
		return n
	})
	if sent != 1 {
		t.Fatalf("closed connection sent %d times, want 1", sent)
	}
}

func TestNotOwnerIsKickedWithItsReason(t *testing.T) {
	l := newLive(t, nil)
	ks := l.onlineMany(t, 1)
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			r := replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN)
			r.Results[0] = &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER, Reason: pb.KickReason_KICK_REASON_REVOKED}
			return r, nil
		}
	})
	l.pass(t)
	for _, k := range ks {
		var m clientpb.Kick
		if !k.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_REVOKED || !k.isClosed() {
			t.Fatalf("conn %x: want a REVOKED Kick and a close", k.c.id)
		}
	}
}

// blockChunks makes Reconcile wait until the returned func is called.
func (l *live) blockChunks(fail bool) (sent <-chan time.Time, release func()) {
	ch := make(chan time.Time, 100)
	gate := make(chan struct{})
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			ch <- time.Now()
			if fail {
				return nil, errors.New("Logic crashed")
			}
			<-gate
			return replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN), nil
		}
	})
	return ch, func() { close(gate) }
}

func logoutCalls(l *live, connID uint64) int {
	return get(l.logic, func() int {
		n := 0
		for _, b := range l.logic.logouts {
			for _, e := range b.GetEntries() {
				if e.GetConnId() == connID {
					n++
				}
			}
		}
		return n
	})
}

func TestFenceCHoldsLogoutUntilTheChunkIsResolved(t *testing.T) {
	l := newLive(t, nil)
	k := l.loginLive(t, 0, "u1")
	sent, release := l.blockChunks(false)
	l.g.recon.Trigger()
	<-sent
	k.clientClose() // race 14: close after the chunk is sent
	time.Sleep(100 * time.Millisecond)
	if logoutCalls(l, k.c.id) != 0 {
		t.Fatal("Logout sent while its Reconcile chunk was unresolved")
	}
	release()
	waitFor(t, "Logout after the reply", func() bool { return logoutCalls(l, k.c.id) == 1 })
}

func TestClientLogoutDuringAChunkGetsLogoutRespAtOnce(t *testing.T) {
	l := newLive(t, nil)
	k := l.loginLive(t, 0, "u1")
	sent, release := l.blockChunks(false)
	l.g.recon.Trigger()
	<-sent
	l.frame(k, clientpb.Cmd_CMD_LOGOUT, 3, nil) // race 18
	if !k.last(t, clientpb.Cmd_CMD_LOGOUT_RESP, nil) {
		t.Fatal("LogoutResp waited for the chunk")
	}
	time.Sleep(100 * time.Millisecond)
	if logoutCalls(l, k.c.id) != 0 {
		t.Fatal("Logout RPC before the chunk was resolved")
	}
	release()
	waitFor(t, "Logout", func() bool { return logoutCalls(l, k.c.id) == 1 })
}

func TestFailedChunkIsResolvedAtSentMsPlusDeadlinePlusOneSecond(t *testing.T) {
	l := newLive(t, shortDeadline) // fence after 1.1s
	k := l.loginLive(t, 0, "u1")
	sent, _ := l.blockChunks(true)
	l.g.recon.Trigger()
	at := <-sent
	k.clientClose()
	waitFor(t, "Logout", func() bool { return logoutCalls(l, k.c.id) == 1 })
	if took := time.Since(at); took < 1100*time.Millisecond {
		t.Fatalf("Logout %v after the failed chunk was sent, want ≥ login_deadline + 1s", took)
	}
	if n := testutil.ToFloat64(l.g.m.ReconcileEntries.WithLabelValues("no_reply")); n != 1 {
		t.Fatalf("no_reply %v, want 1", n)
	}
}

func TestReconcileEndCarriesTheTrackedSetCollectedAfterAsOf(t *testing.T) {
	l := newLive(t, nil)
	held := l.loginLive(t, 0, "held")   // closed, its Logout not acknowledged
	pending := l.authed(1)              // AUTH_PENDING, no Login
	gone := l.loginLive(t, 2, "gone")   // closed and logged out
	online := l.loginLive(t, 3, "live") // ONLINE
	gone.clientClose()
	waitFor(t, "gone untracked", func() bool { return !l.tracked(gone) })
	block := make(chan struct{})
	defer close(block)
	l.logic.set(func() {
		l.logic.logout = func(b *pb.LogoutBatch) (*pb.LogoutReply, error) { <-block; return allDone(b), nil }
	})
	held.clientClose()
	waitFor(t, "held's Logout sent", func() bool { return logoutCalls(l, held.c.id) == 1 })
	before := l.clock.now().UnixMilli()
	end := l.pass(t)
	ids := end.GetTrackedConnIds()
	for _, k := range []*fakeLink{held, pending, online} {
		if !slices.Contains(ids, k.c.id) {
			t.Fatalf("tracked set %x lacks %x", ids, k.c.id)
		}
	}
	if slices.Contains(ids, gone.c.id) {
		t.Fatal("tracked set holds a connection whose Logout was acknowledged")
	}
	if end.GetAsOfMs() < before {
		t.Fatalf("as_of_ms %d before the pass started (%d)", end.GetAsOfMs(), before)
	}
}

func TestIncompleteTrackedSetSendsNoReconcileEnd(t *testing.T) {
	h := newCore(t, nil)
	h.online(0, "u1")
	h.online(1, "u2")
	r := newReconciler(h.g, nil) // a ReconcileEnd would call the nil client and panic
	h.loops[1].close()           // that loop's fragment can't be collected
	r.end(context.Background(), pb.Pace_PACE_PERIODIC)
	r.ends.Wait()
	if n := testutil.ToFloat64(h.g.m.Passes.WithLabelValues("PACE_PERIODIC", "incomplete")); n != 1 {
		t.Fatalf("incomplete passes %v, want 1", n)
	}
}

func TestReconcileEndIsRetriedWithTheSamePayload(t *testing.T) {
	l := newLive(t, nil)
	l.onlineMany(t, 1)
	calls := 0
	l.logic.set(func() {
		l.logic.reconcileEnd = func(*pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("Logic pod died mid-phase")
			}
			return &pb.ReconcileEndReply{Deleted: 2}, nil
		}
	})
	l.g.recon.Trigger()
	waitFor(t, "two ReconcileEnds", func() bool { return passes(l) == 2 })
	ends := get(l.logic, func() []*pb.ReconcileEndRequest { return l.logic.ends })
	if ends[0].GetAsOfMs() != ends[1].GetAsOfMs() || !slices.Equal(ends[0].GetTrackedConnIds(), ends[1].GetTrackedConnIds()) {
		t.Fatal("the retry changed the payload")
	}
	waitFor(t, "deleted counted", func() bool { return testutil.ToFloat64(l.g.m.EndDeleted) == 2 })
}

func TestTriggerCancelsAPeriodicPass(t *testing.T) {
	l := newLive(t, func(c *Config) {
		c.Reconcile.Cycle, c.Reconcile.Jitter = 2*time.Second, 0
		c.Reconcile.Chunk, c.Reconcile.MinRate = 2, 5 // 40 entries at 5/s: the periodic pass takes ~8s
	})
	l.onlineMany(t, 10)
	waitFor(t, "periodic pass started", func() bool { return get(l.logic, func() int { return len(l.logic.chunks) }) > 0 })
	l.g.recon.Trigger()
	waitFor(t, "triggered pass done", func() bool { return passes(l) == 1 })
	end := get(l.logic, func() *pb.ReconcileEndRequest { return l.logic.ends[0] })
	if end.GetPace() != pb.Pace_PACE_TRIGGERED {
		t.Fatalf("first ReconcileEnd has pace %v: a cancelled pass must send none", end.GetPace())
	}
	if n := testutil.ToFloat64(l.g.m.Passes.WithLabelValues("PACE_PERIODIC", "cancelled")); n != 1 {
		t.Fatalf("cancelled periodic passes %v, want 1", n)
	}
}

func TestPassRunsWhileLoopsAcceptAndClose(t *testing.T) {
	l := newLive(t, func(c *Config) { c.Reconcile.Chunk = 5 })
	l.onlineMany(t, 5)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for li := range l.loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := l.authed(li)
				l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("churn", "d", uint64(i)))
				time.Sleep(time.Millisecond)
				k.clientClose()
			}
		}()
	}
	for range 3 {
		l.pass(t)
	}
	close(stop)
	wg.Wait()
}

func count(ids []uint64, id uint64) int {
	n := 0
	for _, x := range ids {
		if x == id {
			n++
		}
	}
	return n
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run 'Chunk|Requeue|NotOwner|Fence|Reconcile|Pass|Trigger'`
Expected: FAIL, `l.g.recon undefined`, `undefined: newReconciler`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/reconcile.go`:

```go
package gateway

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// chunkRec is a Reconcile chunk sent from a loop and not yet resolved. Its
// connections carry inChunk (fence c) until it is.
type chunkRec struct {
	ids   []uint64
	timer timer // sent_ms + login_deadline + 1s
}

// passItem is one pass-queue entry: only the ID, rebuilt from the live table
// when its chunk is built (spec: Pass mechanics).
type passItem struct {
	id       uint64
	requeues uint8
}

// chunkDone tells the controller a chunk was resolved on its loop.
type chunkDone struct {
	chunk   uint64
	refused []uint64
}

// built is a chunk read from a loop's table.
type built struct {
	ids     []uint64
	entries []*pb.ReconcileEntry
	sentMs  int64
}

// flight is the controller's record of a chunk in flight.
type flight struct {
	part     int
	requeues map[uint64]uint8
}

// reconciler is the pass controller goroutine (spec: Reconcile, R4H-O9). It
// is the only code that schedules table work for a pass, always through
// Execute, so every table access stays on its loop.
type reconciler struct {
	g        *Gateway
	logic    pb.LogicClient
	trigger  chan struct{}
	resolved chan chunkDone
	chunkSeq uint64
	ends     sync.WaitGroup
}

func newReconciler(g *Gateway, logic pb.LogicClient) *reconciler {
	return &reconciler{g: g, logic: logic, trigger: make(chan struct{}, 1),
		resolved: make(chan chunkDone, g.cfg.Reconcile.MaxInFlight)}
}

// Trigger asks for a triggered pass (a missing lease reply). Calls coalesce.
func (r *reconciler) Trigger() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

// run schedules passes: periodic every Cycle ± Jitter, first at a random
// point of the cycle so gateways spread out; triggered on demand. One pass
// at a time (spec: One pass at a time).
func (r *reconciler) run(ctx context.Context) {
	cfg := r.g.cfg.Reconcile
	next := time.NewTimer(rand.N(cfg.Cycle))
	defer next.Stop()
	pace := pb.Pace_PACE_UNSPECIFIED
	for {
		if pace == pb.Pace_PACE_UNSPECIFIED {
			select {
			case <-ctx.Done():
				return
			case <-r.trigger:
				pace = pb.Pace_PACE_TRIGGERED
			case <-next.C:
				pace = pb.Pace_PACE_PERIODIC
			}
		}
		started := time.Now()
		again := r.pass(ctx, pace)
		if ctx.Err() != nil {
			return
		}
		if pace == pb.Pace_PACE_PERIODIC {
			jitter := time.Duration(0)
			if cfg.Jitter > 0 {
				jitter = rand.N(2*cfg.Jitter) - cfg.Jitter
			}
			next.Reset(max(0, time.Until(started.Add(cfg.Cycle+jitter))))
		}
		pace = pb.Pace_PACE_UNSPECIFIED
		if again {
			pace = pb.Pace_PACE_TRIGGERED
		}
	}
}

// pass runs one pass. It returns true when a triggered pass must follow: a
// trigger during a periodic pass cancels it; one during a triggered pass is
// served by another triggered pass once this one ends, since the sweep that
// caused it may have run after this pass read those entries.
func (r *reconciler) pass(ctx context.Context, pace pb.Pace) (again bool) {
	g, cfg := r.g, r.g.cfg.Reconcile
	start := time.Now()
	loops := g.loops.bound()
	parts, ok := r.snapshot(ctx, loops)
	if !ok {
		return false
	}
	rate := float64(cfg.TriggeredRate)
	if pace == pb.Pace_PACE_PERIODIC {
		total := 0
		for _, p := range parts {
			total += len(p)
		}
		// Spread over 90% of the cycle, leaving room for ReconcileEnd.
		rate = min(max(float64(total)/(0.9*cfg.Cycle.Seconds()), float64(cfg.MinRate)), rate)
	}
	perEntry := time.Duration(float64(time.Second) / rate)

	inflight := make(map[uint64]flight)
	cancelled := false
	cursor := 0
	due := time.Now()
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		work := !cancelled && hasWork(parts)
		if !work && len(inflight) == 0 {
			break
		}
		var wake <-chan time.Time
		if work && len(inflight) < cfg.MaxInFlight {
			if d := time.Until(due); d > 0 {
				t.Reset(d)
				wake = t.C
			} else {
				// One loop's partition per chunk, loops in turn (round-robin).
				for len(parts[cursor%len(parts)]) == 0 {
					cursor++
				}
				pi := cursor % len(parts)
				cursor++
				n := min(cfg.Chunk, len(parts[pi]))
				take := parts[pi][:n:n]
				parts[pi] = parts[pi][n:]
				// Re-queued entries count against the rate too (R4G-O8).
				if now := time.Now(); due.Before(now) {
					due = now
				}
				due = due.Add(time.Duration(n) * perEntry)
				r.send(ctx, loops[pi], pi, take, inflight)
				continue
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-wake:
		case d := <-r.resolved:
			f, ok := inflight[d.chunk]
			if !ok {
				continue
			}
			delete(inflight, d.chunk)
			if cancelled {
				continue
			}
			for _, id := range d.refused {
				if rq := f.requeues[id]; int(rq) < cfg.MaxRequeues {
					parts[f.part] = append(parts[f.part], passItem{id: id, requeues: rq + 1})
					g.m.Requeued.Inc()
				} else {
					g.m.RequeueLimit.Inc()
				}
			}
		case <-r.trigger:
			again = true
			if pace == pb.Pace_PACE_PERIODIC {
				cancelled = true // no more chunks, no ReconcileEnd
			}
		}
	}
	paceLabel := pace.String()
	if cancelled {
		g.m.Passes.WithLabelValues(paceLabel, "cancelled").Inc()
		return true
	}
	r.end(ctx, pace)
	g.m.PassSeconds.WithLabelValues(paceLabel).Observe(time.Since(start).Seconds())
	return again
}

func hasWork(parts [][]passItem) bool {
	for _, p := range parts {
		if len(p) > 0 {
			return true
		}
	}
	return false
}

// snapshot copies each loop's ONLINE conn_ids into its partition, one Execute
// per loop (spec: Key snapshot).
func (r *reconciler) snapshot(ctx context.Context, loops []*loopState) ([][]passItem, bool) {
	return gather(ctx, loops, func(ls *loopState) []passItem {
		items := make([]passItem, 0, len(ls.conns))
		for id, c := range ls.conns {
			if c.state == stateOnline && !c.closing {
				items = append(items, passItem{id: id})
			}
		}
		return items
	})
}

// gather runs fn on every loop and waits for all of them. It fails if a loop
// has shut down.
func gather[T any](ctx context.Context, loops []*loopState, fn func(*loopState) T) ([]T, bool) {
	out := make([]T, len(loops))
	type res struct {
		i int
		v T
	}
	ch := make(chan res, len(loops))
	for i, ls := range loops {
		if ls.loop().Execute(func() { ch <- res{i, fn(ls)} }) != nil {
			return nil, false
		}
	}
	for range loops {
		select {
		case x := <-ch:
			out[x.i] = x.v
		case <-ctx.Done():
			return nil, false
		}
	}
	return out, true
}

// send builds a chunk on its loop (one Execute) and, if it holds anything,
// sends it.
func (r *reconciler) send(ctx context.Context, ls *loopState, part int, take []passItem, inflight map[uint64]flight) {
	r.chunkSeq++
	id := r.chunkSeq
	got := make(chan built, 1)
	if ls.loop().Execute(func() { got <- r.g.buildChunk(ls, id, take) }) != nil {
		return
	}
	var b built
	select {
	case b = <-got:
	case <-ctx.Done():
		return
	}
	if len(b.ids) == 0 {
		return
	}
	rq := make(map[uint64]uint8, len(take))
	for _, it := range take {
		rq[it.id] = it.requeues
	}
	inflight[id] = flight{part: part, requeues: rq}
	go r.call(ctx, ls, id, b)
}

// buildChunk runs on the loop: it skips IDs no longer ONLINE, copies the
// current fields, marks each connection inChunk and arms the chunk deadline.
func (g *Gateway) buildChunk(ls *loopState, id uint64, items []passItem) built {
	sent := g.now()
	b := built{sentMs: sent.UnixMilli()}
	for _, it := range items {
		c := ls.conns[it.id]
		if c == nil || c.state != stateOnline || c.closing || c.inChunk {
			continue
		}
		c.inChunk = true
		b.ids = append(b.ids, c.id)
		b.entries = append(b.entries, &pb.ReconcileEntry{ConnId: c.id, Uid: c.uid, Appid: c.appid,
			Platform: c.platform, DeviceId: c.deviceID, ConnSeq: c.connSeq, LoginMs: c.recvMs, TokIatMs: c.tokIatMs})
	}
	if len(b.ids) > 0 {
		rec := &chunkRec{ids: b.ids}
		ls.chunks[id] = rec
		// No definitive reply by sent_ms + login_deadline + 1s: the
		// compare-and-set can no longer write, so the chunk is resolved.
		ls.wheel.schedule(&rec.timer, sent.Add(g.cfg.FenceAfter()), func() { g.resolveChunk(ls, id, nil, nil) })
	}
	return b
}

// call sends the chunk. A reply is resolved on the loop; an error is not a
// definitive reply, so the chunk waits for its deadline on the wheel.
func (r *reconciler) call(ctx context.Context, ls *loopState, id uint64, b built) {
	g := r.g
	timeout := time.Duration(b.sentMs+g.cfg.FenceAfter().Milliseconds()-g.now().UnixMilli()) * time.Millisecond
	cctx, cancel := g.rpcCtx(ctx, timeout)
	reply, err := r.logic.Reconcile(cctx, &pb.ReconcileChunk{Gw: g.ref, SentMs: b.sentMs, Entries: b.entries})
	cancel()
	if err != nil || len(reply.GetResults()) != len(b.entries) {
		g.m.ReconcileEntries.WithLabelValues("no_reply").Add(float64(len(b.entries)))
		return
	}
	for _, res := range reply.GetResults() {
		g.m.ReconcileEntries.WithLabelValues(resultLabel(res.GetCode())).Inc()
		if res.GetCode() == pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER {
			g.m.ReconcileNotOwner.WithLabelValues(res.GetReason().String()).Inc()
		}
	}
	_ = ls.loop().Execute(func() { g.resolveChunk(ls, id, b.ids, reply.GetResults()) })
}

func resultLabel(c pb.ReconcileCode) string {
	switch c {
	case pb.ReconcileCode_RECONCILE_CODE_WRITTEN:
		return "written"
	case pb.ReconcileCode_RECONCILE_CODE_UNCHANGED:
		return "unchanged"
	case pb.ReconcileCode_RECONCILE_CODE_REFUSED:
		return "refused"
	case pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER:
		return "not_owner"
	}
	return "unknown"
}

// resolveChunk runs on the loop when a chunk gets its reply (results) or its
// deadline passes (results nil): it clears inChunk, Kicks and closes
// not_owner connections, releases Logouts fence (c) held, and tells the
// controller which entries were refused. A reply that arrives after the
// deadline still Kicks its not_owner connections, and does nothing else.
func (g *Gateway) resolveChunk(ls *loopState, id uint64, ids []uint64, results []*pb.ReconcileResult) {
	rec, live := ls.chunks[id]
	if live {
		delete(ls.chunks, id)
		ls.wheel.stop(&rec.timer)
		ids = rec.ids
	}
	var refused []uint64
	for i, cid := range ids {
		c := ls.conns[cid]
		if c == nil {
			continue
		}
		if live {
			c.inChunk = false
		}
		if i < len(results) {
			switch res := results[i]; res.GetCode() {
			case pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER:
				if c.state == stateOnline && !c.closing {
					g.kickConn(c, clientpb.KickReason(res.GetReason()), "not_owner")
				}
			case pb.ReconcileCode_RECONCILE_CODE_REFUSED:
				if live {
					refused = append(refused, cid)
				}
			}
		}
		if live {
			g.settle(c)
		}
	}
	if live && g.recon != nil {
		select {
		case g.recon.resolved <- chunkDone{chunk: id, refused: refused}:
		default: // unreachable: at most MaxInFlight chunks are unresolved
			g.log.Error("reconcile: resolution channel full", "chunk", id)
		}
	}
}

// end finishes a pass: stamp as_of_ms, then collect every tracked conn_id,
// one Execute per loop, each taken after as_of_ms (spec: Tracked-set
// collection), and send ReconcileEnd. A loop that can't be read means an
// incomplete set, which would delete live sessions: no ReconcileEnd then.
func (r *reconciler) end(ctx context.Context, pace pb.Pace) {
	g := r.g
	asOf := g.now().UnixMilli()
	loops := g.loops.bound() // read after as_of: a loop bound later only has Logins received after it
	frags, ok := gather(ctx, loops, func(ls *loopState) []uint64 {
		ids := make([]uint64, 0, len(ls.conns))
		for id := range ls.conns {
			ids = append(ids, id)
		}
		return ids
	})
	if !ok {
		g.m.Passes.WithLabelValues(pace.String(), "incomplete").Inc()
		return
	}
	var tracked []uint64
	for _, f := range frags {
		tracked = append(tracked, f...)
	}
	g.m.Passes.WithLabelValues(pace.String(), "done").Inc()
	req := &pb.ReconcileEndRequest{Gw: g.ref, AsOfMs: asOf, TrackedConnIds: tracked, Pace: pace}
	r.ends.Add(1)
	go func() {
		defer r.ends.Done()
		r.sendEnd(ctx, req)
	}()
}

// sendEnd sends ReconcileEnd and retries the same payload for up to RetryFor,
// then drops it; the next pass covers what it missed (R3-OP4). The deadline
// covers Logic's rate-limited delete phase (R5S-O8).
func (r *reconciler) sendEnd(ctx context.Context, req *pb.ReconcileEndRequest) {
	g := r.g
	first := time.Now()
	delay := g.cfg.RetryBackoff
	n := len(req.GetTrackedConnIds())
	timeout := 2*time.Duration(n)*time.Second/time.Duration(g.cfg.Reconcile.DeleteRate) + 10*time.Second
	for {
		cctx, cancel := g.rpcCtx(ctx, timeout)
		reply, err := r.logic.ReconcileEnd(cctx, req)
		cancel()
		if err == nil {
			g.m.ReconcileEnds.WithLabelValues("ok").Inc()
			g.m.EndDeleted.Add(float64(reply.GetDeleted()))
			g.m.EndTracked.Set(float64(n))
			return
		}
		if ctx.Err() != nil {
			return
		}
		if time.Since(first)+delay > g.cfg.RetryFor {
			g.m.ReconcileEnds.WithLabelValues("dropped").Inc()
			g.log.Warn("reconcile: ReconcileEnd dropped", "as_of_ms", req.GetAsOfMs(), "err", err)
			return
		}
		g.m.ReconcileEnds.WithLabelValues("retried").Inc()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(2*delay, g.cfg.RetryMaxDelay)
	}
}
```

In `internal/gateway/loops.go`:

Replace:

```go
}

// loopState is one loop's share of the gateway: its worker_id, conn_id
// counter, connection table and timing wheel. Every
// field but bound is touched only on the loop's goroutine, so none needs a
// lock (spec: Per-loop connection tables).
type loopState struct {
```

with:

```go
}

// loopState is one loop's share of the gateway: its worker_id, conn_id
// counter, connection table, timing wheel and open Reconcile chunks. Every
// field but bound is touched only on the loop's goroutine, so none needs a
// lock (spec: Per-loop connection tables).
type loopState struct {
```

Replace:

```go
	counter uint64 // conn_id low 48 bits; only Open touches it
	conns   map[uint64]*Conn
	wheel   *wheel
}

type boundLoop struct{ l Loop }
```

with:

```go
	counter uint64 // conn_id low 48 bits; only Open touches it
	conns   map[uint64]*Conn
	wheel   *wheel
	chunks  map[uint64]*chunkRec // Reconcile chunks sent and not resolved
}

type boundLoop struct{ l Loop }
```

Replace:

```go
			workerID: uint64(i),
			conns:    make(map[uint64]*Conn, tableHint(cfg)),
			wheel:    newWheel(now),
		}
	}
	return ls
```

with:

```go
			workerID: uint64(i),
			conns:    make(map[uint64]*Conn, tableHint(cfg)),
			wheel:    newWheel(now),
			chunks:   make(map[uint64]*chunkRec),
		}
	}
	return ls
```

In `internal/gateway/server.go`:

Replace:

```go
	readyChecks []func() error
	reqSeq      atomic.Uint64

	clients []*grpc.ClientConn
	kick    *grpc.Server
	admin   *http.Server
	lis     [2]net.Listener
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// Start dials Logic on two connections, opens the Kick and admin listeners
```

with:

```go
	readyChecks []func() error
	reqSeq      atomic.Uint64

	recon     *reconciler
	stopRecon context.CancelFunc
	clients   []*grpc.ClientConn
	kick      *grpc.Server
	admin     *http.Server
	lis       [2]net.Listener
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// Start dials Logic on two connections, opens the Kick and admin listeners
```

Replace:

```go
	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	return nil
}
```

with:

```go
	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	g.recon = newReconciler(g, logic)
	rctx, stop := context.WithCancel(ctx)
	g.stopRecon = stop
	g.goRun(func() { g.recon.run(rctx) })
	return nil
}
```

Replace:

```go
	defer cancel()
	g.admin.Shutdown(shut)
	g.wg.Wait()
	g.closeConns()
}
```

with:

```go
	defer cancel()
	g.admin.Shutdown(shut)
	g.wg.Wait()
	g.recon.ends.Wait()
	g.closeConns()
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): Reconcile pass controller with fence (c) and ReconcileEnd"
```

---

### Task 10: Lease loop (R1-O2, R3-O3, R3-OP5, R4M-5, choice #28)

**Files:**
- Create: `internal/gateway/lease.go`
- Modify: `internal/gateway/server.go` (lease client, `leaseOK`, start the loop)
- Test: `internal/gateway/lease_test.go`

**Interfaces:**
- Consumes: `reconciler.Trigger` (Task 9), the second client connection (Task 6).
- Produces: `g.runLease(ctx, lease pb.LeaseClient, trigger func())`, `g.renew(...)`; server fields `leaseOK atomic.Bool` (Task 11's readiness reads it), `leaseC pb.LeaseClient` (Task 12's `EpochEnded` uses it).

- [ ] **Step 1: Write the failing test**

`internal/gateway/lease_test.go`:

```go
package gateway

import (
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

func TestFirstLeaseMissingIsIgnoredAndALaterOneTriggersAPass(t *testing.T) {
	l := newLive(t, nil)
	missing := make(chan bool, 1)
	l.logic.set(func() {
		l.logic.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) {
			select {
			case m := <-missing:
				return &pb.LeaseReply{Missing: m}, nil
			default:
				return &pb.LeaseReply{}, nil
			}
		}
	})
	// newLive's first call already reported missing (the default handler).
	waitFor(t, "lease calls", func() bool { return get(l.logic, func() int { return len(l.logic.leaseCalls) }) >= 3 })
	if passes(l) != 0 {
		t.Fatal("the first lease call after boot triggered a pass (R3-OP5)")
	}
	if !l.g.leaseOK.Load() {
		t.Fatal("the first successful lease call did not count for readiness")
	}
	missing <- true
	waitFor(t, "triggered pass", func() bool { return passes(l) == 1 })
	end := get(l.logic, func() *pb.ReconcileEndRequest { return l.logic.ends[0] })
	if end.GetPace() != pb.Pace_PACE_TRIGGERED {
		t.Fatalf("pace %v, want triggered", end.GetPace())
	}
}

func TestLeaseCallCarriesTheGatewayClock(t *testing.T) {
	l := newLive(t, nil)
	l.clock.jump(2 * time.Second) // a skewed gateway (R4M-5)
	waitFor(t, "a call after the jump", func() bool {
		return get(l.logic, func() bool {
			if len(l.logic.leaseCalls) == 0 {
				return false
			}
			r := l.logic.leaseCalls[len(l.logic.leaseCalls)-1]
			return r.GetGwNowMs() > time.Now().Add(time.Second).UnixMilli()
		})
	})
}

func TestRenewalAfterFailuresForALeaseTTLTriggersAPass(t *testing.T) {
	l := newLive(t, nil) // LeaseTTL 300ms, interval 100ms
	failing := make(chan bool, 1)
	failing <- false
	l.logic.set(func() {
		l.logic.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) {
			f := <-failing
			failing <- f
			if f {
				return nil, errors.New("Logic unreachable")
			}
			return &pb.LeaseReply{}, nil // live: a lost reply may have hidden the missing
		}
	})
	waitFor(t, "first lease", l.g.leaseOK.Load)
	time.Sleep(150 * time.Millisecond)
	if passes(l) != 0 {
		t.Fatal("live replies triggered a pass")
	}
	<-failing
	failing <- true
	time.Sleep(400 * time.Millisecond) // longer than the lease TTL
	<-failing
	failing <- false
	waitFor(t, "triggered pass", func() bool { return passes(l) >= 1 })
}
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run Lease`
Expected: FAIL, `l.g.leaseOK undefined`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/lease.go`:

```go
package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// runLease renews the epoch's lease every LeaseInterval on the dedicated
// lease connection (spec: Lease, R1-O2).
func (g *Gateway) runLease(ctx context.Context, lease pb.LeaseClient, trigger func()) {
	t := time.NewTicker(g.cfg.LeaseInterval)
	defer t.Stop()
	var lastOK time.Time // start of the last successful renewal
	for {
		g.renew(ctx, lease, &lastOK, trigger)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// renew makes one GatewayLease call. A pass is triggered when the reply is
// missing, and also when the renewals before this one failed for at least a
// lease TTL: the key may have expired and been swept, and a lost reply may
// have hidden the missing that re-created it. The first successful call after
// boot always finds a new epoch's key absent, so it neither counts nor acts
// on missing (R3-OP5).
func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.Time, trigger func()) {
	start := time.Now()
	cctx, cancel := g.rpcCtx(ctx, g.cfg.LeaseTimeout)
	reply, err := lease.GatewayLease(cctx, &pb.LeaseRequest{Gw: g.ref, GwNowMs: g.now().UnixMilli()})
	cancel()
	if err != nil {
		g.m.LeaseCalls.WithLabelValues("error").Inc()
		if ctx.Err() == nil {
			g.log.Warn("lease renewal failed", "err", err)
		}
		return
	}
	first := lastOK.IsZero()
	gap := time.Since(*lastOK)
	*lastOK = start
	if first {
		g.m.LeaseCalls.WithLabelValues("first").Inc()
		g.leaseOK.Store(true)
		return
	}
	if reply.GetMissing() {
		g.m.LeaseCalls.WithLabelValues("missing").Inc()
	} else {
		g.m.LeaseCalls.WithLabelValues("live").Inc()
	}
	if reply.GetMissing() || gap >= g.cfg.LeaseTTL {
		trigger()
	}
}
```

In `internal/gateway/server.go`:

Replace:

```go
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error
	reqSeq      atomic.Uint64

	recon     *reconciler
	stopRecon context.CancelFunc
	clients   []*grpc.ClientConn
	kick      *grpc.Server
	admin     *http.Server
```

with:

```go
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error

	leaseOK atomic.Bool // the first GatewayLease succeeded
	reqSeq  atomic.Uint64

	recon     *reconciler
	stopRecon context.CancelFunc
	leaseC    pb.LeaseClient
	clients   []*grpc.ClientConn
	kick      *grpc.Server
	admin     *http.Server
```

Replace:

```go
	}
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])

	g.kick = grpc.NewServer()
	pb.RegisterGatewayServer(g.kick, kickServer{g: g})
```

with:

```go
	}
	ctx, g.cancel = context.WithCancel(ctx)
	logic := pb.NewLogicClient(g.clients[0])
	g.leaseC = pb.NewLeaseClient(g.clients[1])

	g.kick = grpc.NewServer()
	pb.RegisterGatewayServer(g.kick, kickServer{g: g})
```

Replace:

```go
	rctx, stop := context.WithCancel(ctx)
	g.stopRecon = stop
	g.goRun(func() { g.recon.run(rctx) })
	return nil
}
```

with:

```go
	rctx, stop := context.WithCancel(ctx)
	g.stopRecon = stop
	g.goRun(func() { g.recon.run(rctx) })
	g.goRun(func() { g.runLease(ctx, g.leaseC, g.recon.Trigger) })
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): lease loop on its own connection with the trigger rule"
```

---

### Task 11: Readiness and the limits self-check (R4H-O5, R5S-O7, choice #34)

**Files:**
- Create: `internal/gateway/limits.go`
- Modify: `internal/gateway/server.go` (self-check, `Ready`, `watchLogic`, `/readyz`), `internal/gateway/gateway.go` (`limits: hostLimits`), `internal/gateway/logic_test.go` (`okLimits`)
- Test: `internal/gateway/server_test.go`

**Interfaces:**
- Consumes: `leaseOK` (Task 10), `Deps.ReadyChecks` (Task 5).
- Produces: `limitSource{nofile, readFile}`, `hostLimits`, `checkLimits(cfg, src, m) error`; `(*Gateway) Ready() error`; `g.watchLogic(ctx, cc)`; server fields `limits`, `limitsErr`, `logicUp`. Test helpers `okLimits`, `readyz(t, g)`.

- [ ] **Step 1: Write the failing test**

`internal/gateway/server_test.go`:

```go
package gateway

import (
	"errors"
	"io/fs"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus"
)

func TestLimitsSelfCheck(t *testing.T) {
	m := NewMetrics(prometheus.NewRegistry())
	cfg := testConfig()
	cfg.MaxConns = 1000 // needs nofile ≥ 1100, nf_conntrack_max ≥ 2000
	src := func(nofile uint64, conntrack string, err error) limitSource {
		return limitSource{
			nofile:   func() (uint64, error) { return nofile, nil },
			readFile: func(string) ([]byte, error) { return []byte(conntrack), err },
		}
	}
	for _, tc := range []struct {
		name    string
		src     limitSource
		notrack bool
		fail    string
	}{
		{"enough", src(1100, "2000\n", nil), false, ""},
		{"nofile too low", src(1099, "2000\n", nil), false, "RLIMIT_NOFILE"},
		{"conntrack too low", src(1100, "1999\n", nil), false, "nf_conntrack_max"},
		{"conntrack too low but notrack", src(1100, "10\n", nil), true, ""},
		{"conntrack not loaded", src(1100, "", fs.ErrNotExist), false, ""},
		{"conntrack unreadable", src(1100, "", errors.New("permission denied")), false, "nf_conntrack_max"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := cfg
			c.Limits.Notrack = tc.notrack
			err := checkLimits(c, tc.src, m)
			if (err == nil) != (tc.fail == "") || (err != nil && !strings.Contains(err.Error(), tc.fail)) {
				t.Fatalf("err %v, want failure mentioning %q", err, tc.fail)
			}
		})
	}
}

func readyz(t *testing.T, g *Gateway) int {
	t.Helper()
	_, admin := g.Addrs()
	resp, err := http.Get("http://" + admin + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestReadyOnlyAfterLogicAndFirstLease(t *testing.T) {
	gate := make(chan struct{})
	var keysLoaded atomic.Bool
	l := startLive(t, nil, func(g *Gateway, f *fakeLogic) {
		g.readyChecks = []func() error{func() error {
			if !keysLoaded.Load() {
				return errors.New("Noise key set not loaded")
			}
			return nil
		}}
		f.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) { <-gate; return &pb.LeaseReply{Missing: true}, nil }
	})
	waitFor(t, "Logic channel up", l.g.logicUp.Load)
	if readyz(t, l.g) != http.StatusServiceUnavailable {
		t.Fatal("Ready before the first GatewayLease succeeded")
	}
	close(gate)
	waitFor(t, "first lease", l.g.leaseOK.Load)
	if readyz(t, l.g) != http.StatusServiceUnavailable {
		t.Fatal("Ready before the transport's key set is loaded")
	}
	keysLoaded.Store(true)
	if readyz(t, l.g) != http.StatusOK {
		t.Fatalf("not Ready: %v", l.g.Ready())
	}
}

func TestFailedLimitsKeepTheGatewayNotReady(t *testing.T) {
	l := startLive(t, nil, func(g *Gateway, _ *fakeLogic) {
		g.limits = limitSource{nofile: func() (uint64, error) { return 10, nil }, readFile: okLimits.readFile}
	})
	waitFor(t, "first lease", l.g.leaseOK.Load)
	if err := l.g.Ready(); err == nil || !strings.Contains(err.Error(), "RLIMIT_NOFILE") {
		t.Fatalf("Ready() = %v, want the nofile failure", err)
	}
	if readyz(t, l.g) != http.StatusServiceUnavailable {
		t.Fatal("/readyz is OK with a failed limits check")
	}
}
```

In `internal/gateway/logic_test.go`:

Replace:

```go
			mod(c)
		}
	})
	if pre != nil {
		pre(h.g, f)
	}
```

with:

```go
			mod(c)
		}
	})
	h.g.limits = okLimits
	if pre != nil {
		pre(h.g, f)
	}
```

Replace:

```go
	return &live{core: h, logic: f}
}

// kickClient dials the gateway's Kick server.
func (l *live) kickClient(t *testing.T) pb.GatewayClient {
	t.Helper()
```

with:

```go
	return &live{core: h, logic: f}
}

var okLimits = limitSource{
	nofile:   func() (uint64, error) { return 1 << 20, nil },
	readFile: func(string) ([]byte, error) { return []byte("4000000\n"), nil },
}

// kickClient dials the gateway's Kick server.
func (l *live) kickClient(t *testing.T) pb.GatewayClient {
	t.Helper()
```

- [ ] **Step 2: Run it to make sure it fails**

Run: `go test ./internal/gateway/ -run 'Limits|Ready'`
Expected: FAIL, `undefined: limitSource`, `h.g.limits undefined`.

- [ ] **Step 3: Write the implementation**

`internal/gateway/limits.go`:

```go
package gateway

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// limitSource reads the two limits the gateway can see (spec: Limits
// self-check, R5S-O7). Tests replace it.
type limitSource struct {
	nofile   func() (uint64, error)
	readFile func(string) ([]byte, error)
}

var hostLimits = limitSource{
	nofile: func() (uint64, error) {
		var r syscall.Rlimit
		err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &r)
		return r.Cur, err
	},
	readFile: os.ReadFile,
}

// checkLimits returns why the gateway must stay NotReady, or nil:
// RLIMIT_NOFILE ≥ cap × (1 + headroom), and, unless conntrack is disabled for
// the gateway port, nf_conntrack_max ≥ 2 × cap. An absent conntrack file means
// conntrack isn't loaded, which passes.
func checkLimits(cfg Config, src limitSource, m *Metrics) error {
	var errs []error
	needFiles := uint64(math.Ceil(float64(cfg.MaxConns) * (1 + cfg.Limits.Headroom)))
	switch n, err := src.nofile(); {
	case err != nil:
		errs = append(errs, fmt.Errorf("read RLIMIT_NOFILE: %w", err))
	default:
		m.Limits.WithLabelValues("nofile").Set(float64(n))
		if n < needFiles {
			errs = append(errs, fmt.Errorf("RLIMIT_NOFILE %d is below %d (cap %d + %.0f%% headroom)",
				n, needFiles, cfg.MaxConns, cfg.Limits.Headroom*100))
		}
	}
	if !cfg.Limits.Notrack {
		b, err := src.readFile(cfg.Limits.ConntrackPath)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err != nil:
			errs = append(errs, fmt.Errorf("read %s: %w", cfg.Limits.ConntrackPath, err))
		default:
			n, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("parse %s: %w", cfg.Limits.ConntrackPath, err))
				break
			}
			m.Limits.WithLabelValues("nf_conntrack_max").Set(float64(n))
			if need := 2 * uint64(cfg.MaxConns); n < need {
				errs = append(errs, fmt.Errorf("nf_conntrack_max %d is below %d (2 × cap), or set conntrack: notrack", n, need))
			}
		}
	}
	return errors.Join(errs...)
}
```

In `internal/gateway/gateway.go`:

Replace:

```go
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	g.server = server{registry: d.Registry, readyChecks: d.ReadyChecks}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
```

with:

```go
		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
	}
	g.server = server{registry: d.Registry, readyChecks: d.ReadyChecks, limits: hostLimits}
	for s := range g.gauges {
		gauge := m.Conns.WithLabelValues(stateLabel[s])
		g.gauges[s] = stateGauges{gauge.Inc, gauge.Dec}
```

In `internal/gateway/server.go`:

Replace:

```go
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
```

with:

```go
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
```

Replace:

```go
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error

	leaseOK atomic.Bool // the first GatewayLease succeeded
	reqSeq  atomic.Uint64

	recon     *reconciler
```

with:

```go
type server struct {
	registry    *prometheus.Registry
	readyChecks []func() error
	limits      limitSource
	limitsErr   error

	leaseOK atomic.Bool // the first GatewayLease succeeded
	logicUp atomic.Bool // the Logic channel has been connected once
	reqSeq  atomic.Uint64

	recon     *reconciler
```

Replace:

```go
	wg        sync.WaitGroup
}

// Start dials Logic on two connections, opens the Kick and admin listeners
// and starts the background goroutines.
func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
		cc, err := g.dial(target)
		if err != nil {
```

with:

```go
	wg        sync.WaitGroup
}

// Start runs the self-check, dials Logic on two connections, opens the Kick
// and admin listeners and starts every background goroutine.
func (g *Gateway) Start(ctx context.Context, sc ServerConfig) error {
	g.limitsErr = checkLimits(g.cfg, g.limits, g.m)
	if g.limitsErr != nil {
		g.log.Error("limits self-check failed: staying NotReady", "err", g.limitsErr)
	}
	for _, target := range []string{sc.LogicTarget, sc.LeaseTarget} {
		cc, err := g.dial(target)
		if err != nil {
```

Replace:

```go
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	g.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	g.goRun(func() { g.kick.Serve(g.lis[0]) })
	g.goRun(func() {
```

with:

```go
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(g.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := g.Ready(); err != nil {
			http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	g.admin = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	g.goRun(func() { g.kick.Serve(g.lis[0]) })
	g.goRun(func() {
```

Replace:

```go
	})

	g.goRun(func() { g.runTicker(ctx) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	g.recon = newReconciler(g, logic)
```

with:

```go
	})

	g.goRun(func() { g.runTicker(ctx) })
	g.goRun(func() { g.watchLogic(ctx, g.clients[0]) })
	g.startLogin(ctx, logic)
	g.startLogout(ctx, logic)
	g.recon = newReconciler(g, logic)
```

Replace:

```go
	}()
}

// runTicker advances every loop's wheel once per tick, through Execute
// (spec: Timers; gnet's engine-wide OnTick is not used).
func (g *Gateway) runTicker(ctx context.Context) {
```

with:

```go
	}()
}

// Ready reports why the gateway must not get traffic yet, or nil (spec:
// Readiness): not draining, limits pass, the Logic channel connected, the
// first GatewayLease (on the lease channel) succeeded, and the transport's
// checks pass. The conditions latch: once Ready, a later Logic outage does not
// make the gateway NotReady, since every gateway would see it alike.
func (g *Gateway) Ready() error {
	switch {
	case g.draining.Load():
		return errors.New("draining")
	case g.limitsErr != nil:
		return g.limitsErr
	case !g.logicUp.Load():
		return errors.New("Logic channel not connected")
	case !g.leaseOK.Load():
		return errors.New("first GatewayLease has not succeeded")
	}
	for _, check := range g.readyChecks {
		if err := check(); err != nil {
			return err
		}
	}
	return nil
}

// watchLogic latches logicUp once the Logic channel is connected, and keeps
// the Ready gauge current.
func (g *Gateway) watchLogic(ctx context.Context, cc *grpc.ClientConn) {
	cc.Connect()
	for s := cc.GetState(); s != connectivity.Ready; s = cc.GetState() {
		if !cc.WaitForStateChange(ctx, s) {
			return
		}
	}
	g.logicUp.Store(true)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		ready := 0.0
		if g.Ready() == nil {
			ready = 1
		}
		g.m.Ready.Set(ready)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runTicker advances every loop's wheel once per tick, through Execute
// (spec: Timers; gnet's engine-wide OnTick is not used).
func (g *Gateway) runTicker(ctx context.Context) {
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/gateway && go test -race -count=1 ./internal/gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): readiness with the nofile/conntrack self-check"
```

---

### Task 12: Graceful drain and the end-to-end test against Logic (choice #35)

**Files:**
- Create: `internal/gateway/drain.go`
- Test: `internal/gateway/drain_test.go`, `internal/gateway/e2e_test.go`

**Interfaces:**
- Consumes: `gather`, `stopRecon` (Task 9), `leaseC` (Task 10), `reconnect`, `closeConn`, `tracked` (Task 5); in the test only: `logic.NewServer`, `logic.DefaultConfig`, `logic.DefaultServerConfig`, `logic.Deps`, `logic.NewMetrics`, `store.New`, `auth.Sign`, `auth.Verify`, `auth.GenerateKey`, `testredis.New`, `testredis.UID`, `testredis.Gateway`, `session.Decode`, `session.Entry` (Plans 1 and 2).
- Produces: `(*Gateway) Drain(ctx)`, `g.endEpoch(ctx)`.

The end-to-end test runs this gateway against a real Logic pod on miniredis (on the Tier 2 Redis under `make test-redis`): a second device on the same slot gets the first one kicked through Logic's Kick RPC, a client Logout deletes the field through the fence, and a triggered pass restores a deleted field (rule 4) and deletes a ghost of this epoch (delete phase). Only the test file imports Logic; `TestGatewayDoesNotImportTheStore` checks the package itself.

- [ ] **Step 1: Write the failing tests**

`internal/gateway/drain_test.go`:

```go
package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

func TestDrainSendsReconnectClosesTheRestAndEndsTheEpoch(t *testing.T) {
	l := newLive(t, func(c *Config) {
		c.Drain.Window, c.Drain.Jitter, c.Drain.Flush = 2*time.Second, 5*time.Second, time.Second
	})
	kc := l.kickClient(t)
	ks := l.onlineMany(t, 3)
	auth := l.authed(0) // never logs in
	done := make(chan struct{})
	go func() { l.g.Drain(context.Background()); close(done) }()
	waitFor(t, "NotReady", func() bool { return l.g.Ready() != nil })
	if _, reason := l.open(1); reason != RejectDraining {
		t.Fatalf("open during drain: %q, want draining", reason)
	}
	kick(t, kc, "gw-1", 0xE1, ks[0].c.id, pb.KickReason_KICK_REASON_REPLACED) // Kicks still reach a draining gateway
	waitFor(t, "kicked", ks[0].isClosed)
	<-done
	for _, k := range ks[1:] {
		var r clientpb.Reconnect
		if !k.last(t, clientpb.Cmd_CMD_RECONNECT, &r) || r.JitterMs != 5000 {
			t.Fatalf("conn %x: Reconnect %v, want jitter 5000ms", k.c.id, &r)
		}
		if !k.isClosed() {
			t.Fatal("still open after the drain")
		}
	}
	if !auth.isClosed() || l.g.tracked.Load() != 0 {
		t.Fatalf("after drain: auth closed=%v, %d still tracked", auth.isClosed(), l.g.tracked.Load())
	}
	if n := get(l.logic, func() int { return l.logic.epochEnded }); n != 1 {
		t.Fatalf("EpochEnded called %d times", n)
	}
}
```

`internal/gateway/e2e_test.go`:

```go
package gateway

import (
	"context"
	"crypto/ed25519"
	"strconv"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// e2e is this gateway core against a real Logic pod on miniredis (or the
// Tier 2 Redis). Only this test file imports Logic and the store; the
// gateway package itself does not (TestGatewayDoesNotImportTheStore).
type e2e struct {
	*core
	rdb *redis.Client
	key auth.PrivateKey
}

func newE2E(t *testing.T) *e2e {
	t.Helper()
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis
	rdb := testredis.New(t)
	key, err := auth.GenerateKey("k1")
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	var kickAddr string // set once the gateway listens
	dial := func(string) (*grpc.ClientConn, error) {
		return grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	sc := logic.DefaultServerConfig()
	sc.GRPCAddr, sc.LeaseAddr, sc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	srv, err := logic.NewServer(lcfg, sc, logic.Deps{Store: store.New(rdb, lcfg.Store), Keys: staticKeys{key},
		Dial: dial, Registry: reg, Metrics: logic.NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Stop() })
	grpcAddr, leaseAddr, _ := srv.Addrs()

	h := newCore(t, func(c *Config) {
		fastConfig(c)
		c.GatewayID = testredis.Gateway()
		c.LeaseTTL = 30 * time.Second // Logic's
	})
	h.g.limits = okLimits
	if err := h.g.Start(context.Background(), ServerConfig{LogicTarget: grpcAddr, LeaseTarget: leaseAddr,
		KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.g.Stop)
	kickAddr, _ = h.g.Addrs()
	waitFor(t, "gateway Ready", func() bool { return h.g.Ready() == nil })
	return &e2e{core: h, rdb: rdb, key: key}
}

// staticKeys is a key set that is always loaded.
type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

func (e *e2e) token(t *testing.T, uid string) string {
	t.Helper()
	now := time.Now().UnixMilli()
	tok, err := auth.Sign(e.key, auth.Claims{UID: uid, AppID: "app", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3_600_000})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// loginAs logs a device in on loop li and waits for ONLINE.
func (e *e2e) loginAs(t *testing.T, li int, uid, device string, seq uint64) *fakeLink {
	t.Helper()
	k := e.authed(li)
	req := loginReq(uid, device, seq)
	req.Token = e.token(t, uid)
	e.frame(k, clientpb.Cmd_CMD_LOGIN, 1, req)
	waitFor(t, uid+"/"+device+" ONLINE", func() bool { return onLoop(k, func(c *Conn) bool { return c.state == stateOnline }) })
	return k
}

func (e *e2e) field(t *testing.T, uid string) (session.Entry, bool) {
	t.Helper()
	v, err := e.rdb.HGet(context.Background(), "sess:{"+uid+"}", "app:mobile").Result()
	if err == redis.Nil {
		return session.Entry{}, false
	}
	if err != nil {
		t.Fatal(err)
	}
	en, err := session.Decode(v)
	if err != nil {
		t.Fatal(err)
	}
	return en, true
}

func TestE2ELoginKickByOtherDeviceAndLogout(t *testing.T) {
	e := newE2E(t)
	uid := testredis.UID()
	a := e.loginAs(t, 0, uid, "phone-1", 1)
	b := e.loginAs(t, 1, uid, "phone-2", 1) // the same slot from another device
	waitFor(t, "A kicked through Logic's Kick RPC", a.isClosed)
	var m clientpb.Kick
	if !a.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("A's Kick %v", &m)
	}
	if en, ok := e.field(t, uid); !ok || en.ConnID != b.c.id {
		t.Fatalf("stored %+v, want B's conn_id %x", en, b.c.id)
	}
	e.frame(b, clientpb.Cmd_CMD_LOGOUT, 2, nil)
	waitFor(t, "field deleted by the fenced Logout", func() bool { _, ok := e.field(t, uid); return !ok })
}

func TestE2EReconcileRestoresAndDeletesGhosts(t *testing.T) {
	e := newE2E(t)
	ctx := context.Background()
	live := testredis.UID()
	k := e.loginAs(t, 0, live, "d", 1)
	// A lost write: the live connection's field is gone (race 15 / wrongly swept).
	e.rdb.HDel(ctx, "sess:{"+live+"}", "app:mobile")
	// A ghost: a field of this epoch whose connection the gateway doesn't track (race 7).
	ghost := testredis.UID()
	en := session.Entry{Gateway: e.g.cfg.GatewayID, Epoch: e.g.cfg.Epoch, ConnID: 1<<47 | 99,
		DeviceID: "g", ConnSeq: 1, LoginMs: time.Now().Add(-time.Minute).UnixMilli(), TokIatMs: 1}
	e.rdb.HSet(ctx, "sess:{"+ghost+"}", "app:mobile", en.Encode())
	e.rdb.SAdd(ctx, "gwconns:"+e.g.cfg.GatewayID+":{"+strconv.FormatUint(e.g.cfg.Epoch, 10)+"}", ghost)

	before := endsOK(e)
	e.g.recon.Trigger()
	waitFor(t, "ReconcileEnd done", func() bool { return endsOK(e) > before })
	if got, ok := e.field(t, live); !ok || got.ConnID != k.c.id {
		t.Fatalf("live field %+v, %v: want it restored by rule 4", got, ok)
	}
	if _, ok := e.field(t, ghost); ok {
		t.Fatal("ghost not deleted by the delete phase")
	}
}

func endsOK(e *e2e) float64 { return promtest.ToFloat64(e.g.m.ReconcileEnds.WithLabelValues("ok")) }
```

- [ ] **Step 2: Run them to make sure they fail**

Run: `go test ./internal/gateway/ -run 'Drain|E2E'`
Expected: FAIL, `l.g.Drain undefined` (the E2E tests compile only with the package).

- [ ] **Step 3: Write the implementation**

`internal/gateway/drain.go`:

```go
package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// Drain is the SIGTERM path (spec: Graceful drain): go NotReady and refuse
// new connections, stop Reconcile, send Reconnect{jitter_ms} to the ONLINE
// connections in batches over Drain.Window, close the rest, wait for their
// fences and Logouts (at most login_deadline + 1s + Drain.Flush), then call
// EpochEnded. Kicks are still served throughout.
func (g *Gateway) Drain(ctx context.Context) {
	g.draining.Store(true)
	g.m.Ready.Set(0)
	if g.stopRecon != nil {
		g.stopRecon()
	}
	loops := g.loops.bound()
	parts, ok := gather(ctx, loops, func(ls *loopState) []uint64 {
		ids := make([]uint64, 0, len(ls.conns))
		for id, c := range ls.conns {
			if c.state == stateOnline && !c.closing {
				ids = append(ids, id)
			}
		}
		return ids
	})
	if ok {
		steps := max(1, int(g.cfg.Drain.Window/time.Second))
		t := time.NewTicker(g.cfg.Drain.Window / time.Duration(steps))
		for s := 0; s < steps; s++ {
			for i, ls := range loops {
				n := (len(parts[i]) + steps - s - 1) / (steps - s)
				take := parts[i][:n]
				parts[i] = parts[i][n:]
				_ = ls.loop().Execute(func() {
					for _, id := range take {
						if c := ls.conns[id]; c != nil && c.state == stateOnline && !c.closing {
							g.reconnect(c)
						}
					}
				})
			}
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		t.Stop()
	}
	// Close what is left, in any state.
	gather(ctx, g.loops.bound(), func(ls *loopState) struct{} {
		for _, c := range ls.conns {
			if c.state != stateClosed {
				g.closeConn(c, "drain")
			}
		}
		return struct{}{}
	})
	// Wait until no Login or Reconcile write for this epoch can still land
	// and the Logouts are sent: EpochEnded lets the sweeper delete the
	// epoch's fields at once.
	deadline := time.Now().Add(g.cfg.FenceAfter() + g.cfg.Drain.Flush)
	for g.tracked.Load() > 0 && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	g.endEpoch(ctx)
}

// endEpoch calls EpochEnded on the lease connection, a few times on error.
func (g *Gateway) endEpoch(ctx context.Context) {
	for attempt := range 3 {
		cctx, cancel := g.rpcCtx(ctx, g.cfg.LeaseTimeout)
		_, err := g.leaseC.EpochEnded(cctx, &pb.EpochEndedRequest{Gw: g.ref})
		cancel()
		if err == nil {
			return
		}
		g.log.Warn("EpochEnded failed", "attempt", attempt+1, "err", err)
	}
}
```

- [ ] **Step 4: Run the whole suite**

Run: `go vet ./... && go test -race -count=1 ./...`
Expected: PASS for every package.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "feat(gateway): graceful drain; end-to-end test against Logic"
```

---

## After this plan

The gateway core runs against Logic over gRPC on a fake transport. Plan 4 adds the frame codec, Noise NK, the handshake limiter, gnet and `cmd/gateway`, using the seam in *Execution Notes → For Plan 4*.

When the plan is approved, record the decisions on #24–#37 in the roadmap, update its Plan 3 status and fix this plan's test names in the invariant index (already entered as proposed).

The open risks are listed in *Execution Notes*.
