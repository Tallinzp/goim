# Client SDK Implementation Plan — Plan 5a of 6

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Go client SDK, which implements every rule in the spec's *Client SDK rules*, and the scripted test client that Plan 5b's race suite drives. The SDK covers persistence, the storm wait, the local trigger, `Reconnect`, the two retry counters, backoff, server-set windows with clamps, dead-server detection and the stop conditions. The scripted client sends any frame at any time, then closes with a FIN or a reset.

**Architecture:** Two new packages, both client-side only. `internal/clientconn` is one blocking connection: dial, Noise NK through `internal/wire`, and encrypted frames that carry protobuf bodies, with no policy. It is the scripted client, the SDK's transport, and a candidate transport for Plan 6's loadgen. `internal/sdk` splits the rules from the I/O. `windows.go` holds the four reconnect windows and the clamp table. `state.go` persists `device_id`, `conn_seq` and the server-set windows as one record. `policy.go` is pure logic: given how an attempt or connection ended, it picks the next wait and its `reconnect_reason`, and it keeps both counters and the backoff. `attempt.go` runs one connection: dial, handshake, Login, then Ping and the Pong timeout. It classifies every ending. `client.go` is the run loop: one goroutine owns all state, the public methods post commands to it, and events come out in order through a callback. The unit tests run whole clients against a scripted fake gateway on `net.Pipe` inside `testing/synctest` bubbles, so 20s storm waits and 10s Pong timeouts cost no real time. An end-to-end file runs the SDK over real TCP against real gateways and Plan 2's Logic.

**Tech Stack:** Go 1.27 (`testing/synctest`, `math/rand/v2`), `internal/wire` (Plan 4: `ClientHandshake`, `Session`, `ErrClosedBeforeReply`), `api/clientpb` (Plan 3), `google.golang.org/protobuf v1.36.12`. No new dependencies. Only test files import the server (gateway, tcp, Logic, store, miniredis).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md` (authoritative). Sections: *Client protocol* (commands, *Reconnect windows in `LoginResp`*, `LoginReq`, Login errors, Kick reasons, device_id), *Client SDK rules* (all of it), *Storms, drain, failures* (the SDK summary), and *Verification → Unit tests → SDK*. Roadmap, choices and invariant index: `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`. The byte layer this plan uses is `docs/superpowers/plans/2026-10-08-connection-session-core-04-tcp-transport.md`, *Execution Notes → For Plan 5* and *Interface review* (P4I-1, P4I-4, P4I-6).

## Global Constraints

- Module path `github.com/Tallinzp/goim`. Both packages stay under `internal/` (P4I-6: "`internal/wire` stays internal …: the Go SDK lives in this module; the mobile SDKs are native code built against the spec"). `internal/sdk` depends only on `api/clientpb`, `internal/wire` and `internal/clientconn` (Task 5's dependency test).
- conn_seq: "persist `conn_seq` **before** each connection attempt, in the same place as device_id with the same lifetime (e.g. both in the iOS Keychain), so clearing one clears the other."
- Connection lost: "after losing an **established** connection unexpectedly, wait a **uniformly distributed** random **0–20s** … before the first attempt …, then exponential backoff (cap 5m). Obey `retry_after`. The client can't tell a mass disconnect from a loss of its own, so this applies whatever the cause."
- Local trigger: "when the OS reports a network change, or the app returns to the foreground while disconnected, cancel any pending 0–20s wait and attempt within a uniform random **0–1s**."
- Precedence: "a local trigger removes only the 0–20s storm wait. It does not shorten a `Busy{retry_after}` wait, a `Reconnect{jitter_ms}` wait, or exponential backoff after a retry limit, so flapping networks can't bypass server backpressure."
- `Reconnect{jitter_ms}`: "wait the server-given uniform `[0, jitter_ms]`, not 0–20s … Failures after that follow the normal rules." The client closes, waits, then reconnects with `reconnect_reason = Reconnect from drain`.
- Pre-handshake closes: "a TCP connection that the server closes before the Noise handshake completes … Wait a uniformly random **0.5–1.5s**, then reconnect. After **10 in a row**, fall back to exponential backoff (cap 5m)."
- Other retryable rejects: "`Busy{retry_after}`; a Login rejected as `EXPIRED`; a Login rejected as `KEY_UNKNOWN`; a connection closed after the Noise handshake but before any `LoginResp` or `Error` arrives … Each waits a uniformly random 1–3s (or obeys `retry_after` when the server sends one), then reconnects. After **5 in a row**, fall back to exponential backoff (cap 5m)."
- "An outcome of one kind resets the other kind's counter; both reset on a successful login." "The client never gives up permanently while the app wants to be online."
- Server-set windows: "the SDK uses the values from the last `LoginResp` that carried them. It **persists** them next to `device_id`/`conn_seq` and uses them for every later wait, including before the next login. It **clamps** each value to a safe range built into the app … Fresh installs use the built-in defaults." Every field is optional: "an absent field keeps the SDK's current value."
- Clamp table, roadmap choice #13 (decided 2026-10-07): storm wait default 0–20s, `hi` 10–120s, `lo` 0; local trigger 0–1s, `hi` 1–10s, `lo` 0; pre-handshake retry 0.5–1.5s, `hi` 1–5s, `lo` ≥ 0.25s; other retryable 1–3s, `hi` 2–10s, `lo` ≥ 0.5s; "each window `[lo, hi]` clamped at both ends and `lo < hi` enforced".
- Stop conditions: "logout, account switch and app close cancel every pending reconnect or retry, whatever its cause." "Each attempt uses a new source port, so the NLB flow hash can pick another gateway."
- Dead-server detection: "if no frame arrives within **10s** of a Ping, close and reconnect."
- Login errors: `STALE` → "Set counter to `max(local, stored) + 1`, persist, retry after backoff"; `SUPERSEDED` → "Retryable; a retry is a new, later login and wins normally"; `EXPIRED` → retryable reject; `REVOKED` → "Drop the token, show the login screen"; `KEY_UNKNOWN` → retryable reject. Roadmap choice #17 adds `TOKEN_INVALID` ("client gets a new token; Plan 5 confirms") and `BAD_REQUEST`.
- Kick reasons: "**None of them triggers an automatic reconnect**". `REPLACED`: "The client goes idle and reconnects only on a user action or when the app returns to the foreground. The SDK logs it as an anomaly". `KICKED_BY_OTHER_DEVICE`: "shown to the user". `REVOKED`: "the client drops its token and shows the login screen".
- device_id: "The SDK must make sure at most one connection per device_id is in use at a time." `LogoutResp` means "the gateway accepted the logout, not that Redis was updated".
- `LoginReq` carries `token, appid, platform, device_id, conn_seq, reconnect_reason, offline_ms`; `offline_ms` is "ms since the client detected the loss, on the client's monotonic clock". "A retry after the server closed the connection before the handshake completed is reported as `Busy retry`."
- Unit tests the spec lists for this plan (*Verification → Unit tests → SDK*): every sentence of that bullet. They are pinned in Tasks 2, 4 and 5 (see *Execution Notes → Spec test map*).
- Tiers: Tier 1a on the dev machine (2 cores / 2GB), `go test -race`, no Docker. Roadmap choices #1–#46 apply; this plan's choices are #47–#57 below.

## Review Focus

These are the five inputs the spec implies but no rule-by-rule test reaches, most likely first. Each is pinned in Task 5.

1. **A state file that can't be parsed or can't be read.** A record that can't be parsed (a half-written file from an old SDK, disk corruption, a restored backup from another format) must not keep the app offline. `New` starts as a fresh install with a new `device_id`, which is safe because the server sees another device on the slot and `conn_seq` restarts with it. The bad file is kept as `<path>.corrupt`, and an `EventAnomaly` is reported. The prototype's first `New` returned the error, so a damaged file kept the app offline for good. A record that can't be *read yet* (an iOS Keychain before the first unlock, Android Direct Boot) is different: it is kept untouched, and the client reads it again before each attempt, with an anomaly and a failure backoff while it fails (review item 1). Pinned: `TestCorruptStoreStartsFresh`, `TestUnreadableStoreKeepsItsRecord`, `TestFileStoreMovesACorruptFileAside`.
2. **A frame the SDK doesn't know.** Sub-project 2 adds message frames, and an old app will meet them. Expected: ignored, with the connection kept and the Pong timeout still satisfied (any frame counts). Pinned: `TestUnknownServerFramesAreIgnored`.
3. **A `LoginResp` without a heartbeat** (an old gateway, or a field left unset). Expected: the 150s default, never a zero interval that would ping in a loop. Pinned: `TestMissingHeartbeatUsesTheDefault`.
4. **A blackholed gateway.** After a node loss the NLB can still route to an address that never answers, and the TCP connect or the handshake hangs. Expected: one `ConnectTimeout` (10s), then a failure backoff, which a network change cuts short (choice #51). It is never a quick retry. Pinned: `TestBlackholedGatewayBacksOffAndANetworkChangeCutsIt`, and the `no handshake reply within ConnectTimeout` row of `TestEachEndingIsClassified`.
5. **Logout while the Login is still in flight.** Expected: the SDK closes at once, with no 2s wait for a `LogoutResp` that can't come, and it never reconnects. The gateway's fence (a) sends the Logout RPC after the Login reply (Plan 3). Pinned: `TestLogoutDuringAnAttemptClosesItAtOnce`.

## Implementation choices this plan makes (decided 2026-10-08)

The spec leaves these open. Each one is restated in the task that implements it. They go into the roadmap as #47–#57. #47 was decided by the user on 2026-10-08 when this plan was started. **#48–#57 were decided on 2026-10-08 at the plan review, all accepted as proposed.** A second review the same day (*Execution Notes → Plan review*) amended #49, #52, #53, #56 and #57; the amendments are marked *(review item N)* below.

47. **Plan 5 is split, and the race harness runs processes** (decided 2026-10-08). Plan 5a, this plan, is the SDK and the scripted client. Plan 5b covers the fault hooks, the test timing profile, the harness and every race scenario, and is written after 5a's interfaces are reviewed. The harness runs the real binaries (Logic, 4 gateways, devtoken) as local processes on both tiers. At Tier 1b, on the dev machine, Redis is a miniredis server process. At Tier 2 (`make test-tier2 SCENARIO=race`), Redis is the compose primary and replica that `make redis-up` already starts. Kills and pauses are signals, and faults go through a test-only endpoint. Scenarios that need real Redis (a `WAIT` failure) skip at Tier 1b. There is no 4-gateway compose file; Plan 6 adds Kubernetes manifests. This amends P4I-6: the suite runs on the dev machine again (as the spec's Tier 1b wants), and every scenario can be prototyped there.
48. **Packages.** `internal/clientconn` is a blocking connection with no policy. It serves as the scripted client, the SDK's transport, and a candidate transport for Plan 6's loadgen. `Send` is serialized and may run while another goroutine waits in `Recv` (P4I-4). A close or reset before any handshake reply byte matches `clientconn.ErrClosedBeforeReply` (P4I-1). `Reset` sends a RST (`SO_LINGER` 0). `internal/sdk` is the reference SDK. Its non-test code depends only on the client side, and a test enforces that.
49. **How each ending is classified.**
    - `STALE`, `SUPERSEDED` and `TOKEN_INVALID` share the second counter with `Busy`, `EXPIRED`, `KEY_UNKNOWN` and a close before any reply. All three are transient rejections, and the shared limit of 5 stops a loop. A `Busy` with no `retry_after` uses the retryable window.
    - `BAD_REQUEST`, and any code this SDK doesn't know: backoff and an anomaly event, never a fast loop. An `Error` that can't be decoded reads as code 0, an unknown code. The policy alone decides which endings are anomalies (`policy.anomaly`: these codes, a `REPLACED` or unknown Kick reason, a capped server value), and the client emits one event for each *(review item 4)*.
    - A dial failure, a `Token` failure, a failed `Save`, a handshake reply that fails, or no handshake reply within `ConnectTimeout`: a *failure backoff*. These leave both counters as they are.
    - `REVOKED` (an `Error` or a `Kick`): `EventLoginRequired`, then idle.
    - `TOKEN_INVALID`: the next `Token` call gets `refresh = true` and must return a new token. `refresh` stays true until a Login is answered with anything else, and `Logout`/`SwitchAccount` clear it. This confirms choice #17 *(review item 2)*.
50. **Backoff.** Step n waits uniformly in [d/2, d], with d = min(5m, 5s × 2^(n−1)). That is 2.5–5s, then 5–10s, and so on, reaching the cap at step 7. The step count restarts at a successful login, and *(amended 2026-10-08, final review M2)* at `Logout` and `SwitchAccount` together with both counters, so the next account starts fresh; a backoff never undercuts a `retry_after`. Quick retries: pre-handshake closes 1–10 and other rejects 1–5 retry fast; the 11th close or the 6th reject backs off. With 3 of 5 targets full, this matches the spec's 0.6¹⁰ arithmetic.
51. **Local trigger.**
    - It cuts the storm wait, as the spec says, and also a failure backoff. No server asked for that backoff, and without the cut a phone that was offline would wait up to 5m after its network returns.
    - It never cuts `Busy`, `Reconnect`, the quick-retry windows, or a backoff after a limit.
    - `Foreground` also ends the idle state after `REPLACED`. `KICKED_BY_OTHER_DEVICE` and `REVOKED` need `Connect`, a user action.
    - A network change while online does nothing: a dead socket is found by the read error or the Pong timeout.
    - *(amended 2026-10-08, final review I1)* A network change while an attempt is in flight and its Login not yet answered is remembered on that attempt. If the attempt fails, its failure backoff is cut to the local window, once; a server answer (`Busy`, a reject, a pre-handshake close) is never cut. Without this, the event was lost and the backoff after the failed attempt (up to 5m) still ran. In a correlated recovery (a city network) this adds the in-flight phones to the 0–1s burst that the storm-wait and failure-backoff cuts already make; admission control and `retry_after` still shape it.
52. **Clamp details (#13).**
    - The storm and local `lo` are pinned at 0.
    - When `lo ≥ hi` after clamping, `lo` becomes the floor (0.25s, 0.5s), which is always below the smallest `hi`.
    - Only the windows the server sent are persisted and clamped, again on load. The built-in defaults are app constants (`Config.Windows`), so a test profile can shorten them without clamps.
    - A `heartbeat_s` of 0 means 150s. Any other value is clamped to `Config.Heartbeat`, 30s–300s by default, with an anomaly when clamped. The gateway refuses to start with a heartbeat outside its own `HeartbeatMin`/`HeartbeatMax` (the same 30s–300s), so in production the clamp only catches a broken server; the test timing profile lowers both ends together (Task 7) *(review item 6)*.
    - `retry_after` is capped at `Backoff.Max` (5m) and `jitter_ms` at 120s, the largest storm `hi` of the clamp table, each with an anomaly when capped. The spec's server sends 1–3s and 5s, so the caps only act on a broken server *(review item 5)*.
53. **Client timeouts.** `TokenTimeout` 10s bounds the `Token` call: the SDK stops waiting at the deadline (a token server on a dead network can't hold the attempt, which no local trigger can cut), and a timeout is a failure backoff *(review item 3)*. `ConnectTimeout` 10s covers dial and handshake together. `LoginTimeout` 10s runs from `LoginReq` to the reply; the gateway answers by `login_deadline + 1s` = 6s, so a client timeout means a broken path. `PongTimeout` 10s comes from the spec. `LogoutTimeout` 2s is the wait for `LogoutResp`. A zero field takes its default in `New`.
54. **`reconnect_reason` and `offline_ms`.**
    - `reconnect_reason` is the reason of the wait before the attempt:
      - storm wait: `RST_FIN` or `PONG_TIMEOUT`;
      - local trigger: `LOCAL_TRIGGER`;
      - `Reconnect`: `DRAIN`;
      - quick retries, and backoff after a limit: `BUSY_RETRY`;
      - failure backoff: keeps the previous reason;
      - no wait (`Connect`): `FIRST_CONNECT`.
    - `offline_ms` runs on the monotonic clock from the loss of the last established connection, including a Kick, and continues across retries until the next login. Otherwise it is 0.
55. **Ping.** One Ping per heartbeat interval at a fixed cadence, so the gateway and the LB see a frame at least once per interval. The Pong timeout runs from the oldest Ping that no frame has followed, and any frame counts.
56. **Persistence.**
    - `State` (`device_id`, `conn_seq`, server windows) is one record. `FileStore` writes JSON: a temp file, fsync, rename, then a directory fsync.
    - `conn_seq` is saved before every dial. A failed `Save` skips the attempt: anomaly event plus failure backoff, and the number is never used.
    - `STALE` saves `max(local, stored)` at once; the next attempt adds 1.
    - Windows are saved as soon as a `LoginResp` changes them.
    - A corrupt store (`ErrCorrupt`) starts fresh; a store that can't be read yet is kept and read again before each attempt (Review Focus 1, *review item 1*).
    - A `Store` that is a `Locker` is locked by `New` and released by `Close`. `FileStore` takes an exclusive `flock` on `<path>.lock`, so a second `Client` on the same file fails with `ErrStoreInUse`: the spec's "at most one connection per device_id" holds across clients and processes. Native SDKs use their platform's equivalent *(review item 8)*.
57. **API and events.**
    - `New` returns an idle client. Methods: `Connect`, `NetworkChanged`, `Foreground`, `Logout`, `SwitchAccount`, `Close`.
    - One run-loop goroutine owns the state. `OnEvent` gets `Connecting`, `Online`, `Waiting`, `Idle`, `LoginRequired`, `LoggedOut` and `Anomaly`, in order, on that goroutine; it must not block or call the client. Store problems found by `New` are the run loop's first events, not emitted on the caller's goroutine *(review item 9)*.
    - `Token func(ctx, refresh bool)` (#49). A `LogoutResp` ends the connection only after the client sent `Logout`; any other one is ignored with an anomaly *(review item 7)*.
    - `Addrs` lists gateways and one is picked at random per attempt. In production this is the one NLB address; the race suite lists its gateways.
    - `Dial` and `Rand` can be injected for tests.

## Execution Notes

- **Branch:** Plan 4 (`feat/plan4-tcp`) is not merged, so Plan 5a branches from it: `git checkout -b feat/plan5a-sdk feat/plan4-tcp`. Commit this plan and the roadmap edit before Task 1, so the branch diff holds only Plan 5a's code.
- **Prototype:** every code block in this plan was compiled and run before the plan was written. The whole plan was built in a scratch clone of `accdbae`, then replayed task by task into a second clone, one commit per task, with `go vet ./...` and the task's package tests passing under `-race` after each. The replay's final tree matched the first clone byte for byte, and the full suite (`./...`) passed under `-race` three times on the dev machine.
- **Synctest:** checked first. Inside a `testing/synctest` bubble, `net.Pipe` read deadlines and `time.Timer` both run on the bubble's fake clock, so a client with a 10s Pong timeout and a 0–20s storm wait runs in milliseconds and every duration is exact. A test can assert `offline_ms == wait` to the millisecond. `r.take()` calls `synctest.Wait` and returns the events since the last call.
- **Mutation check:** each breakage below was made in the prototype and caught by the named test.
  - Policy:
    - backoff after 9 closes instead of 10 (`TestPreHandshakeClosesRetryQuicklyThenBackOff`);
    - a reject not resetting the close counter (`TestAnOutcomeOfOneKindResetsTheOtherCounter`);
    - a local trigger cutting any wait (`TestLocalTriggerCutsOnlyTheStormWaitAndAFailureBackoff`, `TestNetworkChangeDoesNotShortenRetryAfterOrReconnect`);
    - `retry_after` ignored (`TestOtherRetryableRejectsShareOneCounterOfFive`, `TestNetworkChangeDoesNotShortenRetryAfterOrReconnect`);
    - backoff undercutting `retry_after` (`TestOtherRetryableRejectsShareOneCounterOfFive`);
    - a Kick scheduling a reconnect (`TestKicksDoNotReconnect`, `TestE2EKickedByOtherDeviceDoesNotReconnect`, `TestE2ESharedDeviceIDReplacesOnce`);
    - login not resetting the counters (`TestLoginResetsBothCountersAndTheBackoff`).
  - Windows:
    - a u² sample in place of a uniform one (`TestWindowSampleIsUniform`, `TestJitteredWaitsAreUniform`);
    - no 10s storm floor, and `lo` allowed to reach `hi` (`TestServerWindowsAreClamped`).
  - Client:
    - no `Save` before the dial (`TestFirstConnectPersistsBeforeItDials`, `TestFailedSaveSkipsTheAttempt`);
    - `STALE`'s stored counter ignored (`TestStaleMovesConnSeqPastTheStoredOne`);
    - windows saved only with the next `conn_seq` (`TestServerWindowsAreUsedAndPersistedAcrossRestarts`);
    - `Logout` keeping the pending timer (`TestStopConditionsCancelAPendingRetry`);
    - the loss time not recorded (`TestLostConnectionWaitsTheStormWindowThenReconnects`, `TestNetworkChangeDuringTheStormWaitReconnectsWithinOneSecond`);
    - a corrupt store failing `New` (`TestCorruptStoreStartsFresh`);
    - `Logout` waiting on an attempt that isn't logged in (`TestLogoutDuringAnAttemptClosesItAtOnce`).
  - Attempt:
    - a Pong timeout reported as RST/FIN, and no Pong timeout at all (`TestPongTimeoutClosesAndReconnects`);
    - a pre-handshake close classified as a failure (`TestEachEndingIsClassified`, `TestPreHandshakeClosesReachBackoffAfterTen`);
    - an unknown frame ending the connection (`TestUnknownServerFramesAreIgnored`).
  - clientconn:
    - its own sentinel in place of `wire.ErrClosedBeforeReply` (`TestHandshakeReportsAPreHandshakeClose`);
    - `Send` without its mutex (`TestSendFromSeveralGoroutinesWhileReceiving`, interleaved frames fail to decrypt);
    - `Reset` sending a FIN (`TestCloseSendsFINAndResetSendsRST`).
  - Plan review items (each caught by the named test):
    - any `Load` error treated as corrupt (`TestUnreadableStoreKeepsItsRecord`); the corrupt file not moved aside (`TestFileStoreMovesACorruptFileAside`);
    - `refresh` never set, or cleared by a pre-handshake close or a loss (`TestTokenRefreshAfterTokenInvalid`);
    - no `TokenTimeout` (`TestTokenTimeoutIsAFailureBackoff`);
    - unknown codes not an anomaly (`TestAnomalies`, `TestUnknownErrorCodeBacksOffWithAnAnomaly`);
    - no `retry_after` cap, no `jitter_ms` cap (`TestServerWaitsAreCapped`);
    - no heartbeat clamp (`TestHeartbeatIsClampedToItsRange`);
    - every `LogoutResp` ending the connection (`TestUnsolicitedLogoutRespIsIgnored`);
    - no store lock, and no unlock on `Close` (`TestSecondClientOnTheSameStoreIsRefused`).

    Item 9 has no mutation test: only the goroutine that calls `OnEvent` changes, and a test can't tell that apart without racing the run loop.

  One mutation first survived. With the `Save` after a windows change removed, the windows still reached the store with the next attempt's `conn_seq`, which is too late for an app killed while online. `TestServerWindowsAreUsedAndPersistedAcrossRestarts` now checks the store before any further attempt. A mutation that made the Pong check never fire spun the ping loop at a fixed fake instant and hung the bubble. That was the mutant's own busy loop, so the check uses "no Ping recorded as unanswered" instead.
- **Spec test map** (*Verification → Unit tests → SDK*):
  - 0.5–1.5s pre-handshake retries, then backoff after 10: `TestPreHandshakeClosesRetryQuicklyThenBackOff`, `TestPreHandshakeClosesReachBackoffAfterTen`.
  - `Busy`/`EXPIRED`/`KEY_UNKNOWN` share one count of 5: `TestOtherRetryableRejectsShareOneCounterOfFive`.
  - One kind resets the other, and both reset on login: `TestAnOutcomeOfOneKindResetsTheOtherCounter`, `TestLoginResetsBothCountersAndTheBackoff`.
  - Logout, account switch and app close cancel a pending retry: `TestStopConditionsCancelAPendingRetry`.
  - A network change during the 0–20s wait: `TestNetworkChangeDuringTheStormWaitReconnectsWithinOneSecond`.
  - … during a `retry_after` wait: `TestNetworkChangeDoesNotShortenRetryAfterOrReconnect`.
  - After `Reconnect`, the attempt falls within `[0, jitter_ms]` with the drain reason: `TestReconnectWaitsWithinItsJitterWithTheDrainReason`, `TestNetworkChangeDoesNotShortenRetryAfterOrReconnect`, `TestE2EDrainMovesTheClientWithTheDrainReason`.
  - A close after the handshake with no reply counts toward the 5: `TestOtherRetryableRejectsShareOneCounterOfFive`, `TestEachEndingIsClassified`.
  - Uniformity over ~10k samples: `TestWindowSampleIsUniform`, `TestJitteredWaitsAreUniform`, with `TestUniformityCheckRejectsASkewedSample` showing the check bites.
  - Windows used, persisted across restarts, clamped, and kept when absent: `TestServerWindowsAreUsedAndPersistedAcrossRestarts`, `TestServerWindowsAreClamped`, `TestAbsentWindowsKeepTheirValue`.
  - Races 9 and 10 (no reconnect after `KICKED_BY_OTHER_DEVICE` or `REPLACED`): `TestKicksDoNotReconnect`, `TestE2EKickedByOtherDeviceDoesNotReconnect`, `TestE2ESharedDeviceIDReplacesOnce`.
- **Review decisions (2026-10-08):** #48–#57 all accepted as proposed. #48 and #49 were presented one at a time; the user then accepted the rest without changes.
- **Plan review (2026-10-08, later).** Claude reviewed the plan; the user decided nine items one at a time, all as proposed (option A), item 6 amended:
  1. Only a record that can't be parsed (`ErrCorrupt`, the file moved to `.corrupt`) starts fresh; any other `Load` error keeps the record, and the client reads it again before each attempt (anomaly plus failure backoff). Why: a Keychain before the first unlock would otherwise change `device_id` at every such launch.
  2. `Token(ctx, refresh bool)`, `refresh` true after `TOKEN_INVALID`. Why: an expired cached JWT would otherwise be retried for ever.
  3. `TokenTimeout` 10s, a timeout is a failure backoff. Why: a hung token call held the attempt, and no local trigger cuts an attempt in flight.
  4. The policy marks every anomaly ending; an undecodable `Error` is an unknown code. Why: unknown codes backed off silently, against #49.
  5. `retry_after` ≤ 5m, `jitter_ms` ≤ 120s, anomaly when capped. Why: `uint32` ms is up to 49.7 days, and Precedence lets nothing cut those waits.
  6. *(amended)* Heartbeat range 30s–300s on both ends: `sdk.Config.Heartbeat` and `gateway.Config.HeartbeatMin/Max` with `-heartbeat-min/-max` (Task 7). Amended because the test timing profile scales the heartbeat to ~15s, which a fixed range would refuse.
  7. `LogoutResp` ends the connection only after the client's `Logout`. Why: a stray one became a failure backoff on a healthy connection.
  8. `FileStore.Lock` (`flock`), `New` fails with `ErrStoreInUse`. Why: two clients on one record share a `device_id` and replace each other.
  9. `New`'s store events come from the run loop. Why: `OnEvent`'s one-goroutine guarantee had an exception.

  The prototype was rebuilt from this plan's code blocks (they reproduced the reviewed tree and its green suite), the changes were made and tested there, and the blocks were written back from it; extracting them again gives the prototype byte for byte. The full suite passed under `-race`, and the SDK and `clientconn` packages three more times.
- **Execution mode:** Native (inline), like Plans 1–4.
- **Implemented 2026-10-08** on `feat/plan5a-sdk`: plan commit 0905cb5, task commits 49759b4..1087c51, one per task, code blocks applied byte for byte, no deviations. Each task's test failed first (build failure), except Task 6, which adds tests only and passed at once as planned. Full suite green under `-race`.
- **Final review (fresh Opus, 2026-10-08):** no Critical.
  - Fixed: two goroutines calling `Close` at once panicked with "close of closed channel" (re-graded Minor→Important, an app crash); `quit` is closed under a `sync.Once`, `TestConcurrentCloseDoesNotPanic` (42f4c2a).
  - **I1 (Important), decided by the user: #51 amended**, `TestNetworkChangeDuringAFailingAttemptCutsItsBackoff` (RED→GREEN) and `TestNetworkChangeDuringAnAttemptDoesNotCutRetryAfter` (guards Precedence; a mutation that cut every ending was caught). The finding: a network change that arrives while an attempt is in flight is dropped. If that attempt then fails, the failure backoff it schedules (up to 5m at high step counts) is not cut, so a phone whose network came back can stay offline up to 5m (~10% of recoveries at high backoff: ~20s in flight against a 150–300s wait). Proposed: remember a local trigger seen while an attempt is in flight and not online; if that attempt ends as a failure, schedule the local window instead of the failure backoff (implemented as proposed).
  - **M2, decided by the user: #50 amended**, `TestLogoutAndSwitchAccountResetTheCountersAndTheBackoff` (RED→GREEN). The finding, against #50 ("the step count restarts only at a successful login"): backoff steps and both counters survive `Logout` and `SwitchAccount`, so a newly switched account's first transient failure can wait up to 5m. Proposed: reset the policy in `logout()` (implemented as proposed).
  - Deferred minors: M1 the backoff shift overflows for a `Backoff.Base` ≥ ~8.6s at step 31 (one 0s wait; the 5s default is safe); M4 `Send` has no write deadline, so a blocked `Send` would stall the Pong check once sub-project 2 sends large frames on the same `Conn` (Pings alone can't block).
- **Interface review (2026-10-08), decisions P5I-1..P5I-6.** Claude read the surface Plan 5b and Plan 6 consume against the spec's *Race integration tests* and *Load tests*; the user decided each item one at a time, all as proposed. P5I-1 and P5I-3..P5I-6 were implemented as **Task 8** (no task text in this plan: tests first, each RED on behaviour, then GREEN; full suite green under `-race`, `sdk` and `clientconn` 3x more):
  - **P5I-1** Events say what ended the attempt: `EndKind` is exported (`EndLost`, `EndPongTimeout`, `EndReconnect`, `EndKicked`, `EndPreHandshake`, `EndBusy`, `EndRejected`, `EndNoReply`, `EndFailed`), and `Event.End` and `Event.RetryAfter` (the server's value before the cap) are set on `EventWaiting`, `EventIdle` and `EventLoginRequired`. Before, a `Busy` and a close before any reply both read as `WaitRetryable` with code 0, told apart only by `Err == nil`, and a `Busy` whose body failed to decode lost even that; Plan 6's "Logic slowed: `Busy`, `expired` near zero" test counts both. Tests: an `End` column on `TestEachEndingIsClassified` (plus the row "Busy with a body that can't be decoded"), `TestEventsSayWhatEndedTheConnection`, `TestFailedSaveIsAnEndFailed` (35c25b3).
  - **P5I-2** (decision for Plan 6, no change) Cost per online SDK client, measured with a throwaway test (5,000 clients against the `net.Pipe` fake): 3 goroutines (run loop, reader, pinger); about 18.5KB of stack and 19KB of heap per client together with its fake server, so roughly 25–35KB per client: ~6GB at 200k, ~18GB at 600k, before kernel socket buffers. Plan 6 measures on the Tier 2 host and picks per load test: (a) at 600k and (c) only hold connections and Ping, so they use `clientconn` with a light pinger; the SDK runs where its rules matter (the b1/b2 storms at 200k). Folding the pinger into the reader (a `Recv` deadline at the next Ping or the Pong deadline, 2 goroutines) stays an option if the measurement needs it.
  - **P5I-3** `Connect` while a wait is pending or an attempt is in flight is a local trigger, like `NetworkChanged`: a user's "retry" cuts the storm wait and a failure backoff to the local window, and never a `Busy`, `Reconnect`, quick-retry or limit backoff wait (spec: Precedence). Before, it did nothing. Test: `TestConnectDuringAWaitIsALocalTrigger` (6f40fc5). Amends #51 and #57.
  - **P5I-4** Documented: `Logout` and `SwitchAccount` block the caller and the run loop for up to `LogoutTimeout` (a mobile port calls them off the UI thread); calling a `Client` method from `OnEvent` deadlocks; `Config.Rand` is one per `Client` (b6532ae).
  - **P5I-5** `clientconn.Conn.Send` and `Recv` return `ErrNoHandshake` before a successful `Handshake` instead of panicking on a nil session; `SendRaw` still works then. Test: `TestSendAndRecvBeforeTheHandshakeFail`.
  - **P5I-6** `clientconn.IsClosed` also matches `EPIPE` and `ECONNABORTED`: a `Send` after the reset was read fails with "broken pipe" on Linux (seen in the RED run). Tests: `TestSendAfterAResetIsClosed` (real TCP), `TestIsClosedAcceptsEveryPeerClose` (a5b1ccf).
  - Checked, no change needed: a whole-population RST from loadgen goes through a `Config.Dial` wrapper that keeps the `*net.TCPConn`; the clock-skew scenario's "logs in through another gateway" reads `Attempt.Addr`; the test profile's gateways send no windows (the window flags unset), since the SDK clamps server windows to production ranges; the retry limits (10, 5) and the 120s drain cap need no scaling. Sub-project 2 adds app frames: `serve` drops unknown frames today and the SDK has no app send.
- **Tests and timing.** `internal/clientconn` takes ~1.3s, over real TCP on 127.0.0.1. The SDK unit tests take ~1.5s under `-race`, on fake time. The end-to-end file takes ~10s on real time: each test starts Logic on miniredis and one or two gnet gateways, and two tests watch 2s for a reconnect that must not happen.
- **Not verified at Tier 1a, and where each is closed:**
  1. **Real mobile networks.** Radio wake-up, NAT rebinding, OS network-change events and background limits are the native SDKs' to test. This SDK only defines the rules.
  2. **Real Redis.** The end-to-end tests run on miniredis at Tier 1a and on the real Redis at Tier 2 (`make test-redis` covers `./...`), with unique gateway ids and uids. **Tier 2 run 7 (2026-10-09, commit e8bda03, Redis 7.4.11, host kernel 7.0.0-34, `make test-tier2 SCENARIO=store`): all four checks pass.** Every package passed under `-race` on a real primary and replica, including `internal/sdk` (the five `TestE2E…` tests) and `internal/clientconn`; no Tier 2 skips (only the 8 miniredis-only skips); the `WAIT` mutation was caught; clean tree. Run 6 (6098ac5) had failed only Plan 4's `TestSlowConsumerIsClosedAtTheOutboundCap`, a test artefact on kernel 7.0 fixed in e8bda03 (Plan 4 *Execution Notes*). **Tier 2 run 8 (2026-10-09, commit d76db05, after Task 8 and the interface review): all four checks pass**, with the same 8 miniredis-only skips and no Tier 2 skips; the `WAIT` mutation was caught; clean tree. Redis Cluster and lost replies after a write stay open (Plan 1).
  3. **Scale.** A storm of SDK clients against a dead gateway (the 30s SLO) is Plan 6's loadgen.
  4. **Platform storage.** `FileStore` is the Go reference. The iOS Keychain and Android storage are the native SDKs'.
- **For Plan 5b (race suite):**
  - Scripted clients are `clientconn.Conn`. The SDK runs where a scenario needs the client's rules (drain, Busy retries, storm). Under the test timing profile, set `sdk.Config.Windows` to shorter built-in defaults: only server-sent windows are clamped (choice #52). Set `sdk.Config.Heartbeat` and the gateways' `-heartbeat-min`/`-heartbeat-max` to the same lowered range (review item 6).
  - The harness of choice #47 needs:
    - Logic flags for profile values that are not flags yet: the Kick retry spacing, the JWKS refresh and unknown-`kid` intervals, the sweep batch;
    - a Kick address per gateway process, since `-kick-addr-format` maps a pod name to DNS;
    - the `cmd/gateway` fake-clock flag of P4I-5 as a static offset at start. `gateway.Deps.Clock` also drives the timing wheel, so a step at run time would fire timers early;
    - fault hooks: gRPC interceptors in Logic and on the gateway's Kick server, and a go-redis hook in Logic for "`ZADD gwepochs` fails while the `SET` succeeds", all behind a test-only flag;
    - pause: SIGSTOP on the miniredis process at Tier 1b, `CLIENT PAUSE` at Tier 2;
    - a `WAIT` failure only at Tier 2 (e.g. `DEBUG SLEEP` on the replica).
  - **Dev machine budget (measured 2026-10-08 on 2 cores / 2GB, commit e56e848).** All binaries built with `-race`, in a scratch clone: a miniredis server process (a 15-line `miniredis.StartAddr` wrapper), `devtoken serve`, Logic and 4 gateways (`-loops 1`). Idle RSS: miniredis 27MB, devtoken 24MB, Logic 51MB, each gateway 70MB, about 380MB in all. With 400 SDK clients online across the 4 gateways: miniredis 44MB, Logic 77MB, each gateway 91–96MB, about 520MB, plus 207MB for the client process. About 750MB of memory was still available, and idle CPU was ~9%. No errors in any log. The harness of choice #47 fits on the dev machine at the race suite's scale (tens of clients) with room to spare. CPU contention under the lowered timing profile is untested; that is a flakiness risk for the prototype, not a memory one.
  - The branches keep stacking: Plan 5b is written and implemented on `feat/plan5b-race`, branched from `feat/plan5a-sdk`, and the stack is merged later (the user's decision, 2026-10-08).

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/clientconn/conn.go` | `Conn`: `Dial`, `New`, `Handshake`, `Send`, `SendRaw`, `Recv`, `SetReadDeadline`, `Close`, `Reset`, `LocalAddr`, `ExpectClosed`; `Frame`, `IsClosed`, `ErrClosedBeforeReply` |
| `internal/sdk/windows.go` | package doc; `Window`, `Windows`, `DefaultWindows`; the clamp table; `HeartbeatRange` (`Of`), `DefaultHeartbeatRange`; `ServerWindows` (`Merge`, `Clamp`, `Over`) |
| `internal/sdk/state.go` | `State`, `Store`, `ErrCorrupt`, `Locker`, `ErrStoreInUse`, `FileStore` (`Load`, `Save`, `Lock`), `MemStore`, `NewDeviceID` |
| `internal/sdk/policy.go` | endings, `WaitKind`, `Wait`, `Backoff`; `codeRule`/`ruleOf`, `maxDrainJitter`; `policy` (`after`, `anomaly`, `local`, `localWait`, `loggedIn`, `first`) |
| `internal/sdk/attempt.go` | one attempt: `token` (with `TokenTimeout`), `login` (classification), `serve` (frames), `ping` (heartbeat and Pong timeout), `tcpDial` |
| `internal/sdk/client.go` | `Config`, `DefaultConfig`, `Event`, `EventKind`, `Attempt`, `Client` and its run loop (`load`, `start`, `fail`, `handle`, `logout`) |
| `internal/gateway/config.go`, `config_test.go`, `cmd/gateway/main.go` (modify) | `HeartbeatMin`/`HeartbeatMax` and their check; `-heartbeat-min`/`-heartbeat-max` (Task 7) |

Shared test helpers:
- `internal/clientconn/conn_test.go`: `listen`, `peer`, `dial` (Task 1).
- `internal/sdk/windows_test.go`: `s`, `ms`, `pbw`, `uniform`, `chiSquare`, `seeded` (Task 2).
- `internal/sdk/policy_test.go`: `within`, `after` (Task 4).
- `internal/sdk/fake_test.go`: `fakeGW`, `fakeConn`, `rec`, `testClient`, `kinds`, `expect` (Task 5).

---

### Task 1: The scripted client connection (`internal/clientconn`, choice #48)

**Files:**
- Create: `internal/clientconn/conn.go`
- Test: `internal/clientconn/conn_test.go`

**Interfaces:**
- Consumes: `wire.ClientHandshake`, `wire.Session.WriteFrame`/`ReadFrame`, `wire.ErrClosedBeforeReply` (P4I-1), `wire.ErrHandshake`; the test server uses `wire.GenerateKey`, `wire.Respond`, `wire.HeaderLen`, `wire.HandshakeLen`, `wire.HandshakeMsgLen`; `clientpb.Cmd`.
- Produces: `var ErrClosedBeforeReply = wire.ErrClosedBeforeReply`; `type Frame struct{ Cmd clientpb.Cmd; ReqID uint32; Body []byte }` with `(Frame) Decode(proto.Message) error`; `type Conn`; `New(net.Conn) *Conn`; `Dial(addr string, timeout time.Duration) (*Conn, error)`; `(*Conn) Handshake(keyID uint16, serverPub []byte, deadline time.Time) error`; `Send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) error` (serialized; msg nil = empty body); `SendRaw([]byte) error`; `Recv(deadline time.Time) (Frame, error)`; `SetReadDeadline(time.Time) error`; `Close() error` (FIN); `Reset() error` (RST); `LocalAddr() net.Addr`; `ExpectClosed(deadline time.Time) error`; `IsClosed(error) bool`.

The connection adds no policy: no retries, no timers, no Ping. The race suite (Plan 5b) uses it to send any frame at any moment and to close with a FIN or a reset. The SDK (Task 5) runs on it.

- [ ] **Step 1: Write the failing test**

`internal/clientconn/conn_test.go`:

```go
package clientconn

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
)

// peer is the server side of one test connection: a raw socket, and the
// Noise session once serve has answered the handshake.
type peer struct {
	nc   net.Conn
	sess *wire.Session
}

// listen starts a server that hands every accepted socket to serve on its
// own goroutine, and returns its address and key.
func listen(t *testing.T, serve func(*peer, wire.Key)) (string, wire.Key) {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(func() { l.Close(); wg.Wait() })
	wg.Go(func() {
		for {
			nc, err := l.Accept()
			if err != nil {
				return
			}
			wg.Go(func() {
				defer nc.Close()
				nc.SetDeadline(time.Now().Add(10 * time.Second))
				serve(&peer{nc: nc}, key)
			})
		}
	})
	return l.Addr().String(), key
}

// respond reads frame 1 and answers the handshake.
func (p *peer) respond(key wire.Key) error {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	if _, err := io.ReadFull(p.nc, f); err != nil {
		return err
	}
	sess, reply, err := wire.Respond(key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return err
	}
	p.sess = sess
	_, err = p.nc.Write(reply)
	return err
}

func dial(t *testing.T, addr string, key wire.Key) *Conn {
	t.Helper()
	c, err := Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Handshake(1, key.Public, time.Now().Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSendAndRecvAfterTheHandshake(t *testing.T) {
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		for {
			cmd, id, body, err := p.sess.ReadFrame(p.nc)
			if err != nil {
				return
			}
			// Echo: a Ping gets a Pong, a LoginReq comes back as LoginResp
			// with the body unchanged (the test only checks it decodes).
			reply := clientpb.Cmd_CMD_PONG
			if clientpb.Cmd(cmd) == clientpb.Cmd_CMD_LOGIN {
				reply, body = clientpb.Cmd_CMD_LOGIN_RESP, nil
			}
			p.sess.WriteFrame(p.nc, uint16(reply), id, body)
		}
	})
	c := dial(t, addr, key)
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: "t", DeviceId: "d", ConnSeq: 3}); err != nil {
		t.Fatal(err)
	}
	f, err := c.Recv(time.Now().Add(5 * time.Second))
	var resp clientpb.LoginResp
	if err != nil || f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP || f.ReqID != 1 || f.Decode(&resp) != nil {
		t.Fatalf("got %+v, %v; want LoginResp 1", f, err)
	}
	if err := c.Send(clientpb.Cmd_CMD_PING, 7, nil); err != nil {
		t.Fatal(err)
	}
	if f, err := c.Recv(time.Now().Add(5 * time.Second)); err != nil || f.Cmd != clientpb.Cmd_CMD_PONG || f.ReqID != 7 {
		t.Fatalf("got %+v, %v; want Pong 7", f, err)
	}
}

// A pre-handshake close (spec: Client SDK rules) is told apart from a
// handshake that fails after the server answered.
func TestHandshakeReportsAPreHandshakeClose(t *testing.T) {
	for _, tc := range []struct {
		name  string
		serve func(*peer, wire.Key)
		want  error
	}{
		{"FIN", func(p *peer, _ wire.Key) {}, ErrClosedBeforeReply},
		{"RST", func(p *peer, _ wire.Key) {
			io.ReadFull(p.nc, make([]byte, wire.HeaderLen+wire.HandshakeLen)) // the reset must answer data
			p.nc.(*net.TCPConn).SetLinger(0)
		}, ErrClosedBeforeReply},
		{"garbage reply", func(p *peer, _ wire.Key) {
			io.ReadFull(p.nc, make([]byte, wire.HeaderLen+wire.HandshakeLen))
			p.nc.Write(make([]byte, wire.HeaderLen+wire.HandshakeMsgLen))
		}, wire.ErrHandshake},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, key := listen(t, tc.serve)
			c, err := Dial(addr, 5*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.Handshake(1, key.Public, time.Now().Add(5*time.Second)); !errors.Is(err, tc.want) {
				t.Fatalf("Handshake = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestHandshakeHonoursItsDeadline(t *testing.T) {
	addr, key := listen(t, func(p *peer, _ wire.Key) { io.Copy(io.Discard, p.nc) }) // never answers
	c, err := Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Handshake(1, key.Public, time.Now().Add(100*time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Handshake = %v, want a deadline error", err)
	}
}

// The server tells a client's FIN from its reset; the race suite uses both.
func TestCloseSendsFINAndResetSendsRST(t *testing.T) {
	got := make(chan error, 2)
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		_, err := p.nc.Read(make([]byte, 1))
		got <- err
	})
	dial(t, addr, key).Close()
	if err := <-got; !errors.Is(err, io.EOF) {
		t.Fatalf("after Close the server read %v, want EOF", err)
	}
	dial(t, addr, key).Reset()
	if err := <-got; !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("after Reset the server read %v, want ECONNRESET", err)
	}
}

func TestExpectClosed(t *testing.T) {
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		for {
			cmd, id, _, err := p.sess.ReadFrame(p.nc)
			if err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_PING {
				return // a Logout: close at once
			}
			p.sess.WriteFrame(p.nc, uint16(clientpb.Cmd_CMD_PONG), id, nil)
		}
	})
	c := dial(t, addr, key)
	c.Send(clientpb.Cmd_CMD_LOGOUT, 1, nil)
	if err := c.ExpectClosed(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("ExpectClosed after a server close = %v", err)
	}
	c = dial(t, addr, key)
	c.Send(clientpb.Cmd_CMD_PING, 1, nil)
	if err := c.ExpectClosed(time.Now().Add(5 * time.Second)); err == nil {
		t.Fatal("ExpectClosed passed although a Pong arrived")
	}
	c = dial(t, addr, key)
	if err := c.ExpectClosed(time.Now().Add(50 * time.Millisecond)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ExpectClosed on an open connection = %v, want a timeout", err)
	}
}

// Send is serialized, and may run while another goroutine waits in Recv
// (run under -race).
func TestSendFromSeveralGoroutinesWhileReceiving(t *testing.T) {
	const n = 200
	addr, key := listen(t, func(p *peer, key wire.Key) {
		if p.respond(key) != nil {
			return
		}
		for {
			_, id, _, err := p.sess.ReadFrame(p.nc)
			if err != nil {
				return
			}
			p.sess.WriteFrame(p.nc, uint16(clientpb.Cmd_CMD_PONG), id, nil)
		}
	})
	c := dial(t, addr, key)
	done := make(chan map[uint32]bool)
	go func() {
		seen := map[uint32]bool{}
		for len(seen) < n {
			f, err := c.Recv(time.Now().Add(5 * time.Second))
			if err != nil {
				break
			}
			seen[f.ReqID] = true
		}
		done <- seen
	}()
	var wg sync.WaitGroup
	for g := range 4 {
		wg.Go(func() {
			for i := range n / 4 {
				c.Send(clientpb.Cmd_CMD_PING, uint32(g*n/4+i), nil)
			}
		})
	}
	wg.Wait()
	if seen := <-done; len(seen) != n {
		t.Fatalf("got %d distinct Pongs, want %d (a frame was interleaved or lost)", len(seen), n)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/clientconn`
Expected: FAIL to build, `undefined: Conn` (and `Dial`, `ErrClosedBeforeReply`, …).

- [ ] **Step 3: Write the implementation**

`internal/clientconn/conn.go`:

```go
// Package clientconn is one blocking client connection to a gateway: dial,
// the Noise NK handshake, and encrypted frames carrying protobuf bodies
// (spec: Client protocol). It is the scripted test client (any frame, at any
// time, then a FIN or a reset), and the transport under the Go SDK
// (internal/sdk) and Plan 6's loadgen. It adds no policy: no retries, no
// timers, no Ping.
package clientconn

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/protobuf/proto"
)

// ErrClosedBeforeReply is wire.ErrClosedBeforeReply: the server closed or
// reset the connection before any byte of its handshake reply, the SDK's
// pre-handshake close (spec: Client SDK rules).
var ErrClosedBeforeReply = wire.ErrClosedBeforeReply

// Frame is one decrypted server frame.
type Frame struct {
	Cmd   clientpb.Cmd
	ReqID uint32
	Body  []byte
}

// Decode unmarshals the body into msg.
func (f Frame) Decode(msg proto.Message) error { return proto.Unmarshal(f.Body, msg) }

// Conn is one connection. Send may be called from one goroutine while Recv
// runs on another (wire.Session's directions share no state, P4I-4); Send
// itself is serialized, so several goroutines may send.
type Conn struct {
	nc   net.Conn
	sess *wire.Session
	wmu  sync.Mutex
}

// New wraps a connected net.Conn (a TCP socket, or net.Pipe in tests). Call
// Handshake before any other method.
func New(nc net.Conn) *Conn { return &Conn{nc: nc} }

// Dial connects to addr with a new socket, so each attempt gets a new source
// port and the NLB's flow hash can pick another gateway (R2-O2).
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	nc, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return New(nc), nil
}

// Handshake runs Noise NK against the server key pinned under keyID. It
// must finish by deadline (zero: none). A close or reset before any reply
// byte is reported as errors.Is(err, ErrClosedBeforeReply).
func (c *Conn) Handshake(keyID uint16, serverPub []byte, deadline time.Time) error {
	if err := c.nc.SetDeadline(deadline); err != nil {
		return err
	}
	sess, err := wire.ClientHandshake(c.nc, keyID, serverPub, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return err
	}
	c.sess = sess
	return c.nc.SetDeadline(time.Time{})
}

// Send seals and writes one frame; msg nil sends an empty body.
func (c *Conn) Send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) error {
	var body []byte
	if msg != nil {
		var err error
		if body, err = proto.Marshal(msg); err != nil {
			return err
		}
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return c.sess.WriteFrame(c.nc, uint16(cmd), reqID, body)
}

// SendRaw writes bytes as they are, e.g. a tampered or partial frame.
func (c *Conn) SendRaw(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.nc.Write(b)
	return err
}

// Recv reads one frame. A read past deadline (zero: none) fails with an
// error matching os.ErrDeadlineExceeded.
func (c *Conn) Recv(deadline time.Time) (Frame, error) {
	if err := c.nc.SetReadDeadline(deadline); err != nil {
		return Frame{}, err
	}
	cmd, reqID, body, err := c.sess.ReadFrame(c.nc)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Cmd: clientpb.Cmd(cmd), ReqID: reqID, Body: body}, nil
}

// SetReadDeadline changes the deadline of a Recv in progress, e.g. to arm
// the Pong timeout while another goroutine waits in Recv.
func (c *Conn) SetReadDeadline(t time.Time) error { return c.nc.SetReadDeadline(t) }

// Close closes the socket with a FIN.
func (c *Conn) Close() error { return c.nc.Close() }

// Reset closes the socket with a RST (SO_LINGER 0), as a crashed client or a
// dropped NAT mapping looks to the server. On a non-TCP conn it is Close.
func (c *Conn) Reset() error {
	if tc, ok := c.nc.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	return c.nc.Close()
}

// LocalAddr is the socket's local address (its source port).
func (c *Conn) LocalAddr() net.Addr { return c.nc.LocalAddr() }

// ExpectClosed waits until deadline for the server to close the
// connection. It fails if a frame or any byte arrives first, or on timeout.
func (c *Conn) ExpectClosed(deadline time.Time) error {
	if err := c.nc.SetReadDeadline(deadline); err != nil {
		return err
	}
	var b [1]byte
	n, err := c.nc.Read(b[:])
	switch {
	case n > 0:
		return errors.New("clientconn: got bytes, want a close")
	case IsClosed(err):
		return nil
	default:
		return fmt.Errorf("clientconn: want a close: %w", err)
	}
}

// IsClosed reports whether err means the peer closed (FIN) or reset the
// connection, or it was closed locally.
func IsClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, io.ErrClosedPipe)
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/clientconn && go test -race -count=1 ./internal/clientconn`
Expected: `ok` (~1.3s).

- [ ] **Step 5: Commit**

```bash
git add internal/clientconn
git commit -m "feat(clientconn): scripted client connection over TCP and Noise"
```

---

### Task 2: Reconnect windows and the clamp table (choices #13, #52)

**Files:**
- Create: `internal/sdk/windows.go`
- Test: `internal/sdk/windows_test.go`

**Interfaces:**
- Consumes: `clientpb.LoginResp`, `clientpb.Window`.
- Produces: `type Window struct{ Lo, Hi time.Duration }`, `(Window) Sample(*rand.Rand) time.Duration` (uniform, both ends included); `type Windows struct{ Storm, Local, PreHandshake, Retryable Window }`; `DefaultWindows() Windows`; `type HeartbeatRange struct{ Min, Max time.Duration }` with `Of(*clientpb.LoginResp) (time.Duration, bool)`, `DefaultHeartbeatRange()` (30s, 300s), unexported `defaultHeartbeat` (150s); `type ServerWindows Windows` with `Merge(*clientpb.LoginResp) ServerWindows`, `Clamp() ServerWindows`, `Over(Windows) Windows`; unexported `clampRange`, `clamps`. Test helpers: `s`, `ms(n int64)`, `pbw(lo, hi)`, `uniform(t, name, lo, hi, sample)`, `chiSquare(lo, hi, sample) (float64, [10]int, error)`, `seeded() *rand.Rand`.

`ServerWindows` holds only what the server sent (a zero `Window` was never set); `Over` lays it on the built-in defaults. The clamp table is choice #13; choice #52 pins the storm and local `lo` at 0 and turns a `lo` that reaches `hi` into the floor.

- [ ] **Step 1: Write the failing test**

`internal/sdk/windows_test.go`:

```go
package sdk

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

const s = time.Second

func ms(n int64) time.Duration { return time.Duration(n) * time.Millisecond }

func pbw(lo, hi time.Duration) *clientpb.Window {
	return &clientpb.Window{LoMs: uint32(lo.Milliseconds()), HiMs: uint32(hi.Milliseconds())}
}

// The clamp table, roadmap choice #13: each window clamped at both ends,
// lo < hi enforced.
func TestServerWindowsAreClamped(t *testing.T) {
	for _, tc := range []struct {
		name   string
		resp   *clientpb.LoginResp
		get    func(ServerWindows) Window
		lo, hi time.Duration
	}{
		{"storm in range", &clientpb.LoginResp{StormWait: pbw(0, 60*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 60 * s},
		{"storm hi below 10s", &clientpb.LoginResp{StormWait: pbw(0, 5*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 10 * s},
		{"storm hi above 120s", &clientpb.LoginResp{StormWait: pbw(0, 600*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 120 * s},
		{"storm lo pinned at 0", &clientpb.LoginResp{StormWait: pbw(5*s, 30*s)}, func(w ServerWindows) Window { return w.Storm }, 0, 30 * s},
		{"local hi zero", &clientpb.LoginResp{LocalTrigger: pbw(0, 0)}, func(w ServerWindows) Window { return w.Local }, 0, s},
		{"local hi above 10s", &clientpb.LoginResp{LocalTrigger: pbw(0, 60*s)}, func(w ServerWindows) Window { return w.Local }, 0, 10 * s},
		{"pre lo below 0.25s", &clientpb.LoginResp{PreHandshakeRetry: pbw(0, 2*s)}, func(w ServerWindows) Window { return w.PreHandshake }, ms(250), 2 * s},
		{"pre hi above 5s", &clientpb.LoginResp{PreHandshakeRetry: pbw(s, 9*s)}, func(w ServerWindows) Window { return w.PreHandshake }, s, 5 * s},
		{"pre lo above clamped hi", &clientpb.LoginResp{PreHandshakeRetry: pbw(6*s, 9*s)}, func(w ServerWindows) Window { return w.PreHandshake }, ms(250), 5 * s},
		{"retryable lo below 0.5s", &clientpb.LoginResp{Retryable: pbw(0, 4*s)}, func(w ServerWindows) Window { return w.Retryable }, ms(500), 4 * s},
		{"retryable hi below 2s", &clientpb.LoginResp{Retryable: pbw(ms(600), ms(700))}, func(w ServerWindows) Window { return w.Retryable }, ms(600), 2 * s},
		{"retryable lo equals hi", &clientpb.LoginResp{Retryable: pbw(3*s, 3*s)}, func(w ServerWindows) Window { return w.Retryable }, ms(500), 3 * s},
		{"retryable hi above 10s", &clientpb.LoginResp{Retryable: pbw(s, 60*s)}, func(w ServerWindows) Window { return w.Retryable }, s, 10 * s},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.get(ServerWindows{}.Merge(tc.resp))
			if got != (Window{tc.lo, tc.hi}) {
				t.Fatalf("got [%v, %v], want [%v, %v]", got.Lo, got.Hi, tc.lo, tc.hi)
			}
			if got.Lo >= got.Hi {
				t.Fatalf("lo %v ≥ hi %v", got.Lo, got.Hi)
			}
		})
	}
}

// An absent window keeps the current value; a present one replaces it.
func TestAbsentWindowsKeepTheirValue(t *testing.T) {
	w := ServerWindows{}.Merge(&clientpb.LoginResp{StormWait: pbw(0, 30*s), Retryable: pbw(2*s, 4*s)})
	w = w.Merge(&clientpb.LoginResp{HeartbeatS: 150, Retryable: pbw(s, 5*s)})
	if w.Storm != (Window{0, 30 * s}) || w.Retryable != (Window{s, 5 * s}) {
		t.Fatalf("got %+v", w)
	}
	eff := w.Over(DefaultWindows())
	if eff.Storm != (Window{0, 30 * s}) || eff.Local != DefaultWindows().Local ||
		eff.PreHandshake != DefaultWindows().PreHandshake || eff.Retryable != (Window{s, 5 * s}) {
		t.Fatalf("effective windows %+v", eff)
	}
}

func TestClampRepairsDamagedWindows(t *testing.T) {
	w := ServerWindows{Storm: Window{0, ms(1)}, Retryable: Window{s, 3 * s}}.Clamp()
	if w.Storm != (Window{0, 10 * s}) || w.Retryable != (Window{s, 3 * s}) || w.Local != (Window{}) {
		t.Fatalf("got %+v", w)
	}
}

// uniform fails t unless 10k draws of sample pass chiSquare's check
// (R4G-O12). The draws use a fixed seed, so the test never flakes.
func uniform(t *testing.T, name string, lo, hi time.Duration, sample func() time.Duration) {
	t.Helper()
	if chi2, count, err := chiSquare(lo, hi, sample); err != nil {
		t.Fatalf("%s: %v", name, err)
	} else if chi2 > 27.88 { // chi-square, 9 degrees of freedom, p = 0.001
		t.Fatalf("%s: chi-square %.1f > 27.88, buckets %v", name, chi2, count)
	}
}

// chiSquare draws 10k samples, buckets them in 10 equal buckets over
// [lo, hi] and returns the chi-square statistic against a uniform spread.
func chiSquare(lo, hi time.Duration, sample func() time.Duration) (float64, [10]int, error) {
	const n, buckets = 10_000, 10
	var count [buckets]int
	for range n {
		d := sample()
		if d < lo || d > hi {
			return 0, count, fmt.Errorf("drew %v outside [%v, %v]", d, lo, hi)
		}
		count[int(float64(d-lo)/float64(hi-lo+1)*buckets)]++
	}
	var chi2 float64
	for _, c := range count {
		diff := float64(c) - n/buckets
		chi2 += diff * diff / (n / buckets)
	}
	return chi2, count, nil
}

func seeded() *rand.Rand { return rand.New(rand.NewPCG(1, 2)) }

func TestWindowSampleIsUniform(t *testing.T) {
	r := seeded()
	for name, w := range map[string]Window{"storm 0–20s": DefaultWindows().Storm, "local 0–1s": DefaultWindows().Local,
		"pre-handshake 0.5–1.5s": DefaultWindows().PreHandshake, "retryable 1–3s": DefaultWindows().Retryable,
		"Reconnect 0–5s": {0, 5 * s}} {
		uniform(t, name, w.Lo, w.Hi, func() time.Duration { return w.Sample(r) })
	}
}

// The check rejects a skewed distribution, so a passing check means something.
func TestUniformityCheckRejectsASkewedSample(t *testing.T) {
	r := seeded()
	chi2, count, err := chiSquare(0, 20*s, func() time.Duration { u := r.Float64(); return time.Duration(u * u * float64(20*s)) })
	if err != nil || chi2 <= 27.88 {
		t.Fatalf("a u² sample passed: chi-square %.1f, buckets %v, %v", chi2, count, err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sdk`
Expected: FAIL to build, `undefined: ServerWindows` (and `Window`, `DefaultWindows`, …).

- [ ] **Step 3: Write the implementation**

`internal/sdk/windows.go`:

```go
// Package sdk is the Go client SDK (spec: Client SDK rules): one client
// instance that holds at most one connection, persists device_id and
// conn_seq before every attempt, and reconnects by the spec's rules — the
// 0–20s storm wait, the 0–1s local trigger, Reconnect{jitter_ms}, two
// counters for retryable rejects, exponential backoff, and the server-set
// windows from LoginResp, clamped and persisted. It is the reference the
// mobile SDKs port, and the client the race suite and loadgen drive.
package sdk

import (
	"math/rand/v2"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// Window is a uniform random wait [Lo, Hi].
type Window struct{ Lo, Hi time.Duration }

// Sample draws a wait uniformly from [Lo, Hi].
func (w Window) Sample(r *rand.Rand) time.Duration {
	if w.Hi <= w.Lo {
		return w.Lo
	}
	return w.Lo + time.Duration(r.Int64N(int64(w.Hi-w.Lo)+1))
}

// Windows are the four waits LoginResp can set (R5S-O6).
type Windows struct {
	Storm        Window // after losing an established connection (0–20s)
	Local        Window // after a network change or app foreground (0–1s)
	PreHandshake Window // after a pre-handshake close (0.5–1.5s)
	Retryable    Window // after another retryable reject (1–3s)
}

// DefaultWindows are the built-in defaults a fresh install uses.
func DefaultWindows() Windows {
	return Windows{
		Storm:        Window{0, 20 * time.Second},
		Local:        Window{0, time.Second},
		PreHandshake: Window{500 * time.Millisecond, 1500 * time.Millisecond},
		Retryable:    Window{time.Second, 3 * time.Second},
	}
}

// clampRange is the safe range built into the app for one window (roadmap
// choice #13): Hi is clamped to [HiMin, HiMax] and Lo to [LoMin, Hi); a Lo
// that would reach Hi becomes LoMin. ZeroLo pins Lo at 0.
type clampRange struct {
	LoMin, HiMin, HiMax time.Duration
	ZeroLo              bool
}

var clamps = struct{ Storm, Local, PreHandshake, Retryable clampRange }{
	// The storm upper end has a 10s floor: 200k reconnects over less than
	// 10s exceed the 20k/s headroom.
	Storm:        clampRange{0, 10 * time.Second, 120 * time.Second, true},
	Local:        clampRange{0, time.Second, 10 * time.Second, true},
	PreHandshake: clampRange{250 * time.Millisecond, time.Second, 5 * time.Second, false},
	Retryable:    clampRange{500 * time.Millisecond, 2 * time.Second, 10 * time.Second, false},
}

func (c clampRange) apply(w Window) Window {
	w.Hi = min(max(w.Hi, c.HiMin), c.HiMax)
	if c.ZeroLo {
		w.Lo = 0
		return w
	}
	w.Lo = max(w.Lo, c.LoMin)
	if w.Lo >= w.Hi {
		w.Lo = c.LoMin
	}
	return w
}

// defaultHeartbeat is used when LoginResp carries none.
const defaultHeartbeat = 150 * time.Second

// HeartbeatRange is the Ping interval the app accepts from LoginResp. The
// gateway refuses to start outside the same range (gateway.Config
// HeartbeatMin/HeartbeatMax), so the clamp only catches a broken server; a
// test timing profile lowers both ends together.
type HeartbeatRange struct{ Min, Max time.Duration }

// DefaultHeartbeatRange is 30s–300s: 30s still allows the 60s interval the
// spec measures, and 300s stays below a 350s LB idle timeout.
func DefaultHeartbeatRange() HeartbeatRange {
	return HeartbeatRange{Min: 30 * time.Second, Max: 300 * time.Second}
}

// Of is the Ping interval for resp: absent means 150s, and any value is
// clamped to the range; clamped reports a value the server sent out of range.
func (r HeartbeatRange) Of(resp *clientpb.LoginResp) (hb time.Duration, clamped bool) {
	sent := time.Duration(resp.GetHeartbeatS()) * time.Second
	if sent == 0 {
		return min(max(defaultHeartbeat, r.Min), r.Max), false
	}
	hb = min(max(sent, r.Min), r.Max)
	return hb, hb != sent
}

// ServerWindows are the windows the server has set, clamped; a zero Window
// was never set and leaves the built-in default. They are what the SDK
// persists (spec: Server-set windows).
type ServerWindows Windows

// Merge applies a LoginResp's windows: each present one is clamped and
// replaces the stored value; an absent one keeps it (R5S-O6).
func (s ServerWindows) Merge(resp *clientpb.LoginResp) ServerWindows {
	set := func(dst *Window, src *clientpb.Window, c clampRange) {
		if src != nil {
			*dst = c.apply(Window{time.Duration(src.GetLoMs()) * time.Millisecond, time.Duration(src.GetHiMs()) * time.Millisecond})
		}
	}
	set(&s.Storm, resp.GetStormWait(), clamps.Storm)
	set(&s.Local, resp.GetLocalTrigger(), clamps.Local)
	set(&s.PreHandshake, resp.GetPreHandshakeRetry(), clamps.PreHandshake)
	set(&s.Retryable, resp.GetRetryable(), clamps.Retryable)
	return s
}

// Clamp re-clamps every set window, so a damaged store can't produce a
// zero-jitter storm or an unbounded wait.
func (s ServerWindows) Clamp() ServerWindows {
	one := func(w *Window, c clampRange) {
		if *w != (Window{}) {
			*w = c.apply(*w)
		}
	}
	one(&s.Storm, clamps.Storm)
	one(&s.Local, clamps.Local)
	one(&s.PreHandshake, clamps.PreHandshake)
	one(&s.Retryable, clamps.Retryable)
	return s
}

// Over returns the windows in effect: each server-set window over base.
func (s ServerWindows) Over(base Windows) Windows {
	pick := func(set, def Window) Window {
		if set == (Window{}) {
			return def
		}
		return set
	}
	return Windows{pick(s.Storm, base.Storm), pick(s.Local, base.Local),
		pick(s.PreHandshake, base.PreHandshake), pick(s.Retryable, base.Retryable)}
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/sdk && go test -race -count=1 ./internal/sdk`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/sdk
git commit -m "feat(sdk): reconnect windows, clamp table and uniform sampling"
```

---

### Task 3: Persisted state (choice #56)

**Files:**
- Create: `internal/sdk/state.go`
- Test: `internal/sdk/state_test.go`

**Interfaces:**
- Consumes: `ServerWindows` (Task 2).
- Produces: `type State struct{ DeviceID string; ConnSeq uint64; Windows ServerWindows }`; `type Store interface{ Load() (State, error); Save(State) error }`; `var ErrCorrupt`; `type Locker interface{ Lock() (unlock func(), err error) }`; `var ErrStoreInUse`; `type FileStore struct{ Path string }` (`Load` moves an unparseable file to `.corrupt` and returns `ErrCorrupt`; `Lock` is an exclusive `flock` on `<path>.lock`); `type MemStore struct{ Err error; … }` (Err fails every `Save`); `NewDeviceID() string`.

`device_id` and `conn_seq` live in one record, so clearing one clears the other (R1-O5). `FileStore.Save` is durable when it returns (temp file, fsync, rename, directory fsync), because `conn_seq` is saved before every dial.

- [ ] **Step 1: Write the failing test**

`internal/sdk/state_test.go`:

```go
package sdk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreRoundTrip(t *testing.T) {
	f := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	if st, err := f.Load(); err != nil || st != (State{}) {
		t.Fatalf("Load of a fresh install = %+v, %v; want the zero State", st, err)
	}
	want := State{DeviceID: NewDeviceID(), ConnSeq: 42,
		Windows: ServerWindows{Storm: Window{0, 30 * s}, Retryable: Window{s, 5 * s}}}
	if err := f.Save(want); err != nil {
		t.Fatal(err)
	}
	if got, err := f.Load(); err != nil || got != want {
		t.Fatalf("Load = %+v, %v; want %+v", got, err, want)
	}
	if _, err := os.Stat(f.Path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary file left behind: %v", err)
	}
}

// A record that can't be parsed is ErrCorrupt and moved aside; the next Load
// sees a fresh install.
func TestFileStoreMovesACorruptFileAside(t *testing.T) {
	f := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	os.WriteFile(f.Path, []byte("{not json"), 0o600)
	if _, err := f.Load(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load of a corrupt file = %v, want ErrCorrupt", err)
	}
	if b, err := os.ReadFile(f.Path + ".corrupt"); err != nil || string(b) != "{not json" {
		t.Fatalf("corrupt file not kept: %q, %v", b, err)
	}
	if st, err := f.Load(); err != nil || st != (State{}) {
		t.Fatalf("Load after the corrupt file = %+v, %v; want a fresh install", st, err)
	}
}

// A file that can't be read is not corrupt: it is kept as it is.
func TestFileStoreUnreadableIsNotCorrupt(t *testing.T) {
	f := FileStore{Path: t.TempDir()} // a directory: read fails
	if _, err := f.Load(); err == nil || errors.Is(err, ErrCorrupt) {
		t.Fatalf("Load = %v, want an error that is not ErrCorrupt", err)
	}
}

func TestFileStoreLockIsExclusive(t *testing.T) {
	f := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	unlock, err := f.Lock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Lock(); !errors.Is(err, ErrStoreInUse) {
		t.Fatalf("second Lock = %v, want ErrStoreInUse", err)
	}
	unlock()
	unlock, err = f.Lock()
	if err != nil {
		t.Fatalf("Lock after unlock: %v", err)
	}
	unlock()
}

func TestNewDeviceIDIsRandom(t *testing.T) {
	a, b := NewDeviceID(), NewDeviceID()
	if len(a) != 32 || a == b {
		t.Fatalf("device ids %q, %q", a, b)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sdk`
Expected: FAIL to build, `undefined: FileStore` (and `State`, `NewDeviceID`).

- [ ] **Step 3: Write the implementation**

`internal/sdk/state.go`:

```go
package sdk

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// State is what the SDK persists: device_id and conn_seq together, so
// clearing one clears the other (R1-O5), and the windows the server set
// (R5S-O6).
type State struct {
	DeviceID string        `json:"device_id"`
	ConnSeq  uint64        `json:"conn_seq"`
	Windows  ServerWindows `json:"windows"`
}

// Store persists State. Save must be durable when it returns: conn_seq is
// saved before every connection attempt.
type Store interface {
	// Load returns the zero State for a store never saved, and an error
	// matching ErrCorrupt for a record that can't be parsed. Any other error
	// means the record can't be read yet (an iOS Keychain before the first
	// unlock): the client keeps it and tries again.
	Load() (State, error)
	Save(State) error
}

// ErrCorrupt is a stored record that can't be parsed: the client starts as a
// fresh install (Review Focus 1).
var ErrCorrupt = errors.New("sdk: stored state is corrupt")

// Locker is a Store that can be locked: New takes the lock, so at most one
// Client uses a record, and so at most one connection per device_id is in
// use (spec: device_id). Close releases it.
type Locker interface {
	Lock() (unlock func(), err error)
}

// ErrStoreInUse is a store another Client holds.
var ErrStoreInUse = errors.New("sdk: store in use by another client")

// NewDeviceID returns a random device_id (16 bytes, hex).
func NewDeviceID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// FileStore keeps State in one JSON file, the place an app keeps device_id
// (on iOS a native SDK would use the Keychain for both, and its own lock
// across the app and its extensions).
type FileStore struct{ Path string }

// Load reads the record. A file that can't be parsed is moved aside to
// Path + ".corrupt", kept for diagnosis, and reported as ErrCorrupt.
func (f FileStore) Load() (State, error) {
	b, err := os.ReadFile(f.Path)
	if errors.Is(err, fs.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, err
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		if rerr := os.Rename(f.Path, f.Path+".corrupt"); rerr != nil {
			return State{}, rerr // not moved aside: keep it, and try again
		}
		return State{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return st, nil
}

// Lock takes an exclusive flock on Path + ".lock", held until unlock.
func (f FileStore) Lock() (func(), error) {
	l, err := os.OpenFile(f.Path+".lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(l.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		l.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrStoreInUse
		}
		return nil, err
	}
	return func() { l.Close() }, nil
}

// Save writes a temporary file, syncs it and renames it over the old one,
// so a crash leaves either the old State or the new one.
func (f FileStore) Save(st State) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := f.Path + ".tmp"
	w, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := w.Write(b); err != nil {
		w.Close()
		return err
	}
	if err := w.Sync(); err != nil {
		w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, f.Path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(f.Path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// MemStore keeps State in memory (tests, loadgen). Err, if set, fails every Save.
type MemStore struct {
	mu  sync.Mutex
	st  State
	Err error
}

func (m *MemStore) Load() (State, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.st, nil
}

func (m *MemStore) Save(st State) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.st = st
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/sdk && go test -race -count=1 ./internal/sdk`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/sdk
git commit -m "feat(sdk): persist device_id, conn_seq and server windows in one record"
```

---

### Task 4: The reconnect policy (choices #49, #50, #51, #54)

**Files:**
- Create: `internal/sdk/policy.go`
- Test: `internal/sdk/policy_test.go`

**Interfaces:**
- Consumes: `Window`, `Windows`, `DefaultWindows` (Task 2); test helpers `s`, `ms`, `seeded`, `uniform` (Task 2).
- Produces:
  - unexported: `type endKind` with `endLost`, `endPongTimeout`, `endReconnect`, `endKicked`, `endPreHandshake`, `endBusy`, `endRejected`, `endNoReply`, `endFailed`; `type end struct{ kind endKind; retryAfter, jitter time.Duration; kick clientpb.KickReason; code clientpb.ErrorCode; storedSeq uint64; err error }`; `newPolicy(Windows, Backoff, *rand.Rand) *policy`; `(*policy) after(end) (Wait, bool)` (false: stay idle); `local(cur Wait) (Wait, bool)`; `localWait() Wait`; `loggedIn()`; `first() clientpb.ReconnectReason`; field `w Windows` (the client updates it after a `LoginResp`); constants `preHandshakeLimit = 10`, `retryableLimit = 5`.
  - exported: `type WaitKind` (`WaitStorm`, `WaitLocal`, `WaitDrain`, `WaitPreHandshake`, `WaitRetryable`, `WaitBackoff`, `WaitFailure`, with `String()`); `type Wait struct{ Kind WaitKind; D time.Duration; Reason clientpb.ReconnectReason }`; `type Backoff struct{ Base, Max time.Duration }`; `DefaultBackoff() Backoff` (5s, 5m).
  - unexported, added at the plan review: `codeRule` (`ruleBackoff`, `ruleRetryable`, `ruleRevoked`) and `ruleOf(clientpb.ErrorCode)`; `maxDrainJitter` (120s); `(*policy) anomaly(end) error` (items 4, 5).
  - test helpers: `within(t, step, w, kind, lo, hi, reason)`, `after(t, p, e) Wait`.

The policy does no I/O: the client feeds it each ending and arms the wait it returns. Choice #49 maps each Login error to a rule, #50 sets the backoff, #51 says what a local trigger cuts, and #54 says which `reconnect_reason` each wait leaves for the attempt after it.

- [ ] **Step 1: Write the failing test**

`internal/sdk/policy_test.go`:

```go
package sdk

import (
	"fmt"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

func newTestPolicy() *policy { return newPolicy(DefaultWindows(), DefaultBackoff(), seeded()) }

var (
	preClose  = end{kind: endPreHandshake}
	expired   = end{kind: endRejected, code: clientpb.ErrorCode_ERROR_CODE_EXPIRED}
	keyUnkn   = end{kind: endRejected, code: clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN}
	noReply   = end{kind: endNoReply}
	dialError = end{kind: endFailed}
	busyRetry = clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY
)

func busy(d time.Duration) end { return end{kind: endBusy, retryAfter: d} }

// within fails t unless w has the kind, a duration in [lo, hi] and the reason.
func within(t *testing.T, step string, w Wait, kind WaitKind, lo, hi time.Duration, reason clientpb.ReconnectReason) {
	t.Helper()
	if w.Kind != kind || w.D < lo || w.D > hi || w.Reason != reason {
		t.Fatalf("%s: got %v %v %v, want %v in [%v, %v] %v", step, w.Kind, w.D, w.Reason, kind, lo, hi, reason)
	}
}

func after(t *testing.T, p *policy, e end) Wait {
	t.Helper()
	w, ok := p.after(e)
	if !ok {
		t.Fatalf("after %+v: idle, want a wait", e)
	}
	return w
}

// Pre-handshake closes retry after 0.5–1.5s, and fall back to exponential
// backoff after 10 in a row (R5S-O4).
func TestPreHandshakeClosesRetryQuicklyThenBackOff(t *testing.T) {
	p := newTestPolicy()
	for i := range 10 {
		within(t, fmt.Sprint("close ", i+1), after(t, p, preClose), WaitPreHandshake, ms(500), ms(1500), busyRetry)
	}
	within(t, "close 11", after(t, p, preClose), WaitBackoff, ms(2500), 5*s, busyRetry)
	within(t, "close 12", after(t, p, preClose), WaitBackoff, 5*s, 10*s, busyRetry)
}

// Busy, EXPIRED, KEY_UNKNOWN and a close before any reply share one count of
// 5 in a row (R3-O5, R4H-O2, R4H-O7, R4H-O10).
func TestOtherRetryableRejectsShareOneCounterOfFive(t *testing.T) {
	p := newTestPolicy()
	within(t, "Busy 2s", after(t, p, busy(2*s)), WaitRetryable, 2*s, 2*s, busyRetry) // retry_after obeyed
	within(t, "EXPIRED", after(t, p, expired), WaitRetryable, s, 3*s, busyRetry)
	within(t, "KEY_UNKNOWN", after(t, p, keyUnkn), WaitRetryable, s, 3*s, busyRetry)
	within(t, "close before LoginResp", after(t, p, noReply), WaitRetryable, s, 3*s, busyRetry)
	within(t, "Busy without retry_after", after(t, p, busy(0)), WaitRetryable, s, 3*s, busyRetry)
	within(t, "6th: EXPIRED", after(t, p, expired), WaitBackoff, ms(2500), 5*s, busyRetry)
	// Backoff never undercuts a server's retry_after.
	within(t, "7th: Busy 30s", after(t, p, busy(30*s)), WaitBackoff, 30*s, 30*s, busyRetry)
}

// The other rejects choice #49 puts on the second counter.
func TestStaleSupersededAndTokenInvalidAreRetryable(t *testing.T) {
	for _, code := range []clientpb.ErrorCode{clientpb.ErrorCode_ERROR_CODE_STALE,
		clientpb.ErrorCode_ERROR_CODE_SUPERSEDED, clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID} {
		p := newTestPolicy()
		for range 5 {
			within(t, code.String(), after(t, p, end{kind: endRejected, code: code}), WaitRetryable, s, 3*s, busyRetry)
		}
		within(t, code.String()+" 6th", after(t, p, end{kind: endRejected, code: code}), WaitBackoff, ms(2500), 5*s, busyRetry)
	}
}

// An outcome of one kind resets the other kind's counter.
func TestAnOutcomeOfOneKindResetsTheOtherCounter(t *testing.T) {
	p := newTestPolicy()
	for range 4 {
		after(t, p, expired)
	}
	for range 9 {
		after(t, p, preClose)
	}
	// The pre-handshake close reset the 4 rejects: 5 more are still quick.
	for range 5 {
		within(t, "reject after closes", after(t, p, busy(0)), WaitRetryable, s, 3*s, busyRetry)
	}
	// The rejects reset the 9 closes: 10 more are still quick.
	for range 10 {
		within(t, "close after rejects", after(t, p, preClose), WaitPreHandshake, ms(500), ms(1500), busyRetry)
	}
	within(t, "11th close", after(t, p, preClose), WaitBackoff, 0, 5*time.Minute, busyRetry)
}

// Both counters, and the backoff, reset on a successful login.
func TestLoginResetsBothCountersAndTheBackoff(t *testing.T) {
	p := newTestPolicy()
	for range 12 {
		after(t, p, preClose)
	}
	for range 4 {
		after(t, p, expired)
	}
	p.loggedIn()
	for range 10 {
		within(t, "close after login", after(t, p, preClose), WaitPreHandshake, ms(500), ms(1500), busyRetry)
	}
	within(t, "first backoff after login", after(t, p, preClose), WaitBackoff, ms(2500), 5*s, busyRetry)
	p.loggedIn()
	for range 5 {
		within(t, "reject after login", after(t, p, expired), WaitRetryable, s, 3*s, busyRetry)
	}
}

func TestBackoffDoublesToFiveMinutes(t *testing.T) {
	p := newTestPolicy()
	want := []time.Duration{5 * s, 10 * s, 20 * s, 40 * s, 80 * s, 160 * s, 300 * s, 300 * s}
	for i, d := range want {
		within(t, fmt.Sprint("step ", i+1), after(t, p, dialError), WaitFailure, d/2, d, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
	}
	for range 100 { // no overflow however long it fails
		after(t, p, dialError)
	}
	within(t, "step 108", after(t, p, dialError), WaitFailure, 150*s, 300*s, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
}

// After losing an established connection: the 0–20s storm wait with the
// loss as the reason, then exponential backoff that keeps the reason.
func TestLostConnectionWaitsInTheStormWindowThenBacksOff(t *testing.T) {
	p := newTestPolicy()
	within(t, "RST/FIN", after(t, p, end{kind: endLost}), WaitStorm, 0, 20*s, clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN)
	within(t, "dial fails", after(t, p, dialError), WaitFailure, ms(2500), 5*s, clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN)
	within(t, "Pong timeout", after(t, p, end{kind: endPongTimeout}), WaitStorm, 0, 20*s, clientpb.ReconnectReason_RECONNECT_REASON_PONG_TIMEOUT)
}

// After Reconnect the attempt falls within [0, jitter_ms] and carries the
// drain reason (R4H-O3).
func TestReconnectWaitsWithinItsJitterWithTheDrainReason(t *testing.T) {
	p := newTestPolicy()
	within(t, "Reconnect 5s", after(t, p, end{kind: endReconnect, jitter: 5 * s}), WaitDrain, 0, 5*s, clientpb.ReconnectReason_RECONNECT_REASON_DRAIN)
	within(t, "Reconnect 0", after(t, p, end{kind: endReconnect}), WaitDrain, 0, 0, clientpb.ReconnectReason_RECONNECT_REASON_DRAIN)
}

// No Kick reconnects on its own, and REVOKED stops (spec: Kick reasons,
// races 9 and 10); BAD_REQUEST never loops fast.
func TestKicksAndRevokedAreIdleAndBadRequestBacksOff(t *testing.T) {
	p := newTestPolicy()
	for _, r := range []clientpb.KickReason{clientpb.KickReason_KICK_REASON_REPLACED,
		clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, clientpb.KickReason_KICK_REASON_REVOKED} {
		if w, ok := p.after(end{kind: endKicked, kick: r}); ok {
			t.Fatalf("Kick %v: wait %+v, want idle", r, w)
		}
	}
	if w, ok := p.after(end{kind: endRejected, code: clientpb.ErrorCode_ERROR_CODE_REVOKED}); ok {
		t.Fatalf("REVOKED: wait %+v, want idle", w)
	}
	within(t, "BAD_REQUEST", after(t, p, end{kind: endRejected, code: clientpb.ErrorCode_ERROR_CODE_BAD_REQUEST}),
		WaitBackoff, ms(2500), 5*s, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
	within(t, "unknown code", after(t, p, end{kind: endRejected, code: 99}),
		WaitBackoff, 5*s, 10*s, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
}

// The policy decides which endings are anomalies (choice #49, review items
// 4 and 5).
func TestAnomalies(t *testing.T) {
	p := newTestPolicy()
	for _, tc := range []struct {
		name string
		e    end
		want bool
	}{
		{"BAD_REQUEST", end{kind: endRejected, code: clientpb.ErrorCode_ERROR_CODE_BAD_REQUEST}, true},
		{"code 0", end{kind: endRejected}, true},
		{"code 99", end{kind: endRejected, code: 99}, true},
		{"EXPIRED", expired, false},
		{"REVOKED", end{kind: endRejected, code: clientpb.ErrorCode_ERROR_CODE_REVOKED}, false},
		{"Kick REPLACED", end{kind: endKicked, kick: clientpb.KickReason_KICK_REASON_REPLACED}, true},
		{"Kick reason 9", end{kind: endKicked, kick: 9}, true},
		{"Kick KICKED_BY_OTHER_DEVICE", end{kind: endKicked, kick: clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}, false},
		{"Kick REVOKED", end{kind: endKicked, kick: clientpb.KickReason_KICK_REASON_REVOKED}, false},
		{"Busy 3s", busy(3 * s), false},
		{"Busy 1h", busy(time.Hour), true},
		{"Reconnect 120s", end{kind: endReconnect, jitter: 120 * s}, false},
		{"Reconnect 1h", end{kind: endReconnect, jitter: time.Hour}, true},
		{"lost", end{kind: endLost}, false},
	} {
		if got := p.anomaly(tc.e) != nil; got != tc.want {
			t.Errorf("%s: anomaly %v, want %v", tc.name, got, tc.want)
		}
	}
}

// retry_after is capped at the backoff cap and jitter_ms at the largest
// storm window, so a broken server can't park clients (review item 5).
func TestServerWaitsAreCapped(t *testing.T) {
	p := newTestPolicy()
	within(t, "Busy 1h", after(t, p, busy(time.Hour)), WaitRetryable, 5*time.Minute, 5*time.Minute, busyRetry)
	for range 100 {
		within(t, "Reconnect 1h", after(t, p, end{kind: endReconnect, jitter: time.Hour}), WaitDrain, 0, 120*s,
			clientpb.ReconnectReason_RECONNECT_REASON_DRAIN)
	}
}

// A local trigger cuts only the storm wait, and a failure backoff (choice
// #51); never Busy, Reconnect, the quick retries or backoff after a limit
// (spec: Precedence).
func TestLocalTriggerCutsOnlyTheStormWaitAndAFailureBackoff(t *testing.T) {
	p := newTestPolicy()
	for _, kind := range []WaitKind{WaitStorm, WaitFailure} {
		w, ok := p.local(Wait{Kind: kind, D: 15 * s})
		if !ok {
			t.Fatalf("%v: not cut", kind)
		}
		within(t, kind.String(), w, WaitLocal, 0, s, clientpb.ReconnectReason_RECONNECT_REASON_LOCAL_TRIGGER)
	}
	for _, kind := range []WaitKind{WaitDrain, WaitPreHandshake, WaitRetryable, WaitBackoff, WaitLocal} {
		cur := Wait{Kind: kind, D: 15 * s, Reason: busyRetry}
		if w, ok := p.local(cur); ok || w != cur {
			t.Fatalf("%v: cut to %+v", kind, w)
		}
	}
}

// Each jittered wait, drawn through the policy ~10k times, is uniform
// (R4G-O12).
func TestJitteredWaitsAreUniform(t *testing.T) {
	p := newTestPolicy()
	draw := func(e end) func() time.Duration {
		return func() time.Duration { p.loggedIn(); return after(t, p, e).D }
	}
	uniform(t, "storm 0–20s", 0, 20*s, draw(end{kind: endLost}))
	uniform(t, "retryable 1–3s", s, 3*s, draw(expired))
	uniform(t, "pre-handshake 0.5–1.5s", ms(500), ms(1500), draw(preClose))
	uniform(t, "Reconnect [0, 5s]", 0, 5*s, draw(end{kind: endReconnect, jitter: 5 * s}))
	uniform(t, "local 0–1s", 0, s, func() time.Duration { return p.localWait().D })
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sdk`
Expected: FAIL to build, `undefined: policy` (and `end`, `endPreHandshake`, …).

- [ ] **Step 3: Write the implementation**

`internal/sdk/policy.go`:

```go
package sdk

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// endKind is what ended a connection attempt or an established connection.
type endKind int

const (
	endLost         endKind = iota + 1 // an established connection closed or reset (RST/FIN)
	endPongTimeout                     // no frame within the Pong timeout of a Ping
	endReconnect                       // Reconnect{jitter_ms} from a draining gateway
	endKicked                          // Kick{reason}
	endPreHandshake                    // closed before the handshake reply
	endBusy                            // Busy{retry_after}
	endRejected                        // Error{code}
	endNoReply                         // closed after the handshake, before LoginResp or Error
	endFailed                          // dial, token, persistence or handshake failure
)

// end is one ending, with what the server sent.
type end struct {
	kind       endKind
	retryAfter time.Duration       // endBusy; 0: none sent
	jitter     time.Duration       // endReconnect
	kick       clientpb.KickReason // endKicked
	code       clientpb.ErrorCode  // endRejected
	storedSeq  uint64              // endRejected STALE
	err        error               // the cause, for events (a failure, an undecodable reply)
}

// WaitKind says which rule chose a wait.
type WaitKind int

const (
	WaitStorm        WaitKind = iota + 1 // after losing an established connection (0–20s)
	WaitLocal                            // after a local trigger (0–1s)
	WaitDrain                            // after Reconnect: [0, jitter_ms]
	WaitPreHandshake                     // after a pre-handshake close (0.5–1.5s)
	WaitRetryable                        // after another retryable reject (1–3s, or retry_after)
	WaitBackoff                          // a retry limit was reached, or BAD_REQUEST
	WaitFailure                          // backoff after a failure that never reached a gateway's logic
)

var waitNames = map[WaitKind]string{WaitStorm: "storm", WaitLocal: "local", WaitDrain: "drain",
	WaitPreHandshake: "pre-handshake", WaitRetryable: "retryable", WaitBackoff: "backoff", WaitFailure: "failure"}

func (k WaitKind) String() string { return waitNames[k] }

// Wait is the pause before the next attempt and the reconnect_reason that
// attempt carries.
type Wait struct {
	Kind   WaitKind
	D      time.Duration
	Reason clientpb.ReconnectReason
}

// Backoff is exponential backoff: step n waits uniformly in [d/2, d] with
// d = min(Max, Base × 2^(n−1)). Steps restart at a successful login.
type Backoff struct{ Base, Max time.Duration }

// DefaultBackoff starts at 2.5–5s and reaches the 5m cap at step 7.
func DefaultBackoff() Backoff { return Backoff{Base: 5 * time.Second, Max: 5 * time.Minute} }

// Retry limits (spec: Retryable rejects): quick retries before backoff.
const (
	preHandshakeLimit = 10
	retryableLimit    = 5
)

// maxDrainJitter caps Reconnect{jitter_ms}: the largest storm window (the
// clamp table). A planned move never waits longer than a storm could.
var maxDrainJitter = clamps.Storm.HiMax

// codeRule is what a Login error code leads to (choice #49).
type codeRule int

const (
	ruleBackoff   codeRule = iota // BAD_REQUEST, or a code this SDK doesn't know: backoff and an anomaly
	ruleRetryable                 // the second counter
	ruleRevoked                   // drop the token, show the login screen
)

func ruleOf(code clientpb.ErrorCode) codeRule {
	switch code {
	case clientpb.ErrorCode_ERROR_CODE_STALE, clientpb.ErrorCode_ERROR_CODE_SUPERSEDED,
		clientpb.ErrorCode_ERROR_CODE_EXPIRED, clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN,
		clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID:
		return ruleRetryable
	case clientpb.ErrorCode_ERROR_CODE_REVOKED:
		return ruleRevoked
	}
	return ruleBackoff
}

// policy decides every wait (spec: Client SDK rules). It does no I/O; the
// client feeds it each ending.
type policy struct {
	w       Windows
	backoff Backoff
	r       *rand.Rand

	pre, other int                      // pre-handshake closes and other retryable rejects in a row
	steps      int                      // backoff steps since the last login
	reason     clientpb.ReconnectReason // the reason of the last wait; a backoff keeps it
}

func newPolicy(w Windows, b Backoff, r *rand.Rand) *policy {
	return &policy{w: w, backoff: b, r: r, reason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}
}

// loggedIn resets both counters and the backoff (spec: both reset on a
// successful login).
func (p *policy) loggedIn() { p.pre, p.other, p.steps = 0, 0, 0 }

// first is the reason of an attempt that follows no wait: a first connect,
// or one the user asked for.
func (p *policy) first() clientpb.ReconnectReason {
	p.reason = clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT
	return p.reason
}

// after returns the wait that follows e, or ok false: stay idle until the
// user or the app acts (a Kick, REVOKED).
func (p *policy) after(e end) (w Wait, ok bool) {
	switch e.kind {
	case endLost:
		return p.wait(WaitStorm, p.w.Storm.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN), true
	case endPongTimeout:
		return p.wait(WaitStorm, p.w.Storm.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_PONG_TIMEOUT), true
	case endReconnect:
		return p.wait(WaitDrain, Window{0, min(e.jitter, maxDrainJitter)}.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_DRAIN), true
	case endKicked:
		return Wait{}, false
	case endPreHandshake:
		p.other, p.pre = 0, p.pre+1
		if p.pre > preHandshakeLimit {
			return p.next(WaitBackoff, 0, clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY), true
		}
		return p.wait(WaitPreHandshake, p.w.PreHandshake.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY), true
	case endRejected:
		switch ruleOf(e.code) {
		case ruleRevoked:
			return Wait{}, false
		case ruleRetryable:
			return p.retryable(0), true
		default: // never a fast loop
			return p.next(WaitBackoff, 0, p.reason), true
		}
	case endBusy:
		return p.retryable(min(e.retryAfter, p.backoff.Max)), true
	case endNoReply:
		return p.retryable(0), true
	default: // endFailed
		return p.next(WaitFailure, 0, p.reason), true
	}
}

// anomaly says why e is worth logging, or nil: BAD_REQUEST and unknown
// codes (an undecodable Error reads as code 0), REPLACED and unknown Kick
// reasons, and a retry_after or jitter_ms after cuts to its cap.
func (p *policy) anomaly(e end) error {
	var why error
	switch {
	case e.kind == endRejected && ruleOf(e.code) == ruleBackoff:
		why = fmt.Errorf("sdk: Login error %v", e.code)
	case e.kind == endKicked && e.kick != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE &&
		e.kick != clientpb.KickReason_KICK_REASON_REVOKED:
		why = fmt.Errorf("sdk: Kick %v", e.kick)
	case e.kind == endBusy && e.retryAfter > p.backoff.Max:
		why = fmt.Errorf("sdk: retry_after %v capped at %v", e.retryAfter, p.backoff.Max)
	case e.kind == endReconnect && e.jitter > maxDrainJitter:
		why = fmt.Errorf("sdk: jitter_ms %v capped at %v", e.jitter, maxDrainJitter)
	default:
		return nil
	}
	if e.err != nil {
		return errors.Join(why, e.err)
	}
	return why
}

// retryable is the second counter: Busy, EXPIRED, KEY_UNKNOWN and a close
// before any reply share it. A retry_after from the server is obeyed, also
// as a floor once backoff takes over.
func (p *policy) retryable(retryAfter time.Duration) Wait {
	p.pre, p.other = 0, p.other+1
	busy := clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY
	if p.other > retryableLimit {
		return p.next(WaitBackoff, retryAfter, busy)
	}
	if retryAfter > 0 {
		return p.wait(WaitRetryable, retryAfter, busy)
	}
	return p.wait(WaitRetryable, p.w.Retryable.Sample(p.r), busy)
}

// next is one more backoff step, at least floor.
func (p *policy) next(kind WaitKind, floor time.Duration, reason clientpb.ReconnectReason) Wait {
	p.steps++
	d := p.backoff.Max
	if p.steps < 32 {
		d = min(d, p.backoff.Base<<(p.steps-1))
	}
	return p.wait(kind, max(floor, Window{d / 2, d}.Sample(p.r)), reason)
}

func (p *policy) wait(kind WaitKind, d time.Duration, reason clientpb.ReconnectReason) Wait {
	p.reason = reason
	return Wait{Kind: kind, D: d, Reason: reason}
}

// local is a network change or the app returning to the foreground during
// cur. It cuts only the storm wait (spec: Precedence), and a failure backoff,
// which no server asked for (roadmap choice #51); ok false leaves cur alone.
func (p *policy) local(cur Wait) (Wait, bool) {
	if cur.Kind != WaitStorm && cur.Kind != WaitFailure {
		return cur, false
	}
	return p.localWait(), true
}

// localWait is the 0–1s wait of a local trigger.
func (p *policy) localWait() Wait {
	return p.wait(WaitLocal, p.w.Local.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_LOCAL_TRIGGER)
}
```

- [ ] **Step 4: Run the tests**

Run: `go vet ./internal/sdk && go test -race -count=1 ./internal/sdk`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
git add internal/sdk
git commit -m "feat(sdk): reconnect policy with two retry counters and backoff"
```

---

### Task 5: Attempts, the client run loop and events (choices #49, #51, #53–#57)

**Files:**
- Create: `internal/sdk/attempt.go`, `internal/sdk/client.go`
- Test: `internal/sdk/fake_test.go`, `internal/sdk/client_test.go`, `internal/sdk/layer_test.go`

**Interfaces:**
- Consumes:
  - `clientconn.New`, `Conn.Handshake`/`Send`/`Recv`/`Close`, `Frame.Decode`, `ErrClosedBeforeReply` (Task 1);
  - `Windows`, `ServerWindows`, `DefaultWindows` (Task 2);
  - `State`, `Store`, `MemStore`, `FileStore`, `NewDeviceID` (Task 3);
  - `policy`, `end`, `Wait`, `WaitKind`, `Backoff`, `DefaultBackoff` (Task 4);
  - test helpers `s`, `ms`, `pbw`, `seeded`, `within` (Tasks 2 and 4);
  - in the fake gateway: `wire.GenerateKey`, `wire.Respond`, `Session.ReadFrame`/`WriteFrame`.
- Produces:
  - `type Config struct{ Addrs []string; KeyID uint16; ServerKey []byte; AppID string; Platform clientpb.Platform; Token func(ctx context.Context, refresh bool) (string, error); Store Store; OnEvent func(Event); Windows Windows; Backoff Backoff; Heartbeat HeartbeatRange; TokenTimeout, PongTimeout, ConnectTimeout, LoginTimeout, LogoutTimeout time.Duration; Dial func(context.Context, string) (net.Conn, error); Rand *rand.Rand }`; `DefaultConfig() Config`.
  - `type EventKind` (`EventConnecting`, `EventOnline`, `EventWaiting`, `EventIdle`, `EventLoginRequired`, `EventLoggedOut`, `EventAnomaly`, with `String()`); `type Attempt struct{ Addr string; Reason clientpb.ReconnectReason; ConnSeq uint64; OfflineMs int64 }`; `type Event struct{ Kind EventKind; Attempt Attempt; Resp *clientpb.LoginResp; Wait Wait; Kick clientpb.KickReason; Code clientpb.ErrorCode; Err error }`.
  - `type Client`; `New(Config) (*Client, error)` (fails with `ErrStoreInUse` on a locked store); `(*Client) DeviceID() string` ("" while the store can't be read); `Connect()`; `NetworkChanged()`; `Foreground()`; `Logout()`; `SwitchAccount(token func(context.Context, bool) (string, error))`; `Close()`.
  - unexported: `attempt` (with `refresh`, `loggingOut`), `attemptMsg` (with `note`), `endLogoutResp`, `token`, `loginReply`, `tcpDial`; `(*Client) load`, `fail`.
  - test helpers in `fake_test.go`: `fakeGW` (`newFake`, `dial`, `dials`), `fakeConn` (`hello`, `login`, `online`, `send`, `recv`, `pong`, `drain`), `rec` (`add`, `take`), `testClient`, `kinds`, `expect`; in `client_test.go`: `unreadable`.

One goroutine owns all client state. Each public method runs a function on it and waits. An attempt runs on its own goroutine and reports `online`, then its `end`; a cancelled attempt reports nothing. Choice #53 sets the timeouts, #55 the Ping cadence and Pong timeout, #56 when state is saved, and #57 the API. Every test runs a whole client against `fakeGW` inside a `synctest` bubble: `fakeGW.dial` returns one end of a `net.Pipe` and serves the other end with the test's script.

- [ ] **Step 1: Write the fake gateway and the failing tests**

`internal/sdk/fake_test.go`:

```go
package sdk

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
	"google.golang.org/protobuf/proto"
)

// fakeGW is a scripted gateway on net.Pipe, so a whole client runs inside a
// synctest bubble on fake time. serve runs once per connection (n counts
// from 1) on the connection's own goroutine.
type fakeGW struct {
	t     *testing.T
	key   wire.Key
	serve func(n int, c *fakeConn)

	mu    sync.Mutex
	n     int
	addrs []string
}

func newFake(t *testing.T, serve func(n int, c *fakeConn)) *fakeGW {
	t.Helper()
	key, err := wire.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return &fakeGW{t: t, key: key, serve: serve}
}

func (f *fakeGW) dial(_ context.Context, addr string) (net.Conn, error) {
	client, server := net.Pipe()
	f.mu.Lock()
	f.n++
	n := f.n
	f.addrs = append(f.addrs, addr)
	f.mu.Unlock()
	go func() {
		defer server.Close()
		f.serve(n, &fakeConn{t: f.t, nc: server, key: f.key})
	}()
	return client, nil
}

// dials is the number of connections the client opened.
func (f *fakeGW) dials() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

// fakeConn is the server side of one connection.
type fakeConn struct {
	t    *testing.T
	nc   net.Conn
	key  wire.Key
	sess *wire.Session
}

// hello reads frame 1 without answering it.
func (c *fakeConn) hello() bool {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	_, err := io.ReadFull(c.nc, f)
	return err == nil
}

// login answers the handshake and returns the LoginReq, or nil.
func (c *fakeConn) login() *clientpb.LoginReq {
	f := make([]byte, wire.HeaderLen+wire.HandshakeLen)
	if _, err := io.ReadFull(c.nc, f); err != nil {
		return nil
	}
	sess, reply, err := wire.Respond(c.key, f, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		c.t.Error(err)
		return nil
	}
	c.sess = sess
	if _, err := c.nc.Write(reply); err != nil {
		return nil
	}
	cmd, _, body, err := sess.ReadFrame(c.nc)
	if err != nil || clientpb.Cmd(cmd) != clientpb.Cmd_CMD_LOGIN {
		return nil
	}
	var req clientpb.LoginReq
	if err := proto.Unmarshal(body, &req); err != nil {
		c.t.Error(err)
		return nil
	}
	return &req
}

// online logs the client in with resp (nil: heartbeat 150s, no windows).
func (c *fakeConn) online(resp *clientpb.LoginResp) *clientpb.LoginReq {
	req := c.login()
	if req == nil {
		return nil
	}
	if resp == nil {
		resp = &clientpb.LoginResp{HeartbeatS: 150}
	}
	c.send(clientpb.Cmd_CMD_LOGIN_RESP, 1, resp)
	return req
}

func (c *fakeConn) send(cmd clientpb.Cmd, reqID uint32, msg proto.Message) {
	var body []byte
	if msg != nil {
		body, _ = proto.Marshal(msg)
	}
	c.sess.WriteFrame(c.nc, uint16(cmd), reqID, body)
}

// recv reads one client frame.
func (c *fakeConn) recv() (clientpb.Cmd, error) {
	cmd, _, _, err := c.sess.ReadFrame(c.nc)
	return clientpb.Cmd(cmd), err
}

// pong answers every Ping until the client closes.
func (c *fakeConn) pong() {
	for {
		cmd, id, _, err := c.sess.ReadFrame(c.nc)
		if err != nil {
			return
		}
		if clientpb.Cmd(cmd) == clientpb.Cmd_CMD_PING {
			c.sess.WriteFrame(c.nc, uint16(clientpb.Cmd_CMD_PONG), id, nil)
		}
	}
}

// drain reads and drops client frames until the client closes.
func (c *fakeConn) drain() { io.Copy(io.Discard, c.nc) }

// rec records events.
type rec struct {
	mu  sync.Mutex
	evs []Event
	got int // events already returned by take
}

func (r *rec) add(e Event) {
	r.mu.Lock()
	r.evs = append(r.evs, e)
	r.mu.Unlock()
}

// take waits until the bubble is idle, then returns the events since the
// last take.
func (r *rec) take() []Event {
	synctest.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	evs := r.evs[r.got:]
	r.got = len(r.evs)
	return evs
}

// testClient starts a client against f inside the bubble; it is closed when
// the bubble's function returns.
func testClient(t *testing.T, f *fakeGW, store *MemStore, mod func(*Config)) (*Client, *rec) {
	t.Helper()
	r := &rec{}
	cfg := DefaultConfig()
	cfg.Addrs, cfg.KeyID, cfg.ServerKey = []string{"gw-a:8000"}, 1, f.key.Public
	cfg.AppID, cfg.Platform = "app", clientpb.Platform_PLATFORM_MOBILE
	cfg.Token = func(context.Context, bool) (string, error) { return "tok", nil }
	cfg.Store, cfg.Dial, cfg.Rand, cfg.OnEvent = store, f.dial, seeded(), r.add
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, r
}

// kinds lists the events' kinds, for failure messages and comparisons.
func kinds(evs []Event) []EventKind {
	out := make([]EventKind, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

// expect fails t unless evs have exactly these kinds, and returns them.
func expect(t *testing.T, evs []Event, want ...EventKind) []Event {
	t.Helper()
	got := kinds(evs)
	if len(got) != len(want) {
		t.Fatalf("events %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("events %v, want %v", got, want)
		}
	}
	return evs
}
```

`internal/sdk/client_test.go`:

```go
package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

var (
	reasonFirst = clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT
	reasonRST   = clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN
	reasonPong  = clientpb.ReconnectReason_RECONNECT_REASON_PONG_TIMEOUT
	reasonLocal = clientpb.ReconnectReason_RECONNECT_REASON_LOCAL_TRIGGER
	reasonDrain = clientpb.ReconnectReason_RECONNECT_REASON_DRAIN
)

// A first connect persists device_id and the next conn_seq before it dials
// (R1-O5), and carries FIRST_CONNECT with offline_ms 0.
func TestFirstConnectPersistsBeforeItDials(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		reqs := make(chan *clientpb.LoginReq, 1)
		var storedAtLogin State
		f := newFake(t, func(_ int, c *fakeConn) {
			storedAtLogin, _ = store.Load()
			reqs <- c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, store, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventOnline)
		req := <-reqs
		if req.DeviceId != c.DeviceID() || req.ConnSeq != 1 || req.ReconnectReason != reasonFirst ||
			req.OfflineMs != 0 || req.Token != "tok" || req.Appid != "app" || req.Platform != clientpb.Platform_PLATFORM_MOBILE {
			t.Fatalf("LoginReq %v", req)
		}
		if storedAtLogin.DeviceID != c.DeviceID() || storedAtLogin.ConnSeq != 1 {
			t.Fatalf("store at dial time %+v, want device %s conn_seq 1", storedAtLogin, c.DeviceID())
		}
		if evs[0].Attempt.ConnSeq != 1 || evs[0].Attempt.Reason != reasonFirst {
			t.Fatalf("attempt %+v", evs[0].Attempt)
		}
		c.Connect() // online: no second attempt
		if r.take(); f.dials() != 1 {
			t.Fatalf("%d dials, want 1", f.dials())
		}
	})
}

// Losing an established connection waits 0–20s, then reconnects with
// RST_FIN and the offline time; conn_seq goes up by one per attempt.
func TestLostConnectionWaitsTheStormWindowThenReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reqs := make(chan *clientpb.LoginReq, 2)
		f := newFake(t, func(n int, c *fakeConn) {
			reqs <- c.online(nil)
			if n == 2 {
				c.pong()
			} // n == 1: the gateway dies (FIN)
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		w := evs[2].Wait
		within(t, "storm", w, WaitStorm, 0, 20*s, reasonRST)
		<-reqs
		time.Sleep(w.D - time.Millisecond)
		expect(t, r.take())
		time.Sleep(time.Millisecond)
		expect(t, r.take(), EventConnecting, EventOnline)
		if req := <-reqs; req.ReconnectReason != reasonRST || req.ConnSeq != 2 || req.OfflineMs != w.D.Milliseconds() {
			t.Fatalf("relogin %v, want RST_FIN, conn_seq 2, offline_ms %d", req, w.D.Milliseconds())
		}
	})
}

// No frame within 10s of a Ping: the client closes and waits the storm
// window with PONG_TIMEOUT (spec: Dead-server detection). A gateway that
// answers keeps the connection.
func TestPongTimeoutClosesAndReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pings := make(chan time.Time, 10)
		f := newFake(t, func(n int, c *fakeConn) {
			c.online(&clientpb.LoginResp{HeartbeatS: 30})
			if n == 2 {
				c.pong()
				return
			}
			for { // n == 1: a dead server that still holds the socket
				cmd, err := c.recv()
				if err != nil {
					return
				}
				if cmd == clientpb.Cmd_CMD_PING {
					pings <- time.Now()
				}
			}
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		start := time.Now()
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline)
		time.Sleep(40*time.Second - time.Millisecond)
		expect(t, r.take())
		if p := <-pings; p.Sub(start) != 30*time.Second {
			t.Fatalf("first Ping at %v, want 30s (the heartbeat)", p.Sub(start))
		}
		time.Sleep(time.Millisecond)
		evs := expect(t, r.take(), EventWaiting)
		within(t, "after the Pong timeout", evs[0].Wait, WaitStorm, 0, 20*s, reasonPong)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		time.Sleep(10 * time.Minute) // 20 heartbeats, each answered
		expect(t, r.take())
	})
}

// A network change during the 0–20s wait reconnects within 0–1s with
// LOCAL_TRIGGER (R4H-O3).
func TestNetworkChangeDuringTheStormWaitReconnectsWithinOneSecond(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reqs := make(chan *clientpb.LoginReq, 2)
		f := newFake(t, func(n int, c *fakeConn) {
			reqs <- c.online(nil)
			if n == 2 {
				c.pong()
			}
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		<-reqs
		time.Sleep(100 * time.Millisecond)
		c.NetworkChanged()
		evs := expect(t, r.take(), EventWaiting)
		within(t, "local", evs[0].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		if req := <-reqs; req.ReconnectReason != reasonLocal || req.OfflineMs != (100*time.Millisecond+evs[0].Wait.D).Milliseconds() {
			t.Fatalf("relogin %v", req)
		}
	})
}

// A network change does not shorten a Busy{retry_after} wait (spec:
// Precedence), nor a Reconnect wait.
func TestNetworkChangeDoesNotShortenRetryAfterOrReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			switch n {
			case 1:
				c.login()
				c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{RetryAfterMs: 7000})
			case 2:
				c.online(nil)
				c.send(clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: 60_000})
				c.drain()
			default:
				c.online(nil)
				c.pong()
			}
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		within(t, "Busy", evs[1].Wait, WaitRetryable, 7*s, 7*s, clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY)
		c.NetworkChanged()
		c.Foreground()
		time.Sleep(7*time.Second - time.Millisecond)
		expect(t, r.take())
		time.Sleep(time.Millisecond)
		evs = expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		w := evs[2].Wait
		within(t, "Reconnect", w, WaitDrain, 0, 60*s, reasonDrain)
		c.NetworkChanged()
		time.Sleep(w.D - time.Millisecond)
		expect(t, r.take())
		time.Sleep(time.Millisecond)
		evs = expect(t, r.take(), EventConnecting, EventOnline)
		if evs[0].Attempt.Reason != reasonDrain {
			t.Fatalf("attempt after Reconnect %+v, want DRAIN", evs[0].Attempt)
		}
	})
}

// On Reconnect the client closes the connection itself (R4H-O3).
func TestReconnectClosesTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closed := make(chan error, 1)
		f := newFake(t, func(n int, c *fakeConn) {
			if n > 1 {
				return
			}
			c.online(nil)
			c.send(clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: 5000})
			_, err := c.recv()
			closed <- err
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		r.take()
		if err := <-closed; err == nil {
			t.Fatal("the client kept the connection after Reconnect")
		}
	})
}

// Logout, account switch and app close each cancel a pending retry (R4H-O2).
func TestStopConditionsCancelAPendingRetry(t *testing.T) {
	busy := func(_ int, c *fakeConn) {
		c.login()
		c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{RetryAfterMs: 2000})
	}
	for _, tc := range []struct {
		name string
		stop func(*Client)
	}{
		{"logout", (*Client).Logout},
		{"app close", (*Client).Close},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, busy)
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				expect(t, r.take(), EventConnecting, EventWaiting)
				tc.stop(c)
				time.Sleep(time.Hour)
				r.take()
				if f.dials() != 1 {
					t.Fatalf("%d dials after %s, want 1", f.dials(), tc.name)
				}
			})
		})
	}
	t.Run("account switch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			reqs := make(chan *clientpb.LoginReq, 2)
			f := newFake(t, func(n int, c *fakeConn) {
				if n == 1 {
					busy(n, c)
					return
				}
				reqs <- c.online(nil)
				c.pong()
			})
			c, r := testClient(t, f, &MemStore{}, nil)
			c.Connect()
			expect(t, r.take(), EventConnecting, EventWaiting)
			c.SwitchAccount(func(context.Context, bool) (string, error) { return "tok-b", nil })
			expect(t, r.take(), EventLoggedOut, EventConnecting, EventOnline)
			if req := <-reqs; req.Token != "tok-b" || req.ReconnectReason != reasonFirst {
				t.Fatalf("after the switch %v", req)
			}
			time.Sleep(time.Hour)
			if r.take(); f.dials() != 2 {
				t.Fatalf("%d dials, want 2: the Busy retry must be cancelled", f.dials())
			}
		})
	})
}

// Logout sends Logout, waits for LogoutResp, closes and stays idle.
func TestLogoutSendsLogoutAndStaysIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := make(chan clientpb.Cmd, 1)
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			cmd, _ := c.recv()
			got <- cmd
			c.send(clientpb.Cmd_CMD_LOGOUT_RESP, 1, nil)
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline)
		start := time.Now()
		c.Logout()
		if d := time.Since(start); d != 0 {
			t.Fatalf("Logout took %v; LogoutResp came at once", d)
		}
		if cmd := <-got; cmd != clientpb.Cmd_CMD_LOGOUT {
			t.Fatalf("gateway got %v, want Logout", cmd)
		}
		expect(t, r.take(), EventLoggedOut)
		time.Sleep(time.Hour)
		expect(t, r.take())
		c.Foreground()
		c.NetworkChanged()
		if r.take(); f.dials() != 1 {
			t.Fatal("reconnected after Logout without Connect")
		}
	})
}

// A gateway that never answers Logout costs at most LogoutTimeout.
func TestLogoutWaitsAtMostLogoutTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) { c.online(nil); c.drain() })
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		r.take()
		start := time.Now()
		c.Logout()
		if d := time.Since(start); d != 2*time.Second {
			t.Fatalf("Logout took %v, want the 2s LogoutTimeout", d)
		}
	})
}

// No Kick reconnects on its own (races 9, 10). REPLACED is an anomaly and
// ends on Foreground; KICKED_BY_OTHER_DEVICE only on Connect; REVOKED asks
// for a new login.
func TestKicksDoNotReconnect(t *testing.T) {
	for _, tc := range []struct {
		reason clientpb.KickReason
		want   []EventKind
		wakes  func(*Client) // brings the client back, or nil
		wakeTo clientpb.ReconnectReason
	}{
		{clientpb.KickReason_KICK_REASON_REPLACED, []EventKind{EventAnomaly, EventIdle}, (*Client).Foreground, reasonLocal},
		{clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, []EventKind{EventIdle}, (*Client).Connect, reasonFirst},
		{clientpb.KickReason_KICK_REASON_REVOKED, []EventKind{EventLoginRequired}, (*Client).Connect, reasonFirst},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(n int, c *fakeConn) {
					c.online(nil)
					if n == 1 {
						c.send(clientpb.Cmd_CMD_KICK, 0, &clientpb.Kick{Reason: tc.reason})
						return
					}
					c.pong()
				})
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				evs := expect(t, r.take(), append([]EventKind{EventConnecting, EventOnline}, tc.want...)...)
				if last := evs[len(evs)-1]; last.Kick != tc.reason {
					t.Fatalf("event %+v, want Kick %v", last, tc.reason)
				}
				c.NetworkChanged()
				if tc.reason != clientpb.KickReason_KICK_REASON_REPLACED {
					c.Foreground()
				}
				time.Sleep(time.Hour)
				expect(t, r.take())
				tc.wakes(c)
				time.Sleep(time.Second)
				evs = r.take()
				if f.dials() != 2 || evs[len(evs)-1].Kind != EventOnline {
					t.Fatalf("after waking: %d dials, events %v", f.dials(), kinds(evs))
				}
				for _, e := range evs {
					if e.Kind == EventConnecting && e.Attempt.Reason != tc.wakeTo {
						t.Fatalf("woke with %v, want %v", e.Attempt.Reason, tc.wakeTo)
					}
				}
			})
		})
	}
}

// A Kick in place of the Login reply (kick-pending, R1-O8) is handled the
// same way.
func TestKickInsteadOfTheLoginReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.login()
			c.send(clientpb.Cmd_CMD_KICK, 0, &clientpb.Kick{Reason: clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE})
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting, EventIdle)
	})
}

// STALE sets the counter to max(local, stored) + 1, persisted (R1-O5).
func TestStaleMovesConnSeqPastTheStoredOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		reqs := make(chan *clientpb.LoginReq, 2)
		f := newFake(t, func(n int, c *fakeConn) {
			if n == 1 {
				c.login()
				c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_STALE, StoredConnSeq: 41})
				return
			}
			reqs <- c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, store, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		if evs[1].Code != clientpb.ErrorCode_ERROR_CODE_STALE {
			t.Fatalf("waiting event %+v, want Code STALE", evs[1])
		}
		if st, _ := store.Load(); st.ConnSeq != 41 {
			t.Fatalf("stored conn_seq %d after STALE, want 41", st.ConnSeq)
		}
		time.Sleep(evs[1].Wait.D)
		r.take()
		if req := <-reqs; req.ConnSeq != 42 {
			t.Fatalf("retry conn_seq %d, want 42", req.ConnSeq)
		}
	})
}

// Each server behaviour lands in its rule (spec: Retryable rejects).
func TestEachEndingIsClassified(t *testing.T) {
	for _, tc := range []struct {
		name  string
		serve func(*fakeConn)
		kind  WaitKind
		lo    time.Duration
		hi    time.Duration
		after time.Duration // time the ending takes
	}{
		{"closed before the handshake reply", func(c *fakeConn) { c.hello() }, WaitPreHandshake, ms(500), ms(1500), 0},
		{"closed after the handshake, before any reply", func(c *fakeConn) { c.login() }, WaitRetryable, s, 3 * s, 0},
		{"no reply within LoginTimeout", func(c *fakeConn) { c.login(); c.drain() }, WaitRetryable, s, 3 * s, 10 * s},
		{"EXPIRED", func(c *fakeConn) {
			c.login()
			c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_EXPIRED})
		}, WaitRetryable, s, 3 * s, 0},
		{"KEY_UNKNOWN", func(c *fakeConn) {
			c.login()
			c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN})
		}, WaitRetryable, s, 3 * s, 0},
		{"Busy without retry_after", func(c *fakeConn) { c.login(); c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{}) }, WaitRetryable, s, 3 * s, 0},
		{"handshake reply that fails", func(c *fakeConn) { c.hello(); c.nc.Write(make([]byte, 54)) }, WaitFailure, ms(2500), 5 * s, 0},
		{"no handshake reply within ConnectTimeout", func(c *fakeConn) { c.hello(); c.drain() }, WaitFailure, ms(2500), 5 * s, 10 * s},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(_ int, c *fakeConn) { tc.serve(c) })
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				time.Sleep(tc.after)
				evs := expect(t, r.take(), EventConnecting, EventWaiting)
				w := evs[1].Wait
				if w.Kind != tc.kind || w.D < tc.lo || w.D > tc.hi {
					t.Fatalf("wait %+v, want %v in [%v, %v]", w, tc.kind, tc.lo, tc.hi)
				}
			})
		})
	}
}

// The counters run across real attempts: 10 quick pre-handshake retries,
// then backoff.
func TestPreHandshakeClosesReachBackoffAfterTen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) { c.hello() })
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		var waits []WaitKind
		for len(waits) < 12 {
			for _, e := range r.take() {
				if e.Kind == EventWaiting {
					waits = append(waits, e.Wait.Kind)
					time.Sleep(e.Wait.D)
				}
			}
		}
		for i, k := range waits {
			if want := map[bool]WaitKind{true: WaitPreHandshake, false: WaitBackoff}[i < 10]; k != want {
				t.Fatalf("wait %d is %v, want %v (all: %v)", i+1, k, want, waits)
			}
		}
	})
}

// Windows from LoginResp are used, clamped and persisted across app
// restarts; absent fields keep the current value (R5S-O6).
func TestServerWindowsAreUsedAndPersistedAcrossRestarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		f := newFake(t, func(n int, c *fakeConn) {
			switch n {
			case 1:
				c.online(&clientpb.LoginResp{HeartbeatS: 150, StormWait: pbw(0, 5*s), Retryable: pbw(4*s, 6*s)})
			case 2:
				c.online(&clientpb.LoginResp{HeartbeatS: 150}) // no windows: keep them
				c.pong()
			default:
				c.login()
				c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_EXPIRED})
			}
		})
		c, r := testClient(t, f, store, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		// Persisted when LoginResp arrives, not only with the next conn_seq:
		// an app killed while online restarts with them.
		want := ServerWindows{Storm: Window{0, 10 * s}, Retryable: Window{4 * s, 6 * s}} // 5s clamped to 10s
		if st, _ := store.Load(); st.Windows != want || st.ConnSeq != 1 {
			t.Fatalf("persisted %+v, want windows %+v at conn_seq 1", st, want)
		}
		within(t, "storm after the server's window", evs[2].Wait, WaitStorm, 0, 10*s, reasonRST)
		time.Sleep(evs[2].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		if st, _ := store.Load(); st.Windows != want {
			t.Fatalf("a LoginResp without windows changed them: %+v", st.Windows)
		}
		c.Close()

		// The app restarts on the same store: the next wait uses the
		// persisted window before any LoginResp.
		c2, r2 := testClient(t, f, store, nil)
		c2.Connect()
		evs = expect(t, r2.take(), EventConnecting, EventWaiting)
		within(t, "EXPIRED after restart", evs[1].Wait, WaitRetryable, 4*s, 6*s, clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY)
	})
}

// A conn_seq that can't be persisted is never used: no dial, a backoff.
func TestFailedSaveSkipsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		f := newFake(t, func(_ int, c *fakeConn) { c.online(nil); c.pong() })
		c, r := testClient(t, f, store, nil) // New saves the device_id first
		store.Err = errors.New("disk full")
		c.Connect()
		evs := expect(t, r.take(), EventAnomaly, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure || f.dials() != 0 {
			t.Fatalf("wait %+v, %d dials; want a failure backoff and no dial", evs[1].Wait, f.dials())
		}
		store.mu.Lock()
		store.Err = nil
		store.mu.Unlock()
		time.Sleep(evs[1].Wait.D)
		evs = expect(t, r.take(), EventConnecting, EventOnline)
		if evs[0].Attempt.ConnSeq != 1 {
			t.Fatalf("conn_seq %d, want 1: the failed one was never used", evs[0].Attempt.ConnSeq)
		}
	})
}

// New refuses a config without the essentials and gives zero timing fields
// their defaults, so a zero PongTimeout can't close every connection at once.
func TestNewChecksAndCompletesTheConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New accepted an empty config")
	}
	c, err := New(Config{Addrs: []string{"gw:8000"}, ServerKey: make([]byte, 32), Store: &MemStore{},
		Token: func(context.Context, bool) (string, error) { return "", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	def := DefaultConfig()
	if c.cfg.PongTimeout != def.PongTimeout || c.cfg.ConnectTimeout != def.ConnectTimeout ||
		c.cfg.LoginTimeout != def.LoginTimeout || c.cfg.LogoutTimeout != def.LogoutTimeout ||
		c.cfg.TokenTimeout != def.TokenTimeout || c.cfg.Heartbeat != def.Heartbeat ||
		c.cfg.Backoff != def.Backoff || c.cfg.Windows != def.Windows {
		t.Fatalf("config not completed: %+v", c.cfg)
	}
	if c.DeviceID() == "" {
		t.Fatal("no device_id on a fresh install")
	}
	if _, err := New(Config{Addrs: []string{"gw:8000"}, ServerKey: make([]byte, 32), Store: &MemStore{},
		Token:     func(context.Context, bool) (string, error) { return "", nil },
		Heartbeat: HeartbeatRange{Min: time.Minute, Max: time.Second}}); err == nil {
		t.Fatal("New accepted an inverted heartbeat range")
	}
}

// Review Focus 1: a corrupt record starts the client as a fresh install
// instead of keeping the app offline. The anomaly is the run loop's first
// event.
func TestCorruptStoreStartsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
		os.WriteFile(store.Path, []byte("{not json"), 0o600)
		r := &rec{}
		cfg := DefaultConfig()
		cfg.Addrs, cfg.ServerKey, cfg.Store, cfg.OnEvent = []string{"gw:8000"}, make([]byte, 32), store, r.add
		cfg.Token = func(context.Context, bool) (string, error) { return "", nil }
		c, err := New(cfg)
		if err != nil {
			t.Fatalf("New on a corrupt store: %v", err)
		}
		defer c.Close()
		if evs := expect(t, r.take(), EventAnomaly); !errors.Is(evs[0].Err, ErrCorrupt) {
			t.Fatalf("anomaly %v, want ErrCorrupt", evs[0].Err)
		}
		if st, err := store.Load(); err != nil || st.DeviceID != c.DeviceID() || c.DeviceID() == "" {
			t.Fatalf("store after New: %+v, %v; want the new device_id saved", st, err)
		}
	})
}

// unreadable is a store whose Load fails while err is set, as an iOS
// Keychain does before the first unlock.
type unreadable struct {
	MemStore
	err error
}

func (u *unreadable) Load() (State, error) {
	u.mu.Lock()
	err := u.err
	u.mu.Unlock()
	if err != nil {
		return State{}, err
	}
	return u.MemStore.Load()
}

// A store that can't be read yet keeps its record: no new device_id, no
// Save, no dial, and the client reads it again before the next attempt.
func TestUnreadableStoreKeepsItsRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &unreadable{err: errors.New("keychain locked")}
		store.MemStore.Save(State{DeviceID: "dev-1", ConnSeq: 7})
		reqs := make(chan *clientpb.LoginReq, 1)
		f := newFake(t, func(_ int, c *fakeConn) { reqs <- c.online(nil); c.pong() })
		c, r := testClient(t, f, &store.MemStore, func(cfg *Config) { cfg.Store = store })
		expect(t, r.take(), EventAnomaly)
		if c.DeviceID() != "" {
			t.Fatalf("device_id %q before the store was read", c.DeviceID())
		}
		c.Connect()
		evs := expect(t, r.take(), EventAnomaly, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure || f.dials() != 0 {
			t.Fatalf("wait %+v, %d dials; want a failure backoff and no dial", evs[1].Wait, f.dials())
		}
		if st, _ := store.MemStore.Load(); st != (State{DeviceID: "dev-1", ConnSeq: 7}) {
			t.Fatalf("record changed while unreadable: %+v", st)
		}
		store.mu.Lock()
		store.err = nil
		store.mu.Unlock()
		time.Sleep(evs[1].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		if req := <-reqs; req.DeviceId != "dev-1" || req.ConnSeq != 8 || c.DeviceID() != "dev-1" {
			t.Fatalf("login %v, device %q; want dev-1 at conn_seq 8", req, c.DeviceID())
		}
	})
}

// Two clients on one FileStore would share a device_id: the second New fails.
func TestSecondClientOnTheSameStoreIsRefused(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Addrs, cfg.ServerKey = []string{"gw:8000"}, make([]byte, 32)
	cfg.Store = FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	cfg.Token = func(context.Context, bool) (string, error) { return "", nil }
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg); !errors.Is(err, ErrStoreInUse) {
		t.Fatalf("second New = %v, want ErrStoreInUse", err)
	}
	a.Close()
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New after Close: %v", err)
	}
	if b.DeviceID() != a.DeviceID() {
		t.Fatalf("device_id %q after restart, want %q", b.DeviceID(), a.DeviceID())
	}
	b.Close()
}

// After TOKEN_INVALID the next Token call asks for a new token; once a Login
// is answered otherwise, it doesn't (choice #49, review item 2).
func TestTokenRefreshAfterTokenInvalid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			switch n {
			case 1:
				c.login()
				c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID})
			case 2:
				c.hello() // a pre-handshake close: no Login reply, refresh stays
			case 3:
				c.online(nil) // n == 3 dies after login
			default:
				c.online(nil)
				c.pong()
			}
		})
		var mu sync.Mutex
		var got []bool
		c, r := testClient(t, f, &MemStore{}, func(cfg *Config) {
			cfg.Token = func(_ context.Context, refresh bool) (string, error) {
				mu.Lock()
				got = append(got, refresh)
				mu.Unlock()
				return "tok", nil
			}
		})
		c.Connect()
		for f.dials() < 4 {
			for _, e := range r.take() {
				if e.Kind == EventWaiting {
					time.Sleep(e.Wait.D)
				}
			}
		}
		r.take()
		mu.Lock()
		defer mu.Unlock()
		if want := []bool{false, true, true, false}; fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("refresh %v, want %v", got, want)
		}
	})
}

// A Token call that hangs costs TokenTimeout, then a failure backoff that a
// network change cuts short (review item 3).
func TestTokenTimeoutIsAFailureBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) { c.online(nil); c.pong() })
		var calls atomic.Int32
		c, r := testClient(t, f, &MemStore{}, func(cfg *Config) {
			cfg.Token = func(ctx context.Context, _ bool) (string, error) {
				if calls.Add(1) == 1 {
					<-ctx.Done() // a token server on a dead network
					return "", ctx.Err()
				}
				return "tok", nil
			}
		})
		c.Connect()
		time.Sleep(10*time.Second - time.Millisecond)
		expect(t, r.take(), EventConnecting)
		time.Sleep(time.Millisecond)
		evs := expect(t, r.take(), EventWaiting)
		if evs[0].Wait.Kind != WaitFailure || !errors.Is(evs[0].Err, context.DeadlineExceeded) || f.dials() != 0 {
			t.Fatalf("wait %+v err %v, %d dials; want a failure backoff after the token timeout", evs[0].Wait, evs[0].Err, f.dials())
		}
		c.NetworkChanged()
		evs = expect(t, r.take(), EventWaiting)
		within(t, "network change", evs[0].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
	})
}

// An unknown or undecodable Error backs off with an anomaly (choice #49,
// review item 4).
func TestUnknownErrorCodeBacksOffWithAnAnomaly(t *testing.T) {
	for name, body := range map[string][]byte{"code 99": {0x08, 99}, "undecodable": {0xff}} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(_ int, c *fakeConn) {
					c.login()
					c.sess.WriteFrame(c.nc, uint16(clientpb.Cmd_CMD_ERROR), 1, body)
				})
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				evs := expect(t, r.take(), EventConnecting, EventAnomaly, EventWaiting)
				if evs[1].Err == nil || evs[2].Wait.Kind != WaitBackoff {
					t.Fatalf("anomaly %+v, wait %+v; want an anomaly and a backoff", evs[1], evs[2].Wait)
				}
			})
		})
	}
}

// A heartbeat outside the app's range is clamped with an anomaly; a test
// profile lowers the range (review item 6).
func TestHeartbeatIsClampedToItsRange(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sent    uint32
		rng     HeartbeatRange
		first   time.Duration
		anomaly bool
	}{
		{"5s below 30s", 5, DefaultHeartbeatRange(), 30 * s, true},
		{"1h above 300s", 3600, DefaultHeartbeatRange(), 300 * s, true},
		{"test profile 5s", 5, HeartbeatRange{Min: 5 * s, Max: 30 * s}, 5 * s, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pings := make(chan time.Time, 1)
				f := newFake(t, func(_ int, c *fakeConn) {
					c.online(&clientpb.LoginResp{HeartbeatS: tc.sent})
					if cmd, err := c.recv(); err == nil && cmd == clientpb.Cmd_CMD_PING {
						pings <- time.Now()
					}
					c.pong()
				})
				c, r := testClient(t, f, &MemStore{}, func(cfg *Config) { cfg.Heartbeat = tc.rng })
				start := time.Now()
				c.Connect()
				want := []EventKind{EventConnecting, EventOnline}
				if tc.anomaly {
					want = []EventKind{EventConnecting, EventAnomaly, EventOnline}
				}
				expect(t, r.take(), want...)
				if p := <-pings; p.Sub(start) != tc.first {
					t.Fatalf("first Ping at %v, want %v", p.Sub(start), tc.first)
				}
			})
		})
	}
}

// A LogoutResp the client didn't ask for is ignored with an anomaly; the
// connection stays (review item 7).
func TestUnsolicitedLogoutRespIsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			c.send(clientpb.Cmd_CMD_LOGOUT_RESP, 1, nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(10 * time.Minute)
		expect(t, r.take(), EventConnecting, EventOnline, EventAnomaly)
		if f.dials() != 1 {
			t.Fatalf("%d dials, want 1", f.dials())
		}
	})
}

// Review Focus 2: a frame the SDK doesn't know (sub-project 2 adds message
// frames) is ignored and keeps the connection.
func TestUnknownServerFramesAreIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			c.send(clientpb.Cmd(999), 0, nil)
			c.send(clientpb.Cmd_CMD_PONG, 77, nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(10 * time.Minute)
		expect(t, r.take(), EventConnecting, EventOnline)
	})
}

// Review Focus 3: a LoginResp without a heartbeat uses the 150s default.
func TestMissingHeartbeatUsesTheDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pings := make(chan time.Time, 1)
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(&clientpb.LoginResp{})
			if cmd, err := c.recv(); err == nil && cmd == clientpb.Cmd_CMD_PING {
				pings <- time.Now()
			}
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		start := time.Now()
		c.Connect()
		r.take()
		time.Sleep(151 * time.Second)
		if p := <-pings; p.Sub(start) != 150*time.Second {
			t.Fatalf("first Ping at %v, want 150s", p.Sub(start))
		}
	})
}

// Review Focus 4: a blackholed gateway (no handshake reply) costs one
// ConnectTimeout, then a failure backoff that a network change cuts short.
func TestBlackholedGatewayBacksOffAndANetworkChangeCutsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			if n == 1 {
				c.hello()
				c.drain()
				return
			}
			c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(10 * time.Second)
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure {
			t.Fatalf("wait %+v, want a failure backoff", evs[1].Wait)
		}
		c.NetworkChanged()
		evs = expect(t, r.take(), EventWaiting)
		within(t, "network change", evs[0].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
	})
}

// Review Focus 5: Logout while the Login is in flight closes at once (the
// gateway's fence (a) sends the Logout after the Login reply) and never
// reconnects.
func TestLogoutDuringAnAttemptClosesItAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closed := make(chan error, 1)
		f := newFake(t, func(_ int, c *fakeConn) {
			c.login() // no reply: the Login is "in flight"
			_, err := c.recv()
			closed <- err
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting)
		start := time.Now()
		c.Logout()
		if d := time.Since(start); d != 0 {
			t.Fatalf("Logout took %v, want no wait: nothing is logged in", d)
		}
		if err := <-closed; err == nil {
			t.Fatal("the connection stayed open after Logout")
		}
		time.Sleep(time.Hour)
		expect(t, r.take(), EventLoggedOut)
		if f.dials() != 1 {
			t.Fatalf("%d dials, want 1", f.dials())
		}
	})
}
```

`internal/sdk/layer_test.go`:

```go
package sdk

import (
	"os/exec"
	"strings"
	"testing"
)

// The SDK is the client side only: it depends on the client protocol and
// nothing of the server's (gateway, Logic, store, gRPC, Redis, gnet), so
// it stays the reference a mobile SDK ports. Test files may import the
// server to run end to end.
func TestSDKDependsOnlyOnTheClientSide(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "github.com/Tallinzp/goim/internal/sdk").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	allowed := map[string]bool{
		"github.com/Tallinzp/goim/api/clientpb":        true,
		"github.com/Tallinzp/goim/internal/wire":       true,
		"github.com/Tallinzp/goim/internal/clientconn": true,
		"github.com/Tallinzp/goim/internal/sdk":        true,
	}
	for _, dep := range strings.Fields(string(out)) {
		if strings.HasPrefix(dep, "github.com/Tallinzp/goim/") && !allowed[dep] ||
			strings.HasPrefix(dep, "google.golang.org/grpc") || strings.HasPrefix(dep, "github.com/redis/") ||
			strings.HasPrefix(dep, "github.com/panjf2000/") || strings.HasPrefix(dep, "github.com/prometheus/") {
			t.Fatalf("internal/sdk depends on %s", dep)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./internal/sdk`
Expected: FAIL to build, `undefined: Event` (and `Client`, `Config`, `New`, …).

- [ ] **Step 3: Write the attempt**

`internal/sdk/attempt.go`:

```go
package sdk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/clientconn"
)

// endLogoutResp ends a connection the client logged out of; the policy
// never sees it.
const endLogoutResp endKind = 100

// attempt is one connection attempt and, once it logs in, the connection.
// Its goroutine reports to the client's run loop; cancel aborts it.
type attempt struct {
	cancel     context.CancelFunc
	refresh    bool             // Token's refresh argument
	cc         *clientconn.Conn // set before the attempt reports it is online
	loggingOut atomic.Bool      // the client sent Logout: LogoutResp ends the connection
}

// attemptMsg is what an attempt's goroutine reports: online (resp set),
// then an end, or only an end; note, at any time, is an anomaly to log.
type attemptMsg struct {
	a    *attempt
	resp *clientpb.LoginResp
	end  end
	note error
}

// run dials addr, logs in with req and then serves the connection until it
// ends, reporting to out.
func (a *attempt) run(ctx context.Context, cfg *Config, addr string, req *clientpb.LoginReq, out chan<- attemptMsg) {
	defer a.cancel()
	// A cancelled attempt reports nothing: the run loop has dropped it, or
	// has stopped.
	report := func(m attemptMsg) bool {
		select {
		case out <- m:
			return true
		case <-ctx.Done():
			return false
		}
	}
	resp, e := a.login(ctx, cfg, addr, req)
	if resp == nil {
		report(attemptMsg{a: a, end: e})
		return
	}
	if report(attemptMsg{a: a, resp: resp}) {
		report(attemptMsg{a: a, end: a.serve(ctx, cfg, resp, report)})
	}
}

// login runs the attempt up to the Login reply and classifies every way it
// can end (spec: Retryable rejects).
func (a *attempt) login(ctx context.Context, cfg *Config, addr string, req *clientpb.LoginReq) (*clientpb.LoginResp, end) {
	tok, err := token(ctx, cfg, a.refresh)
	if err != nil {
		return nil, end{kind: endFailed, err: err}
	}
	req.Token = tok
	connectBy := time.Now().Add(cfg.ConnectTimeout) // dial and handshake together
	dctx, cancel := context.WithDeadline(ctx, connectBy)
	defer cancel()
	nc, err := cfg.Dial(dctx, addr)
	if err != nil {
		return nil, end{kind: endFailed, err: err}
	}
	context.AfterFunc(ctx, func() { nc.Close() }) // cancel aborts any read or write
	a.cc = clientconn.New(nc)
	switch err := a.cc.Handshake(cfg.KeyID, cfg.ServerKey, connectBy); {
	case errors.Is(err, clientconn.ErrClosedBeforeReply):
		nc.Close()
		return nil, end{kind: endPreHandshake, err: err}
	case err != nil:
		nc.Close()
		return nil, end{kind: endFailed, err: err}
	}
	if err := a.cc.Send(clientpb.Cmd_CMD_LOGIN, 1, req); err != nil {
		nc.Close()
		return nil, end{kind: endNoReply, err: err}
	}
	deadline := time.Now().Add(cfg.LoginTimeout)
	for {
		f, err := a.cc.Recv(deadline)
		if err != nil { // closed, reset, timed out or undecryptable: no reply
			nc.Close()
			return nil, end{kind: endNoReply, err: err}
		}
		if resp, e, done := loginReply(f); done {
			if resp == nil {
				nc.Close()
			}
			return resp, e
		}
	}
}

// token calls cfg.Token and stops waiting after cfg.TokenTimeout: a token
// server that hangs on a dead network must not hold the attempt, which no
// local trigger can cut short.
func token(ctx context.Context, cfg *Config, refresh bool) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, cfg.TokenTimeout)
	defer cancel()
	type result struct {
		tok string
		err error
	}
	got := make(chan result, 1)
	go func() {
		tok, err := cfg.Token(ctx, refresh)
		got <- result{tok, err}
	}()
	select {
	case r := <-got:
		return r.tok, r.err
	case <-ctx.Done():
		return "", fmt.Errorf("sdk: token: %w", ctx.Err())
	}
}

// loginReply classifies a frame received while waiting for the Login reply;
// done false means keep waiting.
func loginReply(f clientconn.Frame) (resp *clientpb.LoginResp, e end, done bool) {
	switch f.Cmd {
	case clientpb.Cmd_CMD_LOGIN_RESP:
		resp = &clientpb.LoginResp{}
		if err := f.Decode(resp); err != nil {
			return nil, end{kind: endNoReply, err: err}, true
		}
		return resp, end{}, true
	case clientpb.Cmd_CMD_ERROR: // undecodable: code 0, an unknown code
		var m clientpb.Error
		err := f.Decode(&m)
		return nil, end{kind: endRejected, code: m.GetCode(), storedSeq: m.GetStoredConnSeq(), err: err}, true
	case clientpb.Cmd_CMD_BUSY:
		var m clientpb.Busy
		err := f.Decode(&m)
		return nil, end{kind: endBusy, retryAfter: time.Duration(m.GetRetryAfterMs()) * time.Millisecond, err: err}, true
	case clientpb.Cmd_CMD_KICK: // kick-pending: the Kick replaces the Login reply
		var m clientpb.Kick
		err := f.Decode(&m)
		return nil, end{kind: endKicked, kick: m.GetReason(), err: err}, true
	}
	return nil, end{}, false
}

// serve runs a logged-in connection until it ends: one Ping per heartbeat
// interval, and a close when no frame arrives within PongTimeout of a Ping
// (spec: Dead-server detection). Kick and Reconnect end it; the client
// closes on Reconnect (R4H-O3). A LogoutResp ends it only after the client
// sent Logout; any other one is reported as a note and ignored.
func (a *attempt) serve(ctx context.Context, cfg *Config, resp *clientpb.LoginResp, report func(attemptMsg) bool) end {
	hb, _ := cfg.Heartbeat.Of(resp)
	start := time.Now()
	var lastRecv atomic.Int64 // ns since start of the last frame received
	var pongTimedOut atomic.Bool
	pctx, stopPing := context.WithCancel(ctx)
	defer stopPing()
	go a.ping(pctx, cfg, hb, start, &lastRecv, &pongTimedOut)
	defer a.cc.Close()
	for {
		f, err := a.cc.Recv(time.Time{})
		if err != nil {
			if pongTimedOut.Load() {
				return end{kind: endPongTimeout, err: err}
			}
			return end{kind: endLost, err: err}
		}
		lastRecv.Store(int64(time.Since(start)))
		switch f.Cmd {
		case clientpb.Cmd_CMD_KICK:
			var m clientpb.Kick
			err := f.Decode(&m)
			return end{kind: endKicked, kick: m.GetReason(), err: err}
		case clientpb.Cmd_CMD_RECONNECT:
			var m clientpb.Reconnect
			err := f.Decode(&m)
			return end{kind: endReconnect, jitter: time.Duration(m.GetJitterMs()) * time.Millisecond, err: err}
		case clientpb.Cmd_CMD_LOGOUT_RESP:
			if a.loggingOut.Load() {
				return end{kind: endLogoutResp}
			}
			report(attemptMsg{a: a, note: errors.New("sdk: LogoutResp without a Logout")})
		}
	}
}

// ping sends a Ping every hb and closes the connection when no frame has
// arrived within cfg.PongTimeout of the oldest Ping sent since the last
// frame.
func (a *attempt) ping(ctx context.Context, cfg *Config, hb time.Duration, start time.Time, lastRecv *atomic.Int64, timedOut *atomic.Bool) {
	var reqID uint32 = 1
	next := hb                      // next Ping, since start
	unanswered := time.Duration(-1) // send time of the oldest Ping no frame has followed; -1: none
	for {
		at := next
		if unanswered >= 0 {
			at = min(at, unanswered+cfg.PongTimeout)
		}
		t := time.NewTimer(at - time.Since(start))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		now := time.Since(start)
		if unanswered >= 0 && lastRecv.Load() >= int64(unanswered) {
			unanswered = -1
		}
		if unanswered >= 0 && now >= unanswered+cfg.PongTimeout {
			timedOut.Store(true)
			a.cc.Close()
			return
		}
		if now >= next {
			reqID++
			if a.cc.Send(clientpb.Cmd_CMD_PING, reqID, nil) != nil {
				return // the reader sees the broken connection
			}
			if unanswered < 0 {
				unanswered = now
			}
			next = now + hb
		}
	}
}

// tcpDial is the default Config.Dial: a new TCP socket per attempt, so each
// attempt gets a new source port (R2-O2).
func tcpDial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}
```

- [ ] **Step 4: Write the client**

`internal/sdk/client.go`:

```go
package sdk

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sync/atomic"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// Config configures a Client.
type Config struct {
	// Addrs are the gateway addresses; each attempt picks one at random. In
	// production it is the one NLB address; tests list several gateways.
	Addrs     []string
	KeyID     uint16 // key_id of the pinned server key
	ServerKey []byte // the pinned Noise static public key
	AppID     string
	Platform  clientpb.Platform
	// Token returns the access token. It is called before every attempt and
	// must honour ctx (TokenTimeout). refresh is true while the last Login
	// rejection was TOKEN_INVALID: return a new token, not a cached one.
	Token func(ctx context.Context, refresh bool) (string, error)
	// Store holds the persisted State. A Store that is a Locker is locked
	// by New, so a second Client on it fails with ErrStoreInUse.
	Store Store
	// OnEvent receives every event in order, on the client's goroutine. It
	// must not block or call the Client.
	OnEvent func(Event)

	Windows        Windows        // built-in defaults (DefaultWindows)
	Backoff        Backoff        // (DefaultBackoff)
	Heartbeat      HeartbeatRange // accepted LoginResp heartbeat (DefaultHeartbeatRange)
	TokenTimeout   time.Duration  // Token call (10s)
	PongTimeout    time.Duration  // no frame within this of a Ping: dead server (10s)
	ConnectTimeout time.Duration  // dial + handshake (10s)
	LoginTimeout   time.Duration  // LoginReq → reply (10s)
	LogoutTimeout  time.Duration  // Logout → LogoutResp (2s)

	Dial func(ctx context.Context, addr string) (net.Conn, error) // nil: TCP
	Rand *rand.Rand                                               // nil: randomly seeded
}

// DefaultConfig returns the timing defaults; the caller sets the rest. New
// gives any zero timing field its default.
func DefaultConfig() Config {
	return Config{Windows: DefaultWindows(), Backoff: DefaultBackoff(), Heartbeat: DefaultHeartbeatRange(),
		TokenTimeout: 10 * time.Second, PongTimeout: 10 * time.Second, ConnectTimeout: 10 * time.Second,
		LoginTimeout: 10 * time.Second, LogoutTimeout: 2 * time.Second}
}

// EventKind says what happened.
type EventKind int

const (
	EventConnecting    EventKind = iota + 1 // an attempt starts (Attempt)
	EventOnline                             // logged in (Resp)
	EventWaiting                            // the next attempt waits (Wait; Code or Kick: why)
	EventIdle                               // no attempt until Connect, or Foreground after REPLACED (Kick)
	EventLoginRequired                      // REVOKED: drop the token, show the login screen
	EventLoggedOut                          // Logout or SwitchAccount finished
	EventAnomaly                            // worth logging (Err): REPLACED, BAD_REQUEST, an unknown code, a capped server value, a store or Save failure
)

var eventNames = map[EventKind]string{EventConnecting: "connecting", EventOnline: "online", EventWaiting: "waiting",
	EventIdle: "idle", EventLoginRequired: "login-required", EventLoggedOut: "logged-out", EventAnomaly: "anomaly"}

func (k EventKind) String() string { return eventNames[k] }

// Attempt describes one connection attempt.
type Attempt struct {
	Addr      string
	Reason    clientpb.ReconnectReason
	ConnSeq   uint64
	OfflineMs int64
}

// Event is one step of the client's life.
type Event struct {
	Kind    EventKind
	Attempt Attempt             // EventConnecting
	Resp    *clientpb.LoginResp // EventOnline
	Wait    Wait                // EventWaiting
	Kick    clientpb.KickReason // the Kick that caused it, if any
	Code    clientpb.ErrorCode  // the Login error that caused it, if any
	Err     error               // the failure that caused it, if any
}

// Client holds at most one connection for one device_id (spec: device_id)
// and keeps it logged in by the reconnect rules. Its methods may be called
// from any goroutine.
type Client struct {
	cfg      Config
	deviceID atomic.Value // string; "" until the store is read
	unlock   func()       // releases the store's lock, or nil
	cmds     chan func()
	msgs     chan attemptMsg
	quit     chan struct{}
	done     chan struct{}

	// Owned by the run loop.
	pending  []Event // New's events, emitted first by the run loop
	pol      *policy
	st       State
	loaded   bool                // st was read from the store, or started fresh
	refresh  bool                // the last Login rejection was TOKEN_INVALID
	cur      *attempt            // in flight or online
	online   bool                // cur logged in
	timer    *time.Timer         // the pending wait, or nil
	wait     Wait                // what timer is for
	idleKick clientpb.KickReason // REPLACED: Foreground reconnects
	lostAt   time.Time           // the last established connection's loss; zero: none since login
}

// New loads the persisted state, creating device_id on a fresh install or
// when the record is corrupt, and starts the client idle: call Connect. A
// store that can't be read yet is kept: the client reads it again before
// each attempt. Store problems are reported as the first events.
func New(cfg Config) (*Client, error) {
	if len(cfg.Addrs) == 0 || cfg.ServerKey == nil || cfg.Token == nil || cfg.Store == nil {
		return nil, errors.New("sdk: Addrs, ServerKey, Token and Store are required")
	}
	def := DefaultConfig()
	for _, f := range []struct{ v, d *time.Duration }{{&cfg.PongTimeout, &def.PongTimeout},
		{&cfg.ConnectTimeout, &def.ConnectTimeout}, {&cfg.LoginTimeout, &def.LoginTimeout},
		{&cfg.LogoutTimeout, &def.LogoutTimeout}, {&cfg.TokenTimeout, &def.TokenTimeout},
		{&cfg.Backoff.Base, &def.Backoff.Base}, {&cfg.Backoff.Max, &def.Backoff.Max}} {
		if *f.v <= 0 {
			*f.v = *f.d
		}
	}
	if cfg.Windows == (Windows{}) {
		cfg.Windows = def.Windows
	}
	if cfg.Heartbeat == (HeartbeatRange{}) {
		cfg.Heartbeat = def.Heartbeat
	}
	if cfg.Heartbeat.Min <= 0 || cfg.Heartbeat.Min > cfg.Heartbeat.Max {
		return nil, errors.New("sdk: Heartbeat needs 0 < Min ≤ Max")
	}
	if cfg.Dial == nil {
		cfg.Dial = tcpDial
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	if cfg.OnEvent == nil {
		cfg.OnEvent = func(Event) {}
	}
	c := &Client{cfg: cfg, cmds: make(chan func()), msgs: make(chan attemptMsg),
		quit: make(chan struct{}), done: make(chan struct{}),
		pol: newPolicy(cfg.Windows, cfg.Backoff, cfg.Rand)}
	c.deviceID.Store("")
	if l, ok := cfg.Store.(Locker); ok {
		unlock, err := l.Lock()
		if err != nil {
			return nil, err
		}
		c.unlock = unlock
	}
	if err := c.load(); err != nil {
		c.pending = append(c.pending, Event{Kind: EventAnomaly, Err: err})
	}
	go c.run()
	return c, nil
}

// load reads the store. A corrupt record starts a fresh install: a new
// device_id is safe (the server sees another device on the slot), and
// conn_seq restarts with it; the corruption is queued as an anomaly. Any
// other error leaves the client unloaded and the record untouched.
func (c *Client) load() error {
	st, err := c.cfg.Store.Load()
	switch {
	case errors.Is(err, ErrCorrupt):
		c.pending = append(c.pending, Event{Kind: EventAnomaly, Err: err})
		st = State{}
	case err != nil:
		return err
	}
	st.Windows = st.Windows.Clamp()
	if st.DeviceID == "" {
		st.DeviceID = NewDeviceID()
		if err := c.cfg.Store.Save(st); err != nil {
			return err // never stored, so never used
		}
	}
	c.st, c.loaded = st, true
	c.deviceID.Store(st.DeviceID)
	c.pol.w = st.Windows.Over(c.cfg.Windows)
	return nil
}

// DeviceID is this install's device_id, or "" while the store can't be read.
func (c *Client) DeviceID() string { return c.deviceID.Load().(string) }

// Connect starts an attempt now if the client is idle: the first connect,
// or the user asking to go online after a Kick or a Logout.
func (c *Client) Connect() { c.do(func() { c.connect() }) }

// NetworkChanged reports an OS network change: it cuts the storm wait
// short (spec: Local trigger).
func (c *Client) NetworkChanged() { c.do(func() { c.local(false) }) }

// Foreground reports the app returning to the foreground: like a network
// change, and it also ends the idle state after a REPLACED Kick.
func (c *Client) Foreground() { c.do(func() { c.local(true) }) }

// Logout cancels every pending reconnect and retry, sends Logout if logged
// in and waits up to LogoutTimeout for LogoutResp, then closes. The client
// stays idle until Connect.
func (c *Client) Logout() { c.do(func() { c.logout() }) }

// SwitchAccount logs out, then connects with the new account's tokens.
func (c *Client) SwitchAccount(token func(ctx context.Context, refresh bool) (string, error)) {
	c.do(func() {
		c.logout()
		c.cfg.Token = token
		c.connect()
	})
}

// Close is the app closing: it cancels everything, closes any connection
// without a Logout, and stops the client.
func (c *Client) Close() {
	select {
	case <-c.quit:
	default:
		close(c.quit)
	}
	<-c.done
}

// do runs f on the run loop and waits for it.
func (c *Client) do(f func()) {
	ran := make(chan struct{})
	select {
	case c.cmds <- func() { f(); close(ran) }:
		<-ran
	case <-c.done:
	}
}

func (c *Client) run() {
	defer close(c.done)
	for _, e := range c.pending {
		c.emit(e)
	}
	c.pending = nil
	for {
		var fire <-chan time.Time
		if c.timer != nil {
			fire = c.timer.C
		}
		select {
		case <-c.quit:
			c.stopWait()
			c.drop()
			if c.unlock != nil {
				c.unlock()
			}
			return
		case f := <-c.cmds:
			f()
		case <-fire:
			c.timer = nil
			c.start(c.wait.Reason)
		case m := <-c.msgs:
			if m.a == c.cur {
				c.handle(m)
			}
		}
	}
}

func (c *Client) emit(e Event) { c.cfg.OnEvent(e) }

// connect starts an attempt if nothing is in flight or pending.
func (c *Client) connect() {
	if c.cur == nil && c.timer == nil {
		c.idleKick = 0
		c.start(c.pol.first())
	}
}

// local is a local trigger (spec: Precedence).
func (c *Client) local(foreground bool) {
	switch {
	case c.timer != nil:
		if w, ok := c.pol.local(c.wait); ok {
			c.stopWait()
			c.schedule(w, Event{})
		}
	case c.cur == nil && foreground && c.idleKick == clientpb.KickReason_KICK_REASON_REPLACED:
		c.idleKick = 0
		c.schedule(c.pol.localWait(), Event{})
	}
}

// start persists the next conn_seq, then starts an attempt (R1-O5: persist
// before each connection attempt). A store not read yet is read first.
func (c *Client) start(reason clientpb.ReconnectReason) {
	if !c.loaded {
		if err := c.load(); err != nil {
			c.fail(err)
			return
		}
		for _, e := range c.pending { // a corrupt record found now
			c.emit(e)
		}
		c.pending = nil
	}
	c.st.ConnSeq++
	if err := c.cfg.Store.Save(c.st); err != nil {
		c.st.ConnSeq--
		c.fail(err)
		return
	}
	var offline int64
	if !c.lostAt.IsZero() {
		offline = time.Since(c.lostAt).Milliseconds()
	}
	at := Attempt{Addr: c.cfg.Addrs[c.cfg.Rand.IntN(len(c.cfg.Addrs))], Reason: reason, ConnSeq: c.st.ConnSeq, OfflineMs: offline}
	req := &clientpb.LoginReq{Appid: c.cfg.AppID, Platform: c.cfg.Platform, DeviceId: c.st.DeviceID,
		ConnSeq: at.ConnSeq, ReconnectReason: reason, OfflineMs: offline}
	ctx, cancel := context.WithCancel(context.Background())
	a := &attempt{cancel: cancel, refresh: c.refresh}
	c.cur, c.online = a, false
	c.emit(Event{Kind: EventConnecting, Attempt: at})
	cfg := c.cfg // the attempt reads its own copy: SwitchAccount may change Token
	go a.run(ctx, &cfg, at.Addr, req, c.msgs)
}

// fail is a failure before any attempt: an anomaly, then a failure backoff.
func (c *Client) fail(err error) {
	c.emit(Event{Kind: EventAnomaly, Err: err})
	w, _ := c.pol.after(end{kind: endFailed, err: err})
	c.schedule(w, Event{Err: err})
}

func (c *Client) handle(m attemptMsg) {
	if m.note != nil {
		c.emit(Event{Kind: EventAnomaly, Err: m.note})
		return
	}
	if m.resp != nil {
		c.online = true
		c.lostAt = time.Time{}
		c.refresh = false
		c.pol.loggedIn()
		if hb, clamped := c.cfg.Heartbeat.Of(m.resp); clamped {
			c.emit(Event{Kind: EventAnomaly, Err: fmt.Errorf("sdk: heartbeat %ds clamped to %v", m.resp.GetHeartbeatS(), hb)})
		}
		if w := c.st.Windows.Merge(m.resp); w != c.st.Windows {
			c.st.Windows = w
			if err := c.cfg.Store.Save(c.st); err != nil {
				c.emit(Event{Kind: EventAnomaly, Err: err})
			}
			c.pol.w = w.Over(c.cfg.Windows)
		}
		c.emit(Event{Kind: EventOnline, Resp: m.resp})
		return
	}
	e := m.end
	if c.online {
		c.lostAt = time.Now() // the client detected the loss now (offline_ms)
	}
	c.cur, c.online = nil, false
	ev := Event{Kick: e.kick, Code: e.code, Err: e.err}
	if e.kind == endRejected {
		c.refresh = e.code == clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID
	}
	if e.kind == endRejected && e.code == clientpb.ErrorCode_ERROR_CODE_STALE {
		// Set the counter to max(local, stored); the next attempt adds 1.
		c.st.ConnSeq = max(c.st.ConnSeq, e.storedSeq)
		if err := c.cfg.Store.Save(c.st); err != nil {
			c.emit(Event{Kind: EventAnomaly, Err: err})
		}
	}
	if err := c.pol.anomaly(e); err != nil {
		c.emit(Event{Kind: EventAnomaly, Kick: e.kick, Code: e.code, Err: err})
	}
	revoked := e.kick == clientpb.KickReason_KICK_REASON_REVOKED ||
		(e.kind == endRejected && e.code == clientpb.ErrorCode_ERROR_CODE_REVOKED)
	if w, ok := c.pol.after(e); ok {
		c.schedule(w, ev)
		return
	}
	if revoked {
		ev.Kind = EventLoginRequired
	} else {
		ev.Kind = EventIdle
		c.idleKick = e.kick
	}
	c.emit(ev)
}

// schedule arms the wait; ev carries why.
func (c *Client) schedule(w Wait, ev Event) {
	c.wait, c.timer = w, time.NewTimer(w.D)
	ev.Kind, ev.Wait = EventWaiting, w
	c.emit(ev)
}

func (c *Client) stopWait() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
}

// drop aborts the attempt in flight or closes the connection, with no Logout.
func (c *Client) drop() {
	if c.cur != nil {
		c.cur.cancel()
		c.cur, c.online = nil, false
	}
}

// logout is the stop condition (spec: Stop conditions).
func (c *Client) logout() {
	c.stopWait()
	c.idleKick = 0
	c.lostAt = time.Time{}
	c.refresh = false
	if c.cur != nil && c.online {
		a := c.cur
		a.loggingOut.Store(true)
		if a.cc.Send(clientpb.Cmd_CMD_LOGOUT, 1, nil) == nil {
			t := time.NewTimer(c.cfg.LogoutTimeout)
		wait:
			for {
				select {
				case m := <-c.msgs:
					switch {
					case m.a != a:
					case m.note != nil:
						c.emit(Event{Kind: EventAnomaly, Err: m.note})
					case m.resp == nil:
						break wait // LogoutResp, or the connection ended
					}
				case <-t.C:
					break wait
				}
			}
			t.Stop()
		}
	}
	c.drop()
	c.emit(Event{Kind: EventLoggedOut})
}
```

- [ ] **Step 5: Run the tests**

Run: `go vet ./internal/sdk && go test -race -count=1 ./internal/sdk && go test -count=3 ./internal/sdk`
Expected: `ok` each time (~1.5s under `-race`; every client test runs on fake time).

- [ ] **Step 6: Commit**

```bash
git add internal/sdk
git commit -m "feat(sdk): client run loop, attempts, Ping and the Pong timeout"
```

---

### Task 6: End to end over TCP against real gateways and Logic (races 9 and 10)

**Files:**
- Test: `internal/sdk/e2e_test.go`

**Interfaces:**
- Consumes:
  - `Client`, `Config`, `DefaultConfig`, `Event*`, `MemStore`, `State`, `WaitDrain`, `WaitPreHandshake` (Tasks 2–5); test helpers `within`, `ms`, `reasonDrain` (Tasks 2, 4, 5);
  - `clientconn.Dial` (Task 1);
  - `gateway.DefaultConfig`, `NewEpoch`, `New`, `Deps`, `NewMetrics`, `ServerConfig`, `DrainConfig`, `Start`, `Addrs`, `Drain`, `Stop` (Plan 3);
  - `tcp.DefaultConfig`, `New`, `Start`, `Addr`, `Ready`, `Stop` (Plan 4);
  - `logic.DefaultConfig`, `DefaultServerConfig`, `NewServer`, `Deps`, `NewMetrics`, `Start`, `Addrs`, `Stop` (Plan 2);
  - `store.New`, `session.Decode`, `auth.GenerateKey`, `auth.Sign`, `auth.Verify`, `testredis.New`, `UID`, `Gateway`.
- Produces: test helpers `stack` (`newStack`, `gateway`, `token`, `field`, `client`, `script`), `gw`, `liveRec` (`next`, `none`), `staticKeys`.

This task adds tests only. They pass at once, and a failure here is a bug in Tasks 1–5. They run on real time, over TCP and Noise, against real gnet gateways and Plan 2's Logic on miniredis (the real Redis at Tier 2). Only this test file imports the server.

- [ ] **Step 1: Write the tests**

`internal/sdk/e2e_test.go`:

```go
package sdk

import (
	"context"
	"crypto/ed25519"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/clientconn"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/gateway/tcp"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/Tallinzp/goim/internal/wire"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// The end-to-end tests run the SDK over real TCP and Noise against real
// gateways (gnet) and Plan 2's Logic on miniredis (the real Redis at Tier 2),
// on real time.

// staticKeys is a JWT key set that is always loaded.
type staticKeys struct{ k auth.PrivateKey }

func (s staticKeys) Verify(tok string) (auth.Claims, error) {
	return auth.Verify(tok, auth.KeySet{s.k.Kid: s.k.Key.Public().(ed25519.PublicKey)}, time.Now().UnixMilli(), 30_000)
}
func (staticKeys) Ready() bool         { return true }
func (staticKeys) Run(context.Context) {}

// stack is Logic and its gateways.
type stack struct {
	t        *testing.T
	rdb      *redis.Client
	jwt      auth.PrivateKey
	noise    wire.Key
	grpcAddr string
	lease    string

	mu    sync.Mutex
	kicks map[string]string // gateway id → Kick address
}

func newStack(t *testing.T) *stack {
	t.Helper()
	st := &stack{t: t, rdb: testredis.New(t), kicks: map[string]string{}}
	var err error
	if st.jwt, err = auth.GenerateKey("k1"); err != nil {
		t.Fatal(err)
	}
	if st.noise, err = wire.GenerateKey(); err != nil {
		t.Fatal(err)
	}
	lcfg := logic.DefaultConfig()
	lcfg.SweepInterval = time.Hour // never sweep a shared Tier 2 Redis
	lsc := logic.DefaultServerConfig()
	lsc.GRPCAddr, lsc.LeaseAddr, lsc.AdminAddr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	reg := prometheus.NewRegistry()
	srv, err := logic.NewServer(lcfg, lsc, logic.Deps{Store: store.New(st.rdb, lcfg.Store), Keys: staticKeys{st.jwt},
		Dial: func(gw string) (*grpc.ClientConn, error) {
			st.mu.Lock()
			addr := st.kicks[gw]
			st.mu.Unlock()
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}, Registry: reg, Metrics: logic.NewMetrics(reg)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); srv.Stop() })
	st.grpcAddr, st.lease, _ = srv.Addrs()
	return st
}

// gw is one running gateway.
type gw struct {
	id   string
	g    *gateway.Gateway
	tr   *tcp.Transport
	addr string
}

// gateway starts a gateway; mod changes its configuration.
func (st *stack) gateway(mod func(*gateway.Config)) *gw {
	t := st.t
	t.Helper()
	cfg := gateway.DefaultConfig()
	cfg.GatewayID, cfg.Epoch, cfg.NumLoops = testredis.Gateway(), gateway.NewEpoch(), 2
	cfg.MaxConns, cfg.PendingCap, cfg.LoginQueueCap, cfg.LogoutQueue = 100, 100, 100, 100
	cfg.Limits.Notrack = true
	if mod != nil {
		mod(&cfg)
	}
	tcfg := tcp.DefaultConfig(cfg.NumLoops)
	tcfg.Addr = "tcp://127.0.0.1:0"
	reg := prometheus.NewRegistry()
	var tr *tcp.Transport
	g, err := gateway.New(cfg, gateway.Deps{Metrics: gateway.NewMetrics(reg), Registry: reg,
		ReadyChecks: []func() error{func() error { return tr.Ready() }}})
	if err != nil {
		t.Fatal(err)
	}
	if tr, err = tcp.New(g, wire.KeySet{1: st.noise}, tcfg, reg); err != nil {
		t.Fatal(err)
	}
	if err := g.Start(context.Background(), gateway.ServerConfig{LogicTarget: st.grpcAddr,
		LeaseTarget: st.lease, KickAddr: "127.0.0.1:0", AdminAddr: "127.0.0.1:0"}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Start(); err != nil {
		g.Stop()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tr.Stop(ctx)
		g.Stop()
	})
	kick, _ := g.Addrs()
	st.mu.Lock()
	st.kicks[cfg.GatewayID] = kick
	st.mu.Unlock()
	return &gw{id: cfg.GatewayID, g: g, tr: tr, addr: tr.Addr()}
}

func (st *stack) token(uid string) string {
	st.t.Helper()
	now := time.Now().UnixMilli()
	tok, err := auth.Sign(st.jwt, auth.Claims{UID: uid, AppID: "app", Platform: "mobile", IatMs: now - 1000, ExpMs: now + 3_600_000})
	if err != nil {
		st.t.Fatal(err)
	}
	return tok
}

// field is uid's stored mobile session.
func (st *stack) field(uid string) (session.Entry, bool) {
	st.t.Helper()
	v, err := st.rdb.HGet(context.Background(), "sess:{"+uid+"}", "app:mobile").Result()
	if err == redis.Nil {
		return session.Entry{}, false
	}
	if err != nil {
		st.t.Fatal(err)
	}
	en, err := session.Decode(v)
	if err != nil {
		st.t.Fatal(err)
	}
	return en, true
}

// liveRec records events on real time.
type liveRec struct {
	mu  sync.Mutex
	evs []Event
	got int
}

func (r *liveRec) add(e Event) {
	r.mu.Lock()
	r.evs = append(r.evs, e)
	r.mu.Unlock()
}

// next waits up to 5s for an event of kind and returns it; events before
// it are skipped.
func (r *liveRec) next(t *testing.T, kind EventKind) Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		for r.got < len(r.evs) {
			e := r.evs[r.got]
			r.got++
			if e.Kind == kind {
				r.mu.Unlock()
				return e
			}
		}
		r.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no %v event within 5s", kind)
	return Event{}
}

// none fails t if an event of kind arrives within d.
func (r *liveRec) none(t *testing.T, kind EventKind, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.evs[r.got:] {
		if e.Kind == kind {
			t.Fatalf("unexpected %v event: %+v", kind, e)
		}
	}
	r.got = len(r.evs)
}

// client starts an SDK client for uid.
func (st *stack) client(uid string, store Store, addrs []string, mod func(*Config)) (*Client, *liveRec) {
	st.t.Helper()
	r := &liveRec{}
	cfg := DefaultConfig()
	cfg.Addrs, cfg.KeyID, cfg.ServerKey = addrs, 1, st.noise.Public
	cfg.AppID, cfg.Platform = "app", clientpb.Platform_PLATFORM_MOBILE
	cfg.Token = func(context.Context, bool) (string, error) { return st.token(uid), nil }
	cfg.Store, cfg.OnEvent = store, r.add
	if mod != nil {
		mod(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		st.t.Fatal(err)
	}
	st.t.Cleanup(c.Close)
	return c, r
}

// script logs in a scripted client on device as uid through addr.
func (st *stack) script(addr, uid, device string, connSeq uint64) *clientconn.Conn {
	t := st.t
	t.Helper()
	c, err := clientconn.Dial(addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.Handshake(1, st.noise.Public, time.Now().Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := c.Send(clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: st.token(uid), Appid: "app",
		Platform: clientpb.Platform_PLATFORM_MOBILE, DeviceId: device, ConnSeq: connSeq}); err != nil {
		t.Fatal(err)
	}
	if f, err := c.Recv(time.Now().Add(5 * time.Second)); err != nil || f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("scripted login: %+v, %v", f, err)
	}
	return c
}

// Race 9: another device's login kicks the SDK client, which must not
// reconnect on its own.
func TestE2EKickedByOtherDeviceDoesNotReconnect(t *testing.T) {
	st := newStack(t)
	g := st.gateway(nil)
	uid := testredis.UID()
	c, r := st.client(uid, &MemStore{}, []string{g.addr}, nil)
	c.Connect()
	r.next(t, EventOnline)
	st.script(g.addr, uid, "other-device", 1)
	if e := r.next(t, EventIdle); e.Kick != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("idle with %v, want KICKED_BY_OTHER_DEVICE", e.Kick)
	}
	c.NetworkChanged()
	c.Foreground()
	r.none(t, EventConnecting, 2*time.Second)
	if en, ok := st.field(uid); !ok || en.DeviceID != "other-device" {
		t.Fatalf("stored %+v, %v; want the other device's session", en, ok)
	}
}

// Race 10: two instances sharing a device_id replace each other once; the
// replaced one stays idle, so the loop stops.
func TestE2ESharedDeviceIDReplacesOnce(t *testing.T) {
	st := newStack(t)
	g := st.gateway(nil)
	uid := testredis.UID()
	s1 := &MemStore{}
	c1, r1 := st.client(uid, s1, []string{g.addr}, nil)
	c1.Connect()
	r1.next(t, EventOnline)
	shared, _ := s1.Load()
	s2 := &MemStore{}
	s2.Save(State{DeviceID: shared.DeviceID, ConnSeq: 100}) // a copy restored elsewhere, further ahead
	c2, r2 := st.client(uid, s2, []string{g.addr}, nil)
	c2.Connect()
	r2.next(t, EventOnline)
	r1.next(t, EventAnomaly)
	if e := r1.next(t, EventIdle); e.Kick != clientpb.KickReason_KICK_REASON_REPLACED {
		t.Fatalf("first instance idle with %v, want REPLACED", e.Kick)
	}
	r1.none(t, EventConnecting, 2*time.Second)
	r2.none(t, EventWaiting, 0)
	if en, ok := st.field(uid); !ok || en.ConnSeq != 101 {
		t.Fatalf("stored %+v, %v; want the second instance (conn_seq 101)", en, ok)
	}
}

// A draining gateway sends Reconnect: the client closes, waits within
// [0, jitter_ms] and logs in elsewhere with DRAIN (R4H-O3).
func TestE2EDrainMovesTheClientWithTheDrainReason(t *testing.T) {
	st := newStack(t)
	a := st.gateway(func(c *gateway.Config) {
		c.Drain = gateway.DrainConfig{Window: time.Second, Jitter: 300 * time.Millisecond, Flush: time.Second}
	})
	b := st.gateway(nil)
	var mu sync.Mutex
	target := a.addr // the NLB: A until A is deregistered
	uid := testredis.UID()
	c, r := st.client(uid, &MemStore{}, []string{"nlb"}, func(cfg *Config) {
		cfg.Dial = func(ctx context.Context, _ string) (net.Conn, error) {
			mu.Lock()
			addr := target
			mu.Unlock()
			var d net.Dialer
			return d.DialContext(ctx, "tcp", addr)
		}
	})
	c.Connect()
	r.next(t, EventOnline)
	if en, _ := st.field(uid); en.Gateway != a.id {
		t.Fatalf("logged in on %s, want A %s", en.Gateway, a.id)
	}
	mu.Lock()
	target = b.addr
	mu.Unlock()
	drained := make(chan struct{})
	go func() { a.g.Drain(context.Background()); close(drained) }()
	t.Cleanup(func() { <-drained })
	w := r.next(t, EventWaiting).Wait
	within(t, "after Reconnect", w, WaitDrain, 0, 300*time.Millisecond, reasonDrain)
	if e := r.next(t, EventConnecting); e.Attempt.Reason != reasonDrain {
		t.Fatalf("attempt %+v, want DRAIN", e.Attempt)
	}
	r.next(t, EventOnline)
	if en, _ := st.field(uid); en.Gateway != b.id {
		t.Fatalf("after the drain the session is on %s, want B %s", en.Gateway, b.id)
	}
}

// A gateway at its cap closes at accept: the SDK sees a pre-handshake close,
// retries after 0.5–1.5s, and logs in once a slot frees.
func TestE2EAtCapacityIsAPreHandshakeClose(t *testing.T) {
	st := newStack(t)
	g := st.gateway(func(c *gateway.Config) { c.MaxConns = 1 })
	holder := st.script(g.addr, testredis.UID(), "holder", 1)
	c, r := st.client(testredis.UID(), &MemStore{}, []string{g.addr}, nil)
	c.Connect()
	within(t, "at capacity", r.next(t, EventWaiting).Wait, WaitPreHandshake, ms(500), ms(1500), clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY)
	holder.Close()
	r.next(t, EventOnline)
}

// Logout reaches Logic through the gateway's fenced Logout: the field goes.
func TestE2ELogoutDeletesTheSession(t *testing.T) {
	st := newStack(t)
	g := st.gateway(nil)
	uid := testredis.UID()
	c, r := st.client(uid, &MemStore{}, []string{g.addr}, nil)
	c.Connect()
	r.next(t, EventOnline)
	if _, ok := st.field(uid); !ok {
		t.Fatal("no session after login")
	}
	start := time.Now()
	c.Logout()
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Logout took %v; the gateway answers LogoutResp at once", d)
	}
	r.next(t, EventLoggedOut)
	deadline := time.Now().Add(5 * time.Second)
	for _, ok := st.field(uid); ok; _, ok = st.field(uid) {
		if time.Now().After(deadline) {
			t.Fatal("session still stored 5s after Logout")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
```

- [ ] **Step 2: Run them, then the whole suite**

Run: `go test -race -count=1 -run E2E -v ./internal/sdk && go vet ./... && go test -race -count=1 ./...`
Expected: the five `TestE2E…` tests PASS (~10s), then `ok` for every package.

- [ ] **Step 3: Commit**

```bash
git add internal/sdk/e2e_test.go
git commit -m "test(sdk): end to end against real gateways and Logic (races 9, 10)"
```

---

### Task 7: The heartbeat range on the gateway (review item 6)

**Files:**
- Modify: `internal/gateway/config.go`, `cmd/gateway/main.go`
- Test: `internal/gateway/config_test.go`

**Interfaces:**
- Consumes: `gateway.Config.Validate` (Plan 3), the `cmd/gateway` flag set (Plan 4).
- Produces: `gateway.Config.HeartbeatMin`, `HeartbeatMax` (defaults 30s, 300s); flags `-heartbeat-min`, `-heartbeat-max`.

The SDK clamps a `LoginResp` heartbeat to `sdk.Config.Heartbeat` (Task 5). The gateway counts a connection dead after 2 × its own heartbeat + 10s, so if the SDK clamped a value the gateway really used, the two ends would disagree on when a connection is dead. The gateway therefore refuses to start with a heartbeat outside the same range. In production both ends use 30s–300s; the test timing profile (Plan 5b) lowers both together.

- [ ] **Step 1: Write the failing test**

In `internal/gateway/config_test.go`, add three rows to `TestValidateRefuses`, after `"fractional heartbeat"`:

```go
		{"heartbeat below its range", func(c *Config) { c.Heartbeat = 10 * time.Second }, "outside [30s, 5m0s]"},
		{"heartbeat above its range", func(c *Config) { c.Heartbeat = 600 * time.Second }, "outside [30s, 5m0s]"},
		{"inverted heartbeat range", func(c *Config) { c.HeartbeatMin = 10 * time.Minute }, "HeartbeatMin"},
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/gateway -run TestValidateRefuses`
Expected: FAIL to build, `c.HeartbeatMin undefined`.

- [ ] **Step 3: Write the implementation**

In `internal/gateway/config.go`, add the fields after `Heartbeat`:

```go
	Heartbeat        time.Duration // sent in LoginResp; dead after 2× + 10s (150s)
	HeartbeatMin     time.Duration // Heartbeat range, as the SDK's (30s); the test profile lowers it
	HeartbeatMax     time.Duration // (300s, below a 350s LB idle timeout)
```

their defaults in `DefaultConfig`, after `Heartbeat: 150 * time.Second,`:

```go
		HeartbeatMin:     30 * time.Second,
		HeartbeatMax:     300 * time.Second,
```

and the check in `Validate`, after the whole-seconds check of `Heartbeat`:

```go
	if c.HeartbeatMin < time.Second || c.HeartbeatMin > c.HeartbeatMax {
		bad("HeartbeatMin %v and HeartbeatMax %v need 1s ≤ min ≤ max", c.HeartbeatMin, c.HeartbeatMax)
	} else if c.Heartbeat < c.HeartbeatMin || c.Heartbeat > c.HeartbeatMax {
		bad("Heartbeat %v outside [%v, %v]: the SDK would clamp it and disagree on the dead time", c.Heartbeat, c.HeartbeatMin, c.HeartbeatMax)
	}
```

In `cmd/gateway/main.go`, after the `-heartbeat` flag:

```go
	fs.DurationVar(&cfg.HeartbeatMin, "heartbeat-min", cfg.HeartbeatMin, "lowest allowed -heartbeat; the same as the SDK's range")
	fs.DurationVar(&cfg.HeartbeatMax, "heartbeat-max", cfg.HeartbeatMax, "highest allowed -heartbeat; the same as the SDK's range")
```

- [ ] **Step 4: Run the tests, then the whole suite**

Run: `go vet ./... && go test -race -count=1 ./internal/gateway/ ./cmd/... && go test -race -count=1 ./...`
Expected: `ok` for every package. No existing test sets a heartbeat outside 30s–300s.

- [ ] **Step 5: Commit**

```bash
git add internal/gateway/config.go internal/gateway/config_test.go cmd/gateway/main.go
git commit -m "feat(gateway): heartbeat range shared with the SDK (Plan 5a review item 6)"
```

---

## After this plan

The SDK implements every rule in the spec's *Client SDK rules*, and the scripted client can drive a gateway frame by frame. Plan 5b builds the race suite on both: the fault hooks, the test timing profile and its ratio checks, the process harness of choice #47 on miniredis (Tier 1b) and on the compose Redis (Tier 2, `make test-tier2 SCENARIO=race`), the P4I-5 clock flag, and every race integration test in the spec's list. It is written after this plan's interfaces are reviewed (*Execution Notes → For Plan 5b*).

The roadmap records #47–#57 as decided, with the plan review's amendments, and this plan's test names in the invariant index (races 9 and 10).

The open risks are listed in *Execution Notes*.
