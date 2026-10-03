---
spec: docs/superpowers/specs/2026-10-03-connection-session-core-design.md
objections: docs/superpowers/objections/connection-session-core-design.md
date: 2026-10-03
decision_maker: user
---

# Disposition record — Connection & Session Core

The user decides each objection. This file records the decision and the spec changes it implies. The spec itself has not been edited yet.

## O1 — premise — high — ACCEPTED

**Decision:** Split the single 600k figure into a capacity ceiling and an operating point.

**Rationale:** The 600k figure was carried over from bare metal, where boxes were expensive and few. On Kubernetes pods are cheap and blast radius is the cost that matters. Keeping 600k as a benchmark preserves the efficiency goal (gnet, no goroutine per connection). The SLO is sized to the load the cluster actually runs at.

**Spec changes to apply:**
- Success criteria:
  - *Capacity ceiling:* one gateway holds 600k connections (measure CPU / memory / GC). This is a benchmark, not the operating point.
  - *Storm SLO:* when a gateway at its operating point (200k) dies, 99% of users are logged in again within 30s (~7k logins/s sustained, 20k/s peak headroom).
- Add a short "Density" paragraph: operating point is at most 200k per gateway. 1M users need at least 6 gateways (5 at 200k plus 1 for N−1), about 167k each in steady state.
- Storms section, capacity rule: N−1 gateways must absorb one dead gateway's connections, sized at the operating point.
- Load tests: (a) stays at 600k (ceiling); (b) kills a 200k gateway against the SLO, plus a 600k-kill stress variant that is measured but has no SLO; (c) heartbeat CPU cost measured at both 200k and 600k.

## O2 — risk — critical — ACCEPTED

**Decision:** Use three layers together: lease priority, a mass-stale guard on the sweeper, and gateway resync after a sweep.

**Rationale:** Liveness is inferred from lease renewals that pass through Logic and Redis. A Redis outage longer than 60s, or Logic saturated during a storm, would make the sweeper erase the sessions of healthy gateways for the whole cluster. Prevention (layers 1–2) makes that unlikely. Recovery (layer 3) means that if it happens anyway, the gateway restores its sessions on its own.

**Spec changes to apply:**
1. **Lease priority.** The gateway sends `GatewayLease` on a dedicated gRPC connection (or a dedicated high-priority queue), never batched with or queued behind Login/Logout. Logic serves lease RPCs from a separate worker pool so that Login load can't starve them.
2. **Mass-stale guard (sweeper).** Before sweeping, the sweeper counts the epochs that went stale within the same window. If more than one epoch (configurable, for example >1 or >20% of live epochs) is stale at once, it treats the event as systemic (Redis or Logic trouble, not gateway death). It then skips sweeping, raises an alert and metric, and re-checks on the next cycle. An epoch that sent `EpochEnded` is exempt and is always swept. The sweeper also waits one full lease period after Redis becomes reachable again before it trusts any staleness.
3. **Gateway resync.** The `GatewayLease` reply tells the gateway whether its epoch was found live, or was missing or swept (for example, the lease key is absent and the `gwepochs` member is gone). On "missing", the gateway re-registers its epoch and replays its whole ONLINE set to Logic, rate-limited, through a `Resync` batch RPC. Each entry runs a conditional Lua script that writes the field only if it is empty or its stored `gw|epoch` has no live lease. It never overrides a live newer session. It also re-adds the uid to `gwconns`.
- Failure table, "Redis down" row: rewrite it to say that outages longer than 60s are covered by the mass-stale guard, and that any sessions swept anyway are restored by resync.
- Add a failure row, "Logic saturated": lease RPCs keep their own channel and pool, so renewals continue.
- RPC list: add `Resync` (batch).
- Tests: a race integration test with Redis paused for 90s, after which the sessions must still be present (or restored by resync); a storm test that checks the leases of the other gateways are never missed.
- Note: layer 3 is a gateway-driven reconciliation path; see O3.

## O3 — alternatives — high — ACCEPTED

**Decision:** Make the gateway the source of truth through periodic anti-entropy. O2's `Resync` becomes a `Reconcile` that runs on a timer as well as when the lease reply reports a missing epoch.

**Rationale:** The gateway is the only component that knows which connections exist. One tested reconciliation path covers swept sessions (O2), ghost entries on live gateways (O9), writes lost in a Redis failover, and TTL refresh. It replaces several partial patches.

**Spec changes to apply:**
- **Reconcile RPC (batch, streaming or chunked).** Every ~10 min, with random jitter per gateway, and immediately when the lease reply reports a missing epoch, each gateway sends its ONLINE set, `(uid, appid:platform, conn_id, device_id, conn_seq, login_ms)`, in rate-limited chunks. The snapshot carries a `cutoff_ms` taken when it started.
- **Logic, per entry in the set:** a conditional Lua script writes the field if it is empty or its stored `gw|epoch` has no live lease. It never overrides a live newer session. It refreshes the `sess:{uid}` TTL and makes sure the uid is in `gwconns`.
- **Logic, entries for this epoch that are missing from the set:** after the snapshot, Logic SSCANs `gwconns:gw:{epoch}`. For each field that points at this `gw|epoch` but whose conn_id is not in the snapshot, and whose `login_ms < cutoff_ms`, it compare-and-deletes. The cutoff protects Logins that were in flight while the snapshot ran. It also removes uids that have no remaining field from `gwconns`, which fixes the set that only ever grew.
- **Remove:** the "gateways refresh the TTL for connections older than 15 days through a slow batch RPC" mechanism, since Reconcile refreshes the TTL. The 30d TTL stays as the last resort.
- **Reclassify:** the Logout retry queue becomes the fast path, and dropping a Logout is safe because the next Reconcile repairs it. Lazy repair in sub-project 3 becomes optional, no longer something correctness depends on.
- Update the "Why the dropped concepts are not needed" section and the failure table to match ("Logic down: ... dropped Logouts are repaired by the next Reconcile").
- Cost note: 200k entries every 10 min is about 330 Lua calls/s per gateway, about 2k/s cluster-wide at 6 gateways.
- Tests: a ghost entry (Login written after the gateway closed the connection) is removed within one cycle; a missing entry is restored; a newer session on another gateway is never overridden; a Login in flight during a snapshot is not deleted.

## O4 — implementation — high — ACCEPTED

**Clarification:** O4 concerns only two *different physical devices on the same slot* (for example two phones, both `mobile`). Different platforms (mobile, pad, web, pc) are different slots and never conflict. A user can be online on all of them at once.

**Decision:** Order same-slot logins from different devices by gateway receive time, and add an end-to-end deadline on logins.

**Rationale:** `conn_seq` orders logins only within a single device. Between devices, arrival order at Redis decided the winner, so a delayed login could kick a newer device with `KICKED_BY_OTHER_DEVICE` (which does not reconnect) and leave the slot empty. That breaks success criterion 3.

**Spec changes to apply:**
- The gateway stamps `recv_ms` (wall clock, NTP-synced) when it receives the Login frame and passes it in the Login RPC. The stored `login_ms` becomes this `recv_ms`, not the time Logic processes the login.
- New login-table row: *different device_id, new recv_ms < stored login_ms* → reject as `SUPERSEDED`: no write, no kick. The gateway closes the connection with a retryable error. A retry is a new, later login and wins normally.
- Existing row becomes: *different device_id, new recv_ms ≥ stored login_ms* → overwrite and kick the old one with `KICKED_BY_OTHER_DEVICE`.
- **Deadline:** the Lua script reads Redis `TIME`. If `now − recv_ms > login_deadline` (default 5s), it rejects as `EXPIRED`: no write, and the client retries. This bounds how late a login can arrive on any path, and matches the gateway rule that drops queued Logins past their deadline.
- Note the assumption: clock skew between gateways (NTP, a few ms) is far below the time a person takes to switch devices.
- Metrics: add `superseded` and `expired` to the login results.
- Races section: add a race, "Delayed login from another device on the same slot", resolved by recv_ms ordering. Add it to the race integration tests.
- Lua tests: one table row each for SUPERSEDED and EXPIRED.

## O5 — risk — high — ACCEPTED

**Decision:** Limit `STALE` to the deadline window, so it can't lock a device out permanently, and let the client resync its counter.

**Rationale:** A genuinely stale same-device login is an older attempt still in flight. Because of the O4 deadline, it must be received within `login_deadline` of the newer login. A login with a lower `conn_seq` that arrives later than that must come from a counter that went backwards. It should be accepted, not trigger a lockout until the TTL expires.

**Spec changes to apply:**
- Login table, same-device rows:
  - *same device_id, new conn_seq > stored* → overwrite; kick the old connection with `REPLACED` (unchanged).
  - *same device_id, new conn_seq ≤ stored, recv_ms ≤ stored login_ms + login_deadline* → reject as `STALE`; close; no write.
  - *same device_id, new conn_seq ≤ stored, recv_ms > stored login_ms + login_deadline* → treat as a counter regression: overwrite, kick the old connection with `REPLACED`, and increment the `conn_seq_regressed` metric.
- The `STALE` error carries the stored `conn_seq`. The client SDK sets its counter to `max(local, stored) + 1`, persists it, and retries after backoff.
- Client SDK rules (new): persist `conn_seq` **before** each connection attempt, and store it in the same place as device_id with the same lifetime (for example both in the iOS Keychain), so that clearing one clears the other.
- Lua tests: the regression row (low conn_seq outside the window is accepted) and the stale row (low conn_seq inside the window is rejected).
- Metrics: add `conn_seq_regressed`.

## O6 — specification quality — high — ACCEPTED

**Decision:** After `REPLACED` the client does not reconnect automatically. A device_id identifies one connected client instance. On web, one leader tab holds a single connection per browser.

**Rationale:** In normal operation a client only gets `REPLACED` on a connection it has already abandoned. If a *live* client receives it, a second instance shares its device_id, and reconnecting automatically would start a replace loop. Multiple tabs are the normal case on web, so its semantics have to be fixed now.

**Spec changes to apply:**
- Client protocol, kick reasons:
  - `REPLACED`: same device, newer connection; not shown to the user. **The client must not reconnect automatically.** It goes idle and reconnects only on a user action or when the app returns to the foreground. The SDK logs it as an anomaly.
  - `KICKED_BY_OTHER_DEVICE`: unchanged (shown to the user; no automatic reconnect).
- Define device_id: it identifies one client instance that holds a connection. The SDK must make sure at most one connection per device_id is in use at a time. Mobile and desktop: one per install, plus a process-level guard such as a single connection service on Android.
- Web transport (designed now, built later): one connection per browser profile. The tabs elect a leader through `SharedWorker`, falling back to Web Locks plus `BroadcastChannel`. The leader holds the wss connection and relays messages to the other tabs, and when the leader tab closes, another tab takes over. device_id and conn_seq are stored in localStorage.
- Races section, new race: "Two client instances share a device_id": they replace each other once, and because `REPLACED` doesn't reconnect, the loop stops. Add it to the race integration tests.

## O7 — specification quality — high — ACCEPTED

**Decision:** Define the SLO per failure class with an explicit clock, shrink the reconnect jitter to 0–20s, measure with loadgen in tests and with client-reported `offline_ms` in production, and cross-check on the server from lease data.

**Rationale:** As written, the SLO had no start point, ignored silent gateway death, and its 0–30s jitter left no room for a single retry. The server can't observe when a client detects a loss, so the clock has to be measured by the client (loadgen in tests, `offline_ms` in production). "Detection is bounded by the heartbeat" is a design guarantee, not something the server measures.

**Spec changes to apply:**
- Success criteria, storm SLO (sized at the 200k operating point from O1):

  | Failure class | How clients detect it | SLO |
  |---|---|---|
  | Process crash / pod kill (node alive) | Kernel RST/FIN, passed through by the L4 NLB | 99% logged in again within 30s **of gateway death** |
  | Node loss / silent network partition | Client heartbeat (Pong timeout) | 99% logged in again within 30s **of the client detecting the loss**. Detection is bounded by design, at most one heartbeat interval plus the Pong timeout. |
- Client SDK:
  - First reconnect after an unexpected close: wait a random **0–20s**, which leaves 10s for the handshake, login and one Busy retry. Then exponential backoff (cap 5m).
  - Dead-server detection: if no frame arrives within **10s** of a Ping, close and reconnect.
- `LoginReq` gains two fields: `reconnect_reason` (enum: first connect, RST/FIN, Pong timeout, Busy retry, Reconnect from drain, ...) and `offline_ms` (ms since the client detected the loss, on the client's monotonic clock; a duration, so the clocks of the phone and the server don't need to agree).
- Measurement:
  - **Tests:** `cmd/loadgen` records detection time and login-success time per client and computes the SLO distribution directly.
  - **Production:** Logic exports a histogram of `offline_ms` + (login OK − `recv_ms`), labelled by `reconnect_reason`.
  - **Server cross-check:** when a relogin finds the old entry pointing at a dead epoch, record `login_ms − that epoch's last lease renewal` (approximate time from gateway death to relogin, ±10s, needs no data from the client).
- Gateway: during admission overload, `Busy{retry_after}` uses a short value (1–3s, with jitter) so that rejected clients still fit within the SLO window.
- Arithmetic check to record: 200k users over 20s is about 10k logins/s, within the 20k/s headroom.
- Load test (b), two variants: (b1) SIGKILL of the gateway process; (b2) iptables DROP on the gateway to simulate silent node loss. Each is measured against its own clock. The 600k stress variant from O1 is measured too, but has no SLO.
- Failure table, "Gateway crash" row: split it into crash and node-loss rows that match the table above.
- Observability: add the relogin-latency histogram and the dead-epoch cross-check metric.

## O8 — risk — high — ACCEPTED

**Decision:** Adopt all four items. Sub-project 1 implements items 1–3 (Kick hardening, kick-pending in AUTH_PENDING, reconcile backstop). Item 4, the ownership rule, goes into the spec only as an **interface constraint** for sub-project 2. Sub-project 1 does not implement the send-path enforcement.

**Rationale:** Users read `KICKED_BY_OTHER_DEVICE` as "the other session has ended". A best-effort Kick left at least four paths where the displaced connection stays authenticated for weeks. Hardening closes the common paths, Reconcile limits any miss to one cycle, and the ownership rule makes sure sub-project 2 doesn't inherit an unstated answer.

**Spec changes to apply (implemented in sub-project 1):**
1. **Kick hardening.**
   - Logic always sends the Kick and no longer skips it when the old gateway's lease looks expired. Remove the "Logic checks the lease of the old gateway before sending a Kick" note. An epoch mismatch on the gateway still makes a stray Kick harmless.
   - Bounded retry: up to 3 attempts over about 30s on transport errors, then give up and increment a metric.
   - The headless service for gateways sets `publishNotReadyAddresses: true`, so pod DNS keeps resolving while a gateway drains.
   - Failure table, "Kick can't reach the gateway" row: change "ignored (best effort)" to "retried up to 3 times; after that the Reconcile backstop closes the connection within one cycle".
2. **Kick while AUTH_PENDING.** If a Kick finds the connection in `AUTH_PENDING`, the gateway sets a kick-pending mark with the reason. When the Login reply arrives, it sends the Kick frame and closes the connection instead of moving to `ONLINE`. Change "Kick acts only on connections in the `ONLINE` state" to match. Kicks for connections in `HANDSHAKE` or `CLOSED`, or with an unknown conn_id, are still ignored.
3. **Reconcile backstop (extends O3).** For each entry in a Reconcile snapshot whose slot now holds a *different* session with a live lease, Logic returns it in a `not_owner` list together with the kick reason: `KICKED_BY_OTHER_DEVICE` for a different device_id, `REPLACED` for the same device. The gateway closes those connections with that reason. A missed Kick is therefore capped at one Reconcile cycle (~10 min).
- Tests: a Kick is dropped (the gateway can't be reached) and the old connection is closed by the next Reconcile; a Kick that arrives during AUTH_PENDING closes the connection when the Login reply comes back; a draining gateway still receives Kicks.

**Spec changes to apply (interface constraint only, not implemented in sub-project 1):**
4. **Ownership rule.** Add it to the spec as a constraint on later sub-projects: *a connection may act as the user (send messages, perform user operations) only while its `gw|epoch|conn_id` owns its slot in `sess:{uid}`.* Sub-project 2 must enforce this on the send path (a cached ownership check is acceptable). Sub-project 1 only documents it and provides the data it relies on (the session entries and Kick/Reconcile delivery).

## O9 — implementation — medium — ACCEPTED

**Decision:** Add a Logout fence invariant on the gateway, built on the O4 Lua deadline. Dropped Logouts are repaired by Reconcile (O3).

**Rationale:** The ghost is one connection's Login racing its own Logout. `conn_seq` fixes it only if the same device later reconnects. A device that never comes back (app killed, phone off, no network) would leave the ghost indefinitely. The O4 deadline already bounds when a Login can write (`Redis TIME − recv_ms ≤ login_deadline`). Making the gateway wait out that bound before sending Logout means the Logout always lands after any possible write.

**Spec changes to apply:**
- **Logout fence (gateway invariant):** the gateway sends Logout for a connection only after
  - (a) it has a **definitive Login reply**, success or rejection, at which point the Lua write has already happened or never will; or
  - (b) the time `recv_ms + login_deadline + 1s` has passed (the 1s absorbs clock skew between gateway and Redis). This applies to **any** Login without a definitive reply: timeout, RPC error, a gRPC connection reset by `MaxConnectionAge`, or a Logic crash.

  A connection that closes before either condition holds keeps a pending Logout that fires when one becomes true.
- Gateway Login RPC timeout: at least `login_deadline + 1s`, measured from `recv_ms`.
- Rewrite race 4 ("Connection closes while its Login is in flight") and race 5 ("Login RPC times out but Redis was written") in terms of the fence. Drop the "safe because compare-and-delete is idempotent" argument; the fence is what makes it safe.
- Race 6 and the "Logic down" failure row: the stale entry is repaired by the next Reconcile (O3), not by "the sweeper". The sweeper only handles dead epochs.
- Note in the races section: when the same device reconnects, `conn_seq` also replaces any ghost. Reconcile covers devices that never return.
- Tests: race 5 in both orders (Logout before Login and Login before Logout, with Logic delays injected): no ghost after the fence. A Login with no definitive reply caused by a Logic crash: the Logout waits for the fence. A dropped Logout: the ghost is gone after one Reconcile cycle.

## O10 — scope — medium — ACCEPTED

**Decision:** The JWT authenticates only the moment of login. Revocation is enforced at login (inside the login Lua), by a `Revoke` RPC that kicks live connections, and by a Reconcile backstop. Signing keys are distributed as a JWKS keyed by `kid`.

**Rationale:** Connections live for weeks, so security comes from revocation, not from token expiry. Keeping the revocation marker in the user's slot lets the login Lua script check it with no extra round trip. Storing `iat` in the session lets Reconcile catch any connection that a revocation Kick missed, the same backstop as O8. The account service stays out of scope, but how the session layer responds to account events is in scope.

**Spec changes to apply:**
1. **Expiry:** the JWT is a short-lived access token (for example 1h), checked only at Login. Its expiry does not affect a connection that is already authenticated. State this explicitly in the Auth decision.
2. **Revocation:**
   - New key `rev:{uid}` (STRING, hash-tagged `{uid}` like `sess:{uid}`): `revoked_before_ms`, with TTL = max token lifetime + margin. After that, any token old enough to be revoked has expired anyway.
   - Login Lua: read `rev:{uid}` first. If the token's `iat < revoked_before_ms`, reject as `REVOKED`: no write.
   - The session field gains `tok_iat`: `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`.
   - New RPC `Logic.Revoke(uid, scope = all | slot, reason)`, called by the account service on a ban, a password change or "log out all devices". Logic sets `rev:{uid}` (for scope=all), deletes the affected fields, and kicks each connection with the new kick reason `REVOKED`, using the hardened Kick from O8 (retries, no lease check).
   - Client protocol: new kick reason `REVOKED`. The client drops its token, shows the login screen, and does not reconnect automatically.
   - Reconcile backstop: for each snapshot entry whose `tok_iat < revoked_before_ms`, Logic returns `not_owner` with reason `REVOKED`, and the gateway closes the connection. A missed revocation Kick is therefore capped at one Reconcile cycle. The gateway snapshot carries `tok_iat`.
3. **Keys:** Logic loads a JWKS (Ed25519 keys keyed by `kid`) from the account service at startup and refreshes it every 5 min. During a rotation every published key is accepted. If a refresh fails, Logic keeps the last good set and raises an alert. A JWT with an unknown `kid` triggers at most one rate-limited refresh before it is rejected. `cmd/devtoken` publishes a test JWKS.
- RPC list: add `Revoke`.
- Metrics: add `revoked` to the login results, plus a revocation-kick count.
- Tests: Lua (a token with iat before the marker is rejected); integration (Revoke closes connections on two slots, and a Revoke whose Kick is dropped is caught by the next Reconcile); key rotation (both kids are accepted during the overlap).

## O11 — risk — medium — PARTIALLY ACCEPTED

**Decision:** For sub-project 1, which is Android/iOS over TCP only, accept the timeouts, the Login-queue cap, the memory-only pending cap, and a per-gateway global Noise handshake limit (concurrency plus token bucket, never keyed by IP). Reject per-IP limiting and proxy protocol for now.

**Rationale:**
- Mobile IPs are mostly shared through carrier-grade NAT, so per-IP limiting harms legitimate users, especially during a reconnect storm. Any rate limiting should key on **device_id or user_id**, not IP.
- Without per-IP limits, sub-project 1 doesn't need the real client IP. Proxy protocol is deferred to the web (wss) phase.

**Rejected / deferred:**
- Proxy protocol v2 for the real client IP: **deferred** to the web wss phase.
- Per-IP failure bucket and per-IP flood cap: **rejected**. Remove the existing "a token bucket for new connections per IP" from the spec.
- Direction for later: abuse or rate limiting, if added, keys on device_id or user_id. That is possible only once the LoginReq has been decrypted, so after the handshake. Not designed in sub-project 1.

**Spec changes to apply:**
1. **Timeouts:** `HANDSHAKE` 5s (from accept to the end of the handshake) and `AUTH_PENDING` 5s (from the end of the handshake to receiving LoginReq). On expiry the connection closes silently. Both run on the timing wheel.
2. **Login-queue cap (replaces the cap on HANDSHAKE + AUTH_PENDING):** cap the number of Logins waiting for or inside an RPC to Logic, which reflects Logic backpressure. When the cap is reached, reply `Busy{retry_after}` (1–3s with jitter, per O7) and close.
3. **Memory-only pending cap:** a large cap on total pending connections (`HANDSHAKE` + `AUTH_PENDING`) remains, only to protect gateway memory. It is not meant to trigger during a normal storm.
4. **Noise handshake limit (per gateway, global; strictly not per IP):** protects gateway CPU from a flood of Noise handshakes, independent of client IP. It has two parts:
   - a **concurrency limit** on Noise handshake computations running at once (for example a semaphore sized to the number of CPU cores dedicated to handshakes);
   - a **token bucket** on handshakes per second for the whole gateway, sized from the measured Noise NK cost (~100µs per handshake) so that handshakes can't take all the CPU away from established connections.

   When either is exhausted, the gateway does not start the handshake: it closes the connection (or sends `Busy{retry_after}` before any crypto, if the framing allows) and counts a `handshake_rate` reject. The bucket must allow at least the storm SLO rate (20k/s peak per O1/O7) with margin. During a legitimate storm it shapes the load rather than rejecting it.
- Metrics: admission rejects labelled by reason (`handshake_rate`, `login_queue`, `pending_mem`, `handshake_timeout`, `auth_timeout`).
- Tests (handshake limit): a garbage-handshake flood from many sources keeps the gateway's CPU for existing connections (heartbeats still answered on time), and a legitimate 20k/s storm is not rejected by the bucket.
- Tests: a slow-client flood (slots are freed by the timeouts, and legitimate clients get no Busy); Logic slowed down (the Login-queue cap triggers Busy, and clients retry within the SLO).

## O12 — specification quality — low — PARTIALLY ACCEPTED

**Decision:** Fix the RPC list and add a gRPC `MaxConnectionAgeGrace` of 30s. The session-validity read path is deferred to sub-projects 2 and 3.

**Rationale:**
- The RPC list must match the decisions above, or `internal.proto` would be incomplete on day one.
- Without a grace period, forced connection recycling aborts in-flight Login batches. The O9 fence makes that safe, but it still costs needless retries.
- Read path: an exact `HGETALL` + `EXISTS` per epoch is unacceptable for performance at 1M connections. An in-memory live-epoch set would require keeping caches consistent across Logic nodes, which is out of scope for sub-project 1. Optimising the read path is left to sub-projects 2 and 3.

**Rejected / deferred:**
- `internal/session.Lookup` with an in-memory live-epoch set: **deferred** to sub-projects 2/3.
- Lookup through `HGETALL` + `EXISTS` per epoch: **rejected** (performance).

**Spec changes to apply:**
- Logic RPC list becomes `Login` (batch), `Logout` (batch), `GatewayLease` (on its own channel, O2), `EpochEnded`, `Reconcile` (batch or streaming, O2/O3/O8/O10), and `Revoke` (O10). Remove the "slow batch RPC" for refreshing the TTL (replaced by Reconcile, O3).
- Internal RPC decision: the Logic server sets `MaxConnectionAge` ≈5m **and `MaxConnectionAgeGrace` = 30s** (above the 6s Login timeout from O9), so in-flight batches finish after GOAWAY.
- Next to the "A session is valid only if..." rule, add a note: the validity rule is defined here, but an efficient read path for consumers is deferred to sub-projects 2/3. They must not implement it as `HGETALL` + `EXISTS` per epoch.
