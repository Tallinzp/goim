# Sub-project 1: Connection & Session Core — Design (revision 2)

> Revision 2 of `2026-10-03-connection-session-core-design.md`, which is left unchanged. This revision applies every decision in `docs/superpowers/objections/connection-session-core-design-dispositions.md` (O1–O12). References like *(O4)* point at the disposition that introduced a change. Points this revision had to interpret are listed under **Open points** at the end.

## Context

We are building a Go IM backend for a WeChat-like social app (Android, iOS, web) targeting ~1M concurrent users. The user previously built a C++ IM handling 2.6M TCP connections (600k per gateway). Two earlier draft docs (`gpt_design.md`, `claude_design.md`, added in `4ce1fa6` and removed in `05cd570`) disagree on presence model and scope, and leave the "delayed disconnect deletes the new session" race unsolved.

The full system is decomposed into 5 sub-projects (1 connection/session core, 2 messaging, 3 push pipeline, 4 social feed, 5 scale/hardening). **This spec covers sub-project 1 only**: connect, encrypted handshake, login, heartbeat, kick, revoke, disconnect — correct under reconnect races, and meeting a storm-recovery SLO. Sub-project 1 ships Android/iOS over TCP; the web transport is designed here and built later.

**Success criteria**
- **Capacity ceiling** *(O1)*: one gateway holds 600k connections (measure CPU / memory / GC). This is a benchmark, not the operating point.
- **Storm SLO** *(O1, O7)*: sized at the 200k operating point, defined per failure class:

  | Failure class | How clients detect it | SLO |
  |---|---|---|
  | Process crash / pod kill (node alive) | Kernel RST/FIN, passed through by the L4 NLB | 99% logged in again within 30s **of gateway death** |
  | Node loss / silent network partition | Client heartbeat (Pong timeout) | 99% logged in again within 30s **of the client detecting the loss**. Detection is bounded by design: at most one heartbeat interval plus the Pong timeout. |

  Arithmetic: 200k users spread over the 0–20s reconnect jitter is ~10k logins/s, within the 20k/s peak headroom.
- **Correctness:** no stale event (late disconnect, late login from the same or another device, zombie gateway) can delete or override a newer session; covered by automated tests.

**Density** *(O1)*: the operating point is at most 200k connections per gateway. 1M users need at least 6 gateways (5 at 200k plus 1 for N−1), about 167k each in steady state. On Kubernetes pods are cheap and blast radius is the cost that matters; 600k stays as the efficiency benchmark.

## Decisions (agreed)

| Topic | Decision |
|---|---|
| Presence | One session per **slot** = `userid + appid + platform` (`mobile` covers Android and iOS; plus `pad`, `web`, `pc`). New login on the same slot replaces the old one. Platform is validated against the token. Different platforms are different slots and never conflict. |
| Client transport | Mobile: raw TCP via gnet. Web: WebSocket over wss (TLS terminated at the cloud LB), designed now behind a transport interface, built as a late slice. |
| Client encryption | Custom app-layer encryption using the **Noise Protocol Framework, NK pattern** (`flynn/noise`): server static key pinned in the app, with a key ID for rotation; ChaCha20-Poly1305 per frame. No resumption tickets (a handshake costs ~100µs, negligible in storms). Web relies on wss TLS and uses no Noise. |
| Internal RPC | gRPC both ways. Gateway→Logic: `dns:///` headless service + `round_robin`; Logic server sets `MaxConnectionAge` ≈5m **and `MaxConnectionAgeGrace` = 30s** (above the 6s Login timeout) so in-flight batches finish after GOAWAY *(O12)*. `GatewayLease` uses its own gRPC connection *(O2)*. Logic→Gateway: Kick dialed by StatefulSet pod DNS (`gw-17.gateway.im.svc`), so no registry is needed; the gateway headless service sets `publishNotReadyAddresses: true` so draining pods still resolve *(O8)*. |
| Auth | Separate account service issues Ed25519-signed JWTs as **short-lived access tokens (e.g. 1h), checked only at Login**. Token expiry does not affect a connection that is already authenticated; security for long-lived connections comes from revocation *(O10)*. Logic verifies tokens locally against a JWKS keyed by `kid` (no database call on login). `cmd/devtoken` issues tokens and publishes a test JWKS. |
| Source of truth | The gateway is the source of truth for which connections exist. Redis is reconciled to it by periodic anti-entropy (`Reconcile`) *(O3)*. |
| Deployment | Kubernetes on cloud. Gateways run as a StatefulSet behind an L4 NLB; Logic is a stateless Deployment with HPA. |
| Layer rule | The gateway never touches Redis. Only Logic touches Redis. |
| Dropped | Per-user login rate limit, distributed lock, reverse mapping, session version, per-IP limiting (each justified below). |

## Architecture

```
Android/iOS ─TCP──┐
                  ├─ cloud NLB ─► gateway StatefulSet (gnet loops · Noise · frames · heartbeat · admission)
Web ─wss (TLS)────┘                 │ gRPC Login/Logout/Reconcile (batched)      ▲ gRPC Kick (pod DNS)
                                    │ gRPC GatewayLease (own connection)         │
                                    ▼                                            │
                              logic Deployment (stateless, HPA) ─────────────────┘
                                    │                ▲ gRPC Revoke (account service)
                               Redis Cluster
```

Repo: one Go module containing `cmd/{gateway,logic,devtoken,loadgen}`, `internal/...`, `api/{client,internal}.proto`, `deploy/{compose,k8s}`.

## Client protocol

- Outer frame: `[u32 length][payload]`, max 64KB. The gateway buffers partial frames and splits frames that arrive together.
- Frame 1: Noise NK handshake (carries a key ID). After that, every payload is AEAD-sealed, with a nonce counter per direction.
- Plaintext: `Envelope{cmd, req_id, body}`. Commands: `Login`, `LoginResp` (includes the heartbeat interval), `Ping/Pong`, `Logout`, `Kick{reason}` (from the server), `Reconnect{jitter}` (from the server), `Error`, `Busy{retry_after}`.
- `LoginReq` carries:
  - `token`, `appid`, `platform`;
  - `device_id` (see below) and `conn_seq` (counter on the device, incremented on every connection attempt);
  - `reconnect_reason` *(O7)*: enum — first connect, RST/FIN, Pong timeout, Busy retry, Reconnect from drain, …;
  - `offline_ms` *(O7)*: ms since the client detected the loss, on the client's monotonic clock (a duration, so phone and server clocks need not agree).
- Login errors (in `Error`):

  | Code | Meaning | Client action |
  |---|---|---|
  | `STALE` | A newer connection from this device already holds the slot. Carries the stored `conn_seq` *(O5)*. | Set counter to `max(local, stored) + 1`, persist, retry after backoff. |
  | `SUPERSEDED` | Another device on the same slot logged in later *(O4)*. | Retryable; a retry is a new, later login and wins normally. |
  | `EXPIRED` | The login reached Redis more than `login_deadline` after the gateway received it *(O4)*. | Retry. |
  | `REVOKED` | Token issued before the user's revocation marker *(O10)*. | Drop the token, show the login screen. |
- Kick reasons. **None of them triggers an automatic reconnect:**
  - `REPLACED`: same device, newer connection; not shown to the user. The client goes idle and reconnects only on a user action or when the app returns to the foreground. The SDK logs it as an anomaly, since a live client only sees it if a second instance shares its device_id *(O6)*.
  - `KICKED_BY_OTHER_DEVICE`: shown to the user.
  - `REVOKED` *(O10)*: the client drops its token and shows the login screen.
- **device_id** *(O6)* identifies one client instance that holds a connection. The SDK must make sure at most one connection per device_id is in use at a time. Mobile and desktop: one per install, plus a process-level guard (e.g. a single connection service on Android).

### Client SDK rules

- **conn_seq persistence** *(O5)*: persist `conn_seq` **before** each connection attempt, in the same place as device_id with the same lifetime (e.g. both in the iOS Keychain), so clearing one clears the other.
- **Reconnect** *(O7)*: after an unexpected close, wait a random **0–20s** before the first attempt (leaving 10s for handshake, login and one Busy retry), then exponential backoff (cap 5m). Obey `retry_after`.
- **Dead-server detection** *(O7)*: if no frame arrives within **10s** of a Ping, close and reconnect.
- **Web** (designed now, built later) *(O6)*: one connection per browser profile. Tabs elect a leader through `SharedWorker`, falling back to Web Locks plus `BroadcastChannel`. The leader holds the wss connection and relays messages to the other tabs; when the leader tab closes, another tab takes over. device_id and conn_seq live in localStorage.

## Gateway

- gnet **≥ v2.8.0** (needed for `Conn.EventLoop()`). No goroutine per connection. `OnTraffic` never blocks; RPCs go through a bounded worker pool.
- Connection states: `HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`.
- **Identity:** `gateway_id` = pod name; `epoch` = random uint64 chosen at boot; `conn_id = (worker_id << 48) | loopState.counter++`.
  - Checked against gnet's `dev` source: `OnOpen` always runs on the owning loop's goroutine (both accept paths). The loop index is unexported, but `Conn.EventLoop()` gives the loop's identity.
  - `worker_id` is assigned lazily the first time `OnOpen` sees a loop (an atomic that runs about NumCPU times), cached in a `sync.Map` from EventLoop to loopState.
  - The plain `counter++` is safe only because just `OnOpen` touches it. The counter is unexported, and a `-race` test enforces the rule.
- **Per-loop connection tables with no locks:** `map[conn_id]*conn`, touched only by the owning loop. A Kick from gRPC reads `conn_id >> 48`, takes that loop's handle and calls `loop.Execute(...)` to look up the connection, check the epoch and state, and act. This also meets gnet's rule that Close runs on the owning loop.
- **Kick handling** *(O8)*:
  - `ONLINE`: send the Kick frame and close.
  - `AUTH_PENDING` (Login in flight): set a kick-pending mark with the reason. When the Login reply arrives, send the Kick frame and close instead of moving to `ONLINE`.
  - `HANDSHAKE`, `CLOSED`, unknown conn_id, or epoch mismatch: ignore.
- **Heartbeat:** **150s by default, set by the server in `LoginResp`**. Any frame resets the timer. A connection is dead after 2 × interval + 10s (= 310s), and the gateway then sends Logout (subject to the fence). A timing wheel drives the timers.
- **Timeouts** *(O11)*: `HANDSHAKE` 5s (accept → handshake complete); `AUTH_PENDING` 5s (handshake complete → LoginReq received). On expiry the connection closes silently. Both run on the timing wheel.
- **Admission control** *(O11)*:
  - **Noise handshake limit (per gateway, global; never per IP):** a concurrency limit on handshake computations running at once (a semaphore sized to the cores dedicated to handshakes) plus a gateway-wide token bucket on handshakes/s, sized from the measured NK cost (~100µs) so handshakes can't starve established connections. The bucket must allow at least the 20k/s storm peak with margin; in a legitimate storm it shapes load rather than rejecting it. When either is exhausted the gateway does not start the handshake: it closes the connection (or sends `Busy{retry_after}` before any crypto, if the framing allows) and counts a `handshake_rate` reject.
  - **Login-queue cap:** cap the Logins waiting for or inside an RPC to Logic (this reflects Logic backpressure). At the cap, reply `Busy{retry_after}` and close.
  - **Memory-only pending cap:** a large cap on total `HANDSHAKE` + `AUTH_PENDING` connections, only to protect gateway memory. It must not trigger in a normal storm.
  - `Busy{retry_after}` uses a short value, **1–3s with jitter**, so rejected clients still fit within the SLO *(O7)*.
  - One login attempt per connection (a failure closes it).
  - Queued Logins older than `login_deadline` (measured from `recv_ms`) are dropped before being sent; they never reach Logic, so they need no Logout.
- **recv_ms** *(O4)*: the gateway stamps `recv_ms` (wall clock, NTP-synced) when it receives the Login frame and passes it in the Login RPC.
- **Login batching:** up to 200 logins or 5ms per RPC. The Login RPC timeout is at least `login_deadline + 1s` (= 6s by default), measured from `recv_ms` *(O9)*.
- **Logout fence** *(O9)* — gateway invariant: the gateway sends Logout for a connection only after
  - (a) it has a **definitive Login reply** (success or rejection), at which point the Lua write has happened or never will; or
  - (b) `recv_ms + login_deadline + 1s` has passed (the 1s absorbs gateway↔Redis clock skew). This applies to **any** Login without a definitive reply: timeout, RPC error, a gRPC connection reset by `MaxConnectionAge`, or a Logic crash.

  A connection that closes before either condition holds keeps a pending Logout that fires when one becomes true. Logouts go through a bounded retry queue (60s); dropping one is safe because the next Reconcile repairs it *(O3)*.
- **Lease** *(O2)*: call `GatewayLease` every 10s on a **dedicated gRPC connection**, never batched with or queued behind Login/Logout/Reconcile. The reply says whether the epoch was found **live** or **missing** (lease key absent and `gwepochs` member gone, i.e. swept). On `missing`, the gateway's lease call re-registers the epoch and the gateway starts an immediate Reconcile.
- **Reconcile** *(O3, O8, O10)*: every ~10 min, with random jitter per gateway, and immediately on a `missing` lease reply, the gateway sends its ONLINE set — `(uid, appid:platform, conn_id, device_id, conn_seq, login_ms, tok_iat)` — in rate-limited chunks. The snapshot carries a `cutoff_ms` taken when it started. For each entry Logic returns in `not_owner`, the gateway sends the Kick frame with the given reason and closes the connection.
- **Graceful drain:** on SIGTERM, go NotReady and stop accepting, send `Reconnect{jitter}` in batches over about 60s, close the rest, then call `EpochEnded`. `terminationGracePeriodSeconds` must be longer than the drain window. Kicks still reach a draining gateway (`publishNotReadyAddresses`).

## Logic

- Stateless. Every node serves every RPC and can kick any gateway. Scales out horizontally (HPA on CPU).
- **RPCs** *(O12)*: `Login` (batch), `Logout` (batch), `GatewayLease` (own channel and **separate worker pool**, so Login load can't starve it *(O2)*), `EpochEnded`, `Reconcile` (batch or streaming), `Revoke`.
- **Login:** verify the JWT locally (JWKS by `kid`), then run the Lua login script (EVALSHA, pipelined) with `recv_ms` and the token's `iat`, then do the follow-ups: SADD to the epoch index; if the result needs a kick, send it with the hardened Kick below.
- **Hardened Kick** *(O8)*: Logic always sends the Kick; it does not skip it when the old gateway's lease looks expired (an epoch mismatch on the gateway makes a stray Kick harmless). Bounded retry: up to 3 attempts over ~30s on transport errors, then give up and increment a metric. Reconcile is the backstop.
- **JWKS** *(O10)*: load the Ed25519 key set from the account service at startup and refresh every 5 min. During a rotation every published key is accepted. If a refresh fails, keep the last good set and raise an alert. A JWT with an unknown `kid` triggers at most one rate-limited refresh before it is rejected.
- **Revoke** *(O10)*: `Revoke(uid, scope = all | slot, reason)`, called by the account service on a ban, a password change or "log out all devices". Logic sets `rev:{uid}` (scope=all), deletes the affected `sess:{uid}` fields, and kicks each affected connection with reason `REVOKED` using the hardened Kick.
- **Reconcile handling** *(O3, O8, O10)*, per snapshot entry, in this order:
  1. `tok_iat < revoked_before_ms` → return it in `not_owner` with reason `REVOKED`; write nothing.
  2. Field holds this exact `gw|epoch|conn_id` → refresh the `sess:{uid}` TTL; ensure the uid is in `gwconns`.
  3. Field holds a **different** session whose `gw|epoch` has a **live** lease → return it in `not_owner` with `KICKED_BY_OTHER_DEVICE` (different device_id) or `REPLACED` (same device_id). Never override it.
  4. Field is empty, or its `gw|epoch` has no live lease → write the entry (restore), refresh the TTL, ensure the uid is in `gwconns`.

  Because the lease key lives in a different slot from `sess:{uid}`, Logic checks lease liveness outside Lua and passes the observed field value to a compare-and-set Lua script, which writes only if the field is still what Logic observed.

  After the snapshot, Logic SSCANs `gwconns:gw:{epoch}`. For each field that points at this `gw|epoch`, whose conn_id is not in the snapshot, and whose `login_ms < cutoff_ms`, it compare-and-deletes. The cutoff protects Logins that were in flight while the snapshot ran. It also removes uids with no remaining field from `gwconns`.

  Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway, ~2k/s cluster-wide at 6 gateways.

## Redis schema (Cluster; per-user keys hash-tagged `{uid}`)

| Key | Type | Value / TTL | Purpose |
|---|---|---|---|
| `sess:{uid}` | HASH | field `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`; `login_ms` is the gateway's `recv_ms` *(O4)*; key TTL 30d, refreshed on login and by Reconcile | online mark (source of truth in Redis) |
| `rev:{uid}` | STRING | `revoked_before_ms`; TTL = max token lifetime + margin *(O10)* | revocation marker |
| `gwlease:gw:{epoch}` | STRING | `PX 30000`, renewed every 10s with a plain SET | gateway instance is alive |
| `gwepochs` | ZSET | member `gw:epoch`, score = last renewal in ms | registry of epochs, used by the sweeper |
| `gwconns:gw:{epoch}` | SET | uids; pruned by Reconcile | cleanup hint (not atomic with sess, not needed for correctness) |
| `sweep:gw:{epoch}` | STRING | `SET NX PX 300000` | sweeper claim |

**A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`.

> The validity rule is defined here, but an efficient read path for consumers is deferred to sub-projects 2/3. They must **not** implement it as `HGETALL` + `EXISTS` per epoch *(O12)*.

### Login Lua (single slot, atomic)

Inputs: the new entry, `recv_ms`, token `iat`, `login_deadline` (default 5s). Steps:

1. Read `rev:{uid}`. If `iat < revoked_before_ms` → `REVOKED`; no write *(O10)*.
2. Read Redis `TIME`. If `now − recv_ms > login_deadline` → `EXPIRED`; no write *(O4)*.
3. Compare the stored field with the new login:

| Stored vs new | Result |
|---|---|
| empty | write; no kick |
| same device_id, new conn_seq > stored | overwrite; kick the old connection with `REPLACED` |
| same device_id, new conn_seq ≤ stored, `recv_ms ≤ stored login_ms + login_deadline` | reject as `STALE` (return stored conn_seq); close; no write *(O5)* |
| same device_id, new conn_seq ≤ stored, `recv_ms > stored login_ms + login_deadline` | counter regression: overwrite; kick the old connection with `REPLACED`; increment `conn_seq_regressed` *(O5)* |
| different device_id, `recv_ms ≥ stored login_ms` | overwrite; kick the old one with `KICKED_BY_OTHER_DEVICE` |
| different device_id, `recv_ms < stored login_ms` | reject as `SUPERSEDED`; no write, no kick; the gateway closes with a retryable error *(O4)* |

Lease liveness of the stored entry does not change the write decision (Lua can't read the lease key, which is in a different slot). Kicks are always sent (O8).

Assumption *(O4)*: clock skew between gateways (NTP, a few ms) is far below the time a person takes to switch devices.

### Logout Lua

`HDEL` the field only if it still matches `gw|epoch|conn_id`. Because conn_id is never reused, this handles the delayed-disconnect race, including on the same gateway. Ordering against the connection's own Login is guaranteed by the gateway's Logout fence.

### Sweeper (goroutine in every Logic node)

Find `gwepochs` members whose last renewal is more than 60s old (or that received `EpochEnded`), claim each with `sweep:` SET NX, run `SSCAN gwconns`, and for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch. Then delete the set and the ZSET member. Rate-limited to about 10k users/s.

**Mass-stale guard** *(O2)*: before sweeping, count the epochs that went stale in the same window. If more than a configurable threshold (e.g. >1 epoch, or >20% of live epochs) are stale at once, treat it as systemic (Redis or Logic trouble, not gateway death): skip sweeping, raise an alert and metric, and re-check next cycle. Epochs that sent `EpochEnded` are exempt and always swept. After Redis becomes reachable again, the sweeper waits one full lease period (30s) before trusting any staleness. Skipping a sweep never affects correctness — entries of a dead epoch are already invalid by the validity rule; the sweeper only cleans up.

Safety net: the 30-day TTL on `sess:{uid}`, refreshed by login and Reconcile *(O3)*.

### Why the dropped concepts are not needed

- **Login rate limit (per user):** blocks real fast reconnects (Wi-Fi to 4G), and a rejected login costs Redis the same as an accepted one. Abuse protection moves to the gateway at no Redis cost.
- **Per-IP limiting** *(O11)*: mobile IPs are mostly shared through carrier-grade NAT, so per-IP limits hurt legitimate users, especially in a reconnect storm. The gateway's handshake limit is global. Any future abuse limiting keys on device_id or user_id (possible only after LoginReq is decrypted); not designed in sub-project 1. Without per-IP limits, sub-project 1 doesn't need the real client IP, so proxy protocol is deferred to the web phase.
- **Distributed lock:** every change is a single atomic Lua script on a single slot.
- **Reverse mapping:** the gateway knows which user owns each connection; cleanup uses `gwconns`, and Reconcile keeps Redis aligned with the gateway.
- **Session version:** the `gw|epoch|conn_id` triple plus `device_id`/`conn_seq`/`login_ms` covers it.

## Races handled

1. **Delayed disconnect** (old G17 logout after the new G42 login): Logout compare-and-delete doesn't match, so nothing happens.
2. **Delayed login from an abandoned connection, same device** (S1's login lands after S2's): older conn_seq within the deadline window → `STALE`, S2 unaffected. A login later than the deadline is `EXPIRED` anyway.
3. **Delayed login from another device on the same slot** *(O4)*: its `recv_ms` is older than the stored `login_ms` → `SUPERSEDED`; the newer device is not kicked.
4. **Zombie gateway with the same pod name:** leases are keyed per epoch, so the instances don't conflict, and a Kick that reaches the wrong instance is ignored because the epoch doesn't match.
5. **Connection closes while its Login is in flight** *(O9)*: the Logout fence holds the Logout until the definitive Login reply (fence a), so the Logout always lands after the write.
6. **Login without a definitive reply, but Redis was written** (timeout, RPC error, `MaxConnectionAge` reset, Logic crash) *(O9)*: the Lua deadline means no write can happen after `recv_ms + login_deadline`; the gateway waits `recv_ms + login_deadline + 1s` (fence b) before sending Logout, so the Logout always lands after any possible write.
7. **Logout lost while the gateway is alive:** the stale entry is removed by the next Reconcile *(O3)*. The sweeper only handles dead epochs.
8. **Kick lost or arriving early** *(O8)*: a Kick during `AUTH_PENDING` is held as kick-pending and applied when the Login reply arrives; a Kick lost after 3 retries is applied by the next Reconcile through `not_owner` (≤ ~10 min).
9. **Two devices kick each other back and forth:** the client must not reconnect automatically after `KICKED_BY_OTHER_DEVICE`.
10. **Two client instances share a device_id** *(O6)*: they replace each other once, and because `REPLACED` doesn't reconnect, the loop stops.
11. **Device counter went backwards** (reinstall, restored backup) *(O5)*: a low conn_seq outside the deadline window is accepted as a regression; inside it, `STALE` returns the stored counter so the client resyncs.
12. **Sessions swept while their gateway was healthy** (long Redis outage, Logic saturation) *(O2)*: the mass-stale guard usually prevents it; if it happens anyway, the lease reply reports `missing` and Reconcile restores the sessions.
13. **Two loops generating conn_ids at once:** each loop has its own counter, and the worker_id prefix keeps the IDs apart.

Note: when the same device reconnects, `conn_seq` also replaces any ghost entry. Reconcile covers devices that never return.

## Storms, drain, failures

- Client SDK: see *Client SDK rules* (0–20s jitter, exponential backoff, 10s Pong timeout, obey `retry_after`).
- Capacity: N−1 gateways must absorb one dead gateway's connections, sized at the 200k operating point *(O1)*.
- Graceful drain: see *Gateway*.

| Failure | Behavior |
|---|---|
| Logic down | Login gets a retryable error and the client backs off. Logout goes into a bounded retry queue for 60s, then is dropped; dropped Logouts are repaired by the next Reconcile. |
| Logic saturated *(O2)* | Lease RPCs keep their own connection and worker pool, so renewals continue. The gateway's Login-queue cap replies `Busy` (1–3s). |
| Redis down | New logins fail; existing connections stay up. Lease SET resumes when Redis returns, and entries become valid again. Outages longer than 60s are covered by the mass-stale guard; any sessions swept anyway are restored by Reconcile when the lease reply reports `missing`. |
| Gateway process crash / pod kill | Clients see RST/FIN and reconnect within the 0–20s jitter. The lease expires within 30s; the sweeper cleans up after 60s. |
| Node loss / silent partition | Clients detect it by Pong timeout (≤ one heartbeat interval + 10s) and then reconnect. Server side as for a crash. |
| Kick can't reach the gateway | Retried up to 3 times over ~30s; after that the Reconcile backstop closes the connection within one cycle. |
| Handshake flood | The global handshake concurrency limit and token bucket protect established connections; excess handshakes are closed and counted. |
| Slow clients | `HANDSHAKE` / `AUTH_PENDING` 5s timeouts free their slots. |
| Bad frame / decryption failure / frame too large | Close immediately. |

## Interface constraints for later sub-projects

- **Ownership rule** *(O8)*: a connection may act as the user (send messages, perform user operations) only while its `gw|epoch|conn_id` owns its slot in `sess:{uid}`. Sub-project 2 must enforce this on the send path (a cached ownership check is acceptable). Sub-project 1 only documents it and provides the data it relies on (session entries plus Kick/Reconcile delivery).
- **Session-validity read path** *(O12)*: deferred to sub-projects 2/3; must not be `HGETALL` + `EXISTS` per epoch.
- **Lazy repair** in sub-project 3 (a push that gets "unknown conn" leads Logic to compare-and-delete) is optional; correctness no longer depends on it *(O3)*.

## Observability

Prometheus metrics:
- connections by state for each gateway; handshakes/s;
- logins by result: `ok`, `stale`, `replaced`, `kicked`, `superseded`, `expired`, `revoked`, `busy`, `error`; plus `conn_seq_regressed`;
- end-to-end login latency p50/p99; gRPC and Redis script latency;
- **relogin latency** histogram: `offline_ms` + (login OK − `recv_ms`), labelled by `reconnect_reason` *(O7)*;
- **dead-epoch cross-check** *(O7)*: when a relogin finds the old entry pointing at a dead epoch, record `login_ms − that epoch's last lease renewal` (time from gateway death to relogin, ±10s, no client data needed);
- lease renewal failures; lease replies with `missing`;
- admission rejects by reason: `handshake_rate`, `login_queue`, `pending_mem`, `handshake_timeout`, `auth_timeout`;
- Kick attempts, retries and give-ups; revocation kicks;
- Reconcile: entries refreshed, restored, deleted, `not_owner` by reason; cycle duration;
- sweeper progress; **mass-stale guard trips** (also an alert);
- JWKS refresh failures and unknown-`kid` refreshes;
- **reconnect-after-idle rate** (shows NAT timeouts shorter than 150s); Go GC pause and heap.

Logs: `slog` lines with `uid` and `conn_id`; a request ID passed in gRPC metadata.

## Verification

- **Unit tests:** frame codec (partial frames, several frames at once, fuzzing), Noise handshake against a test client, the connection state machine (including kick-pending in `AUTH_PENDING`, both timeouts, and the Logout fence), conn_id generation, and the **gnet loop-ownership test under `-race`** (`OnOpen`, `OnClose` and the `Execute` callback all see the same loop for a given connection).
- **Lua tests** against a real Redis in Docker:
  - one table test for each row of the login table, including `SUPERSEDED`, `EXPIRED`, `STALE` (low conn_seq inside the window is rejected) and the regression row (low conn_seq outside the window is accepted);
  - `REVOKED` (token iat before the marker is rejected);
  - the logout compare-and-delete;
  - the Reconcile compare-and-set and the post-snapshot compare-and-delete with `cutoff_ms`;
  - the sweeper deleting only dead-epoch fields.
- **Race integration tests** (`deploy/compose`: Redis, Logic, 2 gateways and a scripted client):
  - races 1–6 and 10 above; race 6 in both orders (Logout before Login and Login before Logout, with injected Logic delays): no ghost after the fence; a Login with no definitive reply caused by a Logic crash: the Logout waits for the fence;
  - gateway crash → lease expiry → sweep → entries gone;
  - Redis paused for 90s: sessions are still present afterwards, or restored by Reconcile;
  - Reconcile: a ghost entry is removed within one cycle; a missing entry is restored; a newer session on another gateway is never overridden; a Login in flight during a snapshot is not deleted; a dropped Logout is repaired within one cycle;
  - Kick: a dropped Kick is applied by the next Reconcile; a Kick during `AUTH_PENDING` closes the connection when the Login reply returns; a draining gateway still receives Kicks;
  - Revoke closes connections on two slots; a Revoke whose Kick is dropped is caught by the next Reconcile;
  - key rotation: both `kid`s are accepted during the overlap.
- **Load tests** (`cmd/loadgen`, multiple source IPs; loadgen records detection time and login-success time per client and computes the SLO distribution directly):
  - (a) 600k connections on one gateway (ceiling), with GOMEMLIMIT/GOGC tuning;
  - (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of death;
  - (b2) iptables DROP on a 200k gateway (silent node loss): 99% logged in again within 30s of client detection;
  - (b-stress) kill a 600k gateway: measured, no SLO;
  - (c) 150s heartbeat CPU cost at 200k and at 600k;
  - storm: the other gateways never miss a lease renewal;
  - handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket;
  - slow-client flood: slots are freed by the timeouts and legitimate clients get no `Busy`;
  - Logic slowed down: the Login-queue cap triggers `Busy` and clients retry within the SLO.

## Out of scope (later sub-projects)

Messaging (seq, dedup, MongoDB) · push pipeline (RocketMQ, APNs/FCM) · social feed · QR-code login for web · adaptive heartbeat on the client · account service (its events reach us only through `Revoke` and the JWKS) · send-path ownership enforcement (sub-project 2) · session-validity read path (sub-projects 2/3) · proxy protocol and real client IP (web phase) · device_id/user_id-keyed abuse limiting.

## Open points

Points the dispositions left open, interpreted here; confirm or override:

1. **Reconcile vs. lease check atomicity.** The dispositions say the Reconcile Lua writes "if the stored `gw|epoch` has no live lease", but the lease key is in a different Redis slot. This revision checks liveness in Logic and makes the Lua a compare-and-set on the observed field value (see *Reconcile handling*).
2. **Reconcile when the stored live session is older.** Following O3/O8 literally, any *different* session with a live lease wins and the snapshot entry gets `not_owner`, even if the snapshot entry has a later `login_ms` (only possible after a lost write, e.g. a Redis failover). This keeps convergence deterministic; ordering by `login_ms`/`conn_seq` instead is the alternative.
3. **`Revoke(scope = slot)` and Reconcile.** Slot-scoped revocation deletes the field but sets no `rev:{uid}` marker. If its Kick is lost, the next Reconcile would see an empty field and *restore* the session (rule 4) instead of closing it. A per-slot marker (e.g. a `rev:{uid}` hash keyed by `appid:platform`) would close the gap; not yet decided.
4. **Order of `REVOKED` vs `EXPIRED`** in the login Lua: revocation is checked first, as O10 says "read `rev:{uid}` first". The outcome differs only in which error the client sees.
