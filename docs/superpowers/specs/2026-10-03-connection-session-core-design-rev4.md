# Sub-project 1: Connection & Session Core — Design (revision 4)

> 中文版: [2026-10-03-connection-session-core-design-rev4.zh-CN.md](2026-10-03-connection-session-core-design-rev4.zh-CN.md). This English version is authoritative.

> Revision 4 of `2026-10-03-connection-session-core-design-rev3.md`, which is left unchanged. This revision applies every decision in `docs/superpowers/objections/connection-session-core-design-rev3-dispositions.md` (O1–O7 of the third review, plus its clarifications of rev3's open points 4–6), and the follow-up decisions on rev4's own open points recorded in the same file (*R3-OP2*, *R3-OP4*–*R3-OP7*). Every review round numbers its objections from O1, so references are prefixed: *(R1-O4)* points at `connection-session-core-design-dispositions.md` (first review), *(R2-O4)* at the rev2 dispositions (second review), *(R3-O4)* at the rev3 dispositions (third review). Points this revision had to interpret are listed under **Open points** at the end.

## Context

We are building a Go IM backend for a WeChat-like social app (Android, iOS, web) targeting ~1M concurrent users. The user previously built a C++ IM handling 2.6M TCP connections (600k per gateway). Two earlier draft docs (`gpt_design.md`, `claude_design.md`, added in `4ce1fa6` and removed in `05cd570`) disagree on presence model and scope, and leave the "delayed disconnect deletes the new session" race unsolved.

The full system is decomposed into 5 sub-projects (1 connection/session core, 2 messaging, 3 push pipeline, 4 social feed, 5 scale/hardening). **This spec covers sub-project 1 only**: connect, encrypted handshake, login, heartbeat, kick, revoke, disconnect — correct under reconnect races, and meeting a storm-recovery SLO. Sub-project 1 ships Android/iOS over TCP; the web transport is designed here and built later.

**Success criteria**
- **Capacity ceiling** *(R1-O1)*: one gateway holds 600k connections (measure CPU / memory / GC). This is a benchmark, not the operating point.
- **Storm SLO** *(R1-O1, R1-O7, R2-O3)*: covers the loss of **one gateway**, sized at the 200k operating point, defined per failure class:

  | Failure class | How clients detect it | SLO |
  |---|---|---|
  | Process crash / pod kill (node alive) | Kernel RST/FIN, passed through by the L4 NLB | 99% logged in again within 30s **of gateway death** |
  | Node loss / silent network partition | Client heartbeat (Pong timeout) | 99% logged in again within 30s **of the client detecting the loss**. Detection is bounded by design: at most one heartbeat interval plus the Pong timeout. |

  Arithmetic: 200k users spread over the 0–20s reconnect jitter is ~10k logins/s, within the 20k/s peak headroom.

  Losing several gateways at once (e.g. a zone) is **degraded, best effort, with no SLO** *(R2-O3)*: clients may be rejected with `at_capacity` and retry until the lost pods are rescheduled.
- **Correctness:** no stale event (late disconnect, late login from the same or another device, zombie gateway, late Reconcile restore, late sweep) can delete or override a newer session; covered by automated tests.

**Density** *(R1-O1, R2-O2, R2-O3)*: the operating point is at most 200k connections per gateway. 1M users need at least 6 gateways (5 at 200k plus 1 for N−1), about 167k each in steady state. On Kubernetes pods are cheap and blast radius is the cost that matters; 600k stays as the efficiency benchmark. A hard per-gateway connection cap (default 250k, see *Admission control*) bounds skew: with one gateway down the remaining five hold up to 5 × 250k = 1.25M ≥ 1M; with a whole zone down (4 survivors) they can still hold 1M, just barely.

## Decisions (agreed)

| Topic | Decision |
|---|---|
| Presence | One session per **slot** = `userid + appid + platform` (`mobile` covers Android and iOS; plus `pad`, `web`, `pc`). New login on the same slot replaces the old one. Platform is validated against the token. Different platforms are different slots and never conflict. |
| Client transport | Mobile: raw TCP via gnet. Web: WebSocket over wss (TLS terminated at the cloud LB), designed now behind a transport interface, built as a late slice. |
| Client encryption | Custom app-layer encryption using the **Noise Protocol Framework, NK pattern** (`flynn/noise`): server static key pinned in the app, with a key ID for rotation; ChaCha20-Poly1305 per frame. No resumption tickets (a handshake costs ~100µs, negligible in storms). Web relies on wss TLS and uses no Noise. |
| Internal RPC | gRPC both ways. Gateway→Logic: `dns:///` headless service + `round_robin`; Logic server sets `MaxConnectionAge` ≈5m **and `MaxConnectionAgeGrace` = 30s** (above the 6s Login timeout) so in-flight batches finish after GOAWAY *(R1-O12)*. `GatewayLease` uses its own gRPC connection *(R1-O2)*. Logic→Gateway: Kick dialed by StatefulSet pod DNS (`gw-17.gateway.im.svc`), so no registry is needed; the gateway headless service sets `publishNotReadyAddresses: true` so draining pods still resolve *(R1-O8)*. |
| Auth | Separate account service issues Ed25519-signed JWTs as **short-lived access tokens (e.g. 1h), checked only at Login**. Token expiry does not affect a connection that is already authenticated; security for long-lived connections comes from revocation *(R1-O10)*. Logic verifies tokens locally against a JWKS keyed by `kid` (no database call on login), with a ConfigMap fallback for cold start *(R2-O7)*. `cmd/devtoken` issues tokens and publishes a test JWKS. |
| Source of truth | The gateway is the source of truth for which connections exist. Redis is reconciled to it by periodic anti-entropy (`Reconcile` + `ReconcileEnd`) *(R1-O3, R3-O1)*. |
| Deployment | Kubernetes on cloud. Gateways run as a StatefulSet behind an L4 NLB, with `topologySpreadConstraints` on `topology.kubernetes.io/zone` and `kubernetes.io/hostname`, `maxSkew: 1`, so a single node failure is a true N−1 event *(R2-O3)*. Logic is a stateless Deployment with HPA; its `minReplicas` must handle the **20k logins/s peak without scaling**, as measured by the load test. HPA scales above that; the SLO does not depend on it *(R2-O7)*. |
| Layer rule | The gateway never touches Redis. Only Logic touches Redis. |
| Dropped | Per-user login rate limit, distributed lock, reverse mapping, session version, per-IP limiting (each justified below). |

## Architecture

```
Android/iOS ─TCP──┐
                  ├─ cloud NLB ─► gateway StatefulSet (gnet loops · Noise · frames · heartbeat · admission)
Web ─wss (TLS)────┘                 │ gRPC Login/Logout/Reconcile (batched),     ▲ gRPC Kick (pod DNS)
                                    │      ReconcileEnd                          │
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
  - `reconnect_reason` *(R1-O7)*: enum — first connect, RST/FIN, Pong timeout, Busy retry, Reconnect from drain, …. A retry after the server closed the connection before the handshake completed is reported as `Busy retry` *(R3-O5)*;
  - `offline_ms` *(R1-O7)*: ms since the client detected the loss, on the client's monotonic clock (a duration, so phone and server clocks need not agree).
- Login errors (in `Error`):

  | Code | Meaning | Client action |
  |---|---|---|
  | `STALE` | A newer connection from this device already holds the slot. Carries the stored `conn_seq` *(R1-O5)*. | Set counter to `max(local, stored) + 1`, persist, retry after backoff. |
  | `SUPERSEDED` | Another device on the same slot logged in later *(R1-O4)*. | Retryable; a retry is a new, later login and wins normally. |
  | `EXPIRED` | The login reached Redis more than `login_deadline` after the gateway received it *(R1-O4)*. | Retry. |
  | `REVOKED` | Token issued before the user's revocation marker *(R1-O10)*. | Drop the token, show the login screen. |
- Kick reasons. **None of them triggers an automatic reconnect:**
  - `REPLACED`: same device, newer connection; not shown to the user. The client goes idle and reconnects only on a user action or when the app returns to the foreground. The SDK logs it as an anomaly, since a live client only sees it if a second instance shares its device_id *(R1-O6)*.
  - `KICKED_BY_OTHER_DEVICE`: shown to the user.
  - `REVOKED` *(R1-O10)*: the client drops its token and shows the login screen.
- **device_id** *(R1-O6)* identifies one client instance that holds a connection. The SDK must make sure at most one connection per device_id is in use at a time. Mobile and desktop: one per install, plus a process-level guard (e.g. a single connection service on Android).

### Client SDK rules

- **conn_seq persistence** *(R1-O5)*: persist `conn_seq` **before** each connection attempt, in the same place as device_id with the same lifetime (e.g. both in the iOS Keychain), so clearing one clears the other.
- **Reconnect** *(R1-O7, R3-O5)*:
  - After losing an **established** connection unexpectedly, wait a random **0–20s** before the first attempt (leaving 10s for handshake, login and one Busy retry), then exponential backoff (cap 5m). Obey `retry_after`.
  - **A TCP connection that the server closes before the Noise handshake completes counts as a `Busy` retry: wait 1–3s with jitter.** This covers `at_capacity` and `handshake_rate` rejects, which close the socket before any framing, so no `Busy{retry_after}` can be sent. After **5 such closes in a row**, fall back to exponential backoff (cap 5m). The count resets on a successful login.
  - Each attempt uses a new source port, so the NLB flow hash can pick another gateway *(R2-O2)*.
- **Dead-server detection** *(R1-O7)*: if no frame arrives within **10s** of a Ping, close and reconnect.
- **Web** (designed now, built later) *(R1-O6)*: one connection per browser profile. Tabs elect a leader through `SharedWorker`, falling back to Web Locks plus `BroadcastChannel`. The leader holds the wss connection and relays messages to the other tabs; when the leader tab closes, another tab takes over. device_id and conn_seq live in localStorage.

## Gateway

- gnet **≥ v2.8.0** (needed for `Conn.EventLoop()`). No goroutine per connection. `OnTraffic` never blocks; RPCs go through a bounded worker pool.
- Connection states: `HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`.
- **Identity:** `gateway_id` = pod name; `epoch` = random uint64 chosen at boot; `conn_id = (worker_id << 48) | loopState.counter++`.
  - Checked against gnet's `dev` source: `OnOpen` always runs on the owning loop's goroutine (both accept paths). The loop index is unexported, but `Conn.EventLoop()` gives the loop's identity.
  - `worker_id` is assigned lazily the first time `OnOpen` sees a loop (an atomic that runs about NumCPU times), cached in a `sync.Map` from EventLoop to loopState.
  - The plain `counter++` is safe only because just `OnOpen` touches it. The counter is unexported, and a `-race` test enforces the rule.
- **Per-loop connection tables with no locks:** `map[conn_id]*conn`, touched only by the owning loop. A Kick from gRPC reads `conn_id >> 48`, takes that loop's handle and calls `loop.Execute(...)` to look up the connection, check the epoch and state, and act. This also meets gnet's rule that Close runs on the owning loop.
  - **Tracked connections** *(R3-O1)*: a connection stays in its loop's table from accept until it no longer needs a Logout — i.e. it closed without ever sending a Login, or its fenced Logout was acknowledged or dropped from the retry queue. A `CLOSED` entry waiting for its Logout fence is still tracked. This tracked set is what `ReconcileEnd` reports.
- **Kick handling** *(R1-O8)*:
  - `ONLINE`: send the Kick frame and close.
  - `AUTH_PENDING` (Login in flight): set a kick-pending mark with the reason. When the Login reply arrives, send the Kick frame and close instead of moving to `ONLINE`.
  - `HANDSHAKE`, `CLOSED`, unknown conn_id, or epoch mismatch: ignore.
- **Heartbeat:** **150s by default, set by the server in `LoginResp`**. Any frame resets the timer. A connection is dead after 2 × interval + 10s (= 310s), and the gateway then sends Logout (subject to the fence). A timing wheel drives the timers.
- **Timeouts** *(R1-O11)*: `HANDSHAKE` 5s (accept → handshake complete); `AUTH_PENDING` 5s (handshake complete → LoginReq received). On expiry the connection closes silently. Both run on the timing wheel.
- **Admission control** *(R1-O11, R2-O2)*:
  - **Connection cap** *(R2-O2, R3 clarification)*: a hard per-gateway cap on open **sockets**, counted from accept to close, in every state (configurable, default **250k**; it must sit between the 200k operating point and the 600k ceiling). Closed entries kept in the table only for a pending Logout do not count. At the cap the gateway closes new connections at accept, before any crypto, and counts an `at_capacity` reject. It is one counter and one check at accept. Skew below the cap is harmless; active rebalancing is out of scope (sub-project 5).
  - **Noise handshake limit (per gateway, global; never per IP):** a concurrency limit on handshake computations running at once (a semaphore sized to the cores dedicated to handshakes) plus a gateway-wide token bucket on handshakes/s, sized from the measured NK cost (~100µs) so handshakes can't starve established connections. The bucket must allow at least the 20k/s storm peak with margin; in a legitimate storm it shapes load rather than rejecting it. When either is exhausted the gateway does not start the handshake: it closes the connection and counts a `handshake_rate` reject. The client treats this close as a `Busy` retry (see *Client SDK rules*) *(R3-O5)*.
  - **Login-queue cap:** cap the Logins waiting for or inside an RPC to Logic (this reflects Logic backpressure). At the cap, reply `Busy{retry_after}` and close.
  - **Memory-only pending cap:** a large cap on total `HANDSHAKE` + `AUTH_PENDING` connections, only to protect gateway memory. It must not trigger in a normal storm.
  - `Busy{retry_after}` uses a short value, **1–3s with jitter**, so rejected clients still fit within the SLO *(R1-O7)*.
  - One login attempt per connection (a failure closes it).
  - Queued Logins older than `login_deadline` (measured from `recv_ms`) are dropped before being sent; they never reach Logic, so they need no Logout.
- **recv_ms** *(R1-O4)*: the gateway stamps `recv_ms` (wall clock, NTP-synced) when it receives the Login frame and passes it in the Login RPC.
- **Login batching:** up to 200 logins or 5ms per RPC. The Login RPC timeout is at least `login_deadline + 1s` (= 6s by default), measured from `recv_ms` *(R1-O9)*.
- **Logout fence** *(R1-O9, R2-O1)* — gateway invariant: the gateway sends Logout for a connection only when **both** of the following hold.
  - **Login condition** — either:
    - (a) it has a **definitive Login reply** (success or rejection), at which point the Lua write has happened or never will; or
    - (b) `recv_ms + login_deadline + 1s` has passed (the 1s absorbs gateway↔Redis clock skew). This applies to **any** Login without a definitive reply: timeout, RPC error, a gRPC connection reset by `MaxConnectionAge`, or a Logic crash.
  - **Reconcile condition** *(R2-O1)*:
    - (c) no Reconcile chunk containing the connection is unresolved. A chunk is resolved when it has a definitive reply, or when `sent_ms + login_deadline + 1s` has passed (timeout, RPC error, Logic crash), since the Reconcile compare-and-set Lua refuses to write after `sent_ms + login_deadline`. A `refused` entry result is a definitive reply *(R3-O6)*. Implemented as a per-connection `in_reconcile_chunk` mark, set when the chunk is built and cleared when the chunk is resolved.

  A connection that closes before the fence holds keeps a pending Logout that fires when it does. Logouts go through a bounded retry queue (60s); dropping one is safe because the next Reconcile pass repairs it *(R1-O3, R3-O1)*.
- **Lease** *(R1-O2, R3-O3)*: call `GatewayLease` every 10s on a **dedicated gRPC connection**, never batched with or queued behind Login/Logout/Reconcile. The reply says whether the lease was found **live** or **missing**: it is `missing` whenever the renewal found the lease key absent, whatever the state of the `gwepochs` member. The renewal itself re-registers the epoch (lease key and `gwepochs` score). On `missing`, the gateway starts an immediate triggered Reconcile pass. The first lease call after boot always finds the key absent (a new epoch); the gateway ignores `missing` on that call, and neither counts nor acts on it *(R3-OP5)*. A lease that expired but was never swept costs one unneeded triggered pass, which is rare because of the TTL derivation *(R2-O5)*.
- **Reconcile** *(R1-O3, R1-O8, R1-O10, R2-O1, R2-O8, R3-O1, R3-O6)*: the gateway sends its ONLINE set — `(uid, appid:platform, conn_id, device_id, conn_seq, login_ms, tok_iat)` — in rate-limited chunks, then ends the pass with `ReconcileEnd`.
  - **Live chunks** *(R2-O1)*: each chunk is read from the per-loop connection tables (via `loop.Execute`) at the moment it is sent, not from a snapshot frozen at the start of the pass. Each chunk carries `sent_ms` (gateway wall clock when sent). Building a chunk sets `in_reconcile_chunk` on each connection in it (see the Logout fence).
  - **Per-entry results** *(R3-O6)*: Logic returns `written`, `unchanged` (compare-and-set lost to a concurrent change) *(R3-OP7)*, `refused` (deadline), or `not_owner` (with reason) for each entry. All four are definitive replies for fence (c). For each `not_owner` entry the gateway sends the Kick frame with the given reason and closes the connection. A `refused` entry's conn_id goes back on the current pass's queue and is rebuilt from the live tables when its new chunk is sent; it is skipped if the connection is no longer ONLINE. Each entry is re-queued at most 3 times per pass, so the pass always ends; after that it waits for the next pass.
  - **Pass end** *(R3-O1)*: a pass completes when every chunk, including re-queued entries, has been sent and resolved. The gateway then collects every conn_id it still **tracks** (see *Per-loop connection tables*), in any state, including closed connections whose fenced Logout has not been sent yet, stamps `as_of_ms` (gateway wall clock, the same clock as `recv_ms`) and sends `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`. If `ReconcileEnd` fails, it is retried with the same payload through the bounded retry queue used for Logouts (60s), then dropped; the next pass covers what it missed *(R3-OP4)*. Retrying late is safe: a conn_id absent from the list had already passed its Logout fence at `as_of_ms`, and conn_ids are never reused, so no later write for it can appear. A dropped `ReconcileEnd` stretches the ghost bound to two cycles for that pass. Size: 8 bytes × 250k (the cap) ≈ 2MB, under gRPC's 4MB default; at the 600k benchmark the list is split or the limit raised.
  - **Two paces** *(R2-O8)*, both configurable:
    - **periodic**: every ~10 min with random jitter per gateway, spread over the cycle (~330 entries/s at 200k);
    - **triggered by a `missing` lease reply**: ~5k entries/s per gateway (200k in ~40s; ~30k Lua calls/s cluster-wide if all 6 gateways run one at once).
  - **One pass at a time** *(R3 clarification)*: at most one pass runs per gateway. A `missing` reply during a periodic pass cancels it: a cancelled pass builds and sends no further chunks and sends no `ReconcileEnd`. The triggered pass starts once the cancelled pass's in-flight chunks are resolved (≤ `login_deadline + 1s`), so a connection is never in two unresolved chunks.
- **Graceful drain:** on SIGTERM, go NotReady and stop accepting, send `Reconnect{jitter}` in batches over about 60s, close the rest, then call `EpochEnded`. `terminationGracePeriodSeconds` must be longer than the drain window. Kicks still reach a draining gateway (`publishNotReadyAddresses`).

## Logic

- Stateless. Every node serves every RPC and can kick any gateway. Scales out horizontally (HPA on CPU); `minReplicas` covers the storm peak on its own *(R2-O7)*.
- **RPCs** *(R1-O12, R3-O1)*: `Login` (batch), `Logout` (batch), `GatewayLease` (own channel and **separate worker pool**, so Login load can't starve it *(R1-O2)*), `EpochEnded`, `Reconcile` (batch or streaming), `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`, `Revoke`.
- **Login:** verify the JWT locally (JWKS by `kid`), then run the Lua login script (EVALSHA, pipelined) with `recv_ms` and the token's `iat`, then do the follow-ups: SADD to the epoch index; if the result needs a kick, send it with the hardened Kick below.
- **GatewayLease** *(R3-O3)*: renew with `SET gwlease:gw:{epoch} 1 PX <TTL> GET` and update the `gwepochs` score. A nil previous value means the key was absent: reply `missing`; otherwise `live`.
- **Hardened Kick** *(R1-O8, R2-O6)*:
  - The **first attempt is always sent**, even when the target's lease looks expired (an epoch mismatch on the gateway makes a stray Kick harmless).
  - **Retries** (up to 3 over ~30s, on transport errors) happen only while the target `gw|epoch` lease is live. Logic caches lease state for ~1s; there are only a handful of epochs.
  - Kick RPCs are **fail-fast** (no `WaitForReady`).
  - On give-up, increment a metric labelled `target_lease=live|dead`. Reconcile is the backstop.
- **JWKS** *(R1-O10, R2-O7)*:
  - At startup, load the Ed25519 key set from the account service. If that fails, load it from a mounted ConfigMap (`jwks-fallback`) and keep retrying the account service on the normal refresh schedule.
  - Logic reports **Ready only once it holds a key set** from either source.
  - Refresh every 5 min. During a rotation every published key is accepted. If a refresh fails, keep the last good set and raise an alert. Alert while a pod runs on the fallback set.
  - A JWT with an unknown `kid` triggers at most one rate-limited refresh before it is rejected.
  - **Key rotation runbook:** a new signing key is published to the `jwks-fallback` ConfigMap before it signs any token; a key withdrawn because it was compromised is also removed from the ConfigMap. (A stale ConfigMap only matters while the account service is unreachable.)
- **Revoke** *(R1-O10, R2-O4, R3-OP2)*: `Revoke(uid, scope = all | slot, reason)`, called by the account service on a ban, a password change, "log out all devices" (scope=all) or "log this device out" (scope=slot). Both scopes work the same way:
  1. Write the marker field — `*` for scope=all, `appid:platform` for scope=slot — with a Lua script as `revoked_before_ms = max(stored, new)` (idempotent), then `WAIT 1 <timeout>`. If no replica confirms, return a retryable error; the account service retries `Revoke` until it succeeds. **The marker is written synchronously: it must be confirmed before step 2 and before `Revoke` returns success**, so a lost Kick can't let Reconcile resurrect the session.
  2. After the confirmed write, delete the affected `sess:{uid}` field(s) and kick each affected connection with reason `REVOKED` using the hardened Kick.

  A client's own `Logout` command writes no marker: the gateway closes that connection, so it is no longer ONLINE and Reconcile can't restore it.
- **Effective revocation marker** for a slot *(R3-OP2)*: `max(rev[*], rev[appid:platform])` from `rev:{uid}`; a missing field counts as 0. The login Lua and Reconcile rule 1 both use it.
- **Reconcile handling** *(R1-O3, R1-O8, R1-O10, R2-O1, R2-O4, R3-O4, R3-O6)*, per chunk entry, in this order:
  1. `tok_iat <` the effective revocation marker for the entry's slot *(R3-OP2)* → `not_owner` with reason `REVOKED`; write nothing.
  2. Field holds this exact `gw|epoch|conn_id` → refresh the `sess:{uid}` TTL; ensure the uid is in `gwconns`. Result `written`.
  3. Field holds a **different** session whose `gw|epoch` has a **live** lease → compare the two with the login table's ordering, including its regression window *(R2-O4, R3-O4)*:
     - same device_id: if the two `login_ms` are within `login_deadline` of each other, the higher `conn_seq` wins; otherwise the later `login_ms` wins (the regression row);
     - different device_id: the later `login_ms` wins;
     - on a tie in either case, the **stored** session wins.

     Outcomes:
     - Chunk entry newer (only possible after a lost write, e.g. a Redis failover) → compare-and-set write it, refresh the TTL, ensure the uid is in `gwconns`, and, **only if the compare-and-set wrote**, kick the stored session with `REPLACED` (same device_id) or `KICKED_BY_OTHER_DEVICE` (different device_id) using the hardened Kick *(R3-O6)*.
     - Stored session newer → `not_owner` with `REPLACED` (same device_id) or `KICKED_BY_OTHER_DEVICE` (different device_id).
  4. Field is empty, or its `gw|epoch` has no live lease → compare-and-set write the entry (restore), refresh the TTL, ensure the uid is in `gwconns`.

  Because the lease key lives in a different slot from `sess:{uid}`, Logic checks lease liveness outside Lua and passes the observed field value to a **compare-and-set Lua script**, which writes only if the field is still what Logic observed **and** Redis `TIME − sent_ms ≤ login_deadline` *(R2-O1)*.
  - **Deadline scope** *(R3-O6)*: the deadline refuses only writes that create or replace a field (rules 3 and 4). Late TTL refreshes and `gwconns` SADDs are harmless and are not refused. A refused write returns `refused`; the gateway re-queues it in the current pass.
  - A compare-and-set that fails because the field changed is a definitive result with no write, reported as `unchanged` (not `refused`, so it is not re-queued), and no kick is sent; the next pass handles the entry.

  **Delete phase** *(R3-O1)*: the Logic node that receives `ReconcileEnd` SSCANs `gwconns:gw:{epoch}`. For each uid it reads `sess:{uid}` and compare-and-deletes each field that points at this `gw|epoch` whose `conn_id ∉ tracked_conn_ids` and whose `login_ms < as_of_ms`. It also removes uids with no remaining field from `gwconns`. Why this is safe: a conn_id leaves the tracked set only after its Logout fence has held, so no Login or Reconcile write for it can still land; and a connection whose Login was received at or after `as_of_ms` is excluded by the `login_ms` check.

  Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway, ~2k/s cluster-wide at 6 gateways; a triggered pass is ~5k/s per gateway *(R2-O8)*.

## Redis schema (Cluster; per-user keys hash-tagged `{uid}`)

| Key | Type | Value / TTL | Purpose |
|---|---|---|---|
| `sess:{uid}` | HASH | field `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`; `login_ms` is the gateway's `recv_ms` *(R1-O4)*; key TTL 30d, refreshed on login and by Reconcile | online mark (source of truth in Redis) |
| `rev:{uid}` | HASH | field `*` → `revoked_before_ms` for scope=all; field `appid:platform` → `revoked_before_ms` for that slot *(R3-OP2)*. Each field written as `max(stored, new)` and confirmed with `WAIT` *(R2-O4)*; key TTL = max token lifetime + margin *(R1-O10)*, refreshed on every marker write | revocation markers (user-wide and per slot) |
| `gwlease:gw:{epoch}` | STRING | `PX <lease TTL>`, renewed every 10s with `SET ... PX <TTL> GET` (a nil previous value means `missing`) *(R3-O3)*. Lease TTL is configurable (default 30s) and must satisfy **TTL > measured shard failover time + one renewal interval** *(R2-O5)* | gateway instance is alive |
| `gwepochs` | ZSET | member `gw:epoch`, score = last renewal in ms | registry of epochs, used by the sweeper |
| `gwconns:gw:{epoch}` | SET | uids; pruned by the Reconcile delete phase. One key per epoch, so all of a gateway's `SADD`s land on one shard; acceptable at the storm rate (~10k/s) *(R2-O9)* | cleanup hint (not atomic with sess, not needed for correctness) |
| `sweep:gw:{epoch}` | STRING | `SET NX PX 300000` | sweeper claim |

**A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`.

> The validity rule is defined here, but an efficient read path for consumers is deferred to sub-projects 2/3. They must **not** implement it as `HGETALL` + `EXISTS` per epoch *(R1-O12)*.

**Replication** *(R2-O4)*: Redis Cluster replicates asynchronously; a failover can lose ~1s of acknowledged writes. `sess:` and `gwconns` losses are repaired by Reconcile (rule 3 picks the right winner, rule 4 restores); lease losses are covered by the next renewal. `rev:{uid}` is the exception: nothing would repair its loss, so Revoke confirms every marker write, for both scopes, with `WAIT`.

### Login Lua (single slot, atomic)

Inputs: the new entry, `recv_ms`, token `iat`, `login_deadline` (default 5s). Steps:

1. Read `rev:{uid}` fields `*` and `appid:platform`. If `iat <` their max (the effective marker) → `REVOKED`; no write *(R1-O10, R3-OP2)*.
2. Read Redis `TIME`. If `now − recv_ms > login_deadline` → `EXPIRED`; no write *(R1-O4)*.
3. Compare the stored field with the new login:

| Stored vs new | Result |
|---|---|
| empty | write; no kick |
| same device_id, new conn_seq > stored | overwrite; kick the old connection with `REPLACED` |
| same device_id, new conn_seq ≤ stored, `recv_ms ≤ stored login_ms + login_deadline` | reject as `STALE` (return stored conn_seq); close; no write *(R1-O5)* |
| same device_id, new conn_seq ≤ stored, `recv_ms > stored login_ms + login_deadline` | counter regression: overwrite; kick the old connection with `REPLACED`; increment `conn_seq_regressed` *(R1-O5)* |
| different device_id, `recv_ms ≥ stored login_ms` | overwrite; kick the old one with `KICKED_BY_OTHER_DEVICE` |
| different device_id, `recv_ms < stored login_ms` | reject as `SUPERSEDED`; no write, no kick; the gateway closes with a retryable error *(R1-O4)* |

Lease liveness of the stored entry does not change the write decision (Lua can't read the lease key, which is in a different slot). The first Kick attempt is always sent (R1-O8, R2-O6).

Reconcile rule 3 uses the same ordering *(R3-O4)*: a higher `conn_seq` more than `login_deadline` earlier than the stored login cannot be produced by this script (the `EXPIRED` check forbids it), so the window-aware rule agrees with this table on every state it can reach.

Assumption *(R1-O4)*: clock skew between gateways (NTP, a few ms) is far below the time a person takes to switch devices.

### Logout Lua

`HDEL` the field only if it still matches `gw|epoch|conn_id`. Because conn_id is never reused, this handles the delayed-disconnect race, including on the same gateway. Ordering against the connection's own Login and against any Reconcile restore of it is guaranteed by the gateway's Logout fence.

### Sweeper (goroutine in every Logic node)

Find `gwepochs` members whose last renewal is more than **2 × lease TTL** old (60s by default) *(R2-O5)*, or that sent `EpochEnded`. Claim each with `sweep:` SET NX and **record the epoch's `gwepochs` score at claim time** *(R3-O3)*. Run `SSCAN gwconns`, and for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch. Rate-limited to about 10k users/s.

- **Abort on renewal** *(R3-O3)*: between rate-limit batches, re-read the epoch's `gwepochs` score. If it is newer than the claimed score, the epoch has renewed: stop at once, without `UNLINK`ing `gwconns` and without removing the ZSET member, `DEL` the `sweep:gw:{epoch}` claim so it can't block a later legitimate sweep *(R3-OP6)*, and count `sweep_aborted`. The gateway's renewal already reported `missing` (the lease key was absent), so its triggered Reconcile restores whatever was deleted.
- On completion, `UNLINK` the set (freed in a background thread, so a 200k-member delete does not stall the shard) and remove the ZSET member *(R2-O9)*.

**Mass-stale guard** *(R1-O2, R2-O3, R2-O5, R3-O2)*: sweep a stale epoch only if
- at least one **other** epoch renewed within **1.5 × the renewal interval (15s)**, **and this has held continuously for one lease TTL (30s)**. Each sweeper keeps a `renewing_since` timestamp in memory: it is reset when the condition is false and set when it first becomes true. A sweeper restart starts it over, which is conservative. The 1.5× window keeps a lone surviving gateway's 10s rhythm (plus RPC latency) from resetting the clock; and
- the sweeper's own Redis access has been healthy for one full **lease TTL** (30s by default).

Epochs that sent `EpochEnded` are exempt and always swept. While the guard holds back a sweep, it raises an alert and metric and re-checks next cycle. The rule asks directly whether the system is demonstrably renewing leases, so it tells a systemic outage (no one renews) from a real multi-gateway death (others still renew). It **releases one lease TTL after renewals resume, by which time every healthy gateway has renewed** (each has had three renewal chances) *(R3-O2)*. Skipping or delaying a sweep never affects correctness — entries of a dead epoch are already invalid by the validity rule; the sweeper only cleans up.

Safety net: the 30-day TTL on `sess:{uid}`, refreshed by login and Reconcile *(R1-O3)*.

### Why the dropped concepts are not needed

- **Login rate limit (per user):** blocks real fast reconnects (Wi-Fi to 4G), and a rejected login costs Redis the same as an accepted one. Abuse protection moves to the gateway at no Redis cost.
- **Per-IP limiting** *(R1-O11)*: mobile IPs are mostly shared through carrier-grade NAT, so per-IP limits hurt legitimate users, especially in a reconnect storm. The gateway's handshake limit is global. Any future abuse limiting keys on device_id or user_id (possible only after LoginReq is decrypted); not designed in sub-project 1. Without per-IP limits, sub-project 1 doesn't need the real client IP, so proxy protocol is deferred to the web phase.
- **Distributed lock:** every change is a single atomic Lua script on a single slot.
- **Reverse mapping:** the gateway knows which user owns each connection; cleanup uses `gwconns`, and Reconcile keeps Redis aligned with the gateway.
- **Session version:** the `gw|epoch|conn_id` triple plus `device_id`/`conn_seq`/`login_ms` covers it.

## Races handled

1. **Delayed disconnect** (old G17 logout after the new G42 login): Logout compare-and-delete doesn't match, so nothing happens.
2. **Delayed login from an abandoned connection, same device** (S1's login lands after S2's): older conn_seq within the deadline window → `STALE`, S2 unaffected. A login later than the deadline is `EXPIRED` anyway.
3. **Delayed login from another device on the same slot** *(R1-O4)*: its `recv_ms` is older than the stored `login_ms` → `SUPERSEDED`; the newer device is not kicked.
4. **Zombie gateway with the same pod name:** leases are keyed per epoch, so the instances don't conflict, and a Kick that reaches the wrong instance is ignored because the epoch doesn't match.
5. **Connection closes while its Login is in flight** *(R1-O9)*: the Logout fence holds the Logout until the definitive Login reply (fence a), so the Logout always lands after the write.
6. **Login without a definitive reply, but Redis was written** (timeout, RPC error, `MaxConnectionAge` reset, Logic crash) *(R1-O9)*: the Lua deadline means no write can happen after `recv_ms + login_deadline`; the gateway waits `recv_ms + login_deadline + 1s` (fence b) before sending Logout, so the Logout always lands after any possible write.
7. **Logout lost while the gateway is alive:** the stale entry is removed by the delete phase of the next pass to end (`ReconcileEnd`) *(R1-O3, R3-O1)*. The sweeper only handles dead epochs.
8. **Kick lost or arriving early** *(R1-O8)*: a Kick during `AUTH_PENDING` is held as kick-pending and applied when the Login reply arrives; a Kick lost after its retries is applied by the next Reconcile through `not_owner` (≤ ~10 min).
9. **Two devices kick each other back and forth:** the client must not reconnect automatically after `KICKED_BY_OTHER_DEVICE`.
10. **Two client instances share a device_id** *(R1-O6)*: they replace each other once, and because `REPLACED` doesn't reconnect, the loop stops.
11. **Device counter went backwards** (reinstall, restored backup) *(R1-O5)*: a low conn_seq outside the deadline window is accepted as a regression; inside it, `STALE` returns the stored counter so the client resyncs.
12. **Sessions swept while their gateway was healthy** *(R1-O2, R2-O3, R3-O2, R3-O3)*:
    - A systemic outage (long Redis outage, Logic saturation) stops every renewal, so the guard holds and nothing is swept.
    - An outage between the gateways and Logic, with Logic↔Redis healthy, also stops every renewal. When it ends, the guard waits one TTL of continuous renewals, so healthy gateways renew before any sweep.
    - If a single gateway's lease is wrongly starved while the others renew, it may be swept; its lease reply then reports `missing` and a triggered Reconcile restores its sessions within ~1 min.
    - If the starved gateway renews while its epoch is being swept, the reply still reports `missing` (the lease key was absent), the sweeper stops at its next batch, and the triggered Reconcile restores whatever was deleted. Entries deleted by the last in-flight batch after the restore are repaired by the next periodic pass.
13. **Two loops generating conn_ids at once:** each loop has its own counter, and the worker_id prefix keeps the IDs apart.
14. **Connection closes during a Reconcile pass** *(R2-O1)*: its Logout is held until the chunk containing it is acknowledged (fence c), or until the chunk's deadline has passed, so the Logout always lands after any restore. A connection that closed before its chunk was built is simply not in the pass.
15. **Write lost in a Redis failover, older session restored** *(R2-O4, R3-O4)*: if the field rolls back to an older session while the newer connection is live, Reconcile rule 3 compares by the login table's ordering, writes the newer entry back and kicks the older connection. The ordering includes the regression window, so a session that won through a counter regression keeps winning after a rollback. Ties go to the stored session, so two live sessions never overwrite and close each other.
16. **Login in flight, or Logout dropped, around a Reconcile pass** *(R3-O1)*: a connection whose Login is in flight is tracked, so it is in `tracked_conn_ids` and the delete phase leaves its field alone; a Login received after `as_of_ms` fails the `login_ms < as_of_ms` check. A connection whose Logout was dropped has left the tracked set, so its ghost is deleted at the end of the same pass — whether the Logout was dropped before or after its chunk was sent.
17. **Slot-scoped logout whose Kick is lost** *(R3-OP2)*: the connection stays ONLINE on its gateway and its `sess:` field is gone. The confirmed per-slot marker makes the next Reconcile pass return the entry as `not_owner` (`REVOKED`, rule 1) instead of restoring it (rule 4), so the device is closed within one cycle. The user's other slots are unaffected.

Note: when the same device reconnects, `conn_seq` also replaces any ghost entry. Reconcile covers devices that never return.

## Consistency contract *(R2-O8)*

Bounds consumers (sub-projects 2/3) may rely on when sizing caches and fallbacks:

| Divergence consumers may see | Cause | Bound | Repaired by |
|---|---|---|---|
| Ghost: valid entry, but the connection is gone | Logout dropped | ≤ 1 Reconcile cycle (~10 min + jitter): a Logout dropped at any point before a pass ends is repaired at the end of that pass *(R3-O1)* | Reconcile delete phase (`ReconcileEnd`) |
| Old owner still connected after being replaced or revoked (either scope) | Kick lost | ≤ 1 cycle | Reconcile `not_owner` / rule 1 *(R3-OP2)* |
| Entry of a dead gateway | Gateway death | invalid after the lease TTL (30s); deleted after 2 × TTL, plus up to one TTL of guard wait *(R3-O2)*, plus sweep time | validity rule, then sweeper |
| Live connection missing from Redis | Write lost in failover, or wrongly swept | ≤ 1 cycle; ≤ ~1 min after a `missing` lease reply | Reconcile restore |
| Healthy session reads invalid | Logic or lease shard down longer than the TTL | the length of the outage | lease renewal resumes *(R2-O5)* |

## Storms, drain, failures

- Client SDK: see *Client SDK rules* (0–20s jitter after losing an established connection, 1–3s for pre-handshake closes up to 5 in a row, exponential backoff, 10s Pong timeout, obey `retry_after`).
- Capacity: N−1 gateways must absorb one dead gateway's connections, sized at the 200k operating point *(R1-O1)*; the per-gateway connection cap bounds how unevenly they do it *(R2-O2)*.
- Several gateways lost at once (zone): degraded, best effort, no SLO *(R2-O3)*.
- Graceful drain: see *Gateway*.

| Failure | Behavior |
|---|---|
| Logic down | Login gets a retryable error and the client backs off. Logout goes into a bounded retry queue for 60s, then is dropped; dropped Logouts are repaired at the end of the next Reconcile pass. Down for longer than the lease TTL: all sessions read as invalid until renewals resume (consciously carried, see *Interface constraints*) *(R2-O5)*. |
| Logic saturated *(R1-O2)* | Lease RPCs keep their own connection and worker pool, so renewals continue. The gateway's Login-queue cap replies `Busy` (1–3s). Reconcile entries refused by the deadline are re-queued within the pass (up to 3 times) *(R3-O6)*. |
| Gateways cut off from Logic, Logic↔Redis healthy *(R3-O2)* | No epoch renews, so the guard holds. When connectivity returns, the guard releases only after one TTL of continuous renewals, so every healthy gateway has renewed before any sweep. |
| Logic cold start with the account service down *(R2-O7)* | Pods load `jwks-fallback`, become Ready and accept tokens; an alert fires while they run on the fallback. |
| Redis down | New logins fail; existing connections stay up. Lease SET resumes when Redis returns, and entries become valid again. No epoch renews during the outage, so the mass-stale guard holds back sweeping and releases one TTL after renewals resume; any sessions swept anyway are restored by Reconcile when the lease reply reports `missing`. |
| Redis shard failover *(R2-O4, R2-O5)* | Up to ~1s of acknowledged writes may be lost: `sess:`/`gwconns` losses are repaired by Reconcile, `rev:` is protected by `WAIT`. The lease survives because its TTL exceeds the measured failover time plus one renewal interval. |
| Gateway process crash / pod kill | Clients see RST/FIN and reconnect within the 0–20s jitter. The lease expires within the TTL (30s); the sweeper cleans up after 2 × TTL (60s), plus up to one TTL if its guard clock has just started. One Kick attempt per relogin reaches the dead epoch, with no retries *(R2-O6)*. |
| Node loss / silent partition | Clients detect it by Pong timeout (≤ one heartbeat interval + 10s) and then reconnect. Server side as for a crash. |
| Several gateways lost at once (zone) *(R2-O3)* | Best effort: survivors fill to the connection cap; excess clients are closed at accept and retry after 1–3s (up to 5 times, then backoff) until the lost pods are rescheduled *(R3-O5)*. Stale epochs are swept because the survivors keep renewing. |
| Kick can't reach the gateway *(R2-O6)* | Retried (up to 3 times over ~30s) while the target lease is live; otherwise one attempt. Then the Reconcile backstop closes the connection within one cycle. |
| Handshake flood | The global handshake concurrency limit and token bucket protect established connections; excess handshakes are closed and counted. |
| Slow clients | `HANDSHAKE` / `AUTH_PENDING` 5s timeouts free their slots. |
| Bad frame / decryption failure / frame too large | Close immediately. |

## Interface constraints for later sub-projects

- **Ownership rule** *(R1-O8)*: a connection may act as the user (send messages, perform user operations) only while its `gw|epoch|conn_id` owns its slot in `sess:{uid}`. Sub-project 2 must enforce this on the send path (a cached ownership check is acceptable). Sub-project 1 only documents it and provides the data it relies on (session entries plus Kick/Reconcile delivery).
- **Session-validity read path** *(R1-O12)*: deferred to sub-projects 2/3; must not be `HGETALL` + `EXISTS` per epoch.
- **Validity depends on Logic and the lease shard** *(R2-O5)*: if Logic, or the Redis shard holding a gateway's lease key, is unavailable for longer than the lease TTL, that gateway's sessions read as invalid although their connections are healthy. Sub-projects 2/3 must not trigger mass offline pushes on validity alone; the mitigation is theirs to design.
- **Consistency bounds**: see *Consistency contract* *(R2-O8)*.
- **Lazy repair** in sub-project 3 (a push that gets "unknown conn" leads Logic to compare-and-delete) is optional; correctness no longer depends on it *(R1-O3)*.

## Observability

Prometheus metrics:
- connections by state for each gateway; tracked-but-closed entries awaiting Logout; handshakes/s; **connection skew** (max / mean ONLINE per gateway) *(R2-O2)*;
- logins by result: `ok`, `stale`, `replaced`, `kicked`, `superseded`, `expired`, `revoked`, `busy`, `error`; plus `conn_seq_regressed`;
- end-to-end login latency p50/p99; gRPC and Redis script latency;
- **relogin latency** histogram: `offline_ms` + (login OK − `recv_ms`), labelled by `reconnect_reason` *(R1-O7)*;
- **dead-epoch cross-check** *(R1-O7)*: when a relogin finds the old entry pointing at a dead epoch, record `login_ms − that epoch's last lease renewal` (time from gateway death to relogin, ±10s, no client data needed);
- lease renewal failures; lease replies with `missing`;
- admission rejects by reason: `at_capacity` *(R2-O2)*, `handshake_rate`, `login_queue`, `pending_mem`, `handshake_timeout`, `auth_timeout`;
- Kick attempts, retries and give-ups, give-ups labelled `target_lease=live|dead` (alerts use `live` only) *(R2-O6)*; revocation kicks; Revoke `WAIT` failures *(R2-O4)*;
- Reconcile: entries refreshed, restored, `unchanged` *(R3-OP7)*, `refused` and re-queued, re-queue limit reached *(R3-O6)*, `not_owner` by reason, rule-3 overrides (chunk entry newer) *(R2-O4)*; `ReconcileEnd` deletes, `tracked_conn_ids` size, retries and drops *(R3-O1, R3-OP4)*; passes cancelled; cycle duration, by pace *(R2-O8)*;
- sweeper progress; `sweep_aborted` *(R3-O3)*; **mass-stale guard holds** (also an alert), including time held by the `renewing_since` wait *(R3-O2)*;
- JWKS refresh failures and unknown-`kid` refreshes; pods running on `jwks-fallback` (also an alert) *(R2-O7)*;
- **reconnect-after-idle rate** (shows NAT timeouts shorter than 150s); Go GC pause and heap.

Logs: `slog` lines with `uid` and `conn_id`; a request ID passed in gRPC metadata.

## Verification

- **Unit tests:**
  - frame codec (partial frames, several frames at once, fuzzing), Noise handshake against a test client;
  - the connection state machine: kick-pending in `AUTH_PENDING`, both timeouts, the Logout fence with all three conditions, the connection cap at accept (counting sockets, not entries awaiting Logout), and tracked-set membership until the Logout is acknowledged or dropped;
  - Reconcile pass control: `refused` entries are re-queued at most 3 times and skipped if no longer ONLINE; a cancelled pass sends no further chunks and no `ReconcileEnd` *(R3-O6, R3-O1)*;
  - conn_id generation, and the **gnet loop-ownership test under `-race`** (`OnOpen`, `OnClose` and the `Execute` callback all see the same loop for a given connection);
  - sweeper guard: `renewing_since` resets when no other epoch renewed within 15s and is not reset by a 10s rhythm with jitter *(R3-O2)*;
  - **SDK** *(R3-O5)*: pre-handshake closes retry after 1–3s up to 5 times in a row, then fall back to exponential backoff; the count resets on a successful login.
- **Lua tests** against a real Redis in Docker:
  - one table test for each row of the login table, including `SUPERSEDED`, `EXPIRED`, `STALE` (low conn_seq inside the window is rejected) and the regression row (low conn_seq outside the window is accepted);
  - `REVOKED` (token iat before the marker is rejected); the revocation marker write is `max(stored, new)` and idempotent; the effective marker is the max of the `*` and slot fields, and a slot marker blocks login on that slot only *(R3-OP2)*;
  - the logout compare-and-delete;
  - the Reconcile compare-and-set (including refusal after `sent_ms + login_deadline`, and a failed compare returning no write) and the `ReconcileEnd` delete (only fields of this epoch, conn_id not tracked, `login_ms < as_of_ms`) *(R3-O1)*;
  - Reconcile rule 3 after a simulated lost write: restore an older field by hand while the newer connection is live; Reconcile keeps the newer connection and kicks the older one *(R2-O4)*;
  - Reconcile rule 3 with a regressed counter *(R3-O4)*: a regressed-counter session (low conn_seq, login more than `login_deadline` after the stored one) has its write rolled back by hand to the older high-conn_seq entry; Reconcile keeps the regressed session and kicks the older one. Equal `login_ms` on two devices: the stored session wins and no kick loop occurs;
  - lease renewal with `SET ... GET`: absent key → `missing`, present key → `live` *(R3-O3)*;
  - the sweeper deleting only dead-epoch fields.
- **Race integration tests** (`deploy/compose`: Redis, Logic, **4 gateways** and a scripted client) *(R3-O7)*:
  - races 1–6, 10, 14 and 16 above; race 6 in both orders (Logout before Login and Login before Logout, with injected Logic delays): no ghost after the fence; a Login with no definitive reply caused by a Logic crash: the Logout waits for the fence;
  - race 14 *(R2-O1)*: a connection closes after its chunk is sent but before Logic processes it (injected Logic delay); no ghost remains after the Logout;
  - `ReconcileEnd` *(R3-O1)*: a Login in flight when a pass starts (injected Logic delay) is not deleted by that pass's delete phase; a Logout dropped after the connection's chunk was sent is repaired at the end of the same pass; a cancelled pass deletes nothing; a `ReconcileEnd` that fails once is retried and its deletes apply *(R3-OP4)*;
  - Reconcile refusal *(R3-O6)*: inject a Logic delay longer than `login_deadline` on one chunk of a triggered pass: its entries are refused, re-queued and restored within the same pass; a refused rule-3 write sends no kick;
  - gateway crash → lease expiry → sweep → entries gone;
  - kill 2 gateways at once: both epochs are swept while the others renew *(R2-O3)*;
  - kill 3 gateways while one keeps renewing at its 10s rhythm: the guard's `renewing_since` is not reset, and all three epochs are swept after the guard wait *(R3-O7, R3-O2)*;
  - Redis paused for 90s: nothing is swept; sessions are still present afterwards, or restored by Reconcile;
  - gateway→Logic blocked for 90s with Logic↔Redis healthy, then restored: nothing is swept *(R3-O2)*;
  - Logic restart with renewals resuming more than 30s after the sweeper starts: nothing is swept *(R3-O2)*;
  - after a forced sweep of a live gateway, its sessions are restored within ~1 min by the triggered pass *(R2-O8)*;
  - sweep abort *(R3-O3)*: start a sweep of a live gateway's epoch (lease key removed), let the gateway renew mid-sweep: the reply is `missing`, the sweeper aborts without `UNLINK`/`ZREM` and deletes its claim key *(R3-OP6)*, and the sessions are restored within ~1 min. The gateway's first lease call after boot reports `missing` and triggers no pass *(R3-OP5)*. A renewal after a plain lease expiry with no sweep is `missing` and triggers one pass;
  - Reconcile: a ghost entry is removed within one cycle; a missing entry is restored; a newer session on another gateway is never overridden; a dropped Logout is repaired within one cycle;
  - Kick: a dropped Kick to a live gateway is retried and then caught by the next Reconcile *(R2-O6)*; a Kick during `AUTH_PENDING` closes the connection when the Login reply returns; a draining gateway still receives Kicks;
  - Revoke closes connections on two slots; a Revoke whose Kick is dropped is caught by the next Reconcile; a Revoke whose `WAIT` fails returns an error, and the retry is idempotent *(R2-O4)*;
  - slot-scoped Revoke *(R3-OP2)*: with its Kick dropped, the next Reconcile closes the connection through `not_owner` (`REVOKED`) and does not restore the session; another slot of the same user is unaffected; a slot Revoke whose `WAIT` fails returns an error and sends no Kick, and the retry is idempotent;
  - key rotation: both `kid`s are accepted during the overlap;
  - JWKS *(R2-O7)*: Logic starts with the account service down, becomes Ready from the `jwks-fallback` ConfigMap and accepts tokens; a Logic pod with no key set from either source never becomes Ready.
- **Redis failover test** *(R2-O5)*: fail over the shard holding the lease keys and measure the time; the lease must survive it at the configured TTL.
- **Load tests** (`cmd/loadgen`, multiple source IPs; loadgen records detection time and login-success time per client and computes the SLO distribution directly):
  - (a) 600k connections on one gateway (ceiling), with GOMEMLIMIT/GOGC tuning;
  - (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of death; Kick attempts to it are about one per relogin, with no retries *(R2-O6)*;
  - (b2) iptables DROP on a 200k gateway (silent node loss): 99% logged in again within 30s of client detection;
  - (b-stress) kill a 600k gateway: measured, no SLO;
  - (c) 150s heartbeat CPU cost at 200k and at 600k;
  - storm with Logic held at `minReplicas` (no HPA scaling): the SLO is met *(R2-O7)*;
  - rolling restart of all gateways at 1M total: no gateway exceeds the cap, and clients rejected at the cap log in elsewhere within the SLO (using the 1–3s pre-handshake retry) *(R2-O2, R3-O5)*;
  - storm: the other gateways never miss a lease renewal;
  - handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket;
  - slow-client flood: slots are freed by the timeouts and legitimate clients get no `Busy`;
  - Logic slowed down: the Login-queue cap triggers `Busy` and clients retry within the SLO.

## Out of scope (later sub-projects)

Messaging (seq, dedup, MongoDB) · push pipeline (RocketMQ, APNs/FCM) · social feed · QR-code login for web · adaptive heartbeat on the client · account service (its events reach us only through `Revoke` and the JWKS) · send-path ownership enforcement (sub-project 2) · session-validity read path and its mitigation for leases that read invalid during outages (sub-projects 2/3) · active connection rebalancing across gateways (sub-project 5) · sizing for zone loss · proxy protocol and real client IP (web phase) · device_id/user_id-keyed abuse limiting.

## Open points

Rev3's open points 4 (what the connection cap counts), 5 (overlapping Reconcile passes) and 6 (which Reconcile writes the deadline refuses) were confirmed by the third review and are part of the body (*Admission control*, *Reconcile*, *Reconcile handling*). Rev4's open point 2 (slot-scoped Revoke) is resolved by the per-slot revocation marker, and its open points 4–7 are confirmed and folded into the body (*R3-OP2*, *R3-OP4*–*R3-OP7* in the rev3 dispositions).

Still carried, not yet addressed by a review decision:

1. **Reconcile vs. lease check atomicity.** The R1 dispositions say the Reconcile Lua writes "if the stored `gw|epoch` has no live lease", but the lease key is in a different Redis slot. This revision checks liveness in Logic and makes the Lua a compare-and-set on the observed field value (see *Reconcile handling*).
2. **Order of `REVOKED` vs `EXPIRED`** in the login Lua: revocation is checked first, as R1-O10 says "read `rev:{uid}` first". The outcome differs only in which error the client sees.
