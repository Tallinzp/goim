# Sub-project 1: Connection & Session Core — Design

## Context

We are building a Go IM backend for a WeChat-like social app (Android, iOS, web) targeting ~1M concurrent users. The user previously built a C++ IM handling 2.6M TCP connections (600k per gateway). Two earlier draft docs (`gpt_design.md`, `claude_design.md`, added in `4ce1fa6` and removed in `05cd570`) disagree on presence model and scope, and leave the "delayed disconnect deletes the new session" race unsolved.

The full system is decomposed into 5 sub-projects (1 connection/session core, 2 messaging, 3 push pipeline, 4 social feed, 5 scale/hardening). **This spec covers sub-project 1 only**: connect, encrypted handshake, login, heartbeat, kick, disconnect — correct under reconnect races, and meeting a storm-recovery SLO.

**Success criteria**
- One gateway holds 600k connections (measure CPU / memory / GC).
- When a full gateway (600k) dies, 99% of users are logged in again within 30s (20k logins/s sustained, 50k/s peak headroom).
- No stale event (late disconnect, late login, zombie gateway) can delete or override a newer session; covered by automated tests.

## Decisions (agreed)

| Topic | Decision |
|---|---|
| Presence | One session per **slot** = `userid + appid + platform` (`mobile` covers Android and iOS; plus `pad`, `web`, `pc`). New login on the same slot replaces the old one. Platform is validated against the token. |
| Client transport | Mobile: raw TCP via gnet. Web: WebSocket over wss (TLS terminated at the cloud LB), designed now behind a transport interface, built as a late slice. |
| Client encryption | Custom app-layer encryption using the **Noise Protocol Framework, NK pattern** (`flynn/noise`): server static key pinned in the app, with a key ID for rotation; ChaCha20-Poly1305 per frame. No resumption tickets (a handshake costs ~100µs, negligible in storms). Web relies on wss TLS and uses no Noise. |
| Internal RPC | gRPC both ways. Gateway→Logic: `dns:///` headless service + `round_robin`; Logic server sets `MaxConnectionAge` ≈5m so gateways pick up new pods. Logic→Gateway: Kick dialed by StatefulSet pod DNS (`gw-17.gateway.im.svc`), so no registry is needed. |
| Auth | Separate account service issues Ed25519-signed JWTs; Logic verifies them locally (no database call on login). `cmd/devtoken` issues tokens for tests. |
| Deployment | Kubernetes on cloud. Gateways run as a StatefulSet behind an L4 NLB; Logic is a stateless Deployment with HPA. |
| Layer rule | The gateway never touches Redis. Only Logic touches Redis. |
| Dropped | Per-user login rate limit, distributed lock, reverse mapping, session version (each justified below). |

## Architecture

```
Android/iOS ─TCP──┐
                  ├─ cloud NLB ─► gateway StatefulSet (gnet loops · Noise · frames · heartbeat · admission)
Web ─wss (TLS)────┘                 │ gRPC Login/Logout/Lease (batched)       ▲ gRPC Kick (pod DNS)
                                    ▼                                         │
                              logic Deployment (stateless, HPA) ──────────────┘
                                    │
                               Redis Cluster
```

Repo: one Go module containing `cmd/{gateway,logic,devtoken,loadgen}`, `internal/...`, `api/{client,internal}.proto`, `deploy/{compose,k8s}`.

## Client protocol

- Outer frame: `[u32 length][payload]`, max 64KB. The gateway buffers partial frames and splits frames that arrive together.
- Frame 1: Noise NK handshake (carries a key ID). After that, every payload is AEAD-sealed, with a nonce counter per direction.
- Plaintext: `Envelope{cmd, req_id, body}`. Commands: `Login`, `LoginResp` (includes the heartbeat interval), `Ping/Pong`, `Logout`, `Kick{reason}` (from the server), `Reconnect{jitter}` (from the server), `Error`, `Busy{retry_after}`.
- `LoginReq` carries the token, appid, platform, **device_id** (stable per install) and **conn_seq** (counter on the device, incremented on every connection attempt).
- Kick reasons: `REPLACED` (same device, newer connection; the user doesn't see it) and `KICKED_BY_OTHER_DEVICE` (shown to the user; the client must not reconnect automatically).

## Gateway

- gnet **≥ v2.8.0** (needed for `Conn.EventLoop()`). No goroutine per connection. `OnTraffic` never blocks; RPCs go through a bounded worker pool.
- Connection states: `HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`.
- **Identity:** `gateway_id` = pod name; `epoch` = random uint64 chosen at boot; `conn_id = (worker_id << 48) | loopState.counter++`.
  - Checked against gnet's `dev` source: `OnOpen` always runs on the owning loop's goroutine (both accept paths). The loop index is unexported, but `Conn.EventLoop()` gives the loop's identity.
  - `worker_id` is assigned lazily the first time `OnOpen` sees a loop (an atomic that runs about NumCPU times), cached in a `sync.Map` from EventLoop to loopState.
  - The plain `counter++` is safe only because just `OnOpen` touches it. The counter is unexported, and a `-race` test enforces the rule.
- **Per-loop connection tables with no locks:** `map[conn_id]*conn`, touched only by the owning loop. A Kick from gRPC reads `conn_id >> 48`, takes that loop's handle and calls `loop.Execute(...)` to look up the connection, check the epoch and state, send the Kick frame and `Close`. This also meets gnet's rule that Close runs on the owning loop.
- Kick acts only on connections in the `ONLINE` state.
- Heartbeat: **150s by default, set by the server in `LoginResp`**. Any frame resets the timer. A connection is dead after 2 × interval + 10s (= 310s), and the gateway then sends Logout. A timing wheel drives the timers.
- **Admission control:** cap the connections in `HANDSHAKE` + `AUTH_PENDING` per gateway (configurable, for example 5k); above the cap, reply `Busy{retry_after}` and close. One login attempt per connection (a failure closes it). A token bucket for new connections per IP. Queued Logins older than their deadline are dropped.
- Login batching: up to 200 logins or 5ms per RPC.
- Lease: call `GatewayLease` every 10s.

## Logic

- Stateless. Every node serves every RPC and can kick any gateway. Scales out horizontally (HPA on CPU).
- RPCs: `Login` (batch), `Logout` (batch), `GatewayLease`, `EpochEnded`.
- Login: verify the JWT locally, then run the Lua login script (EVALSHA, pipelined), then do the follow-ups (SADD to the epoch index; Kick to the old gateway asynchronously if the result needs one).

## Redis schema (Cluster; per-user keys hash-tagged `{uid}`)

| Key | Type | Value / TTL | Purpose |
|---|---|---|---|
| `sess:{uid}` | HASH | field `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms`; key TTL 30d, refreshed on login | online mark (source of truth) |
| `gwlease:gw:{epoch}` | STRING | `PX 30000`, renewed every 10s with a plain SET | gateway instance is alive |
| `gwepochs` | ZSET | member `gw:epoch`, score = last renewal in ms | registry of epochs, used by the sweeper |
| `gwconns:gw:{epoch}` | SET | uids | cleanup hint (not atomic with sess, and not needed for correctness) |
| `sweep:gw:{epoch}` | STRING | `SET NX PX 300000` | sweeper claim |

**A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`.

### Login Lua (single slot, atomic)

Compare the stored field with the new login:

| Stored vs new | Result |
|---|---|
| empty | write; no kick |
| same device_id, new conn_seq > stored | overwrite; Logic kicks the old connection with `REPLACED` |
| same device_id, new conn_seq ≤ stored | reject the new login as `STALE`; close it; no write |
| different device_id | overwrite (last one wins); kick the old one with `KICKED_BY_OTHER_DEVICE` |

Note: Redis Lua can't check the lease key (a different slot), so Logic checks the lease of the old gateway before sending a Kick. Whether the lease is live doesn't change the write decision.

### Logout Lua

`HDEL` the field only if it still matches `gw|epoch|conn_id`. This solves the delayed-disconnect race, including on the same gateway, because conn_id is never reused.

### Sweeper (goroutine in every Logic node)

Find `gwepochs` members whose last renewal is more than 60s old (or that received `EpochEnded`), claim each with `sweep:` SET NX, run `SSCAN gwconns`, and for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch. Then delete the set and the ZSET member. Rate-limited to about 10k users/s.

Safety net: the 30-day TTL on `sess:{uid}`. Gateways refresh the TTL for connections older than 15 days by sending a slow batch RPC to Logic (the gateway itself never touches Redis).

### Why the dropped concepts are not needed

- **Login rate limit:** blocks real fast reconnects (Wi-Fi to 4G), and a rejected login costs Redis the same as an accepted one. Abuse protection moves to the gateway at no Redis cost.
- **Distributed lock:** every change is a single atomic Lua script on a single slot.
- **Reverse mapping:** the gateway knows which user owns each connection; cleanup uses `gwconns`.
- **Session version:** the `gw|epoch|conn_id` triple plus `device_id`/`conn_seq` covers it.

## Races handled

1. **Delayed disconnect** (old G17 logout after the new G42 login): Logout compare-and-delete doesn't match, so nothing happens.
2. **Delayed login from an abandoned connection** (S1's login lands after S2's): same device, older conn_seq, so the result is `STALE` and S2 is unaffected.
3. **Zombie gateway with the same pod name:** leases are keyed per epoch, so the instances don't conflict, and a Kick that reaches the wrong instance is ignored because the epoch doesn't match.
4. **Connection closes while its Login is in flight:** the gateway sends Logout after the Login reply arrives, so order is kept for each connection.
5. **Login RPC times out but Redis was written:** the gateway closes and sends Logout, which is safe because compare-and-delete is idempotent.
6. **Logout lost while the gateway is alive:** the stale entry gets repaired lazily (in sub-project 3, a push that gets "unknown conn" back leads Logic to compare-and-delete).
7. **Two devices kick each other back and forth:** the client must not reconnect automatically after `KICKED_BY_OTHER_DEVICE`.
8. **Two loops generating conn_ids at once:** each loop has its own counter, and the worker_id prefix keeps the IDs apart.

## Storms, drain, failures

- Client SDK: wait a random 0–30s before reconnecting after an unexpected close; exponential backoff (cap 5m); obey `retry_after`.
- Capacity: N−1 gateways must absorb one dead gateway's connections.
- Graceful drain: on SIGTERM, go NotReady and stop accepting, send `Reconnect{jitter}` in batches over about 60s, close the rest, then call `EpochEnded`. `terminationGracePeriodSeconds` must be longer than the drain window.

| Failure | Behavior |
|---|---|
| Logic down | Login gets a retryable error and the client backs off. Logout goes into a bounded retry queue for 60s, then is dropped (the sweeper and lazy repair cover it). |
| Redis down | New logins fail; existing connections stay up. Lease SET resumes when Redis returns, and the entries become valid again. |
| Gateway crash | The lease expires within 30s; clients reconnect; the sweeper cleans up after 60s. |
| Kick can't reach the gateway | Ignored (best effort). |
| Bad frame / decryption failure / frame too large | Close immediately. |

## Observability

Prometheus metrics: connections by state for each gateway; handshakes/s; logins by result (`ok`, `stale`, `replaced`, `kicked`, `busy`, `error`); end-to-end login latency p50/p99; gRPC and Redis script latency; lease renewal failures; admission rejects; sweeper progress; **reconnect-after-idle rate** (shows NAT timeouts shorter than 150s); Go GC pause and heap. Logs: `slog` lines with `uid` and `conn_id`; a request ID passed in gRPC metadata.

## Verification

- **Unit tests:** frame codec (partial frames, several frames at once, fuzzing), Noise handshake against a test client, the connection state machine, conn_id generation, and the **gnet loop-ownership test under `-race`** (`OnOpen`, `OnClose` and the `Execute` callback all see the same loop for a given connection).
- **Lua tests** against a real Redis in Docker: one table test for each row of the login table, the logout compare-and-delete, and the sweeper deleting only dead-epoch fields.
- **Race integration tests** (`deploy/compose`: Redis, Logic, 2 gateways and a scripted client): races 1–5 above, plus gateway crash leading to lease expiry, sweep, and the entries being gone.
- **Load tests** (`cmd/loadgen`, multiple source IPs): (a) 600k connections on one gateway, with GOMEMLIMIT/GOGC tuning; (b) kill a full gateway and check that 99% of users are logged in again within 30s; (c) 150s heartbeat CPU cost at 600k connections.

## Out of scope (later sub-projects)

Messaging (seq, dedup, MongoDB) · push pipeline (RocketMQ, APNs/FCM) · social feed · QR-code login for web · adaptive heartbeat on the client · account service.
