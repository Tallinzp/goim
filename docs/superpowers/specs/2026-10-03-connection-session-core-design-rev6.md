# Sub-project 1: Connection & Session Core — Design (revision 6)

> Revision 6 of `2026-10-03-connection-session-core-design-rev5.md`, which is left unchanged. This revision applies every decision on the system-design review of rev5, `docs/superpowers/objections/connection-session-core-design-rev5-system-design.md`, recorded in `docs/superpowers/objections/connection-session-core-design-rev5-dispositions.md`; those points are cited as *(R5S-O1)* … *(R5S-O9)*. It also applies, in place (2026-10-08), the follow-up decision PI-4 from the user's review of the Plan 1 store interfaces, recorded in the same dispositions file and cited as *(PI-4)*. A Chinese translation is in `2026-10-03-connection-session-core-design-rev6.zh-CN.md`; this English version is authoritative. The rest of this note is carried over from rev5.

> Revision 5 of `2026-10-03-connection-session-core-design-rev4.md`, which is left unchanged. This revision applies the user's own review of rev4, `docs/superpowers/objections/connection-session-core-design-rev4-human-view.md.md`, decided directly into this spec; those points are cited as *(R4H-O1)* etc., and the user's decision on rev4's remaining open point as *(R4H-OP1)*. It also applies every decision on the two outside reviews of rev4 recorded in `docs/superpowers/objections/connection-session-core-design-rev4-dispositions.md`, cited as *(R4M-<n>)* (MiniMax) and *(R4G-O<n>)* (GPT). A Chinese translation is in `2026-10-03-connection-session-core-design-rev5.zh-CN.md`; this English version is authoritative. The rest of this note is carried over from rev4.

> Revision 4 of `2026-10-03-connection-session-core-design-rev3.md`, which is left unchanged. This revision applies every decision in `docs/superpowers/objections/connection-session-core-design-rev3-dispositions.md` (O1–O7 of the third review, plus its clarifications of rev3's open points 4–6), and the follow-up decisions on rev4's own open points recorded in the same file (*R3-OP2*, *R3-OP4*–*R3-OP7*). Every review round numbers its objections from O1, so references are prefixed: *(R1-O4)* points at `connection-session-core-design-dispositions.md` (first review), *(R2-O4)* at the rev2 dispositions (second review), *(R3-O4)* at the rev3 dispositions (third review). Points this revision had to interpret are listed under **Open points** at the end.

## Context

We are building a Go IM backend for a WeChat-like social app (Android, iOS, web) targeting ~1M concurrent users. The user previously built a C++ IM handling 2.6M TCP connections (600k per gateway). Two earlier draft docs (`gpt_design.md`, `claude_design.md`, added in `4ce1fa6` and removed in `05cd570`) disagree on presence model and scope, and leave the "delayed disconnect deletes the new session" race unsolved.

The full system is decomposed into 5 sub-projects (1 connection/session core, 2 messaging, 3 push pipeline, 4 social feed, 5 scale/hardening). **This spec covers sub-project 1 only**: connect, encrypted handshake, login, heartbeat, kick, revoke, disconnect — correct under reconnect races, and meeting a storm-recovery SLO. Sub-project 1 ships Android/iOS over TCP; the web transport is designed here and built later.

**Success criteria**
- **Capacity ceiling** *(R1-O1)*: one gateway holds 600k connections (measure CPU / memory / GC). This is a benchmark, not the operating point.
- **Storm SLO** *(R1-O1, R1-O7, R2-O3)*: covers the loss of **one gateway**, sized at the 200k operating point, defined per failure class:

  | Failure class | How clients detect it | SLO |
  |---|---|---|
  | Process crash / pod kill (node alive) | Kernel RST/FIN, passed through by the L4 NLB | 99% logged in again within 30s **of gateway death** (in tests: the moment loadgen issues SIGKILL, recorded as `T_death`; in production: approximated by the dead-epoch cross-check) *(R4M-17)* |
  | Node loss / silent network partition | Client heartbeat (Pong timeout) | 99% logged in again within 30s **of the client detecting the loss**. Detection is bounded by design: at most one heartbeat interval plus the Pong timeout. |

  Arithmetic: 200k users spread over the 0–20s reconnect jitter is ~10k logins/s, within the 20k/s peak headroom. The SLO assumes the default storm window (0–20s); widening it through `LoginResp` *(R5S-O6)* deliberately trades relogin time for load.

  Losing several gateways at once (e.g. a zone) is **degraded, best effort, with no SLO** *(R2-O3)*: clients may be rejected with `at_capacity` and retry until the lost pods are rescheduled.

  **Whole-population reconnect** *(R5S-O6)* (LB failover or reset, a regional or carrier network recovering) is also **best effort, with no SLO**. Expected recovery ≈ users ÷ admitted login rate (1M ÷ 20k/s ≈ 50s) plus the backoff tail of clients that hit the reject limits. Admission control (handshake bucket, connection cap, Login-queue cap) shapes it; no component may fail or fall over under it.

  **Node-loss recovery** *(R5S-O3)*: after a node loss, a replacement gateway is Ready within the node-loss threshold (default 2 min) plus gateway startup (≈ 2–3 min in total), after which the fleet is back at N (see *Deployment requirements*).
- **Correctness:** no stale event (late disconnect, late login from the same or another device, zombie gateway, late Reconcile restore, late sweep) can delete or override a newer session; covered by automated tests.

**Density** *(R1-O1, R2-O2, R2-O3)*: the operating point is at most 200k connections per gateway. 1M users need at least 6 gateways (5 at 200k plus 1 for N−1), about 167k each in steady state. On Kubernetes pods are cheap and blast radius is the cost that matters; 600k stays as the efficiency benchmark. A hard per-gateway connection cap (default 250k, see *Admission control*) bounds skew: with one gateway down the remaining five hold up to 5 × 250k = 1.25M ≥ 1M; with a whole zone down (4 survivors) they can still hold 1M, just barely. **250k is a configured value** *(R4M-8, R4G-O2)*: it holds only if load test (a) shows a ceiling of at least 600k for the production gateway's core count (measured per core on the Tier 2 host and scaled to the production node size, see *Test execution model*). If the measured ceiling is lower, the operating point, the cap and the gateway count are re-derived from it before rollout, keeping the same ratios (operating point ≈ ceiling / 3; cap between the operating point and the ceiling).

## Decisions (agreed)

| Topic | Decision |
|---|---|
| Presence | One session per **slot** = `userid + appid + platform` (`mobile` covers Android and iOS; plus `pad`, `web`, `pc`). New login on the same slot replaces the old one. Platform is validated against the token. Different platforms are different slots and never conflict. |
| Client transport | Mobile: raw TCP via gnet. Web: WebSocket over wss (TLS terminated at the cloud LB), designed now behind a transport interface (see *Gateway → Transport seam* *(R4G-O5)*), built as a late slice. |
| Client encryption | Custom app-layer encryption using the **Noise Protocol Framework, NK pattern** (`flynn/noise`): server static key pinned in the app, with a key ID for rotation (see *Gateway → Noise key runbook* *(R4G-O6)*); ChaCha20-Poly1305 per frame. No resumption tickets (a handshake costs ~100µs, negligible in storms). Web relies on wss TLS and uses no Noise. **Why not TLS on mobile** *(R4G-O3)*: Go's `crypto/tls` needs a blocking `net.Conn` (a goroutine per connection, against the gnet model), and gnet v2.10.0 has no TLS; TLS at the NLB leaves plaintext between NLB and pod and puts 1M TLS sessions into the LB's limits and pricing; Noise NK is one round trip (~100µs), has no certificate chain, and its codec is owned by the gateway. TLS at the NLB is the fallback if Noise proves costly in load test (a). |
| Internal RPC | gRPC both ways. Gateway→Logic: `dns:///` headless service + `round_robin`; `Login` streams a result back per entry as soon as that entry finishes, so a slow Redis shard holds back only its own entries *(R5S-O1)*; Logic server sets `MaxConnectionAge` ≈5m **and `MaxConnectionAgeGrace` = 30s** (above the 6s Login timeout) so in-flight batches finish after GOAWAY *(R1-O12)*. `GatewayLease` uses its own gRPC connection *(R1-O2)*. Logic→Gateway: Kick dialed by StatefulSet pod DNS (`gw-17.gateway.im.svc`), so no registry is needed; the gateway headless service sets `publishNotReadyAddresses: true` so draining pods still resolve *(R1-O8)*. |
| Auth | Separate account service issues Ed25519-signed JWTs as **short-lived access tokens (e.g. 1h), checked only at Login**. Token expiry does not affect a connection that is already authenticated; security for long-lived connections comes from revocation *(R1-O10)*. Logic verifies tokens locally against a JWKS keyed by `kid` (no database call on login), with a ConfigMap fallback for cold start *(R2-O7)*. `cmd/devtoken` issues tokens and publishes a test JWKS. |
| Source of truth | Three facts, three owners *(R4M-11)*: **does this connection exist?** — the gateway; **which connection owns this slot?** — `sess:{uid}`, reconciled to the gateway by periodic anti-entropy (`Reconcile` + `ReconcileEnd`) *(R1-O3, R3-O1)*; **is this gateway epoch alive?** — `gwlease:gw:{epoch}` (`gwepochs` is only an advisory index *(R4M-3)*). |
| Deployment | Kubernetes on cloud. Gateways run as a StatefulSet behind an L4 NLB, with `topologySpreadConstraints` on `topology.kubernetes.io/zone` and `kubernetes.io/hostname`, `maxSkew: 1`, so a single node failure is a true N−1 event *(R2-O3)*. Logic is a stateless Deployment with HPA; its `minReplicas` must handle the **20k logins/s peak without scaling**, as measured by the load test. HPA scales above that; the SLO does not depend on it *(R2-O7)*. Disruption budgets, node-loss recovery, the LB algorithm, Redis placement and production node and LB limits: see *Deployment requirements* *(R5S-O3, R5S-O4, R5S-O5, R5S-O7, R5S-O9)*. |
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

### Deployment requirements

- **Load balancer algorithm** *(R5S-O4)*: the cloud LB uses a **least-connections** algorithm where it offers one (e.g. Alibaba CLB/NLB `wlc`, Tencent CLB `least_conn`). Where it only flow-hashes (e.g. AWS NLB), the SDK's pre-handshake retry rule (see *Client SDK rules*) is what keeps the SLO during a rolling restart, and skew stays until churn evens it out (active rebalancing is sub-project 5). Why: during a rolling restart a load-blind LB pushes the pods not yet restarted to the cap; from the third step of a 6-pod roll at 1M, 3 of 5 targets are full.
- **Disruption budgets** *(R5S-O5)*:
  - gateway PodDisruptionBudget `maxUnavailable: 1`. Intended side effect: if a gateway is already unavailable (e.g. after a node loss), the PDB blocks further voluntary evictions until it is back, so an upgrade waits rather than turning a 1-gateway loss into a 2-gateway loss;
  - gateway pods carry `cluster-autoscaler.kubernetes.io/safe-to-evict: "false"`, so the autoscaler never removes a gateway node on its own; gateway capacity changes only through the StatefulSet;
  - Logic PodDisruptionBudget `minAvailable` = `minReplicas`, because the storm SLO assumes those replicas are Ready.
- **Node-loss recovery** *(R5S-O3)*: a StatefulSet does not recreate a pod whose node is unreachable until the pod is confirmed gone. A controller (a small operator, or the cloud's node auto-repair if it applies the same taint) applies `node.kubernetes.io/out-of-service=nodeshutdown:NoExecute` to a node that runs a gateway pod and has been NotReady for longer than the node-loss threshold (default 2 min, configurable). Kubernetes then deletes the pod and its volume attachments, and the StatefulSet recreates `gw-k` on another node. If the taint can't be applied, the fallback is to force-delete the pod (`--grace-period=0 --force`). The taint is removed once the node is back or replaced. This is safe because the design never relies on at most one instance per pod name: a stale `gw-k` still running on the partitioned node has a different epoch, so its lease, Kicks and Redis entries can't conflict with the new instance (race 4). Ending the stuck pod also removes its Endpoint, so Kicks stop resolving to a dead IP.
- **Redis placement** *(R5S-O9)*: Redis Cluster has **≥ 3 masters spread over ≥ 3 zones, and each master's replica in a different zone from its master**, so one zone loss never removes a majority of masters and every master has a replica outside its own zone. For a managed Redis, the equivalent multi-zone setting is named in the deployment config.
- **Production node and LB limits** *(R5S-O7)*, each derived from the configured cap (`maxConnsPerGateway`):
  - container `nofile` (`RLIMIT_NOFILE`) ≥ cap + headroom (default headroom 10%, for gRPC, the pending set and closing sockets);
  - per gateway node, `nf_conntrack_max` ≥ 2 × cap, **or** conntrack disabled for the gateway port (`NOTRACK` / CNI equivalent), declared in the gateway config as `conntrack: notrack`;
  - `net.core.somaxconn` and the gnet listen backlog large enough for the per-gateway accept burst (≥ 4096; sized in load test (b1));
  - cloud LB: concurrent connections ≥ the fleet's total cap (6 × 250k = 1.5M by default; ≥ 1.25M for N−1), and new connections/s ≥ the whole-population reconnect rate it must shape *(R5S-O6)*, not just the 20k/s storm peak;
  - LB idle timeout > heartbeat interval with margin (default 150s): a live client sends a frame at least every interval, so the LB never sees it idle for longer. Where the LB's timeout is fixed and lower, the heartbeat interval in `LoginResp` is lowered to fit.

  The gateway enforces the two limits it can read (see *Readiness*). LB limits can't be read from a pod; the Tier 3 `cloud` run checks them.

## Client protocol

- **Outer frame** *(R4H-O1)*: a fixed 6-byte header, then the payload. All fields big-endian. The gateway buffers partial frames and splits frames that arrive together.

  ```
  +---------+---------+---------+---------+---------+
  | Version |   Cmd   |  Flags  |  Length | Payload |
  | (1 B)   | (2 B)   | (1 B)   | (2 B)   | (N B)   |
  +---------+---------+---------+---------+---------+
  ```

  - `Length` counts the payload only, not the header (max 65535).
  - `Version`: an unsupported version is closed before the handshake and counted as a `bad_version` reject; the client treats the close as a pre-handshake close (see *Client SDK rules*).
  - `Flags`: every bit is reserved in v1 and must be 0; a frame with any bit set is closed as a bad frame.
  - `Cmd` is readable without decrypting, so the gateway can reject or route a frame before any crypto. It reveals the command type to an observer; frame sizes and timing already reveal most of that.
- **Frame 1**: `Cmd = HANDSHAKE`, payload is `key_id(2) || noise_msg` (the raw Noise NK message), with no `req_id` and no AEAD. The `key_id` sits outside the Noise message because the gateway needs it to pick its static key before processing the message *(R4G-O6)*. The header plus `key_id` is mixed into the handshake hash as the Noise prologue, so both are authenticated. **Handshake messages carry no application payload** (LoginReq is the first frame after the handshake) *(R4G-O6)*.
- **Every later frame**: payload = AEAD-seal(`req_id(4) || body`), with a nonce counter per direction. The 6-byte header is passed as AEAD associated data, so a tampered `Version`, `Cmd`, `Flags` or `Length` fails decryption and the connection is closed. `Length` includes the 16-byte tag, so the largest body is 65535 − 4 − 16 = 65515 bytes.
- **Web**: each WebSocket binary message carries the same header and payload (`Length` kept for uniformity). There is no Noise, so the payload is plaintext `req_id || body` and there is no AD binding; wss TLS protects it.
- Commands (`Cmd`): `Login`, `LoginResp` (includes the heartbeat interval and the reconnect windows, see below), `Ping/Pong`, `Logout`, `LogoutResp` (from the server: the gateway accepted the logout, not that Redis was updated *(R4H-O8)*), `Kick{reason}` (from the server), `Reconnect{jitter_ms}` (from the server; the client closes, waits a uniform random `[0, jitter_ms]`, then reconnects with `reconnect_reason = Reconnect from drain` *(R4H-O3)*), `Error`, `Busy{retry_after}`.
- **Reconnect windows in `LoginResp`** *(R5S-O6)*: besides the heartbeat interval, `LoginResp` carries the windows the SDK uses for its waits: storm wait (default 0–20s), local trigger (default 0–1s), pre-handshake retry (default 0.5–1.5s *(R5S-O4)*) and other retryable rejects (default 1–3s). Every field is optional; an absent field keeps the SDK's current value. They let ops widen a window, during an incident or after measurement, without an app release (see *Client SDK rules* for persistence and clamping).
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
  | `EXPIRED` | The login reached Redis more than `login_deadline` after the gateway received it *(R1-O4)*. A backpressure signal. | Drop this Login and its connection; reconnect with a new `conn_seq` as a **retryable reject** (see *Client SDK rules*) *(R4H-O2)*. |
  | `REVOKED` | Token issued before the user's revocation marker *(R1-O10)*. | Drop the token, show the login screen. |
  | `KEY_UNKNOWN` | The token's `kid` is not in Logic's key set; a background refresh may be under way *(R4H-O10)*. | Retryable reject (see *Client SDK rules*). |
- Kick reasons. **None of them triggers an automatic reconnect:**
  - `REPLACED`: same device, newer connection; not shown to the user. The client goes idle and reconnects only on a user action or when the app returns to the foreground. The SDK logs it as an anomaly, since a live client only sees it if a second instance shares its device_id *(R1-O6)*.
  - `KICKED_BY_OTHER_DEVICE`: shown to the user.
  - `REVOKED` *(R1-O10)*: the client drops its token and shows the login screen.
- **device_id** *(R1-O6)* identifies one client instance that holds a connection. The SDK must make sure at most one connection per device_id is in use at a time. Mobile and desktop: one per install, plus a process-level guard (e.g. a single connection service on Android). The server doesn't rely on this for correctness: a violation only makes the instances replace each other on the user's own slot (race 10) *(R4G-O1)*.

### Client SDK rules

- **conn_seq persistence** *(R1-O5)*: persist `conn_seq` **before** each connection attempt, in the same place as device_id with the same lifetime (e.g. both in the iOS Keychain), so clearing one clears the other.
- **Reconnect** *(R1-O7, R3-O5)*:
  - **Connection lost** (RST/FIN or Pong timeout): after losing an **established** connection unexpectedly, wait a **uniformly distributed** random **0–20s** *(R4G-O12)* before the first attempt (leaving 10s for handshake, login and one Busy retry), then exponential backoff (cap 5m). Obey `retry_after`. The client can't tell a mass disconnect from a loss of its own, so this applies whatever the cause *(R4H-O3)*.
  - **Local trigger** *(R4H-O3)*: when the OS reports a network change, or the app returns to the foreground while disconnected, cancel any pending 0–20s wait and attempt within a uniform random **0–1s**. Such events are usually uncorrelated across users. When they are correlated (a carrier or city network recovering), the burst is shaped by gateway admission control and the retry rules below, and ops can widen the local window through `LoginResp` *(R5S-O6)*. This also covers Wi-Fi ↔ cellular: the switch kills the socket and schedules a 0–20s wait, and the network-change event then cuts it short.
  - **Precedence** *(R4H-O3)*: a local trigger removes only the 0–20s storm wait. It does not shorten a `Busy{retry_after}` wait, a `Reconnect{jitter_ms}` wait, or exponential backoff after a retry limit, so flapping networks can't bypass server backpressure.
  - **`Reconnect{jitter_ms}`** *(R4H-O3)*: wait the server-given uniform `[0, jitter_ms]`, not 0–20s; this is a planned move, not an unexpected loss. Failures after that follow the normal rules.
  - **Retryable rejects** *(R3-O5, R4H-O2, R4H-O7, R4H-O10, R5S-O4)*: five outcomes are retried quickly, with **two counters**:
    - **Pre-handshake closes**: a TCP connection that the server closes before the Noise handshake completes (`at_capacity`, `handshake_rate`, `bad_version` and `noise_unknown_key` rejects close the socket before any framing, so no `Busy{retry_after}` can be sent). Wait a uniformly random **0.5–1.5s**, then reconnect. After **10 in a row**, fall back to exponential backoff (cap 5m).
    - **Other retryable rejects**, sharing the second counter:
      - `Busy{retry_after}`;
      - a Login rejected as `EXPIRED`;
      - a Login rejected as `KEY_UNKNOWN` *(R4H-O10)*;
      - a connection closed after the Noise handshake but before any `LoginResp` or `Error` arrives (e.g. the `AUTH_PENDING` timeout, or a failure whose `Error` frame was lost). The client never logged in, so this is not a storm case *(R4H-O7)*.

      Each waits a uniformly random 1–3s (or obeys `retry_after` when the server sends one), then reconnects. After **5 in a row**, fall back to exponential backoff (cap 5m).

    An outcome of one kind resets the other kind's counter; both reset on a successful login. **Why two counters** *(R5S-O4)*: a pre-handshake close costs the gateway one accept and one close, no crypto, so more and faster retries are cheap for the server, and they are what keeps the SLO when a load-blind LB sends clients to full gateways (with 3 of 5 targets full, 0.6¹⁰ ≈ 0.6% of rejected clients reach backoff, against 0.6⁵ ≈ 7.8% with the old limit of 5; 10 retries take ~10s, which fits after the 5s drain jitter). The client can't tell `at_capacity` from the other pre-handshake closes; `handshake_rate` is still shaped by the gateway's bucket, and the persistent `bad_version` / `noise_unknown_key` just reach backoff a few seconds later.

    Fast retries are safe under backpressure: a client has at most one attempt in flight, an `EXPIRED` round already takes ≥ `login_deadline`, so the retry rate falls as Logic slows, and the in-a-row limits hand a lasting overload to exponential backoff. The client never gives up permanently while the app wants to be online.
  - **Server-set windows** *(R5S-O6)*: the 0–20s storm wait, the 0–1s local trigger, the 0.5–1.5s pre-handshake retry and the 1–3s retry are defaults; the SDK uses the values from the last `LoginResp` that carried them. It **persists** them next to `device_id`/`conn_seq` and uses them for every later wait, including before the next login. It **clamps** each value to a safe range built into the app (ranges fixed in the plan: the upper end of the storm wait is clamped to 10–120s, since 200k reconnects over less than 10s exceed the 20k/s headroom; local trigger 1–10s; retries 0.25–10s), so a bad config can't produce a zero-jitter storm or an unbounded wait. Fresh installs use the built-in defaults.
  - **Stop conditions** *(R4H-O2)*: logout, account switch and app close cancel every pending reconnect or retry, whatever its cause.
  - Each attempt uses a new source port, so the NLB flow hash can pick another gateway *(R2-O2)*.
- **Dead-server detection** *(R1-O7)*: if no frame arrives within **10s** of a Ping, close and reconnect.
- **Web** (designed now, built later) *(R1-O6)*: one connection per browser profile. Tabs elect a leader through `SharedWorker`, falling back to Web Locks plus `BroadcastChannel`. The leader holds the wss connection and relays messages to the other tabs; when the leader tab closes, another tab takes over. device_id and conn_seq live in localStorage.

## Gateway

- gnet pinned to exactly **`github.com/panjf2000/gnet/v2 v2.10.0`** in `go.mod` *(R4G-O7)* (needs `Conn.EventLoop()`, present since v2.8.0). Any gnet upgrade must first pass the `-race` loop-ownership test and the loopState tests (Tier 1a). No goroutine per connection. `OnTraffic` never blocks; RPCs go through a bounded worker pool.
- Connection states: `HANDSHAKE → AUTH_PENDING → ONLINE → CLOSED`.
- **Transport seam** *(R4G-O5)*: the gateway core (state machine, timeouts, heartbeat, admission except the Noise limit, Login/Logout/Reconcile, Kick) works only on decoded frames `(cmd, req_id, body)`. A transport turns a connection into: an open event, a stream of decoded frames, `Send(cmd, req_id, body)`, `Close()`, and a "handshake done" signal that moves the connection from `HANDSHAKE` to `AUTH_PENDING`.
  - **TCP transport**: cuts frames from the byte stream by the header's `Length`; runs Noise NK with the header as AEAD associated data *(R4H-O1)*; its `HANDSHAKE` state is the Noise handshake.
  - **wss transport (later slice)**: one binary WebSocket message carries exactly one frame (a `Length` that doesn't match the message size is a bad frame); TLS from the LB, no Noise; its `HANDSHAKE` state is the HTTP upgrade. Whether wss runs on gnet or a separate listener is decided in the web slice.
- **Noise key runbook** *(R4G-O6)*:
  - **Server key set**: gateways load `{key_id → private key}` from a Kubernetes Secret (Readiness requires it) and select the key by the handshake's `key_id`. An unknown or retired `key_id` is closed before the handshake and counted as a `noise_unknown_key` reject; the client treats it as a pre-handshake close.
  - **Rotation** (bounded by app release cycles): (1) add the new key to the Secret and roll the gateways; (2) ship app builds that pin both the current and the new key; (3) remove the old key only when every app version that pins only the old key is below the supported-version floor.
  - **Compromise**: NK session keys mix `ee`, so a stolen static key does not decrypt recorded traffic (provided the handshake messages carry no application payload, see *Client protocol*). It does let an attacker impersonate the server to apps that pin it. Response: emergency app release with a new key, then remove the compromised key from the Secret; app versions that pin only that key then fail the handshake (retryable rejects, then backoff) and must upgrade through the app's update path, outside this protocol.
- **Identity:** `gateway_id` = pod name; `epoch` = random uint64 chosen at boot; `conn_id = (worker_id << 48) | loopState.counter++`.
  - Checked against the gnet **v2.10.0** source *(R4G-O7)*: `OnOpen` always runs on the owning loop's goroutine on both accept paths (main reactor: `accept0` picks the loop through the load balancer and posts `register` to that loop's poller, which runs `register0 → open → OnOpen` there; reuseport: the accepting loop calls `register0` itself). The loop index is unexported, but `Conn.EventLoop()` gives the loop's identity.
  - **loopStates are built at boot, only the binding is lazy** *(R4H-O5)*: at boot the gateway builds `numLoops` loopStates in a slice (`numLoops` = the configured gnet `NumEventLoop`), each with `worker_id = i`, its pre-allocated table and its counter. gnet gives no way to list loops at boot (`Engine` has no such method and `OnBoot` receives only the `Engine`; checked against gnet v2.10.0), so the first time `OnOpen` sees a loop it claims the next loopState from the slice (one atomic increment) and caches it in a `sync.Map` from EventLoop to loopState. Nothing heavy is allocated on the request path. A loop seen beyond `numLoops` is a startup bug: the gateway panics.
  - The per-loop table and counter stay bound to their loop, never drawn from a shared pool: that binding is what makes them lock-free, and Kick routing relies on `conn_id >> 48`.
  - The plain `counter++` is safe only because just `OnOpen` touches it. The counter is unexported, and a `-race` test enforces the rule.
- **Per-loop connection tables with no locks:** `map[conn_id]*conn`, touched only by the owning loop. A Kick from gRPC reads `conn_id >> 48`, takes that loop's handle and calls `loop.Execute(...)` to look up the connection, check the epoch and state, and act. This also meets gnet's rule that Close runs on the owning loop.
  - **Pre-allocation** *(R4H-O4)*: each loop's map is created with `make(map[uint64]*conn, maxConnsPerGateway / numLoops × 1.5)`, so it does not grow (allocate and rehash) during a storm. At the 250k default cap that is ~375k slots in total, roughly 8–10MB.
    - It is a hint, not a limit: a map that goes past it simply grows; the connection cap at accept is the real limit, so a wrong hint costs performance, never correctness.
    - The 1.5 factor covers imbalance between loops: gnet's round-robin load balancer (`gnet.WithLoadBalancing(gnet.RoundRobin)`, the default, chosen explicitly) spreads accepts evenly, closes skew loops apart, and `CLOSED` entries awaiting their Logout fence (normally ≤ `login_deadline + 1s`, not counted by the cap) add a little.
    - The hint follows the configured cap, so the 600k benchmark gets a larger one automatically.
  - **Tracked connections** *(R3-O1)*: a connection stays in its loop's table from accept until it no longer needs a Logout — i.e. it closed without ever sending a Login, or its fenced Logout was acknowledged or dropped from the retry queue. A `CLOSED` entry waiting for its Logout fence is still tracked. This tracked set is what `ReconcileEnd` reports.
- **Kick handling** *(R1-O8)*:
  - `ONLINE`: send the Kick frame and close.
  - `AUTH_PENDING` (Login in flight): set a kick-pending mark with the reason. When the Login reply arrives, send the Kick frame and close instead of moving to `ONLINE`.
  - `HANDSHAKE`, `CLOSED`, unknown conn_id, or epoch mismatch: ignore.
- **Heartbeat:** **150s by default, set by the server in `LoginResp`**. Any frame resets the timer (lazily, see *Timers*). A connection is dead after 2 × interval + 10s (= 310s), and the gateway then sends Logout (subject to the fence). Runs on the loop's timing wheel.
- **Timeouts** *(R1-O11, R4H-O7)*: `HANDSHAKE` (accept → handshake complete) and `AUTH_PENDING` (handshake complete → LoginReq received). On expiry the connection closes silently. Both run on the loop's timing wheel (see *Timers*).
  - Both are configurable in the gateway ConfigMap (one value for the StatefulSet): default **5s**, allowed range **3–30s**. A gateway refuses to start if either is out of range. The 3s floor covers one client→server trip on a weak mobile link plus a retransmission; the memory-only pending cap, not a short timeout, is what bounds slow clients.
  - The `AUTH_PENDING` timer stops when LoginReq arrives. The connection stays in `AUTH_PENDING` while its Login is queued or in flight, but that wait is governed by `login_deadline` and the Login RPC timeout, so a backed-up Login queue never triggers `auth_timeout`.
- **Timers** *(R4H-O6)*: per-connection and per-chunk timers run on a timing wheel; gateway-wide periodic work uses ordinary Go timers.

  | Timer | Owner | Mechanism |
  |---|---|---|
  | `HANDSHAKE` 5s, `AUTH_PENDING` 5s | per connection | wheel |
  | Heartbeat (dead after 310s) | per connection | wheel |
  | Logout fence (b): `recv_ms + login_deadline + 1s` | per connection | wheel |
  | Reconcile chunk deadline (fence c) | per chunk | wheel (one entry per chunk) |
  | Logout / `ReconcileEnd` retry queue (60s) | gateway-wide | own goroutine, `time.Timer` |
  | Lease every 10s, periodic Reconcile ~10 min | gateway-wide | own goroutine, `time.Ticker` |
  | Sweeper | Logic | Logic's own loop |

  Client retry backoff runs in the SDK. The `login_deadline` check on a queued Login is made when its batch is sent, not by a timer.
  - **One wheel per loop**, stored in the loopState and touched only on the loop goroutine, like the table: no locks, and expiry callbacks may read the loop's map. A single ticker goroutine posts `loop.Execute(advance)` to every loop once per tick. gnet's `OnTick` is not used: it is one engine-wide callback, not per loop.
  - **1s tick, single-level wheel** of 512 slots; longer delays carry a round counter. The shortest timer is 5s.
  - **Never fire early**: deadlines round **up** to the next tick. A fence may fire up to 1s late, which is safe; it must never fire early.
  - **Heartbeat lazy re-arm**: a received frame only updates `last_active`. When the entry fires, the loop checks `now − last_active`; if still within 310s it re-arms the entry for the remainder, otherwise the connection is dead. One wheel operation per 310s per connection, not one per frame.
- **Admission control** *(R1-O11, R2-O2)*:
  - **Connection cap** *(R2-O2, R3 clarification)*: a hard per-gateway cap on open **sockets**, counted from accept to close, in every state (configurable, default **250k**; it must sit between the 200k operating point and the 600k ceiling). Closed entries kept in the table only for a pending Logout do not count. At the cap the gateway closes new connections at accept, before any crypto, and counts an `at_capacity` reject. It is one counter and one check at accept. Skew below the cap is harmless; active rebalancing is out of scope (sub-project 5).
  - **Noise handshake limit (per gateway, global; never per IP):** a concurrency limit on handshake computations running at once (a semaphore sized to the cores dedicated to handshakes) plus a gateway-wide token bucket on handshakes/s, sized from the measured NK cost (~100µs) so handshakes can't starve established connections. The bucket must allow at least the 20k/s storm peak with margin; in a legitimate storm it shapes load rather than rejecting it. When either is exhausted the gateway does not start the handshake: it closes the connection and counts a `handshake_rate` reject. The client treats this close as a `Busy` retry (see *Client SDK rules*) *(R3-O5)*.
  - **Login-queue cap:** cap the Logins waiting for or inside an RPC to Logic (this reflects Logic backpressure). At the cap, reply `Busy{retry_after}` and close. **Sizing** *(R4H-O2)*: `login_queue_cap ≤ measured Logic login throughput per gateway × (login_deadline − margin)`, so a queued Login can't reach the deadline. Overload then shows up as a cheap `Busy` at the gateway, not as `EXPIRED` after Logic has done the work; `EXPIRED` is left to rare stalls (GC pause, Redis failover).
  - **Memory-only pending cap:** a large cap on total `HANDSHAKE` + `AUTH_PENDING` connections, only to protect gateway memory. It must not trigger in a normal storm.
  - `Busy{retry_after}` uses a short value, **1–3s, uniformly distributed** *(R4G-O12)*, so rejected clients still fit within the SLO *(R1-O7)*.
  - One login attempt per connection (a failure closes it).
  - Queued Logins older than `login_deadline` (measured from `recv_ms`) are dropped before being sent; they never reach Logic, so they need no Logout.
- **recv_ms** *(R1-O4)*: the gateway stamps `recv_ms` (wall clock, NTP-synced) when it receives the Login frame and passes it in the Login RPC.
- **Login batching:** up to 200 logins or 5ms per RPC. The batch bounds how many entries go out together, not when they come back: Logic streams each entry's result as soon as that entry finishes *(R5S-O1)*, so an entry on a slow Redis shard never holds back an entry on a healthy one. The Login timeout is at least `login_deadline + 1s` (= 6s by default), measured from `recv_ms`, and applies **per entry** *(R1-O9, R5S-O1)*. An entry with no result when its timeout passes, or when the stream ends or fails, has no definitive reply and falls under fence (b).
- **Logout fence** *(R1-O9, R2-O1)* — gateway invariant: the gateway sends Logout for a connection only when **both** of the following hold.
  - **Login condition** — either:
    - (a) it has a **definitive Login reply** (success or rejection), at which point the Lua write has happened or never will; or
    - (b) `recv_ms + login_deadline + 1s` has passed (the 1s absorbs gateway↔Redis clock skew). This applies to **any** Login without a definitive reply: timeout, RPC error, a gRPC connection reset by `MaxConnectionAge`, or a Logic crash.
  - **Reconcile condition** *(R2-O1)*:
    - (c) no Reconcile chunk containing the connection is unresolved. A chunk is resolved when it has a definitive reply, or when `sent_ms + login_deadline + 1s` has passed (timeout, RPC error, Logic crash), since the Reconcile compare-and-set Lua refuses to write after `sent_ms + login_deadline`. A `refused` entry result is a definitive reply *(R3-O6)*. Implemented as a per-connection `in_reconcile_chunk` mark, set when the chunk is built and cleared when the chunk is resolved.

  - **Client-initiated Logout** *(R4H-O8)*: the fence applies to it too. On `Logout` the gateway replies `LogoutResp`, closes, and sends the Logout RPC through the fenced path. Fence (a) always holds for an ONLINE connection (`AUTH_PENDING` accepts only LoginReq), so only fence (c) can delay it, and only while its chunk is in flight (normally milliseconds, at most `login_deadline + 1s`). Exempting it would let a chunk in flight restore the session by rule 4 after the delete, leaving a ghost of an explicit logout for up to one cycle. The client does not wait for the Redis delete: an account switch uses a different `sess:{uid}` key, and the next login on the same slot replaces any leftover entry.

  A connection that closes before the fence holds keeps a pending Logout that fires when it does. Logouts go through a bounded retry queue (60s); dropping one is safe because the next Reconcile pass repairs it *(R1-O3, R3-O1)*.
- **Lease** *(R1-O2, R3-O3)*: call `GatewayLease` every 10s on a **dedicated gRPC connection**, never batched with or queued behind Login/Logout/Reconcile. The reply says whether the lease was found **live** or **missing**: it is `missing` whenever the renewal found the lease key absent, whatever the state of the `gwepochs` member. The renewal itself re-registers the epoch (lease key and `gwepochs` score). On `missing`, the gateway starts an immediate triggered Reconcile pass. The first lease call after boot always finds the key absent (a new epoch); the gateway ignores `missing` on that call, and neither counts nor acts on it *(R3-OP5)*. A lease that expired but was never swept costs one unneeded triggered pass, which is rare because of the TTL derivation *(R2-O5)*. Each call carries the gateway's wall clock `gw_now_ms`, for the skew measurement *(R4M-5)*.
- **Reconcile** *(R1-O3, R1-O8, R1-O10, R2-O1, R2-O8, R3-O1, R3-O6)*: the gateway sends its ONLINE set — `(uid, appid:platform, conn_id, device_id, conn_seq, login_ms, tok_iat)` — in rate-limited chunks, then ends the pass with `ReconcileEnd`.
  - **Pass mechanics** *(R4H-O9)*: a pass is run by a **pass-controller goroutine in the gateway** (never Logic: only gateway code can call `loop.Execute`). Every table access happens on the owning loop, so there is no data race and no lock.
    - **Key snapshot**: at pass start, one `Execute` per loop copies that loop's ONLINE conn_ids into the pass queue, which is **partitioned by loop**. Each queue entry is `(conn_id, requeues)` with `requeues = 0`. Only IDs are copied: a Go map `range` can't be paused across `Execute` calls, and reading and marking all entries up front would hold every Logout behind fence (c) for the whole pass.
    - **Chunk build**: the controller draws up to N IDs from **one** loop's partition, taking turns across loops round-robin (empty partitions are skipped), so each chunk costs exactly **one `Execute`** on that loop. The callback looks up each ID, skips any that is no longer ONLINE, copies the current fields and sets `in_reconcile_chunk`, then hands the chunk back through a buffered channel, so the loop never blocks. Chunks are sent by the controller under the pass rate limit, never from a loop; several may be in flight at once. Re-queued entries count against the pass's rate limit: re-queuing lengthens a pass but never raises its rate *(R4G-O8)*.
    - **Chunk resolve**: when a chunk is resolved (reply, or deadline passed), **one `Execute`** on its loop clears `in_reconcile_chunk`, Kicks and closes `not_owner` connections, and releases any Logout fence (c) was holding. Re-queuing happens on the controller: for each `refused` entry, if `requeues < 3` push `(conn_id, requeues + 1)` back into its loop's partition, otherwise drop it and count `re-queue limit reached`. The counts live only in the pass queue (≈ 16 bytes × 200k ≈ 3.2MB during a pass) and are discarded at pass end.
    - **Cost**: at 200k connections, 16 loops and chunks of 200: ~2,000 chunk `Execute`s plus 2 × 16 for snapshot and collection per pass, ~3.3/s periodic and ~50/s triggered. Each is a task-queue push plus ~10–20µs of loop time.
    - **Tracked-set collection**: at pass end the controller first stamps `as_of_ms`, **then** posts one `Execute` per loop to collect that loop's tracked conn_ids, and merges the fragments (conn_ids are unique, so no dedup). Safety rule: **every fragment is taken at or after `as_of_ms`**. A conn_id missing from such a fragment has passed its fence for good, and a connection whose Login was received after `as_of_ms` is protected by the `login_ms < as_of_ms` check. Collecting at pass end, not during the scan, keeps race 16: a Logout dropped anywhere in the pass is repaired by that pass.
  - **Live chunks** *(R2-O1)*: each chunk is read from the per-loop connection tables (via `loop.Execute`, see *Pass mechanics*) at the moment it is sent, not from a snapshot frozen at the start of the pass. Each chunk carries `sent_ms` (gateway wall clock when sent). Building a chunk sets `in_reconcile_chunk` on each connection in it (see the Logout fence).
  - **Per-entry results** *(R3-O6)*: Logic returns `written`, `unchanged` (compare-and-set lost to a concurrent change) *(R3-OP7)*, `refused` (deadline), or `not_owner` (with reason) for each entry. All four are definitive replies for fence (c). For each `not_owner` entry the gateway sends the Kick frame with the given reason and closes the connection. A `refused` entry's conn_id goes back on the current pass's queue and is rebuilt from the live tables when its new chunk is sent; it is skipped if the connection is no longer ONLINE. Each entry is re-queued at most 3 times per pass, so the pass always ends; after that it waits for the next pass.
  - **Pass end** *(R3-O1)*: a pass completes when every chunk, including re-queued entries, has been sent and resolved. The gateway then stamps `as_of_ms` (gateway wall clock, the same clock as `recv_ms`), collects every conn_id it still **tracks** (see *Per-loop connection tables*), in any state, including closed connections whose fenced Logout has not been sent yet, in that order (see *Pass mechanics*) *(R4H-O9)*, and sends `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`. `ReconcileEnd` returns after Logic's delete phase completes, so its RPC deadline covers the rate-limited phase (≥ tracked uids ÷ delete-phase rate + margin) *(R5S-O8)*. If `ReconcileEnd` fails, it is retried with the same payload through the bounded retry queue used for Logouts (60s), then dropped; the next pass covers what it missed *(R3-OP4)*. Retrying late is safe: a conn_id absent from the list had already passed its Logout fence at `as_of_ms`, and conn_ids are never reused, so no later write for it can appear. A dropped `ReconcileEnd` stretches the ghost bound to two cycles for that pass. Size: 8 bytes × 250k (the cap) ≈ 2MB, under gRPC's 4MB default; at the 600k benchmark the list is split or the limit raised.
  - **Two paces** *(R2-O8)*, both configurable:
    - **periodic**: every ~10 min with random jitter per gateway, spread over the cycle (~330 entries/s at 200k);
    - **triggered by a `missing` lease reply**: ~5k entries/s per gateway (200k in ~40s; ~30k Lua calls/s cluster-wide if all 6 gateways run one at once).
  - **One pass at a time** *(R3 clarification)*: at most one pass runs per gateway. A `missing` reply during a periodic pass cancels it: a cancelled pass builds and sends no further chunks and sends no `ReconcileEnd`. The triggered pass starts once the cancelled pass's in-flight chunks are resolved (≤ `login_deadline + 1s`), so a connection is never in two unresolved chunks.
- **Readiness** *(R4H-O5)*: the gateway reports Ready, so the NLB sends it traffic, only after the gRPC connections to Logic (the Login/Logout/Reconcile channel and the dedicated lease channel) are connected, the Noise static key set is loaded and the first `GatewayLease` call has succeeded. No client pays for a cold start.
  - **Limits self-check** *(R5S-O7)*: at startup the gateway reads `RLIMIT_NOFILE` and, unless configured `conntrack: notrack`, `/proc/sys/net/netfilter/nf_conntrack_max`. If either is below the requirement for the configured cap (see *Deployment requirements*), the gateway stays NotReady and logs which limit failed and the value it needs. `nf_conntrack_max` is node-wide; an absent file means conntrack isn't loaded and passes.
  - Pooling per-connection objects (conn structs, Noise cipher state, write buffers) is left to profiling in load test (a).
- **Graceful drain:** on SIGTERM, go NotReady and stop accepting, send `Reconnect{jitter_ms}` (default `jitter_ms` = 5s, configurable *(R4H-O3)*) in batches over about 60s, close the rest, then call `EpochEnded`. `terminationGracePeriodSeconds` must be longer than the drain window. An eviction (node upgrade, `kubectl drain`) goes through the same SIGTERM drain, and the gateway PDB allows one at a time *(R5S-O5)*, so the grace period also bounds how long one eviction holds up a node upgrade. Kicks still reach a draining gateway (`publishNotReadyAddresses`).

## Logic

- Stateless. Every node serves every RPC and can kick any gateway. Scales out horizontally (HPA on CPU); `minReplicas` covers the storm peak on its own *(R2-O7)*.
- **RPCs** *(R1-O12, R3-O1)*: `Login` (batch request, per-entry results streamed back *(R5S-O1)*), `Logout` (batch), `GatewayLease` (own channel and **separate worker pool**, so Login load can't starve it *(R1-O2)*), `EpochEnded` (`DEL`s the epoch's lease key, so the epoch is invalid at once and the sweeper's lease-key check passes immediately; the gateway has drained *(R4M-3)*), `Reconcile` (batch or streaming), `ReconcileEnd(gw, epoch, as_of_ms, tracked_conn_ids)`, `Revoke`.
- **Login:** verify the JWT locally (JWKS by `kid`), then run the Lua login script (EVALSHA, pipelined) with `recv_ms` and the token's `iat`, then do the follow-ups: SADD to the epoch index; if the result needs a kick, send it with the hardened Kick below.
  - **Per-entry results** *(R5S-O1)*: each entry's script runs and its result is streamed back on its own as soon as it finishes; Logic never waits for the whole pipeline before replying. Why: a pipeline across a Redis Cluster returns only when every node has answered, so one shard's failover would stall every batch and turn a 1/N outage into a login outage for all users.
  - **Kick after the result** *(R5S-O2)*: the Kick follow-up is queued **after** the entry's result is returned; a Login result never waits for a Kick.
- **GatewayLease** *(R3-O3)*: renew with `SET gwlease:gw:{epoch} 1 PX <TTL> GET` and update the `gwepochs` score. A nil previous value means the key was absent: reply `missing`; otherwise `live`. **Skew measurement** *(R4M-5)*: read Redis `TIME` on the lease key's shard and export `clock_skew_ms{gw} = redis_time − gw_now_ms` (RPC latency adds a few ms of noise); alert at |skew| > 500ms, half the budget. Node NTP metrics don't cover a managed Redis's clock; the lease RPC already runs every 10s per gateway on a dedicated, low-latency path.
- **Hardened Kick** *(R1-O8, R2-O6)*:
  - The **first attempt is always sent**, even when the target's lease looks expired (an epoch mismatch on the gateway makes a stray Kick harmless).
  - **Retries** (up to 3 over ~30s, on transport errors) happen only while the target `gw|epoch` lease is live. Logic caches lease state for ~1s; there are only a handful of epochs.
  - Kick RPCs are **fail-fast** (no `WaitForReady`).
  - **Deadline per attempt** *(R5S-O2)*: every attempt, first and retries, has its own deadline (default ~1s, configurable) covering connect and RPC. Why: on node loss the dead pod's DNS name still resolves to an IP that never answers, and a gRPC connect there otherwise blocks for up to 20s.
  - **Bounded queue per target** *(R5S-O2)*: Kicks are queued per target `gw|epoch` in a bounded queue (size configurable; sized in the plan) drained by a bounded number of workers per target; retries go through the same queue. When the queue is full, the attempt is dropped and counted (`kick_dropped{target_lease=live|dead}`); Reconcile is the backstop (race 8). This keeps R2-O6's always-sent first attempt, which protects a starved but alive gateway, while bounding its cost in a storm (~one Kick per relogin to the dead epoch).
  - On give-up, increment a metric labelled `target_lease=live|dead`. Reconcile is the backstop.
- **JWKS** *(R1-O10, R2-O7)*:
  - At startup, load the Ed25519 key set from the account service. If that fails, load it from a mounted ConfigMap (`jwks-fallback`) and keep retrying the account service on the normal refresh schedule.
  - Logic reports **Ready only once it holds a key set** from either source.
  - Refresh every 5 min. During a rotation every published key is accepted. If a refresh fails, keep the last good set and raise an alert. Alert while a pod runs on the fallback set.
  - **Unknown `kid`** *(R4H-O10)*: a JWT whose `kid` is not in the key set is **rejected at once** as `KEY_UNKNOWN`; Login never waits on a fetch (verification runs inside batches during a 20k/s storm). The rejection starts a background refresh if one is allowed:
    - **rate limit**: at most one unknown-`kid` refresh per **30s per Logic pod**, global across kids (a flood costs ~2 fetches/min per pod);
    - **single-flight**: concurrent unknown kids share one in-flight fetch;
    - **negative cache**: a kid still unknown after a refresh is remembered as unknown until the next allowed refresh, so a flood is rejected from memory;
    - **client side**: the client retries after 1–3s (retryable reject) and succeeds once the refresh has landed.
  - **Key rotation runbook:** a new signing key is published in the JWKS **at least one refresh interval (5 min) plus margin before** it signs any token, so every pod already holds it from its periodic refresh and an unknown `kid` means a broken runbook or a forged token *(R4H-O10)*; it is also published to the `jwks-fallback` ConfigMap before it signs any token; a key withdrawn because it was compromised is also removed from the ConfigMap. (A stale ConfigMap only matters while the account service is unreachable.)
- **Revoke** *(R1-O10, R2-O4, R3-OP2, PI-4)*: `Revoke(uid, scope = all | slot, before_ms, reason)`, called by the account service on a ban, a password change, "log out all devices" (scope=all) or "log this device out" (scope=slot). `before_ms` is on the token issuer's clock, in the unit of `tok_iat` (ms): the account service sets it when it decides the revocation and resends the same value on every retry, so the marker means exactly "tokens this issuer issued before `before_ms`" whatever the skew between the account service and Redis, and a retry never moves it *(PI-4)*. Logic rejects `before_ms ≤ 0` or more than 5 minutes ahead of Redis `TIME` with `INVALID_ARGUMENT` (not retryable; this catches a seconds-vs-ms bug). Both scopes work the same way:
  1. Write the marker field — `*` for scope=all, `appid:platform` for scope=slot — with a Lua script as `revoked_before_ms = max(stored, before_ms)` (idempotent), then `WAIT 1 <timeout>`. If no replica confirms, return a retryable error; the account service retries `Revoke` until it succeeds. **The marker is written synchronously: it must be confirmed before step 2 and before `Revoke` returns success**, so a lost Kick can't let Reconcile resurrect the session.
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
  - **The lease check is an observation** *(R4M-2)*: it can be stale when the compare-and-set runs. If rule 4 overwrites an entry whose gateway was only starved (lease key absent, gateway alive), that gateway's next renewal reports `missing` (guaranteed, because its lease key was absent), and its triggered pass applies rule 3 to its own newer session: the stored entry's gateway is live and the chunk entry is newer, so the newer session is written back and the older one kicked, within ~1 min. A lease observed live that expires right after is harmless: rule 3 decides by ordering, not liveness. The lease key stays the authoritative liveness predicate.
  - **Deadline scope** *(R3-O6)*: the deadline refuses only writes that create or replace a field (rules 3 and 4). Late TTL refreshes and `gwconns` SADDs are harmless and are not refused. A refused write returns `refused`; the gateway re-queues it in the current pass.
  - A compare-and-set that fails because the field changed is a definitive result with no write, reported as `unchanged` (not `refused`, so it is not re-queued), and no kick is sent; the next pass handles the entry.

  **Delete phase** *(R3-O1, R5S-O8)*: the Logic node that receives `ReconcileEnd` SSCANs `gwconns:gw:{epoch}`. For each uid it reads `sess:{uid}` and compare-and-deletes each field that points at this `gw|epoch` whose `conn_id ∉ tracked_conn_ids` and whose `login_ms < as_of_ms`. It also removes uids with no remaining field from `gwconns`. Why this is safe: a conn_id leaves the tracked set only after its Logout fence has held, so no Login or Reconcile write for it can still land; and a connection whose Login was received at or after `as_of_ms` is excluded by the `login_ms` check. The phase runs under a configurable rate limit (default ~10k uids/s, the sweeper's rate), ~20s per pass at 200k; ghosts are bounded by the cycle, not by seconds. `ReconcileEnd` replies when the phase completes. If the Logic node dies mid-phase, the RPC fails and the gateway's retry resends the same payload; the compare-and-deletes are idempotent, so a partial first run is safe.

  Cost: 200k entries every 10 min is ~330 Lua calls/s per gateway for the Reconcile writes, plus the delete phase's ~330 reads/s (SSCAN + `sess:` read, and a compare-and-delete only for ghosts): **~660 Redis ops/s per gateway, ~4k/s cluster-wide** at 6 gateways, periodic pace *(R5S-O8)*. A triggered pass is ~5k/s per gateway *(R2-O8)*; the delete phase is capped at ~10k uids/s whatever the pace.

## Redis schema (Cluster; per-user keys hash-tagged `{uid}`)

| Key | Type | Value / TTL | Purpose |
|---|---|---|---|
| `sess:{uid}` | HASH | field `appid:platform` → `gw|epoch|conn_id|device_id|conn_seq|login_ms|tok_iat`; `login_ms` is the gateway's `recv_ms` *(R1-O4)*; key TTL 30d, refreshed on login and by Reconcile | slot ownership record *(R4M-11)* |
| `rev:{uid}` | HASH | field `*` → `revoked_before_ms` for scope=all; field `appid:platform` → `revoked_before_ms` for that slot *(R3-OP2)*. Each field written as `max(stored, new)` and confirmed with `WAIT` *(R2-O4)*; key TTL = the `sess:{uid}` TTL (30d; not separately configurable), refreshed on every marker write *(R4M-1)*. A token's lifetime bounds only logins: Reconcile rule 1 checks connections whose token may be far older, so the marker must outlive the longest time a revoked connection can go without a pass processing it, not just the token | revocation markers (user-wide and per slot) |
| `gwlease:gw:{epoch}` | STRING | `PX <lease TTL>`, renewed every 10s with `SET ... PX <TTL> GET` (a nil previous value means `missing`) *(R3-O3)*. Lease TTL is configurable (default 30s) and must satisfy **TTL > measured shard failover time + one renewal interval** *(R2-O5)* | gateway instance is alive |
| `gwepochs` | ZSET | member `gw:epoch`, score = last renewal in ms | index of candidate epochs for the sweeper; **advisory**: written separately from the lease key, so a stale score never proves an epoch dead *(R4M-3)* |
| `gwconns:gw:{epoch}` | SET | uids; pruned by the Reconcile delete phase. One key per epoch, so all of a gateway's `SADD`s land on one shard; acceptable at the storm rate (~10k/s) *(R2-O9)* | cleanup hint (not atomic with sess, not needed for correctness) |
| `sweep:gw:{epoch}` | STRING | `SET NX PX 300000` | sweeper claim |

**A session is valid** only if its field exists **and** `EXISTS gwlease:{its gw}:{its epoch}`.

> The validity rule is defined here, but an efficient read path for consumers is deferred to sub-projects 2/3. They must **not** implement it as `HGETALL` + `EXISTS` per epoch *(R1-O12)*.

**Replication** *(R2-O4)*: Redis Cluster replicates asynchronously; a failover can lose ~1s of acknowledged writes. `sess:` and `gwconns` losses are repaired by Reconcile (rule 3 picks the right winner, rule 4 restores); lease losses are covered by the next renewal. `rev:{uid}` is the exception: nothing would repair its loss, so Revoke confirms every marker write, for both scopes, with `WAIT`.

### Login Lua (single slot, atomic)

Inputs: the new entry, `recv_ms`, token `iat`, `login_deadline` (default 5s). Steps:

1. Read `rev:{uid}` fields `*` and `appid:platform`. If `iat <` their max (the effective marker) → `REVOKED`; no write *(R1-O10, R3-OP2)*.
2. Read Redis `TIME`. If `now − recv_ms > login_deadline` → `EXPIRED`; no write *(R1-O4)*.

   **Order** *(R4H-OP1)*: revocation is checked before the deadline. Both write nothing, so the order never affects correctness; it only decides what a login that is both revoked and late reports. `REVOKED` first gives the client the final answer at once (drop the token, show the login screen); `EXPIRED` first would make it retry with the revoked token after 1–3s and only then see `REVOKED`. The cost is one read of two `rev:{uid}` fields on the same slot. Reconcile rule 1 follows the same order (the deadline refuses only writes, and rule 1 writes nothing).
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

**Clock contract** *(R1-O4, R4M-5)*: `|gateway clock − Redis TIME| ≤ 1s`, measured by `clock_skew_ms` on the lease path. Normal NTP skew (a few ms) is far below the time a person takes to switch devices. The 1s budget is kept: a larger one would delay every fenced Logout and still not cover larger clock steps. A violation degrades, never silently corrupts:

| Skew | Effect | Severity |
|---|---|---|
| Gateway **ahead** of Redis by > 1s | Fence (b)/(c) releases a Logout before a late Login or Reconcile write can land → a ghost of a closed connection | Bounded: the conn_id is no longer tracked, so the next pass's delete phase removes it (≤ 1 cycle). Logout is exact-match and the login table still orders writes, so no newer session is overridden. |
| Gateway **behind** Redis by ≳ `login_deadline` − RPC latency | Every Login via that gateway is `EXPIRED` | Login outage on that gateway. `EXPIRED` is a retryable reject and each attempt uses a new source port, so many clients land elsewhere, but it is a gateway-wide fault. |
| Two gateways skewed by more than the gap between two device logins | The wrong device wins a same-slot race (race 3, rule 3) | Spurious `KICKED_BY_OTHER_DEVICE`; no state corruption |

### Logout Lua

`HDEL` the field only if it still matches `gw|epoch|conn_id`. Because conn_id is never reused, this handles the delayed-disconnect race, including on the same gateway. Ordering against the connection's own Login and against any Reconcile restore of it is guaranteed by the gateway's Logout fence.

### Sweeper (goroutine in every Logic node)

Find `gwepochs` members whose last renewal is more than **2 × lease TTL** old (60s by default) *(R2-O5)*, or that sent `EpochEnded`. Claim each with `sweep:` SET NX and **record the epoch's `gwepochs` score at claim time** *(R3-O3)*. **Then check `EXISTS gwlease:gw:{epoch}`**: if the lease key exists, the epoch is alive whatever its score says (e.g. its `ZADD` failed while its `SET` succeeded); release the claim (`DEL sweep:`) and skip it *(R4M-3)*. The lease key is the authoritative liveness predicate, as in the validity rule. Run `SSCAN gwconns`, and for each uid run a Lua script that deletes only the fields whose `gw|epoch` match the dead epoch. Rate-limited to about 10k users/s.

- **Abort on renewal** *(R3-O3, R4M-3)*: between rate-limit batches, re-read the epoch's `gwepochs` score and check its lease key. If the score is newer than the claimed score **or the lease key exists**, the epoch has renewed: stop at once, without `UNLINK`ing `gwconns` and without removing the ZSET member, `DEL` the `sweep:gw:{epoch}` claim so it can't block a later legitimate sweep *(R3-OP6)*, and count `sweep_aborted`. The gateway's renewal already reported `missing` (the lease key was absent), so its triggered Reconcile restores whatever was deleted. Because every sweep starts with the lease key absent, a renewal that ends a wrongful sweep always reports `missing` *(R4M-3)*.
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
    - If a live gateway's renewal `SET` succeeds but its `ZADD gwepochs` fails, its score goes stale; the sweeper claims the epoch, finds the lease key present and skips it *(R4M-3)*.
13. **Two loops generating conn_ids at once:** each loop has its own counter, and the worker_id prefix keeps the IDs apart.
14. **Connection closes during a Reconcile pass** *(R2-O1)*: its Logout is held until the chunk containing it is acknowledged (fence c), or until the chunk's deadline has passed, so the Logout always lands after any restore. A connection that closed before its chunk was built is simply not in the pass.
15. **Write lost in a Redis failover, older session restored** *(R2-O4, R3-O4)*: if the field rolls back to an older session while the newer connection is live, Reconcile rule 3 compares by the login table's ordering, writes the newer entry back and kicks the older connection. The ordering includes the regression window, so a session that won through a counter regression keeps winning after a rollback. Ties go to the stored session, so two live sessions never overwrite and close each other.
16. **Login in flight, or Logout dropped, around a Reconcile pass** *(R3-O1)*: a connection whose Login is in flight is tracked, so it is in `tracked_conn_ids` and the delete phase leaves its field alone; a Login received after `as_of_ms` fails the `login_ms < as_of_ms` check. A connection whose Logout was dropped has left the tracked set, so its ghost is deleted at the end of the same pass — whether the Logout was dropped before or after its chunk was sent.
17. **Slot-scoped logout whose Kick is lost** *(R3-OP2)*: the connection stays ONLINE on its gateway and its `sess:` field is gone. The confirmed per-slot marker makes the next Reconcile pass return the entry as `not_owner` (`REVOKED`, rule 1) instead of restoring it (rule 4), so the device is closed within one cycle. The user's other slots are unaffected.
18. **Client Logout during an in-flight Reconcile chunk** *(R4H-O8)*: the Logout RPC waits for fence (c), so Logic processes the chunk (rule 2 refresh, or rule 4 restore) before the Logout deletes the field; no ghost remains.

Note: when the same device reconnects, `conn_seq` also replaces any ghost entry. Reconcile covers devices that never return.

## Consistency contract *(R2-O8)*

Bounds consumers (sub-projects 2/3) may rely on when sizing caches and fallbacks:

| Divergence consumers may see | Cause | Bound | Repaired by |
|---|---|---|---|
| Ghost: valid entry, but the connection is gone | Logout dropped | ≤ 1 Reconcile cycle (~10 min + jitter): a Logout dropped at any point before a pass ends is repaired at the end of that pass *(R3-O1)* | Reconcile delete phase (`ReconcileEnd`) |
| Old owner still connected after being replaced or revoked (either scope) | Kick lost | ≤ 1 cycle (for revocation: provided some pass processes the connection within the 30d marker TTL *(R4M-1)*) | Reconcile `not_owner` / rule 1 *(R3-OP2)* |
| Entry of a dead gateway | Gateway death | invalid after the lease TTL (30s); deleted after 2 × TTL, plus up to one TTL of guard wait *(R3-O2)*, plus sweep time | validity rule, then sweeper |
| Live connection missing from Redis | Write lost in failover, wrongly swept, or overwritten on a stale lease observation *(R4M-2)* | ≤ 1 cycle; ≤ ~1 min after a `missing` lease reply (the owner's, for a stale observation) | Reconcile restore (rule 4), or rule 3 in the owner's triggered pass |
| Healthy session reads invalid | Logic or lease shard down longer than the TTL | the length of the outage | lease renewal resumes *(R2-O5)* |

## Storms, drain, failures

- Client SDK: see *Client SDK rules* (0–20s jitter after losing an established connection, 0–1s after a local network change or app foreground, `[0, jitter_ms]` after `Reconnect`, 0.5–1.5s for pre-handshake closes up to 10 in a row, 1–3s for the other retryable rejects (`Busy`, `EXPIRED`, `KEY_UNKNOWN`, close before `LoginResp`) up to 5 in a row *(R5S-O4)*, exponential backoff, windows adjustable through `LoginResp` *(R5S-O6)*, 10s Pong timeout, obey `retry_after`).
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
| Redis shard failover *(R2-O4, R2-O5, R5S-O1)* | Up to ~1s of acknowledged writes may be lost: `sess:`/`gwconns` losses are repaired by Reconcile, `rev:` is protected by `WAIT`. The lease survives because its TTL exceeds the measured failover time plus one renewal interval. Logins on the failing shard stall or come back `EXPIRED` for the failover time; logins on other shards are unaffected (per-entry results). |
| Zone loss (Redis) *(R5S-O9)* | Masters in the lost zone fail over to their replicas in the surviving zones (placement rule in *Deployment requirements*). Logins on those shards stall or come back `EXPIRED` for the failover time; other shards are unaffected. Lost writes are repaired as for a shard failover. Revoke may return retryable errors for shards whose replica was in the lost zone until a replica is rebuilt; the account service keeps retrying. |
| Gateway process crash / pod kill | Clients see RST/FIN and reconnect within the 0–20s jitter. The lease expires within the TTL (30s); the sweeper cleans up after 2 × TTL (60s), plus up to one TTL if its guard clock has just started. One Kick attempt per relogin reaches the dead epoch, with no retries *(R2-O6)*. |
| Node loss / silent partition | Clients detect it by Pong timeout (≤ one heartbeat interval + 10s) and then reconnect. Server side as for a crash. Kicks to the lost pod time out after ~1s each and never delay relogins; overflow is dropped *(R5S-O2)*. After the node-loss threshold (2 min) the node is tainted `out-of-service` and `gw-k` is recreated elsewhere, restoring N within ≈ 2–3 min *(R5S-O3)*. |
| Several gateways lost at once (zone) *(R2-O3)* | Best effort: survivors fill to the connection cap; excess clients are closed at accept and retry after 0.5–1.5s (up to 10 times, then backoff) until the lost pods are rescheduled *(R3-O5, R5S-O4)*. Stale epochs are swept because the survivors keep renewing. |
| Kick can't reach the gateway *(R2-O6)* | Retried (up to 3 times over ~30s) while the target lease is live; otherwise one attempt. Then the Reconcile backstop closes the connection within one cycle. |
| Handshake flood | The global handshake concurrency limit and token bucket protect established connections; excess handshakes are closed and counted. |
| Slow clients | `HANDSHAKE` / `AUTH_PENDING` 5s timeouts free their slots. |
| Bad frame / decryption failure / frame too large | Close immediately. |
| Whole-population reconnect *(R5S-O6)* | Best effort, no SLO (see *Success criteria*). Handshake bucket, connection cap and Login-queue cap close or reject the excess cheaply; clients retry under the two retry counters, then back off. Expected recovery ≈ users ÷ admitted login rate plus the backoff tail. |
| Rolling restart behind a flow-hash LB *(R5S-O4)* | The pods not yet restarted fill to the cap; rejected clients retry pre-handshake every 0.5–1.5s up to 10 times and land on a pod with room. On a least-connections LB the restarted pods fill instead. |
| Node upgrade / voluntary eviction *(R5S-O5)* | The gateway PDB lets one gateway drain at a time; a further eviction waits until it is Ready again, and none proceeds while a gateway is already down. Logic never drops below `minReplicas`. |

## Interface constraints for later sub-projects

- **Ownership rule** *(R1-O8)*: a connection may act as the user (send messages, perform user operations) only while its `gw|epoch|conn_id` owns its slot in `sess:{uid}`. Sub-project 2 must enforce this on the send path (a cached ownership check is acceptable). Sub-project 1 only documents it and provides the data it relies on (session entries plus Kick/Reconcile delivery).
- **Session-validity read path** *(R1-O12)*: deferred to sub-projects 2/3; must not be `HGETALL` + `EXISTS` per epoch.
- **Validity depends on Logic and the lease shard** *(R2-O5)*: if Logic, or the Redis shard holding a gateway's lease key, is unavailable for longer than the lease TTL, that gateway's sessions read as invalid although their connections are healthy. Sub-projects 2/3 must not trigger mass offline pushes on validity alone; the mitigation is theirs to design.
- **Consistency bounds**: see *Consistency contract* *(R2-O8)*.
- **Lazy repair** in sub-project 3 (a push that gets "unknown conn" leads Logic to compare-and-delete) is optional; correctness no longer depends on it *(R1-O3)*.

## Observability

Prometheus metrics:
- connections by state for each gateway; tracked-but-closed entries awaiting Logout; handshakes/s; **connection skew** (max / mean ONLINE per gateway) *(R2-O2)*;
- logins by result: `ok`, `stale`, `replaced`, `kicked`, `superseded`, `expired`, `revoked`, `key_unknown` *(R4H-O10)*, `busy`, `error`; plus `conn_seq_regressed`;
- end-to-end login latency p50/p99; gRPC and Redis script latency;
- **relogin latency** histogram: `offline_ms` + (login OK − `recv_ms`), labelled by `reconnect_reason` *(R1-O7)*;
- **dead-epoch cross-check** *(R1-O7)*: when a relogin finds the old entry pointing at a dead epoch, record `login_ms − that epoch's last lease renewal` (time from gateway death to relogin, ±10s, no client data needed);
- lease renewal failures; lease replies with `missing`; `clock_skew_ms` per gateway, alert at |skew| > 500ms *(R4M-5)*;
- admission rejects by reason: `at_capacity` *(R2-O2)*, `handshake_rate`, `login_queue`, `pending_mem`, `handshake_timeout`, `auth_timeout`, `bad_version` *(R4H-O1)*, `noise_unknown_key` *(R4G-O6)*;
- Kick attempts, retries and give-ups, give-ups labelled `target_lease=live|dead` (alerts use `live` only) *(R2-O6)*; revocation kicks; Revoke `WAIT` failures *(R2-O4)*;
- Reconcile: entries refreshed, restored, `unchanged` *(R3-OP7)*, `refused` and re-queued, re-queue limit reached *(R3-O6)*, `not_owner` by reason, rule-3 overrides (chunk entry newer) *(R2-O4)*; `ReconcileEnd` deletes, `tracked_conn_ids` size, retries and drops *(R3-O1, R3-OP4)*; passes cancelled; cycle duration, by pace *(R2-O8)*;
- sweeper progress; `sweep_aborted` *(R3-O3)*; **mass-stale guard holds** (also an alert), including time held by the `renewing_since` wait *(R3-O2)*;
- JWKS refresh failures and unknown-`kid` refreshes; pods running on `jwks-fallback` (also an alert) *(R2-O7)*;
- **Kick queue** *(R5S-O2)*: queue depth per target `gw|epoch`, `kick_dropped{target_lease=live|dead}`, attempt latency;
- **Reconcile delete phase** *(R5S-O8)*: duration and uids scanned per pass, by pace;
- **node-loss recovery** *(R5S-O3)*: time from node NotReady to replacement gateway Ready, alert above the target;
- **limits** *(R5S-O7)*: measured `RLIMIT_NOFILE` and `nf_conntrack_max` as gauges, the node's `nf_conntrack_count` where readable (alert at 80% of max), and a Readiness failure reason when the self-check fails;
- **reconnect-after-idle rate** (shows NAT timeouts shorter than 150s); Go GC pause and heap.

Logs: `slog` lines with `uid` and `conn_id`; a request ID passed in gRPC metadata.

## Verification

### Test execution model *(R4H-O11)*

| Tier | Host | Runs | Contents |
|---|---|---|---|
| **1a** | dev machine (2 cores / 2GB / 32GB) | every change | unit tests (with `-race`), Lua tests, small load probe (≤ 5k connections) |
| **1b** | dev machine | before every commit / merge | race integration suite under the test timing profile |
| **2** | dedicated host (12 cores / 26GB / 2TB) | before merging changes to the gateway, Logic or Lua; on demand | Redis failover; load tests (a), (b1), (b2), (b-stress), (c), storm, floods, slowed Logic; all single-host SLO measurements |
| **3** | cloud cluster (`target=cloud`), or the Tier 2 host (`target=local`) | before a release | 1M rolling restart; NLB / drain behaviour; multi-gateway storm on a real network |

- **Test timing profile** (Tier 1b): every interval and TTL is configurable. The profile scales the time-based ones down ~10× while keeping their ratios: lease TTL 3s, renewal every 1s, guard windows, periodic Reconcile, and the 90s outages become 9s. `login_deadline` and the 1s skew allowance are **not** scaled; they are tied to real RPC latency. Token lifetime is set per scenario through devtoken, and the `sess:`/`rev:` TTL stays at 30d. Gateway and Logic refuse to start if a profile breaks a required ratio (e.g. lease TTL ≤ failover time + one renewal interval).
- **CPU isolation** (Tiers 2 and local 3): gateway, loadgen, and Logic + Redis are pinned to disjoint cores with cgroups / `taskset` (e.g. 6 / 4 / 2), because loadgen's Noise handshakes cost about as much as the gateway's. Capacity is reported **per gateway core count**, so the 600k ceiling translates to production node sizes. Host prep provides ≥ 10 source IPs for loadgen at 600k (~64k ports per source IP → destination pair) and sets `nr_open`/ulimit and conntrack (raised, or disabled on the test interface).
- **Automation contract** (Tiers 2 and 3): fully scripted and non-interactive.
  - One entry point per scenario, e.g. `make test-tier2 SCENARIO=b1`, `make test-tier3 SCENARIO=rolling TARGET=local`; no prompts.
  - Idempotent host prep (sysctl, ulimit, IP aliases, CPU pinning); teardown always runs.
  - Machine-readable results (JSON) with pass/fail against the SLO thresholds and a matching exit code; artifacts: pprof, a metrics snapshot, loadgen latency distributions.
  - **Host guard**: Tier 2 and 3 scripts check `nproc`, memory and an explicit `GOIM_TEST_TIER`, and refuse to run on a smaller host. They must not run on the dev machine.
- **Tier 3 target** (configurable, because no cloud cluster exists yet): `TARGET=cloud | local`.
  - **`cloud`**: the full scenario on the cloud Kubernetes cluster with the real NLB, at 1M connections.
  - **`local`**: the same scenario on the Tier 2 host, on a local Kubernetes (kind or k3s) so the StatefulSet, readiness, `publishNotReadyAddresses` and `terminationGracePeriodSeconds` are exercised, with a local L4 load balancer that follows Kubernetes endpoints (e.g. kube-proxy in IPVS mode) standing in for the NLB, so a NotReady pod leaves rotation *(R4M-7)*. Load is scaled by `TIER3_SCALE` (default 0.15): total connections = 1M × scale and the per-gateway cap = 250k × scale (6 gateways, 150k total, cap 37.5k at the default), which keeps the N−1 and cap arithmetic intact. All assertions that don't depend on the real NLB or cloud network run unchanged: no gateway exceeds the cap; rejected clients log in elsewhere within the SLO; every client on a draining pod receives `Reconnect` before close; Kicks reach a draining pod.
  - **Not covered locally**, and listed as such in the JSON result (`"target": "local", "scale": …, "not_covered": [...]`): the real NLB's RST/FIN pass-through, flow-hash spreading and deregistration delay; cloud network latency; the 1M absolute scale. Until a cloud cluster exists, a `local` run is the accepted Tier 3 evidence, with these gaps stated in the release notes.

- **Unit tests** (Tier 1a):
  - frame codec (partial frames, several frames at once, fuzzing), Noise handshake against a test client; a tampered header fails AEAD, a nonzero flag closes, an unknown version is closed before the handshake *(R4H-O1)*; a handshake succeeds with either of two configured key IDs, an unknown key ID is closed before the handshake and counted, and a tampered `key_id` fails the handshake through the prologue *(R4G-O6)*;
  - the connection state machine: kick-pending in `AUTH_PENDING`, both timeouts (an out-of-range value refuses to start; a Login queued longer than the `AUTH_PENDING` timeout is not closed by it *(R4H-O7)*), the Logout fence with all three conditions, the connection cap at accept (counting sockets, not entries awaiting Logout), and tracked-set membership until the Logout is acknowledged or dropped;
  - Reconcile pass control: `refused` entries are re-queued at most 3 times and skipped if no longer ONLINE; a cancelled pass sends no further chunks and no `ReconcileEnd` *(R3-O6, R3-O1)*; each chunk holds IDs of one loop only and partitions are drawn round-robin; under `-race`, a pass runs while loops accept and close connections with no race report, and every tracked fragment is taken after `as_of_ms` *(R4H-O9)*;
  - per-loop table pre-allocation: inserting `cap / numLoops` entries into a table sized by the rule causes no map growth (checked with `testing.AllocsPerRun`) *(R4H-O4)*;
  - loopStates *(R4H-O5)*: the first `OnOpen` on each loop allocates no table (`AllocsPerRun`); a loop seen beyond `numLoops` panics; the gateway is not Ready before its lease and Logic connections are up;
  - timers *(R4H-O6)*: a timer never fires before its deadline (round-up at tick boundaries); a heartbeat entry re-arms on activity and fires 310s after the last frame;
  - the connection state machine runs against a **fake transport**, so the core does not depend on TCP or Noise *(R4G-O5)*;
  - conn_id generation, and the **gnet loop-ownership test under `-race`** (`OnOpen`, `OnClose`, the `Execute` callback and wheel expiry callbacks all see the same loop for a given connection);
  - clock skew *(R4M-5)*: `clock_skew_ms` computation from `gw_now_ms` and Redis `TIME`, and the 500ms alert threshold;
  - sweeper guard: `renewing_since` resets when no other epoch renewed within 15s and is not reset by a 10s rhythm with jitter *(R3-O2)*;
  - JWKS unknown `kid` *(R4H-O10)*: a flood of unknown kids causes at most one fetch per 30s per pod, and concurrent requests share one fetch;
  - **SDK** *(R3-O5, R4H-O2, R4H-O10, R5S-O4)*: pre-handshake closes retry after 0.5–1.5s and fall back to exponential backoff after 10 in a row; `Busy`, `EXPIRED` and `KEY_UNKNOWN` retry after 1–3s and share one count of 5 in a row, then fall back; an outcome of one kind resets the other counter, and both reset on a successful login. Logout, account switch and app close each cancel a pending retry. A network change during the 0–20s wait reconnects within 1s; a network change during a `retry_after` wait does not shorten it; after `Reconnect`, the first attempt falls within `[0, jitter_ms]` and carries the drain reason *(R4H-O3)*. A close after the handshake with no `LoginResp` or `Error` is retried after 1–3s and counts toward the 5 *(R4H-O7)*. Each jittered wait (0–20s, 1–3s, 0.5–1.5s, 0–1s, `[0, jitter_ms]`), sampled ~10k times, passes a uniformity bucket check *(R4G-O12)*. Windows from `LoginResp` are used and persisted across app restarts; out-of-range values are clamped; absent fields keep the current value *(R5S-O6)*;
  - **Logic per-entry Login results** *(R5S-O1)*: with one entry's script delayed, the other entries' results arrive before it;
  - **Kick** *(R5S-O2)*: against a target that never answers (blackholed address), Login results return without waiting for the Kick, each attempt ends within its deadline, and a full per-target queue drops and counts instead of growing;
  - **delete phase** *(R5S-O8)*: a delete phase over N uids takes ≥ N ÷ rate and deletes the same fields as an unthrottled run; a `ReconcileEnd` interrupted mid-phase and retried leaves the same result as one uninterrupted run;
  - **limits self-check** *(R5S-O7)*: Readiness fails for a `nofile` or `nf_conntrack_max` below the cap's requirement, passes with `conntrack: notrack`, and passes when the conntrack file is absent.
- **Lua tests** against a real Redis in Docker (Tier 1a):
  - one table test for each row of the login table, including `SUPERSEDED`, `EXPIRED`, `STALE` (low conn_seq inside the window is rejected) and the regression row (low conn_seq outside the window is accepted);
  - `REVOKED` (token iat before the marker is rejected); the revocation marker write is `max(stored, new)` and idempotent; the effective marker is the max of the `*` and slot fields, and a slot marker blocks login on that slot only *(R3-OP2)*; a login that is both revoked and past the deadline returns `REVOKED` *(R4H-OP1)*; every marker write sets the `rev:` key TTL to the configured `sess:` TTL, and a rewrite refreshes it *(R4M-1)*;
  - the logout compare-and-delete;
  - the Reconcile compare-and-set (including refusal after `sent_ms + login_deadline`, and a failed compare returning no write) and the `ReconcileEnd` delete (only fields of this epoch, conn_id not tracked, `login_ms < as_of_ms`) *(R3-O1)*;
  - Reconcile rule 3 after a simulated lost write: restore an older field by hand while the newer connection is live; Reconcile keeps the newer connection and kicks the older one *(R2-O4)*;
  - Reconcile rule 3 with a regressed counter *(R3-O4)*: a regressed-counter session (low conn_seq, login more than `login_deadline` after the stored one) has its write rolled back by hand to the older high-conn_seq entry; Reconcile keeps the regressed session and kicks the older one. Equal `login_ms` on two devices: the stored session wins and no kick loop occurs;
  - lease renewal with `SET ... GET`: absent key → `missing`, present key → `live` *(R3-O3)*;
  - the sweeper deleting only dead-epoch fields; the sweeper skipping an epoch whose lease key exists although its score is stale *(R4M-3)*.
- **Race integration tests** (`deploy/compose`: Redis, Logic, **4 gateways** and a scripted client; Tier 1b, test timing profile) *(R3-O7)*:
  - races 1–6, 10, 14 and 16 above; race 6 in both orders (Logout before Login and Login before Logout, with injected Logic delays): no ghost after the fence; a Login with no definitive reply caused by a Logic crash: the Logout waits for the fence;
  - race 14 *(R2-O1)*: a connection closes after its chunk is sent but before Logic processes it (injected Logic delay); no ghost remains after the Logout;
  - race 18 *(R4H-O8)*: a client sends `Logout` while its chunk is delayed in Logic; it gets `LogoutResp` immediately and no ghost remains;
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
  - clock skew *(R4M-5)*, with an injectable fake clock in the gateway (the skew allowance is not scaled by the profile): offset one gateway by +2s: the skew alert fires, and a ghost left by a late Login write is removed by the next pass's delete phase; offset one gateway by −5s: its Logins are `EXPIRED`, and the scripted client retries and logs in through another gateway;
  - stale lease observation *(R4M-2)*: starve G42's lease (key removed) while its newer session S2 is stored; deliver G17's older entry S1 with its Kick dropped: rule 4 overwrites S2, then G42's triggered pass restores S2 and kicks S1 within the scaled bound;
  - lease key authoritative *(R4M-3)*: make one gateway's `ZADD gwepochs` fail for 9s (the profile's 90s) while its `SET` succeeds: nothing is swept; remove a lease key mid-sweep without changing the score, then re-add it: the sweeper aborts; an epoch that sent `EpochEnded` is swept without waiting for its TTL;
  - Reconcile: a ghost entry is removed within one cycle; a missing entry is restored; a newer session on another gateway is never overridden; a dropped Logout is repaired within one cycle;
  - Kick: a dropped Kick to a live gateway is retried and then caught by the next Reconcile *(R2-O6)*; a Kick during `AUTH_PENDING` closes the connection when the Login reply returns; a draining gateway still receives Kicks;
  - Revoke closes connections on two slots; a Revoke whose Kick is dropped is caught by the next Reconcile; a Revoke whose `WAIT` fails returns an error, and the retry is idempotent *(R2-O4)*; a retried `Revoke` with the same `before_ms` leaves the marker unchanged, a token whose `iat` is ≥ `before_ms` logs in after the revoke, and a `before_ms` 10 minutes in the future is rejected *(PI-4)*;
  - revocation outlives the token *(R4M-1)*: devtoken issues tokens with a 2s lifetime (shorter than the profile's 9s outage, so the old "token lifetime + margin" TTL would expire during it); cut a gateway off from Logic for 9s, Revoke one of its connections with the Kick dropped, restore connectivity: the triggered pass closes the connection with `REVOKED` and does not restore it;
  - slot-scoped Revoke *(R3-OP2)*: with its Kick dropped, the next Reconcile closes the connection through `not_owner` (`REVOKED`) and does not restore the session; another slot of the same user is unaffected; a slot Revoke whose `WAIT` fails returns an error and sends no Kick, and the retry is idempotent;
  - key rotation: both `kid`s are accepted during the overlap; a key that signs before Logic has fetched it gives `KEY_UNKNOWN`, then a successful login after the background refresh *(R4H-O10)*;
  - JWKS *(R2-O7)*: Logic starts with the account service down, becomes Ready from the `jwks-fallback` ConfigMap and accepts tokens; a Logic pod with no key set from either source never becomes Ready.
- **Redis failover test** (Tier 2) *(R2-O5)*: fail over the shard holding the lease keys and measure the time; the lease must survive it at the configured TTL. During a single-shard failover, logins for uids on the other shards keep their normal p99 *(R5S-O1)*. Also fail over every master of one zone at once (one master with 3 masters, two with 6), together with the replicas in that zone; record failover time and the share of logins affected; the lease must survive at the configured TTL *(R5S-O9)*.
- **Load tests** (`cmd/loadgen`, multiple source IPs; loadgen records detection time and login-success time per client and computes the SLO distribution directly; Tier 2 unless marked Tier 3):
  - (a) 600k connections on one gateway (ceiling), with GOMEMLIMIT/GOGC tuning;
  - (b1) SIGKILL a 200k gateway: 99% logged in again within 30s of `T_death` (loadgen records the SIGKILL time) *(R4M-17)*; Kick attempts to it are about one per relogin, with no retries *(R2-O6)*; per-shard Redis CPU and p99 recorded, with the shards holding `gwconns` keys marked *(R4M-9)*;
  - (b2) iptables DROP on a 200k gateway (silent node loss): 99% logged in again within 30s of client detection; Login p99 is unaffected by Kicks to the dropped gateway, and Logic's goroutine count stays bounded *(R5S-O2)*;
  - (b-stress) kill a 600k gateway: measured, no SLO; includes a 600k-member `gwconns` `SSCAN` + `UNLINK` *(R4M-9)*;
  - `gwconns` shard load *(R4M-9)*: a triggered Reconcile pass at the cap, with per-shard Redis CPU and p99 recorded. The Tier 2 Redis Cluster runs ≥ 3 masters on the Logic + Redis cores; absolute numbers are distorted by core sharing, so the pass criterion is relative: the `gwconns` shard's p99 stays within 2× the other shards' p99;
  - (c) 150s heartbeat CPU cost at 200k and at 600k, plus **60s at 200k**, so the cost of lowering the interval is known if `reconnect-after-idle` forces it *(R4M-6)*;
  - storm with Logic held at `minReplicas` (no HPA scaling): the SLO is met *(R2-O7)*;
  - **(Tier 3)** rolling restart of all gateways at 1M total (`TARGET=local`: 1M × `TIER3_SCALE`): no gateway exceeds the cap, and clients rejected at the cap log in elsewhere within the SLO (using the 0.5–1.5s pre-handshake retry), and fewer than 1% of clients reach exponential backoff; per-pod connection counts are recorded at each step, and on a least-connections LB the restarted pods end within 10% of the mean *(R2-O2, R3-O5, R5S-O4)*. Drain assertions *(R4M-7)*: after NotReady plus the LB's deregistration delay, the draining pod accepts no new connections (`TARGET=local`: the endpoint-following LB is checked, the real NLB deregistration delay is `not_covered`); every client on the draining pod receives `Reconnect{jitter_ms}` before its connection closes; Kicks sent to the draining pod during drain succeed (`publishNotReadyAddresses`);
  - storm: the other gateways never miss a lease renewal;
  - handshake flood: garbage handshakes from many sources leave heartbeats answered on time, and a legitimate 20k/s storm is not rejected by the bucket;
  - slow-client flood: slots are freed by the timeouts and legitimate clients get no `Busy`;
  - **whole-population reconnect** *(R5S-O6)*: drop every connection on all gateways at once (restart the local LB, or RST from loadgen in one instant); measured, no SLO: time until 99% are logged in again and the share of clients reaching backoff; heartbeats of already-reconnected clients are answered on time and no component restarts. The same with every client raising a network-change event at once;
  - **(Tier 3)** node loss *(R5S-O3)*: stop a gateway's node (not just the pod): the node is tainted after the threshold and `gw-k` is Ready on another node within the target (`TARGET=local`: stop the kubelet on a k3s agent node; `not_covered` if the local cluster can't simulate it);
  - **(Tier 3)** disruption budgets *(R5S-O5)*: `kubectl drain` two gateway nodes in parallel: the second eviction waits until the first gateway is Ready again, and at no point are two gateways unavailable; a drain of Logic nodes never leaves fewer than `minReplicas` Ready;
  - **(Tier 3 `cloud`)** LB limits *(R5S-O7)*: the LB's idle timeout and connection limits are recorded in the JSON result, and the run fails if they violate *Deployment requirements*; `TARGET=local` lists them as `not_covered`;
  - Logic slowed down: the Login-queue cap triggers `Busy` and clients retry within the SLO; with the queue cap sized by the rule above, logins are rejected as `Busy`, not `EXPIRED` (`expired` stays near zero) *(R4H-O2)*.

## Out of scope (later sub-projects)

Messaging (seq, dedup, MongoDB) · push pipeline (RocketMQ, APNs/FCM) · social feed · QR-code login for web · adaptive heartbeat on the client · account service (its events reach us only through `Revoke` and the JWKS) · send-path ownership enforcement (sub-project 2) · session-validity read path and its mitigation for leases that read invalid during outages (sub-projects 2/3) · active connection rebalancing across gateways (sub-project 5) · sizing for zone loss · proxy protocol and real client IP (web phase) · device_id/user_id-keyed abuse limiting.

## Open points

Rev3's open points 4 (what the connection cap counts), 5 (overlapping Reconcile passes) and 6 (which Reconcile writes the deadline refuses) were confirmed by the third review and are part of the body (*Admission control*, *Reconcile*, *Reconcile handling*). Rev4's open point 2 (slot-scoped Revoke) is resolved by the per-slot revocation marker, and its open points 4–7 are confirmed and folded into the body (*R3-OP2*, *R3-OP4*–*R3-OP7* in the rev3 dispositions).

The first open point rev4 carried (Reconcile vs. lease check atomicity) is closed by *R4M-2*: the lease check is an observation, and a write made on a stale one is corrected by the owner's triggered pass (*Reconcile handling*).

The other open point rev4 carried (order of `REVOKED` vs `EXPIRED` in the login Lua) is decided by the user *(R4H-OP1)*: revocation first, with the reasoning in *Login Lua*.

Rev5 had no open points. Rev6 leaves two choices to the plan, both recorded in the rev5 dispositions: whether Reconcile chunks also stream per-entry results (not required: a slow shard there only delays chunk resolution and causes re-queues, *R5S-O1*), and the sizes of the per-target Kick queue and the SDK's clamp ranges (*R5S-O2*, *R5S-O6*).

No open points remain in this revision.
