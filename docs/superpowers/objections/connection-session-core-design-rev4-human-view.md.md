O1: Client Outer frame:
+---------+---------+---------+---------+---------+
| Version |  Event  |  Flags  |  Length | Payload |
| (1 Byte)| (2 Byte)| (1 Byte)| (2 Byte)| (N Byte)|
+---------+---------+---------+---------+---------+

Payload:
+---------+---------+
| req_id  |   body  |
| (4 Byte)| (N-4 B) |
+---------+---------+

O2:Login errors:
|Code||Client Action|
|EXPIRED|abandon the stale Login and retry a brand-new Login with exponential backoff plus jitter for at most 3–5 attempts, stopping immediately on logout, account switch, or app close|

O3: Client SDK Reconnect Rule
After a server-initiated mass disconnect or upon receiving Reconnect{jitter}, wait a random 0–20s before the first attempt.
After a client-local network change or app foreground, attempt immediately (or with 0–1s jitter), then use exponential backoff + jitter, obeying retry_after.

O4: Resource Pre-allocation
Per-loop connection tables should be pre-allocated with a capacity of maxConnsPerGateway / numLoops * 1.5 to avoid runtime map growth.

O5: Resource Pool
Lazy worker_id allocation defers heavyweight resource creation to runtime, causing first-request latency. Pre-initialize a resource pool and acquire resources on demand instead of binding them to worker_id.

O6: Timers
A timing wheel drives all per-connection timers (heartbeat, login deadline, retry backoff). System-level long-period timers (Reconcile, sweeper) use separate mechanisms.

O7: AUTH_PENDING timeout
The AUTH_PENDING timeout (handshake complete → LoginReq received) SHOULD be configurable per gateway. Default: 5s. Allowed range: 1–30s. On expiry, the connection closes silently.

O8: Logout fence
Client-initiated Logout is handled synchronously and is not subject to the Logout fence. The fence applies only when the gateway must emit a Logout on behalf of a closed connection.

O9: Reconcile Data Race
If the per-loop map were accessed concurrently by other goroutines, any traversal (e.g., Reconcile) would race with the loop goroutine. To eliminate this data race, Reconcile traversal must be coordinated as follows:
A coordinator (any Logic node or a dedicated gateway goroutine) starts a pass by recording as_of_ms = now().
It posts a scan task to every event loop via gnet.Execute.
Each loop scans its own map[conn_id]*conn in slices, sends chunks, and collects its tracked_conn_ids fragment.
The coordinator waits for all loops to finish, merges fragments (no dedup needed), and sends ReconcileEnd{as_of_ms, tracked_conn_ids}.
All map access happens on the loop thread; no locks are needed.
For a triggered pass, the same flow runs with a fresh as_of_ms


O10: JWKS Refresh
A JWT with an unknown kid triggers at most one rate-limited refresh before it is rejected. The refresh is rate-limited globally per Logic pod (e.g., at most once every 5 minutes), so a flood of unknown kids cannot trigger repeated refreshes.

