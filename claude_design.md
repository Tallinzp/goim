IM SERVER ARCHITECTURE PLAN (v2)
================================

BACKGROUND
- Previous C++ version: 2.6M TCP connections (libevent TCP, libuv/libwebsockets WS)
- New version: Go, target 1M users, new job

PROTOCOL  [NEW]
- Length-prefixed binary frames (e.g. 4-byte length + protobuf body)
  - TCP is a stream: OnTraffic may deliver partial or multiple frames
  - Gateway must buffer and split frames before dispatching
- Every client message carries a client_msg_id (UUID), used for dedup on retry
- App-level heartbeat (e.g. ping every 30-60s) because mobile NATs drop idle
  connections faster than TCP keepalive notices; missed N pings = dead session
- TLS: terminate at the gateway or an L4 LB; enable session resumption
  (full handshakes are the CPU cost that matters during reconnect storms)

GATEWAY LAYER
- gnet event-loop pool (read + write), ~NumCPU loops, no goroutine per connection
- Writes via conn.AsyncWrite(), buffered per connection
- OnTraffic never blocks: decode the frame, then dispatch to a bounded
  logic worker pool
- [NEW] Backpressure: when the queue is full, reject with a "busy" frame or
  disconnect the abusive connection (never drop silently)
- [NEW] Per-connection rate limiting (anti-spam / anti-flood)
- [NEW] Graceful drain for deploys: stop accepting, tell clients to reconnect
  elsewhere with jitter, then close. This avoids a thundering-herd reconnect.

PRESENCE (Redis)  [CHANGED]
- Key per user -> SET of sessions {device_id, gateway_node, worker, conn_id}
  (multi-device, not a single location)
- Session entries carry a TTL refreshed by heartbeat, so a crashed gateway's
  sessions expire on their own
- On gateway boot: clear any stale sessions that name this node

SEQUENCE / IDs  [NEW]
- Per-conversation seq assigned by Redis INCR on key conv:{id}:seq
  (atomic, fast; persist the high-water mark so a Redis loss can't reuse seqs)
- Dedup: (sender, client_msg_id) is checked before a seq is assigned, so a
  client retry never creates a duplicate message

MESSAGE FLOW (1-to-1)
  Gateway -> c2c logic -> auth -> dedup check -> assign seq
  -> persist to MongoDB (source of truth)
  -> ACK to sender {client_msg_id, seq, server_ts}           [NEW]
  -> Redis lookup: recipient's session set
  -> RocketMQ topic per gateway node -> push node -> gateway -> each device
  -> Also push to sender's OTHER devices (multi-device sync)   [NEW]
  -> No online sessions: mobile push via APNs/FCM             [NEW]

DELIVERY RELIABILITY  [NEW — resolves the reconnect race]
- Because data is persisted first, a failed push loses nothing
- Client tracks last_seq per conversation. If it receives seq N+2 while
  holding N, it pulls the gap (N+1) from the server
- On reconnect: client sends its cursors and pulls everything after them
- Result: a stale Redis lookup or a dead gateway only adds latency,
  never data loss. No special race handling is needed.
- Client ACKs delivered seq; server stores it as the read/delivered cursor

GROUP CHAT  [CHANGED]
- STORAGE: every group message is stored ONCE in the group timeline,
  with one per-group seq, regardless of group size
- PER-USER: lightweight conversation list entry {conv_id, last_seq, unread}
- PUSH strategy depends on group size:
  - Small groups (< threshold): push the full message to each online member
  - Large groups (>= threshold): push a lightweight notify {conv_id, latest_seq}
    and the client pulls. Coalesce notifies in busy groups
    (at most 1 per X ms per member).
- Threshold is a config value (start at 500, tune by load test)
- Crossing the threshold changes ONLY the push strategy:
  no data migration, no merged histories, no migration job needed
  (the periodic job becomes a simple size check, or disappears)

STORAGE (MongoDB)  [NEW]
- messages: shard key {conv_id, seq}; unique index on (sender, client_msg_id)
- Hot/cold split: recent messages in Mongo, archive older data (e.g. > N months)
  to cheaper storage
- 1-to-1 conv_id = deterministic from the sorted user-ID pair

OBSERVABILITY  [NEW]
- Metrics: connections per node, msgs/s, queue depth, drops, p50/p99
  end-to-end latency, GC pause, MQ consumer lag, gap-pull rate
  (a rising gap-pull rate means push delivery is failing)
- Trace ID carried from gateway through logic, MQ, and push

OPEN / TO VALIDATE
- [ ] Gateway benchmark vs the 2.6M baseline (harness built)
- [ ] GC behavior under connect/disconnect churn (GOGC / GOMEMLIMIT tuning)
- [ ] Large-group threshold under social-app message rates
- [ ] Redis seq: recovery strategy if a Redis node is lost
- [ ] TLS handshake CPU during a mass reconnect (e.g. after a gateway deploy)
- [ ] Decide: online 1-to-1 via RocketMQ vs direct gRPC to the gateway (latency)

