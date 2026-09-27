IM / SOCIAL APP — 1 MILLION CONCURRENT USERS
================================================

GOAL
----
Build a production-grade social/IM server from zero to 100.
Do NOT build a throwaway old-style demo and gradually mutate it.
Use the production architecture from the beginning, but implement it
incrementally through small, working vertical slices.

STACK
-----
- Go
- gnet
- TCP initially
- WebSocket later
- Redis
- RocketMQ
- MongoDB

HIGH-LEVEL ARCHITECTURE
-----------------------

                    Clients
                       │
                  TCP / WebSocket
                       │
                       ▼
              ┌──────────────────┐
              │ L1: Gateway      │
              │ Go + gnet        │
              │                  │
              │ connections      │
              │ protocol         │
              │ routing          │
              │ heartbeat        │
              └────────┬─────────┘
                       │
                  internal RPC
                       │
                       ▼
              ┌──────────────────┐
              │ L2: Logic Nodes  │
              │                  │
              │ login            │
              │ push             │
              │ comments         │
              │ social logic     │
              │ business logic   │
              └──────┬─────┬─────┘
                     │     │
                 Redis     RocketMQ
                     │     │
                     │     ▼
                     │  Push Workers
                     │     │
                     │     ▼
                     └── MongoDB
                     
IMPORTANT LAYER RULE
--------------------
Gateway does NOT access Redis.

Only Logic Nodes access:
- Redis
- MongoDB
- RocketMQ

Gateway should stay thin and should not contain business logic.


GATEWAY RESPONSIBILITY
----------------------
Gateway handles:

- TCP connection
- later WebSocket connection
- packet decoding/encoding
- connection lifecycle
- heartbeat
- routing requests to Logic
- receiving push messages
- writing data to socket

Gateway should NOT decide:
- login ownership
- social business rules
- database operations
- feed logic
- follower fanout logic


LOGIC NODE
----------
Logic nodes should be as stateless as possible.

Possible internal modules:

logic/
    login/
    user/
    push/
    comment/
    post/
    feed/
    relationship/

Initially these can live in one Logic service/process.
Do NOT immediately split everything into microservices.

Split later only when scale or isolation requires it.


REDIS / MONGODB RESPONSIBILITIES
--------------------------------

Redis:
- online/session state
- user -> gateway mapping
- presence
- cache
- rate limiting
- counters
- short-lived state

MongoDB:
- durable user data
- posts
- comments
- relationships
- notifications
- other persistent social data

Redis should NOT gradually become the permanent database.


USER ONLINE OWNERSHIP
---------------------

Current proposed Redis mappings:

Forward:

    userid + appid
        ->
    gatewayid + workerid + socket

Reverse:

    gatewayid + workerid + socket
        ->
    userid + appid


Example:

    user:12345
        ->
    G17 + W3 + S88

    G17 + W3 + S88
        ->
    user:12345


The userid + appid mapping is considered the authoritative
"online mark".


RECONNECT / TAKEOVER
--------------------

Example:

    User 12345 is connected to G17.

G17 crashes.

User reconnects through G42.

Logic node:

1. Read userid + appid from Redis.
2. Find old gatewayid + workerid + socket.
3. Remove the old reverse mapping.
4. Replace the forward mapping.
5. Create the new reverse mapping.

Result:

    user:12345
        ->
    G42 + W8 + S99

and:

    G42 + W8 + S99
        ->
    user:12345


DISTRIBUTED LOGIN LOCK
----------------------

A distributed lock is useful for protecting concurrent login/session
transitions.

The lock should NOT itself be the ownership mechanism.

Generic lock design:

    Acquire(resourceKey, owner, TTL)
    Renew(resourceKey, owner)
    Release(resourceKey, owner)

The lock resource should be generic.

For example:

    login:user:12345
    session:user:12345
    post:987654
    friendship:12345:67890

The "actionid" can be useful because the distributed lock system
may later protect operations other than login.

For login specifically, appid + userid may be enough to identify
the resource.


LOCK LEASE
----------

A fixed 2-second TTL can be dangerous if the critical section can
last longer than 2 seconds.

Current idea:

    lock owner periodically extends/renews the lock.

Example:

    acquire
       ↓
    renew
       ↓
    renew
       ↓
    renew
       ↓
    release

However, correctness should NOT depend entirely on the lock.

The lock protects concurrent operations.
The session ownership mechanism protects stale ownership.


LOGIN RATE LIMIT
----------------

Current idea:

    If the same user logs in again within ~2 seconds,
    reject the login as "too frequent".

Important distinction:

    Distributed lock
        =
    concurrency control

    Login rate limit
        =
    abuse/frequency control

They solve different problems.


GATEWAY AFFINITY
----------------

A connected TCP client naturally stays on one gateway:

    Client
       ↓
      G17
       │
       │ connection lifetime
       │
       └──────────────→ stays on G17

When the connection dies and the client reconnects:

    reconnect
       ↓
    G42

Do NOT unnecessarily force a user to remain on the same gateway
for an arbitrary period.

Connection identity and user identity should remain separate.


SESSION VERSION QUESTION
------------------------

A session version was considered.

Possible model:

    old:
        user=12345
        gateway=G17
        socket=S1
        token=A

    new:
        user=12345
        gateway=G42
        socket=S8
        token=B

The session version/token would be generated SERVER-SIDE,
never by the client.

BUT:

A session version is NOT automatically necessary.

Current conclusion:

If the existing ownership mapping + distributed lock +
authoritative Redis lookup already solves stale ownership,
do NOT add a session version merely because it sounds useful.

Only introduce it if a concrete stale-session problem requires it.

Avoid unnecessary concepts.


CRITICAL RACE CONDITION DISCOVERED
-----------------------------------

This is currently one of the most important unresolved issues.

Scenario:

Initial state:

    Redis:
        user:123
            -> G17 / W3 / S1

Then user reconnects:

    G42
       ↓
    Logic
       ↓
    Redis:
        user:123
            -> G42 / W8 / S2

But G17's disconnect event was delayed.

Later:

    G17 -> Logic:
        "user 123 disconnected"

If Logic blindly does:

    DEL user:123

it will accidentally delete the NEW G42 session.

Therefore:

    OLD DISCONNECT
        +
    NEW LOGIN
        +
    MESSAGE DELAY
        =
    must be handled safely.

This is currently the next major design problem to solve.


IMPORTANT RULE
--------------

A disconnect operation must NOT delete/overwrite a newer session.

The Logic node must somehow verify:

    "Does this disconnect still refer to the current owner?"

using the information already available:

    appid
    userid
    gatewayid
    workerid
    socket
    Redis


PUSH ARCHITECTURE
-----------------

Basic flow:

    Logic
      ↓
    RocketMQ
      ↓
    Push Worker
      ↓
    Redis
      ↓
    find current gateway/socket
      ↓
    Gateway
      ↓
    client


The push worker should resolve the CURRENT ownership rather than
blindly relying on stale gateway information.


FANOUT
------

Current design idea:

    small follower count
        ->
    write fanout

    large follower count
        ->
    read fanout


Initial proposed threshold:

    ~500 followers

Example:

    User A has 2,000,000 followers.

Use read fanout instead of trying to synchronously push/write
to 2 million followers.

However, 500 should be treated as a tunable starting point,
not a universal law.

Fanout strategy should eventually consider:

    follower_count
    × post_frequency
    × follower_activity
    × feed_read_frequency
    × current system load


WRITE FANOUT
------------

For smaller audiences:

    A posts
       ↓
    find followers
       ↓
    create/update feed entries
       ↓
    push/notify followers


READ FANOUT
-----------

For very large audiences:

    A posts
       ↓
    store A's post

    B opens feed
       ↓
    retrieve relevant posts
       ↓
    merge/rank/feed


NEXT UNSOLVED DESIGN PROBLEM
----------------------------

For User A with 2 million followers:

    A posts
       ↓
    post stored
       ↓
    200,000 followers open feed
       ↓
    another 300,000 followers open feed

Need to design:

1. MongoDB feed/post data model
2. Redis caching strategy
3. How to avoid scanning 2M followers
4. How to query "what changed since last feed position?"
5. Follow/unfollow behavior
6. Feed pagination
7. Ranking/ordering
8. Hot-user behavior


DEVELOPMENT STRATEGY
--------------------

DO NOT:

    simple demo
       ↓
    keep adding features
       ↓
    eventually try to make it production

Instead:

    production architecture
       ↓
    tiny working vertical slice
       ↓
    test
       ↓
    expand
       ↓
    load test
       ↓
    harden
       ↓
    scale


DEVELOPMENT PHASES
------------------

Phase 1:

    TCP
      ↓
    gnet Gateway
      ↓
    Logic
      ↓
    Redis

Implement:

    connect
    login
    heartbeat
    disconnect


Phase 2:

    login
      ↓
    business operation
      ↓
    MongoDB
      ↓
    response


Phase 3:

    Logic
      ↓
    RocketMQ
      ↓
    Push Worker
      ↓
    Redis
      ↓
    Gateway
      ↓
    Client


Phase 4:

    scale Gateway
    scale Logic
    scale Redis
    scale RocketMQ
    scale MongoDB

Measure:

    concurrent connections
    messages/sec
    CPU
    memory
    GC
    network
    Redis latency
    MongoDB latency
    RocketMQ throughput
    reconnect rate
    push latency


IMPORTANT
---------

1 million concurrent users != 1 million requests/sec.

Need actual capacity targets:

    concurrent connections
    average messages/sec
    peak messages/sec
    message size
    read/write ratio
    connection churn/sec
    network bandwidth
    Redis ops/sec
    MongoDB writes/sec
    RocketMQ messages/sec


CURRENT ARCHITECTURAL PRINCIPLES
--------------------------------

1. Gateway is thin.
2. Gateway does not access Redis.
3. Logic owns business logic.
4. Logic accesses Redis/MongoDB/RocketMQ.
5. Redis owns hot/online state.
6. MongoDB owns durable state.
7. RocketMQ decouples asynchronous work.
8. Distributed lock protects concurrent state transitions.
9. Rate limiting protects login frequency.
10. Server generates session/token identity.
11. Do not add session versions without a concrete need.
12. User ownership must survive reconnect races.
13. Old disconnect events must never destroy a newer session.
14. Push must resolve current ownership.
15. Fanout strategy should depend on workload, not only follower count.
16. Build the production architecture incrementally.
17. Measure before deciding scaling limits.


CURRENT NEXT GRILLING QUESTION
------------------------------

How do you prevent this:

    G17 owns user 123 / socket S1

    User reconnects

    G42 owns user 123 / socket S2

    G17's delayed disconnect arrives AFTER the G42 login

    G17 -> Logic:
        "user 123 disconnected"

How does Logic know that the disconnect belongs to
the OLD session and must NOT delete G42's session?

Solve this using the information you already have:

    userid
    appid
    gatewayid
    workerid
    socket
    Redis
    distributed lock

Try to solve it WITHOUT adding a new concept unless necessary.
