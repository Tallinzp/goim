# Plan 6c2a: Failover Tools Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the lease and Logic's slot map survive a Redis shard failover, give loadgen a per-client login log, and give the Tier 2 stack the helpers that drive a failover and a zone loss, so Plan 6c2b can run the spec's *Redis failover test* (R2-O5, R5S-O1, R5S-O9) as scenario `failover`.

**Architecture:** Three parts, one task each:
- **The lease and the slot map** (Task 1, production code). The gateway retries a failed `GatewayLease` call after `-lease-retry` (1s) instead of at the next 10s tick. Logic's cluster client reloads its slot map when it is older than `-redis-reload` (1s) instead of 60s.
- **The login log** (Task 2, loadgen). Light mode records each client's latest login sequence (first attempt's start, login time, attempts); `GET /logins` returns it. 6c2b groups it by the uid's slot owner.
- **The stack's failover helpers** (Task 3, scripts). `stack.sh` kills, restarts, stops and resumes Cluster nodes, makes a manual failover, waits for a role or a whole Cluster, and restores the start layout. The slot map comes from the first node that answers (P6c1I-1), no reader hangs on a stopped node, `cluster-node-timeout` is 10s. A new `scripts/stack_failover_test.sh` drives a kill and a zone stop on a real stack.

**Tech Stack:** Go 1.27.1, go-redis v9.23.0, bash 5.2, python3, the pinned `redis-server` 7.4.11 (`make redis-server`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`: *Redis schema* → `gwlease:` (the lease rule *TTL > measured shard failover time + one renewal interval*, R2-O5, ~l.296), *Interface constraints* → Lease (~l.217), *Deployment requirements* → Redis placement (R5S-O9, ~l.73), *Storms, drain, failures* → Redis shard failover and Zone loss (~l.418–419), *Verification* → Redis failover test (~l.535). Nothing here changes it: Task 1 makes the code meet the lease rule as written.

Inputs:
- The roadmap's #137 (6c split), #138 (6c1 choices: node timeout 15s "measured in 6c2"), #21 (Logic's `-redis-failover` 15s for the ratio check) and the new **#139** (this plan's decisions).
- 6c1's interface review P6c1I-1..4 (the 6c1 plan's *Execution Notes*): the slot map from a live node, a restart with `nodes.conf` kept, the `fail?` window, the new names.

**Decisions (#139, the user, 2026-10-25, with AskUserQuestion, before writing):**
1. **The lease rule needs a retry.** Measured on the dev machine (local Cluster, SIGKILL of a master, time until its replica is master): `cluster-node-timeout` 15s → 20.6, 21.8, 22.7s; 10s → 16.0, 16.8s; 5s → 7.7, 7.8s; `CLUSTER FAILOVER` 7–25ms. The gateway renewed only on its 10s tick, also after an error, so in the worst case (the kill just before a renewal; the key expires at kill + 20s, the last attempt before that is at kill + 10s) the lease lived only through a failover < TTL − 2 × interval = 10s. The user chose: **the gateway retries a failed renewal every 1s** (the spec's rule then holds: failover < TTL − interval = 20s) and **the Tier 2 Cluster runs `cluster-node-timeout` 10s** (failover ~16.5s, a ~3.5s margin).
2. **Zone loss is SIGSTOP, then SIGCONT** (6c2b): a cut-off zone keeps its sockets and answers nothing; on SIGCONT it returns and the old master rejoins as a replica by itself.
3. **The kill lands in the worst case** (6c2b): ~9s after the lease shard's gateway's last renewal.
4. **6c2 is split into 6c2a (this plan, no Tier 2 run) and 6c2b** (scenario `failover`, run 34, the record), each written in its own session.
5. **Logic reloads the slot map every 1s** (`-redis-reload`), found by this plan's prototype and decided after it: go-redis reloads its slot map only after a MOVED, ASK or READONLY reply, or once it is 60s old (`ClusterStateReloadInterval`). A SIGKILLed master answers none of these, so Logic sent that shard's commands to the dead address for up to a minute after its replica took over: in the prototype's test, 11 of 30 new logins gave up (108 `busy` replies) after the failover had ended. With 1s, all 30 log in.

**Branch:** `feat/plan6c` (from `main` `1319ae9`). Execute from its head, this plan's own commit or later.

**Size:** 3 tasks. Task 1: ~85 lines (8 files: the gateway's lease loop and config, Logic's flag). Task 2: ~100 lines (6 loadgen files). Task 3: ~350 lines (`stack.sh` +175, `redis-cluster.sh`, the new test, the Makefile). The prototype `proto-6c2a` (from `1319ae9`; tasks `b8fba88`, `d6b1843`, `eb9e1ec`; pushed) ran every step. Its diff is replayed here byte for byte, one `git apply` patch per task. On the dev machine (AMD EPYC 7742, 2 cores):
- `make test` (race), `go vet ./...`, `make test-scripts` (with the new `stack_failover_test.sh`, 40 checks, ~67s), `make test-redis-local` and `make test-redis-cluster-local` green: 20 packages `ok` in each of the three Go runs, every script suite's all-pass line.
- Mutations: the lease loop waiting for the tick after an error fails `TestFailedRenewalIsRetriedBeforeTheNextTick`; light mode logging the last attempt in place of the sequence fails `TestLightRetriesAPreHandshakeClose`; Logic without `-redis-reload` (go-redis's 60s) fails `logins: 30 more online after the failover` in `stack_failover_test.sh` (19 online, 11 gave up, 108 `busy`).

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...`, `make test-redis-local`, `make test-redis-cluster-local` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- Production changes only in `internal/gateway` (the lease loop, `Config.LeaseRetry`), `cmd/gateway` (`-lease-retry`) and `cmd/logic` (`-redis-reload`). No change to the store, the SDK, the wire or any Lua.
- Lease timing stays as the spec has it: interval 10s, TTL 30s, call timeout 5s. `LeaseRetry` (1s) must not exceed `LeaseInterval`.
- The pinned `redis-server` 7.4.11; no Docker for the Cluster (#66). `STACK_REDIS=single` stays the default: no scenario's numbers move, and no existing check changes.
- The new test's ports: `STACK_PORT_BASE` 17700 (+20..25, bus +30..35), apart from `stack_test.sh`'s and `stack_cluster_test.sh`'s (17600).

## Review Focus

1. **A renewal that hangs, not fails.** On a stopped node (6c2b's zone loss) a lease call waits for go-redis's 3s read timeout and its retries, up to the gateway's 5s `LeaseTimeout`, before it fails; the retry then follows 1s later. So the cadence there is ~6s, not 1s, and the margin against TTL − interval is smaller than for a SIGKILL. No test here stops the lease key's shard; 6c2b's `zone` phase measures it (the lease key's master is in the stopped zone).
2. **The retry in a Redis-wide outage.** Every gateway then calls `GatewayLease` once a second, not once in 10s: 10× the lease RPCs, on their own connection and Logic's separate worker pool (R1-O2). At the spec's fleet (5–10 gateways) that is ≤ 10 calls/s. `TestRenewalAfterFailuresForALeaseTTLTriggersAPass` still triggers the pass after a TTL of failures with the retry in place.
3. **The reload's cost.** go-redis reloads at most once per `-redis-reload` per Logic pod, in the background, when a command finds the map older than that (and waits 200ms between reloads): one `CLUSTER SLOTS` from one node (~200 bytes for 3 shards). No test measures it; 6c2b's `P.redis_latency` lists `cluster` calls only if they are not filtered (`stack_redis_node_latency` leaves `cluster*` out), so 6c2b names it from Logic's side if it matters.
4. **"down" under load.** `stack_redis_role` and `stack_redis_ping` give a node 1s. A node that is merely slow (a 600k-session master in a storm) for over 1s reads `down`, and `stack_redis_latency` then records it as null. 6c2b reads roles outside the storm windows, or names a null as "no answer within 1s".
5. **The restore after a big resync.** `stack_redis_restore` first waits for the Cluster to be whole (every replica's link up), then runs `CLUSTER FAILOVER` on each start master and gives it 10s. At 6c2b's population a returning node first needs a full resync of its master's data; the whole-wait (6c2b passes its own SECONDS) covers that, and the manual failover itself only waits for the last offsets. The test here covers the order with 30–60 sessions only.

## Implementation choices this plan makes

These are #139.6–#139.9 in the roadmap (proposed with this plan; the user decides them at review).
6. **`-lease-retry` 1s, after any failed call.** No backoff: the retry is one RPC a second per gateway, and a backoff would lengthen the gap the rule is about. The ticker keeps running; after a successful retry the next renewal is the next tick (at most 10s later), so a retry never adds a renewal to a healthy cycle.
7. **`-redis-reload` 1s, positive.** A periodic bound, not a reload on each node error: go-redis already loads the map lazily; a short interval needs one option and no store code. 0 is refused (go-redis would read it as its 60s default).
8. **The login log is light mode only, lazily allocated.** 20 bytes per client (`first_ms`, `end_ms`, `attempts`) only once a light client ends a sequence, so sdk mode's heap (`loadgen_heap` expectations) does not change. A sequence is the loop of up to 10 attempts; a later sequence of the same client replaces it.
9. **Helpers keep a node's name, port and directory.** A restart is the same node (`redis-j`, its `nodes.conf`), so `redis/layout`, the slot readers and `stack_health` keep working; its earlier log moves to `logs/redis-j.log.N`. `stack_down` resumes a stopped node before it stops it (a stopped process does not act on SIGTERM).

## File Structure

- `internal/gateway/lease.go`, `config.go`, `cmd/gateway/main.go`: `LeaseRetry`, `-lease-retry`, the retry in `runLease` (Task 1).
- `internal/gateway/lease_test.go`, `config_test.go`, `logic_test.go` (`fastConfig`): the retry test, the config check (Task 1).
- `cmd/logic/main.go`, `main_test.go`: `-redis-reload` (Task 1).
- `internal/loadgen/recorder.go`, `light.go`, `control.go` and their tests: `LoginLogs`, `LoginLog`, `Logins`, `GET /logins` (Task 2).
- `scripts/redis-cluster.sh`: `redis_cluster_wait` (from the join), node timeout 10s (Task 3).
- `scripts/stack.sh`: the live node, bounded readers, the failover helpers, `stack_down`'s resume (Task 3).
- `scripts/stack_failover_test.sh` (new), `Makefile` (`test-scripts`) (Task 3).
- The roadmap (#139, the plan 6 row), this plan's *Execution Notes* (after execution).

---

### Task 1: The lease and the slot map through a failover

**Files:**
- Modify: `internal/gateway/lease.go`, `internal/gateway/config.go`, `cmd/gateway/main.go`, `cmd/logic/main.go`
- Test: `internal/gateway/lease_test.go`, `internal/gateway/config_test.go`, `internal/gateway/logic_test.go`, `cmd/logic/main_test.go`

**Interfaces:**
- Produces (Go): `gateway.Config.LeaseRetry time.Duration` (default 1s; `Validate` refuses ≤ 0 and > `LeaseInterval`); `(*Gateway).renew(...) bool` (true when the call succeeded; unexported).
- Produces (flags, for Task 3 and 6c2b): gateway `-lease-retry` (1s); Logic `-redis-reload` (1s, must be positive; sets go-redis `ClusterStateReloadInterval`).
- Unchanged: `lease_calls_total{result}` (`first`, `live`, `missing`, `error`): a retried failure counts one `error` per call.

- [ ] **Step 1: The tests**

Save the patch below to a file in the session scratchpad and apply only its test files from the repository root: `git apply --include='*_test.go' PATCH`.

```diff
diff --git a/cmd/gateway/main.go b/cmd/gateway/main.go
index b95e23d..ec4a5e3 100644
--- a/cmd/gateway/main.go
+++ b/cmd/gateway/main.go
@@ -155,6 +155,7 @@ func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config
 	fs.DurationVar(&cfg.LoginTimeout, "login-timeout", cfg.LoginTimeout, "per-entry Login timeout from recv_ms")
 	fs.DurationVar(&cfg.LeaseInterval, "lease-interval", cfg.LeaseInterval, "GatewayLease period")
 	fs.DurationVar(&cfg.LeaseTimeout, "lease-timeout", cfg.LeaseTimeout, "GatewayLease call timeout")
+	fs.DurationVar(&cfg.LeaseRetry, "lease-retry", cfg.LeaseRetry, "wait after a failed GatewayLease call before the next one")
 	fs.DurationVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "Logic's lease TTL (the trigger rule)")
 	fs.DurationVar(&cfg.RetryFor, "retry-for", cfg.RetryFor, "Logout and ReconcileEnd retry budget")
 	fs.DurationVar(&cfg.RetryBackoff, "retry-backoff", cfg.RetryBackoff, "first Logout and ReconcileEnd retry delay, doubled per attempt")
diff --git a/cmd/logic/main.go b/cmd/logic/main.go
index 803a6de..dc3da0e 100644
--- a/cmd/logic/main.go
+++ b/cmd/logic/main.go
@@ -12,6 +12,7 @@ import (
 	"os/signal"
 	"strings"
 	"syscall"
+	"time"
 
 	"github.com/Tallinzp/goim/internal/auth"
 	"github.com/Tallinzp/goim/internal/fault"
@@ -33,6 +34,7 @@ func main() {
 type options struct {
 	redisAddrs   string
 	redisCluster bool
+	redisReload  time.Duration
 	jwksURL      string
 	jwksFallback string
 	jwks         auth.ManagerConfig // its intervals; URL and fallback come from the two flags above
@@ -48,6 +50,7 @@ func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error
 	fs := flag.NewFlagSet("logic", flag.ContinueOnError)
 	fs.StringVar(&o.redisAddrs, "redis", "127.0.0.1:6379", "Redis address(es), comma-separated")
 	fs.BoolVar(&o.redisCluster, "redis-cluster", true, "Redis Cluster (false: one node, for tests)")
+	fs.DurationVar(&o.redisReload, "redis-reload", time.Second, "Redis Cluster: reload the slot map when it is older than this; a failed-over master's slots reach its replica within it (#139)")
 	fs.StringVar(&o.jwksURL, "jwks-url", "", "account service JWKS URL")
 	fs.StringVar(&o.jwksFallback, "jwks-fallback", "/etc/goim/jwks-fallback/jwks.json", "jwks-fallback ConfigMap file")
 	fs.DurationVar(&o.jwks.Refresh, "jwks-refresh", o.jwks.Refresh, "periodic JWKS refresh")
@@ -99,6 +102,9 @@ func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error
 	if o.jwks.Refresh <= 0 || o.jwks.RetryEmpty <= 0 || o.jwks.UnknownKid <= 0 {
 		return cfg, sc, o, errors.New("config: the JWKS intervals must be positive")
 	}
+	if o.redisReload <= 0 { // go-redis reads 0 as its 60s default
+		return cfg, sc, o, errors.New("config: -redis-reload must be positive")
+	}
 	return cfg, sc, o, nil
 }
 
@@ -110,7 +116,12 @@ func run(args []string) error {
 	addrs := strings.Split(o.redisAddrs, ",")
 	var rdb redis.UniversalClient
 	if o.redisCluster {
-		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
+		// The client reloads its slot map after a MOVED, ASK or READONLY
+		// reply, or once it is older than ClusterStateReloadInterval (60s by
+		// default). A master that died answers nothing, so without a short
+		// interval its slots stay on the dead address for up to a minute
+		// after its replica took over (Plan 6c2a prototype, #139).
+		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, ClusterStateReloadInterval: o.redisReload})
 	} else {
 		rdb = redis.NewClient(&redis.Options{Addr: addrs[0]})
 	}
diff --git a/cmd/logic/main_test.go b/cmd/logic/main_test.go
index 95fdf43..004180e 100644
--- a/cmd/logic/main_test.go
+++ b/cmd/logic/main_test.go
@@ -11,11 +11,18 @@ func TestDefaultFlagsAreValid(t *testing.T) {
 	if err != nil {
 		t.Fatal(err)
 	}
-	if cfg.Store.LeaseTTL != 30*time.Second || sc.MaxConnectionAgeGrace != 30*time.Second || !o.redisCluster {
+	if cfg.Store.LeaseTTL != 30*time.Second || sc.MaxConnectionAgeGrace != 30*time.Second || !o.redisCluster || o.redisReload != time.Second {
 		t.Fatalf("defaults: %+v %+v %+v", cfg, sc, o)
 	}
 }
 
+// A zero -redis-reload would be go-redis's 60s default (#139).
+func TestRefusesZeroRedisReload(t *testing.T) {
+	if _, _, _, err := parseFlags([]string{"-jwks-url", "x", "-redis-reload", "0s"}); err == nil || !strings.Contains(err.Error(), "-redis-reload") {
+		t.Fatalf("-redis-reload 0s: %v", err)
+	}
+}
+
 // The test timing profile (Plan 5) scales intervals ~10× and keeps the ratios.
 func TestScaledProfileFlags(t *testing.T) {
 	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s",
diff --git a/internal/gateway/config.go b/internal/gateway/config.go
index c2f4a11..9e1c6f7 100644
--- a/internal/gateway/config.go
+++ b/internal/gateway/config.go
@@ -56,6 +56,7 @@ type Config struct {
 
 	LeaseInterval time.Duration // GatewayLease period (10s)
 	LeaseTimeout  time.Duration // per call (5s)
+	LeaseRetry    time.Duration // after a failed call, in place of the next tick (1s; #139)
 	LeaseTTL      time.Duration // Logic's lease TTL (30s), for the trigger rule
 
 	Reconcile ReconcileConfig
@@ -129,6 +130,7 @@ func DefaultConfig() Config {
 		RetryMaxDelay:    10 * time.Second,
 		LeaseInterval:    10 * time.Second,
 		LeaseTimeout:     5 * time.Second,
+		LeaseRetry:       time.Second,
 		LeaseTTL:         30 * time.Second,
 		Reconcile: ReconcileConfig{Cycle: 10 * time.Minute, Jitter: time.Minute, Chunk: 200,
 			TriggeredRate: 5000, MinRate: 10, MaxInFlight: 4, MaxRequeues: 3, DeleteRate: 10_000},
@@ -212,6 +214,7 @@ func (c Config) Validate() error {
 	for name, d := range map[string]time.Duration{"LoginLinger": c.LoginLinger, "LogoutLinger": c.LogoutLinger,
 		"RPCTimeout": c.RPCTimeout, "RetryFor": c.RetryFor, "RetryBackoff": c.RetryBackoff,
 		"RetryMaxDelay": c.RetryMaxDelay, "LeaseInterval": c.LeaseInterval, "LeaseTimeout": c.LeaseTimeout,
+		"LeaseRetry":      c.LeaseRetry,
 		"Reconcile.Cycle": c.Reconcile.Cycle, "Drain.Window": c.Drain.Window, "Drain.Jitter": c.Drain.Jitter} {
 		if d <= 0 {
 			bad("%s %v must be positive", name, d)
@@ -220,6 +223,9 @@ func (c Config) Validate() error {
 	if c.LeaseTimeout > c.LeaseInterval {
 		bad("LeaseTimeout %v must not exceed LeaseInterval %v", c.LeaseTimeout, c.LeaseInterval)
 	}
+	if c.LeaseRetry > c.LeaseInterval {
+		bad("LeaseRetry %v must not exceed LeaseInterval %v", c.LeaseRetry, c.LeaseInterval)
+	}
 	if c.LeaseTTL <= c.LeaseInterval {
 		bad("LeaseTTL %v must exceed LeaseInterval %v", c.LeaseTTL, c.LeaseInterval)
 	}
diff --git a/internal/gateway/config_test.go b/internal/gateway/config_test.go
index 64d0c6c..56e7c05 100644
--- a/internal/gateway/config_test.go
+++ b/internal/gateway/config_test.go
@@ -71,6 +71,7 @@ func TestValidateRefuses(t *testing.T) {
 		{"heartbeat above its range", func(c *Config) { c.Heartbeat = 600 * time.Second }, "outside [30s, 5m0s]"},
 		{"inverted heartbeat range", func(c *Config) { c.HeartbeatMin = 10 * time.Minute }, "HeartbeatMin"},
 		{"lease timeout above the interval", func(c *Config) { c.LeaseTimeout = 11 * time.Second }, "LeaseTimeout"},
+		{"lease retry above the interval", func(c *Config) { c.LeaseRetry = 11 * time.Second }, "LeaseRetry"},
 		{"lease TTL not above the interval", func(c *Config) { c.LeaseTTL = 10 * time.Second }, "LeaseTTL"},
 		{"jitter of half a cycle", func(c *Config) { c.Reconcile.Jitter = 5 * time.Minute }, "Jitter"},
 		{"empty Busy window", func(c *Config) { c.BusyRetry = Window{3 * time.Second, time.Second} }, "BusyRetry"},
diff --git a/internal/gateway/lease.go b/internal/gateway/lease.go
index 30d9028..55c3f89 100644
--- a/internal/gateway/lease.go
+++ b/internal/gateway/lease.go
@@ -8,7 +8,10 @@ import (
 )
 
 // runLease renews the epoch's lease every LeaseInterval on the dedicated
-// lease connection (spec: Lease, R1-O2). It returns when stop is closed,
+// lease connection (spec: Lease, R1-O2). A failed call is retried after
+// LeaseRetry, not at the next tick: the key then outlives a Redis shard
+// failover of up to TTL − LeaseInterval (the spec's lease rule, R2-O5), not
+// TTL − 2 × LeaseInterval (#139). It returns when stop is closed,
 // after the renewal in flight (if any) has finished, so EpochEnded can follow
 // without a renewal re-creating the key behind it.
 func (g *Gateway) runLease(ctx context.Context, stop <-chan struct{}, lease pb.LeaseClient, trigger func()) {
@@ -21,13 +24,16 @@ func (g *Gateway) runLease(ctx context.Context, stop <-chan struct{}, lease pb.L
 			return
 		default:
 		}
-		g.renew(ctx, lease, &lastOK, trigger)
+		next := t.C
+		if !g.renew(ctx, lease, &lastOK, trigger) {
+			next = time.After(g.cfg.LeaseRetry)
+		}
 		select {
 		case <-ctx.Done():
 			return
 		case <-stop:
 			return
-		case <-t.C:
+		case <-next:
 		}
 	}
 }
@@ -37,8 +43,8 @@ func (g *Gateway) runLease(ctx context.Context, stop <-chan struct{}, lease pb.L
 // lease TTL: the key may have expired and been swept, and a lost reply may
 // have hidden the missing that re-created it. The first successful call after
 // boot always finds a new epoch's key absent, so it neither counts nor acts
-// on missing (R3-OP5).
-func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.Time, trigger func()) {
+// on missing (R3-OP5). It reports whether the call succeeded.
+func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.Time, trigger func()) bool {
 	start := time.Now()
 	cctx, cancel := g.rpcCtx(ctx, g.cfg.LeaseTimeout)
 	reply, err := lease.GatewayLease(cctx, &pb.LeaseRequest{Gw: g.ref, GwNowMs: g.now().UnixMilli()})
@@ -48,7 +54,7 @@ func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.
 		if ctx.Err() == nil {
 			g.log.Warn("lease renewal failed", "err", err)
 		}
-		return
+		return false
 	}
 	first := lastOK.IsZero()
 	gap := time.Since(*lastOK)
@@ -56,7 +62,7 @@ func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.
 	if first {
 		g.m.LeaseCalls.WithLabelValues("first").Inc()
 		g.leaseOK.Store(true)
-		return
+		return true
 	}
 	if reply.GetMissing() {
 		g.m.LeaseCalls.WithLabelValues("missing").Inc()
@@ -66,4 +72,5 @@ func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.
 	if reply.GetMissing() || gap >= g.cfg.LeaseTTL {
 		trigger()
 	}
+	return true
 }
diff --git a/internal/gateway/lease_test.go b/internal/gateway/lease_test.go
index 0e3c80c..af83a21 100644
--- a/internal/gateway/lease_test.go
+++ b/internal/gateway/lease_test.go
@@ -5,6 +5,8 @@ import (
 	"testing"
 	"time"
 
+	"github.com/prometheus/client_golang/prometheus/testutil"
+
 	pb "github.com/Tallinzp/goim/api/internalpb"
 )
 
@@ -77,3 +79,32 @@ func TestRenewalAfterFailuresForALeaseTTLTriggersAPass(t *testing.T) {
 	failing <- false
 	waitFor(t, "triggered pass", func() bool { return passes(l) >= 1 })
 }
+
+// A failed renewal is retried after LeaseRetry, not at the next tick, so a
+// lease outlives a Redis failover of up to TTL − interval (spec: the lease
+// rule, R2-O5; roadmap #139). Back on the tick once a call succeeds.
+func TestFailedRenewalIsRetriedBeforeTheNextTick(t *testing.T) {
+	fails := 3
+	l := startLive(t, func(c *Config) {
+		c.LeaseInterval, c.LeaseTTL, c.LeaseRetry = time.Hour, 2*time.Hour, 20*time.Millisecond
+	},
+		func(_ *Gateway, f *fakeLogic) {
+			f.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) {
+				f.mu.Lock()
+				defer f.mu.Unlock()
+				if fails > 0 {
+					fails--
+					return nil, errors.New("shard failing over")
+				}
+				return &pb.LeaseReply{Missing: true}, nil // a new epoch's key is absent
+			}
+		})
+	waitFor(t, "first lease after 3 failed calls", l.g.leaseOK.Load)
+	time.Sleep(200 * time.Millisecond) // 10 × LeaseRetry
+	if n := get(l.logic, func() int { return len(l.logic.leaseCalls) }); n != 4 {
+		t.Fatalf("%d lease calls, want 4: 3 failed and retried, then the hourly tick", n)
+	}
+	if got := testutil.ToFloat64(l.g.m.LeaseCalls.WithLabelValues("error")); got != 3 {
+		t.Fatalf("lease_calls_total{result=error} %v, want 3", got)
+	}
+}
diff --git a/internal/gateway/logic_test.go b/internal/gateway/logic_test.go
index ca4b1e7..a9650f2 100644
--- a/internal/gateway/logic_test.go
+++ b/internal/gateway/logic_test.go
@@ -170,7 +170,7 @@ type live struct {
 
 // fastConfig shrinks the background intervals so tests run quickly.
 func fastConfig(c *Config) {
-	c.LeaseInterval, c.LeaseTimeout, c.LeaseTTL = 100*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond
+	c.LeaseInterval, c.LeaseTimeout, c.LeaseTTL, c.LeaseRetry = 100*time.Millisecond, 100*time.Millisecond, 300*time.Millisecond, 20*time.Millisecond
 	c.Reconcile.Cycle, c.Reconcile.Jitter = time.Hour, 0 // passes only when a test triggers one
 	c.RetryBackoff, c.RetryMaxDelay = 20*time.Millisecond, 50*time.Millisecond
 }
```

- [ ] **Step 2: The tests fail on the old code**

Run: `go vet ./internal/gateway ./cmd/logic`
Expected: 2 issues: `cmd/logic/main_test.go:14:112: o.redisReload undefined` and `internal/gateway/config_test.go:74:58: c.LeaseRetry undefined`.

- [ ] **Step 3: The code**

Apply the rest of the same patch: `git apply --exclude='*_test.go' PATCH`.

- [ ] **Step 4: The tests pass, and the retry is what passes them**

Run: `go vet ./internal/gateway ./cmd/gateway ./cmd/logic && go test -race -count=1 ./internal/gateway ./cmd/gateway ./cmd/logic`
Expected: `ok` for all three packages.

Mutation: in `internal/gateway/lease.go` replace `next = time.After(g.cfg.LeaseRetry)` with `next = t.C`, run `go test -count=1 ./internal/gateway -run TestFailedRenewalIsRetriedBeforeTheNextTick`: FAIL (the first lease never succeeds within `waitFor`'s 5s). Undo the mutation (`git diff internal/gateway/lease.go` shows only the patch's change).

- [ ] **Step 5: Commit**

```bash
git add cmd/gateway/main.go cmd/logic/main.go cmd/logic/main_test.go internal/gateway/config.go internal/gateway/config_test.go internal/gateway/lease.go internal/gateway/lease_test.go internal/gateway/logic_test.go
git commit -m "feat: keep the lease and the slot map through a Redis failover

The gateway renewed its lease only on the 10s tick, also after a
failed call, so a shard failover had to end within TTL - 2 x
interval (10s). Now a failed call is retried after -lease-retry
(1s): the spec's rule, TTL - interval (20s).

Logic's cluster client reloaded its slot map only after a MOVED,
ASK or READONLY reply, or every 60s. A dead master answers none,
so its slots stayed on the dead address up to a minute after the
failover. Now -redis-reload (1s) bounds that.

Refs: #139"
```

---

### Task 2: The login log in loadgen

**Files:**
- Modify: `internal/loadgen/recorder.go`, `internal/loadgen/light.go`, `internal/loadgen/control.go`
- Test: `internal/loadgen/recorder_test.go`, `internal/loadgen/light_test.go`, `internal/loadgen/e2e_test.go`

**Interfaces:**
- Produces (Go): `type LoginLogs struct { FirstMs []int64; EndMs []int64; Attempts []int32 }` (JSON `first_ms`, `end_ms`, `attempts`, by client index); `(*Recorder).LoginLog(i int, first, end time.Time, attempts int)` (end zero: gave up); `(*Recorder).Logins() LoginLogs` (a copy; zeros before any sequence ended).
- Produces (control API, for 6c2b): `GET /logins` → `{"uid_prefix", "first_ms": [...], "end_ms": [...], "attempts": [...]}`; client i is uid `<uid_prefix>-<i>`; `end_ms` 0 with `attempts` > 0: gave up; all 0: no sequence ended yet (or sdk mode).
- Consumes: nothing from Task 1.

- [ ] **Step 1: The tests**

Save the patch below and apply its test files: `git apply --include='*_test.go' PATCH`.

```diff
diff --git a/internal/loadgen/control.go b/internal/loadgen/control.go
index 91a0e7e..1ed637a 100644
--- a/internal/loadgen/control.go
+++ b/internal/loadgen/control.go
@@ -43,6 +43,8 @@ func (r *Runner) Result(within time.Duration) Result {
 //	                                          time (/reset's is close_ms; both keys are in recorded runs)
 //	                                          409: not sdk mode or the mark taken, nothing changed; 500: ErrGhostKeeper
 //	GET  /clients                             {"uid_prefix", "gateways", "gw": [client i's gateway index, -1: offline]}
+//	GET  /logins                              light: {"uid_prefix", "first_ms", "end_ms", "attempts"}: each client's
+//	                                          latest login sequence (LoginLogs; #139)
 //	GET  /slo?mark=death&within=30s           SLO
 //	GET  /runtime                             Runtime: loadgen's GC, memory and scheduling counters
 //	/debug/pprof/                             loadgen's own profiles
@@ -122,6 +124,10 @@ func (r *Runner) Handler() http.Handler {
 	mux.HandleFunc("GET /clients", func(w http.ResponseWriter, _ *http.Request) {
 		writeJSON(w, map[string]any{"uid_prefix": r.cfg.UIDPrefix, "gateways": r.env.GatewayIDs, "gw": r.rec.Gateways()})
 	})
+	mux.HandleFunc("GET /logins", func(w http.ResponseWriter, _ *http.Request) {
+		l := r.rec.Logins()
+		writeJSON(w, map[string]any{"uid_prefix": r.cfg.UIDPrefix, "first_ms": l.FirstMs, "end_ms": l.EndMs, "attempts": l.Attempts})
+	})
 	mux.HandleFunc("POST /network-change", func(w http.ResponseWriter, _ *http.Request) {
 		n, took, err := r.NetworkChange()
 		if err != nil {
diff --git a/internal/loadgen/e2e_test.go b/internal/loadgen/e2e_test.go
index 22795c7..eb93335 100644
--- a/internal/loadgen/e2e_test.go
+++ b/internal/loadgen/e2e_test.go
@@ -282,6 +282,26 @@ func TestLightRetriesAPreHandshakeClose(t *testing.T) {
 	if took := time.Since(begin); took < time.Second {
 		t.Fatalf("logged in after %v: retries must wait 0.5–1.5s each", took)
 	}
+	// GET /logins: the whole sequence, from the first attempt (#139).
+	srv := httptest.NewServer(r.Handler())
+	defer srv.Close()
+	resp, err := http.Get(srv.URL + "/logins")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer resp.Body.Close()
+	var l struct {
+		UIDPrefix string  `json:"uid_prefix"`
+		FirstMs   []int64 `json:"first_ms"`
+		EndMs     []int64 `json:"end_ms"`
+		Attempts  []int32 `json:"attempts"`
+	}
+	if err := json.NewDecoder(resp.Body).Decode(&l); err != nil {
+		t.Fatal(err)
+	}
+	if l.UIDPrefix != "retry" || len(l.Attempts) != 1 || l.Attempts[0] != 3 || l.FirstMs[0] < begin.UnixMilli() || l.EndMs[0]-l.FirstMs[0] < 1000 {
+		t.Fatalf("/logins %+v, want 3 attempts over at least 1s from the first", l)
+	}
 }
 
 // refuseFirst forwards to addr like closeFirst, but reads each connection's
diff --git a/internal/loadgen/light.go b/internal/loadgen/light.go
index 49a1b7a..db78b37 100644
--- a/internal/loadgen/light.go
+++ b/internal/loadgen/light.go
@@ -45,11 +45,14 @@ const lightAttempts = 10
 
 // light logs client lc in and reads until its connection ends.
 func (r *Runner) light(ctx context.Context, lc *lightConn) {
+	first := time.Now()
 	for attempt := 1; ; attempt++ {
 		start := time.Now()
 		gw, why := r.lightConnect(ctx, lc, start.Add(r.cfg.Timeout))
 		if why == "" {
-			r.rec.Online(lc.i, gw, start, time.Now())
+			now := time.Now()
+			r.rec.Online(lc.i, gw, start, now)
+			r.rec.LoginLog(lc.i, first, now, attempt)
 			break
 		}
 		r.rec.Failed(why)
@@ -58,6 +61,7 @@ func (r *Runner) light(ctx context.Context, lc *lightConn) {
 		}
 		if attempt == lightAttempts {
 			r.rec.Failed("gave-up")
+			r.rec.LoginLog(lc.i, first, time.Time{}, attempt)
 			return
 		}
 		select {
diff --git a/internal/loadgen/light_test.go b/internal/loadgen/light_test.go
index 8d6d1e7..fa1fe49 100644
--- a/internal/loadgen/light_test.go
+++ b/internal/loadgen/light_test.go
@@ -304,6 +304,9 @@ func TestLightGivesUpAfterItsAttempts(t *testing.T) {
 		if c.Online != 0 || c.Logins != 0 {
 			t.Errorf("online %d, logins %d, want none", c.Online, c.Logins)
 		}
+		if l := r.Recorder().Logins(); l.FirstMs[0] != start.UnixMilli() || l.EndMs[0] != 0 || l.Attempts[0] != lightAttempts {
+			t.Errorf("login log %d %d %d, want the start, 0 and %d attempts", l.FirstMs[0], l.EndMs[0], l.Attempts[0], lightAttempts)
+		}
 		if lo, hi := (lightAttempts-1)*500*time.Millisecond, (lightAttempts-1)*1500*time.Millisecond; waited < lo || waited > hi {
 			t.Errorf("gave up after %v of waits, want %v-%v", waited, lo, hi)
 		}
diff --git a/internal/loadgen/recorder.go b/internal/loadgen/recorder.go
index 884730f..8bf9e83 100644
--- a/internal/loadgen/recorder.go
+++ b/internal/loadgen/recorder.go
@@ -2,6 +2,7 @@ package loadgen
 
 import (
 	"fmt"
+	"slices"
 	"sort"
 	"sync"
 	"time"
@@ -38,6 +39,7 @@ type Recorder struct {
 	pongRTT        *hist
 	marks          []*mark // by index
 	byName         map[string]*mark
+	loginLog       *LoginLogs // light mode's sequences (#139), made by the first LoginLog
 }
 
 // clientState: gw is the gateway it is logged in on (-1: none); mark and
@@ -114,6 +116,45 @@ func (r *Recorder) Lost(i int, why string, now time.Time) {
 	}
 }
 
+// LoginLogs is each client's latest sequence of login attempts, by client
+// index (GET /logins, #139): its first attempt's start (ms), when it logged
+// in (ms; 0: it gave up), and its attempts. A client with no sequence ended
+// yet is 0 in all three. Light mode only: a Tier 2 scenario reads it per
+// Redis shard (Plan 6c2b).
+type LoginLogs struct {
+	FirstMs  []int64 `json:"first_ms"`
+	EndMs    []int64 `json:"end_ms"`
+	Attempts []int32 `json:"attempts"`
+}
+
+// LoginLog: client i's sequence of attempts started at first and logged in
+// at end after attempts (end zero: it gave up). It replaces the client's
+// earlier sequence.
+func (r *Recorder) LoginLog(i int, first, end time.Time, attempts int) {
+	r.mu.Lock()
+	defer r.mu.Unlock()
+	if r.loginLog == nil {
+		n := len(r.clients)
+		r.loginLog = &LoginLogs{FirstMs: make([]int64, n), EndMs: make([]int64, n), Attempts: make([]int32, n)}
+	}
+	r.loginLog.FirstMs[i], r.loginLog.EndMs[i], r.loginLog.Attempts[i] = first.UnixMilli(), 0, int32(attempts)
+	if !end.IsZero() {
+		r.loginLog.EndMs[i] = end.UnixMilli()
+	}
+}
+
+// Logins is a copy of the login log, zeros for every client before the
+// first LoginLog.
+func (r *Recorder) Logins() LoginLogs {
+	r.mu.Lock()
+	defer r.mu.Unlock()
+	if r.loginLog == nil {
+		n := len(r.clients)
+		return LoginLogs{FirstMs: make([]int64, n), EndMs: make([]int64, n), Attempts: make([]int32, n)}
+	}
+	return LoginLogs{FirstMs: slices.Clone(r.loginLog.FirstMs), EndMs: slices.Clone(r.loginLog.EndMs), Attempts: slices.Clone(r.loginLog.Attempts)}
+}
+
 // Gateways is the gateway index each client is logged in on now, -1 for
 // none (GET /clients, Plan 6b3c review O4).
 func (r *Recorder) Gateways() []int32 {
diff --git a/internal/loadgen/recorder_test.go b/internal/loadgen/recorder_test.go
index af24ce2..e018673 100644
--- a/internal/loadgen/recorder_test.go
+++ b/internal/loadgen/recorder_test.go
@@ -212,3 +212,26 @@ func TestRecorderCountsTheStormSinceMark(t *testing.T) {
 		t.Fatalf("storm since mark %+v", s)
 	}
 }
+
+// The login log keeps each client's latest sequence of attempts: its first
+// attempt's start, when it logged in (0: it gave up) and its attempts
+// (#139). A client with no sequence ended is all zeros.
+func TestRecorderLogsLoginSequences(t *testing.T) {
+	r := NewRecorder(3, []string{"gw-0"})
+	if l := r.Logins(); len(l.FirstMs) != 3 || l.FirstMs[0] != 0 || l.EndMs[0] != 0 || l.Attempts[0] != 0 {
+		t.Fatalf("empty log %+v", l)
+	}
+	r.LoginLog(0, at(100), at(2600), 3)
+	r.LoginLog(1, at(200), time.Time{}, 10)
+	r.LoginLog(0, at(5000), at(5010), 1) // a later sequence replaces the first
+	l := r.Logins()
+	if l.FirstMs[0] != at(5000).UnixMilli() || l.EndMs[0] != at(5010).UnixMilli() || l.Attempts[0] != 1 {
+		t.Fatalf("client 0: %d %d %d", l.FirstMs[0], l.EndMs[0], l.Attempts[0])
+	}
+	if l.FirstMs[1] != at(200).UnixMilli() || l.EndMs[1] != 0 || l.Attempts[1] != 10 {
+		t.Fatalf("client 1 (gave up): %d %d %d", l.FirstMs[1], l.EndMs[1], l.Attempts[1])
+	}
+	if l.FirstMs[2] != 0 || l.Attempts[2] != 0 {
+		t.Fatalf("client 2: %d %d", l.FirstMs[2], l.Attempts[2])
+	}
+}
```

- [ ] **Step 2: The tests fail on the old code**

Run: `go vet ./internal/loadgen`
Expected: `internal/loadgen/light_test.go:307:24: r.Recorder().Logins undefined (type *Recorder has no field or method Logins, but does have field logins)` (vet stops at the first package error).

- [ ] **Step 3: The code**

`git apply --exclude='*_test.go' PATCH`.

- [ ] **Step 4: The tests pass**

Run: `go vet ./internal/loadgen && go test -race -count=1 ./internal/loadgen`
Expected: `ok` (~1 min on the dev machine).

Mutation: in `internal/loadgen/light.go` replace `r.rec.LoginLog(lc.i, first, now, attempt)` with `r.rec.LoginLog(lc.i, start, now, 1)`, run `go test -count=1 ./internal/loadgen -run TestLightRetriesAPreHandshakeClose`: FAIL at `/logins {UIDPrefix:retry ...}, want 3 attempts over at least 1s from the first`. Undo it.

- [ ] **Step 5: Commit**

```bash
git add internal/loadgen/control.go internal/loadgen/e2e_test.go internal/loadgen/light.go internal/loadgen/light_test.go internal/loadgen/recorder.go internal/loadgen/recorder_test.go
git commit -m "feat: log each light client's login sequence in loadgen

GET /logins gives each client's latest sequence of login
attempts: the first attempt's start, the login time (0: gave up)
and the attempts. Plan 6c2b reads it per Redis shard.

Refs: #139"
```

---

### Task 3: The stack's failover helpers

**Files:**
- Create: `scripts/stack_failover_test.sh` (mode 755)
- Modify: `scripts/stack.sh`, `scripts/redis-cluster.sh`, `Makefile`

**Interfaces:**
- Consumes: Task 1's Logic `-redis-reload` default (the test's logins after the kill need it); `scripts/tier2/reconnect.sh`'s `rc_sessions` (6c1) and `scripts/loadgen.sh`'s `lg_in`.
- Produces (shell, for 6c2b), all in `scripts/stack.sh`, Cluster mode unless named:
  - `stack_redis_ping NAME` (true: PONG within 1s); `stack_redis_live` (first answering node of `stack_redis_names`, "" none); `stack_redis_cli` now uses it on a Cluster (fails if none answers); `stack_redis_slots` reads `CLUSTER NODES` through it (P6c1I-1).
  - `STACK_REDIS_TIMEOUT` (seconds; bounds `stack_redis_node_cli` when set).
  - `stack_redis_role NAME` → `master` | `replica` | `down`; `stack_redis_roles` → `{"redis-0": "master", ...}`.
  - `stack_redis_owner KEY` → the master name owning KEY's slot now (fails if none); `stack_lease_key GW` → `gwlease:GW:{EPOCH}` of GW's newest `gwepochs` member.
  - `stack_redis_kill NAME` (SIGKILL + `stack_expect_dead NAME 5`: `NAME expected:137` in `exit-status`); `stack_redis_restart NAME` (same port, args and `nodes.conf`, pinned to `T2_CPUS_LOGIC`; old log → `logs/NAME.log.N`; waits 10s for PING).
  - `stack_redis_stop NAME`, `stack_redis_cont NAME` (SIGSTOP/SIGCONT; `STACK_REDIS_STOPPED[NAME]`; `stack_down` resumes them).
  - `stack_redis_wait_role NAME ROLE SECONDS` (50ms polls); `stack_redis_failover NAME` (`CLUSTER FAILOVER` on a replica, master within 10s).
  - `stack_redis_wait_whole SECONDS`; `stack_redis_restore SECONDS` (the start roles of `redis/layout` back; fails naming the node otherwise).
  - `stack_redis_resetstat`, `stack_redis_node_latency`, `stack_redis_latency`, `stack_snapshot` skip a node that does not answer within 1s (null, or "no answer within 1s" in its `.info`).
- Produces in `scripts/redis-cluster.sh`: `redis_cluster_wait CLI SECONDS PORT...` (the join's end condition, generalised: every node `cluster_state:ok`, sees 3 `slave` and no `fail`/`fail?` flag, every node that is a replica now has `master_link_status:up`; each call bounded by 1s); `REDIS_CLUSTER_NODE_TIMEOUT_MS` default 10000.

- [ ] **Step 1: The test**

Save the patch below and apply only the new test and the Makefile: `git apply --include='scripts/stack_failover_test.sh' --include=Makefile PATCH`, then check the mode: `test -x scripts/stack_failover_test.sh`.

```diff
diff --git a/Makefile b/Makefile
index 73bc46a..2876157 100644
--- a/Makefile
+++ b/Makefile
@@ -31,6 +31,7 @@ test-scripts:
 	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_cluster_test.sh
+	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_failover_test.sh
 	GOIM_REDIS_SERVER=$(REDIS_SERVER) scripts/deploy_test.sh
 
 # The Tier 1a load probe (Plan 6a2, spec: ≤ 5k connections), on the dev
diff --git a/scripts/redis-cluster.sh b/scripts/redis-cluster.sh
index 29bdd98..b0456a6 100644
--- a/scripts/redis-cluster.sh
+++ b/scripts/redis-cluster.sh
@@ -8,11 +8,14 @@
 #
 #   redis_cluster_layout          # "NODE ROLE ZONE MASTER" per node: redis-0..2 masters, redis-3..5 replicas
 #   redis_cluster_args PORT BUS DIR   # sets REDIS_CLUSTER_ARGS: one node's redis-server flags
-#   redis_cluster_join CLI PORT... # the 6 ports in layout order: create, attach replicas, wait until
-#                                 #   every node reports cluster_state:ok and every replica is linked
+#   redis_cluster_join CLI PORT... # the 6 ports in layout order: create, attach replicas, then
+#                                 #   redis_cluster_wait
+#   redis_cluster_wait CLI SECONDS PORT...  # until every node reports cluster_state:ok, sees 3
+#                                 #   replicas and no failed node, and every replica is linked
 #
 # Settings: REDIS_CLUSTER_NODE_TIMEOUT_MS (cluster-node-timeout, default
-# 15000, Redis's default; Plan 6c2 measures the failover it gives);
+# 10000: a SIGKILLed master fails over in ~16.5s, inside the lease rule's
+# TTL 30s - interval 10s; Redis's 15000 took 20.6-22.7s, #139);
 # REDIS_CLUSTER_JOIN_TIMEOUT_S (how long the join waits, default 30).
 
 redis_cluster_layout() {
@@ -27,7 +30,7 @@ redis_cluster_args() {
   REDIS_CLUSTER_ARGS=(--port "$1" --cluster-port "$2" --bind 127.0.0.1 --dir "$3"
     --save "" --appendonly no --maxmemory-policy noeviction
     --cluster-enabled yes --cluster-config-file nodes.conf
-    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-15000}"
+    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-10000}"
     --repl-diskless-sync yes --repl-diskless-sync-delay 0)
 }
 
@@ -55,24 +58,37 @@ redis_cluster_join() {
       --cluster-slave --cluster-master-id "$id" >/dev/null ||
       { echo "redis-cluster: replica on port ${ports[i + 3]} not added" >&2; return 1; }
   done
-  deadline=$((SECONDS + ${REDIS_CLUSTER_JOIN_TIMEOUT_S:-30}))
+  redis_cluster_wait "$cli" "${REDIS_CLUSTER_JOIN_TIMEOUT_S:-30}" "${ports[@]}"
+}
+
+# redis_cluster_wait CLI SECONDS PORT...: waits until every node answers,
+# reports cluster_state:ok, sees 3 replicas and no node flagged fail or
+# fail?, and every node that is a replica now has its master link up: the
+# join's end, and a Cluster whole again after a failover (Plan 6c2a). Each
+# call is bounded by 1s, so a stopped node fails the wait, not hangs it.
+# Fails, naming what did not happen, after SECONDS.
+redis_cluster_wait() {
+  local cli=$1 secs=$2 p ok deadline
+  shift 2
+  deadline=$((SECONDS + secs))
   while :; do
     ok=1
     # Every node must also see the 3 replicas: gossip spreads the new roles
     # after the replicas link, and a client that loads the slot map from a
     # node that has not heard yet sees 6 masters.
-    for p in "${ports[@]}"; do
-      "$cli" -p "$p" cluster info 2>/dev/null | grep -q '^cluster_state:ok' || ok=""
-      [ "$("$cli" -p "$p" cluster nodes 2>/dev/null | grep -cE '[ ,]slave ')" = 3 ] || ok=""
-    done
-    for p in "${ports[@]:3}"; do
-      "$cli" -p "$p" info replication 2>/dev/null | grep -q '^master_link_status:up' || ok=""
+    for p in "$@"; do
+      timeout 1 "$cli" -p "$p" cluster info 2>/dev/null | grep -q '^cluster_state:ok' || { ok=""; break; }
+      timeout 1 "$cli" -p "$p" cluster nodes 2>/dev/null | awk '$3 ~ /(^|,)slave(,|$)/ { s++ } $3 ~ /(^|,)fail\??(,|$)/ { f++ }
+        END { exit !(s == 3 && f == 0) }' || { ok=""; break; }
+      if timeout 1 "$cli" -p "$p" info replication 2>/dev/null | grep -q '^role:slave'; then
+        timeout 1 "$cli" -p "$p" info replication 2>/dev/null | grep -q '^master_link_status:up' || { ok=""; break; }
+      fi
     done
     [ -n "$ok" ] && return 0
     [ "$SECONDS" -ge "$deadline" ] && break
     sleep 0.1
   done
-  echo "redis-cluster: not every node reports cluster_state:ok with its replicas linked within ${REDIS_CLUSTER_JOIN_TIMEOUT_S:-30}s" >&2
-  "$cli" -p "${ports[0]}" cluster nodes >&2
+  echo "redis-cluster: not every node reports cluster_state:ok with its replicas linked within ${secs}s" >&2
+  timeout 1 "$cli" -p "$1" cluster nodes >&2
   return 1
 }
diff --git a/scripts/stack.sh b/scripts/stack.sh
index d7840e1..e8e653c 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -28,10 +28,28 @@
 #   stack_hist_quantile A B Q   # Q's bucket bound in ms over two stack_buckets readings (null: no sample)
 #   stack_redis_names           # the Redis processes: "redis", or redis-0..5 on a Cluster
 #   stack_redis_node_cli NAME ARGS...  # redis-cli against one Redis process
-#   stack_redis_slots           # "START END NAME" per master's slot range ("0 16383 redis" on one Redis)
+#   stack_redis_ping NAME       # true if NAME answers PING within 1s (a stopped node does not)
+#   stack_redis_live            # the first Redis process that answers ("": none)
+#   stack_redis_slots           # "START END NAME" per master's slot range ("0 16383 redis" on one Redis),
+#                               #   as the first node that answers sees it
 #   stack_redis_resetstat       # Redis CONFIG RESETSTAT on every node: a latency window starts
 #   stack_redis_latency         # {"CMD": {"calls", "p50_us", "p99_us", "p999_us"}...} since the reset (null: no answer);
 #                               #   on a Cluster {NAME: {"role", "zone", "cmds": {"CMD": ...}}...}
+#   Cluster failover (Plan 6c2a; each node keeps its name, port and nodes.conf):
+#   stack_redis_role NAME       # master, replica, or down (no answer within 1s)
+#   stack_redis_roles           # {"redis-0": "master", ...}: every node's role now
+#   stack_redis_owner KEY       # the master that owns KEY's slot now
+#   stack_lease_key GW          # gwlease:GW:{EPOCH} of GW's newest epoch in gwepochs
+#   stack_redis_kill NAME       # SIGKILL NAME and reap it (stack_expect_dead)
+#   stack_redis_restart NAME    # start a killed NAME again with its nodes.conf: it rejoins
+#   stack_redis_stop NAME       # SIGSTOP NAME: its sockets stay open, nothing answers (a cut-off zone)
+#   stack_redis_cont NAME       # SIGCONT NAME
+#   stack_redis_wait_role NAME ROLE SECONDS  # until NAME's role is ROLE; fails after SECONDS
+#   stack_redis_failover NAME   # CLUSTER FAILOVER on replica NAME: it becomes master within 10s
+#   stack_redis_wait_whole SECONDS  # until every node answers, reports cluster_state:ok, sees 3
+#                               #   replicas and no failed node, and every replica is linked
+#   stack_redis_restore SECONDS # the start layout back (redis/layout): each start master that is a
+#                               #   replica takes its role back; then stack_redis_wait_whole
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
 #   stack_health                # one line per problem: dead process, unclean stop, panic in a log
 #
@@ -44,7 +62,8 @@
 #                        below the race suite's 20000-32000 and the ephemeral range)
 #   STACK_REDIS          single (default): one redis-server; cluster: 3 masters and 3
 #                        replicas (scripts/redis-cluster.sh: zones z0-z2 as labels,
-#                        layout in redis/layout), Logic with -redis-cluster
+#                        layout in redis/layout, cluster-node-timeout 10s), Logic with
+#                        -redis-cluster
 #   STACK_PIN            1: taskset gateways to T2_CPUS_GATEWAY, Logic and Redis
 #                        to T2_CPUS_LOGIC (lib.sh sets both); 0: no pinning (default)
 #   STACK_REDIS_SERVER   default: redis-server on PATH, else .cache/redis/bin/redis-server
@@ -307,7 +326,7 @@ stack_snapshot() {
   stack_redis_cli info all >"$dir/redis.info" 2>&1
   if [ "${STACK_REDIS:-single}" = cluster ]; then
     for name in $(stack_redis_names); do
-      stack_redis_node_cli "$name" info all >"$dir/$name.info" 2>&1
+      if stack_redis_ping "$name"; then stack_redis_node_cli "$name" info all >"$dir/$name.info" 2>&1; else echo "no answer within 1s" >"$dir/$name.info"; fi
     done
     stack_redis_cli cluster nodes >"$dir/cluster-nodes.txt" 2>&1
   fi
@@ -319,7 +338,8 @@ stack_redis_names() {
 }
 
 # stack_redis_node_cli NAME ARGS...: redis-cli against one Redis process
-# (redis-j on port base + 20 + j), no redirects followed.
+# (redis-j on port base + 20 + j), no redirects followed; bounded by
+# STACK_REDIS_TIMEOUT seconds when it is set.
 stack_redis_node_cli() {
   local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
   local base=${STACK_PORT_BASE:-17000} port name=$1
@@ -329,20 +349,47 @@ stack_redis_node_cli() {
     redis-[0-5]) port=$((base + 20 + ${name#redis-})) ;;
     *) echo "stack: no Redis process $name" >&2; return 1 ;;
   esac
-  "$(dirname "$redis")/redis-cli" -p "$port" "$@"
+  if [ -n "${STACK_REDIS_TIMEOUT:-}" ]; then
+    timeout "$STACK_REDIS_TIMEOUT" "$(dirname "$redis")/redis-cli" -p "$port" "$@"
+  else
+    "$(dirname "$redis")/redis-cli" -p "$port" "$@"
+  fi
+}
+
+# stack_redis_ping NAME: true if NAME answers PING within 1s. A stopped
+# node (stack_redis_stop) accepts the connection and never answers.
+stack_redis_ping() { [ "$(STACK_REDIS_TIMEOUT=1 stack_redis_node_cli "$1" ping 2>/dev/null)" = PONG ]; }
+
+# stack_redis_live: the first process in stack_redis_names that answers
+# PING; nothing if none does.
+stack_redis_live() {
+  local name
+  for name in $(stack_redis_names); do
+    stack_redis_ping "$name" && { echo "$name"; return; }
+  done
 }
 
 # stack_redis_cli ARGS...: redis-cli against the stack's Redis: the one
-# redis-server, or on a Cluster redis-0 (a key it does not own answers
-# MOVED: per-key commands go through stack_redis_slots, review P6b3c2I-3).
+# redis-server, or on a Cluster the first node that answers (review
+# P6c1I-1: redis-0 may be down after a failover; a key a node does not own
+# answers MOVED: per-key commands go through stack_redis_slots, review
+# P6b3c2I-3). Fails if no node answers.
 stack_redis_cli() {
-  if [ "${STACK_REDIS:-single}" = cluster ]; then stack_redis_node_cli redis-0 "$@"; else stack_redis_node_cli redis "$@"; fi
+  if [ "${STACK_REDIS:-single}" = cluster ]; then
+    local live
+    live=$(stack_redis_live)
+    [ -n "$live" ] || return 1
+    stack_redis_node_cli "$live" "$@"
+  else
+    stack_redis_node_cli redis "$@"
+  fi
 }
 
 # stack_redis_slots: "START END NAME" for each master's slot range, from
-# CLUSTER NODES as redis-0 sees it now (after a failover too; a master
-# only suspected, "fail?", keeps its range until it is marked "fail"); "0 16383
-# redis" on one Redis. Prints nothing if Redis does not answer.
+# CLUSTER NODES as the first node that answers sees it now (after a failover
+# too; a master only suspected, "fail?", keeps its range until it is marked
+# "fail", review P6c1I-3); "0 16383 redis" on one Redis. Prints nothing if
+# Redis does not answer.
 stack_redis_slots() {
   if [ "${STACK_REDIS:-single}" != cluster ]; then
     stack_redis_cli ping >/dev/null 2>&1 && echo "0 16383 redis"
@@ -360,7 +407,7 @@ stack_redis_slots() {
 stack_redis_resetstat() {
   local name
   for name in $(stack_redis_names); do
-    [ "$(stack_redis_node_cli "$name" config resetstat 2>/dev/null)" = OK ] || return 1
+    stack_redis_ping "$name" && [ "$(stack_redis_node_cli "$name" config resetstat 2>/dev/null)" = OK ] || return 1
   done
 }
 
@@ -368,9 +415,10 @@ stack_redis_resetstat() {
 # last resetstat, from INFO latencystats (p50, p99 and p99.9 in µs, Redis 7's
 # defaults) and the calls from INFO commandstats, as {"CMD": {"calls",
 # "p50_us", "p99_us", "p999_us"}, ...}. The readers' own INFO and CONFIG
-# are left out. Prints null if the node does not answer.
+# are left out. Prints null if the node does not answer (within 1s).
 stack_redis_node_latency() {
   local info
+  stack_redis_ping "$1" || { echo null; return; }
   info=$(stack_redis_node_cli "$1" info latencystats 2>/dev/null && stack_redis_node_cli "$1" info commandstats 2>/dev/null) &&
     grep -q '^# Latencystats' <<<"$info" || { echo null; return; }
   awk -F'[:,=]' '{ sub(/\r$/, "") }
@@ -394,18 +442,140 @@ stack_redis_latency() {
   local name role zone cmds out="" any=""
   for name in $(stack_redis_names); do
     cmds=$(stack_redis_node_latency "$name")
-    role=$(stack_redis_node_cli "$name" info replication 2>/dev/null | tr -d '\r' | sed -n 's/^role://p')
     zone=$(awk -v n="$name" '$1 == n { print $4 }' "$STACK_DIR/redis/layout" 2>/dev/null)
     if [ "$cmds" = null ]; then
       out+="${out:+, }\"$name\": null"
     else
       any=1
-      out+="${out:+, }\"$name\": {\"role\": \"$([ "$role" = slave ] && echo replica || echo "${role:-unknown}")\", \"zone\": \"$zone\", \"cmds\": $cmds}"
+      role=$(stack_redis_role "$name")
+      out+="${out:+, }\"$name\": {\"role\": \"$([ "$role" = down ] && echo unknown || echo "$role")\", \"zone\": \"$zone\", \"cmds\": $cmds}"
     fi
   done
   if [ -n "$any" ]; then echo "{$out}"; else echo null; fi
 }
 
+# stack_redis_role NAME: master or replica, as NAME itself says now; down
+# if it does not answer within 1s.
+stack_redis_role() {
+  local role
+  role=$(STACK_REDIS_TIMEOUT=1 stack_redis_node_cli "$1" info replication 2>/dev/null | tr -d '\r' | sed -n 's/^role://p')
+  case $role in master) echo master ;; slave) echo replica ;; *) echo down ;; esac
+}
+
+# stack_redis_roles: {"redis-0": "master", ...} for every node, now.
+stack_redis_roles() {
+  local name out=""
+  for name in $(stack_redis_names); do out+="${out:+, }\"$name\": \"$(stack_redis_role "$name")\""; done
+  echo "{$out}"
+}
+
+# stack_redis_owner KEY: the master that owns KEY's slot now (CLUSTER
+# KEYSLOT on a live node, then stack_redis_slots). Fails if none does.
+stack_redis_owner() {
+  local slot
+  slot=$(stack_redis_cli cluster keyslot "$1" 2>/dev/null) && [ -n "$slot" ] || return 1
+  stack_redis_slots | awk -v s="$slot" '$1 <= s && s <= $2 { print $3; found = 1 } END { exit !found }'
+}
+
+# stack_lease_key GW: GW's lease key, gwlease:GW:{EPOCH}, for its newest
+# epoch in gwepochs (the highest score: a gateway started again has two).
+# Fails if GW has none.
+stack_lease_key() {
+  local owner epoch
+  owner=$(stack_redis_owner gwepochs) || return 1
+  epoch=$(stack_redis_node_cli "$owner" zrange gwepochs 0 -1 withscores 2>/dev/null | paste - - |
+    awk -v g="$1" 'index($1, g ":") == 1 && $2 > best { best = $2; e = substr($1, length(g) + 2) } END { if (e != "") print e }')
+  [ -n "$epoch" ] || return 1
+  echo "gwlease:$1:{$epoch}"
+}
+
+# stack_redis_kill NAME: SIGKILL NAME and reap it (stack_expect_dead: its
+# exit is recorded as expected, so stack_health does not report it).
+stack_redis_kill() {
+  [ -n "${STACK_PIDS[$1]:-}" ] || { echo "stack: no process $1" >&2; return 1; }
+  kill -KILL "${STACK_PIDS[$1]}" && stack_expect_dead "$1" 5
+}
+
+# stack_redis_restart NAME: starts a killed Cluster node again on its port,
+# pinned as before, with its directory and nodes.conf kept (review
+# P6c1I-2: not stack_redis_cluster_up, which makes new ones), so it rejoins
+# the Cluster as it was: a master that failed over comes back as a replica
+# of its replacement. Its earlier log moves to NAME.log.N. Waits up to 10s
+# for its PING.
+stack_redis_restart() {
+  local name=$1 base=${STACK_PORT_BASE:-17000} i j n=1
+  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
+  [[ $name == redis-[0-5] ]] || { echo "stack: $name is not a Cluster node" >&2; return 1; }
+  [ -z "${STACK_PIDS[$name]:-}" ] || { echo "stack: $name is still running" >&2; return 1; }
+  [ -f "$STACK_DIR/redis/$name/nodes.conf" ] || { echo "stack: $name has no nodes.conf" >&2; return 1; }
+  while [ -e "$STACK_DIR/logs/$name.log.$n" ]; do n=$((n + 1)); done
+  mv "$STACK_DIR/logs/$name.log" "$STACK_DIR/logs/$name.log.$n"
+  . "$(dirname "${BASH_SOURCE[0]}")/redis-cluster.sh"
+  j=${name#redis-}
+  redis_cluster_args $((base + 20 + j)) $((base + 30 + j)) "$STACK_DIR/redis/$name"
+  stack_start "$name" "${T2_CPUS_LOGIC:-}" "$redis" "${REDIS_CLUSTER_ARGS[@]}"
+  for i in $(seq 100); do
+    stack_redis_ping "$name" && return 0
+    sleep 0.1
+  done
+  echo "stack: $name not answering within 10s of its restart" >&2
+  return 1
+}
+
+# stack_redis_stop NAME, stack_redis_cont NAME: SIGSTOP and SIGCONT. A
+# stopped node keeps its sockets: clients and peers see no answer, not a
+# refusal, as from a zone cut off by the network. stack_down resumes a
+# stopped node before it stops it.
+declare -A STACK_REDIS_STOPPED=()
+stack_redis_stop() { kill -STOP "${STACK_PIDS[$1]:?no process $1}" && STACK_REDIS_STOPPED[$1]=1; }
+stack_redis_cont() { kill -CONT "${STACK_PIDS[$1]:?no process $1}" && unset "STACK_REDIS_STOPPED[$1]"; }
+
+# stack_redis_wait_role NAME ROLE SECONDS: polls every 50ms until NAME says
+# its role is ROLE (master, replica, down); fails after SECONDS.
+stack_redis_wait_role() {
+  local deadline=$((SECONDS + $3))
+  until [ "$(stack_redis_role "$1")" = "$2" ]; do
+    [ "$SECONDS" -ge "$deadline" ] && return 1
+    sleep 0.05
+  done
+}
+
+# stack_redis_failover NAME: CLUSTER FAILOVER on replica NAME, the manual
+# failover (its master stops writes, the replica catches up and takes over:
+# no acknowledged write is lost); waits up to 10s for NAME to be master.
+stack_redis_failover() {
+  [ "$(stack_redis_role "$1")" = replica ] || { echo "stack: $1 is not a replica" >&2; return 1; }
+  [ "$(stack_redis_node_cli "$1" cluster failover 2>/dev/null)" = OK ] || return 1
+  stack_redis_wait_role "$1" master 10
+}
+
+# stack_redis_wait_whole SECONDS: redis_cluster_wait over the 6 nodes: the
+# Cluster is whole again (every node answers, cluster_state:ok, 3 replicas
+# linked, no node flagged fail or fail?).
+stack_redis_wait_whole() {
+  local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
+  local base=${STACK_PORT_BASE:-17000} j ports=()
+  . "$(dirname "${BASH_SOURCE[0]}")/redis-cluster.sh"
+  for j in 0 1 2 3 4 5; do ports+=($((base + 20 + j))); done
+  redis_cluster_wait "$(dirname "$redis")/redis-cli" "$1" "${ports[@]}"
+}
+
+# stack_redis_restore SECONDS: the start layout back, so the next phase
+# starts from it (zones: one master in each): the Cluster whole, then a
+# manual failover on each start master (redis/layout) that is a replica now,
+# then whole again with every node in its start role. Fails after SECONDS.
+stack_redis_restore() {
+  local deadline=$((SECONDS + $1)) name port role zone master
+  stack_redis_wait_whole "$1" || return 1
+  while read -r name port role zone master; do
+    [ "$role" = master ] && [ "$(stack_redis_role "$name")" = replica ] && { stack_redis_failover "$name" || return 1; }
+  done <"$STACK_DIR/redis/layout"
+  stack_redis_wait_whole $((deadline - SECONDS > 1 ? deadline - SECONDS : 1)) || return 1
+  while read -r name port role zone master; do
+    [ "$(stack_redis_role "$name")" = "$role" ] || { echo "stack: $name is $(stack_redis_role "$name"), its start role is $role" >&2; return 1; }
+  done <"$STACK_DIR/redis/layout"
+}
+
 # stack_profile NAME SECONDS: a CPU profile of Logic and every running
 # gateway over the same SECONDS, into artifacts/NAME/<process>.cpu.pb.gz.
 # Blocks for SECONDS; a scenario takes it inside its measured window. Fails
@@ -735,7 +905,9 @@ stack_down() {
     [ -n "${STACK_PIDS[$name]:-}" ] && stack_term 5 "$name"
   done
   # Redis last; a Cluster's nodes together, so no replica is left to
-  # fail over a stopped master.
+  # fail over a stopped master. A node stopped by stack_redis_stop is
+  # resumed first: a stopped process does not act on SIGTERM.
+  for name in "${!STACK_REDIS_STOPPED[@]}"; do stack_redis_cont "$name"; done
   local rs=()
   for name in "${STACK_ORDER[@]}"; do
     [[ $name == redis || $name == redis-* ]] && rs+=("$name")
diff --git a/scripts/stack_failover_test.sh b/scripts/stack_failover_test.sh
new file mode 100755
index 0000000..e736d8a
--- /dev/null
+++ b/scripts/stack_failover_test.sh
@@ -0,0 +1,101 @@
+#!/usr/bin/env bash
+# Tests for scripts/stack.sh's Cluster failover helpers (Plan 6c2a) on any
+# machine (make test-scripts): a SIGKILLed master fails over, the slot map
+# and the session check come from a live node (review P6c1I-1), the node
+# restarts with its nodes.conf and rejoins as a replica (P6c1I-2), the
+# start layout comes back, a stopped zone (one master, one other master's
+# replica) fails over and returns, and nothing hangs on a stopped node.
+# The gateway's lease and new logins live through all of it. cluster-node-
+# timeout is 2s here (a failover in ~3s). Needs redis-server (make
+# redis-server) and curl.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+. scripts/tier2/lib.sh
+. scripts/stack.sh
+. scripts/loadgen.sh
+eval "$(sed -n '/^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_sessions() {/,/^}/p' scripts/tier2/reconnect.sh)"
+STACK_DIR=$(mktemp -d)
+trap 'lg_stop_all; stack_down; rm -rf "$STACK_DIR"' EXIT
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+missing() { stack_metric gw-0 goim_gateway_lease_calls_total 'result="missing"'; }
+
+STACK_GATEWAYS=1
+STACK_PORT_BASE=${STACK_PORT_BASE:-17700}
+STACK_PIN=1
+ncpu=$(getconf _NPROCESSORS_ONLN)
+T2_CPUS_GATEWAY=0-$((ncpu - 1))
+T2_CPUS_LOGIC=0
+STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000)
+STACK_REDIS=cluster
+export REDIS_CLUSTER_NODE_TIMEOUT_MS=2000
+start_roles='{"redis-0": "master", "redis-1": "master", "redis-2": "master", "redis-3": "replica", "redis-4": "replica", "redis-5": "replica"}'
+
+expect "default node timeout: 10s (#139)" "$(unset REDIS_CLUSTER_NODE_TIMEOUT_MS; . scripts/redis-cluster.sh; redis_cluster_args 1 2 d; echo "${REDIS_CLUSTER_ARGS[@]}" | grep -o 'cluster-node-timeout [0-9]*')" "cluster-node-timeout 10000"
+
+stack_build || { echo "build failed"; exit 1; }
+if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi
+expect "roles: the start layout" "$(stack_redis_roles)" "$start_roles"
+lg_in a lg_start a -mode light -clients 30 -rate 100 -uid-prefix fa && lg_in a lg_wait_online 30 30 && ok "loadgen: 30 online" || bad "loadgen: not online"
+key=$(stack_lease_key gw-0)
+case $key in gwlease:gw-0:\{[0-9]*\}) ok "lease key: $key" ;; *) bad "lease key: [$key]" ;; esac
+expect "lease key: owned by a master" "$(stack_redis_owner "$key" | grep -cE '^redis-[012]$')" 1
+expect "owner: a fixed key's slot (gwepochs, 5482) on redis-1" "$(stack_redis_owner gwepochs)" redis-1
+m0=$(missing)
+
+# A SIGKILLed master: the slot map now comes from redis-1 (P6c1I-1), its
+# replica takes over, the lease and new logins live on.
+stack_redis_kill redis-0 && ok "kill: redis-0 reaped" || bad "kill: redis-0"
+expect "kill: recorded as expected" "$(grep -c '^redis-0 expected:137$' "$STACK_DIR/exit-status")" 1
+expect "live: the first node that answers" "$(stack_redis_live)" redis-1
+expect "slots: 3 ranges from a live node" "$(stack_redis_slots | wc -l)" 3
+expect "role: a killed node is down" "$(stack_redis_role redis-0)" down
+t=$SECONDS
+stack_redis_wait_role redis-3 master 15 && ok "failover: redis-3 is master after $((SECONDS - t))s" || bad "failover: redis-3 not master"
+until stack_redis_slots | grep -q '^0 5460 redis-3$' || [ $((SECONDS - t)) -gt 15 ]; do sleep 0.2; done
+expect "slots: redis-0's range on redis-3" "$(stack_redis_slots | paste -sd, -)" "0 5460 redis-3,5461 10922 redis-1,10923 16383 redis-2"
+lg_in b lg_start b -mode light -clients 30 -rate 100 -uid-prefix fb && lg_in b lg_wait_online 30 30 && ok "logins: 30 more online after the failover" || bad "logins: after the failover"
+out=$(stack_redis_wait_whole 2 2>&1) && bad "whole: a dead node must fail the wait" || ok "whole: not whole with a node dead"
+
+# The restart rejoins redis-0 as redis-3's replica (its nodes.conf kept);
+# then the start layout comes back.
+stack_redis_restart redis-0 && ok "restart: redis-0 answers" || bad "restart: redis-0"
+stack_redis_wait_role redis-0 replica 10 && ok "restart: redis-0 rejoins as a replica" || bad "restart: redis-0 is $(stack_redis_role redis-0)"
+expect "restart: it follows redis-3" "$(stack_redis_node_cli redis-0 info replication | tr -d '\r' | sed -n 's/^master_port://p')" $((STACK_PORT_BASE + 23))
+[ -s "$STACK_DIR/logs/redis-0.log.1" ] && ok "restart: the first log kept" || bad "restart: logs/redis-0.log.1 missing"
+stack_redis_wait_whole 30 && ok "whole: after the restart" || bad "whole: after the restart"
+stack_redis_restore 30 && ok "restore: done" || bad "restore"
+expect "restore: the start layout" "$(stack_redis_roles)" "$start_roles"
+expect "restore: redis-0's range back" "$(stack_redis_slots | head -n 1)" "0 5460 redis-0"
+expect "sessions: every client of a and b found after the failover" "$(get "$(lg_in a rc_sessions 0)" 'd["found"]'),$(get "$(lg_in b rc_sessions 0)" 'd["found"]')" "30,30"
+
+# Zone z0 stopped (redis-0, a master, and redis-5, redis-2's replica): no
+# reader hangs on it, redis-3 takes over, and on SIGCONT the zone returns.
+stack_redis_stop redis-0 && stack_redis_stop redis-5 && ok "zone: z0 stopped"
+t=$SECONDS
+expect "role: a stopped node is down" "$(stack_redis_role redis-0)" down
+lat=$(stack_redis_latency)
+expect "latency: the stopped nodes are null, the others read" "$(get "$lat" '[n for n in sorted(d) if d[n] is None]')" "['redis-0', 'redis-5']"
+[ $((SECONDS - t)) -le 6 ] && ok "zone: readers bounded ($((SECONDS - t))s)" || bad "zone: readers took $((SECONDS - t))s"
+out=$(stack_redis_wait_whole 2 2>&1) && bad "whole: a stopped zone must fail the wait" || ok "whole: not whole with a zone stopped"
+stack_redis_wait_role redis-3 master 15 && ok "zone: redis-3 is master" || bad "zone: redis-3 not master"
+expect "zone: a key of redis-2's range (slot 16287) still on redis-2" "$(stack_redis_owner '{x}')" redis-2
+stack_redis_cont redis-0 && stack_redis_cont redis-5 && ok "zone: z0 back"
+stack_redis_wait_role redis-0 replica 15 && ok "zone: the old master returns as a replica" || bad "zone: redis-0 is $(stack_redis_role redis-0)"
+stack_redis_restore 60 && ok "restore: after the zone" || bad "restore: after the zone"
+expect "restore: the start layout again" "$(stack_redis_roles)" "$start_roles"
+expect "lease: no missing reply through both failovers" "$(missing)" "$m0"
+lg_in c lg_start c -mode light -clients 30 -rate 100 -uid-prefix fc && lg_in c lg_wait_online 30 30 && ok "logins: 30 more online after the zone" || bad "logins: after the zone"
+
+# A stopped node at stack_down is resumed and stops cleanly.
+stack_redis_stop redis-4
+lg_stop_all
+stack_down
+expect "down: every node stops with 0" "$(grep -cE '^redis-[0-5] 0$' "$STACK_DIR/exit-status")" 6
+expect "health: the kill was expected, nothing else" "$(stack_health 2>&1)" ""
+
+echo "$fails failure(s)"
+[ "$fails" -eq 0 ]
```

- [ ] **Step 2: The test fails on the old scripts**

Run: `STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_failover_test.sh 2>&1 | grep -E '^(FAIL|[0-9]+ failure)'`
Expected: 25 `FAIL` lines and `25 failure(s)`: first `FAIL default node timeout: 10s (#139): got [cluster-node-timeout 15000], want [cluster-node-timeout 10000]`, then every check of a new helper (`roles: the start layout: got []`, `lease key: []`, `owner: …`, `kill: redis-0`, `live: …`, `failover: redis-3 not master`, `restart: …`, `restore …`, `zone: …`). The stack comes up and the logins pass (Tasks 1–2 are in).

- [ ] **Step 3: The scripts**

`git apply --exclude='scripts/stack_failover_test.sh' --exclude=Makefile PATCH`.

- [ ] **Step 4: Every test passes**

Run: `bash -n scripts/stack.sh scripts/redis-cluster.sh && make test-scripts`
Expected: every suite ends with `0 failure(s)` (or its own all-pass line); `stack_failover_test.sh` prints 40 `ok` lines and `0 failure(s)` in ~70s.

Run: `make test-redis-cluster-local`
Expected: every package `ok` (the test Cluster now runs node timeout 10s; no test kills a node).

Mutation: in `cmd/logic/main.go` set the `-redis-reload` default to `time.Minute`, `go build -o /dev/null ./cmd/logic`, run `scripts/stack_failover_test.sh` (as in Step 2): `2 failure(s)`: `FAIL logins: after the failover` (`[19, {"busy": 109, "gave-up": 11, "pre-handshake": 2}]`) and `FAIL sessions: … got [30,19]`. Undo it.

- [ ] **Step 5: Commit, push**

```bash
git add Makefile scripts/redis-cluster.sh scripts/stack.sh scripts/stack_failover_test.sh
git commit -m "feat: drive Redis Cluster failovers from the Tier 2 stack

stack.sh gains the failover helpers: kill, restart with nodes.conf
kept, stop and continue a zone, a manual failover, wait for a role
or a whole Cluster, and restore the start layout. The slot map and
the session check read the first node that answers. No reader
hangs on a stopped node. cluster-node-timeout is now 10s.

Refs: #139, P6c1I-1, P6c1I-2, P6c1I-3"
git push origin feat/plan6c
```

- [ ] **Step 6: Record**

In this plan's *Execution Notes*: the commits, the Step 2 and Step 4 outputs, the mutations. In the roadmap: the plan 6 row (6c2a executed) and #139's outcome. No Tier 2 run in this plan; the final review and the interface review follow the usual workflow.

---

## After this plan

- **Plan 6c2b** (written in a fresh session): scenario `failover` (`scripts/tier2/failover.sh`, `RECONNECT_REDIS`-style opt-in is not needed: the scenario is Cluster-only) with phases `manual` (`CLUSTER FAILOVER` of the lease key's shard), `kill` (SIGKILL of the lease key's master, ~9s after its gateway's last renewal, #139.3) and `zone` (SIGSTOP of the lease key's master's zone: that master and another master's replica, #139.2), each from the start layout (`stack_redis_restore`) with a light login flow (`GET /logins`) before, through and after the failover. Readings and checks to decide there: the failover time (the replica's `role:master`, then `cluster_state:ok`) against TTL − interval (20s); the lease (`lease_calls_total{result="missing"}` delta 0, no sweep); logins on the other shards in the failover window against the window before (the spec: "keep their normal p99", R5S-O1; a bound such as 2× to propose); the failed shard's affected share (retries, gave-ups, `EXPIRED`/`busy`) recorded; lost writes (`rc_sessions` after `stack_redis_wait_whole`, P6c1I-3); Revoke on the shard that lost its replica in the zone phase (`not_covered` or a reading). Run 34.
- Logic's `-redis-failover` (15s, #21) is below the failover this Cluster gives (~16.5s at node timeout 10s): 6c2b sets it from run 34's measurement or records why not.
- Review Focus 1 and 4 are 6c2b's readings.
- Then **Plan 6c3** (`gwconns` shard load, deadline sizing), then **6d**.

## Execution Notes

(Filled in after execution.)
