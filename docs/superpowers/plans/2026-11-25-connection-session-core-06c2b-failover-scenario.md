# Plan 6c2b: Redis Failover Scenario Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the lease live through a zone loss at the defaults (#140), then run the spec's *Redis failover test* (R2-O5, R5S-O1, R5S-O9) as the Tier 2 scenario `failover` and record it as run 34.

**Architecture:** Four tasks:
- **The remedy** (Task 1, production code and the test Cluster). Logic's go-redis client gets `-redis-read-timeout` (default 1s, also the write timeout; go-redis's own is 5s). The Cluster that `scripts/redis-cluster.sh` builds runs `cluster-node-timeout` 5s (was 10s) and `cluster-require-full-coverage no` (#142, found by this plan's dry run). Measured on dev before this plan (#140): with both, a stopped zone's master fails over in 7.1–9.2s and gw-0's first renewal after the stop lands at L + 18.2–20.6s (L = its last renewal; the key expires at L + 30s).
- **The session check in `lib.sh`** (Task 2, a move). `rc_sessions` and its three helpers move from `reconnect.sh` to `lib.sh` as `t2_sessions*`, so `failover.sh` and `stack_failover_test.sh` use them without the `sed` extraction.
- **Scenario `failover`** (Task 3, scripts). One stack, one ramp of 600k light clients (#141.1, #141.4), then the phases `manual`, `kill`, `zone` on the shard that holds gw-0's lease key, each from the start layout (`stack_redis_restore`). Each phase has three login windows (before, through, after the failover), each a new light loadgen instance whose `GET /logins` is grouped by shard.
- **Run 34** (Task 4): the user runs it on the Tier 2 host; the report is recorded.

**Tech Stack:** Go 1.27.1, go-redis v9.23.0, bash 5.2, python3, the pinned `redis-server` 7.4.11 (`make redis-server`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`: *Verification* → Redis failover test (~l.535), *Storms, drain, failures* → Redis shard failover and Zone loss (Redis) (~l.419–420), *Redis schema* → `gwlease:` (the lease rule, R2-O5, ~l.296), *Deployment requirements* → Redis placement (R5S-O9, ~l.73), *Replication* (R2-O4, ~l.305). Nothing here changes it; #142 and #140 are inputs to its next revision.

Inputs:
- The roadmap's **#139** (6c2's decisions), **#140** (the remedy: both a 1s read timeout and node timeout 5s; amends #139.1), **#141** (the scenario's shape), #21 (Logic's `-redis-failover` 15s).
- Plan 6c2a's *After this plan* and its interface review P6c2aI-1..6 (`docs/superpowers/plans/2026-10-25-connection-session-core-06c2a-failover-tools.md`, *Execution Notes*).

**Decisions (the user, 2026-11-25, with AskUserQuestion, before writing):**
1. **#140, the remedy: both.** Measured on dev with gw-0's lease key's master and another master's replica stopped ~9s after gw-0's last renewal. At 6c2a's defaults (node timeout 10s, go-redis `ReadTimeout` 5s) the key expired in 2 of 3 trials. `ReadTimeout` 1s alone: 1 of 5 expired; node timeout 5s alone: 0 of 5 with under 2s of margin; both: 0 of 5, ~10s of margin.
2. **#141, the shape:** 600k light clients online through every phase plus a small login flow per window; the other shards' login p99 in the failover window at most max(2 × the before window's, + 50ms), a gate; Revoke at zone loss `not_covered`; one stack, `stack_redis_restore` before each phase.
3. **One plan for all of 6c2b** (the remedy, the scenario and run 34).
4. **#142, not full coverage** (the user, 2026-11-25, with AskUserQuestion, after this plan's first dry run): with Redis's default `cluster-require-full-coverage yes`, from the moment a master is marked failed until its replica takes over (0.9–1.1s in the dry run, `Cluster state changed: fail` … `ok` in every node's log), every node refuses every command (`CLUSTERDOWN`). Logins on the other shards failed once and retried: their p99 in the failover window was 1,194ms (kill) and 1,446ms (zone) against 12–13ms before; the slow ones all started 7.3–9.1s after the trigger, at the promotion. Every login, lease call and Lua script here uses one slot (a Cluster refuses a script across slots, `CROSSSLOT`), so none needs the whole key space at once. The test Cluster now runs `cluster-require-full-coverage no`: a failed master stops only its own slots. The spec's next revision adds it to *Deployment requirements* → Redis placement.

**Branch:** `feat/plan6c2b` (from `main` `f2beeda`; the #140/#141 record is `b51e936`). Execute from its head, this plan's own commit or later.

**Size:** 4 tasks. Task 1: ~60 lines (Logic's client and flag, the two Cluster settings). Task 2: a move of ~100 lines (and the new names in 3 tests). Task 3: ~480 lines (`failover.sh` ~420, `failover_test.sh` ~75). Task 4: the run and its record. The prototype `proto-6c2b` (from `b51e936`; tasks `6e16533`, `e6ea511`, `8aef70c`; pushed) ran every step; its diff is replayed here byte for byte, one `git apply` patch per task. On the dev machine (AMD EPYC 7742, 2 cores):
- `go vet ./...` clean; `make test` (race), `make test-redis-local`, `make test-redis-cluster-local`: 20 packages `ok` each; `make test-scripts`: every suite's all-pass line (`failover_test` 25 checks; `stack_failover_test.sh` 0 failures with the 2 new checks). One `lib_test.sh` run beside the Go suites failed `windows: two named windows pass` (its fake window's swap check: the dev box swapped under the parallel suites); it passed 3 times alone and in the final `make test-scripts`.
- Each task's tests fail on the old code as Step 2 says; mutations as each Step 4 says.
- The dry run (Task 3 Step 5, 3,000 clients): 30 checks; the lease lived in every phase (`missing` 0); found #142 on the first run and #144 on the second (Decisions, Task 3 Step 5).

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...`, `make test-redis-local`, `make test-redis-cluster-local` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- Production changes only in `cmd/logic` (`-redis-read-timeout`, `newRedis`). No change to the store, the gateway, the SDK, the wire or any Lua.
- Lease timing stays as the spec and #139 have it: interval 10s, TTL 30s, call timeout 5s, retry 1s. Logic's `-redis-failover` stays 15s (#21; P6c2aI-4: above the measured failovers, ≤ 9.2s at node timeout 5s, and below TTL − interval = 20s, which Logic's start check needs).
- The test Cluster runs `cluster-require-full-coverage no` (#142) in every suite that builds one (`make test-redis-cluster-local`, `stack_cluster_test.sh`, `stack_failover_test.sh`, the Tier 2 stack).
- `REDIS_CLUSTER_NODE_TIMEOUT_MS` still overrides the node timeout (`stack_failover_test.sh` keeps 2s). `STACK_REDIS=single` stays the stack's default; `failover` sets `cluster` itself.
- The pinned `redis-server` 7.4.11; no Docker for the Cluster (#66).
- The new scenario uses the stack's usual ports (`STACK_PORT_BASE` 17000), the CPU sets of `reconnect` (run 33: loadgen `0-5,10,12-17,22`, gateways `6-9,18-21`, Logic and the 6 Redis nodes `11,23`) and at most 2 loadgen instances (`main`, `flow`; `LG_MAX_INSTANCES` is 4).

## Review Focus

1. **A Redis reply slower than 1s now fails the call** (#140's recorded risk). go-redis also sets its pool wait to `ReadTimeout` + 1s, so a command that waits for a pooled connection now fails after 2s (was 6s). Under a login storm a long EVAL or a full pool gives `error` where it gave a late answer. No test here loads Logic: run 34's flow is 200 logins/s beside idle clients. `WAIT` is not affected (go-redis gives it its own timeout + 10s; Revoke's `-wait-timeout` is 1s). Plan 6c3 (`gwconns` shard load) is where a slow reply would show; its plan reads Logic's store errors.
2. **A 5s node timeout fails over a master that stalls for 5s** (#140's other risk). In run 34 the returning node's full resync makes its master fork (diskless sync) with ~200k sessions; a fork that stalls the master ≥ 5s would start a second failover. `P_whole` records the roles after the rejoin and `P_restore` the next phase's start; neither names the cause. Look at the Redis logs (`stack/logs/redis-*.log`, "Marking node … as failing") when either fails.
3. **The worst time is gw-0's.** The trigger waits until gw-0's key has ~21s of TTL left (9s after its renewal). The other two gateways' keys are on whatever shard their epoch hashes to, at a random time in their interval: their `missing` is gated too (`P_lease`), but only gw-0 is tested at the worst time. The result records each key's shard (`lease_keys`).
4. **A short failover window.** In `manual` the failover takes milliseconds: the window (the trigger to the promotion + 1s) holds ~1–2s of the flow, ~270 other-shard logins at 200/s, so the p99 is about the 3rd largest. The 50ms floor (#141.2) covers that noise; the count is in `P_other_shards` (`failover_window.others.clients`).
5. **`gwepochs` in the stopped zone** (P6c2aI-2). `gwepochs` is on `redis-1` in the start layout. When gw-0's key is on `redis-1` too, the `zone` phase stops `gwepochs`' master and every gateway's lease call waits ~1s per try until the failover: `error` replies, each key still renewed. `P_lease` gates on `missing` and the sweep, not on `error` (P6c2aI-3); `P_failover` records `gwepochs_on`.
6. **Logins to a stopped shard hold the login workers (#144, found by the dry run; recorded, not fixed here).** `zone_other_shards` is expected to fail in run 34. Task 3 Step 5 has the numbers; *After this plan* names the fix's plan.

Each line's test (6 has none: it is a known failure, measured by run 34): 1 and 2 are run 34's readings (no dev test can load a Cluster to 600k); 3: `P_lease` covers every gateway; 4: `failover_test.sh`'s nearest-rank and floor checks (`other: 60 within 10 + 50`); 5: `P_lease`'s rule has no `error` term (Task 3's dry run reads `error` > 0 in `zone`).

## Implementation choices this plan makes

These are **#143.1–#143.7** in the roadmap (proposed with this plan; decided by the user 2026-11-25, all as proposed).
1. **The target is gw-0's lease key's shard**, the same in every phase (gw-0 keeps its epoch). Its replica is the one that takes over (`redis/layout`); the zone is the target's zone (the target and the other master's replica there).
2. **The failover window** for the p99 rule: the sequences of the through window whose first attempt started from the trigger to the replica's promotion + 1s (`master_ms`; not `cluster_state:ok`, which with #142 a live node may report throughout). The before p99 is over the whole before window. p99 is the nearest rank over the logged-in sequences (first attempt's start to login, retries included).
3. **`P_failover`'s bound is 20s** (TTL − interval, the spec's lease rule), not Logic's `-redis-failover` 15s, which stays (Global Constraints).
4. **Lost writes are recorded, not repaired in the run.** Reconcile's cycle is 10 minutes; waiting for it would add ~11 minutes per phase. The main clients' sessions (written minutes before the failover) are gated in every phase; the through window's missing sessions are recorded (`through_lost`).
5. **The flow:** 200 logins/s; windows 30s, 40s (through), 30s; light clients (up to 10 attempts, 0.5–1.5s apart); every flow client must log in in the end (`P_logins`), the failing shard's included.
6. **The lease gate:** no `missing` reply on any gateway or in Logic and no sweep deletion, from the through window's start to the end of the rejoin, and each gateway's `live` renewals at least `t2_lease_floor` of the window; `error` replies recorded (P6c2aI-3).
7. **The restore is a check of its own**, at each phase's start (the first is a no-op), and the rejoin after the failover is `P_whole`, both with their times: Review Focus 5 of 6c2a (a 600k full resync).

## File Structure

- `cmd/logic/main.go`, `main_test.go`: `-redis-read-timeout`, `newRedis` (Task 1).
- `scripts/redis-cluster.sh`, `scripts/stack.sh` (header), `scripts/stack_failover_test.sh`: node timeout 5s, not full coverage (Task 1).
- `scripts/tier2/lib.sh`: `t2_sessions`, `t2_sessions_cmp`, `t2_sessions_cmds`, `t2_sessions_join` (Task 2, moved from `reconnect.sh`).
- `scripts/tier2/reconnect.sh`, `reconnect_test.sh`, `scripts/stack_failover_test.sh`, `scripts/stack_cluster_test.sh`: the new names (Task 2).
- `scripts/tier2/failover.sh` (new), `scripts/tier2/failover_test.sh` (new), `Makefile` (`test-scripts`) (Task 3).
- The roadmap (#142 and #144 new, #143 proposed, the plan 6 row), this plan's *Execution Notes* (Tasks 1–4).

---

### Task 1: Logic's read timeout and the test Cluster's settings

**Files:**
- Modify: `cmd/logic/main.go`, `scripts/redis-cluster.sh`, `scripts/stack.sh` (header only)
- Test: `cmd/logic/main_test.go`, `scripts/stack_failover_test.sh`

**Interfaces:**
- Produces (flag): Logic `-redis-read-timeout` (default 1s; refused ≤ 0, since go-redis reads 0 as 5s and −1 as none), the read and the write timeout of both clients.
- Produces (Go, unexported): `newRedis(o options) redis.UniversalClient` in `cmd/logic`: `*redis.ClusterClient` (with `ClusterStateReloadInterval: o.redisReload`) unless `-redis-cluster=false`, then `*redis.Client`. `run` calls it.
- Produces (scripts): `redis_cluster_args` adds `--cluster-node-timeout 5000` (default; `REDIS_CLUSTER_NODE_TIMEOUT_MS` overrides) and `--cluster-require-full-coverage no`.
- Unchanged: `-redis-reload` (1s; its help now names `-redis-read-timeout` in place of "3s"), Revoke's `WAIT` (go-redis gives it its own timeout + 10s).

- [ ] **Step 1: The tests**

Save the patch below to a file in the session scratchpad and apply only its test files from the repository root: `git apply --include='*_test.go' --include='*_test.sh' PATCH`.

```diff
diff --git a/cmd/logic/main.go b/cmd/logic/main.go
index 6895ad7..5e663b6 100644
--- a/cmd/logic/main.go
+++ b/cmd/logic/main.go
@@ -32,14 +32,15 @@ func main() {
 
 // options are the flags that are not part of logic.Config.
 type options struct {
-	redisAddrs   string
-	redisCluster bool
-	redisReload  time.Duration
-	jwksURL      string
-	jwksFallback string
-	jwks         auth.ManagerConfig // its intervals; URL and fallback come from the two flags above
-	kickAddrFile string
-	faults       bool
+	redisAddrs       string
+	redisCluster     bool
+	redisReload      time.Duration
+	redisReadTimeout time.Duration
+	jwksURL          string
+	jwksFallback     string
+	jwks             auth.ManagerConfig // its intervals; URL and fallback come from the two flags above
+	kickAddrFile     string
+	faults           bool
 }
 
 // parseFlags returns the validated configuration. Logic refuses to start on
@@ -50,7 +51,8 @@ func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error
 	fs := flag.NewFlagSet("logic", flag.ContinueOnError)
 	fs.StringVar(&o.redisAddrs, "redis", "127.0.0.1:6379", "Redis address(es), comma-separated")
 	fs.BoolVar(&o.redisCluster, "redis-cluster", true, "Redis Cluster (false: one node, for tests)")
-	fs.DurationVar(&o.redisReload, "redis-reload", time.Second, "Redis Cluster: reload the slot map when it is older than this (#139); a failed-over master's slots reach its replica within it, plus the read timeout (3s) for each node that does not answer the reload")
+	fs.DurationVar(&o.redisReload, "redis-reload", time.Second, "Redis Cluster: reload the slot map when it is older than this (#139); a failed-over master's slots reach its replica within it, plus -redis-read-timeout for each node that does not answer the reload")
+	fs.DurationVar(&o.redisReadTimeout, "redis-read-timeout", time.Second, "Redis read and write timeout per command (go-redis's is 5s; WAIT adds its own); a command to a node that does not answer fails after it (#140)")
 	fs.StringVar(&o.jwksURL, "jwks-url", "", "account service JWKS URL")
 	fs.StringVar(&o.jwksFallback, "jwks-fallback", "/etc/goim/jwks-fallback/jwks.json", "jwks-fallback ConfigMap file")
 	fs.DurationVar(&o.jwks.Refresh, "jwks-refresh", o.jwks.Refresh, "periodic JWKS refresh")
@@ -105,26 +107,37 @@ func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error
 	if o.redisReload <= 0 { // go-redis reads 0 as its 60s default
 		return cfg, sc, o, errors.New("config: -redis-reload must be positive")
 	}
+	if o.redisReadTimeout <= 0 { // go-redis reads 0 as its 5s default, -1 as none
+		return cfg, sc, o, errors.New("config: -redis-read-timeout must be positive")
+	}
 	return cfg, sc, o, nil
 }
 
+// newRedis is Logic's Redis client: a Cluster client unless -redis-cluster
+// is false (tests), with -redis-read-timeout on every command.
+func newRedis(o options) redis.UniversalClient {
+	addrs := strings.Split(o.redisAddrs, ",")
+	if !o.redisCluster {
+		return redis.NewClient(&redis.Options{Addr: addrs[0], ReadTimeout: o.redisReadTimeout, WriteTimeout: o.redisReadTimeout})
+	}
+	// The client reloads its slot map after a MOVED, ASK or READONLY
+	// reply, or once it is older than ClusterStateReloadInterval (60s by
+	// default). A master that died answers nothing, so without a short
+	// interval its slots stay on the dead address for up to a minute
+	// after its replica took over (Plan 6c2a prototype, #139). A stopped
+	// node (a cut-off zone) answers nothing too: each command and each
+	// reload that reaches it waits the read timeout, so at go-redis's 5s a
+	// lease could expire before Logic finds the new master (#140).
+	return redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, ClusterStateReloadInterval: o.redisReload,
+		ReadTimeout: o.redisReadTimeout, WriteTimeout: o.redisReadTimeout})
+}
+
 func run(args []string) error {
 	cfg, sc, o, err := parseFlags(args)
 	if err != nil {
 		return err
 	}
-	addrs := strings.Split(o.redisAddrs, ",")
-	var rdb redis.UniversalClient
-	if o.redisCluster {
-		// The client reloads its slot map after a MOVED, ASK or READONLY
-		// reply, or once it is older than ClusterStateReloadInterval (60s by
-		// default). A master that died answers nothing, so without a short
-		// interval its slots stay on the dead address for up to a minute
-		// after its replica took over (Plan 6c2a prototype, #139).
-		rdb = redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs, ClusterStateReloadInterval: o.redisReload})
-	} else {
-		rdb = redis.NewClient(&redis.Options{Addr: addrs[0]})
-	}
+	rdb := newRedis(o)
 	defer rdb.Close()
 	var faults *fault.Set
 	if o.faults {
diff --git a/cmd/logic/main_test.go b/cmd/logic/main_test.go
index 004180e..755dcc9 100644
--- a/cmd/logic/main_test.go
+++ b/cmd/logic/main_test.go
@@ -4,6 +4,8 @@ import (
 	"strings"
 	"testing"
 	"time"
+
+	"github.com/redis/go-redis/v9"
 )
 
 func TestDefaultFlagsAreValid(t *testing.T) {
@@ -11,7 +13,7 @@ func TestDefaultFlagsAreValid(t *testing.T) {
 	if err != nil {
 		t.Fatal(err)
 	}
-	if cfg.Store.LeaseTTL != 30*time.Second || sc.MaxConnectionAgeGrace != 30*time.Second || !o.redisCluster || o.redisReload != time.Second {
+	if cfg.Store.LeaseTTL != 30*time.Second || sc.MaxConnectionAgeGrace != 30*time.Second || !o.redisCluster || o.redisReload != time.Second || o.redisReadTimeout != time.Second {
 		t.Fatalf("defaults: %+v %+v %+v", cfg, sc, o)
 	}
 }
@@ -23,6 +25,36 @@ func TestRefusesZeroRedisReload(t *testing.T) {
 	}
 }
 
+// A zero -redis-read-timeout would be go-redis's 5s default, a negative one
+// no timeout at all (#140).
+func TestRefusesNonPositiveRedisReadTimeout(t *testing.T) {
+	for _, v := range []string{"0s", "-1s"} {
+		if _, _, _, err := parseFlags([]string{"-jwks-url", "x", "-redis-read-timeout", v}); err == nil || !strings.Contains(err.Error(), "-redis-read-timeout") {
+			t.Fatalf("-redis-read-timeout %s: %v", v, err)
+		}
+	}
+}
+
+// Both clients get the read and write timeout (#140) and the Cluster client
+// the reload interval (#139).
+func TestRedisClientTimeouts(t *testing.T) {
+	_, _, o, err := parseFlags([]string{"-jwks-url", "x", "-redis", "127.0.0.1:1,127.0.0.1:2", "-redis-read-timeout", "700ms"})
+	if err != nil {
+		t.Fatal(err)
+	}
+	c := newRedis(o).(*redis.ClusterClient)
+	defer c.Close()
+	if co := c.Options(); co.ReadTimeout != 700*time.Millisecond || co.WriteTimeout != 700*time.Millisecond || co.ClusterStateReloadInterval != time.Second {
+		t.Fatalf("cluster: read %v write %v reload %v", co.ReadTimeout, co.WriteTimeout, co.ClusterStateReloadInterval)
+	}
+	o.redisCluster = false
+	s := newRedis(o).(*redis.Client)
+	defer s.Close()
+	if so := s.Options(); so.ReadTimeout != 700*time.Millisecond || so.WriteTimeout != 700*time.Millisecond {
+		t.Fatalf("single: read %v write %v", so.ReadTimeout, so.WriteTimeout)
+	}
+}
+
 // The test timing profile (Plan 5) scales intervals ~10× and keeps the ratios.
 func TestScaledProfileFlags(t *testing.T) {
 	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s",
diff --git a/scripts/redis-cluster.sh b/scripts/redis-cluster.sh
index 24103bb..31585bb 100644
--- a/scripts/redis-cluster.sh
+++ b/scripts/redis-cluster.sh
@@ -14,8 +14,10 @@
 #                                 #   replicas and no failed node, and every replica is linked
 #
 # Settings: REDIS_CLUSTER_NODE_TIMEOUT_MS (cluster-node-timeout, default
-# 10000: a SIGKILLed master fails over in ~16.5s, inside the lease rule's
-# TTL 30s - interval 10s; Redis's 15000 took 20.6-22.7s, #139);
+# 5000: a SIGKILLed master fails over in ~7.8s, a stopped zone's in
+# 7.1-9.2s, well inside the lease rule's TTL 30s - interval 10s; 10000 took
+# ~16.5s and let a lease expire at zone loss, #140; Redis's 15000 took
+# 20.6-22.7s, #139);
 # REDIS_CLUSTER_JOIN_TIMEOUT_S (how long the join waits, default 30).
 
 redis_cluster_layout() {
@@ -25,12 +27,15 @@ redis_cluster_layout() {
 
 # redis_cluster_args PORT BUS DIR: nothing persisted, noeviction (as the
 # single Redis), the cluster bus on BUS (not PORT + 10000, which lands in
-# the race suite's 20000-32000), nodes.conf in DIR.
+# the race suite's 20000-32000), nodes.conf in DIR. Not full coverage: a
+# master marked failed stops only its own slots; with Redis's default every
+# node refuses every command (CLUSTERDOWN) until its replica takes over,
+# ~1s, so logins on the other shards failed too (Plan 6c2b's dry run, #142).
 redis_cluster_args() {
   REDIS_CLUSTER_ARGS=(--port "$1" --cluster-port "$2" --bind 127.0.0.1 --dir "$3"
     --save "" --appendonly no --maxmemory-policy noeviction
     --cluster-enabled yes --cluster-config-file nodes.conf
-    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-10000}"
+    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-5000}" --cluster-require-full-coverage no
     --repl-diskless-sync yes --repl-diskless-sync-delay 0)
 }
 
diff --git a/scripts/stack.sh b/scripts/stack.sh
index 86d85e8..4b10f07 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -62,7 +62,7 @@
 #                        below the race suite's 20000-32000 and the ephemeral range)
 #   STACK_REDIS          single (default): one redis-server; cluster: 3 masters and 3
 #                        replicas (scripts/redis-cluster.sh: zones z0-z2 as labels,
-#                        layout in redis/layout, cluster-node-timeout 10s), Logic with
+#                        layout in redis/layout, cluster-node-timeout 5s), Logic with
 #                        -redis-cluster
 #   STACK_PIN            1: taskset gateways to T2_CPUS_GATEWAY, Logic and Redis
 #                        to T2_CPUS_LOGIC (lib.sh sets both); 0: no pinning (default)
diff --git a/scripts/stack_failover_test.sh b/scripts/stack_failover_test.sh
index b1f177c..9853873 100755
--- a/scripts/stack_failover_test.sh
+++ b/scripts/stack_failover_test.sh
@@ -36,7 +36,8 @@ STACK_REDIS=cluster
 export REDIS_CLUSTER_NODE_TIMEOUT_MS=2000
 start_roles='{"redis-0": "master", "redis-1": "master", "redis-2": "master", "redis-3": "replica", "redis-4": "replica", "redis-5": "replica"}'
 
-expect "default node timeout: 10s (#139)" "$(unset REDIS_CLUSTER_NODE_TIMEOUT_MS; . scripts/redis-cluster.sh; redis_cluster_args 1 2 d; echo "${REDIS_CLUSTER_ARGS[@]}" | grep -o 'cluster-node-timeout [0-9]*')" "cluster-node-timeout 10000"
+expect "default node timeout: 5s (#140)" "$(unset REDIS_CLUSTER_NODE_TIMEOUT_MS; . scripts/redis-cluster.sh; redis_cluster_args 1 2 d; echo "${REDIS_CLUSTER_ARGS[@]}" | grep -o 'cluster-node-timeout [0-9]*')" "cluster-node-timeout 5000"
+expect "not full coverage: a failed master stops only its own slots (#142)" "$(. scripts/redis-cluster.sh; redis_cluster_args 1 2 d; echo "${REDIS_CLUSTER_ARGS[@]}" | grep -o 'cluster-require-full-coverage [a-z]*')" "cluster-require-full-coverage no"
 
 stack_build || { echo "build failed"; exit 1; }
 if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi
@@ -50,11 +51,16 @@ m0=$(missing)
 
 # A SIGKILLed master: the slot map now comes from redis-1 (P6c1I-1), its
 # replica takes over, the lease and new logins live on.
+t0k=$SECONDS
 stack_redis_kill redis-0 && ok "kill: redis-0 reaped" || bad "kill: redis-0"
 expect "kill: recorded as expected" "$(grep -c '^redis-0 expected:137$' "$STACK_DIR/exit-status")" 1
 expect "live: the first node that answers" "$(stack_redis_live)" redis-1
 expect "slots: 3 ranges from a live node" "$(stack_redis_slots | wc -l)" 3
 expect "role: a killed node is down" "$(stack_redis_role redis-0)" down
+# Before redis-3 takes over, redis-0 is marked failed (2 x the 2s node
+# timeout at most); redis-1 still serves its own slots ({c}: slot 7365) (#142).
+until stack_redis_node_cli redis-1 cluster nodes 2>/dev/null | grep -q 'master,fail ' || [ $((SECONDS - t0k)) -gt 10 ]; do sleep 0.05; done
+expect "kill: another shard answers while redis-0 is failed" "$(STACK_REDIS_TIMEOUT=1 stack_redis_node_cli redis-1 set '{c}' 1 2>&1)" OK
 t=$SECONDS
 stack_redis_wait_role redis-3 master 15 && ok "failover: redis-3 is master after $((SECONDS - t))s" || bad "failover: redis-3 not master"
 until stack_redis_slots | grep -q '^0 5460 redis-3$' || [ $((SECONDS - t)) -gt 15 ]; do sleep 0.2; done
```

- [ ] **Step 2: The tests fail on the old code**

Run: `go vet ./cmd/logic/`
Expected: a type-check error, `o.redisReadTimeout undefined` (vet stops at the first).

Run: `STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_failover_test.sh 2>&1 | grep -E '^FAIL|failure'`
Expected: `3 failure(s)`: `FAIL default node timeout: 5s (#140): got [cluster-node-timeout 10000] …`, `FAIL not full coverage: … got []`, `FAIL kill: another shard answers while redis-0 is failed: got [CLUSTERDOWN The cluster is down], want [OK]`.

- [ ] **Step 3: The code**

Apply the rest of the patch: `git apply --exclude='*_test.go' --exclude='*_test.sh' PATCH`.

- [ ] **Step 4: The tests pass, and the settings are what passes them**

Run: `go vet ./cmd/logic/ && go test -count=1 ./cmd/logic/`
Expected: `ok`.

Run: `STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_failover_test.sh 2>&1 | tail -1` and `make test-redis-cluster-local 2>&1 | grep -c '^ok'`
Expected: `0 failure(s)`; `20`.

Mutations (undo each):
- In `newRedis`, drop `ReadTimeout: o.redisReadTimeout, ` from the `ClusterOptions`: `TestRedisClientTimeouts` FAIL `cluster: read 5s write 700ms reload 1s`.
- In `scripts/redis-cluster.sh`, `--cluster-require-full-coverage yes`: `stack_failover_test.sh` `2 failure(s)`: `not full coverage: …` and `kill: another shard answers while redis-0 is failed: got [CLUSTERDOWN The cluster is down], want [OK]`.

- [ ] **Step 5: Commit**

```bash
git add cmd/logic scripts/redis-cluster.sh scripts/stack.sh scripts/stack_failover_test.sh
git commit -m "fix: bound Logic's Redis calls and tune the test Cluster" # body: STE, the #140/#142 numbers; refs on their own line
```

---

### Task 2: The session check in lib.sh

**Files:**
- Modify: `scripts/tier2/lib.sh` (the four functions appended), `scripts/tier2/reconnect.sh` (removed there, one call renamed)
- Test: `scripts/tier2/reconnect_test.sh`, `scripts/stack_failover_test.sh`, `scripts/stack_cluster_test.sh` (the new names; no `sed` extraction of them)

**Interfaces:**
- Produces (bash, in `lib.sh`; bodies unchanged, names only): `t2_sessions MARK_MS` (was `rc_sessions`), `t2_sessions_cmp CLIENTS SESSIONS MARK_MS [MS]`, `t2_sessions_cmds CLIENTS SLOTS DIR`, `t2_sessions_join CLIENTS DIR`. `t2_sessions` acts on the current loadgen instance (`lg_in INST t2_sessions 0` for another) and writes `$STACK_DIR/artifacts/clients.json`, `sessions.txt`, `sessions.err` and `$STACK_DIR/sessions/`, as before.

- [ ] **Step 1: The tests**

Save the patch below and apply only its tests: `git apply --include='*_test.sh' PATCH`.

```diff
diff --git a/scripts/stack_cluster_test.sh b/scripts/stack_cluster_test.sh
index 8f5403a..0706b53 100755
--- a/scripts/stack_cluster_test.sh
+++ b/scripts/stack_cluster_test.sh
@@ -10,7 +10,6 @@ cd "$(git rev-parse --show-toplevel)"
 . scripts/tier2/lib.sh
 . scripts/stack.sh
 . scripts/loadgen.sh
-eval "$(sed -n '/^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_sessions() {/,/^}/p' scripts/tier2/reconnect.sh)"
 STACK_DIR=$(mktemp -d)
 trap 'lg_stop; stack_down; rm -rf "$STACK_DIR"' EXIT
 fails=0
@@ -79,7 +78,7 @@ expect "latency: every node, its role and zone" "$(get "$lat" '[(n, d[n]["role"]
 expect "latency: logins' scripts on every master" "$(get "$lat" 'all(d[n]["cmds"].get("evalsha", {}).get("calls", 0) > 0 for n in ("redis-0", "redis-1", "redis-2"))')" True
 expect "latency: no reader's own commands" "$(get "$lat" 'any(k == "info" or k.startswith(("config", "cluster")) for n in d for k in d[n]["cmds"])')" False
 mkdir -p "$STACK_DIR/artifacts"
-s=$(rc_sessions 0)
+s=$(t2_sessions 0)
 expect "sessions: every client found per slot" "$(get "$s" '(d["clients"], d["found"])')" "(30, 30)"
 expect "sessions: the check's time is recorded" "$(get "$s" 'd.get("ms", -1) >= 0')" True
 expect "sessions: no CROSSSLOT" "$(cat "$STACK_DIR/sessions"/*.out | grep -c CROSSSLOT)" 0
diff --git a/scripts/stack_failover_test.sh b/scripts/stack_failover_test.sh
index 9853873..cfb8da2 100755
--- a/scripts/stack_failover_test.sh
+++ b/scripts/stack_failover_test.sh
@@ -15,7 +15,6 @@ cd "$(git rev-parse --show-toplevel)"
 . scripts/tier2/lib.sh
 . scripts/stack.sh
 . scripts/loadgen.sh
-eval "$(sed -n '/^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_sessions() {/,/^}/p' scripts/tier2/reconnect.sh)"
 STACK_DIR=$(mktemp -d)
 trap 'lg_stop_all; stack_down; rm -rf "$STACK_DIR"' EXIT
 fails=0
@@ -78,7 +77,7 @@ stack_redis_wait_whole 30 && ok "whole: after the restart" || bad "whole: after
 stack_redis_restore 30 && ok "restore: done" || bad "restore"
 expect "restore: the start layout" "$(stack_redis_roles)" "$start_roles"
 expect "restore: redis-0's range back" "$(stack_redis_slots | head -n 1)" "0 5460 redis-0"
-expect "sessions: every client of a and b found after the failover" "$(get "$(lg_in a rc_sessions 0)" 'd["found"]'),$(get "$(lg_in b rc_sessions 0)" 'd["found"]')" "30,30"
+expect "sessions: every client of a and b found after the failover" "$(get "$(lg_in a t2_sessions 0)" 'd["found"]'),$(get "$(lg_in b t2_sessions 0)" 'd["found"]')" "30,30"
 
 # Zone z0 stopped (redis-0, a master, and redis-5, redis-2's replica): no
 # reader hangs on it, redis-3 takes over, and on SIGCONT the zone returns.
diff --git a/scripts/tier2/lib.sh b/scripts/tier2/lib.sh
index 83b7bd2..d15c071 100644
--- a/scripts/tier2/lib.sh
+++ b/scripts/tier2/lib.sh
@@ -657,3 +657,120 @@ t2_require_host_setup() {
   why=$(t2_netdrop_leftover "$rules") || t2_refuse "$why"
   why=$(t2_notrack_covers "${STACK_PORT_BASE:-17000}") || t2_refuse "$why"
 }
+
+# The session check (Plan 6b3c2, per slot since Plan 6c1; moved here with
+# its second user, failover, Plan 6c2b).
+
+# t2_sessions_cmp CLIENTS SESSIONS MARK_MS: the session check's reading
+# (reconnect's review O4). CLIENTS is loadgen's GET /clients; SESSIONS one line per client, in
+# order: "N|VALUE", N the sess hash's fields, VALUE the first ("gw|epoch|
+# conn_id|device|conn_seq|login_ms|tok_iat"). {"clients", "offline": not
+# logged in at loadgen, "found": one session on the client's gateway with
+# login_ms >= MARK_MS, "missing", "extra": more than one, "wrong_gateway",
+# "stale": logged in before the mark, "examples": up to 5 [uid, why]}; null
+# if a file is unreadable or SESSIONS has not exactly one line per client.
+t2_sessions_cmp() {
+  python3 -c 'import json, sys
+c = json.load(open(sys.argv[1]))
+lines = open(sys.argv[2]).read().splitlines()
+mark = int(sys.argv[3])
+gw = c["gw"]
+if len(lines) != len(gw):
+    raise SystemExit(1)
+r = {"clients": len(gw), "offline": 0, "found": 0, "missing": 0, "extra": 0, "wrong_gateway": 0, "stale": 0, "examples": []}
+for i, (g, l) in enumerate(zip(gw, lines)):
+    n, _, v = l.partition("|")
+    why = None
+    if g < 0:
+        why = "offline"
+    elif int(n) == 0:
+        why = "missing"
+    elif int(n) > 1:
+        why = "extra"
+    else:
+        f = v.split("|")
+        why = "wrong_gateway" if f[0] != c["gateways"][g] else "stale" if int(f[5]) < mark else None
+    if why:
+        r[why] += 1
+        if len(r["examples"]) < 5:
+            r["examples"].append(["%s-%d" % (c["uid_prefix"], i), why])
+    else:
+        r["found"] += 1
+if len(sys.argv) > 4:
+    r["ms"] = int(sys.argv[4])
+print(json.dumps(r))' "$@" 2>/dev/null || echo null
+}
+
+# t2_sessions_cmds CLIENTS SLOTS DIR: the session check's commands. Each
+# client's sess key goes to the master that owns its slot (SLOTS:
+# stack_redis_slots's "START END NAME" lines), in one EVAL per slot (HVALS
+# each; ~37 keys at 600k): a Cluster refuses a script whose keys span slots
+# (CROSSSLOT, review P6b3c2I-3). Writes DIR/NAME.cmd and DIR/NAME.idx (the
+# client index of each output line, in order) per master; fails if a slot
+# has no owner.
+t2_sessions_cmds() {
+  python3 -c 'import json, sys
+c = json.load(open(sys.argv[1]))
+ranges = [(int(a), int(b), n) for a, b, n in (l.split() for l in open(sys.argv[2]) if l.strip())]
+out = sys.argv[3]
+def crc16(b):  # CRC16-XMODEM, Redis Cluster key slots
+    r = 0
+    for x in b:
+        r ^= x << 8
+        for _ in range(8):
+            r = ((r << 1) ^ 0x1021 if r & 0x8000 else r << 1) & 0xFFFF
+    return r
+lua = "local r = {} for i, k in ipairs(KEYS) do local v = redis.call(\"HVALS\", k) r[i] = #v .. \"|\" .. (v[1] or \"\") end return r"
+slots = {}
+for i in range(len(c["gw"])):
+    uid = "%s-%d" % (c["uid_prefix"], i)
+    slots.setdefault(crc16(uid.encode()) % 16384, []).append(i)
+files = {}
+for slot in sorted(slots):
+    name = next((n for a, b, n in ranges if a <= slot <= b), None)
+    if name is None:
+        raise SystemExit("slot %d has no owner" % slot)
+    if name not in files:
+        files[name] = (open("%s/%s.cmd" % (out, name), "w"), open("%s/%s.idx" % (out, name), "w"))
+    cmd, idx = files[name]
+    b = slots[slot]
+    cmd.write("EVAL %s %d %s\n" % (json.dumps(lua), len(b), " ".join("sess:{%s-%d}" % (c["uid_prefix"], i) for i in b)))
+    idx.write("".join("%d\n" % i for i in b))' "$@"
+}
+
+# t2_sessions_join CLIENTS DIR: the replies (DIR/NAME.out, one line per key)
+# put back in client order, the SESSIONS lines t2_sessions_cmp reads. Fails
+# if a reply's line count is not its NAME.idx's or a client has no line.
+t2_sessions_join() {
+  python3 -c 'import glob, json, sys
+n = len(json.load(open(sys.argv[1]))["gw"])
+lines = [None] * n
+for f in glob.glob(sys.argv[2] + "/*.idx"):
+    idx = [int(x) for x in open(f).read().split()]
+    out = open(f[:-4] + ".out").read().splitlines()
+    if len(out) != len(idx):
+        raise SystemExit(1)
+    for i, l in zip(idx, out):
+        lines[i] = l
+if None in lines:
+    raise SystemExit(1)
+print("\n".join(lines))' "$@"
+}
+
+# t2_sessions MARK_MS: t2_sessions_cmp over loadgen's /clients and every
+# client's sess key, read per slot on each key's master (t2_sessions_cmds):
+# at 600k ~16k short scripts after the hold, on one Redis or a Cluster alike.
+t2_sessions() {
+  local a=$STACK_DIR/artifacts d=$STACK_DIR/sessions f name t0
+  t0=$(date +%s%3N)
+  curl -sf -m 60 "http://$(lg_control)/clients" >"$a/clients.json" || { echo null; return; }
+  rm -rf "$d" && mkdir -p "$d"
+  stack_redis_slots >"$d/slots.txt"
+  t2_sessions_cmds "$a/clients.json" "$d/slots.txt" "$d" 2>"$a/sessions.err" || { echo null; return; }
+  for f in "$d"/*.cmd; do
+    name=$(basename "$f" .cmd)
+    stack_redis_node_cli "$name" --raw <"$f" >"$d/$name.out" 2>&1
+  done
+  t2_sessions_join "$a/clients.json" "$d" >"$a/sessions.txt" 2>>"$a/sessions.err" || { echo null; return; }
+  t2_sessions_cmp "$a/clients.json" "$a/sessions.txt" "$1" $(($(date +%s%3N) - t0))
+}
diff --git a/scripts/tier2/reconnect.sh b/scripts/tier2/reconnect.sh
index 7b30bcd..1f86a4c 100755
--- a/scripts/tier2/reconnect.sh
+++ b/scripts/tier2/reconnect.sh
@@ -371,120 +371,6 @@ rc_heartbeats_ok() {
     lg_true - 'd["l"] == {"lost": d["p"]} and all(v["closes_heartbeat"] <= '"$allowed"' for g, v in d["g"].items())'
 }
 
-# rc_sessions_cmp CLIENTS SESSIONS MARK_MS: P_sessions' reading (review
-# O4). CLIENTS is loadgen's GET /clients; SESSIONS one line per client, in
-# order: "N|VALUE", N the sess hash's fields, VALUE the first ("gw|epoch|
-# conn_id|device|conn_seq|login_ms|tok_iat"). {"clients", "offline": not
-# logged in at loadgen, "found": one session on the client's gateway with
-# login_ms >= MARK_MS, "missing", "extra": more than one, "wrong_gateway",
-# "stale": logged in before the mark, "examples": up to 5 [uid, why]}; null
-# if a file is unreadable or SESSIONS has not exactly one line per client.
-rc_sessions_cmp() {
-  python3 -c 'import json, sys
-c = json.load(open(sys.argv[1]))
-lines = open(sys.argv[2]).read().splitlines()
-mark = int(sys.argv[3])
-gw = c["gw"]
-if len(lines) != len(gw):
-    raise SystemExit(1)
-r = {"clients": len(gw), "offline": 0, "found": 0, "missing": 0, "extra": 0, "wrong_gateway": 0, "stale": 0, "examples": []}
-for i, (g, l) in enumerate(zip(gw, lines)):
-    n, _, v = l.partition("|")
-    why = None
-    if g < 0:
-        why = "offline"
-    elif int(n) == 0:
-        why = "missing"
-    elif int(n) > 1:
-        why = "extra"
-    else:
-        f = v.split("|")
-        why = "wrong_gateway" if f[0] != c["gateways"][g] else "stale" if int(f[5]) < mark else None
-    if why:
-        r[why] += 1
-        if len(r["examples"]) < 5:
-            r["examples"].append(["%s-%d" % (c["uid_prefix"], i), why])
-    else:
-        r["found"] += 1
-if len(sys.argv) > 4:
-    r["ms"] = int(sys.argv[4])
-print(json.dumps(r))' "$@" 2>/dev/null || echo null
-}
-
-# rc_sessions_cmds CLIENTS SLOTS DIR: the session check's commands. Each
-# client's sess key goes to the master that owns its slot (SLOTS:
-# stack_redis_slots's "START END NAME" lines), in one EVAL per slot (HVALS
-# each; ~37 keys at 600k): a Cluster refuses a script whose keys span slots
-# (CROSSSLOT, review P6b3c2I-3). Writes DIR/NAME.cmd and DIR/NAME.idx (the
-# client index of each output line, in order) per master; fails if a slot
-# has no owner.
-rc_sessions_cmds() {
-  python3 -c 'import json, sys
-c = json.load(open(sys.argv[1]))
-ranges = [(int(a), int(b), n) for a, b, n in (l.split() for l in open(sys.argv[2]) if l.strip())]
-out = sys.argv[3]
-def crc16(b):  # CRC16-XMODEM, Redis Cluster key slots
-    r = 0
-    for x in b:
-        r ^= x << 8
-        for _ in range(8):
-            r = ((r << 1) ^ 0x1021 if r & 0x8000 else r << 1) & 0xFFFF
-    return r
-lua = "local r = {} for i, k in ipairs(KEYS) do local v = redis.call(\"HVALS\", k) r[i] = #v .. \"|\" .. (v[1] or \"\") end return r"
-slots = {}
-for i in range(len(c["gw"])):
-    uid = "%s-%d" % (c["uid_prefix"], i)
-    slots.setdefault(crc16(uid.encode()) % 16384, []).append(i)
-files = {}
-for slot in sorted(slots):
-    name = next((n for a, b, n in ranges if a <= slot <= b), None)
-    if name is None:
-        raise SystemExit("slot %d has no owner" % slot)
-    if name not in files:
-        files[name] = (open("%s/%s.cmd" % (out, name), "w"), open("%s/%s.idx" % (out, name), "w"))
-    cmd, idx = files[name]
-    b = slots[slot]
-    cmd.write("EVAL %s %d %s\n" % (json.dumps(lua), len(b), " ".join("sess:{%s-%d}" % (c["uid_prefix"], i) for i in b)))
-    idx.write("".join("%d\n" % i for i in b))' "$@"
-}
-
-# rc_sessions_join CLIENTS DIR: the replies (DIR/NAME.out, one line per key)
-# put back in client order, the SESSIONS lines rc_sessions_cmp reads. Fails
-# if a reply's line count is not its NAME.idx's or a client has no line.
-rc_sessions_join() {
-  python3 -c 'import glob, json, sys
-n = len(json.load(open(sys.argv[1]))["gw"])
-lines = [None] * n
-for f in glob.glob(sys.argv[2] + "/*.idx"):
-    idx = [int(x) for x in open(f).read().split()]
-    out = open(f[:-4] + ".out").read().splitlines()
-    if len(out) != len(idx):
-        raise SystemExit(1)
-    for i, l in zip(idx, out):
-        lines[i] = l
-if None in lines:
-    raise SystemExit(1)
-print("\n".join(lines))' "$@"
-}
-
-# rc_sessions MARK_MS: rc_sessions_cmp over loadgen's /clients and every
-# client's sess key, read per slot on each key's master (rc_sessions_cmds):
-# at 600k ~16k short scripts after the hold, on one Redis or a Cluster alike.
-rc_sessions() {
-  local a=$STACK_DIR/artifacts d=$STACK_DIR/sessions f name t0
-  t0=$(date +%s%3N)
-  curl -sf -m 60 "http://$(lg_control)/clients" >"$a/clients.json" || { echo null; return; }
-  rm -rf "$d" && mkdir -p "$d"
-  stack_redis_slots >"$d/slots.txt"
-  rc_sessions_cmds "$a/clients.json" "$d/slots.txt" "$d" 2>"$a/sessions.err" || { echo null; return; }
-  for f in "$d"/*.cmd; do
-    name=$(basename "$f" .cmd)
-    stack_redis_node_cli "$name" --raw <"$f" >"$d/$name.out" 2>&1
-  done
-  rc_sessions_join "$a/clients.json" "$d" >"$a/sessions.txt" 2>>"$a/sessions.err" || { echo null; return; }
-  rc_sessions_cmp "$a/clients.json" "$a/sessions.txt" "$1" $(($(date +%s%3N) - t0))
-}
-
 t=$SECONDS
 if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
   echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
@@ -628,7 +514,7 @@ for name in $PHASES; do
   [ "$hb_ok" != true ] && [ "$loadgen_bound" = true ] &&
     t2_not_covered "${name}_heartbeats failed while loadgen was the bound: a late read on loadgen's side can also time a Pong out; read ${name}.series lg_runq_ms_per_s and loadgen_set_busy_pct beside the Pong timeouts (${name}.loadgen.storm)"
   t2_lease "$name" "$d" "$window_s" "$ld"
-  sessions=$(rc_sessions "$mark_ms")
+  sessions=$(t2_sessions "$mark_ms")
   t2_check "${name}_sessions" "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
     "\"sessions\": ${sessions:-null}, \"logic\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("logins_replaced", "logouts_deleted", "logouts_not_owner", "logouts_unknown")}')"
 
diff --git a/scripts/tier2/reconnect_test.sh b/scripts/tier2/reconnect_test.sh
index a88abb5..143003c 100755
--- a/scripts/tier2/reconnect_test.sh
+++ b/scripts/tier2/reconnect_test.sh
@@ -2,9 +2,10 @@
 # Tests for scripts/tier2/reconnect.sh's rc_series over synthetic
 # rc_sampler lines (make test-scripts): the per-gateway rates, a late
 # scrape, loadgen's run-queue wait, and the funnel reading and its bar
-# (#133, review O3, O6); rc_sessions_cmp (review O4); and rc_heartbeats_ok
-# (the final review's I-1: the ghost allowance); rc_sessions_cmds and
-# rc_sessions_join (Plan 6c1: the session check per slot). The scenario
+# (#133, review O3, O6); lib.sh's t2_sessions_cmp (review O4); and
+# rc_heartbeats_ok (the final review's I-1: the ghost allowance); lib.sh's
+# t2_sessions_cmds and t2_sessions_join (Plan 6c1: the session check per
+# slot; in lib.sh since Plan 6c2b). The scenario
 # itself runs on the Tier 2 host.
 set -uo pipefail
 cd "$(git rev-parse --show-toplevel)"
@@ -16,7 +17,7 @@ fails=0
 ok() { echo "ok   $1"; }
 bad() { echo "FAIL $1"; fails=$((fails + 1)); }
 expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
-eval "$(sed -n '/^rc_series() {/,/^}/p; /^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_heartbeats_ok() {/,/^}/p' scripts/tier2/reconnect.sh)"
+eval "$(sed -n '/^rc_series() {/,/^}/p; /^rc_heartbeats_ok() {/,/^}/p' scripts/tier2/reconnect.sh)"
 
 # gw HANDSHAKE ONLINE PENDING_MEM: one gateway's reading (its Kick closes
 # = ONLINE); line MS SKEW GW0 GW1 [RQ] one sample (loadgen 10 online, the
@@ -50,21 +51,21 @@ expect "series: unreadable" "$(rc_series "$tmp/none.jsonl" 0 100 5)" null
   line 3000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 9000000; line 4000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 1000; } >"$tmp/q.jsonl"
 expect "series: run-queue gaps are null" "$(get "$(rc_series "$tmp/q.jsonl" 1000 100 5)" '[r[d["columns"].index("lg_runq_ms_per_s")] for r in d["rows"]]')" "[None, None, None]"
 
-# rc_sessions_cmds: 13 clients u-0..u-12. CLUSTER KEYSLOT sess:{u-12} is
+# t2_sessions_cmds: 13 clients u-0..u-12. CLUSTER KEYSLOT sess:{u-12} is
 # 6674 (Redis 7.4.11), so with 6674 alone on redis-1 its file holds u-12's
 # EVAL only; every client is on exactly one line; a slot with no owner fails.
 mkdir -p "$tmp/sc"
 python3 -c 'import json; print(json.dumps({"uid_prefix": "u", "gw": [0] * 13, "gateways": ["gw-0"]}))' >"$tmp/sc/clients.json"
 printf '%s\n' "0 6673 redis-0" "6674 6674 redis-1" "6675 16383 redis-0" >"$tmp/sc/slots.txt"
-rc_sessions_cmds "$tmp/sc/clients.json" "$tmp/sc/slots.txt" "$tmp/sc" && ok "slots: commands written" || bad "slots: commands"
+t2_sessions_cmds "$tmp/sc/clients.json" "$tmp/sc/slots.txt" "$tmp/sc" && ok "slots: commands written" || bad "slots: commands"
 expect "slots: u-12 alone on its slot's master" "$(cat "$tmp/sc/redis-1.idx" 2>/dev/null)" 12
 expect "slots: one EVAL of one key there" "$(awk '{ print $1, $(NF-1), $NF }' "$tmp/sc/redis-1.cmd" 2>/dev/null)" "EVAL 1 sess:{u-12}"
 expect "slots: each client once" "$(cat "$tmp/sc"/*.idx | sort -n | paste -sd, -)" "$(seq 0 12 | paste -sd, -)"
 expect "slots: each EVAL's key count" "$(awk '{ k = 0; for (i = 1; i <= NF; i++) if ($i ~ /^sess:[{]/) k++; if ($(NF - k) != k) bad++ } END { print bad + 0 }' "$tmp/sc"/*.cmd)" 0
 printf '%s\n' "0 100 redis-0" >"$tmp/sc/short.txt"
-rc_sessions_cmds "$tmp/sc/clients.json" "$tmp/sc/short.txt" "$tmp/sc/none" 2>/dev/null && bad "slots: a slot with no owner must fail" || ok "slots: a slot with no owner fails"
+t2_sessions_cmds "$tmp/sc/clients.json" "$tmp/sc/short.txt" "$tmp/sc/none" 2>/dev/null && bad "slots: a slot with no owner must fail" || ok "slots: a slot with no owner fails"
 
-# rc_sessions_join: each node's reply lines go back to their clients; a
+# t2_sessions_join: each node's reply lines go back to their clients; a
 # reply short of its .idx fails.
 mkdir -p "$tmp/sj"
 cp "$tmp/sc/clients.json" "$tmp/sj/"
@@ -72,26 +73,26 @@ printf '%s\n' 3 0 7 >"$tmp/sj/redis-0.idx"
 printf '%s\n' "1|c3" "1|c0" "1|c7" >"$tmp/sj/redis-0.out"
 for i in 1 2 4 5 6 8 9 10 11 12; do echo "$i"; done >"$tmp/sj/redis-1.idx"
 for i in 1 2 4 5 6 8 9 10 11 12; do echo "0|c$i"; done >"$tmp/sj/redis-1.out"
-expect "join: client order" "$(rc_sessions_join "$tmp/sj/clients.json" "$tmp/sj" | head -n 4 | paste -sd' ' -)" "1|c0 0|c1 0|c2 1|c3"
-expect "join: one line per client" "$(rc_sessions_join "$tmp/sj/clients.json" "$tmp/sj" | wc -l)" 13
+expect "join: client order" "$(t2_sessions_join "$tmp/sj/clients.json" "$tmp/sj" | head -n 4 | paste -sd' ' -)" "1|c0 0|c1 0|c2 1|c3"
+expect "join: one line per client" "$(t2_sessions_join "$tmp/sj/clients.json" "$tmp/sj" | wc -l)" 13
 head -n 2 "$tmp/sj/redis-0.out" >"$tmp/sj/x" && mv "$tmp/sj/x" "$tmp/sj/redis-0.out"
-rc_sessions_join "$tmp/sj/clients.json" "$tmp/sj" >/dev/null 2>&1 && bad "join: a short reply must fail" || ok "join: a short reply fails"
+t2_sessions_join "$tmp/sj/clients.json" "$tmp/sj" >/dev/null 2>&1 && bad "join: a short reply must fail" || ok "join: a short reply fails"
 
-# rc_sessions_cmp: 7 clients on gw-0/gw-1, the mark at 1000: one good on
+# t2_sessions_cmp: 7 clients on gw-0/gw-1, the mark at 1000: one good on
 # each gateway, one missing, one with two sessions, one on the wrong
 # gateway, one logged in before the mark, one offline at loadgen.
 echo '{"uid_prefix": "u", "gateways": ["gw-0", "gw-1"], "gw": [0, 1, 0, 1, 0, 1, -1]}' >"$tmp/clients.json"
 printf '%s\n' "1|gw-0|7|1|dev|1|1500|0" "1|gw-1|7|2|dev|1|1200|0" "0|" "2|gw-0|7|4|dev|1|1300|0" "1|gw-1|7|5|dev|1|1300|0" \
   "1|gw-1|7|6|dev|1|900|0" "1|gw-0|7|7|dev|1|1300|0" >"$tmp/sessions.txt"
-c=$(rc_sessions_cmp "$tmp/clients.json" "$tmp/sessions.txt" 1000)
+c=$(t2_sessions_cmp "$tmp/clients.json" "$tmp/sessions.txt" 1000)
 expect "sessions: counts" "$(get "$c" '[d[k] for k in ("clients", "found", "missing", "extra", "wrong_gateway", "stale", "offline")]')" "[7, 2, 1, 1, 1, 1, 1]"
 expect "sessions: examples" "$(get "$c" 'd["examples"]')" "[['u-2', 'missing'], ['u-3', 'extra'], ['u-4', 'wrong_gateway'], ['u-5', 'stale'], ['u-6', 'offline']]"
 head -n 3 "$tmp/sessions.txt" >"$tmp/short.txt"
-expect "sessions: the check's time" "$(get "$(rc_sessions_cmp "$tmp/clients.json" "$tmp/sessions.txt" 1000 1234)" 'd["ms"]')" 1234
+expect "sessions: the check's time" "$(get "$(t2_sessions_cmp "$tmp/clients.json" "$tmp/sessions.txt" 1000 1234)" 'd["ms"]')" 1234
 expect "sessions: no time given" "$(get "$c" '"ms" in d')" False
-expect "sessions: too few lines" "$(rc_sessions_cmp "$tmp/clients.json" "$tmp/short.txt" 1000)" null
+expect "sessions: too few lines" "$(t2_sessions_cmp "$tmp/clients.json" "$tmp/short.txt" 1000)" null
 { cat "$tmp/sessions.txt"; echo "ERR something"; } >"$tmp/long.txt"
-expect "sessions: too many lines" "$(rc_sessions_cmp "$tmp/clients.json" "$tmp/long.txt" 1000)" null
+expect "sessions: too many lines" "$(t2_sessions_cmp "$tmp/clients.json" "$tmp/long.txt" 1000)" null
 
 # rc_heartbeats_ok: gw-0 kept 5 ghosts and Kicked 3 of them, so 2
 # heartbeat closes are its ghosts. A Pong left unread at the ghost instant
```

- [ ] **Step 2: The tests fail on the old scripts**

Run: `scripts/tier2/reconnect_test.sh 2>&1 | tail -1`
Expected: `reconnect_test: 13 failed` (`t2_sessions_*` not found).

- [ ] **Step 3: The move**

`git apply --exclude='*_test.sh' PATCH`. Then `grep -rn 'rc_sessions' scripts/` prints nothing.

- [ ] **Step 4: The tests pass**

Run: `scripts/tier2/reconnect_test.sh | tail -1; scripts/tier2/lib_test.sh | tail -1; STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_failover_test.sh 2>&1 | tail -1`
Expected: `reconnect_test: all passed`, `lib_test: all passed`, `0 failure(s)`; and `STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_cluster_test.sh 2>&1 | tail -1` → `0 failure(s)`.

- [ ] **Step 5: Commit**

```bash
git add scripts/tier2/lib.sh scripts/tier2/reconnect.sh scripts/tier2/reconnect_test.sh scripts/stack_failover_test.sh scripts/stack_cluster_test.sh
git commit -m "refactor: move the session check into lib.sh"
```

---

### Task 3: Scenario failover

**Files:**
- Create: `scripts/tier2/failover.sh`, `scripts/tier2/failover_test.sh`
- Modify: `Makefile` (`test-scripts` runs `failover_test.sh` after `reconnect_test.sh`)

**Interfaces:**
- Consumes: Task 1's defaults (no flag set by the scenario: Logic's read timeout 1s, node timeout 5s, not full coverage), Task 2's `t2_sessions`; 6c2a's `stack_lease_key`, `stack_redis_owner`, `stack_redis_failover`/`_kill`/`_restart`/`_stop`/`_cont`, `stack_redis_wait_role`, `stack_redis_wait_whole`, `stack_redis_restore`, `stack_redis_slots`, loadgen's `GET /logins`; lib.sh's `t2_lease_floor`, `t2_gws_counters`/`_delta`, `t2_record_start`/`_end`.
- Produces (readers in `failover.sh`, tested by `failover_test.sh`; the test pulls them out with `sed`, so each is a multi-line function ending in a `}` line):
  - `fo_zone LAYOUT MASTER` → "MASTER NODE…" (MASTER's zone, MASTER first); `fo_replica LAYOUT MASTER` → its start replica.
  - `fo_shards LOGINS SLOTS TARGET [T0_MS T1_MS]` → `{"shards": {NAME: STATS}, "others": STATS, "target", "window"}`, STATS `{"clients", "logged_in", "gave_up", "retried", "pending", "p50_ms", "p99_ms", "max_ms"}` (nearest rank; null when none); null if a file is unreadable or a slot has no owner.
  - `fo_other_ok BEFORE THROUGH FLOOR_MS` → true/false (#141.2); `fo_affected SHARDS` → the target's (retried + gave_up) / clients, null when none.
  - `fo_wait_at KEY OWNER AT_S` → ms since the key's last renewal when its PTTL first lies in (30000 − AT_S·1000 − 500, 30000 − AT_S·1000], polled every 20ms for up to 25s; null after.
  - `fo_flow NAME CLIENTS`, `fo_flow_done SECONDS FILE`, `fo_wait_ok SECONDS` (the scenario's own steps; not unit-tested).
- Produces (result): 30 checks (`stack`, `online`, `pinning`; per phase `P_restore`, `P_failover`, `P_other_shards`, `P_logins`, `P_whole`, `P_lease`, `P_sessions`, `no_swap_in_P`; `idle`, `processes`, `artifacts`), the fields `manual`, `kill`, `zone` (per phase: target, replica, zone, `gwepochs_on`, the times, the four `fo_shards` readings, the counters, `redis_latency`, the sessions) and `run`, and 4 `not_covered` lines (Revoke at zone loss #141.3; the repair of lost writes; 6 masters; a failover under a storm). The script's header lists every setting and check.

- [ ] **Step 1: The test**

Save the patch below and apply only the test: `git apply --include='scripts/tier2/failover_test.sh' PATCH`; `chmod +x scripts/tier2/failover_test.sh`.

```diff
diff --git a/Makefile b/Makefile
index 2876157..6a089ca 100644
--- a/Makefile
+++ b/Makefile
@@ -28,6 +28,7 @@ test-scripts:
 	scripts/tier2/lib_test.sh
 	scripts/tier2/floods_test.sh
 	scripts/tier2/reconnect_test.sh
+	scripts/tier2/failover_test.sh
 	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_cluster_test.sh
diff --git a/scripts/tier2/failover.sh b/scripts/tier2/failover.sh
new file mode 100755
index 0000000..5e9422b
--- /dev/null
+++ b/scripts/tier2/failover.sh
@@ -0,0 +1,436 @@
+#!/usr/bin/env bash
+# Tier 2: failover (Plan 6c2b), the spec's Redis failover test (R2-O5,
+# R5S-O1, R5S-O9): fail over the shard that holds a lease key and measure
+# the time; the lease must live through it at the configured TTL; logins
+# for uids on the other shards keep their normal p99; then a whole zone at
+# once (with 3 masters: one master and the replica in its zone), with the
+# failover time and the share of logins affected recorded. On a Redis
+# Cluster (3 masters, 3 replicas, zones z0-z2 as labels, stack.sh
+# STACK_REDIS=cluster, cluster-node-timeout 5s: #140). One stack for every
+# phase (#141.4): one start, one ramp of 600k light clients (run 33's
+# scale, #141.1), online through every phase; each phase starts from the
+# start layout (stack_redis_restore, measured). The phases:
+#   manual: CLUSTER FAILOVER on the replica of gw-0's lease key's master
+#       (no write is lost; the replica waits for its master's offset).
+#   kill: SIGKILL of that master (#139.3: ~9s after gw-0's last renewal,
+#       the worst time: the key then has the least TTL left), then the
+#       node starts again with its nodes.conf and rejoins as a replica.
+#   zone: SIGSTOP of that master's zone, the master and the other master's
+#       replica in it (#139.2: a zone cut off by the network, at the same
+#       time), then SIGCONT: the old master rejoins as a replica by itself.
+# Each phase has three login windows (#141.1), each a new light loadgen
+# instance (P6c2aI-5) of FAILOVER_FLOW_RATE logins/s: before (from the
+# start layout), through (the failover lands in it) and after (the Cluster
+# whole again). GET /logins gives each client's sequence (first attempt's
+# start, its login, its attempts); each uid's shard is its sess key's slot
+# owner before the failover (CRC16 of the uid, the hash tag).
+#
+# Usage (on the Tier 2 host, from the repo root), after the one-time
+#   sudo scripts/tier2/host-setup.sh, with the desktop apps closed:
+#   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=failover
+#   Results: tier2-results/failover-<timestamp>/ (result.json, the stack's
+#   logs and artifacts, one directory of login logs per phase), plus ONE
+#   file to send back: tier2-results/failover-<timestamp>.report.txt
+#   Exit code: 0 if every check passes, 1 if any check fails, 2 if the host is refused.
+#   About 20 minutes (the ramp 120s; each phase ~4 min: the restore, three
+#   windows, the rejoin with a full resync of the returning node).
+#
+# Settings (every number whole and positive, refused otherwise before
+# anything starts):
+#   FAILOVER_PHASES       phases to run, in order (default "manual kill zone")
+#   FAILOVER_CLIENTS      light clients online over the 3 gateways (default 600000)
+#   FAILOVER_RATE         their ramp, clients per second (default 5000)
+#   FAILOVER_FLOW_RATE    each window's logins per second (default 200)
+#   FAILOVER_WINDOW_S     the before and after windows' length (default 30)
+#   FAILOVER_THROUGH_S    the through window's length (default 40: the
+#                         wait for the worst time, up to 10s, the failover
+#                         and the logins after it)
+#   FAILOVER_AT_S         seconds after gw-0's last renewal for kill and zone
+#                         (default 9, #139.3; manual too, harmless)
+#   FAILOVER_MAX_S        the failover's bound (default 20: lease TTL 30s minus
+#                         the 10s renewal interval, the spec's lease rule)
+#   FAILOVER_WHOLE_WAIT   seconds for the restore and the rejoin (default 600:
+#                         a returning node's full resync of ~200k sessions)
+#   FAILOVER_P99_FLOOR_MS the other shards' bound's floor over the before
+#                         p99 (default 50, #141.2)
+#   FAILOVER_LG_MEMLIMIT  loadgen's GOMEMLIMIT (default 8GiB)
+#   FAILOVER_CPUS_LOADGEN, FAILOVER_CPUS_GATEWAY, FAILOVER_CPUS_LOGIC
+#                         the CPU sets (default reconnect's, run 33's:
+#                         0-5,10,12-17,22; 6-9,18-21; 11,23: Logic and the
+#                         6 Redis nodes share 11,23, #138)
+#   FAILOVER_GATEWAY_PROCS  each gateway's GOMAXPROCS (default 4)
+#
+# Checks (once):
+#   stack          redis (6 nodes), devtoken, Logic and the 3 gateways Ready
+#   online         every main client logs in, each gateway holds >= 30% of them
+#   pinning        the gateways, Logic + Redis and loadgen on their CPU sets;
+#                  a failure ends the run
+# Checks (per phase P):
+#   P_restore      the start layout back within FAILOVER_WHOLE_WAIT (the
+#                  time recorded; the first phase's is a no-op)
+#   P_failover     the target master's replica is master within
+#                  FAILOVER_MAX_S of the trigger, and a live node then says
+#                  cluster_state:ok; both times, the trigger's time since
+#                  gw-0's last renewal, the zone's nodes, gwepochs' master
+#                  (P6c2aI-2) and each gateway's lease key's master recorded
+#   P_other_shards the through window's logins on the other shards, those
+#                  started from the trigger to the replica's master + 1s: none
+#                  gave up, and their p99 is at most max(2 x, + FLOOR ms)
+#                  the before window's other-shard p99 (#141.2); the
+#                  target shard's affected share (retried or gave up) and
+#                  every window's per-shard numbers recorded
+#   P_logins       every flow client of the three windows logged in in
+#                  the end (light: up to 10 attempts; the failing shard's
+#                  retries are recorded in P_other_shards)
+#   P_whole        the Cluster whole again within FAILOVER_WHOLE_WAIT after
+#                  the node returns (manual: at once); the time recorded
+#   P_lease        from the through window's start to the end of the
+#                  rejoin, no gateway got a missing lease reply and Logic
+#                  deleted nothing in a sweep (the lease lived, R2-O5);
+#                  each gateway renewed at least every interval (live
+#                  floor); the error replies (a retry every 1s while Redis
+#                  refuses, P6c2aI-3) and Logic's renewal errors recorded,
+#                  not gated
+#   P_sessions     after the rejoin, every main client's session is in
+#                  the store, on its gateway (written minutes earlier: no
+#                  failover may lose it); the through window's lost writes
+#                  (its sessions missing then: ~1s of writes may be lost
+#                  in kill and zone, R2-O4, repaired by Reconcile in a
+#                  10-minute cycle) recorded
+#   no_swap_in_P   no page swapped in from the through window's start to
+#                  the end of P_sessions
+#                  (P_* per phase: 8 checks each)
+# Checks (once, at the end):
+#   idle           after loadgen stops, every gateway reaches 0 connections
+#   processes      nothing died, panicked or stopped uncleanly (the killed
+#                  Redis node is expected)
+#   artifacts      the metrics before and after, each window's login log
+set -uo pipefail
+
+. "$(dirname "$0")/lib.sh"
+. "$(dirname "$0")/../stack.sh"
+. "$(dirname "$0")/../loadgen.sh"
+t2_init failover
+t2_need curl taskset sudo python3
+t2_require_host_setup
+
+PHASES=${FAILOVER_PHASES:-manual kill zone}
+named=" "
+for p in $PHASES; do
+  case $p in manual | kill | zone) ;; *) echo "failover: unknown phase '$p' (manual, kill, zone)" >&2; exit 1 ;; esac
+  case $named in *" $p "*) echo "failover: phase '$p' named twice in FAILOVER_PHASES" >&2; exit 1 ;; esac
+  named+="$p "
+done
+for v in FAILOVER_CLIENTS FAILOVER_RATE FAILOVER_FLOW_RATE FAILOVER_WINDOW_S FAILOVER_THROUGH_S FAILOVER_AT_S \
+  FAILOVER_MAX_S FAILOVER_WHOLE_WAIT FAILOVER_P99_FLOOR_MS FAILOVER_GATEWAY_PROCS; do
+  case ${!v-1} in ''|*[!0-9]*|0*) echo "failover: $v must be a positive whole number" >&2; exit 1 ;; esac
+done
+N=${FAILOVER_CLIENTS:-600000} RATE=${FAILOVER_RATE:-5000} FLOW=${FAILOVER_FLOW_RATE:-200}
+WIN=${FAILOVER_WINDOW_S:-30} THROUGH=${FAILOVER_THROUGH_S:-40} AT=${FAILOVER_AT_S:-9} MAX=${FAILOVER_MAX_S:-20}
+WHOLE=${FAILOVER_WHOLE_WAIT:-600} FLOOR=${FAILOVER_P99_FLOOR_MS:-50}
+[ "$AT" -lt 10 ] || { echo "failover: FAILOVER_AT_S must be below the 10s renewal interval" >&2; exit 1; }
+# 600k light clients as capacity's (~16GB with the gateways and Redis).
+t2_mem_budget 16
+t2_nofile 1048576
+
+export STACK_PIN=1 STACK_GATEWAYS=3 STACK_REDIS=cluster
+export T2_CPUS_GATEWAY=${FAILOVER_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${FAILOVER_CPUS_LOADGEN:-0-5,10,12-17,22}
+export T2_CPUS_LOGIC=${FAILOVER_CPUS_LOGIC:-11,23}
+export STACK_GATEWAY_PROCS=${FAILOVER_GATEWAY_PROCS:-4}
+LG_ENV=(GOMEMLIMIT="${FAILOVER_LG_MEMLIMIT:-8GiB}")
+STACK_PORT_BASE=${STACK_PORT_BASE:-17000}
+STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server
+t2_on_exit 'stack_down'
+t2_on_exit 'lg_stop_all' # newest first: loadgen stops before the stack
+
+GW_COUNTERS=(
+  lease_live='goim_gateway_lease_calls_total:result="live"'
+  lease_missing='goim_gateway_lease_calls_total:result="missing"'
+  lease_error='goim_gateway_lease_calls_total:result="error"'
+  logins_ok='goim_gateway_logins_total:result="ok"'
+  logins_busy='goim_gateway_logins_total:result="busy"'
+  logins_expired='goim_gateway_logins_total:result="expired"'
+)
+LOGIC_COUNTERS=(
+  lease_error='goim_logic_lease_renewals_total:result="error"'
+  lease_missing='goim_logic_lease_renewals_total:result="missing"'
+  lease_index_failures=goim_logic_lease_index_failures_total
+  sweep_deleted=goim_logic_sweep_deleted_total
+  logins_ok='goim_logic_logins_total:result="ok"'
+)
+
+t2_report_extra() {
+  local f
+  for f in "$T2_OUT"/stack/logs/*.log; do
+    [ -f "$f" ] || continue
+    t2_section "${f#"$T2_OUT"/} (last 20 lines)"
+    tail -n 20 "$f"
+  done
+}
+
+# fo_zone LAYOUT MASTER: the nodes in MASTER's zone (redis/layout's "NAME
+# PORT ROLE ZONE MASTER" lines), MASTER first, space-separated.
+fo_zone() {
+  awk -v m="$2" '$1 == m { z = $4 } { n[NR] = $1; zz[NR] = $4 } END { printf "%s", m; for (i = 1; i <= NR; i++) if (zz[i] == z && n[i] != m) printf " %s", n[i]; print "" }' "$1"
+}
+
+# fo_replica LAYOUT MASTER: MASTER's replica in the start layout.
+fo_replica() {
+  awk -v m="$2" '$3 == "replica" && $5 == m { print $1 }' "$1"
+}
+
+# fo_shards LOGINS SLOTS TARGET [T0_MS T1_MS]: a window's login log (GET
+# /logins) per shard, each uid's shard its sess key's slot owner in SLOTS
+# (stack_redis_slots's "START END NAME" lines): {"shards": {NAME: STATS},
+# "others": STATS over every shard but TARGET, "target": TARGET, "window"}.
+# With T0_MS and T1_MS only the sequences whose first attempt started in
+# [T0_MS, T1_MS] count. STATS: {"clients", "logged_in", "gave_up",
+# "retried" (logged in after more than one attempt), "pending" (no
+# sequence ended; never in a window), "p50_ms", "p99_ms", "max_ms" (the
+# logged-in sequences' first start to login, nearest rank; null with
+# none)}. null if a file is unreadable or a slot has no owner.
+fo_shards() {
+  python3 -c 'import json, math, sys
+d = json.load(open(sys.argv[1]))
+ranges = [(int(a), int(b), n) for a, b, n in (l.split() for l in open(sys.argv[2]) if l.strip())]
+target = sys.argv[3]
+win = (int(sys.argv[4]), int(sys.argv[5])) if len(sys.argv) > 5 else None
+def crc16(b):  # CRC16-XMODEM, Redis Cluster key slots
+    r = 0
+    for x in b:
+        r ^= x << 8
+        for _ in range(8):
+            r = ((r << 1) ^ 0x1021 if r & 0x8000 else r << 1) & 0xFFFF
+    return r
+def owner(slot):
+    n = next((n for a, b, n in ranges if a <= slot <= b), None)
+    if n is None:
+        raise SystemExit(1)
+    return n
+def new():
+    return {"clients": 0, "logged_in": 0, "gave_up": 0, "retried": 0, "pending": 0, "ms": []}
+groups = {}
+for i, (f, e, a) in enumerate(zip(d["first_ms"], d["end_ms"], d["attempts"])):
+    if win and not (a > 0 and win[0] <= f <= win[1]):
+        continue
+    name = owner(crc16(("%s-%d" % (d["uid_prefix"], i)).encode()) % 16384)
+    for g in (groups.setdefault(name, new()), groups.setdefault("\0others", new()) if name != target else None):
+        if g is None:
+            continue
+        g["clients"] += 1
+        if a == 0:
+            g["pending"] += 1
+        elif e == 0:
+            g["gave_up"] += 1
+        else:
+            g["logged_in"] += 1
+            g["retried"] += a > 1
+            g["ms"].append(e - f)
+def stats(g):
+    ms = sorted(g.pop("ms"))
+    q = lambda p: ms[max(0, math.ceil(p * len(ms)) - 1)] if ms else None
+    g.update(p50_ms=q(0.5), p99_ms=q(0.99), max_ms=ms[-1] if ms else None)
+    return g
+others = stats(groups.pop("\0others", new()))
+print(json.dumps({"shards": {n: stats(g) for n, g in sorted(groups.items())}, "others": others, "target": target,
+                  "window": list(win) if win else None}))' "$@" 2>/dev/null || echo null
+}
+
+# fo_other_ok BEFORE THROUGH FLOOR_MS: P_other_shards' rule over two
+# fo_shards readings: the through window's other shards have logins, none
+# gave up or is pending, and their p99 is at most max(2 x BEFORE's, BEFORE's
+# + FLOOR_MS) (#141.2). Prints true or false.
+fo_other_ok() {
+  echo "{\"b\": ${1:-null}, \"t\": ${2:-null}}" | lg_true - 'd["b"]["others"]["p99_ms"] is not None and d["t"]["others"]["clients"] > 0 and d["t"]["others"]["gave_up"] == d["t"]["others"]["pending"] == 0 and d["t"]["others"]["p99_ms"] <= max(2 * d["b"]["others"]["p99_ms"], d["b"]["others"]["p99_ms"] + '"$3"')'
+}
+
+# fo_affected SHARDS: the target shard's share of a window's sequences
+# that retried or gave up (null with none).
+fo_affected() {
+  echo "${1:-null}" | lg_py - '(lambda g: round((g["retried"] + g["gave_up"]) / g["clients"], 4) if g and g["clients"] else None)(d["shards"].get(d["target"]))'
+}
+
+# fo_flow NAME CLIENTS: a window's light instance (flow, its own uid
+# prefix and source net) started; FLOW logins per second.
+fo_flow() {
+  lg_in flow lg_start "$1" -mode light -clients "$2" -rate "$FLOW" -uid-prefix "$1" -source-net 1
+}
+
+# fo_flow_done SECONDS FILE: waits until every flow client's sequence
+# ended (logged in or gave up), then writes GET /logins to FILE. Fails
+# after SECONDS (FILE then has the log as it is).
+fo_flow_done() {
+  local end=$((SECONDS + $1)) rc=1
+  while [ $SECONDS -lt "$end" ]; do
+    [ "$(lg_in flow lg_get /logins 'all(a > 0 for a in d["attempts"])')" = true ] && { rc=0; break; }
+    sleep 1
+  done
+  curl -sf -m 30 "http://$(lg_in flow lg_control)/logins" >"$2"
+  return $rc
+}
+
+# fo_wait_at KEY OWNER AT_S: waits (up to 25s) until KEY's PTTL on OWNER
+# says AT_S seconds passed since its last renewal (PTTL in (TTL - AT_S -
+# 0.5s, TTL - AT_S], TTL 30s), polling every 20ms; prints the ms since the
+# renewal at the return (null after the 25s).
+fo_wait_at() {
+  local hi=$((30000 - $3 * 1000)) end=$((SECONDS + 25)) p
+  while [ $SECONDS -lt $end ]; do
+    p=$(STACK_REDIS_TIMEOUT=1 stack_redis_node_cli "$2" pttl "$1" 2>/dev/null)
+    case $p in '' | *[!0-9]*) ;; *) [ "$p" -le "$hi" ] && [ "$p" -gt $((hi - 500)) ] && { echo $((30000 - p)); return 0; } ;; esac
+    sleep 0.02
+  done
+  echo null
+}
+
+# fo_wait_ok SECONDS: until a live node says cluster_state:ok; fails after SECONDS.
+fo_wait_ok() {
+  local end=$((SECONDS + $1))
+  until STACK_REDIS_TIMEOUT=1 stack_redis_cli cluster info 2>/dev/null | grep -q '^cluster_state:ok'; do
+    [ $SECONDS -lt "$end" ] || return 1
+    sleep 0.05
+  done
+}
+
+t=$SECONDS
+if ! make redis-server >"$T2_OUT/redis-build.log" 2>&1; then
+  echo "make redis-server failed, see $T2_OUT/redis-build.log" >&2
+  exit 1
+fi
+STACK_DIR=$T2_OUT stack_build >"$T2_OUT/build.log" 2>&1 || { echo "build failed, see $T2_OUT/build.log" >&2; exit 1; }
+t2_duration setup $((SECONDS - t))
+missing=""
+
+# One stack (#141.4): its checks carry no phase name.
+t0=$SECONDS
+export STACK_DIR=$T2_OUT/stack
+mkdir -p "$STACK_DIR"
+ln -sfn ../bin "$STACK_DIR/bin"
+busy=$(stack_busy_ports)
+[ -z "$busy" ] || t2_refuse "$(printf 'refused: the stack ports are in use (a leftover stack?):\n%s\nstop whatever holds them first' "$busy")"
+stack_up 2>"$STACK_DIR/stack-up.err"
+rc=$?
+t2_check stack "$(t2_bool [ $rc -eq 0 ])" "\"gateways\": $STACK_GATEWAYS, \"redis\": \"cluster\", \"error\": $(t2_json_str "$(cat "$STACK_DIR/stack-up.err")")"
+[ $rc -eq 0 ] || { stack_down; t2_duration stack $((SECONDS - t0)); t2_finish; }
+lg_start main -mode light -clients "$N" -rate "$RATE" -uid-prefix main
+t=$SECONDS
+online=false
+lg_wait_online "$N" $((N / RATE * 3 + 60)) &&
+  online=$(lg_get /stats 'd["online"] == '"$N"' and min(d["online_by_gateway"].values()) >= '"$N"' * 0.3')
+[ "$online" = true ] || online=false
+ramp_s=$((SECONDS - t))
+t2_check online "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
+wrong=""
+for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
+for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+have=$(taskset -pc "$LG_PID" 2>/dev/null)
+[ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
+t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
+[ -z "$wrong" ] && [ "$online" = true ] || { lg_stop_all; stack_down; t2_duration stack $((SECONDS - t0)); t2_finish; }
+stack_snapshot before
+t2_duration ramp $((SECONDS - t0))
+layout=$STACK_DIR/redis/layout
+
+for name in $PHASES; do
+  t0=$SECONDS
+  d=$STACK_DIR/$name
+  mkdir -p "$d"
+  # The start layout back (the first phase: already in it).
+  t=$SECONDS
+  restore=$(t2_bool stack_redis_restore "$WHOLE" 2>"$d/restore.err")
+  restore_s=$((SECONDS - t))
+  t2_check "${name}_restore" "$restore" "\"restore_s\": $restore_s, \"roles\": $(stack_redis_roles), \"error\": $(t2_json_str "$(cat "$d/restore.err")")"
+  key=$(stack_lease_key gw-0) target=$(stack_redis_owner "$key")
+  replica=$(fo_replica "$layout" "$target") zone=$(fo_zone "$layout" "$target") epochs=$(stack_redis_owner gwepochs)
+  leases=""
+  for g in gw-0 gw-1 gw-2; do leases+="${leases:+, }\"$g\": \"$(stack_redis_owner "$(stack_lease_key "$g")")\""; done
+  stack_redis_slots >"$d/slots.txt"
+
+  # Before: the normal p99.
+  fo_flow "$name-before" $((FLOW * WIN))
+  before_done=$(t2_bool fo_flow_done $((WIN + 120)) "$d/before.json")
+  lg_in flow lg_stop
+  before=$(fo_shards "$d/before.json" "$d/slots.txt" "$target")
+
+  # Through: the failover lands in it, at the worst time after gw-0's
+  # last renewal (#139.3).
+  t2_record_start "$name"
+  g0=$(t2_gws_counters "${GW_COUNTERS[@]}") l0=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+  redis_reset=true; stack_redis_resetstat || redis_reset=false
+  lease_t=$SECONDS
+  fo_flow "$name-through" $((FLOW * THROUGH))
+  at_ms=$(fo_wait_at "$key" "$target" "$AT")
+  trig_ms=$(date +%s%3N)
+  case $name in
+    manual) stack_redis_failover "$replica" ;;
+    kill) stack_redis_kill "$target" ;;
+    zone) for n in $zone; do stack_redis_stop "$n"; done ;;
+  esac
+  master=false master_ms=null ok=false ok_ms=null
+  stack_redis_wait_role "$replica" master "$MAX" && { master=true; master_ms=$(($(date +%s%3N) - trig_ms)); }
+  fo_wait_ok 60 && { ok=true; ok_ms=$(($(date +%s%3N) - trig_ms)); }
+  t2_check "${name}_failover" "$(t2_bool [ "$master" = true ] && [ "$ok" = true ])" \
+    "\"target\": \"$target\", \"replica\": \"$replica\", \"lease_key\": \"$key\", \"at_ms_since_renewal\": $at_ms, \"master_ms\": $master_ms, \"cluster_ok_ms\": $ok_ms, \"max_s\": $MAX, \"stopped\": $(t2_json_str "$([ "$name" = zone ] && echo "$zone")"), \"gwepochs_on\": \"$epochs\", \"lease_keys\": {$leases}, \"roles\": $(stack_redis_roles)"
+  through_done=$(t2_bool fo_flow_done $((THROUGH + 120)) "$d/through.json")
+  t1=$((trig_ms + ${master_ms/null/$((MAX * 1000))} + 1000))
+  through=$(fo_shards "$d/through.json" "$d/slots.txt" "$target")
+  failwin=$(fo_shards "$d/through.json" "$d/slots.txt" "$target" "$trig_ms" "$t1")
+
+  # The node back; the Cluster whole again (a full resync).
+  case $name in
+    kill) stack_redis_restart "$target" ;;
+    zone) for n in $zone; do stack_redis_cont "$n"; done ;;
+  esac
+  t=$SECONDS
+  whole=$(t2_bool stack_redis_wait_whole "$WHOLE" 2>"$d/whole.err")
+  whole_s=$((SECONDS - t))
+  t2_check "${name}_whole" "$whole" "\"whole_s\": $whole_s, \"roles\": $(stack_redis_roles), \"error\": $(t2_json_str "$(cat "$d/whole.err")")"
+  redis=$(stack_redis_latency)
+  g1=$(t2_gws_counters "${GW_COUNTERS[@]}") l1=$(stack_counters logic "${LOGIC_COUNTERS[@]}")
+  gd=$(t2_gws_delta "$g0" "$g1") ld=$(stack_counters_delta "$l0" "$l1")
+  window_s=$((SECONDS - lease_t))
+  floor=$(t2_lease_floor "$window_s")
+  t2_check "${name}_lease" "$(echo "{\"g\": $gd, \"l\": $ld}" | lg_true - 'all(v["lease_missing"] == 0 and v["lease_live"] >= '"$floor"' for v in d["g"].values()) and d["l"]["lease_missing"] == 0 and d["l"]["sweep_deleted"] == 0')" \
+    "\"window_s\": $window_s, \"live_floor\": $floor, \"gateways\": $(echo "$gd" | lg_py - '{g: {"live": v["lease_live"], "missing": v["lease_missing"], "error": v["lease_error"]} for g, v in d.items()}'), \"logic\": $(echo "$ld" | lg_py - '{k: d[k] for k in ("lease_error", "lease_missing", "lease_index_failures", "sweep_deleted")}')"
+  sessions=$(t2_sessions 0)
+  through_sessions=$(lg_in flow t2_sessions 0)
+  t2_check "${name}_sessions" "$(echo "${sessions:-null}" | lg_true - 'd["found"] == d["clients"] == '"$N")" \
+    "\"main\": ${sessions:-null}, \"through_lost\": $(echo "${through_sessions:-null}" | lg_py - '{k: d[k] for k in ("clients", "offline", "found", "missing", "wrong_gateway", "examples")}')"
+  lg_in flow lg_stop
+  t2_record_end "$name"
+
+  # After: the Cluster whole again.
+  fo_flow "$name-after" $((FLOW * WIN))
+  after_done=$(t2_bool fo_flow_done $((WIN + 120)) "$d/after.json")
+  lg_in flow lg_stop
+  after=$(fo_shards "$d/after.json" "$d/slots.txt" "$target")
+
+  t2_check "${name}_other_shards" "$(fo_other_ok "$before" "$failwin" "$FLOOR")" \
+    "\"before\": $(echo "${before:-null}" | lg_py - 'd["others"]'), \"failover_window\": ${failwin:-null}, \"floor_ms\": $FLOOR, \"target_affected_share\": $(fo_affected "$failwin")"
+  t2_check "${name}_logins" "$(echo "{\"b\": ${before:-null}, \"t\": ${through:-null}, \"a\": ${after:-null}}" | lg_true - 'all(sum(g["logged_in"] for g in d[k]["shards"].values()) == '"$FLOW"' * n for k, n in (("b", '"$WIN"'), ("t", '"$THROUGH"'), ("a", '"$WIN"')))')" \
+    "\"ended\": {\"before\": $before_done, \"through\": $through_done, \"after\": $after_done}, \"gave_up\": $(echo "{\"b\": ${before:-null}, \"t\": ${through:-null}, \"a\": ${after:-null}}" | lg_py - '{k: {n: g["gave_up"] for n, g in v["shards"].items()} for k, v in d.items()}')"
+  t2_field "$name" "{\"target\": \"$target\", \"replica\": \"$replica\", \"zone\": $(t2_json_str "$zone"), \"gwepochs_on\": \"$epochs\", \"restore_s\": $restore_s, \"at_ms_since_renewal\": $at_ms, \"master_ms\": $master_ms, \"cluster_ok_ms\": $ok_ms, \"whole_s\": $whole_s, \"before\": ${before:-null}, \"through\": ${through:-null}, \"failover_window\": ${failwin:-null}, \"after\": ${after:-null}, \"gateways\": $gd, \"logic\": $ld, \"redis_latency\": ${redis:-null}, \"redis_reset\": $redis_reset, \"sessions\": ${sessions:-null}, \"through_sessions\": ${through_sessions:-null}}"
+  t2_duration "$name" $((SECONDS - t0))
+  for f in before through after; do [ "$(lg_true "$d/$f.json" '"attempts" in d')" = true ] || missing+=" $name/$f.json"; done
+done
+
+stack_snapshot after
+lg_stop_all
+t=$SECONDS
+idle=$(t2_bool stack_wait_idle 900 dropped-ok 2>"$STACK_DIR/idle.err")
+t2_check idle "$idle" "\"stop_s\": $((SECONDS - t)), \"error\": $(t2_json_str "$(cat "$STACK_DIR/idle.err")")"
+stack_down
+problems=$(stack_health)
+t2_check processes "$(t2_bool [ -z "$problems" ])" "\"problems\": $(t2_json_str "$problems")"
+a=$STACK_DIR/artifacts
+for f in before/logic after/logic before/gw-0 after/gw-0; do
+  grep -q '^# TYPE' "$a/$f.metrics" 2>/dev/null || missing+=" $f.metrics"
+done
+t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "${missing# }")"
+t2_field run "{\"clients\": $N, \"rate\": $RATE, \"flow_rate\": $FLOW, \"window_s\": $WIN, \"through_s\": $THROUGH, \"at_s\": $AT, \"ramp_s\": $ramp_s, \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"node_timeout_ms\": ${REDIS_CLUSTER_NODE_TIMEOUT_MS:-5000}}"
+
+t2_not_covered "Revoke at zone loss (#141.3): no Tier 2 tool calls Revoke; the store tests cover a WAIT the replica does not acknowledge (TestRevokeFailsWhenReplicaDoesNotAck)" \
+  "the repair of lost writes: Reconcile's 10-minute cycle is not waited for; P_sessions records the through window's lost sessions, and the race suite and store tests cover rules 3 and 4" \
+  "a zone with two masters (6 masters): Tier 2 runs 3 masters (#137); Tier 3 (Plan 6d)" \
+  "a failover under a login storm: the flow is FAILOVER_FLOW_RATE logins/s beside 600k idle clients (#141.1)"
+t2_finish
diff --git a/scripts/tier2/failover_test.sh b/scripts/tier2/failover_test.sh
new file mode 100755
index 0000000..c9433f5
--- /dev/null
+++ b/scripts/tier2/failover_test.sh
@@ -0,0 +1,68 @@
+#!/usr/bin/env bash
+# Tests for scripts/tier2/failover.sh's readers (make test-scripts): the
+# zone and the replica from redis/layout, the login log per shard
+# (fo_shards: the slot owner by CRC16, a window, nearest-rank p99), the
+# other shards' rule (fo_other_ok, #141.2), the affected share and the wait
+# for the worst time after a renewal (fo_wait_at, #139.3). The scenario
+# itself runs on the Tier 2 host.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+. scripts/tier2/lib.sh
+. scripts/loadgen.sh
+tmp=$(mktemp -d)
+trap 'rm -rf "$tmp"' EXIT
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+eval "$(sed -n '/^fo_zone() {/,/^}/p; /^fo_replica() {/,/^}/p; /^fo_shards() {/,/^}/p; /^fo_other_ok() {/,/^}/p; /^fo_affected() {/,/^}/p; /^fo_wait_at() {/,/^}/p' scripts/tier2/failover.sh)"
+
+# The start layout (scripts/redis-cluster.sh): each master's replica in
+# the next zone.
+printf '%s\n' "redis-0 17020 master z0 -" "redis-1 17021 master z1 -" "redis-2 17022 master z2 -" \
+  "redis-3 17023 replica z1 redis-0" "redis-4 17024 replica z2 redis-1" "redis-5 17025 replica z0 redis-2" >"$tmp/layout"
+expect "zone: redis-0's zone, the master first" "$(fo_zone "$tmp/layout" redis-0)" "redis-0 redis-5"
+expect "zone: redis-1's zone" "$(fo_zone "$tmp/layout" redis-1)" "redis-1 redis-3"
+expect "replica: redis-0's" "$(fo_replica "$tmp/layout" redis-0)" redis-3
+expect "replica: redis-2's" "$(fo_replica "$tmp/layout" redis-2)" redis-5
+
+# 8 clients p-0..p-7; their sess slots (CRC16 of the uid): 7905, 3776,
+# 16035, 11906, 7781, 3652, 15911, 11782, so on redis-1, 0, 2, 2, 1, 0, 2, 2.
+# p-5 gave up, p-6 has no sequence yet; p-1 and p-7 retried.
+printf '%s\n' "0 5460 redis-0" "5461 10922 redis-1" "10923 16383 redis-2" >"$tmp/slots"
+echo '{"uid_prefix": "p", "first_ms": [1000, 1000, 2000, 2000, 5000, 2500, 0, 2900], "end_ms": [1100, 3000, 2050, 2300, 5040, 0, 0, 2960], "attempts": [1, 3, 1, 1, 1, 10, 0, 2]}' >"$tmp/logins.json"
+s=$(fo_shards "$tmp/logins.json" "$tmp/slots" redis-0)
+expect "shards: the target" "$(get "$s" 'd["target"], d["window"]')" "('redis-0', None)"
+expect "shards: redis-0" "$(get "$s" 'd["shards"]["redis-0"]')" "{'clients': 2, 'logged_in': 1, 'gave_up': 1, 'retried': 1, 'pending': 0, 'p50_ms': 2000, 'p99_ms': 2000, 'max_ms': 2000}"
+expect "shards: redis-1, nearest rank" "$(get "$s" '[d["shards"]["redis-1"][k] for k in ("clients", "retried", "p50_ms", "p99_ms")]')" "[2, 0, 40, 100]"
+expect "shards: redis-2, one pending" "$(get "$s" '[d["shards"]["redis-2"][k] for k in ("clients", "logged_in", "pending", "p99_ms")]')" "[4, 3, 1, 300]"
+expect "shards: others, every shard but the target" "$(get "$s" 'd["others"]')" "{'clients': 6, 'logged_in': 5, 'gave_up': 0, 'retried': 1, 'pending': 1, 'p50_ms': 60, 'p99_ms': 300, 'max_ms': 300}"
+expect "shards: others counts the pending one" "$(get "$s" 'd["others"]["clients"] - d["others"]["logged_in"]')" 1
+w=$(fo_shards "$tmp/logins.json" "$tmp/slots" redis-0 1000 3000)
+expect "window: only the sequences started in it" "$(get "$w" '[d["others"][k] for k in ("clients", "pending", "p50_ms", "p99_ms")], d["window"]')" "([4, 0, 60, 300], [1000, 3000])"
+expect "window: the target's affected share" "$(fo_affected "$w")" 1.0
+expect "affected: none in the window" "$(fo_affected "$(fo_shards "$tmp/logins.json" "$tmp/slots" redis-0 9000 9999)")" null
+expect "shards: an unreadable log" "$(fo_shards "$tmp/none.json" "$tmp/slots" redis-0)" null
+printf '%s\n' "0 5460 redis-0" "5461 10922 redis-1" >"$tmp/short"
+expect "shards: a slot with no owner" "$(fo_shards "$tmp/logins.json" "$tmp/short" redis-0)" null
+
+# fo_other_ok: at most max(2 x before, before + floor).
+o() { echo "{\"others\": {\"clients\": $1, \"gave_up\": $2, \"pending\": 0, \"p99_ms\": $3}}"; }
+expect "other: 200 within 2 x 100" "$(fo_other_ok "$(o 5 0 100)" "$(o 5 0 200)" 50)" true
+expect "other: 201 over 2 x 100" "$(fo_other_ok "$(o 5 0 100)" "$(o 5 0 201)" 50)" false
+expect "other: 60 within 10 + 50" "$(fo_other_ok "$(o 5 0 10)" "$(o 5 0 60)" 50)" true
+expect "other: 61 over 10 + 50" "$(fo_other_ok "$(o 5 0 10)" "$(o 5 0 61)" 50)" false
+expect "other: a gave-up fails" "$(fo_other_ok "$(o 5 0 100)" "$(o 5 1 100)" 50)" false
+expect "other: no logins fails" "$(fo_other_ok "$(o 5 0 100)" "$(o 0 0 None)" 50)" false
+expect "other: no before p99 fails" "$(fo_other_ok "$(o 0 0 None)" "$(o 5 0 100)" 50)" false
+expect "other: a null reading fails" "$(fo_other_ok null "$(o 5 0 100)" 50)" false
+
+# fo_wait_at: PTTLs 15000 (too late in the interval: waits for the next
+# renewal), 30000, 25000, 21200, then 20900: 9.1s after the renewal.
+printf '%s\n' 15000 30000 25000 21200 20900 20800 >"$tmp/pttl"
+stack_redis_node_cli() { local p; p=$(head -n 1 "$tmp/pttl"); sed -i 1d "$tmp/pttl"; echo "$p"; }
+expect "wait_at: the first PTTL in (20500, 21000]" "$(fo_wait_at k redis-0 9)" 9100
+expect "wait_at: polled until then" "$(cat "$tmp/pttl")" 20800
+
+if [ "$fails" -eq 0 ]; then echo "failover_test: all passed"; else echo "failover_test: $fails failure(s)"; exit 1; fi
```

- [ ] **Step 2: The test fails without the scenario**

Run: `scripts/tier2/failover_test.sh 2>&1 | tail -1`
Expected: `failover_test: 25 failure(s)`: every check (the readers do not exist).

- [ ] **Step 3: The scenario**

`git apply --exclude='scripts/tier2/failover_test.sh' PATCH`; `chmod +x scripts/tier2/failover.sh`; `bash -n scripts/tier2/failover.sh`.

- [ ] **Step 4: Every suite passes**

Run: `scripts/tier2/failover_test.sh | tail -1` → `failover_test: all passed`; then `make test-scripts` → every suite's all-pass line (`failover_test: all passed` among them) and `0 failure(s)` for the three stack tests; `go vet ./...`, `make test`, `make test-redis-local`, `make test-redis-cluster-local` → 20 packages `ok` each.

Mutation: in `fo_shards`, count the target in `others` too (`groups.setdefault("\0others", new()) if name != target else None` → `groups.setdefault("\0others", new())`): `failover_test.sh` FAILs `shards: others, every shard but the target`, `shards: others counts the pending one` and `window: only the sequences started in it`; undo it.

- [ ] **Step 5: Dry run on the dev machine**

A throwaway copy with the CPU sets on the dev machine's 2 CPUs and the scale cut (as Plan 6b3c2's Task 2 Step 3), ~15 minutes:
```bash
D=$S/dry; mkdir -p "$D"; cp scripts/tier2/failover.sh "$D/failover.sh"
sed -i 's|^t2_require_host_setup$|:|; s|^t2_mem_budget [0-9]*$|t2_mem_budget 0|; s|^t2_need curl taskset sudo python3$|t2_need curl taskset python3|' "$D/failover.sh"
sed -i 's|"$(dirname "$0")/lib.sh"|scripts/tier2/lib.sh; T2_MIN_CORES=1 T2_MIN_MEM_GB=1|; s|"$(dirname "$0")/../|scripts/|; s|\.sh"$|.sh|' "$D/failover.sh"
R=$PWD/.cache/redis/bin/redis-server
sed -i "s|^STACK_REDIS_SERVER=.*|STACK_REDIS_SERVER=$R|; s|^if ! make redis-server |if ! true |" "$D/failover.sh"
grep -n '^\. \|^:$\|t2_mem_budget\|STACK_REDIS_SERVER=\|if ! true\|t2_need' "$D/failover.sh"
rm -rf tier2-results/failover-*
GOIM_TEST_TIER=2 FAILOVER_CPUS_GATEWAY=0-1 FAILOVER_CPUS_LOGIC=0-1 FAILOVER_CPUS_LOADGEN=0-1 FAILOVER_GATEWAY_PROCS=1 \
  FAILOVER_CLIENTS=3000 FAILOVER_RATE=500 FAILOVER_FLOW_RATE=20 FAILOVER_WINDOW_S=10 FAILOVER_THROUGH_S=30 \
  FAILOVER_WHOLE_WAIT=120 FAILOVER_LG_MEMLIMIT=512MiB bash "$D/failover.sh" >"$D/out.txt" 2>&1; echo "exit $?"
python3 -c 'import json,glob; d=json.load(open(glob.glob("tier2-results/failover-*/result.json")[0])); c=d["checks"]; print(len(c), [k for k, v in c.items() if not v["pass"]]); [print(p, c[p+"_failover"]["master_ms"], c[p+"_other_shards"]["before"]["p99_ms"], c[p+"_other_shards"]["failover_window"]["others"]["p99_ms"], {g: v["missing"] for g, v in c[p+"_lease"]["gateways"].items()}) for p in ("manual", "kill", "zone")]'
```
The `grep` must show three `. ` lines naming `scripts/`, a `:` line, `t2_mem_budget 0`, the redis path, `if ! true`, and `t2_need` without sudo.

Expected (prototype): `exit 1`; **30** checks; failing `no_swap_in_manual`, `no_swap_in_kill`, `no_swap_in_zone` (the dev machine's swap; none to all three) and **`zone_other_shards`** (#144, below). `manual`: `master_ms` under 1,000 (prototype 35), other-shard p99 before ~11–13ms and in the window ~10–20ms. `kill`: `master_ms` 7,000–10,000 (prototype 9,214), window p99 under 2 × before or + 50ms (prototype 14 against 12). `zone`: `master_ms` 7,000–10,000 (prototype 8,498), window p99 ~0.5–2s (prototype 1,605 against 11). Every gateway's `missing` 0 in every phase. Then `rm -rf tier2-results/failover-*`.

**#144 (found by this dry run; the user: record it, a separate plan fixes it):** in `zone`, a login whose uid is on the stopped master gets no reply: Logic retries it until the login deadline, so each attempt holds one of the gateway's 16 `LoginRPCs` workers and one of Logic's 64 call workers for ~5–6s (prototype: the target shard's sequences took 9.8–13.9s in 2–3 attempts; in `kill` the refused connection fails each attempt at once: 8–10 attempts). Other shards' logins then wait for a worker: one attempt each, 0.1–1.7s, all starting from ~7s after the stop. At run 34's 200 logins/s a gateway would need ~130 workers held at once, so `zone_other_shards` is expected to fail on the host too, and its numbers measure #144 at scale.

- [ ] **Step 6: Commit, push**

```bash
git add scripts/tier2/failover.sh scripts/tier2/failover_test.sh Makefile
git commit -m "feat: add the Tier 2 Redis failover scenario"
git push
```

### Task 4: Run 34 on the Tier 2 host, and the record

**Files:**
- Modify: this plan's *Execution Notes*; the roadmap (the plan 6 row, #143's outcome); `README.md` only if the user asks (no README row for 6c1/6c2a, the user 2026-10-25).

**Interfaces:**
- Consumes: Task 3's scenario, on `feat/plan6c2b` pushed.

- [ ] **Step 1: The user runs it**

Ask the user to run, on the Tier 2 host, from a fresh pull of `feat/plan6c2b`, with the desktop apps closed (after `sudo scripts/tier2/host-setup.sh` if the host rebooted since the last run: the NOTRACK rules do not survive a reboot):
```bash
GOIM_TEST_TIER=2 make test-tier2 SCENARIO=failover
```
and to send back `tier2-results/failover-<timestamp>.report.txt`. About 20 minutes.

- [ ] **Step 2: Read the report**

Expected: exit 1, **3 + 3 × 8 + 3 = 30 checks**, all passing but **`zone_other_shards`** (#144: the stopped shard's logins hold the login workers; Task 3 Step 5). If `zone_other_shards` passes, record that too: #144's plan then starts from run 34's numbers, not the dry run's. For each phase read and record:
- `P_failover`: `master_ms` (manual: under 1,000; kill: ~6,000–9,000 at node timeout 5s, dev 7.7–7.8s for a SIGKILL; zone: dev 7.1–9.2s), `cluster_ok_ms`, `at_ms_since_renewal` (~9,000–9,500), `gwepochs_on`, `target`.
- `P_lease`: `missing` 0 on every gateway, `error` per gateway (kill and zone: a few, 1 per 1s retry while the master is gone, P6c2aI-3).
- `P_other_shards`: the before and the failover window's other-shard p99s and their counts, `target_affected_share`; for `zone` (#144) also the window's `max_ms`, the gateways' `logins_busy` and `logins_expired`, and how many other-shard sequences needed more than one attempt.
- `P_whole` / `P_restore`: the times (the 600k full resync, Review Focus 2).
- `P_sessions`: `through_lost` (kill and zone: the lost writes, up to ~1s of logins on the target shard, R2-O4).
- `P.redis_latency`: the masters' per-command p99 over the through window.

A failing check: record it with its numbers and ask the user (AskUserQuestion) before any change. A failed `P_lease` is the spec's lease rule broken at the defaults, not a scenario bug: stop and ask.

- [ ] **Step 3: Record and commit**

In this plan's *Execution Notes*: run 34's numbers as in Step 2, against the dev measurements (#140). In the roadmap: the plan 6 row (6c2b executed, run 34 and its result) and #143's outcome. Commit (docs), push.

---

## After this plan

- **#144's plan** (written in a fresh session, before or with 6c3; the user 2026-11-25): logins to a stopped shard hold the gateway's and Logic's login workers until the login deadline, so every shard's logins stall at zone loss. Inputs: Task 3 Step 5's numbers, run 34's `zone` reading. Ways to weigh there: Logic fails a shard group that does not answer within one read timeout (no retry to the same node), the gateway frees its worker once a batch is sent (a bound on outstanding entries instead of on RPCs), Logic frees its call worker before the follow-ups.
- **Plan 6c3** (written in a fresh session): `gwconns` shard load and deadline sizing; inputs: Review Focus 1 (a reply slower than 1s now fails), run 34's per-shard latency.
- The spec's next revision (P6c2aI-7 and this plan): the lease retry after 1s (#139), Logic's 1s Redis read timeout, the node timeout and `cluster-require-full-coverage no` as deployment requirements (#140, #142), the zone result of run 34.
- Then **6d** (Tier 3, rolling restart, the LB).

## Execution Notes

**Executed 2026-11-25, inline (superpowers:executing-plans), on `feat/plan6c2b`.** First, the user decided #143.1–7, all as proposed (`8b5db1f`).

- **Task 1** (`67e4b30`): the tests failed as Step 2 says (vet `o.redisReadTimeout undefined`; `stack_failover_test.sh` 3 failures, among them `CLUSTERDOWN The cluster is down`). After the code: `go test ./cmd/logic/` ok, `stack_failover_test.sh` 0 failures, `make test-redis-cluster-local` 20 `ok`. Both mutations were caught as Step 4 says.
- **Task 2** (`e2cffa9`): `reconnect_test` had 13 failures before the move and passed after it. `stack_failover_test.sh` and `stack_cluster_test.sh` had 0 failures, and `grep rc_sessions` prints nothing. `lib_test` `windows: two named windows pass` failed 3 of 5 times right after the stack suites, then passed 10 of 10 alone. It is the swap flake named under *Size*; the move does not touch `t2_record_*`.
- **Task 3** (`be4b287`): `failover_test` had 25 failures before and passed after. The mutation gave the 3 FAILs Step 4 names. `go vet`, `make test`, `make test-redis-local` and `make test-redis-cluster-local` gave 20 `ok` each, and `make test-scripts` was all green. The dry run exited 1 with 30 checks. It failed only `no_swap_in_*` ×3 and `zone_other_shards`. `master_ms`: manual 37, kill 7,813, zone 7,984. Other-shard p99, before → window: 11 → 12, 11 → 15, 12 → 861ms. `missing` was 0 everywhere.
- **Final review** (fresh Opus, whole branch): 0 Critical.
  - **I-1:** the flow clients' 10 login attempts span ~9s (9 waits of 0.5–1.5s). A SIGKILLed master refuses each attempt at once, and the failover took ~8.2s, so `kill_logins` could fail by chance at 200/s. Fixed with the user's agreement, after run 34: loadgen `-light-attempts` (default 10, refused below 1; `Config.Attempts`, 0 → 10) and the scenario's `FAILOVER_FLOW_ATTEMPTS` (default 30, recorded in `run`). Tests `TestLightGivesUpAfterConfiguredAttempts` and `TestRunRefusesBadFlags` (`-light-attempts 0`) went RED → GREEN.
  - **Deferred minors:** M1, a null `at_ms_since_renewal` (the worst time not reached) is recorded, not failed. M2, the flow's `t2_sessions` read overwrites main's raw files in `artifacts/`. M3, go-redis retries a read timeout up to `MaxRedirects` (3) times, so a stopped node costs ~4 × 1s, and a reply slower than 1s from a live master sends a non-idempotent EVAL again; this is carried into #144. M4, `P_lease` passes on an empty gateway map. M5, `t2_sessions`' bulk `redis-cli` has no timeout.
- **Run 34** (the user, Tier 2 host, `be4b287`, 2026-11-25; 485s; ramp 122s, 600,000 online): **exit 1, 28/30**, failing `kill_other_shards` and `zone_other_shards`. No swap in any phase.

| | manual | kill | zone |
|---|---|---|---|
| `master_ms` / `cluster_ok_ms` | 23 / 34 | 8,225 / 8,235 (dev 7.7–7.8s) | 8,228 / 8,240 (dev 7.1–9.2s) |
| `at_ms_since_renewal` | 9,020 | 9,024 | 9,018 |
| lease `missing` (gw-0/1/2, Logic) | 0 | 0 | 0 |
| lease `error` (gw-0/1/2; Logic) | 0/0/0; 0 | 7/0/7; 14 | 1/1/2; 3 |
| sweep deletions | 0 | 0 | 0 |
| restore / rejoin (`whole_s`) | 0s / 0s | 2s / 1s | 2s / 2s |
| other shards' p99, before → window (n) | 21 → 20ms (134) | 21 → **104ms** (1,228; bound 71) | 21 → **4,470ms** (1,223) |
| other shards in the window: retried, max | 0, 26ms | 4, 1,284ms | 0, 4,743ms (p50 936) |
| target shard in the window: n, retried, p50/p99/max | 71, 0, 7/63/63ms | 617, 581, 4,647/9,384/9,836ms | 623, 407, 6,825/13,611/14,125ms |
| `target_affected_share` | 0.0 | 0.94 | 0.65 |
| flow clients that gave up | 0 | 0 | 0 |
| main sessions (600k) / through window lost | all found / 0 of 8,000 | all found / 0 | all found / 0 |

  - The target was `redis-1` in every phase. gw-0's and gw-2's lease keys and `gwepochs` were all on it (Review Focus 5 happened); gw-1's key was on `redis-0`. The worst time was reached in every phase (~9.0s after gw-0's renewal), and the key lived: at the defaults with #140, the lease rule holds through a kill and a zone loss.
  - Zone: the gateways refused no login for want of a worker (`logins_busy` 0); `logins_expired` was 51 on gw-1 and 1 on gw-2. The other shards' logins queued: #144 at scale, as Task 3 Step 5 expected.
  - **Kill (not expected):** the other shards' window p99 was 104ms against a 71ms bound. 13 sequences were above it, and 4 retried, up to 1,284ms. A likely cause (not verified): a Login batch that holds a uid on the killed master keeps its `LoginRPCs` worker until its last reply, while go-redis retries the refused dial with backoff. That is #144's mechanism, but lighter. The user chose to record it under #144 and fix it in that plan.
  - Review Focus 2: the returning node's full resync loaded 202,590 keys in 0.4s (fork CoW 8MB). No second failover.
  - The masters' `evalsha` p99 over the through windows was 170–237µs.
  - Review Focus 1: Logic's log shows `redis: connection pool timeout` and `context deadline exceeded` on `gwconns SADD` from 02:07:21 to 02:07:25, at the end of the zone failover window and just after it.
- **Not rerun after I-1's fix on the host:** run 34 is the recorded result (its flow had 0 give-ups). The fix was checked by the dev dry run (below).

