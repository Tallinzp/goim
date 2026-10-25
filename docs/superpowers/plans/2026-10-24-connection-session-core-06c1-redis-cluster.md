# Plan 6c1: The Tier 2 Redis Cluster Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring up a Redis Cluster as local processes (#66) for the tests and the Tier 2 stack: every test runs on it (P5bI-4), `stack.sh` runs a stack on it, and the per-shard helpers and the session check work on it (P6b3c2I-3). Plans 6c2 (failover, zone loss) and 6c3 (`gwconns` shard load, deadline sizing) build on it.

**Architecture:** Three parts, one task each:
- **The test Cluster** (Task 1). `scripts/redis-cluster.sh` (sourced) holds the layout and the join: 3 masters, 3 replicas, zones as labels (master i in zone zi, its replica in z(i+1)). `scripts/redis-cluster-local.sh` starts the 6 processes and runs a command with `GOIM_TEST_REDIS_CLUSTER` set. `testredis.New` returns a `*redis.ClusterClient` then; `MasterOf` and `ReplicaOf` replace `NewReplica`. New targets `make test-redis-cluster-local` (dev) and `make test-redis-cluster` (Tier 2 host).
- **The stack's Cluster** (Task 2). `STACK_REDIS=cluster` starts the 6 nodes as `redis-0`..`redis-5` (pinned to the Logic set, as the single Redis), Logic with `-redis-cluster`. New helpers: `stack_redis_names`, `stack_redis_node_cli`, `stack_redis_slots`; `stack_redis_resetstat` and `stack_redis_latency` cover every node. `reconnect`'s session check sends one EVAL per slot to the slot's master. `RECONNECT_REDIS=cluster` runs `reconnect` on the Cluster.
- **The `store` scenario on the Cluster** (Task 3): checks 5–8 repeat the suite, the WAIT mutation and the eviction policy on the Cluster. Then runs 32 (`store`) and 33 (`reconnect` `reset` on the Cluster) on the host, and the record.

**Tech Stack:** Go 1.27.1 (`internal/testredis` and test files only), go-redis v9, bash 5.2, python3, the pinned `redis-server` 7.4.11 (`make redis-server`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`: *Redis schema* (Cluster, hash tags, ~l.290), *Deployment requirements* → Redis placement (R5S-O9, ~l.73), *Verification* → Tiers (~l.470). Nothing here changes it.

Inputs:
- The roadmap's #65 (the 6c scope), #66 (the Cluster as local processes), the plan 6 row's *6c inputs* (P6b3c2I-3), and the new #137 (the split, decided) and #138 (this plan's choices, proposed).
- P5bI-4 (`2026-10-08-connection-session-core-05b1-race-faults.md`, ~l.165).
- Run 31's single-instance baseline (the 6b3c2 plan's *Execution Notes*, ~l.2058).

**Decisions:**
- **#137 (the user, 2026-10-24, before writing):** 6c is split into 6c1 (this plan), 6c2 (failover incl. zone loss) and 6c3 (`gwconns` shard load, ReconcileEnd/drain deadline sizing); the Cluster is 3 masters + 3 replicas.
- **#138 (decided by the user 2026-10-24, with no advocatus-diaboli review):** *Implementation choices this plan makes* as written; run 33 keeps run 31's Logic set (11, 23); the slot map from any live node is Plan 6c2's (Review Focus 3).

**Branch:** `feat/plan6c` (from `main` `7bc215d`). Execute from its head, this plan's own commit or later.

**Size:** 3 tasks. Task 1: ~300 lines (2 new scripts, `testredis`, 9 test files, the Makefile). Task 2: ~350 lines (`stack.sh` +160, `reconnect.sh`, 4 scenarios' pinning line, 2 tests). Task 3: ~60 lines of `store.sh`, two runs and the record. The prototype `proto-6c1` (from `7bc215d`; tasks `c159d30`, `bb621e6`, `8b867c6`; pushed) ran every step. Its diff is replayed here byte for byte, one `git apply` patch per task. On the dev machine (AMD EPYC 7742, 2 cores):
- The local Cluster is up in ~11s. `make test-redis-cluster-local` (race, `-v`): every package passes in 1m54s; 0 skips as "TODO: Tier 2 integration" (51 race-suite skips and 5 miniredis-only sweeper skips, as on one primary).
- `make test` (race), `go vet ./...`, `make test-redis-local` and `make test-scripts` (with the new `stack_cluster_test.sh`, 30 checks, ~16s) green.
- Mutations: the CRC16 polynomial (0x1022) fails 2 `reconnect_test` lines; replicas joined to the next master fail `layout: replicas follow their masters`; the store scenario's WAIT mutant on the Cluster fails `TestRevokeFailsWhenReplicaDoesNotAck` with `want ErrNotReplicated` (the unmutated code passes).
- `store.sh` itself needs Docker and the host guard; its Cluster part (`mutant_run` under the Cluster script, the policy reading `noeviction=6`) ran alone.

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...`, `make test-redis-local` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- No change to the gateway, Logic, the store, the SDK, loadgen or the wire: Go changes only in `internal/testredis` and `_test.go` files.
- The pinned `redis-server` 7.4.11; no Docker for the Cluster (#66). Its ports stay below the race suite's 20000–32000: tests 16400–16405 (bus 16410–16415), the stack `STACK_PORT_BASE` + 20..25 (bus + 30..35).
- `STACK_REDIS` defaults to `single`: every scenario keeps its Redis and its numbers; a Cluster run is opt-in (`RECONNECT_REDIS=cluster`). No check of any scenario changes; `store` gains 4 (8 in all).

## Review Focus

1. **A Cluster that is not yet one.** After the join, gossip spreads the replicas' roles over ~2s; a client that loads the slot map from a node that has not heard sees 6 masters. `redis_cluster_join` waits until every node reports `cluster_state:ok` and sees 3 replicas, and every replica's link is up. `stack_cluster_test.sh` checks the layout from the replicas' side.
2. **`CLUSTER SLOTS` leaves out a replica until its first write** (Redis 7: replication offset 0). Anything that looks for replicas reads `CLUSTER SHARDS`, `NODES` or `REPLICAS`; `TestClusterHasThreeMastersWithReplicas` reads `SHARDS`.
3. **The session check after a failover** (Plan 6c2). `stack_redis_slots` reads `CLUSTER NODES` from `redis-0`; with `redis-0` down it prints nothing, and `rc_sessions` gives null (`P_sessions` fails, named). Not reached in this plan; 6c2 must read the map from a live node.
4. **The session check's cost on one Redis.** It now sends ~16k one-slot EVALs at 600k (was ~1,200 of 500 keys): one code path for both modes. Run 33 shows the time on the Cluster; `P_sessions` gates the same reading as before.
5. **Test helpers on an interface.** The test stores and fixtures hold a `redis.UniversalClient`; code that needs one node (WAIT, `INFO replication`, `DEBUG`) asks `testredis.MasterOf`/`ReplicaOf` for the key's node. A test that used the pool of one node for a key on another would fail with MOVED on the Cluster only; the Cluster run passes every package.

## Implementation choices this plan makes

These are #138 in the roadmap (decided 2026-10-24).
1. **Zones as labels.** One host has no zones. Master i is in zone zi, its replica in z((i+1) mod 3), so one zone holds one master and another master's replica, the spec's placement (R5S-O9). `redis/layout` records it for Plan 6c2's zone loss.
2. **The bus port named** (`--cluster-port`): the default (port + 10000) lands in the race suite's range.
3. **`cluster-node-timeout` 15s** (Redis's default), as `REDIS_CLUSTER_NODE_TIMEOUT_MS`. Plan 6c2 measures the failover it gives against the lease rule (TTL 30s > failover + renewal 10s).
4. **`STACK_REDIS=single` stays the default.** The Cluster's 6 nodes share the Logic set with Logic (#66), which changes every scenario's numbers; scenarios opt in.
5. **One EVAL per slot for the session check**, in both modes (P6b3c2I-3's "per-key or per-slot form"); per-key `HVALS` would be 600k round trips.
6. **The store scenario keeps its Docker primary + replica** for checks 1–4 and adds the Cluster (pinned build, no Docker) as 5–8: the race suite and checks 1–4 stay on one primary (P5bI-4).
7. **`stack_redis_latency` on a Cluster is per node** (`{NAME: {"role", "zone", "cmds"}}`): its readers only record it. The `gwconns` shard marking is Plan 6c3's.

## File Structure

- `scripts/redis-cluster.sh` (new): `redis_cluster_layout`, `redis_cluster_args`, `redis_cluster_join` (Task 1).
- `scripts/redis-cluster-local.sh` (new): the test Cluster around a command (Task 1).
- `internal/testredis/testredis.go`, `testredis_test.go`: `Cluster`, `New` → `redis.UniversalClient`, `MasterOf`, `ReplicaOf` (Task 1).
- `internal/store/{helpers,each,revoke,revoke_reroute}_test.go`, `internal/{logic,gateway,sdk}/…_test.go`: the interface type, the Cluster's node lookups (Task 1).
- `Makefile`: `test-redis-cluster-local`, `test-redis-cluster` (Task 1); `stack_cluster_test.sh` in `test-scripts` (Task 2).
- `scripts/stack.sh`: `STACK_REDIS`, `stack_redis_cluster_up`, the per-node helpers, `stack_down`'s Redis step (Task 2).
- `scripts/stack_cluster_test.sh` (new) (Task 2).
- `scripts/tier2/reconnect.sh`, `reconnect_test.sh`: `rc_sessions_cmds`, `rc_sessions_join`, `RECONNECT_REDIS` (Task 2).
- `scripts/tier2/{capacity,gwloss,reconnect,slowlogic}.sh`: the pinning check over `stack_redis_names` (Task 2).
- `scripts/tier2/store.sh`: checks 5–8 (Task 3).
- This plan's *Execution Notes*, the roadmap, `README.md` if run 33 gives a row (Task 3).

---

### Task 1: The test Cluster and `testredis`

**Files:**
- Create: `scripts/redis-cluster.sh`, `scripts/redis-cluster-local.sh`
- Modify: `internal/testredis/testredis.go`, `internal/testredis/testredis_test.go`, `internal/store/helpers_test.go`, `internal/store/each_test.go`, `internal/store/revoke_test.go`, `internal/store/revoke_reroute_test.go`, `internal/logic/{helpers,kick,service,sweeper}_test.go`, `internal/gateway/e2e_test.go`, `internal/sdk/e2e_test.go`, `Makefile`

**Interfaces:**
- Produces (shell, for Task 2 and Plan 6c2): `redis_cluster_layout` (6 lines `NODE ROLE ZONE MASTER`, `redis-0..2 master z0..z2 -`, `redis-3 replica z1 redis-0`, `redis-4 replica z2 redis-1`, `redis-5 replica z0 redis-2`); `redis_cluster_args PORT BUS DIR` (sets the array `REDIS_CLUSTER_ARGS`); `redis_cluster_join CLI P0..P5` (0 = formed; 1 with a message after 30s); setting `REDIS_CLUSTER_NODE_TIMEOUT_MS` (15000).
- Produces (Go, test only): `testredis.Cluster() bool`; `testredis.New(t) redis.UniversalClient` (a `*redis.ClusterClient` when `GOIM_TEST_REDIS_CLUSTER` is set); `testredis.MasterOf(t, rdb, key) *redis.Client`; `testredis.ReplicaOf(t, rdb, key) *redis.Client` (replaces `NewReplica`). `Real()` is also true on a Cluster.
- Produces (make): `test-redis-cluster-local`, `test-redis-cluster` (guarded).

- [ ] **Step 1: The patch**

Apply from the repository root with `git apply` (the patch below, saved to a file in the session scratchpad), then `chmod +x scripts/redis-cluster-local.sh` (a patch carries the mode, but check it: `git apply` keeps `new file mode 100755`):

```diff
diff --git a/Makefile b/Makefile
index fdf4dee..affd4e8 100644
--- a/Makefile
+++ b/Makefile
@@ -7,7 +7,7 @@ export GOIM_REDIS_REPLICA_PORT ?= 16380
 COMPOSE_REDIS := docker compose -f deploy/compose/redis-test.yml
 TIER2_GUARD := test "$$GOIM_TEST_TIER" = 2 || { echo "Tier 2 host only: set GOIM_TEST_TIER=2"; exit 1; }
 
-.PHONY: test test-race-suite test-scripts test-load-probe redis-server test-redis-local proto redis-up redis-down test-redis test-tier2 deploy deploy-down deploy-status deploy-logs deploy-token
+.PHONY: test test-race-suite test-scripts test-load-probe redis-server test-redis-local test-redis-cluster-local proto redis-up redis-down test-redis test-redis-cluster test-tier2 deploy deploy-down deploy-status deploy-logs deploy-token
 
 # Tier 1a (dev machine): unit and Lua tests on in-process miniredis, race detector on.
 test:
@@ -51,6 +51,11 @@ redis-server:
 test-redis-local:
 	scripts/redis-local.sh $(REDIS_SERVER) go test -race -count=1 ./...
 
+# The same on a local Redis Cluster (P5bI-4, Plan 6c1): 3 masters and 3
+# replicas from scripts/redis-cluster-local.sh, no Docker.
+test-redis-cluster-local:
+	scripts/redis-cluster-local.sh $(REDIS_SERVER) go test -race -count=1 ./...
+
 # Regenerate api/internalpb and api/clientpb from api/internal.proto and
 # api/client.proto. Needs protoc 33.1 and the two pinned plugins (Plan 2,
 # Task 1); the generated code is committed.
@@ -72,6 +77,11 @@ test-redis:
 	@$(TIER2_GUARD)
 	GOIM_TEST_REDIS=127.0.0.1:$(GOIM_REDIS_PORT) GOIM_TEST_REDIS_REPLICA=127.0.0.1:$(GOIM_REDIS_REPLICA_PORT) go test -race -count=1 ./...
 
+# Tier 2 host: every test on the pinned 7.4.11 as a Redis Cluster (P5bI-4).
+test-redis-cluster:
+	@$(TIER2_GUARD)
+	scripts/redis-cluster-local.sh $(REDIS_SERVER) go test -race -count=1 ./...
+
 # Tier 2 scenario entry point (spec: automation contract). The caller sets
 # GOIM_TEST_TIER (P5bI-5), e.g. GOIM_TEST_TIER=2 make test-tier2 SCENARIO=race;
 # scenarios: store, race, foundation, smoke, capacity, gwloss, slowlogic, floods, reconnect, probe (read-only, no guard).
diff --git a/internal/gateway/e2e_test.go b/internal/gateway/e2e_test.go
index d737a69..16d8ed7 100644
--- a/internal/gateway/e2e_test.go
+++ b/internal/gateway/e2e_test.go
@@ -25,7 +25,7 @@ import (
 // gateway package itself does not (TestGatewayDoesNotImportTheStore).
 type e2e struct {
 	*core
-	rdb *redis.Client
+	rdb redis.UniversalClient
 	key auth.PrivateKey
 }
 
diff --git a/internal/logic/helpers_test.go b/internal/logic/helpers_test.go
index 626a6d2..33178d8 100644
--- a/internal/logic/helpers_test.go
+++ b/internal/logic/helpers_test.go
@@ -39,7 +39,7 @@ func testConfig() Config {
 }
 
 // newStore returns a real store on a fresh miniredis (or the Tier 2 Redis).
-func newStore(t *testing.T, cfg Config) (*store.Store, *redis.Client) {
+func newStore(t *testing.T, cfg Config) (*store.Store, redis.UniversalClient) {
 	t.Helper()
 	rdb := testredis.New(t)
 	return store.New(rdb, cfg.Store), rdb
diff --git a/internal/logic/kick_test.go b/internal/logic/kick_test.go
index 2707b34..35c2e18 100644
--- a/internal/logic/kick_test.go
+++ b/internal/logic/kick_test.go
@@ -100,7 +100,7 @@ func staticDial(addrs map[string]string) DialFunc {
 type kickEnv struct {
 	k   *Kicker
 	st  *store.Store
-	rdb *redis.Client
+	rdb redis.UniversalClient
 	m   *Metrics
 	cfg KickConfig
 }
diff --git a/internal/logic/service_test.go b/internal/logic/service_test.go
index 068992c..b60c366 100644
--- a/internal/logic/service_test.go
+++ b/internal/logic/service_test.go
@@ -54,7 +54,7 @@ type env struct {
 	cfg   Config
 	svc   *Service
 	real  *store.Store
-	rdb   *redis.Client
+	rdb   redis.UniversalClient
 	kicks *kickRecorder
 	m     *Metrics
 	key   auth.PrivateKey
diff --git a/internal/logic/sweeper_test.go b/internal/logic/sweeper_test.go
index 93ed598..23952a0 100644
--- a/internal/logic/sweeper_test.go
+++ b/internal/logic/sweeper_test.go
@@ -16,7 +16,7 @@ type sweepEnv struct {
 	t   *testing.T
 	sw  *Sweeper
 	st  Store
-	rdb *redis.Client
+	rdb redis.UniversalClient
 	clk *fakeClock
 	m   *Metrics
 	cfg Config
diff --git a/internal/sdk/e2e_test.go b/internal/sdk/e2e_test.go
index cda7d91..e78eb6e 100644
--- a/internal/sdk/e2e_test.go
+++ b/internal/sdk/e2e_test.go
@@ -40,7 +40,7 @@ func (staticKeys) Run(context.Context) {}
 // stack is Logic and its gateways.
 type stack struct {
 	t        *testing.T
-	rdb      *redis.Client
+	rdb      redis.UniversalClient
 	jwt      auth.PrivateKey
 	noise    wire.Key
 	grpcAddr string
diff --git a/internal/store/each_test.go b/internal/store/each_test.go
index 9c1ca64..a2a2ffc 100644
--- a/internal/store/each_test.go
+++ b/internal/store/each_test.go
@@ -146,8 +146,25 @@ func TestEachReportsInvalidAtOnceAndKeepsOrderInSlices(t *testing.T) {
 }
 
 func TestMasterAddrOnSingleNodeIsOneGroup(t *testing.T) {
+	if testredis.Cluster() {
+		t.Skip("a Cluster run: TestMasterAddrOnClusterIsTheOwner")
+	}
 	s, _ := newStore(t)
 	if a, b := s.masterAddr(ctx, sessKey("u1")), s.masterAddr(ctx, sessKey("u2")); a != "" || b != "" {
 		t.Fatalf("a single node must form the one \"\" group: %q, %q", a, b)
 	}
 }
+
+// Tier 2 Cluster (P5bI-4): runEach groups by the master that owns the key.
+func TestMasterAddrOnClusterIsTheOwner(t *testing.T) {
+	if !testredis.Cluster() {
+		t.Skip("not a Cluster run: TestMasterAddrOnSingleNodeIsOneGroup")
+	}
+	s, rdb := newStore(t)
+	for _, uid := range []string{"a", "b", "c"} { // one on each master
+		key := sessKey(uid)
+		if got, want := s.masterAddr(ctx, key), testredis.MasterOf(t, rdb, key).Options().Addr; got != want {
+			t.Fatalf("masterAddr(%s) = %q, want its owner %q", key, got, want)
+		}
+	}
+}
diff --git a/internal/store/helpers_test.go b/internal/store/helpers_test.go
index 83c648b..8c47bee 100644
--- a/internal/store/helpers_test.go
+++ b/internal/store/helpers_test.go
@@ -13,7 +13,7 @@ var ctx = context.Background()
 
 const field = "im:mobile"
 
-func newStore(t *testing.T) (*Store, *redis.Client) {
+func newStore(t *testing.T) (*Store, redis.UniversalClient) {
 	t.Helper()
 	rdb := testredis.New(t)
 	return New(rdb, DefaultConfig()), rdb
@@ -43,7 +43,7 @@ func login1(t *testing.T, s *Store, uid string, e session.Entry) LoginResult {
 	return res
 }
 
-func stored(t *testing.T, rdb *redis.Client, uid string) string {
+func stored(t *testing.T, rdb redis.UniversalClient, uid string) string {
 	t.Helper()
 	v, err := rdb.HGet(ctx, sessKey(uid), field).Result()
 	if err == redis.Nil {
diff --git a/internal/store/revoke_reroute_test.go b/internal/store/revoke_reroute_test.go
index a764ca8..93f1fae 100644
--- a/internal/store/revoke_reroute_test.go
+++ b/internal/store/revoke_reroute_test.go
@@ -29,16 +29,17 @@ func failedOverMaster(t *testing.T, reply string) *redis.Client {
 }
 
 // rerouteStore resolves every key to old until the slot map has been reloaded
-// mapAfter times, and to the store's own node after that (mapAfter < 0: never).
-// It returns the store, its own node and the reload counter.
-func rerouteStore(t *testing.T, old *redis.Client, mapAfter int32) (*Store, *redis.Client, *atomic.Int32) {
+// mapAfter times, and to the key's master on the store's own Redis after that
+// (mapAfter < 0: never). It returns the store, its own client and the reload
+// counter.
+func rerouteStore(t *testing.T, old *redis.Client, mapAfter int32) (*Store, redis.UniversalClient, *atomic.Int32) {
 	t.Helper()
 	s, rdb := newStore(t)
 	var reloads atomic.Int32
 	s.reloadSlots = func(context.Context) { reloads.Add(1) }
-	s.masterFor = func(context.Context, string) (*redis.Client, error) {
+	s.masterFor = func(_ context.Context, key string) (*redis.Client, error) {
 		if mapAfter >= 0 && reloads.Load() >= mapAfter {
-			return rdb, nil
+			return testredis.MasterOf(t, rdb, key), nil
 		}
 		return old, nil
 	}
@@ -110,12 +111,12 @@ func TestRevokeGivesUpWhenTheSlotMapDoesNotChange(t *testing.T) {
 	}
 }
 
-// PI-6: a single-node client has no slot map, and ErrNotReplicated or a
+// PI-6: only a cluster client has a slot map, and ErrNotReplicated or a
 // script error is final: neither triggers a re-route.
 func TestRevokeDoesNotRerouteFinalErrors(t *testing.T) {
 	s, _ := newStore(t)
-	if s.reloadSlots != nil {
-		t.Fatal("a single-node client must not get a slot-map reload")
+	if got := s.reloadSlots != nil; got != testredis.Cluster() {
+		t.Fatalf("slot-map reload set = %v, want %v: only a cluster client has one", got, testredis.Cluster())
 	}
 	for _, err := range []error{ErrNotReplicated, context.DeadlineExceeded,
 		redis.Nil /* a Redis reply */, nil} {
diff --git a/internal/store/revoke_test.go b/internal/store/revoke_test.go
index d55476a..0a88440 100644
--- a/internal/store/revoke_test.go
+++ b/internal/store/revoke_test.go
@@ -146,18 +146,18 @@ func TestRevokeEndToEnd(t *testing.T) {
 func TestRevokeFailsWhenReplicaDoesNotAck(t *testing.T) {
 	testredis.RequireReal(t)
 	rdb := testredis.New(t)
-	replica := testredis.NewReplica(t)
+	uid := testredis.UID()
+	replica := testredis.ReplicaOf(t, rdb, revKey(uid))
 	cfg := DefaultConfig()
 	cfg.WaitTimeout = 200 * time.Millisecond
 	s := New(rdb, cfg)
-	uid := testredis.UID()
 	e := entry(1, "d", 1, nowMs(t, s))
 	if err := rdb.HSet(ctx, sessKey(uid), field, e.Encode()).Err(); err != nil {
 		t.Fatal(err)
 	}
 
 	// Every write so far is acknowledged; only the marker write will not be.
-	waitReplicaAcked(t, rdb)
+	waitReplicaAcked(t, testredis.MasterOf(t, rdb, revKey(uid)))
 
 	slept := make(chan error, 1)
 	go func() { slept <- replica.Do(ctx, "DEBUG", "SLEEP", "1").Err() }()
diff --git a/internal/testredis/testredis.go b/internal/testredis/testredis.go
index 4bd5074..8ee508a 100644
--- a/internal/testredis/testredis.go
+++ b/internal/testredis/testredis.go
@@ -4,9 +4,12 @@
 // miniredis. With GOIM_TEST_REDIS (and GOIM_TEST_REDIS_REPLICA) set, tests run
 // against a real Redis primary with one replica: on the dev machine `make
 // test-redis-local` starts two redis-server processes (no Docker); on the Tier
-// 2 host `make redis-up` starts the compose one. Tests that need real Redis
-// behaviour (replication, WAIT, DEBUG) call RequireReal and are skipped
-// without it; Plan 1's Execution Notes list what that leaves unverified.
+// 2 host `make redis-up` starts the compose one. With GOIM_TEST_REDIS_CLUSTER
+// set (the nodes' addresses, comma-separated) they run against a Redis
+// Cluster instead (P5bI-4, Plan 6c1): `make test-redis-cluster-local` starts
+// 3 masters and 3 replicas. Tests that need real Redis behaviour
+// (replication, WAIT, DEBUG) call RequireReal and are skipped without it;
+// Plan 1's Execution Notes list what that leaves unverified.
 package testredis
 
 import (
@@ -14,6 +17,7 @@ import (
 	"encoding/hex"
 	"math/rand/v2"
 	"os"
+	"strings"
 	"testing"
 	"time"
 
@@ -21,21 +25,30 @@ import (
 	"github.com/redis/go-redis/v9"
 )
 
-// Real reports whether tests run against a real Redis (GOIM_TEST_REDIS is set).
-func Real() bool { return os.Getenv("GOIM_TEST_REDIS") != "" }
+// Real reports whether tests run against a real Redis (GOIM_TEST_REDIS or
+// GOIM_TEST_REDIS_CLUSTER is set).
+func Real() bool { return os.Getenv("GOIM_TEST_REDIS") != "" || Cluster() }
+
+// Cluster reports whether tests run against a Redis Cluster (GOIM_TEST_REDIS_CLUSTER is set).
+func Cluster() bool { return os.Getenv("GOIM_TEST_REDIS_CLUSTER") != "" }
 
 // RequireReal skips t at Tier 1a.
 func RequireReal(t testing.TB) {
 	t.Helper()
 	if !Real() {
 		t.Skip("TODO: Tier 2 integration — needs a real Redis primary and replica " +
-			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA; make test-redis-local); see Plan 1 Execution Notes")
+			"(GOIM_TEST_REDIS, GOIM_TEST_REDIS_REPLICA; make test-redis-local), or a Cluster " +
+			"(GOIM_TEST_REDIS_CLUSTER; make test-redis-cluster-local); see Plan 1 Execution Notes")
 	}
 }
 
-// New returns a client for the test Redis, closed when the test ends.
-func New(t testing.TB) *redis.Client {
+// New returns a client for the test Redis, closed when the test ends: a
+// *redis.ClusterClient on a Cluster, else a *redis.Client.
+func New(t testing.TB) redis.UniversalClient {
 	t.Helper()
+	if Cluster() {
+		return dialCluster(t, strings.Split(os.Getenv("GOIM_TEST_REDIS_CLUSTER"), ","))
+	}
 	if Real() {
 		return dial(t, os.Getenv("GOIM_TEST_REDIS"))
 	}
@@ -45,14 +58,55 @@ func New(t testing.TB) *redis.Client {
 	return c
 }
 
-// NewReplica returns a client for the real replica. It skips t at Tier 1a.
-func NewReplica(t testing.TB) *redis.Client {
+// MasterOf returns a client for the master that owns key: rdb itself on a
+// single Redis, the owning node's client on a Cluster (not closed by the
+// test; the cluster client owns it).
+func MasterOf(t testing.TB, rdb redis.UniversalClient, key string) *redis.Client {
+	t.Helper()
+	switch c := rdb.(type) {
+	case *redis.Client:
+		return c
+	case *redis.ClusterClient:
+		m, err := c.MasterForKey(context.Background(), key)
+		if err != nil {
+			t.Fatalf("master for %s: %v", key, err)
+		}
+		return m
+	default:
+		t.Fatalf("testredis: unsupported client %T", rdb)
+		return nil
+	}
+}
+
+// ReplicaOf returns a client for the replica of key's master: the one
+// replica (GOIM_TEST_REDIS_REPLICA) on a single Redis, on a Cluster the
+// first replica CLUSTER REPLICAS names. It skips t at Tier 1a.
+func ReplicaOf(t testing.TB, rdb redis.UniversalClient, key string) *redis.Client {
 	t.Helper()
 	RequireReal(t)
-	addr := os.Getenv("GOIM_TEST_REDIS_REPLICA")
-	if addr == "" {
-		addr = "127.0.0.1:16380"
+	if !Cluster() {
+		addr := os.Getenv("GOIM_TEST_REDIS_REPLICA")
+		if addr == "" {
+			addr = "127.0.0.1:16380"
+		}
+		return dial(t, addr)
+	}
+	m := MasterOf(t, rdb, key)
+	ctx := context.Background()
+	id, err := m.ClusterMyID(ctx).Result()
+	if err != nil {
+		t.Fatalf("CLUSTER MYID on %s: %v", m.Options().Addr, err)
+	}
+	lines, err := m.Do(ctx, "CLUSTER", "REPLICAS", id).StringSlice()
+	if err != nil || len(lines) == 0 {
+		t.Fatalf("CLUSTER REPLICAS of %s: %v %v (want at least one replica)", m.Options().Addr, lines, err)
+	}
+	// "<id> <ip:port@cport> slave <master-id> ...": the address before the '@'.
+	f := strings.Fields(lines[0])
+	if len(f) < 2 {
+		t.Fatalf("CLUSTER REPLICAS line %q", lines[0])
 	}
+	addr, _, _ := strings.Cut(f[1], "@")
 	return dial(t, addr)
 }
 
@@ -68,6 +122,18 @@ func dial(t testing.TB, addr string) *redis.Client {
 	return c
 }
 
+func dialCluster(t testing.TB, addrs []string) *redis.ClusterClient {
+	t.Helper()
+	c := redis.NewClusterClient(&redis.ClusterOptions{Addrs: addrs})
+	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
+	defer cancel()
+	if err := c.ForEachShard(ctx, func(ctx context.Context, n *redis.Client) error { return n.Ping(ctx).Err() }); err != nil {
+		t.Fatalf("test Redis Cluster not reachable at %v (make test-redis-cluster-local): %v", addrs, err)
+	}
+	t.Cleanup(func() { _ = c.Close() })
+	return c
+}
+
 // UID returns a uid no other test uses, so tests never need FLUSHALL.
 func UID() string { return "u" + randHex() }
 
diff --git a/internal/testredis/testredis_test.go b/internal/testredis/testredis_test.go
index ad4ebe5..58915fb 100644
--- a/internal/testredis/testredis_test.go
+++ b/internal/testredis/testredis_test.go
@@ -4,6 +4,8 @@ import (
 	"context"
 	"strings"
 	"testing"
+
+	"github.com/redis/go-redis/v9"
 )
 
 func TestNewGivesAWorkingRedis(t *testing.T) {
@@ -13,11 +15,50 @@ func TestNewGivesAWorkingRedis(t *testing.T) {
 }
 
 // Tier 2: the real Redis must have a linked replica that accepts DEBUG;
-// Revoke's WAIT test needs both.
+// Revoke's WAIT test needs both. On a Cluster, every master has one.
 func TestReplicaIsAttached(t *testing.T) {
 	ctx := context.Background()
-	New(t)
-	replica := NewReplica(t)
+	rdb := New(t)
+	// Slots 15495, 3300 and 7365: one key on each master of the even split.
+	for _, key := range []string{"{a}", "{b}", "{c}"} {
+		replica := ReplicaOf(t, rdb, key)
+		attached(t, ctx, replica)
+	}
+}
+
+// Tier 2 Cluster (P5bI-4): 3 masters, each with a replica, in the zone
+// layout of scripts/redis-cluster.sh.
+func TestClusterHasThreeMastersWithReplicas(t *testing.T) {
+	if !Cluster() {
+		t.Skip("not a Cluster run (GOIM_TEST_REDIS_CLUSTER unset)")
+	}
+	c := New(t).(*redis.ClusterClient)
+	// CLUSTER SHARDS, not SLOTS: SLOTS leaves out a replica until its
+	// replication offset is above 0, i.e. until the first write.
+	shards, err := c.ClusterShards(context.Background()).Result()
+	if err != nil {
+		t.Fatal(err)
+	}
+	owners := 0
+	for _, sh := range shards {
+		roles := map[string]int{}
+		for _, n := range sh.Nodes {
+			roles[n.Role]++
+		}
+		if len(sh.Slots) > 0 {
+			owners++
+		}
+		if roles["master"] != 1 || roles["replica"] != 1 {
+			t.Fatalf("shard with slots %v has roles %v, want one master and one replica", sh.Slots, roles)
+		}
+	}
+	if owners != 3 || len(shards) != 3 {
+		t.Fatalf("%d shards, %d owning slots; want 3 and 3", len(shards), owners)
+	}
+}
+
+func attached(t *testing.T, ctx context.Context, replica *redis.Client) {
+	t.Helper()
 	info, err := replica.Info(ctx, "replication").Result()
 	if err != nil {
 		t.Fatal(err)
diff --git a/scripts/redis-cluster-local.sh b/scripts/redis-cluster-local.sh
new file mode 100755
index 0000000..4a76bd4
--- /dev/null
+++ b/scripts/redis-cluster-local.sh
@@ -0,0 +1,52 @@
+#!/usr/bin/env bash
+# Runs a command against a local Redis Cluster (Plan 6c1, P5bI-4): 3 masters
+# and 3 replicas from scripts/redis-cluster.sh, no Docker, on ports
+# GOIM_REDIS_CLUSTER_PORT (16400) + 0..5, cluster bus + 10..15. Every node
+# accepts DEBUG (Revoke's WAIT test puts a replica to sleep). The command
+# runs with GOIM_TEST_REDIS_CLUSTER set to the 6 addresses; the processes are
+# stopped when it ends, and its exit code is the script's.
+#
+# Usage: scripts/redis-cluster-local.sh REDIS_SERVER COMMAND...
+#   make test-redis-cluster-local (dev machine), make test-redis-cluster (Tier 2 host)
+set -uo pipefail
+. "$(dirname "$0")/redis-cluster.sh"
+server=$1
+shift
+base=${GOIM_REDIS_CLUSTER_PORT:-16400}
+cli="$(dirname "$server")/redis-cli"
+for f in "$server" "$cli"; do
+  [ -x "$f" ] || { echo "redis-cluster-local: $f not found (make redis-server)" >&2; exit 2; }
+done
+for ((i = 0; i < 6; i++)); do
+  for p in $((base + i)) $((base + 10 + i)); do
+    if (exec 3<>"/dev/tcp/127.0.0.1/$p") 2>/dev/null; then
+      echo "redis-cluster-local: port $p is in use; pick another base with GOIM_REDIS_CLUSTER_PORT" >&2
+      exit 2
+    fi
+  done
+done
+
+dir=$(mktemp -d)
+pids=()
+stop() {
+  kill "${pids[@]}" 2>/dev/null
+  wait "${pids[@]}" 2>/dev/null
+  rm -rf "$dir"
+}
+trap stop EXIT
+ports=() addrs=""
+for ((i = 0; i < 6; i++)); do
+  mkdir "$dir/$i"
+  redis_cluster_args $((base + i)) $((base + 10 + i)) "$dir/$i"
+  "$server" "${REDIS_CLUSTER_ARGS[@]}" --enable-debug-command yes >"$dir/$i.log" 2>&1 &
+  pids+=($!)
+  ports+=($((base + i)))
+  addrs+="${addrs:+,}127.0.0.1:$((base + i))"
+done
+if ! redis_cluster_join "$cli" "${ports[@]}"; then
+  tail -n 5 "$dir"/*.log >&2
+  exit 2
+fi
+echo "redis-cluster-local: $("$server" --version | cut -d' ' -f3) cluster $addrs"
+
+GOIM_TEST_REDIS_CLUSTER=$addrs "$@"
diff --git a/scripts/redis-cluster.sh b/scripts/redis-cluster.sh
new file mode 100644
index 0000000..846580e
--- /dev/null
+++ b/scripts/redis-cluster.sh
@@ -0,0 +1,75 @@
+# shellcheck shell=bash
+# redis-cluster.sh: a Redis Cluster of local redis-server processes (Plan 6c1,
+# choice #66): 3 masters and 3 replicas, no Docker. Zones are labels only
+# (spec: Redis placement, R5S-O9): master i is in zone zi, its replica in
+# zone z((i+1)%3), so one zone holds one master and one other master's
+# replica. Source it; the caller starts the processes (stack.sh in its own
+# process groups, redis-cluster-local.sh in the background):
+#
+#   redis_cluster_layout          # "NODE ROLE ZONE MASTER" per node: redis-0..2 masters, redis-3..5 replicas
+#   redis_cluster_args PORT BUS DIR   # sets REDIS_CLUSTER_ARGS: one node's redis-server flags
+#   redis_cluster_join CLI PORT... # the 6 ports in layout order: create, attach replicas, wait until
+#                                 #   every node reports cluster_state:ok and every replica is linked
+#
+# Settings: REDIS_CLUSTER_NODE_TIMEOUT_MS (cluster-node-timeout, default
+# 15000, Redis's default; Plan 6c2 measures the failover it gives).
+
+redis_cluster_layout() {
+  printf '%s\n' "redis-0 master z0 -" "redis-1 master z1 -" "redis-2 master z2 -" \
+    "redis-3 replica z1 redis-0" "redis-4 replica z2 redis-1" "redis-5 replica z0 redis-2"
+}
+
+# redis_cluster_args PORT BUS DIR: nothing persisted, noeviction (as the
+# single Redis), the cluster bus on BUS (not PORT + 10000, which lands in
+# the race suite's 20000-32000), nodes.conf in DIR.
+redis_cluster_args() {
+  REDIS_CLUSTER_ARGS=(--port "$1" --cluster-port "$2" --bind 127.0.0.1 --dir "$3"
+    --save "" --appendonly no --maxmemory-policy noeviction
+    --cluster-enabled yes --cluster-config-file nodes.conf
+    --cluster-node-timeout "${REDIS_CLUSTER_NODE_TIMEOUT_MS:-15000}"
+    --repl-diskless-sync yes --repl-diskless-sync-delay 0)
+}
+
+# redis_cluster_join CLI P0 P1 P2 P3 P4 P5: P0-P2 become the masters (slots
+# split evenly by redis-cli), P3-P5 the replicas of P0-P2 in that order.
+# Fails, naming what did not happen, after 30s.
+redis_cluster_join() {
+  local cli=$1 i p id ok
+  shift
+  local ports=("$@")
+  [ ${#ports[@]} -eq 6 ] || { echo "redis-cluster: want 6 ports, got ${#ports[@]}" >&2; return 1; }
+  for p in "${ports[@]}"; do
+    ok=""
+    for i in $(seq 100); do
+      "$cli" -p "$p" ping >/dev/null 2>&1 && { ok=1; break; }
+      sleep 0.1
+    done
+    [ -n "$ok" ] || { echo "redis-cluster: no answer on port $p within 10s" >&2; return 1; }
+  done
+  "$cli" --cluster create "127.0.0.1:${ports[0]}" "127.0.0.1:${ports[1]}" "127.0.0.1:${ports[2]}" \
+    --cluster-yes >/dev/null || { echo "redis-cluster: --cluster create failed" >&2; return 1; }
+  for i in 0 1 2; do
+    id=$("$cli" -p "${ports[i]}" cluster myid)
+    "$cli" --cluster add-node "127.0.0.1:${ports[i + 3]}" "127.0.0.1:${ports[0]}" \
+      --cluster-slave --cluster-master-id "$id" >/dev/null ||
+      { echo "redis-cluster: replica on port ${ports[i + 3]} not added" >&2; return 1; }
+  done
+  for i in $(seq 300); do
+    ok=1
+    # Every node must also see the 3 replicas: gossip spreads the new roles
+    # after the replicas link, and a client that loads the slot map from a
+    # node that has not heard yet sees 6 masters.
+    for p in "${ports[@]}"; do
+      "$cli" -p "$p" cluster info 2>/dev/null | grep -q '^cluster_state:ok' || ok=""
+      [ "$("$cli" -p "$p" cluster nodes 2>/dev/null | grep -cE '[ ,]slave ')" = 3 ] || ok=""
+    done
+    for p in "${ports[@]:3}"; do
+      "$cli" -p "$p" info replication 2>/dev/null | grep -q '^master_link_status:up' || ok=""
+    done
+    [ -n "$ok" ] && return 0
+    sleep 0.1
+  done
+  echo "redis-cluster: not every node reports cluster_state:ok with its replicas linked within 30s" >&2
+  "$cli" -p "${ports[0]}" cluster nodes >&2
+  return 1
+}
```

- [ ] **Step 2: The Cluster test fails on the old code, then everything passes**

Run (the tests against the old `testredis` first, then everything; `$S` is the session scratchpad):
```bash
make redis-server
git diff -- internal/testredis >"$S/tr.patch" && git apply -R "$S/tr.patch" && go vet ./internal/... 2>&1 | grep -v '^#' | head -3; git apply "$S/tr.patch"
scripts/redis-cluster-local.sh .cache/redis/bin/redis-server go test -race -count=1 ./... 2>&1 | grep -E '^(redis-cluster-local|FAIL|--- FAIL)'; echo "cluster rc=${PIPESTATUS[0]}"
make test && go vet ./... && make test-redis-local
scripts/redis-cluster-local.sh .cache/redis/bin/redis-server go test -count=1 -v ./internal/store/ ./internal/testredis/ 2>&1 |
  grep -E -- '--- (SKIP|PASS): Test(Cluster|MasterAddr|ReplicaIsAttached|RevokeFailsWhenReplica)'
```
Expected: against the old `testredis`, `vet: internal/store/each_test.go:149:15: undefined: testredis.Cluster`. Then only the line `redis-cluster-local: v=7.4.11 cluster 127.0.0.1:16400,…,127.0.0.1:16405` and `cluster rc=0` (~2 min on the dev machine; the Cluster is up in ~11s); `make test` and `make test-redis-local` green; `PASS` for `TestClusterHasThreeMastersWithReplicas`, `TestMasterAddrOnClusterIsTheOwner`, `TestReplicaIsAttached`, `TestRevokeFailsWhenReplicaDoesNotAck`, and `SKIP` only for `TestMasterAddrOnSingleNodeIsOneGroup`.

- [ ] **Step 3: Commit**

```bash
git add scripts/redis-cluster.sh scripts/redis-cluster-local.sh internal Makefile
git commit -m "feat: run every test on a local Redis Cluster" -m "scripts/redis-cluster-local.sh starts 3 masters and 3 replicas of the
pinned redis-server and runs a command against them. testredis gives a
cluster client when GOIM_TEST_REDIS_CLUSTER is set. MasterOf and
ReplicaOf find the node of a key, for WAIT and DEBUG." -m "Refs: P5bI-4, #66, #138 (Plan 6c1 Task 1)."
```

---

### Task 2: The stack's Cluster and the per-slot session check

**Files:**
- Create: `scripts/stack_cluster_test.sh`
- Modify: `scripts/stack.sh`, `scripts/tier2/reconnect.sh`, `scripts/tier2/reconnect_test.sh`, `scripts/tier2/capacity.sh`, `scripts/tier2/gwloss.sh`, `scripts/tier2/slowlogic.sh`, `Makefile`

**Interfaces:**
- Consumes: Task 1's `redis_cluster_layout`, `redis_cluster_args`, `redis_cluster_join`.
- Produces (in `stack.sh`, for Plans 6c2/6c3): setting `STACK_REDIS` (`single` | `cluster`); `stack_redis_names` (`redis` or `redis-0 … redis-5`); `stack_redis_node_cli NAME ARGS...`; `stack_redis_slots` (`START END NAME` per master's range, from `redis-0`'s `CLUSTER NODES`; `0 16383 redis` on one Redis); `stack_redis_node_latency NAME` (the old single-node form); `stack_redis_latency` (single: as before; Cluster: `{NAME: {"role": "master"|"replica", "zone", "cmds": {...}}}`); `$STACK_DIR/redis/layout` (`NAME PORT ROLE ZONE MASTER`, the start's layout); `stack.env` `REDIS_MODE`, and `REDIS_ADDR` with the 6 nodes; `stack_snapshot` also writes `redis-<j>.info` and `cluster-nodes.txt`.
- Produces (in `reconnect.sh`): `rc_sessions_cmds CLIENTS SLOTS DIR`, `rc_sessions_join CLIENTS DIR`; setting `RECONNECT_REDIS` (recorded as the phase field's `redis`).

- [ ] **Step 1: The patch**

Apply from the repository root with `git apply` (as in Task 1):

```diff
diff --git a/Makefile b/Makefile
index affd4e8..73bc46a 100644
--- a/Makefile
+++ b/Makefile
@@ -30,6 +30,7 @@ test-scripts:
 	scripts/tier2/reconnect_test.sh
 	scripts/loadgen_test.sh
 	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_test.sh
+	STACK_REDIS_SERVER=$(REDIS_SERVER) scripts/stack_cluster_test.sh
 	GOIM_REDIS_SERVER=$(REDIS_SERVER) scripts/deploy_test.sh
 
 # The Tier 1a load probe (Plan 6a2, spec: ≤ 5k connections), on the dev
diff --git a/scripts/stack.sh b/scripts/stack.sh
index ce6bb53..0919626 100644
--- a/scripts/stack.sh
+++ b/scripts/stack.sh
@@ -1,6 +1,7 @@
 # shellcheck shell=bash
 # stack.sh: a goim stack as local processes for load runs (Plan 6a1, P5bI-1):
-# one redis-server, devtoken serving the JWKS, Logic and N gateways, built
+# one redis-server (or a Redis Cluster, STACK_REDIS=cluster: Plan 6c1),
+# devtoken serving the JWKS, Logic and N gateways, built
 # without -race and run at production timing. Tier 2 scenarios pin it by
 # core type (choice #71); the Tier 1a load probe (Plan 6a2) runs it unpinned.
 # Source it, set STACK_DIR, then:
@@ -25,8 +26,12 @@
 #   stack_counters_delta A B    # {"KEY": B - A, ...} of two stack_counters readings
 #   stack_buckets NAME METRIC   # {"LE": cumulative count, ...} of histogram METRIC on NAME (null: no answer)
 #   stack_hist_quantile A B Q   # Q's bucket bound in ms over two stack_buckets readings (null: no sample)
-#   stack_redis_resetstat       # Redis CONFIG RESETSTAT: a latency window starts
-#   stack_redis_latency         # {"CMD": {"calls", "p50_us", "p99_us", "p999_us"}...} since the reset (null: no answer)
+#   stack_redis_names           # the Redis processes: "redis", or redis-0..5 on a Cluster
+#   stack_redis_node_cli NAME ARGS...  # redis-cli against one Redis process
+#   stack_redis_slots           # "START END NAME" per master's slot range ("0 16383 redis" on one Redis)
+#   stack_redis_resetstat       # Redis CONFIG RESETSTAT on every node: a latency window starts
+#   stack_redis_latency         # {"CMD": {"calls", "p50_us", "p99_us", "p999_us"}...} since the reset (null: no answer);
+#                               #   on a Cluster {NAME: {"role", "zone", "cmds": {"CMD": ...}}...}
 #   stack_down                  # gateways, then Logic, devtoken, Redis (#78); call it from the exit trap
 #   stack_health                # one line per problem: dead process, unclean stop, panic in a log
 #
@@ -34,8 +39,12 @@
 #   STACK_DIR            work dir (required): bin/, keys/, logs/, artifacts/, stack.env
 #   STACK_GATEWAYS       gateways, ids gw-0.. (default 1)
 #   STACK_PORT_BASE      redis +0, devtoken +1, Logic gRPC +2, lease +3, admin +4;
+#                        Cluster node j: +20+j, its bus +30+j;
 #                        gateway i: listen +100+10i, Kick +101+10i, admin +102+10i (default 17000:
 #                        below the race suite's 20000-32000 and the ephemeral range)
+#   STACK_REDIS          single (default): one redis-server; cluster: 3 masters and 3
+#                        replicas (scripts/redis-cluster.sh: zones z0-z2 as labels,
+#                        layout in redis/layout), Logic with -redis-cluster
 #   STACK_PIN            1: taskset gateways to T2_CPUS_GATEWAY, Logic and Redis
 #                        to T2_CPUS_LOGIC (lib.sh sets both); 0: no pinning (default)
 #   STACK_REDIS_SERVER   default: redis-server on PATH, else .cache/redis/bin/redis-server
@@ -54,7 +63,8 @@
 # the same gateway order: GATEWAY_ADDRS (client listeners), GATEWAY_IDS,
 # GATEWAY_ADMINS, GATEWAY_KICK_ADDRS; then GATEWAY_PROCS (each gateway's
 # GOMAXPROCS), NOISE_KEY_ID, NOISE_PUBLIC, JWT_KEY (the devtoken private key
-# file), REDIS_ADDR and LOGIC_ADMIN. Every process is started from this shell
+# file), REDIS_ADDR (on a Cluster the 6 nodes, comma-separated), REDIS_MODE
+# (single or cluster) and LOGIC_ADMIN. Every process is started from this shell
 # in a process group of its own, added to T2_OURS_PGIDS (lib.sh counts it as
 # ours), so a Ctrl-C reaches the run's script, whose teardown stops the stack
 # in order.
@@ -110,6 +120,10 @@ stack_port_free() { timeout 2 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$0"' "$1" 2>/
 stack_ports() {
   local base=${STACK_PORT_BASE:-17000} i
   seq "$base" $((base + 8))
+  if [ "${STACK_REDIS:-single}" = cluster ]; then
+    seq $((base + 20)) $((base + 25))
+    seq $((base + 30)) $((base + 35))
+  fi
   for ((i = 0; i < ${STACK_GATEWAYS:-1}; i++)); do
     echo $((base + 100 + 10 * i)) $((base + 101 + 10 * i)) $((base + 102 + 10 * i))
   done
@@ -141,6 +155,10 @@ stack_up() {
     return 1
   fi
   [ -x "$redis" ] || { echo "stack: $redis not found (make redis-server)" >&2; return 1; }
+  case ${STACK_REDIS:-single} in
+    single | cluster) ;;
+    *) echo "stack: STACK_REDIS=$STACK_REDIS: want single or cluster" >&2; return 1 ;;
+  esac
   local busy
   busy=$(stack_busy_ports)
   if [ -n "$busy" ]; then
@@ -178,17 +196,24 @@ stack_up() {
   done
   echo "{$kick}" >"$STACK_DIR/kick.json"
 
-  stack_start redis "$logic_cpus" "$redis" --port "$base" --bind 127.0.0.1 --dir "$STACK_DIR/redis" \
-    --save "" --appendonly no --maxmemory-policy noeviction
-  local pong=""
-  for i in $(seq 100); do
-    "$(dirname "$redis")/redis-cli" -p "$base" ping >/dev/null 2>&1 && { pong=1; break; }
-    sleep 0.1
-  done
-  if [ -z "$pong" ]; then
-    echo "stack: redis not answering on port $base within 10s; last log lines:" >&2
-    tail -n 20 "$STACK_DIR/logs/redis.log" >&2
-    return 1
+  if [ "${STACK_REDIS:-single}" = cluster ]; then
+    stack_redis_cluster_up "$redis" "$logic_cpus" || return 1
+    local j
+    redis_addr=""
+    for j in 0 1 2 3 4 5; do redis_addr+="${redis_addr:+,}127.0.0.1:$((base + 20 + j))"; done
+  else
+    stack_start redis "$logic_cpus" "$redis" --port "$base" --bind 127.0.0.1 --dir "$STACK_DIR/redis" \
+      --save "" --appendonly no --maxmemory-policy noeviction
+    local pong=""
+    for i in $(seq 100); do
+      "$(dirname "$redis")/redis-cli" -p "$base" ping >/dev/null 2>&1 && { pong=1; break; }
+      sleep 0.1
+    done
+    if [ -z "$pong" ]; then
+      echo "stack: redis not answering on port $base within 10s; last log lines:" >&2
+      tail -n 20 "$STACK_DIR/logs/redis.log" >&2
+      return 1
+    fi
   fi
   stack_start devtoken "$logic_cpus" "$bin/devtoken" serve -addr "$devtoken" -jwks "$STACK_DIR/keys/jwks.json"
   stack_wait devtoken "http://$devtoken/" || return 1
@@ -198,7 +223,7 @@ stack_up() {
   local logic=("$bin/logic")
   [ -n "${STACK_LOGIC_CPU_QUOTA:-}" ] &&
     logic=(systemd-run --user --scope --quiet -p "CPUQuota=$STACK_LOGIC_CPU_QUOTA" -- "$bin/logic")
-  stack_start logic "$logic_cpus" "${logic[@]}" -redis "$redis_addr" -redis-cluster=false \
+  stack_start logic "$logic_cpus" "${logic[@]}" -redis "$redis_addr" -redis-cluster="$([ "${STACK_REDIS:-single}" = cluster ] && echo true || echo false)" \
     -grpc-addr "$grpc" -lease-addr "$lease" -admin-addr "$admin" \
     -jwks-url "http://$devtoken/" -jwks-fallback "$STACK_DIR/keys/jwks.json" \
     -kick-addr-file "$STACK_DIR/kick.json" "${STACK_LOGIC_ARGS[@]}"
@@ -227,10 +252,35 @@ NOISE_KEY_ID=1
 NOISE_PUBLIC=$pub
 JWT_KEY=$STACK_DIR/keys/jwt.jwk
 REDIS_ADDR=$redis_addr
+REDIS_MODE=${STACK_REDIS:-single}
 LOGIC_ADMIN=$admin
 ENV
 }
 
+# stack_redis_cluster_up SERVER CPUS: the Cluster's 6 nodes (redis-0..5,
+# each a process of its own pinned to CPUS like the single Redis), joined
+# in scripts/redis-cluster.sh's layout; redis/layout gets "NAME PORT ROLE
+# ZONE MASTER" per node, the layout at the start (roles change in a failover).
+stack_redis_cluster_up() {
+  local server=$1 cpus=$2 base=${STACK_PORT_BASE:-17000} j name role zone master ports=()
+  . "$(dirname "${BASH_SOURCE[0]}")/redis-cluster.sh"
+  : >"$STACK_DIR/redis/layout"
+  j=0
+  while read -r name role zone master; do
+    mkdir -p "$STACK_DIR/redis/$name"
+    redis_cluster_args $((base + 20 + j)) $((base + 30 + j)) "$STACK_DIR/redis/$name"
+    stack_start "$name" "$cpus" "$server" "${REDIS_CLUSTER_ARGS[@]}"
+    echo "$name $((base + 20 + j)) $role $zone $master" >>"$STACK_DIR/redis/layout"
+    ports+=($((base + 20 + j)))
+    j=$((j + 1))
+  done < <(redis_cluster_layout)
+  redis_cluster_join "$(dirname "$server")/redis-cli" "${ports[@]}" >"$STACK_DIR/logs/redis-join.log" 2>&1 || {
+    echo "stack: the Redis Cluster did not form; last log lines:" >&2
+    tail -n 20 "$STACK_DIR/logs/redis-join.log" "$STACK_DIR/logs/redis-0.log" >&2
+    return 1
+  }
+}
+
 # stack_admins: "NAME ADMIN" for Logic and each running gateway.
 stack_admins() {
   local base=${STACK_PORT_BASE:-17000} i
@@ -253,27 +303,72 @@ stack_snapshot() {
     curl -sf "http://$admin/debug/pprof/goroutine?debug=1" >"$dir/$name.goroutine.txt"
   done < <(stack_admins)
   stack_redis_cli info all >"$dir/redis.info" 2>&1
+  if [ "${STACK_REDIS:-single}" = cluster ]; then
+    for name in $(stack_redis_names); do
+      stack_redis_node_cli "$name" info all >"$dir/$name.info" 2>&1
+    done
+    stack_redis_cli cluster nodes >"$dir/cluster-nodes.txt" 2>&1
+  fi
 }
 
-# stack_redis_cli ARGS...: redis-cli against the stack's Redis.
-stack_redis_cli() {
+# stack_redis_names: the Redis processes, "redis" or redis-0..5.
+stack_redis_names() {
+  if [ "${STACK_REDIS:-single}" = cluster ]; then echo redis-0 redis-1 redis-2 redis-3 redis-4 redis-5; else echo redis; fi
+}
+
+# stack_redis_node_cli NAME ARGS...: redis-cli against one Redis process
+# (redis-j on port base + 20 + j), no redirects followed.
+stack_redis_node_cli() {
   local redis=${STACK_REDIS_SERVER:-$(command -v redis-server || echo "$PWD/.cache/redis/bin/redis-server")}
-  "$(dirname "$redis")/redis-cli" -p "${STACK_PORT_BASE:-17000}" "$@"
+  local base=${STACK_PORT_BASE:-17000} port name=$1
+  shift
+  case $name in
+    redis) port=$base ;;
+    redis-[0-5]) port=$((base + 20 + ${name#redis-})) ;;
+    *) echo "stack: no Redis process $name" >&2; return 1 ;;
+  esac
+  "$(dirname "$redis")/redis-cli" -p "$port" "$@"
 }
 
-# stack_redis_resetstat: CONFIG RESETSTAT, so Redis's per-command latency
-# percentiles and call counts cover a window from now (Plan 6b2a-stress).
-stack_redis_resetstat() { [ "$(stack_redis_cli config resetstat 2>/dev/null)" = OK ]; }
+# stack_redis_cli ARGS...: redis-cli against the stack's Redis: the one
+# redis-server, or on a Cluster redis-0 (a key it does not own answers
+# MOVED: per-key commands go through stack_redis_slots, review P6b3c2I-3).
+stack_redis_cli() {
+  if [ "${STACK_REDIS:-single}" = cluster ]; then stack_redis_node_cli redis-0 "$@"; else stack_redis_node_cli redis "$@"; fi
+}
 
-# stack_redis_latency: Redis's own per-command latency since the last
-# resetstat, from INFO latencystats (p50, p99 and p99.9 in µs, Redis 7's
+# stack_redis_slots: "START END NAME" for each master's slot range, from
+# CLUSTER NODES as redis-0 sees it now (after a failover too); "0 16383
+# redis" on one Redis. Prints nothing if Redis does not answer.
+stack_redis_slots() {
+  if [ "${STACK_REDIS:-single}" != cluster ]; then
+    stack_redis_cli ping >/dev/null 2>&1 && echo "0 16383 redis"
+    return
+  fi
+  stack_redis_cli cluster nodes 2>/dev/null | awk -v base="${STACK_PORT_BASE:-17000}" '
+    $3 ~ /master/ && $3 !~ /fail/ {
+      split($2, a, "[:@]"); name = "redis-" (a[2] - base - 20)
+      for (i = 9; i <= NF; i++) { if ($i ~ /^\[/) continue; n = split($i, r, "-"); print r[1], r[n], name } }' | sort -n
+}
+
+# stack_redis_resetstat: CONFIG RESETSTAT on every Redis process, so their
+# per-command latency percentiles and call counts cover a window from now
+# (Plan 6b2a-stress). Fails if any node did not answer OK.
+stack_redis_resetstat() {
+  local name
+  for name in $(stack_redis_names); do
+    [ "$(stack_redis_node_cli "$name" config resetstat 2>/dev/null)" = OK ] || return 1
+  done
+}
+
+# stack_redis_node_latency NAME: one node's per-command latency since the
+# last resetstat, from INFO latencystats (p50, p99 and p99.9 in µs, Redis 7's
 # defaults) and the calls from INFO commandstats, as {"CMD": {"calls",
-# "p50_us", "p99_us", "p999_us"}, ...}: the single Redis's p99 for the
-# spec's per-shard numbers (R4M-9), which need a Cluster (6c). The readers'
-# own INFO and CONFIG are left out. Prints null if Redis does not answer.
-stack_redis_latency() {
+# "p50_us", "p99_us", "p999_us"}, ...}. The readers' own INFO and CONFIG
+# are left out. Prints null if the node does not answer.
+stack_redis_node_latency() {
   local info
-  info=$(stack_redis_cli info latencystats 2>/dev/null && stack_redis_cli info commandstats 2>/dev/null) &&
+  info=$(stack_redis_node_cli "$1" info latencystats 2>/dev/null && stack_redis_node_cli "$1" info commandstats 2>/dev/null) &&
     grep -q '^# Latencystats' <<<"$info" || { echo null; return; }
   awk -F'[:,=]' '{ sub(/\r$/, "") }
     /^latency_percentiles_usec_/ { c = substr($1, 26); p50[c] = $3; p99[c] = $5; p999[c] = $7; cmd[++n] = c }
@@ -281,11 +376,33 @@ stack_redis_latency() {
     END { out = ""
           for (i = 1; i <= n; i++) {
             c = cmd[i]
-            if (c == "info" || index(c, "config") == 1) continue
+            if (c == "info" || index(c, "config") == 1 || index(c, "cluster") == 1) continue
             out = out (out == "" ? "" : ", ") sprintf("\"%s\": {\"calls\": %d, \"p50_us\": %s, \"p99_us\": %s, \"p999_us\": %s}", c, calls[c], p50[c], p99[c], p999[c]) }
           print "{" out "}" }' <<<"$info"
 }
 
+# stack_redis_latency: stack_redis_node_latency of the one Redis; on a
+# Cluster, per node with its role now (master or replica) and its zone from
+# redis/layout: {NAME: {"role", "zone", "cmds": {...}}, ...}, the per-shard
+# numbers the spec asks for (R4M-9). Prints null if no node answers; a node
+# that does not answer is null.
+stack_redis_latency() {
+  [ "${STACK_REDIS:-single}" = cluster ] || { stack_redis_node_latency redis; return; }
+  local name role zone cmds out="" any=""
+  for name in $(stack_redis_names); do
+    cmds=$(stack_redis_node_latency "$name")
+    role=$(stack_redis_node_cli "$name" info replication 2>/dev/null | tr -d '\r' | sed -n 's/^role://p')
+    zone=$(awk -v n="$name" '$1 == n { print $4 }' "$STACK_DIR/redis/layout" 2>/dev/null)
+    if [ "$cmds" = null ]; then
+      out+="${out:+, }\"$name\": null"
+    else
+      any=1
+      out+="${out:+, }\"$name\": {\"role\": \"$([ "$role" = slave ] && echo replica || echo "${role:-unknown}")\", \"zone\": \"$zone\", \"cmds\": $cmds}"
+    fi
+  done
+  if [ -n "$any" ]; then echo "{$out}"; else echo null; fi
+}
+
 # stack_profile NAME SECONDS: a CPU profile of Logic and every running
 # gateway over the same SECONDS, into artifacts/NAME/<process>.cpu.pb.gz.
 # Blocks for SECONDS; a scenario takes it inside its measured window. Fails
@@ -611,9 +728,16 @@ stack_down() {
     [[ $name == gw-* ]] && gws+=("$name")
   done
   [ ${#gws[@]} -gt 0 ] && stack_term "${STACK_DOWN_WAIT:-10}" "${gws[@]}"
-  for name in logic devtoken redis; do
+  for name in logic devtoken; do
     [ -n "${STACK_PIDS[$name]:-}" ] && stack_term 5 "$name"
   done
+  # Redis last; a Cluster's nodes together, so no replica is left to
+  # fail over a stopped master.
+  local rs=()
+  for name in "${STACK_ORDER[@]}"; do
+    [[ $name == redis || $name == redis-* ]] && rs+=("$name")
+  done
+  [ ${#rs[@]} -gt 0 ] && stack_term 5 "${rs[@]}"
   STACK_ORDER=()
   STACK_PIDS=()
 }
diff --git a/scripts/stack_cluster_test.sh b/scripts/stack_cluster_test.sh
new file mode 100755
index 0000000..6d32f23
--- /dev/null
+++ b/scripts/stack_cluster_test.sh
@@ -0,0 +1,85 @@
+#!/usr/bin/env bash
+# Tests for scripts/stack.sh's Redis Cluster (Plan 6c1, STACK_REDIS=cluster)
+# on any machine (make test-scripts): 3 masters and 3 replicas in the zone
+# layout come up pinned with the stack, Logic runs as a cluster client,
+# logins land on every master, the per-node helpers read each node, the
+# session check reads every client's key per slot, and stack_down stops
+# every node. Needs redis-server (make redis-server) and curl.
+set -uo pipefail
+cd "$(git rev-parse --show-toplevel)"
+. scripts/tier2/lib.sh
+. scripts/stack.sh
+. scripts/loadgen.sh
+eval "$(sed -n '/^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_sessions() {/,/^}/p' scripts/tier2/reconnect.sh)"
+STACK_DIR=$(mktemp -d)
+trap 'lg_stop; stack_down; rm -rf "$STACK_DIR"' EXIT
+fails=0
+ok() { echo "ok   $1"; }
+bad() { echo "FAIL $1"; fails=$((fails + 1)); }
+expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
+get() { python3 -c 'import json,sys; d=json.loads(sys.argv[1]); print(eval(sys.argv[2]))' "$1" "$2"; }
+
+STACK_GATEWAYS=1
+STACK_PORT_BASE=${STACK_PORT_BASE:-17600}
+STACK_PIN=1
+ncpu=$(getconf _NPROCESSORS_ONLN)
+T2_CPUS_GATEWAY=0-$((ncpu - 1))
+T2_CPUS_LOGIC=0
+STACK_GATEWAY_ARGS=(-max-conns 1000 -pending-cap 1000 -logout-queue 1000)
+b=$STACK_PORT_BASE
+
+expect "ports: single has no Cluster ports" "$(STACK_REDIS=single stack_ports | grep -cx $((b + 20)))" 0
+STACK_REDIS=cluster
+expect "ports: the nodes and their buses" "$(stack_ports | grep -cxE "$((b + 20))|$((b + 25))|$((b + 30))|$((b + 35))")" 4
+expect "names: redis-0..5" "$(stack_redis_names)" "redis-0 redis-1 redis-2 redis-3 redis-4 redis-5"
+out=$(STACK_REDIS=bogus; stack_up 2>&1) && bad "up: STACK_REDIS=bogus must fail" ||
+  case $out in *"want single or cluster"*) ok "up: an unknown STACK_REDIS is refused" ;; *) bad "up: STACK_REDIS=bogus: [$out]" ;; esac
+
+stack_build || { echo "build failed"; exit 1; }
+if stack_up; then ok "up: every process Ready"; else bad "up"; exit 1; fi
+. "$STACK_DIR/stack.env"
+expect "env: REDIS_MODE" "$REDIS_MODE" cluster
+expect "env: the 6 nodes" "$REDIS_ADDR" "$(for j in 0 1 2 3 4 5; do printf '127.0.0.1:%d\n' $((b + 20 + j)); done | paste -sd, -)"
+grep -q -- '-redis-cluster=true' "/proc/${STACK_PIDS[logic]}/cmdline" 2>/dev/null ||
+  tr '\0' ' ' <"/proc/${STACK_PIDS[logic]}/cmdline" | grep -q -- '-redis-cluster=true' && ok "logic: a cluster client" || bad "logic: not -redis-cluster=true"
+wrong=""
+for n in $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+expect "pin: every node on the Logic set" "${wrong# }" ""
+expect "layout: masters and replicas in zones" "$(awk '{ print $1, $3, $4, $5 }' "$STACK_DIR/redis/layout" | paste -sd, -)" \
+  "redis-0 master z0 -,redis-1 master z1 -,redis-2 master z2 -,redis-3 replica z1 redis-0,redis-4 replica z2 redis-1,redis-5 replica z0 redis-2"
+expect "layout: replicas follow their masters" "$(for j in 3 4 5; do stack_redis_node_cli redis-$j info replication | tr -d '\r' | sed -n 's/^master_port://p'; done | paste -sd, -)" \
+  "$((b + 20)),$((b + 21)),$((b + 22))"
+expect "slots: 3 ranges, 0-16383, one per master" "$(stack_redis_slots | paste -sd, -)" "0 5460 redis-0,5461 10922 redis-1,10923 16383 redis-2"
+
+stack_redis_resetstat && ok "redis: resetstat on every node" || bad "redis: resetstat"
+lg_start c -mode light -clients 30 -rate 100 -uid-prefix sc && lg_wait_online 30 30 && ok "loadgen: 30 online" || bad "loadgen: not online"
+# 30 uids over 3 masters: each gets some (all on two: p ~ 1.5e-5).
+expect "logins: sess keys on every master" "$(for j in 0 1 2; do [ "$(stack_redis_node_cli redis-$j dbsize)" -gt 0 ] && echo y; done | wc -l)" 3
+lat=$(stack_redis_latency)
+expect "latency: every node, its role and zone" "$(get "$lat" '[(n, d[n]["role"], d[n]["zone"]) for n in sorted(d)]')" \
+  "[('redis-0', 'master', 'z0'), ('redis-1', 'master', 'z1'), ('redis-2', 'master', 'z2'), ('redis-3', 'replica', 'z1'), ('redis-4', 'replica', 'z2'), ('redis-5', 'replica', 'z0')]"
+expect "latency: logins' scripts on every master" "$(get "$lat" 'all(d[n]["cmds"].get("evalsha", {}).get("calls", 0) > 0 for n in ("redis-0", "redis-1", "redis-2"))')" True
+expect "latency: no reader's own commands" "$(get "$lat" 'any(k == "info" or k.startswith(("config", "cluster")) for n in d for k in d[n]["cmds"])')" False
+mkdir -p "$STACK_DIR/artifacts"
+s=$(rc_sessions 0)
+expect "sessions: every client found per slot" "$(get "$s" '(d["clients"], d["found"])')" "(30, 30)"
+expect "sessions: no CROSSSLOT" "$(cat "$STACK_DIR/sessions"/*.out | grep -c CROSSSLOT)" 0
+stack_snapshot snap
+for f in redis-0.info redis-5.info cluster-nodes.txt; do
+  [ -s "$STACK_DIR/artifacts/snap/$f" ] && ok "snapshot: $f" || bad "snapshot: $f missing"
+done
+grep -q '^redis_version:' "$STACK_DIR/artifacts/snap/redis.info" && ok "snapshot: redis.info (foundation)" || bad "snapshot: redis.info"
+lg_stop
+stack_wait_idle 30 && ok "idle: the logins' Logouts settle" || bad "idle: after loadgen"
+
+pids=("${STACK_PIDS[@]}")
+stack_down
+expect "down: every node's exit status" "$(grep -c '^redis-[0-5] 0$' "$STACK_DIR/exit-status")" 6
+expect "health: a clean stop has no problems" "$(stack_health 2>&1)" ""
+left=0
+for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null && left=$((left + 1)); done
+expect "down: no process left" "$left" 0
+expect "down: no port left" "$(stack_busy_ports)" ""
+
+echo "$fails failure(s)"
+[ "$fails" -eq 0 ]
diff --git a/scripts/tier2/capacity.sh b/scripts/tier2/capacity.sh
index 59a786d..e8f82f0 100755
--- a/scripts/tier2/capacity.sh
+++ b/scripts/tier2/capacity.sh
@@ -181,7 +181,7 @@ for phase in $PHASES; do
     first=""
     wrong=""
     stack_pinned gw-0 "$T2_CPUS_GATEWAY" || wrong+=" gw-0"
-    for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+    for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
     have=$(taskset -pc "$LG_PID" 2>/dev/null)
     [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
     t2_check pinning "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
diff --git a/scripts/tier2/gwloss.sh b/scripts/tier2/gwloss.sh
index dce5ff1..329b139 100755
--- a/scripts/tier2/gwloss.sh
+++ b/scripts/tier2/gwloss.sh
@@ -497,7 +497,7 @@ for name in $PHASES; do
 
   wrong=""
   for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
-  for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+  for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
   have=$(taskset -pc "$LG_PID" 2>/dev/null)
   [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
   t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $(. "$STACK_DIR/stack.env"; echo "$GATEWAY_PROCS"), \"wrong\": $(t2_json_str "${wrong# }")"
diff --git a/scripts/tier2/reconnect.sh b/scripts/tier2/reconnect.sh
index 89708a1..e41cbdf 100755
--- a/scripts/tier2/reconnect.sh
+++ b/scripts/tier2/reconnect.sh
@@ -70,6 +70,9 @@
 #                         Pong, or its 10s Pong timeout); ghost holds at
 #                         least until DeadAfter + 5s after the mark
 #   RECONNECT_FUNNEL_BAR_S  the funnel's bar, seconds in a row (default 5)
+#   RECONNECT_REDIS       single (default, run 31's) or cluster: 3 masters and
+#                         3 replicas on the Logic set (stack.sh STACK_REDIS,
+#                         Plan 6c1); P.redis_latency is then per node
 #   RECONNECT_LG_MEMLIMIT loadgen's GOMEMLIMIT (default 16GiB, as b_stress)
 #   RECONNECT_CPUS_LOADGEN, RECONNECT_CPUS_GATEWAY, RECONNECT_CPUS_LOGIC
 #                         the CPU sets (default b_stress's: 0-5,10,12-17,22;
@@ -146,6 +149,7 @@ for v in RECONNECT_CLIENTS RECONNECT_RATE RECONNECT_HEARTBEAT RECONNECT_CAP RECO
   RECONNECT_BACK_WAIT RECONNECT_HOLD RECONNECT_GATEWAY_PROCS RECONNECT_FUNNEL_BAR_S; do
   case ${!v-1} in ''|*[!0-9]*|0*) echo "reconnect: $v must be a positive whole number" >&2; exit 1 ;; esac
 done
+case ${RECONNECT_REDIS:-single} in single | cluster) ;; *) echo "reconnect: RECONNECT_REDIS must be single or cluster" >&2; exit 1 ;; esac
 HB=${RECONNECT_HEARTBEAT:-30}
 [ "$HB" -ge 30 ] && [ "$HB" -le 300 ] || { echo "reconnect: RECONNECT_HEARTBEAT must be 30 to 300 (the gateway's -heartbeat)" >&2; exit 1; }
 N=${RECONNECT_CLIENTS:-600000} RATE=${RECONNECT_RATE:-5000}
@@ -162,7 +166,7 @@ t2_mem_budget 23
 # loadgen's fds, over the host's former 1,048,576 (#136.5, host-setup.sh).
 case " $PHASES " in *" ghost "*) t2_nofile 2097152 ;; *) t2_nofile 1048576 ;; esac
 
-export STACK_PIN=1 STACK_GATEWAYS=3
+export STACK_PIN=1 STACK_GATEWAYS=3 STACK_REDIS=${RECONNECT_REDIS:-single}
 export T2_CPUS_GATEWAY=${RECONNECT_CPUS_GATEWAY:-6-9,18-21} T2_CPUS_LOADGEN=${RECONNECT_CPUS_LOADGEN:-0-5,10,12-17,22}
 export T2_CPUS_LOGIC=${RECONNECT_CPUS_LOGIC:-11,23}
 export STACK_GATEWAY_PROCS=${RECONNECT_GATEWAY_PROCS:-4}
@@ -405,20 +409,76 @@ for i, (g, l) in enumerate(zip(gw, lines)):
 print(json.dumps(r))' "$1" "$2" "$3" 2>/dev/null || echo null
 }
 
-# rc_sessions MARK_MS: rc_sessions_cmp over loadgen's /clients and every
-# client's sess key, read in EVALs of 500 keys (HVALS each) through one
-# redis-cli: ~1,200 short scripts on the one Redis after the hold.
-rc_sessions() {
-  local a=$STACK_DIR/artifacts
-  curl -sf -m 60 "http://$(lg_control)/clients" >"$a/clients.json" || { echo null; return; }
+# rc_sessions_cmds CLIENTS SLOTS DIR: the session check's commands. Each
+# client's sess key goes to the master that owns its slot (SLOTS:
+# stack_redis_slots's "START END NAME" lines), in one EVAL per slot (HVALS
+# each; ~37 keys at 600k): a Cluster refuses a script whose keys span slots
+# (CROSSSLOT, review P6b3c2I-3). Writes DIR/NAME.cmd and DIR/NAME.idx (the
+# client index of each output line, in order) per master; fails if a slot
+# has no owner.
+rc_sessions_cmds() {
   python3 -c 'import json, sys
 c = json.load(open(sys.argv[1]))
+ranges = [(int(a), int(b), n) for a, b, n in (l.split() for l in open(sys.argv[2]) if l.strip())]
+out = sys.argv[3]
+def crc16(b):  # CRC16-XMODEM, Redis Cluster key slots
+    r = 0
+    for x in b:
+        r ^= x << 8
+        for _ in range(8):
+            r = ((r << 1) ^ 0x1021 if r & 0x8000 else r << 1) & 0xFFFF
+    return r
 lua = "local r = {} for i, k in ipairs(KEYS) do local v = redis.call(\"HVALS\", k) r[i] = #v .. \"|\" .. (v[1] or \"\") end return r"
-keys = ["sess:{%s-%d}" % (c["uid_prefix"], i) for i in range(len(c["gw"]))]
-for j in range(0, len(keys), 500):
-    b = keys[j:j + 500]
-    print("EVAL %s %d %s" % (json.dumps(lua), len(b), " ".join(b)))' "$a/clients.json" >"$STACK_DIR/sessions.cmd" || { echo null; return; }
-  stack_redis_cli --raw <"$STACK_DIR/sessions.cmd" >"$a/sessions.txt" 2>&1
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
+# rc_sessions_join CLIENTS DIR: the replies (DIR/NAME.out, one line per key)
+# put back in client order, the SESSIONS lines rc_sessions_cmp reads. Fails
+# if a reply's line count is not its NAME.idx's or a client has no line.
+rc_sessions_join() {
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
+# rc_sessions MARK_MS: rc_sessions_cmp over loadgen's /clients and every
+# client's sess key, read per slot on each key's master (rc_sessions_cmds):
+# at 600k ~16k short scripts after the hold, on one Redis or a Cluster alike.
+rc_sessions() {
+  local a=$STACK_DIR/artifacts d=$STACK_DIR/sessions f name
+  curl -sf -m 60 "http://$(lg_control)/clients" >"$a/clients.json" || { echo null; return; }
+  rm -rf "$d" && mkdir -p "$d"
+  stack_redis_slots >"$d/slots.txt"
+  rc_sessions_cmds "$a/clients.json" "$d/slots.txt" "$d" 2>"$a/sessions.err" || { echo null; return; }
+  for f in "$d"/*.cmd; do
+    name=$(basename "$f" .cmd)
+    stack_redis_node_cli "$name" --raw <"$f" >"$d/$name.out" 2>&1
+  done
+  rc_sessions_join "$a/clients.json" "$d" >"$a/sessions.txt" 2>>"$a/sessions.err" || { echo null; return; }
   rc_sessions_cmp "$a/clients.json" "$a/sessions.txt" "$1"
 }
 
@@ -450,7 +510,7 @@ for name in $PHASES; do
   t2_check "${name}_online" "$online" "\"want\": $N, \"online\": $(lg_get /stats 'd["online"]'), \"by_gateway\": $(lg_get /stats 'd["online_by_gateway"]'), \"ramp_s\": $ramp_s, \"failed\": $(lg_get /stats 'd["failed"]')"
   wrong=""
   for ((i = 0; i < STACK_GATEWAYS; i++)); do stack_pinned "gw-$i" "$T2_CPUS_GATEWAY" || wrong+=" gw-$i"; done
-  for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+  for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
   have=$(taskset -pc "$LG_PID" 2>/dev/null)
   [ "$(t2_cpulist_expand "${have##*: }")" = "$(t2_cpulist_expand "$T2_CPUS_LOADGEN")" ] || wrong+=" loadgen"
   t2_check "${name}_pinning" "$(t2_bool [ -z "$wrong" ])" "\"gateway_cpus\": $(t2_json_str "$T2_CPUS_GATEWAY"), \"logic_cpus\": $(t2_json_str "$T2_CPUS_LOGIC"), \"loadgen_cpus\": $(t2_json_str "$T2_CPUS_LOADGEN"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"wrong\": $(t2_json_str "${wrong# }")"
@@ -585,7 +645,7 @@ for name in $PHASES; do
   # "reset" is the drop's reply: /reset's {"mark", "population", "close_ms"},
   # in ghost /ghost's {"mark", "population", "ms"} (the ghosting time, not
   # a close time). The name stays as run 31 recorded it (P6b3c2I-2).
-  t2_field "$name" "{\"clients\": $N, \"rate\": $RATE, \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"precompute_at_mark\": $precompute, \"reset\": ${reply:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms, \"back_s\": $back_s, \"hold_s\": $HOLD, \"window_s\": $window_s, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"slo\": $(cat "$slo"), \"loadgen\": $st, \"gateways\": $d, \"logic\": $ld, \"cpu_sets\": $cpu_sets, \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3"), \"series\": $series, \"redis_latency\": ${redis:-null}, \"redis_reset\": $redis_reset, \"ghosts\": ${gh:-null}, \"sessions\": ${sessions:-null}, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS}"
+  t2_field "$name" "{\"clients\": $N, \"rate\": $RATE, \"redis\": \"$STACK_REDIS\", \"gateway_args\": $(t2_json_str "${STACK_GATEWAY_ARGS[*]}"), \"gateway_procs\": $STACK_GATEWAY_PROCS, \"loadgen_env\": $(t2_json_str "${LG_ENV[*]}"), \"ramp_s\": $ramp_s, \"ramp\": $ramp, \"precompute_at_mark\": $precompute, \"reset\": ${reply:-null}, \"network_change\": $nc, \"network_change_after_ms\": $nc_after_ms, \"back_s\": $back_s, \"hold_s\": $HOLD, \"window_s\": $window_s, \"storm\": $storm, \"storm_first30\": $storm30, \"loadgen_bound\": $loadgen_bound, \"slo\": $(cat "$slo"), \"loadgen\": $st, \"gateways\": $d, \"logic\": $ld, \"cpu_sets\": $cpu_sets, \"tcp\": $(t2_tcpstat_json "$STACK_DIR/tcpstat-2" "$STACK_DIR/tcpstat-3"), \"series\": $series, \"redis_latency\": ${redis:-null}, \"redis_reset\": $redis_reset, \"ghosts\": ${gh:-null}, \"sessions\": ${sessions:-null}, \"stop_s\": $stop_s, \"loadgen_exit\": $LG_STATUS}"
   t2_duration "$name" $((SECONDS - t0))
 
   a=$STACK_DIR/artifacts
@@ -602,7 +662,6 @@ t2_check artifacts "$(t2_bool [ -z "$missing" ])" "\"missing\": $(t2_json_str "$
 t2_not_covered "the drop as an LB restart or failover: Tier 2's LB is loadgen's in-process balancer (#134); Tier 3 (Plan 6d)" \
   "a carrier or regional network recovering for part of the population: ghost drops every client (the spec's worst case)" \
   "heartbeat Pong times: SDK clients record a Pong only by its absence (the 10s Pong timeout), not its delay" \
-  "per-shard Redis load with the gwconns shards marked (one redis-server, its per-command latency in P.redis_latency; Redis Cluster in Plan 6c)" \
-  "the session check in Redis Cluster: its EVALs read 500 keys of many slots (one redis-server here)" \
+  "$([ "$STACK_REDIS" = cluster ] && echo "the gwconns shards marked in P.redis_latency (Plan 6c3)" || echo "per-shard Redis load (one redis-server, its per-command latency in P.redis_latency; RECONNECT_REDIS=cluster for a Cluster)")" \
   "the network change at once: /network-change calls each client in turn (64 workers, each waits for the client's run loop); at 600k it took 27-36s in run 31, after most storm waits ended; several loadgens at Tier 3 (Plan 6d)"
 t2_finish
diff --git a/scripts/tier2/reconnect_test.sh b/scripts/tier2/reconnect_test.sh
index 098c5bf..3bd0862 100755
--- a/scripts/tier2/reconnect_test.sh
+++ b/scripts/tier2/reconnect_test.sh
@@ -3,7 +3,8 @@
 # rc_sampler lines (make test-scripts): the per-gateway rates, a late
 # scrape, loadgen's run-queue wait, and the funnel reading and its bar
 # (#133, review O3, O6); rc_sessions_cmp (review O4); and rc_heartbeats_ok
-# (the final review's I-1: the ghost allowance). The scenario
+# (the final review's I-1: the ghost allowance); rc_sessions_cmds and
+# rc_sessions_join (Plan 6c1: the session check per slot). The scenario
 # itself runs on the Tier 2 host.
 set -uo pipefail
 cd "$(git rev-parse --show-toplevel)"
@@ -15,7 +16,7 @@ fails=0
 ok() { echo "ok   $1"; }
 bad() { echo "FAIL $1"; fails=$((fails + 1)); }
 expect() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1: got [$2], want [$3]"; fi; }
-eval "$(sed -n '/^rc_series() {/,/^}/p; /^rc_sessions_cmp() {/,/^}/p; /^rc_heartbeats_ok() {/,/^}/p' scripts/tier2/reconnect.sh)"
+eval "$(sed -n '/^rc_series() {/,/^}/p; /^rc_sessions_cmp() {/,/^}/p; /^rc_sessions_cmds() {/,/^}/p; /^rc_sessions_join() {/,/^}/p; /^rc_heartbeats_ok() {/,/^}/p' scripts/tier2/reconnect.sh)"
 
 # gw HANDSHAKE ONLINE PENDING_MEM: one gateway's reading (its Kick closes
 # = ONLINE); line MS SKEW GW0 GW1 [RQ] one sample (loadgen 10 online, the
@@ -49,6 +50,33 @@ expect "series: unreadable" "$(rc_series "$tmp/none.jsonl" 0 100 5)" null
   line 3000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 9000000; line 4000 10 "$(gw 0 1 0)" "$(gw 0 1 0)" 1000; } >"$tmp/q.jsonl"
 expect "series: run-queue gaps are null" "$(get "$(rc_series "$tmp/q.jsonl" 1000 100 5)" '[r[d["columns"].index("lg_runq_ms_per_s")] for r in d["rows"]]')" "[None, None, None]"
 
+# rc_sessions_cmds: 13 clients u-0..u-12. CLUSTER KEYSLOT sess:{u-12} is
+# 6674 (Redis 7.4.11), so with 6674 alone on redis-1 its file holds u-12's
+# EVAL only; every client is on exactly one line; a slot with no owner fails.
+mkdir -p "$tmp/sc"
+python3 -c 'import json; print(json.dumps({"uid_prefix": "u", "gw": [0] * 13, "gateways": ["gw-0"]}))' >"$tmp/sc/clients.json"
+printf '%s\n' "0 6673 redis-0" "6674 6674 redis-1" "6675 16383 redis-0" >"$tmp/sc/slots.txt"
+rc_sessions_cmds "$tmp/sc/clients.json" "$tmp/sc/slots.txt" "$tmp/sc" && ok "slots: commands written" || bad "slots: commands"
+expect "slots: u-12 alone on its slot's master" "$(cat "$tmp/sc/redis-1.idx" 2>/dev/null)" 12
+expect "slots: one EVAL of one key there" "$(awk '{ print $1, $(NF-1), $NF }' "$tmp/sc/redis-1.cmd" 2>/dev/null)" "EVAL 1 sess:{u-12}"
+expect "slots: each client once" "$(cat "$tmp/sc"/*.idx | sort -n | paste -sd, -)" "$(seq 0 12 | paste -sd, -)"
+expect "slots: each EVAL's key count" "$(awk '{ k = 0; for (i = 1; i <= NF; i++) if ($i ~ /^sess:[{]/) k++; if ($(NF - k) != k) bad++ } END { print bad + 0 }' "$tmp/sc"/*.cmd)" 0
+printf '%s\n' "0 100 redis-0" >"$tmp/sc/short.txt"
+rc_sessions_cmds "$tmp/sc/clients.json" "$tmp/sc/short.txt" "$tmp/sc/none" 2>/dev/null && bad "slots: a slot with no owner must fail" || ok "slots: a slot with no owner fails"
+
+# rc_sessions_join: each node's reply lines go back to their clients; a
+# reply short of its .idx fails.
+mkdir -p "$tmp/sj"
+cp "$tmp/sc/clients.json" "$tmp/sj/"
+printf '%s\n' 3 0 7 >"$tmp/sj/redis-0.idx"
+printf '%s\n' "1|c3" "1|c0" "1|c7" >"$tmp/sj/redis-0.out"
+for i in 1 2 4 5 6 8 9 10 11 12; do echo "$i"; done >"$tmp/sj/redis-1.idx"
+for i in 1 2 4 5 6 8 9 10 11 12; do echo "0|c$i"; done >"$tmp/sj/redis-1.out"
+expect "join: client order" "$(rc_sessions_join "$tmp/sj/clients.json" "$tmp/sj" | head -n 4 | paste -sd' ' -)" "1|c0 0|c1 0|c2 1|c3"
+expect "join: one line per client" "$(rc_sessions_join "$tmp/sj/clients.json" "$tmp/sj" | wc -l)" 13
+head -n 2 "$tmp/sj/redis-0.out" >"$tmp/sj/x" && mv "$tmp/sj/x" "$tmp/sj/redis-0.out"
+rc_sessions_join "$tmp/sj/clients.json" "$tmp/sj" >/dev/null 2>&1 && bad "join: a short reply must fail" || ok "join: a short reply fails"
+
 # rc_sessions_cmp: 7 clients on gw-0/gw-1, the mark at 1000: one good on
 # each gateway, one missing, one with two sessions, one on the wrong
 # gateway, one logged in before the mark, one offline at loadgen.
diff --git a/scripts/tier2/slowlogic.sh b/scripts/tier2/slowlogic.sh
index 72fac11..0955e03 100755
--- a/scripts/tier2/slowlogic.sh
+++ b/scripts/tier2/slowlogic.sh
@@ -284,7 +284,7 @@ want_max="$(python3 -c 'import sys; print(round(float(sys.argv[1]) * 100000))' "
 gomaxprocs=$(stack_metric logic go_sched_gomaxprocs_threads)
 wrong=""
 for g in gw-0 gw-1; do stack_pinned "$g" "$T2_CPUS_GATEWAY" || wrong+=" $g"; done
-for n in logic redis; do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
+for n in logic $(stack_redis_names); do stack_pinned "$n" "$T2_CPUS_LOGIC" || wrong+=" $n"; done
 scope=false
 [ "$cpu_max" = "$want_max" ] && [ -z "$wrong" ] && scope=true
 t2_check slow_scope "$scope" \
```

- [ ] **Step 2: The new tests fail without the change, then pass**

Run (the patch's tests against the old `stack.sh` and `reconnect.sh` first, then all):
```bash
git diff -- scripts/stack.sh scripts/tier2/reconnect.sh >"$S/sr.patch" && git apply -R "$S/sr.patch" &&
  { STACK_REDIS_SERVER=$PWD/.cache/redis/bin/redis-server scripts/stack_cluster_test.sh 2>&1 | grep -E '^FAIL|unbound'; scripts/tier2/reconnect_test.sh 2>&1 | grep -c '^FAIL'; }
git apply "$S/sr.patch"
make test-scripts 2>&1 | grep -E '^FAIL|failure'
```
Expected: against the old scripts, `stack_cluster_test.sh` prints `FAIL names: redis-0..5: got []…`, `FAIL up: STACK_REDIS=bogus: […]` and stops at `REDIS_MODE: unbound variable`; `reconnect_test.sh` counts 7 `FAIL` lines (no `rc_sessions_cmds`/`_join`). Then `make test-scripts` prints no `FAIL`, and `stack_cluster_test.sh` its `0 failure(s)` (30 `ok` lines, ~16s on the dev machine).

- [ ] **Step 3: The scenarios still parse and pin by name**

Run:
```bash
for f in scripts/stack.sh scripts/tier2/*.sh; do bash -n "$f" || echo "BAD $f"; done
grep -n 'for n in logic' scripts/tier2/*.sh
```
Expected: no `BAD`; four lines (`capacity`, `gwloss`, `reconnect`, `slowlogic`), each `for n in logic $(stack_redis_names); do stack_pinned …`.

- [ ] **Step 4: Commit**

```bash
git add scripts/stack.sh scripts/stack_cluster_test.sh scripts/tier2/capacity.sh scripts/tier2/gwloss.sh scripts/tier2/slowlogic.sh scripts/tier2/reconnect.sh scripts/tier2/reconnect_test.sh Makefile
git commit -m "feat: run the Tier 2 stack on a Redis Cluster" -m "STACK_REDIS=cluster starts 3 masters and 3 replicas on the Logic CPUs,
and Logic uses a cluster client. The Redis helpers read every node.
The session check of reconnect sends one EVAL per slot to the master
of that slot. RECONNECT_REDIS=cluster runs reconnect on the Cluster." -m "Refs: P6b3c2I-3, #66, #138 (Plan 6c1 Task 2)."
```

---

### Task 3: The `store` scenario on the Cluster, runs 32 and 33, the record

**Files:**
- Modify: `scripts/tier2/store.sh`
- Modify (record): this plan's *Execution Notes*, `docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md`

**Interfaces:**
- Consumes: Task 1's `scripts/redis-cluster-local.sh`, `TestClusterHasThreeMastersWithReplicas`.
- Produces: `store` checks `cluster_tests`, `cluster_no_skips`, `cluster_mutation`, `cluster_eviction`; field `redis_cluster` (the pinned server's version).

- [ ] **Step 1: The patch**

Apply from the repository root with `git apply` (as in Task 1):

```diff
diff --git a/scripts/tier2/store.sh b/scripts/tier2/store.sh
index bfcc195..5dd7341 100755
--- a/scripts/tier2/store.sh
+++ b/scripts/tier2/store.sh
@@ -1,5 +1,6 @@
 #!/usr/bin/env bash
-# Tier 2: run Plan 1's store tests against a real Redis primary + replica.
+# Tier 2: run every test against a real Redis primary + replica, then
+# against a Redis Cluster (P5bI-4, Plan 6c1).
 #
 # Usage (on the Tier 2 host, from the repo root):
 #   GOIM_TEST_TIER=2 make test-tier2 SCENARIO=store
@@ -14,6 +15,11 @@
 #   3. mutation: with Revoke's WAIT moved to a pooled connection,
 #                TestRevokeFailsWhenReplicaDoesNotAck fails (the test catches the bug)
 #   4. eviction: the test Redis reports maxmemory-policy noeviction
+#   5-8. cluster_tests, cluster_no_skips, cluster_mutation, cluster_eviction:
+#        1-4 again on a Redis Cluster of the pinned 7.4.11 (make redis-server;
+#        scripts/redis-cluster-local.sh: 3 masters, 3 replicas, no Docker);
+#        cluster_no_skips also needs TestClusterHasThreeMastersWithReplicas
+#        passed, so the suite really ran on a Cluster
 set -uo pipefail
 
 . "$(dirname "$0")/lib.sh"
@@ -42,6 +48,12 @@ t2_report_extra() {
   fi
   t2_section "mutation.log"
   tail -n 200 "$out/mutation.log" 2>/dev/null
+  t2_section "tests-cluster.log (RUN/PASS lines removed)"
+  if [ -f "$out/tests-cluster.log" ]; then
+    grep -vE '^[[:space:]]*(=== (RUN|PAUSE|CONT)|--- PASS)' "$out/tests-cluster.log" | head -n 3000
+  fi
+  t2_section "mutation-cluster.log"
+  tail -n 200 "$out/mutation-cluster.log" 2>/dev/null
   t2_section "redis logs (last 100 lines each)"
   cat "$out/redis.log" 2>/dev/null
 }
@@ -76,6 +88,22 @@ skips=$(grep -c 'TODO: Tier 2 integration' "$out/tests.log")
 failed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)
 tests_s=$((SECONDS - t))
 
+# mutant_run LOG [WRAPPER...]: the mutant's WAIT test, run in $mutdir under
+# WRAPPER (a command that runs the rest, e.g. the Cluster script), appended
+# to LOG; prints the verdict.
+mutant_run() {
+  local log=$1
+  shift
+  if (cd "$mutdir" && "$@" go test -race -count=1 -v -run '^TestRevokeFailsWhenReplicaDoesNotAck$' ./internal/store/) >>"$log" 2>&1; then
+    echo "fail: test still passes with WAIT on a pooled connection"
+  elif grep -q -- '--- FAIL: TestRevokeFailsWhenReplicaDoesNotAck' "$log" &&
+       grep -q 'want ErrNotReplicated' "$log"; then
+    echo pass
+  else
+    echo "error: the test failed for another reason, see $(basename "$log")"
+  fi
+}
+
 # 3: mutation check in a throwaway worktree, so the checkout is never modified.
 # The mutant sends WAIT through node (the pooled *redis.Client) instead of the
 # Conn that wrote the marker. It counts as caught only if it compiles and the
@@ -88,13 +116,8 @@ if ! grep -q 'node.Wait(ctx, 1' "$mutdir/internal/store/revoke.go"; then
   mutation="error: could not apply the mutation"
 elif ! (cd "$mutdir" && go vet ./internal/store/) >"$out/mutation.log" 2>&1; then
   mutation="error: the mutant does not compile"
-elif (cd "$mutdir" && go test -race -count=1 -v -run '^TestRevokeFailsWhenReplicaDoesNotAck$' ./internal/store/) >>"$out/mutation.log" 2>&1; then
-  mutation="fail: test still passes with WAIT on a pooled connection"
-elif grep -q -- '--- FAIL: TestRevokeFailsWhenReplicaDoesNotAck' "$out/mutation.log" &&
-     grep -q 'want ErrNotReplicated' "$out/mutation.log"; then
-  mutation="pass"
 else
-  mutation="error: the test failed for another reason, see mutation.log"
+  mutation=$(mutant_run "$out/mutation.log")
 fi
 mutation_s=$((SECONDS - t))
 
@@ -107,6 +130,33 @@ t2_check tests "$(t2_bool [ "$tests_rc" -eq 0 ])" "\"failed\": $(t2_json_str "$f
 t2_check no_skips "$(t2_bool [ "$skips" -eq 0 ])" "\"tier2_skips\": $skips"
 t2_check mutation "$(t2_bool [ "$mutation" = pass ])" "\"detail\": $(t2_json_str "$mutation"), \"duration_s\": $mutation_s"
 t2_check eviction "$(t2_bool [ "$policy" = noeviction ])" "\"maxmemory_policy\": $(t2_json_str "$policy")"
-t2_not_covered "Redis Cluster: MasterForKey, MOVED after failover, multi-node NOSCRIPT, CRC16 key slots" \
+# 5-8: the same on a Redis Cluster (P5bI-4). The mutant reuses the worktree.
+t=$SECONDS
+server=$PWD/.cache/redis/bin/redis-server
+cluster=$PWD/scripts/redis-cluster-local.sh
+if ! make redis-server >"$out/redis-build.log" 2>&1; then
+  ctests_rc=2 cskips=0 cfailed="make redis-server failed, see redis-build.log" cmutation="error: no redis-server" cpolicy=""
+else
+  "$cluster" "$server" go test -race -count=1 -v ./... >"$out/tests-cluster.log" 2>&1
+  ctests_rc=$?
+  cskips=$(grep -c 'TODO: Tier 2 integration' "$out/tests-cluster.log")
+  cfailed=$(grep -E '^[[:space:]]*--- FAIL' "$out/tests-cluster.log" | sed 's/^[[:space:]]*--- FAIL: //; s/ (.*//' | paste -sd, -)
+  if [ "$mutation" = "error: could not apply the mutation" ] || [ "$mutation" = "error: the mutant does not compile" ]; then
+    cmutation=$mutation
+  else
+    cmutation=$(mutant_run "$out/mutation-cluster.log" "$cluster" "$server")
+  fi
+  # Every node's policy, one line each.
+  cpolicy=$("$cluster" "$server" bash -c 'for a in ${GOIM_TEST_REDIS_CLUSTER//,/ }; do "$0" -p "${a#*:}" config get maxmemory-policy | tail -1; done' \
+    "$(dirname "$server")/redis-cli" 2>/dev/null | grep -v '^redis-cluster-local:' | sort | uniq -c | awk '{ print $2 "=" $1 }' | paste -sd, -)
+fi
+cluster_s=$((SECONDS - t))
+cran=$(grep -c -- '--- PASS: TestClusterHasThreeMastersWithReplicas' "$out/tests-cluster.log" 2>/dev/null)
+t2_field redis_cluster "$(t2_json_str "$("$server" --version 2>/dev/null)")"
+t2_check cluster_tests "$(t2_bool [ "$ctests_rc" -eq 0 ])" "\"failed\": $(t2_json_str "$cfailed"), \"duration_s\": $cluster_s"
+t2_check cluster_no_skips "$(t2_bool [ "$cskips" -eq 0 ] && [ "${cran:-0}" -eq 1 ])" "\"tier2_skips\": $cskips, \"cluster_test_passed\": ${cran:-0}"
+t2_check cluster_mutation "$(t2_bool [ "$cmutation" = pass ])" "\"detail\": $(t2_json_str "$cmutation")"
+t2_check cluster_eviction "$(t2_bool [ "$cpolicy" = noeviction=6 ])" "\"maxmemory_policy\": $(t2_json_str "$cpolicy")"
+t2_not_covered "Redis Cluster failover: MOVED and NOSCRIPT on a new master, the lease across it (Plan 6c2)" \
   "lost replies after a write (needs a fault-injecting proxy)"
 t2_finish
```

- [ ] **Step 2: The Cluster part runs on the dev machine**

`store.sh` needs Docker and the host guard, so run its Cluster part alone (a scratch script; `$S` is the session scratchpad):
```bash
S=<scratchpad>; mkdir -p "$S/storec"; cat >"$S/storec.sh" <<'SH'
set -uo pipefail
out=$1
eval "$(sed -n '/^mutant_run() {/,/^}/p' scripts/tier2/store.sh)"
mutdir=$(mktemp -d)
git worktree add --detach "$mutdir" HEAD >/dev/null 2>&1
sed -i 's/conn\.Wait(ctx, 1, s\.cfg\.WaitTimeout)/node.Wait(ctx, 1, s.cfg.WaitTimeout)/' "$mutdir/internal/store/revoke.go"
server=$PWD/.cache/redis/bin/redis-server cluster=$PWD/scripts/redis-cluster-local.sh
echo "mutation: $(mutant_run "$out/mutation-cluster.log" "$cluster" "$server")"
echo "policy: $("$cluster" "$server" bash -c 'for a in ${GOIM_TEST_REDIS_CLUSTER//,/ }; do "$0" -p "${a#*:}" config get maxmemory-policy | tail -1; done' "$(dirname "$server")/redis-cli" 2>/dev/null | grep -v '^redis-cluster-local:' | sort | uniq -c | awk '{ print $2 "=" $1 }' | paste -sd, -)"
echo "unmutated: $(mutdir=$PWD; mutant_run "$out/unmut.log" "$cluster" "$server")"
git worktree remove --force "$mutdir"
SH
git commit -qam wip-6c1-t3 && bash "$S/storec.sh" "$S/storec"; git reset -q --soft HEAD~1
bash -n scripts/tier2/store.sh && echo parses
```
Expected: `mutation: pass`, `policy: noeviction=6`, `unmutated: fail: test still passes with WAIT on a pooled connection` (the real code passes the test: the reading for an unmutated tree), `parses`. The temporary commit gives the mutant's worktree the patched tree; the soft reset undoes it.

- [ ] **Step 3: Commit, push**

```bash
git add scripts/tier2/store.sh
git commit -m "feat: run the store scenario on a Redis Cluster too" -m "Checks 5 to 8 run the suite, the WAIT mutant and the eviction policy
again on a Cluster of the pinned redis-server, with no Docker.
cluster_no_skips also needs the Cluster's own test passed." -m "Refs: P5bI-4, #138 (Plan 6c1 Task 3)."
git push -u origin feat/plan6c
```

- [ ] **Step 4 (the user, on the Tier 2 host): run 32, `store`**

```bash
git pull && GOIM_TEST_TIER=2 make test-tier2 SCENARIO=store
```
Expected: 8/8 checks (`tests`, `no_skips`, `mutation`, `eviction`, then the four `cluster_*`). About 10 minutes. A `cluster_tests` failure is the reading P5bI-4 asks for: name the test from `tests-cluster.log` in the report.

- [ ] **Step 5 (the user, on the Tier 2 host): run 33, `reconnect` `reset` on the Cluster**

```bash
GOIM_TEST_TIER=2 RECONNECT_REDIS=cluster RECONNECT_PHASES=reset make test-tier2 SCENARIO=reconnect
```
Expected: run 31's `reset` checks, now with Logic and the 6 nodes on Logic's 2 CPUs (11, 23). `P_sessions` reads every client per slot (~16k EVALs); `P.redis_latency` is per node (`evalsha` on the 3 masters). No check is new. A failure from the shared set (Logic and 6 nodes on 2 CPUs) is a reading for Plan 6c3's sizing, not a defect of this plan; record it with `logic_set_busy_pct` and each node's `cpu_s` (the phase's `ramp`/`storm` usage has one entry per node). About 10 minutes.

- [ ] **Step 6: Record**

In this plan's *Execution Notes*: the commits; runs 32 and 33 (commit, duration, checks; run 33 against run 31's `reset`: p99 from the mark, all back, backoff share; per node `evalsha` p99/p99.9 against run 31's single-instance `evalsha` p99 75–94µs, p99.9 2.6–3.1ms; the session check's time). In the roadmap: the plan 6 row (6c1 executed, runs 32/33). `README.md` only if run 33 gives a row the user wants.

```bash
git add docs/superpowers/plans/2026-10-07-connection-session-core-00-roadmap.md docs/superpowers/plans/2026-10-24-connection-session-core-06c1-redis-cluster.md
git commit -m "docs: record Plan 6c1 and runs 32 and 33" -m "Refs: P5bI-4, #138 (Plan 6c1 Task 3)."
```

Push the branch. Fast-forwarding `main` is the user's call.

---

## After this plan

- **Plan 6c2** (failover, R2-O5, R5S-O1, R5S-O9): fail over the lease keys' shard (`CLUSTER FAILOVER` and a SIGKILL of a master) and measure the time against the lease TTL; logins on the other shards keep their p99; zone loss (one master and one replica, `redis/layout`). Inputs: Review Focus 3 (the slot map from a live node), #21 (the 15s failover default in the sweeper's ratio check), `REDIS_CLUSTER_NODE_TIMEOUT_MS`.
- **Plan 6c3**: `gwconns` shard load (a triggered Reconcile pass at the cap, the `gwconns` shard's p99 ≤ 2× the others', R4M-9), Reconcile's `not_owner` Kicks as a reading (run 31: 4,119 on gw-0), and the ReconcileEnd and drain deadlines against `MaxConnectionAgeGrace`/`ShutdownGrace` (30s: ~280k `gwconns` members at 10k/s; Plan 2's deferred minor).
- Then **6d** (Tier 3, k8s).

## Execution Notes

**Tasks 1–3 (code) executed 2026-10-24** (inline, superpowers:executing-plans) on `feat/plan6c` from `d4a7a08`: `ed9932c` (Task 1), `48ac535` (Task 2), `3c1db43` (Task 3, `store.sh`). The code is byte-identical to `proto-6c1`.
- Task 1: the old `testredis` fails vet (`undefined: testredis.Cluster`); on the local Cluster every package passes under `-race` with 0 "TODO: Tier 2" skips; `make test`, `go vet ./...`, `make test-redis-local` green; the four named tests pass, `TestMasterAddrOnSingleNodeIsOneGroup` skips.
- Task 2: on the old scripts `stack_cluster_test.sh` fails `ports`, `names`, `up: STACK_REDIS=bogus` and stops at `REDIS_MODE: unbound variable`; `reconnect_test.sh` 7 `FAIL`. Then `make test-scripts` green; `stack_cluster_test.sh` has 29 checks and the `0 failure(s)` line (the plan's "30 ok lines" counts that line). The 4 scenarios pin by `stack_redis_names`.
- Task 3: the Cluster part alone: `mutation: pass`, `policy: noeviction=6`, `unmutated: fail: …` (the reading for an unmutated tree), `parses`.

**Final review (fresh Opus reviewer, 2026-10-24):** 0 Critical, 0 Important, 7 Minor. All 7 fixed on the user's request 2026-10-25 (`898795c`, `66150d5`); each fix but M7 has a check that failed first:
- M1: `stack_redis_cluster_up` kept an old `nodes.conf` in a reused `STACK_DIR`, so `--cluster create` refused the node. Now each node directory is new (check `up again`).
- M2: `stack_redis_slots` dropped a master only suspected down (`fail?`). Now only `fail` drops it (check with a fixed `CLUSTER NODES`). Plan 6c2 must still read the map from a live node (Review Focus 3).
- M3: `TestMasterAddrOnClusterIsTheOwner` compared `MasterForKey` with itself. Now the owner comes from `CLUSTER SHARDS` and the 3 keys need 3 masters (a mutant on both lookups: old test passed, new failed).
- M4: "sess keys on every master" counted `DBSIZE` (lease, `gwconns`, `gwepochs` keys too). Now it scans `sess:*` (a mutant with `redis-1`'s sess keys deleted, `gwepochs` left: old passed, new failed).
- M5: the session check's time was not recorded. `rc_sessions` now adds `"ms"` to its JSON (so to `P_sessions`'s detail and the phase field).
- M6: the join's limit was 300 passes (~45–60s on 2 cores), its message said 30s. Now a clock deadline, `REDIS_CLUSTER_JOIN_TIMEOUT_S` (default 30).
- M7: `dialCluster` waits 5s, not 2s (it also loads the slot map). No failing test is possible for a timeout value.
- Not judged by the reviewer (host data): the 23GiB budget with 6 nodes (replicas double the Redis memory), Logic and 6 nodes on 2 CPUs against run 31, the single-mode cost of ~16k one-slot EVALs, `store.sh`'s Docker part.

**Run 32 (2026-10-25, `3c1db43` (before the minor fixes), 147s, setup 8s): 8/8.** `tests` 55s, `no_skips` 0, `mutation` pass (11s), `eviction` `noeviction`; `cluster_tests` 73s, `cluster_no_skips` 0 (`cluster_test_passed` 1), `cluster_mutation` pass, `cluster_eviction` `noeviction=6`. The single run uses the Docker Redis (jemalloc), the Cluster the pinned build (libc malloc): same 7.4.11. Both mutant logs fail `TestRevokeFailsWhenReplicaDoesNotAck` with `want ErrNotReplicated`. P5bI-4 holds: every package passes on a Cluster. `not_covered`: the failover (Plan 6c2) and lost replies after a write.
