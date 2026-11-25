# Plan 6c2c: Stalled Shard Logins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the other shards' logins at their normal p99 while one Redis master is stopped or killed (#144, R5S-O1), by the remedy #145, and record the `failover` scenario again as run 35.

**Architecture:** Four tasks:
- **The store's group bounds** (Task 1, `internal/store`, `cmd/logic`). `runEach` takes one of `GroupWorkers` slots per master (Logic: `-workers`, 64) before it sends a group, then bounds the group by `GroupTimeout` (Logic: `-redis-read-timeout`, 1s). A stopped master then fails its group in one read timeout, and go-redis has no time to send a timed-out script again (6c2b final review M3). A stalled master uses only its own slots.
- **Logic's Login frees its worker before the store** (Task 2, `internal/logic`). The shared pool (`-workers`) bounds the token checks only; the store calls wait on their master's slots (Task 1).
- **The gateway's senders do not wait for replies** (Task 3, `internal/gateway`, `cmd/gateway`). A sender takes one token per entry from `-login-inflight` (default 3,200 = 16 × 200), then runs the batch's RPC on its own goroutine and goes on. Each resolved entry gives its token back. The gateway bounds the entries with no result, not the RPCs.
- **Dry run and run 35** (Task 4): the dev dry run of `failover`, then the user runs it on the Tier 2 host; the report is recorded.

**Tech Stack:** Go 1.27.1, go-redis v9.23.0, gRPC, the pinned `redis-server` 7.4.11 (`make redis-server`).

**Spec:** `docs/superpowers/specs/2026-10-03-connection-session-core-design-rev6.md`: *Verification* → Redis failover test (~l.535: the other shards are unaffected), *Storms, drain, failures* → Redis shard failover and Zone loss (Redis) (~l.419–420), *Login batching* (the per-entry result and timeout). Nothing here changes it; the spec's next revision names `-login-inflight` and the per-master bound beside #140 and #142.

Inputs:
- The roadmap's **#144** (the defect, run 34's numbers: zone other-shard p99 4,470ms against 21ms before; kill 104ms against a 71ms bound) and **#145** (the remedy, decided by the user 2026-11-25, and the prototype's record).
- Plan 6c2b (`docs/superpowers/plans/2026-11-25-connection-session-core-06c2b-failover-scenario.md`): the scenario, its dry-run recipe (Task 3 Step 5), run 34 and final review M3 (*Execution Notes*).

**Decisions (the user, 2026-11-25, with AskUserQuestion, before writing; #145):**
1. **Both sides change.** The gateway's Login worker returns once a batch is sent; the gateway bounds the entries with no result, not the RPCs. Logic bounds its Login concurrency per master. Not a fail-fast breaker in Logic alone (the gateway still waits ~1s per batch; the recovery rule can misjudge). Not the gateway alone (Logic's 64 shared workers would still fill at a higher login rate).
2. **The go-redis read-timeout resend is fixed here** (6c2b final review M3): a shard group waits at most one read timeout, and a timed-out script is not sent again. MOVED/ASK redirects stay.
3. **A prototype first**, then this plan from it, replayed byte for byte.

**Branch:** `feat/plan6c2c` (from `main` `599346a`; #145 is `5399bff`, the prototype's record `1100634`). Execute from its head, this plan's own commit or later.

**Size:** 4 tasks. Task 1: ~190 lines (store ~70, its test ~120, Logic's flag wiring and its test ~15). Task 2: ~35 lines. Task 3: ~70 lines. Task 4: the dry run, run 35 and the record. The prototype `proto-6c2c` (from `5399bff`; `e5ca13c`, `55ff6ce`, `7db34af`; pushed) ran every step; its diff is replayed here byte for byte, one `git apply` patch per task. On the dev machine (AMD EPYC 7742, 2 cores):
- `go vet ./...` clean; `make test` (race), `make test-redis-local`, `make test-redis-cluster-local`: every package `ok`.
- Each task's tests fail on the old code as Step 2 says; mutations as each Step 4 says.
- **Replayed 2026-11-25** in a fresh worktree from `1100634`: the three patches taken from this file, every step run as written (Step 2 failures, the 7 mutations, Task 3 Step 5's suites, 20 packages `ok` each); the code after Task 3 equals `proto-6c2c`'s (empty `git diff`). The replay's dry run (Task 4 Step 1): 30 checks, failing only the three swap checks; other-shard p99 before → window: manual 12 → 12, kill 14 → 13, zone 11 → 14ms; `master_ms` 40, 8,545, 8,562; `missing` 0 everywhere.
- The dry run (Task 4 Step 1, 3,000 clients, flow 20/s): 30 checks, only the three dev swap checks fail. Other-shard p99 in the failover window: kill 16ms (before 13), **zone 16ms (before 14; 6c2b's dry run 1,605ms)**. The target shard's logins all succeed (kill p99 9.2s, zone 12.1s; none gave up). Lease `missing` 0 everywhere. `master_ms`: kill 6,795, zone 7,277.

## Global Constraints

- `make test` (`go test -race -count=1 ./...`), `go vet ./...`, `make test-redis-local`, `make test-redis-cluster-local` and `make test-scripts` stay green on the dev machine (2 cores, ~1GB, no Docker, no root).
- Production changes only in `internal/store` (`runEach`), `internal/logic` (`Login`), `internal/gateway` (the Login sender, `Config`), `cmd/logic` and `cmd/gateway` (flags). No change to the wire, the SDK, any Lua script, the lease calls, Logout's or Reconcile's handlers, or any script under `scripts/`.
- No new Logic flag: `GroupTimeout` is `-redis-read-timeout` (1s, #140), `GroupWorkers` is `-workers` (64). One new gateway flag: `-login-inflight` (3,200; refused below `-login-batch`). `-login-rpcs` keeps its name and default (16); it now counts senders.
- A zero `GroupTimeout` or `GroupWorkers` in `store.Config` means no bound (the store's tests and other callers keep today's behaviour).
- MOVED/ASK redirects and the NOSCRIPT fallback (`runBatch`) stay as they are, inside the group's bound.
- Run 35 is the unchanged `failover` scenario (`scripts/tier2/failover.sh`) at its defaults, the CPU sets of run 34.

## Review Focus

1. **A healthy master whose whole group takes over 1s now fails the group.** `GroupTimeout` bounds one pipeline of up to `LoginBatch` (200) EVALSHAs plus any NOSCRIPT re-runs, not each command. Before, each command had its own 1s read timeout (#140). A master slowed past 1s under load fails every entry of the group (`error`; the client retries). Run 35's flow is 200 logins/s: Task 4 reads Logic's store errors in the before windows, where nothing is failing over.
2. **The kill case.** go-redis retries a refused dial with backoff; the group's 1s bound now stops that too, so a batch with a uid on the killed master holds nothing past 1s. Run 34's `kill_other_shards` failed (104ms against 71ms); the dev dry run passed (16 against 13) at 20 logins/s only. Run 35 at 200/s is the test.
3. **More RPCs in flight.** At a low login rate a batch holds 1 entry (5ms linger), so with a stalled shard up to 3,200 single-entry RPCs per gateway may be open at once (each now ends within ~1s). Logic's shared pool still queues their token checks; grpc-go sets no stream limit by default. Run 35's `zone` phase reads it (`P.logic` CPU, the gateways' `logins_expired`).
4. **Shutdown drops waiting batches.** A sender stopped while it waits for tokens returns without a reply to those entries and without `loginFinished` (the `login_queue` gauge). Only at shutdown, when the batcher leaves its queue unsent too.
5. **The slot map per group never shrinks.** `Store.slots` keeps one channel per master address ever seen (`""` on a single node); a failover adds one. 3–6 entries in a test Cluster.

Each line's test: 1, 2 and 3 are run 35's readings (Task 4 Step 3; no dev test loads a Cluster to 200 logins/s beside 600k clients); 4: none (shutdown only; the gateway's shutdown tests still pass); 5: none (bounded by the node count).

## Implementation choices this plan makes

These are **#146.1–#146.6** in the roadmap (proposed with this plan; decided by the user 2026-11-25, all as proposed, with AskUserQuestion; execution inline, superpowers:executing-plans, in a fresh session):
1. **`-login-inflight` defaults to 3,200** (`LoginRPCs` × `LoginBatch`, today's most entries in flight) and is refused below `-login-batch` (a full batch must fit) and at 0.
2. **`-login-rpcs` keeps its name** (default 16), now the number of senders; its help text says so. Not renamed: deployments and the Tier 2 scripts keep working.
3. **`GroupWorkers` is Logic's `-workers`** (64 per master), shared by Login and Logout groups to that master; no new flag.
4. **`GroupTimeout` is `-redis-read-timeout`** (1s) over the whole group, from its send to its last reply; no new flag. The wait for a slot is outside it: a busy healthy master queues its calls (bounded by the caller's deadline), it does not fail them.
5. **Only Login frees its shared worker early.** Logout keeps its worker for the whole call (its groups now end within 1s too); Reconcile and the lease calls do not use `runEach` and are unchanged.
6. **Run 35 is the whole `failover` scenario**, as run 34 (~20 minutes), expected 30/30.

## File Structure

- `internal/store/store.go`: `Config.GroupTimeout`, `Config.GroupWorkers`, `runEach`, `acquireGroup`, `groupCtx`, `failedCmd` (Task 1).
- `internal/store/group_test.go` (new): `silentRedis`, two tests (Task 1).
- `cmd/logic/main.go`, `main_test.go`: the store's group bounds from `-redis-read-timeout` and `-workers` (Task 1).
- `internal/logic/login.go`, `login_test.go`: Login releases its worker after the token checks (Task 2).
- `internal/gateway/config.go`, `config_test.go`, `gateway.go`, `login.go`, `login_test.go`, `cmd/gateway/main.go`: `LoginInflight`, `-login-inflight`, `sendLogins`/`runLogins` (Task 3).
- The roadmap (#146 new, the plan 6 row), this plan's *Execution Notes* (Tasks 1–4).

---

### Task 1: The store's group bounds

**Files:**
- Modify: `internal/store/store.go`, `cmd/logic/main.go`
- Create: `internal/store/group_test.go`
- Test: `internal/store/group_test.go`, `cmd/logic/main_test.go`

**Interfaces:**
- Produces (Go): `store.Config.GroupTimeout time.Duration` (0: none) and `store.Config.GroupWorkers int` (0: no bound); `DefaultConfig` leaves both 0.
- Produces (unexported): `(*Store).acquireGroup(ctx, g string) (release func(), err error)`, `(*Store).groupCtx(ctx) (context.Context, context.CancelFunc)`, `failedCmd(ctx, err) *redis.Cmd`. A group that gets no slot before ctx ends fails each of its calls with ctx's error.
- Produces (flags): Logic sets `cfg.Store.GroupTimeout = -redis-read-timeout`, `cfg.Store.GroupWorkers = -workers` in `parseFlags`; `-workers`' help names the per-master bound.
- Consumed by: Task 2 (its test does not need it; production does).

- [ ] **Step 1: The tests**

Save the patch below to a file in the session scratchpad (`PATCH`) and apply only its test files from the repository root: `git apply --include='*_test.go' PATCH`.

```diff
diff --git a/cmd/logic/main.go b/cmd/logic/main.go
index 5e663b6..80e83cc 100644
--- a/cmd/logic/main.go
+++ b/cmd/logic/main.go
@@ -64,7 +64,7 @@ func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error
 	fs.StringVar(&sc.GRPCAddr, "grpc-addr", sc.GRPCAddr, "Logic and Revocation gRPC listen address")
 	fs.StringVar(&sc.LeaseAddr, "lease-addr", sc.LeaseAddr, "Lease gRPC listen address (dedicated, R1-O2)")
 	fs.StringVar(&sc.AdminAddr, "admin-addr", sc.AdminAddr, "/metrics, /healthz, /readyz, /debug/pprof/ listen address")
-	fs.IntVar(&sc.Workers, "workers", sc.Workers, "concurrent Login/Logout/Reconcile calls")
+	fs.IntVar(&sc.Workers, "workers", sc.Workers, "concurrent Login/Logout/Reconcile calls, and Login/Logout script groups in flight per Redis master")
 	fs.IntVar(&sc.LeaseWorkers, "lease-workers", sc.LeaseWorkers, "concurrent lease calls")
 	fs.DurationVar(&sc.MaxConnectionAge, "max-conn-age", sc.MaxConnectionAge, "gRPC MaxConnectionAge")
 	fs.DurationVar(&sc.MaxConnectionAgeGrace, "max-conn-age-grace", sc.MaxConnectionAgeGrace, "gRPC MaxConnectionAgeGrace")
@@ -110,6 +110,9 @@ func parseFlags(args []string) (logic.Config, logic.ServerConfig, options, error
 	if o.redisReadTimeout <= 0 { // go-redis reads 0 as its 5s default, -1 as none
 		return cfg, sc, o, errors.New("config: -redis-read-timeout must be positive")
 	}
+	// One master's group of Login/Logout scripts waits at most one read
+	// timeout, with -workers slots per master (#145).
+	cfg.Store.GroupTimeout, cfg.Store.GroupWorkers = o.redisReadTimeout, sc.Workers
 	return cfg, sc, o, nil
 }
 
diff --git a/cmd/logic/main_test.go b/cmd/logic/main_test.go
index 755dcc9..5390774 100644
--- a/cmd/logic/main_test.go
+++ b/cmd/logic/main_test.go
@@ -55,6 +55,18 @@ func TestRedisClientTimeouts(t *testing.T) {
 	}
 }
 
+// The store bounds one master's Login/Logout group by the read timeout,
+// with -workers slots per master (#145).
+func TestStoreGroupFlags(t *testing.T) {
+	cfg, _, _, err := parseFlags([]string{"-jwks-url", "x", "-workers", "5", "-redis-read-timeout", "700ms"})
+	if err != nil {
+		t.Fatal(err)
+	}
+	if cfg.Store.GroupWorkers != 5 || cfg.Store.GroupTimeout != 700*time.Millisecond {
+		t.Fatalf("store groups: workers %d timeout %v", cfg.Store.GroupWorkers, cfg.Store.GroupTimeout)
+	}
+}
+
 // The test timing profile (Plan 5) scales intervals ~10× and keeps the ratios.
 func TestScaledProfileFlags(t *testing.T) {
 	_, _, _, err := parseFlags([]string{"-jwks-url", "x", "-lease-ttl", "3s", "-renew-interval", "1s",
diff --git a/internal/store/group_test.go b/internal/store/group_test.go
new file mode 100644
index 0000000..98cd545
--- /dev/null
+++ b/internal/store/group_test.go
@@ -0,0 +1,117 @@
+package store
+
+import (
+	"bufio"
+	"context"
+	"net"
+	"strings"
+	"sync"
+	"sync/atomic"
+	"testing"
+	"time"
+
+	"github.com/Tallinzp/goim/internal/testredis"
+	"github.com/redis/go-redis/v9"
+)
+
+// silentRedis accepts connections, refuses HELLO, then reads commands and
+// never replies: a stopped (SIGSTOPped) master. It counts the EVALSHA commands it receives.
+func silentRedis(t *testing.T) (addr string, evals *atomic.Int64) {
+	t.Helper()
+	lis, err := net.Listen("tcp", "127.0.0.1:0")
+	if err != nil {
+		t.Fatal(err)
+	}
+	t.Cleanup(func() { lis.Close() })
+	evals = &atomic.Int64{}
+	go func() {
+		for {
+			c, err := lis.Accept()
+			if err != nil {
+				return
+			}
+			t.Cleanup(func() { c.Close() })
+			go func() {
+				r := bufio.NewScanner(c)
+				for r.Scan() {
+					switch strings.ToLower(r.Text()) {
+					case "hello": // the handshake: refused, so the client goes on with RESP2
+						_, _ = c.Write([]byte("-ERR unknown command\r\n"))
+					case "evalsha":
+						evals.Add(1)
+					}
+				}
+			}()
+		}
+	}()
+	return lis.Addr().String(), evals
+}
+
+// #145 (6c2b final review M3): a group to a master that never answers fails
+// in GroupTimeout, and its script is sent once: go-redis has no time to send
+// it again.
+func TestRunEachStalledGroupFailsInGroupTimeoutAndSendsOnce(t *testing.T) {
+	addr, evals := silentRedis(t)
+	const timeout = 200 * time.Millisecond
+	rdb := redis.NewClient(&redis.Options{Addr: addr, ReadTimeout: timeout, WriteTimeout: timeout,
+		DisableIdentity: true})
+	t.Cleanup(func() { rdb.Close() })
+	cfg := DefaultConfig()
+	cfg.GroupTimeout = timeout
+	s := New(rdb, cfg)
+	start := time.Now()
+	res := s.Login(ctx, []LoginReq{{UID: testredis.UID(), Field: field, Entry: entry(1, "d", 1, time.Now().UnixMilli())}})
+	if res[0].Err == nil {
+		t.Fatalf("a silent master gave %+v, want an error", res[0])
+	}
+	if d := time.Since(start); d > 2*timeout {
+		t.Fatalf("the group failed after %v, want about %v", d, timeout)
+	}
+	time.Sleep(timeout) // a late resend would arrive now
+	if n := evals.Load(); n != 1 {
+		t.Fatalf("EVALSHA sent %d times, want once", n)
+	}
+}
+
+// #145: GroupWorkers bounds the groups in flight to one master; another
+// master's group does not wait for them.
+func TestRunEachGroupWorkersBoundOneMasterOnly(t *testing.T) {
+	slow := testredis.UID()
+	s, release := gatedStore(t, slow)
+	s.cfg.GroupWorkers = 1
+	s.groupOf = func(_ context.Context, key string) string {
+		if key == sessKey(slow) || strings.HasPrefix(key, "sess:{same") {
+			return "stalled"
+		}
+		return key
+	}
+	now := nowMs(t, s)
+	var wg sync.WaitGroup
+	wg.Add(1)
+	go func() { // holds the stalled master's only slot until release
+		defer wg.Done()
+		s.Login(ctx, []LoginReq{{UID: slow, Field: field, Entry: entry(1, "d", 1, now)}})
+	}()
+	time.Sleep(100 * time.Millisecond)
+	queued := make(chan LoginResult, 1)
+	go func() {
+		queued <- s.Login(ctx, []LoginReq{{UID: "same" + testredis.UID(), Field: field, Entry: entry(2, "d", 1, now)}})[0]
+	}()
+	start := time.Now()
+	if r := s.Login(ctx, []LoginReq{{UID: testredis.UID(), Field: field, Entry: entry(3, "d", 1, now)}})[0]; r.Err != nil || r.Code != LoginWritten {
+		t.Fatalf("other master: %+v", r)
+	}
+	if d := time.Since(start); d > time.Second {
+		t.Fatalf("the other master's group waited %v", d)
+	}
+	select {
+	case r := <-queued:
+		t.Fatalf("the stalled master's second group ran while its slot was held: %+v", r)
+	case <-time.After(200 * time.Millisecond):
+	}
+	close(release)
+	wg.Wait()
+	if r := <-queued; r.Err != nil || r.Code != LoginWritten {
+		t.Fatalf("queued group after release: %+v", r)
+	}
+}
diff --git a/internal/store/store.go b/internal/store/store.go
index 702757e..3d504e2 100644
--- a/internal/store/store.go
+++ b/internal/store/store.go
@@ -18,6 +18,13 @@ type Config struct {
 	SessTTL       time.Duration // sess:{uid} TTL, also used for rev:{uid} (R4M-1); default 30d
 	LeaseTTL      time.Duration // gwlease TTL (default 30s)
 	WaitTimeout   time.Duration // Revoke's WAIT 1 <timeout> (default 1s)
+	// GroupTimeout bounds one master's group in runEach, from its send to
+	// its last reply, so a stopped master fails its calls in one read
+	// timeout and go-redis has no time to send them again (#145). 0: none.
+	GroupTimeout time.Duration
+	// GroupWorkers bounds the groups in flight to one master, so a stalled
+	// master uses only its own share (#145). 0: no bound.
+	GroupWorkers int
 }
 
 // DefaultConfig returns the spec defaults.
@@ -48,6 +55,9 @@ type Store struct {
 	masterFor   func(ctx context.Context, key string) (*redis.Client, error)
 	reloadSlots func(ctx context.Context)
 	rerouteWait time.Duration // how long Revoke waits for a new master
+
+	slotsMu sync.Mutex
+	slots   map[string]chan struct{} // per group, GroupWorkers each
 }
 
 // rerouteWait's default, and how often the slot map is re-read meanwhile.
@@ -136,22 +146,72 @@ func (s *Store) runEach(ctx context.Context, sc *redis.Script, calls []scriptCal
 		groups[g] = append(groups[g], j)
 	}
 	var wg sync.WaitGroup
-	for _, js := range groups {
+	for g, js := range groups {
 		wg.Add(1)
-		go func(js []int) {
+		go func(g string, js []int) {
 			defer wg.Done()
 			sub := make([]scriptCall, len(js))
 			for k, j := range js {
 				sub[k] = calls[j]
 			}
-			for k, cmd := range s.runBatch(ctx, sc, sub) {
+			release, err := s.acquireGroup(ctx, g)
+			if err != nil {
+				for _, j := range js {
+					done(j, failedCmd(ctx, err))
+				}
+				return
+			}
+			defer release()
+			gctx, cancel := s.groupCtx(ctx)
+			defer cancel()
+			for k, cmd := range s.runBatch(gctx, sc, sub) {
 				done(js[k], cmd)
 			}
-		}(js)
+		}(g, js)
 	}
 	wg.Wait()
 }
 
+// acquireGroup takes one of group g's GroupWorkers slots, waiting until ctx
+// ends. The wait is outside GroupTimeout: a busy healthy master queues its
+// calls, it does not fail them.
+func (s *Store) acquireGroup(ctx context.Context, g string) (release func(), err error) {
+	if s.cfg.GroupWorkers <= 0 {
+		return func() {}, nil
+	}
+	s.slotsMu.Lock()
+	if s.slots == nil {
+		s.slots = map[string]chan struct{}{}
+	}
+	ch, ok := s.slots[g]
+	if !ok {
+		ch = make(chan struct{}, s.cfg.GroupWorkers)
+		s.slots[g] = ch
+	}
+	s.slotsMu.Unlock()
+	select {
+	case ch <- struct{}{}:
+		return func() { <-ch }, nil
+	case <-ctx.Done():
+		return nil, ctx.Err()
+	}
+}
+
+// groupCtx is ctx bounded by GroupTimeout, when set.
+func (s *Store) groupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
+	if s.cfg.GroupTimeout <= 0 {
+		return ctx, func() {}
+	}
+	return context.WithTimeout(ctx, s.cfg.GroupTimeout)
+}
+
+// failedCmd is a script call that was never sent, failed with err.
+func failedCmd(ctx context.Context, err error) *redis.Cmd {
+	cmd := redis.NewCmd(ctx)
+	cmd.SetErr(err)
+	return cmd
+}
+
 // nodeFor returns the client for the master that owns key, so commands that
 // must share one connection (a write followed by WAIT) can use Conn().
 func (s *Store) nodeFor(ctx context.Context, key string) (*redis.Client, error) {
```

- [ ] **Step 2: Run the tests on the old code**

Run: `go vet ./internal/store/ ./cmd/logic/`
Expected: type-check errors, `internal/store/group_test.go:60:6: cfg.GroupTimeout undefined` and `cmd/logic/main_test.go:65:15: cfg.Store.GroupWorkers undefined`.

- [ ] **Step 3: The code**

Apply the rest of the patch: `git apply --exclude='*_test.go' PATCH`.

Run: `go vet ./internal/store/ ./cmd/logic/ && go test -count=1 ./internal/store/ ./cmd/logic/`
Expected: `ok` for both.

- [ ] **Step 4: Mutations (undo each)**

1. In `groupCtx`, change `if s.cfg.GroupTimeout <= 0 {` to `if true {`. Run `go test -count=1 -run TestRunEach ./internal/store/`. Expected: `TestRunEachStalledGroupFailsInGroupTimeoutAndSendsOnce` fails with `the group failed after ~900ms, want about 200ms` (go-redis sent it 4 times, 200ms each).
2. In `acquireGroup`, add `g = ""` before `ch, ok := s.slots[g]`. Same run. Expected: `TestRunEachGroupWorkersBoundOneMasterOnly` fails with `the other master's group waited ~1.9s`.
3. In `cmd/logic/main.go`, delete the line `cfg.Store.GroupTimeout, cfg.Store.GroupWorkers = o.redisReadTimeout, sc.Workers`. Run `go test -count=1 -run TestStoreGroupFlags ./cmd/logic/`. Expected: `FAIL` (`store groups: workers 0 timeout 0s`).

Then `git diff --stat` shows only this task's files, with no mutation left.

- [ ] **Step 5: Commit**

```bash
git add internal/store/store.go internal/store/group_test.go cmd/logic/main.go cmd/logic/main_test.go
git commit -m "fix: bound each Redis master's login groups" # body: STE, the timeout and the slots per master; refs #145 and M3 on their own line
```

### Task 2: Logic's Login frees its worker before the store

**Files:**
- Modify: `internal/logic/login.go`
- Test: `internal/logic/login_test.go`

**Interfaces:**
- Consumes: the test's `gatedLogin` and `newEnvWith` (already in `internal/logic`); the service's test config has 8 workers.
- Produces: `(*Service).Login` calls the pool's `release()` right after `s.m.LoginPrepare.Observe`, not on return.

- [ ] **Step 1: The test**

Save the patch below as `PATCH` and apply only its test file: `git apply --include='*_test.go' PATCH`.

```diff
diff --git a/internal/logic/login.go b/internal/logic/login.go
index 78bdc5a..1236197 100644
--- a/internal/logic/login.go
+++ b/internal/logic/login.go
@@ -23,11 +23,13 @@ func (s *Service) Login(b *pb.LoginBatch, stream grpc.ServerStreamingServer[pb.L
 	if err != nil {
 		return invalid("%v", err)
 	}
+	// The shared pool bounds the token checks only. The store calls take a
+	// slot of their master's own pool (store GroupWorkers), so a stalled
+	// shard holds no slot the other shards need (#145).
 	release, err := s.pool.acquire(ctx)
 	if err != nil {
 		return err
 	}
-	defer release()
 
 	var mu sync.Mutex // Send is not safe for concurrent use; LoginEach's done is concurrent
 	var sendErr error
@@ -58,6 +60,7 @@ func (s *Service) Login(b *pb.LoginBatch, stream grpc.ServerStreamingServer[pb.L
 	// #115): the batch's token checks, then each entry's age from
 	// the gateway's recv_ms as the batch goes to the store.
 	s.m.LoginPrepare.Observe(time.Since(prepStart).Seconds())
+	release()
 	nowMs := s.now().UnixMilli()
 	for _, r := range reqs {
 		s.m.LoginAge.Observe(float64(nowMs-r.Entry.LoginMs) / 1000)
diff --git a/internal/logic/login_test.go b/internal/logic/login_test.go
index 3a7c494..1aab6a8 100644
--- a/internal/logic/login_test.go
+++ b/internal/logic/login_test.go
@@ -424,3 +424,31 @@ func TestLoginObservesEntryAgeAndPrepare(t *testing.T) {
 		t.Fatalf("login_prepare_seconds count %d, want 1 (one per batch)", n)
 	}
 }
+
+// #145: Login gives its shared worker back before the store calls, so logins
+// held on a stalled shard do not keep a login on another shard waiting.
+func TestLoginFreesItsWorkerBeforeTheStore(t *testing.T) {
+	slow := testredis.UID()
+	gate := &gatedLogin{slowUID: slow, release: make(chan struct{})}
+	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { gate.Store = s; return gate })
+	c := e.dial()
+	for i := range 9 { // one more than the service's 8 workers
+		if _, err := c.Login(ctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: []*pb.LoginEntry{e.loginEntry(slow, uint64(i+1), "d", uint64(i+1))}}); err != nil {
+			t.Fatal(err)
+		}
+	}
+	time.Sleep(100 * time.Millisecond) // the nine reach the store
+	start := time.Now()
+	stream, err := c.Login(ctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: []*pb.LoginEntry{e.loginEntry(testredis.UID(), 20, "d", 1)}})
+	if err != nil {
+		t.Fatal(err)
+	}
+	r, err := stream.Recv()
+	if err != nil || r.Code != pb.LoginCode_LOGIN_CODE_OK {
+		t.Fatalf("fast login: %+v %v", r, err)
+	}
+	if d := time.Since(start); d > time.Second {
+		t.Fatalf("fast login took %v: it waited for a worker the stalled ones held", d)
+	}
+	close(gate.release)
+}
```

- [ ] **Step 2: Run the test on the old code**

Run: `go test -count=1 -run TestLoginFreesItsWorkerBeforeTheStore ./internal/logic/`
Expected: `FAIL`, `fast login took 1.9…s: it waited for a worker the stalled ones held` (the gate opens after 2s).

- [ ] **Step 3: The code**

Apply the rest: `git apply --exclude='*_test.go' PATCH`.

Run: `go test -count=1 ./internal/logic/`
Expected: `ok`.

- [ ] **Step 4: Mutation (undo it)**

Put `defer release()` back after the `acquire` error check and delete the new `release()` line. Run Step 2's command. Expected: the same failure as Step 2.

- [ ] **Step 5: Commit**

```bash
git add internal/logic/login.go internal/logic/login_test.go
git commit -m "fix: free Logic's login worker before the store calls" # body: STE; refs #145 on its own line
```

### Task 3: The gateway's senders do not wait for replies

**Files:**
- Modify: `internal/gateway/config.go`, `internal/gateway/gateway.go`, `internal/gateway/login.go`, `cmd/gateway/main.go`
- Test: `internal/gateway/config_test.go`, `internal/gateway/login_test.go`

**Interfaces:**
- Produces (Go): `gateway.Config.LoginInflight int` (default 3,200; `Validate` refuses ≤ 0 and `< LoginBatch`); `Gateway.inflight chan struct{}` of that capacity.
- Produces (unexported): `sendLogins` takes one token per item (returns, giving back what it took, if ctx ends), gives back the tokens of items dropped before sending, then `goRun`s `runLogins(ctx, logic, live, entries, latest)`, which is the old send-and-collect body; its `resolve` gives back one token per entry. `releaseInflight(n int)`.
- Produces (flag): gateway `-login-inflight` (3,200); `-login-rpcs`' help: senders.
- Changed test: `TestQueuedLoginPastTheDeadlineIsDroppedBeforeSending` sets `LoginInflight` 1, so its held login holds the only token (it held the only worker before).

- [ ] **Step 1: The tests**

Save the patch below as `PATCH` and apply only its test files: `git apply --include='*_test.go' PATCH`.

```diff
diff --git a/cmd/gateway/main.go b/cmd/gateway/main.go
index ec4a5e3..617778e 100644
--- a/cmd/gateway/main.go
+++ b/cmd/gateway/main.go
@@ -143,7 +143,8 @@ func parseFlags(args []string) (gateway.Config, gateway.ServerConfig, tcp.Config
 	fs.IntVar(&cfg.MaxConns, "max-conns", cfg.MaxConns, "connection cap: open sockets in any state")
 	fs.IntVar(&cfg.PendingCap, "pending-cap", cfg.PendingCap, "HANDSHAKE + AUTH_PENDING sockets")
 	fs.IntVar(&cfg.LoginQueueCap, "login-queue-cap", cfg.LoginQueueCap, "Logins waiting for or inside a Login RPC")
-	fs.IntVar(&cfg.LoginRPCs, "login-rpcs", cfg.LoginRPCs, "concurrent Login RPCs to Logic")
+	fs.IntVar(&cfg.LoginRPCs, "login-rpcs", cfg.LoginRPCs, "Login senders: each sends a batch to Logic and goes on")
+	fs.IntVar(&cfg.LoginInflight, "login-inflight", cfg.LoginInflight, "Login entries sent to Logic with no result yet")
 	fs.IntVar(&cfg.LoginBatch, "login-batch", cfg.LoginBatch, "entries per Login RPC")
 	fs.IntVar(&cfg.LogoutQueue, "logout-queue", cfg.LogoutQueue, "Logouts queued or waiting for a retry (default: -max-conns)")
 	fs.DurationVar(&cfg.HandshakeTimeout, "handshake-timeout", cfg.HandshakeTimeout, "accept → handshake done (3s–30s)")
diff --git a/internal/gateway/config.go b/internal/gateway/config.go
index 9e1c6f7..88052cb 100644
--- a/internal/gateway/config.go
+++ b/internal/gateway/config.go
@@ -42,7 +42,8 @@ type Config struct {
 	LoginTimeout  time.Duration // per entry from recv_ms, ≥ LoginDeadline + 1s (6s)
 	LoginBatch    int           // entries per Login RPC (200)
 	LoginLinger   time.Duration // longest wait to fill a batch (5ms)
-	LoginRPCs     int           // concurrent Login RPCs (16)
+	LoginRPCs     int           // Login senders (16): each sends a batch and goes on (#145)
+	LoginInflight int           // entries sent with no result yet (3,200 = LoginRPCs × LoginBatch, #145)
 	BusyRetry     Window        // Busy{retry_after}, uniform (1–3s)
 
 	LogoutBatch   int           // entries per Logout RPC (200)
@@ -119,6 +120,7 @@ func DefaultConfig() Config {
 		LoginBatch:       200,
 		LoginLinger:      5 * time.Millisecond,
 		LoginRPCs:        16,
+		LoginInflight:    3200,
 		BusyRetry:        Window{time.Second, 3 * time.Second},
 		LogoutBatch:      200,
 		LogoutLinger:     5 * time.Millisecond,
@@ -175,7 +177,8 @@ func (c Config) Validate() error {
 	}
 	for name, v := range map[string]int{"MaxConns": c.MaxConns, "PendingCap": c.PendingCap,
 		"LoginQueueCap": c.LoginQueueCap, "LoginBatch": c.LoginBatch, "LoginRPCs": c.LoginRPCs,
-		"LogoutBatch": c.LogoutBatch, "LogoutRPCs": c.LogoutRPCs, "LogoutQueue": c.LogoutQueue,
+		"LoginInflight": c.LoginInflight,
+		"LogoutBatch":   c.LogoutBatch, "LogoutRPCs": c.LogoutRPCs, "LogoutQueue": c.LogoutQueue,
 		"Reconcile.Chunk": c.Reconcile.Chunk, "Reconcile.TriggeredRate": c.Reconcile.TriggeredRate,
 		"Reconcile.MinRate": c.Reconcile.MinRate, "Reconcile.MaxInFlight": c.Reconcile.MaxInFlight,
 		"Reconcile.DeleteRate": c.Reconcile.DeleteRate} {
@@ -220,6 +223,9 @@ func (c Config) Validate() error {
 			bad("%s %v must be positive", name, d)
 		}
 	}
+	if c.LoginInflight < c.LoginBatch {
+		bad("LoginInflight %d must be at least LoginBatch %d: a full batch must fit", c.LoginInflight, c.LoginBatch)
+	}
 	if c.LeaseTimeout > c.LeaseInterval {
 		bad("LeaseTimeout %v must not exceed LeaseInterval %v", c.LeaseTimeout, c.LeaseInterval)
 	}
diff --git a/internal/gateway/config_test.go b/internal/gateway/config_test.go
index 56e7c05..006c18c 100644
--- a/internal/gateway/config_test.go
+++ b/internal/gateway/config_test.go
@@ -72,6 +72,8 @@ func TestValidateRefuses(t *testing.T) {
 		{"inverted heartbeat range", func(c *Config) { c.HeartbeatMin = 10 * time.Minute }, "HeartbeatMin"},
 		{"lease timeout above the interval", func(c *Config) { c.LeaseTimeout = 11 * time.Second }, "LeaseTimeout"},
 		{"lease retry above the interval", func(c *Config) { c.LeaseRetry = 11 * time.Second }, "LeaseRetry"},
+		{"inflight below one batch", func(c *Config) { c.LoginInflight = c.LoginBatch - 1 }, "LoginInflight"},
+		{"no inflight", func(c *Config) { c.LoginInflight = 0 }, "LoginInflight"},
 		{"lease TTL not above the interval", func(c *Config) { c.LeaseTTL = 10 * time.Second }, "LeaseTTL"},
 		{"jitter of half a cycle", func(c *Config) { c.Reconcile.Jitter = 5 * time.Minute }, "Jitter"},
 		{"empty Busy window", func(c *Config) { c.BusyRetry = Window{3 * time.Second, time.Second} }, "BusyRetry"},
diff --git a/internal/gateway/gateway.go b/internal/gateway/gateway.go
index 9a28058..18a42bd 100644
--- a/internal/gateway/gateway.go
+++ b/internal/gateway/gateway.go
@@ -30,6 +30,7 @@ type Gateway struct {
 	draining    atomic.Bool
 
 	loginCh  chan *loginItem
+	inflight chan struct{} // one token per entry sent with no result yet (LoginInflight)
 	logoutCh chan *logoutItem
 	gauges   [3]stateGauges
 
@@ -67,6 +68,7 @@ func New(cfg Config, d Deps) (*Gateway, error) {
 		loops:    newLoops(cfg, now()),
 		log:      slog.Default().With("gw", cfg.GatewayID, "epoch", cfg.Epoch),
 		loginCh:  make(chan *loginItem, cfg.LoginQueueCap),
+		inflight: make(chan struct{}, cfg.LoginInflight),
 		logoutCh: make(chan *logoutItem, cfg.LogoutQueue),
 	}
 	g.server = server{registry: d.Registry, readyChecks: d.ReadyChecks, limits: hostLimits, faults: d.Faults}
diff --git a/internal/gateway/login.go b/internal/gateway/login.go
index b19c787..80ba642 100644
--- a/internal/gateway/login.go
+++ b/internal/gateway/login.go
@@ -202,10 +202,18 @@ func (g *Gateway) startLogin(ctx context.Context, logic pb.LogicClient) {
 	}
 }
 
-// sendLogins sends one batch and hands each entry's reply to its loop as it
-// arrives (R5S-O1). An entry with no result by recv_ms + LoginTimeout, or when
-// the stream ends, has no definitive reply (spec: Login batching).
+// sendLogins takes an inflight token per entry and starts the batch's RPC on
+// its own goroutine, so a sender is never held by a slow shard's entries:
+// the gateway bounds the entries with no result, not the RPCs (#145).
 func (g *Gateway) sendLogins(ctx context.Context, logic pb.LogicClient, items []*loginItem) {
+	for n := range items {
+		select {
+		case g.inflight <- struct{}{}:
+		case <-ctx.Done():
+			g.releaseInflight(n)
+			return
+		}
+	}
 	nowMs := g.now().UnixMilli()
 	live := make([]*loginItem, 0, len(items))
 	entries := make([]*pb.LoginEntry, 0, len(items))
@@ -226,12 +234,27 @@ func (g *Gateway) sendLogins(ctx context.Context, logic pb.LogicClient, items []
 		entries = append(entries, it.entry)
 		latest = max(latest, it.entry.RecvMs)
 	}
+	g.releaseInflight(len(items) - len(live))
 	if len(live) == 0 {
 		return
 	}
 	for _, it := range live {
 		g.m.LoginQueueSeconds.Observe(float64(nowMs-it.entry.RecvMs) / 1000)
 	}
+	g.goRun(func() { g.runLogins(ctx, logic, live, entries, latest) })
+}
+
+func (g *Gateway) releaseInflight(n int) {
+	for range n {
+		<-g.inflight
+	}
+}
+
+// runLogins sends one batch and hands each entry's reply to its loop as it
+// arrives (R5S-O1). An entry with no result by recv_ms + LoginTimeout, or when
+// the stream ends, has no definitive reply (spec: Login batching). Each
+// resolved entry gives back its inflight token.
+func (g *Gateway) runLogins(ctx context.Context, logic pb.LogicClient, live []*loginItem, entries []*pb.LoginEntry, latest int64) {
 	var sentMs int64
 	resolved := make([]bool, len(live))
 	left := len(live)
@@ -243,6 +266,7 @@ func (g *Gateway) sendLogins(ctx context.Context, logic pb.LogicClient, items []
 				g.m.LoginLogicSeconds.Observe(float64(g.now().UnixMilli()-sentMs) / 1000)
 			}
 			g.loginFinished()
+			g.releaseInflight(1)
 			g.post(live[i], r)
 		}
 	}
diff --git a/internal/gateway/login_test.go b/internal/gateway/login_test.go
index dd0fb53..3229d75 100644
--- a/internal/gateway/login_test.go
+++ b/internal/gateway/login_test.go
@@ -146,12 +146,12 @@ func TestLogicCodesReachTheClient(t *testing.T) {
 }
 
 func TestQueuedLoginPastTheDeadlineIsDroppedBeforeSending(t *testing.T) {
-	l := newLive(t, func(c *Config) { shortDeadline(c); c.LoginRPCs, c.LoginBatch = 1, 1 })
+	l := newLive(t, func(c *Config) { shortDeadline(c); c.LoginRPCs, c.LoginBatch, c.LoginInflight = 1, 1, 1 })
 	release := make(chan struct{})
 	l.logic.set(func() {
 		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
 			if tokenSubject(b.Entries[0].GetToken()) == "first" {
-				<-release // the only worker is busy
+				<-release // the only inflight token is held
 			}
 			return s.Send(okResult(0, b.Entries[0]))
 		}
@@ -206,3 +206,24 @@ func histDTO(t *testing.T, h prometheus.Histogram) *dto.Histogram {
 	}
 	return m.GetHistogram()
 }
+
+// #145: a sender hands a sent batch to its own goroutine, so a batch held in
+// Logic (a stalled shard) does not hold the next batch back.
+func TestHeldLoginBatchDoesNotHoldTheSender(t *testing.T) {
+	l := newLive(t, func(c *Config) { c.LoginRPCs, c.LoginBatch = 1, 1 })
+	release := make(chan struct{})
+	t.Cleanup(func() { close(release) })
+	l.logic.set(func() {
+		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
+			if tokenSubject(b.Entries[0].GetToken()) == "held" {
+				<-release
+			}
+			return s.Send(okResult(0, b.Entries[0]))
+		}
+	})
+	held, next := l.authed(0), l.authed(1)
+	l.frame(held, clientpb.Cmd_CMD_LOGIN, 1, loginReq("held", "d", 1))
+	waitFor(t, "held sent", func() bool { return get(l.logic, func() int { return len(l.logic.logins) }) == 1 })
+	l.frame(next, clientpb.Cmd_CMD_LOGIN, 1, loginReq("next", "d", 1))
+	waitFor(t, "next logged in while the first is held", func() bool { return next.last(t, clientpb.Cmd_CMD_LOGIN_RESP, nil) })
+}
```

- [ ] **Step 2: Run the tests on the old code**

Run: `go vet ./internal/gateway/`
Expected: a type-check error, `internal/gateway/config_test.go:75:52: c.LoginInflight undefined`.

- [ ] **Step 3: The code**

Apply the rest: `git apply --exclude='*_test.go' PATCH`.

Run: `go vet ./internal/gateway/ ./cmd/gateway/ && go test -count=1 ./internal/gateway/ ./cmd/gateway/`
Expected: `ok` for both.

- [ ] **Step 4: Mutations (undo each)**

1. In `sendLogins`, replace `g.goRun(func() { g.runLogins(ctx, logic, live, entries, latest) })` with `g.runLogins(ctx, logic, live, entries, latest)`. Run `go test -count=1 -run TestHeldLoginBatchDoesNotHoldTheSender ./internal/gateway/`. Expected: `FAIL`, `timed out waiting for next logged in while the first is held` (~5s).
2. In `Validate`, change `if c.LoginInflight < c.LoginBatch {` to `if false {`. Run `go test -count=1 -run TestValidateRefuses ./internal/gateway/`. Expected: `TestValidateRefuses/inflight_below_one_batch` fails, `want an error about LoginInflight`.

- [ ] **Step 5: The whole suite, commit**

Run: `go vet ./... && make test && make test-redis-local && make test-redis-cluster-local`
Expected: no vet output; every package `ok` in each (no `FAIL`).

```bash
git add internal/gateway cmd/gateway
git commit -m "fix: let gateway login senders go on after a send" # body: STE, the inflight bound; refs #145 on its own line
```

### Task 4: Dry run, run 35, and the record

**Files:**
- Modify: this plan's *Execution Notes*; the roadmap (the plan 6 row, #144's and #146's outcome).

**Interfaces:**
- Consumes: Tasks 1–3 on `feat/plan6c2c`, pushed; `scripts/tier2/failover.sh` unchanged.

- [ ] **Step 1: Dry run on the dev machine**

Plan 6c2b's Task 3 Step 5 recipe, unchanged (a throwaway copy of `failover.sh` on the dev machine's 2 CPUs, 3,000 clients, flow 20/s; ~15 minutes), with `S` the session scratchpad:
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

Expected (prototype): `exit 1`; **30** checks; failing only `no_swap_in_manual`, `no_swap_in_kill`, `no_swap_in_zone` (the dev machine's swap; none to all three). `kill` and `zone`: `master_ms` 6,000–10,000 (prototype 6,795 and 7,277); the window's other-shard p99 within max(2 × before, before + 50ms) (prototype 16 against 13, 16 against 14). Every gateway's `missing` 0 in every phase. **`zone_other_shards` passes** (it failed in 6c2b's dry run, 1,605ms). Then `rm -rf tier2-results/failover-*`.

- [ ] **Step 2: The user runs run 35**

Ask the user to run, on the Tier 2 host, from a fresh pull of `feat/plan6c2c`, with the desktop apps closed (after `sudo scripts/tier2/host-setup.sh` if the host rebooted since the last run):
```bash
GOIM_TEST_TIER=2 make test-tier2 SCENARIO=failover
```
and to send back `tier2-results/failover-<timestamp>.report.txt`. About 20 minutes.

- [ ] **Step 3: Read the report**

Expected: exit 0, **30/30** checks; `kill_other_shards` and `zone_other_shards` pass (run 34: 104ms against a 71ms bound, 4,470ms against 21ms). For each phase read and record, against run 34 (6c2b *Execution Notes*):
- `P_other_shards`: the before and the window's other-shard p99s, their counts, retried sequences and `max_ms`; the target shard's n, retried, p50/p99/max; `target_affected_share`.
- `P_failover`: `master_ms`, `at_ms_since_renewal`, `target`, `gwepochs_on`. `P_lease`: `missing` 0, `error` per gateway.
- The gateways' `logins_busy` and `logins_expired` (Review Focus 3), Logic's CPU in the through window.
- Logic's log (`stack/logs/logic*.log`): store errors in the before windows (Review Focus 1), and `context deadline exceeded` counts in the windows.
- `P_sessions`: `through_lost`.

A failing check: record it with its numbers and ask the user (AskUserQuestion) before any change. A failed `P_lease` breaks the spec's lease rule: stop and ask.

- [ ] **Step 4: Record and commit**

In this plan's *Execution Notes*: the dry run and run 35's numbers as in Step 3, beside run 34's. In the roadmap: the plan 6 row (6c2c executed, run 35 and its result), #144's outcome and #146's. Commit (docs), push.

---

## After this plan

- **Plan 6c3** (written in a fresh session): `gwconns` shard load and the ReconcileEnd and drain deadlines; inputs: run 35's per-shard latency, Review Focus 1 and 3 (a group slower than 1s fails; more single-entry RPCs at low rates).
- The spec's next revision: `-login-inflight` and the per-master group bound (#145) beside #139, #140 and #142.
- Then **6d** (Tier 3, rolling restart, the LB).

## Execution Notes

**Executed 2026-11-25** inline (superpowers:executing-plans), on `feat/plan6c2c` from `9dc7d12`.
- Task 1 `0e6604d`, Task 2 `7049e63`, Task 3 `fb35e05`: the three patches applied as written. Every Step 2 failure and every mutation matched its `Expected:` line. Task 3 Step 5: 60 package runs `ok` (`make test`, `make test-redis-local`, `make test-redis-cluster-local`). The code after Task 3 equals `proto-6c2c`'s (empty `git diff`).
- **Dry run (Task 4 Step 1) on `fb35e05`:** 30 checks, only the three swap checks fail. `master_ms` manual 50, kill 9,158, zone 6,591. Other-shard p99 before → window: manual 13 → 9, kill 15 → 14, zone 13 → 13ms. `missing` 0 for every gateway in every phase.
- **Final review** (a fresh reviewer, whole branch): no Critical. Two Important, both fixed with a test that failed first:
  - **I-1 `8247e19`:** senders took their tokens one at a time, all at once. With `-login-inflight` below `-login-rpcs` × `-login-batch` (allowed by `Validate`), two senders could each hold part of the tokens and wait for good. Logins then stopped until a restart. The defaults were safe only because 16 × 200 = 3,200. Fix: one sender takes tokens at a time (`Gateway.taking`). Test `TestSendersDoNotSplitTheInflightTokens` (timed out 3/3 before the fix).
  - **I-2 `38319a0`:** Logout held a shared worker while it waited for its master's slot. That wait lies outside the 1s group bound and lasts up to the gateway's 5s RPC timeout. A stalled shard makes more Logouts (fence b), so with many gateways per Logic they could fill the shared pool and slow the other shards' logins again. Review Focus 3's "each now ends within ~1s" is wrong for the same reason: an entry waiting for a slot lasts up to its login timeout. The user chose (AskUserQuestion) that Logout frees its worker before the store too, as Login does; #146.5 amended. Test `TestLogoutFreesItsWorkerBeforeTheStore` (1.9s before the fix).
  - After both fixes: `go vet ./...` clean; 60 package runs `ok`.
  - **Minors, deferred:** M1 the 1s group bound also counts the wait for a go-redis pool connection (Logic sets no `PoolSize`; 64 groups per master can be in flight), so read run 35's before-window store errors with that in mind. M2 shutdown drops batches waiting for tokens (Review Focus 4, as stated). M3 the slot map never shrinks, and keys with no resolved master share one `""` pool (Review Focus 5). M4 `TestLoginFreesItsWorkerBeforeTheStore` can hang if it fails before `close(gate.release)`. M5 `silentRedis` calls `t.Cleanup` from its accept goroutine. M6 Login sends rejected entries while it holds the shared worker; `LoginQueueSeconds` now includes the token wait.
- **Dry run again on `38319a0`:** 30 checks, only the three swap checks fail. `master_ms` manual 47, kill 8,688, zone 8,468. Other-shard p99 before → window: manual 12 → 12, kill 12 → 12, zone 13 → 13ms. `missing` 0 everywhere.
- **Run 35:** pending (the user, on the Tier 2 host).
