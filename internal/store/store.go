// Package store is Logic's only access path to Redis: the session schema
// and its Lua scripts. Gateways must never import it (layer rule).
package store

import (
	"context"
	"embed"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds the values the scripts depend on.
type Config struct {
	LoginDeadline time.Duration // login_deadline (default 5s)
	SessTTL       time.Duration // sess:{uid} TTL, also used for rev:{uid} (R4M-1); default 30d
	LeaseTTL      time.Duration // gwlease TTL (default 30s)
	WaitTimeout   time.Duration // Revoke's WAIT 1 <timeout> (default 1s)
	// GroupTimeout bounds one master's group in runEach, from its send to
	// its last reply, so a stopped master fails its calls in one read
	// timeout and go-redis has no time to send them again (#145). 0: none.
	GroupTimeout time.Duration
	// GroupWorkers bounds the groups in flight to one master, so a stalled
	// master uses only its own share (#145). 0: no bound.
	GroupWorkers int
}

// DefaultConfig returns the spec defaults.
func DefaultConfig() Config {
	return Config{
		LoginDeadline: 5 * time.Second,
		SessTTL:       30 * 24 * time.Hour,
		LeaseTTL:      30 * time.Second,
		WaitTimeout:   time.Second,
	}
}

// sweepClaimTTL is the sweep:gw:{epoch} claim's PX (spec: SET NX PX 300000).
const sweepClaimTTL = 300 * time.Second

// Store runs every session-store operation against a Redis Cluster or,
// in tests, a single Redis node.
type Store struct {
	rdb redis.UniversalClient
	cfg Config
	// groupOf names the node group of a key; calls in one group share a
	// pipeline. It is masterAddr in production; tests replace it.
	groupOf func(ctx context.Context, key string) string
	// masterFor returns the node that owns key (nodeFor in production), and
	// reloadSlots asks the cluster client to reload its slot map (nil for a
	// single node). Revoke uses them to re-route after a failover (PI-6);
	// tests replace both.
	masterFor   func(ctx context.Context, key string) (*redis.Client, error)
	reloadSlots func(ctx context.Context)
	rerouteWait time.Duration // how long Revoke waits for a new master

	slotsMu sync.Mutex
	slots   map[string]chan struct{} // per group, GroupWorkers each
}

// rerouteWait's default, and how often the slot map is re-read meanwhile.
const (
	defaultRerouteWait = 2 * time.Second
	reroutePoll        = 100 * time.Millisecond
)

// New returns a Store. rdb must be a *redis.ClusterClient or a *redis.Client.
func New(rdb redis.UniversalClient, cfg Config) *Store {
	s := &Store{rdb: rdb, cfg: cfg, rerouteWait: defaultRerouteWait}
	s.groupOf = s.masterAddr
	s.masterFor = s.nodeFor
	if c, ok := rdb.(*redis.ClusterClient); ok {
		s.reloadSlots = c.ReloadState
	}
	return s
}

//go:embed lua/*.lua
var luaFS embed.FS

func mustScript(name string) *redis.Script {
	src, err := luaFS.ReadFile("lua/" + name)
	if err != nil {
		panic(err)
	}
	return redis.NewScript(string(src))
}

func ms(d time.Duration) int64 { return d.Milliseconds() }

// scriptCall is one script invocation inside a pipelined batch.
type scriptCall struct {
	keys []string
	args []any
}

// runBatch pipelines EVALSHA for every call. Calls that fail with NOSCRIPT
// (a new master after failover has an empty script cache) did not run, so
// they are re-run one by one with Run, which falls back to EVAL.
func (s *Store) runBatch(ctx context.Context, sc *redis.Script, calls []scriptCall) []*redis.Cmd {
	cmds := make([]*redis.Cmd, len(calls))
	if len(calls) == 0 {
		return cmds
	}
	// The pipeline's own error repeats the first failed command; each cmd is inspected below.
	_, _ = s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i, c := range calls {
			cmds[i] = sc.EvalSha(ctx, p, c.keys, c.args...)
		}
		return nil
	})
	for i, cmd := range cmds {
		if redis.HasErrorPrefix(cmd.Err(), "NOSCRIPT") {
			cmds[i] = sc.Run(ctx, s.rdb, calls[i].keys, calls[i].args...)
		}
	}
	return cmds
}

// masterAddr returns the address of the master that owns key, or "" on a
// single node or when the owner can't be resolved (the cluster client then
// routes those calls as usual).
func (s *Store) masterAddr(ctx context.Context, key string) string {
	c, ok := s.rdb.(*redis.ClusterClient)
	if !ok {
		return ""
	}
	n, err := c.MasterForKey(ctx, key)
	if err != nil {
		return ""
	}
	return n.Options().Addr
}

// runEach is runBatch split by owning master (R5S-O1): each group is
// pipelined on its own, concurrently, and done(j, cmd) runs for its calls as
// soon as that group returns, so a slow or failing-over shard delays only its
// own calls. done may run on several goroutines at once. runEach returns
// after every done call.
func (s *Store) runEach(ctx context.Context, sc *redis.Script, calls []scriptCall, done func(j int, cmd *redis.Cmd)) {
	groups := map[string][]int{}
	for j, c := range calls {
		g := s.groupOf(ctx, c.keys[0])
		groups[g] = append(groups[g], j)
	}
	var wg sync.WaitGroup
	for g, js := range groups {
		wg.Add(1)
		go func(g string, js []int) {
			defer wg.Done()
			sub := make([]scriptCall, len(js))
			for k, j := range js {
				sub[k] = calls[j]
			}
			release, err := s.acquireGroup(ctx, g)
			if err != nil {
				for _, j := range js {
					done(j, failedCmd(ctx, err))
				}
				return
			}
			defer release()
			gctx, cancel := s.groupCtx(ctx)
			defer cancel()
			for k, cmd := range s.runBatch(gctx, sc, sub) {
				done(js[k], cmd)
			}
		}(g, js)
	}
	wg.Wait()
}

// acquireGroup takes one of group g's GroupWorkers slots, waiting until ctx
// ends. The wait is outside GroupTimeout: a busy healthy master queues its
// calls, it does not fail them.
func (s *Store) acquireGroup(ctx context.Context, g string) (release func(), err error) {
	if s.cfg.GroupWorkers <= 0 {
		return func() {}, nil
	}
	s.slotsMu.Lock()
	if s.slots == nil {
		s.slots = map[string]chan struct{}{}
	}
	ch, ok := s.slots[g]
	if !ok {
		ch = make(chan struct{}, s.cfg.GroupWorkers)
		s.slots[g] = ch
	}
	s.slotsMu.Unlock()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// groupCtx is ctx bounded by GroupTimeout, when set.
func (s *Store) groupCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.cfg.GroupTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.cfg.GroupTimeout)
}

// failedCmd is a script call that was never sent, failed with err.
func failedCmd(ctx context.Context, err error) *redis.Cmd {
	cmd := redis.NewCmd(ctx)
	cmd.SetErr(err)
	return cmd
}

// nodeFor returns the client for the master that owns key, so commands that
// must share one connection (a write followed by WAIT) can use Conn().
func (s *Store) nodeFor(ctx context.Context, key string) (*redis.Client, error) {
	switch c := s.rdb.(type) {
	case *redis.ClusterClient:
		return c.MasterForKey(ctx, key)
	case *redis.Client:
		return c, nil
	default:
		return nil, fmt.Errorf("store: unsupported redis client %T", s.rdb)
	}
}

// RedisNowMs returns Redis TIME in milliseconds.
func (s *Store) RedisNowMs(ctx context.Context) (int64, error) {
	t, err := s.rdb.Time(ctx).Result()
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}
