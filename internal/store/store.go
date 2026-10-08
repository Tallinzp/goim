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
}

// New returns a Store. rdb must be a *redis.ClusterClient or a *redis.Client.
func New(rdb redis.UniversalClient, cfg Config) *Store {
	s := &Store{rdb: rdb, cfg: cfg}
	s.groupOf = s.masterAddr
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
	for _, js := range groups {
		wg.Add(1)
		go func(js []int) {
			defer wg.Done()
			sub := make([]scriptCall, len(js))
			for k, j := range js {
				sub[k] = calls[j]
			}
			for k, cmd := range s.runBatch(ctx, sc, sub) {
				done(js[k], cmd)
			}
		}(js)
	}
	wg.Wait()
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
