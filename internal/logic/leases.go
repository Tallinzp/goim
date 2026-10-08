package logic

import (
	"context"
	"sync"
	"time"

	"github.com/Tallinzp/goim/internal/store"
)

// LeaseCache reuses lease observations for a short time (spec: Logic caches
// lease state for ~1s; there are only a handful of epochs). A cached answer is
// an observation, never a guarantee (R4M-2).
type LeaseCache struct {
	st  Store
	ttl time.Duration
	now func() time.Time

	mu     sync.Mutex
	live   map[store.EpochRef]obs[bool]
	scores map[store.EpochRef]obs[score]
}

type obs[T any] struct {
	v  T
	at time.Time
}

type score struct {
	ms int64
	ok bool
}

// pruneAt bounds the maps: epochs accumulate with every gateway restart.
const pruneAt = 1024

// NewLeaseCache returns a cache whose entries live for ttl.
func NewLeaseCache(st Store, ttl time.Duration, now func() time.Time) *LeaseCache {
	return &LeaseCache{st: st, ttl: ttl, now: now,
		live: map[store.EpochRef]obs[bool]{}, scores: map[store.EpochRef]obs[score]{}}
}

// Live reports whether each ref's lease key exists, from the cache where fresh
// and with one LeasesExist call for the rest.
func (c *LeaseCache) Live(ctx context.Context, refs []store.EpochRef) (map[store.EpochRef]bool, error) {
	out := make(map[store.EpochRef]bool, len(refs))
	var miss []store.EpochRef
	c.mu.Lock()
	now := c.now()
	for _, r := range refs {
		if o, ok := c.live[r]; ok && now.Sub(o.at) < c.ttl {
			out[r] = o.v
		} else if _, dup := out[r]; !dup {
			miss = append(miss, r)
			out[r] = false
		}
	}
	c.mu.Unlock()
	if len(miss) == 0 {
		return out, nil
	}
	exists, err := c.st.LeasesExist(ctx, miss)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, r := range miss {
		out[r] = exists[i]
		c.live[r] = obs[bool]{exists[i], now}
	}
	prune(c.live, now, c.ttl)
	return out, nil
}

// LiveOne is Live for one ref.
func (c *LeaseCache) LiveOne(ctx context.Context, ref store.EpochRef) (bool, error) {
	m, err := c.Live(ctx, []store.EpochRef{ref})
	if err != nil {
		return false, err
	}
	return m[ref], nil
}

// Peek returns ref's cached observation without reading Redis; ok is false
// if there is none or it is older than the cache TTL.
func (c *LeaseCache) Peek(ref store.EpochRef) (live, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o, found := c.live[ref]
	if !found || c.now().Sub(o.at) >= c.ttl {
		return false, false
	}
	return o.v, true
}

// Set records a lease state Logic has just caused (a renewal or EpochEnded).
func (c *LeaseCache) Set(ref store.EpochRef, live bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.live[ref] = obs[bool]{live, c.now()}
}

// RenewedMs returns ref's gwepochs score (last renewal, Redis ms); ok is false
// if the member is gone. Cached like Live, so a relogin storm toward one dead
// epoch reads the gwepochs key about once per ttl, not once per login.
func (c *LeaseCache) RenewedMs(ctx context.Context, ref store.EpochRef) (int64, bool, error) {
	c.mu.Lock()
	now := c.now()
	if o, ok := c.scores[ref]; ok && now.Sub(o.at) < c.ttl {
		c.mu.Unlock()
		return o.v.ms, o.v.ok, nil
	}
	c.mu.Unlock()
	ms, ok, err := c.st.EpochScoreOf(ctx, ref)
	if err != nil {
		return 0, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.scores[ref] = obs[score]{score{ms, ok}, now}
	prune(c.scores, now, c.ttl)
	return ms, ok, nil
}

func prune[T any](m map[store.EpochRef]obs[T], now time.Time, ttl time.Duration) {
	if len(m) < pruneAt {
		return
	}
	for k, o := range m {
		if now.Sub(o.at) >= ttl {
			delete(m, k)
		}
	}
}
