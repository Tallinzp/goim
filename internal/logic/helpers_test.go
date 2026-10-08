package logic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const field = "im:mobile"

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Unix(1_000_000, 0)} }

func (c *fakeClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

// testConfig is DefaultConfig with a Kick client that gives up quickly.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Kick.Deadline = 300 * time.Millisecond
	cfg.Kick.RetrySpacing = 50 * time.Millisecond
	cfg.Kick.IdleTimeout = 200 * time.Millisecond
	return cfg
}

// newStore returns a real store on a fresh miniredis (or the Tier 2 Redis).
func newStore(t *testing.T, cfg Config) (*store.Store, *redis.Client) {
	t.Helper()
	rdb := testredis.New(t)
	return store.New(rdb, cfg.Store), rdb
}

func newMetrics() *Metrics { return NewMetrics(prometheus.NewRegistry()) }

func counter(c prometheus.Collector) float64 { return testutil.ToFloat64(c) }

func redisNow(t *testing.T, st Store) int64 {
	t.Helper()
	n, err := st.RedisNowMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// countingStore counts LeasesExist calls.
type countingStore struct {
	*store.Store
	mu     sync.Mutex
	leases int
}

func (s *countingStore) LeasesExist(ctx context.Context, refs []store.EpochRef) ([]bool, error) {
	s.mu.Lock()
	s.leases++
	s.mu.Unlock()
	return s.Store.LeasesExist(ctx, refs)
}

func histogram(t *testing.T, h prometheus.Histogram) *dto.Histogram {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetHistogram()
}

func sampleCount(t *testing.T, h prometheus.Histogram) uint64 {
	return histogram(t, h).GetSampleCount()
}
func sampleSum(t *testing.T, h prometheus.Histogram) float64 { return histogram(t, h).GetSampleSum() }
