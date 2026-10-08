package logic

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestLeaseCacheReusesObservationsForTTL(t *testing.T) {
	cfg := testConfig()
	real, _ := newStore(t, cfg)
	st := &countingStore{Store: real}
	clk := newClock()
	c := NewLeaseCache(st, time.Second, clk.Now)
	alive := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1 << 62}
	dead := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 7}
	if _, err := real.RenewLease(ctx, alive); err != nil {
		t.Fatal(err)
	}

	got, err := c.Live(ctx, []store.EpochRef{alive, dead, alive})
	if err != nil {
		t.Fatal(err)
	}
	if !got[alive] || got[dead] || st.leases != 1 {
		t.Fatalf("got %v after %d calls", got, st.leases)
	}

	// Within the TTL the cache answers, even though the lease is gone.
	if err := real.EndEpoch(ctx, alive); err != nil {
		t.Fatal(err)
	}
	clk.Add(999 * time.Millisecond)
	if live, _ := c.LiveOne(ctx, alive); !live || st.leases != 1 {
		t.Fatalf("cached: live %v calls %d", live, st.leases)
	}
	clk.Add(time.Millisecond)
	if live, _ := c.LiveOne(ctx, alive); live || st.leases != 2 {
		t.Fatalf("expired: live %v calls %d", live, st.leases)
	}

	// Set overrides at once (a renewal Logic just made).
	c.Set(dead, true)
	if live, _ := c.LiveOne(ctx, dead); !live || st.leases != 2 {
		t.Fatalf("after Set: live %v calls %d", live, st.leases)
	}
}

func TestLeaseCacheRenewedMs(t *testing.T) {
	cfg := testConfig()
	st, _ := newStore(t, cfg)
	clk := newClock()
	c := NewLeaseCache(st, time.Second, clk.Now)
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 9}
	if _, ok, err := c.RenewedMs(ctx, ref); ok || err != nil {
		t.Fatalf("unknown epoch: ok %v err %v", ok, err)
	}
	res, err := st.RenewLease(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	clk.Add(time.Second)
	ms, ok, err := c.RenewedMs(ctx, ref)
	if err != nil || !ok || ms != res.RedisNowMs {
		t.Fatalf("score %d ok %v err %v, want %d", ms, ok, err, res.RedisNowMs)
	}
}
