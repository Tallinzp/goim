package logic

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
)

type sweepEnv struct {
	t   *testing.T
	sw  *Sweeper
	st  Store
	rdb *redis.Client
	clk *fakeClock
	m   *Metrics
	cfg Config
}

func newSweepEnv(t *testing.T, wrap func(*store.Store) Store) *sweepEnv {
	t.Helper()
	// A cycle reads and sweeps the global gwepochs key. The shared Tier 2
	// Redis holds other tests' epochs (other packages run in parallel), so a
	// cycle there would see their renewals and sweep their data. The store
	// primitives a cycle calls are verified on real Redis by Plan 1's tests.
	if testredis.Real() {
		t.Skip("sweeper loop tests need a gwepochs of their own (miniredis only)")
	}
	cfg := testConfig()
	cfg.SweepBatch = 2
	real, rdb := newStore(t, cfg)
	st := wrap(real)
	clk := newClock()
	m := newMetrics()
	return &sweepEnv{t: t, sw: NewSweeper(cfg, st, m, clk.Now), st: st, rdb: rdb, clk: clk, m: m, cfg: cfg}
}

// deadEpoch registers an epoch whose last renewal is 2 × TTL + 1s ago, with no
// lease key, and n sessions; one uid also holds a field of another epoch.
func (e *sweepEnv) deadEpoch(n int) (store.EpochRef, []string) {
	e.t.Helper()
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 1}
	now := redisNow(e.t, e.st)
	member := fmt.Sprintf("%s:%d", ref.Gateway, ref.Epoch)
	if err := e.rdb.ZAdd(ctx, "gwepochs", redis.Z{Score: float64(now - e.cfg.StaleAfter().Milliseconds() - 1000), Member: member}).Err(); err != nil {
		e.t.Fatal(err)
	}
	uids := uidList(n)
	for i, uid := range uids {
		ent := session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: uint64(i + 1), DeviceID: "d", ConnSeq: 1, LoginMs: now, TokIatMs: 1}
		if err := e.rdb.HSet(ctx, "sess:{"+uid+"}", field, ent.Encode()).Err(); err != nil {
			e.t.Fatal(err)
		}
	}
	other := session.Entry{Gateway: "gw-other", Epoch: 5, ConnID: 1, DeviceID: "d", ConnSeq: 1, LoginMs: now, TokIatMs: 1}
	if err := e.rdb.HSet(ctx, "sess:{"+uids[0]+"}", "im:pad", other.Encode()).Err(); err != nil {
		e.t.Fatal(err)
	}
	if err := e.st.AddConns(ctx, ref, uids); err != nil {
		e.t.Fatal(err)
	}
	return ref, uids
}

// liveEpoch renews a lease now (another gateway that keeps renewing).
func (e *sweepEnv) liveEpoch() store.EpochRef {
	e.t.Helper()
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	if _, err := e.st.RenewLease(ctx, ref); err != nil {
		e.t.Fatal(err)
	}
	return ref
}

func (e *sweepEnv) fields(uids []string) (mobile, pad int) {
	for _, uid := range uids {
		if v, _ := e.rdb.HGet(ctx, "sess:{"+uid+"}", field).Result(); v != "" {
			mobile++
		}
		if v, _ := e.rdb.HGet(ctx, "sess:{"+uid+"}", "im:pad").Result(); v != "" {
			pad++
		}
	}
	return
}

func (e *sweepEnv) exists(key string) bool { return e.rdb.Exists(ctx, key).Val() == 1 }

func keysOf(ref store.EpochRef) (conns, claim, member string) {
	suffix := fmt.Sprintf("%s:{%d}", ref.Gateway, ref.Epoch)
	return "gwconns:" + suffix, "sweep:" + suffix, fmt.Sprintf("%s:%d", ref.Gateway, ref.Epoch)
}

func TestSweeperSweepsDeadEpochAfterGuard(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, uids := e.deadEpoch(5)
	live := e.liveEpoch()

	e.sw.Cycle(ctx) // renewing_since starts now: held
	if m, _ := e.fields(uids); m != 5 || counter(e.m.GuardHolds) != 1 || counter(e.m.GuardHolding) != 1 {
		t.Fatalf("swept before the guard released: %d fields left, holds %v", m, counter(e.m.GuardHolds))
	}
	e.clk.Add(e.cfg.Store.LeaseTTL)
	if _, err := e.st.RenewLease(ctx, live); err != nil { // the live gateway keeps renewing
		t.Fatal(err)
	}
	e.sw.Cycle(ctx)
	mobile, pad := e.fields(uids)
	if mobile != 0 || pad != 1 {
		t.Fatalf("after sweep: %d dead-epoch fields left, %d other-epoch fields (want 0, 1)", mobile, pad)
	}
	conns, _, member := keysOf(ref)
	if e.exists(conns) || e.rdb.ZScore(ctx, "gwepochs", member).Err() != redis.Nil {
		t.Fatal("gwconns not unlinked or gwepochs member not removed")
	}
	if counter(e.m.Sweeps.WithLabelValues("done")) != 1 || counter(e.m.GuardHolding) != 0 {
		t.Fatal("metrics")
	}
}

// Spec (R4M-3): the sweeper skips an epoch whose lease key exists although its
// score is stale, and releases its claim.
func TestSweeperSkipsEpochWhoseLeaseExists(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, uids := e.deadEpoch(3)
	e.liveEpoch()
	conns, claim, _ := keysOf(ref)
	if err := e.rdb.Set(ctx, "gwlease:"+conns[len("gwconns:"):], "x", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	e.sw.Cycle(ctx)
	e.clk.Add(e.cfg.Store.LeaseTTL)
	e.liveEpoch()
	e.sw.Cycle(ctx)
	if m, _ := e.fields(uids); m != 3 {
		t.Fatalf("%d fields left, want 3", m)
	}
	if e.exists(claim) || counter(e.m.Sweeps.WithLabelValues("lease_live")) != 1 {
		t.Fatal("claim not released or result not counted")
	}
}

// renewMidSweep renews the epoch after the first SweepUsers batch, like a
// starved gateway coming back during its sweep.
type renewMidSweep struct {
	*store.Store
	ref     store.EpochRef
	batches int
	missing bool
}

func (r *renewMidSweep) SweepUsers(ctx context.Context, ref store.EpochRef, uids []string) (int, error) {
	n, err := r.Store.SweepUsers(ctx, ref, uids)
	if r.batches++; r.batches == 1 {
		res, err := r.Store.RenewLease(ctx, r.ref)
		if err != nil {
			panic(err)
		}
		r.missing = res.Missing
	}
	return n, err
}

// Spec (R3-O3, R3-OP6): a renewal mid-sweep reports missing; the sweeper
// aborts without UNLINK or ZREM and deletes its claim key.
func TestSweeperAbortsWhenEpochRenews(t *testing.T) {
	hook := &renewMidSweep{}
	e := newSweepEnv(t, func(s *store.Store) Store { hook.Store = s; return hook })
	ref, uids := e.deadEpoch(6)
	hook.ref = ref
	e.liveEpoch()
	e.sw.Cycle(ctx)
	e.clk.Add(e.cfg.Store.LeaseTTL)
	e.liveEpoch()
	e.sw.Cycle(ctx)

	if !hook.missing {
		t.Fatal("the renewal during a sweep must report missing")
	}
	if m, _ := e.fields(uids); m != 4 { // one batch of 2 deleted before the abort
		t.Fatalf("%d fields left, want 4", m)
	}
	conns, claim, member := keysOf(ref)
	if !e.exists(conns) || e.rdb.ZScore(ctx, "gwepochs", member).Err() != nil {
		t.Fatal("an aborted sweep unlinked gwconns or removed the member")
	}
	if e.exists(claim) || counter(e.m.Sweeps.WithLabelValues("aborted")) != 1 {
		t.Fatal("claim not deleted or abort not counted")
	}
}

// Epochs that sent EpochEnded are exempt from the guard (roadmap choice #2).
func TestSweeperEndedEpochIsExempt(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, uids := e.deadEpoch(2)
	if err := e.st.EndEpoch(ctx, ref); err != nil { // score 0
		t.Fatal(err)
	}
	e.sw.Cycle(ctx) // fresh sweeper, no renewals at all: the guard would hold
	if m, _ := e.fields(uids); m != 0 {
		t.Fatalf("%d fields of an ended epoch left", m)
	}
}

// A systemic outage: nobody renews, so a stale epoch is never swept.
func TestSweeperHoldsWhenNobodyRenews(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	_, uids := e.deadEpoch(2)
	for range 20 {
		e.sw.Cycle(ctx)
		e.clk.Add(e.cfg.SweepInterval)
	}
	if m, _ := e.fields(uids); m != 2 || counter(e.m.GuardHolding) != 1 {
		t.Fatalf("%d fields left, holding %v", m, counter(e.m.GuardHolding))
	}
}

func TestTwoSweepersClaimOnce(t *testing.T) {
	e := newSweepEnv(t, func(s *store.Store) Store { return s })
	ref, _ := e.deadEpoch(2)
	if ok, err := e.st.ClaimSweep(ctx, ref); !ok || err != nil { // another node holds it
		t.Fatalf("claim %v %v", ok, err)
	}
	e.liveEpoch()
	e.sw.Cycle(ctx)
	e.clk.Add(e.cfg.Store.LeaseTTL)
	e.liveEpoch()
	e.sw.Cycle(ctx)
	if counter(e.m.Sweeps.WithLabelValues("claimed_elsewhere")) != 1 {
		t.Fatal("swept an epoch another node had claimed")
	}
}
