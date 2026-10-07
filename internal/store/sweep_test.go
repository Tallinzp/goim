package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestSweepDeletesOnlyDeadEpochFields(t *testing.T) {
	s, rdb := newStore(t)
	dead := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 1}
	// Same pod name, new epoch (race 4): must survive the sweep of the old epoch.
	alive := EpochRef{Gateway: dead.Gateway, Epoch: 1 << 63}
	mk := func(r EpochRef, conn uint64) string {
		return session.Entry{Gateway: r.Gateway, Epoch: r.Epoch, ConnID: conn, DeviceID: "d",
			ConnSeq: 1, LoginMs: 1, TokIatMs: 1}.Encode()
	}
	uid := testredis.UID()
	rdb.HSet(ctx, sessKey(uid), "im:mobile", mk(dead, 1), "im:pc", mk(alive, 2), "im:pad", mk(dead, 3))
	rdb.SAdd(ctx, dead.connsKey(), uid)
	if _, err := s.RenewLease(ctx, dead); err != nil {
		t.Fatal(err)
	}

	ok, err := s.ClaimSweep(ctx, dead)
	if err != nil || !ok {
		t.Fatalf("first claim: %v, %v", ok, err)
	}
	if ok, _ := s.ClaimSweep(ctx, dead); ok {
		t.Fatal("second claim must fail while the first is held")
	}
	n, err := s.SweepUsers(ctx, dead, []string{uid})
	if err != nil || n != 2 {
		t.Fatalf("SweepUsers = %d, %v; want 2", n, err)
	}
	if f := rdb.HKeys(ctx, sessKey(uid)).Val(); len(f) != 1 || f[0] != "im:pc" {
		t.Fatalf("fields left = %v, want [im:pc]", f)
	}
	if err := s.FinishSweep(ctx, dead); err != nil {
		t.Fatal(err)
	}
	if rdb.Exists(ctx, dead.connsKey()).Val() != 0 {
		t.Fatal("gwconns not unlinked")
	}
	if _, ok, _ := s.EpochScoreOf(ctx, dead); ok {
		t.Fatal("gwepochs member not removed")
	}
}

func TestReleaseSweepFreesTheClaim(t *testing.T) {
	s, _ := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 5}
	if ok, _ := s.ClaimSweep(ctx, ref); !ok {
		t.Fatal("claim failed")
	}
	if err := s.ReleaseSweep(ctx, ref); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.ClaimSweep(ctx, ref); !ok {
		t.Fatal("claim after release failed")
	}
}
