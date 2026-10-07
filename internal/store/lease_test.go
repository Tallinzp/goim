package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/testredis"
)

func TestRenewLease(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 3}

	first, err := s.RenewLease(ctx, ref)
	if err != nil || !first.Missing || first.IndexErr != nil {
		t.Fatalf("first renewal of a new epoch: %+v, %v; want missing", first, err)
	}
	second, err := s.RenewLease(ctx, ref)
	if err != nil || second.Missing {
		t.Fatalf("second renewal: %+v, %v; want live", second, err)
	}
	if ttl := rdb.PTTL(ctx, ref.leaseKey()).Val(); ttl <= 0 || ttl > DefaultConfig().LeaseTTL {
		t.Fatalf("lease PTTL = %v, want (0, 30s]", ttl)
	}
	if d := second.RedisNowMs - nowMs(t, s); d > 1000 || d < -1000 {
		t.Fatalf("RedisNowMs off by %dms", d)
	}
	score, err := rdb.ZScore(ctx, epochsKey, ref.member()).Result()
	if err != nil || int64(score) != second.RedisNowMs {
		t.Fatalf("gwepochs score = %v (%v), want %d", score, err, second.RedisNowMs)
	}

	rdb.Del(ctx, ref.leaseKey()) // expired or swept
	third, err := s.RenewLease(ctx, ref)
	if err != nil || !third.Missing {
		t.Fatalf("renewal after the key vanished: %+v, %v; want missing", third, err)
	}
}

func TestEndEpochAndLeasesExist(t *testing.T) {
	s, rdb := newStore(t)
	live := EpochRef{Gateway: testredis.Gateway(), Epoch: 1}
	ended := EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	for _, r := range []EpochRef{live, ended} {
		if _, err := s.RenewLease(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EndEpoch(ctx, ended); err != nil {
		t.Fatal(err)
	}
	got, err := s.LeasesExist(ctx, []EpochRef{live, ended})
	if err != nil || !got[0] || got[1] {
		t.Fatalf("LeasesExist = %v, %v; want [true false]", got, err)
	}
	if sc := rdb.ZScore(ctx, epochsKey, ended.member()).Val(); sc != 0 {
		t.Fatalf("ended epoch score = %v, want 0", sc)
	}

	all, err := s.ListEpochs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := map[EpochRef]int64{}
	for _, e := range all {
		found[e.EpochRef] = e.ScoreMs
	}
	if sc, ok := found[ended]; !ok || sc != 0 {
		t.Fatalf("ListEpochs: ended epoch %v, %v", sc, ok)
	}
	if sc, ok := found[live]; !ok || sc <= 0 {
		t.Fatalf("ListEpochs: live epoch %v, %v", sc, ok)
	}
}
