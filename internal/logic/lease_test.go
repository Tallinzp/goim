package logic

import (
	"context"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newLeaseService(t *testing.T, workers int) (*LeaseService, *store.Store, *Metrics) {
	t.Helper()
	cfg := testConfig()
	st, _ := newStore(t, cfg)
	m := newMetrics()
	return NewLeaseService(st, NewLeaseCache(st, time.Second, time.Now), m, workers), st, m
}

func TestGatewayLeaseReportsMissingThenLive(t *testing.T) {
	s, _, m := newLeaseService(t, 4)
	gw := &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1 << 63}
	for i, want := range []bool{true, false, false} {
		rep, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: gw, GwNowMs: time.Now().UnixMilli()})
		if err != nil {
			t.Fatal(err)
		}
		if rep.Missing != want {
			t.Fatalf("call %d: missing %v, want %v", i, rep.Missing, want)
		}
	}
	if counter(m.LeaseRenewals.WithLabelValues("missing")) != 1 || counter(m.LeaseRenewals.WithLabelValues("live")) != 2 {
		t.Fatal("renewal counters")
	}
}

func TestGatewayLeaseClockSkew(t *testing.T) {
	s, st, m := newLeaseService(t, 4)
	gwName := testredis.Gateway()
	gw := &pb.GatewayRef{Gateway: gwName, Epoch: 1}
	for _, c := range []struct {
		offsetMs  int64 // gateway clock minus Redis clock
		wantAlert float64
	}{{-2000, 1}, {+2000, 1}, {-100, 0}} {
		now := redisNow(t, st)
		if _, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: gw, GwNowMs: now + c.offsetMs}); err != nil {
			t.Fatal(err)
		}
		skew := counter(m.ClockSkewMs.WithLabelValues(gwName))
		// skew = redis − gw = −offset, plus the few ms the call took.
		if d := skew + float64(c.offsetMs); d < 0 || d > 200 {
			t.Fatalf("offset %d: skew %v", c.offsetMs, skew)
		}
		if a := counter(m.ClockSkewAlert.WithLabelValues(gwName)); a != c.wantAlert {
			t.Fatalf("offset %d: alert %v, want %v", c.offsetMs, a, c.wantAlert)
		}
	}
}

func TestEpochEndedEndsTheLease(t *testing.T) {
	s, st, _ := newLeaseService(t, 4)
	ref := store.EpochRef{Gateway: testredis.Gateway(), Epoch: 5}
	if _, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: pbRef(ref)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.EpochEnded(ctx, &pb.EpochEndedRequest{Gw: pbRef(ref)}); err != nil {
		t.Fatal(err)
	}
	live, err := st.LeasesExist(ctx, []store.EpochRef{ref})
	if err != nil || live[0] {
		t.Fatalf("lease still exists: %v %v", live, err)
	}
	if score, ok, _ := st.EpochScoreOf(ctx, ref); !ok || score != 0 {
		t.Fatalf("gwepochs score %d ok %v, want 0 (ended)", score, ok)
	}
	if l, _ := s.leases.LiveOne(ctx, ref); l {
		t.Fatal("cache still says live")
	}
}

func TestLeaseRejectsBadGateway(t *testing.T) {
	s, _, _ := newLeaseService(t, 4)
	_, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: &pb.GatewayRef{Gateway: "GW|1"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("err %v", err)
	}
}

// The lease pool is separate: a full pool makes lease calls wait, and only
// lease calls (the Login pool is another pool).
func TestLeasePoolBoundsConcurrency(t *testing.T) {
	s, _, _ := newLeaseService(t, 1)
	release, err := s.pool.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = s.GatewayLease(c, &pb.LeaseRequest{Gw: &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1}})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("err %v, want DeadlineExceeded while the pool is full", err)
	}
	release()
	if _, err := s.GatewayLease(ctx, &pb.LeaseRequest{Gw: &pb.GatewayRef{Gateway: testredis.Gateway(), Epoch: 1}}); err != nil {
		t.Fatal(err)
	}
}
