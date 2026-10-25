package gateway

import (
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

func TestFirstLeaseMissingIsIgnoredAndALaterOneTriggersAPass(t *testing.T) {
	l := newLive(t, nil)
	missing := make(chan bool, 1)
	l.logic.set(func() {
		l.logic.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) {
			select {
			case m := <-missing:
				return &pb.LeaseReply{Missing: m}, nil
			default:
				return &pb.LeaseReply{}, nil
			}
		}
	})
	// newLive's first call already reported missing (the default handler).
	waitFor(t, "lease calls", func() bool { return get(l.logic, func() int { return len(l.logic.leaseCalls) }) >= 3 })
	if passes(l) != 0 {
		t.Fatal("the first lease call after boot triggered a pass (R3-OP5)")
	}
	if !l.g.leaseOK.Load() {
		t.Fatal("the first successful lease call did not count for readiness")
	}
	missing <- true
	waitFor(t, "triggered pass", func() bool { return passes(l) == 1 })
	end := get(l.logic, func() *pb.ReconcileEndRequest { return l.logic.ends[0] })
	if end.GetPace() != pb.Pace_PACE_TRIGGERED {
		t.Fatalf("pace %v, want triggered", end.GetPace())
	}
}

func TestLeaseCallCarriesTheGatewayClock(t *testing.T) {
	l := newLive(t, nil)
	l.clock.jump(2 * time.Second) // a skewed gateway (R4M-5)
	waitFor(t, "a call after the jump", func() bool {
		return get(l.logic, func() bool {
			if len(l.logic.leaseCalls) == 0 {
				return false
			}
			r := l.logic.leaseCalls[len(l.logic.leaseCalls)-1]
			return r.GetGwNowMs() > time.Now().Add(time.Second).UnixMilli()
		})
	})
}

func TestRenewalAfterFailuresForALeaseTTLTriggersAPass(t *testing.T) {
	l := newLive(t, nil) // LeaseTTL 300ms, interval 100ms
	failing := make(chan bool, 1)
	failing <- false
	l.logic.set(func() {
		l.logic.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) {
			f := <-failing
			failing <- f
			if f {
				return nil, errors.New("Logic unreachable")
			}
			return &pb.LeaseReply{}, nil // live: a lost reply may have hidden the missing
		}
	})
	waitFor(t, "first lease", l.g.leaseOK.Load)
	time.Sleep(150 * time.Millisecond)
	if passes(l) != 0 {
		t.Fatal("live replies triggered a pass")
	}
	<-failing
	failing <- true
	time.Sleep(400 * time.Millisecond) // longer than the lease TTL
	<-failing
	failing <- false
	waitFor(t, "triggered pass", func() bool { return passes(l) >= 1 })
}

// A failed renewal is retried after LeaseRetry, not at the next tick, so a
// lease outlives a Redis failover of up to TTL − interval (spec: the lease
// rule, R2-O5; roadmap #139). Back on the tick once a call succeeds.
func TestFailedRenewalIsRetriedBeforeTheNextTick(t *testing.T) {
	fails := 3
	l := startLive(t, func(c *Config) {
		c.LeaseInterval, c.LeaseTTL, c.LeaseRetry = time.Hour, 2*time.Hour, 20*time.Millisecond
	},
		func(_ *Gateway, f *fakeLogic) {
			f.lease = func(*pb.LeaseRequest) (*pb.LeaseReply, error) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if fails > 0 {
					fails--
					return nil, errors.New("shard failing over")
				}
				return &pb.LeaseReply{Missing: true}, nil // a new epoch's key is absent
			}
		})
	waitFor(t, "first lease after 3 failed calls", l.g.leaseOK.Load)
	time.Sleep(200 * time.Millisecond) // 10 × LeaseRetry
	if n := get(l.logic, func() int { return len(l.logic.leaseCalls) }); n != 4 {
		t.Fatalf("%d lease calls, want 4: 3 failed and retried, then the hourly tick", n)
	}
	if got := testutil.ToFloat64(l.g.m.LeaseCalls.WithLabelValues("error")); got != 3 {
		t.Fatalf("lease_calls_total{result=error} %v, want 3", got)
	}
}
