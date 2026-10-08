package logic

import (
	"context"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// LeaseService serves GatewayLease and EpochEnded on Logic's dedicated lease
// port, with its own worker pool (R1-O2).
type LeaseService struct {
	pb.UnimplementedLeaseServer
	st     Store
	leases *LeaseCache
	m      *Metrics
	pool   pool
}

// NewLeaseService returns the lease handlers; workers bounds concurrent calls.
func NewLeaseService(st Store, leases *LeaseCache, m *Metrics, workers int) *LeaseService {
	return &LeaseService{st: st, leases: leases, m: m, pool: newPool(workers)}
}

// GatewayLease renews the caller's lease and reports whether the key was
// missing before (R3-O3). It also records clock_skew_ms = Redis TIME on the
// lease shard − gw_now_ms (R4M-5, PI-5).
func (s *LeaseService) GatewayLease(ctx context.Context, req *pb.LeaseRequest) (*pb.LeaseReply, error) {
	ref, err := epochRef(req.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	res, err := s.st.RenewLease(ctx, ref)
	if err != nil {
		s.m.LeaseRenewals.WithLabelValues("error").Inc()
		return nil, unavailable("lease renewal: %v", err)
	}
	s.leases.Set(ref, true)
	if res.IndexErr != nil {
		s.m.LeaseIndexFailures.Inc()
		logger(ctx).Warn("gwepochs update failed after a lease renewal", "gw", ref.Gateway, "epoch", ref.Epoch, "err", res.IndexErr)
	}
	skew := res.RedisNowMs - req.GetGwNowMs()
	s.m.ClockSkewMs.WithLabelValues(ref.Gateway).Set(float64(skew))
	alert := 0.0
	if skew > SkewAlertMs || skew < -SkewAlertMs {
		alert = 1
	}
	s.m.ClockSkewAlert.WithLabelValues(ref.Gateway).Set(alert)
	if res.Missing {
		s.m.LeaseRenewals.WithLabelValues("missing").Inc()
	} else {
		s.m.LeaseRenewals.WithLabelValues("live").Inc()
	}
	return &pb.LeaseReply{Missing: res.Missing}, nil
}

// EpochEnded deletes the drained epoch's lease key, so its entries are
// invalid at once, and marks it ended for the sweeper (roadmap choice #2).
func (s *LeaseService) EpochEnded(ctx context.Context, req *pb.EpochEndedRequest) (*pb.EpochEndedReply, error) {
	ref, err := epochRef(req.GetGw())
	if err != nil {
		return nil, invalid("%v", err)
	}
	release, err := s.pool.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.st.EndEpoch(ctx, ref); err != nil {
		return nil, unavailable("epoch ended: %v", err)
	}
	s.leases.Set(ref, false)
	return &pb.EpochEndedReply{}, nil
}
