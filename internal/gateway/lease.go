package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// runLease renews the epoch's lease every LeaseInterval on the dedicated
// lease connection (spec: Lease, R1-O2).
func (g *Gateway) runLease(ctx context.Context, lease pb.LeaseClient, trigger func()) {
	t := time.NewTicker(g.cfg.LeaseInterval)
	defer t.Stop()
	var lastOK time.Time // start of the last successful renewal
	for {
		g.renew(ctx, lease, &lastOK, trigger)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// renew makes one GatewayLease call. A pass is triggered when the reply is
// missing, and also when the renewals before this one failed for at least a
// lease TTL: the key may have expired and been swept, and a lost reply may
// have hidden the missing that re-created it. The first successful call after
// boot always finds a new epoch's key absent, so it neither counts nor acts
// on missing (R3-OP5).
func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.Time, trigger func()) {
	start := time.Now()
	cctx, cancel := g.rpcCtx(ctx, g.cfg.LeaseTimeout)
	reply, err := lease.GatewayLease(cctx, &pb.LeaseRequest{Gw: g.ref, GwNowMs: g.now().UnixMilli()})
	cancel()
	if err != nil {
		g.m.LeaseCalls.WithLabelValues("error").Inc()
		if ctx.Err() == nil {
			g.log.Warn("lease renewal failed", "err", err)
		}
		return
	}
	first := lastOK.IsZero()
	gap := time.Since(*lastOK)
	*lastOK = start
	if first {
		g.m.LeaseCalls.WithLabelValues("first").Inc()
		g.leaseOK.Store(true)
		return
	}
	if reply.GetMissing() {
		g.m.LeaseCalls.WithLabelValues("missing").Inc()
	} else {
		g.m.LeaseCalls.WithLabelValues("live").Inc()
	}
	if reply.GetMissing() || gap >= g.cfg.LeaseTTL {
		trigger()
	}
}
