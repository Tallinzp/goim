package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// runLease renews the epoch's lease every LeaseInterval on the dedicated
// lease connection (spec: Lease, R1-O2). A failed call is retried after
// LeaseRetry, not at the next tick: the key then outlives a Redis shard
// failover of up to TTL − LeaseInterval (the spec's lease rule, R2-O5), not
// TTL − 2 × LeaseInterval (#139). It returns when stop is closed,
// after the renewal in flight (if any) has finished, so EpochEnded can follow
// without a renewal re-creating the key behind it.
func (g *Gateway) runLease(ctx context.Context, stop <-chan struct{}, lease pb.LeaseClient, trigger func()) {
	t := time.NewTicker(g.cfg.LeaseInterval)
	defer t.Stop()
	var lastOK time.Time // start of the last successful renewal
	for {
		select {
		case <-stop:
			return
		default:
		}
		next := t.C
		if !g.renew(ctx, lease, &lastOK, trigger) {
			next = time.After(g.cfg.LeaseRetry)
		}
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-next:
		}
	}
}

// renew makes one GatewayLease call. A pass is triggered when the reply is
// missing, and also when the renewals before this one failed for at least a
// lease TTL: the key may have expired and been swept, and a lost reply may
// have hidden the missing that re-created it. The first successful call after
// boot always finds a new epoch's key absent, so it neither counts nor acts
// on missing (R3-OP5). It reports whether the call succeeded.
func (g *Gateway) renew(ctx context.Context, lease pb.LeaseClient, lastOK *time.Time, trigger func()) bool {
	start := time.Now()
	cctx, cancel := g.rpcCtx(ctx, g.cfg.LeaseTimeout)
	reply, err := lease.GatewayLease(cctx, &pb.LeaseRequest{Gw: g.ref, GwNowMs: g.now().UnixMilli()})
	cancel()
	if err != nil {
		g.m.LeaseCalls.WithLabelValues("error").Inc()
		if ctx.Err() == nil {
			g.log.Warn("lease renewal failed", "err", err)
		}
		return false
	}
	first := lastOK.IsZero()
	gap := time.Since(*lastOK)
	*lastOK = start
	if first {
		g.m.LeaseCalls.WithLabelValues("first").Inc()
		g.leaseOK.Store(true)
		return true
	}
	if reply.GetMissing() {
		g.m.LeaseCalls.WithLabelValues("missing").Inc()
	} else {
		g.m.LeaseCalls.WithLabelValues("live").Inc()
	}
	if reply.GetMissing() || gap >= g.cfg.LeaseTTL {
		trigger()
	}
	return true
}
