package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// Drain is the SIGTERM path (spec: Graceful drain): go NotReady and refuse
// new connections, stop Reconcile, send Reconnect{jitter_ms} to the ONLINE
// connections in batches over Drain.Window, close the rest, wait for their
// fences and Logouts (at most login_deadline + 1s + Drain.Flush), then call
// EpochEnded. Kicks are still served throughout.
func (g *Gateway) Drain(ctx context.Context) {
	g.draining.Store(true)
	g.m.Ready.Set(0)
	if g.stopRecon != nil {
		g.stopRecon()
	}
	loops := g.loops.bound()
	parts, ok := gather(ctx, loops, func(ls *loopState) []uint64 {
		ids := make([]uint64, 0, len(ls.conns))
		for id, c := range ls.conns {
			if c.state == stateOnline && !c.closing {
				ids = append(ids, id)
			}
		}
		return ids
	})
	if ok {
		steps := max(1, int(g.cfg.Drain.Window/time.Second))
		t := time.NewTicker(g.cfg.Drain.Window / time.Duration(steps))
		for s := 0; s < steps; s++ {
			for i, ls := range loops {
				n := (len(parts[i]) + steps - s - 1) / (steps - s)
				take := parts[i][:n]
				parts[i] = parts[i][n:]
				_ = ls.loop().Execute(func() {
					for _, id := range take {
						if c := ls.conns[id]; c != nil && c.state == stateOnline && !c.closing {
							g.reconnect(c)
						}
					}
				})
			}
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		t.Stop()
	}
	// Close what is left, in any state.
	gather(ctx, g.loops.bound(), func(ls *loopState) struct{} {
		for _, c := range ls.conns {
			if c.state != stateClosed {
				g.closeConn(c, "drain")
			}
		}
		return struct{}{}
	})
	// Wait until no Login or Reconcile write for this epoch can still land
	// and the Logouts are sent: EpochEnded lets the sweeper delete the
	// epoch's fields at once.
	deadline := time.Now().Add(g.cfg.FenceAfter() + g.cfg.Drain.Flush)
	for g.tracked.Load() > 0 && time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
	// A renewal after EpochEnded would re-create the lease key and undo the
	// ended mark, so stop the lease loop and wait for its last call.
	if g.stopLease != nil {
		g.stopLease()
		select {
		case <-g.leaseDone:
		case <-ctx.Done():
			return
		}
	}
	g.endEpoch(ctx)
}

// endEpoch calls EpochEnded on the lease connection, a few times on error.
func (g *Gateway) endEpoch(ctx context.Context) {
	for attempt := range 3 {
		cctx, cancel := g.rpcCtx(ctx, g.cfg.LeaseTimeout)
		_, err := g.leaseC.EpochEnded(cctx, &pb.EpochEndedRequest{Gw: g.ref})
		cancel()
		if err == nil {
			return
		}
		g.log.Warn("EpochEnded failed", "attempt", attempt+1, "err", err)
	}
}
