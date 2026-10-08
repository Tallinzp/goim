package gateway

import (
	"context"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// A logoutItem is one fenced Logout. The connection stays tracked until the
// Logout is acknowledged or dropped from the retry queue (spec: Tracked
// connections).
type logoutItem struct {
	ls       *loopState
	connID   uint64
	entry    *pb.LogoutEntry
	first    time.Time // first send, for the RetryFor budget
	attempts int
}

// queueLogout hands a closed connection's Logout to the sender. It runs on
// the loop once the fence holds. A full queue drops it: the next Reconcile
// pass repairs a dropped Logout (R1-O3, R3-O1).
func (g *Gateway) queueLogout(c *Conn, uid string) {
	it := &logoutItem{ls: c.ls, connID: c.id, entry: &pb.LogoutEntry{
		ConnId: c.id, Uid: uid, Appid: c.appid, Platform: c.platform}}
	select {
	case g.logoutCh <- it:
	default:
		g.m.Logouts.WithLabelValues("dropped").Inc()
		g.untrack(c)
	}
}

// startLogout runs the Logout batcher and LogoutRPCs workers.
func (g *Gateway) startLogout(ctx context.Context, logic pb.LogicClient) {
	work := make(chan []*logoutItem)
	g.goRun(func() { batch(ctx, g.logoutCh, work, g.cfg.LogoutBatch, g.cfg.LogoutLinger) })
	for range g.cfg.LogoutRPCs {
		g.goRun(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case b := <-work:
					g.sendLogouts(ctx, logic, b)
				}
			}
		})
	}
}

// sendLogouts sends one batch. An entry whose done is false, or every entry
// when the RPC fails, goes to the retry queue.
func (g *Gateway) sendLogouts(ctx context.Context, logic pb.LogicClient, items []*logoutItem) {
	now := time.Now()
	entries := make([]*pb.LogoutEntry, len(items))
	for i, it := range items {
		if it.first.IsZero() {
			it.first = now
		}
		it.attempts++
		entries[i] = it.entry
	}
	cctx, cancel := g.rpcCtx(ctx, g.cfg.RPCTimeout)
	reply, err := logic.Logout(cctx, &pb.LogoutBatch{Gw: g.ref, Entries: entries})
	cancel()
	var done, retry []*logoutItem
	for i, it := range items {
		if err == nil && i < len(reply.GetDone()) && reply.GetDone()[i] {
			done = append(done, it)
		} else {
			retry = append(retry, it)
		}
	}
	g.m.Logouts.WithLabelValues("done").Add(float64(len(done)))
	g.untrackAll(done)
	if len(retry) > 0 {
		g.retryLogouts(retry)
	}
}

// retryLogouts re-queues items after a backoff (RetryBackoff, doubling, at
// most RetryMaxDelay) while their RetryFor budget lasts, then drops them.
func (g *Gateway) retryLogouts(items []*logoutItem) {
	var again, dropped []*logoutItem
	attempts := 0
	for _, it := range items {
		attempts = max(attempts, it.attempts)
	}
	delay := min(g.cfg.RetryBackoff<<min(attempts-1, 30), g.cfg.RetryMaxDelay)
	for _, it := range items {
		if time.Since(it.first)+delay > g.cfg.RetryFor {
			dropped = append(dropped, it)
		} else {
			again = append(again, it)
		}
	}
	g.m.Logouts.WithLabelValues("dropped").Add(float64(len(dropped)))
	g.untrackAll(dropped)
	if len(again) == 0 {
		return
	}
	g.m.Logouts.WithLabelValues("retried").Add(float64(len(again)))
	time.AfterFunc(delay, func() {
		var full []*logoutItem
		for _, it := range again {
			select {
			case g.logoutCh <- it:
			default:
				full = append(full, it)
			}
		}
		g.m.Logouts.WithLabelValues("dropped").Add(float64(len(full)))
		g.untrackAll(full)
	})
}

// untrackAll ends tracking for settled Logouts, one Execute per loop.
func (g *Gateway) untrackAll(items []*logoutItem) {
	byLoop := make(map[*loopState][]uint64)
	for _, it := range items {
		byLoop[it.ls] = append(byLoop[it.ls], it.connID)
	}
	for ls, ids := range byLoop {
		_ = ls.loop().Execute(func() {
			for _, id := range ids {
				if c := ls.conns[id]; c != nil && c.state == stateClosed && c.logoutQueued {
					g.untrack(c)
				}
			}
		})
	}
}
