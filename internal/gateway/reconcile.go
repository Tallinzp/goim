package gateway

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// chunkRec is a Reconcile chunk sent from a loop and not yet resolved. Its
// connections carry inChunk (fence c) until it is.
type chunkRec struct {
	ids   []uint64
	timer timer // sent_ms + login_deadline + 1s
}

// passItem is one pass-queue entry: only the ID, rebuilt from the live table
// when its chunk is built (spec: Pass mechanics).
type passItem struct {
	id       uint64
	requeues uint8
}

// chunkDone tells the controller a chunk was resolved on its loop.
type chunkDone struct {
	chunk   uint64
	refused []uint64
}

// built is a chunk read from a loop's table.
type built struct {
	ids     []uint64
	entries []*pb.ReconcileEntry
	sentMs  int64
}

// flight is the controller's record of a chunk in flight.
type flight struct {
	part     int
	requeues map[uint64]uint8
}

// reconciler is the pass controller goroutine (spec: Reconcile, R4H-O9). It
// is the only code that schedules table work for a pass, always through
// Execute, so every table access stays on its loop.
type reconciler struct {
	g        *Gateway
	logic    pb.LogicClient
	trigger  chan struct{}
	resolved chan chunkDone
	chunkSeq uint64
	ends     sync.WaitGroup
}

func newReconciler(g *Gateway, logic pb.LogicClient) *reconciler {
	return &reconciler{g: g, logic: logic, trigger: make(chan struct{}, 1),
		resolved: make(chan chunkDone, g.cfg.Reconcile.MaxInFlight)}
}

// Trigger asks for a triggered pass (a missing lease reply). Calls coalesce.
func (r *reconciler) Trigger() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

// run schedules passes: periodic every Cycle ± Jitter, first at a random
// point of the cycle so gateways spread out; triggered on demand. One pass
// at a time (spec: One pass at a time).
func (r *reconciler) run(ctx context.Context) {
	cfg := r.g.cfg.Reconcile
	next := time.NewTimer(rand.N(cfg.Cycle))
	defer next.Stop()
	pace := pb.Pace_PACE_UNSPECIFIED
	for {
		if pace == pb.Pace_PACE_UNSPECIFIED {
			select {
			case <-ctx.Done():
				return
			case <-r.trigger:
				pace = pb.Pace_PACE_TRIGGERED
			case <-next.C:
				pace = pb.Pace_PACE_PERIODIC
			}
		}
		started := time.Now()
		again := r.pass(ctx, pace)
		if ctx.Err() != nil {
			return
		}
		if pace == pb.Pace_PACE_PERIODIC {
			jitter := time.Duration(0)
			if cfg.Jitter > 0 {
				jitter = rand.N(2*cfg.Jitter) - cfg.Jitter
			}
			next.Reset(max(0, time.Until(started.Add(cfg.Cycle+jitter))))
		}
		pace = pb.Pace_PACE_UNSPECIFIED
		if again {
			pace = pb.Pace_PACE_TRIGGERED
		}
	}
}

// pass runs one pass. It returns true when a triggered pass must follow: a
// trigger during a periodic pass cancels it; one during a triggered pass is
// served by another triggered pass once this one ends, since the sweep that
// caused it may have run after this pass read those entries.
func (r *reconciler) pass(ctx context.Context, pace pb.Pace) (again bool) {
	g, cfg := r.g, r.g.cfg.Reconcile
	start := time.Now()
	loops := g.loops.bound()
	parts, ok := r.snapshot(ctx, loops)
	if !ok {
		return false
	}
	rate := float64(cfg.TriggeredRate)
	if pace == pb.Pace_PACE_PERIODIC {
		total := 0
		for _, p := range parts {
			total += len(p)
		}
		// Spread over 90% of the cycle, leaving room for ReconcileEnd.
		rate = min(max(float64(total)/(0.9*cfg.Cycle.Seconds()), float64(cfg.MinRate)), rate)
	}
	perEntry := time.Duration(float64(time.Second) / rate)

	inflight := make(map[uint64]flight)
	cancelled := false
	cursor := 0
	due := time.Now()
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		work := !cancelled && hasWork(parts)
		if !work && len(inflight) == 0 {
			break
		}
		var wake <-chan time.Time
		if work && len(inflight) < cfg.MaxInFlight {
			if d := time.Until(due); d > 0 {
				t.Reset(d)
				wake = t.C
			} else {
				// One loop's partition per chunk, loops in turn (round-robin).
				for len(parts[cursor%len(parts)]) == 0 {
					cursor++
				}
				pi := cursor % len(parts)
				cursor++
				n := min(cfg.Chunk, len(parts[pi]))
				take := parts[pi][:n:n]
				parts[pi] = parts[pi][n:]
				// Re-queued entries count against the rate too (R4G-O8).
				if now := time.Now(); due.Before(now) {
					due = now
				}
				due = due.Add(time.Duration(n) * perEntry)
				r.send(ctx, loops[pi], pi, take, inflight)
				continue
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-wake:
		case d := <-r.resolved:
			f, ok := inflight[d.chunk]
			if !ok {
				continue
			}
			delete(inflight, d.chunk)
			if cancelled {
				continue
			}
			for _, id := range d.refused {
				if rq := f.requeues[id]; int(rq) < cfg.MaxRequeues {
					parts[f.part] = append(parts[f.part], passItem{id: id, requeues: rq + 1})
					g.m.Requeued.Inc()
				} else {
					g.m.RequeueLimit.Inc()
				}
			}
		case <-r.trigger:
			again = true
			if pace == pb.Pace_PACE_PERIODIC {
				cancelled = true // no more chunks, no ReconcileEnd
			}
		}
	}
	paceLabel := pace.String()
	if cancelled {
		g.m.Passes.WithLabelValues(paceLabel, "cancelled").Inc()
		return true
	}
	r.end(ctx, pace)
	g.m.PassSeconds.WithLabelValues(paceLabel).Observe(time.Since(start).Seconds())
	return again
}

func hasWork(parts [][]passItem) bool {
	for _, p := range parts {
		if len(p) > 0 {
			return true
		}
	}
	return false
}

// snapshot copies each loop's ONLINE conn_ids into its partition, one Execute
// per loop (spec: Key snapshot).
func (r *reconciler) snapshot(ctx context.Context, loops []*loopState) ([][]passItem, bool) {
	return gather(ctx, loops, func(ls *loopState) []passItem {
		items := make([]passItem, 0, len(ls.conns))
		for id, c := range ls.conns {
			if c.state == stateOnline && !c.closing {
				items = append(items, passItem{id: id})
			}
		}
		return items
	})
}

// gather runs fn on every loop and waits for all of them. It fails if a loop
// has shut down.
func gather[T any](ctx context.Context, loops []*loopState, fn func(*loopState) T) ([]T, bool) {
	out := make([]T, len(loops))
	type res struct {
		i int
		v T
	}
	ch := make(chan res, len(loops))
	for i, ls := range loops {
		if ls.loop().Execute(func() { ch <- res{i, fn(ls)} }) != nil {
			return nil, false
		}
	}
	for range loops {
		select {
		case x := <-ch:
			out[x.i] = x.v
		case <-ctx.Done():
			return nil, false
		}
	}
	return out, true
}

// send builds a chunk on its loop (one Execute) and, if it holds anything,
// sends it.
func (r *reconciler) send(ctx context.Context, ls *loopState, part int, take []passItem, inflight map[uint64]flight) {
	r.chunkSeq++
	id := r.chunkSeq
	got := make(chan built, 1)
	if ls.loop().Execute(func() { got <- r.g.buildChunk(ls, id, take) }) != nil {
		return
	}
	var b built
	select {
	case b = <-got:
	case <-ctx.Done():
		return
	}
	if len(b.ids) == 0 {
		return
	}
	rq := make(map[uint64]uint8, len(take))
	for _, it := range take {
		rq[it.id] = it.requeues
	}
	inflight[id] = flight{part: part, requeues: rq}
	go r.call(ctx, ls, id, b)
}

// buildChunk runs on the loop: it skips IDs no longer ONLINE, copies the
// current fields, marks each connection inChunk and arms the chunk deadline.
func (g *Gateway) buildChunk(ls *loopState, id uint64, items []passItem) built {
	sent := g.now()
	b := built{sentMs: sent.UnixMilli()}
	for _, it := range items {
		c := ls.conns[it.id]
		if c == nil || c.state != stateOnline || c.closing || c.inChunk {
			continue
		}
		c.inChunk = true
		b.ids = append(b.ids, c.id)
		b.entries = append(b.entries, &pb.ReconcileEntry{ConnId: c.id, Uid: c.uid, Appid: c.appid,
			Platform: c.platform, DeviceId: c.deviceID, ConnSeq: c.connSeq, LoginMs: c.recvMs, TokIatMs: c.tokIatMs})
	}
	if len(b.ids) > 0 {
		rec := &chunkRec{ids: b.ids}
		ls.chunks[id] = rec
		// No definitive reply by sent_ms + login_deadline + 1s: the
		// compare-and-set can no longer write, so the chunk is resolved.
		ls.wheel.schedule(&rec.timer, sent.Add(g.cfg.FenceAfter()), func() { g.resolveChunk(ls, id, nil, nil) })
	}
	return b
}

// call sends the chunk. A reply is resolved on the loop; an error is not a
// definitive reply, so the chunk waits for its deadline on the wheel.
func (r *reconciler) call(ctx context.Context, ls *loopState, id uint64, b built) {
	g := r.g
	timeout := time.Duration(b.sentMs+g.cfg.FenceAfter().Milliseconds()-g.now().UnixMilli()) * time.Millisecond
	cctx, cancel := g.rpcCtx(ctx, timeout)
	reply, err := r.logic.Reconcile(cctx, &pb.ReconcileChunk{Gw: g.ref, SentMs: b.sentMs, Entries: b.entries})
	cancel()
	if err != nil || len(reply.GetResults()) != len(b.entries) {
		g.m.ReconcileEntries.WithLabelValues("no_reply").Add(float64(len(b.entries)))
		return
	}
	for _, res := range reply.GetResults() {
		g.m.ReconcileEntries.WithLabelValues(resultLabel(res.GetCode())).Inc()
		if res.GetCode() == pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER {
			g.m.ReconcileNotOwner.WithLabelValues(res.GetReason().String()).Inc()
		}
	}
	_ = ls.loop().Execute(func() { g.resolveChunk(ls, id, b.ids, reply.GetResults()) })
}

func resultLabel(c pb.ReconcileCode) string {
	switch c {
	case pb.ReconcileCode_RECONCILE_CODE_WRITTEN:
		return "written"
	case pb.ReconcileCode_RECONCILE_CODE_UNCHANGED:
		return "unchanged"
	case pb.ReconcileCode_RECONCILE_CODE_REFUSED:
		return "refused"
	case pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER:
		return "not_owner"
	}
	return "unknown"
}

// resolveChunk runs on the loop when a chunk gets its reply (results) or its
// deadline passes (results nil): it clears inChunk, Kicks and closes
// not_owner connections, releases Logouts fence (c) held, and tells the
// controller which entries were refused. A reply that arrives after the
// deadline still Kicks its not_owner connections, and does nothing else.
func (g *Gateway) resolveChunk(ls *loopState, id uint64, ids []uint64, results []*pb.ReconcileResult) {
	rec, live := ls.chunks[id]
	if live {
		delete(ls.chunks, id)
		ls.wheel.stop(&rec.timer)
		ids = rec.ids
	}
	var refused []uint64
	for i, cid := range ids {
		c := ls.conns[cid]
		if c == nil {
			continue
		}
		if live {
			c.inChunk = false
		}
		if i < len(results) {
			switch res := results[i]; res.GetCode() {
			case pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER:
				if c.state == stateOnline && !c.closing {
					g.kickConn(c, clientpb.KickReason(res.GetReason()), "not_owner")
				}
			case pb.ReconcileCode_RECONCILE_CODE_REFUSED:
				if live {
					refused = append(refused, cid)
				}
			}
		}
		if live {
			g.settle(c)
		}
	}
	if live && g.recon != nil {
		select {
		case g.recon.resolved <- chunkDone{chunk: id, refused: refused}:
		default: // unreachable: at most MaxInFlight chunks are unresolved
			g.log.Error("reconcile: resolution channel full", "chunk", id)
		}
	}
}

// end finishes a pass: stamp as_of_ms, then collect every tracked conn_id,
// one Execute per loop, each taken after as_of_ms (spec: Tracked-set
// collection), and send ReconcileEnd. A loop that can't be read means an
// incomplete set, which would delete live sessions: no ReconcileEnd then.
func (r *reconciler) end(ctx context.Context, pace pb.Pace) {
	g := r.g
	asOf := g.now().UnixMilli()
	loops := g.loops.bound() // read after as_of: a loop bound later only has Logins received after it
	frags, ok := gather(ctx, loops, func(ls *loopState) []uint64 {
		ids := make([]uint64, 0, len(ls.conns))
		for id := range ls.conns {
			ids = append(ids, id)
		}
		return ids
	})
	if !ok {
		g.m.Passes.WithLabelValues(pace.String(), "incomplete").Inc()
		return
	}
	var tracked []uint64
	for _, f := range frags {
		tracked = append(tracked, f...)
	}
	g.m.Passes.WithLabelValues(pace.String(), "done").Inc()
	req := &pb.ReconcileEndRequest{Gw: g.ref, AsOfMs: asOf, TrackedConnIds: tracked, Pace: pace}
	r.ends.Add(1)
	go func() {
		defer r.ends.Done()
		r.sendEnd(ctx, req)
	}()
}

// sendEnd sends ReconcileEnd and retries the same payload for up to RetryFor,
// then drops it; the next pass covers what it missed (R3-OP4). The deadline
// covers Logic's rate-limited delete phase (R5S-O8).
func (r *reconciler) sendEnd(ctx context.Context, req *pb.ReconcileEndRequest) {
	g := r.g
	first := time.Now()
	delay := g.cfg.RetryBackoff
	n := len(req.GetTrackedConnIds())
	timeout := 2*time.Duration(n)*time.Second/time.Duration(g.cfg.Reconcile.DeleteRate) + 10*time.Second
	for {
		cctx, cancel := g.rpcCtx(ctx, timeout)
		reply, err := r.logic.ReconcileEnd(cctx, req)
		cancel()
		if err == nil {
			g.m.ReconcileEnds.WithLabelValues("ok").Inc()
			g.m.EndDeleted.Add(float64(reply.GetDeleted()))
			g.m.EndTracked.Set(float64(n))
			return
		}
		if ctx.Err() != nil {
			return
		}
		if time.Since(first)+delay > g.cfg.RetryFor {
			g.m.ReconcileEnds.WithLabelValues("dropped").Inc()
			g.log.Warn("reconcile: ReconcileEnd dropped", "as_of_ms", req.GetAsOfMs(), "err", err)
			return
		}
		g.m.ReconcileEnds.WithLabelValues("retried").Inc()
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(2*delay, g.cfg.RetryMaxDelay)
	}
}
