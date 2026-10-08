package gateway

import (
	"sync"
	"sync/atomic"
	"time"
)

// Loop is a transport's event loop: the goroutine that owns a set of
// connections. Execute queues fn to run on that goroutine; it is safe from any
// goroutine and never blocks. It does not keep order between calls: gnet
// v2.10.0 moves low-priority tasks to a second queue under load, so a later
// task can run first. It fails only once the loop has shut down.
type Loop interface {
	Execute(fn func()) error
}

// loopState is one loop's share of the gateway: its worker_id, conn_id
// counter, connection table and timing wheel. Every
// field but bound is touched only on the loop's goroutine, so none needs a
// lock (spec: Per-loop connection tables).
type loopState struct {
	workerID uint64
	bound    atomic.Pointer[boundLoop] // set once, by the first Open on the loop

	counter uint64 // conn_id low 48 bits; only Open touches it
	conns   map[uint64]*Conn
	wheel   *wheel
}

type boundLoop struct{ l Loop }

// loop returns the bound Loop; call only after the loopState has been bound.
func (ls *loopState) loop() Loop { return ls.bound.Load().l }

// nextID returns a new conn_id: worker_id << 48 | counter (spec: Identity).
// IDs start at 1 and are never reused within an epoch.
func (ls *loopState) nextID() uint64 {
	ls.counter++
	return ls.workerID<<48 | ls.counter
}

// loops holds every loopState, built at boot (R4H-O5). A transport can't list
// its loops at boot (gnet has no such call), so the first Open on a loop
// claims the next loopState and caches the binding.
type loops struct {
	states  []*loopState
	claimed atomic.Int64
	byLoop  sync.Map // Loop → *loopState
}

// tableHint pre-sizes each loop's table so it does not grow during a storm:
// cap / loops × 1.5 (R4H-O4). It is a hint, not a limit.
func tableHint(cfg Config) int { return cfg.MaxConns / cfg.NumLoops * 3 / 2 }

func newLoops(cfg Config, now time.Time) *loops {
	ls := &loops{states: make([]*loopState, cfg.NumLoops)}
	for i := range ls.states {
		ls.states[i] = &loopState{
			workerID: uint64(i),
			conns:    make(map[uint64]*Conn, tableHint(cfg)),
			wheel:    newWheel(now),
		}
	}
	return ls
}

// bind returns l's loopState, claiming the next free one the first time l is
// seen. It runs on l's goroutine. More loops than NumLoops is a startup bug.
func (ls *loops) bind(l Loop) *loopState {
	if s, ok := ls.byLoop.Load(l); ok {
		return s.(*loopState)
	}
	i := ls.claimed.Add(1) - 1
	if i >= int64(len(ls.states)) {
		panic("gateway: an event loop beyond NumLoops opened a connection")
	}
	s := ls.states[i]
	s.bound.Store(&boundLoop{l})
	ls.byLoop.Store(l, s)
	return s
}

// bound returns the loopStates bound so far.
func (ls *loops) bound() []*loopState {
	n := min(ls.claimed.Load(), int64(len(ls.states)))
	out := make([]*loopState, 0, n)
	for _, s := range ls.states[:n] {
		if s.bound.Load() != nil {
			out = append(out, s)
		}
	}
	return out
}

// byWorker returns the bound loopState for a worker_id, or nil.
func (ls *loops) byWorker(w uint64) *loopState {
	if w >= uint64(len(ls.states)) {
		return nil
	}
	if s := ls.states[w]; s.bound.Load() != nil {
		return s
	}
	return nil
}
