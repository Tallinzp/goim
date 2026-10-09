package gateway

import "time"

// Timing wheel (spec: Timers, R4H-O6): one per loop, touched only on the
// loop's goroutine, so it needs no lock and its callbacks may use the loop's
// connection table. A 1s tick and 512 slots; a timer stores its absolute
// tick, which plays the role of the round counter for delays past 512s.
const (
	wheelTick  = time.Second
	wheelSlots = 512
)

// timer is embedded in its owner (a connection or a chunk), so arming one
// allocates nothing. The zero value is a stopped timer.
type timer struct {
	at         int64 // tick index; fires when the wheel reaches it
	fn         func()
	prev, next *timer
	armed      bool // in a slot
	firing     bool // taken out by advance, not run yet
}

type wheel struct {
	tickMs int64
	cur    int64 // last tick processed
	heads  [wheelSlots]*timer
	due    []*timer // scratch for advance
	n      int      // armed timers
}

func newWheel(now time.Time) *wheel {
	w := &wheel{tickMs: wheelTick.Milliseconds()}
	w.cur = now.UnixMilli() / w.tickMs
	return w
}

// schedule arms t to run fn at the first tick at or after at: deadlines round
// up, so a timer never fires early. It fires at most one tick late, because
// runTicker advances the wheel just after each tick begins (untilNextTick).
// An armed t is moved.
func (w *wheel) schedule(t *timer, at time.Time, fn func()) {
	w.stop(t)
	ms := at.UnixMilli()
	tick := ms / w.tickMs
	if ms%w.tickMs != 0 {
		tick++
	}
	if tick <= w.cur {
		tick = w.cur + 1
	}
	t.at, t.fn, t.armed = tick, fn, true
	h := &w.heads[tick%wheelSlots]
	t.prev, t.next = nil, *h
	if *h != nil {
		(*h).prev = t
	}
	*h = t
	w.n++
}

// stop disarms t, also when advance has taken it out but not yet run it
// (another callback of the same advance stops it); stopping a stopped timer
// is a no-op.
func (w *wheel) stop(t *timer) {
	if t.firing {
		t.firing, t.fn = false, nil
	}
	if !t.armed {
		return
	}
	if t.prev != nil {
		t.prev.next = t.next
	} else {
		w.heads[t.at%wheelSlots] = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	}
	t.prev, t.next, t.fn, t.armed = nil, nil, nil, false
	w.n--
}

// advance runs every timer whose tick is at or before now's tick. A callback
// that arms a timer lands on a later tick, never in this advance.
func (w *wheel) advance(now time.Time) {
	target := now.UnixMilli() / w.tickMs
	if target <= w.cur {
		return
	}
	steps := min(target-w.cur, wheelSlots) // a jump past one turn visits every slot once
	for k := target - steps + 1; k <= target; k++ {
		for t := w.heads[k%wheelSlots]; t != nil; {
			next := t.next
			if t.at <= target {
				fn := t.fn
				w.stop(t)
				t.fn, t.firing = fn, true
				w.due = append(w.due, t)
			}
			t = next
		}
	}
	w.cur = target
	due := w.due
	for i, t := range due {
		due[i] = nil
		if !t.firing {
			continue // stopped or re-armed by an earlier callback
		}
		fn := t.fn
		t.firing, t.fn = false, nil
		fn()
	}
	w.due = due[:0]
}

// untilNextTick is how long from now until the next whole tick of the clock.
// The ticker waits that long, so advance runs just after each boundary and a
// timer fires at most one tick after its deadline (the deadline rounds up to
// a tick, and that tick is processed as soon as it begins).
func untilNextTick(now time.Time) time.Duration {
	ms := now.UnixMilli() % wheelTick.Milliseconds()
	return wheelTick - time.Duration(ms)*time.Millisecond
}
