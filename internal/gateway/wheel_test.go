package gateway

import (
	"testing"
	"time"
)

var t0 = time.UnixMilli(1_760_000_000_000) // on a tick boundary

func TestWheelNeverFiresEarly(t *testing.T) {
	for _, tc := range []struct {
		name     string
		start    time.Duration // wheel's now, after t0
		deadline time.Duration // after t0
	}{
		{"on a boundary", 0, 5 * time.Second},
		{"between ticks", 300 * time.Millisecond, 5*time.Second + 300*time.Millisecond},
		{"1ms past a boundary", 0, 5*time.Second + time.Millisecond},
		{"past one turn of the wheel", 0, 600 * time.Second},
		{"in the past", 2 * time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWheel(t0.Add(tc.start))
			var tm timer
			var firedAt time.Time
			now := t0.Add(tc.start)
			w.schedule(&tm, t0.Add(tc.deadline), func() { firedAt = now })
			for step := 0; firedAt.IsZero() && step < 10_000; step++ {
				now = now.Add(100 * time.Millisecond)
				w.advance(now)
			}
			if firedAt.IsZero() {
				t.Fatal("never fired")
			}
			if firedAt.Before(t0.Add(tc.deadline)) {
				t.Fatalf("fired at %v, before its deadline %v", firedAt.Sub(t0), tc.deadline)
			}
			if late := firedAt.Sub(t0.Add(tc.deadline)); tc.deadline > tc.start && late > wheelTick {
				t.Fatalf("fired %v late, more than one tick", late)
			}
		})
	}
}

func TestWheelLongDelayWaitsForItsRound(t *testing.T) {
	w := newWheel(t0)
	var tm timer
	fired := false
	w.schedule(&tm, t0.Add(600*time.Second), func() { fired = true }) // slot 600 % 512 = 88
	w.advance(t0.Add(88 * time.Second))
	w.advance(t0.Add(599 * time.Second))
	if fired {
		t.Fatal("fired on an earlier turn of the wheel")
	}
	w.advance(t0.Add(600 * time.Second))
	if !fired {
		t.Fatal("did not fire at 600s")
	}
}

func TestWheelStopAndRearm(t *testing.T) {
	w := newWheel(t0)
	var a, b timer
	var got []string
	w.schedule(&a, t0.Add(5*time.Second), func() { got = append(got, "a") })
	w.schedule(&b, t0.Add(5*time.Second), func() { got = append(got, "b") })
	w.stop(&a)
	w.stop(&a) // no-op
	w.schedule(&b, t0.Add(7*time.Second), func() { got = append(got, "b2") })
	w.advance(t0.Add(6 * time.Second))
	if len(got) != 0 {
		t.Fatalf("fired %v", got)
	}
	w.advance(t0.Add(7 * time.Second))
	if len(got) != 1 || got[0] != "b2" {
		t.Fatalf("got %v, want [b2]", got)
	}
	if w.n != 0 {
		t.Fatalf("%d timers still armed", w.n)
	}
}

func TestWheelCallbackStopsAnotherDueTimer(t *testing.T) {
	w := newWheel(t0)
	var a, b timer
	var got []string
	// Both are due in the same advance; whichever runs first stops the other.
	w.schedule(&a, t0.Add(time.Second), func() { got = append(got, "a"); w.stop(&b) })
	w.schedule(&b, t0.Add(time.Second), func() { got = append(got, "b"); w.stop(&a) })
	w.advance(t0.Add(time.Second))
	if len(got) != 1 {
		t.Fatalf("got %v, want exactly one", got)
	}
}

func TestWheelCallbackRearmsForALaterTick(t *testing.T) {
	w := newWheel(t0)
	var tm timer
	n := 0
	var fn func()
	fn = func() { n++; w.schedule(&tm, t0, fn) } // a deadline in the past
	w.schedule(&tm, t0.Add(time.Second), fn)
	w.advance(t0.Add(3 * time.Second))
	if n != 1 {
		t.Fatalf("ran %d times in one advance, want 1", n)
	}
	w.advance(t0.Add(4 * time.Second))
	if n != 2 {
		t.Fatalf("ran %d times after the next tick, want 2", n)
	}
}

func TestWheelClockJumpFiresEveryDueTimerOnce(t *testing.T) {
	w := newWheel(t0)
	timers := make([]timer, 1000)
	fired := make([]int, len(timers))
	for i := range timers {
		w.schedule(&timers[i], t0.Add(time.Duration(i+1)*time.Second), func() { fired[i]++ })
	}
	w.advance(t0.Add(2 * time.Hour))
	for i, n := range fired {
		if n != 1 {
			t.Fatalf("timer %d fired %d times", i, n)
		}
	}
}

func TestWheelScheduleDoesNotAllocate(t *testing.T) {
	w := newWheel(t0)
	var tm timer
	fn := func() {}
	allocs := testing.AllocsPerRun(1000, func() {
		w.schedule(&tm, t0.Add(310*time.Second), fn)
		w.stop(&tm)
	})
	if allocs != 0 {
		t.Fatalf("%v allocations per schedule+stop", allocs)
	}
}
