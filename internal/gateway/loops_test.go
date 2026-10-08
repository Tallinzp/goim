package gateway

import (
	"runtime"
	"sync"
	"testing"
)

// stubLoop is a Loop that is never run; bind only needs its identity. The
// field keeps each one a distinct pointer (zero-size values may share one).
type stubLoop struct{ _ int }

func (*stubLoop) Execute(func()) error { return nil }

func TestConnIDsAreUniqueAndCarryTheWorkerID(t *testing.T) {
	cfg := testConfig()
	ls := newLoops(cfg, t0)
	const perLoop = 1000
	ids := make([][]uint64, cfg.NumLoops)
	var wg sync.WaitGroup
	for i := range cfg.NumLoops {
		wg.Add(1)
		go func() { // each goroutine is one loop: binds, then opens connections
			defer wg.Done()
			s := ls.bind(&stubLoop{})
			for range perLoop {
				ids[i] = append(ids[i], s.nextID())
			}
		}()
	}
	wg.Wait()
	seen := make(map[uint64]bool)
	workers := make(map[uint64]bool)
	for i, list := range ids {
		worker := list[0] >> 48
		workers[worker] = true
		for _, id := range list {
			if seen[id] {
				t.Fatalf("conn_id %#x issued twice", id)
			}
			seen[id] = true
			if id>>48 != worker || id&(1<<48-1) == 0 {
				t.Fatalf("loop %d: conn_id %#x (worker %d, counter 0 not allowed)", i, id, worker)
			}
		}
	}
	if len(workers) != cfg.NumLoops || len(ls.bound()) != cfg.NumLoops {
		t.Fatalf("%d worker ids and %d bound loops, want %d", len(workers), len(ls.bound()), cfg.NumLoops)
	}
	for w := range uint64(cfg.NumLoops) {
		if ls.byWorker(w) == nil {
			t.Fatalf("byWorker(%d) = nil", w)
		}
	}
	if ls.byWorker(uint64(cfg.NumLoops)) != nil {
		t.Fatal("byWorker beyond NumLoops")
	}
}

func TestBindIsStable(t *testing.T) {
	ls := newLoops(testConfig(), t0)
	l := &stubLoop{}
	if a, b := ls.bind(l), ls.bind(l); a != b || ls.claimed.Load() != 1 {
		t.Fatal("binding the same loop twice claimed two loopStates")
	}
}

func TestLoopBeyondNumLoopsPanics(t *testing.T) {
	cfg := testConfig()
	cfg.NumLoops = 1
	ls := newLoops(cfg, t0)
	ls.bind(&stubLoop{})
	defer func() {
		if recover() == nil {
			t.Fatal("a second loop with NumLoops = 1 did not panic")
		}
	}()
	ls.bind(&stubLoop{})
}

// bytesAllocated measures what f allocates. AllocsPerRun counts allocations,
// not their size, and a table is one allocation like any small struct.
func bytesAllocated(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestFirstOpenOnALoopAllocatesNoTable(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns, cfg.NumLoops = 250_000, 16 // tables of 23k slots each
	ls := newLoops(cfg, t0)
	fakes := make([]*stubLoop, cfg.NumLoops)
	for i := range fakes {
		fakes[i] = &stubLoop{}
	}
	for i := range fakes {
		if n := bytesAllocated(func() { ls.bind(fakes[i]) }); n > 1024 {
			t.Fatalf("binding loop %d allocated %d bytes: the table must be built at boot", i, n)
		}
	}
}

func TestPreSizedTableDoesNotGrow(t *testing.T) {
	cfg := testConfig()
	cfg.MaxConns, cfg.NumLoops = 64_000, 4
	n := cfg.MaxConns / cfg.NumLoops // a loop's share of the cap
	conns := make([]*Conn, n)
	for i := range conns {
		conns[i] = &Conn{}
	}
	tables := []map[uint64]*Conn{
		make(map[uint64]*Conn, tableHint(cfg)), // warm-up run
		make(map[uint64]*Conn, tableHint(cfg)), // measured run
	}
	run := 0
	allocs := testing.AllocsPerRun(1, func() {
		m := tables[run]
		run++
		for i, c := range conns {
			m[uint64(i)+1] = c
		}
	})
	if allocs != 0 {
		t.Fatalf("inserting cap/loops entries into a pre-sized table allocated %v times (map growth)", allocs)
	}
}
