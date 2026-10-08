package gateway

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// onlineMany logs n connections in on each loop.
func (l *live) onlineMany(t *testing.T, n int) []*fakeLink {
	t.Helper()
	var ks []*fakeLink
	for li := range l.loops {
		for i := range n {
			ks = append(ks, l.loginLive(t, li, "u"+string(rune('a'+li))+string(rune('a'+i))))
		}
	}
	return ks
}

func chunkIDs(c *pb.ReconcileChunk) []uint64 {
	var ids []uint64
	for _, e := range c.GetEntries() {
		ids = append(ids, e.GetConnId())
	}
	return ids
}

// passes counts ReconcileEnd calls.
func passes(l *live) int { return get(l.logic, func() int { return len(l.logic.ends) }) }

// pass triggers a pass and waits for its ReconcileEnd.
func (l *live) pass(t *testing.T) *pb.ReconcileEndRequest {
	t.Helper()
	n := passes(l)
	l.g.recon.Trigger()
	waitFor(t, "ReconcileEnd", func() bool { return passes(l) > n })
	return get(l.logic, func() *pb.ReconcileEndRequest { return l.logic.ends[n] })
}

func TestChunksHoldOneLoopEachAndTakeLoopsInTurn(t *testing.T) {
	// One chunk in flight at a time, so Logic sees them in the order sent.
	l := newLive(t, func(c *Config) { c.Reconcile.Chunk, c.Reconcile.MaxInFlight = 3, 1 })
	ks := l.onlineMany(t, 5)
	l.pass(t)
	chunks := get(l.logic, func() []*pb.ReconcileChunk { return l.logic.chunks })
	var workers []uint64
	seen := map[uint64]int{}
	for _, c := range chunks {
		ids := chunkIDs(c)
		for _, id := range ids {
			if id>>48 != ids[0]>>48 {
				t.Fatalf("chunk mixes loops: %x", ids)
			}
			seen[id]++
		}
		workers = append(workers, ids[0]>>48)
	}
	if len(seen) != len(ks) {
		t.Fatalf("%d connections reconciled, want %d", len(seen), len(ks))
	}
	first := slices.Clone(workers[:4])
	slices.Sort(first)
	if !slices.Equal(first, []uint64{0, 1, 2, 3}) {
		t.Fatalf("first four chunks came from loops %v, want each loop once", workers[:4])
	}
	if e := chunks[0].GetEntries()[0]; e.GetUid() == "" || e.GetLoginMs() == 0 || e.GetTokIatMs() != 1 || e.GetDeviceId() == "" {
		t.Fatalf("entry %v lacks the session fields", e)
	}
}

func TestRefusedEntriesAreRequeuedAtMostThreeTimes(t *testing.T) {
	l := newLive(t, nil)
	ks := l.onlineMany(t, 1)
	refused := ks[0].c.id
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			r := replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN)
			for i, e := range c.GetEntries() {
				if e.GetConnId() == refused {
					r.Results[i].Code = pb.ReconcileCode_RECONCILE_CODE_REFUSED
				}
			}
			return r, nil
		}
	})
	l.pass(t)
	sent := get(l.logic, func() int {
		n := 0
		for _, c := range l.logic.chunks {
			n += count(chunkIDs(c), refused)
		}
		return n
	})
	if sent != 4 || testutil.ToFloat64(l.g.m.RequeueLimit) != 1 {
		t.Fatalf("refused entry sent %d times (want 1 + 3 re-queues), limit hits %v", sent, testutil.ToFloat64(l.g.m.RequeueLimit))
	}
}

func TestRequeuedEntryNoLongerOnlineIsSkipped(t *testing.T) {
	l := newLive(t, nil)
	ks := l.onlineMany(t, 1)
	victim := ks[0]
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			r := replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN)
			for i, e := range c.GetEntries() {
				if e.GetConnId() == victim.c.id {
					r.Results[i].Code = pb.ReconcileCode_RECONCILE_CODE_REFUSED
					victim.clientClose() // gone before its re-queued chunk is built
				}
			}
			return r, nil
		}
	})
	l.pass(t)
	sent := get(l.logic, func() int {
		n := 0
		for _, c := range l.logic.chunks {
			n += count(chunkIDs(c), victim.c.id)
		}
		return n
	})
	if sent != 1 {
		t.Fatalf("closed connection sent %d times, want 1", sent)
	}
}

func TestNotOwnerIsKickedWithItsReason(t *testing.T) {
	l := newLive(t, nil)
	ks := l.onlineMany(t, 1)
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			r := replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN)
			r.Results[0] = &pb.ReconcileResult{Code: pb.ReconcileCode_RECONCILE_CODE_NOT_OWNER, Reason: pb.KickReason_KICK_REASON_REVOKED}
			return r, nil
		}
	})
	l.pass(t)
	for _, k := range ks {
		var m clientpb.Kick
		if !k.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_REVOKED || !k.isClosed() {
			t.Fatalf("conn %x: want a REVOKED Kick and a close", k.c.id)
		}
	}
}

// blockChunks makes Reconcile wait until the returned func is called.
func (l *live) blockChunks(fail bool) (sent <-chan time.Time, release func()) {
	ch := make(chan time.Time, 100)
	gate := make(chan struct{})
	l.logic.set(func() {
		l.logic.reconcile = func(c *pb.ReconcileChunk) (*pb.ReconcileReply, error) {
			ch <- time.Now()
			if fail {
				return nil, errors.New("Logic crashed")
			}
			<-gate
			return replyAll(c, pb.ReconcileCode_RECONCILE_CODE_WRITTEN), nil
		}
	})
	return ch, func() { close(gate) }
}

func logoutCalls(l *live, connID uint64) int {
	return get(l.logic, func() int {
		n := 0
		for _, b := range l.logic.logouts {
			for _, e := range b.GetEntries() {
				if e.GetConnId() == connID {
					n++
				}
			}
		}
		return n
	})
}

func TestFenceCHoldsLogoutUntilTheChunkIsResolved(t *testing.T) {
	l := newLive(t, nil)
	k := l.loginLive(t, 0, "u1")
	sent, release := l.blockChunks(false)
	l.g.recon.Trigger()
	<-sent
	k.clientClose() // race 14: close after the chunk is sent
	time.Sleep(100 * time.Millisecond)
	if logoutCalls(l, k.c.id) != 0 {
		t.Fatal("Logout sent while its Reconcile chunk was unresolved")
	}
	release()
	waitFor(t, "Logout after the reply", func() bool { return logoutCalls(l, k.c.id) == 1 })
}

func TestClientLogoutDuringAChunkGetsLogoutRespAtOnce(t *testing.T) {
	l := newLive(t, nil)
	k := l.loginLive(t, 0, "u1")
	sent, release := l.blockChunks(false)
	l.g.recon.Trigger()
	<-sent
	l.frame(k, clientpb.Cmd_CMD_LOGOUT, 3, nil) // race 18
	if !k.last(t, clientpb.Cmd_CMD_LOGOUT_RESP, nil) {
		t.Fatal("LogoutResp waited for the chunk")
	}
	time.Sleep(100 * time.Millisecond)
	if logoutCalls(l, k.c.id) != 0 {
		t.Fatal("Logout RPC before the chunk was resolved")
	}
	release()
	waitFor(t, "Logout", func() bool { return logoutCalls(l, k.c.id) == 1 })
}

func TestFailedChunkIsResolvedAtSentMsPlusDeadlinePlusOneSecond(t *testing.T) {
	l := newLive(t, shortDeadline) // fence after 1.1s
	k := l.loginLive(t, 0, "u1")
	sent, _ := l.blockChunks(true)
	l.g.recon.Trigger()
	at := <-sent
	k.clientClose()
	waitFor(t, "Logout", func() bool { return logoutCalls(l, k.c.id) == 1 })
	if took := time.Since(at); took < 1100*time.Millisecond {
		t.Fatalf("Logout %v after the failed chunk was sent, want ≥ login_deadline + 1s", took)
	}
	if n := testutil.ToFloat64(l.g.m.ReconcileEntries.WithLabelValues("no_reply")); n != 1 {
		t.Fatalf("no_reply %v, want 1", n)
	}
}

func TestReconcileEndCarriesTheTrackedSetCollectedAfterAsOf(t *testing.T) {
	l := newLive(t, nil)
	held := l.loginLive(t, 0, "held")   // closed, its Logout not acknowledged
	pending := l.authed(1)              // AUTH_PENDING, no Login
	gone := l.loginLive(t, 2, "gone")   // closed and logged out
	online := l.loginLive(t, 3, "live") // ONLINE
	gone.clientClose()
	waitFor(t, "gone untracked", func() bool { return !l.tracked(gone) })
	block := make(chan struct{})
	defer close(block)
	l.logic.set(func() {
		l.logic.logout = func(b *pb.LogoutBatch) (*pb.LogoutReply, error) { <-block; return allDone(b), nil }
	})
	held.clientClose()
	waitFor(t, "held's Logout sent", func() bool { return logoutCalls(l, held.c.id) == 1 })
	before := l.clock.now().UnixMilli()
	end := l.pass(t)
	ids := end.GetTrackedConnIds()
	for _, k := range []*fakeLink{held, pending, online} {
		if !slices.Contains(ids, k.c.id) {
			t.Fatalf("tracked set %x lacks %x", ids, k.c.id)
		}
	}
	if slices.Contains(ids, gone.c.id) {
		t.Fatal("tracked set holds a connection whose Logout was acknowledged")
	}
	if end.GetAsOfMs() < before {
		t.Fatalf("as_of_ms %d before the pass started (%d)", end.GetAsOfMs(), before)
	}
}

func TestIncompleteTrackedSetSendsNoReconcileEnd(t *testing.T) {
	h := newCore(t, nil)
	h.online(0, "u1")
	h.online(1, "u2")
	r := newReconciler(h.g, nil) // a ReconcileEnd would call the nil client and panic
	h.loops[1].close()           // that loop's fragment can't be collected
	r.end(context.Background(), pb.Pace_PACE_PERIODIC)
	r.ends.Wait()
	if n := testutil.ToFloat64(h.g.m.Passes.WithLabelValues("PACE_PERIODIC", "incomplete")); n != 1 {
		t.Fatalf("incomplete passes %v, want 1", n)
	}
}

func TestReconcileEndIsRetriedWithTheSamePayload(t *testing.T) {
	l := newLive(t, nil)
	l.onlineMany(t, 1)
	calls := 0
	l.logic.set(func() {
		l.logic.reconcileEnd = func(*pb.ReconcileEndRequest) (*pb.ReconcileEndReply, error) {
			calls++
			if calls == 1 {
				return nil, errors.New("Logic pod died mid-phase")
			}
			return &pb.ReconcileEndReply{Deleted: 2}, nil
		}
	})
	l.g.recon.Trigger()
	waitFor(t, "two ReconcileEnds", func() bool { return passes(l) == 2 })
	ends := get(l.logic, func() []*pb.ReconcileEndRequest { return l.logic.ends })
	if ends[0].GetAsOfMs() != ends[1].GetAsOfMs() || !slices.Equal(ends[0].GetTrackedConnIds(), ends[1].GetTrackedConnIds()) {
		t.Fatal("the retry changed the payload")
	}
	waitFor(t, "deleted counted", func() bool { return testutil.ToFloat64(l.g.m.EndDeleted) == 2 })
}

func TestTriggerCancelsAPeriodicPass(t *testing.T) {
	l := newLive(t, func(c *Config) {
		c.Reconcile.Cycle, c.Reconcile.Jitter = 2*time.Second, 0
		c.Reconcile.Chunk, c.Reconcile.MinRate = 2, 5 // 40 entries at 5/s: the periodic pass takes ~8s
	})
	l.onlineMany(t, 10)
	waitFor(t, "periodic pass started", func() bool { return get(l.logic, func() int { return len(l.logic.chunks) }) > 0 })
	l.g.recon.Trigger()
	waitFor(t, "triggered pass done", func() bool { return passes(l) == 1 })
	end := get(l.logic, func() *pb.ReconcileEndRequest { return l.logic.ends[0] })
	if end.GetPace() != pb.Pace_PACE_TRIGGERED {
		t.Fatalf("first ReconcileEnd has pace %v: a cancelled pass must send none", end.GetPace())
	}
	if n := testutil.ToFloat64(l.g.m.Passes.WithLabelValues("PACE_PERIODIC", "cancelled")); n != 1 {
		t.Fatalf("cancelled periodic passes %v, want 1", n)
	}
}

func TestPassRunsWhileLoopsAcceptAndClose(t *testing.T) {
	l := newLive(t, func(c *Config) { c.Reconcile.Chunk = 5 })
	l.onlineMany(t, 5)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for li := range l.loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := l.authed(li)
				l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("churn", "d", uint64(i)))
				time.Sleep(time.Millisecond)
				k.clientClose()
			}
		}()
	}
	for range 3 {
		l.pass(t)
	}
	close(stop)
	wg.Wait()
}

func count(ids []uint64, id uint64) int {
	n := 0
	for _, x := range ids {
		if x == id {
			n++
		}
	}
	return n
}
