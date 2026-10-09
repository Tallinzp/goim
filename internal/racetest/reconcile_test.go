package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
)

// Race 14: the connection closes after its chunk is sent and before Logic
// processes it. Fence (c) holds the Logout until the chunk is resolved, so
// the chunk's refresh (or restore) lands first and the Logout deletes it.
// The check is that the Logout waits: a Logout that went first would let
// the chunk restore a ghost, which this pass's own delete phase would then
// remove before any later check could see it.
func TestRace14CloseDuringAChunk(t *testing.T) {
	s := New(t, Options{})
	const uid = "r14"
	c := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(3 * time.Second), Count: 1})
	s.Reconcile(0)
	eventually(t, "the chunk is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	c.Close()
	holds(t, "fence (c) holds the Logout while the chunk is in Logic", 2*time.Second, func() bool { return s.Redis.has(uid) })
	s.endsOK(0, 1, 10*time.Second)
	eventually(t, "the fenced Logout deleted the entry", 5*time.Second, func() bool { return !s.Redis.has(uid) })
	holds(t, "no ghost", time.Second, func() bool { return !s.Redis.has(uid) })
}

// Race 18: a client Logout while its chunk is held in Logic gets LogoutResp
// at once; the Logout RPC waits for fence (c), so no ghost remains.
func TestRace18ClientLogoutDuringAChunk(t *testing.T) {
	s := New(t, Options{})
	const uid = "r18"
	c := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(3 * time.Second), Count: 1})
	s.Reconcile(0)
	eventually(t, "the chunk is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	start := time.Now()
	c.Send(clientpb.Cmd_CMD_LOGOUT, nil)
	if f := c.Next(time.Second); f.Cmd != clientpb.Cmd_CMD_LOGOUT_RESP {
		t.Fatalf("got %v, want LogoutResp", f.Cmd)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Fatalf("LogoutResp after %v: it must not wait for the chunk", d)
	}
	holds(t, "fence (c) holds the Logout RPC while the chunk is in Logic", 1500*time.Millisecond, func() bool { return s.Redis.has(uid) })
	s.endsOK(0, 1, 10*time.Second)
	eventually(t, "the Logout deleted the entry after the chunk", 5*time.Second, func() bool { return !s.Redis.has(uid) })
	holds(t, "no ghost", time.Second, func() bool { return !s.Redis.has(uid) })
}

// Race 16 / ReconcileEnd: a Login in flight when the pass starts is tracked,
// so the pass's delete phase leaves its field alone even though the field
// was written before the phase ran.
func TestReconcileEndKeepsALoginInFlight(t *testing.T) {
	s := New(t, Options{})
	const uid = "r16a"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	s.LogicFault(fault.Rule{Op: logic.FaultReconcileEnd, Key: s.GW[0].ID, Delay: dur(3 * time.Second), Count: 1})
	c := s.Dial(0)
	c.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	eventually(t, "the Login is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultLogin) == 1 })
	s.Reconcile(0) // no ONLINE connection: the pass ends at once with the Login's conn_id tracked
	if f := c.Next(5 * time.Second); f.Cmd != clientpb.Cmd_CMD_LOGIN_RESP {
		t.Fatalf("got %v %s, want LoginResp", f.Cmd, describe(f))
	}
	s.endsOK(0, 1, 10*time.Second) // the delete phase ran after the write
	if !s.Redis.has(uid) {
		t.Fatal("the delete phase removed a Login that was in flight when its pass started")
	}
}

// Race 16 / ReconcileEnd: a Logout dropped after the connection's chunk was
// sent is repaired at the end of the same pass. The pass is kept running by
// another entry whose writes are refused and re-queued (R3-O6).
func TestReconcileEndRepairsALogoutDroppedDuringThePass(t *testing.T) {
	s := New(t, Options{})
	const dropped, slow = "r16b", "r16c"
	a := s.Online(0, dropped, "d1", 1)
	s.Online(0, slow, "d2", 1)
	s.Redis.Del(slow) // slow needs a rule-4 restore, which the deadline can refuse
	s.LogicFault(fault.Rule{Op: logic.FaultLogout, Key: dropped, Fail: true})
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: slow, Delay: dur(5400 * time.Millisecond), Count: 3})
	s.Reconcile(0)
	eventually(t, "the first chunk is held in Logic", 3*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	a.Close() // its Logout waits for the chunk, then fails until the 6s budget drops it
	eventually(t, "the Logout was dropped", 15*time.Second, func() bool { return s.gwMetric(0, sprintf(gwLogouts, "dropped")) == 1 })
	if s.gwMetric(0, sprintf(gwPasses, triggered, "done")) != 0 {
		t.Fatal("the pass ended before the Logout was dropped; the scenario needs a longer pass")
	}
	if !s.Redis.has(dropped) {
		t.Fatal("no ghost to repair")
	}
	s.endsOK(0, 1, 15*time.Second)
	if s.Redis.has(dropped) {
		t.Fatal("the pass's delete phase left the dropped Logout's ghost")
	}
	if !s.Redis.has(slow) || s.gwMetric(0, gwRequeued) != 3 {
		t.Fatalf("slow restored %v after %v re-queues, want restored after 3", s.Redis.has(slow), s.gwMetric(0, gwRequeued))
	}
}

// ReconcileEnd: a periodic pass cancelled by a trigger sends no
// ReconcileEnd, so it deletes nothing (R3-O1). From the moment its chunk is
// held in Logic, Logic counts ReconcileEnds: only the triggered pass that
// follows may send one. The cycle is 10s, so the next periodic pass comes
// well after the check; one gateway, so the count is its own.
func TestCancelledPassSendsNoReconcileEnd(t *testing.T) {
	s := New(t, Options{Gateways: 1, PeriodicPasses: true, Gateway: []string{"-reconcile-cycle", "10s", "-reconcile-jitter", "0s"}})
	const uid = "r16d"
	s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	eventually(t, "a periodic pass's chunk is held in Logic", 12*time.Second, func() bool { return s.LogicFired(logic.FaultReconcile) == 1 })
	s.LogicFault(fault.Rule{Op: logic.FaultReconcileEnd, Key: s.GW[0].ID}) // counts, changes nothing
	s.Reconcile(0)                                                         // cancels the periodic pass
	eventually(t, "the periodic pass was cancelled", 5*time.Second, func() bool {
		return s.gwMetric(0, sprintf(gwPasses, periodic, "cancelled")) == 1
	})
	s.passDone(0, 1, 5*time.Second)
	s.endsOK(0, 1, 5*time.Second)
	time.Sleep(500 * time.Millisecond)
	if n := s.LogicFired(logic.FaultReconcileEnd); n != 1 {
		t.Fatalf("Logic got %d ReconcileEnds since the periodic pass started, want 1 (the triggered pass)", n)
	}
}

// ReconcileEnd: one that fails is retried, and its deletes apply.
func TestFailedReconcileEndIsRetried(t *testing.T) {
	s := New(t, Options{})
	const ghost = "r16f"
	s.Redis.Ghost(ghost, s.GW[0], 1<<48-1)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcileEnd, Key: s.GW[0].ID, Fail: true, Count: 1})
	s.Reconcile(0)
	s.endsOK(0, 1, 10*time.Second)
	if s.gwMetric(0, sprintf(gwEnds, "retried")) != 1 {
		t.Fatalf("retried %v, want 1", s.gwMetric(0, sprintf(gwEnds, "retried")))
	}
	if s.Redis.has(ghost) {
		t.Fatal("the retried ReconcileEnd's delete did not apply")
	}
}

// Reconcile refusal (R3-O6): one chunk held past login_deadline comes back
// refused; its entry is re-queued and restored within the same pass.
func TestRefusedChunkIsRequeuedAndRestoredInThePass(t *testing.T) {
	s := New(t, Options{})
	const uid = "rr1"
	s.Online(0, uid, "d1", 1)
	s.Redis.Del(uid)
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(5400 * time.Millisecond), Count: 1})
	s.Reconcile(0)
	s.passDone(0, 1, 15*time.Second)
	if !s.Redis.has(uid) || s.gwMetric(0, gwRequeued) != 1 || s.gwMetric(0, sprintf(gwPasses, triggered, "done")) != 1 {
		t.Fatalf("restored %v, re-queued %v, passes %v; want restored by the one pass after one re-queue",
			s.Redis.has(uid), s.gwMetric(0, gwRequeued), s.gwMetric(0, sprintf(gwPasses, triggered, "done")))
	}
}

// Reconcile refusal: a refused rule-3 write sends no Kick. The stored entry
// is an older session of the same device on live gw-1 (a lost write put it
// back); only the write that lands, after the re-queue, kicks it.
func TestRefusedRule3WriteSendsNoKick(t *testing.T) {
	s := New(t, Options{})
	const uid = "rr2"
	s.Online(0, uid, "d1", 2)
	now := s.Redis.Now()
	s.Redis.Put(uid, session.Entry{Gateway: s.GW[1].ID, Epoch: s.GW[1].Epoch, ConnID: 1<<48 - 1, DeviceID: "d1",
		ConnSeq: 1, LoginMs: now - 60_000, TokIatMs: now - 61_000})
	s.LogicFault(fault.Rule{Op: logic.FaultReconcile, Key: uid, Delay: dur(5400 * time.Millisecond), Count: 1})
	s.Reconcile(0)
	s.passDone(0, 1, 15*time.Second)
	if e, _ := s.Redis.Entry(uid); e.Gateway != s.GW[0].ID || s.gwMetric(0, gwRequeued) != 1 {
		t.Fatalf("entry %+v after %v re-queues, want gw-0's session written back after one", e, s.gwMetric(0, gwRequeued))
	}
	eventually(t, "the written rule-3 entry's Kick reached gw-1", 5*time.Second, func() bool {
		return s.gwMetric(1, sprintf(gwKicks, "ignored")) >= 1
	})
	time.Sleep(500 * time.Millisecond)
	if n := s.gwMetric(1, sprintf(gwKicks, "ignored")); n != 1 {
		t.Fatalf("gw-1 got %v Kicks, want 1: the refused write must not kick", n)
	}
}

// Reconcile: a missing entry is restored (rule 4), and a ghost of this
// epoch is deleted by the delete phase.
func TestReconcileRestoresMissingAndDeletesGhosts(t *testing.T) {
	s := New(t, Options{})
	const live, ghost = "rc1", "rc2"
	s.Online(0, live, "d1", 1)
	s.Redis.Del(live)
	s.Redis.Ghost(ghost, s.GW[0], 1<<48-1)
	s.Reconcile(0)
	s.endsOK(0, 1, 10*time.Second)
	if !s.Redis.has(live) || s.Redis.has(ghost) {
		t.Fatalf("live restored %v, ghost left %v", s.Redis.has(live), s.Redis.has(ghost))
	}
}

// Reconcile: a ghost is removed within one cycle by a periodic pass, with
// no trigger. The cycle here is 6s (the profile's 60s would cost a minute).
func TestPeriodicPassRemovesAGhostWithinOneCycle(t *testing.T) {
	s := New(t, Options{PeriodicPasses: true, Gateway: []string{"-reconcile-cycle", "6s", "-reconcile-jitter", "600ms"}})
	const ghost = "rc3"
	s.Redis.Ghost(ghost, s.GW[0], 1<<48-1)
	eventually(t, "a periodic pass removed the ghost", 8*time.Second, func() bool { return !s.Redis.has(ghost) })
	if s.gwMetric(0, sprintf(gwPasses, periodic, "done")) < 1 {
		t.Fatal("no periodic pass ran")
	}
}

// Reconcile: a newer session on another gateway is never overridden. The
// older connection, whose Kick was dropped, is closed as not_owner.
func TestReconcileNeverOverridesANewerSession(t *testing.T) {
	s := New(t, Options{})
	const uid = "rc4"
	old := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.Online(1, uid, "d2", 1)
	eventually(t, "Logic gave up on the dropped Kick", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	s.ClearFault(0, gateway.FaultKick)
	s.Reconcile(0)
	old.ExpectKick(clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, 10*time.Second)
	if e, _ := s.Redis.Entry(uid); e.Gateway != "gw-1" || e.DeviceID != "d2" {
		t.Fatalf("entry %+v, want d2 on gw-1", e)
	}
}

// Reconcile: a Logout dropped while the gateway is alive leaves a ghost
// that the next pass deletes (race 7).
func TestDroppedLogoutIsRepairedByTheNextPass(t *testing.T) {
	s := New(t, Options{})
	const uid = "rc5"
	c := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultLogout, Key: uid, Fail: true})
	c.Close()
	eventually(t, "the Logout was dropped", 15*time.Second, func() bool { return s.gwMetric(0, sprintf(gwLogouts, "dropped")) == 1 })
	if !s.Redis.has(uid) {
		t.Fatal("no ghost")
	}
	s.Reconcile(0)
	s.endsOK(0, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("the next pass left the ghost")
	}
}
