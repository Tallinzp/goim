package racetest

import (
	"strconv"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/session"
)

// sweepBound is how long after a gateway's last renewal its entries must be
// gone at the profile: 2 × lease TTL (6s), up to one TTL of guard wait (3s),
// and the sweep itself, with a second of slack for -race on two cores.
const sweepBound = 11 * time.Second

// swept reports whether gw's epoch is gone from gwepochs (FinishSweep).
func (s *Stack) swept(g *Gateway) bool { _, ok := s.Redis.Score(g.ID, g.Epoch); return !ok }

// onlineOnEach logs one client in on every gateway and returns their uids.
func (s *Stack) onlineOnEach(prefix string) []string {
	s.t.Helper()
	uids := make([]string, len(s.GW))
	for i := range s.GW {
		uids[i] = prefix + strconv.Itoa(i)
		s.Online(i, uids[i], "d1", 1)
	}
	return uids
}

// noSweepFor checks that Logic sweeps nothing for d: no sweep finishes, and
// none deletes a field before it aborts (the triggered pass would restore
// those sessions by the time the scenario looks).
func (s *Stack) noSweepFor(d time.Duration) {
	s.t.Helper()
	holds(s.t, "nothing is swept", d, func() bool {
		return s.logicMetric(sprintf(logicSweeps, "done")) == 0 && s.logicMetric(logicSweepDeleted) == 0
	})
}

// Gateway crash: its lease expires, and the sweeper deletes its entries and
// its gwepochs member while the other gateways renew.
func TestGatewayCrashIsSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw1-")
	dead := s.GW[0]
	dead.kill()
	eventually(t, "the lease expired", 4*time.Second, func() bool { return !s.Redis.LeaseExists(dead.ID, dead.Epoch) })
	eventually(t, "the dead epoch was swept", sweepBound, func() bool { return s.swept(dead) })
	if s.Redis.has(uids[0]) || len(s.Redis.Conns(dead.ID, dead.Epoch)) != 0 {
		t.Fatal("the sweep left the dead gateway's entry or gwconns")
	}
	for _, uid := range uids[1:] {
		if !s.Redis.has(uid) {
			t.Fatalf("%s on a live gateway was deleted", uid)
		}
	}
}

// Two gateways killed at once: both epochs are swept while the others renew (R2-O3).
func TestTwoGatewaysKilledAtOnceAreBothSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw2-")
	s.GW[0].kill()
	s.GW[1].kill()
	eventually(t, "both epochs swept", sweepBound, func() bool { return s.swept(s.GW[0]) && s.swept(s.GW[1]) })
	if s.Redis.has(uids[0]) || s.Redis.has(uids[1]) || !s.Redis.has(uids[2]) || !s.Redis.has(uids[3]) {
		t.Fatal("wrong entries swept")
	}
}

// Three gateways killed while one keeps renewing at its 1s rhythm: the
// guard's renewing_since is never reset, and all three epochs are swept
// after the guard wait (R3-O7, R3-O2).
func TestThreeKilledWhileOneRenewsAreAllSwept(t *testing.T) {
	s := New(t, Options{})
	s.onlineOnEach("sw3-")
	for _, g := range s.GW[:3] {
		g.kill()
	}
	last := s.logicMetric(logicRenewing)
	eventually(t, "all three epochs swept", sweepBound, func() bool {
		if r := s.logicMetric(logicRenewing); r < last {
			t.Fatalf("renewing_since was reset: %vs after %vs", r, last)
		} else {
			last = r
		}
		return s.swept(s.GW[0]) && s.swept(s.GW[1]) && s.swept(s.GW[2])
	})
	if s.swept(s.GW[3]) {
		t.Fatal("the renewing gateway was swept")
	}
}

// Redis paused for 9s (the spec's 90s): every epoch looks stale when Redis
// resumes, the guard holds them back until the gateways renew, and nothing is
// swept. The gateways' renewals keep failing for 1.5s after the pause, so the
// sweeper is sure to look before they land (otherwise it is a race, and a
// run where they win proves nothing about the guard). The sessions are still there afterwards: each gateway's renewals
// failed for longer than a lease TTL, so it runs a triggered pass that
// refreshes them (its lease reply may or may not report missing).
func TestRedisPausedNothingIsSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw4-")
	for i := range s.GW {
		s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true, For: dur(10500 * time.Millisecond)})
	}
	s.Redis.PauseFor(9 * time.Second)
	time.Sleep(9 * time.Second)
	s.noSweepFor(8 * time.Second)
	if s.logicMetric(logicGuardHolds) == 0 {
		t.Fatal("the guard never held: the sweeper saw no stale epoch, so the scenario proved nothing")
	}
	for i := range s.GW {
		s.passDone(i, 1, time.Second)
	}
	for _, uid := range uids {
		if !s.Redis.has(uid) {
			t.Fatalf("%s is gone after the Redis pause", uid)
		}
	}
}

// Every gateway cut off from Logic for 9s, Logic↔Redis healthy: no epoch
// renews, so the guard holds the stale epochs back; when the gateways
// return, they renew before the guard releases (R3-O2). Meanwhile their
// sessions read as invalid (no lease key), and valid again after: the
// consistency contract's "healthy session reads invalid" row.
func TestGatewaysCutOffFromLogicNothingIsSwept(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw5-")
	for i := range s.GW {
		s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Fail: true, For: dur(9 * time.Second)})
	}
	time.Sleep(5 * time.Second)
	for _, g := range s.GW {
		if s.Redis.LeaseExists(g.ID, g.Epoch) {
			t.Fatalf("%s's lease outlived 5s without renewals", g.ID)
		}
	}
	time.Sleep(4 * time.Second)
	s.noSweepFor(6 * time.Second)
	for _, g := range s.GW {
		if !s.Redis.LeaseExists(g.ID, g.Epoch) {
			t.Fatalf("%s's lease is not back", g.ID)
		}
	}
	if s.logicMetric(logicGuardHolds) == 0 {
		t.Fatal("the guard never held: the epochs never looked stale, so the scenario proved nothing")
	}
	for _, uid := range uids {
		if !s.Redis.has(uid) {
			t.Fatalf("%s is gone", uid)
		}
	}
}

// Logic restarts and renewals resume more than one lease TTL (3s) after its
// sweeper started: its fresh guard holds, and nothing is swept (R3-O2).
func TestLogicRestartWithLateRenewalsSweepsNothing(t *testing.T) {
	s := New(t, Options{})
	uids := s.onlineOnEach("sw6-")
	for i := range s.GW {
		s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true, For: dur(9 * time.Second)})
	}
	s.RestartLogic()
	time.Sleep(9 * time.Second)
	s.noSweepFor(6 * time.Second)
	if s.logicMetric(logicGuardHolds) == 0 {
		t.Fatal("the guard never held")
	}
	for _, uid := range uids {
		if !s.Redis.has(uid) {
			t.Fatalf("%s is gone", uid)
		}
	}
}

// forceSweep makes gateway i's live epoch look dead to the sweeper: its
// renewals are blocked, its lease key deleted and its score aged. A renewal
// already past the fault check would re-create the key and renew the score,
// so it waits until a renewal has failed: they are serial, so none is left
// in flight.
func (s *Stack) forceSweep(i int) {
	s.t.Helper()
	g := s.GW[i]
	failed := s.gwMetric(i, sprintf(gwLeaseCalls, "error"))
	s.Fault(i, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true})
	eventually(s.t, g.ID+": a renewal hit the fault", 3*time.Second, func() bool {
		return s.gwMetric(i, sprintf(gwLeaseCalls, "error")) > failed
	})
	s.Redis.DelLease(g.ID, g.Epoch)
	s.Redis.SetScore(g.ID, g.Epoch, s.Redis.Now()-10_000)
}

// forceSweep works with a renewal in flight: one that already passed the
// fault check (here: in its 800ms delay) would re-create the lease key behind
// DelLease and keep the epoch from being swept.
func TestForceSweepWaitsOutARenewalInFlight(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw10"
	s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Delay: dur(800 * time.Millisecond)})
	time.Sleep(1500 * time.Millisecond) // a renewal is most likely in its delay
	s.forceSweep(0)
	eventually(t, "the live epoch was swept", 5*time.Second, func() bool { return s.swept(s.GW[0]) && !s.Redis.has(uid) })
}

// A forced sweep of a live gateway: its sessions are deleted, then restored
// by the pass its next renewal (missing) triggers, within ~6s (R2-O8).
func TestForcedSweepOfALiveGatewayIsRestored(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw7"
	s.Online(0, uid, "d1", 1)
	s.forceSweep(0)
	eventually(t, "the live epoch was swept", 5*time.Second, func() bool { return s.swept(s.GW[0]) && !s.Redis.has(uid) })
	s.ClearFault(0, gateway.FaultLogic)
	eventually(t, "the triggered pass restored the session", 6*time.Second, func() bool { return s.Redis.has(uid) })
	if s.gwMetric(0, sprintf(gwLeaseCalls, "missing")) < 1 {
		t.Fatal("the renewal after the sweep did not report missing")
	}
}

// fill writes n fake entries for epoch (gw, epoch), so a slow sweep of it
// takes a while.
func (s *Stack) fill(prefix, gw string, epoch uint64, n int) {
	now := s.Redis.Now()
	for i := 0; i < n; i++ {
		s.Redis.Put(prefix+strconv.Itoa(i), session.Entry{Gateway: gw, Epoch: epoch, ConnID: uint64(i + 1),
			DeviceID: "fake", ConnSeq: 1, LoginMs: now - 60_000, TokIatMs: now - 61_000})
	}
}

func (s *Stack) claimed(gw string, epoch uint64) bool {
	return s.Redis.Client().Exists(ctx, "sweep:"+epochSuffix(gw, epoch)).Val() == 1
}

// slowSweep is a sweeper that takes ~3s for 300 uids.
var slowSweep = []string{"-sweep-rate", "100", "-sweep-batch", "10"}

// Sweep abort (R3-O3): a live gateway renews while its epoch is being
// swept. The reply is missing, the sweeper aborts at its next batch without
// UNLINK or ZREM and deletes its claim (R3-OP6), and the triggered pass
// restores the session within ~6s.
func TestSweepAbortsWhenTheGatewayRenews(t *testing.T) {
	s := New(t, Options{Logic: slowSweep})
	const uid = "sw8"
	g := s.GW[0]
	s.Online(0, uid, "d1", 1)
	s.fill("sw8f", g.ID, g.Epoch, 300)
	s.forceSweep(0)
	eventually(t, "the sweep started", 5*time.Second, func() bool { return s.claimed(g.ID, g.Epoch) })
	s.ClearFault(0, gateway.FaultLogic)
	eventually(t, "the sweep aborted", 5*time.Second, func() bool { return s.logicMetric(sprintf(logicSweeps, "aborted")) == 1 })
	if s.claimed(g.ID, g.Epoch) || s.swept(g) || len(s.Redis.Conns(g.ID, g.Epoch)) == 0 {
		t.Fatalf("after the abort: claim %v, member gone %v, gwconns %d; want no claim, member and gwconns kept",
			s.claimed(g.ID, g.Epoch), s.swept(g), len(s.Redis.Conns(g.ID, g.Epoch)))
	}
	eventually(t, "the triggered pass restored the session", 6*time.Second, func() bool { return s.Redis.has(uid) })
	if s.logicMetric(sprintf(logicSweeps, "done")) != 0 {
		t.Fatal("a sweep finished")
	}
}

// The first lease call after boot finds a new epoch's key absent and
// triggers no pass (R3-OP5); a renewal after a plain expiry, with no sweep,
// is missing and triggers exactly one pass.
func TestFirstLeaseTriggersNoPassAndAPlainExpiryTriggersOne(t *testing.T) {
	s := New(t, Options{})
	time.Sleep(2 * time.Second)
	if s.gwMetric(0, sprintf(gwLeaseCalls, "first")) != 1 || s.gwMetric(0, sprintf(gwPasses, triggered, "done")) != 0 {
		t.Fatalf("first %v, passes %v; want 1 first call and no pass",
			s.gwMetric(0, sprintf(gwLeaseCalls, "first")), s.gwMetric(0, sprintf(gwPasses, triggered, "done")))
	}
	s.Fault(0, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true, For: dur(4 * time.Second)})
	eventually(t, "the lease expired", 5*time.Second, func() bool { return !s.Redis.LeaseExists(s.GW[0].ID, s.GW[0].Epoch) })
	s.passDone(0, 1, 6*time.Second)
	time.Sleep(2 * time.Second)
	if m, p := s.gwMetric(0, sprintf(gwLeaseCalls, "missing")), s.gwMetric(0, sprintf(gwPasses, triggered, "done")); m != 1 || p != 1 {
		t.Fatalf("missing %v, passes %v; want one of each", m, p)
	}
}

// The lease key is authoritative (R4M-3): with gw-0's ZADD gwepochs failing
// for 9s while its lease SET succeeds, its score goes stale, the sweeper
// claims it, finds the lease key and skips it.
func TestLeaseKeyIsAuthoritative(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw9"
	s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultGWEpochs, Key: s.GW[0].ID, For: dur(9 * time.Second)})
	eventually(t, "the sweeper skipped the live epoch", 10*time.Second, func() bool {
		return s.logicMetric(sprintf(logicSweeps, "lease_live")) >= 1
	})
	time.Sleep(3 * time.Second)
	if s.logicMetric(sprintf(logicSweeps, "done")) != 0 || !s.Redis.has(uid) {
		t.Fatal("the live epoch was swept")
	}
}

// The lease key is authoritative mid-sweep: a lease key that reappears
// while a sweep runs, with the score unchanged, aborts it.
func TestSweepAbortsWhenALeaseKeyReappears(t *testing.T) {
	s := New(t, Options{Logic: slowSweep})
	const gw, epoch = "gw-9", 77
	s.fill("sw10f", gw, epoch, 300)
	s.Redis.SetScore(gw, epoch, s.Redis.Now()-10_000)
	eventually(t, "the sweep started", 5*time.Second, func() bool { return s.claimed(gw, epoch) })
	s.Redis.Client().Set(ctx, "gwlease:"+epochSuffix(gw, epoch), "x", time.Minute)
	eventually(t, "the sweep aborted", 5*time.Second, func() bool { return s.logicMetric(sprintf(logicSweeps, "aborted")) == 1 })
	if _, ok := s.Redis.Score(gw, epoch); !ok {
		t.Fatal("the aborted sweep removed the gwepochs member")
	}
}

// An epoch that sent EpochEnded is swept at once, not after 2 × TTL.
func TestEndedEpochIsSweptAtOnce(t *testing.T) {
	s := New(t, Options{})
	g := s.GW[0]
	go g.term(30 * time.Second)
	eventually(t, "EpochEnded set the score to 0", 15*time.Second, func() bool {
		ms, ok := s.Redis.Score(g.ID, g.Epoch)
		return ok && ms == 0
	})
	start := time.Now()
	eventually(t, "the ended epoch was swept", 3*time.Second, func() bool { return s.swept(g) })
	t.Logf("swept %v after EpochEnded", time.Since(start))
}

// Stale lease observation (R4M-2): gw-1's lease is starved (key removed)
// while its newer session S2 is stored; gw-0's older S1, whose Kick was
// dropped, overwrites S2 by rule 4. gw-1's next renewal reports missing, and
// its triggered pass writes S2 back by rule 3 and kicks S1, within ~6s.
func TestStaleLeaseObservationIsCorrectedByTheOwner(t *testing.T) {
	s := New(t, Options{})
	const uid = "sw11"
	s1 := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.Online(1, uid, "d1", 2)
	eventually(t, "Logic gave up on S1's Kick", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	s.Fault(1, fault.Rule{Op: gateway.FaultLogic, Key: "GatewayLease", Fail: true})
	eventually(t, "gw-1's renewals are blocked", 3*time.Second, func() bool { return s.Fired(1, gateway.FaultLogic) >= 1 })
	s.Redis.DelLease(s.GW[1].ID, s.GW[1].Epoch)
	s.Reconcile(0)
	eventually(t, "rule 4 overwrote S2 with S1", 5*time.Second, func() bool {
		e, _ := s.Redis.Entry(uid)
		return e.Gateway == s.GW[0].ID
	})
	s.ClearFault(0, gateway.FaultKick)
	s.ClearFault(1, gateway.FaultLogic)
	start := time.Now()
	s1.ExpectKick(clientpb.KickReason_KICK_REASON_REPLACED, 8*time.Second)
	if e, _ := s.Redis.Entry(uid); e.Gateway != s.GW[1].ID || e.ConnSeq != 2 {
		t.Fatalf("entry %+v, want S2 written back", e)
	}
	t.Logf("corrected %v after gw-1 renewed", time.Since(start))
}
