package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/Tallinzp/goim/internal/logic"
)

// Race 4, zombie gateway with the same pod name: gw-0 freezes and is
// replaced by a new epoch under the same name, whose first connection gets
// the same conn_id. The relogin's Kick for the old epoch reaches the new
// instance (pod DNS follows the name) and is ignored for its epoch.
func TestRace4ZombieGatewayKickForTheOldEpochIsIgnored(t *testing.T) {
	s := New(t, Options{})
	const uid = "r4"
	s.Online(0, uid, "d1", 1)
	first, _ := s.Redis.Entry(uid)
	zombie := s.GW[0]
	zombie.pause()
	defer zombie.kill()
	s.RestartGateway(0)
	c := s.Online(0, uid, "d1", 2)
	e, _ := s.Redis.Entry(uid)
	if e.Epoch == first.Epoch || e.ConnID != first.ConnID {
		t.Fatalf("new entry %+v, old %+v: the scenario needs a new epoch with the same conn_id", e, first)
	}
	eventually(t, "the new instance ignored the Kick for the old epoch", 5*time.Second, func() bool {
		return s.gwMetric(0, sprintf(gwKicks, "ignored")) == 1
	})
	c.Quiet(time.Second)
}

// A Kick that arrives while the connection's Login is in flight (AUTH_PENDING)
// is held, and applied when the Login reply arrives: the client gets the
// Kick instead of LoginResp (race 8).
func TestKickDuringAuthPendingClosesWhenTheLoginReplyArrives(t *testing.T) {
	s := New(t, Options{})
	const uid = "k1"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, After: true, Delay: dur(2 * time.Second), Count: 1})
	a := s.Dial(0)
	a.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	eventually(t, "a's Login was written", 3*time.Second, func() bool { return s.Redis.has(uid) })
	s.Online(1, uid, "d2", 1) // kicks a while its Login reply is held
	eventually(t, "the Kick is pending on gw-0", 3*time.Second, func() bool { return s.gwMetric(0, sprintf(gwKicks, "pending")) == 1 })
	a.ExpectKick(clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, 5*time.Second)
}

// A Kick to a live gateway that is lost is retried (3 times, 1s apart at the
// profile) while the target's lease is live, then given up; the next
// Reconcile pass closes the connection (race 8, R2-O6).
func TestDroppedKickIsRetriedThenCaughtByReconcile(t *testing.T) {
	s := New(t, Options{})
	const uid = "k2"
	old := s.Online(0, uid, "d1", 1)
	s.Fault(0, fault.Rule{Op: gateway.FaultKick, Fail: true})
	s.Online(1, uid, "d1", 2)
	eventually(t, "Logic gave up after its retries", 10*time.Second, func() bool {
		return s.logicMetric(logicKickGiveUps) == 1
	})
	if n := s.logicMetric(sprintf(logicKickAttempts, "retry", "error")); n != 3 {
		t.Fatalf("%v retries, want 3", n)
	}
	old.Quiet(500 * time.Millisecond)
	s.ClearFault(0, gateway.FaultKick)
	s.Reconcile(0)
	old.ExpectKick(clientpb.KickReason_KICK_REASON_REPLACED, 10*time.Second)
}

// A draining gateway still receives Kicks: its Kick server stays up until
// the end of the drain (publishNotReadyAddresses).
func TestDrainingGatewayStillReceivesKicks(t *testing.T) {
	s := New(t, Options{})
	const uid = "k3"
	a := s.Online(0, uid, "d1", 1)
	go s.GW[0].term(30 * time.Second)
	if f := a.Next(5 * time.Second); f.Cmd != clientpb.Cmd_CMD_RECONNECT {
		t.Fatalf("got %v, want Reconnect from the draining gateway", f.Cmd)
	}
	// The client ignores Reconnect; another instance of the device logs in,
	// and Logic kicks this one on the draining gateway. Nothing comes in
	// between: one Reconnect per connection (Task 9).
	s.Online(1, uid, "d1", 2)
	a.ExpectKick(clientpb.KickReason_KICK_REASON_REPLACED, 5*time.Second)
	// The client sees the Kick before Logic's RPC returns and counts it.
	eventually(t, "Logic's Kick attempt succeeded", 2*time.Second, func() bool {
		return s.logicMetric(sprintf(logicKickAttempts, "first", "ok")) == 1
	})
}
