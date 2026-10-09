package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
)

// Clock skew ahead (R4M-5): +2s fires the skew alert. A gateway ahead by
// more than the 1s skew allowance can also see a Login written after its
// fence (b) released the Logout: the Lua's deadline runs on the gateway's
// recv_ms, the fence on its wheel. That ghost is deleted by the next pass's
// delete phase. The ghost uses +5s: the fence fires 6–7s after recv (the
// wheel rounds the deadline up to a whole second), and +5s puts the Lua
// deadline at 10s of real time, so a write at 9s lands well after the fence
// and a second before the deadline.
func TestClockAheadAlertsAndItsLateWriteIsCleanedUp(t *testing.T) {
	s := New(t, Options{})
	s.GW[0].kill()
	s.RestartGateway(0, "-clock-offset", "2s")
	s.GW[1].kill()
	s.RestartGateway(1, "-clock-offset", "5s")
	eventually(t, "the skew alert fired for both", 5*time.Second, func() bool {
		return s.logicMetric(`goim_logic_clock_skew_alert{gw="gw-0"}`) == 1 &&
			s.logicMetric(`goim_logic_clock_skew_alert{gw="gw-1"}`) == 1
	})
	const uid = "sk1"
	// The Login reaches Redis 9s after recv: after the fence (at most 7s)
	// and inside the Lua deadline, 10s of real time with recv_ms 5s ahead.
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(9 * time.Second), Count: 1})
	if f := s.Dial(1).Login(s.Req(uid, "d1", 1)); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy", f.Cmd, describe(f))
	}
	eventually(t, "the late write left a ghost", 5*time.Second, func() bool { return s.Redis.has(uid) })
	if s.gwMetric(1, `goim_gateway_tracked`) != 0 {
		t.Fatal("the connection is still tracked: its Logout has not gone yet")
	}
	s.Reconcile(1)
	s.endsOK(1, 1, 10*time.Second)
	if s.Redis.has(uid) {
		t.Fatal("the pass's delete phase left the ghost")
	}
}

// Clock skew −5s: the gateway's recv_ms is already past the deadline when
// the Lua runs, so its Logins are EXPIRED; the client logs in elsewhere.
func TestClockBehindLoginsExpireAndTheClientLogsInElsewhere(t *testing.T) {
	s := New(t, Options{})
	s.GW[0].kill()
	s.RestartGateway(0, "-clock-offset", "-5s")
	const uid = "sk2"
	if code := errorCode(t, s.Dial(0).Login(s.Req(uid, "d1", 1))); code != clientpb.ErrorCode_ERROR_CODE_EXPIRED {
		t.Fatalf("got %v, want EXPIRED", code)
	}
	s.Online(1, uid, "d1", 2)
}
