package racetest

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/fault"
	"github.com/Tallinzp/goim/internal/logic"
	"github.com/Tallinzp/goim/internal/sdk"
)

// Race 1, delayed disconnect: the old connection's Logout reaches Logic
// after the device logged in again on another gateway. The compare-and-delete
// misses, and the new session stays.
func TestRace1DelayedDisconnect(t *testing.T) {
	s := New(t, Options{})
	const uid = "r1"
	old := s.Online(0, uid, "d1", 1)
	s.LogicFault(fault.Rule{Op: logic.FaultLogout, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	old.Close() // gw-0 sends the Logout; Logic holds it for 2s
	time.Sleep(500 * time.Millisecond)
	s.Online(1, uid, "d1", 2)
	eventually(t, "the delayed Logout ran and missed", 5*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogouts, "not_owner")) == 1
	})
	if e, ok := s.Redis.Entry(uid); !ok || e.Gateway != "gw-1" || e.ConnSeq != 2 {
		t.Fatalf("entry %+v %v, want the new session on gw-1", e, ok)
	}
}

// Race 2, delayed login from an abandoned connection of the same device:
// S1's Login reaches Redis after S2's, inside the deadline, and is STALE
// with S2's counter; S2 is not kicked.
func TestRace2DelayedLoginSameDeviceIsStale(t *testing.T) {
	s := New(t, Options{})
	const uid = "r2"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	s1 := s.Dial(0)
	s1.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	time.Sleep(500 * time.Millisecond) // S1's Login is held in Logic
	s2 := s.Online(1, uid, "d1", 2)
	f := s1.Next(5 * time.Second)
	var e clientpb.Error
	if f.Cmd != clientpb.Cmd_CMD_ERROR || f.Decode(&e) != nil || e.Code != clientpb.ErrorCode_ERROR_CODE_STALE || e.StoredConnSeq != 2 {
		t.Fatalf("S1 got %v %s, want STALE with stored_conn_seq 2", f.Cmd, describe(f))
	}
	s2.Quiet(time.Second)
	if en, _ := s.Redis.Entry(uid); en.Gateway != "gw-1" || en.ConnSeq != 2 {
		t.Fatalf("entry %+v, want S2", en)
	}
}

// Race 2, a login later than the deadline is EXPIRED and writes nothing.
func TestRace2LoginPastTheDeadlineIsExpired(t *testing.T) {
	s := New(t, Options{})
	const uid = "r2x"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(5500 * time.Millisecond), Count: 1})
	c := s.Dial(0)
	if code := errorCode(t, c.Login(s.Req(uid, "d1", 1))); code != clientpb.ErrorCode_ERROR_CODE_EXPIRED {
		t.Fatalf("got %v, want EXPIRED", code)
	}
	if _, ok := s.Redis.Entry(uid); ok {
		t.Fatal("an EXPIRED login wrote an entry")
	}
}

// Race 3, delayed login from another device: its recv_ms is older than the
// stored login_ms, so it is SUPERSEDED and the newer device is not kicked.
func TestRace3DelayedLoginOtherDeviceIsSuperseded(t *testing.T) {
	s := New(t, Options{})
	const uid = "r3"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	d1 := s.Dial(0)
	d1.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	time.Sleep(500 * time.Millisecond)
	d2 := s.Online(1, uid, "d2", 1)
	if code := errorCode(t, d1.Next(5*time.Second)); code != clientpb.ErrorCode_ERROR_CODE_SUPERSEDED {
		t.Fatalf("d1 got %v, want SUPERSEDED", code)
	}
	d2.Quiet(time.Second)
	if en, _ := s.Redis.Entry(uid); en.DeviceID != "d2" {
		t.Fatalf("entry %+v, want d2", en)
	}
}

// Race 5, the connection closes while its Login is in Logic: fence (a)
// holds the Logout until the Login reply, so the Logout lands after the
// write and no ghost remains.
func TestRace5CloseWhileLoginInFlight(t *testing.T) {
	s := New(t, Options{})
	const uid = "r5"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(2 * time.Second), Count: 1})
	c := s.Dial(0)
	c.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	time.Sleep(500 * time.Millisecond)
	c.Close()
	eventually(t, "Logic wrote the login", 5*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogins, "ok")) == 1
	})
	eventually(t, "the fenced Logout deleted it", 3*time.Second, func() bool { _, ok := s.Redis.Entry(uid); return !ok })
	holds(t, "no ghost", time.Second, func() bool { _, ok := s.Redis.Entry(uid); return !ok })
}

// Race 6, Login before Logout: Redis is written but the reply comes after
// the gateway's Login timeout. The client gets Busy, and fence (b) sends the
// Logout at recv_ms + login_deadline + 1s, after the write: no ghost.
func TestRace6WrittenWithoutReplyLoginBeforeLogout(t *testing.T) {
	s := New(t, Options{})
	const uid = "r6a"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, After: true, Delay: dur(8 * time.Second), Count: 1})
	c := s.Dial(0)
	if f := c.Login(s.Req(uid, "d1", 1)); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy (no definitive reply)", f.Cmd, describe(f))
	}
	eventually(t, "the fenced Logout deleted the written entry", 3*time.Second, func() bool {
		_, ok := s.Redis.Entry(uid)
		return !ok && s.Metric(s.Logic.Admin, sprintf(logicLogouts, "deleted")) == 1
	})
}

// Race 6, Logout before Login: the Login reaches Redis only after the fence
// released the Logout. The login Lua's deadline refuses it (EXPIRED), so the
// early Logout leaves nothing behind.
func TestRace6WrittenWithoutReplyLogoutBeforeLogin(t *testing.T) {
	s := New(t, Options{})
	const uid = "r6b"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, Delay: dur(7 * time.Second), Count: 1})
	c := s.Dial(0)
	if f := c.Login(s.Req(uid, "d1", 1)); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy", f.Cmd, describe(f))
	}
	eventually(t, "the Logout ran first and found nothing", 3*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogouts, "not_owner")) == 1
	})
	eventually(t, "the late Login was refused", 3*time.Second, func() bool {
		return s.Metric(s.Logic.Admin, sprintf(logicLogins, "expired")) == 1
	})
	if _, ok := s.Redis.Entry(uid); ok {
		t.Fatal("ghost: the late Login wrote after the Logout")
	}
}

// Race 6, Logic crashes with the Login written but not answered: the client
// gets Busy, and the Logout still waits for fence (b) although Logic is back
// within a second.
func TestRace6LogicCrashLogoutWaitsForTheFence(t *testing.T) {
	s := New(t, Options{})
	const uid = "r6c"
	s.LogicFault(fault.Rule{Op: logic.FaultLogin, Key: uid, After: true, Delay: dur(time.Minute), Count: 1})
	c := s.Dial(0)
	sent := time.Now()
	c.Send(clientpb.Cmd_CMD_LOGIN, s.Req(uid, "d1", 1))
	eventually(t, "Logic wrote the login", 3*time.Second, func() bool { _, ok := s.Redis.Entry(uid); return ok })
	s.RestartLogic()
	if f := c.Next(10 * time.Second); f.Cmd != clientpb.Cmd_CMD_BUSY {
		t.Fatalf("got %v %s, want Busy", f.Cmd, describe(f))
	}
	// The fence releases at recv_ms + 6s; Logic has been back since well before.
	holds(t, "the Logout waits for the fence", time.Until(sent.Add(5*time.Second)), func() bool {
		_, ok := s.Redis.Entry(uid)
		return ok
	})
	eventually(t, "the fenced Logout deleted it", 5*time.Second, func() bool { _, ok := s.Redis.Entry(uid); return !ok })
}

// Race 10, two client instances share a device_id, on different gateways:
// they replace each other once, the replaced one goes idle (REPLACED does
// not reconnect), and the loop stops.
func TestRace10SharedDeviceIDReplacesOnce(t *testing.T) {
	s := New(t, Options{})
	const uid = "r10"
	st1 := &sdk.MemStore{}
	c1, r1 := s.SDK(uid, st1, 0)
	c1.Connect()
	r1.next(t, sdk.EventOnline, 10*time.Second)
	shared, _ := st1.Load()
	st2 := &sdk.MemStore{}
	st2.Save(sdk.State{DeviceID: shared.DeviceID, ConnSeq: 100})
	c2, r2 := s.SDK(uid, st2, 1)
	c2.Connect()
	r2.next(t, sdk.EventOnline, 10*time.Second)
	if e := r1.next(t, sdk.EventIdle, 10*time.Second); e.Kick != clientpb.KickReason_KICK_REASON_REPLACED {
		t.Fatalf("first instance idle with %v, want REPLACED", e.Kick)
	}
	r1.none(t, sdk.EventConnecting, 3*time.Second)
	r2.none(t, sdk.EventWaiting, 0)
	if en, _ := s.Redis.Entry(uid); en.Gateway != "gw-1" || en.ConnSeq != 101 {
		t.Fatalf("entry %+v, want the second instance on gw-1", en)
	}
}
