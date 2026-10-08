package gateway

import (
	"slices"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

func TestLoginBatchCarriesTheEntryAndGateway(t *testing.T) {
	l := newLive(t, nil)
	before := l.clock.now().UnixMilli()
	k := l.loginLive(t, 0, "u1")
	b := get(l.logic, func() *pb.LoginBatch { return l.logic.logins[0] })
	e := b.GetEntries()[0]
	if b.GetGw().GetGateway() != "gw-1" || b.GetGw().GetEpoch() != 0xE1 {
		t.Fatalf("batch gw %v", b.GetGw())
	}
	if e.GetConnId() != k.c.id || e.GetDeviceId() != "dev-u1" || e.GetConnSeq() != 1 ||
		e.GetAppid() != "app" || e.GetPlatform() != pb.Platform_PLATFORM_MOBILE {
		t.Fatalf("entry %v", e)
	}
	if e.GetRecvMs() < before || e.GetRecvMs() > l.clock.now().UnixMilli() {
		t.Fatalf("recv_ms %d is not the gateway clock at receipt", e.GetRecvMs())
	}
}

func TestLoginBatchesFillUpToTheBatchSize(t *testing.T) {
	l := newLive(t, func(c *Config) { c.LoginBatch, c.LoginLinger = 3, 300*time.Millisecond })
	var links []*fakeLink
	for i := range 7 {
		k := l.authed(i % 4)
		l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq(string(rune('a'+i)), "d", 1))
		links = append(links, k)
	}
	waitFor(t, "all ONLINE", func() bool {
		for _, k := range links {
			if !onLoop(k, func(c *Conn) bool { return c.state == stateOnline }) {
				return false
			}
		}
		return true
	})
	sizes := get(l.logic, func() []int {
		var s []int
		for _, b := range l.logic.logins {
			s = append(s, len(b.GetEntries()))
		}
		return s
	})
	slices.Sort(sizes)
	if !slices.Equal(sizes, []int{1, 3, 3}) {
		t.Fatalf("batch sizes %v, want [1 3 3]", sizes)
	}
}

func TestLoginResultsArrivePerEntry(t *testing.T) {
	l := newLive(t, func(c *Config) { c.LoginBatch, c.LoginLinger = 2, time.Second })
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			s.Send(okResult(1, b.Entries[1])) // entry 1 is on a healthy shard
			<-release                         // entry 0's shard is slow
			return s.Send(okResult(0, b.Entries[0]))
		}
	})
	slow, fast := l.authed(0), l.authed(1)
	l.frame(slow, clientpb.Cmd_CMD_LOGIN, 1, loginReq("slow", "d", 1))
	l.frame(fast, clientpb.Cmd_CMD_LOGIN, 1, loginReq("fast", "d", 1))
	waitFor(t, "fast ONLINE", func() bool { return onLoop(fast, func(c *Conn) bool { return c.state == stateOnline }) })
	if onLoop(slow, func(c *Conn) bool { return c.state != stateAuthPending }) {
		t.Fatal("the slow entry finished before its result")
	}
	close(release)
	waitFor(t, "slow ONLINE", func() bool { return onLoop(slow, func(c *Conn) bool { return c.state == stateOnline }) })
}

// shortDeadline makes the per-entry Login timeout 1.1s.
func shortDeadline(c *Config) {
	c.LoginDeadline, c.LoginTimeout = 100*time.Millisecond, 1100*time.Millisecond
}

func TestLoginEntryWithoutResultByItsTimeoutGetsBusyThenFencedLogout(t *testing.T) {
	l := newLive(t, func(c *Config) { shortDeadline(c); c.LoginBatch, c.LoginLinger = 2, time.Second })
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			s.Send(okResult(0, b.Entries[0]))
			<-s.Context().Done() // entry 1 never gets a result
			return nil
		}
	})
	ok, lost := l.authed(0), l.authed(1)
	l.frame(ok, clientpb.Cmd_CMD_LOGIN, 1, loginReq("ok", "d", 1))
	l.frame(lost, clientpb.Cmd_CMD_LOGIN, 1, loginReq("lost", "d", 1))
	waitFor(t, "lost gets Busy", func() bool { return lost.last(t, clientpb.Cmd_CMD_BUSY, nil) })
	if !onLoop(ok, func(c *Conn) bool { return c.state == stateOnline }) {
		t.Fatal("the entry with a result is not ONLINE")
	}
	// Fence (b) on the 1s wheel queues the Logout, with the token's sub as uid.
	waitFor(t, "Logout queued for lost", func() bool {
		return onLoop(lost, func(c *Conn) bool { return c.logoutQueued && c.uid == "lost" })
	})
}

func TestLoginStreamEndingEarlyIsNoDefinitiveReply(t *testing.T) {
	l := newLive(t, nil)
	l.logic.set(func() {
		l.logic.login = func(*pb.LoginBatch, grpc.ServerStreamingServer[pb.LoginResult]) error { return nil }
	})
	k := l.authed(0)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u1", "d", 1))
	waitFor(t, "Busy", func() bool { return k.last(t, clientpb.Cmd_CMD_BUSY, nil) })
	if got := onLoop(k, func(c *Conn) outcome { return c.outcome }); got != loginUnknown {
		t.Fatalf("outcome %d, want unknown (fence b)", got)
	}
}

func TestLogicCodesReachTheClient(t *testing.T) {
	l := newLive(t, nil)
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			for i, e := range b.GetEntries() {
				code := pb.LoginCode_LOGIN_CODE_REVOKED
				if tokenSubject(e.GetToken()) == "unknown" {
					code = pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN
				}
				s.Send(&pb.LoginResult{Index: uint32(i), Code: code})
			}
			return nil
		}
	})
	rev, unk := l.authed(0), l.authed(1)
	l.frame(rev, clientpb.Cmd_CMD_LOGIN, 1, loginReq("rev", "d", 1))
	l.frame(unk, clientpb.Cmd_CMD_LOGIN, 1, loginReq("unknown", "d", 1))
	var e clientpb.Error
	waitFor(t, "Error", func() bool { return rev.last(t, clientpb.Cmd_CMD_ERROR, &e) })
	if e.Code != clientpb.ErrorCode_ERROR_CODE_REVOKED {
		t.Fatalf("code %v, want REVOKED", e.Code)
	}
	waitFor(t, "Busy for OUTCOME_UNKNOWN", func() bool { return unk.last(t, clientpb.Cmd_CMD_BUSY, nil) })
}

func TestQueuedLoginPastTheDeadlineIsDroppedBeforeSending(t *testing.T) {
	l := newLive(t, func(c *Config) { shortDeadline(c); c.LoginRPCs, c.LoginBatch = 1, 1 })
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			if tokenSubject(b.Entries[0].GetToken()) == "first" {
				<-release // the only worker is busy
			}
			return s.Send(okResult(0, b.Entries[0]))
		}
	})
	first, late := l.authed(0), l.authed(1)
	l.frame(first, clientpb.Cmd_CMD_LOGIN, 1, loginReq("first", "d", 1))
	waitFor(t, "first sent", func() bool { return get(l.logic, func() int { return len(l.logic.logins) }) == 1 })
	l.frame(late, clientpb.Cmd_CMD_LOGIN, 1, loginReq("late", "d", 1))
	time.Sleep(200 * time.Millisecond) // past login_deadline while queued
	close(release)
	var e clientpb.Error
	waitFor(t, "late gets EXPIRED", func() bool { return late.last(t, clientpb.Cmd_CMD_ERROR, &e) })
	if e.Code != clientpb.ErrorCode_ERROR_CODE_EXPIRED {
		t.Fatalf("code %v, want EXPIRED", e.Code)
	}
	time.Sleep(100 * time.Millisecond)
	if n := get(l.logic, func() int { return len(l.logic.logins) }); n != 1 {
		t.Fatalf("%d Login RPCs, want 1: the late entry must never reach Logic", n)
	}
	l.sync()
	if l.tracked(late) || l.g.loginQueued.Load() != 0 {
		t.Fatal("a dropped Login needs no Logout and frees its queue slot")
	}
}
