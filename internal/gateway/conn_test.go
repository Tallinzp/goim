package gateway

import (
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func rejects(h *core, reason string) int {
	return int(testutil.ToFloat64(h.g.m.Rejects.WithLabelValues(reason)))
}

func TestConnectionCapCountsSocketsNotEntriesAwaitingLogout(t *testing.T) {
	h := newCore(t, func(c *Config) { c.MaxConns = 3 })
	a := h.online(0, "ua")
	h.online(1, "ub")
	h.authed(2)
	if _, reason := h.open(3); reason != RejectAtCapacity {
		t.Fatalf("4th socket: reject %q, want at_capacity", reason)
	}
	if n := rejects(h, RejectAtCapacity); n != 1 {
		t.Fatalf("at_capacity counted %d times", n)
	}
	a.clientClose()
	h.sync()
	if !h.tracked(a) {
		t.Fatal("a closed ONLINE connection must stay tracked until its Logout is settled")
	}
	if _, reason := h.open(3); reason != "" {
		t.Fatalf("after a close, open rejected with %q: an entry awaiting Logout must not count", reason)
	}
}

func TestPendingCapCountsHandshakeAndAuthPendingOnly(t *testing.T) {
	h := newCore(t, func(c *Config) { c.PendingCap = 2 })
	h.online(0, "ua") // ONLINE: not pending
	h.open(0)         // HANDSHAKE
	h.authed(1)       // AUTH_PENDING
	if _, reason := h.open(2); reason != RejectPendingMem {
		t.Fatalf("reject %q, want pending_mem", reason)
	}
	if got := h.g.open.Load(); got != 3 {
		t.Fatalf("open sockets %d after a pending_mem reject, want 3", got)
	}
}

func TestHandshakeTimeoutClosesSilently(t *testing.T) {
	h := newCore(t, nil)
	k, _ := h.open(0)
	h.advance(4 * time.Second)
	if k.isClosed() {
		t.Fatal("closed before the 5s handshake timeout")
	}
	h.advance(2 * time.Second)
	if !k.isClosed() || len(k.sent()) != 0 {
		t.Fatalf("closed=%v frames=%d, want a silent close", k.isClosed(), len(k.sent()))
	}
	h.sync()
	if rejects(h, RejectHandshakeTimeout) != 1 || h.tracked(k) {
		t.Fatal("want one handshake_timeout reject and the entry untracked")
	}
}

func TestAuthTimeoutClosesSilently(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	h.advance(6 * time.Second)
	if !k.isClosed() || len(k.sent()) != 0 || rejects(h, RejectAuthTimeout) != 1 {
		t.Fatal("want a silent close counted as auth_timeout")
	}
}

func TestQueuedLoginIsNotClosedByAuthTimeout(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	h.login(k, "u1") // queued, no reply
	h.advance(5500 * time.Millisecond)
	if k.isClosed() || rejects(h, RejectAuthTimeout) != 0 {
		t.Fatal("a Login waiting past the AUTH_PENDING timeout was closed by it (R4H-O7)")
	}
}

func TestHeartbeatReArmsOnActivityAndFires310sAfterTheLastFrame(t *testing.T) {
	h := newCore(t, nil)
	k := h.online(0, "u1")
	for range 3 { // 3 × 200s with a Ping each time: alive for 600s
		h.advance(200 * time.Second)
		h.frame(k, clientpb.Cmd_CMD_PING, 9, nil)
	}
	if k.isClosed() {
		t.Fatal("closed while frames kept arriving")
	}
	if !k.last(t, clientpb.Cmd_CMD_PONG, nil) {
		t.Fatal("Ping got no Pong")
	}
	h.advance(309 * time.Second)
	if k.isClosed() {
		t.Fatal("closed 309s after the last frame")
	}
	h.advance(2 * time.Second)
	if !k.isClosed() {
		t.Fatal("still open 311s after the last frame")
	}
}

func TestProtocolViolationsClose(t *testing.T) {
	h := newCore(t, nil)
	for name, run := range map[string]func() *fakeLink{
		"Ping before Login": func() *fakeLink {
			k := h.authed(0)
			h.frame(k, clientpb.Cmd_CMD_PING, 1, nil)
			return k
		},
		"undecodable LoginReq": func() *fakeLink {
			k := h.authed(0)
			k.loop.do(func() { h.g.Frame(k.c, clientpb.Cmd_CMD_LOGIN, 1, []byte{0xff, 0xff}) })
			return k
		},
		"second LoginReq": func() *fakeLink {
			k := h.authed(0)
			h.login(k, "u2")
			h.frame(k, clientpb.Cmd_CMD_LOGIN, 2, loginReq("u2", "d", 2))
			return k
		},
		"LoginReq while ONLINE": func() *fakeLink {
			k := h.online(0, "u3")
			h.frame(k, clientpb.Cmd_CMD_LOGIN, 2, loginReq("u3", "d", 2))
			return k
		},
		"frame before the handshake": func() *fakeLink {
			k, _ := h.open(0)
			h.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u4", "d", 1))
			return k
		},
	} {
		if k := run(); !k.isClosed() {
			t.Errorf("%s: not closed", name)
		}
	}
}

func TestLoginRepliesReachTheClient(t *testing.T) {
	h := newCore(t, func(c *Config) {
		c.Windows.StormWait = Window{0, 40 * time.Second}
	})
	t.Run("ok", func(t *testing.T) {
		k := h.online(0, "ok")
		var resp clientpb.LoginResp
		if !k.last(t, clientpb.Cmd_CMD_LOGIN_RESP, &resp) {
			t.Fatal("no LoginResp")
		}
		if resp.HeartbeatS != 150 || resp.StormWait.GetHiMs() != 40_000 || resp.LocalTrigger != nil {
			t.Fatalf("LoginResp %v: want heartbeat 150, storm hi 40s, local trigger absent", &resp)
		}
		if k.sent()[0].reqID != 7 {
			t.Fatal("LoginResp does not echo the LoginReq's req_id")
		}
	})
	t.Run("stale", func(t *testing.T) {
		k := h.authed(1)
		it := h.login(k, "st")
		it.state.Store(itemSent)
		h.reply(it, loginReply{outcome: loginRejected, code: clientpb.ErrorCode_ERROR_CODE_STALE, label: "stale", storedConnSeq: 42})
		var e clientpb.Error
		if !k.last(t, clientpb.Cmd_CMD_ERROR, &e) || e.Code != clientpb.ErrorCode_ERROR_CODE_STALE || e.StoredConnSeq != 42 {
			t.Fatalf("Error %v, want STALE with stored conn_seq 42", &e)
		}
		if !k.isClosed() {
			t.Fatal("a rejected Login must close the connection")
		}
	})
	t.Run("no definitive reply", func(t *testing.T) {
		k := h.authed(2)
		it := h.login(k, "un")
		it.state.Store(itemSent)
		h.reply(it, unknownReply)
		var b clientpb.Busy
		if !k.last(t, clientpb.Cmd_CMD_BUSY, &b) || b.RetryAfterMs < 1000 || b.RetryAfterMs > 3000 {
			t.Fatalf("Busy %v, want retry_after in [1s, 3s]", &b)
		}
		if !k.isClosed() {
			t.Fatal("not closed")
		}
	})
}

func TestLoginQueueCapRepliesBusy(t *testing.T) {
	h := newCore(t, func(c *Config) { c.LoginQueueCap = 2 })
	h.login(h.authed(0), "a")
	h.login(h.authed(1), "b")
	k := h.authed(2)
	h.frame(k, clientpb.Cmd_CMD_LOGIN, 3, loginReq("c", "dc", 1))
	if !k.last(t, clientpb.Cmd_CMD_BUSY, nil) || !k.isClosed() || rejects(h, RejectLoginQueue) != 1 {
		t.Fatal("the third Login must get Busy, close, and count login_queue")
	}
	h.sync()
	if h.tracked(k) {
		t.Fatal("a connection that never sent a Login needs no Logout")
	}
}

func TestClientLogoutRepliesAtOnceThenLogsOut(t *testing.T) {
	h := newCore(t, nil)
	k := h.online(0, "u1")
	h.frame(k, clientpb.Cmd_CMD_LOGOUT, 5, nil)
	if !k.last(t, clientpb.Cmd_CMD_LOGOUT_RESP, nil) || !k.isClosed() {
		t.Fatal("want LogoutResp and a close")
	}
	h.sync()
	it := h.takeLogout()
	if it == nil || it.entry.Uid != "u1" || it.entry.Appid != "app" || it.connID != k.c.id {
		t.Fatalf("Logout %+v, want u1/app for this conn_id", it)
	}
}

// ---- Logout fence (a) and (b) ----

func TestFenceAHoldsLogoutUntilTheLoginReply(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reply      loginReply
		wantLogout bool
	}{
		{"ok", loginReply{outcome: loginOK, label: "ok", uid: "u1"}, true},
		{"rejected", loginReply{outcome: loginRejected, code: clientpb.ErrorCode_ERROR_CODE_SUPERSEDED, label: "superseded"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newCore(t, nil)
			k := h.authed(0)
			it := h.login(k, "u1")
			it.state.Store(itemSent) // the worker sent it
			k.clientClose()          // race 5: close while the Login is in flight
			h.sync()
			if h.takeLogout() != nil {
				t.Fatal("Logout sent before the definitive Login reply")
			}
			h.reply(it, tc.reply)
			got := h.takeLogout()
			if (got != nil) != tc.wantLogout {
				t.Fatalf("Logout queued = %v, want %v", got != nil, tc.wantLogout)
			}
			if !tc.wantLogout && h.tracked(k) {
				t.Fatal("a rejected Login wrote nothing: the entry must be untracked")
			}
		})
	}
}

func TestFenceBReleasesLogoutAfterLoginDeadlinePlusOneSecond(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	it := h.login(k, "u6")
	it.state.Store(itemSent)
	h.reply(it, unknownReply) // race 6: no definitive reply; the client gets Busy
	h.sync()
	h.advance(5900 * time.Millisecond)
	if h.takeLogout() != nil {
		t.Fatal("Logout before recv_ms + login_deadline + 1s")
	}
	h.advance(1100 * time.Millisecond)
	got := h.takeLogout()
	if got == nil {
		t.Fatal("no Logout after fence (b)")
	}
	if got.entry.Uid != "u6" {
		t.Fatalf("Logout uid %q, want the token's sub u6", got.entry.Uid)
	}
	if !h.tracked(k) {
		t.Fatal("untracked before its Logout settled")
	}
}

func TestCloseWhileQueuedNeedsNoLogout(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	it := h.login(k, "u1")
	k.clientClose()
	h.sync()
	if it.state.Load() != itemCancelled || h.tracked(k) || h.g.loginQueued.Load() != 0 {
		t.Fatal("a Login cancelled before it was sent must free its slot and leave nothing tracked")
	}
	h.advance(10 * time.Second)
	if h.takeLogout() != nil {
		t.Fatal("Logout for a Login that never reached Logic")
	}
}

func TestUnreadableTokenNeedsNoLogout(t *testing.T) {
	h := newCore(t, nil)
	k := h.authed(0)
	h.frame(k, clientpb.Cmd_CMD_LOGIN, 1, &clientpb.LoginReq{Token: "garbage", Appid: "app", DeviceId: "d"})
	it := <-h.g.loginCh
	it.state.Store(itemSent)
	k.clientClose()
	h.advance(7 * time.Second)
	if h.takeLogout() != nil || h.tracked(k) {
		t.Fatal("a token without a readable sub can't have been written: no Logout, untracked")
	}
}

func TestTokenSubject(t *testing.T) {
	for tok, want := range map[string]string{
		fakeToken("u-1"):                        "u-1",
		"a.b":                                   "",
		"onlyone":                               "",
		"a.!!!.c":                               "",
		"a." + strings.Repeat("A", 5000) + ".c": "",
	} {
		if got := tokenSubject(tok); got != want {
			t.Errorf("tokenSubject(%.20q) = %q, want %q", tok, got, want)
		}
	}
}
