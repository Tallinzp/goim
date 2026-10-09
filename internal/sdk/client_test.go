package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

var (
	reasonFirst = clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT
	reasonRST   = clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN
	reasonPong  = clientpb.ReconnectReason_RECONNECT_REASON_PONG_TIMEOUT
	reasonLocal = clientpb.ReconnectReason_RECONNECT_REASON_LOCAL_TRIGGER
	reasonDrain = clientpb.ReconnectReason_RECONNECT_REASON_DRAIN
)

// A first connect persists device_id and the next conn_seq before it dials
// (R1-O5), and carries FIRST_CONNECT with offline_ms 0.
func TestFirstConnectPersistsBeforeItDials(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		reqs := make(chan *clientpb.LoginReq, 1)
		var storedAtLogin State
		f := newFake(t, func(_ int, c *fakeConn) {
			storedAtLogin, _ = store.Load()
			reqs <- c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, store, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventOnline)
		req := <-reqs
		if req.DeviceId != c.DeviceID() || req.ConnSeq != 1 || req.ReconnectReason != reasonFirst ||
			req.OfflineMs != 0 || req.Token != "tok" || req.Appid != "app" || req.Platform != clientpb.Platform_PLATFORM_MOBILE {
			t.Fatalf("LoginReq %v", req)
		}
		if storedAtLogin.DeviceID != c.DeviceID() || storedAtLogin.ConnSeq != 1 {
			t.Fatalf("store at dial time %+v, want device %s conn_seq 1", storedAtLogin, c.DeviceID())
		}
		if evs[0].Attempt.ConnSeq != 1 || evs[0].Attempt.Reason != reasonFirst {
			t.Fatalf("attempt %+v", evs[0].Attempt)
		}
		c.Connect() // online: no second attempt
		if r.take(); f.dials() != 1 {
			t.Fatalf("%d dials, want 1", f.dials())
		}
	})
}

// Losing an established connection waits 0–20s, then reconnects with
// RST_FIN and the offline time; conn_seq goes up by one per attempt.
func TestLostConnectionWaitsTheStormWindowThenReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reqs := make(chan *clientpb.LoginReq, 2)
		f := newFake(t, func(n int, c *fakeConn) {
			reqs <- c.online(nil)
			if n == 2 {
				c.pong()
			} // n == 1: the gateway dies (FIN)
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		w := evs[2].Wait
		within(t, "storm", w, WaitStorm, 0, 20*s, reasonRST)
		<-reqs
		time.Sleep(w.D - time.Millisecond)
		expect(t, r.take())
		time.Sleep(time.Millisecond)
		expect(t, r.take(), EventConnecting, EventOnline)
		if req := <-reqs; req.ReconnectReason != reasonRST || req.ConnSeq != 2 || req.OfflineMs != w.D.Milliseconds() {
			t.Fatalf("relogin %v, want RST_FIN, conn_seq 2, offline_ms %d", req, w.D.Milliseconds())
		}
	})
}

// No frame within 10s of a Ping: the client closes and waits the storm
// window with PONG_TIMEOUT (spec: Dead-server detection). A gateway that
// answers keeps the connection.
func TestPongTimeoutClosesAndReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pings := make(chan time.Time, 10)
		f := newFake(t, func(n int, c *fakeConn) {
			c.online(&clientpb.LoginResp{HeartbeatS: 30})
			if n == 2 {
				c.pong()
				return
			}
			for { // n == 1: a dead server that still holds the socket
				cmd, err := c.recv()
				if err != nil {
					return
				}
				if cmd == clientpb.Cmd_CMD_PING {
					pings <- time.Now()
				}
			}
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		start := time.Now()
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline)
		time.Sleep(40*time.Second - time.Millisecond)
		expect(t, r.take())
		if p := <-pings; p.Sub(start) != 30*time.Second {
			t.Fatalf("first Ping at %v, want 30s (the heartbeat)", p.Sub(start))
		}
		time.Sleep(time.Millisecond)
		evs := expect(t, r.take(), EventWaiting)
		within(t, "after the Pong timeout", evs[0].Wait, WaitStorm, 0, 20*s, reasonPong)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		time.Sleep(10 * time.Minute) // 20 heartbeats, each answered
		expect(t, r.take())
	})
}

// A network change during the 0–20s wait reconnects within 0–1s with
// LOCAL_TRIGGER (R4H-O3).
func TestNetworkChangeDuringTheStormWaitReconnectsWithinOneSecond(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reqs := make(chan *clientpb.LoginReq, 2)
		f := newFake(t, func(n int, c *fakeConn) {
			reqs <- c.online(nil)
			if n == 2 {
				c.pong()
			}
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		<-reqs
		time.Sleep(100 * time.Millisecond)
		c.NetworkChanged()
		evs := expect(t, r.take(), EventWaiting)
		within(t, "local", evs[0].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		if req := <-reqs; req.ReconnectReason != reasonLocal || req.OfflineMs != (100*time.Millisecond+evs[0].Wait.D).Milliseconds() {
			t.Fatalf("relogin %v", req)
		}
	})
}

// A network change does not shorten a Busy{retry_after} wait (spec:
// Precedence), nor a Reconnect wait.
func TestNetworkChangeDoesNotShortenRetryAfterOrReconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			switch n {
			case 1:
				c.login()
				c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{RetryAfterMs: 7000})
			case 2:
				c.online(nil)
				c.send(clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: 60_000})
				c.drain()
			default:
				c.online(nil)
				c.pong()
			}
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		within(t, "Busy", evs[1].Wait, WaitRetryable, 7*s, 7*s, clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY)
		c.NetworkChanged()
		c.Foreground()
		time.Sleep(7*time.Second - time.Millisecond)
		expect(t, r.take())
		time.Sleep(time.Millisecond)
		evs = expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		w := evs[2].Wait
		within(t, "Reconnect", w, WaitDrain, 0, 60*s, reasonDrain)
		c.NetworkChanged()
		time.Sleep(w.D - time.Millisecond)
		expect(t, r.take())
		time.Sleep(time.Millisecond)
		evs = expect(t, r.take(), EventConnecting, EventOnline)
		if evs[0].Attempt.Reason != reasonDrain {
			t.Fatalf("attempt after Reconnect %+v, want DRAIN", evs[0].Attempt)
		}
	})
}

// On Reconnect the client closes the connection itself (R4H-O3).
func TestReconnectClosesTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closed := make(chan error, 1)
		f := newFake(t, func(n int, c *fakeConn) {
			if n > 1 {
				return
			}
			c.online(nil)
			c.send(clientpb.Cmd_CMD_RECONNECT, 0, &clientpb.Reconnect{JitterMs: 5000})
			_, err := c.recv()
			closed <- err
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		r.take()
		if err := <-closed; err == nil {
			t.Fatal("the client kept the connection after Reconnect")
		}
	})
}

// Logout, account switch and app close each cancel a pending retry (R4H-O2).
func TestStopConditionsCancelAPendingRetry(t *testing.T) {
	busy := func(_ int, c *fakeConn) {
		c.login()
		c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{RetryAfterMs: 2000})
	}
	for _, tc := range []struct {
		name string
		stop func(*Client)
	}{
		{"logout", (*Client).Logout},
		{"app close", (*Client).Close},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, busy)
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				expect(t, r.take(), EventConnecting, EventWaiting)
				tc.stop(c)
				time.Sleep(time.Hour)
				r.take()
				if f.dials() != 1 {
					t.Fatalf("%d dials after %s, want 1", f.dials(), tc.name)
				}
			})
		})
	}
	t.Run("account switch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			reqs := make(chan *clientpb.LoginReq, 2)
			f := newFake(t, func(n int, c *fakeConn) {
				if n == 1 {
					busy(n, c)
					return
				}
				reqs <- c.online(nil)
				c.pong()
			})
			c, r := testClient(t, f, &MemStore{}, nil)
			c.Connect()
			expect(t, r.take(), EventConnecting, EventWaiting)
			c.SwitchAccount(func(context.Context, bool) (string, error) { return "tok-b", nil })
			expect(t, r.take(), EventLoggedOut, EventConnecting, EventOnline)
			if req := <-reqs; req.Token != "tok-b" || req.ReconnectReason != reasonFirst {
				t.Fatalf("after the switch %v", req)
			}
			time.Sleep(time.Hour)
			if r.take(); f.dials() != 2 {
				t.Fatalf("%d dials, want 2: the Busy retry must be cancelled", f.dials())
			}
		})
	})
}

// Logout sends Logout, waits for LogoutResp, closes and stays idle.
func TestLogoutSendsLogoutAndStaysIdle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := make(chan clientpb.Cmd, 1)
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			cmd, _ := c.recv()
			got <- cmd
			c.send(clientpb.Cmd_CMD_LOGOUT_RESP, 1, nil)
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting, EventOnline)
		start := time.Now()
		c.Logout()
		if d := time.Since(start); d != 0 {
			t.Fatalf("Logout took %v; LogoutResp came at once", d)
		}
		if cmd := <-got; cmd != clientpb.Cmd_CMD_LOGOUT {
			t.Fatalf("gateway got %v, want Logout", cmd)
		}
		expect(t, r.take(), EventLoggedOut)
		time.Sleep(time.Hour)
		expect(t, r.take())
		c.Foreground()
		c.NetworkChanged()
		if r.take(); f.dials() != 1 {
			t.Fatal("reconnected after Logout without Connect")
		}
	})
}

// A gateway that never answers Logout costs at most LogoutTimeout.
func TestLogoutWaitsAtMostLogoutTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) { c.online(nil); c.drain() })
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		r.take()
		start := time.Now()
		c.Logout()
		if d := time.Since(start); d != 2*time.Second {
			t.Fatalf("Logout took %v, want the 2s LogoutTimeout", d)
		}
	})
}

// No Kick reconnects on its own (races 9, 10). REPLACED is an anomaly and
// ends on Foreground; KICKED_BY_OTHER_DEVICE only on Connect; REVOKED asks
// for a new login.
func TestKicksDoNotReconnect(t *testing.T) {
	for _, tc := range []struct {
		reason clientpb.KickReason
		want   []EventKind
		wakes  func(*Client) // brings the client back, or nil
		wakeTo clientpb.ReconnectReason
	}{
		{clientpb.KickReason_KICK_REASON_REPLACED, []EventKind{EventAnomaly, EventIdle}, (*Client).Foreground, reasonLocal},
		{clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, []EventKind{EventIdle}, (*Client).Connect, reasonFirst},
		{clientpb.KickReason_KICK_REASON_REVOKED, []EventKind{EventLoginRequired}, (*Client).Connect, reasonFirst},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(n int, c *fakeConn) {
					c.online(nil)
					if n == 1 {
						c.send(clientpb.Cmd_CMD_KICK, 0, &clientpb.Kick{Reason: tc.reason})
						return
					}
					c.pong()
				})
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				evs := expect(t, r.take(), append([]EventKind{EventConnecting, EventOnline}, tc.want...)...)
				if last := evs[len(evs)-1]; last.Kick != tc.reason {
					t.Fatalf("event %+v, want Kick %v", last, tc.reason)
				}
				c.NetworkChanged()
				if tc.reason != clientpb.KickReason_KICK_REASON_REPLACED {
					c.Foreground()
				}
				time.Sleep(time.Hour)
				expect(t, r.take())
				tc.wakes(c)
				time.Sleep(time.Second)
				evs = r.take()
				if f.dials() != 2 || evs[len(evs)-1].Kind != EventOnline {
					t.Fatalf("after waking: %d dials, events %v", f.dials(), kinds(evs))
				}
				for _, e := range evs {
					if e.Kind == EventConnecting && e.Attempt.Reason != tc.wakeTo {
						t.Fatalf("woke with %v, want %v", e.Attempt.Reason, tc.wakeTo)
					}
				}
			})
		})
	}
}

// A Kick in place of the Login reply (kick-pending, R1-O8) is handled the
// same way.
func TestKickInsteadOfTheLoginReply(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.login()
			c.send(clientpb.Cmd_CMD_KICK, 0, &clientpb.Kick{Reason: clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE})
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting, EventIdle)
	})
}

// STALE sets the counter to max(local, stored) + 1, persisted (R1-O5).
func TestStaleMovesConnSeqPastTheStoredOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		reqs := make(chan *clientpb.LoginReq, 2)
		f := newFake(t, func(n int, c *fakeConn) {
			if n == 1 {
				c.login()
				c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_STALE, StoredConnSeq: 41})
				return
			}
			reqs <- c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, store, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		if evs[1].Code != clientpb.ErrorCode_ERROR_CODE_STALE {
			t.Fatalf("waiting event %+v, want Code STALE", evs[1])
		}
		if st, _ := store.Load(); st.ConnSeq != 41 {
			t.Fatalf("stored conn_seq %d after STALE, want 41", st.ConnSeq)
		}
		time.Sleep(evs[1].Wait.D)
		r.take()
		if req := <-reqs; req.ConnSeq != 42 {
			t.Fatalf("retry conn_seq %d, want 42", req.ConnSeq)
		}
	})
}

// Each server behaviour lands in its rule (spec: Retryable rejects).
func TestEachEndingIsClassified(t *testing.T) {
	for _, tc := range []struct {
		name  string
		serve func(*fakeConn)
		kind  WaitKind
		lo    time.Duration
		hi    time.Duration
		after time.Duration // time the ending takes
	}{
		{"closed before the handshake reply", func(c *fakeConn) { c.hello() }, WaitPreHandshake, ms(500), ms(1500), 0},
		{"closed after the handshake, before any reply", func(c *fakeConn) { c.login() }, WaitRetryable, s, 3 * s, 0},
		{"no reply within LoginTimeout", func(c *fakeConn) { c.login(); c.drain() }, WaitRetryable, s, 3 * s, 10 * s},
		{"EXPIRED", func(c *fakeConn) {
			c.login()
			c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_EXPIRED})
		}, WaitRetryable, s, 3 * s, 0},
		{"KEY_UNKNOWN", func(c *fakeConn) {
			c.login()
			c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN})
		}, WaitRetryable, s, 3 * s, 0},
		{"Busy without retry_after", func(c *fakeConn) { c.login(); c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{}) }, WaitRetryable, s, 3 * s, 0},
		{"handshake reply that fails", func(c *fakeConn) { c.hello(); c.nc.Write(make([]byte, 54)) }, WaitFailure, ms(2500), 5 * s, 0},
		{"no handshake reply within ConnectTimeout", func(c *fakeConn) { c.hello(); c.drain() }, WaitFailure, ms(2500), 5 * s, 10 * s},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(_ int, c *fakeConn) { tc.serve(c) })
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				time.Sleep(tc.after)
				evs := expect(t, r.take(), EventConnecting, EventWaiting)
				w := evs[1].Wait
				if w.Kind != tc.kind || w.D < tc.lo || w.D > tc.hi {
					t.Fatalf("wait %+v, want %v in [%v, %v]", w, tc.kind, tc.lo, tc.hi)
				}
			})
		})
	}
}

// The counters run across real attempts: 10 quick pre-handshake retries,
// then backoff.
func TestPreHandshakeClosesReachBackoffAfterTen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) { c.hello() })
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		var waits []WaitKind
		for len(waits) < 12 {
			for _, e := range r.take() {
				if e.Kind == EventWaiting {
					waits = append(waits, e.Wait.Kind)
					time.Sleep(e.Wait.D)
				}
			}
		}
		for i, k := range waits {
			if want := map[bool]WaitKind{true: WaitPreHandshake, false: WaitBackoff}[i < 10]; k != want {
				t.Fatalf("wait %d is %v, want %v (all: %v)", i+1, k, want, waits)
			}
		}
	})
}

// Windows from LoginResp are used, clamped and persisted across app
// restarts; absent fields keep the current value (R5S-O6).
func TestServerWindowsAreUsedAndPersistedAcrossRestarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		f := newFake(t, func(n int, c *fakeConn) {
			switch n {
			case 1:
				c.online(&clientpb.LoginResp{HeartbeatS: 150, StormWait: pbw(0, 5*s), Retryable: pbw(4*s, 6*s)})
			case 2:
				c.online(&clientpb.LoginResp{HeartbeatS: 150}) // no windows: keep them
				c.pong()
			default:
				c.login()
				c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_EXPIRED})
			}
		})
		c, r := testClient(t, f, store, nil)
		c.Connect()
		evs := expect(t, r.take(), EventConnecting, EventOnline, EventWaiting)
		// Persisted when LoginResp arrives, not only with the next conn_seq:
		// an app killed while online restarts with them.
		want := ServerWindows{Storm: Window{0, 10 * s}, Retryable: Window{4 * s, 6 * s}} // 5s clamped to 10s
		if st, _ := store.Load(); st.Windows != want || st.ConnSeq != 1 {
			t.Fatalf("persisted %+v, want windows %+v at conn_seq 1", st, want)
		}
		within(t, "storm after the server's window", evs[2].Wait, WaitStorm, 0, 10*s, reasonRST)
		time.Sleep(evs[2].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		if st, _ := store.Load(); st.Windows != want {
			t.Fatalf("a LoginResp without windows changed them: %+v", st.Windows)
		}
		c.Close()

		// The app restarts on the same store: the next wait uses the
		// persisted window before any LoginResp.
		c2, r2 := testClient(t, f, store, nil)
		c2.Connect()
		evs = expect(t, r2.take(), EventConnecting, EventWaiting)
		within(t, "EXPIRED after restart", evs[1].Wait, WaitRetryable, 4*s, 6*s, clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY)
	})
}

// A conn_seq that can't be persisted is never used: no dial, a backoff.
func TestFailedSaveSkipsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &MemStore{}
		f := newFake(t, func(_ int, c *fakeConn) { c.online(nil); c.pong() })
		c, r := testClient(t, f, store, nil) // New saves the device_id first
		store.Err = errors.New("disk full")
		c.Connect()
		evs := expect(t, r.take(), EventAnomaly, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure || f.dials() != 0 {
			t.Fatalf("wait %+v, %d dials; want a failure backoff and no dial", evs[1].Wait, f.dials())
		}
		store.mu.Lock()
		store.Err = nil
		store.mu.Unlock()
		time.Sleep(evs[1].Wait.D)
		evs = expect(t, r.take(), EventConnecting, EventOnline)
		if evs[0].Attempt.ConnSeq != 1 {
			t.Fatalf("conn_seq %d, want 1: the failed one was never used", evs[0].Attempt.ConnSeq)
		}
	})
}

// New refuses a config without the essentials and gives zero timing fields
// their defaults, so a zero PongTimeout can't close every connection at once.
func TestNewChecksAndCompletesTheConfig(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New accepted an empty config")
	}
	c, err := New(Config{Addrs: []string{"gw:8000"}, ServerKey: make([]byte, 32), Store: &MemStore{},
		Token: func(context.Context, bool) (string, error) { return "", nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	def := DefaultConfig()
	if c.cfg.PongTimeout != def.PongTimeout || c.cfg.ConnectTimeout != def.ConnectTimeout ||
		c.cfg.LoginTimeout != def.LoginTimeout || c.cfg.LogoutTimeout != def.LogoutTimeout ||
		c.cfg.TokenTimeout != def.TokenTimeout || c.cfg.Heartbeat != def.Heartbeat ||
		c.cfg.Backoff != def.Backoff || c.cfg.Windows != def.Windows {
		t.Fatalf("config not completed: %+v", c.cfg)
	}
	if c.DeviceID() == "" {
		t.Fatal("no device_id on a fresh install")
	}
	if _, err := New(Config{Addrs: []string{"gw:8000"}, ServerKey: make([]byte, 32), Store: &MemStore{},
		Token:     func(context.Context, bool) (string, error) { return "", nil },
		Heartbeat: HeartbeatRange{Min: time.Minute, Max: time.Second}}); err == nil {
		t.Fatal("New accepted an inverted heartbeat range")
	}
}

// Review Focus 1: a corrupt record starts the client as a fresh install
// instead of keeping the app offline. The anomaly is the run loop's first
// event.
func TestCorruptStoreStartsFresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
		os.WriteFile(store.Path, []byte("{not json"), 0o600)
		r := &rec{}
		cfg := DefaultConfig()
		cfg.Addrs, cfg.ServerKey, cfg.Store, cfg.OnEvent = []string{"gw:8000"}, make([]byte, 32), store, r.add
		cfg.Token = func(context.Context, bool) (string, error) { return "", nil }
		c, err := New(cfg)
		if err != nil {
			t.Fatalf("New on a corrupt store: %v", err)
		}
		defer c.Close()
		if evs := expect(t, r.take(), EventAnomaly); !errors.Is(evs[0].Err, ErrCorrupt) {
			t.Fatalf("anomaly %v, want ErrCorrupt", evs[0].Err)
		}
		if st, err := store.Load(); err != nil || st.DeviceID != c.DeviceID() || c.DeviceID() == "" {
			t.Fatalf("store after New: %+v, %v; want the new device_id saved", st, err)
		}
	})
}

// unreadable is a store whose Load fails while err is set, as an iOS
// Keychain does before the first unlock.
type unreadable struct {
	MemStore
	err error
}

func (u *unreadable) Load() (State, error) {
	u.mu.Lock()
	err := u.err
	u.mu.Unlock()
	if err != nil {
		return State{}, err
	}
	return u.MemStore.Load()
}

// A store that can't be read yet keeps its record: no new device_id, no
// Save, no dial, and the client reads it again before the next attempt.
func TestUnreadableStoreKeepsItsRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &unreadable{err: errors.New("keychain locked")}
		store.MemStore.Save(State{DeviceID: "dev-1", ConnSeq: 7})
		reqs := make(chan *clientpb.LoginReq, 1)
		f := newFake(t, func(_ int, c *fakeConn) { reqs <- c.online(nil); c.pong() })
		c, r := testClient(t, f, &store.MemStore, func(cfg *Config) { cfg.Store = store })
		expect(t, r.take(), EventAnomaly)
		if c.DeviceID() != "" {
			t.Fatalf("device_id %q before the store was read", c.DeviceID())
		}
		c.Connect()
		evs := expect(t, r.take(), EventAnomaly, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure || f.dials() != 0 {
			t.Fatalf("wait %+v, %d dials; want a failure backoff and no dial", evs[1].Wait, f.dials())
		}
		if st, _ := store.MemStore.Load(); st != (State{DeviceID: "dev-1", ConnSeq: 7}) {
			t.Fatalf("record changed while unreadable: %+v", st)
		}
		store.mu.Lock()
		store.err = nil
		store.mu.Unlock()
		time.Sleep(evs[1].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
		if req := <-reqs; req.DeviceId != "dev-1" || req.ConnSeq != 8 || c.DeviceID() != "dev-1" {
			t.Fatalf("login %v, device %q; want dev-1 at conn_seq 8", req, c.DeviceID())
		}
	})
}

// Two clients on one FileStore would share a device_id: the second New fails.
func TestSecondClientOnTheSameStoreIsRefused(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Addrs, cfg.ServerKey = []string{"gw:8000"}, make([]byte, 32)
	cfg.Store = FileStore{Path: filepath.Join(t.TempDir(), "goim.json")}
	cfg.Token = func(context.Context, bool) (string, error) { return "", nil }
	a, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg); !errors.Is(err, ErrStoreInUse) {
		t.Fatalf("second New = %v, want ErrStoreInUse", err)
	}
	a.Close()
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New after Close: %v", err)
	}
	if b.DeviceID() != a.DeviceID() {
		t.Fatalf("device_id %q after restart, want %q", b.DeviceID(), a.DeviceID())
	}
	b.Close()
}

// After TOKEN_INVALID the next Token call asks for a new token; once a Login
// is answered otherwise, it doesn't (choice #49, review item 2).
func TestTokenRefreshAfterTokenInvalid(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			switch n {
			case 1:
				c.login()
				c.send(clientpb.Cmd_CMD_ERROR, 1, &clientpb.Error{Code: clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID})
			case 2:
				c.hello() // a pre-handshake close: no Login reply, refresh stays
			case 3:
				c.online(nil) // n == 3 dies after login
			default:
				c.online(nil)
				c.pong()
			}
		})
		var mu sync.Mutex
		var got []bool
		c, r := testClient(t, f, &MemStore{}, func(cfg *Config) {
			cfg.Token = func(_ context.Context, refresh bool) (string, error) {
				mu.Lock()
				got = append(got, refresh)
				mu.Unlock()
				return "tok", nil
			}
		})
		c.Connect()
		for f.dials() < 4 {
			for _, e := range r.take() {
				if e.Kind == EventWaiting {
					time.Sleep(e.Wait.D)
				}
			}
		}
		r.take()
		mu.Lock()
		defer mu.Unlock()
		if want := []bool{false, true, true, false}; fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("refresh %v, want %v", got, want)
		}
	})
}

// A Token call that hangs costs TokenTimeout, then a failure backoff that a
// network change cuts short (review item 3).
func TestTokenTimeoutIsAFailureBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) { c.online(nil); c.pong() })
		var calls atomic.Int32
		c, r := testClient(t, f, &MemStore{}, func(cfg *Config) {
			cfg.Token = func(ctx context.Context, _ bool) (string, error) {
				if calls.Add(1) == 1 {
					<-ctx.Done() // a token server on a dead network
					return "", ctx.Err()
				}
				return "tok", nil
			}
		})
		c.Connect()
		time.Sleep(10*time.Second - time.Millisecond)
		expect(t, r.take(), EventConnecting)
		time.Sleep(time.Millisecond)
		evs := expect(t, r.take(), EventWaiting)
		if evs[0].Wait.Kind != WaitFailure || !errors.Is(evs[0].Err, context.DeadlineExceeded) || f.dials() != 0 {
			t.Fatalf("wait %+v err %v, %d dials; want a failure backoff after the token timeout", evs[0].Wait, evs[0].Err, f.dials())
		}
		c.NetworkChanged()
		evs = expect(t, r.take(), EventWaiting)
		within(t, "network change", evs[0].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
	})
}

// An unknown or undecodable Error backs off with an anomaly (choice #49,
// review item 4).
func TestUnknownErrorCodeBacksOffWithAnAnomaly(t *testing.T) {
	for name, body := range map[string][]byte{"code 99": {0x08, 99}, "undecodable": {0xff}} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(_ int, c *fakeConn) {
					c.login()
					c.sess.WriteFrame(c.nc, uint16(clientpb.Cmd_CMD_ERROR), 1, body)
				})
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				evs := expect(t, r.take(), EventConnecting, EventAnomaly, EventWaiting)
				if evs[1].Err == nil || evs[2].Wait.Kind != WaitBackoff {
					t.Fatalf("anomaly %+v, wait %+v; want an anomaly and a backoff", evs[1], evs[2].Wait)
				}
			})
		})
	}
}

// A heartbeat outside the app's range is clamped with an anomaly; a test
// profile lowers the range (review item 6).
func TestHeartbeatIsClampedToItsRange(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sent    uint32
		rng     HeartbeatRange
		first   time.Duration
		anomaly bool
	}{
		{"5s below 30s", 5, DefaultHeartbeatRange(), 30 * s, true},
		{"1h above 300s", 3600, DefaultHeartbeatRange(), 300 * s, true},
		{"test profile 5s", 5, HeartbeatRange{Min: 5 * s, Max: 30 * s}, 5 * s, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				pings := make(chan time.Time, 1)
				f := newFake(t, func(_ int, c *fakeConn) {
					c.online(&clientpb.LoginResp{HeartbeatS: tc.sent})
					if cmd, err := c.recv(); err == nil && cmd == clientpb.Cmd_CMD_PING {
						pings <- time.Now()
					}
					c.pong()
				})
				c, r := testClient(t, f, &MemStore{}, func(cfg *Config) { cfg.Heartbeat = tc.rng })
				start := time.Now()
				c.Connect()
				want := []EventKind{EventConnecting, EventOnline}
				if tc.anomaly {
					want = []EventKind{EventConnecting, EventAnomaly, EventOnline}
				}
				expect(t, r.take(), want...)
				if p := <-pings; p.Sub(start) != tc.first {
					t.Fatalf("first Ping at %v, want %v", p.Sub(start), tc.first)
				}
			})
		})
	}
}

// A LogoutResp the client didn't ask for is ignored with an anomaly; the
// connection stays (review item 7).
func TestUnsolicitedLogoutRespIsIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			c.send(clientpb.Cmd_CMD_LOGOUT_RESP, 1, nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(10 * time.Minute)
		expect(t, r.take(), EventConnecting, EventOnline, EventAnomaly)
		if f.dials() != 1 {
			t.Fatalf("%d dials, want 1", f.dials())
		}
	})
}

// Review Focus 2: a frame the SDK doesn't know (sub-project 2 adds message
// frames) is ignored and keeps the connection.
func TestUnknownServerFramesAreIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(nil)
			c.send(clientpb.Cmd(999), 0, nil)
			c.send(clientpb.Cmd_CMD_PONG, 77, nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(10 * time.Minute)
		expect(t, r.take(), EventConnecting, EventOnline)
	})
}

// Review Focus 3: a LoginResp without a heartbeat uses the 150s default.
func TestMissingHeartbeatUsesTheDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pings := make(chan time.Time, 1)
		f := newFake(t, func(_ int, c *fakeConn) {
			c.online(&clientpb.LoginResp{})
			if cmd, err := c.recv(); err == nil && cmd == clientpb.Cmd_CMD_PING {
				pings <- time.Now()
			}
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		start := time.Now()
		c.Connect()
		r.take()
		time.Sleep(151 * time.Second)
		if p := <-pings; p.Sub(start) != 150*time.Second {
			t.Fatalf("first Ping at %v, want 150s", p.Sub(start))
		}
	})
}

// Review Focus 4: a blackholed gateway (no handshake reply) costs one
// ConnectTimeout, then a failure backoff that a network change cuts short.
func TestBlackholedGatewayBacksOffAndANetworkChangeCutsIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			if n == 1 {
				c.hello()
				c.drain()
				return
			}
			c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(10 * time.Second)
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure {
			t.Fatalf("wait %+v, want a failure backoff", evs[1].Wait)
		}
		c.NetworkChanged()
		evs = expect(t, r.take(), EventWaiting)
		within(t, "network change", evs[0].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[0].Wait.D)
		expect(t, r.take(), EventConnecting, EventOnline)
	})
}

// Review Focus 5: Logout while the Login is in flight closes at once (the
// gateway's fence (a) sends the Logout after the Login reply) and never
// reconnects.
func TestLogoutDuringAnAttemptClosesItAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		closed := make(chan error, 1)
		f := newFake(t, func(_ int, c *fakeConn) {
			c.login() // no reply: the Login is "in flight"
			_, err := c.recv()
			closed <- err
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		expect(t, r.take(), EventConnecting)
		start := time.Now()
		c.Logout()
		if d := time.Since(start); d != 0 {
			t.Fatalf("Logout took %v, want no wait: nothing is logged in", d)
		}
		if err := <-closed; err == nil {
			t.Fatal("the connection stayed open after Logout")
		}
		time.Sleep(time.Hour)
		expect(t, r.take(), EventLoggedOut)
		if f.dials() != 1 {
			t.Fatalf("%d dials, want 1", f.dials())
		}
	})
}

// Close may be called from several goroutines at once (app teardown and an
// account-switch handler); none of them may panic.
func TestConcurrentCloseDoesNotPanic(t *testing.T) {
	for range 500 {
		cfg := DefaultConfig()
		cfg.Addrs, cfg.Store, cfg.ServerKey = []string{"gw-a:8000"}, &MemStore{}, make([]byte, 32)
		cfg.Token = func(context.Context, bool) (string, error) { return "tok", nil }
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { <-start; c.Close() })
		}
		close(start)
		wg.Wait()
	}
}

// Choice #51 (amended, final review I1): a network change while an attempt
// is in flight is remembered. If that attempt fails, the failure backoff it
// would schedule is cut to the local window, once.
func TestNetworkChangeDuringAFailingAttemptCutsItsBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			if n <= 2 {
				c.hello()
				c.drain() // blackholed: no handshake reply
				return
			}
			c.online(nil)
			c.pong()
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(5 * time.Second)
		c.NetworkChanged() // the attempt is still waiting for its handshake reply
		time.Sleep(5 * time.Second)
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		within(t, "after the trigger", evs[1].Wait, WaitLocal, 0, s, reasonLocal)
		time.Sleep(evs[1].Wait.D + 10*time.Second) // the second attempt is blackholed too
		evs = expect(t, r.take(), EventConnecting, EventWaiting)
		if evs[1].Wait.Kind != WaitFailure {
			t.Fatalf("second wait %+v, want a failure backoff: the trigger is used once", evs[1].Wait)
		}
	})
}

// The remembered trigger never cuts a server's answer (spec: Precedence).
func TestNetworkChangeDuringAnAttemptDoesNotCutRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t, func(n int, c *fakeConn) {
			c.login()
			time.Sleep(3 * time.Second)
			c.send(clientpb.Cmd_CMD_BUSY, 1, &clientpb.Busy{RetryAfterMs: 2000})
		})
		c, r := testClient(t, f, &MemStore{}, nil)
		c.Connect()
		time.Sleep(time.Second)
		c.NetworkChanged()
		time.Sleep(3 * time.Second)
		evs := expect(t, r.take(), EventConnecting, EventWaiting)
		if w := evs[1].Wait; w.Kind != WaitRetryable || w.D != 2*time.Second {
			t.Fatalf("wait %+v, want retry_after 2s", w)
		}
	})
}

// Choice #50 (amended, final review M2): Logout and SwitchAccount reset both
// counters and the backoff, so the next account starts fresh.
func TestLogoutAndSwitchAccountResetTheCountersAndTheBackoff(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reset func(*Client)
	}{
		{"logout", func(c *Client) { c.Logout(); c.Connect() }},
		{"switch account", func(c *Client) {
			c.SwitchAccount(func(context.Context, bool) (string, error) { return "tok-b", nil })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFake(t, func(_ int, c *fakeConn) { c.hello() }) // every attempt: a pre-handshake close
				c, r := testClient(t, f, &MemStore{}, nil)
				c.Connect()
				// 10 quick retries, then 2 backoffs: steps = 2.
				for n := 0; n < 12; {
					for _, e := range r.take() {
						if e.Kind == EventWaiting {
							n++
							if n < 12 {
								time.Sleep(e.Wait.D)
							}
						}
					}
				}
				tc.reset(c) // cancels the pending backoff
				var waits []Wait
				for len(waits) < 11 {
					for _, e := range r.take() {
						if e.Kind == EventWaiting {
							waits = append(waits, e.Wait)
							time.Sleep(e.Wait.D)
						}
					}
				}
				if waits[0].Kind != WaitPreHandshake {
					t.Fatalf("first wait after %s is %v, want a quick retry", tc.name, waits[0].Kind)
				}
				if w := waits[10]; w.Kind != WaitBackoff || w.D < 2500*time.Millisecond || w.D > 5*time.Second {
					t.Fatalf("11th wait %v %v, want backoff step 1 (2.5–5s)", w.Kind, w.D)
				}
			})
		})
	}
}
