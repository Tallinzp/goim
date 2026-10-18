package loadgen

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/sdk"
)

// sdkRun starts n SDK clients on env with the storm window storm, serves
// their control API, and waits until all are online.
func sdkRun(t *testing.T, env Env, n int, storm sdk.Window) (*Runner, *httptest.Server) {
	t.Helper()
	r, err := New(Config{Mode: "sdk", Clients: n, Rate: 300, UIDPrefix: "rst"}, env)
	if err != nil {
		t.Fatal(err)
	}
	r.cfg.windows = sdk.DefaultWindows()
	r.cfg.windows.Storm = storm
	t.Cleanup(run(r))
	srv := httptest.NewServer(r.Handler())
	t.Cleanup(srv.Close)
	waitFor(t, "all online", 10*time.Second, func() bool { s := r.Stats(); return s.RampDone && s.Online == n })
	return r, srv
}

// POST /reset marks every gateway and closes every SDK connection at once:
// every client loses it and logs in again after its storm wait, and each
// gateway sees its clients close (#134). A taken mark closes nothing.
func TestResetDropsEveryConnection(t *testing.T) {
	env, _ := startStack(t, 2)
	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 0, Hi: time.Second})
	code, m := post(t, srv.URL+"/reset?mark=reset")
	if code != http.StatusOK || m["population"] != 20.0 || m["close_ms"] == nil {
		t.Fatalf("/reset: %d %v", code, m)
	}
	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Back == 20 })
	s, _ := r.Recorder().SLO("reset", 5*time.Second)
	st := r.Stats()
	if s.Gateway != "all" || s.Population != 20 || s.Lost != 20 || st.Lost["lost"] != 20 || st.StormSinceMark.Waits["storm"] != 20 {
		t.Fatalf("slo %+v, lost %v, waits %v: want all 20 lost, each after one storm wait", s, st.Lost, st.StormSinceMark.Waits)
	}
	if open := st.BalancerOpen[env.GatewayIDs[0]] + st.BalancerOpen[env.GatewayIDs[1]]; open != 20 {
		t.Fatalf("balancer open %v, want the 20 back", st.BalancerOpen)
	}
	waitFor(t, "the gateways count 20 client closes", 5*time.Second, func() bool {
		return metric(t, env.GatewayIDs[0], "goim_gateway_closes_total", "reason", "client")+
			metric(t, env.GatewayIDs[1], "goim_gateway_closes_total", "reason", "client") >= 20
	})
	if code, _ := post(t, srv.URL+"/reset?mark=reset"); code != http.StatusConflict {
		t.Fatalf("/reset with a taken mark: %d", code)
	}
	if code, _ := post(t, srv.URL+"/reset"); code != http.StatusBadRequest {
		t.Fatalf("/reset without a mark: %d", code)
	}
	if st := r.Stats(); st.Online != 20 || st.Lost["lost"] != 20 {
		t.Fatalf("a refused /reset closed connections: %+v", st.Counts)
	}
}

// abort closes with an RST: the peer's read fails with ECONNRESET, not
// EOF (SO_LINGER 0, #134).
func TestAbortSendsAnRST(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	b, err := NewBalancer([]string{ln.Addr().String()}, 1)
	if err != nil {
		t.Fatal(err)
	}
	nc, _, err := b.Dial(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	(&replyConn{Conn: nc}).abort()
	peer.SetReadDeadline(time.Now().Add(5 * time.Second))
	var buf [1]byte
	if _, err := peer.Read(buf[:]); !errors.Is(err, syscall.ECONNRESET) {
		t.Fatalf("peer read: %v, want ECONNRESET", err)
	}
	if b.Open()[0] != 0 {
		t.Fatalf("balancer open %v, want the slot freed", b.Open())
	}
}

// POST /network-change after a reset cuts every client's storm wait to the
// local trigger's 0–1s (spec: Local trigger): with a 30–60s storm window
// all are back within seconds, each after one storm and one local wait.
func TestNetworkChangeCutsTheStormWait(t *testing.T) {
	env, _ := startStack(t, 2)
	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 30 * time.Second, Hi: 60 * time.Second})
	if code, _ := post(t, srv.URL+"/reset?mark=reset"); code != http.StatusOK {
		t.Fatalf("/reset: %d", code)
	}
	waitFor(t, "all lost", 5*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Lost == 20 })
	waitFor(t, "all in their storm wait", 5*time.Second, func() bool { return r.Stats().StormSinceMark.Waits["storm"] == 20 })
	code, m := post(t, srv.URL+"/network-change")
	if code != http.StatusOK || m["clients"] != 20.0 {
		t.Fatalf("/network-change: %d %v", code, m)
	}
	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("reset", time.Second); return s.Back == 20 })
	if w := r.Stats().StormSinceMark.Waits; w["local"] != 20 {
		t.Fatalf("waits since the mark %v, want 20 local", w)
	}
}

// Outside sdk mode /reset, /network-change and /ghost are refused, and no
// mark is taken: light clients don't follow the SDK's reconnect rules (#134).
func TestResetAndNetworkChangeNeedSDKMode(t *testing.T) {
	env, _ := startStack(t, 1)
	r, err := New(Config{Mode: "light", Clients: 2, Rate: 100, UIDPrefix: "nr", PingEvery: time.Second, Timeout: 5 * time.Second}, env)
	if err != nil {
		t.Fatal(err)
	}
	defer run(r)()
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()
	waitFor(t, "2 online", 10*time.Second, func() bool { return r.Stats().Online == 2 })
	for _, p := range []string{"/reset?mark=reset", "/network-change", "/ghost?mark=reset"} {
		if code, _ := post(t, srv.URL+p); code != http.StatusConflict {
			t.Fatalf("%s in light mode: %d", p, code)
		}
	}
	if _, err := r.Recorder().SLO("reset", time.Second); err == nil || r.Stats().Online != 2 {
		t.Fatal("a refused /reset or /ghost took a mark or closed connections")
	}
}
