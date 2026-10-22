package loadgen

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/sdk"
)

// POST /ghost loses every connection to its client while the gateways
// still hold it ONLINE: no gateway sees a client close. After a network
// change every client is back, and each relogin's Kick closes its ghost,
// which loadgen then closes and counts (review O1).
func TestGhostKeepsTheGatewaysConnections(t *testing.T) {
	env, _ := startStack(t, 2)
	r, srv := sdkRun(t, env, 20, sdk.Window{Lo: 30 * time.Second, Hi: 60 * time.Second})
	code, m := post(t, srv.URL+"/ghost?mark=ghost")
	if code != http.StatusOK || m["population"] != 20.0 || m["ms"] == nil {
		t.Fatalf("/ghost: %d %v", code, m)
	}
	waitFor(t, "all lost", 5*time.Second, func() bool { s, _ := r.Recorder().SLO("ghost", time.Second); return s.Lost == 20 })
	gwSum := func(name, label, value string) float64 {
		return metric(t, env.GatewayIDs[0], name, label, value) + metric(t, env.GatewayIDs[1], name, label, value)
	}
	time.Sleep(200 * time.Millisecond) // a close would reach the gateways by now
	if st := r.Stats(); st.GhostsOpen != 20 || st.Lost["lost"] != 20 {
		t.Fatalf("ghosts open %d, lost %v: want 20 kept, 20 lost", st.GhostsOpen, st.Lost)
	}
	if c, on := gwSum("goim_gateway_closes_total", "reason", "client"), gwSum("goim_gateway_connections", "state", "online"); c != 0 || on != 20 {
		t.Fatalf("the gateways saw %v client closes and hold %v online, want 0 and the 20 ghosts", c, on)
	}
	if code, _ := post(t, srv.URL+"/network-change"); code != http.StatusOK {
		t.Fatalf("/network-change: %d", code)
	}
	waitFor(t, "all back", 10*time.Second, func() bool { s, _ := r.Recorder().SLO("ghost", time.Second); return s.Back == 20 })
	waitFor(t, "every ghost kicked and closed", 10*time.Second, func() bool { return r.Stats().GhostsOpen == 0 })
	st := r.Stats()
	var kick, silent int64
	for _, g := range st.GhostsClosed {
		kick, silent = kick+g.Kick, silent+g.Silent
	}
	if kick != 20 || silent != 0 || gwSum("goim_gateway_closes_total", "reason", "kick") != 20 {
		t.Fatalf("ghosts closed %v, gateway kicks %v: want 20 by Kick", st.GhostsClosed, gwSum("goim_gateway_closes_total", "reason", "kick"))
	}
	if open := st.BalancerOpen[env.GatewayIDs[0]] + st.BalancerOpen[env.GatewayIDs[1]]; open != 20 || st.Lost["lost"] != 20 {
		t.Fatalf("balancer open %v, lost %v: want the 20 back and no other loss", st.BalancerOpen, st.Lost)
	}
	if code, _ := post(t, srv.URL+"/ghost?mark=ghost"); code != http.StatusConflict {
		t.Fatalf("/ghost with a taken mark: %d", code)
	}
}

// A ghosted connection fails its reads and writes at once, nothing it
// writes reaches the peer, and its socket stays open until the peer closes
// it; then the keeper closes it, frees its balancer slot and counts the
// close: "silent" with no data first, "kick" after a frame.
func TestGhostKeeperWaitsForThePeer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	b, err := NewBalancer([]string{ln.Addr().String()}, 1)
	if err != nil {
		t.Fatal(err)
	}
	r := &Runner{ghosts: newGhosts(1), rec: NewRecorder(1, []string{"gw-0"})}
	defer r.ghosts.stop()
	sc := &sdkClient{r: r}
	ghost := func() (*replyConn, net.Conn) {
		nc, gw, err := b.Dial(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		peer, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		rc := sc.track(nc, sc.dials.Add(1))
		rc.gw = gw
		if !rc.ghost() {
			t.Fatal("ghost refused an open connection")
		}
		return rc, peer
	}
	rc, peer := ghost()
	var buf [8]byte
	if _, err := rc.Read(buf[:]); !errors.Is(err, errGhost) {
		t.Fatalf("ghost read: %v", err)
	}
	if _, err := rc.Write([]byte("ping")); !errors.Is(err, errGhost) {
		t.Fatalf("ghost write: %v", err)
	}
	rc.Close() // the SDK ends the connection: the keeper takes the socket
	peer.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := peer.Read(buf[:]); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("peer read %d, %v: want silence (no byte, no close)", n, err)
	}
	if r.ghosts.open.Load() != 1 || b.Open()[0] != 1 {
		t.Fatalf("ghosts open %d, balancer %v: want the socket kept", r.ghosts.open.Load(), b.Open())
	}
	peer.Close()
	waitFor(t, "the silent ghost closed", 5*time.Second, func() bool { return r.ghosts.open.Load() == 0 })
	if r.ghosts.silent[0].Load() != 1 || b.Open()[0] != 0 {
		t.Fatalf("silent %d, balancer %v: want 1 silent close, the slot freed", r.ghosts.silent[0].Load(), b.Open())
	}

	rc, peer = ghost()
	rc.Close()
	peer.Write([]byte("kick"))
	peer.Close()
	waitFor(t, "the kicked ghost closed", 5*time.Second, func() bool { return r.ghosts.open.Load() == 0 })
	if r.ghosts.kick[0].Load() != 1 || r.ghosts.silent[0].Load() != 1 {
		t.Fatalf("kick %d, silent %d: want one each", r.ghosts.kick[0].Load(), r.ghosts.silent[0].Load())
	}

	rc, peer = ghost() // a /reset after the /ghost leaves the kept socket (M2)
	rc.Close()
	rc.abort()
	peer.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	if n, err := peer.Read(buf[:]); !errors.Is(err, os.ErrDeadlineExceeded) || r.ghosts.open.Load() != 1 {
		t.Fatalf("peer read %d, %v, ghosts open %d: want the socket kept after abort", n, err, r.ghosts.open.Load())
	}
	peer.Close()
	waitFor(t, "the third ghost closed", 5*time.Second, func() bool { return r.ghosts.open.Load() == 0 })

	rc, _ = ghost() // closed before the ghost: not ghosted
	rc.state.Store(connClosed)
	if rc.ghost() {
		t.Fatal("ghost took a closed connection")
	}
}

// A socket the keeper cannot take is closed at once and counted unkept,
// not silent: the SDK's Close does not wait (6b3c2 review M1).
func TestGhostUnkeptClosesAtOnce(t *testing.T) {
	r := &Runner{ghosts: newGhosts(1)}
	defer r.ghosts.stop()
	a, b := net.Pipe() // not TCP: no epoll for it
	defer b.Close()
	rc := (&sdkClient{r: r}).track(a, 1)
	rc.gw = 0
	if !rc.ghost() {
		t.Fatal("ghost refused an open connection")
	}
	begin := time.Now()
	rc.Close()
	if took := time.Since(begin); took > 100*time.Millisecond {
		t.Fatalf("Close took %v, want at once", took)
	}
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("peer read: want the pipe closed")
	}
	if u, s, o := r.ghosts.unkept.Load(), r.ghosts.silent[0].Load(), r.ghosts.open.Load(); u != 1 || s != 0 || o != 0 {
		t.Fatalf("unkept %d, silent %d, open %d: want 1, 0, 0", u, s, o)
	}
}

// Close racing ghost: a connection ghost() took is always handed to the
// keeper, never closed by Close (6b3c2 review M3).
func TestGhostCloseRace(t *testing.T) {
	r := &Runner{ghosts: newGhosts(1)}
	defer r.ghosts.stop()
	sc := &sdkClient{r: r}
	for i := range 200000 {
		a, b := net.Pipe()
		rc := sc.track(a, uint64(i))
		before := r.ghosts.unkept.Load()
		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(2)
		var ghosted bool
		go func() { defer done.Done(); start.Wait(); ghosted = rc.ghost() }()
		go func() { defer done.Done(); start.Wait(); rc.Close() }()
		start.Done()
		done.Wait()
		if kept := r.ghosts.unkept.Load() != before; kept != ghosted {
			t.Fatalf("round %d: ghost() %v, keeper got it %v", i, ghosted, kept)
		}
		b.Close()
	}
}

// GET /clients gives each client's gateway, -1 when offline (review O4).
func TestClientsGivesEachGateway(t *testing.T) {
	env, _ := startStack(t, 2)
	r, srv := sdkRun(t, env, 6, sdk.DefaultWindows().Storm)
	resp, err := http.Get(srv.URL + "/clients")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var c struct {
		UIDPrefix string   `json:"uid_prefix"`
		Gateways  []string `json:"gateways"`
		GW        []int    `json:"gw"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.UIDPrefix != "rst" || len(c.Gateways) != 2 || len(c.GW) != 6 {
		t.Fatalf("/clients: %+v", c)
	}
	n := map[string]int{}
	for i, g := range c.GW {
		if g < 0 || g > 1 {
			t.Fatalf("client %d on gateway %d, want 0 or 1", i, g)
		}
		n[c.Gateways[g]]++
	}
	if by := r.Stats().OnlineByGateway; n[env.GatewayIDs[0]] != by[env.GatewayIDs[0]] || n[env.GatewayIDs[1]] != by[env.GatewayIDs[1]] {
		t.Fatalf("/clients per gateway %v, online by gateway %v", n, by)
	}
}

// The keeper's epoll set failing is loadgen's own error, 500, not a
// refusal like sdk mode's 409 (6b3c2 review M9).
func TestGhostKeeperFailureIs500(t *testing.T) {
	g := newGhosts(1)
	g.once.Do(func() { g.err = errors.New("epoll_create1: too many open files") })
	r := &Runner{cfg: Config{Mode: "sdk"}, ghosts: g}
	if _, _, err := r.Ghost("ghost"); !errors.Is(err, ErrGhostKeeper) {
		t.Fatalf("Ghost: %v, want ErrGhostKeeper", err)
	}
	srv := httptest.NewServer(r.Handler())
	defer srv.Close()
	if code, _ := post(t, srv.URL+"/ghost?mark=ghost"); code != http.StatusInternalServerError {
		t.Fatalf("/ghost: %d, want 500", code)
	}
	if !strings.Contains(ErrNotSDK.Error(), "/ghost") {
		t.Fatalf("ErrNotSDK %q does not name /ghost", ErrNotSDK)
	}
}
