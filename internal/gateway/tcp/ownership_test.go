package tcp

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/gateway"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// goid returns the calling goroutine's id, read from its stack header.
func goid() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	id, _ := strconv.ParseUint(string(b[:bytes.IndexByte(b, ' ')]), 10, 64)
	return id
}

// The gnet loop-ownership test (spec: Verification, race 13): for every
// connection, OnOpen, OnTraffic, OnClose, the Execute callbacks (Kick, wheel
// ticks) and the wheel's expiry callbacks run on one goroutine, and each
// worker_id belongs to exactly one loop goroutine. Run under -race, with
// connections opening, handshaking, logging in, being kicked, timing out and
// sending garbage at once.
func TestLoopOwnershipUnderRace(t *testing.T) {
	type event struct {
		site string
		g    uint64
	}
	var mu sync.Mutex
	seen := map[*conn][]event{}
	s := newServer(t, func(c *gateway.Config, tc *Config) {
		c.NumLoops = 4
		c.HandshakeTimeout = 3 * time.Second // idle sockets close from the wheel
		tc.HandshakeConcurrency = 4          // every loop may handshake at once
	}, func(tr *Transport) {
		tr.onLoop = func(c *conn, site string) {
			g := goid()
			mu.Lock()
			seen[c] = append(seen[c], event{site, g})
			mu.Unlock()
		}
	})

	kickAddr, _ := s.g.Addrs()
	cc, err := grpc.NewClient(kickAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	kick := pb.NewGatewayClient(cc)

	const each = 20
	var wg sync.WaitGroup
	for i := range each {
		wg.Go(func() { // log in, ping, hang up
			c := s.online(fmt.Sprintf("full-%d", i))
			for id := uint32(2); id < 5; id++ {
				c.send(clientpb.Cmd_CMD_PING, id, nil)
				c.recv(nil)
			}
			c.nc.Close()
		})
		wg.Go(func() { // log in, then a Kick through Execute
			uid := fmt.Sprintf("kick-%d", i)
			c := s.online(uid)
			id, _ := s.logic.connID(uid)
			if _, err := kick.Kick(context.Background(), &pb.KickRequest{Gw: s.ref, ConnId: id,
				Reason: pb.KickReason_KICK_REASON_REPLACED}); err != nil {
				t.Error(err)
			}
			c.recv(nil)
			c.closedByServer()
		})
		wg.Go(func() { s.dial() })                                                             // idle: the HANDSHAKE timeout
		wg.Go(func() { s.dial().nc.Write(handshakeFrame(t, s, func(f []byte) { f[0] = 9 })) }) // garbage
	}
	wg.Wait()
	waitForLong(t, "every connection closed", 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		closed := 0
		for _, evs := range seen {
			for _, e := range evs {
				if e.site == "close" {
					closed++
				}
			}
		}
		return closed == 4*each
	})

	mu.Lock()
	defer mu.Unlock()
	loopOf := map[uint64]uint64{} // worker_id → goroutine
	owner := map[uint64]uint64{}  // goroutine → worker_id
	sites := map[string]int{}
	for c, evs := range seen {
		w := c.c.ID() >> 48
		for _, e := range evs {
			sites[e.site]++
			if e.g != evs[0].g {
				t.Fatalf("conn %x: %s on goroutine %d, opened on %d", c.c.ID(), e.site, e.g, evs[0].g)
			}
		}
		if g, ok := loopOf[w]; ok && g != evs[0].g {
			t.Fatalf("worker_id %d on goroutines %d and %d", w, g, evs[0].g)
		}
		if o, ok := owner[evs[0].g]; ok && o != w {
			t.Fatalf("goroutine %d owns worker_ids %d and %d", evs[0].g, o, w)
		}
		loopOf[w], owner[evs[0].g] = evs[0].g, w
	}
	if len(loopOf) != 4 {
		t.Fatalf("connections spread over %d loops, want 4 (round robin)", len(loopOf))
	}
	for _, site := range []string{"open", "traffic", "send", "closereq", "close"} {
		if sites[site] == 0 {
			t.Fatalf("no %q event: the test does not exercise it", site)
		}
	}
	waitForLong(t, "nothing tracked", 10*time.Second, func() bool { return testutil.ToFloat64(s.m.Tracked) == 0 })
}

func waitForLong(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// gnet is pinned to exactly v2.10.0 (R4G-O7). Upgrading it means re-running
// this package's loop-ownership test and the core's loopState tests, then
// changing this pin.
func TestGnetIsPinned(t *testing.T) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		t.Fatal("no build info")
	}
	for _, d := range bi.Deps {
		if d.Path == "github.com/panjf2000/gnet/v2" {
			if d.Version != "v2.10.0" || d.Replace != nil {
				t.Fatalf("gnet %s (replace %v); the design was checked against v2.10.0", d.Version, d.Replace)
			}
			return
		}
	}
	t.Fatal("gnet not in the build")
}
