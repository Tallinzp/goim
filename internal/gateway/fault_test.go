package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/fault"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func liveWithFaults(t *testing.T) (*live, *fault.Set) {
	t.Helper()
	f := fault.New()
	l := startLive(t, nil, func(g *Gateway, _ *fakeLogic) { g.faults = f })
	return l, f
}

func TestKickFaultDropsKicksUntilCleared(t *testing.T) {
	l, f := liveWithFaults(t)
	kc := l.kickClient(t)
	k := l.loginLive(t, 1, "u1")
	f.Put(fault.Rule{Op: FaultKick, Key: strconv.FormatUint(k.c.id, 10), Fail: true})
	_, err := kc.Kick(context.Background(), &pb.KickRequest{Gw: &pb.GatewayRef{Gateway: "gw-1", Epoch: 0xE1},
		ConnId: k.c.id, Reason: pb.KickReason_KICK_REASON_REPLACED})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("dropped Kick: %v, want UNAVAILABLE (Logic retries those)", err)
	}
	time.Sleep(50 * time.Millisecond)
	if k.isClosed() {
		t.Fatal("a dropped Kick closed the connection")
	}
	f.Clear(FaultKick)
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_REPLACED)
	waitFor(t, "closed by the Kick after the fault cleared", k.isClosed)
}

func TestLogicFaultBlocksOneMethod(t *testing.T) {
	l, f := liveWithFaults(t)
	waitFor(t, "first lease", func() bool { return len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls })) > 0 })
	f.Put(fault.Rule{Op: FaultLogic, Key: "GatewayLease", Fail: true})
	time.Sleep(150 * time.Millisecond) // a renewal already in flight may still land
	n := len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls }))
	l.loginLive(t, 1, "u1") // Login still reaches Logic
	time.Sleep(300 * time.Millisecond)
	if m := len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls })); m != n {
		t.Fatalf("lease calls went from %d to %d while blocked", n, m)
	}
	f.Clear(FaultLogic)
	waitFor(t, "renewals resume", func() bool { return len(get(l.logic, func() []*pb.LeaseRequest { return l.logic.leaseCalls })) > n })
}

func TestFaultInfoAndReconcileAction(t *testing.T) {
	l, _ := liveWithFaults(t)
	l.loginLive(t, 1, "u1")
	_, admin := l.g.Addrs()
	resp, err := http.Get("http://" + admin + "/fault/info")
	if err != nil {
		t.Fatal(err)
	}
	var info faultInfo
	json.NewDecoder(resp.Body).Decode(&info)
	resp.Body.Close()
	if info.Gateway != "gw-1" || info.Epoch != strconv.FormatUint(0xE1, 10) || info.Kick == "" {
		t.Fatalf("info %+v", info)
	}
	resp, err = http.Post("http://"+admin+"/fault/action/reconcile", "", strings.NewReader(""))
	if err != nil || resp.StatusCode != http.StatusNoContent {
		t.Fatalf("action: %v %v", resp, err)
	}
	resp.Body.Close()
	waitFor(t, "the triggered pass's ReconcileEnd", func() bool {
		return len(get(l.logic, func() []*pb.ReconcileEndRequest { return l.logic.ends })) == 1
	})
	if e := get(l.logic, func() []*pb.ReconcileEndRequest { return l.logic.ends }); e[0].GetPace() != pb.Pace_PACE_TRIGGERED {
		t.Fatalf("pass pace %v, want triggered", e[0].GetPace())
	}
	if c := get(l.logic, func() []*pb.ReconcileChunk { return l.logic.chunks }); len(c) != 1 || len(c[0].GetEntries()) != 1 {
		t.Fatalf("chunks %v, want one chunk with u1", c)
	}
}

// Without Faults there is no /fault/ endpoint and nothing is broken.
func TestNoFaultEndpointInProduction(t *testing.T) {
	l := newLive(t, nil)
	_, admin := l.g.Addrs()
	resp, err := http.Get("http://" + admin + "/fault/info")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("/fault/info = %d, want 404", resp.StatusCode)
	}
}
