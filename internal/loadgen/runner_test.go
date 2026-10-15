package loadgen

import (
	"strings"
	"testing"
	"time"
)

// The default source count holds a gateway's share after another gateway
// is killed: /kill moves its clients onto N−1 gateways (6a2b minor 1).
func TestNewSizesSourcesForOneGatewayLess(t *testing.T) {
	for _, tc := range []struct{ gateways, clients, want int }{
		{1, 5_000, 1},
		{1, 5_001, 2},
		{2, 40_000, 8},  // 20,000 per gateway, 40,000 on the survivor
		{4, 100_000, 7}, // 25,000 per gateway, 33,334 on each of 3
		{2, 200_000, 40},
		{1, 600_000, 120},   // run 17: 600k online in 121s; 30 sources topped out near 505k
		{1, 2_000_000, 253}, // capped at the 253 source addresses
	} {
		env := Env{}
		for i := range tc.gateways {
			env.GatewayAddrs = append(env.GatewayAddrs, "127.0.0.1:1")
			env.GatewayIDs = append(env.GatewayIDs, "gw-"+itoa(i))
		}
		r, err := New(Config{Mode: "sdk", Clients: tc.clients, Rate: 1, UIDPrefix: "x"}, env)
		if err != nil {
			t.Fatal(err)
		}
		if r.cfg.Sources != tc.want {
			t.Errorf("%d gateways, %d clients: %d sources, want %d", tc.gateways, tc.clients, r.cfg.Sources, tc.want)
		}
	}
}

// With gateways down at the start (b_stress's victim ramps alone, #93), the
// default sources hold every client on the gateways that are up: 600k on
// one gateway needs 120, not the 60 sized for two survivors.
func TestNewSizesSourcesForTheGatewaysUp(t *testing.T) {
	env := Env{GatewayAddrs: []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}, GatewayIDs: []string{"gw-0", "gw-1", "gw-2"}}
	for _, tc := range []struct {
		down []string
		want int
	}{
		{nil, 60},                       // 300,000 on each of 2 after a kill
		{[]string{"gw-1", "gw-2"}, 120}, // 600,000 on gw-0 alone
		{[]string{"gw-2"}, 60},          // 300,000 on each of 2
		{[]string{"gw-2", "gw-2"}, 60},  // a repeated id is one gateway
	} {
		r, err := New(Config{Mode: "sdk", Clients: 600_000, Rate: 1, UIDPrefix: "x", Down: tc.down}, env)
		if err != nil {
			t.Fatal(err)
		}
		if r.cfg.Sources != tc.want {
			t.Errorf("down %v: %d sources, want %d", tc.down, r.cfg.Sources, tc.want)
		}
		for i, id := range env.GatewayIDs {
			want := false
			for _, d := range tc.down {
				want = want || d == id
			}
			if r.bal.Down(i) != want {
				t.Errorf("down %v: %s down %v", tc.down, id, r.bal.Down(i))
			}
		}
	}
	for _, down := range [][]string{{"gw-9"}, {"gw-0", "gw-1", "gw-2"}} {
		_, err := New(Config{Mode: "sdk", Clients: 1, Rate: 1, UIDPrefix: "x", Down: down}, env)
		if err == nil || !strings.Contains(err.Error(), "down") {
			t.Errorf("down %v: err %v", down, err)
		}
	}
}

// #106: preparing tokens and handshakes is for SDK clients.
func TestNewRefusesPrecomputeInLightMode(t *testing.T) {
	env := Env{GatewayIDs: []string{"gw-0"}, GatewayAddrs: []string{"127.0.0.1:1"}}
	_, err := New(Config{Mode: "light", Clients: 1, Rate: 1, UIDPrefix: "x", PingEvery: time.Second,
		Timeout: time.Second, Precompute: true}, env)
	if err == nil || !strings.Contains(err.Error(), "precompute") {
		t.Fatalf("err %v, want a precompute error", err)
	}
}
