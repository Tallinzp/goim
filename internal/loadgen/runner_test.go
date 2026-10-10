package loadgen

import "testing"

// The default source count holds a gateway's share after another gateway
// is killed: /kill moves its clients onto N−1 gateways (6a2b minor 1).
func TestNewSizesSourcesForOneGatewayLess(t *testing.T) {
	for _, tc := range []struct{ gateways, clients, want int }{
		{1, 20_000, 1},
		{1, 20_001, 2},
		{2, 40_000, 2},  // 20,000 per gateway, 40,000 on the survivor
		{4, 100_000, 2}, // 25,000 per gateway, 33,334 on each of 3
		{2, 200_000, 10},
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
