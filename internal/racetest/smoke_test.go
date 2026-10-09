package racetest

import (
	"testing"
)

// The stack comes up: every gateway is Ready with its own epoch, renews its
// lease, and logs a client in; Logic writes the entry with the gateway's
// epoch.
func TestStackComesUp(t *testing.T) {
	s := New(t, Options{})
	for i, g := range s.GW {
		if !s.Redis.LeaseExists(g.ID, g.Epoch) {
			t.Fatalf("%s: no lease key", g.ID)
		}
		uid := "smoke" + g.ID[3:]
		s.Online(i, uid, "d1", 1)
		e, ok := s.Redis.Entry(uid)
		if !ok || e.Gateway != g.ID || e.Epoch != g.Epoch {
			t.Fatalf("%s: entry %+v %v", uid, e, ok)
		}
	}
}
