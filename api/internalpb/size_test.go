package internalpb

import (
	"math/rand/v2"
	"testing"

	"google.golang.org/protobuf/proto"
)

// Spec (Reconcile, pass end): tracked_conn_ids at the 250k cap is 8 bytes
// each, ≈2MB, under gRPC's 4MB default message limit.
func TestReconcileEndFitsDefaultLimitAtCap(t *testing.T) {
	ids := make([]uint64, 250_000)
	for i := range ids {
		ids[i] = rand.Uint64()
	}
	b, err := proto.Marshal(&ReconcileEndRequest{Gw: &GatewayRef{Gateway: "gw-17", Epoch: rand.Uint64()},
		AsOfMs: 1759900000123, TrackedConnIds: ids, Pace: Pace_PACE_TRIGGERED})
	if err != nil {
		t.Fatal(err)
	}
	if len(b) < 8*len(ids) || len(b) >= 4<<20 {
		t.Fatalf("ReconcileEnd at the cap is %d bytes; want packed fixed64 (≥ %d) under 4MB", len(b), 8*len(ids))
	}
}

// P6b1I-1: a ReconcileEnd of MaxTrackedConnIDs fits MaxMsgBytes, and the
// 600k benchmark's list (4.8MB) is over gRPC's 4MB default, which is why
// Logic raises it.
func TestReconcileEndFitsMaxMsgBytes(t *testing.T) {
	for _, c := range []struct {
		n   int
		max int
	}{{MaxTrackedConnIDs, MaxMsgBytes}, {600_000, 1 << 62}} {
		b, err := proto.Marshal(&ReconcileEndRequest{Gw: &GatewayRef{Gateway: "gw-17", Epoch: rand.Uint64()},
			AsOfMs: 1759900000123, TrackedConnIds: make([]uint64, c.n), Pace: Pace_PACE_TRIGGERED})
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > c.max {
			t.Errorf("%d conn_ids: %d bytes, over %d", c.n, len(b), c.max)
		}
		if c.n == 600_000 && len(b) <= 4<<20 {
			t.Errorf("600k conn_ids: %d bytes, expected over gRPC's 4MB default", len(b))
		}
	}
}
