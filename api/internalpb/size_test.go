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
