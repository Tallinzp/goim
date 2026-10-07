package store

import (
	"strings"
	"testing"
)

// hashTag mirrors Redis Cluster's rule: the slot is computed from the text
// between the first '{' and the next '}', if that text is non-empty.
func hashTag(key string) string {
	i := strings.IndexByte(key, '{')
	if i < 0 {
		return key
	}
	j := strings.IndexByte(key[i+1:], '}')
	if j <= 0 {
		return key
	}
	return key[i+1 : i+1+j]
}

func TestKeysShareSlotsWhereScriptsNeedIt(t *testing.T) {
	if hashTag(sessKey("10086")) != hashTag(revKey("10086")) {
		t.Fatal("sess:{uid} and rev:{uid} must share a slot: the login and Reconcile Lua read both")
	}
	r := EpochRef{Gateway: "gw-17", Epoch: 1<<63 + 1}
	if hashTag(r.leaseKey()) != hashTag(r.connsKey()) || hashTag(r.leaseKey()) != hashTag(r.sweepKey()) {
		t.Fatal("per-epoch keys must share a slot")
	}
	if got, want := r.leaseKey(), "gwlease:gw-17:{9223372036854775809}"; got != want {
		t.Fatalf("leaseKey = %q, want %q", got, want)
	}
	if got, want := r.member(), "gw-17:9223372036854775809"; got != want {
		t.Fatalf("member = %q, want %q", got, want)
	}
}
