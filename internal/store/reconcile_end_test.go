package store

import (
	"testing"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestDeleteUntracked(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 9}
	const asOf = 10_000
	mk := func(conn uint64, login int64) session.Entry {
		return session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch, ConnID: conn, DeviceID: "d",
			ConnSeq: 1, LoginMs: login, TokIatMs: 1}
	}
	put := func(uid, f string, e session.Entry) {
		if err := rdb.HSet(ctx, sessKey(uid), f, e.Encode()).Err(); err != nil {
			t.Fatal(err)
		}
		rdb.SAdd(ctx, ref.connsKey(), uid)
	}

	ghost := testredis.UID()   // conn 1: not tracked, logged in before as_of → deleted
	live := testredis.UID()    // conn 2: tracked → kept
	late := testredis.UID()    // conn 3: not tracked, login_ms >= as_of → kept
	otherGw := testredis.UID() // field of another gateway → kept, uid leaves gwconns
	mixed := testredis.UID()   // one ghost slot, one tracked slot of this epoch
	put(ghost, field, mk(1, asOf-1))
	put(live, field, mk(2, asOf-1))
	put(late, field, mk(3, asOf))
	other := mk(4, 1)
	other.Gateway = "gw-other"
	put(otherGw, field, other)
	put(mixed, "im:mobile", mk(5, 1))
	put(mixed, "im:pc", mk(6, 1))

	tracked := map[uint64]struct{}{2: {}, 6: {}}
	n, err := s.DeleteUntracked(ctx, ref, asOf, []string{ghost, live, late, otherGw, mixed}, tracked)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("deleted %d fields, want 2 (ghost, mixed:mobile)", n)
	}
	for uid, want := range map[string]bool{ghost: false, live: true, late: true, otherGw: true} {
		if got := stored(t, rdb, uid) != ""; got != want {
			t.Errorf("uid %s: field present = %v, want %v", uid, got, want)
		}
	}
	if rdb.HExists(ctx, sessKey(mixed), "im:mobile").Val() || !rdb.HExists(ctx, sessKey(mixed), "im:pc").Val() {
		t.Error("mixed: wrong field deleted")
	}
	members := rdb.SMembers(ctx, ref.connsKey()).Val()
	want := map[string]bool{live: true, late: true, mixed: true}
	if len(members) != len(want) {
		t.Fatalf("gwconns = %v, want %v", members, want)
	}
	for _, m := range members {
		if !want[m] {
			t.Fatalf("gwconns kept %s", m)
		}
	}
}

func TestScanConnsPagesThroughTheSet(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 3}
	for i := 0; i < 50; i++ {
		rdb.SAdd(ctx, ref.connsKey(), testredis.UID())
	}
	seen := map[string]bool{}
	var cursor uint64
	for {
		uids, next, err := s.ScanConns(ctx, ref, cursor, 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, u := range uids {
			seen[u] = true
		}
		if cursor = next; cursor == 0 {
			break
		}
	}
	if len(seen) != 50 {
		t.Fatalf("scanned %d uids, want 50", len(seen))
	}
}

// An undecodable value carrying this epoch's prefix (an older format, or
// written by hand) must not stop the delete phase for the other uids. It
// can't name a tracked connection or a login in flight, so it is a ghost:
// compare-and-deleted like any other (PI-3).
func TestDeleteUntrackedDeletesUndecodableEntries(t *testing.T) {
	s, rdb := newStore(t)
	ref := EpochRef{Gateway: testredis.Gateway(), Epoch: 1<<63 + 9}
	bad := testredis.UID()
	ghost := testredis.UID()
	rdb.HSet(ctx, sessKey(bad), field, ref.prefix()+"not-a-conn-id|d|1|1|1")
	rdb.HSet(ctx, sessKey(ghost), field, session.Entry{Gateway: ref.Gateway, Epoch: ref.Epoch,
		ConnID: 1, DeviceID: "d", ConnSeq: 1, LoginMs: 1, TokIatMs: 1}.Encode())
	rdb.SAdd(ctx, ref.connsKey(), bad, ghost)

	n, err := s.DeleteUntracked(ctx, ref, 10_000, []string{bad, ghost}, nil)
	if err != nil || n != 2 {
		t.Fatalf("DeleteUntracked = %d, %v; want 2 deleted and no error", n, err)
	}
	if rdb.Exists(ctx, sessKey(bad), sessKey(ghost)).Val() != 0 {
		t.Fatal("both fields must be gone")
	}
	if m := rdb.SMembers(ctx, ref.connsKey()).Val(); len(m) != 0 {
		t.Fatalf("gwconns = %v, want empty", m)
	}
}
