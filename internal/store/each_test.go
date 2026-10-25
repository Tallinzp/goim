package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
)

// gateHook holds every pipeline that touches slowKey until release is closed
// (or 2s pass, so a broken implementation fails instead of hanging).
type gateHook struct {
	slowKey string
	release chan struct{}
}

func (h gateHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (h gateHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h gateHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, c := range cmds {
			for _, a := range c.Args() {
				if a == h.slowKey {
					select {
					case <-h.release:
					case <-time.After(2 * time.Second):
					}
					return next(ctx, cmds)
				}
			}
		}
		return next(ctx, cmds)
	}
}

// gatedStore puts every key in its own group (one miniredis node stands in for
// a cluster) and holds the pipeline of slowUID's sess key until release closes.
func gatedStore(t *testing.T, slowUID string) (*Store, chan struct{}) {
	t.Helper()
	s, rdb := newStore(t)
	s.groupOf = func(_ context.Context, key string) string { return key }
	release := make(chan struct{})
	rdb.AddHook(gateHook{slowKey: sessKey(slowUID), release: release})
	return s, release
}

func TestLoginEachSlowGroupDoesNotDelayOthers(t *testing.T) {
	slow := testredis.UID()
	s, release := gatedStore(t, slow)
	now := nowMs(t, s)
	fast := []string{testredis.UID(), testredis.UID(), testredis.UID()}

	reqs := []LoginReq{{UID: slow, Field: field, Entry: entry(1, "d", 1, now)}}
	for i, uid := range fast {
		reqs = append(reqs, LoginReq{UID: uid, Field: field, Entry: entry(uint64(i+2), "d", 1, now)})
	}

	var mu sync.Mutex
	var order []int
	fastLeft := len(fast)
	released := false
	s.LoginEach(ctx, reqs, func(i int, r LoginResult) {
		mu.Lock()
		defer mu.Unlock()
		if r.Err != nil || r.Code != LoginWritten {
			t.Errorf("entry %d: %+v", i, r)
		}
		order = append(order, i)
		if i != 0 {
			fastLeft--
			if fastLeft == 0 {
				released = true
				close(release) // the slow group may finish only now
			}
		}
	})
	if !released {
		t.Fatalf("fast entries did not all arrive while the slow group was held; order %v", order)
	}
	if len(order) != len(reqs) || order[len(order)-1] != 0 {
		t.Fatalf("want each entry once and the slow one last, got %v", order)
	}
}

func TestLogoutEachSlowGroupDoesNotDelayOthers(t *testing.T) {
	s, rdb := newStore(t)
	s.groupOf = func(_ context.Context, key string) string { return key }
	now := nowMs(t, s)
	slow := testredis.UID()
	fast := []string{testredis.UID(), testredis.UID()}
	var reqs []LogoutReq
	for i, uid := range append([]string{slow}, fast...) {
		e := entry(uint64(i+1), "d", 1, now)
		login1(t, s, uid, e) // before the gate is installed
		reqs = append(reqs, LogoutReq{UID: uid, Field: field, Gateway: e.Gateway, Epoch: e.Epoch, ConnID: e.ConnID})
	}
	release := make(chan struct{})
	rdb.AddHook(gateHook{slowKey: sessKey(slow), release: release})

	var mu sync.Mutex
	var order []int
	fastLeft := len(fast)
	s.LogoutEach(ctx, reqs, func(i int, r LogoutResult) {
		mu.Lock()
		defer mu.Unlock()
		if r.Err != nil || !r.Deleted {
			t.Errorf("entry %d: %+v", i, r)
		}
		order = append(order, i)
		if i != 0 {
			if fastLeft--; fastLeft == 0 {
				close(release)
			}
		}
	})
	if len(order) != len(reqs) || order[len(order)-1] != 0 {
		t.Fatalf("want each entry once and the slow one last, got %v", order)
	}
}

func TestEachReportsInvalidAtOnceAndKeepsOrderInSlices(t *testing.T) {
	s, _ := newStore(t)
	now := nowMs(t, s)
	good := testredis.UID()
	reqs := []LoginReq{
		{UID: "bad|uid", Field: field, Entry: entry(1, "d", 1, now)},
		{UID: good, Field: field, Entry: entry(2, "d", 1, now)},
	}
	seen := map[int]int{}
	var mu sync.Mutex
	s.LoginEach(ctx, reqs, func(i int, r LoginResult) {
		mu.Lock()
		defer mu.Unlock()
		seen[i]++
	})
	if seen[0] != 1 || seen[1] != 1 {
		t.Fatalf("each request must be reported exactly once, got %v", seen)
	}
	out := s.Login(ctx, []LoginReq{reqs[0], {UID: testredis.UID(), Field: field, Entry: entry(3, "d", 1, now)}})
	if out[0].Err == nil || out[1].Err != nil || out[1].Code != LoginWritten {
		t.Fatalf("Login must keep request order: %+v", out)
	}
}

func TestMasterAddrOnSingleNodeIsOneGroup(t *testing.T) {
	if testredis.Cluster() {
		t.Skip("a Cluster run: TestMasterAddrOnClusterIsTheOwner")
	}
	s, _ := newStore(t)
	if a, b := s.masterAddr(ctx, sessKey("u1")), s.masterAddr(ctx, sessKey("u2")); a != "" || b != "" {
		t.Fatalf("a single node must form the one \"\" group: %q, %q", a, b)
	}
}

// Tier 2 Cluster (P5bI-4): runEach groups by the master that owns the key.
func TestMasterAddrOnClusterIsTheOwner(t *testing.T) {
	if !testredis.Cluster() {
		t.Skip("not a Cluster run: TestMasterAddrOnSingleNodeIsOneGroup")
	}
	s, rdb := newStore(t)
	for _, uid := range []string{"a", "b", "c"} { // one on each master
		key := sessKey(uid)
		if got, want := s.masterAddr(ctx, key), testredis.MasterOf(t, rdb, key).Options().Addr; got != want {
			t.Fatalf("masterAddr(%s) = %q, want its owner %q", key, got, want)
		}
	}
}
