package gateway

import (
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func logouts(l *live, result string) int {
	return int(testutil.ToFloat64(l.g.m.Logouts.WithLabelValues(result)))
}

func TestConnectionStaysTrackedUntilItsLogoutIsAcknowledged(t *testing.T) {
	l := newLive(t, nil)
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.logout = func(b *pb.LogoutBatch) (*pb.LogoutReply, error) {
			<-release
			return allDone(b), nil
		}
	})
	k := l.loginLive(t, 0, "u1")
	k.clientClose()
	waitFor(t, "Logout sent", func() bool { return get(l.logic, func() int { return len(l.logic.logouts) }) == 1 })
	if !l.tracked(k) {
		t.Fatal("untracked while its Logout is in flight")
	}
	close(release)
	waitFor(t, "untracked", func() bool { return !l.tracked(k) })
	e := get(l.logic, func() *pb.LogoutEntry { return l.logic.logouts[0].GetEntries()[0] })
	if e.GetUid() != "u1" || e.GetConnId() != k.c.id || e.GetAppid() != "app" {
		t.Fatalf("Logout entry %v", e)
	}
}

func TestLogoutRetriesEntriesNotDone(t *testing.T) {
	l := newLive(t, nil)
	calls := 0
	l.logic.set(func() {
		l.logic.logout = func(b *pb.LogoutBatch) (*pb.LogoutReply, error) {
			calls++
			if calls == 1 {
				return &pb.LogoutReply{Done: make([]bool, len(b.GetEntries()))}, nil // outcome unknown
			}
			return allDone(b), nil
		}
	})
	k := l.loginLive(t, 0, "u1")
	k.clientClose()
	waitFor(t, "untracked after the retry", func() bool { return !l.tracked(k) })
	if n := get(l.logic, func() int { return len(l.logic.logouts) }); n != 2 || logouts(l, "retried") != 1 {
		t.Fatalf("%d Logout RPCs and %d retries, want 2 and 1", n, logouts(l, "retried"))
	}
}

func TestLogoutIsDroppedWhenItsRetryBudgetRunsOut(t *testing.T) {
	l := newLive(t, func(c *Config) { c.RetryFor = 150 * time.Millisecond })
	l.logic.set(func() {
		l.logic.logout = func(*pb.LogoutBatch) (*pb.LogoutReply, error) { return nil, errors.New("Logic down") }
	})
	k := l.loginLive(t, 0, "u1")
	k.clientClose()
	waitFor(t, "dropped and untracked", func() bool { return !l.tracked(k) })
	if logouts(l, "dropped") != 1 || logouts(l, "done") != 0 {
		t.Fatal("want one dropped Logout: the next Reconcile pass repairs it")
	}
}
