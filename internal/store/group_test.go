package store

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
)

// silentRedis accepts connections, refuses HELLO, then reads commands and
// never replies: a stopped (SIGSTOPped) master. It counts the EVALSHA commands it receives.
func silentRedis(t *testing.T) (addr string, evals *atomic.Int64) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	evals = &atomic.Int64{}
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { c.Close() })
			go func() {
				r := bufio.NewScanner(c)
				for r.Scan() {
					switch strings.ToLower(r.Text()) {
					case "hello": // the handshake: refused, so the client goes on with RESP2
						_, _ = c.Write([]byte("-ERR unknown command\r\n"))
					case "evalsha":
						evals.Add(1)
					}
				}
			}()
		}
	}()
	return lis.Addr().String(), evals
}

// #145 (6c2b final review M3): a group to a master that never answers fails
// in GroupTimeout, and its script is sent once: go-redis has no time to send
// it again.
func TestRunEachStalledGroupFailsInGroupTimeoutAndSendsOnce(t *testing.T) {
	addr, evals := silentRedis(t)
	const timeout = 200 * time.Millisecond
	rdb := redis.NewClient(&redis.Options{Addr: addr, ReadTimeout: timeout, WriteTimeout: timeout,
		DisableIdentity: true})
	t.Cleanup(func() { rdb.Close() })
	cfg := DefaultConfig()
	cfg.GroupTimeout = timeout
	s := New(rdb, cfg)
	start := time.Now()
	res := s.Login(ctx, []LoginReq{{UID: testredis.UID(), Field: field, Entry: entry(1, "d", 1, time.Now().UnixMilli())}})
	if res[0].Err == nil {
		t.Fatalf("a silent master gave %+v, want an error", res[0])
	}
	if d := time.Since(start); d > 2*timeout {
		t.Fatalf("the group failed after %v, want about %v", d, timeout)
	}
	time.Sleep(timeout) // a late resend would arrive now
	if n := evals.Load(); n != 1 {
		t.Fatalf("EVALSHA sent %d times, want once", n)
	}
}

// #145: GroupWorkers bounds the groups in flight to one master; another
// master's group does not wait for them.
func TestRunEachGroupWorkersBoundOneMasterOnly(t *testing.T) {
	slow := testredis.UID()
	s, release := gatedStore(t, slow)
	s.cfg.GroupWorkers = 1
	s.groupOf = func(_ context.Context, key string) string {
		if key == sessKey(slow) || strings.HasPrefix(key, "sess:{same") {
			return "stalled"
		}
		return key
	}
	now := nowMs(t, s)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // holds the stalled master's only slot until release
		defer wg.Done()
		s.Login(ctx, []LoginReq{{UID: slow, Field: field, Entry: entry(1, "d", 1, now)}})
	}()
	time.Sleep(100 * time.Millisecond)
	queued := make(chan LoginResult, 1)
	go func() {
		queued <- s.Login(ctx, []LoginReq{{UID: "same" + testredis.UID(), Field: field, Entry: entry(2, "d", 1, now)}})[0]
	}()
	start := time.Now()
	if r := s.Login(ctx, []LoginReq{{UID: testredis.UID(), Field: field, Entry: entry(3, "d", 1, now)}})[0]; r.Err != nil || r.Code != LoginWritten {
		t.Fatalf("other master: %+v", r)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the other master's group waited %v", d)
	}
	select {
	case r := <-queued:
		t.Fatalf("the stalled master's second group ran while its slot was held: %+v", r)
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	wg.Wait()
	if r := <-queued; r.Err != nil || r.Code != LoginWritten {
		t.Fatalf("queued group after release: %+v", r)
	}
}
