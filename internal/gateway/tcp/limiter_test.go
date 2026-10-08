package tcp

import (
	"testing"
	"time"
)

func TestLimiterSemaphore(t *testing.T) {
	l := newLimiter(2, 1000, 1000, time.Now)
	if l.acquire() != "" || l.acquire() != "" {
		t.Fatal("two slots")
	}
	if why := l.acquire(); why != noSlot {
		t.Fatalf("a third handshake with two slots: %q, want %q", why, noSlot)
	}
	l.release()
	if l.acquire() != "" {
		t.Fatal("a released slot is not reusable")
	}
}

func TestLimiterBucket(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(10, 10, 2, func() time.Time { return now }) // 10/s, burst 2
	for i := range 2 {
		if l.acquire() != "" {
			t.Fatalf("burst handshake %d refused", i)
		}
		l.release()
	}
	if why := l.acquire(); why != noToken {
		t.Fatalf("bucket empty: %q, want %q", why, noToken)
	}
	now = now.Add(100 * time.Millisecond) // one token at 10/s
	if l.acquire() != "" {
		t.Fatal("refill refused")
	}
	l.release()
	if l.acquire() == "" {
		t.Fatal("one token, two handshakes")
	}
}

// A full semaphore must not spend tokens: the bucket shapes handshakes that
// actually run.
func TestFullSemaphoreSpendsNoToken(t *testing.T) {
	now := time.Unix(1000, 0)
	l := newLimiter(1, 1, 2, func() time.Time { return now })
	if l.acquire() != "" {
		t.Fatal("first")
	}
	for range 5 {
		if why := l.acquire(); why != noSlot {
			t.Fatalf("semaphore full: %q", why)
		}
	}
	l.release()
	if l.acquire() != "" {
		t.Fatal("the second token was spent by refused handshakes")
	}
}
