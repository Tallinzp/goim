package tcp

import (
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// limiter is the Noise handshake limit (spec: Admission control): a
// concurrency limit on handshakes computing at once plus a gateway-wide token
// bucket on handshakes/s. It is global, never per IP. Handshakes run on the
// event loops, which can't wait, so a handshake that finds either exhausted
// is not started: the connection is closed as handshake_rate.
type limiter struct {
	free   atomic.Int64 // free semaphore slots
	bucket *rate.Limiter
	now    func() time.Time
}

func newLimiter(concurrency int, perSec float64, burst int, now func() time.Time) *limiter {
	l := &limiter{bucket: rate.NewLimiter(rate.Limit(perSec), burst), now: now}
	l.free.Store(int64(concurrency))
	return l
}

// Why acquire refused a handshake: the handshakes_total result, so the
// flood and storm tests can tell which limit fired.
const (
	noSlot  = "no_slot"  // HandshakeConcurrency handshakes already computing
	noToken = "no_token" // the handshakes/s bucket is empty
)

// acquire takes a slot and a token, or neither, and returns "" or why it
// refused. The slot is taken first, so a full semaphore never spends a token.
func (l *limiter) acquire() string {
	if l.free.Add(-1) < 0 {
		l.free.Add(1)
		return noSlot
	}
	if !l.bucket.AllowN(l.now(), 1) {
		l.free.Add(1)
		return noToken
	}
	return ""
}

// release returns the slot once the handshake computation is done.
func (l *limiter) release() { l.free.Add(1) }
