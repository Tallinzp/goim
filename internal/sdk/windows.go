// Package sdk is the Go client SDK (spec: Client SDK rules): one client
// instance that holds at most one connection, persists device_id and
// conn_seq before every attempt, and reconnects by the spec's rules — the
// 0–20s storm wait, the 0–1s local trigger, Reconnect{jitter_ms}, two
// counters for retryable rejects, exponential backoff, and the server-set
// windows from LoginResp, clamped and persisted. It is the reference the
// mobile SDKs port, and the client the race suite and loadgen drive.
package sdk

import (
	"math/rand/v2"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// Window is a uniform random wait [Lo, Hi].
type Window struct{ Lo, Hi time.Duration }

// Sample draws a wait uniformly from [Lo, Hi].
func (w Window) Sample(r *rand.Rand) time.Duration {
	if w.Hi <= w.Lo {
		return w.Lo
	}
	return w.Lo + time.Duration(r.Int64N(int64(w.Hi-w.Lo)+1))
}

// Windows are the four waits LoginResp can set (R5S-O6).
type Windows struct {
	Storm        Window // after losing an established connection (0–20s)
	Local        Window // after a network change or app foreground (0–1s)
	PreHandshake Window // after a pre-handshake close (0.5–1.5s)
	Retryable    Window // after another retryable reject (1–3s)
}

// DefaultWindows are the built-in defaults a fresh install uses.
func DefaultWindows() Windows {
	return Windows{
		Storm:        Window{0, 20 * time.Second},
		Local:        Window{0, time.Second},
		PreHandshake: Window{500 * time.Millisecond, 1500 * time.Millisecond},
		Retryable:    Window{time.Second, 3 * time.Second},
	}
}

// clampRange is the safe range built into the app for one window (roadmap
// choice #13): Hi is clamped to [HiMin, HiMax] and Lo to [LoMin, Hi); a Lo
// that would reach Hi becomes LoMin. ZeroLo pins Lo at 0.
type clampRange struct {
	LoMin, HiMin, HiMax time.Duration
	ZeroLo              bool
}

var clamps = struct{ Storm, Local, PreHandshake, Retryable clampRange }{
	// The storm upper end has a 10s floor: 200k reconnects over less than
	// 10s exceed the 20k/s headroom.
	Storm:        clampRange{0, 10 * time.Second, 120 * time.Second, true},
	Local:        clampRange{0, time.Second, 10 * time.Second, true},
	PreHandshake: clampRange{250 * time.Millisecond, time.Second, 5 * time.Second, false},
	Retryable:    clampRange{500 * time.Millisecond, 2 * time.Second, 10 * time.Second, false},
}

func (c clampRange) apply(w Window) Window {
	w.Hi = min(max(w.Hi, c.HiMin), c.HiMax)
	if c.ZeroLo {
		w.Lo = 0
		return w
	}
	w.Lo = max(w.Lo, c.LoMin)
	if w.Lo >= w.Hi {
		w.Lo = c.LoMin
	}
	return w
}

// defaultHeartbeat is used when LoginResp carries none.
const defaultHeartbeat = 150 * time.Second

// HeartbeatRange is the Ping interval the app accepts from LoginResp. The
// gateway refuses to start outside the same range (gateway.Config
// HeartbeatMin/HeartbeatMax), so the clamp only catches a broken server; a
// test timing profile lowers both ends together.
type HeartbeatRange struct{ Min, Max time.Duration }

// DefaultHeartbeatRange is 30s–300s: 30s still allows the 60s interval the
// spec measures, and 300s stays below a 350s LB idle timeout.
func DefaultHeartbeatRange() HeartbeatRange {
	return HeartbeatRange{Min: 30 * time.Second, Max: 300 * time.Second}
}

// Of is the Ping interval for resp: absent means 150s, and any value is
// clamped to the range; clamped reports a value the server sent out of range.
func (r HeartbeatRange) Of(resp *clientpb.LoginResp) (hb time.Duration, clamped bool) {
	sent := time.Duration(resp.GetHeartbeatS()) * time.Second
	if sent == 0 {
		return min(max(defaultHeartbeat, r.Min), r.Max), false
	}
	hb = min(max(sent, r.Min), r.Max)
	return hb, hb != sent
}

// ServerWindows are the windows the server has set, clamped; a zero Window
// was never set and leaves the built-in default. They are what the SDK
// persists (spec: Server-set windows).
type ServerWindows Windows

// Merge applies a LoginResp's windows: each present one is clamped and
// replaces the stored value; an absent one keeps it (R5S-O6).
func (s ServerWindows) Merge(resp *clientpb.LoginResp) ServerWindows {
	set := func(dst *Window, src *clientpb.Window, c clampRange) {
		if src != nil {
			*dst = c.apply(Window{time.Duration(src.GetLoMs()) * time.Millisecond, time.Duration(src.GetHiMs()) * time.Millisecond})
		}
	}
	set(&s.Storm, resp.GetStormWait(), clamps.Storm)
	set(&s.Local, resp.GetLocalTrigger(), clamps.Local)
	set(&s.PreHandshake, resp.GetPreHandshakeRetry(), clamps.PreHandshake)
	set(&s.Retryable, resp.GetRetryable(), clamps.Retryable)
	return s
}

// Clamp re-clamps every set window, so a damaged store can't produce a
// zero-jitter storm or an unbounded wait.
func (s ServerWindows) Clamp() ServerWindows {
	one := func(w *Window, c clampRange) {
		if *w != (Window{}) {
			*w = c.apply(*w)
		}
	}
	one(&s.Storm, clamps.Storm)
	one(&s.Local, clamps.Local)
	one(&s.PreHandshake, clamps.PreHandshake)
	one(&s.Retryable, clamps.Retryable)
	return s
}

// Over returns the windows in effect: each server-set window over base.
func (s ServerWindows) Over(base Windows) Windows {
	pick := func(set, def Window) Window {
		if set == (Window{}) {
			return def
		}
		return set
	}
	return Windows{pick(s.Storm, base.Storm), pick(s.Local, base.Local),
		pick(s.PreHandshake, base.PreHandshake), pick(s.Retryable, base.Retryable)}
}
