package sdk

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

// EndKind is what ended a connection attempt or an established connection
// (Event.End).
type EndKind int

const (
	EndLost         EndKind = iota + 1 // an established connection closed or reset (RST/FIN)
	EndPongTimeout                     // no frame within the Pong timeout of a Ping
	EndReconnect                       // Reconnect{jitter_ms} from a draining gateway
	EndKicked                          // Kick{reason}
	EndPreHandshake                    // closed before the handshake reply
	EndBusy                            // Busy{retry_after}
	EndRejected                        // Error{code}
	EndNoReply                         // closed after the handshake, before LoginResp or Error
	EndFailed                          // dial, token, persistence or handshake failure
)

var endNames = map[EndKind]string{EndLost: "lost", EndPongTimeout: "pong-timeout", EndReconnect: "reconnect",
	EndKicked: "kicked", EndPreHandshake: "pre-handshake", EndBusy: "busy", EndRejected: "rejected",
	EndNoReply: "no-reply", EndFailed: "failed"}

func (k EndKind) String() string { return endNames[k] }

// end is one ending, with what the server sent.
type end struct {
	kind       EndKind
	retryAfter time.Duration       // EndBusy; 0: none sent
	jitter     time.Duration       // EndReconnect
	kick       clientpb.KickReason // EndKicked
	code       clientpb.ErrorCode  // EndRejected
	storedSeq  uint64              // EndRejected STALE
	err        error               // the cause, for events (a failure, an undecodable reply)
}

// WaitKind says which rule chose a wait.
type WaitKind int

const (
	WaitStorm        WaitKind = iota + 1 // after losing an established connection (0–20s)
	WaitLocal                            // after a local trigger (0–1s)
	WaitDrain                            // after Reconnect: [0, jitter_ms]
	WaitPreHandshake                     // after a pre-handshake close (0.5–1.5s)
	WaitRetryable                        // after another retryable reject (1–3s, or retry_after)
	WaitBackoff                          // a retry limit was reached, or BAD_REQUEST
	WaitFailure                          // backoff after a failure that never reached a gateway's logic
)

var waitNames = map[WaitKind]string{WaitStorm: "storm", WaitLocal: "local", WaitDrain: "drain",
	WaitPreHandshake: "pre-handshake", WaitRetryable: "retryable", WaitBackoff: "backoff", WaitFailure: "failure"}

func (k WaitKind) String() string { return waitNames[k] }

// Wait is the pause before the next attempt and the reconnect_reason that
// attempt carries.
type Wait struct {
	Kind   WaitKind
	D      time.Duration
	Reason clientpb.ReconnectReason
}

// Backoff is exponential backoff: step n waits uniformly in [d/2, d] with
// d = min(Max, Base × 2^(n−1)). Steps restart at a successful login.
type Backoff struct{ Base, Max time.Duration }

// DefaultBackoff starts at 2.5–5s and reaches the 5m cap at step 7.
func DefaultBackoff() Backoff { return Backoff{Base: 5 * time.Second, Max: 5 * time.Minute} }

// Retry limits (spec: Retryable rejects): quick retries before backoff.
const (
	preHandshakeLimit = 10
	retryableLimit    = 5
)

// maxDrainJitter caps Reconnect{jitter_ms}: the largest storm window (the
// clamp table). A planned move never waits longer than a storm could.
var maxDrainJitter = clamps.Storm.HiMax

// codeRule is what a Login error code leads to (choice #49).
type codeRule int

const (
	ruleBackoff   codeRule = iota // BAD_REQUEST, or a code this SDK doesn't know: backoff and an anomaly
	ruleRetryable                 // the second counter
	ruleRevoked                   // drop the token, show the login screen
)

func ruleOf(code clientpb.ErrorCode) codeRule {
	switch code {
	case clientpb.ErrorCode_ERROR_CODE_STALE, clientpb.ErrorCode_ERROR_CODE_SUPERSEDED,
		clientpb.ErrorCode_ERROR_CODE_EXPIRED, clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN,
		clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID:
		return ruleRetryable
	case clientpb.ErrorCode_ERROR_CODE_REVOKED:
		return ruleRevoked
	}
	return ruleBackoff
}

// policy decides every wait (spec: Client SDK rules). It does no I/O; the
// client feeds it each ending.
type policy struct {
	w       Windows
	backoff Backoff
	r       *rand.Rand

	pre, other int                      // pre-handshake closes and other retryable rejects in a row
	steps      int                      // backoff steps since the last login
	reason     clientpb.ReconnectReason // the reason of the last wait; a backoff keeps it
}

func newPolicy(w Windows, b Backoff, r *rand.Rand) *policy {
	return &policy{w: w, backoff: b, r: r, reason: clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT}
}

// loggedIn resets both counters and the backoff (spec: both reset on a
// successful login).
func (p *policy) loggedIn() { p.pre, p.other, p.steps = 0, 0, 0 }

// first is the reason of an attempt that follows no wait: a first connect,
// or one the user asked for.
func (p *policy) first() clientpb.ReconnectReason {
	p.reason = clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT
	return p.reason
}

// after returns the wait that follows e, or ok false: stay idle until the
// user or the app acts (a Kick, REVOKED).
func (p *policy) after(e end) (w Wait, ok bool) {
	switch e.kind {
	case EndLost:
		return p.wait(WaitStorm, p.w.Storm.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN), true
	case EndPongTimeout:
		return p.wait(WaitStorm, p.w.Storm.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_PONG_TIMEOUT), true
	case EndReconnect:
		return p.wait(WaitDrain, Window{0, min(e.jitter, maxDrainJitter)}.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_DRAIN), true
	case EndKicked:
		return Wait{}, false
	case EndPreHandshake:
		p.other, p.pre = 0, p.pre+1
		if p.pre > preHandshakeLimit {
			return p.next(WaitBackoff, 0, clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY), true
		}
		return p.wait(WaitPreHandshake, p.w.PreHandshake.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY), true
	case EndRejected:
		switch ruleOf(e.code) {
		case ruleRevoked:
			return Wait{}, false
		case ruleRetryable:
			return p.retryable(0), true
		default: // never a fast loop
			return p.next(WaitBackoff, 0, p.reason), true
		}
	case EndBusy:
		return p.retryable(min(e.retryAfter, p.backoff.Max)), true
	case EndNoReply:
		return p.retryable(0), true
	default: // EndFailed
		return p.next(WaitFailure, 0, p.reason), true
	}
}

// anomaly says why e is worth logging, or nil: BAD_REQUEST and unknown
// codes (an undecodable Error reads as code 0), REPLACED and unknown Kick
// reasons, and a retry_after or jitter_ms after cuts to its cap.
func (p *policy) anomaly(e end) error {
	var why error
	switch {
	case e.kind == EndRejected && ruleOf(e.code) == ruleBackoff:
		why = fmt.Errorf("sdk: Login error %v", e.code)
	case e.kind == EndKicked && e.kick != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE &&
		e.kick != clientpb.KickReason_KICK_REASON_REVOKED:
		why = fmt.Errorf("sdk: Kick %v", e.kick)
	case e.kind == EndBusy && e.retryAfter > p.backoff.Max:
		why = fmt.Errorf("sdk: retry_after %v capped at %v", e.retryAfter, p.backoff.Max)
	case e.kind == EndReconnect && e.jitter > maxDrainJitter:
		why = fmt.Errorf("sdk: jitter_ms %v capped at %v", e.jitter, maxDrainJitter)
	default:
		return nil
	}
	if e.err != nil {
		return errors.Join(why, e.err)
	}
	return why
}

// retryable is the second counter: Busy, EXPIRED, KEY_UNKNOWN and a close
// before any reply share it. A retry_after from the server is obeyed, also
// as a floor once backoff takes over.
func (p *policy) retryable(retryAfter time.Duration) Wait {
	p.pre, p.other = 0, p.other+1
	busy := clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY
	if p.other > retryableLimit {
		return p.next(WaitBackoff, retryAfter, busy)
	}
	if retryAfter > 0 {
		return p.wait(WaitRetryable, retryAfter, busy)
	}
	return p.wait(WaitRetryable, p.w.Retryable.Sample(p.r), busy)
}

// next is one more backoff step, at least floor.
func (p *policy) next(kind WaitKind, floor time.Duration, reason clientpb.ReconnectReason) Wait {
	p.steps++
	d := p.backoff.Max
	if p.steps < 32 {
		d = min(d, p.backoff.Base<<(p.steps-1))
	}
	return p.wait(kind, max(floor, Window{d / 2, d}.Sample(p.r)), reason)
}

func (p *policy) wait(kind WaitKind, d time.Duration, reason clientpb.ReconnectReason) Wait {
	p.reason = reason
	return Wait{Kind: kind, D: d, Reason: reason}
}

// local is a network change or the app returning to the foreground during
// cur. It cuts only the storm wait (spec: Precedence), and a failure backoff,
// which no server asked for (roadmap choice #51); ok false leaves cur alone.
func (p *policy) local(cur Wait) (Wait, bool) {
	if cur.Kind != WaitStorm && cur.Kind != WaitFailure {
		return cur, false
	}
	return p.localWait(), true
}

// localWait is the 0–1s wait of a local trigger.
func (p *policy) localWait() Wait {
	return p.wait(WaitLocal, p.w.Local.Sample(p.r), clientpb.ReconnectReason_RECONNECT_REASON_LOCAL_TRIGGER)
}
