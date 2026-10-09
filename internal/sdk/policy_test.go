package sdk

import (
	"fmt"
	"testing"
	"time"

	"github.com/Tallinzp/goim/api/clientpb"
)

func newTestPolicy() *policy { return newPolicy(DefaultWindows(), DefaultBackoff(), seeded()) }

var (
	preClose  = end{kind: EndPreHandshake}
	expired   = end{kind: EndRejected, code: clientpb.ErrorCode_ERROR_CODE_EXPIRED}
	keyUnkn   = end{kind: EndRejected, code: clientpb.ErrorCode_ERROR_CODE_KEY_UNKNOWN}
	noReply   = end{kind: EndNoReply}
	dialError = end{kind: EndFailed}
	busyRetry = clientpb.ReconnectReason_RECONNECT_REASON_BUSY_RETRY
)

func busy(d time.Duration) end { return end{kind: EndBusy, retryAfter: d} }

// within fails t unless w has the kind, a duration in [lo, hi] and the reason.
func within(t *testing.T, step string, w Wait, kind WaitKind, lo, hi time.Duration, reason clientpb.ReconnectReason) {
	t.Helper()
	if w.Kind != kind || w.D < lo || w.D > hi || w.Reason != reason {
		t.Fatalf("%s: got %v %v %v, want %v in [%v, %v] %v", step, w.Kind, w.D, w.Reason, kind, lo, hi, reason)
	}
}

func after(t *testing.T, p *policy, e end) Wait {
	t.Helper()
	w, ok := p.after(e)
	if !ok {
		t.Fatalf("after %+v: idle, want a wait", e)
	}
	return w
}

// Pre-handshake closes retry after 0.5–1.5s, and fall back to exponential
// backoff after 10 in a row (R5S-O4).
func TestPreHandshakeClosesRetryQuicklyThenBackOff(t *testing.T) {
	p := newTestPolicy()
	for i := range 10 {
		within(t, fmt.Sprint("close ", i+1), after(t, p, preClose), WaitPreHandshake, ms(500), ms(1500), busyRetry)
	}
	within(t, "close 11", after(t, p, preClose), WaitBackoff, ms(2500), 5*s, busyRetry)
	within(t, "close 12", after(t, p, preClose), WaitBackoff, 5*s, 10*s, busyRetry)
}

// Busy, EXPIRED, KEY_UNKNOWN and a close before any reply share one count of
// 5 in a row (R3-O5, R4H-O2, R4H-O7, R4H-O10).
func TestOtherRetryableRejectsShareOneCounterOfFive(t *testing.T) {
	p := newTestPolicy()
	within(t, "Busy 2s", after(t, p, busy(2*s)), WaitRetryable, 2*s, 2*s, busyRetry) // retry_after obeyed
	within(t, "EXPIRED", after(t, p, expired), WaitRetryable, s, 3*s, busyRetry)
	within(t, "KEY_UNKNOWN", after(t, p, keyUnkn), WaitRetryable, s, 3*s, busyRetry)
	within(t, "close before LoginResp", after(t, p, noReply), WaitRetryable, s, 3*s, busyRetry)
	within(t, "Busy without retry_after", after(t, p, busy(0)), WaitRetryable, s, 3*s, busyRetry)
	within(t, "6th: EXPIRED", after(t, p, expired), WaitBackoff, ms(2500), 5*s, busyRetry)
	// Backoff never undercuts a server's retry_after.
	within(t, "7th: Busy 30s", after(t, p, busy(30*s)), WaitBackoff, 30*s, 30*s, busyRetry)
}

// The other rejects choice #49 puts on the second counter.
func TestStaleSupersededAndTokenInvalidAreRetryable(t *testing.T) {
	for _, code := range []clientpb.ErrorCode{clientpb.ErrorCode_ERROR_CODE_STALE,
		clientpb.ErrorCode_ERROR_CODE_SUPERSEDED, clientpb.ErrorCode_ERROR_CODE_TOKEN_INVALID} {
		p := newTestPolicy()
		for range 5 {
			within(t, code.String(), after(t, p, end{kind: EndRejected, code: code}), WaitRetryable, s, 3*s, busyRetry)
		}
		within(t, code.String()+" 6th", after(t, p, end{kind: EndRejected, code: code}), WaitBackoff, ms(2500), 5*s, busyRetry)
	}
}

// An outcome of one kind resets the other kind's counter.
func TestAnOutcomeOfOneKindResetsTheOtherCounter(t *testing.T) {
	p := newTestPolicy()
	for range 4 {
		after(t, p, expired)
	}
	for range 9 {
		after(t, p, preClose)
	}
	// The pre-handshake close reset the 4 rejects: 5 more are still quick.
	for range 5 {
		within(t, "reject after closes", after(t, p, busy(0)), WaitRetryable, s, 3*s, busyRetry)
	}
	// The rejects reset the 9 closes: 10 more are still quick.
	for range 10 {
		within(t, "close after rejects", after(t, p, preClose), WaitPreHandshake, ms(500), ms(1500), busyRetry)
	}
	within(t, "11th close", after(t, p, preClose), WaitBackoff, 0, 5*time.Minute, busyRetry)
}

// Both counters, and the backoff, reset on a successful login.
func TestLoginResetsBothCountersAndTheBackoff(t *testing.T) {
	p := newTestPolicy()
	for range 12 {
		after(t, p, preClose)
	}
	for range 4 {
		after(t, p, expired)
	}
	p.loggedIn()
	for range 10 {
		within(t, "close after login", after(t, p, preClose), WaitPreHandshake, ms(500), ms(1500), busyRetry)
	}
	within(t, "first backoff after login", after(t, p, preClose), WaitBackoff, ms(2500), 5*s, busyRetry)
	p.loggedIn()
	for range 5 {
		within(t, "reject after login", after(t, p, expired), WaitRetryable, s, 3*s, busyRetry)
	}
}

func TestBackoffDoublesToFiveMinutes(t *testing.T) {
	p := newTestPolicy()
	want := []time.Duration{5 * s, 10 * s, 20 * s, 40 * s, 80 * s, 160 * s, 300 * s, 300 * s}
	for i, d := range want {
		within(t, fmt.Sprint("step ", i+1), after(t, p, dialError), WaitFailure, d/2, d, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
	}
	for range 100 { // no overflow however long it fails
		after(t, p, dialError)
	}
	within(t, "step 108", after(t, p, dialError), WaitFailure, 150*s, 300*s, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
}

// After losing an established connection: the 0–20s storm wait with the
// loss as the reason, then exponential backoff that keeps the reason.
func TestLostConnectionWaitsInTheStormWindowThenBacksOff(t *testing.T) {
	p := newTestPolicy()
	within(t, "RST/FIN", after(t, p, end{kind: EndLost}), WaitStorm, 0, 20*s, clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN)
	within(t, "dial fails", after(t, p, dialError), WaitFailure, ms(2500), 5*s, clientpb.ReconnectReason_RECONNECT_REASON_RST_FIN)
	within(t, "Pong timeout", after(t, p, end{kind: EndPongTimeout}), WaitStorm, 0, 20*s, clientpb.ReconnectReason_RECONNECT_REASON_PONG_TIMEOUT)
}

// After Reconnect the attempt falls within [0, jitter_ms] and carries the
// drain reason (R4H-O3).
func TestReconnectWaitsWithinItsJitterWithTheDrainReason(t *testing.T) {
	p := newTestPolicy()
	within(t, "Reconnect 5s", after(t, p, end{kind: EndReconnect, jitter: 5 * s}), WaitDrain, 0, 5*s, clientpb.ReconnectReason_RECONNECT_REASON_DRAIN)
	within(t, "Reconnect 0", after(t, p, end{kind: EndReconnect}), WaitDrain, 0, 0, clientpb.ReconnectReason_RECONNECT_REASON_DRAIN)
}

// No Kick reconnects on its own, and REVOKED stops (spec: Kick reasons,
// races 9 and 10); BAD_REQUEST never loops fast.
func TestKicksAndRevokedAreIdleAndBadRequestBacksOff(t *testing.T) {
	p := newTestPolicy()
	for _, r := range []clientpb.KickReason{clientpb.KickReason_KICK_REASON_REPLACED,
		clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE, clientpb.KickReason_KICK_REASON_REVOKED} {
		if w, ok := p.after(end{kind: EndKicked, kick: r}); ok {
			t.Fatalf("Kick %v: wait %+v, want idle", r, w)
		}
	}
	if w, ok := p.after(end{kind: EndRejected, code: clientpb.ErrorCode_ERROR_CODE_REVOKED}); ok {
		t.Fatalf("REVOKED: wait %+v, want idle", w)
	}
	within(t, "BAD_REQUEST", after(t, p, end{kind: EndRejected, code: clientpb.ErrorCode_ERROR_CODE_BAD_REQUEST}),
		WaitBackoff, ms(2500), 5*s, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
	within(t, "unknown code", after(t, p, end{kind: EndRejected, code: 99}),
		WaitBackoff, 5*s, 10*s, clientpb.ReconnectReason_RECONNECT_REASON_FIRST_CONNECT)
}

// The policy decides which endings are anomalies (choice #49, review items
// 4 and 5).
func TestAnomalies(t *testing.T) {
	p := newTestPolicy()
	for _, tc := range []struct {
		name string
		e    end
		want bool
	}{
		{"BAD_REQUEST", end{kind: EndRejected, code: clientpb.ErrorCode_ERROR_CODE_BAD_REQUEST}, true},
		{"code 0", end{kind: EndRejected}, true},
		{"code 99", end{kind: EndRejected, code: 99}, true},
		{"EXPIRED", expired, false},
		{"REVOKED", end{kind: EndRejected, code: clientpb.ErrorCode_ERROR_CODE_REVOKED}, false},
		{"Kick REPLACED", end{kind: EndKicked, kick: clientpb.KickReason_KICK_REASON_REPLACED}, true},
		{"Kick reason 9", end{kind: EndKicked, kick: 9}, true},
		{"Kick KICKED_BY_OTHER_DEVICE", end{kind: EndKicked, kick: clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE}, false},
		{"Kick REVOKED", end{kind: EndKicked, kick: clientpb.KickReason_KICK_REASON_REVOKED}, false},
		{"Busy 3s", busy(3 * s), false},
		{"Busy 1h", busy(time.Hour), true},
		{"Reconnect 120s", end{kind: EndReconnect, jitter: 120 * s}, false},
		{"Reconnect 1h", end{kind: EndReconnect, jitter: time.Hour}, true},
		{"lost", end{kind: EndLost}, false},
	} {
		if got := p.anomaly(tc.e) != nil; got != tc.want {
			t.Errorf("%s: anomaly %v, want %v", tc.name, got, tc.want)
		}
	}
}

// retry_after is capped at the backoff cap and jitter_ms at the largest
// storm window, so a broken server can't park clients (review item 5).
func TestServerWaitsAreCapped(t *testing.T) {
	p := newTestPolicy()
	within(t, "Busy 1h", after(t, p, busy(time.Hour)), WaitRetryable, 5*time.Minute, 5*time.Minute, busyRetry)
	for range 100 {
		within(t, "Reconnect 1h", after(t, p, end{kind: EndReconnect, jitter: time.Hour}), WaitDrain, 0, 120*s,
			clientpb.ReconnectReason_RECONNECT_REASON_DRAIN)
	}
}

// A local trigger cuts only the storm wait, and a failure backoff (choice
// #51); never Busy, Reconnect, the quick retries or backoff after a limit
// (spec: Precedence).
func TestLocalTriggerCutsOnlyTheStormWaitAndAFailureBackoff(t *testing.T) {
	p := newTestPolicy()
	for _, kind := range []WaitKind{WaitStorm, WaitFailure} {
		w, ok := p.local(Wait{Kind: kind, D: 15 * s})
		if !ok {
			t.Fatalf("%v: not cut", kind)
		}
		within(t, kind.String(), w, WaitLocal, 0, s, clientpb.ReconnectReason_RECONNECT_REASON_LOCAL_TRIGGER)
	}
	for _, kind := range []WaitKind{WaitDrain, WaitPreHandshake, WaitRetryable, WaitBackoff, WaitLocal} {
		cur := Wait{Kind: kind, D: 15 * s, Reason: busyRetry}
		if w, ok := p.local(cur); ok || w != cur {
			t.Fatalf("%v: cut to %+v", kind, w)
		}
	}
}

// Each jittered wait, drawn through the policy ~10k times, is uniform
// (R4G-O12).
func TestJitteredWaitsAreUniform(t *testing.T) {
	p := newTestPolicy()
	draw := func(e end) func() time.Duration {
		return func() time.Duration { p.loggedIn(); return after(t, p, e).D }
	}
	uniform(t, "storm 0–20s", 0, 20*s, draw(end{kind: EndLost}))
	uniform(t, "retryable 1–3s", s, 3*s, draw(expired))
	uniform(t, "pre-handshake 0.5–1.5s", ms(500), ms(1500), draw(preClose))
	uniform(t, "Reconnect [0, 5s]", 0, 5*s, draw(end{kind: EndReconnect, jitter: 5 * s}))
	uniform(t, "local 0–1s", 0, s, func() time.Duration { return p.localWait().D })
}
