package loadgen

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/sdk"
)

// An SDK client's backoff and failure waits count as backoff; the quick
// retries before them do not (P6a2bI-2).
func TestSDKClientCountsBackoffWaits(t *testing.T) {
	r, err := New(Config{Mode: "sdk", Clients: 2, Rate: 1, UIDPrefix: "x"}, Env{GatewayAddrs: []string{"127.0.0.1:1"}, GatewayIDs: []string{"gw-0"}})
	if err != nil {
		t.Fatal(err)
	}
	sc := &sdkClient{r: r, i: 1}
	for _, k := range []sdk.WaitKind{sdk.WaitPreHandshake, sdk.WaitRetryable, sdk.WaitStorm, sdk.WaitBackoff, sdk.WaitFailure} {
		sc.event(sdk.Event{Kind: sdk.EventWaiting, Wait: sdk.Wait{Kind: k}, End: sdk.EndFailed})
	}
	c := r.Stats().Counts
	if c.BackoffClients != 1 || c.BackoffWaits["backoff"] != 1 || c.BackoffWaits["failure"] != 1 || len(c.BackoffWaits) != 2 {
		t.Fatalf("backoff: clients %d, waits %v", c.BackoffClients, c.BackoffWaits)
	}
}

// #107: an attempt with no wait before it (the first Connect) records no
// attempt lag; one after a wait records one, and only once.
func TestAttemptLagOnlyAfterAWait(t *testing.T) {
	r := &Runner{rec: NewRecorder(1, []string{"gw-0"})}
	sc := &sdkClient{r: r}
	sc.event(sdk.Event{Kind: sdk.EventConnecting})
	if n := r.rec.Counts().StormSinceMark.AttemptLag.N; n != 0 {
		t.Fatalf("first attempt: %d lags, want 0", n)
	}
	sc.event(sdk.Event{Kind: sdk.EventWaiting, Wait: sdk.Wait{Kind: sdk.WaitRetryable, D: time.Millisecond}})
	sc.event(sdk.Event{Kind: sdk.EventConnecting})
	sc.event(sdk.Event{Kind: sdk.EventConnecting})
	if n := r.rec.Counts().StormSinceMark.AttemptLag.N; n != 1 {
		t.Fatalf("after one wait: %d lags, want 1", n)
	}
}
