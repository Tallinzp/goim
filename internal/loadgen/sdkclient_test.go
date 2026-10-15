package loadgen

import (
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/sdk"
)

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
