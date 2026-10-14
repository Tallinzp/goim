package loadgen

import (
	"testing"

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
