package loadgen

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/Tallinzp/goim/internal/wire"
)

// #106: the workers prepare nothing until the ramp ends, so the ramp's
// logins (each makes its own first message) do not pay for a second one on
// the same CPUs, which <phase>_loadgen_headroom measures.
func TestPrecomputeWaitsForTheRampsEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		key, err := wire.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		r := &Runner{env: Env{NoiseKeyID: 1, NoisePublic: key.Public}, ramped: make(chan struct{})}
		p := newPrecomputer(r, 1, 1)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go p.run(ctx)
		p.enqueue(&sdkClient{r: r})
		synctest.Wait()
		if s := p.stats(); s.Ready != 0 || s.Pending != 1 {
			t.Fatalf("in the ramp: %+v, want 1 pending, none ready", s)
		}
		close(r.ramped)
		synctest.Wait()
		if s := p.stats(); s.Ready != 1 || s.Pending != 0 {
			t.Fatalf("after the ramp: %+v, want 1 ready", s)
		}
	})
}
