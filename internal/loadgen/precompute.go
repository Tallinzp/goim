package loadgen

import (
	"context"
	"sync/atomic"

	"github.com/Tallinzp/goim/api/clientpb"
	"github.com/Tallinzp/goim/internal/wire"
)

// prepared is what a phone's own CPU has made when its connection drops:
// the first handshake message (#106). The token is not in it: the client
// keeps that itself (sdkClient.tok). One serves one attempt.
type prepared struct {
	in    *wire.Initiator
	frame []byte
}

// precomputer prepares each logged-in SDK client's next attempt in the
// background, from the ramp's end until the first mark (#106), so a storm's
// first attempts do not spend loadgen's CPU on what a phone has ready. It
// waits for the ramp's end because each ramp login makes its own first
// message: preparing beside them would double the ramp's handshake work.
type precomputer struct {
	r       *Runner
	q       chan *sdkClient // sized to Clients: each client is queued at most once
	workers int
	stopped atomic.Bool
	pending atomic.Int64 // queued or being prepared
	ready   atomic.Int64 // clients holding a prepared attempt
}

// PrecomputeStats is /stats' precompute (#106).
type PrecomputeStats struct {
	Pending int64 `json:"pending"` // logged-in clients queued or being prepared
	Ready   int64 `json:"ready"`   // clients holding a prepared handshake
	Stopped bool  `json:"stopped"` // a mark was taken: no more are prepared
}

func newPrecomputer(r *Runner, clients, workers int) *precomputer {
	return &precomputer{r: r, q: make(chan *sdkClient, clients), workers: max(1, workers)}
}

// enqueue queues a client that just logged in; it runs on the client's run
// loop and never blocks.
func (p *precomputer) enqueue(sc *sdkClient) {
	if p.stopped.Load() || sc.pre.Load() != nil || !sc.queued.CompareAndSwap(false, true) {
		return
	}
	p.pending.Add(1)
	p.q <- sc
}

// run prepares queued clients from the ramp's end until ctx ends.
func (p *precomputer) run(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-p.r.ramped:
	}
	for {
		select {
		case <-ctx.Done():
			return
		case sc := <-p.q:
			sc.queued.Store(false)
			if !p.stopped.Load() && sc.pre.Load() == nil {
				if pr, err := p.r.prepare(); err == nil && sc.pre.CompareAndSwap(nil, pr) {
					p.ready.Add(1)
				}
			}
			p.pending.Add(-1)
		}
	}
}

// take hands client sc's prepared attempt to its Initiate, or nil.
func (p *precomputer) take(sc *sdkClient) *prepared {
	pr := sc.pre.Swap(nil)
	if pr != nil {
		p.ready.Add(-1)
	}
	return pr
}

func (p *precomputer) stats() *PrecomputeStats {
	return &PrecomputeStats{Pending: p.pending.Load(), Ready: p.ready.Load(), Stopped: p.stopped.Load()}
}

// prepare makes a client's next first handshake message.
func (r *Runner) prepare() (*prepared, error) {
	in, frame, err := wire.Initiate(r.env.NoiseKeyID, r.env.NoisePublic, uint16(clientpb.Cmd_CMD_HANDSHAKE))
	if err != nil {
		return nil, err
	}
	return &prepared{in: in, frame: frame}, nil
}
