package store

import (
	"strconv"

	"github.com/Tallinzp/goim/internal/session"
)

// Redis keys (spec: Redis schema). Per-user keys are hash-tagged {uid};
// per-epoch keys are hash-tagged {epoch}, so one gateway epoch's lease and
// gwconns set live on one shard.

const epochsKey = "gwepochs"

func sessKey(uid string) string { return "sess:{" + uid + "}" }
func revKey(uid string) string  { return "rev:{" + uid + "}" }

// EpochRef names one gateway instance: gateway_id (pod name) plus its boot epoch.
type EpochRef struct {
	Gateway string
	Epoch   uint64
}

func (r EpochRef) validate() error { return session.ValidGateway(r.Gateway) }

func (r EpochRef) suffix() string { return r.Gateway + ":{" + strconv.FormatUint(r.Epoch, 10) + "}" }

func (r EpochRef) leaseKey() string { return "gwlease:" + r.suffix() }
func (r EpochRef) connsKey() string { return "gwconns:" + r.suffix() }
func (r EpochRef) sweepKey() string { return "sweep:" + r.suffix() }

// member is the gwepochs ZSET member "gw:epoch".
func (r EpochRef) member() string { return r.Gateway + ":" + strconv.FormatUint(r.Epoch, 10) }

// prefix is "gw|epoch|", the start of every sess value this epoch wrote.
func (r EpochRef) prefix() string { return session.EpochPrefix(r.Gateway, r.Epoch) }
