package logic

import (
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/store"
)

// Verifier checks an access token (an *auth.Manager in production).
type Verifier interface {
	Verify(token string) (auth.Claims, error)
}

// KickSender queues a Kick (a *Kicker in production). It must not block.
type KickSender interface {
	Kick(ref store.EpochRef, connID uint64, reason pb.KickReason)
}

// Service implements the Logic and Revocation gRPC services.
type Service struct {
	pb.UnimplementedLogicServer
	pb.UnimplementedRevocationServer

	cfg    Config
	st     Store
	keys   Verifier
	leases *LeaseCache
	kick   KickSender
	m      *Metrics
	pool   pool // Login, Logout and Reconcile; the lease RPCs have their own (R1-O2)
	now    func() time.Time
}

// NewService wires the handlers; workers bounds concurrent Login, Logout and
// Reconcile calls.
func NewService(cfg Config, st Store, keys Verifier, leases *LeaseCache, kick KickSender, m *Metrics, workers int) *Service {
	return &Service{cfg: cfg, st: st, keys: keys, leases: leases, kick: kick, m: m,
		pool: newPool(workers), now: time.Now}
}
