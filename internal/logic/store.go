package logic

import (
	"context"

	"github.com/Tallinzp/goim/internal/store"
)

// Store is the part of *store.Store that Logic uses. Handlers take the
// interface so a test can wrap the real store and delay or fail one call.
type Store interface {
	LoginEach(ctx context.Context, reqs []store.LoginReq, done func(i int, r store.LoginResult))
	AddConns(ctx context.Context, ref store.EpochRef, uids []string) error
	LogoutEach(ctx context.Context, reqs []store.LogoutReq, done func(i int, r store.LogoutResult))

	ReadSlots(ctx context.Context, refs []store.SlotRef) []store.SlotState
	CompareAndSet(ctx context.Context, reqs []store.CASReq) []store.CASResult
	RefreshSlots(ctx context.Context, ref store.EpochRef, uids []string) error
	ScanConns(ctx context.Context, ref store.EpochRef, cursor uint64, count int64) ([]string, uint64, error)
	DeleteUntracked(ctx context.Context, ref store.EpochRef, asOfMs int64, uids []string, tracked map[uint64]struct{}) (int, error)

	Revoke(ctx context.Context, uid, scope string, beforeMs int64) ([]store.Revoked, error)

	RenewLease(ctx context.Context, ref store.EpochRef) (store.LeaseResult, error)
	EndEpoch(ctx context.Context, ref store.EpochRef) error
	LeasesExist(ctx context.Context, refs []store.EpochRef) ([]bool, error)
	ListEpochs(ctx context.Context) ([]store.EpochScore, error)
	EpochScoreOf(ctx context.Context, ref store.EpochRef) (int64, bool, error)

	ClaimSweep(ctx context.Context, ref store.EpochRef) (bool, error)
	ReleaseSweep(ctx context.Context, ref store.EpochRef) error
	SweepUsers(ctx context.Context, ref store.EpochRef, uids []string) (int, error)
	FinishSweep(ctx context.Context, ref store.EpochRef) error

	RedisNowMs(ctx context.Context) (int64, error)
}

var _ Store = (*store.Store)(nil)
