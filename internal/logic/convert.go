package logic

import (
	"context"
	"fmt"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// epochRef converts and validates a GatewayRef.
func epochRef(g *pb.GatewayRef) (store.EpochRef, error) {
	if g == nil {
		return store.EpochRef{}, fmt.Errorf("missing gateway ref")
	}
	ref := store.EpochRef{Gateway: g.GetGateway(), Epoch: g.GetEpoch()}
	if err := session.ValidGateway(ref.Gateway); err != nil {
		return store.EpochRef{}, err
	}
	return ref, nil
}

func pbRef(ref store.EpochRef) *pb.GatewayRef {
	return &pb.GatewayRef{Gateway: ref.Gateway, Epoch: ref.Epoch}
}

var platforms = map[pb.Platform]session.Platform{
	pb.Platform_PLATFORM_MOBILE: session.Mobile,
	pb.Platform_PLATFORM_PAD:    session.Pad,
	pb.Platform_PLATFORM_WEB:    session.Web,
	pb.Platform_PLATFORM_PC:     session.PC,
}

// slotField returns the sess field "appid:platform".
func slotField(appid string, p pb.Platform) (string, error) {
	sp, ok := platforms[p]
	if !ok {
		return "", fmt.Errorf("invalid platform %v", p)
	}
	return session.SlotField(appid, sp)
}

// kickReason is the reason a replaced entry is kicked with: REPLACED for the
// same device, KICKED_BY_OTHER_DEVICE otherwise.
func kickReason(sameDevice bool) pb.KickReason {
	if sameDevice {
		return pb.KickReason_KICK_REASON_REPLACED
	}
	return pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE
}

func invalid(format string, a ...any) error {
	return status.Errorf(codes.InvalidArgument, format, a...)
}

func unavailable(format string, a ...any) error {
	return status.Errorf(codes.Unavailable, format, a...)
}

// pool bounds how many handlers of one kind run at once (spec: GatewayLease
// has its own worker pool, so Login load can't starve it, R1-O2).
type pool chan struct{}

func newPool(n int) pool { return make(pool, n) }

// acquire waits for a slot until ctx ends. A nil pool never blocks.
func (p pool) acquire(ctx context.Context) (release func(), err error) {
	if p == nil {
		return func() {}, nil
	}
	select {
	case p <- struct{}{}:
		return func() { <-p }, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}
