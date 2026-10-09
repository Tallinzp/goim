package gateway

import (
	"context"
	"strconv"
	"strings"

	"github.com/Tallinzp/goim/internal/fault"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fault ops a gateway serves (Plan 5b, choice #58), set by the race suite on
// a gateway started with -faults.
const (
	// FaultKick fails Kick RPCs with UNAVAILABLE before they reach a loop,
	// as a lost Kick looks to Logic (which retries while the lease is live).
	// After: the Kick is carried out first, then its reply is lost.
	// Key: the conn_id in decimal; "" drops every Kick.
	FaultKick = "kick"
	// FaultLogic fails this gateway's calls to Logic with UNAVAILABLE: a
	// gateway cut off from Logic. After: the call reaches Logic and its
	// reply (on the Login stream, its first read) is late or lost. Key: the
	// method name, e.g. "GatewayLease"; "" blocks every call.
	FaultLogic = "logic"
)

// faultInfo is what GET /fault/info returns: the ids the race suite needs to
// find this gateway's keys in Redis.
type faultInfo struct {
	Gateway string `json:"gw"`
	Epoch   string `json:"epoch"` // decimal, as in the Redis keys
	Kick    string `json:"kick"`
	Admin   string `json:"admin"`
}

// logicFault matches a FaultLogic rule against method. A rule for before
// the call is applied here; an After rule is returned for the caller to
// apply once Logic has the call.
func (g *Gateway) logicFault(ctx context.Context, method string) (after *fault.Hit, err error) {
	return applyBefore(ctx, g.faults, FaultLogic, method[strings.LastIndexByte(method, '/')+1:], "Logic blocked")
}

func applyBefore(ctx context.Context, f *fault.Set, op, key, what string) (*fault.Hit, error) {
	h, ok := f.Match(op, key)
	switch {
	case !ok:
		return nil, nil
	case h.After:
		return &h, nil
	}
	return nil, faultErr(ctx, h, what)
}

// faultErr waits out h and returns the UNAVAILABLE a failing rule causes.
func faultErr(ctx context.Context, h fault.Hit, what string) error {
	if err := h.Wait(ctx); err != nil {
		return status.Errorf(codes.Unavailable, "fault: %s: %v", what, err)
	}
	return nil
}

func (g *Gateway) faultUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	after, err := g.logicFault(ctx, method)
	if err != nil {
		return err
	}
	if err := invoker(ctx, method, req, reply, cc, opts...); err != nil || after == nil {
		return err
	}
	// Logic got the call; its reply is late or lost.
	return faultErr(ctx, *after, "Logic reply lost")
}

func (g *Gateway) faultStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	after, err := g.logicFault(ctx, method)
	if err != nil {
		return nil, err
	}
	cs, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil || after == nil {
		return cs, err
	}
	return &afterStream{ClientStream: cs, h: *after}, nil
}

// afterStream applies an After rule at the stream's first read: the request
// has gone to Logic, and its replies are late or lost.
type afterStream struct {
	grpc.ClientStream
	h    fault.Hit
	done bool
}

func (s *afterStream) RecvMsg(m any) error {
	if !s.done {
		s.done = true
		if err := faultErr(s.Context(), s.h, "Logic replies lost"); err != nil {
			return err
		}
	}
	return s.ClientStream.RecvMsg(m)
}

// kickFault matches a FaultKick rule against a Kick for connID, as
// logicFault does for calls to Logic.
func (g *Gateway) kickFault(ctx context.Context, connID uint64) (after *fault.Hit, err error) {
	return applyBefore(ctx, g.faults, FaultKick, strconv.FormatUint(connID, 10), "Kick dropped")
}
