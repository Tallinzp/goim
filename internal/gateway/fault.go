package gateway

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fault ops a gateway serves (Plan 5b, choice #58), set by the race suite on
// a gateway started with -faults.
const (
	// FaultKick fails Kick RPCs with UNAVAILABLE before they reach a loop,
	// as a lost Kick looks to Logic (which retries while the lease is live).
	// Key: the conn_id in decimal; "" drops every Kick.
	FaultKick = "kick"
	// FaultLogic fails this gateway's calls to Logic with UNAVAILABLE: a
	// gateway cut off from Logic. Key: the method name, e.g. "GatewayLease";
	// "" blocks every call.
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

// logicFault applies a FaultLogic rule to one call to method.
func (g *Gateway) logicFault(ctx context.Context, method string) error {
	h, ok := g.faults.Match(FaultLogic, method[strings.LastIndexByte(method, '/')+1:])
	if !ok {
		return nil
	}
	if err := h.Wait(ctx); err != nil {
		return status.Errorf(codes.Unavailable, "fault: Logic blocked: %v", err)
	}
	return nil
}

func (g *Gateway) faultUnary(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if err := g.logicFault(ctx, method); err != nil {
		return err
	}
	return invoker(ctx, method, req, reply, cc, opts...)
}

func (g *Gateway) faultStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if err := g.logicFault(ctx, method); err != nil {
		return nil, err
	}
	return streamer(ctx, desc, cc, method, opts...)
}

// kickFault applies a FaultKick rule to a Kick for connID.
func (g *Gateway) kickFault(ctx context.Context, connID uint64) error {
	h, ok := g.faults.Match(FaultKick, strconv.FormatUint(connID, 10))
	if !ok {
		return nil
	}
	if err := h.Wait(ctx); err != nil {
		return status.Errorf(codes.Unavailable, "fault: Kick dropped: %v", err)
	}
	return nil
}
