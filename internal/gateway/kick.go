package gateway

import (
	"context"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// kickServer is the Gateway service Logic calls by pod DNS (spec: Kick
// handling). It never blocks on a loop: the Kick is applied by an Execute on
// the connection's loop, found from conn_id >> 48.
type kickServer struct {
	pb.UnimplementedGatewayServer
	g *Gateway
}

func (k kickServer) Kick(_ context.Context, req *pb.KickRequest) (*pb.KickReply, error) {
	g := k.g
	// A Kick for another epoch of this pod name is ignored (race 4).
	if req.GetGw().GetGateway() != g.cfg.GatewayID || req.GetGw().GetEpoch() != g.cfg.Epoch {
		g.m.Kicks.WithLabelValues("ignored").Inc()
		return &pb.KickReply{}, nil
	}
	ls := g.loops.byWorker(req.GetConnId() >> 48)
	if ls == nil {
		g.m.Kicks.WithLabelValues("ignored").Inc()
		return &pb.KickReply{}, nil
	}
	id, reason := req.GetConnId(), clientpb.KickReason(req.GetReason())
	if ls.loop().Execute(func() { g.kickOnLoop(ls, id, reason) }) != nil {
		g.m.Kicks.WithLabelValues("ignored").Inc()
	}
	return &pb.KickReply{}, nil
}

// kickOnLoop: ONLINE → Kick frame and close; AUTH_PENDING with a Login in
// flight → kick-pending, applied when the Login reply arrives; anything else
// (HANDSHAKE, CLOSED, unknown conn_id) is ignored.
func (g *Gateway) kickOnLoop(ls *loopState, id uint64, reason clientpb.KickReason) {
	c := ls.conns[id]
	switch {
	case reason == clientpb.KickReason_KICK_REASON_UNSPECIFIED || c == nil || c.closing:
		g.m.Kicks.WithLabelValues("ignored").Inc()
	case c.state == stateOnline:
		g.kickConn(c, reason, "kick")
		g.m.Kicks.WithLabelValues("closed").Inc()
	case c.state == stateAuthPending && c.outcome == loginPending:
		c.kickPending = reason
		g.m.Kicks.WithLabelValues("pending").Inc()
	default:
		g.m.Kicks.WithLabelValues("ignored").Inc()
	}
}
