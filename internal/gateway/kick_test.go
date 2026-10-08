package gateway

import (
	"context"
	"testing"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
	"google.golang.org/grpc"
)

func kick(t *testing.T, kc pb.GatewayClient, gw string, epoch, connID uint64, reason pb.KickReason) {
	t.Helper()
	if _, err := kc.Kick(context.Background(), &pb.KickRequest{
		Gw: &pb.GatewayRef{Gateway: gw, Epoch: epoch}, ConnId: connID, Reason: reason}); err != nil {
		t.Fatal(err)
	}
}

func TestKickOnlineSendsKickAndCloses(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	k := l.loginLive(t, 1, "u1")
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE)
	waitFor(t, "closed", k.isClosed)
	var m clientpb.Kick
	if !k.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("Kick frame %v", &m)
	}
}

func TestKickDuringAuthPendingIsAppliedWhenTheLoginReplyArrives(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			<-release
			return s.Send(okResult(0, b.Entries[0]))
		}
	})
	k := l.authed(0)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u1", "d", 1))
	waitFor(t, "Login sent", func() bool { return get(l.logic, func() int { return len(l.logic.logins) }) == 1 })
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_REPLACED) // race 8: Kick before the reply
	waitFor(t, "kick-pending", func() bool {
		return onLoop(k, func(c *Conn) bool { return c.kickPending == clientpb.KickReason_KICK_REASON_REPLACED })
	})
	if k.isClosed() {
		t.Fatal("closed before the Login reply")
	}
	close(release)
	waitFor(t, "closed", k.isClosed)
	if k.last(t, clientpb.Cmd_CMD_LOGIN_RESP, nil) || !k.last(t, clientpb.Cmd_CMD_KICK, nil) {
		t.Fatal("want a Kick frame instead of LoginResp")
	}
}

func TestKickForAnotherEpochOrUnknownConnIsIgnored(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	k := l.loginLive(t, 0, "u1")
	kick(t, kc, "gw-1", 0xE2, k.c.id, pb.KickReason_KICK_REASON_REPLACED) // race 4: zombie's epoch
	kick(t, kc, "gw-2", 0xE1, k.c.id, pb.KickReason_KICK_REASON_REPLACED)
	kick(t, kc, "gw-1", 0xE1, k.c.id+1000, pb.KickReason_KICK_REASON_REPLACED) // unknown conn_id
	kick(t, kc, "gw-1", 0xE1, 9<<48|1, pb.KickReason_KICK_REASON_REPLACED)     // worker beyond NumLoops
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_UNSPECIFIED)
	l.sync()
	if k.isClosed() {
		t.Fatal("an ignored Kick closed the connection")
	}
}

// A Kick for this conn_id proves Logic wrote the session and replaced it, so
// a Login that then has no definitive reply ends with that Kick, not Busy: a
// Busy retry would log in again and evict the device that replaced it.
func TestKickPendingWinsOverNoDefinitiveReply(t *testing.T) {
	l := newLive(t, nil)
	kc := l.kickClient(t)
	release := make(chan struct{})
	l.logic.set(func() {
		l.logic.login = func(b *pb.LoginBatch, s grpc.ServerStreamingServer[pb.LoginResult]) error {
			<-release
			return nil // the stream ends with no result for the entry
		}
	})
	k := l.authed(0)
	l.frame(k, clientpb.Cmd_CMD_LOGIN, 1, loginReq("u1", "d", 1))
	waitFor(t, "Login sent", func() bool { return get(l.logic, func() int { return len(l.logic.logins) }) == 1 })
	kick(t, kc, "gw-1", 0xE1, k.c.id, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE)
	waitFor(t, "kick-pending", func() bool {
		return onLoop(k, func(c *Conn) bool { return c.kickPending != clientpb.KickReason_KICK_REASON_UNSPECIFIED })
	})
	close(release)
	waitFor(t, "closed", k.isClosed)
	var m clientpb.Kick
	if k.last(t, clientpb.Cmd_CMD_BUSY, nil) || !k.last(t, clientpb.Cmd_CMD_KICK, &m) || m.Reason != clientpb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE {
		t.Fatalf("want the pending Kick instead of Busy, got Kick %v", &m)
	}
}
