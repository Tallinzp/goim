package gateway

import (
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
)

// A logoutItem is one fenced Logout. The connection stays tracked until the
// Logout is acknowledged or dropped from the retry queue (spec: Tracked
// connections).
type logoutItem struct {
	ls       *loopState
	connID   uint64
	entry    *pb.LogoutEntry
	first    time.Time // first send, for the RetryFor budget
	attempts int
}

// queueLogout hands a closed connection's Logout to the sender. It runs on
// the loop once the fence holds. A full queue drops it: the next Reconcile
// pass repairs a dropped Logout (R1-O3, R3-O1).
func (g *Gateway) queueLogout(c *Conn, uid string) {
	it := &logoutItem{ls: c.ls, connID: c.id, entry: &pb.LogoutEntry{
		ConnId: c.id, Uid: uid, Appid: c.appid, Platform: c.platform}}
	select {
	case g.logoutCh <- it:
	default:
		g.m.Logouts.WithLabelValues("dropped").Inc()
		g.untrack(c)
	}
}
