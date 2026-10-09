package gateway

import (
	"sync/atomic"

	"github.com/Tallinzp/goim/api/clientpb"
	pb "github.com/Tallinzp/goim/api/internalpb"
)

// Link is the transport's side of one connection (spec: Transport seam). The
// core calls it only on the connection's loop goroutine.
type Link interface {
	// Send queues one frame; body is the encoded message for cmd.
	Send(cmd clientpb.Cmd, reqID uint32, body []byte)
	// Close closes the socket. The transport then calls Gateway.Closed once,
	// on the loop, as gnet does from OnClose.
	Close()
}

// state is the connection state machine (spec: Connection states).
type state uint8

const (
	stateHandshake state = iota
	stateAuthPending
	stateOnline
	stateClosed
)

var stateLabel = [...]string{"handshake", "auth_pending", "online", "closed"}

// outcome is what the gateway knows about a connection's Login.
type outcome uint8

const (
	loginNone     outcome = iota // no Login sent to Logic: nothing to log out
	loginPending                 // sent or queued, no definitive reply yet
	loginOK                      // definitive: the session was written
	loginRejected                // definitive: nothing was written
	loginUnknown                 // no definitive reply (fence b)
)

// Conn is one tracked connection. It lives in its loop's table from accept
// until it no longer needs a Logout (spec: Tracked connections), and is
// touched only on that loop's goroutine.
type Conn struct {
	id      uint64
	ls      *loopState
	link    Link // nil once closed
	state   state
	closing bool // Close requested; Closed not called yet

	life    timer // HANDSHAKE, then AUTH_PENDING, then heartbeat
	fence   timer // Logout fence (b): recv_ms + login_deadline + 1s
	onLife  func()
	onFence func()

	lastActive int64 // ms, last frame while ONLINE

	login       *loginItem // the Login while pending or unknown
	loginReqID  uint32
	outcome     outcome
	fenceB      bool
	kickPending clientpb.KickReason // a Kick that arrived during AUTH_PENDING

	// The session, for Reconcile and Logout. uid is the token's verified
	// subject from Logic's reply.
	uid       string
	appid     string
	platform  pb.Platform
	deviceID  string
	connSeq   uint64
	recvMs    int64 // login_ms
	tokIatMs  int64
	reason    clientpb.ReconnectReason
	offlineMs int64

	inChunk      bool // in a Reconcile chunk not yet resolved (fence c)
	logoutQueued bool
	reconnected  bool // Reconnect sent (drain): once per connection
}

// ID returns the conn_id.
func (c *Conn) ID() uint64 { return c.id }

// A loginItem is one Login between the loop and Logic. Its state is the
// hand-off between the loop (which may cancel it on close) and the Login
// worker (which sends it or drops it at login_deadline): whoever moves it out
// of itemQueued owns the outcome.
type loginItem struct {
	ls     *loopState
	connID uint64
	entry  *pb.LoginEntry
	state  atomic.Int32
}

const (
	itemQueued int32 = iota
	itemSent
	itemCancelled // the connection closed first: never sent, no Logout
	itemDropped   // older than login_deadline at send time: never sent
)
