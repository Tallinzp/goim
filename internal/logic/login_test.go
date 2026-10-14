package logic

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/Tallinzp/goim/api/internalpb"
	"github.com/Tallinzp/goim/internal/auth"
	"github.com/Tallinzp/goim/internal/store"
	"github.com/Tallinzp/goim/internal/testredis"
)

func TestLoginWritesAndReturnsUID(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	entry := e.loginEntry(uid, 7, "d1", 1)
	r := e.login(c, entry)[0]
	if r.Code != pb.LoginCode_LOGIN_CODE_OK || r.Uid != uid || r.TokIatMs != entry.RecvMs-1000 {
		t.Fatalf("result %+v", r)
	}
	uids, _, err := e.real.ScanConns(ctx, e.gw, 0, 100)
	if err != nil || len(uids) != 1 || uids[0] != uid {
		t.Fatalf("gwconns %v %v, want [%s]", uids, err, uid)
	}
	if len(e.kicks.all()) != 0 {
		t.Fatal("kick for an empty slot")
	}
}

func TestLoginKicksReplacedEntry(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	e.login(c, e.loginEntry(uid, 1, "d1", 1))
	// Same device, higher conn_seq: REPLACED.
	if r := e.login(c, e.loginEntry(uid, 2, "d1", 2))[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v", r)
	}
	// Another device: KICKED_BY_OTHER_DEVICE.
	if r := e.login(c, e.loginEntry(uid, 3, "d2", 1))[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v", r)
	}
	want := []recordedKick{
		{e.gw, 1, pb.KickReason_KICK_REASON_REPLACED},
		{e.gw, 2, pb.KickReason_KICK_REASON_KICKED_BY_OTHER_DEVICE},
	}
	got := e.kicks.all()
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("kicks %+v, want %+v", got, want)
	}
	if counter(e.m.Logins.WithLabelValues("replaced")) != 1 || counter(e.m.Logins.WithLabelValues("kicked")) != 1 {
		t.Fatal("result counters")
	}
}

func TestLoginRejections(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	k9, err := auth.GenerateKey("k9")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("stale returns the stored conn_seq", func(t *testing.T) {
		uid := testredis.UID()
		e.login(c, e.loginEntry(uid, 1, "d1", 5))
		r := e.login(c, e.loginEntry(uid, 2, "d1", 3))[0]
		if r.Code != pb.LoginCode_LOGIN_CODE_STALE || r.StoredConnSeq != 5 {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("superseded", func(t *testing.T) {
		uid := testredis.UID()
		e.login(c, e.loginEntry(uid, 1, "d1", 1))
		late := e.loginEntry(uid, 2, "d2", 1)
		late.RecvMs -= 100 // received before the stored login
		if r := e.login(c, late)[0]; r.Code != pb.LoginCode_LOGIN_CODE_SUPERSEDED {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("expired", func(t *testing.T) {
		old := e.loginEntry(testredis.UID(), 1, "d1", 1)
		old.RecvMs -= e.cfg.Store.LoginDeadline.Milliseconds() + 1000
		if r := e.login(c, old)[0]; r.Code != pb.LoginCode_LOGIN_CODE_EXPIRED {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("revoked", func(t *testing.T) {
		uid := testredis.UID()
		entry := e.loginEntry(uid, 1, "d1", 1) // token iat = recv − 1s
		if err := e.rdb.HSet(ctx, "rev:{"+uid+"}", "*", entry.RecvMs).Err(); err != nil {
			t.Fatal(err)
		}
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_REVOKED {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("token issued in the marker's millisecond logs in", func(t *testing.T) {
		uid := testredis.UID()
		entry := e.loginEntry(uid, 1, "d1", 1) // iat = recv − 1000
		if err := e.rdb.HSet(ctx, "rev:{"+uid+"}", "*", entry.RecvMs-1000).Err(); err != nil {
			t.Fatal(err)
		}
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_OK {
			t.Fatalf("iat == marker: %+v", r)
		}
	})
	t.Run("key unknown", func(t *testing.T) {
		entry := e.loginEntry(testredis.UID(), 1, "d1", 1)
		entry.Token, _ = auth.Sign(k9, auth.Claims{UID: "u", AppID: "im", Platform: "mobile",
			IatMs: 1, ExpMs: time.Now().UnixMilli() + 60_000})
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_KEY_UNKNOWN {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("token for another slot", func(t *testing.T) {
		entry := e.loginEntry(testredis.UID(), 1, "d1", 1) // token says mobile
		entry.Platform = pb.Platform_PLATFORM_PAD
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_TOKEN_INVALID {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("bad signature", func(t *testing.T) {
		entry := e.loginEntry(testredis.UID(), 1, "d1", 1)
		// Flip a character in the middle of the signature (the last one is
		// partly padding bits, which the decoder ignores).
		tok := []byte(entry.Token)
		i := len(tok) - 20
		if tok[i] == 'A' {
			tok[i] = 'B'
		} else {
			tok[i] = 'A'
		}
		entry.Token = string(tok)
		if r := e.login(c, entry)[0]; r.Code != pb.LoginCode_LOGIN_CODE_TOKEN_INVALID {
			t.Fatalf("result %+v", r)
		}
	})
	t.Run("bad request", func(t *testing.T) {
		pipe := e.loginEntry(testredis.UID(), 1, "d|1", 1)
		noPlatform := e.loginEntry(testredis.UID(), 2, "d1", 1)
		noPlatform.Platform = pb.Platform_PLATFORM_UNSPECIFIED
		bigSeq := e.loginEntry(testredis.UID(), 3, "d1", 1<<53)
		good := e.loginEntry(testredis.UID(), 4, "d1", 1)
		rs := e.login(c, pipe, noPlatform, bigSeq, good)
		for i := range uint32(3) {
			if rs[i].Code != pb.LoginCode_LOGIN_CODE_BAD_REQUEST {
				t.Fatalf("entry %d: %+v", i, rs[i])
			}
		}
		if rs[3].Code != pb.LoginCode_LOGIN_CODE_OK {
			t.Fatalf("a bad entry failed its neighbour: %+v", rs[3])
		}
	})
}

// gatedLogin delays the entries of one uid until release is closed (or 2s
// pass, so a handler that collects results fails instead of hanging).
type gatedLogin struct {
	*store.Store
	slowUID string
	release chan struct{}
}

func (g *gatedLogin) LoginEach(ctx context.Context, reqs []store.LoginReq, done func(int, store.LoginResult)) {
	var fast, slow []int
	for i, r := range reqs {
		if r.UID == g.slowUID {
			slow = append(slow, i)
		} else {
			fast = append(fast, i)
		}
	}
	run := func(is []int) {
		sub := make([]store.LoginReq, len(is))
		for k, i := range is {
			sub[k] = reqs[i]
		}
		g.Store.LoginEach(ctx, sub, func(k int, r store.LoginResult) { done(is[k], r) })
	}
	run(fast)
	select {
	case <-g.release:
	case <-time.After(2 * time.Second):
	}
	run(slow)
}

// Spec (R5S-O1): with one entry's script delayed, the other entries' results
// arrive before it.
func TestLoginStreamsEachEntryAsItFinishes(t *testing.T) {
	slow := testredis.UID()
	gate := &gatedLogin{slowUID: slow, release: make(chan struct{})}
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { gate.Store = s; return gate })
	c := e.dial()
	entries := []*pb.LoginEntry{e.loginEntry(slow, 1, "d", 1)}
	for i := range 3 {
		entries = append(entries, e.loginEntry(testredis.UID(), uint64(i+2), "d", 1))
	}
	stream, err := c.Login(ctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for range 3 {
		r, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if r.Index == 0 {
			t.Fatal("the delayed entry arrived before the others")
		}
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("fast entries took %v: held back by the slow one", d)
	}
	close(gate.release)
	r, err := stream.Recv()
	if err != nil || r.Index != 0 || r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("slow entry: %+v %v", r, err)
	}
}

// failingLogin reports every entry's outcome as unknown.
type failingLogin struct{ *store.Store }

func (f failingLogin) LoginEach(_ context.Context, reqs []store.LoginReq, done func(int, store.LoginResult)) {
	for i := range reqs {
		done(i, store.LoginResult{Err: errors.New("i/o timeout")})
	}
}

func TestLoginOutcomeUnknownIsNotDefinitive(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return failingLogin{s} })
	r := e.login(e.dial(), e.loginEntry(testredis.UID(), 1, "d", 1))[0]
	if r.Code != pb.LoginCode_LOGIN_CODE_OUTCOME_UNKNOWN {
		t.Fatalf("result %+v", r)
	}
}

// Spec (R5S-O2): Login results return without waiting for the Kick, even when
// the replaced connection's gateway never answers.
func TestLoginDoesNotWaitForKick(t *testing.T) {
	e := newEnv(t)
	deadGW := testredis.Gateway()
	kicker := NewKicker(e.cfg.Kick, e.svc.leases, e.m, staticDial(map[string]string{deadGW: blackhole(t)}))
	t.Cleanup(kicker.Close)
	e.svc.kick = kicker
	c := e.dial()
	uid := testredis.UID()

	// The stored session lives on a gateway that is now blackholed.
	other := *e
	other.gw = store.EpochRef{Gateway: deadGW, Epoch: 1}
	other.login(c, other.loginEntry(uid, 1, "d1", 1))

	start := time.Now()
	r := e.login(c, e.loginEntry(uid, 2, "d2", 1))[0]
	if d := time.Since(start); d >= e.cfg.Kick.Deadline/2 {
		t.Fatalf("login took %v with a %v kick deadline", d, e.cfg.Kick.Deadline)
	}
	if r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v", r)
	}
	waitFor(t, "the kick attempt", func() bool { return counter(e.m.KickAttempts.WithLabelValues("first", "timeout")) == 1 })
}

func TestLoginDeadEpochCrossCheck(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	uid := testredis.UID()
	dead := *e
	dead.gw = store.EpochRef{Gateway: testredis.Gateway(), Epoch: 2}
	res, err := e.real.RenewLease(ctx, dead.gw) // last renewal recorded in gwepochs
	if err != nil {
		t.Fatal(err)
	}
	dead.login(c, dead.loginEntry(uid, 1, "d1", 1))
	if _, err := e.rdb.Del(ctx, "gwlease:"+dead.gw.Gateway+":{2}").Result(); err != nil {
		t.Fatal(err)
	}
	entry := e.loginEntry(uid, 2, "d1", 2)
	e.login(c, entry)
	h := e.m.DeadEpochRelogin
	if n := sampleCount(t, h); n != 1 {
		t.Fatalf("cross-check samples %d, want 1", n)
	}
	if s := sampleSum(t, h); s < 0 || s > float64(entry.RecvMs-res.RedisNowMs)/1000+0.01 {
		t.Fatalf("cross-check %vs", s)
	}
}

func TestLogout(t *testing.T) {
	e := newEnv(t)
	c := e.dial()
	mine, theirs := testredis.UID(), testredis.UID()
	e.login(c, e.loginEntry(mine, 1, "d1", 1), e.loginEntry(theirs, 2, "d1", 1))
	e.login(c, e.loginEntry(theirs, 3, "d1", 2)) // conn 2 replaced by conn 3

	rep, err := c.Logout(ctx, &pb.LogoutBatch{Gw: pbRef(e.gw), Entries: []*pb.LogoutEntry{
		{ConnId: 1, Uid: mine, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE},
		{ConnId: 2, Uid: theirs, Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE}, // not the owner any more
		{ConnId: 4, Uid: "bad uid", Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Done) != 3 || !rep.Done[0] || !rep.Done[1] || !rep.Done[2] {
		t.Fatalf("done %v", rep.Done)
	}
	if v, _ := e.rdb.HGet(ctx, "sess:{"+mine+"}", field).Result(); v != "" {
		t.Fatalf("own field not deleted: %q", v)
	}
	if v, _ := e.rdb.HGet(ctx, "sess:{"+theirs+"}", field).Result(); v == "" {
		t.Fatal("a newer session was deleted")
	}
	for label, want := range map[string]float64{"deleted": 1, "not_owner": 1, "invalid": 1} {
		if got := counter(e.m.Logouts.WithLabelValues(label)); got != want {
			t.Errorf("%s = %v, want %v", label, got, want)
		}
	}
}

// failingLogout reports every entry's outcome as unknown.
type failingLogout struct{ *store.Store }

func (f failingLogout) LogoutEach(_ context.Context, reqs []store.LogoutReq, done func(int, store.LogoutResult)) {
	for i := range reqs {
		done(i, store.LogoutResult{Err: errors.New("i/o timeout")})
	}
}

func TestLogoutUnknownOutcomeIsRetried(t *testing.T) {
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { return failingLogout{s} })
	rep, err := e.dial().Logout(ctx, &pb.LogoutBatch{Gw: pbRef(e.gw), Entries: []*pb.LogoutEntry{
		{ConnId: 1, Uid: testredis.UID(), Appid: "im", Platform: pb.Platform_PLATFORM_MOBILE}}})
	if err != nil || len(rep.Done) != 1 || rep.Done[0] {
		t.Fatalf("reply %v err %v: want done=[false]", rep, err)
	}
}

// Review Focus 3: two logins for one slot in the same batch (a fast reconnect
// within one 5ms batch) resolve in order: the later one wins and kicks the first.
func TestLoginSameSlotTwiceInOneBatch(t *testing.T) {
	e := newEnv(t)
	uid := testredis.UID()
	rs := e.login(e.dial(), e.loginEntry(uid, 1, "d1", 1), e.loginEntry(uid, 2, "d1", 2))
	if rs[0].Code != pb.LoginCode_LOGIN_CODE_OK || rs[1].Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("results %+v %+v", rs[0], rs[1])
	}
	want := recordedKick{e.gw, 1, pb.KickReason_KICK_REASON_REPLACED}
	if k := e.kicks.all(); len(k) != 1 || k[0] != want {
		t.Fatalf("kicks %+v, want %+v", k, want)
	}
}

// Review Focus 4: an empty batch is answered with an empty stream.
func TestLoginEmptyBatch(t *testing.T) {
	e := newEnv(t)
	e.login(e.dial()) // login() fails the test on a result count other than 0
}

// heldAddConns holds the gwconns SADD until release, then records whether its
// context was still alive.
type heldAddConns struct {
	*store.Store
	release chan struct{}
	ctxErr  chan error
}

func (h *heldAddConns) AddConns(ctx context.Context, ref store.EpochRef, uids []string) error {
	<-h.release
	h.ctxErr <- ctx.Err()
	return h.Store.AddConns(ctx, ref, uids)
}

// Review Focus 2: a gateway that cancels the stream once it has every result
// does not cancel the follow-ups.
func TestLoginFollowUpsOutliveTheStream(t *testing.T) {
	h := &heldAddConns{release: make(chan struct{}), ctxErr: make(chan error, 1)}
	e := newEnvWith(t, testConfig(), func(s *store.Store) Store { h.Store = s; return h })
	uid := testredis.UID()
	sctx, cancel := context.WithCancel(ctx)
	stream, err := e.dial().Login(sctx, &pb.LoginBatch{Gw: pbRef(e.gw), Entries: []*pb.LoginEntry{e.loginEntry(uid, 1, "d", 1)}})
	if err != nil {
		t.Fatal(err)
	}
	if r, err := stream.Recv(); err != nil || r.Code != pb.LoginCode_LOGIN_CODE_OK {
		t.Fatalf("result %+v %v", r, err)
	}
	cancel()
	time.Sleep(50 * time.Millisecond) // let the cancellation reach the server
	close(h.release)
	if err := <-h.ctxErr; err != nil {
		t.Fatalf("follow-up context cancelled with the stream: %v", err)
	}
	waitFor(t, "uid in gwconns", func() bool {
		uids, _, _ := e.real.ScanConns(ctx, e.gw, 0, 10)
		return len(uids) == 1
	})
}
