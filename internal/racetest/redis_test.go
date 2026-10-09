package racetest

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

// Redis is the stack's Redis: a redis-server primary and a replica, two
// processes started fresh for each scenario, on the dev machine and on the
// Tier 2 host alike (choice #62). Revoke's WAIT needs the replica.
type Redis struct {
	t                *testing.T
	Addr             string
	rdb              *redis.Client
	primary, replica *proc
}

// startRedis starts the primary and its replica and waits until the replica
// is linked, so a WAIT in the scenario fails only when the scenario stops it.
func startRedis(t *testing.T, dir string) *Redis {
	t.Helper()
	r := &Redis{t: t, Addr: freePort(t)}
	replicaAddr := freePort(t)
	host, port, _ := net.SplitHostPort(r.Addr)
	r.primary = startRedisServer(t, dir, "redis", r.Addr)
	r.replica = startRedisServer(t, dir, "redis-replica", replicaAddr, "--replicaof", host, port)
	r.rdb = redis.NewClient(&redis.Options{Addr: r.Addr})
	replica := redis.NewClient(&redis.Options{Addr: replicaAddr})
	t.Cleanup(func() { r.rdb.Close(); replica.Close() })
	eventually(t, "the replica is linked to the primary", 10*time.Second, func() bool {
		return strings.Contains(replica.Info(ctx, "replication").Val(), "master_link_status:up") &&
			strings.Contains(r.rdb.Info(ctx, "replication").Val(), "connected_slaves:1")
	})
	return r
}

// startRedisServer runs redis-server on addr with nothing persisted and the
// eviction policy production uses (noeviction). A diskless sync with no
// delay links a replica in well under a second.
func startRedisServer(t *testing.T, dir, name, addr string, extra ...string) *proc {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	data := filepath.Join(dir, name)
	if err := os.MkdirAll(data, 0o755); err != nil {
		t.Fatal(err)
	}
	args := []string{"--bind", host, "--port", port, "--dir", data, "--save", "", "--appendonly", "no",
		"--maxmemory-policy", "noeviction", "--repl-diskless-sync", "yes", "--repl-diskless-sync-delay", "0"}
	return startProc(t, dir, name, redisServer, append(args, extra...)...)
}

// PauseFor makes Redis stop answering for d and returns at once: every
// client's commands block until it resumes (spec: Redis paused for 90s).
// The primary is SIGSTOPped; when it resumes, keys whose TTL ran out
// meanwhile expire, as Redis's expiry times are absolute.
func (r *Redis) PauseFor(d time.Duration) {
	r.primary.pause()
	time.AfterFunc(d, r.primary.resume)
}

// ReplicaDownFor stops the replica for d, so it acknowledges no write and a
// Revoke's WAIT times out with 0 replicas.
func (r *Redis) ReplicaDownFor(d time.Duration) {
	r.replica.pause()
	time.AfterFunc(d, r.replica.resume)
}

// Client is a plain client for checks and for writing state by hand.
func (r *Redis) Client() *redis.Client { return r.rdb }

const slot = "im:mobile" // every scenario logs in as appid im, platform mobile

// Entry returns the stored owner of uid's im:mobile slot.
func (r *Redis) Entry(uid string) (session.Entry, bool) { return r.EntryOf(uid, slot) }

// EntryOf returns the stored owner of uid's field.
func (r *Redis) EntryOf(uid, field string) (session.Entry, bool) {
	r.t.Helper()
	v, err := r.rdb.HGet(ctx, "sess:{"+uid+"}", field).Result()
	if err == redis.Nil {
		return session.Entry{}, false
	}
	if err != nil {
		r.t.Fatal(err)
	}
	e, err := session.Decode(v)
	if err != nil {
		r.t.Fatalf("sess:{%s} %s = %q: %v", uid, field, v, err)
	}
	return e, true
}

func epochSuffix(gw string, epoch uint64) string {
	return gw + ":{" + strconv.FormatUint(epoch, 10) + "}"
}

// LeaseExists reports whether the epoch's lease key exists.
func (r *Redis) LeaseExists(gw string, epoch uint64) bool {
	return r.rdb.Exists(ctx, "gwlease:"+epochSuffix(gw, epoch)).Val() == 1
}

// DelLease deletes the epoch's lease key: the epoch reads as dead, and its
// next renewal reports missing.
func (r *Redis) DelLease(gw string, epoch uint64) {
	if err := r.rdb.Del(ctx, "gwlease:"+epochSuffix(gw, epoch)).Err(); err != nil {
		r.t.Fatal(err)
	}
}

// Score returns the epoch's gwepochs score; ok is false once it was swept.
func (r *Redis) Score(gw string, epoch uint64) (ms float64, ok bool) {
	v, err := r.rdb.ZScore(ctx, "gwepochs", gw+":"+strconv.FormatUint(epoch, 10)).Result()
	if err == redis.Nil {
		return 0, false
	}
	if err != nil {
		r.t.Fatal(err)
	}
	return v, true
}

// SetScore sets the epoch's gwepochs score, e.g. to make it look stale.
func (r *Redis) SetScore(gw string, epoch uint64, ms int64) {
	if err := r.rdb.ZAdd(ctx, "gwepochs", redis.Z{Score: float64(ms), Member: gw + ":" + strconv.FormatUint(epoch, 10)}).Err(); err != nil {
		r.t.Fatal(err)
	}
}

// Conns returns the epoch's gwconns members.
func (r *Redis) Conns(gw string, epoch uint64) []string {
	return r.rdb.SMembers(ctx, "gwconns:"+epochSuffix(gw, epoch)).Val()
}

// Now returns Redis TIME in ms.
func (r *Redis) Now() int64 {
	r.t.Helper()
	tm, err := r.rdb.Time(ctx).Result()
	if err != nil {
		r.t.Fatal(err)
	}
	return tm.UnixMilli()
}

// Put writes e as uid's im:mobile entry by hand and adds uid to the epoch's
// gwconns set, as a Login would: a ghost, or an entry a lost write left.
func (r *Redis) Put(uid string, e session.Entry) {
	r.t.Helper()
	if _, err := r.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, "sess:{"+uid+"}", slot, e.Encode())
		p.SAdd(ctx, "gwconns:"+epochSuffix(e.Gateway, e.Epoch), uid)
		return nil
	}); err != nil {
		r.t.Fatal(err)
	}
}

// Del deletes uid's im:mobile entry by hand, as a lost write or a sweep would.
func (r *Redis) Del(uid string) {
	if err := r.rdb.HDel(ctx, "sess:{"+uid+"}", slot).Err(); err != nil {
		r.t.Fatal(err)
	}
}

// Ghost writes an entry for gateway g's current epoch whose connection g
// never had (conn_id from the top of the range), logged in a minute ago.
func (r *Redis) Ghost(uid string, g *Gateway, connID uint64) session.Entry {
	r.t.Helper()
	now := r.Now()
	e := session.Entry{Gateway: g.ID, Epoch: g.Epoch, ConnID: connID, DeviceID: "ghost-" + uid, ConnSeq: 1,
		LoginMs: now - 60_000, TokIatMs: now - 61_000}
	r.Put(uid, e)
	return e
}

func (r *Redis) has(uid string) bool { _, ok := r.Entry(uid); return ok }
