package store

import (
	"context"
	"testing"

	"github.com/Tallinzp/goim/internal/session"
	"github.com/Tallinzp/goim/internal/testredis"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()

const field = "im:mobile"

func newStore(t *testing.T) (*Store, redis.UniversalClient) {
	t.Helper()
	rdb := testredis.New(t)
	return New(rdb, DefaultConfig()), rdb
}

func nowMs(t *testing.T, s *Store) int64 {
	t.Helper()
	n, err := s.RedisNowMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// entry builds a valid entry on gateway gw-1, epoch 1<<60 (above 2^53).
func entry(conn uint64, dev string, seq uint64, loginMs int64) session.Entry {
	return session.Entry{Gateway: "gw-1", Epoch: 1 << 60, ConnID: conn, DeviceID: dev,
		ConnSeq: seq, LoginMs: loginMs, TokIatMs: 1000}
}

func login1(t *testing.T, s *Store, uid string, e session.Entry) LoginResult {
	t.Helper()
	res := s.Login(ctx, []LoginReq{{UID: uid, Field: field, Entry: e}})[0]
	if res.Err != nil {
		t.Fatalf("login: %v", res.Err)
	}
	return res
}

func stored(t *testing.T, rdb redis.UniversalClient, uid string) string {
	t.Helper()
	v, err := rdb.HGet(ctx, sessKey(uid), field).Result()
	if err == redis.Nil {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return v
}
