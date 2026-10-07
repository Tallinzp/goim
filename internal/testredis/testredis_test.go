package testredis

import (
	"context"
	"strings"
	"testing"
)

func TestNewGivesAWorkingRedis(t *testing.T) {
	if err := New(t).Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
}

// Tier 2: the real Redis must have a linked replica that accepts DEBUG;
// Revoke's WAIT test needs both.
func TestReplicaIsAttached(t *testing.T) {
	ctx := context.Background()
	New(t)
	replica := NewReplica(t)
	info, err := replica.Info(ctx, "replication").Result()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(info, "role:slave") || !strings.Contains(info, "master_link_status:up") {
		t.Fatalf("replica is not linked to the primary:\n%s", info)
	}
	if err := replica.Do(ctx, "DEBUG", "SLEEP", "0").Err(); err != nil {
		t.Fatalf("replica rejects DEBUG (start it with --enable-debug-command yes): %v", err)
	}
}
