package testredis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestNewGivesAWorkingRedis(t *testing.T) {
	if err := New(t).Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
}

// Tier 2: the real Redis must have a linked replica that accepts DEBUG;
// Revoke's WAIT test needs both. On a Cluster, every master has one.
func TestReplicaIsAttached(t *testing.T) {
	ctx := context.Background()
	rdb := New(t)
	// Slots 15495, 3300 and 7365: one key on each master of the even split.
	for _, key := range []string{"{a}", "{b}", "{c}"} {
		replica := ReplicaOf(t, rdb, key)
		attached(t, ctx, replica)
	}
}

// Tier 2 Cluster (P5bI-4): 3 masters, each with a replica, in the zone
// layout of scripts/redis-cluster.sh.
func TestClusterHasThreeMastersWithReplicas(t *testing.T) {
	if !Cluster() {
		t.Skip("not a Cluster run (GOIM_TEST_REDIS_CLUSTER unset)")
	}
	c := New(t).(*redis.ClusterClient)
	// CLUSTER SHARDS, not SLOTS: SLOTS leaves out a replica until its
	// replication offset is above 0, i.e. until the first write.
	shards, err := c.ClusterShards(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	owners := 0
	for _, sh := range shards {
		roles := map[string]int{}
		for _, n := range sh.Nodes {
			roles[n.Role]++
		}
		if len(sh.Slots) > 0 {
			owners++
		}
		if roles["master"] != 1 || roles["replica"] != 1 {
			t.Fatalf("shard with slots %v has roles %v, want one master and one replica", sh.Slots, roles)
		}
	}
	if owners != 3 || len(shards) != 3 {
		t.Fatalf("%d shards, %d owning slots; want 3 and 3", len(shards), owners)
	}
}

func attached(t *testing.T, ctx context.Context, replica *redis.Client) {
	t.Helper()
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
