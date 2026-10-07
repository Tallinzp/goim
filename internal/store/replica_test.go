package store

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestReplicaAckOffsets(t *testing.T) {
	info := "# Replication\r\nrole:master\r\nconnected_slaves:1\r\n" +
		"slave0:ip=172.19.0.3,port=6380,state=online,offset=1234,lag=0\r\n" +
		"master_failover_state:no-failover\r\nmaster_repl_offset:1300\r\n"
	master, acked, ok := replicaAckOffsets(info)
	if !ok || master != 1300 || acked != 1234 {
		t.Fatalf("replicaAckOffsets = %d, %d, %v; want 1300, 1234, true", master, acked, ok)
	}
	if _, _, ok := replicaAckOffsets("# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_repl_offset:5\r\n"); ok {
		t.Fatal("no replica must report ok=false")
	}
}

// replicaAckOffsets parses the primary's INFO replication: its own offset and
// the offset the first replica last acknowledged. ok is false without a replica.
func replicaAckOffsets(info string) (master, acked int64, ok bool) {
	var haveMaster bool
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if v, found := strings.CutPrefix(line, "master_repl_offset:"); found {
			master, _ = strconv.ParseInt(v, 10, 64)
			haveMaster = true
		}
		if v, found := strings.CutPrefix(line, "slave0:"); found {
			for _, kv := range strings.Split(v, ",") {
				if off, found := strings.CutPrefix(kv, "offset="); found {
					acked, _ = strconv.ParseInt(off, 10, 64)
					ok = true
				}
			}
		}
	}
	return master, acked, ok && haveMaster
}

// waitReplicaAcked waits until the primary has recorded the replica's ack of
// every write made so far. Then a WAIT on any connection without a newer write
// returns at once, which is what makes a WAIT on the wrong connection visible.
func waitReplicaAcked(t *testing.T, rdb *redis.Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		master, acked, ok := replicaAckOffsets(rdb.Info(ctx, "replication").Val())
		if ok && acked >= master {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the primary never recorded the replica's ack of earlier writes")
}
