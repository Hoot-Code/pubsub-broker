package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// makeReplicatorNode builds a Node with a real TCP transport ready for
// replication tests.
func makeReplicatorNode(t *testing.T, nodeID string, seeds []string) *Node {
	t.Helper()
	cfg := config.ClusterConfig{
		Enabled:            true,
		NodeID:             nodeID,
		BindAddr:           "127.0.0.1:0",
		Seeds:              seeds,
		HeartbeatInterval:  50,
		ElectionTimeoutMin: 150,
		ElectionTimeoutMax: 250,
	}
	n, err := NewNode(cfg, nil, nopLogger{})
	if err != nil {
		t.Fatalf("NewNode %s: %v", nodeID, err)
	}
	return n
}

// openTestLog creates a PartitionLog backed by a temporary directory.
func openTestLog(t *testing.T) *storage.PartitionLog {
	t.Helper()
	pl, err := storage.OpenPartitionLog(t.TempDir(), 64*1024*1024, 4096, "always")
	if err != nil {
		t.Fatalf("OpenPartitionLog: %v", err)
	}
	return pl
}

// waitMembership polls until membership.Len() == want or timeout expires.
func waitMembership(t *testing.T, m *Membership, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.Len() == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Errorf("membership: got %d members, want %d", m.Len(), want)
}

// waitLeader polls until any node in the list is leader or timeout expires.
// Returns the leader and follower nodes (leader, follower).
func waitLeader(t *testing.T, timeout time.Duration,
	nodes ...*Node) (leader, follower *Node) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				leader = n
				for _, other := range nodes {
					if other != n {
						follower = other
					}
				}
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return
}

// TestReplicatorLeaderToFollower verifies that messages appended on the leader
// are replicated to the follower with matching offsets and payloads.
func TestReplicatorLeaderToFollower(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeReplicatorNode(t, "repl-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	nodeB := makeReplicatorNode(t, "repl-b", []string{nodeA.self.Addr})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	waitMembership(t, nodeA.membership, 2, 3*time.Second)
	waitMembership(t, nodeB.membership, 2, 3*time.Second)

	// Wait for a stable leader (same node for 3 consecutive 100 ms probes).
	var leader, follower *Node
	for attempt := 0; attempt < 30; attempt++ {
		l1, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		time.Sleep(100 * time.Millisecond)
		l2, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		time.Sleep(100 * time.Millisecond)
		l3, f3 := waitLeader(t, 2*time.Second, nodeA, nodeB)
		if l1 == l2 && l2 == l3 {
			leader, follower = l3, f3
			break
		}
	}
	if leader == nil {
		t.Fatal("could not elect a stable leader")
	}
	t.Logf("stable leader=%s follower=%s", leader.self.NodeID, follower.self.NodeID)

	plA := openTestLog(t)
	plB := openTestLog(t)
	defer plA.Close() //nolint:errcheck
	defer plB.Close() //nolint:errcheck

	const topic = "repl-test"
	const part = int32(0)

	// Register partitions before writing.
	nodeA.RegisterPartition(topic, part, plA)
	nodeB.RegisterPartition(topic, part, plB)

	var leaderPL, followerPL *storage.PartitionLog
	if leader == nodeA {
		leaderPL, followerPL = plA, plB
	} else {
		leaderPL, followerPL = plB, plA
	}

	// Append 20 messages on the leader.
	const count = 20
	for i := 0; i < count; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("msg-%d", i),
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
		}
		if _, err := leaderPL.Append(msg); err != nil {
			t.Fatalf("leader append %d: %v", i, err)
		}
	}

	// Wait up to 8 s for the follower to catch up.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if followerPL.NextOffset() >= int64(count) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	got := followerPL.NextOffset()
	if got < int64(count) {
		t.Fatalf("follower has %d messages, want %d", got, count)
	}

	// Verify offsets match.
	msgs, err := followerPL.Read(0, count)
	if err != nil {
		t.Fatalf("follower Read: %v", err)
	}
	if len(msgs) != count {
		t.Fatalf("follower Read returned %d messages, want %d", len(msgs), count)
	}
	for i, m := range msgs {
		if m.Offset != int64(i) {
			t.Errorf("msgs[%d].Offset = %d, want %d", i, m.Offset, i)
		}
	}
}

// TestReplicatorCatchUp verifies that a follower catches up when it is 50
// messages behind the leader at the time replication begins.
func TestReplicatorCatchUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeReplicatorNode(t, "catch-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	nodeB := makeReplicatorNode(t, "catch-b", []string{nodeA.self.Addr})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	waitMembership(t, nodeA.membership, 2, 3*time.Second)
	waitMembership(t, nodeB.membership, 2, 3*time.Second)

	// Wait for stable leadership: poll until the same node reports as leader
	// for three consecutive checks 100 ms apart, eliminating re-election races.
	var leader *Node
	for attempt := 0; attempt < 30; attempt++ {
		l1, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		time.Sleep(100 * time.Millisecond)
		l2, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		time.Sleep(100 * time.Millisecond)
		l3, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		if l1 == l2 && l2 == l3 {
			leader = l1
			break
		}
	}
	if leader == nil {
		t.Fatal("could not elect a stable leader")
	}
	t.Logf("stable leader=%s", leader.self.NodeID)

	plA := openTestLog(t)
	plB := openTestLog(t)
	defer plA.Close() //nolint:errcheck
	defer plB.Close() //nolint:errcheck

	var leaderPL, followerPL *storage.PartitionLog
	if leader == nodeA {
		leaderPL, followerPL = plA, plB
	} else {
		leaderPL, followerPL = plB, plA
	}

	const topic = "catch-up"
	const part = int32(0)
	const backlog = 50

	// Register partitions first so the replicator knows about them before we
	// write messages. This avoids a gap where messages exist but no replication
	// channel is active.
	nodeA.RegisterPartition(topic, part, plA)
	nodeB.RegisterPartition(topic, part, plB)

	// Pre-write 50 messages to the leader log AFTER registering with the
	// replicator so the follower has a registered channel to receive them on.
	for i := 0; i < backlog; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("catch-%d", i),
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("data-%d", i)),
		}
		if _, err := leaderPL.Append(msg); err != nil {
			t.Fatalf("pre-write %d: %v", i, err)
		}
	}

	// Follower must catch up within 8 seconds.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if followerPL.NextOffset() >= backlog {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if followerPL.NextOffset() < backlog {
		t.Errorf("catch-up incomplete: follower at offset %d, want >= %d",
			followerPL.NextOffset(), backlog)
	}
}

// TestReplicatorNoDuplicates verifies that when a leader appends 10 messages
// and replication runs, the follower log ends up with exactly 10 messages —
// not 20 — even if both the leaderTick push and a MsgReplicaFetch response
// deliver the same batch concurrently.
func TestReplicatorNoDuplicates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeReplicatorNode(t, "nodup-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	nodeB := makeReplicatorNode(t, "nodup-b", []string{nodeA.self.Addr})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	waitMembership(t, nodeA.membership, 2, 3*time.Second)
	waitMembership(t, nodeB.membership, 2, 3*time.Second)

	// Wait for a stable leader (same node for 3 consecutive 100 ms probes).
	var leader *Node
	for attempt := 0; attempt < 30; attempt++ {
		l1, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		time.Sleep(100 * time.Millisecond)
		l2, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		time.Sleep(100 * time.Millisecond)
		l3, _ := waitLeader(t, 2*time.Second, nodeA, nodeB)
		if l1 == l2 && l2 == l3 {
			leader = l1
			break
		}
	}
	if leader == nil {
		t.Fatal("could not elect a stable leader")
	}
	t.Logf("stable leader=%s", leader.self.NodeID)

	plA := openTestLog(t)
	plB := openTestLog(t)
	defer plA.Close() //nolint:errcheck
	defer plB.Close() //nolint:errcheck

	const topic = "nodup-test"
	const part = int32(0)

	// Register partitions before writing so the replication channel is ready.
	nodeA.RegisterPartition(topic, part, plA)
	nodeB.RegisterPartition(topic, part, plB)

	var leaderPL, followerPL *storage.PartitionLog
	if leader == nodeA {
		leaderPL, followerPL = plA, plB
	} else {
		leaderPL, followerPL = plB, plA
	}

	// Append exactly 10 messages on the leader.
	const count = 10
	for i := 0; i < count; i++ {
		msg := &types.Message{
			ID:      fmt.Sprintf("nodup-%d", i),
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
		}
		if _, err := leaderPL.Append(msg); err != nil {
			t.Fatalf("leader append %d: %v", i, err)
		}
	}

	// Wait for follower to catch up.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if followerPL.NextOffset() >= int64(count) {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Allow an extra 300 ms for any spurious second delivery to arrive.
	time.Sleep(300 * time.Millisecond)

	got := followerPL.NextOffset()
	if got != int64(count) {
		t.Fatalf("follower has %d messages, want exactly %d (duplicate detection failed)", got, count)
	}
}
