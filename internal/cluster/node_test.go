package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// nopLogger satisfies the Logger interface and discards all output.
type nopLogger struct{}

func (nopLogger) Debug(msg string, args ...any) {}
func (nopLogger) Info(msg string, args ...any)  {}
func (nopLogger) Warn(msg string, args ...any)  {}
func (nopLogger) Error(msg string, args ...any) {}

// makeTestNode builds a Node with ":0" bind address so the OS assigns a port.
func makeTestNode(t *testing.T, nodeID string, seeds []string) *Node {
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

// waitForLen polls until m.Len() == want or timeout elapses.
func waitForLen(m *Membership, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if m.Len() == want {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestNodeJoinCluster starts two Nodes (A, B with A as seed) and verifies
// that after B joins, both nodes see exactly 2 members.
func TestNodeJoinCluster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start node A (no seeds — it is the first node).
	nodeA := makeTestNode(t, "node-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	addrA := nodeA.self.Addr
	t.Logf("node-a listening on %s", addrA)

	// Start node B using node A as a seed.
	nodeB := makeTestNode(t, "node-b", []string{addrA})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	// Both nodes should settle on 2 members within 2 seconds.
	const timeout = 2 * time.Second
	if !waitForLen(nodeA.membership, 2, timeout) {
		t.Errorf("node-a: expected 2 members, got %d (members: %v)",
			nodeA.membership.Len(), memberIDs(nodeA.membership.All()))
	}
	if !waitForLen(nodeB.membership, 2, timeout) {
		t.Errorf("node-b: expected 2 members, got %d (members: %v)",
			nodeB.membership.Len(), memberIDs(nodeB.membership.All()))
	}
}

// TestNodeLeaveCluster starts three nodes, waits for full membership, stops one,
// and verifies the remaining two observe exactly 2 members within 1 second.
func TestNodeLeaveCluster(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeTestNode(t, "leave-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}

	addrA := nodeA.self.Addr

	nodeB := makeTestNode(t, "leave-b", []string{addrA})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}

	nodeC := makeTestNode(t, "leave-c", []string{addrA})
	if err := nodeC.Start(ctx); err != nil {
		t.Fatalf("nodeC.Start: %v", err)
	}

	// Wait for all three to form a cluster.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.membership.Len() == 3 && nodeB.membership.Len() == 3 && nodeC.membership.Len() == 3 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if nodeA.membership.Len() != 3 {
		t.Logf("warning: not all nodes saw 3 members before stop; a=%d b=%d c=%d",
			nodeA.membership.Len(), nodeB.membership.Len(), nodeC.membership.Len())
	}

	// Stop node C.
	if err := nodeC.Stop(); err != nil {
		t.Logf("nodeC.Stop: %v", err)
	}

	// A and B should each see 2 members within 1 second.
	const timeout = 1 * time.Second
	if !waitForLen(nodeA.membership, 2, timeout) {
		t.Errorf("node-a: expected 2 after leave, got %d (%v)",
			nodeA.membership.Len(), memberIDs(nodeA.membership.All()))
	}
	if !waitForLen(nodeB.membership, 2, timeout) {
		t.Errorf("node-b: expected 2 after leave, got %d (%v)",
			nodeB.membership.Len(), memberIDs(nodeB.membership.All()))
	}

	// Cleanup.
	nodeA.Stop() //nolint:errcheck
	nodeB.Stop() //nolint:errcheck
}

func memberIDs(members []Member) []string {
	ids := make([]string, len(members))
	for i, m := range members {
		ids[i] = fmt.Sprintf("%s@%s", m.NodeID, m.Addr)
	}
	return ids
}

// ─── Part A.e — partition assignment on leader ─────────────────────────────

// TestPartitionAssignmentOnLeader creates a 2-node cluster, waits for leader
// election, calls ReassignAll with a 4-partition topic, and verifies that each
// node owns exactly 2 partitions.
func TestPartitionAssignmentOnLeader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeTestNode(t, "node-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	addrA := nodeA.self.Addr
	nodeB := makeTestNode(t, "node-b", []string{addrA})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	// Wait for full cluster formation.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.membership.Len() == 2 && nodeB.membership.Len() == 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if nodeA.membership.Len() != 2 || nodeB.membership.Len() != 2 {
		t.Fatalf("cluster did not form: a=%d b=%d",
			nodeA.membership.Len(), nodeB.membership.Len())
	}

	// Wait for leader election.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.IsLeader() || nodeB.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	var leader *Node
	if nodeA.IsLeader() {
		leader = nodeA
	} else if nodeB.IsLeader() {
		leader = nodeB
	} else {
		t.Fatal("no leader elected")
	}

	// Attach a topic provider returning 4-partition "orders" topic.
	provider := func() []TopicInfo {
		return []TopicInfo{{Name: "orders", Partitions: 4}}
	}
	nodeA.SetTopicProvider(provider)
	nodeB.SetTopicProvider(provider)

	// Leader re-assigns and broadcasts.
	leader.ReassignAll()

	// Allow time for the partition map to propagate to the follower.
	time.Sleep(500 * time.Millisecond)

	// Count ownerships on each node.
	var aOwned, bOwned int
	for p := 0; p < 4; p++ {
		ownerA := nodeA.OwnerOf("orders", int32(p))
		ownerB := nodeB.OwnerOf("orders", int32(p))
		if ownerA == "node-a" {
			aOwned++
		}
		if ownerB == "node-b" {
			bOwned++
		}
	}
	if aOwned != 2 {
		t.Errorf("node-a owns %d partitions, want 2", aOwned)
	}
	if bOwned != 2 {
		t.Errorf("node-b owns %d partitions, want 2", bOwned)
	}

	// OwnsPartition must be exclusive: exactly one node owns each partition.
	for p := 0; p < 4; p++ {
		ownsA := nodeA.OwnsPartition("orders", int32(p))
		ownsB := nodeB.OwnsPartition("orders", int32(p))
		if ownsA == ownsB {
			t.Errorf("partition %d: ownsA=%v ownsB=%v — must be exclusive",
				p, ownsA, ownsB)
		}
	}
}

// ─── Part E5 — rebalance on membership change ─────────────────────────────

// TestRebalanceOnJoin starts a 2-node cluster with 4 partitions, verifies the
// 2-way split, adds a third node, and verifies no node owns more than 2
// partitions.
func TestRebalanceOnJoin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeTestNode(t, "rj-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	nodeB := makeTestNode(t, "rj-b", []string{nodeA.self.Addr})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	// Wait for 2-member cluster.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.membership.Len() == 2 && nodeB.membership.Len() == 2 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	provider := func() []TopicInfo {
		return []TopicInfo{{Name: "t1", Partitions: 4}}
	}
	nodeA.SetTopicProvider(provider)
	nodeB.SetTopicProvider(provider)

	// Wait for leader and trigger initial assignment.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.IsLeader() || nodeB.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	var leader *Node
	if nodeA.IsLeader() {
		leader = nodeA
	} else {
		leader = nodeB
	}
	leader.ReassignAll()
	time.Sleep(300 * time.Millisecond)

	// Add a third node.
	nodeC := makeTestNode(t, "rj-c", []string{nodeA.self.Addr})
	nodeC.SetTopicProvider(provider)
	if err := nodeC.Start(ctx); err != nil {
		t.Fatalf("nodeC.Start: %v", err)
	}
	defer nodeC.Stop() //nolint:errcheck

	// Wait 1 s for rebalance to complete.
	time.Sleep(1 * time.Second)

	// No node should own more than 2 partitions of a 4-partition topic
	// across 3 nodes (ceil(4/3) = 2).
	for _, n := range []*Node{nodeA, nodeB, nodeC} {
		owned := 0
		for p := 0; p < 4; p++ {
			if n.OwnerOf("t1", int32(p)) == n.self.NodeID {
				owned++
			}
		}
		if owned > 2 {
			t.Errorf("node %s owns %d partitions after rebalance, want ≤ 2",
				n.self.NodeID, owned)
		}
	}
}

// TestRebalanceOnLeave starts a 3-node cluster with 6 partitions, stops one
// node, and verifies the remaining 2 nodes together own all 6 partitions.
func TestRebalanceOnLeave(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodeA := makeTestNode(t, "rl-a", nil)
	if err := nodeA.Start(ctx); err != nil {
		t.Fatalf("nodeA.Start: %v", err)
	}
	defer nodeA.Stop() //nolint:errcheck

	nodeB := makeTestNode(t, "rl-b", []string{nodeA.self.Addr})
	if err := nodeB.Start(ctx); err != nil {
		t.Fatalf("nodeB.Start: %v", err)
	}
	defer nodeB.Stop() //nolint:errcheck

	nodeC := makeTestNode(t, "rl-c", []string{nodeA.self.Addr})
	if err := nodeC.Start(ctx); err != nil {
		t.Fatalf("nodeC.Start: %v", err)
	}

	// Wait for 3-member cluster.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.membership.Len() == 3 && nodeB.membership.Len() == 3 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	provider := func() []TopicInfo {
		return []TopicInfo{{Name: "t2", Partitions: 6}}
	}
	nodeA.SetTopicProvider(provider)
	nodeB.SetTopicProvider(provider)
	nodeC.SetTopicProvider(provider)

	// Wait for leader and trigger assignment.
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if nodeA.IsLeader() || nodeB.IsLeader() || nodeC.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	var leader *Node
	for _, n := range []*Node{nodeA, nodeB, nodeC} {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	if leader != nil {
		leader.ReassignAll()
	}
	time.Sleep(300 * time.Millisecond)

	// Stop nodeC.
	if err := nodeC.Stop(); err != nil {
		t.Logf("nodeC.Stop: %v", err)
	}

	// Wait 1 s for rebalance.
	time.Sleep(1 * time.Second)

	// Remaining 2 nodes must together own all 6 partitions.
	// Both nodes share the same partition map after rebalance, so checking
	// from nodeA's perspective is sufficient.
	totalOwned := 0
	for p := 0; p < 6; p++ {
		owner := nodeA.OwnerOf("t2", int32(p))
		if owner == "rl-a" || owner == "rl-b" {
			totalOwned++
		}
	}
	if totalOwned != 6 {
		for p := 0; p < 6; p++ {
			t.Logf("t2[%d] -> nodeA sees %q  nodeB sees %q",
				p, nodeA.OwnerOf("t2", int32(p)), nodeB.OwnerOf("t2", int32(p)))
		}
		t.Errorf("remaining nodes together own %d/6 partitions after rebalance", totalOwned)
	}
}
