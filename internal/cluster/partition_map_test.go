package cluster

import (
	"encoding/json"
	"testing"
)

// TestAssignRoundRobin verifies that 3 nodes, 1 topic, 6 partitions results
// in exactly 2 partitions per node, and that the assignment is identical
// across 100 calls (deterministic).
func TestAssignRoundRobin(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	topic := "orders"
	partitions := 6

	// Run 100 times and check consistency.
	var first PartitionMap
	first.Assign(topic, partitions, nodes)

	for i := 0; i < 100; i++ {
		var pm PartitionMap
		pm.Assign(topic, partitions, nodes)
		for p := int32(0); p < int32(partitions); p++ {
			got := pm.Owner(topic, p)
			want := first.Owner(topic, p)
			if got != want {
				t.Fatalf("run %d partition %d: owner %q != %q (not deterministic)", i, p, got, want)
			}
		}
	}

	// Count partitions per node — each should own exactly 2.
	counts := map[string]int{}
	for p := int32(0); p < int32(partitions); p++ {
		owner := first.Owner(topic, p)
		counts[owner]++
	}
	for _, n := range nodes {
		if counts[n] != 2 {
			t.Errorf("node %s owns %d partitions, want 2", n, counts[n])
		}
	}
}

// TestOwnsPartition marshals then unmarshals a PartitionMap and verifies
// that Owner() returns identical results before and after the round-trip.
func TestOwnsPartition(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	const topic = "events"
	const partitions = 9

	var orig PartitionMap
	orig.Assign(topic, partitions, nodes)

	data, err := json.Marshal(&orig)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var restored PartitionMap
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	for p := int32(0); p < partitions; p++ {
		want := orig.Owner(topic, p)
		got := restored.Owner(topic, p)
		if got != want {
			t.Errorf("partition %d: before=%q after=%q", p, want, got)
		}
	}
}

// TestOwnedBy verifies that OwnedBy returns the correct set of partitions.
func TestOwnedBy(t *testing.T) {
	nodes := []string{"node-a", "node-b", "node-c"}
	const topic = "logs"
	const partitions = 6

	var pm PartitionMap
	pm.Assign(topic, partitions, nodes)

	for _, n := range nodes {
		owned := pm.OwnedBy(n)
		parts, ok := owned[topic]
		if !ok {
			t.Errorf("node %s: no entry for topic %q in OwnedBy result", n, topic)
			continue
		}
		if len(parts) != 2 {
			t.Errorf("node %s: expected 2 partitions, got %v", n, parts)
		}
	}
}
