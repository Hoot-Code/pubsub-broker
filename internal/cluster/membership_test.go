package cluster

import (
	"fmt"
	"testing"
	"time"
)

// TestMembershipAll adds 5 members, removes 2, and verifies All() returns
// the remaining 3 sorted by NodeID.
func TestMembershipAll(t *testing.T) {
	self := Member{NodeID: "node-a", Addr: "127.0.0.1:10001", JoinedAt: time.Now()}
	m := NewMembership(self)

	// Add 4 more members (self is already present).
	for i := 1; i <= 4; i++ {
		m.Add(Member{
			NodeID:   fmt.Sprintf("node-%c", 'a'+i),
			Addr:     fmt.Sprintf("127.0.0.1:%d", 10001+i),
			JoinedAt: time.Now(),
		})
	}

	if m.Len() != 5 {
		t.Fatalf("expected 5 members, got %d", m.Len())
	}

	// Remove node-b and node-c.
	m.Remove("node-b")
	m.Remove("node-c")

	all := m.All()
	if len(all) != 3 {
		t.Fatalf("expected 3 members after removes, got %d", len(all))
	}

	// Verify sorted order and correct IDs.
	wantIDs := []string{"node-a", "node-d", "node-e"}
	for i, want := range wantIDs {
		if all[i].NodeID != want {
			t.Errorf("All()[%d].NodeID = %q, want %q", i, all[i].NodeID, want)
		}
	}
}

// TestMembershipSelfNotRemovable verifies that the self node cannot be removed.
func TestMembershipSelfNotRemovable(t *testing.T) {
	self := Member{NodeID: "leader", Addr: "127.0.0.1:5000", JoinedAt: time.Now()}
	m := NewMembership(self)
	m.Remove("leader") // should be a no-op
	if m.Len() != 1 {
		t.Errorf("expected self to survive Remove, got Len=%d", m.Len())
	}
}

// TestMembershipGet verifies Get and the not-found path.
func TestMembershipGet(t *testing.T) {
	self := Member{NodeID: "x", Addr: "127.0.0.1:1", JoinedAt: time.Now()}
	m := NewMembership(self)
	m.Add(Member{NodeID: "y", Addr: "127.0.0.1:2", JoinedAt: time.Now()})

	mem, ok := m.Get("y")
	if !ok || mem.NodeID != "y" {
		t.Error("Get: expected to find node-y")
	}

	_, ok = m.Get("z")
	if ok {
		t.Error("Get: expected not-found for unknown nodeID")
	}
}
