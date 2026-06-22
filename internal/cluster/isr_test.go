package cluster

import (
	"testing"
	"time"
)

// TestISRInSync verifies that two replicas with small lag are included in the ISR.
func TestISRInSync(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)
	tr.Update("node-a", 95)
	tr.Update("node-b", 98)

	isr := tr.ISR("leader", 100)
	// ISR must contain leader, node-a, and node-b.
	if len(isr) != 3 {
		t.Errorf("expected 3 in ISR, got %d: %v", len(isr), isr)
	}
	has := func(id string) bool {
		for _, s := range isr {
			if s == id {
				return true
			}
		}
		return false
	}
	for _, id := range []string{"leader", "node-a", "node-b"} {
		if !has(id) {
			t.Errorf("ISR missing %q: got %v", id, isr)
		}
	}
}

// TestISRLagEviction verifies that a replica whose lag exceeds maxLagMessages
// is excluded from the ISR.
func TestISRLagEviction(t *testing.T) {
	// maxLagMessages = 10; node-a lags by 50 (evicted), node-b lags by 5 (in-sync).
	tr := NewISRTracker(10, 10_000)
	tr.Update("node-a", 50) // lag = 100 - 50 = 50 > 10 → evicted
	tr.Update("node-b", 95) // lag = 100 - 95 = 5  ≤ 10 → in-sync

	isr := tr.ISR("leader", 100)

	hasLeader, hasA, hasB := false, false, false
	for _, id := range isr {
		switch id {
		case "leader":
			hasLeader = true
		case "node-a":
			hasA = true
		case "node-b":
			hasB = true
		}
	}
	if !hasLeader {
		t.Errorf("leader missing from ISR: %v", isr)
	}
	if hasA {
		t.Errorf("node-a should be evicted (lag > maxLagMessages): %v", isr)
	}
	if !hasB {
		t.Errorf("node-b should be in ISR: %v", isr)
	}
}

// TestISRTimeEviction verifies that a replica whose last contact exceeds
// maxLagMs is excluded from the ISR when the clock is advanced.
func TestISRTimeEviction(t *testing.T) {
	base := time.Now()
	fakeNow := base
	tr := NewISRTracker(1000, 5_000) // 5-second timeout
	tr.now = func() time.Time { return fakeNow }

	// Update while within the time window.
	tr.Update("node-a", 100)

	// Advance the fake clock past maxLagMs.
	fakeNow = base.Add(6 * time.Second)

	isr := tr.ISR("leader", 100)
	for _, id := range isr {
		if id == "node-a" {
			t.Errorf("node-a should be time-evicted from ISR, got %v", isr)
		}
	}
	// Leader must still be present.
	found := false
	for _, id := range isr {
		if id == "leader" {
			found = true
		}
	}
	if !found {
		t.Errorf("leader missing from ISR after time eviction: %v", isr)
	}
}

// TestISRRemove verifies that Remove evicts a node from tracking.
func TestISRRemove(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)
	tr.Update("node-a", 10)
	tr.Remove("node-a")

	isr := tr.ISR("leader", 10)
	for _, id := range isr {
		if id == "node-a" {
			t.Errorf("node-a should have been removed: %v", isr)
		}
	}
}

// TestISRSnapshot verifies Snapshot returns all tracked replicas.
func TestISRSnapshot(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)
	tr.Update("node-a", 5)
	tr.Update("node-b", 10)

	snap := tr.Snapshot()
	if len(snap) != 2 {
		t.Errorf("expected 2 snapshot entries, got %d", len(snap))
	}
}

// TestISRSnapshotVsISR verifies that Snapshot returns all replicas while
// ISR returns only the in-sync subset (excluding lagging replicas).
func TestISRSnapshotVsISR(t *testing.T) {
	tr := NewISRTracker(10, 10_000) // maxLagMessages = 10
	// Leader at offset 100.
	// node-a at 98 (lag 2, in-sync).
	// node-b at 95 (lag 5, in-sync).
	// node-c at 50 (lag 50, exceeds maxLagMessages).
	tr.Update("node-a", 98)
	tr.Update("node-b", 95)
	tr.Update("node-c", 50)

	snap := tr.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("Snapshot should have 3 replicas, got %d: %v", len(snap), snap)
	}

	isr := tr.ISR("leader", 100)
	// ISR should contain leader + node-a + node-b (3 total), but not node-c.
	if len(isr) != 3 {
		t.Errorf("ISR should have 3 entries (leader + 2 in-sync), got %d: %v", len(isr), isr)
	}
	has := func(id string) bool {
		for _, s := range isr {
			if s == id {
				return true
			}
		}
		return false
	}
	if !has("leader") {
		t.Errorf("ISR missing leader")
	}
	if !has("node-a") {
		t.Errorf("ISR missing node-a (should be in-sync)")
	}
	if !has("node-b") {
		t.Errorf("ISR missing node-b (should be in-sync)")
	}
	if has("node-c") {
		t.Errorf("node-c should NOT be in ISR (lag exceeds maxLagMessages)")
	}
}
