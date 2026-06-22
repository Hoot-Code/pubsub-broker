package cluster

import (
	"context"
	"testing"
	"time"
)

// TestQuorumReached verifies that WaitForQuorum returns nil quickly when
// enough replicas have acknowledged the required offset.
func TestQuorumReached(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)
	tr.Update("node-a", 10)
	tr.Update("node-b", 10)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// quorum=2: leader + one replica is sufficient.
	if err := WaitForQuorum(ctx, tr, "leader", 10, 2, 5*time.Millisecond); err != nil {
		t.Errorf("expected quorum to be reached, got: %v", err)
	}
}

// TestQuorumTimeout verifies that WaitForQuorum returns a non-nil error when
// quorum cannot be reached before the context deadline.
func TestQuorumTimeout(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)
	// Only leader is in ISR; quorum of 2 can never be reached.

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := WaitForQuorum(ctx, tr, "leader", 10, 2, 5*time.Millisecond)
	if err == nil {
		t.Error("expected quorum timeout error, got nil")
	}
}

// TestQuorumSingleNode verifies that a quorum of 1 is satisfied immediately
// by the leader alone without any polling.
func TestQuorumSingleNode(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := WaitForQuorum(ctx, tr, "leader", 0, 1, 5*time.Millisecond); err != nil {
		t.Errorf("single-node quorum should return immediately, got: %v", err)
	}
}

// TestQuorumDynamicUpdate verifies that WaitForQuorum eventually succeeds when
// replicas acknowledge the required offset after the call starts.
func TestQuorumDynamicUpdate(t *testing.T) {
	tr := NewISRTracker(1000, 10_000)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Simulate a replica acknowledging after a short delay.
	done := make(chan error, 1)
	go func() {
		done <- WaitForQuorum(ctx, tr, "leader", 20, 2, 5*time.Millisecond)
	}()

	time.Sleep(30 * time.Millisecond)
	tr.Update("node-a", 20)

	if err := <-done; err != nil {
		t.Errorf("expected quorum after dynamic update, got: %v", err)
	}
}
