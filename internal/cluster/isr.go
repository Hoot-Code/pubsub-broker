// Package cluster implements cluster membership, leader election, partition
// ownership, ISR tracking, log replication, and quorum writes.
package cluster

import (
	"sync"
	"time"
)

// ISRState holds the last known state for a single replica node.
type ISRState struct {
	// NodeID is the identifier of the replica node.
	NodeID string
	// LastOffset is the highest message offset the replica has acknowledged.
	LastOffset int64
	// LastContact is the time of the most recent acknowledgement from this replica.
	LastContact time.Time
}

// ISRTracker tracks which replicas are in-sync with the leader for a single
// partition. A replica is in-sync when BOTH conditions hold:
//
//	leaderOffset - replica.LastOffset ≤ maxLagMessages
//	time.Since(replica.LastContact)   ≤ maxLagMs
//
// The leader itself is always considered in-sync and is included in ISR()
// results via the leaderID argument.
type ISRTracker struct {
	mu             sync.Mutex
	replicas       map[string]*ISRState
	maxLagMessages int64
	maxLagMs       time.Duration

	// now is the clock function; injected during tests to advance time.
	now func() time.Time
}

// NewISRTracker creates an ISRTracker with the given lag bounds.
// maxLagMessages is the maximum number of messages a replica may lag behind
// the leader before being removed from the ISR.
// maxLagMs is the maximum number of milliseconds since the replica's last
// contact before it is removed from the ISR.
func NewISRTracker(maxLagMessages int64, maxLagMs int) *ISRTracker {
	return &ISRTracker{
		replicas:       make(map[string]*ISRState),
		maxLagMessages: maxLagMessages,
		maxLagMs:       time.Duration(maxLagMs) * time.Millisecond,
		now:            time.Now,
	}
}

// Update records a replica's latest acknowledged offset and resets its contact
// time to now. It is safe to call from multiple goroutines.
func (t *ISRTracker) Update(nodeID string, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, ok := t.replicas[nodeID]
	if !ok {
		s = &ISRState{NodeID: nodeID}
		t.replicas[nodeID] = s
	}
	s.LastOffset = offset
	s.LastContact = t.now()
}

// ISR returns the set of NodeIDs currently in the in-sync replica set,
// including leaderID unconditionally. A follower replica is included when
// both its lag and age constraints are satisfied.
func (t *ISRTracker) ISR(leaderID string, leaderOffset int64) []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	out := []string{leaderID}
	for nodeID, s := range t.replicas {
		if nodeID == leaderID {
			continue
		}
		lag := leaderOffset - s.LastOffset
		age := now.Sub(s.LastContact)
		if lag <= t.maxLagMessages && age <= t.maxLagMs {
			out = append(out, nodeID)
		}
	}
	return out
}

// Remove evicts a node from tracking entirely.
func (t *ISRTracker) Remove(nodeID string) {
	t.mu.Lock()
	delete(t.replicas, nodeID)
	t.mu.Unlock()
}

// Snapshot returns a copy of all tracked replica states for diagnostics.
// Order of the returned slice is not guaranteed.
func (t *ISRTracker) Snapshot() []ISRState {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]ISRState, 0, len(t.replicas))
	for _, s := range t.replicas {
		out = append(out, *s)
	}
	return out
}
