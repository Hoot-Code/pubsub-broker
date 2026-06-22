package cluster

import (
	"sort"
	"sync"
)

// Membership is a thread-safe, in-memory registry of cluster members.
type Membership struct {
	mu      sync.RWMutex
	self    Member
	members map[string]Member // NodeID → Member
}

// NewMembership creates a Membership registry with self pre-registered.
func NewMembership(self Member) *Membership {
	m := &Membership{
		self:    self,
		members: make(map[string]Member),
	}
	m.members[self.NodeID] = self
	return m
}

// Self returns the local node's Member record.
func (m *Membership) Self() Member {
	return m.self
}

// Add inserts or updates a member in the registry.
func (m *Membership) Add(member Member) {
	m.mu.Lock()
	m.members[member.NodeID] = member
	m.mu.Unlock()
}

// Remove deletes the member with the given NodeID from the registry.
// Removing the self node is a no-op.
func (m *Membership) Remove(nodeID string) {
	if nodeID == m.self.NodeID {
		return
	}
	m.mu.Lock()
	delete(m.members, nodeID)
	m.mu.Unlock()
}

// Get returns the member with the given NodeID and a boolean indicating
// whether it was found.
func (m *Membership) Get(nodeID string) (Member, bool) {
	m.mu.RLock()
	mem, ok := m.members[nodeID]
	m.mu.RUnlock()
	return mem, ok
}

// All returns a snapshot of all known members, sorted by NodeID.
func (m *Membership) All() []Member {
	m.mu.RLock()
	out := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, mem)
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

// Len returns the current number of known members (including self).
func (m *Membership) Len() int {
	m.mu.RLock()
	n := len(m.members)
	m.mu.RUnlock()
	return n
}
