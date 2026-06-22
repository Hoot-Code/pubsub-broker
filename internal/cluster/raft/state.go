// Package raft implements the Raft consensus algorithm as defined in
// "In Search of an Understandable Consensus Algorithm" (Ongaro &
// Ousterhout). It provides leader election and log replication for the
// cluster module, replacing the Bully algorithm behind a config flag.
package raft

// Role represents the current role of a Raft node.
type Role uint8

const (
	// Follower is the default role; the node defers to the current leader.
	Follower Role = 0
	// Candidate means the node has started an election and is soliciting votes.
	Candidate Role = 1
	// Leader means the node has won the election and replicates its log to followers.
	Leader Role = 2
)

// LogEntry is a single entry in the Raft replicated log.
type LogEntry struct {
	// Term is the term when the entry was received by the leader.
	Term uint64
	// Index is the log index (1-based; index 0 means "no entries").
	Index uint64
	// Command is an opaque application command (typically JSON-encoded).
	Command []byte
}

// PersistentState holds the Raft state that must survive process restarts.
// This is required for Raft safety — a node that forgets its vote can
// violate the election safety property.
type PersistentState struct {
	// CurrentTerm is the latest term this node has seen. It is incremented
	// when the node transitions to Candidate, and whenever it discovers a
	// higher term.
	CurrentTerm uint64
	// VotedFor is the NodeID of the candidate this node voted for in the
	// current term. "" means no vote has been cast.
	VotedFor string
	// Log is the replicated log of commands, indexed from 1.
	Log []LogEntry
}

// PersistentStore persists PersistentState to stable storage.
// It must survive process restarts — this is required for Raft safety.
type PersistentStore interface {
	// Save atomically persists the given state to stable storage.
	Save(state PersistentState) error
	// Load returns the last successfully saved state. If no state has been
	// saved yet, it returns a zero-value PersistentState (term 0, no vote,
	// empty log).
	Load() (PersistentState, error)
}
