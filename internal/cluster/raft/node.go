package raft

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"
)

// Logger is the minimal logging interface the Raft node needs.
type Logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// noopLogger discards all log output.
type noopLogger struct{}

func (noopLogger) Info(string, ...any)  {}
func (noopLogger) Warn(string, ...any)  {}
func (noopLogger) Error(string, ...any) {}

// ErrNotLeader is returned by Propose when this node is not the leader.
var ErrNotLeader = errors.New("raft: not leader")

// ErrStopped is returned by Propose when the node has been stopped.
var ErrStopped = errors.New("raft: node stopped")

// Node is a single Raft node that participates in leader election and log
// replication. It communicates via a Transporter using RaftMessage values.
type Node struct {
	selfID    string
	peers     []string
	transport Transporter
	store     PersistentStore
	apply     func(cmd []byte) error
	log       Logger

	mu    sync.Mutex
	state PersistentState
	role  Role

	commitIndex uint64
	lastApplied uint64
	leaderID    string
	leaderKnown bool

	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	voteGranted map[string]bool

	proposalMu sync.Mutex
	proposals  map[uint64]chan struct{}

	inCh   chan *RaftMessage
	stopCh chan struct{}

	stopOnce sync.Once

	peerAddrMap map[string]string

	electionTimeoutMin  int
	electionTimeoutMax  int
	heartbeatIntervalMs int
}

// NewNode creates a new Raft node. selfID is this node's unique identifier.
// peers is the list of other node IDs in the cluster. transport is used for
// inter-node communication. store persists the Raft state across restarts.
// apply is called when a log entry is committed and should be applied to the
// state machine. log may be nil for silent operation.
func NewNode(selfID string, peers []string, transport Transporter,
	store PersistentStore, apply func(cmd []byte) error, log Logger) *Node {
	if log == nil {
		log = noopLogger{}
	}
	return &Node{
		selfID:      selfID,
		peers:       peers,
		transport:   transport,
		store:       store,
		apply:       apply,
		log:         log,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		voteGranted: make(map[string]bool),
		proposals:   make(map[uint64]chan struct{}),
		inCh:        make(chan *RaftMessage, 1024),
		stopCh:      make(chan struct{}),
	}
}

// Start loads persistent state and begins the Raft event loop.
func (n *Node) Start(ctx context.Context) {
	state, err := n.store.Load()
	if err != nil {
		n.log.Error("raft: failed to load state", "err", err)
	} else {
		n.mu.Lock()
		n.state = state
		n.mu.Unlock()
	}
	n.log.Info("raft: node starting", "id", n.selfID, "term", n.state.CurrentTerm, "log_len", len(n.state.Log))

	// Fast path: single-node cluster becomes leader immediately at startup
	// without waiting for the election timer (150-300 ms default).
	if len(n.peers) == 0 {
		n.mu.Lock()
		n.state.CurrentTerm++
		n.state.VotedFor = n.selfID
		n.persistLocked()
		n.becomeLeaderLocked()
		n.mu.Unlock()
	}

	go n.run(ctx)
}

// Stop shuts down the Raft node.
func (n *Node) Stop() {
	n.stopOnce.Do(func() { close(n.stopCh) })
}

// Role returns the node's current role.
func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

// IsLeader reports whether this node is the current leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

// LeaderID returns the current leader's NodeID and whether a leader is known.
func (n *Node) LeaderID() (string, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID, n.leaderKnown
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.CurrentTerm
}

// CommitIndex returns the current commit index.
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

// Propose submits a new command to the log. Only valid on the leader;
// returns an error if this node is not the leader. Blocks until the entry
// is committed (replicated to a majority) or ctx is cancelled.
func (n *Node) Propose(ctx context.Context, cmd []byte) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	term := n.state.CurrentTerm

	entry := LogEntry{
		Term:    term,
		Index:   uint64(len(n.state.Log)) + 1,
		Command: cmd,
	}
	n.state.Log = append(n.state.Log, entry)
	n.persistLocked()
	index := entry.Index

	ch := make(chan struct{}, 1)
	n.proposals[index] = ch
	n.mu.Unlock()

	n.sendAppendEntriesToAll()

	// For single-node clusters, advance commit index immediately.
	n.mu.Lock()
	n.advanceCommitIndexLocked()
	n.mu.Unlock()

	select {
	case _, ok := <-ch:
		if !ok {
			return 0, ErrNotLeader
		}
		return index, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
}

// OnMessage processes one incoming RaftMessage. Must be wired into the
// same dispatch loop as the Bully election and replication messages.
func (n *Node) OnMessage(msg *RaftMessage) {
	select {
	case n.inCh <- msg:
	case <-n.stopCh:
	}
}

// ─── Event loop ─────────────────────────────────────────────────────────────

func (n *Node) run(ctx context.Context) {
	electionTimer := time.NewTimer(n.randomElectionTimeout())
	defer electionTimer.Stop()

	hbTicker := time.NewTicker(n.heartbeatInterval())
	defer hbTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		case <-electionTimer.C:
			n.handleElectionTimeout()
			drainTimer(electionTimer)
			electionTimer.Reset(n.randomElectionTimeout())
		case msg := <-n.inCh:
			n.mu.Lock()
			wasLeader := n.role == Leader
			n.mu.Unlock()
			reset := n.dispatchMsg(msg)
			if reset {
				drainTimer(electionTimer)
				electionTimer.Reset(n.randomElectionTimeout())
			}
			if wasLeader {
				n.mu.Lock()
				isLeader := n.role == Leader
				n.mu.Unlock()
				if !isLeader {
					n.proposalMu.Lock()
					for idx, ch := range n.proposals {
						close(ch)
						delete(n.proposals, idx)
					}
					n.proposalMu.Unlock()
				}
			}
		case <-hbTicker.C:
			if n.IsLeader() {
				n.sendAppendEntriesToAll()
			}
		}
	}
}

// ─── Message dispatch ───────────────────────────────────────────────────────

func (n *Node) dispatchMsg(msg *RaftMessage) bool {
	switch msg.Type {
	case MsgRaftRequestVote:
		return n.handleRequestVote(msg)
	case MsgRaftVoteResponse:
		return n.handleVoteResponse(msg)
	case MsgRaftAppendEntries:
		return n.handleAppendEntries(msg)
	case MsgRaftAppendResponse:
		return n.handleAppendResponse(msg)
	default:
		return false
	}
}

// ─── Internal helpers ───────────────────────────────────────────────────────

func (n *Node) persistLocked() {
	if err := n.store.Save(n.state); err != nil {
		n.log.Error("raft: persist state failed", "err", err)
	}
}

func (n *Node) peerAddr(peerID string) string {
	if n.peerAddrMap != nil {
		if addr, ok := n.peerAddrMap[peerID]; ok {
			return addr
		}
	}
	return peerID
}

func (n *Node) randomElectionTimeout() time.Duration {
	minMs := 150
	maxMs := 301
	if n.electionTimeoutMin > 0 {
		minMs = n.electionTimeoutMin
	}
	if n.electionTimeoutMax > 0 {
		maxMs = n.electionTimeoutMax
	}
	ms := minMs + rand.Intn(maxMs-minMs)
	return time.Duration(ms) * time.Millisecond
}

func (n *Node) heartbeatInterval() time.Duration {
	if n.heartbeatIntervalMs > 0 {
		return time.Duration(n.heartbeatIntervalMs) * time.Millisecond
	}
	return 50 * time.Millisecond
}

func drainTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

// SetPeerAddrs sets the mapping from peer NodeIDs to network addresses.
// Must be called before Start.
func (n *Node) SetPeerAddrs(addrs map[string]string) {
	n.peerAddrMap = addrs
}

// SetElectionTimeout overrides the default election timeout range (150-300ms).
// Must be called before Start. Intended for testing.
func (n *Node) SetElectionTimeout(minMs, maxMs int) {
	n.electionTimeoutMin = minMs
	n.electionTimeoutMax = maxMs
}

// SetHeartbeatInterval overrides the default heartbeat interval (50ms).
// Must be called before Start. Intended for testing.
func (n *Node) SetHeartbeatInterval(ms int) {
	n.heartbeatIntervalMs = ms
}

// NodeSnapshot is a read-only snapshot of Raft internal state, exposed for
// operational visibility without mutating or blocking the hot path.
type NodeSnapshot struct {
	// Role is the current Raft role as a string: "leader", "follower", or "candidate".
	Role string
	// Term is the current Raft term.
	Term uint64
	// CommitIndex is the highest log index known to be committed.
	CommitIndex uint64
	// LastApplied is the highest log index applied to the state machine.
	LastApplied uint64
	// LeaderID is the current leader's NodeID, or empty if unknown.
	LeaderID string
	// LogLength is the number of entries in the Raft log.
	LogLength int
	// MatchIndex holds the highest replicated index per peer.
	MatchIndex map[string]uint64
	// NextIndex holds the next index to send per peer.
	NextIndex map[string]uint64
}

// Snapshot returns a point-in-time snapshot of the Raft node's internal state.
// It acquires the same lock used for consistent reads but releases it quickly,
// so it does not block the hot path for long.
func (n *Node) Snapshot() NodeSnapshot {
	n.mu.Lock()
	defer n.mu.Unlock()

	var roleStr string
	switch n.role {
	case Leader:
		roleStr = "leader"
	case Follower:
		roleStr = "follower"
	case Candidate:
		roleStr = "candidate"
	default:
		roleStr = "unknown"
	}

	matchIndex := make(map[string]uint64, len(n.matchIndex))
	for k, v := range n.matchIndex {
		matchIndex[k] = v
	}
	nextIndex := make(map[string]uint64, len(n.nextIndex))
	for k, v := range n.nextIndex {
		nextIndex[k] = v
	}

	return NodeSnapshot{
		Role:        roleStr,
		Term:        n.state.CurrentTerm,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LeaderID:    n.leaderID,
		LogLength:   len(n.state.Log),
		MatchIndex:  matchIndex,
		NextIndex:   nextIndex,
	}
}
