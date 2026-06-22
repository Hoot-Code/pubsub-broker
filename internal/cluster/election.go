// KNOWN LIMITATION: This implementation uses the Bully algorithm for
// leader election. The Bully algorithm does not guarantee safety under
// network partitions (split-brain scenarios). Two nodes may
// simultaneously believe they are leader if the network partitions
// between them. For production deployments requiring strict
// linearizability, replace this with a Raft-based consensus algorithm.
// See: https://raft.github.io/
package cluster

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
)

// ─── Role ─────────────────────────────────────────────────────────────────────

// Role represents the current election role of a node.
type Role uint8

const (
	// RoleFollower is the default role; the node defers to the current leader.
	RoleFollower Role = 0
	// RoleCandidate means the node has started an election and is soliciting votes.
	RoleCandidate Role = 1
	// RoleLeader means the node has won the election and sends heartbeats.
	RoleLeader Role = 2
)

// ─── msgEffect ────────────────────────────────────────────────────────────────

// msgEffect is a deferred send action produced by the election state machine.
type msgEffect struct {
	addr string
	msg  *ClusterMsg
}

// ─── Election ─────────────────────────────────────────────────────────────────

// Election implements the Bully leader-election algorithm.
// The node with the highest NodeID (lexicographic) that is reachable wins.
//
// Usage:
//
//	e := NewElection(self, membership, transport)
//	e.Start(ctx)
//	// Route incoming cluster messages: e.OnMessage(msg)
type Election struct {
	// Immutable after construction.
	self Member

	// Configurable timeouts (milliseconds); set before Start.
	heartbeatIntervalMs  int
	electionTimeoutMinMs int
	electionTimeoutMaxMs int

	// Mutable election state, protected by mu.
	mu           sync.RWMutex
	role         Role
	term         uint64
	leader       *Member
	grantsNeeded map[string]struct{} // NodeIDs (lower than self) we need grants from
	grantsHad    map[string]struct{} // NodeIDs that have granted in current term

	// Dependencies.
	members     *Membership
	transportMu sync.RWMutex
	transport   Transporter // may be nil (single-node or test-injected)
	metricsMu   sync.RWMutex
	metrics     *metrics.Broker // may be nil

	// Channels.
	inCh   chan *ClusterMsg
	stopCh chan struct{}

	stopOnce sync.Once

	// onBecomeLeader is called (if set) each time this node wins an election.
	leaderCbMu     sync.Mutex
	onBecomeLeader func()
}

// NewElection creates an Election for self, backed by the given membership
// registry and transport. transport may be nil for single-node operation;
// use SetTransport to inject a test double.
func NewElection(self Member, members *Membership, transport *Transport) *Election {
	var tr Transporter
	if transport != nil {
		tr = transport
	}
	return &Election{
		self:                 self,
		heartbeatIntervalMs:  150,
		electionTimeoutMinMs: 450,
		electionTimeoutMaxMs: 750,
		members:              members,
		transport:            tr,
		inCh:                 make(chan *ClusterMsg, 256),
		stopCh:               make(chan struct{}),
	}
}

// SetTransport replaces the transport used for sending messages.
// It is safe to call before Start; intended for test injection.
func (e *Election) SetTransport(t Transporter) {
	e.transportMu.Lock()
	e.transport = t
	e.transportMu.Unlock()
}

// SetMetrics attaches optional broker-level metrics to the election.
func (e *Election) SetMetrics(m *metrics.Broker) {
	e.metricsMu.Lock()
	e.metrics = m
	e.metricsMu.Unlock()
}

// SetLeaderCallback registers a function called each time this node wins an
// election. Only one callback is stored; subsequent calls replace the previous.
func (e *Election) SetLeaderCallback(fn func()) {
	e.leaderCbMu.Lock()
	e.onBecomeLeader = fn
	e.leaderCbMu.Unlock()
}

// ForceBecomeLeader transitions this node directly to Leader state without
// waiting for the election timer. Intended for single-node clusters where
// the node is the only member and should immediately assume leadership at
// startup. Emits the same metrics and leader callback as a normal election.
func (e *Election) ForceBecomeLeader() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.role == RoleLeader {
		return
	}
	e.term++
	e.emitElectionCountLocked()
	_ = e.becomeLeaderLocked()
}

// Start launches the election loop in a background goroutine.
// The loop exits when ctx is cancelled or Stop is called.
func (e *Election) Start(ctx context.Context) {
	go e.run(ctx)
}

// Stop shuts down the election loop. Safe to call more than once.
func (e *Election) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
}

// Role returns the node's current election role.
func (e *Election) Role() Role {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.role
}

// Leader returns the currently known leader and true, or a zero Member and
// false if no leader has been established yet.
func (e *Election) Leader() (Member, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.leader == nil {
		return Member{}, false
	}
	return *e.leader, true
}

// Term returns the current election term.
func (e *Election) Term() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.term
}

// OnMessage delivers an incoming cluster message to the election state machine.
// It must be called from the Transport.Recv() dispatch loop (or from tests).
func (e *Election) OnMessage(msg *ClusterMsg) {
	select {
	case e.inCh <- msg:
	case <-e.stopCh:
	}
}

// ─── Election loop ────────────────────────────────────────────────────────────

func (e *Election) run(ctx context.Context) {
	hbInterval := time.Duration(e.heartbeatIntervalMs) * time.Millisecond
	hbTicker := time.NewTicker(hbInterval)
	defer hbTicker.Stop()

	electionTimer := time.NewTimer(e.randomTimeout())
	defer electionTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return

		case <-electionTimer.C:
			effects := e.handleElectionTimeout()
			e.applyEffects(effects)
			drainTimer(electionTimer)
			electionTimer.Reset(e.randomTimeout())

		case msg := <-e.inCh:
			reset, effects := e.handleMsg(msg)
			e.applyEffects(effects)
			if reset {
				drainTimer(electionTimer)
				electionTimer.Reset(e.randomTimeout())
			}

		case <-hbTicker.C:
			effects := e.maybeSendHeartbeats()
			e.applyEffects(effects)
		}
	}
}

// ─── State machine ────────────────────────────────────────────────────────────

// handleElectionTimeout fires when the election timer expires.
// Returns a list of outbound messages to send after the lock is released.
func (e *Election) handleElectionTimeout() []msgEffect {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.term++
	e.role = RoleCandidate

	// Compute which lower-ID nodes we need grants from.
	e.grantsNeeded = make(map[string]struct{})
	e.grantsHad = make(map[string]struct{})
	for _, m := range e.members.All() {
		if m.NodeID < e.self.NodeID {
			e.grantsNeeded[m.NodeID] = struct{}{}
		}
	}

	e.emitElectionCountLocked()

	// If there are no lower-ID nodes, we win immediately.
	if len(e.grantsNeeded) == 0 {
		return e.becomeLeaderLocked()
	}

	// Broadcast VoteRequest to all other members.
	term := e.term
	from := e.self.NodeID
	var effects []msgEffect
	for _, m := range e.members.All() {
		if m.NodeID == e.self.NodeID {
			continue
		}
		effects = append(effects, msgEffect{
			addr: m.Addr,
			msg:  &ClusterMsg{Type: MsgVoteRequest, From: from, Term: term},
		})
	}
	return effects
}

// handleMsg processes one incoming message and returns (resetTimer, effects).
func (e *Election) handleMsg(msg *ClusterMsg) (resetTimer bool, effects []msgEffect) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Always advance term if the message carries a newer one.
	if msg.Term > e.term {
		e.term = msg.Term
	}

	switch msg.Type {
	case MsgHeartbeat:
		// Accept the sender as leader only if their NodeID is higher than ours.
		// This ensures the highest-NodeID node always wins long-term.
		if msg.From > e.self.NodeID {
			e.setLeaderLocked(msg.From, msg.Term)
			return true, nil // reset election timer
		}
		// Lower-ID node claims leadership — do not reset timer; we'll eventually
		// start an election and out-rank them.
		return false, nil

	case MsgVoteRequest:
		if msg.From > e.self.NodeID && msg.Term >= e.term {
			// Higher-ID candidate: grant and concede any own candidacy.
			e.term = msg.Term
			e.role = RoleFollower
			e.grantsNeeded = nil
			e.grantsHad = nil

			addr := e.peerAddr(msg.From)
			if addr != "" {
				effects = append(effects, msgEffect{
					addr: addr,
					msg:  &ClusterMsg{Type: MsgVoteGrant, From: e.self.NodeID, Term: msg.Term},
				})
			}
			return true, effects // reset timer
		}
		// Lower-ID candidate: deny.
		addr := e.peerAddr(msg.From)
		if addr != "" {
			effects = append(effects, msgEffect{
				addr: addr,
				msg:  &ClusterMsg{Type: MsgVoteDeny, From: e.self.NodeID, Term: e.term},
			})
		}
		return false, effects

	case MsgVoteGrant:
		if e.role == RoleCandidate && msg.Term == e.term {
			e.grantsHad[msg.From] = struct{}{}
			// Check whether all needed grants have arrived.
			allGranted := true
			for nodeID := range e.grantsNeeded {
				if _, ok := e.grantsHad[nodeID]; !ok {
					allGranted = false
					break
				}
			}
			if allGranted {
				return false, e.becomeLeaderLocked()
			}
		}

	case MsgVoteDeny:
		// Nothing to do; stay candidate and wait for the next timeout.
	}

	return false, nil
}

// maybeSendHeartbeats returns heartbeat effects when this node is the leader.
func (e *Election) maybeSendHeartbeats() []msgEffect {
	e.mu.RLock()
	isLeader := e.role == RoleLeader
	term := e.term
	from := e.self.NodeID
	e.mu.RUnlock()

	if !isLeader {
		return nil
	}

	var effects []msgEffect
	for _, m := range e.members.All() {
		if m.NodeID == e.self.NodeID {
			continue
		}
		effects = append(effects, msgEffect{
			addr: m.Addr,
			msg:  &ClusterMsg{Type: MsgHeartbeat, From: from, Term: term},
		})
	}
	return effects
}

// ─── Locked helpers (call with e.mu held) ────────────────────────────────────

// becomeLeaderLocked transitions this node to leader and returns broadcast effects.
// Caller must hold e.mu.
func (e *Election) becomeLeaderLocked() []msgEffect {
	old := e.leader
	e.role = RoleLeader
	self := e.self
	e.leader = &self
	e.grantsNeeded = nil
	e.grantsHad = nil

	bm := e.getMetrics()
	if bm != nil {
		if old == nil || old.NodeID != e.self.NodeID {
			bm.LeaderChanges.Inc(1)
		}
		bm.IsLeader.Set(1.0)
		bm.CurrentTerm.Set(float64(e.term))
	}

	// Notify the leader callback (without holding mu to avoid deadlock).
	go e.fireLeaderCallback()

	// Broadcast heartbeat to all other members.
	term := e.term
	from := e.self.NodeID
	var effects []msgEffect
	for _, m := range e.members.All() {
		if m.NodeID == e.self.NodeID {
			continue
		}
		effects = append(effects, msgEffect{
			addr: m.Addr,
			msg:  &ClusterMsg{Type: MsgHeartbeat, From: from, Term: term},
		})
	}
	return effects
}

// setLeaderLocked updates the known leader and transitions this node to follower.
// Caller must hold e.mu.
func (e *Election) setLeaderLocked(nodeID string, term uint64) {
	e.term = term
	prev := e.role
	e.role = RoleFollower
	e.grantsNeeded = nil
	e.grantsHad = nil

	m, ok := e.members.Get(nodeID)
	if !ok {
		return
	}
	old := e.leader
	e.leader = &m

	bm := e.getMetrics()
	if bm != nil {
		if prev == RoleLeader || old == nil || old.NodeID != nodeID {
			bm.LeaderChanges.Inc(1)
		}
		bm.IsLeader.Set(0)
		bm.CurrentTerm.Set(float64(term))
	}
}

// emitElectionCountLocked increments the election counter if metrics are set.
// Caller must hold e.mu.
func (e *Election) emitElectionCountLocked() {
	bm := e.getMetrics()
	if bm != nil {
		bm.ElectionCount.Inc(1)
		bm.CurrentTerm.Set(float64(e.term))
	}
}

// peerAddr looks up the network address for the peer with the given NodeID.
// Returns "" if the peer is not in the membership registry.
// Caller must hold e.mu (reads membership under its own lock, so safe).
func (e *Election) peerAddr(nodeID string) string {
	m, ok := e.members.Get(nodeID)
	if !ok {
		return ""
	}
	return m.Addr
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (e *Election) applyEffects(effects []msgEffect) {
	tr := e.getTransport()
	if tr == nil {
		return
	}
	for _, eff := range effects {
		_ = tr.Send(eff.addr, eff.msg)
	}
}

func (e *Election) getTransport() Transporter {
	e.transportMu.RLock()
	defer e.transportMu.RUnlock()
	return e.transport
}

func (e *Election) getMetrics() *metrics.Broker {
	e.metricsMu.RLock()
	defer e.metricsMu.RUnlock()
	return e.metrics
}

func (e *Election) fireLeaderCallback() {
	e.leaderCbMu.Lock()
	fn := e.onBecomeLeader
	e.leaderCbMu.Unlock()
	if fn != nil {
		fn()
	}
}

func (e *Election) randomTimeout() time.Duration {
	lo := e.electionTimeoutMinMs
	hi := e.electionTimeoutMaxMs
	if hi <= lo {
		return time.Duration(lo) * time.Millisecond
	}
	ms := lo + rand.Intn(hi-lo)
	return time.Duration(ms) * time.Millisecond
}

func drainTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
