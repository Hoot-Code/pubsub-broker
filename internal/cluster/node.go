package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/cluster/raft"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
)

// Logger is the logging interface expected by the cluster package.
// *logging.Logger satisfies this interface.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// ─── TopicInfo ────────────────────────────────────────────────────────────────

// TopicInfo carries the minimal topic metadata needed by the leader to build a
// partition assignment.  Brokers supply this via SetTopicProvider.
type TopicInfo struct {
	// Name is the topic name.
	Name string
	// Partitions is the number of partitions the topic was created with.
	Partitions int
}

// ─── metaSyncPayload ─────────────────────────────────────────────────────────

// metaSyncKind distinguishes the two varieties of MsgMetaSync body.
type metaSyncKind string

const (
	metaSyncMember     metaSyncKind = "member"
	metaSyncPartitions metaSyncKind = "partitions"
)

// metaSyncPayload is the JSON wrapper placed in ClusterMsg.Body for MsgMetaSync.
type metaSyncPayload struct {
	Kind metaSyncKind    `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// ─── Node ─────────────────────────────────────────────────────────────────────

// Node is the top-level cluster coordinator.
// It wires together Membership, Transport, Election (or Raft), and the
// Replicator and implements the join / leave handshake protocol.
// The consensus algorithm is selected by ClusterConfig.ConsensusAlgorithm:
// "bully" (default) uses the existing Bully election; "raft" uses the Raft
// implementation for split-brain safety.
type Node struct {
	cfg     config.ClusterConfig
	metrics *metrics.Broker
	log     Logger

	self       Member
	membership *Membership
	transport  *Transport
	election   *Election
	replicator *Replicator

	// raftNode is non-nil when ConsensusAlgorithm == "raft".
	raftNode *raftNode

	partMu  sync.RWMutex
	partMap *PartitionMap

	// topicProvider is set by the broker so the leader can discover all topics
	// when building a partition assignment. Protected by topicProviderMu.
	topicProviderMu sync.Mutex
	topicProvider   func() []TopicInfo

	// partChangeCb is called (if set) after the local partition map is updated
	// via a MsgMetaSync from the leader.
	partChangeMu sync.Mutex
	partChangeCb func(newPM *PartitionMap)

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// raftNode wraps a raft.Node to provide the same IsLeader/Leader/Propose
// interface that the cluster.Node exposes, regardless of which consensus
// algorithm is active underneath.
type raftNode struct {
	node *raft.Node
}

// IsLeader returns true if this node is the Raft leader.
func (rn *raftNode) IsLeader() bool { return rn.node.IsLeader() }

// Leader returns the leader's NodeID if known.
func (rn *raftNode) Leader() (string, bool) { return rn.node.LeaderID() }

// raftTransport adapts a cluster.Transport to the raft.Transporter interface,
// converting between cluster.ClusterMsg and raft.RaftMessage to avoid import
// cycles.
type raftTransport struct {
	clusterTransport *Transport
}

// SendRaft adapts a raft.RaftMessage to a cluster.ClusterMsg and sends it
// via the underlying cluster transport.
func (rt *raftTransport) SendRaft(addr string, msg *raft.RaftMessage) error {
	cmsg := &ClusterMsg{
		Type: MsgType(msg.Type),
		From: msg.From,
		Term: msg.Term,
		Body: msg.Body,
	}
	return rt.clusterTransport.Send(addr, cmsg)
}

// NewNode creates a fully wired cluster Node from the given configuration.
// Start must be called separately to begin accepting connections and join seeds.
func NewNode(cfg config.ClusterConfig, bm *metrics.Broker, log Logger) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("cluster: node_id is required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("cluster: bind_addr is required")
	}

	self := Member{
		NodeID:   cfg.NodeID,
		Addr:     cfg.BindAddr,
		JoinedAt: time.Now(),
	}

	membership := NewMembership(self)

	transport, err := NewTransportWithConfig(TransportConfig{
		BindAddr:     cfg.BindAddr,
		MTLSCertFile: cfg.MTLSCertFile,
		MTLSKeyFile:  cfg.MTLSKeyFile,
		MTLSCAFile:   cfg.MTLSCAFile,
	})
	if err != nil {
		return nil, fmt.Errorf("cluster: new transport: %w", err)
	}

	// Use the resolved address from the listener (important when BindAddr was ":0").
	self.Addr = transport.Addr()
	// Backfill the resolved addr into membership (self was registered with ":0").
	membership.mu.Lock()
	membership.self = self
	membership.members[self.NodeID] = self
	membership.mu.Unlock()

	hbMs := cfg.HeartbeatInterval
	if hbMs <= 0 {
		hbMs = 150
	}
	etMin := cfg.ElectionTimeoutMin
	if etMin <= 0 {
		etMin = 450
	}
	etMax := cfg.ElectionTimeoutMax
	if etMax <= 0 {
		etMax = 750
	}

	election := NewElection(self, membership, transport)
	election.heartbeatIntervalMs = hbMs
	election.electionTimeoutMinMs = etMin
	election.electionTimeoutMaxMs = etMax
	if bm != nil {
		election.SetMetrics(bm)
	}

	replicator := NewReplicator(self, membership, transport, log)

	n := &Node{
		cfg:        cfg,
		metrics:    bm,
		log:        log,
		self:       self,
		membership: membership,
		transport:  transport,
		election:   election,
		replicator: replicator,
		partMap:    &PartitionMap{},
		stopCh:     make(chan struct{}),
	}

	// When this node becomes leader, re-assign partitions and broadcast.
	election.SetLeaderCallback(n.onBecomeLeader)

	// If Raft consensus is configured, create the Raft node.
	if cfg.ConsensusAlgorithm == "raft" {
		raftDataDir := cfg.RaftDataDir
		if raftDataDir == "" {
			raftDataDir = "data/raft"
		}
		storePath := raftDataDir + "/" + cfg.NodeID + "-state.json"
		store := raft.NewFilePersistentStore(storePath)

		var peerIDs []string
		for _, m := range membership.All() {
			if m.NodeID != self.NodeID {
				peerIDs = append(peerIDs, m.NodeID)
			}
		}
		sort.Strings(peerIDs)

		applyFn := func(cmd []byte) error {
			n.onBecomeLeader()
			return nil
		}

		rn := raft.NewNode(self.NodeID, peerIDs, &raftTransport{clusterTransport: transport}, store, applyFn, nil)
		n.raftNode = &raftNode{node: rn}
	}

	return n, nil
}

// Start begins cluster operations:
//  1. Starts the message dispatch loop.
//  2. Starts the replicator.
//  3. Sends MsgJoin to each configured seed.
//  4. Starts the leader election.
func (n *Node) Start(ctx context.Context) error {
	n.wg.Add(1)
	go n.dispatchLoop(ctx)

	// Start replication loop.
	n.replicator.Start(ctx, n.election)

	// Send MsgJoin to every seed node.
	selfBody, err := json.Marshal(n.self)
	if err != nil {
		return fmt.Errorf("cluster: marshal self for join: %w", err)
	}
	joinMsg := &ClusterMsg{
		Type: MsgJoin,
		From: n.self.NodeID,
		Body: selfBody,
	}
	for _, seed := range n.cfg.Seeds {
		if seed == n.self.Addr {
			continue // do not join ourselves
		}
		if err := n.transport.Send(seed, joinMsg); err != nil {
			n.log.Warn("cluster: join seed failed", "seed", seed, "err", err)
		} else {
			n.log.Info("cluster: sent join", "seed", seed)
		}
	}

	n.election.Start(ctx)

	// Fast path: single-node cluster elects itself leader immediately at
	// startup without waiting for the election timer (450-750 ms default).
	// This ensures the node is reachable as Leader within milliseconds.
	if n.membership.Len() <= 1 {
		n.election.ForceBecomeLeader()
	}

	// If Raft consensus is configured, start the Raft node in parallel.
	if n.cfg.ConsensusAlgorithm == "raft" && n.raftNode != nil {
		n.raftNode.node.Start(ctx)
	}

	return nil
}

// Stop gracefully shuts down the node, broadcasting MsgLeave to all peers.
func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stopCh) })
	if n.raftNode != nil {
		n.raftNode.node.Stop()
	} else {
		n.election.Stop()
	}

	// Broadcast MsgLeave.
	leaveMsg := &ClusterMsg{Type: MsgLeave, From: n.self.NodeID}
	for _, m := range n.membership.All() {
		if m.NodeID == n.self.NodeID {
			continue
		}
		_ = n.transport.Send(m.Addr, leaveMsg)
	}

	n.wg.Wait()
	return n.transport.Close()
}

// IsLeader reports whether this node is currently the cluster leader.
// Works identically regardless of whether Bully or Raft consensus is active.
func (n *Node) IsLeader() bool {
	if n.raftNode != nil {
		return n.raftNode.IsLeader()
	}
	return n.election.Role() == RoleLeader
}

// Leader returns the current cluster leader and true, or zero/false if unknown.
// Works identically regardless of whether Bully or Raft consensus is active.
func (n *Node) Leader() (Member, bool) {
	if n.raftNode != nil {
		id, ok := n.raftNode.Leader()
		if !ok {
			return Member{}, false
		}
		m, ok := n.membership.Get(id)
		return m, ok
	}
	return n.election.Leader()
}

// Members returns a snapshot of all known cluster members sorted by NodeID.
func (n *Node) Members() []Member {
	return n.membership.All()
}

// SelfID returns this node's NodeID.
func (n *Node) SelfID() string {
	return n.self.NodeID
}

// OwnerOf returns the NodeID that owns the given topic partition according to
// the current partition map.  Returns "" if not yet assigned.
func (n *Node) OwnerOf(topic string, partition int32) string {
	n.partMu.RLock()
	defer n.partMu.RUnlock()
	return n.partMap.Owner(topic, partition)
}

// OwnsPartition reports whether this node owns the given topic partition.
// Returns true when the partition is unassigned (owner == "") or assigned to
// this node.
func (n *Node) OwnsPartition(topic string, partition int32) bool {
	owner := n.OwnerOf(topic, partition)
	return owner == "" || owner == n.self.NodeID
}

// AssignPartitions assigns partitions for topic across all current nodes and
// broadcasts the resulting PartitionMap to all followers.
func (n *Node) AssignPartitions(topic string, partitions int) {
	members := n.membership.All()
	nodeIDs := make([]string, len(members))
	for i, m := range members {
		nodeIDs[i] = m.NodeID
	}
	sort.Strings(nodeIDs)

	n.partMu.Lock()
	n.partMap.Assign(topic, partitions, nodeIDs)
	pm := n.partMap
	n.partMu.Unlock()

	n.broadcastPartitionMap(pm)
}

// PartitionMap returns a snapshot of the current partition map.
func (n *Node) PartitionMap() *PartitionMap {
	n.partMu.RLock()
	defer n.partMu.RUnlock()
	return n.partMap
}

// SetTopicProvider registers a function that the leader calls to discover all
// existing topics when building or refreshing the partition assignment.
// fn must be safe for concurrent calls.
func (n *Node) SetTopicProvider(fn func() []TopicInfo) {
	n.topicProviderMu.Lock()
	n.topicProvider = fn
	n.topicProviderMu.Unlock()
}

// SetPartitionChangeCallback registers a function that is called after the
// local partition map is updated from a MsgMetaSync message sent by the leader.
// The callback receives the new PartitionMap.  fn must be safe for concurrent
// calls.
func (n *Node) SetPartitionChangeCallback(fn func(newPM *PartitionMap)) {
	n.partChangeMu.Lock()
	n.partChangeCb = fn
	n.partChangeMu.Unlock()
}

// RegisterPartition wires a PartitionLog into the replicator so that the
// replicator can push data to followers (leader mode) or receive data from the
// leader (follower mode).
func (n *Node) RegisterPartition(topic string,
	partition int32, pl *storage.PartitionLog) {
	n.replicator.RegisterPartition(topic, partition, pl)
}

// ReassignAll re-runs the partition assignment for every topic returned by the
// topic provider, then broadcasts the updated PartitionMap to all followers.
// It is idempotent: calling it multiple times with the same membership and
// topics produces the same result because PartitionMap.Assign uses sorted
// round-robin.
func (n *Node) ReassignAll() {
	n.topicProviderMu.Lock()
	fn := n.topicProvider
	n.topicProviderMu.Unlock()
	if fn == nil {
		return
	}
	topics := fn()

	members := n.membership.All()
	nodeIDs := make([]string, 0, len(members))
	for _, m := range members {
		nodeIDs = append(nodeIDs, m.NodeID)
	}
	sort.Strings(nodeIDs)

	n.partMu.Lock()
	for _, t := range topics {
		n.partMap.Assign(t.Name, t.Partitions, nodeIDs)
	}
	pm := n.partMap
	n.partMu.Unlock()

	n.broadcastPartitionMap(pm)
}

// ─── Dispatch loop ────────────────────────────────────────────────────────────

func (n *Node) dispatchLoop(ctx context.Context) {
	defer n.wg.Done()
	ch := n.transport.Recv()
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			n.dispatch(msg)
		case <-ctx.Done():
			return
		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) dispatch(msg *ClusterMsg) {
	switch msg.Type {
	case MsgHeartbeat, MsgVoteRequest, MsgVoteGrant, MsgVoteDeny:
		if n.raftNode == nil {
			n.election.OnMessage(msg)
		}
	case MsgRaftRequestVote, MsgRaftVoteResponse, MsgRaftAppendEntries, MsgRaftAppendResponse:
		if n.raftNode != nil {
			n.raftNode.node.OnMessage(&raft.RaftMessage{
				Type: uint8(msg.Type),
				From: msg.From,
				Term: msg.Term,
				Body: msg.Body,
			})
		}
	case MsgJoin:
		n.handleJoin(msg)
	case MsgJoinAck:
		n.handleJoinAck(msg)
	case MsgLeave:
		n.handleLeave(msg)
	case MsgMetaSync:
		n.handleMetaSync(msg)
	case MsgReplicaFetch, MsgReplicaData, MsgReplicaAck:
		if err := n.replicator.OnMessage(msg); err != nil {
			n.log.Warn("cluster: replicator message error",
				"type", msg.Type, "from", msg.From, "err", err)
		}
	default:
		n.log.Warn("cluster: unknown message type", "type", msg.Type, "from", msg.From)
	}
}

// ─── Cluster message handlers ────────────────────────────────────────────────

func (n *Node) handleJoin(msg *ClusterMsg) {
	var joiner Member
	if err := json.Unmarshal(msg.Body, &joiner); err != nil {
		n.log.Error("cluster: handleJoin unmarshal", "err", err)
		return
	}

	n.membership.Add(joiner)
	n.log.Info("cluster: node joined", "node_id", joiner.NodeID, "addr", joiner.Addr)

	// Respond with the full current member list.
	members := n.membership.All()
	ackBody, err := json.Marshal(members)
	if err != nil {
		n.log.Error("cluster: handleJoin marshal ack", "err", err)
		return
	}
	ackMsg := &ClusterMsg{
		Type: MsgJoinAck,
		From: n.self.NodeID,
		Body: ackBody,
	}
	if err := n.transport.Send(joiner.Addr, ackMsg); err != nil {
		n.log.Warn("cluster: send JoinAck failed", "to", joiner.Addr, "err", err)
	}

	// Broadcast MsgMetaSync(member) to all existing members except self and joiner.
	joinerData, err := json.Marshal(joiner)
	if err != nil {
		n.log.Error("cluster: handleJoin marshal joiner for sync", "err", err)
		return
	}
	payload := metaSyncPayload{Kind: metaSyncMember, Data: joinerData}
	syncBody, err := json.Marshal(payload)
	if err != nil {
		n.log.Error("cluster: handleJoin marshal metasync", "err", err)
		return
	}
	syncMsg := &ClusterMsg{Type: MsgMetaSync, From: n.self.NodeID, Body: syncBody}

	for _, m := range members {
		if m.NodeID == n.self.NodeID || m.NodeID == joiner.NodeID {
			continue
		}
		if err := n.transport.Send(m.Addr, syncMsg); err != nil {
			n.log.Warn("cluster: broadcast MetaSync failed", "to", m.Addr, "err", err)
		}
	}

	// Part E1: if this node is the leader, rebalance partitions across the
	// enlarged membership.
	if n.IsLeader() {
		n.ReassignAll()
	}
}

func (n *Node) handleJoinAck(msg *ClusterMsg) {
	var members []Member
	if err := json.Unmarshal(msg.Body, &members); err != nil {
		n.log.Error("cluster: handleJoinAck unmarshal", "err", err)
		return
	}
	for _, m := range members {
		n.membership.Add(m)
	}
	n.log.Info("cluster: received JoinAck", "total_members", n.membership.Len())
}

func (n *Node) handleLeave(msg *ClusterMsg) {
	n.membership.Remove(msg.From)
	n.log.Info("cluster: node left", "node_id", msg.From)

	// Part E2: if this node is the leader, rebalance partitions across the
	// reduced membership.
	if n.IsLeader() {
		n.ReassignAll()
	}
}

func (n *Node) handleMetaSync(msg *ClusterMsg) {
	var payload metaSyncPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		n.log.Error("cluster: handleMetaSync unmarshal", "err", err)
		return
	}

	switch payload.Kind {
	case metaSyncMember:
		var m Member
		if err := json.Unmarshal(payload.Data, &m); err != nil {
			n.log.Error("cluster: handleMetaSync member unmarshal", "err", err)
			return
		}
		n.membership.Add(m)
		n.log.Info("cluster: meta sync added member", "node_id", m.NodeID)

	case metaSyncPartitions:
		var pm PartitionMap
		if err := json.Unmarshal(payload.Data, &pm); err != nil {
			n.log.Error("cluster: handleMetaSync partition map unmarshal", "err", err)
			return
		}
		n.partMu.Lock()
		n.partMap = &pm
		n.partMu.Unlock()
		n.log.Info("cluster: partition map updated from leader")

		// Notify the broker of the new partition map (Part E4).
		n.partChangeMu.Lock()
		cb := n.partChangeCb
		n.partChangeMu.Unlock()
		if cb != nil {
			cb(&pm)
		}

	default:
		n.log.Warn("cluster: unknown MetaSync kind", "kind", payload.Kind)
	}
}

// ─── Leader callbacks ─────────────────────────────────────────────────────────

// onBecomeLeader is invoked by the election when this node wins an election.
// It uses the topic provider to assign all partitions across current members
// and broadcasts the resulting PartitionMap.
func (n *Node) onBecomeLeader() {
	n.log.Info("cluster: became leader, reassigning partitions")
	n.ReassignAll()
}

func (n *Node) broadcastPartitionMap(pm *PartitionMap) {
	pmData, err := json.Marshal(pm)
	if err != nil {
		n.log.Error("cluster: broadcastPartitionMap marshal", "err", err)
		return
	}
	payload := metaSyncPayload{Kind: metaSyncPartitions, Data: pmData}
	body, err := json.Marshal(payload)
	if err != nil {
		n.log.Error("cluster: broadcastPartitionMap marshal payload", "err", err)
		return
	}
	syncMsg := &ClusterMsg{Type: MsgMetaSync, From: n.self.NodeID, Body: body}

	for _, m := range n.membership.All() {
		if m.NodeID == n.self.NodeID {
			continue
		}
		if err := n.transport.Send(m.Addr, syncMsg); err != nil {
			n.log.Warn("cluster: broadcastPartitionMap send failed", "to", m.Addr, "err", err)
		}
	}
}

// RaftSnapshot returns a point-in-time snapshot of the Raft node's internal
// state when Raft consensus is active. Returns (zero value, false) when Raft
// is not the active consensus algorithm or the cluster is disabled.
func (n *Node) RaftSnapshot() (raft.NodeSnapshot, bool) {
	if n.raftNode == nil {
		return raft.NodeSnapshot{}, false
	}
	return n.raftNode.node.Snapshot(), true
}
