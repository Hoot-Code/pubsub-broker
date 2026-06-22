package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/storage"
)

// partKey is the map key for a specific topic-partition pair.
type partKey struct {
	topic     string
	partition int32
}

// Replicator manages log replication between cluster nodes.
//
// When running as the leader it pushes newly appended entries to followers on a
// 50 ms tick.  When running as a follower it sends MsgReplicaFetch to the
// current leader and retries every 500 ms if no data arrives.
//
// All exported methods are safe for concurrent use.
type Replicator struct {
	self       Member
	membership *Membership
	transport  *Transport
	log        Logger

	// mu guards the partitions map.
	mu         sync.RWMutex
	partitions map[partKey]*storage.PartitionLog

	// followerMu guards followerOffsets (leader-side state).
	// followerOffsets[nodeID][key] is the next offset to send to that follower.
	followerMu      sync.Mutex
	followerOffsets map[string]map[partKey]int64

	// fetchMu guards lastFetch (follower-side state).
	fetchMu   sync.Mutex
	lastFetch map[partKey]time.Time

	// prevRole is used to detect transitions into the follower role so that
	// fetch requests are sent immediately on role change.
	prevRole Role
}

// NewReplicator creates a Replicator wired to the given transport and membership.
func NewReplicator(self Member, membership *Membership,
	transport *Transport, log Logger) *Replicator {
	return &Replicator{
		self:            self,
		membership:      membership,
		transport:       transport,
		log:             log,
		partitions:      make(map[partKey]*storage.PartitionLog),
		followerOffsets: make(map[string]map[partKey]int64),
		lastFetch:       make(map[partKey]time.Time),
		prevRole:        RoleFollower,
	}
}

// RegisterPartition registers a PartitionLog so the replicator knows which log
// to read from (as leader) or write to (as follower).
func (r *Replicator) RegisterPartition(topic string,
	partition int32, pl *storage.PartitionLog) {
	key := partKey{topic: topic, partition: partition}
	r.mu.Lock()
	r.partitions[key] = pl
	r.mu.Unlock()
}

// Start launches the replication loop in a background goroutine.
// When this node is the leader it pushes new entries to followers.
// When a follower, it requests entries from the leader.
func (r *Replicator) Start(ctx context.Context, election *Election) {
	go r.run(ctx, election)
}

func (r *Replicator) run(ctx context.Context, election *Election) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			role := election.Role()

			// Detect transition INTO follower role → send fetch requests immediately.
			if role == RoleFollower && r.prevRole != RoleFollower {
				if leader, ok := election.Leader(); ok {
					r.sendFetchAll(leader)
				}
			}
			r.prevRole = role

			switch role {
			case RoleLeader:
				r.leaderTick()
			case RoleFollower:
				r.followerTick(election)
			}
		}
	}
}

// leaderTick pushes log entries to followers that are behind.
func (r *Replicator) leaderTick() {
	r.mu.RLock()
	parts := make(map[partKey]*storage.PartitionLog, len(r.partitions))
	for k, v := range r.partitions {
		parts[k] = v
	}
	r.mu.RUnlock()

	members := r.membership.All()

	for key, pl := range parts {
		leaderOffset := pl.NextOffset()

		for _, m := range members {
			if m.NodeID == r.self.NodeID {
				continue
			}

			followerOff := r.getFollowerOffset(m.NodeID, key)
			if followerOff >= leaderOffset {
				continue // follower is up to date
			}

			msgs, err := pl.Read(followerOff, 256)
			if err != nil || len(msgs) == 0 {
				continue
			}

			body, err := json.Marshal(&ReplicaDataBody{
				Topic:     key.topic,
				Partition: key.partition,
				Messages:  msgs,
			})
			if err != nil {
				r.log.Error("replicator: marshal ReplicaData",
					"topic", key.topic, "partition", key.partition, "err", err)
				continue
			}
			out := &ClusterMsg{
				Type: MsgReplicaData,
				From: r.self.NodeID,
				Body: body,
			}
			if err := r.transport.Send(m.Addr, out); err != nil {
				r.log.Warn("replicator: send ReplicaData",
					"to", m.NodeID, "topic", key.topic,
					"partition", key.partition, "err", err)
			}
		}
	}
}

// followerTick sends fetch requests to the leader for partitions whose retry
// window (500 ms) has elapsed since the last received data.
func (r *Replicator) followerTick(election *Election) {
	leader, ok := election.Leader()
	if !ok || leader.NodeID == r.self.NodeID {
		return
	}

	r.mu.RLock()
	parts := make(map[partKey]*storage.PartitionLog, len(r.partitions))
	for k, v := range r.partitions {
		parts[k] = v
	}
	r.mu.RUnlock()

	now := time.Now()
	r.fetchMu.Lock()
	defer r.fetchMu.Unlock()

	for key, pl := range parts {
		last, seen := r.lastFetch[key]
		if seen && now.Sub(last) < 500*time.Millisecond {
			continue // retry window has not elapsed
		}
		r.sendFetchLocked(leader.Addr, key, pl.NextOffset())
		r.lastFetch[key] = now
	}
}

// sendFetchAll immediately sends a MsgReplicaFetch for every registered
// partition to the given leader. Called on follower role transition.
func (r *Replicator) sendFetchAll(leader Member) {
	r.mu.RLock()
	parts := make(map[partKey]*storage.PartitionLog, len(r.partitions))
	for k, v := range r.partitions {
		parts[k] = v
	}
	r.mu.RUnlock()

	now := time.Now()
	r.fetchMu.Lock()
	defer r.fetchMu.Unlock()

	for key, pl := range parts {
		r.sendFetchLocked(leader.Addr, key, pl.NextOffset())
		r.lastFetch[key] = now
	}
}

// sendFetchLocked enqueues a single MsgReplicaFetch. fetchMu must be held.
func (r *Replicator) sendFetchLocked(leaderAddr string, key partKey, fromOffset int64) {
	body, err := json.Marshal(&ReplicaFetchBody{
		Topic:      key.topic,
		Partition:  key.partition,
		FromOffset: fromOffset,
	})
	if err != nil {
		r.log.Error("replicator: marshal ReplicaFetch", "err", err)
		return
	}
	msg := &ClusterMsg{
		Type: MsgReplicaFetch,
		From: r.self.NodeID,
		Body: body,
	}
	if err := r.transport.Send(leaderAddr, msg); err != nil {
		r.log.Warn("replicator: send ReplicaFetch",
			"leader", leaderAddr, "topic", key.topic,
			"partition", key.partition, "err", err)
	}
}

// OnMessage dispatches an incoming replication message to the correct handler.
// Must be called by the node's dispatch loop for MsgReplicaFetch, MsgReplicaData,
// and MsgReplicaAck messages.
func (r *Replicator) OnMessage(msg *ClusterMsg) error {
	switch msg.Type {
	case MsgReplicaFetch:
		return r.handleFetch(msg)
	case MsgReplicaData:
		return r.handleData(msg)
	case MsgReplicaAck:
		return r.handleAck(msg)
	default:
		return fmt.Errorf("replicator: unexpected message type %d", msg.Type)
	}
}

// handleFetch is called on the leader when a follower sends MsgReplicaFetch.
func (r *Replicator) handleFetch(msg *ClusterMsg) error {
	var body ReplicaFetchBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("replicator: unmarshal ReplicaFetchBody: %w", err)
	}

	key := partKey{topic: body.Topic, partition: body.Partition}
	r.mu.RLock()
	pl, ok := r.partitions[key]
	r.mu.RUnlock()
	if !ok {
		return nil // partition not registered on this node
	}

	// Record the follower's current offset so leaderTick won't re-send stale data.
	r.setFollowerOffset(msg.From, key, body.FromOffset)

	leaderOffset := pl.NextOffset()
	if body.FromOffset >= leaderOffset {
		return nil // nothing to send
	}

	msgs, err := pl.Read(body.FromOffset, 256)
	if err != nil {
		return fmt.Errorf("replicator: read partition %s/%d: %w",
			body.Topic, body.Partition, err)
	}
	if len(msgs) == 0 {
		return nil
	}

	follower, ok := r.membership.Get(msg.From)
	if !ok {
		r.log.Warn("replicator: handleFetch sender not in membership", "from", msg.From)
		return nil
	}

	dataBody, err := json.Marshal(&ReplicaDataBody{
		Topic:     body.Topic,
		Partition: body.Partition,
		Messages:  msgs,
	})
	if err != nil {
		return fmt.Errorf("replicator: marshal ReplicaDataBody: %w", err)
	}
	resp := &ClusterMsg{
		Type: MsgReplicaData,
		From: r.self.NodeID,
		Body: dataBody,
	}
	if err := r.transport.Send(follower.Addr, resp); err != nil {
		return fmt.Errorf("replicator: send ReplicaData to %s: %w", msg.From, err)
	}
	return nil
}

// handleData is called on the follower when the leader sends MsgReplicaData.
func (r *Replicator) handleData(msg *ClusterMsg) error {
	var body ReplicaDataBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("replicator: unmarshal ReplicaDataBody: %w", err)
	}

	key := partKey{topic: body.Topic, partition: body.Partition}
	r.mu.RLock()
	pl, ok := r.partitions[key]
	r.mu.RUnlock()
	if !ok {
		return nil
	}

	// Reset the retry timer so followerTick won't re-fetch prematurely.
	r.fetchMu.Lock()
	r.lastFetch[key] = time.Now()
	r.fetchMu.Unlock()

	var highestOffset int64 = -1
	for _, m := range body.Messages {
		// Skip messages already written to prevent duplicates when
		// leaderTick and a MsgReplicaFetch response race to deliver the same batch.
		if m.Offset < pl.NextOffset() {
			if m.Offset > highestOffset {
				highestOffset = m.Offset
			}
			continue // already written; still track for ack
		}
		offset, err := pl.Append(m)
		if err != nil {
			r.log.Warn("replicator: append message",
				"topic", body.Topic, "partition", body.Partition, "err", err)
			continue
		}
		if offset > highestOffset {
			highestOffset = offset
		}
	}

	if highestOffset < 0 {
		return nil // nothing was written
	}

	// Ack back to the sender (leader).
	sender, ok := r.membership.Get(msg.From)
	if !ok {
		r.log.Warn("replicator: handleData sender not in membership", "from", msg.From)
		return nil
	}

	ackBody, err := json.Marshal(&ReplicaAckBody{
		Topic:     body.Topic,
		Partition: body.Partition,
		Offset:    highestOffset,
	})
	if err != nil {
		return fmt.Errorf("replicator: marshal ReplicaAckBody: %w", err)
	}
	ack := &ClusterMsg{
		Type: MsgReplicaAck,
		From: r.self.NodeID,
		Body: ackBody,
	}
	if err := r.transport.Send(sender.Addr, ack); err != nil {
		return fmt.Errorf("replicator: send ReplicaAck to %s: %w", msg.From, err)
	}
	return nil
}

// handleAck is called on the leader when a follower sends MsgReplicaAck.
func (r *Replicator) handleAck(msg *ClusterMsg) error {
	var body ReplicaAckBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return fmt.Errorf("replicator: unmarshal ReplicaAckBody: %w", err)
	}

	key := partKey{topic: body.Topic, partition: body.Partition}
	// Follower has written up to body.Offset; next expected offset is body.Offset+1.
	r.setFollowerOffset(msg.From, key, body.Offset+1)

	// Notify the ISR tracker attached to the partition log.
	r.mu.RLock()
	pl, ok := r.partitions[key]
	r.mu.RUnlock()
	if ok {
		if isr := pl.ISRTracker(); isr != nil {
			isr.Update(msg.From, body.Offset)
		}
	}
	return nil
}

// ─── follower offset helpers ──────────────────────────────────────────────────

func (r *Replicator) getFollowerOffset(nodeID string, key partKey) int64 {
	r.followerMu.Lock()
	defer r.followerMu.Unlock()
	if r.followerOffsets[nodeID] == nil {
		return 0
	}
	return r.followerOffsets[nodeID][key]
}

func (r *Replicator) setFollowerOffset(nodeID string, key partKey, offset int64) {
	r.followerMu.Lock()
	defer r.followerMu.Unlock()
	if r.followerOffsets[nodeID] == nil {
		r.followerOffsets[nodeID] = make(map[partKey]int64)
	}
	r.followerOffsets[nodeID][key] = offset
}
