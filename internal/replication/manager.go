// Package replication implements leader-follower log replication.
//
// Architecture:
//   - Each topic-partition has exactly one leader and N-1 followers.
//   - Followers pull from the leader periodically (pull replication).
//   - ISR (In-Sync Replicas) tracking determines ack safety.
//   - The manager exposes replication lag as a metric.
package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Replica state ────────────────────────────────────────────────────────────

// ReplicaState tracks the sync position of one follower (when this node is
// the leader) or the address of the leader (when this node is a follower).
type ReplicaState struct {
	mu          sync.Mutex
	NodeID      string
	Addr        string // leader addr when this node is a follower; follower addr otherwise
	NextOffset  int64
	LastContact time.Time
	InSync      bool
	// Topic and Partition identify which topic-partition this replica entry
	// represents. They are populated by SetReplicaPartition and consumed by
	// Pull so the follower's FetchRequest identifies the correct log to the
	// leader. Defaults to "" / 0 when unset (legacy wiring); Pull
	// still sends them so a misconfigured follower fails loudly at the leader
	// rather than silently fetching the wrong partition.
	Topic     string
	Partition int32
}

func (r *ReplicaState) update(offset int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.NextOffset = offset
	r.LastContact = time.Now()
	r.InSync = true
}

func (r *ReplicaState) lag(leaderOffset int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	lag := leaderOffset - r.NextOffset
	if lag < 0 {
		return 0
	}
	return lag
}

// ─── ReplicationLog interface ─────────────────────────────────────────────────

// ReplicationLog is the subset of storage.PartitionLog needed by the replicator.
type ReplicationLog interface {
	Append(msg *types.Message) (int64, error)
	Read(startOffset int64, maxCount int) ([]*types.Message, error)
	NextOffset() int64
}

// ─── Manager ─────────────────────────────────────────────────────────────────

// Manager orchestrates replication for all topic-partitions on this node.
type Manager struct {
	mu            sync.RWMutex
	nodeID        string
	isLeader      atomic.Bool
	replicas      map[string]*ReplicaState  // nodeID → state
	partitionLogs map[string]ReplicationLog // nodeID → log for follower Pull
	syncInterval  time.Duration
	ackTimeout    time.Duration
	factor        int
	log           *logging.Logger
	metricsBundle *metrics.Broker
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// NewManager creates a replication Manager for this broker node.
func NewManager(
	nodeID string,
	factor int,
	syncInterval, ackTimeout time.Duration,
	log *logging.Logger,
	m *metrics.Broker,
) *Manager {
	return &Manager{
		nodeID:        nodeID,
		replicas:      make(map[string]*ReplicaState),
		partitionLogs: make(map[string]ReplicationLog),
		syncInterval:  syncInterval,
		ackTimeout:    ackTimeout,
		factor:        factor,
		log:           log,
		metricsBundle: m,
	}
}

// Start begins replication work. Call as a goroutine.
func (mgr *Manager) Start(ctx context.Context) {
	ctx, mgr.cancel = context.WithCancel(ctx)
	mgr.wg.Add(1)
	go func() {
		defer mgr.wg.Done()
		mgr.heartbeatLoop(ctx)
	}()
}

// Stop halts replication and waits for the heartbeat goroutine to exit.
func (mgr *Manager) Stop() {
	if mgr.cancel != nil {
		mgr.cancel()
		mgr.wg.Wait()
	}
}

// RegisterReplica adds a follower node. Called when a follower connects.
func (mgr *Manager) RegisterReplica(nodeID, addr string) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.replicas[nodeID] = &ReplicaState{
		NodeID:      nodeID,
		Addr:        addr,
		LastContact: time.Now(),
	}
	mgr.log.Replication("replica_registered", nodeID, 0)
}

// AddReplica stores a replica entry with the leader address alongside the
// ReplicaState and associates the given partition log. When this node is a
// follower, leaderAddr is the address to Pull from; nodeID identifies which
// partition/replica this entry represents.
func (mgr *Manager) AddReplica(nodeID, leaderAddr string, pl ReplicationLog) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.replicas[nodeID] = &ReplicaState{
		NodeID:      nodeID,
		Addr:        leaderAddr,
		LastContact: time.Now(),
	}
	mgr.partitionLogs[nodeID] = pl
	mgr.log.Replication("replica_added", nodeID, 0)
}

// SetPartitionLog associates a ReplicationLog with the given replica nodeID.
// The broker calls this when wiring a follower's partition log on RegisterReplica.
func (mgr *Manager) SetPartitionLog(nodeID string, pl ReplicationLog) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	mgr.partitionLogs[nodeID] = pl
}

// SetReplicaPartition records which topic-partition a replica entry represents
// so that the follower Pull path can populate FetchRequest.Topic and
// FetchRequest.Partition. Without this, Pull sent an empty Topic and
// the leader's handleFetch could not identify which log to read, returning an
// error or the wrong data on every pull.
func (mgr *Manager) SetReplicaPartition(nodeID, topic string, partition int32) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	if r, ok := mgr.replicas[nodeID]; ok {
		r.mu.Lock()
		r.Topic = topic
		r.Partition = partition
		r.mu.Unlock()
	}
}

// SetLeader marks this node as the partition leader or follower.
func (mgr *Manager) SetLeader(leader bool) { mgr.isLeader.Store(leader) }

// IsLeader reports whether this node is the current partition leader.
func (mgr *Manager) IsLeader() bool { return mgr.isLeader.Load() }

// ReplicaLag returns the maximum replication lag across all replicas (messages).
func (mgr *Manager) ReplicaLag(leaderOffset int64) int64 {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	var maxLag int64
	for _, r := range mgr.replicas {
		if l := r.lag(leaderOffset); l > maxLag {
			maxLag = l
		}
	}
	return maxLag
}

// ISRCount returns the number of replicas currently in sync.
func (mgr *Manager) ISRCount() int {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	staleBefore := time.Now().Add(-3 * mgr.syncInterval)
	count := 1 // count self (leader)
	for _, r := range mgr.replicas {
		r.mu.Lock()
		inSync := r.InSync && r.LastContact.After(staleBefore)
		r.mu.Unlock()
		if inSync {
			count++
		}
	}
	return count
}

// ─── Leader: Replicate ────────────────────────────────────────────────────────

// Replicate sends new messages from pl (starting at fromOffset) to a single follower.
// Called by the leader when a follower polls for updates.
func (mgr *Manager) Replicate(followerNodeID string, pl ReplicationLog, fromOffset int64, conn net.Conn) error {
	msgs, err := pl.Read(fromOffset, 256)
	if err != nil {
		return fmt.Errorf("read for replication: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	enc := protocol.NewEncoder(conn)
	for _, msg := range msgs {
		body, _ := json.Marshal(msg)
		if err := enc.Encode(protocol.CmdPublish, 0, json.RawMessage(body)); err != nil {
			return fmt.Errorf("replicate send: %w", err)
		}
	}

	mgr.mu.RLock()
	r, ok := mgr.replicas[followerNodeID]
	mgr.mu.RUnlock()
	if ok {
		r.update(fromOffset + int64(len(msgs)))
	}

	lag := mgr.ReplicaLag(pl.NextOffset())
	mgr.metricsBundle.ReplicationLag.Set(float64(lag))
	mgr.log.Replication("replicated", followerNodeID, lag)
	return nil
}

// ─── Follower: Pull ───────────────────────────────────────────────────────────

// Pull fetches new messages from the leader and appends them to pl.
// This is called by a follower on a background ticker.
//
// Topic and partition are required parameters so the leader's
// handleFetch can identify which log to read. Previously Pull left
// FetchRequest.Topic and .Group empty, so the leader returned an error or the
// wrong data on every pull.
func (mgr *Manager) Pull(ctx context.Context, leaderAddr, topic string, partition int32, pl ReplicationLog) error {
	conn, err := net.DialTimeout("tcp", leaderAddr, mgr.ackTimeout)
	if err != nil {
		return fmt.Errorf("pull: dial leader %s: %w", leaderAddr, err)
	}
	defer conn.Close()

	fromOffset := pl.NextOffset()
	enc := protocol.NewEncoder(conn)
	if err := enc.Encode(protocol.CmdFetch, 0, &protocol.FetchRequest{
		Topic:     topic,
		Partition: partition,
		Offset:    fromOffset,
		MaxCount:  256,
	}); err != nil {
		return fmt.Errorf("pull: send fetch: %w", err)
	}

	dec := protocol.NewDecoder(conn)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(mgr.ackTimeout))
		f, err := dec.Decode()
		if err != nil {
			break
		}
		if f.Command != protocol.CmdPublish {
			break
		}
		var msg types.Message
		if err := protocol.Unmarshal(f, &msg); err != nil {
			return fmt.Errorf("pull: decode message: %w", err)
		}
		if _, err := pl.Append(&msg); err != nil {
			return fmt.Errorf("pull: append: %w", err)
		}
	}
	return nil
}

// ─── Heartbeat loop ───────────────────────────────────────────────────────────

func (mgr *Manager) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(mgr.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if mgr.IsLeader() {
				mgr.checkReplicaHealth()
			} else {
				// Follower: pull from any registered leader addresses.
				mgr.pullFromLeaders(ctx)
			}
		}
	}
}

// pullFromLeaders iterates replicas that have a non-empty Addr (meaning
// this node is a follower) and calls Pull for each associated partition log.
func (mgr *Manager) pullFromLeaders(ctx context.Context) {
	type target struct {
		addr      string
		topic     string
		partition int32
		pl        ReplicationLog
	}

	mgr.mu.RLock()
	var targets []target
	for nodeID, r := range mgr.replicas {
		r.mu.Lock()
		addr := r.Addr
		topic := r.Topic
		partition := r.Partition
		r.mu.Unlock()
		if addr == "" {
			continue
		}
		pl, ok := mgr.partitionLogs[nodeID]
		if !ok {
			continue
		}
		targets = append(targets, target{addr: addr, topic: topic, partition: partition, pl: pl})
	}
	mgr.mu.RUnlock()

	for _, t := range targets {
		// Pass topic+partition so the leader's handleFetch reads the
		// correct log instead of erroring on an empty Topic.
		if err := mgr.Pull(ctx, t.addr, t.topic, t.partition, t.pl); err != nil {
			mgr.log.Error("follower pull failed", "leader", t.addr, "err", err)
		}
	}
}

func (mgr *Manager) checkReplicaHealth() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	staleBefore := time.Now().Add(-3 * mgr.syncInterval)
	for _, r := range mgr.replicas {
		r.mu.Lock()
		if r.InSync && r.LastContact.Before(staleBefore) {
			r.InSync = false
			mgr.log.Replication("replica_stale", r.NodeID, -1)
		}
		r.mu.Unlock()
	}
}
