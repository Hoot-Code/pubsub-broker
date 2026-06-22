package broker

import (
	"github.com/Hoot-Code/pubsub-broker/internal/cluster"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
)

// OwnsPartition reports whether this node owns the given topic partition.
// Always returns true in single-node mode.
func (b *Broker) OwnsPartition(topicName string, partition int32) bool {
	if b.clusterNode == nil {
		return true
	}
	owner := b.clusterNode.OwnerOf(topicName, partition)
	if owner == "" {
		return true // unassigned; accept optimistically
	}
	return owner == b.clusterNode.SelfID()
}

// wirePartition attaches an ISR tracker to a partition log and registers it
// with the cluster replicator. No-op in single-node mode.
func (b *Broker) wirePartition(topicName string, partition int32,
	pl *storage.PartitionLog) {
	if b.clusterNode == nil {
		return
	}
	tracker := cluster.NewISRTracker(1000, 10_000)
	pl.SetISRTracker(tracker)
	b.storeISRTracker(topicName, partition, tracker)
	b.clusterNode.RegisterPartition(topicName, partition, pl)
}

// storeISRTracker registers an ISR tracker for a topic-partition pair so that
// updateDynamicMetrics and handlePublish can access it.
func (b *Broker) storeISRTracker(topicName string, partition int32, t *cluster.ISRTracker) {
	b.partTrackersMu.Lock()
	defer b.partTrackersMu.Unlock()
	if b.partTrackers == nil {
		b.partTrackers = make(map[string]map[int32]*cluster.ISRTracker)
	}
	if b.partTrackers[topicName] == nil {
		b.partTrackers[topicName] = make(map[int32]*cluster.ISRTracker)
	}
	b.partTrackers[topicName][partition] = t
}

// getISRTracker returns the ISR tracker for a topic-partition pair, or nil if
// cluster mode is disabled or no tracker has been registered.
func (b *Broker) getISRTracker(topicName string, partition int32) *cluster.ISRTracker {
	b.partTrackersMu.RLock()
	defer b.partTrackersMu.RUnlock()
	if b.partTrackers == nil {
		return nil
	}
	return b.partTrackers[topicName][partition]
}

// onClusterMetaSync is called when a MsgMetaSync-driven partition map update
// arrives. It logs acquired/released partition ownership changes.
func (b *Broker) onClusterMetaSync(newPM *cluster.PartitionMap) {
	nodeID := b.cfg.Get().Cluster.NodeID
	if nodeID == "" {
		nodeID = b.cfg.Get().Broker.NodeID
	}
	topics := b.topics.List()

	b.prevOwnedMu.Lock()
	defer b.prevOwnedMu.Unlock()
	if b.prevOwned == nil {
		b.prevOwned = make(map[string]map[int32]bool)
	}

	for _, tm := range topics {
		name := tm.Config.Name
		for p := 0; p < tm.Config.Partitions; p++ {
			part := int32(p)
			newOwner := newPM.Owner(name, part)
			newOwns := newOwner == nodeID
			oldOwns := b.prevOwned[name][part]

			if !oldOwns && newOwns {
				b.log.Info("acquired partition ownership",
					"topic", name, "partition", part)
			} else if oldOwns && !newOwns {
				b.log.Info("released partition ownership",
					"topic", name, "partition", part)
			}

			if b.prevOwned[name] == nil {
				b.prevOwned[name] = make(map[int32]bool)
			}
			b.prevOwned[name][part] = newOwns
		}
	}
}
