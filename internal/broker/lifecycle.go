package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/gateway"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// runEmbeddedGateway waits for the broker's own TCP listener to come up,
// dials it as an ordinary pkg/client.Client over loopback, and starts the
// optional HTTP/WebSocket gateway against that connection. It runs in its
// own goroutine, started from Start() when GatewayConfig.Enabled is true.
func (b *Broker) runEmbeddedGateway(gwCfg config.GatewayConfig) {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && b.Addr() == "" {
		time.Sleep(10 * time.Millisecond)
	}
	addr := b.Addr()
	if addr == "" {
		b.log.Error("gateway: broker tcp listener did not become ready in time")
		return
	}

	gw := gateway.NewGateway(gwCfg, addr, nil, b.log)
	b.gatewayMu.Lock()
	b.gw = gw
	b.gatewayMu.Unlock()

	b.log.Info("gateway starting", "addr", gwCfg.Addr)
	if err := gw.Start(b.stopCtx); err != nil && b.stopCtx.Err() == nil {
		b.log.Error("gateway: start error", "err", err)
	}
}

// replayOffsetWAL replays the offset WAL and restores committed consumer offsets.
func (b *Broker) replayOffsetWAL() error {
	restoredOffsets, err := b.offsetWAL.Replay()
	if err != nil {
		return fmt.Errorf("broker: replay offset wal: %w", err)
	}
	b.offsets.Restore(restoredOffsets)
	b.log.Info("offset wal restored", "keys", len(restoredOffsets))
	return nil
}

// replayTopicWAL replays the topic WAL, re-creating deleted and created topics.
func (b *Broker) replayTopicWAL() error {
	topicCreates, topicDeletes, err := b.topicWAL.Replay()
	if err != nil {
		return fmt.Errorf("broker: replay topic wal: %w", err)
	}
	for _, name := range topicDeletes {
		_ = b.topics.Delete(name)
		b.log.Info("topic wal: replayed delete", "topic", name)
	}
	for _, cfg := range topicCreates {
		if cerr := b.topics.Create(cfg); cerr != nil {
			b.log.Debug("topic wal: replay create skip", "topic", cfg.Name, "err", cerr)
		} else {
			b.log.Info("topic wal: replayed create", "topic", cfg.Name)
		}
	}
	return nil
}

// replayMessageWAL replays recovered WAL entries back through the producer.
func (b *Broker) replayMessageWAL() {
	if len(b.walEntries) == 0 {
		return
	}
	recCtx, recCancel := context.WithCancel(context.Background())
	b.log.Info("replaying message wal", "entries", len(b.walEntries))
	for _, entry := range b.walEntries {
		var req protocol.PublishRequest
		if err := json.Unmarshal(entry.Data, &req); err != nil {
			b.log.Error("wal: unmarshal entry", "offset", entry.Offset, "err", err)
			continue
		}
		if req.TargetOffset != nil {
			if tp, tErr := b.topics.Get(req.Topic); tErr == nil {
				part, _ := b.partitioner.Assign(req.Topic, req.Key)
				if pl, pErr := tp.PartitionLog(part); pErr == nil {
					if pl.NextOffset() > *req.TargetOffset {
						b.log.Debug("wal: replay skip (already written)",
							"offset", entry.Offset,
							"target_offset", *req.TargetOffset)
						continue
					}
				}
			}
		}
		if _, err := b.producers.Publish(
			recCtx, req.Topic, req.Key, req.Payload, req.Headers,
			types.DeliveryMode(req.DeliveryMode), req.SeqNum, req.Codec,
		); err != nil {
			b.log.Debug("wal: replay skip", "offset", entry.Offset, "err", err)
		}
	}
	recCancel()
	b.walEntries = nil
}

// wirePartitions attaches ISR trackers to all existing topic partitions.
func (b *Broker) wirePartitions() {
	if b.clusterNode == nil {
		return
	}
	for _, tm := range b.topics.List() {
		tp, tErr := b.topics.Get(tm.Config.Name)
		if tErr != nil {
			continue
		}
		for p := 0; p < tm.Config.Partitions; p++ {
			pl, pErr := tp.PartitionLog(int32(p))
			if pErr == nil {
				b.wirePartition(tm.Config.Name, int32(p), pl)
			}
		}
	}
}

// collectMetricsSnapshot returns the current metric values as a map suitable
// for the HistoryStore. It reuses the same accessors as /metrics.
func (b *Broker) collectMetricsSnapshot() map[string]float64 {
	snap := make(map[string]float64)

	snap["messages_published_total"] = float64(b.metrics.MessagesPublished.Value())
	snap["messages_consumed_total"] = float64(b.metrics.MessagesConsumed.Value())
	snap["messages_errored_total"] = float64(b.metrics.MessagesErrored.Value())
	snap["messages_acked_total"] = float64(b.metrics.MessagesAcked.Value())
	snap["messages_nacked_total"] = float64(b.metrics.MessagesNacked.Value())
	snap["active_connections"] = b.metrics.ActiveConnections.Value()
	snap["consumer_lag_total"] = b.metrics.ConsumerLagTotal.Value()
	snap["active_consumer_groups"] = b.metrics.ActiveConsumerGroups.Value()
	snap["bytes_published_total"] = float64(b.metrics.BytesPublished.Value())
	snap["bytes_consumed_total"] = float64(b.metrics.BytesConsumed.Value())
	snap["topic_count"] = b.metrics.TopicCount.Value()
	snap["partition_count"] = b.metrics.PartitionCount.Value()
	snap["wal_bytes_total"] = float64(b.metrics.WALBytesTotal.Value())
	snap["wal_entries_total"] = float64(b.metrics.WALEntriesTotal.Value())
	snap["process_resident_memory_bytes"] = b.metrics.ProcessResidentMemoryBytes.Value()
	snap["go_gc_duration_seconds_count"] = float64(b.metrics.GoGCDurationSecondsCount.Value())
	snap["process_cpu_seconds_total"] = b.metrics.ProcessCPUSecondsTotal.Value()
	return snap
}
