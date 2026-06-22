package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/cluster"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/producer"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/internal/tracing"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// handlePublish handles CmdPublish: a single-message publish.
// Requires PermPublish and topic-level ACL (D4, D5).
// WAL is appended before the segment write for crash recovery.
//
// The per-partition mutex (b.partitionMutex) is held from TargetOffset
// capture through producers.Publish (which calls pl.Append). This guarantees
// the WAL entry's TargetOffset equals the offset pl.Append will actually
// assign, even under concurrent publishes to the same partition. Without this,
// two goroutines can read the same NextOffset and one produces a WAL entry
// whose TargetOffset is stale by the time it is used during crash replay,
// causing messages to be silently skipped on recovery.
func (b *Broker) handlePublish(ctx context.Context, conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.PublishRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid publish body")
	}
	if req.Topic == "" {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "topic required")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermPublish), req.Topic) {
		b.logAudit(audit.EventForbidden, conn, req.Topic, false, "FORBIDDEN: publish")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to publish")
	}

	// Compute the partition once; producers.Publish will derive the same value
	// from the same (topic, key) pair via the same partitioner.
	part, _ := b.partitioner.Assign(req.Topic, req.Key)

	// Cluster partition-ownership check.
	if b.clusterNode != nil {
		if tp, err := b.topics.Get(req.Topic); err == nil {
			if numParts := int32(tp.Config().Partitions); numParts > 0 {
				if !b.OwnsPartition(req.Topic, part) {
					ownerID := b.clusterNode.OwnerOf(req.Topic, part)
					return conn.SendError(f.RequestID, "NOT_LEADER",
						fmt.Sprintf("partition %d owned by %s", part, ownerID))
				}
			}
		}
	}

	// Tracing.
	ctx, sp := b.tracer.Start(ctx, "broker.publish",
		"topic", req.Topic,
		"partition", "0",
	)
	defer sp.End()

	clientID := conn.ClientID()
	b.authMu.RLock()
	rl := b.rateLimiter
	b.authMu.RUnlock()
	if !rl.Allow(clientID, req.Topic) {
		sp.SetStatus(tracing.StatusError, "rate limit exceeded")
		return conn.SendError(f.RequestID, string(types.ErrBrokerOverloaded), "rate limit exceeded")
	}

	cfg := b.cfg.Get()

	// Hold the per-partition mutex across TargetOffset capture, WAL
	// append, and producers.Publish (which calls pl.Append) so the WAL entry's
	// TargetOffset cannot be stale.
	pmu := b.partitionMutex(req.Topic, part)
	pmu.Lock()

	// Capture TargetOffset INSIDE the critical section — this is the offset
	// that WILL be assigned by pl.Append, not the offset that was current when
	// the handler started.
	if tp, tErr := b.topics.Get(req.Topic); tErr == nil {
		if pl, pErr := tp.PartitionLog(part); pErr == nil {
			targetOff := pl.NextOffset()
			req.TargetOffset = &targetOff
		}
	}
	walData, err := json.Marshal(&req)
	if err != nil {
		pmu.Unlock()
		sp.SetStatus(tracing.StatusError, err.Error())
		return conn.SendError(f.RequestID, string(types.ErrInternal), "wal marshal error")
	}

	// Producer-side WAL backpressure.
	b.pendingWALBytes.Add(int64(len(walData)))
	thresh := cfg.Storage.WalBackpressureThreshold
	if thresh > 0 && b.pendingWALBytes.Load() > thresh {
		b.pendingWALBytes.Add(-int64(len(walData)))
		pmu.Unlock()
		return conn.SendError(f.RequestID, string(types.ErrBrokerOverloaded),
			"broker WAL backpressure: too many pending writes")
	}

	if _, err := b.msgWAL.Append(walData); err != nil {
		b.pendingWALBytes.Add(-int64(len(walData)))
		pmu.Unlock()
		sp.SetStatus(tracing.StatusError, err.Error())
		return conn.SendError(f.RequestID, string(types.ErrInternal),
			fmt.Sprintf("wal append: %v", err))
	}
	b.metrics.WALBytesTotal.Inc(uint64(len(walData)))
	b.metrics.WALEntriesTotal.Inc(1)

	// pl.Append happens inside Publish; still under the partition lock so the
	// offset it assigns equals the TargetOffset captured above.
	result, err := b.producers.Publish(
		ctx, req.Topic, req.Key, req.Payload, req.Headers,
		types.DeliveryMode(req.DeliveryMode), req.SeqNum, req.Codec,
	)
	b.pendingWALBytes.Add(-int64(len(walData)))
	pmu.Unlock()
	if err != nil {
		sp.SetStatus(tracing.StatusError, err.Error())
		return conn.SendError(f.RequestID, string(types.ErrInternal), err.Error())
	}

	sp.AddAttr("partition", fmt.Sprintf("%d", result.Partition))
	sp.AddAttr("offset", fmt.Sprintf("%d", result.Offset))
	b.metrics.BytesPublished.Inc(uint64(len(req.Payload)))
	b.metrics.MessagesPublished.Inc(1)

	// Quorum wait in cluster mode.
	if err := b.waitForQuorum(ctx, req.Topic, result.Partition, result.Offset); err != nil {
		return conn.SendError(f.RequestID, "QUORUM_TIMEOUT", err.Error())
	}

	// Build a single message for both Dispatch and the Explorer live-tap.
	// This avoids constructing a second literal (which would lose the
	// Timestamp set by NewMessage inside producer.Publish).
	msg := &types.Message{
		ID:        result.MessageID,
		Topic:     req.Topic,
		Partition: result.Partition,
		Offset:    result.Offset,
		Timestamp: time.Now().UnixNano(),
		Payload:   req.Payload,
		Headers:   req.Headers,
		Key:       req.Key,
	}

	b.consumers.Dispatch(msg)

	// Live-tap: fan out to Explorer sessions (Phase 17).
	b.explorerHub.Publish(req.Topic, result.Partition, clientID, msg)

	b.logAudit(audit.EventPublish, conn, req.Topic, true, "")
	return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.PublishResponse{
		MessageID: result.MessageID,
		Partition: result.Partition,
		Offset:    result.Offset,
	})
}

// handleBatchPublish handles CmdBatchPublish.
// Only single-topic batches are supported; mixed-topic batches return BAD_REQUEST.
// Requires PermPublish and topic-level ACL (D4, D5).
//
// Each message's WAL entry carries the TargetOffset that will be
// assigned to that specific message, captured under the same per-partition
// mutex that spans pl.Append.
//
// The WAL backpressure check accounts for the TOTAL payload size of
// the entire batch (sum of all message payloads) and rejects the whole batch
// with BROKER_OVERLOADED if the threshold would be exceeded; no partial accept.
func (b *Broker) handleBatchPublish(ctx context.Context, conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.BatchPublishRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid batch-publish body")
	}
	if len(req.Messages) == 0 {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "batch is empty")
	}

	firstTopic := req.Messages[0].Topic
	for _, m := range req.Messages[1:] {
		if m.Topic != firstTopic {
			return conn.SendError(f.RequestID, "BAD_REQUEST",
				fmt.Sprintf("mixed-topic batches not supported: %q vs %q", firstTopic, m.Topic))
		}
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermPublish), firstTopic) {
		b.logAudit(audit.EventForbidden, conn, firstTopic, false, "FORBIDDEN: publish")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to publish")
	}

	clientID := conn.ClientID()
	b.authMu.RLock()
	rl := b.rateLimiter
	b.authMu.RUnlock()
	if !rl.Allow(clientID, firstTopic) {
		return conn.SendError(f.RequestID, string(types.ErrBrokerOverloaded), "rate limit exceeded")
	}

	cfg := b.cfg.Get()

	// Backpressure check on the TOTAL batch payload. Sum every message's
	// payload and reject the entire batch up front if it would exceed the
	// threshold — never partial-accept.
	thresh := cfg.Storage.WalBackpressureThreshold
	var totalPayload int64
	for _, m := range req.Messages {
		totalPayload += int64(len(m.Payload))
	}
	if thresh > 0 && b.pendingWALBytes.Load()+totalPayload > thresh {
		return conn.SendError(f.RequestID, string(types.ErrBrokerOverloaded),
			fmt.Sprintf("broker WAL backpressure: batch payload %d bytes exceeds threshold", totalPayload))
	}

	// results holds one PublishResult per input message. We drive per-message
	// locking + WAL + Publish directly from this handler (rather than calling
	// PublishBatch) so the per-partition mutex can span TargetOffset capture →
	// pl.Append for each message.
	results := make([]producer.PublishResult, len(req.Messages))
	// batchPendingBytes tracks the cumulative pending WAL bytes added by this
	// batch so we can roll them back on a mid-batch failure.
	var batchPendingBytes int64

	for i := range req.Messages {
		m := &req.Messages[i]
		part, _ := b.partitioner.Assign(m.Topic, m.Key)

		// Per-message partition lock spanning TargetOffset capture,
		// WAL append, and pl.Append (inside Publish).
		pmu := b.partitionMutex(m.Topic, part)
		pmu.Lock()

		// Capture the TargetOffset that WILL be assigned to THIS message.
		if tp, tErr := b.topics.Get(m.Topic); tErr == nil {
			if pl, pErr := tp.PartitionLog(part); pErr == nil {
				targetOff := pl.NextOffset()
				m.TargetOffset = &targetOff
			}
		}
		walData, err := json.Marshal(m)
		if err != nil {
			pmu.Unlock()
			// Roll back the pending WAL bytes already accounted for this batch.
			b.pendingWALBytes.Add(-batchPendingBytes)
			return conn.SendError(f.RequestID, string(types.ErrInternal), "batch wal marshal error")
		}

		walLen := int64(len(walData))
		b.pendingWALBytes.Add(walLen)
		batchPendingBytes += walLen
		if _, err := b.msgWAL.Append(walData); err != nil {
			pmu.Unlock()
			b.pendingWALBytes.Add(-batchPendingBytes)
			return conn.SendError(f.RequestID, string(types.ErrInternal),
				fmt.Sprintf("batch wal append: %v", err))
		}
		b.metrics.WALBytesTotal.Inc(uint64(walLen))
		b.metrics.WALEntriesTotal.Inc(1)

		// Append to the segment log under the same lock.
		r, pErr := b.producers.Publish(
			ctx, m.Topic, m.Key, m.Payload, m.Headers,
			types.DeliveryMode(m.DeliveryMode), m.SeqNum, m.Codec,
		)
		b.pendingWALBytes.Add(-walLen)
		batchPendingBytes -= walLen
		pmu.Unlock()
		if pErr != nil {
			results[i] = producer.PublishResult{Error: pErr}
			continue
		}
		results[i] = *r
	}

	// Dispatch + build response (outside all partition locks).
	responses := make([]protocol.PublishResponse, len(results))
	for i, r := range results {
		if r.Error != nil {
			continue
		}
		// Build a single message for both Dispatch and the Explorer live-tap.
		msg := &types.Message{
			ID:        r.MessageID,
			Topic:     firstTopic,
			Partition: r.Partition,
			Offset:    r.Offset,
			Timestamp: time.Now().UnixNano(),
			Payload:   req.Messages[i].Payload,
			Headers:   req.Messages[i].Headers,
			Key:       req.Messages[i].Key,
		}
		b.consumers.Dispatch(msg)
		// Live-tap: fan out to Explorer sessions (Phase 17).
		b.explorerHub.Publish(firstTopic, r.Partition, clientID, msg)
		b.metrics.BytesPublished.Inc(uint64(len(req.Messages[i].Payload)))
		b.metrics.MessagesPublished.Inc(1)
		responses[i] = protocol.PublishResponse{
			MessageID: r.MessageID,
			Partition: r.Partition,
			Offset:    r.Offset,
		}
	}

	return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.BatchPublishResponse{
		Results: responses,
	})
}

// waitForQuorum blocks until a quorum of ISR replicas acknowledge the write
// at offset in topic/partition. Returns nil immediately in single-node mode
// or when the ISR is too small to require waiting.
func (b *Broker) waitForQuorum(ctx context.Context, topicName string,
	partition int32, offset int64) error {
	if b.clusterNode == nil {
		return nil
	}
	tracker := b.getISRTracker(topicName, partition)
	if tracker == nil {
		return nil
	}
	nodeID := b.clusterNode.SelfID()
	isr := tracker.ISR(nodeID, offset)
	quorum := len(isr)/2 + 1
	if quorum <= 1 {
		return nil
	}
	timeoutMs := b.cfg.Get().Cluster.QuorumTimeoutMs
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	qCtx, qCancel := context.WithTimeout(ctx, time.Duration(timeoutMs)*time.Millisecond)
	defer qCancel()
	if err := cluster.WaitForQuorum(qCtx, tracker, nodeID, offset, quorum, 0); err != nil {
		b.log.Warn("quorum write timeout",
			"topic", topicName,
			"partition", partition,
			"offset", offset,
			"quorum", quorum,
			"isr_size", len(isr))
		return fmt.Errorf("quorum not reached before deadline")
	}
	return nil
}
