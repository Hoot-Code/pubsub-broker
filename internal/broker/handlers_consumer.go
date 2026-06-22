package broker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
	"github.com/Hoot-Code/pubsub-broker/internal/auth"
	"github.com/Hoot-Code/pubsub-broker/internal/consumer"
	"github.com/Hoot-Code/pubsub-broker/internal/networking"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/internal/tracing"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// handleSubscribe handles CmdSubscribe.
// Requires PermSubscribe and topic-level ACL (D4, D5).
func (b *Broker) handleSubscribe(ctx context.Context, conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.SubscribeRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid subscribe body")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermSubscribe), req.Topic) {
		b.logAudit(audit.EventForbidden, conn, req.Topic, false, "FORBIDDEN: subscribe")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to subscribe")
	}

	tp, err := b.topics.Get(req.Topic)
	if err != nil {
		return conn.SendError(f.RequestID, string(types.ErrTopicNotFound), err.Error())
	}

	partCount := int32(tp.Config().Partitions)
	c, err := b.consumers.Subscribe(req.Group, req.ConsumerID, conn.ClientID(), req.Topic, partCount)
	if err != nil {
		if strings.Contains(err.Error(), "invalid group name") {
			return conn.SendError(f.RequestID, "BAD_REQUEST", err.Error())
		}
		return conn.SendError(f.RequestID, string(types.ErrInternal), err.Error())
	}

	// Push mode: start background delivery loop.
	if req.Push {
		c.SetSink(conn)

		// Send the OK response BEFORE starting the replay and delivery
		// loop. This guarantees the client receives the subscription
		// acknowledgment before any CmdPush frames from replay, which
		// prevents a race where replay CmdPush frames arrive before the
		// client has registered its push router.
		b.logAudit(audit.EventSubscribe, conn, req.Topic, true, "")
		if err := conn.SendOK(f.RequestID); err != nil {
			return err
		}

		go b.pushDeliveryLoop(c, conn, req.Group, req.ConsumerID, req.Topic)

		// Replay existing messages for brand-new consumer groups that have
		// never committed an offset (auto.offset.reset=earliest semantics).
		// The replay is performed synchronously so that all replay frames are
		// dispatched to the consumer's push channel in a deterministic
		// order before any live delivery begins.
		if !b.consumers.HasCommittedOffset(req.Group, req.Topic, 0) {
			b.replayToConsumer(tp, req.Group, req.Topic, partCount)
		}
		return nil
	}

	b.logAudit(audit.EventSubscribe, conn, req.Topic, true, "")
	return conn.SendOK(f.RequestID)
}

// replayToConsumer reads all existing messages from each partition and
// dispatches them to the given group. This is called when a brand-new consumer
// group (no committed offsets) subscribes in push mode, so the consumer
// receives historical messages before live delivery begins.
func (b *Broker) replayToConsumer(
	tp interface {
		PartitionLog(partition int32) (*storage.PartitionLog, error)
		Config() types.TopicConfig
	},
	group, topic string, partCount int32,
) {
	for p := int32(0); p < partCount; p++ {
		pl, err := tp.PartitionLog(p)
		if err != nil {
			continue
		}
		offset := int64(0)
		for {
			msgs, err := pl.Read(offset, 100)
			if err != nil || len(msgs) == 0 {
				break
			}
			for _, msg := range msgs {
				b.consumers.DispatchToGroup(group, topic, p, msg)
			}
			offset = msgs[len(msgs)-1].Offset + 1
		}
	}
}

// pushDeliveryLoop reads from the consumer's push channel and writes each
// message as a CmdPush frame to conn.
// The loop applies flow control: when the push channel is full it pauses for
// FlowControlPauseMs (default 100 ms), then moves the message to the DLQ if
// the channel is still saturated.
func (b *Broker) pushDeliveryLoop(
	c *consumer.Consumer,
	conn *networking.Conn,
	group, consumerID, topic string,
) {
	pushCh := c.PushMessages()
	defer func() {
		c.SetSink(nil)
		b.consumers.Unsubscribe(group, consumerID, topic)
	}()
	for {
		select {
		case msg, ok := <-pushCh:
			if !ok {
				return
			}
			// Flow control: pause and optionally DLQ when channel is full.
			// Read the current pause value from the atomic on each use so
			// hot-reloaded config changes take effect immediately.
			if len(pushCh) == cap(pushCh) {
				pauseMs := int(b.flowControlPauseMs.Load())
				if pauseMs <= 0 {
					pauseMs = 100
				}
				flowPause := time.Duration(pauseMs) * time.Millisecond
				select {
				case <-time.After(flowPause):
				case <-c.Done():
					return
				case <-b.stopCtx.Done():
					return
				}
				if len(pushCh) == cap(pushCh) {
					b.consumers.DLQ().Push(consumer.DLQEntry{
						Original: msg,
						Reason: fmt.Sprintf("push channel full after %s pause, consumer %s",
							flowPause, consumerID),
						Attempts: 1,
					})
					continue
				}
			}
			frame := &protocol.PushFrame{
				Topic:      msg.Topic,
				Group:      group,
				ConsumerID: consumerID,
				Partition:  msg.Partition,
				Messages:   []*types.Message{msg},
			}
			if err := conn.WriteFrame(protocol.CmdPush, 0, frame); err != nil {
				b.log.Warn("push: write error",
					"consumer", consumerID, "group", group, "topic", topic, "err", err)
				return
			}
		case <-c.Done():
			return
		case <-b.stopCtx.Done():
			return
		}
	}
}

// handleUnsubscribe handles CmdUnsubscribe.
func (b *Broker) handleUnsubscribe(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.SubscribeRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid unsubscribe body")
	}
	b.consumers.Unsubscribe(req.Group, req.ConsumerID, req.Topic)
	return conn.SendOK(f.RequestID)
}

// handleFetch handles CmdFetch: pull-mode message retrieval.
// Requires PermFetch and topic-level ACL (D4, D5).
func (b *Broker) handleFetch(ctx context.Context, conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.FetchRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid fetch body")
	}

	// RBAC + topic ACL gate (D4, D5).
	if !conn.Can(string(auth.PermFetch), req.Topic) {
		b.logAudit(audit.EventForbidden, conn, req.Topic, false, "FORBIDDEN: fetch")
		return conn.SendError(f.RequestID, "FORBIDDEN", "insufficient permissions to fetch")
	}

	// Tracing.
	_, sp := b.tracer.Start(ctx, "broker.fetch",
		"topic", req.Topic,
		"group", req.Group,
	)
	defer sp.End()

	tp, err := b.topics.Get(req.Topic)
	if err != nil {
		sp.SetStatus(tracing.StatusError, err.Error())
		return conn.SendError(f.RequestID, string(types.ErrTopicNotFound), err.Error())
	}

	pl, err := tp.PartitionLog(req.Partition)
	if err != nil {
		sp.SetStatus(tracing.StatusError, err.Error())
		return conn.SendError(f.RequestID, string(types.ErrPartitionNotFound), err.Error())
	}

	maxCount := req.MaxCount
	if maxCount <= 0 || maxCount > 1000 {
		maxCount = 100
	}

	var msgs []*types.Message
	if req.Group == "" {
		// No consumer group: a stateless, direct read starting at the
		// caller-supplied offset (used e.g. by the HTTP gateway's REST
		// fetch endpoint, which has no notion of a consumer group).
		msgs, err = pl.Read(req.Offset, maxCount)
	} else {
		msgs, err = b.consumers.PollPartitionLog(ctx, req.Group, req.Topic, req.Partition, pl, maxCount)
	}
	if err != nil {
		if err == context.DeadlineExceeded || err == context.Canceled {
			return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.FetchResponse{
				Topic:     req.Topic,
				Partition: req.Partition,
				Messages:  []*types.Message{},
			})
		}
		sp.SetStatus(tracing.StatusError, err.Error())
		return conn.SendError(f.RequestID, string(types.ErrInternal), err.Error())
	}

	sp.AddAttr("messages_fetched", fmt.Sprintf("%d", len(msgs)))
	var totalBytes uint64
	for _, m := range msgs {
		totalBytes += uint64(len(m.Payload))
	}
	b.metrics.BytesConsumed.Inc(totalBytes)
	b.metrics.MessagesConsumed.Inc(uint64(len(msgs)))

	// Optional zero-copy fast-path via sendfile(2).
	if req.RawTransfer {
		const maxRawBytes = 4 * 1024 * 1024
		n, err := pl.SendTo(conn.RawConn(), req.Offset, maxRawBytes)
		if err != nil {
			// SendTo failed: log and fall through to the normal response path
			// so the client still receives a well-formed FetchResponse.
			b.log.Warn("handleFetch: SendTo failed, falling back",
				"topic", req.Topic, "partition", req.Partition, "err", err)
		} else {
			// SendTo succeeded — the payload was streamed out-of-band
			// directly to the socket. We MUST still send a FetchResponse frame
			// so the client knows the transfer completed and how many bytes to
			// expect. RawBytes=true tells the client NOT to unmarshal Messages.
			return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.FetchResponse{
				Topic:     req.Topic,
				Partition: req.Partition,
				Messages:  []*types.Message{},
				RawBytes:  true,
				BytesSent: n,
			})
		}
	}

	return conn.WriteFrame(protocol.CmdResponse, f.RequestID, &protocol.FetchResponse{
		Topic:     req.Topic,
		Partition: req.Partition,
		Messages:  msgs,
	})
}

// handleCommitOffset handles CmdCommitOffset.
// Commits a consumer offset and persists it to the offset WAL.
func (b *Broker) handleCommitOffset(conn *networking.Conn, f *protocol.Frame) error {
	var req protocol.CommitOffsetRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid commit-offset body")
	}
	if err := b.consumers.CommitOffset(req.Group, req.ConsumerID, req.Topic, req.Partition, req.Offset); err != nil {
		return conn.SendError(f.RequestID, string(types.ErrInternal), err.Error())
	}
	if err := b.offsetWAL.Commit(req.Group, req.Topic, req.Partition, req.Offset); err != nil {
		b.log.Warn("offset wal commit error", "err", err)
	}
	b.commitCount.Add(1)
	b.log.Consumer("offset_committed", req.Group, req.Topic, req.Partition, req.Offset)
	return conn.SendOK(f.RequestID)
}

// handleAck handles CmdAck: acknowledges successful message processing.
func (b *Broker) handleAck(conn *networking.Conn, f *protocol.Frame) error {
	if !conn.IsAuthed() {
		return conn.SendError(f.RequestID, string(types.ErrUnauthorized), "authentication required")
	}
	var req protocol.AckRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid ack body")
	}
	b.offsets.Commit(req.ConsumerID, req.Topic, req.Partition, req.Offset)
	b.metrics.MessagesAcked.Inc(1)
	b.log.Consumer("ack", req.ConsumerID, req.Topic, req.Partition, req.Offset)
	return conn.SendOK(f.RequestID)
}

// handleNack handles CmdNack: negatively-acknowledges a message.
// On requeue=true the message is re-dispatched; on requeue=false it goes to DLQ.
func (b *Broker) handleNack(ctx context.Context, conn *networking.Conn, f *protocol.Frame) error {
	if !conn.IsAuthed() {
		return conn.SendError(f.RequestID, string(types.ErrUnauthorized), "authentication required")
	}
	var req protocol.NackRequest
	if err := protocol.Unmarshal(f, &req); err != nil {
		return conn.SendError(f.RequestID, "BAD_REQUEST", "invalid nack body")
	}

	tp, err := b.topics.Get(req.Topic)
	if err != nil {
		return conn.SendError(f.RequestID, string(types.ErrTopicNotFound), err.Error())
	}
	pl, err := tp.PartitionLog(req.Partition)
	if err != nil {
		return conn.SendError(f.RequestID, string(types.ErrPartitionNotFound), err.Error())
	}
	msgs, err := pl.Read(req.Offset, 1)
	if err != nil {
		return conn.SendError(f.RequestID, string(types.ErrInternal),
			fmt.Sprintf("read offset %d: %v", req.Offset, err))
	}

	var msg *types.Message
	if len(msgs) > 0 {
		msg = msgs[0]
	}
	b.metrics.MessagesNacked.Inc(1)

	if req.Requeue {
		if msg != nil {
			// Requeue must deliver ONLY to the originating group, not
			// fan out to every subscribed group. req.Group carries the
			// originating group ID from the client. If empty (legacy client),
			// fall back to the original fan-out Dispatch so old clients are not
			// silently broken.
			if req.Group != "" {
				b.consumers.DispatchToGroup(req.Group, req.Topic, req.Partition, msg)
			} else {
				b.consumers.Dispatch(msg)
			}
		}
		b.log.Consumer("nack_requeue", req.ConsumerID, req.Topic, req.Partition, req.Offset)
	} else {
		entry := consumer.DLQEntry{
			Reason:   fmt.Sprintf("nack from consumer %s", req.ConsumerID),
			Attempts: 1,
		}
		if msg != nil {
			entry.Original = msg
		}
		b.consumers.DLQ().Push(entry)
		b.log.Consumer("nack_dlq", req.ConsumerID, req.Topic, req.Partition, req.Offset)
	}
	return conn.SendOK(f.RequestID)
}
