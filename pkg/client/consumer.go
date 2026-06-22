package client

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	proto "github.com/Hoot-Code/pubsub-broker/pkg/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Consumer reads messages from a broker topic using server-push delivery.
// Obtain one via Client.NewConsumer.
type Consumer struct {
	c     *Client
	group string
	topic string
	id    string
	opts  consumerOptions

	// msgCh delivers pushed messages to callers.
	msgCh chan *types.Message

	closed   atomic.Bool
	closedCh chan struct{}
	once     sync.Once

	// routerID is the unique ID returned by registerPushRouter.
	// It is used to remove exactly this consumer's router when Close is called.
	routerID uint64

	// pushBuf holds messages received between router registration and
	// Subscribe() completion. This eliminates the race where CmdPush frames
	// (e.g. from new-consumer-group replay) arrive before the client has
	// registered its push router.
	pushBufMu sync.Mutex
	pushBuf   []*types.Message

	// subscribed indicates that Subscribe() has returned successfully and
	// pushBuf has been drained. When true, handlePush delivers directly to
	// msgCh instead of buffering.
	subscribed atomic.Bool
}

// consumerIDCounter generates unique consumer IDs within a process.
var consumerIDCounter atomic.Uint64

// NewConsumer returns a Consumer for the given group and topic.
// Call Subscribe to start receiving messages.
func (c *Client) NewConsumer(group, topic string, opts ...ConsumerOption) *Consumer {
	o := defaultConsumerOptions()
	for _, fn := range opts {
		fn(&o)
	}
	id := fmt.Sprintf("sdk-consumer-%d", consumerIDCounter.Add(1))
	return &Consumer{
		c:        c,
		group:    group,
		topic:    topic,
		id:       id,
		opts:     o,
		msgCh:    make(chan *types.Message, o.bufferSize),
		closedCh: make(chan struct{}),
	}
}

// Subscribe sends CmdSubscribe with Push:true and delivers arriving CmdPush
// frames to Messages().
//
// pushActive is incremented BEFORE the subscribe round-trip so that
// readLoop immediately clears the read deadline. If the increment happened
// after sendRecv returned there would be a window where readLoop could set a
// short readTimeout deadline, time out, and die before any push consumer was
// known to be active — breaking subsequent Publish calls on the same connection.
//
// The push router is registered BEFORE the subscribe round-trip so that
// CmdPush frames arriving during the round-trip (e.g. from new-consumer-group
// replay) are captured by our handler instead of being silently dropped.
// Frames received before sendRecv returns are buffered and drained
// afterward.
func (cs *Consumer) Subscribe(ctx context.Context) error {
	// Increment before the network round-trip so readLoop sees pushActive > 0
	// for the entire duration and keeps the read deadline clear.
	cs.c.pushActive.Add(1)

	// Initialize the pre-subscribe buffer.
	cs.pushBufMu.Lock()
	cs.pushBuf = cs.pushBuf[:0]
	cs.pushBufMu.Unlock()

	// Register the push router BEFORE the round-trip so that any CmdPush
	// frames (e.g. from new-consumer-group replay) that arrive before the
	// Subscribe OK response are captured rather than silently dropped.
	cs.routerID = cs.c.registerPushRouter(cs.handlePush)

	req := &proto.SubscribeRequest{
		Topic:      cs.topic,
		Group:      cs.group,
		ConsumerID: cs.id,
		Push:       true,
	}
	_, err := cs.c.sendRecv(ctx, proto.CmdSubscribe, req)
	if err != nil {
		cs.c.deregisterPushRouter(cs.routerID)
		cs.c.pushActive.Add(-1) // roll back on failure
		return fmt.Errorf("consumer subscribe: %w", err)
	}

	// Drain any buffered frames and mark as subscribed atomically.
	// The lock ensures no frame is buffered between drain and the
	// subscribed flag being set, preventing silent message loss.
	cs.pushBufMu.Lock()
	buf := cs.pushBuf
	cs.pushBuf = nil
	cs.subscribed.Store(true)
	cs.pushBufMu.Unlock()

	// Deliver buffered frames to msgCh after releasing the lock.
	// Since subscribed is now true, any concurrent handlePush delivers
	// directly to msgCh.
	for _, msg := range buf {
		select {
		case cs.msgCh <- msg:
		case <-cs.closedCh:
			return nil
		}
	}
	return nil
}

// handlePush is called by the client's readLoop for every CmdPush frame.
// Returns true only when topic, group AND consumerID all match,
// preventing cross-contamination between consumers in the same process.
//
// Before Subscribe() returns, incoming frames are buffered in pushBuf so
// that replay messages (which may arrive before the Subscribe OK response)
// are not silently dropped.
func (cs *Consumer) handlePush(f *proto.Frame) bool {
	if cs.closed.Load() {
		return false
	}
	var push proto.PushFrame
	if err := proto.Unmarshal(f, &push); err != nil {
		return false
	}
	// Require group and consumerID to match in addition to topic.
	if push.Topic != cs.topic || push.Group != cs.group || push.ConsumerID != cs.id {
		return false
	}

	if cs.subscribed.Load() {
		// Normal path: deliver directly to msgCh.
		for _, msg := range push.Messages {
			select {
			case cs.msgCh <- msg:
			case <-cs.closedCh:
				return true
			}
		}
		return true
	}

	// Pre-subscribe path: buffer frames until Subscribe() drains them.
	cs.pushBufMu.Lock()
	cs.pushBuf = append(cs.pushBuf, push.Messages...)
	cs.pushBufMu.Unlock()
	return true
}

// Messages returns the channel on which pushed messages are delivered.
// The channel is closed when Close is called.
func (cs *Consumer) Messages() <-chan *types.Message { return cs.msgCh }

// Commit sends CmdCommitOffset and blocks until the broker acknowledges.
func (cs *Consumer) Commit(ctx context.Context, partition int32, offset int64) error {
	req := &proto.CommitOffsetRequest{
		Group:      cs.group,
		ConsumerID: cs.id,
		Topic:      cs.topic,
		Partition:  partition,
		Offset:     offset,
	}
	_, err := cs.c.sendRecv(ctx, proto.CmdCommitOffset, req)
	if err != nil {
		return fmt.Errorf("consumer commit offset: %w", err)
	}
	return nil
}

// Close stops the consumer and closes the Messages() channel.
// Calls deregisterPushRouter with the stored ID (not a function pointer).
// Decrements pushActive so readLoop can reinstate the read deadline.
func (cs *Consumer) Close() error {
	cs.once.Do(func() {
		cs.closed.Store(true)
		close(cs.closedCh)
		close(cs.msgCh)
		// Use the stored routerID for O(1) removal.
		cs.c.deregisterPushRouter(cs.routerID)
		// Decrement push-active counter.
		cs.c.pushActive.Add(-1)
	})
	return nil
}

// SeekToTimestamp seeks the consumer group to the first offset whose record
// timestamp is >= timestampNs (Unix nanoseconds). A timestampNs of 0 resets
// to the beginning of each partition. Returns the new offset per partition.
func (cs *Consumer) SeekToTimestamp(ctx context.Context, timestampNs int64) (map[int32]int64, error) {
	req := &proto.SeekRequest{
		Topic:       cs.topic,
		Group:       cs.group,
		TimestampNs: timestampNs,
	}
	f, err := cs.c.sendRecv(ctx, proto.CmdSeek, req)
	if err != nil {
		return nil, fmt.Errorf("consumer seek to timestamp: %w", err)
	}
	var resp proto.SeekResponse
	if err := proto.Unmarshal(f, &resp); err != nil {
		return nil, fmt.Errorf("consumer seek response: %w", err)
	}
	return parseOffsetMap(resp.Offsets), nil
}

// SeekToEnd seeks the consumer group to the latest offset of each partition,
// so only subsequently published messages are received.
// Returns the new offset per partition.
func (cs *Consumer) SeekToEnd(ctx context.Context) (map[int32]int64, error) {
	req := &proto.SeekRequest{
		Topic: cs.topic,
		Group: cs.group,
		ToEnd: true,
	}
	f, err := cs.c.sendRecv(ctx, proto.CmdSeek, req)
	if err != nil {
		return nil, fmt.Errorf("consumer seek to end: %w", err)
	}
	var resp proto.SeekResponse
	if err := proto.Unmarshal(f, &resp); err != nil {
		return nil, fmt.Errorf("consumer seek-to-end response: %w", err)
	}
	return parseOffsetMap(resp.Offsets), nil
}

// SeekToOffset seeks the consumer group to the given absolute offset on every
// partition in the topic. An offset of -1 resets to the beginning.
// Returns the new effective offset per partition.
func (cs *Consumer) SeekToOffset(ctx context.Context, offset int64) (map[int32]int64, error) {
	req := &proto.SeekRequest{
		Topic:  cs.topic,
		Group:  cs.group,
		Offset: &offset,
	}
	f, err := cs.c.sendRecv(ctx, proto.CmdSeek, req)
	if err != nil {
		return nil, fmt.Errorf("consumer seek to offset: %w", err)
	}
	var resp proto.SeekResponse
	if err := proto.Unmarshal(f, &resp); err != nil {
		return nil, fmt.Errorf("consumer seek-to-offset response: %w", err)
	}
	return parseOffsetMap(resp.Offsets), nil
}

// Reset resets all committed offsets for this consumer group+topic to 0,
// so that the next fetch starts from the beginning of each partition.
func (cs *Consumer) Reset(ctx context.Context) error {
	req := &proto.ResetGroupRequest{
		Group: cs.group,
		Topic: cs.topic,
	}
	_, err := cs.c.sendRecv(ctx, proto.CmdResetGroup, req)
	if err != nil {
		return fmt.Errorf("consumer reset: %w", err)
	}
	return nil
}

// parseOffsetMap converts a map[string]int64 (partition-string → offset) to
// map[int32]int64 (partition-int32 → offset).
func parseOffsetMap(m map[string]int64) map[int32]int64 {
	out := make(map[int32]int64, len(m))
	for k, v := range m {
		var p int32
		fmt.Sscanf(k, "%d", &p)
		out[p] = v
	}
	return out
}
