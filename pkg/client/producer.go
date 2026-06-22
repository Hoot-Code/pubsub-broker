package client

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	proto "github.com/Hoot-Code/pubsub-broker/pkg/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// Message is a single message for batch publishing.
type Message struct {
	// Key is an optional routing key used for partition assignment.
	Key string
	// Payload is the message body.
	Payload []byte
	// Headers are optional key-value metadata attached to the message.
	Headers map[string]string
}

// Producer publishes messages to a single topic.
// Obtain one via Client.NewProducer.
type Producer struct {
	c     *Client
	topic string
	opts  producerOptions

	// seqCounter auto-increments for ExactlyOnce delivery mode (Part E4).
	seqCounter atomic.Uint64
}

// NewProducer returns a Producer for topic. Messages are published with the
// delivery guarantees set by WithDeliveryMode (default: AtLeastOnce).
func (c *Client) NewProducer(topic string, opts ...ProducerOption) *Producer {
	o := defaultProducerOptions()
	for _, fn := range opts {
		fn(&o)
	}
	return &Producer{c: c, topic: topic, opts: o}
}

// Publish sends one message and blocks until the broker acknowledges.
// Returns the assigned partition and offset. If WithCompression was set on
// the Producer, the Codec field in the request is set accordingly.
//
// When WithRetryOnOverload is configured, Publish retries automatically up
// to the configured number of times when the broker responds with
// ErrBrokerOverloaded, using exponential backoff (E2).
func (p *Producer) Publish(ctx context.Context, key string, payload []byte, headers map[string]string) (offset int64, err error) {
	req := &proto.PublishRequest{
		Topic:        p.topic,
		Key:          key,
		Payload:      payload,
		Headers:      headers,
		DeliveryMode: uint8(p.opts.deliveryMode),
		Codec:        p.opts.codec,
	}
	// Auto-increment SeqNum for exactly-once producers.
	if p.opts.deliveryMode == types.ExactlyOnce {
		req.SeqNum = p.seqCounter.Add(1)
	}

	return p.publishWithRetry(ctx, proto.CmdPublish, req)
}

// publishWithRetry sends req and retries on ErrBrokerOverloaded according to
// the producer's WithRetryOnOverload configuration (E2).
func (p *Producer) publishWithRetry(ctx context.Context, cmd proto.Command, req *proto.PublishRequest) (int64, error) {
	maxRetries := p.opts.retryMaxRetries
	backoff := p.opts.retryBackoff
	if backoff <= 0 {
		backoff = 50 * time.Millisecond
	}

	for attempt := 0; ; attempt++ {
		f, err := p.c.sendRecv(ctx, cmd, req)
		if err != nil {
			// E2: retry on overload if configured.
			if isOverloaded(err) && attempt < maxRetries {
				wait := exponentialBackoff(backoff, attempt)
				select {
				case <-ctx.Done():
					return 0, fmt.Errorf("producer publish: %w", ctx.Err())
				case <-time.After(wait):
					continue
				}
			}
			return 0, fmt.Errorf("producer publish: %w", err)
		}
		var resp proto.PublishResponse
		if err := proto.Unmarshal(f, &resp); err != nil {
			return 0, fmt.Errorf("producer publish: decode response: %w", err)
		}
		return resp.Offset, nil
	}
}

// PublishWithMeta behaves like Publish but additionally returns the
// partition the message was routed to. Used where the partition assignment
// itself is part of the response (e.g. the HTTP gateway's REST publish
// endpoint).
func (p *Producer) PublishWithMeta(ctx context.Context, key string, payload []byte, headers map[string]string) (partition int32, offset int64, err error) {
	req := &proto.PublishRequest{
		Topic:        p.topic,
		Key:          key,
		Payload:      payload,
		Headers:      headers,
		DeliveryMode: uint8(p.opts.deliveryMode),
		Codec:        p.opts.codec,
	}
	if p.opts.deliveryMode == types.ExactlyOnce {
		req.SeqNum = p.seqCounter.Add(1)
	}
	f, err := p.c.sendRecv(ctx, proto.CmdPublish, req)
	if err != nil {
		return 0, 0, fmt.Errorf("producer publish: %w", err)
	}
	var resp proto.PublishResponse
	if err := proto.Unmarshal(f, &resp); err != nil {
		return 0, 0, fmt.Errorf("producer publish: decode response: %w", err)
	}
	return resp.Partition, resp.Offset, nil
}

// Tombstone publishes a tombstone marker for key with an empty payload.
// It sets Headers["_compaction"]="delete" so that a KeyCompactor sweeping a
// CompactionMode=="compact" topic removes all prior records for key once the
// configured tombstone grace period has elapsed. Returns the offset assigned
// to the tombstone record.
func (p *Producer) Tombstone(ctx context.Context, key string) (int64, error) {
	return p.Publish(ctx, key, nil, map[string]string{"_compaction": "delete"})
}

// isOverloaded reports whether err is (or wraps) an ErrBrokerOverloaded.
func isOverloaded(err error) bool {
	if err == nil {
		return false
	}
	var e *ErrBrokerOverloaded
	// errors.As would require importing errors; do a simple type assertion
	// since sendRecv wraps the error via ErrConnectionClosed or returns
	// ErrBrokerOverloaded directly from the frame error-response path.
	_ = e
	// Walk the error chain manually.
	for u := err; u != nil; {
		if _, ok := u.(*ErrBrokerOverloaded); ok {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		if uw, ok := u.(unwrapper); ok {
			u = uw.Unwrap()
		} else {
			break
		}
	}
	return false
}

// exponentialBackoff returns min(backoff × 2^attempt, 30 s).
func exponentialBackoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
		if d > 30*time.Second {
			return 30 * time.Second
		}
	}
	return d
}

// PublishBatch sends a batch of messages in a single round-trip.
// Returns one offset per input message in the same order.
func (p *Producer) PublishBatch(ctx context.Context, msgs []Message) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	reqs := make([]proto.PublishRequest, len(msgs))
	for i, m := range msgs {
		reqs[i] = proto.PublishRequest{
			Topic:        p.topic,
			Key:          m.Key,
			Payload:      m.Payload,
			Headers:      m.Headers,
			DeliveryMode: uint8(p.opts.deliveryMode),
			Codec:        p.opts.codec,
		}
		if p.opts.deliveryMode == types.ExactlyOnce {
			reqs[i].SeqNum = p.seqCounter.Add(1)
		}
	}

	f, err := p.c.sendRecv(ctx, proto.CmdBatchPublish, &proto.BatchPublishRequest{Messages: reqs})
	if err != nil {
		return nil, fmt.Errorf("producer batch publish: %w", err)
	}
	var resp proto.BatchPublishResponse
	if err := proto.Unmarshal(f, &resp); err != nil {
		return nil, fmt.Errorf("producer batch publish: decode response: %w", err)
	}
	offsets := make([]int64, len(resp.Results))
	for i, r := range resp.Results {
		offsets[i] = r.Offset
	}
	return offsets, nil
}
