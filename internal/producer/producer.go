// Package producer implements the publish side of the broker.
package producer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/compress"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/internal/topic"
	"github.com/Hoot-Code/pubsub-broker/internal/tracing"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── seqKey ──────────────────────────────────────────────────────────────────

// seqKey is the composite key used by the idempotency store.
type seqKey struct {
	producerID string
	seqNum     uint64
}

// PublishResult describes the result of a single publish operation.
type PublishResult struct {
	MessageID string
	Partition int32
	Offset    int64
	Error     error
}

// Producer handles message publishing for the broker.
type Producer struct {
	// ProducerID is a unique identifier for this producer instance,
	// used for exactly-once idempotency tracking.
	ProducerID       string
	idempotencyStore sync.Map // seqKey → *PublishResult (legacy; kept for backward compat)

	// dedup provides bounded, ring-buffer-based deduplication for exactly-once
	// producers. It replaces the unbounded idempotencyStore for new seqNum paths.
	dedup *DedupWindow

	topics      *topic.Manager
	partitioner *partition.HashPartitioner
	log         *logging.Logger
	metrics     *metrics.Broker
	maxRetries  int
	retryDelay  time.Duration
	// tracer is used to emit "producer.write" child spans (Part D3).
	// May be nil if tracing is not configured.
	tracer *tracing.Tracer
}

// SetTracer attaches a Tracer to the producer so that each Publish call
// emits a "producer.write" child span (Part D3).
func (p *Producer) SetTracer(t *tracing.Tracer) { p.tracer = t }

// NewProducer creates a Producer.
func NewProducer(
	topics *topic.Manager,
	partitioner *partition.HashPartitioner,
	log *logging.Logger,
	m *metrics.Broker,
	maxRetries int,
	retryDelay time.Duration,
) *Producer {
	return &Producer{
		ProducerID:  fmt.Sprintf("producer-%s", uuid()),
		dedup:       NewDedupWindow(10_000),
		topics:      topics,
		partitioner: partitioner,
		log:         log,
		metrics:     m,
		maxRetries:  maxRetries,
		retryDelay:  retryDelay,
	}
}

// Publish stores a single message and returns its assigned partition+offset.
// seqNum is required when mode is ExactlyOnce; pass 0 for other modes.
// clientID is used for ExactlyOnce deduplication; pass "" for other modes.
// codecOverride, when non-zero, overrides the topic-level compression codec.
func (p *Producer) Publish(ctx context.Context, topicName, key string, payload []byte, headers map[string]string, mode types.DeliveryMode, seqNum uint64, codecOverride uint8, clientID ...string) (*PublishResult, error) {
	start := time.Now()

	tp, err := p.topics.Get(topicName)
	if err != nil {
		return nil, err
	}

	partID, err := p.partitioner.Assign(topicName, key)
	if err != nil {
		return nil, err
	}

	pl, err := tp.PartitionLog(partID)
	if err != nil {
		return nil, err
	}

	msg := types.NewMessage(topicName, payload, key, headers)
	msg.Partition = partID

	// ── Tracing: producer.write child span (Part D3) ──────────────────────
	if p.tracer != nil {
		var pSp *tracing.Span
		ctx, pSp = p.tracer.Start(ctx, "producer.write",
			"topic", topicName,
			"partition", fmt.Sprintf("%d", partID),
		)
		defer pSp.End()
	}

	// ── Compression (B3) ─────────────────────────────────────────────────
	// Use the per-message codec if provided (non-zero), otherwise fall back
	// to the topic-level default.
	codecToUse := compress.Codec(tp.Config().CompressionCodec)
	if codecOverride > 0 {
		codecToUse = compress.Codec(codecOverride)
	}
	if codecToUse != compress.CodecNone {
		compressed, cErr := compress.Compress(codecToUse, payload)
		if cErr != nil {
			return nil, fmt.Errorf("compress payload: %w", cErr)
		}
		msg.Payload = compressed
		msg.Codec = uint8(codecToUse)
	}

	var offset int64
	switch mode {
	case types.AtMostOnce:
		// Best-effort: attempt once, no retry on failure.
		offset, err = pl.Append(msg)
		if err != nil {
			p.metrics.MessagesErrored.Inc(1)
			return nil, fmt.Errorf("at-most-once publish: %w", err)
		}

	case types.AtLeastOnce:
		// Retry with exponential backoff.
		offset, err = p.publishWithRetry(ctx, pl, msg)
		if err != nil {
			p.metrics.MessagesErrored.Inc(1)
			return nil, err
		}

	case types.ExactlyOnce:
		// Idempotent producer: deduplicate by (clientID, SeqNum) using the
		// bounded ring-buffer DedupWindow (Part E / Fix E3).
		if seqNum == 0 {
			return nil, fmt.Errorf("exactly-once publish: seqNum required (must be > 0)")
		}
		cid := p.ProducerID
		if len(clientID) > 0 && clientID[0] != "" {
			cid = clientID[0]
		}

		legacyKey := seqKey{producerID: cid, seqNum: seqNum}

		// Check ring-buffer window first (bounded memory, Part E).
		if _, inWindow := p.dedup.LookupOffset(cid, seqNum); inWindow {
			// Duplicate detected: return the full cached result from idempotencyStore
			// so that the MessageID is preserved for callers that rely on it.
			if cached, ok := p.idempotencyStore.Load(legacyKey); ok {
				return cached.(*PublishResult), nil
			}
			// Fallback when idempotencyStore entry was evicted; return with offset only.
			off, _ := p.dedup.LookupOffset(cid, seqNum)
			return &PublishResult{Partition: partID, Offset: off}, nil
		}
		// Legacy sync.Map check for backward compat.
		if cached, ok := p.idempotencyStore.Load(legacyKey); ok {
			return cached.(*PublishResult), nil
		}

		// First delivery: write and cache.
		offset, err = p.publishWithRetry(ctx, pl, msg)
		if err != nil {
			p.metrics.MessagesErrored.Inc(1)
			return nil, err
		}
		result := &PublishResult{
			MessageID: msg.ID,
			Partition: partID,
			Offset:    offset,
		}
		msg.Offset = offset
		p.metrics.MessagesPublished.Inc(1)
		p.metrics.MessageSizeBytes.Observe(float64(len(payload)))
		elapsed := float64(time.Since(start).Milliseconds())
		p.metrics.PublishLatencyMs.Observe(elapsed)
		p.log.Debug("message published",
			"topic", topicName,
			"partition", partID,
			"offset", offset,
			"size", len(payload),
			"mode", mode,
		)
		// Record in both stores.
		p.dedup.Mark(cid, seqNum, offset)
		p.idempotencyStore.Store(legacyKey, result)
		return result, nil

	default:
		return nil, fmt.Errorf("unsupported delivery mode: %s", mode)
	}

	msg.Offset = offset
	p.metrics.MessagesPublished.Inc(1)
	p.metrics.MessageSizeBytes.Observe(float64(len(payload)))

	elapsed := float64(time.Since(start).Milliseconds())
	p.metrics.PublishLatencyMs.Observe(elapsed)

	p.log.Debug("message published",
		"topic", topicName,
		"partition", partID,
		"offset", offset,
		"size", len(payload),
		"mode", mode,
	)

	return &PublishResult{
		MessageID: msg.ID,
		Partition: partID,
		Offset:    offset,
	}, nil
}

// PurgeIdempotencyStore removes all entries whose seqNum is strictly less than
// olderThanSeq. The broker may call this periodically to bound memory usage.
func (p *Producer) PurgeIdempotencyStore(olderThanSeq uint64) {
	p.idempotencyStore.Range(func(k, _ any) bool {
		if k.(seqKey).seqNum < olderThanSeq {
			p.idempotencyStore.Delete(k)
		}
		return true
	})
}

// PublishBatch stores multiple messages, all to the same topic.
// Returns one PublishResult per input message. Partial failures are included.
func (p *Producer) PublishBatch(ctx context.Context, topicName string, requests []BatchItem) ([]PublishResult, error) {
	results := make([]PublishResult, len(requests))
	for i, req := range requests {
		r, err := p.Publish(ctx, topicName, req.Key, req.Payload, req.Headers, req.Mode, req.SeqNum, req.Codec)
		if err != nil {
			results[i] = PublishResult{Error: err}
		} else {
			results[i] = *r
		}
	}
	return results, nil
}

// BatchItem is a single item in a batch publish.
type BatchItem struct {
	Key     string
	Payload []byte
	Headers map[string]string
	Mode    types.DeliveryMode
	// SeqNum is required when Mode is ExactlyOnce; ignored otherwise.
	SeqNum uint64
	// Codec overrides the topic-level compression for this item.
	Codec uint8
}

func (p *Producer) publishWithRetry(ctx context.Context, pl *storage.PartitionLog, msg *types.Message) (int64, error) {
	delay := p.retryDelay
	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		offset, err := pl.Append(msg)
		if err == nil {
			return offset, nil
		}
		lastErr = err
		if attempt < p.maxRetries {
			p.log.Warn("publish retry",
				"attempt", attempt+1,
				"max", p.maxRetries,
				"delay", delay,
				"err", err,
			)
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return 0, ctx.Err()
			case <-timer.C:
			}
			delay *= 2 // exponential backoff
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
	return 0, fmt.Errorf("publish failed after %d attempts: %w", p.maxRetries+1, lastErr)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

// uuid generates a cryptographically random hex string.
func uuid() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b[:])
}
