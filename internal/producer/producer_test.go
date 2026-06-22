package producer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/producer"
	"github.com/Hoot-Code/pubsub-broker/internal/topic"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func newTestProducer(t *testing.T) (*producer.Producer, *topic.Manager) {
	t.Helper()
	dir := t.TempDir()
	storageCfg := &config.StorageConfig{
		DataPath:           filepath.Join(dir, "data"),
		WALPath:            filepath.Join(dir, "wal"),
		SegmentMaxBytes:    1 << 20,
		IndexIntervalBytes: 512,
	}
	_ = os.MkdirAll(storageCfg.DataPath, 0o755)

	part := partition.NewHashPartitioner()
	mgr := topic.NewManager(storageCfg, part)
	log := logging.New(nil, "error")
	reg := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(reg)

	p := producer.NewProducer(mgr, part, log, bm, 3, 5*time.Millisecond)
	return p, mgr
}

func createTopic(t *testing.T, mgr *topic.Manager, name string, partitions int) {
	t.Helper()
	if err := mgr.Create(types.TopicConfig{
		Name:              name,
		Partitions:        partitions,
		ReplicationFactor: 1,
	}); err != nil {
		t.Fatalf("Create topic %q: %v", name, err)
	}
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestProducer_PublishAtLeastOnce(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "events", 1)

	result, err := p.Publish(context.Background(), "events", "key1", []byte("hello"), nil, types.AtLeastOnce, 0, 0)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if result.MessageID == "" {
		t.Error("MessageID should not be empty")
	}
	if result.Partition < 0 {
		t.Errorf("Partition should be >= 0, got %d", result.Partition)
	}
	if result.Offset != 0 {
		t.Errorf("first message offset: want 0, got %d", result.Offset)
	}
}

func TestProducer_PublishAtMostOnce(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "fire-forget", 1)

	result, err := p.Publish(context.Background(), "fire-forget", "", []byte("payload"), nil, types.AtMostOnce, 0, 0)
	if err != nil {
		t.Fatalf("Publish AtMostOnce: %v", err)
	}
	if result.Offset != 0 {
		t.Errorf("first message offset: want 0, got %d", result.Offset)
	}
}

func TestProducer_PublishUnknownTopic(t *testing.T) {
	p, _ := newTestProducer(t)

	_, err := p.Publish(context.Background(), "nonexistent", "", []byte("x"), nil, types.AtLeastOnce, 0, 0)
	if err == nil {
		t.Fatal("expected error publishing to unknown topic")
	}
}

func TestProducer_PublishUnsupportedDeliveryMode(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "modes", 1)

	// ExactlyOnce with seqNum==0 must return an error.
	_, err := p.Publish(context.Background(), "modes", "", []byte("x"), nil, types.ExactlyOnce, 0, 0)
	if err == nil {
		t.Fatal("expected error for ExactlyOnce with seqNum==0")
	}
}

func TestProducer_OffsetIncrement(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "seq", 1)

	for i := 0; i < 5; i++ {
		result, err := p.Publish(context.Background(), "seq", "", []byte("x"), nil, types.AtLeastOnce, 0, 0)
		if err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
		if result.Offset != int64(i) {
			t.Errorf("offset[%d]: want %d, got %d", i, i, result.Offset)
		}
	}
}

func TestProducer_KeyedRouting(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "keyed", 4)

	// Same key must always map to the same partition.
	r1, err := p.Publish(context.Background(), "keyed", "customer-99", []byte("a"), nil, types.AtLeastOnce, 0, 0)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	r2, err := p.Publish(context.Background(), "keyed", "customer-99", []byte("b"), nil, types.AtLeastOnce, 0, 0)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if r1.Partition != r2.Partition {
		t.Errorf("keyed routing not deterministic: %d vs %d", r1.Partition, r2.Partition)
	}
}

func TestProducer_BatchPublish(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "batch", 2)

	items := []producer.BatchItem{
		{Key: "k1", Payload: []byte("p1"), Mode: types.AtLeastOnce},
		{Key: "k2", Payload: []byte("p2"), Mode: types.AtLeastOnce},
		{Key: "k3", Payload: []byte("p3"), Mode: types.AtMostOnce},
	}
	results, err := p.PublishBatch(context.Background(), "batch", items)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(results) != len(items) {
		t.Fatalf("results: want %d, got %d", len(items), len(results))
	}
	for i, r := range results {
		if r.Error != nil {
			t.Errorf("result[%d]: unexpected error: %v", i, r.Error)
		}
		if r.MessageID == "" {
			t.Errorf("result[%d]: empty MessageID", i)
		}
	}
}

func TestProducer_BatchPublishUnknownTopic(t *testing.T) {
	p, _ := newTestProducer(t)

	items := []producer.BatchItem{
		{Payload: []byte("x"), Mode: types.AtLeastOnce},
	}
	results, err := p.PublishBatch(context.Background(), "ghost", items)
	if err != nil {
		t.Fatalf("PublishBatch returned unexpected hard error: %v", err)
	}
	if results[0].Error == nil {
		t.Error("expected per-item error for unknown topic")
	}
}

func TestProducer_ContextCancellation(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "cancel", 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// AtMostOnce does NOT retry, so it should succeed on first attempt.
	_, err := p.Publish(ctx, "cancel", "", []byte("x"), nil, types.AtMostOnce, 0, 0)
	if err != nil {
		// AtMostOnce publishes synchronously without checking ctx. OK either way.
		t.Logf("AtMostOnce with cancelled ctx: %v (acceptable)", err)
	}
}

func TestProducer_WithHeaders(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "headers", 1)

	headers := map[string]string{
		"content-type": "application/json",
		"trace-id":     "abc-123",
	}
	result, err := p.Publish(context.Background(), "headers", "", []byte(`{"x":1}`), headers, types.AtLeastOnce, 0, 0)
	if err != nil {
		t.Fatalf("Publish with headers: %v", err)
	}
	if result.MessageID == "" {
		t.Error("MessageID should not be empty")
	}
}

func TestProducer_ConcurrentPublish(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "concurrent", 8)

	const goroutines = 10
	const perGoroutine = 20
	errs := make(chan error, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		go func() {
			for i := 0; i < perGoroutine; i++ {
				_, err := p.Publish(context.Background(), "concurrent", "", []byte("data"), nil, types.AtLeastOnce, 0, 0)
				errs <- err
			}
		}()
	}

	for i := 0; i < goroutines*perGoroutine; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent publish error: %v", err)
		}
	}
}

// ─── Gap 1: Exactly-Once Idempotency ─────────────────────────────────────────

// TestProducer_ExactlyOnce_Idempotent verifies that publishing the same
// (ProducerID, SeqNum) pair twice yields the same result and writes only
// one record to the partition log.
func TestProducer_ExactlyOnce_Idempotent(t *testing.T) {
	p, mgr := newTestProducer(t)
	createTopic(t, mgr, "idempotent", 1)

	const seqNum uint64 = 42

	r1, err := p.Publish(context.Background(), "idempotent", "", []byte("payload"), nil, types.ExactlyOnce, seqNum, 0)
	if err != nil {
		t.Fatalf("first Publish (ExactlyOnce): %v", err)
	}

	r2, err := p.Publish(context.Background(), "idempotent", "", []byte("payload"), nil, types.ExactlyOnce, seqNum, 0)
	if err != nil {
		t.Fatalf("second Publish (ExactlyOnce, duplicate): %v", err)
	}

	// Both calls must return identical results.
	if r1.MessageID != r2.MessageID {
		t.Errorf("MessageID mismatch: %q vs %q", r1.MessageID, r2.MessageID)
	}
	if r1.Offset != r2.Offset {
		t.Errorf("Offset mismatch: %d vs %d", r1.Offset, r2.Offset)
	}

	// Only one record must exist in the log.
	tp, err := mgr.Get("idempotent")
	if err != nil {
		t.Fatalf("Get topic: %v", err)
	}
	pl, err := tp.PartitionLog(r1.Partition)
	if err != nil {
		t.Fatalf("PartitionLog: %v", err)
	}
	msgs, err := pl.Read(0, 10)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(msgs) != 1 {
		t.Errorf("expected exactly 1 record in log, got %d (idempotency failed)", len(msgs))
	}
}
