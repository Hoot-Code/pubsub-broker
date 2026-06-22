package producer_test

import (
	"context"
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

// ─── DedupWindow unit tests ──────────────────────────────────────────────────

// TestDedupWindow_IsDuplicate verifies that the same (clientID, seqNum) pair
// is detected as a duplicate and the original offset is returned.
func TestDedupWindow_IsDuplicate(t *testing.T) {
	t.Parallel()
	d := producer.NewDedupWindow(100)

	const (
		client = "producer-A"
		seq    = uint64(42)
		offset = int64(7)
	)

	// Not seen yet.
	if d.IsDuplicate(client, seq) {
		t.Fatal("seqNum not yet marked should not be a duplicate")
	}

	d.Mark(client, seq, offset)

	// Now it is a duplicate.
	if !d.IsDuplicate(client, seq) {
		t.Fatal("marked seqNum should be a duplicate")
	}

	// Offset is preserved.
	got, ok := d.LookupOffset(client, seq)
	if !ok {
		t.Fatal("marked seqNum offset not found")
	}
	if got != offset {
		t.Errorf("offset: want %d, got %d", offset, got)
	}
}

// TestDedupWindow_Eviction verifies that once the window fills beyond `size`
// entries the oldest entry is evicted, so a re-send of that entry is treated
// as a new message (not a duplicate).
func TestDedupWindow_Eviction(t *testing.T) {
	t.Parallel()
	const windowSize = 10
	d := producer.NewDedupWindow(windowSize)

	const client = "producer-B"

	// Fill the window with seqNums 1..windowSize.
	for i := uint64(1); i <= windowSize; i++ {
		d.Mark(client, i, int64(i*100))
	}

	// All entries should still be present.
	for i := uint64(1); i <= windowSize; i++ {
		if !d.IsDuplicate(client, i) {
			t.Errorf("seqNum %d should still be in the window", i)
		}
	}

	// Add windowSize+1-th entry — seqNum 1 should be evicted (oldest).
	d.Mark(client, windowSize+1, int64((windowSize+1)*100))

	// seqNum 1 is evicted; re-sending it should NOT be a duplicate.
	if d.IsDuplicate(client, 1) {
		t.Error("seqNum 1 should have been evicted; re-send should not be duplicate")
	}

	// All others (2..windowSize+1) should still be present.
	for i := uint64(2); i <= windowSize+1; i++ {
		if !d.IsDuplicate(client, i) {
			t.Errorf("seqNum %d should still be in window", i)
		}
	}
}

// TestDedupWindow_MultipleClients verifies independent windows per client.
func TestDedupWindow_MultipleClients(t *testing.T) {
	t.Parallel()
	d := producer.NewDedupWindow(5)

	d.Mark("client-X", 1, 100)
	d.Mark("client-Y", 1, 200)

	xOff, _ := d.LookupOffset("client-X", 1)
	yOff, _ := d.LookupOffset("client-Y", 1)

	if xOff != 100 || yOff != 200 {
		t.Errorf("client offsets: X want 100 got %d, Y want 200 got %d", xOff, yOff)
	}
}

// ─── End-to-end: DedupWindow via producer.Publish ───────────────────────────

func newDedupProducer(t *testing.T) (*producer.Producer, string) {
	t.Helper()
	dir := t.TempDir()
	reg := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(reg)
	log := logging.New(nil, "error")
	part := partition.NewHashPartitioner()
	storageCfg := &config.StorageConfig{
		DataPath:           filepath.Join(dir, "data"),
		SegmentMaxBytes:    1 << 20,
		IndexIntervalBytes: 512,
	}
	topicMgr := topic.NewManager(storageCfg, part)
	_ = topicMgr.Create(types.TopicConfig{Name: "dedup-test", Partitions: 1, ReplicationFactor: 1})
	p := producer.NewProducer(topicMgr, part, log, bm, 1, time.Millisecond)
	return p, "dedup-test"
}

// TestProducer_ExactlyOnce_NoDuplicate verifies that publishing the same
// (clientID, seqNum) twice returns the same offset without writing a new
// segment entry.
func TestProducer_ExactlyOnce_NoDuplicate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p, topicName := newDedupProducer(t)

	// First publish — should succeed and return offset.
	r1, err := p.Publish(ctx, topicName, "k1", []byte("payload"), nil,
		types.ExactlyOnce, 1, 0, "client-eo")
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}

	// Second publish with same seqNum — must return same offset, no new entry.
	r2, err := p.Publish(ctx, topicName, "k1", []byte("payload"), nil,
		types.ExactlyOnce, 1, 0, "client-eo")
	if err != nil {
		t.Fatalf("second publish (duplicate): %v", err)
	}
	if r1.Offset != r2.Offset {
		t.Errorf("duplicate seqNum: want same offset %d, got %d", r1.Offset, r2.Offset)
	}
	if r1.Partition != r2.Partition {
		t.Errorf("duplicate seqNum: want same partition %d, got %d", r1.Partition, r2.Partition)
	}

	// Third publish with a new seqNum — must be a distinct entry.
	r3, err := p.Publish(ctx, topicName, "k1", []byte("payload2"), nil,
		types.ExactlyOnce, 2, 0, "client-eo")
	if err != nil {
		t.Fatalf("third publish (new seqNum): %v", err)
	}
	if r3.Offset == r1.Offset {
		t.Errorf("new seqNum should have a different offset; got same %d", r3.Offset)
	}
}
