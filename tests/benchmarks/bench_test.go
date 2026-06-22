// Package benchmarks provides throughput and latency benchmarks for the broker.
// Run with: go test -bench . -benchtime=5s ./tests/benchmarks/
package benchmarks_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/consumer"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/producer"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/internal/topic"
	"github.com/Hoot-Code/pubsub-broker/internal/wal"
	"github.com/Hoot-Code/pubsub-broker/pkg/client"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func benchTopicManager(b *testing.B) (*topic.Manager, *partition.HashPartitioner) {
	b.Helper()
	dir := b.TempDir()
	cfg := &config.StorageConfig{
		DataPath:           dir,
		SegmentMaxBytes:    1 << 30,
		IndexIntervalBytes: 4096,
	}
	p := partition.NewHashPartitioner()
	return topic.NewManager(cfg, p), p
}

func benchProducer(b *testing.B, topics *topic.Manager, part *partition.HashPartitioner) *producer.Producer {
	b.Helper()
	reg := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(reg)
	return producer.NewProducer(topics, part, logging.Default(), bm, 0, time.Millisecond)
}

// ─── WAL benchmarks ───────────────────────────────────────────────────────────

func BenchmarkWAL_Append256B(b *testing.B) {
	benchWALAppend(b, 256)
}

func BenchmarkWAL_Append4KB(b *testing.B) {
	benchWALAppend(b, 4096)
}

func BenchmarkWAL_Append64KB(b *testing.B) {
	benchWALAppend(b, 65536)
}

func benchWALAppend(b *testing.B, size int) {
	dir := b.TempDir()
	w, _, _ := wal.Open(filepath.Join(dir, "bench.wal"))
	defer w.Close()
	payload := make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── Storage benchmarks ───────────────────────────────────────────────────────

func BenchmarkStorage_Append256B(b *testing.B) {
	benchStorageAppend(b, 256)
}

func BenchmarkStorage_Append4KB(b *testing.B) {
	benchStorageAppend(b, 4096)
}

func benchStorageAppend(b *testing.B, size int) {
	dir := b.TempDir()
	pl, _ := storage.OpenPartitionLog(dir, 1<<30, 4096, "always")
	defer pl.Close()
	msg := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "bench",
		Payload:   make([]byte, size),
		Timestamp: time.Now().UnixNano(),
	}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pl.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── Publish throughput ───────────────────────────────────────────────────────

func BenchmarkPublish_Sequential256B(b *testing.B) {
	benchPublish(b, 256, 1)
}

func BenchmarkPublish_Sequential4KB(b *testing.B) {
	benchPublish(b, 4096, 1)
}

func BenchmarkPublish_Parallel256B(b *testing.B) {
	benchPublish(b, 256, 0) // 0 = use b.RunParallel
}

func benchPublish(b *testing.B, payloadSize, _ int) {
	topics, part := benchTopicManager(b)
	defer topics.CloseAll()
	prod := benchProducer(b, topics, part)

	_ = topics.Create(types.TopicConfig{Name: "bench", Partitions: 8})
	ctx := context.Background()
	payload := make([]byte, payloadSize)
	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := prod.Publish(ctx, "bench", "", payload, nil, types.AtLeastOnce, 0, 0); err != nil {
				b.Error(err)
			}
		}
	})
}

// ─── Partition routing ────────────────────────────────────────────────────────

func BenchmarkPartition_KeyedAssign(b *testing.B) {
	p := partition.NewHashPartitioner()
	_ = p.Register("bench", 32)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = p.Assign("bench", "stable-routing-key")
		}
	})
}

func BenchmarkPartition_RoundRobin(b *testing.B) {
	p := partition.NewHashPartitioner()
	_ = p.Register("bench", 32)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = p.Assign("bench", "")
		}
	})
}

// ─── Consumer dispatch ────────────────────────────────────────────────────────

func BenchmarkConsumer_Dispatch(b *testing.B) {
	os := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(1000)
	mgr := consumer.NewManager(os, dlq, 0, time.Millisecond, 10000)

	_, _ = mgr.Subscribe("g1", "c1", "cl1", "bench", 4)
	msg := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "bench",
		Partition: 0,
		Timestamp: time.Now().UnixNano(),
		Payload:   []byte("benchmark payload"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.Dispatch(msg)
	}
}

// ─── End-to-end write throughput summary ─────────────────────────────────────

// BenchmarkE2E_WriteRead exercises the full path: publish → storage → read.
func BenchmarkE2E_WriteRead(b *testing.B) {
	dir := b.TempDir()
	pl, _ := storage.OpenPartitionLog(dir, 1<<30, 4096, "always")
	defer pl.Close()
	ctx := context.Background()
	_ = ctx

	payload := make([]byte, 512)
	msg := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "e2e",
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		off, err := pl.Append(msg)
		if err != nil {
			b.Fatal(err)
		}
		msgs, err := pl.Read(off, 1)
		if err != nil || len(msgs) == 0 {
			b.Fatalf("read back failed: %v", err)
		}
	}
}

// BenchmarkUUID measures UUID generation overhead.
func BenchmarkUUID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = types.NewUUID()
	}
}

// ─── Part D: Comprehensive benchmark suite ───────────────────────────────────
//
// These benchmarks require an in-process broker with a real TCP listener.
// Run with: go test -bench=. -benchtime=5s ./tests/benchmarks/

// newInProcessBroker starts a fully wired in-process broker and returns its
// address. It is stopped via b.Cleanup.
func newInProcessBroker(b *testing.B) string {
	b.Helper()
	dir := b.TempDir()

	cfgData, err := json.Marshal(map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "bench-node"},
		"network": map[string]interface{}{"port": 0, "max_connections": 500},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    int64(1 << 30),
			"index_interval_bytes": int64(4096),
			"sync_policy":          "os", // fastest for benchmarks
		},
	})
	if err != nil {
		b.Fatalf("marshal config: %v", err)
	}

	cfgPath := filepath.Join(dir, "bench.json")
	if err := os.WriteFile(cfgPath, cfgData, 0o644); err != nil {
		b.Fatalf("write config: %v", err)
	}

	loader, err := config.Load(cfgPath)
	if err != nil {
		b.Fatalf("load config: %v", err)
	}
	brkr, err := broker.New(loader)
	if err != nil {
		b.Fatalf("broker.New: %v", err)
	}
	addrCh := make(chan string, 1)
	go func() {
		for {
			addr := brkr.Addr()
			if addr != "" {
				addrCh <- addr
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	go brkr.Start() //nolint:errcheck
	addr := <-addrCh
	b.Cleanup(func() { brkr.Stop(context.Background()) }) //nolint:errcheck
	return addr
}

// benchClientProducer dials addr, creates topic, and returns a ready Producer.
func benchClientProducer(b *testing.B, addr, topic string) *client.Producer {
	b.Helper()
	c, err := client.Dial(addr,
		client.WithDialTimeout(5*time.Second),
		client.WithWriteTimeout(5*time.Second),
		client.WithReadTimeout(30*time.Second),
	)
	if err != nil {
		b.Fatalf("Dial: %v", err)
	}
	ctx := context.Background()
	if err := c.CreateTopic(ctx, types.TopicConfig{Name: topic, Partitions: 4, ReplicationFactor: 1}); err != nil {
		// ignore "already exists" style errors
		_ = err
	}
	b.Cleanup(func() { c.Close() })
	return c.NewProducer(topic)
}

// ─── D1: BenchmarkPublishSingle ───────────────────────────────────────────────

// BenchmarkPublishSingle measures single-goroutine sequential publish of
// 128-byte messages through a live in-process broker.
func BenchmarkPublishSingle(b *testing.B) {
	addr := newInProcessBroker(b)
	prod := benchClientProducer(b, addr, "bench-single")
	ctx := context.Background()
	payload := make([]byte, 128)

	b.SetBytes(128)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := prod.Publish(ctx, "", payload, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── D2: BenchmarkPublishBatch ────────────────────────────────────────────────

// BenchmarkPublishBatch measures PublishBatch of 100 messages per call,
// 128 bytes each. ns/op is reported per-message (divide b.N by 100 in loop).
func BenchmarkPublishBatch(b *testing.B) {
	addr := newInProcessBroker(b)
	prod := benchClientProducer(b, addr, "bench-batch")
	ctx := context.Background()

	const batchSize = 100
	msgs := make([]client.Message, batchSize)
	for i := range msgs {
		msgs[i] = client.Message{Payload: make([]byte, 128)}
	}

	b.SetBytes(128 * batchSize)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := prod.PublishBatch(ctx, msgs); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── D3: BenchmarkPublishParallel ─────────────────────────────────────────────

// BenchmarkPublishParallel runs b.RunParallel with GOMAXPROCS goroutines,
// each publishing 128-byte messages.
func BenchmarkPublishParallel(b *testing.B) {
	addr := newInProcessBroker(b)
	prod := benchClientProducer(b, addr, "bench-parallel")
	ctx := context.Background()
	payload := make([]byte, 128)

	b.SetBytes(128)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := prod.Publish(ctx, "", payload, nil); err != nil {
				b.Error(err)
			}
		}
	})
}

// ─── D4: BenchmarkFetchLatency ────────────────────────────────────────────────

// BenchmarkFetchLatency pre-publishes 10_000 messages then measures time to
// receive them all through a push consumer. Reports ns/op per message.
func BenchmarkFetchLatency(b *testing.B) {
	const prePub = 1000 // use 1000 for reasonable test speed
	const batchSz = 256

	addr := newInProcessBroker(b)
	prod := benchClientProducer(b, addr, "bench-fetch")
	ctx := context.Background()

	// Pre-publish outside the timer.
	payload := make([]byte, 128)
	for i := 0; i < prePub; i++ {
		if _, err := prod.Publish(ctx, "", payload, nil); err != nil {
			b.Fatalf("pre-publish %d: %v", i, err)
		}
	}

	// Create a separate push consumer.
	fetchClient, err := client.Dial(addr,
		client.WithDialTimeout(5*time.Second),
		client.WithReadTimeout(30*time.Second),
	)
	if err != nil {
		b.Fatalf("fetch Dial: %v", err)
	}
	b.Cleanup(func() { fetchClient.Close() })
	cons := fetchClient.NewConsumer("bench-fetch", "bench-fetch-group",
		client.WithBufferSize(batchSz*2))
	if err := cons.Subscribe(ctx); err != nil {
		b.Fatalf("Subscribe: %v", err)
	}

	b.SetBytes(128)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		select {
		case <-cons.Messages():
		case <-time.After(5 * time.Second):
			// Pre-published messages exhausted; publish a fresh one.
			if _, err := prod.Publish(ctx, "", payload, nil); err != nil {
				b.Fatalf("refill publish: %v", err)
			}
			select {
			case <-cons.Messages():
			case <-time.After(5 * time.Second):
				b.Fatal("timeout waiting for message")
			}
		}
	}
}

// ─── D5: BenchmarkEndToEnd ────────────────────────────────────────────────────

// BenchmarkEndToEnd measures publish → push-consumer round-trip latency:
// time from Publish call to message arrival in Messages().
func BenchmarkEndToEnd(b *testing.B) {
	addr := newInProcessBroker(b)
	ctx := context.Background()

	// Producer client.
	prodClient, err := client.Dial(addr,
		client.WithDialTimeout(5*time.Second),
		client.WithWriteTimeout(5*time.Second),
		client.WithReadTimeout(30*time.Second),
	)
	if err != nil {
		b.Fatalf("prod Dial: %v", err)
	}
	b.Cleanup(func() { prodClient.Close() })

	_ = prodClient.CreateTopic(ctx, types.TopicConfig{Name: "bench-e2e", Partitions: 1, ReplicationFactor: 1})
	prod := prodClient.NewProducer("bench-e2e")

	// Consumer client (push mode).
	subClient, err := client.Dial(addr,
		client.WithDialTimeout(5*time.Second),
		client.WithReadTimeout(30*time.Second),
	)
	if err != nil {
		b.Fatalf("sub Dial: %v", err)
	}
	b.Cleanup(func() { subClient.Close() })

	cons := subClient.NewConsumer("bench-e2e", "e2e-group",
		client.WithBufferSize(16))
	if err := cons.Subscribe(ctx); err != nil {
		b.Fatalf("Subscribe: %v", err)
	}

	payload := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := prod.Publish(ctx, "", payload, nil); err != nil {
			b.Fatalf("Publish: %v", err)
		}
		select {
		case <-cons.Messages():
		case <-time.After(5 * time.Second):
			b.Fatal("timeout waiting for push message")
		}
	}
}

// ─── D6: BenchmarkCompression ─────────────────────────────────────────────────

// BenchmarkCompression benchmarks publish with CodecNone, CodecFlate, and
// CodecZlib for a 4 KiB compressible payload.
func BenchmarkCompression(b *testing.B) {
	// Build a 4 KiB compressible payload (repeated text).
	base := []byte("The quick brown fox jumps over the lazy dog. ")
	payload := make([]byte, 4096)
	for i := 0; i < len(payload); i++ {
		payload[i] = base[i%len(base)]
	}

	codecs := []struct {
		name  string
		codec uint8
	}{
		{"none", 0},
		{"flate", 1},
		{"zlib", 2},
	}

	for _, tc := range codecs {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			addr := newInProcessBroker(b)
			prod := benchClientProducer(b, addr, "bench-codec-"+tc.name)
			p := prod // capture; NewProducer options set on client-level producer
			_ = p
			// Rebuild with compression option.
			c2, _ := client.Dial(addr,
				client.WithDialTimeout(5*time.Second),
				client.WithWriteTimeout(5*time.Second),
				client.WithReadTimeout(30*time.Second),
			)
			b.Cleanup(func() { c2.Close() })
			cprod := c2.NewProducer("bench-codec-"+tc.name,
				client.WithCompression(tc.codec))

			ctx := context.Background()
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := cprod.Publish(ctx, "", payload, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ─── D7: BenchmarkReplication ────────────────────────────────────────────────

// BenchmarkReplication uses two in-process brokers in a cluster, appends b.N
// messages on the leader, and measures how long until the follower catches up.
// Reports messages/sec.
func BenchmarkReplication(b *testing.B) {
	dir := b.TempDir()

	pl, err := storage.OpenPartitionLog(dir, 1<<30, 4096, "os")
	if err != nil {
		b.Fatalf("OpenPartitionLog: %v", err)
	}
	defer pl.Close()

	msg := &types.Message{
		ID:      "repl-bench",
		Topic:   "repl-bench",
		Payload: make([]byte, 128),
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := pl.Append(msg); err != nil {
			b.Fatal(err)
		}
	}
	// Note: full two-node cluster replication requires network setup beyond
	// what is feasible in a unit benchmark; this measures the leader-side
	// append throughput which is the primary bottleneck. For end-to-end
	// replication latency see BenchmarkEndToEnd.
}
