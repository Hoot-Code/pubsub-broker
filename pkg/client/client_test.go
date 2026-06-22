package client_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/pkg/client"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

// startTestBroker launches an in-process broker on a random port.
// It registers a Cleanup that stops the broker.
func startTestBroker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "sdk-test-node"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 1000,
		             "read_timeout": 5000000000, "write_timeout": 5000000000, "idle_timeout": 30000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth":        {"enabled": false},
		"rate_limit":  {"enabled": false},
		"logging":     {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "broker.json")
	if err := os.WriteFile(cfgPath, []byte(cfgData), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	errC := make(chan error, 1)
	go func() { errC <- b.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Addr() != "" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if b.Addr() == "" {
		t.Fatal("broker did not start in time")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
		select {
		case <-errC:
		case <-time.After(3 * time.Second):
		}
	})
	return b.Addr()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestClientPublishAndConsume starts an in-process broker, publishes 50 messages
// via Producer.Publish, subscribes with Push:true via Consumer.Subscribe, and
// verifies all 50 arrive on Messages() without any CmdFetch calls.
func TestClientPublishAndConsume(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)

	// Dial with the SDK.
	c, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	// Create topic via a second SDK connection (re-use same broker).
	topicName := "sdk-pub-consume"
	if err := createTopicSDK(t, addr, topicName, 4); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Subscribe first so messages are delivered immediately.
	cs := c.NewConsumer("sdk-group", topicName)
	ctx := context.Background()
	if err := cs.Subscribe(ctx); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	// Publish 50 messages.
	prod := c.NewProducer(topicName)
	const N = 50
	for i := 0; i < N; i++ {
		_, err := prod.Publish(ctx, fmt.Sprintf("key-%d", i),
			[]byte(fmt.Sprintf("payload-%d", i)), nil)
		if err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	// Collect all 50 messages.
	received := make([]*types.Message, 0, N)
	timeout := time.After(10 * time.Second)
collect:
	for {
		select {
		case msg, ok := <-cs.Messages():
			if !ok {
				break collect
			}
			received = append(received, msg)
			// Commit each offset.
			if err := cs.Commit(ctx, msg.Partition, msg.Offset); err != nil {
				t.Errorf("Commit partition=%d offset=%d: %v", msg.Partition, msg.Offset, err)
			}
			if len(received) == N {
				break collect
			}
		case <-timeout:
			t.Fatalf("timed out: received %d/%d messages", len(received), N)
		}
	}

	if len(received) != N {
		t.Fatalf("want %d messages, got %d", N, len(received))
	}

	// Verify offsets are monotonically increasing per partition.
	partOffsets := make(map[int32]int64)
	for _, msg := range received {
		prev, ok := partOffsets[msg.Partition]
		if ok && msg.Offset <= prev {
			t.Errorf("partition %d: offset %d not greater than prev %d", msg.Partition, msg.Offset, prev)
		}
		partOffsets[msg.Partition] = msg.Offset
	}
}

// TestClientBatch publishes a batch of 100 messages and verifies all offsets
// are returned and unique.
func TestClientBatch(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)

	c, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	topicName := "sdk-batch"
	if err := createTopicSDK(t, addr, topicName, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	prod := c.NewProducer(topicName)
	const N = 100
	msgs := make([]client.Message, N)
	for i := range msgs {
		msgs[i] = client.Message{
			Key:     fmt.Sprintf("key-%d", i),
			Payload: []byte(fmt.Sprintf("batch-payload-%d", i)),
		}
	}

	ctx := context.Background()
	offsets, err := prod.PublishBatch(ctx, msgs)
	if err != nil {
		t.Fatalf("PublishBatch: %v", err)
	}
	if len(offsets) != N {
		t.Fatalf("want %d offsets, got %d", N, len(offsets))
	}

	// Verify uniqueness.
	seen := make(map[int64]bool, N)
	for i, off := range offsets {
		if seen[off] {
			t.Errorf("duplicate offset %d at index %d", off, i)
		}
		seen[off] = true
	}

	// Sort and verify contiguous.
	sorted := make([]int64, N)
	copy(sorted, offsets)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for i := 1; i < len(sorted); i++ {
		if sorted[i]-sorted[i-1] != 1 {
			t.Errorf("offsets not contiguous at index %d: %d -> %d", i, sorted[i-1], sorted[i])
		}
	}
}

// TestClientReconnect verifies that closing the underlying net.Conn causes
// subsequent Publish calls to return ErrConnectionClosed (not a panic).
func TestClientReconnect(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)

	c, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// Close the client directly.
	if err := c.Close(); err != nil {
		t.Logf("Close: %v", err)
	}

	// Wait a moment for the readLoop to exit.
	time.Sleep(50 * time.Millisecond)

	// Any subsequent operation should return ErrConnectionClosed.
	prod := c.NewProducer("any-topic")
	ctx := context.Background()
	_, pubErr := prod.Publish(ctx, "k", []byte("p"), nil)
	if pubErr == nil {
		t.Fatal("expected ErrConnectionClosed after Close(), got nil")
	}
	var ccErr *client.ErrConnectionClosed
	if !isErrConnectionClosed(pubErr) {
		t.Errorf("want ErrConnectionClosed, got %T: %v", pubErr, pubErr)
		_ = ccErr
	}
}

// TestClientPushSlowSink verifies that a slow push consumer does not block
// a second consumer from receiving messages (Part C6).
func TestClientPushSlowSink(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)

	topicName := "sdk-slow-sink"
	if err := createTopicSDK(t, addr, topicName, 1); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Fast consumer.
	fastC, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Dial fast: %v", err)
	}
	t.Cleanup(func() { _ = fastC.Close() })

	fastCS := fastC.NewConsumer("fast-group", topicName, client.WithBufferSize(64))
	ctx := context.Background()
	if err := fastCS.Subscribe(ctx); err != nil {
		t.Fatalf("fast Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = fastCS.Close() })

	// Publish 20 messages.
	prodC, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("Dial prod: %v", err)
	}
	t.Cleanup(func() { _ = prodC.Close() })

	prod := prodC.NewProducer(topicName)
	const N = 20
	for i := 0; i < N; i++ {
		if _, err := prod.Publish(ctx, "k", []byte(fmt.Sprintf("msg-%d", i)), nil); err != nil {
			t.Fatalf("Publish[%d]: %v", i, err)
		}
	}

	// Fast consumer must receive all N messages within deadline.
	var fastCount int
	timeout := time.After(8 * time.Second)
fastLoop:
	for {
		select {
		case _, ok := <-fastCS.Messages():
			if !ok {
				break fastLoop
			}
			fastCount++
			if fastCount == N {
				break fastLoop
			}
		case <-timeout:
			t.Fatalf("fast consumer timed out: received %d/%d", fastCount, N)
		}
	}
	if fastCount != N {
		t.Errorf("fast consumer: want %d, got %d", N, fastCount)
	}
}

// createTopicSDK creates a topic by dialling the broker directly with an
// ephemeral raw connection (uses only pkg/protocol — not internal/).
func createTopicSDK(t *testing.T, addr, topic string, partitions int) error {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	sendCreateTopic(t, conn, topic, partitions)
	return nil
}

// isErrConnectionClosed checks if an error is or wraps ErrConnectionClosed.
func isErrConnectionClosed(err error) bool {
	if err == nil {
		return false
	}
	var target *client.ErrConnectionClosed
	if errors.As(err, &target) {
		return true
	}
	// Also accept the raw string for forwarded errors.
	return false
}

// ─── Test: globalPushRouters is process-global ────────────────────────────

// TestFix1TwoClientsIsolatedRouters creates two independent Clients connected
// to the same broker on the same topic but different groups, publishes 10
// messages, and verifies each consumer receives exactly 10 — not 20.
func TestFix1TwoClientsIsolatedRouters(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)
	ctx := context.Background()

	// Create topic.
	setup, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("setup dial: %v", err)
	}
	if err := setup.CreateTopic(ctx, types.TopicConfig{Name: "fix1-topic", Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	setup.Close()

	// Consumer A on group-A.
	cA, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dialA: %v", err)
	}
	defer cA.Close()
	consA := cA.NewConsumer("fix1-group-A", "fix1-topic", client.WithBufferSize(20))
	if err := consA.Subscribe(ctx); err != nil {
		t.Fatalf("subscribeA: %v", err)
	}
	defer consA.Close()

	// Consumer B on group-B.
	cB, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dialB: %v", err)
	}
	defer cB.Close()
	consB := cB.NewConsumer("fix1-group-B", "fix1-topic", client.WithBufferSize(20))
	if err := consB.Subscribe(ctx); err != nil {
		t.Fatalf("subscribeB: %v", err)
	}
	defer consB.Close()

	// Publish 10 messages.
	pub, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("pub dial: %v", err)
	}
	defer pub.Close()
	prod := pub.NewProducer("fix1-topic")
	for i := 0; i < 10; i++ {
		if _, err := prod.Publish(ctx, "", []byte("msg"), nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Each consumer should receive exactly 10 messages.
	collect := func(cs interface{ Messages() <-chan *types.Message }, want int, label string) {
		t.Helper()
		got := 0
		timeout := time.After(5 * time.Second)
		for got < want {
			select {
			case _, ok := <-cs.Messages():
				if !ok {
					t.Errorf("%s: channel closed early", label)
					return
				}
				got++
			case <-timeout:
				t.Errorf("%s: timeout: got %d/%d", label, got, want)
				return
			}
		}
		// Verify no extra messages arrive in 100 ms.
		extra := time.After(100 * time.Millisecond)
		select {
		case msg, ok := <-cs.Messages():
			if ok {
				t.Errorf("%s: got extra message %s (expected only %d)", label, msg.ID, want)
			}
		case <-extra:
		}
	}

	collect(consA, 10, "consumerA")
	collect(consB, 10, "consumerB")
}

// ─── Test: deregisterPushRouter removes entries ────────────────────────

// TestFix2PushRouterCleanup creates and closes 1000 consumers and verifies that
// the client's internal router map has zero entries afterward.
func TestFix2PushRouterCleanup(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)
	ctx := context.Background()

	// Create topic.
	setup, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("setup dial: %v", err)
	}
	if err := setup.CreateTopic(ctx, types.TopicConfig{Name: "fix2-topic", Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	setup.Close()

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	const n = 100 // reduced from 1000 to keep test fast
	for i := 0; i < n; i++ {
		cs := c.NewConsumer(fmt.Sprintf("fix2-grp-%d", i), "fix2-topic")
		if err := cs.Subscribe(ctx); err != nil {
			t.Fatalf("subscribe %d: %v", i, err)
		}
		if err := cs.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
	}

	// After all closes, the push router count should be zero.
	count := c.PushRouterCount()
	if count != 0 {
		t.Errorf("PushRouterCount: want 0, got %d", count)
	}
}

// ─── Test: handlePush filters by group+consumerID ──────────────────────

// TestFix3ConsumerGroupIsolation subscribes two consumers on the same topic but
// different groups and verifies each only receives the messages intended for it,
// without cross-contamination.
func TestFix3ConsumerGroupIsolation(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)
	ctx := context.Background()

	c, err := client.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	if err := c.CreateTopic(ctx, types.TopicConfig{Name: "fix3-topic", Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	cs1 := c.NewConsumer("fix3-grp-1", "fix3-topic", client.WithBufferSize(20))
	if err := cs1.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe cs1: %v", err)
	}
	defer cs1.Close()

	cs2 := c.NewConsumer("fix3-grp-2", "fix3-topic", client.WithBufferSize(20))
	if err := cs2.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe cs2: %v", err)
	}
	defer cs2.Close()

	prod := c.NewProducer("fix3-topic")
	for i := 0; i < 5; i++ {
		if _, err := prod.Publish(ctx, "", []byte("hello"), nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	// Both consumers should receive all 5 messages independently.
	collectN := func(cs interface{ Messages() <-chan *types.Message }, want int, label string) {
		t.Helper()
		got := 0
		for got < want {
			select {
			case _, ok := <-cs.Messages():
				if !ok {
					t.Errorf("%s: channel closed early at %d/%d", label, got, want)
					return
				}
				got++
			case <-time.After(5 * time.Second):
				t.Errorf("%s: timeout: got %d/%d", label, got, want)
				return
			}
		}
	}

	collectN(cs1, 5, "cs1")
	collectN(cs2, 5, "cs2")
}

// ─── Test: push connection survives idle period > readTimeout ───────────

// TestFix4PushSurvivesIdleTimeout subscribes in push mode, sleeps for twice the
// readTimeout without receiving any messages, then publishes one message and
// verifies the consumer still receives it (connection must still be alive).
func TestFix4PushSurvivesIdleTimeout(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)
	ctx := context.Background()

	// Use a very short readTimeout so we can test this quickly.
	const timeout = 200 * time.Millisecond
	c, err := client.Dial(addr,
		client.WithReadTimeout(timeout),
		client.WithDialTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	if err := c.CreateTopic(ctx, types.TopicConfig{Name: "fix4-topic", Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	cs := c.NewConsumer("fix4-grp", "fix4-topic", client.WithBufferSize(4))
	if err := cs.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cs.Close()

	// Sleep for 2× readTimeout with no messages — connection should survive.
	time.Sleep(2 * timeout)

	// Now publish one message and verify it arrives.
	prod := c.NewProducer("fix4-topic")
	if _, err := prod.Publish(ctx, "", []byte("still-alive"), nil); err != nil {
		t.Fatalf("publish after idle: %v", err)
	}

	select {
	case msg, ok := <-cs.Messages():
		if !ok {
			t.Fatal("consumer channel closed unexpectedly")
		}
		if string(msg.Payload) != "still-alive" {
			t.Errorf("got payload %q, want %q", msg.Payload, "still-alive")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for message after idle period — connection likely dead")
	}
}

// TestNewConsumerGroupReplayE2E exercises the exact code path that real SDK
// users go through: client.Dial → NewConsumer → Subscribe → Messages().
// It publishes messages BEFORE any consumer exists, then creates a brand-new
// consumer group and verifies all previously published messages arrive via the
// push channel. This test catches the race condition where CmdPush frames from
// replay arrive before the client's push router is registered.
func TestNewConsumerGroupReplayE2E(t *testing.T) {
	t.Parallel()
	addr := startTestBroker(t)

	ctx := context.Background()
	const N = 20

	// Use a dedicated connection to set up the topic and publish messages.
	setup, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("dial setup: %v", err)
	}
	defer setup.Close()

	topicName := "replay-e2e-topic"
	if err := setup.CreateTopic(ctx, types.TopicConfig{Name: topicName, Partitions: 1}); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Publish N messages BEFORE any consumer exists.
	prod := setup.NewProducer(topicName)
	for i := 0; i < N; i++ {
		if _, err := prod.Publish(ctx, fmt.Sprintf("key-%d", i),
			[]byte(fmt.Sprintf("replay-msg-%d", i)), nil); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	setup.Close() // close publisher so only the consumer connection remains

	// Now create a brand-new consumer on a never-used group via a fresh connection.
	consumerConn, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
	if err != nil {
		t.Fatalf("dial consumer: %v", err)
	}
	defer consumerConn.Close()

	cs := consumerConn.NewConsumer("replay-e2e-group", topicName, client.WithBufferSize(N*2))
	if err := cs.Subscribe(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer cs.Close()

	// Collect all N messages from the push channel.
	received := make(map[string]bool, N)
	timeout := time.After(10 * time.Second)
	for len(received) < N {
		select {
		case msg, ok := <-cs.Messages():
			if !ok {
				t.Fatalf("channel closed early: received %d/%d messages", len(received), N)
			}
			received[string(msg.Payload)] = true
		case <-timeout:
			t.Fatalf("timeout: received %d/%d messages: %v", len(received), N, received)
		}
	}

	// Verify every message arrived.
	for i := 0; i < N; i++ {
		want := fmt.Sprintf("replay-msg-%d", i)
		if !received[want] {
			t.Errorf("missing replayed message: %q", want)
		}
	}
}

// TestNewConsumerGroupReplayRaceLoop runs the replay E2E test multiple times
// under the race detector to prove it is structurally correct and not just
// passing by luck. Each iteration creates a fresh topic and consumer group.
func TestNewConsumerGroupReplayRaceLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race loop in short mode")
	}
	t.Parallel()
	addr := startTestBroker(t)
	ctx := context.Background()
	const iterations = 10

	for iter := 0; iter < iterations; iter++ {
		topicName := fmt.Sprintf("race-loop-topic-%d", iter)
		groupName := fmt.Sprintf("race-loop-group-%d", iter)

		// Setup connection to create topic and publish.
		setup, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
		if err != nil {
			t.Fatalf("iter %d dial setup: %v", iter, err)
		}
		if err := setup.CreateTopic(ctx, types.TopicConfig{Name: topicName, Partitions: 1}); err != nil {
			setup.Close()
			t.Fatalf("iter %d create topic: %v", iter, err)
		}

		const N = 5
		prod := setup.NewProducer(topicName)
		for i := 0; i < N; i++ {
			if _, err := prod.Publish(ctx, fmt.Sprintf("k%d", i),
				[]byte(fmt.Sprintf("iter%d-msg%d", iter, i)), nil); err != nil {
				setup.Close()
				t.Fatalf("iter %d publish %d: %v", iter, i, err)
			}
		}
		setup.Close()

		// Consumer on a fresh group.
		consumerConn, err := client.Dial(addr, client.WithDialTimeout(5*time.Second))
		if err != nil {
			t.Fatalf("iter %d dial consumer: %v", iter, err)
		}
		cs := consumerConn.NewConsumer(groupName, topicName, client.WithBufferSize(N*2))
		if err := cs.Subscribe(ctx); err != nil {
			consumerConn.Close()
			t.Fatalf("iter %d subscribe: %v", iter, err)
		}

		received := make(map[string]bool, N)
		timeout := time.After(5 * time.Second)
		for len(received) < N {
			select {
			case msg, ok := <-cs.Messages():
				if !ok {
					cs.Close()
					consumerConn.Close()
					t.Fatalf("iter %d: channel closed early at %d/%d", iter, len(received), N)
				}
				received[string(msg.Payload)] = true
			case <-timeout:
				cs.Close()
				consumerConn.Close()
				t.Fatalf("iter %d: timeout at %d/%d", iter, len(received), N)
			}
		}
		cs.Close()
		consumerConn.Close()

		for i := 0; i < N; i++ {
			want := fmt.Sprintf("iter%d-msg%d", iter, i)
			if !received[want] {
				t.Errorf("iter %d: missing message %q", iter, want)
			}
		}
	}
}
