package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

func msg(topic string, partition int32, key string, payload string) *types.Message {
	return &types.Message{
		ID:        types.NewUUID(),
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Payload:   []byte(payload),
		Timestamp: time.Now().UnixNano(),
	}
}

// TestExplorerFilterMatching verifies that a session with Topic="orders",
// Partition=-1 (all partitions) only receives messages for "orders" and not
// for other topics.
func TestExplorerFilterMatching(t *testing.T) {
	hub := NewExplorerHub()
	var received []*types.Message
	var mu sync.Mutex

	sess := hub.NewSession(ExplorerFilter{Topic: "orders", Partition: -1},
		func(ev ExplorerEvent) error {
			mu.Lock()
			received = append(received, ev.Message)
			mu.Unlock()
			return nil
		})
	defer sess.Close()

	hub.Publish("orders", 0, "p1", msg("orders", 0, "k1", "payload-1"))
	hub.Publish("orders", 1, "p1", msg("orders", 1, "k2", "payload-2"))
	hub.Publish("other-topic", 0, "p1", msg("other-topic", 0, "k3", "payload-3"))

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(received))
	}
	for _, m := range received {
		if m.Topic != "orders" {
			t.Errorf("unexpected topic %q", m.Topic)
		}
	}
}

// TestExplorerKeyFilter verifies exact key matching.
func TestExplorerKeyFilter(t *testing.T) {
	hub := NewExplorerHub()
	var received []*types.Message
	var mu sync.Mutex

	sess := hub.NewSession(ExplorerFilter{Topic: "t", Partition: -1, Key: "match"},
		func(ev ExplorerEvent) error {
			mu.Lock()
			received = append(received, ev.Message)
			mu.Unlock()
			return nil
		})
	defer sess.Close()

	hub.Publish("t", 0, "p1", msg("t", 0, "match", "a"))
	hub.Publish("t", 0, "p1", msg("t", 0, "nomatch", "b"))

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if received[0].Key != "match" {
		t.Errorf("expected key 'match', got %q", received[0].Key)
	}
}

// TestExplorerProducerFilter verifies producer (client ID) matching via
// the ExplorerEvent.ClientID field — not via msg.Headers.
func TestExplorerProducerFilter(t *testing.T) {
	hub := NewExplorerHub()
	var receivedClientIDs []string
	var mu sync.Mutex

	sess := hub.NewSession(ExplorerFilter{Topic: "t", Partition: -1, Producer: "service-a"},
		func(ev ExplorerEvent) error {
			mu.Lock()
			receivedClientIDs = append(receivedClientIDs, ev.ClientID)
			mu.Unlock()
			return nil
		})
	defer sess.Close()

	hub.Publish("t", 0, "service-a", msg("t", 0, "k1", "a"))
	hub.Publish("t", 0, "service-b", msg("t", 0, "k2", "b"))

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(receivedClientIDs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(receivedClientIDs))
	}
	if receivedClientIDs[0] != "service-a" {
		t.Errorf("expected clientID 'service-a', got %q", receivedClientIDs[0])
	}
}

// TestExplorerSearchFilter verifies case-insensitive substring matching.
func TestExplorerSearchFilter(t *testing.T) {
	hub := NewExplorerHub()
	var received []*types.Message
	var mu sync.Mutex

	sess := hub.NewSession(ExplorerFilter{Topic: "t", Partition: -1, Search: "HELLO"},
		func(ev ExplorerEvent) error {
			mu.Lock()
			received = append(received, ev.Message)
			mu.Unlock()
			return nil
		})
	defer sess.Close()

	hub.Publish("t", 0, "p1", msg("t", 0, "k1", "say HELLO world"))
	hub.Publish("t", 0, "p1", msg("t", 0, "k2", "goodbye world"))

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
	if string(received[0].Payload) != "say HELLO world" {
		t.Errorf("unexpected payload %q", string(received[0].Payload))
	}
}

// TestExplorerPauseResume verifies that a paused session discards events
// and that resumed sessions deliver queued events.
func TestExplorerPauseResume(t *testing.T) {
	hub := NewExplorerHub()
	var received []*types.Message
	var mu sync.Mutex

	sess := hub.NewSession(ExplorerFilter{Topic: "t", Partition: -1},
		func(ev ExplorerEvent) error {
			mu.Lock()
			received = append(received, ev.Message)
			mu.Unlock()
			return nil
		})
	defer sess.Close()

	// Pause, publish 5 messages.
	sess.Pause()
	for i := 0; i < 5; i++ {
		hub.Publish("t", 0, "p1", msg("t", 0, "k", "payload"))
	}
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	cnt := len(received)
	mu.Unlock()
	if cnt != 0 {
		t.Fatalf("expected 0 messages while paused, got %d", cnt)
	}

	// Resume — previously queued messages should now arrive.
	sess.Resume()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	cnt = len(received)
	mu.Unlock()
	// We should get at least the messages that were queued (up to channel cap).
	if cnt == 0 {
		t.Fatalf("expected messages after resume, got 0")
	}
}

// TestExplorerBackpressureNoBlock verifies that Publish() never blocks
// even when a session's sink blocks forever (simulating a dead/slow client).
// Events are dropped and DroppedCount ends up > 0.
func TestExplorerBackpressureNoBlock(t *testing.T) {
	hub := NewExplorerHub()
	sess := hub.NewSession(ExplorerFilter{Topic: "t", Partition: -1},
		func(ev ExplorerEvent) error {
			// Block forever — simulating a dead client.
			select {}
		})
	defer sess.Close()

	// Fill the channel to capacity.
	for i := 0; i < explorerChanCap; i++ {
		hub.Publish("t", 0, "p1", msg("t", 0, "k", "x"))
	}

	// Now try publishing 1000 more with a timeout — must not block.
	done := make(chan struct{})
	go func() {
		start := time.Now()
		for i := 0; i < 1000; i++ {
			if time.Since(start) > 5*time.Second {
				t.Errorf("Publish blocked for more than 5 seconds")
				break
			}
			hub.Publish("t", 0, "p1", msg("t", 0, "k", "x"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Publish to complete")
	}

	if sess.DroppedCount() == 0 {
		t.Fatal("expected DroppedCount > 0")
	}
	t.Logf("DroppedCount: %d", sess.DroppedCount())
}

// TestExplorerZeroSessionsFastPath verifies that Publish() is effectively
// a no-op when there are no registered sessions — no allocation, no payload
// decode.
func TestExplorerZeroSessionsFastPath(t *testing.T) {
	hub := NewExplorerHub()
	m := msg("t", 0, "k", "payload")

	start := time.Now()
	const iterations = 100_000
	for i := 0; i < iterations; i++ {
		hub.Publish("t", 0, "p1", m)
	}
	elapsed := time.Since(start)
	perCall := float64(elapsed) / float64(iterations)

	t.Logf("%d Publish calls in %v (%.0f ns/call)", iterations, elapsed, perCall)
	if perCall > 1000 { // 1 microsecond
		t.Errorf("Publish too slow with zero sessions: %.0f ns/call (>1000 ns)", perCall)
	}
}

// TestExplorerEventClientID verifies that the clientID passed to Publish
// is threaded through to the sink as ExplorerEvent.ClientID.
func TestExplorerEventClientID(t *testing.T) {
	hub := NewExplorerHub()
	var gotClientID string
	var mu sync.Mutex

	sess := hub.NewSession(ExplorerFilter{Topic: "t", Partition: -1},
		func(ev ExplorerEvent) error {
			mu.Lock()
			gotClientID = ev.ClientID
			mu.Unlock()
			return nil
		})
	defer sess.Close()

	hub.Publish("t", 0, "my-real-client-id", msg("t", 0, "k", "payload"))
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if gotClientID != "my-real-client-id" {
		t.Errorf("expected clientID 'my-real-client-id', got %q", gotClientID)
	}
}
