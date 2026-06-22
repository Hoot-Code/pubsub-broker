// Package integration runs end-to-end tests against a live broker instance.
// Each test spins up a real broker bound to an ephemeral port, connects
// via TCP, and exercises the full protocol stack.
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Test helpers ─────────────────────────────────────────────────────────────

type testBroker struct {
	b    *broker.Broker
	addr string
	done chan error
}

func startTestBroker(t *testing.T) *testBroker {
	t.Helper()
	dir := t.TempDir()

	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "integration-node"},
		"network": map[string]interface{}{"port": 0, "host": "127.0.0.1", "max_connections": 100},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
		},
		"auth": map[string]interface{}{"enabled": false},
	}
	path := filepath.Join(dir, "c.json")
	data, _ := json.Marshal(cfg)
	_ = os.WriteFile(path, data, 0o644)

	loader, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(loader.Close)

	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}

	tb := &testBroker{b: b, done: make(chan error, 1)}
	go func() { tb.done <- b.Start() }()

	// Wait for the broker to accept connections and record the actual address.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Addr() != "" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if b.Addr() == "" {
		t.Fatal("broker did not start in time")
	}
	tb.addr = b.Addr()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})
	return tb
}

// dial opens a TCP connection to the test broker.
func dial(t *testing.T, tb *testBroker) (net.Conn, *protocol.Encoder, *protocol.Decoder) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", tb.addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	return conn, enc, dec
}

func send(t *testing.T, enc *protocol.Encoder, dec *protocol.Decoder, cmd protocol.Command, reqID uint64, body interface{}) *protocol.Frame {
	t.Helper()
	if err := enc.Encode(cmd, reqID, body); err != nil {
		t.Fatalf("encode %s: %v", cmd, err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode response to %s: %v", cmd, err)
	}
	return f
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestIntegration_Ping(t *testing.T) {
	tb := startTestBroker(t)
	_, enc, dec := dial(t, tb)

	f := send(t, enc, dec, protocol.CmdPing, 1, nil)
	if f.Command != protocol.CmdPong {
		t.Errorf("ping: want PONG, got %s", f.Command)
	}
}

func TestIntegration_CreateAndListTopic(t *testing.T) {
	tb := startTestBroker(t)
	_, enc, dec := dial(t, tb)

	// Create topic.
	f := send(t, enc, dec, protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{
		Name:       "integration-test",
		Partitions: 2,
	})
	if f.Command != protocol.CmdResponse {
		t.Fatalf("create-topic: want RESPONSE, got %s; body: %s", f.Command, f.Body)
	}

	// List topics.
	f = send(t, enc, dec, protocol.CmdListTopics, 2, nil)
	if f.Command != protocol.CmdResponse {
		t.Fatalf("list-topics: want RESPONSE, got %s", f.Command)
	}
	var topics []interface{}
	if err := json.Unmarshal(f.Body, &topics); err != nil {
		t.Fatalf("unmarshal topics: %v", err)
	}
	if len(topics) == 0 {
		t.Error("expected at least one topic in list")
	}
}

func TestIntegration_PublishAndFetch(t *testing.T) {
	tb := startTestBroker(t)
	_, enc, dec := dial(t, tb)

	// Create topic.
	send(t, enc, dec, protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{
		Name:       "events",
		Partitions: 1,
	})

	// Subscribe.
	send(t, enc, dec, protocol.CmdSubscribe, 2, &protocol.SubscribeRequest{
		Topic:      "events",
		Group:      "test-group",
		ConsumerID: "consumer-1",
	})

	// Publish 5 messages.
	for i := 0; i < 5; i++ {
		f := send(t, enc, dec, protocol.CmdPublish, uint64(10+i), &protocol.PublishRequest{
			Topic:        "events",
			Key:          fmt.Sprintf("k%d", i),
			Payload:      []byte(fmt.Sprintf(`{"seq":%d}`, i)),
			DeliveryMode: uint8(types.AtLeastOnce),
		})
		if f.Command != protocol.CmdResponse {
			t.Fatalf("publish[%d]: want RESPONSE, got %s; body: %s", i, f.Command, f.Body)
		}
	}

	// Fetch from offset 0.
	f := send(t, enc, dec, protocol.CmdFetch, 20, &protocol.FetchRequest{
		Topic:     "events",
		Group:     "test-group",
		Partition: 0,
		Offset:    0,
		MaxCount:  10,
	})
	if f.Command != protocol.CmdResponse {
		t.Fatalf("fetch: want RESPONSE, got %s; body: %s", f.Command, f.Body)
	}

	var resp protocol.FetchResponse
	// FetchResponse.Messages is []interface{} for JSON flexibility.
	if err := json.Unmarshal(f.Body, &resp); err != nil {
		t.Fatalf("unmarshal fetch response: %v", err)
	}
}

func TestIntegration_CommitOffset(t *testing.T) {
	tb := startTestBroker(t)
	_, enc, dec := dial(t, tb)

	send(t, enc, dec, protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{Name: "offsets", Partitions: 1})
	send(t, enc, dec, protocol.CmdSubscribe, 2, &protocol.SubscribeRequest{
		Topic:      "offsets",
		Group:      "g1",
		ConsumerID: "c1",
	})
	send(t, enc, dec, protocol.CmdPublish, 3, &protocol.PublishRequest{
		Topic:        "offsets",
		Payload:      []byte("hello"),
		DeliveryMode: uint8(types.AtLeastOnce),
	})

	f := send(t, enc, dec, protocol.CmdCommitOffset, 4, &protocol.CommitOffsetRequest{
		Group:      "g1",
		ConsumerID: "c1",
		Topic:      "offsets",
		Partition:  0,
		Offset:     0,
	})
	if f.Command != protocol.CmdResponse {
		t.Fatalf("commit-offset: want RESPONSE, got %s", f.Command)
	}
}

func TestIntegration_DeleteTopic(t *testing.T) {
	tb := startTestBroker(t)
	_, enc, dec := dial(t, tb)

	send(t, enc, dec, protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{Name: "tmp", Partitions: 1})
	f := send(t, enc, dec, protocol.CmdDeleteTopic, 2, &protocol.DeleteTopicRequest{Name: "tmp"})
	if f.Command != protocol.CmdResponse {
		t.Fatalf("delete-topic: want RESPONSE, got %s; body: %s", f.Command, f.Body)
	}

	// Second delete → error response.
	f = send(t, enc, dec, protocol.CmdDeleteTopic, 3, &protocol.DeleteTopicRequest{Name: "tmp"})
	if f.Command != protocol.CmdError {
		t.Errorf("delete non-existent: want ERROR, got %s", f.Command)
	}
}

func TestIntegration_MultipleClients(t *testing.T) {
	tb := startTestBroker(t)

	_, enc1, dec1 := dial(t, tb)
	send(t, enc1, dec1, protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{Name: "multi", Partitions: 2})

	// Multiple publisher connections.
	var failures []string
	for i := 0; i < 5; i++ {
		_, enc, dec := dial(t, tb)
		f := send(t, enc, dec, protocol.CmdPublish, 1, &protocol.PublishRequest{
			Topic:        "multi",
			Key:          fmt.Sprintf("k%d", i),
			Payload:      []byte(fmt.Sprintf("msg-%d", i)),
			DeliveryMode: uint8(types.AtMostOnce),
		})
		if f.Command != protocol.CmdResponse {
			failures = append(failures, fmt.Sprintf("client %d: want RESPONSE, got %s", i, f.Command))
		}
	}
	for _, fail := range failures {
		t.Error(fail)
	}
}
