package broker_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Helpers ──────────────────────────────────────────────────────────────────

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	dir := t.TempDir()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "test-node"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
		},
	}
	path := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(cfg)
	_ = os.WriteFile(path, data, 0o644)

	loader, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(func() { loader.Close() })

	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	return b
}

// startBroker launches the broker in a background goroutine and blocks until
// the TCP port is ready to accept connections (or the test fails).
func startBroker(t *testing.T, b *broker.Broker) string {
	t.Helper()
	go b.Start() //nolint:errcheck
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		addr := b.Addr()
		if addr != "" {
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err == nil {
				conn.Close()
				return addr
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("broker TCP server did not become ready within 5 s")
	return ""
}

// ─── testClient — minimal raw-protocol client ────────────────────────────────

type testClient struct {
	t    *testing.T
	conn net.Conn
	enc  *protocol.Encoder
	dec  *protocol.Decoder
	seq  uint64
}

func dialBroker(t *testing.T, addr string) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial broker %s: %v", addr, err)
	}
	t.Cleanup(func() { conn.Close() })
	return &testClient{
		t:    t,
		conn: conn,
		enc:  protocol.NewEncoder(conn),
		dec:  protocol.NewDecoder(conn),
	}
}

// send encodes cmd+body, reads one response frame, and returns it.
func (tc *testClient) send(cmd protocol.Command, body interface{}) *protocol.Frame {
	tc.t.Helper()
	tc.seq++
	if err := tc.enc.Encode(cmd, tc.seq, body); err != nil {
		tc.t.Fatalf("send %s: %v", cmd, err)
	}
	_ = tc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := tc.dec.Decode()
	if err != nil {
		tc.t.Fatalf("recv after %s: %v", cmd, err)
	}
	return f
}

// sendOK sends cmd+body and asserts the response carries ok:true.
func (tc *testClient) sendOK(cmd protocol.Command, body interface{}) {
	tc.t.Helper()
	f := tc.send(cmd, body)
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := json.Unmarshal(f.Body, &resp); err != nil {
		tc.t.Fatalf("unmarshal %s response: %v (body: %s)", cmd, err, f.Body)
	}
	if !resp.OK {
		tc.t.Fatalf("%s: expected ok:true, got: %s", cmd, f.Body)
	}
}

// encodeFrame writes a raw binary protocol frame directly to conn.
// Used to test unauthenticated / malformed scenarios.
func encodeFrame(conn net.Conn, cmd protocol.Command, reqID uint64, body []byte) error {
	var h [protocol.HeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:4], protocol.Magic)
	h[4] = protocol.Version
	h[5] = byte(cmd)
	binary.LittleEndian.PutUint64(h[6:14], reqID)
	binary.LittleEndian.PutUint32(h[14:18], uint32(len(body)))
	if _, err := conn.Write(h[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		_, err := conn.Write(body)
		return err
	}
	return nil
}

// ─── Original tests (semantics unchanged) ────────────────────────────────────

func TestBroker_InitialStatus(t *testing.T) {
	b := newTestBroker(t)
	if b.Status() != types.NodeActive {
		t.Errorf("initial status: want ACTIVE, got %s", b.Status())
	}
}

func TestBroker_StopWithContext(t *testing.T) {
	b := newTestBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if b.Status() != types.NodeUnhealthy {
		t.Errorf("post-stop status: want UNHEALTHY, got %s", b.Status())
	}
}

// TestBroker_HTTPHealth verifies the health endpoint returns valid JSON.
// We test the handler logic in isolation using httptest.
func TestBroker_HTTPHealth(t *testing.T) {
	b := newTestBroker(t)
	defer b.Stop(context.Background()) //nolint:errcheck

	// Build a minimal handler inline to test status JSON logic.
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": string(b.Status())})
	})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("health status: want 200, got %d", rr.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal health response: %v", err)
	}
	if body["status"] != string(types.NodeActive) {
		t.Errorf("health.status: want ACTIVE, got %s", body["status"])
	}
}

// ─── Gap 2: AckNack ───────────────────────────────────────────────────────────

// TestBroker_AckNack spins up a real broker, publishes one message, and then:
//   - Sends a CmdAck → asserts ok:true.
//   - Sends a CmdNack(requeue=true) → asserts ok:true.
//   - Sends a CmdNack(requeue=false) → asserts ok:true and DLQ has exactly
//     one new entry.
func TestBroker_AckNack(t *testing.T) {
	b := newTestBroker(t)
	addr := startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	tc := dialBroker(t, addr)

	// 1. Create a topic.
	tc.sendOK(protocol.CmdCreateTopic, &protocol.CreateTopicRequest{
		Name:              "ack-test",
		Partitions:        1,
		ReplicationFactor: 1,
	})

	// 2. Publish one message and capture its partition+offset.
	pubFrame := tc.send(protocol.CmdPublish, &protocol.PublishRequest{
		Topic:        "ack-test",
		Key:          "k1",
		Payload:      []byte(`{"hello":"world"}`),
		DeliveryMode: uint8(types.AtLeastOnce),
	})
	var pubResp protocol.PublishResponse
	if err := json.Unmarshal(pubFrame.Body, &pubResp); err != nil {
		t.Fatalf("unmarshal publish response: %v (body: %s)", err, pubFrame.Body)
	}
	t.Logf("published: offset=%d partition=%d", pubResp.Offset, pubResp.Partition)

	// 3. CmdAck → ok:true.
	tc.sendOK(protocol.CmdAck, &protocol.AckRequest{
		ConsumerID: "consumer-1",
		Topic:      "ack-test",
		Partition:  pubResp.Partition,
		Offset:     pubResp.Offset,
	})

	// 4. CmdNack requeue=true → ok:true.
	tc.sendOK(protocol.CmdNack, &protocol.NackRequest{
		ConsumerID: "consumer-1",
		Topic:      "ack-test",
		Partition:  pubResp.Partition,
		Offset:     pubResp.Offset,
		Requeue:    true,
	})

	// 5. CmdNack requeue=false → ok:true, DLQ gains exactly one entry.
	dlqBefore := b.ConsumerDLQ().Len()
	tc.sendOK(protocol.CmdNack, &protocol.NackRequest{
		ConsumerID: "consumer-1",
		Topic:      "ack-test",
		Partition:  pubResp.Partition,
		Offset:     pubResp.Offset,
		Requeue:    false,
	})
	dlqAfter := b.ConsumerDLQ().Len()
	if dlqAfter != dlqBefore+1 {
		t.Errorf("DLQ after NACK(requeue=false): want %d, got %d", dlqBefore+1, dlqAfter)
	}
}

// TestBroker_AckUnauthorized verifies that ACK frames return UNAUTHORIZED when
// auth is enabled and the connection has not been authenticated.
func TestBroker_AckUnauthorized(t *testing.T) {
	dir := t.TempDir()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "auth-node"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100},
		"auth":    map[string]interface{}{"enabled": true, "api_keys": []interface{}{}},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
		},
	}
	path := filepath.Join(dir, "config.json")
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
	addr := startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	// Connect without authenticating.
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	ackBody, _ := json.Marshal(&protocol.AckRequest{
		ConsumerID: "x", Topic: "t", Partition: 0, Offset: 0,
	})
	if err := encodeFrame(conn, protocol.CmdAck, 1, ackBody); err != nil {
		t.Fatalf("write ack frame: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	dec := protocol.NewDecoder(conn)
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if f.Command != protocol.CmdError {
		t.Errorf("expected CmdError for unauthenticated ACK, got %s", f.Command)
	}
	var errResp protocol.ErrorResponse
	if err := json.Unmarshal(f.Body, &errResp); err != nil {
		t.Fatalf("unmarshal error response: %v", err)
	}
	if errResp.Code != string(types.ErrUnauthorized) {
		t.Errorf("error code: want %q, got %q", types.ErrUnauthorized, errResp.Code)
	}
}

// Ensure fmt is used (compile-time check for import completeness).
var _ = fmt.Sprintf
