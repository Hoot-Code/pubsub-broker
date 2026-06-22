package broker_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

// ─── Minimal WebSocket client ─────────────────────────────────────────────

type wsTestClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func dialWS(t *testing.T, url string) *wsTestClient {
	t.Helper()
	host, path := splitWSURL(t, url)
	conn, err := net.DialTimeout("tcp", host, 5*time.Second)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand: %v", err)
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("ws handshake write: %v", err)
	}
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("ws read status: %v", err)
	}
	if !bytes.Contains([]byte(statusLine), []byte("101")) {
		for {
			line, lerr := br.ReadString('\n')
			if lerr != nil || line == "\r\n" || line == "\n" {
				break
			}
		}
		t.Fatalf("ws handshake failed: %s", strings.TrimSpace(statusLine))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" || line == "\n" {
			break
		}
	}
	return &wsTestClient{conn: conn, br: br}
}

func splitWSURL(t *testing.T, url string) (host, path string) {
	t.Helper()
	const prefix = "ws://"
	rest := url[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i], rest[i:]
		}
	}
	return rest, "/"
}

func (c *wsTestClient) readTextFrame(t *testing.T) string {
	t.Helper()
	hdr := make([]byte, 2)
	if _, err := wsReadFull(c.br, hdr); err != nil {
		t.Fatalf("ws read frame header: %v", err)
	}
	opcode := hdr[0] & 0x0F
	length := int64(hdr[1] & 0x7F)
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err := wsReadFull(c.br, ext); err != nil {
			t.Fatalf("ws read ext: %v", err)
		}
		length = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := wsReadFull(c.br, ext); err != nil {
			t.Fatalf("ws read ext: %v", err)
		}
		length = int64(binary.BigEndian.Uint64(ext))
	}
	payload := make([]byte, length)
	if _, err := wsReadFull(c.br, payload); err != nil {
		t.Fatalf("ws read payload: %v", err)
	}
	if opcode == 0x8 {
		t.Fatalf("ws received close frame")
	}
	return string(payload)
}

func (c *wsTestClient) tryReadTextFrame() (string, error) {
	hdr := make([]byte, 2)
	if _, err := wsReadFull(c.br, hdr); err != nil {
		return "", err
	}
	opcode := hdr[0] & 0x0F
	length := int64(hdr[1] & 0x7F)
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err := wsReadFull(c.br, ext); err != nil {
			return "", err
		}
		length = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := wsReadFull(c.br, ext); err != nil {
			return "", err
		}
		length = int64(binary.BigEndian.Uint64(ext))
	}
	payload := make([]byte, length)
	if _, err := wsReadFull(c.br, payload); err != nil {
		return "", err
	}
	if opcode == 0x8 {
		return "", fmt.Errorf("received close frame")
	}
	return string(payload), nil
}

func (c *wsTestClient) writeTextFrame(t *testing.T, data string) {
	t.Helper()
	payload := []byte(data)
	maskBytes := make([]byte, 4)
	_, _ = rand.Read(maskBytes)
	first := byte(0x81)
	n := len(payload)
	var hdr []byte
	switch {
	case n <= 125:
		hdr = []byte{first, byte(n) | 0x80}
	case n <= 0xFFFF:
		hdr = []byte{first, 126 | 0x80, byte(n >> 8), byte(n)}
	default:
		hdr = make([]byte, 10)
		hdr[0] = first
		hdr[1] = 127 | 0x80
		for i := 0; i < 8; i++ {
			hdr[2+i] = byte(n >> (8 * (7 - i)))
		}
	}
	_, _ = c.conn.Write(hdr)
	_, _ = c.conn.Write(maskBytes)
	masked := make([]byte, n)
	for i := range payload {
		masked[i] = payload[i] ^ maskBytes[i%4]
	}
	_, _ = c.conn.Write(masked)
}

func (c *wsTestClient) Close() { _ = c.conn.Close() }

func wsReadFull(br *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := br.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ─── Broker helpers ───────────────────────────────────────────────────────

func writeExplorerConfig(t *testing.T, dir, networkExtra string) string {
	t.Helper()
	cfg := fmt.Sprintf(`{
		"broker": {"node_id": "explorer-test"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 100,
		            "read_timeout": 5000000000, "write_timeout": 5000000000%s},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth":        {"enabled": false},
		"rate_limit":  {"enabled": false},
		"logging":     {"level": "error", "format": "json"}
	}`, networkExtra, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "broker.json")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return cfgPath
}

func startExplorerTestBrokerWithConfig(t *testing.T, cfgPath string) (addr string, httpAddr string, b *broker.Broker) {
	t.Helper()
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	b, err = broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	errC := make(chan error, 1)
	go func() { errC <- b.Start() }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && b.Addr() == "" {
		time.Sleep(5 * time.Millisecond)
	}
	if b.Addr() == "" {
		t.Fatal("broker did not start in time")
	}

	httpAddr = b.HTTPAddr()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
		select {
		case <-errC:
		case <-time.After(3 * time.Second):
		}
	})
	return b.Addr(), httpAddr, b
}

func startExplorerTestBroker(t *testing.T, networkExtra string) (addr string, httpAddr string, b *broker.Broker) {
	t.Helper()
	dir := t.TempDir()
	cfgPath := writeExplorerConfig(t, dir, networkExtra)
	return startExplorerTestBrokerWithConfig(t, cfgPath)
}

func createTopicViaProto(t *testing.T, addr, topic string, partitions int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	_ = enc.Encode(protocol.CmdCreateTopic, 1, &protocol.CreateTopicRequest{
		Name: topic, Partitions: partitions, ReplicationFactor: 1,
	})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	dec := protocol.NewDecoder(conn)
	_, _ = dec.Decode()
}

func publishViaProto(t *testing.T, addr, topic, key, payload string) (int32, int64) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	seq := uint64(time.Now().UnixNano())
	_ = enc.Encode(protocol.CmdPublish, seq, &protocol.PublishRequest{
		Topic: topic, Key: key, Payload: []byte(payload),
	})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	var resp protocol.PublishResponse
	_ = json.Unmarshal(f.Body, &resp)
	return resp.Partition, resp.Offset
}

func rawHTTPUpgrade(t *testing.T, addr, path, authHeader string) (statusCode string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	req := "GET " + path + " HTTP/1.1\r\nHost: " + addr + "\r\n" +
		authHeader +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	_, _ = conn.Write([]byte(req))
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(conn)
	sl, _ := br.ReadString('\n')
	// Drain rest of response.
	for {
		line, lerr := br.ReadString('\n')
		if lerr != nil || line == "\r\n" || line == "\n" {
			break
		}
	}
	return strings.TrimSpace(sl)
}

// ─── Tests ────────────────────────────────────────────────────────────────

func TestExplorerEndpointReceivesLiveMessages(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerTestBroker(t, "")
	createTopicViaProto(t, brokerAddr, "exp-topic", 1)

	ws := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=exp-topic", httpAddr))
	defer ws.Close()
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 5; i++ {
		publishViaProto(t, brokerAddr, "exp-topic", fmt.Sprintf("k%d", i), fmt.Sprintf("msg-%d", i))
	}

	received := make(map[string]bool)
	for i := 0; i < 5; i++ {
		_ = ws.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		text := ws.readTextFrame(t)
		var frame struct {
			Offset  int64  `json:"offset"`
			Payload string `json:"payload"`
		}
		_ = json.Unmarshal([]byte(text), &frame)
		payload, _ := base64.StdEncoding.DecodeString(frame.Payload)
		received[string(payload)] = true
		t.Logf("received: offset=%d payload=%s", frame.Offset, string(payload))
	}
	for i := 0; i < 5; i++ {
		want := fmt.Sprintf("msg-%d", i)
		if !received[want] {
			t.Errorf("missing message %q", want)
		}
	}
}

func TestExplorerEndpointFiltersNonMatching(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerTestBroker(t, "")
	createTopicViaProto(t, brokerAddr, "exp-filter", 2)

	// Connect filtering for partition 0 only.
	ws := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=exp-filter&partition=0", httpAddr))
	defer ws.Close()
	time.Sleep(100 * time.Millisecond)

	// Find a key that hashes to partition 1.
	var foundPart1 bool
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("fk-%d", i)
		part, _ := publishViaProto(t, brokerAddr, "exp-filter", key, "should-not-arrive")
		if part == 1 {
			foundPart1 = true
			break
		}
	}
	if !foundPart1 {
		t.Skip("could not find key for partition 1")
	}

	// Verify no messages arrive (the ones that went to partition 1 should be filtered).
	ws.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err := ws.tryReadTextFrame()
	if err == nil {
		t.Fatal("expected timeout, but received a frame")
	}
	t.Log("correctly received no messages for filtered-out partition")
}

func TestExplorerEndpointPauseResume(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerTestBroker(t, "")
	createTopicViaProto(t, brokerAddr, "exp-pr", 1)

	ws := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=exp-pr", httpAddr))
	defer ws.Close()
	time.Sleep(100 * time.Millisecond)

	// Verify connection works.
	publishViaProto(t, brokerAddr, "exp-pr", "k", "alive")
	_ = ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	text := ws.readTextFrame(t)
	var f struct {
		Payload string `json:"payload"`
	}
	_ = json.Unmarshal([]byte(text), &f)
	pl, _ := base64.StdEncoding.DecodeString(f.Payload)
	if string(pl) != "alive" {
		t.Fatalf("expected 'alive', got %q", string(pl))
	}

	// Pause.
	ws.writeTextFrame(t, `{"action":"pause"}`)
	time.Sleep(300 * time.Millisecond) // ensure pause is fully processed by drain goroutine

	// Publish while paused.
	for i := 0; i < 3; i++ {
		publishViaProto(t, brokerAddr, "exp-pr", "k", fmt.Sprintf("paused-%d", i))
	}
	time.Sleep(200 * time.Millisecond)

	// Verify nothing arrives.
	ws.conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	_, err := ws.tryReadTextFrame()
	if err == nil {
		t.Fatal("expected timeout while paused")
	}

	// Resume.
	ws.writeTextFrame(t, `{"action":"resume"}`)
	time.Sleep(300 * time.Millisecond)

	// Queued messages should arrive.
	received := 0
	ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < 3; i++ {
		text, err := ws.tryReadTextFrame()
		if err != nil {
			break
		}
		var f2 struct {
			Payload string `json:"payload"`
		}
		_ = json.Unmarshal([]byte(text), &f2)
		pl2, _ := base64.StdEncoding.DecodeString(f2.Payload)
		if strings.HasPrefix(string(pl2), "paused-") {
			received++
		}
	}
	if received == 0 {
		t.Fatal("expected messages after resume")
	}
	t.Logf("received %d messages after resume", received)
}

func TestExplorerEndpointDisabledReturns403(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeExplorerConfig(t, dir, `, "explorer_enabled": false`)
	_, httpAddr, _ := startExplorerTestBrokerWithConfig(t, cfgPath)

	status := rawHTTPUpgrade(t, httpAddr, "/explorer/stream?topic=any", "")
	if !strings.Contains(status, "403") {
		t.Fatalf("expected 403, got: %s", status)
	}
	t.Logf("correctly got 403: %s", status)
}

func TestExplorerEndpointConnectionCap(t *testing.T) {
	dir := t.TempDir()
	cfgPath := writeExplorerConfig(t, dir, `, "explorer_max_connections": 2`)
	brokerAddr, httpAddr, _ := startExplorerTestBrokerWithConfig(t, cfgPath)
	createTopicViaProto(t, brokerAddr, "exp-cap", 1)

	ws1 := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=exp-cap", httpAddr))
	defer ws1.Close()
	ws2 := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=exp-cap", httpAddr))
	defer ws2.Close()
	time.Sleep(100 * time.Millisecond)

	// 3rd should get 503.
	status := rawHTTPUpgrade(t, httpAddr, "/explorer/stream?topic=exp-cap", "")
	if !strings.Contains(status, "503") {
		t.Fatalf("expected 503, got: %s", status)
	}
	t.Logf("correctly got 503: %s", status)
}

func TestExplorerEndpointRBAC(t *testing.T) {
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "explorer-rbac"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 100,
		            "read_timeout": 5000000000, "write_timeout": 5000000000,
		            "explorer_enabled": true, "explorer_max_connections": 50},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth": {
			"enabled": true,
			"api_keys": [
				{"key": "viewer-key", "client_id": "viewer", "role": "consumer", "topics": ["allowed-topic"]}
			]
		},
		"rate_limit":  {"enabled": false},
		"logging":     {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "broker.json")
	_ = os.WriteFile(cfgPath, []byte(cfgData), 0o644)
	_, httpAddr, b := startExplorerTestBrokerWithConfig(t, cfgPath)

	createTopicViaProto(t, b.Addr(), "restricted-topic", 1)
	time.Sleep(100 * time.Millisecond)

	status := rawHTTPUpgrade(t, httpAddr, "/explorer/stream?topic=restricted-topic",
		"Authorization: Bearer viewer-key\r\n")
	if !strings.Contains(status, "403") {
		t.Fatalf("expected 403, got: %s", status)
	}
	t.Logf("correctly got 403: %s", status)
}

// publishViaProtoAuth authenticates a new connection with apiKey, then
// publishes a single message and returns partition + offset.
func publishViaProtoAuth(t *testing.T, addr, apiKey, topic, key, payload string) (int32, int64) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	_ = enc.Encode(protocol.CmdAuth, 1, &protocol.AuthRequest{APIKey: apiKey})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = dec.Decode()
	seq := uint64(time.Now().UnixNano())
	_ = enc.Encode(protocol.CmdPublish, seq, &protocol.PublishRequest{
		Topic: topic, Key: key, Payload: []byte(payload),
	})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	var resp protocol.PublishResponse
	_ = json.Unmarshal(f.Body, &resp)
	return resp.Partition, resp.Offset
}

// createTopicViaProtoAuth authenticates and then creates a topic.
func createTopicViaProtoAuth(t *testing.T, addr, apiKey, topic string, partitions int) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	_ = enc.Encode(protocol.CmdAuth, 1, &protocol.AuthRequest{APIKey: apiKey})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = dec.Decode()
	_ = enc.Encode(protocol.CmdCreateTopic, 2, &protocol.CreateTopicRequest{
		Name: topic, Partitions: partitions, ReplicationFactor: 1,
	})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = dec.Decode()
}

// TestExplorerTimestampPopulated verifies that the Explorer WebSocket frame
// carries a non-zero timestamp_ns that is close to the publish time.
func TestExplorerTimestampPopulated(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerTestBroker(t, "")
	createTopicViaProto(t, brokerAddr, "ts-topic", 1)

	ws := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=ts-topic", httpAddr))
	defer ws.Close()
	time.Sleep(100 * time.Millisecond)

	before := time.Now().UnixNano()
	publishViaProto(t, brokerAddr, "ts-topic", "k", "hello-ts")
	after := time.Now().UnixNano()

	_ = ws.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	text := ws.readTextFrame(t)
	var frame struct {
		TimestampNs int64  `json:"timestamp_ns"`
		Payload     string `json:"payload"`
	}
	if err := json.Unmarshal([]byte(text), &frame); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}

	const margin = int64(time.Second)
	if frame.TimestampNs == 0 {
		t.Fatal("timestamp_ns is zero — Explorer frame missing timestamp")
	}
	if frame.TimestampNs < before-margin || frame.TimestampNs > after+margin {
		t.Errorf("timestamp_ns=%d outside publish window [%d, %d]",
			frame.TimestampNs, before-margin, after+margin)
	}
	t.Logf("timestamp_ns=%d (before=%d after=%d)", frame.TimestampNs, before, after)
}

// TestExplorerEndpointProducerFieldFromRealPublish verifies that the
// "producer" field in the Explorer frame is populated from the actual
// publishing client's ClientID — not from msg.Headers["producer"].
func TestExplorerEndpointProducerFieldFromRealPublish(t *testing.T) {
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "explorer-pid"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 100,
		            "read_timeout": 5000000000, "write_timeout": 5000000000,
		            "explorer_enabled": true, "explorer_max_connections": 50},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth": {
			"enabled": true,
			"api_keys": [
				{"key": "prod-key-123", "client_id": "real-producer", "role": "admin", "topics": ["pid-topic"]}
			]
		},
		"rate_limit":  {"enabled": false},
		"logging":     {"level": "error", "format": "json"}
	}`, filepath.Join(dir, "wal"), filepath.Join(dir, "data"))

	cfgPath := filepath.Join(dir, "broker.json")
	_ = os.WriteFile(cfgPath, []byte(cfgData), 0o644)
	brokerAddr, httpAddr, _ := startExplorerTestBrokerWithConfig(t, cfgPath)

	// Create topic via authenticated protocol connection.
	createTopicViaProtoAuth(t, brokerAddr, "prod-key-123", "pid-topic", 1)
	time.Sleep(100 * time.Millisecond)

	// Publish via protocol with authentication.
	publishViaProtoAuth(t, brokerAddr, "prod-key-123", "pid-topic", "k1", "producer-test")

	// Connect to Explorer with the same API key (Bearer token auth).
	ws := dialWSAuth(t, httpAddr, "/explorer/stream?topic=pid-topic", "prod-key-123")
	defer ws.Close()
	time.Sleep(100 * time.Millisecond)

	// Publish another message so the Explorer session definitely sees it.
	publishViaProtoAuth(t, brokerAddr, "prod-key-123", "pid-topic", "k2", "producer-test-2")

	_ = ws.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	text := ws.readTextFrame(t)
	var frame struct {
		Producer string `json:"producer"`
		Payload  string `json:"payload"`
	}
	if err := json.Unmarshal([]byte(text), &frame); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	if frame.Producer != "real-producer" {
		t.Errorf("expected producer %q, got %q", "real-producer", frame.Producer)
	}
	t.Logf("producer=%q payload=%s", frame.Producer, frame.Payload)
}

// dialWSAuth is like dialWS but sends an Authorization header with the
// given API key for HTTP-level auth (explorer endpoint uses HTTP auth).
func dialWSAuth(t *testing.T, httpAddr, path, apiKey string) *wsTestClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", httpAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand: %v", err)
	}
	wsKey := base64.StdEncoding.EncodeToString(keyBytes)
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + httpAddr + "\r\n" +
		"Authorization: Bearer " + apiKey + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + wsKey + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("ws handshake write: %v", err)
	}
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("ws read status: %v", err)
	}
	if !bytes.Contains([]byte(statusLine), []byte("101")) {
		for {
			line, lerr := br.ReadString('\n')
			if lerr != nil || line == "\r\n" || line == "\n" {
				break
			}
		}
		t.Fatalf("ws handshake failed: %s", strings.TrimSpace(statusLine))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil || line == "\r\n" || line == "\n" {
			break
		}
	}
	return &wsTestClient{conn: conn, br: br}
}
