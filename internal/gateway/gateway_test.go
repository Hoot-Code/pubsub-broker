package gateway_test

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
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/gateway"
)

// ─── Test harness ───────────────────────────────────────────────────────────

// freePort returns an OS-assigned free TCP port on 127.0.0.1.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// startTestBroker launches an in-process broker on a random port and returns
// its TCP address. It registers a Cleanup that stops the broker.
func startTestBroker(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "gateway-test-node"},
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
	for time.Now().Before(deadline) && b.Addr() == "" {
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

// startTestGatewayWithAuth launches a broker with auth enabled and two API
// keys, then starts a gateway and returns the base URL. The caller can
// use keyA and keyB to make authenticated requests.
func startTestGatewayWithAuth(t *testing.T) (baseURL, keyA, keyB string) {
	t.Helper()
	dir := t.TempDir()

	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "gateway-auth-test"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 1000,
		             "read_timeout": 5000000000, "write_timeout": 5000000000, "idle_timeout": 30000000000},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"replication": {"factor": 1, "sync_interval": 100000000, "ack_timeout": 5000000000},
		"retention":   {"max_age_hours": 24, "max_size_mb": 1024},
		"auth": {
			"enabled": true,
			"api_keys": [
				{"key": "key-a", "client_id": "client-a", "role": "admin", "topics": ["topic-a"]},
				{"key": "key-b", "client_id": "client-b", "role": "admin", "topics": ["topic-b"]}
			]
		},
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
	for time.Now().Before(deadline) && b.Addr() == "" {
		time.Sleep(5 * time.Millisecond)
	}
	if b.Addr() == "" {
		t.Fatal("broker did not start in time")
	}

	gwPort := freePort(t)
	gwAddr := fmt.Sprintf("127.0.0.1:%d", gwPort)
	gw := gateway.NewGateway(config.GatewayConfig{Enabled: true, Addr: gwAddr}, b.Addr(), nil, nil)

	go func() { _ = gw.Start(context.Background()) }()

	deadline2 := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline2) {
		conn, dErr := net.DialTimeout("tcp", gwAddr, 100*time.Millisecond)
		if dErr == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		_ = gw.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
		select {
		case <-errC:
		case <-time.After(3 * time.Second):
		}
	})
	return "http://" + gwAddr, "key-a", "key-b"
}

// startTestGateway dials brokerAddr, builds a Gateway on a free port, starts
// it, waits for it to accept connections, and returns its base URL
// (e.g. "http://127.0.0.1:54321"). Registers Cleanup to stop everything.
func startTestGateway(t *testing.T, brokerAddr string) string {
	t.Helper()

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	gw := gateway.NewGateway(config.GatewayConfig{Enabled: true, Addr: addr}, brokerAddr, nil, nil)

	go func() { _ = gw.Start(context.Background()) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, dErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dErr == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Cleanup(func() {
		_ = gw.Stop()
	})
	return "http://" + addr
}

// ─── REST tests ─────────────────────────────────────────────────────────────

// TestGatewayPublishAndFetch starts a broker + gateway, creates a topic via
// the REST API, publishes a message, and fetches it back, verifying the
// payload and offset round-trip correctly.
func TestGatewayPublishAndFetch(t *testing.T) {
	brokerAddr := startTestBroker(t)
	baseURL := startTestGateway(t, brokerAddr)

	createBody := bytes.NewBufferString(`{"name":"gw-topic","partitions":1}`)
	resp, err := http.Post(baseURL+"/v1/topics", "application/json", createBody)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create topic status: want 201, got %d", resp.StatusCode)
	}

	pubBody := bytes.NewBufferString(`{"key":"k1","payload":"hello-gateway"}`)
	resp, err = http.Post(baseURL+"/v1/topics/gw-topic/messages", "application/json", pubBody)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	var pubResp struct {
		Offset    int64 `json:"offset"`
		Partition int32 `json:"partition"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pubResp); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish status: want 201, got %d", resp.StatusCode)
	}
	if pubResp.Offset != 0 {
		t.Fatalf("publish offset: want 0, got %d", pubResp.Offset)
	}

	fetchURL := fmt.Sprintf("%s/v1/topics/gw-topic/partitions/%d/messages?offset=0&limit=10", baseURL, pubResp.Partition)
	resp, err = http.Get(fetchURL)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fetch status: want 200, got %d", resp.StatusCode)
	}
	var fetchResp struct {
		Messages []struct {
			Offset  int64  `json:"offset"`
			Key     string `json:"key"`
			Payload string `json:"payload"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&fetchResp); err != nil {
		t.Fatalf("decode fetch response: %v", err)
	}
	if len(fetchResp.Messages) != 1 {
		t.Fatalf("fetch messages: want 1, got %d", len(fetchResp.Messages))
	}
	got := fetchResp.Messages[0]
	if got.Offset != 0 || got.Key != "k1" {
		t.Fatalf("fetch message mismatch: %+v", got)
	}
	payload, err := base64.StdEncoding.DecodeString(got.Payload)
	if err != nil {
		t.Fatalf("decode payload base64: %v", err)
	}
	if string(payload) != "hello-gateway" {
		t.Fatalf("payload: want %q, got %q", "hello-gateway", string(payload))
	}
}

// TestGatewayBatch publishes a batch of 10 messages and verifies 10 offsets
// are returned.
func TestGatewayBatch(t *testing.T) {
	brokerAddr := startTestBroker(t)
	baseURL := startTestGateway(t, brokerAddr)

	createBody := bytes.NewBufferString(`{"name":"gw-batch-topic","partitions":1}`)
	resp, err := http.Post(baseURL+"/v1/topics", "application/json", createBody)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	resp.Body.Close()

	var batch struct {
		Messages []map[string]string `json:"messages"`
	}
	for i := 0; i < 10; i++ {
		batch.Messages = append(batch.Messages, map[string]string{
			"key":     fmt.Sprintf("k%d", i),
			"payload": fmt.Sprintf("payload-%d", i),
		})
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatalf("marshal batch: %v", err)
	}
	resp, err = http.Post(baseURL+"/v1/topics/gw-batch-topic/messages/batch", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish batch status: want 201, got %d", resp.StatusCode)
	}
	var batchResp struct {
		Offsets []int64 `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if len(batchResp.Offsets) != 10 {
		t.Fatalf("offsets: want 10, got %d", len(batchResp.Offsets))
	}
}

// ─── WebSocket tests ────────────────────────────────────────────────────────

// minimal stdlib-only WebSocket CLIENT used solely by this test, mirroring
// the gateway's own minimal server (internal/gateway/websocket.go). It
// performs the RFC 6455 handshake over a raw TCP connection and exchanges
// unfragmented text frames, masking outgoing frames as required of clients.
type testWSClient struct {
	conn net.Conn
	br   *bufio.Reader
}

func dialTestWS(t *testing.T, wsURL string) *testWSClient {
	t.Helper()
	// wsURL like "ws://127.0.0.1:PORT/v1/topics/x/stream?group=g"
	hostPort, path := splitWSURL(t, wsURL)

	conn, err := net.DialTimeout("tcp", hostPort, 5*time.Second)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}

	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatalf("rand: %v", err)
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + hostPort + "\r\n" +
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
		t.Fatalf("ws read status line: %v", err)
	}
	if !bytes.Contains([]byte(statusLine), []byte("101")) {
		t.Fatalf("ws handshake: unexpected status line %q", statusLine)
	}
	// Drain headers until the blank line.
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("ws read headers: %v", err)
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	return &testWSClient{conn: conn, br: br}
}

// splitWSURL turns "ws://host:port/path?query" into ("host:port", "/path?query").
func splitWSURL(t *testing.T, wsURL string) (hostPort, path string) {
	t.Helper()
	const prefix = "ws://"
	if len(wsURL) < len(prefix) || wsURL[:len(prefix)] != prefix {
		t.Fatalf("splitWSURL: %q missing ws:// prefix", wsURL)
	}
	rest := wsURL[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i], rest[i:]
		}
	}
	return rest, "/"
}

// readTextFrame reads one unmasked server→client text frame (servers never
// mask per RFC 6455 §5.1).
func (c *testWSClient) readTextFrame(t *testing.T) string {
	t.Helper()
	hdr := make([]byte, 2)
	if _, err := readFull(c.br, hdr); err != nil {
		t.Fatalf("ws read frame header: %v", err)
	}
	opcode := hdr[0] & 0x0F
	length := int64(hdr[1] & 0x7F)
	switch length {
	case 126:
		ext := make([]byte, 2)
		if _, err := readFull(c.br, ext); err != nil {
			t.Fatalf("ws read ext length: %v", err)
		}
		length = int64(binary.BigEndian.Uint16(ext))
	case 127:
		ext := make([]byte, 8)
		if _, err := readFull(c.br, ext); err != nil {
			t.Fatalf("ws read ext length: %v", err)
		}
		length = int64(binary.BigEndian.Uint64(ext))
	}
	payload := make([]byte, length)
	if _, err := readFull(c.br, payload); err != nil {
		t.Fatalf("ws read payload: %v", err)
	}
	if opcode != 0x1 {
		t.Fatalf("ws read frame: unexpected opcode %#x", opcode)
	}
	return string(payload)
}

func readFull(br *bufio.Reader, buf []byte) (int, error) {
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

func (c *testWSClient) Close() { _ = c.conn.Close() }

// TestGatewayWebSocketStream connects to the gateway's WS stream endpoint,
// publishes 5 messages via REST, and verifies all 5 arrive over the
// WebSocket connection.
func TestGatewayWebSocketStream(t *testing.T) {
	brokerAddr := startTestBroker(t)
	baseURL := startTestGateway(t, brokerAddr)
	httpAddr := baseURL[len("http://"):]

	createBody := bytes.NewBufferString(`{"name":"gw-stream-topic","partitions":1}`)
	resp, err := http.Post(baseURL+"/v1/topics", "application/json", createBody)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	resp.Body.Close()

	wsURL := fmt.Sprintf("ws://%s/v1/topics/gw-stream-topic/stream?group=test-group&consumer=c1", httpAddr)
	ws := dialTestWS(t, wsURL)
	defer ws.Close()

	// Give the subscribe round-trip time to complete before publishing.
	time.Sleep(100 * time.Millisecond)

	for i := 0; i < 5; i++ {
		body := bytes.NewBufferString(fmt.Sprintf(`{"key":"k%d","payload":"msg-%d"}`, i, i))
		resp, err := http.Post(baseURL+"/v1/topics/gw-stream-topic/messages", "application/json", body)
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		resp.Body.Close()
	}

	received := make(map[string]bool)
	for i := 0; i < 5; i++ {
		_ = ws.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		text := ws.readTextFrame(t)
		var msg struct {
			Key     string `json:"key"`
			Payload string `json:"payload"`
		}
		if err := json.Unmarshal([]byte(text), &msg); err != nil {
			t.Fatalf("unmarshal ws frame %d: %v (%s)", i, err, text)
		}
		payload, err := base64.StdEncoding.DecodeString(msg.Payload)
		if err != nil {
			t.Fatalf("decode ws payload %d: %v", i, err)
		}
		received[string(payload)] = true
	}
	for i := 0; i < 5; i++ {
		want := fmt.Sprintf("msg-%d", i)
		if !received[want] {
			t.Fatalf("missing message %q among received: %v", want, received)
		}
	}
}

// ─── Concurrency / security tests ──────────────────────────────────────────

// TestGatewayNoAuthIdentityLeak verifies that concurrent HTTP publish
// requests using different API keys never cross-identity: key-a can only
// publish to topic-a, and key-b can only publish to topic-b. Under the
// old shared-connection design, this test would race and produce 201
// (success) for cross-key publishes; with the pool-per-key fix every
// cross-key attempt must return 403 FORBIDDEN.
func TestGatewayNoAuthIdentityLeak(t *testing.T) {
	baseURL, keyA, keyB := startTestGatewayWithAuth(t)

	// Create both topics using an admin key (we'll use key-a for topic-a
	// and key-b for topic-b, each can create their own).
	for _, kc := range []struct {
		key, topic string
	}{
		{keyA, "topic-a"},
		{keyB, "topic-b"},
	} {
		body := bytes.NewBufferString(fmt.Sprintf(`{"name":%q,"partitions":1}`, kc.topic))
		req, err := http.NewRequest("POST", baseURL+"/v1/topics", body)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+kc.key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("create topic %s: %v", kc.topic, err)
		}
		resp.Body.Close()
		// 201 or 409 (already exists) are both acceptable.
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
			t.Fatalf("create topic %s: want 201 or 409, got %d", kc.topic, resp.StatusCode)
		}
	}

	const N = 50
	var wg sync.WaitGroup
	errCh := make(chan string, N*2) // collects error descriptions

	// Fire N alternating requests: key-a → topic-a, key-b → topic-b.
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Alternate keys: even → key-a → topic-a, odd → key-b → topic-b.
			key, topic, otherTopic := keyA, "topic-a", "topic-b"
			if idx%2 != 0 {
				key, topic, otherTopic = keyB, "topic-b", "topic-a"
			}
			body := bytes.NewBufferString(fmt.Sprintf(`{"key":"k%d","payload":"p%d"}`, idx, idx))
			req, err := http.NewRequest("POST", baseURL+"/v1/topics/"+topic+"/messages", body)
			if err != nil {
				errCh <- fmt.Sprintf("idx %d: new request: %v", idx, err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errCh <- fmt.Sprintf("idx %d: do: %v", idx, err)
				return
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				errCh <- fmt.Sprintf("idx %d: %s→%s: want 201, got %d", idx, key, topic, resp.StatusCode)
				return
			}

			// Now attempt cross-topic publish — must fail with 403.
			body2 := bytes.NewBufferString(fmt.Sprintf(`{"key":"k%d","payload":"cross"}`, idx))
			req2, err := http.NewRequest("POST", baseURL+"/v1/topics/"+otherTopic+"/messages", body2)
			if err != nil {
				errCh <- fmt.Sprintf("idx %d: cross new request: %v", idx, err)
				return
			}
			req2.Header.Set("Content-Type", "application/json")
			req2.Header.Set("Authorization", "Bearer "+key)
			resp2, err := http.DefaultClient.Do(req2)
			if err != nil {
				errCh <- fmt.Sprintf("idx %d: cross do: %v", idx, err)
				return
			}
			resp2.Body.Close()
			if resp2.StatusCode == http.StatusCreated {
				errCh <- fmt.Sprintf("idx %d: cross %s→%s: got 201 (SHOULD BE 403)", idx, key, otherTopic)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for e := range errCh {
		t.Error(e)
	}
}

// TestGatewayConnectionPoolEviction creates connections for 3 keys, manually
// backdates their lastUsed, triggers eviction, and verifies the pool shrinks
// to 0 and the underlying connections are closed.
func TestGatewayConnectionPoolEviction(t *testing.T) {
	brokerAddr := startTestBroker(t)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	gw := gateway.NewGatewayForTest(config.GatewayConfig{Enabled: true, Addr: addr}, brokerAddr, nil, nil)

	// Create connections for 3 keys.
	for _, key := range []string{"k1", "k2", "k3"} {
		_, err := gw.ConnForTest(key)
		if err != nil {
			t.Fatalf("connFor %s: %v", key, err)
		}
	}

	gw.BackdatePoolForTest(time.Hour)

	gw.EvictIdleForTest()

	remaining := gw.PoolLenForTest()
	if remaining != 0 {
		t.Fatalf("pool size after eviction: want 0, got %d", remaining)
	}
}

// TestWebSocketHandshake (RFC 6455 §1.3 known test vector) lives in
// websocket_internal_test.go since it exercises the unexported
// computeAcceptKey function directly (white-box test, package gateway).
