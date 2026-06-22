package broker_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
)

// wsDialWithCookie performs a WebSocket handshake with the given session cookie.
func wsDialWithCookie(t *testing.T, httpAddr, path, cookieVal string) *wsTestClient {
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
	cookieLine := ""
	if cookieVal != "" {
		cookieLine = "Cookie: pubsub_dashboard_session=" + cookieVal + "\r\n"
	}
	req := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + httpAddr + "\r\n" +
		cookieLine +
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

// startExplorerAuthBroker starts a broker with auth enabled for explorer tests.
func startExplorerAuthBroker(t *testing.T) (brokerAddr, httpAddr string, b *broker.Broker) {
	t.Helper()
	dir := t.TempDir()
	cfgData := fmt.Sprintf(`{
		"broker":  {"node_id": "explorer-auth-test"},
		"network": {"host": "127.0.0.1", "port": 0, "max_connections": 100,
		            "read_timeout": 5000000000, "write_timeout": 5000000000,
		            "explorer_enabled": true, "explorer_max_connections": 50},
		"storage": {"wal_path": %q, "data_path": %q,
		            "segment_max_bytes": 1048576, "index_interval_bytes": 512, "sync_policy": "always"},
		"auth": {
			"enabled": true,
			"api_keys": [
				{"key": "explorer-admin", "client_id": "admin-user", "role": "admin"},
				{"key": "explorer-viewer", "client_id": "viewer-user", "role": "consumer", "topics": ["allowed-topic"]}
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

	// Wait for HTTP server to be ready.
	httpAddr = b.HTTPAddr()
	httpDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(httpDeadline) {
		resp, err := http.Get("http://" + httpAddr + "/healthz/live")
		if err == nil {
			resp.Body.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
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
	return b.Addr(), httpAddr, b
}

func TestExplorerEndpointAuthViaSessionCookie(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerAuthBroker(t)

	// Create topic via protocol with authentication.
	conn, err := net.DialTimeout("tcp", brokerAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)
	_ = enc.Encode(protocol.CmdAuth, 1, &protocol.AuthRequest{APIKey: "explorer-admin"})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = dec.Decode()
	_ = enc.Encode(protocol.CmdCreateTopic, 2, &protocol.CreateTopicRequest{
		Name: "exp-cookie-topic", Partitions: 1, ReplicationFactor: 1,
	})
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = dec.Decode()
	conn.Close()

	time.Sleep(100 * time.Millisecond)

	// Login to get session cookie.
	body, _ := json.Marshal(map[string]string{"api_key": "explorer-admin"})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status %d", resp.StatusCode)
	}
	var cookieVal string
	for _, c := range resp.Cookies() {
		if c.Name == "pubsub_dashboard_session" {
			cookieVal = c.Value
		}
	}
	if cookieVal == "" {
		t.Fatal("no session cookie")
	}

	// Connect to Explorer WebSocket using session cookie.
	ws := wsDialWithCookie(t, httpAddr, "/explorer/stream?topic=exp-cookie-topic", cookieVal)
	defer ws.Close()
	time.Sleep(200 * time.Millisecond)

	// Publish a message.
	pubConn, err := net.DialTimeout("tcp", brokerAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial publish: %v", err)
	}
	pubEnc := protocol.NewEncoder(pubConn)
	pubDec := protocol.NewDecoder(pubConn)
	_ = pubEnc.Encode(protocol.CmdAuth, 1, &protocol.AuthRequest{APIKey: "explorer-admin"})
	_ = pubConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = pubDec.Decode()
	_ = pubEnc.Encode(protocol.CmdPublish, 2, &protocol.PublishRequest{
		Topic: "exp-cookie-topic", Key: "k1", Payload: []byte("cookie-auth-msg"),
	})
	_ = pubConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _ = pubDec.Decode()
	pubConn.Close()

	// Read from WebSocket.
	_ = ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	text, err := ws.tryReadTextFrame()
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var frame struct {
		Payload string `json:"payload"`
	}
	if err := json.Unmarshal([]byte(text), &frame); err != nil {
		t.Fatalf("unmarshal frame: %v", err)
	}
	payload, _ := base64.StdEncoding.DecodeString(frame.Payload)
	if string(payload) != "cookie-auth-msg" {
		t.Errorf("payload = %q, want %q", string(payload), "cookie-auth-msg")
	}
}

func TestExplorerEndpointAuthViaCookieRespectsTopicAllowlist(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerAuthBroker(t)

	// Create topics.
	for _, topic := range []string{"restricted-topic", "allowed-topic"} {
		conn, err := net.DialTimeout("tcp", brokerAddr, 3*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		enc := protocol.NewEncoder(conn)
		dec := protocol.NewDecoder(conn)
		_ = enc.Encode(protocol.CmdAuth, 1, &protocol.AuthRequest{APIKey: "explorer-admin"})
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = dec.Decode()
		_ = enc.Encode(protocol.CmdCreateTopic, 2, &protocol.CreateTopicRequest{
			Name: topic, Partitions: 1, ReplicationFactor: 1,
		})
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		_, _ = dec.Decode()
		conn.Close()
	}
	time.Sleep(100 * time.Millisecond)

	// Login with viewer key (topics: ["allowed-topic"]).
	body, _ := json.Marshal(map[string]string{"api_key": "explorer-viewer"})
	resp, err := http.Post("http://"+httpAddr+"/dashboard/login", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("login status %d", resp.StatusCode)
	}
	var cookieVal string
	for _, c := range resp.Cookies() {
		if c.Name == "pubsub_dashboard_session" {
			cookieVal = c.Value
		}
	}
	if cookieVal == "" {
		t.Fatal("no session cookie")
	}

	// Try connecting to restricted-topic → should fail (403).
	status := rawWSUpgradeWithCookie(t, httpAddr, "/explorer/stream?topic=restricted-topic", cookieVal)
	if !strings.Contains(status, "403") {
		t.Fatalf("expected 403 for restricted topic, got: %s", status)
	}

	// Try connecting to allowed-topic → should succeed (101).
	status2 := rawWSUpgradeWithCookie(t, httpAddr, "/explorer/stream?topic=allowed-topic", cookieVal)
	if !strings.Contains(status2, "101") {
		t.Fatalf("expected 101 for allowed topic, got: %s", status2)
	}
}

func TestExplorerEndpointNoCookieReturns401(t *testing.T) {
	_, httpAddr, _ := startExplorerAuthBroker(t)

	status := rawWSUpgradeWithCookie(t, httpAddr, "/explorer/stream?topic=any", "")
	if !strings.Contains(status, "401") {
		t.Fatalf("expected 401 without auth, got: %s", status)
	}
}

// rawWSUpgradeWithCookie performs a raw HTTP upgrade with the given cookie.
func rawWSUpgradeWithCookie(t *testing.T, httpAddr, path, cookieVal string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", httpAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	keyBytes := make([]byte, 16)
	_, _ = rand.Read(keyBytes)
	key := base64.StdEncoding.EncodeToString(keyBytes)
	cookieLine := ""
	if cookieVal != "" {
		cookieLine = "Cookie: pubsub_dashboard_session=" + cookieVal + "\r\n"
	}
	req := "GET " + path + " HTTP/1.1\r\nHost: " + httpAddr + "\r\n" +
		cookieLine +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\nSec-WebSocket-Version: 13\r\n\r\n"
	_, _ = conn.Write([]byte(req))
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(conn)
	sl, _ := br.ReadString('\n')
	for {
		line, lerr := br.ReadString('\n')
		if lerr != nil || line == "\r\n" || line == "\n" {
			break
		}
	}
	return strings.TrimSpace(sl)
}
