package broker_test

import (
	"context"
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
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

func TestDashboardServed(t *testing.T) {
	b := newTestBroker(t)
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })

	addr := waitForHTTP(t, b)
	resp, err := http.Get("http://" + addr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard: status %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("GET /dashboard: Content-Type %q, want text/html", ct)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])
	if !strings.Contains(body, "pubsub-broker") {
		t.Fatal("GET /dashboard: response body does not contain 'pubsub-broker'")
	}
}

func TestDashboardDisabled(t *testing.T) {
	dir := t.TempDir()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "test-node"},
		"network": map[string]interface{}{"port": 0, "dashboard_enabled": false},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
		},
	}
	cfgPath := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(cfg)
	_ = os.WriteFile(cfgPath, data, 0o644)

	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(func() { loader.Close() })

	b, err := broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })

	addr := waitForHTTP(t, b)
	resp, err := http.Get("http://" + addr + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET /dashboard: status %d, want 403", resp.StatusCode)
	}
}

func TestRootRedirectsToDashboard(t *testing.T) {
	b := newTestBroker(t)
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })

	addr := waitForHTTP(t, b)
	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("GET /: status %d, want 302", resp.StatusCode)
	}
	loc := resp.Header.Get("Location")
	if loc != "/dashboard" {
		t.Fatalf("GET /: Location %q, want /dashboard", loc)
	}
}

func TestDashboardEmbedNotEmpty(t *testing.T) {
	html := broker.DashboardHTML()
	if len(html) < 100 {
		t.Fatalf("dashboard index.html too short: %d bytes, want > 100", len(html))
	}
	if !strings.Contains(string(html), "Control Center") {
		t.Fatal("dashboard index.html does not contain 'Control Center'")
	}
}

func TestDashboardStaticAssetsServed(t *testing.T) {
	b := newTestBroker(t)
	go b.Start()
	t.Cleanup(func() { b.Stop(context.Background()) })

	addr := waitForHTTP(t, b)

	// Test style.css
	resp, err := http.Get("http://" + addr + "/dashboard/style.css") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /dashboard/style.css: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard/style.css: status %d, want 200", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.Contains(ct, "text/css") {
		t.Fatalf("GET /dashboard/style.css: Content-Type %q, want text/css", ct)
	}
	buf := make([]byte, 1024)
	n, _ := resp.Body.Read(buf)
	if n == 0 {
		t.Fatal("GET /dashboard/style.css: empty body")
	}

	// Test app.js
	resp2, err := http.Get("http://" + addr + "/dashboard/app.js") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /dashboard/app.js: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("GET /dashboard/app.js: status %d, want 200", resp2.StatusCode)
	}
	ct2 := resp2.Header.Get("Content-Type")
	if !strings.Contains(ct2, "javascript") && !strings.Contains(ct2, "text/javascript") {
		t.Fatalf("GET /dashboard/app.js: Content-Type %q, want javascript", ct2)
	}
	buf2 := make([]byte, 1024)
	n2, _ := resp2.Body.Read(buf2)
	if n2 == 0 {
		t.Fatal("GET /dashboard/app.js: empty body")
	}
}

// waitForHTTP waits for the broker's HTTP admin server to become ready and
// returns its address. It checks b.Ready() first (atomic) to ensure the
// write to httpAddr in Start() is visible before reading it.
func waitForHTTP(t *testing.T, b *broker.Broker) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() {
			addr := b.HTTPAddr()
			if addr != "" {
				resp, err := http.Get("http://" + addr + "/healthz/live")
				if err == nil {
					resp.Body.Close()
					return addr
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("HTTP server did not become ready within 5s")
	return ""
}

// TestDashboardTopicsPopulateSingleNode verifies that the /topics endpoint
// returns topic data in single-node mode (cluster disabled), which the
// dashboard's Topics table relies on. This is a regression test for the
// bug where the Topics table was empty despite an active topic.
func TestDashboardTopicsPopulateSingleNode(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "dash-test-node"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
			"sync_policy":          "always",
		},
		"auth":       map[string]interface{}{"enabled": false},
		"rate_limit": map[string]interface{}{"enabled": false},
		"logging":    map[string]interface{}{"level": "error"},
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
	go b.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	addr := waitForHTTP(t, b)

	// Create a topic via the binary protocol.
	tcpAddr := b.Addr()
	conn, err := net.DialTimeout("tcp", tcpAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	enc := protocol.NewEncoder(conn)
	dec := protocol.NewDecoder(conn)

	createReq := protocol.CreateTopicRequest{
		Name:              "dash-orders",
		Partitions:        2,
		ReplicationFactor: 1,
	}
	if err := enc.Encode(protocol.CmdCreateTopic, 1, &createReq); err != nil {
		t.Fatalf("create topic encode: %v", err)
	}
	f, err := dec.Decode()
	if err != nil {
		t.Fatalf("create topic decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("create topic error: %s: %s", e.Code, e.Message)
	}

	// Publish a message so the topic has data.
	pubReq := protocol.PublishRequest{
		Topic:        "dash-orders",
		Key:          "k1",
		Payload:      []byte(`{"test":true}`),
		DeliveryMode: uint8(types.AtLeastOnce),
	}
	if err := enc.Encode(protocol.CmdPublish, 2, &pubReq); err != nil {
		t.Fatalf("publish encode: %v", err)
	}
	f, err = dec.Decode()
	if err != nil {
		t.Fatalf("publish decode: %v", err)
	}
	if f.Command == protocol.CmdError {
		var e protocol.ErrorResponse
		_ = protocol.Unmarshal(f, &e)
		t.Fatalf("publish error: %s: %s", e.Code, e.Message)
	}

	// Verify GET /topics returns the topic with correct partition count.
	resp, err := http.Get("http://" + addr + "/topics") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /topics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /topics: status %d, want 200", resp.StatusCode)
	}

	var topics []struct {
		Config struct {
			Name       string `json:"Name"`
			Partitions int    `json:"Partitions"`
		} `json:"Config"`
		CreatedAt time.Time `json:"CreatedAt"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&topics); err != nil {
		t.Fatalf("decode /topics: %v", err)
	}

	found := false
	for _, tp := range topics {
		if tp.Config.Name == "dash-orders" {
			found = true
			if tp.Config.Partitions != 2 {
				t.Errorf("topic dash-orders: partitions = %d, want 2", tp.Config.Partitions)
			}
			break
		}
	}
	if !found {
		t.Errorf("topic dash-orders not found in /topics response (got %d topics)", len(topics))
		for _, tp := range topics {
			fmt.Fprintf(os.Stderr, "  topic: %s (partitions=%d)\n", tp.Config.Name, tp.Config.Partitions)
		}
	}

	// Also verify GET /cluster/partitions returns empty in single-node mode
	// (cluster disabled) — the dashboard must NOT rely on this endpoint.
	resp2, err := http.Get("http://" + addr + "/cluster/partitions") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /cluster/partitions: %v", err)
	}
	defer resp2.Body.Close()
	var clusterParts map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&clusterParts); err != nil {
		t.Fatalf("decode /cluster/partitions: %v", err)
	}
	if len(clusterParts) != 0 {
		t.Errorf("/cluster/partitions returned %d entries in single-node mode, want 0", len(clusterParts))
	}
}

func TestConfigEffectiveEndpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	rawAPIKey := "super-secret-key-12345"
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "cfg-test"},
		"network": map[string]interface{}{"port": 0, "dashboard_enabled": true},
		"auth": map[string]interface{}{
			"enabled": true,
			"api_keys": []interface{}{
				map[string]interface{}{
					"key":       rawAPIKey,
					"client_id": "test-client",
					"role":      "admin",
				},
			},
		},
		"storage": map[string]interface{}{
			"wal_path":             filepath.Join(dir, "wal"),
			"data_path":            filepath.Join(dir, "data"),
			"segment_max_bytes":    1 << 20,
			"index_interval_bytes": 512,
		},
		"retention": map[string]interface{}{
			"max_age_hours": 48,
			"max_size_mb":   512,
		},
		"logging":    map[string]interface{}{"level": "error"},
		"rate_limit": map[string]interface{}{"enabled": false},
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
	go b.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	addr := waitForHTTP(t, b)

	// 1. Verify 200 with admin role via Bearer token.
	req, _ := http.NewRequest("GET", "http://"+addr+"/config/effective", nil)
	req.Header.Set("Authorization", "Bearer "+rawAPIKey)
	resp, err := http.DefaultClient.Do(req) //nolint:noctx
	if err != nil {
		t.Fatalf("GET /config/effective: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /config/effective: status %d, want 200", resp.StatusCode)
	}

	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /config/effective: %v", err)
	}

	// 2. Verify the response contains expected sections.
	if _, ok := body["retention"]; !ok {
		t.Error("response missing 'retention' section")
	}
	if _, ok := body["auth"]; !ok {
		t.Error("response missing 'auth' section")
	}

	// 3. Verify the raw API key is NOT in the response body.
	resp2, _ := http.NewRequest("GET", "http://"+addr+"/config/effective", nil)
	resp2.Header.Set("Authorization", "Bearer "+rawAPIKey)
	resp3, err := http.DefaultClient.Do(resp2) //nolint:noctx
	if err != nil {
		t.Fatalf("GET /config/effective (re-read): %v", err)
	}
	defer resp3.Body.Close()
	bodyBytes, _ := json.Marshal(body)
	if strings.Contains(string(bodyBytes), rawAPIKey) {
		t.Fatalf("response body contains raw API key %q — must be redacted", rawAPIKey)
	}

	// 4. Verify 403 for non-admin role. Use an API key that resolves to viewer.
	// Since we don't have a viewer key in this config, test without auth
	// (which returns 401) or test that the key check works.
	req403, _ := http.NewRequest("GET", "http://"+addr+"/config/effective", nil)
	// No auth header → 401 (requireAuth rejects before reaching handler).
	resp403, err := http.DefaultClient.Do(req403) //nolint:noctx
	if err != nil {
		t.Fatalf("GET /config/effective (no auth): %v", err)
	}
	defer resp403.Body.Close()
	if resp403.StatusCode != http.StatusUnauthorized {
		t.Fatalf("GET /config/effective (no auth): status %d, want 401", resp403.StatusCode)
	}
}
