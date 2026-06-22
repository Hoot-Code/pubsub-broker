package broker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// startConfigTestBroker creates a broker with auth enabled and an admin API key,
// starts it, and returns the HTTP address for making admin requests.
func startConfigTestBroker(t *testing.T) (httpAddr string, b *broker.Broker, adminKey string) {
	t.Helper()
	dir := t.TempDir()
	adminKey = "test-admin-key-12345"
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "config-test"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100},
		"auth": map[string]interface{}{
			"enabled": true,
			"api_keys": []interface{}{
				map[string]interface{}{
					"key":       adminKey,
					"client_id": "admin",
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
	}
	path := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(cfg)
	_ = os.WriteFile(path, data, 0o644)

	loader, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(func() { loader.Close() })

	b, err = broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	go b.Start()
	// Wait until the HTTP server is accepting connections.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() {
			// Small sleep to ensure httpAddr is visible (avoids race
			// between Start()'s goroutine writing httpAddr and this
			// goroutine reading it via HTTPAddr).
			time.Sleep(10 * time.Millisecond)
			addr := b.HTTPAddr()
			if addr != "" {
				resp, rErr := http.Get("http://" + addr + "/health")
				if rErr == nil {
					resp.Body.Close()
					return addr, b, adminKey
				}
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("broker did not become ready within 5 s")
	return "", nil, ""
}

func adminReq(t *testing.T, method, url, apiKey string, body interface{}) *http.Response {
	t.Helper()
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reqBody = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func TestConfigPatchAppliesAndPersists(t *testing.T) {
	httpAddr, b, adminKey := startConfigTestBroker(t)

	// PATCH rate_limit.per_client_rps to 50.
	resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"rate_limit.per_client_rps": float64(50),
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PATCH status: want 200, got %d: %s", resp.StatusCode, body)
	}

	// Verify the live config reflects the change.
	resp2 := adminReq(t, "GET", "http://"+httpAddr+"/config/effective", adminKey, nil)
	defer resp2.Body.Close()
	var effective map[string]interface{}
	if err := json.NewDecoder(resp2.Body).Decode(&effective); err != nil {
		t.Fatalf("decode effective: %v", err)
	}
	rl := effective["rate_limit"].(map[string]interface{})
	rlPCR := rl["per_client_rps"].(map[string]interface{})
	if int(rlPCR["value"].(float64)) != 50 {
		t.Errorf("per_client_rps in effective: want 50, got %v", rlPCR["value"])
	}

	// Verify config file on disk reflects the change.
	cfgPath := b.ConfigPath()
	diskData, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config file: %v", err)
	}
	var diskCfg config.Config
	if err := json.Unmarshal(diskData, &diskCfg); err != nil {
		t.Fatalf("unmarshal disk config: %v", err)
	}
	if diskCfg.RateLimit.PerClientRPS != 50 {
		t.Errorf("disk per_client_rps: want 50, got %d", diskCfg.RateLimit.PerClientRPS)
	}
}

func TestConfigPatchRejectsNonHotReloadable(t *testing.T) {
	httpAddr, _, adminKey := startConfigTestBroker(t)

	resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"network.port": float64(8080),
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PATCH status: want 400, got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	errMsg, _ := body["error"].(string)
	if !strings.Contains(errMsg, "restart") {
		t.Errorf("error should mention restart: %s", errMsg)
	}
	rejected, _ := body["rejected_fields"].([]interface{})
	if len(rejected) == 0 {
		t.Error("rejected_fields should list network.port")
	}
}

func TestConfigPatchRejectsInvalidValue(t *testing.T) {
	httpAddr, b, adminKey := startConfigTestBroker(t)

	// Capture config file state before.
	cfgPath := b.ConfigPath()
	beforeData, _ := os.ReadFile(cfgPath)

	resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"rate_limit.per_client_rps": float64(-5),
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PATCH status: want 400, got %d", resp.StatusCode)
	}

	// Verify config file on disk is UNCHANGED.
	afterData, _ := os.ReadFile(cfgPath)
	if string(beforeData) != string(afterData) {
		t.Error("config file should not change after invalid patch")
	}
}

func TestConfigPatchRequiresAdmin(t *testing.T) {
	httpAddr, _, _ := startConfigTestBroker(t)

	// Try with a non-admin key — should get 401 (auth fails).
	resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", "non-admin-key", map[string]interface{}{
		"changes": map[string]interface{}{
			"rate_limit.per_client_rps": float64(50),
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("PATCH status: want 401, got %d", resp.StatusCode)
	}
}

func TestConfigPatchAuditLogged(t *testing.T) {
	dir := t.TempDir()
	adminKey := "audit-test-key"
	auditPath := filepath.Join(dir, "audit.log")
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "audit-test"},
		"network": map[string]interface{}{"port": 0, "max_connections": 100},
		"auth": map[string]interface{}{
			"enabled": true,
			"api_keys": []interface{}{
				map[string]interface{}{
					"key":       adminKey,
					"client_id": "admin",
					"role":      "admin",
				},
			},
		},
		"audit_log_file": auditPath,
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
	go b.Start()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() {
			time.Sleep(10 * time.Millisecond)
			if b.HTTPAddr() != "" {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = b.Stop(ctx)
	})

	resp := adminReq(t, "PATCH", "http://"+b.HTTPAddr()+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"rate_limit.per_client_rps": float64(75),
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PATCH status: want 200, got %d: %s", resp.StatusCode, body)
	}

	// Read audit log and check for config_reload event.
	time.Sleep(100 * time.Millisecond) // give audit logger time to flush
	auditData, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	if !strings.Contains(string(auditData), "config_reload") {
		t.Error("audit log should contain config_reload event")
	}
	if !strings.Contains(string(auditData), "per_client_rps") {
		t.Error("audit log should contain per_client_rps field detail")
	}
}

func TestConfigPatchRateLimited(t *testing.T) {
	httpAddr, _, adminKey := startConfigTestBroker(t)

	// Send 5 patches (should all succeed).
	for i := 0; i < 5; i++ {
		resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
			"changes": map[string]interface{}{
				"rate_limit.per_client_rps": float64(100 + i),
			},
		})
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("patch %d: want 200, got %d", i, resp.StatusCode)
		}
	}

	// The 6th should be rate limited.
	resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"rate_limit.per_client_rps": float64(200),
		},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("6th PATCH status: want 429, got %d", resp.StatusCode)
	}
}
