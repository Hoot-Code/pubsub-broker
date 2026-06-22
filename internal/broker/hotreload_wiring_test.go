package broker_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

func startWiringTestBroker(t *testing.T, cfgJSON string) (httpAddr string, b *broker.Broker, loader *config.Loader) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	_ = os.WriteFile(path, []byte(cfgJSON), 0o644)

	var err error
	loader, err = config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	t.Cleanup(func() { loader.Close() })

	b, err = broker.New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	go b.Start()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() {
			time.Sleep(10 * time.Millisecond)
			addr := b.HTTPAddr()
			if addr != "" {
				resp, rErr := http.Get("http://" + addr + "/health")
				if rErr == nil {
					resp.Body.Close()
					t.Cleanup(func() {
						ctx := context.Background()
						_ = b.Stop(ctx)
					})
					return addr, b, loader
				}
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("broker did not become ready within 5 s")
	return "", nil, nil
}

// TestRateLimitHotReloadTakesEffect verifies that changing the per-client rate
// limit via PATCH /config immediately causes rapid requests to be 429'd.
func TestRateLimitHotReloadTakesEffect(t *testing.T) {
	adminKey := "ratelimit-hr-key"
	httpAddr, _, _ := startWiringTestBroker(t, `{
		"broker": {"node_id": "ratelimit-hr"},
		"network": {"port": 0, "max_connections": 100},
		"auth": {
			"enabled": true,
			"api_keys": [{"key": "`+adminKey+`", "client_id": "admin", "role": "admin"}]
		},
		"rate_limit": {"enabled": true, "per_client_rps": 10000, "per_topic_rps": 50000, "burst_multiplier": 2},
		"storage": {"wal_path": "/tmp/wiring-test-wal", "data_path": "/tmp/wiring-test-data", "segment_max_bytes": 1048576, "index_interval_bytes": 512}
	}`)

	// PATCH to strict limit: 1 RPS.
	resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"rate_limit.per_client_rps": float64(1),
		},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH status: want 200, got %d", resp.StatusCode)
	}

	// Make rapid PATCH requests — the rate limiter should return 429 after 5.
	var rateLimited int
	for i := 0; i < 10; i++ {
		resp := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
			"changes": map[string]interface{}{
				"rate_limit.per_client_rps": float64(1),
			},
		})
		resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests {
			rateLimited++
		}
	}
	if rateLimited == 0 {
		t.Error("expected at least some 429 responses after strict rate limit")
	}
}

// TestLogLevelHotReloadTakesEffect verifies that changing the log level via
// PATCH /config causes the effective config to reflect the change.
func TestLogLevelHotReloadTakesEffect(t *testing.T) {
	adminKey := "loglevel-hr-key"
	httpAddr, _, _ := startWiringTestBroker(t, `{
		"broker": {"node_id": "loglevel-hr"},
		"network": {"port": 0, "max_connections": 100},
		"logging": {"level": "warn", "format": "json"},
		"storage": {"wal_path": "/tmp/wiring-test-wal2", "data_path": "/tmp/wiring-test-data2", "segment_max_bytes": 1048576, "index_interval_bytes": 512}
	}`)

	// Verify initial level is "warn".
	resp := adminReq(t, "GET", "http://"+httpAddr+"/config/effective", adminKey, nil)
	defer resp.Body.Close()
	var effective map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&effective)
	lg := effective["logging"].(map[string]interface{})
	lvl := lg["level"].(map[string]interface{})
	if lvl["value"] != "warn" {
		t.Fatalf("initial logging.level: want warn, got %v", lvl["value"])
	}

	// PATCH to debug level.
	resp2 := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"logging.level": "debug",
		},
	})
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("PATCH status: want 200, got %d", resp2.StatusCode)
	}

	// Verify effective config now shows debug.
	resp3 := adminReq(t, "GET", "http://"+httpAddr+"/config/effective", adminKey, nil)
	defer resp3.Body.Close()
	var effective2 map[string]interface{}
	_ = json.NewDecoder(resp3.Body).Decode(&effective2)
	lg2 := effective2["logging"].(map[string]interface{})
	lvl2 := lg2["level"].(map[string]interface{})
	if lvl2["value"] != "debug" {
		t.Errorf("logging.level after patch: want debug, got %v", lvl2["value"])
	}

	// PATCH back to warn.
	resp4 := adminReq(t, "PATCH", "http://"+httpAddr+"/config", adminKey, map[string]interface{}{
		"changes": map[string]interface{}{
			"logging.level": "warn",
		},
	})
	resp4.Body.Close()
	if resp4.StatusCode != http.StatusOK {
		t.Fatalf("PATCH back status: want 200, got %d", resp4.StatusCode)
	}

	resp5 := adminReq(t, "GET", "http://"+httpAddr+"/config/effective", adminKey, nil)
	defer resp5.Body.Close()
	var effective3 map[string]interface{}
	_ = json.NewDecoder(resp5.Body).Decode(&effective3)
	lg3 := effective3["logging"].(map[string]interface{})
	lvl3 := lg3["level"].(map[string]interface{})
	if lvl3["value"] != "warn" {
		t.Errorf("logging.level after second patch: want warn, got %v", lvl3["value"])
	}
}

// Ensure io is used.
var _ = io.ReadAll
