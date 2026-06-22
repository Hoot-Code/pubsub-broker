package broker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/broker"
	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

// TestClusterRaftEndpoint verifies GET /cluster/raft returns 404 when raft
// is not active (single-node mode).
func TestClusterRaftEndpoint(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	// Single-node mode: raft is not active.
	resp, err := http.Get("http://" + httpAddr + "/cluster/raft") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /cluster/raft: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("single-node: status %d, want 404", resp.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] == "" {
		t.Error("expected error message in response")
	}
	t.Logf("response: %v", body)
}

// TestClusterRaftEndpointWithRaftEnabled verifies that with raft enabled,
// the endpoint returns 200 with valid snapshot data.
func TestClusterRaftEndpointWithRaftEnabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "raft-node-1"},
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
		"cluster": map[string]interface{}{
			"enabled":             true,
			"node_id":             "raft-node-1",
			"bind_addr":           "127.0.0.1:0",
			"consensus_algorithm": "raft",
			"raft_data_dir":       filepath.Join(dir, "raft"),
		},
	}
	cfgPath := filepath.Join(dir, "config.json")
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

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
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	resp, err := http.Get("http://" + httpAddr + "/cluster/raft") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /cluster/raft: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("raft enabled: status %d, want 200", resp.StatusCode)
	}

	var snap map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}

	role, _ := snap["role"].(string)
	if role == "" {
		t.Error("role should be non-empty")
	}
	if snap["term"] == nil {
		t.Error("term missing")
	}
	t.Logf("raft snapshot: %v", snap)
}
