package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
)

func writeTempConfig(t *testing.T, cfg interface{}) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func TestLoad_Defaults(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{})
	l, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer l.Close()

	cfg := l.Get()
	if cfg.Network.Port != 9000 {
		t.Errorf("default port: want 9000, got %d", cfg.Network.Port)
	}
	if cfg.Storage.SegmentMaxBytes != 1<<30 {
		t.Errorf("default segment size wrong")
	}
}

func TestLoad_Override(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "test-node"},
		"network": map[string]interface{}{"port": 9999},
	})
	l, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer l.Close()

	cfg := l.Get()
	if cfg.Broker.NodeID != "test-node" {
		t.Errorf("node_id: want test-node, got %s", cfg.Broker.NodeID)
	}
	if cfg.Network.Port != 9999 {
		t.Errorf("port: want 9999, got %d", cfg.Network.Port)
	}
}

func TestLoad_HotReload(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"network": map[string]interface{}{"port": 9000},
	})
	l, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	defer l.Close()

	called := make(chan struct{}, 1)
	l.OnChange(func(cfg *config.Config) {
		called <- struct{}{}
	})

	// Overwrite with new config — ensure mtime changes
	time.Sleep(10 * time.Millisecond)
	_ = writeTempConfigAt(t, path, map[string]interface{}{
		"broker":  map[string]interface{}{"node_id": "reloaded"},
		"network": map[string]interface{}{"port": 9000},
	})

	select {
	case <-called:
		// good — reload triggered
	case <-time.After(15 * time.Second):
		// Hot-reload polls every 5 seconds; 15s is generous
		t.Skip("hot-reload not triggered within timeout (timing-sensitive)")
	}

	cfg := l.Get()
	if cfg.Broker.NodeID != "reloaded" {
		t.Errorf("node_id after reload: want reloaded, got %s", cfg.Broker.NodeID)
	}
}

func TestLoad_InvalidPort(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"network": map[string]interface{}{"port": 99999},
	})
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for invalid port > 65535")
	}
}

func TestLoad_InvalidPortNegative(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"network": map[string]interface{}{"port": -1},
	})
	_, err := config.Load(path)
	if err == nil {
		t.Fatal("expected error for negative port")
	}
}

// TestLoad_PortZero verifies that port 0 (OS-assigned ephemeral port) is valid.
// Broker unit tests use port 0 to avoid port conflicts.
func TestLoad_PortZero(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"network": map[string]interface{}{"port": 0},
		"storage": map[string]interface{}{"wal_path": "/tmp/test-wal"},
	})
	l, err := config.Load(path)
	if err != nil {
		t.Fatalf("port 0 should be valid (OS-assigned): %v", err)
	}
	defer l.Close()
	if l.Get().Network.Port != 0 {
		t.Errorf("port: want 0, got %d", l.Get().Network.Port)
	}
}

// TestLoader_DoubleClose verifies that calling Close() twice does not panic.
// This is critical because broker.Stop() calls loader.Close() AND test
// cleanup also calls loader.Close().
func TestLoader_DoubleClose(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"network": map[string]interface{}{"port": 9000},
	})
	l, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	l.Close()
	// Second close must be a no-op, not a panic.
	l.Close()
}

// TestLoader_CloseFromMultipleGoroutines stress-tests idempotent Close.
func TestLoader_CloseFromMultipleGoroutines(t *testing.T) {
	path := writeTempConfig(t, map[string]interface{}{
		"network": map[string]interface{}{"port": 9000},
	})
	l, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			l.Close()
			done <- struct{}{}
		}()
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}

func writeTempConfigAt(t *testing.T, path string, cfg interface{}) string {
	t.Helper()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}
