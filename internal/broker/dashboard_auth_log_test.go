package broker

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
)

// safeBuffer is a thread-safe bytes.Buffer for capturing log output.
type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// newBrokerWithLogBuf creates a broker whose logger writes to buf.
func newBrokerWithLogBuf(t *testing.T, cfgJSON string, buf *safeBuffer) *Broker {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(cfgJSON), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	loader, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	t.Cleanup(loader.Close)

	b, err := New(loader)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	b.log = logging.New(buf, "info")
	return b
}

// TestDashboardAuthDisabledLogsReason verifies that when auth.Enabled=false
// and DashboardEnabled=true, the broker logs the expected INFO line at startup.
func TestDashboardAuthDisabledLogsReason(t *testing.T) {
	var buf safeBuffer
	cfgJSON := `{
		"broker": {"node_id": "log-test"},
		"network": {"port": 0, "dashboard_enabled": true},
		"storage": {"wal_path": "x", "data_path": "y"},
		"auth": {"enabled": false},
		"logging": {"level": "info"}
	}`
	b := newBrokerWithLogBuf(t, cfgJSON, &buf)
	go b.Start() //nolint:errcheck
	t.Cleanup(func() { b.Stop(nil) })
	waitForBrokerReady(t, b)

	b.Stop(nil)
	out := buf.String()

	want := "dashboard auth disabled: broker auth.enabled is false"
	if !strings.Contains(out, want) {
		t.Errorf("log output missing %q\ngot: %s", want, out)
	}
	count := strings.Count(out, want)
	if count != 1 {
		t.Errorf("expected %q exactly once, found %d occurrences", want, count)
	}
}

// TestDashboardAuthExplicitlyOverriddenLogsReason verifies that when
// auth.Enabled=true but DashboardAuthEnabled is explicitly set to false,
// the broker logs the override message at startup.
func TestDashboardAuthExplicitlyOverriddenLogsReason(t *testing.T) {
	var buf safeBuffer
	cfgJSON := `{
		"broker": {"node_id": "override-test"},
		"network": {"port": 0, "dashboard_enabled": true, "dashboard_auth_enabled": false},
		"storage": {"wal_path": "x", "data_path": "y"},
		"auth": {"enabled": true, "api_keys": [{"key": "k1", "client_id": "c1", "role": "admin"}]},
		"logging": {"level": "info"}
	}`
	b := newBrokerWithLogBuf(t, cfgJSON, &buf)
	go b.Start() //nolint:errcheck
	t.Cleanup(func() { b.Stop(nil) })
	waitForBrokerReady(t, b)

	b.Stop(nil)
	out := buf.String()

	want := "dashboard auth disabled: overridden via config (network.dashboard_auth_enabled=false)"
	if !strings.Contains(out, want) {
		t.Errorf("log output missing %q\ngot: %s", want, out)
	}
	count := strings.Count(out, want)
	if count != 1 {
		t.Errorf("expected %q exactly once, found %d occurrences", want, count)
	}
	if strings.Contains(out, "broker auth.enabled is false") {
		t.Error("log should not contain the 'auth.enabled is false' message when auth is enabled")
	}
}

// TestDashboardAuthEnabledNoLog verifies that when dashboard auth is fully
// enabled, no "dashboard auth disabled" message is logged.
func TestDashboardAuthEnabledNoLog(t *testing.T) {
	var buf safeBuffer
	cfgJSON := `{
		"broker": {"node_id": "enabled-test"},
		"network": {"port": 0, "dashboard_enabled": true},
		"storage": {"wal_path": "x", "data_path": "y"},
		"auth": {"enabled": true, "api_keys": [{"key": "k1", "client_id": "c1", "role": "admin"}]},
		"logging": {"level": "info"}
	}`
	b := newBrokerWithLogBuf(t, cfgJSON, &buf)
	go b.Start() //nolint:errcheck
	t.Cleanup(func() { b.Stop(nil) })
	waitForBrokerReady(t, b)

	b.Stop(nil)
	out := buf.String()

	if strings.Contains(out, "dashboard auth disabled") {
		t.Errorf("unexpected 'dashboard auth disabled' log line\ngot: %s", out)
	}
}

// waitForBrokerReady blocks until the broker is ready and the HTTP server
// is responding, or the test times out after 5 seconds.
func waitForBrokerReady(t *testing.T, b *Broker) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b.Ready() && b.HTTPAddr() != "" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("broker did not become ready within 5 s")
}
