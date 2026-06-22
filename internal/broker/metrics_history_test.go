package broker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
)

// TestHistoryStoreRetentionEviction verifies that samples older than the
// retention window are evicted.
func TestHistoryStoreRetentionEviction(t *testing.T) {
	t.Parallel()
	retention := 200 * time.Millisecond
	interval := 20 * time.Millisecond
	h := metrics.NewHistoryStore(retention, interval)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var tick int64
	h.Start(ctx, func() map[string]float64 {
		tick++
		return map[string]float64{"test_metric": float64(tick)}
	})

	// Wait for a few samples to be collected.
	time.Sleep(100 * time.Millisecond)

	// Verify we have some samples.
	samples := h.Snapshot("test_metric")
	if len(samples) == 0 {
		t.Fatal("expected at least one sample after 100ms")
	}

	// Wait for old samples to be evicted.
	time.Sleep(300 * time.Millisecond)

	samples = h.Snapshot("test_metric")
	// After 400ms total with 200ms retention, we should have at most a few
	// recent samples, not all of them.
	if len(samples) > 20 {
		t.Errorf("too many samples retained: %d (retention=%v)", len(samples), retention)
	}

	cancel()
	h.Stop()
}

// TestMetricsHistoryEndpoint verifies GET /metrics/history for each valid
// range value and one invalid value.
func TestMetricsHistoryEndpoint(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	// Wait for at least one sample.
	time.Sleep(200 * time.Millisecond)

	for _, validRange := range []string{"5m", "15m", "1h", "24h"} {
		resp, err := http.Get("http://" + httpAddr + "/metrics/history?range=" + validRange) //nolint:noctx
		if err != nil {
			t.Fatalf("GET /metrics/history?range=%s: %v", validRange, err)
		}
		var body map[string]interface{}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			resp.Body.Close()
			t.Fatalf("range=%s: decode error: %v", validRange, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("range=%s: status %d, want 200", validRange, resp.StatusCode)
		}
		if body["range"] != validRange {
			t.Errorf("range=%s: response range=%v", validRange, body["range"])
		}
		if body["interval_seconds"] != float64(10) {
			t.Errorf("range=%s: interval_seconds=%v, want 10", validRange, body["interval_seconds"])
		}
	}

	// Invalid range.
	resp, err := http.Get("http://" + httpAddr + "/metrics/history?range=30m") //nolint:noctx
	if err != nil {
		t.Fatalf("GET invalid range: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("invalid range: status %d, want 400", resp.StatusCode)
	}
}
