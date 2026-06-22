package broker_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestRuntimeMetricsExposed verifies that the runtime metrics appear in /metrics
// output with correct TYPE lines.
func TestRuntimeMetricsExposed(t *testing.T) {
	t.Parallel()
	b := newTestBroker(t)
	startBroker(t, b)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = b.Stop(ctx)
	})

	httpAddr := waitForHTTP(t, b)

	resp, err := http.Get("http://" + httpAddr + "/metrics") //nolint:noctx
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d, want 200", resp.StatusCode)
	}

	buf := make([]byte, 64*1024)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])

	for _, want := range []string{
		"# HELP process_resident_memory_bytes",
		"# TYPE process_resident_memory_bytes gauge",
		"process_resident_memory_bytes",
		"# HELP go_gc_duration_seconds_count",
		"# TYPE go_gc_duration_seconds_count counter",
		"go_gc_duration_seconds_count",
		"# HELP process_cpu_seconds_total",
		"# TYPE process_cpu_seconds_total gauge",
		"process_cpu_seconds_total",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in /metrics output", want)
		}
	}
}
