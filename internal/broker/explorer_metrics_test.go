package broker_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestExplorerMetricsAppearInOutput verifies that the three Explorer metric
// names appear in the /metrics output.
func TestExplorerMetricsAppearInOutput(t *testing.T) {
	brokerAddr, httpAddr, _ := startExplorerTestBroker(t, "")

	// Create a topic and publish a message to exercise the explorer path.
	createTopicViaProto(t, brokerAddr, "metrics-topic", 1)
	time.Sleep(50 * time.Millisecond)

	// Connect an Explorer session to make ActiveSessions > 0.
	ws := dialWS(t, fmt.Sprintf("ws://%s/explorer/stream?topic=metrics-topic", httpAddr))
	defer ws.Close()
	time.Sleep(100 * time.Millisecond)

	// Publish a message so sent count > 0.
	publishViaProto(t, brokerAddr, "metrics-topic", "k", "v")
	time.Sleep(100 * time.Millisecond)

	// Fetch /metrics.
	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", httpAddr))
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: status %d", resp.StatusCode)
	}

	buf := make([]byte, 64*1024)
	n, _ := resp.Body.Read(buf)
	body := string(buf[:n])

	required := []string{
		"explorer_active_sessions",
		"explorer_messages_sent_total",
		"explorer_messages_dropped_total",
	}
	for _, name := range required {
		if !strings.Contains(body, name) {
			t.Errorf("/metrics output missing %q", name)
		} else {
			t.Logf("found metric %q", name)
		}
	}
}
