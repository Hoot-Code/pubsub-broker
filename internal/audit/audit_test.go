package audit_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/audit"
)

// TestAuditLogFormat logs 3 events and verifies each line is valid JSON with
// all required fields present.
func TestAuditLogFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := audit.NewLogger(&buf)

	events := []audit.Event{
		{
			Type:       audit.EventAuth,
			ClientID:   "svc-orders",
			RemoteAddr: "127.0.0.1:54321",
			Success:    true,
		},
		{
			Type:       audit.EventPublish,
			ClientID:   "svc-orders",
			RemoteAddr: "127.0.0.1:54321",
			Topic:      "orders",
			Success:    true,
			Details:    map[string]string{"partition": "0", "offset": "42"},
		},
		{
			Type:       audit.EventForbidden,
			ClientID:   "viewer",
			RemoteAddr: "10.0.0.1:9000",
			Topic:      "orders",
			Success:    false,
			Error:      "FORBIDDEN: insufficient role",
		},
	}

	for _, e := range events {
		logger.Log(e)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != len(events) {
		t.Fatalf("expected %d JSON lines, got %d:\n%s", len(events), len(lines), buf.String())
	}

	for i, line := range lines {
		var got map[string]interface{}
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Errorf("line %d is not valid JSON: %v\nraw: %q", i, err, line)
			continue
		}

		requiredKeys := []string{"time", "type", "client_id", "remote_addr", "success"}
		for _, k := range requiredKeys {
			if _, ok := got[k]; !ok {
				t.Errorf("line %d missing required field %q: %s", i, k, line)
			}
		}

		// Verify type matches.
		wantType := string(events[i].Type)
		if got["type"] != wantType {
			t.Errorf("line %d: type=%q want %q", i, got["type"], wantType)
		}

		// Verify client_id matches.
		if got["client_id"] != events[i].ClientID {
			t.Errorf("line %d: client_id=%q want %q", i, got["client_id"], events[i].ClientID)
		}

		// Verify time is present and parseable.
		timeStr, ok := got["time"].(string)
		if !ok || timeStr == "" {
			t.Errorf("line %d: missing or non-string 'time' field", i)
		} else if _, err := time.Parse(time.RFC3339Nano, timeStr); err != nil {
			t.Errorf("line %d: time %q is not RFC3339: %v", i, timeStr, err)
		}
	}
}

// TestRingBuffer verifies that adding capacity+10 events results in exactly
// capacity events in Snapshot, in newest-first order.
func TestRingBuffer(t *testing.T) {
	t.Parallel()

	const capacity = 10
	rb := audit.NewRingBuffer(capacity)

	total := capacity + 10 // deliberate overflow
	for i := 0; i < total; i++ {
		rb.Add(audit.Event{
			Type:     audit.EventPublish,
			ClientID: "c",
			Details:  map[string]string{"seq": string(rune('A' + i))},
		})
	}

	snap := rb.Snapshot()
	if len(snap) != capacity {
		t.Fatalf("expected %d events in snapshot, got %d", capacity, len(snap))
	}

	// The snapshot must be newest-first: snap[0] is the last event added.
	// The last event added was at index total-1 == capacity+9.
	// Its seq marker: rune('A' + capacity + 9).
	wantFirst := string(rune('A' + total - 1))
	if gotFirst := snap[0].Details["seq"]; gotFirst != wantFirst {
		t.Errorf("snap[0] seq=%q want %q (newest first)", gotFirst, wantFirst)
	}

	// snap[capacity-1] is the oldest retained event.
	wantLast := string(rune('A' + total - capacity))
	if gotLast := snap[capacity-1].Details["seq"]; gotLast != wantLast {
		t.Errorf("snap[%d] seq=%q want %q (oldest retained)", capacity-1, gotLast, wantLast)
	}
}

// TestRingBufferEmpty verifies that Snapshot on an empty buffer returns nil.
func TestRingBufferEmpty(t *testing.T) {
	t.Parallel()

	rb := audit.NewRingBuffer(100)
	if snap := rb.Snapshot(); snap != nil {
		t.Errorf("expected nil snapshot from empty ring buffer, got %v", snap)
	}
}

// TestRingBufferPartial verifies partial-fill behaviour (count < capacity).
func TestRingBufferPartial(t *testing.T) {
	t.Parallel()

	rb := audit.NewRingBuffer(50)
	for i := 0; i < 5; i++ {
		rb.Add(audit.Event{Type: audit.EventAuth, ClientID: "c"})
	}
	snap := rb.Snapshot()
	if len(snap) != 5 {
		t.Errorf("expected 5 events, got %d", len(snap))
	}
}

// TestLoggerRecentN verifies that Recent(n) returns at most n events.
func TestLoggerRecentN(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := audit.NewLogger(&buf)

	for i := 0; i < 20; i++ {
		logger.Log(audit.Event{Type: audit.EventAuth, ClientID: "c"})
	}

	got := logger.Recent(5)
	if len(got) != 5 {
		t.Errorf("Recent(5) returned %d events, want 5", len(got))
	}
}
