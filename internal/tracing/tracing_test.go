package tracing_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/tracing"
)

// TestSpanContext verifies that a child span inherits the parent's TraceID
// and sets its ParentID to the parent's SpanID.
func TestSpanContext(t *testing.T) {
	tr := tracing.NewTracer()

	rootCtx, root := tr.Start(context.Background(), "root-op")
	defer root.End()

	_, child := tr.Start(rootCtx, "child-op")
	defer child.End()

	if child.TraceID != root.TraceID {
		t.Errorf("child TraceID %x != root TraceID %x", child.TraceID, root.TraceID)
	}
	if child.ParentID != root.SpanID {
		t.Errorf("child ParentID %x != root SpanID %x", child.ParentID, root.SpanID)
	}
	var zero [8]byte
	if root.ParentID != zero {
		t.Errorf("root span ParentID should be zero, got %x", root.ParentID)
	}
}

// TestSpanContextFromContext verifies SpanFromContext returns the active span.
func TestSpanContextFromContext(t *testing.T) {
	tr := tracing.NewTracer()
	ctx, sp := tr.Start(context.Background(), "lookup-op")
	defer sp.End()

	got := tracing.SpanFromContext(ctx)
	if got != sp {
		t.Error("SpanFromContext returned wrong span")
	}
	if tracing.SpanFromContext(context.Background()) != nil {
		t.Error("SpanFromContext on plain context should return nil")
	}
}

// TestJSONExporter verifies that exporting a span produces the expected JSON fields.
func TestJSONExporter(t *testing.T) {
	tr := tracing.NewTracer()
	var buf bytes.Buffer
	exp := tracing.NewJSONExporter(&buf)
	tr.SetExporter(func(sp *tracing.Span) { _ = exp.Export(sp) })

	ctx, root := tr.Start(context.Background(), "json-test",
		"key1", "val1", "key2", "val2")
	time.Sleep(time.Millisecond) // ensure duration > 0
	root.SetStatus(tracing.StatusOK, "")
	root.End()

	_, child := tr.Start(ctx, "child-op")
	child.End()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) < 2 {
		t.Fatalf("expected ≥2 JSON lines, got %q", buf.String())
	}

	var rec struct {
		TraceID    string            `json:"trace_id"`
		SpanID     string            `json:"span_id"`
		ParentID   string            `json:"parent_id"`
		Name       string            `json:"name"`
		StartUS    int64             `json:"start_us"`
		DurationUS int64             `json:"duration_us"`
		Attrs      map[string]string `json:"attrs"`
		Status     int               `json:"status"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("unmarshal root span: %v", err)
	}

	if rec.Name != "json-test" {
		t.Errorf("name: got %q, want %q", rec.Name, "json-test")
	}
	if rec.TraceID == "" {
		t.Error("trace_id should not be empty")
	}
	if rec.SpanID == "" {
		t.Error("span_id should not be empty")
	}
	if rec.ParentID != "" {
		t.Errorf("root span parent_id should be omitted, got %q", rec.ParentID)
	}
	if rec.DurationUS <= 0 {
		t.Errorf("duration_us should be > 0, got %d", rec.DurationUS)
	}
	if rec.Attrs["key1"] != "val1" || rec.Attrs["key2"] != "val2" {
		t.Errorf("attrs mismatch: %v", rec.Attrs)
	}
	if rec.Status != int(tracing.StatusOK) {
		t.Errorf("status: got %d, want %d", rec.Status, tracing.StatusOK)
	}

	// Verify child span has parent_id set.
	var childRec struct {
		ParentID string `json:"parent_id"`
		TraceID  string `json:"trace_id"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &childRec); err != nil {
		t.Fatalf("unmarshal child span: %v", err)
	}
	if childRec.ParentID == "" {
		t.Error("child span parent_id should be present")
	}
	if childRec.TraceID != rec.TraceID {
		t.Errorf("child TraceID %q != root TraceID %q", childRec.TraceID, rec.TraceID)
	}
}

// TestJSONExporterParentIDOmitted confirms parent_id is absent for root spans.
func TestJSONExporterParentIDOmitted(t *testing.T) {
	tr := tracing.NewTracer()
	var buf bytes.Buffer
	exp := tracing.NewJSONExporter(&buf)
	tr.SetExporter(func(sp *tracing.Span) { _ = exp.Export(sp) })

	_, root := tr.Start(context.Background(), "root-only")
	root.End()

	var raw map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(buf.String())), &raw); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := raw["parent_id"]; ok {
		t.Error("parent_id key should be absent for root spans")
	}
}

// TestSpanStore verifies that adding capacity+10 spans results in Snapshot
// returning exactly capacity spans, and they are the newest ones.
func TestSpanStore(t *testing.T) {
	const capacity = 5
	store := tracing.NewSpanStore(capacity)
	tr := tracing.NewTracer()
	tr.SetExporter(func(sp *tracing.Span) { store.Add(sp) })

	total := capacity + 10
	ids := make([]string, total)
	for i := 0; i < total; i++ {
		_, sp := tr.Start(context.Background(), "span")
		ids[i] = hex.EncodeToString(sp.SpanID[:])
		sp.End()
	}

	snaps := store.Snapshot()
	if len(snaps) != capacity {
		t.Fatalf("Snapshot: want %d spans, got %d", capacity, len(snaps))
	}

	// Snapshot is newest-first: ids[total-1] .. ids[total-capacity].
	for i, sp := range snaps {
		want := ids[total-1-i]
		got := hex.EncodeToString(sp.SpanID[:])
		if got != want {
			t.Errorf("Snapshot[%d]: want spanID %s, got %s", i, want, got)
		}
	}
}

// TestSpanStoreConcurrent verifies concurrent Add+Snapshot do not race.
func TestSpanStoreConcurrent(t *testing.T) {
	store := tracing.NewSpanStore(100)
	tr := tracing.NewTracer()
	tr.SetExporter(func(sp *tracing.Span) { store.Add(sp) })

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			_, sp := tr.Start(context.Background(), "concurrent")
			sp.End()
		}
		close(done)
	}()
	for {
		select {
		case <-done:
			return
		default:
			store.Snapshot()
		}
	}
}
