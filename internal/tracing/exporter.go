package tracing

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// JSONExporter writes one JSON object per completed span to an io.Writer.
// Each line has the form:
//
//	{"trace_id":"<hex>","span_id":"<hex>","parent_id":"<hex>",
//	 "name":"<name>","start_us":<unix_micros>,"duration_us":<micros>,
//	 "attrs":{...},"status":<0|1>}
//
// The parent_id field is omitted when the span is a root (zero ParentID).
// JSONExporter is safe for concurrent use.
type JSONExporter struct {
	mu sync.Mutex
	w  io.Writer
}

// NewJSONExporter creates a JSONExporter that writes to w.
func NewJSONExporter(w io.Writer) *JSONExporter {
	return &JSONExporter{w: w}
}

// Export serialises sp as a newline-terminated JSON object and writes it to
// the underlying writer. Returns an error only if the write fails.
func (e *JSONExporter) Export(sp *Span) error {
	type record struct {
		TraceID    string            `json:"trace_id"`
		SpanID     string            `json:"span_id"`
		ParentID   string            `json:"parent_id,omitempty"`
		Name       string            `json:"name"`
		StartUS    int64             `json:"start_us"`
		DurationUS int64             `json:"duration_us"`
		Attrs      map[string]string `json:"attrs,omitempty"`
		Status     SpanStatus        `json:"status"`
	}

	sp.mu.Lock()
	traceID := hex.EncodeToString(sp.TraceID[:])
	spanID := hex.EncodeToString(sp.SpanID[:])
	var parentID string
	if sp.ParentID != [8]byte{} {
		parentID = hex.EncodeToString(sp.ParentID[:])
	}
	name := sp.Name
	startUS := sp.StartTime.UnixMicro()
	var durationUS int64
	if !sp.EndTime.IsZero() {
		durationUS = sp.EndTime.Sub(sp.StartTime).Microseconds()
	}
	attrs := sp.Attributes
	status := sp.Status
	sp.mu.Unlock()

	rec := record{
		TraceID:    traceID,
		SpanID:     spanID,
		ParentID:   parentID,
		Name:       name,
		StartUS:    startUS,
		DurationUS: durationUS,
		Attrs:      attrs,
		Status:     status,
	}

	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("tracing: json marshal: %w", err)
	}
	b = append(b, '\n')

	e.mu.Lock()
	defer e.mu.Unlock()
	_, err = e.w.Write(b)
	return err
}
