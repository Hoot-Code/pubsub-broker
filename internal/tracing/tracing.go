// Package tracing implements a minimal W3C TraceContext-compatible tracer
// using stdlib only — no external OpenTelemetry SDK is required.
package tracing

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"time"
)

// SpanStatus indicates whether a span completed successfully or with an error.
type SpanStatus uint8

const (
	// StatusOK indicates the span completed without error.
	StatusOK SpanStatus = 0
	// StatusError indicates the span completed with an error.
	StatusError SpanStatus = 1
)

// Span represents a single unit of work in a distributed trace.
type Span struct {
	// TraceID is the 128-bit trace identifier shared by all spans in a trace.
	TraceID [16]byte
	// SpanID is the 64-bit unique identifier for this span.
	SpanID [8]byte
	// ParentID is the SpanID of the parent span; zero if this is a root span.
	ParentID [8]byte
	// Name is a human-readable description of the operation.
	Name string
	// StartTime is when the span began.
	StartTime time.Time
	// EndTime is when End was called; zero until End is called.
	EndTime time.Time
	// Attributes holds key-value pairs attached to the span.
	Attributes map[string]string
	// Status indicates success or error.
	Status SpanStatus
	// statusMsg holds the error message when Status == StatusError.
	statusMsg string

	mu     sync.Mutex
	tracer *Tracer
	ended  bool
}

// End marks the span as complete, recording its end time.
// Subsequent calls to End are no-ops.
func (sp *Span) End() {
	sp.mu.Lock()
	if sp.ended {
		sp.mu.Unlock()
		return
	}
	sp.ended = true
	sp.EndTime = time.Now()
	sp.mu.Unlock()

	// Read the exporter under the tracer's lock, then call it without
	// holding any span lock so that Export can safely access span fields.
	if sp.tracer == nil {
		return
	}
	sp.tracer.mu.Lock()
	exporter := sp.tracer.exporter
	sp.tracer.mu.Unlock()
	if exporter != nil {
		exporter(sp)
	}
}

// SetStatus records the final status and optional message for this span.
// Calling SetStatus after End has no effect.
func (sp *Span) SetStatus(s SpanStatus, msg string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.Status = s
	sp.statusMsg = msg
}

// AddAttr adds or overwrites a key-value attribute on the span.
func (sp *Span) AddAttr(key, value string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.Attributes == nil {
		sp.Attributes = make(map[string]string)
	}
	sp.Attributes[key] = value
}

// StatusMsg returns the status message set via SetStatus.
func (sp *Span) StatusMsg() string {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.statusMsg
}

// ─── context key ─────────────────────────────────────────────────────────────

type spanContextKey struct{}

// SpanFromContext returns the active Span stored in ctx, or nil if absent.
func SpanFromContext(ctx context.Context) *Span {
	v := ctx.Value(spanContextKey{})
	if v == nil {
		return nil
	}
	sp, _ := v.(*Span)
	return sp
}

// ─── Tracer ──────────────────────────────────────────────────────────────────

// Tracer creates and manages spans.
type Tracer struct {
	mu       sync.Mutex
	exporter func(*Span)
}

// NewTracer returns a new Tracer. To receive completed spans, register an
// exporter via SetExporter after construction.
func NewTracer() *Tracer {
	return &Tracer{}
}

// SetExporter registers a function that is called synchronously after Span.End
// with the completed span. Replaces any previously set exporter.
func (t *Tracer) SetExporter(fn func(*Span)) {
	t.mu.Lock()
	t.exporter = fn
	t.mu.Unlock()
}

// Start creates a new Span named name, derives a child context from ctx that
// carries the span, and returns both. attrs is a flat list of key-value pairs
// (even length required); Start panics if len(attrs) is odd.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...string) (context.Context, *Span) {
	if len(attrs)%2 != 0 {
		panic(fmt.Sprintf("tracing: Start attrs must be key-value pairs, got %d element(s)", len(attrs)))
	}

	sp := &Span{
		Name:      name,
		StartTime: time.Now(),
		tracer:    t,
	}

	// Generate new SpanID.
	if _, err := rand.Read(sp.SpanID[:]); err != nil {
		panic(fmt.Sprintf("tracing: crypto/rand failed: %v", err))
	}

	// If there is a parent span in context, inherit its TraceID and set ParentID.
	if parent := SpanFromContext(ctx); parent != nil {
		sp.TraceID = parent.TraceID
		sp.ParentID = parent.SpanID
	} else {
		// Root span: generate new TraceID.
		if _, err := rand.Read(sp.TraceID[:]); err != nil {
			panic(fmt.Sprintf("tracing: crypto/rand failed: %v", err))
		}
	}

	// Attach attributes.
	if len(attrs) > 0 {
		sp.Attributes = make(map[string]string, len(attrs)/2)
		for i := 0; i < len(attrs); i += 2 {
			sp.Attributes[attrs[i]] = attrs[i+1]
		}
	}

	return context.WithValue(ctx, spanContextKey{}, sp), sp
}
