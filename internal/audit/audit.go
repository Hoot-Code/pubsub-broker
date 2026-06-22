// Package audit provides a structured, thread-safe audit logger that writes
// one JSON event per line to any io.Writer (typically an os.File) and
// simultaneously maintains an in-memory ring buffer for the last N events
// so that GET /audit/recent can serve them without a disk read.
package audit

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// ─── Event types ─────────────────────────────────────────────────────────────

// EventType categorises an audit event.
type EventType string

const (
	// EventAuth is logged for every authentication attempt (success or failure).
	EventAuth EventType = "auth"
	// EventPublish is logged when a message is published successfully.
	EventPublish EventType = "publish"
	// EventSubscribe is logged when a consumer subscribes to a topic.
	EventSubscribe EventType = "subscribe"
	// EventCreateTopic is logged when a topic is created.
	EventCreateTopic EventType = "create_topic"
	// EventDeleteTopic is logged when a topic is deleted.
	EventDeleteTopic EventType = "delete_topic"
	// EventSeek is logged when a consumer-group offset is seeked.
	EventSeek EventType = "seek"
	// EventForbidden is logged when a request is denied by RBAC or topic ACL.
	EventForbidden EventType = "forbidden"
	// EventConfigReload is logged when a hot-reloadable config field is
	// changed via PATCH /config. Details contain old/new values per field.
	EventConfigReload EventType = "config_reload"
)

// ─── Event ───────────────────────────────────────────────────────────────────

// Event is a single, immutable audit record.  All fields are exported so that
// encoding/json can serialise them without reflection trickery.
type Event struct {
	// Time is the UTC wall-clock time when the event occurred.
	Time time.Time `json:"time"`
	// Type classifies the action that triggered this event.
	Type EventType `json:"type"`
	// ClientID is the authenticated client identifier (from the AUTH frame).
	ClientID string `json:"client_id"`
	// RemoteAddr is the TCP address of the client connection.
	RemoteAddr string `json:"remote_addr"`
	// Topic is the topic name, if the action was topic-specific.
	Topic string `json:"topic,omitempty"`
	// Success indicates whether the action completed successfully.
	Success bool `json:"success"`
	// Error is the error string for failed actions.
	Error string `json:"error,omitempty"`
	// Details carries optional action-specific key/value metadata.
	Details map[string]string `json:"details,omitempty"`
}

// ─── Logger ───────────────────────────────────────────────────────────────────

// Logger writes audit events as newline-delimited JSON to an io.Writer and
// records every event in an in-memory RingBuffer so that recent events can be
// served via HTTP without a disk seek.
//
// Logger is safe for concurrent use.
type Logger struct {
	mu  sync.Mutex
	w   io.Writer
	enc *json.Encoder
	rb  *RingBuffer
}

// NewLogger creates a Logger that writes to w and keeps the last 1 000 events
// in memory.  w is typically an *os.File opened with O_APPEND|O_CREATE|O_WRONLY.
func NewLogger(w io.Writer) *Logger {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &Logger{
		w:   w,
		enc: enc,
		rb:  NewRingBuffer(1000),
	}
}

// Log appends e to the underlying writer (one JSON object per line) and adds
// it to the in-memory ring buffer.  If the writer returns an error it is
// silently dropped to avoid disrupting the broker's critical path.
func (l *Logger) Log(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	l.mu.Lock()
	_ = l.enc.Encode(e) // Encode appends '\n'; errors are intentionally dropped.
	l.mu.Unlock()
	l.rb.Add(e)
}

// Recent returns the last n events (newest first) from the in-memory ring
// buffer.  If n <= 0 or n > RingBuffer capacity, all buffered events are
// returned.
func (l *Logger) Recent(n int) []Event {
	snap := l.rb.Snapshot()
	if n > 0 && n < len(snap) {
		return snap[:n]
	}
	return snap
}

// ─── RingBuffer ───────────────────────────────────────────────────────────────

// RingBuffer is a bounded, thread-safe circular buffer for audit Events.
// When the buffer is full the oldest event is silently overwritten.
type RingBuffer struct {
	mu       sync.Mutex
	buf      []Event
	capacity int
	head     int // index where the next write goes
	count    int // number of valid entries (0 ≤ count ≤ capacity)
}

// NewRingBuffer creates a RingBuffer with the given capacity.
// Panics if capacity < 1.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity < 1 {
		panic("audit: RingBuffer capacity must be >= 1")
	}
	return &RingBuffer{
		buf:      make([]Event, capacity),
		capacity: capacity,
	}
}

// Add appends e to the ring buffer, overwriting the oldest entry when full.
func (rb *RingBuffer) Add(e Event) {
	rb.mu.Lock()
	rb.buf[rb.head] = e
	rb.head = (rb.head + 1) % rb.capacity
	if rb.count < rb.capacity {
		rb.count++
	}
	rb.mu.Unlock()
}

// Snapshot returns a copy of all buffered events in newest-first order.
// The returned slice is a fresh allocation and safe to modify.
func (rb *RingBuffer) Snapshot() []Event {
	rb.mu.Lock()
	n := rb.count
	if n == 0 {
		rb.mu.Unlock()
		return nil
	}
	out := make([]Event, n)
	// head-1 is the most recently written slot. Walk backwards.
	for i := 0; i < n; i++ {
		idx := (rb.head - 1 - i + rb.capacity) % rb.capacity
		out[i] = rb.buf[idx]
	}
	rb.mu.Unlock()
	return out
}
