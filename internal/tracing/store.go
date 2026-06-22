package tracing

import "sync"

// SpanStore is a thread-safe ring-buffer that retains the last capacity
// completed spans. Once full, the oldest span is overwritten.
type SpanStore struct {
	mu       sync.Mutex
	buf      []*Span
	capacity int
	head     int // next write position
	count    int // number of spans stored so far (capped at capacity)
}

// NewSpanStore creates a SpanStore that holds up to capacity spans.
func NewSpanStore(capacity int) *SpanStore {
	if capacity <= 0 {
		capacity = 1
	}
	return &SpanStore{
		buf:      make([]*Span, capacity),
		capacity: capacity,
	}
}

// Add stores sp in the ring buffer, overwriting the oldest entry if full.
func (s *SpanStore) Add(sp *Span) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf[s.head] = sp
	s.head = (s.head + 1) % s.capacity
	if s.count < s.capacity {
		s.count++
	}
}

// Snapshot returns up to capacity spans in newest-first order.
func (s *SpanStore) Snapshot() []*Span {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count == 0 {
		return nil
	}
	out := make([]*Span, s.count)
	// Walk backwards from the most recently written slot.
	for i := 0; i < s.count; i++ {
		idx := (s.head - 1 - i + s.capacity) % s.capacity
		out[i] = s.buf[idx]
	}
	return out
}
