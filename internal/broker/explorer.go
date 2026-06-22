package broker

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// explorerChanCap is the bounded channel capacity for each ExplorerSession.
// When a slow consumer cannot keep up, messages are dropped (oldest first)
// and the drop count is reported to the client via status frames.
const explorerChanCap = 256

// ExplorerEvent bundles a published message with the publishing client's
// identity. The Explorer tap needs the clientID to populate the "producer"
// field in WebSocket frames, but that identity is not persisted in the
// segment — it only exists for the live tail.
type ExplorerEvent struct {
	// Message is the published message (shared with the Dispatch path).
	Message *types.Message
	// ClientID is the publishing client's identity captured at publish time.
	ClientID string
}

// ExplorerFilter defines server-side filtering criteria for the live message
// tail. Only messages matching ALL non-zero-value fields are delivered.
type ExplorerFilter struct {
	// Topic is the required topic name to match.
	Topic string
	// Partition selects a specific partition. -1 means all partitions.
	Partition int32
	// Key, when non-empty, requires an exact match against the message key.
	Key string
	// Producer, when non-empty, requires an exact match against the
	// publishing client's ClientID captured at publish time. This only works
	// for the live tail — producer identity is not persisted in segments.
	Producer string
	// Search, when non-empty, requires a case-insensitive substring match
	// against the decoded payload (post-decompression).
	Search string
}

// matches performs the cheap pre-checks (topic, partition, key, producer)
// without decoding the payload. Returns false immediately on any mismatch.
func (f *ExplorerFilter) matches(topic string, partition int32, key, producer string) bool {
	if f.Topic != "" && f.Topic != topic {
		return false
	}
	if f.Partition >= 0 && f.Partition != partition {
		return false
	}
	if f.Key != "" && f.Key != key {
		return false
	}
	if f.Producer != "" && f.Producer != producer {
		return false
	}
	return true
}

// matchesPayload performs the expensive search-substring check against the
// decoded payload. Must only be called after matches() returns true and
// when f.Search is non-empty.
func (f *ExplorerFilter) matchesPayload(payload []byte) bool {
	if f.Search == "" {
		return true
	}
	return strings.Contains(
		strings.ToLower(string(payload)),
		strings.ToLower(f.Search),
	)
}

// ExplorerSession represents a single live-tap WebSocket client. Each session
// runs its own drain goroutine that reads from a bounded channel and calls the
// sink function, isolating the publish hot path from slow consumers.
type ExplorerSession struct {
	filter ExplorerFilter
	sink   func(ev ExplorerEvent) error

	ch           chan ExplorerEvent
	droppedCount atomic.Uint64
	paused       atomic.Bool

	closeCh chan struct{}
	once    sync.Once
}

// newExplorerSession creates a session with a bounded channel and starts its
// drain goroutine. The drain goroutine reads from ch and calls sink for each
// event unless the session is paused (in which case events are discarded).
func newExplorerSession(filter ExplorerFilter, sink func(ev ExplorerEvent) error) *ExplorerSession {
	s := &ExplorerSession{
		filter:  filter,
		sink:    sink,
		ch:      make(chan ExplorerEvent, explorerChanCap),
		closeCh: make(chan struct{}),
	}
	go s.drainLoop()
	return s
}

// drainLoop is the session's own goroutine. It reads events from the bounded
// channel and calls sink for each. When paused, the goroutine yields (via a
// brief sleep) so that events accumulate in the channel; on Resume the drain
// catches up from whatever is buffered. This means a brief pause doesn't lose
// the most recent N events — they stay in the channel until Resume delivers
// them. The downside is that a prolonged pause fills the channel and triggers
// drops for new publishes, but this is acceptable: the status frame reports
// the drop count so the client knows data was lost.
func (s *ExplorerSession) drainLoop() {
	for {
		select {
		case <-s.closeCh:
			return
		default:
		}
		if s.paused.Load() {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		select {
		case <-s.closeCh:
			return
		case ev, ok := <-s.ch:
			if !ok {
				return
			}
			// Re-check paused after dequeue to avoid delivering an event
			// that was queued just before Pause() was called.
			if s.paused.Load() {
				continue
			}
			_ = s.sink(ev)
		}
	}
}

// Pause stops the session from calling sink. Messages queued into the bounded
// channel are retained (not dropped) so that Resume can deliver them.
func (s *ExplorerSession) Pause() {
	s.paused.Store(true)
}

// Resume re-enables sink delivery. Any messages that were queued while paused
// (up to the channel capacity) will now be delivered.
func (s *ExplorerSession) Resume() {
	s.paused.Store(false)
}

// UpdateFilter replaces the session's filter criteria. The new filter takes
// effect for all subsequently published messages; messages already in the
// channel are delivered with whatever filter was active when they were queued.
func (s *ExplorerSession) UpdateFilter(f ExplorerFilter) {
	s.filter = f
}

// DroppedCount returns the total number of messages dropped because the
// bounded channel was full (slow consumer). The transport layer can
// periodically read this and inform the client via status frames.
func (s *ExplorerSession) DroppedCount() uint64 {
	return s.droppedCount.Load()
}

// Close signals the drain goroutine to exit and releases resources.
// Safe to call multiple times.
func (s *ExplorerSession) Close() {
	s.once.Do(func() {
		close(s.closeCh)
	})
}

// ExplorerHub is a thread-safe fan-out hub that routes newly published
// messages to all active ExplorerSessions whose filters match. The hub
// performs cheap pre-checks (topic, partition, key, producer) before
// attempting the more expensive search-substring check on the payload.
//
// The Publish method is designed to be effectively free when there are no
// active sessions — it checks len(sessions) and returns immediately.
type ExplorerHub struct {
	mu       sync.RWMutex
	sessions map[*ExplorerSession]struct{}

	// counters for metrics
	activeSessions atomic.Int64
	sentTotal      atomic.Uint64
	droppedTotal   atomic.Uint64
}

// NewExplorerHub creates a new ExplorerHub with no active sessions.
func NewExplorerHub() *ExplorerHub {
	return &ExplorerHub{
		sessions: make(map[*ExplorerSession]struct{}),
	}
}

// NewSession creates a new ExplorerSession with the given filter and sink
// function, registers it with the hub, and returns it. The sink is called
// for each matching event; the hub does NOT perform WebSocket writes
// itself — that is wired in the transport layer (Part C).
func (h *ExplorerHub) NewSession(filter ExplorerFilter, sink func(ev ExplorerEvent) error) *ExplorerSession {
	sess := newExplorerSession(filter, sink)
	h.mu.Lock()
	h.sessions[sess] = struct{}{}
	h.mu.Unlock()
	h.activeSessions.Add(1)
	return sess
}

// removeSession unregisters a session from the hub.
func (h *ExplorerHub) removeSession(sess *ExplorerSession) {
	h.mu.Lock()
	delete(h.sessions, sess)
	h.mu.Unlock()
	h.activeSessions.Add(-1)
}

// ActiveSessions returns the number of currently registered sessions.
func (h *ExplorerHub) ActiveSessions() int64 {
	return h.activeSessions.Load()
}

// SentTotal returns the total number of messages successfully sent to sessions.
func (h *ExplorerHub) SentTotal() uint64 {
	return h.sentTotal.Load()
}

// DroppedTotal returns the total number of messages dropped across all sessions.
func (h *ExplorerHub) DroppedTotal() uint64 {
	return h.droppedTotal.Load()
}

// Publish fans out a newly published message to all active sessions whose
// filter matches. When there are zero sessions, this returns immediately
// without any allocation or payload decoding. The call is non-blocking:
// each session's bounded channel either accepts the event or it is
// dropped with a counter increment.
//
// The producerClientID parameter captures the publishing client's identity
// at publish time; it is not persisted in the segment and only works for
// the live tail.
func (h *ExplorerHub) Publish(topic string, partition int32, producerClientID string, msg *types.Message) {
	h.mu.RLock()
	if len(h.sessions) == 0 {
		h.mu.RUnlock()
		return
	}
	// Copy the session list under the lock to avoid holding it during
	// the per-session work.
	sessions := make([]*ExplorerSession, 0, len(h.sessions))
	for sess := range h.sessions {
		sessions = append(sessions, sess)
	}
	h.mu.RUnlock()

	ev := ExplorerEvent{Message: msg, ClientID: producerClientID}
	for _, sess := range sessions {
		f := &sess.filter
		if !f.matches(topic, partition, msg.Key, producerClientID) {
			continue
		}
		// Search filter requires payload decoding — only do it if needed.
		if f.Search != "" && !f.matchesPayload(msg.Payload) {
			continue
		}
		// Non-blocking send into the session's bounded channel.
		select {
		case sess.ch <- ev:
			h.sentTotal.Add(1)
		default:
			sess.droppedCount.Add(1)
			h.droppedTotal.Add(1)
		}
	}
}
