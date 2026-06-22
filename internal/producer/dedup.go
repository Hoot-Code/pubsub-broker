// Package producer — dedup.go implements a bounded sliding-window deduplication
// store for idempotent (exactly-once) producers.
package producer

import (
	"sync"
)

// seqEntry is one slot in the per-client ring buffer.
type seqEntry struct {
	seqNum uint64
	offset int64
	valid  bool
}

// clientWindow is a fixed-size ring buffer of (seqNum, offset) pairs for a
// single client ID.
type clientWindow struct {
	ring []seqEntry
	head int // index of the next slot to overwrite
	size int
}

func newClientWindow(size int) *clientWindow {
	return &clientWindow{ring: make([]seqEntry, size), size: size}
}

// lookup returns (offset, true) if seqNum is in the window.
func (cw *clientWindow) lookup(seqNum uint64) (int64, bool) {
	for i := 0; i < cw.size; i++ {
		e := cw.ring[i]
		if e.valid && e.seqNum == seqNum {
			return e.offset, true
		}
	}
	return 0, false
}

// mark records (seqNum, offset) in the ring, evicting the oldest entry.
func (cw *clientWindow) mark(seqNum uint64, offset int64) {
	cw.ring[cw.head] = seqEntry{seqNum: seqNum, offset: offset, valid: true}
	cw.head = (cw.head + 1) % cw.size
}

// ─── DedupWindow ──────────────────────────────────────────────────────────────

// DedupWindow is a thread-safe, bounded sliding-window deduplication store.
// It tracks the last `size` (clientID, seqNum) pairs, returning cached offsets
// for duplicates and evicting the oldest entry when the window is full.
//
// Memory is bounded: per-client ring buffers use a fixed array of `size` slots,
// so total memory is O(unique_clients × size).
type DedupWindow struct {
	mu        sync.Mutex
	perClient map[string]*clientWindow
	size      int
}

// NewDedupWindow creates a DedupWindow that remembers the last `size` sequence
// numbers per client. Calls with seqNum already in the window are duplicates.
// When size is ≤ 0 it defaults to 10 000.
func NewDedupWindow(size int) *DedupWindow {
	if size <= 0 {
		size = 10_000
	}
	return &DedupWindow{
		perClient: make(map[string]*clientWindow),
		size:      size,
	}
}

// IsDuplicate reports whether (clientID, seqNum) has been seen before.
func (d *DedupWindow) IsDuplicate(clientID string, seqNum uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	cw, ok := d.perClient[clientID]
	if !ok {
		return false
	}
	_, found := cw.lookup(seqNum)
	return found
}

// LookupOffset returns the offset stored for (clientID, seqNum).
// The second return value is false if the entry is not in the window.
func (d *DedupWindow) LookupOffset(clientID string, seqNum uint64) (int64, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cw, ok := d.perClient[clientID]
	if !ok {
		return 0, false
	}
	return cw.lookup(seqNum)
}

// Mark records (clientID, seqNum, offset) in the window.
// If the window is full the oldest entry is evicted.
func (d *DedupWindow) Mark(clientID string, seqNum uint64, offset int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	cw, ok := d.perClient[clientID]
	if !ok {
		cw = newClientWindow(d.size)
		d.perClient[clientID] = cw
	}
	cw.mark(seqNum, offset)
}
