package broker

import (
	"context"
	"time"
)

// SetCheckpointHook sets a function called after each successful offset
// checkpoint. Intended for tests that need to observe checkpoint timing.
func (b *Broker) SetCheckpointHook(f func()) {
	b.onCheckpoint = f
}

// offsetCheckpointLoop periodically snapshots the offset store and compacts
// the offset WAL. It triggers on:
//   - every 30 seconds (wall-clock tick), or
//   - every 1000 commits (polled on a 100 ms interval).
func (b *Broker) offsetCheckpointLoop(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	pollTicker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	defer pollTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.runOffsetCheckpoint()
		case <-pollTicker.C:
			if b.commitCount.Load() >= 1000 {
				b.runOffsetCheckpoint()
			}
		}
	}
}

// runOffsetCheckpoint snapshots the current offset store and compacts the
// offset WAL. If the WAL checkpoint succeeds it resets the commit counter and
// invokes the onCheckpoint hook (if set).
func (b *Broker) runOffsetCheckpoint() {
	snap := b.offsets.Snapshot()
	if len(snap) == 0 {
		return
	}
	if err := b.offsetWAL.Checkpoint(snap); err != nil {
		b.log.Warn("offset wal checkpoint error", "err", err)
	} else {
		b.commitCount.Store(0)
		b.log.Info("offset wal checkpointed", "keys", len(snap))
		if b.onCheckpoint != nil {
			b.onCheckpoint()
		}
	}
}
