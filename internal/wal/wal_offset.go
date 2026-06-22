package wal

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// offsetRecord is the on-disk representation of a single committed offset.
type offsetRecord struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

// OffsetWAL wraps a *WAL to provide typed Commit/Replay semantics for
// consumer group offsets. Each committed offset is persisted as a JSON
// entry in the underlying WAL so that offsets survive broker restarts.
//
// Checkpoint compacts the WAL by replacing all historical entries with a
// single snapshot entry per key, bounding recovery time.
type OffsetWAL struct {
	mu          sync.Mutex
	wal         *WAL
	commitCount atomic.Int64 // total commits since last checkpoint
}

// OpenOffsetWAL opens (or creates) the offset WAL at path and returns the
// OffsetWAL ready for use. Call Replay() immediately after to restore state.
func OpenOffsetWAL(path string) (*OffsetWAL, error) {
	w, _, err := Open(path)
	if err != nil {
		return nil, fmt.Errorf("offset wal: open: %w", err)
	}
	return &OffsetWAL{wal: w}, nil
}

// Commit persists a committed offset for (group, topic, partition) to the WAL.
// It is safe for concurrent use.
func (ow *OffsetWAL) Commit(group, topic string, partition int32, offset int64) error {
	rec := offsetRecord{
		Group:     group,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("offset wal: marshal: %w", err)
	}
	if _, err := ow.wal.Append(data); err != nil {
		return fmt.Errorf("offset wal: append: %w", err)
	}
	ow.commitCount.Add(1)
	return nil
}

// Replay reads all WAL entries and returns a map of the latest committed
// offset per key. The map key format is "group/topic/partition".
// Last-write-wins: if the same key appears multiple times, the last value
// in WAL order is returned (matching the checkpoint semantics).
func (ow *OffsetWAL) Replay() (map[string]int64, error) {
	ow.mu.Lock()
	defer ow.mu.Unlock()

	// Re-open the WAL to read all entries from offset 0.
	w, entries, err := Open(ow.wal.path)
	if err != nil {
		return nil, fmt.Errorf("offset wal: replay open: %w", err)
	}
	// Close the temporary read-only handle immediately after recovery to
	// prevent file-descriptor leaks.
	defer w.Close()

	result := make(map[string]int64, len(entries))
	for _, e := range entries {
		var rec offsetRecord
		if err := json.Unmarshal(e.Data, &rec); err != nil {
			// Skip corrupt entries; WAL recovery already removed tail corruption.
			continue
		}
		key := offsetKey(rec.Group, rec.Topic, rec.Partition)
		result[key] = rec.Offset
	}
	return result, nil
}

// Checkpoint compacts the WAL: it snapshots the current committed offsets
// (from snapshot), truncates the WAL, and rewrites one entry per key.
// This bounds recovery time to O(unique keys) regardless of commit history.
// snapshot must be the full current state (e.g. from OffsetStore.Snapshot()).
func (ow *OffsetWAL) Checkpoint(snapshot map[string]int64) error {
	ow.mu.Lock()
	defer ow.mu.Unlock()

	newEntries := make([][]byte, 0, len(snapshot))
	for key, offset := range snapshot {
		rec, err := parseOffsetKey(key, offset)
		if err != nil {
			return fmt.Errorf("offset wal: checkpoint parse key %q: %w", key, err)
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("offset wal: checkpoint marshal: %w", err)
		}
		newEntries = append(newEntries, data)
	}

	if err := ow.wal.Truncate(newEntries); err != nil {
		return fmt.Errorf("offset wal: checkpoint truncate: %w", err)
	}
	ow.commitCount.Store(0)
	return nil
}

// CommitCount returns the total number of commits since the last checkpoint.
func (ow *OffsetWAL) CommitCount() int64 {
	return ow.commitCount.Load()
}

// Close closes the underlying WAL file.
func (ow *OffsetWAL) Close() error {
	return ow.wal.Close()
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// offsetKey converts (group, topic, partition) to the standard map key format
// "group/topic/partition". Group and topic names are validated to not contain
// "/" by the consumer manager, so the separator is unambiguous.
func offsetKey(group, topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d", group, topic, partition)
}

// parseOffsetKey parses a key of the form "group/topic/partition" back into
// an offsetRecord. The partition field is the last "/"-delimited segment.
func parseOffsetKey(key string, offset int64) (offsetRecord, error) {
	// Find last "/" to split off partition.
	lastSlash := strings.LastIndex(key, "/")
	if lastSlash < 0 {
		return offsetRecord{}, fmt.Errorf("malformed key (no slash): %q", key)
	}
	partStr := key[lastSlash+1:]
	groupTopic := key[:lastSlash]

	// Find second-to-last "/" to split group from topic.
	midSlash := strings.Index(groupTopic, "/")
	if midSlash < 0 {
		return offsetRecord{}, fmt.Errorf("malformed key (single slash): %q", key)
	}
	group := groupTopic[:midSlash]
	topic := groupTopic[midSlash+1:]

	var partition int32
	if _, err := fmt.Sscanf(partStr, "%d", &partition); err != nil {
		return offsetRecord{}, fmt.Errorf("malformed partition %q: %w", partStr, err)
	}

	return offsetRecord{
		Group:     group,
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}, nil
}
