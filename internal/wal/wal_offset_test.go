package wal_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/wal"
)

func TestOffsetWAL_CommitAndReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.wal")

	ow, err := wal.OpenOffsetWAL(path)
	if err != nil {
		t.Fatalf("OpenOffsetWAL: %v", err)
	}

	// Commit 20 offsets across 3 groups.
	type commit struct {
		group     string
		topic     string
		partition int32
		offset    int64
	}
	commits := []commit{
		{"g1", "orders", 0, 10}, {"g1", "orders", 1, 20}, {"g1", "orders", 2, 30},
		{"g1", "events", 0, 5}, {"g1", "events", 1, 15},
		{"g2", "orders", 0, 100}, {"g2", "orders", 1, 200}, {"g2", "orders", 2, 300},
		{"g2", "events", 0, 50}, {"g2", "events", 1, 150},
		{"g3", "metrics", 0, 1}, {"g3", "metrics", 1, 2}, {"g3", "metrics", 2, 3},
		{"g3", "metrics", 3, 4},
		{"g1", "orders", 0, 11}, // duplicate key – last write wins
		{"g1", "orders", 1, 21},
		{"g1", "events", 0, 6},
		{"g2", "orders", 0, 101},
		{"g3", "metrics", 0, 99},
		{"g3", "metrics", 3, 88},
	}

	for _, c := range commits {
		if err := ow.Commit(c.group, c.topic, c.partition, c.offset); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	}
	if err := ow.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay.
	ow2, err := wal.OpenOffsetWAL(path)
	if err != nil {
		t.Fatalf("OpenOffsetWAL (reopen): %v", err)
	}
	defer ow2.Close()

	offsets, err := ow2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// Build expected: last-write-wins per key.
	expected := map[string]int64{
		"g1/orders/0": 11, "g1/orders/1": 21, "g1/orders/2": 30,
		"g1/events/0": 6, "g1/events/1": 15,
		"g2/orders/0": 101, "g2/orders/1": 200, "g2/orders/2": 300,
		"g2/events/0": 50, "g2/events/1": 150,
		"g3/metrics/0": 99, "g3/metrics/1": 2, "g3/metrics/2": 3,
		"g3/metrics/3": 88,
	}

	for key, want := range expected {
		got, ok := offsets[key]
		if !ok {
			t.Errorf("key %q missing from replay", key)
			continue
		}
		if got != want {
			t.Errorf("key %q: want %d, got %d", key, want, got)
		}
	}
	if len(offsets) != len(expected) {
		t.Errorf("replay returned %d keys, want %d", len(offsets), len(expected))
	}
}

func TestOffsetWAL_Checkpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "offsets.wal")

	ow, err := wal.OpenOffsetWAL(path)
	if err != nil {
		t.Fatalf("OpenOffsetWAL: %v", err)
	}

	// Write 1000 entries (many updates to the same keys).
	const keys = 10
	const updatesPerKey = 100
	for i := 0; i < keys*updatesPerKey; i++ {
		partition := int32(i % keys)
		offset := int64(i / keys)
		if err := ow.Commit("g1", "topic", partition, offset); err != nil {
			t.Fatalf("Commit i=%d: %v", i, err)
		}
	}

	// Build final snapshot: for each partition, the last offset written.
	snapshot := make(map[string]int64, keys)
	for p := 0; p < keys; p++ {
		key := fmt.Sprintf("g1/topic/%d", p)
		snapshot[key] = int64(updatesPerKey - 1)
	}

	// Checkpoint compacts to exactly `keys` entries.
	if err := ow.Checkpoint(snapshot); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if err := ow.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and verify Replay returns correct values in one pass.
	ow2, err := wal.OpenOffsetWAL(path)
	if err != nil {
		t.Fatalf("OpenOffsetWAL (reopen): %v", err)
	}
	defer ow2.Close()

	offsets, err := ow2.Replay()
	if err != nil {
		t.Fatalf("Replay after checkpoint: %v", err)
	}
	if len(offsets) != keys {
		t.Errorf("want %d keys after checkpoint, got %d", keys, len(offsets))
	}
	for p := 0; p < keys; p++ {
		key := fmt.Sprintf("g1/topic/%d", p)
		got, ok := offsets[key]
		if !ok {
			t.Errorf("key %q missing", key)
			continue
		}
		want := int64(updatesPerKey - 1)
		if got != want {
			t.Errorf("key %q: want %d, got %d", key, want, got)
		}
	}
}
