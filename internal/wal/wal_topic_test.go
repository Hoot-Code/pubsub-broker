package wal_test

import (
	"path/filepath"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/wal"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// TestTopicWAL_CreateDeleteReplay creates 5 topics, deletes 2, closes the
// WAL, reopens it, replays, and verifies exactly 3 topics remain with the
// correct configurations.
func TestTopicWAL_CreateDeleteReplay(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "topics.wal")

	tw, err := wal.OpenTopicWAL(path)
	if err != nil {
		t.Fatalf("OpenTopicWAL: %v", err)
	}

	// Create 5 topics.
	topics := []types.TopicConfig{
		{Name: "alpha", Partitions: 1, ReplicationFactor: 1},
		{Name: "beta", Partitions: 2, ReplicationFactor: 1},
		{Name: "gamma", Partitions: 3, ReplicationFactor: 2},
		{Name: "delta", Partitions: 4, ReplicationFactor: 1},
		{Name: "epsilon", Partitions: 1, ReplicationFactor: 1, RetentionHours: 48},
	}
	for _, cfg := range topics {
		if err := tw.Append(cfg); err != nil {
			t.Fatalf("Append(%s): %v", cfg.Name, err)
		}
	}

	// Delete 2 topics.
	if err := tw.Delete("beta"); err != nil {
		t.Fatalf("Delete(beta): %v", err)
	}
	if err := tw.Delete("delta"); err != nil {
		t.Fatalf("Delete(delta): %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay.
	tw2, err := wal.OpenTopicWAL(path)
	if err != nil {
		t.Fatalf("OpenTopicWAL (reopen): %v", err)
	}
	defer tw2.Close()

	creates, _, err := tw2.Replay()
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}

	// Expect exactly 3 remaining topics.
	if len(creates) != 3 {
		t.Fatalf("want 3 topics after replay, got %d: %v", len(creates), creates)
	}

	// Index by name for easy lookup.
	byName := make(map[string]types.TopicConfig, len(creates))
	for _, c := range creates {
		byName[c.Name] = c
	}

	// "beta" and "delta" were deleted.
	for _, deleted := range []string{"beta", "delta"} {
		if _, ok := byName[deleted]; ok {
			t.Errorf("topic %q should have been deleted but is present in replay", deleted)
		}
	}

	// Verify surviving topics have correct configs.
	for _, want := range []types.TopicConfig{
		{Name: "alpha", Partitions: 1, ReplicationFactor: 1},
		{Name: "gamma", Partitions: 3, ReplicationFactor: 2},
		{Name: "epsilon", Partitions: 1, ReplicationFactor: 1, RetentionHours: 48},
	} {
		got, ok := byName[want.Name]
		if !ok {
			t.Errorf("topic %q missing from replay", want.Name)
			continue
		}
		if got.Partitions != want.Partitions {
			t.Errorf("topic %q: partitions want %d got %d", want.Name, want.Partitions, got.Partitions)
		}
		if got.ReplicationFactor != want.ReplicationFactor {
			t.Errorf("topic %q: replication_factor want %d got %d",
				want.Name, want.ReplicationFactor, got.ReplicationFactor)
		}
		if got.RetentionHours != want.RetentionHours {
			t.Errorf("topic %q: retention_hours want %d got %d",
				want.Name, want.RetentionHours, got.RetentionHours)
		}
	}
}

// TestTopicWAL_ReplayIdempotent verifies that reopening and replaying the
// same WAL file multiple times returns consistent results.
func TestTopicWAL_ReplayIdempotent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "idempotent.wal")

	tw, err := wal.OpenTopicWAL(path)
	if err != nil {
		t.Fatalf("OpenTopicWAL: %v", err)
	}
	_ = tw.Append(types.TopicConfig{Name: "orders", Partitions: 4, ReplicationFactor: 2})
	_ = tw.Append(types.TopicConfig{Name: "events", Partitions: 2, ReplicationFactor: 1})
	_ = tw.Delete("orders")
	_ = tw.Close()

	for i := 0; i < 3; i++ {
		tw2, err := wal.OpenTopicWAL(path)
		if err != nil {
			t.Fatalf("iter %d OpenTopicWAL: %v", i, err)
		}
		creates, _, err := tw2.Replay()
		tw2.Close()
		if err != nil {
			t.Fatalf("iter %d Replay: %v", i, err)
		}
		if len(creates) != 1 || creates[0].Name != "events" {
			t.Fatalf("iter %d: want [events], got %v", i, creates)
		}
	}
}
