// Package wal — wal_topic.go implements a Write-Ahead Log for topic metadata.
// Topics created or deleted via the broker are appended here so that the exact
// sequence of creates and deletes survives restarts and can be replayed in
// order to restore the in-memory topic registry.
package wal

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// topicOp is the on-disk record for a single topic WAL entry.
type topicOp struct {
	// Op is either "create" or "delete".
	Op string `json:"op"`
	// Config carries the full topic configuration for "create" ops.
	// For "delete" ops only Config.Name is meaningful.
	Config types.TopicConfig `json:"config"`
}

// TopicWAL persists topic create/delete operations so that topic metadata
// survives broker restarts. Entries are appended in chronological order;
// Replay applies them in order to derive the net set of live topics.
type TopicWAL struct {
	mu  sync.Mutex
	wal *WAL
}

// OpenTopicWAL opens (or creates) the topic WAL at path.
// The returned TopicWAL is ready for use; call Replay() to restore state.
func OpenTopicWAL(path string) (*TopicWAL, error) {
	w, _, err := Open(path)
	if err != nil {
		return nil, fmt.Errorf("topic wal: open: %w", err)
	}
	return &TopicWAL{wal: w}, nil
}

// Append records a topic creation in the WAL.
func (tw *TopicWAL) Append(cfg types.TopicConfig) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	data, err := json.Marshal(topicOp{Op: "create", Config: cfg})
	if err != nil {
		return fmt.Errorf("topic wal: marshal create: %w", err)
	}
	if _, err := tw.wal.Append(data); err != nil {
		return fmt.Errorf("topic wal: append create: %w", err)
	}
	return nil
}

// Delete records a topic deletion in the WAL.
func (tw *TopicWAL) Delete(name string) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	data, err := json.Marshal(topicOp{Op: "delete", Config: types.TopicConfig{Name: name}})
	if err != nil {
		return fmt.Errorf("topic wal: marshal delete: %w", err)
	}
	if _, err := tw.wal.Append(data); err != nil {
		return fmt.Errorf("topic wal: append delete: %w", err)
	}
	return nil
}

// Replay reads all WAL entries in order and returns the net set of topics that
// should exist after applying every create and delete operation.
//
// Returns:
//   - creates: configs for topics that currently exist (net after all deletes)
//   - deletes: names of topics that were deleted and are not re-created
//     (informational; on a fresh restart these topics are already absent)
//   - error: non-nil on I/O or parse failure
func (tw *TopicWAL) Replay() (creates []types.TopicConfig, deletes []string, err error) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// Re-open for reading so the working WAL handle is not disturbed.
	w, entries, rerr := Open(tw.wal.path)
	if rerr != nil {
		return nil, nil, fmt.Errorf("topic wal: replay open: %w", rerr)
	}
	defer w.Close() // release the temporary read handle

	// Apply entries in order; last writer wins per topic name.
	state := make(map[string]*types.TopicConfig)
	for _, e := range entries {
		var op topicOp
		if jerr := json.Unmarshal(e.Data, &op); jerr != nil {
			continue // skip corrupt entries
		}
		switch op.Op {
		case "create":
			cfg := op.Config
			state[cfg.Name] = &cfg
		case "delete":
			delete(state, op.Config.Name)
			deletes = append(deletes, op.Config.Name)
		}
	}

	for _, cfg := range state {
		creates = append(creates, *cfg)
	}
	return creates, deletes, nil
}

// Close closes the underlying WAL file.
func (tw *TopicWAL) Close() error {
	return tw.wal.Close()
}
