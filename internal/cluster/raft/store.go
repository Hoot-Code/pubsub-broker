package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// FilePersistentStore implements PersistentStore using a single JSON file
// with fsync-on-save, written via a temp-file + rename pattern for
// atomicity (the file is never left partially written because rename is
// atomic on POSIX).
type FilePersistentStore struct {
	path string
}

// NewFilePersistentStore creates a FilePersistentStore that persists state
// to the given file path. The file is created if it does not exist. The
// parent directory must already exist.
func NewFilePersistentStore(path string) *FilePersistentStore {
	return &FilePersistentStore{path: path}
}

// Save atomically persists state to the underlying JSON file using a
// temp-file + fsync + rename pattern.
func (s *FilePersistentStore) Save(state PersistentState) error {
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, "raft-state-*.tmp")
	if err != nil {
		return fmt.Errorf("raft: create temp file: %w", err)
	}
	tmpPath := tmp.Name()

	data, err := json.Marshal(state)
	if err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raft: marshal state: %w", err)
	}

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raft: write temp file: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raft: sync temp file: %w", err)
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raft: close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("raft: rename temp file: %w", err)
	}
	return nil
}

// Load returns the last successfully saved state. If the file does not
// exist yet, it returns a zero-value PersistentState (term 0, no vote,
// empty log) — this is the correct initial state for a brand-new node.
func (s *FilePersistentStore) Load() (PersistentState, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return PersistentState{}, nil
		}
		return PersistentState{}, fmt.Errorf("raft: read state file: %w", err)
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistentState{}, fmt.Errorf("raft: unmarshal state: %w", err)
	}
	return state, nil
}
