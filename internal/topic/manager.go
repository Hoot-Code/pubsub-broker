// Package topic manages topic lifecycle and per-topic metadata.
package topic

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/storage"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// validTopicName rejects names that could create malformed or malicious paths.
// Allowed: alphanumerics, hyphens, underscores, dots (not at the start).
var validTopicName = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}$`)

// Topic holds the runtime state of a single topic.
type Topic struct {
	mu         sync.RWMutex
	cfg        types.TopicConfig
	createdAt  time.Time
	partitions []*storage.PartitionLog
}

// Config returns the topic's configuration.
func (t *Topic) Config() types.TopicConfig {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cfg
}

// PartitionLog returns the PartitionLog for the given partition index.
func (t *Topic) PartitionLog(id int32) (*storage.PartitionLog, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id < 0 || int(id) >= len(t.partitions) {
		return nil, fmt.Errorf("partition %d out of range for topic %s", id, t.cfg.Name)
	}
	return t.partitions[id], nil
}

// Close closes all partition logs.
func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, pl := range t.partitions {
		_ = pl.Close()
	}
}

// ─── Manager ─────────────────────────────────────────────────────────────────

// Manager manages the set of topics in the broker.
type Manager struct {
	mu          sync.RWMutex
	topics      map[string]*Topic
	partitioner *partition.HashPartitioner
	dataRoot    string
	segMax      int64
	idxEvery    int64
	syncPolicy  string
	retentionMu sync.RWMutex
	retention   *config.RetentionConfig
}

// NewManager creates a Manager.
func NewManager(cfg *config.StorageConfig, p *partition.HashPartitioner) *Manager {
	sp := cfg.SyncPolicy
	if sp == "" {
		sp = "interval"
	}
	return &Manager{
		topics:      make(map[string]*Topic),
		partitioner: p,
		dataRoot:    cfg.DataPath,
		segMax:      cfg.SegmentMaxBytes,
		idxEvery:    cfg.IndexIntervalBytes,
		syncPolicy:  sp,
	}
}

// Create creates a topic with the given configuration.
// Returns a *types.BrokerError with code ErrTopicExists if the topic already
// exists, a plain error for invalid names, and a wrapped error for storage
// failures so callers can use errors.As to distinguish them.
func (m *Manager) Create(cfg types.TopicConfig) error {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.ReplicationFactor <= 0 {
		cfg.ReplicationFactor = 1
	}
	if !validTopicName.MatchString(cfg.Name) {
		return fmt.Errorf("invalid topic name %q: must match [a-zA-Z0-9][a-zA-Z0-9._-]{0,248}", cfg.Name)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.topics[cfg.Name]; ok {
		return types.NewBrokerError(types.ErrTopicExists, fmt.Sprintf("topic %q already exists", cfg.Name))
	}

	t := &Topic{cfg: cfg, createdAt: time.Now()}
	for i := 0; i < cfg.Partitions; i++ {
		dir := partitionDir(m.dataRoot, cfg.Name, int32(i))
		pl, err := storage.OpenPartitionLog(dir, m.segMax, m.idxEvery, m.syncPolicy)
		if err != nil {
			// Roll back successfully created partitions.
			for _, existing := range t.partitions {
				_ = existing.Close()
			}
			return fmt.Errorf("create partition %d for %s: %w", i, cfg.Name, err)
		}
		t.partitions = append(t.partitions, pl)
	}

	m.topics[cfg.Name] = t
	_ = m.partitioner.Register(cfg.Name, int32(cfg.Partitions))
	return nil
}

// Delete removes a topic, releasing all resources.
func (m *Manager) Delete(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.topics[name]
	if !ok {
		return types.NewBrokerError(types.ErrTopicNotFound, fmt.Sprintf("topic %q not found", name))
	}
	t.Close()
	delete(m.topics, name)
	_ = m.partitioner.Register(name, 0)
	return nil
}

// Get returns the Topic for name, or an error.
func (m *Manager) Get(name string) (*Topic, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.topics[name]
	if !ok {
		return nil, types.NewBrokerError(types.ErrTopicNotFound, fmt.Sprintf("topic %q not found", name))
	}
	return t, nil
}

// List returns metadata for all topics.
func (m *Manager) List() []types.TopicMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]types.TopicMetadata, 0, len(m.topics))
	for _, t := range m.topics {
		t.mu.RLock()
		out = append(out, types.TopicMetadata{
			Config:    t.cfg,
			CreatedAt: t.createdAt,
		})
		t.mu.RUnlock()
	}
	return out
}

// Exists reports whether a topic exists.
func (m *Manager) Exists(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.topics[name]
	return ok
}

// CloseAll releases all topics. Called on broker shutdown.
func (m *Manager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, t := range m.topics {
		t.Close()
	}
}

// IsNameError returns true when err is a topic-name validation error.
// Use this in broker handlers to return BAD_REQUEST rather than ErrInternal.
func IsNameError(err error) bool {
	if err == nil {
		return false
	}
	// Name errors are plain fmt.Errorf strings (not *types.BrokerError).
	var be *types.BrokerError
	if errors.As(err, &be) {
		return false
	}
	return true
}

// StartRetentionLoop spawns a background goroutine that periodically compacts
// all partition logs according to the given retention policy. It runs every
// 60 seconds and stops when ctx is cancelled.
func (m *Manager) StartRetentionLoop(ctx context.Context, cfg *config.RetentionConfig, log *logging.Logger) {
	m.retentionMu.Lock()
	m.retention = cfg
	m.retentionMu.Unlock()
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.retentionMu.RLock()
				curCfg := m.retention
				m.retentionMu.RUnlock()
				m.compactAll(curCfg, log)
			}
		}
	}()
}

// UpdateRetentionConfig swaps the live retention configuration. The new
// values take effect on the next scheduled retention sweep (every 60 s),
// not immediately.
func (m *Manager) UpdateRetentionConfig(maxAgeHours int, maxSizeMB int64) {
	m.retentionMu.Lock()
	defer m.retentionMu.Unlock()
	if m.retention != nil {
		m.retention.MaxAgeHours = maxAgeHours
		m.retention.MaxSizeMB = maxSizeMB
	} else {
		m.retention = &config.RetentionConfig{
			MaxAgeHours: maxAgeHours,
			MaxSizeMB:   maxSizeMB,
		}
	}
}

// compactAll iterates over all topics and partitions and calls Compact.
// Topics configured with CompactionMode == "compact" are skipped here; their
// retention is handled exclusively by storage.KeyCompactor.
func (m *Manager) compactAll(cfg *config.RetentionConfig, log *logging.Logger) {
	m.mu.RLock()
	type entry struct {
		name string
		mode string
		pls  []*storage.PartitionLog
	}
	snapshot := make([]entry, 0, len(m.topics))
	for name, t := range m.topics {
		t.mu.RLock()
		pls := make([]*storage.PartitionLog, len(t.partitions))
		copy(pls, t.partitions)
		mode := t.cfg.CompactionMode
		t.mu.RUnlock()
		snapshot = append(snapshot, entry{name: name, mode: mode, pls: pls})
	}
	m.mu.RUnlock()

	for _, e := range snapshot {
		if e.mode == "compact" {
			continue
		}
		for partIdx, pl := range e.pls {
			deleted, err := pl.Compact(int64(cfg.MaxAgeHours), cfg.MaxSizeMB)
			if log != nil {
				if err != nil {
					log.Debug("retention compact",
						"topic", e.name, "partition", partIdx,
						"deleted_segments", deleted, "err", err,
					)
				} else {
					log.Debug("retention compact",
						"topic", e.name, "partition", partIdx,
						"deleted_segments", deleted,
					)
				}
			}
		}
	}
}

// CompactablePartitions returns every PartitionLog belonging to a topic whose
// CompactionMode == "compact". It satisfies storage.PartitionSource so a
// *Manager can be passed directly to storage.KeyCompactor.Start.
func (m *Manager) CompactablePartitions() []*storage.PartitionLog {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*storage.PartitionLog
	for _, t := range m.topics {
		t.mu.RLock()
		if t.cfg.CompactionMode == "compact" {
			out = append(out, t.partitions...)
		}
		t.mu.RUnlock()
	}
	return out
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func partitionDir(root, topic string, partition int32) string {
	return filepath.Join(root, topic, fmt.Sprintf("%d", partition))
}
