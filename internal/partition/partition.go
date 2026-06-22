// Package partition handles message-to-partition routing.
// Keys are routed deterministically via FNV-1a hash; keyless messages
// use per-topic atomic round-robin for balanced distribution.
package partition

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
)

// Partitioner routes messages to partitions.
type Partitioner interface {
	// Assign returns the partition index for a given key and topic.
	Assign(topic, key string) (int32, error)
}

// ─── HashPartitioner ─────────────────────────────────────────────────────────

// HashPartitioner routes keyed messages deterministically and keyless
// messages via round-robin, both based on the configured partition count
// per topic.
type HashPartitioner struct {
	mu     sync.RWMutex
	topics map[string]int32  // topic → partition count
	rrCur  map[string]*int64 // topic → round-robin counter
}

// NewHashPartitioner returns a HashPartitioner with no topics registered.
func NewHashPartitioner() *HashPartitioner {
	return &HashPartitioner{
		topics: make(map[string]int32),
		rrCur:  make(map[string]*int64),
	}
}

// Register adds (or updates) the partition count for a topic.
// Calling with count==0 removes the topic.
func (p *HashPartitioner) Register(topic string, count int32) error {
	if count < 0 {
		return fmt.Errorf("partition count must be >= 0, got %d", count)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if count == 0 {
		delete(p.topics, topic)
		delete(p.rrCur, topic)
		return nil
	}
	p.topics[topic] = count
	if _, ok := p.rrCur[topic]; !ok {
		var ctr int64
		p.rrCur[topic] = &ctr
	}
	return nil
}

// Assign returns the target partition for (topic, key).
func (p *HashPartitioner) Assign(topic, key string) (int32, error) {
	p.mu.RLock()
	count, ok := p.topics[topic]
	ctr := p.rrCur[topic]
	p.mu.RUnlock()

	if !ok {
		return 0, fmt.Errorf("partition: topic %q not registered", topic)
	}
	if key != "" {
		return hashPartition(key, count), nil
	}
	// Round-robin for keyless messages.
	idx := atomic.AddInt64(ctr, 1) - 1
	return int32(idx % int64(count)), nil
}

// TopicPartitionCount returns the partition count for a topic (0 if unknown).
func (p *HashPartitioner) TopicPartitionCount(topic string) int32 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.topics[topic]
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// hashPartition maps a string key to a partition in [0, n).
func hashPartition(key string, n int32) int32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int32(h.Sum32() % uint32(n))
}

// ─── OffsetStore ─────────────────────────────────────────────────────────────
// Persistent consumer-group offset tracking, stored per partition directory.
// The OffsetStore is embedded here (close conceptually to partition logic)
// and is used by the consumer package.

// OffsetStore tracks committed offsets for (group, topic, partition) triples.
type OffsetStore struct {
	mu      sync.RWMutex
	offsets map[string]int64 // key: "group/topic/partition"
}

// NewOffsetStore creates an in-memory OffsetStore (persisted externally via WAL).
func NewOffsetStore() *OffsetStore {
	return &OffsetStore{offsets: make(map[string]int64)}
}

// Commit records the offset for the given group/topic/partition.
func (s *OffsetStore) Commit(group, topic string, partition int32, offset int64) {
	s.mu.Lock()
	s.offsets[offsetKey(group, topic, partition)] = offset
	s.mu.Unlock()
}

// Load returns the last committed offset, or -1 if none.
func (s *OffsetStore) Load(group, topic string, partition int32) int64 {
	s.mu.RLock()
	v, ok := s.offsets[offsetKey(group, topic, partition)]
	s.mu.RUnlock()
	if !ok {
		return -1
	}
	return v
}

// Snapshot returns all committed offsets for external persistence.
func (s *OffsetStore) Snapshot() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64, len(s.offsets))
	for k, v := range s.offsets {
		out[k] = v
	}
	return out
}

// Restore loads offsets from a previously saved snapshot.
func (s *OffsetStore) Restore(snapshot map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, v := range snapshot {
		s.offsets[k] = v
	}
}

func offsetKey(group, topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d", group, topic, partition)
}
