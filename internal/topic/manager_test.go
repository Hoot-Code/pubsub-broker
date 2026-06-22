package topic_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/Hoot-Code/pubsub-broker/internal/config"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/internal/topic"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

func newManager(t *testing.T) *topic.Manager {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.StorageConfig{
		DataPath:           dir,
		SegmentMaxBytes:    1 << 20,
		IndexIntervalBytes: 512,
	}
	p := partition.NewHashPartitioner()
	return topic.NewManager(cfg, p)
}

func TestManager_CreateAndGet(t *testing.T) {
	m := newManager(t)
	defer m.CloseAll()

	cfg := types.TopicConfig{Name: "orders", Partitions: 4, ReplicationFactor: 1}
	if err := m.Create(cfg); err != nil {
		t.Fatalf("Create: %v", err)
	}

	tp, err := m.Get("orders")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if tp.Config().Partitions != 4 {
		t.Errorf("partitions: want 4, got %d", tp.Config().Partitions)
	}
}

func TestManager_CreateDuplicate(t *testing.T) {
	m := newManager(t)
	defer m.CloseAll()

	cfg := types.TopicConfig{Name: "dup", Partitions: 1}
	_ = m.Create(cfg)
	err := m.Create(cfg)
	if err == nil {
		t.Fatal("expected error creating duplicate topic")
	}
	be, ok := err.(*types.BrokerError)
	if !ok || be.Code != types.ErrTopicExists {
		t.Errorf("wrong error type: %v", err)
	}
}

func TestManager_Delete(t *testing.T) {
	m := newManager(t)
	defer m.CloseAll()

	_ = m.Create(types.TopicConfig{Name: "tmp", Partitions: 2})
	if err := m.Delete("tmp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if m.Exists("tmp") {
		t.Error("topic should not exist after deletion")
	}
}

func TestManager_DeleteNonExistent(t *testing.T) {
	m := newManager(t)
	defer m.CloseAll()
	err := m.Delete("ghost")
	if err == nil {
		t.Fatal("expected error deleting non-existent topic")
	}
}

func TestManager_List(t *testing.T) {
	m := newManager(t)
	defer m.CloseAll()

	for _, name := range []string{"a", "b", "c"} {
		_ = m.Create(types.TopicConfig{Name: name, Partitions: 1})
	}
	list := m.List()
	if len(list) != 3 {
		t.Errorf("list: want 3 topics, got %d", len(list))
	}
}

func TestManager_PartitionLog(t *testing.T) {
	m := newManager(t)
	defer m.CloseAll()

	_ = m.Create(types.TopicConfig{Name: "pl-test", Partitions: 3})
	tp, _ := m.Get("pl-test")

	_, err := tp.PartitionLog(2)
	if err != nil {
		t.Fatalf("PartitionLog(2): %v", err)
	}
	_, err = tp.PartitionLog(3) // out of range
	if err == nil {
		t.Error("expected error for out-of-range partition")
	}
}

// ─── typed error codes ──────────────────────────────────────────────────

// TestManager_Create_ErrTopicExists verifies that creating a topic twice returns
// a *types.BrokerError with code ErrTopicExists.
func TestManager_Create_ErrTopicExists(t *testing.T) {
	t.Parallel()
	m := newManager(t)
	defer m.CloseAll()

	cfg := types.TopicConfig{Name: "dup", Partitions: 1, ReplicationFactor: 1}
	if err := m.Create(cfg); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	err := m.Create(cfg)
	if err == nil {
		t.Fatal("second Create should fail with ErrTopicExists")
	}
	var brokerErr *types.BrokerError
	if !errors.As(err, &brokerErr) {
		t.Fatalf("want *types.BrokerError, got %T: %v", err, err)
	}
	if brokerErr.Code != types.ErrTopicExists {
		t.Errorf("code: want %s, got %s", types.ErrTopicExists, brokerErr.Code)
	}
}

// TestManager_Create_BadName verifies that invalid topic names return a plain
// error (not *types.BrokerError) containing "invalid topic name".
func TestManager_Create_BadName(t *testing.T) {
	t.Parallel()
	m := newManager(t)
	defer m.CloseAll()

	badNames := []string{"", "/bad", "has space", ".dot-start", strings.Repeat("x", 251)}
	for _, name := range badNames {
		err := m.Create(types.TopicConfig{Name: name, Partitions: 1})
		if err == nil {
			t.Errorf("Create(%q): want error, got nil", name)
			continue
		}
		var brokerErr *types.BrokerError
		if errors.As(err, &brokerErr) {
			t.Errorf("Create(%q): got *BrokerError, want plain name-validation error", name)
		}
		if !strings.Contains(err.Error(), "invalid topic name") {
			t.Errorf("Create(%q): error %q doesn't mention 'invalid topic name'", name, err.Error())
		}
	}
}
