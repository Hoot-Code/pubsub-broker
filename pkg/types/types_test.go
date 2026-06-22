package types_test

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── DeliveryMode ─────────────────────────────────────────────────────────────

func TestDeliveryMode_String(t *testing.T) {
	cases := []struct {
		mode types.DeliveryMode
		want string
	}{
		{types.AtMostOnce, "at-most-once"},
		{types.AtLeastOnce, "at-least-once"},
		{types.ExactlyOnce, "exactly-once"},
		{types.DeliveryMode(99), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.mode.String(); got != tc.want {
			t.Errorf("DeliveryMode(%d).String(): want %q, got %q", tc.mode, tc.want, got)
		}
	}
}

// ─── Message ──────────────────────────────────────────────────────────────────

func TestNewMessage_FieldsPopulated(t *testing.T) {
	before := time.Now().UnixNano()
	m := types.NewMessage("orders", []byte("payload"), "key-1", map[string]string{"x": "y"})
	after := time.Now().UnixNano()

	if m.ID == "" {
		t.Error("ID must not be empty")
	}
	if m.Topic != "orders" {
		t.Errorf("Topic: want orders, got %s", m.Topic)
	}
	if string(m.Payload) != "payload" {
		t.Errorf("Payload: want payload, got %s", m.Payload)
	}
	if m.Key != "key-1" {
		t.Errorf("Key: want key-1, got %s", m.Key)
	}
	if m.Headers["x"] != "y" {
		t.Errorf("Header x: want y, got %s", m.Headers["x"])
	}
	if m.Timestamp < before || m.Timestamp > after {
		t.Errorf("Timestamp %d not in [%d, %d]", m.Timestamp, before, after)
	}
}

func TestNewMessage_NilHeaders(t *testing.T) {
	m := types.NewMessage("t", []byte("x"), "", nil)
	if m.Headers != nil {
		// Nil headers are preserved (no allocation) — just ensure no panic.
		_ = m.Headers
	}
}

func TestNewMessage_UniqueIDs(t *testing.T) {
	const n = 1000
	ids := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		m := types.NewMessage("t", nil, "", nil)
		if _, dup := ids[m.ID]; dup {
			t.Fatalf("duplicate ID at iteration %d: %s", i, m.ID)
		}
		ids[m.ID] = struct{}{}
	}
}

// ─── UUID ─────────────────────────────────────────────────────────────────────

func TestNewUUID_Format(t *testing.T) {
	id := types.NewUUID()
	// Expected format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
	if len(id) != 36 {
		t.Errorf("UUID length: want 36, got %d (%q)", len(id), id)
	}
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("UUID parts: want 5, got %d", len(parts))
	}
	lengths := []int{8, 4, 4, 4, 12}
	for i, p := range parts {
		if len(p) != lengths[i] {
			t.Errorf("UUID part[%d]: want len %d, got %d (%q)", i, lengths[i], len(p), p)
		}
	}
}

func TestNewUUID_Version4Variant(t *testing.T) {
	id := types.NewUUID()
	// Version 4: 13th char must be '4'
	if id[14] != '4' {
		t.Errorf("UUID version: want '4' at position 14, got %q in %s", id[14], id)
	}
	// RFC 4122 variant: 17th char must be '8', '9', 'a', or 'b'
	v := id[19]
	if v != '8' && v != '9' && v != 'a' && v != 'b' {
		t.Errorf("UUID variant: want [89ab] at position 19, got %q in %s", v, id)
	}
}

func TestNewUUID_ConcurrentSafe(t *testing.T) {
	const goroutines = 50
	const perGoroutine = 100
	mu := sync.Mutex{}
	seen := make(map[string]struct{}, goroutines*perGoroutine)
	var wg sync.WaitGroup
	errs := make(chan string, goroutines*perGoroutine)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := types.NewUUID()
				mu.Lock()
				if _, dup := seen[id]; dup {
					errs <- id
				}
				seen[id] = struct{}{}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for id := range errs {
		t.Errorf("duplicate UUID: %s", id)
	}
}

// ─── NodeStatus ───────────────────────────────────────────────────────────────

func TestNodeStatus_Values(t *testing.T) {
	if types.NodeActive == "" {
		t.Error("NodeActive must not be empty")
	}
	if types.NodeDegraded == "" {
		t.Error("NodeDegraded must not be empty")
	}
	if types.NodeUnhealthy == "" {
		t.Error("NodeUnhealthy must not be empty")
	}
	if types.NodeActive == types.NodeDegraded || types.NodeActive == types.NodeUnhealthy {
		t.Error("NodeStatus values must be distinct")
	}
}

// ─── BrokerError ─────────────────────────────────────────────────────────────

func TestBrokerError_Error(t *testing.T) {
	e := types.NewBrokerError(types.ErrTopicNotFound, "topic 'x' not found")
	s := e.Error()
	if !strings.Contains(s, "TOPIC_NOT_FOUND") {
		t.Errorf("Error() missing code: %q", s)
	}
	if !strings.Contains(s, "topic 'x' not found") {
		t.Errorf("Error() missing message: %q", s)
	}
}

func TestBrokerError_AllCodes(t *testing.T) {
	codes := []types.ErrorCode{
		types.ErrTopicNotFound,
		types.ErrTopicExists,
		types.ErrUnauthorized,
		types.ErrInvalidMessage,
		types.ErrPartitionNotFound,
		types.ErrBrokerOverloaded,
		types.ErrInternal,
		types.ErrRetryExceeded,
	}
	seen := make(map[types.ErrorCode]struct{})
	for _, c := range codes {
		if c == "" {
			t.Error("ErrorCode must not be empty")
		}
		if _, dup := seen[c]; dup {
			t.Errorf("duplicate ErrorCode: %q", c)
		}
		seen[c] = struct{}{}

		e := types.NewBrokerError(c, "msg")
		if e.Code != c {
			t.Errorf("Code: want %q, got %q", c, e.Code)
		}
	}
}

// ─── TopicConfig ──────────────────────────────────────────────────────────────

func TestTopicConfig_ZeroValues(t *testing.T) {
	var cfg types.TopicConfig
	if cfg.Name != "" {
		t.Error("zero Name should be empty string")
	}
	if cfg.Partitions != 0 {
		t.Error("zero Partitions should be 0")
	}
}

func TestTopicMetadata_CreatedAt(t *testing.T) {
	m := types.TopicMetadata{
		Config:    types.TopicConfig{Name: "t", Partitions: 1},
		CreatedAt: time.Now(),
	}
	if m.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

// ─── ConsumerGroupOffset ──────────────────────────────────────────────────────

func TestConsumerGroupOffset_Fields(t *testing.T) {
	o := types.ConsumerGroupOffset{
		Group:     "g1",
		Topic:     "t1",
		Partition: 2,
		Offset:    42,
		UpdatedAt: time.Now(),
	}
	if o.Group != "g1" {
		t.Errorf("Group: want g1, got %s", o.Group)
	}
	if o.Offset != 42 {
		t.Errorf("Offset: want 42, got %d", o.Offset)
	}
}

// ─── Permission constants ─────────────────────────────────────────────────────

func TestPermissions_NotEmpty(t *testing.T) {
	perms := []types.Permission{types.PermPublish, types.PermSubscribe, types.PermAdmin}
	seen := make(map[types.Permission]struct{})
	for _, p := range perms {
		if p == "" {
			t.Error("Permission must not be empty")
		}
		if _, dup := seen[p]; dup {
			t.Errorf("duplicate Permission: %q", p)
		}
		seen[p] = struct{}{}
	}
}
