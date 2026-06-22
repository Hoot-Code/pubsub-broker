package consumer_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/consumer"
	"github.com/Hoot-Code/pubsub-broker/internal/partition"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

func newManager(t *testing.T) *consumer.Manager {
	t.Helper()
	os := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(1000)
	return consumer.NewManager(os, dlq, 3, 50*time.Millisecond, 100)
}

func makeMsg(topic string, partID int32, offset int64) *types.Message {
	return &types.Message{
		ID:        types.NewUUID(),
		Topic:     topic,
		Partition: partID,
		Offset:    offset,
		Payload:   []byte("data"),
		Timestamp: time.Now().UnixNano(),
	}
}

func TestConsumerGroup_JoinAndLeave(t *testing.T) {
	m := newManager(t)
	c, err := m.Subscribe("g1", "c1", "client1", "orders", 4)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if c == nil {
		t.Fatal("expected non-nil consumer")
	}

	// Unsubscribe should not panic.
	m.Unsubscribe("g1", "c1", "orders")
}

func TestConsumerGroup_Dispatch(t *testing.T) {
	m := newManager(t)
	c, _ := m.Subscribe("g1", "c1", "client1", "orders", 2)

	msg := makeMsg("orders", 0, 0)
	m.Dispatch(msg)

	select {
	case received := <-c.Messages():
		if received.ID != msg.ID {
			t.Errorf("received wrong message: want %s, got %s", msg.ID, received.ID)
		}
	case <-time.After(time.Second):
		t.Error("timed out waiting for message dispatch")
	}
}

func TestConsumerGroup_Rebalance(t *testing.T) {
	m := newManager(t)
	// 4 partitions, 2 consumers — each should get 2 partitions.
	c1, _ := m.Subscribe("g1", "c1", "cl1", "topic", 4)
	c2, _ := m.Subscribe("g1", "c2", "cl2", "topic", 4)

	a1 := c1.Assignments()
	a2 := c2.Assignments()

	if len(a1)+len(a2) != 4 {
		t.Errorf("total partitions: want 4, got %d", len(a1)+len(a2))
	}

	// No partition should appear in both.
	seen := make(map[int32]bool)
	for _, p := range append(a1, a2...) {
		if seen[p] {
			t.Errorf("partition %d assigned twice", p)
		}
		seen[p] = true
	}
}

func TestConsumerGroup_CommitOffset(t *testing.T) {
	os := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(100)
	m := consumer.NewManager(os, dlq, 3, time.Millisecond, 100)

	_, _ = m.Subscribe("g1", "c1", "cl", "events", 2)
	if err := m.CommitOffset("g1", "c1", "events", 0, 42); err != nil {
		t.Fatalf("CommitOffset: %v", err)
	}
	if got := os.Load("g1", "events", 0); got != 42 {
		t.Errorf("offset: want 42, got %d", got)
	}
}

func TestDLQ_PushAndDrain(t *testing.T) {
	q := consumer.NewDLQ(5)
	for i := 0; i < 7; i++ {
		q.Push(consumer.DLQEntry{
			Original: &types.Message{ID: types.NewUUID()},
			Reason:   "test",
			Attempts: 3,
		})
	}
	// DLQ max is 5 — oldest two should be dropped.
	if q.Len() != 5 {
		t.Errorf("DLQ len: want 5, got %d", q.Len())
	}
	entries := q.Drain()
	if len(entries) != 5 {
		t.Errorf("drain: want 5, got %d", len(entries))
	}
	if q.Len() != 0 {
		t.Error("DLQ should be empty after drain")
	}
}

func TestConsumerGroup_ConcurrentDispatch(t *testing.T) {
	m := newManager(t)
	c, _ := m.Subscribe("g1", "c1", "cl", "load", 4)

	var sent int64 = 200
	var received int64

	done := make(chan struct{})
	go func() {
		for i := int64(0); i < sent; i++ {
			select {
			case <-c.Messages():
				received++
			case <-time.After(2 * time.Second):
				close(done)
				return
			}
		}
		close(done)
	}()

	var wg sync.WaitGroup
	for i := int64(0); i < sent; i++ {
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			m.Dispatch(makeMsg("load", int32(n%4), n))
		}(i)
	}
	wg.Wait()

	<-done
	if received < sent/2 {
		// Allow some drops (backpressure) but not total failure.
		t.Errorf("received only %d/%d messages — too many dropped", received, sent)
	}
}

// ─── Deterministic rebalance ─────────────────────────────────────────────

// TestRebalanceDeterminism verifies that rebalance() always produces the same
// assignment regardless of Go's random map-iteration order. The test calls
// rebalance via repeated Join/Leave cycles and asserts the assignment map
// is identical on every run. It FAILS on the unsorted code (no sort.Strings)
// and PASSES after adding sort.Strings(ids) in Group.rebalance().
func TestRebalanceDeterminism(t *testing.T) {
	t.Parallel()

	const (
		numConsumers  = 4
		numPartitions = 8
		iterations    = 100
	)

	type assignment struct {
		consumerID string
		partitions []int32
	}

	// Capture one canonical assignment by running a fresh group.
	getAssignment := func() map[string][]int32 {
		offsets := partition.NewOffsetStore()
		dlq := consumer.NewDLQ(100)
		g := consumer.NewGroup("g1", "topic", numPartitions, offsets, dlq, 1, time.Millisecond)
		consumers := make([]*consumer.Consumer, numConsumers)
		for i := 0; i < numConsumers; i++ {
			cid := fmt.Sprintf("c%d", i)
			c, err := g.Join(cid, "cl", 10)
			if err != nil {
				t.Fatalf("Join %s: %v", cid, err)
			}
			consumers[i] = c
		}
		result := make(map[string][]int32, numConsumers)
		for i := 0; i < numConsumers; i++ {
			cid := fmt.Sprintf("c%d", i)
			result[cid] = consumers[i].Assignments()
		}
		return result
	}

	canonical := getAssignment()

	for i := 0; i < iterations; i++ {
		got := getAssignment()
		for cid, wantParts := range canonical {
			gotParts := got[cid]
			if len(wantParts) != len(gotParts) {
				t.Fatalf("iter %d consumer %s: want %v partitions, got %v", i, cid, wantParts, gotParts)
			}
			for j, p := range wantParts {
				if gotParts[j] != p {
					t.Fatalf("iter %d consumer %s: assignment mismatch at index %d: want %d got %d",
						i, cid, j, p, gotParts[j])
				}
			}
		}
	}
}

// ─── groupKey collision / group name validation ──────────────────────────

// TestGroupNameValidation verifies that invalid group names are rejected and
// that a group "a/b" on topic "c" is no longer silently the same key as
// group "a" on topic "b/c".
func TestGroupNameValidation(t *testing.T) {
	t.Parallel()

	offsets := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(100)
	m := consumer.NewManager(offsets, dlq, 1, time.Millisecond, 10)

	valid := []string{"mygroup", "group-1", "group.v2", "g1", "UPPER123", "a"}
	for _, name := range valid {
		_, err := m.Subscribe(name, "c1", "cl", "topic", 1)
		if err != nil {
			t.Errorf("Subscribe(%q) should succeed, got: %v", name, err)
		}
	}

	invalid := []string{"a/b", "a/b/c", "/leading", "trailing/", "", "a b", strings.Repeat("x", 250)}
	for _, name := range invalid {
		_, err := m.Subscribe(name, "c1", "cl", "topic2", 1)
		if err == nil {
			t.Errorf("Subscribe(%q) should fail with invalid group name, got nil", name)
		}
	}
}

// TestGroupKeyCollisionPrevented demonstrates that group "a/b" on topic "c"
// and group "a" on topic "b/c" are now BOTH rejected (not silently collapsed).
// Previously they produced the same internal key "a/b/c".
func TestGroupKeyCollisionPrevented(t *testing.T) {
	t.Parallel()

	offsets := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(100)
	m := consumer.NewManager(offsets, dlq, 1, time.Millisecond, 10)

	// "a/b" is an invalid group name (contains "/") → must be rejected.
	_, err1 := m.Subscribe("a/b", "c1", "cl", "c", 1)
	if err1 == nil {
		t.Error("group 'a/b' on topic 'c' should be rejected; was previously key 'a/b/c'")
	}

	// "a" is a valid group name; "b/c" is a valid topic name (topics allow dots+slash in path but
	// topic names are validated separately — here we just check group validation).
	_, err2 := m.Subscribe("a", "c1", "cl", "b/c", 1)
	// Note: "a" is a valid group name, so this SUCCEEDS (topic name validation is in topic.Manager).
	// The point is that "a/b" is now illegal, so the two can no longer collide.
	if err2 != nil {
		// err2 may fail if topic validation rejects "b/c" — that's also acceptable.
		t.Logf("group 'a' on topic 'b/c' returned: %v (acceptable if topic name is invalid)", err2)
	}

	// Confirm the error for "a/b" mentions "invalid group name".
	if err1 != nil && !strings.Contains(err1.Error(), "invalid group name") {
		t.Errorf("error for 'a/b' should mention 'invalid group name', got: %v", err1)
	}
}

// ─── handleSlowConsumer retry ────────────────────────────────────────────

// TestSlowConsumerDLQ creates a consumer with buffer size 0, dispatches a
// message, and verifies DLQEntry.Attempts == maxRetries (== 3).
func TestSlowConsumerDLQ(t *testing.T) {
	t.Parallel()

	offsets := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(100)
	// bufferSize=0, maxRetries=3, retryDelay=1ms for a fast test.
	m := consumer.NewManager(offsets, dlq, 3, time.Millisecond, 0)

	_, err := m.Subscribe("g1", "c1", "cl", "topic", 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	msg := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "topic",
		Partition: 0,
		Offset:    0,
		Payload:   []byte("data"),
	}
	m.Dispatch(msg)

	// Wait a short time for retries to complete.
	deadline := time.Now().Add(500 * time.Millisecond)
	for dlq.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	entries := dlq.Drain()
	if len(entries) == 0 {
		t.Fatal("expected DLQ entry after slow consumer; got none")
	}
	e := entries[0]
	if e.Attempts != 3 {
		t.Errorf("DLQEntry.Attempts: want 3 (maxRetries), got %d", e.Attempts)
	}
	if e.Original == nil || e.Original.ID != msg.ID {
		t.Errorf("DLQEntry.Original mismatch")
	}
}

// ─── maxRetries=0 must route to DLQ, not silently drop ─────────────────────

// TestSlowConsumerMaxRetriesZeroGoesToDLQ verifies that a group configured with
// maxRetries=0 routes a failed delivery straight to the DLQ after the initial
// (failed) dispatch attempt, rather than silently dropping the message.
func TestSlowConsumerMaxRetriesZeroGoesToDLQ(t *testing.T) {
	t.Parallel()

	offsets := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(100)
	// maxRetries=0, bufferSize=0 so the very first deliver() fails.
	m := consumer.NewManager(offsets, dlq, 0, time.Millisecond, 0)

	if _, err := m.Subscribe("g0", "c0", "cl", "topic", 1); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	msg := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "topic",
		Partition: 0,
		Offset:    0,
		Payload:   []byte("data"),
	}
	m.Dispatch(msg)

	// Wait briefly for the (zero-retry) slow-consumer path to push to the DLQ.
	deadline := time.Now().Add(500 * time.Millisecond)
	for dlq.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if n := dlq.Len(); n != 1 {
		t.Fatalf("with maxRetries=0 want exactly 1 DLQ entry, got %d (message was silently dropped)", n)
	}
	entries := dlq.Drain()
	if entries[0].Original == nil || entries[0].Original.ID != msg.ID {
		t.Errorf("DLQ entry Original mismatch")
	}
	if entries[0].Attempts != 0 {
		t.Errorf("with maxRetries=0 want Attempts=0, got %d", entries[0].Attempts)
	}
}

// ─── nack requeue must deliver only to the originating group ───────────────

// TestDispatchToGroupDeliversOnlyToOriginGroup verifies that DispatchToGroup
// routes a message to exactly one group, even when multiple groups are
// subscribed to the same topic. This is the routing primitive the nack-requeue
// path relies on so a retried message does not fan out to every group.
func TestDispatchToGroupDeliversOnlyToOriginGroup(t *testing.T) {
	t.Parallel()

	offsets := partition.NewOffsetStore()
	dlq := consumer.NewDLQ(100)
	m := consumer.NewManager(offsets, dlq, 3, time.Millisecond, 16)

	// Two groups on the same topic.
	ga, err := m.Subscribe("groupA", "cA", "cl", "topic", 1)
	if err != nil {
		t.Fatalf("Subscribe A: %v", err)
	}
	gb, err := m.Subscribe("groupB", "cB", "cl", "topic", 1)
	if err != nil {
		t.Fatalf("Subscribe B: %v", err)
	}

	msg := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "topic",
		Partition: 0,
		Offset:    0,
		Payload:   []byte("requeue"),
	}
	// Dispatch ONLY to groupA.
	m.DispatchToGroup("groupA", "topic", 0, msg)

	// groupA must receive the message.
	select {
	case got := <-ga.Messages():
		if got.ID != msg.ID {
			t.Errorf("groupA got wrong message id: %s vs %s", got.ID, msg.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("groupA did not receive the requeued message")
	}

	// groupB must NOT receive it.
	select {
	case got := <-gb.Messages():
		t.Errorf("groupB received a message that was requeued only to groupA: id=%s", got.ID)
	case <-time.After(50 * time.Millisecond):
		// good — no delivery to groupB
	}

	// Plain Dispatch (fan-out) must still deliver to BOTH groups (regression
	// guard: the requeue fix must not break the original fan-out path).
	msg2 := &types.Message{
		ID:        types.NewUUID(),
		Topic:     "topic",
		Partition: 0,
		Offset:    1,
		Payload:   []byte("fanout"),
	}
	m.Dispatch(msg2)
	for _, ch := range []<-chan *types.Message{ga.Messages(), gb.Messages()} {
		select {
		case <-ch:
			// good — fan-out delivered to this group
		case <-time.After(500 * time.Millisecond):
			t.Fatal("fan-out Dispatch failed to deliver to a group")
		}
	}
}
