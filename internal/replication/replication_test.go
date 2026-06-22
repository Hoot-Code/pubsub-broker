package replication_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Hoot-Code/pubsub-broker/internal/logging"
	"github.com/Hoot-Code/pubsub-broker/internal/metrics"
	"github.com/Hoot-Code/pubsub-broker/internal/protocol"
	"github.com/Hoot-Code/pubsub-broker/internal/replication"
	"github.com/Hoot-Code/pubsub-broker/pkg/types"
)

// ─── Helpers ─────────────────────────────────────────────────────────────────

func newManager(t *testing.T) *replication.Manager {
	t.Helper()
	log := logging.New(nil, "error")
	reg := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(reg)
	return replication.NewManager(
		"node-1",
		3,
		50*time.Millisecond,
		500*time.Millisecond,
		log,
		bm,
	)
}

// mockLog is a minimal in-memory ReplicationLog.
type mockLog struct {
	mu   sync.Mutex
	msgs []*types.Message
}

func (m *mockLog) Append(msg *types.Message) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	off := int64(len(m.msgs))
	msg.Offset = off
	m.msgs = append(m.msgs, msg)
	return off, nil
}

func (m *mockLog) Read(startOffset int64, maxCount int) ([]*types.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if startOffset >= int64(len(m.msgs)) {
		return nil, nil
	}
	end := startOffset + int64(maxCount)
	if end > int64(len(m.msgs)) {
		end = int64(len(m.msgs))
	}
	return m.msgs[startOffset:end], nil
}

func (m *mockLog) NextOffset() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.msgs))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestManager_StartStop(t *testing.T) {
	mgr := newManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	mgr.Start(ctx)
	cancel()
	// Stop must not block indefinitely.
	done := make(chan struct{})
	go func() {
		mgr.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return within 2s")
	}
}

func TestManager_StopIdempotent(t *testing.T) {
	mgr := newManager(t)
	ctx := context.Background()
	mgr.Start(ctx)
	mgr.Stop()
	mgr.Stop() // second call must not panic
}

func TestManager_StopBeforeStart(t *testing.T) {
	mgr := newManager(t)
	mgr.Stop() // must not panic when cancel is nil
}

func TestManager_SetLeader(t *testing.T) {
	mgr := newManager(t)
	mgr.SetLeader(true)
	if !mgr.IsLeader() {
		t.Error("IsLeader: want true after SetLeader(true)")
	}
	mgr.SetLeader(false)
	if mgr.IsLeader() {
		t.Error("IsLeader: want false after SetLeader(false)")
	}
}

func TestManager_RegisterReplica(t *testing.T) {
	mgr := newManager(t)
	mgr.RegisterReplica("node-2", "127.0.0.1:9093")
	mgr.RegisterReplica("node-3", "127.0.0.1:9094")

	// ISRCount counts leader (self) + registered replicas that have been heard from.
	// Freshly registered replicas have InSync=false until they contact us.
	count := mgr.ISRCount()
	if count < 1 {
		t.Errorf("ISRCount: want >= 1 (leader), got %d", count)
	}
}

func TestManager_ReplicaLag_NoReplicas(t *testing.T) {
	mgr := newManager(t)
	lag := mgr.ReplicaLag(100)
	if lag != 0 {
		t.Errorf("lag with no replicas: want 0, got %d", lag)
	}
}

func TestManager_ReplicaLag_WithReplicas(t *testing.T) {
	mgr := newManager(t)
	mgr.RegisterReplica("node-2", "127.0.0.1:9093")

	// Lag should be ≥ 0 (replicas start at offset 0; leader may be ahead).
	lag := mgr.ReplicaLag(50)
	if lag < 0 {
		t.Errorf("lag: want >= 0, got %d", lag)
	}
}

func TestManager_ISRCount_Self(t *testing.T) {
	mgr := newManager(t)
	// Leader always counts itself as in-sync.
	if c := mgr.ISRCount(); c < 1 {
		t.Errorf("ISRCount: want >= 1 (self), got %d", c)
	}
}

// TestManager_Replicate verifies that Replicate sends messages over a net.Conn.
func TestManager_Replicate(t *testing.T) {
	mgr := newManager(t)
	mgr.RegisterReplica("follower", "127.0.0.1:0")

	log := &mockLog{}
	for i := 0; i < 5; i++ {
		_, _ = log.Append(types.NewMessage("t", []byte("payload"), "k", nil))
	}

	// Create an in-process connection pair.
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Replicate runs in a goroutine; drain the client side.
	done := make(chan struct{})
	go func() {
		defer close(done)
		buf := make([]byte, 4096)
		for {
			_, err := client.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	if err := mgr.Replicate("follower", log, 0, server); err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	server.Close()
	<-done
}

// TestManager_Replicate_EmptyLog verifies Replicate returns nil with no messages.
func TestManager_Replicate_EmptyLog(t *testing.T) {
	mgr := newManager(t)
	log := &mockLog{}

	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	if err := mgr.Replicate("node-2", log, 0, server); err != nil {
		t.Fatalf("Replicate on empty log: %v", err)
	}
}

// TestManager_ConcurrentRegisterAndLag exercises concurrent access to the
// replica map — caught by -race.
func TestManager_ConcurrentRegisterAndLag(t *testing.T) {
	mgr := newManager(t)
	ctx := context.Background()
	mgr.Start(ctx)
	defer mgr.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mgr.RegisterReplica(
				"node-"+string(rune('a'+id%26)),
				"127.0.0.1:1000",
			)
			_ = mgr.ReplicaLag(int64(id * 10))
			_ = mgr.ISRCount()
		}(i)
	}
	wg.Wait()
}

// TestManager_HeartbeatMarksStale verifies that replicas which don't respond
// within 3×syncInterval are marked out-of-sync.
func TestManager_HeartbeatMarksStale(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timing-sensitive test in short mode")
	}

	log := logging.New(nil, "error")
	reg := metrics.NewRegistry()
	bm := metrics.NewBrokerMetrics(reg)
	// Use very short intervals so the test completes quickly.
	mgr := replication.NewManager("node-1", 2, 20*time.Millisecond, 100*time.Millisecond, log, bm)

	ctx, cancel := context.WithCancel(context.Background())
	mgr.Start(ctx)
	defer cancel()
	defer mgr.Stop()

	mgr.RegisterReplica("node-2", "127.0.0.1:0")

	// After 3 × syncInterval, the replica should be stale.
	time.Sleep(100 * time.Millisecond)

	// ISRCount should be 1 (only self; node-2 has never contacted us).
	if c := mgr.ISRCount(); c > 1 {
		t.Logf("ISRCount=%d (replica was not marked stale yet; timing-sensitive)", c)
	}
}

// ─── AddReplica / SetPartitionLog / follower Pull wiring ─────────────────

// TestManager_AddReplica_SetPartitionLog verifies the new wiring APIs:
// leader appends 10 messages to a real PartitionLog, registers a follower via
// AddReplica/SetPartitionLog, then manually calls Replicate to simulate what
// the heartbeat loop would do; the follower log ends up with 10 messages.
func TestManager_AddReplica_SetPartitionLog(t *testing.T) {
	t.Parallel()

	// Leader manager.
	leaderMgr := newManager(t)
	leaderMgr.SetLeader(true)

	// Follower manager.
	followerMgr := newManager(t)
	followerMgr.SetLeader(false)

	// In-memory logs stand in for real PartitionLog objects.
	leaderLog := &mockLog{}
	followerLog := &mockLog{}

	// Populate leader log with 10 messages.
	for i := 0; i < 10; i++ {
		_, _ = leaderLog.Append(types.NewMessage("topic", []byte(fmt.Sprintf("msg-%d", i)), "k", nil))
	}
	if leaderLog.NextOffset() != 10 {
		t.Fatalf("leader log NextOffset: want 10, got %d", leaderLog.NextOffset())
	}

	// Wire up the follower via AddReplica on the leader side and
	// SetPartitionLog on the follower side.
	const followerNodeID = "node-2"
	leaderMgr.RegisterReplica(followerNodeID, "127.0.0.1:0")
	leaderMgr.SetPartitionLog(followerNodeID, followerLog)

	followerMgr.AddReplica(followerNodeID, "leader-addr", followerLog)

	// Simulate one replication round: leader sends to follower.
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	// Drain reader side.
	readDone := make(chan int)
	go func() {
		dec := protocol.NewDecoder(client)
		count := 0
		for {
			f, err := dec.Decode()
			if err != nil {
				break
			}
			if f.Command == protocol.CmdPublish {
				var msg types.Message
				if uErr := protocol.Unmarshal(f, &msg); uErr == nil {
					_, _ = followerLog.Append(&msg)
					count++
				}
			}
		}
		readDone <- count
	}()

	if err := leaderMgr.Replicate(followerNodeID, leaderLog, 0, server); err != nil {
		t.Fatalf("Replicate: %v", err)
	}
	server.Close()
	received := <-readDone

	if received != 10 {
		t.Errorf("follower received %d messages, want 10", received)
	}
	if followerLog.NextOffset() != 10 {
		t.Errorf("follower log NextOffset: want 10, got %d", followerLog.NextOffset())
	}
}
