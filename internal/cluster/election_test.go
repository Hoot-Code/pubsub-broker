package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ─── In-process test transport ───────────────────────────────────────────────

// testBus routes messages between in-process transports by addr.
type testBus struct {
	mu      sync.Mutex
	members map[string]chan *ClusterMsg // addr → recv channel
	closed  map[string]bool
}

func newTestBus() *testBus {
	return &testBus{
		members: make(map[string]chan *ClusterMsg),
		closed:  make(map[string]bool),
	}
}

// register creates and returns the recv channel for the given addr.
func (b *testBus) register(addr string) chan *ClusterMsg {
	ch := make(chan *ClusterMsg, 256)
	b.mu.Lock()
	b.members[addr] = ch
	b.mu.Unlock()
	return ch
}

// unregister removes addr from routing so subsequent sends silently drop.
func (b *testBus) unregister(addr string) {
	b.mu.Lock()
	b.closed[addr] = true
	b.mu.Unlock()
}

func (b *testBus) deliver(addr string, msg *ClusterMsg) error {
	b.mu.Lock()
	ch := b.members[addr]
	closed := b.closed[addr]
	b.mu.Unlock()
	if closed || ch == nil {
		return nil // silently drop — peer is offline
	}
	select {
	case ch <- msg:
	default:
		// drop on full buffer to avoid blocking tests
	}
	return nil
}

// inProcTransport is a Transporter backed by the shared testBus.
type inProcTransport struct {
	addr string
	bus  *testBus
	recv chan *ClusterMsg
}

func newInProcTransport(addr string, bus *testBus) *inProcTransport {
	return &inProcTransport{
		addr: addr,
		bus:  bus,
		recv: bus.register(addr),
	}
}

func (t *inProcTransport) Send(addr string, msg *ClusterMsg) error {
	return t.bus.deliver(addr, msg)
}

func (t *inProcTransport) Recv() <-chan *ClusterMsg {
	return t.recv
}

func (t *inProcTransport) Close() error {
	t.bus.unregister(t.addr)
	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// pumpMessages reads from tr.Recv() and calls e.OnMessage() until ctx is done.
func pumpMessages(ctx context.Context, tr Transporter, e *Election) {
	go func() {
		for {
			select {
			case msg, ok := <-tr.Recv():
				if !ok {
					return
				}
				e.OnMessage(msg)
			case <-ctx.Done():
				return
			}
		}
	}()
}

// waitForRole polls until e.Role() == want or the deadline elapses.
func waitForRole(e *Election, want Role, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if e.Role() == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitForLeader polls until e.Leader() returns a Member with the given NodeID.
func waitForLeader(e *Election, nodeID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l, ok := e.Leader(); ok && l.NodeID == nodeID {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// makeElection builds a fully wired Election with an in-proc transport.
func makeElection(bus *testBus, self Member, peers []Member) (*Election, *inProcTransport) {
	m := NewMembership(self)
	for _, p := range peers {
		m.Add(p)
	}
	tr := newInProcTransport(self.Addr, bus)
	e := NewElection(self, m, nil)
	e.SetTransport(tr)
	// Use tighter timeouts so tests run fast.
	e.heartbeatIntervalMs = 50
	e.electionTimeoutMinMs = 150
	e.electionTimeoutMaxMs = 250
	return e, tr
}

// ─── Tests ────────────────────────────────────────────────────────────────────

// TestSingleNodeBecomesLeader verifies that a cluster of 1 elects itself leader
// within 1 second.
func TestSingleNodeBecomesLeader(t *testing.T) {
	t.Parallel()
	bus := newTestBus()
	self := Member{NodeID: "node-a", Addr: "node-a:0"}
	e, tr := makeElection(bus, self, nil)
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pumpMessages(ctx, tr, e)
	e.Start(ctx)

	if !waitForRole(e, RoleLeader, 1*time.Second) {
		t.Fatalf("single node did not become leader within 1s; role=%v term=%d", e.Role(), e.Term())
	}
}

// TestHighestNodeWins starts 3 in-process Election instances with NodeIDs
// "node-a", "node-b", "node-c" and verifies "node-c" becomes the leader on
// all three within 3 seconds.
func TestHighestNodeWins(t *testing.T) {
	t.Parallel()
	bus := newTestBus()

	mA := Member{NodeID: "node-a", Addr: "node-a:0"}
	mB := Member{NodeID: "node-b", Addr: "node-b:0"}
	mC := Member{NodeID: "node-c", Addr: "node-c:0"}

	eA, trA := makeElection(bus, mA, []Member{mB, mC})
	eB, trB := makeElection(bus, mB, []Member{mA, mC})
	eC, trC := makeElection(bus, mC, []Member{mA, mB})

	defer trA.Close()
	defer trB.Close()
	defer trC.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, pair := range []struct {
		tr Transporter
		e  *Election
	}{
		{trA, eA}, {trB, eB}, {trC, eC},
	} {
		pumpMessages(ctx, pair.tr, pair.e)
		pair.e.Start(ctx)
	}

	const timeout = 3 * time.Second

	if !waitForLeader(eA, "node-c", timeout) {
		t.Errorf("node-a: expected leader node-c, got %v (role=%v)", func() string {
			l, ok := eA.Leader()
			if !ok {
				return "<none>"
			}
			return l.NodeID
		}(), eA.Role())
	}
	if !waitForLeader(eB, "node-c", timeout) {
		t.Errorf("node-b: expected leader node-c, got %v (role=%v)", func() string {
			l, ok := eB.Leader()
			if !ok {
				return "<none>"
			}
			return l.NodeID
		}(), eB.Role())
	}
	if eC.Role() != RoleLeader {
		t.Errorf("node-c: expected RoleLeader, got %v", eC.Role())
	}
}

// TestLeaderFailover verifies that when the leader (node-c) stops, node-b
// becomes the new leader within 3 seconds.
func TestLeaderFailover(t *testing.T) {
	t.Parallel()
	bus := newTestBus()

	mA := Member{NodeID: "node-a", Addr: "node-a:1"}
	mB := Member{NodeID: "node-b", Addr: "node-b:1"}
	mC := Member{NodeID: "node-c", Addr: "node-c:1"}

	eA, trA := makeElection(bus, mA, []Member{mB, mC})
	eB, trB := makeElection(bus, mB, []Member{mA, mC})
	eC, trC := makeElection(bus, mC, []Member{mA, mB})

	defer trA.Close()
	defer trB.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, pair := range []struct {
		tr Transporter
		e  *Election
	}{
		{trA, eA}, {trB, eB}, {trC, eC},
	} {
		pumpMessages(ctx, pair.tr, pair.e)
		pair.e.Start(ctx)
	}

	// Wait for node-c to become leader.
	if !waitForRole(eC, RoleLeader, 3*time.Second) {
		t.Fatalf("node-c did not become leader before failover; role=%v", eC.Role())
	}

	// Stop node-c's election loop and remove it from the bus.
	eC.Stop()
	trC.Close()

	// Within 3 seconds, node-b should become the new leader.
	if !waitForLeader(eB, "node-b", 3*time.Second) {
		l, ok := eB.Leader()
		leaderStr := "<none>"
		if ok {
			leaderStr = l.NodeID
		}
		t.Errorf("node-b: expected new leader node-b, got %s (role=%v)", leaderStr, eB.Role())
	}
	if eB.Role() != RoleLeader {
		t.Errorf("node-b role = %v, want RoleLeader", eB.Role())
	}
}

// TestSingleNodeClusterBecomesLeaderImmediately verifies that a single-node
// cluster elects itself leader within 1 second of Start (regression test for
// the bug where single-node clusters never self-elected as leader).
func TestSingleNodeClusterBecomesLeaderImmediately(t *testing.T) {
	t.Parallel()
	bus := newTestBus()
	self := Member{NodeID: "node-a", Addr: "node-a:0"}
	e, tr := makeElection(bus, self, nil)
	defer tr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pumpMessages(ctx, tr, e)
	e.Start(ctx)

	// With the startup fast path, the node should become leader within ~100ms.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if e.Role() == RoleLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("single node did not become leader within 1s; role=%v term=%d", e.Role(), e.Term())
}
