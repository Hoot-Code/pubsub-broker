package raft

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeTransport struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	drop    map[string]map[string]bool
	dropAll bool
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		nodes: make(map[string]*Node),
		drop:  make(map[string]map[string]bool),
	}
}

func (ft *fakeTransport) register(id string, n *Node) {
	ft.mu.Lock()
	ft.nodes[id] = n
	ft.mu.Unlock()
}

func (ft *fakeTransport) SendRaft(addr string, msg *RaftMessage) error {
	ft.mu.Lock()
	if ft.dropAll {
		ft.mu.Unlock()
		return nil
	}
	if drops, ok := ft.drop[msg.From]; ok {
		if drops[addr] {
			ft.mu.Unlock()
			return nil
		}
	}
	target, ok := ft.nodes[addr]
	ft.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown node: %s", addr)
	}
	copy := *msg
	select {
	case target.inCh <- &copy:
	default:
		go func() {
			target.inCh <- &copy
		}()
	}
	return nil
}

func (ft *fakeTransport) setDrop(from, to string, drop bool) {
	ft.mu.Lock()
	if ft.drop[from] == nil {
		ft.drop[from] = make(map[string]bool)
	}
	ft.drop[from][to] = drop
	ft.mu.Unlock()
}

type nopStore struct {
	mu    sync.Mutex
	state PersistentState
}

func (s *nopStore) Save(state PersistentState) error {
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
	return nil
}

func (s *nopStore) Load() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state, nil
}

func fileStore(t *testing.T) *FilePersistentStore {
	t.Helper()
	return NewFilePersistentStore(fmt.Sprintf("%s/raft-state.json", t.TempDir()))
}

func startTestCluster(t *testing.T, n int) ([]*Node, []*nopStore, *fakeTransport) {
	t.Helper()
	ft := newFakeTransport()
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("node-%d", i)
	}
	var nodes []*Node
	var stores []*nopStore
	for _, id := range ids {
		var peers []string
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		st := &nopStore{}
		stores = append(stores, st)
		nn := NewNode(id, peers, ft, st, nil, nil)
		addrs := make(map[string]string)
		for _, pid := range ids {
			if pid != id {
				addrs[pid] = pid
			}
		}
		nn.SetPeerAddrs(addrs)
		nn.SetElectionTimeout(500, 1000)
		nn.SetHeartbeatInterval(25)
		nodes = append(nodes, nn)
		ft.register(id, nn)
	}
	return nodes, stores, ft
}

func waitForLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.IsLeader() {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func encodeCommand(cmd string) []byte {
	b, _ := json.Marshal(cmd)
	return b
}

func decodeCommand(data []byte) string {
	var s string
	_ = json.Unmarshal(data, &s)
	return s
}
