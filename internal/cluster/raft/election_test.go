package raft

import (
	"context"
	"testing"
	"time"
)

func TestSingleNodeElectsItself(t *testing.T) {
	ft := newFakeTransport()
	st := &nopStore{}
	n := NewNode("self", nil, ft, st, nil, nil)
	n.SetPeerAddrs(map[string]string{})
	ft.register("self", n)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.Start(ctx)
	defer n.Stop()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("single node did not become leader within timeout")
}

func TestThreeNodeElection(t *testing.T) {
	nodes, _, _ := startTestCluster(t, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 2*time.Second)
	time.Sleep(200 * time.Millisecond)

	var leaderCount int
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, got %d", leaderCount)
	}
	if !leader.IsLeader() {
		t.Fatal("selected leader is no longer leader")
	}
}

func TestLeaderFailureTriggersReElection(t *testing.T) {
	nodes, _, _ := startTestCluster(t, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		n.Start(ctx)
	}

	leader := waitForLeader(t, nodes, 2*time.Second)
	t.Logf("initial leader: %s", leader.selfID)

	leader.Stop()
	time.Sleep(200 * time.Millisecond)

	remaining := make([]*Node, 0, 2)
	for _, n := range nodes {
		if n != leader {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 3*time.Second)
	if newLeader == nil {
		t.Fatal("no new leader elected after killing old leader")
	}
	t.Logf("new leader: %s", newLeader.selfID)

	_, err := newLeader.Propose(ctx, encodeCommand("after-failover"))
	if err != nil {
		t.Fatalf("propose on new leader: %v", err)
	}
}

func TestElectionRestrictionDeniesStaleCandidate(t *testing.T) {
	nodes, _, _ := startTestCluster(t, 3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 2*time.Second)

	_, err := leader.Propose(ctx, encodeCommand("advance"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	leader.mu.Lock()
	leaderTerm := leader.state.CurrentTerm
	leader.mu.Unlock()

	args := RequestVoteArgs{
		Term:         leaderTerm + 1,
		CandidateID:  "stale-candidate",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	body := EncodeRequestVoteArgs(&args)
	msg := &RaftMessage{
		Type: MsgRaftRequestVote,
		From: "stale-candidate",
		Term: args.Term,
		Body: body,
	}

	follower.OnMessage(msg)
	time.Sleep(100 * time.Millisecond)

	follower.mu.Lock()
	votedFor := follower.state.VotedFor
	follower.mu.Unlock()

	if votedFor == "stale-candidate" {
		t.Fatal("follower granted vote to stale candidate")
	}
}

func TestNoSplitVoteSplitBrain(t *testing.T) {
	ft := newFakeTransport()
	ids := []string{"A", "B", "C", "D", "E"}
	var nodes []*Node

	for _, id := range ids {
		var peers []string
		for _, pid := range ids {
			if pid != id {
				peers = append(peers, pid)
			}
		}
		st := &nopStore{}
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

	ab := []string{"A", "B"}
	cde := []string{"C", "D", "E"}
	for _, from := range ab {
		for _, to := range cde {
			ft.setDrop(from, to, true)
			ft.setDrop(to, from, true)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, n := range nodes {
		n.Start(ctx)
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	time.Sleep(5 * time.Second)

	var abLeaders, cdeLeaders int
	for _, n := range nodes {
		if n.IsLeader() {
			switch n.selfID {
			case "A", "B":
				abLeaders++
			case "C", "D", "E":
				cdeLeaders++
			}
		}
	}

	if abLeaders > 0 {
		t.Fatalf("minority side {A,B} elected %d leader(s) — split brain!", abLeaders)
	}
	if cdeLeaders != 1 {
		t.Fatalf("majority side {C,D,E} has %d leaders, want exactly 1", cdeLeaders)
	}
	for _, n := range nodes {
		if n.IsLeader() {
			t.Logf("leader: %s (term %d)", n.selfID, n.Term())
		}
	}
}

func TestSingleNodeClusterBecomesLeaderImmediately(t *testing.T) {
	ft := newFakeTransport()
	st := &nopStore{}
	n := NewNode("self", nil, ft, st, nil, nil)
	n.SetPeerAddrs(map[string]string{})
	ft.register("self", n)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n.Start(ctx)
	defer n.Stop()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("single node did not become leader within 1s")
}
