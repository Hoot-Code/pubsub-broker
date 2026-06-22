package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLogReplication(t *testing.T) {
	nodes, _, _ := startTestCluster(t, 3)

	var appliedMu sync.Mutex
	applied := make(map[string][]string)

	for _, n := range nodes {
		id := n.selfID
		n.apply = func(cmd []byte) error {
			appliedMu.Lock()
			applied[id] = append(applied[id], decodeCommand(cmd))
			appliedMu.Unlock()
			return nil
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

	leader := waitForLeader(t, nodes, 2*time.Second)
	_ = leader

	for i := 0; i < 10; i++ {
		cmd := fmt.Sprintf("cmd-%d", i)
		for attempt := 0; attempt < 100; attempt++ {
			var cl *Node
			for _, nd := range nodes {
				if nd.IsLeader() {
					cl = nd
					break
				}
			}
			if cl == nil {
				time.Sleep(20 * time.Millisecond)
				continue
			}
			_, err := cl.Propose(ctx, encodeCommand(cmd))
			if err == nil {
				break
			}
			if attempt == 99 {
				t.Fatalf("propose cmd-%d: %v", i, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		allDone := true
		for _, n := range nodes {
			appliedMu.Lock()
			count := len(applied[n.selfID])
			appliedMu.Unlock()
			if count < 10 {
				allDone = false
				break
			}
		}
		if allDone {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, n := range nodes {
		appliedMu.Lock()
		cmds := applied[n.selfID]
		appliedMu.Unlock()
		if len(cmds) != 10 {
			t.Fatalf("node %s: applied %d commands, want 10", n.selfID, len(cmds))
		}
		for i, cmd := range cmds {
			want := fmt.Sprintf("cmd-%d", i)
			if cmd != want {
				t.Fatalf("node %s: cmd[%d] = %q, want %q", n.selfID, i, cmd, want)
			}
		}
	}
}

func TestLogMatchingRejectsInconsistentAppend(t *testing.T) {
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

	_, err := leader.Propose(ctx, encodeCommand("init"))
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

	wrongArgs := AppendEntriesArgs{
		Term:         leaderTerm,
		LeaderID:     leader.selfID,
		PrevLogIndex: 1,
		PrevLogTerm:  9999,
		Entries:      []LogEntry{{Term: leaderTerm, Index: 2, Command: encodeCommand("bad")}},
		LeaderCommit: 0,
	}
	body := EncodeAppendEntriesArgs(&wrongArgs)
	msg := &RaftMessage{
		Type: MsgRaftAppendEntries,
		From: leader.selfID,
		Term: leaderTerm,
		Body: body,
	}

	follower.OnMessage(msg)
	time.Sleep(100 * time.Millisecond)

	follower.mu.Lock()
	logLen := len(follower.state.Log)
	follower.mu.Unlock()

	if logLen != 1 {
		t.Fatalf("follower log length: want 1, got %d (bad entry was accepted)", logLen)
	}
}

func TestNoCommitAdvanceOnFailedConsistencyCheck(t *testing.T) {
	ft := newFakeTransport()
	ids := []string{"leader", "follower"}
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

	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	_, err := leader.Propose(ctx, encodeCommand("init"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	follower.mu.Lock()
	if len(follower.state.Log) > 0 {
		follower.state.Log[0] = LogEntry{Term: 99, Index: 1, Command: encodeCommand("stale")}
	}
	ciBefore := follower.commitIndex
	follower.mu.Unlock()

	leader.mu.Lock()
	leaderTerm := leader.state.CurrentTerm
	leader.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         leaderTerm,
		LeaderID:     leader.selfID,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: leaderTerm, Index: 2, Command: encodeCommand("bad")}},
		LeaderCommit: 5,
	}
	body := EncodeAppendEntriesArgs(&args)
	msg := &RaftMessage{
		Type: MsgRaftAppendEntries,
		From: leader.selfID,
		Term: leaderTerm,
		Body: body,
	}

	follower.OnMessage(msg)
	time.Sleep(100 * time.Millisecond)

	follower.mu.Lock()
	commitAfter := follower.commitIndex
	follower.mu.Unlock()

	if commitAfter != ciBefore {
		t.Fatalf("follower commitIndex advanced from %d to %d on a failed consistency check",
			ciBefore, commitAfter)
	}
}

func TestCommitAdvancesOnlyAfterSuccessfulAppend(t *testing.T) {
	ft := newFakeTransport()
	st := &nopStore{}
	leader := NewNode("leader", []string{"follower"}, ft, st, nil, nil)
	leader.SetPeerAddrs(map[string]string{"follower": "follower"})
	leader.SetElectionTimeout(50000, 100000)
	ft.register("leader", leader)

	fSt := &nopStore{}
	follower := NewNode("follower", []string{"leader"}, ft, fSt, nil, nil)
	follower.SetPeerAddrs(map[string]string{"leader": "leader"})
	follower.SetElectionTimeout(50000, 100000)
	ft.register("follower", follower)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	follower.Start(ctx)
	defer follower.Stop()

	successEntries := []LogEntry{
		{Term: 1, Index: 1, Command: encodeCommand("e1")},
		{Term: 1, Index: 2, Command: encodeCommand("e2")},
		{Term: 1, Index: 3, Command: encodeCommand("e3")},
	}
	args1 := AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      successEntries,
		LeaderCommit: 3,
	}
	body1 := EncodeAppendEntriesArgs(&args1)
	msg1 := &RaftMessage{
		Type: MsgRaftAppendEntries,
		From: "leader",
		Term: 1,
		Body: body1,
	}
	follower.OnMessage(msg1)
	time.Sleep(100 * time.Millisecond)

	follower.mu.Lock()
	ci := follower.commitIndex
	logLen := uint64(len(follower.state.Log))
	follower.mu.Unlock()

	if ci != 3 {
		t.Fatalf("after successful append: commitIndex = %d, want 3", ci)
	}
	if ci > logLen {
		t.Fatalf("commitIndex %d exceeds log length %d", ci, logLen)
	}

	follower.mu.Lock()
	ciBefore := follower.commitIndex
	follower.mu.Unlock()

	badArgs := AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 1,
		PrevLogTerm:  9999,
		Entries:      []LogEntry{{Term: 1, Index: 2, Command: encodeCommand("bad")}},
		LeaderCommit: 10,
	}
	badBody := EncodeAppendEntriesArgs(&badArgs)
	badMsg := &RaftMessage{
		Type: MsgRaftAppendEntries,
		From: "leader",
		Term: 1,
		Body: badBody,
	}
	follower.OnMessage(badMsg)
	time.Sleep(100 * time.Millisecond)

	follower.mu.Lock()
	ciAfter := follower.commitIndex
	logLen2 := uint64(len(follower.state.Log))
	follower.mu.Unlock()

	if ciAfter != ciBefore {
		t.Fatalf("commitIndex advanced from %d to %d on failed consistency check",
			ciBefore, ciAfter)
	}
	if ciAfter > logLen2 {
		t.Fatalf("commitIndex %d exceeds log length %d after failed check",
			ciAfter, logLen2)
	}

	args3 := AppendEntriesArgs{
		Term:         1,
		LeaderID:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 10,
	}
	body3 := EncodeAppendEntriesArgs(&args3)
	msg3 := &RaftMessage{
		Type: MsgRaftAppendEntries,
		From: "leader",
		Term: 1,
		Body: body3,
	}
	follower.OnMessage(msg3)
	time.Sleep(100 * time.Millisecond)

	follower.mu.Lock()
	ciFinal := follower.commitIndex
	logLenFinal := uint64(len(follower.state.Log))
	follower.mu.Unlock()

	if ciFinal != 3 {
		t.Fatalf("after capped heartbeat: commitIndex = %d, want 3", ciFinal)
	}
	if ciFinal > logLenFinal {
		t.Fatalf("commitIndex %d exceeds log length %d after capped heartbeat",
			ciFinal, logLenFinal)
	}
}
