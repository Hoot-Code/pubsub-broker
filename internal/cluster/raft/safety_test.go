package raft

import (
	"context"
	"testing"
	"time"
)

func TestPersistentStateSurvivesRestart(t *testing.T) {
	ft := newFakeTransport()
	st := fileStore(t)

	n := NewNode("self", []string{"peer"}, ft, st, nil, nil)
	n.SetPeerAddrs(map[string]string{"peer": "peer"})
	ft.register("self", n)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.Start(ctx)

	n.mu.Lock()
	n.state.CurrentTerm = 5
	n.state.VotedFor = "candidate-x"
	n.state.Log = append(n.state.Log, LogEntry{Term: 5, Index: 1, Command: encodeCommand("test")})
	n.persistLocked()
	n.mu.Unlock()

	n.Stop()
	time.Sleep(100 * time.Millisecond)

	ft2 := newFakeTransport()
	n2 := NewNode("self", []string{"peer"}, ft2, st, nil, nil)
	n2.SetPeerAddrs(map[string]string{"peer": "peer"})
	n2.SetElectionTimeout(5000, 10000)
	ft2.register("self", n2)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	n2.Start(ctx2)
	defer n2.Stop()

	time.Sleep(100 * time.Millisecond)

	args := RequestVoteArgs{
		Term:         5,
		CandidateID:  "candidate-y",
		LastLogIndex: 2,
		LastLogTerm:  3,
	}
	body := EncodeRequestVoteArgs(&args)
	msg := &RaftMessage{
		Type: MsgRaftRequestVote,
		From: "candidate-y",
		Term: 5,
		Body: body,
	}

	n2.OnMessage(msg)
	time.Sleep(100 * time.Millisecond)

	n2.mu.Lock()
	votedFor := n2.state.VotedFor
	n2.mu.Unlock()

	if votedFor == "candidate-y" {
		t.Fatal("node changed vote after restart — election safety violated!")
	}
	if votedFor != "candidate-x" {
		t.Fatalf("node voted for %q, want candidate-x", votedFor)
	}
}
