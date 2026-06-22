package raft

// ─── Election ───────────────────────────────────────────────────────────────

func (n *Node) handleElectionTimeout() {
	n.mu.Lock()
	if n.role == Leader {
		n.mu.Unlock()
		return
	}

	n.state.CurrentTerm++
	n.state.VotedFor = n.selfID
	n.role = Candidate
	n.leaderID = ""
	n.leaderKnown = false
	n.voteGranted = make(map[string]bool)
	n.voteGranted[n.selfID] = true
	term := n.state.CurrentTerm
	n.persistLocked()

	if len(n.peers) == 0 {
		n.becomeLeaderLocked()
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.selfID,
		LastLogIndex: uint64(len(n.state.Log)),
	}
	if len(n.state.Log) > 0 {
		args.LastLogTerm = n.state.Log[len(n.state.Log)-1].Term
	}

	body := EncodeRequestVoteArgs(&args)
	msg := &RaftMessage{
		Type: MsgRaftRequestVote,
		From: n.selfID,
		Term: term,
		Body: body,
	}

	for _, peer := range n.peers {
		addr := n.peerAddr(peer)
		if addr != "" {
			_ = n.transport.SendRaft(addr, msg)
		}
	}
}

func (n *Node) handleRequestVote(msg *RaftMessage) bool {
	var args RequestVoteArgs
	if err := DecodeRequestVoteArgs(msg.Body, &args); err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.state.CurrentTerm {
		n.state.CurrentTerm = args.Term
		n.state.VotedFor = ""
		n.role = Follower
		n.leaderID = ""
		n.leaderKnown = false
		n.persistLocked()
	}

	reply := RequestVoteReply{Term: n.state.CurrentTerm}

	if args.Term >= n.state.CurrentTerm {
		votedFor := n.state.VotedFor
		if votedFor == "" || votedFor == args.CandidateID {
			lastIdx, lastTerm := n.lastLogIndexAndTermLocked()
			logUpToDate := args.LastLogTerm > lastTerm ||
				(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)
			if logUpToDate {
				reply.VoteGranted = true
				n.state.VotedFor = args.CandidateID
				n.persistLocked()
			}
		}
	}

	body := EncodeRequestVoteReply(&reply)
	resp := &RaftMessage{
		Type: MsgRaftVoteResponse,
		From: n.selfID,
		Term: n.state.CurrentTerm,
		Body: body,
	}

	addr := n.peerAddr(args.CandidateID)
	if addr != "" {
		_ = n.transport.SendRaft(addr, resp)
	}

	return reply.VoteGranted || reply.Term > n.state.CurrentTerm
}

func (n *Node) handleVoteResponse(msg *RaftMessage) bool {
	var reply RequestVoteReply
	if err := DecodeRequestVoteReply(msg.Body, &reply); err != nil {
		return false
	}

	n.mu.Lock()

	if reply.Term > n.state.CurrentTerm {
		n.state.CurrentTerm = reply.Term
		n.state.VotedFor = ""
		n.role = Follower
		n.leaderID = ""
		n.leaderKnown = false
		n.persistLocked()
		n.mu.Unlock()
		return true
	}

	if n.role != Candidate || reply.Term != n.state.CurrentTerm {
		n.mu.Unlock()
		return false
	}

	if reply.VoteGranted {
		n.voteGranted[msg.From] = true
		votes := len(n.voteGranted)
		quorum := (len(n.peers)+1)/2 + 1
		if votes >= quorum {
			n.becomeLeaderLocked()
			n.mu.Unlock()
			n.sendAppendEntriesToAll()
			return false
		}
	}

	n.mu.Unlock()
	return false
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.selfID
	n.leaderKnown = true

	lastIdx := uint64(len(n.state.Log))
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}

	n.log.Info("raft: became leader", "id", n.selfID, "term", n.state.CurrentTerm)
}
