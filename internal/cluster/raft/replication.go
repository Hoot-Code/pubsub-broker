package raft

// ─── AppendEntries handling ─────────────────────────────────────────────────

func (n *Node) handleAppendEntries(msg *RaftMessage) bool {
	var args AppendEntriesArgs
	if err := DecodeAppendEntriesArgs(msg.Body, &args); err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.state.CurrentTerm {
		n.state.CurrentTerm = args.Term
		n.state.VotedFor = ""
		n.role = Follower
		n.leaderID = args.LeaderID
		n.leaderKnown = true
		n.persistLocked()
	}

	reply := AppendEntriesReply{Term: n.state.CurrentTerm}

	if args.Term < n.state.CurrentTerm {
		body := EncodeAppendEntriesReply(&reply)
		resp := &RaftMessage{
			Type: MsgRaftAppendResponse,
			From: n.selfID,
			Term: n.state.CurrentTerm,
			Body: body,
		}
		addr := n.peerAddr(args.LeaderID)
		if addr != "" {
			_ = n.transport.SendRaft(addr, resp)
		}
		return false
	}

	n.state.CurrentTerm = args.Term
	n.state.VotedFor = ""
	n.role = Follower
	n.leaderID = args.LeaderID
	n.leaderKnown = true
	n.persistLocked()

	success, conflictIndex, conflictTerm := n.appendEntriesFromLeader(args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	reply.Success = success
	if !success {
		reply.ConflictIndex = conflictIndex
		reply.ConflictTerm = conflictTerm
	}
	if success {
		reply.MatchIndex = uint64(len(n.state.Log))
	}

	if success && args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if uint64(len(n.state.Log)) < newCommit {
			newCommit = uint64(len(n.state.Log))
		}
		n.commitIndex = newCommit
		n.applyCommittedLocked()
	}

	body := EncodeAppendEntriesReply(&reply)
	resp := &RaftMessage{
		Type: MsgRaftAppendResponse,
		From: n.selfID,
		Term: n.state.CurrentTerm,
		Body: body,
	}
	addr := n.peerAddr(args.LeaderID)
	if addr != "" {
		_ = n.transport.SendRaft(addr, resp)
	}

	return true
}

func (n *Node) handleAppendResponse(msg *RaftMessage) bool {
	var reply AppendEntriesReply
	if err := DecodeAppendEntriesReply(msg.Body, &reply); err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.state.CurrentTerm {
		n.state.CurrentTerm = reply.Term
		n.state.VotedFor = ""
		n.role = Follower
		n.leaderID = ""
		n.leaderKnown = false
		n.persistLocked()
		return true
	}

	if n.role != Leader {
		return false
	}

	peer := msg.From
	if reply.Success {
		n.matchIndex[peer] = reply.MatchIndex
		n.nextIndex[peer] = reply.MatchIndex + 1
		n.advanceCommitIndexLocked()
	} else {
		if reply.ConflictTerm > 0 {
			nextIdx := reply.ConflictIndex
			for i := uint64(len(n.state.Log)); i > 0; i-- {
				if n.state.Log[i-1].Term == reply.ConflictTerm {
					nextIdx = i + 1
					break
				}
			}
			n.nextIndex[peer] = nextIdx
		} else {
			n.nextIndex[peer] = reply.ConflictIndex
		}
		if n.nextIndex[peer] < 1 {
			n.nextIndex[peer] = 1
		}
	}

	return false
}

func (n *Node) advanceCommitIndexLocked() {
	if n.role != Leader {
		return
	}
	for idx := uint64(len(n.state.Log)); idx > n.commitIndex; idx-- {
		if n.state.Log[idx-1].Term != n.state.CurrentTerm {
			continue
		}
		replicated := 1
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicated++
			}
		}
		quorum := (len(n.peers)+1)/2 + 1
		if replicated >= quorum {
			n.commitIndex = idx
			n.applyCommittedLocked()
			break
		}
	}
}

func (n *Node) applyCommittedLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.state.Log[n.lastApplied-1]
		if n.apply != nil {
			_ = n.apply(entry.Command)
		}
		n.proposalMu.Lock()
		if ch, ok := n.proposals[n.lastApplied]; ok {
			select {
			case ch <- struct{}{}:
			default:
			}
			delete(n.proposals, n.lastApplied)
		}
		n.proposalMu.Unlock()
	}
}

// ─── AppendEntries sending ──────────────────────────────────────────────────

func (n *Node) sendAppendEntriesToAll() {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	type peerMsg struct {
		addr string
		msg  *RaftMessage
	}
	var msgs []peerMsg
	term := n.state.CurrentTerm
	leaderID := n.selfID
	leaderCommit := n.commitIndex
	logLen := uint64(len(n.state.Log))

	for _, peer := range n.peers {
		nextIdx := n.nextIndex[peer]
		if nextIdx > logLen {
			nextIdx = logLen + 1
			n.nextIndex[peer] = nextIdx
		}
		prevIdx := nextIdx - 1
		var prevTerm uint64
		if prevIdx > 0 && prevIdx <= logLen {
			prevTerm = n.state.Log[prevIdx-1].Term
		}

		var entries []LogEntry
		if nextIdx <= logLen && nextIdx >= 1 {
			entries = make([]LogEntry, len(n.state.Log[nextIdx-1:]))
			copy(entries, n.state.Log[nextIdx-1:])
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     leaderID,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}

		body := EncodeAppendEntriesArgs(&args)
		msg := &RaftMessage{
			Type: MsgRaftAppendEntries,
			From: n.selfID,
			Term: term,
			Body: body,
		}

		addr := n.peerAddr(peer)
		if addr != "" {
			msgs = append(msgs, peerMsg{addr: addr, msg: msg})
		}
	}
	n.mu.Unlock()

	for _, m := range msgs {
		_ = n.transport.SendRaft(m.addr, m.msg)
	}
}
