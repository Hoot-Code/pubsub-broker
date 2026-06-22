package raft

// ─── Log helpers ────────────────────────────────────────────────────────────

func (n *Node) lastLogIndexAndTermLocked() (uint64, uint64) {
	if len(n.state.Log) == 0 {
		return 0, 0
	}
	last := n.state.Log[len(n.state.Log)-1]
	return last.Index, last.Term
}

func (n *Node) appendEntriesFromLeader(prevIndex, prevTerm uint64, entries []LogEntry) (success bool, conflictIndex, conflictTerm uint64) {
	if prevIndex > uint64(len(n.state.Log)) {
		return false, uint64(len(n.state.Log)) + 1, 0
	}

	if prevIndex > 0 {
		entry := n.state.Log[prevIndex-1]
		if entry.Term != prevTerm {
			ct := entry.Term
			ci := prevIndex
			for ci > 1 && n.state.Log[ci-2].Term == ct {
				ci--
			}
			return false, ci, ct
		}
	}

	start := prevIndex + 1
	for i, newEntry := range entries {
		idx := start + uint64(i)
		if idx > uint64(len(n.state.Log)) {
			n.state.Log = append(n.state.Log, entries[i:]...)
			break
		}
		existing := n.state.Log[idx-1]
		if existing.Term != newEntry.Term {
			n.state.Log = n.state.Log[:idx-1]
			n.state.Log = append(n.state.Log, entries[i:]...)
			break
		}
	}

	n.persistLocked()
	return true, 0, 0
}

// TruncateLogFrom removes all log entries from index onward (1-based).
func (n *Node) TruncateLogFrom(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > 0 && index <= uint64(len(n.state.Log)) {
		n.state.Log = n.state.Log[:index-1]
		n.persistLocked()
	}
}

// truncateLogFrom removes all log entries from index onward (1-based).
func (n *Node) truncateLogFrom(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index > 0 && index <= uint64(len(n.state.Log)) {
		n.state.Log = n.state.Log[:index-1]
		n.persistLocked()
	}
}
