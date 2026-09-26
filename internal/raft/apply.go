package raft

// applier is a background goroutine that watches commitIndex and
// sends newly-committed entries out on applyCh, in order, one at a
// time. It sleeps (via applyCond.Wait) whenever there's nothing new
// to apply, and wakes up whenever advanceCommit or
// HandleAppendEntries moves commitIndex forward.
func (n *Node) applier() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for {
		for n.lastApplied >= n.commitIndex && !n.dead {
			n.applyCond.Wait() // atomically unlocks while waiting, relocks on wake
		}
		if n.dead {
			return
		}

		n.lastApplied++
		entry := n.log[n.lastApplied]
		msg := ApplyMsg{
			Index:   n.lastApplied,
			Term:    entry.Term,
			Command: entry.Command,
		}

		// Send without holding the lock — the receiver (the future
		// state machine / store layer) might be slow, and we must
		// never let a slow consumer freeze the whole Raft node.
		n.mu.Unlock()
		n.applyCh <- msg
		n.mu.Lock()
	}
}

// Propose asks the cluster to replicate cmd. Only the current leader
// accepts proposals — everyone else returns isLeader=false so the
// caller knows to redirect the client elsewhere.
//
// A true return does NOT mean cmd is committed yet, only that this
// node accepted it as leader and started replicating it. The caller
// must wait for it to actually arrive on applyCh (Phase 4 shows how).
func (n *Node) Propose(cmd []byte) (index, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return 0, 0, false
	}

	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: cmd})
	n.persist()
	n.advanceCommit()   // handles the single-node-cluster instant-commit case
	n.broadcastAppend() // don't wait for the next scheduled heartbeat

	return len(n.log) - 1, n.currentTerm, true
}