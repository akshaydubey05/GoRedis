package raft

import "time"

// leaderLoop sends heartbeats/AppendEntries to every peer on a fixed
// interval for as long as we remain leader in this specific term.
func (n *Node) leaderLoop(term int) {
	for {
		n.mu.Lock()
		if n.dead || n.role != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return // no longer leader in this term; stop looping
		}
		n.broadcastAppend()
		n.mu.Unlock()

		time.Sleep(heartbeatInterval)
	}
}

func (n *Node) broadcastAppend() { // lock held
	for _, p := range n.peers {
		go n.replicateTo(p)
	}
}

// replicateTo sends one AppendEntries to a single peer, containing
// whatever entries that peer is missing (possibly none, i.e. a plain
// heartbeat).
func (n *Node) replicateTo(peerID int) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	next := n.nextIndex[peerID]
	prevIdx := next - 1
	if prevIdx < 0 || prevIdx >= len(n.log) {
		n.mu.Unlock()
		return // shouldn't happen in normal operation; be defensive
	}

	// Copy the entries slice — we're about to unlock, and the
	// leader's log could change underneath us while the network call
	// is in flight. Sharing memory across that boundary is a classic
	// data race that -race will catch.
	entries := append([]LogEntry(nil), n.log[next:]...)

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  n.log[prevIdx].Term,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	term := n.currentTerm
	n.mu.Unlock() // NEVER hold the lock across a network call

	reply, err := n.tr.AppendEntries(peerID, args)
	if err != nil {
		return // peer unreachable; we'll just retry on the next heartbeat
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	// Stale reply protection, same idea as in election.go.
	if n.role != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		matchIdx := args.PrevLogIndex + len(args.Entries)
		if matchIdx > n.matchIndex[peerID] {
			n.matchIndex[peerID] = matchIdx
		}
		n.nextIndex[peerID] = n.matchIndex[peerID] + 1
		n.advanceCommit()
	} else if n.nextIndex[peerID] > 1 {
		// The follower's log didn't match at prevIdx; step back by
		// one and we'll retry with an earlier entry next heartbeat.
		// (A production optimization: the follower could tell us how
		// far back to jump instead of one at a time — see the guide's
		// "optional optimization" note for later.)
		n.nextIndex[peerID]--
	}
}

// advanceCommit looks for the highest log index that's now stored on
// a majority of nodes (including ourselves) — but ONLY counts
// entries from our OWN current term. This restriction is subtle but
// essential: an older-term entry sitting on a majority can still be
// silently overwritten by a future leader that lacks it (see the
// Raft paper's Figure 8 / the guide's explanation). Once we commit a
// current-term entry, log matching guarantees all earlier entries
// are safe too.
func (n *Node) advanceCommit() { // lock held
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			break
		}
		count := 1 // ourselves
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 {
			n.commitIndex = idx
			n.emit("commit", "advanced commit index")
			n.applyCond.Broadcast()
			break
		}
	}
}

// HandleAppendEntries is called (via Transport) when we receive an
// AppendEntries RPC — either a heartbeat or real entries to append.
// This implements the AppendEntries RPC receiver logic from the
// Raft paper's Figure 2.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		// This message is from an old, stale leader.
		return &AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	// A valid leader for a term at least as new as ours: we are
	// definitely a follower, and we just heard from our leader.
	n.stepDown(args.Term)
	n.leaderID = args.LeaderID
	n.resetElectionTimer()

	// Consistency check: do we have an entry at PrevLogIndex whose
	// term matches what the leader expects there?
	if args.PrevLogIndex >= len(n.log) || n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return &AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	// Append the new entries, but only after resolving any conflicts:
	// if we already have a DIFFERENT entry at some position, the
	// leader's version wins and everything from that point on gets
	// discarded. If our existing entry already matches, we leave it
	// alone (this matters so we don't destroy already-committed data
	// due to a duplicate/delayed message).
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(n.log) {
			if n.log[idx].Term == e.Term {
				continue // already have this exact entry; nothing to do
			}
			n.log = n.log[:idx] // real conflict: truncate from here
		}
		n.log = append(n.log, args.Entries[i:]...)
		break
	}
	n.persist()

	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if lastNew := args.PrevLogIndex + len(args.Entries); lastNew < newCommit {
			newCommit = lastNew
		}
		n.commitIndex = newCommit
		n.applyCond.Broadcast()
	}

	return &AppendEntriesReply{Term: n.currentTerm, Success: true}
}