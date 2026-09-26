package raft

// startElection turns us into a candidate and asks every peer for a
// vote. This is called from ticker() when our election timer fires
// with no leader heard from.
func (n *Node) startElection() { // lock held
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id // vote for ourselves
	n.leaderID = -1
	n.persist()
	n.resetElectionTimer()
	n.emit("election", "started election")

	term := n.currentTerm
	lastIdx, lastTerm := n.lastLog()
	votes := 1 // our own vote

	if len(n.peers) == 0 {
		// Single-node "cluster": we trivially have a majority.
		n.becomeLeader()
		return
	}

	for _, peerID := range n.peers {
		go func(peerID int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}

			reply, err := n.tr.RequestVote(peerID, args) // NOTE: no lock held during network call
			if err != nil {
				return // peer unreachable; simply don't count its vote
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}
			// Stale reply protection: only count this vote if we're
			// still a candidate in the SAME term we asked about.
			if n.role != Candidate || n.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.becomeLeader()
				}
			}
		}(peerID)
	}
}

// HandleRequestVote is called (via Transport) when another node asks
// us for a vote. This implements the RequestVote RPC receiver logic
// from the Raft paper's Figure 2 exactly.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		// The candidate is behind us; tell it so and refuse.
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		// A newer term means a clean slate: become a follower and
		// forget any vote from an earlier term.
		n.stepDown(args.Term)
	}

	lastIdx, lastTerm := n.lastLog()
	logIsUpToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	canVote := n.votedFor == -1 || n.votedFor == args.CandidateID

	if canVote && logIsUpToDate {
		n.votedFor = args.CandidateID
		n.persist() // MUST save before replying — see guide's note on why
		n.resetElectionTimer() // granting a vote counts as "hearing from a leader-ish node"
		n.emit("vote", "granted vote")
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}

	return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
}

// becomeLeader transitions us into the Leader role. We immediately
// append a no-op entry in our new term — this lets us commit older
// entries quickly once it's replicated (see types.go's comment on
// LogEntry.Command).
func (n *Node) becomeLeader() { // lock held
	n.role = Leader
	n.leaderID = n.id
	n.emit("leader", "became leader")

	lastIdx, _ := n.lastLog()
	n.nextIndex = map[int]int{}
	n.matchIndex = map[int]int{}
	for _, p := range n.peers {
		n.nextIndex[p] = lastIdx + 1
		n.matchIndex[p] = 0
	}

	n.log = append(n.log, LogEntry{Term: n.currentTerm, Command: nil}) // no-op entry
	n.persist()

	go n.leaderLoop(n.currentTerm)
}