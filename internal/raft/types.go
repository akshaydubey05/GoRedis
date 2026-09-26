// Package raft implements the Raft consensus algorithm: leader
// election and log replication across a group of nodes, so they
// behave like one reliable server as long as a majority are alive.
package raft

import "time"

// Role is which of the three states a node is in at any moment.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// LogEntry is one item in a node's replicated log. Command is nil
// for the "no-op" entry a new leader appends right after winning an
// election (this lets it commit older entries quickly — see the
// guide's section on why leaders can't commit old-term entries by
// counting replicas alone).
type LogEntry struct {
	Term    int
	Command []byte
}

// ---------- RPC messages (straight from the Raft paper's Figure 2) ----------

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// ApplyMsg is how a Node tells the outside world "this command is
// now committed; go execute it." The store/state-machine layer
// listens on the channel this gets sent on.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte
}

// Transport hides HOW RequestVote/AppendEntries messages actually
// travel between nodes. In tests, simnet implements this over a fake
// in-memory network that can drop or delay messages on command. In
// production (Phase 4), a real TCP-based implementation will satisfy
// the same interface — Node's code never needs to know the
// difference.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// Timing constants. Election timeouts are randomized within this
// range specifically to avoid split votes (see the guide's Phase 2
// notes on why). Heartbeats fire much faster than the minimum
// election timeout so a healthy leader is never mistaken for dead.
const (
	heartbeatInterval = 50 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
)