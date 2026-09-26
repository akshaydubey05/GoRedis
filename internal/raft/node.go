package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Node is one Raft server. Every field below is protected by mu —
// any function with the comment "lock held" assumes the caller
// already holds it; anything that acquires mu itself says so.
type Node struct {
	mu      sync.Mutex
	id      int
	peers   []int // IDs of the OTHER nodes in the cluster (not including self)
	tr      Transport
	storage Storage
	applyCh chan ApplyMsg

	// --- persistent state (must survive a restart; see storage.go) ---
	currentTerm int
	votedFor    int        // -1 means "haven't voted this term"
	log         []LogEntry // log[0] is a dummy sentinel; real entries start at index 1

	// --- volatile state (rebuilt on restart, doesn't need saving) ---
	role        Role
	commitIndex int
	lastApplied int
	leaderID    int // who we currently believe the leader is, -1 if unknown
	electionAt  time.Time

	// --- leader-only volatile state (reset every time we become leader) ---
	nextIndex  map[int]int
	matchIndex map[int]int

	applyCond *sync.Cond
	dead      bool

	// OnEvent, if set, is called (non-blocking) whenever something
	// interesting happens — used later by the dashboard. Safe to
	// leave nil for now.
	OnEvent func(Event)
}

// Event is a notification for observers (tests, dashboards) about
// something that just happened inside this node.
type Event struct {
	At     time.Time
	Node   int
	Kind   string // "election", "leader", "vote", "append", "commit"
	Detail string
}

func NewNode(id int, peers []int, tr Transport, storage Storage, applyCh chan ApplyMsg) *Node {
	n := &Node{
		id:       id,
		peers:    peers,
		tr:       tr,
		storage:  storage,
		applyCh:  applyCh,
		votedFor: -1,
		leaderID: -1,
		log:      []LogEntry{{Term: 0}}, // dummy entry at index 0
	}
	n.applyCond = sync.NewCond(&n.mu)
	n.restore()
	n.resetElectionTimer()

	go n.ticker()
	go n.applier()

	return n
}

// persist saves currentTerm, votedFor, and log to disk. Call this
// EVERY time any of those three change, before releasing the lock
// or replying to any RPC — see the guide's note on why votedFor in
// particular must never be lost.
func (n *Node) persist() { // lock held
	data := encodeState(n.currentTerm, n.votedFor, n.log)
	if err := n.storage.Save(data); err != nil {
		panic("raft: persist failed: " + err.Error())
	}
}

func (n *Node) restore() {
	data, err := n.storage.Load()
	if err != nil {
		panic("raft: failed to load persisted state: " + err.Error())
	}
	if len(data) == 0 {
		return // fresh node, nothing saved yet
	}
	term, votedFor, log, err := decodeState(data)
	if err != nil {
		panic("raft: failed to decode persisted state: " + err.Error())
	}
	n.currentTerm, n.votedFor, n.log = term, votedFor, log
}

// lastLog returns the index and term of the last entry in our log.
func (n *Node) lastLog() (index, term int) { // lock held
	i := len(n.log) - 1
	return i, n.log[i].Term
}

// resetElectionTimer picks a new random deadline. Called whenever we
// hear from a legitimate leader, grant a vote, or start an election
// ourselves.
func (n *Node) resetElectionTimer() { // lock held
	span := electionTimeoutMax - electionTimeoutMin
	d := electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
	n.electionAt = time.Now().Add(d)
}

// stepDown makes us a follower. If term is strictly newer than ours,
// we also forget who we voted for — a new term means a clean slate.
// If term equals our current term, we do NOT reset votedFor, or we
// could end up voting twice in the same term (a safety violation).
func (n *Node) stepDown(term int) { // lock held
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = -1
		n.leaderID = -1
	}
	n.role = Follower
	n.persist()
}

func (n *Node) emit(kind, detail string) { // lock held
	if n.OnEvent != nil {
		ev := Event{At: time.Now(), Node: n.id, Kind: kind, Detail: detail}
		// Never block the Raft goroutine on a slow observer.
		go n.OnEvent(ev)
	}
}

// ticker wakes up frequently to check whether it's time to start an
// election. This is deliberately simple (poll every 10ms) rather
// than using timers/channels, which keeps the locking straightforward.
func (n *Node) ticker() {
	for {
		time.Sleep(10 * time.Millisecond)

		n.mu.Lock()
		if n.dead {
			n.mu.Unlock()
			return
		}
		if n.role != Leader && time.Now().After(n.electionAt) {
			n.startElection()
		}
		n.mu.Unlock()
	}
}

// State is a small read-only snapshot, safe to call from tests or
// (later) the dashboard.
func (n *Node) State() (term int, role Role, leaderID int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.role, n.leaderID
}

// Status gives a fuller snapshot, used by the dashboard in Phase 6.
type Status struct {
	ID          int
	Term        int
	Role        string
	CommitIndex int
	LastApplied int
	LogLen      int
	LeaderID    int
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.id,
		Term:        n.currentTerm,
		Role:        n.role.String(),
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LogLen:      len(n.log) - 1, // exclude the dummy sentinel
		LeaderID:    n.leaderID,
	}
}

// Kill stops all of this node's background goroutines. Call it when
// a test or the real process is shutting this node down.
func (n *Node) Kill() {
	n.mu.Lock()
	n.dead = true
	n.applyCond.Broadcast() // wake the applier so it can see dead=true and exit
	n.mu.Unlock()
}