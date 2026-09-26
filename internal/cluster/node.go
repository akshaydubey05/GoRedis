// Package cluster wires a raft.Node together with an fsm.FSM,
// turning "propose a command" into a blocking call that returns only
// once the command has actually been committed and applied.
package cluster

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/fsm"
	"github.com/akshaydubey05/GoRedis/internal/raft"
)

// waiter is what a client's goroutine is blocked on while its
// command makes its way through Raft.
type waiter struct {
	term int // the term this entry was proposed in, for stale-result detection
	ch   chan fsm.Reply
}

// Node is one cluster member: a Raft node plus the state machine it
// drives, plus the bookkeeping to match committed entries back to
// the client that proposed them.
type Node struct {
	raft *raft.Node
	fsm  *fsm.FSM

	// clientAddrs maps every node ID to the address CLIENTS should
	// connect to (not the Raft-to-Raft address) — used for MOVED
	// redirects when a client hits a non-leader node.
	clientAddrs map[int]string

	mu      sync.Mutex
	pending map[int]waiter // log index -> the goroutine waiting on it
}

// New wires everything together and starts the background loop that
// drains committed entries off applyCh. applyCh must be the same
// channel passed to raft.NewNode when this node's raft.Node was
// created.
func New(rn *raft.Node, f *fsm.FSM, applyCh chan raft.ApplyMsg, clientAddrs map[int]string) *Node {
	n := &Node{
		raft:        rn,
		fsm:         f,
		clientAddrs: clientAddrs,
		pending:     map[int]waiter{},
	}
	go n.applyLoop(applyCh)
	return n
}

// applyLoop runs forever, executing every committed entry against
// the FSM — on EVERY node, not just the leader, since that's what
// makes the data replicated. If some other client (or a previous
// leader) is waiting on this exact index, it gets woken with the
// result; otherwise the result is simply discarded (this node wasn't
// the one that originally proposed it).
func (n *Node) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.Command == nil {
			continue // no-op entry from a leader election; nothing to run
		}

		cmd, err := fsm.Decode(msg.Command)
		if err != nil {
			continue // corrupt entry; shouldn't happen, but don't crash the loop
		}

		reply := n.fsm.Apply(cmd) // executed on every node

		n.mu.Lock()
		w, ok := n.pending[msg.Index]
		if ok {
			delete(n.pending, msg.Index)
		}
		n.mu.Unlock()

		if !ok {
			continue // nobody on this node is waiting for this index
		}

		if w.term == msg.Term {
			w.ch <- reply
		} else {
			// A different leader's entry ended up at this index than
			// the one our waiter originally proposed — the original
			// command was almost certainly overwritten before being
			// committed. Tell the client to just retry.
			w.ch <- fsm.Reply{Kind: "err", Str: "TRYAGAIN leadership changed, please retry"}
		}
	}
}

// Do proposes args as a command and blocks until it's committed and
// applied, returning the result. If this node isn't the leader, it
// returns a reply telling the caller where the real leader is.
func (n *Node) Do(args []string) fsm.Reply {
	cmd := fsm.Command{Args: args, TS: time.Now().UnixMilli()}
	data := fsm.Encode(cmd)

	// Lock is held across Propose AND registering the waiter. This
	// closes a real race: without it, a single-node cluster (or a
	// very fast commit) could apply the entry and run applyLoop
	// BEFORE we've registered anyone to receive the result, and the
	// client would hang until timeout for no reason.
	n.mu.Lock()
	index, term, isLeader := n.raft.Propose(data)
	if !isLeader {
		n.mu.Unlock()
		return n.notLeaderReply()
	}
	ch := make(chan fsm.Reply, 1)
	n.pending[index] = waiter{term: term, ch: ch}
	n.mu.Unlock()

	select {
	case reply := <-ch:
		return reply
	case <-time.After(3 * time.Second):
		n.mu.Lock()
		delete(n.pending, index)
		n.mu.Unlock()
		return fsm.Reply{Kind: "err", Str: "TIMEOUT command was not committed in time"}
	}
}

// notLeaderReply tells the client where to find the real leader,
// using the same MOVED convention real Redis Cluster uses — so
// `redis-cli -c` (cluster mode) follows it automatically.
func (n *Node) notLeaderReply() fsm.Reply {
	_, _, leaderID := n.raft.State()
	if leaderID < 0 {
		return fsm.Reply{Kind: "err", Str: "CLUSTERDOWN no leader elected yet, please retry"}
	}
	addr, ok := n.clientAddrs[leaderID]
	if !ok {
		return fsm.Reply{Kind: "err", Str: "CLUSTERDOWN leader unknown, please retry"}
	}
	return fsm.Reply{Kind: "err", Str: "MOVED 0 " + addr}
}

// Status exposes the underlying raft.Node's status, for the gateway
// (Phase 6) to report cluster state.
func (n *Node) Status() raft.Status {
	return n.raft.Status()
}

var _ = json.Marshal // silence unused import if you trim this file later