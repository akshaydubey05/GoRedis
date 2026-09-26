// Package simnet provides a fake, in-process network for testing
// Raft. It can simulate nodes being down, links being cut (network
// partitions), added latency, and dropped messages — all the chaos
// scenarios Raft needs to be tested against.
package simnet

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/raft"
)

type Network struct {
	mu      sync.Mutex
	nodes   map[int]*raft.Node
	down    map[int]bool
	cut     map[[2]int]bool
	delay   time.Duration
	dropPct float64
}

func New() *Network {
	return &Network{
		nodes: map[int]*raft.Node{},
		down:  map[int]bool{},
		cut:   map[[2]int]bool{},
	}
}

func (nw *Network) Register(id int, n *raft.Node) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.nodes[id] = n
}

func (nw *Network) SetDown(id int, isDown bool) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.down[id] = isDown
}

func (nw *Network) IsDown(id int) bool {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	return nw.down[id]
}

func (nw *Network) SetDelay(d time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.delay = d
}

func (nw *Network) SetDrop(pct float64) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dropPct = pct
}

// Cut blocks messages between a and b in BOTH directions, simulating
// a network partition between exactly those two nodes.
func (nw *Network) Cut(a, b int) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.cut[[2]int{a, b}] = true
	nw.cut[[2]int{b, a}] = true
}

func (nw *Network) HealAll() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.cut = map[[2]int]bool{}
}

func (nw *Network) route(from, to int) (*raft.Node, time.Duration, error) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	if nw.down[from] || nw.down[to] || nw.cut[[2]int{from, to}] {
		return nil, 0, errors.New("simnet: unreachable")
	}
	if nw.dropPct > 0 && rand.Float64() < nw.dropPct {
		return nil, 0, errors.New("simnet: dropped")
	}
	return nw.nodes[to], nw.delay, nil
}

// Endpoint is the raft.Transport implementation a single node uses
// to reach everyone else through this fake network.
type Endpoint struct {
	nw   *Network
	from int
}

func (nw *Network) Endpoint(from int) *Endpoint {
	return &Endpoint{nw: nw, from: from}
}

func (e *Endpoint) RequestVote(to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	n, delay, err := e.nw.route(e.from, to)
	if err != nil {
		return nil, err
	}
	time.Sleep(delay)
	return n.HandleRequestVote(args), nil
}

func (e *Endpoint) AppendEntries(to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	n, delay, err := e.nw.route(e.from, to)
	if err != nil {
		return nil, err
	}
	time.Sleep(delay)
	return n.HandleAppendEntries(args), nil
}