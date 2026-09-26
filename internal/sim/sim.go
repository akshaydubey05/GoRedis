// Package sim runs a complete Raft+GoRedis cluster inside a single
// process, connected over simnet instead of real TCP. This is the
// same Raft code used in production — just wired to a fake network
// so the whole cluster fits on a free web host with no separate
// processes needed.
package sim

import (
	"fmt"
	"sync"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/cluster"
	"github.com/akshaydubey05/GoRedis/internal/fsm"
	"github.com/akshaydubey05/GoRedis/internal/raft"
	"github.com/akshaydubey05/GoRedis/internal/simnet"
	"github.com/akshaydubey05/GoRedis/internal/store"
)

type LogView struct {
	Term      int    `json:"term"`
	Cmd       string `json:"cmd"`
	Committed bool   `json:"committed"`
}

type NodeView struct {
	ID          int       `json:"id"`
	Alive       bool      `json:"alive"`
	Role        string    `json:"role"`
	Term        int       `json:"term"`
	CommitIndex int       `json:"commitIndex"`
	LogLen      int       `json:"logLen"`
	Log         []LogView `json:"log"`
}

type State struct {
	Nodes    []NodeView `json:"nodes"`
	LeaderID int        `json:"leaderId"`
}

type Sim struct {
	mu      sync.Mutex
	size    int
	net     *simnet.Network
	rafts   []*raft.Node
	cnodes  []*cluster.Node
	storage []*raft.MemStorage
}

func New(size int) *Sim {
	s := &Sim{size: size}
	s.rebuild()
	return s
}

// rebuild tears down (if needed) and creates a completely fresh
// cluster — used both at startup and by Reset().
func (s *Sim) rebuild() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.rafts != nil {
		for _, n := range s.rafts {
			n.Kill()
		}
	}

	s.net = simnet.New()
	s.rafts = make([]*raft.Node, s.size)
	s.cnodes = make([]*cluster.Node, s.size)
	s.storage = make([]*raft.MemStorage, s.size)

	clientAddrs := map[int]string{} // not used for real redirects in sim mode
	for i := 0; i < s.size; i++ {
		s.startLocked(i, clientAddrs)
	}
}

func (s *Sim) startLocked(i int, clientAddrs map[int]string) {
	var peers []int
	for j := 0; j < s.size; j++ {
		if j != i {
			peers = append(peers, j)
		}
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	st := &raft.MemStorage{}
	s.storage[i] = st

	rn := raft.NewNode(i, peers, s.net.Endpoint(i), st, applyCh)
	s.rafts[i] = rn
	s.net.Register(i, rn)
	s.net.SetDown(i, false)

	db := store.New()
	machine := fsm.New(db)
	s.cnodes[i] = cluster.New(rn, machine, applyCh, clientAddrs)
}

// Do finds the current leader and runs a command against it,
// returning the reply and which node handled it (-1 if no leader
// was available).
func (s *Sim) Do(args []string) (fsm.Reply, int) {
	s.mu.Lock()
	var leaderIdx = -1
	for i, n := range s.rafts {
		if s.net.IsDown(i) {
			continue
		}
		if _, role, _ := n.State(); role == raft.Leader {
			leaderIdx = i
			break
		}
	}
	cn := s.cnodes
	s.mu.Unlock()

	if leaderIdx == -1 {
		return fsm.Reply{Kind: "err", Str: "CLUSTERDOWN no leader elected yet, please retry"}, -1
	}
	return cn[leaderIdx].Do(args), leaderIdx
}

func (s *Sim) Kill(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id < 0 || id >= s.size {
		return
	}
	s.net.SetDown(id, true)
}

func (s *Sim) Restart(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id < 0 || id >= s.size {
		return
	}
	// "Restart" in sim mode: keep the same MemStorage (so it still
	// remembers its term/vote/log, just like a real disk would),
	// but spin up a fresh Node instance reading from it.
	s.rafts[id].Kill()
	rn := raft.NewNode(id, peersExcept(s.size, id), s.net.Endpoint(id), s.storage[id], make(chan raft.ApplyMsg, 256))
	s.rafts[id] = rn
	s.net.Register(id, rn)
	s.net.SetDown(id, false)

	db := store.New() // NOTE: sim mode doesn't replay old state into a fresh store;
	// the log will still replay through applyCh from Raft's own persisted log.
	machine := fsm.New(db)
	applyCh := make(chan raft.ApplyMsg, 256)
	rn2 := raft.NewNode(id, peersExcept(s.size, id), s.net.Endpoint(id), s.storage[id], applyCh)
	s.rafts[id] = rn2
	s.net.Register(id, rn2)
	s.cnodes[id] = cluster.New(rn2, machine, applyCh, map[int]string{})
}

func peersExcept(size, id int) []int {
	var peers []int
	for j := 0; j < size; j++ {
		if j != id {
			peers = append(peers, j)
		}
	}
	return peers
}

// Partition cuts every pair of nodes that fall into different
// groups, leaving nodes within the same group able to talk.
func (s *Sim) Partition(groups [][]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groupOf := map[int]int{}
	for gi, g := range groups {
		for _, id := range g {
			groupOf[id] = gi
		}
	}
	for a := 0; a < s.size; a++ {
		for b := a + 1; b < s.size; b++ {
			if groupOf[a] != groupOf[b] {
				s.net.Cut(a, b)
			}
		}
	}
}

func (s *Sim) Heal() {
	s.net.HealAll()
}

func (s *Sim) Reset() {
	s.rebuild()
}

func (s *Sim) Status() State {
	s.mu.Lock()
	defer s.mu.Unlock()

	var leaderID = -1
	nodes := make([]NodeView, s.size)
	for i := 0; i < s.size; i++ {
		alive := !s.net.IsDown(i)
		st := s.rafts[i].Status()
		if alive && st.Role == "leader" {
			leaderID = i
		}
		nodes[i] = NodeView{
			ID:          i,
			Alive:       alive,
			Role:        st.Role,
			Term:        st.Term,
			CommitIndex: st.CommitIndex,
			LogLen:      st.LogLen,
			Log:         []LogView{}, // kept simple for now; extend later if you want per-entry detail
		}
	}
	return State{Nodes: nodes, LeaderID: leaderID}
}

var _ = fmt.Sprintf // silence unused import if trimmed later
var _ = time.Now