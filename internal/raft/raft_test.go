package raft_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/raft"
	"github.com/akshaydubey05/GoRedis/internal/simnet"
)

// cluster wraps a set of Raft nodes wired together over a simnet
// Network, and records what each node has applied so tests can check
// they all agree.
type cluster struct {
	t   *testing.T
	net *simnet.Network

	mu      sync.Mutex
	nodes   []*raft.Node
	stores  []*raft.MemStorage
	applied []map[int]string // per node: log index -> command string
}

func newCluster(t *testing.T, size int) *cluster {
	c := &cluster{
		t:       t,
		net:     simnet.New(),
		nodes:   make([]*raft.Node, size),
		stores:  make([]*raft.MemStorage, size),
		applied: make([]map[int]string, size),
	}
	for i := 0; i < size; i++ {
		c.stores[i] = &raft.MemStorage{}
		c.applied[i] = map[int]string{}
		c.start(i, size)
	}
	return c
}

func (c *cluster) start(i, size int) {
	var peers []int
	for j := 0; j < size; j++ {
		if j != i {
			peers = append(peers, j)
		}
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	n := raft.NewNode(i, peers, c.net.Endpoint(i), c.stores[i], applyCh)

	c.mu.Lock()
	c.nodes[i] = n
	c.mu.Unlock()

	c.net.Register(i, n)
	c.net.SetDown(i, false)

	go func() {
		for msg := range applyCh {
			if msg.Command == nil {
				continue // skip no-op entries from leader elections
			}
			c.mu.Lock()
			c.applied[i][msg.Index] = string(msg.Command)
			c.mu.Unlock()
		}
	}()
}

func (c *cluster) crash(i int) {
	c.net.SetDown(i, true)
	c.nodes[i].Kill()
}

func (c *cluster) restart(i int) {
	c.start(i, len(c.nodes)) // same storage = picks up its old persisted state
}

func (c *cluster) shutdown() {
	for _, n := range c.nodes {
		n.Kill()
	}
}

// waitForLeader polls until exactly one live node reports itself as
// leader, and returns its ID. Fails the test after ~5 seconds.
func (c *cluster) waitForLeader() int {
	for attempt := 0; attempt < 100; attempt++ {
		time.Sleep(50 * time.Millisecond)

		var leaders []int
		for i, n := range c.nodes {
			if c.net.IsDown(i) {
				continue
			}
			if _, role, _ := n.State(); role == raft.Leader {
				leaders = append(leaders, i)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
	}
	c.t.Fatal("no single leader elected within timeout")
	return -1
}

func (c *cluster) waitApplied(index int, cmd string, wantCount int) {
	for attempt := 0; attempt < 100; attempt++ {
		time.Sleep(50 * time.Millisecond)

		c.mu.Lock()
		count := 0
		for _, m := range c.applied {
			if m[index] == cmd {
				count++
			}
		}
		c.mu.Unlock()

		if count >= wantCount {
			return
		}
	}
	c.t.Fatalf("index %d (%q) not applied on %d nodes within timeout", index, cmd, wantCount)
}

// ---------- the tests ----------

func TestElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	l1 := c.waitForLeader()

	// A healthy leader should stay leader — no reason for it to change.
	time.Sleep(time.Second)
	if l2 := c.waitForLeader(); l2 != l1 {
		t.Fatal("leader changed with no failure")
	}

	// Kill the leader; someone else must take over.
	c.crash(l1)
	if l3 := c.waitForLeader(); l3 == l1 {
		t.Fatal("a dead node is still reporting as leader")
	}
}

func TestReplicate(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.waitForLeader()
	idx, _, ok := c.nodes[leader].Propose([]byte("SET a 1"))
	if !ok {
		t.Fatal("leader refused a Propose call")
	}
	c.waitApplied(idx, "SET a 1", 3) // all three nodes should apply it
}

func TestFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.waitForLeader()
	follower := (leader + 1) % 3

	c.crash(follower)
	idx, _, _ := c.nodes[leader].Propose([]byte("SET b 2"))
	c.waitApplied(idx, "SET b 2", 2) // majority (2 of 3) still commits fine

	c.restart(follower)
	c.waitApplied(idx, "SET b 2", 3) // the restarted node should catch up
}

func TestLeaderCrashKeepsCommittedData(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.waitForLeader()
	idx, _, _ := c.nodes[leader].Propose([]byte("SET k v"))
	c.waitApplied(idx, "SET k v", 3)

	c.crash(leader)
	newLeader := c.waitForLeader()

	idx2, _, ok := c.nodes[newLeader].Propose([]byte("SET k2 v2"))
	if !ok {
		t.Fatal("new leader refused a Propose call")
	}
	c.waitApplied(idx2, "SET k2 v2", 2)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applied[newLeader][idx] != "SET k v" {
		t.Fatal("a previously committed entry was lost after leader crash")
	}
}

func TestPartitionMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 3)
	defer c.shutdown()

	leader := c.waitForLeader()
	others := []int{}
	for i := 0; i < 3; i++ {
		if i != leader {
			others = append(others, i)
		}
	}

	// Cut the leader off from both other nodes: it's now a minority
	// of one, so it should be unable to commit anything new.
	c.net.Cut(leader, others[0])
	c.net.Cut(leader, others[1])

	idx, _, ok := c.nodes[leader].Propose([]byte("SET stuck 1"))
	if !ok {
		t.Fatal("old leader should still accept the propose call locally")
	}

	// Give it a moment; it must NOT be able to commit with no majority.
	time.Sleep(500 * time.Millisecond)
	c.mu.Lock()
	count := 0
	for _, m := range c.applied {
		if m[idx] == "SET stuck 1" {
			count++
		}
	}
	c.mu.Unlock()
	if count > 0 {
		t.Fatal("minority leader managed to commit an entry — this must never happen")
	}

	// The healthy majority (the other two nodes) should elect a new
	// leader among themselves and keep making progress.
	c.net.HealAll()
	newLeader := c.waitForLeader()
	idx2, _, ok := c.nodes[newLeader].Propose([]byte("SET progress 1"))
	if !ok {
		t.Fatal("new leader refused propose after healing")
	}
	c.waitApplied(idx2, "SET progress 1", 3)
}

// ---------- chaos testing helpers ----------

// randomFault does one random destructive thing to the cluster: crash
// a node, restart a dead one, partition the network, or heal it.
func (c *cluster) randomFault(rng *rand.Rand) {
	c.mu.Lock()
	n := len(c.nodes)
	c.mu.Unlock()

	switch rng.Intn(4) {
	case 0:
		i := rng.Intn(n)
		if !c.net.IsDown(i) {
			c.crash(i)
		}
	case 1:
		i := rng.Intn(n)
		if c.net.IsDown(i) {
			c.restart(i)
		}
	case 2:
		a, b := rng.Intn(n), rng.Intn(n)
		if a != b {
			c.net.Cut(a, b)
		}
	case 3:
		c.net.HealAll()
	}
}

// clientOp is one record of "a client tried to do something", used
// both for our own agreement check and for the Porcupine history.
type clientOp struct {
	clientID  int
	value     string
	callTime  time.Time
	returnAt  time.Time // zero means it never returned (timed out)
	committed bool
}

// runClients starts n goroutines proposing unique SET commands
// against whichever node currently looks like the leader, for the
// given duration, recording every attempt.
func (c *cluster) runClients(numClients int, dur time.Duration) []clientOp {
	var mu sync.Mutex
	var ops []clientOp
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup

	for cid := 0; cid < numClients; cid++ {
		wg.Add(1)
		go func(cid int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				i++
				value := fmt.Sprintf("c%d-v%d", cid, i)
				callTime := time.Now()

				// Find some node that currently claims to be leader;
				// if none, just wait a bit and retry.
				c.mu.Lock()
				var leaderNode *raft.Node
				for idx, n := range c.nodes {
					if c.net.IsDown(idx) {
						continue
					}
					if _, role, _ := n.State(); role == raft.Leader {
						leaderNode = n
						break
					}
				}
				c.mu.Unlock()

				if leaderNode == nil {
					time.Sleep(20 * time.Millisecond)
					continue
				}

				_, term, ok := leaderNode.Propose([]byte(value))
				if !ok {
					time.Sleep(20 * time.Millisecond)
					continue
				}

				// Poll briefly to see if it actually got applied
				// anywhere, so we can mark returnAt. This is a test
				// helper, not production code, so simple polling is fine.
				applied := false
				for wait := 0; wait < 40; wait++ {
					time.Sleep(20 * time.Millisecond)
					c.mu.Lock()
					for _, m := range c.applied {
						for _, v := range m {
							if v == value {
								applied = true
							}
						}
					}
					c.mu.Unlock()
					if applied {
						break
					}
				}

				op := clientOp{clientID: cid, value: value, callTime: callTime, committed: applied}
				if applied {
					op.returnAt = time.Now()
				}
				_ = term

				mu.Lock()
				ops = append(ops, op)
				mu.Unlock()

				time.Sleep(10 * time.Millisecond)
			}
		}(cid)
	}
	wg.Wait()
	return ops
}

// checkAgreement verifies that for every log index, every node which
// applied that index applied the SAME command. This is Raft's core
// safety property, checked directly against what actually happened.
func (c *cluster) checkAgreement(t *testing.T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	byIndex := map[int]string{}
	for _, m := range c.applied {
		for idx, cmd := range m {
			if existing, ok := byIndex[idx]; ok {
				if existing != cmd {
					t.Fatalf("SAFETY VIOLATION: index %d applied as %q on one node, %q on another", idx, existing, cmd)
				}
			} else {
				byIndex[idx] = cmd
			}
		}
	}
}

// checkAcked verifies every op we marked "committed" during the
// chaos run is actually present somewhere in the final applied log —
// i.e. nothing we told a client succeeded was secretly lost.
func (c *cluster) checkAcked(t *testing.T, ops []clientOp) {
	c.mu.Lock()
	defer c.mu.Unlock()

	present := map[string]bool{}
	for _, m := range c.applied {
		for _, cmd := range m {
			present[cmd] = true
		}
	}

	for _, op := range ops {
		if op.committed && !present[op.value] {
			t.Fatalf("LOST WRITE: client was told %q committed, but it's missing from every node's applied log", op.value)
		}
	}
}


func TestChaos(t *testing.T) {
	c := newCluster(t, 5) // 5 nodes: tolerates 2 simultaneous failures
	defer c.shutdown()

	rng := rand.New(rand.NewSource(42)) // fixed seed = reproducible failures
	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(300 * time.Millisecond):
				c.randomFault(rng)
			}
		}
	}()

	ops := c.runClients(3, 8*time.Second)
	close(stop)
	wg.Wait()

	// Heal everything and give the cluster time to settle before
	// making any final assertions.
	c.net.HealAll()
	for i := 0; i < 5; i++ {
		if c.net.IsDown(i) {
			c.restart(i)
		}
	}
	c.waitForLeader()
	time.Sleep(2 * time.Second)

	c.checkAgreement(t)
	c.checkAcked(t, ops)
}