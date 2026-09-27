package raft_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/akshaydubey05/GoRedis/internal/raft"
)

type regInput struct {
	op    string
	value string
}
type regOutput struct {
	value string
	ok    bool
}

var registerModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(regInput)
		out := output.(regOutput)
		st := state.(string)
		if in.op == "get" {
			return out.value == st, st
		}
		return true, in.value
	},
}

func TestLinearizability(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	const key = "linkey"
	rng := rand.New(rand.NewSource(7))

	stop := make(chan struct{})
	var faultWg sync.WaitGroup
	faultWg.Add(1)
	go func() {
		defer faultWg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(400 * time.Millisecond):
				c.randomFault(rng)
			}
		}
	}()

	var historyMu sync.Mutex
	var history []porcupine.Operation
	var opWg sync.WaitGroup

	doOp := func(clientID int, isPut bool, val string) {
		call := time.Now()

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
			return
		}

		var cmdStr string
		if isPut {
			cmdStr = fmt.Sprintf("PUT:%s:%s", key, val)
		} else {
			cmdStr = fmt.Sprintf("GET:%s", key)
		}

		idx, term, ok := leaderNode.Propose([]byte(cmdStr))
		if !ok {
			return
		}
		_ = term

		// Find the SPECIFIC node that applied our exact command at
		// this exact index. That node is guaranteed (by how applyLoop
		// works — it processes log indices strictly in order) to have
		// already applied every index before ours too, so it's safe
		// to use as the single source of truth for replay.
		confirmedNode := -1
		for wait := 0; wait < 60; wait++ {
			time.Sleep(20 * time.Millisecond)
			c.mu.Lock()
			for ni, m := range c.applied {
				if actualCmd, ok := m[idx]; ok {
					if actualCmd == cmdStr {
						confirmedNode = ni
					}
					break
				}
			}
			c.mu.Unlock()
			if confirmedNode != -1 {
				break
			}
		}
		if confirmedNode == -1 {
			return // our proposal never committed as ours; skip recording
		}

		ret := time.Now()

		if isPut {
			historyMu.Lock()
			history = append(history, porcupine.Operation{
				ClientId: clientID,
				Input:    regInput{op: "put", value: val},
				Call:     call.UnixNano(),
				Output:   regOutput{},
				Return:   ret.UnixNano(),
			})
			historyMu.Unlock()
		} else {
			currentVal := replayValueOnNode(c, confirmedNode, key, idx)

			historyMu.Lock()
			history = append(history, porcupine.Operation{
				ClientId: clientID,
				Input:    regInput{op: "get", value: ""},
				Call:     call.UnixNano(),
				Output:   regOutput{value: currentVal, ok: true},
				Return:   ret.UnixNano(),
			})
			historyMu.Unlock()
		}
	}

	for cid := 0; cid < 3; cid++ {
		opWg.Add(1)
		go func(cid int) {
			defer opWg.Done()
			deadline := time.Now().Add(6 * time.Second)
			i := 0
			for time.Now().Before(deadline) {
				i++
				if rng.Intn(2) == 0 {
					doOp(cid, true, fmt.Sprintf("v%d-%d", cid, i))
				} else {
					doOp(cid, false, "")
				}
				time.Sleep(15 * time.Millisecond)
			}
		}(cid)
	}
	opWg.Wait()
	close(stop)
	faultWg.Wait()

	c.net.HealAll()
	for i := 0; i < 5; i++ {
		if c.net.IsDown(i) {
			c.restart(i)
		}
	}
	c.waitForLeader()
	time.Sleep(1 * time.Second)

	historyMu.Lock()
	finalHistory := append([]porcupine.Operation(nil), history...)
	historyMu.Unlock()

	result, _ := porcupine.CheckOperationsVerbose(registerModel, finalHistory, 0)
	if result != porcupine.Ok {
		t.Fatalf("history is NOT linearizable (result: %v) — see porcupine output above", result)
	}
}

// replayValueOnNode reconstructs what "key" was set to as of log
// index upTo, using ONE specific node's applied-commands map. This
// node must already be known (by the caller) to have applied upTo —
// since applyLoop processes indices strictly in increasing order
// with no gaps, that guarantees every earlier index has been
// processed too, making this replay fully accurate. Always locks
// c.mu, since c.applied is written concurrently by other goroutines.
func replayValueOnNode(c *cluster, nodeIdx int, key string, upTo int) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := c.applied[nodeIdx]
	val := ""
	for idx := 1; idx <= upTo; idx++ {
		cmd, ok := m[idx]
		if !ok {
			continue
		}
		if len(cmd) > 4 && cmd[:4] == "PUT:" {
			rest := cmd[4:]
			for j := 0; j < len(rest); j++ {
				if rest[j] == ':' {
					if rest[:j] == key {
						val = rest[j+1:]
					}
					break
				}
			}
		}
	}
	return val
}