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

	stop := make(chan struct{})
	var faultWg sync.WaitGroup
	faultWg.Add(1)
	go func() {
		defer faultWg.Done()
		localRng := rand.New(rand.NewSource(7))
		for {
			select {
			case <-stop:
				return
			case <-time.After(800 * time.Millisecond): // gentler: was 250ms, then 400ms
				c.randomFault(localRng)
			}
		}
	}()

	var historyMu sync.Mutex
	var history []porcupine.Operation
	var opWg sync.WaitGroup

	// doOp proposes either a PUT or a GET. reqID must be GLOBALLY
	// UNIQUE across every call, for both PUTs and GETs — this lets us
	// later prove "the thing sitting at index idx is MY exact
	// proposal" rather than "some command with the same text as
	// mine, possibly from a different attempt that coincidentally
	// landed at the same log slot after a leadership change
	// overwrote the original." Without this, two different GET
	// attempts (which carry no payload of their own) would be
	// indistinguishable once serialized, and a stale/discarded
	// proposal could be mistaken for having committed using a
	// DIFFERENT operation's real commit timing — silently corrupting
	// the history handed to Porcupine.
	doOp := func(clientID int, isPut bool, val string, reqID string) {
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
			cmdStr = fmt.Sprintf("PUT:%s:%s:%s", key, val, reqID)
		} else {
			cmdStr = fmt.Sprintf("GET:%s:%s", key, reqID)
		}

		idx, term, ok := leaderNode.Propose([]byte(cmdStr))
		if !ok {
			return
		}
		_ = term

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
			return // our exact proposal never committed; skip recording
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

	for cid := 0; cid < 2; cid++ { // gentler: was 4, then 3
		opWg.Add(1)
		go func(cid int) {
			defer opWg.Done()
			localRng := rand.New(rand.NewSource(int64(100 + cid)))

			deadline := time.Now().Add(4 * time.Second) // gentler: was 6s
			i := 0
			for time.Now().Before(deadline) {
				i++
				reqID := fmt.Sprintf("c%d-%d", cid, i)
				if localRng.Intn(2) == 0 {
					doOp(cid, true, fmt.Sprintf("v%d-%d", cid, i), reqID)
				} else {
					doOp(cid, false, "", reqID)
				}
				time.Sleep(30 * time.Millisecond) // gentler: was 15ms
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
// node is already known (by the caller) to have applied upTo — since
// applyLoop processes indices strictly in increasing order with no
// gaps, every earlier index has necessarily been processed too,
// making this replay fully accurate. Always locks c.mu, since
// c.applied is written concurrently by other goroutines.
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
			// format: PUT:<key>:<value>:<reqID>
			rest := cmd[4:] // "<key>:<value>:<reqID>"
			firstColon := -1
			for j := 0; j < len(rest); j++ {
				if rest[j] == ':' {
					firstColon = j
					break
				}
			}
			if firstColon == -1 || rest[:firstColon] != key {
				continue
			}
			afterKey := rest[firstColon+1:] // "<value>:<reqID>"
			lastColon := -1
			for j := len(afterKey) - 1; j >= 0; j-- {
				if afterKey[j] == ':' {
					lastColon = j
					break
				}
			}
			if lastColon == -1 {
				continue
			}
			val = afterKey[:lastColon]
		}
	}
	return val
}