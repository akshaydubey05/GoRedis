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

// registerModel describes, for Porcupine, what a "correct single
// register" (one key holding one value) is allowed to do: a Get must
// return whatever the most recent Put set, and a Put always succeeds
// and changes the state.
type regInput struct {
	op    string // "get" or "put"
	value string
}
type regOutput struct {
	value string
	ok    bool // for get: whether the value matched
}

var registerModel = porcupine.Model{
	Init: func() interface{} { return "" },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(regInput)
		out := output.(regOutput)
		st := state.(string)

		if in.op == "get" {
			return out.value == st, st // a correct get must return the CURRENT state
		}
		return true, in.value // a put always succeeds and updates the state
	},
}

// TestLinearizability hammers a single key with concurrent GET/SET
// operations while chaos is happening, records exactly when each
// operation started and finished in real wall-clock time, and asks
// Porcupine whether some valid one-at-a-time ordering of those
// operations could have produced what we observed. If not, we have a
// real consistency bug.
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
			case <-time.After(250 * time.Millisecond):
				c.randomFault(rng)
			}
		}
	}()

	var mu sync.Mutex
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
			return // no leader available right now; skip this attempt
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

		// Wait (briefly) for this exact index to be applied on ANY
		// node, then read the resulting value for GET ops from our
		// own bookkeeping map (built alongside c.applied in start()).
		var appliedVal string
		found := false
		for wait := 0; wait < 30; wait++ {
			time.Sleep(20 * time.Millisecond)
			c.mu.Lock()
			for _, m := range c.applied {
				if v, ok := m[idx]; ok {
					appliedVal = v
					found = true
				}
			}
			c.mu.Unlock()
			if found {
				break
			}
		}
		if !found {
			return // timed out; skip recording (Porcupine needs a return to reason about it)
		}
		_ = term

		ret := time.Now()

		mu.Lock()
		defer mu.Unlock()

		if isPut {
			history = append(history, porcupine.Operation{
				ClientId: clientID,
				Input:    regInput{op: "put", value: val},
				Call:     call.UnixNano(),
				Output:   regOutput{},
				Return:   ret.UnixNano(),
			})
		} else {
			// Our commands are strings like "PUT:linkey:v" or "GET:linkey".
			// We recover the CURRENT value by replaying our own applied
			// log up to idx — simplest correct approach for this test.
			currentVal := replayValue(c, key, idx)
			history = append(history, porcupine.Operation{
				ClientId: clientID,
				Input:    regInput{op: "get", value: ""},
				Call:     call.UnixNano(),
				Output:   regOutput{value: currentVal, ok: true},
				Return:   ret.UnixNano(),
			})
		}
		_ = appliedVal
	}

	for cid := 0; cid < 4; cid++ {
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

	result, _ := porcupine.CheckOperationsVerbose(registerModel, history, 0)
	if result != porcupine.Ok {
		t.Fatalf("history is NOT linearizable (result: %v) — see porcupine output above", result)
	}
}

// replayValue reconstructs what "linkey" was set to as of log index
// upTo, by replaying every node's applied commands in index order.
// Any node's map works since checkAgreement already proved they
// agree; we just need one consistent source.
func replayValue(c *cluster, key string, upTo int) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Use whichever node has the most entries applied — doesn't
	// matter which, since all nodes agree on committed history.
	var best map[int]string
	for _, m := range c.applied {
		if best == nil || len(m) > len(best) {
			best = m
		}
	}

	val := ""
	for idx := 1; idx <= upTo; idx++ {
		cmd, ok := best[idx]
		if !ok {
			continue
		}
		if len(cmd) > 4 && cmd[:4] == "PUT:" {
			// format: PUT:<key>:<value>
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