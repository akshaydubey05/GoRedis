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

// registerModel describes, for Porcupine, what a correct single
// register is allowed to do: a GET must return the value written
// by the most recent PUT in the linearized history.
type regInput struct {
	op    string // "get" or "put"
	value string
}

type regOutput struct {
	value string
	ok    bool
}

var registerModel = porcupine.Model{
	Init: func() interface{} {
		return ""
	},

	Step: func(state, input, output interface{}) (bool, interface{}) {
		in := input.(regInput)
		out := output.(regOutput)
		st := state.(string)

		if in.op == "get" {
			return out.ok && out.value == st, st
		}

		// PUT always succeeds and updates the register.
		return true, in.value
	},
}

// TestLinearizability hammers a single key with concurrent GET/PUT
// operations while the cluster is subjected to faults. Every recorded
// operation has a real call and return timestamp, and Porcupine checks
// whether the observed history can be explained by some valid
// sequential ordering.
func TestLinearizability(t *testing.T) {
	c := newCluster(t, 5)
	defer c.shutdown()

	const key = "linkey"

	stop := make(chan struct{})

	// IMPORTANT:
	// rand.Rand is not safe for concurrent use. Keep the fault RNG
	// private to the fault goroutine.
	faultRng := rand.New(rand.NewSource(7))

	var faultWg sync.WaitGroup
	faultWg.Add(1)

	go func() {
		defer faultWg.Done()

		for {
			select {
			case <-stop:
				return

			case <-time.After(250 * time.Millisecond):
				c.randomFault(faultRng)
			}
		}
	}()

	var historyMu sync.Mutex
	var history []porcupine.Operation

	var opWg sync.WaitGroup

	doOp := func(clientID int, isPut bool, val string) {
		call := time.Now()

		// Find a currently reachable leader.
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
			// No leader available right now. Do not record an
			// incomplete operation in the linearizability history.
			return
		}

		var cmdStr string

		if isPut {
			cmdStr = fmt.Sprintf("PUT:%s:%s", key, val)
		} else {
			cmdStr = fmt.Sprintf("GET:%s", key)
		}

		idx, _, ok := leaderNode.Propose([]byte(cmdStr))
		if !ok {
			return
		}

		// Wait until this exact Raft log index has been applied.
		//
		// Keep the map that actually contains idx. We use that same
		// map when reconstructing the GET result rather than choosing
		// an unrelated node's map.
		var applied map[int]string

		for wait := 0; wait < 30; wait++ {
			time.Sleep(20 * time.Millisecond)

			c.mu.Lock()

			for _, m := range c.applied {
				if _, exists := m[idx]; exists {
					applied = m
					break
				}
			}

			c.mu.Unlock()

			if applied != nil {
				break
			}
		}

		if applied == nil {
			// The command did not become observable within the test's
			// timeout. Do not record an operation without a result.
			return
		}

		ret := time.Now()

		historyMu.Lock()
		defer historyMu.Unlock()

		if isPut {
			history = append(history, porcupine.Operation{
				ClientId: clientID,
				Input: regInput{
					op:    "put",
					value: val,
				},
				Call: call.UnixNano(),
				Output: regOutput{
					ok: true,
				},
				Return: ret.UnixNano(),
			})

			return
		}

		// The GET itself is represented by a Raft log entry. Since we
		// know that entry was applied at idx, reconstruct the value
		// after applying all commands through idx.
		currentVal := replayValueFromApplied(applied, key, idx)

		history = append(history, porcupine.Operation{
			ClientId: clientID,
			Input: regInput{
				op:    "get",
				value: "",
			},
			Call: call.UnixNano(),
			Output: regOutput{
				value: currentVal,
				ok:    true,
			},
			Return: ret.UnixNano(),
		})
	}

	for cid := 0; cid < 4; cid++ {
		opWg.Add(1)

		go func(cid int) {
			defer opWg.Done()

			// Every client gets its own RNG.
			//
			// math/rand.Rand is not safe for concurrent use, so sharing
			// one RNG between the four clients would trigger -race.
			clientRng := rand.New(rand.NewSource(int64(100 + cid)))

			deadline := time.Now().Add(6 * time.Second)
			i := 0

			for time.Now().Before(deadline) {
				i++

				if clientRng.Intn(2) == 0 {
					doOp(
						cid,
						true,
						fmt.Sprintf("v%d-%d", cid, i),
					)
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

	// Restore the cluster before shutting the test down.
	c.net.HealAll()

	for i := 0; i < 5; i++ {
		if c.net.IsDown(i) {
			c.restart(i)
		}
	}

	c.waitForLeader()
	time.Sleep(1 * time.Second)

	historyMu.Lock()
	historyCopy := append([]porcupine.Operation(nil), history...)
	historyMu.Unlock()

	result, _ := porcupine.CheckOperationsVerbose(
		registerModel,
		historyCopy,
		0,
	)

	if result != porcupine.Ok {
		t.Fatalf(
			"history is NOT linearizable (result: %v) — see porcupine output above",
			result,
		)
	}
}

// replayValueFromApplied reconstructs the value of key after all
// commands through upTo have been applied.
//
// The map passed here is the same applied map that was observed to
// contain the requested log index, so we don't accidentally reconstruct
// the state from a different/staler node.
func replayValueFromApplied(
	applied map[int]string,
	key string,
	upTo int,
) string {
	val := ""

	for idx := 1; idx <= upTo; idx++ {
		cmd, ok := applied[idx]
		if !ok {
			continue
		}

		if len(cmd) <= 4 || cmd[:4] != "PUT:" {
			continue
		}

		// Command format:
		//
		// PUT:<key>:<value>
		//
		// Split only at the first ':' after the key so values are
		// allowed to contain ':' as well.
		rest := cmd[4:]

		for j := 0; j < len(rest); j++ {
			if rest[j] != ':' {
				continue
			}

			if rest[:j] == key {
				val = rest[j+1:]
			}

			break
		}
	}

	return val
}