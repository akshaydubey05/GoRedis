// Package transport implements raft.Transport over real TCP
// connections, using Go's built-in net/rpc package. This is what
// lets separate GoRedis processes (not just goroutines in one
// process) actually talk to each other as a Raft cluster.
package transport

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/raft"
)

// ---------- server side: expose a raft.Node's RPC handlers ----------

// RPCServer wraps a raft.Node so net/rpc can call its two handler
// methods over the wire.
type RPCServer struct {
	n *raft.Node
}

func (s *RPCServer) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = *s.n.HandleRequestVote(args)
	return nil
}

func (s *RPCServer) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = *s.n.HandleAppendEntries(args)
	return nil
}

// Serve starts listening for incoming Raft RPCs on addr. Call this
// once per node at startup, alongside starting the node itself.
func Serve(addr string, n *raft.Node) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &RPCServer{n: n}); err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()
	return nil
}

// ---------- client side: implements raft.Transport ----------

// TCP is a raft.Transport that reaches peers over real network
// connections, lazily connecting and reconnecting as needed.
type TCP struct {
	addrs map[int]string

	mu      sync.Mutex
	clients map[int]*rpc.Client
}

func NewTCP(addrs map[int]string) *TCP {
	return &TCP{addrs: addrs, clients: map[int]*rpc.Client{}}
}

func (t *TCP) client(to int) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.clients[to]; ok {
		return c, nil
	}
	conn, err := net.DialTimeout("tcp", t.addrs[to], 200*time.Millisecond)
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.clients[to] = c
	return c, nil
}

// drop closes and forgets a cached client, so the next call will
// reconnect fresh — used after any error, since a stale broken
// connection is worse than no connection.
func (t *TCP) drop(to int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.clients[to]; ok {
		c.Close()
		delete(t.clients, to)
	}
}

func (t *TCP) call(to int, method string, args, reply any) error {
	c, err := t.client(to)
	if err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- c.Call(method, args, reply) }()

	select {
	case err := <-done:
		if err != nil {
			t.drop(to)
		}
		return err
	case <-time.After(300 * time.Millisecond):
		t.drop(to) // slow or hung peer: give up rather than block Raft's timing
		return errors.New("transport: rpc call timed out")
	}
}

func (t *TCP) RequestVote(to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	if err := t.call(to, "Raft.RequestVote", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *TCP) AppendEntries(to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	if err := t.call(to, "Raft.AppendEntries", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}