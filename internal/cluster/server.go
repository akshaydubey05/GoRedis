package cluster

import (
	"net"
	"strings"

	"github.com/akshaydubey05/GoRedis/internal/fsm"
	"github.com/akshaydubey05/GoRedis/internal/resp"
)

// ServeRESP starts a TCP listener speaking the Redis protocol,
// backed by this cluster Node. Every mutating and reading command
// goes through Raft via n.Do(); only a couple of purely local
// commands (PING, COMMAND) are answered directly without touching
// consensus at all.
func (n *Node) ServeRESP(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go n.handleConn(conn)
	}
}

func (n *Node) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := resp.NewResp(conn)
	writer := resp.NewWriter(conn)

	for {
		v, err := reader.Read()
		if err != nil {
			return
		}
		if v.Typ != "array" || len(v.Array) == 0 {
			writer.Write(resp.Value{Typ: "error", Str: "ERR expected array command"})
			continue
		}

		args := make([]string, len(v.Array))
		for i, a := range v.Array {
			args[i] = a.Bulk
		}

		switch strings.ToUpper(args[0]) {
		case "PING":
			writer.Write(resp.Value{Typ: "string", Str: "PONG"})
		case "COMMAND":
			writer.Write(resp.Value{Typ: "array", Array: []resp.Value{}})
		default:
			writer.Write(toRESP(n.Do(args)))
		}
	}
}

// toRESP converts an fsm.Reply (protocol-agnostic) into the actual
// resp.Value bytes we send back to redis-cli.
func toRESP(r fsm.Reply) resp.Value {
	switch r.Kind {
	case "ok":
		return resp.Value{Typ: "string", Str: "OK"}
	case "bulk":
		return resp.Value{Typ: "bulk", Bulk: r.Str}
	case "null":
		return resp.Value{Typ: "null"}
	case "int":
		return resp.Value{Typ: "integer", Num: int(r.Int)}
	case "err":
		return resp.Value{Typ: "error", Str: "ERR " + r.Str}
	case "array":
		arr := make([]resp.Value, len(r.Array))
		for i, s := range r.Array {
			arr[i] = resp.Value{Typ: "bulk", Bulk: s}
		}
		return resp.Value{Typ: "array", Array: arr}
	default:
		return resp.Value{Typ: "error", Str: "ERR internal: unknown reply kind"}
	}
}