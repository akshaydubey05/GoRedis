// Package server accepts TCP connections from Redis clients and
// dispatches their commands to the store.
package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/akshaydubey05/GoRedis/internal/resp"
	"github.com/akshaydubey05/GoRedis/internal/store"
)

// AOFWriter is the one thing server needs from your AOF: a way to
// persist a command. Defined as an interface (not a concrete type)
// so server doesn't need to import the aof package directly, and so
// tests can pass in a fake that does nothing.
type AOFWriter interface {
	Write(v resp.Value) error
}

type Server struct {
	db  *store.Store
	aof AOFWriter
}

func New(db *store.Store, aof AOFWriter) *Server {
	return &Server{db: db, aof: aof}
}

// Listen starts accepting connections on addr (e.g. ":6379") and
// blocks forever. This is the fix for your original bug: Accept()
// is called in a loop, and every connection gets its own goroutine,
// so many redis-cli windows can connect at once.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	fmt.Println("Listening on", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := resp.NewResp(conn)
	writer := resp.NewWriter(conn)

	for {
		v, err := reader.Read()
		if err != nil {
			return // client disconnected or sent bad data; end this goroutine
		}

		if v.Typ != "array" || len(v.Array) == 0 {
			writer.Write(resp.Value{Typ: "error", Str: "ERR expected array command"})
			continue
		}

		command := strings.ToUpper(v.Array[0].Bulk)
		args := v.Array[1:]

		// Persist mutating commands before executing them, same as
		// your original main.go did.
		if isWriteCommand(command) {
			if err := s.aof.Write(v); err != nil {
				fmt.Println("aof write error:", err)
			}
		}

		result := s.dispatch(command, args)
		writer.Write(result)
	}
}

func isWriteCommand(cmd string) bool {
	switch cmd {
	case "SET", "HSET", "DEL", "EXPIRE", "INCR", "DECR", "INCRBY":
		return true
	}
	return false
}

// dispatch runs one command against the store and returns the reply.
// now is captured once per command so a single command sees a single
// consistent point in time.
func (s *Server) dispatch(command string, args []resp.Value) resp.Value {
	now := time.Now().UnixMilli()

	switch command {
	case "PING":
		if len(args) == 0 {
			return resp.Value{Typ: "string", Str: "PONG"}
		}
		return resp.Value{Typ: "string", Str: args[0].Bulk}

	case "COMMAND":
		// redis-cli sends this on connect; an empty array keeps it happy.
		return resp.Value{Typ: "array", Array: []resp.Value{}}

	case "SET":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'set' command")
		}
		s.db.Set(args[0].Bulk, args[1].Bulk)
		return resp.Value{Typ: "string", Str: "OK"}

	case "GET":
		if len(args) != 1 {
			return errReply("wrong number of arguments for 'get' command")
		}
		v, ok := s.db.Get(args[0].Bulk, now)
		if !ok {
			return resp.Value{Typ: "null"}
		}
		return resp.Value{Typ: "bulk", Bulk: v}

	case "DEL":
		if len(args) == 0 {
			return errReply("wrong number of arguments for 'del' command")
		}
		return resp.Value{Typ: "integer", Num: s.db.Del(now, bulkStrings(args)...)}

	case "EXISTS":
		if len(args) == 0 {
			return errReply("wrong number of arguments for 'exists' command")
		}
		return resp.Value{Typ: "integer", Num: s.db.Exists(now, bulkStrings(args)...)}

	case "INCR":
		if len(args) != 1 {
			return errReply("wrong number of arguments for 'incr' command")
		}
		n, err := s.db.Incr(args[0].Bulk, 1, now)
		if err != nil {
			return errReply(err.Error())
		}
		return resp.Value{Typ: "integer", Num: int(n)}

	case "DECR":
		if len(args) != 1 {
			return errReply("wrong number of arguments for 'decr' command")
		}
		n, err := s.db.Incr(args[0].Bulk, -1, now)
		if err != nil {
			return errReply(err.Error())
		}
		return resp.Value{Typ: "integer", Num: int(n)}

	case "INCRBY":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'incrby' command")
		}
		delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil {
			return errReply("value is not an integer or out of range")
		}
		n, err := s.db.Incr(args[0].Bulk, delta, now)
		if err != nil {
			return errReply(err.Error())
		}
		return resp.Value{Typ: "integer", Num: int(n)}

	case "EXPIRE":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'expire' command")
		}
		secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
		if err != nil {
			return errReply("value is not an integer or out of range")
		}
		ok := s.db.ExpireAt(args[0].Bulk, now+secs*1000, now)
		if ok {
			return resp.Value{Typ: "integer", Num: 1}
		}
		return resp.Value{Typ: "integer", Num: 0}

	case "TTL":
		if len(args) != 1 {
			return errReply("wrong number of arguments for 'ttl' command")
		}
		return resp.Value{Typ: "integer", Num: int(s.db.TTL(args[0].Bulk, now))}

	case "HSET":
		if len(args) != 3 {
			return errReply("wrong number of arguments for 'hset' command")
		}
		s.db.HSet(args[0].Bulk, args[1].Bulk, args[2].Bulk)
		return resp.Value{Typ: "string", Str: "OK"}

	case "HGET":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'hget' command")
		}
		v, ok := s.db.HGet(args[0].Bulk, args[1].Bulk)
		if !ok {
			return resp.Value{Typ: "null"}
		}
		return resp.Value{Typ: "bulk", Bulk: v}

	case "HGETALL":
		if len(args) != 1 {
			return errReply("wrong number of arguments for 'hgetall' command")
		}
		fields := s.db.HGetAll(args[0].Bulk)
		arr := make([]resp.Value, 0, len(fields)*2)
		for k, v := range fields {
			arr = append(arr,
				resp.Value{Typ: "bulk", Bulk: k},
				resp.Value{Typ: "bulk", Bulk: v},
			)
		}
		return resp.Value{Typ: "array", Array: arr}

	default:
		return errReply("unknown command '" + command + "'")
	}
}

func errReply(msg string) resp.Value {
	return resp.Value{Typ: "error", Str: "ERR " + msg}
}

func bulkStrings(args []resp.Value) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = a.Bulk
	}
	return out
}

// ApplyFromAOF runs a command directly against the store without
// writing it back to the AOF (used only during startup replay).
func (s *Server) ApplyFromAOF(v resp.Value) {
	if v.Typ != "array" || len(v.Array) == 0 {
		return
	}
	command := strings.ToUpper(v.Array[0].Bulk)
	s.dispatch(command, v.Array[1:])
}