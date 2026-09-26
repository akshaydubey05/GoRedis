// Package fsm is the "state machine" that Raft drives. Every node in
// the cluster runs the exact same sequence of committed commands
// through Apply, so they all end up with identical data — this is
// what "replicated state machine" means in practice.
package fsm

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/akshaydubey05/GoRedis/internal/store"
)

// Command is what actually gets put inside a raft.LogEntry.Command
// ([]byte, via JSON). TS is stamped ONCE by the leader when it first
// accepts the command — every node uses this same value instead of
// calling time.Now() itself. If nodes used their own clocks, EXPIRE
// would compute a different expiry time on each node, and the
// cluster would silently diverge. This is the single most important
// design decision in this file — see the guide's Phase 4 notes.
type Command struct {
	Args []string `json:"args"`
	TS   int64    `json:"ts"`
}

// Reply mirrors what a RESP reply needs to carry, without importing
// the resp package here (fsm should stay independent of the wire
// protocol — server.go will translate Reply -> resp.Value).
type Reply struct {
	Kind  string   // "ok", "bulk", "null", "int", "err", "array"
	Str   string   // used by "bulk", "err"
	Int   int64    // used by "int"
	Array []string // used by "array" (as flat key,value,key,value... for HGETALL)
}

func errReply(msg string) Reply { return Reply{Kind: "err", Str: msg} }

type FSM struct {
	db *store.Store
}

func New(db *store.Store) *FSM {
	return &FSM{db: db}
}

// Apply executes one committed command against the store. This is
// called on EVERY node in the cluster (not just the leader) once the
// command is committed — that's how replication actually produces
// identical data everywhere.
//
// CRITICAL RULE: this function must never call time.Now(), never use
// randomness, and never read anything from outside c.Args/c.TS. Any
// such "hidden input" would make different nodes compute different
// results from the identical committed command, silently breaking
// the cluster's consistency guarantee.
func (f *FSM) Apply(c Command) Reply {
	if len(c.Args) == 0 {
		return errReply("empty command")
	}

	args := c.Args
	now := c.TS

	switch strings.ToUpper(args[0]) {

	case "SET":
		if len(args) != 3 {
			return errReply("wrong number of arguments for 'set' command")
		}
		f.db.Set(args[1], args[2])
		return Reply{Kind: "ok"}

	case "GET":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'get' command")
		}
		v, ok := f.db.Get(args[1], now)
		if !ok {
			return Reply{Kind: "null"}
		}
		return Reply{Kind: "bulk", Str: v}

	case "DEL":
		if len(args) < 2 {
			return errReply("wrong number of arguments for 'del' command")
		}
		return Reply{Kind: "int", Int: int64(f.db.Del(now, args[1:]...))}

	case "EXISTS":
		if len(args) < 2 {
			return errReply("wrong number of arguments for 'exists' command")
		}
		return Reply{Kind: "int", Int: int64(f.db.Exists(now, args[1:]...))}

	case "INCR":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'incr' command")
		}
		n, err := f.db.Incr(args[1], 1, now)
		if err != nil {
			return errReply(err.Error())
		}
		return Reply{Kind: "int", Int: n}

	case "DECR":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'decr' command")
		}
		n, err := f.db.Incr(args[1], -1, now)
		if err != nil {
			return errReply(err.Error())
		}
		return Reply{Kind: "int", Int: n}

	case "INCRBY":
		if len(args) != 3 {
			return errReply("wrong number of arguments for 'incrby' command")
		}
		delta, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return errReply("value is not an integer or out of range")
		}
		n, err := f.db.Incr(args[1], delta, now)
		if err != nil {
			return errReply(err.Error())
		}
		return Reply{Kind: "int", Int: n}

	case "EXPIRE":
		if len(args) != 3 {
			return errReply("wrong number of arguments for 'expire' command")
		}
		secs, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return errReply("value is not an integer or out of range")
		}
		// now+secs*1000 is computed here from the LEADER's stamped
		// "now" — identical on every node, so every replica sets the
		// exact same expiry timestamp.
		if f.db.ExpireAt(args[1], now+secs*1000, now) {
			return Reply{Kind: "int", Int: 1}
		}
		return Reply{Kind: "int", Int: 0}

	case "TTL":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'ttl' command")
		}
		return Reply{Kind: "int", Int: f.db.TTL(args[1], now)}

	case "HSET":
		if len(args) != 4 {
			return errReply("wrong number of arguments for 'hset' command")
		}
		f.db.HSet(args[1], args[2], args[3])
		return Reply{Kind: "ok"}

	case "HGET":
		if len(args) != 3 {
			return errReply("wrong number of arguments for 'hget' command")
		}
		v, ok := f.db.HGet(args[1], args[2])
		if !ok {
			return Reply{Kind: "null"}
		}
		return Reply{Kind: "bulk", Str: v}

	case "HGETALL":
		if len(args) != 2 {
			return errReply("wrong number of arguments for 'hgetall' command")
		}
		fields := f.db.HGetAll(args[1])
		arr := make([]string, 0, len(fields)*2)
		for k, v := range fields {
			arr = append(arr, k, v)
		}
		return Reply{Kind: "array", Array: arr}

	default:
		return errReply("unknown command '" + args[0] + "'")
	}
}

// Encode turns a Command into the []byte that gets stored in a
// raft.LogEntry.
func Encode(c Command) []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic("fsm: failed to encode command: " + err.Error()) // args/TS are always encodable
	}
	return data
}

// Decode reverses Encode. Returns an error if the bytes are corrupt
// or from an incompatible version — callers should treat that as a
// serious problem (it means the log itself is unreadable).
func Decode(data []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(data, &c)
	return c, err
}