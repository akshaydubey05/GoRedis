![CI](https://github.com/akshaydubey05/GoRedis/actions/workflows/ci.yml/badge.svg)

# GoRedis

A Redis-compatible, fault-tolerant key-value store written in Go, with the **Raft consensus algorithm implemented from scratch** (no consensus libraries). Three nodes agree on every write; if the leader dies, the others elect a new one and no committed data is lost.

**Live demo:** https://go-redis-five.vercel.app/ (kill the leader and watch a new election happen)

> The hosted demo runs all 3 nodes inside one process over a simulated network, to fit free hosting. The Raft code is identical to the real multi-process cluster, which you can run locally over TCP (see below). The backend is on a free tier, so the first load may take up to a minute to wake up.

## What it does

- Speaks the Redis protocol (RESP), so `redis-cli` works against it
- Replicates every write across a Raft cluster before acknowledging it
- Survives node crashes and network partitions without losing committed writes
- Rebuilds each node's data by replaying its persisted Raft log after a restart
- Ships a live dashboard that visualizes elections and lets visitors inject faults

### Supported commands

`PING`, `SET`, `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `EXPIRE`, `TTL`, `HSET`, `HGET`, `HGETALL`

## Architecture

```text
redis-cli ──RESP/TCP──> cluster.Node ──Propose──> raft.Node ──AppendEntries──> peers
                             ^                        |
                             |                  committed entries
                             └──── fsm.Apply <────────┘   (runs on every node)
```

- **Raft** (`internal/raft`): leader election, log replication, commit rules, crash-safe persistence of term, vote and log.
- **State machine** (`internal/fsm`): executes only committed commands. It is deterministic: the leader stamps the timestamp into each command, so `EXPIRE` produces the same expiry time on every node.
- **Cluster glue** (`internal/cluster`): turns a client command into a Raft proposal, waits for commit, and replies. Non-leaders answer with `MOVED <leader address>`, so `redis-cli -c` follows the redirect automatically.
- **Transport** (`internal/transport`): Raft RPCs over TCP using Go's `net/rpc`.
- **Simulated network** (`internal/simnet`, `internal/sim`): an in-process network that can drop, delay, cut and crash nodes. Used by the tests and the hosted demo.
- **Gateway and dashboard** (`internal/gateway`, `cmd/gateway`, `web/`): an HTTP API with a Server-Sent Events stream, and a React + TypeScript dashboard.

## Run a 3-node cluster locally

Open three terminals from the repo root.

```bash
export C="0=127.0.0.1:7000@127.0.0.1:6380,1=127.0.0.1:7001@127.0.0.1:6381,2=127.0.0.1:7002@127.0.0.1:6382"

go run ./cmd/goredis --id 0 --cluster "$C" --data data0
go run ./cmd/goredis --id 1 --cluster "$C" --data data1
go run ./cmd/goredis --id 2 --cluster "$C" --data data2
```

On Windows PowerShell, set the variable with `$C = "0=127.0.0.1:7000@127.0.0.1:6380,1=127.0.0.1:7001@127.0.0.1:6381,2=127.0.0.1:7002@127.0.0.1:6382"` and use `--cluster $C`.

Then, in a fourth terminal:

```bash
redis-cli -c -p 6380
```

```redis
SET name akshay
GET name
INCR counter
EXPIRE counter 10
HSET user:1 first_name Akshay
HGETALL user:1
```

Try it: press Ctrl+C in the leader's terminal, reconnect `redis-cli` to one of the surviving ports, and run `GET name`. The data is still there. Restart the killed node with the same `--data` folder and it catches up.

## Run the dashboard locally

```bash
go run ./cmd/gateway          # starts the simulated 3-node cluster on :8080
cd web
npm install
npm run dev                   # open the printed URL
```

## Testing

```bash
go test -race ./...
```

- Unit tests for the RESP parser and the store (including a concurrent `INCR` test)
- Raft tests: election, replication, follower catch-up after restart, leader crash, and a partitioned leader that must not commit
- A chaos test: random crashes, restarts and partitions on a 5-node cluster while clients write, then checks that all nodes agree and no acknowledged write is lost
- A linearizability test using [Porcupine](https://github.com/anishathalye/porcupine) over the history of operations recorded under faults

CI runs `go vet`, `go build` and `go test -race` on every push.

## Project structure

```text
GoRedis/
├── cmd/
│   ├── goredis/      # one cluster node (real TCP)
│   └── gateway/      # HTTP gateway over the simulated cluster
├── internal/
│   ├── resp/         # RESP reader and writer
│   ├── store/        # thread-safe key-value store
│   ├── raft/         # Raft implementation and tests
│   ├── simnet/       # fake network with fault injection
│   ├── sim/          # whole cluster in one process
│   ├── fsm/          # deterministic state machine
│   ├── cluster/      # glue between clients, Raft and the FSM
│   ├── transport/    # Raft RPC over TCP
│   ├── gateway/      # HTTP and SSE API
│   ├── server/       # original single-node server
│   └── aof/          # original append-only-file persistence
├── web/              # React + TypeScript dashboard
├── Dockerfile
└── BENCHMARKS.md
```

`internal/server` and `internal/aof` are the original single-node implementation. They are kept for reference and are no longer used by `cmd/goredis`.

## Benchmarks

Single-node baseline from the original server, measured with `redis-benchmark -t set,get -n 100000 -c 50`: about 17.7k SET/s and 17.5k GET/s. See `BENCHMARKS.md`.

## Limitations and future work

- Every command, including `GET`, goes through the Raft log, so reads are linearizable but slower. A ReadIndex or follower-read path would speed them up.
- No snapshots or log compaction, so the Raft log grows without bound.
- No dynamic membership changes; the cluster size is fixed at startup.
- Supports a small subset of Redis commands.
- The hosted demo uses a simulated network rather than separate machines.

## References

- Ongaro and Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper)
- MIT 6.5840 (formerly 6.824) lectures and labs
