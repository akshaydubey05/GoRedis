![CI](https://github.com/akshaydubey05/GoRedis/actions/workflows/ci.yml/badge.svg)


# GoRedis

A lightweight Redis-like server built from scratch in Go to learn RESP parsing, command handling, and append-only persistence.

## Features

- RESP (Redis Serialization Protocol) parser and writer
- In-memory key-value support:
  - `PING`
  - `SET key value`
  - `GET key`
- In-memory hash support:
  - `HSET hash field value`
  - `HGET hash field`
  - `HGETALL hash`
- Append Only File (AOF) persistence (`database.aof`) with replay on startup

## Project Map

```text
GoRedis/
├── go.mod
├── go.sum
├── README.md
├── BENCHMARKS.md
├── .github/
│   └── workflows/
│       └── ci.yml
├── cmd/
│   ├── goredis/
│   │   └── main.go
│   └── bench/
│       └── main.go
├── internal/
│   ├── resp/
│   │   ├── resp.go
│   │   └── resp_test.go
│   ├── store/
│   │   ├── store.go
│   │   └── store_test.go
│   ├── aof/
│   │   └── aof.go
│   ├── server/
│   │   └── server.go
│   ├── raft/
│   │   ├── types.go
│   │   ├── storage.go
│   │   ├── node.go
│   │   ├── election.go
│   │   ├── replication.go
│   │   ├── apply.go
│   │   └── raft_test.go
│   ├── simnet/
│   │   └── simnet.go
│   ├── fsm/
│   │   └── fsm.go
│   ├── cluster/
│   │   ├── node.go
│   │   └── server.go
│   └── transport/
│       └── tcp.go
└── data/
```

## Run the Server

From the repository root:

```bash
go run ./cmd/goredis
```

The server listens on `:6379`.

## Try with redis-cli

In another terminal:

```bash
redis-cli -p 6379
```

Example commands:

```redis
PING
SET name akshay
GET name
HSET user:1 first_name Akshay
HSET user:1 last_name Dubey
HGET user:1 first_name
HGETALL user:1
```

## Persistence

- `SET` and `HSET` are appended to `database.aof`.
- On startup, commands in `database.aof` are replayed to restore in-memory state.

## Current Limitations

- Handles one TCP client connection at a time.
- Supports only a small subset of Redis commands.
