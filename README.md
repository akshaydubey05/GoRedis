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

## Project Structure

- `main.go` — TCP server bootstrap, request loop, AOF replay/write
- `handler.go` — command handlers and in-memory data stores
- `resp.go` — RESP reader/writer and marshaling logic
- `aof.go` — append-only file read/write and periodic sync

## Prerequisites

- Go 1.27.1 (as defined in `go.mod`)

## Run the Server

From the repository root:

```bash
go run .
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
