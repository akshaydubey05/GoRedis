# Benchmarks

## Baseline — single node, AOF persistence

**Date:** 25-09-2026
**Machine:** Inte; i7-13th gen, 16gb Ram
**Command:** `redis-benchmark -p 6379 -t set,get -n 100000 -c 50 -q`

| Command | Requests/sec |
|---|---|
| SET | 17755.68 requests per second |
| GET | 17488.63 requests per second |

## Notes

- Single node, no replication yet.
- AOF fsync happens once per second in the background (not per-command),
  trading a small durability window for throughput.
- This baseline will be compared against the 3-node Raft cluster once
  Phase 4 is complete, to show the throughput cost of replication.