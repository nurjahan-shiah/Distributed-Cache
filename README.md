# Distributed Cache, KV Store

A thread safe, in memory key value store with LRU eviction, TTL expiry, and
disk persistence, built in small daily commits as big tech interview prep.
The point is not just a working cache, it is the concurrency, eviction, and
scaling decisions recorded along the way, so you can talk about them in an
interview.

**Status:** In progress, one feature per day, pushed daily.
Day 3 is done, 2026-11-04. The project is finished when every item below is checked off.

## What it does so far
- `PUT /cache/{key}` with a value and an optional TTL in seconds
- `GET /cache/{key}`, returns 404 on a miss or an expired key
- `DELETE /cache/{key}`
- `GET /stats`, hits, misses, evictions, and current size
- LRU eviction when the store is full, thread safe access with a lock
- Disk persistence, the store snapshots to data/cache_snapshot.json on
  every write and delete, and loads that snapshot on startup, so keys
  survive a restart
- Consistent hashing, keys route to one of three nodes through a ring
  with virtual nodes, `GET /cluster` shows per node sizes and
  `GET /cluster/route/{key}` shows which node owns a key

## Stack
Python, FastAPI, standard library only for the store itself.

## Run it
```bash
pip install -r requirements.txt
uvicorn app.main:app
```

## Run tests
```bash
pytest
```

## Daily build plan (one feature plus tests per day)
- [x] Day 1, Core store plus HTTP API, LRU, TTL, thread safety, tests
- [x] Day 2, Persistence to disk, snapshot and load on startup, 2026-11-03
- [x] Day 3, Consistent hashing, sharded nodes, 2026-11-04
- [ ] Day 4, LFU eviction policy switch
- [ ] Day 5, Background TTL sweep
- [ ] Day 6, Stats and health endpoints
- [ ] Day 7, Concurrency benchmark, lock review
- [ ] Day 8, Batch get and set endpoints
- [ ] Day 9, Compare and set
- [ ] Day 10, Replication, primary and replica sync
- [ ] Day 11, Config from environment, capacity and defaults
- [ ] Day 12, Write ahead log for durability
- [ ] Day 13, Eviction and memory limits, size tracking
- [ ] Day 14, Load test script and real benchmark numbers
- [ ] Day 15, System design writeup, docs/system-design.md
- [ ] Day 16, Interview talking points in README
- [ ] Day 17, Final polish, cleanup, project complete

## Design decisions (log them here as you go)
- LRU uses an ordered dict, most recently used keys move to the end, eviction
  takes from the front. It is simple, fast, and easy to reason about in an
  interview.
- A single lock guards the whole store on Day 1. Finer grained locking is a
  later optimisation, and the tradeoff is recorded when it lands.
- The database is not the source of truth here, the cache is. Persistence is
  a best effort snapshot, not a durable log, until Day 12.
- Day 2 snapshot design: snapshot writes go to a temp file in the same
  directory and are renamed over the old snapshot, so a crash mid write
  never leaves a half written file. Expired entries are dropped on
  snapshot and on load, a missing or corrupt snapshot leaves the store
  empty instead of failing startup, and load enforces capacity in LRU
  order. Snapshotting on every write is simple and correct, but it is
  O(n) per write, which is the cost Day 12, the write ahead log, exists
  to fix.
- Day 3 sharding design: each real node owns 100 virtual nodes on an
  MD5 ring, so keys spread evenly and adding a node moves only the keys
  on that node's virtual nodes, not the whole dataset. The cluster
  migrates keys on a clean add or remove, preserving TTLs, so a planned
  topology change does not lose keys. An unplanned node loss still loses
  its keys, since there is no replication yet, and that is exactly what
  Day 10 is for.
