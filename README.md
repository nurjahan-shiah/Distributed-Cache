# Distributed Cache, KV Store

A thread safe, in memory key value store with LRU eviction, TTL expiry, and
disk persistence, built in small daily commits as big tech interview prep.
The point is not just a working cache, it is the concurrency, eviction, and
scaling decisions recorded along the way, so you can talk about them in an
interview.

**Status:** In progress, one feature per day, pushed daily.
Day 1 is done. The project is finished when every item below is checked off.

## What it does so far
- `PUT /cache/{key}` with a value and an optional TTL in seconds
- `GET /cache/{key}`, returns 404 on a miss or an expired key
- `DELETE /cache/{key}`
- `GET /stats`, hits, misses, evictions, and current size
- LRU eviction when the store is full, thread safe access with a lock

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
- [ ] Day 2, Persistence to disk, snapshot and load on startup
- [ ] Day 3, Consistent hashing, sharded nodes
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
