# Distributed Cache, KV Store

A thread safe, in memory key value store with LRU eviction, TTL expiry, and
disk persistence, built in small daily commits as big tech interview prep.
The point is not just a working cache, it is the concurrency, eviction, and
scaling decisions recorded along the way, so you can talk about them in an
interview.

**Status:** In progress, one feature per day, pushed daily.
Day 7 is done, 2026-11-09. The project is finished when every item below is checked off.

## What it does so far
- `PUT /cache/{key}` with a value and an optional TTL in seconds
- `GET /cache/{key}`, returns 404 on a miss or an expired key
- `DELETE /cache/{key}`
- `GET /stats`, hits, misses, hit rate, evictions, current size, uptime,
  and a cluster rollup with per node sizes and hit rate
- `GET /health`, liveness probe, always 200 while the process is up
- `GET /health/ready`, readiness probe, 200 when the cache can serve,
  503 when a sweeper stopped or the cluster has no nodes
- LRU eviction when the store is full, thread safe access with a lock
- Disk persistence, the store snapshots to data/cache_snapshot.json on
  every write and delete, and loads that snapshot on startup, so keys
  survive a restart
- Consistent hashing, keys route to one of three nodes through a ring
  with virtual nodes, `GET /cluster` shows per node sizes and
  `GET /cluster/route/{key}` shows which node owns a key
- LFU eviction as an alternative to LRU, switchable at runtime with
  `POST /config/eviction-policy`, frequencies tracked on every get and
  set, ties broken by least recently used
- Background TTL sweep, a daemon thread sweeps expired keys every 10
  seconds on the store and on every cluster shard, `POST /sweep` runs
  one on demand, and `/stats` reports keys swept and sweeps run
- Concurrency benchmark, `python -m benchmarks.concurrency` measures
  mixed get and set throughput for one store and for the cluster,
  under disjoint and hot key workloads, and verifies no writes are
  lost, with the lock review findings recorded below

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
- [x] Day 4, LFU eviction policy switch, 2026-11-05
- [x] Day 5, Background TTL sweep, 2026-11-07
- [x] Day 6, Stats and health endpoints, 2026-11-08
- [x] Day 7, Concurrency benchmark, lock review, 2026-11-09
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
- Day 4 eviction design: every key carries an access count, incremented
  on each get and on a set of an existing key, and LFU evicts the lowest
  count, breaking ties by least recently used, which is the key's
  position in the ordered dict. A brand new key starts at a frequency
  of 1, so it can be evicted straight away when the cache is full of
  hotter keys, which is the known LFU admission tradeoff, fixed in
  production by admission policies such as TinyLFU. Frequencies are tracked in both modes,
  so switching policy at runtime is instant and keeps the recorded
  history, and deleting or expiring a key clears its count. The LFU
  scan is O(n) per eviction, simple and correct at this scale, while a
  production O(1) LFU would keep frequency buckets instead. Counts are
  runtime state and are not stored in the snapshot, so loaded keys
  start at a frequency of 1 after a restart.
- Day 5 sweep design: lazy expiry on get keeps reads correct, but a
  key that is never read again would sit in memory until the store
  fills, so a daemon thread sweeps expired keys every 10 seconds on
  the store and on every cluster shard. The sweep takes the same lock
  as reads and writes, so it never races a get or a set, and it stops
  promptly because the thread waits on an event instead of sleeping.
  Swept keys count in expired_swept, not in misses, since no read
  failed. A sweep does not rewrite the snapshot, the snapshot still
  holds the expired keys until the next write, but load already drops
  expired entries, so a restart stays correct. The 10 second interval
  is a demo default, a production cache would tune it against write
  rate and memory pressure, or sweep in small batches to bound pause
  time on a large store.
- Day 6 health design: liveness and readiness are separate probes.
  Liveness, `GET /health`, only says the process is alive, so it stays
  200 even when the store is full or busy, and a load balancer restarts
  the process only when liveness fails. Readiness, `GET /health/ready`,
  says the cache can take traffic, so it returns 503 when the cluster
  has no nodes or a sweeper stopped, and the load balancer keeps traffic
  away until it recovers. Hit rate is hits over all reads, 0.0 before
  any read, and it is reported per store and rolled up across the
  cluster, so a single bad shard shows up in the total. Uptime is wall
  clock time since the store was created, recorded at startup, so a
  restart is visible as a reset uptime.
- Day 7 lock review: one `threading.Lock` guards the whole store, so
  every get, set, delete, sweep, and stats call serializes through it.
  The benchmark, `python -m benchmarks.concurrency`, 75 percent writes
  and 25 percent reads, measured about 563,000 ops/s single threaded,
  about 609,000 ops/s with 8 threads on disjoint keys, and about
  678,000 ops/s with 8 threads on 21 hot keys, all correct with no
  lost writes. Threads add almost nothing on one store, because the
  lock and the GIL serialize the work, so real scaling has to come
  from sharding across processes or nodes, not from more threads. The
  3 node cluster measured about 246,000 ops/s with 8 threads, slower
  per op than one store, because every op pays an MD5 hash plus a
  second lock, which is the honest cost of routing. The review found
  and fixed three races: a sweeper start race, where two threads
  starting a sweeper at once could both pass the alive check and leak
  a second sweeper, now serialized by a dedicated sweeper lock; a ring
  race, where a get_node racing a remove_node could bisect to a point
  already popped from the map and raise KeyError, now guarded by a
  ring lock; and cluster dict races, where a topology change could
  resize the shard dict mid loop, now fixed by snapshotting the shards
  under a topology lock and routing through it. One race is left on
  purpose: a write racing a migration can land on a shard after
  migration already moved its keys, so the write is lost. Fixing it
  properly needs a read write lock, versioned routing, or quiesced
  writes during topology changes, and pretending a plain lock fixes
  it would be wrong. The hot workload uses 21 keys, not 20, because
  20 shares a factor with the 4 step read pattern, so 5 of the 20 keys
  would only ever be read and never set. Numbers were measured on the
  dev machine, 2026-11-09, 8 threads, 1000 ops per thread, and will
  differ on other hardware.

