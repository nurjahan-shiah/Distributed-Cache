# Distributed Cache, KV Store

A thread safe, in memory key value store with LRU eviction, TTL expiry, and
disk persistence, built in small daily commits as big tech interview prep.
The point is not just a working cache, it is the concurrency, eviction, and
scaling decisions recorded along the way, so you can talk about them in an
interview.

**Status:** In progress, one feature per day, pushed daily.
Day 13 is done, 2026-11-17. The project is finished when every item below is checked off.

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
- Disk persistence, the store snapshots to data/cache_snapshot.json at
  checkpoints and loads that snapshot on startup, so keys survive a
  restart
- Write ahead log, every mutation is appended to
  data/cache_wal.jsonl and fsynced before it is applied, so a crash
  never loses an acknowledged write, and on startup the log replays
  after the snapshot, with `POST /checkpoint` writing a snapshot and
  truncating the log on demand, and an automatic checkpoint after
  1000 logged writes
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
- Batch get and set, `POST /cache/batch/get` with a list of keys
  returns the values found plus the keys that missed, and
  `POST /cache/batch/set` with a dict of items sets them all in one
  call, with one snapshot at the end, and both are capped at 1000
  keys per request
- Compare and set, `POST /cache/cas` with a key, an expected value,
  and a new value, writes only when the current value matches, and
  an expected value of null creates the key only when it is missing,
  so it works as a lock or a counter primitive, with success and
  failure counts in `/stats`
- Replication, each cluster node holds a primary plus one replica,
  every read and write applies to both copies, so they stay
  identical, `GET /cluster/replication` shows per node sync state,
  and `POST /cluster/node/{node}/fail-primary` fails over to the
  replica while `POST /cluster/node/{node}/recover-primary` resyncs
  the primary and brings it back, with the same pair of endpoints
  for a single replica
- Config from environment, capacity, eviction policy, snapshot path,
  sweep interval, node list, replication factor, ring replicas, and
  max batch size all come from `CACHE_` variables, with the old
  hardcoded values as defaults, and `GET /config` shows the
  effective config plus whether each value came from the environment
  or a default, while an invalid value fails fast at startup
- Eviction and memory limits, each entry is charged its key and
  value as UTF-8 bytes plus a fixed 64 byte overhead, the store
  evicts by LRU or LFU when it passes its byte budget as well as
  its key count capacity, `GET /stats` and `GET /memory` report
  memory used, the budget, and the percentage used, and a single
  value bigger than the whole budget is kept and counted as an
  oversize write instead of vanishing on write

## Stack
Python, FastAPI, standard library only for the store itself.

## Run it
```bash
pip install -r requirements.txt
uvicorn app.main:app
```

Config comes from the environment, all optional, with the defaults
shown here. A `.env` file is not loaded, export the variables
instead, and changing one needs a restart, since config is read once
at startup.
```bash
CACHE_CAPACITY=1000 CACHE_EVICTION_POLICY=lru \
CACHE_SNAPSHOT_PATH=data/cache_snapshot.json \
CACHE_SWEEP_INTERVAL_SECONDS=10 CACHE_NODES=node-a,node-b,node-c \
CACHE_REPLICATION_FACTOR=2 CACHE_RING_REPLICAS=100 \
CACHE_MAX_BATCH_SIZE=1000 CACHE_WAL_PATH=data/cache_wal.jsonl \
CACHE_WAL_CHECKPOINT_RECORDS=1000 \
CACHE_MAX_MEMORY_BYTES=0 uvicorn app.main:app
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
- [x] Day 8, Batch get and set endpoints, 2026-11-10
- [x] Day 9, Compare and set, 2026-11-11
- [x] Day 10, Replication, primary and replica sync, 2026-11-13
- [x] Day 11, Config from environment, capacity and defaults, 2026-11-15
- [x] Day 12, Write ahead log for durability, 2026-11-16
- [x] Day 13, Eviction and memory limits, size tracking, 2026-11-17
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
  holds the expired keys until the next checkpoint, but load already drops
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
- Day 8 batch design: a batch is one lock hold, not one lock hold
  per key, so a 100 key batch pays one lock acquisition and one
  eviction pass instead of 100 of each. `get_many` returns only the
  keys that were found, and the endpoint adds the missing list, so
  the caller can tell a miss from a key it never asked for, and a
  repeated key in one request is read once. Each found key counts
  as a hit and each missing key as a miss, so the stats stay
  consistent with single gets. `set_many` gives every key the same
  TTL and updates frequencies the same way a single set does, so
  LRU and LFU behave the same as setting the keys one by one. The
  cluster groups keys by shard first, so a batch spanning the
  cluster still makes one call per shard. Batches are capped at
  1000 keys, so one huge request cannot hold the store lock for a
  long time, and the cap is a 400, not a silent truncation, so the
  caller knows its batch did not run. The batch set logs one record,
  which is the point of batching: one log record instead of one per
  key, and the snapshot only happens at a checkpoint since Day 12.
- Day 9 compare and set design: the compare and the write happen
  under one lock hold, so no other thread can slip a write in
  between, which is what makes CAS safe for locks, leader election,
  and counters built on retries. An expected value of null means
  the key must be missing, so CAS doubles as create if absent, and
  an expired key counts as missing, so a stale lock can be
  reclaimed. On a match the value is replaced, and the TTL is
  replaced only when a new one is given, otherwise the old expiry
  is kept, so a lock refresh does not silently extend a lease it
  did not ask to extend. A failed compare changes nothing, not even
  recency or frequency, so a failed attempt cannot keep a cold key
  alive. Success and failure counts are in stats, and the cluster
  routes a CAS to the owning shard, so it stays atomic on one
  shard. A CAS is not a transaction across keys, and it does not
  span shards, so a multi key atomic update still needs a lock or a
  transaction layer on top.
- Day 10 replication design: each cluster node is a primary plus
  replicas, and every operation, read or write, applies to every
  healthy copy under one shard lock, so the copies stay identical,
  including LRU order and access frequencies, which is what keeps
  eviction identical on all of them. A write returns only after
  every healthy copy has it, so there is no replication lag, and a
  read never sees a stale value from a replica. Failover is just
  the serving copy moving down the list: fail_primary marks the
  primary down and the first healthy replica serves, and while the
  primary is down writes go to that replica and the primary is
  flagged out of sync. Recover resyncs by copying every live entry
  from the serving copy, deleting keys the target holds that the
  source does not, so deletes that happened while it was down are
  honoured, and TTLs are recomputed from the absolute expiry, so a
  key keeps its original expiry. A down replica is skipped and
  counted, not written to, and it resyncs the same way. The cost
  is stated, not hidden: replicating reads costs one store op per
  copy per read, which is slow next to reading one copy, and
  production systems usually replicate writes only and accept lag
  and eviction drift, so a failover there can turn a hit into a
  miss. Copies live on the same node, so this fixes a primary
  failure, not a whole node loss, which would need copies on other
  nodes. verify_sync compares the serving copy against every
  healthy copy and reports missing, extra, and different keys, so
  the sync invariant is checked, not trusted.
- Day 11 config design: every tunable the app used to hardcode now
  comes from a `CACHE_` variable, with the old hardcoded value kept
  as the default, so a plain run with nothing set behaves exactly
  as it did before Day 11. Config is read once at startup into a
  frozen dataclass, so it cannot drift while the process runs, and
  changing a variable needs a restart, which is the simple and
  predictable choice for a cache, since resizing a live store would
  mean deciding what to evict mid flight. Invalid values fail fast
  with a ValueError naming the variable, so a typo like
  CACHE_CAPACITY=abc stops startup instead of silently running
  with the wrong size, and an empty value counts as unset, so an
  empty export does not break a start. `GET /config` reports the
  effective config plus a sources map, env or default per value,
  so an operator can see what is actually running without guessing
  which variable took effect. The config module takes a plain dict,
  not os.environ directly, so tests inject a fake env without
  touching global state. A `.env` file is not loaded, there is no
  dotenv dependency, the variables must be exported, and `.env` is
  in .gitignore so secrets never land in the repo by accident.
- Day 12 write ahead log design: a mutation is appended to the log
  and fsynced before it is applied in memory, so the log is the
  durability path and the snapshot is only a checkpoint, which fixes
  the O(n) snapshot per write cost recorded on Day 2, since a write
  now costs one small append plus fsync instead of rewriting the
  whole store. Records are one JSON object per line, stamped with a
  sequence number that only ever rises, and the snapshot records the
  last seq it contains, so replay after a load skips records the
  snapshot already holds. That seq is what makes the crash window
  between a snapshot write and a log truncate safe: replaying a
  record twice would re-add a key the snapshot already has, and
  could evict a different key than the live run did. A checkpoint
  snapshots and truncates under the same lock hold, so no write can
  slip in between and be lost, and the store lock became an RLock
  so a checkpoint triggered inside a mutation can snapshot while
  the mutation holds it, with the tradeoff that writers pause
  briefly while the snapshot file is written. A batch is one log
  record, so a torn write drops the whole batch instead of half
  applying it, and a torn last line is truncated on open, so it
  never replays and never blocks a later append. Sweep removals are
  not logged, replay drops expired sets by their absolute expiry,
  and only successful writes are logged, so a failed compare and
  set and a delete of a missing key add nothing to the log. The
  cluster shards still have no log, they were never persisted, so
  the log covers the single store behind the API, and giving each
  shard its own log is a later step. The fsync per write is the
  honest cost of durability on one disk, and production systems
  usually group commit or fsync on a timer to trade a little
  durability for throughput.
- Day 13 memory limit design: an entry is charged its key and value
  as UTF-8 bytes plus a fixed 64 byte overhead for the dict slot,
  the expiry, the frequency, and the tuple, so the budget is an
  estimate, not a process RSS reading, and the same number is what
  the tests assert, which keeps it honest. The store keeps a
  running total and a per key size map, updated on every mutation,
  expiry, eviction, load, and replay, so checking the budget is
  O(1) and never rescans the store, and snapshot load and WAL
  replay recompute sizes instead of storing them, so a restart
  reports the exact same total. Eviction now has two triggers,
  key count and charged bytes, and both use the same LRU or LFU
  victim choice, so one eviction path serves both limits. A single
  value bigger than the whole budget is kept, not refused and not
  evicted on write, because refusing it would fail a write the
  caller already made, and evicting it would make the write
  silently vanish, so it is stored, counted in oversize_writes,
  and visible in stats, and the next write evicts it first under
  LRU, since it is the oldest entry. The limit is per store and
  per shard, the same way capacity already works, so a cluster
  budget is the sum across nodes, not one shared pool, and the
  cluster stats roll the totals up. The default is 0, meaning no
  limit, so a plain run behaves exactly as it did before Day 13,
  and the limit is a startup config value, not a live resize,
  because resizing live would mean deciding what to evict mid
  flight, which is the same reason Day 11 kept config frozen.

