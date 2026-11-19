# Load test results

Measured with `python -m benchmarks.load_test`, see that script for
the flags. The headline numbers are the live server runs, a
uvicorn server on 127.0.0.1, single worker, default config,
capacity 1000, 100 byte values, 1000 distinct keys unless noted.
All runs were correct, 0 errors and 0 wrong values.

## Live server, 2026-11-18

| Scenario | Threads | Ops | Read share | ops/s | p50 ms | p95 ms | p99 ms | max ms | Hit rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mixed, disjoint keys | 8 | 16000 | 80 percent | 782.8 | 7.264 | 32.086 | 40.127 | 86.674 | 1.0 |
| Read heavy, disjoint keys | 8 | 16000 | 95 percent | 797.4 | 7.109 | 32.467 | 40.517 | 69.152 | 1.0 |
| Write only, disjoint keys | 8 | 16000 | 0 percent | 659.9 | 9.662 | 30.183 | 37.832 | 73.876 | n/a |
| Mixed, 21 shared hot keys | 8 | 16000 | 80 percent | 791.1 | 7.47 | 30.237 | 39.292 | 83.109 | 1.0 |
| Mixed, single thread | 1 | 2000 | 80 percent | 626.1 | 1.49 | 2.253 | 3.292 | 10.353 | 1.0 |

Commands used, with the server started first on port 8123:

```bash
uvicorn app.main:app
python -m benchmarks.load_test -u http://127.0.0.1:8123
python -m benchmarks.load_test -u http://127.0.0.1:8123 -r 0.95
python -m benchmarks.load_test -u http://127.0.0.1:8123 -r 0.0
python -m benchmarks.load_test -u http://127.0.0.1:8123 -d shared -k 21
python -m benchmarks.load_test -u http://127.0.0.1:8123 -t 1
```

## What the numbers say

- Threads add latency without adding throughput. One thread does
  626.1 ops/s at p50 1.49 ms, eight threads do 782.8 ops/s at
  p50 7.26 ms, so the extra threads mostly queue behind the store
  lock and the WAL fsync on every write. This is the same finding
  as Day 7, now measured over HTTP: real scaling has to come from
  sharding across processes or nodes, not from more threads.
- Write only is the slowest mix, 659.9 ops/s against 797.4 for
  read heavy, because every write fsyncs the WAL before it is
  applied, which is the durability cost Day 12 chose on purpose.
- Hot keys cost nothing extra. 21 shared keys measure 791.1
  ops/s, the same as 1000 disjoint keys at 782.8 ops/s, so key
  contention is not the bottleneck, lock and fsync contention is.
- The single thread run saw 875 evictions, because earlier runs
  had already filled the store to its 1000 key capacity, and its
  warmup wrote 1000 more keys. Its own reads were unaffected,
  0 evicted reads, since it only reads keys it wrote in the timed
  run, which are the most recent.

## In process, 2026-11-18

| Scenario | Threads | Ops | Read share | ops/s | p50 ms | p95 ms | p99 ms | max ms | Hit rate |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Mixed, disjoint keys | 8 | 16000 | 80 percent | 132.9 | 56.642 | 108.651 | 134.143 | 177.687 | 1.0 |

Do not compare this row with the live rows. In process runs go
through a TestClient portal per thread, and the portal overhead
dominates the measurement, so 132.9 ops/s measures the test
harness, not the cache. In process mode is for correctness smoke
tests, the live server is how throughput is measured.

## How to reproduce

Start a fresh server with an empty data directory, so earlier
runs cannot fill the store before the timed run, then run the
script with -u. Numbers were measured on the dev VM, 1.5 shared
CPUs, and will differ on other hardware, so rerun rather than
quoting these as the cache's speed.
