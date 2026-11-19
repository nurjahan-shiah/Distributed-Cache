"""Day 14, HTTP load test script and real benchmark numbers.

Run it from the repo root:

    python -m benchmarks.load_test
    python -m benchmarks.load_test -t 4 -n 500 -r 0.8 -k 500
    python -m benchmarks.load_test -u http://127.0.0.1:8000

By default it drives the app in process through a TestClient, one
per thread, so no server needs to be running. Give -u to drive a
live server instead, which is the honest way to measure, since it
includes real HTTP, uvicorn, and JSON encoding. The in process
numbers are higher, because they skip the network and the server,
so the README records the live server numbers as the headline.

Each thread works its own keys in disjoint mode, the default, so a
read can check the value it expects, and a lost or corrupt write
shows up as a wrong value, not as a fast run. In shared mode every
thread works the same key pool, like a hot key workload, and value
checks are off, because another thread may have overwritten the
key, so only status codes and errors are counted there. A warmup
prefills every key once, untimed, so reads mostly hit. Latencies
are measured per request with perf_counter, and the report gives
ops per second plus mean, p50, p95, p99, and max latency, with
hit rate and evictions read from /stats before and after the run.
"""
import argparse
import json
import random
import sys
import threading
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

DISTRIBUTIONS = ("disjoint", "shared")


def percentile(sorted_values, pct):
    """Linear interpolation percentile of an already sorted list.

    pct is 0 to 100, so percentile(values, 50) is the median. Raises
    ValueError on an empty list or a pct outside 0 to 100.
    """
    if not sorted_values:
        raise ValueError("percentile needs at least one value")
    if pct < 0 or pct > 100:
        raise ValueError("pct must be between 0 and 100")
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = pct / 100 * (len(sorted_values) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    frac = rank - low
    return sorted_values[low] * (1 - frac) + sorted_values[high] * frac


def summarize_latencies(latencies):
    """Mean, p50, p95, p99, and max latency in milliseconds.

    latencies are seconds, as measured with perf_counter. Raises
    ValueError when the list is empty.
    """
    if not latencies:
        raise ValueError("no latencies to summarize")
    ordered = sorted(latencies)
    total = sum(ordered)
    return {
        "count": len(ordered),
        "mean_ms": round(total / len(ordered) * 1000, 3),
        "p50_ms": round(percentile(ordered, 50) * 1000, 3),
        "p95_ms": round(percentile(ordered, 95) * 1000, 3),
        "p99_ms": round(percentile(ordered, 99) * 1000, 3),
        "max_ms": round(ordered[-1] * 1000, 3),
    }


def _make_client(base_url, app):
    """One client per thread, in process or against a live server.

    In process mode uses a TestClient, one per thread, since the
    installed httpx ASGI transport is async only and cannot serve
    a sync client, and the app is imported lazily, so a live server
    run never imports it at all. raise_server_exceptions is off,
    so a 500 counts as a failed request in the report instead of
    raising inside the worker thread. The live client sets
    trust_env False, so it connects direct to the target server
    instead of reading proxy variables from the environment, which
    a load test must never route through.
    """
    if base_url is None:
        if app is None:
            from app.main import app as live_app

            app = live_app
        from fastapi.testclient import TestClient

        return TestClient(app, raise_server_exceptions=False)
    return httpx.Client(base_url=base_url, timeout=30.0, trust_env=False)


def _worker(client, thread_id, keys, ops_per_thread, read_ratio,
            value_size, distribution, latencies, errors, counts):
    """One thread of the timed workload, reads and writes by ratio.

    Each write stores a value only this thread wrote, so a later
    read of the same key can check it. A read of a key this thread
    wrote that comes back 404 means the key was evicted, which is
    counted, not treated as an error, since eviction is correct
    behaviour when the store is full. A read that returns a
    different value is a wrong value, which is an error, and in
    shared mode value checks are skipped, since another thread may
    have overwritten the key. Errors are collected, never raised,
    so the report can show them instead of crashing the run.
    """
    own_writes = {}
    reads = 0
    writes = 0
    wrong_values = 0
    evicted_reads = 0
    rng = random.Random(1000 + thread_id)
    try:
        for op in range(ops_per_thread):
            if (
                read_ratio > 0
                and own_writes
                and rng.random() < read_ratio
            ):
                key = rng.choice(list(own_writes.keys()))
                expected = own_writes[key]
                started = time.perf_counter()
                response = client.get(f"/cache/{key}")
                latencies.append(time.perf_counter() - started)
                reads += 1
                if response.status_code == 200:
                    if (
                        distribution == "disjoint"
                        and response.json()["value"] != expected
                    ):
                        wrong_values += 1
                        errors.append(
                            f"thread {thread_id}: {key} held a "
                            "different value than written"
                        )
                elif response.status_code == 404:
                    evicted_reads += 1
                else:
                    errors.append(
                        f"thread {thread_id}: GET {key} returned "
                        f"{response.status_code}"
                    )
            else:
                key = keys[rng.randrange(len(keys))]
                value = f"v{thread_id}:{op}"
                if len(value) < value_size:
                    value = value + "x" * (value_size - len(value))
                started = time.perf_counter()
                response = client.put(
                    f"/cache/{key}", json={"value": value}
                )
                latencies.append(time.perf_counter() - started)
                writes += 1
                if response.status_code != 200:
                    errors.append(
                        f"thread {thread_id}: PUT {key} returned "
                        f"{response.status_code}"
                    )
                else:
                    own_writes[key] = value
    except Exception as exc:
        errors.append(f"thread {thread_id}: {exc!r}")
    counts["reads"] += reads
    counts["writes"] += writes
    counts["wrong_values"] += wrong_values
    counts["evicted_reads"] += evicted_reads


def _stats(client):
    """Hit, miss, and eviction counters from /stats, or None.

    Returns None when /stats is unreachable, so a server without
    that endpoint still gets a load test, just without a hit rate.
    """
    try:
        response = client.get("/stats")
        if response.status_code != 200:
            return None
        body = response.json()
        return {
            "hits": body["hits"],
            "misses": body["misses"],
            "evictions": body["evictions"],
            "size": body["size"],
            "capacity": body["capacity"],
        }
    except Exception:
        return None


def run_load_test(*, base_url=None, app=None, threads=8,
                  ops_per_thread=2000, read_ratio=0.8, keys=1000,
                  value_size=100, distribution="disjoint"):
    """Run one timed HTTP load test and return a result dict.

    base_url None means in process, otherwise a live server URL.
    keys is the distinct key pool, split across threads in disjoint
    mode and shared by all threads in shared mode. read_ratio is
    the share of reads, 0 for writes only and 1 for reads only
    after warmup. The result reports ops, elapsed seconds, ops per
    second, latency percentiles, reads and writes, hit rate and
    evictions from /stats, and whether any write was lost or any
    request errored. Evicted reads are reported, not counted as
    errors, since eviction is correct when the store is full.
    """
    if threads < 1 or ops_per_thread < 1:
        raise ValueError("threads and ops_per_thread must be positive")
    if keys < 1 or value_size < 1:
        raise ValueError("keys and value_size must be positive")
    if read_ratio < 0 or read_ratio > 1:
        raise ValueError("read_ratio must be between 0 and 1")
    if distribution not in DISTRIBUTIONS:
        raise ValueError("distribution must be disjoint or shared")

    if distribution == "disjoint":
        per_thread = max(1, keys // threads)
        key_pools = [
            [f"load:t{t}:k{i}" for i in range(per_thread)]
            for t in range(threads)
        ]
        warmup_keys = [key for pool in key_pools for key in pool]
    else:
        key_pools = [[f"load:k{i}" for i in range(keys)]] * threads
        warmup_keys = key_pools[0]

    warmup = _make_client(base_url, app)
    stats_before = _stats(warmup)
    for key in warmup_keys:
        response = warmup.put(
            f"/cache/{key}", json={"value": "warmup"}
        )
        if response.status_code != 200:
            warmup.close()
            raise RuntimeError(
                f"warmup PUT {key} returned {response.status_code}"
            )
    warmup.close()

    latencies = []
    errors = []
    counts = {"reads": 0, "writes": 0, "wrong_values": 0,
              "evicted_reads": 0}
    lock = threading.Lock()

    def guarded_client():
        return _make_client(base_url, app)

    def run_thread(thread_id, pool):
        client = guarded_client()
        thread_latencies = []
        thread_errors = []
        thread_counts = {"reads": 0, "writes": 0, "wrong_values": 0,
                         "evicted_reads": 0}
        _worker(client, thread_id, pool, ops_per_thread, read_ratio,
                value_size, distribution, thread_latencies,
                thread_errors, thread_counts)
        client.close()
        with lock:
            latencies.extend(thread_latencies)
            errors.extend(thread_errors)
            for name in counts:
                counts[name] += thread_counts[name]

    workers = [
        threading.Thread(target=run_thread, args=(t, key_pools[t]))
        for t in range(threads)
    ]
    started = time.perf_counter()
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    elapsed = time.perf_counter() - started

    after = _make_client(base_url, app)
    stats_after = _stats(after)
    after.close()

    ops = threads * ops_per_thread
    summary = summarize_latencies(latencies)
    hit_rate = None
    evictions = None
    if stats_before is not None and stats_after is not None:
        reads = (
            stats_after["hits"] + stats_after["misses"]
            - stats_before["hits"] - stats_before["misses"]
        )
        if reads > 0:
            hit_rate = round(
                (stats_after["hits"] - stats_before["hits"]) / reads, 4
            )
        evictions = stats_after["evictions"] - stats_before["evictions"]
    return {
        "mode": "live server" if base_url else "in-process",
        "base_url": base_url,
        "distribution": distribution,
        "threads": threads,
        "ops_per_thread": ops_per_thread,
        "ops": ops,
        "read_ratio": read_ratio,
        "value_size_bytes": value_size,
        "distinct_keys": len(warmup_keys),
        "elapsed_seconds": round(elapsed, 4),
        "ops_per_second": round(ops / elapsed, 1) if elapsed > 0 else 0,
        "latency_ms": summary,
        "reads": counts["reads"],
        "writes": counts["writes"],
        "hit_rate": hit_rate,
        "evictions": evictions,
        "errors": errors,
        "wrong_values": counts["wrong_values"],
        "evicted_reads": counts["evicted_reads"],
        "correct": not errors and counts["wrong_values"] == 0,
    }


def _report(result):
    latency = result["latency_ms"]
    print(
        f"Load test, {result['mode']}, {result['distribution']}, "
        f"{result['threads']} threads, "
        f"{result['ops_per_thread']} ops per thread, "
        f"{result['ops']} ops, "
        f"{int(result['read_ratio'] * 100)} percent reads, "
        f"{result['distinct_keys']} keys, "
        f"{result['value_size_bytes']} byte values"
    )
    print(
        f"{result['ops']} ops in {result['elapsed_seconds']}s, "
        f"{result['ops_per_second']} ops/s"
    )
    print(
        f"latency ms, mean {latency['mean_ms']}, "
        f"p50 {latency['p50_ms']}, p95 {latency['p95_ms']}, "
        f"p99 {latency['p99_ms']}, max {latency['max_ms']}"
    )
    print(
        f"reads {result['reads']}, writes {result['writes']}, "
        f"hit rate {result['hit_rate']}, "
        f"evictions {result['evictions']}"
    )
    print(
        f"errors {len(result['errors'])}, "
        f"wrong values {result['wrong_values']}, "
        f"evicted reads {result['evicted_reads']}, "
        f"{'correct' if result['correct'] else 'INCORRECT'}"
    )
    for error in result["errors"][:10]:
        print(f"  error: {error}")
    if len(result["errors"]) > 10:
        print(f"  ... and {len(result['errors']) - 10} more errors")


def main(argv=None):
    """Run the load test from the command line and print the report.

    Returns 0 when every write verified and no request errored,
    1 otherwise. -o writes the result dict as JSON as well.
    """
    parser = argparse.ArgumentParser(
        description="HTTP load test for the distributed cache."
    )
    parser.add_argument("-u", dest="base_url", default=None,
                        help="live server base URL, "
                        "default is in-process")
    parser.add_argument("-t", type=int, default=8, dest="threads",
                        help="threads, default 8")
    parser.add_argument("-n", type=int, default=2000, dest="ops",
                        help="ops per thread, default 2000")
    parser.add_argument("-r", type=float, default=0.8, dest="read_ratio",
                        help="share of reads from 0 to 1, default 0.8")
    parser.add_argument("-k", type=int, default=1000, dest="keys",
                        help="distinct keys, default 1000")
    parser.add_argument("-v", type=int, default=100, dest="value_size",
                        help="value size in bytes, default 100")
    parser.add_argument("-d", dest="distribution", default="disjoint",
                        choices=DISTRIBUTIONS,
                        help="disjoint or shared keys, "
                        "default disjoint")
    parser.add_argument("-o", dest="output", default=None,
                        help="also write the result as JSON to this file")
    args = parser.parse_args(argv)
    if args.threads < 1 or args.ops < 1:
        parser.error("threads and ops must be positive")
    if args.keys < 1 or args.value_size < 1:
        parser.error("keys and value size must be positive")
    if args.read_ratio < 0 or args.read_ratio > 1:
        parser.error("read ratio must be between 0 and 1")

    result = run_load_test(
        base_url=args.base_url,
        threads=args.threads,
        ops_per_thread=args.ops,
        read_ratio=args.read_ratio,
        keys=args.keys,
        value_size=args.value_size,
        distribution=args.distribution,
    )
    _report(result)
    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2))
        print(f"result written to {args.output}")
    return 0 if result["correct"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
