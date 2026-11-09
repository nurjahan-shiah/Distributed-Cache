"""Day 7, concurrency benchmark and lock review.

Run it from the repo root:

    python -m benchmarks.concurrency
    python -m benchmarks.concurrency -t 16 -n 2000

It measures mixed get and set throughput for one KVStore and for a
three node ShardedStore, under a disjoint key workload, where threads
only contend on the lock, and under a hot key workload, where threads
contend on the same 21 keys. Every timed op is a set except every
fourth op, which reads the key the same thread wrote just before, so
the mix is 75 percent writes and 25 percent reads, and the exact set
of written keys is known in advance. That is what makes the
correctness check possible: after the run, every written key must
still hold the value its thread wrote, because a fast benchmark that
loses writes is worthless.

Lock review findings, recorded in the README with the numbers this
script prints on this machine: one lock guards the whole store, so
throughput flattens once threads pile onto it, the sweeper start race
and the ring lookup race are fixed, cluster topology changes are
serialized, and one write racing a migration can still be lost, which
is documented, not hidden.
"""
import argparse
import re
import sys
import threading
import time
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cache.cluster import ShardedStore  # noqa: E402
from cache.store import KVStore  # noqa: E402

HOT_KEYS = 21
VALUE_PATTERN = re.compile(r"^v\d+:\d+$")


def _worker(store, thread_id, ops_per_thread, mode, errors):
    """One thread of the timed workload, 3 sets then 1 read, repeating.

    In disjoint mode each thread writes only its own keys, so threads
    contend on the store lock but never on the same key. In hot mode
    every thread writes the same 21 keys, so threads contend on the
    lock and on the same entries. Errors are collected, never raised,
    so the report can show them instead of crashing the run.
    """
    try:
        for j in range(ops_per_thread):
            if mode == "hot":
                if j % 4 == 3:
                    store.get(f"hot:{(j + 1) % HOT_KEYS}")
                else:
                    store.set(f"hot:{j % HOT_KEYS}", f"v{thread_id}:{j}")
            else:
                if j % 4 == 3:
                    store.get(f"t{thread_id}:k{j - 1}")
                else:
                    store.set(f"t{thread_id}:k{j}", f"v{thread_id}:{j}")
    except Exception as exc:
        errors.append(f"thread {thread_id}: {exc!r}")


def _verify_disjoint(store, threads, ops_per_thread):
    """Every written key still holds the value its thread wrote.

    Keys written on a read step, every fourth op, were never set, so
    they are skipped. A missing or wrong value means a write was lost.
    """
    for i in range(threads):
        for j in range(ops_per_thread):
            if j % 4 == 3:
                continue
            if store.get(f"t{i}:k{j}") != f"v{i}:{j}":
                return False
    return True


def _verify_hot(store):
    """The hot key set is exactly the shared keys, with real values.

    Every value must match the pattern the workers wrote, so a value
    from nowhere, or a lost key, fails the check. There are 21 hot
    keys, not 20, because 20 shares a factor with the 4 step read
    pattern, so 5 of the 20 keys would only ever be read and never
    set, and the check would fail on a correct run.
    """
    entries = store.entries()
    if len(entries) != HOT_KEYS:
        return False
    return all(VALUE_PATTERN.match(value) for _key, value, _exp in entries)


def run_workload(store, *, threads=8, ops_per_thread=1000, mode="disjoint"):
    """Run one timed workload and verify it, returns a result dict.

    mode is disjoint, hot, or cluster, where cluster is the disjoint
    workload run against a ShardedStore. Capacity is the caller's job,
    give the store room for every key, or eviction will drop keys the
    check expects to find. The result reports ops, elapsed seconds,
    ops per second, any thread errors, and whether the data is correct.
    """
    if threads < 1 or ops_per_thread < 1:
        raise ValueError("threads and ops_per_thread must be positive")
    if mode not in ("disjoint", "hot", "cluster"):
        raise ValueError("mode must be disjoint, hot, or cluster")
    errors = []
    workers = [
        threading.Thread(
            target=_worker, args=(store, i, ops_per_thread, mode, errors)
        )
        for i in range(threads)
    ]
    started = time.perf_counter()
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    elapsed = time.perf_counter() - started
    ops = threads * ops_per_thread
    if mode == "hot":
        correct = _verify_hot(store)
    else:
        correct = _verify_disjoint(store, threads, ops_per_thread)
    return {
        "mode": mode,
        "threads": threads,
        "ops": ops,
        "elapsed_seconds": round(elapsed, 4),
        "ops_per_second": round(ops / elapsed, 1) if elapsed > 0 else 0,
        "errors": errors,
        "correct": correct and not errors,
    }


def _report(result):
    status = "correct" if result["correct"] else "INCORRECT"
    print(
        f"{result['mode']}, {result['threads']} threads, "
        f"{result['ops']} ops, {result['elapsed_seconds']}s, "
        f"{result['ops_per_second']} ops/s, {status}"
    )
    for error in result["errors"]:
        print(f"  error: {error}")


def main(argv=None):
    """Run the four benchmark scenarios and print the results.

    Returns 0 when every scenario is correct, 1 when any scenario
    lost data or raised an error.
    """
    parser = argparse.ArgumentParser(
        description="Concurrency benchmark for the distributed cache."
    )
    parser.add_argument("-t", type=int, default=8, dest="threads",
                        help="threads per scenario")
    parser.add_argument("-n", type=int, default=1000, dest="ops",
                        help="ops per thread per scenario")
    args = parser.parse_args(argv)
    if args.threads < 1 or args.ops < 1:
        parser.error("threads and ops must be positive")

    print("Concurrency benchmark, 75 percent writes, 25 percent reads")
    results = []
    single = KVStore(capacity=100000)
    results.append(
        run_workload(single, threads=1, ops_per_thread=args.ops, mode="disjoint")
    )
    store = KVStore(capacity=100000)
    results.append(
        run_workload(
            store, threads=args.threads, ops_per_thread=args.ops, mode="disjoint"
        )
    )
    hot = KVStore(capacity=100000)
    results.append(
        run_workload(
            hot, threads=args.threads, ops_per_thread=args.ops, mode="hot"
        )
    )
    cluster = ShardedStore(
        nodes=("node-a", "node-b", "node-c"), capacity=100000
    )
    results.append(
        run_workload(
            cluster, threads=args.threads, ops_per_thread=args.ops, mode="cluster"
        )
    )
    for result in results:
        _report(result)
    print(
        "Lock review: one lock per store, sweeper start and ring lookup "
        "races fixed, topology changes serialized, one write racing a "
        "migration can still be lost, see the README."
    )
    return 0 if all(r["correct"] for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
