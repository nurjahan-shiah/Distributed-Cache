"""Day 7, concurrency benchmark and lock review tests.

These tests hammer the store, the ring, and the cluster from many
threads at once, and check the properties a cache must keep under
concurrency: no lost writes, no crash on a racing topology change,
exactly one sweeper after concurrent starts, and stats that add up.
"""
import random
import threading

import pytest

from benchmarks.concurrency import main as benchmark_main
from benchmarks.concurrency import run_workload
from cache.cluster import ShardedStore
from cache.ring import ConsistentHashRing
from cache.store import KVStore


def test_concurrent_disjoint_writes_are_not_lost():
    store = KVStore(capacity=100000)
    result = run_workload(store, threads=8, ops_per_thread=250, mode="disjoint")
    assert result["ops"] == 2000
    assert result["errors"] == []
    assert result["correct"] is True
    assert store.stats()["size"] == 1504


def test_concurrent_hot_key_writes_stay_consistent():
    store = KVStore(capacity=100000)
    result = run_workload(store, threads=8, ops_per_thread=200, mode="hot")
    assert result["errors"] == []
    assert result["correct"] is True
    assert store.stats()["size"] == 21


def test_concurrent_mixed_workload_stats_add_up():
    store = KVStore(capacity=100)
    gets_per_thread = 100

    def worker(seed):
        rng = random.Random(seed)
        for _ in range(100):
            store.set(f"shared:{rng.randrange(200)}", "x")
        for _ in range(gets_per_thread):
            store.get(f"shared:{rng.randrange(200)}")

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    stats = store.stats()
    assert stats["hits"] + stats["misses"] == 4 * gets_per_thread
    assert stats["size"] <= 100


def test_concurrent_sweeper_start_starts_only_one():
    store = KVStore(capacity=10)
    started = []
    barrier = threading.Barrier(8)

    def starter():
        barrier.wait()
        started.append(store.start_sweeper(interval_seconds=10))

    try:
        threads = [threading.Thread(target=starter) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert started.count(True) == 1
        assert store.sweeper_running is True
        assert store.stop_sweeper() is True
        assert store.stop_sweeper() is False
    finally:
        store.stop_sweeper()


def test_concurrent_ring_mutation_never_breaks_lookups():
    ring = ConsistentHashRing(nodes=("node-a", "node-b", "node-c"))
    errors = []
    barrier = threading.Barrier(5)

    def reader():
        try:
            barrier.wait()
            for _ in range(2000):
                node = ring.get_node("some-key")
                assert node in ("node-a", "node-b", "node-c", "node-d")
        except Exception as exc:
            errors.append(repr(exc))

    def writer():
        try:
            barrier.wait()
            for _ in range(50):
                ring.add_node("node-d")
                ring.remove_node("node-d")
        except Exception as exc:
            errors.append(repr(exc))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    threads.append(threading.Thread(target=writer))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []


def test_concurrent_cluster_add_node_keeps_keys():
    cluster = ShardedStore(nodes=("node-a", "node-b"))
    for i in range(100):
        cluster.set(f"k{i}", str(i))
    added = []
    barrier = threading.Barrier(2)

    def adder(node):
        barrier.wait()
        added.append(cluster.add_node(node))

    threads = [
        threading.Thread(target=adder, args=("node-c",)),
        threading.Thread(target=adder, args=("node-d",)),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sorted(added) == [True, True]
    assert all(cluster.get(f"k{i}") == str(i) for i in range(100))
    assert cluster.stats()["nodes"] == ["node-a", "node-b", "node-c", "node-d"]


def test_concurrent_cluster_stats_during_topology_change():
    cluster = ShardedStore(nodes=("node-a", "node-b"))
    for i in range(50):
        cluster.set(f"k{i}", str(i))
    errors = []
    stop = threading.Event()

    def reader():
        try:
            while not stop.is_set():
                cluster.stats()
        except Exception as exc:
            errors.append(repr(exc))

    def writer():
        try:
            for _ in range(20):
                cluster.add_node("node-c")
                cluster.remove_node("node-c")
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            stop.set()

    readers = [threading.Thread(target=reader) for _ in range(2)]
    writer_thread = threading.Thread(target=writer)
    for thread in readers:
        thread.start()
    writer_thread.start()
    writer_thread.join()
    for thread in readers:
        thread.join()
    assert errors == []
    assert all(cluster.get(f"k{i}") == str(i) for i in range(50))


def test_concurrent_cluster_ops_during_remove_do_not_raise():
    cluster = ShardedStore(nodes=("node-a", "node-b", "node-c"))
    for i in range(100):
        cluster.set(f"k{i}", str(i))
    errors = []
    stop = threading.Event()

    def worker():
        try:
            i = 0
            while not stop.is_set():
                cluster.set(f"k{i % 100}", "v")
                cluster.get(f"k{i % 100}")
                cluster.delete(f"gone-{i}")
                i += 1
        except Exception as exc:
            errors.append(repr(exc))

    def remover():
        try:
            for _ in range(10):
                cluster.remove_node("node-c")
                cluster.add_node("node-c")
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            stop.set()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    remover_thread = threading.Thread(target=remover)
    for thread in threads:
        thread.start()
    remover_thread.start()
    remover_thread.join()
    for thread in threads:
        thread.join()
    assert errors == []


def test_benchmark_reports_real_throughput():
    store = KVStore(capacity=100000)
    result = run_workload(store, threads=2, ops_per_thread=100, mode="disjoint")
    assert result["ops"] == 200
    assert result["elapsed_seconds"] > 0
    assert result["ops_per_second"] > 0
    assert result["correct"] is True


def test_benchmark_cluster_workload_is_correct():
    cluster = ShardedStore(nodes=("node-a", "node-b", "node-c"), capacity=100000)
    result = run_workload(cluster, threads=4, ops_per_thread=100, mode="cluster")
    assert result["errors"] == []
    assert result["correct"] is True
    assert cluster.stats()["total_size"] == 300


def test_benchmark_rejects_bad_args():
    store = KVStore(capacity=10)
    with pytest.raises(ValueError):
        run_workload(store, threads=0)
    with pytest.raises(ValueError):
        run_workload(store, ops_per_thread=0)
    with pytest.raises(ValueError):
        run_workload(store, mode="sideways")


def test_benchmark_main_returns_zero():
    assert benchmark_main(["-t", "2", "-n", "50"]) == 0
