"""Day 13 tests, eviction and memory limits, size tracking.

Each entry is charged its key and value as UTF-8 bytes plus a fixed
per entry overhead, so the numbers below are exact: a one byte key
and a one byte value cost 66 bytes with the 64 byte overhead. The
tests check the total moves the right way on every mutation, that
eviction kicks in on bytes as well as on key count, and that a single
value bigger than the whole budget is kept and counted, not dropped.
"""
import time

import pytest
from fastapi.testclient import TestClient

from app.main import app
from cache.cluster import ShardedStore
from cache.config import CacheConfig
from cache.replication import ReplicatedShard
from cache.store import ENTRY_OVERHEAD_BYTES, KVStore

client = TestClient(app)


def test_size_tracking_counts_key_value_and_overhead():
    s = KVStore(capacity=100)
    assert s.memory_bytes == 0
    s.set("a", "1")
    assert s.memory_bytes == 1 + 1 + ENTRY_OVERHEAD_BYTES
    s.set("k1", "v1")
    assert s.memory_bytes == (1 + 1 + 64) + (2 + 2 + 64)
    stats = s.stats()
    assert stats["memory_bytes"] == s.memory_bytes
    assert stats["max_memory_bytes"] is None
    assert stats["memory_used_percent"] is None
    assert stats["entry_overhead_bytes"] == 64
    assert stats["oversize_writes"] == 0


def test_multibyte_values_count_as_utf8_bytes():
    s = KVStore(capacity=100)
    s.set("é", "€")
    assert s.memory_bytes == 2 + 3 + ENTRY_OVERHEAD_BYTES


def test_updating_a_value_changes_the_size():
    s = KVStore(capacity=100)
    s.set("a", "1")
    assert s.memory_bytes == 66
    s.set("a", "12345")
    assert s.memory_bytes == 1 + 5 + 64
    s.set("a", "1")
    assert s.memory_bytes == 66


def test_delete_frees_memory():
    s = KVStore(capacity=100)
    s.set("a", "1")
    s.set("b", "2")
    assert s.memory_bytes == 132
    assert s.delete("a") is True
    assert s.memory_bytes == 66
    assert s.delete("missing") is False
    assert s.memory_bytes == 66


def test_expired_get_frees_memory():
    s = KVStore(capacity=100)
    s.set("a", "1", ttl_seconds=0.05)
    assert s.memory_bytes == 66
    time.sleep(0.07)
    assert s.get("a") is None
    assert s.memory_bytes == 0


def test_sweep_frees_memory():
    s = KVStore(capacity=100)
    s.set("a", "1", ttl_seconds=0.05)
    s.set("b", "2")
    time.sleep(0.07)
    assert s.sweep_expired() == 1
    assert s.memory_bytes == 66


def test_memory_eviction_lru():
    s = KVStore(capacity=100, max_memory_bytes=132)
    s.set("a", "1")
    s.set("b", "2")
    assert s.memory_bytes == 132
    s.set("c", "3")
    assert s.get("a") is None
    assert s.get("b") == "2"
    assert s.get("c") == "3"
    assert s.memory_bytes == 132
    assert s.stats()["evictions"] == 1
    assert s.stats()["memory_used_percent"] == 100.0


def test_memory_eviction_lfu_keeps_the_hot_key():
    s = KVStore(capacity=100, eviction_policy="lfu", max_memory_bytes=132)
    s.set("a", "1")
    s.set("b", "2")
    s.get("a")
    s.get("a")
    s.set("c", "3")
    assert s.get("a") == "1"
    assert s.get("b") is None
    assert s.get("c") == "3"
    assert s.memory_bytes == 132


def test_oversize_single_value_is_kept_and_counted():
    s = KVStore(capacity=100, max_memory_bytes=100)
    big = "x" * 200
    s.set("big", big)
    assert s.get("big") == big
    assert s.memory_bytes == 3 + 200 + 64
    assert s.stats()["oversize_writes"] == 1
    assert s.stats()["evictions"] == 0
    assert s.stats()["memory_used_percent"] == 267.0


def test_oversize_key_is_evicted_by_the_next_write():
    s = KVStore(capacity=100, max_memory_bytes=100)
    s.set("big", "x" * 200)
    s.set("s", "1")
    assert s.get("big") is None
    assert s.get("s") == "1"
    assert s.memory_bytes == 66
    assert s.stats()["oversize_writes"] == 1


def test_no_limit_means_no_memory_eviction():
    s = KVStore(capacity=100)
    for i in range(10):
        s.set(f"k{i}", "v")
    assert s.stats()["size"] == 10
    assert s.memory_bytes == 10 * (2 + 1 + 64)
    assert s.stats()["evictions"] == 0


def test_set_many_respects_the_memory_limit():
    s = KVStore(capacity=100, max_memory_bytes=132)
    count = s.set_many({"a": "1", "b": "2", "c": "3"})
    assert count == 3
    assert s.get("a") is None
    assert s.get("b") == "2"
    assert s.get("c") == "3"
    assert s.memory_bytes == 132


def test_capacity_and_memory_limits_both_apply():
    s = KVStore(capacity=2, max_memory_bytes=1000)
    s.set("a", "1")
    s.set("b", "2")
    s.set("c", "3")
    assert s.get("a") is None
    assert s.stats()["size"] == 2
    assert s.memory_bytes == 132


def test_memory_used_percent_tracks_the_budget():
    s = KVStore(capacity=100, max_memory_bytes=200)
    s.set("a", "1")
    assert s.stats()["memory_used_percent"] == 33.0
    s.set("b", "2")
    assert s.stats()["memory_used_percent"] == 66.0


def test_invalid_memory_limit_fails_fast():
    with pytest.raises(ValueError, match="max_memory_bytes"):
        KVStore(max_memory_bytes=0)
    with pytest.raises(ValueError, match="max_memory_bytes"):
        KVStore(max_memory_bytes=-5)


def test_cas_oversize_write_is_counted():
    s = KVStore(capacity=100, max_memory_bytes=100)
    assert s.compare_and_set("big", None, "x" * 200) is True
    assert s.get("big") == "x" * 200
    assert s.stats()["oversize_writes"] == 1


def test_cas_failure_changes_no_size():
    s = KVStore(capacity=100, max_memory_bytes=1000)
    s.set("a", "1")
    assert s.compare_and_set("a", "wrong", "2") is False
    assert s.memory_bytes == 66


def test_memory_is_recomputed_on_snapshot_load(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=100, snapshot_path=path)
    s.set("a", "1")
    s.set("b", "2")
    assert s.snapshot() == 2
    check = KVStore(capacity=100, snapshot_path=path)
    assert check.load() == 2
    assert check.memory_bytes == 132
    assert check.get("a") == "1"


def test_memory_is_recomputed_on_wal_replay(tmp_path):
    path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=100, wal_path=path)
    s.set("a", "1")
    s.set("b", "2")
    check = KVStore(capacity=100, wal_path=path)
    assert check.load() == 2
    assert check.memory_bytes == 132
    assert check.stats()["wal_replayed"] == 2


def test_snapshot_load_enforces_the_memory_limit(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=100, snapshot_path=path)
    s.set("a", "1")
    s.set("b", "2")
    s.set("c", "3")
    assert s.snapshot() == 3
    check = KVStore(capacity=100, snapshot_path=path, max_memory_bytes=132)
    assert check.load() == 3
    assert check.stats()["size"] == 2
    assert check.memory_bytes == 132


def test_cluster_memory_rollup_and_per_shard_limit():
    cluster = ShardedStore(
        nodes=("n1", "n2"),
        capacity=100,
        replication_factor=1,
        max_memory_bytes=1000,
    )
    for i in range(10):
        cluster.set(f"k{i}", "v")
    stats = cluster.stats()
    assert stats["total_size"] == 10
    assert stats["total_memory_bytes"] == 10 * (2 + 1 + 64)
    assert stats["max_memory_bytes"] == 1000
    for node in stats["per_node"].values():
        assert node["max_memory_bytes"] == 1000
        assert node["memory_bytes"] <= 1000


def test_cluster_shard_evicts_on_memory():
    cluster = ShardedStore(
        nodes=("n1",),
        capacity=100,
        replication_factor=1,
        max_memory_bytes=132,
    )
    cluster.set("a", "1")
    cluster.set("b", "2")
    cluster.set("c", "3")
    stats = cluster.stats()
    assert stats["total_size"] == 2
    assert stats["total_memory_bytes"] == 132


def test_replicated_shard_enforces_the_memory_limit():
    shard = ReplicatedShard(
        capacity=100, replication_factor=2, max_memory_bytes=132
    )
    shard.set("a", "1")
    shard.set("b", "2")
    shard.set("c", "3")
    assert shard.primary.stats()["size"] == 2
    assert shard.primary.memory_bytes == 132
    assert shard.replicas[0].stats()["size"] == 2
    assert shard.replicas[0].memory_bytes == 132
    assert shard.primary.get("a") is None
    assert shard.replicas[0].get("a") is None


def test_config_memory_limit_defaults_to_unlimited():
    cfg = CacheConfig.from_env({})
    assert cfg.max_memory_bytes == 0
    assert cfg.sources["max_memory_bytes"] == "default"


def test_config_memory_limit_from_env():
    cfg = CacheConfig.from_env({"CACHE_MAX_MEMORY_BYTES": "65536"})
    assert cfg.max_memory_bytes == 65536
    assert cfg.sources["max_memory_bytes"] == "env"
    assert cfg.to_dict()["max_memory_bytes"] == 65536


def test_config_negative_memory_limit_fails_fast():
    with pytest.raises(ValueError, match="CACHE_MAX_MEMORY_BYTES"):
        CacheConfig.from_env({"CACHE_MAX_MEMORY_BYTES": "-1"})
    with pytest.raises(ValueError, match="CACHE_MAX_MEMORY_BYTES"):
        CacheConfig.from_env({"CACHE_MAX_MEMORY_BYTES": "abc"})


def test_api_stats_reports_memory():
    r = client.get("/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["entry_overhead_bytes"] == 64
    assert isinstance(body["memory_bytes"], int)
    assert body["max_memory_bytes"] is None
    assert body["memory_used_percent"] is None
    assert isinstance(body["oversize_writes"], int)


def test_api_memory_endpoint():
    r = client.get("/memory")
    assert r.status_code == 200
    body = r.json()
    assert body["entry_overhead_bytes"] == 64
    assert isinstance(body["memory_bytes"], int)
    assert body["cluster"]["total_memory_bytes"] == sum(
        node["memory_bytes"]
        for node in body["cluster"]["per_node"].values()
    )
    assert sorted(body["cluster"]["per_node"]) == [
        "node-a",
        "node-b",
        "node-c",
    ]
    for node in body["cluster"]["per_node"].values():
        assert isinstance(node["memory_bytes"], int)
        assert node["max_memory_bytes"] is None
