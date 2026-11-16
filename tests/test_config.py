"""Day 11 tests, config from environment variables.

The config is built from a plain dict, so these tests pass a fake env
instead of touching os.environ, except for the API tests, which check
the live app's config matches the config it started with.
"""
import pytest
from fastapi.testclient import TestClient

from app.main import app, cluster as api_cluster, config as api_config, store as api_store
from cache.cluster import ShardedStore
from cache.config import CacheConfig, load_config

client = TestClient(app)


def test_defaults_when_env_is_empty():
    cfg = CacheConfig.from_env({})
    assert cfg.capacity == 1000
    assert cfg.eviction_policy == "lru"
    assert cfg.snapshot_path == "data/cache_snapshot.json"
    assert cfg.sweep_interval_seconds == 10
    assert cfg.nodes == ("node-a", "node-b", "node-c")
    assert cfg.replication_factor == 2
    assert cfg.ring_replicas == 100
    assert cfg.max_batch_size == 1000
    assert cfg.wal_path == "data/cache_wal.jsonl"
    assert cfg.wal_checkpoint_records == 1000
    assert set(cfg.sources.values()) == {"default"}


def test_env_overrides_each_value():
    cfg = CacheConfig.from_env(
        {
            "CACHE_CAPACITY": "50",
            "CACHE_EVICTION_POLICY": "lfu",
            "CACHE_SNAPSHOT_PATH": "data/other.json",
            "CACHE_SWEEP_INTERVAL_SECONDS": "30",
            "CACHE_NODES": "node-x, node-y",
            "CACHE_REPLICATION_FACTOR": "3",
            "CACHE_RING_REPLICAS": "10",
            "CACHE_MAX_BATCH_SIZE": "25",
            "CACHE_WAL_PATH": "data/wal-test.jsonl",
            "CACHE_WAL_CHECKPOINT_RECORDS": "5",
        }
    )
    assert cfg.capacity == 50
    assert cfg.eviction_policy == "lfu"
    assert cfg.snapshot_path == "data/other.json"
    assert cfg.sweep_interval_seconds == 30
    assert cfg.nodes == ("node-x", "node-y")
    assert cfg.replication_factor == 3
    assert cfg.ring_replicas == 10
    assert cfg.max_batch_size == 25
    assert cfg.wal_path == "data/wal-test.jsonl"
    assert cfg.wal_checkpoint_records == 5
    assert set(cfg.sources.values()) == {"env"}


def test_empty_string_counts_as_unset():
    cfg = CacheConfig.from_env({"CACHE_CAPACITY": "", "CACHE_NODES": ""})
    assert cfg.capacity == 1000
    assert cfg.nodes == ("node-a", "node-b", "node-c")
    assert cfg.sources["capacity"] == "default"


def test_unknown_cache_vars_are_ignored():
    cfg = CacheConfig.from_env({"CACHE_TYPO_VAR": "5", "CACHE_CAPACITY": "7"})
    assert cfg.capacity == 7


def test_non_integer_capacity_fails_fast():
    with pytest.raises(ValueError, match="CACHE_CAPACITY"):
        CacheConfig.from_env({"CACHE_CAPACITY": "abc"})


def test_zero_or_negative_capacity_fails_fast():
    with pytest.raises(ValueError, match="CACHE_CAPACITY"):
        CacheConfig.from_env({"CACHE_CAPACITY": "0"})
    with pytest.raises(ValueError, match="CACHE_CAPACITY"):
        CacheConfig.from_env({"CACHE_CAPACITY": "-5"})


def test_non_positive_sweep_interval_fails_fast():
    with pytest.raises(ValueError, match="CACHE_SWEEP_INTERVAL_SECONDS"):
        CacheConfig.from_env({"CACHE_SWEEP_INTERVAL_SECONDS": "0"})


def test_non_positive_replication_factor_fails_fast():
    with pytest.raises(ValueError, match="CACHE_REPLICATION_FACTOR"):
        CacheConfig.from_env({"CACHE_REPLICATION_FACTOR": "0"})


def test_non_positive_wal_checkpoint_records_fails_fast():
    with pytest.raises(ValueError, match="CACHE_WAL_CHECKPOINT_RECORDS"):
        CacheConfig.from_env({"CACHE_WAL_CHECKPOINT_RECORDS": "0"})
    with pytest.raises(ValueError, match="CACHE_WAL_CHECKPOINT_RECORDS"):
        CacheConfig.from_env({"CACHE_WAL_CHECKPOINT_RECORDS": "abc"})


def test_unknown_policy_fails_fast():
    with pytest.raises(ValueError, match="CACHE_EVICTION_POLICY"):
        CacheConfig.from_env({"CACHE_EVICTION_POLICY": "random"})


def test_nodes_must_list_at_least_one():
    with pytest.raises(ValueError, match="CACHE_NODES"):
        CacheConfig.from_env({"CACHE_NODES": " , , "})


def test_to_dict_is_json_friendly_and_reports_sources():
    cfg = CacheConfig.from_env({"CACHE_CAPACITY": "42"})
    result = cfg.to_dict()
    assert result["capacity"] == 42
    assert result["nodes"] == ["node-a", "node-b", "node-c"]
    assert result["sources"]["capacity"] == "env"
    assert result["sources"]["eviction_policy"] == "default"


def test_load_config_is_the_same_builder():
    assert load_config({"CACHE_CAPACITY": "9"}).capacity == 9


def test_config_is_frozen_after_startup():
    cfg = CacheConfig.from_env({})
    with pytest.raises(AttributeError):
        cfg.capacity = 5


def test_cluster_built_from_config_uses_its_values():
    cfg = CacheConfig.from_env(
        {
            "CACHE_CAPACITY": "2",
            "CACHE_NODES": "n1,n2",
            "CACHE_REPLICATION_FACTOR": "1",
        }
    )
    cluster = ShardedStore(
        nodes=cfg.nodes,
        capacity=cfg.capacity,
        replicas=cfg.ring_replicas,
        eviction_policy=cfg.eviction_policy,
        replication_factor=cfg.replication_factor,
    )
    assert sorted(cluster.shards) == ["n1", "n2"]
    assert cluster.capacity == 2
    assert cluster.replication_factor == 1


def test_capacity_from_config_is_enforced():
    cfg = CacheConfig.from_env({"CACHE_CAPACITY": "2"})
    cluster = ShardedStore(
        nodes=cfg.nodes,
        capacity=cfg.capacity,
        replication_factor=1,
    )
    for i in range(10):
        cluster.set(f"k{i}", str(i))
    total = cluster.stats()["total_size"]
    assert total <= 2 * len(cfg.nodes)


def test_api_config_endpoint_matches_startup_config():
    r = client.get("/config")
    assert r.status_code == 200
    assert r.json() == api_config.to_dict()


def test_api_store_and_cluster_built_from_config():
    assert api_store.capacity == api_config.capacity
    assert api_store.eviction_policy == api_config.eviction_policy
    assert str(api_store.snapshot_path) == api_config.snapshot_path
    assert api_cluster.capacity == api_config.capacity
    assert api_cluster.replication_factor == api_config.replication_factor
    assert sorted(api_cluster.shards) == sorted(api_config.nodes)
