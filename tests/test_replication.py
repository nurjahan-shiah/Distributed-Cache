"""Day 10, replication tests: primary and replica sync, failover, resync.

These tests check the guarantee a replicated shard makes: every healthy
copy holds the same keys and values, a primary failure moves reads and
writes to a replica without losing keys, and a recover copies the missed
writes back, so the copies agree again. They also check the honest
limits: a down copy misses writes until it is resynced, and a failover
with no healthy replica to serve is refused instead of pretending.
"""
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.main import app, cluster as api_cluster
from cache.cluster import ShardedStore
from cache.replication import ReplicatedShard

client = TestClient(app)


def test_replication_writes_reach_replicas():
    shard = ReplicatedShard(capacity=100, replication_factor=2)
    shard.set("a", "1")
    shard.set("b", "2")
    assert shard.replicas[0].get("a") == "1"
    assert shard.replicas[0].get("b") == "2"
    assert shard.replicas[0].stats()["size"] == 2
    assert shard.stats()["replicated_writes"] == 2
    assert shard.stats()["in_sync"] is True


def test_replication_factor_three_keeps_two_replicas():
    shard = ReplicatedShard(capacity=10, replication_factor=3)
    shard.set("a", "1")
    assert len(shard.replicas) == 2
    assert all(replica.get("a") == "1" for replica in shard.replicas)
    assert shard.stats()["replication_factor"] == 3
    assert shard.stats()["healthy_replicas"] == 2


def test_replication_reads_touch_every_copy():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("a", "1")
    assert shard.get("a") == "1"
    assert shard.primary.stats()["hits"] == 1
    assert shard.replicas[0].stats()["hits"] == 1
    assert shard.get("missing") is None
    assert shard.primary.stats()["misses"] == 1
    assert shard.replicas[0].stats()["misses"] == 1


def test_replication_factor_one_has_no_replicas():
    shard = ReplicatedShard(capacity=10, replication_factor=1)
    assert shard.replicas == []
    assert shard.set("a", "1") is True
    assert shard.get("a") == "1"
    assert shard.fail_primary() is False
    assert shard.stats()["replication_factor"] == 1
    assert shard.stats()["in_sync"] is True


def test_invalid_replication_factor():
    with pytest.raises(ValueError):
        ReplicatedShard(capacity=10, replication_factor=0)


def test_failover_serves_reads_and_writes():
    shard = ReplicatedShard(capacity=100, replication_factor=2)
    shard.set("a", "1")
    assert shard.fail_primary() is True
    assert shard.stats()["primary_down"] is True
    assert shard.stats()["failovers"] == 1
    assert shard.get("a") == "1"
    assert shard.set("b", "2") is True
    assert shard.get("b") == "2"
    assert shard.stats()["writes_while_primary_down"] == 1
    assert shard.stats()["in_sync"] is False


def test_failover_twice_is_noop():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    assert shard.fail_primary() is True
    assert shard.fail_primary() is False
    assert shard.stats()["failovers"] == 1


def test_recover_primary_resyncs_writes_and_deletes():
    shard = ReplicatedShard(capacity=100, replication_factor=2)
    shard.set("keep", "1")
    shard.set("drop", "2")
    assert shard.fail_primary() is True
    shard.set("added", "3")
    assert shard.delete("drop") is True
    assert shard.primary.get("added") is None
    assert shard.primary.get("drop") == "2"
    assert shard.recover_primary() is True
    assert shard.stats()["primary_down"] is False
    assert shard.stats()["resyncs"] == 1
    assert shard.primary.get("added") == "3"
    assert shard.primary.get("drop") is None
    assert shard.primary.get("keep") == "1"
    assert shard.stats()["in_sync"] is True
    assert shard.verify_sync() == {"replica-0": {"missing": [], "extra": [], "different": []}}


def test_recover_primary_when_up_is_noop():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    assert shard.recover_primary() is False
    assert shard.stats()["resyncs"] == 0


def test_failover_with_all_replicas_down_is_refused():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("a", "1")
    assert shard.fail_replica(0) is True
    assert shard.fail_primary() is False
    assert shard.stats()["primary_down"] is False
    assert shard.get("a") == "1"
    assert shard.recover_replica(0) is True


def test_replica_failure_skips_writes_then_resyncs():
    shard = ReplicatedShard(capacity=100, replication_factor=2)
    shard.set("before", "1")
    assert shard.fail_replica(0) is True
    assert shard.stats()["replicas_down"] == [0]
    shard.set("during", "2")
    assert shard.replicas[0].get("during") is None
    assert shard.stats()["replication_skipped"] == 1
    assert shard.stats()["in_sync"] is False
    assert shard.recover_replica(0) is True
    assert shard.replicas[0].get("during") == "2"
    assert shard.replicas[0].get("before") == "1"
    assert shard.stats()["replicas_down"] == []
    assert shard.stats()["in_sync"] is True


def test_replica_failure_twice_is_noop():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    assert shard.fail_replica(0) is True
    assert shard.fail_replica(0) is False
    assert shard.fail_replica(5) is False
    assert shard.fail_replica(-1) is False


def test_recover_replica_when_up_is_noop():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    assert shard.recover_replica(0) is False
    assert shard.recover_replica(5) is False


def test_verify_sync_reports_a_missing_key():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("shared", "1")
    shard.primary.set("sneaky", "2")
    report = shard.verify_sync()
    assert report["replica-0"]["missing"] == ["sneaky"]
    assert report["replica-0"]["extra"] == []
    assert report["replica-0"]["different"] == []


def test_verify_sync_reports_a_different_value():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("a", "1")
    shard.replicas[0].set("a", "other")
    report = shard.verify_sync()
    assert report["replica-0"]["different"] == ["a"]
    assert report["replica-0"]["missing"] == []


def test_cas_replicates_to_replicas():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("a", "1")
    assert shard.compare_and_set("a", "1", "2") is True
    assert shard.replicas[0].get("a") == "2"
    assert shard.compare_and_set("a", "1", "3") is False
    assert shard.replicas[0].get("a") == "2"
    assert shard.stats()["cas_success"] == 1
    assert shard.stats()["cas_failure"] == 1


def test_delete_replicates():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("a", "1")
    assert shard.delete("a") is True
    assert shard.replicas[0].get("a") is None
    assert shard.replicas[0].stats()["size"] == 0


def test_batch_replicates():
    shard = ReplicatedShard(capacity=100, replication_factor=2)
    assert shard.set_many({f"k{i}": str(i) for i in range(50)}) == 50
    assert shard.get_many([f"k{i}" for i in range(50)]) == {
        f"k{i}": str(i) for i in range(50)
    }
    assert shard.replicas[0].stats()["size"] == 50
    assert shard.verify_sync() == {"replica-0": {"missing": [], "extra": [], "different": []}}


def test_ttl_replicates_and_expires_on_replicas():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("short", "1", ttl_seconds=0.05)
    assert shard.replicas[0].get("short") == "1"
    time.sleep(0.07)
    assert shard.get("short") is None
    assert shard.replicas[0].get("short") is None


def test_eviction_policy_switch_reaches_every_copy():
    shard = ReplicatedShard(capacity=10, replication_factor=2, eviction_policy="lru")
    shard.set_eviction_policy("lfu")
    assert shard.primary.stats()["eviction_policy"] == "lfu"
    assert shard.replicas[0].stats()["eviction_policy"] == "lfu"
    assert shard.stats()["eviction_policy"] == "lfu"


def test_sweep_expired_sweeps_every_copy():
    shard = ReplicatedShard(capacity=10, replication_factor=2)
    shard.set("fresh", "1")
    shard.set("stale", "2", ttl_seconds=0.05)
    time.sleep(0.07)
    assert shard.sweep_expired() == 2
    assert shard.primary.stats()["size"] == 1
    assert shard.replicas[0].stats()["size"] == 1


def test_concurrent_writes_during_failover_are_not_lost():
    shard = ReplicatedShard(capacity=1000, replication_factor=2)
    errors = []
    stop = threading.Event()

    def worker():
        try:
            i = 0
            while not stop.is_set():
                shard.set(f"k{i % 100}", str(i))
                shard.get(f"k{i % 100}")
                i += 1
        except Exception as exc:
            errors.append(repr(exc))

    def flipper():
        try:
            for _ in range(10):
                shard.fail_primary()
                time.sleep(0.01)
                shard.recover_primary()
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            stop.set()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    flipper_thread = threading.Thread(target=flipper)
    for thread in threads:
        thread.start()
    flipper_thread.start()
    flipper_thread.join()
    for thread in threads:
        thread.join()
    assert errors == []
    shard.recover_primary()
    assert all(shard.get(f"k{i}") is not None for i in range(100))
    assert shard.verify_sync() == {"replica-0": {"missing": [], "extra": [], "different": []}}


def test_cluster_replication_factor_two():
    cluster = ShardedStore(
        nodes=("node-a", "node-b", "node-c"), replication_factor=2
    )
    for i in range(50):
        cluster.set(f"k{i}", str(i))
    assert all(cluster.get(f"k{i}") == str(i) for i in range(50))
    assert cluster.stats()["total_size"] == 50
    assert cluster.stats()["replication_factor"] == 2
    status = cluster.replication_status()
    assert set(status) == {"node-a", "node-b", "node-c"}
    assert all(entry["in_sync"] for entry in status.values())
    assert all(entry["replication_factor"] == 2 for entry in status.values())
    assert all(entry["verify"] == {"replica-0": {"missing": [], "extra": [], "different": []}} for entry in status.values())


def test_cluster_failover_keeps_keys():
    cluster = ShardedStore(
        nodes=("node-a", "node-b", "node-c"), replication_factor=2
    )
    owned = None
    for i in range(200):
        key = f"owner-k{i}"
        if cluster.node_for(key) == "node-a":
            owned = key
            break
    assert owned is not None
    cluster.set(owned, "kept")
    cluster.set("other", "1")
    try:
        assert cluster.fail_primary("node-a") is True
        assert cluster.get(owned) == "kept"
        assert cluster.set(owned, "kept-again") is True
        assert cluster.get(owned) == "kept-again"
        assert cluster.get("other") == "1"
        assert cluster.replication_status()["node-a"]["primary_down"] is True
    finally:
        assert cluster.recover_primary("node-a") is True
    assert cluster.get(owned) == "kept-again"
    assert cluster.replication_status()["node-a"]["in_sync"] is True


def test_cluster_replication_status_on_plain_cluster():
    cluster = ShardedStore(nodes=("node-a", "node-b"))
    cluster.set("a", "1")
    status = cluster.replication_status()
    assert status["node-a"]["replication_factor"] == 1
    assert status["node-a"]["in_sync"] is True
    assert status["node-a"]["verify"] == {}
    assert cluster.fail_primary("node-a") is False
    assert cluster.recover_primary("node-a") is False


def test_cluster_fail_primary_unknown_node():
    cluster = ShardedStore(
        nodes=("node-a", "node-b"), replication_factor=2
    )
    assert cluster.fail_primary("node-z") is False
    assert cluster.recover_primary("node-z") is False
    assert cluster.fail_replica("node-z", 0) is False


def test_cluster_invalid_replication_factor():
    with pytest.raises(ValueError):
        ShardedStore(nodes=("node-a",), replication_factor=0)


def test_api_replication_status():
    r = client.get("/cluster/replication")
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"node-a", "node-b", "node-c"}
    for entry in body.values():
        assert entry["replication_factor"] == 2
        assert entry["primary_down"] is False
        assert entry["in_sync"] is True
        assert entry["verify"] == {"replica-0": {"missing": [], "extra": [], "different": []}}


def test_api_cluster_stats_reports_replication():
    body = client.get("/cluster").json()
    assert body["replication_factor"] == 2
    assert body["per_node"]["node-a"]["replication_factor"] == 2
    assert body["per_node"]["node-a"]["primary_down"] is False


def test_api_failover_and_recover():
    owned = None
    for i in range(200):
        key = f"api-owner-k{i}"
        if api_cluster.node_for(key) == "node-a":
            owned = key
            break
    assert owned is not None
    api_cluster.set(owned, "kept")
    try:
        r = client.post("/cluster/node/node-a/fail-primary")
        assert r.status_code == 200
        assert r.json()["failover"] is True
        assert r.json()["replication"]["primary_down"] is True
        assert api_cluster.get(owned) == "kept"
        r = client.post("/cluster/node/node-a/fail-primary")
        assert r.json()["failover"] is False
        r = client.post("/cluster/node/node-a/recover-primary")
        assert r.status_code == 200
        assert r.json()["recovered"] is True
        assert r.json()["replication"]["primary_down"] is False
        assert api_cluster.get(owned) == "kept"
        assert client.get("/cluster/replication").json()["node-a"]["in_sync"] is True
    finally:
        client.post("/cluster/node/node-a/recover-primary")


def test_api_failover_unknown_node():
    r = client.post("/cluster/node/node-z/fail-primary")
    assert r.status_code == 404
    r = client.post("/cluster/node/node-z/recover-primary")
    assert r.status_code == 404


def test_api_fail_replica_bad_index():
    r = client.post("/cluster/node/node-a/fail-replica/5")
    assert r.status_code == 400
    r = client.post("/cluster/node/node-a/fail-replica/-1")
    assert r.status_code == 400
    r = client.post("/cluster/node/node-z/fail-replica/0")
    assert r.status_code == 404


def test_api_fail_and_recover_replica():
    try:
        r = client.post("/cluster/node/node-b/fail-replica/0")
        assert r.status_code == 200
        assert r.json()["changed"] is True
        assert r.json()["replication"]["replicas_down"] == [0]
        r = client.post("/cluster/node/node-b/fail-replica/0")
        assert r.json()["changed"] is False
        r = client.post("/cluster/node/node-b/recover-replica/0")
        assert r.status_code == 200
        assert r.json()["recovered"] is True
        assert r.json()["replication"]["replicas_down"] == []
        assert client.get("/cluster/replication").json()["node-b"]["in_sync"] is True
    finally:
        client.post("/cluster/node/node-b/recover-replica/0")
