import threading
import time

from fastapi.testclient import TestClient

from app.main import app
from cache.store import KVStore

client = TestClient(app)


def test_set_get_delete():
    s = KVStore(capacity=10)
    s.set("a", "1")
    assert s.get("a") == "1"
    assert s.delete("a") is True
    assert s.get("a") is None


def test_lru_eviction():
    s = KVStore(capacity=2)
    s.set("a", "1")
    s.set("b", "2")
    s.get("a")
    s.set("c", "3")
    assert s.get("b") is None
    assert s.get("a") == "1"


def test_ttl_expiry():
    s = KVStore(capacity=10)
    s.set("a", "1", ttl_seconds=0.05)
    assert s.get("a") == "1"
    time.sleep(0.07)
    assert s.get("a") is None


def test_thread_safety():
    s = KVStore(capacity=1000)
    threads = [
        threading.Thread(target=lambda i=i: s.set(f"k{i}", str(i)))
        for i in range(100)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert s.stats()["size"] == 100


def test_api():
    r = client.put("/cache/hello", json={"value": "world"})
    assert r.status_code == 200
    r = client.get("/cache/hello")
    assert r.json()["value"] == "world"
    assert client.get("/cache/nope").status_code == 404


def test_snapshot_and_load_roundtrip(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=10, snapshot_path=path)
    s.set("a", "1")
    s.set("b", "2")
    assert s.snapshot() == 2
    assert path.exists()
    restarted = KVStore(capacity=10, snapshot_path=path)
    assert restarted.load() == 2
    assert restarted.get("a") == "1"
    assert restarted.get("b") == "2"


def test_snapshot_preserves_lru_order(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=3, snapshot_path=path)
    s.set("a", "1")
    s.set("b", "2")
    s.set("c", "3")
    s.get("a")
    s.snapshot()
    restarted = KVStore(capacity=3, snapshot_path=path)
    restarted.load()
    restarted.set("d", "4")
    assert restarted.get("b") is None
    assert restarted.get("a") == "1"


def test_snapshot_skips_expired(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=10, snapshot_path=path)
    s.set("fresh", "1")
    s.set("stale", "2", ttl_seconds=0.05)
    time.sleep(0.07)
    assert s.snapshot() == 1
    restarted = KVStore(capacity=10, snapshot_path=path)
    assert restarted.load() == 1
    assert restarted.get("stale") is None


def test_snapshot_leaves_no_temp_file(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=10, snapshot_path=path)
    s.set("a", "1")
    s.snapshot()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["snap.json"]


def test_load_missing_file_is_noop(tmp_path):
    s = KVStore(capacity=10, snapshot_path=tmp_path / "missing.json")
    assert s.load() == 0
    assert s.stats()["size"] == 0


def test_load_corrupt_file_is_noop(tmp_path):
    path = tmp_path / "snap.json"
    path.write_text("not json at all")
    s = KVStore(capacity=10, snapshot_path=path)
    assert s.load() == 0
    assert s.stats()["size"] == 0


def test_load_enforces_capacity(tmp_path):
    path = tmp_path / "snap.json"
    s = KVStore(capacity=10, snapshot_path=path)
    s.set("a", "1")
    s.set("b", "2")
    s.set("c", "3")
    s.snapshot()
    small = KVStore(capacity=2, snapshot_path=path)
    small.load()
    assert small.stats()["size"] == 2
    assert small.get("a") is None
    assert small.get("c") == "3"


def test_api_store_has_snapshot_path():
    from app.main import store as api_store

    assert api_store.snapshot_path is not None


def test_ring_routes_stably():
    from cache.ring import ConsistentHashRing

    ring = ConsistentHashRing(nodes=("node-a", "node-b", "node-c"))
    assert ring.get_node("hello") == ring.get_node("hello")
    assert ring.get_node("hello") in ("node-a", "node-b", "node-c")


def test_ring_spreads_keys_across_nodes():
    from cache.ring import ConsistentHashRing

    ring = ConsistentHashRing(nodes=("node-a", "node-b", "node-c"))
    owners = {ring.get_node(f"key{i}") for i in range(500)}
    assert owners == {"node-a", "node-b", "node-c"}


def test_ring_add_node_moves_only_a_fraction():
    from cache.ring import ConsistentHashRing

    ring = ConsistentHashRing(nodes=("node-a", "node-b", "node-c"))
    keys = [f"key{i}" for i in range(1000)]
    before = {k: ring.get_node(k) for k in keys}
    ring.add_node("node-d")
    moved = sum(1 for k in keys if ring.get_node(k) != before[k])
    assert 0 < moved < 500


def test_ring_remove_node_reassigns_its_keys():
    from cache.ring import ConsistentHashRing

    ring = ConsistentHashRing(nodes=("node-a", "node-b", "node-c"))
    keys = [f"key{i}" for i in range(500)]
    doomed = {k for k in keys if ring.get_node(k) == "node-b"}
    assert doomed
    ring.remove_node("node-b")
    assert all(ring.get_node(k) in ("node-a", "node-c") for k in doomed)
    assert ring.get_node("anything") is not None


def test_ring_empty_returns_none():
    from cache.ring import ConsistentHashRing

    assert ConsistentHashRing().get_node("x") is None


def test_cluster_routes_and_retrieves():
    from cache.cluster import ShardedStore

    cluster = ShardedStore(nodes=("node-a", "node-b"))
    assert cluster.set("hello", "world") is True
    assert cluster.get("hello") == "world"
    assert cluster.delete("hello") is True
    assert cluster.get("hello") is None


def test_cluster_add_node_keeps_keys():
    from cache.cluster import ShardedStore

    cluster = ShardedStore(nodes=("node-a", "node-b"))
    for i in range(100):
        cluster.set(f"k{i}", str(i))
    cluster.add_node("node-c")
    assert all(cluster.get(f"k{i}") == str(i) for i in range(100))
    assert cluster.stats()["total_size"] == 100


def test_cluster_remove_node_keeps_keys():
    from cache.cluster import ShardedStore

    cluster = ShardedStore(nodes=("node-a", "node-b", "node-c"))
    for i in range(100):
        cluster.set(f"k{i}", str(i))
    cluster.remove_node("node-b")
    assert all(cluster.get(f"k{i}") == str(i) for i in range(100))
    assert cluster.stats()["nodes"] == ["node-a", "node-c"]


def test_cluster_api():
    r = client.get("/cluster")
    assert r.status_code == 200
    assert r.json()["nodes"] == ["node-a", "node-b", "node-c"]
    r = client.get("/cluster/route/hello")
    assert r.json()["node"] in ("node-a", "node-b", "node-c")
