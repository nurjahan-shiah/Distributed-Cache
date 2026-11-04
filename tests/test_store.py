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
