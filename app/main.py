"""Day 5, HTTP API around the KV store, with disk persistence, sharding,
a switchable LRU and LFU eviction policy, and a background TTL sweeper."""
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from cache.cluster import ShardedStore
from cache.store import KVStore

app = FastAPI(title="Distributed Cache, KV Store")
store = KVStore(capacity=1000, snapshot_path=Path("data/cache_snapshot.json"))
store.load()
store.start_sweeper(interval_seconds=10)
cluster = ShardedStore(nodes=("node-a", "node-b", "node-c"), capacity=1000)
cluster.start_sweeper(interval_seconds=10)


class SetRequest(BaseModel):
    value: str
    ttl_seconds: int | None = None


class EvictionPolicyRequest(BaseModel):
    policy: str


@app.post("/config/eviction-policy")
def set_eviction_policy(req: EvictionPolicyRequest):
    try:
        store.set_eviction_policy(req.policy)
        cluster.set_eviction_policy(req.policy)
    except ValueError:
        raise HTTPException(status_code=400, detail="policy must be lru or lfu")
    return {"eviction_policy": req.policy}


@app.put("/cache/{key}")
def set_key(key: str, req: SetRequest):
    store.set(key, req.value, req.ttl_seconds)
    store.snapshot()
    return {"key": key, "value": req.value}


@app.get("/cache/{key}")
def get_key(key: str):
    value = store.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.delete("/cache/{key}")
def delete_key(key: str):
    if not store.delete(key):
        raise HTTPException(status_code=404, detail="Key not found")
    store.snapshot()
    return {"key": key, "deleted": True}


@app.get("/stats")
def stats():
    return store.stats()


@app.post("/sweep")
def sweep():
    """Run a TTL sweep on demand and report what it removed."""
    removed = store.sweep_expired()
    cluster.sweep_expired()
    return {"removed": removed, "expired_swept": store.expired_swept}


@app.get("/cluster")
def cluster_stats():
    return cluster.stats()


@app.get("/cluster/route/{key}")
def cluster_route(key: str):
    node = cluster.node_for(key)
    if node is None:
        raise HTTPException(status_code=404, detail="No nodes available")
    return {"key": key, "node": node}

