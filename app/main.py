"""Day 7, HTTP API around the KV store, with disk persistence, sharding,
a switchable LRU and LFU eviction policy, a background TTL sweeper,
stats and health endpoints, and a concurrency benchmark with a lock
review in benchmarks/concurrency.py."""
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
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
    result = store.stats()
    result["cluster"] = cluster.stats()
    return result


def readiness(store, cluster):
    """Readiness check for a store and a cluster.

    Returns a (ready, reason) pair. The cache is ready when it has at
    least one shard to route to and its background sweepers are running,
    so expired keys do not pile up. A stopped sweeper or an empty
    cluster means not ready, which the endpoint reports as a 503, not
    a crash, so a load balancer can keep traffic away until it recovers.
    It reads the cluster through stats(), never the live shard dict,
    so a topology change cannot resize the dict mid check.
    """
    cluster_stats = cluster.stats()
    if not cluster_stats["nodes"]:
        return False, "no cluster nodes available"
    if not store.sweeper_running:
        return False, "store sweeper is not running"
    if not cluster_stats["sweeper_running"]:
        return False, "a cluster shard sweeper is not running"
    return True, "ok"


@app.get("/health")
def health():
    """Liveness probe, cheap and always 200 while the process is up.

    It reports uptime and current size, so an operator can tell the
    process is alive and roughly how full it is, without waiting on
    any slow dependency. Liveness never fails on a full or busy store,
    only a dead process fails it.
    """
    stats = store.stats()
    return {
        "status": "ok",
        "uptime_seconds": stats["uptime_seconds"],
        "started_at": datetime.fromtimestamp(
            store.started_at, tz=timezone.utc
        ).isoformat(),
        "size": stats["size"],
        "capacity": stats["capacity"],
        "eviction_policy": stats["eviction_policy"],
        "sweeper_running": stats["sweeper_running"],
        "cluster_nodes": len(cluster.shards),
    }


@app.get("/health/ready")
def ready():
    """Readiness probe, 200 when the cache can serve, 503 when not.

    Readiness is stricter than liveness: a process can be alive but
    not ready to take traffic, for example when its sweeper stopped
    or its cluster has no nodes left.
    """
    ok, reason = readiness(store, cluster)
    body = {
        "status": "ready" if ok else "not_ready",
        "reason": reason,
        "cluster_nodes": len(cluster.shards),
        "sweeper_running": store.sweeper_running,
    }
    if not ok:
        return JSONResponse(status_code=503, content=body)
    return body


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

