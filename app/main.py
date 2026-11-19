"""Day 13, HTTP API around the KV store, with disk persistence, sharding,
a switchable LRU and LFU eviction policy, a background TTL sweeper,
stats and health endpoints, batch get and set endpoints, a compare
and set endpoint, replication with primary and replica sync on the
cluster, config from environment variables, a write ahead log for
durability with a checkpoint endpoint, eviction and memory limits
with per entry size tracking, and a concurrency benchmark with a
lock review in benchmarks/concurrency.py, and a load test script
with real benchmark numbers in benchmarks/load_test.py."""
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from cache.cluster import ShardedStore
from cache.config import load_config
from cache.store import KVStore

app = FastAPI(title="Distributed Cache, KV Store")
config = load_config()
store = KVStore(
    capacity=config.capacity,
    snapshot_path=Path(config.snapshot_path),
    eviction_policy=config.eviction_policy,
    wal_path=Path(config.wal_path),
    wal_checkpoint_records=config.wal_checkpoint_records,
    max_memory_bytes=config.max_memory_bytes or None,
)
store.load()
store.start_sweeper(interval_seconds=config.sweep_interval_seconds)
cluster = ShardedStore(
    nodes=config.nodes,
    capacity=config.capacity,
    replicas=config.ring_replicas,
    eviction_policy=config.eviction_policy,
    replication_factor=config.replication_factor,
    max_memory_bytes=config.max_memory_bytes or None,
)
cluster.start_sweeper(interval_seconds=config.sweep_interval_seconds)


class SetRequest(BaseModel):
    value: str
    ttl_seconds: int | None = None


class EvictionPolicyRequest(BaseModel):
    policy: str


MAX_BATCH_SIZE = config.max_batch_size


class BatchGetRequest(BaseModel):
    keys: list[str]


class BatchSetRequest(BaseModel):
    items: dict[str, str]
    ttl_seconds: int | None = None


class CompareAndSetRequest(BaseModel):
    key: str
    expected: str | None = None
    value: str
    ttl_seconds: int | None = None


@app.post("/cache/cas")
def compare_and_set(req: CompareAndSetRequest):
    """Set a key only when its current value matches expected.

    expected=None means the key must be missing, so this also
    creates a key only when nothing is there. A failed compare is
    a 200 with success false, not an error, because the caller
    asked a question and got an answer. A successful write is
    logged to the write ahead log by the store, so it survives a
    restart, and a failed compare changed nothing.
    """
    success = store.compare_and_set(
        req.key, req.expected, req.value, req.ttl_seconds
    )
    return {"key": req.key, "success": success}


@app.post("/cache/batch/get")
def batch_get(req: BatchGetRequest):
    """Get many keys in one request.

    Returns the values that were found, plus the keys that missed,
    so the caller can tell a miss from a key it never asked for.
    The batch is capped at MAX_BATCH_SIZE keys, so one huge request
    cannot hold the store lock while the caller waits on it.
    """
    if len(req.keys) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"batch is limited to {MAX_BATCH_SIZE} keys",
        )
    values = store.get_many(req.keys)
    missing = [key for key in dict.fromkeys(req.keys) if key not in values]
    return {"values": values, "found": len(values), "missing": missing}


@app.post("/cache/batch/set")
def batch_set(req: BatchSetRequest):
    """Set many keys in one request, logged as one WAL record.

    Every key gets the same TTL. The batch is logged before it is
    applied, so a crash cannot half apply it, and it is capped at
    MAX_BATCH_SIZE keys.
    """
    if len(req.items) > MAX_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=f"batch is limited to {MAX_BATCH_SIZE} keys",
        )
    count = store.set_many(req.items, req.ttl_seconds)
    return {"set": count, "keys": list(req.items)}


@app.post("/checkpoint")
def checkpoint():
    """Snapshot the store and truncate the write ahead log.

    After this, the snapshot holds every key and the log is empty,
    so a restart replays nothing. The log also checkpoints itself
    after CACHE_WAL_CHECKPOINT_RECORDS writes, so this endpoint is
    for an operator who wants a clean snapshot on demand.
    """
    entries = store.checkpoint()
    return {
        "entries": entries,
        "checkpoints": store.checkpoints,
        "wal_records": store.wal_records,
    }


@app.post("/config/eviction-policy")
def set_eviction_policy(req: EvictionPolicyRequest):
    try:
        store.set_eviction_policy(req.policy)
        cluster.set_eviction_policy(req.policy)
    except ValueError:
        raise HTTPException(status_code=400, detail="policy must be lru or lfu")
    return {"eviction_policy": req.policy}


@app.get("/config")
def get_config():
    """The effective config this process started with.

    Values come from the environment or from the defaults, and the
    sources map says which, so an operator can check what is actually
    running without guessing which variable took effect. Config is
    read once at startup, so changing a variable needs a restart.
    """
    return config.to_dict()


@app.put("/cache/{key}")
def set_key(key: str, req: SetRequest):
    store.set(key, req.value, req.ttl_seconds)
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
    return {"key": key, "deleted": True}


@app.get("/stats")
def stats():
    result = store.stats()
    result["cluster"] = cluster.stats()
    return result


@app.get("/memory")
def memory():
    """Memory use for the store and for the cluster, in one place.

    Sizes are the charged bytes the store tracks, key plus value as
    UTF-8 plus the per entry overhead, not a process RSS reading, so
    the numbers match what the memory limit enforces. The limit is
    per store and per shard, so the cluster total is the sum across
    nodes, and the per node detail lives in /stats under cluster.
    memory_used_percent is null when no limit is set.
    """
    stats = store.stats()
    cluster_stats = cluster.stats()
    return {
        "size": stats["size"],
        "capacity": stats["capacity"],
        "memory_bytes": stats["memory_bytes"],
        "max_memory_bytes": stats["max_memory_bytes"],
        "memory_used_percent": stats["memory_used_percent"],
        "entry_overhead_bytes": stats["entry_overhead_bytes"],
        "evictions": stats["evictions"],
        "oversize_writes": stats["oversize_writes"],
        "cluster": {
            "nodes": cluster_stats["nodes"],
            "total_size": cluster_stats["total_size"],
            "total_memory_bytes": cluster_stats["total_memory_bytes"],
            "max_memory_bytes": cluster_stats["max_memory_bytes"],
            "per_node": {
                name: {
                    "size": node["size"],
                    "memory_bytes": node["memory_bytes"],
                    "max_memory_bytes": node["max_memory_bytes"],
                    "memory_used_percent": node["memory_used_percent"],
                }
                for name, node in cluster_stats["per_node"].items()
            },
        },
    }


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


@app.get("/cluster/replication")
def cluster_replication():
    """Replication state for every node, plus a sync check per node.

    Each node reports whether its primary is down, which replicas
    are down, how many writes replicated or were skipped, and
    whether its copies agree, with the verify detail showing any
    missing, extra, or different keys per copy.
    """
    return cluster.replication_status()


@app.post("/cluster/node/{node}/fail-primary")
def failover_primary(node: str):
    """Simulate a primary failure on one node and fail over.

    Reads and writes for that node's keys move to its first healthy
    replica. Returns failover false when the node is not replicated,
    its primary is already down, or no replica is healthy enough to
    serve, so the caller knows nothing changed.
    """
    if node not in cluster.shards:
        raise HTTPException(status_code=404, detail="Unknown node")
    changed = cluster.fail_primary(node)
    return {
        "node": node,
        "failover": changed,
        "replication": cluster.replication_status()[node],
    }


@app.post("/cluster/node/{node}/recover-primary")
def recover_primary_endpoint(node: str):
    """Recover one node's primary and resync it from its replica.

    The primary copies every live entry from the serving replica,
    including writes that landed while it was down, and takes over
    serving again. Returns recovered false when the primary was not
    down or no replica could serve as the resync source.
    """
    if node not in cluster.shards:
        raise HTTPException(status_code=404, detail="Unknown node")
    changed = cluster.recover_primary(node)
    return {
        "node": node,
        "recovered": changed,
        "replication": cluster.replication_status()[node],
    }


@app.post("/cluster/node/{node}/fail-replica/{index}")
def failover_replica(node: str, index: int):
    """Simulate one replica failing on one node, so writes skip it.

    The replica keeps its data but stops receiving writes, and the
    skipped writes are counted, until it is recovered and resynced.
    """
    if node not in cluster.shards:
        raise HTTPException(status_code=404, detail="Unknown node")
    shard = cluster.shards[node]
    if index < 0 or index >= len(getattr(shard, "replicas", [])):
        raise HTTPException(status_code=400, detail="Replica index out of range")
    changed = cluster.fail_replica(node, index)
    return {
        "node": node,
        "replica": index,
        "changed": changed,
        "replication": cluster.replication_status()[node],
    }


@app.post("/cluster/node/{node}/recover-replica/{index}")
def recover_replica_endpoint(node: str, index: int):
    """Recover one replica on one node and resync it from the primary.

    The replica copies every live entry from the serving copy and
    starts receiving writes again.
    """
    if node not in cluster.shards:
        raise HTTPException(status_code=404, detail="Unknown node")
    shard = cluster.shards[node]
    if index < 0 or index >= len(getattr(shard, "replicas", [])):
        raise HTTPException(status_code=400, detail="Replica index out of range")
    changed = cluster.recover_replica(node, index)
    return {
        "node": node,
        "replica": index,
        "recovered": changed,
        "replication": cluster.replication_status()[node],
    }


@app.get("/cluster")
def cluster_stats():
    return cluster.stats()


@app.get("/cluster/route/{key}")
def cluster_route(key: str):
    node = cluster.node_for(key)
    if node is None:
        raise HTTPException(status_code=404, detail="No nodes available")
    return {"key": key, "node": node}

