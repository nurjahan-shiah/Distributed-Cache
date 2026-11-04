"""Day 2, HTTP API around the KV store, with disk persistence."""
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from cache.store import KVStore

app = FastAPI(title="Distributed Cache, KV Store")
store = KVStore(capacity=1000, snapshot_path=Path("data/cache_snapshot.json"))
store.load()


class SetRequest(BaseModel):
    value: str
    ttl_seconds: int | None = None


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
