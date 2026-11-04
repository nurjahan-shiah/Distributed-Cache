"""Day 1, thread safe key value store with LRU eviction and TTL expiry."""
import json
import threading
import time
from collections import OrderedDict
from pathlib import Path


class KVStore:
    def __init__(self, capacity=1000, snapshot_path=None):
        self.capacity = capacity
        self.snapshot_path = Path(snapshot_path) if snapshot_path else None
        self._data = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _is_expired(self, expires_at):
        return expires_at is not None and expires_at <= time.time()

    def set(self, key, value, ttl_seconds=None):
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        with self._lock:
            if key in self._data:
                del self._data[key]
            self._data[key] = (value, expires_at)
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)
                self.evictions += 1

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item is None:
                self.misses += 1
                return None
            value, expires_at = item
            if self._is_expired(expires_at):
                del self._data[key]
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self.hits += 1
            return value

    def delete(self, key):
        with self._lock:
            return self._data.pop(key, None) is not None

    def stats(self):
        with self._lock:
            return {
                "size": len(self._data),
                "capacity": self.capacity,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }

    def snapshot(self):
        if not self.snapshot_path:
            return
        with self._lock:
            data = {
                k: {"value": v, "expires_at": e}
                for k, (v, e) in self._data.items()
                if not self._is_expired(e)
            }
        self.snapshot_path.write_text(json.dumps(data))

    def load(self):
        if not self.snapshot_path or not self.snapshot_path.exists():
            return
        data = json.loads(self.snapshot_path.read_text())
        with self._lock:
            for key, item in data.items():
                if not self._is_expired(item["expires_at"]):
                    self._data[key] = (item["value"], item["expires_at"])
