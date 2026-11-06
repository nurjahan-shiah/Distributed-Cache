"""Thread safe key value store with switchable LRU and LFU eviction, TTL expiry."""
import json
import threading
import time
from collections import OrderedDict
from pathlib import Path

POLICIES = ("lru", "lfu")


class KVStore:
    def __init__(self, capacity=1000, snapshot_path=None, eviction_policy="lru"):
        if eviction_policy not in POLICIES:
            raise ValueError("eviction_policy must be lru or lfu")
        self.capacity = capacity
        self.snapshot_path = Path(snapshot_path) if snapshot_path else None
        self.eviction_policy = eviction_policy
        self._data = OrderedDict()
        self._freq = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def _is_expired(self, expires_at):
        return expires_at is not None and expires_at <= time.time()

    def set_eviction_policy(self, policy):
        """Switch the eviction policy at runtime.

        Frequencies are tracked in both modes, so switching to LFU uses
        the access history already recorded, and switching back to LRU
        just stops consulting it. Raises ValueError on an unknown policy.
        """
        if policy not in POLICIES:
            raise ValueError("eviction_policy must be lru or lfu")
        with self._lock:
            self.eviction_policy = policy

    def _evict_if_needed(self):
        """Evict until the store fits capacity. Callers hold the lock.

        LRU evicts the front of the ordered dict, the least recently
        used key. LFU evicts the key with the lowest access count, and
        breaks ties by least recently used, which is the key's position
        in the ordered dict. The LFU scan is O(n) per eviction, simple
        and correct at this scale, while a production O(1) LFU would
        keep frequency buckets instead.
        """
        while len(self._data) > self.capacity:
            if self.eviction_policy == "lfu":
                victim = min(
                    enumerate(self._data),
                    key=lambda pair: (self._freq.get(pair[1], 1), pair[0]),
                )[1]
                del self._data[victim]
                self._freq.pop(victim, None)
            else:
                victim, _value = self._data.popitem(last=False)
                self._freq.pop(victim, None)
            self.evictions += 1

    def set(self, key, value, ttl_seconds=None):
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        with self._lock:
            if key in self._data:
                del self._data[key]
                self._freq[key] = self._freq.get(key, 0) + 1
            else:
                self._freq[key] = 1
            self._data[key] = (value, expires_at)
            self._evict_if_needed()

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if item is None:
                self.misses += 1
                return None
            value, expires_at = item
            if self._is_expired(expires_at):
                del self._data[key]
                self._freq.pop(key, None)
                self.misses += 1
                return None
            self._data.move_to_end(key)
            self._freq[key] = self._freq.get(key, 0) + 1
            self.hits += 1
            return value

    def delete(self, key):
        with self._lock:
            self._freq.pop(key, None)
            return self._data.pop(key, None) is not None

    def entries(self):
        """Snapshot of live entries as (key, value, expires_at) triples.

        Expired entries are dropped. Used by the sharded cluster on
        Day 3 to move keys when a node joins or leaves.
        """
        with self._lock:
            live = []
            for key, (value, expires_at) in list(self._data.items()):
                if self._is_expired(expires_at):
                    del self._data[key]
                    self._freq.pop(key, None)
                else:
                    live.append((key, value, expires_at))
            return live

    def stats(self):
        with self._lock:
            return {
                "size": len(self._data),
                "capacity": self.capacity,
                "eviction_policy": self.eviction_policy,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
            }

    def snapshot(self):
        """Write live, unexpired entries to disk atomically.

        The copy is taken under the lock, then written to a temp file in
        the same directory and renamed over the snapshot. A crash mid
        write therefore leaves either the old snapshot or the new one,
        never a half written file. Returns how many entries were saved,
        or 0 when no snapshot path is configured.
        """
        if not self.snapshot_path:
            return 0
        with self._lock:
            data = {
                k: {"value": v, "expires_at": e}
                for k, (v, e) in self._data.items()
                if not self._is_expired(e)
            }
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.snapshot_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(data))
        tmp_path.replace(self.snapshot_path)
        return len(data)

    def load(self):
        """Load a snapshot written by snapshot(), if one exists.

        Expired entries are dropped, malformed entries are skipped, and
        a missing or corrupt snapshot leaves the store empty instead of
        failing startup. Capacity is enforced in the current policy's
        eviction order, so a snapshot larger than capacity still loads
        cleanly. Access frequencies are runtime state and are not stored
        in the snapshot, so loaded keys start at a frequency of 1.
        Returns how many entries were loaded.
        """
        if not self.snapshot_path or not self.snapshot_path.exists():
            return 0
        try:
            data = json.loads(self.snapshot_path.read_text())
        except (ValueError, OSError):
            return 0
        if not isinstance(data, dict):
            return 0
        loaded = 0
        with self._lock:
            for key, item in data.items():
                if not isinstance(item, dict):
                    continue
                if "value" not in item or "expires_at" not in item:
                    continue
                if self._is_expired(item["expires_at"]):
                    continue
                if key in self._data:
                    del self._data[key]
                self._data[key] = (item["value"], item["expires_at"])
                self._freq[key] = 1
                loaded += 1
                self._evict_if_needed()
        return loaded
