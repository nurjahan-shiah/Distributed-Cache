"""Thread safe key value store with switchable LRU and LFU eviction, TTL
expiry, a background sweeper that removes expired keys, and an atomic
compare and set."""
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
        self.expired_swept = 0
        self.sweeps_run = 0
        self.cas_success = 0
        self.cas_failure = 0
        self.started_at = time.time()
        self._sweeper_thread = None
        self._sweeper_stop = None
        # Day 7 lock review: start and stop used to check and replace the
        # sweeper thread with no lock, so two threads calling start at
        # once could both pass the alive check and start two sweepers,
        # leaking one that stop could never reach. This lock serializes
        # the check and replace, so exactly one sweeper can start.
        self._sweeper_lock = threading.Lock()

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

    def get_many(self, keys):
        """Get many keys at once, under one lock hold.

        Returns a dict of the keys that were present and not
        expired, in the order they were requested. Missing and
        expired keys are left out, and expired keys are dropped,
        just like a single get. Each found key counts as a hit and
        each missing key as a miss, so stats stay consistent with
        single gets. A repeated key in the request is read once.
        """
        found = {}
        with self._lock:
            for key in dict.fromkeys(keys):
                item = self._data.get(key)
                if item is None:
                    self.misses += 1
                    continue
                value, expires_at = item
                if self._is_expired(expires_at):
                    del self._data[key]
                    self._freq.pop(key, None)
                    self.misses += 1
                    continue
                self._data.move_to_end(key)
                self._freq[key] = self._freq.get(key, 0) + 1
                self.hits += 1
                found[key] = value
        return found

    def set_many(self, items, ttl_seconds=None):
        """Set many keys at once, under one lock hold.

        items is a dict of key to value, or an iterable of (key,
        value) pairs. Every key gets the same TTL, and frequencies
        update the same way as a single set, so LRU and LFU behave
        the same as setting the keys one by one. Eviction runs once
        at the end, not once per key, so the batch pays one eviction
        pass instead of one per key. Returns how many keys were set.
        """
        pairs = list(items.items()) if isinstance(items, dict) else list(items)
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        with self._lock:
            for key, value in pairs:
                if key in self._data:
                    del self._data[key]
                    self._freq[key] = self._freq.get(key, 0) + 1
                else:
                    self._freq[key] = 1
                self._data[key] = (value, expires_at)
            self._evict_if_needed()
        return len(pairs)

    def compare_and_set(self, key, expected, value, ttl_seconds=None):
        """Atomically set a key only when its current value matches.

        The compare and the write happen under one lock hold, so no
        other thread can slip a write in between, which is what makes
        this safe for locks, leader election, and counters built on
        retries. expected=None means the key must be missing, so it
        creates the key only when nothing is there, and an expired key
        counts as missing, so a stale lock can be reclaimed. On a
        match, the value is replaced, and the TTL is replaced only
        when ttl_seconds is given, otherwise the old expiry is kept.
        A failed compare changes nothing, not even recency or
        frequency, so a failed attempt cannot keep a cold key alive.
        Frequencies update like a set on success. Returns True when
        the value was written, False when the compare failed. A
        successful write follows the same eviction rules as set, so
        on a full LFU store a brand new cold key can still be evicted
        right away, which is the admission tradeoff recorded on Day 4.
        """
        with self._lock:
            item = self._data.get(key)
            if item is not None:
                _current, expires_at = item
                if self._is_expired(expires_at):
                    del self._data[key]
                    self._freq.pop(key, None)
                    item = None
            if item is None:
                if expected is not None:
                    self.cas_failure += 1
                    return False
                expires_at = time.time() + ttl_seconds if ttl_seconds else None
                self._data[key] = (value, expires_at)
                self._freq[key] = 1
                self._evict_if_needed()
                self.cas_success += 1
                return True
            _current, expires_at = item
            if _current != expected:
                self.cas_failure += 1
                return False
            if ttl_seconds:
                expires_at = time.time() + ttl_seconds
            del self._data[key]
            self._data[key] = (value, expires_at)
            self._freq[key] = self._freq.get(key, 0) + 1
            self._evict_if_needed()
            self.cas_success += 1
            return True

    def delete(self, key):
        with self._lock:
            self._freq.pop(key, None)
            return self._data.pop(key, None) is not None

    def sweep_expired(self):
        """Remove every expired entry in one pass under the lock.

        Lazy expiry on get already keeps reads correct, but without a
        sweep, keys that are never read again sit in memory until the
        store fills up. A sweep reclaims that space proactively. It
        counts as a sweep run, and the keys it removes are counted in
        expired_swept, not in misses, since no read failed. Returns how
        many entries were removed.
        """
        with self._lock:
            now = time.time()
            expired = [
                key
                for key, (_value, expires_at) in self._data.items()
                if expires_at is not None and expires_at <= now
            ]
            for key in expired:
                del self._data[key]
                self._freq.pop(key, None)
            self.expired_swept += len(expired)
            self.sweeps_run += 1
            return len(expired)

    def start_sweeper(self, interval_seconds=10):
        """Start a daemon thread that sweeps expired keys on an interval.

        The thread waits on an event, so stop_sweeper wakes it at once
        instead of waiting out the interval, and being a daemon thread
        it never blocks process exit. Starting twice is a no-op and
        returns False, the interval must be positive. Returns True when
        a new sweeper thread was started. The start check and the thread
        creation hold the sweeper lock, so two threads starting at once
        cannot both start a sweeper.
        """
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        with self._sweeper_lock:
            if self._sweeper_thread is not None and self._sweeper_thread.is_alive():
                return False
            stop = threading.Event()
            thread = threading.Thread(
                target=self._sweeper_loop,
                args=(interval_seconds, stop),
                daemon=True,
            )
            self._sweeper_stop = stop
            self._sweeper_thread = thread
            thread.start()
            return True

    def stop_sweeper(self):
        """Stop the background sweeper, if one is running.

        Returns True when a running sweeper was stopped, False when
        none was running. The thread reference is cleared under the
        sweeper lock, then the stop event is set and the thread is
        joined outside the lock, so a second stop racing this one sees
        no thread and returns False instead of joining twice.
        """
        with self._sweeper_lock:
            if self._sweeper_thread is None or not self._sweeper_thread.is_alive():
                return False
            stop = self._sweeper_stop
            thread = self._sweeper_thread
            self._sweeper_thread = None
            self._sweeper_stop = None
        stop.set()
        thread.join(timeout=2)
        return True

    def _sweeper_loop(self, interval_seconds, stop):
        while not stop.wait(interval_seconds):
            self.sweep_expired()

    @property
    def sweeper_running(self):
        """True when a sweeper thread is alive.

        The check takes the sweeper lock, so it never reads a thread
        reference half replaced by a racing start or stop. stats() calls
        this while holding the store lock, which is safe, because the
        sweeper lock is never held while taking the store lock, so the
        two locks always nest in the same order and cannot deadlock.
        """
        with self._sweeper_lock:
            return self._sweeper_thread is not None and self._sweeper_thread.is_alive()

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
        """Counters plus derived health figures.

        hit_rate is hits over all reads, 0.0 before any read, so a fresh
        store does not divide by zero. uptime_seconds is wall clock time
        since this store was created, and started_at is the same moment
        as an epoch timestamp.
        """
        with self._lock:
            reads = self.hits + self.misses
            return {
                "size": len(self._data),
                "capacity": self.capacity,
                "eviction_policy": self.eviction_policy,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / reads, 4) if reads else 0.0,
                "evictions": self.evictions,
                "expired_swept": self.expired_swept,
                "sweeps_run": self.sweeps_run,
                "cas_success": self.cas_success,
                "cas_failure": self.cas_failure,
                "sweeper_running": self.sweeper_running,
                "started_at": self.started_at,
                "uptime_seconds": round(time.time() - self.started_at, 3),
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
