"""Thread safe key value store with switchable LRU and LFU eviction, TTL
expiry, a background sweeper that removes expired keys, an atomic
compare and set, a write ahead log for durability, and a memory
limit with per entry size tracking.

Every mutation is written to the log and fsynced before it is applied
in memory, so a crash never loses an acknowledged write. On startup,
load() reads the snapshot and then replays the log, so keys written
since the last checkpoint come back. A checkpoint writes a snapshot
and truncates the log, which bounds replay and log size.

Size tracking is an estimate, not a process memory reading. Each entry
counts its key and value as UTF-8 bytes plus a fixed overhead for the
bookkeeping around them, so the store can enforce a byte budget and
evict before the process grows without bound.
"""
import json
import threading
import time
from collections import OrderedDict
from pathlib import Path

from cache.wal import WriteAheadLog

POLICIES = ("lru", "lfu")

# Bytes charged per entry on top of its key and value, for the dict
# slot, the expiry float, the access frequency, and the tuple holding
# the value. It is a fixed estimate, not a measured RSS figure, so it
# is stable across runs and easy to reason about in a test, and it is
# reported in stats so an operator can see what it assumes.
ENTRY_OVERHEAD_BYTES = 64


class KVStore:
    def __init__(
        self,
        capacity=1000,
        snapshot_path=None,
        eviction_policy="lru",
        wal_path=None,
        wal_checkpoint_records=1000,
        max_memory_bytes=None,
    ):
        if eviction_policy not in POLICIES:
            raise ValueError("eviction_policy must be lru or lfu")
        if wal_checkpoint_records <= 0:
            raise ValueError("wal_checkpoint_records must be positive")
        if max_memory_bytes is not None and max_memory_bytes <= 0:
            raise ValueError("max_memory_bytes must be positive or None")
        self.capacity = capacity
        self.max_memory_bytes = max_memory_bytes
        self.snapshot_path = Path(snapshot_path) if snapshot_path else None
        self.eviction_policy = eviction_policy
        self.wal_path = Path(wal_path) if wal_path else None
        self.wal_checkpoint_records = wal_checkpoint_records
        self.wal = WriteAheadLog(self.wal_path) if self.wal_path else None
        self.wal_records = 0
        self.wal_replayed = 0
        self.checkpoints = 0
        if self.wal is not None:
            # A snapshot can hold a higher seq than the log file
            # shows, when the log was truncated at a checkpoint and
            # the process restarted, so new records must start above
            # the snapshot seq, or replay would skip them as already
            # snapshotted. load() repeats this after reading the
            # snapshot, this covers writes before any load() call.
            self.wal.next_seq = max(
                self.wal.next_seq, self._snapshot_wal_seq() + 1
            )
        self._data = OrderedDict()
        self._freq = {}
        # Day 13, size tracking: _sizes holds the charged bytes per
        # key, and _memory_bytes is the running total, so the store
        # never has to rescan the whole dict to know how full it is.
        # Every place that adds or removes an entry updates both, so
        # the total stays exact for the estimate it tracks.
        self._sizes = {}
        self._memory_bytes = 0
        # Day 12: a plain Lock would deadlock when a checkpoint runs
        # inside a mutation, because checkpoint snapshots while the
        # mutation already holds the lock. An RLock lets the same
        # thread re-enter, and no other thread can hold it at the
        # same time, so the lock still serializes every operation.
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expired_swept = 0
        self.sweeps_run = 0
        self.cas_success = 0
        self.cas_failure = 0
        # Day 13: how many writes left the store over its memory
        # budget because one value alone was bigger than the budget.
        # Those writes are kept, not refused, and this counts them.
        self.oversize_writes = 0
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

    @staticmethod
    def _entry_size(key, value):
        """Charged bytes for one entry, key plus value plus overhead.

        Keys and values are strings in this API, so UTF-8 length is
        the honest size. A non string value falls back to its repr,
        so a caller passing one never breaks the accounting.
        """
        key_bytes = len(key.encode("utf-8")) if isinstance(key, str) else len(repr(key).encode("utf-8"))
        if isinstance(value, str):
            value_bytes = len(value.encode("utf-8"))
        else:
            value_bytes = len(repr(value).encode("utf-8"))
        return key_bytes + value_bytes + ENTRY_OVERHEAD_BYTES

    def _record_size(self, key, value):
        """Charge an entry, replacing its old size when it exists.

        Callers hold the lock, and call this after writing _data, so
        the size map and the data map agree. An overwrite subtracts
        the old size first, so the total never double counts a key.
        """
        size = self._entry_size(key, value)
        self._memory_bytes += size - self._sizes.get(key, 0)
        self._sizes[key] = size

    def _forget_size(self, key):
        """Drop the charge for a removed entry. Callers hold the lock."""
        self._memory_bytes -= self._sizes.pop(key, 0)

    def _over_budget(self):
        """True when a memory limit is set and the store is over it."""
        return (
            self.max_memory_bytes is not None
            and self._memory_bytes > self.max_memory_bytes
        )

    @property
    def memory_bytes(self):
        """Charged bytes held now, under the lock, so a concurrent
        write cannot change the total mid read."""
        with self._lock:
            return self._memory_bytes

    def _note_oversize_write(self, keys):
        """Count a write that left the store over its memory budget.

        Callers hold the lock, and call this after eviction, so it
        only counts when eviction could not bring the store back
        under budget, which means one value alone is bigger than
        the budget. A write that fits after evicting colder keys is
        not oversize, and neither is a write LFU evicted straight
        away as the coldest key, so the check also requires at
        least one written key to still be present.
        """
        if self._over_budget() and any(key in self._data for key in keys):
            self.oversize_writes += 1

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
        """Evict until the store fits capacity and its memory budget.

        Callers hold the lock. LRU evicts the front of the ordered
        dict, the least recently used key. LFU evicts the key with
        the lowest access count, and breaks ties by least recently
        used, which is the key's position in the ordered dict. The
        LFU scan is O(n) per eviction, simple and correct at this
        scale, while a production O(1) LFU would keep frequency
        buckets instead. Memory eviction stops at one entry, so a
        single value bigger than the whole budget is kept and
        counted as an oversize write, instead of being evicted the
        moment it is written, which would make the write silently
        vanish. With no memory limit set, only the capacity rule
        applies, so a plain store behaves exactly as before.
        """
        while len(self._data) > self.capacity or (
            self._over_budget() and len(self._data) > 1
        ):
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
            self._forget_size(victim)
            self.evictions += 1

    def _append_wal(self, record):
        """Append a record to the write ahead log, if one is configured.

        Callers hold the lock, and the record is written and fsynced
        before the mutation is applied, so a crash after the fsync
        never loses the write. Returns the seq, or None with no WAL.
        Raises OSError when the write fails, and the caller must not
        apply the mutation.
        """
        if self.wal is None:
            return None
        seq = self.wal.append(record)
        self.wal_records += 1
        return seq

    def _maybe_checkpoint(self):
        """Checkpoint when the log has grown past its limit.

        Callers hold the lock, and call this after the mutation is
        applied, so the snapshot includes every logged record. The
        snapshot and the truncate happen under the same lock hold, so
        no write can slip in between them and be lost.
        """
        if self.wal is None:
            return
        if self.wal_records < self.wal_checkpoint_records:
            return
        self._checkpoint_locked()

    def _checkpoint_locked(self):
        """Snapshot and truncate the log. Callers hold the lock.

        Returns how many entries the snapshot saved.
        """
        entries = self.snapshot()
        self.wal.truncate()
        self.wal_records = 0
        self.checkpoints += 1
        return entries

    def checkpoint(self):
        """Write a snapshot and truncate the write ahead log.

        After this, the snapshot holds every key and the log is
        empty, so a restart replays nothing. Returns how many
        entries the snapshot saved. With no WAL configured this is
        just a snapshot.
        """
        with self._lock:
            if self.wal is None:
                return self.snapshot()
            return self._checkpoint_locked()

    def set(self, key, value, ttl_seconds=None):
        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        with self._lock:
            self._append_wal(
                {
                    "op": "set",
                    "key": key,
                    "value": value,
                    "expires_at": expires_at,
                }
            )
            if key in self._data:
                del self._data[key]
                self._freq[key] = self._freq.get(key, 0) + 1
            else:
                self._freq[key] = 1
            self._data[key] = (value, expires_at)
            self._record_size(key, value)
            self._evict_if_needed()
            self._note_oversize_write([key])
            self._maybe_checkpoint()

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
                self._forget_size(key)
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
                    self._forget_size(key)
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
            if pairs:
                # One record for the whole batch, so a torn write
                # drops the batch instead of half applying it.
                self._append_wal(
                    {
                        "op": "set_many",
                        "items": dict(pairs),
                        "expires_at": expires_at,
                    }
                )
            for key, value in pairs:
                if key in self._data:
                    del self._data[key]
                    self._freq[key] = self._freq.get(key, 0) + 1
                else:
                    self._freq[key] = 1
                self._data[key] = (value, expires_at)
                self._record_size(key, value)
            self._evict_if_needed()
            self._note_oversize_write([key for key, _value in pairs])
            self._maybe_checkpoint()
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
                    self._forget_size(key)
                    item = None
            if item is None:
                if expected is not None:
                    self.cas_failure += 1
                    return False
                expires_at = time.time() + ttl_seconds if ttl_seconds else None
                # A successful write is logged as a plain set, so
                # replay restores it, and a failed compare logs
                # nothing, so replay never invents a key.
                self._append_wal(
                    {
                        "op": "set",
                        "key": key,
                        "value": value,
                        "expires_at": expires_at,
                    }
                )
                self._data[key] = (value, expires_at)
                self._freq[key] = 1
                self._record_size(key, value)
                self._evict_if_needed()
                self._note_oversize_write([key])
                self.cas_success += 1
                self._maybe_checkpoint()
                return True
            _current, expires_at = item
            if _current != expected:
                self.cas_failure += 1
                return False
            if ttl_seconds:
                expires_at = time.time() + ttl_seconds
            self._append_wal(
                {
                    "op": "set",
                    "key": key,
                    "value": value,
                    "expires_at": expires_at,
                }
            )
            del self._data[key]
            self._data[key] = (value, expires_at)
            self._freq[key] = self._freq.get(key, 0) + 1
            self._record_size(key, value)
            self._evict_if_needed()
            self._note_oversize_write([key])
            self.cas_success += 1
            self._maybe_checkpoint()
            return True

    def delete(self, key):
        with self._lock:
            if key not in self._data:
                return False
            # Only a delete that removes something is logged, so the
            # log never grows on deletes of missing keys.
            self._append_wal({"op": "delete", "key": key})
            self._freq.pop(key, None)
            del self._data[key]
            self._forget_size(key)
            self._maybe_checkpoint()
            return True

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
                self._forget_size(key)
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
                    self._forget_size(key)
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
                "memory_bytes": self._memory_bytes,
                "max_memory_bytes": self.max_memory_bytes,
                "memory_used_percent": (
                    round(
                        self._memory_bytes / self.max_memory_bytes * 100, 2
                    )
                    if self.max_memory_bytes
                    else None
                ),
                "entry_overhead_bytes": ENTRY_OVERHEAD_BYTES,
                "oversize_writes": self.oversize_writes,
                "eviction_policy": self.eviction_policy,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / reads, 4) if reads else 0.0,
                "evictions": self.evictions,
                "expired_swept": self.expired_swept,
                "sweeps_run": self.sweeps_run,
                "cas_success": self.cas_success,
                "cas_failure": self.cas_failure,
                "wal_path": str(self.wal_path) if self.wal_path else None,
                "wal_records": self.wal_records,
                "wal_replayed": self.wal_replayed,
                "checkpoints": self.checkpoints,
                "sweeper_running": self.sweeper_running,
                "started_at": self.started_at,
                "uptime_seconds": round(time.time() - self.started_at, 3),
            }

    def snapshot(self):
        """Write live, unexpired entries to disk atomically.

        The copy is taken under the lock, then written to a temp file in
        the same directory and renamed over the snapshot. A crash mid
        write therefore leaves either the old snapshot or the new one,
        never a half written file. The file also records the WAL seq
        of the last record included in the snapshot, so replay after
        a load skips records the snapshot already holds. Returns how
        many entries were saved, or 0 when no snapshot path is
        configured.
        """
        if not self.snapshot_path:
            return 0
        with self._lock:
            data = {
                k: {"value": v, "expires_at": e}
                for k, (v, e) in self._data.items()
                if not self._is_expired(e)
            }
            wal_seq = self.wal.last_seq if self.wal is not None else 0
        payload = {"_snapshot_format": 2, "wal_seq": wal_seq, "entries": data}
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.snapshot_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload))
        tmp_path.replace(self.snapshot_path)
        return len(data)

    def _snapshot_wal_seq(self):
        """The WAL seq recorded in the snapshot file, best effort.

        Returns 0 when there is no snapshot, when it is corrupt, or
        when it was written before snapshots carried a seq. Used to
        keep new WAL seqs above the snapshot seq after a restart.
        """
        if not self.snapshot_path or not self.snapshot_path.exists():
            return 0
        try:
            data = json.loads(self.snapshot_path.read_text())
        except (ValueError, OSError):
            return 0
        if not isinstance(data, dict):
            return 0
        if data.get("_snapshot_format") != 2:
            return 0
        wal_seq = data.get("wal_seq", 0)
        return wal_seq if isinstance(wal_seq, int) else 0

    def _replay_wal(self, snapshot_seq):
        """Apply WAL records newer than the snapshot, in file order.

        Callers hold the lock. Records at or below the snapshot seq
        are already in the snapshot and are skipped, expired sets are
        dropped, deletes remove the key, and capacity is enforced as
        records apply. A record missing the fields its op needs is
        skipped, so a corrupt record can never crash startup. The
        log keeps the new seqs above the snapshot seq, so a later
        write is never skipped as already snapshotted. Returns how
        many records were applied.
        """
        if self.wal is None:
            return 0
        self.wal.next_seq = max(self.wal.next_seq, snapshot_seq + 1)
        records = self.wal.replay()
        applied = 0
        with self._lock:
            for seq, record in records:
                if seq <= snapshot_seq:
                    continue
                op = record.get("op")
                if op == "set":
                    if "key" not in record or "value" not in record:
                        continue
                    if self._is_expired(record.get("expires_at")):
                        continue
                    key = record["key"]
                    if key in self._data:
                        del self._data[key]
                    self._data[key] = (record["value"], record.get("expires_at"))
                    self._freq[key] = 1
                    self._record_size(key, record["value"])
                    applied += 1
                    self._evict_if_needed()
                elif op == "set_many":
                    items = record.get("items")
                    if not isinstance(items, dict):
                        continue
                    if self._is_expired(record.get("expires_at")):
                        continue
                    for key, value in items.items():
                        if key in self._data:
                            del self._data[key]
                        self._data[key] = (value, record.get("expires_at"))
                        self._freq[key] = 1
                        self._record_size(key, value)
                        applied += 1
                    self._evict_if_needed()
                elif op == "delete":
                    if "key" not in record:
                        continue
                    self._data.pop(record["key"], None)
                    self._freq.pop(record["key"], None)
                    self._forget_size(record["key"])
                    applied += 1
        self.wal_replayed = applied
        return applied

    def load(self):
        """Load a snapshot written by snapshot(), then replay the WAL.

        Expired entries are dropped, malformed entries are skipped, and
        a missing or corrupt snapshot leaves the store empty instead of
        failing startup. Capacity is enforced in the current policy's
        eviction order, so a snapshot larger than capacity still loads
        cleanly. Access frequencies are runtime state and are not stored
        in the snapshot, so loaded keys start at a frequency of 1.
        Entry sizes are recomputed the same way on load, so the
        memory total is exact after a restart without storing it.
        Snapshots written before Day 12 carry no WAL seq and load as
        holding seq 0, so every WAL record replays. Returns how many
        entries the snapshot loaded plus how many WAL records were
        replayed. Call this once at startup, before serving writes,
        so new WAL seqs start above the snapshot seq.
        """
        loaded = 0
        snapshot_seq = 0
        if self.snapshot_path and self.snapshot_path.exists():
            try:
                data = json.loads(self.snapshot_path.read_text())
            except (ValueError, OSError):
                data = None
            if isinstance(data, dict):
                if data.get("_snapshot_format") == 2 and isinstance(
                    data.get("entries"), dict
                ):
                    snapshot_seq = data.get("wal_seq", 0)
                    if not isinstance(snapshot_seq, int) or isinstance(
                        snapshot_seq, bool
                    ):
                        snapshot_seq = 0
                    entries = data["entries"]
                else:
                    entries = data
                with self._lock:
                    for key, item in entries.items():
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
                        self._record_size(key, item["value"])
                        loaded += 1
                        self._evict_if_needed()
        if self.wal is not None:
            loaded += self._replay_wal(snapshot_seq)
        return loaded
