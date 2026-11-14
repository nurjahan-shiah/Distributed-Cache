"""Day 10, replication: one primary plus replica copies, kept in sync.

Each shard is a primary KVStore plus one or more replica KVStores.
Every operation, read or write, applies to every healthy copy, so the
copies stay identical, including LRU order and access frequencies,
which is what keeps eviction identical on all of them. A write returns
only after every healthy copy has it, so there is no replication lag
to reason about, and a read never sees a stale value from a replica.

Failure is simulated, not faked with real processes: fail_primary
marks the primary down, and reads and writes move to the first healthy
replica, which is the failover. While the primary is down, writes go
to the serving replica and the primary is flagged out of sync, and
recover_primary copies every live entry back to the primary and brings
it back into service, which is the resync. The same pair exists per
replica: fail_replica takes one replica out, writes skip it and count
as skipped, and recover_replica resyncs it from the serving copy.

The copies are identical only while they are all healthy. A copy that
is down misses writes, so it is out of sync until it is resynced, and
verify_sync compares the serving copy against every healthy copy and
reports missing, extra, and different keys, so a test or an operator
can check the invariant instead of trusting it.

Tradeoffs, stated plainly: replicating reads costs one store op per
copy per read, which is slow compared to reading one copy, and it is
not how production systems usually replicate. Production caches
replicate writes only and accept replica lag and eviction drift, so a
failover there can turn a hit into a miss. This shard chooses the
simple, exact form, so the sync invariant is easy to test, and the
cost is visible in the design instead of hidden.
"""
import threading
import time

from cache.store import KVStore


class ReplicatedShard:
    def __init__(self, capacity=1000, replication_factor=2, eviction_policy="lru"):
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        self.capacity = capacity
        self.replication_factor = replication_factor
        self.eviction_policy = eviction_policy
        # The primary and the replicas are public attributes on purpose,
        # so tests and operators can inspect one copy directly instead
        # of guessing what it holds from the outside.
        self.primary = KVStore(capacity=capacity, eviction_policy=eviction_policy)
        self.replicas = [
            KVStore(capacity=capacity, eviction_policy=eviction_policy)
            for _ in range(replication_factor - 1)
        ]
        self._copies = [self.primary] + self.replicas
        # _down and _out_of_sync line up with _copies, index 0 is the
        # primary, index i + 1 is replicas[i]. A copy is down when it
        # is unreachable, and out of sync when it missed a write or a
        # replicated write failed on it, so it needs a resync.
        self._down = [False] * len(self._copies)
        self._out_of_sync = [False] * len(self._copies)
        self._lock = threading.Lock()
        self.replicated_writes = 0
        self.replication_skipped = 0
        self.writes_while_primary_down = 0
        self.failovers = 0
        self.resyncs = 0

    def _serving(self):
        """The first healthy copy, primary first, or None.

        Callers hold the shard lock. Reads and writes go here, and
        when the primary is down this is the first healthy replica,
        which is the whole failover: nothing else changes, the serving
        copy just moves down the list.
        """
        for copy, down in zip(self._copies, self._down):
            if not down:
                return copy
        return None

    def _is_down(self, copy):
        return self._down[self._copies.index(copy)]

    def _flag_out_of_sync(self, copy):
        self._out_of_sync[self._copies.index(copy)] = True

    def _replicate_write(self, serving, apply):
        """Apply a write that landed on serving to every other copy.

        apply is a function taking a store. Copies that are down are
        skipped and counted, and a copy whose apply returns False has
        diverged, so it is flagged out of sync and will need a resync.
        set, delete, and set_many report nothing, so only a compare
        and set can report a failure here. Callers hold the shard lock.
        """
        for copy in self._copies:
            if copy is serving:
                continue
            if self._is_down(copy):
                if copy is self.primary:
                    self._out_of_sync[0] = True
                else:
                    self.replication_skipped += 1
                continue
            if apply(copy) is False:
                self._flag_out_of_sync(copy)
            else:
                self.replicated_writes += 1

    def _replicate_read(self, serving, apply):
        """Apply a read that landed on serving to every healthy copy.

        Reads replicate too, so a read moves the key to the end of the
        LRU order and bumps its frequency on every copy, which keeps
        eviction identical across copies. Copies that are down are
        left alone. Callers hold the shard lock.
        """
        for copy in self._copies:
            if copy is serving or self._is_down(copy):
                continue
            apply(copy)

    def _note_primary_down_write(self, serving):
        """Record a write that landed while the primary was down.

        Callers hold the shard lock. The write went to a replica, so
        the primary missed it and is out of sync until recover.
        """
        if serving is not self.primary:
            self.writes_while_primary_down += 1
            self._out_of_sync[0] = True

    def _resync_store(self, target, source):
        """Copy every live entry from source to target, exactly.

        Keys the target holds that the source does not are deleted
        first, so deletes that happened while the target was down are
        honoured, then the source entries are set in LRU order, which
        keeps the target's order the same as the source. Remaining TTL
        is recomputed from the absolute expiry, so a key keeps its
        original expiry instead of gaining a fresh lease. Access
        frequencies are not copied, same as a snapshot load, so
        resynced keys start at a frequency of 1. Callers hold the
        shard lock.
        """
        source_entries = source.entries()
        source_keys = {key for key, _value, _expires_at in source_entries}
        for key, _value, _expires_at in target.entries():
            if key not in source_keys:
                target.delete(key)
        for key, value, expires_at in source_entries:
            ttl = None
            if expires_at is not None:
                ttl = expires_at - time.time()
                if ttl <= 0:
                    continue
            target.set(key, value, ttl_seconds=ttl)

    def set(self, key, value, ttl_seconds=None):
        with self._lock:
            serving = self._serving()
            if serving is None:
                return False
            serving.set(key, value, ttl_seconds)
            self._note_primary_down_write(serving)
            self._replicate_write(
                serving, lambda copy: copy.set(key, value, ttl_seconds)
            )
            return True

    def get(self, key):
        with self._lock:
            serving = self._serving()
            if serving is None:
                return None
            value = serving.get(key)
            self._replicate_read(serving, lambda copy: copy.get(key))
            return value

    def delete(self, key):
        with self._lock:
            serving = self._serving()
            if serving is None:
                return False
            deleted = serving.delete(key)
            self._note_primary_down_write(serving)
            self._replicate_write(serving, lambda copy: copy.delete(key))
            return deleted

    def get_many(self, keys):
        """Get many keys, reading the serving copy and the replicas.

        Returns what the serving copy found, missing keys are left
        out, and the read replicates to every healthy copy, so their
        recency and frequency stay in step with the serving copy.
        """
        with self._lock:
            serving = self._serving()
            if serving is None:
                return {}
            found = serving.get_many(keys)
            self._replicate_read(serving, lambda copy: copy.get_many(keys))
            return found

    def set_many(self, items, ttl_seconds=None):
        """Set many keys on the serving copy, then on every replica.

        items is a dict of key to value, or an iterable of (key,
        value) pairs. Returns how many keys were set, or 0 when no
        copy is healthy enough to serve.
        """
        pairs = list(items.items()) if isinstance(items, dict) else list(items)
        with self._lock:
            serving = self._serving()
            if serving is None:
                return 0
            count = serving.set_many(pairs, ttl_seconds)
            self._note_primary_down_write(serving)
            self._replicate_write(
                serving, lambda copy: copy.set_many(pairs, ttl_seconds)
            )
            return count

    def compare_and_set(self, key, expected, value, ttl_seconds=None):
        """Compare and set on the serving copy, then on every replica.

        The compare and the write stay atomic on the serving copy,
        under its lock, and the winning write replicates as the same
        compare and set, which keeps the old expiry on the replicas
        when no new TTL is given, because the copies hold the same
        state. A replica whose compare fails has diverged and is
        flagged out of sync. Returns False when the compare failed
        or when no copy is healthy enough to serve.
        """
        with self._lock:
            serving = self._serving()
            if serving is None:
                return False
            ok = serving.compare_and_set(key, expected, value, ttl_seconds)
            if not ok:
                return False
            self._note_primary_down_write(serving)
            self._replicate_write(
                serving,
                lambda copy: copy.compare_and_set(
                    key, expected, value, ttl_seconds
                ),
            )
            return True

    def entries(self):
        """Live entries of the serving copy, as (key, value, expires_at).

        Expired entries are dropped. Used by the cluster to move keys
        when a node joins or leaves, and by resync to copy a copy.
        """
        with self._lock:
            serving = self._serving()
            if serving is None:
                return []
            return serving.entries()

    def set_eviction_policy(self, policy):
        """Switch every copy, healthy or down, to the given policy.

        A down copy keeps the new policy too, so it is uniform when
        it recovers, and a resync does not silently change it back.
        """
        with self._lock:
            for copy in self._copies:
                copy.set_eviction_policy(policy)
            self.eviction_policy = policy

    def sweep_expired(self):
        """Sweep expired keys on every copy, returns the total removed."""
        with self._lock:
            return sum(copy.sweep_expired() for copy in self._copies)

    def start_sweeper(self, interval_seconds=10):
        """Start a background sweeper on every copy.

        Returns True when at least one copy started a new sweeper.
        """
        with self._lock:
            started = [
                copy.start_sweeper(interval_seconds) for copy in self._copies
            ]
            return any(started)

    def stop_sweeper(self):
        """Stop the background sweeper on every copy.

        Returns True when at least one copy stopped a running sweeper.
        """
        with self._lock:
            stopped = [copy.stop_sweeper() for copy in self._copies]
            return any(stopped)

    def fail_primary(self):
        """Mark the primary down and fail over to the first replica.

        Returns False when there is nothing to fail over to, because
        there are no replicas, when the primary is already down, or
        when every replica is down too, so no copy could serve. The
        primary keeps its data, it is just unreachable, and it comes
        back through recover_primary.
        """
        with self._lock:
            if len(self.replicas) == 0 or self._down[0]:
                return False
            if all(self._down[1:]):
                return False
            self._down[0] = True
            self.failovers += 1
            return True

    def recover_primary(self):
        """Bring the primary back and resync it from the serving copy.

        Returns False when the primary is not down, or when no
        replica is healthy enough to copy from. A successful recover
        clears the down and out of sync flags, so the shard reports
        in sync again once the copies match.
        """
        with self._lock:
            if not self._down[0]:
                return False
            serving = self._serving()
            if serving is None:
                return False
            self._resync_store(self.primary, serving)
            self._down[0] = False
            self._out_of_sync[0] = False
            self.resyncs += 1
            return True

    def fail_replica(self, index):
        """Mark one replica down, so writes skip it.

        Returns False when the index is out of range or the replica
        is already down. The replica keeps its data, it just stops
        receiving writes, so it is out of sync until it recovers.
        """
        with self._lock:
            if index < 0 or index >= len(self.replicas):
                return False
            if self._down[index + 1]:
                return False
            self._down[index + 1] = True
            return True

    def recover_replica(self, index):
        """Bring one replica back and resync it from the serving copy.

        Returns False when the index is out of range, when the replica
        is not down, or when no copy is healthy enough to copy from.
        """
        with self._lock:
            if index < 0 or index >= len(self.replicas):
                return False
            if not self._down[index + 1]:
                return False
            serving = self._serving()
            if serving is None:
                return False
            self._resync_store(self.replicas[index], serving)
            self._down[index + 1] = False
            self._out_of_sync[index + 1] = False
            self.resyncs += 1
            return True

    def verify_sync(self):
        """Compare every healthy copy against the serving copy.

        Returns a dict keyed by copy name, primary or replica-N, with
        the keys missing on that copy, the keys extra on it, and the
        keys whose values differ. Down copies are left out, they are
        already known to be out of sync. An empty report for every
        copy means the copies agree.
        """
        with self._lock:
            serving = self._serving()
            if serving is None:
                return {}
            reference = {key: value for key, value, _e in serving.entries()}
            report = {}
            for index, copy in enumerate(self._copies):
                if copy is serving or self._down[index]:
                    continue
                entries = {key: value for key, value, _e in copy.entries()}
                label = "primary" if index == 0 else f"replica-{index - 1}"
                report[label] = {
                    "missing": sorted(set(reference) - set(entries)),
                    "extra": sorted(set(entries) - set(reference)),
                    "different": sorted(
                        key
                        for key in set(reference) & set(entries)
                        if reference[key] != entries[key]
                    ),
                }
            return report

    def replication_stats(self):
        """Replication counters and state, without the store stats.

        in_sync is flag based, not a content comparison: it is True
        only when no copy is down and no copy is flagged out of sync.
        verify_sync is the content check that proves it.
        """
        with self._lock:
            return self._replication_stats_locked()

    def _replication_stats_locked(self):
        """Replication stats, callers hold the shard lock."""
        return {
            "replication_factor": self.replication_factor,
            "primary_down": self._down[0],
            "replicas_down": [
                index
                for index, down in enumerate(self._down[1:])
                if down
            ],
            "healthy_replicas": sum(
                1 for down in self._down[1:] if not down
            ),
            "replicated_writes": self.replicated_writes,
            "replication_skipped": self.replication_skipped,
            "writes_while_primary_down": self.writes_while_primary_down,
            "failovers": self.failovers,
            "resyncs": self.resyncs,
            "in_sync": not any(self._down)
            and not any(self._out_of_sync),
        }

    def stats(self):
        """Serving copy stats plus the replication state.

        Hits, misses, size, and evictions come from the serving copy,
        which is the copy clients actually read, so the numbers match
        what a caller observed. When no copy can serve, the primary's
        stats are reported instead, so the endpoint still answers.
        """
        with self._lock:
            serving = self._serving()
            base = (serving if serving is not None else self.primary).stats()
            base.update(self._replication_stats_locked())
            return base
