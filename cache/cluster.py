"""Day 3, sharded store: keys route to nodes through a hash ring.

Each node is a plain KVStore. The cluster owns the ring, so a caller
never picks a node directly. When a node joins or leaves, keys whose
owner changes are moved to their new owner, so a topology change does
not silently lose keys.

Day 10, replication: a node can be a ReplicatedShard instead of a
plain KVStore, when the cluster is built with a replication factor
above 1. Each node then holds a primary plus replica copies, so an
unplanned primary failure on one node no longer loses its keys, the
reads and writes fail over to a replica on the same node. A whole node
disappearing without a clean remove still loses its keys, since the
copies live on that node, and fixing that needs copies on other nodes,
which is a later step.

Day 7 lock review: three races were fixed here. Topology changes now
hold one topology lock, so two add or remove calls cannot migrate at
the same time and lose each other's keys. Methods that loop over the
shards snapshot the shard dict under that lock first, so a topology
change cannot resize the dict mid loop and raise RuntimeError. Routing
looks up the shard under the same lock, so a remove cannot pop the
shard between the ring lookup and the dict lookup and raise KeyError.
One race is left on purpose and recorded in the README: a write that
races a migration can land on a shard after migration already moved
its keys, so a production cluster would quiesce writes during a
topology change, or route by versioned epochs, instead of this.
"""
import threading
import time

from cache.replication import ReplicatedShard
from cache.ring import ConsistentHashRing
from cache.store import KVStore


class ShardedStore:
    def __init__(
        self,
        nodes=(),
        capacity=1000,
        replicas=100,
        eviction_policy="lru",
        replication_factor=1,
        max_memory_bytes=None,
    ):
        if replication_factor < 1:
            raise ValueError("replication_factor must be at least 1")
        self.capacity = capacity
        self.max_memory_bytes = max_memory_bytes
        self.eviction_policy = eviction_policy
        self.replication_factor = replication_factor
        self.ring = ConsistentHashRing(replicas=replicas)
        self.shards = {}
        self._topology_lock = threading.Lock()
        for node in nodes:
            self.add_node(node)

    def _make_shard(self):
        """Build one node's store, replicated when the factor is above 1.

        A factor of 1 keeps the plain KVStore, so an unreplicated
        cluster behaves exactly as it did before Day 10. A factor
        above 1 builds a ReplicatedShard, one primary plus replicas,
        behind the same method calls, so the rest of the cluster code
        does not care which kind of shard it is talking to. The
        memory limit is per shard, not per cluster, so each node
        enforces its own budget, the same way capacity already works.
        """
        if self.replication_factor > 1:
            return ReplicatedShard(
                capacity=self.capacity,
                replication_factor=self.replication_factor,
                eviction_policy=self.eviction_policy,
                max_memory_bytes=self.max_memory_bytes,
            )
        return KVStore(
            capacity=self.capacity,
            eviction_policy=self.eviction_policy,
            max_memory_bytes=self.max_memory_bytes,
        )

    def node_for(self, key):
        return self.ring.get_node(key)

    def _shard_for(self, key):
        """The shard that owns a key, or None when there is no node.

        The ring lookup and the shard dict lookup happen under the
        topology lock, so a remove cannot pop the shard in between.
        The shard itself is returned, so the caller can use it after
        the lock is released, and a later remove only detaches it.
        """
        with self._topology_lock:
            node = self.ring.get_node(key)
            if node is None:
                return None
            return self.shards.get(node)

    def _shards_snapshot(self):
        """A copy of the shard list, taken under the topology lock.

        Iterating the live dict while a topology change adds or removes
        a shard raises RuntimeError, so every method that loops over
        the shards works from this snapshot instead.
        """
        with self._topology_lock:
            return list(self.shards.items())

    def _migrate_all(self):
        """Move every key whose ring owner changed to its new owner.

        Callers hold the topology lock, so no second migration can run
        at the same time, and no shard is added or removed mid move.
        """
        all_entries = []
        for shard in self.shards.values():
            all_entries.extend(shard.entries())
        for shard in self.shards.values():
            for key, _value, _expires_at in shard.entries():
                shard.delete(key)
        for key, value, expires_at in all_entries:
            node = self.ring.get_node(key)
            if node is None:
                continue
            ttl = None
            if expires_at is not None:
                ttl = expires_at - time.time()
                if ttl <= 0:
                    continue
            self.shards[node].set(key, value, ttl_seconds=ttl)

    def set_eviction_policy(self, policy):
        """Switch every shard to the given policy, lru or lfu."""
        for _name, shard in self._shards_snapshot():
            shard.set_eviction_policy(policy)
        self.eviction_policy = policy

    def sweep_expired(self):
        """Sweep expired keys on every shard, returns the total removed."""
        return sum(shard.sweep_expired() for _name, shard in self._shards_snapshot())

    def start_sweeper(self, interval_seconds=10):
        """Start a background sweeper on every shard.

        Returns True when at least one shard started a new sweeper.
        """
        started = [
            shard.start_sweeper(interval_seconds)
            for _name, shard in self._shards_snapshot()
        ]
        return any(started)

    def stop_sweeper(self):
        """Stop the background sweeper on every shard.

        Returns True when at least one shard stopped a running sweeper.
        """
        stopped = [
            shard.stop_sweeper() for _name, shard in self._shards_snapshot()
        ]
        return any(stopped)

    def add_node(self, node):
        with self._topology_lock:
            if node in self.shards:
                return False
            self.shards[node] = self._make_shard()
            self.ring.add_node(node)
            self._migrate_all()
            return True

    def remove_node(self, node):
        with self._topology_lock:
            if node not in self.shards:
                return False
            doomed = self.shards.pop(node)
            self.ring.remove_node(node)
            entries = doomed.entries()
            for key, value, expires_at in entries:
                owner = self.ring.get_node(key)
                if owner is None:
                    continue
                ttl = None
                if expires_at is not None:
                    ttl = expires_at - time.time()
                    if ttl <= 0:
                        continue
                self.shards[owner].set(key, value, ttl_seconds=ttl)
            return True

    def set(self, key, value, ttl_seconds=None):
        shard = self._shard_for(key)
        if shard is None:
            return False
        shard.set(key, value, ttl_seconds)
        return True

    def get(self, key):
        shard = self._shard_for(key)
        if shard is None:
            return None
        return shard.get(key)

    def delete(self, key):
        shard = self._shard_for(key)
        if shard is None:
            return False
        return shard.delete(key)

    def get_many(self, keys):
        """Get many keys, grouped by shard, one call per shard.

        Keys are grouped by their owning shard first, so a batch
        that spans the cluster still makes one call per shard
        instead of one call per key. Returns a dict of the keys
        that were found, missing keys are left out, and keys with
        no shard are skipped.
        """
        groups = []
        for key in dict.fromkeys(keys):
            shard = self._shard_for(key)
            if shard is None:
                continue
            for group_shard, group_keys in groups:
                if group_shard is shard:
                    group_keys.append(key)
                    break
            else:
                groups.append([shard, [key]])
        found = {}
        for shard, shard_keys in groups:
            found.update(shard.get_many(shard_keys))
        return found

    def set_many(self, items, ttl_seconds=None):
        """Set many keys, grouped by shard, one call per shard.

        items is a dict of key to value, or an iterable of (key,
        value) pairs. Every key gets the same TTL. Returns how many
        keys were set, which is less than the batch size when the
        cluster has no nodes to route some keys to.
        """
        pairs = list(items.items()) if isinstance(items, dict) else list(items)
        groups = []
        for key, value in pairs:
            shard = self._shard_for(key)
            if shard is None:
                continue
            for group_shard, group_pairs in groups:
                if group_shard is shard:
                    group_pairs.append((key, value))
                    break
            else:
                groups.append([shard, [(key, value)]])
        return sum(
            shard.set_many(shard_pairs, ttl_seconds)
            for shard, shard_pairs in groups
        )

    def compare_and_set(self, key, expected, value, ttl_seconds=None):
        """Compare and set on the shard that owns the key.

        Routing is the same as get and set, so the compare and the
        write land on one shard, under that shard's lock, and stay
        atomic. Returns False when the cluster has no nodes to route
        to, otherwise the shard's result.
        """
        shard = self._shard_for(key)
        if shard is None:
            return False
        return shard.compare_and_set(key, expected, value, ttl_seconds)

    def _replicated_shard(self, node):
        """The ReplicatedShard for a node, or None.

        Returns None when the node is unknown or when the cluster
        runs with a replication factor of 1, so its shards are plain
        KVStores with no primary to fail over. Callers hold the
        topology lock only when they need a stable lookup, this
        helper takes it itself.
        """
        if self.replication_factor <= 1:
            return None
        with self._topology_lock:
            shard = self.shards.get(node)
        if not isinstance(shard, ReplicatedShard):
            return None
        return shard

    def fail_primary(self, node):
        """Fail over one node's primary to its first replica.

        Returns False when the node is unknown, when the cluster is
        not replicated, or when the shard refuses the failover,
        because its primary is already down or no replica is healthy.
        """
        shard = self._replicated_shard(node)
        if shard is None:
            return False
        return shard.fail_primary()

    def recover_primary(self, node):
        """Recover one node's primary and resync it from its replica.

        Returns False when the node is unknown, when the cluster is
        not replicated, or when the shard refuses the recover,
        because its primary is not down or no replica can serve.
        """
        shard = self._replicated_shard(node)
        if shard is None:
            return False
        return shard.recover_primary()

    def fail_replica(self, node, index):
        """Mark one replica down on one node, so writes skip it.

        Returns False when the node is unknown, when the cluster is
        not replicated, or when the shard refuses, because the index
        is out of range or the replica is already down.
        """
        shard = self._replicated_shard(node)
        if shard is None:
            return False
        return shard.fail_replica(index)

    def recover_replica(self, node, index):
        """Recover one replica on one node and resync it.

        Returns False when the node is unknown, when the cluster is
        not replicated, or when the shard refuses, because the index
        is out of range or the replica is not down.
        """
        shard = self._replicated_shard(node)
        if shard is None:
            return False
        return shard.recover_replica(index)

    def replication_status(self):
        """Replication state for every node, plus a sync check.

        Replicated nodes report their counters and the result of
        verify_sync, which compares each healthy copy against the
        serving copy. Unreplicated nodes report a factor of 1 and an
        empty verify, since a single copy is trivially in sync.
        """
        status = {}
        for name, shard in self._shards_snapshot():
            if isinstance(shard, ReplicatedShard):
                entry = shard.replication_stats()
                entry["verify"] = shard.verify_sync()
            else:
                entry = {
                    "replication_factor": 1,
                    "primary_down": False,
                    "replicas_down": [],
                    "healthy_replicas": 0,
                    "replicated_writes": 0,
                    "replication_skipped": 0,
                    "writes_while_primary_down": 0,
                    "failovers": 0,
                    "resyncs": 0,
                    "in_sync": True,
                    "verify": {},
                }
            status[name] = entry
        return status

    def stats(self):
        snapshot = self._shards_snapshot()
        per_node = {name: shard.stats() for name, shard in snapshot}
        total_hits = sum(s["hits"] for s in per_node.values())
        total_misses = sum(s["misses"] for s in per_node.values())
        reads = total_hits + total_misses
        return {
            "nodes": sorted(per_node),
            "replication_factor": self.replication_factor,
            "total_size": sum(s["size"] for s in per_node.values()),
            "total_memory_bytes": sum(
                s["memory_bytes"] for s in per_node.values()
            ),
            "max_memory_bytes": self.max_memory_bytes,
            "total_hits": total_hits,
            "total_misses": total_misses,
            "hit_rate": round(total_hits / reads, 4) if reads else 0.0,
            "total_cas_success": sum(s["cas_success"] for s in per_node.values()),
            "total_cas_failure": sum(s["cas_failure"] for s in per_node.values()),
            "sweeper_running": bool(per_node)
            and all(s["sweeper_running"] for s in per_node.values()),
            "per_node": per_node,
        }
