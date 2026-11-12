"""Day 3, sharded store: keys route to nodes through a hash ring.

Each node is a plain KVStore. The cluster owns the ring, so a caller
never picks a node directly. When a node joins or leaves, keys whose
owner changes are moved to their new owner, so a topology change does
not silently lose keys. Replication is not here yet, that is Day 10,
so a node that disappears without a clean remove still loses its keys.

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

from cache.ring import ConsistentHashRing
from cache.store import KVStore


class ShardedStore:
    def __init__(self, nodes=(), capacity=1000, replicas=100, eviction_policy="lru"):
        self.capacity = capacity
        self.eviction_policy = eviction_policy
        self.ring = ConsistentHashRing(replicas=replicas)
        self.shards = {}
        self._topology_lock = threading.Lock()
        for node in nodes:
            self.add_node(node)

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
            self.shards[node] = KVStore(
                capacity=self.capacity, eviction_policy=self.eviction_policy
            )
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

    def stats(self):
        snapshot = self._shards_snapshot()
        per_node = {name: shard.stats() for name, shard in snapshot}
        total_hits = sum(s["hits"] for s in per_node.values())
        total_misses = sum(s["misses"] for s in per_node.values())
        reads = total_hits + total_misses
        return {
            "nodes": sorted(per_node),
            "total_size": sum(s["size"] for s in per_node.values()),
            "total_hits": total_hits,
            "total_misses": total_misses,
            "hit_rate": round(total_hits / reads, 4) if reads else 0.0,
            "total_cas_success": sum(s["cas_success"] for s in per_node.values()),
            "total_cas_failure": sum(s["cas_failure"] for s in per_node.values()),
            "sweeper_running": bool(per_node)
            and all(s["sweeper_running"] for s in per_node.values()),
            "per_node": per_node,
        }
