"""Day 3, sharded store: keys route to nodes through a hash ring.

Each node is a plain KVStore. The cluster owns the ring, so a caller
never picks a node directly. When a node joins or leaves, keys whose
owner changes are moved to their new owner, so a topology change does
not silently lose keys. Replication is not here yet, that is Day 10,
so a node that disappears without a clean remove still loses its keys.
"""
import time

from cache.ring import ConsistentHashRing
from cache.store import KVStore


class ShardedStore:
    def __init__(self, nodes=(), capacity=1000, replicas=100, eviction_policy="lru"):
        self.capacity = capacity
        self.eviction_policy = eviction_policy
        self.ring = ConsistentHashRing(replicas=replicas)
        self.shards = {}
        for node in nodes:
            self.add_node(node)

    def node_for(self, key):
        return self.ring.get_node(key)

    def _migrate_all(self):
        """Move every key whose ring owner changed to its new owner."""
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
        for shard in self.shards.values():
            shard.set_eviction_policy(policy)
        self.eviction_policy = policy

    def sweep_expired(self):
        """Sweep expired keys on every shard, returns the total removed."""
        return sum(shard.sweep_expired() for shard in self.shards.values())

    def start_sweeper(self, interval_seconds=10):
        """Start a background sweeper on every shard.

        Returns True when at least one shard started a new sweeper.
        """
        started = [
            shard.start_sweeper(interval_seconds) for shard in self.shards.values()
        ]
        return any(started)

    def stop_sweeper(self):
        """Stop the background sweeper on every shard.

        Returns True when at least one shard stopped a running sweeper.
        """
        stopped = [shard.stop_sweeper() for shard in self.shards.values()]
        return any(stopped)

    def add_node(self, node):
        if node in self.shards:
            return False
        self.shards[node] = KVStore(
            capacity=self.capacity, eviction_policy=self.eviction_policy
        )
        self.ring.add_node(node)
        self._migrate_all()
        return True

    def remove_node(self, node):
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
        node = self.ring.get_node(key)
        if node is None:
            return False
        self.shards[node].set(key, value, ttl_seconds)
        return True

    def get(self, key):
        node = self.ring.get_node(key)
        if node is None:
            return None
        return self.shards[node].get(key)

    def delete(self, key):
        node = self.ring.get_node(key)
        if node is None:
            return False
        return self.shards[node].delete(key)

    def stats(self):
        per_node = {name: shard.stats() for name, shard in self.shards.items()}
        total_hits = sum(s["hits"] for s in per_node.values())
        total_misses = sum(s["misses"] for s in per_node.values())
        reads = total_hits + total_misses
        return {
            "nodes": sorted(self.shards),
            "total_size": sum(s["size"] for s in per_node.values()),
            "total_hits": total_hits,
            "total_misses": total_misses,
            "hit_rate": round(total_hits / reads, 4) if reads else 0.0,
            "sweeper_running": bool(per_node)
            and all(s["sweeper_running"] for s in per_node.values()),
            "per_node": per_node,
        }

