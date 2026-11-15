"""Day 11, configuration from environment variables.

Every tunable the app used to hardcode now comes from the environment,
with the old hardcoded value kept as the default, so a plain run with
no variables set behaves exactly as it did before Day 11. The config
is read once at startup, from a plain dict, so the app and the tests
can build it from os.environ or from a fake dict without touching
global state.

Variables, all optional:
    CACHE_CAPACITY, int, default 1000, max keys per store and per shard.
    CACHE_EVICTION_POLICY, lru or lfu, default lru.
    CACHE_SNAPSHOT_PATH, path, default data/cache_snapshot.json.
    CACHE_SWEEP_INTERVAL_SECONDS, int, default 10.
    CACHE_NODES, comma separated, default node-a,node-b,node-c.
    CACHE_REPLICATION_FACTOR, int, default 2.
    CACHE_RING_REPLICAS, int, default 100, virtual nodes per real node.
    CACHE_MAX_BATCH_SIZE, int, default 1000.

Invalid values fail fast with a ValueError at startup instead of
silently falling back to a default. A typo like CACHE_CAPACITY=abc
should stop the process, not quietly run a cache with the wrong size,
because the operator would otherwise never know the value was ignored.
"""
import os
from dataclasses import dataclass, field


def _parse_int(raw, name):
    """Parse an env var as an int, failing fast on a bad value.

    Raises ValueError naming the variable, so the startup error says
    which variable is wrong instead of a bare int() traceback.
    """
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


def _parse_positive_int(raw, name):
    """Parse an env var as a positive int, failing fast on a bad value.

    Zero or negative capacities, intervals, and factors make no sense,
    so they are rejected here, at startup, instead of failing later
    in a store or a sweeper with a confusing error.
    """
    value = _parse_int(raw, name)
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


@dataclass(frozen=True)
class CacheConfig:
    """The effective config for one process, plus where each value came from.

    sources maps each field name to "env" or "default", so the
    GET /config endpoint can show an operator what is actually running
    and whether a value was set or fell back. The dataclass is frozen,
    so config cannot drift after startup, and the app reads it once.
    """

    capacity: int = 1000
    eviction_policy: str = "lru"
    snapshot_path: str = "data/cache_snapshot.json"
    sweep_interval_seconds: int = 10
    nodes: tuple = ("node-a", "node-b", "node-c")
    replication_factor: int = 2
    ring_replicas: int = 100
    max_batch_size: int = 1000
    sources: dict = field(default_factory=dict, repr=False)

    @classmethod
    def from_env(cls, env=None):
        """Build a config from a mapping of variable names to strings.

        env defaults to os.environ. A variable that is set but empty
        counts as unset, so CACHE_CAPACITY= behaves like it was never
        set, which is friendlier than failing on an empty export.
        Unknown CACHE_ variables are ignored, so a typo in a new
        variable name does not break startup, it just has no effect.
        """
        if env is None:
            env = os.environ
        values = {}
        sources = {}

        def take(name, parse=None):
            raw = env.get(name)
            if raw is None or raw == "":
                return None
            value = parse(raw, name) if parse else raw
            values[name] = value
            sources[name] = "env"
            return value

        capacity = take("CACHE_CAPACITY", _parse_positive_int)
        policy = take("CACHE_EVICTION_POLICY")
        if policy is not None and policy not in ("lru", "lfu"):
            raise ValueError(
                f"CACHE_EVICTION_POLICY must be lru or lfu, got {policy!r}"
            )
        snapshot_path = take("CACHE_SNAPSHOT_PATH")
        sweep = take("CACHE_SWEEP_INTERVAL_SECONDS", _parse_positive_int)
        nodes_raw = take("CACHE_NODES")
        nodes = None
        if nodes_raw is not None:
            nodes = tuple(
                part.strip() for part in nodes_raw.split(",") if part.strip()
            )
            if not nodes:
                raise ValueError(
                    "CACHE_NODES must list at least one node, got "
                    f"{nodes_raw!r}"
                )
        replication_factor = take("CACHE_REPLICATION_FACTOR", _parse_positive_int)
        ring_replicas = take("CACHE_RING_REPLICAS", _parse_positive_int)
        max_batch = take("CACHE_MAX_BATCH_SIZE", _parse_positive_int)

        kwargs = {}
        if capacity is not None:
            kwargs["capacity"] = capacity
        if policy is not None:
            kwargs["eviction_policy"] = policy
        if snapshot_path is not None:
            kwargs["snapshot_path"] = snapshot_path
        if sweep is not None:
            kwargs["sweep_interval_seconds"] = sweep
        if nodes is not None:
            kwargs["nodes"] = nodes
        if replication_factor is not None:
            kwargs["replication_factor"] = replication_factor
        if ring_replicas is not None:
            kwargs["ring_replicas"] = ring_replicas
        if max_batch is not None:
            kwargs["max_batch_size"] = max_batch
        kwargs["sources"] = {
            field_name: sources.get(env_name, "default")
            for field_name, env_name in _ENV_NAMES.items()
        }
        return cls(**kwargs)

    def to_dict(self):
        """The effective config as plain JSON friendly values.

        Nodes become a list, so the endpoint returns JSON, not a
        tuple, and sources is included, so the caller can see which
        values came from the environment and which are defaults.
        """
        return {
            "capacity": self.capacity,
            "eviction_policy": self.eviction_policy,
            "snapshot_path": self.snapshot_path,
            "sweep_interval_seconds": self.sweep_interval_seconds,
            "nodes": list(self.nodes),
            "replication_factor": self.replication_factor,
            "ring_replicas": self.ring_replicas,
            "max_batch_size": self.max_batch_size,
            "sources": dict(self.sources),
        }


_ENV_NAMES = {
    "capacity": "CACHE_CAPACITY",
    "eviction_policy": "CACHE_EVICTION_POLICY",
    "snapshot_path": "CACHE_SNAPSHOT_PATH",
    "sweep_interval_seconds": "CACHE_SWEEP_INTERVAL_SECONDS",
    "nodes": "CACHE_NODES",
    "replication_factor": "CACHE_REPLICATION_FACTOR",
    "ring_replicas": "CACHE_RING_REPLICAS",
    "max_batch_size": "CACHE_MAX_BATCH_SIZE",
}


def load_config(env=None):
    """Read the config from the environment, defaults when unset."""
    return CacheConfig.from_env(env)
