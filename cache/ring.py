"""Day 3, consistent hashing ring with virtual nodes.

Keys hash onto a ring, and each real node owns many virtual nodes spaced
around it. A key belongs to the first virtual node at or after its hash,
wrapping around the end of the ring. Adding or removing a node therefore
moves only the keys that fall on that node's virtual nodes, not every key.

Day 7 lock review: the ring is shared by every cluster operation, and a
lookup reads two structures, the sorted point list and the point to node
map, that a mutation updates in two steps. A get_node racing a
remove_node could bisect to a point already popped from the map and
raise KeyError, so one lock now guards every read and write of the ring.
"""
import bisect
import hashlib
import threading


def _hash(value):
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)


class ConsistentHashRing:
    def __init__(self, nodes=(), replicas=100):
        self.replicas = replicas
        self._ring = {}
        self._sorted_keys = []
        self._nodes = set()
        self._lock = threading.Lock()
        for node in nodes:
            self.add_node(node)

    def add_node(self, node):
        with self._lock:
            if node in self._nodes:
                return False
            self._nodes.add(node)
            for i in range(self.replicas):
                point = _hash(f"{node}#{i}")
                self._ring[point] = node
                bisect.insort(self._sorted_keys, point)
            return True

    def remove_node(self, node):
        with self._lock:
            if node not in self._nodes:
                return False
            self._nodes.discard(node)
            for i in range(self.replicas):
                point = _hash(f"{node}#{i}")
                self._ring.pop(point, None)
                index = bisect.bisect_left(self._sorted_keys, point)
                if index < len(self._sorted_keys) and self._sorted_keys[index] == point:
                    self._sorted_keys.pop(index)
            return True

    def get_node(self, key):
        with self._lock:
            if not self._sorted_keys:
                return None
            point = _hash(key)
            index = bisect.bisect_right(self._sorted_keys, point)
            if index == len(self._sorted_keys):
                index = 0
            return self._ring[self._sorted_keys[index]]

    def nodes(self):
        with self._lock:
            return sorted(self._nodes)
