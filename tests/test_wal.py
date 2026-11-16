"""Day 12 tests, write ahead log for durability.

Every mutation is appended to the log and fsynced before it is
applied, so a restart replays the log after the snapshot and
recovers keys written since the last checkpoint. A checkpoint
writes a snapshot and truncates the log.
"""
import json
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app, store as api_store
from cache.store import KVStore
from cache.wal import WriteAheadLog

client = TestClient(app)


def test_wal_appends_and_replays_in_order(tmp_path):
    path = tmp_path / "wal.jsonl"
    wal = WriteAheadLog(path)
    assert wal.append({"op": "set", "key": "a", "value": "1", "expires_at": None}) == 1
    assert wal.append({"op": "set", "key": "b", "value": "2", "expires_at": None}) == 2
    assert wal.append({"op": "delete", "key": "a"}) == 3
    records = wal.replay()
    assert [seq for seq, _record in records] == [1, 2, 3]
    assert [record["op"] for _seq, record in records] == [
        "set",
        "set",
        "delete",
    ]
    assert records[0][1]["key"] == "a"


def test_wal_continues_sequence_after_reopen(tmp_path):
    path = tmp_path / "wal.jsonl"
    wal = WriteAheadLog(path)
    wal.append({"op": "set", "key": "a", "value": "1", "expires_at": None})
    reopened = WriteAheadLog(path)
    assert reopened.append({"op": "set", "key": "b", "value": "2", "expires_at": None}) == 2
    assert [seq for seq, _record in reopened.replay()] == [1, 2]


def test_wal_drops_torn_tail_and_truncates(tmp_path):
    path = tmp_path / "wal.jsonl"
    wal = WriteAheadLog(path)
    wal.append({"op": "set", "key": "a", "value": "1", "expires_at": None})
    wal.append({"op": "set", "key": "b", "value": "2", "expires_at": None})
    with open(path, "ab") as handle:
        handle.write(b'{"seq": 3, "op": "se')
    reopened = WriteAheadLog(path)
    records = reopened.replay()
    assert [record["key"] for _seq, record in records] == ["a", "b"]
    assert reopened.append({"op": "set", "key": "c", "value": "3", "expires_at": None}) == 3
    assert [record["key"] for _seq, record in reopened.replay()] == ["a", "b", "c"]


def test_wal_truncate_keeps_sequence_rising(tmp_path):
    path = tmp_path / "wal.jsonl"
    wal = WriteAheadLog(path)
    wal.append({"op": "set", "key": "a", "value": "1", "expires_at": None})
    wal.append({"op": "set", "key": "b", "value": "2", "expires_at": None})
    wal.truncate()
    assert wal.replay() == []
    assert wal.append({"op": "set", "key": "c", "value": "3", "expires_at": None}) == 3


def test_store_recovers_from_wal_without_snapshot(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    s.set("a", "1")
    s.set("b", "2", ttl_seconds=60)
    restarted = KVStore(capacity=10, wal_path=wal_path)
    assert restarted.load() == 2
    assert restarted.get("a") == "1"
    assert restarted.get("b") == "2"
    assert restarted.stats()["wal_replayed"] == 2


def test_store_replays_deletes_from_wal(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    s.set("a", "1")
    s.set("b", "2")
    assert s.delete("a") is True
    assert s.delete("missing") is False
    restarted = KVStore(capacity=10, wal_path=wal_path)
    restarted.load()
    assert restarted.get("a") is None
    assert restarted.get("b") == "2"


def test_store_replays_set_many_from_wal(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    assert s.set_many({"a": "1", "b": "2", "c": "3"}) == 3
    restarted = KVStore(capacity=10, wal_path=wal_path)
    restarted.load()
    assert restarted.get_many(["a", "b", "c"]) == {"a": "1", "b": "2", "c": "3"}
    assert restarted.stats()["wal_replayed"] == 3


def test_store_skips_expired_wal_records(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    s.set("fresh", "1")
    s.set("stale", "2", ttl_seconds=0.05)
    time.sleep(0.07)
    restarted = KVStore(capacity=10, wal_path=wal_path)
    restarted.load()
    assert restarted.get("fresh") == "1"
    assert restarted.get("stale") is None


def test_cas_success_is_logged_and_failure_is_not(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    s.set("a", "1")
    assert s.compare_and_set("a", "1", "2") is True
    assert s.compare_and_set("a", "wrong", "3") is False
    assert s.compare_and_set("missing", None, "x") is True
    assert s.compare_and_set("also-missing", "nope", "y") is False
    restarted = KVStore(capacity=10, wal_path=wal_path)
    restarted.load()
    assert restarted.get("a") == "2"
    assert restarted.get("missing") == "x"
    assert restarted.get("also-missing") is None


def test_wal_replay_enforces_capacity(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=2, wal_path=wal_path)
    s.set("a", "1")
    s.set("b", "2")
    s.set("c", "3")
    restarted = KVStore(capacity=2, wal_path=wal_path)
    restarted.load()
    assert restarted.stats()["size"] == 2
    assert restarted.get("a") is None
    assert restarted.get("c") == "3"


def test_checkpoint_truncates_wal_and_snapshot_holds_data(tmp_path):
    snap_path = tmp_path / "snap.json"
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, snapshot_path=snap_path, wal_path=wal_path)
    s.set("a", "1")
    s.set("b", "2")
    assert s.checkpoint() == 2
    assert s.stats()["checkpoints"] == 1
    assert s.stats()["wal_records"] == 0
    assert wal_path.read_bytes() == b""
    restarted = KVStore(capacity=10, snapshot_path=snap_path, wal_path=wal_path)
    assert restarted.load() == 2
    assert restarted.get("a") == "1"
    assert restarted.get("b") == "2"
    assert restarted.stats()["wal_replayed"] == 0


def test_auto_checkpoint_after_record_limit(tmp_path):
    snap_path = tmp_path / "snap.json"
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(
        capacity=10,
        snapshot_path=snap_path,
        wal_path=wal_path,
        wal_checkpoint_records=3,
    )
    s.set("a", "1")
    s.set("b", "2")
    assert s.stats()["checkpoints"] == 0
    s.set("c", "3")
    assert s.stats()["checkpoints"] == 1
    assert s.stats()["wal_records"] == 0
    restarted = KVStore(capacity=10, snapshot_path=snap_path, wal_path=wal_path)
    restarted.load()
    assert restarted.get_many(["a", "b", "c"]) == {"a": "1", "b": "2", "c": "3"}


def test_replay_skips_records_already_in_snapshot(tmp_path):
    snap_path = tmp_path / "snap.json"
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, snapshot_path=snap_path, wal_path=wal_path)
    s.set("a", "1")
    # Snapshot without truncating, like a crash between the snapshot
    # write and the log truncate at a checkpoint.
    assert s.snapshot() == 1
    s.set("a", "2")
    restarted = KVStore(capacity=10, snapshot_path=snap_path, wal_path=wal_path)
    restarted.load()
    assert restarted.get("a") == "2"
    # Only the second record replayed, the first was already in the
    # snapshot and was skipped by its seq.
    assert restarted.stats()["wal_replayed"] == 1


def test_load_recovers_old_format_snapshot(tmp_path):
    snap_path = tmp_path / "snap.json"
    snap_path.write_text(json.dumps({"a": {"value": "1", "expires_at": None}}))
    s = KVStore(capacity=10, snapshot_path=snap_path)
    assert s.load() == 1
    assert s.get("a") == "1"


def test_load_with_torn_wal_tail_recovers_valid_keys(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    s.set("a", "1")
    s.set("b", "2")
    with open(wal_path, "ab") as handle:
        handle.write(b'{"seq": 3, "op": "set", "key": "c"')
    restarted = KVStore(capacity=10, wal_path=wal_path)
    restarted.load()
    assert restarted.get("a") == "1"
    assert restarted.get("b") == "2"
    assert restarted.get("c") is None
    assert wal_path.read_bytes().count(b"\n") == 2


def test_wal_stats_reported(tmp_path):
    wal_path = tmp_path / "wal.jsonl"
    s = KVStore(capacity=10, wal_path=wal_path)
    assert s.stats()["wal_path"] == str(wal_path)
    assert s.stats()["wal_records"] == 0
    assert s.stats()["checkpoints"] == 0
    s.set("a", "1")
    assert s.stats()["wal_records"] == 1
    plain = KVStore(capacity=10)
    assert plain.stats()["wal_path"] is None
    assert plain.stats()["wal_records"] == 0


def test_wal_checkpoint_records_must_be_positive(tmp_path):
    with pytest.raises(ValueError):
        KVStore(capacity=10, wal_path=tmp_path / "wal.jsonl", wal_checkpoint_records=0)


def test_api_put_is_recovered_from_wal():
    # A unique key per run, because the API store loads its snapshot
    # and replays its log on startup, so a fixed key could already
    # exist from an earlier run.
    key = f"wal-a-{uuid.uuid4().hex[:8]}"
    r = client.put(f"/cache/{key}", json={"value": "durable"})
    assert r.status_code == 200
    check = KVStore(
        capacity=1000,
        snapshot_path=api_store.snapshot_path,
        wal_path=api_store.wal_path,
    )
    assert check.load() >= 1
    assert check.get(key) == "durable"


def test_api_checkpoint_endpoint():
    key = f"wal-ckpt-{uuid.uuid4().hex[:8]}"
    r = client.put(f"/cache/{key}", json={"value": "snapshotted"})
    assert r.status_code == 200
    before = client.get("/stats").json()
    r = client.post("/checkpoint")
    assert r.status_code == 200
    assert r.json()["checkpoints"] == before["checkpoints"] + 1
    assert r.json()["wal_records"] == 0
    # The key just written has no TTL, so it is in the snapshot.
    assert r.json()["entries"] >= 1
    # After the checkpoint the log is empty, so a fresh store
    # recovers the key from the snapshot alone.
    check = KVStore(
        capacity=1000,
        snapshot_path=api_store.snapshot_path,
        wal_path=api_store.wal_path,
    )
    check.load()
    assert check.get(key) == "snapshotted"
    assert check.stats()["wal_replayed"] == 0


def test_api_store_has_wal_path():
    assert api_store.wal_path is not None
    assert str(api_store.wal_path).endswith("cache_wal.jsonl")
