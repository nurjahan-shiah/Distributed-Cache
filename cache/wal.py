"""Day 12, write ahead log for durability.

Every mutation is appended to the log and fsynced before it is applied
in memory, so a crash after the fsync never loses a write that was
acknowledged. On startup the log is replayed after the snapshot, so
keys written since the last checkpoint come back. A checkpoint writes
a snapshot and truncates the log, which bounds how much replay has to
read and how big the log file grows.

Records are one JSON object per line, each stamped with a sequence
number that only ever rises, even across checkpoints and restarts.
The snapshot records the last seq it contains, and replay skips
records at or below that seq, so a record that is already in the
snapshot is never applied twice. Keys and values must be JSON
serializable, which holds for the string keys and values the API
uses.

A crash mid write can leave a half written last line. The log scan
stops at the first line that does not parse and truncates the file
there, so a torn record never replays and a later append does not
glue onto it.
"""
import json
import os
import threading
from pathlib import Path


class WriteAheadLog:
    def __init__(self, path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self.next_seq = 1
        self.last_seq = 0
        self._prepare()

    def _scan(self):
        """Read the log, return (valid records, torn byte offset).

        A record is valid when it parses as JSON and carries an int
        seq and a string op. The scan stops at the first bad line,
        because everything after a torn write is suspect too. The
        offset is where the bad line starts, or None when the whole
        file is clean. A missing file reads as empty.
        """
        valid = []
        torn_at = None
        try:
            raw = self.path.read_bytes()
        except OSError:
            return valid, None
        offset = 0
        for line in raw.splitlines(keepends=True):
            if not line.strip():
                offset += len(line)
                continue
            try:
                record = json.loads(line)
            except ValueError:
                torn_at = offset
                break
            if (
                not isinstance(record, dict)
                or not isinstance(record.get("seq"), int)
                or isinstance(record.get("seq"), bool)
                or not isinstance(record.get("op"), str)
            ):
                torn_at = offset
                break
            valid.append((record["seq"], record))
            offset += len(line)
        return valid, torn_at

    def _truncate_at(self, offset):
        """Cut the file at a byte offset, dropping a torn tail."""
        with open(self.path, "r+b") as handle:
            handle.truncate(offset)

    def _prepare(self):
        """Scan an existing log, drop any torn tail, set the next seq.

        Sequence numbers continue from the highest valid record, so
        a restart never reuses a seq and replay never confuses a new
        record with one the snapshot already holds.
        """
        if not self.path.exists():
            return
        valid, torn_at = self._scan()
        if torn_at is not None:
            self._truncate_at(torn_at)
        if valid:
            self.last_seq = max(seq for seq, _record in valid)
            self.next_seq = self.last_seq + 1

    def append(self, record):
        """Append one record, fsync, and return its sequence number.

        The record is a dict without a seq, the log stamps it. The
        write is flushed and fsynced before this returns, so a crash
        after the call cannot lose the record. Raises OSError when
        the write fails, and the caller must not apply the mutation.
        """
        with self._lock:
            stamped = dict(record)
            stamped["seq"] = self.next_seq
            line = (json.dumps(stamped) + "\n").encode("utf-8")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "ab") as handle:
                handle.write(line)
                handle.flush()
                os.fsync(handle.fileno())
            self.last_seq = self.next_seq
            self.next_seq += 1
            return self.last_seq

    def replay(self):
        """Valid records in file order, dropping any torn tail.

        Returns a list of (seq, record) pairs. A torn last line is
        truncated from the file first, so it never replays and a
        later append does not glue onto it.
        """
        with self._lock:
            valid, torn_at = self._scan()
            if torn_at is not None:
                self._truncate_at(torn_at)
            return valid

    def truncate(self):
        """Empty the log after a checkpoint.

        Sequence numbers keep rising, they are not reset, because
        the snapshot records the last seq it contains, and replay
        skips records at or below it. Resetting to 1 would make new
        records look already snapshotted.
        """
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(b"")
