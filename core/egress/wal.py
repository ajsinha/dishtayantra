"""
core/egress/wal.py - Write-Ahead Log for the async egress subsystem (roadmap A5).

A WAL is a per-destination, append-only, single-consumer log that is the durable
buffer between the compute thread (which appends and returns) and the egress
drainer (which writes to the broker and acks). Backends are pluggable and, except
for the optional ``lmdb``, require no native dependency:

  - ``memory``  - bounded in-process ring; no crash durability (fastest).
  - ``filelog`` - segmented append log via the Python stdlib; cross-platform,
                  durable, length-prefix + CRC32 framing with torn-tail recovery.
  - ``sqlite``  - stdlib sqlite3 (WAL journal mode); transactional, portable.
  - ``lmdb``    - opt-in, only if the lmdb package is installed.

Contract (single producer thread, single consumer/drainer thread):
  append(payload) -> offset            durable once returned per fsync policy
  read_next(max_records) -> [(off,p)]  sequential, offsets strictly increasing
  ack(offset)                          persist committed offset (<= written)
  reclaim()                            drop fully-acked storage
  size_bytes(), is_full(), high_water(), committed_offset(), close()

POLICY - every append is immediately visible / flushed to the OS:
  Each backend guarantees an appended record is visible to the consumer as soon as
  append() returns, and is flushed out of user space into the OS page cache. Two
  consequences: (1) the drainer always sees new records promptly; (2) records
  survive a *process* crash (the kernel owns the page cache) even before any fsync.
  This is separate from on-disk durability: fsync()-ing the page cache to physical
  disk (to survive power loss) is governed by the `fsync` policy (always/interval/os).
  flush() is cheap (a write() syscall, not disk I/O); fsync() is the expensive one.
  - memory : intrinsic (writer and reader share one in-process list)
  - sqlite : intrinsic (commit() per append makes rows visible)
  - filelog: flush() on every append (then fsync per policy)
"""

import os
import pickle
import struct
import threading
import time
import zlib
from abc import ABC, abstractmethod

_FRAME = struct.Struct(">II")  # length, crc32


class WalFull(Exception):
    """Raised by append() when the WAL is at capacity (caller applies policy)."""


class WalBackend(ABC):
    """Contract every WAL backend must honour.

    Concurrency model: exactly one producer thread (the compute thread calling
    ``append``) and one consumer thread (the drainer calling ``read_next``/``ack``/
    ``reclaim``). Implementations guard shared state with ``self._lock`` (reentrant,
    see below) and need not support multiple concurrent producers or consumers.

    Offsets are 1-based, contiguous, and strictly increasing in append order; they
    are the unit of progress for read/ack/reclaim.
    """

    def __init__(self, max_bytes, high_water_pct):
        self.max_bytes = int(max_bytes)
        self.high_water_pct = int(high_water_pct)
        # RLock, not Lock: append() holds the lock and calls is_full() -> size_bytes(),
        # which (in some backends) re-acquires it. A plain Lock would self-deadlock.
        self._lock = threading.RLock()

    @abstractmethod
    def append(self, payload):
        """Append one record and return its offset.

        Once this returns the record is *visible* to the consumer and flushed to the
        OS (so it survives a process crash); durability to physical disk follows the
        backend's fsync policy. Raises ``WalFull`` if the WAL is at ``max_bytes`` so
        the caller can apply its overflow policy (block/drop)."""

    @abstractmethod
    def read_next(self, max_records):
        """Return up to ``max_records`` un-consumed records as ``[(offset, payload)]``
        in offset order, advancing an internal single-consumer read cursor. Returns
        an empty list when the tail is reached. Re-reads nothing already returned in
        this process; on restart the cursor resumes just after ``committed_offset``."""

    @abstractmethod
    def ack(self, offset):
        """Mark every record up to and including ``offset`` as delivered, and persist
        that committed offset durably. Call only after the broker has accepted the
        record: a crash replays everything after the last acked offset (at-least-once).
        Acked records become eligible for ``reclaim``."""

    @abstractmethod
    def committed_offset(self):
        """The highest durably-acked offset (0 if nothing acked). The resume point."""

    @abstractmethod
    def size_bytes(self):
        """Approximate on-disk/in-memory size of un-reclaimed records, for the cap and
        high-water checks. Cheap; may over-estimate, must not under-estimate badly."""

    @abstractmethod
    def reclaim(self):
        """Drop storage for fully-acked records (offset <= committed_offset). Safe to
        call at any time; never drops un-acked data. This is what keeps the WAL
        bounded in steady state."""

    def is_full(self):
        """True when the WAL has hit its hard cap (``max_bytes``); 0 disables the cap."""
        return self.max_bytes > 0 and self.size_bytes() >= self.max_bytes

    def high_water(self):
        """True past the high-water mark — the signal to warn / apply backpressure
        before the hard cap is reached."""
        return self.max_bytes > 0 and \
            self.size_bytes() >= self.max_bytes * self.high_water_pct / 100.0

    def pending_count(self):
        """v5.15.0: records appended but not yet committed (drained) - i.e. still
        waiting to be flushed to the real sink. 0 means fully drained.

        Offset-based (highest assigned offset minus committed offset), NOT byte
        size: the filelog active segment keeps committed bytes on disk until it
        rolls, so size_bytes() never reaches 0 even when drained. All backends
        follow the _next (next offset to assign) / _committed convention.
        """
        nxt = getattr(self, "_next", 1)
        committed = getattr(self, "_committed", None)
        if committed is None:
            try:
                committed = self.committed_offset()
            except Exception:  # noqa: BLE001
                committed = 0
        return max(0, (nxt - 1) - (committed or 0))

    def close(self):
        """Flush and release resources (file handles, db connections). Idempotent."""


class MemoryWal(WalBackend):
    """Bounded in-memory ring. No durability across process restart."""

    def __init__(self, max_bytes=64 * 1024 * 1024, high_water_pct=80, **_):
        super().__init__(max_bytes, high_water_pct)
        self._items = []          # list of (offset, payload, size)
        self._next = 1
        self._committed = 0
        self._bytes = 0
        self._read_idx = 0

    def append(self, payload):
        with self._lock:
            if self.is_full():
                raise WalFull()
            size = len(pickle.dumps(payload, pickle.HIGHEST_PROTOCOL))
            off = self._next
            self._items.append((off, payload, size))
            self._next += 1
            self._bytes += size
            return off

    def read_next(self, max_records):
        with self._lock:
            out = self._items[self._read_idx:self._read_idx + max_records]
            self._read_idx += len(out)
            return [(o, p) for (o, p, _s) in out]

    def ack(self, offset):
        with self._lock:
            self._committed = max(self._committed, offset)

    def committed_offset(self):
        return self._committed

    def size_bytes(self):
        return self._bytes

    def reclaim(self):
        with self._lock:
            keep_from = 0
            for i, (off, _p, size) in enumerate(self._items):
                if off <= self._committed:
                    self._bytes -= size
                    keep_from = i + 1
                else:
                    break
            if keep_from:
                self._items = self._items[keep_from:]
                self._read_idx = max(0, self._read_idx - keep_from)


class FileLogWal(WalBackend):
    """Segmented append-only log using only the stdlib. Cross-platform, durable."""

    def __init__(self, path, max_bytes=2 * 1024 ** 3, high_water_pct=80,
                 segment_bytes=128 * 1024 * 1024, fsync="interval",
                 fsync_interval_ms=50, **_):
        super().__init__(max_bytes, high_water_pct)
        self.dir = path
        self.segment_bytes = int(segment_bytes)
        self.fsync = fsync
        self.fsync_interval = fsync_interval_ms / 1000.0
        os.makedirs(self.dir, exist_ok=True)
        self._segments = []       # list of dict(base=, path=)
        self._active = None       # open file handle (append)
        self._active_base = 1
        self._next = 1
        self._committed = self._load_checkpoint()
        self._last_fsync = 0.0
        self._reader = None       # dict(fh=, offset=, seg_idx=)
        self._recover()

    # --- checkpoint -------------------------------------------------------
    def _ckpt_path(self):
        return os.path.join(self.dir, "checkpoint")

    def _load_checkpoint(self):
        try:
            with open(self._ckpt_path()) as fh:
                return int(fh.read().strip() or 0)
        except (OSError, ValueError):
            return 0

    def _persist_checkpoint(self):
        tmp = self._ckpt_path() + ".tmp"
        with open(tmp, "w") as fh:
            fh.write(str(self._committed))
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._ckpt_path())

    # --- recovery (rebuild offsets, detect torn tail) ---------------------
    def _seg_path(self, base):
        return os.path.join(self.dir, f"seg-{base:020d}.log")

    def _recover(self):
        files = sorted(f for f in os.listdir(self.dir) if f.startswith("seg-"))
        offset = 0
        for fname in files:
            base = int(fname[4:].split(".")[0])
            full = os.path.join(self.dir, fname)
            count = self._scan_count(full)  # valid records (truncates torn tail)
            if count == 0 and base != 1 and not files[-1] == fname:
                self._segments.append({"base": base, "path": full})
                continue
            self._segments.append({"base": base, "path": full})
            offset = base + count - 1
        self._next = (offset + 1) if offset else 1
        # open/continue active segment
        if self._segments:
            last = self._segments[-1]
            self._active_base = last["base"]
            self._active = open(last["path"], "ab")
        else:
            self._active_base = 1
            self._active = open(self._seg_path(1), "ab")
            self._segments.append({"base": 1, "path": self._seg_path(1)})

    def _scan_count(self, path):
        n = 0
        try:
            with open(path, "rb") as fh:
                while True:
                    hdr = fh.read(_FRAME.size)
                    if len(hdr) < _FRAME.size:
                        break
                    length, crc = _FRAME.unpack(hdr)
                    body = fh.read(length)
                    if len(body) < length or zlib.crc32(body) != crc:
                        break  # torn tail
                    n += 1
        except OSError:
            pass
        return n

    # --- append -----------------------------------------------------------
    def append(self, payload):
        with self._lock:
            if self.is_full():
                raise WalFull()
            body = pickle.dumps(payload, pickle.HIGHEST_PROTOCOL)
            frame = _FRAME.pack(len(body), zlib.crc32(body)) + body
            self._active.write(frame)
            self._active.flush()          # push to OS so the reader handle sees it
            off = self._next
            self._next += 1
            self._maybe_fsync()           # fsync to disk per durability policy
            if self._active.tell() >= self.segment_bytes:
                self._roll()
            return off

    def _maybe_fsync(self):
        if self.fsync == "always":
            self._active.flush(); os.fsync(self._active.fileno())
        elif self.fsync == "interval":
            now = time.monotonic()
            if now - self._last_fsync >= self.fsync_interval:
                self._active.flush(); os.fsync(self._active.fileno())
                self._last_fsync = now
        # "os" -> rely on page cache

    def _roll(self):
        self._active.flush(); os.fsync(self._active.fileno())
        self._active.close()
        base = self._next
        self._active_base = base
        self._active = open(self._seg_path(base), "ab")
        self._segments.append({"base": base, "path": self._seg_path(base)})

    # --- read (single consumer) ------------------------------------------
    def read_next(self, max_records):
        with self._lock:
            if self._reader is None:
                self._open_reader(self._committed)
            out = []
            while len(out) < max_records:
                rec = self._reader_read_one()
                if rec is None:
                    break
                out.append(rec)
            return out

    def _open_reader(self, after_offset):
        # find segment containing after_offset+1
        target = after_offset + 1
        seg_idx = 0
        for i, seg in enumerate(self._segments):
            nxt = self._segments[i + 1]["base"] if i + 1 < len(self._segments) else self._next
            if seg["base"] <= target < nxt:
                seg_idx = i
                break
        else:
            seg_idx = len(self._segments) - 1
        fh = open(self._segments[seg_idx]["path"], "rb")
        cur = self._segments[seg_idx]["base"] - 1
        # skip records up to after_offset
        while cur < after_offset:
            hdr = fh.read(_FRAME.size)
            if len(hdr) < _FRAME.size:
                break
            length, _crc = _FRAME.unpack(hdr)
            fh.seek(length, os.SEEK_CUR)
            cur += 1
        self._reader = {"fh": fh, "offset": cur, "seg_idx": seg_idx}

    def _reader_read_one(self):
        r = self._reader
        while True:
            hdr = r["fh"].read(_FRAME.size)
            if len(hdr) < _FRAME.size:
                # advance to next segment if any
                if r["seg_idx"] + 1 < len(self._segments):
                    r["fh"].close()
                    r["seg_idx"] += 1
                    r["fh"] = open(self._segments[r["seg_idx"]]["path"], "rb")
                    continue
                return None
            length, crc = _FRAME.unpack(hdr)
            body = r["fh"].read(length)
            if len(body) < length or zlib.crc32(body) != crc:
                return None  # torn tail / not yet flushed
            r["offset"] += 1
            return (r["offset"], pickle.loads(body))

    def ack(self, offset):
        with self._lock:
            self._committed = max(self._committed, offset)
            self._persist_checkpoint()

    def committed_offset(self):
        return self._committed

    def size_bytes(self):
        total = 0
        for seg in self._segments:
            try:
                total += os.path.getsize(seg["path"])
            except OSError:
                pass
        return total

    def reclaim(self):
        with self._lock:
            keep = []
            for i, seg in enumerate(self._segments):
                is_active = (i == len(self._segments) - 1)
                seg_max = (self._segments[i + 1]["base"] - 1) if not is_active else self._next - 1
                if not is_active and seg_max <= self._committed:
                    try:
                        os.remove(seg["path"])
                    except OSError:
                        pass
                else:
                    keep.append(seg)
            self._segments = keep

    def close(self):
        with self._lock:
            try:
                if self._active:
                    self._active.flush(); os.fsync(self._active.fileno()); self._active.close()
                if self._reader:
                    self._reader["fh"].close()
            except OSError:
                pass


class SqliteWal(WalBackend):
    """Transactional, portable WAL on the stdlib sqlite3 module (WAL journal)."""

    def __init__(self, path, max_bytes=2 * 1024 ** 3, high_water_pct=80, **_):
        super().__init__(max_bytes, high_water_pct)
        import sqlite3
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("CREATE TABLE IF NOT EXISTS wal(offset INTEGER PRIMARY KEY AUTOINCREMENT, payload BLOB)")
        self._db.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v INTEGER)")
        self._db.execute("INSERT OR IGNORE INTO meta(k,v) VALUES('committed',0)")
        self._db.commit()
        row = self._db.execute("SELECT v FROM meta WHERE k='committed'").fetchone()
        self._committed = row[0] if row else 0
        self._read_after = self._committed

    def append(self, payload):
        with self._lock:
            if self.is_full():
                raise WalFull()
            body = pickle.dumps(payload, pickle.HIGHEST_PROTOCOL)
            cur = self._db.execute("INSERT INTO wal(payload) VALUES(?)", (body,))
            self._db.commit()
            return cur.lastrowid

    def read_next(self, max_records):
        with self._lock:
            rows = self._db.execute(
                "SELECT offset,payload FROM wal WHERE offset>? ORDER BY offset LIMIT ?",
                (self._read_after, max_records)).fetchall()
            if rows:
                self._read_after = rows[-1][0]
            return [(o, pickle.loads(b)) for (o, b) in rows]

    def ack(self, offset):
        with self._lock:
            self._committed = max(self._committed, offset)
            self._db.execute("UPDATE meta SET v=? WHERE k='committed'", (self._committed,))
            self._db.commit()

    def committed_offset(self):
        return self._committed

    def size_bytes(self):
        with self._lock:
            row = self._db.execute("SELECT COALESCE(SUM(LENGTH(payload)),0) FROM wal").fetchone()
            return row[0] if row else 0

    def reclaim(self):
        with self._lock:
            self._db.execute("DELETE FROM wal WHERE offset<=?", (self._committed,))
            self._db.commit()

    def close(self):
        with self._lock:
            self._db.close()


def create_wal(backend, name, cfg):
    """Factory. cfg is a dict of WAL settings; name keys the per-destination store."""
    backend = (backend or "filelog").lower()
    common = {"max_bytes": cfg.get("max_bytes", 2 * 1024 ** 3),
              "high_water_pct": cfg.get("high_water_pct", 80)}
    if backend == "memory":
        return MemoryWal(**common)
    if backend == "filelog":
        return FileLogWal(path=os.path.join(cfg.get("path", "./data/egress_wal"), _safe(name)),
                          segment_bytes=cfg.get("segment_bytes", 128 * 1024 * 1024),
                          fsync=cfg.get("fsync", "interval"),
                          fsync_interval_ms=cfg.get("fsync_interval_ms", 50), **common)
    if backend == "sqlite":
        return SqliteWal(path=os.path.join(cfg.get("path", "./data/egress_wal"), _safe(name) + ".sqlite"),
                         **common)
    if backend == "lmdb":
        try:
            import lmdb  # noqa: F401
        except Exception as exc:
            raise RuntimeError(
                "egress.wal.backend=lmdb but the lmdb package is not installed; "
                "use filelog or sqlite (pure stdlib) instead") from exc
        raise NotImplementedError("lmdb WAL backend pending; use filelog or sqlite")
    raise ValueError(f"unknown WAL backend: {backend}")


def _safe(name):
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in str(name))
