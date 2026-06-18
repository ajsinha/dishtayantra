"""Tests for the async egress subsystem (core/egress) - roadmap A5."""

import os
import time

import pytest

from core.egress.async_publisher import AsyncPublisher, maybe_wrap_publisher
from core.egress.drainer import (AsyncEgressManager, DestinationChannel,
                                 get_manager)
from core.egress.wal import (FileLogWal, MemoryWal, SqliteWal, WalFull,
                             create_wal)


def _drain(channel, max_pumps=2000):
    """Deterministically pump a channel until it stops making progress."""
    for _ in range(max_pumps):
        if not channel.pump():
            break


def _wait_for(pred, timeout=5.0, interval=0.01):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(interval)
    return pred()


# ----------------------------- WAL backends --------------------------------
@pytest.mark.parametrize("make", [
    lambda d: MemoryWal(max_bytes=10_000_000),
    lambda d: FileLogWal(path=os.path.join(d, "fl"), segment_bytes=4096, fsync="always"),
    lambda d: SqliteWal(path=os.path.join(d, "wal.sqlite")),
])
def test_wal_append_read_ack_reclaim(make, tmp_path):
    wal = make(str(tmp_path))
    offs = [wal.append({"i": i}) for i in range(5)]
    assert offs == [1, 2, 3, 4, 5]                      # contiguous, ordered
    got = wal.read_next(10)
    assert [p["i"] for _o, p in got] == [0, 1, 2, 3, 4]  # FIFO order preserved
    wal.ack(got[-1][0])
    assert wal.committed_offset() == 5
    wal.reclaim()
    wal.close()


def test_filelog_durable_across_reopen(tmp_path):
    path = os.path.join(str(tmp_path), "d")
    wal = FileLogWal(path=path, fsync="always")
    for i in range(4):
        wal.append({"i": i})
    first = wal.read_next(2)
    wal.ack(first[-1][0])          # commit first 2
    wal.close()

    wal2 = FileLogWal(path=path, fsync="always")   # reopen
    assert wal2.committed_offset() == 2
    resumed = wal2.read_next(10)                    # resumes after committed
    assert [p["i"] for _o, p in resumed] == [2, 3]
    wal2.close()


def test_filelog_torn_tail_recovery(tmp_path):
    path = os.path.join(str(tmp_path), "torn")
    wal = FileLogWal(path=path, fsync="always")
    for i in range(3):
        wal.append({"i": i})
    seg = wal._segments[-1]["path"]
    wal.close()
    with open(seg, "ab") as fh:                     # simulate a half-written record
        fh.write(b"\x00\x00\x00\x10partialgarbage")
    wal2 = FileLogWal(path=path, fsync="always")
    recovered = wal2.read_next(10)
    assert [p["i"] for _o, p in recovered] == [0, 1, 2]   # torn tail ignored
    wal2.close()


def test_filelog_interval_fsync_reader_sees_appends(tmp_path):
    """Regression: with fsync=interval, appends must still be visible to the
    drainer's read handle promptly (flush-to-OS on every append)."""
    path = os.path.join(str(tmp_path), "vis")
    wal = FileLogWal(path=path, fsync="interval", fsync_interval_ms=10_000)
    for i in range(10):
        wal.append({"i": i})
    got = wal.read_next(100)                 # no fsync has fired yet
    assert [p["i"] for _o, p in got] == list(range(10))
    wal.close()


def test_wal_full_raises():
    wal = MemoryWal(max_bytes=200)
    with pytest.raises(WalFull):
        for _ in range(1000):
            wal.append({"payload": "x" * 50})


def test_create_wal_lmdb_clear_error(tmp_path):
    with pytest.raises((RuntimeError, NotImplementedError)):
        create_wal("lmdb", "d", {"path": str(tmp_path)})


# ----------------------------- channel / pool -------------------------------
def test_channel_delivers_in_order():
    wal = MemoryWal(max_bytes=10_000_000)
    sink = []
    ch = DestinationChannel("t1", wal, sink.append)
    for i in range(50):
        wal.append({"i": i})
    _drain(ch)
    assert [m["i"] for m in sink] == list(range(50))   # strict FIFO
    assert wal.committed_offset() == 50


def test_channel_stop_the_line_no_loss_no_reorder():
    wal = MemoryWal(max_bytes=10_000_000)
    sink = []
    state = {"fail_until": 5}      # first 5 write attempts fail (connection loss)

    def flaky(payload):
        if state["fail_until"] > 0:
            state["fail_until"] -= 1
            raise ConnectionError("broker down")
        sink.append(payload)

    ch = DestinationChannel("t2", wal, flaky, backoff_initial_ms=0, backoff_max_ms=0)
    for i in range(10):
        wal.append({"i": i})
    _drain(ch)
    assert [m["i"] for m in sink] == list(range(10))   # nothing lost or reordered
    assert ch.retries >= 5


def test_worker_pool_respects_cap_and_delivers():
    """Many destinations multiplex onto a bounded pool; all deliver in order."""
    mgr = AsyncEgressManager()
    sinks = {}
    for d in range(12):                       # 12 destinations, cap of 4 workers
        key = f"dest{d}"
        sinks[key] = []
        wal = MemoryWal(max_bytes=10_000_000)
        for i in range(20):
            wal.append({"d": d, "i": i})
        mgr.register(key, wal, sinks[key].append, max_workers=4)
    assert _wait_for(lambda: all(len(s) == 20 for s in sinks.values()))
    assert mgr.worker_count() <= 4            # never exceeds the cap
    for d in range(12):
        assert [m["i"] for m in sinks[f"dest{d}"]] == list(range(20))  # per-dest FIFO
    mgr.stop_all()


def test_one_stuck_destination_does_not_starve_siblings():
    """A stalled destination must not block other destinations on the same worker."""
    mgr = AsyncEgressManager()
    good = []
    stuck_writes = []

    def stuck(_payload):
        stuck_writes.append(1)
        raise ConnectionError("permanently down")

    # force both onto the SAME single worker (max_workers=1)
    wal_good = MemoryWal(); wal_bad = MemoryWal()
    for i in range(10):
        wal_good.append({"i": i}); wal_bad.append({"i": i})
    mgr.register("bad", wal_bad, stuck, max_workers=1, backoff_initial_ms=1, backoff_max_ms=5)
    mgr.register("good", wal_good, good.append, max_workers=1)
    # the good destination still drains fully despite the stuck sibling
    assert _wait_for(lambda: len(good) == 10)
    assert [m["i"] for m in good] == list(range(10))
    mgr.stop_all()


# ----------------------------- wrapper -------------------------------------
class _FakePublisher:
    def __init__(self):
        self.name = "fake_pub_" + str(id(self))
        self.received = []

    def publish(self, data):
        self.received.append(data)


def test_async_publisher_non_blocking_and_ordered():
    inner = _FakePublisher()
    cfg = {"backend": "memory", "batch_max_records": 100, "overflow_policy": "block"}
    pub = AsyncPublisher(inner, cfg, name=inner.name)
    for i in range(30):
        pub.publish({"seq": i})
    assert _wait_for(lambda: len(inner.received) == 30)
    assert [m["seq"] for m in inner.received] == list(range(30))
    pub.close()


def test_async_publisher_drop_policy():
    inner = _FakePublisher()
    cfg = {"backend": "memory", "max_bytes": 200, "overflow_policy": "drop",
           "batch_max_records": 1}
    pub = AsyncPublisher(inner, cfg, name=inner.name)
    for i in range(500):
        pub.publish({"x": "y" * 40})
    pub.close()
    assert pub.dropped > 0          # capacity enforced, no exception, no OOM


def test_maybe_wrap_disabled_is_passthrough():
    inner = _FakePublisher()
    # default config (disabled) -> returns the same object unchanged
    assert maybe_wrap_publisher(inner, {"egress.async.enabled": False}) is inner


def test_maybe_wrap_enabled_wraps():
    inner = _FakePublisher()
    cfg = {"egress.async.enabled": True, "egress.wal.backend": "memory"}
    wrapped = maybe_wrap_publisher(inner, cfg, name=inner.name)
    assert isinstance(wrapped, AsyncPublisher)
    wrapped.publish({"a": 1})
    assert _wait_for(lambda: inner.received == [{"a": 1}])
    wrapped.close()


def test_mem_destination_never_wrapped():
    """In-memory destinations must never use the WAL, even with async enabled."""
    inner = _FakePublisher()
    cfg = {"egress.async.enabled": True, "egress.wal.backend": "memory"}
    for dest in ("mem://queue/q1", "inmemory://x", "memory://y"):
        out = maybe_wrap_publisher(inner, cfg, name=inner.name,
                                   publisher_config={"destination": dest})
        assert out is inner          # unchanged -> publishes inline


def test_per_publisher_optout_and_optin():
    """async_egress in the publisher config overrides; a mix is possible."""
    cfg = {"egress.async.enabled": True, "egress.wal.backend": "memory"}
    a, b, c = _FakePublisher(), _FakePublisher(), _FakePublisher()
    # explicit opt-out -> inline (unchanged)
    out_a = maybe_wrap_publisher(a, cfg, name="a", scope="dagA",
                                 publisher_config={"destination": "kafka://t",
                                                   "async_egress": False})
    assert out_a is a
    # explicit opt-in -> wrapped
    out_b = maybe_wrap_publisher(b, cfg, name="b", scope="dagB",
                                 publisher_config={"destination": "kafka://t",
                                                   "async_egress": True})
    assert isinstance(out_b, AsyncPublisher)
    # opt-in-only mode: default off -> unspecified stays inline
    cfg_optin = dict(cfg); cfg_optin["egress.async.default"] = False
    out_c = maybe_wrap_publisher(c, cfg_optin, name="c", scope="dagC",
                                 publisher_config={"destination": "kafka://t"})
    assert out_c is c
    out_b.close()


def test_worker_count_config_is_honored():
    """egress.worker.count caps the pool the wrapper registers onto."""
    import core.egress.drainer as drainer_mod
    drainer_mod._manager = None          # fresh, unconfigured singleton for this test
    cfg = {"egress.async.enabled": True, "egress.wal.backend": "memory",
           "egress.worker.count": 2}
    pubs = []
    for i in range(6):
        inner = _FakePublisher()
        w = maybe_wrap_publisher(inner, cfg, name=f"p{i}", scope=f"dag{i}",
                                 publisher_config={"destination": "kafka://t"})
        pubs.append(w)
    mgr = get_manager()
    assert mgr.max_workers == 2
    assert mgr.worker_count() <= 2
    for p in pubs:
        p.close()


def test_wal_key_namespacing_no_collision(monkeypatch):
    """DAG names are universally unique, so (DAG, publisher) namespaces the WAL
    with no worker id. Setting a worker id must NOT change the key - that is what
    keeps resume correct when a DAG is reassigned to a different worker."""
    from core.egress.drainer import get_manager
    monkeypatch.setenv("DY_WORKER_ID", "7")        # must be ignored for keying
    cfg = {"egress.async.enabled": True, "egress.wal.backend": "memory"}
    a, b = _FakePublisher(), _FakePublisher()
    wa = maybe_wrap_publisher(a, cfg, name="kpub", scope="dagA")
    wb = maybe_wrap_publisher(b, cfg, name="kpub", scope="dagB")
    mgr = get_manager()
    assert mgr.get("dagA.kpub") is not None         # DAG.publisher, no worker id
    assert mgr.get("dagB.kpub") is not None
    assert mgr.get("7.dagA.kpub") is None           # worker id not in the key
    assert mgr.get("dagA.kpub") is not mgr.get("dagB.kpub")
    wa.publish({"x": 1}); wb.publish({"y": 2})
    assert _wait_for(lambda: a.received == [{"x": 1}] and b.received == [{"y": 2}])
    wa.close(); wb.close()
