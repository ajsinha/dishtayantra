"""
Flow Store - the persistence side of Flow Time-Travel
=====================================================

Pluggable, append-only stores for flow events. The recorder
(:mod:`core.flow_recorder`) writes batches here from its drain thread; the API
(:mod:`routes.flow_routes`) reads time ranges back out.

Backends (selected by ``flow_recorder.store`` in configuration):

* ``sqlite`` - :class:`SqliteFlowStore` (default). Frugal, zero extra
  dependencies; a SEPARATE file from the application DB. WAL mode lets the UI
  read while the drain thread writes.
* ``postgres`` - :class:`core.flow_store_sql.SqlFlowStore`. A separate
  SQLAlchemy database (PostgreSQL, or any URL incl. sqlite) via
  ``flow_recorder.db_url``. Lets many instances share one central flow DB.
* ``dao``    - FORBIDDEN. Previously shared the application DB; flow history
  must never co-locate with users/keys/audit. The factory rejects it - use
  ``sqlite`` or ``postgres`` (both separate databases).
* ``noop``   - :class:`NoopFlowStore`. Discards everything.
* ``paimon`` / ``aerospike`` - OPTIONAL documented stubs.

Every store implements the :class:`FlowStore` ABC and stamps each event with
provenance ``(instance, host, port)`` so shared databases stay origin-aware.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

# Default rows-per-transaction for the retention purge. Bounded so a large
# sweep never holds the write lock long enough to starve the drain thread.
DEFAULT_PURGE_BATCH = 5000

# TTL (seconds) for the cached DAG inventory (distinct_dags). The underlying
# query is a full aggregate over all events; caching bounds how often it runs.
DEFAULT_DAGS_CACHE_TTL = 30.0

# The flow schema is defined by ONE dedicated DDL file per dialect under
# config/schema/ (no migrations - the file is the single source of truth). The
# raw-sqlite store applies flow_events_sqlite.sql verbatim at startup.
_SCHEMA_DIR = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), os.pardir, "config", "schema"))
FLOW_SQLITE_SCHEMA_FILE = os.path.join(_SCHEMA_DIR, "flow_events_sqlite.sql")
FLOW_POSTGRES_SCHEMA_FILE = os.path.join(_SCHEMA_DIR, "flow_events_postgres.sql")


def normalise_provenance(prov):
    """Return a clean {instance, host, port} dict (any may be None)."""
    prov = prov or {}
    port = prov.get("port")
    try:
        port = int(port) if port not in (None, "") else None
    except (TypeError, ValueError):
        port = None
    return {"instance": prov.get("instance") or None,
            "host": prov.get("host") or None, "port": port}


class FlowStore(ABC):
    """Pluggable flow-history backend contract.

    Implementations: :class:`SqliteFlowStore` (raw sqlite3, default, a SEPARATE
    file from the application DB), :class:`~core.flow_store_sql.SqlFlowStore`
    (SQLAlchemy: a separate **SQLite or PostgreSQL** database chosen by
    ``flow_recorder.db_url``), :class:`NoopFlowStore`, and the optional backends
    :class:`~core.flow_store_paimon.PaimonFlowStore` /
    :class:`~core.flow_store_aerospike.AerospikeFlowStore`. (The old ``dao``
    backend, which shared the application DB, is no longer permitted.)

    Provenance: every event is stamped with ``(instance, host, port)`` so several
    servers can safely write to ONE shared flow database (e.g. a central
    Postgres) and queries can disambiguate origin via the ``instance`` filter.
    """

    @abstractmethod
    def write_batch(self, events):
        ...

    @abstractmethod
    def query(self, dag_id, t0_ms=None, t1_ms=None, nodes=None, limit=5000,
              after_seq=None, instance=None):
        ...

    @abstractmethod
    def state_at(self, dag_id, ts_ms, instance=None):
        ...

    @abstractmethod
    def distinct_dags(self):
        ...

    @abstractmethod
    def purge_older_than_ms(self, cutoff_ms, batch=DEFAULT_PURGE_BATCH):
        ...

    @abstractmethod
    def iter_export(self, dag_id, t0_ms=None, t1_ms=None, nodes=None,
                    chunk=2000, instance=None):
        ...

    def maintenance(self, mode="none"):
        return False

    def histogram(self, dag_id, t0_ms, t1_ms, buckets=120, instance=None):
        """Per-bucket counts of compute cycles and events across [t0, t1).

        Returns ``{"from","to","bucket_ms","buckets":[{i,t0,t1,cycles,events}]}``
        with exactly ``buckets`` entries (missing buckets are zero-filled). This
        default streams via ``iter_export``; subclasses may override with a single
        SQL aggregate. Used to draw the replay distribution chart cheaply without
        sending events to the browser.
        """
        buckets = max(1, min(int(buckets), 1000))
        t0, t1 = int(t0_ms), int(t1_ms)
        span = max(1, t1 - t0)
        width = span / buckets
        ev = [0] * buckets
        cyc = [set() for _ in range(buckets)]
        for e in self.iter_export(dag_id, t0, t1, None, instance=instance):
            ts = e.get("ts_ms")
            if ts is None:
                continue
            i = int((ts - t0) / width)
            i = 0 if i < 0 else (buckets - 1 if i >= buckets else i)
            ev[i] += 1
            c = e.get("cycle_id")
            if c is not None:
                cyc[i].add(c)
        out = [{"i": i, "t0": int(t0 + i * width), "t1": int(t0 + (i + 1) * width),
                "events": ev[i], "cycles": len(cyc[i])} for i in range(buckets)]
        return {"from": t0, "to": t1, "bucket_ms": width, "buckets": out}


def make_flow_store(kind: str, **opts):
    """Factory: build a store from a config string (no silent fallback).

    Recognised kinds: ``sqlite`` (default; separate file), ``postgres`` (a
    separate SQLAlchemy database via ``db_url``; works for any SQLAlchemy URL
    incl. sqlite), ``dao`` (shares the app DB), ``noop``, ``paimon`` (stub),
    ``aerospike`` (stub). ``provenance`` ({instance, host, port}) is stamped on
    every written event.
    """
    kind = (kind or "sqlite").strip().lower()
    prov = normalise_provenance(opts.get("provenance"))
    if kind == "sqlite":
        return SqliteFlowStore(opts.get("path", "data/flow_history.db"),
                               provenance=prov)
    if kind in ("postgres", "postgresql", "sql"):
        url = opts.get("db_url")
        if not url:
            raise ValueError(
                "flow_recorder.store=postgres requires flow_recorder.db_url "
                "(e.g. postgresql+psycopg://user:pass@host:5432/flowdb)")
        from core.flow_store_sql import SqlFlowStore
        return SqlFlowStore(url, provenance=prov)
    if kind == "dao":
        raise ValueError(
            "flow_recorder.store=dao is not permitted: flow history must NEVER "
            "share the application database (users/keys/audit). Use store=sqlite "
            "(separate file) or store=postgres (separate database via "
            "flow_recorder.db_url).")
    if kind == "noop":
        return NoopFlowStore()
    if kind == "paimon":
        from core.flow_store_paimon import PaimonFlowStore
        return PaimonFlowStore(opts.get("warehouse", "data/flow_warehouse"))
    if kind == "aerospike":
        from core.flow_store_aerospike import AerospikeFlowStore
        return AerospikeFlowStore(opts.get("aerospike_hosts", "127.0.0.1:3000"),
                                  namespace=opts.get("aerospike_namespace",
                                                     "dishtayantra"),
                                  provenance=prov)
    raise ValueError(f"Unknown flow_recorder.store '{kind}' (expected one of: "
                     f"sqlite, postgres, dao, noop, paimon, aerospike)")


class NoopFlowStore(FlowStore):
    """Persists nothing. Useful to keep wiring in place with zero storage."""

    def write_batch(self, events):
        pass

    def query(self, *a, **k):
        return []

    def state_at(self, *a, **k):
        return {"nodes": {}, "ts_ms": None}

    def purge_older_than_ms(self, cutoff_ms, batch=DEFAULT_PURGE_BATCH):
        return 0

    def maintenance(self, mode="none"):
        return False

    def distinct_dags(self):
        return []

    def iter_export(self, *a, **k):
        return iter(())


class SqliteFlowStore(FlowStore):
    """Append-only SQLite store, indexed on (dag_id, ts_ms).

    A SEPARATE database file from the application DB. One persistent connection
    is used for writes (the single drain thread); reads open short-lived
    connections so the UI never contends with the writer. WAL mode makes
    concurrent read-during-write safe.
    """

    def __init__(self, path="data/flow_history.db", provenance=None):
        self.path = path
        self._prov = normalise_provenance(provenance)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._wlock = threading.Lock()
        self._wconn = sqlite3.connect(path, check_same_thread=False)
        self._wconn.execute("PRAGMA journal_mode=WAL")
        self._wconn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema(self._wconn)
        # distinct_dags() is a full index-scan aggregate (GROUP BY over every
        # event); at tens of millions of rows that is seconds. It feeds the DAG
        # inventory, not a hot path, so a short TTL cache bounds how often the
        # scan runs while keeping the list fresh enough. (WAL means the scan
        # never blocks the writer regardless.)
        self._dags_ttl = DEFAULT_DAGS_CACHE_TTL
        self._dags_cache = None        # (expires_at_monotonic, result)

    @staticmethod
    def _init_schema(conn):
        """Apply the single dedicated flow DDL file (no migrations).

        The schema is defined once in config/schema/flow_events_sqlite.sql and
        applied verbatim here (CREATE TABLE/INDEX IF NOT EXISTS). There is no
        ALTER-based migration path: the file is the single source of truth, so a
        flow DB that predates a schema change must be recreated (it is transient
        history, default 24h retention, in its own file separate from the app DB).
        """
        with open(FLOW_SQLITE_SCHEMA_FILE, "r", encoding="utf-8") as fh:
            conn.executescript(fh.read())
        conn.commit()

    def write_batch(self, events):
        if not events:
            return
        inst, host, port = (self._prov["instance"], self._prov["host"],
                            self._prov["port"])
        rows = [(e["dag_id"], e["node_id"], e["seq"], e["ts_ms"],
                 e.get("inputs_json"), e.get("output_json"),
                 e.get("targets_json"), e.get("compute_us"),
                 e.get("instance", inst), e.get("host", host),
                 e.get("port", port), e.get("cycle_id")) for e in events]
        with self._wlock:
            self._wconn.executemany(
                "INSERT INTO flow_events (dag_id, node_id, seq, ts_ms, "
                "inputs_json, output_json, targets_json, compute_us, "
                "instance, host, port, cycle_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows)
            self._wconn.commit()

    def _read_conn(self):
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def query(self, dag_id, t0_ms=None, t1_ms=None, nodes=None, limit=5000,
              after_seq=None, instance=None):
        sql = ["SELECT * FROM flow_events WHERE dag_id = ?"]
        args = [dag_id]
        if instance is not None:
            sql.append("AND instance = ?"); args.append(instance)
        if t0_ms is not None:
            sql.append("AND ts_ms >= ?"); args.append(int(t0_ms))
        if t1_ms is not None:
            sql.append("AND ts_ms <= ?"); args.append(int(t1_ms))
        if nodes:
            sql.append("AND node_id IN (%s)" % ",".join("?" * len(nodes)))
            args.extend(nodes)
        if after_seq is not None:
            sql.append("AND seq > ?"); args.append(int(after_seq))
        sql.append("ORDER BY seq ASC LIMIT ?"); args.append(int(limit))
        conn = self._read_conn()
        try:
            cur = conn.execute(" ".join(sql), args)
            return [self._row_to_dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    def state_at(self, dag_id, ts_ms, instance=None):
        """Latest output per node at or before ``ts_ms`` (state reconstruction).

        ``instance`` scopes to one origin when several servers share this DB.
        """
        clause = "dag_id = ?" + (" AND instance = ?" if instance is not None else "")
        inner_args = [dag_id] + ([instance] if instance is not None else []) + [int(ts_ms)]
        outer_args = [dag_id] + ([instance] if instance is not None else [])
        conn = self._read_conn()
        try:
            cur = conn.execute(f"""
                SELECT e.* FROM flow_events e
                JOIN (SELECT node_id, MAX(ts_ms) mts FROM flow_events
                      WHERE {clause} AND ts_ms <= ? GROUP BY node_id) last
                ON e.node_id = last.node_id AND e.ts_ms = last.mts
                WHERE {clause}""", (*inner_args, *outer_args))
            nodes = {r["node_id"]: self._row_to_dict(r) for r in cur.fetchall()}
            return {"ts_ms": int(ts_ms), "nodes": nodes}
        finally:
            conn.close()

    def iter_export(self, dag_id, t0_ms=None, t1_ms=None, nodes=None,
                    chunk=2000, instance=None):
        """Generator over events for streaming export (memory-safe)."""
        after = 0
        while True:
            batch = self.query(dag_id, t0_ms, t1_ms, nodes, limit=chunk,
                               after_seq=after, instance=instance)
            if not batch:
                return
            for e in batch:
                yield e
            after = batch[-1]["seq"]
            if len(batch) < chunk:
                return

    def histogram(self, dag_id, t0_ms, t1_ms, buckets=120, instance=None):
        """Fast per-bucket cycle/event counts via a single GROUP BY aggregate
        (no events leave the DB). Reads on a separate connection (WAL: never
        blocks the writer)."""
        buckets = max(1, min(int(buckets), 1000))
        t0, t1 = int(t0_ms), int(t1_ms)
        span = max(1, t1 - t0)
        width = span / buckets
        sql = ("SELECT CAST((ts_ms - ?) * ? / ? AS INTEGER) AS bkt, "
               "COUNT(*) AS events, COUNT(DISTINCT cycle_id) AS cycles "
               "FROM flow_events WHERE dag_id=? AND ts_ms>=? AND ts_ms<? ")
        params = [t0, buckets, span, dag_id, t0, t1]
        if instance:
            sql += "AND instance=? "
            params.append(instance)
        sql += "GROUP BY bkt"
        conn = self._read_conn()
        try:
            agg = {}
            for r in conn.execute(sql, params):
                if r["bkt"] is None:
                    continue
                b = int(r["bkt"])
                b = 0 if b < 0 else (buckets - 1 if b >= buckets else b)
                e, c = agg.get(b, (0, 0))
                agg[b] = (e + r["events"], c + r["cycles"])
        finally:
            conn.close()
        out = [{"i": i, "t0": int(t0 + i * width), "t1": int(t0 + (i + 1) * width),
                "events": agg.get(i, (0, 0))[0], "cycles": agg.get(i, (0, 0))[1]}
               for i in range(buckets)]
        return {"from": t0, "to": t1, "bucket_ms": width, "buckets": out}

    def purge_older_than_ms(self, cutoff_ms, batch=DEFAULT_PURGE_BATCH):
        """Delete events older than ``cutoff_ms`` in bounded batches.

        Each batch is its own short transaction and the write lock is released
        between batches, so a large sweep (e.g. deleting an hour of history at a
        high event rate) never holds the writer for long and the drain thread
        keeps making progress. Returns the total rows removed.
        """
        cutoff = int(cutoff_ms)
        batch = max(int(batch), 1)
        total = 0
        while True:
            with self._wlock:
                cur = self._wconn.execute(
                    "DELETE FROM flow_events WHERE id IN ("
                    "  SELECT id FROM flow_events WHERE ts_ms < ? "
                    "  ORDER BY id LIMIT ?)", (cutoff, batch))
                self._wconn.commit()
                n = cur.rowcount or 0
            total += n
            if n < batch:    # lock released between batches (drain can interleave)
                break
        return total

    def maintenance(self, mode="none"):
        """Optional on-disk reclaim for the dedicated flow DB. Returns True if
        it did work.

        Usually unnecessary: deletes free pages that subsequent inserts reuse,
        so the file plateaus at steady state. Modes:
          * ``none``        - do nothing (default).
          * ``incremental`` - PRAGMA incremental_vacuum (bounded, short lock);
            only reclaims if the DB was created with auto_vacuum=INCREMENTAL,
            otherwise it is a no-op.
          * ``full``        - VACUUM: full rebuild under an exclusive lock; run
            off-hours. Safe here because this is a dedicated file and the
            recorder is decoupled from DAG execution.
        """
        mode = (mode or "none").lower()
        if mode not in ("incremental", "full"):
            return False
        with self._wlock:
            if mode == "incremental":
                self._wconn.execute("PRAGMA incremental_vacuum")
            else:
                self._wconn.execute("VACUUM")
            self._wconn.commit()
        return True

    def distinct_dags(self):
        now = time.monotonic()
        c = self._dags_cache
        if c is not None and now < c[0]:
            return c[1]
        result = self._distinct_dags_uncached()
        self._dags_cache = (now + self._dags_ttl, result)
        return result

    def _distinct_dags_uncached(self):
        conn = self._read_conn()
        try:
            cur = conn.execute(
                "SELECT instance, dag_id, COUNT(*) n, MIN(ts_ms) mn, "
                "MAX(ts_ms) mx FROM flow_events GROUP BY instance, dag_id "
                "ORDER BY instance, dag_id")
            return [{"dag_id": r["dag_id"], "instance": r["instance"],
                     "count": r["n"], "min_ts": r["mn"], "max_ts": r["mx"]}
                    for r in cur.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def _row_to_dict(r):
        keys = r.keys()
        return {
            "id": r["id"], "dag_id": r["dag_id"], "node_id": r["node_id"],
            "seq": r["seq"], "ts_ms": r["ts_ms"],
            "inputs": _loads(r["inputs_json"]),
            "output": _loads(r["output_json"]),
            "targets": _loads(r["targets_json"]) or [],
            "compute_us": r["compute_us"],
            "cycle_id": r["cycle_id"] if "cycle_id" in keys else None,
            "instance": r["instance"] if "instance" in keys else None,
            "host": r["host"] if "host" in keys else None,
            "port": r["port"] if "port" in keys else None,
        }


def _loads(s):
    if s is None:
        return None
    try:
        return json.loads(s)
    except Exception:  # noqa: BLE001
        return s
