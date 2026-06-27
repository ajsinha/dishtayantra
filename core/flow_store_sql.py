"""
SQLAlchemy flow store - a SEPARATE relational database for flow history
=======================================================================

:class:`SqlFlowStore` persists flow events to its **own** SQLAlchemy engine,
independent of the application database. The same code serves SQLite or
PostgreSQL (and any other SQLAlchemy dialect) - you only change the URL:

    flow_recorder.store   = postgres
    flow_recorder.db_url  = postgresql+psycopg://user:pass@db-host:5432/flowdb
    # or a separate sqlite file:
    flow_recorder.db_url  = sqlite:////var/lib/dishtayantra/flow_history.db

Because the engine is separate from the app DB, flow history never contends
with the users/keys/audit database. A central Postgres also lets MANY instances
write to one shared flow database; every row carries provenance
``(instance, host, port)`` so queries can be scoped to one origin (see the
``instance`` argument).

PostgreSQL requires a driver, e.g. ``pip install psycopg[binary]`` (psycopg 3,
URL ``postgresql+psycopg://``) or ``psycopg2-binary`` (``postgresql+psycopg2://``).
The store reuses the canonical :class:`core.db.models.FlowEvent` table so the
schema matches the rest of the system; see config/schema/flow_events_postgres.sql
for the standalone DDL.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""
import logging

from sqlalchemy import (asc, create_engine, delete, func, select)
from sqlalchemy.orm import sessionmaker

from core.db.models import FlowEvent
from core.flow_store import (DEFAULT_DAGS_CACHE_TTL, DEFAULT_PURGE_BATCH,
                             FlowStore, normalise_provenance)

logger = logging.getLogger(__name__)


class SqlFlowStore(FlowStore):
    """Flow store backed by a dedicated SQLAlchemy engine (sqlite or postgres)."""

    def __init__(self, db_url, provenance=None, pool_size=5, echo=False):
        self.db_url = db_url
        self._prov = normalise_provenance(provenance)
        kw = {"future": True, "echo": echo}
        if not db_url.startswith("sqlite"):
            kw.update(pool_size=pool_size, max_overflow=pool_size,
                      pool_pre_ping=True)
        self.engine = create_engine(db_url, **kw)
        # Schema provisioning policy (mirrors the application DB):
        #   - SQLite     -> the application CREATES the schema from the dedicated
        #                   file (config/schema/flow_events_sqlite.sql).
        #   - PostgreSQL -> the application does NOT create the schema; apply
        #                   config/schema/flow_events_postgres.sql once yourself.
        #                   We only validate the table exists and fail clearly.
        self._ensure_schema()
        self._Session = sessionmaker(bind=self.engine, future=True)
        self._dags_ttl = DEFAULT_DAGS_CACHE_TTL
        self._dags_cache = None
        logger.info("SqlFlowStore configured (dialect=%s) - separate from the "
                    "application database", self.engine.dialect.name)

    def _ensure_schema(self):
        from sqlalchemy import inspect as sa_inspect, text
        dialect = self.engine.dialect.name
        if dialect in ("postgresql", "postgres"):
            # Do NOT create on PostgreSQL - provisioned manually, once.
            if not sa_inspect(self.engine).has_table("flow_events"):
                raise RuntimeError(
                    "flow_events table not found in the flow database. On "
                    "PostgreSQL the application does not create the flow schema; "
                    "apply config/schema/flow_events_postgres.sql once "
                    "(psql -f config/schema/flow_events_postgres.sql) and retry.")
            return
        if dialect == "sqlite":
            from core.flow_store import FLOW_SQLITE_SCHEMA_FILE
            with open(FLOW_SQLITE_SCHEMA_FILE, "r", encoding="utf-8") as fh:
                body = "\n".join(ln for ln in fh.read().splitlines()
                                 if not ln.strip().startswith("--"))
            with self.engine.begin() as conn:
                for stmt in body.split(";"):
                    if stmt.strip():
                        conn.execute(text(stmt))
            return
        # Other dialects are undocumented; create from the model as a courtesy.
        FlowEvent.__table__.create(self.engine, checkfirst=True)

    # ----------------------------------------------------------------- write
    def write_batch(self, events):
        if not events:
            return
        p = self._prov
        rows = [{"dag_id": e["dag_id"], "node_id": e["node_id"], "seq": e["seq"],
                 "ts_ms": e["ts_ms"], "inputs_json": e.get("inputs_json"),
                 "output_json": e.get("output_json"),
                 "targets_json": e.get("targets_json"),
                 "compute_us": e.get("compute_us"),
                 "cycle_id": e.get("cycle_id"),
                 "instance": e.get("instance", p["instance"]),
                 "host": e.get("host", p["host"]),
                 "port": e.get("port", p["port"])} for e in events]
        with self._Session() as s:
            s.execute(FlowEvent.__table__.insert(), rows)
            s.commit()

    # ------------------------------------------------------------------ read
    def query(self, dag_id, t0_ms=None, t1_ms=None, nodes=None, limit=5000,
              after_seq=None, instance=None):
        stmt = select(FlowEvent).where(FlowEvent.dag_id == dag_id)
        if instance is not None:
            stmt = stmt.where(FlowEvent.instance == instance)
        if t0_ms is not None:
            stmt = stmt.where(FlowEvent.ts_ms >= int(t0_ms))
        if t1_ms is not None:
            stmt = stmt.where(FlowEvent.ts_ms <= int(t1_ms))
        if nodes:
            stmt = stmt.where(FlowEvent.node_id.in_(nodes))
        if after_seq is not None:
            stmt = stmt.where(FlowEvent.seq > int(after_seq))
        stmt = stmt.order_by(asc(FlowEvent.seq)).limit(int(limit))
        with self._Session() as s:
            return [e.to_dict() for e in s.execute(stmt).scalars().all()]

    def state_at(self, dag_id, ts_ms, instance=None):
        where = [FlowEvent.dag_id == dag_id, FlowEvent.ts_ms <= int(ts_ms)]
        outer = [FlowEvent.dag_id == dag_id]
        if instance is not None:
            where.append(FlowEvent.instance == instance)
            outer.append(FlowEvent.instance == instance)
        from sqlalchemy import and_
        sub = (select(FlowEvent.node_id, func.max(FlowEvent.ts_ms).label("mts"))
               .where(*where).group_by(FlowEvent.node_id).subquery())
        stmt = (select(FlowEvent).join(
            sub, and_(FlowEvent.node_id == sub.c.node_id,
                      FlowEvent.ts_ms == sub.c.mts)).where(*outer))
        with self._Session() as s:
            nodes = {e.node_id: e.to_dict()
                     for e in s.execute(stmt).scalars().all()}
        return {"ts_ms": int(ts_ms), "nodes": nodes}

    def iter_export(self, dag_id, t0_ms=None, t1_ms=None, nodes=None,
                    chunk=2000, instance=None):
        after = 0
        while True:
            batch = self.query(dag_id, t0_ms, t1_ms, nodes, chunk, after,
                               instance=instance)
            if not batch:
                return
            for e in batch:
                yield e
            after = batch[-1]["seq"]
            if len(batch) < chunk:
                return

    def distinct_dags(self):
        import time
        now = time.monotonic()
        c = self._dags_cache
        if c is not None and now < c[0]:
            return c[1]
        stmt = (select(FlowEvent.instance, FlowEvent.dag_id,
                       func.count().label("n"),
                       func.min(FlowEvent.ts_ms).label("mn"),
                       func.max(FlowEvent.ts_ms).label("mx"))
                .group_by(FlowEvent.instance, FlowEvent.dag_id)
                .order_by(FlowEvent.instance, FlowEvent.dag_id))
        with self._Session() as s:
            rows = s.execute(stmt).all()
        result = [{"instance": r[0], "dag_id": r[1], "count": r[2],
                   "min_ts": r[3], "max_ts": r[4]} for r in rows]
        self._dags_cache = (now + self._dags_ttl, result)
        return result

    # --------------------------------------------------------------- retention
    def purge_older_than_ms(self, cutoff_ms, batch=DEFAULT_PURGE_BATCH):
        cutoff = int(cutoff_ms)
        batch = max(int(batch), 1)
        total = 0
        while True:
            sub = (select(FlowEvent.id).where(FlowEvent.ts_ms < cutoff)
                   .order_by(FlowEvent.id).limit(batch))
            with self._Session() as s:
                n = s.execute(delete(FlowEvent).where(
                    FlowEvent.id.in_(sub))).rowcount or 0
                s.commit()
            total += n
            if n < batch:
                break
        return total

    def maintenance(self, mode="none"):
        # Reclaim is engine-specific and operational (e.g. Postgres autovacuum
        # handles dead tuples). We intentionally do not issue VACUUM here; the
        # dedicated DB plateaus the same way and DBAs own server-level upkeep.
        return False
