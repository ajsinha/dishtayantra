"""
Paimon flow store (Apache Paimon via pure-Python PyPaimon >= 1.4)
=================================================================

Real implementation of the :class:`core.flow_store.FlowStore` contract on an
append-only, **hourly-partitioned** Apache Paimon table, using the pure-Python
PyPaimon SDK (no JVM). A local filesystem warehouse works out of the box; the
same code targets object-store warehouses by warehouse URI.

* Append-only table partitioned by ``dt`` = ``YYYYMMDDHH`` (UTC hour) - matches
  Paimon's strength and makes retention a cheap *partition drop* (append tables
  do not support row deletes).
* Reads push down predicates (dag_id, instance, ts range, node set, seq);
  ordering/limit/after_seq are applied client-side (Paimon has no ORDER BY).
* Provenance (instance, host, port) stored per row so instances can share a
  warehouse.

Import-guarded: needs ``pip install pypaimon`` (pulls pyarrow).

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""
import datetime as _dt
import json
import logging

from core.flow_store import DEFAULT_PURGE_BATCH, FlowStore, normalise_provenance

logger = logging.getLogger(__name__)

_FIELDS = ["dag_id", "node_id", "seq", "ts_ms", "inputs_json", "output_json",
           "targets_json", "compute_us", "instance", "host", "port", "dt"]


def _hour(ts_ms):
    return _dt.datetime.fromtimestamp(
        int(ts_ms) / 1000.0, _dt.timezone.utc).strftime("%Y%m%d%H")


def _loads(s):
    if s is None:
        return None
    try:
        return json.loads(s)
    except Exception:  # noqa: BLE001
        return s


class PaimonFlowStore(FlowStore):
    """Append-only, hourly-partitioned Apache Paimon backend (PyPaimon)."""

    def __init__(self, warehouse="data/flow_warehouse", database="flow",
                 table="flow_events", provenance=None):
        try:
            import pyarrow as pa
            from pypaimon import CatalogFactory, Schema
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "flow_recorder.store=paimon requires pypaimon + pyarrow "
                "(pip install pypaimon). Not available: %s" % exc)
        self._pa = pa
        self._prov = normalise_provenance(provenance)
        self._ident = f"{database}.{table}"
        self._warned_no_drop = False
        self._schema = pa.schema([
            ("dag_id", pa.string()), ("node_id", pa.string()),
            ("seq", pa.int64()), ("ts_ms", pa.int64()),
            ("inputs_json", pa.string()), ("output_json", pa.string()),
            ("targets_json", pa.string()), ("compute_us", pa.int64()),
            ("instance", pa.string()), ("host", pa.string()),
            ("port", pa.int64()), ("dt", pa.string())])
        self._catalog = CatalogFactory.create({"warehouse": warehouse})
        self._catalog.create_database(database, ignore_if_exists=True)
        schema = Schema.from_pyarrow_schema(
            self._schema, partition_keys=["dt"], options={"bucket": "-1"})
        self._catalog.create_table(self._ident, schema, ignore_if_exists=True)
        logger.info("PaimonFlowStore configured (warehouse=%s, table=%s), "
                    "append-only, hourly-partitioned.", warehouse, self._ident)

    def _table(self):
        return self._catalog.get_table(self._ident)

    def write_batch(self, events):
        if not events:
            return
        p = self._prov
        cols = {f: [] for f in _FIELDS}
        for e in events:
            cols["dag_id"].append(e["dag_id"])
            cols["node_id"].append(e["node_id"])
            cols["seq"].append(int(e["seq"]))
            cols["ts_ms"].append(int(e["ts_ms"]))
            cols["inputs_json"].append(e.get("inputs_json"))
            cols["output_json"].append(e.get("output_json"))
            cols["targets_json"].append(e.get("targets_json"))
            cols["compute_us"].append(e.get("compute_us"))
            cols["instance"].append(e.get("instance", p["instance"]))
            cols["host"].append(e.get("host", p["host"]))
            cols["port"].append(e.get("port", p["port"]))
            cols["dt"].append(_hour(e["ts_ms"]))
        wb = self._table().new_batch_write_builder()
        w = wb.new_write()
        commit = wb.new_commit()
        try:
            w.write_arrow(self._pa.Table.from_pydict(cols, schema=self._schema))
            commit.commit(w.prepare_commit())
        finally:
            w.close()

    def _scan(self, dag_id=None, instance=None, t0_ms=None, t1_ms=None,
              nodes=None, after_seq=None):
        rb = self._table().new_read_builder()
        pb = rb.new_predicate_builder()
        preds = []
        if dag_id is not None:
            preds.append(pb.equal("dag_id", dag_id))
        if instance is not None:
            preds.append(pb.equal("instance", instance))
        if t0_ms is not None:
            preds.append(pb.greater_or_equal("ts_ms", int(t0_ms)))
        if t1_ms is not None:
            preds.append(pb.less_or_equal("ts_ms", int(t1_ms)))
        if nodes:
            preds.append(pb.is_in("node_id", list(nodes)))
        if after_seq is not None:
            preds.append(pb.greater_than("seq", int(after_seq)))
        if preds:
            rb = rb.with_filter(preds[0] if len(preds) == 1
                                else pb.and_predicates(preds))
        scan = rb.new_scan()
        arrow = rb.new_read().to_arrow(scan.plan().splits())
        return arrow.to_pylist() if arrow is not None else []

    @staticmethod
    def _row(r):
        return {"dag_id": r["dag_id"], "node_id": r["node_id"], "seq": r["seq"],
                "ts_ms": r["ts_ms"], "inputs": _loads(r.get("inputs_json")),
                "output": _loads(r.get("output_json")),
                "targets": _loads(r.get("targets_json")) or [],
                "compute_us": r.get("compute_us"), "instance": r.get("instance"),
                "host": r.get("host"), "port": r.get("port")}

    def query(self, dag_id, t0_ms=None, t1_ms=None, nodes=None, limit=5000,
              after_seq=None, instance=None):
        rows = self._scan(dag_id, instance, t0_ms, t1_ms, nodes, after_seq)
        rows.sort(key=lambda r: r["seq"])
        return [self._row(r) for r in rows[:int(limit)]]

    def state_at(self, dag_id, ts_ms, instance=None):
        latest = {}
        for r in self._scan(dag_id, instance, t1_ms=int(ts_ms)):
            cur = latest.get(r["node_id"])
            if cur is None or r["ts_ms"] > cur["ts_ms"]:
                latest[r["node_id"]] = r
        return {"ts_ms": int(ts_ms),
                "nodes": {k: self._row(v) for k, v in latest.items()}}

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
        agg = {}
        for r in self._scan():
            key = (r.get("instance"), r["dag_id"])
            a = agg.get(key)
            if a is None:
                agg[key] = {"instance": key[0], "dag_id": key[1], "count": 1,
                            "min_ts": r["ts_ms"], "max_ts": r["ts_ms"]}
            else:
                a["count"] += 1
                a["min_ts"] = min(a["min_ts"], r["ts_ms"])
                a["max_ts"] = max(a["max_ts"], r["ts_ms"])
        return sorted(agg.values(),
                      key=lambda d: (d["instance"] or "", d["dag_id"]))

    def purge_older_than_ms(self, cutoff_ms, batch=DEFAULT_PURGE_BATCH):
        """Drop whole partitions (hours) older than the cutoff hour.

        Returns the number of rows in the dropped partitions (best-effort, from
        Paimon's per-partition record_count).

        NOTE: PyPaimon's local *filesystem* catalog does NOT support programmatic
        partition drop (it raises NotImplementedError - use a REST catalog). On
        such catalogs this logs a one-time warning and returns 0; bound retention
        there via the table option ``partition.expiration-time`` or a REST
        catalog. Read/write/query are fully functional regardless.
        """
        cutoff_hour = _hour(cutoff_ms)
        drop, rows = [], 0
        try:
            token = None
            while True:
                page = self._catalog.list_partitions_paged(
                    self._ident, page_token=token)
                for part in (getattr(page, "elements", None) or []):
                    spec = getattr(part, "spec", None) or {}
                    dt = spec.get("dt")
                    if dt and dt < cutoff_hour:
                        drop.append(dt)
                        rows += int(getattr(part, "record_count", 0) or 0)
                token = getattr(page, "next_page_token", None)
                if not token:
                    break
        except Exception:  # noqa: BLE001 - listing API variance
            return 0
        if not drop:
            return 0
        try:
            self._catalog.drop_partitions(self._ident, [{"dt": n} for n in drop])
        except NotImplementedError:
            if not self._warned_no_drop:
                self._warned_no_drop = True
                logger.warning(
                    "PaimonFlowStore: this catalog cannot drop partitions "
                    "(use a REST catalog, or set table option "
                    "partition.expiration-time for auto-expiry). Flow retention "
                    "by partition drop is a no-op here; read/write are unaffected.")
            return 0
        return rows

    def maintenance(self, mode="none"):
        return False
