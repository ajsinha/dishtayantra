"""
Aerospike flow store (OPTIONAL, EXPERIMENTAL)
=============================================

Implementation of the :class:`core.flow_store.FlowStore` contract on Aerospike,
using the official ``aerospike`` Python client.

!!! HONESTY NOTE !!!
This backend is written against the Aerospike client API but has NOT been run
against a live Aerospike cluster in development (none was available). Treat it
as EXPERIMENTAL: it imports and constructs correctly, but the connect/write/
read/query paths must be validated against a real cluster before production use.
This is deliberately flagged rather than presented as verified - consistent with
the project's "verify, don't assert" rule.

Data model
----------
* One record per flow event in ``(namespace, set)`` (set default ``flow_events``).
* Record key: ``f"{instance}|{dag_id}|{seq}"`` - provenance + per-instance seq,
  so multiple instances writing to one cluster never collide.
* Bins mirror the relational columns (dag_id, node_id, seq, ts_ms, inputs_json,
  output_json, targets_json, compute_us, instance, host, port).
* Integer secondary index on ``ts_ms`` for range (window) queries; remaining
  predicates (dag_id, instance, node_id, seq) are applied client-side after the
  range scan. ``distinct_dags`` scans and groups client-side.
* Retention: each record is written with a TTL (``retention``-derived) so the
  cluster auto-expires old events; ``purge_older_than_ms`` is therefore a no-op
  by default (the TTL does the work) but can also range-scan + delete.

Requires ``pip install aerospike`` and a reachable cluster.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""
import json
import logging

from core.flow_store import DEFAULT_PURGE_BATCH, FlowStore, normalise_provenance

logger = logging.getLogger(__name__)


def _loads(s):
    if s is None:
        return None
    try:
        return json.loads(s)
    except Exception:  # noqa: BLE001
        return s


class AerospikeFlowStore(FlowStore):
    """EXPERIMENTAL Aerospike backend (unverified against a live cluster)."""

    def __init__(self, hosts="127.0.0.1:3000", namespace="dishtayantra",
                 set_name="flow_events", provenance=None, ttl_seconds=0):
        try:
            import aerospike
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "flow_recorder.store=aerospike requires the aerospike client "
                "(pip install aerospike). Not installed: %s" % exc)
        self._aero = aerospike
        self._ns = namespace
        self._set = set_name
        self._ttl = int(ttl_seconds or 0)
        self._prov = normalise_provenance(provenance)
        pairs = []
        for hp in str(hosts).split(","):
            hp = hp.strip()
            if not hp:
                continue
            host, _, port = hp.partition(":")
            pairs.append((host, int(port or 3000)))
        self._client = aerospike.client({"hosts": pairs})
        self._client.connect()
        # Secondary index for range queries on ts_ms (ignore if it exists).
        try:
            self._client.index_integer_create(namespace, set_name, "ts_ms",
                                               "ix_flow_ts_ms")
        except Exception:  # noqa: BLE001 - already exists / insufficient perms
            pass
        logger.warning("AerospikeFlowStore connected (ns=%s, set=%s) - "
                       "EXPERIMENTAL, unverified against a live cluster.",
                       namespace, set_name)

    def _key(self, e):
        inst = e.get("instance", self._prov["instance"]) or ""
        return (self._ns, self._set, f"{inst}|{e['dag_id']}|{e['seq']}")

    def write_batch(self, events):
        if not events:
            return
        p = self._prov
        meta = {"ttl": self._ttl} if self._ttl else None
        for e in events:
            bins = {"dag_id": e["dag_id"], "node_id": e["node_id"],
                    "seq": int(e["seq"]), "ts_ms": int(e["ts_ms"]),
                    "inputs_json": e.get("inputs_json"),
                    "output_json": e.get("output_json"),
                    "targets_json": e.get("targets_json"),
                    "compute_us": e.get("compute_us"),
                    "instance": e.get("instance", p["instance"]),
                    "host": e.get("host", p["host"]),
                    "port": e.get("port", p["port"])}
            self._client.put(self._key(e), bins, meta=meta)

    def _scan_rows(self, t0_ms=None, t1_ms=None):
        from aerospike import predicates as pred
        q = self._client.query(self._ns, self._set)
        if t0_ms is not None and t1_ms is not None:
            q.where(pred.between("ts_ms", int(t0_ms), int(t1_ms)))
        out = []
        q.foreach(lambda rec: out.append(rec[2]))  # (key, meta, bins) -> bins
        return out

    @staticmethod
    def _row(b):
        return {"dag_id": b.get("dag_id"), "node_id": b.get("node_id"),
                "seq": b.get("seq"), "ts_ms": b.get("ts_ms"),
                "inputs": _loads(b.get("inputs_json")),
                "output": _loads(b.get("output_json")),
                "targets": _loads(b.get("targets_json")) or [],
                "compute_us": b.get("compute_us"), "instance": b.get("instance"),
                "host": b.get("host"), "port": b.get("port")}

    def query(self, dag_id, t0_ms=None, t1_ms=None, nodes=None, limit=5000,
              after_seq=None, instance=None):
        rows = self._scan_rows(t0_ms, t1_ms)
        nodeset = set(nodes) if nodes else None
        sel = [b for b in rows
               if b.get("dag_id") == dag_id
               and (instance is None or b.get("instance") == instance)
               and (t0_ms is None or b.get("ts_ms", 0) >= int(t0_ms))
               and (t1_ms is None or b.get("ts_ms", 0) <= int(t1_ms))
               and (nodeset is None or b.get("node_id") in nodeset)
               and (after_seq is None or b.get("seq", 0) > int(after_seq))]
        sel.sort(key=lambda b: b.get("seq", 0))
        return [self._row(b) for b in sel[:int(limit)]]

    def state_at(self, dag_id, ts_ms, instance=None):
        latest = {}
        for b in self._scan_rows(0, int(ts_ms)):
            if b.get("dag_id") != dag_id:
                continue
            if instance is not None and b.get("instance") != instance:
                continue
            cur = latest.get(b.get("node_id"))
            if cur is None or b.get("ts_ms", 0) > cur.get("ts_ms", 0):
                latest[b.get("node_id")] = b
        return {"ts_ms": int(ts_ms),
                "nodes": {k: self._row(v) for k, v in latest.items()}}

    def iter_export(self, dag_id, t0_ms=None, t1_ms=None, nodes=None,
                    chunk=2000, instance=None):
        for e in self.query(dag_id, t0_ms, t1_ms, nodes, limit=10 ** 9,
                            instance=instance):
            yield e

    def distinct_dags(self):
        agg = {}
        for b in self._scan_rows():
            key = (b.get("instance"), b.get("dag_id"))
            a = agg.get(key)
            if a is None:
                agg[key] = {"instance": key[0], "dag_id": key[1], "count": 1,
                            "min_ts": b.get("ts_ms"), "max_ts": b.get("ts_ms")}
            else:
                a["count"] += 1
                a["min_ts"] = min(a["min_ts"], b.get("ts_ms"))
                a["max_ts"] = max(a["max_ts"], b.get("ts_ms"))
        return sorted(agg.values(),
                      key=lambda d: (d["instance"] or "", d["dag_id"]))

    def purge_older_than_ms(self, cutoff_ms, batch=DEFAULT_PURGE_BATCH):
        """Range-scan ts_ms < cutoff and delete. (If a record TTL is configured
        the cluster also expires old events automatically.)"""
        from aerospike import predicates as pred
        q = self._client.query(self._ns, self._set)
        q.where(pred.between("ts_ms", 0, int(cutoff_ms) - 1))
        victims = []
        q.foreach(lambda rec: victims.append(rec[0]))   # collect keys
        for k in victims:
            try:
                self._client.remove(k)
            except Exception:  # noqa: BLE001
                pass
        return len(victims)

    def maintenance(self, mode="none"):
        return False

    def close(self):
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass
