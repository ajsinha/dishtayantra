"""Tests for batched flow-history purge + optional maintenance (v5.33.0)."""
import os
import tempfile
import time

from core.flow_store import SqliteFlowStore, NoopFlowStore


def _rows(n, base_ts, seq0=0):
    return [{"dag_id": "d", "node_id": "n", "seq": seq0 + i, "ts_ms": base_ts + i,
             "inputs_json": "{}", "output_json": "{}", "targets_json": None,
             "compute_us": 1} for i in range(n)]


def _store():
    d = tempfile.mkdtemp(prefix="flowtest_")
    return SqliteFlowStore(os.path.join(d, "f.db"))


def test_batched_purge_removes_all_old_and_keeps_new():
    s = _store()
    now = int(time.time() * 1000)
    s.write_batch(_rows(350, now - 10_000_000))          # old
    s.write_batch(_rows(40, now, seq0=10_000))           # fresh
    # batch smaller than the old set -> exercises the multi-batch loop
    removed = s.purge_older_than_ms(now - 5_000_000, batch=100)
    assert removed == 350
    assert len(s.query("d", limit=10_000)) == 40


def test_purge_count_matches_and_idempotent():
    s = _store()
    now = int(time.time() * 1000)
    s.write_batch(_rows(120, now - 9_000_000))
    cutoff = now - 1_000_000
    assert s.purge_older_than_ms(cutoff, batch=50) == 120
    assert s.purge_older_than_ms(cutoff, batch=50) == 0   # nothing left to remove


def test_maintenance_modes():
    s = _store()
    s.write_batch(_rows(100, int(time.time() * 1000)))
    assert s.maintenance("none") is False
    assert s.maintenance("full") is True          # VACUUM on the dedicated file
    from core.flow_store import NoopFlowStore
    assert NoopFlowStore().maintenance("full") is False


def test_dao_store_is_forbidden():
    """flow history must never share the application DB: store=dao is rejected."""
    import pytest
    from core.flow_store import make_flow_store
    with pytest.raises(ValueError):
        make_flow_store("dao")


def test_distinct_dags_is_cached():
    """The DAG inventory is served from a short TTL cache (bounds the full-table
    aggregate at scale) and refreshes when the cache expires/invalidates."""
    s = _store()
    now = int(time.time() * 1000)
    s.write_batch([dict(dag_id="a", node_id="n", seq=i, ts_ms=now + i,
                        inputs_json="{}", output_json="{}", targets_json=None,
                        compute_us=1) for i in range(10)])
    first = [d["dag_id"] for d in s.distinct_dags()]
    s.write_batch([dict(dag_id="b", node_id="n", seq=100 + i, ts_ms=now + i,
                        inputs_json="{}", output_json="{}", targets_json=None,
                        compute_us=1) for i in range(5)])
    cached = [d["dag_id"] for d in s.distinct_dags()]   # within TTL: stale snapshot
    s._dags_cache = None                                # invalidate -> recompute
    fresh = [d["dag_id"] for d in s.distinct_dags()]
    assert first == ["a"]
    assert cached == ["a"]            # 'b' not visible until the cache refreshes
    assert fresh == ["a", "b"]


# ---------------------------------------------------------------- v5.34.0 ----
def test_sqlite_store_stamps_provenance():
    import tempfile
    from core.flow_store import SqliteFlowStore
    s = SqliteFlowStore(os.path.join(tempfile.mkdtemp(), "f.db"),
                        provenance={"instance": "A", "host": "h", "port": 18080})
    s.write_batch(_rows(3, int(time.time() * 1000)))
    row = s.query("d", limit=10)[0]
    assert row["instance"] == "A" and row["host"] == "h" and row["port"] == 18080


def test_sqlite_schema_comes_from_dedicated_file():
    """No migration: a fresh flow DB gets the full schema (incl. provenance
    columns and the instance index) from the dedicated DDL file."""
    import sqlite3
    import tempfile
    from core.flow_store import SqliteFlowStore, FLOW_SQLITE_SCHEMA_FILE
    assert os.path.exists(FLOW_SQLITE_SCHEMA_FILE)
    p = os.path.join(tempfile.mkdtemp(), "fresh.db")
    s = SqliteFlowStore(p, provenance={"instance": "X", "host": "h", "port": 1})
    cols = {r[1] for r in s._wconn.execute("PRAGMA table_info(flow_events)")}
    assert {"instance", "host", "port"} <= cols
    idx = {r[1] for r in s._wconn.execute("PRAGMA index_list(flow_events)")}
    assert "ix_flow_instance_dag_ts" in idx
    s.write_batch(_rows(2, int(time.time() * 1000)))
    assert s.query("d", limit=10)[0]["instance"] == "X"


def test_sql_flow_store_shared_db_multi_instance():
    """Two instances writing the same dag name into one DB stay disambiguable."""
    import tempfile
    from core.flow_store import make_flow_store
    url = "sqlite:///" + os.path.join(tempfile.mkdtemp(), "shared.db")
    now = int(time.time() * 1000)
    a = make_flow_store("postgres", db_url=url,
                        provenance={"instance": "A", "host": "ha", "port": 1})
    b = make_flow_store("postgres", db_url=url,
                        provenance={"instance": "B", "host": "hb", "port": 2})
    a.write_batch([dict(dag_id="hb", node_id="n", seq=i, ts_ms=now + i,
                        inputs_json="{}", output_json="{}", targets_json=None,
                        compute_us=1) for i in range(5)])
    b.write_batch([dict(dag_id="hb", node_id="n", seq=i, ts_ms=now + i,
                        inputs_json="{}", output_json="{}", targets_json=None,
                        compute_us=1) for i in range(3)])
    assert len(a.query("hb", limit=100)) == 8
    assert len(a.query("hb", limit=100, instance="A")) == 5
    inv = {(r["instance"], r["dag_id"]): r["count"] for r in a.distinct_dags()}
    assert inv[("A", "hb")] == 5 and inv[("B", "hb")] == 3


def test_postgres_store_requires_url():
    import pytest
    from core.flow_store import make_flow_store
    with pytest.raises(ValueError):
        make_flow_store("postgres")          # no db_url


def test_abc_blocks_incomplete_backend():
    import pytest
    from core.flow_store import FlowStore
    class Bad(FlowStore):
        pass
    with pytest.raises(TypeError):           # abstract methods unimplemented
        Bad()


def test_aerospike_requires_client_or_cluster():
    """Aerospike backend is experimental: without the client it raises
    RuntimeError; with the client but no cluster, construction fails at connect.
    Either way it never silently succeeds without a real backend."""
    import pytest
    from core.flow_store_aerospike import AerospikeFlowStore
    with pytest.raises(Exception):
        AerospikeFlowStore(hosts="127.0.0.1:3000", provenance={"instance": "A"})


def test_paimon_roundtrip_if_available():
    """Real Paimon backend: write -> filtered/window read -> state_at ->
    distinct_dags, on a local filesystem warehouse. Skips if pypaimon absent."""
    import pytest
    import tempfile
    try:
        from core.flow_store_paimon import PaimonFlowStore
    except Exception:
        pytest.skip("pypaimon not importable")
    try:
        s = PaimonFlowStore(warehouse=tempfile.mkdtemp(prefix="pmtest_"),
                            provenance={"instance": "A", "host": "h", "port": 1})
    except RuntimeError:
        pytest.skip("pypaimon/pyarrow not installed")
    now = int(time.time() * 1000)
    s.write_batch([dict(dag_id="d", node_id="n%d" % (i % 2), seq=i, ts_ms=now + i,
                        inputs_json="{}", output_json='{"v":%d}' % i,
                        targets_json=None, compute_us=i) for i in range(6)])
    rows = s.query("d", limit=100)
    assert len(rows) == 6
    assert rows[0]["instance"] == "A"
    st = s.state_at("d", now + 100)
    assert set(st["nodes"].keys()) == {"n0", "n1"}
    inv = {(r["instance"], r["dag_id"]): r["count"] for r in s.distinct_dags()}
    assert inv[("A", "d")] == 6
