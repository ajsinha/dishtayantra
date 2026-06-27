#!/usr/bin/env python3
"""Query-side benchmark for Flow Time-Travel history (SQLite backend).

Purpose: replace intuition with numbers when asking "will the single-file
SQLite flow store scale?". It builds an isolated temp database with the REAL
``flow_events`` schema + indexes, fills it with a realistic bounded volume, and
times the actual ``FlowDAO`` query paths used in production (time-window query,
state-at reconstruction, distinct-dags) plus the batched-insert throughput.

This does NOT touch the app's configured database (it uses a private temp DB),
so it is safe to run anywhere.

Examples
--------
    # default: ~1M rows across 20 dags
    python3 perftest/bench_flow_queries.py

    # model 24h at 50 events/sec (=4.32M rows), 40 dags, p95 over 50 probes
    python3 perftest/bench_flow_queries.py --rate 50 --hours 24 --dags 40 --iters 50

    # specify total rows directly
    python3 perftest/bench_flow_queries.py --rows 5000000

    # retention CHURN/plateau: prove the file size plateaus at steady state and
    # the batched purge stays cheap (no periodic VACUUM needed to bound size)
    python3 perftest/bench_flow_queries.py --cycles 20 --rate 1000 --hours 24 --sweep-min 30

Notes
-----
* The FILL phase uses fast bulk inserts (core executemany + load-time PRAGMAs)
  purely to reach volume quickly; it is not a production write-rate claim. A
  separate measured ``FlowDAO.write_batch`` throughput number IS reported, since
  that is the real ingest path. (For full write-path/recorder benchmarking see
  perftest/bench_flow_recorder.py.)
* Queries are measured against the real DAO with the production indexes
  (ix_flow_dag_ts on (dag_id, ts_ms)), so window-query latency here reflects
  what the app would see at the same steady-state size.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""
import argparse
import contextlib
import json
import os
import random
import statistics
import sys
import tempfile
import time

# Allow running as a plain script from the repo root (python3 perftest/...).
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, insert, text
from sqlalchemy.orm import sessionmaker

from core.db.dao import FlowDAO
from core.db.models import FlowEvent


class _BenchDB:
    """Minimal DatabaseManager-shaped shim bound to a private SQLite file, so
    the real FlowDAO runs unmodified against it."""

    def __init__(self, path, fast_load=False):
        self.engine = create_engine(f"sqlite:///{path}", future=True)
        FlowEvent.__table__.create(self.engine, checkfirst=True)
        if fast_load:
            with self.engine.begin() as c:
                c.execute(text("PRAGMA journal_mode=MEMORY"))
                c.execute(text("PRAGMA synchronous=OFF"))
        self._Session = sessionmaker(bind=self.engine, future=True)

    @contextlib.contextmanager
    def session_scope(self):
        s = self._Session()
        try:
            yield s
            s.commit()
        except Exception:
            s.rollback()
            raise
        finally:
            s.close()


def _fill(db, n_rows, n_dags, nodes_per_dag, window_ms):
    """Bulk-load synthetic events spread over `window_ms` (e.g. 24h)."""
    dags = [f"dag_{i:03d}" for i in range(n_dags)]
    t0 = int(time.time() * 1000) - window_ms
    payload = json.dumps({"v": 1, "x": 3.14159, "note": "synthetic"})
    rows, seq, batch = [], 0, 20000
    started = time.perf_counter()
    with db.engine.begin() as conn:
        for i in range(n_rows):
            seq += 1
            d = dags[i % n_dags]
            rows.append({
                "dag_id": d,
                "node_id": f"{d}_node_{i % nodes_per_dag}",
                "seq": seq,
                "ts_ms": t0 + int(window_ms * (i / n_rows)),
                "inputs_json": payload, "output_json": payload,
                "targets_json": None, "compute_us": 42,
            })
            if len(rows) >= batch:
                conn.execute(insert(FlowEvent.__table__), rows)
                rows = []
        if rows:
            conn.execute(insert(FlowEvent.__table__), rows)
    elapsed = time.perf_counter() - started
    return dags, t0, elapsed


def _timeit(fn, iters):
    samples = []
    for _ in range(iters):
        t = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t) * 1000.0)  # ms
    samples.sort()
    p95 = samples[min(len(samples) - 1, int(0.95 * len(samples)))]
    return statistics.median(samples), p95


def _count(store):
    conn = store._read_conn()
    try:
        return conn.execute("SELECT COUNT(*) FROM flow_events").fetchone()[0]
    finally:
        conn.close()


def churn_mode(args):
    """Simulate steady-state retention: fill a 24h window, then repeatedly add a
    sweep-interval of new events and purge the oldest, reporting the DB file
    size each cycle. The point is to show the file PLATEAUS (freed pages are
    reused) and the batched purge stays cheap - i.e. no periodic full VACUUM is
    needed just to keep size bounded.
    """
    from core.flow_store import SqliteFlowStore

    rate = args.rate or 1000.0
    window_rows = args.rows if args.rows is not None else int(rate * args.hours * 3600)
    sweep_ms = int(args.sweep_min * 60 * 1000)
    sweep_rows = int(rate * args.sweep_min * 60)
    window_ms = int(args.hours * 3600 * 1000)
    dags = [f"dag_{i:03d}" for i in range(args.dags)]

    print("=" * 72)
    print("Flow retention CHURN/plateau benchmark (real SqliteFlowStore)")
    print(f"  rate={rate:g}/s  window={args.hours}h => {window_rows:,} steady-state rows")
    print(f"  sweep={args.sweep_min:g} min => +{sweep_rows:,} insert / -purge per cycle"
          f"  | cycles={args.cycles}")
    print("=" * 72)

    tmp = tempfile.mkdtemp(prefix="flowchurn_")
    path = os.path.join(tmp, "flow_history.db")
    store = SqliteFlowStore(path)
    seq = [0]
    now = [int(time.time() * 1000)]

    def mkrows(n, t_end, t_span):
        out = []
        for i in range(n):
            seq[0] += 1
            out.append({"dag_id": dags[seq[0] % len(dags)], "node_id": "n",
                        "seq": seq[0], "ts_ms": t_end - t_span + int(t_span * i / max(n, 1)),
                        "inputs_json": "{}", "output_json": '{"v":1}',
                        "targets_json": None, "compute_us": 7})
        return out

    def insert(n, t_end, t_span):
        done = 0
        while done < n:
            c = min(50000, n - done)
            store.write_batch(mkrows(c, t_end, t_span))
            done += c

    t = time.perf_counter()
    insert(window_rows, now[0], window_ms)
    print(f"\nFILL: {window_rows:,} rows in {time.perf_counter()-t:.1f}s | "
          f"file {os.path.getsize(path)/1048576:.1f} MB | rows {_count(store):,}")

    print("\ncycle |  rows       | file MB | insert ms | purge ms | purged")
    print("-" * 64)
    for cyc in range(1, args.cycles + 1):
        now[0] += sweep_ms
        ti = time.perf_counter()
        insert(sweep_rows, now[0], sweep_ms)
        ti = (time.perf_counter() - ti) * 1000
        tp = time.perf_counter()
        removed = store.purge_older_than_ms(now[0] - window_ms, batch=5000)
        tp = (time.perf_counter() - tp) * 1000
        print(f" {cyc:4d} | {_count(store):10,} | {os.path.getsize(path)/1048576:7.1f} "
              f"| {ti:8.0f}  | {tp:7.0f}  | {removed:,}")

    print("\nReading: if 'file MB' stops growing after the window fills and stays "
          "flat across cycles, freed pages are being reused -> the single file "
          "plateaus and no periodic VACUUM is needed to bound size. 'purge ms' is "
          "the batched delete cost per sweep (short, interleaves with the writer).")
    with contextlib.suppress(Exception):
        os.remove(path)
        for f in os.listdir(tmp):
            os.remove(os.path.join(tmp, f))
        os.rmdir(tmp)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rows", type=int, default=None,
                    help="total rows (overrides --rate/--hours)")
    ap.add_argument("--rate", type=float, default=None,
                    help="events/sec (with --hours, sets total rows)")
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--dags", type=int, default=20)
    ap.add_argument("--nodes", type=int, default=10)
    ap.add_argument("--iters", type=int, default=30,
                    help="probes per query type (median + p95)")
    ap.add_argument("--cycles", type=int, default=0,
                    help="if >0, run the retention churn/plateau benchmark instead")
    ap.add_argument("--sweep-min", type=float, default=30.0,
                    help="retention sweep interval in minutes (churn mode)")
    args = ap.parse_args()

    if args.cycles > 0:
        churn_mode(args)
        return

    window_ms = int(args.hours * 3600 * 1000)
    if args.rows is not None:
        n_rows = args.rows
    elif args.rate is not None:
        n_rows = int(args.rate * args.hours * 3600)
    else:
        n_rows = 1_000_000

    print("=" * 68)
    print("Flow history query benchmark (SQLite, real FlowDAO)")
    print(f"  rows={n_rows:,}  dags={args.dags}  nodes/dag={args.nodes}  "
          f"window={args.hours}h  probes={args.iters}")
    print("=" * 68)

    tmp = tempfile.mkdtemp(prefix="flowbench_")
    path = os.path.join(tmp, "flow_history.db")
    db = _BenchDB(path, fast_load=True)
    dao = FlowDAO(db=db)

    dags, t0, fill_s = _fill(db, n_rows, args.dags, args.nodes, window_ms)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"\nFILL: {n_rows:,} rows in {fill_s:.1f}s "
          f"({n_rows / fill_s:,.0f} rows/s, fast-load PRAGMAs) | "
          f"db file {size_mb:.1f} MB on disk")

    # Real ingest-path throughput via the production DAO.write_batch.
    sample = [{"dag_id": "dag_000", "node_id": "n", "seq": 10 ** 9 + i,
               "ts_ms": t0 + i, "inputs_json": "{}", "output_json": "{}",
               "targets_json": None, "compute_us": 1} for i in range(20000)]
    w = time.perf_counter()
    dao.write_batch(sample)
    w = time.perf_counter() - w
    print(f"DAO.write_batch: 20,000 rows in {w * 1000:.0f} ms "
          f"({len(sample) / w:,.0f} rows/s, real ORM ingest path)")

    span = window_ms
    one_dag = dags[len(dags) // 2]

    def q_5min():
        s = t0 + random.randint(0, max(1, span - 300_000))
        dao.query(one_dag, t0_ms=s, t1_ms=s + 300_000, limit=5000)

    def q_1h():
        s = t0 + random.randint(0, max(1, span - 3_600_000))
        dao.query(one_dag, t0_ms=s, t1_ms=s + 3_600_000, limit=5000)

    def q_state_at():
        dao.state_at(one_dag, t0 + random.randint(0, span))

    def q_distinct():
        dao.distinct_dags()

    print("\nQUERY latency (median / p95), real indexed DAO calls:")
    for label, fn in (("window 5-min  ", q_5min),
                      ("window 1-hour ", q_1h),
                      ("state_at(ts)  ", q_state_at),
                      ("distinct_dags ", q_distinct)):
        med, p95 = _timeit(fn, args.iters)
        print(f"  {label}: {med:7.2f} ms  /  {p95:7.2f} ms (p95)")

    print("\nReading: window/state_at are the hot paths the flow UI issues.")
    print("If these stay low single-digit ms at your real 24h volume, the")
    print("single SQLite file is not the bottleneck. Re-run with --rate/--hours")
    print("set to YOUR peak to confirm before considering another backend.")

    with contextlib.suppress(Exception):
        os.remove(path)
        os.rmdir(tmp)


if __name__ == "__main__":
    main()
