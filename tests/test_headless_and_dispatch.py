"""Tests for the headless run-once runner and the job dispatcher.

These are deterministic and fast: the headless runner is exercised in-process on a
tiny DAG (no subprocess), and the dispatcher is exercised against a trivial fake
worker (perftest/_fake_worker.py) so we test launch/idempotency/concurrency/
completion-recording without spawning full engine children.
"""

import json
import os
import sys
import tempfile
import time

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_TINY_DAG = {
    "name": "tiny_headless", "start_time": None, "end_time": None,
    "subscribers": [{"name": "in", "config": {"source": "mem://queue/tiny_in", "max_depth": 100000}}],
    "publishers": [{"name": "out", "config": {"destination": "mem://queue/tiny_out"}}],
    "calculators": [{"name": "p", "type": "PassthruCalculator", "config": {}}],
    "transformers": [],
    "nodes": [
        {"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "in"},
        {"name": "calc", "type": "CalculationNode", "config": {}, "calculator": "p"},
        {"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["out"]},
    ],
    "edges": [{"from_node": "ingest", "to_node": "calc"},
              {"from_node": "calc", "to_node": "sink"}],
}


def test_headless_runner_completes_bounded_feed_in_process():
    from core.dag.headless_runner import HeadlessRunner
    wd = tempfile.mkdtemp(prefix="dy_hr_")
    cfg = os.path.join(wd, "dag.json")
    with open(cfg, "w") as fh:
        json.dump(_TINY_DAG, fh)
    feed = os.path.join(wd, "feed.jsonl")
    n = 500
    with open(feed, "w") as fh:
        for i in range(n):
            fh.write(json.dumps({"id": i, "v": i * 2}) + "\n")
    summary_path = os.path.join(wd, "summary.json")

    r = HeadlessRunner(cfg, replay=feed, expect=n, summary_path=summary_path,
                       job_id="t1", timeout=60).run()

    assert r["status"] == "ok"
    assert r["fed"] == n
    assert r["published"] >= n           # >= because the engine emits one startup msg
    assert r["completed_by"] == "count"
    assert os.path.exists(summary_path)
    assert json.load(open(summary_path))["status"] == "ok"


def _dispatcher(**overrides):
    from core.dag.job_dispatch import JobDispatchCalculator
    sumdir = tempfile.mkdtemp(prefix="dy_disp_")
    cfg = {"command": [sys.executable, os.path.join(REPO, "perftest", "_fake_worker.py")],
           "summary_dir": sumdir, "max_concurrent": 2}
    cfg.update(overrides)
    return JobDispatchCalculator("disp", cfg), sumdir


def _await_results(calc, n, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if len(calc.job_results()) >= n:
            return True
        time.sleep(0.05)
    return False


def test_dispatch_is_idempotent_per_key():
    calc, _ = _dispatcher()
    base = {"config": "/tmp/none.json", "expect": 0}
    first = calc.calculate({**base, "job_id": "A"})
    dup = calc.calculate({**base, "job_id": "A"})
    assert first["dispatch"] == "launched"
    assert dup["dispatch"] == "duplicate-skipped"
    assert _await_results(calc, 1)
    assert set(calc.job_results().keys()) == {"A"}
    calc.shutdown()


def test_dispatch_records_completion_for_each_unique_job():
    calc, _ = _dispatcher(max_concurrent=4)
    for k in ("A", "B", "C"):
        calc.calculate({"job_id": k, "config": "/tmp/none.json", "expect": 7})
    assert _await_results(calc, 3)
    res = calc.job_results()
    assert set(res) == {"A", "B", "C"}
    assert all(v["status"] == "ok" for v in res.values())
    assert all(v["published"] == 7 for v in res.values())
    calc.shutdown()


def test_dispatch_respects_concurrency_cap():
    # max_concurrent=1: the 2nd/3rd should queue, not launch immediately
    calc, _ = _dispatcher(max_concurrent=1)
    d1 = calc.calculate({"job_id": "A", "config": "/tmp/none.json"})
    d2 = calc.calculate({"job_id": "B", "config": "/tmp/none.json"})
    assert d1["dispatch"] == "launched"
    assert d2["dispatch"] == "queued"
    # everything still completes as slots free up
    assert _await_results(calc, 2)
    calc.shutdown()


def test_dispatch_missing_fields_error():
    calc, _ = _dispatcher()
    assert calc.calculate({"config": "/tmp/none.json"})["dispatch"] == "error"   # no job_id
    assert calc.calculate({"job_id": "X"})["dispatch"] == "error"                # no config
    calc.shutdown()
