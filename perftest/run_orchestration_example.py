#!/usr/bin/env python3
"""End-to-end demo of the control-plane / headless-worker orchestration pattern.

A long-running parent DAG:
  job_requests -> [dispatch] -> sink         (launches headless workers)
  job_done     -> [react]    -> job_done_out (reacts to completions)

We publish several job requests (plus a duplicate, to prove idempotency). The
dispatch node launches a *separate headless DishtaYantra process* per unique job
(bounded by max_concurrent), each runs an EOD enrichment DAG to completion and
exits, and on exit the parent receives a job-done event and reacts to it.

Usage:
    python -m perftest.run_orchestration_example --jobs 3
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import tempfile
import time
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

_JOB_DAG = {
    "name": "headless_eod_job", "start_time": None, "end_time": None,
    "subscribers": [{"name": "in", "config": {"source": "mem://queue/job_in", "max_depth": 2000000}}],
    "publishers": [{"name": "out", "config": {"destination": "mem://queue/job_out"}}],
    "calculators": [{"name": "enrich",
                     "type": "perftest.eod_enrichment_calculators.ReferenceEnrichCalculator",
                     "config": {"fx_rates": {"USD": 1.0, "EUR": 1.08, "GBP": 1.27},
                                "client_limits": {"C1": 1000000, "C2": 50000}}}],
    "transformers": [],
    "nodes": [
        {"name": "ingest", "type": "SubscriptionNode", "config": {}, "subscriber": "in"},
        {"name": "enrich_node", "type": "CalculationNode", "config": {}, "calculator": "enrich"},
        {"name": "sink", "type": "PublicationNode", "config": {}, "publishers": ["out"]},
    ],
    "edges": [{"from_node": "ingest", "to_node": "enrich_node"},
              {"from_node": "enrich_node", "to_node": "sink"}],
}


def _write_job_assets(workdir: str, rows: int):
    cfg_path = os.path.join(workdir, "job_dag.json")
    with open(cfg_path, "w") as fh:
        json.dump(_JOB_DAG, fh)
    feed_path = os.path.join(workdir, "feed.jsonl")
    rng = random.Random(7)
    with open(feed_path, "w") as fh:
        for i in range(rows):
            fh.write(json.dumps({"trade_id": i, "client_id": rng.choice(["C1", "C2"]),
                                 "currency": rng.choice(["USD", "EUR", "GBP"]),
                                 "quantity": rng.randint(1, 5000),
                                 "price": round(rng.uniform(1, 900), 2)}) + "\n")
    return cfg_path, feed_path


def _parent_config(summary_dir: str, max_concurrent: int):
    return {
        "name": "control_plane", "start_time": None, "end_time": None,
        "subscribers": [
            {"name": "req", "config": {"source": "mem://queue/job_requests", "max_depth": 100000}},
            {"name": "done", "config": {"source": "mem://queue/job_done", "max_depth": 100000}},
        ],
        "publishers": [
            {"name": "disp_log", "config": {"destination": "mem://queue/dispatch_log"}},
            {"name": "done_out", "config": {"destination": "mem://queue/job_done_out"}},
        ],
        "calculators": [
            {"name": "dispatch", "type": "core.dag.job_dispatch.JobDispatchCalculator",
             "config": {"max_concurrent": max_concurrent, "summary_dir": summary_dir,
                        "done_queue": "job_done"}},
            {"name": "react", "type": "PassthruCalculator", "config": {}},
        ],
        "transformers": [],
        "nodes": [
            {"name": "req_in", "type": "SubscriptionNode", "config": {}, "subscriber": "req"},
            {"name": "dispatch_node", "type": "CalculationNode", "config": {}, "calculator": "dispatch"},
            {"name": "disp_sink", "type": "PublicationNode", "config": {}, "publishers": ["disp_log"]},
            {"name": "done_in", "type": "SubscriptionNode", "config": {}, "subscriber": "done"},
            {"name": "react_node", "type": "CalculationNode", "config": {}, "calculator": "react"},
            {"name": "done_sink", "type": "PublicationNode", "config": {}, "publishers": ["done_out"]},
        ],
        "edges": [
            {"from_node": "req_in", "to_node": "dispatch_node"},
            {"from_node": "dispatch_node", "to_node": "disp_sink"},
            {"from_node": "done_in", "to_node": "react_node"},
            {"from_node": "react_node", "to_node": "done_sink"},
        ],
    }


def run_example(jobs: int = 3, rows: int = 800, max_concurrent: int = 2,
                quiet: bool = True) -> Dict[str, Any]:
    logging.disable(logging.INFO)
    from core.dag.compute_graph import ComputeGraph
    from core.pubsub.inmemorypubsub import InMemoryPubSub

    workdir = tempfile.mkdtemp(prefix="dy_orch_")
    summary_dir = os.path.join(workdir, "summaries")
    cfg_path, feed_path = _write_job_assets(workdir, rows)

    parent = ComputeGraph(_parent_config(summary_dir, max_concurrent))
    parent.start()
    time.sleep(0.4)

    ps = InMemoryPubSub()
    # publish unique job requests + one duplicate (same job_id as job-0)
    requests = []
    for j in range(jobs):
        requests.append({"job_id": f"job-{j}", "config": cfg_path, "replay": feed_path, "expect": rows})
    requests.append({"job_id": "job-0", "config": cfg_path, "replay": feed_path, "expect": rows})  # dup
    for r in requests:
        ps.publish_to_queue("job_requests", json.dumps(r))

    dispatch_calc = parent.get_node("dispatch_node")._calculator

    # wait for all unique jobs to complete (children exit + summaries read)
    deadline = time.time() + 120
    while time.time() < deadline:
        if len(dispatch_calc.job_results()) >= jobs:
            break
        time.sleep(0.1)
    time.sleep(0.5)  # let final job-done events propagate through the react branch

    results = dispatch_calc.job_results()
    # drain the reactive completion branch output (non-blocking: block=False
    # returns None on an empty queue instead of waiting forever)
    done_events = []
    while True:
        m = ps.consume_from_queue("job_done_out", block=False)
        if m is None:
            break
        done_events.append(json.loads(m) if isinstance(m, str) else m)
    done_events = [e for e in done_events if isinstance(e, dict) and e.get("job_id")]

    try:
        dispatch_calc.shutdown()
    except Exception:
        pass
    try:
        parent.stop()
    except Exception:
        pass

    all_ok = all(r.get("status") == "ok" for r in results.values())
    report = {
        "jobs_requested_unique": jobs,
        "duplicates_sent": 1,
        "jobs_completed": len(results),
        "all_status_ok": all_ok and len(results) == jobs,
        "dispatched_unique": dispatch_calc._calculation_count,  # calculate() calls = requests seen
        "reactive_done_events": len(done_events),
        "per_job": {k: {"status": v.get("status"), "published": v.get("published"),
                        "completed_by": v.get("completed_by")} for k, v in sorted(results.items())},
        "workdir": workdir,
    }
    if not quiet:
        _print(report)
    return report


def _print(r):
    print("=" * 72)
    print("Orchestration — control plane dispatches headless workers, reacts to done")
    print("=" * 72)
    print(f"unique jobs requested: {r['jobs_requested_unique']} (+{r['duplicates_sent']} duplicate)")
    print(f"  jobs completed: {r['jobs_completed']} | all ok: {r['all_status_ok']}")
    print(f"  reactive job-done events received by parent: {r['reactive_done_events']}")
    for k, v in r["per_job"].items():
        print(f"    {k}: status={v['status']} published={v['published']} by={v['completed_by']}")
    print("  NOTE: each unique job ran as a SEPARATE headless process and exited;")
    print("  the duplicate request was skipped (idempotent dispatch).")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Orchestration demo")
    p.add_argument("--jobs", type=int, default=3)
    p.add_argument("--rows", type=int, default=800)
    p.add_argument("--max-concurrent", type=int, default=2)
    args = p.parse_args(argv)
    r = run_example(jobs=args.jobs, rows=args.rows, max_concurrent=args.max_concurrent, quiet=False)
    return 0 if r["all_status_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
