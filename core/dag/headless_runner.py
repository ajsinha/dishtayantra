#!/usr/bin/env python3
"""Headless run-once runner for DishtaYantra.

Starts a single DAG via the compute engine WITHOUT the web UI / FastAPI app,
optionally replays a bounded input feed, waits until processing is complete
(count-based and/or quiescence), drains, stops, writes a summary JSON, and exits
with a status code (0 = success, 1 = incomplete/error). This is the "headless
worker" used by the orchestration pattern (a long-running control-plane instance
dispatches these to do heavy, run-to-completion ETL).

The engine has always been usable without the webapp (the perftest/EOD runners
start a ComputeGraph directly); this packages that into a first-class CLI with
completion detection and clean, zero-loss shutdown.

Usage:
    python -m core.dag.headless_runner --config job.json \
        [--replay feed.jsonl] [--expect N] [--summary out.json] [--job-id ID]

Notes:
  * --replay publishes each line of the file to the DAG's first in-memory
    subscriber queue (mem://queue/NAME). With a real broker the feed arrives via
    the broker and --replay is unnecessary.
  * Completion = sink published >= expected (when known), else quiescence
    (no dirty nodes and empty input queues for several consecutive polls).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

logger = logging.getLogger(__name__)


def _load_config(path: str) -> Dict[str, Any]:
    with open(path) as fh:
        text = fh.read()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        import yaml  # optional
        return yaml.safe_load(text)


def _queue_name(source: str) -> Optional[str]:
    # mem://queue/NAME -> NAME
    if source and "://" in source:
        return source.rsplit("/", 1)[-1]
    return None


def _published_count(graph) -> int:
    total = 0
    for node in getattr(graph, "nodes", {}).values():
        c = getattr(node, "_published_count", None)
        if isinstance(c, int):
            total += c
    return total


def _any_dirty(graph) -> bool:
    for node in getattr(graph, "nodes", {}).values():
        try:
            if node.isdirty():
                return True
        except Exception:
            pass
    return False


def _input_queue_empty(graph, in_queue: Optional[str]) -> bool:
    if not in_queue:
        return True
    try:
        from core.pubsub.inmemorypubsub import InMemoryPubSub
        return InMemoryPubSub().get_queue_size(in_queue) == 0
    except Exception:
        return True


class HeadlessRunner:
    """Runs one DAG to completion, headless, then stops."""

    def __init__(self, config_path: str, replay: Optional[str] = None,
                 expect: Optional[int] = None, summary_path: Optional[str] = None,
                 job_id: Optional[str] = None, timeout: float = 300.0,
                 quiescence_polls: int = 6, poll_interval: float = 0.05):
        self.config_path = config_path
        self.replay = replay
        self.expect = expect
        self.summary_path = summary_path
        self.job_id = job_id
        self.timeout = timeout
        self.quiescence_polls = quiescence_polls
        self.poll_interval = poll_interval

    def run(self) -> Dict[str, Any]:
        from core.dag.compute_graph import ComputeGraph
        from core.pubsub.inmemorypubsub import InMemoryPubSub

        cfg = _load_config(self.config_path)
        in_queue = None
        subs = cfg.get("subscribers") or []
        if subs:
            in_queue = _queue_name((subs[0].get("config") or {}).get("source", ""))

        t0 = time.perf_counter()
        graph = ComputeGraph(cfg)
        graph.start()
        time.sleep(0.3)

        fed = 0
        if self.replay:
            ps = InMemoryPubSub()
            with open(self.replay) as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    ps.publish_to_queue(in_queue, line)
                    fed += 1
        expected = self.expect if self.expect is not None else (fed if self.replay else None)

        completed_by = None
        stable = 0
        deadline = time.time() + self.timeout
        while time.time() < deadline:
            published = _published_count(graph)
            if expected is not None and published >= expected:
                completed_by = "count"
                break
            quiescent = (not _any_dirty(graph)) and _input_queue_empty(graph, in_queue)
            # only trust quiescence once we've fed everything (or there is no replay)
            if quiescent and (not self.replay or fed > 0):
                stable += 1
                if stable >= self.quiescence_polls:
                    completed_by = "quiescence"
                    break
            else:
                stable = 0
            time.sleep(self.poll_interval)

        # drain guard: give any in-flight work a moment, honour zero-loss
        for _ in range(self.quiescence_polls):
            if _input_queue_empty(graph, in_queue) and not _any_dirty(graph):
                break
            time.sleep(self.poll_interval)

        published = _published_count(graph)
        try:
            graph.stop()
        except Exception as e:
            logger.error("error stopping graph: %s", e)

        elapsed = round(time.perf_counter() - t0, 3)
        status = "ok" if (completed_by is not None and
                          (expected is None or published >= expected)) else "incomplete"
        summary = {
            "job_id": self.job_id,
            "dag": cfg.get("name"),
            "fed": fed,
            "published": published,
            "expected": expected,
            "completed_by": completed_by,
            "elapsed_s": elapsed,
            "status": status,
        }
        if self.summary_path:
            os.makedirs(os.path.dirname(os.path.abspath(self.summary_path)), exist_ok=True)
            with open(self.summary_path, "w") as fh:
                json.dump(summary, fh, indent=2)
        return summary


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="DishtaYantra headless run-once runner")
    p.add_argument("--config", required=True, help="path to a DAG config (json/yaml)")
    p.add_argument("--replay", help="optional feed file replayed onto the input queue")
    p.add_argument("--expect", type=int, help="expected output count (count-based completion)")
    p.add_argument("--summary", help="path to write the job summary JSON")
    p.add_argument("--job-id", help="dedup/correlation id for this job")
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args(argv)
    if args.quiet:
        logging.disable(logging.INFO)

    runner = HeadlessRunner(args.config, replay=args.replay, expect=args.expect,
                            summary_path=args.summary, job_id=args.job_id,
                            timeout=args.timeout)
    summary = runner.run()
    if not args.quiet:
        print(json.dumps(summary, indent=2))
    return 0 if summary["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
