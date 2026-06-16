#!/usr/bin/env python3
"""Job dispatch for the control-plane / headless-worker orchestration pattern.

A long-running (UI / control-plane) DishtaYantra instance reacts to events; a
``JobDispatchCalculator`` node turns selected events into headless worker
processes that run a DAG to completion and exit. The dispatcher is the deliberate
*side-effecting* node in an otherwise functional graph, so it is built to be:

  * idempotent  -- exactly one launch per unique job key, even on recompute or a
                   replayed event (dedup set);
  * asynchronous-- launches and returns immediately; never blocks the compute
                   loop (a daemon thread waits on each child);
  * bounded     -- a max-concurrency cap with a pending queue (a real worker
                   pool), so a burst of events cannot fork-bomb the host;
  * observable  -- on child exit it records the child's summary and (optionally)
                   publishes a completion event to a queue the parent DAG can
                   subscribe to, closing the loop reactively.

In production the completion event is published to a shared broker
(Kafka/MQ/Redis) so the parent reacts to ``job-done``/``job-failed`` like any
other event; the in-memory ``done_queue`` here demonstrates the same contract
in-process.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
from typing import Any, Dict, List, Optional

from core.calculator.core_calculator import DataCalculator

logger = logging.getLogger(__name__)

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class JobDispatchCalculator(DataCalculator):
    """Launch a headless worker per qualifying event; exactly-once, async, bounded.

    Config (all optional except behaviour is keyed off the event):
        command:       base argv (default: [python, -m, core.dag.headless_runner])
        key_field:     event field holding the dedup/correlation key (default "job_id")
        config_field:  event field holding the job DAG config path (default "config")
        replay_field:  event field holding a feed file to replay (default "replay")
        expect_field:  event field holding expected output count (default "expect")
        max_concurrent:int worker cap (default 4)
        summary_dir:   where child summaries are written (default /tmp/dy_jobs)
        done_queue:    in-memory queue to publish each completion to (optional)
        cwd:           working dir for children (default repo root)
        extra_args:    list[str] appended to every command
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        self.command = list(cfg.get("command") or [sys.executable, "-m", "core.dag.headless_runner"])
        self.key_field = cfg.get("key_field", "job_id")
        self.config_field = cfg.get("config_field", "config")
        self.replay_field = cfg.get("replay_field", "replay")
        self.expect_field = cfg.get("expect_field", "expect")
        self.max_concurrent = int(cfg.get("max_concurrent", 4))
        self.summary_dir = cfg.get("summary_dir", "/tmp/dy_jobs")
        self.done_queue = cfg.get("done_queue")
        self.cwd = cfg.get("cwd", _REPO_ROOT)
        self.extra_args = list(cfg.get("extra_args") or [])

        self._seen: set = set()
        self._results: Dict[str, Dict[str, Any]] = {}
        self._procs: Dict[str, "subprocess.Popen"] = {}
        self._active = 0
        self._pending: List[tuple] = []
        self._lock = threading.Lock()
        os.makedirs(self.summary_dir, exist_ok=True)

    # ---- public accessors (for the parent / tests) -------------------------
    def job_results(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return dict(self._results)

    def active_count(self) -> int:
        with self._lock:
            return self._active

    # ---- calculator contract -----------------------------------------------
    def calculate(self, data):
        self._calculation_count += 1
        event = dict(data)
        key = event.get(self.key_field)
        if key is None:
            return {**event, "dispatch": "error", "reason": f"missing '{self.key_field}'"}
        key = str(key)
        config_path = event.get(self.config_field)
        if not config_path:
            return {**event, "dispatch": "error", "reason": f"missing '{self.config_field}'"}

        with self._lock:
            if key in self._seen:
                return {**event, "dispatch": "duplicate-skipped", "job_id": key}
            self._seen.add(key)
            cmd = self._build_cmd(key, event)
            if self._active < self.max_concurrent:
                self._launch(key, cmd)
                disp = "launched"
            else:
                self._pending.append((key, cmd))
                disp = "queued"
        return {**event, "dispatch": disp, "job_id": key}

    # ---- internals ----------------------------------------------------------
    def _summary_path(self, key: str) -> str:
        return os.path.join(self.summary_dir, f"{key}.json")

    def _build_cmd(self, key: str, event: Dict[str, Any]) -> List[str]:
        cmd = list(self.command) + [
            "--config", str(event[self.config_field]),
            "--job-id", key,
            "--summary", self._summary_path(key),
            "--quiet",
        ]
        if event.get(self.replay_field):
            cmd += ["--replay", str(event[self.replay_field])]
        if event.get(self.expect_field) is not None:
            cmd += ["--expect", str(event[self.expect_field])]
        cmd += self.extra_args
        return cmd

    def _launch(self, key: str, cmd: List[str]) -> None:
        # caller holds self._lock
        try:
            proc = subprocess.Popen(cmd, cwd=self.cwd)
        except Exception as e:
            logger.error("dispatch %s failed to launch: %s", key, e)
            self._results[key] = {"job_id": key, "status": "error", "reason": str(e)}
            return
        self._active += 1
        self._procs[key] = proc
        logger.info("dispatched job %s (pid %s): %s", key, proc.pid, " ".join(cmd))
        threading.Thread(target=self._wait, args=(key, proc), daemon=True).start()

    def shutdown(self, terminate_running: bool = True, timeout: float = 5.0) -> None:
        """Stop accepting work and (optionally) terminate still-running children so
        the control plane never orphans worker processes on shutdown."""
        with self._lock:
            self._pending.clear()
            procs = list(self._procs.items())
        if not terminate_running:
            return
        for key, proc in procs:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass
        for key, proc in procs:
            try:
                proc.wait(timeout=timeout)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def _wait(self, key: str, proc: "subprocess.Popen") -> None:
        rc = proc.wait()
        summary = {"job_id": key, "status": "error", "returncode": rc}
        path = self._summary_path(key)
        if os.path.exists(path):
            try:
                with open(path) as fh:
                    summary = json.load(fh)
            except Exception as e:
                summary = {"job_id": key, "status": "error", "reason": f"bad summary: {e}"}
        summary.setdefault("returncode", rc)

        with self._lock:
            self._results[key] = summary
            self._active -= 1
            if self.done_queue:
                try:
                    from core.pubsub.inmemorypubsub import InMemoryPubSub
                    InMemoryPubSub().publish_to_queue(self.done_queue, json.dumps(summary))
                except Exception as e:
                    logger.error("could not publish completion for %s: %s", key, e)
            if self._pending and self._active < self.max_concurrent:
                nk, nc = self._pending.pop(0)
                self._launch(nk, nc)

    def details(self):
        d = super().details()
        with self._lock:
            d.update({"dispatched": len(self._seen), "active": self._active,
                      "pending": len(self._pending), "completed": len(self._results)})
        return d
