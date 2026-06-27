"""
Flow Time-Travel Routes (FastAPI)
=================================

The query/visualize/download side of Flow Time-Travel. Read endpoints are
login-protected; the runtime enable/disable switch is admin-only. All handlers
call the guards as their first statement, exactly like the other route classes.

    GET  /flow                          -> the Time Travel UI page
    GET  /api/flow/status               -> recorder stats + enabled flag
    POST /api/flow/enable   (admin)     -> turn capture on at runtime
    POST /api/flow/disable  (admin)     -> turn capture off at runtime
    GET  /api/flow/dags                 -> DAGs with event counts + time bounds
    GET  /api/flow/{dag_id}?from&to&nodes&limit&after  -> events in a window
    GET  /api/flow/{dag_id}/state-at?ts=ms             -> reconstructed state
    GET  /api/flow/{dag_id}/export?from&to&format=jsonl|csv&download=1

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import csv
import io
import json
import logging
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from web.fastapi_compat import AuthGuards, render

logger = logging.getLogger(__name__)

DAY_MS = 24 * 3600 * 1000


class FlowRoutes:
    """Time-travel viewer + query/export API over recorded flow events."""

    def __init__(self, app: FastAPI, guards: AuthGuards, recorder, store,
                 alert_engine=None, max_streams=5, stream_max_seconds=120):
        self.app = app
        self.guards = guards
        self.recorder = recorder
        self.store = store
        self.alert_engine = alert_engine
        # Protect the compute host from UI replay load: cap simultaneous SSE
        # replays and bound how long any one stream may run (also bounds the
        # WAL read-snapshot it holds). These are web-tier guards; the compute
        # path is already isolated (separate WAL flow DB, enqueue-only hot path).
        import threading
        self._stream_sem = threading.BoundedSemaphore(max(1, int(max_streams)))
        self._stream_max_seconds = max(5, int(stream_max_seconds))
        self._register_routes()

    def _register_routes(self):
        add = self.app.add_api_route
        add('/flow', self.page, methods=['GET'], name='flow_time_travel')
        add('/api/flow/status', self.status, methods=['GET'])
        add('/api/flow/enable', self.enable, methods=['POST'])
        add('/api/flow/disable', self.disable, methods=['POST'])
        add('/api/flow/dags', self.dags, methods=['GET'])
        add('/api/alerts', self.alerts, methods=['GET'])
        add('/api/flow/{dag_id}', self.events, methods=['GET'])
        add('/api/flow/{dag_id}/state-at', self.state_at, methods=['GET'])
        add('/api/flow/{dag_id}/histogram', self.histogram, methods=['GET'])
        add('/api/flow/{dag_id}/distribution', self.distribution, methods=['GET'])
        add('/api/flow/{dag_id}/stream', self.stream, methods=['GET'])
        add('/api/flow/{dag_id}/export', self.export, methods=['GET'])

    # ------------------------------------------------------- distribution
    # ------------------------------------------------------- distribution
    def distribution(self, request: Request, dag_id: str):
        """Rolling last-24h compute-cycle/event distribution for a DAG, served
        from the recorder's in-memory roll-up (120 x 12-min buckets, O(120), no
        DB scan). This is what the replay bottom-pane chart loads by default, so
        many concurrent replays cost the database nothing. Use /histogram for an
        exact aggregate over an arbitrary sub-window."""
        self.guards.login_required(request)
        try:
            data = self.recorder.distribution(dag_id)
        except Exception as ex:  # noqa: BLE001
            return JSONResponse({"error": str(ex), "buckets": []}, status_code=200)
        return JSONResponse(data)

    def histogram(self, request: Request, dag_id: str):
        """Per-bucket compute-cycle and event counts across a window, for the
        replay distribution chart. Cheap aggregate (no events sent to the
        browser). Window capped to 24h. Query: from, to (ms), buckets (default
        120)."""
        self.guards.login_required(request)
        qp = request.query_params
        t0, t1 = self._window(qp)
        if t1 - t0 > DAY_MS:
            t0 = t1 - DAY_MS
        try:
            buckets = max(1, min(int(qp.get('buckets', 120)), 1000))
        except (TypeError, ValueError):
            buckets = 120
        inst = qp.get('instance') or None
        try:
            data = self.store.histogram(dag_id, t0, t1, buckets, instance=inst)
        except Exception as ex:  # noqa: BLE001
            return JSONResponse({"error": str(ex), "from": t0, "to": t1,
                                 "buckets": []}, status_code=200)
        return JSONResponse(data)

    # ----------------------------------------------------------- SSE replay
    def stream(self, request: Request, dag_id: str):
        """Stream a time window as Server-Sent Events, grouped by COMPUTE CYCLE.

        Each push carries a configurable number of *complete* compute cycles
        (``cycles``, default 256) - never a partial cycle - so the UI always
        renders whole start-to-finish waves, in order, as play proceeds. Events
        within a cycle are contiguous in seq (one engine sweep), so a cycle is
        complete once a higher cycle_id appears. A hard per-push event cap bounds
        memory for legacy rows that predate cycle_id (cycle_id is null) or a
        pathologically large single cycle. The window is capped to 24h.
        Query: from, to (ms), cycles (complete cycles per push), batch (hard
        event cap per push). Emits `event: batch` {events, count, cycles} then
        `event: done` {total, from, to}; `event: error` on failure.
        """
        self.guards.login_required(request)
        qp = request.query_params
        t0, t1 = self._window(qp)
        if t1 - t0 > DAY_MS:               # hard 24h cap (server-side guard)
            t0 = t1 - DAY_MS
        nodes = [n for n in (qp.get('nodes') or '').split(',') if n] or None
        try:
            cycles_per_push = max(1, min(int(qp.get('cycles', 256)), 5000))
        except (TypeError, ValueError):
            cycles_per_push = 256
        try:
            hard_cap = max(500, min(int(qp.get('batch', 20000)), 50000))
        except (TypeError, ValueError):
            hard_cap = 20000

        def gen():
            # Concurrency cap: don't let UI replays pile up on the web tier.
            if not self._stream_sem.acquire(blocking=False):
                yield ("event: error\ndata: " + json.dumps(
                    {"error": "Server is busy (maximum concurrent replays "
                              "reached). Try again shortly."}) + "\n\n")
                return
            start = time.time()
            buf, total, complete = [], 0, 0
            cur_cycle = _UNSET = object()

            def _flush():
                nonlocal buf, complete
                n = len(buf)
                payload = json.dumps({"events": buf, "count": total,
                                      "cycles": complete}, default=str)
                buf, complete = [], 0
                return ("event: batch\ndata: " + payload + "\n\n"), n

            try:
                for e in self.store.iter_export(dag_id, t0, t1, nodes):
                    cid = e.get("cycle_id")
                    # A cycle boundary: the previous cycle just completed.
                    if buf and cid != cur_cycle:
                        complete += 1
                        if complete >= cycles_per_push:
                            msg, n = _flush(); total += n; yield msg
                            if time.time() - start > self._stream_max_seconds:
                                yield ("event: done\ndata: " + json.dumps(
                                    {"total": total, "from": t0, "to": t1,
                                     "truncated": True,
                                     "reason": "stream time limit reached"})
                                    + "\n\n")
                                return
                    cur_cycle = cid
                    buf.append(e)
                    if len(buf) >= hard_cap:       # safety bound per push
                        msg, n = _flush(); total += n; yield msg
                        cur_cycle = _UNSET
                if buf:
                    complete += 1
                    msg, n = _flush(); total += n; yield msg
                yield ("event: done\ndata: " +
                       json.dumps({"total": total, "from": t0, "to": t1}) + "\n\n")
            except GeneratorExit:          # client disconnected mid-stream
                return
            except Exception as ex:        # noqa: BLE001
                yield "event: error\ndata: " + json.dumps({"error": str(ex)}) + "\n\n"
            finally:
                self._stream_sem.release()

        return StreamingResponse(
            gen(), media_type='text/event-stream',
            headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive',
                     'X-Accel-Buffering': 'no'})

    # ------------------------------------------------------------- SLO alerts
    def alerts(self, request: Request):
        """SLO / staleness alert status (output-change staleness, v1)."""
        self.guards.login_required(request)
        if self.alert_engine is None:
            return JSONResponse({"enabled": False, "alerts": [],
                                 "summary": {"total": 0, "ok": 0, "breach": 0}})
        return JSONResponse(self.alert_engine.evaluate())

    # ------------------------------------------------------------------ page
    def page(self, request: Request):
        self.guards.login_required(request)
        return render(request, 'dag/flow_time_travel.html')

    # --------------------------------------------------------------- control
    def status(self, request: Request):
        self.guards.login_required(request)
        sem = self._stream_sem
        active = max(0, sem._initial_value - sem._value)   # current live replays
        return JSONResponse({**self.recorder.stats(),
                             "active_streams": active,
                             "max_streams": sem._initial_value})

    def enable(self, request: Request):
        self.guards.admin_required(request)
        dag = request.query_params.get('dag') or None
        self.recorder.enable(dag)
        return JSONResponse({"ok": True, "scope": dag or "all",
                             **self.recorder.stats()})

    def disable(self, request: Request):
        self.guards.admin_required(request)
        dag = request.query_params.get('dag') or None
        self.recorder.disable(dag)
        return JSONResponse({"ok": True, "scope": dag or "all",
                             **self.recorder.stats()})

    # ----------------------------------------------------------------- query
    def dags(self, request: Request):
        self.guards.login_required(request)
        return JSONResponse({"dags": self.store.distinct_dags()})

    def _window(self, qp):
        now = int(time.time() * 1000)
        try:
            t1 = int(qp.get('to')) if qp.get('to') else now
        except (TypeError, ValueError):
            t1 = now
        try:
            t0 = int(qp.get('from')) if qp.get('from') else t1 - DAY_MS
        except (TypeError, ValueError):
            t0 = t1 - DAY_MS
        return t0, t1

    def events(self, request: Request, dag_id: str):
        self.guards.login_required(request)
        qp = request.query_params
        t0, t1 = self._window(qp)
        nodes = [n for n in (qp.get('nodes') or '').split(',') if n] or None
        try:
            limit = min(max(int(qp.get('limit', 5000)), 1), 100000)
        except (TypeError, ValueError):
            limit = 5000
        after = qp.get('after')
        after = int(after) if (after or '').isdigit() else None
        rows = self.store.query(dag_id, t0, t1, nodes, limit=limit,
                                after_seq=after)
        return JSONResponse({"dag_id": dag_id, "from": t0, "to": t1,
                             "count": len(rows), "events": rows,
                             "next": rows[-1]["seq"] if rows else None})

    def state_at(self, request: Request, dag_id: str):
        self.guards.login_required(request)
        ts = request.query_params.get('ts')
        ts = int(ts) if (ts or '').isdigit() else int(time.time() * 1000)
        return JSONResponse(self.store.state_at(dag_id, ts))

    # ---------------------------------------------------------------- export
    def export(self, request: Request, dag_id: str):
        self.guards.login_required(request)
        qp = request.query_params
        t0, t1 = self._window(qp)
        nodes = [n for n in (qp.get('nodes') or '').split(',') if n] or None
        fmt = (qp.get('format') or 'jsonl').lower()
        gen = self.store.iter_export(dag_id, t0, t1, nodes)
        fname = f"flow_{dag_id}_{t0}-{t1}.{ 'csv' if fmt=='csv' else 'jsonl' }"
        if fmt == 'csv':
            stream, media = self._csv_stream(gen), 'text/csv'
        else:
            stream, media = self._jsonl_stream(gen), 'application/x-ndjson'
        headers = {}
        if qp.get('download'):
            headers['Content-Disposition'] = f'attachment; filename="{fname}"'
        return StreamingResponse(stream, media_type=media, headers=headers)

    @staticmethod
    def _jsonl_stream(gen):
        for e in gen:
            yield json.dumps(e, default=str) + "\n"

    @staticmethod
    def _csv_stream(gen):
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["seq", "ts_ms", "dag_id", "node_id", "targets",
                    "inputs", "output", "compute_us"])
        yield buf.getvalue(); buf.seek(0); buf.truncate(0)
        for e in gen:
            w.writerow([e.get("seq"), e.get("ts_ms"), e.get("dag_id"),
                        e.get("node_id"), json.dumps(e.get("targets")),
                        json.dumps(e.get("inputs"), default=str),
                        json.dumps(e.get("output"), default=str),
                        e.get("compute_us")])
            yield buf.getvalue(); buf.seek(0); buf.truncate(0)
