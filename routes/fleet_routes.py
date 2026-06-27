"""Fleet view (Phase 4) - the read-only single-pane-of-glass.

Aggregates the local plane and every trusted server into one overview: per-plane
reachability, version, and DAG counts. Fan-out is per-plane isolated - each
plane reports its own ``ok``/``error`` so one unreachable or slow plane never
breaks the page and there are no partial-success lies. Read-only: no mutations
here (use the switcher to act on a specific plane).

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import asyncio
import json
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from core.service.client import active_target
from web.fastapi_compat import AuthGuards, render

logger = logging.getLogger(__name__)


class FleetRoutes:
    def __init__(self, app: FastAPI, guards: AuthGuards, registry):
        self.app = app
        self.guards = guards
        self.registry = registry
        app.add_api_route('/fleet', self.page, methods=['GET'],
                          name='fleet_page', include_in_schema=False)
        app.add_api_route('/api/fleet/overview', self.overview, methods=['GET'],
                          name='fleet_overview')
        # Phase 5: live push transport. Replaces manual refresh on /fleet with a
        # server-sent-events stream so up/down transitions appear without a
        # reload. The stream talks only to the local plane (it fans out
        # server-side), so it never interacts with the gateway proxy.
        app.add_api_route('/api/fleet/stream', self.stream, methods=['GET'],
                          name='fleet_stream', include_in_schema=False)

    def page(self, request: Request):
        self.guards.login_required(request)
        return render(request, 'fleet/fleet.html')

    async def stream(self, request: Request):
        """SSE stream of fleet overviews.

        Emits one overview immediately, then every ~5s, until the client
        disconnects. ``?once=1`` emits a single event and ends (handy for curl
        and tests). Per-plane probes run in a threadpool so the event loop is
        never blocked, and a failed probe is reported as an SSE ``error`` event
        rather than killing the stream.
        """
        self.guards.login_required(request)
        once = request.query_params.get("once") in ("1", "true", "yes")
        interval = 5.0

        async def gen():
            yield ":ok\n\n"  # establish the stream (comment line)
            try:
                while True:
                    try:
                        planes = await run_in_threadpool(self._collect, request)
                        payload = json.dumps({"planes": planes,
                                              "active_target":
                                                  active_target(request)})
                        yield f"data: {payload}\n\n"
                    except Exception as e:  # noqa: BLE001 - never kill the stream
                        yield ("event: error\ndata: "
                               + json.dumps({"error": str(e)}) + "\n\n")
                    if once:
                        return
                    # sleep in small steps so a disconnect is noticed promptly
                    for _ in range(int(interval * 2)):
                        if await request.is_disconnected():
                            return
                        await asyncio.sleep(0.5)
            except asyncio.CancelledError:  # client went away
                return

        headers = {"Cache-Control": "no-cache", "Connection": "keep-alive",
                   "X-Accel-Buffering": "no"}
        return StreamingResponse(gen(), media_type="text/event-stream",
                                 headers=headers)

    async def overview(self, request: Request):
        self.guards.login_required(request)
        planes = await run_in_threadpool(self._collect, request)
        return JSONResponse({"planes": planes,
                             "active_target": active_target(request)})

    # ------------------------------------------------------------------ #
    def _collect(self, request):
        planes = [self._probe_local(request)]
        if self.registry is not None:
            try:
                servers = self.registry.list()
            except Exception as e:  # noqa: BLE001
                logger.warning("fleet: cannot list trusted servers: %s", e)
                servers = []
            for ts in servers:
                planes.append(self._probe_remote(ts))
        return planes

    def _probe_local(self, request):
        from core.service.client import _local_client
        # The local plane is in-process: always reachable. Manageability can
        # still fail if the local server is somehow unhealthy.
        tile = {"id": "local", "name": "Local", "url": "(in-process)",
                "reachable": True, "manageable": False, "version": None,
                "error": None, "dag_count": 0, "running": 0}
        try:
            tile.update(self._summarise(_local_client(request)))
            tile["manageable"] = True
        except Exception as e:  # noqa: BLE001
            tile["error"] = str(e)
        return tile

    def _probe_remote(self, ts):
        tile = {"id": ts["server_id"], "name": ts.get("name") or ts["server_id"],
                "url": ts.get("url"), "reachable": False, "manageable": False,
                "version": None, "error": None, "dag_count": 0, "running": 0,
                "role": ts.get("role")}
        try:
            client = self.registry.build_client(ts["server_id"])
            client.timeout = 5.0
        except Exception as e:  # noqa: BLE001
            tile["error"] = str(e)
            return tile
        # 1) reachability: cheap, unauthenticated liveness probe.
        try:
            client.live()
            tile["reachable"] = True
        except Exception as e:  # noqa: BLE001 - transport failure => down
            tile["error"] = str(e)
            return tile
        # 2) manageability: our key authenticates and the contract answers.
        try:
            tile.update(self._summarise(client))
            tile["manageable"] = True
        except Exception as e:  # noqa: BLE001 - up, but not manageable by us
            tile["error"] = f"reachable, not manageable: {e}"
        return tile

    @staticmethod
    def _summarise(client):
        info = client.info()
        status = client.server_status()
        dags = status.get("dags", []) or []
        running = sum(1 for d in dags
                      if str(d.get("state", d.get("status", ""))).lower()
                      in ("running", "started", "active"))
        return {"version": info.version,
                "dag_count": status.get("dag_count", len(dags)),
                "running": running}
