"""
Egress Monitoring Routes (read-only)
====================================

Surfaces the async egress subsystem's live stats - the egress worker-thread count
and per-destination WAL/delivery counters that ``core.egress`` already computes -
over HTTP, plus a dashboard page that polls them.

IMPORTANT scope note: ``get_manager()`` is a per-process singleton, and the egress
worker threads live in whatever process runs the DAGs. So in single-process mode this
view is complete, but in multiprocess worker mode it reflects egress in the CURRENT
(web) process only - DAGs pinned to worker processes maintain their own egress pools
that this endpoint cannot see. This is strictly read-only; no management actions
(pause/flush/reset) are exposed, by design.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import AuthGuards, render

logger = logging.getLogger(__name__)


class EgressRoutes:
    """Read-only egress monitoring: a JSON stats endpoint + a dashboard page.

    Available to any logged-in user (it exposes no secrets and takes no actions)."""

    def __init__(self, app: FastAPI, guards: AuthGuards):
        self.app = app
        self.guards = guards
        self._register_routes()
        logger.info("Egress routes initialized")

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/egress', self.egress_page, methods=['GET'], name='egress_monitoring')
        add('/egress/stats', self.egress_stats, methods=['GET'], name='egress_stats')

    def egress_page(self, request: Request):
        """Render the read-only egress monitoring page (login required)."""
        self.guards.login_required(request)
        return render(request, 'egress.html')

    def egress_stats(self, request: Request):
        """JSON snapshot of the egress pool for THIS process: the live worker-thread
        count and one record per destination (written/retries/connected/committed
        offset/WAL bytes/high-water/last error). Never raises to the client - returns
        an empty, well-formed payload on error so the page degrades gracefully."""
        self.guards.login_required(request)
        try:
            from core.egress.drainer import get_manager
            mgr = get_manager()
            return JSONResponse({
                'worker_count': mgr.worker_count(),
                'destinations': mgr.stats(),
            })
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reading egress stats: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse(
                {'error': str(e), 'worker_count': 0, 'destinations': []},
                status_code=500)
