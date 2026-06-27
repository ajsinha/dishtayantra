"""Service-plane JSON management API (Phase 1).

A clean JSON-in/JSON-out management surface that returns *structured* results
(not HTML/flash). It shares the exact same operation path as the HTML handlers -
both go through :func:`core.service.client.get_service_client` and the
``LocalServiceClient`` - so there is one source of truth for each operation.

This API is the contract the future ``RestServiceClient`` will target, letting a
UI plane manage a remote service plane. The promised surface is enumerated once
in :data:`core.service.contract.SERVICE_OPERATIONS`; a drift test asserts every
promised operation actually appears in the served OpenAPI schema.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.service.client import get_service_client
from web.fastapi_compat import AuthGuards

logger = logging.getLogger(__name__)


class ServiceRoutes:
    """Registers the /api/service/* management endpoints."""

    def __init__(self, app: FastAPI, guards: AuthGuards):
        self.app = app
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/api/service/info', self.info, methods=['GET'],
            name='service_info')
        add('/api/service/dags', self.dags, methods=['GET'],
            name='service_dags')
        add('/api/service/dag/{dag_id}', self.dag_details, methods=['GET'],
            name='service_dag_details')
        add('/api/service/status', self.status, methods=['GET'],
            name='service_status')
        add('/api/service/dags/reload', self.reload, methods=['POST'],
            name='service_reload')
        add('/api/service/dag/{dag_id}/start', self.start, methods=['POST'],
            name='service_start')
        add('/api/service/dag/{dag_id}/stop', self.stop, methods=['POST'],
            name='service_stop')
        add('/api/service/dag/{dag_id}/suspend', self.suspend, methods=['POST'],
            name='service_suspend')
        add('/api/service/dag/{dag_id}/resume', self.resume, methods=['POST'],
            name='service_resume')

    # ---- reads ---------------------------------------------------------
    def info(self, request: Request):
        self.guards.login_required(request)
        return JSONResponse(get_service_client(request).info().to_dict())

    def dags(self, request: Request):
        self.guards.login_required(request)
        return JSONResponse({"dags": get_service_client(request).list_dags()})

    def dag_details(self, request: Request, dag_id: str):
        self.guards.login_required(request)
        try:
            return JSONResponse(get_service_client(request).dag_details(dag_id))
        except ValueError as e:
            return JSONResponse({"ok": False, "message": str(e),
                                 "level": "error", "data": {}},
                                status_code=404)

    def status(self, request: Request):
        self.guards.login_required(request)
        return JSONResponse(get_service_client(request).server_status())

    # ---- mutations -----------------------------------------------------
    def _mutate(self, request: Request, op_name: str, *args):
        """Run a lifecycle mutation and shape a uniform JSON result.

        ValueError (e.g. unknown DAG) -> 404; anything else -> 500. Both return
        an OpResult-shaped body so a remote caller gets structured errors rather
        than an opaque stack trace.
        """
        self.guards.admin_required(request)
        svc = get_service_client(request)
        op = getattr(svc, op_name)
        try:
            return JSONResponse(op(*args).to_dict())
        except ValueError as e:
            return JSONResponse({"ok": False, "message": str(e),
                                 "level": "error", "data": {}},
                                status_code=404)
        except Exception as e:  # noqa: BLE001 - surface as structured 500
            logger.exception("service mutation %s failed", op_name)
            return JSONResponse({"ok": False, "message": str(e),
                                 "level": "error", "data": {}},
                                status_code=500)

    def reload(self, request: Request):
        return self._mutate(request, "reload_dags")

    def start(self, request: Request, dag_id: str):
        return self._mutate(request, "start_dag", dag_id)

    def stop(self, request: Request, dag_id: str):
        return self._mutate(request, "stop_dag", dag_id)

    def suspend(self, request: Request, dag_id: str):
        return self._mutate(request, "suspend_dag", dag_id)

    def resume(self, request: Request, dag_id: str):
        return self._mutate(request, "resume_dag", dag_id)
