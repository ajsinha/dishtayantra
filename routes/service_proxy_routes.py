"""Service-plane proxy (Phase 3 breadth) - the UI plane as a gateway.

When a user has selected a remote trusted server, broad read/op surfaces that
are not part of the typed management contract (flow time-travel, metrics/health,
worker *status*, egress *status*, designer components/validate/deploy) need to
re-target too. Rather than duplicate every subsystem's API into the service
contract - which would re-introduce the parallel-structure drift this project
keeps eliminating - the UI plane proxies a **strict whitelist** of existing API
paths to the active target.

Design:
* Single explicit route ``/api/proxy/{path}`` (login-required) so the blast
  radius is contained (no dispatch-intercepting middleware). A tiny fetch shim
  in base.html re-routes whitelisted calls here only when a remote target is
  active; local behaviour is untouched.
* Forwards with the trusted server's ``Bearer`` key + ``X-DY-On-Behalf-Of`` so
  the remote authorises and audits correctly.
* The whitelist explicitly EXCLUDES host-scoped admin (worker pool start/stop,
  DAG migrate, JVM/C++/Rust runtimes, maintenance) and all auth/user/session
  surfaces - those always act on the plane you logged into.
* ``requests`` runs in a threadpool so the async event loop is not blocked.
  Streaming exports are buffered for v1 (bounded windows); a push/stream
  transport is the Phase 5 SSE item.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from core.service.client import active_target
from web.fastapi_compat import AuthGuards

logger = logging.getLogger(__name__)

# (method, path-prefix) rules that may be proxied to a remote plane. A request
# is allowed if its method matches and its path starts with a listed prefix.
# Read surfaces + the few safe mutations that §3.3 marks as re-targetable
# (flow capture toggle, designer deploy). Nothing host-scoped, nothing auth.
_ALLOW = (
    ("GET",  "/api/flow"),
    ("POST", "/api/flow/enable"),
    ("POST", "/api/flow/disable"),
    ("GET",  "/api/dag-designer/components"),
    ("GET",  "/api/dag-designer/folders"),
    ("POST", "/api/dag-designer/validate"),
    ("POST", "/api/dag-designer/save"),
    ("GET",  "/api/workers/status"),
    ("GET",  "/api/workers/assignments"),
    ("GET",  "/api/workers/load-summary"),
    ("GET",  "/metrics"),
    ("GET",  "/health"),
    ("GET",  "/egress/stats"),
    ("GET",  "/api/service"),
)

_HOP_BY_HOP = {"connection", "keep-alive", "transfer-encoding", "te",
               "trailer", "upgrade", "proxy-authorization",
               "proxy-authenticate", "content-encoding", "content-length"}


def _is_allowed(method: str, path: str) -> bool:
    return any(method == m and path.startswith(p) for m, p in _ALLOW)


class ServiceProxyRoutes:
    """Registers the whitelisted /api/proxy/{path} gateway route."""

    def __init__(self, app: FastAPI, guards: AuthGuards, registry):
        self.app = app
        self.guards = guards
        self.registry = registry
        app.add_api_route('/api/proxy/{path:path}', self.proxy,
                          methods=['GET', 'POST'], name='service_proxy',
                          include_in_schema=False)

    async def proxy(self, request: Request, path: str):
        # Authenticated UI-plane users only - never an open relay.
        username = self.guards.login_required(request)
        target = active_target(request)
        if target == 'local' or self.registry is None:
            return JSONResponse(
                {"ok": False, "message": "no remote target selected"},
                status_code=409)
        ts = self.registry.get(target)
        if ts is None:
            return JSONResponse({"ok": False, "message": "unknown target"},
                                status_code=404)

        upstream_path = "/" + path.lstrip("/")
        if not _is_allowed(request.method, upstream_path):
            logger.warning("proxy refused %s %s -> %s",
                           request.method, upstream_path, target)
            return JSONResponse(
                {"ok": False,
                 "message": "this surface is not proxied to remote servers"},
                status_code=403)

        key = self.registry.get_clear_key(target)
        instance = getattr(request.app.state, "service_instance_name", "uiplane")
        url = ts["url"].rstrip("/") + upstream_path
        headers = {"Accept": request.headers.get("accept", "application/json")}
        if key:
            headers["Authorization"] = f"Bearer {key}"
        headers["X-DY-On-Behalf-Of"] = f"{username or 'unknown'}@{instance}"
        params = dict(request.query_params)
        body = await request.body() if request.method == "POST" else None
        if body and request.headers.get("content-type"):
            headers["Content-Type"] = request.headers["content-type"]

        try:
            resp = await run_in_threadpool(
                self._send, target, ts["verify_tls"], request.method, url,
                headers, params, body)
        except Exception as e:  # noqa: BLE001 - surface, never 500-stack
            logger.warning("proxy to %s failed: %s", target, e)
            return JSONResponse(
                {"ok": False, "message": f"remote '{target}' unreachable: {e}"},
                status_code=502)

        out_headers = {k: v for k, v in resp.headers.items()
                       if k.lower() not in _HOP_BY_HOP}
        return Response(content=resp.content, status_code=resp.status_code,
                        headers=out_headers,
                        media_type=resp.headers.get("content-type"))

    def _send(self, target, verify_tls, method, url, headers, params, body):
        sess = self.registry._session_for(target, verify_tls)
        return sess.request(method, url, headers=headers, params=params,
                            data=body, timeout=30.0)
