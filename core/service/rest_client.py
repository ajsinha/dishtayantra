"""Remote flavour of the ServiceClient seam.

``RestServiceClient`` talks to a *trusted* DishtaYantra instance's JSON
management API (``/api/service/*``, Phase 1) over HTTP, authenticating with that
instance's ``dyk_`` API key and annotating each call with an
``X-DY-On-Behalf-Of`` header so the remote's audit log records which UI-plane
user initiated the action (advisory only; never an authorization input).

It mirrors :class:`core.service.client.LocalServiceClient` exactly, including the
contract that **mutations raise on failure** (``ValueError`` for an unknown DAG,
to match local) - so route code behaves identically whichever flavour it gets.
Transport-only failures (unreachable / timeout / auth / malformed) raise the
typed errors in :mod:`core.service.errors`.

The HTTP transport is injectable (any object exposing ``.request(method, url,
headers=, params=, json=, timeout=)`` returning a response with
``.status_code`` and ``.json()``); the default builds a ``requests.Session``.
This keeps the client testable in-process against the app via a TestClient.

Connection pooling/health-checking is intentionally deferred to Phase 5; here a
client is constructed per request, which is correct if not maximally efficient.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from core.service.client import ServiceClient
from core.service.errors import (
    ServiceAuthError,
    ServiceProtocolError,
    ServiceTimeout,
    ServiceUnavailable,
)
from core.service.types import OpResult, ServiceInfo

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10.0  # seconds, per call


def _default_transport(verify_tls: bool):
    import requests
    sess = requests.Session()
    sess.verify = verify_tls
    return sess


class RestServiceClient(ServiceClient):
    def __init__(self, base_url, api_key, *, role="admin", verify_tls=True,
                 on_behalf=None, timeout=DEFAULT_TIMEOUT, http=None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.role = role
        self.on_behalf = on_behalf
        self.timeout = timeout
        self._http = http if http is not None else _default_transport(verify_tls)

    # ------------------------------------------------------------- transport
    def _headers(self):
        h = {"Accept": "application/json"}
        if self.api_key:
            h["Authorization"] = f"Bearer {self.api_key}"
        if self.on_behalf:
            h["X-DY-On-Behalf-Of"] = self.on_behalf
        return h

    def _send(self, method, path, params=None, json_body=None):
        url = f"{self.base_url}{path}"
        try:
            resp = self._http.request(
                method, url, headers=self._headers(), params=params,
                json=json_body, timeout=self.timeout)
        except Exception as exc:  # noqa: BLE001 - classify transport failures
            name = type(exc).__name__.lower()
            if "timeout" in name:
                raise ServiceTimeout(f"{self.base_url} timed out") from exc
            raise ServiceUnavailable(
                f"{self.base_url} unreachable: {exc}") from exc
        if resp.status_code in (401, 403):
            raise ServiceAuthError(
                f"{self.base_url} rejected credentials ({resp.status_code})")
        return resp

    @staticmethod
    def _json(resp):
        try:
            return resp.json()
        except Exception as exc:  # noqa: BLE001
            raise ServiceProtocolError(
                f"non-JSON response ({resp.status_code})") from exc

    def _read(self, path, params=None):
        resp = self._send("GET", path, params=params)
        if resp.status_code == 404:
            raise ValueError(self._json(resp).get("message", "not found"))
        if resp.status_code >= 400:
            raise ServiceProtocolError(
                f"GET {path} -> {resp.status_code}")
        return self._json(resp)

    def _mutate(self, path):
        resp = self._send("POST", path)
        body = self._json(resp)
        if resp.status_code == 404:
            # mirror LocalServiceClient: unknown DAG is a ValueError
            raise ValueError(body.get("message", "not found"))
        if resp.status_code >= 400:
            raise ServiceProtocolError(
                body.get("message", f"POST {path} -> {resp.status_code}"))
        return OpResult(ok=body.get("ok", True), message=body.get("message", ""),
                        level=body.get("level", "success"),
                        data=body.get("data", {}) or {})

    # ------------------------------------------------------------- interface
    def live(self) -> bool:
        """Liveness probe: GET /health/live (cheap, unauthenticated).

        Returns True if the server answered with a non-5xx status (it is up and
        serving). Raises the typed transport errors (ServiceUnavailable /
        ServiceTimeout) when the server cannot be reached at all - so callers
        can distinguish "down" (transport error) from "up but unhealthy"
        (a 5xx, returned as False). This is the *reachability* signal, separate
        from whether our credentials can actually manage it.
        """
        resp = self._send("GET", "/health/live")
        return resp.status_code < 500

    def info(self) -> ServiceInfo:
        d = self._read("/api/service/info")
        return ServiceInfo(
            name=d.get("name", "?"), version=d.get("version", "?"),
            build_date=d.get("build_date", "?"),
            schema_version=d.get("schema_version", "?"),
            capabilities=d.get("capabilities", []))

    def list_dags(self) -> list:
        return self._read("/api/service/dags").get("dags", [])

    def dag_details(self, dag_id: str) -> dict:
        return self._read(f"/api/service/dag/{dag_id}")

    def server_status(self) -> dict:
        return self._read("/api/service/status")

    def reload_dags(self) -> OpResult:
        return self._mutate("/api/service/dags/reload")

    def start_dag(self, dag_id: str) -> OpResult:
        return self._mutate(f"/api/service/dag/{dag_id}/start")

    def stop_dag(self, dag_id: str) -> OpResult:
        return self._mutate(f"/api/service/dag/{dag_id}/stop")

    def suspend_dag(self, dag_id: str) -> OpResult:
        return self._mutate(f"/api/service/dag/{dag_id}/suspend")

    def resume_dag(self, dag_id: str) -> OpResult:
        return self._mutate(f"/api/service/dag/{dag_id}/resume")
