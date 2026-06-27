"""The ``ServiceClient`` seam.

``ServiceClient`` is the management surface the UI talks to. Two flavours
implement it:

* :class:`LocalServiceClient` - in-process calls to this instance's
  ``DAGComputeServer``. This is today's behaviour and the contract reference;
  it has no network and cannot fail in the ways a remote can.
* ``RestServiceClient`` (later phase) - HTTPS calls to a trusted remote
  instance's JSON management API.

Route code obtains a client via :func:`get_service_client` and never cares which
flavour it got. In Phase 0 the resolver always returns the local client; Phase 2
extends it to honour the session's selected trusted server.

Mutations return :class:`~core.service.types.OpResult` and **raise** on failure
(e.g. ``ValueError`` for an unknown DAG) rather than swallowing - callers map
exceptions to their medium (HTML flash, JSON error body). This keeps the client
honest and side-effect-free of presentation, consistent with the project's
"no swallowed exceptions" rule.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import abc

from core.service.contract import CAPABILITIES, SCHEMA_VERSION
from core.service.types import OpResult, ServiceInfo
from core.version import APP_NAME, BUILD_DATE, VERSION


class ServiceClient(abc.ABC):
    """Management surface for one DAG server (local or remote)."""

    # ---- identity ------------------------------------------------------
    @abc.abstractmethod
    def info(self) -> ServiceInfo:
        ...

    # ---- inventory (read) ---------------------------------------------
    @abc.abstractmethod
    def list_dags(self) -> list:
        ...

    @abc.abstractmethod
    def dag_details(self, dag_id: str) -> dict:
        ...

    @abc.abstractmethod
    def server_status(self) -> dict:
        ...

    # ---- lifecycle (mutations) ----------------------------------------
    @abc.abstractmethod
    def reload_dags(self) -> OpResult:
        ...

    @abc.abstractmethod
    def start_dag(self, dag_id: str) -> OpResult:
        ...

    @abc.abstractmethod
    def stop_dag(self, dag_id: str) -> OpResult:
        ...

    @abc.abstractmethod
    def suspend_dag(self, dag_id: str) -> OpResult:
        ...

    @abc.abstractmethod
    def resume_dag(self, dag_id: str) -> OpResult:
        ...


class LocalServiceClient(ServiceClient):
    """In-process flavour: thin wrapper over the local ``DAGComputeServer``.

    The behaviour is identical to calling the server directly - this class adds
    only the uniform :class:`ServiceClient` shape so the same route code can
    later target a remote instance.
    """

    def __init__(self, dag_server, worker_pool=None):
        self._dag_server = dag_server
        self._worker_pool = worker_pool

    def info(self) -> ServiceInfo:
        return ServiceInfo(
            name=APP_NAME,
            version=VERSION,
            build_date=BUILD_DATE,
            schema_version=SCHEMA_VERSION,
            capabilities=list(CAPABILITIES),
        )

    def list_dags(self) -> list:
        return self._dag_server.list_dags()

    def dag_details(self, dag_id: str) -> dict:
        return self._dag_server.details(dag_id)

    def server_status(self) -> dict:
        return self._dag_server.get_server_status()

    def reload_dags(self) -> OpResult:
        # remove_missing=True mirrors the HTML reload handler so the JSON and
        # HTML reloads have identical effect.
        result = self._dag_server.reload_from_storage(remove_missing=True) or {}
        added = result.get("added", [])
        removed = result.get("removed", [])
        msg = (f"DAGs reloaded (added {len(added)}, removed {len(removed)})"
               if (added or removed)
               else "DAGs reloaded - registry already matches storage")
        return OpResult.success(msg, **result)

    def start_dag(self, dag_id: str) -> OpResult:
        self._dag_server.start(dag_id)
        return OpResult.success(f"DAG {dag_id} started")

    def stop_dag(self, dag_id: str) -> OpResult:
        self._dag_server.stop(dag_id)
        return OpResult.success(f"DAG {dag_id} stopped")

    def suspend_dag(self, dag_id: str) -> OpResult:
        self._dag_server.suspend(dag_id)
        return OpResult.success(f"DAG {dag_id} suspended")

    def resume_dag(self, dag_id: str) -> OpResult:
        self._dag_server.resume(dag_id)
        return OpResult.success(f"DAG {dag_id} resumed")


def _local_client(request):
    client = getattr(request.app.state, "local_service_client", None)
    if client is None:
        dag_server = getattr(request.app.state, "dag_server", None)
        if dag_server is None:
            raise RuntimeError(
                "No service client available: app.state has neither "
                "local_service_client nor dag_server")
        client = LocalServiceClient(dag_server)
        request.app.state.local_service_client = client
    return client


def active_target(request) -> str:
    """The session's selected target id ('local' or a trusted server_id)."""
    try:
        return request.session.get("service_target") or "local"
    except Exception:  # noqa: BLE001 - no session (e.g. API-key call)
        return "local"


def active_target_is_remote(request) -> bool:
    return active_target(request) != "local"


def get_service_client(request) -> ServiceClient:
    """Resolve the active :class:`ServiceClient` for this request.

    Reads the per-session selected target. 'local' (or no selection, or an
    API-key call with no session) -> the in-process client. Otherwise build a
    remote client for the selected trusted server.

    A selection pointing at a server that no longer exists is treated as stale
    and reset to local (safe: the server was removed). If a *present* server's
    client cannot be built (e.g. the decryption secret is misconfigured), the
    error propagates rather than silently acting on the local server - acting
    locally when the user believes they are on a remote would be dangerous for
    mutations.
    """
    target = active_target(request)
    if target == "local":
        return _local_client(request)
    registry = getattr(request.app.state, "trusted_registry", None)
    if registry is None or registry.get(target) is None:
        # registry absent or stale selection -> safe reset to local
        try:
            request.session["service_target"] = "local"
        except Exception:  # noqa: BLE001
            pass
        return _local_client(request)
    username = None
    try:
        username = request.session.get("username")
    except Exception:  # noqa: BLE001
        pass
    instance = getattr(request.app.state, "service_instance_name", None) \
        or APP_NAME
    on_behalf = f"{username or 'unknown'}@{instance}"
    client = registry.build_client(target, on_behalf=on_behalf)
    return client if client is not None else _local_client(request)
