"""
FastAPI Compatibility Layer (v2.0.0)
====================================

Bridges the ~60 Jinja templates written for Flask onto FastAPI/Starlette so
the FastAPI migration required zero template changes.  Provides:

    - ``url_for(endpoint, **params)`` Jinja global with Flask semantics:
      path params are substituted, unknown kwargs become the query string,
      and ``url_for('static', filename=...)`` maps to the mounted static app.
    - ``session`` in every template context (the Starlette session dict).
    - ``get_flashed_messages(with_categories=...)`` with Flask semantics,
      backed by the session (``flash(request, message, category)`` helper).
    - :class:`NotAuthenticatedError` / :class:`NotAuthorizedError` raised by
      the auth guards and translated by app-level exception handlers into
      redirects (HTML pages) or 401/403 JSON (``/api/...`` paths).
    - ``render(request, template, **context)`` and ``redirect_to(request,
      endpoint, flash_message=None, ...)`` convenience wrappers used by every
      route class.

Error policy (architecture mandate): UI failures are flashed to the user AND
logged with the full stack trace; nothing is swallowed.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback
from typing import Iterable, List, Tuple, Union
from urllib.parse import urlencode

from fastapi import Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates

from core.version import VERSION

logger = logging.getLogger(__name__)

TEMPLATES_DIR = "web/templates"

# Filled in by DishtaYantraWebApp during construction so templates can show
# application metadata (name, version, author, copyright, ...).
APP_TEMPLATE_PROPS: dict = {
    "app_name": "DishtaYantra",
    "app_version": VERSION,
}


class NotAuthenticatedError(Exception):
    """Raised by guards when no user is logged in."""


class NotAuthorizedError(Exception):
    """Raised by guards when the user lacks the required role."""

    def __init__(self, message: str = "Admin access required"):
        super().__init__(message)
        self.message = message


# --------------------------------------------------------------------------- #
# Flash messages (Flask semantics on top of the Starlette session)
# --------------------------------------------------------------------------- #

def flash(request: Request, message: str, category: str = "message") -> None:
    """Queue a one-shot message to display on the next rendered page."""
    flashes = list(request.session.get("_flashes", []))
    flashes.append([category, message])
    request.session["_flashes"] = flashes


def get_flashed_messages(request: Request, with_categories: bool = False,
                         category_filter: Iterable[str] = ()
                         ) -> Union[List[str], List[Tuple[str, str]]]:
    """Pop and return queued flash messages (Flask-compatible signature)."""
    flashes = request.session.pop("_flashes", [])
    if category_filter:
        flashes = [f for f in flashes if f[0] in category_filter]
    if with_categories:
        return [tuple(f) for f in flashes]
    return [f[1] for f in flashes]


# --------------------------------------------------------------------------- #
# url_for with Flask semantics
# --------------------------------------------------------------------------- #

def flask_style_url_for(request: Request, name: str, **params) -> str:
    """
    Resolve a route name to a URL the way Flask's ``url_for`` does:

    - ``static`` routes use the ``filename`` kwarg as the static path.
    - kwargs matching the route's path parameters are substituted.
    - all remaining kwargs are appended as the query string.
    """
    app = request.app
    if name == "static":
        filename = params.pop("filename", "")
        url = app.url_path_for("static", path=filename)
        return f"{url}?{urlencode(params)}" if params else url

    path_param_names = set()
    for route in app.routes:
        if getattr(route, "name", None) == name:
            path_param_names = set(getattr(route, "param_convertors", {}).keys())
            break
    path_params = {k: v for k, v in params.items() if k in path_param_names}
    query_params = {k: v for k, v in params.items() if k not in path_param_names}
    url = app.url_path_for(name, **path_params)
    if query_params:
        url = f"{url}?{urlencode(query_params)}"
    return url


# --------------------------------------------------------------------------- #
# Templates with Flask-compatible context
# --------------------------------------------------------------------------- #

def _inject_flask_compat(request: Request) -> dict:
    """Context processor adding Flask-style globals to every template."""
    context = dict(APP_TEMPLATE_PROPS)
    context["session"] = request.session
    context["url_for"] = lambda name, **params: \
        flask_style_url_for(request, name, **params)
    context["get_flashed_messages"] = \
        lambda with_categories=False, category_filter=(): \
        get_flashed_messages(request, with_categories, category_filter)

    # v2.2: surface HA role (PRIMARY/SECONDARY) in the navbar on every
    # page - but only for logged-in users. ha_role stays None for
    # anonymous visitors, so the template simply omits the badge.
    context["ha_role"] = None
    if request.session.get("username"):
        try:
            dag_server = getattr(request.app.state, "dag_server", None)
            if dag_server is not None:
                context["ha_role"] = (
                    "PRIMARY" if dag_server.is_primary else "SECONDARY")
        except Exception:  # noqa: BLE001 - navbar badge is best-effort
            context["ha_role"] = None
    # v5.29.0: surface the active service-plane target + trusted-server list in
    # the navbar switcher (logged-in users only; best-effort).
    context["service_target"] = "local"
    context["trusted_servers"] = []
    if request.session.get("username"):
        try:
            from core.service.client import active_target
            context["service_target"] = active_target(request)
            registry = getattr(request.app.state, "trusted_registry", None)
            if registry is not None:
                context["trusted_servers"] = registry.list()
        except Exception:  # noqa: BLE001 - switcher is best-effort
            context["service_target"] = "local"
            context["trusted_servers"] = []
    return context


templates = Jinja2Templates(directory=TEMPLATES_DIR,
                            context_processors=[_inject_flask_compat])


def render(request: Request, template_name: str, status_code: int = 200,
           **context):
    """Render a Jinja template with the Flask-compatible context."""
    return templates.TemplateResponse(
        request=request, name=template_name, context=context,
        status_code=status_code)


def redirect_to(request: Request, endpoint: str, flash_message: str = None,
                flash_category: str = "success", **params) -> RedirectResponse:
    """
    Redirect to a named route, optionally flashing a message first.
    Uses 303 See Other so POST handlers correctly redirect to a GET.
    """
    if flash_message:
        flash(request, flash_message, flash_category)
    return RedirectResponse(
        url=flask_style_url_for(request, endpoint, **params), status_code=303)


def flash_error_and_log(request: Request, user_message: str,
                        exc: Exception) -> None:
    """
    Architecture mandate #5: show the error to the user via flash AND write
    the complete stack trace to the logs. Never swallow.
    """
    logger.error("%s: %s", user_message, exc)
    logger.error("Full stack trace:\n%s", traceback.format_exc())
    flash(request, f"{user_message}: {exc}", "error")


# --------------------------------------------------------------------------- #
# Auth guards (constructed once by the webapp, passed to route classes)
# --------------------------------------------------------------------------- #

class AuthGuards:
    """
    Login/admin guards used by every protected route handler.

    Handlers call ``guards.login_required(request)`` or
    ``guards.admin_required(request)`` as their first statement; the raised
    exceptions are translated by the app-level handlers registered in
    :mod:`web.dishtayantra_webapp` into redirects (HTML) or JSON errors
    (``/api/...`` paths).
    """

    def __init__(self, user_registry):
        self.user_registry = user_registry

    def _api_key_username(self, request: Request):
        """Resolve a username from an API key header, or None.

        Accepts ``Authorization: Bearer <key>`` or ``X-API-Key: <key>``.
        """
        auth = request.headers.get("authorization", "")
        key = None
        if auth[:7].lower() == "bearer ":
            key = auth[7:].strip()
        if not key:
            key = request.headers.get("x-api-key")
        if not key:
            return None
        info = self.user_registry.authenticate_api_key(key)
        return info.get("username") if info else None

    def _authenticated_username(self, request: Request):
        """Session cookie first, then API key header. None if neither."""
        return request.session.get("username") or self._api_key_username(request)

    def login_required(self, request: Request) -> str:
        """Ensure a user is logged in (session OR API key); returns the username."""
        username = self._authenticated_username(request)
        if not username:
            raise NotAuthenticatedError()
        return username

    def admin_required(self, request: Request) -> str:
        """Ensure the authenticated user holds the admin role."""
        username = self.login_required(request)
        if not self.user_registry.has_role(username, "admin"):
            raise NotAuthorizedError("Admin access required")
        return username


def wants_json(request: Request) -> bool:
    """Heuristic mirroring the legacy behaviour: API paths get JSON errors."""
    path = request.url.path
    return path.startswith("/api/") or "/api/" in path
