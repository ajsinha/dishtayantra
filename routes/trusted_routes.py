"""Trusted-server admin + the per-session service-plane switch.

Admin-only management of the trusted-server registry (add / test / rotate /
remove), plus the login-only ``/service/select`` endpoint that sets the active
target in the user's session. The active target is read back by
``core.service.client.get_service_client`` so the management surface re-targets.

The clear API key is accepted only on add/rotate (form field), is encrypted
immediately by the registry, and is never rendered back. Mutations are
audit-logged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse

from core.audit_log import audit, client_ip
from core.service import crypto
from web.fastapi_compat import (
    AuthGuards,
    flash,
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


class TrustedServerRoutes:
    """Routes for managing trusted servers and switching the active target."""

    def __init__(self, app: FastAPI, guards: AuthGuards, registry):
        self.app = app
        self.guards = guards
        self.registry = registry
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/admin/trusted-servers', self.page, methods=['GET'],
            name='trusted_servers_page', include_in_schema=False)
        add('/admin/trusted-servers/add', self.add_server, methods=['POST'],
            name='trusted_add', include_in_schema=False)
        add('/admin/trusted-servers/{server_id}/test', self.test_server,
            methods=['POST'], name='trusted_test', include_in_schema=False)
        add('/admin/trusted-servers/{server_id}/rotate', self.rotate_server,
            methods=['POST'], name='trusted_rotate', include_in_schema=False)
        add('/admin/trusted-servers/{server_id}/remove', self.remove_server,
            methods=['POST'], name='trusted_remove', include_in_schema=False)
        add('/service/select', self.select, methods=['POST'],
            name='service_select', include_in_schema=False)

    # ------------------------------------------------------------------ page
    def page(self, request: Request):
        self.guards.admin_required(request)
        servers = []
        secret_ok = True
        try:
            servers = self.registry.list()
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading trusted servers', e)
        try:
            secret_ok = crypto.secret_available()
        except Exception:  # noqa: BLE001
            secret_ok = False
        return render(request, 'admin/trusted_servers.html',
                      servers=servers, secret_ok=secret_ok)

    # --------------------------------------------------------------- actions
    async def add_server(self, request: Request):
        admin = self.guards.admin_required(request)
        form = await request.form()
        sid = (form.get('server_id') or '').strip()
        name = (form.get('name') or '').strip()
        url = (form.get('url') or '').strip()
        key = (form.get('api_key') or '').strip()
        role = (form.get('role') or 'admin').strip()
        verify_tls = (form.get('verify_tls') or 'on') in ('on', '1', 'true', 'yes')
        if not (sid and name and url and key):
            flash(request, 'server_id, name, url and api_key are all required',
                  'error')
            return redirect_to(request, 'trusted_servers_page')
        if role not in ('admin', 'user'):
            role = 'admin'
        try:
            self.registry.add(sid, name, url, key, role=role,
                              verify_tls=verify_tls, added_by=admin)
            audit('trusted_server.add', actor=admin, target=sid,
                  detail=f"url={url} role={role}", source_ip=client_ip(request))
            flash(request, f"Trusted server '{name}' added and verified", 'success')
        except Exception as e:  # noqa: BLE001 - probe/crypto/dup failures
            audit('trusted_server.add', actor=admin, target=sid,
                  detail=str(e), source_ip=client_ip(request), success=False)
            flash_error_and_log(request, f"Could not add '{sid}'", e)
        return redirect_to(request, 'trusted_servers_page')

    async def rotate_server(self, request: Request, server_id: str):
        admin = self.guards.admin_required(request)
        form = await request.form()
        key = (form.get('api_key') or '').strip()
        if not key:
            flash(request, 'A new api_key is required to rotate', 'error')
            return redirect_to(request, 'trusted_servers_page')
        try:
            self.registry.rotate_key(server_id, key, rotated_by=admin)
            audit('trusted_server.rotate', actor=admin, target=server_id,
                  source_ip=client_ip(request))
            flash(request, f"Key rotated for '{server_id}'", 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, f"Could not rotate '{server_id}'", e)
        return redirect_to(request, 'trusted_servers_page')

    def test_server(self, request: Request, server_id: str):
        self.guards.admin_required(request)
        try:
            info = self.registry.test(server_id)
            flash(request, f"'{server_id}' reachable - {info.name} v{info.version}",
                  'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, f"'{server_id}' probe failed", e)
        return redirect_to(request, 'trusted_servers_page')

    def remove_server(self, request: Request, server_id: str):
        admin = self.guards.admin_required(request)
        try:
            self.registry.remove(server_id, removed_by=admin)
            audit('trusted_server.remove', actor=admin, target=server_id,
                  source_ip=client_ip(request))
            flash(request, f"Trusted server '{server_id}' removed", 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, f"Could not remove '{server_id}'", e)
        return redirect_to(request, 'trusted_servers_page')

    # ---------------------------------------------------------------- switch
    async def select(self, request: Request):
        """Set the per-session active target. Login-only (any user may view)."""
        self.guards.login_required(request)
        form = await request.form()
        target = (form.get('target') or 'local').strip()
        if target != 'local':
            ts = None
            try:
                ts = self.registry.get(target)
            except Exception:  # noqa: BLE001
                ts = None
            if ts is None:
                flash(request, f"Unknown server '{target}'", 'error')
                target = 'local'
        request.session['service_target'] = target
        if target != 'local':
            flash(request, f"Now viewing '{target}'", 'info')
        else:
            flash(request, 'Now viewing the local server', 'info')
        # return to the page the user came from, else the dashboard
        referer = request.headers.get('referer')
        if referer:
            return RedirectResponse(referer, status_code=303)
        return redirect_to(request, 'dashboard')
