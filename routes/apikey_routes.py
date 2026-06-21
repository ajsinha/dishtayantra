"""
API Key Management Routes (FastAPI)
===================================

Admin-only web UI and form endpoints for managing programmatic API keys. Keys
are bound to a user (they inherit that user's roles) and are stored in the
database as salted hashes; the clear key is shown exactly once, at creation.

These pages complement the local minting tool (tools/dyapikey.py) by letting an
admin create / list / revoke keys from the browser, so API-key auth can be
managed without server-host access.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, Request

from web.fastapi_compat import (
    AuthGuards,
    flash_error_and_log,
    redirect_to,
    render,
)
from core.audit_log import audit, client_ip

logger = logging.getLogger(__name__)


class ApiKeyRoutes:
    """Admin-only API-key management (web UI + form posts)."""

    def __init__(self, app: FastAPI, user_registry, guards: AuthGuards):
        self.app = app
        self.user_registry = user_registry
        self.guards = guards
        self._register_routes()

    def _register_routes(self):
        add = self.app.add_api_route
        add('/admin/api-keys', self.api_keys_page, methods=['GET'],
            name='apikey_management')
        add('/admin/api-keys/create', self.api_key_create, methods=['POST'],
            name='apikey_create')
        add('/admin/api-keys/{key_id:int}/revoke', self.api_key_revoke,
            methods=['POST'], name='apikey_revoke')

    # -- helpers ---------------------------------------------------------
    def _all_keys(self):
        """Aggregate every user's keys, tagged with the owning username."""
        rows = []
        users = self.user_registry.list_all_users()
        for username in sorted(users):
            for k in self.user_registry.list_api_keys(username):
                k = dict(k)
                k['username'] = username
                rows.append(k)
        # newest first when created_at is available
        rows.sort(key=lambda r: (r.get('created_at') or ''), reverse=True)
        return rows

    def _context(self, request, new_key=None, new_key_meta=None):
        return {
            'users': sorted(self.user_registry.list_all_users().keys()),
            'api_keys': self._all_keys(),
            'new_key': new_key,
            'new_key_meta': new_key_meta or {},
        }

    # -- handlers --------------------------------------------------------
    def api_keys_page(self, request: Request):
        self.guards.admin_required(request)
        return render(request, 'admin/api_keys.html', **self._context(request))

    async def api_key_create(self, request: Request):
        admin_user = self.guards.admin_required(request)
        try:
            form = await request.form()
            username = (form.get('username') or '').strip()
            key_name = (form.get('key_name') or '').strip()
            expires_days_raw = (form.get('expires_days') or '').strip()

            if not username or not self.user_registry.user_exists(username):
                return redirect_to(request, 'apikey_management',
                                   flash_message='Select a valid user for the key',
                                   flash_category='error')
            if not key_name:
                return redirect_to(request, 'apikey_management',
                                   flash_message='A key name is required',
                                   flash_category='error')

            expires_at = None
            if expires_days_raw:
                try:
                    days = int(expires_days_raw)
                    if days <= 0:
                        raise ValueError
                except ValueError:
                    return redirect_to(
                        request, 'apikey_management',
                        flash_message='Expiry (days) must be a positive whole '
                                      'number, or left blank for no expiry',
                        flash_category='error')
                expires_at = (datetime.now(timezone.utc)
                              + timedelta(days=days)).replace(tzinfo=None)

            clear_key, record = self.user_registry.create_api_key(
                username, key_name, created_by=admin_user, expires_at=expires_at)
            logger.info("API key '%s' (prefix %s) created for user '%s' by '%s'",
                        key_name, record.get('key_prefix'), username, admin_user)
            audit('apikey.create', actor=admin_user,
                  target=f"{username}/{key_name}",
                  detail=f"prefix {record.get('key_prefix')}"
                         + (f", expires {expires_at.isoformat()}Z" if expires_at
                            else ", no expiry"),
                  source_ip=client_ip(request))

            # Render directly (not a redirect) so the clear key can be shown
            # exactly once in the response body - it is never stored anywhere
            # retrievable and never put in the session cookie.
            meta = {'username': username, 'name': key_name,
                    'prefix': record.get('key_prefix'),
                    'expires_at': expires_at.isoformat() + 'Z' if expires_at else None}
            return render(request, 'admin/api_keys.html',
                          **self._context(request, new_key=clear_key,
                                          new_key_meta=meta))
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error creating API key', e)
            return redirect_to(request, 'apikey_management')

    async def api_key_revoke(self, request: Request, key_id: int):
        admin_user = self.guards.admin_required(request)
        try:
            self.user_registry.revoke_api_key(key_id, revoked_by=admin_user)
            logger.info("API key id=%s revoked by '%s'", key_id, admin_user)
            audit('apikey.revoke', actor=admin_user, target=f"key#{key_id}",
                  source_ip=client_ip(request))
            return redirect_to(request, 'apikey_management',
                               flash_message=f'API key #{key_id} revoked')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error revoking API key', e)
            return redirect_to(request, 'apikey_management')
