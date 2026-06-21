"""
Audit Trail Routes (FastAPI)
============================

Admin-only, read-only view over the audit log (append-only records of
security/admin-relevant actions). Supports filtering by actor, action, and
outcome. Events are written from across the app via core.audit_log.audit.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from fastapi import FastAPI, Request

from web.fastapi_compat import AuthGuards, render

logger = logging.getLogger(__name__)


class AuditRoutes:
    """Admin-only audit-trail viewer."""

    def __init__(self, app: FastAPI, guards: AuthGuards):
        self.app = app
        self.guards = guards
        from core.db.dao import AuditDAO
        self.dao = AuditDAO()
        self._register_routes()

    def _register_routes(self):
        self.app.add_api_route('/admin/audit', self.audit_page, methods=['GET'],
                               name='audit_log')

    def audit_page(self, request: Request):
        self.guards.admin_required(request)
        qp = request.query_params
        actor = (qp.get('actor') or '').strip() or None
        action = (qp.get('action') or '').strip() or None
        success_raw = (qp.get('success') or '').strip().lower()
        success = True if success_raw in ('true', '1', 'yes') else \
            False if success_raw in ('false', '0', 'no') else None
        try:
            limit = min(max(int(qp.get('limit', 200)), 1), 1000)
        except (TypeError, ValueError):
            limit = 200

        events = self.dao.list_events(limit=limit, actor=actor, action=action,
                                      success=success)
        return render(request, 'admin/audit.html',
                      events=events,
                      actions=self.dao.distinct_actions(),
                      f_actor=actor or '',
                      f_action=action or '',
                      f_success=success_raw,
                      limit=limit)
