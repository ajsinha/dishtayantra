"""
routes/maintenance_routes.py - admin "maintenance window" controls (v5.15.0).

Freeze / unfreeze DAG subscribers (drain mode) so a maintenance window can be
prepared gracefully: stop intake, let queues + publications drain, then restart.
Works for every broker (the freeze is a generic consume-loop flag; Kafka also
gets a broker-level pause). Subscribers running in worker processes are reached
via the worker pool's broadcast.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import AuthGuards, flash, redirect_to, render

logger = logging.getLogger(__name__)


class MaintenanceRoutes:
    """Admin freeze/drain controls for maintenance windows."""

    def __init__(self, app: FastAPI, dag_server, guards: AuthGuards,
                 worker_pool=None):
        self.app = app
        self.dag_server = dag_server
        self.guards = guards
        self.worker_pool = worker_pool
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/admin/maintenance', self.maintenance_page, methods=['GET'],
            name='maintenance_page', include_in_schema=False)
        add('/admin/maintenance/apply', self.maintenance_apply, methods=['POST'],
            name='maintenance_apply', include_in_schema=False)
        add('/admin/maintenance/status', self.maintenance_status, methods=['GET'],
            name='maintenance_status')

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _is_worker_dag(self, dag_name):
        wp = self.worker_pool
        if wp is None:
            return False
        try:
            if not wp.is_running():
                return False
            return wp.get_dag_assignment(dag_name) is not None
        except Exception:  # noqa: BLE001
            return False

    def _dag_view(self, dag_name):
        """Fetch the view-model for a DAG (worker snapshot or local build)."""
        if self._is_worker_dag(dag_name):
            try:
                snap = self.worker_pool.get_dag_state(dag_name)
                if isinstance(snap, dict) and isinstance(snap.get('view'), dict):
                    return snap['view']
            except Exception:  # noqa: BLE001
                pass
        dag = self.dag_server.dags.get(dag_name)
        if dag is not None:
            from core.dag.dag_view import build_dag_view
            return build_dag_view(dag)
        return None

    def _collect(self):
        """Per-DAG drain/subscriber summary for the page + status API."""
        out = []
        for dag_name in sorted(self.dag_server.dags.keys()):
            view = self._dag_view(dag_name)
            if not view:
                continue
            stats = view.get('dag_stats', {})
            drain = stats.get('drain', {})
            subs = [{'name': s['name'],
                     'source': s.get('source', ''),
                     'frozen': s.get('frozen', False),
                     'queue_depth': s.get('queue_depth', 0) or 0}
                    for s in stats.get('ingest_breakdown', [])]
            out.append({
                'dag_name': dag_name,
                'on_worker': self._is_worker_dag(dag_name),
                'drained': drain.get('drained', True),
                'any_frozen': drain.get('any_frozen', False),
                'subscriber_queue_total': drain.get('subscriber_queue_total', 0),
                'publisher_queue_total': drain.get('publisher_queue_total', 0),
                'wal_pending': drain.get('wal_pending', 0),
                'subscribers': subs,
            })
        return out

    def _apply(self, dag_name, subscribers, freeze):
        """Apply freeze/unfreeze locally or via the worker that hosts the DAG."""
        if self._is_worker_dag(dag_name):
            fn = (self.worker_pool.freeze_subscribers if freeze
                  else self.worker_pool.unfreeze_subscribers)
            return bool(fn(dag_name, subscribers))
        dag = self.dag_server.dags.get(dag_name)
        if dag is None:
            return False
        (dag.freeze_subscribers if freeze else dag.unfreeze_subscribers)(subscribers)
        return True

    # ------------------------------------------------------------------ #
    # Routes
    # ------------------------------------------------------------------ #
    def maintenance_page(self, request: Request):
        self.guards.admin_required(request)
        dags = self._collect()
        all_drained = all(d['drained'] for d in dags) if dags else True
        any_frozen = any(d['any_frozen'] for d in dags)
        return render(request, 'admin/maintenance.html',
                      dags=dags, all_drained=all_drained, any_frozen=any_frozen,
                      worker_mode=bool(self.worker_pool))

    def maintenance_status(self, request: Request):
        """JSON drain status for auto-refresh and restart automation."""
        self.guards.admin_required(request)
        dags = self._collect()
        return JSONResponse({
            'dags': dags,
            'all_drained': all(d['drained'] for d in dags) if dags else True,
            'any_frozen': any(d['any_frozen'] for d in dags),
        })

    async def maintenance_apply(self, request: Request):
        self.guards.admin_required(request)
        form = await request.form()
        action = (form.get('action') or '').strip()         # freeze | unfreeze
        scope = (form.get('scope') or 'dag').strip()         # dag | global
        dag_name = (form.get('dag_name') or '').strip()
        subs_raw = (form.get('subscribers') or 'ALL').strip()
        subscribers = None if subs_raw.upper() == 'ALL' else \
            [s.strip() for s in subs_raw.split(',') if s.strip()]
        freeze = (action == 'freeze')
        if action not in ('freeze', 'unfreeze'):
            flash(request, "Unknown action.", 'error')
            return redirect_to(request, 'maintenance_page')

        try:
            if scope == 'global':
                targets = list(self.dag_server.dags.keys())
                done = sum(1 for d in targets if self._apply(d, None, freeze))
                flash(request,
                      f"{'Froze' if freeze else 'Unfroze'} subscribers on "
                      f"{done}/{len(targets)} DAG(s). "
                      + ("Watch the drain status before restarting."
                         if freeze else ""),
                      'success')
            else:
                if not dag_name:
                    flash(request, "No DAG specified.", 'error')
                    return redirect_to(request, 'maintenance_page')
                ok = self._apply(dag_name, subscribers, freeze)
                tgt = "all subscribers" if subscribers is None \
                    else ", ".join(subscribers)
                flash(request,
                      (f"{'Froze' if freeze else 'Unfroze'} {tgt} on "
                       f"'{dag_name}'." if ok
                       else f"Could not apply to '{dag_name}'."),
                      'success' if ok else 'error')
        except Exception as e:  # noqa: BLE001
            flash(request, f"Maintenance action failed: {e}", 'error')
        return redirect_to(request, 'maintenance_page')
