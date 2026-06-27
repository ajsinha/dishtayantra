"""
Dashboard Routes (FastAPI, v2.0.0)
==================================

Main dashboard, per-DAG details, and per-DAG state views.  Read-only pages
available to any logged-in user.  Route names match the legacy Flask
endpoint names so templates work unchanged.

UI error policy (architecture mandate #5): failures are flashed to the user
AND logged with the full stack trace.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import os

from core.dag.dag_view import build_dag_view
from core.dag.edge_value import is_batch
from core.service.client import active_target_is_remote, get_service_client

from fastapi import FastAPI, Request

from web.fastapi_compat import (
    AuthGuards,
    flash,
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


def _state_value_safe(value, preview_rows=5):
    """Make a node input/output safe for the state view's ``| tojson`` filter.

    Arrow nodes hold a ``pyarrow.RecordBatch`` as their input/output, which is not
    JSON-serializable. Summarize it (schema + row count + a small preview) instead
    of letting the template raise. Dicts, lists, scalars, and None pass through
    UNCHANGED so normal (row) node state still renders as proper JSON.
    """
    try:
        if is_batch(value):
            try:
                preview = value.slice(0, preview_rows).to_pylist()
            except Exception:  # noqa: BLE001
                preview = []
            return {
                '__type__': 'pyarrow.RecordBatch',
                'num_rows': value.num_rows,
                'num_columns': value.num_columns,
                'schema': [f"{f.name}: {f.type}" for f in value.schema],
                'preview': preview,
                'preview_truncated': value.num_rows > preview_rows,
            }
    except Exception:  # noqa: BLE001
        pass
    return value


def _json_safe(value):
    """Coerce a value to something JSON-serializable for the graph payload.

    datetimes -> ISO string; everything else is returned as-is (str fallback
    for exotic types) so a node's live state never breaks template rendering.
    """
    if value is None:
        return None
    from datetime import datetime, date
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class DashboardRoutes:
    """Handles dashboard-related routes."""

    def __init__(self, app: FastAPI, dag_server, user_registry,
                 guards: AuthGuards, worker_pool=None):
        self.app = app
        self.dag_server = dag_server
        self.user_registry = user_registry
        self.guards = guards
        self.worker_pool = worker_pool  # v1.5.2: worker pool for assignments
        # v5.14.0: short-TTL cache of the assembled details view-model, keyed by
        # dag_name. Protects a busy server from rebuilding the view (and, for
        # worker DAGs, round-tripping get_dag_state) on every refresh / viewer.
        # 1s keeps the page effectively live. Bounded by the number of DAGs.
        self._view_cache = {}            # dag_name -> (monotonic_ts, view)
        self._view_cache_ttl = 1.0       # seconds
        self._register_routes()

    def _register_routes(self) -> None:
        """Register all dashboard routes."""
        add = self.app.add_api_route
        add('/dashboard', self.dashboard, methods=['GET'],
            name='dashboard', include_in_schema=False)
        add('/dag/{dag_name}/details', self.dag_details, methods=['GET'],
            name='dag_details', include_in_schema=False)
        add('/dag/{dag_name}/state', self.dag_state, methods=['GET'],
            name='dag_state', include_in_schema=False)

    def dashboard(self, request: Request):
        """Main dashboard view."""
        username = self.guards.login_required(request)
        try:
            svc = get_service_client(request)
            server_status = svc.server_status()
            dags = svc.list_dags()
            return render(request, 'dashboard.html',
                          dags=dags,
                          is_admin=self.user_registry.has_role(username,
                                                               'admin'),
                          name_collisions=getattr(self.dag_server,
                                                  'name_collisions', None) or [],
                          server_status=server_status)
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error loading dashboard', e)
            return render(request, 'dashboard.html', dags=[],
                          is_admin=False, server_status={})

    def dag_details(self, request: Request, dag_name: str):
        """DAG details view."""
        username = self.guards.login_required(request)
        if active_target_is_remote(request):
            flash(request, 'Detailed DAG view is not yet available for remote '
                           'servers - switch to Local to inspect a DAG.', 'info')
            return redirect_to(request, 'dashboard')
        try:
            # The DAG object in this (main) process. When the DAG runs in a
            # worker subprocess this is a lazy, unbuilt copy; the live view-model
            # is fetched from the worker below so the page is identical to
            # single-process mode.
            dag = self.dag_server.dags[dag_name]

            # v1.5.2: Execution location information
            is_running = (dag._compute_thread and
                          dag._compute_thread.is_alive()) \
                if hasattr(dag, '_compute_thread') else False

            worker_pool_enabled = (self.worker_pool is not None and
                                   self.worker_pool.is_running())

            if worker_pool_enabled:
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    worker_status = self.worker_pool.get_worker_status(
                        worker_id)
                    worker_info = {
                        'worker_id': worker_id,
                        'status': worker_status.get('state', 'unknown')
                        if worker_status else 'unknown',
                        'pid': worker_status.get('pid')
                        if worker_status else None,
                        'dag_count': worker_status.get('dag_count', 0)
                        if worker_status else 0,
                        'cpu_percent': worker_status.get('cpu_percent', 0)
                        if worker_status else 0,
                        'memory_mb': worker_status.get('memory_mb', 0)
                        if worker_status else 0,
                        'execution_mode': 'worker'
                    }
                else:
                    worker_info = {
                        'worker_id': None,
                        'execution_mode': 'main_process',
                        'status': 'running' if is_running else 'stopped',
                        'pid': os.getpid(),
                        'message': 'Running in main process (DAG server)'
                    }
            else:
                worker_info = {
                    'worker_id': None,
                    'execution_mode': 'single_process',
                    'status': 'running' if is_running else 'stopped',
                    'pid': os.getpid(),
                    'message': 'Single-process mode (worker pool disabled)'
                }

            # v2.2: the original source JSON (what the DAG was created from),
            # distinct from the enriched `details` runtime view. Prefer the
            # verbatim stored file; fall back to the in-memory config dict.
            try:
                source_json, source_config = \
                    self.dag_server.get_dag_source_config(dag_name)
            except Exception:  # noqa: BLE001 - never block the page
                source_json, source_config = None, None

            # v5.12.0: single source of truth for the details view-model. For a
            # DAG running in a worker subprocess, fetch the live snapshot the worker
            # built with the SAME builder (build_dag_view); otherwise build it from
            # the local (built) DAG. Either way the template gets an identical
            # structure, so the page looks the same wherever the DAG executes.
            # v5.14.0: served from a ~1s TTL cache to absorb refresh/viewer bursts.
            import time as _time
            _now = _time.monotonic()
            _cached = self._view_cache.get(dag_name)
            if _cached is not None and (_now - _cached[0]) < self._view_cache_ttl:
                view = _cached[1]
            else:
                view = None
                if worker_pool_enabled and \
                        self.worker_pool.get_dag_assignment(dag_name) is not None:
                    try:
                        snap = self.worker_pool.get_dag_state(dag_name)
                        if isinstance(snap, dict) and isinstance(snap.get('view'), dict):
                            view = snap['view']
                    except Exception:  # noqa: BLE001 - fall back to the local view
                        view = None
                if view is None:
                    view = build_dag_view(dag)
                self._view_cache[dag_name] = (_now, view)

            details = view['details']
            node_details = view['node_details']
            graph_data = view['graph_data']
            dag_stats = view['dag_stats']

            # v5.13.0: the throughput chart is now driven by true per-minute
            # ingest counts (dag_stats['ingest_buckets']) accumulated by the
            # running DAG's rate meters - see core/metrics/rate_meter.py. This
            # replaces the old page-load-sampled rate_history sparkline, which
            # only captured the moments a human opened this page and never worked
            # for DAGs running in a worker subprocess.

            return render(request, 'dag/details.html',
                          dag_name=dag_name,
                          details=details,
                          source_json=source_json,
                          source_config=source_config,
                          graph_data=graph_data,
                          dag_stats=dag_stats,
                          node_details=node_details,
                          worker_info=worker_info,
                          worker_pool_enabled=worker_pool_enabled,
                          is_running=is_running,
                          is_admin=self.user_registry.has_role(username,
                                                               'admin'))
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading DAG details', e)
            return redirect_to(request, 'dashboard')

    def dag_state(self, request: Request, dag_name: str):
        """DAG state view.

        v1.5.2: Handles lazy-initialized DAGs; fetches the actual state from
        the worker process when the DAG runs in a worker.
        """
        self.guards.login_required(request)
        if active_target_is_remote(request):
            flash(request, 'DAG state view is not yet available for remote '
                           'servers - switch to Local to inspect a DAG.', 'info')
            return redirect_to(request, 'dashboard')
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                flash(request, f'DAG {dag_name} not found', 'error')
                return redirect_to(request, 'dashboard')

            # Check if running in worker
            worker_id = None
            worker_state = None
            if self.worker_pool and self.worker_pool.is_running():
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    logger.info(f"Fetching DAG state for {dag_name} from "
                                f"Worker {worker_id}")
                    worker_state = self.worker_pool.get_dag_state(
                        dag_name, timeout=5.0)
                    if worker_state:
                        logger.info(
                            f"Received state from worker: "
                            f"{len(worker_state.get('node_states', []))} "
                            f"nodes")

            # v1.5.2: A DAG is lazy-initialized if components aren't built
            is_lazy_init = (
                (hasattr(dag, '_components_built') and
                 not dag._components_built) or
                (not dag.nodes)
            )

            node_states = []
            subscriber_states = {}

            if worker_state and worker_state.get('node_states'):
                logger.info(f"Using actual runtime state from Worker "
                            f"{worker_id}")
                node_states = worker_state['node_states']
                subscriber_states = worker_state.get('subscriber_states', {})
            elif is_lazy_init:
                logger.info(f"DAG {dag_name} is lazy-initialized, showing "
                            f"state from config")
                for node_cfg in dag.config.get('nodes', []):
                    node_states.append({
                        'name': node_cfg.get('name'),
                        'type': node_cfg.get('type', 'Unknown'),
                        'input': None,
                        'output': None,
                        'isdirty': False,
                        'errors': [],
                        'calculation_count': None,
                        'last_calculation': None,
                        'status_note': 'Running in Worker'
                        if worker_id is not None else 'Not Started'
                    })
            else:
                sorted_nodes = dag.topological_sort()
                logger.info(f"DAG {dag_name} has built components, "
                            f"{len(sorted_nodes)} nodes")
                for node in sorted_nodes:
                    calculation_count = getattr(
                        node, '_compute_count',
                        getattr(node, 'compute_count', None))
                    last_calculation = getattr(
                        node, '_last_compute',
                        getattr(node, 'last_compute', None))
                    node_states.append({
                        'name': node.name,
                        'type': type(node).__name__,
                        'input': node._input,
                        'output': node._output,
                        'isdirty': node._isdirty,
                        'errors': list(node._errors),
                        'calculation_count': calculation_count,
                        'last_calculation': last_calculation,
                        'status_note': None
                    })

            # Arrow nodes carry a pyarrow.RecordBatch as input/output, which the
            # state template would try to JSON-serialize and fail. Make every
            # node's input/output JSON-safe regardless of which path populated it.
            for ns in node_states:
                if isinstance(ns, dict):
                    if 'input' in ns:
                        ns['input'] = _state_value_safe(ns['input'])
                    if 'output' in ns:
                        ns['output'] = _state_value_safe(ns['output'])

            logger.info(f"Rendering state page with {len(node_states)} nodes")
            return render(request, 'dag/state.html',
                          dag_name=dag_name,
                          node_states=node_states,
                          subscriber_states=subscriber_states,
                          is_lazy_init=is_lazy_init,
                          worker_id=worker_id,
                          has_worker_state=worker_state is not None)
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading DAG state', e)
            return redirect_to(request, 'dashboard')
