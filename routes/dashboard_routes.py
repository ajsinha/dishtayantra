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

from fastapi import FastAPI, Request

from web.fastapi_compat import (
    AuthGuards,
    flash,
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


class DashboardRoutes:
    """Handles dashboard-related routes."""

    def __init__(self, app: FastAPI, dag_server, user_registry,
                 guards: AuthGuards, worker_pool=None):
        self.app = app
        self.dag_server = dag_server
        self.user_registry = user_registry
        self.guards = guards
        self.worker_pool = worker_pool  # v1.5.2: worker pool for assignments
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
            server_status = self.dag_server.get_server_status()
            dags = self.dag_server.list_dags()
            return render(request, 'dashboard.html',
                          dags=dags,
                          is_admin=self.user_registry.has_role(username,
                                                               'admin'),
                          server_status=server_status)
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error loading dashboard', e)
            return render(request, 'dashboard.html', dags=[],
                          is_admin=False, server_status={})

    def dag_details(self, request: Request, dag_name: str):
        """DAG details view."""
        username = self.guards.login_required(request)
        try:
            details = self.dag_server.details(dag_name)

            # Get topological order
            dag = self.dag_server.dags[dag_name]
            sorted_nodes = dag.topological_sort()

            # Build dependency info with additional details
            node_details = []
            for node in sorted_nodes:
                dependencies = [edge.from_node.name
                                for edge in node._incoming_edges]
                last_calculation = getattr(node, '_last_compute', None)
                errors = list(getattr(node, '_errors', []))
                node_details.append({
                    'name': node.name,
                    'type': node.__class__.__name__,
                    'dependencies': dependencies,
                    'config': node.config,
                    'last_calculation': last_calculation,
                    'errors': errors
                })

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

            # v2.2: compact node/edge graph for the visual DAG view.
            # Node-type -> semantic role used for colouring in the SVG.
            def _role(type_name):
                t = (type_name or '').lower()
                if 'subscription' in t:
                    return 'source'
                if 'metronome' in t:
                    return 'source'
                if 'publishersink' in t or 'sink' in t:
                    return 'sink'
                if 'subgraph' in t:
                    return 'subgraph'
                return 'compute'

            graph_nodes = []
            graph_edges = []
            for node in sorted_nodes:
                graph_nodes.append({
                    'id': node.name,
                    'label': node.name,
                    'type': node.__class__.__name__,
                    'role': _role(node.__class__.__name__),
                })
                for edge in node._incoming_edges:
                    edge_obj = {
                        'from': edge.from_node.name,
                        'to': node.name,
                    }
                    # Surface a transformer / pname label on the edge if present.
                    transformer = getattr(edge, 'data_transformer', None)
                    if transformer is not None:
                        tname = getattr(transformer, 'name', None) \
                            or transformer.__class__.__name__
                        edge_obj['label'] = tname
                    pname = getattr(edge, 'pname', None)
                    if pname:
                        edge_obj['pname'] = pname
                    graph_edges.append(edge_obj)
            graph_data = {'nodes': graph_nodes, 'edges': graph_edges}

            return render(request, 'dag/details.html',
                          dag_name=dag_name,
                          details=details,
                          source_json=source_json,
                          source_config=source_config,
                          graph_data=graph_data,
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
