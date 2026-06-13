"""
Worker Management Routes (FastAPI, v2.0.0)
==========================================

Admin-only monitoring and control of the multiprocess worker pool:
pool/worker status, manual worker restart, pool start/stop, DAG-to-worker
assignments, DAG migration, and the load summary.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.  Error policy: every exception is logged with the full stack
trace and surfaced as detailed JSON.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import AuthGuards, render

logger = logging.getLogger(__name__)


class WorkerRoutes:
    """Routes for worker pool management and monitoring."""

    def __init__(self, app: FastAPI, worker_pool_manager=None,
                 guards: AuthGuards = None):
        """
        Args:
            app: The FastAPI application.
            worker_pool_manager: WorkerPoolManager instance (optional; the
                routes report a clear error when the pool is disabled).
            guards: AuthGuards used to enforce admin access.
        """
        self.app = app
        self.worker_pool = worker_pool_manager
        self.guards = guards
        self._register_routes()

    def set_worker_pool(self, worker_pool_manager) -> None:
        """Set the worker pool manager after construction."""
        self.worker_pool = worker_pool_manager

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/admin/workers', self.workers_index, methods=['GET'],
            name='admin_workers', include_in_schema=False)
        add('/api/workers/status', self.api_workers_status, methods=['GET'],
            name='api_workers_status')
        add('/api/workers/assignments', self.api_dag_assignments,
            methods=['GET'], name='api_dag_assignments')
        add('/api/workers/load-summary', self.api_load_summary,
            methods=['GET'], name='api_load_summary')
        add('/api/workers/pool/start', self.api_pool_start, methods=['POST'],
            name='api_pool_start')
        add('/api/workers/pool/stop', self.api_pool_stop, methods=['POST'],
            name='api_pool_stop')
        add('/api/workers/dag/{dag_name}/migrate', self.api_dag_migrate,
            methods=['POST'], name='api_dag_migrate')
        add('/api/workers/{worker_id:int}', self.api_worker_details,
            methods=['GET'], name='api_worker_details')
        add('/api/workers/{worker_id:int}/restart', self.api_worker_restart,
            methods=['POST'], name='api_worker_restart')

    # ------------------------------------------------------------------ #

    def workers_index(self, request: Request):
        """Main workers monitoring page."""
        self.guards.admin_required(request)
        pool_status = {}
        if self.worker_pool:
            try:
                pool_status = self.worker_pool.get_pool_status()
            except Exception as e:  # noqa: BLE001 - shown + full-trace logged
                logger.error(f"Error getting pool status: {e}")
                logger.error(traceback.format_exc())
                pool_status = {'error': str(e)}
        return render(request, 'admin/workers.html',
                      pool_status=pool_status, is_admin=True)

    def api_workers_status(self, request: Request):
        """API: status of all workers."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized',
                                 'pool_enabled': False})
        try:
            return JSONResponse(self.worker_pool.get_pool_status())
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting workers status: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    def api_worker_details(self, request: Request, worker_id: int):
        """API: detailed status of one worker."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized'},
                                status_code=400)
        try:
            status = self.worker_pool.get_worker_status(worker_id)
            if status is None:
                return JSONResponse({'error': f'Worker {worker_id} not '
                                              f'found'}, status_code=404)
            return JSONResponse(status)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting worker {worker_id} status: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    def api_worker_restart(self, request: Request, worker_id: int):
        """API: manually restart a worker via the health monitor."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized'},
                                status_code=400)
        try:
            health = self.worker_pool.health_monitor.get_health_status(
                worker_id)
            if health is None:
                return JSONResponse({'error': f'Worker {worker_id} not '
                                              f'found'}, status_code=404)
            self.worker_pool._handle_worker_restart(worker_id, health)
            return JSONResponse({'success': True,
                                 'message': f'Worker {worker_id} restart '
                                            f'initiated'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error restarting worker {worker_id}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    def api_pool_start(self, request: Request):
        """API: start the worker pool."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not configured'},
                                status_code=400)
        try:
            if self.worker_pool.is_running():
                return JSONResponse({'success': False,
                                     'message': 'Worker pool already '
                                                'running'})
            self.worker_pool.start()
            return JSONResponse({'success': True,
                                 'message': 'Worker pool started'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error starting worker pool: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    def api_pool_stop(self, request: Request):
        """API: stop the worker pool."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized'},
                                status_code=400)
        try:
            if not self.worker_pool.is_running():
                return JSONResponse({'success': False,
                                     'message': 'Worker pool not running'})
            self.worker_pool.stop()
            return JSONResponse({'success': True,
                                 'message': 'Worker pool stopped'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error stopping worker pool: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    async def api_dag_migrate(self, request: Request, dag_name: str):
        """API: migrate a DAG to a different worker."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized'},
                                status_code=400)
        try:
            try:
                data = await request.json()
            except Exception:  # noqa: BLE001 - empty body is legal here
                data = {}
            to_worker = data.get('to_worker')
            if to_worker is None:
                return JSONResponse({'error': 'to_worker is required'},
                                    status_code=400)
            to_worker = int(to_worker)
            success = self.worker_pool.migrate_dag(dag_name, to_worker)
            if success:
                return JSONResponse({'success': True,
                                     'message': f'DAG {dag_name} migration '
                                                f'to worker {to_worker} '
                                                f'initiated'})
            return JSONResponse({'success': False,
                                 'message': f'Failed to migrate DAG '
                                            f'{dag_name}'}, status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error migrating DAG {dag_name}: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    def api_dag_assignments(self, request: Request):
        """API: all DAG-to-worker assignments."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized'},
                                status_code=400)
        try:
            assignments = self.worker_pool.get_all_dag_assignments()
            return JSONResponse({'assignments': assignments,
                                 'total_dags': len(assignments)})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting DAG assignments: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)

    def api_load_summary(self, request: Request):
        """API: worker load summary."""
        self.guards.admin_required(request)
        if not self.worker_pool:
            return JSONResponse({'error': 'Worker pool not initialized'},
                                status_code=400)
        try:
            return JSONResponse(self.worker_pool.get_load_summary())
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting load summary: {e}")
            logger.error(traceback.format_exc())
            return JSONResponse({'error': str(e)}, status_code=500)
