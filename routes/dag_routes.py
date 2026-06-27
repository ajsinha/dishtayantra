"""
DAG Management Routes (FastAPI, v2.0.0)
=======================================

Admin-only DAG lifecycle operations: create (upload JSON), clone with a new
time window, delete, start/stop, suspend/resume, manual message publishing
to a subscriber's external broker, and subgraph light-up/light-down control.

Route names match the legacy Flask endpoint names so templates work
unchanged.  UI error policy (architecture mandate #5): failures are flashed
to the user AND logged with the full stack trace; API endpoints return
detailed JSON errors.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import re
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from core.service.client import get_service_client
from web.fastapi_compat import (
    AuthGuards,
    flash,
    flash_error_and_log,
    redirect_to,
    render,
)

from routes.dag_messaging_routes import DAGMessagingMixin

logger = logging.getLogger(__name__)


class DAGRoutes(DAGMessagingMixin):
    """Handles DAG management routes (admin only)."""

    def __init__(self, app: FastAPI, dag_server, guards: AuthGuards,
                 worker_pool=None):
        self.app = app
        self.dag_server = dag_server
        self.guards = guards
        self.worker_pool = worker_pool  # v1.5.2: worker pool for dispatch
        self._register_routes()

    def _register_routes(self) -> None:
        """Register all DAG management routes."""
        add = self.app.add_api_route
        add('/dags/reload', self.reload_dags, methods=['POST'],
            name='reload_dags', include_in_schema=False)
        add('/dags/collision/delete', self.delete_collision_file,
            methods=['POST'], name='delete_collision_file',
            include_in_schema=False)
        add('/dag/create', self.create_dag_page, methods=['GET'],
            name='create_dag', include_in_schema=False)
        add('/dag/create', self.create_dag_submit, methods=['POST'],
            include_in_schema=False)
        add('/dag/{dag_name}/clone', self.clone_dag_page, methods=['GET'],
            name='clone_dag', include_in_schema=False)
        add('/dag/{dag_name}/clone', self.clone_dag_submit, methods=['POST'],
            include_in_schema=False)
        add('/dag/{dag_name}/delete', self.delete_dag, methods=['POST'],
            name='delete_dag', include_in_schema=False)
        add('/dag/{dag_name}/start', self.start_dag, methods=['POST'],
            name='start_dag', include_in_schema=False)
        add('/dag/{dag_name}/stop', self.stop_dag, methods=['POST'],
            name='stop_dag', include_in_schema=False)
        add('/dag/{dag_name}/suspend', self.suspend_dag, methods=['POST'],
            name='suspend_dag', include_in_schema=False)
        add('/dag/{dag_name}/resume', self.resume_dag, methods=['POST'],
            name='resume_dag', include_in_schema=False)
        add('/dag/{dag_name}/subscriber/{subscriber_name}/publish',
            self.publish_message_page, methods=['GET'],
            name='publish_message', include_in_schema=False)
        add('/dag/{dag_name}/subscriber/{subscriber_name}/publish',
            self.publish_message_submit, methods=['POST'],
            include_in_schema=False)
        # Subgraph control routes
        add('/dag/{dag_name}/subgraph/control', self.subgraph_control,
            methods=['POST'], name='dag_subgraph_control',
            include_in_schema=False)
        add('/dag/{dag_name}/subgraph/status', self.subgraph_status,
            methods=['GET'], name='dag_subgraph_status',
            include_in_schema=False)

    # ------------------------------------------------------------------ #
    # Create / clone / delete
    # ------------------------------------------------------------------ #

    def create_dag_page(self, request: Request):
        """Render the DAG creation (JSON upload) page."""
        self.guards.admin_required(request)
        return render(request, 'dag/create.html')

    async def create_dag_submit(self, request: Request):
        """Create a new DAG from an uploaded JSON configuration file."""
        self.guards.admin_required(request)
        try:
            form = await request.form()
            upload = form.get('config_file')
            if upload is None:
                return redirect_to(request, 'create_dag',
                                   flash_message='No file provided',
                                   flash_category='error')
            if not upload.filename:
                return redirect_to(request, 'create_dag',
                                   flash_message='No file selected',
                                   flash_category='error')
            if not upload.filename.endswith('.json'):
                return redirect_to(request, 'create_dag',
                                   flash_message='File must be a JSON file',
                                   flash_category='error')

            content = await upload.read()
            config_data = json.loads(content)
            dag_name = self.dag_server.add_dag(config_data, upload.filename)
            return redirect_to(
                request, 'dashboard',
                flash_message=f'DAG {dag_name} created successfully')
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error creating DAG', e)
            return redirect_to(request, 'create_dag')

    def clone_dag_page(self, request: Request, dag_name: str):
        """Render the clone page with the original DAG's time window."""
        self.guards.admin_required(request)
        try:
            dag = self.dag_server.dags.get(dag_name)
            if not dag:
                return redirect_to(request, 'dashboard',
                                   flash_message=f'DAG {dag_name} not found',
                                   flash_category='error')
            return render(request, 'dag/clone.html',
                          dag_name=dag_name,
                          original_start=dag.start_time,
                          original_end=dag.end_time,
                          original_duration=dag.duration,
                          original_schedule=dag.schedule.to_dict()
                          if getattr(dag, 'schedule', None) else None)
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading clone page', e)
            return redirect_to(request, 'dashboard')

    async def clone_dag_submit(self, request: Request, dag_name: str):
        """Clone an existing DAG with an optional new time window."""
        self.guards.admin_required(request)
        try:
            form = await request.form()
            start_time = (form.get('start_time') or '').strip()
            duration = (form.get('duration') or '').strip()

            start_time = start_time or None
            duration = duration or None

            # Clean start_time - remove colons if present
            if start_time:
                start_time = start_time.replace(':', '')
                if len(start_time) != 4 or not start_time.isdigit():
                    return redirect_to(
                        request, 'clone_dag', dag_name=dag_name,
                        flash_message='Invalid start_time format. Use HHMM '
                                      'format (e.g., 0900)',
                        flash_category='error')
                hour, minute = int(start_time[:2]), int(start_time[2:])
                if hour > 23 or minute > 59:
                    return redirect_to(
                        request, 'clone_dag', dag_name=dag_name,
                        flash_message='Invalid start_time. Hour must be '
                                      '0-23, minute must be 0-59',
                        flash_category='error')

            # Validate duration format if provided
            if duration:
                if not re.match(r'^(\d+h)?(\d+m)?$', duration.lower()):
                    return redirect_to(
                        request, 'clone_dag', dag_name=dag_name,
                        flash_message='Invalid duration format. Use format '
                                      'like: 1h, 30m, or 1h30m',
                        flash_category='error')
                if 'h' not in duration.lower() and 'm' not in duration.lower():
                    return redirect_to(
                        request, 'clone_dag', dag_name=dag_name,
                        flash_message='Duration must include hours (h) or '
                                      'minutes (m)',
                        flash_category='error')

            cloned_name = self.dag_server.clone_dag(dag_name, start_time,
                                                    duration)

            if start_time and duration:
                message = (f'DAG cloned to {cloned_name} with '
                           f'start_time={start_time}, duration={duration}')
            elif start_time:
                message = (f'DAG cloned to {cloned_name} with '
                           f'start_time={start_time} '
                           f'(default duration: -5 minutes)')
            else:
                message = (f'DAG cloned to {cloned_name} with perpetual '
                           f'running (24/7)')
            return redirect_to(request, 'dashboard', flash_message=message)
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error cloning DAG', e)
            return redirect_to(request, 'clone_dag', dag_name=dag_name)

    def reload_dags(self, request: Request):
        """Re-scan the DAG storage prefix and reconcile the live registry.

        Adds DAGs whose JSON files were dropped into the prefix and removes
        DAGs whose source files were deleted. Admin-only; primary-only
        (the server rejects the mutation on a secondary instance).
        """
        self.guards.admin_required(request)
        try:
            result = self.dag_server.reload_from_storage(remove_missing=True)
            added = result.get('added', [])
            removed = result.get('removed', [])
            errors = result.get('errors', [])

            parts = []
            if added:
                parts.append(f"added {len(added)} ({', '.join(added)})")
            if removed:
                parts.append(f"removed {len(removed)} ({', '.join(removed)})")
            if not added and not removed:
                parts.append("no changes - registry already matches storage")

            if errors:
                # Partial success: surface the count, log the details.
                flash(request,
                      f"Reloaded DAGs: {'; '.join(parts)}; "
                      f"{len(errors)} error(s) - see logs.", 'warning')
            else:
                flash(request, f"Reloaded DAGs: {'; '.join(parts)}.",
                      'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error reloading DAGs from storage', e)
        return redirect_to(request, 'dashboard')

    async def delete_collision_file(self, request: Request):
        """Delete an offending DAG JSON file that caused a name collision.

        Driven by the dashboard's red collision banner. Admin-only. For
        safety, the path MUST match a currently-recorded collision's
        ``offending_file`` - arbitrary paths are refused so this cannot be
        used to delete unrelated storage objects.
        """
        self.guards.admin_required(request)
        try:
            form = await request.form()
            offending = (form.get('offending_file') or '').strip()
            if not offending:
                flash(request, 'No file specified for deletion.', 'warning')
                return redirect_to(request, 'dashboard')

            # Validate against the recorded collisions (allow-list).
            recorded = {c['offending_file'] for c in
                        (getattr(self.dag_server, 'name_collisions', None) or [])}
            if offending not in recorded:
                flash(request,
                      'Refused: that file is not a recorded collision.',
                      'warning')
                return redirect_to(request, 'dashboard')

            # Delete the storage object, then clear the recorded collision.
            self.dag_server._storage.delete(offending)
            self.dag_server.clear_name_collision(offending)
            logger.warning("Deleted colliding DAG file '%s' via dashboard",
                           offending)
            flash(request,
                  f"Deleted colliding file '{offending}'. "
                  f"Click Reload to pick up any remaining changes.",
                  'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request,
                                'Error deleting colliding DAG file', e)
        return redirect_to(request, 'dashboard')

    def delete_dag(self, request: Request, dag_name: str):
        """Delete a DAG."""
        self.guards.admin_required(request)
        try:
            self.dag_server.delete(dag_name)
            flash(request, f'DAG {dag_name} deleted', 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error deleting DAG', e)
        return redirect_to(request, 'dashboard')

    # ------------------------------------------------------------------ #
    # Lifecycle: start / stop / suspend / resume
    # ------------------------------------------------------------------ #

    def start_dag(self, request: Request, dag_name: str):
        """Start a DAG (worker pool dispatch handled by the server)."""
        self.guards.admin_required(request)
        try:
            get_service_client(request).start_dag(dag_name)
            if self.worker_pool and self.worker_pool.is_running():
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
                if worker_id is not None:
                    flash(request, f'DAG {dag_name} started on Worker '
                                   f'{worker_id}', 'success')
                else:
                    flash(request, f'DAG {dag_name} started', 'success')
            else:
                flash(request, f'DAG {dag_name} started', 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error starting DAG', e)
        return redirect_to(request, 'dashboard')

    def stop_dag(self, request: Request, dag_name: str):
        """Stop a DAG."""
        self.guards.admin_required(request)
        try:
            worker_id = None
            if self.worker_pool and self.worker_pool.is_running():
                worker_id = self.worker_pool.get_dag_assignment(dag_name)
            get_service_client(request).stop_dag(dag_name)
            if worker_id is not None:
                flash(request, f'DAG {dag_name} stopped (was on Worker '
                               f'{worker_id})', 'success')
            else:
                flash(request, f'DAG {dag_name} stopped', 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error stopping DAG', e)
        return redirect_to(request, 'dashboard')

    def suspend_dag(self, request: Request, dag_name: str):
        """Suspend a DAG."""
        self.guards.admin_required(request)
        try:
            get_service_client(request).suspend_dag(dag_name)
            flash(request, f'DAG {dag_name} suspended', 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error suspending DAG', e)
        return redirect_to(request, 'dashboard')

    def resume_dag(self, request: Request, dag_name: str):
        """Resume a DAG."""
        self.guards.admin_required(request)
        try:
            get_service_client(request).resume_dag(dag_name)
            flash(request, f'DAG {dag_name} resumed', 'success')
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error resuming DAG', e)
        return redirect_to(request, 'dashboard')
