"""
User Management Routes (FastAPI, v2.0.0)
========================================

Admin-only pages and JSON API for managing users, roles, and (via the
database-backed registry) credentials.  In v2.0.0 the registry persists to
SQLite/PostgreSQL through the DAO layer; passwords are stored as
PBKDF2-SHA256 hashes.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.  Protections preserved from v1.x: the 'admin' account cannot be
edited or deleted, users cannot delete themselves, and the last admin can
never lose the admin role.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from web.fastapi_compat import (
    AuthGuards,
    flash,
    flash_error_and_log,
    redirect_to,
    render,
)
from core.audit_log import audit, client_ip

logger = logging.getLogger(__name__)


def _is_json_request(request: Request) -> bool:
    """Mirror Flask's request.is_json check."""
    return 'application/json' in (request.headers.get('content-type') or '')


def _roles_from_form(form) -> list:
    """Translate the role checkboxes of the create/edit forms to a list."""
    roles = []
    if form.get('role_user'):
        roles.append('user')
    if form.get('role_operator'):
        roles.append('operator')
    if form.get('role_admin'):
        roles.append('admin')
    return roles


class UserRoutes:
    """Handles user management routes (admin only)."""

    def __init__(self, app: FastAPI, user_registry, guards: AuthGuards):
        self.app = app
        self.user_registry = user_registry
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/users', self.user_management, methods=['GET'],
            name='user_management', include_in_schema=False)
        add('/users/create', self.user_create_page, methods=['GET'],
            name='user_create_page', include_in_schema=False)
        add('/users/create', self.user_create_submit, methods=['POST'],
            include_in_schema=False)
        add('/users/edit/{username}', self.user_edit_page, methods=['GET'],
            name='user_edit_page', include_in_schema=False)
        add('/users/api/list', self.users_list, methods=['GET'],
            name='users_list')
        add('/users/api/reload', self.users_reload, methods=['POST'],
            name='users_reload')
        add('/users/api/create', self.users_create, methods=['POST'],
            name='users_create')
        add('/users/api/update', self.users_update, methods=['PUT', 'POST'],
            name='users_update')
        add('/users/api/delete', self.users_delete, methods=['DELETE'],
            name='users_delete')
        add('/users/api/stats', self.users_stats, methods=['GET'],
            name='users_stats')

    # ------------------------------------------------------------------ #
    # Pages
    # ------------------------------------------------------------------ #

    def user_management(self, request: Request):
        """User management console."""
        self.guards.admin_required(request)
        return render(request, 'user/management.html')

    def user_create_page(self, request: Request):
        """User creation form."""
        self.guards.admin_required(request)
        return render(request, 'user/create.html')

    async def user_create_submit(self, request: Request):
        """Handle the user creation form post."""
        admin_user = self.guards.admin_required(request)
        try:
            form = await request.form()
            username = (form.get('username') or '').strip()
            password = (form.get('password') or '').strip()
            full_name = (form.get('full_name') or '').strip()
            roles = _roles_from_form(form)

            if not username:
                return redirect_to(request, 'user_create_page',
                                   flash_message='Username is required',
                                   flash_category='error')
            if not password:
                return redirect_to(request, 'user_create_page',
                                   flash_message='Password is required',
                                   flash_category='error')
            if len(password) < 6:
                return redirect_to(
                    request, 'user_create_page',
                    flash_message='Password must be at least 6 characters '
                                  'long',
                    flash_category='error')
            if not roles:
                return redirect_to(
                    request, 'user_create_page',
                    flash_message='At least one role is required',
                    flash_category='error')
            if self.user_registry.user_exists(username):
                return redirect_to(
                    request, 'user_create_page',
                    flash_message=f'User {username} already exists',
                    flash_category='error')

            user_data = {'password': password,
                         'full_name': full_name or username,
                         'roles': roles}
            success = self.user_registry.create_user(username, user_data,
                                                     admin_user)
            if success:
                logger.info(f"User {username} created by {admin_user}")
                audit('user.create', actor=admin_user, target=username, source_ip=client_ip(request))
                return redirect_to(request, 'user_management',
                                   flash_message=f'User {username} created '
                                                 f'successfully')
            return redirect_to(request, 'user_create_page',
                               flash_message='Failed to create user',
                               flash_category='error')
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error creating user', e)
            return redirect_to(request, 'user_create_page')

    def user_edit_page(self, request: Request, username: str):
        """User edit form (the 'admin' account is protected)."""
        self.guards.admin_required(request)
        try:
            if not self.user_registry.user_exists(username):
                return redirect_to(request, 'user_management',
                                   flash_message=f'User {username} not '
                                                 f'found',
                                   flash_category='error')
            if username.lower() == 'admin':
                return redirect_to(
                    request, 'user_management',
                    flash_message='Cannot edit the protected admin user',
                    flash_category='error')
            user_data = self.user_registry.get_user(username,
                                                    include_password=False)
            return render(request, 'user/edit.html', user_data=user_data)
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading user edit page', e)
            return redirect_to(request, 'user_management')

    # ------------------------------------------------------------------ #
    # JSON API
    # ------------------------------------------------------------------ #

    def users_list(self, request: Request):
        """List all users (passwords never included)."""
        self.guards.admin_required(request)
        try:
            users = self.user_registry.list_all_users(
                include_passwords=False)
            users_list = []
            for username, user_data in users.items():
                user_info = user_data.copy()
                user_info['username'] = username
                users_list.append(user_info)
            return JSONResponse({'success': True, 'users': users_list,
                                 'total': len(users_list)})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error listing users: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def users_reload(self, request: Request):
        """Force refresh of the registry (DB-backed no-op kept for the UI)."""
        admin_user = self.guards.admin_required(request)
        try:
            logger.info(f"User registry reload triggered by: {admin_user}")
            success = self.user_registry.force_reload()
            if success:
                user_count = self.user_registry.get_user_count()
                logger.info(f"User registry reloaded successfully. Total "
                            f"users: {user_count}")
                return JSONResponse({'success': True,
                                     'message': f'User registry reloaded. '
                                                f'Total users: {user_count}'})
            return JSONResponse({'success': False,
                                 'error': 'Failed to reload user registry'},
                                status_code=500)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error reloading user registry: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    async def users_create(self, request: Request):
        """Create a new user (JSON API)."""
        admin_user = self.guards.admin_required(request)
        try:
            data = await request.json()
            username = (data.get('username') or '').strip()
            password = (data.get('password') or '').strip()
            full_name = (data.get('full_name') or '').strip()
            roles = data.get('roles', [])

            if not username:
                return JSONResponse({'success': False,
                                     'error': 'Username is required'},
                                    status_code=400)
            if not password:
                return JSONResponse({'success': False,
                                     'error': 'Password is required'},
                                    status_code=400)
            if not roles:
                return JSONResponse({'success': False,
                                     'error': 'At least one role is '
                                              'required'}, status_code=400)
            if self.user_registry.user_exists(username):
                return JSONResponse({'success': False,
                                     'error': f'User {username} already '
                                              f'exists'}, status_code=400)

            user_data = {'password': password,
                         'full_name': full_name or username,
                         'roles': roles or ['user']}
            success = self.user_registry.create_user(username, user_data,
                                                     admin_user)
            if success:
                logger.info(f"User {username} created by {admin_user}")
                audit('user.create', actor=admin_user, target=username, source_ip=client_ip(request))
                return JSONResponse({'success': True,
                                     'message': f'User {username} created '
                                                f'successfully'})
            return JSONResponse({'success': False,
                                 'error': 'Failed to create user'},
                                status_code=500)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error creating user: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    async def users_update(self, request: Request):
        """Update an existing user (JSON or HTML-form post)."""
        admin_user = self.guards.admin_required(request)
        is_json = _is_json_request(request)
        username = ''
        try:
            if is_json:
                data = await request.json()
            else:
                form = await request.form()
                data = dict(form)
                data['roles'] = _roles_from_form(form)

            username = (data.get('username') or '').strip()
            password = (data.get('password') or '').strip()
            full_name = (data.get('full_name') or '').strip()
            roles = data.get('roles', [])

            if not username:
                if is_json:
                    return JSONResponse({'success': False,
                                         'error': 'Username is required'},
                                        status_code=400)
                flash(request, 'Username is required', 'error')
                return redirect_to(request, 'user_management')

            if username.lower() == 'admin':
                if is_json:
                    return JSONResponse(
                        {'success': False,
                         'error': 'Cannot edit the protected admin user'},
                        status_code=403)
                flash(request, 'Cannot edit the protected admin user',
                      'error')
                return redirect_to(request, 'user_management')

            if not self.user_registry.user_exists(username):
                if is_json:
                    return JSONResponse({'success': False,
                                         'error': f'User {username} not '
                                                  f'found'},
                                        status_code=404)
                flash(request, f'User {username} not found', 'error')
                return redirect_to(request, 'user_management')

            user_data = {}
            if password:
                user_data['password'] = password
            if full_name:
                user_data['full_name'] = full_name
            if roles is not None:
                if len(roles) == 0:
                    if is_json:
                        return JSONResponse(
                            {'success': False,
                             'error': 'At least one role is required'},
                            status_code=400)
                    flash(request, 'At least one role is required', 'error')
                    return redirect_to(request, 'user_edit_page',
                                       username=username)
                user_data['roles'] = roles

            # Prevent removing admin role from the last admin
            if 'roles' in user_data and 'admin' not in user_data['roles']:
                if self.user_registry.has_role(username, 'admin'):
                    all_users = self.user_registry.list_all_users()
                    admin_count = sum(
                        1 for u in all_users.values()
                        if 'admin' in u.get('roles', []))
                    if admin_count <= 1:
                        message = ('Cannot remove admin role from the last '
                                   'admin user')
                        if is_json:
                            return JSONResponse({'success': False,
                                                 'error': message},
                                                status_code=400)
                        flash(request, message, 'error')
                        return redirect_to(request, 'user_edit_page',
                                           username=username)

            success = self.user_registry.modify_user(username, user_data,
                                                     admin_user)
            if success:
                logger.info(f"User {username} updated by {admin_user}")
                audit('user.update', actor=admin_user, target=username, source_ip=client_ip(request))
                if is_json:
                    return JSONResponse({'success': True,
                                         'message': f'User {username} '
                                                    f'updated successfully'})
                return redirect_to(request, 'user_management',
                                   flash_message=f'User {username} updated '
                                                 f'successfully')
            if is_json:
                return JSONResponse({'success': False,
                                     'error': 'Failed to update user'},
                                    status_code=500)
            flash(request, 'Failed to update user', 'error')
            return redirect_to(request, 'user_edit_page', username=username)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error updating user: {str(e)}")
            logger.error(traceback.format_exc())
            if is_json:
                return JSONResponse({'success': False, 'error': str(e)},
                                    status_code=500)
            flash(request, f'Error updating user: {str(e)}', 'error')
            return redirect_to(request, 'user_management')

    async def users_delete(self, request: Request):
        """Delete a user (self-deletion and admin account are blocked)."""
        admin_user = self.guards.admin_required(request)
        try:
            data = await request.json()
            username = (data.get('username') or '').strip()

            if not username:
                return JSONResponse({'success': False,
                                     'error': 'Username is required'},
                                    status_code=400)
            if username.lower() == 'admin':
                return JSONResponse(
                    {'success': False,
                     'error': 'Cannot delete the protected admin user'},
                    status_code=403)
            if username == admin_user:
                return JSONResponse(
                    {'success': False,
                     'error': 'Cannot delete your own account'},
                    status_code=400)
            if not self.user_registry.user_exists(username):
                return JSONResponse({'success': False,
                                     'error': f'User {username} not found'},
                                    status_code=404)

            success = self.user_registry.delete_user(username, admin_user)
            if success:
                logger.info(f"User {username} deleted by {admin_user}")
                audit('user.delete', actor=admin_user, target=username, source_ip=client_ip(request))
                return JSONResponse({'success': True,
                                     'message': f'User {username} deleted '
                                                f'successfully'})
            return JSONResponse(
                {'success': False,
                 'error': 'Failed to delete user. Cannot delete the last '
                          'admin user.'}, status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error deleting user: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def users_stats(self, request: Request):
        """User statistics: total users and counts per role."""
        self.guards.admin_required(request)
        try:
            all_users = self.user_registry.list_all_users()
            role_counts = {}
            for user_data in all_users.values():
                for role in user_data.get('roles', []):
                    role_counts[role] = role_counts.get(role, 0) + 1
            return JSONResponse({'success': True,
                                 'stats': {'total_users': len(all_users),
                                           'role_counts': role_counts}})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting user stats: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)
