"""
Authentication Routes (FastAPI, v2.0.0)
=======================================

Login, logout, and the root redirect.  Authentication is verified against
the database-backed :class:`core.user_registry.UserRegistry` (PBKDF2-SHA256
password hashes).  Session state is held in the signed cookie session
provided by Starlette's SessionMiddleware.

Route names intentionally match the legacy Flask endpoint names so all
templates keep working unchanged via the ``url_for`` compatibility global.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging

from fastapi import FastAPI, Request

from web.fastapi_compat import (
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


class AuthRoutes:
    """Handles authentication-related routes."""

    def __init__(self, app: FastAPI, user_registry):
        """
        Args:
            app: The FastAPI application.
            user_registry: Database-backed UserRegistry.
        """
        self.app = app
        self.user_registry = user_registry
        self._register_routes()

    def _register_routes(self) -> None:
        """Register all authentication routes (names match Flask)."""
        self.app.add_api_route('/', self.index, methods=['GET'],
                               name='index', include_in_schema=False)
        self.app.add_api_route('/login', self.login_page, methods=['GET'],
                               name='login', include_in_schema=False)
        self.app.add_api_route('/login', self.login_submit, methods=['POST'],
                               include_in_schema=False)
        self.app.add_api_route('/logout', self.logout, methods=['GET'],
                               name='logout', include_in_schema=False)

    def index(self, request: Request):
        """Root route (v2.1.0): authenticated users go straight to the
        dashboard; anonymous visitors see the public landing page."""
        if 'username' in request.session:
            return redirect_to(request, 'dashboard')
        return render(request, 'landing.html')

    def login_page(self, request: Request):
        """Render the login page."""
        return render(request, 'login.html')

    async def login_submit(self, request: Request):
        """Authenticate the posted credentials and establish the session."""
        try:
            form = await request.form()
            username = form.get('username')
            password = form.get('password')

            user_data = self.user_registry.authenticate(username, password)
            if user_data:
                request.session['username'] = username
                request.session['full_name'] = user_data.get('full_name',
                                                             username)
                request.session['roles'] = user_data.get('roles', ['user'])
                logger.info("User '%s' logged in", username)
                return redirect_to(request, 'dashboard',
                                   flash_message='Login successful')
            return redirect_to(request, 'login',
                               flash_message='Invalid username or password',
                               flash_category='error')
        except Exception as exc:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error during login', exc)
            return redirect_to(request, 'login')

    def logout(self, request: Request):
        """Clear the session and return to the public landing page."""
        username = request.session.get('username')
        request.session.clear()
        if username:
            logger.info("User '%s' logged out", username)
        # v5.16.2: land on the public landing page (the root route renders it
        # for anonymous visitors) rather than the login form.
        return redirect_to(request, 'index',
                           flash_message='Logged out successfully')
