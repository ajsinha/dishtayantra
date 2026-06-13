"""
Cache Management Routes - Pages (FastAPI, v2.0.0)
=================================================

HTML pages for the in-memory Redis-clone cache: the management console plus
create / edit / view entry pages.  The JSON API endpoints live separately in
:mod:`routes.cache_api_routes` (architecture mandate: routes logically
grouped, files small).  :class:`CacheRoutes` composes both so the webapp
only constructs one object, exactly like in v1.x.

Route names match the legacy Flask endpoint names so templates work
unchanged.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging

from fastapi import FastAPI, Request

from web.fastapi_compat import (
    AuthGuards,
    flash_error_and_log,
    redirect_to,
    render,
)

logger = logging.getLogger(__name__)


def format_ttl(ttl: int) -> str:
    """Render a TTL (seconds, or -1/-2 sentinels) as a human string."""
    if ttl is None or ttl < 0:
        return 'No expiration'
    hours = ttl // 3600
    minutes = (ttl % 3600) // 60
    seconds = ttl % 60
    if hours > 0:
        return f'{hours}h {minutes}m {seconds}s'
    if minutes > 0:
        return f'{minutes}m {seconds}s'
    return f'{seconds}s'


class CachePageRoutes:
    """HTML pages for cache management."""

    def __init__(self, app: FastAPI, redis_cache, user_registry,
                 guards: AuthGuards):
        self.app = app
        self.redis_cache = redis_cache
        self.user_registry = user_registry
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/cache', self.cache_management, methods=['GET'],
            name='cache_management', include_in_schema=False)
        add('/cache/create', self.cache_create_page, methods=['GET'],
            name='cache_create_page', include_in_schema=False)
        add('/cache/edit/{key:path}', self.cache_edit_page, methods=['GET'],
            name='cache_edit_page', include_in_schema=False)
        add('/cache/view/{key:path}', self.cache_view_page, methods=['GET'],
            name='cache_view_page', include_in_schema=False)

    def cache_management(self, request: Request):
        """Cache management console."""
        username = self.guards.login_required(request)
        return render(request, 'cache/management.html',
                      is_admin=self.user_registry.has_role(username,
                                                           'admin'))

    def cache_create_page(self, request: Request):
        """Cache entry creation page (admin)."""
        self.guards.admin_required(request)
        return render(request, 'cache/create.html')

    def cache_edit_page(self, request: Request, key: str):
        """Cache entry edit page (admin)."""
        self.guards.admin_required(request)
        try:
            if not self.redis_cache.exists(key):
                return redirect_to(
                    request, 'cache_management',
                    flash_message=f'Cache key "{key}" not found',
                    flash_category='error')
            current_value = self.redis_cache.get(key)
            current_ttl = self.redis_cache.ttl(key)
            return render(request, 'cache/edit.html',
                          key=key,
                          current_value=current_value,
                          current_ttl=current_ttl,
                          current_ttl_display=format_ttl(current_ttl))
        except Exception as e:  # noqa: BLE001 - flashed + full-trace logged
            flash_error_and_log(request, 'Error loading cache edit page', e)
            return redirect_to(request, 'cache_management')

    def cache_view_page(self, request: Request, key: str):
        """Cache entry view page."""
        username = self.guards.login_required(request)
        try:
            if not self.redis_cache.exists(key):
                return redirect_to(
                    request, 'cache_management',
                    flash_message=f'Cache key "{key}" not found',
                    flash_category='error')

            value = self.redis_cache.get(key)
            entry_type = self.redis_cache.type(key)
            ttl = self.redis_cache.ttl(key)

            is_json = False
            value_formatted = value
            try:
                parsed = json.loads(value)
                is_json = True
                value_formatted = json.dumps(parsed, indent=2)
            except (TypeError, ValueError):
                # Value is not JSON - displayed verbatim. This is an
                # expected, non-error condition (not a swallowed failure).
                pass

            value_size = len(value.encode('utf-8')) if value else 0
            return render(request, 'cache/view.html',
                          key=key,
                          value=value,
                          value_formatted=value_formatted,
                          value_size=value_size,
                          entry_type=entry_type,
                          ttl=ttl,
                          ttl_display=format_ttl(ttl),
                          is_json=is_json,
                          is_admin=self.user_registry.has_role(username,
                                                               'admin'))
        except Exception as e:  # noqa: BLE001
            flash_error_and_log(request, 'Error loading cache view page', e)
            return redirect_to(request, 'cache_management')


class CacheRoutes:
    """Composition facade: pages + API, constructed like in v1.x."""

    def __init__(self, app: FastAPI, redis_cache, user_registry,
                 guards: AuthGuards):
        from routes.cache_api_routes import CacheApiRoutes
        self.pages = CachePageRoutes(app, redis_cache, user_registry, guards)
        self.api = CacheApiRoutes(app, redis_cache, user_registry, guards)
