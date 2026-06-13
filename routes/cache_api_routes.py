"""
Cache Management Routes - JSON API (FastAPI, v2.0.0)
====================================================

JSON endpoints backing the cache management console: query/pagination,
create/update (accepting both JSON and HTML-form posts, mirroring the v1.x
behaviour), delete, TTL management, clear, export download, statistics,
manual dump trigger, and session check.

Route names match the legacy Flask endpoint names so templates/JS work
unchanged.  Error policy: every exception is logged with the full stack
trace; API callers receive detailed JSON errors, form posts get a flash.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import traceback
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from routes.cache_routes import format_ttl
from web.fastapi_compat import AuthGuards, flash, redirect_to

logger = logging.getLogger(__name__)


def _is_json_request(request: Request) -> bool:
    """Mirror Flask's request.is_json check."""
    return 'application/json' in (request.headers.get('content-type') or '')


class CacheApiRoutes:
    """JSON API endpoints for cache management."""

    def __init__(self, app: FastAPI, redis_cache, user_registry,
                 guards: AuthGuards):
        self.app = app
        self.redis_cache = redis_cache
        self.user_registry = user_registry
        self.guards = guards
        self._register_routes()

    def _register_routes(self) -> None:
        add = self.app.add_api_route
        add('/cache/api/query', self.cache_query, methods=['POST'],
            name='cache_query')
        add('/cache/api/create', self.cache_create, methods=['POST'],
            name='cache_create')
        add('/cache/update/{key:path}', self.cache_update, methods=['POST'],
            name='cache_update')
        add('/cache/api/delete', self.cache_delete, methods=['DELETE'],
            name='cache_delete')
        add('/cache/api/ttl', self.cache_update_ttl, methods=['PUT'],
            name='cache_update_ttl')
        add('/cache/api/clear', self.cache_clear, methods=['POST'],
            name='cache_clear')
        add('/cache/api/download', self.cache_download, methods=['GET'],
            name='cache_download')
        add('/cache/api/stats', self.cache_stats, methods=['GET'],
            name='cache_stats')
        add('/cache/api/dump/trigger', self.trigger_manual_dump,
            methods=['POST'], name='trigger_manual_dump')
        add('/cache/api/session-check', self.check_session, methods=['GET'],
            name='check_session')

    # ------------------------------------------------------------------ #

    async def cache_query(self, request: Request):
        """Query cache keys by pattern with pagination."""
        self.guards.login_required(request)
        try:
            data = await request.json()
            pattern = data.get('pattern', '*')
            page = data.get('page', 1)
            per_page = data.get('per_page')

            all_keys = self.redis_cache.keys(pattern)
            total_keys = len(all_keys)

            if per_page and per_page != 'all':
                per_page = int(per_page)
                start = (page - 1) * per_page
                end = start + per_page
                keys = all_keys[start:end]
                total_pages = (total_keys + per_page - 1) // per_page
            else:
                keys = all_keys
                per_page = None
                total_pages = 1

            results = []
            for key in keys:
                try:
                    value = self.redis_cache.get(key)
                    ttl = self.redis_cache.ttl(key)
                    key_type = self.redis_cache.type(key)
                    if ttl == -1:
                        ttl_display = 'No expiry'
                    elif ttl == -2:
                        ttl_display = 'Key not found'
                    else:
                        ttl_display = format_ttl(ttl)
                    results.append({
                        'key': key,
                        'value': value[:100] + '...'
                        if value and len(value) > 100 else value,
                        'full_value': value,
                        'type': key_type,
                        'ttl': ttl,
                        'ttl_display': ttl_display
                    })
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Error getting key {key}: {str(e)}")
                    logger.error(traceback.format_exc())

            return JSONResponse({'success': True,
                                 'results': results,
                                 'total': total_keys,
                                 'page': page,
                                 'per_page': per_page if per_page
                                 else total_keys,
                                 'total_pages': total_pages})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error querying cache: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    async def cache_create(self, request: Request):
        """Create a cache entry (accepts JSON body or HTML form post)."""
        self.guards.admin_required(request)
        is_json = _is_json_request(request)
        try:
            if is_json:
                data = await request.json()
                key, value, ttl = data.get('key'), data.get('value'), \
                    data.get('ttl')
            else:
                form = await request.form()
                key = (form.get('key') or '').strip()
                value = (form.get('value') or '').strip()
                ttl = (form.get('ttl') or '').strip()

            if not key:
                if is_json:
                    return JSONResponse({'success': False,
                                         'error': 'Key is required'},
                                        status_code=400)
                return redirect_to(request, 'cache_create_page',
                                   flash_message='Key is required',
                                   flash_category='error')
            if value is None or value == '':
                if is_json:
                    return JSONResponse({'success': False,
                                         'error': 'Value is required'},
                                        status_code=400)
                return redirect_to(request, 'cache_create_page',
                                   flash_message='Value is required',
                                   flash_category='error')

            if ttl and ttl != '' and ttl != '-1':
                try:
                    ttl_int = int(ttl)
                except ValueError:
                    if is_json:
                        return JSONResponse({'success': False,
                                             'error': 'Invalid TTL value'},
                                            status_code=400)
                    return redirect_to(request, 'cache_create_page',
                                       flash_message='Invalid TTL value',
                                       flash_category='error')
                if ttl_int > 0:
                    self.redis_cache.set(key, value, ex=ttl_int)
                else:
                    self.redis_cache.set(key, value)
            else:
                self.redis_cache.set(key, value)

            logger.info(f"Cache entry created: {key}")
            if is_json:
                return JSONResponse({'success': True,
                                     'message': f'Entry {key} created '
                                                f'successfully'})
            return redirect_to(request, 'cache_management',
                               flash_message=f'Cache entry "{key}" created '
                                             f'successfully')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error creating cache entry: {str(e)}")
            logger.error(traceback.format_exc())
            if is_json:
                return JSONResponse({'success': False, 'error': str(e)},
                                    status_code=500)
            flash(request, f'Error creating cache entry: {str(e)}', 'error')
            return redirect_to(request, 'cache_create_page')

    async def cache_update(self, request: Request, key: str):
        """Update an existing cache entry (JSON or HTML form post)."""
        self.guards.admin_required(request)
        is_json = _is_json_request(request)
        try:
            if not self.redis_cache.exists(key):
                flash(request, f'Cache key "{key}" not found', 'error')
                return redirect_to(request, 'cache_management')

            if is_json:
                data = await request.json()
                value, ttl = data.get('value'), data.get('ttl')
            else:
                form = await request.form()
                value = (form.get('value') or '').strip()
                ttl = (form.get('ttl') or '').strip()

            if value is None or value == '':
                if is_json:
                    return JSONResponse({'success': False,
                                         'error': 'Value is required'},
                                        status_code=400)
                return redirect_to(request, 'cache_edit_page', key=key,
                                   flash_message='Value is required',
                                   flash_category='error')

            if ttl is not None and ttl != '':
                try:
                    ttl_int = int(ttl)
                except ValueError:
                    if is_json:
                        return JSONResponse({'success': False,
                                             'error': 'Invalid TTL value'},
                                            status_code=400)
                    return redirect_to(request, 'cache_edit_page', key=key,
                                       flash_message='Invalid TTL value',
                                       flash_category='error')
                if ttl_int > 0:
                    self.redis_cache.set(key, value, ex=ttl_int)
                    logger.info(f"Cache entry updated with new TTL: {key} "
                                f"(TTL: {ttl_int}s)")
                elif ttl_int == -1:
                    self.redis_cache.set(key, value)
                    logger.info(f"Cache entry updated (persistent): {key}")
                else:
                    if is_json:
                        return JSONResponse({'success': False,
                                             'error': 'Invalid TTL value'},
                                            status_code=400)
                    return redirect_to(request, 'cache_edit_page', key=key,
                                       flash_message='Invalid TTL value',
                                       flash_category='error')
            else:
                # Update value but keep existing TTL
                current_ttl = self.redis_cache.ttl(key)
                if current_ttl > 0:
                    self.redis_cache.set(key, value, ex=current_ttl)
                else:
                    self.redis_cache.set(key, value)
                logger.info(f"Cache entry updated (keeping TTL): {key}")

            if is_json:
                return JSONResponse({'success': True,
                                     'message': f'Entry {key} updated '
                                                f'successfully'})
            return redirect_to(request, 'cache_management',
                               flash_message=f'Cache entry "{key}" updated '
                                             f'successfully')
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error updating cache entry: {str(e)}")
            logger.error(traceback.format_exc())
            if is_json:
                return JSONResponse({'success': False, 'error': str(e)},
                                    status_code=500)
            flash(request, f'Error updating cache entry: {str(e)}', 'error')
            return redirect_to(request, 'cache_edit_page', key=key)

    async def cache_delete(self, request: Request):
        """Delete a cache entry."""
        self.guards.admin_required(request)
        try:
            data = await request.json()
            key = data.get('key')
            if not key:
                return JSONResponse({'success': False,
                                     'error': 'Key is required'},
                                    status_code=400)
            deleted = self.redis_cache.delete(key)
            if deleted:
                logger.info(f"Cache entry deleted: {key}")
                return JSONResponse({'success': True,
                                     'message': f'Entry {key} deleted '
                                                f'successfully'})
            return JSONResponse({'success': False, 'error': 'Key not found'},
                                status_code=404)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error deleting cache entry: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    async def cache_update_ttl(self, request: Request):
        """Update or remove the TTL for a cache entry."""
        self.guards.admin_required(request)
        try:
            data = await request.json()
            key, ttl = data.get('key'), data.get('ttl')
            if not key:
                return JSONResponse({'success': False,
                                     'error': 'Key is required'},
                                    status_code=400)
            if ttl is None:
                return JSONResponse({'success': False,
                                     'error': 'TTL is required'},
                                    status_code=400)
            if not self.redis_cache.exists(key):
                return JSONResponse({'success': False,
                                     'error': 'Key not found'},
                                    status_code=404)
            ttl_int = int(ttl)
            if ttl_int > 0:
                self.redis_cache.expire(key, ttl_int)
                logger.info(f"TTL updated for key {key}: {ttl_int}s")
                return JSONResponse({'success': True,
                                     'message': f'TTL for {key} updated to '
                                                f'{ttl_int}s'})
            if ttl_int == -1:
                self.redis_cache.persist(key)
                logger.info(f"TTL removed for key {key}")
                return JSONResponse({'success': True,
                                     'message': f'TTL removed for {key}'})
            return JSONResponse({'success': False,
                                 'error': 'TTL must be positive or -1 for '
                                          'no expiry'}, status_code=400)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error updating TTL: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def cache_clear(self, request: Request):
        """Clear the entire cache."""
        self.guards.admin_required(request)
        try:
            self.redis_cache.flushall()
            logger.warning("Cache cleared by admin")
            return JSONResponse({'success': True,
                                 'message': 'Cache cleared successfully'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error clearing cache: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def cache_download(self, request: Request):
        """Download the entire cache as a JSON attachment."""
        self.guards.admin_required(request)
        try:
            all_keys = self.redis_cache.keys('*')
            cache_data = {}
            for key in all_keys:
                try:
                    value = self.redis_cache.get(key)
                    ttl = self.redis_cache.ttl(key)
                    key_type = self.redis_cache.type(key)
                    cache_data[key] = {'value': value,
                                       'type': key_type,
                                       'ttl': ttl if ttl > 0 else None}
                except Exception as e:  # noqa: BLE001
                    logger.error(f"Error reading key {key}: {str(e)}")
                    logger.error(traceback.format_exc())

            json_str = json.dumps(cache_data, indent=2)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'cache_export_{timestamp}.json'
            logger.info(f"Cache downloaded: {len(all_keys)} keys")
            return Response(
                content=json_str,
                media_type='application/json',
                headers={'Content-Disposition':
                         f'attachment; filename="{filename}"'})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error downloading cache: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def cache_stats(self, request: Request):
        """Cache statistics: counts by type, TTL usage, dump info."""
        self.guards.login_required(request)
        try:
            all_keys = self.redis_cache.keys('*')
            total_keys = len(all_keys)
            type_counts = {}
            keys_with_ttl = 0
            for key in all_keys:
                key_type = self.redis_cache.type(key)
                type_counts[key_type] = type_counts.get(key_type, 0) + 1
                if self.redis_cache.ttl(key) > 0:
                    keys_with_ttl += 1
            dump_info = self.redis_cache.get_dump_info()
            return JSONResponse({'success': True,
                                 'stats': {
                                     'total_keys': total_keys,
                                     'keys_with_ttl': keys_with_ttl,
                                     'keys_without_ttl': total_keys
                                     - keys_with_ttl,
                                     'types': type_counts},
                                 'dump_info': dump_info})
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error getting cache stats: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def trigger_manual_dump(self, request: Request):
        """Manually trigger a cache dump to disk."""
        self.guards.admin_required(request)
        try:
            logger.info(f"Manual dump triggered by user: "
                        f"{request.session.get('username', 'unknown')}")
            success = self.redis_cache.dump_to_file()
            if success:
                dump_info = self.redis_cache.get_dump_info()
                logger.info("Manual dump successful")
                return JSONResponse({'success': True,
                                     'message': 'Cache dumped successfully',
                                     'dump_info': dump_info})
            logger.error("Manual dump failed")
            return JSONResponse({'success': False, 'error': 'Dump failed'},
                                status_code=500)
        except Exception as e:  # noqa: BLE001
            logger.error(f"Error triggering dump: {str(e)}")
            logger.error(traceback.format_exc())
            return JSONResponse({'success': False, 'error': str(e)},
                                status_code=500)

    def check_session(self, request: Request):
        """Report the logged-in user's session details."""
        self.guards.login_required(request)
        username = request.session.get('username')
        return JSONResponse({'success': True,
                             'username': username,
                             'is_admin': self.user_registry.has_role(
                                 username, 'admin'),
                             'roles': request.session.get('roles', [])})
