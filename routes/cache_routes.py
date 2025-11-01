"""Cache management routes module"""
import json
import logging
from io import BytesIO
from datetime import datetime
from flask import render_template, request, redirect, url_for, flash, jsonify, send_file, session

logger = logging.getLogger(__name__)


class CacheRoutes:
    """Handles cache management routes"""
    
    def __init__(self, app, redis_cache, user_registry, login_required, admin_required):
        self.app = app
        self.redis_cache = redis_cache
        self.user_registry = user_registry
        self.login_required = login_required
        self.admin_required = admin_required
        self._register_routes()
    
    def _register_routes(self):
        """Register all cache management routes"""
        # View routes
        self.app.add_url_rule('/cache', 'cache_management', 
                             self.login_required(self.cache_management))
        self.app.add_url_rule('/cache/create', 'cache_create_page', 
                             self.admin_required(self.cache_create_page), 
                             methods=['GET'])
        self.app.add_url_rule('/cache/edit/<path:key>', 'cache_edit_page', 
                             self.admin_required(self.cache_edit_page), 
                             methods=['GET'])
        self.app.add_url_rule('/cache/view/<path:key>', 'cache_view_page', 
                             self.login_required(self.cache_view_page), 
                             methods=['GET'])
        
        # API routes
        self.app.add_url_rule('/cache/api/query', 'cache_query', 
                             self.login_required(self.cache_query), 
                             methods=['POST'])
        self.app.add_url_rule('/cache/api/create', 'cache_create', 
                             self.admin_required(self.cache_create), 
                             methods=['POST'])
        self.app.add_url_rule('/cache/update/<path:key>', 'cache_update', 
                             self.admin_required(self.cache_update), 
                             methods=['POST'])
        self.app.add_url_rule('/cache/api/delete', 'cache_delete', 
                             self.admin_required(self.cache_delete), 
                             methods=['DELETE'])
        self.app.add_url_rule('/cache/api/ttl', 'cache_update_ttl', 
                             self.admin_required(self.cache_update_ttl), 
                             methods=['PUT'])
        self.app.add_url_rule('/cache/api/clear', 'cache_clear', 
                             self.admin_required(self.cache_clear), 
                             methods=['POST'])
        self.app.add_url_rule('/cache/api/download', 'cache_download', 
                             self.admin_required(self.cache_download), 
                             methods=['GET'])
        self.app.add_url_rule('/cache/api/stats', 'cache_stats', 
                             self.login_required(self.cache_stats), 
                             methods=['GET'])
        self.app.add_url_rule('/cache/api/dump/trigger', 'trigger_manual_dump', 
                             self.admin_required(self.trigger_manual_dump), 
                             methods=['POST'])
        self.app.add_url_rule('/cache/api/session-check', 'check_session', 
                             self.login_required(self.check_session), 
                             methods=['GET'])
    
    def cache_management(self):
        """Cache management page"""
        return render_template('cache_management.html',
                               is_admin=self.user_registry.has_role(session.get('username'), 'admin'))
    
    def cache_query(self):
        """Query cache with pattern"""
        try:
            data = request.get_json()
            pattern = data.get('pattern', '*')
            page = data.get('page', 1)
            per_page = data.get('per_page')

            # Get matching keys
            all_keys = self.redis_cache.keys(pattern)
            total_keys = len(all_keys)

            # Pagination
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

            # Get details for each key
            results = []
            for key in keys:
                try:
                    value = self.redis_cache.get(key)
                    ttl = self.redis_cache.ttl(key)
                    key_type = self.redis_cache.type(key)

                    # Format TTL display
                    if ttl == -1:
                        ttl_display = 'No expiry'
                    elif ttl == -2:
                        ttl_display = 'Key not found'
                    else:
                        hours = ttl // 3600
                        minutes = (ttl % 3600) // 60
                        seconds = ttl % 60
                        if hours > 0:
                            ttl_display = f'{hours}h {minutes}m {seconds}s'
                        elif minutes > 0:
                            ttl_display = f'{minutes}m {seconds}s'
                        else:
                            ttl_display = f'{seconds}s'

                    results.append({
                        'key': key,
                        'value': value[:100] + '...' if value and len(value) > 100 else value,
                        'full_value': value,
                        'type': key_type,
                        'ttl': ttl,
                        'ttl_display': ttl_display
                    })
                except Exception as e:
                    logger.error(f"Error getting key {key}: {str(e)}")

            return jsonify({
                'success': True,
                'results': results,
                'total': total_keys,
                'page': page,
                'per_page': per_page if per_page else total_keys,
                'total_pages': total_pages
            })
        except Exception as e:
            logger.error(f"Error querying cache: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def cache_create_page(self):
        """Cache entry create page"""
        return render_template('cache_create.html')
    
    def cache_edit_page(self, key):
        """Cache entry edit page"""
        try:
            # Check if key exists
            if not self.redis_cache.exists(key):
                flash(f'Cache key "{key}" not found', 'error')
                return redirect(url_for('cache_management'))

            # Get current value and TTL
            current_value = self.redis_cache.get(key)
            current_ttl = self.redis_cache.ttl(key)

            # Format TTL display
            if current_ttl > 0:
                hours = current_ttl // 3600
                minutes = (current_ttl % 3600) // 60
                seconds = current_ttl % 60

                if hours > 0:
                    ttl_display = f'{hours}h {minutes}m {seconds}s'
                elif minutes > 0:
                    ttl_display = f'{minutes}m {seconds}s'
                else:
                    ttl_display = f'{seconds}s'
            elif current_ttl == -1:
                ttl_display = 'No expiration'
            else:
                ttl_display = 'No expiration'

            return render_template('cache_edit.html',
                                   key=key,
                                   current_value=current_value,
                                   current_ttl=current_ttl,
                                   current_ttl_display=ttl_display)
        except Exception as e:
            logger.error(f"Error loading cache edit page: {str(e)}")
            flash(f'Error: {str(e)}', 'error')
            return redirect(url_for('cache_management'))
    
    def cache_view_page(self, key):
        """Cache entry view page"""
        try:
            # Check if key exists
            if not self.redis_cache.exists(key):
                flash(f'Cache key "{key}" not found', 'error')
                return redirect(url_for('cache_management'))

            # Get value, type, and TTL
            value = self.redis_cache.get(key)
            entry_type = self.redis_cache.type(key)
            ttl = self.redis_cache.ttl(key)

            # Format TTL display
            if ttl > 0:
                hours = ttl // 3600
                minutes = (ttl % 3600) // 60
                seconds = ttl % 60

                if hours > 0:
                    ttl_display = f'{hours}h {minutes}m {seconds}s'
                elif minutes > 0:
                    ttl_display = f'{minutes}m {seconds}s'
                else:
                    ttl_display = f'{seconds}s'
            elif ttl == -1:
                ttl_display = 'No expiration'
            else:
                ttl_display = 'No expiration'

            # Check if value is JSON
            is_json = False
            value_formatted = value
            try:
                parsed = json.loads(value)
                is_json = True
                value_formatted = json.dumps(parsed, indent=2)
            except:
                pass

            # Calculate value size
            value_size = len(value.encode('utf-8')) if value else 0

            return render_template('cache_view.html',
                                   key=key,
                                   value=value,
                                   value_formatted=value_formatted,
                                   value_size=value_size,
                                   entry_type=entry_type,
                                   ttl=ttl,
                                   ttl_display=ttl_display,
                                   is_json=is_json,
                                   is_admin=self.user_registry.has_role(session.get('username'), 'admin'))
        except Exception as e:
            logger.error(f"Error loading cache view page: {str(e)}")
            flash(f'Error: {str(e)}', 'error')
            return redirect(url_for('cache_management'))
    
    def cache_create(self):
        """Create a new cache entry"""
        try:
            # Handle both JSON (API) and form data (from create page)
            if request.is_json:
                data = request.get_json()
                key = data.get('key')
                value = data.get('value')
                ttl = data.get('ttl')
            else:
                key = request.form.get('key', '').strip()
                value = request.form.get('value', '').strip()
                ttl = request.form.get('ttl', '').strip()

            if not key:
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Key is required'}), 400
                else:
                    flash('Key is required', 'error')
                    return redirect(url_for('cache_create_page'))

            if value is None or value == '':
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Value is required'}), 400
                else:
                    flash('Value is required', 'error')
                    return redirect(url_for('cache_create_page'))

            # Set the value
            if ttl and ttl != '' and ttl != '-1':
                try:
                    ttl_int = int(ttl)
                    if ttl_int > 0:
                        self.redis_cache.set(key, value, ex=ttl_int)
                    else:
                        self.redis_cache.set(key, value)
                except ValueError:
                    if request.is_json:
                        return jsonify({'success': False, 'error': 'Invalid TTL value'}), 400
                    else:
                        flash('Invalid TTL value', 'error')
                        return redirect(url_for('cache_create_page'))
            else:
                self.redis_cache.set(key, value)

            logger.info(f"Cache entry created: {key}")

            if request.is_json:
                return jsonify({'success': True, 'message': f'Entry {key} created successfully'})
            else:
                flash(f'Cache entry "{key}" created successfully', 'success')
                return redirect(url_for('cache_management'))

        except Exception as e:
            logger.error(f"Error creating cache entry: {str(e)}")
            if request.is_json:
                return jsonify({'success': False, 'error': str(e)}), 500
            else:
                flash(f'Error creating cache entry: {str(e)}', 'error')
                return redirect(url_for('cache_create_page'))
    
    def cache_update(self, key):
        """Update an existing cache entry"""
        try:
            # Check if key exists
            if not self.redis_cache.exists(key):
                flash(f'Cache key "{key}" not found', 'error')
                return redirect(url_for('cache_management'))

            # Handle both JSON (API) and form data (from edit page)
            if request.is_json:
                data = request.get_json()
                value = data.get('value')
                ttl = data.get('ttl')
            else:
                value = request.form.get('value', '').strip()
                ttl = request.form.get('ttl', '').strip()

            if value is None or value == '':
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Value is required'}), 400
                else:
                    flash('Value is required', 'error')
                    return redirect(url_for('cache_edit_page', key=key))

            # Update the value (this preserves TTL if we're not changing it)
            if ttl and ttl != '':
                try:
                    ttl_int = int(ttl)
                    if ttl_int > 0:
                        # Set with new TTL
                        self.redis_cache.set(key, value, ex=ttl_int)
                        logger.info(f"Cache entry updated with new TTL: {key} (TTL: {ttl_int}s)")
                    elif ttl_int == -1:
                        # Set without TTL (persistent)
                        self.redis_cache.set(key, value)
                        logger.info(f"Cache entry updated (persistent): {key}")
                    else:
                        if request.is_json:
                            return jsonify({'success': False, 'error': 'Invalid TTL value'}), 400
                        else:
                            flash('Invalid TTL value', 'error')
                            return redirect(url_for('cache_edit_page', key=key))
                except ValueError:
                    if request.is_json:
                        return jsonify({'success': False, 'error': 'Invalid TTL value'}), 400
                    else:
                        flash('Invalid TTL value', 'error')
                        return redirect(url_for('cache_edit_page', key=key))
            else:
                # Update value but keep existing TTL
                current_ttl = self.redis_cache.ttl(key)
                if current_ttl > 0:
                    self.redis_cache.set(key, value, ex=current_ttl)
                else:
                    self.redis_cache.set(key, value)
                logger.info(f"Cache entry updated (keeping TTL): {key}")

            if request.is_json:
                return jsonify({'success': True, 'message': f'Entry {key} updated successfully'})
            else:
                flash(f'Cache entry "{key}" updated successfully', 'success')
                return redirect(url_for('cache_management'))

        except Exception as e:
            logger.error(f"Error updating cache entry: {str(e)}")
            if request.is_json:
                return jsonify({'success': False, 'error': str(e)}), 500
            else:
                flash(f'Error updating cache entry: {str(e)}', 'error')
                return redirect(url_for('cache_edit_page', key=key))
    
    def cache_delete(self):
        """Delete a cache entry"""
        try:
            data = request.get_json()
            key = data.get('key')

            if not key:
                return jsonify({'success': False, 'error': 'Key is required'}), 400

            deleted = self.redis_cache.delete(key)

            if deleted:
                logger.info(f"Cache entry deleted: {key}")
                return jsonify({'success': True, 'message': f'Entry {key} deleted successfully'})
            else:
                return jsonify({'success': False, 'error': 'Key not found'}), 404
        except Exception as e:
            logger.error(f"Error deleting cache entry: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def cache_update_ttl(self):
        """Update TTL for a cache entry"""
        try:
            data = request.get_json()
            key = data.get('key')
            ttl = data.get('ttl')

            if not key:
                return jsonify({'success': False, 'error': 'Key is required'}), 400

            if ttl is None:
                return jsonify({'success': False, 'error': 'TTL is required'}), 400

            # Check if key exists
            if not self.redis_cache.exists(key):
                return jsonify({'success': False, 'error': 'Key not found'}), 404

            ttl_int = int(ttl)
            if ttl_int > 0:
                self.redis_cache.expire(key, ttl_int)
                logger.info(f"TTL updated for key {key}: {ttl_int}s")
                return jsonify({'success': True, 'message': f'TTL for {key} updated to {ttl_int}s'})
            elif ttl_int == -1:
                self.redis_cache.persist(key)
                logger.info(f"TTL removed for key {key}")
                return jsonify({'success': True, 'message': f'TTL removed for {key}'})
            else:
                return jsonify({'success': False, 'error': 'TTL must be positive or -1 for no expiry'}), 400
        except Exception as e:
            logger.error(f"Error updating TTL: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def cache_clear(self):
        """Clear entire cache"""
        try:
            self.redis_cache.flushall()
            logger.warning("Cache cleared by admin")
            return jsonify({'success': True, 'message': 'Cache cleared successfully'})
        except Exception as e:
            logger.error(f"Error clearing cache: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def cache_download(self):
        """Download entire cache as JSON"""
        try:
            all_keys = self.redis_cache.keys('*')
            cache_data = {}

            for key in all_keys:
                try:
                    value = self.redis_cache.get(key)
                    ttl = self.redis_cache.ttl(key)
                    key_type = self.redis_cache.type(key)

                    cache_data[key] = {
                        'value': value,
                        'type': key_type,
                        'ttl': ttl if ttl > 0 else None
                    }
                except Exception as e:
                    logger.error(f"Error reading key {key}: {str(e)}")

            # Create JSON file in memory
            json_str = json.dumps(cache_data, indent=2)
            json_bytes = BytesIO(json_str.encode('utf-8'))

            logger.info(f"Cache downloaded: {len(all_keys)} keys")

            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'cache_export_{timestamp}.json'

            return send_file(
                json_bytes,
                mimetype='application/json',
                as_attachment=True,
                download_name=filename
            )
        except Exception as e:
            logger.error(f"Error downloading cache: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def cache_stats(self):
        """Get cache statistics"""
        try:
            all_keys = self.redis_cache.keys('*')
            total_keys = len(all_keys)

            # Count keys by type
            type_counts = {}
            keys_with_ttl = 0

            for key in all_keys:
                key_type = self.redis_cache.type(key)
                type_counts[key_type] = type_counts.get(key_type, 0) + 1

                ttl = self.redis_cache.ttl(key)
                if ttl > 0:
                    keys_with_ttl += 1

            # Get dump info
            dump_info = self.redis_cache.get_dump_info()

            return jsonify({
                'success': True,
                'stats': {
                    'total_keys': total_keys,
                    'keys_with_ttl': keys_with_ttl,
                    'keys_without_ttl': total_keys - keys_with_ttl,
                    'types': type_counts
                },
                'dump_info': dump_info
            })
        except Exception as e:
            logger.error(f"Error getting cache stats: {str(e)}")
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def trigger_manual_dump(self):
        """Manually trigger a cache dump"""
        try:
            logger.info(f"Manual dump triggered by user: {session.get('username', 'unknown')}")
            success = self.redis_cache.dump_to_file()
            if success:
                dump_info = self.redis_cache.get_dump_info()
                logger.info(f"Manual dump successful")
                return jsonify({
                    'success': True,
                    'message': 'Cache dumped successfully',
                    'dump_info': dump_info
                })
            else:
                logger.error("Manual dump failed")
                return jsonify({'success': False, 'error': 'Dump failed'}), 500
        except Exception as e:
            logger.error(f"Error triggering dump: {str(e)}", exc_info=True)
            return jsonify({'success': False, 'error': str(e)}), 500
    
    def check_session(self):
        """Check if user session is valid"""
        return jsonify({
            'success': True,
            'username': session.get('username'),
            'is_admin': self.user_registry.has_role(session.get('username'), 'admin'),
            'roles': session.get('roles', [])
        })
