import json
import logging
import os
from io import BytesIO

from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify, send_file
from functools import wraps
from core.dag.dag_server import DAGComputeServer
from core.pubsub.inmemory_redisclone import InMemoryRedisClone
from core.user_registry import UserRegistry
from core.properties_configurator import PropertiesConfigurator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Load configuration
props = None
try:
    props = PropertiesConfigurator(['config/application.properties'])
    app.secret_key = props.get('flask.secret_key', os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me'))
except Exception as e:
    logger.warning(f"Could not load properties: {e}, using defaults")
    app.secret_key = os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me')

# Initialize DAG server
DAG_CONFIG_FOLDER = os.environ.get('DAG_CONFIG_FOLDER', './config/dags')
ZOOKEEPER_HOSTS = os.environ.get('ZOOKEEPER_HOSTS', 'localhost:2181')
USERS_FILE = os.environ.get('USERS_FILE', './config/users.json')

# Get user registry reload interval from properties (default: 600 seconds = 10 minutes)
USER_RELOAD_INTERVAL = 600
if props:
    USER_RELOAD_INTERVAL = props.get_int('user.registry.reload_interval', 600)

# Ensure directories exist
os.makedirs(DAG_CONFIG_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(USERS_FILE), exist_ok=True)
os.makedirs('./logs', exist_ok=True)

# Initialize UserRegistry
user_registry = UserRegistry(users_file=USERS_FILE, reload_interval=USER_RELOAD_INTERVAL)

# Initialize DAG server
dag_server = DAGComputeServer(DAG_CONFIG_FOLDER, ZOOKEEPER_HOSTS)

# Initialize shared InMemoryRedis instance
redis_cache = InMemoryRedisClone()


# Authentication decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)

    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        # Check if user has admin role using UserRegistry
        if not user_registry.has_role(session.get('username'), 'admin'):
            flash('Admin access required', 'error')
            return redirect(url_for('dashboard'))
        return f(*args, **kwargs)

    return decorated_function


@app.route('/')
def index():
    if 'username' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        # Authenticate using UserRegistry
        user_data = user_registry.authenticate(username, password)
        if user_data:
            session['username'] = username
            session['full_name'] = user_data.get('full_name', username)
            session['roles'] = user_data.get('roles', ['user'])
            flash('Login successful', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Invalid username or password', 'error')

    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out successfully', 'success')
    return redirect(url_for('login'))


@app.route('/dashboard')
@login_required
def dashboard():
    try:
        server_status = dag_server.get_server_status()
        dags = dag_server.list_dags()
        return render_template('dashboard.html',
                               dags=dags,
                               is_admin=user_registry.has_role(session.get('username'), 'admin'),
                               server_status=server_status)
    except Exception as e:
        logger.error(f"Error loading dashboard: {str(e)}")
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dashboard.html', dags=[], is_admin=False, server_status={})


@app.route('/dag/<dag_name>/details')
@login_required
def dag_details(dag_name):
    try:
        details = dag_server.details(dag_name)

        # Get topological order
        dag = dag_server.dags[dag_name]
        sorted_nodes = dag.topological_sort()

        # Build dependency info with additional details
        node_details = []
        for node in sorted_nodes:
            dependencies = [edge.from_node.name for edge in node._incoming_edges]

            # Get last calculation time
            last_calculation = None
            if hasattr(node, '_last_compute'):
                last_calculation = node._last_compute

            # Get errors
            errors = []
            if hasattr(node, '_errors'):
                errors = list(node._errors)

            node_details.append({
                'name': node.name,
                'type': node.__class__.__name__,
                'dependencies': dependencies,
                'config': node.config,
                'last_calculation': last_calculation,
                'errors': errors
            })

        return render_template('dag_details.html',
                               dag_name=dag_name,
                               details=details,
                               node_details=node_details,
                               is_admin=user_registry.has_role(session.get('username'), 'admin'))
    except Exception as e:
        logger.error(f"Error loading DAG details: {str(e)}")
        flash(f'Error loading DAG details: {str(e)}', 'error')
        return redirect(url_for('dashboard'))


@app.route('/dag/<dag_name>/state')
@login_required
def dag_state(dag_name):
    try:
        details = dag_server.details(dag_name)

        # Get topological order
        dag = dag_server.dags[dag_name]
        sorted_nodes = dag.topological_sort()

        # Build state info with calculation details
        node_states = []
        for node in sorted_nodes:
            # Get calculation count
            calculation_count = None
            if hasattr(node, '_compute_count'):
                calculation_count = node._compute_count
            elif hasattr(node, 'compute_count'):
                calculation_count = node.compute_count

            # Get last calculation time
            last_calculation = None
            if hasattr(node, '_last_compute'):
                last_calculation = node._last_compute
            elif hasattr(node, 'last_compute'):
                last_calculation = node.last_compute

            node_states.append({
                'name': node.name,
                'input': node._input,
                'output': node._output,
                'isdirty': node._isdirty,
                'errors': list(node._errors),
                'calculation_count': calculation_count,
                'last_calculation': last_calculation
            })

        return render_template('dag_state.html',
                               dag_name=dag_name,
                               node_states=node_states)
    except Exception as e:
        logger.error(f"Error loading DAG state: {str(e)}")
        flash(f'Error loading DAG state: {str(e)}', 'error')
        return redirect(url_for('dashboard'))


@app.route('/dag/create', methods=['GET', 'POST'])
@admin_required
def create_dag():
    if request.method == 'GET':
        return render_template('create_dag.html')

    try:
        if 'config_file' not in request.files:
            flash('No file provided', 'error')
            return redirect(url_for('create_dag'))

        file = request.files['config_file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(url_for('create_dag'))

        if not file.filename.endswith('.json'):
            flash('File must be a JSON file', 'error')
            return redirect(url_for('create_dag'))

        config_data = json.load(file)
        dag_name = dag_server.add_dag(config_data, file.filename)

        flash(f'DAG {dag_name} created successfully', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        logger.error(f"Error creating DAG: {str(e)}")
        flash(f'Error creating DAG: {str(e)}', 'error')
        return redirect(url_for('create_dag'))


@app.route('/dag/<dag_name>/clone', methods=['GET', 'POST'])
@admin_required
def clone_dag(dag_name):
    if request.method == 'GET':
        try:
            # Get original DAG details
            dag = dag_server.dags.get(dag_name)
            if not dag:
                flash(f'DAG {dag_name} not found', 'error')
                return redirect(url_for('dashboard'))

            return render_template('clone_dag.html',
                                   dag_name=dag_name,
                                   original_start=dag.start_time,
                                   original_end=dag.end_time,
                                   original_duration=dag.duration)
        except Exception as e:
            logger.error(f"Error loading clone page: {str(e)}")
            flash(f'Error: {str(e)}', 'error')
            return redirect(url_for('dashboard'))

    try:
        # Get form data
        start_time = request.form.get('start_time', '').strip()
        duration = request.form.get('duration', '').strip()

        # Convert empty strings to None
        if not start_time:
            start_time = None
        if not duration:
            duration = None

        # Clean start_time - remove colons if present
        if start_time:
            start_time = start_time.replace(':', '')

            # Validate start_time format
            if len(start_time) != 4 or not start_time.isdigit():
                flash('Invalid start_time format. Use HHMM format (e.g., 0900)', 'error')
                return redirect(url_for('clone_dag', dag_name=dag_name))

            hour = int(start_time[:2])
            minute = int(start_time[2:])
            if hour > 23 or minute > 59:
                flash('Invalid start_time. Hour must be 0-23, minute must be 0-59', 'error')
                return redirect(url_for('clone_dag', dag_name=dag_name))

        # Validate duration format if provided
        if duration:
            import re
            duration_pattern = r'^(\d+h)?(\d+m)?$'
            if not re.match(duration_pattern, duration.lower()):
                flash('Invalid duration format. Use format like: 1h, 30m, or 1h30m', 'error')
                return redirect(url_for('clone_dag', dag_name=dag_name))

            # Check that duration has at least hours or minutes
            if 'h' not in duration.lower() and 'm' not in duration.lower():
                flash('Duration must include hours (h) or minutes (m)', 'error')
                return redirect(url_for('clone_dag', dag_name=dag_name))

        # Clone the DAG with new time window
        cloned_name = dag_server.clone_dag(dag_name, start_time, duration)

        # Prepare success message
        if start_time and duration:
            flash(f'DAG cloned to {cloned_name} with start_time={start_time}, duration={duration}', 'success')
        elif start_time:
            flash(f'DAG cloned to {cloned_name} with start_time={start_time} (default duration: -5 minutes)', 'success')
        else:
            flash(f'DAG cloned to {cloned_name} with perpetual running (24/7)', 'success')

        return redirect(url_for('dashboard'))
    except Exception as e:
        logger.error(f"Error cloning DAG: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        flash(f'Error cloning DAG: {str(e)}', 'error')
        return redirect(url_for('clone_dag', dag_name=dag_name))


@app.route('/dag/<dag_name>/delete', methods=['POST'])
@admin_required
def delete_dag(dag_name):
    try:
        dag_server.delete(dag_name)
        flash(f'DAG {dag_name} deleted', 'success')
    except Exception as e:
        logger.error(f"Error deleting DAG: {str(e)}")
        flash(f'Error deleting DAG: {str(e)}', 'error')

    return redirect(url_for('dashboard'))


@app.route('/dag/<dag_name>/start', methods=['POST'])
@admin_required
def start_dag(dag_name):
    try:
        dag_server.start(dag_name)
        flash(f'DAG {dag_name} started', 'success')
    except Exception as e:
        logger.error(f"Error starting DAG: {str(e)}")
        flash(f'Error starting DAG: {str(e)}', 'error')

    return redirect(url_for('dashboard'))


@app.route('/dag/<dag_name>/stop', methods=['POST'])
@admin_required
def stop_dag(dag_name):
    try:
        dag_server.stop(dag_name)
        flash(f'DAG {dag_name} stopped', 'success')
    except Exception as e:
        logger.error(f"Error stopping DAG: {str(e)}")
        flash(f'Error stopping DAG: {str(e)}', 'error')

    return redirect(url_for('dashboard'))


@app.route('/dag/<dag_name>/suspend', methods=['POST'])
@admin_required
def suspend_dag(dag_name):
    try:
        dag_server.suspend(dag_name)
        flash(f'DAG {dag_name} suspended', 'success')
    except Exception as e:
        logger.error(f"Error suspending DAG: {str(e)}")
        flash(f'Error suspending DAG: {str(e)}', 'error')

    return redirect(url_for('dashboard'))


@app.route('/dag/<dag_name>/resume', methods=['POST'])
@admin_required
def resume_dag(dag_name):
    try:
        dag_server.resume(dag_name)
        flash(f'DAG {dag_name} resumed', 'success')
    except Exception as e:
        logger.error(f"Error resuming DAG: {str(e)}")
        flash(f'Error resuming DAG: {str(e)}', 'error')

    return redirect(url_for('dashboard'))


# Cache Management Routes
@app.route('/cache')
@login_required
def cache_management():
    return render_template('cache_management.html',
                           is_admin=user_registry.has_role(session.get('username'), 'admin'))


@app.route('/cache/api/query', methods=['POST'])
@login_required
def cache_query():
    """Query cache with pattern"""
    try:
        data = request.get_json()
        pattern = data.get('pattern', '*')
        page = data.get('page', 1)
        per_page = data.get('per_page')

        # Get matching keys
        all_keys = redis_cache.keys(pattern)
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
                value = redis_cache.get(key)
                ttl = redis_cache.ttl(key)
                key_type = redis_cache.type(key)

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


@app.route('/cache/create', methods=['GET'])
@admin_required
def cache_create_page():
    """Cache entry create page"""
    return render_template('cache_create.html')


@app.route('/cache/api/create', methods=['POST'])
@admin_required
def cache_create():
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
                    redis_cache.set(key, value, ex=ttl_int)
                else:
                    redis_cache.set(key, value)
            except ValueError:
                if request.is_json:
                    return jsonify({'success': False, 'error': 'Invalid TTL value'}), 400
                else:
                    flash('Invalid TTL value', 'error')
                    return redirect(url_for('cache_create_page'))
        else:
            redis_cache.set(key, value)

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


@app.route('/cache/api/delete', methods=['DELETE'])
@admin_required
def cache_delete():
    """Delete a cache entry"""
    try:
        data = request.get_json()
        key = data.get('key')

        if not key:
            return jsonify({'success': False, 'error': 'Key is required'}), 400

        deleted = redis_cache.delete(key)

        if deleted:
            logger.info(f"Cache entry deleted: {key}")
            return jsonify({'success': True, 'message': f'Entry {key} deleted successfully'})
        else:
            return jsonify({'success': False, 'error': 'Key not found'}), 404
    except Exception as e:
        logger.error(f"Error deleting cache entry: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/cache/api/ttl', methods=['PUT'])
@admin_required
def cache_update_ttl():
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
        if not redis_cache.exists(key):
            return jsonify({'success': False, 'error': 'Key not found'}), 404

        ttl_int = int(ttl)
        if ttl_int > 0:
            redis_cache.expire(key, ttl_int)
            logger.info(f"TTL updated for key {key}: {ttl_int}s")
            return jsonify({'success': True, 'message': f'TTL for {key} updated to {ttl_int}s'})
        elif ttl_int == -1:
            redis_cache.persist(key)
            logger.info(f"TTL removed for key {key}")
            return jsonify({'success': True, 'message': f'TTL removed for {key}'})
        else:
            return jsonify({'success': False, 'error': 'TTL must be positive or -1 for no expiry'}), 400
    except Exception as e:
        logger.error(f"Error updating TTL: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/cache/api/clear', methods=['POST'])
@admin_required
def cache_clear():
    """Clear entire cache"""
    try:
        redis_cache.flushall()
        logger.warning("Cache cleared by admin")
        return jsonify({'success': True, 'message': 'Cache cleared successfully'})
    except Exception as e:
        logger.error(f"Error clearing cache: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/cache/api/download', methods=['GET'])
@admin_required
def cache_download():
    """Download entire cache as JSON"""
    try:
        all_keys = redis_cache.keys('*')
        cache_data = {}

        for key in all_keys:
            try:
                value = redis_cache.get(key)
                ttl = redis_cache.ttl(key)
                key_type = redis_cache.type(key)

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
        from datetime import datetime
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


@app.route('/cache/api/stats', methods=['GET'])
@login_required
def cache_stats():
    """Get cache statistics"""
    try:
        all_keys = redis_cache.keys('*')
        total_keys = len(all_keys)

        # Count keys by type
        type_counts = {}
        keys_with_ttl = 0

        for key in all_keys:
            key_type = redis_cache.type(key)
            type_counts[key_type] = type_counts.get(key_type, 0) + 1

            ttl = redis_cache.ttl(key)
            if ttl > 0:
                keys_with_ttl += 1

        # Get dump info
        dump_info = redis_cache.get_dump_info()

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


@app.route('/cache/api/dump/trigger', methods=['POST'])
@admin_required
def trigger_manual_dump():
    """Manually trigger a cache dump"""
    try:
        logger.info(f"Manual dump triggered by user: {session.get('username', 'unknown')}")
        success = redis_cache.dump_to_file()
        if success:
            dump_info = redis_cache.get_dump_info()
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


@app.route('/cache/api/session-check', methods=['GET'])
@login_required
def check_session():
    """Check if user session is valid"""
    return jsonify({
        'success': True,
        'username': session.get('username'),
        'is_admin': user_registry.has_role(session.get('username'), 'admin'),
        'roles': session.get('roles', [])
    })


# User Management Routes
@app.route('/users')
@admin_required
def user_management():
    """User management page"""
    return render_template('user_management.html')


@app.route('/users/api/list', methods=['GET'])
@admin_required
def users_list():
    """List all users"""
    try:
        users = user_registry.list_all_users(include_passwords=False)

        # Convert to list format with username included
        users_list = []
        for username, user_data in users.items():
            user_info = user_data.copy()
            user_info['username'] = username
            users_list.append(user_info)

        return jsonify({
            'success': True,
            'users': users_list,
            'total': len(users_list)
        })
    except Exception as e:
        logger.error(f"Error listing users: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/users/api/create', methods=['POST'])
@admin_required
def users_create():
    """Create a new user"""
    try:
        data = request.get_json()
        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        full_name = data.get('full_name', '').strip()
        roles = data.get('roles', [])

        # Validation
        if not username:
            return jsonify({'success': False, 'error': 'Username is required'}), 400

        if not password:
            return jsonify({'success': False, 'error': 'Password is required'}), 400

        if user_registry.user_exists(username):
            return jsonify({'success': False, 'error': f'User {username} already exists'}), 400

        # Create user data
        user_data = {
            'password': password,
            'full_name': full_name if full_name else username,
            'roles': roles if roles else ['user']
        }

        # Create user
        success = user_registry.create_user(username, user_data, session.get('username'))

        if success:
            logger.info(f"User {username} created by {session.get('username')}")
            return jsonify({'success': True, 'message': f'User {username} created successfully'})
        else:
            return jsonify({'success': False, 'error': 'Failed to create user'}), 500

    except Exception as e:
        logger.error(f"Error creating user: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/users/api/update', methods=['PUT', 'POST'])
@admin_required
def users_update():
    """Update an existing user"""
    try:
        # Handle both JSON (API) and form data (from edit page)
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form.to_dict()
            # Convert roles from form checkboxes
            roles = []
            if request.form.get('role_user') == 'on':
                roles.append('user')
            if request.form.get('role_operator') == 'on':
                roles.append('operator')
            if request.form.get('role_admin') == 'on':
                roles.append('admin')
            data['roles'] = roles

        username = data.get('username', '').strip()
        password = data.get('password', '').strip()
        full_name = data.get('full_name', '').strip()
        roles = data.get('roles', [])

        # Validation
        if not username:
            if request.is_json:
                return jsonify({'success': False, 'error': 'Username is required'}), 400
            else:
                flash('Username is required', 'error')
                return redirect(url_for('user_edit_page', username=username))

        # Protect admin user
        if username.lower() == 'admin':
            if request.is_json:
                return jsonify({'success': False, 'error': 'Cannot edit the protected admin user'}), 403
            else:
                flash('Cannot edit the protected admin user', 'error')
                return redirect(url_for('user_management'))

        if not user_registry.user_exists(username):
            if request.is_json:
                return jsonify({'success': False, 'error': f'User {username} not found'}), 404
            else:
                flash(f'User {username} not found', 'error')
                return redirect(url_for('user_management'))

        # Build update data
        user_data = {}
        if password:
            user_data['password'] = password
        if full_name:
            user_data['full_name'] = full_name
        if roles is not None:
            if len(roles) == 0:
                if request.is_json:
                    return jsonify({'success': False, 'error': 'At least one role is required'}), 400
                else:
                    flash('At least one role is required', 'error')
                    return redirect(url_for('user_edit_page', username=username))
            user_data['roles'] = roles

        # Prevent removing admin role from the last admin
        if 'roles' in user_data and 'admin' not in user_data['roles']:
            if user_registry.has_role(username, 'admin'):
                # Count total admins
                all_users = user_registry.list_all_users()
                admin_count = sum(1 for u in all_users.values() if 'admin' in u.get('roles', []))
                if admin_count <= 1:
                    if request.is_json:
                        return jsonify(
                            {'success': False, 'error': 'Cannot remove admin role from the last admin user'}), 400
                    else:
                        flash('Cannot remove admin role from the last admin user', 'error')
                        return redirect(url_for('user_edit_page', username=username))

        # Update user
        success = user_registry.modify_user(username, user_data, session.get('username'))

        if success:
            logger.info(f"User {username} updated by {session.get('username')}")
            if request.is_json:
                return jsonify({'success': True, 'message': f'User {username} updated successfully'})
            else:
                flash(f'User {username} updated successfully', 'success')
                return redirect(url_for('user_management'))
        else:
            if request.is_json:
                return jsonify({'success': False, 'error': 'Failed to update user'}), 500
            else:
                flash('Failed to update user', 'error')
                return redirect(url_for('user_edit_page', username=username))

    except Exception as e:
        logger.error(f"Error updating user: {str(e)}")
        if request.is_json:
            return jsonify({'success': False, 'error': str(e)}), 500
        else:
            flash(f'Error updating user: {str(e)}', 'error')
            return redirect(url_for('user_management'))


@app.route('/users/api/delete', methods=['DELETE'])
@admin_required
def users_delete():
    """Delete a user"""
    try:
        data = request.get_json()
        username = data.get('username', '').strip()

        # Validation
        if not username:
            return jsonify({'success': False, 'error': 'Username is required'}), 400

        # Protect admin user
        if username.lower() == 'admin':
            return jsonify({'success': False, 'error': 'Cannot delete the protected admin user'}), 403

        # Prevent self-deletion
        if username == session.get('username'):
            return jsonify({'success': False, 'error': 'Cannot delete your own account'}), 400

        if not user_registry.user_exists(username):
            return jsonify({'success': False, 'error': f'User {username} not found'}), 404

        # Delete user (UserRegistry will prevent deleting last admin)
        success = user_registry.delete_user(username, session.get('username'))

        if success:
            logger.info(f"User {username} deleted by {session.get('username')}")
            return jsonify({'success': True, 'message': f'User {username} deleted successfully'})
        else:
            return jsonify(
                {'success': False, 'error': 'Failed to delete user. Cannot delete the last admin user.'}), 400

    except Exception as e:
        logger.error(f"Error deleting user: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/users/api/stats', methods=['GET'])
@admin_required
def users_stats():
    """Get user statistics"""
    try:
        all_users = user_registry.list_all_users()

        # Count users by role
        role_counts = {}
        for user_data in all_users.values():
            for role in user_data.get('roles', []):
                role_counts[role] = role_counts.get(role, 0) + 1

        return jsonify({
            'success': True,
            'stats': {
                'total_users': len(all_users),
                'role_counts': role_counts
            }
        })
    except Exception as e:
        logger.error(f"Error getting user stats: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/users/edit/<username>', methods=['GET'])
@admin_required
def user_edit_page(username):
    """User edit page"""
    try:
        # Check if user exists
        if not user_registry.user_exists(username):
            flash(f'User {username} not found', 'error')
            return redirect(url_for('user_management'))

        # Protect admin user
        if username.lower() == 'admin':
            flash('Cannot edit the protected admin user', 'error')
            return redirect(url_for('user_management'))

        # Get user data
        user_data = user_registry.get_user(username, include_password=False)

        return render_template('user_edit.html', user_data=user_data)
    except Exception as e:
        logger.error(f"Error loading user edit page: {str(e)}")
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('user_management'))


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)