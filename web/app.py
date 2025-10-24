import json
import logging
import os
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
from core.dag.dag_server import DAGComputeServer
from core.pubsub.inmemory_redisclone import InMemoryRedisClone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me')

# Initialize DAG server
DAG_CONFIG_FOLDER = os.environ.get('DAG_CONFIG_FOLDER', './config/dags')
ZOOKEEPER_HOSTS = os.environ.get('ZOOKEEPER_HOSTS', 'localhost:2181')
USERS_FILE = os.environ.get('USERS_FILE', './config/users.json')

# Ensure directories exist
os.makedirs(DAG_CONFIG_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(USERS_FILE), exist_ok=True)
os.makedirs('./logs', exist_ok=True)

# Create default users file if it doesn't exist
if not os.path.exists(USERS_FILE):
    default_users = {
        "admin": {
            "password": "admin123",
            "full_name": "System Administrator",
            "roles": ["admin", "user"]
        },
        "user1": {
            "password": "user123",
            "full_name": "John Doe",
            "roles": ["user"]
        }
    }
    with open(USERS_FILE, 'w') as f:
        json.dump(default_users, f, indent=2)
    logger.info(f"Created default users file: {USERS_FILE}")

dag_server = DAGComputeServer(DAG_CONFIG_FOLDER, ZOOKEEPER_HOSTS)

# Initialize shared InMemoryRedis instance
# This should be the same instance used by DAG nodes
redis_cache = InMemoryRedisClone()

# Load users
def load_users():
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    return {}


users_db = load_users()


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
        # Check if user has admin role in their roles list
        if 'admin' not in session.get('roles', []):
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

        if username in users_db and users_db[username]['password'] == password:
            session['username'] = username
            session['full_name'] = users_db[username].get('full_name', username)
            session['roles'] = users_db[username].get('roles', ['user'])
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
                               is_admin='admin' in session.get('roles', []),
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

            # Get last calculation time - FIXED: use _last_compute from Node class
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
                               is_admin='admin' in session.get('roles', []))
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
            # Get calculation count - FIXED: use _compute_count from Node class
            calculation_count = None
            if hasattr(node, '_compute_count'):
                calculation_count = node._compute_count
            elif hasattr(node, 'compute_count'):
                calculation_count = node.compute_count

            # Get last calculation time - FIXED: use _last_compute from Node class
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
                                   original_end=dag.end_time)
        except Exception as e:
            logger.error(f"Error loading clone page: {str(e)}")
            flash(f'Error: {str(e)}', 'error')
            return redirect(url_for('dashboard'))

    try:
        start_time = request.form.get('start_time')
        end_time = request.form.get('end_time')

        cloned_name = dag_server.clone_dag(dag_name, start_time, end_time)
        flash(f'DAG cloned to {cloned_name}', 'success')
        return redirect(url_for('dashboard'))
    except Exception as e:
        logger.error(f"Error cloning DAG: {str(e)}")
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


@app.route('/dag/<dag_name>/subscriber/<subscriber_name>/publish', methods=['GET', 'POST'])
@admin_required
def publish_message(dag_name, subscriber_name):
    """Display the publish message page (GET) or handle message submission (POST)"""
    if request.method == 'GET':
        try:
            details = dag_server.details(dag_name)

            # Check if subscriber exists
            if subscriber_name not in details.get('subscribers', {}):
                flash(f'Subscriber {subscriber_name} not found', 'error')
                return redirect(url_for('dag_details', dag_name=dag_name))

            subscriber_info = details['subscribers'][subscriber_name]

            return render_template(
                'publish_message.html',
                dag_name=dag_name,
                subscriber_name=subscriber_name,
                subscriber_info=subscriber_info,
                is_admin=True
            )
        except Exception as e:
            logger.error(f"Error loading publish message page: {str(e)}")
            flash(f'Error: {str(e)}', 'error')
            return redirect(url_for('dag_details', dag_name=dag_name))

    # POST method - handle message submission
    try:
        message = request.form.get('message')
        if not message:
            return jsonify({'error': 'No message provided'}), 400

        message_data = json.loads(message)

        # Get the subscriber from DAG
        dag = dag_server.dags.get(dag_name)
        if not dag:
            return jsonify({'error': 'DAG not found'}), 404

        subscriber = dag.subscribers.get(subscriber_name)
        if not subscriber:
            return jsonify({'error': 'Subscriber not found'}), 404

        # Check if subscriber type supports publishing
        source = subscriber.source
        if not any(prefix in source for prefix in ['mem://', 'kafka://', 'redischannel://', 'activemq://']):
            return jsonify({'error': 'Subscriber type does not support publishing'}), 400

        # Create a temporary publisher to the same source
        from core.pubsub.pubsubfactory import create_publisher
        config = subscriber.config.copy()
        config['destination'] = source

        temp_publisher = create_publisher(f'temp_pub_{subscriber_name}', config)
        temp_publisher.publish(message_data)
        temp_publisher.stop()

        return jsonify({'success': True, 'message': 'Message published successfully'})
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {str(e)}")
        return jsonify({'error': f'Invalid JSON: {str(e)}'}), 400
    except Exception as e:
        logger.error(f"Error publishing message: {str(e)}")
        return jsonify({'error': str(e)}), 500


# ==================== CACHE MANAGEMENT ROUTES ====================

@app.route('/cache')
@login_required
def cache_management():
    """Display cache management page"""
    return render_template('cache_management.html',
                           is_admin='admin' in session.get('roles', []))


@app.route('/cache/api/query', methods=['GET'])
@login_required
def cache_query():
    """Query cache with optional pattern matching"""
    try:
        pattern = request.args.get('pattern', '*')
        page = int(request.args.get('page', 1))
        per_page = request.args.get('per_page', '10')

        # Handle 'all' option
        if per_page == 'all':
            per_page = None
        else:
            per_page = int(per_page)

        # Get all keys matching pattern
        all_keys = redis_cache.keys(pattern)
        total_keys = len(all_keys)

        # Sort keys for consistent pagination
        all_keys.sort()

        # Apply pagination
        if per_page:
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            keys_page = all_keys[start_idx:end_idx]
            total_pages = (total_keys + per_page - 1) // per_page
        else:
            keys_page = all_keys
            total_pages = 1

        # Get details for each key
        results = []
        for key in keys_page:
            try:
                value = redis_cache.get(key)
                key_type = redis_cache.type(key)
                ttl = redis_cache.ttl(key)

                # Format TTL
                if ttl == -1:
                    ttl_display = 'No expiry'
                elif ttl == -2:
                    ttl_display = 'Expired/Not found'
                else:
                    ttl_display = f'{ttl}s'

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


@app.route('/cache/api/create', methods=['POST'])
@admin_required
def cache_create():
    """Create a new cache entry"""
    try:
        data = request.get_json()
        key = data.get('key')
        value = data.get('value')
        ttl = data.get('ttl')

        if not key:
            return jsonify({'success': False, 'error': 'Key is required'}), 400

        if value is None:
            return jsonify({'success': False, 'error': 'Value is required'}), 400

        # Set the value
        if ttl and int(ttl) > 0:
            redis_cache.set(key, value, ex=int(ttl))
        else:
            redis_cache.set(key, value)

        logger.info(f"Cache entry created: {key}")
        return jsonify({'success': True, 'message': f'Entry {key} created successfully'})
    except Exception as e:
        logger.error(f"Error creating cache entry: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


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

        return send_file(
            json_bytes,
            mimetype='application/json',
            as_attachment=True,
            download_name=f'cache_export_{int(os.times().elapsed)}.json'
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

        return jsonify({
            'success': True,
            'stats': {
                'total_keys': total_keys,
                'keys_with_ttl': keys_with_ttl,
                'keys_without_ttl': total_keys - keys_with_ttl,
                'types': type_counts
            }
        })
    except Exception as e:
        logger.error(f"Error getting cache stats: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)