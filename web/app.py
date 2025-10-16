import json
import logging
import os
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from functools import wraps
from core.dag.dag_server import DAGComputeServer

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
            "role": "admin"
        },
        "user1": {
            "password": "user123",
            "role": "user"
        }
    }
    with open(USERS_FILE, 'w') as f:
        json.dump(default_users, f, indent=2)
    logger.info(f"Created default users file: {USERS_FILE}")

dag_server = DAGComputeServer(DAG_CONFIG_FOLDER, ZOOKEEPER_HOSTS)


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
        if session.get('role') != 'admin':
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
            session['role'] = users_db[username].get('role', 'user')
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
                               is_admin=session.get('role') == 'admin',
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

        # Build dependency info
        node_details = []
        for node in sorted_nodes:
            dependencies = [edge.from_node.name for edge in node._incoming_edges]
            node_details.append({
                'name': node.name,
                'type': node.__class__.__name__,
                'dependencies': dependencies,
                'config': node.config
            })

        return render_template('dag_details.html',
                               dag_name=dag_name,
                               details=details,
                               node_details=node_details,
                               is_admin=session.get('role') == 'admin')
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

        # Build state info
        node_states = []
        for node in sorted_nodes:
            node_states.append({
                'name': node.name,
                'input': node._input,
                'output': node._output,
                'isdirty': node._isdirty,
                'errors': list(node._errors)
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


@app.route('/dag/<dag_name>/subscriber/<subscriber_name>/publish', methods=['POST'])
@admin_required
def publish_to_subscriber(dag_name, subscriber_name):
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
    except Exception as e:
        logger.error(f"Error publishing message: {str(e)}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)