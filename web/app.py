"""
Flask application entry point
Refactored to use modular route handlers
"""
import logging
import os

from flask import Flask
from core.dag.dag_server import DAGComputeServer
from core.pubsub.inmemory_redisclone import InMemoryRedisClone
from core.user_registry import UserRegistry
from core.properties_configurator import PropertiesConfigurator

# Import route handlers
from routes import AuthRoutes, DashboardRoutes, DAGRoutes, CacheRoutes, UserRoutes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__)

# Load configuration
props = None
app_name = "DishtaYantra"  # Default value
try:
    props = PropertiesConfigurator(['config/application.properties'])
    app.secret_key = props.get('flask.secret_key', os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me'))
    app_name = props.get('app.name', 'DishtaYantra')
except Exception as e:
    logger.warning(f"Could not load properties: {e}, using defaults")
    app.secret_key = os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me')

# Make app_name available to all templates
@app.context_processor
def inject_app_name():
    return {'app_name': app_name}

# Initialize DAG server configuration
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

# Initialize core components
user_registry = UserRegistry(users_file=USERS_FILE, reload_interval=USER_RELOAD_INTERVAL)
dag_server = DAGComputeServer(DAG_CONFIG_FOLDER, ZOOKEEPER_HOSTS)
redis_cache = InMemoryRedisClone()

# Initialize route handlers with dependency injection
logger.info("Initializing route handlers...")

# Auth routes provide decorators for other routes
auth_routes = AuthRoutes(app, user_registry)
login_required = auth_routes.login_required
admin_required = auth_routes.admin_required

# Initialize remaining route handlers
dashboard_routes = DashboardRoutes(app, dag_server, user_registry, login_required)
dag_routes = DAGRoutes(app, dag_server, admin_required)
cache_routes = CacheRoutes(app, redis_cache, user_registry, login_required, admin_required)
user_routes = UserRoutes(app, user_registry, admin_required)

logger.info("Route handlers initialized successfully")


if __name__ == '__main__':
    logger.info("Starting Flask application...")
    app.run(debug=True, host='0.0.0.0', port=5000)