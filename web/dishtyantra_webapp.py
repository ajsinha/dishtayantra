"""
Flask application entry point
Refactored to use Singleton pattern with modular route handlers
"""
import logging
import os
import threading

from flask import Flask
from core.dag.dag_server import DAGComputeServer
from core.pubsub.inmemory_redisclone import InMemoryRedisClone
from core.user_registry import UserRegistry
from core.properties_configurator import PropertiesConfigurator

# Import route handlers
from routes import AuthRoutes, DashboardRoutes, DAGRoutes, CacheRoutes, UserRoutes, DAGDesignerRoutes

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DishtaYantraWebApp:
    """
    Singleton class for DishtaYantra Web Application
    Manages Flask app lifecycle and component initialization
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """Implement Singleton pattern"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DishtaYantraWebApp, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the web application (only once due to Singleton)"""
        if self._initialized:
            return

        logger.info("Initializing DishtaYantra Web Application...")

        # Initialize Flask app
        self.app = Flask(__name__)

        # Initialize configuration
        self.props = None
        self.app_name = "DishtaYantra"  # Default value

        # Initialize component references
        self.user_registry = None
        self.dag_server = None
        self.redis_cache = None

        # Initialize route handlers references
        self.auth_routes = None
        self.dashboard_routes = None
        self.dag_routes = None
        self.cache_routes = None
        self.user_routes = None
        self.dagdesigner_routes = None

        # Perform initialization
        self._load_configuration()
        self._setup_app_context()
        self._initialize_directories()
        self._initialize_components()
        self._initialize_routes()

        self._initialized = True
        logger.info("DishtaYantra Web Application initialized successfully")

    def _load_configuration(self):
        """Load application configuration from properties file"""
        try:
            self.props = PropertiesConfigurator(['config/application.properties'])
            self.app.secret_key = self.props.get(
                'flask.secret_key',
                os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me')
            )
            self.app_name = self.props.get('app.name', 'DishtaYantra')
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load properties: {e}, using defaults")
            self.app.secret_key = os.environ.get('SECRET_KEY', 'dagserver_secret_key_change_me')

    def _setup_app_context(self):
        """Setup Flask app context processors"""
        @self.app.context_processor
        def inject_app_name():
            return {'app_name': self.app_name}

    def _initialize_directories(self):
        """Create necessary directories if they don't exist"""
        # Get configuration paths
        dag_config_folder = os.environ.get('DAG_CONFIG_FOLDER', './config/dags')
        users_file = os.environ.get('USERS_FILE', './config/users.json')

        # Create directories
        directories = [
            dag_config_folder,
            os.path.dirname(users_file),
            './logs'
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")

    def _initialize_components(self):
        """Initialize core application components"""
        logger.info("Initializing core components...")

        # Get configuration from environment or properties
        dag_config_folder = os.environ.get('DAG_CONFIG_FOLDER', './config/dags')
        zookeeper_hosts = os.environ.get('ZOOKEEPER_HOSTS', 'localhost:2181')
        users_file = os.environ.get('USERS_FILE', './config/users.json')

        # Get user registry reload interval from properties (default: 600 seconds = 10 minutes)
        user_reload_interval = 600
        if self.props:
            user_reload_interval = self.props.get_int('user.registry.reload_interval', 600)

        # Initialize components
        self.user_registry = UserRegistry(users_file=users_file, reload_interval=user_reload_interval)
        self.dag_server = DAGComputeServer(dag_config_folder, zookeeper_hosts)
        self.redis_cache = InMemoryRedisClone()

        logger.info("Core components initialized successfully")

    def _initialize_routes(self):
        """Initialize route handlers with dependency injection"""
        logger.info("Initializing route handlers...")

        # Auth routes provide decorators for other routes
        self.auth_routes = AuthRoutes(self.app, self.user_registry)
        login_required = self.auth_routes.login_required
        admin_required = self.auth_routes.admin_required

        # Initialize remaining route handlers
        self.dashboard_routes = DashboardRoutes(
            self.app, self.dag_server, self.user_registry, login_required
        )
        self.dag_routes = DAGRoutes(self.app, self.dag_server, admin_required)
        self.cache_routes = CacheRoutes(
            self.app, self.redis_cache, self.user_registry, login_required, admin_required
        )
        self.user_routes = UserRoutes(self.app, self.user_registry, admin_required)
        self.dagdesigner_routes = DAGDesignerRoutes(self.app, self.dag_server, self.user_registry, login_required)

        logger.info("Route handlers initialized successfully")

    def start(self, host='0.0.0.0', port=5000, debug=False, ssl_context=None):
        """
        Start the Flask web application

        Args:
            host (str): Host address to bind to
            port (int): Port number to listen on
            debug (bool): Enable debug mode
            ssl_context (tuple): SSL certificate and key file paths (cert_file, key_file)
        """
        logger.info(f"Starting DishtaYantra Web Application on {host}:{port}")

        try:
            if ssl_context:
                self.app.run(host=host, port=port, debug=debug, ssl_context=ssl_context)
            else:
                self.app.run(host=host, port=port, debug=debug)
        except Exception as e:
            logger.error(f"Error starting web application: {e}")
            raise

    def shutdown(self):
        """Shutdown the web application and cleanup resources"""
        logger.info("Shutting down DishtaYantra Web Application...")

        try:
            # Shutdown DAG server
            if self.dag_server:
                logger.info("Shutting down DAG server...")
                self.dag_server.shutdown()

            # Cleanup user registry
            if self.user_registry:
                logger.info("Cleaning up user registry...")
                # Add any cleanup logic for user registry if needed

            # Cleanup redis cache
            if self.redis_cache:
                logger.info("Cleaning up redis cache...")
                # Add any cleanup logic for redis cache if needed

            logger.info("DishtaYantra Web Application shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            raise

    @classmethod
    def get_instance(cls):
        """Get the singleton instance of DishtaYantraWebApp"""
        return cls()


# For backward compatibility and direct execution
app = None
dag_server = None

def _initialize_legacy_references():
    """Initialize legacy module-level references for backward compatibility"""
    global app, dag_server
    webapp = DishtaYantraWebApp.get_instance()
    app = webapp.app
    dag_server = webapp.dag_server

_initialize_legacy_references()


if __name__ == '__main__':
    logger.info("Starting Flask application directly...")
    webapp = DishtaYantraWebApp.get_instance()
    webapp.start(debug=True, host='0.0.0.0', port=5000)