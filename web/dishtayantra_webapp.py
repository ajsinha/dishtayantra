"""
DishtaYantra Web Application (FastAPI)
======================================

FastAPI application entry point using the Singleton pattern with modular
route handlers.  Replaces the legacy Flask application (and the misspelled
``web/dishtyantra_webapp.py`` module name) in v2.0.0.

Responsibilities:
    - Build the FastAPI app, session middleware, and static mount.
    - Initialize core components: the database-backed UserRegistry, the
      DAGComputeServer (storage-abstracted + HA-managed), and the in-memory
      Redis clone cache.
    - Construct every route handler class with dependency injection.
    - Install app-level handlers translating the auth-guard exceptions into
      redirects (HTML pages) or 401/403 JSON (``/api/...`` paths).
    - Run the server via uvicorn (with optional TLS).

v1.5.2 behaviour preserved: the worker pool is created by ``run_server.py``
before the singleton and handed in through ``get_instance(worker_pool=...)``.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import os
import threading
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from core.dag.dag_server import DAGComputeServer
from core.properties_configurator import PropertiesConfigurator
from core.pubsub.inmemory_redisclone import InMemoryRedisClone
from core.user_registry import UserRegistry
from core.version import VERSION

from web import fastapi_compat
from web.fastapi_compat import (
    AuthGuards,
    NotAuthenticatedError,
    NotAuthorizedError,
    flash,
    flask_style_url_for,
    wants_json,
)

# Import route handlers
from routes import (
    AdminLogRoutes,
    MaintenanceRoutes,
    AdminRoutes,
    AuthRoutes,
    CacheRoutes,
    CPPRoutes,
    DAGDesignerRoutes,
    DAGRoutes,
    DashboardRoutes,
    JVMRoutes,
    MetricsRoutes,
    NoAuthRoutes,
    RustRoutes,
    EgressRoutes,
    UserRoutes,
    WorkerRoutes,
)

# Configure logging (text or JSON, driven by logging.* config; stdout only at
# import time - run_server adds the file handler on real startup)
from core.log_config import configure_logging
configure_logging(logfile=None)
logger = logging.getLogger(__name__)


class DishtaYantraWebApp:
    """
    Singleton class for the DishtaYantra Web Application.
    Manages the FastAPI app lifecycle and component initialization.

    v1.5.2: Accepts worker_pool from run_server.py for early initialization.
    v2.0.0: FastAPI + uvicorn; DB-backed users; storage-abstracted DAGs.
    """
    _instance = None
    _lock = threading.Lock()
    _pending_worker_pool = None  # v1.5.2: stored before singleton creation

    def __new__(cls):
        """Implement Singleton pattern."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DishtaYantraWebApp, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """Initialize the web application (only once due to Singleton)."""
        if self._initialized:
            return

        logger.info("Initializing DishtaYantra Web Application (FastAPI)...")

        self.props = None
        self.app_name = "DishtaYantra"

        # Component references
        self.user_registry = None
        self.dag_server = None
        self.redis_cache = None
        self.guards = None

        # v1.5.2: Worker pool from class variable (set by get_instance)
        self.worker_pool = self.__class__._pending_worker_pool

        # Route handler references
        self.auth_routes = None
        self.noauth_routes = None
        self.dashboard_routes = None
        self.dag_routes = None
        self.cache_routes = None
        self.user_routes = None
        self.dagdesigner_routes = None
        self.admin_routes = None
        self.admin_log_routes = None
        self.metrics_routes = None
        self.worker_routes = None
        self.jvm_routes = None
        self.cpp_routes = None
        self.rust_routes = None

        self._load_configuration()
        self._build_app()
        self._setup_app_context()
        self._initialize_directories()
        self._initialize_components()
        self._initialize_routes()

        self._initialized = True
        self._apply_gc_tuning()
        logger.info("DishtaYantra Web Application initialized successfully")

    # ------------------------------------------------------------------ #
    # Initialization steps
    # ------------------------------------------------------------------ #

    def _load_configuration(self):
        """Load application configuration from the properties file."""
        try:
            from core.config_parsers import find_default_config
            self.props = PropertiesConfigurator(
                [find_default_config('config')])
            self.app_name = self.props.get('app.name', 'DishtaYantra')
            logger.info("Configuration loaded successfully")
        except Exception as e:
            logger.error(f"Could not load application properties: {e}")
            logger.error(traceback.format_exc())
            raise

    def _secret_key(self) -> str:
        """
        Resolve the session signing secret.

        Order: app.secret_key property -> legacy flask.secret_key property
        -> SECRET_KEY environment variable.  A missing secret is a fatal
        configuration error (v2.0.0 mandate: no silent defaults).
        """
        secret = self.props.get('app.secret_key',
                                self.props.get('flask.secret_key',
                                               os.environ.get('SECRET_KEY')))
        if not secret:
            raise ValueError(
                "No session secret configured. Set 'app.secret_key' in "
                "config/application.properties (or the SECRET_KEY "
                "environment variable) and restart. Without it user "
                "sessions cannot be signed securely.")
        return secret

    def _build_app(self):
        """Construct the FastAPI app, middleware, static mount, handlers."""
        self.app = FastAPI(title=self.app_name, version=VERSION,
                           docs_url=None, redoc_url=None,
                           openapi_url=None)
        self.app.add_middleware(SessionMiddleware,
                                secret_key=self._secret_key())
        self.app.mount('/static',
                       StaticFiles(directory='web/static'), name='static')

        # Translate guard exceptions: HTML -> redirect+flash, API -> JSON.
        @self.app.exception_handler(NotAuthenticatedError)
        async def _not_authenticated(request: Request,
                                     exc: NotAuthenticatedError):
            if wants_json(request):
                return JSONResponse({'success': False,
                                     'error': 'Authentication required'},
                                    status_code=401)
            flash(request, 'Please log in to access this page.', 'error')
            return RedirectResponse(
                url=flask_style_url_for(request, 'login'), status_code=303)

        @self.app.exception_handler(NotAuthorizedError)
        async def _not_authorized(request: Request,
                                  exc: NotAuthorizedError):
            if wants_json(request):
                return JSONResponse({'success': False,
                                     'error': exc.message},
                                    status_code=403)
            flash(request, f'{exc.message}.', 'error')
            return RedirectResponse(
                url=flask_style_url_for(request, 'dashboard'),
                status_code=303)

    def _setup_app_context(self):
        """Publish app properties to the template compatibility layer.

        v1.7.6 behaviour preserved: all app.* metadata is injected into
        every template; v2.0.0 sources the version from core.version.
        """
        fastapi_compat.APP_TEMPLATE_PROPS.update({
            'app_name': self.app_name,
            # Single source of truth: the running version always comes from
            # core.version.VERSION, never from a config value that can drift
            # (a stale app.version in config once made the About page show an
            # old release). config app.version is kept only as documentation.
            'app_version': VERSION,
            'author_name': self.props.get('app.author.name',
                                          'Ashutosh Sinha'),
            'author_email': self.props.get('app.author.email',
                                           'ajsinha@gmail.com'),
            'copyright_years': self.props.get('app.copyright.years',
                                              '2025-2030'),
            'copyright_holder': self.props.get('app.copyright.holder',
                                               'Ashutosh Sinha'),
            'github_repo': self.props.get(
                'app.github.repo',
                'https://github.com/ajsinha/dishtayantra'),
            'trademark_notice': self.props.get(
                'app.trademark.notice',
                'DishtaYantra™ is a trademark of Ashutosh Sinha'),
        })

    def _initialize_directories(self):
        """Create necessary local directories if they don't exist."""
        dag_config_folder = os.environ.get('DAG_CONFIG_FOLDER',
                                           './config/dags')
        users_file = os.environ.get('USERS_FILE', './config/users.json')
        for directory in [dag_config_folder, os.path.dirname(users_file),
                          './logs', './data']:
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"Ensured directory exists: {directory}")

    def _apply_gc_tuning(self):
        """v5.14.0: optional GC tuning to reduce collection overhead under load.

        gc.freeze() moves everything allocated during startup (the app, loaded
        DAGs, templates, etc.) into a permanent generation that GC never scans
        again, so steady-state collections only walk the much smaller set of
        per-message objects. Optionally raises the gen-0 threshold to collect
        less often. Both are safe and config-gated; the defaults preserve prior
        behavior except for the (harmless) freeze.

            performance.gc.freeze=true|false     (default: true)
            performance.gc.threshold=<int>       (default: unset -> unchanged)
        """
        import gc
        try:
            freeze = str(self.props.get('performance.gc.freeze', 'true')).lower() \
                in ('1', 'true', 'yes', 'on')
            if freeze:
                gc.collect()
                gc.freeze()
                frozen = gc.get_freeze_count() if hasattr(gc, 'get_freeze_count') else -1
                logger.info("GC tuning: froze %d startup objects (gc.freeze)", frozen)
            thr = self.props.get('performance.gc.threshold', None)
            if thr:
                _, g1, g2 = gc.get_threshold()
                gc.set_threshold(int(thr), g1, g2)
                logger.info("GC tuning: gen0 threshold set to %s", thr)
        except Exception as e:  # noqa: BLE001 - tuning must never break startup
            logger.warning("GC tuning skipped: %s", e)

    def _initialize_components(self):
        """Initialize core application components.

        v1.5.2: The worker pool is passed from run_server.py and provided
        to DAGComputeServer during initialization.
        v2.0.0: UserRegistry is database-backed; DAG configs flow through
        the storage abstraction; HA is configured via ha.* properties.
        """
        logger.info("Initializing core components...")

        dag_config_folder = os.environ.get('DAG_CONFIG_FOLDER',
                                           './config/dags')
        users_file = os.environ.get('USERS_FILE', './config/users.json')

        # DB-backed registry; users_file only drives the one-time migration.
        self.user_registry = UserRegistry(users_file=users_file)

        # v1.5.2: Pass worker_pool to DAGComputeServer.
        # v2.0.0: the second argument (zookeeper hosts) is deprecated and
        # ignored - HA is fully configured via the ha.* properties.
        self.dag_server = DAGComputeServer(
            dag_config_folder,
            worker_pool=self.worker_pool
        )
        # v2.2: expose the server on app.state so the template context
        # processor can surface HA role (PRIMARY/SECONDARY) in the navbar
        # on every page (for logged-in users only).
        self.app.state.dag_server = self.dag_server
        self.redis_cache = InMemoryRedisClone()
        self.guards = AuthGuards(self.user_registry)

        if self.worker_pool:
            logger.info(f"DAGComputeServer initialized with worker pool "
                        f"({self.worker_pool.num_workers} workers)")
        else:
            logger.info("DAGComputeServer initialized in single-process "
                        "mode")
        logger.info("Core components initialized successfully")

    def _initialize_routes(self):
        """Initialize all route handlers with dependency injection."""
        logger.info("Initializing route handlers...")

        self.auth_routes = AuthRoutes(self.app, self.user_registry)
        self.noauth_routes = NoAuthRoutes(self.app)
        self.dashboard_routes = DashboardRoutes(
            self.app, self.dag_server, self.user_registry, self.guards,
            self.worker_pool)
        self.dag_routes = DAGRoutes(self.app, self.dag_server, self.guards,
                                    self.worker_pool)
        self.cache_routes = CacheRoutes(self.app, self.redis_cache,
                                        self.user_registry, self.guards)
        self.user_routes = UserRoutes(self.app, self.user_registry,
                                      self.guards)
        self.dagdesigner_routes = DAGDesignerRoutes(
            self.app, self.dag_server, self.user_registry, self.guards)
        self.admin_routes = AdminRoutes(self.app, self.dag_server,
                                        self.guards)
        self.admin_log_routes = AdminLogRoutes(self.app, self.guards,
                                               worker_pool=self.worker_pool)
        self.maintenance_routes = MaintenanceRoutes(
            self.app, self.dag_server, self.guards,
            worker_pool=self.worker_pool)
        self.metrics_routes = MetricsRoutes(self.app, self.dag_server,
                                            self.redis_cache)
        self.worker_routes = WorkerRoutes(self.app, self.worker_pool,
                                          self.guards)  # v1.5.2
        self.jvm_routes = JVMRoutes(self.app)    # v1.6.0
        self.cpp_routes = CPPRoutes(self.app)    # v1.7.0
        self.rust_routes = RustRoutes(self.app)  # v1.7.0
        self.egress_routes = EgressRoutes(self.app, self.guards)  # read-only egress monitoring

        logger.info("Route handlers initialized successfully")

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self, host='0.0.0.0', port=5000, debug=False,
              ssl_context=None):
        """
        Start the web application with uvicorn.

        Args:
            host: Host address to bind to.
            port: Port number to listen on.
            debug: Enables uvicorn debug-level logging when True.
            ssl_context: Optional (cert_file, key_file) tuple for TLS.

        v1.5.2: The worker pool is initialized by run_server.py before this
        is called.
        """
        import uvicorn
        logger.info(f"Starting DishtaYantra Web Application on "
                    f"{host}:{port}")
        try:
            kwargs = {'host': host, 'port': port,
                      'log_level': 'debug' if debug else 'info'}
            if ssl_context:
                kwargs['ssl_certfile'] = ssl_context[0]
                kwargs['ssl_keyfile'] = ssl_context[1]
            uvicorn.run(self.app, **kwargs)
        except Exception as e:
            logger.error(f"Error starting web application: {e}")
            logger.error(traceback.format_exc())
            raise

    def shutdown(self):
        """Shutdown the web application and clean up resources."""
        logger.info("Shutting down DishtaYantra Web Application...")
        try:
            # v1.5.2: shutdown worker pool first
            if self.worker_pool:
                logger.info("Shutting down worker pool...")
                try:
                    self.worker_pool.stop()
                except Exception as e:
                    logger.error(f"Error shutting down worker pool: {e}")
                    logger.error(traceback.format_exc())

            if self.dag_server:
                logger.info("Shutting down DAG server...")
                self.dag_server.shutdown()

            if self.user_registry:
                logger.info("Cleaning up user registry...")
                self.user_registry.stop_reload()

            logger.info("DishtaYantra Web Application shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            logger.error(traceback.format_exc())
            raise

    @classmethod
    def get_instance(cls, worker_pool=None):
        """Get the singleton instance of DishtaYantraWebApp.

        Args:
            worker_pool: Optional WorkerPoolManager (v1.5.2). Should be
                passed on the first call from run_server.py.
        """
        if worker_pool is not None:
            cls._pending_worker_pool = worker_pool
        return cls()


# Legacy module-level references for backward compatibility
app = None
dag_server = None


def _initialize_legacy_references():
    """Populate legacy module-level references after get_instance()."""
    global app, dag_server
    if DishtaYantraWebApp._instance is not None:
        webapp = DishtaYantraWebApp._instance
        app = webapp.app
        dag_server = webapp.dag_server


if __name__ == '__main__':
    logger.info("Starting FastAPI application directly...")
    webapp = DishtaYantraWebApp.get_instance()
    _initialize_legacy_references()
    webapp.start(debug=True, host='0.0.0.0', port=5000)
