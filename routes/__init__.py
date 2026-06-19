"""
Routes Package - All application route handlers (FastAPI)
==========================================================

Version: 2.0.0

Each module groups one logical area of the web UI/API:

    auth_routes        - login / logout / root redirect
    noauth_routes      - about, help pages, user guides, research paper
    dashboard_routes   - dashboard, DAG details/state views
    dag_routes         - DAG lifecycle (create/clone/start/stop/...)
    cache_routes       - cache HTML pages (+ CacheRoutes composition facade)
    cache_api_routes   - cache JSON API
    user_routes        - user management pages + API (DB-backed in v2.0.0)
    dagdesigner_routes - visual designer + validation/components API
    admin_routes       - system monitoring dashboard + metrics API
    admin_log_routes   - log viewer, download, live SSE stream
    worker_routes      - worker pool monitoring/control
    metrics_routes     - Prometheus /metrics + health probes (no auth)
    jvm_routes         - Java (Py4J) integration management
    cpp_routes         - C++ (pybind11) integration management
    rust_routes        - Rust (PyO3) integration management

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""
from .auth_routes import AuthRoutes
from .noauth_routes import NoAuthRoutes
from .dashboard_routes import DashboardRoutes
from .dag_routes import DAGRoutes
from .cache_routes import CacheRoutes
from .cache_api_routes import CacheApiRoutes
from .user_routes import UserRoutes
from .dagdesigner_routes import DAGDesignerRoutes
from .metrics_routes import MetricsRoutes
from .worker_routes import WorkerRoutes
from .jvm_routes import JVMRoutes
from .cpp_routes import CPPRoutes
from .rust_routes import RustRoutes
from .egress_routes import EgressRoutes
from .admin_routes import AdminRoutes
from .admin_log_routes import AdminLogRoutes
from .maintenance_routes import MaintenanceRoutes

__all__ = [
    'AuthRoutes',
    'NoAuthRoutes',
    'DashboardRoutes',
    'DAGRoutes',
    'CacheRoutes',
    'CacheApiRoutes',
    'UserRoutes',
    'DAGDesignerRoutes',
    'MetricsRoutes',
    'WorkerRoutes',
    'JVMRoutes',
    'CPPRoutes',
    'RustRoutes',
    'EgressRoutes',
    'AdminRoutes',
    'AdminLogRoutes',
    'MaintenanceRoutes',
]
