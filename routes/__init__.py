"""
Routes Module - Contains all application route handlers
========================================================

Version: 1.5.0
"""
from .auth_routes import AuthRoutes
from .noauth_routes import NoAuthRoutes
from .dashboard_routes import DashboardRoutes
from .dag_routes import DAGRoutes
from .cache_routes import CacheRoutes
from .user_routes import UserRoutes
from .dagdesigner_routes import DAGDesignerRoutes
from .metrics_routes import MetricsRoutes

__all__ = [
    'AuthRoutes',
    'NoAuthRoutes',
    'DashboardRoutes',
    'DAGRoutes',
    'CacheRoutes',
    'UserRoutes',
    'DAGDesignerRoutes',
    'MetricsRoutes'
]
