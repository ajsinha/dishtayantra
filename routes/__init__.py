"""Routes module - Contains all application route handlers"""
from .auth_routes import AuthRoutes
from .dashboard_routes import DashboardRoutes
from .dag_routes import DAGRoutes
from .cache_routes import CacheRoutes
from .user_routes import UserRoutes

__all__ = [
    'AuthRoutes',
    'DashboardRoutes',
    'DAGRoutes',
    'CacheRoutes',
    'UserRoutes'
]
