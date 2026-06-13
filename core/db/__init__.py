"""
core.db - Database layer for DishtaYantra v2.0.0.

ALL database interaction for users/roles/api keys is concentrated here:
    - models.py            SQLAlchemy ORM models
    - database_manager.py  Engine/session lifecycle (sqlite | postgresql)
    - dao.py               UserDAO / ApiKeyDAO (the only query layer)

The legacy db_connection_pool* modules (used by the SQL pub/sub) also live
in this package and are unrelated to the user store.
"""
from core.db.database_manager import DatabaseManager, DatabaseConfigurationError
from core.db.dao import UserDAO, ApiKeyDAO, PasswordHasher
from core.db.models import Base, User, Role, ApiKey

__all__ = [
    "DatabaseManager", "DatabaseConfigurationError",
    "UserDAO", "ApiKeyDAO", "PasswordHasher",
    "Base", "User", "Role", "ApiKey",
]
