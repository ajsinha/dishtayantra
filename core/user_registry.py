"""
User Registry - Database-Backed Authentication and RBAC (v2.0.0)
================================================================

Facade over :class:`core.db.dao.UserDAO` / :class:`core.db.dao.ApiKeyDAO`
that preserves the public API of the legacy JSON-file registry so that the
web layer and any external automation keep working unchanged.

What changed in v2.0.0:
    - Users, roles, and API keys live in a relational database (SQLite by
      default; switch to PostgreSQL purely via configuration - see
      core/db/database_manager.py).
    - Passwords are stored as PBKDF2-SHA256 hashes, never in clear text.
    - A one-time migration imports the legacy ``config/users.json`` (when it
      exists and the users table is empty), hashing every password.
    - API keys: issue / verify / revoke programmatic keys per user.

All database interaction is delegated to the DAO layer in :mod:`core.db.dao`;
this module contains no SQL whatsoever.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import os
import threading
import traceback
from typing import Any, Dict, List, Optional, Tuple

from core.db.dao import ApiKeyDAO, UserDAO

logger = logging.getLogger(__name__)


class UserRegistry:
    """
    Singleton registry of users, roles, and API keys backed by the database.

    The constructor signature keeps the legacy ``users_file`` argument: it is
    now used ONLY as the source for the one-time JSON -> database migration.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, users_file: str = 'config/users.json',
                 reload_interval: int = 600):
        """
        Args:
            users_file: Legacy JSON file. Used only for the one-time migration
                        into the database when the users table is empty.
            reload_interval: Retained for API compatibility (unused - the
                        database is always the live source of truth).
        """
        if self._initialized:
            return
        self.users_file = users_file
        self.user_dao = UserDAO()
        self.api_key_dao = ApiKeyDAO()
        self.user_dao.ensure_default_roles()
        self._migrate_legacy_json_if_needed()
        self._ensure_bootstrap_admin()
        self._initialized = True
        logger.info("UserRegistry initialized (database-backed, %d users)",
                    self.user_dao.count_users())

    @classmethod
    def reset_instance(cls):
        """Drop the singleton (used by tests)."""
        with cls._lock:
            cls._instance = None

    # ------------------------------------------------------------------ #
    # Bootstrap / migration
    # ------------------------------------------------------------------ #

    def _migrate_legacy_json_if_needed(self) -> None:
        """
        One-time migration: if the users table is empty and the legacy JSON
        file exists, import every user (hashing the clear-text passwords).
        The JSON file is renamed to ``<file>.migrated`` afterwards so the
        clear-text passwords no longer linger on disk.
        """
        if self.user_dao.count_users() > 0:
            return
        if not self.users_file or not os.path.exists(self.users_file):
            return
        try:
            with open(self.users_file, 'r', encoding='utf-8') as fh:
                legacy = json.load(fh)
            migrated = 0
            for username, data in legacy.items():
                password = data.get('password')
                if not password:
                    logger.error(
                        "Legacy user '%s' has no password - skipped during "
                        "migration", username)
                    continue
                self.user_dao.create_user(
                    username=username,
                    password=password,
                    full_name=data.get('full_name', username),
                    roles=data.get('roles', ['user']),
                    created_by='legacy-json-migration',
                )
                migrated += 1
            os.rename(self.users_file, self.users_file + '.migrated')
            logger.info(
                "Migrated %d users from legacy %s into the database; the "
                "JSON file was renamed to %s.migrated",
                migrated, self.users_file, self.users_file)
        except Exception as exc:  # noqa: BLE001
            logger.error("Legacy users.json migration failed: %s", exc)
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            raise

    def _ensure_bootstrap_admin(self) -> None:
        """
        Guarantee at least one admin exists.  On a pristine database the
        well-known ``admin/admin123`` bootstrap account is created (matching
        the documented default credentials); operators must change it.
        """
        if self.user_dao.count_admins() == 0:
            if not self.user_dao.user_exists('admin'):
                self.user_dao.create_user(
                    username='admin',
                    password='admin123',
                    full_name='System Administrator',
                    roles=['admin', 'user'],
                    created_by='bootstrap',
                )
                logger.warning(
                    "Bootstrap 'admin' account created with the documented "
                    "default password - CHANGE IT IMMEDIATELY in production.")
            else:
                self.user_dao.add_role('admin', 'admin', 'bootstrap')

    # ------------------------------------------------------------------ #
    # Authentication / roles (legacy-compatible API)
    # ------------------------------------------------------------------ #

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Verify credentials; returns the user dict on success else None."""
        try:
            return self.user_dao.authenticate(username, password)
        except Exception as exc:  # noqa: BLE001
            logger.error("Authentication error for '%s': %s", username, exc)
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            raise

    def authenticate_api_key(self, clear_key: str) -> Optional[Dict[str, Any]]:
        """Verify an API key; returns the owning user dict on success."""
        return self.api_key_dao.verify_api_key(clear_key)

    def has_role(self, username: str, role: str) -> bool:
        """True when ``username`` holds ``role``."""
        if not username:
            return False
        user = self.user_dao.get_user(username)
        return bool(user) and role in user.get('roles', [])

    def has_any_role(self, username: str, roles: List[str]) -> bool:
        """True when ``username`` holds at least one role in ``roles``."""
        if not username:
            return False
        user = self.user_dao.get_user(username)
        if not user:
            return False
        return any(r in user.get('roles', []) for r in roles)

    def get_all_roles(self, username: str) -> List[str]:
        """All roles held by ``username`` (empty list when absent)."""
        user = self.user_dao.get_user(username)
        return user.get('roles', []) if user else []

    def list_role_catalogue(self) -> List[dict]:
        """The full role catalogue from the database."""
        return self.user_dao.list_roles()

    # ------------------------------------------------------------------ #
    # User CRUD (legacy-compatible API)
    # ------------------------------------------------------------------ #

    def create_user(self, username: str, user_data: Dict[str, Any],
                    created_by: str) -> bool:
        """
        Create a user from the legacy dict shape
        ``{'password': ..., 'full_name': ..., 'roles': [...]}``.
        Returns True on success; raises on database failure.
        """
        password = user_data.get('password')
        if not password:
            raise ValueError("user_data must include a 'password'")
        self.user_dao.create_user(
            username=username,
            password=password,
            full_name=user_data.get('full_name', username),
            roles=user_data.get('roles', ['user']),
            created_by=created_by,
        )
        return True

    def modify_user(self, username: str, user_data: Dict[str, Any],
                    modified_by: str) -> bool:
        """Update password / full_name / roles. Returns True on success."""
        self.user_dao.update_user(
            username=username,
            password=user_data.get('password'),
            full_name=user_data.get('full_name'),
            roles=user_data.get('roles'),
            modified_by=modified_by,
        )
        return True

    def delete_user(self, username: str, deleted_by: str) -> bool:
        """
        Delete a user.  Refuses (returns False) to delete the last remaining
        admin, mirroring legacy behaviour.
        """
        if self.has_role(username, 'admin') and self.user_dao.count_admins() <= 1:
            logger.warning("Refusing to delete '%s': last remaining admin",
                           username)
            return False
        self.user_dao.delete_user(username, deleted_by)
        return True

    def add_role(self, username: str, role: str, modified_by: str) -> bool:
        """Grant a role. Returns True on success."""
        self.user_dao.add_role(username, role, modified_by)
        return True

    def revoke_role(self, username: str, role: str, modified_by: str) -> bool:
        """
        Revoke a role.  Refuses (returns False) to strip 'admin' from the
        last remaining admin.
        """
        if role == 'admin' and self.has_role(username, 'admin') \
                and self.user_dao.count_admins() <= 1:
            logger.warning("Refusing to revoke admin from '%s': last admin",
                           username)
            return False
        self.user_dao.revoke_role(username, role, modified_by)
        return True

    def get_user(self, username: str,
                 include_password: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch a user dict.  ``include_password`` is accepted for backward
        compatibility but clear passwords no longer exist - only hashes are
        stored and they are never exposed.
        """
        return self.user_dao.get_user(username)

    def list_all_users(self, include_passwords: bool = False) -> Dict[str, Dict[str, Any]]:
        """All users keyed by username. Passwords are never exposed."""
        return self.user_dao.list_users()

    def get_user_count(self) -> int:
        """Total user count."""
        return self.user_dao.count_users()

    def user_exists(self, username: str) -> bool:
        """True when the username exists."""
        return self.user_dao.user_exists(username)

    # ------------------------------------------------------------------ #
    # API keys (new in v2.0.0)
    # ------------------------------------------------------------------ #

    def create_api_key(self, username: str, key_name: str,
                       created_by: str, expires_at=None) -> Tuple[str, dict]:
        """Issue a new API key; returns (clear_key_shown_once, record).

        ``expires_at`` is an optional naive-UTC datetime after which the key is
        rejected; ``None`` means the key never expires.
        """
        return self.api_key_dao.create_api_key(username, key_name, created_by,
                                               expires_at=expires_at)

    def list_api_keys(self, username: str) -> List[dict]:
        """List API key records for a user (hashes are never exposed)."""
        return self.api_key_dao.list_api_keys(username)

    def revoke_api_key(self, key_id: int, revoked_by: str) -> None:
        """Deactivate an API key."""
        self.api_key_dao.revoke_api_key(key_id, revoked_by)

    def delete_api_key(self, key_id: int, deleted_by: str) -> None:
        """Permanently delete an API key."""
        self.api_key_dao.delete_api_key(key_id, deleted_by)

    # ------------------------------------------------------------------ #
    # Legacy no-op shims
    # ------------------------------------------------------------------ #

    def force_reload(self) -> bool:
        """Legacy shim: the database is always live, nothing to reload."""
        logger.info("force_reload requested - database registry is always "
                    "current; no action required")
        return True

    def stop_reload(self) -> None:
        """Legacy shim: there is no background reload thread anymore."""
