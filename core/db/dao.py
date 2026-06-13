"""
Data Access Objects - The ONLY Database Access Layer
====================================================

Every read or write against the user/role/API-key schema is concentrated in
this module.  Nothing outside :mod:`core.db` issues SQL or touches the ORM
session - higher layers (e.g. :mod:`core.user_registry`) call the DAOs only.

Contents:
    :class:`PasswordHasher` - PBKDF2-SHA256 hashing/verification helper
    :class:`UserDAO`        - users + roles CRUD and authentication queries
    :class:`ApiKeyDAO`      - API key issuing, verification, revocation

Error handling policy (architecture mandate): no exception is swallowed;
failures are logged with full stack traces by ``DatabaseManager.session_scope``
and propagate to the caller.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import hashlib
import hmac
import logging
import secrets
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select

from core.db.database_manager import DatabaseManager
from core.db.models import ApiKey, Role, User

logger = logging.getLogger(__name__)

DEFAULT_ROLES = (
    ("admin", "Full administrative access"),
    ("operator", "Operational control of DAGs"),
    ("user", "Read-only / basic access"),
)


class PasswordHasher:
    """PBKDF2-SHA256 password hashing with per-password random salt."""

    ITERATIONS = 240_000
    ALGORITHM = "pbkdf2_sha256"

    @classmethod
    def hash_password(cls, password: str) -> str:
        """Return ``pbkdf2_sha256$<iterations>$<salt-hex>$<hash-hex>``."""
        salt = secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt), cls.ITERATIONS
        ).hex()
        return f"{cls.ALGORITHM}${cls.ITERATIONS}${salt}${digest}"

    @classmethod
    def verify_password(cls, password: str, stored: str) -> bool:
        """Constant-time verification of ``password`` against ``stored``."""
        try:
            algorithm, iterations, salt, digest = stored.split("$", 3)
            if algorithm != cls.ALGORITHM:
                logger.error("Unknown password hash algorithm: %s", algorithm)
                return False
            candidate = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"),
                bytes.fromhex(salt), int(iterations)
            ).hex()
            return hmac.compare_digest(candidate, digest)
        except ValueError as exc:
            logger.error("Malformed stored password hash: %s", exc, exc_info=True)
            return False


class UserDAO:
    """All user/role persistence operations."""

    def __init__(self, db: DatabaseManager = None):
        self.db = db or DatabaseManager.get_instance()

    # ----------------------------- roles ----------------------------- #

    def ensure_default_roles(self) -> None:
        """Insert the built-in role catalogue if missing (idempotent)."""
        with self.db.session_scope() as session:
            for name, description in DEFAULT_ROLES:
                existing = session.execute(
                    select(Role).where(Role.name == name)
                ).scalar_one_or_none()
                if existing is None:
                    session.add(Role(name=name, description=description))
                    logger.info("Created default role '%s'", name)

    def _resolve_roles(self, session, role_names: List[str]) -> List[Role]:
        """Resolve role names to Role rows, creating unknown roles on the fly."""
        roles: List[Role] = []
        for name in role_names:
            role = session.execute(
                select(Role).where(Role.name == name)
            ).scalar_one_or_none()
            if role is None:
                role = Role(name=name, description=f"Auto-created role '{name}'")
                session.add(role)
                session.flush()
                logger.info("Auto-created role '%s'", name)
            roles.append(role)
        return roles

    def list_roles(self) -> List[dict]:
        """Return the full role catalogue."""
        with self.db.session_scope() as session:
            roles = session.execute(select(Role).order_by(Role.name)).scalars().all()
            return [{"name": r.name, "description": r.description} for r in roles]

    # ----------------------------- users ----------------------------- #

    def create_user(self, username: str, password: str, full_name: str,
                    roles: List[str], created_by: str) -> dict:
        """
        Create a new user.  Raises ValueError when the username is taken.
        Returns the created user's dict representation.
        """
        with self.db.session_scope() as session:
            existing = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if existing is not None:
                raise ValueError(f"User '{username}' already exists")
            user = User(
                username=username,
                password_hash=PasswordHasher.hash_password(password),
                full_name=full_name or username,
                created_by=created_by,
                created_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
            user.roles = self._resolve_roles(session, roles)
            session.add(user)
            session.flush()
            logger.info("User '%s' created by '%s' with roles %s",
                        username, created_by, roles)
            return user.to_dict()

    def get_user(self, username: str) -> Optional[dict]:
        """Fetch a user dict (None when absent)."""
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            return user.to_dict() if user else None

    def list_users(self) -> Dict[str, dict]:
        """Return all users keyed by username (legacy registry shape)."""
        with self.db.session_scope() as session:
            users = session.execute(select(User).order_by(User.username)).scalars().all()
            return {u.username: u.to_dict() for u in users}

    def count_users(self) -> int:
        """Total user count."""
        with self.db.session_scope() as session:
            return len(session.execute(select(User.id)).scalars().all())

    def count_admins(self) -> int:
        """Number of users holding the 'admin' role."""
        with self.db.session_scope() as session:
            users = session.execute(select(User)).scalars().all()
            return sum(1 for u in users if "admin" in u.role_names)

    def user_exists(self, username: str) -> bool:
        """True when the username exists."""
        return self.get_user(username) is not None

    def authenticate(self, username: str, password: str) -> Optional[dict]:
        """
        Verify credentials.  Returns the user dict on success, None otherwise.
        Inactive users always fail authentication.
        """
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None or not user.is_active:
                return None
            if PasswordHasher.verify_password(password, user.password_hash):
                return user.to_dict()
            return None

    def update_user(self, username: str, password: Optional[str],
                    full_name: Optional[str], roles: Optional[List[str]],
                    modified_by: str) -> dict:
        """
        Update password / full_name / roles for ``username``.  ``None`` fields
        are left unchanged.  Raises ValueError when the user does not exist.
        """
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            if password:
                user.password_hash = PasswordHasher.hash_password(password)
            if full_name:
                user.full_name = full_name
            if roles is not None:
                user.roles = self._resolve_roles(session, roles)
            user.modified_at = datetime.now(timezone.utc).replace(tzinfo=None)
            user.modified_by = modified_by
            session.flush()
            logger.info("User '%s' updated by '%s'", username, modified_by)
            return user.to_dict()

    def delete_user(self, username: str, deleted_by: str) -> None:
        """Delete a user.  Raises ValueError when the user does not exist."""
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            session.delete(user)
            logger.info("User '%s' deleted by '%s'", username, deleted_by)

    def add_role(self, username: str, role_name: str, modified_by: str) -> dict:
        """Grant ``role_name`` to ``username`` (idempotent)."""
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            if role_name not in user.role_names:
                user.roles.extend(self._resolve_roles(session, [role_name]))
                user.modified_at = datetime.now(timezone.utc).replace(tzinfo=None)
                user.modified_by = modified_by
                logger.info("Role '%s' granted to '%s' by '%s'",
                            role_name, username, modified_by)
            return user.to_dict()

    def revoke_role(self, username: str, role_name: str, modified_by: str) -> dict:
        """Revoke ``role_name`` from ``username`` (idempotent)."""
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            user.roles = [r for r in user.roles if r.name != role_name]
            user.modified_at = datetime.now(timezone.utc).replace(tzinfo=None)
            user.modified_by = modified_by
            logger.info("Role '%s' revoked from '%s' by '%s'",
                        role_name, username, modified_by)
            return user.to_dict()


class ApiKeyDAO:
    """All API key persistence operations."""

    KEY_PREFIX = "dyk"  # DishtaYantra Key

    def __init__(self, db: DatabaseManager = None):
        self.db = db or DatabaseManager.get_instance()

    @staticmethod
    def _hash_key(clear_key: str) -> str:
        """SHA-256 hex digest of the clear API key."""
        return hashlib.sha256(clear_key.encode("utf-8")).hexdigest()

    def create_api_key(self, username: str, key_name: str,
                       created_by: str,
                       expires_at: Optional[datetime] = None) -> Tuple[str, dict]:
        """
        Issue a new API key for ``username``.

        Returns:
            (clear_key, key_record_dict).  The clear key is shown exactly once
            and only the hash is persisted.

        Raises:
            ValueError: when the user does not exist.
        """
        clear_key = f"{self.KEY_PREFIX}_{secrets.token_urlsafe(32)}"
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            record = ApiKey(
                user_id=user.id,
                name=key_name,
                key_hash=self._hash_key(clear_key),
                key_prefix=clear_key[:12],
                created_by=created_by,
                expires_at=expires_at,
            )
            session.add(record)
            session.flush()
            logger.info("API key '%s' created for '%s' by '%s'",
                        key_name, username, created_by)
            return clear_key, record.to_dict()

    def verify_api_key(self, clear_key: str) -> Optional[dict]:
        """
        Verify an API key.  Returns the owning user's dict on success
        (and stamps ``last_used_at``); None on failure / expiry / inactive.
        """
        key_hash = self._hash_key(clear_key)
        with self.db.session_scope() as session:
            record = session.execute(
                select(ApiKey).where(ApiKey.key_hash == key_hash)
            ).scalar_one_or_none()
            if record is None or not record.is_active:
                return None
            if record.expires_at and record.expires_at < datetime.now(timezone.utc).replace(tzinfo=None):
                logger.warning("Rejected expired API key '%s'", record.key_prefix)
                return None
            record.last_used_at = datetime.now(timezone.utc).replace(tzinfo=None)
            user = session.get(User, record.user_id)
            if user is None or not user.is_active:
                return None
            return user.to_dict()

    def list_api_keys(self, username: str) -> List[dict]:
        """List the API key records for ``username`` (no hashes exposed)."""
        with self.db.session_scope() as session:
            user = session.execute(
                select(User).where(User.username == username)
            ).scalar_one_or_none()
            if user is None:
                raise ValueError(f"User '{username}' not found")
            return [key.to_dict() for key in user.api_keys]

    def revoke_api_key(self, key_id: int, revoked_by: str) -> None:
        """Deactivate an API key by id. Raises ValueError when missing."""
        with self.db.session_scope() as session:
            record = session.get(ApiKey, key_id)
            if record is None:
                raise ValueError(f"API key id {key_id} not found")
            record.is_active = False
            logger.info("API key id=%s revoked by '%s'", key_id, revoked_by)

    def delete_api_key(self, key_id: int, deleted_by: str) -> None:
        """Permanently delete an API key by id."""
        with self.db.session_scope() as session:
            record = session.get(ApiKey, key_id)
            if record is None:
                raise ValueError(f"API key id {key_id} not found")
            session.delete(record)
            logger.info("API key id=%s deleted by '%s'", key_id, deleted_by)
