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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from sqlalchemy import select, desc, delete, func, asc, and_

from core.db.database_manager import DatabaseManager
from core.db.models import ApiKey, AuditEvent, FlowEvent, Role, User

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


class AuditDAO:
    """Append-only access to the audit trail (record + query)."""

    def __init__(self, db: Optional[DatabaseManager] = None):
        self.db = db or DatabaseManager.get_instance()

    def record(self, action: str, actor: Optional[str] = None,
               target: Optional[str] = None, detail: Optional[str] = None,
               source_ip: Optional[str] = None, success: bool = True) -> None:
        """Insert one audit row. Callers should not let auditing break the
        primary operation; see core.audit_log.audit for the safe wrapper."""
        with self.db.session_scope() as session:
            session.add(AuditEvent(
                action=action, actor=actor, target=target, detail=detail,
                source_ip=source_ip, success=success,
                created_at=datetime.now(timezone.utc).replace(tzinfo=None)))

    def list_events(self, limit: int = 200, offset: int = 0,
                    actor: Optional[str] = None, action: Optional[str] = None,
                    success: Optional[bool] = None) -> List[dict]:
        """Most-recent-first, with optional filters."""
        with self.db.session_scope() as session:
            stmt = select(AuditEvent)
            if actor:
                stmt = stmt.where(AuditEvent.actor == actor)
            if action:
                stmt = stmt.where(AuditEvent.action == action)
            if success is not None:
                stmt = stmt.where(AuditEvent.success == success)
            stmt = stmt.order_by(desc(AuditEvent.created_at),
                                 desc(AuditEvent.id)).limit(limit).offset(offset)
            return [e.to_dict() for e in session.execute(stmt).scalars().all()]

    def purge_older_than(self, days: int) -> int:
        """Delete audit rows older than ``days`` days; returns the count removed.

        ``days`` <= 0 (or None) disables purging and keeps everything.
        """
        if not days or days <= 0:
            return 0
        cutoff = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - timedelta(days=days))
        with self.db.session_scope() as session:
            result = session.execute(
                delete(AuditEvent).where(AuditEvent.created_at < cutoff))
            return result.rowcount or 0

    def distinct_actions(self) -> List[str]:
        """The set of action names seen so far (for filter dropdowns)."""
        with self.db.session_scope() as session:
            rows = session.execute(
                select(AuditEvent.action).distinct().order_by(AuditEvent.action)
            ).scalars().all()
            return list(rows)


class FlowDAO:
    """Append-only access to recorded flow events (Flow Time-Travel).

    Mirrors AuditDAO: all SQL is here, every call goes through
    ``DatabaseManager.session_scope``. Rows are written in batches by the
    recorder's drain thread and read back as time windows by the API.
    """

    def __init__(self, db: Optional[DatabaseManager] = None):
        self.db = db or DatabaseManager.get_instance()

    def write_batch(self, rows: List[dict]) -> None:
        """Insert a batch of already-serialized event rows."""
        if not rows:
            return
        with self.db.session_scope() as session:
            session.add_all([FlowEvent(
                dag_id=r["dag_id"], node_id=r["node_id"], seq=r["seq"],
                ts_ms=r["ts_ms"], inputs_json=r.get("inputs_json"),
                output_json=r.get("output_json"),
                targets_json=r.get("targets_json"),
                compute_us=r.get("compute_us"),
                instance=r.get("instance"), host=r.get("host"),
                port=r.get("port")) for r in rows])

    def query(self, dag_id: str, t0_ms: Optional[int] = None,
              t1_ms: Optional[int] = None, nodes: Optional[List[str]] = None,
              limit: int = 5000, after_seq: Optional[int] = None,
              instance: Optional[str] = None) -> List[dict]:
        with self.db.session_scope() as session:
            stmt = select(FlowEvent).where(FlowEvent.dag_id == dag_id)
            if instance is not None:
                stmt = stmt.where(FlowEvent.instance == instance)
            if t0_ms is not None:
                stmt = stmt.where(FlowEvent.ts_ms >= int(t0_ms))
            if t1_ms is not None:
                stmt = stmt.where(FlowEvent.ts_ms <= int(t1_ms))
            if nodes:
                stmt = stmt.where(FlowEvent.node_id.in_(nodes))
            if after_seq is not None:
                stmt = stmt.where(FlowEvent.seq > int(after_seq))
            stmt = stmt.order_by(asc(FlowEvent.seq)).limit(int(limit))
            return [e.to_dict() for e in session.execute(stmt).scalars().all()]

    def state_at(self, dag_id: str, ts_ms: int,
                 instance: Optional[str] = None) -> dict:
        """Latest output per node at or before ``ts_ms`` (state reconstruction)."""
        with self.db.session_scope() as session:
            where = [FlowEvent.dag_id == dag_id, FlowEvent.ts_ms <= int(ts_ms)]
            if instance is not None:
                where.append(FlowEvent.instance == instance)
            sub = (select(FlowEvent.node_id,
                          func.max(FlowEvent.ts_ms).label("mts"))
                   .where(*where)
                   .group_by(FlowEvent.node_id).subquery())
            outer = [FlowEvent.dag_id == dag_id]
            if instance is not None:
                outer.append(FlowEvent.instance == instance)
            stmt = (select(FlowEvent).join(
                sub, and_(FlowEvent.node_id == sub.c.node_id,
                          FlowEvent.ts_ms == sub.c.mts))
                .where(*outer))
            nodes = {e.node_id: e.to_dict()
                     for e in session.execute(stmt).scalars().all()}
            return {"ts_ms": int(ts_ms), "nodes": nodes}

    def purge_older_than_ms(self, cutoff_ms: int, batch: int = 5000) -> int:
        """Delete flow rows older than ``cutoff_ms`` in bounded batches.

        Each batch is its own committed transaction, so a large sweep never
        holds one long write lock (which would starve the recorder's drain
        thread). Returns the total count removed.
        """
        cutoff = int(cutoff_ms)
        batch = max(int(batch), 1)
        total = 0
        while True:
            with self.db.session_scope() as session:
                sub = (select(FlowEvent.id)
                       .where(FlowEvent.ts_ms < cutoff)
                       .order_by(FlowEvent.id).limit(batch))
                result = session.execute(
                    delete(FlowEvent).where(FlowEvent.id.in_(sub)))
                n = result.rowcount or 0
            total += n
            if n < batch:
                break
        return total

    def distinct_dags(self) -> List[dict]:
        with self.db.session_scope() as session:
            rows = session.execute(
                select(FlowEvent.instance, FlowEvent.dag_id,
                       func.count().label("n"),
                       func.min(FlowEvent.ts_ms).label("mn"),
                       func.max(FlowEvent.ts_ms).label("mx"))
                .group_by(FlowEvent.instance, FlowEvent.dag_id)
                .order_by(FlowEvent.instance, FlowEvent.dag_id)).all()
            return [{"instance": r[0], "dag_id": r[1], "count": r[2],
                     "min_ts": r[3], "max_ts": r[4]} for r in rows]
