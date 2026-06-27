"""
SQLAlchemy ORM Models - User / Role / API Key Domain
====================================================

All persistent security entities for DishtaYantra v2.0.0 live here:

    users      - login accounts (passwords stored as PBKDF2-SHA256 hashes)
    roles      - role catalogue (admin / operator / user, extensible)
    user_roles - many-to-many association between users and roles
    api_keys   - programmatic access keys bound to a user

These models map onto the DDL shipped in
``config/schema/schema_sqlite.sql`` and ``config/schema/schema_postgres.sql``,
which are the **single source of truth** for the schema. There is deliberately
**no** migration framework: on startup the SQLite database is created by
applying ``schema_sqlite.sql`` verbatim (the models map to it for queries only),
and PostgreSQL is provisioned once by executing ``schema_postgres.sql``. A
database that predates a schema change is recreated, never migrated.

All database access goes through :mod:`core.db.dao` - no SQL anywhere else.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Declarative base for the **application** database (users, roles, API
    keys, audit, trusted servers). The app DB is created from this metadata via
    create_all (SQLite) / config/schema/schema_*.sql (Postgres)."""


class FlowBase(DeclarativeBase):
    """Separate declarative base for the **flow-history** store. Kept distinct
    from ``Base`` on purpose: flow history lives in its OWN database, so
    ``FlowEvent`` must NOT be created in the application DB by the app's
    create_all. Its schema is the dedicated config/schema/flow_events_*.sql."""


# Association table: users <-> roles (no extra payload, plain link table)
user_roles_table = Table(
    "user_roles",
    Base.metadata,
    Column("user_id", Integer, ForeignKey("users.id", ondelete="CASCADE"),
           primary_key=True),
    Column("role_id", Integer, ForeignKey("roles.id", ondelete="CASCADE"),
           primary_key=True),
)


class Role(Base):
    """A named role granting capabilities in the web UI / API."""

    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False, unique=True)
    description = Column(String(255), nullable=True)

    users = relationship("User", secondary=user_roles_table,
                         back_populates="roles")

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<Role name={self.name}>"


class User(Base):
    """A login account. Passwords are stored as PBKDF2-SHA256 hashes only."""

    __tablename__ = "users"
    __table_args__ = (UniqueConstraint("username", name="uq_users_username"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(128), nullable=False, index=True)
    password_hash = Column(String(512), nullable=False)
    full_name = Column(String(255), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_by = Column(String(128), nullable=True)
    modified_at = Column(DateTime, nullable=True)
    modified_by = Column(String(128), nullable=True)

    roles = relationship("Role", secondary=user_roles_table,
                         back_populates="users", lazy="selectin")
    api_keys = relationship("ApiKey", back_populates="user",
                            cascade="all, delete-orphan", lazy="selectin")

    @property
    def role_names(self) -> list:
        """Return role names sorted alphabetically for stable output."""
        return sorted(role.name for role in self.roles)

    def to_dict(self, include_keys: bool = False) -> dict:
        """Serialize to the dict shape the legacy JSON registry exposed."""
        data = {
            "username": self.username,
            "full_name": self.full_name or self.username,
            "roles": self.role_names,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "created_by": self.created_by,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
            "modified_by": self.modified_by,
        }
        if include_keys:
            data["api_keys"] = [key.to_dict() for key in self.api_keys]
        return data

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<User username={self.username} roles={self.role_names}>"


class ApiKey(Base):
    """A programmatic API key bound to a user account."""

    __tablename__ = "api_keys"
    __table_args__ = (UniqueConstraint("key_hash", name="uq_api_keys_key_hash"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),
                     nullable=False, index=True)
    name = Column(String(128), nullable=False)
    # Only a SHA-256 hash of the key material is persisted; the clear key is
    # shown exactly once at creation time.
    key_hash = Column(String(128), nullable=False, index=True)
    key_prefix = Column(String(16), nullable=False)  # for display: "dyk_ab12..."
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_by = Column(String(128), nullable=True)
    last_used_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="api_keys")

    def to_dict(self) -> dict:
        """Serialize for the management UI (never exposes the hash)."""
        return {
            "id": self.id,
            "name": self.name,
            "key_prefix": self.key_prefix,
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "created_by": self.created_by,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<ApiKey name={self.name} prefix={self.key_prefix}>"


class AuditEvent(Base):
    """An append-only record of a security/admin-relevant action.

    Audit rows are never updated or deleted by the application; they capture
    who did what, when, and from where, for accountability and forensics.
    """

    __tablename__ = "audit_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow,
                        index=True)
    actor = Column(String(128), nullable=True, index=True)   # username or 'system'
    action = Column(String(64), nullable=False, index=True)  # e.g. 'apikey.create'
    target = Column(String(256), nullable=True)              # subject of the action
    detail = Column(Text, nullable=True)                     # human-readable note
    source_ip = Column(String(64), nullable=True)
    success = Column(Boolean, nullable=False, default=True)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "actor": self.actor,
            "action": self.action,
            "target": self.target,
            "detail": self.detail,
            "source_ip": self.source_ip,
            "success": self.success,
        }

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<AuditEvent {self.action} actor={self.actor}>"


class FlowEvent(FlowBase):
    """An append-only record of one node-fire, for Flow Time-Travel.

    Captured by the engine's equality gate (a node only fires when its output
    changes), so these rows are the DAG's compact change-log. Written in
    batches by core.flow_recorder; never updated; purged by age via
    core.flow_retention. Indexed on (dag_id, ts_ms) for fast window queries.
    """

    __tablename__ = "flow_events"
    __table_args__ = (
        Index("ix_flow_dag_ts", "dag_id", "ts_ms"),
        Index("ix_flow_dag_node_ts", "dag_id", "node_id", "ts_ms"),
        # Provenance index: when many instances write to one shared flow DB,
        # queries scope by origin first.
        Index("ix_flow_instance_dag_ts", "instance", "dag_id", "ts_ms"),
        Index("ix_flow_dag_cycle", "dag_id", "cycle_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    dag_id = Column(String(128), nullable=False)
    node_id = Column(String(128), nullable=False)
    seq = Column(Integer, nullable=False)        # per-instance monotonic order
    ts_ms = Column(Integer, nullable=False)       # epoch milliseconds
    inputs_json = Column(Text, nullable=True)     # bounded JSON snapshot
    output_json = Column(Text, nullable=True)     # bounded JSON snapshot
    targets_json = Column(Text, nullable=True)    # downstream node names (JSON)
    compute_us = Column(Integer, nullable=True)   # optional compute time
    # Compute-cycle group (v5.42.0): all fires produced by one engine sweep
    # (one start-to-finish propagation wave) share a cycle_id, so a window can be
    # grouped and inspected one whole cycle at a time. Nullable (older rows null).
    cycle_id = Column(Integer, nullable=True)
    # Provenance (v5.34.0): which instance produced this event. Lets several
    # servers safely share ONE flow database (e.g. a central Postgres) and lets
    # queries disambiguate origin. Nullable for backward compatibility.
    instance = Column(String(128), nullable=True)
    host = Column(String(255), nullable=True)
    port = Column(Integer, nullable=True)

    def to_dict(self) -> dict:
        import json as _json

        def _load(s):
            if s is None:
                return None
            try:
                return _json.loads(s)
            except Exception:  # noqa: BLE001
                return s

        return {
            "id": self.id, "dag_id": self.dag_id, "node_id": self.node_id,
            "seq": self.seq, "ts_ms": self.ts_ms,
            "inputs": _load(self.inputs_json), "output": _load(self.output_json),
            "targets": _load(self.targets_json) or [], "compute_us": self.compute_us,
            "cycle_id": self.cycle_id,
            "instance": self.instance, "host": self.host, "port": self.port,
        }

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<FlowEvent {self.dag_id}/{self.node_id} seq={self.seq}>"


class TrustedServer(Base):
    """A trusted remote DishtaYantra instance this UI plane may manage.

    Part of the UI-plane / service-plane split (v5.29.0). An admin registers
    another instance by URL + a ``dyk_`` API key minted on that instance; the
    key is stored symmetric-encrypted (``api_key_enc``; see
    ``core.service.crypto``) and never exposed in the clear via API or UI. The
    ``role`` records whether the supplied key is an 'admin' (full lifecycle) or
    'user' (read-only) key, so the UI can gate mutating controls. Probe columns
    cache the last reachability/version check against the remote's
    ``/api/service/info``.
    """

    __tablename__ = "trusted_servers"
    __table_args__ = (
        Index("ix_trusted_server_id", "server_id", unique=True),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    server_id = Column(String(64), nullable=False, unique=True)  # stable handle
    name = Column(String(128), nullable=False)                   # display label
    url = Column(String(512), nullable=False)
    api_key_enc = Column(Text, nullable=False)                   # encrypted key
    role = Column(String(16), nullable=False, default="admin")   # admin | user
    verify_tls = Column(Boolean, nullable=False, default=True)
    added_by = Column(String(128), nullable=True)
    added_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    last_probe_at = Column(DateTime, nullable=True)
    last_probe_ok = Column(Boolean, nullable=True)
    last_probe_version = Column(String(32), nullable=True)

    def to_dict(self) -> dict:
        """Safe dict for API/UI: NEVER includes the key (not even encrypted)."""
        return {
            "server_id": self.server_id,
            "name": self.name,
            "url": self.url,
            "role": self.role,
            "verify_tls": self.verify_tls,
            "added_by": self.added_by,
            "added_at": self.added_at.isoformat() if self.added_at else None,
            "last_probe_at": (self.last_probe_at.isoformat()
                              if self.last_probe_at else None),
            "last_probe_ok": self.last_probe_ok,
            "last_probe_version": self.last_probe_version,
        }

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<TrustedServer {self.server_id} {self.url}>"
