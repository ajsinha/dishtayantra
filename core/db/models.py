"""
SQLAlchemy ORM Models - User / Role / API Key Domain
====================================================

All persistent security entities for DishtaYantra v2.0.0 live here:

    users      - login accounts (passwords stored as PBKDF2-SHA256 hashes)
    roles      - role catalogue (admin / operator / user, extensible)
    user_roles - many-to-many association between users and roles
    api_keys   - programmatic access keys bound to a user

These models intentionally mirror the DDL shipped in
``config/schema/schema_sqlite.sql`` and ``config/schema/schema_postgres.sql``.
There is deliberately **no** migration framework: SQLite databases are created
automatically from the model metadata on startup, while PostgreSQL databases
are provisioned once by executing the PostgreSQL schema file.

All database access goes through :mod:`core.db.dao` - no SQL anywhere else.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    """Declarative base shared by every DishtaYantra ORM model."""


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
