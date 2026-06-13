"""
Database Manager - Engine and Session Lifecycle
===============================================

Central owner of the SQLAlchemy engine and session factory for the
user/role/API-key domain.  Selection of the backend is configuration driven:

    db.engine = sqlite | postgresql            (REQUIRED - no default)

SQLite mode (default shipped configuration) requires:

    db.sqlite.path                              Path to the .db file. Tables
                                                are created AUTOMATICALLY from
                                                the ORM metadata on first run
                                                (mirrors config/schema/
                                                schema_sqlite.sql).

PostgreSQL mode requires:

    db.postgres.host
    db.postgres.port
    db.postgres.database
    db.postgres.user
    db.postgres.password

For PostgreSQL the schema is *not* auto-created; provision it once with::

    psql -U <user> -d <database> -f config/schema/schema_postgres.sql

In accordance with the v2.0.0 architecture mandate, **missing required
properties are fatal** - the system crashes at startup with a detailed error
naming the missing key, rather than guessing a value.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import os
import threading
import traceback
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.db.models import Base

logger = logging.getLogger(__name__)


class DatabaseConfigurationError(Exception):
    """Raised when database configuration is missing or invalid."""


class DatabaseManager:
    """
    Singleton owning the SQLAlchemy engine + session factory.

    Usage::

        db = DatabaseManager.get_instance()
        with db.session_scope() as session:
            ...
    """

    _instance = None
    _lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Construction
    # ------------------------------------------------------------------ #

    def __init__(self, props=None, echo: bool = False):
        """
        Build the engine from configuration.

        Args:
            props: PropertiesConfigurator (global singleton when None).
            echo:  Enable SQLAlchemy statement echo (debug only).

        Raises:
            DatabaseConfigurationError: when required properties are absent.
        """
        if props is None:
            from core.properties_configurator import PropertiesConfigurator
            props = PropertiesConfigurator()
        self._props = props

        engine_name = self._require("db.engine").strip().lower()
        if engine_name == "sqlite":
            url = self._build_sqlite_url()
            self.engine = create_engine(
                url, echo=echo, connect_args={"check_same_thread": False}
            )
            self._auto_create_sqlite_schema()
        elif engine_name in ("postgres", "postgresql"):
            url = self._build_postgres_url()
            self.engine = create_engine(url, echo=echo, pool_pre_ping=True)
            logger.info(
                "PostgreSQL engine created. Schema is NOT auto-created; "
                "apply config/schema/schema_postgres.sql once if not done."
            )
        else:
            raise DatabaseConfigurationError(
                f"Unsupported db.engine '{engine_name}'. "
                f"Supported values: sqlite, postgresql"
            )

        self.engine_name = engine_name
        self._session_factory = sessionmaker(
            bind=self.engine, expire_on_commit=False
        )
        logger.info("DatabaseManager initialized (engine=%s)", engine_name)

    @classmethod
    def get_instance(cls, props=None) -> "DatabaseManager":
        """Return the process-wide singleton, building it lazily."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls(props=props)
        return cls._instance

    @classmethod
    def reset_instance(cls) -> None:
        """Drop the singleton (used by tests)."""
        with cls._lock:
            if cls._instance is not None:
                try:
                    cls._instance.engine.dispose()
                except Exception:  # noqa: BLE001 - log fully, never swallow silently
                    logger.error("Error disposing engine:\n%s", traceback.format_exc())
            cls._instance = None

    @classmethod
    def set_instance(cls, instance: "DatabaseManager") -> "DatabaseManager":
        """
        Install ``instance`` as the process-wide singleton (dependency
        injection for tests / embedders).  Returns the instance.
        """
        with cls._lock:
            cls._instance = instance
        return instance

    # ------------------------------------------------------------------ #
    # Configuration helpers
    # ------------------------------------------------------------------ #

    def _require(self, key: str) -> str:
        """Fetch a required property or raise a detailed configuration error."""
        value = self._props.get(key)
        if value is None:
            raise DatabaseConfigurationError(
                f"Required database property '{key}' is missing from "
                f"application.properties (or the environment). The system "
                f"will not default this value - define '{key}' and restart."
            )
        return value

    def _build_sqlite_url(self) -> str:
        """Build the SQLite URL and ensure the parent directory exists."""
        path = self._require("db.sqlite.path")
        path = os.path.abspath(os.path.expanduser(os.path.expandvars(path)))
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        logger.info("SQLite database file: %s", path)
        return f"sqlite:///{path}"

    def _build_postgres_url(self) -> str:
        """Build the PostgreSQL URL from the five required properties."""
        host = self._require("db.postgres.host")
        port = self._require("db.postgres.port")
        database = self._require("db.postgres.database")
        user = self._require("db.postgres.user")
        password = self._require("db.postgres.password")
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

    def _auto_create_sqlite_schema(self) -> None:
        """
        SQLite mode: create all tables automatically from the ORM metadata.
        This mirrors config/schema/schema_sqlite.sql exactly, so the schema
        file remains the human-readable reference while runtime creation is
        guaranteed to match the models.
        """
        try:
            Base.metadata.create_all(self.engine)
            logger.info("SQLite schema verified/created automatically")
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to auto-create SQLite schema: %s", exc)
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            raise

    # ------------------------------------------------------------------ #
    # Session management
    # ------------------------------------------------------------------ #

    def create_session(self):
        """Create a raw session (caller owns commit/rollback/close)."""
        return self._session_factory()

    @contextmanager
    def session_scope(self):
        """
        Transactional scope: commits on success, rolls back and re-raises on
        failure (with the full stack trace logged - never swallowed).
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as exc:  # noqa: BLE001
            session.rollback()
            logger.error("Database transaction failed: %s", exc)
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            raise
        finally:
            session.close()

    def details(self) -> dict:
        """Diagnostic information for monitoring UIs."""
        return {
            "engine": self.engine_name,
            "url": str(self.engine.url).replace(
                self.engine.url.password or "", "***"
            ),
        }
