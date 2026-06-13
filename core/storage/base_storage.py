"""
Storage Provider Abstraction - Base Classes
===========================================

Defines the abstract :class:`StorageProvider` contract used everywhere the
system needs to persist or read configuration artifacts (DAG JSON files,
external module payloads, documents, exports, ...).

Concrete implementations:
    - :mod:`core.storage.filesystem_storage` - local file system
    - :mod:`core.storage.s3_storage`         - Amazon S3 (boto3)
    - :mod:`core.storage.azureblob_storage`  - Azure Blob Storage
    - :mod:`core.storage.gcs_storage`        - Google Cloud Storage

The active provider is selected via the ``storage.provider`` property (see
:mod:`core.storage.storage_factory`).  All paths handed to a provider are
*relative* logical paths (e.g. ``dags/sample_dag.json``); each provider maps
them onto its own namespace (directory root, bucket+prefix, container+prefix).

Design principles enforced here (per the v2.0.0 architecture mandate):
    1. No exception is ever swallowed - every failure is logged with a full
       stack trace and re-raised as a :class:`StorageError` carrying context.
    2. No silent defaulting of required configuration - missing properties
       raise :class:`StorageConfigurationError` with a detailed message.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)


class StorageError(Exception):
    """Raised when a storage operation fails. Always carries full context."""


class StorageConfigurationError(StorageError):
    """Raised when required storage configuration is missing or invalid."""


class StorageObjectNotFoundError(StorageError):
    """Raised when a requested object/path does not exist in the store."""


@dataclass
class StorageObjectInfo:
    """Metadata describing a single stored object."""
    path: str                 # logical path relative to the provider root
    size_bytes: int           # object size in bytes
    last_modified: Optional[str] = None  # ISO-8601 timestamp when available


class StorageProvider(ABC):
    """
    Abstract base class for all storage backends.

    Every method operates on *logical* relative paths.  Implementations must
    be thread-safe for concurrent reads and writes of distinct paths.
    """

    def __init__(self, name: str):
        self.name = name

    # ------------------------------------------------------------------ #
    # Abstract operations
    # ------------------------------------------------------------------ #

    @abstractmethod
    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        """Read and return the full text content of ``path``."""

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """Read and return the full binary content of ``path``."""

    @abstractmethod
    def write_text(self, path: str, content: str, encoding: str = "utf-8") -> None:
        """Create or overwrite ``path`` with ``content``."""

    @abstractmethod
    def write_bytes(self, path: str, content: bytes) -> None:
        """Create or overwrite ``path`` with binary ``content``."""

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Return True if ``path`` exists in the store."""

    @abstractmethod
    def delete(self, path: str) -> None:
        """Delete ``path``. Raises StorageObjectNotFoundError if missing."""

    @abstractmethod
    def list_objects(self, prefix: str = "", suffix: str = "") -> List[StorageObjectInfo]:
        """
        List objects whose logical path starts with ``prefix`` and ends with
        ``suffix`` (both optional). Returned paths are relative to root.
        """

    @abstractmethod
    def ensure_prefix(self, prefix: str) -> None:
        """
        Ensure the given prefix exists.  A directory create for filesystem
        providers; a no-op for flat object stores (S3/Azure/GCS).
        """

    # ------------------------------------------------------------------ #
    # Shared helpers
    # ------------------------------------------------------------------ #

    def list_names(self, prefix: str = "", suffix: str = "") -> List[str]:
        """Convenience wrapper returning only logical path names."""
        return [obj.path for obj in self.list_objects(prefix=prefix, suffix=suffix)]

    def _wrap_and_raise(self, operation: str, path: str, exc: Exception) -> None:
        """
        Log the failure with a complete stack trace and raise a StorageError.
        Never swallows the original exception - it is chained via ``from``.
        """
        message = (
            f"Storage operation '{operation}' failed on provider "
            f"'{self.name}' for path '{path}': {exc}"
        )
        logger.error(message)
        logger.error("Full stack trace:\n%s", traceback.format_exc())
        if isinstance(exc, StorageError):
            raise exc
        raise StorageError(message) from exc

    def details(self) -> dict:
        """Return diagnostic details for monitoring UIs."""
        return {"name": self.name, "type": self.__class__.__name__}
