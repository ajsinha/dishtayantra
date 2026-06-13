"""
Storage Provider Factory
========================

Builds the configured :class:`core.storage.base_storage.StorageProvider` from
``application.properties``.  The active backend is selected with::

    storage.provider = filesystem | s3 | azureblob | gcs

Each backend then requires its own ``storage.<provider>.*`` properties; see
the individual provider modules for the exact list.  In line with the v2.0.0
architecture mandate, *missing required properties are a fatal configuration
error* - the system refuses to start and reports exactly which property is
absent rather than silently falling back to a default.

Usage::

    from core.storage import get_storage_provider
    storage = get_storage_provider()              # singleton, props-driven
    text = storage.read_text("dags/sample.json")

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import threading

from core.storage.base_storage import (
    StorageProvider,
    StorageError,
    StorageConfigurationError,
    StorageObjectNotFoundError,
    StorageObjectInfo,
)
from core.storage.filesystem_storage import FileSystemStorageProvider

logger = logging.getLogger(__name__)

_singleton_lock = threading.Lock()
_singleton: StorageProvider = None

SUPPORTED_PROVIDERS = ("filesystem", "s3", "azureblob", "gcs")


def _require(props, key: str) -> str:
    """
    Fetch a required property.  Raises StorageConfigurationError with a
    detailed, actionable message if the property is absent.
    """
    value = props.get(key)
    if value is None:
        raise StorageConfigurationError(
            f"Required storage property '{key}' is missing from "
            f"application.properties (or the environment). The system will "
            f"not silently default this value - please define '{key}' and "
            f"restart."
        )
    return value


def create_storage_provider(props=None) -> StorageProvider:
    """
    Create a new StorageProvider from configuration (no singleton caching).

    Args:
        props: A PropertiesConfigurator instance. When None, the global
               PropertiesConfigurator singleton is used.

    Returns:
        A fully constructed StorageProvider.

    Raises:
        StorageConfigurationError: when configuration is missing or invalid.
    """
    if props is None:
        from core.properties_configurator import PropertiesConfigurator
        props = PropertiesConfigurator()

    provider_name = _require(props, "storage.provider").strip().lower()
    logger.info("Creating storage provider: %s", provider_name)

    if provider_name == "filesystem":
        return FileSystemStorageProvider(root=_require(props, "storage.filesystem.root"))

    if provider_name == "s3":
        from core.storage.s3_storage import S3StorageProvider
        return S3StorageProvider(
            bucket=_require(props, "storage.s3.bucket"),
            prefix=_require(props, "storage.s3.prefix"),
            region=_require(props, "storage.s3.region"),
            endpoint_url=props.get("storage.s3.endpoint_url"),
            access_key_id=props.get("storage.s3.access_key_id"),
            secret_access_key=props.get("storage.s3.secret_access_key"),
        )

    if provider_name == "azureblob":
        from core.storage.azureblob_storage import AzureBlobStorageProvider
        return AzureBlobStorageProvider(
            connection_string=_require(props, "storage.azureblob.connection_string"),
            container=_require(props, "storage.azureblob.container"),
            prefix=_require(props, "storage.azureblob.prefix"),
        )

    if provider_name == "gcs":
        from core.storage.gcs_storage import GCSStorageProvider
        return GCSStorageProvider(
            bucket=_require(props, "storage.gcs.bucket"),
            prefix=_require(props, "storage.gcs.prefix"),
            credentials_file=props.get("storage.gcs.credentials_file"),
        )

    raise StorageConfigurationError(
        f"Unknown storage.provider '{provider_name}'. "
        f"Supported providers: {', '.join(SUPPORTED_PROVIDERS)}"
    )


def get_storage_provider(props=None) -> StorageProvider:
    """Return the process-wide StorageProvider singleton, creating it lazily."""
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = create_storage_provider(props)
    return _singleton


def reset_storage_provider() -> None:
    """Drop the singleton (used by tests and configuration reloads)."""
    global _singleton
    with _singleton_lock:
        _singleton = None


__all__ = [
    "StorageProvider",
    "StorageError",
    "StorageConfigurationError",
    "StorageObjectNotFoundError",
    "StorageObjectInfo",
    "FileSystemStorageProvider",
    "create_storage_provider",
    "get_storage_provider",
    "reset_storage_provider",
    "SUPPORTED_PROVIDERS",
]
