"""
core.storage - Pluggable storage abstraction for DishtaYantra v2.0.0.

Select the backend with the ``storage.provider`` property:
filesystem | s3 | azureblob | gcs.  See storage_factory for details.
"""
from core.storage.storage_factory import (
    StorageProvider,
    StorageError,
    StorageConfigurationError,
    StorageObjectNotFoundError,
    StorageObjectInfo,
    FileSystemStorageProvider,
    create_storage_provider,
    get_storage_provider,
    reset_storage_provider,
    SUPPORTED_PROVIDERS,
)

__all__ = [
    "StorageProvider", "StorageError", "StorageConfigurationError",
    "StorageObjectNotFoundError", "StorageObjectInfo",
    "FileSystemStorageProvider", "create_storage_provider",
    "get_storage_provider", "reset_storage_provider", "SUPPORTED_PROVIDERS",
]
