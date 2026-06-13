"""
Azure Blob Storage Provider
===========================

:class:`core.storage.base_storage.StorageProvider` implementation backed by
Azure Blob Storage via the ``azure-storage-blob`` SDK (imported lazily).

Required properties (no silent defaults - missing properties raise
StorageConfigurationError with a detailed message):

    storage.azureblob.connection_string  Azure storage account connection string
    storage.azureblob.container          Target container name
    storage.azureblob.prefix             Blob name prefix (may be empty string,
                                         but the property must be present)

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
from typing import List

from core.storage.base_storage import (
    StorageProvider,
    StorageObjectInfo,
    StorageObjectNotFoundError,
    StorageConfigurationError,
)

logger = logging.getLogger(__name__)


class AzureBlobStorageProvider(StorageProvider):
    """Stores objects as blobs named ``<prefix>/<logical path>``."""

    def __init__(self, connection_string: str, container: str, prefix: str):
        super().__init__(name="azureblob")
        if not connection_string:
            raise StorageConfigurationError(
                "AzureBlobStorageProvider requires "
                "'storage.azureblob.connection_string' to be set."
            )
        if not container:
            raise StorageConfigurationError(
                "AzureBlobStorageProvider requires "
                "'storage.azureblob.container' to be set."
            )
        if prefix is None:
            raise StorageConfigurationError(
                "AzureBlobStorageProvider requires 'storage.azureblob.prefix' "
                "to be set (it may be an empty string, but must exist)."
            )

        try:
            from azure.storage.blob import BlobServiceClient  # Lazy import
        except ImportError as exc:
            raise StorageConfigurationError(
                "azure-storage-blob is not installed but "
                "storage.provider=azureblob was selected. "
                "Install with: pip install azure-storage-blob"
            ) from exc

        self.container = container
        self.prefix = prefix.strip("/")
        self._service = BlobServiceClient.from_connection_string(connection_string)
        self._container_client = self._service.get_container_client(container)
        logger.info(
            "AzureBlobStorageProvider initialized container=%s prefix=%s",
            container, self.prefix
        )

    # ------------------------------------------------------------------ #

    def _blob_name(self, path: str) -> str:
        path = path.lstrip("/")
        return f"{self.prefix}/{path}" if self.prefix else path

    def _is_missing(self, exc: Exception) -> bool:
        return exc.__class__.__name__ == "ResourceNotFoundError"

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def read_bytes(self, path: str) -> bytes:
        try:
            blob = self._container_client.get_blob_client(self._blob_name(path))
            try:
                return blob.download_blob().readall()
            except Exception as exc:  # noqa: BLE001
                if self._is_missing(exc):
                    raise StorageObjectNotFoundError(
                        f"Azure blob not found: {self.container}/{self._blob_name(path)}"
                    ) from exc
                raise
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("read_bytes", path, exc)

    def write_text(self, path: str, content: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, content.encode(encoding))

    def write_bytes(self, path: str, content: bytes) -> None:
        try:
            blob = self._container_client.get_blob_client(self._blob_name(path))
            blob.upload_blob(content, overwrite=True)
            logger.debug(
                "Wrote %d bytes to azure://%s/%s",
                len(content), self.container, self._blob_name(path)
            )
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("write_bytes", path, exc)

    def exists(self, path: str) -> bool:
        try:
            blob = self._container_client.get_blob_client(self._blob_name(path))
            return blob.exists()
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("exists", path, exc)

    def delete(self, path: str) -> None:
        try:
            if not self.exists(path):
                raise StorageObjectNotFoundError(
                    f"Azure blob not found: {self.container}/{self._blob_name(path)}"
                )
            blob = self._container_client.get_blob_client(self._blob_name(path))
            blob.delete_blob()
            logger.info("Deleted azure://%s/%s", self.container, self._blob_name(path))
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("delete", path, exc)

    def list_objects(self, prefix: str = "", suffix: str = "") -> List[StorageObjectInfo]:
        try:
            full_prefix = self._blob_name(prefix) if prefix else (
                self.prefix + "/" if self.prefix else ""
            )
            results: List[StorageObjectInfo] = []
            for item in self._container_client.list_blobs(name_starts_with=full_prefix):
                name = item.name
                rel = name[len(self.prefix) + 1:] if self.prefix else name
                if suffix and not rel.endswith(suffix):
                    continue
                results.append(StorageObjectInfo(
                    path=rel,
                    size_bytes=item.size or 0,
                    last_modified=item.last_modified.isoformat() if item.last_modified else None,
                ))
            return sorted(results, key=lambda o: o.path)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("list_objects", prefix, exc)

    def ensure_prefix(self, prefix: str) -> None:
        """Azure blob containers have no directories. No-op."""
        logger.debug("ensure_prefix is a no-op for Azure Blob (prefix=%s)", prefix)

    def details(self) -> dict:
        info = super().details()
        info.update({"container": self.container, "prefix": self.prefix})
        return info
