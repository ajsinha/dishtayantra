"""
Google Cloud Storage Provider
=============================

:class:`core.storage.base_storage.StorageProvider` implementation backed by
Google Cloud Storage via the ``google-cloud-storage`` SDK (imported lazily).

Required properties (no silent defaults - missing properties raise
StorageConfigurationError with a detailed message):

    storage.gcs.bucket    Target bucket name
    storage.gcs.prefix    Object name prefix (may be empty string, but the
                          property must be present)

Optional properties:

    storage.gcs.credentials_file   Path to a service-account JSON key file.
                                   When absent, Application Default
                                   Credentials are used.

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


class GCSStorageProvider(StorageProvider):
    """Stores objects as GCS objects named ``<prefix>/<logical path>``."""

    def __init__(self, bucket: str, prefix: str, credentials_file: str = None):
        super().__init__(name="gcs")
        if not bucket:
            raise StorageConfigurationError(
                "GCSStorageProvider requires 'storage.gcs.bucket' to be set."
            )
        if prefix is None:
            raise StorageConfigurationError(
                "GCSStorageProvider requires 'storage.gcs.prefix' to be set "
                "(it may be an empty string, but the property must exist)."
            )

        try:
            from google.cloud import storage as gcs  # Lazy import
        except ImportError as exc:
            raise StorageConfigurationError(
                "google-cloud-storage is not installed but storage.provider=gcs "
                "was selected. Install with: pip install google-cloud-storage"
            ) from exc

        if credentials_file:
            self._client = gcs.Client.from_service_account_json(credentials_file)
        else:
            self._client = gcs.Client()
        self.bucket_name = bucket
        self.prefix = prefix.strip("/")
        self._bucket = self._client.bucket(bucket)
        logger.info(
            "GCSStorageProvider initialized bucket=%s prefix=%s", bucket, self.prefix
        )

    # ------------------------------------------------------------------ #

    def _blob_name(self, path: str) -> str:
        path = path.lstrip("/")
        return f"{self.prefix}/{path}" if self.prefix else path

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def read_bytes(self, path: str) -> bytes:
        try:
            blob = self._bucket.blob(self._blob_name(path))
            if not blob.exists():
                raise StorageObjectNotFoundError(
                    f"GCS object not found: gs://{self.bucket_name}/{self._blob_name(path)}"
                )
            return blob.download_as_bytes()
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("read_bytes", path, exc)

    def write_text(self, path: str, content: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, content.encode(encoding))

    def write_bytes(self, path: str, content: bytes) -> None:
        try:
            blob = self._bucket.blob(self._blob_name(path))
            blob.upload_from_string(content)
            logger.debug(
                "Wrote %d bytes to gs://%s/%s",
                len(content), self.bucket_name, self._blob_name(path)
            )
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("write_bytes", path, exc)

    def exists(self, path: str) -> bool:
        try:
            return self._bucket.blob(self._blob_name(path)).exists()
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("exists", path, exc)

    def delete(self, path: str) -> None:
        try:
            blob = self._bucket.blob(self._blob_name(path))
            if not blob.exists():
                raise StorageObjectNotFoundError(
                    f"GCS object not found: gs://{self.bucket_name}/{self._blob_name(path)}"
                )
            blob.delete()
            logger.info("Deleted gs://%s/%s", self.bucket_name, self._blob_name(path))
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("delete", path, exc)

    def list_objects(self, prefix: str = "", suffix: str = "") -> List[StorageObjectInfo]:
        try:
            full_prefix = self._blob_name(prefix) if prefix else (
                self.prefix + "/" if self.prefix else ""
            )
            results: List[StorageObjectInfo] = []
            for blob in self._client.list_blobs(self.bucket_name, prefix=full_prefix):
                rel = blob.name[len(self.prefix) + 1:] if self.prefix else blob.name
                if suffix and not rel.endswith(suffix):
                    continue
                results.append(StorageObjectInfo(
                    path=rel,
                    size_bytes=blob.size or 0,
                    last_modified=blob.updated.isoformat() if blob.updated else None,
                ))
            return sorted(results, key=lambda o: o.path)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("list_objects", prefix, exc)

    def ensure_prefix(self, prefix: str) -> None:
        """GCS buckets have no directories. No-op."""
        logger.debug("ensure_prefix is a no-op for GCS (prefix=%s)", prefix)

    def details(self) -> dict:
        info = super().details()
        info.update({"bucket": self.bucket_name, "prefix": self.prefix})
        return info
