"""
Amazon S3 Storage Provider
==========================

:class:`core.storage.base_storage.StorageProvider` implementation backed by
Amazon S3 via boto3.  The boto3 dependency is imported lazily so that the
application can run without the AWS SDK when this provider is not selected.

Required properties (no silent defaults - missing properties raise
StorageConfigurationError with a detailed message):

    storage.s3.bucket           Target bucket name
    storage.s3.prefix           Key prefix under which logical paths live
                                (may be empty string, but must be present)
    storage.s3.region           AWS region (e.g. ``us-east-1``)

Optional properties:

    storage.s3.endpoint_url     Custom endpoint (MinIO / localstack)
    storage.s3.access_key_id    Explicit credentials. When absent boto3's
    storage.s3.secret_access_key  standard credential chain is used.

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


class S3StorageProvider(StorageProvider):
    """Stores objects as S3 keys ``<prefix>/<logical path>``."""

    def __init__(self, bucket: str, prefix: str, region: str,
                 endpoint_url: str = None,
                 access_key_id: str = None,
                 secret_access_key: str = None):
        super().__init__(name="s3")
        if bucket is None or bucket == "":
            raise StorageConfigurationError(
                "S3StorageProvider requires 'storage.s3.bucket' to be set."
            )
        if prefix is None:
            raise StorageConfigurationError(
                "S3StorageProvider requires 'storage.s3.prefix' to be set "
                "(it may be an empty string, but the property must exist)."
            )
        if region is None or region == "":
            raise StorageConfigurationError(
                "S3StorageProvider requires 'storage.s3.region' to be set."
            )

        try:
            import boto3  # Lazy import - only needed when this provider is used
        except ImportError as exc:
            raise StorageConfigurationError(
                "boto3 is not installed but storage.provider=s3 was selected. "
                "Install with: pip install boto3"
            ) from exc

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        client_kwargs = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        if access_key_id and secret_access_key:
            client_kwargs["aws_access_key_id"] = access_key_id
            client_kwargs["aws_secret_access_key"] = secret_access_key
        self._client = boto3.client("s3", **client_kwargs)
        logger.info(
            "S3StorageProvider initialized bucket=%s prefix=%s region=%s",
            bucket, self.prefix, region
        )

    # ------------------------------------------------------------------ #

    def _key(self, path: str) -> str:
        """Map a logical path to a full S3 object key."""
        path = path.lstrip("/")
        return f"{self.prefix}/{path}" if self.prefix else path

    def _is_missing(self, exc: Exception) -> bool:
        """Detect 'object does not exist' errors from botocore."""
        code = getattr(getattr(exc, "response", {}), "get", lambda *_: None)("Error")
        if isinstance(code, dict):
            return code.get("Code") in ("404", "NoSuchKey", "NotFound")
        return exc.__class__.__name__ in ("NoSuchKey", "NotFound")

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        return self.read_bytes(path).decode(encoding)

    def read_bytes(self, path: str) -> bytes:
        try:
            try:
                response = self._client.get_object(Bucket=self.bucket, Key=self._key(path))
            except Exception as exc:  # noqa: BLE001
                if self._is_missing(exc):
                    raise StorageObjectNotFoundError(
                        f"S3 object not found: s3://{self.bucket}/{self._key(path)}"
                    ) from exc
                raise
            return response["Body"].read()
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("read_bytes", path, exc)

    def write_text(self, path: str, content: str, encoding: str = "utf-8") -> None:
        self.write_bytes(path, content.encode(encoding))

    def write_bytes(self, path: str, content: bytes) -> None:
        try:
            self._client.put_object(Bucket=self.bucket, Key=self._key(path), Body=content)
            logger.debug("Wrote %d bytes to s3://%s/%s", len(content), self.bucket, self._key(path))
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("write_bytes", path, exc)

    def exists(self, path: str) -> bool:
        try:
            try:
                self._client.head_object(Bucket=self.bucket, Key=self._key(path))
                return True
            except Exception as exc:  # noqa: BLE001
                if self._is_missing(exc):
                    return False
                raise
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("exists", path, exc)

    def delete(self, path: str) -> None:
        try:
            if not self.exists(path):
                raise StorageObjectNotFoundError(
                    f"S3 object not found: s3://{self.bucket}/{self._key(path)}"
                )
            self._client.delete_object(Bucket=self.bucket, Key=self._key(path))
            logger.info("Deleted s3://%s/%s", self.bucket, self._key(path))
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("delete", path, exc)

    def list_objects(self, prefix: str = "", suffix: str = "") -> List[StorageObjectInfo]:
        try:
            full_prefix = self._key(prefix) if prefix else (self.prefix + "/" if self.prefix else "")
            results: List[StorageObjectInfo] = []
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
                for item in page.get("Contents", []):
                    key = item["Key"]
                    rel = key[len(self.prefix) + 1:] if self.prefix else key
                    if suffix and not rel.endswith(suffix):
                        continue
                    last_mod = item.get("LastModified")
                    results.append(StorageObjectInfo(
                        path=rel,
                        size_bytes=item.get("Size", 0),
                        last_modified=last_mod.isoformat() if last_mod else None,
                    ))
            return sorted(results, key=lambda o: o.path)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("list_objects", prefix, exc)

    def ensure_prefix(self, prefix: str) -> None:
        """S3 has no directories; prefixes exist implicitly. No-op."""
        logger.debug("ensure_prefix is a no-op for S3 (prefix=%s)", prefix)

    def details(self) -> dict:
        info = super().details()
        info.update({"bucket": self.bucket, "prefix": self.prefix})
        return info
