"""
FileSystem Storage Provider
===========================

Default :class:`core.storage.base_storage.StorageProvider` implementation
backed by the local file system.

Required properties (no defaults are applied - a missing property is a
hard configuration error per the v2.0.0 architecture mandate):

    storage.filesystem.root   Root directory under which all logical paths
                              are resolved (e.g. ``./``).

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import os
from datetime import datetime, timezone
from typing import List

from core.storage.base_storage import (
    StorageProvider,
    StorageObjectInfo,
    StorageObjectNotFoundError,
    StorageConfigurationError,
)

logger = logging.getLogger(__name__)


class FileSystemStorageProvider(StorageProvider):
    """Stores objects as ordinary files under a configured root directory."""

    def __init__(self, root: str):
        """
        Args:
            root: Root directory. Created if it does not exist.

        Raises:
            StorageConfigurationError: if ``root`` is empty/None.
        """
        super().__init__(name="filesystem")
        if not root:
            raise StorageConfigurationError(
                "FileSystemStorageProvider requires a non-empty root directory. "
                "Set property 'storage.filesystem.root' in application.properties."
            )
        self.root = os.path.abspath(os.path.expanduser(os.path.expandvars(root)))
        os.makedirs(self.root, exist_ok=True)
        logger.info("FileSystemStorageProvider initialized with root=%s", self.root)

    # ------------------------------------------------------------------ #

    def _full(self, path: str) -> str:
        """Resolve a logical path to an absolute path, blocking traversal."""
        full = os.path.abspath(os.path.join(self.root, path))
        if not full.startswith(self.root):
            raise StorageObjectNotFoundError(
                f"Path '{path}' escapes the storage root '{self.root}' "
                f"(directory traversal blocked)"
            )
        return full

    def read_text(self, path: str, encoding: str = "utf-8") -> str:
        try:
            full = self._full(path)
            if not os.path.isfile(full):
                raise StorageObjectNotFoundError(f"File not found: {full}")
            with open(full, "r", encoding=encoding) as fh:
                return fh.read()
        except Exception as exc:  # noqa: BLE001 - rewrapped with stack trace
            self._wrap_and_raise("read_text", path, exc)

    def read_bytes(self, path: str) -> bytes:
        try:
            full = self._full(path)
            if not os.path.isfile(full):
                raise StorageObjectNotFoundError(f"File not found: {full}")
            with open(full, "rb") as fh:
                return fh.read()
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("read_bytes", path, exc)

    def write_text(self, path: str, content: str, encoding: str = "utf-8") -> None:
        try:
            full = self._full(path)
            os.makedirs(os.path.dirname(full) or self.root, exist_ok=True)
            with open(full, "w", encoding=encoding) as fh:
                fh.write(content)
            logger.debug("Wrote %d chars to %s", len(content), full)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("write_text", path, exc)

    def write_bytes(self, path: str, content: bytes) -> None:
        try:
            full = self._full(path)
            os.makedirs(os.path.dirname(full) or self.root, exist_ok=True)
            with open(full, "wb") as fh:
                fh.write(content)
            logger.debug("Wrote %d bytes to %s", len(content), full)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("write_bytes", path, exc)

    def exists(self, path: str) -> bool:
        try:
            return os.path.isfile(self._full(path))
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("exists", path, exc)

    def delete(self, path: str) -> None:
        try:
            full = self._full(path)
            if not os.path.isfile(full):
                raise StorageObjectNotFoundError(f"File not found: {full}")
            os.remove(full)
            logger.info("Deleted file %s", full)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("delete", path, exc)

    def list_objects(self, prefix: str = "", suffix: str = "") -> List[StorageObjectInfo]:
        try:
            results: List[StorageObjectInfo] = []
            base = self._full(prefix) if prefix else self.root
            scan_root = base if os.path.isdir(base) else os.path.dirname(base)
            if not os.path.isdir(scan_root):
                return results
            for dirpath, _dirnames, filenames in os.walk(scan_root):
                for fname in filenames:
                    full = os.path.join(dirpath, fname)
                    rel = os.path.relpath(full, self.root).replace(os.sep, "/")
                    if prefix and not rel.startswith(prefix.rstrip("/")):
                        continue
                    if suffix and not rel.endswith(suffix):
                        continue
                    stat = os.stat(full)
                    results.append(
                        StorageObjectInfo(
                            path=rel,
                            size_bytes=stat.st_size,
                            last_modified=datetime.fromtimestamp(
                                stat.st_mtime, tz=timezone.utc
                            ).isoformat(),
                        )
                    )
            return sorted(results, key=lambda o: o.path)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("list_objects", prefix, exc)

    def ensure_prefix(self, prefix: str) -> None:
        try:
            os.makedirs(self._full(prefix), exist_ok=True)
        except Exception as exc:  # noqa: BLE001
            self._wrap_and_raise("ensure_prefix", prefix, exc)

    def details(self) -> dict:
        info = super().details()
        info["root"] = self.root
        return info
