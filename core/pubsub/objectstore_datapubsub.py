"""
Object-Store Pub/Sub Base (S3 / Azure Blob / GCS)
=================================================

Shared machinery for the cloud object-store DataPublisher / DataSubscriber
implementations introduced in v2.0.0:

    - :mod:`core.pubsub.s3_datapubsub`        (``s3://bucket/prefix``)
    - :mod:`core.pubsub.azureblob_datapubsub` (``azureblob://container/prefix``)
    - :mod:`core.pubsub.gcs_datapubsub`       (``gcs://bucket/prefix``)

Model:
    Publisher  - every message becomes ONE object named
                 ``<prefix>/msg-<utc-nanos>-<uuid>.json`` so keys sort in
                 publish order and never collide across instances.
    Subscriber - polls the prefix every ``poll_interval`` seconds, reads any
                 object it has not yet consumed (in key order), and either
                 deletes it (``delete_on_read=true``, queue semantics, the
                 default) or remembers it in a seen-set (topic-style replay
                 for other consumers).

Each concrete module only supplies a small client adapter exposing
``put / list / get / delete``; everything else (naming, ordering, dedup,
logging, error policy) lives here.  Errors are never swallowed - every
failure is logged with the full stack trace and re-raised where the base
class contract requires it.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional

import queue

from core.pubsub.datapubsub import DataPublisher, DataSubscriber

logger = logging.getLogger(__name__)


class ObjectStoreClient(ABC):
    """Minimal client contract each cloud adapter must implement."""

    @abstractmethod
    def put_text(self, key: str, content: str) -> None:
        """Create/overwrite the object ``key`` with ``content``."""

    @abstractmethod
    def get_text(self, key: str) -> str:
        """Read the full text content of ``key``."""

    @abstractmethod
    def list_keys(self, prefix: str) -> List[str]:
        """List all object keys under ``prefix`` (unordered ok)."""

    @abstractmethod
    def delete(self, key: str) -> None:
        """Delete object ``key``."""

    @abstractmethod
    def location(self) -> str:
        """Human readable location string for logging."""


class ObjectStoreDataPublisher(DataPublisher):
    """Publishes each message as one JSON object under a key prefix."""

    def __init__(self, name, destination, config, client: ObjectStoreClient,
                 prefix: str):
        """
        Args:
            client: Cloud-specific ObjectStoreClient adapter.
            prefix: Object key prefix under which messages are written.
        """
        super().__init__(name, destination, config)
        self._client = client
        self._prefix = prefix.strip("/")
        logger.info("%s publisher created for %s (prefix=%s)",
                    self.__class__.__name__, client.location(), self._prefix)

    def _message_key(self) -> str:
        """Time-ordered, collision-free object key for one message."""
        return f"{self._prefix}/msg-{time.time_ns():020d}-{uuid.uuid4().hex[:8]}.json"

    def _do_publish(self, data):
        """Serialize ``data`` to JSON and write it as a new object."""
        try:
            if hasattr(data, "to_dict"):
                payload = json.dumps(data.to_dict(), separators=(",", ":"))
            else:
                payload = json.dumps(data, separators=(",", ":"))
            key = self._message_key()
            self._client.put_text(key, payload)
            with self._lock:
                self._last_publish = datetime.now().isoformat()
                self._publish_count += 1
            logger.debug("Published object %s to %s", key, self._client.location())
        except Exception as e:
            logger.error("Error publishing to %s (%s): %s",
                         self.name, self._client.location(), str(e))
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            raise

    def details(self):
        info = super().details()
        info["object_store"] = self._client.location()
        info["prefix"] = self._prefix
        return info


class ObjectStoreDataSubscriber(DataSubscriber):
    """Polls a key prefix and consumes message objects in key order."""

    def __init__(self, name, source, config, client: ObjectStoreClient,
                 prefix: str, given_queue: queue.Queue = None):
        """
        Config keys:
            poll_interval   Seconds between listings (default 2).
            delete_on_read  True (default): delete consumed objects (queue
                            semantics). False: keep them and track a local
                            seen-set (topic-style; other consumers can also
                            read).
        """
        super().__init__(name, source, config, given_queue)
        self._client = client
        self._prefix = prefix.strip("/")
        self.poll_interval = float(config.get("poll_interval", 2))
        self.delete_on_read = bool(config.get("delete_on_read", True))
        self._seen_keys = set()
        self._pending: List[str] = []
        logger.info("%s subscriber created for %s (prefix=%s "
                    "poll=%ss delete_on_read=%s)",
                    self.__class__.__name__, client.location(), self._prefix,
                    self.poll_interval, self.delete_on_read)

    def _do_subscribe(self) -> Optional[dict]:
        """Return the next unseen message, or None after an idle poll."""
        try:
            if not self._pending:
                keys = sorted(self._client.list_keys(self._prefix))
                self._pending = [k for k in keys if k not in self._seen_keys]
                if not self._pending:
                    time.sleep(self.poll_interval)
                    return None
            key = self._pending.pop(0)
            content = self._client.get_text(key)
            if self.delete_on_read:
                self._client.delete(key)
            else:
                self._seen_keys.add(key)
            return json.loads(content)
        except Exception as e:
            logger.error("Error subscribing in %s (%s): %s",
                         self.name, self._client.location(), str(e))
            logger.error("Full stack trace:\n%s", traceback.format_exc())
            time.sleep(self.poll_interval)
            return None

    def details(self):
        info = super().details()
        info["object_store"] = self._client.location()
        info["prefix"] = self._prefix
        info["poll_interval"] = self.poll_interval
        info["delete_on_read"] = self.delete_on_read
        return info


def parse_objectstore_uri(uri: str, scheme: str):
    """
    Split ``<scheme>://<bucket-or-container>/<prefix...>`` into its parts.

    Returns:
        (bucket_or_container, prefix)

    Raises:
        ValueError: when the URI is malformed (detailed message).
    """
    expected = f"{scheme}://"
    if not uri.startswith(expected):
        raise ValueError(
            f"Object-store URI '{uri}' must start with '{expected}'")
    remainder = uri[len(expected):]
    parts = remainder.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    if not bucket:
        raise ValueError(
            f"Object-store URI '{uri}' is missing the bucket/container name "
            f"(expected {expected}<bucket>/<prefix>)")
    return bucket, prefix
