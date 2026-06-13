"""
S3 HA Provider
==============

Primary/secondary failover using an S3 lease object:

    - The lease is a small JSON object at ``lease_key`` containing the
      holder's instance id and a heartbeat timestamp.
    - The primary rewrites the object every ``lease_seconds / 3`` seconds.
    - Secondaries poll the object on the same cadence; when the heartbeat is
      older than ``lease_seconds`` the lease is considered expired and a
      secondary writes its own id to claim it, then re-reads after a short
      jittered delay to confirm it actually won (S3 offers no compare-and-set,
      so the read-back resolves races: last-writer-wins and every contender
      observes the same winner).

This mechanism trades a few seconds of failover latency for zero additional
infrastructure - ideal when S3 is already in use for storage.

Required properties:
    ha.s3.bucket
    ha.s3.lease_key        e.g. ``ha/dishtayantra-primary.lease``
    ha.s3.region
    ha.s3.lease_seconds    e.g. ``30``

Optional:
    ha.s3.endpoint_url     Custom endpoint (MinIO / localstack)

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
import logging
import random
import socket
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone

from core.ha.ha_manager import HAProvider, HAConfigurationError

logger = logging.getLogger(__name__)


class S3HAProvider(HAProvider):
    """Lease-object based failover on Amazon S3 (or compatible stores)."""

    def __init__(self, bucket: str, lease_key: str, region: str,
                 lease_seconds: int, endpoint_url: str = None):
        super().__init__(name="s3")
        if lease_seconds < 6:
            raise HAConfigurationError(
                "ha.s3.lease_seconds must be >= 6 for safe heartbeat cadence")
        try:
            import boto3
        except ImportError as exc:
            raise HAConfigurationError(
                "boto3 is not installed but ha.provider=s3 was selected. "
                "Install with: pip install boto3"
            ) from exc
        client_kwargs = {"region_name": region}
        if endpoint_url:
            client_kwargs["endpoint_url"] = endpoint_url
        self._client = boto3.client("s3", **client_kwargs)
        self.bucket = bucket
        self.lease_key = lease_key
        self.lease_seconds = lease_seconds
        self.instance_id = f"{socket.gethostname()}:{uuid.uuid4().hex[:12]}"
        self._stop_event = threading.Event()
        self._thread: threading.Thread = None

    # ------------------------------------------------------------------ #

    def _read_lease(self) -> dict:
        """Read the lease object; returns {} when absent."""
        try:
            response = self._client.get_object(
                Bucket=self.bucket, Key=self.lease_key)
            return json.loads(response["Body"].read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            code = getattr(exc, "response", {}).get("Error", {}).get("Code") \
                if hasattr(exc, "response") else None
            if code in ("NoSuchKey", "404", "NotFound"):
                return {}
            raise

    def _write_lease(self) -> None:
        """Write this instance's id + heartbeat into the lease object."""
        payload = json.dumps({
            "holder": self.instance_id,
            "heartbeat": datetime.now(timezone.utc).isoformat(),
        })
        self._client.put_object(Bucket=self.bucket, Key=self.lease_key,
                                Body=payload.encode("utf-8"))

    def _lease_expired(self, lease: dict) -> bool:
        """True when the heartbeat is missing or older than the lease TTL."""
        heartbeat = lease.get("heartbeat")
        if not heartbeat:
            return True
        try:
            then = datetime.fromisoformat(heartbeat)
        except ValueError:
            logger.error("S3HAProvider: malformed heartbeat %r - treating "
                         "lease as expired", heartbeat)
            return True
        age = (datetime.now(timezone.utc) - then).total_seconds()
        return age > self.lease_seconds

    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Begin the heartbeat/claim loop in a daemon thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._lease_loop, daemon=True, name="S3HAProvider")
        self._thread.start()
        logger.info("S3HAProvider started (s3://%s/%s lease=%ss id=%s)",
                    self.bucket, self.lease_key, self.lease_seconds,
                    self.instance_id)

    def _lease_loop(self) -> None:
        interval = max(2, self.lease_seconds // 3)
        while not self._stop_event.is_set():
            try:
                lease = self._read_lease()
                holder = lease.get("holder")
                if self.is_primary():
                    if holder == self.instance_id:
                        self._write_lease()  # heartbeat renewal
                    else:
                        logger.warning(
                            "S3HAProvider: lease stolen by %s - demoting",
                            holder)
                        self._transition_to_secondary()
                else:
                    if holder == self.instance_id and not self._lease_expired(lease):
                        # We previously claimed and won the race.
                        self._transition_to_primary()
                    elif self._lease_expired(lease):
                        # Claim, then confirm after a jittered delay to
                        # resolve concurrent claimers deterministically.
                        logger.info("S3HAProvider: lease expired - claiming")
                        self._write_lease()
                        time.sleep(1.0 + random.uniform(0, 1.0))
                        confirm = self._read_lease()
                        if confirm.get("holder") == self.instance_id:
                            self._transition_to_primary()
                        else:
                            logger.info(
                                "S3HAProvider: lost claim race to %s",
                                confirm.get("holder"))
            except Exception as exc:  # noqa: BLE001
                logger.error("S3HAProvider lease loop error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
                self._transition_to_secondary()
            self._stop_event.wait(interval)

    def stop(self) -> None:
        """Release the lease (if held) and stop participating."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self.is_primary():
            try:
                self._client.delete_object(Bucket=self.bucket,
                                           Key=self.lease_key)
            except Exception as exc:  # noqa: BLE001
                logger.error("S3HAProvider lease release error: %s", exc)
                logger.error("Full stack trace:\n%s", traceback.format_exc())
        self._transition_to_secondary()
        logger.info("S3HAProvider stopped")
