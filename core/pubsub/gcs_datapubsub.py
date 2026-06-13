"""
Google Cloud Storage DataPublisher / DataSubscriber (v2.0.0)
============================================================

Pub/sub over GCS objects.  URI format::

    gcs://<bucket>/<prefix>

Optional config keys (publisher and subscriber):
    credentials_file   Path to a service-account JSON key file; when absent
                       Application Default Credentials are used.

Subscriber config additionally supports:
    poll_interval   Seconds between prefix listings (default 2)
    delete_on_read  Queue semantics (default true) vs topic-style replay.

Example DAG snippets::

    {"name": "gcs_out", "config": {
        "destination": "gcs://my-bucket/dishtayantra/out"}}

    {"name": "gcs_in", "config": {
        "source": "gcs://my-bucket/dishtayantra/in",
        "poll_interval": 2}}

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import logging
import queue
from typing import List

from core.pubsub.objectstore_datapubsub import (
    ObjectStoreClient,
    ObjectStoreDataPublisher,
    ObjectStoreDataSubscriber,
    parse_objectstore_uri,
)

logger = logging.getLogger(__name__)


class GCSClient(ObjectStoreClient):
    """Thin google-cloud-storage adapter for the ObjectStoreClient contract."""

    def __init__(self, bucket: str, config: dict):
        try:
            from google.cloud import storage as gcs
        except ImportError as exc:
            raise ImportError(
                "google-cloud-storage is required for gcs:// pub/sub. "
                "Install with: pip install google-cloud-storage"
            ) from exc
        credentials_file = config.get("credentials_file")
        if credentials_file:
            self._client = gcs.Client.from_service_account_json(credentials_file)
        else:
            self._client = gcs.Client()
        self.bucket_name = bucket
        self._bucket = self._client.bucket(bucket)

    def put_text(self, key: str, content: str) -> None:
        self._bucket.blob(key).upload_from_string(content)

    def get_text(self, key: str) -> str:
        return self._bucket.blob(key).download_as_bytes().decode("utf-8")

    def list_keys(self, prefix: str) -> List[str]:
        return [blob.name for blob in
                self._client.list_blobs(self.bucket_name, prefix=prefix)]

    def delete(self, key: str) -> None:
        self._bucket.blob(key).delete()

    def location(self) -> str:
        return f"gcs://{self.bucket_name}"


class GCSDataPublisher(ObjectStoreDataPublisher):
    """Publishes each message as an object under ``gcs://bucket/prefix``."""

    def __init__(self, name, destination, config):
        bucket, prefix = parse_objectstore_uri(destination, "gcs")
        super().__init__(name, destination, config,
                         client=GCSClient(bucket, config), prefix=prefix)


class GCSDataSubscriber(ObjectStoreDataSubscriber):
    """Consumes message objects from ``gcs://bucket/prefix``."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        bucket, prefix = parse_objectstore_uri(source, "gcs")
        super().__init__(name, source, config,
                         client=GCSClient(bucket, config), prefix=prefix,
                         given_queue=given_queue)
