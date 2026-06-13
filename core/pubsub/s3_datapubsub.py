"""
Amazon S3 DataPublisher / DataSubscriber (v2.0.0)
=================================================

Pub/sub over S3 objects.  URI format::

    s3://<bucket>/<prefix>

Publisher config (all optional beyond the standard DataPublisher keys):
    region          AWS region (default us-east-1 only when omitted by an
                    explicit user choice in the DAG config; recommended to
                    always set it)
    endpoint_url    Custom endpoint (MinIO / localstack)
    access_key_id / secret_access_key
                    Explicit credentials; otherwise boto3's standard chain.

Subscriber config additionally supports:
    poll_interval   Seconds between prefix listings (default 2)
    delete_on_read  Queue semantics (default true) vs topic-style replay.

Example DAG snippets::

    {"name": "s3_out", "config": {
        "destination": "s3://my-bucket/dishtayantra/out",
        "region": "us-east-1"}}

    {"name": "s3_in", "config": {
        "source": "s3://my-bucket/dishtayantra/in",
        "region": "us-east-1", "poll_interval": 2}}

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


class S3Client(ObjectStoreClient):
    """Thin boto3 adapter satisfying the ObjectStoreClient contract."""

    def __init__(self, bucket: str, config: dict):
        try:
            import boto3
        except ImportError as exc:
            raise ImportError(
                "boto3 is required for s3:// pub/sub. "
                "Install with: pip install boto3"
            ) from exc
        kwargs = {"region_name": config.get("region", "us-east-1")}
        if config.get("endpoint_url"):
            kwargs["endpoint_url"] = config["endpoint_url"]
        if config.get("access_key_id") and config.get("secret_access_key"):
            kwargs["aws_access_key_id"] = config["access_key_id"]
            kwargs["aws_secret_access_key"] = config["secret_access_key"]
        self._s3 = boto3.client("s3", **kwargs)
        self.bucket = bucket

    def put_text(self, key: str, content: str) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key,
                            Body=content.encode("utf-8"))

    def get_text(self, key: str) -> str:
        response = self._s3.get_object(Bucket=self.bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def list_keys(self, prefix: str) -> List[str]:
        keys: List[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys.extend(item["Key"] for item in page.get("Contents", []))
        return keys

    def delete(self, key: str) -> None:
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def location(self) -> str:
        return f"s3://{self.bucket}"


class S3DataPublisher(ObjectStoreDataPublisher):
    """Publishes each message as an object under ``s3://bucket/prefix``."""

    def __init__(self, name, destination, config):
        bucket, prefix = parse_objectstore_uri(destination, "s3")
        super().__init__(name, destination, config,
                         client=S3Client(bucket, config), prefix=prefix)


class S3DataSubscriber(ObjectStoreDataSubscriber):
    """Consumes message objects from ``s3://bucket/prefix``."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        bucket, prefix = parse_objectstore_uri(source, "s3")
        super().__init__(name, source, config,
                         client=S3Client(bucket, config), prefix=prefix,
                         given_queue=given_queue)
