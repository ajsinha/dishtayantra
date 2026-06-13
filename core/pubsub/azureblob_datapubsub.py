"""
Azure Blob Storage DataPublisher / DataSubscriber (v2.0.0)
==========================================================

Pub/sub over Azure blobs.  URI format::

    azureblob://<container>/<prefix>

Required config key (publisher and subscriber):
    connection_string   Azure storage account connection string.  Per the
                        v2.0.0 mandate this is NOT defaulted - a missing
                        value raises a detailed error.

Subscriber config additionally supports:
    poll_interval   Seconds between prefix listings (default 2)
    delete_on_read  Queue semantics (default true) vs topic-style replay.

Example DAG snippets::

    {"name": "az_out", "config": {
        "destination": "azureblob://events/dishtayantra/out",
        "connection_string": "${AZURE_STORAGE_CONNECTION_STRING}"}}

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


class AzureBlobClient(ObjectStoreClient):
    """Thin azure-storage-blob adapter for the ObjectStoreClient contract."""

    def __init__(self, container: str, config: dict):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as exc:
            raise ImportError(
                "azure-storage-blob is required for azureblob:// pub/sub. "
                "Install with: pip install azure-storage-blob"
            ) from exc
        connection_string = config.get("connection_string")
        if not connection_string:
            raise ValueError(
                "Azure Blob pub/sub requires 'connection_string' in the "
                "publisher/subscriber config. No default is applied - define "
                "it (a ${ENV_VAR} placeholder is supported) and restart."
            )
        service = BlobServiceClient.from_connection_string(connection_string)
        self._container = service.get_container_client(container)
        self.container_name = container

    def put_text(self, key: str, content: str) -> None:
        self._container.get_blob_client(key).upload_blob(
            content.encode("utf-8"), overwrite=True)

    def get_text(self, key: str) -> str:
        return self._container.get_blob_client(key).download_blob() \
            .readall().decode("utf-8")

    def list_keys(self, prefix: str) -> List[str]:
        return [blob.name for blob in
                self._container.list_blobs(name_starts_with=prefix)]

    def delete(self, key: str) -> None:
        self._container.get_blob_client(key).delete_blob()

    def location(self) -> str:
        return f"azureblob://{self.container_name}"


class AzureBlobDataPublisher(ObjectStoreDataPublisher):
    """Publishes each message as a blob under ``azureblob://container/prefix``."""

    def __init__(self, name, destination, config):
        container, prefix = parse_objectstore_uri(destination, "azureblob")
        super().__init__(name, destination, config,
                         client=AzureBlobClient(container, config),
                         prefix=prefix)


class AzureBlobDataSubscriber(ObjectStoreDataSubscriber):
    """Consumes message blobs from ``azureblob://container/prefix``."""

    def __init__(self, name, source, config, given_queue: queue.Queue = None):
        container, prefix = parse_objectstore_uri(source, "azureblob")
        super().__init__(name, source, config,
                         client=AzureBlobClient(container, config),
                         prefix=prefix, given_queue=given_queue)
