"""
LMDB Transport Types & Codecs (v2.2 module split)
=================================================

DataFormat / LMDBConfig / DataEnvelope plus the serializer-codec mixin
(msgpack, pickle, numpy, arrow), extracted verbatim from lmdb_transport.py
to respect the 500-line architecture limit. Re-exported from
core.lmdb.lmdb_transport.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

"""
LMDB Transport Layer for Zero-Copy Data Exchange

This module provides lightning-fast data exchange between the Python DAG Engine
and native calculators (Java, C++, Rust) using LMDB memory-mapped files.

Key Benefits:
- Zero-copy data access via memory mapping
- Sub-microsecond latency for large payloads (100KB+)
- ACID transactions for data integrity
- Cross-language support (Python, Java, C++, Rust)
- Automatic cleanup and resource management

Patent-Pending Innovation:
This architecture enables heterogeneous language calculators to exchange
large datasets without serialization overhead, achieving 100-1000x speedup
compared to traditional JSON/MessagePack serialization.

Copyright © 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import os
import json
import time
import uuid
import struct
import logging
import threading
from typing import Any, Dict, Optional, Union, Tuple
from pathlib import Path
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)




class DataFormat(Enum):
    """Supported data formats for LMDB transport"""
    JSON = "json"           # Human-readable, good for debugging
    MSGPACK = "msgpack"     # Compact binary, good balance
    RAW_BYTES = "raw"       # Raw bytes, maximum performance
    NUMPY = "numpy"         # NumPy arrays, for numerical data
    ARROW = "arrow"         # Apache Arrow, for columnar data


@dataclass
class LMDBConfig:
    """Configuration for LMDB transport"""
    # Database path - loaded from config or environment
    db_path: str = ""
    
    # Maximum database size (default 1GB)
    map_size: int = 1024 * 1024 * 1024
    
    # Maximum number of named databases
    max_dbs: int = 100
    
    # Data format for serialization
    data_format: DataFormat = DataFormat.MSGPACK
    
    # TTL for data entries (seconds, 0 = no expiry)
    ttl_seconds: int = 300
    
    # Enable read-ahead for sequential access
    read_ahead: bool = True
    
    # Sync mode (True = safer, False = faster)
    sync_mode: bool = False
    
    # Maximum readers
    max_readers: int = 126
    
    # Cleanup interval (seconds)
    cleanup_interval: int = 60
    
    # Enable compression for large payloads
    compress_threshold: int = 10240  # 10KB
    
    # Metadata database name
    metadata_db: str = "_metadata"
    
    def __post_init__(self):
        """Set default db_path if not provided"""
        if not self.db_path:
            self.db_path = self._get_default_db_path()
    
    @staticmethod
    def _get_default_db_path() -> str:
        """Get default LMDB path from config or environment"""
        # Try environment variable first
        env_path = os.environ.get('LMDB_DB_PATH')
        if env_path:
            return env_path
        
        # Try loading from properties configurator
        try:
            from core.properties_configurator import PropertiesConfigurator
            config = PropertiesConfigurator()
            db_path = config.get('lmdb.db.path')
            if db_path:
                return db_path
        except Exception:
            pass
        
        # Fall back to default
        return "/tmp/dishtayantra_lmdb"
    
    @classmethod
    def from_properties(cls) -> 'LMDBConfig':
        """Create LMDBConfig from application.properties"""
        try:
            from core.properties_configurator import PropertiesConfigurator
            config = PropertiesConfigurator()
            
            return cls(
                db_path=config.get('lmdb.db.path', cls._get_default_db_path()),
                map_size=int(config.get('lmdb.map.size', str(1024 * 1024 * 1024))),
                max_dbs=int(config.get('lmdb.max.dbs', '100')),
                ttl_seconds=int(config.get('lmdb.ttl.seconds', '300')),
                max_readers=int(config.get('lmdb.max.readers', '126')),
                cleanup_interval=int(config.get('lmdb.cleanup.interval', '60'))
            )
        except Exception as e:
            logger.warning(f"Could not load LMDB config from properties: {e}, using defaults")
            return cls()


@dataclass
class DataEnvelope:
    """Envelope for data stored in LMDB"""
    # Unique transaction ID
    txn_id: str
    
    # Data key
    key: str
    
    # Data format
    format: DataFormat
    
    # Timestamp (Unix epoch)
    timestamp: float
    
    # TTL in seconds
    ttl: int
    
    # Data size in bytes
    size: int
    
    # Is data compressed
    compressed: bool = False
    
    # Source calculator/node
    source: str = ""
    
    # Target calculator/node
    target: str = ""
    
    # Checksum for integrity
    checksum: int = 0
    
    def to_bytes(self) -> bytes:
        """Serialize envelope to bytes"""
        data = {
            'txn_id': self.txn_id,
            'key': self.key,
            'format': self.format.value,
            'timestamp': self.timestamp,
            'ttl': self.ttl,
            'size': self.size,
            'compressed': self.compressed,
            'source': self.source,
            'target': self.target,
            'checksum': self.checksum
        }
        return json.dumps(data).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'DataEnvelope':
        """Deserialize envelope from bytes"""
        d = json.loads(data.decode('utf-8'))
        return cls(
            txn_id=d['txn_id'],
            key=d['key'],
            format=DataFormat(d['format']),
            timestamp=d['timestamp'],
            ttl=d['ttl'],
            size=d['size'],
            compressed=d.get('compressed', False),
            source=d.get('source', ''),
            target=d.get('target', ''),
            checksum=d.get('checksum', 0)
        )




class LMDBCodecMixin:
    """Serializer registry + numpy/arrow codecs for LMDBTransport (all
    state lives on the transport)."""

    def _setup_serializers(self):
        """Setup serializers for different data formats"""
        # JSON serializer
        self._serializers[DataFormat.JSON] = (
            lambda d: json.dumps(d).encode('utf-8'),
            lambda b: json.loads(b.decode('utf-8'))
        )
        
        # MessagePack serializer (if available)
        try:
            import msgpack
            self._serializers[DataFormat.MSGPACK] = (
                lambda d: msgpack.packb(d, use_bin_type=True),
                lambda b: msgpack.unpackb(b, raw=False)
            )
        except ImportError:
            logger.warning("msgpack not available, falling back to JSON")
            self._serializers[DataFormat.MSGPACK] = self._serializers[DataFormat.JSON]
        
        # Raw bytes (pass-through)
        self._serializers[DataFormat.RAW_BYTES] = (
            lambda d: d if isinstance(d, bytes) else str(d).encode('utf-8'),
            lambda b: b
        )
        
        # NumPy serializer (if available)
        try:
            import numpy as np
            import io
            self._serializers[DataFormat.NUMPY] = (
                lambda d: self._serialize_numpy(d),
                lambda b: self._deserialize_numpy(b)
            )
        except ImportError:
            self._serializers[DataFormat.NUMPY] = self._serializers[DataFormat.JSON]
        
        # Arrow serializer (if available)
        try:
            import pyarrow as pa
            self._serializers[DataFormat.ARROW] = (
                lambda d: self._serialize_arrow(d),
                lambda b: self._deserialize_arrow(b)
            )
        except ImportError:
            self._serializers[DataFormat.ARROW] = self._serializers[DataFormat.JSON]
    
    def _serialize_numpy(self, data) -> bytes:
        """Serialize NumPy array to bytes"""
        import numpy as np
        import io
        
        if isinstance(data, np.ndarray):
            buffer = io.BytesIO()
            np.save(buffer, data, allow_pickle=False)
            return buffer.getvalue()
        else:
            # Fall back to msgpack for non-array data
            return self._serializers[DataFormat.MSGPACK][0](data)
    
    def _deserialize_numpy(self, data: bytes):
        """Deserialize bytes to NumPy array"""
        import numpy as np
        import io
        
        buffer = io.BytesIO(data)
        try:
            return np.load(buffer, allow_pickle=False)
        except Exception:
            # Fall back to msgpack
            return self._serializers[DataFormat.MSGPACK][1](data)
    
    def _serialize_arrow(self, data) -> bytes:
        """Serialize to Apache Arrow format"""
        import pyarrow as pa
        
        if isinstance(data, pa.Table):
            sink = pa.BufferOutputStream()
            writer = pa.ipc.new_stream(sink, data.schema)
            writer.write_table(data)
            writer.close()
            return sink.getvalue().to_pybytes()
        elif isinstance(data, dict):
            table = pa.Table.from_pydict(data)
            return self._serialize_arrow(table)
        else:
            return self._serializers[DataFormat.MSGPACK][0](data)
    
    def _deserialize_arrow(self, data: bytes):
        """Deserialize from Apache Arrow format"""
        import pyarrow as pa
        
        try:
            reader = pa.ipc.open_stream(data)
            return reader.read_all()
        except Exception:
            return self._serializers[DataFormat.MSGPACK][1](data)
    
