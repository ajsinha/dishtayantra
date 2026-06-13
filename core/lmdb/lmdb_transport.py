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



# v2.2 module split: types + codec mixin live in lmdb_types.py.
from core.lmdb.lmdb_types import (  # noqa: F401
    DataEnvelope,
    DataFormat,
    LMDBCodecMixin,
    LMDBConfig,
)


class LMDBTransport(LMDBCodecMixin):
    """
    LMDB-based transport for zero-copy data exchange between
    Python DAG Engine and native calculators.
    
    This class manages:
    - LMDB environment lifecycle
    - Data serialization/deserialization
    - Transaction management
    - Automatic cleanup of expired data
    - Cross-process synchronization
    """
    
    _instances: Dict[str, 'LMDBTransport'] = {}
    _lock = threading.Lock()
    
    def __init__(self, config: Optional[LMDBConfig] = None):
        """Initialize LMDB transport"""
        self.config = config or LMDBConfig()
        self._env = None
        self._dbs: Dict[str, Any] = {}
        self._cleanup_thread: Optional[threading.Thread] = None
        self._shutdown = threading.Event()
        self._initialized = False
        
        # Import lmdb lazily
        self._lmdb = None
        
        # Serializers
        self._serializers: Dict[DataFormat, Tuple[callable, callable]] = {}
        self._setup_serializers()
    
    @classmethod
    def get_instance(cls, db_path: Optional[str] = None) -> 'LMDBTransport':
        """Get or create singleton instance for a given path"""
        # Use configured path if none provided
        if db_path is None:
            db_path = LMDBConfig._get_default_db_path()
        
        with cls._lock:
            if db_path not in cls._instances:
                config = LMDBConfig(db_path=db_path)
                instance = cls(config)
                instance.initialize()
                cls._instances[db_path] = instance
            return cls._instances[db_path]
    
    def initialize(self) -> bool:
        """Initialize LMDB environment"""
        if self._initialized:
            return True
        
        try:
            import lmdb
            self._lmdb = lmdb
        except ImportError:
            logger.error("lmdb package not installed. Install with: pip install lmdb")
            return False
        
        try:
            # Create database directory
            Path(self.config.db_path).mkdir(parents=True, exist_ok=True)
            
            # Open LMDB environment
            self._env = self._lmdb.open(
                self.config.db_path,
                map_size=self.config.map_size,
                max_dbs=self.config.max_dbs,
                max_readers=self.config.max_readers,
                readahead=self.config.read_ahead,
                sync=self.config.sync_mode,
                writemap=True,  # Use writable memory map
                metasync=self.config.sync_mode
            )
            
            # Create metadata database
            with self._env.begin(write=True) as txn:
                self._dbs[self.config.metadata_db] = self._env.open_db(
                    self.config.metadata_db.encode(),
                    txn=txn,
                    create=True
                )
            
            # Start cleanup thread
            self._start_cleanup_thread()
            
            self._initialized = True
            logger.info(f"LMDB transport initialized at {self.config.db_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize LMDB transport: {e}")
            return False
    
    def _start_cleanup_thread(self):
        """Start background cleanup thread"""
        if self._cleanup_thread is not None:
            return
        
        def cleanup_loop():
            while not self._shutdown.is_set():
                try:
                    self._cleanup_expired()
                except Exception as e:
                    logger.error(f"Cleanup error: {e}")
                self._shutdown.wait(self.config.cleanup_interval)
        
        self._cleanup_thread = threading.Thread(target=cleanup_loop, daemon=True)
        self._cleanup_thread.start()
    
    def _cleanup_expired(self):
        """Clean up expired data entries"""
        if not self._initialized or self._env is None:
            return
        
        current_time = time.time()
        expired_keys = []
        
        try:
            with self._env.begin() as txn:
                cursor = txn.cursor(self._dbs.get(self.config.metadata_db))
                for key, value in cursor:
                    try:
                        envelope = DataEnvelope.from_bytes(value)
                        if envelope.ttl > 0 and (current_time - envelope.timestamp) > envelope.ttl:
                            expired_keys.append(key)
                    except Exception:
                        pass
            
            if expired_keys:
                with self._env.begin(write=True) as txn:
                    for key in expired_keys:
                        self._delete_key(txn, key.decode())
                logger.debug(f"Cleaned up {len(expired_keys)} expired entries")
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def _get_or_create_db(self, db_name: str):
        """Get or create a named database"""
        if db_name in self._dbs:
            return self._dbs[db_name]
        
        with self._env.begin(write=True) as txn:
            db = self._env.open_db(db_name.encode(), txn=txn, create=True)
            self._dbs[db_name] = db
            return db
    
    def _compress(self, data: bytes) -> Tuple[bytes, bool]:
        """Compress data if above threshold"""
        if len(data) < self.config.compress_threshold:
            return data, False
        
        try:
            import zlib
            compressed = zlib.compress(data, level=1)  # Fast compression
            if len(compressed) < len(data) * 0.9:  # Only if 10%+ savings
                return compressed, True
        except Exception:
            pass
        
        return data, False
    
    def _decompress(self, data: bytes, is_compressed: bool) -> bytes:
        """Decompress data if compressed"""
        if not is_compressed:
            return data
        
        try:
            import zlib
            return zlib.decompress(data)
        except Exception:
            return data
    
    def _compute_checksum(self, data: bytes) -> int:
        """Compute simple checksum for data integrity"""
        import zlib
        return zlib.crc32(data) & 0xffffffff
    
    def put(
        self,
        key: str,
        data: Any,
        format: Optional[DataFormat] = None,
        ttl: Optional[int] = None,
        source: str = "",
        target: str = "",
        db_name: str = "default"
    ) -> str:
        """
        Store data in LMDB for consumption by native calculators.
        
        Args:
            key: Unique key for the data
            data: Data to store (will be serialized based on format)
            format: Data format for serialization
            ttl: Time-to-live in seconds (0 = use config default)
            source: Source calculator/node name
            target: Target calculator/node name
            db_name: Named database to use
            
        Returns:
            Transaction ID for tracking
        """
        if not self._initialized:
            raise RuntimeError("LMDB transport not initialized")
        
        format = format or self.config.data_format
        ttl = ttl if ttl is not None else self.config.ttl_seconds
        
        # Serialize data
        serializer, _ = self._serializers[format]
        serialized = serializer(data)
        
        # Compress if needed
        compressed_data, is_compressed = self._compress(serialized)
        
        # Create envelope
        txn_id = str(uuid.uuid4())
        envelope = DataEnvelope(
            txn_id=txn_id,
            key=key,
            format=format,
            timestamp=time.time(),
            ttl=ttl,
            size=len(compressed_data),
            compressed=is_compressed,
            source=source,
            target=target,
            checksum=self._compute_checksum(compressed_data)
        )
        
        # Store in LMDB
        db = self._get_or_create_db(db_name)
        metadata_db = self._dbs[self.config.metadata_db]
        
        data_key = f"{db_name}:{key}".encode()
        metadata_key = f"{db_name}:{key}:meta".encode()
        
        with self._env.begin(write=True) as txn:
            txn.put(data_key, compressed_data, db=db)
            txn.put(metadata_key, envelope.to_bytes(), db=metadata_db)
        
        logger.debug(f"Stored {len(compressed_data)} bytes with key={key}, txn_id={txn_id}")
        return txn_id
    
    def get(
        self,
        key: str,
        db_name: str = "default",
        verify_checksum: bool = True
    ) -> Optional[Any]:
        """
        Retrieve data from LMDB.
        
        Args:
            key: Key to retrieve
            db_name: Named database to use
            verify_checksum: Whether to verify data integrity
            
        Returns:
            Deserialized data or None if not found
        """
        if not self._initialized:
            raise RuntimeError("LMDB transport not initialized")
        
        db = self._dbs.get(db_name)
        if db is None:
            return None
        
        data_key = f"{db_name}:{key}".encode()
        metadata_key = f"{db_name}:{key}:meta".encode()
        
        with self._env.begin() as txn:
            # Get metadata
            meta_bytes = txn.get(metadata_key, db=self._dbs[self.config.metadata_db])
            if meta_bytes is None:
                return None
            
            envelope = DataEnvelope.from_bytes(meta_bytes)
            
            # Check TTL
            if envelope.ttl > 0:
                if (time.time() - envelope.timestamp) > envelope.ttl:
                    logger.debug(f"Data expired for key={key}")
                    return None
            
            # Get data
            data = txn.get(data_key, db=db)
            if data is None:
                return None
            
            # Verify checksum
            if verify_checksum:
                computed = self._compute_checksum(data)
                if computed != envelope.checksum:
                    logger.error(f"Checksum mismatch for key={key}")
                    return None
            
            # Decompress
            decompressed = self._decompress(data, envelope.compressed)
            
            # Deserialize
            _, deserializer = self._serializers[envelope.format]
            return deserializer(decompressed)
    
    def get_raw(
        self,
        key: str,
        db_name: str = "default"
    ) -> Optional[Tuple[bytes, DataEnvelope]]:
        """
        Get raw bytes and metadata (for native calculator use).
        
        Args:
            key: Key to retrieve
            db_name: Named database to use
            
        Returns:
            Tuple of (raw_bytes, envelope) or None
        """
        if not self._initialized:
            raise RuntimeError("LMDB transport not initialized")
        
        db = self._dbs.get(db_name)
        if db is None:
            return None
        
        data_key = f"{db_name}:{key}".encode()
        metadata_key = f"{db_name}:{key}:meta".encode()
        
        with self._env.begin() as txn:
            meta_bytes = txn.get(metadata_key, db=self._dbs[self.config.metadata_db])
            if meta_bytes is None:
                return None
            
            envelope = DataEnvelope.from_bytes(meta_bytes)
            data = txn.get(data_key, db=db)
            
            if data is None:
                return None
            
            # Decompress for native use
            decompressed = self._decompress(data, envelope.compressed)
            return decompressed, envelope
    
    def delete(self, key: str, db_name: str = "default") -> bool:
        """Delete a key from LMDB"""
        if not self._initialized:
            return False
        
        with self._env.begin(write=True) as txn:
            return self._delete_key(txn, key, db_name)
    
    def _delete_key(self, txn, key: str, db_name: str = "default") -> bool:
        """Internal delete with transaction"""
        try:
            db = self._dbs.get(db_name)
            if db is None:
                return False
            
            data_key = f"{db_name}:{key}".encode()
            metadata_key = f"{db_name}:{key}:meta".encode()
            
            txn.delete(data_key, db=db)
            txn.delete(metadata_key, db=self._dbs[self.config.metadata_db])
            return True
        except Exception:
            return False
    
    def get_db_path(self) -> str:
        """Get the LMDB database path for native calculator configuration"""
        return self.config.db_path
    
    def get_info(self) -> Dict[str, Any]:
        """Get LMDB environment information"""
        if not self._initialized:
            return {}
        
        stat = self._env.stat()
        info = self._env.info()
        
        return {
            'path': self.config.db_path,
            'map_size': info['map_size'],
            'last_pgno': info['last_pgno'],
            'last_txnid': info['last_txnid'],
            'max_readers': info['max_readers'],
            'num_readers': info['num_readers'],
            'entries': stat['entries'],
            'depth': stat['depth'],
            'branch_pages': stat['branch_pages'],
            'leaf_pages': stat['leaf_pages'],
            'overflow_pages': stat['overflow_pages']
        }
    
    def close(self):
        """Close LMDB environment"""
        self._shutdown.set()
        
        if self._cleanup_thread is not None:
            self._cleanup_thread.join(timeout=2)
            self._cleanup_thread = None
        
        if self._env is not None:
            self._env.close()
            self._env = None
        
        self._initialized = False
        logger.info("LMDB transport closed")
    
    def __enter__(self):
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


# v2.2 module split: the calculator mixin + helpers live in
# lmdb_calculator.py (imported AFTER LMDBTransport is defined because
# get_transport() constructs it at call time).
from core.lmdb.lmdb_zero_copy import (  # noqa: E402,F401
    LMDBCalculatorMixin,
    get_transport,
    lmdb_delete,
    lmdb_get,
    lmdb_put,
)
