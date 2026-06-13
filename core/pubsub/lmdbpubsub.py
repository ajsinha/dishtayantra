"""
LMDB DataPublisher and DataSubscriber
Version: 1.5.2

Provides pub/sub functionality using LMDB for cross-process communication.
LMDB is ideal for this because:
- Memory-mapped for high performance
- Multiple readers supported (perfect for multiple worker processes)
- ACID compliant
- Zero-copy reads

URI Format: lmdb://channel_name
            lmdb://path/to/db::channel_name

Copyright © 2025 Ashutosh Sinha. All rights reserved.
"""

import os
import time
import json
import threading
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from queue import Queue, Empty, Full
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# Constants
DEFAULT_LMDB_PATH = "data/pubsub_lmdb"
DEFAULT_MAP_SIZE = 1024 * 1024 * 1024  # 1GB
MESSAGE_EXPIRY_SECONDS = 300  # 5 minutes


@dataclass
class LMDBMessage:
    """Message structure for LMDB pub/sub"""
    channel: str
    data: Any
    timestamp: float
    message_id: str
    publisher_id: str
    sequence: int
    
    def to_bytes(self) -> bytes:
        """Serialize message to bytes"""
        msg_dict = {
            'channel': self.channel,
            'data': self.data,
            'timestamp': self.timestamp,
            'message_id': self.message_id,
            'publisher_id': self.publisher_id,
            'sequence': self.sequence
        }
        return json.dumps(msg_dict).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'LMDBMessage':
        """Deserialize message from bytes"""
        msg_dict = json.loads(data.decode('utf-8'))
        return cls(**msg_dict)


class LMDBPubSubManager:
    """
    Singleton manager for LMDB pub/sub connections.
    
    Manages shared LMDB environment across publishers and subscribers.
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, lmdb_path: str = None, map_size: int = None):
        if self._initialized:
            return
        
        self.lmdb_path = lmdb_path or DEFAULT_LMDB_PATH
        self.map_size = map_size or DEFAULT_MAP_SIZE
        
        self.env = None
        self.channels: Dict[str, Any] = {}  # channel -> database handle
        self.sequences: Dict[str, int] = {}  # channel -> latest sequence
        
        self._init_lock = threading.Lock()
        self._initialized = True
        
        self._initialize_lmdb()
    
    def _initialize_lmdb(self):
        """Initialize LMDB environment"""
        try:
            import lmdb
            
            os.makedirs(self.lmdb_path, exist_ok=True)
            
            self.env = lmdb.open(
                self.lmdb_path,
                map_size=self.map_size,
                max_dbs=100,  # Support up to 100 channels
                max_readers=256,
                writemap=True,
                metasync=False,
                sync=False  # For better performance (data is in memory anyway)
            )
            
            # Create metadata database
            with self.env.begin(write=True) as txn:
                self.meta_db = self.env.open_db(b'__metadata__', txn=txn, create=True)
            
            logger.info(f"LMDB PubSub initialized at {self.lmdb_path} "
                       f"(map_size={self.map_size / 1024 / 1024:.0f}MB)")
            
        except ImportError:
            logger.error("lmdb package not installed. Install with: pip install lmdb")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize LMDB: {e}")
            raise
    
    def get_channel_db(self, channel: str):
        """Get or create database handle for a channel"""
        if channel in self.channels:
            return self.channels[channel]
        
        with self._init_lock:
            if channel not in self.channels:
                with self.env.begin(write=True) as txn:
                    db = self.env.open_db(
                        f"channel:{channel}".encode(),
                        txn=txn,
                        create=True,
                        integerkey=True  # Use sequence numbers as keys
                    )
                    self.channels[channel] = db
                    
                    # Initialize sequence from metadata
                    seq_key = f"seq:{channel}".encode()
                    seq_data = txn.get(seq_key, db=self.meta_db)
                    self.sequences[channel] = int(seq_data.decode()) if seq_data else 0
                
                logger.debug(f"Created LMDB channel: {channel}")
        
        return self.channels[channel]
    
    def get_next_sequence(self, channel: str) -> int:
        """Get next sequence number for a channel"""
        with self._init_lock:
            if channel not in self.sequences:
                self.sequences[channel] = 0
            self.sequences[channel] += 1
            
            # Persist sequence (periodically would be better for performance)
            with self.env.begin(write=True) as txn:
                txn.put(
                    f"seq:{channel}".encode(),
                    str(self.sequences[channel]).encode(),
                    db=self.meta_db
                )
            
            return self.sequences[channel]
    
    def get_current_sequence(self, channel: str) -> int:
        """Get current sequence number for a channel"""
        return self.sequences.get(channel, 0)
    
    def publish(self, channel: str, message: LMDBMessage):
        """Publish a message to a channel"""
        db = self.get_channel_db(channel)
        
        with self.env.begin(write=True) as txn:
            # Use sequence as key (8-byte integer)
            key = message.sequence.to_bytes(8, 'big')
            txn.put(key, message.to_bytes(), db=db)
    
    def get_messages_since(self, channel: str, since_sequence: int, 
                          limit: int = 1000) -> List[LMDBMessage]:
        """Get messages since a sequence number"""
        db = self.get_channel_db(channel)
        messages = []
        
        with self.env.begin() as txn:
            cursor = txn.cursor(db=db)
            
            # Seek to first key greater than since_sequence
            start_key = (since_sequence + 1).to_bytes(8, 'big')
            
            if cursor.set_range(start_key):
                count = 0
                while count < limit:
                    try:
                        key, value = cursor.item()
                        msg = LMDBMessage.from_bytes(value)
                        messages.append(msg)
                        count += 1
                        
                        if not cursor.next():
                            break
                    except:
                        break
        
        return messages
    
    def cleanup_old_messages(self, channel: str, max_age_seconds: int = MESSAGE_EXPIRY_SECONDS):
        """Remove messages older than max_age_seconds"""
        db = self.get_channel_db(channel)
        cutoff_time = time.time() - max_age_seconds
        deleted = 0
        
        with self.env.begin(write=True) as txn:
            cursor = txn.cursor(db=db)
            
            if cursor.first():
                while True:
                    try:
                        key, value = cursor.item()
                        msg = LMDBMessage.from_bytes(value)
                        
                        if msg.timestamp < cutoff_time:
                            cursor.delete()
                            deleted += 1
                        else:
                            # Messages are ordered by sequence/time, so we can stop
                            break
                        
                        if not cursor.next():
                            break
                    except:
                        break
        
        if deleted > 0:
            logger.debug(f"Cleaned up {deleted} old messages from channel {channel}")
        
        return deleted
    
    def close(self):
        """Close the LMDB environment"""
        if self.env:
            self.env.close()
            self.env = None
            logger.info("LMDB PubSub closed")




# v2.2 module split: endpoints live in lmdbpubsub_endpoints.py.
from core.pubsub.lmdbpubsub_endpoints import (  # noqa: E402,F401
    LMDBDataPublisher,
    LMDBDataSubscriber,
    create_lmdb_publisher,
    create_lmdb_subscriber,
)
