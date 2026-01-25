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


class LMDBDataPublisher:
    """
    DataPublisher implementation using LMDB.
    
    Publishes messages to an LMDB-backed channel that can be read
    by multiple processes (workers).
    
    Configuration:
        destination: lmdb://channel_name
        lmdb_path: path/to/lmdb (optional, uses default)
        map_size_mb: LMDB map size in MB (optional)
    """
    
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        
        # Parse destination
        destination = config.get('destination', '')
        if destination.startswith('lmdb://'):
            self.channel = destination[7:]  # Remove 'lmdb://'
        else:
            self.channel = destination
        
        # Configuration
        self.lmdb_path = config.get('lmdb_path', DEFAULT_LMDB_PATH)
        map_size_mb = config.get('map_size_mb', 1024)
        self.map_size = map_size_mb * 1024 * 1024
        
        # Get manager
        self.manager = LMDBPubSubManager(self.lmdb_path, self.map_size)
        
        # Stats
        self.published_count = 0
        self.last_publish_time = None
        self.running = False
        
        logger.info(f"LMDBDataPublisher '{name}' initialized for channel '{self.channel}'")
    
    def start(self):
        """Start the publisher"""
        self.running = True
        logger.info(f"LMDBDataPublisher '{self.name}' started")
    
    def stop(self):
        """Stop the publisher"""
        self.running = False
        logger.info(f"LMDBDataPublisher '{self.name}' stopped "
                   f"(published {self.published_count} messages)")
    
    def publish(self, data: Any):
        """
        Publish a message.
        
        Args:
            data: Data to publish (must be JSON-serializable)
        """
        if not self.running:
            logger.warning(f"Publisher '{self.name}' not running, message dropped")
            return
        
        sequence = self.manager.get_next_sequence(self.channel)
        
        message = LMDBMessage(
            channel=self.channel,
            data=data,
            timestamp=time.time(),
            message_id=f"{self.name}:{sequence}",
            publisher_id=self.name,
            sequence=sequence
        )
        
        self.manager.publish(self.channel, message)
        
        self.published_count += 1
        self.last_publish_time = time.time()
    
    def details(self) -> dict:
        """Get publisher details"""
        return {
            'name': self.name,
            'type': 'LMDBDataPublisher',
            'channel': self.channel,
            'lmdb_path': self.lmdb_path,
            'published_count': self.published_count,
            'last_publish_time': self.last_publish_time,
            'current_sequence': self.manager.get_current_sequence(self.channel),
            'running': self.running
        }


class LMDBDataSubscriber:
    """
    DataSubscriber implementation using LMDB.
    
    Subscribes to an LMDB-backed channel, polling for new messages.
    Safe for use across multiple processes.
    
    Configuration:
        source: lmdb://channel_name
        lmdb_path: path/to/lmdb (optional)
        poll_interval_ms: polling interval in milliseconds (default: 10)
        max_depth: maximum queue depth (default: 10000)
        auto_package_non_dict: package non-dict messages (default: true)
    """
    
    def __init__(self, name: str, config: dict):
        self.name = name
        self.config = config
        
        # Parse source
        source = config.get('source', '')
        if source.startswith('lmdb://'):
            self.channel = source[7:]
        else:
            self.channel = source
        self.source = source
        
        # Configuration
        self.lmdb_path = config.get('lmdb_path', DEFAULT_LMDB_PATH)
        map_size_mb = config.get('map_size_mb', 1024)
        self.map_size = map_size_mb * 1024 * 1024
        
        self.poll_interval = config.get('poll_interval_ms', 10) / 1000.0
        self.max_depth = config.get('max_depth', 10000)
        
        # v1.5.2: Non-dict message packaging
        self.auto_package = config.get('auto_package_non_dict', True)
        self.package_wrapper_key = config.get('package_wrapper_key', 'payload')
        self.add_metadata = config.get('add_package_metadata', True)
        
        # Get manager
        self.manager = LMDBPubSubManager(self.lmdb_path, self.map_size)
        
        # Internal queue
        self._queue = Queue(maxsize=self.max_depth)
        
        # State
        self.last_sequence = 0
        self.received_count = 0
        self.packaged_count = 0
        self.last_receive_time = None
        self.running = False
        
        # Subscription thread
        self._thread: Optional[threading.Thread] = None
        
        logger.info(f"LMDBDataSubscriber '{name}' initialized for channel '{self.channel}'")
    
    def start(self):
        """Start the subscriber"""
        if self.running:
            return
        
        self.running = True
        
        # Start polling thread
        self._thread = threading.Thread(
            target=self._poll_loop,
            name=f"LMDBSubscriber-{self.name}",
            daemon=True
        )
        self._thread.start()
        
        logger.info(f"LMDBDataSubscriber '{self.name}' started")
    
    def stop(self):
        """Stop the subscriber"""
        self.running = False
        
        if self._thread:
            self._thread.join(timeout=2)
            self._thread = None
        
        logger.info(f"LMDBDataSubscriber '{self.name}' stopped "
                   f"(received {self.received_count} messages)")
    
    def _poll_loop(self):
        """Main polling loop"""
        while self.running:
            try:
                # Get new messages since last sequence
                messages = self.manager.get_messages_since(
                    self.channel,
                    self.last_sequence,
                    limit=100  # Batch size
                )
                
                for msg in messages:
                    self._process_message(msg)
                    self.last_sequence = msg.sequence
                
                # Small sleep if no messages
                if not messages:
                    time.sleep(self.poll_interval)
                    
            except Exception as e:
                logger.error(f"Error in LMDB subscriber poll loop: {e}")
                time.sleep(0.1)
    
    def _process_message(self, msg: LMDBMessage):
        """Process a received message"""
        data = msg.data
        
        # v1.5.2: Package non-dict messages if enabled
        if self.auto_package and not isinstance(data, dict):
            data = self._package_message(data, msg)
        elif isinstance(data, dict):
            # Add DAG priority if not present
            if '_dag_priority' not in data:
                data['_dag_priority'] = self.config.get('dag_priority', 5)
        
        # Add to queue
        try:
            self._queue.put_nowait(data)
            self.received_count += 1
            self.last_receive_time = time.time()
        except Full:
            logger.warning(f"Subscriber '{self.name}' queue full, dropping message")
    
    def _package_message(self, data: Any, msg: LMDBMessage) -> dict:
        """Package a non-dict message into standard format"""
        packaged = {
            self.package_wrapper_key: data,
            '_packaged': True,
            '_original_type': type(data).__name__,
            '_dag_priority': self.config.get('dag_priority', 5)
        }
        
        if self.add_metadata:
            packaged['_metadata'] = {
                'subscriber_name': self.name,
                'source': self.source,
                'channel': self.channel,
                'received_at': datetime.now().isoformat(),
                'message_id': msg.message_id,
                'sequence': msg.sequence,
                'packaging_version': '1.5.2'
            }
        
        self.packaged_count += 1
        return packaged
    
    def get(self, timeout: float = None) -> Optional[dict]:
        """
        Get next message from queue.
        
        Args:
            timeout: Timeout in seconds (None for non-blocking)
            
        Returns:
            Message dict or None
        """
        try:
            if timeout is None:
                return self._queue.get_nowait()
            else:
                return self._queue.get(timeout=timeout)
        except Empty:
            return None
    
    def peek(self) -> Optional[dict]:
        """Peek at next message without removing it"""
        try:
            msg = self._queue.get_nowait()
            # Put it back (not ideal but works for simple cases)
            self._queue.put(msg)
            return msg
        except Empty:
            return None
    
    def qsize(self) -> int:
        """Get current queue size"""
        return self._queue.qsize()
    
    def details(self) -> dict:
        """Get subscriber details"""
        return {
            'name': self.name,
            'type': 'LMDBDataSubscriber',
            'source': self.source,
            'channel': self.channel,
            'lmdb_path': self.lmdb_path,
            'current_depth': self._queue.qsize(),
            'max_depth': self.max_depth,
            'received_count': self.received_count,
            'packaged_count': self.packaged_count,
            'last_sequence': self.last_sequence,
            'last_receive_time': self.last_receive_time,
            'auto_package_enabled': self.auto_package,
            'running': self.running
        }


# Factory functions for pubsubfactory.py integration
def create_lmdb_publisher(name: str, config: dict) -> LMDBDataPublisher:
    """Factory function to create LMDB publisher"""
    return LMDBDataPublisher(name, config)


def create_lmdb_subscriber(name: str, config: dict) -> LMDBDataSubscriber:
    """Factory function to create LMDB subscriber"""
    return LMDBDataSubscriber(name, config)
