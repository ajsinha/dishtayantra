"""
Pub/Sub Base Types (v2.0.0 module split)
========================================

Shared foundation for every DataPublisher/DataSubscriber implementation,
extracted verbatim from datapubsub.py so each module stays within the
500-line architecture limit. All names are re-exported from
core.pubsub.datapubsub, so existing imports keep working.

Contains:
    smart_deserialize        - universal raw-message -> dict adapter
    PriorityExtractor family - DAG-priority extraction strategy
    DataAwarePayload         - priority-aware payload envelope
    AbstractDataPubSub       - common base (naming, details, lifecycle)

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

import json
from abc import ABC, abstractmethod
import threading
import queue
import time
import logging
import traceback
from datetime import datetime
from typing import Any, Protocol, runtime_checkable, Union

from core import instantiate_from_full_name

logger = logging.getLogger(__name__)



def smart_deserialize(raw_data: Union[bytes, str, None], source_name: str = "unknown") -> dict:
    """
    v1.7.2: Universal smart deserializer for all pubsub implementations.
    
    Handles both JSON and non-JSON messages, ensuring downstream DAG components
    always receive a dict. This mimics auto_package_non_dict=true behavior.
    
    Args:
        raw_data: Raw message data (bytes, str, or None)
        source_name: Name of the source for logging purposes
        
    Returns:
        dict: Either parsed JSON dict or auto-packaged dict
        
    Examples:
        >>> smart_deserialize(b'{"key": "value"}', "kafka")
        {'key': 'value'}
        
        >>> smart_deserialize(b'hello world', "activemq")
        {'_raw_data': 'hello world', '_raw_type': 'string', '_auto_packaged': True, '_original_format': 'plain_text'}
    """
    if raw_data is None:
        return {"_raw_data": None, "_raw_type": "null", "_auto_packaged": True}
    
    try:
        # Handle bytes
        if isinstance(raw_data, bytes):
            decoded = raw_data.decode('utf-8')
        else:
            decoded = str(raw_data)
        
        # Try to parse as JSON
        parsed = json.loads(decoded)
        
        # If it's already a dict, return as-is
        if isinstance(parsed, dict):
            return parsed
        
        # If it's a list or primitive, wrap it
        return {
            "_raw_data": parsed,
            "_raw_type": type(parsed).__name__,
            "_auto_packaged": True
        }
        
    except json.JSONDecodeError:
        # Not valid JSON - wrap raw string in dict
        if isinstance(raw_data, bytes):
            decoded = raw_data.decode('utf-8', errors='replace')
        else:
            decoded = str(raw_data)
        
        logger.info(f"[{source_name}] Auto-packaging non-JSON message: {decoded[:100]}...")
        return {
            "_raw_data": decoded,
            "_raw_type": "string",
            "_auto_packaged": True,
            "_original_format": "plain_text"
        }
    except Exception as e:
        # Fallback for any other errors
        logger.warning(f"[{source_name}] Error deserializing message, packaging as raw: {e}")
        if isinstance(raw_data, bytes):
            raw_str = raw_data.hex()
        else:
            raw_str = str(raw_data)
        return {
            "_raw_data": raw_str,
            "_raw_type": "bytes" if isinstance(raw_data, bytes) else type(raw_data).__name__,
            "_auto_packaged": True,
            "_error": str(e)
        }


class PriorityExtractorLike(Protocol):
    def resolve(self, data: Any) -> int:
        ... # The '...' indicates an abstract/required method

class PriorityExtractor(ABC):

    def __init__(self):
        self.default_priority = 5
        self.priority_key='_dag_priority'

    @abstractmethod
    def resolve(self, data: Any) -> int:
        pass

class DefaultPriorityExtractor(PriorityExtractor):

    def __init__(self, priority_key='_dag_priority'):
        PriorityExtractor.__init__(self)
        self.priority_key = priority_key


    def resolve(self, data):
        """
        Extract priority from data.

        Rules:
        1. Only priorities 1, 2, 3, 4, 5 are allowed
        2. Default priority is 5
        3. Priority extraction order:
           a. If data is DataAwarePayload instance, use its dag_priority attribute
           b. If data is a dictionary and has the priority key, use that value
           c. Otherwise default to 5
        4. Validation rules (apply to all sources):
           - If value is <= 0 (zero or negative), default to 5
           - If value is > 5, cap to 5
           - If conversion fails, default to 5

        Args:
            data: The data to extract priority from (DataAwarePayload, dict, or other)
            priority_key: Name of the key to look for priority in dictionaries (default: '_dag_priority')

        Returns:
            int: Priority value between 1-5 (lower number = higher priority)
                 1 = highest priority, 5 = lowest priority
        """


        # Check if data is DataAwarePayload instance
        if isinstance(data, DataAwarePayload):
            try:
                priority = int(data.dag_priority)

                # Zero or negative values default to 5
                if priority <= 0:
                    logger.warning(
                        f"DataAwarePayload priority {priority} is zero or negative, using default: {self.default_priority}")
                    return self.default_priority

                # Values greater than 5 are capped to 5
                if priority > 5:
                    logger.warning(f"DataAwarePayload priority {priority} exceeds maximum, capping to: 5")
                    return 5

                # Valid priority (1-5)
                return priority

            except (ValueError, TypeError, AttributeError):
                logger.warning(f"Failed to extract priority from DataAwarePayload, using default: {self.default_priority}")
                return self.default_priority

        # Check if data is dictionary
        if not isinstance(data, dict):
            return self.default_priority

        if self.priority_key not in data:
            return self.default_priority

        try:
            priority = int(data[self.priority_key])

            # Zero or negative values default to 5
            if priority <= 0:
                logger.warning(f"Priority {priority} is zero or negative, using default: {self.default_priority}")
                return self.default_priority

            # Values greater than 5 are capped to 5
            if priority > 5:
                logger.warning(f"Priority {priority} exceeds maximum, capping to: 5")
                return 5

            # Valid priority (1-5)
            return priority

        except (ValueError, TypeError):
            logger.warning(f"Failed to convert {self.priority_key} to int, using default: {self.default_priority}")
            return self.default_priority


class DataAwarePayload:
    """Class that carries a payload - typically a dictionary - along with destination i.e. queue or topic."""

    def __init__(self, destination, cde, payload, dag_priority=5):
        self.destination = destination
        self.cde = cde
        self.payload = payload
        self.dag_priority = dag_priority  # Default priority is 5

    def add_to_cde(self, k, v):
        self.cde[k] = v

    def to_dict(self):
        return {"destination": self.destination, 'cde': self.cde, 'payload': self.payload,
                'dag_priority': self.dag_priority}

    def get_data_for_publication(self):
        if self.destination is None or len(self.destination) == 0:
            if self.cde is None or len(self.cde) == 0:
                return '', self.payload
            else:
                return '', {'cde': self.cde, 'payload': self.payload}
        else:
            if self.cde is None or len(self.cde) == 0:
                return self.destination, self.payload
            else:
                return self.destination, {'cde': self.cde, 'payload': self.payload}

    def __str__(self):
        local_dict = {'destination': self.destination, 'cde': self.cde, 'payload': self.payload,
                      'dag_priority': self.dag_priority}
        return json.dumps(local_dict)

class AbstractDataPubSub(ABC):

    def __init__(self, name, config: dict):
        self.name = name
        self.config = config

        # Get priority key name from config (defaults to '_dag_priority')
        self.queue_type = config.get('queue_type', 'fifo').lower()
        self.priority_key = config.get('dag_priority', '_dag_priority')
        priority_extractor_module = config.get('priority_extractor', 'core.pubsub.datapubsub.DefaultPriorityExtractor')
        self.priority_extractor: PriorityExtractorLike = instantiate_from_full_name(priority_extractor_module)
        if 'dag_priority_key' in self.config.keys():
            self.priority_key = config.get('dag_priority_key')
            self.priority_extractor.priority_key = self.priority_key
        else:
            self.priority_key = self.priority_extractor.priority_key

    def is_priority_queue(self):
        return self.queue_type == 'priority'

    def set_priority_extractor(self, extractor: PriorityExtractorLike):
            self.priority_extractor = extractor


