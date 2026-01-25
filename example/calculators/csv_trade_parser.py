"""
Example Calculator: CSV Trade Parser with Packaged Message Support

This calculator demonstrates how to handle the v1.5.2 auto-packaged
non-dictionary messages from DataSubscriber.

When auto_package_non_dict is enabled, non-dict messages are wrapped:
    Input:  "TRADE_001,BUY,100,AAPL,150.25"
    Becomes: {
        'payload': "TRADE_001,BUY,100,AAPL,150.25",
        '_packaged': True,
        '_original_type': 'str',
        '_dag_priority': 5,
        '_metadata': {
            'subscriber_name': 'kafka_csv_trades',
            'source': 'kafka://topic/csv_trades',
            'received_at': '2025-01-13T10:30:00'
        }
    }

Author: Ashutosh Sinha
Version: 1.5.2
"""

import logging
from typing import Any, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class CSVTradeParserCalculator:
    """
    Parses CSV trade strings from packaged messages.
    
    Handles both:
    1. Packaged messages (auto_package_non_dict=True)
    2. Direct dictionary messages (backward compatible)
    
    CSV Format: trade_id,side,quantity,symbol,price
    Example: "TRADE_001,BUY,100,AAPL,150.25"
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.delimiter = config.get('delimiter', ',')
        self.field_names = config.get('field_names', [
            'trade_id', 'side', 'quantity', 'symbol', 'price'
        ])
        self.field_types = config.get('field_types', {
            'quantity': int,
            'price': float
        })
        
        # Statistics
        self._processed_count = 0
        self._packaged_count = 0
        self._error_count = 0
        
        logger.info(f"CSVTradeParserCalculator '{name}' initialized")
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse incoming data - handles both packaged and direct formats.
        
        Args:
            data: Either a packaged message or direct dictionary
            
        Returns:
            Parsed trade dictionary
        """
        self._processed_count += 1
        
        # Check if this is a packaged message (v1.5.2 feature)
        if data.get('_packaged'):
            return self._process_packaged_message(data)
        else:
            return self._process_direct_message(data)
    
    def _process_packaged_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process auto-packaged non-dictionary message.
        
        The DataSubscriber wraps non-dict messages with metadata:
        - payload: Original data
        - _packaged: True
        - _original_type: Python type name
        - _metadata: Subscriber info (if enabled)
        """
        self._packaged_count += 1
        
        # Extract original data and metadata
        raw_data = data.get('payload') or data.get(self.config.get('wrapper_key', 'payload'))
        original_type = data.get('_original_type', 'unknown')
        metadata = data.get('_metadata', {})
        
        logger.debug(f"Processing packaged {original_type} message from {metadata.get('subscriber_name', 'unknown')}")
        
        # Handle based on original type
        if original_type == 'str':
            result = self._parse_csv_string(raw_data)
        elif original_type == 'bytes':
            # Decode bytes to string first
            decoded = raw_data.decode('utf-8') if isinstance(raw_data, bytes) else str(raw_data)
            result = self._parse_csv_string(decoded)
        elif original_type == 'list':
            # If it's a list of CSV strings, process first item
            if raw_data and isinstance(raw_data[0], str):
                result = self._parse_csv_string(raw_data[0])
                result['_batch_size'] = len(raw_data)
            else:
                result = {'items': raw_data, '_original_type': original_type}
        else:
            # Unknown type - pass through with warning
            logger.warning(f"Unexpected original type: {original_type}")
            result = {'raw_data': raw_data, '_original_type': original_type}
        
        # Preserve source metadata for audit trail
        if metadata:
            result['_source_metadata'] = metadata
        
        return result
    
    def _process_direct_message(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process direct dictionary message (backward compatible).
        
        If the input is already a dictionary, pass through or
        extract CSV string from a known field.
        """
        # Check if there's a CSV string in a field
        csv_field = self.config.get('csv_field', 'csv_data')
        if csv_field in data and isinstance(data[csv_field], str):
            result = self._parse_csv_string(data[csv_field])
            # Merge with original data
            for key, value in data.items():
                if key != csv_field and key not in result:
                    result[key] = value
            return result
        
        # Already a dict, pass through
        return data
    
    def _parse_csv_string(self, csv_string: str) -> Dict[str, Any]:
        """
        Parse CSV string into dictionary.
        
        Args:
            csv_string: CSV formatted string like "TRADE_001,BUY,100,AAPL,150.25"
            
        Returns:
            Dictionary with parsed fields
        """
        try:
            # Split by delimiter
            parts = csv_string.strip().split(self.delimiter)
            
            # Build result dictionary
            result = {}
            for i, field_name in enumerate(self.field_names):
                if i < len(parts):
                    value = parts[i].strip()
                    
                    # Apply type conversion if specified
                    if field_name in self.field_types:
                        try:
                            value = self.field_types[field_name](value)
                        except (ValueError, TypeError) as e:
                            logger.warning(f"Type conversion failed for {field_name}: {e}")
                    
                    result[field_name] = value
            
            # Add parsing metadata
            result['_parsed'] = True
            result['_parse_timestamp'] = datetime.now().isoformat()
            
            return result
            
        except Exception as e:
            self._error_count += 1
            logger.error(f"CSV parsing error: {e}")
            return {
                '_parse_error': str(e),
                '_raw_input': csv_string,
                '_parsed': False
            }
    
    def get_stats(self) -> Dict[str, Any]:
        """Return calculator statistics."""
        return {
            'name': self.name,
            'processed_count': self._processed_count,
            'packaged_count': self._packaged_count,
            'error_count': self._error_count,
            'packaged_ratio': self._packaged_count / max(1, self._processed_count)
        }


class BatchArrayProcessor:
    """
    Example calculator for processing packaged JSON arrays.
    
    When REST API returns: [{"id": 1}, {"id": 2}]
    DataSubscriber packages it as:
    {
        'items': [{"id": 1}, {"id": 2}],
        '_packaged': True,
        '_original_type': 'list'
    }
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.wrapper_key = config.get('wrapper_key', 'items')
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process batch array data."""
        
        if data.get('_packaged') and data.get('_original_type') == 'list':
            items = data.get(self.wrapper_key, data.get('payload', []))
            
            return {
                'batch_id': datetime.now().strftime('%Y%m%d%H%M%S%f'),
                'item_count': len(items),
                'items': items,
                'processed_at': datetime.now().isoformat(),
                '_source': data.get('_metadata', {}).get('subscriber_name', 'unknown')
            }
        
        # Direct dict - pass through
        return data


# =============================================================================
# Example Usage
# =============================================================================

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    # Initialize calculator
    calc = CSVTradeParserCalculator('test_parser', {
        'delimiter': ',',
        'field_names': ['trade_id', 'side', 'quantity', 'symbol', 'price'],
        'field_types': {'quantity': int, 'price': float}
    })
    
    print("=" * 60)
    print("Testing CSVTradeParserCalculator with Packaged Messages")
    print("=" * 60)
    
    # Test 1: Packaged CSV string (v1.5.2 format)
    packaged_input = {
        'payload': "TRADE_001,BUY,100,AAPL,150.25",
        '_packaged': True,
        '_original_type': 'str',
        '_dag_priority': 5,
        '_metadata': {
            'subscriber_name': 'kafka_csv_trades',
            'source': 'kafka://topic/csv_trades',
            'received_at': '2025-01-13T10:30:00.123456',
            'packaging_version': '1.5.2'
        }
    }
    
    print("\n--- Test 1: Packaged CSV String ---")
    print(f"Input: {packaged_input}")
    result = calc.calculate(packaged_input)
    print(f"Output: {result}")
    
    # Test 2: Direct dictionary (backward compatible)
    direct_input = {
        'trade_id': 'TRADE_002',
        'side': 'SELL',
        'quantity': 50,
        'symbol': 'GOOGL',
        'price': 2800.50
    }
    
    print("\n--- Test 2: Direct Dictionary ---")
    print(f"Input: {direct_input}")
    result = calc.calculate(direct_input)
    print(f"Output: {result}")
    
    # Test 3: Packaged bytes
    packaged_bytes = {
        'payload': b"TRADE_003,BUY,200,MSFT,380.00",
        '_packaged': True,
        '_original_type': 'bytes',
        '_metadata': {'subscriber_name': 'binary_feed'}
    }
    
    print("\n--- Test 3: Packaged Bytes ---")
    print(f"Input: {packaged_bytes}")
    result = calc.calculate(packaged_bytes)
    print(f"Output: {result}")
    
    # Print statistics
    print("\n--- Calculator Statistics ---")
    print(calc.get_stats())
