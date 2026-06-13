# DataSubscriber Non-Dictionary Message Packaging Guide

**Applies to: DishtaYantra current release**
**Author:** Ashutosh Sinha  
**Email:** ajsinha@gmail.com  
**Date:** January 2025

---

## Table of Contents

1. [Overview](#overview)
2. [The Problem](#the-problem)
3. [The Solution](#the-solution)
4. [Configuration Options](#configuration-options)
5. [How It Works](#how-it-works)
6. [Examples](#examples)
7. [Calculator Integration](#calculator-integration)
8. [Best Practices](#best-practices)
9. [Troubleshooting](#troubleshooting)

---

## Overview

DishtaYantra provides **automatic packaging of non-dictionary messages** in the DataSubscriber class. This enhancement ensures consistent data format for downstream calculators regardless of the original message format from external sources like Kafka, RabbitMQ, REST APIs, and other message brokers.

---

## The Problem

External message brokers can send various message formats:

| Format | Example |
|--------|---------|
| **String** | `"TRADE_123,BUY,100,AAPL"` |
| **Number** | `42`, `3.14159` |
| **List/Array** | `[1, 2, 3, 4, 5]` |
| **Binary/Bytes** | `b'\x00\x01\x02'` |
| **JSON Array** | `[{"id": 1}, {"id": 2}]` |

### Issues with Non-Dictionary Messages

1. **Priority Extraction Fails**: The default priority extractor expects dictionaries
2. **Inconsistent Calculator Input**: Each calculator must handle different data types
3. **No Metadata**: No information about message origin or timing
4. **Debugging Difficulty**: Hard to trace message flow without standardized format

---

## The Solution

The DataSubscriber now **automatically wraps non-dictionary messages** into a standardized dictionary format before placing them in the internal queue.

### Packaged Message Format

```python
{
    'payload': <original_data>,        # Original message content
    '_packaged': True,                 # Flag indicating auto-packaging
    '_original_type': 'str',           # Original Python type name
    '_dag_priority': 5,                # Default priority for routing
    '_metadata': {                     # Optional metadata (if enabled)
        'subscriber_name': 'kafka_sub',
        'source': 'trades_topic',
        'received_at': '2025-01-13T10:30:00.123456',
        'packaging_version': '2.2'  # current VERSION
    }
}
```

---

## Configuration Options

Configure packaging behavior in your subscriber configuration:

```json
{
    "name": "kafka_trade_subscriber",
    "type": "kafka",
    "source": "trades_topic",
    "config": {
        "bootstrap_servers": ["localhost:9092"],
        "auto_package_non_dict": true,
        "package_wrapper_key": "payload",
        "add_package_metadata": true,
        "preserve_raw_on_error": true
    }
}
```

### Configuration Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `auto_package_non_dict` | bool | `true` | Enable automatic packaging of non-dict messages |
| `package_wrapper_key` | str | `"payload"` | Key name for original data in packaged dict |
| `add_package_metadata` | bool | `true` | Add subscriber metadata to packaged messages |
| `preserve_raw_on_error` | bool | `true` | If packaging fails, preserve raw data in minimal wrapper |

---

## How It Works

### Message Flow

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  External       │    │  DataSubscriber  │    │   Calculator    │
│  Message Broker │───▶│  _package_msg()  │───▶│   (Receives     │
│  (Kafka, etc.)  │    │                  │    │    dict)        │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                      │                       │
         │                      │                       │
    "CSV,STRING"         {'payload':           process(data)
                          "CSV,STRING",
                          '_packaged': True,
                          ...}
```

### Processing Logic

```python
def _package_message(self, data):
    # If already a dict, return as-is (backward compatible)
    if isinstance(data, dict):
        return data
    
    # If it's a DataAwarePayload, convert to dict
    if isinstance(data, DataAwarePayload):
        return data.to_dict()
    
    # Package non-dict into standardized format
    packaged = {
        self.package_wrapper_key: data,
        '_packaged': True,
        '_original_type': type(data).__name__,
        '_dag_priority': 5,
    }
    
    if self.add_package_metadata:
        packaged['_metadata'] = {
            'subscriber_name': self.name,
            'source': self.source,
            'received_at': datetime.now().isoformat(),
        }
    
    return packaged
```

---

## Examples

### Example 1: CSV String from Kafka

**Input from Kafka:**
```
"TRADE_001,BUY,1000,AAPL,150.25"
```

**Packaged Output:**
```python
{
    'payload': "TRADE_001,BUY,1000,AAPL,150.25",
    '_packaged': True,
    '_original_type': 'str',
    '_dag_priority': 5,
    '_metadata': {
        'subscriber_name': 'kafka_trade_sub',
        'source': 'trades_topic',
        'received_at': '2025-01-13T10:30:00.123456',
        'packaging_version': '2.2'  # current VERSION
    }
}
```

### Example 2: JSON Array from REST API

**Input from REST:**
```python
[{"id": 1, "value": 100}, {"id": 2, "value": 200}]
```

**Packaged Output:**
```python
{
    'payload': [{"id": 1, "value": 100}, {"id": 2, "value": 200}],
    '_packaged': True,
    '_original_type': 'list',
    '_dag_priority': 5,
    '_metadata': {
        'subscriber_name': 'rest_api_sub',
        'source': '/api/data',
        'received_at': '2025-01-13T10:31:00.789012',
        'packaging_version': '2.2'  # current VERSION
    }
}
```

### Example 3: Binary Data from File

**Input:**
```python
b'\x00\x01\x02\x03\x04'
```

**Packaged Output:**
```python
{
    'payload': b'\x00\x01\x02\x03\x04',
    '_packaged': True,
    '_original_type': 'bytes',
    '_dag_priority': 5,
    '_metadata': {
        'subscriber_name': 'file_sub',
        'source': '/data/binary_feed',
        'received_at': '2025-01-13T10:32:00.456789',
        'packaging_version': '2.2'  # current VERSION
    }
}
```

### Example 4: Dictionary (No Packaging Needed)

**Input:**
```python
{"trade_id": "T001", "symbol": "AAPL", "quantity": 100}
```

**Output (Unchanged):**
```python
{"trade_id": "T001", "symbol": "AAPL", "quantity": 100}
```

---

## Calculator Integration

### Handling Packaged Messages

Create calculators that can handle both packaged and non-packaged messages:

```python
from core.calculator.core_calculator import DataCalculator

class TradeParserCalculator(DataCalculator):
    """
    Parses trade messages from various formats.
    Handles both packaged (non-dict) and direct dict inputs.
    """
    
    def __init__(self, name: str, config: dict):
        super().__init__(name, config)
        self.delimiter = config.get('delimiter', ',')
    
    def calculate(self, data: dict) -> dict:
        # Check if this was auto-packaged
        if data.get('_packaged'):
            return self._process_packaged(data)
        else:
            return self._process_direct(data)
    
    def _process_packaged(self, data: dict) -> dict:
        """Process auto-packaged non-dict messages."""
        raw_data = data['payload']
        original_type = data.get('_original_type', 'unknown')
        metadata = data.get('_metadata', {})
        
        if original_type == 'str':
            # Parse CSV string
            parts = raw_data.split(self.delimiter)
            return {
                'trade_id': parts[0],
                'side': parts[1],
                'quantity': int(parts[2]),
                'symbol': parts[3],
                'price': float(parts[4]),
                '_source_metadata': metadata
            }
        elif original_type == 'list':
            # Process list of items
            return {
                'items': raw_data,
                'count': len(raw_data),
                '_source_metadata': metadata
            }
        elif original_type == 'bytes':
            # Decode binary data
            return {
                'raw_bytes': raw_data,
                'length': len(raw_data),
                '_source_metadata': metadata
            }
        else:
            # Unknown type, pass through
            return {
                'data': raw_data,
                'type': original_type,
                '_source_metadata': metadata
            }
    
    def _process_direct(self, data: dict) -> dict:
        """Process direct dictionary input."""
        # Already a dict, process normally
        return data
```

### Using Type Information

```python
class SmartCalculator(DataCalculator):
    """Calculator that uses type information for smart processing."""
    
    def calculate(self, data: dict) -> dict:
        if not data.get('_packaged'):
            return self._default_process(data)
        
        original_type = data.get('_original_type')
        payload = data.get('payload')
        
        processors = {
            'str': self._process_string,
            'int': self._process_number,
            'float': self._process_number,
            'list': self._process_list,
            'bytes': self._process_bytes,
        }
        
        processor = processors.get(original_type, self._process_unknown)
        return processor(payload, data.get('_metadata', {}))
```

---

## Best Practices

### 1. Always Check `_packaged` Flag

```python
def calculate(self, data: dict) -> dict:
    if data.get('_packaged'):
        # Handle packaged message
        raw_data = data['payload']
    else:
        # Handle direct dictionary
        raw_data = data
```

### 2. Preserve Metadata for Tracing

```python
def calculate(self, data: dict) -> dict:
    result = self._process(data)
    
    # Preserve source metadata for debugging
    if '_metadata' in data:
        result['_source_metadata'] = data['_metadata']
    
    return result
```

### 3. Use Type-Safe Processing

```python
def calculate(self, data: dict) -> dict:
    if data.get('_packaged'):
        original_type = data.get('_original_type', 'unknown')
        
        if original_type not in ['str', 'list', 'dict']:
            logger.warning(f"Unexpected type: {original_type}")
            return {'error': f'Unsupported type: {original_type}'}
```

### 4. Configure Per-Subscriber

Different subscribers may need different packaging settings:

```json
{
    "subscribers": [
        {
            "name": "csv_feed",
            "config": {
                "auto_package_non_dict": true,
                "package_wrapper_key": "csv_data"
            }
        },
        {
            "name": "json_feed",
            "config": {
                "auto_package_non_dict": false
            }
        }
    ]
}
```

---

## Troubleshooting

### Issue: Calculator Not Receiving Expected Format

**Symptoms:** Calculator fails with KeyError or unexpected data format

**Solution:** Check if `auto_package_non_dict` is enabled:
```python
# In calculator
logger.info(f"Received data type: {type(data)}")
logger.info(f"Is packaged: {data.get('_packaged', False)}")
logger.info(f"Original type: {data.get('_original_type', 'N/A')}")
```

### Issue: Metadata Not Present

**Symptoms:** `_metadata` key is missing from packaged messages

**Solution:** Enable metadata in subscriber config:
```json
{
    "config": {
        "add_package_metadata": true
    }
}
```

### Issue: Performance Concerns

**Symptoms:** High latency with many messages

**Solution:** Disable metadata if not needed:
```json
{
    "config": {
        "add_package_metadata": false
    }
}
```

### Issue: Backward Compatibility

**Symptoms:** Existing calculators break with new format

**Solution:** Temporarily disable packaging:
```json
{
    "config": {
        "auto_package_non_dict": false
    }
}
```

---

## API Reference

### DataSubscriber Configuration

```python
class DataSubscriber:
    def __init__(self, name, source, config, given_internal_queue=None):
        # Packaging configuration
        self.auto_package_non_dict = config.get('auto_package_non_dict', True)
        self.package_wrapper_key = config.get('package_wrapper_key', 'payload')
        self.add_package_metadata = config.get('add_package_metadata', True)
        self.preserve_raw_on_error = config.get('preserve_raw_on_error', True)
```

### Subscriber Details (JSON)

The `details()` method now includes packaging statistics:

```python
{
    'name': 'kafka_sub',
    'source': 'trades_topic',
    'max_depth': 100000,
    'current_depth': 42,
    'receive_count': 1000,
    'auto_package_enabled': True,
    'packaged_count': 850,
    'package_wrapper_key': 'payload'
}
```

---

## Conclusion

The non-dictionary message packaging feature provides:

- **Consistency**: All calculators receive dictionary format
- **Traceability**: Metadata tracks message origin
- **Flexibility**: Configurable per-subscriber
- **Backward Compatibility**: Dictionary messages pass through unchanged

This enhancement simplifies calculator development and improves debugging capabilities across the DAG pipeline.

---

**Copyright © 2025 Ashutosh Sinha. All rights reserved.**

**DishtaYantra™ is a trademark of Ashutosh Sinha.**
