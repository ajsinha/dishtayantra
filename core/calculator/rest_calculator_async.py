"""
AsyncRestCalculator (v2.2 module split)
=======================================

Extracted verbatim from rest_calculator.py to respect the 500-line
architecture limit. Re-exported from core.calculator.rest_calculator.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

"""
REST API Calculator Integration for DishtaYantra
================================================

This module provides integration with REST API endpoints as calculators.
REST calculators allow you to invoke external services via HTTP POST requests.

Features:
- HTTP POST to any REST endpoint
- Authentication: API Key, Basic Auth, Bearer Token, Custom Headers
- Configurable timeouts and retries
- Connection pooling for performance
- Response transformation
- Error handling with fallback values

The REST endpoint must accept JSON input and return JSON output.
"""

import logging
import time
import json
from typing import Dict, Any, Optional, List
from urllib.parse import urljoin
import threading

logger = logging.getLogger(__name__)

# Check for requests library
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    logger.warning("requests library not installed. REST calculators will not be available.")



from core.calculator.rest_calculator import RestCalculator, RestCalculatorError

class AsyncRestCalculator:
    """
    Asynchronous version of RestCalculator using aiohttp.
    
    Use this when you need to make concurrent REST calls.
    Requires: pip install aiohttp
    
    Example:
    ```python
    async with AsyncRestCalculator('api', config) as calc:
        result = await calc.calculate(data)
    ```
    """
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """Initialize async REST calculator."""
        try:
            import aiohttp
            self._aiohttp = aiohttp
        except ImportError:
            raise RuntimeError(
                "aiohttp library is required for AsyncRestCalculator. "
                "Install with: pip install aiohttp"
            )
        
        self.name = name
        self.config = config
        self.endpoint = config.get('endpoint') or config.get('url')
        
        if not self.endpoint:
            raise ValueError("endpoint URL must be specified")
        
        self.auth_type = config.get('auth_type', 'none').lower()
        self.timeout = config.get('timeout', 30)
        self.verify_ssl = config.get('verify_ssl', True)
        
        self._session = None
        self._setup_auth()
    
    def _setup_auth(self):
        """Setup authentication."""
        self.auth = None
        self.auth_headers = {}
        
        if self.auth_type == 'api_key':
            api_key = self.config.get('api_key')
            header_name = self.config.get('api_key_header', 'X-API-Key')
            self.auth_headers[header_name] = api_key
            
        elif self.auth_type == 'basic':
            username = self.config.get('username')
            password = self.config.get('password')
            self.auth = self._aiohttp.BasicAuth(username, password)
            
        elif self.auth_type == 'bearer':
            token = self.config.get('bearer_token') or self.config.get('token')
            self.auth_headers['Authorization'] = f'Bearer {token}'
    
    async def __aenter__(self):
        """Async context manager entry."""
        connector = self._aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ssl=self.verify_ssl
        )
        timeout = self._aiohttp.ClientTimeout(total=self.timeout)
        self._session = self._aiohttp.ClientSession(
            connector=connector,
            timeout=timeout
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._session:
            await self._session.close()
    
    async def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute calculation asynchronously."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        headers.update(self.auth_headers)
        
        async with self._session.post(
            self.endpoint,
            json=data,
            headers=headers,
            auth=self.auth
        ) as response:
            response.raise_for_status()
            result = await response.json()
            
            output = dict(data)
            if isinstance(result, dict):
                output.update(result)
            else:
                output['result'] = result
            
            return output
    
    def details(self) -> Dict[str, Any]:
        """Get calculator details."""
        return {
            'name': self.name,
            'type': 'AsyncRestCalculator',
            'language': 'REST API (async)',
            'endpoint': self.endpoint,
            'auth_type': self.auth_type
        }


def is_requests_available() -> bool:
    """Check if requests library is available."""
    return REQUESTS_AVAILABLE
