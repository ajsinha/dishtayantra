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


class RestCalculatorError(Exception):
    """Exception raised for REST calculator errors."""
    pass


class RestCalculator:
    """
    Calculator that invokes REST API endpoints via HTTP POST.
    
    The REST endpoint should:
    - Accept POST requests with JSON body
    - Return JSON response
    - Process data in the same format as Python calculators
    
    Authentication Options:
    1. API Key: Sent as header (default: X-API-Key)
    2. Basic Auth: Username/password
    3. Bearer Token: OAuth2 style token
    4. Custom Headers: Any additional headers
    
    Configuration:
    ```python
    config = {
        'calculator': 'rest',
        'endpoint': 'https://api.example.com/calculate',
        
        # Authentication (choose one)
        'auth_type': 'api_key',  # or 'basic', 'bearer', 'none'
        'api_key': 'your-api-key',
        'api_key_header': 'X-API-Key',  # optional, default X-API-Key
        
        # Or for basic auth:
        'auth_type': 'basic',
        'username': 'user',
        'password': 'pass',
        
        # Or for bearer token:
        'auth_type': 'bearer',
        'bearer_token': 'your-token',
        
        # Optional settings
        'timeout': 30,  # seconds
        'retries': 3,
        'retry_backoff': 0.5,
        'verify_ssl': True,
        'custom_headers': {'X-Custom': 'value'},
        
        # Response handling
        'response_path': 'data.result',  # JSONPath to extract result
        'error_on_http_error': True,
        'fallback_value': None,
    }
    ```
    
    Example:
    ```python
    calc = RestCalculator('pricing_api', config)
    result = calc.calculate({'price': 100, 'quantity': 10})
    ```
    """
    
    # Connection pool shared across instances (per endpoint)
    _sessions: Dict[str, 'requests.Session'] = {}
    _sessions_lock = threading.Lock()
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize REST calculator.
        
        Args:
            name: Calculator name
            config: Configuration dictionary
        """
        if not REQUESTS_AVAILABLE:
            raise RuntimeError(
                "requests library is required for REST calculators. "
                "Install with: pip install requests"
            )
        
        self.name = name
        self.config = config
        
        # Required: endpoint URL
        self.endpoint = config.get('endpoint') or config.get('url')
        if not self.endpoint:
            raise ValueError("endpoint URL must be specified for REST calculators")
        
        # Authentication configuration
        self.auth_type = config.get('auth_type', 'none').lower()
        self._setup_auth()
        
        # Request settings
        self.timeout = config.get('timeout', 30)
        self.retries = config.get('retries', 3)
        self.retry_backoff = config.get('retry_backoff', 0.5)
        self.verify_ssl = config.get('verify_ssl', True)
        self.custom_headers = config.get('custom_headers', {})
        
        # Response handling
        self.response_path = config.get('response_path')
        self.error_on_http_error = config.get('error_on_http_error', True)
        self.fallback_value = config.get('fallback_value')
        
        # Request transformation
        self.request_wrapper = config.get('request_wrapper')  # e.g., 'data' wraps as {"data": ...}
        self.include_metadata = config.get('include_metadata', False)
        
        # Statistics
        self._calculation_count = 0
        self._success_count = 0
        self._error_count = 0
        self._total_time_ms = 0.0
        self._last_calculation = None
        self._last_error = None
        self._stats_lock = threading.Lock()
        
        # Get or create session with connection pooling
        self._session = self._get_session()
        
        logger.info(f"RestCalculator '{name}' initialized for endpoint: {self.endpoint}")
    
    def _setup_auth(self):
        """Setup authentication based on config."""
        self.auth = None
        self.auth_headers = {}
        
        if self.auth_type == 'api_key':
            api_key = self.config.get('api_key')
            if not api_key:
                raise ValueError("api_key must be specified for api_key authentication")
            header_name = self.config.get('api_key_header', 'X-API-Key')
            self.auth_headers[header_name] = api_key
            
        elif self.auth_type == 'basic':
            username = self.config.get('username')
            password = self.config.get('password')
            if not username or not password:
                raise ValueError("username and password must be specified for basic authentication")
            self.auth = (username, password)
            
        elif self.auth_type == 'bearer':
            token = self.config.get('bearer_token') or self.config.get('token')
            if not token:
                raise ValueError("bearer_token must be specified for bearer authentication")
            self.auth_headers['Authorization'] = f'Bearer {token}'
            
        elif self.auth_type == 'oauth2':
            # OAuth2 with client credentials
            token = self.config.get('access_token')
            if token:
                self.auth_headers['Authorization'] = f'Bearer {token}'
            # Note: Token refresh would need to be handled externally
            
        elif self.auth_type != 'none':
            raise ValueError(f"Unknown auth_type: {self.auth_type}. "
                           f"Supported: none, api_key, basic, bearer, oauth2")
    
    def _get_session(self) -> 'requests.Session':
        """Get or create a session with connection pooling and retry logic."""
        with self._sessions_lock:
            # Use endpoint as key for session reuse
            endpoint_key = self.endpoint.split('?')[0]  # Remove query params
            
            if endpoint_key not in self._sessions:
                session = requests.Session()
                
                # Configure retry strategy
                retry_strategy = Retry(
                    total=self.retries,
                    backoff_factor=self.retry_backoff,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["POST", "GET"]
                )
                
                adapter = HTTPAdapter(
                    max_retries=retry_strategy,
                    pool_connections=10,
                    pool_maxsize=20
                )
                
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                
                self._sessions[endpoint_key] = session
            
            return self._sessions[endpoint_key]
    
    def _build_headers(self) -> Dict[str, str]:
        """Build request headers."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'DishtaYantra/1.1.0'
        }
        headers.update(self.auth_headers)
        headers.update(self.custom_headers)
        return headers
    
    def _prepare_request_body(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare request body, optionally wrapping data."""
        body = dict(data)
        
        # Add metadata if configured
        if self.include_metadata:
            body['_metadata'] = {
                'calculator': self.name,
                'timestamp': time.time()
            }
        
        # Wrap in container if configured
        if self.request_wrapper:
            body = {self.request_wrapper: body}
        
        return body
    
    def _extract_response(self, response_data: Any) -> Dict[str, Any]:
        """Extract result from response using response_path if configured."""
        if not self.response_path:
            if isinstance(response_data, dict):
                return response_data
            return {'result': response_data}
        
        # Navigate JSONPath-like path (e.g., "data.result")
        result = response_data
        for key in self.response_path.split('.'):
            if isinstance(result, dict):
                result = result.get(key)
            elif isinstance(result, list) and key.isdigit():
                result = result[int(key)]
            else:
                result = None
                break
        
        if isinstance(result, dict):
            return result
        return {'result': result}
    
    def calculate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute calculation by calling REST endpoint.
        
        Args:
            data: Input data dictionary
            
        Returns:
            Response data dictionary
            
        Raises:
            RestCalculatorError: If request fails and error_on_http_error is True
        """
        start_time = time.time()
        
        with self._stats_lock:
            self._calculation_count += 1
            self._last_calculation = time.strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # Prepare request
            headers = self._build_headers()
            body = self._prepare_request_body(data)
            
            # Make request
            response = self._session.post(
                self.endpoint,
                json=body,
                headers=headers,
                auth=self.auth,
                timeout=self.timeout,
                verify=self.verify_ssl
            )
            
            # Calculate elapsed time
            elapsed_ms = (time.time() - start_time) * 1000
            
            with self._stats_lock:
                self._total_time_ms += elapsed_ms
            
            # Check for HTTP errors
            if not response.ok:
                error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                with self._stats_lock:
                    self._error_count += 1
                    self._last_error = error_msg
                
                if self.error_on_http_error:
                    raise RestCalculatorError(error_msg)
                
                logger.warning(f"RestCalculator '{self.name}' HTTP error: {error_msg}")
                
                if self.fallback_value is not None:
                    result = dict(data)
                    result.update(self.fallback_value if isinstance(self.fallback_value, dict) 
                                 else {'result': self.fallback_value})
                    return result
                return dict(data)
            
            # Parse response
            try:
                response_data = response.json()
            except json.JSONDecodeError as e:
                raise RestCalculatorError(f"Invalid JSON response: {e}")
            
            # Extract result
            result = self._extract_response(response_data)
            
            # Merge with input data (input takes precedence for conflicts)
            output = dict(data)
            output.update(result)
            
            with self._stats_lock:
                self._success_count += 1
            
            return output
            
        except requests.exceptions.Timeout:
            elapsed_ms = (time.time() - start_time) * 1000
            error_msg = f"Request timeout after {self.timeout}s"
            
            with self._stats_lock:
                self._error_count += 1
                self._last_error = error_msg
                self._total_time_ms += elapsed_ms
            
            if self.error_on_http_error:
                raise RestCalculatorError(error_msg)
            
            logger.warning(f"RestCalculator '{self.name}' timeout: {error_msg}")
            return dict(data)
            
        except requests.exceptions.RequestException as e:
            elapsed_ms = (time.time() - start_time) * 1000
            error_msg = f"Request failed: {str(e)}"
            
            with self._stats_lock:
                self._error_count += 1
                self._last_error = error_msg
                self._total_time_ms += elapsed_ms
            
            if self.error_on_http_error:
                raise RestCalculatorError(error_msg)
            
            logger.warning(f"RestCalculator '{self.name}' error: {error_msg}")
            return dict(data)
    
    def details(self) -> Dict[str, Any]:
        """Get calculator details and statistics."""
        with self._stats_lock:
            avg_time = (self._total_time_ms / self._calculation_count 
                       if self._calculation_count > 0 else 0)
            
            return {
                'name': self.name,
                'type': 'RestCalculator',
                'language': 'REST API',
                'endpoint': self.endpoint,
                'auth_type': self.auth_type,
                'timeout': self.timeout,
                'retries': self.retries,
                'verify_ssl': self.verify_ssl,
                'calculation_count': self._calculation_count,
                'success_count': self._success_count,
                'error_count': self._error_count,
                'avg_response_time_ms': round(avg_time, 2),
                'last_calculation': self._last_calculation,
                'last_error': self._last_error
            }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the REST endpoint.
        
        Returns:
            Dictionary with health status
        """
        try:
            start_time = time.time()
            
            # Try a simple request (could be GET to a health endpoint)
            health_endpoint = self.config.get('health_endpoint', self.endpoint)
            
            response = self._session.get(
                health_endpoint,
                headers=self._build_headers(),
                auth=self.auth,
                timeout=5,
                verify=self.verify_ssl
            )
            
            elapsed_ms = (time.time() - start_time) * 1000
            
            return {
                'healthy': response.ok,
                'status_code': response.status_code,
                'response_time_ms': round(elapsed_ms, 2),
                'endpoint': health_endpoint
            }
            
        except Exception as e:
            return {
                'healthy': False,
                'error': str(e),
                'endpoint': self.endpoint
            }
    
    @classmethod
    def close_all_sessions(cls):
        """Close all shared sessions. Call on application shutdown."""
        with cls._sessions_lock:
            for session in cls._sessions.values():
                session.close()
            cls._sessions.clear()




# v2.2 module split: the async variant lives in rest_calculator_async.py.
from core.calculator.rest_calculator_async import (  # noqa: E402,F401
    AsyncRestCalculator,
    is_requests_available,
)
