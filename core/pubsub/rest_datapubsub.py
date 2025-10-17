"""
REST API DataPublisher and DataSubscriber implementation

Publisher posts data to REST endpoints, Subscriber polls REST endpoints.
"""

import json
import logging
import time
from datetime import datetime
from core.pubsub.datapubsub import DataPublisher, DataSubscriber

try:
    import requests
except ImportError:
    logging.warning("requests library not found. Install with: pip install requests")
    requests = None

logger = logging.getLogger(__name__)


class RESTDataPublisher(DataPublisher):
    """Publisher that sends data to REST API endpoints"""

    def __init__(self, name, destination, config):
        super().__init__(name, destination, config)

        if not requests:
            raise ImportError("requests library required for REST. Install with: pip install requests")

        self.rest_dest_name = destination.replace('rest://', '')
        self.rest_config_file = config.get('rest_config_file')

        # Load REST configuration
        with open(self.rest_config_file, 'r') as f:
            self.rest_config = json.load(f)

        # REST endpoint settings
        self.base_url = self.rest_config.get('base_url')
        self.endpoint = self.rest_config.get('publish_endpoint', self.rest_dest_name)
        self.full_url = f"{self.base_url.rstrip('/')}/{self.endpoint.lstrip('/')}"

        # HTTP method (POST, PUT, PATCH)
        self.http_method = self.rest_config.get('http_method', 'POST').upper()

        # Headers
        self.headers = self.rest_config.get('headers', {})
        if 'Content-Type' not in self.headers:
            self.headers['Content-Type'] = 'application/json'

        # Authentication
        self._setup_auth()

        # Request settings
        self.timeout = self.rest_config.get('timeout', 30)
        self.verify_ssl = self.rest_config.get('verify_ssl', True)
        self.max_retries = self.rest_config.get('max_retries', 3)
        self.retry_delay = self.rest_config.get('retry_delay', 1)

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        if hasattr(self, 'auth'):
            self.session.auth = self.auth

        logger.info(f"REST publisher created for {self.full_url}")

    def _setup_auth(self):
        """Setup authentication based on config"""
        auth_type = self.rest_config.get('auth_type', 'none')

        if auth_type == 'basic':
            username = self.rest_config.get('username')
            password = self.rest_config.get('password')
            self.auth = (username, password)
        elif auth_type == 'bearer':
            token = self.rest_config.get('token')
            self.headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'api_key':
            api_key = self.rest_config.get('api_key')
            key_header = self.rest_config.get('api_key_header', 'X-API-Key')
            self.headers[key_header] = api_key
        # else: no authentication

    def _do_publish(self, data):
        """Send data to REST API"""
        last_error = None

        for attempt in range(self.max_retries):
            try:
                # Prepare request
                if self.http_method == 'POST':
                    response = self.session.post(
                        self.full_url,
                        json=data,
                        timeout=self.timeout,
                        verify=self.verify_ssl
                    )
                elif self.http_method == 'PUT':
                    response = self.session.put(
                        self.full_url,
                        json=data,
                        timeout=self.timeout,
                        verify=self.verify_ssl
                    )
                elif self.http_method == 'PATCH':
                    response = self.session.patch(
                        self.full_url,
                        json=data,
                        timeout=self.timeout,
                        verify=self.verify_ssl
                    )
                else:
                    raise ValueError(f"Unsupported HTTP method: {self.http_method}")

                # Check response
                response.raise_for_status()

                with self._lock:
                    self._last_publish = datetime.now().isoformat()
                    self._publish_count += 1

                logger.debug(f"Published to REST API {self.full_url}: {response.status_code}")
                return

            except requests.exceptions.RequestException as e:
                last_error = e
                logger.warning(f"REST publish attempt {attempt + 1} failed: {str(e)}")

                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
            except Exception as e:
                logger.error(f"Error publishing to REST API: {str(e)}")
                raise

        # All retries failed
        raise Exception(f"Failed to publish after {self.max_retries} attempts: {last_error}")

    def stop(self):
        """Stop the publisher"""
        super().stop()
        if self.session:
            self.session.close()


class RESTDataSubscriber(DataSubscriber):
    """Subscriber that polls data from REST API endpoints"""

    def __init__(self, name, source, config):
        super().__init__(name, source, config)

        if not requests:
            raise ImportError("requests library required for REST. Install with: pip install requests")

        self.rest_source_name = source.replace('rest://', '')
        self.rest_config_file = config.get('rest_config_file')
        self.poll_interval = config.get('poll_interval', 5)

        # Load REST configuration
        with open(self.rest_config_file, 'r') as f:
            self.rest_config = json.load(f)

        # REST endpoint settings
        self.base_url = self.rest_config.get('base_url')
        self.endpoint = self.rest_config.get('subscribe_endpoint', self.rest_source_name)
        self.full_url = f"{self.base_url.rstrip('/')}/{self.endpoint.lstrip('/')}"

        # HTTP method (usually GET)
        self.http_method = self.rest_config.get('http_method', 'GET').upper()

        # Headers
        self.headers = self.rest_config.get('headers', {})

        # Authentication
        self._setup_auth()

        # Request settings
        self.timeout = self.rest_config.get('timeout', 30)
        self.verify_ssl = self.rest_config.get('verify_ssl', True)

        # Query parameters
        self.query_params = self.rest_config.get('query_params', {})

        # Response handling
        self.response_data_key = self.rest_config.get('response_data_key', None)
        self.pagination_enabled = self.rest_config.get('pagination_enabled', False)
        self.pagination_key = self.rest_config.get('pagination_key', 'offset')
        self.pagination_value = 0

        # Tracking
        self.last_id = self.rest_config.get('initial_last_id', None)
        self.last_id_key = self.rest_config.get('last_id_key', 'id')

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        if hasattr(self, 'auth'):
            self.session.auth = self.auth

        logger.info(f"REST subscriber created for {self.full_url}")

    def _setup_auth(self):
        """Setup authentication based on config"""
        auth_type = self.rest_config.get('auth_type', 'none')

        if auth_type == 'basic':
            username = self.rest_config.get('username')
            password = self.rest_config.get('password')
            self.auth = (username, password)
        elif auth_type == 'bearer':
            token = self.rest_config.get('token')
            self.headers['Authorization'] = f'Bearer {token}'
        elif auth_type == 'api_key':
            api_key = self.rest_config.get('api_key')
            key_header = self.rest_config.get('api_key_header', 'X-API-Key')
            self.headers[key_header] = api_key
        # else: no authentication

    def _do_subscribe(self):
        """Poll data from REST API"""
        try:
            # Build query parameters
            params = self.query_params.copy()

            # Add last_id for incremental fetching if configured
            if self.last_id is not None and self.last_id_key:
                params[f'after_{self.last_id_key}'] = self.last_id

            # Add pagination if enabled
            if self.pagination_enabled:
                params[self.pagination_key] = self.pagination_value

            # Make request
            if self.http_method == 'GET':
                response = self.session.get(
                    self.full_url,
                    params=params,
                    timeout=self.timeout,
                    verify=self.verify_ssl
                )
            elif self.http_method == 'POST':
                # Some APIs use POST for queries
                response = self.session.post(
                    self.full_url,
                    json=params,
                    timeout=self.timeout,
                    verify=self.verify_ssl
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {self.http_method}")

            # Check response
            response.raise_for_status()

            # Parse response
            response_data = response.json()

            # Extract data from response
            if self.response_data_key:
                # Data is nested in response
                data = response_data.get(self.response_data_key)
            else:
                # Data is at root level
                data = response_data

            # Handle different response formats
            if data is None:
                logger.debug("No data in response")
                time.sleep(self.poll_interval)
                return None

            # If data is a list, return first item
            if isinstance(data, list):
                if len(data) > 0:
                    item = data[0]

                    # Update last_id if tracking
                    if self.last_id_key and self.last_id_key in item:
                        self.last_id = item[self.last_id_key]

                    # Update pagination
                    if self.pagination_enabled:
                        self.pagination_value += 1

                    logger.debug(f"Received data from REST API {self.full_url}")
                    return item
                else:
                    logger.debug("Empty list in response")
                    time.sleep(self.poll_interval)
                    return None
            else:
                # Single object response
                if self.last_id_key and self.last_id_key in data:
                    self.last_id = data[self.last_id_key]

                logger.debug(f"Received data from REST API {self.full_url}")
                return data

        except requests.exceptions.RequestException as e:
            logger.error(f"REST API error: {str(e)}")
            time.sleep(self.poll_interval)
            return None
        except Exception as e:
            logger.error(f"Error subscribing from REST API: {str(e)}")
            time.sleep(self.poll_interval)
            return None

    def stop(self):
        """Stop the subscriber"""
        super().stop()
        if self.session:
            self.session.close()