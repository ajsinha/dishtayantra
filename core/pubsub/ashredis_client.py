"""
AshRedis Python Client
A Python client library for connecting to AshRedis Clone server

Usage:
    from ashredis_client import AshRedisClient

    client = AshRedisClient('localhost', 6379)
    client.connect()

    client.set('mykey', 'myvalue')
    value = client.get('mykey')

    client.close()
"""

import socket
import threading
from typing import Optional, Set, Callable, List


class AshRedisClient:
    """Python client for AshRedis Clone"""

    def __init__(self, host: str = 'localhost', port: int = 6379, default_region: Optional[str] = None):
        self.host = host
        self.port = port
        self.default_region = default_region
        self.socket: Optional[socket.socket] = None
        self._buffer = b''
        self._lock = threading.Lock()

        # Subscription management
        self._channel_subscribers = {}
        self._subscription_thread: Optional[threading.Thread] = None
        self._subscription_active = False

    def connect(self):
        """Connect to the AshRedis server"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))

    def is_connected(self) -> bool:
        """Check if connected to server"""
        return self.socket is not None

    def use_region(self, region: str):
        """Set default region for all operations"""
        self.default_region = region

    def ping(self) -> str:
        """PING command"""
        return self._send_command('PING')

    def set(self, key: str, value: str, region: Optional[str] = None,
            ttl_seconds: Optional[int] = None) -> str:
        """SET command"""
        region = region or self.default_region

        cmd = ['SET']
        if region:
            cmd.append(f'@{region}')
        cmd.extend([key, value])

        if ttl_seconds and ttl_seconds > 0:
            cmd.extend(['EX', str(ttl_seconds)])

        return self._send_command(' '.join(cmd))

    def get(self, key: str, region: Optional[str] = None) -> Optional[str]:
        """GET command"""
        region = region or self.default_region

        cmd = ['GET']
        if region:
            cmd.append(f'@{region}')
        cmd.append(key)

        return self._send_command(' '.join(cmd))

    def delete(self, *keys: str, region: Optional[str] = None) -> int:
        """DEL command"""
        region = region or self.default_region

        cmd = ['DEL']
        if region:
            cmd.append(f'@{region}')
        cmd.extend(keys)

        response = self._send_command(' '.join(cmd))
        return int(response) if response else 0

    def exists(self, *keys: str, region: Optional[str] = None) -> int:
        """EXISTS command"""
        region = region or self.default_region

        cmd = ['EXISTS']
        if region:
            cmd.append(f'@{region}')
        cmd.extend(keys)

        response = self._send_command(' '.join(cmd))
        return int(response) if response else 0

    def expire(self, key: str, seconds: int, region: Optional[str] = None) -> bool:
        """EXPIRE command"""
        region = region or self.default_region

        cmd = ['EXPIRE']
        if region:
            cmd.append(f'@{region}')
        cmd.extend([key, str(seconds)])

        response = self._send_command(' '.join(cmd))
        return int(response) == 1 if response else False

    def ttl(self, key: str, region: Optional[str] = None) -> int:
        """TTL command"""
        region = region or self.default_region

        cmd = ['TTL']
        if region:
            cmd.append(f'@{region}')
        cmd.append(key)

        response = self._send_command(' '.join(cmd))
        return int(response) if response else -2

    def persist(self, key: str, region: Optional[str] = None) -> bool:
        """PERSIST command"""
        region = region or self.default_region

        cmd = ['PERSIST']
        if region:
            cmd.append(f'@{region}')
        cmd.append(key)

        response = self._send_command(' '.join(cmd))
        return int(response) == 1 if response else False

    def keys(self, pattern: str = '*', region: Optional[str] = None) -> Set[str]:
        """KEYS command"""
        region = region or self.default_region

        cmd = ['KEYS']
        if region:
            cmd.append(f'@{region}')
        cmd.append(pattern)

        response = self._send_command(' '.join(cmd))
        return self._parse_array(response)

    def incr(self, key: str, region: Optional[str] = None) -> int:
        """INCR command"""
        region = region or self.default_region

        cmd = ['INCR']
        if region:
            cmd.append(f'@{region}')
        cmd.append(key)

        response = self._send_command(' '.join(cmd))
        return int(response) if response else 0

    def decr(self, key: str, region: Optional[str] = None) -> int:
        """DECR command"""
        region = region or self.default_region

        cmd = ['DECR']
        if region:
            cmd.append(f'@{region}')
        cmd.append(key)

        response = self._send_command(' '.join(cmd))
        return int(response) if response else 0

    def append(self, key: str, value: str, region: Optional[str] = None) -> int:
        """APPEND command"""
        region = region or self.default_region

        cmd = ['APPEND']
        if region:
            cmd.append(f'@{region}')
        cmd.extend([key, value])

        response = self._send_command(' '.join(cmd))
        return int(response) if response else 0

    def subscribe(self, channel: str, callback: Callable[[str], None]):
        """Subscribe to a channel"""
        self._channel_subscribers[channel] = callback

        if not self._subscription_active:
            self._start_subscription_thread()

    def unsubscribe(self, channel: str):
        """Unsubscribe from a channel"""
        self._channel_subscribers.pop(channel, None)

    def _start_subscription_thread(self):
        """Start background thread for handling subscriptions"""
        self._subscription_active = True
        self._subscription_thread = threading.Thread(target=self._subscription_loop, daemon=True)
        self._subscription_thread.start()

    def _subscription_loop(self):
        """Background loop for processing subscription messages"""
        try:
            while self._subscription_active and self.is_connected():
                message = self._receive_line()
                if message:
                    self._process_subscription_message(message)
        except Exception as e:
            if self._subscription_active:
                print(f"Subscription error: {e}")

    def _process_subscription_message(self, message: str):
        """Process a subscription message and dispatch to callback"""
        if message.startswith('CHANNEL:'):
            parts = message.split(':', 2)
            if len(parts) == 3:
                channel = parts[1]
                content = parts[2]

                callback = self._channel_subscribers.get(channel)
                if callback:
                    callback(content)

    def _send_command(self, command: str) -> Optional[str]:
        """Send command to server and return response"""
        if not self.is_connected():
            raise ConnectionError("Not connected to server")

        with self._lock:
            self.socket.sendall(f"{command}\n".encode('utf-8'))
            response = self._receive_line()

            if response is None:
                raise ConnectionError("Connection closed by server")

            return self._parse_response(response)

    def _receive_line(self) -> Optional[str]:
        """Receive a line from the socket"""
        while b'\n' not in self._buffer:
            chunk = self.socket.recv(4096)
            if not chunk:
                return None
            self._buffer += chunk

        line, self._buffer = self._buffer.split(b'\n', 1)
        return line.decode('utf-8').strip()

    def _parse_response(self, response: str) -> Optional[str]:
        """Parse server response based on Redis protocol"""
        if not response:
            return None

        first_char = response[0]

        if first_char == '+':
            # Simple string
            return response[1:]
        elif first_char == '-':
            # Error
            raise Exception(f"Error: {response[1:]}")
        elif first_char == ':':
            # Integer
            return response[1:]
        elif first_char == '$':
            # Bulk string
            length = int(response[1:])
            if length == -1:
                return None
            return self._receive_line()
        elif first_char == '*':
            # Array
            return response

        return response

    def _parse_array(self, response: str) -> Set[str]:
        """Parse array response"""
        result = set()

        if response and response.startswith('*'):
            count_str = response[1:].split('\r\n')[0]
            count = int(count_str)

            for _ in range(count):
                length_line = self._receive_line()
                if length_line and length_line.startswith('$'):
                    length = int(length_line[1:])
                    if length > 0:
                        value = self._receive_line()
                        if value:
                            result.add(value)

        return result

    def close(self):
        """Close the connection"""
        self._subscription_active = False

        if self._subscription_thread:
            self._subscription_thread.join(timeout=1)

        if self.socket:
            self.socket.close()
            self.socket = None

    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Example usage
if __name__ == '__main__':
    # Using context manager
    with AshRedisClient('localhost', 6379) as client:
        # PING
        print(f"PING: {client.ping()}")

        # SET and GET
        client.set('mykey', 'Hello, AshRedis!')
        value = client.get('mykey')
        print(f"GET mykey: {value}")

        # Use a specific region
        client.set('user:1', 'John Doe', region='users')
        user = client.get('user:1', region='users')
        print(f"GET user:1 from users: {user}")

        # Increment counter
        client.set('counter', '0')
        new_value = client.incr('counter')
        print(f"Counter after INCR: {new_value}")

        # Check existence
        exists_count = client.exists('mykey', 'counter')
        print(f"Keys exist: {exists_count}")

        # Set expiration
        client.expire('mykey', 60)
        ttl = client.ttl('mykey')
        print(f"TTL for mykey: {ttl} seconds")

        # Pattern matching
        all_keys = client.keys('*')
        print(f"All keys: {all_keys}")

        # Delete
        deleted = client.delete('mykey')
        print(f"Deleted keys: {deleted}")