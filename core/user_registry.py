"""
User Registry for managing users with auto-reload capability
"""
import json
import os
import threading
import time
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class UserRegistry:
    """
    Singleton thread-safe class for managing users from a JSON file.
    Supports auto-reload of user data at configured intervals.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, users_file: str = 'config/users.json', reload_interval: int = 600):
        """
        Initialize the UserRegistry

        Args:
            users_file: Path to the users JSON file (default: config/users.json)
            reload_interval: Interval in seconds for auto-reload (default: 600 seconds = 10 minutes)
        """
        if hasattr(self, '_initialized'):
            return

        self._initialized = True
        self._users_file = users_file
        self._reload_interval = reload_interval
        self._users: Dict[str, Dict[str, Any]] = {}
        self._users_lock = threading.RLock()
        self._stop_reload = threading.Event()
        self._file_timestamp: float = 0

        # Ensure directory exists
        os.makedirs(os.path.dirname(self._users_file), exist_ok=True)

        # Create default users file if it doesn't exist
        if not os.path.exists(self._users_file):
            self._create_default_users_file()

        # Initial load
        self._load_users()

        # Start auto-reload thread
        self._reload_thread = threading.Thread(target=self._auto_reload_worker, daemon=True)
        self._reload_thread.start()

        logger.info(f"UserRegistry initialized with file: {self._users_file}, reload interval: {self._reload_interval}s")

    def _create_default_users_file(self):
        """Create default users file with admin user"""
        default_users = {
            "admin": {
                "password": "admin123",
                "full_name": "System Administrator",
                "roles": ["admin", "user"],
                "created_at": datetime.now().isoformat(),
                "created_by": "system"
            }
        }
        try:
            with open(self._users_file, 'w') as f:
                json.dump(default_users, f, indent=2)
            logger.info(f"Created default users file: {self._users_file}")
        except Exception as e:
            logger.error(f"Error creating default users file: {str(e)}")

    def _load_users(self):
        """Load users from JSON file"""
        with self._users_lock:
            try:
                if not os.path.exists(self._users_file):
                    logger.warning(f"Users file not found: {self._users_file}")
                    self._users = {}
                    return

                # Track file modification time
                self._file_timestamp = os.path.getmtime(self._users_file)

                with open(self._users_file, 'r', encoding='utf-8') as f:
                    self._users = json.load(f)

                logger.info(f"Loaded {len(self._users)} users from {self._users_file}")
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing users file: {str(e)}")
                self._users = {}
            except Exception as e:
                logger.error(f"Error loading users: {str(e)}")
                self._users = {}

    def _save_users(self):
        """Save users to JSON file"""
        with self._users_lock:
            try:
                # Write to a temporary file first
                temp_file = self._users_file + '.tmp'
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self._users, f, indent=2)

                # Rename to actual file (atomic operation on most systems)
                os.replace(temp_file, self._users_file)

                # Update timestamp
                self._file_timestamp = os.path.getmtime(self._users_file)

                logger.info(f"Saved {len(self._users)} users to {self._users_file}")
                return True
            except Exception as e:
                logger.error(f"Error saving users: {str(e)}")
                return False

    def _auto_reload_worker(self):
        """Worker thread for auto-reloading users"""
        while not self._stop_reload.wait(self._reload_interval):
            try:
                # Check if file has been modified
                if os.path.exists(self._users_file):
                    current_mtime = os.path.getmtime(self._users_file)
                    if current_mtime > self._file_timestamp:
                        logger.info("Users file modified, reloading...")
                        self._load_users()
            except Exception as e:
                logger.error(f"Error in auto-reload: {str(e)}")

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Authenticate a user

        Args:
            username: Username
            password: Password

        Returns:
            User data dictionary if authentication successful, None otherwise
        """
        with self._users_lock:
            user = self._users.get(username)
            if user and user.get('password') == password:
                # Return user data without password
                user_data = user.copy()
                user_data.pop('password', None)
                user_data['username'] = username
                return user_data
            return None

    def has_role(self, username: str, role: str) -> bool:
        """
        Check if a user has a specific role

        Args:
            username: Username
            role: Role to check

        Returns:
            True if user has the role, False otherwise
        """
        with self._users_lock:
            user = self._users.get(username)
            if user:
                roles = user.get('roles', [])
                return role in roles
            return False

    def has_any_role(self, username: str, roles: List[str]) -> bool:
        """
        Check if a user has any of the specified roles

        Args:
            username: Username
            roles: List of roles to check

        Returns:
            True if user has any of the roles, False otherwise
        """
        with self._users_lock:
            user = self._users.get(username)
            if user:
                user_roles = user.get('roles', [])
                return any(role in user_roles for role in roles)
            return False

    def get_all_roles(self, username: str) -> List[str]:
        """
        Get all roles for a user

        Args:
            username: Username

        Returns:
            List of roles, empty list if user not found
        """
        with self._users_lock:
            user = self._users.get(username)
            if user:
                return user.get('roles', []).copy()
            return []

    def add_role(self, username: str, role: str, modified_by: str) -> bool:
        """
        Add a role to a user

        Args:
            username: Username
            role: Role to add
            modified_by: Username of the person making the change

        Returns:
            True if successful, False otherwise
        """
        with self._users_lock:
            user = self._users.get(username)
            if not user:
                logger.warning(f"User not found: {username}")
                return False

            roles = user.get('roles', [])
            if role not in roles:
                roles.append(role)
                user['roles'] = roles
                user['modified_at'] = datetime.now().isoformat()
                user['modified_by'] = modified_by
                self._save_users()
                logger.info(f"Added role '{role}' to user '{username}' by '{modified_by}'")
                return True
            else:
                logger.info(f"User '{username}' already has role '{role}'")
                return False

    def revoke_role(self, username: str, role: str, modified_by: str) -> bool:
        """
        Revoke a role from a user

        Args:
            username: Username
            role: Role to revoke
            modified_by: Username of the person making the change

        Returns:
            True if successful, False otherwise
        """
        with self._users_lock:
            user = self._users.get(username)
            if not user:
                logger.warning(f"User not found: {username}")
                return False

            roles = user.get('roles', [])
            if role in roles:
                roles.remove(role)
                user['roles'] = roles
                user['modified_at'] = datetime.now().isoformat()
                user['modified_by'] = modified_by
                self._save_users()
                logger.info(f"Revoked role '{role}' from user '{username}' by '{modified_by}'")
                return True
            else:
                logger.info(f"User '{username}' does not have role '{role}'")
                return False

    def create_user(self, username: str, user_data: Dict[str, Any], created_by: str) -> bool:
        """
        Create a new user

        Args:
            username: Username
            user_data: User data dictionary (must include 'password', optionally 'full_name', 'roles')
            created_by: Username of the person creating the user

        Returns:
            True if successful, False otherwise
        """
        with self._users_lock:
            if username in self._users:
                logger.warning(f"User already exists: {username}")
                return False

            if 'password' not in user_data:
                logger.error("Password is required for user creation")
                return False

            # Create user with default values
            new_user = {
                'password': user_data['password'],
                'full_name': user_data.get('full_name', username),
                'roles': user_data.get('roles', ['user']),
                'created_at': datetime.now().isoformat(),
                'created_by': created_by
            }

            self._users[username] = new_user
            self._save_users()
            logger.info(f"Created user '{username}' by '{created_by}'")
            return True

    def modify_user(self, username: str, user_data: Dict[str, Any], modified_by: str) -> bool:
        """
        Modify an existing user

        Args:
            username: Username
            user_data: User data dictionary (can include 'password', 'full_name', 'roles')
            modified_by: Username of the person modifying the user

        Returns:
            True if successful, False otherwise
        """
        with self._users_lock:
            if username not in self._users:
                logger.warning(f"User not found: {username}")
                return False

            user = self._users[username]

            # Update fields
            if 'password' in user_data:
                user['password'] = user_data['password']
            if 'full_name' in user_data:
                user['full_name'] = user_data['full_name']
            if 'roles' in user_data:
                user['roles'] = user_data['roles']

            user['modified_at'] = datetime.now().isoformat()
            user['modified_by'] = modified_by

            self._save_users()
            logger.info(f"Modified user '{username}' by '{modified_by}'")
            return True

    def delete_user(self, username: str, deleted_by: str) -> bool:
        """
        Delete a user

        Args:
            username: Username
            deleted_by: Username of the person deleting the user

        Returns:
            True if successful, False otherwise
        """
        with self._users_lock:
            if username not in self._users:
                logger.warning(f"User not found: {username}")
                return False

            # Prevent deleting the last admin
            if self.has_role(username, 'admin'):
                admin_count = sum(1 for user in self._users.values() if 'admin' in user.get('roles', []))
                if admin_count <= 1:
                    logger.error("Cannot delete the last admin user")
                    return False

            del self._users[username]
            self._save_users()
            logger.info(f"Deleted user '{username}' by '{deleted_by}'")
            return True

    def get_user(self, username: str, include_password: bool = False) -> Optional[Dict[str, Any]]:
        """
        Get user data

        Args:
            username: Username
            include_password: Whether to include password in returned data

        Returns:
            User data dictionary or None if not found
        """
        with self._users_lock:
            user = self._users.get(username)
            if user:
                user_data = user.copy()
                if not include_password:
                    user_data.pop('password', None)
                user_data['username'] = username
                return user_data
            return None

    def list_all_users(self, include_passwords: bool = False) -> Dict[str, Dict[str, Any]]:
        """
        Get all users

        Args:
            include_passwords: Whether to include passwords in returned data

        Returns:
            Dictionary of all users
        """
        with self._users_lock:
            if include_passwords:
                return {username: data.copy() for username, data in self._users.items()}
            else:
                result = {}
                for username, data in self._users.items():
                    user_data = data.copy()
                    user_data.pop('password', None)
                    result[username] = user_data
                return result

    def get_user_count(self) -> int:
        """
        Get total number of users

        Returns:
            Number of users
        """
        with self._users_lock:
            return len(self._users)

    def user_exists(self, username: str) -> bool:
        """
        Check if a user exists

        Args:
            username: Username

        Returns:
            True if user exists, False otherwise
        """
        with self._users_lock:
            return username in self._users

    def force_reload(self) -> bool:
        """
        Force an immediate reload of users from the file

        Returns:
            True if reload was successful, False otherwise
        """
        try:
            logger.info("Force reload triggered")
            self._load_users()
            return True
        except Exception as e:
            logger.error(f"Error in force reload: {str(e)}")
            return False

    def stop_reload(self):
        """Stop the auto-reload thread"""
        self._stop_reload.set()
        if hasattr(self, '_reload_thread'):
            self._reload_thread.join(timeout=5)
        logger.info("UserRegistry auto-reload stopped")