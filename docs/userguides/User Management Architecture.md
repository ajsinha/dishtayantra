# User Management Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DishtaYantra Compute Server               │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                         Web Layer                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Login   │  │Dashboard │  │  Cache   │  │  Users   │   │
│  │          │  │          │  │   Mgmt   │  │  Mgmt    │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
│       ▲              ▲              ▲              ▲        │
│       └──────────────┴──────────────┴──────────────┘        │
│                         Flask Routes                         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
│  ┌────────────────────────────────────────────────────────┐ │
│  │                     app.py                             │ │
│  │  • Route Handlers                                      │ │
│  │  • Authentication Decorators                           │ │
│  │  • Session Management                                  │ │
│  │  • Error Handlers                                      │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Core Layer                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              UserRegistry (Singleton)                  │ │
│  │  ┌──────────────────────────────────────────────────┐ │ │
│  │  │  • authenticate(username, password)              │ │ │
│  │  │  • get_user(username)                            │ │ │
│  │  │  • add_user(...)                                 │ │ │
│  │  │  • update_user(...)                              │ │ │
│  │  │  • delete_user(username)                         │ │ │
│  │  │  • has_role(username, role)                      │ │ │
│  │  │  • is_admin(username)                            │ │ │
│  │  └──────────────────────────────────────────────────┘ │ │
│  │                                                          │ │
│  │  ┌──────────────────────────────────────────────────┐ │ │
│  │  │        Auto-Refresh Thread                       │ │ │
│  │  │  • Runs every N seconds (configurable)           │ │ │
│  │  │  • Reloads users.json automatically              │ │ │
│  │  └──────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      Data Layer                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │              config/users.json                         │ │
│  │  {                                                     │ │
│  │    "username": {                                       │ │
│  │      "password": "...",                                │ │
│  │      "full_name": "...",                               │ │
│  │      "roles": ["admin", "user"]                        │ │
│  │    }                                                   │ │
│  │  }                                                     │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Request Flow

### Authentication Flow
```
1. User → Login Page
2. Submit Credentials → app.py (POST /login)
3. app.py → UserRegistry.authenticate(username, password)
4. UserRegistry → Checks users.json
5. UserRegistry → Returns True/False
6. app.py → Creates Session (if successful)
7. app.py → Redirects to Dashboard
```

### Authorization Flow
```
1. User → Protected Route
2. app.py → Check Session
3. app.py → @login_required decorator
4. app.py → @admin_required decorator (if needed)
5. UserRegistry → has_role() / is_admin()
6. app.py → Allow or Deny Access
```

### User Management Flow (Admin Only)
```
# User Management System Implementation Guide

## Overview
This implementation provides a comprehensive user management system for the DishtaYantra Compute Server, featuring:
- **UserRegistry** singleton class for thread-safe user management
- Auto-reload capability for user data at configurable intervals
- Full CRUD operations (Create, Read, Update, Delete) for users
- Role-based access control (admin, operator, user)
- Admin-only web interface for user management
- Consistent design with existing cache management interface

## Files

### 1. Core Components
- **user_registry.py** - Singleton UserRegistry class (should be placed in `core/` directory)
- **app.py** - Updated Flask application with UserRegistry integration and user management routes

### 2. Web Interface
- **user_management.html** - User management web page (should be placed in `templates/` directory)
- **base.html** - Updated base template with Users menu link (should be placed in `templates/` directory)

### 3. Configuration
- **application.properties** - Updated configuration with user registry settings


```
project_root/
├── core/
│   ├── __init__.py
│   ├── properties_configurator.py  (existing)
│   ├── user_registry.py            (NEW - place here)
│   └── dag/
├── web/
│   └── app.py                       (REPLACE with new version)
├── templates/
│   ├── base.html                    (REPLACE with new version)
│   ├── cache_management.html        (existing)
│   ├── dashboard.html               (existing)
│   └── user_management.html         (NEW - place here)
├── config/
│   ├── application.properties       (UPDATE with new version)
│   └── users.json                   (will be auto-created)




### Configuration

Edit `config/application.properties` to set the user registry reload interval (optional):

```properties
# User Registry Configuration
# Auto-reload interval for users.json file (in seconds, default: 600 = 10 minutes)
user.registry.reload_interval=600
```

You can also set it to a different value:
- 300 = 5 minutes
- 900 = 15 minutes  
- 1800 = 30 minutes

### Initial Setup

On first run, the system will automatically create a default admin user:
- Username: `admin`
- Password: `admin123`
- Roles: `admin`, `user`

**IMPORTANT**: Change the default admin password immediately after first login!

## UserRegistry Class Features

### Initialization
```python
from core.user_registry import UserRegistry

# Initialize with defaults
user_registry = UserRegistry()

# Initialize with custom settings
user_registry = UserRegistry(
    users_file='config/users.json',
    reload_interval=600  # 10 minutes
)
```

### Key Methods

#### Authentication
```python
# Authenticate user
user_data = user_registry.authenticate(username, password)
if user_data:
    print(f"Welcome {user_data['full_name']}")
```

#### Role Management
```python
# Check if user has a role
if user_registry.has_role('john', 'admin'):
    print("User is admin")

# Check if user has any role from a list
if user_registry.has_any_role('john', ['admin', 'operator']):
    print("User has admin or operator role")

# Get all roles for a user
roles = user_registry.get_all_roles('john')

# Add a role to user
user_registry.add_role('john', 'operator', modified_by='admin')

# Revoke a role from user
user_registry.revoke_role('john', 'operator', modified_by='admin')
```

#### User CRUD Operations
```python
# Create user
user_data = {
    'password': 'secure_password',
    'full_name': 'John Doe',
    'roles': ['user', 'operator']
}
user_registry.create_user('john', user_data, created_by='admin')

# Modify user
user_data = {
    'full_name': 'John Smith',
    'roles': ['user', 'operator', 'admin']
}
user_registry.modify_user('john', user_data, modified_by='admin')

# Delete user
user_registry.delete_user('john', deleted_by='admin')

# Get user info (without password)
user_info = user_registry.get_user('john')

# Get user info (with password)
user_info = user_registry.get_user('john', include_password=True)

# List all users
all_users = user_registry.list_all_users()

# Check if user exists
if user_registry.user_exists('john'):
    print("User exists")

# Get user count
total_users = user_registry.get_user_count()
```

### Thread Safety
The UserRegistry class is thread-safe and uses:
- Singleton pattern (only one instance)
- RLock for reentrant locking
- Atomic file operations

### Auto-Reload Feature
- Automatically checks for file modifications every configured interval
- Reloads user data if the file has changed
- No service restart required for user updates
- Can also manually edit `config/users.json` and changes will be picked up

## Web Interface Usage

### Accessing User Management

1. **Login as Admin:**
   - Navigate to `http://your-server:5002/`
   - Login with admin credentials

2. **Access User Management:**
   - Click on "Users" in the navigation menu (only visible to admins)
   - Or navigate directly to `http://your-server:5002/users`

### Features

#### Statistics Dashboard
- Total Users count
- Admin count
- Operator count  
- Regular User count

#### User Table
- Sortable columns (Username, Full Name, Roles, Created Date)
- Search functionality
- Pagination (10, 50, 100, or All rows)
- Role badges (color-coded)

#### Create User
- Click "Create User" button
- Fill in:
  - Username (required, unique)
  - Password (required)
  - Full Name (optional)
  - Roles (at least one required)
- Supports multiple roles simultaneously

#### Edit User
- Click edit button (pencil icon) for any user
- Update:
  - Password (leave blank to keep current)
  - Full Name
  - Roles
- Cannot remove admin role from last admin

#### Delete User
- Click delete button (trash icon)
- Confirmation dialog appears
- Cannot delete:
  - Your own account
  - Last admin user

### Role Types

1. **admin** - Full system access
   - Can manage DAGs
   - Can manage cache
   - Can manage users
   - Red badge in UI

2. **operator** - Operational access
   - Can view and operate DAGs
   - Can view cache
   - Yellow badge in UI

3. **user** - Basic access
   - Can view DAGs
   - Can view cache
   - Gray badge in UI

Users can have multiple roles simultaneously.

## Security Features

### Protection Mechanisms
1. **Admin-Only Access**: All user management operations require admin role
2. **Self-Deletion Prevention**: Users cannot delete their own accounts
3. **Last Admin Protection**: System prevents deletion of or removing admin role from the last admin
4. **Session Validation**: All API calls validate user session and roles
5. **Password Storage**: Currently plain text (recommend implementing password hashing)

### Security Recommendations

**IMPORTANT**: This implementation stores passwords in plain text for simplicity. For production use, you should:

1. **Add Password Hashing:**
   ```python
   from werkzeug.security import generate_password_hash, check_password_hash
   
   # In user_registry.py, update create_user and modify_user:
   user_data['password'] = generate_password_hash(password)
   
   # In authenticate method:
   if check_password_hash(user['password'], password):
       # Authentication successful
   ```

2. **Add Password Strength Requirements:**
   - Minimum length
   - Mixed case
   - Numbers and special characters

3. **Add Rate Limiting:**
   - Limit login attempts
   - Add temporary account lockout

4. **Add Password Expiration:**
   - Force password changes every N days
   - Track password history

5. **Add Audit Logging:**
   - Log all user management actions
   - Track login attempts

## JSON File Format

The `config/users.json` file has the following structure:

```json
{
  "admin": {
    "password": "admin123",
    "full_name": "System Administrator",
    "roles": ["admin", "user"],
    "created_at": "2025-01-15T10:30:00.123456",
    "created_by": "system"
  },
  "john": {
    "password": "secure_password",
    "full_name": "John Doe",
    "roles": ["user", "operator"],
    "created_at": "2025-01-15T14:20:00.123456",
    "created_by": "admin",
    "modified_at": "2025-01-16T09:15:00.123456",
    "modified_by": "admin"
  }
}
```

### Fields
- **password** (required): User password
- **full_name** (optional): Full display name
- **roles** (required): Array of role names
- **created_at** (auto): ISO timestamp of creation
- **created_by** (auto): Username of creator
- **modified_at** (auto): ISO timestamp of last modification
- **modified_by** (auto): Username of last modifier

## API Endpoints

All endpoints require admin authentication.

### GET /users
Returns the user management page

### GET /users/api/list
Returns list of all users (without passwords)

**Response:**
```json
{
  "success": true,
  "users": [
    {
      "username": "admin",
      "full_name": "System Administrator",
      "roles": ["admin", "user"],
      "created_at": "2025-01-15T10:30:00.123456"
    }
  ],
  "total": 1
}
```

### POST /users/api/create
Create a new user

**Request:**
```json
{
  "username": "john",
  "password": "secure_password",
  "full_name": "John Doe",
  "roles": ["user", "operator"]
}
```

**Response:**
```json
{
  "success": true,
  "message": "User john created successfully"
}
```

### PUT /users/api/update
Update an existing user

**Request:**
```json
{
  "username": "john",
  "password": "new_password",
  "full_name": "John Smith",
  "roles": ["user", "operator", "admin"]
}
```

Note: Password is optional. If not provided, current password is kept.

**Response:**
```json
{
  "success": true,
  "message": "User john updated successfully"
}
```

### DELETE /users/api/delete
Delete a user

**Request:**
```json
{
  "username": "john"
}
```

**Response:**
```json
{
  "success": true,
  "message": "User john deleted successfully"
}
```

### GET /users/api/stats
Get user statistics

**Response:**
```json
{
  "success": true,
  "stats": {
    "total_users": 4,
    "role_counts": {
      "admin": 2,
      "operator": 1,
      "user": 4
    }
  }
}
```


## Troubleshooting

### Issue: Users file not found
**Solution**: The system auto-creates `config/users.json` with default admin user on first run.

### Issue: Cannot login after update
**Solution**: 
1. Check `config/users.json` exists
2. Verify file permissions (should be readable)
3. Check logs for UserRegistry initialization errors

### Issue: Changes to users.json not reflected
**Solution**:
1. Wait for auto-reload interval (default 10 minutes)
2. Or restart the server for immediate effect
3. Check file timestamps in logs

### Issue: Cannot delete last admin
**Solution**: This is by design. Create another admin user first, then delete the original.

### Issue: User menu not visible
**Solution**: Ensure you're logged in as a user with admin role.

## Testing

### Test Script
```python
#!/usr/bin/env python3
"""Test UserRegistry functionality"""

from core.user_registry import UserRegistry

# Initialize
registry = UserRegistry('config/users.json', reload_interval=60)

# Test authentication
user = registry.authenticate('admin', 'admin123')
print(f"Authenticated: {user}")

# Test role check
is_admin = registry.has_role('admin', 'admin')
print(f"Is admin: {is_admin}")

# Test user creation
registry.create_user('testuser', {
    'password': 'test123',
    'full_name': 'Test User',
    'roles': ['user']
}, created_by='admin')

# Test user listing
all_users = registry.list_all_users()
print(f"Total users: {len(all_users)}")

# Test user modification
registry.modify_user('testuser', {
    'roles': ['user', 'operator']
}, modified_by='admin')

# Test role addition
registry.add_role('testuser', 'admin', modified_by='admin')

# Test role revocation  
registry.revoke_role('testuser', 'admin', modified_by='admin')

# Test user deletion
registry.delete_user('testuser', deleted_by='admin')

print("All tests passed!")
```

## Support and Maintenance

### Logging
The UserRegistry class logs all operations to the application log:
- User creation/modification/deletion
- Role additions/revocations
- File reloads
- Errors and warnings

Check logs at: `logs/dagserver.log`

### Monitoring
Key metrics to monitor:
- User count
- Failed login attempts
- Admin user count (should never be zero)
- File reload operations

## Future Enhancements

Potential improvements for production use:
1. Password hashing and salting
2. Password strength requirements
3. Password expiration and history
4. Two-factor authentication
5. Login attempt rate limiting
6. Account lockout after failed attempts
7. User activity logging
8. Role-based permissions (more granular)
9. User groups
10. Email notifications for user operations
11. API key authentication
12. OAuth/SSO integration
13. User self-service password reset


---
**Version**: 1.0  
**Last Updated**: January 2025  
**Author**: DishtaYantra Development Team