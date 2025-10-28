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
Add User:
1. Admin → /users/add
2. Submit Form → app.py
3. app.py → UserRegistry.add_user(...)
4. UserRegistry → Updates users dict
5. UserRegistry → Saves to users.json
6. app.py → Redirects to users list

Edit User:
1. Admin → /users/edit/<username>
2. Submit Form → app.py
3. app.py → UserRegistry.update_user(...)
4. UserRegistry → Updates users dict
5. UserRegistry → Saves to users.json
6. app.py → Redirects to users list

Delete User:
1. Admin → Confirms deletion
2. POST /users/delete/<username>
3. app.py → UserRegistry.delete_user(...)
4. UserRegistry → Removes from dict
5. UserRegistry → Saves to users.json
6. app.py → Redirects to users list
```

## Key Design Patterns

### 1. Singleton Pattern (UserRegistry)
```python
class UserRegistry:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
```

**Benefits:**
- Single source of truth for user data
- Consistent state across the application
- Thread-safe access to user information
- Automatic refresh without restart

### 2. Decorator Pattern (Route Protection)
```python
@login_required
@admin_required
def protected_route():
    pass
```

**Benefits:**
- Clean separation of concerns
- Reusable authentication logic
- Easy to apply to any route
- Maintainable code structure

### 3. Repository Pattern (UserRegistry as Data Access)
```python
# app.py doesn't access users.json directly
user_registry.get_user(username)
user_registry.add_user(...)
```

**Benefits:**
- Abstraction of data storage
- Easy to switch storage backend
- Centralized data access logic
- Consistent API for user operations

## Threading Model

```
Main Thread:
├── Flask Application
├── Route Handlers
└── Session Management

Background Thread:
└── Auto-Refresh Loop
    ├── Sleep for N seconds
    ├── Reload users.json
    ├── Update UserRegistry.users
    └── Repeat
```

**Thread Safety:**
- UserRegistry uses threading.Lock()
- Read operations are thread-safe
- Write operations are synchronized
- File I/O is protected

## Security Layers

```
Layer 1: Session Management
├── Flask sessions with secret key
├── HTTP-only cookies
└── Session timeout

Layer 2: Authentication
├── Username/password validation
├── UserRegistry.authenticate()
└── Session creation on success

Layer 3: Authorization
├── Role-based access control
├── Route-level decorators
├── Template-level permissions
└── UserRegistry.has_role()

Layer 4: Business Logic Protection
├── Cannot delete own account
├── Cannot delete last admin
├── Username uniqueness
└── Role validation
```

## Configuration Flow

```
application.properties
        │
        ▼
    app.py (load_config)
        │
        ▼
    UserRegistry.__init__
        │
        ├─→ Sets users_file path
        ├─→ Sets refresh_interval
        └─→ Starts auto-refresh thread
```

## Data Flow

```
users.json
    │
    ▼ (Initial Load)
UserRegistry.users (dict)
    │
    ├─→ (Read) Authentication
    ├─→ (Read) Authorization
    ├─→ (Read) Display Users
    │
    ├─→ (Write) Add User
    ├─→ (Write) Update User
    ├─→ (Write) Delete User
    │
    ▼ (Save)
users.json
    │
    ▼ (Auto-refresh every N seconds)
UserRegistry.users (reloaded)
```

## Scalability Considerations

### Current Implementation
- Single JSON file storage
- In-memory user dictionary
- Thread-safe singleton pattern
- Suitable for small to medium deployments (< 1000 users)

### Future Scalability Options
- Database backend (PostgreSQL, MySQL)
- Redis for session storage
- Distributed cache (Memcached)
- Load balancer with shared session store
- Microservices architecture

## Error Handling

```
Request
  │
  ├─→ Authentication Error → Flash message → Redirect to login
  ├─→ Authorization Error → Flash message → Redirect to dashboard
  ├─→ Validation Error → Flash message → Stay on form
  ├─→ File I/O Error → Log error → Return error message
  ├─→ 404 Not Found → Render 404.html
  └─→ 500 Server Error → Render 500.html
```

## Performance Optimization

### Current Optimizations
- In-memory user cache (UserRegistry.users)
- Background thread for refresh (non-blocking)
- Session-based authentication (no DB query per request)
- Static file caching (Bootstrap CDN)

### Future Optimizations
- Connection pooling for database
- Query result caching
- Lazy loading of user details
- Pagination for user lists
- Search indexing

## Monitoring Points

Key metrics to monitor:
- User login attempts (success/failure)
- Session duration
- User management operations (add/edit/delete)
- Auto-refresh operations
- File I/O errors
- Response times
- Memory usage (UserRegistry size)
- Thread health

## Deployment Topology

```
Development:
    Flask Built-in Server
    ├── localhost:5000
    └── Single process

Production (Recommended):
    Reverse Proxy (nginx)
    │
    ├── SSL/TLS Termination
    │
    └──→ Gunicorn (WSGI Server)
         ├── Worker 1
         ├── Worker 2
         ├── Worker 3
         └── Worker N
         
         └──→ Flask Application
              └──→ UserRegistry (Singleton)
```

## Summary

The DishtaYantra Compute Server implements a clean, modular architecture with:

✅ **Separation of Concerns**: Web, Application, Core, and Data layers
✅ **Singleton Pattern**: Single UserRegistry instance
✅ **Thread Safety**: Protected concurrent access
✅ **Auto-Refresh**: Background user data updates
✅ **Security**: Multiple layers of protection
✅ **Maintainability**: Decorator-based route protection
✅ **Extensibility**: Easy to add new features and roles