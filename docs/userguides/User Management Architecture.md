# User Management Architecture

© 2025-2030 Ashutosh Sinha

## Overview

DishtaYantra authenticates users and authorizes actions through a
**database-backed** user system. Users, their roles, and API keys live in a
relational database (SQLite by default, PostgreSQL optionally). Passwords are
never stored in clear text — they are hashed with **PBKDF2-SHA256**.

A flat `config/users.json` file is **only** used for a one-time migration:
when the users table is empty and that file exists, every account is imported
(passwords hashed) and the file is renamed `config/users.json.migrated` so the
clear-text passwords no longer linger on disk. After migration the database is
the single source of truth; the JSON file is not read again.

## System Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                       FastAPI Routes                           │
│   /login   /logout   /user/management   /api/*  (API keys)     │
└───────────────┬────────────────────────────────────────────────┘
                │
        ┌───────▼────────┐        session cookie (Starlette)
        │   AuthGuards    │  login_required / admin_required
        └───────┬────────┘
                │
        ┌───────▼────────┐
        │  UserRegistry   │  singleton, legacy-compatible API
        └───────┬────────┘
                │  delegates to
        ┌───────▼────────┐      ┌──────────────┐
        │    UserDAO      │      │  ApiKeyDAO    │
        └───────┬────────┘      └──────┬───────┘
                │                       │
        ┌───────▼───────────────────────▼───────┐
        │   Relational database (SQLAlchemy)     │
        │   sqlite (default)  |  postgresql      │
        │   tables: users, roles, api_keys       │
        └────────────────────────────────────────┘
```

The `UserRegistry` keeps the same method surface it always had, so calling code
is unchanged; underneath it now delegates to `UserDAO` and `ApiKeyDAO` instead
of reading a JSON file.

## Configuration

Configure the database in `config/application.yaml` (or `.properties`):

```yaml
db:
  engine: sqlite              # sqlite | postgresql
  sqlite:
    path: data/dishtayantra.db
  # For PostgreSQL instead:
  # engine: postgresql
  # postgresql:
  #   host: localhost
  #   port: 5432
  #   database: dishtayantra
  #   user: dishtayantra
  #   password: ${DB_PASSWORD}
```

The SQLite schema is created automatically on first run. For PostgreSQL, apply
`config/schema/schema_postgres.sql` once (there is no migration framework).

## Bootstrap & First Login

On first boot with an empty users table:

1. If `config/users.json` exists, its accounts are migrated (and the file is
   renamed `.migrated`).
2. Otherwise a bootstrap **admin / admin123** account is created.

Change the bootstrap password immediately from the **Users** page
(`/user/management`, admin only).

## Roles

Roles gate what a user can do. The `admin` role unlocks administration —
managing users, cloning DAGs, and the native-module (JVM/C++/Rust) pages.
Default roles are ensured at startup (`ensure_default_roles`). A user may hold
several roles; the system guarantees at least one admin always exists (the last
admin cannot be removed).

## API Keys

Programmatic clients authenticate to `/api/*` endpoints with an **API key**
instead of a session cookie. Keys are created per user, stored hashed, and
resolved by `authenticate_api_key`. Manage them from the Users page.

## Request Flows

### Authentication (interactive)
1. User submits `/login` with username + password.
2. `UserRegistry.authenticate` verifies the PBKDF2 hash via `UserDAO`.
3. On success the username (and roles) are stored in the Starlette session
   cookie; subsequent requests are recognized by `AuthGuards.login_required`.

### Authorization
1. A protected route calls `AuthGuards.login_required` (any logged-in user) or
   `admin_required` (must hold `admin`).
2. The guard checks the session and, for role checks, `has_role` /
   `has_any_role`.
3. Page routes redirect unauthenticated users to `/login`; `/api/*` routes
   return `401`/`403` JSON.

### API-key authentication
1. Client sends its key (header/parameter as documented in the API reference).
2. `authenticate_api_key` resolves the hashed key to a user and roles.

## UserRegistry API

The registry is a singleton with a legacy-compatible API.

### Authentication
```python
user_data = user_registry.authenticate(username, password)
if user_data:
    print(f"Welcome {user_data['full_name']}")

# API-key authentication (for /api/* clients)
user_data = user_registry.authenticate_api_key(clear_key)
```

### Role management
```python
user_registry.has_role('john', 'admin')
user_registry.has_any_role('john', ['admin', 'operator'])
user_registry.get_all_roles('john')
user_registry.add_role('john', 'operator', modified_by='admin')
user_registry.revoke_role('john', 'operator', modified_by='admin')
```

### User CRUD
```python
# Create
user_registry.create_user('john', {
    'password': 'secure_password',
    'full_name': 'John Doe',
    'roles': ['user', 'operator'],
}, created_by='admin')

# Modify
user_registry.modify_user('john', {
    'full_name': 'John Smith',
    'roles': ['user', 'operator', 'admin'],
}, modified_by='admin')

# Delete
user_registry.delete_user('john', deleted_by='admin')

# Read
user_registry.get_user('john')                 # without password hash
user_registry.list_all_users()                 # all users
user_registry.user_exists('john')
user_registry.get_user_count()
```

### API keys
```python
key = user_registry.create_api_key('john', key_name='ci-pipeline',
                                   created_by='admin')
# Store the returned clear key securely; only its hash is persisted.
```

## Security Notes

- Passwords are PBKDF2-SHA256 hashed; clear text is never persisted.
- The one-time JSON migration renames the source file to `.migrated` so
  clear-text passwords are removed from disk.
- API keys are stored hashed and shown in clear only at creation time.
- Sessions use signed cookies; set a strong `SECRET_KEY`.
- At least one admin account is always retained.

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Cannot log in after fresh install | Use bootstrap **admin / admin123**, then change it. |
| Old `users.json` accounts missing | Confirm migration ran — the file should now be `users.json.migrated`; if the users table was non-empty, migration is skipped by design. |
| `SECRET_KEY` error at startup | Export `SECRET_KEY` or set `app.secret_key`. |
| PostgreSQL: relation does not exist | Apply `config/schema/schema_postgres.sql` once. |
| Locked out (no admin) | The system prevents removing the last admin; if the DB is corrupt, restore it or recreate from an empty table to trigger bootstrap. |
