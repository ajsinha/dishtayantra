-- ===========================================================================
-- DishtaYantra v2.0.0 - SQLite Schema (users / roles / api_keys)
-- ===========================================================================
-- NOTE: In SQLite mode this schema is created AUTOMATICALLY at startup from
-- the SQLAlchemy ORM metadata (core/db/models.py).  This file is the
-- human-readable reference and must be kept in sync with the models.
-- There is intentionally NO migration framework.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS roles (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        VARCHAR(64)  NOT NULL UNIQUE,
    description VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS users (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    username      VARCHAR(128) NOT NULL,
    password_hash VARCHAR(512) NOT NULL,
    full_name     VARCHAR(255),
    is_active     BOOLEAN      NOT NULL DEFAULT 1,
    created_at    DATETIME     NOT NULL,
    created_by    VARCHAR(128),
    modified_at   DATETIME,
    modified_by   VARCHAR(128),
    CONSTRAINT uq_users_username UNIQUE (username)
);
CREATE INDEX IF NOT EXISTS ix_users_username ON users (username);

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS api_keys (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name         VARCHAR(128) NOT NULL,
    key_hash     VARCHAR(128) NOT NULL,
    key_prefix   VARCHAR(16)  NOT NULL,
    is_active    BOOLEAN      NOT NULL DEFAULT 1,
    created_at   DATETIME     NOT NULL,
    created_by   VARCHAR(128),
    last_used_at DATETIME,
    expires_at   DATETIME,
    CONSTRAINT uq_api_keys_key_hash UNIQUE (key_hash)
);
CREATE INDEX IF NOT EXISTS ix_api_keys_user_id  ON api_keys (user_id);
CREATE INDEX IF NOT EXISTS ix_api_keys_key_hash ON api_keys (key_hash);

-- Default role catalogue (the application also seeds these at startup)
INSERT OR IGNORE INTO roles (name, description) VALUES
    ('admin',    'Full administrative access'),
    ('operator', 'Operational control of DAGs'),
    ('user',     'Read-only / basic access');
