-- ===========================================================================
-- DishtaYantra v2.0.0 - PostgreSQL Schema (users / roles / api_keys)
-- ===========================================================================
-- Apply ONCE before running the application in PostgreSQL mode:
--     psql -U <user> -d <database> -f config/schema/schema_postgres.sql
-- There is intentionally NO migration framework; this file is the single
-- authoritative PostgreSQL DDL and must be kept in sync with
-- core/db/models.py.
-- ===========================================================================

CREATE TABLE IF NOT EXISTS roles (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(64)  NOT NULL UNIQUE,
    description VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(128) NOT NULL,
    password_hash VARCHAR(512) NOT NULL,
    full_name     VARCHAR(255),
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMP    NOT NULL,
    created_by    VARCHAR(128),
    modified_at   TIMESTAMP,
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
    id           SERIAL PRIMARY KEY,
    user_id      INTEGER      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name         VARCHAR(128) NOT NULL,
    key_hash     VARCHAR(128) NOT NULL,
    key_prefix   VARCHAR(16)  NOT NULL,
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP    NOT NULL,
    created_by   VARCHAR(128),
    last_used_at TIMESTAMP,
    expires_at   TIMESTAMP,
    CONSTRAINT uq_api_keys_key_hash UNIQUE (key_hash)
);
CREATE INDEX IF NOT EXISTS ix_api_keys_user_id  ON api_keys (user_id);
CREATE INDEX IF NOT EXISTS ix_api_keys_key_hash ON api_keys (key_hash);

INSERT INTO roles (name, description) VALUES
    ('admin',    'Full administrative access'),
    ('operator', 'Operational control of DAGs'),
    ('user',     'Read-only / basic access')
ON CONFLICT (name) DO NOTHING;
