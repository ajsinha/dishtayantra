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

-- Audit trail: append-only record of security/admin-relevant actions.
CREATE TABLE IF NOT EXISTS audit_events (
    id         SERIAL PRIMARY KEY,
    created_at TIMESTAMP    NOT NULL,
    actor      VARCHAR(128),
    action     VARCHAR(64)  NOT NULL,
    target     VARCHAR(256),
    detail     TEXT,
    source_ip  VARCHAR(64),
    success    BOOLEAN      NOT NULL DEFAULT TRUE
);
CREATE INDEX IF NOT EXISTS ix_audit_events_created_at ON audit_events (created_at);
CREATE INDEX IF NOT EXISTS ix_audit_events_actor      ON audit_events (actor);
CREATE INDEX IF NOT EXISTS ix_audit_events_action     ON audit_events (action);

-- NOTE: Flow Time-Travel history lives in its OWN database, never here. Its
-- schema is the dedicated config/schema/flow_events_postgres.sql.

-- Trusted remote servers (UI-plane / service-plane split, v5.29.0). api_key_enc
-- holds the symmetric-encrypted key; never stored/returned in the clear.
CREATE TABLE IF NOT EXISTS trusted_servers (
    id                 SERIAL PRIMARY KEY,
    server_id          VARCHAR(64)  NOT NULL UNIQUE,
    name               VARCHAR(128) NOT NULL,
    url                VARCHAR(512) NOT NULL,
    api_key_enc        TEXT         NOT NULL,
    role               VARCHAR(16)  NOT NULL DEFAULT 'admin',
    verify_tls         BOOLEAN      NOT NULL DEFAULT TRUE,
    added_by           VARCHAR(128),
    added_at           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_probe_at      TIMESTAMP,
    last_probe_ok      BOOLEAN,
    last_probe_version VARCHAR(32)
);
CREATE UNIQUE INDEX IF NOT EXISTS ix_trusted_server_id ON trusted_servers (server_id);

INSERT INTO roles (name, description) VALUES
    ('admin',    'Full administrative access'),
    ('operator', 'Operational control of DAGs'),
    ('user',     'Read-only / basic access')
ON CONFLICT (name) DO NOTHING;
