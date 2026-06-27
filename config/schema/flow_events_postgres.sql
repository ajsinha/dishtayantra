-- Flow Time-Travel history schema (PostgreSQL) -- v5.34.0
-- Apply this ONCE yourself before enabling flow recording on Postgres:
--   psql -f config/schema/flow_events_postgres.sql
-- The application does NOT auto-create the flow schema on PostgreSQL (it does on
-- SQLite). A SEPARATE database from the application DB. Point flow_recorder.db_url at it:
--   flow_recorder.store  = postgres
--   flow_recorder.db_url = postgresql+psycopg://user:pass@host:5432/flowdb
-- Multiple DishtaYantra instances may share ONE such database; every row carries
-- provenance (instance, host, port) so queries can be scoped to one origin.

CREATE TABLE IF NOT EXISTS flow_events (
    id           BIGSERIAL PRIMARY KEY,
    dag_id       VARCHAR(128) NOT NULL,
    node_id      VARCHAR(128) NOT NULL,
    seq          BIGINT       NOT NULL,   -- per-instance monotonic order
    ts_ms        BIGINT       NOT NULL,   -- epoch milliseconds
    inputs_json  TEXT,
    output_json  TEXT,
    targets_json TEXT,
    compute_us   INTEGER,
    cycle_id     BIGINT,
    instance     VARCHAR(128),            -- provenance: which instance produced this
    host         VARCHAR(255),
    port         INTEGER
);

CREATE INDEX IF NOT EXISTS ix_flow_dag_ts          ON flow_events (dag_id, ts_ms);
CREATE INDEX IF NOT EXISTS ix_flow_dag_node_ts     ON flow_events (dag_id, node_id, ts_ms);
CREATE INDEX IF NOT EXISTS ix_flow_instance_dag_ts ON flow_events (instance, dag_id, ts_ms);

-- Retention is driven by our own bounded sweep (core.flow_retention); rows older
-- than flow_recorder.retention_hours are deleted in batches. PostgreSQL autovacuum
-- reclaims dead tuples, so no application-issued VACUUM is needed.
CREATE INDEX IF NOT EXISTS ix_flow_dag_cycle ON flow_events (dag_id, cycle_id);
