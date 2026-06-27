-- Flow Time-Travel history schema (SQLite) -- single source of truth.
-- A SEPARATE database file from the application DB (data/dishtayantra.db).
-- Applied VERBATIM at startup by SqliteFlowStore (flow_recorder.store_path,
-- default data/flow_history.db). There is no migration path: edit this file to
-- change the schema, and recreate the (transient, ~24h) flow DB if it predates
-- the change.

CREATE TABLE IF NOT EXISTS flow_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    dag_id       TEXT    NOT NULL,
    node_id      TEXT    NOT NULL,
    seq          INTEGER NOT NULL,        -- per-instance monotonic order
    ts_ms        INTEGER NOT NULL,        -- epoch milliseconds
    inputs_json  TEXT,
    output_json  TEXT,
    targets_json TEXT,
    compute_us   INTEGER,
    cycle_id     INTEGER,                  -- compute-cycle group (one engine sweep)
    instance     TEXT,                    -- provenance (see flow_events_postgres.sql)
    host         TEXT,
    port         INTEGER
);

CREATE INDEX IF NOT EXISTS ix_flow_dag_ts          ON flow_events (dag_id, ts_ms);
CREATE INDEX IF NOT EXISTS ix_flow_dag_node_ts     ON flow_events (dag_id, node_id, ts_ms);
CREATE INDEX IF NOT EXISTS ix_flow_instance_dag_ts ON flow_events (instance, dag_id, ts_ms);
CREATE INDEX IF NOT EXISTS ix_flow_dag_cycle        ON flow_events (dag_id, cycle_id);
