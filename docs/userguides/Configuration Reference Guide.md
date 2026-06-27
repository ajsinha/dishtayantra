# Configuration Reference Guide

A single reference for **every** configuration item in DishtaYantra, with sample
values and explanations. There are two layers:

1. **Server configuration** — `config/application.yaml` (or the classic
   `application.properties`). Controls the process: ports, logging, storage,
   database, async egress, HA, etc.
2. **DAG configuration** — the JSON files under `config/dags/` (and any extra DAG
   folders). Defines subscribers, publishers, calculators, transformers, nodes,
   and edges for each compute graph.

> **Variable substitution** works everywhere in `application.yaml`/`.properties`:
> `${VAR}` and `${VAR:default}` resolve against other keys, environment variables,
> and command-line `--key=value` overrides (nested references supported). Values
> are read as strings and coerced on access (`get_int`/`get_bool`/`get_list`), so
> `port: 5002` and `port: "${PORT:5002}"` behave identically.
>
> **Authoritative version** is always `core/version.py` `VERSION`; `app.version`
> in config is informational only.

---

# Part 1 — Server configuration (`application.yaml`)

## `app`

| Key | Sample | Explanation |
|-----|--------|-------------|
| `app.name` | `DishtaYantra` | Display name. |
| `app.version` | `"5.17.2"` | Informational only; runtime version is `core/version.py`. |
| `app.author.name` / `app.author.email` | `Ashutosh Sinha` / `ajsinha@gmail.com` | Author metadata (About page). |
| `app.copyright.years` / `app.copyright.holder` | `"2025-2030"` / `Ashutosh Sinha` | Copyright metadata. |
| `app.github.repo` | `https://github.com/ajsinha/dishtayantra` | Repo link. |
| `app.trademark.notice` | `"DishtaYantra(TM) ..."` | Trademark notice. |
| `app.secret_key` | `"${SECRET_KEY:...}"` | **Required.** Session signing secret. The server refuses to start without it. Override via the `SECRET_KEY` env var in production. |

## `server`

| Key | Sample | Explanation |
|-----|--------|-------------|
| `server.host` | `0.0.0.0` | Bind address. |
| `server.port` | `5002` | HTTP port. |
| `server.debug` | `false` | Debug mode (verbose; do not use in production). |

## `logging`

| Key | Sample | Explanation |
|-----|--------|-------------|
| `logging.format` | `json` | `text` or `json` (one compact JSON object per line). |
| `logging.level` | `INFO` | Root level: `DEBUG`/`INFO`/`WARNING`/`ERROR`/`CRITICAL`. Changeable at runtime via `/admin/logging`. |
| `logging.json_fields` | `timestamp,level,logger,message` | Comma list from: `timestamp,epoch,level,logger,message,module,func,line,path,process,thread`. |
| `logging.json_include_extra` | `"true"` | Also emit keys passed via `logger.x(..., extra={...})`. |
| `logging.text_format` | `"%(asctime)s - %(name)s - ..."` | Python logging format string when `format: text`. |

## `backpressure` (credit-based flow control, opt-in)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `backpressure.enabled` | `"false"` | When OFF, in-memory fan-out is non-blocking drop-on-full (legacy). When ON, subscribers grant bounded in-flight credits. |
| `backpressure.capacity` | `1000` | Number of in-flight credits per subscriber. |
| `backpressure.policy` | `block` | `block` (publisher waits for a credit — true backpressure) or `drop` (drop, counted, when no credit). |
| `backpressure.timeout_ms` | `0` | Under `block`, max wait before falling back to a drop (`0` = wait forever). With single-threaded producer/consumer, prefer `drop` or a finite timeout. |

## `egress.async` (WAL-backed decoupled publication, opt-in)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `egress.async.enabled` | `"false"` | Master switch for async egress. |
| `egress.async.default` | `"true"` | Default for publishers that don't set `async_egress` explicitly. |
| `egress.worker.count` | `4` | Bounded egress worker-pool size per process. |
| `egress.wal.backend` | `filelog` | `filelog` \| `sqlite` \| `lmdb` \| `memory` (filelog/sqlite are pure stdlib). |
| `egress.wal.path` | `./data/egress_wal` | WAL directory. |
| `egress.wal.max_bytes` | `2147483648` | Hard WAL size cap (2 GiB). |
| `egress.wal.high_water_pct` | `80` | Warn / apply backpressure past this % of `max_bytes`. |
| `egress.wal.segment_bytes` | `134217728` | Filelog segment roll size (128 MiB). |
| `egress.wal.fsync` | `interval` | `always` \| `interval` \| `os`. |
| `egress.wal.fsync_interval_ms` | `50` | Flush cadence when `fsync: interval`. |
| `egress.batch.max_records` | `500` | Max records a drain worker writes per batch. |
| `egress.overflow.policy` | `block` | `block` \| `drop` when the WAL is full. |
| `egress.overflow.block_timeout_ms` | `0` | Under `block`, max wait (`0` = forever). |

## `storage` (all stored objects: DAG configs, holiday calendars, ...)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `storage.provider` | `filesystem` | `filesystem` \| `s3` \| `azureblob` \| `gcs`. |
| `storage.filesystem.root` | `./` | Root for the filesystem provider. |
| `storage.dags.prefix` | `config/dags` | Always-scanned DAG folder. |
| `storage.dags.prefixes` | `config/risk_dags,config/trade_dags` | Extra DAG folders (comma list). Each scanned for its **direct** `.json` children only. DAG names must be globally unique across all folders (collision is fatal at startup). |
| `storage.s3.bucket` / `.prefix` / `.region` / `.endpoint_url` / `.access_key_id` / `.secret_access_key` | `my-bucket` / `dishtayantra` / `us-east-1` / `http://localhost:9000` / `${AWS_ACCESS_KEY_ID:}` / `${AWS_SECRET_ACCESS_KEY:}` | S3 provider settings (`endpoint_url` for MinIO/localstack). |
| `storage.azureblob.connection_string` / `.container` / `.prefix` | `${AZURE_STORAGE_CONNECTION_STRING:}` / `dishtayantra` / `dishtayantra` | Azure Blob settings. |
| `storage.gcs.bucket` / `.prefix` / `.credentials_file` | `my-bucket` / `dishtayantra` / `/etc/gcp/sa.json` | GCS settings. |

## `db` (users, roles, API keys)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `db.engine` | `sqlite` | `sqlite` \| `postgresql`. |
| `db.sqlite.path` | `data/dishtayantra.db` | SQLite file path. |
| `db.postgres.host` / `.port` / `.database` / `.user` / `.password` | `localhost` / `5432` / `dishtayantra` / `${POSTGRES_USER:...}` / `${POSTGRES_PASSWORD:}` | PostgreSQL settings (apply `config/schema/schema_postgres.sql` once). |

## `audit` (audit-trail retention)

The audit trail (Admin &rarr; Audit Trail) is bounded by a background sweep that
deletes old events.

| Key | Sample | Explanation |
|-----|--------|-------------|
| `audit.retention_days` | `15` | Days to keep audit events; older rows are purged. Default `15`. Set to `0` (or &le; 0) to disable purging and keep events indefinitely. |
| `audit.retention_sweep_hours` | `6` | How often the retention sweep runs (hours). Default `6`. The sweep also runs once at startup. |

## `ha` (High Availability manager)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `ha.provider` | `none` | `none` \| `zookeeper` \| `redis` \| `s3` \| `socket`. |
| `ha.zookeeper.hosts` / `.election_path` | `localhost:2181` / `/dishtayantra/election` | ZooKeeper leader election. |
| `ha.redis.host` / `.port` / `.lock_key` / `.lease_seconds` | `localhost` / `6379` / `dishtayantra:ha:lock` / `10` | Redis lock-based HA. |
| `ha.s3.bucket` / `.lease_key` / `.region` / `.lease_seconds` | `my-bucket` / `dishtayantra/ha/lease.json` / `us-east-1` / `15` | S3 lease-based HA. |
| `ha.socket.host` / `.port` / `.retry_seconds` | `0.0.0.0` / `5599` / `3` | Socket-based HA. |

## `holidays` / `schedule` (server-side scheduling refresh)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `holidays.prefix` | `config/holidays` | Folder of holiday-calendar files. |
| `holidays.reload_seconds` | `180` | Calendar reload cadence. |
| `schedule.refresh_seconds` | `240` | Schedule re-evaluation cadence. |

## `external`, `user`, `kafka`, `lmdb`

| Key | Sample | Explanation |
|-----|--------|-------------|
| `external.module.path.<id>` | `/opt/custom_modules` | Extra import paths for custom calculator/transformer plugins (any number of named entries). |
| `user.registry.reload_interval` | `600` | User-registry reload (seconds). |
| `kafka.connection.max_retries` | `5` | Kafka reconnect attempts. |
| `kafka.connection.retry_delay` | `3` | Seconds between reconnects. |
| `kafka.connection.auto_reconnect` | `true` | Auto-reconnect on broker loss. |
| `kafka.connection.health_check_interval` | `30` | Kafka health-check cadence (seconds). |
| `lmdb.db.path` | `${LMDB_DB_PATH:/tmp/dishtayantra_lmdb}` | LMDB transport directory. |
| `lmdb.map.size` | `1073741824` | LMDB map size (1 GiB). |
| `lmdb.max.dbs` / `lmdb.max.readers` | `100` / `126` | LMDB named-DB and reader limits. |
| `lmdb.ttl.seconds` | `300` | Entry TTL. |
| `lmdb.cleanup.interval` | `60` | TTL cleanup cadence (seconds). |

---

## `flow_recorder` (Flow Time-Travel capture, opt-in)

Records the engine's equality-gate change-log so you can replay a DAG's history
and reconstruct state at any point in time (UI: **Manage → Flow Time Travel**).
**On by default** (SQLite). The flow store is always a database **separate** from the
application DB; co-locating with the app DB (the old `store=dao`) is **forbidden**.

| Key | Sample | Explanation |
|-----|--------|-------------|
| `flow_recorder.enabled` | `true` | Capture on at startup. Default **true** (records DAG node fires to the SQLite flow DB). Set `false` to disable. An admin can also toggle capture at runtime from **Admin -> System Monitoring** (globally or per-DAG) or via the `dyflow enable|disable [--dag NAME]` CLI (in-memory; does not persist across restart). |
| `flow_recorder.store` | `sqlite` | Backend: `sqlite` (default) \| `postgres` \| `noop` \| `paimon` \| `aerospike`. `dao` is rejected. |
| `flow_recorder.store_path` | `data/flow_history.db` | `store=sqlite`: the SQLite file (a **separate** file from `db.sqlite.path`). |
| `flow_recorder.db_url` | `postgresql+psycopg://user:pass@host:5432/flowdb` | `store=postgres`: a dedicated SQLAlchemy database (any URL; PostgreSQL needs a driver, e.g. `pip install psycopg[binary]`). Several instances may share one — each row carries provenance `(instance, host, port)`. |
| `flow_recorder.warehouse` | `data/flow_warehouse` | `store=paimon`: Paimon warehouse path (`pip install pypaimon`). |
| `flow_recorder.aerospike_hosts` / `flow_recorder.aerospike_namespace` | `127.0.0.1:3000` / `dishtayantra` | `store=aerospike` (experimental). |
| `flow_recorder.maxsize` | `100000` | Bounded queue between the engine and the drain thread; overflow is dropped (counted), never blocks the hot path. |
| `flow_recorder.max_payload_bytes` | `2048` | Per-side JSON cap on captured input/output snapshots. |
| `flow_recorder.sample_rate` | `1.0` | `1.0` = record every gate fire; lower to shed load. |
| `flow_recorder.retention_hours` | `24` | History window kept; `<= 0` disables purging. |
| `flow_recorder.retention_sweep_minutes` | `30` | Retention sweep cadence; deletes happen in bounded batches. |
| `flow_recorder.maintenance` | `none` | `none` \| `incremental` \| `full` — optional daily SQLite VACUUM (never applied to a shared/app DB). |

## `alerts` (SLO / staleness alerting, opt-in)

**Off by default.** Output-change staleness derived from the flow history: a rule
breaches when a DAG (or a named node) has produced **no output change** within
`max_age_seconds`. Evaluated on demand at `GET /api/alerts` and shown on **Admin →
System Monitoring** (and via `dyflow alerts`). Because of the equality gate this is
*output-change* staleness — a constant-but-healthy stream will read as stale, so
size limits to the slowest legitimate change interval. (Execution-liveness and
error-rate SLOs are planned follow-ups; they need small engine additions.)

| Key | Default | Notes |
|-----|---------|-------|
| `alerts.enabled` | `false` | Evaluate rules. While off, no rules load and `/api/alerts` reports `enabled:false`. |
| `alerts.rules_file` | *(empty)* | Path to a JSON rules file (a top-level list, or `{"rules":[...]}`). Empty ⇒ no rules. See `config/alerts.example.json`. |

Each rule: `{"name": "...", "dag": "trade-etl", "node": "enrich_fx" (optional),
"max_age_seconds": 120}`. Omit `node` for a DAG-level rule. Invalid rules are
skipped (logged); a missing file is treated as no rules — alerting never takes the
server down.

**Schema provisioning (no migrations):** like the application DB, **SQLite
auto-creates** the flow schema from `config/schema/flow_events_sqlite.sql`, while
on **PostgreSQL the application does NOT create the schema** — apply
`config/schema/flow_events_postgres.sql` once yourself
(`psql -f config/schema/flow_events_postgres.sql`). If the `flow_events` table is
missing on Postgres, startup fails with a clear message rather than guessing.

**Backend caveats (read before enabling in production):**
- **postgres** — implemented as dialect-agnostic SQLAlchemy and exercised via SQLite in tests; not yet validated against a live PostgreSQL server.
- **paimon** — real append-only hourly-partitioned backend; on a **local-filesystem** Paimon catalog, partition-drop retention is a no-op (PyPaimon needs a **REST catalog** to drop partitions). Read/write are unaffected; bound retention with a REST catalog or the table's `partition.expiration-time`.
- **aerospike** — complete but **experimental/unverified** against a live cluster.

---

# Part 2 — DAG configuration (JSON)

## DAG top-level keys

| Key | Sample | Explanation |
|-----|--------|-------------|
| `name` | `"trade_etl"` | **Required.** Globally unique DAG name (collisions across folders are fatal). |
| `description` | `"Kafka -> enrich -> sinks"` | Free-text description. |
| `version` | `"5.17.2"` | Optional informational version tag for the DAG. |
| `start_time` / `end_time` | `"0930"` / `null` | Daily active window (HHMM, local). `null` = no bound. |
| `duration` | `null` | Optional run duration. |
| `schedule` | `{ ... }` | Day-of-week + holiday scheduling block (see below). |
| `subscribers` | `[ ... ]` | Input connectors. |
| `publishers` | `[ ... ]` | Output connectors. |
| `calculators` | `[ ... ]` | Calculator definitions (referenced by nodes). |
| `transformers` | `[ ... ]` | Transformer definitions (optional). |
| `nodes` | `[ ... ]` | Compute nodes. |
| `edges` | `[ ... ]` | Directed connections between nodes. |

## `schedule` block

| Key | Sample | Explanation |
|-----|--------|-------------|
| `enabled` | `true` | Whether scheduling is active. |
| `start_time` / `end_time` | `"0930"` / `"1600"` | Daily active window. |
| `timezone` | `"America/New_York"` | IANA timezone for the window. |
| `days_of_week` | `["MON","TUE","WED","THU","FRI"]` | Days the DAG runs. |
| `exclude_days_of_week` | `["SAT","SUN"]` | Days to skip. |
| `holiday_calendars` | `["US"]` | Holiday calendars (files under `holidays.prefix`) on which to skip. |

## Subscriber element

| Key | Sample | Explanation |
|-----|--------|-------------|
| `name` | `"trade_kafka"` | **Required.** Unique within the DAG. |
| `config.source` | `"kafka://topic/trades"` | **Required.** Source URI (scheme table below) or `metronome`. |
| `config.priority` | `5` | Queue priority (priority-queue subscribers). |
| `config.max_depth` | `2000000` | Internal queue capacity. |
| `config.idle_poll_interval` | `0.002` | Sleep (s) when the source is idle. |
| `config.add_package_metadata` | `false` | Wrap each message with packaging metadata. |
| `config.auto_package_non_dict` | `true` | Wrap non-dict payloads into a dict. |
| `config.package_wrapper_key` | `"payload"` | Key used when wrapping non-dict payloads. |
| `config.preserve_raw_on_error` | `true` | Keep the raw message if parsing fails. |
| `config.<connector-specific>` | — | e.g. Kafka `bootstrap_servers`, `group_id`, `kafka_library`; see the connector's own guide. |

## Publisher element

| Key | Sample | Explanation |
|-----|--------|-------------|
| `name` | `"output_kafka"` | **Required.** Unique within the DAG. |
| `config.destination` | `"kafka://topic/enriched"` | **Required.** Destination URI (scheme table below). |
| `config.async_egress` | `true` | Route this publisher through the WAL-backed AsyncPublisher (requires `egress.async.enabled`). In-memory destinations stay inline. |
| `config.batch_size` | `500` | Max messages per publish batch. |
| `config.max_queue_depth` | `100000` | Publisher queue capacity. |
| `config.publish_interval` | `0.0` | Optional pacing between publishes (s). |
| `config.partition_key` | `"symbol"` | **Kafka only.** Name of a field in each outgoing message; its value becomes the Kafka message key, so all messages sharing that value are routed to the **same partition** (preserves per-key ordering, enables co-partition joins and log compaction). When unset, messages are sent with no key and Kafka's default partitioner spreads them (the original behavior). Missing field or non-dict message falls back to no key. |
| `config.<connector-specific>` | — | e.g. `bootstrap_servers`, `kafka_library`; see the connector's own guide. |

## Source / destination URI schemes

| Scheme | Example | Notes |
|--------|---------|-------|
| `kafka://` | `kafka://topic/trades` | Kafka (set `kafka_library`, `bootstrap_servers`, `group_id`). |
| `mem://` / `memory://` / `inmemory://` | `mem://queue/work` | In-process queue/topic. |
| `file://` | `file:///tmp/out.jsonl` | File source/sink (JSONL). |
| `lmdb://` | `lmdb://db/key` | LMDB zero-copy transport. |
| `redis://` / `redischannel://` | `redis://list/key` | Redis list / pub-sub channel. |
| `ashredischannel://` / `inmemoryredischannel://` | — | In-memory Redis-clone channel. |
| `sql://` | `sql://table/trades` | SQL source/sink (with DB pooling). |
| `rest://` / `http://` / `https://` | `rest://host/path` | REST source/sink. |
| `grpc://` | `grpc://host:port/svc` | gRPC. |
| `activemq://` / `rabbitmq://` / `tibcoems://` / `websphere://` | — | JMS/AMQP/EMS/IBM-MQ brokers. |
| `aerospike://` | `aerospike://ns/set` | Aerospike. |
| `sns://` / `sqs://` / `kinesis://` / `eventhubs://` / `servicebus://` / `azureblob://` / `gcs://` | — | Cloud pub/sub + object stores. |
| `fanin://` | `fanin://sub_a,sub_b,sub_c` | Merge several subscribers into one stream. |
| `router://` / `composite://` / `custom://` | — | Message router / composite / custom connector. |
| `metronome` | `metronome` (with `interval`, `message`) | Timer source. |

## Node element

| Key | Sample | Explanation |
|-----|--------|-------------|
| `name` | `"fx_node"` | **Required.** Unique within the DAG. |
| `type` | `"CalculationNode"` | **Required.** Built-in type (table below) or a dotted path to a custom node class. |
| `subscriber` | `"trade_kafka"` | For source nodes: the subscriber to read from. |
| `calculator` | `"fx"` | Calculator name to apply (calculation/subscription nodes). |
| `config` | `{ ... }` | Per-node config (table below). |
| `transformers` | `["t1"]` | Optional node-level transformers (not supported on Arrow source nodes). |

## Node types

| Type | Role | Key config |
|------|------|------------|
| `SubscriptionNode` | Reads one message at a time from a subscriber. | `subscriber`, `calculator` |
| `CalculationNode` | Applies a calculator to upstream input. | `calculator` |
| `BatchingSubscriptionNode` | Drains up to N messages into a dict batch envelope. | `subscriber`, `batch.max_size` (or `max_batch_size`, default 500), `batch_key` |
| `ArrowBatchingSubscriptionNode` | Drains N messages into one columnar `pyarrow.RecordBatch` (zero-copy edges). | `subscriber`, `batch.max_size` |
| `PublicationNode` / `PublisherSinkNode` / `SinkNode` | Publishes to one or more publishers. | `publishers` (list) |
| `FlatteningPublicationNode` | Unbatches a dict batch back to per-message and publishes. | `publishers` |
| `ArrowFlatteningPublicationNode` | Flattens a RecordBatch back to per-row dicts and publishes. | `publishers` |
| `MetronomeNode` | Timer-driven tick source. | `interval` |
| `SubgraphWrapperNode` | Embeds another DAG as a node. | subgraph reference (see Subgraph guide) |
| `<module.path.ClassName>` | Custom node loaded by dotted path. | `Cls(name=name, config=config)` |

## Node `config` keys (built-in)

| Key | Sample | Explanation |
|-----|--------|-------------|
| `batch.max_size` / `max_batch_size` | `5000` / `500` | Max messages drained per cycle (batching/Arrow source nodes). |
| `batch_key` | `"batch"` | Envelope key for dict batches. |
| `publishers` | `["output_file","output_kafka"]` | Publisher names for sink nodes. |
| `interval` | `60` | Tick interval (seconds) for `MetronomeNode`. |
| `extras_key` | `"extras_json"` | (Custom Arrow trade node) column holding non-core attributes. |

## Heterogeneous Arrow source: `NormalizingArrowBatchingSubscriptionNode`

For an Arrow path fed by a source whose records carry **different attribute sets**
per message, `perftest.arrow_trade_nodes.NormalizingArrowBatchingSubscriptionNode`
normalizes each batch to a **stable schema** at ingress: a fixed set of typed
**core columns** plus one JSON `extras_json` column that losslessly carries every
non-core attribute (including nested dicts/lists). Every batch then has identical
columns regardless of input shape, which is what lets a heterogeneous feed ride
the columnar path. (Full mechanism, worked example, and robustness rules are in
the **Arrow Columnar Data Plane tutorial**, "Heterogeneous sources".)

Selected by dotted-path `type`; config keys:

| Key | Sample | Explanation |
|-----|--------|-------------|
| `batch.max_size` | `5000` | Messages drained into one RecordBatch per cycle. |
| `extras_key` | `"extras_json"` | Name of the JSON catch-all column for non-core attributes. |
| `core_fields` | see below | Override the default core schema (omit to use the trade defaults: `trade_id`, `seq`, `symbol`, `side`, `quantity`, `price`, `currency`). |

Each `core_fields` entry is `{name, type, coerce, default}`:

| Field | Values | Meaning |
|-------|--------|---------|
| `name` | `"quantity"` | Output column name. |
| `type` | `string` \| `float64` \| `int64` \| `bool` | Arrow column type. |
| `coerce` | `str` \| `upper` \| `float` \| `int` | Per-value coercion (never raises; bad input → default). |
| `default` | `0.0` | Value used when the attribute is missing. |

```json
{
  "name": "trade_ingest",
  "type": "perftest.arrow_trade_nodes.NormalizingArrowBatchingSubscriptionNode",
  "subscriber": "trade_kafka",
  "calculator": "validate",
  "config": {
    "batch": { "max_size": 5000 },
    "extras_key": "extras_json",
    "core_fields": [
      {"name": "trade_id", "type": "string",  "coerce": "str",   "default": ""},
      {"name": "symbol",   "type": "string",  "coerce": "upper", "default": ""},
      {"name": "quantity", "type": "float64", "coerce": "float", "default": 0.0}
    ]
  }
}
```

## Calculator / transformer element

| Key | Sample | Explanation |
|-----|--------|-------------|
| `name` | `"fx"` | **Required.** Referenced by nodes. |
| `type` | `"perftest.etl_calculators.FxConvertCalculator"` | **Required.** Built-in class name or dotted path. Instantiated as `Cls(**config)` for calculators, `Cls(name=name, config=config)` for nodes. |
| `config` | `{ "default_rate": 1.0 }` | Passed to the constructor. |

## Edge element

| Key | Sample | Explanation |
|-----|--------|-------------|
| `from_node` | `"fx_node"` | **Required.** Source node name. |
| `to_node` | `"notional_node"` | **Required.** Destination node name. |

> RecordBatch edges support fan-**out** (one batch to many nodes, shared by
> reference) but **not** fan-**in** (a node may receive at most one batch edge and
> no other inputs); violations fail fast.

---

# Connector-specific parameters

Each connector accepts additional `config` keys beyond the common ones above
(credentials, brokers, serialization, ack modes, pooling, etc.). Those are
documented exhaustively in the dedicated per-connector guides — e.g. *Kafka
DataPublisher and DataSubscriber Setup Guide*, *Redis ...*, *SQL ...*, *LMDB
Zero-Copy Integration Guide*, *TIBCO EMS ...*, and the cloud pub/sub guides.
This reference covers the common surface and the URI schemes; consult the
connector guide for the long tail of a specific broker.
