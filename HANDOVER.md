# DishtaYantra — Handover (state snapshot)

**Version:** 5.38.0   **Date:** 2026-06-23
**Tests:** 303 passing + 1 skipped on a fresh extract (no optional deps);
304 passing with `pip install pypaimon` (the Paimon round-trip then runs for real).

End-of-session snapshot. This session forbade the app-DB-sharing flow store,
implemented Paimon for real (tested), and added an experimental Aerospike
backend. Pair with `NEW_SESSION_PROMPT.md` and the clean archive.

## v5.35.1: Live Logs visible to all authenticated users
Live log page + SSE stream switched admin_required -> login_required; navbar
Manage > Live Logs ungated. Other log endpoints stay admin-only. Security note:
the stream tails dagserver/application/error logs (may contain stack traces /
request detail) - all logged-in users can now see it, by explicit request.

## v5.48.0 (latest): perftest generator --randomrate bursty batch mode
perftest/generate_trades.py: new --randomrate N sends trades in BURSTS, each batch a random int 1..N,
flushed together (one burst downstream). Total still bounded by --count (last batch trimmed); combine with
--rate to bound avg msgs/sec. Implemented via emit_one()/report() helpers + a bursty branch in the send
loop; steady path unchanged. Validation rejects --randomrate<1. Verified against a fake producer: batch
sizes in [1,N], totals == count across cases. Tooling-only; suite still 345. File 454 lines (<500).

## v5.47.0: Slow-node / bottleneck highlighting on the DAG details graph (zero server load)
New "Slow nodes" toggle (#graphBottleneck) on details.html Graph View highlights STALLED (red: in>0, out~0)
and LAGGING (amber: out<0.5*in) nodes, dims keeping-up nodes, and shows a ranked "Slowest nodes" overlay
(#slowPanel; click to centre the node). computeNodeRates now tracks BOTH in/out deltas -> {inRate,outRate}
(old number-shape sessionStorage auto-migrated). classifyNode(r,role): source never flagged; idle when
in<1&&out<1; stalled when out<0.05*in; lagging when out<0.5*in. Toggle persists via sessionStorage
(dagBottleneck). HONEST CAVEAT: equality gate lets a node legitimately emit < it receives, so this flags
for ATTENTION not proof; a true per-node compute-latency timer (perf_counter on hot path) was OFFERED and
deliberately NOT built (user is hot-path/cost sensitive). classifyNode + rate math verified in Node. UI-only;
backend untouched. 345 tests.

## v5.46.0: Per-node total + throughput inside DAG-details node rectangles (zero server load)
The LIVE DAG details Graph View (details.html, DAG_CY - distinct from /flow replay) now shows inside each
node rectangle the cumulative message count + throughput in msgs/min. Throughput is computed CLIENT-SIDE:
the page already renders each node's cumulative messages_out (from build_dag_view) and reloads every ~30s,
so the rate = delta vs the previous snapshot (kept in sessionStorage) normalised to msgs/min. NO new
requests, NO DB scan, NO hot-path rate-meter. Restart (counter reset) clamps to 0; first load shows total
only. fmtNum abbreviates (1.3k/1.3M); nodes size to longest line, height 52; header documents the 2nd line.
computeNodeRates() verified in Node (100->250/30s = 300/min; idle=0; restart=0). Built the real
heartbeat_monitor DAG and confirmed graph_data carries messages_in/out + the tojson injections render.
UI-only; backend untouched. 345 tests.

## v5.45.0: Per-node message counts + dropdown time pickers + unified Clear
Flow graph nodes show a running message count beneath their name during replay (bumpNode in renderEvent;
reset via resetNodeCounts in resetReplay) - DAG lights up partially per data/branch logic. Replaced flaky
datetime-local calendar with dropdowns: Today/Yesterday + hh:mm:ss + AM/PM (pure pickerMs(); 12h->24h +
today/yesterday math verified in Node). Window changes re-slice in-memory buckets client-side (no server
hit). Single CLEAR button (#clearBtn): stopStream (SSE close -> server releases slot) + resetReplay (graph,
counts, events) in one action; removed redundant jump-to-start. UI-only change; backend unchanged. 345 tests.

## v5.44.0: In-memory rolling 24h distribution per DAG + auto-focused chart
Recorder maintains a per-DAG rolling last-24h distribution in memory (120 x 12-min buckets; events +
distinct compute-cycle count per bucket, exact via monotonic cycle_id), updated on the drain thread (off
compute). NEW GET /api/flow/{dag}/distribution serves it O(120), NO DB scan -> bottom-pane chart is cheap
even with several concurrent replays (no per-load SQL aggregate). /histogram (SQL) kept for exact arbitrary
windows. Client auto-focuses the window to the active bucket range on load (no sparse 24h); From/To changes
re-slice client-side (no server hit). _RollingDistribution in core/flow_recorder.py. 4 new tests; 345 total.

## v5.43.0: Editable From/To pickers + compute-cycle distribution chart
Fixed the flow From/To pickers (were min/max-locked to last-24h and re-clamped each edit -> "didn't take");
now freely editable, respects the last-edited field, only caps span to 24h by moving the other field.
Replaced the slider/density bars with a cycle-count DISTRIBUTION CHART: NEW GET /api/flow/{dag}/histogram
?from&to&buckets=120 -> per-bucket cycle/event counts via one SQL GROUP BY (SqliteFlowStore override; ABC
has an iter_export fallback). Bottom pane draws cycles across the window (bars+line+area) + amber playhead
advancing during replay + From/To x-axis labels. Removed draggable timeline handles (picker-driven, simpler).
3 new tests (test_flow_histogram.py); 341 total.

## v5.42.2: Flow replay "Stop" button (explicit disconnect + resource release)
Added a Stop button (#stopBtn) to the flow replay transport. Closes the SSE EventSource -> server generator
hits GeneratorExit -> finally releases the concurrency slot + frees the WAL read snapshot; then resets the
replay and refreshes the "replays N/5" pill. Same release already happens on pause / leaving the page; the
button makes it explicit. Tests: slot released after normal completion AND after early client disconnect. 338 tests.

## v5.42.1: No DB migrations - schema files are the single source of truth
Removed the defensive ALTER TABLE ADD COLUMN cycle_id from 5.42.0. There is now NO schema-migration code
anywhere. The four DDL files (schema_sqlite.sql, schema_postgres.sql, flow_events_sqlite.sql,
flow_events_postgres.sql - one per dialect for core app and flow) are the single source of truth; a DB that
predates a schema change is recreated, never migrated. Core app SQLite is now created by applying
schema_sqlite.sql VERBATIM (DBAPI executescript) instead of SQLAlchemy create_all, so the file - not the ORM -
defines the schema (verified file==ORM produce identical tables first; Postgres already file-provisioned).
cycle_id added to FlowEvent.to_dict + SqlFlowStore write for parity. New guard tests (test_no_migrations.py):
exactly 4 schema files + no ALTER/ADD COLUMN in sources or DDL. 336 tests. NOTE: the legacy users.json import
in core/user_registry.py is a one-time DATA bootstrap, not a schema migration (left in place).

## v5.42.0: Compute-cycle id + cycle-grouped replay + replay caps
NEW compute-cycle id: one engine sweep that does work = one compute cycle (start-to-finish wave). Graph
stamps monotonic cycle_id (_begin_compute_cycle/_commit_compute_cycle; idle sweeps no-gap) before each sweep;
every fire in the sweep carries it. New nullable flow_events.cycle_id (schema + FlowEvent ORM); existing
SQLite flow DBs get the column added defensively at startup. SSE /stream now groups by cycle: pushes N
COMPLETE cycles per message (cycles, default 256), never partial; hard per-push event cap for legacy/huge.
Replay caps: flow_recorder.max_concurrent_streams (default 5; over-cap -> "busy" event) +
flow_recorder.stream_max_seconds (default 120; bounds WAL read snapshot). /api/flow/status reports
active_streams/max_streams; header shows "replays N/5" + warns at capacity. UI topology now from state-at
(all nodes, complete) not a 1500 sample; rows/detail show cycle_id; ES closed on done/error (no auto-reconnect
re-stream). 6 new tests (333 total). Docs: help page, design doc, QUICKSTART.

## v5.41.0: Flow replay scales to huge windows (SSE + bounded rendering)
Fixed /flow freezing on DAGs with 100k+ events. Was: bulk-loaded up to 100k events + scanned the whole
array every animation frame (O(N)). Now: NEW GET /api/flow/{dag}/stream streams the window as SSE batches
(default 1000, via memory-safe iter_export, 24h-capped server-side); client drains a bounded render queue,
event list capped at 300 rows, density built incrementally; page load fetches only a small topology sample.
Added From/To pickers (synced to timeline, 24h cap client+server). Download still exports window as JSONL
(one record/line). No engine/recorder change. 3 new tests (327 total). Docs: help page, design doc, QUICKSTART.

## v5.40.0: SLO / staleness alerting (v1)
New opt-in alerting on the flow change-log. A rule breaches when a DAG (or named node) has no OUTPUT
CHANGE within max_age_seconds. core/alerts.py is pure/decoupled (reads flow store via a provider;
evaluates on demand - no thread). Per-DAG=distinct_dags max_ts; per-node=state_at(now). Config:
alerts.enabled (default false) + alerts.rules_file (JSON; config/alerts.example.json). Surfaced:
GET /api/alerts (login), SLO panel on Admin->System Monitoring (15s poll), dyflow alerts. Honest
scope: equality-gate => output-change staleness (constant streams read stale). Deferred follow-ups:
execution-liveness (increment_compute_count is commented out) + error-rate (errors deque capped 10).
9 new tests. Docs in lockstep: Config Reference, help page+design doc, QUICKSTART, Admin guide.

## v5.39.0: flow-log replay & recovery - design + foundation slice (C6)
Started the replay/recovery program on the shipped flow change-log (no new infra). Landed slice 1:
NEW core/replay.py (read-only, offline): ReplaySource = ordered event tape over a window
(iter_export, filter by node/instance/window) + fidelity_report; recovery_seed/_detail = per-node
latest-output map for warm start; detects __truncated__ payloads and flags them. No engine changes.
6 tests (315 total). Design: docs/design/replay-and-recovery.md (slices + consistency questions:
determinism, egress side-effects, truncation fidelity, warm-start != exactly-once). ROADMAP: C6
under Phase 3 + section 10 mapping every identified enhancement to a phase. Next slices: offline
replay runner over a sandbox ComputeGraph; regression diff + dyflow replay/API; recover_on_start.

## v5.38.1: doc audit - flow control location / per-DAG / CLI
Doc-only. Fixed stale references that said capture is toggled from /flow. QUICKSTART, the
Flow Time-Travel design doc, and the Administration & Maintenance guide now consistently state:
enable/disable lives on Admin -> System Monitoring (global or per-DAG), /flow shows status read-only,
CLI is dyflow enable|disable [--dag NAME]; design doc documents the per-DAG override model. 309 tests.

## v5.38.0: admin-only flow toggle on System Monitoring; per-DAG; CLI
Enable/disable CONTROL moved to Admin -> System Monitoring (/admin/monitoring, admin-only): new Flow
Recording panel with global status + scope dropdown (All DAGs / one DAG) + Enable/Disable. /flow pill is
now READ-ONLY status. Per-DAG: recorder._dag_overrides; effective=override.get(dag,global); API
/api/flow/enable|disable accept ?dag=NAME; status reports dag_overrides. CLI dyflow enable|disable --dag.
Non-admins cannot toggle (admin_required + admin-only page). 309 tests.

## v5.37.1: fix - flow events tagged with the real DAG name
The engine hook recorded dag_id via getattr(node,"dag_id"), which real nodes do NOT have, so every
fire was tagged "default" (the /flow dropdown could only show one DAG). Fix: _event_from_node reads
node._graph.name (set by ComputeGraph.set_graph) with fallbacks; no engine/node change. Test stub
_make_node made realistic (._graph.name, not fake dag_id) + 2 new tests -> 306. UI enable/disable
verified: /flow toggle -> admin-guarded /api/flow/enable|disable; flips status; 401 if unauth.

## v5.37.0: flow UI re-homed into base.html; recording on by default
/flow now extends base.html (navbar/theme/footer); bespoke CSS scoped under .flow-tt (no more
:root/global-element clashes); structural tags -> .ftt-* divs. Removed the synthetic mock generator
from the SERVED page -> honest empty state + disabled "no recorded DAGs" dropdown when empty (mock
remains only in flow_time_travel_standalone.html). flow_recorder.enabled now defaults TRUE (SQLite):
DAG node fires captured out of the box (set false to disable). Verified end-to-end. 304 tests.

## v5.36.1: flow store schema provisioning matches the app DB
SqlFlowStore now mirrors the app DB policy: SQLite auto-creates the flow schema; PostgreSQL does
NOT (apply config/schema/flow_events_postgres.sql once; missing table -> clear startup error).

## v5.36.0: no DB migrations; dedicated schema file per subsystem/dialect
Removed flow-store schema migration (`_migrate`). `SqliteFlowStore` applies the
dedicated `config/schema/flow_events_sqlite.sql` verbatim at startup — the schema
file is the single source of truth; no ALTER path. Core-app schema files
(`schema_sqlite.sql` / `schema_postgres.sql`) now contain ONLY app tables;
`flow_events` removed from them. Flow has its own `flow_events_{sqlite,postgres}.sql`.
Fixed a leak: `FlowEvent` shared the app ORM `Base`, so `create_all()` was creating
an empty `flow_events` in the APP DB — it now uses a separate `FlowBase` (never in
the app DB). `SqlFlowStore` creates from the `.sql` file so PostgreSQL gets
`BIGINT`/`BIGSERIAL` (model `Integer` would overflow on epoch-ms `ts_ms`). 304 tests.
NOTE: a legacy `users.json` → users-table **data** bootstrap remains in
`core/user_registry.py`; it is a data import, not a schema migration — left in place
(ask owner if removal is wanted).

## v5.35.2: documentation audit fixes
Doc-only pass resolving all items in docs/DOC_AUDIT_5_35_1.md (only code change:
new help page + route /help/flow-time-travel). flow-time-travel design doc, QUICKSTART
nav, Config Reference (new flow_recorder section), Admin guide navbar+live-logs note,
MULTI_INSTANCE (fleet SSE + two-node kit + health split), ROADMAP, about.html.

## What shipped previously — v5.35.0
### `store=dao` is FORBIDDEN (flow never in the app DB)
- Factory rejects `dao` with a clear ValueError. `core/flow_store.py` no longer
  defines `DaoFlowStore`.
- `web/dishtayantra_webapp.py`: if `store=dao`, logs an error and disables
  recording (NoopFlowStore). **Safety net:** also refuses any
  `flow_recorder.db_url`/`store_path` that resolves to `db.sqlite.path` (the app
  DB) — recording is disabled rather than co-located.
- Backends now: `sqlite` (default, separate file) · `postgres` · `noop` ·
  `paimon` · `aerospike`.

### Paimon — REAL implementation (tested)
- `core/flow_store_paimon.py`: append-only, hourly-partitioned (`dt=YYYYMMDDHH`,
  UTC) Apache Paimon table via pure-Python PyPaimon. write/query (predicate
  push-down for dag_id/instance/ts-range/nodes/seq)/state_at/distinct_dags/
  instance-filter implemented and **verified on a local FS warehouse**.
- **Verified caveat:** PyPaimon's local FS catalog raises NotImplementedError on
  `drop_partitions` (needs a REST catalog). So partition-drop retention is a
  graceful, logged no-op on local FS; read/write unaffected. Bound retention via
  a REST catalog or the table's `partition.expiration-time`.

### Aerospike — full implementation, EXPERIMENTAL/UNVERIFIED
- `core/flow_store_aerospike.py`: complete API-correct implementation (record
  key `instance|dag|seq`, ts_ms secondary index + client-side filtering, TTL/
  range-delete retention). **Not run against a live cluster** (none available);
  connect/write/read paths are unverified. Labelled honestly; validate before use.

### Provenance / abstraction (from v5.34.0, intact)
- `FlowStore` ABC; `(instance, host, port)` stamped on every event; `query()`/
  `state_at()` `instance` filter; `distinct_dags()` groups by `(instance,dag)`.
  Separate schema files: `config/schema/flow_events_{postgres,sqlite}.sql`.

### Tests
- `tests/test_flow_retention_purge.py`: + `test_dao_store_is_forbidden`,
  `test_paimon_roundtrip_if_available` (real, skips if pypaimon absent),
  `test_aerospike_requires_client_or_cluster` (never silently succeeds without a
  backend). Removed the old DaoFlowStore assertions.

## Honest status of the optional backends
- **sqlite** (default): separate file, fully tested, recommended.
- **postgres** (`SqlFlowStore`): separate DB; tested via a sqlite URL (same
  SQLAlchemy code) — NOT yet run against a live PostgreSQL.
- **paimon**: write/read/query tested on local FS; retention needs REST catalog.
- **aerospike**: EXPERIMENTAL, unverified against a cluster.
- **dao**: removed (forbidden).

## Note on the dev environment
Installing `pypaimon` here downgraded `pandas` 3.0.2 -> 2.3.3 (pypaimon dep). The
full suite still passes at that pandas version. Nothing pins pandas in the repo;
pypaimon is an optional install. A fresh extract without pypaimon skips the
Paimon round-trip test (303 + 1 skip).

## Open decisions / candidates
- Validate `SqlFlowStore` against a real PostgreSQL; validate Aerospike against a
  cluster; consider Paimon REST-catalog retention / `partition.expiration-time`.
- Wire the flow UI to surface/filter by `instance` (data is there).
- Host-scoped admin re-targeting (Phase 5); flow-console SSE; `/api/service/info`
  name vs instance_name.

## Config quick-ref
```
flow_recorder.enabled = true
flow_recorder.store   = sqlite     # separate data/flow_history.db (default)
#                     = postgres ; flow_recorder.db_url = postgresql+psycopg://...
#                     = paimon   ; flow_recorder.warehouse = data/flow_warehouse
#                     = aerospike ; flow_recorder.aerospike_hosts = host:3000  (EXPERIMENTAL)
# store=dao is rejected; flow history never shares the application DB.
```

## Service-plane + flow status
Service-plane Phases 0-4 done; Phase 5 PARTIAL (pooling/health/rotation + fleet
SSE; flow-console SSE + host-admin open). Flow store: time-travel (v5.26) ->
batched purge/maintenance (v5.33.0) -> cached inventory (v5.33.1) -> pluggable
separate-DB/Postgres/provenance (v5.34.0) -> **Paimon real + Aerospike experimental
+ dao forbidden (v5.35.0)**.

## Conventions reminder
Verify-don't-assert · additive/backward-compatible · no silent defaults · no
swallowed exceptions (except defensive recording/audit) · files < 500 lines ·
sh/dash only · `pip --break-system-packages` · Playwright on 8080 · six-point
release ritual · deliverables to `/mnt/user-data/outputs/`, archive extracts to
`dishtayantra/`, cleaned to `.gitignore` but keeping `.gitignore`.
