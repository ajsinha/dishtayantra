"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Per-release highlights are kept in docs/CHANGELOG.md (not here, to keep this
file small). Most recent release:

Version 5.48.0 highlights (perftest generator: --randomrate bursty batch mode):
    - perftest/generate_trades.py gains --randomrate N: trades are sent in BURSTS, each batch a random
      integer from 1 to N trades, flushed together so each burst lands downstream as one group. The total
      is still bounded by --count (last batch trimmed); combine with --rate to bound the average msgs/sec.
    - Lets the perf stream produce ragged, variable-sized bursts - exercising variable compute-cycle sizes
      that the flow distribution chart and per-node throughput surfaces reflect. Steady mode unchanged.

Version 5.47.0 highlights (Slow-node / bottleneck highlighting on the DAG details graph, zero server load):
    - New "Slow nodes" toggle on the DAG details Graph View highlights slacker nodes: STALLED (red - taking
      work in but emitting ~nothing) and LAGGING (amber - emitting far less than it receives), and dims the
      nodes that are keeping up so the laggards stand out. A ranked "Slowest nodes" overlay lists them
      (stalled first), with in/out rates, and clicking one centres it in the graph.
    - Built entirely on the existing client-side in/out rate deltas (now tracking BOTH directions): no new
      requests, no DB scan, no rate-meter on the compute hot path. The toggle state persists across the 30s
      auto-refresh via sessionStorage.
    - Honest by design: because the equality gate lets a node legitimately emit less than it takes in, the
      highlight is a heuristic for ATTENTION, not proof - a true compute-latency timer remains an opt-in we
      have deliberately NOT wired onto the hot path.
    - UI-only change; backend untouched. 345 tests still pass.

Version 5.46.0 highlights (Per-node total + throughput shown inside DAG-details nodes, zero added server load):
    - The live DAG details graph now shows, inside each node rectangle, the cumulative MESSAGE COUNT and a
      THROUGHPUT figure in messages/minute, so slow or stalled nodes stand out at a glance.
    - Throughput is derived CLIENT-SIDE with no extra server work: the page already renders each node's
      cumulative count and reloads on its ~30s cycle, so the rate is the delta between the current and the
      previous snapshot (kept in sessionStorage) normalised to msgs/min. No new requests, no DB scan, and no
      rate-meter on the compute hot path. Counter resets (DAG restart) clamp to 0 rather than going negative.
    - The graph header documents the second line; nodes size to their longest line and gained a little height.
    - UI-only change; backend untouched. 345 tests still pass.

Version 5.45.0 highlights (Per-node message counts, dropdown time pickers, unified Clear button):
    - Each node on the flow replay graph now shows a running MESSAGE COUNT beneath its name as play
      proceeds, so you can watch the DAG light up partially depending on the data and branch logic.
    - Replaced the flaky native calendar From/To pickers with simple dropdowns: Today/Yesterday +
      hh:mm:ss + AM/PM (no calendar widget). The 12h->24h / today-vs-yesterday math was verified.
    - Window changes stay client-side (re-slice the in-memory buckets; no server hit) and the chosen
      range drives both the distribution chart and the replay window.
    - New single CLEAR button: stops the stream (disconnect -> server releases the slot), and resets the
      graph, per-node counts and event list in one action. Removed the redundant jump-to-start button.

Version 5.44.0 highlights (In-memory rolling 24h distribution per DAG + auto-focused chart):
    - The recorder now maintains a rolling last-24h distribution PER DAG in memory (120 x 12-min buckets),
      updated from the drain thread as events are written (event count + distinct compute-cycle count per
      bucket; cycle_ids are monotonic per bucket so the distinct count is exact). Reads are O(120).
    - NEW GET /api/flow/{dag}/distribution serves that roll-up with NO database scan, so the replay
      bottom-pane chart stays cheap even when several flows are viewed at once (no more per-load SQL
      aggregate). /histogram (SQL) remains for an exact aggregate over an arbitrary window.
    - The chart auto-focuses the window to where the DAG was actually active on load (a quiet 24h no longer
      looks sparse); changing From/To re-slices the buckets already in the browser - no further server hits.
    - 4 new tests (345 total). The distribution roll-up runs on the drain thread and can never affect compute.

Version 5.43.0 highlights (Editable From/To pickers + compute-cycle distribution chart):
    - Fixed the flow replay From/To time pickers: they were constrained to a fixed last-24h min/max and
      re-clamped on every edit, so calendar changes appeared not to "take". They are now freely editable;
      the window respects your most-recent edit and only caps the span to 24h (adjusting the other field).
    - Replaced the bottom slider/density bars with a professional compute-cycle DISTRIBUTION CHART. NEW
      GET /api/flow/{dag}/histogram?from&to&buckets=120 returns per-bucket cycle/event counts via a single
      SQL aggregate (no events sent to the browser; base-class iter_export fallback for non-SQLite stores).
      The chart shows cycle counts across the window with an amber playhead that advances during replay.
    - Removed the draggable timeline handles (window is now picker-driven - simpler). 3 new tests (341 total).

Version 5.42.2 highlights (Flow replay "Stop" button - explicit disconnect + resource release):
    - Added a Stop button to the flow replay transport. It closes the SSE EventSource, which makes the
      server generator hit GeneratorExit and release its concurrency slot (and the WAL read snapshot it
      held), then resets the replay and refreshes the "replays N/5" indicator. The same release already
      happens on pause or when leaving the page; the button just makes it explicit and discoverable.
    - Tests: slot released after a stream completes AND after an early client disconnect. 338 tests total.

Version 5.42.1 highlights (No DB migrations - schema files are the single source of truth):
    - Removed the defensive ALTER TABLE ... ADD COLUMN cycle_id introduced in 5.42.0. There is now NO
      schema-migration code anywhere: the four DDL files (schema_sqlite.sql, schema_postgres.sql,
      flow_events_sqlite.sql, flow_events_postgres.sql) are the single source of truth. A DB that predates a
      schema change is recreated, never migrated (flow history is transient, 24h retention, separate file).
    - Core app SQLite now created by applying schema_sqlite.sql VERBATIM (not SQLAlchemy create_all), so the
      file - not the ORM - defines the schema, matching the flow store. Verified the file and ORM produce
      functionally identical tables before switching. PostgreSQL was already file-provisioned.
    - New guard tests: exactly four schema files exist, and no ALTER TABLE / ADD COLUMN appears in the
      Python sources or the DDL files. 336 tests total.

Version 5.42.0 highlights (Compute-cycle id + cycle-grouped replay + replay caps):
    - NEW compute-cycle id: one engine sweep that does work = one compute cycle (a start-to-finish
      propagation wave). The graph stamps a monotonic cycle_id before each sweep; every node fire in that
      sweep carries it. New nullable flow_events.cycle_id column (older rows null; existing SQLite flow DBs
      get the column added defensively at startup). Lets a window be grouped/inspected one whole cycle at a time.
    - SSE replay now groups by compute cycle: /stream pushes a configurable number of COMPLETE cycles per
      message (cycles, default 256) - never a partial cycle - so the UI is always a consistent wave and
      cycles arrive in order. Hard per-push event cap bounds legacy/huge-cycle cases.
    - Replay caps to protect the web tier: max_concurrent_streams (default 5; over the cap -> "server busy")
      and stream_max_seconds (default 120s, also bounds the WAL read snapshot). /api/flow/status now reports
      active_streams/max_streams; the replay page header shows "replays N/5" and flags when at capacity.
    - UI: topology now derived from state-at (ALL nodes, one row each) instead of a 1500-event sample, so the
      drawn graph is complete; event rows + detail show the cycle id. EventSource closed on done/error to
      prevent silent auto-reconnect re-streaming. 6 new tests (333 total).

Version 5.41.0 highlights (Flow replay scales to huge windows: SSE streaming + bounded rendering):
    - Fixed the Flow Time-Travel page freezing on DAGs with hundreds of thousands of events. The page no
      longer bulk-loads the window (it had fetched up to 100k events and scanned the whole array every
      animation frame). NEW GET /api/flow/{dag}/stream streams the window as SSE batches (default 1000,
      via the memory-safe iter_export); the client consumes batches through a bounded render queue and a
      capped (300-row) event list, so memory/DOM stay flat. From/To time pickers (synced with the timeline,
      24h cap, enforced client+server). Download still exports the window as JSONL (one record per line).
      3 new tests. No engine/recorder change.

Version 5.40.0 highlights (SLO / staleness alerting v1):
    - New opt-in alerting built on the flow change-log: a rule breaches when a DAG (or a named node)
      has produced no OUTPUT CHANGE within max_age_seconds. core/alerts.py is pure/decoupled (reads the
      flow store via a provider; evaluates on demand - no background thread). Per-DAG uses distinct_dags
      (max_ts); per-node uses state_at(now). Config: alerts.enabled (default false) + alerts.rules_file
      (JSON; see config/alerts.example.json). Surfaced at GET /api/alerts, an SLO panel on Admin -> System
      Monitoring, and `dyflow alerts`. Honest scope: equality-gate means this is output-change staleness
      (constant-but-healthy streams read as stale). Execution-liveness + error-rate SLOs are deferred
      follow-ups (need small engine additions). 9 new tests.


Version 5.39.0 highlights (flow-log replay & recovery: design + foundation slice):
    - Started the C6 program: replay a recorded flow window back through the engine, and warm-start
      recovery by seeding node outputs from the log - both built on the already-shipped Flow Time-Travel
      change-log, needing no new infrastructure (a frugal on-ramp to B2, not a replacement).
    - Landed slice 1: NEW core/replay.py - read-only, offline foundation. ReplaySource streams a
      recorded window as an ordered event tape (memory-bounded via iter_export, filter by node/instance/
      window) with a fidelity_report; recovery_seed/recovery_seed_detail turn the log into a per-node
      latest-output map for warm start. Detects __truncated__ payloads (capped at capture) and flags
      them - faithful replay needs un-truncated payloads. No engine changes. 6 new tests (315 total).
    - Design: docs/design/replay-and-recovery.md (slices, the hard consistency questions - determinism,
      egress side-effects, payload-truncation fidelity, warm-start-not-exactly-once). ROADMAP updated:
      C6 added under Phase 3, plus a section mapping every recently identified enhancement to a phase.


Version 5.38.1 highlights (doc audit: flow control location/per-DAG/CLI):
    - Fixed stale docs that still said capture is toggled from the /flow page. QUICKSTART now states
      /flow shows recording status read-only and that enable/disable (global or per-DAG) lives on
      Admin -> System Monitoring; the design doc and Administration guide were corrected the same way
      and now document the per-DAG override model and the dyflow enable|disable [--dag] CLI. Doc-only.

Version 5.38.0 highlights (admin-only flow toggle on System Monitoring; per-DAG; CLI):
    - The flow enable/disable CONTROL moved to the admin System Monitoring page (/admin/monitoring,
      admin-only). The /flow page now shows recording status READ-ONLY (the pill is no longer clickable).
      Non-admins cannot toggle (server already admin-guards; the control is also only on an admin page).
    - Per-DAG enable/disable: admin can toggle capture for ALL DAGs or a single DAG (scope dropdown on the
      monitoring panel). Recorder grew a _dag_overrides map; effective = override.get(dag, global). Hot
      path stays cheap (fast-return when globally off with no overrides). API /api/flow/enable|disable
      accept optional ?dag=NAME; status/stats report dag_overrides.
    - CLI: dyflow enable|disable now take --dag NAME (omit for all DAGs); status prints per-DAG overrides.
    - 306 -> 309 tests (per-DAG on/off + override-status). Docs (help page, Config Reference) updated.

Version 5.37.1 highlights (fix: flow events now tagged with the real DAG name):
    - Bug: the engine hook recorded dag_id via getattr(node,"dag_id") - an attribute real nodes do NOT
      have - so every captured fire was tagged "default" (the /flow dropdown could only ever show one
      DAG). Fix: _event_from_node resolves the DAG from node._graph.name (set by ComputeGraph.set_graph),
      with fallbacks. No engine/node code change. Added engine-hook tests (graph-name + default).
      306 tests. (UI enable/disable confirmed working: /flow toggle -> admin-guarded /api/flow/enable|disable.)

Version 5.37.0 highlights (flow UI re-homed into base.html; recording on by default):
    - The /flow page now EXTENDS base.html (standard navbar/theme/layout) instead of being a standalone
      HTML document. Its bespoke CSS is scoped under .flow-tt so it no longer redefines :root vars or
      global element selectors (was clashing with Bootstrap). Markup wrapped in .flow-tt with ftt-* divs.
    - Removed the synthetic/mock data generator from the SERVED page. When there is no recorded data the
      page now shows an honest EMPTY STATE (and the DAG dropdown shows "no recorded DAGs", disabled)
      instead of fabricated trade-ETL data. The synthetic generator remains only in the standalone demo.
    - flow_recorder.enabled now defaults to TRUE (SQLite): DAG node fires are captured out of the box.
      Set flow_recorder.enabled=false to disable. Verified end-to-end (record -> drain -> sqlite -> query).
    - Docs (design doc, Config Reference, help page) updated to on-by-default. 304 tests.

Version 5.36.1 highlights (flow store schema provisioning matches the app DB policy):
    - SqlFlowStore now follows the same rule as the application DB: on SQLite the application CREATES
      the flow schema (from flow_events_sqlite.sql); on PostgreSQL it does NOT create - apply
      config/schema/flow_events_postgres.sql once yourself. Missing table on PG -> clear startup error.

Version 5.36.0 highlights (no DB migrations; one dedicated schema file per subsystem/dialect):
    - Removed ALL flow-store schema migration. SqliteFlowStore no longer ALTERs old DBs; it applies
      the dedicated config/schema/flow_events_sqlite.sql verbatim at startup. The schema FILE is the
      single source of truth - to change it, edit the file and recreate the (transient ~24h) flow DB.
    - Clean separation of schema files: the core-app schema files (schema_sqlite.sql / schema_postgres.sql)
      now contain ONLY app tables (users/roles/keys/audit/trusted_servers); the flow_events table was
      removed from them. Flow has its own dedicated files (flow_events_sqlite.sql / flow_events_postgres.sql).
    - Fixed a real leak: FlowEvent shared the app ORM Base, so Base.metadata.create_all() was creating an
      empty flow_events table INSIDE the application DB. FlowEvent now uses a separate FlowBase, so it is
      never created in the app DB. (Reinforces: flow history never co-locates with the app DB.)
    - SqlFlowStore now creates its table from the dedicated .sql file (not the ORM model), so PostgreSQL
      gets BIGINT/BIGSERIAL - the model Integer would 32-bit-overflow on epoch-ms ts_ms.
    - 304 tests (migration test replaced with a schema-from-file test).

Version 5.35.2 highlights (documentation audit fixes):
    - Documentation pass fixing all items from the v5.35.1 audit (DOC_AUDIT_5_35_1.md). No code
      behaviour change except a NEW help page + route (/help/flow-time-travel).
    - flow-time-travel design doc rewritten to current reality (backends sqlite|postgres|noop|paimon|
      aerospike; dao FORBIDDEN; provenance; separate-DB; Paimon/Aerospike/Postgres caveats).
    - Configuration Reference Guide: added the full flow_recorder section (all keys + caveats).
    - NEW Flow Time-Travel help page (web/templates/help/flow_time_travel.html) + route + index card.
    - QUICKSTART navigation map corrected (real paths; Manage/Admin/About grouping; live-logs access).
    - Administration guide: corrected navbar diagram; added Live Logs access + log-exposure note.
    - MULTI_INSTANCE: fixed stale "SSE is planned" (fleet SSE shipped); added two-node kit +
      DY_CONFIG_FILE + reachable/manageable health coverage. ROADMAP + about.html minor fixes.

Version 5.35.1 highlights (Live Logs visible to all authenticated users):
    - Live Logs (page /admin/logs/live + SSE stream /admin/logs/live/stream) changed from
      admin_required to login_required: any logged-in user can view the live log tail. The navbar
      'Manage > Live Logs' item is no longer admin-gated. All OTHER log endpoints (system logs
      page/api/download, logging control/apply) remain admin-only. No data-model change.

Version 5.35.0 highlights (Paimon implemented, Aerospike experimental, dao forbidden):
    - store=dao is now FORBIDDEN: flow history must never share the application database. The factory
      rejects it; the webapp disables recording (degrade-to-inert) and also refuses any flow db_url/
      path that resolves to the application DB. Use store=sqlite or store=postgres (both separate DBs).
    - PaimonFlowStore is now a REAL implementation (core/flow_store_paimon.py) on an append-only,
      hourly-partitioned Apache Paimon table via pure-Python PyPaimon. write/query (predicate push-
      down)/state_at/distinct_dags/instance-filter are implemented and TESTED on a local filesystem
      warehouse. Caveat (verified): PyPaimon's local FS catalog cannot drop partitions programmatically
      (needs a REST catalog), so partition-drop retention is a graceful no-op there (read/write
      unaffected); bound retention via a REST catalog or the table's partition.expiration-time.
    - AerospikeFlowStore is a full, API-correct implementation (core/flow_store_aerospike.py) but is
      EXPERIMENTAL: it could not be run against a live Aerospike cluster in development, so the
      connect/write/read paths are unverified. Clearly labelled rather than presented as done.
    - Additive (new backends optional, dao removal is the only breaking change by explicit design).
      303 tests (+1 paimon round-trip when pypaimon installed = 304).

Version 5.34.0 highlights (pluggable flow store: separate DB, Postgres, provenance):
    - Formalised the flow-store contract as a FlowStore ABC; all backends implement it. Backends now:
      sqlite (default, separate file), postgres (NEW), dao (shares app DB), noop, paimon (stub),
      aerospike (NEW stub). Factory make_flow_store selects by flow_recorder.store.
    - NEW SqlFlowStore (core/flow_store_sql.py): persists flow history to its OWN SQLAlchemy engine -
      a SEPARATE database from the application DB - selected by flow_recorder.db_url. Same code serves
      PostgreSQL or SQLite (only the URL changes). A central Postgres lets MANY instances share one
      flow database. (PostgreSQL needs a driver, e.g. pip install psycopg[binary].)
    - Provenance columns (instance, host, port) added to flow_events (model + raw sqlite schema, with
      in-place migration of existing flow DBs). Every event is stamped with its origin so multiple
      instances writing to one shared DB stay disambiguable; query()/state_at() gained an optional
      instance filter and distinct_dags() groups by (instance, dag_id).
    - Separate schema files: config/schema/flow_events_postgres.sql and flow_events_sqlite.sql.
    - store=dao now logs a loud startup WARNING (flow events go into the application DB; not
      recommended above low volume) - no longer a silent foot-gun.
    - NEW AerospikeFlowStore stub (core/flow_store_aerospike.py): documented contract mapping (record
      key, secondary-index range query, state_at/distinct strategies, TTL retention); import-guarded,
      raises NotImplementedError until wired to a real cluster.
    - Additive and backward-compatible (new columns nullable; new params optional). 296 -> 302 tests.

Version 5.33.1 highlights (cached DAG inventory):
    - distinct_dags() (the DAG inventory) is now served from a short TTL cache (DEFAULT_DAGS_CACHE_TTL
      = 30s) in both SqliteFlowStore and DaoFlowStore. The underlying query is a full aggregate over
      every event - seconds at tens of millions of rows - and it feeds the inventory, not a hot path,
      so caching bounds how often the scan runs while keeping the list fresh enough. WAL means the
      scan never blocks the writer regardless. Addresses the one query that grew with total volume in
      the 86.4M-row extrapolation. Additive; 295 -> 296 tests.

Version 5.33.0 highlights (batched retention purge + optional flow-store maintenance):
    - Flow retention purge now deletes in bounded batches (5000 rows/txn) instead of one large
      DELETE. At high event rates a sweep could delete ~1.8M rows (e.g. 1000 ev/s over a 30-min
      window) in a single transaction, holding the write lock long enough to starve the recorder's
      drain thread; batching keeps each write lock short (~tens of ms) so the writer interleaves.
      Applies to both SqliteFlowStore and the DAO path (FlowDAO.purge_older_than_ms).
    - NEW optional, config-gated on-disk maintenance: flow_recorder.maintenance =
      none (default) | incremental | full, run once daily at flow_recorder.maintenance_hour. Only the
      dedicated SqliteFlowStore reclaims; the shared-DB 'dao' store is intentionally never VACUUMed.
      Usually unnecessary - deleted pages are reused so the file plateaus - but available for capping
      disk after retention changes. The recorder is decoupled from DAG execution, so even a full
      VACUUM never blocks DAGs (at worst flow-history events are buffered/shed during the window).
    - bench_flow_queries.py gains a --cycles churn/plateau mode (real SqliteFlowStore): fills a 24h
      window then repeatedly inserts a sweep-interval and purges the oldest, reporting file size per
      cycle. Demonstrated (1000 ev/s scaled): row count and file size hold flat across cycles ->
      freed pages reused -> single SQLite file plateaus, no periodic VACUUM needed to bound size.
    - Additive and backward-compatible. 292 -> 295 tests.

Version 5.32.0 highlights (fleet live push (SSE) + switcher health dots):
    - Phase 5 live transport for the fleet: NEW SSE endpoint GET /api/fleet/stream pushes a fresh
      overview immediately and then every ~5s (?once=1 emits a single event for curl/tests). The
      /fleet page now subscribes via EventSource and updates tiles in place, so up/down transitions
      appear WITHOUT a manual refresh; it falls back to periodic polling if EventSource is
      unavailable. Per-plane probes run in a threadpool (loop never blocked) and a failed probe is an
      SSE 'error' event, never a dead stream; the stream stops on client disconnect. This is the
      promised SSE upgrade (decision E kept v1 on polling) - scoped to the fleet live view; the
      gateway-proxied data APIs remain on polling for now.
    - The navbar service-plane switcher now shows a live reachability dot per trusted server (green
      reachable+manageable / amber up-but-not-manageable / red unreachable), probed lazily only when
      the dropdown opens (cached ~15s) so normal page loads cost nothing. Same reachable/manageable
      signal as /fleet.
    - Additive and backward-compatible. 291 -> 292 tests.

Version 5.31.0 highlights (fleet health split + flow query benchmark):
    - Fleet/registry health now distinguishes two signals instead of one: reachable (is the box up?
      via a cheap, unauthenticated GET /health/live - RestServiceClient.live()) vs manageable (can our
      key authenticate and the management contract answer? via info()). registry.health() and the
      /fleet tiles report both, so a server that is up-but-not-manageable (bad/expired key, role, or
      version) is shown distinctly from one that is down, and a down peer never blanks the page. New
      down-server test exercises the unreachable path end to end.
    - NEW perftest/bench_flow_queries.py: a query-side benchmark for Flow Time-Travel history. Builds
      an isolated temp DB with the real flow_events schema + indexes, fills a realistic bounded volume
      (--rate/--hours or --rows), and times the actual FlowDAO paths (5-min/1-hour window query,
      state_at, distinct_dags) plus real DAO.write_batch throughput. Turns "will SQLite scale?" into
      numbers (e.g. 1M rows/24h/20 dags: 5-min window ~3ms, 1-hour ~28ms median). Use it to decide
      SQLite-vs-Paimon on evidence, not intuition.
    - Additive and backward-compatible. 291 tests.

Version 5.30.2 highlights (two-node ports):
    - twonode/ dev setup ports changed to avoid clashes: Node A (UI plane) 8080 -> 18080,
      Node B (service plane) 8090 -> 18090. Updated across generate_configs.py, the generated node
      configs, run scripts, setup.sh, bootstrap.py defaults, README, and the override test. No
      product code change.

Version 5.30.1 highlights (two-node dev setup + DY_CONFIG_FILE):
    - NEW twonode/ kit to run two instances locally and exercise the multi-instance features end to
      end: setup.sh, generate_configs.py (derives per-node configs from canonical config/application.yaml
      so they cannot drift), run-node-a.sh / run-node-b.sh, stop.sh, and bootstrap.py (mints a key on
      node B and registers it as a trusted server on node A). README in twonode/.
    - Config layer now honours the DY_CONFIG_FILE env var: PropertiesConfigurator (a process
      singleton) and run_server.py both load the pointed-at config first, so a per-instance config
      wins regardless of import order. Unset -> unchanged behaviour. This fixes the bug where the
      singleton's first initialiser (canonical config, via an import) shadowed an explicit per-node
      load. Verified live: two instances on 8080/8090 with isolated SQLite DBs; A manages B via the
      switcher, fleet, and gateway proxy; host-scoped admin correctly refused to remote.
    - Additive and backward-compatible. 2 new tests (290 total).

Version 5.30.0 highlights (service-plane Phases 3-5: breadth, fleet, hardening):
    - Phase 3 (breadth): the UI plane now acts as a gateway. A single whitelisted proxy route
      (/api/proxy/{path}, routes/service_proxy_routes.py) forwards broad read/op surfaces - flow
      time-travel, metrics/health, worker *status*, egress *status*, designer components/validate/
      deploy - to the active trusted server with Bearer + X-DY-On-Behalf-Of, while host-scoped admin
      (worker pool start/stop, migrate, JVM/C++/Rust, maintenance) and all auth/user surfaces stay
      strictly local. A tiny fetch shim in base.html re-routes whitelisted calls through the proxy
      only when a remote target is active, so existing pages re-target transparently with no per-page
      edits and local behaviour unchanged.
    - Phase 4 (fleet view): a read-only /fleet single-pane-of-glass (routes/fleet_routes.py,
      /api/fleet/overview) fans out across the local plane and every trusted server, with per-plane
      reachability / version / DAG counts. Per-plane isolation - one unreachable plane never blanks
      the page, no partial-success lies.
    - Phase 5 (hardening, partial): RestServiceClient now reuses a pooled requests.Session per
      trusted server (keep-alive) and a never-raising registry.health() probe backs the fleet tiles.
      Key rotation already shipped in v5.29.0. SSE/websocket transport is intentionally deferred
      (decision E: proxied polling for v1). One open decision remains: whether host-scoped admin ever
      becomes re-targetable (currently no).
    - Additive and backward-compatible. 5 new tests (288 total). Docs refreshed across README,
      ARCHITECTURE, help (multi_instance), and the service-plane design/roadmap. Full history:
      docs/CHANGELOG.md.

Version 5.29.0 highlights (service-plane Phase 2 - manage remote instances):
    - Phase 2 of the UI-plane / service-plane split: one UI can now manage other DishtaYantra
      instances. NEW RestServiceClient (core/service/rest_client.py) mirrors the ServiceClient
      interface over HTTPS + Bearer dyk_ key, with per-call timeouts, typed errors
      (core/service/errors.py) and an X-DY-On-Behalf-Of header so the remote's audit log records
      which UI-plane user acted (advisory only, never authz). NEW trusted-server registry
      (core/service/registry.py + core/db/trusted_dao.py + TrustedServer model + trusted_servers
      table): admins register other instances by URL + an API key minted there; the key is stored
      symmetric-encrypted at rest (core/service/crypto.py, Fernet; secret from the env var named by
      service.secret_env, default DY_SECRET_KEY) and never returned or sent to the browser. Add-time
      probe of the remote /api/service/info validates auth + records its version.
    - Per-session switch: get_service_client(request) now reads the session's selected target, so
      the dashboard list and DAG lifecycle (start/stop/suspend/resume) re-target to the chosen
      server. A navbar switcher + admin page (/admin/trusted-servers) drive it. The browser only
      talks to its own UI plane, which proxies to the selected plane. Empty trusted list (default)
      => local-only, exactly as before; cryptography is an optional dependency needed only when
      trusted servers are used. 10 new tests (283 total). Design: docs/design/service-plane-split.md.

Version 5.28.0 highlights (service-plane seam + JSON management contract):
    - Phase 0/1 of the UI-plane / service-plane split. NEW core/service/ package introduces a
      ServiceClient seam: an interface with a LocalServiceClient (in-process, wraps this instance's
      DAG server - today's behaviour, the contract reference) and a get_service_client(request)
      resolver (always local for now; a later phase returns a remote client for a selected trusted
      server). The DAG-lifecycle HTML handlers (start/stop/suspend/resume) now route the operation
      through the seam; presentation (flash/redirect) stays in the route, so behaviour is unchanged.
    - NEW JSON management API at /api/service/* (info, dags, dag details, status, reload,
      start/stop/suspend/resume) returning structured OpResult-shaped bodies, sharing the same
      LocalServiceClient operation path as the HTML handlers (one source of truth). GET
      /api/service/info advertises name/version/build_date/capabilities/schema_version. The promised
      surface is single-sourced in core/service/contract.py (SERVICE_OPERATIONS) and a drift guard
      (tests/test_service_contract.py) asserts every promised op is in the served OpenAPI schema.
    - Additive and backward-compatible; no behaviour change to existing pages. 8 new tests
      (273 total). Design: docs/design/service-plane-split.md; phases: docs/design/service-plane-roadmap.md.

Version 5.27.0 highlights (API-driven Designer palette):
    - The DAG Designer palette now renders dynamically from
      /api/dag-designer/components instead of a hand-maintained block of HTML. The component
      catalogue moved into core/dag/designer_catalogue.py as a single source of truth shared by
      the API and the palette, so the two can no longer drift. This fixes a latent drift bug: the
      old hardcoded palette had silently fallen seven components behind the backend
      (PublisherSinkNode, NullCalculator, RandomCalculator, AttributeFilterAwayCalculator,
      AttributeNameChangeCalculator, NullDataTransformer, AttributeFilterAwayDataTransformer were
      undraggable in the Designer). A frontend presentation map supplies icon/colour/label per
      type, with a category-default fallback so any newly-added backend component always appears.
    - Additive and backward-compatible: the /api/dag-designer/components response is unchanged;
      drag/drop keys off the same data-type/data-category attributes. Extracting the catalogue also
      brought routes/dagdesigner_routes.py back under the 500-line limit (561 -> 439). New
      regression test (tests/test_designer_catalogue.py, 6 tests) locks the palette to the API and
      guards against future drift. 265 tests total. Full history: docs/CHANGELOG.md.

Version 5.26.0 highlights (flow time-travel):
    - NEW flow time-travel: an opt-in recorder captures the DAG change-log (each node fire's
      inputs -> output, taken off the equality gate so history is naturally compact) into a
      pluggable store (sqlite default; dao/noop; paimon optional). The hot path is a non-blocking
      bounded queue with a background drain: disabled is a true no-op (~96 ns/fire, native
      throughput restored) and overflow is shed as counted drops - it never blocks or raises into
      compute. Browser Time-Travel console at /flow: 24h timeline with firing-density histogram,
      range brush, play/scrub, per-node firing animation on Cytoscape, event inspector, download.
      API under /api/flow (status, enable/disable, dags, query, state-at, streaming export
      jsonl|csv). New CLI tools/dyflow.py downloads windows (streamed to file). Retention via its
      own 24h sweep daemon (flow_recorder.retention_hours), not assumed store auto-expiry.
    - Off by default (flow_recorder.enabled=false); all config additive in properties/yaml; new
      flow_events table (sqlite + postgres). 12 new tests (259 total). Full history: docs/CHANGELOG.md.
"""

VERSION = "5.48.0"
BUILD_DATE = "2026-06-23"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
