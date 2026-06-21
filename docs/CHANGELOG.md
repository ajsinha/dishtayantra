# DishtaYantra Changelog

The authoritative current version is `core/version.py` (`VERSION`). This file
records the per-release highlights that previously lived in the version.py
docstring.


## Version 5.25.1 highlights (fix: trade generator must not choose Kafka partitions):
    - perftest/generate_trades.py passed an explicit partition= computed as crc32(product) % N,
      which crashed ("Unrecognized partition") when the topic had fewer partitions than requested.
      It now sets only the message key and lets Kafka's partitioner route by key hash (same value ->
      same partition, any partition count). Removed partition_for/zlib + per-partition counts;
      --partitions now only provisions the topic's partition count; --dry-run shows key grouping.
      Message Broker Connectors Guide clarified. No server code changed.


## Version 5.25.0 highlights (audit-trail retention):
    - Audit trail bounded by a background sweep: events older than audit.retention_days (default 15)
      are auto-deleted; sweep runs at startup then every audit.retention_sweep_hours (default 6);
      set retention_days <= 0 to keep forever. AuditDAO.purge_older_than + core/audit_retention.py
      daemon wired into the webapp lifecycle. Config keys in application.properties/yaml. Admin and
      Configuration Reference guides updated. 3 new tests (247 total).


## Version 5.24.0 highlights (append-only audit trail):
    - NEW audit trail of security/admin actions, viewed at Admin -> Audit Trail (/admin/audit) with
      actor/action/outcome filters. Auto-recorded for auth login/logout/failed-login, apikey
      create/revoke, and user create/update/delete; each row captures timestamp, actor, action,
      target, detail, source IP, and success. New audit_events table (AuditEvent + AuditDAO),
      core.audit_log.audit() safe helper, routes/audit_routes.py, admin/audit.html, nav entry,
      schema DDL, and Administration guide section. 5 new tests (244 total).


## Version 5.23.0 highlights (admin web UI for API-key management):
    - NEW Admin -> API Keys page (/admin/api-keys): create (with optional expiry), list, and revoke
      API keys from the browser; clear key shown once, never stored retrievably. Keys stay bound to
      a user and inherit its roles. Registry create_api_key gained optional expires_at. New routes
      (routes/apikey_routes.py), template (admin/api_keys.html), nav entry. 4 new tests (239 total).
      API Reference help page and Admin CLI guide updated.


## Version 5.22.0 highlights (API-key authentication for admin endpoints + CLI):
    - AuthGuards now accept a session cookie OR an API key ("Authorization: Bearer <key>" or
      "X-API-Key" header), wiring the existing API-key store into request authentication (backward
      compatible). NEW tools/dyapikey.py mints/lists/revokes keys locally on the server host (clear
      key shown once; optional --expires-days). tools/dyadmin.py gained --api-key / DY_API_KEY
      (skips session login). API Reference help page and Admin CLI (dyadmin) Guide updated. Verified
      end-to-end against a live server.


## Version 5.21.0 highlights (dyadmin admin CLI):
    - NEW tools/dyadmin.py: command-line client for all admin functions of a running server
      (monitor, dag lifecycle, maintenance/drain, runtime logging, logs, workers, native JVM/C++/
      Rust). Session-cookie auth (POST /login), admin role required; creds via flags or
      DY_URL/DY_ADMIN_USER/DY_ADMIN_PASSWORD. --dry-run and --json supported. Verified end-to-end
      against a live server. NEW Admin CLI (dyadmin) Guide; README updated. Client-only.


## Version 5.20.0 highlights (Kafka publisher partition_key):
    - Kafka DataPublisher gained an optional "partition_key" config: name a field whose value
      becomes the Kafka message key, routing all messages that share it to the same partition
      (per-key ordering, co-partition joins, log compaction). Unset/missing field/non-dict ->
      no key -> original spread behavior (backward compatible). Added a key_serializer default to
      the kafka-python producer factory. Documented in the Configuration Reference Guide, the
      Message Broker Connectors Guide (Kafka), and the Kafka help page.


## Version 5.19.1 highlights (trade generator: partition by product type):
    - perftest/generate_trades.py now routes each trade to a Kafka partition by product type
      (default field: symbol) so same-product trades share a partition. Deterministic
      CRC32(product) % N; message key set to the product value. New args --partitions (default 5)
      and --partition-key (default symbol; --partitions 1 = legacy single-partition). Best-effort
      creates the topic with N partitions; --dry-run prints the product->partition map; live run
      prints per-partition counts. Previously trades had no key/partition (round-robin or all on
      partition 0). perftest-only.


## Version 5.19.0 highlights (new JSON-array trade-ETL lane):
    - NEW perftest_trade_etl_array.json + perftest/array_trade_calculators.py: a list-of-dicts
      (JSON array) trade-ETL lane - stock BatchingSubscriptionNode + array calculators (whole
      list per calculate()) + stock FlatteningPublicationNode sinks, no Arrow. Bit-for-bit output
      parity with the row lane; speedup is pure batch amortization (sits between row and Arrow).
      Indicative calc-chain throughput on 50k heterogeneous trades: row ~7.4k, array ~17k (2.3x),
      arrow ~55k (7.4x). Documented in the Benchmarking and Performance Harness Guide.
    - Additive/opt-in; engine untouched.


## Version 5.18.3 highlights (Configuration Reference Guide: heterogeneous Arrow source node):
    - Added "Heterogeneous Arrow source: NormalizingArrowBatchingSubscriptionNode" to the
      Configuration Reference Guide (config keys batch.max_size/extras_key/core_fields + wiring
      snippet + pointer to the Arrow tutorial). Mirrors the 5.18.2 Arrow-guide detail.


## Version 5.18.2 highlights (Arrow guide: heterogeneous-attribute handling in full detail):
    - docs/TUTORIAL_arrow.md "Heterogeneous sources" expanded from overview to detailed
      reference: stable core-schema table (type/default/coercion), worked before->after example
      (nested dicts/lists preserved in extras_json), robustness rules, node config
      (batch.max_size/extras_key/core_fields), DAG wiring snippet, sink round-trip, and the
      compute-on-typed-columns perf rule. All values verified against arrow_trade_nodes.py.


## Version 5.18.1 highlights (fix: DAG state page crashed for Arrow DAGs):
    - /dag/<name>/state rendered each node's raw _input/_output via the template's `| tojson`
      filter. Arrow nodes hold a pyarrow.RecordBatch (not JSON-serializable), so the state page
      failed with "Object of type RecordBatch is not JSON serializable". Added
      _state_value_safe() in routes/dashboard_routes.py: RecordBatch -> {schema, num_rows,
      preview} summary; dicts/lists/scalars/None pass through unchanged (no regression for row
      nodes). Applied to all node input/output before render. build_dag_view verified unaffected.


## Version 5.18.0 highlights (documentation consolidation - markdown is the single portable source):
    - User guides consolidated 40 -> 20 (no content loss): 18 connector guides -> 5 family guides
      (Message Broker, Data Store, In-Process & Local, Network, Advanced Connector Patterns);
      3 calculator guides -> 1; 4 admin/observability guides -> 2 (Administration & Maintenance,
      Observability); the two Arrow tutorials -> docs/TUTORIAL_arrow.md.
    - Principle: documentation lives in (portable) markdown; HTML help pages are thin overviews
      that point to the canonical guide. The 9 connector HTML pages were repointed to their
      family guides; subgraph/autoclone/prometheus/py4j/pybind11/rust gained a canonical-guide
      pointer banner. All help_userguide_view links verified resolvable.
    - Docs-only; no application code changed.


## Version 5.17.3 highlights (Configuration Reference Guide + documentation accuracy sweep):
    - NEW "Configuration Reference Guide" user guide: table-based reference for every config item
      across application.yaml and the DAG JSON schema (subscribers, publishers, node types, node
      config, URI schemes, calculators/transformers, edges, schedule), each with a sample value
      and explanation. Derived from the source of truth (config files + parsing code). Surfaces
      in /help/userguides.
    - Accuracy sweep: version references clean; 167 internal file-path references verified with no
      broken links. Fixed the User Management Architecture guide's PostgreSQL example, which used
      a "postgresql:" config section while the code reads "db.postgres.*" (now "postgres:").
    - Docs-only; no application code changed.


## Version 5.17.2 highlights (documentation completeness + accuracy pass):
    - Accuracy: TUTORIAL_arrow.md and TUTORIAL_arrow.md no longer tell
      users to avoid Arrow for heterogeneous data (contradicted by v5.17.0). Corrected to the
      normalize-to-stable-schema nuance, with a new "Heterogeneous sources" section.
    - Completeness (were CHANGELOG-only): NEW "Admin Maintenance and Drain Mode Guide" and
      "UI Themes Guide" user guides; "Logging and Observability Guide" extended with Runtime
      Logging Control (/admin/logging) and the per-minute ingest histogram. New guides
      auto-surface in /help/userguides.
    - README: Arrow heterogeneous-source note + Admin Maintenance/Drain, Runtime Logging
      Control, and UI themes entries.
    - Docs-only; no application code changed.


## Version 5.17.1 highlights (heterogeneous trade generator for reliable row+Arrow testing):
    - perftest/generate_trades.py reworked to emit heterogeneous trades by default. Core fields
      always present; optional fields vary per record and include nested dicts, lists,
      lists-of-dicts, dict-in-dict, and type-varying keys, across seven archetypes (simple/
      block/multi_leg/algo/cross_ccy/minimal/kitchen_sink). New CLI flags: --hetero-level
      {low,med,high} and --uniform (legacy flat single-shape baseline). The same stream now
      meaningfully tests both the row DAG and the Arrow RecordBatch DAG.
    - Verified: ~57 distinct shapes per 60 records; the row ETL and the Arrow normalize-ingest
      + vectorized chain both process the stream without error; nested/list attributes are
      preserved losslessly in the Arrow extras_json column; row-vs-Arrow parity holds on all
      core numeric/categorical outputs. Additive; no app code changed.


## Version 5.17.0 highlights (high-throughput Arrow RecordBatch trade-ETL for heterogeneous trades):
    New files (all additive; engine untouched):
    - perftest/perftest_trade_etl_arrow.json: columnar/zero-copy RecordBatch version of
      perftest_trade_etl. Kafka -> normalize-ingest -> FX -> notional -> fees -> risk ->
      classify -> anomaly -> summarize -> {file, kafka, kafka-async} sinks. Linear topology
      (RecordBatch fan-in unsupported); original fan-out/fan-in fused into a vectorized chain.
      batch.max_size 5000 + deep queue for ~10k msg/s.
    - perftest/arrow_trade_nodes.py: NormalizingArrowBatchingSubscriptionNode. Drains up to
      max_size heterogeneous trade dicts and builds ONE stable-schema RecordBatch: typed core
      columns (trade_id/seq/symbol/side/quantity/price/currency, with symbol/side/currency
      upper-cased and missing/bad values coerced to defaults, never raising) plus a JSON
      extras_json column that losslessly preserves every non-core attribute. Solves the
      heterogeneous-trade problem: raw pa.Table.from_pylist infers schema from the first row
      only (drops later-row attributes) and crashes on cross-row type conflicts. Uses ev_equals
      gate + by-reference output. Selected by dotted-path node type.
    - perftest/arrow_trade_etl_calculators.py: vectorized ArrowValidateCalculator (_valid),
      ArrowClassifyCalculator (size_bucket/risk_tier/requires_review via nested pc.if_else),
      ArrowAnomalyCalculator (boolean flag columns + is_anomalous), ArrowSummarizeCalculator
      (processed_at/pipeline). FX/notional/fees/risk reuse perftest.arrow_etl_calculators.

    Notes:
    - Verified: stable schema on ragged/type-conflicting input (no drops, no crash); extras
      preserved end-to-end to the sink; bit-for-bit parity with the row ETL on a heterogeneous
      sample; in-memory DAG build + batch round-trip through all three flattening sinks.
    - Columnar anomaly stage emits boolean flag columns rather than the row pipeline's
      list-of-strings anomaly_flags (cheaper and queryable). Backward compatible; additive only.


## Version 5.16.2 highlights (logout returns to the public landing page):
    - routes/auth_routes.py logout(): now redirects to 'index' (the root route '/', which
      renders the public landing page for anonymous visitors) instead of 'login'. Previously
      logging out dropped the user on the login form; now they see the landing page. The
      "Logged out successfully" flash is preserved.


## Version 5.16.1 highlights (contrast fix: dark comparison table under light-family themes):
    - web/templates/comparison.html: the Head-to-Head table rendered with washed-out,
      low-contrast cell text under light-family themes (Light, Blue). Root cause: the table is
      a deliberately dark card, but Bootstrap 5.3 paints each cell with --bs-table-bg, which is
      light when data-bs-theme="light" (both Light and Blue), so the light cell text became
      near-invisible. Fix: pin the Bootstrap table tokens on .cmp-table (--bs-table-bg:
      transparent, --bs-table-color: #eaeef4, plus striped/hover/active variants) so the dark
      card shows through and text stays readable under every theme.
    - Contrast review of other surfaces: About "Competitive Advantage" dark card uses explicit
      #ffffff text (ok); standard app tables use theme-aware dark-on-light text (ok in Blue);
      code blocks use fixed light text on a dark background (ok); hero sections are hardcoded
      dark with white text (ok). No other dark-surface-with-theme-text contrast issues found.


## Version 5.16.0 highlights (drain check accounts for async-egress WAL; new "Blue" theme):
    Drain / WAL:
    - core/egress/wal.py: WalBackend.pending_count() added - records appended but not yet
      committed (drained), computed from offsets (_next - 1 minus committed), 0 == fully
      drained. Deliberately offset-based, NOT byte size: the filelog active segment keeps
      committed bytes on disk until it rolls, so size_bytes() never reaches 0. Verified for
      memory and filelog (pending -> 0 on ack even though filelog size_bytes stays > 0).
    - core/egress/async_publisher.py: AsyncPublisher.async_pending() returns wal.pending_count().
      ComputeGraph.drain_status() already sums publishers' async_pending(), so the maintenance
      "drained" signal now truly accounts for the async-egress WAL, not just the in-process
      publisher queue.

    Theme:
    - NEW "Blue" UI theme inspired by BMO (bmo.com/en-us/main/personal). BMO Blue (#0079c0,
      confirmed from the site's theme-color) on white, a deep-navy (#0b2545) header gradient,
      and the signature BMO red (#e11b22) as the accent; cool-grey surfaces. Implemented as a
      [data-theme="blue"] variable block in web/templates/base.html with a Bootstrap 5.3 light
      token bridge, registered in the theme switcher (THEMES/ICONS/setTheme) and the theme
      menu. Light-family, so it pairs with Bootstrap's light base.

    Notes:
    - Backward compatible: additive only. 235 passed with lmdb; 234 + 1 skipped without.


## Version 5.15.0 highlights (admin drain-mode freeze of subscribers for maintenance):
    - NEW admin Maintenance / Drain page (/admin/maintenance, admin-only): "drain mode" -
      freeze subscribers so they stop pulling NEW messages from brokers while in-flight data
      keeps flowing, so queues and publications drain. Distinct from pause (which holds the
      queues in place). Targets: a single subscriber, a subset, all in a DAG, or everything
      (global). A live drain-status readout plus a JSON status API (/admin/maintenance/status)
      show when queues/WAL have reached zero so it is safe to restart.

    Mechanism (works for ALL brokers):
    - core/pubsub/datasubscriber.py: a generic _frozen flag is checked in the shared
      _subscription_loop before _do_subscribe(), so intake stops for every subscriber type
      uniformly. freeze()/unfreeze()/is_frozen() + _on_freeze()/_on_unfreeze() hooks added;
      details() exposes 'frozen'. Verified end-to-end on the in-memory broker (frozen ->
      queue stays 0 despite published messages; unfreeze -> drains in).
    - Kafka (v2 broker-aware): core/pubsub/kafka_base.py AbstractKafkaConsumerWrapper gained a
      defensive pause()/resume() (works for kafka-python and confluent-kafka via the
      underlying consumer's assignment()/pause()/resume()); KafkaDataSubscriber overrides the
      hooks to pause the consumer, keeping it in the group (no rebalance) during a long freeze.
      Falls back to the generic flag if the consumer can't be paused. NOTE: the Kafka pause
      path is implemented defensively but was not validated against a live Kafka cluster in
      this environment - the generic freeze is the guarantee for all brokers.

    DAG / view / workers:
    - ComputeGraph.freeze_subscribers()/unfreeze_subscribers()/drain_status() added.
    - build_dag_view surfaces a per-subscriber 'frozen' flag and a dag_stats['drain'] block
      (frozen list, subscriber/publisher queue totals, WAL pending, drained bool), so it works
      identically for worker-hosted DAGs via the snapshot.
    - Worker pool: new ControlMessageType.FREEZE_SUBSCRIBERS / UNFREEZE_SUBSCRIBERS, a
      ControlMessage.data payload {subscribers: [...]|None}, worker handlers
      (_freeze_subscribers/_unfreeze_subscribers), and worker_pool.freeze_subscribers()/
      unfreeze_subscribers() that route to the worker hosting the DAG. Unknown control types
      were already ignored, so older workers are unaffected.

    UI / routes:
    - routes/maintenance_routes.py (MaintenanceRoutes), template admin/maintenance.html with a
      3s-poll live drain status, global + per-DAG + per-subscriber freeze/unfreeze, and a
      "safe to restart" banner. Nav link added under the admin menu.

    Notes:
    - Backward compatible: additive and admin-gated. Freeze state is ephemeral by design (it
      clears on restart, which is the expected next step of a maintenance window). 235 passed
      with lmdb; 234 + 1 skipped without.


## Version 5.14.0 highlights (Tier-1 performance pass + runtime logging control UI):
    Performance:
    - Hot-path logging gated. DataSubscriber._log_message_received and
      DataPublisher._log_publish_attempt each built a json.dumps preview and emitted 5
      INFO lines PER MESSAGE. Both now return immediately unless the logger is at DEBUG
      (early isEnabledFor guard) and the lines are DEBUG, so INFO (production default) pays
      nothing. The per-propagation "setting child dirty" INFO in node_implementations.py is
      DEBUG-gated too.
    - topological_sort() memoized on the graph (compute_graph_support.py); the order is
      static after build_dag(), which now sets self._topo_cache = None to invalidate.
    - Dashboard Details view-model served from a ~1s per-DAG TTL cache (dashboard_routes.py),
      absorbing refresh/viewer bursts and, for worker DAGs, repeated get_dag_state round-trips.
    - Optional GC tuning at startup (web/dishtayantra_webapp._apply_gc_tuning): gc.freeze()
      after load plus an optional gen-0 threshold, config-gated via performance.gc.* (freeze
      defaults on; both safe).

    Runtime logging control (UI):
    - NEW admin page /admin/logging (admin-only): change the root log level and per-logger
      overrides at runtime with no restart. Hot-path loggers are listed first so they can be
      dropped to WARNING to cut logging overhead live - the runtime equivalent of the gating
      above. Effective vs explicit (override) levels are shown; INHERIT clears an override.
    - core/log_config.py gained set_root_level / set_logger_level / get_logging_state plus a
      MANAGED_LOGGERS list and LOG_LEVEL_NAMES.
    - Worker processes each own their logging state, so changes are broadcast: new
      ControlMessageType.SET_LOG_LEVEL + an optional ControlMessage.data field;
      worker_pool.broadcast_log_level() fans it out; the worker control loop applies it
      (_apply_log_level). Unknown control types were already ignored, so older workers are
      unaffected (BC).
    - Changes are ephemeral by design: they revert to logging.level (application.properties)
      on restart.

    Notes:
    - Backward compatible: every change is additive and admin-gated; defaults reproduce
      prior behavior except the harmless gc.freeze. 235 passed with lmdb; 234 + 1 skipped
      without.
    - The previously-suggested "WAL double-pickle fix" was dropped: on inspection the default
      FileLogWal pickles exactly once; the flagged line belonged to a different (non-default)
      MemoryWal backend, so there was nothing to fix without moving cost to read time.


## Version 5.13.1 highlights (Message Throughput histogram widened to 30 minutes):
    - Per request, the Details page per-minute ingest histogram now shows 30 one-minute
      buckets (last 30 minutes) plus the in-progress partial minute, instead of 10.
    - core/metrics/rate_meter.py: RateMeter._minute_window raised 10 -> 30. Still O(1)
      memory (a bounded ring of 31 small ints).
    - core/dag/dag_view.py: build_dag_view no longer hardcodes the window - it derives the
      length from each meter's minute_buckets() result (window_minutes / len), so the meter
      and the aggregator can never drift out of sync.
    - web/templates/dag/details.html: chart title, x-axis and the dev comment updated to
      "last 30 minutes" / "30 min ago".
    - Verified end-to-end (TestClient): 30 completed buckets, the oldest bar maps to 30 min
      ago, 31 <rect> bars render, JSON round-trip intact, and it works in both single-process
      and worker-snapshot modes. 235 passed with lmdb; 234 + 1 skipped without.


## Version 5.13.0 highlights (DAG Details: true per-minute throughput histogram):
    - The Details page "Message Throughput" panel now shows the ACTUAL number of messages
      received in each 1-minute tumbling window for the last 10 minutes (plus the current,
      in-progress partial minute), drawn as a dependency-free inline SVG bar chart. This
      replaces the EWMA "msg/min" headline and the old sparkline, which were misleading:
      the EWMA is a smoothed estimate (not a count), and the sparkline's x-axis was "the
      last 60 times someone opened this page," not real time.
    - Plain-language captions (small font) explain it for non-experts: each blue bar is the
      exact message count for that clock minute; the lighter last bar is the current minute
      still filling up (so it usually looks short); the "smoothed" figure is a fast-reacting
      estimate, not an exact tally. Hovering a bar shows its count and how many minutes ago.
    - core/metrics/rate_meter.py: RateMeter gained minute_buckets() - a bounded ring of
      exact per-minute counts keyed by a monotonic-minute index (immune to wall-clock/NTP
      jumps). O(1) memory (window+1 small ints), preserving the meter's no-growing-buffer
      design. mark() increments the current minute; reads roll forward so idle minutes are a
      true 0 and a long-idle meter reports all zeros.
    - core/dag/dag_view.py: build_dag_view sums the per-minute buckets across all subscribers
      into dag_stats['ingest_buckets'] = {completed:[10], current_partial, window_minutes}
      and dag_stats['last_full_minute'] (the new headline number). Because the worker builds
      the view for worker-run DAGs (v5.12.0), the histogram is identical in worker-pool mode
      with no extra IPC. The EWMA rate_per_minute is retained but relabeled as a secondary
      "smoothed ~N/min" estimate.
    - routes/dashboard_routes.py: removed the per-DAG _rate_history deque and its page-load
      append - it only captured page-open moments and never worked for worker-run DAGs. The
      template's sparkline (dag_stats.rate_history) was replaced by the bar chart.
    - Verified end-to-end (TestClient): cross-subscriber bucket aggregation is correct
      (e.g. two subscribers' 1-minute-ago counts 7 and 3 sum to 10), the view round-trips
      through JSON, and the chart + captions + hover counts render in both single-process and
      worker-snapshot modes; the headline reads the last completed minute's real count.
      235 passed with lmdb; 234 passed + 1 skipped in the shipped repo.



    - The DAG "Details" page now shows the same live data whether a DAG runs in the main
      process or in a worker subprocess. Previously, with the worker pool enabled the main
      process held only a lazy (unbuilt) copy of a worker-run DAG, so its details() returned
      config-derived placeholders (queue_depth 0, no current_depth/max_depth) and the page
      showed 0 / infinity for queue depth (and, before 5.11.9, crashed).
    - NEW core/dag/dag_view.py - build_dag_view(dag): the single source of truth for the
      Details view-model. From a live (built) ComputeGraph it produces
      {details, node_details, graph_data, dag_stats} exactly as the dashboard route used to
      assemble inline. Everything returned is JSON/pickle-safe so it can cross the worker
      IPC boundary. The rolling rate-history sparkline is intentionally excluded (owned by
      the main process and appended by the route).
    - Worker side (core/workers/worker_process_runtime.py _send_dag_state): in addition to
      the existing State-page node_states/subscriber_states, it now builds
      data['view'] = build_dag_view(dag) on the worker's REAL running DAG. So the snapshot
      carries live queue depth/max, message counts and rates. Defensive: a view-build
      failure does not break the State response.
    - Main side (routes/dashboard_routes.py dag_details): refactored to build the view from
      a single place. When the DAG is assigned to a worker it fetches the live snapshot via
      worker_pool.get_dag_state() and renders from snapshot['view']; otherwise it builds the
      view locally with build_dag_view(dag). Either branch yields an identical structure, so
      the template renders identically. If the worker doesn't answer, it falls back to the
      local view rather than failing.
    - Verified end-to-end (TestClient): with the worker snapshot supplying distinct values
      (max_depth 9,999, current_depth 1,234, received 777,777) the page renders those live
      values and does NOT fall back to the local DAG's figures; single-process rendering is
      unchanged. The view round-trips through JSON intact (proving IPC-shippability). Tests:
      235 passed with lmdb installed; 234 passed + 1 skipped in the shipped repo.



    - FIX (DAG Details page, worker pool enabled): rendering crashed with "unsupported
      format string passed to Undefined.__format__". The queue_bar macro in
      web/templates/dag/details.html guarded its depth/capacity args with `is not none`,
      but for a DAG executing in a worker subprocess the live queue figures
      (sub.current_depth / sub.max_depth / pub.queue_depth / pub.max_queue_depth) aren't
      in the main-process view and arrive as Jinja Undefined - which is NOT `none`, so it
      passed the guard and reached "{:,}".format(Undefined). The macro now guards with
      `(x is defined and x is not none)`; Undefined/None both render as 0. The State page
      was unaffected because it doesn't use queue_bar.
    - FIX (DAG clone with LMDB subscriber): "'LMDBDataSubscriber' object has no attribute
      'is_composite'". LMDBDataSubscriber / LMDBDataPublisher in
      core/pubsub/lmdbpubsub_endpoints.py are standalone classes that never implemented
      the pub/sub interface method the builder's composite-adjustment pass calls on every
      subscriber (compute_graph_builders.py: `if sub_object.is_composite()`). Added
      is_composite() -> False to both. Clone rebuilds the DAG via that path, hence the
      crash on clone.
    - FIX (example DAGs that validated but couldn't build): worker_affinity_example,
      worker_affinity_example_dag and cross_worker_consumer_dag declared calculators with
      the legacy `type: "PythonCalculator"` + module/class form pointing at non-existent
      classes (calculators.pricing.PriceCalculator, example_calculators.*). The builder
      ignores module/class, so these never resolved. Repointed to the built-in
      PassthruCalculator (names preserved so node references still resolve); they now build
      and clone. A _calculator_note documents how to swap in a real dotted-path calculator.
    - perftest/perftest_trade_etl.json: added a second Kafka result publisher
      `output_kafka_async` (destination kafka://topic/perftest_trades_enriched_async,
      `async_egress: true`) feeding a new `kafka_async_sink` PublisherSinkNode off
      summarize_node, beside the existing inline `output_kafka`. With egress.async.enabled
      set, output_kafka_async is wrapped by the WAL-backed AsyncPublisher while output_kafka
      stays inline - a sync-vs-async egress benchmark in one DAG. Now 12 nodes / 13 edges.
    - DOCS: refreshed the embedded examples in web/templates/help/py4j_integration.html,
      pybind11_integration.html and rust_integration.html to the current schema
      (subscribers/publishers/calculators sections; bridge calculators as
      type cpp/java/rust + *_class/*_module config; SubscriptionNode/CalculationNode/
      PublisherSinkNode; from_node/to_node edges) so they match the migrated example files.
      The full-DAG examples now mirror cpp_math_pipeline_dag.json / rust_math_pipeline_dag.json.
    - Tests: 235 passed with lmdb installed (the previously-skipped lmdb test now runs and
      exercises the new is_composite path); 234 passed + 1 skipped in the shipped repo
      (no lmdb dependency bundled).



    - ComputeGraph builder (core/dag/compute_graph_builders.py): the calculators-building
      loop now routes bridge calculators through CalculatorFactory. A calculator whose
      `type` is "cpp"/"java"/"rust" (case-insensitive), or whose `config` carries a
      cpp_class / java_class / rust_class key, is created via
      CalculatorFactory.create(name, {..config, "calculator": type}), which loads the
      pybind11 (C++), Py4J (Java), or PyO3 (Rust) backend. The schema's `type` is mapped
      onto the factory's `calculator` key (setdefault, so an explicit one is preserved).
      Built-in and dotted-path calculators are unchanged - this is purely additive.
    - Migrated the 7 legacy bridge example DAGs (cpp_math_pipeline_dag, cpp_trade_pricing_dag,
      rust_math_pipeline_dag, rust_timeseries_analysis_dag, java_math_hybrid_dag,
      java_risk_calculation_dag, java_trade_pricing_dag) from the obsolete inline
      source/calculator/destination + from/to schema to the current
      subscribers/publishers/calculators sections, SubscriptionNode/CalculationNode/
      PublisherSinkNode nodes, and from_node/to_node edges. Bridge calculators are now
      declared in the calculators section using the factory config (cpp_class+cpp_module,
      java_class+gateway_config, rust_class+rust_module). The two Python preprocessing
      steps in the java DAGs (formerly example classes) are now built-in PassthruCalculator.
      The inline `condition` on the java risk-alerts sink (never part of the node schema)
      was dropped.
    - Result: all 7 validate clean through /api/dag-designer/validate and build straight
      to the factory, failing only on the absent native backend (an environment dependency,
      not a schema defect). config/example/dags is now 31/31 deployable & clean (was 24/31).
      Original filenames were preserved so py4j_integration / pybind11_integration /
      rust_integration help-page links remain valid. NOTE: those three help pages still
      embed old-schema JSON examples that no longer match the migrated files - a docs
      refresh is outstanding. 234 passed, 1 skipped.



    - DAG Designer importer (importDagJson) now accepts legacy edge keys. Edges are read
      as from_node ?? from ?? source and to_node ?? to ?? target, so a DAG whose edges
      still use the old from/to keys loads with its endpoints intact and re-exports as
      from_node/to_node. Previously such a DAG round-tripped to edges with from_node:null
      and deploy failed with "Edge references non-existent source node: None". The
      cross_worker_consumer/producer and worker_affinity examples now deploy clean.
    - DAG Designer validator (_validate_config and _detect_cycles) hardened against
      malformed / legacy-schema DAGs so it returns actionable errors instead of crashing:
        * Nodes or components missing the required 'name' (legacy id-based java DAGs use
          'id') are reported as "A <kind> entry is missing the required 'name' field"
          rather than raising KeyError('name') - which had reached the user as the opaque
          "Save failed: 'name'".
        * Reference fields holding a non-string (legacy inline "calculator": {"type":
          "cpp", ...} dicts) are reported as "non-string <field> reference" instead of
          raising "unhashable type: 'dict'" from a `dict not in set` test.
        * Non-dict node entries and non-list reference fields are skipped safely.
      Net effect: the 7 legacy cpp/rust/java bridge DAGs now fail to deploy with clear,
      specific messages enumerating exactly what is wrong, instead of cryptic crashes;
      no functional DAG behaviour changed. 234 passed.


## Version 5.11.6 highlights (implicit passthru/null transformers + hard-fail on dangling refs + example audit):
    - Implicit built-in transformers: 'passthru' (PassthruDataTransformer) and 'null'
      (NullDataTransformer) now resolve by name even when a DAG does not define them in
      its "transformers" section. Defined once in compute_graph_builders.
      IMPLICIT_TRANSFORMERS and seeded into self.transformers after the explicit
      transformers are built, so an explicit transformer of the same name overrides the
      implicit one. The DAG Designer validator unions these names in too, so a bare
      passthru/null reference no longer reports as "non-existent".
    - ComputeGraph hard-fails on dangling node references. Previously a node that
      referenced an undefined subscriber/publisher silently got no wiring, and an
      undefined calculator/input_transformer/output_transformer was skipped (5.11.5
      downgraded this to a warning). All five are now hard errors at build:
      "DAG '<name>': node '<node>' references undefined <kind> '<ref>'". This matches the
      DAG Designer validator, which already errored, so the two paths can no longer
      disagree, and aligns with the project's no-silent-skip principle.
    - Full audit of every DAG in config/example/dags + config/dags (33 total), with
      passthru/null treated as implicit:
        * 8 build clean (coordination_consumer/producer, cross_worker_producer_dag,
          the duration/perpetual/autoclone-default samples, sample_dag).
        * 14 are schema-clean with NO dangling references but cannot fully build in a
          bare sandbox because they need external infrastructure - Kafka brokers,
          the 'lmdb' module, a configured storage.provider, or (worker_affinity) hit a
          pre-existing LMDBDataSubscriber.is_composite connector bug. These are env /
          connector issues, not reference problems.
        * 7 legacy bridge DAGs (cpp_math_pipeline, cpp_trade_pricing, rust_math_pipeline,
          rust_timeseries_analysis, java_math_hybrid, java_risk_calculation,
          java_trade_pricing) use an obsolete schema (SourceNode/CalculatorNode/SinkNode
          or lowercase source/calculator/sink, inline source/calculator/destination
          blocks, from/to edge keys, id-based java nodes) and depend on pybind11 / PyO3 /
          py4j bridges. They were NOT rewritten - the bridge calculator type strings
          can't be determined from the current factory to do it verifiably, and 4 of the
          7 are referenced in docs so blind removal would break links. With this release
          they fail loudly on load (correct) rather than misbehaving silently;
          recommend a focused rewrite-or-remove pass.
    - Data fixes applied: cross_worker_consumer_dag, cross_worker_producer_dag,
      worker_affinity_example, worker_affinity_example_dag had legacy from/to edge keys
      rewritten to from_node/to_node (the engine reads only from_node/to_node);
      cross_worker_producer_dag's calculator 'data_transformer' was repointed from the
      fictional type "PythonCalculator" (module example_calculators.transformer) to the
      real builtin PassthruCalculator. 234 passed.


## Version 5.11.5 highlights (example_trade_ingest passthru fix + dangling-ref warnings):
    - Fixed DAG Designer deploy failure on example_trade_ingest.json: "Node
      'kafka_trade_discriminator_publisher' references non-existent input transformer:
      passthru". The node listed input_transformers: ["passthru"] but the DAG defined no
      transformers section, so the reference was dangling. Added the section
      ({"name":"passthru","type":"PassthruDataTransformer"}) - the DAG now validates and
      deploys clean (validator returns valid:true). Note 'passthru' is not a builtin TYPE
      name; the resolvable class is PassthruDataTransformer. Only this one shipped DAG had
      a dangling transformer reference (scanned all of config/example/dags + config/dags).
    - Root-caused an asymmetry: the DAG Designer validator correctly errors on undefined
      transformer references, but ComputeGraph.build silently skipped undefined
      calculator / input_transformer / output_transformer references (if name in
      self.<...>), which is why the bad reference built in the engine yet failed in the
      designer. ComputeGraph now logs a warning for each skipped reference instead of
      swallowing it. (Behaviour still lenient - it skips, not raises - but no longer
      silent.) 234 passed.


## Version 5.11.4 highlights (worker-pool main-loop fix + lmdb default off):
    - Fixed the worker process "spinning forever" behaviour: _main_loop was a 1ms
      busy-poll (get_nowait + sleep(0.001)) that woke ~1000x/sec doing nothing - idle
      workers measured ~4-6% CPU each. It now blocks on control_queue.get(timeout=
      status_interval), so an idle worker uses ~0% CPU (verified) while still honouring
      control messages immediately and emitting heartbeats every status_interval.
      Shutdown remains prompt (SHUTDOWN message unblocks the get; shutdown_event is
      re-checked each iteration).
    - Removed the dead _run_dag_cycles(): ComputeGraph exposes start/stop/do_compute but
      no run_cycle/execute, so its dispatch never fired - yet it looped over every DAG
      each 1ms and fabricated per-DAG stats (nodes_executed, avg_cycle_time_ms). DAGs
      actually run via their own do_compute thread (started by dag.start() in _load_dag);
      real per-DAG state for the UI comes from the GET_DAG_STATE path. total_cycles now
      truthfully counts main-loop iterations.
    - Defaulted use_lmdb_for_cross_worker to false in config/worker_config.json, the
      example config, and WorkerPoolManager's built-in defaults, with an explanatory
      note. Rationale: the worker control plane (load/unload/status/heartbeat) always
      uses multiprocessing queues and is independent of this flag; the per-worker LMDB
      env it gated is vestigial (nothing reads it). Cross-worker DATA flows through the
      transport a channel names - external brokers (kafka/redis/rabbitmq) and lmdb://
      endpoints are cross-process regardless; only mem:// channels become worker-local.


## Version 5.11.3 highlights (architecture page contrast sweep + JSON logging default):
    - Architecture help page: completed the dark-theme contrast pass beyond the System
      Overview tiles. All remaining bg-light surfaces (info boxes, stat boxes, the
      bottom card - ~16) converted to Bootstrap 5.3 theme-aware bg-body-tertiary; the
      SinkNode trash icon (text-dark on a dark card) -> text-secondary; a bg-light/
      text-dark badge -> text-bg-secondary; and the two hardcoded light gradients in the
      Light Up/Light Down (subgraph) section -> translucent success/danger tints. The
      bg-warning text-dark badges/headers were left as-is (black-on-yellow reads in every
      theme). Also fixed the matching light gradient in dag/publish_message.html's
      .active state (now a color-mix tint over var(--bg-card)).
    - JSON logging is now the DEFAULT: logging.format=json in both application.properties
      and application.yaml (kept in sync). Emits one compact JSON object per line via the
      existing JsonFormatter; revert with logging.format=text. No call sites changed.


## Version 5.11.2 highlights (architecture-page card contrast + 5.11.1 hotfix):
    - Architecture help page System Overview tiles: three tiles plus High Availability
      used Bootstrap .bg-light, so in the dark/green/ubuntu themes they kept a light
      surface with low-contrast text. Converted all six tiles to the translucent tinted
      pattern (bg-<color> bg-opacity-10 + matching border) already used by the Worker
      Pool and LMDB tiles - readable AND visually distinct from the card body in every
      theme. (Only architecture.html used this bare-bg-light tile pattern.)
    - Hotfix for 5.11.1: a regex edit had left core/version.py with an unterminated
      module docstring that swallowed VERSION and broke startup; restored. 234 passed.


## Version 5.11.1 highlights (merge root example/ into examples/):
    - Merged the root example/ package into examples/ (no filename collisions) and
      removed example/. example/ was a Python package using relative imports internally,
      so the rename is safe; updated the four external dotted/path references
      (config/example/dags/example_trade_ingest.json -> examples.tradeprocessor...,
      the two stale java_* dags' example.calculators prefix, and the subgraph_demo.py
      path in help/subgraph.html + the subgraph userguide), plus the moved lmdb README.
      Verified examples.tradeprocessor.dag.trade_ingest imports and example_trade_ingest
      resolves its node (Kafka env-fail only). 234 passed.


## Version 5.11.0 highlights (trade-stream example set + Ubuntu theme):
    - Example use-case set (perftest/), all driven by the canonical trade stream:
      added perftest_wasm.json + run_wasm_example.py (notional = price*quantity computed
      inside a sandboxed wasmtime module; verified end-to-end 100/100) and
      perftest_eod_batch.json (standalone batched EOD enrichment; verified 3000 trades,
      results identical to the sequential path). Added client_id to generate_trades so
      the SAME stream drives client-centric (EOD) pipelines. perftest/README.md now
      carries a use-case catalog (row ETL / Arrow ETL / mixed / auto-batch / Arrow
      transport / EOD / WASM). Arrow + mixed + autobatch examples already existed.
    - New "Ubuntu" theme (4th theme): aubergine + Ubuntu-orange palette, switcher entry,
      maps to data-bs-theme=dark. All text/background pairs verified >= WCAG AA by
      computed contrast (primary 16.8:1, muted 8.4:1, links 8.0:1 on aubergine). Green's
      ~20 dark-family override selectors twinned for Ubuntu in base.html + help pages.
      DAG Designer: dedicated aubergine canvas/panel tokens (contrast-verified); the
      Cytoscape node/edge pills already use concrete canvas-independent colors so they
      read on aubergine as on the other canvases. (234 passed; pages render 200.)


## Version 5.10.1 highlights (example DAG reconcile + compliance audit):
    - Consolidated example DAGs: merged config/dags/examples/* into config/example/dags/
      (additive - no filename collisions, nothing overwritten) and removed
      config/dags/examples/. Updated the stale path references (QUICKSTART, DAG Folders
      guide, dag_server_loader docstring).
    - Compliance audit: built all 31 example DAGs against the current engine. 8 build
      clean; the rest fail only for ENVIRONMENT reasons (no Kafka broker / py4j / lmdb /
      storage.provider) and are schema-compliant. Genuinely non-compliant (stale
      schema) found and the safe ones fixed: cross_worker_*/worker_affinity_* used
      lowercase node types (subscriber/calculator/publisher) -> corrected to the current
      class names (SubscriptionNode/CalculationNode/PublicationNode); verified against
      the live node registry.
    - KNOWN STALE (not auto-fixed): the cpp_*/rust_*/java_* examples use an older
      inline-source / id-based schema the current loader doesn't support and target
      optional bridges (pybind11/PyO3/py4j) that can't be build-verified here. Flagged
      for rewrite to the current subscribers/calculators/nodes schema. (234 passed.)


## Version 5.10.0 highlights (read-only egress monitoring UI):
    - New /egress dashboard page + /egress/stats JSON endpoint (routes/egress_routes.py,
      web/templates/egress.html), wired into the nav. Surfaces what core.egress already
      tracks: live worker-thread count and per-destination written/retries/connected/
      committed-offset/WAL-bytes/high-water/last-error, auto-refreshing every 5s.
    - Strictly READ-ONLY (no pause/flush/reset) by design. Honest scope note in the UI:
      shows the egress pool in THIS process; in multiprocess worker mode each worker
      runs its own pool not visible here. Uses standard Bootstrap card/table components
      so it inherits every theme. Login required; 3 route tests added (234 passed).


## Version 5.9.8 highlights (high-value comments, pass 4 - remaining modules):
    - Connectors finished: activemq (event-driven STOMP failure detection via listener
      callbacks vs poll-based detection), tibcoems (ResilientSession session/durable-
      subscription restoration), websphere (ResilientQueue handle re-open model).
    - Engine: compute_graph.py do_compute (the event-driven sweep loop) was already at
      standard; filled the accessor gaps. dag_server.start already documents the
      worker-dispatch-vs-main-process dual path.
    - Extension contracts: DataCalculator.calculate and DataTransformer.transform - the
      most-implemented methods in the system - now carry real contracts (treat input as
      read-only, be deterministic, the equality gate compares outputs by value).
      DataCalculatorLike protocol documented.
    - Workers: worker_process run/_main_loop/_process_control_messages (per-process
      lifecycle, control-plane draining) and dag_affinity.assign_dag (the whole-DAG ->
      one-worker invariant + resolution order).
    - Auth/permission: the security-critical core (dao.verify_password constant-time
      check, user_registry authenticate/has_role) was found already well-documented;
      thin route handlers deliberately left bare per the standard (no padding).
    - Behaviour unchanged throughout (231 passed). High-value commenting pass complete.


## Version 5.9.7 highlights (high-value comments, pass 3: node_impl done + rabbitmq/redis):
    - node_implementations.py fully complete (Sink + both Flattening computes documented).
    - resilient_rabbitmq.py: documented the topology-replay model (ResilientChannel
      records QoS/exchanges/queues/bindings/consumers and replays them after a drop
      because RabbitMQ channel state is connection-scoped), the single-flight reconnect
      orchestration (reconnect -> restore channels -> flush buffer), _execute_with_retry
      as the uniform choke point, and the buffer processor's re-queue caveat.
    - resilient_redis.py: documented execute_command as the resilience choke point with
      an honest at-least-once / double-apply caveat (buffered command is also retried
      inline), drop-oldest buffering (load-shedding), connection-scoped pub/sub
      subscription restore, and pipeline snapshot-and-retry.
    - Behaviour unchanged (231 passed). Remaining connectors: activemq, tibcoems,
      websphere; then compute_graph/dag_server, routes/, calculators/transformers/workers.


## Version 5.9.6 highlights (high-value comments, pass 2: node_implementations + kafka):
    - core/dag/node_implementations.py fully documented to the standard: every node
      type (Sink, PublisherSink, Subscription, Publication, Metronome, Batching,
      ArrowBatching/Flattening) now states its contract and the *why* - the equality
      gate on both producer and sink sides, the metronome's deliberate equality-gate
      bypass and its ticker-thread concurrency model, and the Arrow nodes' ev_equals
      + zero-copy-by-reference semantics.
    - core/pubsub/resilient_kafka.py: AbstractResilientConsumer/Producer now carry full
      method contracts; the canonical kafka-python consumer/producer document the
      FIXED-interval (non-exponential) blocking reconnect schedule, the no-loss
      buffer-on-failure model and its background flush thread, and the honest
      cross-outage ORDERING caveat. Confluent variants point to those contracts.
    - Behaviour unchanged (231 passed). Remaining for later passes: other resilient_*
      connectors, compute_graph/dag_server, routes/, calculators/transformers/workers.


## Version 5.9.5 highlights (commenting standard + high-value comments, pass 1):
    - Added docs/CODE_COMMENTING_STANDARD.md: comment the *why* not the *what*;
      document contracts/invariants/concurrency/units/failure-modes; avoid boilerplate.
    - Applied it as pass 1 (NOT the whole codebase yet): core/egress/wal.py now carries
      full per-method contract docstrings on the WalBackend ABC (concurrency model,
      offset semantics, when ack is durable, what reclaim drops, visibility policy);
      core/dag/node_implementations.py compute path documents the equality gate
      (subscription + publication sides) which is the core correctness invariant.
    - This is an incremental effort: remaining high-value targets are the resilient_*
      connectors, the rest of node_implementations, dag_server/compute_graph, and
      routes/. Tracked, to be done module-by-module against the standard.


## Version 5.9.4 highlights (remove star imports from compute_graph_builders):
    - core/dag/compute_graph_builders.py no longer uses `from ... import *`. Built-in
      calculator/transformer types are resolved via _resolve_builtin_type() looking in
      the core_calculator / core_transformer module namespaces, replacing the legacy
      globals() lookup that the star imports fed. Also dropped unused imports
      (traceback, SubgraphConfigError, load_subgraph_from_config) and imported
      DataCalculatorLike explicitly. The module is now pyflakes-clean and fully
      statically analyzable; behaviour preserved (231 passed).


## Version 5.9.3 highlights (codebase audit: shrink version.py, CHANGELOG extracted):
    - Audit pass on file sizes, syntax, and static analysis. version.py reduced from
      592 to ~45 lines by moving the accumulated release-highlights changelog out of
      its module docstring into this docs/CHANGELOG.md; future highlights go here.
    - Confirmed: all Python compiles; the egress subsystem (core/egress/) is clean
      and under the 500-line convention (wal 431, drainer 235, async_publisher 176).
    - Reported (not changed) pre-existing items: 14 files predating this work exceed
      500 lines (resilient_* connectors, node_implementations, run_server, ...), and
      the wider core/ carries pre-existing lint debt (unused imports, star imports).
      These are flagged for a future cleanup, not silently "fixed".


## Version 5.9.2 highlights (codify WAL flush-on-every-write policy):
    - Documented the WAL invariant explicitly in core/egress/wal.py and the guide:
      every append is immediately flushed to the OS (visible to the drainer at once;
      survives a process crash via the page cache) - cheap, always on. fsync to
      physical disk (power-loss durability) stays the separate tunable policy.
      Scope clarification: the earlier visibility bug was the filelog backend only
      (memory = shared list, sqlite = commit-per-append are intrinsically visible);
      it was the WAL backend, not the destination publisher type.


## Version 5.9.1 highlights (egress doc audit, new multiprocess tutorial, WAL fix):
    - BUGFIX: FileLogWal now flushes to the OS on every append, so the drainer's
      read handle sees records promptly under fsync=interval/os (previously only
      the first record drained until an fsync fired). Regression test added.
    - New tutorial docs/TUTORIAL_async_egress_multiprocess.md: async publication in
      single-process vs multiprocess worker mode (bounded pool per process, WAL
      namespaced by DAG, parallel egress, resume). Both tutorial simulations verified.
    - Doc audit: fixed the basic async tutorial (stale DestinationDrainer ->
      DestinationChannel.pump; per-publisher framing); README + QUICKSTART now
      mention async egress; design doc notes the shipped first-cut pool model and a
      duplicate heading was removed. Guide already covers per-publisher + worker pool.


## Version 5.9.0 highlights (egress: per-publisher opt-in + bounded worker pool):
    - Per-publisher control: a publisher's config may set async_egress true/false to
      use the WAL or publish inline, so a DAG can MIX WAL-backed and direct
      publishers. egress.async.default sets the behaviour for publishers that don't
      specify (opt-out by default; set false for opt-in mode).
    - In-memory destinations (mem://, inmemory://, memory://) ALWAYS publish inline
      and never use the WAL (a durable buffer in front of an in-process queue adds
      latency for no benefit).
    - Bounded egress worker pool: egress.worker.count (default 4) caps egress worker
      threads per process; destinations multiplex onto the pool, each assigned to one
      worker by a stable hash of its WAL key (preserving per-destination FIFO). A
      stalled destination backs off without starving siblings on the same worker
      (drain is non-blocking across channels). Replaces one-thread-per-destination.
    - Config keys added to both files (parity preserved): egress.async.default,
      egress.worker.count. Guide updated. Tests: tests/test_egress.py now covers the
      pool cap, sibling non-starvation, per-publisher opt-in/out, and mem skip.


## Version 5.8.3 highlights (single-version doc reconciliation):
    - Removed independent doc-edition version labels that masqueraded as the
      product version. QUICKSTART and ARCHITECTURE titles are now version-neutral;
      docs/CONFIG_AND_CLOUD_v2.2.md renamed to docs/CONFIG_AND_CLOUD.md (title +
      all references updated); footers/subtitles/comments carrying "v2.2" cleared.
      There is now ONE product version (core/version.py), mirrored only to the
      README badge/line and config app.version; historical "introduced in vX.Y.Z"
      markers are retained as facts. Policy codified in the version.py checklist.


## Version 5.8.2 highlights (documentation audit + research paper & architecture update):
    - Research paper (docs/research + web/static/research .md/.tex/.pdf, regenerated
      via xelatex, now 29pp) expanded with the recent advances: native Arrow C Data
      Interface calculators, WASM sandboxed calculators, and the WAL-backed async
      egress subsystem (per-destination FIFO, durable resume, portable backends,
      massively parallel egress in worker mode).
    - ARCHITECTURE.md (edition 2.3): added sections for RecordBatch edge transport,
      native Arrow C Data Interface, WASM sandboxed calculators, credit-based
      backpressure, and async egress; corrected the stale "next A1 increment" note
      (RecordBatch edges have shipped); updated ToC.
    - architecture.html help page: new "Recent Subsystems" card + quick-nav link.
    - ROADMAP: corrected stale baseline (v3.3.0 -> v5.8.1). Verified guides have no
      stale shipped-feature claims and the egress guide/tutorial are discoverable.


## Version 5.8.1 highlights (egress: namespace WAL by DAG name only):
    - WAL key is now (DAG, publisher), dropping the worker id. DAG names are
      universally unique so the key is globally unique on its own; keying by the
      stable DAG name (not the worker slot) also fixes resume when a DAG is
      reassigned to a different worker after restart - it reopens its own WAL
      instead of orphaning the un-drained tail. Reverted the DY_WORKER_ID env hook.


## Version 5.8.0 highlights (IMPLEMENT async egress - WAL-backed publication, A5):
    - New core/egress/ subsystem (off by default, fully backward compatible):
      publish() can become a non-blocking WAL append drained by a background
      writer, freeing the compute thread. Portable WAL backends with NO native
      dependency - filelog (segmented stdlib append log, CRC32 + torn-tail
      recovery) and sqlite (WAL-mode), plus memory; lmdb is a recognized opt-in
      that errors clearly when absent. Per-destination FIFO via a single ordered
      drainer with stop-the-line, order-preserving retries; auto-reconnect; durable
      acked-offset resume (at-least-once); bounded WAL with periodic reclaim +
      overflow policy (block/drop) so it never fills the host.
    - Transparent integration: publishers are wrapped via maybe_wrap_publisher in
      the DAG builder only when egress.async.enabled=true; default off = identical
      behaviour (verified: full suite green with feature off).
    - Multiprocess/worker mode: because a whole DAG is pinned to one worker
      (dag_affinity), each DAG's publishers+WAL+drainer co-locate in that process;
      workers drain their own WALs in parallel (massively parallel egress). WAL is
      namespaced by (DAG, publisher) - DAG names are universally unique - so
      concurrent processes never collide and a DAG resumes its own log on any worker.
    - Config keys added to both config files (parity preserved). New user guide
      "Async Egress (WAL-Backed Publication) Guide" + TUTORIAL_async_egress.md.
      Tests: tests/test_egress.py (14). Design: docs/design/A5-async-egress-subsystem.md.


## Version 5.7.4 highlights (A5 design: portable WAL, WAL maintenance, auto-recovery):
    - Platform portability: WAL backends no longer require LMDB. Defaults are pure
      Python stdlib and cross-platform - `filelog` (segmented append log) and
      `sqlite` (WAL-mode, transactional); `lmdb` is opt-in/fast where available;
      `memory` for loss-tolerant. So a host without LMDB keeps full durability.
    - WAL maintenance (§6.2): a background janitor owned by the egress subsystem
      reclaims fully-acked segments under a hard size cap, with a disk-free floor and
      the overflow policy as the wall - the WAL self-trims to "un-acked backlog + one
      active segment" and can never fill the host. Added segment/maintenance/disk
      config keys.
    - Connection loss & recovery (§5.3): destination workers auto-reconnect with
      backoff + circuit breaker, stop-the-line while down (no reorder), keep the
      un-acked tail durably in the WAL, and replay from the last acked offset on
      reconnect - no message lost. Confirmed scope: per-destination FIFO ordering.
      Docs only.


## Version 5.7.3 highlights (A5 design: lock in per-destination FIFO ordering):
    - Decision recorded in docs/design/A5-async-egress-subsystem.md: egress
      guarantees per-destination FIFO - messages reach a destination in the exact
      order the DAG produced them (a correctness requirement: never fire a Sell
      before its Buy is committed). New §5.2 spells out enforcement: append order =
      production order, a single ordered writer per destination, parallelism only
      across destinations or via key-partitioning (never round-robin), and
      order-preserving retries that stop-the-line on failure instead of skipping
      ahead. Documented scope: this orders per-destination, not across different
      destinations. Resolved the related open question; updated config notes and the
      risk table. Docs only.


## Version 5.7.2 highlights (refine A5 async-egress design: lifted config, worker cap):
    - Updated docs/design/A5-async-egress-subsystem.md per review: egress workers
      auto-configure broker endpoints by lifting them from the DAG's EXISTING
      publisher definitions (via the connector factory) - no second/duplicate
      egress configuration; the only new config is egress *behaviour*. Added a
      hard cap on egress worker processes (egress.worker.max_total) with automatic
      allocation of DAGs/destinations onto the bounded pool (by_destination default,
      by_dag pin for strict ordering). Spelled out WAL crash/restart durability
      (process-crash vs power-loss, fsync=always|interval|os, LMDB ACID vs mmapfile
      torn-tail detection, durable acked-offset replay). Docs only.


## Version 5.7.1 highlights (design: decoupled async egress subsystem - roadmap A5):
    - Added docs/design/A5-async-egress-subsystem.md: external-broker publication
      decoupled from the single compute thread via a non-blocking append to a
      per-destination WAL, drained by DishtaYantra-spawned egress processes (one
      pool per destination) that batch and write in parallel. The WAL is both the
      durable buffer and the zero-copy inter-process channel; memory stays bounded
      because backpressure is the hard floor (block/spill/drop/dead-letter), so a
      slow broker slows the source instead of OOM-ing. Covers spawn-clean fork
      hygiene, at-least-once via acked-offset resume, and HA WAL resume.
    - Per the request: the WAL backend is configurable (mmapfile / lmdb / memory -
      LMDB optional), the whole feature is off by default and opt-in per publisher,
      and it includes a dedicated Egress Management admin UI (status, WAL depth,
      lag, retries, circuit-breaker state; pause/drain/flush/restart controls).
    - Added roadmap item A5 (Phase 1) + dependency-map node. Docs only.


## Version 5.7.0 highlights (WebAssembly sandboxed calculators - first cut of C2):
    - New WasmCalculator (core/calculator/wasm_calculator.py): runs a calculator
      compiled to WebAssembly inside the sandboxed wasmtime runtime. Logic can be
      written in any language (Rust, C, AssemblyScript, TinyGo, or hand-written
      WAT), compiled to a .wasm module, and executed memory-isolated - it cannot
      touch host memory/files/network, and a per-call `fuel` budget caps CPU and
      stops a runaway module deterministically. One runtime for every language.
    - To run one the host needs only: the wasmtime runtime (pip install wasmtime,
      which bundles the engine - no host C/Rust compiler), a .wasm/.wat module
      exporting the function, and the calculator config. Authoring toolchains are
      needed only when building the module, never on the host.
    - Ships an example module (examples/wasm/calculators.wat), a runnable demo
      (examples/wasm/run_wasm_calculator_example.py), 11 tests, and a Help guide
      ("WebAssembly (WASM) Sandboxed Calculators Guide"). wasmtime is optional and
      lazily imported.
    - Additive/backward-compatible: deployments not using WASM calculators need
      nothing new; a configured WASM calculator with the runtime missing fails
      loud (no silent fallback). v1 uses the f64 scalar boundary; batch/Arrow
      handoff is the documented next step (A1). 211 passed / 1 skipped.


## Version 5.6.4 highlights (design: dynamic / elastic DAG topology — roadmap C5):
    - Added a detailed design document, docs/design/C5-dynamic-dag-topology.md,
      for runtime graph growth/shrink (add/modify/remove nodes, calculators,
      edges) and data-driven template expansion: a templatized DAG section that
      materializes a per-key sub-pipeline behind a stable dispatcher->collector
      boundary and tears idle instances down. Grounded in the current engine:
      structural mutations applied transactionally between sweeps on the single
      compute thread, a generation-stamped re-sort, preserved equality gate and
      cycle checks, a low-risk pub/sub-mediated first cut, and HA standby
      convergence via deterministic-from-data design plus a replayable mutation log.
    - Added roadmap item C5 (Phase 3) referencing the design. Docs only.


## Version 5.6.3 highlights (version-reporting consistency + free-threading doc reconcile):
    - Verified a single source of truth: every UI page (app_version), the
      /metrics and health endpoints, the startup and HA banners, message
      packaging, and the FastAPI app metadata all derive from VERSION below;
      nothing reads a version from config. Added a release checklist here.
    - Removed stale version drift: config app.version (was 3.2.0) and the
      configuration help-page example (was 5.1.1) now track VERSION; both config
      files stay in parity and are marked informational-only.
    - Reconciled the free-threading guide to target Python 3.14t (officially
      supported free-threading; introduced experimentally in 3.13) to match the
      roadmap and the benchmark spike; renamed the guide accordingly. Docs/config
      only; no runtime behaviour change.


## Version 5.6.2 highlights (fix: Green-theme contrast on the DAG Designer):
    - The DAG Designer (web/templates/dag/designer.html) defines its own CSS
      variable blocks (a light :root and a [data-theme="dark"] flip) but had no
      [data-theme="green"] block, so under the Green theme the light defaults
      leaked through and clashed with the green global background - toolbar
      buttons, palette headers and item labels rendered dark-on-dark. Added a
      matching [data-theme="green"] block (green canvas/panels, light text, vivid
      node swatches) so the Designer reads correctly in all three themes.
    - Root cause: the earlier "green twin" pass only globbed top-level and help/
      templates, missing subdirectories (dag/). Verified no template now has a
      dark block without a green counterpart. CSS only; no behaviour change.


## Version 5.6.1 highlights (documentation: user guides for recent features + accuracy pass):
    - Added four user guides (docs/userguides/, auto-listed and categorized in the
      in-app Help Center): "Logging and Observability", "Backpressure (Credit-Based
      Flow Control)", "Benchmarking and Performance Harness", and "Native Arrow
      Calculators (Zero-Copy C Data Interface)" - closing the doc gap for the
      v5.4-v5.6 features (JSON logging, backpressure, the benchmark harness/Nexmark,
      and the A1 native handoff).
    - Updated benchmarks/README.md for the Nexmark workload and the --workload flag;
      added Help-Center categorization keywords (backpressure/benchmark/observability).
    - Accuracy sweep: corrected a stale "current VERSION" example pinned to 5.1.1 in
      the message-packaging guide. Docs only; no code/behaviour change.


## Version 5.6.0 highlights (A1 Arrow C Data Interface handoff + Nexmark benchmark workload):
    - A1 polyglot handoff (keystone): core/cpp/arrow_cdata.c is a native (C) kernel
      that reads exported Apache Arrow buffers IN PLACE via the standard C Data
      Interface ABI structs - no copy, no serialization. core/calculator/native_arrow.py
      exports a pyarrow column across that interface to the kernel and reads the
      result back, behind an opt-in NativeAffineCalculator. It compiles the kernel
      on first use and degrades gracefully to an identical pyarrow implementation
      when no C compiler is available, so nothing breaks. The same ABI is exactly
      what a C++, Rust, or Java/JNI calculator uses - this is the template for all
      of them. Byte-parity tests vs pyarrow (10 tests).
    - Phase 0 benchmark harness completed with a Nexmark workload: benchmarks/
      now runs BOTH a finance trade-ETL DAG and a Nexmark-subset DAG (Q1 currency
      convert + Q2 auction select) over the real in-memory engine
      (`python -m benchmarks.run_benchmark --workload {trade_etl,nexmark}`),
      reporting throughput, latency p50/p95/p99 and peak RSS, with CI smoke tests
      (4 tests). Recovery-time benchmarking remains TODO.
    - Additive and backward-compatible: default calculator/runner behaviour is
      unchanged; --workload defaults to trade_etl. 200 passed / 1 skipped.


## Version 5.5.0 highlights (credit-based backpressure - opt-in flow control):
    - New core/pubsub/backpressure.py: a reusable CreditController (thread-safe
      credit accounting + block/drop policy + optional timeout + observable stats)
      and a CreditQueue that returns a credit automatically on each get() - so no
      consumer call site changes.
    - Wired transparently into the in-memory topic fan-out: today's path does a
      non-blocking put and silently drops to a slow subscriber once its queue is
      full; with backpressure ON, each subscriber grants the publisher a bounded
      number of in-flight credits, the publisher spends one per message (waiting
      under 'block' or dropping-with-a-count under 'drop'), and the subscriber
      returns one per consume - pinning the producer's rate to the consumer's.
    - Fully opt-in and OFF by default (backpressure.* in application.yaml/.properties,
      kept in parity): when disabled, subscribe_to_topic returns a plain queue and
      the publish path is byte-identical to before - all prior tests pass unchanged.
    - get_backpressure_stats() exposes per-subscriber credit/in-flight/blocked/
      dropped/wait metrics for observability. 9 new tests
      (tests/test_backpressure.py). Engine critical path untouched when disabled.


## Version 5.4.0 highlights (configurable structured / JSON logging at the formatter level):
    - New core/log_config.py is the single source of truth for log configuration.
      A JsonFormatter renders each record as one compact line of JSON; a single
      configure_logging() attaches the chosen formatter to the root handlers, so
      the entire application switches between text and JSON purely at the
      formatter level - NOT ONE logger.*() call site changes.
    - Fully config-driven (logging.* in application.yaml/.properties, kept in
      parity): logging.format (text|json), logging.level, logging.json_fields
      (choose exactly which fields appear), logging.json_include_extra, and
      logging.text_format. Unknown fields/levels/format values raise rather than
      defaulting silently.
    - Structured per-event data is opt-in and additive: any call site may pass
      logger.info("msg", extra={"dag": name}) and those keys appear in the JSON
      automatically; untouched call sites just emit the base fields.
    - Wired through all three entry points from one place: the server
      (run_server), the webapp, and each multiprocessing worker (worker logs gain
      a `worker` field in JSON, or the [Worker-N] prefix in text).
    - Default remains text, so behaviour is unchanged until json is selected.
      11 new tests (tests/test_log_config.py). Logging/config only; engine
      untouched.


## Version 5.3.0 highlights (new "Green" theme + 3-way switcher; research paper refresh):
    - NEW THEME "Green": a gold-on-dark-green palette inspired by a green-dial gold
      watch (very dark green, golden yellow, black, white). It is a dark-family theme,
      so it inherits every dark-mode light-on-dark contrast fix; all palette colors
      verified AA/AAA. Doc/tutorial pages pick up gold headings/links for cohesion.
    - The theme control is now a DROPDOWN (Light / Dark / Green) with an active-check
      and per-theme icon, replacing the 2-state toggle. Choice persists in localStorage;
      Green maps to Bootstrap's dark component base.
    - Comparison table: brightened competitor/first-column text for a crisper read.
    - Research paper updated to reflect the v5.x advances: new abstract/contribution
      notes plus a "§7B Recent Advances" section covering the Arrow-native columnar
      (RecordBatch) zero-copy edge transport and the headless execution + orchestration
      model. PDF and LaTeX regenerated from the markdown (23pp) and synced across the
      docs/ and static/ copies. Theme/docs/CSS only; no engine or behaviour change.


## Version 5.2.1 highlights (accessibility: dark/light theme contrast audit):
    - Systematic WCAG contrast audit of every UI page in both themes, using a
      color-math tool to compute exact ratios rather than eyeballing.
    - BIGGEST FIX: the markdown viewer (.doc-content) that renders all 12 tutorials
      and ~30 user guides hardcoded dark text colors meant for a light pane, so in
      dark theme every heading, paragraph, table cell, link, inline-code chip and
      callout was near-invisible. Added a complete [data-theme="dark"] override set
      (headings, body, lists, tables + hover, links, code chips, callout blockquotes)
      — all AA/AAA on the dark card.
    - Fixed: comparison-table red marker (4.29 -> AA), dashboard pagination text,
      login form labels, landing-page captions, the Research and Time-Windows help
      pages, the user-guide listing, and dark-theme tab labels + brand icons on the
      pybind11/Rust/REST/Kafka/IBM-MQ integration pages (Kafka's near-black logo was
      invisible on dark).
    - Verified: theme palette variables all pass AA in both themes; remaining flagged
      items are decorative brand icons/arrows that are redundant with adjacent text
      labels (WCAG-exempt) or syntax colors on always-dark code panes. Docs/CSS only;
      no engine or behaviour change.


## Version 5.2.0 highlights (all tutorials unified to markdown, single world-class renderer):
    - Converted the 8 in-app HTML tutorials (Your First DAG ... JVM Pool) to markdown
      (docs/TUTORIAL_01_*.md ... TUTORIAL_08_*.md), faithfully preserving content:
      plain-English primers, step structure, code blocks (with language tags), ASCII
      flow diagrams (as code fences), callouts (as styled blockquotes), and tables.
    - All 12 tutorials (the 8 numbered + the 4 deep-dives: Arrow, High-Performance
      Arrow, EOD, Headless) now render through ONE path (the markdown viewer), ending
      the HTML/markdown split. Polished the shared viewer: callout boxes (gradient +
      accent), and a tutorial-aware "Back to Tutorials" button.
    - Removed the 8 bespoke tutorial HTML templates and their routes; repointed all
      Help Center tutorial cards to the markdown viewer (verified all 12 render 200,
      internal cross-links repaired). Single source of truth, portable, diffable.
      Docs/help only; no engine/behaviour change.


## Version 5.1.2 highlights (documentation: in-app tutorials elaborated + made discoverable):
    - New dedicated deep-dive tutorial docs/TUTORIAL_arrow.md:
      extremely high-performance DAGs with zero-copy Arrow RecordBatch — theory
      (where time goes, columnar/vectorization, immutability), design (edge_value
      dispatch, equality-gate preservation, layered speedups), full calculators +
      DAG, measurement, and a discussion of limits/tuning/when-to-use. Layman-first.
    - Elaborated the in-app HTML tutorials: added a consistent "in plain English"
      primer (analogy + new concepts in everyday terms + one-sentence summary) to
      tutorials 2-6 to match the quality of 1/7/8.
    - All 12 tutorials are now reachable from the Help Center: the 8 HTML tutorials
      plus a new "Hands-On Guides & Deep Dives" section linking the 4 markdown
      tutorials (Arrow how-to, High-Performance Arrow, EOD, Headless) via the
      markdown viewer. Verified all render. Docs-only; no engine/behaviour change.


## Version 5.1.1 highlights (documentation: elaborated tutorials + help consolidation):
    - Rewrote the EOD and headless tutorials (docs/TUTORIAL_eod_batch.md,
      docs/TUTORIAL_headless.md) to be layman-friendly: a plain-English concepts
      primer (DAG/node/edge/calculator/reactive), analogies, worked numbers, and
      expected-output walkthroughs. Added a "Part 0 — plain English" primer to the
      Arrow tutorial (rows-vs-columns, vectorization, Arrow/RecordBatch, the
      setup-cost trade-off). Tutorials verified accurate against the shipped code.
    - Help consolidation: the 9 broker integration pages (Kafka, Redis, RabbitMQ,
      ActiveMQ, LMDB, IBM MQ, TIBCO, REST, In-Memory) now carry an "overview ->
      full setup guide" cross-link to their detailed user guide, giving the two doc
      systems clear, complementary roles instead of competing. (Earlier in 5.1.x:
      removed two orphan help templates, free_threading.html / worker_pool.html,
      superseded by parallelism.html.) Docs-only; no engine/behaviour change.


## Version 5.1.0 highlights (A1 keystone: Arrow RecordBatch on edges — zero-copy transport):
    - core/dag/edge_value.py: value-type dispatch for the three things the engine
      does to edge values — copy / compare (equality gate) / consolidate — plus a
      JSON-safe describe. For any non-batch value each helper is the exact previous
      dict behaviour; a pyarrow.RecordBatch is shared BY REFERENCE (immutable, so
      safe), removing the per-stage deep-copy.
    - graph_elements.py: minimal mechanical substitution (deepcopy->ev_copy,
      ==/!=->ev_equals, dict.update->ev_consolidate, details->ev_describe, plus a
      batch-aware Edge.get_data). The dict path is behaviourally identical; diff is
      trivially reviewable; existing classes byte-identical.
    - ArrowCalculator.calculate gains a RecordBatch fast-path (stay columnar, no
      dict<->Arrow conversion mid-pipeline). New opt-in nodes
      ArrowBatchingSubscriptionNode / ArrowFlatteningPublicationNode (append-only)
      convert dict->batch once at ingress and batch->dict once at egress.
    - core/transformer/arrow_transformer.py: the per-edge telescopic view in Arrow
      — ProjectionBatchTransformer (zero-copy select/rename) and
      RowTransformerBatchAdapter (bridge a row transformer onto a batch edge).
    - Measured (perftest/run_arrow_transport_example.py, 20k trades): per-trade
      output IDENTICAL to the dict path; throughput ~2.29x the v4.5.0 envelope path
      by removing the per-stage copies. Equality-gate invariant preserved via
      RecordBatch.equals (not bypassed). Fail-fast on batch fan-in / row transformer
      on a batch edge. pyarrow stays optional for the core.
    - Tests: tests/test_edge_value.py + tests/test_arrow_transport.py (16). Suite:
      166 passed, 1 skipped.


## Version 5.0.0 highlights (headless execution + control-plane/worker orchestration):
    - core/dag/headless_runner.py: a first-class "run-once" CLI that starts a DAG
      WITHOUT the web UI, optionally replays a bounded feed, detects completion
      (count-based or quiescence), drains with zero message loss, writes a summary
      JSON, and exits 0/1 so a scheduler can react.
    - core/dag/job_dispatch.py: JobDispatchCalculator, an idempotent (exactly-once
      per job key), asynchronous (non-blocking), bounded (max_concurrent + pending
      queue / worker pool), observable (records each child's summary, can publish a
      completion event, shutdown() terminates live children) dispatcher node. A
      long-running control-plane DAG reacts to events and launches ephemeral
      headless workers for heavy, isolated, run-to-completion ETL.
    - perftest/run_orchestration_example.py: end-to-end demo — a control-plane DAG
      dispatches headless workers (exactly-once, capped) that each process an EOD
      feed and exit; the parent reacts to their completion.
    - docs/HEADLESS_AND_ORCHESTRATION.md + Help page; ROADMAP notes the operational
      capability. Tests: tests/test_headless_and_dispatch.py (5). Suite: 150 pass.
    - Fully additive: new modules + a calculator selected only by DAGs that
      reference it; the web app / DAGComputeServer / engine paths are untouched.


## Version 4.9.0 highlights (design: edge transformers / telescopic views in Arrow):
    - Extended docs/design/A1-recordbatch-edges.md (section 5.1) covering how the
      per-edge transformer's "telescopic view" of upstream state maps onto Arrow:
      zero-copy columnar projection/slice/rename, a transform_batch contract, a
      RowTransformerBatchAdapter for legacy transformers, and the edge as the
      per-consumer batch/row boundary adapter. Design only.


## Version 4.8.0 highlights (design doc: Arrow RecordBatch on edges):
    - docs/design/A1-recordbatch-edges.md: detailed design for the final A1
      increment (zero-copy RecordBatch transport on DAG edges) and its
      backward-compatibility strategy. Design only -- no code/engine change yet.


## Version 4.7.0 highlights (light/dark theme text-contrast fixes - UI only):
    - Fixed low-contrast text across both themes to meet WCAG AA (>= 4.5:1):
      darkened light-theme --text-muted (#9aa3c0 -> #616a8c, 2.5:1 -> 5.3:1) and
      --text-secondary; lightened dark-theme --text-muted (#5a6488 -> #868fb0,
      3.1:1 -> 5.6:1) and --text-secondary, in web/templates/base.html.
    - About page: the author card has an always-light background, so its text is
      now pinned to dark values; the creator name was invisible in dark theme
      (#eef0f8 on a light card, ~1:1) and is now ~16:1.
    - Comparison page: the head-to-head table sits on an always-dark card, so its
      cell text is now forced light with !important (Bootstrap's theme table
      colour was overriding it to dark-on-dark in light theme); faint legend/note
      text brightened.
    - No engine, API, or logic changes; templates/CSS only.


## Version 4.6.0 highlights (batch-file / EOD processing guide + example):
    - New Help page "Batch File Processing" (web/templates/help/batch_file_processing.html,
      route /help/batch-file-processing, linked from the Help index) plus
      docs/BATCH_FILE_PROCESSING.md: how to process large interrelated batch
      files (e.g. EOD trade + FX + client-limit feeds) with DishtaYantra, and
      when one-record-at-a-time is and isn't needed.
    - Runnable example in perftest/: eod_enrichment_calculators.py
      (ReferenceEnrichCalculator for fact x dimension enrichment;
      RunningExposureCalculator for the genuinely-sequential case) and
      run_eod_example.py (replays a trade file enriched against FX + limit feeds,
      one-at-a-time vs auto-batched, with limit-breach detection).
    - Documentation only / additive; no engine change.


## Version 4.5.0 highlights (A1 automatic source-batching - additive, opt-in):
    - Two new opt-in node types make Arrow batching automatic while preserving the
      external per-message contract: BatchingSubscriptionNode drains incoming
      messages into one columnar envelope per cycle (load-adaptive), and
      FlatteningPublicationNode republishes each record on output.
    - The existing SubscriptionNode / PublicationNode classes are byte-for-byte
      unchanged (verified by diff); the new types are appended subclasses, so
      every existing DAG behaves exactly as before.
    - Worked example perftest/perftest_arrow_autobatch.json +
      perftest/run_autobatch_example.py: ordinary per-message trades in and out,
      batched internally, output identical to the all-row pipeline (CI:
      tests/test_autobatch.py). Throughput gain is currently modest (the dataflow
      deep-copies each envelope per stage); carrying Arrow RecordBatches on edges
      is the next A1 increment.
    - Docs updated: ROADMAP, TUTORIAL, ARCHITECTURE, README, A1 design docs, and
      the architecture help page.


## Version 4.4.0 highlights (Arrow tutorial + adapter ergonomics):
    - Hands-on tutorial at docs/TUTORIAL_arrow.md: write an ArrowCalculator,
      use it as a drop-in, vectorize batches, mix row + Arrow in one graph, bridge
      legacy calculators with RowCalculatorBatchAdapter, and measure the result.
    - RowCalculatorBatchAdapter now resolves config["wrapped"] using the same
      {"type": <builtin|dotted.path>, "config": {...}} shape as a DAG calculator
      entry (with CalculatorFactory fallback), so it is configurable directly in
      DAG JSON. Backward compatible (passing a wrapped instance still works).
    - README linked to the tutorial. No engine behavior change.


## Version 4.3.0 highlights (A1 worked example + old/new coexistence):
    - Worked example hosted in perftest/: arrow_etl_calculators.py (vectorized
      FX/notional/fee/risk, each output-identical to the row versions),
      perftest_arrow_mixed.json (a SINGLE graph mixing row and Arrow calculators),
      and run_arrow_example.py (a runnable demonstration).
    - CONFIRMED by execution + CI (tests/test_arrow_coexistence.py): old-style and
      new-style DAGs/calculators coexist in the same instance, AND a single graph
      can contain both row and Arrow nodes (mixed graph output is bit-identical to
      the all-row equivalent over 20,000 trades).
    - Detailed design doc + decision tree at
      docs/design/A1-worked-example-and-coexistence.md; README, ARCHITECTURE.md,
      and the Calculators/Architecture help pages updated accordingly.
    - Engine (core/dag/*) and core_calculator.py remain unchanged.


## Version 4.2.0 highlights (Phase 1 / A1 vertical slice - additive, opt-in):
    - ArrowCalculator contract (core/calculator/arrow_calculator.py): an opt-in
      columnar calculator that is ALSO a drop-in row DataCalculator, so adopting
      it requires NO engine change. The engine (core/dag/*) and core_calculator.py
      are byte-for-byte unchanged; existing DAGs and stored data are unaffected.
    - Vectorized ArrowFxConvert / ArrowNotional calculators that are output-
      identical to the perftest row calculators (exact parity, validated in CI).
    - End-to-end vertical slice through the real ComputeGraph
      (benchmarks/a1_vertical.py): the same two-stage pipeline run row-at-a-time
      vs Arrow batch-envelopes, verified IDENTICAL per-trade and measured ~1.8x
      faster end-to-end (the pure kernel is ~11.8x; the node-boundary dict<->Arrow
      conversion accounts for the rest - the documented next increment is Arrow
      transport to remove that bridging).
    - RowCalculatorBatchAdapter lets legacy row calculators run in a batched path
      unchanged (mixed-DAG compatibility).


## Version 4.1.0 highlights (Phase 1 / A1 kickoff - design + de-risking spike):
    - A1 Arrow columnar data-plane design RFC at docs/design/A1-arrow-data-plane.md
      (Arrow RecordBatch edges, an additive ArrowCalculator batch contract,
      micro-batching with a linger cap, zero-copy polyglot handoff, a one-path
      vertical slice, and a backward-compatible migration plan)
    - Runnable A1 spike (benchmarks/arrow_vectorization_spike.py) proving the
      core claim on this codebase's own trade-ETL numerics: vectorized Arrow
      kernels ran ~11.8x faster than the current row-at-a-time model with
      bit-identical output (validated row-vs-Arrow in the test suite)
    - Still no production engine behavior change; this de-risks A1 and gives the
      RFC measured evidence before the core refactor begins


## Version 4.0.0 highlights (start of the "best-in-class" roadmap):
    - Strategic feature roadmap published at docs/ROADMAP.md (Arrow data plane,
      no-GIL Python, embedded streaming SQL, event-time/exactly-once,
      incremental materialized views, WASM calculators, real-time AI, and a
      graceful single-node-first scale path)
    - Phase 0 foundations shipped: a reproducible benchmark harness
      (benchmarks/) that drives the real engine over the in-memory broker and
      reports latency percentiles, throughput, and peak memory
    - Free-threading readiness spike (benchmarks/freethreading_spike.py) that
      measures CPU-bound calculator scaling across threads and inventories
      native-extension GIL readiness, de-risking the no-GIL work (step A3)
    - No engine behavior change in this release; it establishes the
      measurement baseline that all later roadmap work is compared against


## Version 3.1.0 highlights:
    - Multiple DAG folders: 'storage.dags.prefixes' lists extra, logically
      grouped folders (config/dags is always included); each scanned for its
      DIRECT .json children only (never sub-folders)
    - Globally-unique DAG names enforced: a name collision across folders is
      FATAL at startup (server refuses to boot, listing every collision) and
      REJECTED at reload (incumbent keeps running, newcomer not booted)
    - Persistent red dashboard banner for reload-time collisions, with a
      one-click button to delete the offending DAG file
    - External library paths (external.module.path.*) documented: add custom
      calculator/transformer module directories to the import path at startup
    - Zero-message-loss ingestion: every subscriber applies backpressure
      (block-retry) instead of dropping on a full queue; Kafka consumer now
      buffers the full polled batch and delivers one message at a time


## Version 3.0.0 highlights:
    - Event-driven compute loop: DAGs react to data immediately instead of
      polling on a fixed interval, collapsing per-hop latency from tens/hundreds
      of milliseconds to sub-millisecond on the in-process path
    - Subscribers wake the compute loop on message arrival (notify hook); the
      idle poll interval is configurable (idle_poll_interval) and far lower by
      default, cutting ingress latency ~180x in benchmarks
    - Suitable for near-real-time, low-latency pipelines (low-single-digit-ms
      hops) in addition to throughput workloads
    - Free-threading (Python 3.13 no-GIL) enablement + testing guide and a
      dependency compatibility checker
    - Interactive Cytoscape.js graph view (zoom/pan/drag, live node-state panel)
    - Expanded tutorials: two-subgraph showcase, parallel-DAG coordination,
      multi-worker-pool execution, and the JVM gateway pool


## Version 2.2 highlights:
    - FastAPI web layer (replacing Flask) served by uvicorn, with full
      light/dark theming and locally-vendored front-end assets
    - Pluggable storage abstraction (FileSystem / S3 / Azure Blob / GCS)
    - Database-backed users, roles, and API keys (SQLAlchemy DAO layer,
      SQLite default with PostgreSQL switchable via configuration)
    - Configurable HA Manager (Zookeeper / S3 / Redis / Socket providers),
      with the active role surfaced in the navbar on every page
    - Format-agnostic configuration (YAML or .properties) with
      ${VAR:default} resolution and env / command-line overrides
    - AWS (SQS / Kinesis / SNS) and Azure (Service Bus / Event Hubs)
      managed-messaging pub/sub, plus S3 / Azure Blob / GCS object stores
    - Market-aware scheduling: time windows with duration syntax,
      day-of-week allow/deny lists, and USA / Canada holiday calendars
    - Resilient wrappers for REST, SQL, Aerospike, gRPC, and cloud messaging
    - Greatly expanded automated test coverage
