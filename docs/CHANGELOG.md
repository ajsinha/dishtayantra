# DishtaYantra Changelog

The authoritative current version is `core/version.py` (`VERSION`). This file
records the per-release highlights that previously lived in the version.py
docstring.


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
    - New dedicated deep-dive tutorial docs/TUTORIAL_high_performance_arrow.md:
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
    - Hands-on tutorial at docs/TUTORIAL_arrow_dag.md: write an ArrowCalculator,
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
