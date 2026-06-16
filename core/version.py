"""
DishtaYantra Version Information
================================

Single source of truth for the application version. Every module, template,
banner, and document must reference this module rather than hard-coding a
version string.

Version 5.3.0 highlights (new "Green" theme + 3-way switcher; research paper refresh):
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

Version 5.2.1 highlights (accessibility: dark/light theme contrast audit):
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

Version 5.2.0 highlights (all tutorials unified to markdown, single world-class renderer):
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

Version 5.1.2 highlights (documentation: in-app tutorials elaborated + made discoverable):
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

Version 5.1.1 highlights (documentation: elaborated tutorials + help consolidation):
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

Version 5.1.0 highlights (A1 keystone: Arrow RecordBatch on edges — zero-copy transport):
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

Version 5.0.0 highlights (headless execution + control-plane/worker orchestration):
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

Version 4.9.0 highlights (design: edge transformers / telescopic views in Arrow):
    - Extended docs/design/A1-recordbatch-edges.md (section 5.1) covering how the
      per-edge transformer's "telescopic view" of upstream state maps onto Arrow:
      zero-copy columnar projection/slice/rename, a transform_batch contract, a
      RowTransformerBatchAdapter for legacy transformers, and the edge as the
      per-consumer batch/row boundary adapter. Design only.

Version 4.8.0 highlights (design doc: Arrow RecordBatch on edges):
    - docs/design/A1-recordbatch-edges.md: detailed design for the final A1
      increment (zero-copy RecordBatch transport on DAG edges) and its
      backward-compatibility strategy. Design only -- no code/engine change yet.

Version 4.7.0 highlights (light/dark theme text-contrast fixes - UI only):
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

Version 4.6.0 highlights (batch-file / EOD processing guide + example):
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

Version 4.5.0 highlights (A1 automatic source-batching - additive, opt-in):
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

Version 4.4.0 highlights (Arrow tutorial + adapter ergonomics):
    - Hands-on tutorial at docs/TUTORIAL_arrow_dag.md: write an ArrowCalculator,
      use it as a drop-in, vectorize batches, mix row + Arrow in one graph, bridge
      legacy calculators with RowCalculatorBatchAdapter, and measure the result.
    - RowCalculatorBatchAdapter now resolves config["wrapped"] using the same
      {"type": <builtin|dotted.path>, "config": {...}} shape as a DAG calculator
      entry (with CalculatorFactory fallback), so it is configurable directly in
      DAG JSON. Backward compatible (passing a wrapped instance still works).
    - README linked to the tutorial. No engine behavior change.

Version 4.3.0 highlights (A1 worked example + old/new coexistence):
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

Version 4.2.0 highlights (Phase 1 / A1 vertical slice - additive, opt-in):
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

Version 4.1.0 highlights (Phase 1 / A1 kickoff - design + de-risking spike):
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

Version 4.0.0 highlights (start of the "best-in-class" roadmap):
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

Version 3.1.0 highlights:
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

Version 3.0.0 highlights:
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

Version 2.2 highlights:
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

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

VERSION = "5.3.0"
BUILD_DATE = "2026-06-15"
APP_NAME = "DishtaYantra"


def get_version_banner() -> str:
    """Return a one-line human readable version banner."""
    return f"{APP_NAME} Compute Server v{VERSION} (build {BUILD_DATE})"
