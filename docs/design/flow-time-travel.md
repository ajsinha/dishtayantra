# Flow Time-Travel — Design & Wiring

> Go back over the last 24 hours, pick a time range, watch the data flow
> through the DAG, and download it — via UI or API. On by default (SQLite),
> non-blocking, and disable-able at will so throughput is never held hostage.

---

## 1. The core idea: the equality gate already gives us a change-log

DishtaYantra's engine recomputes a node **only when its input changes** (the
equality gate in `core/dag/graph_elements.py:Node.compute`). So the complete
history of a DAG *is* the ordered stream of "node fired: input → output at time
T". We don't sample every tick — we capture only changes. That stream is
naturally compact and is exactly what's needed to replay flow. **Time-travel =
persist that change-log, then replay a window of it.**

The capture point is one line, right after the gate confirms the output changed:

```python
# core/dag/graph_elements.py, inside compute(), after self._messages_out += 1
FLOW_RECORDER.record_node_fire(self)   # no-op & ~free when off; never raises
```

---

## 2. Architecture

```
 compute thread                         background (one daemon thread)
 ────────────                           ──────────────────────────────
 Node.compute() fires
   └─ FLOW_RECORDER.record_node_fire(node)
        if not enabled: return         ← single bool read when OFF
        cheap snapshot (shallow)       ← no JSON on the hot path
        queue.put_nowait(evt) ─────────▶  bounded queue (maxsize)
        (queue Full → drop + count)         └─ drain: batch → JSON-encode
                                               → store.write_batch(rows)
                                                   │
                          ┌──────────────┬──────┴──────┬──────────────┐
                          ▼              ▼             ▼              ▼
                       sqlite        postgres        noop          paimon
                     (default,     (SqlFlowStore,  (discard)     (optional)   + aerospike
                   separate file)  separate DB)                               (experimental)

 Every row is stamped with provenance (instance, host, port) so several servers
 can share ONE flow database (e.g. a central Postgres) and queries can scope to
 one origin. The old `dao` backend (which shared the application DB) is FORBIDDEN.

 retention sweep (daemon): every N min, store.purge_older_than_ms(cutoff)
 API/UI (FastAPI):  GET /flow  +  /api/flow/*   →  store.query / iter_export
```

New modules: `core/flow_recorder.py`, `core/flow_store.py`,
`core/flow_retention.py`, `routes/flow_routes.py`,
`web/templates/dag/flow_time_travel.html`.

---

## 3. Performance & the disable switch (measured)

Design rules, in priority order: **(1)** zero cost when disabled; **(2)** never
block the compute thread; **(3)** never raise into the caller. These mirror the
discipline of `core/audit_log.audit`.

* **Disabled is the hot path's first line** — `if not self._enabled: return`.
  Turning the feature off (config `flow_recorder.enabled=false`, a runtime
  `POST /api/flow/disable`, the admin control on **Admin → System Monitoring**,
  or `dyflow disable`) restores full throughput instantly, no restart. Capture can
  also be toggled **per-DAG**: the recorder keeps an override map and the effective
  state for a DAG is `override.get(dag, global_flag)`. The `/flow` page shows status
  read-only; the enable/disable control lives only on the admin monitoring page.
* **Serialization is off the hot path.** The compute thread takes a cheap
  shallow snapshot and does `put_nowait`; all JSON encoding + I/O happen on the
  drain thread.
* **A full queue drops (and counts), never blocks.** A slow or stuck store can
  never stall compute — overflow is shed and surfaced in `stats().dropped`.
* **Sampling dials the cost.** `flow_recorder.sample_rate < 1.0` records a
  fraction of fires.

Benchmark (`/home/claude/flow_demo/bench_flow2.py`, this machine, trivial
compute so recording cost is the *worst-case* relative share):

| scenario | throughput | notes |
|---|---|---|
| recording **disabled** | **6.1M fires/sec** (163 ns) | the kill switch fully restores throughput |
| enabled, every fire, sqlite | 147k fires/sec | GIL contention with the writer in a zero-work microbench |
| enabled, `sample_rate=0.1` | 1.15M fires/sec | sampling dials cost ~8× |
| sustainable load | 50k fired = 50k written, **0 dropped** | no loss when the store keeps up |
| burst vs a stuck 10ms/batch store | producer **268k/sec**, 268k dropped, **0 errors** | 5× the store's capacity — compute never blocked |

The enabled overhead is dominated by the background writer competing for the
GIL in a benchmark where a "fire" otherwise costs nothing; a real node also does
calculator/transformer work, so the *relative* hit is smaller. The guarantees
that matter — disabled is free, compute never blocks, nothing is lost under
sustainable load — hold regardless.

---

## 4. Storage backends (`flow_recorder.store`)

| value | class | when |
|---|---|---|
| `sqlite` *(default)* | `SqliteFlowStore` | frugal, zero new deps, on-prem single node; a **separate file** from the application DB; WAL lets the UI read while the drain writes |
| `postgres` | `SqlFlowStore` (`core/flow_store_sql.py`) | a **separate** SQLAlchemy database via `flow_recorder.db_url` — works for PostgreSQL or any URL (incl. sqlite). Lets **many instances share one central flow DB** |
| `noop` | `NoopFlowStore` | keep wiring in place, persist nothing |
| `paimon` | `PaimonFlowStore` (`core/flow_store_paimon.py`) | **optional** lake/lakehouse archive (see §4.1) |
| `aerospike` | `AerospikeFlowStore` (`core/flow_store_aerospike.py`) | **optional, experimental** (see §4.2) |
| ~~`dao`~~ | — | **FORBIDDEN.** Previously shared the application DB; flow history must never co-locate with users/keys/audit. The factory rejects it and the webapp refuses any `db_url`/`store_path` that resolves to the app DB. |

All backends implement the `FlowStore` ABC (`core/flow_store.py`) and stamp every
event with **provenance** `(instance, host, port)`. `query()` / `state_at()` take
an optional `instance` filter and `distinct_dags()` groups by `(instance, dag_id)`,
so a shared database (e.g. central Postgres) stays origin-aware.

Schema (`flow_events`, incl. the provenance columns) ships as
`config/schema/flow_events_sqlite.sql` and `flow_events_postgres.sql`, and is
mirrored by the `FlowEvent` ORM model. Indexed on `(dag_id, ts_ms)` for window
queries, `(dag_id, node_id, ts_ms)` for `state-at`, and `(instance, dag_id, ts_ms)`
for origin-scoped queries. The schema is defined by **one dedicated DDL file per
dialect** (`config/schema/flow_events_sqlite.sql`, `flow_events_postgres.sql`) and
applied as-is — there is **no migration path**. To change the schema, edit the
file and recreate the (transient, ~24h) flow database.

### 4.1 Apache Paimon — implemented (pure-Python, no Flink/Spark)

`PaimonFlowStore` is a **real** backend on an append-only, hourly-partitioned
(`dt=YYYYMMDDHH`, UTC) Paimon table via pure-Python **PyPaimon ≥ 1.4** (no JVM).
`write_batch`, `query` (predicate push-down for dag_id/instance/ts-range/nodes/seq),
`state_at`, `distinct_dags`, and the `instance` filter are implemented and tested
on a local filesystem warehouse.

* Import-guarded: absent `pypaimon`/`pyarrow`, constructing it raises a clear error.
* **Retention caveat (verified):** PyPaimon's local *filesystem* catalog cannot
  drop partitions programmatically (it raises `NotImplementedError`; partition
  drop needs a **REST catalog**). So `purge_older_than_ms` is a graceful, logged
  no-op on a local FS warehouse — read/write are unaffected. Bound retention with
  a REST catalog or the table's `partition.expiration-time`.

### 4.2 Aerospike — experimental, unverified

`AerospikeFlowStore` is a complete, API-correct implementation (record key
`instance|dag|seq`, `ts_ms` integer secondary index + client-side filtering,
TTL/range-delete retention), import-guarded behind the `aerospike` client. It has
**not been run against a live Aerospike cluster**; treat the connect/write/read
paths as experimental and validate before production use.

Recommendation: `sqlite` is the frugal default. Use `postgres` when several
instances should share one flow database. Reach for `paimon` only for columnar,
long-horizon, time-travel-queryable archives readable by DuckDB/Trino/Spark/Ray.

---

## 5. API (behind `AuthGuards`)

| method | path | auth | purpose |
|---|---|---|---|
| GET | `/flow` | login | the Time-Travel UI page |
| GET | `/api/flow/status` | login | recorder stats + `enabled` |
| POST | `/api/flow/enable` | admin | turn capture on at runtime |
| POST | `/api/flow/disable` | admin | turn capture off at runtime |
| GET | `/api/flow/dags` | login | DAGs with counts + time bounds |
| GET | `/api/flow/{dag_id}?from&to&nodes&limit&after` | login | events in a window (default last 24h) |
| GET | `/api/flow/{dag_id}/state-at?ts=` | login | reconstructed latest output per node |
| GET | `/api/flow/{dag_id}/export?from&to&format=jsonl\|csv&download=1` | login | streamed attachment (memory-safe) |

`from`/`to` are epoch-ms; omitting them defaults to the last 24h. Export streams
via a generator so large windows don't blow memory.

### 5.1 Command-line client — `tools/dyflow.py`

A sibling to `dyadmin`/`dyapikey`: same auth (API key Bearer, or session login),
same `--url`/`--dry-run`/`--json`/`--timeout` flags. It scripts the endpoints
above and, crucially, **streams `download` straight to a file** so a 24h window
never has to fit in memory. Time windows accept human input — `--last 24h`/`90m`/
`2d`, ISO 8601, or epoch (ms or s).

```
dyflow status                                                  # global flag + per-DAG overrides
dyflow enable  --dag trade_etl                                 # or omit --dag for all DAGs
dyflow disable                                                 # all DAGs (admin)
dyflow dags --last 24h
dyflow events trade_etl --last 1h --nodes validate,fx --limit 500
dyflow state-at trade_etl --at 2026-06-22T10:00
dyflow download trade_etl --last 24h --format csv -o flow.csv   # or -o - for stdout
dyflow --dry-run download trade_etl --last 30m                  # show the URL only
```

Connection via flags or env: `DY_URL`, `DY_API_KEY`, `DY_ADMIN_USER`,
`DY_ADMIN_PASSWORD`.

---

## 6. UI — the Time-Travel console

**Rolling 24h distribution (in-memory).** The recorder keeps a per-DAG rolling 24h histogram (120 x 12-min buckets) updated from the drain thread as events are written - event count and distinct compute-cycle count per bucket (cycle_ids are monotonic per bucket, so the distinct count is exact). `GET /api/flow/{dag}/distribution` serves it in O(120) with no DB scan, so the bottom-pane chart stays cheap even with several concurrent replays. The client auto-focuses the window to the active bucket range on load (so a quiet 24h isn't sparse) and re-slices client-side as From/To change - no further server hits. `/histogram` remains for an exact SQL aggregate over an arbitrary window.

**Distribution chart.** The bottom pane is a cycle-count distribution chart, not a raw slider: `GET /api/flow/{dag}/histogram?from&to&buckets=N` returns per-bucket compute-cycle and event counts via a single SQL aggregate (SQLite override; the base class has an iter_export fallback), so no events are shipped to the browser. The window is set by the From/To pickers (max 24h); an amber playhead advances over the chart during replay.

**Compute-cycle id.** The engine runs each propagation wave as one topological sweep in `do_compute`; a sweep that does work is one *compute cycle*. The graph stamps a monotonic `cycle_id` (per dag-instance) before each sweep, and every node fire recorded in that sweep carries it (column `flow_events.cycle_id`, nullable; older rows null). Idle sweeps neither burn an id nor leave a gap. This lets a window be grouped and inspected one whole cycle at a time.

**Cycle-grouped SSE.** `/stream` groups events by `cycle_id` and pushes a configurable number of *complete* cycles per SSE message (`cycles`, default 256), never a partial cycle, so the UI render is always a consistent start-to-finish wave and cycles arrive in order. A hard per-push event cap bounds memory for legacy (null cycle_id) rows or a pathologically large single cycle.

**Replay guards.** Concurrent replays are capped by `flow_recorder.max_concurrent_streams` (default 5; over the cap a replay gets a 'busy' event) and each replay by `flow_recorder.stream_max_seconds` (default 120s, which also bounds the WAL read snapshot it holds). The page header shows active replays / cap. These are web-tier guards; the compute path is isolated (separate WAL flow DB; enqueue-only hot path).

**Large-window replay (SSE).** The page does not load a whole window into
the browser; on play it opens `GET /api/flow/{dag}/stream?from&to&batch=1000`
and consumes Server-Sent-Event batches through a bounded render queue, so
memory and DOM stay flat for windows with hundreds of thousands of events.
The window is capped to 24h (From/To pickers or timeline handles); Download
exports the same window as JSONL via `iter_export` (streaming, one record
per line).

A full-screen instrument console (`/flow`): a dark Cytoscape canvas where a
playhead sweeps a 24h timeline and the DAG lights up as historical data moves
through it — teal = data on an edge, amber = a node firing, cyan = playhead.

* **Timeline** with a firing-density histogram (15-min buckets), a draggable
  playhead, and a two-handle **range brush** to sub-select a window.
* **Transport**: play/pause, speed 0.25×–12×, jump-to-start; the DAG animates
  in event order with edge-value labels and node firing halos.
* **Event inspector**: the event stream for the window; click any event to see
  its input → output JSON.
* **Download** exports the selected window. The **Recording pill** shows capture
  status **read-only**; the enable/disable control lives on Admin → System
  Monitoring (global or per-DAG), or use `dyflow enable|disable [--dag NAME]`.

The in-app template builds the graph dynamically from live API data and falls
back to a synthetic 24h trade-ETL sample so the page is never blank on first
use. Cytoscape is the **view only**; the recorded events are the model — and per
the known Cytoscape quirk, edge-label styles use concrete values, not `var()`.

A standalone, self-contained version (CDN Cytoscape + embedded sample) ships as
`web/templates/dag/flow_time_travel_standalone.html` for offline demos.

---

## 7. Configuration (ON by default, SQLite)

```
flow_recorder.enabled=true               # capture on at startup (default ON)
flow_recorder.store=sqlite               # sqlite | postgres | noop | paimon | aerospike  (dao FORBIDDEN)
flow_recorder.store_path=data/flow_history.db   # sqlite: separate file from the app DB
#flow_recorder.db_url=postgresql+psycopg://user:pass@host:5432/flowdb  # store=postgres
#flow_recorder.warehouse=data/flow_warehouse                          # store=paimon
#flow_recorder.aerospike_hosts=127.0.0.1:3000                         # store=aerospike (experimental)
flow_recorder.maxsize=100000             # bounded queue; overflow dropped (counted)
flow_recorder.max_payload_bytes=2048     # per-side JSON cap on captured values
flow_recorder.sample_rate=1.0            # 1.0 = every fire; lower to shed load
flow_recorder.retention_hours=24         # history kept; <= 0 disables purging
flow_recorder.retention_sweep_minutes=30
flow_recorder.maintenance=none           # none | incremental | full (sqlite VACUUM, config-gated)
```

The recorder is **ON by default** (`enabled=true`, SQLite store); disable it
with `enabled=false`, or toggle capture at runtime from the UI (admin). The runtime toggle is in-memory and
does not persist across restart — startup state always comes from
`flow_recorder.enabled`.

(Mirrored in `application.properties` and `application.yaml`.)

---

## 8. Files

**New:** `core/flow_recorder.py`, `core/flow_store.py` (+ `FlowStore` ABC,
`SqliteFlowStore`, `NoopFlowStore`, factory), `core/flow_store_sql.py`
(`SqlFlowStore` — separate sqlite/Postgres), `core/flow_store_paimon.py`
(`PaimonFlowStore`), `core/flow_store_aerospike.py` (`AerospikeFlowStore`),
`core/flow_retention.py`, `routes/flow_routes.py`,
`web/templates/dag/flow_time_travel.html`,
`web/templates/dag/flow_time_travel_standalone.html`,
`tools/dyflow.py` (CLI), `tests/test_flow_recorder.py`,
`tests/test_flow_retention_purge.py`,
`config/schema/flow_events_sqlite.sql` + `flow_events_postgres.sql`.

**Modified (additive, backward-compatible):** `core/db/models.py` (+`FlowEvent`
incl. provenance columns `instance/host/port` + indexes), `core/dag/graph_elements.py`
(import + 1-line hook), `web/dishtayantra_webapp.py` (construct recorder/store with
provenance, forbid `dao`/app-DB, register `FlowRoutes`, start retention, stop on
shutdown), `routes/__init__.py` (export `FlowRoutes`),
`config/application.properties` + `application.yaml` (keys).
(The earlier `core/db/dao.py` `FlowDAO` path is retained only for the benchmark
harness against an isolated DB; it is **not** a selectable flow store — `dao` is
forbidden.)

---

## 9. Verification done

* `bench_flow2.py` — throughput, no-loss-under-load, burst non-blocking, sampling.
* `FlowDAO` end-to-end against SQLite — write/range-query/`state_at`/`distinct_dags`/purge.
* FastAPI `TestClient` — enable/disable, status, window query, `state-at`, JSONL+CSV export.
* `py_compile` of every touched module; engine hook presence asserted.

---

## 10. SLO / staleness alerting (v1)

Built directly on the change-log, opt-in (`alerts.enabled`, `alerts.rules_file`).
A rule breaches when a DAG (or a named node) has produced **no output change**
within `max_age_seconds`. The engine (`core/alerts.py`) is pure and decoupled: it
reads the flow store through a provider callable and evaluates on demand at
`GET /api/alerts` (no background thread). Per-DAG rules use `distinct_dags()`
(`max_ts`); per-node rules use one `state_at(now)` per DAG, cached within an
evaluation. Surfaced on Admin → System Monitoring and via `dyflow alerts`.

**Honest signal scope.** Because of the equality gate this is *output-change*
staleness — a constant-but-healthy stream reads as stale; size limits accordingly.
Two follow-ups are deliberately out of v1 because they need small engine additions:
*execution-liveness* ("ran but produced no change" — `increment_compute_count()` is
currently commented out, so there is no reliable per-node last-run timestamp yet)
and *error-rate* SLOs (the per-node `_errors` deque is bounded to 10, so it can't
express a true rate without a counter).

## 11. Release ritual

This is a feature addition (target **v5.26.0**). The six sync points still need
the lockstep bump: `core/version.py` `VERSION`; `application.properties`
`app.version`; `application.yaml` `version`; `README.md` badge + Current version;
`web/templates/help/configuration.html`; `about.html` (dynamic). Docs to refresh
alongside: Configuration Reference, Administration & Maintenance, API Reference.
