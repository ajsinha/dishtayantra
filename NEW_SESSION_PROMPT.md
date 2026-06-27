# DishtaYantra — New Session Prompt

*Paste this into a new chat to continue work cleanly. Upload the accompanying
archive (`dishtayantra-<version>-<stamp>.zip`) in the same message.*

---

You are picking up ongoing engineering on **DishtaYantra**, my single-node,
in-process reactive dataflow **DAG compute server** (FastAPI + uvicorn, Python
3.12). I (Ashutosh Sinha, ajsinha@gmail.com) own it. The full project is in the
attached archive; extract it and work from there. **Current version: 5.49.0.**

## What it is
Nodes wrap pub/sub connectors and calculators and are wired into a DAG. The
engine is **reactive with an equality gate**: a node recomputes only when an
input actually changes. `dagState` is the authoritative graph model; the
Cytoscape canvas is only a view. It is designed to be frugal: the simplicity of
a single process, scaled out by pipelining / paired-node failover rather than a
heavy cluster.

## How I work (please match this exactly)
- **Verify, don't assert.** Reproduce bugs before fixing; back claims with a test
  run or benchmark. I value honest pushback over agreeableness.
- **Additive & backward-compatible only.** No breaking changes. **No silent
  config defaults. No swallowed exceptions** — *except* deliberately defensive
  recording/audit paths that must never raise into callers (e.g. the flow
  recorder and `core/audit_log.py`).
- **Terse is fine.** "go ahead" = approval to proceed.
- **Code files stay under 500 lines** (HTML templates and docs are exempt).
- **Shell is sh/dash only** — no bash arrays, substring ops, or process
  substitution. **`pip` always with `--break-system-packages`.**
- **Playwright tests use port 8080** (Chromium rejects 5061 as unsafe).
- **Release ritual (six sync points)** on every version bump:
  `core/version.py` (VERSION + BUILD_DATE + the most-recent-release docstring),
  `config/application.properties` (app.version), `config/application.yaml`
  (version), `README.md` (badge + "Current version"),
  `web/templates/help/configuration.html` (example value), and `about.html`
  (dynamic `{{ app_version }}` — no manual edit). Also prepend a
  `docs/CHANGELOG.md` entry.
- **Output deliverables to `/mnt/user-data/outputs/`.** Archives extract to a
  top-level `dishtayantra/` folder and are cleaned to honor `.gitignore`
  (no `__pycache__`, `.pyc`, `data/`, `logs/`, `.idea/`, `*.db`, `*_lmdb/`),
  while **keeping `.gitignore` itself**. Zip with `-x "*/.git/*"`.
- **Sessions end with a handover package**: clean archive + `HANDOVER.md` +
  `NEW_SESSION_PROMPT.md`.

## Orient yourself first
```
unzip dishtayantra-*.zip && cd dishtayantra
python3 -m pytest tests/ -q          # baseline: 303 passed + 1 skipped (no optional deps) / 304 with pypaimon (293 + 11 wasm-only)
python3 run_server.py                 # open /flow for the Time-Travel console
```
Read `HANDOVER.md` (state snapshot), `docs/design/flow-time-travel.md`,
`docs/ARCHITECTURE.md`, and `docs/ROADMAP.md` before proposing changes.

## Most recent feature (v5.33.0): batched retention purge + flow-store maintenance
Flow retention purge now deletes in bounded batches (5000/txn) so a big sweep
(1000 ev/s -> ~1.8M rows/30min) never holds one long write lock that starves the
recorder drain thread (SqliteFlowStore + FlowDAO). NEW optional config-gated
maintenance (flow_recorder.maintenance none|incremental|full, daily at
maintenance_hour) - only the dedicated sqlite store reclaims, never the shared
app DB. bench_flow_queries.py --cycles proves the file PLATEAUS at steady state
(freed pages reused) so no periodic VACUUM is needed to bound size. Tests:
test_flow_retention_purge. distinct_dags now cached (30s TTL) in v5.33.1 — bounds the full-table aggregate at scale.

## Earlier feature (v5.32.0): fleet live push (SSE) + switcher health dots
Fleet view is now LIVE: NEW `GET /api/fleet/stream` (SSE) pushes overviews every
~5s (?once=1 = single event); /fleet uses EventSource and updates tiles in place
(down/up appears without refresh), falling back to polling. The navbar switcher
shows live reachability dots per trusted server (green/amber/red), probed lazily
on dropdown open. This is the Phase 5 SSE transport, scoped to the fleet (flow
console still polls). Tests: test_fleet_sse_stream_once.

## Earlier feature (v5.31.0): fleet health split + flow query benchmark
Fleet/registry health now splits **reachable** (cheap unauth GET /health/live,
RestServiceClient.live()) from **manageable** (authenticated info()); /fleet tiles
show reachable / "up · not manageable" / unreachable, so a down peer never blanks
the page and up-but-unmanageable is distinct. NEW perftest/bench_flow_queries.py
times the real FlowDAO query paths at realistic volume against an isolated temp DB
(answers "will SQLite scale?" with numbers; 1M/24h/20dags: 5-min ~3ms, 1h ~28ms).
Tests: test_down_server_shows_unreachable.

## Earlier feature (v5.30.1/2): two-node dev setup + DY_CONFIG_FILE (ports 18080/18090)
NEW `twonode/` kit runs two instances locally to exercise the multi-instance
features end to end (setup.sh, generate_configs.py, run-node-a/b.sh, stop.sh,
bootstrap.py, README). Config layer honours `DY_CONFIG_FILE` (PropertiesConfigurator
singleton + run_server) so a per-instance config wins regardless of import order
— fixing a bug where the singleton's first initialiser shadowed the per-node
load. Verified live: A:18080 manages B:18090 via switcher/fleet/proxy; host-scoped
admin refused to remote. Run it: `./twonode/setup.sh` then run-node-a/b in two
terminals, then `python3 twonode/bootstrap.py`, open http://127.0.0.1:18080.
Tests: `tests/test_config_override.py`.

## Earlier feature (v5.30.0): service-plane Phases 3-5 (breadth, fleet, hardening)
Completes "one UI managing many instances" through Phase 4. **Phase 3 (gateway):**
`routes/service_proxy_routes.py` proxies a strict whitelist of broad surfaces
(flow, metrics/health, worker/egress status, designer deploy) to the active
trusted server with Bearer + on-behalf; a `fetch` shim in base.html re-routes
whitelisted calls only when a remote target is active (host-scoped admin + auth
surfaces excluded). **Phase 4 (fleet):** read-only `/fleet` + `/api/fleet/overview`
with per-plane isolation. **Phase 5 (partial):** pooled `requests.Session` per
server + never-raising `registry.health()`; rotation already in v5.29.0; SSE
deferred (decision E); host-scoped-admin re-targeting left OPEN. Docs refreshed
(README, ARCHITECTURE, `docs/MULTI_INSTANCE.md`, help/multi_instance). Tests
`tests/test_service_plane_breadth.py`. Phases + status:
`docs/design/service-plane-roadmap.md`. **Next:** decide host-admin re-target;
optionally Phase 5 SSE transport.

## Earlier feature (v5.29.0): service-plane Phase 2 (manage remote instances)
One UI can now manage multiple DishtaYantra instances. NEW `RestServiceClient`
(remote flavour of the seam: HTTPS + Bearer dyk_, timeouts, typed errors,
`X-DY-On-Behalf-Of` audit header) + a DB-backed **trusted-server registry**
(admin adds URL + API key at `/admin/trusted-servers`; keys encrypted at rest
via Fernet from `DY_SECRET_KEY`, masked everywhere, never sent to the browser;
add-time probe of `/api/service/info` validates auth + records version) + a
**per-session switch** (`/service/select`, nav switcher + active-target pill).
`get_service_client(request)` resolves the session target → Local or a remote
RestServiceClient, so the DAG-management surface re-targets. Browser only talks
to its own UI plane (server-side proxy). Config: `service.instance_name`,
`service.secret_env`. Decisions A–E recorded in `docs/design/service-plane-split.md` §10.
**Next: Phase 3** (convert remaining re-targetable surfaces — dashboard list,
designer deploy, flow console, metrics — onto the seam). See
`docs/design/service-plane-roadmap.md`.

## Earlier feature (v5.28.0): service-plane seam + JSON contract
Phases 0+1 of the UI-plane / service-plane split (one UI managing many DAG
servers). NEW `core/service/` package: a `ServiceClient` interface, a
`LocalServiceClient` wrapping this instance's DAG server (today's behaviour,
the contract reference), and `get_service_client(request)` (always local for
now). DAG-lifecycle HTML handlers route the operation through the seam;
presentation unchanged. NEW JSON management API `/api/service/*` (info, dags,
status, lifecycle) returning `OpResult`-shaped JSON, sharing the same operation
path. Contract single-sourced in `core/service/contract.py`; drift-guarded by
`tests/test_service_contract.py`. Design: `docs/design/service-plane-split.md`;
phases + status: `docs/design/service-plane-roadmap.md`. **Next: Phase 2**
(RestServiceClient + trusted-server registry + switch) — blocked on the
service-plane open decisions.

## Earlier feature (v5.27.0): API-driven Designer palette
The DAG Designer palette (`web/templates/dag/designer.html`) now renders
dynamically from `GET /api/dag-designer/components` instead of hand-maintained
HTML. The catalogue lives in `core/dag/designer_catalogue.py`
(`build_component_catalogue`) as the single source of truth shared by the API
and the palette. This resolved the old parallel-structure debt and fixed a
drift bug it had already caused (seven backend components were undraggable). A
frontend `PALETTE_PRESENTATION` map styles each type, with a category-default
fallback so new components always appear. Guard: `tests/test_designer_catalogue.py`.

## Earlier feature (v5.26.0): Flow Time-Travel
Opt-in recording of the DAG change-log so I can go back over the last 24h, pick
a time window, watch data flow animate in the DAG, and download it (API / UI /
CLI). **Off by default** (`flow_recorder.enabled=false`) and disable-able at
runtime; **must never degrade engine throughput**.

Key pieces (all in the archive):
- `core/flow_recorder.py` — hot path. First line when off is `if not
  self._enabled: return` (true no-op, ~96 ns/fire, native throughput restored).
  When on: cheap snapshot + non-blocking enqueue to a bounded queue; a
  background thread drains and serializes off the hot path; overflow is shed as
  **counted drops** and never blocks or raises into compute.
- `core/flow_store.py` — pluggable store via `make_flow_store(kind)` with **no
  silent fallback**: `sqlite` (default), `dao`, `noop`, and `paimon`
  (**intentional stub — raises `NotImplementedError`**, documented optional
  backend).
- `core/flow_retention.py` — own 24h sweep daemon (mirrors
  `core/audit_retention.py`), not assumed store auto-expiry.
- `routes/flow_routes.py` — `/api/flow` (status, enable/disable [admin], dags,
  range query, state-at, streaming export jsonl|csv) + the `/flow` page.
- `web/templates/dag/flow_time_travel.html` — in-app console (fetches the API,
  vendored Cytoscape). `..._standalone.html` — self-contained demo with
  synthetic data.
- `tools/dyflow.py` — CLI (mirrors `dyadmin`): status/dags/events/state-at/
  **download** (streamed to file); human-friendly `--last 24h`/ISO/epoch windows.
- `tests/test_flow_recorder.py` — 12 tests; `perftest/bench_flow_recorder.py` —
  throughput proof.
- Engine hook: `core/dag/graph_elements.py` calls
  `FLOW_RECORDER.record_node_fire(self)` right after the equality gate fires.
- Model/DAO: `FlowEvent` in `core/db/models.py`, `FlowDAO` in `core/db/dao.py`,
  `flow_events` table in both `config/schema/schema_*.sql`.
- Config: `flow_recorder.*` in `application.properties` / `application.yaml`.

## Open items (your input needed — don't guess)

**Service-plane (Phases 0-4 shipped; see `docs/design/service-plane-roadmap.md`):**
- **Host-scoped admin re-targeting (Phase 5):** should worker-pool / language
  runtimes / maintenance ever act on a remote plane? Currently local-only. Pending.
- SSE/websocket live transport: deferred (decision E = polling).

_The A-E decisions below were settled for Phase 2 and are recorded in the design doc §10:_
- A. Trusted-server API key at rest: env-var indirection (default) / chmod-600
  file / encrypted-in-DB.
- B. Active-target scope: per-session (recommended) / global-per-instance.
- C. Remote role granularity: one key per server / split read-only + admin keys.
- D. On-behalf-of audit header: include (recommended) / omit.
- E. Live-data transport: confirm v1 = proxied polling (SSE/websocket → Phase 5).

**Flow time-travel (carried over):**
1. **Defensive store fallback decision.** If `flow_recorder.store` is misconfigured,
   startup currently logs loudly and **degrades to inert `NoopFlowStore`** rather
   than hard-failing. This bends "fail loudly" for an optional off-by-default
   feature (matches the audit carve-out). Decide: keep degrade-to-inert, or make
   a bad store value **hard-fail boot**.
2. **`paimon` backend is a stub.** If wanted, implement `PaimonFlowStore` against
   pure-Python **PyPaimon ≥ 1.3** (no JVM/Flink/Spark): append table,
   hourly-partitioned, retention driven by our own sweep daemon (not Paimon
   auto-expiry — snapshot expiry doesn't delete append-table records; only
   partition expiration does, and there is no row-level TTL). SQLite stays the
   frugal default.
3. **`_repro_pool.py`** at the project root is a hand-written scratch repro (not
   gitignored). Remove if you don't want it shipped.

## Known deferred debt
- **(RESOLVED in v5.27.0)** The Designer's connector-palette HTML and the route's
  `get_available_components` list were parallel hardcoded structures that drifted;
  the palette is now API-driven from a single source
  (`core/dag/designer_catalogue.py`). No remaining parallel-structure debt of this
  kind is known.

Start by confirming the baseline (run the tests), then tell me what you'd tackle.
