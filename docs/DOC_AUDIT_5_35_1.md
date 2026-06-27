# Documentation Audit — DishtaYantra v5.35.1

**Scope:** all Markdown docs (`README.md`, `QUICKSTART.md`, `docs/**`, `twonode/README.md`),
all 44 in-app help templates (`web/templates/help/*.html`), `about.html`, `comparison.html`,
the research paper, and the requirement/deck binaries (catalogued, not parsed).
**Method:** cross-referenced doc claims against the live code/config (routes, `core/flow_store*`,
`config/application.yaml`, navbar `base.html`) rather than reading prose in isolation. Evidence is
cited as `file:line`.

---

## Headline assessment

The documentation corpus is **broad, well-structured, and accurate for the mature subsystems**
(connectors, calculators, DAG/subgraph model, worker pool, async egress, themes, user management).
There are ~60 Markdown docs, 8 numbered tutorials, ~20 topical user guides, design docs, and 44 help
pages — genuinely strong coverage.

The weaknesses cluster, predictably, around the **newest subsystems shipped in the last several
releases** (flow Time-Travel + its pluggable stores, the separate-DB/Postgres/provenance work, the
Paimon/Aerospike backends, the `dao` prohibition, the navbar reorg, and the Live-Logs access change).
For these, the *only* fully current source is `docs/CHANGELOG.md`. The user-facing references — the
Configuration Reference Guide, the in-app config help, QUICKSTART's navigation map, and the
flow-time-travel design doc — have **not** been updated and are in places **actively inaccurate**.

Net: **Accuracy = good except for a few high-severity stale spots. Completeness = a real gap for the
flow recorder and its backends. Robustness = the honest caveats exist but only in the changelog, not
where a user configuring the feature would see them.**

---

## 1. Accuracy

### HIGH — `docs/design/flow-time-travel.md` is materially out of date
The design doc still describes the pre-v5.34 world:
- Backend diagram lists `sqlite / dao / noop / paimon` (`:42`) and the storage-backend table presents
  `dao → DaoFlowStore` as a valid option that "shares the configured SQLAlchemy database" (`:97`).
  **`dao` is now forbidden** (the factory raises `ValueError`, `DaoFlowStore` was deleted), and the
  webapp additionally refuses any flow DB that resolves to the app DB.
- The config example reads `flow_recorder.store=sqlite # sqlite | dao | noop | paimon` (`:202`) —
  missing `postgres` and `aerospike`, and still advertising `dao`.
- It references `core/db/dao.py (+FlowDAO)` and `config/schema/schema_sqlite.sql` (`:224`); the flow
  store now has its own modules (`flow_store_sql.py`, `flow_store_paimon.py`,
  `flow_store_aerospike.py`) and dedicated schema files (`config/schema/flow_events_{sqlite,postgres}.sql`).
- No mention of **provenance** (`instance/host/port`), the **separate-database** guarantee, the
  **`SqlFlowStore`** (Postgres) path, or the Paimon local-FS retention caveat.

### HIGH — `QUICKSTART.md` navigation map is stale and has wrong paths
- `:169` lists "**Admin** > System / Live Logs | `/admin/system_logs`, `/admin/live_logs`". Three
  problems: (1) the paths are wrong — real routes are `/admin/logs` and `/admin/logs/live`
  (`admin_log_routes.py:74,80`); (2) Live Logs is no longer under **Admin**, it's under **Manage**;
  (3) it is no longer admin-only — it is now `login_required` (any authenticated user).
- `:555` repeats the wrong `/admin/live_logs` path.
- The whole table predates the navbar reorg: there is **no "Manage" group**, **no Flow Time-Travel /
  `/flow` entry**, and **no "Compare under About"**. It documents a navbar that no longer exists.

### LOW — minor stale framing
- `about.html:579` lists "System and live log views" in a way that still implies an admin-only feature
  (cosmetic; the page version badge itself is correctly dynamic — `about.html:139` uses `{{ app_version }}`).
- `docs/ROADMAP.md:75,209` lists "Iceberg/Paimon lakehouse sink" as a future item. That roadmap item
  (a data-plane sink) is distinct from the new Paimon *flow-store* backend, but a reader may conflate
  them; a one-line note would disambiguate.
- Historical version strings in CHANGELOG and design docs (`v5.27.0`, `v5.30.0`, …) are legitimate
  historical references, **not** drift. `README.md:480` correctly shows current = **5.35.1**.

---

## 2. Completeness

### HIGH — the flow recorder is absent from the Configuration Reference Guide
`docs/userguides/Configuration Reference Guide.md` (338 lines) documents `app, server, logging,
backpressure, egress.async, storage, db, audit, ha, holidays/schedule, external/user/kafka/lmdb` —
but has **zero** coverage of `flow_recorder` (enabled, store, store_path, db_url, retention,
maintenance, warehouse, aerospike_*). The primary configuration reference omits an entire shipped,
configurable subsystem.

### HIGH — no user-facing documentation for Flow Time-Travel or its backends
- There is **no help page** for Flow Time-Travel (`web/templates/help/` has `time_windows.html` but
  nothing for the flow recorder), and `help/index.html` does not link one — even though the feature is
  now reachable from **Manage > Flow Time Travel** in the navbar.
- `web/templates/help/configuration.html` has **no** `flow_recorder` content.
- **Provenance**, **`db_url`**, the **Postgres flow store**, and the **`instance/host/port`** model are
  documented **only** in `docs/CHANGELOG.md`. Paimon-as-flow-store appears only in CHANGELOG and the
  (stale) design doc. Aerospike-as-flow-store appears only in CHANGELOG.

### MEDIUM — access-control and navbar changes not reflected in user docs
The Live-Logs access change (admin → any authenticated user) and the navbar grouping (Manage / About
dropdowns) are correct in code and CHANGELOG but not described in QUICKSTART or any UI/help page a user
would consult.

### MEDIUM — the two-node / fleet / service-plane material is thin in user docs
`MULTI_INSTANCE.md`, `help/multi_instance.html`, and `twonode/README.md` exist, but the fleet SSE,
health split (reachable vs manageable), and the `DY_CONFIG_FILE` per-node override are mostly captured
in design docs/changelog rather than a user guide.

---

## 3. Robustness

### MEDIUM — honest caveats live only in the changelog
The project's verify-don't-assert discipline produced accurate caveats, but they are **not** where a
user enabling these backends will read them:
- **Paimon retention** is a no-op on a local-filesystem catalog (PyPaimon can't drop partitions without
  a REST catalog). A user setting `flow_recorder.store=paimon` from a (future) config doc would not
  learn this and could assume retention is enforced.
- **Aerospike** is experimental/unverified against a live cluster.
- **Postgres** flow store is exercised via SQLite in tests, not yet against a live PostgreSQL.
These belong in the Configuration Reference and any flow help page, not just the changelog.

### MEDIUM — security note for Live Logs is undocumented for users
The live stream tails `dagserver.log` / `application.log` / `error.log`, which can contain stack traces
and request detail. Now that **any authenticated user** can view it, the widened exposure should be
noted in the admin/operations docs so operators make an informed choice.

### LOW — doc set has no single "what changed for operators" page
CHANGELOG is developer-oriented. There is no concise operator-facing "upgrade notes" surface that would
flag breaking changes (e.g. `store=dao` removal) for someone upgrading a deployment.

---

## What is solid (credit where due)
- Connector guides (Kafka, RabbitMQ, Redis, REST, TIBCO, IBM MQ, LMDB, cloud pub/sub), calculator docs
  (Rust/pybind11/py4j/WASM/Arrow), subgraph and worker-pool guides are detailed and consistent.
- Tutorials 01–08 plus topical tutorials form a coherent learning path.
- `about.html` version badge is dynamic; `README.md` "current version" is correct.
- CHANGELOG is thorough, current, and unusually honest about caveats.

---

## Prioritized recommendations
1. **(High)** Rewrite `docs/design/flow-time-travel.md` to the v5.35.1 reality: backends
   `sqlite | postgres | noop | paimon | aerospike`; `dao` forbidden; provenance columns; separate-DB
   guarantee; Paimon/Aerospike/Postgres caveats.
2. **(High)** Add a `flow_recorder` section to the **Configuration Reference Guide** (all keys + the
   three caveats) and a short **Flow Time-Travel help page** linked from `help/index.html`.
3. **(High)** Fix `QUICKSTART.md`: correct live-log paths (`/admin/logs`, `/admin/logs/live`), regroup
   the navbar table (Manage: Egress/Flow/Cache/Live Logs; About: About/Compare), add the `/flow` entry,
   and note Live Logs is now available to all authenticated users.
3. **(Medium)** Add the Live-Logs exposure note to the Administration & Maintenance guide.
4. **(Low)** One-line disambiguation in ROADMAP between the Paimon lakehouse *sink* and the Paimon flow
   *store*; tidy the `about.html` live-log phrasing.

No code defects were found in this audit — these are documentation issues only. None of the inaccuracies
affect runtime behaviour; they affect what a reader is told about it.

---

## Resolution — fixed in v5.35.2

All identified items were addressed in a documentation pass (the only code change
is a new help page + route `/help/flow-time-travel`):

- **HIGH** — `flow-time-travel.md` rewritten (backends, `dao` forbidden, provenance,
  separate-DB, caveats); `QUICKSTART.md` navigation corrected (real paths +
  Manage/Admin/About grouping + Live-Logs access); `flow_recorder` section added to
  the Configuration Reference Guide; new Flow Time-Travel help page + index card.
- **MEDIUM** — Administration guide navbar diagram corrected and a Live-Logs
  access/log-exposure note added; `MULTI_INSTANCE.md` corrected (fleet SSE shipped)
  and expanded (two-node kit, `DY_CONFIG_FILE`, reachable-vs-manageable health).
- **LOW** — `ROADMAP.md` Paimon sink-vs-flow-store disambiguation; `about.html`
  live-log wording. Operator-facing breaking-change note for `store=dao` now lives
  in the changelog, design doc, config guide, and help page.

Backend caveats (Postgres tested-via-sqlite, Paimon local-FS retention no-op,
Aerospike experimental) now appear in user-facing docs, not only the changelog.
