# Service-Plane Split — Delivery Roadmap & Phase Tracker

Authoritative phase tracker for the UI-plane / service-plane separation. Design
rationale lives in `docs/design/service-plane-split.md`; this file tracks *what
ships when* and the status of each phase. Update the **Status** column as phases
land.

> Recap of the shape (see design doc for detail): keep the monolith; introduce a
> server-side `ServiceClient` seam with **local** (in-process) and **remote**
> (REST to a trusted instance) flavours; an admin registers trusted servers
> (URL + API key); a user switches the UI between local and trusted servers and
> the management surface re-targets. The browser only ever talks to its own UI
> plane, which proxies to the selected service plane. The compute/data plane is
> unchanged — this is control-plane separation, not compute scale-out.

## Phases at a glance

| Phase | Theme | Headline outcome | Risk | Status |
|------:|-------|------------------|------|--------|
| **0** | Local seam | `ServiceClient` interface + `LocalServiceClient`; DAG-lifecycle handlers routed through it. Zero behaviour change. | Low | **DONE (v5.28.0)** |
| **1** | JSON contract | JSON management API returning structured results + `GET /api/service/info`; OpenAPI-vs-contract drift guard. | Medium | **DONE (v5.28.0)** |
| **2** | Remote + registry + switch | `RestServiceClient`; trusted-server registry (admin adds URL + key); per-session target + nav switcher; proven end-to-end on one surface. | Med-High | **DONE (v5.29.0)** |
| **3** | Breadth | Convert all remaining re-targetable surfaces (designer deploy, full flow console, metrics/health, worker/egress status) onto the seam. | Low-Med | **DONE (v5.30.0)** |
| **4** | Fleet view | Read-only multi-plane dashboard: per-plane status tiles, aggregated inventory, explicit per-plane success/failure. | Medium | **DONE (v5.30.0)** |
| **5** | Hardening | SSE/websocket pass-through for live views, connection pooling/health, key rotation, decide whether host-scoped admin ever re-targets. | Low | **PARTIAL (v5.30.0–v5.32.0)** — pooling/health + rotation done; SSE live transport done for the fleet view (v5.32.0); SSE for the flow console still future; host-admin re-target = open decision |

Natural release boundaries: **0+1** are the de-risking block (no user-visible
cross-management yet — just the seam + the contract), landed together. **2** is
the first user-visible cross-management feature. **3–5** broaden and harden.

---

## Phase 0 — Local seam (DONE, v5.28.0)
**Goal:** introduce the abstraction with no externally visible change.
- `core/service/` package: `ServiceClient` interface, result types
  (`ServiceInfo`, `OpResult`), `LocalServiceClient` wrapping `dag_server`, and
  `get_service_client(request)` resolving via `app.state` (always Local in P0).
- DAG-lifecycle handlers (`start/stop/suspend/resume/reload`) in
  `routes/dag_routes.py` route the *operation* through the seam; presentation
  (flash/redirect) stays in the route.
- **Exit:** full suite green, behaviour identical, additive only, files < 500.

## Phase 1 — JSON management contract (DONE, v5.28.0)
**Goal:** the management surface becomes a structured, versioned network
contract — the prerequisite for the remote flavour.
- `routes/service_routes.py`: JSON-in/JSON-out endpoints under `/api/service/*`
  (info, dags, dag details, status, reload, start/stop/suspend/resume), each
  returning `OpResult`-shaped JSON, sharing the *same* `LocalServiceClient`
  operation path as the HTML handlers (single source of truth).
- `GET /api/service/info` → name, version, build_date, capabilities,
  schema_version.
- `core/service/contract.py`: single-sourced `SERVICE_OPERATIONS`,
  `CAPABILITIES`, `SCHEMA_VERSION`.
- Drift guard `tests/test_service_contract.py`: every promised operation in
  `SERVICE_OPERATIONS` must appear in the app's served OpenAPI schema (same
  spirit as the v5.27.0 palette parity test — makes contract drift impossible to
  ship silently).
- **Exit:** JSON API callable standalone; drift test green.

## Phase 2 — Remote flavour + trusted-server registry + switch (DONE, v5.29.0)
**Goal:** first real cross-plane management.
- `RestServiceClient` mirroring the interface over HTTPS + `Bearer dyk_…`, with
  per-call timeouts, typed errors (`ServiceUnavailable/AuthError/Timeout/
  ProtocolError`) and the optional `X-DY-On-Behalf-Of` audit header.
- Trusted-server registry + admin API/UI: admin adds `{name,url,api_key,
  verify_tls}`; add-time probe of remote `/api/service/info` validates auth +
  records version; keys redacted everywhere, never sent to the browser. Key-at-
  rest per the §10 decision (default: env-var indirection).
- Per-session active target + nav switcher + active-target pill;
  `get_service_client` resolves session target → Local or pooled Remote.
- Prove end-to-end on **one** surface (DAG start/stop or flow console) before
  breadth.
- **Exit:** register a second instance and start/stop its DAGs from the first.

## Phase 3 — Breadth (DONE, v5.30.0)
Convert remaining re-targetable surfaces onto the seam (designer deploy, full
flow time-travel console, metrics/health, worker/egress status). Mostly
mechanical on a proven seam. **Exit:** every surface in the design-doc §3.3
"re-targets" table works against a remote.

## Phase 4 — Fleet view (DONE, v5.30.0)
Read-only multi-plane dashboard: per-plane status tiles, aggregated DAG
inventory, reachability at a glance, explicit per-plane success/failure (no
partial-success lies). **Exit:** one screen summarises all trusted planes.

## Phase 5 — Hardening & optimisation (PARTIAL, v5.30.0–v5.32.0)
SSE/websocket pass-through to replace proxied polling where it matters;
`RestServiceClient` connection pooling + health-checking; trusted-key rotation
flow; decision on whether host-scoped admin (worker pool, runtimes, maintenance)
ever becomes re-targetable (currently local-only). **Exit:** live views are
push-based; ops runbook for key rotation.

---

## Open decisions blocking later phases (from design-doc §10)
These do **not** block Phase 0/1. **All five DECIDED 2026-06-23 — Phase 2 is
unblocked:**
1. Trusted-server key at rest: **encrypted-in-DB.**
2. Active-target scope: **per-session.**
3. Remote role granularity: **one key per server, admin OR user role.**
4. On-behalf-of audit header: **include** (advisory only, never authz).
5. Live-data transport: **proxied polling for v1** (SSE/websocket → Phase 5).

---

## Open decision carried by Phase 5 (needs owner input)
**Should host-scoped admin ever become re-targetable?** Worker-pool start/stop,
DAG migration, JVM/C++/Rust runtime control, and maintenance/drain are currently
**local-only**: they act on the machine the UI plane runs on and are excluded
from both the typed contract and the gateway proxy whitelist. Making them
re-targetable would let an operator drain or restart a *remote* plane's workers
from the fleet UI — powerful, but it widens the blast radius of a mis-selected
target and needs per-action confirmation. **Decision pending; default remains
local-only.** SSE/websocket live-transport (also Phase 5) stays deferred per
decision E (v1 = proxied polling).
