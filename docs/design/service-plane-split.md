# Service-Plane / UI-Plane Split via a Client Interface — Design

> Keep DishtaYantra a single deployable monolith, but introduce a server-side
> **`ServiceClient`** seam with two flavours — **local** (in-process, today's
> behaviour) and **remote** (REST to another DishtaYantra instance). An admin
> registers **trusted servers** (other instances) with a URL + API key; a user
> can then switch the UI between the local server and any trusted server, and
> the management UI re-targets accordingly. The browser only ever talks to its
> own UI plane, which proxies to the selected service plane.

Status: **implemented through Phase 4; Phase 5 partial (v5.30.0).** Phases 0-1
shipped in v5.28.0 (seam + JSON contract), Phase 2 in v5.29.0 (remote flavour,
trusted-server registry, per-session switch), Phases 3-4 in v5.30.0 (gateway
proxy breadth + fleet view), Phase 5 partial in v5.30.0 (connection pooling/
health + key rotation; SSE deferred per decision E; host-scoped admin
re-targeting left as an open decision). Live phase tracker:
`docs/design/service-plane-roadmap.md`. Target first increment was: the local
seam only (zero behaviour change). Remote flavour + trusted-server registry +
switch UI land on top of a stable seam.

---

## 1. Motivation & scope

Today every DishtaYantra instance is a monolith: one FastAPI app serving the UI
*and* hosting the `DAGComputeServer` in-process. Operating N instances means N
logins and N tabs; there is no single pane of glass and no cross-instance
management.

We want **one UI to manage many DAG servers**, without turning DishtaYantra into
a cluster. This is a **control-plane / UI-plane separation**, deliberately *not*
compute scale-out:

* The **compute/data plane stays exactly as is** — single node, single compute
  thread, equality gate, `dagState` authoritative, the Cytoscape canvas a view.
  The ROADMAP non-goal ("not a cluster-by-default system") is about *this*
  plane and is fully preserved.
* The **management surface** (list/start/stop DAGs, designer deploy, flow
  time-travel, metrics/health, status views) gains the ability to act on a
  *remote* instance instead of the local one.

This extends a principle the codebase already lives by — "`dagState` is
authoritative; the canvas is only a view" — up one level: *the service plane's
state is authoritative; the UI plane is a (possibly remote) view of it.*

### 1.1 Non-goals

* No identity propagation as a requirement (see §6 — optional audit header
  only). The trust model is **instance-to-instance**, keyed by an API key the
  admin provides.
* No automatic version negotiation or skew handling. **The admin is responsible**
  for adding only compatible-version servers to the trusted list. We *surface*
  the remote version (§7) but do not enforce it.
* No mandatory two-process topology. A solo/air-gapped user runs exactly one
  process with an empty trusted list and sees precisely today's behaviour.
* Not splitting auth: login/session/users/API-keys always belong to the plane
  you logged into. Switching never re-targets *who you are*.

---

## 2. Topology — the UI plane is a gateway

```
         browser  ──HTTPS (single origin)──▶  UI plane  (this instance)
                                                  │
                                  ServiceClient seam (server-side)
                                   ┌──────────────┴───────────────┐
                                   ▼                              ▼
                          LocalServiceClient            RestServiceClient
                          (in-process calls to          (HTTPS + Bearer dyk_…
                           this instance's                to a trusted server's
                           DAGComputeServer)               REST management API)
                                   │                              │
                              local DAG server            trusted server B
                                                          (another full instance)
```

**The browser only ever talks to its own UI plane.** When a user selects a
trusted server, the UI plane's *request handlers* route through
`RestServiceClient` and relay the result back. The browser never receives plane
B's URL or API key.

Consequences, all of them desirable:

* **Single origin** — no CORS, no browser-side secret handling.
* **Secrets stay server-side** — the trusted-server API key never leaves the UI
  plane process.
* **Reachability** — the UI plane can manage planes on a private network the
  browser cannot itself reach (UI plane is the only ingress).
* Every DishtaYantra instance is simultaneously a potential UI plane *and* a
  potential service plane; "UI plane" vs "service plane" is a runtime role, not
  a build. The same binary, the same config schema.

---

## 3. The `ServiceClient` seam

### 3.1 Where the seam goes (and why it is not just "swap `dag_server`")

Today's management routes are **HTML form handlers that mix the operation with
presentation**. For example (`routes/dag_routes.py`):

```python
def start_dag(self, request, dag_name):
    self.guards.admin_required(request)
    self.dag_server.start(dag_name)          # <- the operation
    flash(request, f'DAG {dag_name} started') # <- presentation
    return redirect_to(request, 'dashboard')  # <- presentation
```

The seam wraps **the operation**, not the whole handler. The route keeps the
presentation (flash/redirect/render); the data call goes through the client:

```python
def start_dag(self, request, dag_name):
    self.guards.admin_required(request)
    svc = get_service_client(request)          # Local or Remote
    result = svc.start_dag(dag_name)            # structured result
    flash(request, result.message, result.level)
    return redirect_to(request, 'dashboard')
```

This implies a **prerequisite the remote flavour cannot avoid**: a clean,
JSON-in/JSON-out **REST management API** on the service side that returns
*structured results* (not HTML, not flash). `LocalServiceClient` implements the
interface by calling `dag_server` in-process and shaping the same structured
result; `RestServiceClient` implements it by calling that JSON API over HTTP and
deserialising. Both satisfy the identical Python interface, so route code is
flavour-agnostic.

> This is the "the REST API becomes the product" cost called out earlier. It is
> real and front-loaded: the management surface becomes a versioned network
> contract. The upside is that once it exists, the same contract powers
> `tools/dyadmin`, `tools/dyflow`, automation, *and* cross-plane management.

### 3.2 Interface (initial surface — the DAG-management plane)

A single Python ABC, returning plain dataclasses/dicts (JSON-serialisable). Read
operations return data; mutations return a `{ok, message, level, data}` result.

```
class ServiceClient(Protocol):
    # identity / capability
    def info(self) -> ServiceInfo            # version, capabilities, name

    # DAG lifecycle + inventory
    def list_dags(self) -> list[DagSummary]
    def dag_details(self, dag_id) -> DagDetails
    def dag_status(self, dag_id) -> DagStatus
    def reload_dags(self) -> OpResult
    def start_dag(self, dag_id) -> OpResult
    def stop_dag(self, dag_id) -> OpResult
    def suspend_dag(self, dag_id) -> OpResult
    def resume_dag(self, dag_id) -> OpResult
    def create_dag(self, spec) -> OpResult
    def clone_dag(self, dag_id, new_name) -> OpResult
    def delete_dag(self, dag_id) -> OpResult
    def publish_to_subscriber(self, dag_id, subscriber, payload) -> OpResult

    # designer
    def component_catalogue(self) -> dict     # already API-driven (v5.27.0)
    def validate_dag(self, spec) -> ValidationResult
    def deploy_dag(self, spec, folder) -> OpResult

    # flow time-travel (read-only views; capture toggle is admin)
    def flow_status(self) -> dict
    def flow_dags(self) -> list
    def flow_query(self, dag_id, t0, t1, nodes, limit, after) -> list
    def flow_state_at(self, dag_id, ts) -> dict
    def flow_export(self, dag_id, t0, t1, fmt) -> Iterator[bytes]  # streamed

    # observability (read)
    def metrics_snapshot(self) -> dict
    def health(self) -> dict
    def worker_status(self) -> dict
    def egress_status(self) -> dict
```

`LocalServiceClient` is the **default and the contract reference**: it is
exactly today's in-process behaviour, so the local path can never regress and
needs no network. `RestServiceClient` mirrors each method onto an HTTP call.

### 3.3 What re-targets vs what stays local

| Surface | On switch | Rationale |
|---|---|---|
| DAG list / details / status | **re-targets** | core of cross-management |
| DAG start/stop/suspend/resume/reload | **re-targets** | the point of the feature |
| Designer validate / components / **deploy** | **re-targets** | design once, deploy to any plane |
| Flow time-travel console (read) + capture toggle | **re-targets** | view any plane's history |
| Metrics / health / worker & egress **status** | **re-targets** | observe any plane |
| Login / session / **your users** / **your API keys** | **always local** | identity belongs to the plane you logged into |
| Worker-pool **control**, JVM/C++/Rust runtimes, maintenance | **local-only (v1)** | host-scoped admin of the machine the plane runs on; manage on that plane's own console |

The interface is the *DAG-management surface*, deliberately **not** "everything".
Host-scoped admin re-targeting is a possible later phase, explicitly out of v1.

---

## 4. Trusted-server registry

A **trusted server** is another DishtaYantra instance this one is permitted to
manage. The admin configures each entry; **the admin supplies the API key** for
that remote instance at configure time (a `dyk_…` key minted on the remote with
appropriate role — admin role for lifecycle, user role for read-only).

### 4.1 Record shape

```
{
  "id":        "ord-prod-1",          # stable local handle
  "name":      "Orders — Prod 1",     # display label
  "url":       "https://10.0.3.21:5061",
  "api_key":   "<dyk_… cleartext at use; see §4.2 for storage>",
  "verify_tls": true,                 # allow self-signed opt-out per entry
  "added_by":  "admin",
  "added_at":  "2026-06-23T…"
}
```

The **local server** is always present as an implicit, unremovable entry
(`id="local"`), backed by `LocalServiceClient`. An empty trusted list ⇒ only
"local" ⇒ today's behaviour exactly.

### 4.2 Where the API key lives (OPEN — see §10)

The UI plane needs the **cleartext** remote key to send as `Authorization:
Bearer`. Candidates, frugal-first:

* **(A) env-var indirection in config** — config stores `api_key_env: DY_TRUSTED_ORD1`; the value is read from the environment at runtime. Nothing secret on disk. *(default recommendation)*
* **(B) chmod-600 credentials file** — a separate `config/trusted_servers.secret` outside the normal config, referenced by id.
* **(C) encrypted in the DB** — symmetric-encrypt with a key from env/KMS; most moving parts.

Whatever we pick, the key is **redacted everywhere** (logs, API responses, the
admin UI shows only a masked suffix + "rotate" action), and is **never** sent to
the browser.

### 4.3 Management API + UI (admin-only)

* `GET /api/trusted-servers` → list (keys masked).
* `POST /api/trusted-servers` → add `{name,url,api_key,verify_tls}`; on add, the
  UI plane immediately calls the remote `GET /api/service/info` (§7) to validate
  reachability + auth and record the reported version. Refuses to save if the
  probe fails (loud, not silent).
* `DELETE /api/trusted-servers/{id}` → remove.
* `POST /api/trusted-servers/{id}/test` → re-probe (auth + version).
* All behind `admin_required`. An **audit-log** entry on add/remove/rotate
  (mirrors `core/audit_log.py`).
* Admin UI page: a table of trusted servers with name, URL, masked key, last
  probed version, reachable✓/✗, and add/test/rotate/remove. Reuses the existing
  api-keys admin page patterns.

---

## 5. Target selection ("the switch")

* A **server switcher** in the top nav lists `local` + reachable trusted
  servers; selecting one sets the **active target**.
* **Scope: per-session** (stored in the user's session), so two users — or two
  browser sessions — can view different planes at once. This matches "the UI is
  a view"; the active target is view state, not instance state. *(OPEN — §10:
  per-session vs global-per-instance.)*
* `get_service_client(request)` resolves: read active target id from session →
  `local` returns `LocalServiceClient`; otherwise look up the trusted server and
  return a (pooled) `RestServiceClient`. Unknown/removed target falls back to
  `local` with a flash notice.
* The active target is shown persistently (e.g. a coloured pill: "Viewing:
  Orders — Prod 1 @ v5.27.0") so a user can never forget they are acting on a
  remote plane — important before a destructive op like stop/delete.
* Re-targetable pages read the client through the seam; local-only pages ignore
  the target and always use Local (and should visually indicate they are
  showing the local host).

---

## 6. Auth across the hop

* **Service-to-service** uses the existing `dyk_` API-key infrastructure and
  `AuthGuards` (which already accepts `Authorization: Bearer <key>` or
  `X-API-Key`). No new auth mechanism is invented — this is an asset already in
  hand.
* The UI plane authenticates the **user** locally (unchanged). For a remote
  call, it attaches the trusted server's configured key. The remote plane sees a
  valid key with whatever role the admin minted — that is the entire trust
  decision, and it rests with the admin.
* **Optional audit attribution (recommended):** the UI plane adds
  `X-DY-On-Behalf-Of: <user>@<ui-plane-name>` to remote calls; the remote plane
  records it in its audit log alongside the key identity. Shared-creds otherwise
  loses "who started this DAG on plane B" — and this project cares about audit
  attribution. It is advisory metadata only, never an authorization input.

---

## 7. Version surfacing (not enforcement)

* New endpoint `GET /api/service/info` (login-or-key) returns:
  `{name, version, build_date, capabilities: [...], schema_version}`.
* `RestServiceClient` calls it on connect/probe; the UI displays the remote
  version in the switcher and the trusted-server admin table.
* If the remote version differs from the local version, the UI shows a
  **non-blocking warning** ("remote is v5.21.0; this UI is v5.27.0 — verify
  compatibility"). It does **not** refuse, per the non-goal: the admin owns
  compatibility. `capabilities` lets the UI gracefully hide controls a remote is
  too old to support, instead of erroring on click.

---

## 8. Failure semantics

A remote plane can be down, slow, or mid-restart. The seam must degrade
predictably:

* **Per-call timeouts** on `RestServiceClient` (connect + read), surfaced as a
  flash/inline error, never a hung page. Defaults conservative; configurable.
* **No partial-success lies.** Single-target ops report that target's result
  plainly. (A future *fleet* view that fans an op across many planes must report
  per-plane success/failure explicitly — out of v1 scope.)
* **Reachability is first-class:** the switcher marks unreachable targets and
  prevents selecting them for mutations (or warns hard).
* `RestServiceClient` errors are typed (`ServiceUnavailable`, `ServiceAuthError`,
  `ServiceTimeout`, `ServiceProtocolError`) so routes render meaningful messages.
* Local path has **none** of these modes — another reason Local stays the
  default and the contract reference.

---

## 9. Avoiding the drift trap (lesson from v5.27.0)

The v5.27.0 palette bug was two hand-maintained structures (palette HTML vs the
route catalogue) drifting. A UI plane carrying its own hand-written notion of the
remote API is **the same trap at fleet scale.** Mitigations baked into this
design:

* The service plane already emits an **OpenAPI schema** for free (FastAPI). The
  REST management contract is single-sourced from the route definitions.
* `RestServiceClient` should be **validated against the served schema**, with a
  CI check that the client's expected operations exist in the local app's
  OpenAPI document — failing the build on drift. (Same spirit as
  `tests/test_designer_catalogue.py`: a test that makes drift impossible to ship
  silently.)
* `GET /api/service/info`'s `capabilities`/`schema_version` give a runtime
  cross-check to back up the static one.

---

## 10. Open decisions (need owner input — don't guess)

> **DECIDED (2026-06-23), ready for Phase 2:**
> 1. **Key at rest → encrypted-in-DB (C).** Trusted-server API keys are stored
>    symmetric-encrypted in the database; the encryption key comes from the
>    environment/secret, never committed. Redacted everywhere; never sent to the
>    browser.
> 2. **Active-target scope → per-session.** The selected target lives in the
>    user's session; two users can view different planes at once. Fresh session
>    defaults to local.
> 3. **Remote role granularity → admin OR user key per server.** Each trusted
>    server entry carries one key, which may be an admin key (full lifecycle) or
>    a user/read-only key; the UI gates mutating controls by the role the remote
>    reports / the key allows.
> 4. **On-behalf-of header → include.** `X-DY-On-Behalf-Of: <user>@<plane>` is
>    sent on remote calls and recorded in the remote's audit log. Advisory
>    metadata only — never an authorization input.
> 5. **Live transport → proxied polling for v1.** Existing fetch-polling is
>    proxied server-side unchanged; SSE/websocket pass-through deferred to Phase 5.

The original framing of each decision (kept for context):

1. **Trusted-server key at rest** — env-var indirection (A, default) /
   chmod-600 file (B) / encrypted-in-DB (C)? → **C chosen.** (§4.2)
2. **Active-target scope** — per-session (recommended) or global-per-instance? →
   **per-session chosen.** (§5)
3. **Remote role granularity** — one key per trusted server (simplest), or
   allow a read-only key + a separate admin key per server so "view" vs "manage"
   is enforced remotely? → **one key per server, admin or user role.** (§6)
4. **On-behalf-of header** — include it (recommended, for audit) or omit for
   strict minimalism? → **include chosen.** (§6)
5. **Live-data transport** — v1 keeps existing fetch-polling proxied
   server-side (works unchanged); SSE/websocket pass-through is a later
   optimisation. → **v1 = proxied polling confirmed.** (deferred to Phase 5)

---

## 11. Phased delivery

**Phase 0 — local seam, zero behaviour change (first increment).**
Define `ServiceClient` + `ServiceInfo`/`OpResult`/… result types; implement
`LocalServiceClient` wrapping today's `dag_server` calls; add
`get_service_client(request)` returning Local always; route the DAG-lifecycle
handlers through it. No registry, no REST flavour, no switch. **Exit:** full
suite green, behaviour identical, files < 500 lines, additive only.

**Phase 1 — REST management API (the contract).**
Add the JSON-in/JSON-out management endpoints the interface needs (those that
don't already exist), each returning structured results; add
`GET /api/service/info`. Add the OpenAPI-vs-client drift test (§9). **Exit:**
`tools/dyadmin`-style calls work against the JSON API; schema test green.

**Phase 2 — remote flavour + trusted-server registry + switch.**
Implement `RestServiceClient` (timeouts, typed errors, on-behalf-of header);
trusted-server registry + admin API/UI (key handling per §4.2 decision);
session-scoped target + nav switcher + active-target pill. Prove end-to-end on
**one** surface (DAG start/stop or the flow console) before converting the rest.

**Phase 3 — breadth + fleet view.**
Convert remaining re-targetable surfaces; add a read-only multi-plane dashboard
(per-plane status tiles) with explicit per-plane success/failure. Host-scoped
admin re-targeting considered here or deferred.

**Conventions honoured throughout:** additive & backward-compatible; no silent
config defaults (empty trusted list = local-only = today); no swallowed
exceptions except the existing defensive audit/recording paths; files < 500
lines; six-point release ritual per version bump; deliverables to
`/mnt/user-data/outputs/`.
