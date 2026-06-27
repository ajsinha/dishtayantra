# Multi-Instance Management Guide (Service-Plane Split)

DishtaYantra can manage **several instances from one UI**. This guide covers
setup, the trust model, switching, the fleet view, and the security model.

> This is *control-plane* separation, not clustering. Each instance still runs
> as an independent single-node compute server; one instance simply acts as a
> **UI plane** that can view and control the others (**service planes**) over
> their REST management API.

## Concepts

- **UI plane** — the instance you log into. Your browser talks only to this
  instance; it proxies to whichever server you've selected.
- **Service plane** — any instance being managed (including the UI plane's own
  local server). The local server is always present and selectable.
- **Trusted server** — a remote instance registered (by an admin) on the UI
  plane with its URL and an API key.
- **Active target** — the server your session is currently pointed at
  (per-session: your selection doesn't affect other users).

## Setup

1. **Pick a UI-plane instance** and set a stable encryption secret (used to
   encrypt trusted-server keys at rest):

   ```sh
   export DY_SECRET_KEY="a-long-stable-passphrase"   # keep it stable & secret
   python3 run_server.py
   ```

   The env var name is configurable via `service.secret_env`; the displayed
   instance name via `service.instance_name`.

2. **Mint an API key on each remote instance** (Admin → API Keys). Choose the
   role deliberately:
   - **admin** key → full lifecycle control of that server.
   - **user** key → read-only (inventory, status, flow, metrics).

3. **Register each remote** on the UI plane at **Admin → Manage servers**
   (`/admin/trusted-servers`): enter a stable id, display name, URL, the API
   key, and the role. On save, the UI plane probes the remote's
   `/api/service/info` to verify reachability + auth and records its version;
   a bad URL/key/unreachable server is rejected at configure time.

## Using it

- **Switch servers** from the navbar **server switcher**. The active target is
  shown as an amber pill so you always know which server you're acting on. The
  management surface re-targets: DAG list/lifecycle, flow time-travel,
  metrics/health, worker & egress *status*, and designer **deploy** (design
  once, deploy to any server).
- **What stays local:** your login/session, your users, and your API keys
  always belong to the UI plane. Host-scoped admin — worker-pool start/stop,
  DAG migration, JVM/C++/Rust runtimes, and maintenance/drain — also stays on
  the local plane (manage those on each server's own console).
- **Fleet view** (`/fleet`): a read-only overview of the local plane and every
  trusted server — reachability, version, DAG counts — with a one-click switch.
  One unreachable server shows its own error tile rather than blanking the page.

## How it works (brief)

The UI plane resolves a `ServiceClient` per request: **local** (in-process) or
**remote** (`RestServiceClient` over HTTPS + `Bearer <dyk_…>` +
`X-DY-On-Behalf-Of`). For broad read/op pages, the UI plane proxies a strict
whitelist of API paths to the active server (`/api/proxy/{path}`); a small
`fetch` shim re-routes those calls only when a remote target is active, so
pages re-target transparently. Everything not whitelisted (host-scoped admin,
auth/user surfaces) is never proxied.

## Security model

- Trusted-server API keys are **encrypted at rest** (Fernet, key derived from
  `DY_SECRET_KEY`), **masked** in the UI and logs, and **never sent to the
  browser**. The browser cannot see a remote's URL or key — only the UI plane
  can reach remotes (useful for private-network servers).
- Cross-server actions carry `X-DY-On-Behalf-Of: <user>@<ui-plane>` so the
  remote's audit log attributes the action to the originating user. This is
  advisory metadata only — the remote still authorises purely on the API key.
- The UI plane authenticates *you* locally before it will proxy anything; it is
  never an open relay.
- **Version compatibility is the admin's responsibility.** The UI surfaces a
  remote's version and warns on mismatch but does not enforce it.

## Operational notes

- `DY_SECRET_KEY` must be stable. Rotating it invalidates stored keys, which
  must then be re-entered. Keep it in the environment, not in config files or
  version control.
- Prefer HTTPS URLs for trusted servers. `verify_tls` can be disabled per entry
  for self-signed lab setups (not recommended for production).
- The **fleet view pushes live updates** over Server-Sent Events
  (`GET /api/fleet/stream`, emitting an overview immediately then every ~5s;
  `?once=1` for a single event). The navbar server switcher shows a lazily-probed
  reachability dot per trusted server. Other proxied views still poll.
- **Health is reported in two parts**: *reachable* (a cheap unauthenticated
  `GET /health/live`) and *manageable* (an authenticated `info()` call). A tile
  can be "up · not manageable" (reachable but credentials/trust missing), which is
  distinct from unreachable — so a missing key doesn't look like an outage.

## Two-node local kit (`twonode/`)

A ready-made two-node setup ships under `twonode/` for trying the split locally:
Node A on **:18080** (UI plane) and Node B on **:18090** (service plane), each
with its own isolated SQLite database.

- `twonode/setup.sh` derives `configs/nodeA.yaml` / `nodeB.yaml` from the
  canonical `config/application.yaml` (no drift), then `run-node-a.sh` /
  `run-node-b.sh` launch them and `bootstrap.py` mints a key on B and registers it
  as trusted on A.
- Per-node configuration is selected with the **`DY_CONFIG_FILE`** environment
  variable, honoured at the configuration singleton itself (first initialiser
  wins), so each process binds its own port and database regardless of import
  order. Unset = default behaviour.

## Reference

- Design: `docs/design/service-plane-split.md`
- Phase tracker: `docs/design/service-plane-roadmap.md`
- Architecture: `docs/ARCHITECTURE.md` → *Service-Plane / UI-Plane Split*
- Two-node kit: `twonode/README.md`
- JSON management API: `GET /api/service/info`, `/api/service/dags`,
  `/api/service/dag/{id}/{start,stop,suspend,resume}`, `/api/service/status`;
  fleet: `GET /api/fleet/overview`, `GET /api/fleet/stream`.
