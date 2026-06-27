# Two-Node Dev Setup

Run **two DishtaYantra instances locally** to exercise the multi-instance
(service-plane) features end to end: trusted servers, the per-session switch,
the gateway proxy, and the fleet view.

| Node | Role | URL | Instance name |
|------|------|-----|---------------|
| A | UI plane (you log in here, manages others) | http://127.0.0.1:18080 | UI-Plane-A |
| B | service plane (managed by A) | http://127.0.0.1:18090 | Service-Plane-B |

Both run from this one checkout and the same code; they differ only in config
(port, instance name, and isolated SQLite/flow-store paths). See
`docs/MULTI_INSTANCE.md` for the feature itself.

## How it works

`run_server.py` and the config layer honour the `DY_CONFIG_FILE` environment
variable: set it and the instance loads that config file instead of
auto-discovering `config/application.yaml`. The per-node configs in
`twonode/configs/` are **generated from the canonical `config/application.yaml`**
(so they never drift) with only the values that must differ between co-located
instances changed:

- `server.port` — 18080 (A) / 18090 (B)
- `service.instance_name` — UI-Plane-A / Service-Plane-B
- `db.sqlite.path` and `flow_recorder.store_path` — separate `data/twonode/nodeA`
  and `data/twonode/nodeB` directories so the two processes never share a
  database.

> Note: the two nodes still share `storage.root: ./` and therefore the same
> `config/dags` definitions — fine for testing lifecycle/flow/metrics
> re-targeting. If you want a deploy to B to be visibly separate, point B at its
> own DAG folder in `twonode/configs/nodeB.yaml`.

## Quick start

```sh
# 1. one-time setup: generate node configs + data dirs
./twonode/setup.sh

# 2. start each node in its own terminal
./twonode/run-node-a.sh     # UI plane  -> http://127.0.0.1:18080
./twonode/run-node-b.sh     # service plane -> http://127.0.0.1:18090

# 3. wire B into A (mint a key on B, register B as a trusted server on A)
python3 twonode/bootstrap.py

# 4. open the UI plane
#    http://127.0.0.1:18080  -> log in (admin / admin123)
#    - navbar server switcher -> "Service-Plane-B"
#    - or open /fleet for the multi-plane overview

# stop both nodes
./twonode/stop.sh
```

`run-node-a.sh` sets a demo `DY_SECRET_KEY` (used to encrypt trusted-server keys
at rest). Override it in your environment for anything beyond local testing.

## Doing it manually (instead of bootstrap.py)

1. On B (`:18090`), log in and go to **Admin → API Keys**; create a key for
   `admin` (admin role = full control, or user role = read-only). Copy it.
2. On A (`:18080`), go to **Admin → Manage servers**
   (`/admin/trusted-servers`); add: id `node-b`, name `Service-Plane-B`, URL
   `http://127.0.0.1:18090`, the key, role `admin`, untick *verify TLS* (plain
   HTTP for local). Save — A probes B and records its version.
3. Use the switcher or `/fleet`.

## What you can verify

- **Switch** to Service-Plane-B and watch DAG lists, flow time-travel,
  metrics/health, worker/egress status re-target to B.
- **Fleet** (`/fleet`) shows both planes, reachability, version, DAG counts.
- The gateway proxies B's data APIs through A
  (`/api/proxy/api/service/info`, `/api/proxy/api/flow/status`, …), while
  host-scoped admin (`/api/proxy/api/workers/pool/start`) is refused with 403.

## Files

- `setup.sh` — generate configs + data dirs.
- `generate_configs.py` — derive `configs/nodeA.yaml` and `configs/nodeB.yaml`
  from canonical config.
- `run-node-a.sh`, `run-node-b.sh` — start each node.
- `stop.sh` — stop both.
- `bootstrap.py` — mint a key on B and register B on A automatically.
