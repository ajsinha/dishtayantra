# Admin CLI (dyadmin) Guide

`tools/dyadmin.py` is a command-line client for a **running** DishtaYantra
server. It exposes the same admin functions as the web UI, for scripting and
automation. It authenticates as an **admin** user using either a session login
(`POST /login` -> signed-cookie session, the same as the web UI) **or** an API
key. For CLIs and automation, an API key is preferred &mdash; no password in the
environment, and it can be scoped/expired/revoked independently.

## Connection and auth

Pass connection details as flags or environment variables:

| Flag | Env | Default |
|------|-----|---------|
| `--url` | `DY_URL` | `http://localhost:5002` |
| `--api-key` | `DY_API_KEY` | (none) |
| `--username` | `DY_ADMIN_USER` | (none) |
| `--password` | `DY_ADMIN_PASSWORD` | (none) |
| `--timeout` | — | `30` (seconds) |

When `--api-key` (or `DY_API_KEY`) is set, the key is sent on every request and
no session login happens; `--username`/`--password` are not needed. The key is
sent as `Authorization: Bearer <key>` (the server also accepts an `X-API-Key`
header). A key inherits the roles of its owning user, so an admin user's key
grants admin access.

Global options: `--json` prints raw JSON responses; `--dry-run` prints the exact
HTTP request that would be sent and exits without contacting the server (handy
for review and for building runbooks).

```bash
# API-key auth (recommended for automation)
export DY_URL=http://prod-host:5002
export DY_API_KEY=dyk_...            # minted with dyapikey, see below
python3 tools/dyadmin.py monitor

# or session auth
export DY_ADMIN_USER=admin
export DY_ADMIN_PASSWORD=...         # avoid putting secrets in shell history
python3 tools/dyadmin.py monitor
```

### Minting API keys (`tools/dyapikey.py`)

API keys are minted on the **server host** with `tools/dyapikey.py`, which talks
to the database directly (this is the bootstrap path &mdash; it avoids needing a
key in order to create a key). Admins can also manage keys from the browser at
**Admin &rarr; API Keys** (`/admin/api-keys`): create (with optional expiry),
list, and revoke. Either way the clear key is shown exactly once at creation;
store it securely, as it cannot be recovered afterward.

```bash
python3 tools/dyapikey.py create admin ci-runner        # mint a key for 'admin'
python3 tools/dyapikey.py create admin nightly --expires-days 90
python3 tools/dyapikey.py list admin                    # list a user's keys
python3 tools/dyapikey.py revoke 4 --by admin           # revoke by key id
```

Run it from the project root with the same environment the server uses (e.g.
`SECRET_KEY` set). The minted key then goes into `DY_API_KEY` for `dyadmin` (or
any other client). Revoke a key any time with `dyapikey revoke <id>`; revoked or
expired keys are rejected immediately.

## Command groups

| Group | Commands | Maps to |
|-------|----------|---------|
| `monitor` | (none) | system metrics + free-threading status |
| `dag` | `list`, `start <name>`, `stop <name>`, `suspend <name>`, `resume <name>`, `reload` | DAG lifecycle |
| `maintenance` | `status`, `freeze`, `unfreeze` | drain mode / maintenance windows |
| `logging` | `set` | runtime log-level control (broadcast to workers) |
| `logs` | `tail` | recent server log entries (JSON) |
| `workers` | `status`, `assignments`, `load`, `info <id>`, `pool-start`, `pool-stop`, `restart <id>`, `migrate <dag>` | worker-pool control |
| `native` | `status`, `calculators`, `modules`, `reload-config`, `gateways`, `module-{load,unload,reload,build} <name>`, `gateway-{restart,stop,reconnect} <name>` (per runtime `jvm`/`cpp`/`rust`) | native module/gateway management |

## Examples

```bash
# observe
dyadmin monitor
dyadmin dag list
dyadmin maintenance status
dyadmin workers status

# DAG lifecycle
dyadmin dag stop perftest_trade_etl_arrow
dyadmin dag start perftest_trade_etl_arrow
dyadmin dag reload

# maintenance window: freeze everything, wait until drained, then deploy
dyadmin maintenance freeze --scope global
dyadmin maintenance status            # repeat until all_drained: True
#   ... do maintenance ...
dyadmin maintenance unfreeze --scope global

# freeze just one DAG (or specific subscribers)
dyadmin maintenance freeze --scope dag --dag orders
dyadmin maintenance freeze --scope dag --dag orders --subscribers sub_a,sub_b

# runtime logging (no restart; broadcast to workers)
dyadmin logging set --scope root --level WARNING
dyadmin logging set --scope logger --logger core.dag --level DEBUG

# workers / native runtimes
dyadmin workers restart 3
dyadmin native status jvm
dyadmin native rust module-reload my_module

# review a request without sending it
dyadmin --dry-run maintenance freeze --scope global
```

## Notes

- **Maintenance scope.** `--scope dag` requires `--dag`; `--subscribers` takes
  `ALL` (default) or a comma-separated list. `--scope global` affects every DAG.
- **Action results.** Lifecycle/maintenance/logging commands print
  `OK - action applied.` on success (the server uses post/redirect/get, so there
  is no JSON body); read-only commands print a formatted summary, or full JSON
  with `--json`.
- **DAG listing** is sourced from the drain-status endpoint (it lists every DAG
  with its freeze/drain state); there is no separate JSON dag-list endpoint.
- **Auth failures** (wrong credentials, an invalid/expired/revoked API key,
  missing admin role, or an expired session) exit with a clear message.
  Credentials and keys are best supplied via environment variables rather than
  flags so they don't land in shell history.
