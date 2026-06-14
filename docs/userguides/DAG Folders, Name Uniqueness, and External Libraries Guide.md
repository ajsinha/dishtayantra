# DAG Folders, Name Uniqueness, and External Libraries

This guide covers three related operational topics introduced/clarified in
v3.1.0:

1. Organising DAG JSON files across **multiple folders**.
2. The **globally-unique DAG name** policy and what happens on a collision.
3. Loading **external calculator/transformer libraries** from custom paths.

---

## 1. Multiple DAG folders

By default the server loads DAG JSON files from a single folder,
`config/dags`. From v3.1.0 you can keep logically-grouped DAGs in several
folders.

### Configuration

In `config/application.yaml`:

```yaml
storage:
  dags:
    prefix: config/dags                       # always scanned (the default)
    prefixes: config/risk_dags,config/trade_dags   # additional folders
```

Or, equivalently, in `config/application.properties`:

```properties
storage.dags.prefix=config/dags
storage.dags.prefixes=config/risk_dags,config/trade_dags
```

Rules:

- **`config/dags` is always scanned**, whether or not it appears in
  `prefixes` — you never lose the default location.
- `prefixes` is a comma-separated list of additional folders.
- Each folder is scanned for its **direct `.json` children only**. Sub-folders
  are **never** scanned (this is deliberate — e.g. `config/dags/examples/`
  holds samples that must not auto-load). If you want another group of DAGs,
  add another sibling folder to `prefixes`, do not nest.
- A DAG's source file is remembered by its **full path** (e.g.
  `config/risk_dags/foo.json`), so two folders may contain files with the same
  *filename*, and deleting a file then reloading removes exactly the right DAG.

### Reload behaviour

The dashboard **Reload DAGs** button re-scans **all** configured folders. A
DAG whose source file was deleted from any folder is stopped and removed;
in-memory DAGs (Designer/clones, which have no source file) are left untouched.

---

## 2. DAG names must be globally unique

A DAG's `name` (the field inside the JSON) is its identity everywhere in the
server — the dashboard, the `/dag/{name}/details` URL, pub/sub wiring, worker
dispatch, and metrics labels. It **must be unique across every folder**.
Folders organise *files*; they do **not** create separate name-spaces.

### What happens on a collision

The policy is intentionally strict and loud, because an ambiguous name is a
configuration error:

**At startup** — if any two loaded DAG files declare the same name, the server
**refuses to boot**. It collects *every* collision (not just the first) and
logs a single FATAL message naming each duplicated name and both files:

```
FATAL: DAG name collision(s) detected - names must be globally unique
across all folders:
  - DAG name 'risk_etl' declared in BOTH 'config/risk_dags/a.json' and
    'config/trade_dags/b.json'
Rename or remove the duplicate file(s) and restart.
```

Fix the names (or remove a file) and restart.

**At reload (server already running)** — the **incumbent wins**. A newly-added
file whose name collides with an already-running DAG is **rejected and not
booted**; the running DAG is left completely untouched (a fat-fingered
duplicate can never disrupt something already serving). The collision is:

- logged at CRITICAL level, and
- shown on the dashboard as a **persistent red banner** listing the duplicated
  name, where it is already loaded from, and the rejected file — with a
  one-click **Delete file** button (admin only) to remove the offending JSON
  from storage. After deleting, click **Reload DAGs** to pick up any remaining
  changes.

The banner persists across page refreshes until the offending file is removed,
so it cannot be missed.

---

## 3. External calculator / transformer libraries

You can keep custom calculators, transformers, or any Python modules **outside
the project tree** and reference them by their normal dotted import path. The
server adds the configured directories to the Python import path at startup.

### Configuration

Add one or more `external.module.path.N` properties:

```properties
external.module.path.1=/opt/dishtayantra/libs
external.module.path.2=${HOME}/my_calculators
```

or in YAML:

```yaml
external:
  module:
    path:
      1: /opt/dishtayantra/libs
      2: ${HOME}/my_calculators
```

Each path is environment-expanded (`${VAR}`, `~`), made absolute, and
prepended to `sys.path`. The startup banner reports each one as `OK` (added)
or `WARN` (not found), so the resolution is auditable in the logs.

### Using an external calculator in a DAG

Once the directory is on the path, reference the class by its full dotted path
exactly as you would an in-tree one. For example, if
`/opt/dishtayantra/libs/risk_calcs/var.py` defines `VarCalculator`:

```json
{
  "calculators": [
    { "name": "var", "type": "risk_calcs.var.VarCalculator", "config": {} }
  ]
}
```

The custom class must follow the calculator contract: subclass
`core.calculator.core_calculator.DataCalculator`, implement
`calculate(self, data) -> dict`, and bump `_calculation_count` /
`_last_calculation`.

> **Security note:** configuring an external module path tells the server to
> import code from that location at startup. Only point it at directories you
> control. The added paths are logged so they can be reviewed.

---

## Summary

- `storage.dags.prefixes` adds extra DAG folders; `config/dags` is always
  included; direct children only, never sub-folders.
- DAG names are globally unique: collisions are FATAL at startup and rejected
  (incumbent wins) at reload, surfaced via a persistent red dashboard banner
  with a delete-file button.
- `external.module.path.N` adds directories to the import path so custom
  calculator/transformer libraries can live outside the project tree.
