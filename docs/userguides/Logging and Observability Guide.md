# Logging and Observability Guide

DishtaYantra can emit logs as either human-readable text (the default) or
structured single-line JSON, and it exposes runtime metrics for flow control.
Everything is handled at the **formatter level**, so switching format reformats
every log line in the application without changing any code.

## Quick start

To switch the whole application to JSON logs, set this in `config/application.yaml`
(or the matching keys in `config/application.properties`):

```yaml
logging:
  format: json            # text | json
  level: INFO
  json_fields: timestamp,level,logger,message
  json_include_extra: "true"
  text_format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

A JSON line looks like this (one compact object per line, newline-terminated):

```json
{"timestamp":"2026-06-16T17:49:23.189121+00:00","level":"INFO","logger":"core.dag.compute_graph","message":"node recomputed","dag":"pricing","node":"risk"}
```

The default is `text`, so nothing changes until you opt in.

## Configuration reference

| Key | Values | Meaning |
|---|---|---|
| `logging.format` | `text` \| `json` | Output format for every handler. |
| `logging.level` | `DEBUG`/`INFO`/`WARNING`/... | Root log level. |
| `logging.json_fields` | comma list | Which fields appear in each JSON object, in order. |
| `logging.json_include_extra` | `true` \| `false` | Also emit structured fields passed via `extra={...}`. |
| `logging.text_format` | `%`-style format | Format string used when `format: text`. |

Invalid values (an unknown field name, an unknown level, or a format other than
`text`/`json`) **raise on startup** rather than being silently ignored, so a
typo fails fast instead of producing surprising output.

### Selectable JSON fields

`logging.json_fields` may contain any of:

`timestamp` (ISO-8601 UTC), `epoch` (float seconds), `level`, `logger`,
`message`, `module`, `func`, `line`, `path`, `process`, `thread`.

List exactly the fields you want, in the order you want them. Exceptions are
always rendered into an `exception` field (full traceback) when present, and a
`stack` field when stack info is captured.

## Structured per-event fields (opt-in, additive)

You do not need to change existing log calls. Anywhere richer context helps, pass
an `extra` dictionary and (with `json_include_extra: true`) those keys appear in
the JSON automatically:

```python
logger.info("node recomputed", extra={"dag": dag_name, "node": node_id})
# -> {... ,"message":"node recomputed","dag":"orders","node":"risk"}
```

Untouched call sites simply emit the base fields you configured.

## Workers and processes

Logging is configured from one place (`core/log_config.py`) and wired through all
three entry points: the server, the web app, and each worker-pool process. In
JSON mode a worker's lines gain a `worker` field; in text mode they keep the
`[Worker-N]` prefix. No extra setup is needed.

## Third-party loggers

Application logs (everything under the project's loggers) honor the format above.
Libraries that install their own handlers — notably `uvicorn` and `httpx` — only
appear as JSON where they propagate to the root logger. If you need their lines as
JSON too, reconfigure their loggers onto the root (e.g. start uvicorn with
`log_config=None`).

## Observability: backpressure metrics

When credit-based backpressure is enabled (see the *Backpressure (Credit-Based
Flow Control) Guide*), the in-memory broker exposes per-subscriber flow-control
counters via `InMemoryPubSub.get_backpressure_stats()`:

```python
from core.pubsub.inmemorypubsub import InMemoryPubSub
stats = InMemoryPubSub().get_backpressure_stats()
# {topic: [{"capacity":1000,"available":987,"in_flight":13,"granted":...,
#           "blocked":...,"dropped":...,"max_in_flight":...,"total_wait_seconds":...}]}
```

These are useful both as a live health signal (rising `in_flight`/`blocked`
indicates a slow consumer) and as fields to log or export to a metrics system.
