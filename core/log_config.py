"""
core/log_config.py - Single source of truth for application log configuration.

The whole application is switched between human-readable text logs and
structured single-line JSON logs purely at the *formatter/handler* level, so
individual ``logger.info(...)`` / ``logger.error(...)`` call sites never change.
Everything is driven by ``logging.*`` configuration keys (see application.yaml).

Design notes
------------
- ``JsonFormatter`` renders each ``LogRecord`` as one line of JSON.
- Which fields appear is configurable via ``logging.json_fields`` (a comma list
  of names from ``SUPPORTED_FIELDS``); unknown names raise rather than being
  silently dropped.
- Structured per-event data is opt-in and additive: anywhere it helps, a call
  site may pass ``logger.info("msg", extra={"dag": name})`` and the formatter
  picks the key up automatically (``logging.json_include_extra``). Untouched
  call sites simply emit the base fields.
- ``configure_logging()`` attaches the chosen formatter to the root handlers and
  is the only place that needs calling from each entry point (server, webapp,
  worker process), keeping the configuration in one place.
"""

import datetime
import json
import logging
import os
import sys
from typing import List, Optional

# Mapping of configurable field name -> how to extract it from a LogRecord.
_FIELD_MAP = {
    "timestamp": lambda r: datetime.datetime.fromtimestamp(
        r.created, datetime.timezone.utc).isoformat(),
    "epoch":   lambda r: r.created,
    "level":   lambda r: r.levelname,
    "logger":  lambda r: r.name,
    "message": lambda r: r.getMessage(),
    "module":  lambda r: r.module,
    "func":    lambda r: r.funcName,
    "line":    lambda r: r.lineno,
    "path":    lambda r: r.pathname,
    "process": lambda r: r.process,
    "thread":  lambda r: r.threadName,
}

SUPPORTED_FIELDS = sorted(_FIELD_MAP)

# Intrinsic LogRecord attributes; anything else on the record is treated as
# user-supplied "extra" and may be emitted when include_extra is on.
_RESERVED = set(logging.makeLogRecord({}).__dict__) | {
    "message", "asctime", "taskName"}

_DEFAULT_FIELDS = ["timestamp", "level", "logger", "message"]
_DEFAULT_TEXT_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


class JsonFormatter(logging.Formatter):
    """Render each LogRecord as a single line of JSON.

    Args:
        fields: ordered field names to emit (must be in SUPPORTED_FIELDS).
        include_extra: when True, any non-reserved attribute on the record
            (typically supplied via ``logger.x("msg", extra={...})``) is added.
    """

    def __init__(self, fields: List[str], include_extra: bool = True):
        super().__init__()
        unknown = [f for f in fields if f not in _FIELD_MAP]
        if unknown:
            raise ValueError(
                f"Unknown logging.json_fields {unknown}; "
                f"valid fields are {SUPPORTED_FIELDS}")
        if not fields:
            raise ValueError("logging.json_fields must list at least one field")
        self.fields = list(fields)
        self.include_extra = include_extra

    def format(self, record: logging.LogRecord) -> str:
        out = {f: _FIELD_MAP[f](record) for f in self.fields}
        if self.include_extra:
            for key, value in record.__dict__.items():
                if key not in _RESERVED and not key.startswith("_") \
                        and key not in out:
                    out[key] = value
        if record.exc_info:
            out["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            out["stack"] = self.formatStack(record.stack_info)
        # separators -> compact; default=str -> never blow up on odd extras;
        # ensure_ascii=False -> keep unicode (e.g. micro/arrow) readable.
        return json.dumps(out, separators=(",", ":"),
                          default=str, ensure_ascii=False)


class _StaticFieldFilter(logging.Filter):
    """Stamp static key/value pairs (e.g. a worker id) onto every record so
    they flow into the JSON output via include_extra (or a chosen field)."""

    def __init__(self, **fields):
        super().__init__()
        self._fields = fields

    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in self._fields.items():
            setattr(record, key, value)
        return True


def _load_config():
    """Best-effort fetch of the PropertiesConfigurator singleton.

    Works at module-import time and inside a freshly spawned worker process
    (where it re-reads the config files). Returns None if configuration cannot
    be loaded, in which case documented text defaults apply.
    """
    try:
        from core.properties_configurator import PropertiesConfigurator
        from core.config_parsers import find_default_config
        return PropertiesConfigurator([find_default_config("config")])
    except Exception:
        return None


def configure_logging(config=None, *, worker_id: Optional[int] = None,
                      log_dir: str = "logs",
                      logfile: Optional[str] = "dagserver.log",
                      to_stdout: bool = True) -> logging.Logger:
    """Configure the root logger's handlers and formatter from configuration.

    Idempotent: existing root handlers are replaced, so this may be called more
    than once (e.g. webapp import then server startup). No call sites change.

    Args:
        config: a PropertiesConfigurator; if None, it is loaded best-effort.
        worker_id: when set, stamps a ``worker`` field (JSON) or a ``[Worker-N]``
            prefix (text) onto every record.
        log_dir / logfile: file handler target; pass ``logfile=None`` to log to
            stdout only (used by the webapp import hook and worker processes).
        to_stdout: attach a stdout StreamHandler.

    Config keys (all optional; documented defaults shown):
        logging.format               text | json          (default: text)
        logging.level                INFO/DEBUG/...        (default: INFO)
        logging.json_fields          csv of SUPPORTED_FIELDS
                                     (default: timestamp,level,logger,message)
        logging.json_include_extra   true | false          (default: true)
        logging.text_format          logging % format str  (default: classic)
    """
    if config is None:
        config = _load_config()

    def cstr(key, default):
        value = config.get(key) if config else None
        return value if value not in (None, "") else default

    def cbool(key, default):
        value = config.get_bool(key) if config else None
        return default if value is None else value

    def clist(key, default):
        value = config.get_list(key) if config else None
        return value if value else default

    fmt = cstr("logging.format", "text").strip().lower()
    level_name = cstr("logging.level", "INFO").strip().upper()
    level = getattr(logging, level_name, None)
    if not isinstance(level, int):
        raise ValueError(
            f"logging.level must be a valid level name (e.g. INFO, DEBUG); "
            f"got {level_name!r}")

    if fmt == "json":
        formatter = JsonFormatter(
            clist("logging.json_fields", _DEFAULT_FIELDS),
            include_extra=cbool("logging.json_include_extra", True))
    elif fmt == "text":
        text_format = cstr("logging.text_format", _DEFAULT_TEXT_FORMAT)
        if worker_id is not None:
            text_format = (f"%(asctime)s [Worker-{worker_id}] "
                           f"%(levelname)s %(name)s: %(message)s")
        formatter = logging.Formatter(text_format)
    else:
        raise ValueError(
            f"logging.format must be 'text' or 'json'; got {fmt!r}")

    handlers = []
    if to_stdout:
        handlers.append(logging.StreamHandler(sys.stdout))
    if logfile:
        os.makedirs(log_dir, exist_ok=True)
        handlers.append(logging.FileHandler(os.path.join(log_dir, logfile)))

    for handler in handlers:
        handler.setFormatter(formatter)
        if worker_id is not None:
            handler.addFilter(_StaticFieldFilter(worker=worker_id))

    root = logging.getLogger()
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.setLevel(level)
    for handler in handlers:
        root.addHandler(handler)
    return root
