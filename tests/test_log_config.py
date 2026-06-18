"""Tests for core.log_config - structured (JSON) logging at the formatter level."""

import json
import logging

import pytest

from core.log_config import (JsonFormatter, configure_logging,
                             SUPPORTED_FIELDS)


def _record(msg="hello %s", args=("world",), **extra):
    rec = logging.LogRecord(name="dy.test", level=logging.INFO,
                            pathname="/x/y.py", lineno=42, msg=msg,
                            args=args, exc_info=None, func="do_it")
    for key, value in extra.items():
        setattr(rec, key, value)
    return rec


class _StubConfig:
    """Minimal PropertiesConfigurator stand-in for deterministic tests."""

    def __init__(self, values):
        self._v = values

    def get(self, key, default=None):
        return self._v.get(key, default)

    def get_bool(self, key, default=None):
        v = self._v.get(key)
        if v is None:
            return default
        return str(v).strip().lower() in ("true", "1", "yes", "on")

    def get_list(self, key, delim=","):
        v = self._v.get(key)
        if not v:
            return None
        return [s.strip() for s in v.split(delim) if s.strip()]


@pytest.fixture
def preserve_root_logging():
    """Save/restore the global root logger so configure_logging() in a test
    does not leak handlers/level into the rest of the suite."""
    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)


# --------------------------------------------------------------- JsonFormatter
def test_json_is_single_line_and_valid():
    line = JsonFormatter(["timestamp", "level", "logger", "message"]).format(_record())
    assert "\n" not in line                      # single line
    obj = json.loads(line)                        # valid JSON
    assert obj["message"] == "hello world"        # %-args expanded
    assert obj["level"] == "INFO"
    assert obj["logger"] == "dy.test"


def test_field_selection_emits_only_requested():
    obj = json.loads(JsonFormatter(["level", "message"]).format(_record()))
    assert set(obj.keys()) == {"level", "message"}


def test_include_extra_picks_up_extra_kwargs():
    rec = _record(dag="pricing", node="risk")
    on = json.loads(JsonFormatter(["message"], include_extra=True).format(rec))
    assert on["dag"] == "pricing" and on["node"] == "risk"
    off = json.loads(JsonFormatter(["message"], include_extra=False).format(_record(dag="pricing")))
    assert "dag" not in off


def test_unknown_field_raises():
    with pytest.raises(ValueError):
        JsonFormatter(["timestamp", "not_a_field"])
    with pytest.raises(ValueError):
        JsonFormatter([])


def test_exception_field_present():
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys
        rec = _record()
        rec.exc_info = sys.exc_info()
    obj = json.loads(JsonFormatter(["message"]).format(rec))
    assert "exception" in obj and "RuntimeError" in obj["exception"]


def test_all_supported_fields_render():
    obj = json.loads(JsonFormatter(list(SUPPORTED_FIELDS)).format(_record()))
    assert set(obj.keys()) == set(SUPPORTED_FIELDS)


# ------------------------------------------------------------ configure_logging
def test_configure_logging_json_wires_json_formatter(preserve_root_logging):
    cfg = _StubConfig({"logging.format": "json", "logging.level": "INFO",
                       "logging.json_fields": "timestamp,level,message",
                       "logging.json_include_extra": "true"})
    configure_logging(config=cfg, logfile=None)
    root = logging.getLogger()
    assert root.handlers, "root should have at least one handler"
    assert any(isinstance(h.formatter, JsonFormatter) for h in root.handlers)
    assert root.level == logging.INFO


def test_configure_logging_text_default(preserve_root_logging):
    cfg = _StubConfig({"logging.format": "text", "logging.level": "DEBUG",
                       "logging.text_format": "%(levelname)s:%(message)s"})
    configure_logging(config=cfg, logfile=None)
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert all(not isinstance(h.formatter, JsonFormatter) for h in root.handlers)


def test_configure_logging_invalid_format_raises(preserve_root_logging):
    with pytest.raises(ValueError):
        configure_logging(config=_StubConfig({"logging.format": "xml"}), logfile=None)


def test_configure_logging_invalid_level_raises(preserve_root_logging):
    with pytest.raises(ValueError):
        configure_logging(config=_StubConfig({"logging.format": "text",
                                              "logging.level": "LOUD"}), logfile=None)


def test_worker_id_stamps_worker_field(preserve_root_logging):
    cfg = _StubConfig({"logging.format": "json", "logging.level": "INFO",
                       "logging.json_fields": "message", "logging.json_include_extra": "true"})
    configure_logging(config=cfg, worker_id=3, logfile=None)
    root = logging.getLogger()
    rec = _record()
    handler = root.handlers[0]
    for f in handler.filters:           # apply the static-field filter
        f.filter(rec)
    obj = json.loads(handler.formatter.format(rec))
    assert obj["worker"] == 3
