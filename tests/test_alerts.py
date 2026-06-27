"""Tests for the SLO/staleness alert engine (core/alerts.py)."""
import json
import os
import tempfile

from core.alerts import AlertEngine, AlertRule, load_rules

NOW = 1_000_000_000_000  # fixed 'now' in ms for deterministic ages


class FakeStore:
    """Minimal flow store: per-DAG max_ts and per-node latest ts."""

    def __init__(self, dag_max=None, node_ts=None):
        self._dag_max = dag_max or {}            # dag -> max_ts
        self._node_ts = node_ts or {}            # dag -> {node -> ts}

    def distinct_dags(self):
        return [{"dag_id": d, "instance": "i", "count": 1,
                 "min_ts": mx, "max_ts": mx} for d, mx in self._dag_max.items()]

    def state_at(self, dag_id, ts_ms, instance=None):
        nodes = {n: {"node_id": n, "ts_ms": ts}
                 for n, ts in self._node_ts.get(dag_id, {}).items()}
        return {"ts_ms": ts_ms, "nodes": nodes}


def _engine(rules, dag_max=None, node_ts=None, enabled=True, store=True):
    s = FakeStore(dag_max, node_ts) if store else None
    return AlertEngine(lambda: s, enabled=enabled, rules=rules,
                       clock=lambda: NOW)


def test_disabled_returns_no_alerts():
    e = _engine([AlertRule(dag="d", max_age_seconds=60)], enabled=False)
    out = e.evaluate()
    assert out["enabled"] is False and out["alerts"] == []


def test_dag_level_ok_and_breach():
    rules = [AlertRule(dag="fresh", max_age_seconds=300),
             AlertRule(dag="stale", max_age_seconds=300)]
    e = _engine(rules, dag_max={"fresh": NOW - 10_000,      # 10s ago -> OK
                                "stale": NOW - 600_000})    # 600s ago -> BREACH
    res = {a["name"]: a for a in e.evaluate()["alerts"]}
    assert res["fresh"]["status"] == "OK"
    assert res["stale"]["status"] == "BREACH"
    assert res["stale"]["age_seconds"] >= 600


def test_dag_with_no_events_breaches():
    e = _engine([AlertRule(dag="never", max_age_seconds=60)], dag_max={})
    a = e.evaluate()["alerts"][0]
    assert a["status"] == "BREACH" and a["last_change_ms"] is None
    assert "no recorded output changes" in a["reason"]


def test_per_node_ok_and_breach():
    rules = [AlertRule(dag="d", node="fast", max_age_seconds=120),
             AlertRule(dag="d", node="slow", max_age_seconds=120)]
    e = _engine(rules, node_ts={"d": {"fast": NOW - 5_000,      # OK
                                      "slow": NOW - 300_000}})  # BREACH
    res = {a["name"]: a for a in e.evaluate()["alerts"]}
    assert res["d/fast"]["status"] == "OK"
    assert res["d/slow"]["status"] == "BREACH"


def test_per_node_missing_node_breaches():
    e = _engine([AlertRule(dag="d", node="ghost", max_age_seconds=60)],
                node_ts={"d": {"other": NOW}})
    a = e.evaluate()["alerts"][0]
    assert a["status"] == "BREACH" and "node" in a["reason"]


def test_store_unavailable_is_unknown_not_crash():
    e = _engine([AlertRule(dag="d", max_age_seconds=60)], store=False)
    out = e.evaluate()
    assert out["alerts"][0]["status"] == "UNKNOWN"
    assert out["summary"]["breach"] == 0


def test_summary_counts():
    rules = [AlertRule(dag="a", max_age_seconds=300),
             AlertRule(dag="b", max_age_seconds=300)]
    e = _engine(rules, dag_max={"a": NOW - 1000, "b": NOW - 10**7})
    s = e.evaluate()["summary"]
    assert s == {"total": 2, "ok": 1, "breach": 1}


def test_load_rules_from_json_file():
    d = tempfile.mkdtemp()
    p = os.path.join(d, "alerts.json")
    json.dump({"rules": [
        {"dag": "trade-etl", "max_age_seconds": 300},
        {"dag": "trade-etl", "node": "enrich", "max_age_seconds": 120,
         "name": "enrich-stale"},
        {"dag": "", "max_age_seconds": 60},          # invalid -> skipped
    ]}, open(p, "w"))
    rules = load_rules(p)
    assert len(rules) == 2
    assert rules[1].name == "enrich-stale" and rules[1].node == "enrich"


def test_load_rules_missing_file_is_empty():
    assert load_rules("/nonexistent/alerts.json") == []
    assert load_rules("") == []
