"""SLO / staleness alerting (v1) - output-change staleness from the flow store.

The honest signal available today is the flow change-log: because of the engine's
equality gate, a node only emits an event when its OUTPUT actually changes. So a
rule here breaches when a DAG (or a specific node) has produced **no output change**
within ``max_age_seconds``. This detects frozen/stuck upstreams well; a DAG that is
*intentionally* emitting a constant value will also read as stale (documented).

Design notes:
  * Pure and decoupled - the engine reads the flow store through a provider
    callable, so it never holds a stale handle and is trivial to unit-test.
  * Cheap - evaluation is on demand (when /api/alerts or the monitoring panel is
    hit). distinct_dags() is already TTL-cached in the store; per-node rules use
    one state_at() per referenced DAG, cached within a single evaluation.
  * No new capture path and no hot-path change. Execution-liveness ("ran but
    produced no change") and error-rate SLOs are deliberately out of v1 because
    they require small engine additions (see the design notes / CHANGELOG).
"""
from __future__ import annotations

import json
import logging
import os
import time

logger = logging.getLogger(__name__)

_VALID_KINDS = ("stale_change",)


class AlertRule:
    """A single staleness rule.

    dag             : DAG id the rule applies to (required).
    node            : optional node id; when set the rule is per-node, else per-DAG.
    max_age_seconds : breach when the last output change is older than this.
    name            : human label (defaults to dag[/node]).
    kind            : reserved for future rule types; only 'stale_change' in v1.
    """

    __slots__ = ("name", "dag", "node", "max_age_seconds", "kind")

    def __init__(self, dag, node=None, max_age_seconds=300, name=None,
                 kind="stale_change"):
        if not dag:
            raise ValueError("alert rule requires a 'dag'")
        if kind not in _VALID_KINDS:
            raise ValueError(f"unknown alert kind: {kind!r}")
        try:
            max_age_seconds = int(max_age_seconds)
        except (TypeError, ValueError):
            raise ValueError("max_age_seconds must be an integer")
        if max_age_seconds <= 0:
            raise ValueError("max_age_seconds must be positive")
        self.dag = str(dag)
        self.node = str(node) if node else None
        self.max_age_seconds = max_age_seconds
        self.kind = kind
        self.name = str(name) if name else (
            f"{self.dag}/{self.node}" if self.node else self.dag)

    @classmethod
    def from_dict(cls, d):
        return cls(dag=d.get("dag"), node=d.get("node"),
                   max_age_seconds=d.get("max_age_seconds", 300),
                   name=d.get("name"), kind=d.get("kind", "stale_change"))

    def to_dict(self):
        return {"name": self.name, "dag": self.dag, "node": self.node,
                "max_age_seconds": self.max_age_seconds, "kind": self.kind}


def load_rules(path):
    """Load rules from a JSON file: either a top-level list, or {"rules": [...]}.

    Returns [] (and logs) when the path is empty, missing, or malformed - alerting
    should never take the server down.
    """
    if not path:
        return []
    if not os.path.exists(path):
        logger.warning("alerts.rules_file %s not found; no alert rules loaded", path)
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        items = data.get("rules", []) if isinstance(data, dict) else data
        rules = []
        for it in items:
            try:
                rules.append(AlertRule.from_dict(it))
            except Exception as e:  # noqa: BLE001
                logger.warning("skipping invalid alert rule %r: %s", it, e)
        logger.info("loaded %d alert rule(s) from %s", len(rules), path)
        return rules
    except Exception as e:  # noqa: BLE001
        logger.warning("failed to read alerts.rules_file %s: %s", path, e)
        return []


class AlertEngine:
    """Evaluates staleness rules against the flow store on demand."""

    def __init__(self, store_provider, enabled=False, rules=None, clock=None):
        self._store_provider = store_provider      # callable -> FlowStore | None
        self.enabled = bool(enabled)
        self.rules = list(rules or [])
        self._clock = clock or (lambda: int(time.time() * 1000))

    def set_rules(self, rules):
        self.rules = list(rules or [])

    def evaluate(self, now_ms=None):
        """Return {enabled, generated_ms, summary, alerts:[...]} for all rules."""
        now_ms = int(now_ms if now_ms is not None else self._clock())
        if not self.enabled:
            return {"enabled": False, "generated_ms": now_ms,
                    "summary": {"total": 0, "ok": 0, "breach": 0},
                    "alerts": []}
        store = None
        try:
            store = self._store_provider()
        except Exception:  # noqa: BLE001
            store = None
        if store is None:
            return {"enabled": True, "generated_ms": now_ms,
                    "summary": {"total": len(self.rules), "ok": 0,
                                "breach": 0, "unknown": len(self.rules)},
                    "alerts": [self._result(r, None, now_ms, unknown=True)
                               for r in self.rules],
                    "note": "flow store unavailable"}

        dag_max = self._dag_max_ts(store)
        node_state = {}                                  # dag -> state_at(now) cache
        alerts, ok, breach = [], 0, 0
        for r in self.rules:
            if r.node:
                st = node_state.get(r.dag)
                if st is None:
                    try:
                        st = store.state_at(r.dag, now_ms)
                    except Exception:  # noqa: BLE001
                        st = {"nodes": {}}
                    node_state[r.dag] = st
                rec = (st.get("nodes") or {}).get(r.node)
                last_ts = rec.get("ts_ms") if rec else None
            else:
                last_ts = dag_max.get(r.dag)
            res = self._result(r, last_ts, now_ms)
            alerts.append(res)
            ok += res["status"] == "OK"
            breach += res["status"] == "BREACH"
        return {"enabled": True, "generated_ms": now_ms,
                "summary": {"total": len(self.rules), "ok": ok, "breach": breach},
                "alerts": alerts}

    # ----------------------------------------------------------------- helpers
    @staticmethod
    def _dag_max_ts(store):
        out = {}
        try:
            for d in store.distinct_dags():
                mx = d.get("max_ts")
                if mx is not None:
                    out[d["dag_id"]] = max(out.get(d["dag_id"], 0), int(mx))
        except Exception:  # noqa: BLE001
            pass
        return out

    @staticmethod
    def _result(rule, last_ts, now_ms, unknown=False):
        base = {"name": rule.name, "dag": rule.dag, "node": rule.node,
                "kind": rule.kind, "max_age_seconds": rule.max_age_seconds,
                "last_change_ms": last_ts}
        if unknown:
            base.update(status="UNKNOWN", age_seconds=None,
                        reason="flow store unavailable")
            return base
        if last_ts is None:
            base.update(status="BREACH", age_seconds=None,
                        reason="no recorded output changes for this "
                               + ("node" if rule.node else "DAG"))
            return base
        age_s = max(0.0, (now_ms - int(last_ts)) / 1000.0)
        breached = age_s > rule.max_age_seconds
        base.update(
            status="BREACH" if breached else "OK",
            age_seconds=round(age_s, 1),
            reason=(f"no output change for {age_s:.0f}s "
                    f"(limit {rule.max_age_seconds}s)") if breached
                   else f"last change {age_s:.0f}s ago")
        return base
