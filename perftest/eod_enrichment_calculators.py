"""End-of-day (EOD) batch-file processing calculators.

Two patterns for "interrelated" feeds:

1. ReferenceEnrichCalculator — FACT x DIMENSION enrichment. The large fact feed
   (trades) is enriched against small dimension feeds (FX rates, client limits)
   that are loaded ONCE into in-memory lookup tables. Each trade is independent
   given the loaded tables, so this is fast and parallelizable — it does NOT
   require one-record-at-a-time processing.

2. RunningExposureCalculator — RECORD x RECORD sequential dependency. A client's
   running exposure depends on the trades before it, so records must be processed
   in order, one at a time, with state carried forward. This is the case that
   genuinely needs sequential processing.

See docs/BATCH_FILE_PROCESSING.md (and the Help page) for the full pattern and a
runnable demo in perftest/run_eod_example.py.
"""

from __future__ import annotations

import json
from typing import Any, Dict

from core.calculator.core_calculator import DataCalculator


def _load_table(inline, path) -> Dict[str, Any]:
    table: Dict[str, Any] = dict(inline or {})
    if path:
        with open(path) as fh:
            table.update(json.load(fh))
    return table


class ReferenceEnrichCalculator(DataCalculator):
    """Enrich each trade with its FX rate and client limit, then value + check it.

    Reference tables are loaded once from config (inline or file path):
        config = {
          "fx_rates":      {"EUR": 1.08, ...}   OR  "fx_file": "/path/fx.json",
          "client_limits": {"C1": 1000000, ...} OR  "limits_file": "/path/limits.json",
          "default_rate":  1.0
        }
    Output adds: fx_rate, notional_usd, client_limit, limit_breach.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        self.fx = _load_table(cfg.get("fx_rates"), cfg.get("fx_file"))
        self.limits = _load_table(cfg.get("client_limits"), cfg.get("limits_file"))
        self.default_rate = float(cfg.get("default_rate", 1.0))

    def calculate(self, data):
        self._calculation_count += 1
        r = dict(data)
        rate = float(self.fx.get(r.get("currency"), self.default_rate))
        notional = float(r.get("quantity", 0) or 0) * float(r.get("price", 0) or 0) * rate
        r["fx_rate"] = rate
        r["notional_usd"] = round(notional, 2)
        limit = self.limits.get(str(r.get("client_id")))
        r["client_limit"] = float(limit) if limit is not None else None
        r["limit_breach"] = limit is not None and abs(notional) > float(limit)
        return r


class RunningExposureCalculator(DataCalculator):
    """Sequential/stateful: maintain cumulative notional per client across the
    ORDERED trade stream and flag when running exposure breaches the client limit.

    This depends on the trades seen so far, so it must process records in order,
    one at a time. (Do NOT batch/vectorize across this dependency.)
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        cfg = config or {}
        self.limits = _load_table(cfg.get("client_limits"), cfg.get("limits_file"))
        self._cum: Dict[str, float] = {}

    def calculate(self, data):
        self._calculation_count += 1
        r = dict(data)
        client = str(r.get("client_id"))
        amount = float(r.get("notional_usd", 0) or 0)
        self._cum[client] = self._cum.get(client, 0.0) + amount
        r["running_exposure_usd"] = round(self._cum[client], 2)
        limit = self.limits.get(client)
        r["running_breach"] = limit is not None and abs(self._cum[client]) > float(limit)
        return r
