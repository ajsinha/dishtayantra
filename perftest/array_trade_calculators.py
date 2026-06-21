"""
JSON-array (list-of-dicts) trade-ETL calculators for DishtaYantra.

This is the *array* lane of the trade-ETL benchmark: the same pipeline as the
row version (``perftest/etl_calculators.py``) and the Arrow version
(``perftest/arrow_trade_etl_calculators.py``), but the unit of work on every
edge is a plain Python **list of dicts** (a JSON array) rather than one message
(row lane) or a ``pyarrow.RecordBatch`` (Arrow lane).

How it works:
  - A stock ``BatchingSubscriptionNode`` drains N messages into the standard
    batch envelope ``{batch_key: [ {trade}, {trade}, ... ]}``.
  - Each calculator here receives that envelope, processes the whole list in a
    single ``calculate()`` call (so the per-message node/queue/gate overhead is
    amortized across the batch), and returns a new envelope.
  - A stock ``FlatteningPublicationNode`` unbatches at the sink, so the external
    one-message-in / one-message-out contract is unchanged.

Why no Arrow: this lane gets its speedup purely from *batch amortization* of node
overhead, not from columnar vectorization. The arithmetic is still pure-Python
per element, so it is expected to sit between the row lane and the Arrow lane.
Useful (a) when data is too irregular/nested to normalize into Arrow columns and
(b) as a benchmark baseline that isolates batching gains from vectorization gains.

Parity: the per-trade output is byte-identical to the row lane (the canonical
logic and constants are imported from ``perftest.etl_calculators``), except for
the non-deterministic ``processed_at`` timestamp and the ``pipeline`` tag.

Heterogeneous trades: each element is just a dict, so missing/extra attributes
are handled naturally with ``.get(key, default)`` - no stable schema needed. Like
the row lane, ``summarize`` projects to a fixed output shape, so any extra
attributes simply fall away at the end (the Arrow lane instead carries them in an
``extras_json`` column).

Reference these from a DAG by dotted path, e.g.
  "type": "perftest.array_trade_calculators.ArrayFxConvertCalculator"
"""

from datetime import datetime, timezone

from core.calculator.core_calculator import DataCalculator

# Import the canonical constants/logic from the row lane so the two lanes can
# never drift apart numerically.
from perftest.etl_calculators import (
    _FX_TO_USD,
    ValidateTradeCalculator,
    RiskScoreCalculator,
    SummarizeTradeCalculator,
)

_REQUIRED = ValidateTradeCalculator.REQUIRED
_OUTPUT_FIELDS = SummarizeTradeCalculator.OUTPUT_FIELDS
_vol = RiskScoreCalculator._vol


class _ArrayBatchCalculator(DataCalculator):
    """Base: apply ``_process`` to the list inside a ``{batch_key: [...]}``
    envelope, in one pass, with a single calculation-count bump per batch.

    Robust to envelope shape: uses the configured ``batch_key`` (default
    ``batch``); if that key is absent it falls back to the single list-valued key
    so it works regardless of how the upstream batching node was configured.
    """

    def __init__(self, name, config):
        super().__init__(name, config)
        self._batch_key = (config or {}).get('batch_key', 'batch')

    def _resolve_key(self, data):
        if isinstance(data, dict):
            if self._batch_key in data and isinstance(data[self._batch_key], list):
                return self._batch_key
            list_keys = [k for k, v in data.items() if isinstance(v, list)]
            if len(list_keys) == 1:
                return list_keys[0]
        return None

    def calculate(self, data):
        self._calculation_count += 1
        self._last_calculation = datetime.now().isoformat()
        key = self._resolve_key(data)
        if key is None:
            # Not a batch envelope - nothing this lane can do; pass through.
            return data
        trades = data[key]
        out = dict(data)
        out[key] = self._process(trades)
        return out

    def _process(self, trades):  # pragma: no cover - overridden
        raise NotImplementedError


class ArrayValidateCalculator(_ArrayBatchCalculator):
    """Stage 1: validate required fields; tag (don't drop) bad records."""

    def _process(self, trades):
        required = _REQUIRED
        out = []
        for t in trades:
            errors = []
            for field in required:
                if field not in t or t[field] in (None, ""):
                    errors.append(f"missing:{field}")
            side = str(t.get("side", "")).upper()
            if side not in ("BUY", "SELL"):
                errors.append("bad_side")
            try:
                if float(t.get("quantity", 0)) <= 0:
                    errors.append("nonpositive_qty")
            except (TypeError, ValueError):
                errors.append("bad_qty")
            out.append({**t, "_valid": len(errors) == 0, "_errors": errors})
        return out


class ArrayNormalizeCalculator(_ArrayBatchCalculator):
    """Stage 2: normalize types and canonical field shapes."""

    def _process(self, trades):
        out = []
        for t in trades:
            rec = {**t}
            rec["symbol"] = str(t.get("symbol", "")).upper().strip()
            rec["side"] = str(t.get("side", "")).upper().strip()
            rec["currency"] = str(t.get("currency", "USD")).upper().strip()
            for num_field in ("quantity", "price"):
                try:
                    rec[num_field] = float(t.get(num_field, 0) or 0)
                except (TypeError, ValueError):
                    rec[num_field] = 0.0
            out.append(rec)
        return out


class ArrayFxConvertCalculator(_ArrayBatchCalculator):
    """Stage 3: convert price to USD using the shared static FX table."""

    def _process(self, trades):
        default_rate = self.config.get("default_rate", 1.0)
        fx = _FX_TO_USD
        out = []
        for t in trades:
            rate = fx.get(t.get("currency", "USD"), default_rate)
            out.append({**t,
                        "fx_rate_to_usd": rate,
                        "price_usd": round(float(t.get("price", 0.0)) * rate, 6)})
        return out


class ArrayNotionalCalculator(_ArrayBatchCalculator):
    """Stage 4: notional = quantity * price_usd (signed by side)."""

    def _process(self, trades):
        out = []
        for t in trades:
            notional = float(t.get("quantity", 0.0)) * float(t.get("price_usd", 0.0))
            sign = -1.0 if t.get("side") == "SELL" else 1.0
            out.append({**t,
                        "notional_usd": round(notional, 2),
                        "signed_notional_usd": round(notional * sign, 2)})
        return out


class ArrayFeeCalculator(_ArrayBatchCalculator):
    """Stage 5: commission + exchange fee from configurable rates."""

    def _process(self, trades):
        commission_rate = float(self.config.get("commission_rate", 0.0005))
        exchange_fee_rate = float(self.config.get("exchange_fee_rate", 0.0001))
        min_fee = float(self.config.get("min_fee", 0.50))
        out = []
        for t in trades:
            notional = abs(float(t.get("notional_usd", 0.0)))
            commission = max(notional * commission_rate, min_fee)
            exchange_fee = notional * exchange_fee_rate
            out.append({**t,
                        "commission_usd": round(commission, 4),
                        "exchange_fee_usd": round(exchange_fee, 4),
                        "total_fees_usd": round(commission + exchange_fee, 4),
                        "net_notional_usd": round(
                            float(t.get("signed_notional_usd", 0.0))
                            - (commission + exchange_fee), 2)})
        return out


class ArrayRiskScoreCalculator(_ArrayBatchCalculator):
    """Stage 6: deterministic exposure/risk score (shared volatility proxy)."""

    def _process(self, trades):
        vol_fn = _vol
        out = []
        for t in trades:
            notional = abs(float(t.get("notional_usd", 0.0)))
            vol = vol_fn(t.get("symbol", ""))
            var_1d = notional * vol * 1.65
            out.append({**t,
                        "volatility": vol,
                        "var_1d_usd": round(var_1d, 2),
                        "risk_score": round(min(100.0, (var_1d / 1000.0)), 2)})
        return out


class ArrayAnomalyCalculator(_ArrayBatchCalculator):
    """Stage 7: rule-based outlier flags."""

    def _process(self, trades):
        fat_finger_qty = float(self.config.get("fat_finger_qty", 1_000_000))
        max_price = float(self.config.get("max_price_usd", 1_000_000))
        out = []
        for t in trades:
            flags = []
            price = float(t.get("price_usd", 0.0))
            qty = float(t.get("quantity", 0.0))
            if qty >= fat_finger_qty:
                flags.append("FAT_FINGER_QTY")
            if price >= max_price:
                flags.append("PRICE_OUTLIER")
            if price <= 0:
                flags.append("ZERO_OR_NEG_PRICE")
            if not t.get("_valid", True):
                flags.append("VALIDATION_FAILED")
            out.append({**t, "anomaly_flags": flags, "is_anomalous": len(flags) > 0})
        return out


class ArrayClassifyCalculator(_ArrayBatchCalculator):
    """Stage 8: bucket trades by size and risk into a tier."""

    def _process(self, trades):
        out = []
        for t in trades:
            notional = abs(float(t.get("notional_usd", 0.0)))
            risk = float(t.get("risk_score", 0.0))
            if notional >= 1_000_000:
                size_bucket = "BLOCK"
            elif notional >= 100_000:
                size_bucket = "LARGE"
            elif notional >= 10_000:
                size_bucket = "MEDIUM"
            else:
                size_bucket = "SMALL"
            if risk >= 50:
                risk_tier = "HIGH"
            elif risk >= 15:
                risk_tier = "MEDIUM"
            else:
                risk_tier = "LOW"
            out.append({**t,
                        "size_bucket": size_bucket,
                        "risk_tier": risk_tier,
                        "requires_review": (size_bucket in ("BLOCK", "LARGE")
                                            and risk_tier == "HIGH")})
        return out


class ArraySummarizeCalculator(_ArrayBatchCalculator):
    """Stage 9: project to the fixed output shape written to the sink."""

    def _process(self, trades):
        fields = _OUTPUT_FIELDS
        now = datetime.now(timezone.utc).isoformat()
        out = []
        for t in trades:
            rec = {k: t.get(k) for k in fields}
            rec["processed_at"] = now
            rec["pipeline"] = "perftest_trade_etl_array"
            out.append(rec)
        return out
