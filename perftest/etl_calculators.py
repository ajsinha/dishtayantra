"""
Custom trade-ETL calculators for the DishtaYantra performance test.

These implement realistic, meaningful trade-processing logic so the perf graph
exercises real CPU work (string/number handling, dict mutation, branching) on
every message rather than no-op passthroughs.

Each calculator follows the platform contract:
  - subclass DataCalculator
  - implement calculate(self, data) -> dict
  - bump _calculation_count / _last_calculation

Reference these from a DAG by their full dotted path, e.g.
  "type": "perftest.etl_calculators.NormalizeTradeCalculator"
"""

from datetime import datetime, timezone
import copy

from core.calculator.core_calculator import DataCalculator


# A tiny static FX table (to USD). Realistic enough for a deterministic test.
_FX_TO_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0067,
    "CHF": 1.12,
    "INR": 0.012,
    "AUD": 0.66,
    "CAD": 0.73,
}


def _bump(calc):
    calc._calculation_count += 1
    calc._last_calculation = datetime.now().isoformat()


class ValidateTradeCalculator(DataCalculator):
    """Stage 1: validate required fields and tag bad records.

    Does not drop records (so throughput is measurable end to end); instead it
    stamps `_valid` and `_errors` so downstream stages and the sink can see them.
    """

    REQUIRED = ("trade_id", "symbol", "side", "quantity", "price", "currency")

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data) if isinstance(data, dict) else {"_raw": data}
        errors = []
        for field in self.REQUIRED:
            if field not in result or result[field] in (None, ""):
                errors.append(f"missing:{field}")
        side = str(result.get("side", "")).upper()
        if side not in ("BUY", "SELL"):
            errors.append("bad_side")
        try:
            if float(result.get("quantity", 0)) <= 0:
                errors.append("nonpositive_qty")
        except (TypeError, ValueError):
            errors.append("bad_qty")
        result["_valid"] = len(errors) == 0
        result["_errors"] = errors
        return result


class NormalizeTradeCalculator(DataCalculator):
    """Stage 2: normalize types and canonical field shapes."""

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        result["symbol"] = str(result.get("symbol", "")).upper().strip()
        result["side"] = str(result.get("side", "")).upper().strip()
        result["currency"] = str(result.get("currency", "USD")).upper().strip()
        for num_field in ("quantity", "price"):
            try:
                result[num_field] = float(result.get(num_field, 0) or 0)
            except (TypeError, ValueError):
                result[num_field] = 0.0
        return result


class FxConvertCalculator(DataCalculator):
    """Stage 3: convert price to USD using a static FX table."""

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        ccy = result.get("currency", "USD")
        rate = _FX_TO_USD.get(ccy, self.config.get("default_rate", 1.0))
        result["fx_rate_to_usd"] = rate
        result["price_usd"] = round(float(result.get("price", 0.0)) * rate, 6)
        return result


class NotionalCalculator(DataCalculator):
    """Stage 4: notional = quantity * price_usd (signed by side)."""

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        qty = float(result.get("quantity", 0.0))
        px = float(result.get("price_usd", 0.0))
        notional = qty * px
        sign = -1.0 if result.get("side") == "SELL" else 1.0
        result["notional_usd"] = round(notional, 2)
        result["signed_notional_usd"] = round(notional * sign, 2)
        return result


class FeeCalculator(DataCalculator):
    """Stage 5: commission + exchange fee from configurable rates."""

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        commission_rate = float(self.config.get("commission_rate", 0.0005))
        exchange_fee_rate = float(self.config.get("exchange_fee_rate", 0.0001))
        min_fee = float(self.config.get("min_fee", 0.50))
        notional = abs(float(result.get("notional_usd", 0.0)))
        commission = max(notional * commission_rate, min_fee)
        exchange_fee = notional * exchange_fee_rate
        result["commission_usd"] = round(commission, 4)
        result["exchange_fee_usd"] = round(exchange_fee, 4)
        result["total_fees_usd"] = round(commission + exchange_fee, 4)
        result["net_notional_usd"] = round(
            float(result.get("signed_notional_usd", 0.0)) - (commission + exchange_fee), 2)
        return result


class RiskScoreCalculator(DataCalculator):
    """Stage 6: a simple deterministic exposure/risk score.

    Combines notional magnitude with a per-symbol volatility proxy so the score
    varies by instrument - meaningful branching work, not a constant.
    """

    # crude volatility proxy by first letter bucket (deterministic, no RNG)
    @staticmethod
    def _vol(symbol):
        if not symbol:
            return 0.15
        c = symbol[0]
        if c in "ABCDE":
            return 0.12
        if c in "FGHIJ":
            return 0.20
        if c in "KLMNO":
            return 0.28
        if c in "PQRST":
            return 0.35
        return 0.45

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        notional = abs(float(result.get("notional_usd", 0.0)))
        vol = self._vol(result.get("symbol", ""))
        # 1-day 95% parametric VaR-ish figure: notional * vol * 1.65
        var_1d = notional * vol * 1.65
        result["volatility"] = vol
        result["var_1d_usd"] = round(var_1d, 2)
        # normalized 0-100 risk score (log-ish bucketing without importing math)
        score = min(100.0, (var_1d / 1000.0))
        result["risk_score"] = round(score, 2)
        return result


class ClassifyTradeCalculator(DataCalculator):
    """Stage 7: bucket trades by size and risk into a tier."""

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        notional = abs(float(result.get("notional_usd", 0.0)))
        risk = float(result.get("risk_score", 0.0))
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
        result["size_bucket"] = size_bucket
        result["risk_tier"] = risk_tier
        result["requires_review"] = (size_bucket in ("BLOCK", "LARGE")
                                     and risk_tier == "HIGH")
        return result


class AnomalyFlagCalculator(DataCalculator):
    """Stage 8: flag suspicious trades via simple rule-based outlier checks."""

    def calculate(self, data):
        _bump(self)
        result = copy.deepcopy(data)
        flags = []
        price = float(result.get("price_usd", 0.0))
        qty = float(result.get("quantity", 0.0))
        fat_finger_qty = float(self.config.get("fat_finger_qty", 1_000_000))
        max_price = float(self.config.get("max_price_usd", 1_000_000))
        if qty >= fat_finger_qty:
            flags.append("FAT_FINGER_QTY")
        if price >= max_price:
            flags.append("PRICE_OUTLIER")
        if price <= 0:
            flags.append("ZERO_OR_NEG_PRICE")
        if not result.get("_valid", True):
            flags.append("VALIDATION_FAILED")
        result["anomaly_flags"] = flags
        result["is_anomalous"] = len(flags) > 0
        return result


class SummarizeTradeCalculator(DataCalculator):
    """Stage 9: build the compact, ordered output record written to the sink.

    Keeps only the fields a downstream consumer cares about, plus a processing
    timestamp - so the file output is clean and the work of projecting/renaming
    is part of the measured pipeline.
    """

    OUTPUT_FIELDS = (
        "trade_id", "seq", "symbol", "side", "quantity", "currency",
        "price_usd", "notional_usd", "signed_notional_usd",
        "total_fees_usd", "net_notional_usd",
        "var_1d_usd", "risk_score", "size_bucket", "risk_tier",
        "requires_review", "is_anomalous", "anomaly_flags", "_valid",
    )

    def calculate(self, data):
        _bump(self)
        out = {k: data.get(k) for k in self.OUTPUT_FIELDS}
        out["processed_at"] = datetime.now(timezone.utc).isoformat()
        out["pipeline"] = "perftest_trade_etl"
        return out
