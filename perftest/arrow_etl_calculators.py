"""Arrow (columnar) trade-ETL calculators for the A1 worked example.

These are vectorized, output-identical equivalents of the numeric stages in
``perftest.etl_calculators`` (FX convert, notional, fees, risk). They subclass
``core.calculator.arrow_calculator.ArrowCalculator``, so each is BOTH a drop-in
row calculator AND a vectorized batch calculator (see that module). The engine
is not modified by using them.

Reference from a DAG by dotted path, e.g.
  "type": "perftest.arrow_etl_calculators.ArrowFxConvertCalculator"

Exact parity: rounded fields use Python-correct rounding (py_round_array) so
results are bit-identical to the scalar row calculators; only the heavy
arithmetic is vectorized.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from core.calculator.arrow_calculator import ArrowCalculator, append_columns, py_round_array

# Same FX table as perftest.etl_calculators._FX_TO_USD.
_CCY = ["USD", "EUR", "GBP", "JPY", "CHF", "INR", "AUD", "CAD"]
_RATE = [1.0, 1.08, 1.27, 0.0067, 1.12, 0.012, 0.66, 0.73]


class ArrowFxConvertCalculator(ArrowCalculator):
    """Vectorized FxConvertCalculator: fx_rate_to_usd, price_usd=round(price*rate,6)."""

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        default_rate = float(self.config.get("default_rate", 1.0))
        price = pc.cast(batch.column("price"), pa.float64())
        idx = pc.index_in(batch.column("currency"), value_set=pa.array(_CCY))
        rate = pc.fill_null(pa.array(_RATE, pa.float64()).take(idx), default_rate)
        price_usd = py_round_array(pc.multiply(price, rate), 6)
        return append_columns(batch, {"fx_rate_to_usd": rate, "price_usd": price_usd})


class ArrowNotionalCalculator(ArrowCalculator):
    """Vectorized NotionalCalculator: notional_usd, signed_notional_usd."""

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        qty = pc.cast(batch.column("quantity"), pa.float64())
        px = pc.cast(batch.column("price_usd"), pa.float64())
        notional = pc.multiply(qty, px)
        sign = pc.if_else(pc.equal(batch.column("side"), "SELL"), -1.0, 1.0)
        return append_columns(batch, {
            "notional_usd": py_round_array(notional, 2),
            "signed_notional_usd": py_round_array(pc.multiply(notional, sign), 2),
        })


class ArrowFeeCalculator(ArrowCalculator):
    """Vectorized FeeCalculator: commission, exchange fee, total, net notional."""

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        commission_rate = float(self.config.get("commission_rate", 0.0005))
        exchange_fee_rate = float(self.config.get("exchange_fee_rate", 0.0001))
        min_fee = float(self.config.get("min_fee", 0.50))

        notional = pc.abs(pc.cast(batch.column("notional_usd"), pa.float64()))
        signed = pc.cast(batch.column("signed_notional_usd"), pa.float64())
        commission = pc.max_element_wise(pc.multiply(notional, commission_rate), min_fee)
        exchange_fee = pc.multiply(notional, exchange_fee_rate)
        total = pc.add(commission, exchange_fee)
        return append_columns(batch, {
            "commission_usd": py_round_array(commission, 4),
            "exchange_fee_usd": py_round_array(exchange_fee, 4),
            "total_fees_usd": py_round_array(total, 4),
            "net_notional_usd": py_round_array(pc.subtract(signed, total), 2),
        })


class ArrowRiskScoreCalculator(ArrowCalculator):
    """Vectorized RiskScoreCalculator: volatility, var_1d_usd, risk_score."""

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        symbol = pc.cast(batch.column("symbol"), pa.string())
        notional = pc.abs(pc.cast(batch.column("notional_usd"), pa.float64()))

        first = pc.utf8_upper(pc.utf8_slice_codeunits(symbol, 0, 1))
        vol = pc.if_else(
            pc.is_in(first, value_set=pa.array(list("ABCDE"))), 0.12,
            pc.if_else(
                pc.is_in(first, value_set=pa.array(list("FGHIJ"))), 0.20,
                pc.if_else(
                    pc.is_in(first, value_set=pa.array(list("KLMNO"))), 0.28,
                    pc.if_else(
                        pc.is_in(first, value_set=pa.array(list("PQRST"))), 0.35, 0.45,
                    ),
                ),
            ),
        )
        # Empty symbol -> 0.15 (matches the row calculator's guard).
        vol = pc.if_else(pc.equal(pc.utf8_length(symbol), 0), 0.15, vol)

        var_1d = pc.multiply(pc.multiply(notional, vol), 1.65)
        score = pc.min_element_wise(pc.divide(var_1d, 1000.0), 100.0)
        return append_columns(batch, {
            "volatility": vol,
            "var_1d_usd": py_round_array(var_1d, 2),
            "risk_score": py_round_array(score, 2),
        })
