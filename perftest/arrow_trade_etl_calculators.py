"""
Vectorized (Arrow columnar) trade-ETL calculators for the high-throughput
``perftest_trade_etl_arrow`` DAG.

These complement the FX / notional / fee / risk calculators already in
``perftest.arrow_etl_calculators`` (which this DAG reuses). Each one subclasses
``ArrowCalculator`` and implements ``calculate_batch`` so the engine carries one
immutable ``pyarrow.RecordBatch`` per cycle and every stage is a single
columnar pass (no per-row Python on the hot path).

They operate only on the stable "core" columns guaranteed by
``NormalizingArrowBatchingSubscriptionNode`` (trade_id/symbol/side/quantity/
price/currency, plus columns appended by upstream stages), so heterogeneous
input never causes a missing-column error. The original variable attributes ride
through untouched in the ``extras_json`` column.

Copyright (c) 2025-2030 Ashutosh Sinha. All rights reserved.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc

from core.calculator.arrow_calculator import ArrowCalculator, append_columns


class ArrowValidateCalculator(ArrowCalculator):
    """Vectorized validate: side in {BUY,SELL} and quantity > 0 -> bool ``_valid``.

    (Required-field presence is already guaranteed by the normalizing ingest
    node's fixed schema, so this only checks the value-level rules.)
    """

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        side = batch.column("side")
        qty = pc.cast(batch.column("quantity"), pa.float64())
        good_side = pc.is_in(side, value_set=pa.array(["BUY", "SELL"]))
        good_qty = pc.greater(qty, 0.0)
        return append_columns(batch, {"_valid": pc.and_(good_side, good_qty)})


class ArrowClassifyCalculator(ArrowCalculator):
    """Vectorized classify: size_bucket, risk_tier, requires_review."""

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        notional = pc.abs(pc.cast(batch.column("notional_usd"), pa.float64()))
        risk = pc.cast(batch.column("risk_score"), pa.float64())
        size_bucket = pc.if_else(
            pc.greater_equal(notional, 1_000_000), "BLOCK",
            pc.if_else(
                pc.greater_equal(notional, 100_000), "LARGE",
                pc.if_else(pc.greater_equal(notional, 10_000), "MEDIUM", "SMALL")))
        risk_tier = pc.if_else(
            pc.greater_equal(risk, 50.0), "HIGH",
            pc.if_else(pc.greater_equal(risk, 15.0), "MEDIUM", "LOW"))
        requires_review = pc.and_(
            pc.is_in(size_bucket, value_set=pa.array(["BLOCK", "LARGE"])),
            pc.equal(risk_tier, "HIGH"))
        return append_columns(batch, {
            "size_bucket": size_bucket,
            "risk_tier": risk_tier,
            "requires_review": requires_review,
        })


class ArrowAnomalyCalculator(ArrowCalculator):
    """Vectorized anomaly flags. Emits boolean flag columns + ``is_anomalous``.

    The row pipeline produced a list-of-strings ``anomaly_flags``; the columnar
    variant instead emits one boolean column per rule (cheaper and queryable),
    plus the combined ``is_anomalous``.
    """

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        fat_finger = float(self.config.get("fat_finger_qty", 1_000_000))
        max_price = float(self.config.get("max_price_usd", 1_000_000))
        price = pc.cast(batch.column("price_usd"), pa.float64())
        qty = pc.cast(batch.column("quantity"), pa.float64())
        flag_ff = pc.greater_equal(qty, fat_finger)
        flag_outlier = pc.greater_equal(price, max_price)
        flag_zero = pc.less_equal(price, 0.0)
        if "_valid" in batch.schema.names:
            flag_invalid = pc.invert(batch.column("_valid"))
        else:
            flag_invalid = pa.array([False] * batch.num_rows, pa.bool_())
        is_anom = pc.or_(pc.or_(flag_ff, flag_outlier),
                         pc.or_(flag_zero, flag_invalid))
        return append_columns(batch, {
            "flag_fat_finger": flag_ff,
            "flag_price_outlier": flag_outlier,
            "flag_zero_or_neg_price": flag_zero,
            "flag_validation_failed": flag_invalid,
            "is_anomalous": is_anom,
        })


class ArrowSummarizeCalculator(ArrowCalculator):
    """Vectorized summarize: stamp processing metadata.

    Appends ``processed_at`` (one timestamp per batch) and ``pipeline``. All
    computed columns - and the original variable attributes in ``extras_json`` -
    are preserved, so the sink output is lossless.
    """

    def calculate_batch(self, batch: "pa.RecordBatch") -> "pa.RecordBatch":
        n = batch.num_rows
        ts = datetime.now(timezone.utc).isoformat()
        return append_columns(batch, {
            "processed_at": pa.array([ts] * n, pa.string()),
            "pipeline": pa.array(["perftest_trade_etl_arrow"] * n, pa.string()),
        })
