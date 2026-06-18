"""
Nexmark-style benchmark calculators for DishtaYantra.

A representative *subset* of the Nexmark streaming benchmark (the canonical
auction/bid stream): Q1 (currency conversion - a stateless map) and Q2 (auction
selection - a stateless filter expressed as a flag, since the linear bench DAG
maps each input to one output). These exercise real per-message calculator work,
mirroring how the finance trade-ETL workload is built, so both can be driven by
the same in-memory DAG harness. This is not the full 8-query Nexmark suite.

Reference from a DAG by dotted path, e.g.
  "type": "benchmarks.nexmark_calculators.Q1CurrencyConvertCalculator"
"""

from datetime import datetime

from core.calculator.core_calculator import DataCalculator

# Nexmark Q1 converts bid prices from US dollars to euros at a fixed rate.
_USD_TO_EUR = 0.908
# Nexmark Q2 selects auctions whose id matches a sampling predicate.
_Q2_MODULUS = 123


def _bump(calc):
    calc._calculation_count += 1
    calc._last_calculation = datetime.now().isoformat()


class Q1CurrencyConvertCalculator(DataCalculator):
    """Nexmark Q1: map bid.price (USD) -> price_eur (EUR) at a fixed rate."""

    def calculate(self, data):
        if not isinstance(data, dict) or "price" not in data:
            return data
        out = dict(data)
        try:
            out["price_eur"] = round(float(data["price"]) * _USD_TO_EUR, 4)
        except (TypeError, ValueError):
            out["price_eur"] = None
        _bump(self)
        return out


class Q2AuctionFilterCalculator(DataCalculator):
    """Nexmark Q2: select bids whose auction id matches the sampling predicate.

    Expressed as a boolean ``selected`` flag (the linear bench DAG is 1-in/1-out);
    a real deployment would route on this flag or drop unselected rows at a sink.
    """

    def calculate(self, data):
        if not isinstance(data, dict):
            return data
        out = dict(data)
        auction = data.get("auction")
        out["selected"] = isinstance(auction, int) and (auction % _Q2_MODULUS == 0)
        _bump(self)
        return out
