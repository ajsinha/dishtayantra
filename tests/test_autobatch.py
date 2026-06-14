"""Tests for A1 source-batching (BatchingSubscriptionNode / FlatteningPublicationNode).

Verifies the opt-in nodes: ordinary per-message in, ordinary per-message out,
output identical to an all-row pipeline, and that batching actually happened
inside the source node. Existing SubscriptionNode/PublicationNode are unchanged
(proven separately by diff + the rest of the suite).
"""

import pytest

pytest.importorskip("pyarrow")

from perftest.run_autobatch_example import run_example  # noqa: E402


def test_autobatch_per_message_contract_and_parity():
    r = run_example(trades=3000, quiet=True)

    # Per-message contract preserved on both ends.
    assert r["row_delivered"] == 3000
    assert r["autobatch_delivered"] == 3000

    # Identical per-trade output vs the all-row pipeline.
    assert r["identical"] is True
    assert r["max_abs_error"] == 0.0
    assert r["mismatches"] == 0

    # Batching actually happened: far fewer envelopes than messages.
    assert r["source_messages_in"] == 3000
    assert r["source_envelopes_out"] is not None
    assert r["source_envelopes_out"] < r["source_messages_in"]
