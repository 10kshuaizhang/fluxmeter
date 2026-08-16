"""Gateway estimates must use catalog cost_micro (no tier_at_token shortcut)."""

from __future__ import annotations

import sys

import pytest

sys.path.insert(0, "api")

from gateway.pricing_estimate import estimate_request_cost, estimate_stream_cost
from pricing_loader import PricingCatalog, reload_catalog


def test_estimate_stream_matches_catalog_cost_micro():
    reload_catalog(PricingCatalog.load_from_file())
    model = "gpt-4o"
    inp, out = 1000, 500
    via_estimate = estimate_stream_cost(model, inp, out)
    micro = PricingCatalog.load_from_file().calculate_cost_micro(
        {"modelId": model, "inputTokens": inp, "outputTokens": out},
        monthly_tokens_before=0,
    )
    assert via_estimate == pytest.approx(micro / 1_000_000)


def test_estimate_request_positive():
    assert estimate_request_cost("gpt-4o-mini", 128) > 0
