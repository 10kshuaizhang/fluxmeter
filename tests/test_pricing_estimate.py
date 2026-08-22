"""Gateway estimates must use catalog cost_micro (no tier_at_token shortcut)."""

from __future__ import annotations

import sys

import pytest

sys.path.insert(0, "api")

from pricing_loader import PricingCatalog, reload_catalog


def test_estimate_stream_matches_catalog_cost_micro():
    reload_catalog(PricingCatalog.load_from_file())
    model = "gpt-4o"
    inp, out = 1000, 500
    catalog = PricingCatalog.load_from_file()
    via_estimate = catalog.quote_usd(model, input_tokens=inp, output_tokens=out)
    micro = catalog.calculate_cost_micro(
        {"modelId": model, "inputTokens": inp, "outputTokens": out},
        monthly_tokens_before=0,
    )
    assert via_estimate == pytest.approx(micro / 1_000_000)


def test_estimate_request_positive():
    assert PricingCatalog.load_from_file().estimate_completion_usd(
        "gpt-4o-mini", max_output_tokens=128
    ) > 0
