"""Shared Pricing Catalog golden vectors (Java + Python)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pricing_loader import PricingCatalog

VECTORS = Path("docs/contracts/pricing-vectors.json")


@pytest.mark.parametrize("case", json.loads(VECTORS.read_text()), ids=lambda c: c["name"])
def test_pricing_vector(case):
    catalog = PricingCatalog.load_from_file(case["catalog"])
    micro = catalog.calculate_cost_micro(
        case["event"], monthly_tokens_before=case.get("monthly_tokens_before", 0)
    )
    assert micro == case["expected_micro"]
