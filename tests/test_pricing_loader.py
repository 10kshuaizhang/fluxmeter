"""Unit tests for pricing_loader — no Redis/Docker."""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "api")

from pricing_loader import (  # noqa: E402
    PricingCatalog,
    billing_period_month,
    calculate_cost_micro,
    period_volume_key,
    reload_catalog,
)


def _load_tiered():
    path = Path("contrib/pricing/tiered-example.json")
    return PricingCatalog(json.loads(path.read_text()))


class TestPricingLoaderFlat:
    def setup_method(self):
        reload_catalog(PricingCatalog.load_from_file())

    def test_gpt4o_one_million_input(self):
        cost = calculate_cost_micro({"modelId": "gpt-4o", "inputTokens": 1_000_000})
        assert cost == 2_500_000

    def test_normalize_version_suffix(self):
        from pricing_loader import normalize_model_id

        assert normalize_model_id("gpt-4o-2024-08-06") == "gpt-4o"


class TestPricingLoaderVolume:
    def setup_method(self):
        reload_catalog(_load_tiered())

    def test_tier_one_while_under_cap(self):
        event = {"modelId": "gpt-4o-mini", "inputTokens": 1_000_000}
        assert calculate_cost_micro(event, 9_000_000) == 150_000

    def test_tier_two_after_cap(self):
        event = {"modelId": "gpt-4o-mini", "inputTokens": 1_000_000}
        assert calculate_cost_micro(event, 10_000_000) == 120_000


class TestPricingLoaderGraduated:
    def setup_method(self):
        reload_catalog(_load_tiered())

    def test_splits_at_boundary(self):
        event = {
            "modelId": "claude-sonnet-4",
            "inputTokens": 100_000,
            "outputTokens": 100_000,
        }
        assert calculate_cost_micro(event, 900_000) == 400_000


class TestPeriodKeys:
    def test_utc_calendar_month(self):
        ts = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)
        assert billing_period_month(ts) == "2026-07"

    def test_customer_model_scope(self):
        ts = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)
        key = period_volume_key(None, "cust1", "gpt-4o", ts)
        assert key == "customer:cust1:model:gpt-4o:period:2026-07:volume_tokens"

    def test_tenant_scoped(self):
        ts = int(datetime(2026, 7, 1, tzinfo=timezone.utc).timestamp() * 1000)
        key = period_volume_key("t1", "cust1", "gpt-4o", ts)
        assert key.startswith("tenant:t1:customer:cust1:model:gpt-4o:period:")
