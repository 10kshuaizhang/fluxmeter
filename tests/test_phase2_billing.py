"""Phase 2 billing features — unit tests, no Stripe credentials."""

from __future__ import annotations

import sys
import time
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, "api")
sys.path.insert(0, "services/control-plane")


class TestBillingExportModes:
    def test_cost_export_value_in_cents(self):
        from billing_export import export_value

        usage = {"new_events": 100, "new_cost_usd": 1.234}
        assert export_value(usage) == 100

        import billing_export as be
        old = be.STRIPE_EXPORT_MODE
        be.STRIPE_EXPORT_MODE = "cost"
        try:
            assert export_value(usage) == 123
        finally:
            be.STRIPE_EXPORT_MODE = old

    def test_monthly_export_gate(self):
        from billing_export import should_run_export_cycle, mark_export_cycle
        from pricing_loader import billing_period_month

        r = MagicMock()
        r.get.return_value = billing_period_month(int(time.time() * 1000))

        import billing_export as be
        old = be.BILLING_EXPORT_PERIOD
        be.BILLING_EXPORT_PERIOD = "monthly"
        try:
            assert should_run_export_cycle(r) is False
            mark_export_cycle(r)
            r.set.assert_called()
        finally:
            be.BILLING_EXPORT_PERIOD = old


class TestPackageDrawdown:
    def test_package_exhausted_rejects(self):
        import redis
        try:
            r = redis.Redis(host="localhost", port=6379, decode_responses=True)
            r.ping()
        except redis.ConnectionError:
            pytest.skip("Redis not available")

        import json
        import uuid
        from pathlib import Path

        from lite_aggregate_lua import LiteAggregator
        from pricing_loader import PricingCatalog, reload_catalog

        reload_catalog(PricingCatalog.load_from_file())
        agg = LiteAggregator(r)
        cid = f"pkg_{uuid.uuid4().hex[:8]}"
        r.set(f"package:{cid}:tokens_remaining", "50")

        result = agg.aggregate({
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 100,
            "outputTokens": 0,
            "eventId": str(uuid.uuid4()),
        })
        assert result["status"] == "rejected"
        assert result["reason"] == "package_exhausted"


class TestStripeCheckout:
    def test_create_checkout_session_mocked(self):
        from unittest.mock import patch, MagicMock
        from stripe_billing import create_checkout_session

        mock_stripe = MagicMock()
        mock_stripe.checkout.Session.create.return_value = MagicMock(url="https://checkout.stripe.test/s")
        with patch("stripe_billing.stripe", mock_stripe):
            with patch.dict("os.environ", {"STRIPE_GROWTH_PRICE_ID": "price_growth"}):
                url = create_checkout_session("cus_x", "growth", "http://ok", "http://cancel")
        assert url == "https://checkout.stripe.test/s"
