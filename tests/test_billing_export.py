"""Billing export unit tests — Stripe integration with mocked API.

Run with: pytest tests/test_billing_export.py -v
Does NOT require Stripe credentials or running infrastructure.
"""

import time
from unittest.mock import MagicMock, patch

import pytest


class TestUsageCollection:
    """Test usage data collection from Redis."""

    def test_collects_per_customer_usage(self):
        """Reads customer counters and builds Stripe meter event payload."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import collect_customer_usage

        mock_redis = MagicMock()
        mock_redis.get.side_effect = lambda k: {
            "customer:cust_1:total_tokens": "50000",
            "customer:cust_1:event_count": "100",
            "customer:cust_1:cost_usd": "2.50",
            "billing:cust_1:stripe_customer_id": "cus_abc123",
            "billing:cust_1:last_reported_events": "50",
            "billing:cust_1:last_reported_cost_usd": "0",
        }.get(k)

        usage = collect_customer_usage(mock_redis, "cust_1")

        assert usage["stripe_customer_id"] == "cus_abc123"
        assert usage["new_events"] == 50  # 100 total - 50 already reported
        assert usage["total_cost_usd"] == 2.50
        assert usage["new_cost_usd"] == 2.50

    def test_skips_customer_without_stripe_id(self):
        """Customers not linked to Stripe are skipped."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import collect_customer_usage

        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        usage = collect_customer_usage(mock_redis, "cust_no_stripe")
        assert usage is None


class TestStripeReporting:
    """Test Stripe API interaction (mocked)."""

    @patch("billing_export.stripe")
    def test_reports_meter_event(self, mock_stripe):
        """Creates a Stripe billing meter event for usage."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import report_to_stripe

        report_to_stripe(
            stripe_customer_id="cus_abc123",
            event_name="token_events_processed",
            value=500,
            timestamp=int(time.time()),
        )

        mock_stripe.billing.MeterEvent.create.assert_called_once()
        call_kwargs = mock_stripe.billing.MeterEvent.create.call_args[1]
        assert call_kwargs["event_name"] == "token_events_processed"
        assert call_kwargs["payload"]["stripe_customer_id"] == "cus_abc123"
        assert call_kwargs["payload"]["value"] == "500"

    @patch("billing_export.stripe")
    def test_skips_zero_usage(self, mock_stripe):
        """Does not report to Stripe if no new usage."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import report_to_stripe

        report_to_stripe(
            stripe_customer_id="cus_abc123",
            event_name="token_events_processed",
            value=0,
            timestamp=int(time.time()),
        )

        mock_stripe.billing.MeterEvent.create.assert_not_called()
