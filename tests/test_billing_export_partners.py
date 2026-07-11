"""Partner billing export tests — Metronome / Orb / multi-target (mocked HTTP)."""

from __future__ import annotations

import sys
import time
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, "api")


class TestMultiPlatformCollection:
    def test_collects_metronome_linked_customer(self):
        from billing_export import collect_customer_usage

        mock_redis = MagicMock()
        mock_redis.get.side_effect = lambda k: {
            "billing:cust_1:metronome_customer_id": "mtr_abc",
            "customer:cust_1:event_count": "10",
            "customer:cust_1:input_tokens": "1000",
            "customer:cust_1:output_tokens": "500",
            "customer:cust_1:cost_usd": "1.5",
            "billing:cust_1:last_reported_events": "0",
            "billing:cust_1:last_reported_cost_usd": "0",
            "billing:cust_1:last_reported_input_tokens": "0",
            "billing:cust_1:last_reported_output_tokens": "0",
        }.get(k)

        usage = collect_customer_usage(mock_redis, "cust_1")
        assert usage is not None
        assert usage["metronome_customer_id"] == "mtr_abc"
        assert usage["new_events"] == 10
        assert usage["new_input_tokens"] == 1000
        assert usage["new_output_tokens"] == 500

    def test_skips_unlinked_customer(self):
        from billing_export import collect_customer_usage

        mock_redis = MagicMock()
        mock_redis.get.return_value = None
        assert collect_customer_usage(mock_redis, "orphan") is None


class TestIdempotencyKey:
    def test_hourly_key_contains_customer_and_mode(self):
        from billing_export import idempotency_key

        key = idempotency_key("cust_x", 1700000000)
        assert key.startswith("fluxmeter-cust_x-")
        assert key.endswith(f"-{__import__('billing_export').STRIPE_EXPORT_MODE}")


class TestMetronomeExport:
    @patch("billing_export.httpx.Client")
    def test_cost_mode_payload(self, mock_client_cls):
        from billing_export import report_to_metronome

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        import billing_export as be

        old_token = be.METRONOME_API_TOKEN
        old_mode = be.STRIPE_EXPORT_MODE
        be.METRONOME_API_TOKEN = "test-token"
        be.STRIPE_EXPORT_MODE = "cost"
        try:
            usage = {"new_cost_usd": 2.5, "new_events": 0, "new_input_tokens": 0, "new_output_tokens": 0}
            report_to_metronome(
                "mtr_1",
                usage,
                timestamp_iso="2026-07-01T00:00:00+00:00",
                transaction_id="fluxmeter-cust-202607-cost",
            )
            call_json = mock_client.post.call_args[1]["json"]
            assert call_json[0]["customer_id"] == "mtr_1"
            assert call_json[0]["properties"]["total_cost_usd"] == 2.5
            assert call_json[0]["transaction_id"] == "fluxmeter-cust-202607-cost"
        finally:
            be.METRONOME_API_TOKEN = old_token
            be.STRIPE_EXPORT_MODE = old_mode


class TestOrbExport:
    @patch("billing_export.httpx.Client")
    def test_events_mode_splits_input_output(self, mock_client_cls):
        from billing_export import report_to_orb

        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        import billing_export as be

        old_key = be.ORB_API_KEY
        old_mode = be.STRIPE_EXPORT_MODE
        be.ORB_API_KEY = "orb-key"
        be.STRIPE_EXPORT_MODE = "events"
        try:
            usage = {
                "new_cost_usd": 0,
                "new_events": 2,
                "new_input_tokens": 100,
                "new_output_tokens": 50,
            }
            report_to_orb(
                "orb_cust",
                usage,
                timestamp_iso="2026-07-01T00:00:00+00:00",
                idempotency_key="fluxmeter-cust-202607-events",
            )
            events = mock_client.post.call_args[1]["json"]["events"]
            assert len(events) == 2
            assert events[0]["properties"]["token_type"] == "input"
            assert events[1]["properties"]["tokens"] == 50
        finally:
            be.ORB_API_KEY = old_key
            be.STRIPE_EXPORT_MODE = old_mode


class TestExportCustomerToTargets:
    @patch("billing_export.report_to_stripe")
    @patch("billing_export.report_to_metronome")
    def test_multi_target_marks_reported(self, mock_metro, mock_stripe):
        from billing_export import export_customer_to_targets

        mock_redis = MagicMock()
        usage = {
            "customer_id": "cust_1",
            "stripe_customer_id": "cus_x",
            "metronome_customer_id": "mtr_x",
            "orb_customer_id": None,
            "new_events": 5,
            "total_events": 5,
            "new_input_tokens": 100,
            "new_output_tokens": 50,
            "total_input_tokens": 100,
            "total_output_tokens": 50,
            "new_cost_usd": 0.5,
            "total_cost_usd": 0.5,
        }
        n = export_customer_to_targets(
            mock_redis, usage, ["stripe", "metronome"], now=int(time.time())
        )
        assert n == 2
        mock_stripe.assert_called_once()
        mock_metro.assert_called_once()
        mock_redis.set.assert_any_call("billing:cust_1:last_reported_events", "5")


class TestDiscoverBillable:
    def test_discovers_across_platforms(self):
        from billing_export import discover_billable_customers

        mock_redis = MagicMock()
        mock_redis.scan.side_effect = [
            (0, ["billing:alice:stripe_customer_id"]),
            (0, ["billing:bob:metronome_customer_id"]),
            (0, []),
        ]
        customers = discover_billable_customers(mock_redis)
        assert "alice" in customers
        assert "bob" in customers
