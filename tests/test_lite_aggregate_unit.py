"""Unit tests for lite_aggregate_lua — no HTTP, direct Redis + LiteAggregator.

Run: pytest tests/test_lite_aggregate_unit.py -v
Requires: Redis on localhost:6379 (lite or full stack).
"""

from __future__ import annotations

import sys
import uuid

import pytest
import redis

sys.path.insert(0, "api")

from lite_aggregate_lua import (  # noqa: E402
    LiteAggregator,
    calculate_cost_micro,
    normalize_model_id,
)


@pytest.fixture
def r():
    try:
        conn = redis.Redis(host="localhost", port=6379, decode_responses=True)
        conn.ping()
    except redis.ConnectionError:
        pytest.skip("Redis not available on localhost:6379")
    return conn


@pytest.fixture
def agg(r):
    return LiteAggregator(r)


class TestPricingHelpers:
    def test_normalize_model_id_version_suffix(self):
        assert normalize_model_id("gpt-4o-2024-08-06") == "gpt-4o"

    def test_calculate_cost_micro_gpt4o(self):
        cost = calculate_cost_micro({
            "modelId": "gpt-4o",
            "inputTokens": 1_000_000,
            "outputTokens": 0,
        })
        assert cost == 2_500_000


class TestLiteAggregator:
    def test_rejects_missing_customer(self, agg):
        result = agg.aggregate({"modelId": "gpt-4o", "inputTokens": 1})
        assert result["status"] == "rejected"

    def test_duplicate_event_id(self, agg, r):
        cid = f"unit_idemp_{uuid.uuid4().hex[:8]}"
        eid = str(uuid.uuid4())
        event = {
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 100,
            "outputTokens": 0,
            "eventId": eid,
        }
        first = agg.aggregate(event)
        second = agg.aggregate(event)
        assert first["status"] == "ok"
        assert second["status"] == "duplicate"
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 1

    def test_normalized_model_key(self, agg, r):
        cid = f"unit_model_{uuid.uuid4().hex[:8]}"
        event = {
            "customerId": cid,
            "modelId": "gpt-4o-2024-08-06",
            "inputTokens": 50,
            "outputTokens": 0,
            "eventId": str(uuid.uuid4()),
        }
        agg.aggregate(event)
        assert r.get(f"customer:{cid}:model:gpt-4o:input_tokens") == "50"

    def test_inline_budget_deduction(self, agg, r):
        cid = f"unit_budget_{uuid.uuid4().hex[:8]}"
        r.set(f"budget:{cid}:balance_usd", "10.0")
        event = {
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 1000,
            "outputTokens": 0,
            "eventId": str(uuid.uuid4()),
        }
        result = agg.aggregate(event)
        assert result["status"] == "ok"
        assert result["balance_usd"] < 10.0
        assert float(r.get(f"budget:{cid}:balance_usd") or 0) == result["balance_usd"]

    def test_budget_exhausted_alert(self, agg, r):
        cid = f"unit_exhaust_{uuid.uuid4().hex[:8]}"
        r.set(f"budget:{cid}:balance_usd", "0.0001")
        event = {
            "customerId": cid,
            "modelId": "gpt-4o",
            "inputTokens": 10_000,
            "outputTokens": 5_000,
            "eventId": str(uuid.uuid4()),
        }
        result = agg.aggregate(event)
        assert result["status"] == "ok"
        assert result.get("budget_alert") == "BUDGET_EXHAUSTED"
        assert float(r.get(f"budget:{cid}:balance_usd") or 0) == 0.0

    def test_aggregate_batch(self, agg, r):
        cid = f"unit_batch_{uuid.uuid4().hex[:8]}"
        events = [
            {
                "customerId": cid,
                "modelId": "gpt-4o-mini",
                "inputTokens": 10,
                "outputTokens": 0,
                "eventId": str(uuid.uuid4()),
            }
            for _ in range(3)
        ]
        results = agg.aggregate_batch(events)
        assert len(results) == 3
        assert all(r_["status"] == "ok" for r_ in results)
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 3

    def test_tenant_isolated_counters(self, agg, r):
        """Same customerId under different tenantId → separate Redis keys."""
        cid = f"unit_tenant_{uuid.uuid4().hex[:8]}"
        tid_a = f"ta_{uuid.uuid4().hex[:6]}"
        tid_b = f"tb_{uuid.uuid4().hex[:6]}"
        base = {
            "customerId": cid,
            "modelId": "gpt-4o-mini",
            "inputTokens": 100,
            "outputTokens": 0,
        }
        agg.aggregate({**base, "tenantId": tid_a, "eventId": str(uuid.uuid4())})
        agg.aggregate({**base, "tenantId": tid_b, "eventId": str(uuid.uuid4())})
        key_a = f"tenant:{tid_a}:customer:{cid}:event_count"
        key_b = f"tenant:{tid_b}:customer:{cid}:event_count"
        assert int(r.get(key_a) or 0) == 1
        assert int(r.get(key_b) or 0) == 1
        assert r.get(f"customer:{cid}:event_count") is None
