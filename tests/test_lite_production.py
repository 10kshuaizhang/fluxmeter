"""Production-grade lite aggregator tests.

Run with: pytest tests/test_lite_production.py -v --timeout=60
Requires: docker compose up (lite stack)
"""

import time
import uuid

import httpx
import pytest
import redis

API = "http://localhost:8000"
TIMEOUT = httpx.Timeout(10.0)


@pytest.fixture(scope="module")
def r():
    """Direct Redis connection for assertions."""
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


@pytest.fixture(autouse=True)
def health_check():
    """Ensure API is healthy before each test."""
    resp = httpx.get(f"{API}/health", timeout=TIMEOUT)
    assert resp.status_code == 200


def make_event(customer_id: str, model_id: str = "gpt-4o",
               input_tokens: int = 1000, output_tokens: int = 500,
               event_id: str = None):
    return {
        "customerId": customer_id,
        "modelId": model_id,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "timestamp": int(time.time() * 1000),
        "eventId": event_id or str(uuid.uuid4()),
    }


class TestAtomicAggregation:
    """Verify Lua-based aggregation is atomic (all-or-nothing)."""

    def test_single_event_increments_all_counters(self, r):
        cid = f"test_atomic_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, input_tokens=1000, output_tokens=500)

        resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp.status_code == 202

        # All counters updated atomically
        assert int(r.get(f"customer:{cid}:input_tokens") or 0) == 1000
        assert int(r.get(f"customer:{cid}:output_tokens") or 0) == 500
        assert int(r.get(f"customer:{cid}:total_tokens") or 0) == 1500
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 1
        assert float(r.get(f"customer:{cid}:cost_usd") or 0) > 0

    def test_batch_ingest_all_counted(self, r):
        cid = f"test_batch_{uuid.uuid4().hex[:8]}"
        events = [make_event(cid, input_tokens=100, output_tokens=50) for _ in range(10)]

        resp = httpx.post(f"{API}/ingest/batch", json=events, timeout=TIMEOUT)
        assert resp.status_code == 202

        assert int(r.get(f"customer:{cid}:input_tokens") or 0) == 1000
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 10


class TestIdempotency:
    """Verify duplicate events are rejected."""

    def test_duplicate_event_id_rejected(self, r):
        cid = f"test_idemp_{uuid.uuid4().hex[:8]}"
        eid = str(uuid.uuid4())
        event = make_event(cid, event_id=eid, input_tokens=500)

        # First ingest succeeds
        resp1 = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp1.status_code == 202

        # Second ingest with same eventId is accepted (202) but not double-counted
        resp2 = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp2.status_code == 202

        # Only counted once
        assert int(r.get(f"customer:{cid}:input_tokens") or 0) == 500
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 1

    def test_no_event_id_always_counted(self, r):
        """Events without eventId are always counted (fire-and-forget mode)."""
        cid = f"test_no_eid_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, input_tokens=100)
        del event["eventId"]

        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        assert int(r.get(f"customer:{cid}:event_count") or 0) == 2


class TestInlineBudgetDeduction:
    """Verify budget is deducted atomically with aggregation in lite mode."""

    def test_budget_deducted_on_ingest(self, r):
        cid = f"test_budget_{uuid.uuid4().hex[:8]}"

        # Set a budget
        resp = httpx.post(f"{API}/budget/{cid}",
                          json={"balance_usd": 100.0, "threshold_pct": 20},
                          timeout=TIMEOUT)
        assert resp.status_code == 200

        # Ingest event (should deduct from budget)
        event = make_event(cid, model_id="gpt-4o", input_tokens=1000, output_tokens=500)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        # Budget decreased
        balance = float(r.get(f"budget:{cid}:balance_usd") or 0)
        assert balance < 100.0
        assert balance > 0  # Not fully exhausted

    def test_budget_check_reflects_inline_deduction(self, r):
        cid = f"test_check_{uuid.uuid4().hex[:8]}"

        # Set budget to $1.00
        httpx.post(f"{API}/budget/{cid}",
                   json={"balance_usd": 1.0, "threshold_pct": 50},
                   timeout=TIMEOUT)

        # Ingest expensive event (claude-opus-4: $15/M input + $75/M output)
        # 10000 input + 5000 output = $0.15 + $0.375 = $0.525
        event = make_event(cid, model_id="claude-opus-4",
                           input_tokens=10000, output_tokens=5000)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        # Budget check shows reduced balance
        resp = httpx.get(f"{API}/budget/{cid}/check", timeout=TIMEOUT)
        data = resp.json()
        assert data["balance_usd"] < 1.0
        assert data["allowed"] is True  # Still has funds

    def test_exhausted_budget_blocks_check(self, r):
        cid = f"test_exhaust_{uuid.uuid4().hex[:8]}"

        # Set tiny budget ($0.001)
        httpx.post(f"{API}/budget/{cid}",
                   json={"balance_usd": 0.001, "threshold_pct": 90},
                   timeout=TIMEOUT)

        # Ingest event that costs more than budget
        event = make_event(cid, model_id="gpt-4o",
                           input_tokens=10000, output_tokens=5000)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        # Budget check denies (balance at zero or negative capped)
        resp = httpx.get(f"{API}/budget/{cid}/check",
                         params={"estimated_cost_usd": "0.01"},
                         timeout=TIMEOUT)
        data = resp.json()
        assert data["allowed"] is False
