"""Production-grade lite aggregator tests.

Run with: pytest tests/test_lite_production.py -v --timeout=60
Requires: docker compose up (lite stack)
"""

import time
import uuid

import httpx
import pytest
import redis

from helpers import API, TIMEOUT


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
               event_id: str = None, tenant_id: str = None):
    event = {
        "customerId": customer_id,
        "modelId": model_id,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "timestamp": int(time.time() * 1000),
        "eventId": event_id or str(uuid.uuid4()),
    }
    if tenant_id:
        event["tenantId"] = tenant_id
    return event


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


class TestTenantIsolation:
    """Lite ingest honors tenantId for Redis key prefixes (Phase 1 E2E)."""

    def test_tenant_id_scopes_ingest_counters(self, r):
        cid = f"test_tenant_{uuid.uuid4().hex[:8]}"
        tid_a = f"ta_{uuid.uuid4().hex[:6]}"
        tid_b = f"tb_{uuid.uuid4().hex[:6]}"

        resp_a = httpx.post(
            f"{API}/ingest",
            json=make_event(cid, input_tokens=100, tenant_id=tid_a),
            timeout=TIMEOUT,
        )
        resp_b = httpx.post(
            f"{API}/ingest",
            json=make_event(cid, input_tokens=200, tenant_id=tid_b),
            timeout=TIMEOUT,
        )
        assert resp_a.status_code == 202
        assert resp_b.status_code == 202

        assert int(r.get(f"tenant:{tid_a}:customer:{cid}:input_tokens") or 0) == 100
        assert int(r.get(f"tenant:{tid_b}:customer:{cid}:input_tokens") or 0) == 200
        assert r.get(f"customer:{cid}:input_tokens") is None


class TestBillingQueries:
    """v2.6.1 period/day/session billing query endpoints."""

    def test_session_aggregates_on_ingest(self, r):
        cid = f"test_sess_{uuid.uuid4().hex[:8]}"
        sid = f"sess_{uuid.uuid4().hex[:8]}"

        for model, inp, out, reasoning in (
            ("gpt-4o", 1000, 400, 0),
            ("claude-sonnet-4", 600, 200, 100),
        ):
            event = make_event(cid, model_id=model, input_tokens=inp, output_tokens=out)
            event["sessionId"] = sid
            if reasoning:
                event["reasoningTokens"] = reasoning
            resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
            assert resp.status_code == 202

        resp = httpx.get(f"{API}/usage/session/{sid}", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert data["customer_id"] == cid
        assert data["event_count"] == 2
        assert data["input_tokens"] == 1600
        assert data["output_tokens"] == 600
        assert data["reasoning_tokens"] == 100
        assert data["total_tokens"] == 2300  # input + output + reasoning
        assert data["cost_usd"] > 0

    def test_span_aggregates_on_ingest(self, r):
        cid = f"test_span_{uuid.uuid4().hex[:8]}"
        span_id = f"job_{uuid.uuid4().hex[:8]}"
        base_ts = int(time.time() * 1000)

        for i, (model, inp, out) in enumerate(
            (("gpt-4o", 1000, 400), ("gpt-4o-mini", 600, 200), ("claude-haiku-4", 300, 100))
        ):
            event = make_event(cid, model_id=model, input_tokens=inp, output_tokens=out)
            event["parentSpanId"] = span_id
            event["timestamp"] = base_ts + i * 5000
            resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
            assert resp.status_code == 202

        resp = httpx.get(f"{API}/usage/span/{span_id}", timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert data["customer_id"] == cid
        assert data["call_count"] == 3
        assert data["total_tokens"] == 2600  # (1000+400) + (600+200) + (300+100)
        assert data["cost_usd"] > 0
        assert data["duration_ms"] == 10000  # 2 gaps × 5000ms

        top = httpx.get(f"{API}/usage/customer/{cid}/spans?limit=5", timeout=TIMEOUT)
        assert top.status_code == 200
        spans = top.json()
        assert any(s["span_id"] == span_id for s in spans)

    def test_span_not_found_404(self):
        resp = httpx.get(f"{API}/usage/span/span_nonexistent_{uuid.uuid4().hex}", timeout=TIMEOUT)
        assert resp.status_code == 404

    def test_session_not_found_404(self):
        resp = httpx.get(f"{API}/usage/session/sess_nonexistent_{uuid.uuid4().hex}", timeout=TIMEOUT)
        assert resp.status_code == 404

    def test_period_and_day_after_rollup(self, r):
        import sys
        sys.path.insert(0, "api")
        from pricing_loader import billing_period_day, billing_period_month
        from rollup_worker import rollup_customer_minute

        cid = f"test_period_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, input_tokens=2000, output_tokens=800)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        now_ms = int(time.time() * 1000)
        period = billing_period_month(now_ms)
        day = billing_period_day(now_ms)
        rollup_customer_minute(r, cid, int(time.time()))

        period_resp = httpx.get(f"{API}/usage/customer/{cid}/period/{period}", timeout=TIMEOUT)
        assert period_resp.status_code == 200
        period_data = period_resp.json()
        assert period_data["bucket"] == period
        assert period_data["input_tokens"] == 2000
        assert period_data["event_count"] == 1

        day_resp = httpx.get(f"{API}/usage/customer/{cid}/day/{day}", timeout=TIMEOUT)
        assert day_resp.status_code == 200
        day_data = day_resp.json()
        assert day_data["bucket"] == day
        assert day_data["input_tokens"] == 2000

    def test_period_invalid_format_400(self):
        resp = httpx.get(f"{API}/usage/customer/cust_x/period/2026-7", timeout=TIMEOUT)
        assert resp.status_code == 400

    def test_day_invalid_format_400(self):
        resp = httpx.get(f"{API}/usage/customer/cust_x/day/2026-07-5", timeout=TIMEOUT)
        assert resp.status_code == 400

    def test_period_missing_404(self):
        resp = httpx.get(f"{API}/usage/customer/cust_missing/period/2099-01", timeout=TIMEOUT)
        assert resp.status_code == 404


class TestDomesticModelPricing:
    """v2.6.0 Chinese domestic models — lite ingest cost path."""

    def test_hunyuan_lite_zero_cost(self, r):
        cid = f"test_hy_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, model_id="hunyuan-lite", input_tokens=5000, output_tokens=2000)
        resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp.status_code == 202
        body = resp.json()
        assert body.get("cost_usd", -1) == 0.0
        assert float(r.get(f"customer:{cid}:cost_usd") or 0) == 0.0

    def test_deepseek_v4_flash_priced(self, r):
        cid = f"test_ds_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, model_id="deepseek-v4-flash", input_tokens=1_000_000, output_tokens=0)
        resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp.status_code == 202
        cost = float(r.get(f"customer:{cid}:cost_usd") or 0)
        assert 0.13 < cost < 0.15  # $0.14/M input
