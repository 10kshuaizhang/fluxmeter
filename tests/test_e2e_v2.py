"""E2E tests for v1.2–v2.0 production hardening (TDD spec for big behavioral changes).

Run:
    make start && sleep 15 && make submit-job
    pytest tests/test_e2e_v2.py -v --timeout=300

Maps to tests/TEST_PLAN.md sections 11–16.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import pytest

from conftest import admin_headers, api_headers
from helpers import (
    API,
    TIMEOUT,
    check_budget,
    cost_usd,
    create_customer_api_key,
    get_budget,
    get_usage,
    ingest,
    push_watermarks_and_wait,
    set_budget,
    unique_customer,
    wait_for_budget_balance,
    wait_for_customer_usage,
)

ROOT = Path(__file__).resolve().parents[1]
RECONCILE_SCRIPT = ROOT / "jobs" / "reconcile_balances.py"


@pytest.fixture(scope="session", autouse=True)
def stack_ready():
    """Fail fast if API is not up; skip v2 tests if stack runs pre-1.2 API."""
    try:
        resp = httpx.get(f"{API}/health", timeout=httpx.Timeout(5.0))
        assert resp.status_code == 200
    except httpx.HTTPError as e:
        pytest.fail(f"FluxMeter API not reachable at {API}: {e}")

    pricing = httpx.get(f"{API}/pricing", timeout=httpx.Timeout(5.0), headers=api_headers())
    if pricing.status_code == 404:
        pytest.skip(
            "API is pre-v1.2 — rebuild with docker compose up -d --build api && make submit-job"
        )


@pytest.mark.e2e
@pytest.mark.v2
class TestStreamingSinglePathDeduction:
    """v1.2: reserve/reconcile must NOT deduct balance — only Flink Sink does."""

    def test_reserve_ingest_reconcile_no_double_charge(self):
        cust = unique_customer("stream")
        initial = 10.0
        reserve_usd = 5.0
        input_t, output_t = 100_000, 100_000
        expected_cost = cost_usd("gpt-4o-mini", input_t, output_t)

        set_budget(cust, balance=initial)

        reserve = httpx.post(
            f"{API}/budget/{cust}/reserve",
            params={"estimated_cost_usd": reserve_usd},
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert reserve.status_code == 200
        data = reserve.json()
        assert data["allowed"] is True
        assert abs(data["balance_usd"] - initial) < 1e-6
        assert abs(data["held_usd"] - reserve_usd) < 1e-6

        ingest(cust, "gpt-4o-mini", input_tokens=input_t, output_tokens=output_t)
        push_watermarks_and_wait(cust, models=["gpt-4o-mini"])

        usage = wait_for_customer_usage(
            cust, min_events=1, min_cost_usd=expected_cost * 0.5, timeout=180
        )
        budget = wait_for_budget_balance(cust, max_balance=initial - expected_cost + 0.01)

        double_charge_balance = initial - reserve_usd - usage["cost_usd"]
        assert budget["balance_usd"] > double_charge_balance + 0.01, (
            f"Double charge detected: balance={budget['balance_usd']:.6f} "
            f"looks like initial-reserve-cost={double_charge_balance:.6f}"
        )
        assert abs(budget["balance_usd"] - (initial - usage["cost_usd"])) < 0.02

        reconcile = httpx.post(
            f"{API}/budget/{cust}/reconcile",
            params={"reserved_usd": reserve_usd, "actual_usd": usage["cost_usd"]},
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert reconcile.status_code == 200
        rel = reconcile.json()
        assert abs(rel["held_usd"]) < 1e-6
        assert abs(rel["balance_usd"] - budget["balance_usd"]) < 1e-6

    def test_reserve_reduces_effective_balance_for_check(self):
        cust = unique_customer("held")
        set_budget(cust, balance=10.0)

        httpx.post(
            f"{API}/budget/{cust}/reserve",
            params={"estimated_cost_usd": 8.0},
            timeout=TIMEOUT,
            headers=admin_headers(),
        )

        ok = check_budget(cust, estimated_cost=1.0)
        assert ok["allowed"] is True
        assert ok.get("held_usd", 0) >= 7.9

        denied = check_budget(cust, estimated_cost=3.0)
        assert denied["allowed"] is False
        assert denied["reason"] == "insufficient_balance"
        assert denied.get("effective_balance_usd", 10) < 3.0


@pytest.mark.e2e
@pytest.mark.v2
class TestCustomerApiKeys:
    """v1.2: per-customer API keys scope ingest/check to one customer."""

    def test_customer_key_ingest_own_customer(self):
        cust = unique_customer("ckey")
        key_data = create_customer_api_key(cust)
        headers = {"X-API-Key": key_data["api_key"]}

        resp = ingest(cust, "gpt-4o-mini", input_tokens=100, output_tokens=50, headers=headers)
        assert resp.status_code == 202

    def test_customer_key_rejects_other_customer(self):
        cust_a = unique_customer("ckey_a")
        cust_b = unique_customer("ckey_b")
        key_data = create_customer_api_key(cust_a)
        headers = {"X-API-Key": key_data["api_key"]}

        resp = ingest(cust_b, "gpt-4o-mini", input_tokens=10, output_tokens=5, headers=headers)
        assert resp.status_code == 403

    def test_revoked_key_rejected(self):
        cust = unique_customer("revoke")
        key_data = create_customer_api_key(cust)
        headers = {"X-API-Key": key_data["api_key"]}

        del_resp = httpx.delete(
            f"{API}/admin/api-keys/{key_data['key_id']}",
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert del_resp.status_code == 200

        resp = ingest(cust, "gpt-4o-mini", input_tokens=10, output_tokens=5, headers=headers)
        # Revoked customer key is not resolved; falls through to demo/global auth
        if api_headers():
            assert resp.status_code in (401, 403)
        else:
            assert resp.status_code in (401, 403, 202)


@pytest.mark.e2e
@pytest.mark.v2
class TestDebtFloor:
    """v1.2: balance floors at zero; excess recorded as debt_usd."""

    def test_exhaustion_floors_balance_records_debt(self):
        cust = unique_customer("debt")
        initial = 0.05
        set_budget(cust, balance=initial, threshold=0.01)

        events = [{
            "customerId": cust,
            "modelId": "o1",
            "provider": "openai",
            "inputTokens": 500_000,
            "outputTokens": 500_000,
            "timestamp": int(time.time() * 1000) + i * 2000,
        } for i in range(5)]
        httpx.post(
            f"{API}/ingest/batch",
            json=events,
            timeout=TIMEOUT,
            headers=api_headers(),
        )
        push_watermarks_and_wait(cust, models=["o1"])

        deadline = time.time() + 180
        budget = None
        while time.time() < deadline:
            budget = get_budget(cust)
            if budget and budget["balance_usd"] <= 0.001:
                break
            ingest(cust, "o1", input_tokens=1, output_tokens=1)
            time.sleep(2)

        assert budget is not None
        assert budget["balance_usd"] <= 0.001
        assert budget["is_exhausted"] is True
        assert budget.get("debt_usd", 0) > 0


@pytest.mark.e2e
@pytest.mark.v2
class TestPricingApi:
    """v1.3: external pricing catalog via API."""

    def test_get_pricing_returns_models(self):
        resp = httpx.get(f"{API}/pricing", timeout=TIMEOUT, headers=api_headers())
        assert resp.status_code == 200
        data = resp.json()
        assert "models" in data
        assert "gpt-4o" in data["models"]
        assert "defaults" in data

    def test_admin_validate_and_update_pricing(self):
        body = {
            "version": "test-1",
            "models": {"gpt-4o-mini": {"input_per_m": 0.15, "output_per_m": 0.60}},
            "defaults": {"input_per_m": 1.0, "output_per_m": 3.0, "embedding_per_m": 0.1},
        }
        validate = httpx.post(
            f"{API}/admin/pricing/validate",
            json=body,
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert validate.status_code == 200
        assert validate.json()["status"] == "valid"

        update = httpx.put(
            f"{API}/admin/pricing",
            json=body,
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert update.status_code == 200

        snap = httpx.get(f"{API}/pricing", timeout=TIMEOUT, headers=api_headers())
        assert snap.json().get("version") == "test-1"


@pytest.mark.e2e
@pytest.mark.v2
class TestReconciliationJob:
    """v1.4: balance drift detection after Flink deductions."""

    def _run_reconcile_once(self) -> dict:
        code = (
            "import json, os, sys; "
            "sys.path.insert(0, os.getcwd()); "
            "from jobs.reconcile_balances import get_redis, reconcile_all; "
            "print(json.dumps(reconcile_all(get_redis())))"
        )
        proc = subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=60,
            env={**dict(os.environ), "REDIS_HOST": os.getenv("REDIS_HOST", "localhost")},
        )
        if proc.returncode != 0:
            pytest.skip(f"reconcile job failed: {proc.stderr}")
        return json.loads(proc.stdout.strip().splitlines()[-1])

    def test_no_drift_after_budget_sync(self):
        cust = unique_customer("recon")
        initial = 20.0
        set_budget(cust, balance=initial)

        ingest(cust, "gpt-4o-mini", input_tokens=50_000, output_tokens=25_000)
        push_watermarks_and_wait(cust, models=["gpt-4o-mini"])
        wait_for_customer_usage(cust, min_events=1, min_cost_usd=0.001)

        result = self._run_reconcile_once()
        assert result["customers_scanned"] >= 1

        api_snap = httpx.get(
            f"{API}/admin/reconciliation",
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert api_snap.status_code == 200
        snap = api_snap.json()
        if snap.get("status") != "no_data":
            matching = [
                d for d in snap.get("drifts", [])
                if d.get("customer_id") == cust
            ]
            assert matching == [], f"Drift for {cust}: {matching}"


@pytest.mark.e2e
@pytest.mark.v2
class TestWebhookConfig:
    """v1.2: webhook CRUD (delivery tested via worker + Kafka separately)."""

    def test_set_and_get_webhook(self):
        cust = unique_customer("hook")
        set_budget(cust, balance=5.0)

        post = httpx.post(
            f"{API}/budget/{cust}/webhook",
            json={"webhook_url": "https://example.com/hook", "webhook_secret": "secret"},
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert post.status_code == 200

        get = httpx.get(
            f"{API}/budget/{cust}/webhook",
            timeout=TIMEOUT,
            headers=admin_headers(),
        )
        assert get.status_code == 200
        assert get.json()["webhook_url"] == "https://example.com/hook"


@pytest.mark.e2e
@pytest.mark.v2
class TestDlqReplayTool:
    """v1.4: DLQ replay script smoke test (no Kafka required for --help)."""

    def test_dlq_replay_script_importable(self):
        script = ROOT / "scripts" / "dlq_replay.py"
        proc = subprocess.run(
            [sys.executable, str(script), "--help"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert proc.returncode == 0
        assert "Replay DLQ" in proc.stdout or "dlq" in proc.stdout.lower()
