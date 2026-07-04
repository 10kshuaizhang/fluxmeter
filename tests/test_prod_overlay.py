"""Production overlay smoke tests — API auth + Redis password."""

import httpx
import pytest

from conftest import admin_headers, api_headers
from helpers import API, TIMEOUT


class TestProdAuth:
    def test_health_no_auth_required(self):
        resp = httpx.get(f"{API}/health", timeout=TIMEOUT)
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_ingest_requires_api_key_when_auth_enforced(self):
        resp = httpx.post(
            f"{API}/ingest",
            json={
                "customerId": "auth_test",
                "modelId": "gpt-4o",
                "inputTokens": 1,
                "outputTokens": 1,
            },
            timeout=TIMEOUT,
        )
        if not api_headers():
            pytest.skip("FLUXMETER_API_KEY not set — demo mode")
        assert resp.status_code == 401

    def test_ingest_with_api_key_succeeds(self):
        headers = api_headers()
        if not headers:
            pytest.skip("FLUXMETER_API_KEY not set — demo mode")
        resp = httpx.post(
            f"{API}/ingest",
            json={
                "customerId": "auth_test_ok",
                "modelId": "gpt-4o",
                "inputTokens": 1,
                "outputTokens": 1,
            },
            timeout=TIMEOUT,
            headers=headers,
        )
        assert resp.status_code == 202

    def test_budget_set_requires_admin_key(self):
        admin = admin_headers()
        api = api_headers()
        if not admin or not api:
            pytest.skip("API keys not set — demo mode")
        if admin == api:
            pytest.skip("ADMIN_KEY same as API_KEY in this env")

        resp = httpx.post(
            f"{API}/budget/auth_test_cust",
            json={"balance_usd": 10.0},
            timeout=TIMEOUT,
            headers=api,
        )
        assert resp.status_code == 401

        resp = httpx.post(
            f"{API}/budget/auth_test_cust",
            json={"balance_usd": 10.0},
            timeout=TIMEOUT,
            headers=admin,
        )
        assert resp.status_code == 200

    def test_usage_query_with_api_key(self):
        headers = api_headers()
        if not headers:
            pytest.skip("FLUXMETER_API_KEY not set — demo mode")
        resp = httpx.get(f"{API}/usage/global", timeout=TIMEOUT, headers=headers)
        assert resp.status_code == 200
        assert "total_tokens" in resp.json()
