"""Control plane API tests.

Run with: pytest tests/test_control_plane.py -v --timeout=30
Requires: docker compose -f docker-compose.saas.yml up
"""

import uuid

import httpx
import pytest

CP_API = "http://localhost:8001"
TIMEOUT = httpx.Timeout(10.0)
ADMIN_KEY = "cp_admin_test_key"


@pytest.fixture(scope="module")
def admin_headers():
    return {"X-Admin-Key": ADMIN_KEY}


class TestTenantCRUD:
    """Tenant lifecycle management."""

    def test_create_tenant(self, admin_headers):
        resp = httpx.post(f"{CP_API}/tenants", json={
            "name": f"Test Corp {uuid.uuid4().hex[:6]}",
            "email": "admin@testcorp.example",
            "plan": "growth",
        }, headers=admin_headers, timeout=TIMEOUT)
        assert resp.status_code == 201
        data = resp.json()
        assert "tenant_id" in data
        assert "api_key" in data
        assert data["plan"] == "growth"

    def test_list_tenants(self, admin_headers):
        resp = httpx.get(f"{CP_API}/tenants", headers=admin_headers, timeout=TIMEOUT)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_get_tenant_usage(self, admin_headers):
        # Create a tenant first
        create = httpx.post(f"{CP_API}/tenants", json={
            "name": "Usage Corp",
            "email": "usage@test.example",
            "plan": "free",
        }, headers=admin_headers, timeout=TIMEOUT)
        tid = create.json()["tenant_id"]

        resp = httpx.get(f"{CP_API}/tenants/{tid}/usage",
                         headers=admin_headers, timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert "total_events" in data
        assert "total_cost_usd" in data


class TestPlanEnforcement:
    """Rate limiting per plan tier."""

    def test_free_plan_has_rate_limit(self, admin_headers):
        create = httpx.post(f"{CP_API}/tenants", json={
            "name": "Free Corp",
            "email": "free@test.example",
            "plan": "free",
        }, headers=admin_headers, timeout=TIMEOUT)
        data = create.json()
        assert data["limits"]["max_events_per_month"] == 100_000
        assert data["limits"]["max_eps"] == 100

    def test_growth_plan_has_higher_limits(self, admin_headers):
        create = httpx.post(f"{CP_API}/tenants", json={
            "name": "Growth Corp",
            "email": "growth@test.example",
            "plan": "growth",
        }, headers=admin_headers, timeout=TIMEOUT)
        data = create.json()
        assert data["limits"]["max_events_per_month"] == 10_000_000
        assert data["limits"]["max_eps"] == 10_000
