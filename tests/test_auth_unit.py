"""Unit tests for API auth (no docker stack required)."""

import asyncio
import inspect

import pytest
from fastapi import HTTPException

import auth
from auth import require_api_key, require_customer_access


def test_optional_anonymous_auth_is_async_and_skips_identity_lookups(monkeypatch):
    """The benchmark hot path must not enter FastAPI's sync dependency pool."""
    monkeypatch.setattr(auth, "AUTH_OPTIONAL", True)
    monkeypatch.setattr(auth, "API_KEY", "")
    monkeypatch.setattr(auth, "ADMIN_API_KEY", "")
    monkeypatch.setattr(
        auth,
        "resolve_customer_from_key",
        lambda _key: pytest.fail("anonymous auth performed a customer-key lookup"),
    )
    monkeypatch.setattr(
        auth,
        "resolve_tenant_from_key",
        lambda _key: pytest.fail("anonymous auth performed a tenant-key lookup"),
    )

    assert inspect.iscoroutinefunction(require_api_key)
    asyncio.run(require_api_key(None))


class TestRequireCustomerAccess:
    def test_customer_key_mismatch_always_forbidden(self, monkeypatch):
        """Regression: customer key for A must not access B."""
        monkeypatch.setattr(
            "auth.resolve_customer_from_key",
            lambda k: "cust_a" if k == "fm_live_a" else None,
        )
        monkeypatch.setattr("auth.is_global_api_key", lambda k: False)

        with pytest.raises(HTTPException) as exc:
            require_customer_access("cust_b", x_api_key="fm_live_a")
        assert exc.value.status_code == 403

    def test_matching_customer_key_allowed_with_mock(self, monkeypatch):
        monkeypatch.setattr(
            "auth.resolve_customer_from_key",
            lambda k: "cust_a" if k == "fm_live_test" else None,
        )
        monkeypatch.setattr("auth.is_global_api_key", lambda k: False)
        monkeypatch.setenv("FLUXMETER_AUTH_OPTIONAL", "true")
        monkeypatch.setenv("FLUXMETER_API_KEY", "")
        monkeypatch.setenv("FLUXMETER_ADMIN_KEY", "")

        require_customer_access("cust_a", x_api_key="fm_live_test")

    def test_mismatch_customer_key_forbidden(self, monkeypatch):
        monkeypatch.setattr(
            "auth.resolve_customer_from_key",
            lambda k: "cust_a" if k == "fm_live_test" else None,
        )
        monkeypatch.setattr("auth.is_global_api_key", lambda k: False)

        with pytest.raises(HTTPException) as exc:
            require_customer_access("cust_b", x_api_key="fm_live_test")
        assert exc.value.status_code == 403

    def test_control_plane_tenant_key_may_ingest_tenant_customer(self, monkeypatch):
        monkeypatch.setattr("auth.is_global_api_key", lambda _k: False)
        monkeypatch.setattr("auth.resolve_customer_from_key", lambda _k: None)
        monkeypatch.setattr(
            "auth.resolve_tenant_from_key",
            lambda key: "tenant_a" if key == "cp_tenant_key" else None,
        )

        require_customer_access("cust_inside_tenant", x_api_key="cp_tenant_key")
