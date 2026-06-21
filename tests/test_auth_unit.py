"""Unit tests for API auth (no docker stack required)."""

import pytest
from fastapi import HTTPException

from auth import require_customer_access


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
