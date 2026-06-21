"""Shared fixtures for integration and prod overlay tests."""

from __future__ import annotations

import os

import pytest


def api_headers() -> dict[str, str]:
    key = os.getenv("FLUXMETER_API_KEY")
    return {"X-API-Key": key} if key else {}


def admin_headers() -> dict[str, str]:
    key = os.getenv("FLUXMETER_ADMIN_KEY") or os.getenv("FLUXMETER_API_KEY")
    return {"X-API-Key": key} if key else {}


@pytest.fixture(scope="session")
def prod_mode() -> bool:
    return os.getenv("FLUXMETER_AUTH_OPTIONAL", "true").lower() == "false"
