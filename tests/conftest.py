"""Shared fixtures for integration and prod overlay tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# Allow `from auth import ...` in unit tests (api/ on sys.path)
_API_DIR = Path(__file__).resolve().parents[1] / "api"
if str(_API_DIR) not in sys.path:
    sys.path.insert(0, str(_API_DIR))


def api_headers() -> dict[str, str]:
    key = os.getenv("FLUXMETER_API_KEY")
    return {"X-API-Key": key} if key else {}


def admin_headers() -> dict[str, str]:
    key = os.getenv("FLUXMETER_ADMIN_KEY") or os.getenv("FLUXMETER_API_KEY")
    return {"X-API-Key": key} if key else {}


@pytest.fixture(scope="session")
def prod_mode() -> bool:
    return os.getenv("FLUXMETER_AUTH_OPTIONAL", "true").lower() == "false"
