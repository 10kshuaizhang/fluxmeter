"""API key authentication for FluxMeter endpoints."""

from __future__ import annotations

import logging
import os

from fastapi import Header, HTTPException

logger = logging.getLogger(__name__)

API_KEY = os.getenv("FLUXMETER_API_KEY", "")
ADMIN_API_KEY = os.getenv("FLUXMETER_ADMIN_KEY", "")
# When true (default), missing keys allow unauthenticated access (local demo only)
AUTH_OPTIONAL = os.getenv("FLUXMETER_AUTH_OPTIONAL", "true").lower() == "true"


def _check_key(provided: str | None, expected: str, label: str) -> None:
    if not expected:
        if AUTH_OPTIONAL:
            return
        raise HTTPException(
            status_code=503,
            detail=f"{label} not configured — set env var or FLUXMETER_AUTH_OPTIONAL=true for demo",
        )
    if not provided or provided != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Read/query/ingest endpoints."""
    _check_key(x_api_key, API_KEY, "FLUXMETER_API_KEY")


def require_admin_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Budget mutations, rerate, topup, reserve, reconcile."""
    if ADMIN_API_KEY:
        _check_key(x_api_key, ADMIN_API_KEY, "FLUXMETER_ADMIN_KEY")
    else:
        _check_key(x_api_key, API_KEY, "FLUXMETER_API_KEY")
