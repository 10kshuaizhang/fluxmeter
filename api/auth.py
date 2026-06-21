"""API key authentication for FluxMeter endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import uuid

import redis
from fastapi import Header, HTTPException

logger = logging.getLogger(__name__)

API_KEY = os.getenv("FLUXMETER_API_KEY", "")
ADMIN_API_KEY = os.getenv("FLUXMETER_ADMIN_KEY", "")
AUTH_OPTIONAL = os.getenv("FLUXMETER_AUTH_OPTIONAL", "true").lower() == "true"

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

_pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)


def _redis() -> redis.Redis:
    return redis.Redis(connection_pool=_pool)


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def _check_global_key(provided: str | None, expected: str, label: str) -> None:
    if not expected:
        if AUTH_OPTIONAL:
            return
        raise HTTPException(
            status_code=503,
            detail=f"{label} not configured — set env var or FLUXMETER_AUTH_OPTIONAL=true for demo",
        )
    if not provided or provided != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")


def resolve_customer_from_key(provided: str | None) -> str | None:
    """Resolve customer_id from per-customer API key, or None if not a customer key."""
    if not provided:
        return None
    r = _redis()
    data = r.get(f"apikey:{_hash_key(provided)}")
    if not data:
        return None
    try:
        payload = json.loads(data)
        key_id = payload.get("key_id")
        if key_id:
            meta = r.get(f"apikey:meta:{key_id}")
            if meta and json.loads(meta).get("revoked"):
                return None
        return payload["customer_id"]
    except (json.JSONDecodeError, KeyError):
        return None


def is_admin_key(provided: str | None) -> bool:
    if not provided:
        return False
    if ADMIN_API_KEY and provided == ADMIN_API_KEY:
        return True
    if not ADMIN_API_KEY and API_KEY and provided == API_KEY:
        return True
    return False


def is_global_api_key(provided: str | None) -> bool:
    if not provided:
        return False
    if API_KEY and provided == API_KEY:
        return True
    return is_admin_key(provided)


def create_customer_api_key(customer_id: str) -> dict:
    """Create a customer-scoped API key stored in Redis."""
    r = _redis()
    key_id = str(uuid.uuid4())
    raw_key = f"fm_live_{secrets.token_urlsafe(32)}"
    payload = {"customer_id": customer_id, "key_id": key_id}
    r.set(f"apikey:{_hash_key(raw_key)}", json.dumps(payload))
    r.sadd(f"customer:{customer_id}:apikeys", key_id)
    r.set(f"apikey:meta:{key_id}", json.dumps({"customer_id": customer_id, "revoked": False}))
    return {"key_id": key_id, "api_key": raw_key, "customer_id": customer_id}


def revoke_customer_api_key(key_id: str) -> bool:
    r = _redis()
    meta = r.get(f"apikey:meta:{key_id}")
    if not meta:
        return False
    info = json.loads(meta)
    customer_id = info.get("customer_id")
    info["revoked"] = True
    r.set(f"apikey:meta:{key_id}", json.dumps(info))
    if customer_id:
        r.srem(f"customer:{customer_id}:apikeys", key_id)
    return True


def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Read/query/ingest — global or customer key."""
    if is_global_api_key(x_api_key):
        return
    if resolve_customer_from_key(x_api_key):
        return
    if AUTH_OPTIONAL and not API_KEY and not ADMIN_API_KEY:
        return
    _check_global_key(x_api_key, API_KEY, "FLUXMETER_API_KEY")


def require_admin_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """Budget mutations, rerate, topup, reserve, reconcile."""
    if is_admin_key(x_api_key):
        return
    if AUTH_OPTIONAL and not ADMIN_API_KEY and not API_KEY:
        return
    if ADMIN_API_KEY:
        _check_global_key(x_api_key, ADMIN_API_KEY, "FLUXMETER_ADMIN_KEY")
    else:
        _check_global_key(x_api_key, API_KEY, "FLUXMETER_API_KEY")


def require_customer_access(
    customer_id: str,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> None:
    """Ensure caller may access this customer (admin/global or matching customer key)."""
    if is_global_api_key(x_api_key):
        return
    resolved = resolve_customer_from_key(x_api_key)
    if resolved and resolved == customer_id:
        return
    if AUTH_OPTIONAL and not API_KEY and not ADMIN_API_KEY:
        return
    raise HTTPException(status_code=403, detail="API key not authorized for this customer")
