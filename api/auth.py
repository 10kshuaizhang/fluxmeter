"""API key authentication for FluxMeter endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import secrets
import time
import uuid

import redis
from fastapi import Header, HTTPException

from pricing_loader import billing_period_day, billing_period_month

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


def resolve_key_context(provided: str | None) -> tuple[str | None, str | None]:
    """Return (customer_id, key_id) for a customer API key, else (None, None)."""
    if not provided:
        return None, None
    r = _redis()
    data = r.get(f"apikey:{_hash_key(provided)}")
    if not data:
        return None, None
    try:
        payload = json.loads(data)
        key_id = payload.get("key_id")
        if key_id:
            meta = r.get(f"apikey:meta:{key_id}")
            if meta and json.loads(meta).get("revoked"):
                return None, None
        return payload.get("customer_id"), key_id
    except (json.JSONDecodeError, KeyError):
        return None, None


def resolve_customer_from_key(provided: str | None) -> str | None:
    """Resolve customer_id from per-customer API key, or None if not a customer key."""
    customer_id, _ = resolve_key_context(provided)
    return customer_id


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


def set_api_key_budget(
    key_id: str,
    *,
    daily_budget_usd: float | None = None,
    monthly_budget_usd: float | None = None,
) -> dict:
    """Set optional daily/monthly spend caps on an API key."""
    r = _redis()
    meta_raw = r.get(f"apikey:meta:{key_id}")
    if not meta_raw:
        raise HTTPException(status_code=404, detail="API key not found")
    info = json.loads(meta_raw)
    if daily_budget_usd is not None:
        info["daily_budget_usd"] = daily_budget_usd
    if monthly_budget_usd is not None:
        info["monthly_budget_usd"] = monthly_budget_usd
    r.set(f"apikey:meta:{key_id}", json.dumps(info))
    return {
        "key_id": key_id,
        "customer_id": info.get("customer_id"),
        "daily_budget_usd": info.get("daily_budget_usd"),
        "monthly_budget_usd": info.get("monthly_budget_usd"),
    }


def check_api_key_budget(
    r: redis.Redis,
    key_id: str,
    estimated_cost_usd: float,
) -> dict | None:
    """Return deny payload if key daily/monthly cap would be exceeded."""
    meta_raw = r.get(f"apikey:meta:{key_id}")
    if not meta_raw:
        return None
    info = json.loads(meta_raw)
    daily_cap = info.get("daily_budget_usd")
    monthly_cap = info.get("monthly_budget_usd")
    if daily_cap is None and monthly_cap is None:
        return None

    now_ms = int(time.time() * 1000)
    day = billing_period_day(now_ms)
    month = billing_period_month(now_ms)
    est = max(estimated_cost_usd, 0.0)

    if daily_cap is not None:
        daily_spent = float(r.get(f"apikey:{key_id}:spent:d:{day}") or 0)
        if daily_spent + est > float(daily_cap):
            return {
                "allowed": False,
                "reason": "api_key_daily_budget",
                "key_id": key_id,
                "spent_usd": daily_spent,
                "budget_usd": float(daily_cap),
                "period": day,
            }

    if monthly_cap is not None:
        monthly_spent = float(r.get(f"apikey:{key_id}:spent:m:{month}") or 0)
        if monthly_spent + est > float(monthly_cap):
            return {
                "allowed": False,
                "reason": "api_key_monthly_budget",
                "key_id": key_id,
                "spent_usd": monthly_spent,
                "budget_usd": float(monthly_cap),
                "period": month,
            }
    return None


def record_api_key_spend(r: redis.Redis, key_id: str, cost_usd: float) -> None:
    """Increment per-key spend counters after ingest."""
    if cost_usd <= 0:
        return
    now_ms = int(time.time() * 1000)
    day = billing_period_day(now_ms)
    month = billing_period_month(now_ms)
    pipe = r.pipeline()
    pipe.incrbyfloat(f"apikey:{key_id}:spent:d:{day}", cost_usd)
    pipe.expire(f"apikey:{key_id}:spent:d:{day}", 86400 * 2)
    pipe.incrbyfloat(f"apikey:{key_id}:spent:m:{month}", cost_usd)
    pipe.expire(f"apikey:{key_id}:spent:m:{month}", 86400 * 62)
    pipe.execute()


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
    if resolved is not None:
        if resolved == customer_id:
            return
        raise HTTPException(status_code=403, detail="API key not authorized for this customer")
    if AUTH_OPTIONAL and not API_KEY and not ADMIN_API_KEY:
        return
    raise HTTPException(status_code=403, detail="API key not authorized for this customer")
