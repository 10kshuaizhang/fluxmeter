"""Whitelist metadata dimension counters for feature/workflow attribution."""

from __future__ import annotations

import os
from typing import Any

import redis

from pricing_loader import billing_period_month

DIM_TTL_SEC = int(os.getenv("FLUXMETER_DIM_TTL_SEC", str(400 * 86400)))
MAX_METADATA_KEYS = 8

_allowed_raw = os.getenv("FLUXMETER_USAGE_DIMS", "room_id,feature")
ALLOWED_DIMS = frozenset(d.strip() for d in _allowed_raw.split(",") if d.strip())


def validate_metadata(metadata: dict[str, str] | None) -> dict[str, str] | None:
    """Return sanitized metadata or raise ValueError."""
    if not metadata:
        return None
    if len(metadata) > MAX_METADATA_KEYS:
        raise ValueError(f"metadata exceeds {MAX_METADATA_KEYS} keys")
    out: dict[str, str] = {}
    for key, value in metadata.items():
        if key not in ALLOWED_DIMS:
            raise ValueError(f"metadata key '{key}' not in whitelist {sorted(ALLOWED_DIMS)}")
        if not isinstance(value, str) or not value:
            raise ValueError(f"metadata values must be non-empty strings")
        out[key] = value
    return out or None


def increment_dims(
    r: redis.Redis,
    metadata: dict[str, str] | None,
    *,
    cost_usd: float,
    event_ts_ms: int,
) -> None:
    """Increment whitelisted dimension counters."""
    if not metadata or cost_usd <= 0:
        return
    period = billing_period_month(event_ts_ms)
    pipe = r.pipeline()
    for key, value in metadata.items():
        if key not in ALLOWED_DIMS:
            continue
        base = f"dim:{key}:{value}"
        pipe.incrbyfloat(f"{base}:cost_usd", cost_usd)
        pipe.incrby(f"{base}:event_count", 1)
        pipe.incrbyfloat(f"{base}:period:{period}:cost_usd", cost_usd)
        pipe.incrby(f"{base}:period:{period}:event_count", 1)
        for suffix in (":cost_usd", ":event_count", f":period:{period}:cost_usd", f":period:{period}:event_count"):
            pipe.expire(f"{base}{suffix}", DIM_TTL_SEC)
    pipe.execute()


def read_dim_usage(
    r: redis.Redis,
    dim_key: str,
    dim_value: str,
    *,
    period: str | None = None,
) -> dict[str, Any] | None:
    """Read dimension usage (lifetime or monthly period)."""
    if dim_key not in ALLOWED_DIMS:
        return None
    base = f"dim:{dim_key}:{dim_value}"
    if period:
        cost = r.get(f"{base}:period:{period}:cost_usd")
        events = r.get(f"{base}:period:{period}:event_count")
        if cost is None and events is None:
            return None
        return {
            "dim_key": dim_key,
            "dim_value": dim_value,
            "period": period,
            "cost_usd": float(cost or 0),
            "event_count": int(events or 0),
        }
    cost = r.get(f"{base}:cost_usd")
    events = r.get(f"{base}:event_count")
    if cost is None and events is None:
        return None
    return {
        "dim_key": dim_key,
        "dim_value": dim_value,
        "cost_usd": float(cost or 0),
        "event_count": int(events or 0),
    }
