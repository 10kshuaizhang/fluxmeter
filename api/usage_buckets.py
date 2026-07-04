"""Rollup bucket keys, session counters — shared by rollup worker and query API."""

from __future__ import annotations

import os
from typing import Any

import redis

from pricing_loader import billing_period_day, billing_period_month

DAY_BUCKET_TTL = int(os.getenv("FLUXMETER_DAY_BUCKET_TTL_SEC", str(400 * 86400)))
SESSION_TTL_SEC = int(os.getenv("FLUXMETER_SESSION_TTL_SEC", str(90 * 86400)))

BUCKET_FIELDS = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "event_count",
    "cost_usd",
    "cache_read_tokens",
    "reasoning_tokens",
)


def rollup_month_key(customer_id: str, period: str) -> str:
    return f"rollup:{customer_id}:period:{period}"


def rollup_day_key(customer_id: str, date: str) -> str:
    return f"rollup:{customer_id}:d:{date}"


def read_usage_bucket(r: redis.Redis, key: str) -> dict[str, Any] | None:
    """Read a rollup hash. Returns None if bucket is missing or empty."""
    if not r.exists(key):
        return None
    data: dict[str, Any] = {}
    for field in BUCKET_FIELDS:
        val = r.hget(key, field)
        if field == "cost_usd":
            data[field] = float(val or 0)
        else:
            data[field] = int(val or 0)
    if data["event_count"] == 0 and data["total_tokens"] == 0:
        return None
    return data


def increment_session(
    r: redis.Redis,
    customer_id: str,
    session_id: str,
    *,
    input_tokens: int,
    output_tokens: int,
    total_tokens: int,
    cost_usd: float,
    cache_read_tokens: int = 0,
    reasoning_tokens: int = 0,
) -> None:
    """Accumulate usage for a conversation/project session (lite ingest path)."""
    key = f"session:{session_id}"
    pipe = r.pipeline()
    pipe.set(f"{key}:customer_id", customer_id, ex=SESSION_TTL_SEC)
    pipe.incrby(f"{key}:input_tokens", input_tokens)
    pipe.incrby(f"{key}:output_tokens", output_tokens)
    pipe.incrby(f"{key}:total_tokens", total_tokens)
    pipe.incrby(f"{key}:event_count", 1)
    pipe.incrbyfloat(f"{key}:cost_usd", cost_usd)
    if cache_read_tokens > 0:
        pipe.incrby(f"{key}:cache_read_tokens", cache_read_tokens)
    if reasoning_tokens > 0:
        pipe.incrby(f"{key}:reasoning_tokens", reasoning_tokens)
    pipe.expire(f"{key}:customer_id", SESSION_TTL_SEC)
    for suffix in BUCKET_FIELDS:
        if suffix == "customer_id":
            continue
        pipe.expire(f"{key}:{suffix}", SESSION_TTL_SEC)
    pipe.execute()


def read_session(r: redis.Redis, session_id: str) -> dict[str, Any] | None:
    key = f"session:{session_id}"
    if r.get(f"{key}:cost_usd") is None and r.get(f"{key}:event_count") is None:
        return None
    data: dict[str, Any] = {"session_id": session_id, "customer_id": r.get(f"{key}:customer_id")}
    for field in BUCKET_FIELDS:
        val = r.get(f"{key}:{field}")
        if field == "cost_usd":
            data[field] = float(val or 0)
        else:
            data[field] = int(val or 0)
    if data["event_count"] == 0 and data["total_tokens"] == 0:
        return None
    return data


__all__ = [
    "rollup_month_key",
    "rollup_day_key",
    "read_usage_bucket",
    "increment_session",
    "read_session",
    "DAY_BUCKET_TTL",
    "SESSION_TTL_SEC",
]
