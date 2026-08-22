"""Rollup bucket keys and read helpers — shared by query API and Intelligence."""

from __future__ import annotations

import os
from typing import Any

import redis

from tenant_keys import scope_prefix_for_read

DAY_BUCKET_TTL = int(os.getenv("FLUXMETER_DAY_BUCKET_TTL_SEC", str(400 * 86400)))
SESSION_TTL_SEC = int(os.getenv("FLUXMETER_SESSION_TTL_SEC", str(90 * 86400)))
SPAN_TTL_SEC = int(os.getenv("FLUXMETER_SPAN_TTL_SEC", str(86400)))  # 24h — matches SpanSink.java

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


def model_period_key(customer_id: str, model_id: str, period: str) -> str:
    return f"rollup:{customer_id}:model:{model_id}:period:{period}"


def period_customers_index(period: str) -> str:
    return f"idx:period:{period}:customers"


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


def read_session(
    r: redis.Redis, session_id: str, tenant_id: str | None = None
) -> dict[str, Any] | None:
    key = scope_prefix_for_read(r, tenant_id, "session", session_id)
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
    "model_period_key",
    "period_customers_index",
    "read_usage_bucket",
    "read_session",
    "DAY_BUCKET_TTL",
    "SESSION_TTL_SEC",
    "SPAN_TTL_SEC",
]
