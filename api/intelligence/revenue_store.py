from __future__ import annotations

import json

import redis


def _key(customer_id: str, period: str) -> str:
    return f"intel:revenue:{customer_id}:{period}"


def set_revenue(
    r: redis.Redis,
    customer_id: str,
    period: str,
    *,
    revenue_usd: float,
    source: str = "manual",
) -> None:
    r.set(_key(customer_id, period), json.dumps({"revenue_usd": revenue_usd, "source": source}))


def get_revenue(r: redis.Redis, customer_id: str, period: str) -> dict | None:
    raw = r.get(_key(customer_id, period))
    return json.loads(raw) if raw else None
