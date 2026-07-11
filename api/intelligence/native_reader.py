from __future__ import annotations

import redis

from billing_dims import ALLOWED_DIMS
from usage_buckets import read_usage_bucket


def list_customer_period_costs(r: redis.Redis, period: str) -> dict[str, float]:
    out: dict[str, float] = {}
    cursor = 0
    pattern = f"rollup:*:period:{period}"
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=200)
        for key in keys:
            parts = key.split(":")
            if len(parts) == 4 and parts[0] == "rollup" and parts[2] == "period":
                cid = parts[1]
                data = read_usage_bucket(r, key)
                if data:
                    out[cid] = data["cost_usd"]
        if cursor == 0:
            break
    return out


def list_model_period_costs(
    r: redis.Redis, period: str, *, customer_id: str | None = None
) -> dict[str, float]:
    out: dict[str, float] = {}
    pattern = (
        f"rollup:{customer_id}:model:*:period:{period}"
        if customer_id
        else f"rollup:*:model:*:period:{period}"
    )
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=200)
        for key in keys:
            parts = key.split(":")
            if len(parts) == 6 and parts[2] == "model" and parts[4] == "period":
                model_id = parts[3]
                data = read_usage_bucket(r, key)
                if data:
                    out[model_id] = out.get(model_id, 0.0) + data["cost_usd"]
        if cursor == 0:
            break
    return out


def list_dim_period_costs(r: redis.Redis, period: str) -> dict[str, dict[str, float]]:
    """{dim_key: {dim_value: cost}} for whitelisted dims."""
    result: dict[str, dict[str, float]] = {d: {} for d in ALLOWED_DIMS}
    for dim_key in ALLOWED_DIMS:
        cursor = 0
        while True:
            cursor, keys = r.scan(
                cursor, match=f"dim:{dim_key}:*:period:{period}:cost_usd", count=200
            )
            for key in keys:
                parts = key.split(":")
                dim_value = parts[2]
                cost = float(r.get(key) or 0)
                if cost > 0:
                    result[dim_key][dim_value] = cost
            if cursor == 0:
                break
    return result


def list_customer_daily_costs(
    r: redis.Redis, customer_id: str, date_prefix: str
) -> dict[str, float]:
    """Daily costs for a customer where date keys start with YYYY-MM."""
    out: dict[str, float] = {}
    pattern = f"rollup:{customer_id}:d:{date_prefix}*"
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=200)
        for key in keys:
            parts = key.split(":")
            if len(parts) == 4 and parts[0] == "rollup" and parts[2] == "d":
                date = parts[3]
                data = read_usage_bucket(r, key)
                if data:
                    out[date] = data["cost_usd"]
        if cursor == 0:
            break
    return out


def list_global_daily_costs(r: redis.Redis, date_prefix: str) -> dict[str, float]:
    """Aggregate daily costs across all customers for dates matching prefix."""
    out: dict[str, float] = {}
    pattern = f"rollup:*:d:{date_prefix}*"
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=200)
        for key in keys:
            parts = key.split(":")
            if len(parts) == 4 and parts[0] == "rollup" and parts[2] == "d":
                date = parts[3]
                data = read_usage_bucket(r, key)
                if data:
                    out[date] = out.get(date, 0.0) + data["cost_usd"]
        if cursor == 0:
            break
    return out


def list_global_period_costs(r: redis.Redis, periods: list[str]) -> dict[str, float]:
    """Total cost per calendar month (global aggregate)."""
    return {
        period: sum(list_customer_period_costs(r, period).values())
        for period in periods
    }


def list_dim_margin_series(
    r: redis.Redis, dim_key: str, periods: list[str]
) -> dict[str, dict[str, float]]:
    """Cross-month cost series for one dimension key. {period: {dim_value: cost}}."""
    if dim_key not in ALLOWED_DIMS:
        return {p: {} for p in periods}
    result: dict[str, dict[str, float]] = {}
    for period in periods:
        all_dims = list_dim_period_costs(r, period)
        result[period] = dict(all_dims.get(dim_key, {}))
    return result
