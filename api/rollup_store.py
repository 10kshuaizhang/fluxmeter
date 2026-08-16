"""RollupStore — indexed period customer costs with SCAN fallback."""

from __future__ import annotations

import redis

from usage_buckets import period_customers_index, read_usage_bucket, rollup_month_key


def list_customer_period_costs(r: redis.Redis, period: str) -> dict[str, float]:
    """Customer → cost_usd for a billing period.

    Prefer idx:period:{period}:customers; fall back to SCAN when index is absent
    (pre-deploy Redis). ponytail: dual-read until all windows have written idx.
    """
    out: dict[str, float] = {}
    idx = period_customers_index(period)
    members = r.smembers(idx)
    if members:
        for cid in members:
            data = read_usage_bucket(r, rollup_month_key(cid, period))
            if data:
                out[cid] = data["cost_usd"]
        return out

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
