"""RollupStore period-customer index with SCAN fallback."""

from __future__ import annotations

import sys

import fakeredis
import pytest

sys.path.insert(0, "api")

from rollup_store import list_customer_period_costs
from usage_buckets import period_customers_index, rollup_month_key


def _seed_period(r, cid: str, period: str, cost: float) -> None:
    r.hset(
        rollup_month_key(cid, period),
        mapping={
            "cost_usd": str(cost),
            "event_count": "1",
            "total_tokens": "10",
            "input_tokens": "5",
            "output_tokens": "5",
        },
    )


def test_prefers_index_when_present():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed_period(r, "a", "2026-08", 1.5)
    _seed_period(r, "b", "2026-08", 2.5)
    # orphan rollup not in index must be ignored when index exists
    _seed_period(r, "orphan", "2026-08", 99.0)
    r.sadd(period_customers_index("2026-08"), "a", "b")

    costs = list_customer_period_costs(r, "2026-08")
    assert costs == {"a": 1.5, "b": 2.5}


def test_falls_back_to_scan_without_index():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed_period(r, "c1", "2026-07", 3.0)
    costs = list_customer_period_costs(r, "2026-07")
    assert costs == {"c1": 3.0}
