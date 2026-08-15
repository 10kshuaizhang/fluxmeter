"""Intelligence MVP — model-period rollup reader contract."""

from __future__ import annotations

import sys
import fakeredis

sys.path.insert(0, "api")

from usage_buckets import model_period_key, read_usage_bucket  # noqa: E402


def test_model_period_key_format():
    assert model_period_key("cust-a", "gpt-4o", "2026-07") == "rollup:cust-a:model:gpt-4o:period:2026-07"


def test_read_model_period_bucket():
    r = fakeredis.FakeRedis(decode_responses=True)
    key = model_period_key("cust-a", "gpt-4o", "2026-07")
    r.hset(
        key,
        mapping={
            "cost_usd": "12.5",
            "event_count": "3",
            "total_tokens": "100",
            "input_tokens": "60",
            "output_tokens": "40",
        },
    )
    data = read_usage_bucket(r, key)
    assert data["cost_usd"] == 12.5
    assert data["event_count"] == 3
