"""Intelligence MVP — model-period rollup keys on lite ingest."""

from __future__ import annotations

import sys
import uuid

import fakeredis

sys.path.insert(0, "api")

from lite_aggregate_lua import LiteAggregator  # noqa: E402
from pricing_loader import PricingCatalog, billing_period_month, reload_catalog  # noqa: E402
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


def test_aggregate_increments_model_period_bucket():
    r = fakeredis.FakeRedis(decode_responses=True)
    reload_catalog(PricingCatalog.load_from_file())
    agg = LiteAggregator(r)

    cid = "cust-a"
    model = "gpt-4o"
    ts = 1782864000000  # 2026-07-01T00:00:00Z
    period = billing_period_month(ts)
    event = {
        "customerId": cid,
        "modelId": model,
        "inputTokens": 60,
        "outputTokens": 40,
        "eventId": str(uuid.uuid4()),
        "timestamp": ts,
    }

    result = agg.aggregate(event)
    assert result["status"] == "ok"

    key = model_period_key(cid, model, period)
    data = read_usage_bucket(r, key)
    assert data is not None
    assert data["input_tokens"] == 60
    assert data["output_tokens"] == 40
    assert data["total_tokens"] == 100
    assert data["event_count"] == 1
    assert data["cost_usd"] == result["cost_usd"]


def test_duplicate_does_not_increment_model_period():
    r = fakeredis.FakeRedis(decode_responses=True)
    reload_catalog(PricingCatalog.load_from_file())
    agg = LiteAggregator(r)

    cid = "cust-dup"
    model = "gpt-4o-mini"
    ts = 1782864000000
    period = billing_period_month(ts)
    eid = str(uuid.uuid4())
    event = {
        "customerId": cid,
        "modelId": model,
        "inputTokens": 50,
        "outputTokens": 0,
        "eventId": eid,
        "timestamp": ts,
    }

    agg.aggregate(event)
    agg.aggregate(event)

    key = model_period_key(cid, model, period)
    data = read_usage_bucket(r, key)
    assert data is not None
    assert data["event_count"] == 1
    assert data["input_tokens"] == 50
