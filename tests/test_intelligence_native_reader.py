import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.native_reader import (
    list_customer_daily_costs,
    list_customer_period_costs,
    list_dim_margin_series,
    list_global_period_costs,
    list_model_period_costs,
)
from usage_buckets import rollup_month_key, model_period_key, rollup_day_key
from billing_dims import increment_dims, validate_metadata


def _seed_day_cost(r, customer_id: str, date: str, cost_usd: float) -> None:
    key = rollup_day_key(customer_id, date)
    r.hset(key, mapping={
        "cost_usd": str(cost_usd), "event_count": "1", "total_tokens": "100",
        "input_tokens": "50", "output_tokens": "50",
    })


def test_list_customer_period_costs():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(rollup_month_key("a", "2026-07"), mapping={"cost_usd": "10", "event_count": "1",
           "total_tokens": "100", "input_tokens": "50", "output_tokens": "50"})
    r.hset(rollup_month_key("b", "2026-07"), mapping={"cost_usd": "20", "event_count": "1",
           "total_tokens": "100", "input_tokens": "50", "output_tokens": "50"})
    costs = list_customer_period_costs(r, "2026-07")
    assert costs == {"a": 10.0, "b": 20.0}


def test_list_model_period_costs():
    r = fakeredis.FakeRedis(decode_responses=True)
    key = model_period_key("a", "gpt-4o", "2026-07")
    r.hset(key, mapping={"cost_usd": "7", "event_count": "1", "total_tokens": "10",
                         "input_tokens": "5", "output_tokens": "5"})
    models = list_model_period_costs(r, "2026-07", customer_id="a")
    assert models == {"gpt-4o": 7.0}


def test_list_customer_daily_costs():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed_day_cost(r, "a", "2026-07-01", 5.0)
    _seed_day_cost(r, "a", "2026-07-02", 7.0)
    daily = list_customer_daily_costs(r, "a", "2026-07")
    assert daily == {"2026-07-01": 5.0, "2026-07-02": 7.0}


def test_list_global_period_costs():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(rollup_month_key("a", "2026-06"), mapping={
        "cost_usd": "10", "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5"})
    r.hset(rollup_month_key("b", "2026-06"), mapping={
        "cost_usd": "15", "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5"})
    r.hset(rollup_month_key("a", "2026-07"), mapping={
        "cost_usd": "20", "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5"})
    trend = list_global_period_costs(r, ["2026-06", "2026-07"])
    assert trend == {"2026-06": 25.0, "2026-07": 20.0}


def test_list_dim_margin_series():
    r = fakeredis.FakeRedis(decode_responses=True)
    meta = validate_metadata({"feature": "chat"})
    increment_dims(r, meta, cost_usd=10.0, event_ts_ms=1719792000000)  # 2024-07-01
    increment_dims(r, meta, cost_usd=20.0, event_ts_ms=1722470400000)  # 2024-08-01
    # billing_period_month for those timestamps — use actual periods from keys
    from pricing_loader import billing_period_month
    p1 = billing_period_month(1719792000000)
    p2 = billing_period_month(1722470400000)
    series = list_dim_margin_series(r, "feature", [p1, p2])
    assert series[p1]["chat"] == 10.0
    assert series[p2]["chat"] == 20.0
