import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.pricing_optimizer import compute_pricing_recommendations
from intelligence.revenue_store import set_revenue
from usage_buckets import model_period_key, rollup_month_key


def _seed_month(r, cid, period, cost):
    r.hset(rollup_month_key(cid, period), mapping={
        "cost_usd": str(cost), "event_count": "1", "total_tokens": "100",
        "input_tokens": "60", "output_tokens": "40",
    })


def test_loss_customer_price_increase_roi():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed_month(r, "cust-a", "2026-07", 620)
    set_revenue(r, "cust-a", "2026-07", revenue_usd=500.0)
    recs = compute_pricing_recommendations(r, period="2026-07")
    price_recs = [x for x in recs if x.action == "price_increase" and x.customer_id == "cust-a"]
    assert price_recs
    assert price_recs[0].roi_annual_usd is not None
    assert price_recs[0].roi_annual_usd > 0


def test_model_switch_recommendation():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed_month(r, "cust-b", "2026-07", 100)
    set_revenue(r, "cust-b", "2026-07", revenue_usd=50.0)
    r.hset(model_period_key("cust-b", "gpt-4o", "2026-07"), mapping={
        "cost_usd": "80", "event_count": "1", "total_tokens": "1000",
        "input_tokens": "800", "output_tokens": "200",
    })
    recs = compute_pricing_recommendations(r, period="2026-07")
    switch = [x for x in recs if x.action == "model_switch" and x.customer_id == "cust-b"]
    assert switch
    assert switch[0].roi_annual_usd is not None
