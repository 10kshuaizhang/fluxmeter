import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.revenue_store import set_revenue
from intelligence.unit_economics import compute_unit_economics
from usage_buckets import rollup_month_key


def test_unit_economics_loss_recommendation():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(rollup_month_key("cust-a", "2026-07"), mapping={
        "cost_usd": "620", "event_count": "1", "total_tokens": "10", "input_tokens": "5", "output_tokens": "5"})
    set_revenue(r, "cust-a", "2026-07", revenue_usd=500.0)
    rows = compute_unit_economics(r, period="2026-07")
    row = next(x for x in rows if x.customer_id == "cust-a")
    assert row.status == "loss"
    assert row.margin_usd == -120.0
    assert row.recommendation is not None
    assert "upgrade" in row.recommendation.lower()
