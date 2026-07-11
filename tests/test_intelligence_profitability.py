import sys
sys.path.insert(0, "api")

import fakeredis
from billing_dims import increment_dims, validate_metadata
from intelligence.profitability import build_profitability_dashboard
from intelligence.revenue_store import set_revenue
from usage_buckets import rollup_month_key


def test_profitability_dashboard():
    r = fakeredis.FakeRedis(decode_responses=True)
    for period, cost in [("2026-06", 100), ("2026-07", 150)]:
        r.hset(rollup_month_key("c1", period), mapping={
            "cost_usd": str(cost), "event_count": "1", "total_tokens": "10",
            "input_tokens": "5", "output_tokens": "5",
        })
    set_revenue(r, "c1", "2026-07", revenue_usd=200.0)
    meta = validate_metadata({"feature": "chat"})
    increment_dims(r, meta, cost_usd=50.0, event_ts_ms=1782864000000)

    dash = build_profitability_dashboard(r, period="2026-07", months=2)
    assert dash.totals["cost_usd"] == 150.0
    assert dash.totals["loss_customer_count"] == 0
    assert len(dash.trend) == 2
    assert dash.by_product
