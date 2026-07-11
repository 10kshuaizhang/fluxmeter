import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.report import build_report_json, build_report_markdown
from intelligence.revenue_store import set_revenue
from usage_buckets import rollup_month_key


def test_report_json_and_markdown():
    r = fakeredis.FakeRedis(decode_responses=True)
    for period, cost in [("2026-06", 100), ("2026-07", 140)]:
        r.hset(rollup_month_key("c1", period), mapping={
            "cost_usd": str(cost), "event_count": "1", "total_tokens": "10",
            "input_tokens": "5", "output_tokens": "5",
        })
    set_revenue(r, "c1", "2026-07", revenue_usd=500.0)

    data = build_report_json(r, period="2026-07", baseline_period="2026-06")
    assert "headline" in data
    assert "forecast" in data
    assert "profitability" in data

    md = build_report_markdown(r, period="2026-07", baseline_period="2026-06")
    assert "# FluxMeter Intelligence Report" in md
    assert "Executive Summary" in md
    assert "Forecast vs Budget" in md
