import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.root_cause import analyze_root_cause
from usage_buckets import rollup_month_key, model_period_key


def test_root_cause_model_dominates():
    r = fakeredis.FakeRedis(decode_responses=True)
    for period, cost in [("2026-06", "100"), ("2026-07", "140")]:
        r.hset(rollup_month_key("c1", period), mapping={
            "cost_usd": cost, "event_count": "1", "total_tokens": "10",
            "input_tokens": "5", "output_tokens": "5"})
    r.hset(model_period_key("c1", "gpt-4o", "2026-06"), mapping={
        "cost_usd": "60", "event_count": "1", "total_tokens": "10", "input_tokens": "5", "output_tokens": "5"})
    r.hset(model_period_key("c1", "gpt-4o", "2026-07"), mapping={
        "cost_usd": "100", "event_count": "1", "total_tokens": "10", "input_tokens": "5", "output_tokens": "5"})
    report = analyze_root_cause(r, period="2026-07", baseline_period="2026-06", scope="global")
    assert report.delta_usd == 40.0
    assert report.delta_pct == 40.0
    top = report.contributors[0]
    assert top.dimension == "model"
    assert "gpt-4o" in top.key
    assert "40%" in report.summary or "40.0%" in report.summary
