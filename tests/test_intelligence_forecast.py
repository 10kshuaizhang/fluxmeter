import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.forecast import compute_forecast
from usage_buckets import rollup_day_key


def _seed_day(r, cid, date, cost, scope="customer"):
    key = rollup_day_key(cid, date) if scope == "customer" else f"rollup:{cid}:d:{date}"
    r.hset(key, mapping={
        "cost_usd": str(cost), "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5",
    })


def test_forecast_linear_extrapolation():
    r = fakeredis.FakeRedis(decode_responses=True)
    for day in range(1, 11):
        _seed_day(r, "global", f"2026-07-{day:02d}", 10.0)
    # global scope aggregates all customers — seed one customer
    r = fakeredis.FakeRedis(decode_responses=True)
    for day in range(1, 11):
        _seed_day(r, "c1", f"2026-07-{day:02d}", 10.0)

    fc = compute_forecast(r, period="2026-07", scope="customer:c1")
    assert fc.mtd_cost_usd == 100.0
    assert fc.forecast_eom_cost_usd >= fc.mtd_cost_usd


def test_forecast_over_budget():
    r = fakeredis.FakeRedis(decode_responses=True)
    for day in range(1, 6):
        _seed_day(r, "c1", f"2026-07-{day:02d}", 20.0)
    r.set("budget:c1:initial_balance_usd", "50")
    fc = compute_forecast(r, period="2026-07", scope="customer:c1")
    assert fc.budget_usd == 50.0
    assert fc.status in ("at_risk", "over_budget", "on_track")
