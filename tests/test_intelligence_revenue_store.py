import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.revenue_store import get_revenue, set_revenue


def test_set_and_get_revenue():
    r = fakeredis.FakeRedis(decode_responses=True)
    set_revenue(r, "cust-a", "2026-07", revenue_usd=500.0, source="manual")
    assert get_revenue(r, "cust-a", "2026-07") == {"revenue_usd": 500.0, "source": "manual"}
    assert get_revenue(r, "cust-a", "2026-06") is None
