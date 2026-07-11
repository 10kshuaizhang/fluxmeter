import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.revenue_store import get_revenue

SAMPLE = {
    "events": [
        {"customerId": "cust-a", "subject": "revenue", "value": 500, "time": "2026-07-15T00:00:00Z"},
        {"customerId": "cust-a", "subject": "tokens", "value": 1000000, "time": "2026-07-15T00:00:00Z"},
    ]
}


def test_import_openmeter_revenue():
    r = fakeredis.FakeRedis(decode_responses=True)
    from intelligence.connectors.openmeter import import_openmeter_events

    stats = import_openmeter_events(r, SAMPLE, period="2026-07")
    assert stats["revenue_rows"] == 1
    assert stats["ignored_rows"] == 1
    assert get_revenue(r, "cust-a", "2026-07") == {"revenue_usd": 500.0, "source": "openmeter"}
    assert r.get("intel:overlay:openmeter:2026-07:imported_at") is not None
