import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.alerts import (
    collect_alerts,
    detect_cost_spike,
    detect_margin_loss,
    mark_alert_sent,
    should_send_alert,
)
from intelligence.revenue_store import set_revenue
from usage_buckets import rollup_month_key


def _seed(r, cid, period, cost):
    r.hset(rollup_month_key(cid, period), mapping={
        "cost_usd": str(cost), "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5",
    })


def test_detect_cost_spike():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed(r, "c1", "2026-06", 100)
    _seed(r, "c1", "2026-07", 150)
    alert = detect_cost_spike(r, period="2026-07")
    assert alert is not None
    assert alert.type == "INTEL_COST_SPIKE"


def test_detect_margin_loss_new_customer():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed(r, "lossy", "2026-07", 620)
    set_revenue(r, "lossy", "2026-07", revenue_usd=500.0)
    alert = detect_margin_loss(r, period="2026-07")
    assert alert is not None
    assert "lossy" in alert.summary


def test_alert_debounce():
    r = fakeredis.FakeRedis(decode_responses=True)
    assert should_send_alert(r, "INTEL_COST_SPIKE", "2026-07")
    mark_alert_sent(r, "INTEL_COST_SPIKE", "2026-07")
    assert not should_send_alert(r, "INTEL_COST_SPIKE", "2026-07")


def test_collect_alerts():
    r = fakeredis.FakeRedis(decode_responses=True)
    _seed(r, "c1", "2026-06", 100)
    _seed(r, "c1", "2026-07", 200)
    alerts = collect_alerts(r, period="2026-07")
    assert any(a.type == "INTEL_COST_SPIKE" for a in alerts)
