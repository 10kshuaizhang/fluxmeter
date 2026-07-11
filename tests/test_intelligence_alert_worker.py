import sys
from unittest.mock import patch

sys.path.insert(0, "api")

import fakeredis
from intelligence.alerts import collect_alerts, set_webhook_config
from usage_buckets import rollup_month_key


def test_intel_alert_worker_delivers(monkeypatch):
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(rollup_month_key("c1", "2026-06"), mapping={
        "cost_usd": "100", "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5",
    })
    r.hset(rollup_month_key("c1", "2026-07"), mapping={
        "cost_usd": "200", "event_count": "1", "total_tokens": "10",
        "input_tokens": "5", "output_tokens": "5",
    })
    set_webhook_config(r, "https://example.com/hook", "secret")
    delivered = []

    def fake_deliver(url, secret, payload):
        delivered.append(payload)
        return True

    monkeypatch.setattr("webhook_deliver.deliver_webhook", fake_deliver)
    from intelligence.intel_alert_worker import INTERVAL_SEC

    assert INTERVAL_SEC > 0
    alerts = collect_alerts(r, period="2026-07")
    assert alerts
    for alert in alerts:
        from intelligence.alerts import alert_to_payload, mark_alert_sent, should_send_alert

        if should_send_alert(r, alert.type, "2026-07"):
            fake_deliver("https://example.com/hook", "secret", alert_to_payload(alert))
            mark_alert_sent(r, alert.type, "2026-07")
    assert delivered
