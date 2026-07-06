"""Unit tests for Lite-path budget webhooks (no Kafka)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from webhook_deliver import deliver_lite_alerts, lite_alerts_for_result


def test_lite_alerts_exhausted():
    r = MagicMock()
    alerts = lite_alerts_for_result(
        r, "c1", {"budget_alert": "BUDGET_EXHAUSTED", "balance_usd": 0.0, "cost_usd": 0.01}
    )
    assert alerts == [("BUDGET_EXHAUSTED", {})]
    r.delete.assert_any_call("budget:c1:webhook_low_sent")


def test_lite_alerts_low_debounced():
    r = MagicMock()

    def get(k):
        return {
            "budget:c1:alert_threshold_usd": "1.0",
            "budget:c1:initial_balance_usd": "100",
            "budget:c1:total_topup_usd": "0",
        }.get(k)

    r.get.side_effect = get
    r.set.return_value = True
    alerts = lite_alerts_for_result(
        r, "c1", {"status": "ok", "balance_usd": 0.5, "cost_usd": 0.01}
    )
    types = [a[0] for a in alerts]
    assert "BUDGET_LOW" in types
    # 0.5 balance on 100 initial → 99.5% spent → both warn tiers
    assert "BUDGET_WARN" in types


def test_warn_ladder_70_then_90():
    r = MagicMock()
    store = {
        "budget:c1:initial_balance_usd": "100",
        "budget:c1:total_topup_usd": "0",
    }

    def get(k):
        return store.get(k)

    r.get.side_effect = get
    r.set.return_value = True

    # 25 remaining → 75% spent → only 70 warn
    alerts = lite_alerts_for_result(
        r, "c1", {"status": "ok", "balance_usd": 25.0, "cost_usd": 1.0}
    )
    warns = [a for a in alerts if a[0] == "BUDGET_WARN"]
    assert len(warns) == 1
    assert warns[0][1]["warn_pct"] == 70

    # 5 remaining → 95% spent → 70 and 90
    alerts2 = lite_alerts_for_result(
        r, "c1", {"status": "ok", "balance_usd": 5.0, "cost_usd": 1.0}
    )
    pcs = sorted(a[1]["warn_pct"] for a in alerts2 if a[0] == "BUDGET_WARN")
    assert pcs == [70, 90]


def test_lite_alerts_clears_debounce_when_recovered():
    r = MagicMock()
    r.get.side_effect = lambda k: {
        "budget:c1:alert_threshold_usd": "1.0",
        "budget:c1:initial_balance_usd": "100",
        "budget:c1:total_topup_usd": "0",
    }.get(k)
    alerts = lite_alerts_for_result(
        r, "c1", {"status": "ok", "balance_usd": 50.0, "cost_usd": 0.01}
    )
    assert alerts == []
    r.delete.assert_any_call("budget:c1:webhook_low_sent")
    r.delete.assert_any_call("budget:c1:webhook_warn_70_sent")


def test_deliver_lite_alerts_posts():
    r = MagicMock()
    r.get.side_effect = lambda k: {
        "budget:c1:webhook_url": "https://hooks.example/x",
        "budget:c1:webhook_secret": "s",
        "budget:c1:alert_threshold_usd": None,
        "budget:c1:initial_balance_usd": None,
    }.get(k)
    with patch("webhook_deliver.deliver_webhook", return_value=True) as post:
        deliver_lite_alerts(
            r, "c1", {"budget_alert": "BUDGET_EXHAUSTED", "balance_usd": 0.0, "cost_usd": 0.02},
            model_id="gpt-4o-mini",
        )
        assert post.called
        payload = post.call_args[0][2]
        assert payload["type"] == "BUDGET_EXHAUSTED"
        assert payload["customer_id"] == "c1"
