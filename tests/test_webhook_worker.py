"""Replay-safe Kafka budget webhook transitions."""

from unittest.mock import patch
import pytest

import fakeredis

from webhook_worker import process_alert


def test_replayed_alert_is_delivered_once():
    r = fakeredis.FakeRedis(decode_responses=True)
    alert = {
        "customerId": "cust_1",
        "type": "BUDGET_WARN",
        "windowStart": 123,
        "warnPct": 70,
    }

    with patch("webhook_worker.fire_budget_webhook", return_value=True) as deliver:
        assert process_alert(r, alert) is True
        assert process_alert(r, alert) is False

    deliver.assert_called_once()


def test_failed_delivery_can_be_retried():
    r = fakeredis.FakeRedis(decode_responses=True)
    alert = {"customerId": "cust_1", "type": "BUDGET_LOW", "windowStart": 456}

    with patch("webhook_worker.fire_budget_webhook", side_effect=[False, True]) as deliver:
        with pytest.raises(RuntimeError, match="delivery failed"):
            process_alert(r, alert)
        assert process_alert(r, alert) is True

    assert deliver.call_count == 2
