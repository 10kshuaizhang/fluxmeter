"""Background intelligence alert delivery loop."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import redis

from intelligence.alerts import (
    alert_to_payload,
    collect_alerts,
    get_webhook_config,
    mark_alert_sent,
    should_send_alert,
)
from pricing_loader import billing_period_month
from webhook_deliver import deliver_webhook

logger = logging.getLogger(__name__)

INTERVAL_SEC = int(__import__("os").getenv("INTEL_ALERT_INTERVAL_SEC", "300"))


async def intel_alert_loop(r: redis.Redis):
    logger.info("Intelligence alert worker started (interval=%ds)", INTERVAL_SEC)
    while True:
        try:
            await asyncio.sleep(INTERVAL_SEC)
            url, secret = get_webhook_config(r)
            if not url:
                continue

            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            period = billing_period_month(now_ms)
            for alert in collect_alerts(r, period=period):
                if not should_send_alert(r, alert.type, period):
                    continue
                payload = alert_to_payload(alert)
                payload["timestamp"] = now_ms
                ok = deliver_webhook(url, secret, payload)
                if ok:
                    mark_alert_sent(r, alert.type, period)
                    logger.info("Intel alert sent: %s %s", alert.type, period)
                else:
                    logger.error("Intel alert delivery failed: %s", alert.type)
        except Exception as e:
            logger.error("Intel alert loop error: %s", e)
            await asyncio.sleep(5)
