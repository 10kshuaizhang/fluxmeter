"""HTTPS delivery for budget alerts consumed from Kafka."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Optional

import httpx
import redis

logger = logging.getLogger("webhook_deliver")

MAX_RETRIES = int(os.getenv("WEBHOOK_MAX_RETRIES", "3"))
ALERT_TYPES = frozenset({"BUDGET_LOW", "BUDGET_EXHAUSTED", "BUDGET_WARN"})


def sign_payload(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def deliver_webhook(url: str, secret: str, payload: dict[str, Any]) -> bool:
    body = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if secret:
        headers["X-FluxMeter-Signature"] = sign_payload(secret, body)
    for attempt in range(MAX_RETRIES):
        try:
            resp = httpx.post(url, content=body, headers=headers, timeout=10.0)
            if resp.status_code < 500:
                return resp.status_code < 400
        except httpx.HTTPError as e:
            logger.warning("Webhook attempt %d failed: %s", attempt + 1, e)
        time.sleep(2 ** attempt)
    return False


def fire_budget_webhook(
    r: redis.Redis,
    customer_id: str,
    alert_type: str,
    *,
    balance_usd: Optional[float] = None,
    window_cost_usd: Optional[float] = None,
    model_id: Optional[str] = None,
    warn_pct: Optional[int] = None,
    spent_pct: Optional[float] = None,
    initial_balance_usd: Optional[float] = None,
) -> bool:
    """POST configured webhook for budget alerts. Returns delivery ok."""
    if alert_type not in ALERT_TYPES:
        return False

    url = r.get(f"budget:{customer_id}:webhook_url")
    if not url:
        return False
    secret = r.get(f"budget:{customer_id}:webhook_secret") or ""

    payload: dict[str, Any] = {
        "type": alert_type,
        "customer_id": customer_id,
        "balance_usd": balance_usd,
        "window_cost_usd": window_cost_usd,
        "model_id": model_id,
        "timestamp": int(time.time() * 1000),
    }
    if warn_pct is not None:
        payload["warn_pct"] = warn_pct
    if spent_pct is not None:
        payload["spent_pct"] = spent_pct
    if initial_balance_usd is not None:
        payload["initial_balance_usd"] = initial_balance_usd

    ok = deliver_webhook(url, secret, payload)
    if not ok:
        logger.error("Webhook delivery failed for %s %s", customer_id, alert_type)
        r.incr("metrics:webhook_delivery_failed")
    return ok
