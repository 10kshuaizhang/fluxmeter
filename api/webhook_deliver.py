"""HTTPS budget webhook delivery (shared by Kafka worker and Lite path)."""

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
LOW_ALERT_TTL_SEC = int(os.getenv("WEBHOOK_LOW_DEBOUNCE_SEC", "3600"))
# Spent-fraction ladder (Claude/Cursor style). Overridable, comma-separated percents.
WARN_PCTS = tuple(
    int(p.strip())
    for p in os.getenv("BUDGET_WARN_PCTS", "70,90").split(",")
    if p.strip().isdigit()
)
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


def _spent_pct(initial: float, balance: float) -> float:
    if initial <= 0:
        return 100.0 if balance <= 0 else 0.0
    spent = max(0.0, initial - balance)
    return min(100.0, (spent / initial) * 100.0)


def lite_alerts_for_result(
    r: redis.Redis,
    customer_id: str,
    result: dict[str, Any],
    model_id: Optional[str] = None,
) -> list[tuple[str, dict[str, Any]]]:
    """Return (alert_type, extra_payload_fields) list. Side-effect: debounce keys."""
    alerts: list[tuple[str, dict[str, Any]]] = []
    balance = result.get("balance_usd")

    if result.get("budget_alert") == "BUDGET_EXHAUSTED":
        alerts.append(("BUDGET_EXHAUSTED", {}))
        r.delete(f"budget:{customer_id}:webhook_low_sent")
        for pct in WARN_PCTS:
            r.delete(f"budget:{customer_id}:webhook_warn_{pct}_sent")
        return alerts

    if balance is None:
        return alerts

    balance_f = float(balance)
    initial_raw = r.get(f"budget:{customer_id}:initial_balance_usd")
    initial = float(initial_raw) if initial_raw is not None else None
    # Effective ceiling for % ladder: initial + topups (if tracked)
    topup_raw = r.get(f"budget:{customer_id}:total_topup_usd")
    if initial is not None and topup_raw is not None:
        try:
            initial = float(initial) + float(topup_raw)
        except (TypeError, ValueError):
            pass

    if initial is not None and initial > 0:
        spent = _spent_pct(initial, balance_f)
        for pct in sorted(WARN_PCTS):
            key = f"budget:{customer_id}:webhook_warn_{pct}_sent"
            if spent >= pct:
                if r.set(key, "1", nx=True, ex=LOW_ALERT_TTL_SEC):
                    alerts.append((
                        "BUDGET_WARN",
                        {
                            "warn_pct": pct,
                            "spent_pct": round(spent, 2),
                            "initial_balance_usd": initial,
                        },
                    ))
            else:
                r.delete(key)
    else:
        for pct in WARN_PCTS:
            r.delete(f"budget:{customer_id}:webhook_warn_{pct}_sent")

    threshold_raw = r.get(f"budget:{customer_id}:alert_threshold_usd")
    if threshold_raw is not None:
        try:
            threshold = float(threshold_raw)
        except (TypeError, ValueError):
            threshold = None
        if threshold is not None:
            if balance_f <= threshold:
                if r.set(
                    f"budget:{customer_id}:webhook_low_sent", "1",
                    nx=True, ex=LOW_ALERT_TTL_SEC,
                ):
                    alerts.append(("BUDGET_LOW", {}))
            else:
                r.delete(f"budget:{customer_id}:webhook_low_sent")

    return alerts


def deliver_lite_alerts(
    r: redis.Redis,
    customer_id: str,
    result: dict[str, Any],
    model_id: Optional[str] = None,
) -> None:
    for alert_type, extra in lite_alerts_for_result(r, customer_id, result, model_id):
        fire_budget_webhook(
            r,
            customer_id,
            alert_type,
            balance_usd=result.get("balance_usd"),
            window_cost_usd=result.get("cost_usd"),
            model_id=model_id,
            **extra,
        )
