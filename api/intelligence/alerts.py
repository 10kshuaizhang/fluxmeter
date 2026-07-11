from __future__ import annotations

import json
import os
from typing import Any

import redis

from intelligence.forecast import compute_forecast
from intelligence.models import IntelAlertPayload
from intelligence.root_cause import analyze_root_cause
from intelligence.unit_economics import compute_unit_economics

COST_SPIKE_PCT = float(os.getenv("INTEL_COST_SPIKE_PCT", "25"))


def _prior_period(period: str) -> str:
    year, month = int(period[:4]), int(period[5:7])
    if month == 1:
        return f"{year - 1}-12"
    return f"{year:04d}-{month - 1:02d}"


def _actions_for(period: str) -> list[dict[str, str]]:
    return [
        {
            "label": "Review profitability",
            "endpoint": f"/intelligence/profitability?period={period}",
        },
        {
            "label": "Download report",
            "endpoint": f"/intelligence/report?period={period}&format=markdown",
        },
    ]


def detect_cost_spike(r: redis.Redis, *, period: str) -> IntelAlertPayload | None:
    baseline = _prior_period(period)
    report = analyze_root_cause(r, period=period, baseline_period=baseline, scope="global")
    if report.baseline_cost_usd <= 0 or report.delta_pct <= COST_SPIKE_PCT:
        return None
    return IntelAlertPayload(
        type="INTEL_COST_SPIKE",
        period=period,
        summary=report.summary,
        recommendation="Review top contributors and consider model switch or pricing adjustment",
        actions=_actions_for(period),
    )


def detect_margin_loss(r: redis.Redis, *, period: str) -> IntelAlertPayload | None:
    economics = compute_unit_economics(r, period=period)
    loss_ids = {e.customer_id for e in economics if e.status == "loss"}

    snap_key = f"intel:snapshot:loss_customers:{period}"
    prev_raw = r.get(snap_key)
    prev_ids = set(json.loads(prev_raw)) if prev_raw else set()
    r.set(snap_key, json.dumps(sorted(loss_ids)), ex=86400 * 400)

    new_loss = loss_ids - prev_ids
    if not new_loss:
        return None

    names = ", ".join(sorted(new_loss)[:5])
    extra = f" (+{len(new_loss) - 5} more)" if len(new_loss) > 5 else ""
    return IntelAlertPayload(
        type="INTEL_MARGIN_LOSS",
        period=period,
        summary=f"{len(new_loss)} new unprofitable customer(s): {names}{extra}",
        recommendation="Review unit economics and pricing recommendations",
        actions=_actions_for(period),
    )


def detect_forecast_risk(r: redis.Redis, *, period: str) -> IntelAlertPayload | None:
    fc = compute_forecast(r, period=period, scope="global")
    if fc.status not in ("at_risk", "over_budget"):
        return None
    return IntelAlertPayload(
        type="INTEL_FORECAST_RISK",
        period=period,
        summary=fc.summary,
        recommendation="Align spend with budget or raise limits",
        actions=_actions_for(period),
    )


def collect_alerts(r: redis.Redis, *, period: str) -> list[IntelAlertPayload]:
    alerts: list[IntelAlertPayload] = []
    for detector in (detect_cost_spike, detect_margin_loss, detect_forecast_risk):
        alert = detector(r, period=period)
        if alert:
            alerts.append(alert)
    return alerts


def alert_debounce_key(alert_type: str, period: str) -> str:
    return f"intel:alert:{alert_type}:{period}:sent"


def should_send_alert(r: redis.Redis, alert_type: str, period: str) -> bool:
    return not r.exists(alert_debounce_key(alert_type, period))


def mark_alert_sent(r: redis.Redis, alert_type: str, period: str) -> None:
    r.set(alert_debounce_key(alert_type, period), "1", ex=86400)


def get_webhook_config(r: redis.Redis) -> tuple[str, str]:
    url = os.getenv("FLUXMETER_INTEL_WEBHOOK_URL") or r.get("intel:webhook:url") or ""
    secret = os.getenv("FLUXMETER_INTEL_WEBHOOK_SECRET") or r.get("intel:webhook:secret") or ""
    return url, secret


def set_webhook_config(r: redis.Redis, url: str, secret: str = "") -> None:
    r.set("intel:webhook:url", url)
    r.set("intel:webhook:secret", secret)


def alert_to_payload(alert: IntelAlertPayload) -> dict[str, Any]:
    return alert.model_dump()
