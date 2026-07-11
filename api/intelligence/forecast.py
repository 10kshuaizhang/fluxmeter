from __future__ import annotations

import calendar
from datetime import date, datetime, timezone

import redis

from intelligence.models import SpendForecast
from intelligence.native_reader import list_customer_daily_costs, list_global_daily_costs


def _days_in_period(period: str) -> int:
    year, month = int(period[:4]), int(period[5:7])
    return calendar.monthrange(year, month)[1]


def _current_day_of_month(period: str) -> int:
    now = datetime.now(timezone.utc)
    year, month = int(period[:4]), int(period[5:7])
    if now.year == year and now.month == month:
        return now.day
    return _days_in_period(period)


def _budget_for_scope(r: redis.Redis, scope: str) -> float | None:
    if scope == "global":
        return None
    if scope.startswith("customer:"):
        cid = scope.split(":", 1)[1]
        initial = r.get(f"budget:{cid}:initial_balance_usd")
        return float(initial) if initial is not None else None
    return None


def compute_forecast(r: redis.Redis, *, period: str, scope: str = "global") -> SpendForecast:
    date_prefix = period
    if scope.startswith("customer:"):
        cid = scope.split(":", 1)[1]
        daily = list_customer_daily_costs(r, cid, date_prefix)
    else:
        daily = list_global_daily_costs(r, date_prefix)

    mtd = sum(daily.values())
    days_elapsed = max(len(daily), _current_day_of_month(period), 1)
    days_total = _days_in_period(period)
    days_remaining = max(days_total - days_elapsed, 0)
    avg_daily = mtd / days_elapsed if days_elapsed else 0.0
    forecast_eom = mtd + avg_daily * days_remaining

    budget = _budget_for_scope(r, scope)
    variance = (forecast_eom - budget) if budget is not None else None

    if budget is None:
        status = "no_budget"
        summary = f"MTD ${mtd:.2f}; forecast EOM ${forecast_eom:.2f} (no budget configured)"
    elif forecast_eom > budget:
        status = "over_budget"
        summary = f"Forecast EOM ${forecast_eom:.2f} exceeds budget ${budget:.2f} by ${variance:.2f}"
    elif forecast_eom > budget * 0.85:
        status = "at_risk"
        summary = f"Forecast EOM ${forecast_eom:.2f} is at risk vs budget ${budget:.2f}"
    else:
        status = "on_track"
        summary = f"Forecast EOM ${forecast_eom:.2f} on track vs budget ${budget:.2f}"

    return SpendForecast(
        period=period,
        scope=scope,
        mtd_cost_usd=mtd,
        forecast_eom_cost_usd=forecast_eom,
        budget_usd=budget,
        variance_usd=variance,
        status=status,
        summary=summary,
    )
