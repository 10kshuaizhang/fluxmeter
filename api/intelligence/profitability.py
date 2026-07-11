from __future__ import annotations

import redis

from intelligence.models import CustomerEconomics, PeriodTotals, ProductEconomics, ProfitabilityDashboard
from intelligence.native_reader import (
    list_customer_period_costs,
    list_dim_period_costs,
    list_global_period_costs,
)
from intelligence.revenue_store import get_revenue
from intelligence.unit_economics import compute_unit_economics


def _prior_periods(period: str, months: int) -> list[str]:
    year, month = int(period[:4]), int(period[5:7])
    out: list[str] = []
    y, m = year, month
    for _ in range(months):
        out.append(f"{y:04d}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return list(reversed(out))


def _by_product(
    r: redis.Redis, period: str, economics: list[CustomerEconomics]
) -> list[ProductEconomics]:
    # ponytail: allocate customer revenue to features by cost share; upgrade: per-SKU revenue
    dims = list_dim_period_costs(r, period).get("feature", {})
    if not dims:
        return []

    total_feature_cost = sum(dims.values()) or 1.0
    global_rev = sum(e.revenue_usd for e in economics if e.revenue_usd is not None)

    products: list[ProductEconomics] = []
    for feature, cost in dims.items():
        allocated_rev = global_rev * (cost / total_feature_cost) if global_rev else None
        margin = (allocated_rev - cost) if allocated_rev is not None else None
        margin_pct = (margin / allocated_rev * 100) if allocated_rev and allocated_rev > 0 else None
        products.append(
            ProductEconomics(
                product=feature,
                period=period,
                cost_usd=cost,
                revenue_usd=allocated_rev,
                margin_usd=margin,
                margin_pct=margin_pct,
            )
        )
    products.sort(key=lambda p: p.margin_usd if p.margin_usd is not None else float("inf"))
    return products


def build_profitability_dashboard(
    r: redis.Redis, *, period: str, months: int = 3
) -> ProfitabilityDashboard:
    months = max(1, min(months, 12))
    period_list = _prior_periods(period, months)

    by_customer = compute_unit_economics(r, period=period)
    by_customer.sort(key=lambda e: e.margin_usd if e.margin_usd is not None else float("-inf"))

    total_cost = sum(e.cost_usd for e in by_customer)
    total_rev_vals = [e.revenue_usd for e in by_customer if e.revenue_usd is not None]
    total_revenue = sum(total_rev_vals) if total_rev_vals else None
    total_margin = (total_revenue - total_cost) if total_revenue is not None else None
    margin_pct = (
        (total_margin / total_revenue * 100) if total_revenue and total_margin is not None else None
    )
    loss_count = sum(1 for e in by_customer if e.status == "loss")

    trend_costs = list_global_period_costs(r, period_list)
    trend: list[PeriodTotals] = []
    for p in period_list:
        cost = trend_costs.get(p, 0.0)
        customer_costs = list_customer_period_costs(r, p)
        rev = sum(
            (get_revenue(r, cid, p) or {}).get("revenue_usd", 0)
            for cid in customer_costs
        )
        rev_val = rev if rev > 0 else None
        margin = (rev_val - cost) if rev_val is not None else None
        trend.append(
            PeriodTotals(period=p, cost_usd=cost, revenue_usd=rev_val, margin_usd=margin)
        )

    return ProfitabilityDashboard(
        period=period,
        months=months,
        totals={
            "revenue_usd": total_revenue,
            "cost_usd": total_cost,
            "margin_usd": total_margin,
            "margin_pct": margin_pct,
            "loss_customer_count": loss_count,
        },
        by_customer=by_customer,
        by_product=_by_product(r, period, by_customer),
        trend=trend,
    )
