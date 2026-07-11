from __future__ import annotations

import redis

from intelligence.models import CustomerEconomics
from intelligence.native_reader import list_customer_period_costs
from intelligence.revenue_store import get_revenue

_LOSS_REC = "Customer losing money — suggest plan upgrade or usage cap"
_LOW_MARGIN_REC = "Low margin — review model mix or pricing"
_UNKNOWN_REVENUE_REC = "Connect revenue (OpenMeter overlay or POST /intelligence/revenue)"


def compute_unit_economics(r: redis.Redis, *, period: str) -> list[CustomerEconomics]:
    costs = list_customer_period_costs(r, period)
    rows: list[CustomerEconomics] = []
    for customer_id, cost_usd in costs.items():
        rev = get_revenue(r, customer_id, period)
        revenue_usd = rev["revenue_usd"] if rev else None

        if revenue_usd is None:
            rows.append(
                CustomerEconomics(
                    customer_id=customer_id,
                    period=period,
                    revenue_usd=None,
                    cost_usd=cost_usd,
                    margin_usd=None,
                    margin_pct=None,
                    status="unknown_revenue",
                    recommendation=_UNKNOWN_REVENUE_REC,
                )
            )
            continue

        margin_usd = revenue_usd - cost_usd
        margin_pct = (margin_usd / revenue_usd * 100) if revenue_usd > 0 else None

        if margin_usd < 0:
            status = "loss"
            recommendation = _LOSS_REC
        else:
            status = "profitable"
            recommendation = (
                _LOW_MARGIN_REC if margin_pct is not None and margin_pct < 10 else None
            )

        rows.append(
            CustomerEconomics(
                customer_id=customer_id,
                period=period,
                revenue_usd=revenue_usd,
                cost_usd=cost_usd,
                margin_usd=margin_usd,
                margin_pct=margin_pct,
                status=status,
                recommendation=recommendation,
            )
        )
    return rows
