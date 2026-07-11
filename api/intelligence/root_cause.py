from __future__ import annotations

import redis

from billing_dims import ALLOWED_DIMS
from intelligence.models import Contributor, RootCauseReport
from intelligence.native_reader import (
    list_customer_period_costs,
    list_dim_period_costs,
    list_model_period_costs,
)


def _pct_delta(delta: float, baseline: float) -> float:
    return (delta / baseline * 100) if baseline != 0 else 0.0


def _share_of_total(delta: float, total_delta: float) -> float:
    return (delta / total_delta * 100) if total_delta != 0 else 0.0


def _build_contributors(
    dimension: str,
    current: dict[str, float],
    baseline: dict[str, float],
    total_delta: float,
) -> list[Contributor]:
    out: list[Contributor] = []
    for key in set(current) | set(baseline):
        curr = current.get(key, 0.0)
        base = baseline.get(key, 0.0)
        delta = curr - base
        out.append(
            Contributor(
                dimension=dimension,
                key=key,
                current_cost_usd=curr,
                baseline_cost_usd=base,
                delta_usd=delta,
                delta_pct=_pct_delta(delta, base),
                share_of_total_delta_pct=_share_of_total(delta, total_delta),
            )
        )
    return out


def analyze_root_cause(
    r: redis.Redis,
    *,
    period: str,
    baseline_period: str,
    scope: str = "global",
) -> RootCauseReport:
    customer_id: str | None = None
    if scope.startswith("customer:"):
        customer_id = scope.split(":", 1)[1]

    curr_customers = list_customer_period_costs(r, period)
    base_customers = list_customer_period_costs(r, baseline_period)
    if customer_id:
        curr_customers = {k: v for k, v in curr_customers.items() if k == customer_id}
        base_customers = {k: v for k, v in base_customers.items() if k == customer_id}

    total_current = sum(curr_customers.values())
    total_baseline = sum(base_customers.values())
    total_delta = total_current - total_baseline
    total_delta_pct = _pct_delta(total_delta, total_baseline)

    contributors: list[Contributor] = []
    curr_models = list_model_period_costs(r, period, customer_id=customer_id)
    base_models = list_model_period_costs(r, baseline_period, customer_id=customer_id)
    contributors.extend(_build_contributors("model", curr_models, base_models, total_delta))
    contributors.extend(
        _build_contributors(
            "customer",
            curr_customers,
            base_customers,
            total_delta,
        )
    )

    curr_dims = list_dim_period_costs(r, period)
    base_dims = list_dim_period_costs(r, baseline_period)
    for dim_key in ALLOWED_DIMS:
        contributors.extend(
            _build_contributors(
                dim_key,
                curr_dims.get(dim_key, {}),
                base_dims.get(dim_key, {}),
                total_delta,
            )
        )

    contributors.sort(key=lambda c: abs(c.delta_usd), reverse=True)
    top_contributors = contributors[:10]

    if top_contributors:
        top = top_contributors[0]
        summary = (
            f"Cost {total_delta_pct:+.1f}% vs {baseline_period}. "
            f"Top driver: {top.dimension} {top.key} "
            f"({top.share_of_total_delta_pct:.0f}% of change)."
        )
    else:
        summary = f"Cost {total_delta_pct:+.1f}% vs {baseline_period}. No significant contributors."

    return RootCauseReport(
        period=period,
        baseline_period=baseline_period,
        total_cost_usd=total_current,
        baseline_cost_usd=total_baseline,
        delta_usd=total_delta,
        delta_pct=total_delta_pct,
        summary=summary,
        contributors=top_contributors,
    )
