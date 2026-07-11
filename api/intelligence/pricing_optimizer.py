from __future__ import annotations

import redis

from pricing_loader import get_catalog

from intelligence.models import PricingRecommendation
from intelligence.native_reader import list_model_period_costs
from intelligence.simulation import _model_cost_usd, simulate_model_switch
from intelligence.unit_economics import compute_unit_economics
from usage_buckets import model_period_key, read_usage_bucket

TARGET_MARGIN = 0.30


def _prior_period(period: str) -> str:
    year, month = int(period[:4]), int(period[5:7])
    if month == 1:
        return f"{year - 1}-12"
    return f"{year:04d}-{month - 1:02d}"


def _cheaper_model(
    from_model: str, input_tokens: int, output_tokens: int
) -> tuple[str | None, float | None]:
    from_cost = _model_cost_usd(from_model, input_tokens, output_tokens)
    best_model: str | None = None
    best_cost = from_cost
    for model_id in get_catalog().models:
        if model_id == from_model:
            continue
        cost = _model_cost_usd(model_id, input_tokens, output_tokens)
        if cost < best_cost:
            best_cost = cost
            best_model = model_id
    if best_model is None:
        return None, None
    return best_model, (from_cost - best_cost) * 12


def _model_tokens_for_customer(
    r: redis.Redis, customer_id: str, period: str, model_id: str
) -> tuple[int, int]:
    key = model_period_key(customer_id, model_id, period)
    data = read_usage_bucket(r, key)
    if not data:
        return 1_000_000, 500_000
    inp = max(data.get("input_tokens", 0), 1)
    out = max(data.get("output_tokens", 0), 1)
    return inp, out


def compute_pricing_recommendations(
    r: redis.Redis, *, period: str
) -> list[PricingRecommendation]:
    rows = compute_unit_economics(r, period=period)
    prior = _prior_period(period)
    has_prior = bool(list_model_period_costs(r, prior))
    recs: list[PricingRecommendation] = []

    for row in rows:
        confidence = "high" if has_prior and row.revenue_usd is not None else "medium"

        if row.status == "unknown_revenue":
            recs.append(
                PricingRecommendation(
                    customer_id=row.customer_id,
                    period=period,
                    action="connect_revenue",
                    current_margin_pct=None,
                    suggested_change="Connect revenue via OpenMeter overlay or POST /intelligence/revenue",
                    roi_annual_usd=None,
                    confidence="medium",
                )
            )
            continue

        if row.status == "loss" and row.revenue_usd is not None:
            suggested_rev = row.cost_usd / (1.0 - TARGET_MARGIN)
            roi = (suggested_rev - row.revenue_usd) * 12
            recs.append(
                PricingRecommendation(
                    customer_id=row.customer_id,
                    period=period,
                    action="price_increase",
                    current_margin_pct=row.margin_pct,
                    suggested_change=(
                        f"Raise price to ${suggested_rev:.2f}/mo for {TARGET_MARGIN:.0%} margin "
                        f"(current ${row.revenue_usd:.2f})"
                    ),
                    roi_annual_usd=roi,
                    confidence=confidence,
                )
            )

        models = list_model_period_costs(r, period, customer_id=row.customer_id)
        if models:
            top_model = max(models, key=models.get)
            inp, out = _model_tokens_for_customer(r, row.customer_id, period, top_model)
            alt, annual_save = _cheaper_model(top_model, inp, out)
            if alt and annual_save and annual_save > 0:
                sim = simulate_model_switch(
                    input_tokens=inp,
                    output_tokens=out,
                    from_model=top_model,
                    to_model=alt,
                )
                recs.append(
                    PricingRecommendation(
                        customer_id=row.customer_id,
                        period=period,
                        action="model_switch",
                        current_margin_pct=row.margin_pct,
                        suggested_change=sim.notes,
                        roi_annual_usd=sim.annual_savings_usd,
                        confidence=confidence,
                    )
                )
            continue

        if row.status == "profitable" and row.margin_pct is not None and row.margin_pct < 10:
            suggested_rev = row.cost_usd / (1.0 - TARGET_MARGIN)
            roi = (suggested_rev - (row.revenue_usd or 0)) * 12
            recs.append(
                PricingRecommendation(
                    customer_id=row.customer_id,
                    period=period,
                    action="price_increase",
                    current_margin_pct=row.margin_pct,
                    suggested_change=f"Low margin — consider price increase to ${suggested_rev:.2f}/mo",
                    roi_annual_usd=max(0.0, roi),
                    confidence=confidence,
                )
            )

    recs.sort(key=lambda x: x.roi_annual_usd or 0, reverse=True)
    return recs
