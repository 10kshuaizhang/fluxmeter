from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from auth import require_admin_key, require_api_key
from intelligence.connectors.openmeter import import_openmeter_events
from intelligence.alerts import set_webhook_config
from intelligence.forecast import compute_forecast
from intelligence.models import (
    CustomerEconomics,
    PricingRecommendation,
    ProfitabilityDashboard,
    RootCauseReport,
    SimulationRequest,
    SimulationResult,
    SpendForecast,
)
from intelligence.pricing_optimizer import compute_pricing_recommendations
from intelligence.profitability import build_profitability_dashboard
from intelligence.report import build_report_json, build_report_markdown
from intelligence.revenue_store import set_revenue
from intelligence.root_cause import analyze_root_cause
from intelligence.simulation import (
    simulate_model_switch,
    simulate_prompt_reduction,
    simulate_token_grant,
)
from intelligence.unit_economics import compute_unit_economics

router = APIRouter(prefix="/intelligence", tags=["intelligence"])


def _redis():
    from main import get_redis

    return get_redis()


@router.get(
    "/root-cause",
    response_model=RootCauseReport,
    dependencies=[Depends(require_api_key)],
)
def root_cause(
    period: str,
    baseline_period: str,
    scope: str = "global",
) -> RootCauseReport:
    return analyze_root_cause(
        _redis(),
        period=period,
        baseline_period=baseline_period,
        scope=scope,
    )


@router.get(
    "/unit-economics",
    response_model=list[CustomerEconomics],
    dependencies=[Depends(require_api_key)],
)
def unit_economics(period: str) -> list[CustomerEconomics]:
    return compute_unit_economics(_redis(), period=period)


@router.post(
    "/simulate",
    response_model=SimulationResult,
    dependencies=[Depends(require_api_key)],
)
def simulate(body: SimulationRequest) -> SimulationResult:
    scenario = body.scenario
    if scenario == "model_switch":
        if (
            body.input_tokens is None
            or body.output_tokens is None
            or not body.from_model
            or not body.to_model
        ):
            raise HTTPException(
                status_code=400,
                detail="model_switch requires input_tokens, output_tokens, from_model, to_model",
            )
        return simulate_model_switch(
            input_tokens=body.input_tokens,
            output_tokens=body.output_tokens,
            from_model=body.from_model,
            to_model=body.to_model,
            monthly_occurrences=body.monthly_occurrences,
        )
    if scenario == "prompt_reduction":
        if body.cost_usd is None or body.input_reduction_pct is None:
            raise HTTPException(
                status_code=400,
                detail="prompt_reduction requires cost_usd, input_reduction_pct",
            )
        return simulate_prompt_reduction(
            cost_usd=body.cost_usd,
            input_reduction_pct=body.input_reduction_pct,
        )
    if scenario == "token_grant":
        if (
            body.cost_usd is None
            or body.grant_tokens is None
            or body.signup_lift_pct is None
            or body.avg_revenue_per_customer_usd is None
            or body.customer_count is None
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "token_grant requires cost_usd, grant_tokens, signup_lift_pct, "
                    "avg_revenue_per_customer_usd, customer_count"
                ),
            )
        return simulate_token_grant(
            cost_usd=body.cost_usd,
            grant_tokens=body.grant_tokens,
            signup_lift_pct=body.signup_lift_pct,
            avg_revenue_per_customer_usd=body.avg_revenue_per_customer_usd,
            customer_count=body.customer_count,
        )
    raise HTTPException(status_code=400, detail=f"unknown scenario: {scenario}")


class RevenueBody(BaseModel):
    period: str
    revenue_usd: float


@router.post("/revenue/{customer_id}", dependencies=[Depends(require_admin_key)])
def post_revenue(customer_id: str, body: RevenueBody) -> dict[str, str]:
    set_revenue(_redis(), customer_id, body.period, revenue_usd=body.revenue_usd)
    return {"status": "ok"}


@router.post("/import/openmeter", dependencies=[Depends(require_admin_key)])
def import_openmeter(payload: dict, period: str | None = None) -> dict[str, int]:
    effective_period = payload.get("period") or period
    if not effective_period:
        raise HTTPException(status_code=400, detail="period required in body or query")
    return import_openmeter_events(_redis(), payload, period=effective_period)


@router.get("/summary", dependencies=[Depends(require_api_key)])
def summary(period: str, baseline_period: str) -> dict:
    r = _redis()
    rc = analyze_root_cause(r, period=period, baseline_period=baseline_period)
    economics = compute_unit_economics(r, period=period)
    loss_customers = [e.model_dump() for e in economics if e.status == "loss"]

    headline = rc.summary
    if loss_customers:
        headline = (
            f"{rc.summary} {len(loss_customers)} customer(s) unprofitable in {period}."
        )

    return {
        "headline": headline,
        "root_cause_summary": rc.summary,
        "loss_customers": loss_customers,
    }


@router.get(
    "/pricing-recommendations",
    response_model=list[PricingRecommendation],
    dependencies=[Depends(require_api_key)],
)
def pricing_recommendations(period: str) -> list[PricingRecommendation]:
    return compute_pricing_recommendations(_redis(), period=period)


@router.get(
    "/profitability",
    response_model=ProfitabilityDashboard,
    dependencies=[Depends(require_api_key)],
)
def profitability(period: str, months: int = 3) -> ProfitabilityDashboard:
    return build_profitability_dashboard(_redis(), period=period, months=months)


@router.get(
    "/forecast",
    response_model=SpendForecast,
    dependencies=[Depends(require_api_key)],
)
def forecast(period: str, scope: str = "global") -> SpendForecast:
    return compute_forecast(_redis(), period=period, scope=scope)


@router.get("/report", dependencies=[Depends(require_api_key)])
def report(
    period: str,
    baseline_period: str | None = None,
    format: str = "json",
):
    r = _redis()
    if format == "markdown":
        from fastapi.responses import PlainTextResponse

        return PlainTextResponse(
            build_report_markdown(r, period=period, baseline_period=baseline_period),
            media_type="text/markdown",
        )
    return build_report_json(r, period=period, baseline_period=baseline_period)


class IntelWebhookBody(BaseModel):
    url: str
    secret: str = ""


@router.post("/alerts/webhook", dependencies=[Depends(require_admin_key)])
def configure_intel_webhook(body: IntelWebhookBody) -> dict[str, str]:
    set_webhook_config(_redis(), body.url, body.secret)
    return {"status": "ok"}
