"""What-if scenario simulation — pure functions, no Redis."""

from __future__ import annotations

from pricing_loader import get_catalog

from intelligence.models import SimulationResult

_MICRO = 1_000_000


def _model_cost_usd(model: str, input_tokens: int, output_tokens: int) -> float:
    event = {"modelId": model, "inputTokens": input_tokens, "outputTokens": output_tokens}
    return get_catalog().calculate_cost_micro(event) / _MICRO


def simulate_model_switch(
    *,
    input_tokens: int,
    output_tokens: int,
    from_model: str,
    to_model: str,
    monthly_occurrences: int = 1,
) -> SimulationResult:
    from_cost = _model_cost_usd(from_model, input_tokens, output_tokens)
    to_cost = _model_cost_usd(to_model, input_tokens, output_tokens)
    monthly_savings = from_cost - to_cost
    annual_savings = monthly_savings * 12 * monthly_occurrences
    return SimulationResult(
        scenario="model_switch",
        annual_savings_usd=annual_savings,
        annual_profit_delta_usd=None,
        notes=(
            f"Switch {from_model} → {to_model}: "
            f"${from_cost:.2f}/mo → ${to_cost:.2f}/mo (${monthly_savings:+.2f}/mo)"
        ),
    )


def simulate_prompt_reduction(*, cost_usd: float, input_reduction_pct: float) -> SimulationResult:
    # ponytail: input ~50% of cost; cutting input by X% saves X% of that half
    monthly_savings = cost_usd * (input_reduction_pct / 100.0) * 0.5
    annual_savings = monthly_savings * 12
    return SimulationResult(
        scenario="prompt_reduction",
        annual_savings_usd=annual_savings,
        annual_profit_delta_usd=None,
        notes=f"Reduce input tokens by {input_reduction_pct:.0f}% → ~${monthly_savings:.2f}/mo saved",
    )


def simulate_token_grant(
    *,
    cost_usd: float,
    grant_tokens: int,
    signup_lift_pct: float,
    avg_revenue_per_customer_usd: float,
    customer_count: int,
) -> SimulationResult:
    cost_per_token = cost_usd / grant_tokens if grant_tokens > 0 else 0.0
    new_customers = customer_count * (signup_lift_pct / 100.0)
    revenue_gain = new_customers * avg_revenue_per_customer_usd
    grant_cost = (grant_tokens * cost_per_token) * new_customers
    profit_delta = revenue_gain - grant_cost
    annual_profit_delta = profit_delta * 12
    return SimulationResult(
        scenario="token_grant",
        annual_savings_usd=None,
        annual_profit_delta_usd=annual_profit_delta,
        notes=(
            f"+{new_customers:.0f} new customers @ ${avg_revenue_per_customer_usd:.2f}/mo revenue; "
            f"grant cost ${grant_cost:.2f}/mo"
        ),
    )
