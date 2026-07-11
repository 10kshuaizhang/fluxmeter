import sys

sys.path.insert(0, "api")

from intelligence.simulation import (
    simulate_model_switch,
    simulate_prompt_reduction,
    simulate_token_grant,
)


def test_simulate_model_switch():
    result = simulate_model_switch(
        input_tokens=1_000_000,
        output_tokens=500_000,
        from_model="gpt-4o",
        to_model="claude-sonnet-4",
        monthly_occurrences=1,
    )
    assert result.scenario == "model_switch"
    assert result.annual_savings_usd is not None


def test_simulate_prompt_reduction():
    result = simulate_prompt_reduction(cost_usd=1000.0, input_reduction_pct=20.0)
    # ponytail: input ~50% of cost → 20% input cut saves 10% of monthly cost
    assert result.annual_savings_usd == 1200.0  # 100/mo * 12


def test_simulate_token_grant():
    result = simulate_token_grant(
        cost_usd=1000.0,
        grant_tokens=1_000_000,
        signup_lift_pct=30.0,
        avg_revenue_per_customer_usd=50.0,
        customer_count=100,
    )
    assert result.scenario == "token_grant"
    assert result.annual_profit_delta_usd is not None
