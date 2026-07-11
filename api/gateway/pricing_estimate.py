"""Cost estimation for Gateway pre-check and reserve."""

from __future__ import annotations

import os

from pricing_loader import get_catalog

DEFAULT_ESTIMATE_USD = float(os.getenv("GATEWAY_DEFAULT_ESTIMATE_USD", "0.05"))
DEFAULT_INPUT_TOKENS = int(os.getenv("GATEWAY_DEFAULT_INPUT_TOKENS", "512"))


def estimate_request_cost(model: str, max_tokens: int | None = None) -> float:
    """Estimate USD cost for a chat completion request."""
    catalog = get_catalog()
    output_tokens = max_tokens if max_tokens and max_tokens > 0 else 1024
    event = {
        "modelId": model or "unknown",
        "inputTokens": DEFAULT_INPUT_TOKENS,
        "outputTokens": output_tokens,
    }
    micro = catalog.calculate_cost_micro(event, monthly_tokens_before=0)
    return max(micro / 1_000_000, 0.000001)


def token_rates_per_token(model: str) -> tuple[float, float]:
    """Return (input_cost_per_token, output_cost_per_token) in USD."""
    pricing = get_catalog().model_pricing(model or "unknown")
    tier = pricing.tier_at_token(0)
    return tier.input_per_m / 1_000_000, tier.output_per_m / 1_000_000


def estimate_stream_cost(
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> float:
    cost_in, cost_out = token_rates_per_token(model)
    return input_tokens * cost_in + output_tokens * cost_out
