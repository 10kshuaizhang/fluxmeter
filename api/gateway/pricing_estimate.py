"""Cost estimation for Gateway pre-check and reserve — catalog cost_micro only."""

from __future__ import annotations

import os

from pricing_loader import get_catalog

DEFAULT_INPUT_TOKENS = int(os.getenv("GATEWAY_DEFAULT_INPUT_TOKENS", "512"))


def estimate_request_cost(model: str, max_tokens: int | None = None) -> float:
    """Estimate USD cost for a chat completion request (monthly_before=0, advisory)."""
    catalog = get_catalog()
    output_tokens = max_tokens if max_tokens and max_tokens > 0 else 1024
    event = {
        "modelId": model or "unknown",
        "inputTokens": DEFAULT_INPUT_TOKENS,
        "outputTokens": output_tokens,
    }
    micro = catalog.calculate_cost_micro(event, monthly_tokens_before=0)
    return max(micro / 1_000_000, 0.000001)


def estimate_stream_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Running stream estimate via the same catalog path as billing (monthly_before=0)."""
    micro = get_catalog().calculate_cost_micro(
        {
            "modelId": model or "unknown",
            "inputTokens": max(0, input_tokens),
            "outputTokens": max(0, output_tokens),
        },
        monthly_tokens_before=0,
    )
    return micro / 1_000_000
