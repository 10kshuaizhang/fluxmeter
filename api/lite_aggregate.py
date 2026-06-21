"""Lite-mode per-event Redis aggregation (no Flink).

Mirrors OptimizedRedisSink key schema so /usage/* endpoints work identically.
Pricing logic matches UsageAggregate.java (microdollar arithmetic).
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

KNOWN_MODELS = frozenset({
    "gpt-4o", "gpt-4o-mini", "o1", "o3-mini",
    "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
    "gemini-1.5-pro", "gemini-1.5-flash",
    "text-embedding-3-small", "text-embedding-3-large",
})

PREFIX_MODELS = [
    "gpt-4o-mini", "gpt-4o", "o3-mini", "o1",
    "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
    "gemini-1.5-pro", "gemini-1.5-flash",
    "text-embedding-3-large", "text-embedding-3-small",
]


def normalize_model_id(model: str) -> str:
    if not model:
        return "unknown"
    if model in KNOWN_MODELS:
        return model
    for known in PREFIX_MODELS:
        if model.startswith(known):
            return known
    return model


def _input_price(model: str) -> float:
    model = normalize_model_id(model)
    prices = {
        "gpt-4o": 2.50,
        "gpt-4o-mini": 0.15,
        "o1": 15.00,
        "o3-mini": 1.10,
        "claude-opus-4": 15.00,
        "claude-sonnet-4": 3.00,
        "claude-haiku-4": 0.80,
        "gemini-1.5-pro": 3.50,
        "gemini-1.5-flash": 0.075,
    }
    return prices.get(model, 1.00)


def _output_price(model: str) -> float:
    model = normalize_model_id(model)
    prices = {
        "gpt-4o": 10.00,
        "gpt-4o-mini": 0.60,
        "o1": 60.00,
        "o3-mini": 4.40,
        "claude-opus-4": 75.00,
        "claude-sonnet-4": 15.00,
        "claude-haiku-4": 4.00,
        "gemini-1.5-pro": 10.50,
        "gemini-1.5-flash": 0.30,
    }
    return prices.get(model, 3.00)


def _embedding_price(model: str) -> float:
    model = normalize_model_id(model)
    prices = {
        "text-embedding-3-small": 0.02,
        "text-embedding-3-large": 0.13,
    }
    return prices.get(model, 0.10)


def calculate_event_cost_micro(event: dict[str, Any]) -> int:
    model = event.get("modelId", "unknown")
    cost = 0.0
    cost += event.get("inputTokens", 0) * _input_price(model)
    cost += event.get("outputTokens", 0) * _output_price(model)
    cost += event.get("cacheReadTokens", 0) * _input_price(model) * 0.5
    cost += event.get("reasoningTokens", 0) * _output_price(model)
    cost += event.get("cacheWriteTokens", 0) * _input_price(model)
    cost += event.get("embeddingTokens", 0) * _embedding_price(model)
    return round(cost)


def total_tokens(event: dict[str, Any]) -> int:
    return (
        event.get("inputTokens", 0)
        + event.get("outputTokens", 0)
        + event.get("cacheReadTokens", 0)
        + event.get("cacheWriteTokens", 0)
        + event.get("reasoningTokens", 0)
        + event.get("embeddingTokens", 0)
    )


def aggregate_event(r, event: dict[str, Any]) -> bool:
    """Increment Redis counters for one event. Returns False if duplicate eventId."""
    event_id = event.get("eventId")
    if event_id:
        idemp_key = "e:" + hashlib.sha256(event_id.encode()).hexdigest()[:16]
        if not r.set(idemp_key, "1", nx=True, ex=600):
            return False

    customer_id = event["customerId"]
    model_id = event["modelId"]
    customer_key = f"customer:{customer_id}"
    model_key = f"{customer_key}:model:{model_id}"

    input_t = event.get("inputTokens", 0)
    output_t = event.get("outputTokens", 0)
    cache_read = event.get("cacheReadTokens", 0)
    reasoning = event.get("reasoningTokens", 0)
    total_t = total_tokens(event)
    cost_usd = calculate_event_cost_micro(event) / 1_000_000.0
    now_ms = event.get("timestamp", int(time.time() * 1000))

    pipe = r.pipeline()
    pipe.incrby(f"{customer_key}:input_tokens", input_t)
    pipe.incrby(f"{customer_key}:output_tokens", output_t)
    pipe.incrby(f"{customer_key}:total_tokens", total_t)
    pipe.incrby(f"{customer_key}:event_count", 1)
    pipe.incrbyfloat(f"{customer_key}:cost_usd", cost_usd)
    if cache_read > 0:
        pipe.incrby(f"{customer_key}:cache_read_tokens", cache_read)
    if reasoning > 0:
        pipe.incrby(f"{customer_key}:reasoning_tokens", reasoning)

    pipe.incrby(f"{model_key}:input_tokens", input_t)
    pipe.incrby(f"{model_key}:output_tokens", output_t)
    pipe.incrby(f"{model_key}:total_tokens", total_t)
    pipe.incrbyfloat(f"{model_key}:cost_usd", cost_usd)

    pipe.incrby("global:total_tokens", total_t)
    pipe.incrby("global:input_tokens", input_t)
    pipe.incrby("global:output_tokens", output_t)
    pipe.incrby("global:total_events", 1)
    pipe.incrbyfloat("global:total_cost_usd", cost_usd)
    pipe.set("global:last_window_end", str(now_ms))
    pipe.execute()
    return True
