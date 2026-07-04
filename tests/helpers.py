"""Shared HTTP helpers for FluxMeter integration / E2E tests."""

from __future__ import annotations

import os
import time
import uuid
from typing import Optional

import httpx

from conftest import admin_headers, api_headers

# ponytail: 127.0.0.1 — macOS often binds Docker on IPv4 only; localhost → ::1 returns 503
API = os.getenv("FLUXMETER_API", "http://127.0.0.1:8000")
CP_API = os.getenv("FLUXMETER_CP_API", "http://127.0.0.1:8001")
TIMEOUT = httpx.Timeout(10.0)
POLL_TIMEOUT_SEC = 180
POLL_INTERVAL_SEC = 2

# Per-token microdollar rates (matches PricingCatalog: tokens * price_per_M)
PRICE_PER_M = {
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "o1": {"input": 15.00, "output": 60.00},
}


def cost_micro(model: str, input_tokens: int, output_tokens: int = 0) -> int:
    """Expected cost in microdollars for one event."""
    rates = PRICE_PER_M.get(model, {"input": 1.0, "output": 3.0})
    return round(input_tokens * rates["input"]) + round(output_tokens * rates["output"])


def cost_usd(model: str, input_tokens: int, output_tokens: int = 0) -> float:
    return cost_micro(model, input_tokens, output_tokens) / 1_000_000.0


def ingest(
    customer_id: str,
    model_id: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    headers: Optional[dict[str, str]] = None,
    **kwargs,
) -> dict:
    event = {
        "customerId": customer_id,
        "modelId": model_id,
        "provider": kwargs.get("provider", "openai"),
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "timestamp": kwargs.get("timestamp", int(time.time() * 1000)),
    }
    if kwargs.get("parent_span_id"):
        event["parentSpanId"] = kwargs["parent_span_id"]
    if kwargs.get("event_id"):
        event["eventId"] = kwargs["event_id"]
    hdrs = headers or api_headers()
    resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT, headers=hdrs)
    return resp


def ingest_batch(events: list[dict], headers: Optional[dict[str, str]] = None) -> httpx.Response:
    hdrs = headers or api_headers()
    return httpx.post(f"{API}/ingest/batch", json=events, timeout=TIMEOUT, headers=hdrs)


def get_usage(customer_id: str, headers: Optional[dict[str, str]] = None) -> Optional[dict]:
    hdrs = headers or api_headers()
    resp = httpx.get(f"{API}/usage/customer/{customer_id}", timeout=TIMEOUT, headers=hdrs)
    if resp.status_code == 404:
        return None
    return resp.json()


def get_budget(customer_id: str, headers: Optional[dict[str, str]] = None) -> Optional[dict]:
    hdrs = headers or api_headers()
    resp = httpx.get(f"{API}/budget/{customer_id}", timeout=TIMEOUT, headers=hdrs)
    if resp.status_code == 404:
        return None
    return resp.json()


def set_budget(customer_id: str, balance: float, threshold: float = 1.0, max_rpm: int = 0) -> dict:
    body = {"balance_usd": balance, "alert_threshold_usd": threshold}
    if max_rpm > 0:
        body["max_rpm"] = max_rpm
    resp = httpx.post(
        f"{API}/budget/{customer_id}",
        json=body,
        timeout=TIMEOUT,
        headers=admin_headers(),
    )
    assert resp.status_code == 200
    return resp.json()


def check_budget(customer_id: str, estimated_cost: float = 0.0, headers: Optional[dict[str, str]] = None) -> dict:
    hdrs = headers or api_headers()
    resp = httpx.get(
        f"{API}/budget/{customer_id}/check",
        params={"estimated_cost_usd": estimated_cost},
        timeout=TIMEOUT,
        headers=hdrs,
    )
    return resp.json()


def push_watermarks_and_wait(
    cust: str,
    models: Optional[list[str]] = None,
    rounds: int = 2,
    pause_sec: float = 12.0,
):
    if models is None:
        models = ["gpt-4o-mini"]
    for model in models:
        for _ in range(rounds):
            ingest(cust, model, input_tokens=1, output_tokens=1)
            time.sleep(pause_sec)
    time.sleep(5)


def wait_for_customer_usage(
    customer_id: str,
    min_events: int = 1,
    min_cost_usd: float = 0.0,
    timeout: float = POLL_TIMEOUT_SEC,
    keepalive_model: Optional[str] = "gpt-4o-mini",
) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        usage = get_usage(customer_id)
        if (
            usage is not None
            and usage.get("event_count", 0) >= min_events
            and usage.get("cost_usd", 0) >= min_cost_usd
        ):
            return usage
        if keepalive_model:
            ingest(customer_id, keepalive_model, input_tokens=1, output_tokens=1)
        time.sleep(POLL_INTERVAL_SEC)
    usage = get_usage(customer_id)
    raise AssertionError(
        f"Usage for {customer_id} not ready after {timeout}s (last={usage})"
    )


def wait_for_budget_balance(
    customer_id: str,
    max_balance: float,
    timeout: float = POLL_TIMEOUT_SEC,
) -> dict:
    """Poll until balance_usd <= max_balance (Flink sink deducted)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        budget = get_budget(customer_id)
        if budget and budget["balance_usd"] <= max_balance + 1e-6:
            return budget
        ingest(customer_id, "gpt-4o-mini", input_tokens=1, output_tokens=1)
        time.sleep(POLL_INTERVAL_SEC)
    budget = get_budget(customer_id)
    raise AssertionError(
        f"Budget for {customer_id} still > {max_balance} after {timeout}s: {budget}"
    )


def create_customer_api_key(customer_id: str) -> dict:
    resp = httpx.post(
        f"{API}/admin/customers/{customer_id}/api-keys",
        timeout=TIMEOUT,
        headers=admin_headers(),
    )
    assert resp.status_code == 200
    return resp.json()


def unique_customer(prefix: str = "e2e") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"
